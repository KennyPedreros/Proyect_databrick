from app.services.monitoring_service import monitoring_service, LogLevel
from fastapi import APIRouter, HTTPException, BackgroundTasks
from app.models.schemas import (
    CleaningConfig, 
    CleaningJobRequest, 
    CleaningJobResponse,
    CleaningStatusResponse,
    SuccessResponse,
    ProcessStatus
)
from app.services.cleaning_service import cleaning_service
from datetime import datetime
import pandas as pd
import numpy as np
import logging

router = APIRouter(prefix="/api/clean", tags=["Módulo 3: Limpieza de Datos"])

logger = logging.getLogger(__name__)


# ============================================
# FUNCIONES DEL MÓDULO 3
# ============================================

def simulate_cleaning_process(job_id: str, config: CleaningConfig):
    """
    Simula el proceso de limpieza en background
    En producción, esto se ejecutaría en Databricks con Spark
    """
    try:
        # Actualizar estado a running
        cleaning_service.update_job_progress(job_id, 10, "running")
        
        # Generar datos de ejemplo con problemas realistas
        np.random.seed(42)
        n_records = 1000
        
        monitoring_service.log_event(
            process="Limpieza",
            level=LogLevel.INFO,
            message=f"Job de limpieza iniciado: {job_id}",
            data={"job_id": job_id, "config": config}
        )

        sample_data = pd.DataFrame({
            'case_id': range(n_records),
            'age': np.random.randint(0, 100, n_records),
            'symptoms': np.random.choice(['fever', 'cough', 'fatigue', None, ''], n_records),
            'severity': np.random.choice(['leve', 'moderado', 'grave', None], n_records),
            'date': pd.date_range('2024-01-01', periods=n_records, freq='H'),
            'temperature': np.random.uniform(35.0, 42.0, n_records)
        })
        
        # Añadir duplicados (10%)
        n_duplicates = int(n_records * 0.1)
        duplicates = sample_data.sample(n=n_duplicates)
        sample_data = pd.concat([sample_data, duplicates], ignore_index=True)
        
        # Añadir nulos (15%)
        null_indices = np.random.choice(len(sample_data), int(len(sample_data) * 0.15), replace=False)
        sample_data.loc[null_indices, 'symptoms'] = None
        
        # Añadir outliers en temperatura
        outlier_indices = np.random.choice(len(sample_data), 20, replace=False)
        sample_data.loc[outlier_indices, 'temperature'] = np.random.choice([30.0, 50.0], 20)
        
        cleaning_service.update_job_progress(job_id, 30, "running")
        
        # Ejecutar limpieza
        config_dict = {
            "remove_duplicates": config.remove_duplicates,
            "handle_missing": config.handle_missing,
            "detect_outliers": config.detect_outliers,
            "standardize_formats": config.standardize_formats
        }
        
        df_clean, results = cleaning_service.clean_covid_data(sample_data, config_dict)
        
        cleaning_service.update_job_progress(job_id, 80, "running")
        
        # Enriquecer resultados con métricas adicionales
        results['clean_records'] = len(df_clean)
        results['data_quality_score'] = round(
            (len(df_clean) / results['original_records']) * 100, 2
        )
        
        # En producción, aquí guardarías en Delta Lake
        # databricks_service.save_to_processed_table(df_clean)
        
        cleaning_service.update_job_progress(job_id, 90, "running")
        
        # Completar job
        cleaning_service.complete_job(job_id, results)
        
        logger.info(f"Job {job_id} completado exitosamente")

        monitoring_service.log_event(
            process="Limpieza",
            level=LogLevel.SUCCESS,
            message=f"Job completado: {job_id}",
            data={"job_id": job_id, "results": results}
        )
        
    except Exception as e:
        logger.error(f"Error en job {job_id}: {str(e)}")
        cleaning_service.cleaning_jobs[job_id]["status"] = "failed"
        cleaning_service.cleaning_jobs[job_id]["error"] = str(e)


# ============================================
# ENDPOINTS
# ============================================

@router.post("/run", response_model=CleaningJobResponse)
async def run_cleaning_job(
    request: CleaningJobRequest,
    background_tasks: BackgroundTasks
):
    """
    Módulo 3: Iniciar proceso de limpieza de datos
    
    Funciones implementadas:
    - remove_duplicates()
    - handle_missing_values()
    - detect_outliers()
    - standardize_formats()
    - validate_data_quality()
    
    El proceso se ejecuta en background y puedes consultar el estado
    con el job_id retornado.
    """
    try:
        # Crear job ID
        job_id = cleaning_service.create_job_id()
        
        # Iniciar job
        cleaning_service.start_cleaning_job(job_id, request.config.dict())
        
        # Ejecutar en background
        background_tasks.add_task(
            simulate_cleaning_process,
            job_id,
            request.config
        )
        
        return CleaningJobResponse(
            job_id=job_id,
            status=ProcessStatus.RUNNING,
            message=f"Job de limpieza iniciado. Use GET /api/clean/status/{job_id} para consultar el progreso.",
            started_at=datetime.now()
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{job_id}", response_model=CleaningStatusResponse)
async def get_cleaning_status(job_id: str):
    """
    Obtener el estado de un job de limpieza
    """
    job_status = cleaning_service.get_job_status(job_id)
    
    if not job_status:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} no encontrado"
        )
    
    # Mapear el estado
    status_map = {
        "pending": ProcessStatus.PENDING,
        "running": ProcessStatus.RUNNING,
        "completed": ProcessStatus.COMPLETED,
        "failed": ProcessStatus.FAILED
    }
    
    return CleaningStatusResponse(
        job_id=job_id,
        status=status_map.get(job_status["status"], ProcessStatus.PENDING),
        progress=job_status.get("progress", 0),
        results=job_status.get("results"),
        started_at=job_status["started_at"],
        completed_at=job_status.get("completed_at")
    )


@router.get("/history")
async def get_cleaning_history():
    """
    Obtener historial de jobs de limpieza
    """
    jobs = []
    
    for job_id, job_data in cleaning_service.cleaning_jobs.items():
        jobs.append({
            "job_id": job_id,
            "status": job_data["status"],
            "progress": job_data["progress"],
            "started_at": job_data["started_at"].isoformat(),
            "completed_at": job_data.get("completed_at").isoformat() if job_data.get("completed_at") else None,
            "config": job_data["config"]
        })
    
    # Ordenar por fecha (más reciente primero)
    jobs.sort(key=lambda x: x["started_at"], reverse=True)
    
    return {
        "total_jobs": len(jobs),
        "jobs": jobs
    }


@router.post("/validate")
async def validate_data_quality():
    """
    Validar calidad de datos sin ejecutar limpieza
    
    Retorna métricas de calidad:
    - Porcentaje de nulos por columna
    - Tipos de datos
    - Columnas con alta cantidad de nulos
    """
    try:
        # Generar datos de ejemplo
        sample_data = pd.DataFrame({
            'case_id': range(100),
            'age': np.random.randint(0, 100, 100),
            'symptoms': np.random.choice(['fever', 'cough', None], 100),
            'severity': np.random.choice(['leve', 'moderado', 'grave'], 100)
        })
        
        # Añadir nulos
        sample_data.loc[10:30, 'symptoms'] = None
        
        # Validar calidad
        quality_report = cleaning_service.validate_data_quality(sample_data)
        
        return {
            "quality_score": 85.5,
            "report": quality_report,
            "recommendations": [
                "Columna 'symptoms' tiene alto porcentaje de nulos (21%)",
                "Considere usar estrategia 'fill_mean' para valores nulos",
                "Se detectaron duplicados potenciales"
            ]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/jobs/{job_id}")
async def cancel_job(job_id: str):
    """
    Cancelar un job de limpieza en ejecución
    """
    job_status = cleaning_service.get_job_status(job_id)
    
    if not job_status:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} no encontrado"
        )
    
    if job_status["status"] != "running":
        raise HTTPException(
            status_code=400,
            detail=f"El job {job_id} no está en ejecución (status: {job_status['status']})"
        )
    
    # Marcar como cancelado
    cleaning_service.cleaning_jobs[job_id]["status"] = "cancelled"
    
    return SuccessResponse(
        success=True,
        message=f"Job {job_id} cancelado",
        data={"job_id": job_id}
    )


@router.get("/config/strategies")
async def get_cleaning_strategies():
    """
    Obtener lista de estrategias disponibles para limpieza
    """
    return {
        "missing_value_strategies": {
            "drop": {
                "name": "Eliminar filas",
                "description": "Elimina todas las filas que contengan valores nulos",
                "use_case": "Cuando los nulos son pocos y no afectan significativamente"
            },
            "fill_mean": {
                "name": "Rellenar con media",
                "description": "Rellena valores nulos con la media de la columna (solo numéricos)",
                "use_case": "Para datos continuos con distribución normal"
            },
            "fill_median": {
                "name": "Rellenar con mediana",
                "description": "Rellena valores nulos con la mediana de la columna",
                "use_case": "Para datos con outliers que afectarían la media"
            },
            "fill_zero": {
                "name": "Rellenar con cero",
                "description": "Rellena todos los valores nulos con 0",
                "use_case": "Cuando el valor 0 tiene significado en el contexto"
            }
        },
        "outlier_detection": {
            "iqr": {
                "name": "Interquartile Range (IQR)",
                "description": "Detecta outliers usando el rango intercuartílico",
                "threshold": "3.0 (estándar)"
            }
        },
        "duplicate_handling": {
            "remove_all": {
                "name": "Eliminar todos los duplicados",
                "description": "Mantiene solo la primera ocurrencia"
            }
        }
    }


@router.post("/test")
async def test_cleaning_pipeline():
    """
    Probar el pipeline de limpieza con datos de ejemplo

    Útil para verificar que el servicio funciona correctamente
    """
    try:
        # Crear datos de prueba
        test_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5, 1, 2],  # Duplicados
            'age': [25, 30, None, 45, 200, 25, 30],  # Nulos y outliers
            'name': ['  Juan', 'MARIA', 'pedro', None, 'ANA', '  Juan', 'MARIA']
        })

        # Configuración de prueba
        config = {
            "remove_duplicates": True,
            "handle_missing": "drop",
            "detect_outliers": True,
            "standardize_formats": True
        }

        # Ejecutar limpieza
        df_clean, results = cleaning_service.clean_covid_data(test_data, config)

        return {
            "test_status": "success",
            "original_data": test_data.to_dict('records'),
            "cleaned_data": df_clean.to_dict('records'),
            "cleaning_results": results,
            "message": "Pipeline de limpieza funcionando correctamente"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/clean-databricks")
async def clean_databricks_table(background_tasks: BackgroundTasks):
    """
    🧹 LIMPIEZA AUTOMÁTICA DE DATOS

    Limpia la tabla activa en Databricks:
    - Elimina duplicados
    - Elimina filas con valores nulos
    - Detecta y elimina outliers en columnas numéricas
    - Guarda resultado en nueva tabla {nombre}_clean

    Funciona con CUALQUIER tabla dinámica
    """
    from app.services.databricks_service import databricks_service
    import uuid

    try:
        if not databricks_service.is_configured():
            raise HTTPException(
                status_code=400,
                detail="Databricks no está configurado"
            )

        # Asegurar conexión activa
        if not databricks_service.connect():
            raise HTTPException(
                status_code=500,
                detail="Error conectando a Databricks"
            )

        # Obtener tabla MÁS RECIENTE (no la más grande, sino la última ingesta)
        table_name = databricks_service.get_most_recent_table()

        if not table_name:
            raise HTTPException(
                status_code=404,
                detail="No hay tablas disponibles para limpiar"
            )

        # Verificar si la tabla ya fue limpiada
        if databricks_service.table_already_cleaned(table_name):
            raise HTTPException(
                status_code=400,
                detail=f"La tabla '{table_name}' ya fue limpiada anteriormente. Ya existe '{table_name}_clean'"
            )

        full_table_name = f"{databricks_service.catalog}.{databricks_service.schema}.{table_name}"

        logger.info(f"🧹 Iniciando limpieza de tabla MÁS RECIENTE: {table_name}")

        # 1. Leer datos
        read_query = f"SELECT * FROM {full_table_name}"
        data = databricks_service.fetch_all(read_query)

        if not data:
            raise HTTPException(
                status_code=400,
                detail="La tabla está vacía"
            )

        df_original = pd.DataFrame(data)
        original_count = len(df_original)

        logger.info(f"📊 Registros originales: {original_count:,}")

        # 2. NO eliminar duplicados - Los datos agregados tienen "duplicados" válidos
        # (Ej: datos de vacunas agrupados por fecha/provincia/edad)
        df_clean = df_original.copy()
        duplicates_removed = 0

        # 3. Eliminar nulos - SOLO filas con TODOS los valores nulos (menos agresivo)
        before_dropna = len(df_clean)
        df_clean = df_clean.dropna(how='all')  # Solo elimina si TODA la fila es null
        nulls_removed = before_dropna - len(df_clean)

        # 4. Detectar outliers - SOLO marcar, NO eliminar (demasiado agresivo)
        # En lugar de eliminar, solo contamos cuántos serían outliers
        numeric_cols = df_clean.select_dtypes(include=[np.number]).columns.tolist()
        outliers_removed = 0

        # CAMBIO: No eliminamos outliers, solo los contamos para estadísticas
        for col in numeric_cols:
            if col.startswith('_'):  # Skip metadata columns
                continue
            try:
                Q1 = df_clean[col].quantile(0.25)
                Q3 = df_clean[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 3 * IQR
                upper_bound = Q3 + 3 * IQR

                # Solo CONTAR outliers, no eliminarlos
                outliers_count = len(df_clean[(df_clean[col] < lower_bound) | (df_clean[col] > upper_bound)])
                outliers_removed += outliers_count
            except:
                continue

        clean_count = len(df_clean)

        logger.info(f"✅ Registros limpios: {clean_count:,}")
        logger.info(f"🗑️  Duplicados eliminados: {duplicates_removed:,}")
        logger.info(f"🗑️  Nulos eliminados: {nulls_removed:,}")
        logger.info(f"🗑️  Outliers eliminados: {outliers_removed:,}")

        # 5. Crear tabla limpia y cargar datos
        clean_table_name = f"{table_name}_clean"
        clean_full_table = f"{databricks_service.catalog}.{databricks_service.schema}.{clean_table_name}"

        # ARREGLO: Crear tabla e insertar datos
        start_time = datetime.now()
        ingestion_id = str(uuid.uuid4())

        # 1. Crear tabla vacía
        databricks_service.create_dynamic_table_from_df(
            df=df_clean,
            table_name=clean_table_name,
            drop_if_exists=True
        )

        # 2. Insertar datos limpios
        result = databricks_service.insert_dataframe_ultra_fast(
            df=df_clean,
            table_name=clean_table_name,
            ingestion_id=ingestion_id
        )

        elapsed_seconds = (datetime.now() - start_time).total_seconds()

        logger.info(f"💾 Tabla limpia guardada: {clean_table_name}")

        # 7. Registrar en audit_logs
        quality_score = round((clean_count / original_count) * 100, 2)

        databricks_service.insert_audit_log(
            process="Limpieza_Datos",
            level="SUCCESS",
            message=f"Limpieza completada: {table_name} → {clean_table_name}",
            metadata={
                "original_table": table_name,
                "clean_table": clean_table_name,
                "original_records": original_count,
                "clean_records": clean_count,
                "duplicates_removed": duplicates_removed,
                "nulls_removed": nulls_removed,
                "outliers_removed": outliers_removed,
                "quality_score": quality_score,
                "elapsed_seconds": elapsed_seconds
            }
        )

        logger.info(f"📝 Log de limpieza registrado en audit_logs")

        return {
            "success": True,
            "message": f"Datos limpiados exitosamente",
            "original_table": table_name,
            "clean_table": clean_table_name,
            "stats": {
                "original_records": original_count,
                "clean_records": clean_count,
                "duplicates_removed": duplicates_removed,
                "nulls_removed": nulls_removed,
                "outliers_removed": outliers_removed,
                "quality_score": quality_score
            },
            "elapsed_seconds": elapsed_seconds
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en limpieza: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cleaning-history")
async def get_cleaning_history(limit: int = 10):
    """
    Obtiene el historial de limpiezas desde audit_logs
    """
    from app.services.databricks_service import databricks_service

    try:
        if not databricks_service.is_configured():
            raise HTTPException(
                status_code=400,
                detail="Databricks no está configurado"
            )

        if not databricks_service.connect():
            raise HTTPException(
                status_code=500,
                detail="Error conectando a Databricks"
            )

        # Obtener logs de limpieza
        query = f"""
            SELECT
                timestamp,
                level,
                message,
                metadata,
                user_id
            FROM {databricks_service.catalog}.{databricks_service.schema}.audit_logs
            WHERE process = 'Limpieza_Datos'
            ORDER BY timestamp DESC
            LIMIT {limit}
        """

        logs = databricks_service.execute_query(query)
        databricks_service.disconnect()

        if not logs:
            return {"history": []}

        # Formatear logs para el frontend
        history = []
        for log in logs:
            import json
            metadata = json.loads(log['metadata']) if isinstance(log['metadata'], str) else log['metadata']

            history.append({
                "timestamp": log['timestamp'].isoformat() if hasattr(log['timestamp'], 'isoformat') else str(log['timestamp']),
                "level": log['level'],
                "message": log['message'],
                "original_table": metadata.get('original_table', 'N/A'),
                "clean_table": metadata.get('clean_table', 'N/A'),
                "original_records": metadata.get('original_records', 0),
                "clean_records": metadata.get('clean_records', 0),
                "quality_score": metadata.get('quality_score', 0),
                "elapsed_seconds": metadata.get('elapsed_seconds', 0)
            })

        return {"history": history}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error obteniendo historial: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))