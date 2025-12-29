from fastapi import APIRouter, HTTPException
from app.services.databricks_service import databricks_service
from pydantic import BaseModel
from typing import List, Optional
import logging
from datetime import datetime

router = APIRouter(prefix="/api/classify", tags=["Módulo 4: Clasificación"])
logger = logging.getLogger(__name__)


class AnalyzeRequest(BaseModel):
    table_name: Optional[str] = None  # Si no se provee, usa tabla más reciente


class ClassificationConfig(BaseModel):
    column: str
    new_column: str
    type: str  # "numeric_ranges", "year", "month", "quarter", "direct"
    ranges: Optional[List[dict]] = None  # Solo para numeric_ranges


class ExecuteClassificationRequest(BaseModel):
    table_name: str
    classifications: List[ClassificationConfig]


@router.post("/analyze")
async def analyze_for_classification(request: AnalyzeRequest):
    """
    Analiza una tabla y sugiere clasificaciones automáticas
    """
    try:
        if not databricks_service.is_configured():
            raise HTTPException(
                status_code=400,
                detail="Databricks no está configurado"
            )

        # Obtener tabla a analizar
        if request.table_name:
            table_name = request.table_name
        else:
            # Usar tabla más reciente (limpia si existe, sino original)
            if not databricks_service.connect():
                raise HTTPException(
                    status_code=500,
                    detail="Error conectando a Databricks"
                )

            most_recent = databricks_service.get_most_recent_table()
            if not most_recent:
                raise HTTPException(
                    status_code=404,
                    detail="No hay tablas disponibles para clasificar"
                )

            # ARREGLO: Priorizar tabla limpia, pero NUNCA una tabla _classified
            if databricks_service.table_already_cleaned(most_recent):
                table_name = f"{most_recent}_clean"
            else:
                table_name = most_recent

            # Si la tabla ya tiene _classified, usar la versión sin clasificar
            if table_name.endswith('_classified'):
                table_name = table_name.replace('_classified', '')

        logger.info(f"🔍 Analizando tabla para clasificación: {table_name}")

        # Obtener esquema
        schema = databricks_service.get_table_schema(table_name)
        if not schema:
            raise HTTPException(
                status_code=404,
                detail=f"No se pudo obtener esquema de la tabla {table_name}"
            )

        # Analizar cada columna
        classifiable_columns = []
        non_classifiable_columns = []

        # ARREGLO: schema es un dict con key 'columns', no una lista directa
        columns_list = schema.get('columns', []) if isinstance(schema, dict) else schema

        for col in columns_list:
            col_name = col['name']
            col_type = col['type']

            # Saltar columnas de metadatos
            if col_name.startswith('_') or col_name in ['ingestion_id', 'created_at']:
                continue

            analysis = databricks_service.analyze_column_for_classification(
                table_name=table_name,
                column_name=col_name,
                column_type=col_type
            )

            if analysis and analysis['is_classifiable']:
                classifiable_columns.append(analysis)
            else:
                non_classifiable_columns.append({
                    "column_name": col_name,
                    "column_type": col_type,
                    "reason": "No clasificable (texto libre o ID único)"
                })

        logger.info(f"✅ Análisis completado: {len(classifiable_columns)} columnas clasificables encontradas")

        total_cols = len(columns_list)

        return {
            "table_name": table_name,
            "total_columns": total_cols,
            "classifiable_columns": classifiable_columns,
            "non_classifiable_columns": non_classifiable_columns,
            "total_classifiable": len(classifiable_columns)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en análisis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/execute")
async def execute_classification(request: ExecuteClassificationRequest):
    """
    Ejecuta clasificaciones seleccionadas y crea tabla _classified
    """
    try:
        if not databricks_service.is_configured():
            raise HTTPException(
                status_code=400,
                detail="Databricks no está configurado"
            )

        if not request.classifications or len(request.classifications) == 0:
            raise HTTPException(
                status_code=400,
                detail="Debes seleccionar al menos una clasificación"
            )

        logger.info(f"🚀 Ejecutando {len(request.classifications)} clasificaciones en {request.table_name}")

        # Verificar si ya existe tabla classified
        classified_table = f"{request.table_name}_classified"
        if databricks_service.connect():
            check_query = f"SHOW TABLES IN {databricks_service.catalog}.{databricks_service.schema} LIKE '{classified_table}'"
            existing = databricks_service.execute_query(check_query)
            if existing and len(existing) > 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"La tabla '{classified_table}' ya existe. Elimínala primero si deseas reclasificar."
                )

        start_time = datetime.now()

        # Convertir request a formato esperado por el servicio
        classifications_data = []
        for c in request.classifications:
            class_dict = {
                "column": c.column,
                "new_column": c.new_column,
                "type": c.type
            }
            if c.ranges:
                class_dict["ranges"] = c.ranges
            classifications_data.append(class_dict)

        # Ejecutar clasificación
        result = databricks_service.execute_classification(
            source_table=request.table_name,
            classifications=classifications_data
        )

        elapsed_seconds = (datetime.now() - start_time).total_seconds()

        # Registrar en audit_logs
        databricks_service.insert_audit_log(
            process="Clasificación_ML",
            level="SUCCESS",
            message=f"Clasificación completada: {request.table_name} → {classified_table}",
            metadata={
                "source_table": request.table_name,
                "classified_table": classified_table,
                "total_records": result['total_records'],
                "classifications_applied": result['classifications_applied'],
                "elapsed_seconds": elapsed_seconds
            }
        )

        logger.info(f"✅ Clasificación completada en {elapsed_seconds:.2f}s")

        return {
            "success": True,
            "message": "Clasificación ejecutada exitosamente",
            "source_table": request.table_name,
            "classified_table": classified_table,
            "total_records": result['total_records'],
            "classifications_applied": result['classifications_applied'],
            "elapsed_seconds": elapsed_seconds
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error ejecutando clasificación: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/classification-history")
async def get_classification_history(limit: int = 10):
    """
    Obtiene el historial de clasificaciones desde audit_logs
    """
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

        # Obtener logs de clasificación
        query = f"""
            SELECT
                timestamp,
                level,
                message,
                metadata
            FROM {databricks_service.catalog}.{databricks_service.schema}.audit_logs
            WHERE process = 'Clasificación_ML'
            ORDER BY timestamp DESC
            LIMIT {limit}
        """

        logs = databricks_service.execute_query(query)
        databricks_service.disconnect()

        if not logs:
            return {"history": []}

        # Formatear logs
        history = []
        for log in logs:
            import json
            metadata = json.loads(log['metadata']) if isinstance(log['metadata'], str) else log['metadata']

            history.append({
                "timestamp": log['timestamp'].isoformat() if hasattr(log['timestamp'], 'isoformat') else str(log['timestamp']),
                "level": log['level'],
                "message": log['message'],
                "source_table": metadata.get('source_table', 'N/A'),
                "classified_table": metadata.get('classified_table', 'N/A'),
                "total_records": metadata.get('total_records', 0),
                "classifications_applied": metadata.get('classifications_applied', 0),
                "elapsed_seconds": metadata.get('elapsed_seconds', 0)
            })

        return {"history": history}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error obteniendo historial: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
