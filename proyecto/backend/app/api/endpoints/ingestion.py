from fastapi import APIRouter, UploadFile, File, HTTPException
from app.services.monitoring_service import monitoring_service, LogLevel
from app.services.databricks_service import databricks_service  # ✅ AGREGADO
from app.models.schemas import IngestionResponse, DataSourceInfo, SuccessResponse
from typing import List
import uuid
from datetime import datetime
import os
import pandas as pd
import io
import logging
import chardet  # ✅ Para detectar encoding automáticamente

router = APIRouter(prefix="/api/ingest", tags=["Módulo 1: Ingesta de Datos"])
logger = logging.getLogger(__name__)

# Simulación de base de datos (temporal)
uploaded_files_db = []

def crear_id_ingesta() -> str:
    """Genera un ID único para la ingesta"""
    return f"ING-{uuid.uuid4().hex[:8].upper()}"

def validate_schema(filename: str, size: int) -> tuple[bool, str]:
    """Valida el esquema del archivo"""
    # Validar extensión
    allowed_extensions = ['.csv', '.xlsx', '.xls', '.json']
    ext = os.path.splitext(filename)[1].lower()
    
    if ext not in allowed_extensions:
        return False, f"Extensión no permitida: {ext}. Solo se permiten: {', '.join(allowed_extensions)}"
    
    # Validar tamaño (max 500MB)
    max_size = 500 * 1024 * 1024  # 500 MB
    if size > max_size:
        size_mb = size / (1024 * 1024)
        return False, f"Archivo demasiado grande: {size_mb:.2f}MB. Máximo permitido: 500MB"
    
    if size == 0:
        return False, "El archivo está vacío"
    
    return True, "OK"

def log_ingestion_status(ingestion_id: str, status: str):
    """Registra el estado de la ingesta en logs"""
    logger.info(f"[INGESTION] {ingestion_id} - Status: {status} - {datetime.now()}")

def detect_encoding(file_content: bytes) -> str:
    """Detecta el encoding del archivo automáticamente"""
    try:
        # Usar chardet para detectar encoding
        result = chardet.detect(file_content[:10000])  # Analizar primeros 10KB
        encoding = result['encoding']
        confidence = result['confidence']
        
        logger.info(f"Encoding detectado: {encoding} (confianza: {confidence:.2%})")
        
        # Si la confianza es baja, intentar con encodings comunes
        if confidence < 0.7:
            logger.warning(f"Baja confianza en encoding detectado, intentando alternativas...")
            return 'latin-1'  # Fallback común para archivos con ñ, á, é, etc.
        
        return encoding if encoding else 'latin-1'
    except Exception as e:
        logger.warning(f"Error detectando encoding: {str(e)}, usando latin-1 como fallback")
        return 'latin-1'

def count_records_from_file(file_content: bytes, filename: str) -> int:
    """Cuenta los registros reales del archivo con manejo de encoding"""
    try:
        ext = os.path.splitext(filename)[1].lower()
        
        if ext == '.csv':
            # Detectar encoding automáticamente
            encoding = detect_encoding(file_content)
            
            # Intentar con el encoding detectado
            try:
                df = pd.read_csv(io.BytesIO(file_content), encoding=encoding)
                logger.info(f"✅ CSV leído exitosamente con encoding: {encoding}")
            except UnicodeDecodeError:
                # Si falla, intentar con otros encodings comunes
                logger.warning(f"Fallo con {encoding}, intentando encodings alternativos...")
                encodings_to_try = ['latin-1', 'iso-8859-1', 'cp1252', 'utf-8']
                
                for enc in encodings_to_try:
                    try:
                        df = pd.read_csv(io.BytesIO(file_content), encoding=enc)
                        logger.info(f"✅ CSV leído exitosamente con encoding: {enc}")
                        break
                    except:
                        continue
                else:
                    raise Exception("No se pudo leer el archivo con ningún encoding conocido")
        
        elif ext in ['.xlsx', '.xls']:
            df = pd.read_excel(io.BytesIO(file_content))
            logger.info("✅ Excel leído exitosamente")
        
        elif ext == '.json':
            # JSON generalmente es UTF-8
            try:
                df = pd.read_json(io.BytesIO(file_content))
            except UnicodeDecodeError:
                # Intentar decodificar manualmente
                json_str = file_content.decode('latin-1')
                df = pd.read_json(io.StringIO(json_str))
            logger.info("✅ JSON leído exitosamente")
        
        else:
            return 0
        
        return len(df)
    
    except Exception as e:
        logger.error(f"Error counting records: {str(e)}")
        raise HTTPException(
            status_code=400,
            detail=f"No se pudo leer el archivo. Error: {str(e)}. Intenta con otro formato o verifica que el archivo no esté corrupto."
        )

@router.post("/upload", response_model=IngestionResponse)
async def upload_covid_data(file: UploadFile = File(...)):
    """
    Módulo 1: Carga de archivos COVID-19
    
    Límite: 500MB por archivo
    Formatos: CSV, Excel (.xlsx, .xls), JSON
    """
    try:
        # Crear ID único
        ingestion_id = crear_id_ingesta()
        
        logger.info(f"Recibiendo archivo: {file.filename}")
        
        # Leer archivo en chunks para archivos grandes
        contents = bytearray()
        chunk_size = 1024 * 1024  # 1MB chunks
        
        while chunk := await file.read(chunk_size):
            contents.extend(chunk)
        
        file_size = len(contents)
        
        logger.info(f"Tamaño del archivo: {file_size / 1024 / 1024:.2f}MB")
        
        # Validar archivo
        is_valid, validation_message = validate_schema(file.filename, file_size)
        if not is_valid:
            logger.error(f"Validación fallida: {validation_message}")
            raise HTTPException(status_code=400, detail=validation_message)
        
        # Contar registros reales
        logger.info("Contando registros...")
        records_count = count_records_from_file(bytes(contents), file.filename)
        
        if records_count == 0:
            raise HTTPException(
                status_code=400,
                detail="No se encontraron registros en el archivo."
            )
        
        logger.info(f"Registros encontrados: {records_count}")
        
        # AGREGAR: Guardar en Databricks (SOLO si está configurado)
        if databricks_service.connect():
            try:
                # Detectar encoding
                encoding = detect_encoding(bytes(contents))
                
                # Convertir a DataFrame
                if file.filename.endswith('.csv'):
                    df = pd.read_csv(io.BytesIO(contents), encoding=encoding)
                elif file.filename.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(io.BytesIO(contents))
                elif file.filename.endswith('.json'):
                    try:
                        df = pd.read_json(io.BytesIO(contents))
                    except:
                        json_str = contents.decode(encoding)
                        df = pd.read_json(io.StringIO(json_str))
                
                logger.info(f"DataFrame creado con {len(df)} filas y {len(df.columns)} columnas")
                logger.info(f"Columnas encontradas: {list(df.columns)}")
                
                # Insertar registros en Delta Lake (en lotes para mejor performance)
                batch_size = 1000
                total_inserted = 0
                
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i+batch_size]
                    
                    for _, row in batch.iterrows():
                        # Mapear columnas del archivo a las columnas de la tabla
                        # Ajustar según las columnas reales del archivo
                        try:
                            query = f"""
                            INSERT INTO {databricks_service.catalog}.{databricks_service.schema}.covid_processed
                            (case_id, date, country, region, age, gender, symptoms, severity, outcome, vaccinated, processed_at)
                            VALUES (
                                '{row.get('case_id', row.get('id', f'CASE-{uuid.uuid4().hex[:8].upper()}'))}',
                                '{row.get('date', row.get('fecha', datetime.now().strftime('%Y-%m-%d')))}',
                                '{row.get('country', row.get('pais', 'Ecuador'))}',
                                '{row.get('region', row.get('provincia', row.get('ciudad', 'Unknown')))}',
                                {row.get('age', row.get('edad', 0))},
                                '{row.get('gender', row.get('sexo', row.get('genero', 'Unknown')))}',
                                '{str(row.get('symptoms', row.get('sintomas', 'Unknown'))).replace("'", "''")}',
                                NULL,
                                '{row.get('outcome', row.get('estado', 'Activo'))}',
                                {1 if row.get('vaccinated', row.get('vacunado', False)) else 0},
                                current_timestamp()
                            )
                            """
                            databricks_service.execute_query(query)
                            total_inserted += 1
                        except Exception as e:
                            logger.warning(f"Error insertando fila: {str(e)[:100]}")
                            continue
                    
                    logger.info(f"Insertados {total_inserted}/{len(df)} registros...")
                
                databricks_service.disconnect()
                logger.info(f"✅ {total_inserted} registros guardados en Delta Lake")
                
            except Exception as e:
                logger.error(f"Error guardando en Delta Lake: {str(e)}")
                databricks_service.disconnect()
        else:
            logger.warning("⚠️ Databricks no está conectado. Datos no guardados en Delta Lake.")
        
        # Guardar información en la "base de datos"
        file_info = {
            "ingestion_id": ingestion_id,
            "filename": file.filename,
            "size_bytes": file_size,
            "records_count": records_count,
            "uploaded_at": datetime.now()
        }

        # Log de monitoreo
        monitoring_service.log_event(
            process="Ingesta",
            level=LogLevel.SUCCESS,
            message=f"Archivo cargado: {file.filename} ({records_count} registros)",
            data={
                "ingestion_id": ingestion_id,
                "filename": file.filename,
                "records": records_count,
                "size_mb": round(file_size / 1024 / 1024, 2)
            }
        )
        uploaded_files_db.append(file_info)
        
        # Log de auditoría
        log_ingestion_status(ingestion_id, "SUCCESS")
        
        return IngestionResponse(
            ingestion_id=ingestion_id,
            filename=file.filename,
            records_count=records_count,
            status="success",
            message=f"Archivo cargado exitosamente. {records_count:,} registros detectados."
        )
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}", exc_info=True)
        log_ingestion_status("ERROR", f"Error: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Error al procesar archivo: {str(e)}"
        )

@router.get("/sources", response_model=List[DataSourceInfo])
def get_data_sources():
    """Listar fuentes de datos disponibles"""
    sources = [
        DataSourceInfo(
            source_id="OMS-001",
            name="Organización Mundial de la Salud",
            type="api",
            last_updated=datetime.now()
        ),
        DataSourceInfo(
            source_id="JHU-001",
            name="Johns Hopkins University",
            type="api",
            last_updated=datetime.now()
        ),
        DataSourceInfo(
            source_id="OWID-001",
            name="Our World in Data",
            type="csv",
            last_updated=datetime.now()
        )
    ]
    return sources

@router.get("/status/{ingestion_id}")
def get_ingestion_status(ingestion_id: str):
    """Obtener estado de una ingesta específica"""
    for file_info in uploaded_files_db:
        if file_info["ingestion_id"] == ingestion_id:
            return {
                "ingestion_id": ingestion_id,
                "status": "completed",
                "file_info": file_info
            }
    
    raise HTTPException(status_code=404, detail="Ingesta no encontrada")

@router.get("/history")
def get_ingestion_history():
    """Obtener historial de ingestas"""
    return {
        "total": len(uploaded_files_db),
        "uploads": uploaded_files_db
    }