from fastapi import APIRouter, UploadFile, File, HTTPException
from app.services.monitoring_service import monitoring_service, LogLevel
from app.models.schemas import IngestionResponse, DataSourceInfo, SuccessResponse
from typing import List
import uuid
from datetime import datetime
import os
import pandas as pd
import io
import logging

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

def count_records_from_file(file_content: bytes, filename: str) -> int:
    """Cuenta los registros reales del archivo"""
    try:
        ext = os.path.splitext(filename)[1].lower()
        
        if ext == '.csv':
            df = pd.read_csv(io.BytesIO(file_content))
        elif ext in ['.xlsx', '.xls']:
            df = pd.read_excel(io.BytesIO(file_content))
        elif ext == '.json':
            df = pd.read_json(io.BytesIO(file_content))
        else:
            return 0
        
        return len(df)
    except Exception as e:
        logger.error(f"Error counting records: {str(e)}")
        raise HTTPException(
            status_code=400,
            detail=f"No se pudo leer el archivo. Asegúrate de que sea un archivo válido. Error: {str(e)}"
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
        
        # MODIFICAR: Leer archivo en chunks para archivos grandes
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
        
        # AGREGAR: Guardar en Databricks
        if databricks_service.connect():
            try:
                # Convertir a DataFrame
                import io
                if file.filename.endswith('.csv'):
                    df = pd.read_csv(io.BytesIO(contents))
                elif file.filename.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(io.BytesIO(contents))
                elif file.filename.endswith('.json'):
                    df = pd.read_json(io.BytesIO(contents))
                
                # Insertar registros en Delta Lake
                for _, row in df.iterrows():
                    query = f"""
                    INSERT INTO {databricks_service.catalog}.{databricks_service.schema}.covid_processed
                    (case_id, date, country, region, age, gender, symptoms, severity, outcome, vaccinated, processed_at)
                    VALUES (
                        '{row.get('case_id', f'CASE-{uuid.uuid4().hex[:8].upper()}')}',
                        '{row.get('date', datetime.now().strftime('%Y-%m-%d'))}',
                        '{row.get('country', 'Ecuador')}',
                        '{row.get('region', 'Unknown')}',
                        {row.get('age', 0)},
                        '{row.get('gender', 'Unknown')}',
                        '{row.get('symptoms', 'Unknown')}',
                        NULL,
                        '{row.get('outcome', 'Activo')}',
                        {1 if row.get('vaccinated', False) else 0},
                        current_timestamp()
                    )
                    """
                    databricks_service.execute_query(query)
                
                databricks_service.disconnect()
                logger.info(f"✅ Datos guardados en Delta Lake")
            except Exception as e:
                logger.error(f"Error guardando en Delta Lake: {str(e)}")
                databricks_service.disconnect()
        
        # Guardar información en la "base de datos"
        file_info = {
            "ingestion_id": ingestion_id,
            "filename": file.filename,
            "size_bytes": file_size,
            "records_count": records_count,
            "uploaded_at": datetime.now()
        }

        # DESPUÉS de guardar el archivo exitosamente:
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
        
        # AGREGAR: Log de auditoría
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