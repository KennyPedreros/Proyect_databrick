from fastapi import APIRouter, UploadFile, File, HTTPException
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
        
        # Leer contenido del archivo
        contents = await file.read()
        file_size = len(contents)
        
        logger.info(f"Tamaño del archivo: {file_size / 1024 / 1024:.2f}MB")
        
        # Validar archivo
        is_valid, validation_message = validate_schema(file.filename, file_size)
        if not is_valid:
            logger.error(f"Validación fallida: {validation_message}")
            raise HTTPException(status_code=400, detail=validation_message)
        
        # Contar registros reales
        logger.info("Contando registros...")
        records_count = count_records_from_file(contents, file.filename)
        
        if records_count == 0:
            raise HTTPException(
                status_code=400,
                detail="No se encontraron registros en el archivo. Verifica que el archivo contenga datos."
            )
        
        logger.info(f"Registros encontrados: {records_count}")
        
        # Guardar información en la "base de datos"
        file_info = {
            "ingestion_id": ingestion_id,
            "filename": file.filename,
            "size_bytes": file_size,
            "records_count": records_count,
            "uploaded_at": datetime.now()
        }
        uploaded_files_db.append(file_info)
        
        # Log del proceso
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