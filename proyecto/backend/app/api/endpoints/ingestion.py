from fastapi import APIRouter, UploadFile, File, HTTPException
from app.models.schemas import IngestionResponse, DataSourceInfo, SuccessResponse
from typing import List
import uuid
from datetime import datetime
import os

router = APIRouter(prefix="/api/ingest", tags=["Módulo 1: Ingesta de Datos"])

# Simulación de base de datos (temporal)
uploaded_files_db = []

# ============================================
# FUNCIONES DEL MÓDULO 1
# ============================================

def crear_id_ingesta() -> str:
    """Genera un ID único para la ingesta"""
    return f"ING-{uuid.uuid4().hex[:8].upper()}"

def validate_schema(filename: str, size: int) -> bool:
    """Valida el esquema del archivo"""
    # Validar extensión
    allowed_extensions = ['.csv', '.xlsx', '.xls', '.json']
    ext = os.path.splitext(filename)[1].lower()
    
    if ext not in allowed_extensions:
        return False
    
    # Validar tamaño (max 50MB)
    max_size = 50 * 1024 * 1024  # 50 MB
    if size > max_size:
        return False
    
    return True

def log_ingestion_status(ingestion_id: str, status: str):
    """Registra el estado de la ingesta en logs"""
    print(f"[INGESTION] {ingestion_id} - Status: {status} - {datetime.now()}")

# ============================================
# ENDPOINTS
# ============================================

@router.post("/upload", response_model=IngestionResponse)
async def upload_covid_data(file: UploadFile = File(...)):
    """
    Módulo 1: Carga de archivos COVID-19
    
    Funciones implementadas:
    - crear_id_ingesta()
    - load_data()
    - validate_schema()
    - log_ingestion_status()
    """
    try:
        # Crear ID único
        ingestion_id = crear_id_ingesta()
        
        # Validar archivo
        contents = await file.read()
        file_size = len(contents)
        
        if not validate_schema(file.filename, file_size):
            raise HTTPException(
                status_code=400,
                detail="Archivo inválido. Solo se permiten CSV, Excel o JSON (max 50MB)"
            )
        
        # Simular conteo de registros (en producción, esto leería el archivo)
        # Por ahora, estimamos basados en el tamaño
        estimated_records = file_size // 100  # Estimación simple
        
        # Guardar información en la "base de datos"
        file_info = {
            "ingestion_id": ingestion_id,
            "filename": file.filename,
            "size_bytes": file_size,
            "records_count": estimated_records,
            "uploaded_at": datetime.now()
        }
        uploaded_files_db.append(file_info)
        
        # Log del proceso
        log_ingestion_status(ingestion_id, "SUCCESS")
        
        return IngestionResponse(
            ingestion_id=ingestion_id,
            filename=file.filename,
            records_count=estimated_records,
            status="success",
            message=f"Archivo cargado exitosamente. {estimated_records} registros detectados."
        )
        
    except HTTPException as he:
        raise he
    except Exception as e:
        log_ingestion_status("ERROR", f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al procesar archivo: {str(e)}")

@router.get("/sources", response_model=List[DataSourceInfo])
def get_data_sources():
    """
    Listar fuentes de datos disponibles
    """
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
    """
    Obtener estado de una ingesta específica
    """
    # Buscar en la "base de datos"
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
    """
    Obtener historial de ingestas
    """
    return {
        "total": len(uploaded_files_db),
        "uploads": uploaded_files_db
    }