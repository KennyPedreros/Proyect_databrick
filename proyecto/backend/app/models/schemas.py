from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

# ============================================
# ENUMS
# ============================================

class ProcessStatus(str, Enum):
    """Estados posibles de un proceso"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class SeverityLevel(str, Enum):
    """Niveles de severidad COVID-19"""
    LEVE = "Leve"
    MODERADO = "Moderado"
    GRAVE = "Grave"
    CRITICO = "Crítico"

class LogLevel(str, Enum):
    """Niveles de log"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    SUCCESS = "SUCCESS"

# ============================================
# MÓDULO 1: INGESTA DE DATOS
# ============================================

class IngestionResponse(BaseModel):
    """Respuesta al cargar un archivo"""
    ingestion_id: str
    filename: str
    records_count: int
    status: str
    message: str
    timestamp: datetime = Field(default_factory=datetime.now)

class DataSourceInfo(BaseModel):
    """Información de fuente de datos"""
    source_id: str
    name: str
    type: str  # "csv", "api", "excel"
    last_updated: Optional[datetime] = None

# ============================================
# MÓDULO 2: ALMACENAMIENTO
# ============================================

class StorageStatus(BaseModel):
    """Estado del almacenamiento"""
    storage_id: str
    location: str  # "delta_lake", "raw", etc.
    size_mb: float
    integrity_check: bool
    backup_exists: bool

# ============================================
# MÓDULO 3: LIMPIEZA DE DATOS
# ============================================

class CleaningConfig(BaseModel):
    """Configuración para limpieza de datos"""
    remove_duplicates: bool = True
    handle_missing: str = "drop"  # "drop", "fill_mean", "fill_median", "fill_zero"
    detect_outliers: bool = True
    standardize_formats: bool = True

class CleaningJobRequest(BaseModel):
    """Request para iniciar job de limpieza"""
    config: CleaningConfig
    dataset_id: Optional[str] = None

class CleaningJobResponse(BaseModel):
    """Respuesta al iniciar job de limpieza"""
    job_id: str
    status: ProcessStatus
    message: str
    started_at: datetime = Field(default_factory=datetime.now)

class CleaningStatusResponse(BaseModel):
    """Estado de un job de limpieza"""
    job_id: str
    status: ProcessStatus
    progress: int  # 0-100
    results: Optional[Dict[str, Any]] = None
    started_at: datetime
    completed_at: Optional[datetime] = None

# ============================================
# MÓDULO 4: CLASIFICACIÓN
# ============================================

class ClassificationRequest(BaseModel):
    """Request para clasificación"""
    use_llm: bool = True
    batch_size: int = 100

class ClassificationResult(BaseModel):
    """Resultado de clasificación"""
    classification_id: str
    total_classified: int
    distribution: Dict[str, int]  # {"Leve": 100, "Moderado": 50, ...}
    samples: List[Dict[str, Any]]

class ModelMetrics(BaseModel):
    """Métricas del modelo de clasificación"""
    accuracy: float
    precision: float
    recall: float
    f1_score: float

# ============================================
# MÓDULO 5: DASHBOARD
# ============================================

class DashboardMetrics(BaseModel):
    """Métricas para el dashboard"""
    total_cases: int
    active_cases: int
    recovered: int
    deaths: int
    last_updated: datetime

class TimeSeriesData(BaseModel):
    """Datos de series temporales"""
    date: str
    cases: int
    deaths: int
    vaccinated: int

# ============================================
# MÓDULO 6: MONITOREO
# ============================================

class ProcessInfo(BaseModel):
    """Información de un proceso"""
    name: str
    description: str
    status: ProcessStatus
    last_run: str
    duration: Optional[str] = None
    progress: int = 0

class LogEntry(BaseModel):
    """Entrada de log"""
    timestamp: str
    process: str
    level: LogLevel
    message: str

class MonitoringStatus(BaseModel):
    """Estado del sistema"""
    processes: List[ProcessInfo]
    total_successful: int
    total_running: int
    total_failed: int
    total_pending: int

# ============================================
# RAG: CONSULTAS INTELIGENTES
# ============================================

class RAGQueryRequest(BaseModel):
    """Request para consulta RAG"""
    question: str = Field(..., min_length=5, max_length=500)

class RAGQueryResponse(BaseModel):
    """Respuesta de consulta RAG"""
    answer: str
    sources: List[str]
    confidence: float = 0.0
    query_id: str
    timestamp: datetime = Field(default_factory=datetime.now)

# ============================================
# RESPONSES GENERALES
# ============================================

class SuccessResponse(BaseModel):
    """Respuesta genérica exitosa"""
    success: bool = True
    message: str
    data: Optional[Dict[str, Any]] = None

class ErrorResponse(BaseModel):
    """Respuesta genérica de error"""
    success: bool = False
    error: str
    detail: Optional[str] = None