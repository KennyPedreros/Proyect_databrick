from fastapi import APIRouter, HTTPException
from app.models.schemas import SuccessResponse
from app.services.databricks_service import databricks_service
from datetime import datetime, timedelta
import logging

router = APIRouter(prefix="/api/monitoring", tags=["Módulo 6: Monitoreo"])
logger = logging.getLogger(__name__)

# Simulación de logs en memoria (en producción vendría de Delta Lake)
LOGS_DB = []
PROCESSES_DB = {
    "ingestion": {
        "name": "Ingesta de Datos",
        "description": "Carga de archivos CSV",
        "status": "completed",
        "last_run": datetime.now().strftime("%H:%M %p"),
        "duration": "2m 15s",
        "progress": 100
    },
    "cleaning": {
        "name": "Limpieza de Datos",
        "description": "Procesamiento Spark",
        "status": "running",
        "last_run": datetime.now().strftime("%H:%M %p"),
        "duration": "5m 30s",
        "progress": 65
    },
    "classification": {
        "name": "Clasificación ML",
        "description": "Auto-etiquetado",
        "status": "pending",
        "last_run": (datetime.now() - timedelta(hours=1)).strftime("%H:%M %p"),
        "duration": None,
        "progress": 0
    },
    "dashboard": {
        "name": "Generación Dashboard",
        "description": "Agregaciones",
        "status": "completed",
        "last_run": datetime.now().strftime("%H:%M %p"),
        "duration": "1m 45s",
        "progress": 100
    },
    "backup": {
        "name": "Backup Delta Lake",
        "description": "Respaldo de datos",
        "status": "failed",
        "last_run": (datetime.now() - timedelta(hours=3)).strftime("%H:%M %p"),
        "duration": "30s",
        "progress": 100
    },
    "audit": {
        "name": "Auditoría",
        "description": "Logs del sistema",
        "status": "completed",
        "last_run": datetime.now().strftime("%H:%M %p"),
        "duration": "45s",
        "progress": 100
    }
}

def add_log(process: str, level: str, message: str):
    """Agrega un log al sistema"""
    log = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "process": process,
        "level": level,
        "message": message
    }
    LOGS_DB.insert(0, log)  # Más reciente primero
    
    # Mantener solo los últimos 1000 logs
    if len(LOGS_DB) > 1000:
        LOGS_DB.pop()
    
    logger.info(f"[{level}] {process}: {message}")


@router.get("/processes")
async def get_process_status():
    """
    Módulo 6: Obtener estado de todos los procesos
    
    Retorna el estado actual de cada proceso del sistema:
    - Ingesta, Limpieza, Clasificación, Dashboard, Backup, Auditoría
    """
    try:
        processes = list(PROCESSES_DB.values())
        
        return {
            "total_processes": len(processes),
            "processes": processes,
            "total_successful": sum(1 for p in processes if p["status"] == "completed"),
            "total_running": sum(1 for p in processes if p["status"] == "running"),
            "total_failed": sum(1 for p in processes if p["status"] == "failed"),
            "total_pending": sum(1 for p in processes if p["status"] == "pending")
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo estado de procesos: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs")
async def get_system_logs(limit: int = 50, level: str = None):
    """
    Obtener logs del sistema
    
    Params:
    - limit: Cantidad de logs a retornar (default: 50)
    - level: Filtrar por nivel (INFO, WARNING, ERROR, SUCCESS)
    """
    try:
        logs = LOGS_DB.copy()
        
        # Filtrar por nivel si se especifica
        if level:
            logs = [log for log in logs if log["level"] == level.upper()]
        
        # Limitar cantidad
        logs = logs[:limit]
        
        return {
            "total": len(logs),
            "logs": logs
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo logs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts")
async def get_system_alerts():
    """
    Obtener alertas activas del sistema
    """
    try:
        alerts = []
        
        # Revisar procesos fallidos
        for process_id, process in PROCESSES_DB.items():
            if process["status"] == "failed":
                alerts.append({
                    "type": "error",
                    "process": process["name"],
                    "message": f"El proceso '{process['name']}' ha fallado",
                    "timestamp": process["last_run"]
                })
        
        # Revisar procesos con progreso detenido
        for process_id, process in PROCESSES_DB.items():
            if process["status"] == "running" and process["progress"] < 50:
                alerts.append({
                    "type": "warning",
                    "process": process["name"],
                    "message": f"El proceso '{process['name']}' tiene progreso bajo ({process['progress']}%)",
                    "timestamp": process["last_run"]
                })
        
        return {
            "total_alerts": len(alerts),
            "alerts": alerts
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo alertas: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/audit-report")
async def generate_audit_report(start_date: str = None, end_date: str = None):
    """
    Generar reporte de auditoría
    
    Params:
    - start_date: Fecha inicial (YYYY-MM-DD)
    - end_date: Fecha final (YYYY-MM-DD)
    """
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        # Query para auditoría (ejemplo)
        query = f"""
        SELECT 
            'audit' as report_type,
            COUNT(*) as total_operations,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        """
        
        results = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        add_log("Auditoría", "SUCCESS", "Reporte de auditoría generado exitosamente")
        
        return {
            "report_id": f"AUDIT-{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "generated_at": datetime.now().isoformat(),
            "period": {
                "start": start_date or "2024-01-01",
                "end": end_date or datetime.now().strftime("%Y-%m-%d")
            },
            "summary": results[0] if results else {},
            "report_url": "/api/monitoring/audit-report/download"
        }
        
    except Exception as e:
        logger.error(f"Error generando reporte: {str(e)}")
        add_log("Auditoría", "ERROR", f"Error generando reporte: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def get_system_health():
    """
    Verificar salud del sistema
    """
    try:
        health = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "components": {}
        }
        
        # Verificar Databricks
        try:
            db_connected = databricks_service.connect()
            health["components"]["databricks"] = {
                "status": "up" if db_connected else "down",
                "message": "Databricks conectado" if db_connected else "Error de conexión"
            }
            databricks_service.disconnect()
        except:
            health["components"]["databricks"] = {
                "status": "down",
                "message": "Error de conexión"
            }
        
        # Verificar procesos
        running = sum(1 for p in PROCESSES_DB.values() if p["status"] == "running")
        failed = sum(1 for p in PROCESSES_DB.values() if p["status"] == "failed")
        
        health["components"]["processes"] = {
            "status": "warning" if failed > 0 else "up",
            "running": running,
            "failed": failed
        }
        
        # Estado general
        if failed > 2 or not health["components"]["databricks"]["status"] == "up":
            health["status"] = "unhealthy"
        elif failed > 0:
            health["status"] = "degraded"
        
        return health
        
    except Exception as e:
        logger.error(f"Error verificando salud: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/log")
async def add_manual_log(process: str, level: str, message: str):
    """
    Agregar log manual al sistema
    """
    try:
        add_log(process, level.upper(), message)
        return SuccessResponse(
            success=True,
            message="Log agregado exitosamente",
            data={
                "process": process,
                "level": level,
                "message": message
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/process/{process_id}/status")
async def update_process_status(process_id: str, status: str, progress: int = None):
    """
    Actualizar estado de un proceso
    """
    try:
        if process_id not in PROCESSES_DB:
            raise HTTPException(status_code=404, detail="Proceso no encontrado")
        
        PROCESSES_DB[process_id]["status"] = status
        PROCESSES_DB[process_id]["last_run"] = datetime.now().strftime("%H:%M %p")
        
        if progress is not None:
            PROCESSES_DB[process_id]["progress"] = progress
        
        add_log(
            PROCESSES_DB[process_id]["name"],
            "INFO",
            f"Estado actualizado a: {status}"
        )
        
        return SuccessResponse(
            success=True,
            message="Estado actualizado",
            data=PROCESSES_DB[process_id]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Inicializar algunos logs de ejemplo
add_log("Sistema", "INFO", "Sistema de monitoreo iniciado")
add_log("Ingesta", "SUCCESS", "Archivo cargado: covid_cases_2024.csv (15,234 registros)")
add_log("Limpieza", "INFO", "Job de limpieza iniciado para dataset_20241112")
add_log("Dashboard", "INFO", "KPIs actualizados exitosamente")
add_log("Backup", "ERROR", "Connection timeout to storage service")
add_log("Clasificación", "WARNING", "Model confidence below threshold (0.75)")