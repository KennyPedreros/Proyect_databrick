from fastapi import APIRouter, HTTPException
from app.models.schemas import SuccessResponse
from app.services.databricks_service import databricks_service
from datetime import datetime, timedelta
import logging

router = APIRouter(prefix="/api/monitoring", tags=["Módulo 6: Monitoreo"])
logger = logging.getLogger(__name__)


@router.get("/processes")
async def get_process_status():
    """
    Módulo 6: Obtener estado de todos los procesos desde Delta Lake
    """
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        # Obtener estadísticas de cada proceso desde los logs
        processes_queries = {
            "Ingesta": f"""
                SELECT 
                    'Ingesta de Datos' as name,
                    'Carga de archivos' as description,
                    CASE 
                        WHEN MAX(timestamp) > DATE_SUB(NOW(), INTERVAL 5 MINUTE) THEN 'completed'
                        ELSE 'pending'
                    END as status,
                    DATE_FORMAT(MAX(timestamp), '%H:%i %p') as last_run,
                    COUNT(*) as operations
                FROM {databricks_service.catalog}.{databricks_service.schema}.audit_logs
                WHERE process = 'Ingesta'
            """,
            "Limpieza": f"""
                SELECT 
                    'Limpieza de Datos' as name,
                    'Procesamiento' as description,
                    CASE 
                        WHEN MAX(timestamp) > DATE_SUB(NOW(), INTERVAL 5 MINUTE) THEN 'completed'
                        ELSE 'pending'
                    END as status,
                    DATE_FORMAT(MAX(timestamp), '%H:%i %p') as last_run,
                    COUNT(*) as operations
                FROM {databricks_service.catalog}.{databricks_service.schema}.audit_logs
                WHERE process = 'Limpieza'
            """,
            "Clasificación": f"""
                SELECT 
                    'Clasificación ML' as name,
                    'Auto-etiquetado' as description,
                    CASE 
                        WHEN MAX(timestamp) > DATE_SUB(NOW(), INTERVAL 5 MINUTE) THEN 'completed'
                        ELSE 'pending'
                    END as status,
                    DATE_FORMAT(MAX(timestamp), '%H:%i %p') as last_run,
                    COUNT(*) as operations
                FROM {databricks_service.catalog}.{databricks_service.schema}.audit_logs
                WHERE process = 'Clasificación'
            """
        }
        
        processes = []
        for process_name, query in processes_queries.items():
            try:
                result = databricks_service.execute_query(query)
                if result and len(result) > 0:
                    processes.append({
                        "name": result[0].get("name", process_name),
                        "description": result[0].get("description", ""),
                        "status": result[0].get("status", "pending"),
                        "last_run": result[0].get("last_run", "N/A"),
                        "duration": "N/A",
                        "progress": 100 if result[0].get("status") == "completed" else 0
                    })
            except Exception as e:
                logger.error(f"Error obteniendo estado de {process_name}: {str(e)}")
                processes.append({
                    "name": process_name,
                    "description": "Error al obtener estado",
                    "status": "failed",
                    "last_run": "N/A",
                    "duration": "N/A",
                    "progress": 0
                })
        
        databricks_service.disconnect()
        
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
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs")
async def get_system_logs(limit: int = 50, level: str = None):
    """
    Obtener logs reales del sistema desde Delta Lake
    """
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        where_clause = ""
        if level:
            where_clause = f"WHERE level = '{level.upper()}'"
        
        query = f"""
        SELECT 
            timestamp,
            process,
            level,
            message
        FROM {databricks_service.catalog}.{databricks_service.schema}.audit_logs
        {where_clause}
        ORDER BY timestamp DESC
        LIMIT {limit}
        """
        
        logs = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        # Formatear timestamps
        for log in logs:
            if 'timestamp' in log:
                log['timestamp'] = str(log['timestamp'])
        
        return {
            "total": len(logs),
            "logs": logs
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo logs: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts")
async def get_system_alerts():
    """
    Obtener alertas activas basadas en los logs
    """
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        # Buscar logs de error en las últimas 24 horas
        query = f"""
        SELECT 
            process,
            message,
            timestamp,
            level
        FROM {databricks_service.catalog}.{databricks_service.schema}.audit_logs
        WHERE level IN ('ERROR', 'WARNING')
        AND timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
        ORDER BY timestamp DESC
        LIMIT 20
        """
        
        results = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        alerts = []
        for log in results:
            alerts.append({
                "type": "error" if log["level"] == "ERROR" else "warning",
                "process": log["process"],
                "message": log["message"],
                "timestamp": str(log["timestamp"])
            })
        
        return {
            "total_alerts": len(alerts),
            "alerts": alerts
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo alertas: {str(e)}")
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
            if db_connected:
                # Verificar que podemos leer datos
                query = f"SELECT COUNT(*) as total FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed"
                result = databricks_service.execute_query(query)
                health["components"]["databricks"]["records"] = result[0]["total"] if result else 0
            databricks_service.disconnect()
        except Exception as e:
            health["components"]["databricks"] = {
                "status": "down",
                "message": f"Error: {str(e)}"
            }
            health["status"] = "unhealthy"
        
        # Verificar logs recientes
        try:
            if databricks_service.connect():
                query = f"""
                SELECT COUNT(*) as errors 
                FROM {databricks_service.catalog}.{databricks_service.schema}.audit_logs
                WHERE level = 'ERROR' 
                AND timestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
                """
                result = databricks_service.execute_query(query)
                error_count = result[0]["errors"] if result else 0
                
                health["components"]["logs"] = {
                    "status": "warning" if error_count > 0 else "up",
                    "recent_errors": error_count
                }
                
                if error_count > 5:
                    health["status"] = "degraded"
                
                databricks_service.disconnect()
        except:
            databricks_service.disconnect()
        
        return health
        
    except Exception as e:
        logger.error(f"Error verificando salud: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))