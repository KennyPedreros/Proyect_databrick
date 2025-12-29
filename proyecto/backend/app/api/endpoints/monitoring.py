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

        processes = []

        # ============================================
        # 1. INGESTA DE DATOS
        # ============================================
        try:
            # Obtener tabla más reciente
            most_recent_table = databricks_service.get_most_recent_table()

            if most_recent_table:
                # Obtener información de la última ingesta desde audit_logs
                ingesta_query = f"""
                    SELECT
                        timestamp,
                        metadata
                    FROM {databricks_service.catalog}.{databricks_service.schema}.audit_logs
                    WHERE process = 'Ingesta_UltraRápida'
                    AND metadata LIKE '%{most_recent_table}%'
                    ORDER BY timestamp DESC
                    LIMIT 1
                """
                ingesta_log = databricks_service.execute_query(ingesta_query)

                if ingesta_log and len(ingesta_log) > 0:
                    import json
                    metadata = json.loads(ingesta_log[0]['metadata']) if isinstance(ingesta_log[0]['metadata'], str) else ingesta_log[0]['metadata']
                    timestamp = ingesta_log[0]['timestamp']

                    # Formatear timestamp
                    if hasattr(timestamp, 'strftime'):
                        fecha = timestamp.strftime('%Y-%m-%d')
                        hora = timestamp.strftime('%H:%M')
                    else:
                        fecha = str(timestamp)[:10]
                        hora = str(timestamp)[11:16]

                    elapsed_seconds = metadata.get('elapsed_seconds', 0)

                    processes.append({
                        "name": "Ingesta de Datos",
                        "description": f"Archivo: {most_recent_table}",
                        "status": "completed",
                        "last_run": f"{fecha} {hora}",
                        "duration": f"{elapsed_seconds:.1f}s",
                        "progress": 100
                    })
                else:
                    # Hay tabla pero no hay log (raro, pero posible)
                    processes.append({
                        "name": "Ingesta de Datos",
                        "description": f"Archivo: {most_recent_table}",
                        "status": "completed",
                        "last_run": "N/A",
                        "duration": "N/A",
                        "progress": 100
                    })
            else:
                # No hay tablas ingresadas
                processes.append({
                    "name": "Ingesta de Datos",
                    "description": "Carga de archivos",
                    "status": "pending",
                    "last_run": "N/A",
                    "duration": "N/A",
                    "progress": 0
                })
        except Exception as e:
            logger.error(f"Error obteniendo estado de Ingesta: {str(e)}")
            processes.append({
                "name": "Ingesta de Datos",
                "description": "Error al obtener estado",
                "status": "failed",
                "last_run": "N/A",
                "duration": "N/A",
                "progress": 0
            })

        # ============================================
        # 2. LIMPIEZA DE DATOS
        # ============================================
        try:
            # Obtener tabla más reciente
            most_recent_table = databricks_service.get_most_recent_table()

            if most_recent_table:
                # Verificar si existe tabla _clean
                if databricks_service.table_already_cleaned(most_recent_table):
                    # Obtener info de la limpieza desde audit_logs
                    limpieza_query = f"""
                        SELECT
                            timestamp,
                            metadata
                        FROM {databricks_service.catalog}.{databricks_service.schema}.audit_logs
                        WHERE process = 'Limpieza_Datos'
                        AND metadata LIKE '%{most_recent_table}%'
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """
                    limpieza_log = databricks_service.execute_query(limpieza_query)

                    if limpieza_log and len(limpieza_log) > 0:
                        import json
                        metadata = json.loads(limpieza_log[0]['metadata']) if isinstance(limpieza_log[0]['metadata'], str) else limpieza_log[0]['metadata']
                        timestamp = limpieza_log[0]['timestamp']

                        if hasattr(timestamp, 'strftime'):
                            hora = timestamp.strftime('%H:%M')
                        else:
                            hora = str(timestamp)[11:16]

                        quality_score = metadata.get('quality_score', 0)
                        duration = metadata.get('elapsed_seconds', 0)

                        processes.append({
                            "name": "Limpieza de Datos",
                            "description": f"{most_recent_table}_clean (Calidad: {quality_score}%)",
                            "status": "completed",
                            "last_run": hora,
                            "duration": f"{duration:.1f}s" if duration else "N/A",
                            "progress": 100
                        })
                    else:
                        # Existe _clean pero no hay log
                        processes.append({
                            "name": "Limpieza de Datos",
                            "description": f"{most_recent_table}_clean",
                            "status": "completed",
                            "last_run": "N/A",
                            "duration": "N/A",
                            "progress": 100
                        })
                else:
                    # No existe tabla _clean, está pendiente
                    processes.append({
                        "name": "Limpieza de Datos",
                        "description": f"Pendiente para: {most_recent_table}",
                        "status": "pending",
                        "last_run": "N/A",
                        "duration": "N/A",
                        "progress": 0
                    })
            else:
                # No hay archivos ingresados
                processes.append({
                    "name": "Limpieza de Datos",
                    "description": "Procesamiento",
                    "status": "pending",
                    "last_run": "N/A",
                    "duration": "N/A",
                    "progress": 0
                })
        except Exception as e:
            logger.error(f"Error obteniendo estado de Limpieza: {str(e)}")
            processes.append({
                "name": "Limpieza de Datos",
                "description": "Error al obtener estado",
                "status": "failed",
                "last_run": "N/A",
                "duration": "N/A",
                "progress": 0
            })

        # ============================================
        # 3. CLASIFICACIÓN ML
        # ============================================
        try:
            most_recent_table = databricks_service.get_most_recent_table()

            if most_recent_table:
                # Buscar ambas posibilidades: tabla_classified o tabla_clean_classified
                classified_table = f"{most_recent_table}_classified"
                clean_classified_table = f"{most_recent_table}_clean_classified"

                # Verificar si existe alguna tabla clasificada
                check_query1 = f"SHOW TABLES IN {databricks_service.catalog}.{databricks_service.schema} LIKE '{classified_table}'"
                result1 = databricks_service.execute_query(check_query1)

                check_query2 = f"SHOW TABLES IN {databricks_service.catalog}.{databricks_service.schema} LIKE '{clean_classified_table}'"
                result2 = databricks_service.execute_query(check_query2)

                table_exists = (result1 and len(result1) > 0) or (result2 and len(result2) > 0)
                classified_name = clean_classified_table if (result2 and len(result2) > 0) else classified_table

                if table_exists:
                    # Obtener info de la clasificación desde audit_logs
                    clasificacion_query = f"""
                        SELECT
                            timestamp,
                            metadata
                        FROM {databricks_service.catalog}.{databricks_service.schema}.audit_logs
                        WHERE process = 'Clasificación_ML'
                        AND (metadata LIKE '%{classified_table}%' OR metadata LIKE '%{clean_classified_table}%')
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """
                    clasificacion_log = databricks_service.execute_query(clasificacion_query)

                    if clasificacion_log and len(clasificacion_log) > 0:
                        import json
                        metadata = json.loads(clasificacion_log[0]['metadata']) if isinstance(clasificacion_log[0]['metadata'], str) else clasificacion_log[0]['metadata']
                        timestamp = clasificacion_log[0]['timestamp']

                        if hasattr(timestamp, 'strftime'):
                            hora = timestamp.strftime('%H:%M')
                        else:
                            hora = str(timestamp)[11:16]

                        duration = metadata.get('elapsed_seconds', 0)
                        classifications_applied = metadata.get('classifications_applied', 0)

                        processes.append({
                            "name": "Clasificación ML",
                            "description": f"{classified_name} ({classifications_applied} clasificaciones)",
                            "status": "completed",
                            "last_run": hora,
                            "duration": f"{duration:.1f}s" if duration else "N/A",
                            "progress": 100
                        })
                    else:
                        # Existe tabla pero no hay log
                        processes.append({
                            "name": "Clasificación ML",
                            "description": f"{classified_name}",
                            "status": "completed",
                            "last_run": "N/A",
                            "duration": "N/A",
                            "progress": 100
                        })
                else:
                    # No existe tabla clasificada, está pendiente
                    processes.append({
                        "name": "Clasificación ML",
                        "description": "Auto-etiquetado pendiente",
                        "status": "pending",
                        "last_run": "N/A",
                        "duration": "N/A",
                        "progress": 0
                    })
            else:
                # No hay archivos ingresados
                processes.append({
                    "name": "Clasificación ML",
                    "description": "Auto-etiquetado",
                    "status": "pending",
                    "last_run": "N/A",
                    "duration": "N/A",
                    "progress": 0
                })
        except Exception as e:
            logger.error(f"Error obteniendo estado de Clasificación: {str(e)}")
            processes.append({
                "name": "Clasificación ML",
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
        AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
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
                # Verificar que podemos leer datos de la tabla activa
                try:
                    active_table = databricks_service.get_active_table()
                    if active_table:
                        query = f"SELECT COUNT(*) as total FROM {databricks_service.catalog}.{databricks_service.schema}.{active_table}"
                        result = databricks_service.execute_query(query)
                        health["components"]["databricks"]["records"] = result[0]["total"] if result else 0
                        health["components"]["databricks"]["active_table"] = active_table
                    else:
                        health["components"]["databricks"]["records"] = 0
                        health["components"]["databricks"]["active_table"] = "None"
                except:
                    health["components"]["databricks"]["records"] = 0
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
                AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOURS
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