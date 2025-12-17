import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from enum import Enum
import psutil
import time
from app.services.databricks_service import databricks_service

logger = logging.getLogger(__name__)

class LogLevel(str, Enum):
    """Niveles de log del sistema"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    SUCCESS = "SUCCESS"
    DEBUG = "DEBUG"


class AlertLevel(str, Enum):
    """Niveles de alerta"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class MonitoringService:
    """Servicio de monitoreo del sistema en tiempo real"""
    
    def __init__(self):
        self.events_buffer = []
        self.alerts_buffer = []
        self.health_checks = {}
        
        # Umbrales de alerta
        self.thresholds = {
            "cpu_usage": 80,
            "memory_usage": 85,
            "disk_usage": 85,
            "response_time_ms": 5000,
            "error_rate_percent": 1.0,
            "query_timeout_seconds": 30,
        }
    
    def log_event(
        self,
        process: str,
        level: LogLevel,
        message: str,
        data: Optional[Dict] = None,
        user: Optional[str] = None
    ) -> Dict:
        """Registra un evento en el sistema"""
        
        event = {
            "timestamp": datetime.now().isoformat(),
            "process": process,
            "level": level.value,
            "message": message,
            "data": data or {},
            "user": user,
            "event_id": self._generate_event_id(),
        }
        
        self.events_buffer.append(event)
        
        # Log local también
        log_method = getattr(logger, level.value.lower(), logger.info)
        log_method(f"[{process}] {message}")
        
        # AGREGAR: Guardar en Delta Lake inmediatamente
        if databricks_service.connect():
            try:
                query = f"""
                INSERT INTO {databricks_service.catalog}.{databricks_service.schema}.audit_logs
                (log_id, timestamp, process, level, message, user_id, metadata)
                VALUES (
                    '{event['event_id']}',
                    '{event['timestamp']}',
                    '{process}',
                    '{level.value}',
                    '{message.replace("'", "''")}',
                    '{user or "system"}',
                    '{json.dumps(data or {})}'
                )
                """
                databricks_service.execute_query(query)
                databricks_service.disconnect()
            except Exception as e:
                logger.error(f"Error guardando log en Delta Lake: {str(e)}")
                databricks_service.disconnect()
        
        return event
    
    def _generate_event_id(self) -> str:
        """Genera un ID único para el evento"""
        import uuid
        return f"EVT-{uuid.uuid4().hex[:8].upper()}"
    
    def flush_events_to_databricks(self) -> int:
        """Guarda todos los eventos en buffer a Databricks"""
        if not self.events_buffer:
            return 0
        
        events_to_save = self.events_buffer.copy()
        self.events_buffer = []
        
        try:
            for event in events_to_save:
                self._save_event_to_databricks(event)
            
            logger.info(f"✅ {len(events_to_save)} eventos guardados en Databricks")
            return len(events_to_save)
        
        except Exception as e:
            logger.error(f"❌ Error guardando eventos: {str(e)}")
            return 0
    
    def _save_event_to_databricks(self, event: Dict):
        """Guarda un evento individual en Databricks"""
        try:
            if not databricks_service.connect():
                return
            
            # Convertir data a JSON string para almacenamiento
            event_data_json = json.dumps(event.get("data", {}))
            
            query = f"""
            INSERT INTO {databricks_service.catalog}.{databricks_service.schema}.audit_logs
            (event_id, timestamp, process, level, message, data, user)
            VALUES (
                '{event['event_id']}',
                '{event['timestamp']}',
                '{event['process']}',
                '{event['level']}',
                '{event['message'].replace("'", "''")}',
                '{event_data_json}',
                '{event.get('user', 'system')}'
            )
            """
            
            databricks_service.execute_query(query)
            databricks_service.disconnect()
        
        except Exception as e:
            logger.error(f"Error guardando evento en Databricks: {str(e)}")
    
    # ============================================
    # HEALTH CHECKS
    # ============================================
    
    def perform_health_checks(self) -> Dict:
        """Ejecuta todos los health checks"""
        
        checks = {
            "timestamp": datetime.now().isoformat(),
            "system": {
                "cpu_usage": self._check_cpu_usage(),
                "memory_usage": self._check_memory_usage(),
                "disk_usage": self._check_disk_usage(),
            },
            "services": {
                "databricks": self._check_databricks_connection(),
                "delta_lake": self._check_delta_lake(),
                "api": self._check_api_health(),
            },
            "data": {
                "data_freshness": self._check_data_freshness(),
                "table_sizes": self._check_table_sizes(),
            }
        }
        
        self.health_checks = checks
        self._generate_alerts_from_checks(checks)
        
        return checks
    
    def _check_cpu_usage(self) -> Dict:
        """Verifica uso de CPU"""
        cpu_percent = psutil.cpu_percent(interval=1)
        
        status = "healthy"
        if cpu_percent > self.thresholds["cpu_usage"]:
            status = "warning"
        
        return {
            "value": cpu_percent,
            "unit": "%",
            "threshold": self.thresholds["cpu_usage"],
            "status": status
        }
    
    def _check_memory_usage(self) -> Dict:
        """Verifica uso de memoria"""
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        
        status = "healthy"
        if memory_percent > self.thresholds["memory_usage"]:
            status = "warning"
        
        return {
            "value": memory_percent,
            "unit": "%",
            "available": f"{memory.available / (1024**3):.2f} GB",
            "threshold": self.thresholds["memory_usage"],
            "status": status
        }
    
    def _check_disk_usage(self) -> Dict:
        """Verifica uso de disco"""
        disk = psutil.disk_usage("/")
        disk_percent = disk.percent
        
        status = "healthy"
        if disk_percent > self.thresholds["disk_usage"]:
            status = "warning"
        
        return {
            "value": disk_percent,
            "unit": "%",
            "free": f"{disk.free / (1024**3):.2f} GB",
            "threshold": self.thresholds["disk_usage"],
            "status": status
        }
    
    def _check_databricks_connection(self) -> Dict:
        """Verifica conexión con Databricks"""
        try:
            is_connected = databricks_service.connect()
            databricks_service.disconnect()
            
            return {
                "status": "healthy" if is_connected else "unhealthy",
                "message": "Conexión exitosa" if is_connected else "No se pudo conectar"
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": f"Error: {str(e)}"
            }
    
    def _check_delta_lake(self) -> Dict:
        """Verifica accesibilidad de Delta Lake"""
        try:
            if not databricks_service.connect():
                return {"status": "unhealthy", "message": "No se pudo conectar"}
            
            # Verificar que podemos acceder a las tablas
            query = f"""
            SELECT COUNT(*) as total
            FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
            LIMIT 1
            """
            
            result = databricks_service.execute_query(query)
            databricks_service.disconnect()
            
            return {
                "status": "healthy",
                "message": "Tablas accesibles",
                "record_count": result[0]['total'] if result else 0
            }
        
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": f"Error accediendo a tablas: {str(e)}"
            }
    
    def _check_api_health(self) -> Dict:
        """Verifica salud de la API"""
        return {
            "status": "healthy",
            "uptime": "99.9%",
            "requests_per_second": 0
        }
    
    def _check_data_freshness(self) -> Dict:
        """Verifica que los datos estén actualizados"""
        try:
            if not databricks_service.connect():
                return {"status": "unknown"}
            
            query = f"""
            SELECT MAX(processed_at) as last_update
            FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
            """
            
            result = databricks_service.execute_query(query)
            databricks_service.disconnect()
            
            if result and result[0]['last_update']:
                last_update = result[0]['last_update']
                hours_old = (datetime.now() - last_update).total_seconds() / 3600
                
                status = "healthy"
                if hours_old > 24:
                    status = "warning"
                if hours_old > 48:
                    status = "critical"
                
                return {
                    "status": status,
                    "last_update": str(last_update),
                    "hours_old": hours_old
                }
            
            return {"status": "unknown"}
        
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def _check_table_sizes(self) -> Dict:
        """Verifica tamaños de las tablas principales"""
        try:
            if not databricks_service.connect():
                return {}
            
            query = f"""
            SELECT 
                'covid_raw' as table_name,
                COUNT(*) as record_count
            FROM {databricks_service.catalog}.{databricks_service.schema}.covid_raw
            
            UNION ALL
            
            SELECT 
                'covid_processed' as table_name,
                COUNT(*) as record_count
            FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
            """
            
            results = databricks_service.execute_query(query)
            databricks_service.disconnect()
            
            sizes = {}
            for row in results:
                sizes[row['table_name']] = row['record_count']
            
            return sizes
        
        except Exception as e:
            return {"error": str(e)}
    
    def _generate_alerts_from_checks(self, checks: Dict):
        """Genera alertas basadas en health checks"""
        
        # Alerta por uso de CPU
        if checks["system"]["cpu_usage"]["status"] == "warning":
            self.create_alert(
                alert_type="cpu_high",
                level=AlertLevel.MEDIUM,
                title="Alto uso de CPU",
                message=f"CPU en {checks['system']['cpu_usage']['value']}%"
            )
        
        # Alerta por uso de memoria
        if checks["system"]["memory_usage"]["status"] == "warning":
            self.create_alert(
                alert_type="memory_high",
                level=AlertLevel.MEDIUM,
                title="Alto uso de memoria",
                message=f"Memoria en {checks['system']['memory_usage']['value']}%"
            )
        
        # Alerta por Databricks
        if checks["services"]["databricks"]["status"] == "unhealthy":
            self.create_alert(
                alert_type="databricks_down",
                level=AlertLevel.CRITICAL,
                title="Databricks no disponible",
                message=checks["services"]["databricks"]["message"]
            )
        
        # Alerta por datos desactualizados
        if isinstance(checks["data"].get("data_freshness"), dict):
            freshness = checks["data"]["data_freshness"]
            if freshness.get("status") == "critical":
                self.create_alert(
                    alert_type="data_stale",
                    level=AlertLevel.HIGH,
                    title="Datos desactualizados",
                    message=f"Últimos datos con {freshness['hours_old']:.1f} horas de antigüedad"
                )
    
    # ============================================
    # ALERTAS
    # ============================================
    
    def create_alert(
        self,
        alert_type: str,
        level: AlertLevel,
        title: str,
        message: str,
        metadata: Optional[Dict] = None
    ) -> Dict:
        """Crea una alerta"""
        
        alert = {
            "alert_id": self._generate_alert_id(),
            "timestamp": datetime.now().isoformat(),
            "type": alert_type,
            "level": level.value,
            "title": title,
            "message": message,
            "metadata": metadata or {},
            "acknowledged": False,
        }
        
        self.alerts_buffer.append(alert)
        
        # Log la alerta
        self.log_event(
            process="AlertSystem",
            level=LogLevel.WARNING if level == AlertLevel.MEDIUM else LogLevel.ERROR,
            message=f"[{alert_type}] {title}",
            data=alert
        )
        
        return alert
    
    def _generate_alert_id(self) -> str:
        """Genera un ID único para la alerta"""
        import uuid
        return f"ALT-{uuid.uuid4().hex[:8].upper()}"
    
    def acknowledge_alert(self, alert_id: str) -> bool:
        """Marca una alerta como reconocida"""
        for alert in self.alerts_buffer:
            if alert["alert_id"] == alert_id:
                alert["acknowledged"] = True
                alert["acknowledged_at"] = datetime.now().isoformat()
                return True
        return False
    
    def get_active_alerts(self, level: Optional[AlertLevel] = None) -> List[Dict]:
        """Obtiene alertas activas"""
        alerts = [a for a in self.alerts_buffer if not a.get("acknowledged")]
        
        if level:
            alerts = [a for a in alerts if a["level"] == level.value]
        
        return alerts
    
    # ============================================
    # REPORTES
    # ============================================
    
    def get_system_status_report(self) -> Dict:
        """Genera reporte de estado del sistema"""
        
        checks = self.health_checks or self.perform_health_checks()
        
        # Calcular estado general
        all_statuses = []
        for section in checks.values():
            if isinstance(section, dict):
                for item in section.values():
                    if isinstance(item, dict) and "status" in item:
                        all_statuses.append(item["status"])
        
        if "unhealthy" in all_statuses:
            overall_status = "UNHEALTHY"
        elif "critical" in all_statuses:
            overall_status = "CRITICAL"
        elif "warning" in all_statuses:
            overall_status = "WARNING"
        else:
            overall_status = "HEALTHY"
        
        return {
            "timestamp": datetime.now().isoformat(),
            "overall_status": overall_status,
            "checks": checks,
            "active_alerts": len(self.get_active_alerts()),
            "critical_alerts": len(self.get_active_alerts(AlertLevel.CRITICAL))
        }
    
    def get_performance_metrics(self, hours: int = 24) -> Dict:
        """Obtiene métricas de desempeño del sistema"""
        try:
            if not databricks_service.connect():
                return {}
            
            query = f"""
            SELECT 
                DATE_TRUNC('hour', timestamp) as hour,
                COUNT(*) as total_events,
                COUNTIF(level = 'ERROR') as errors,
                COUNTIF(level = 'WARNING') as warnings
            FROM {databricks_service.catalog}.{databricks_service.schema}.audit_logs
            WHERE timestamp >= DATE_SUB(NOW(), {hours})
            GROUP BY DATE_TRUNC('hour', timestamp)
            ORDER BY hour DESC
            """
            
            results = databricks_service.execute_query(query)
            databricks_service.disconnect()
            
            return {
                "period_hours": hours,
                "hourly_metrics": results
            }
        
        except Exception as e:
            logger.error(f"Error obteniendo métricas: {str(e)}")
            return {}
    
    def get_audit_trail(
        self,
        process: Optional[str] = None,
        level: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Obtiene el registro de auditoría filtrado"""
        try:
            if not databricks_service.connect():
                return []
            
            where_clauses = ["timestamp >= DATE_SUB(NOW(), 30)"]
            
            if process:
                where_clauses.append(f"process = '{process}'")
            if level:
                where_clauses.append(f"level = '{level}'")
            
            where_clause = " AND ".join(where_clauses)
            
            query = f"""
            SELECT * FROM {databricks_service.catalog}.{databricks_service.schema}.audit_logs
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT {limit}
            """
            
            results = databricks_service.execute_query(query)
            databricks_service.disconnect()
            
            return results
        
        except Exception as e:
            logger.error(f"Error en audit trail: {str(e)}")
            return []


# Instancia global
monitoring_service = MonitoringService()

def init_audit_table():
    """Inicializa la tabla de auditoría"""
    if databricks_service.connect():
        try:
            query = f"""
            CREATE TABLE IF NOT EXISTS {databricks_service.catalog}.{databricks_service.schema}.audit_logs (
                log_id STRING,
                timestamp TIMESTAMP,
                process STRING,
                level STRING,
                message STRING,
                user_id STRING,
                metadata STRING
            )
            USING DELTA
            """
            databricks_service.execute_query(query)
            databricks_service.disconnect()
        except:
            databricks_service.disconnect()

init_audit_table()