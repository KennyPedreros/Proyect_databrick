from fastapi import APIRouter, HTTPException
from app.services.databricks_service import databricks_service
from datetime import datetime
import logging

router = APIRouter(prefix="/api/dashboard", tags=["Módulo 5: Dashboard"])
logger = logging.getLogger(__name__)

def get_active_table():
    """Obtiene la tabla activa más reciente"""
    if not databricks_service.is_configured():
        return None
    
    try:
        databricks_service.ensure_connected()
        table_name = databricks_service.get_latest_table()
        
        if not table_name:
            logger.warning("⚠️ No hay tablas disponibles")
            return None
        
        logger.info(f"📊 Usando tabla: {table_name}")
        return table_name
    
    except Exception as e:
        logger.error(f"Error obteniendo tabla activa: {str(e)}")
        return None

@router.get("/metrics")
async def get_dashboard_metrics():
    """Métricas principales - DATOS REALES"""
    try:
        if not databricks_service.is_configured():
            return {
                "total_cases": 0,
                "active_cases": 0,
                "recovered": 0,
                "deaths": 0,
                "last_updated": datetime.now().isoformat(),
                "data_source": "not_configured",
                "message": "⚠️ Databricks no configurado. Configura .env y reinicia el servidor."
            }
        
        table_name = get_active_table()
        
        if not table_name:
            return {
                "total_cases": 0,
                "active_cases": 0,
                "recovered": 0,
                "deaths": 0,
                "last_updated": datetime.now().isoformat(),
                "data_source": "no_data",
                "message": "⚠️ No hay datos. Sube archivos usando /api/ingest/upload"
            }
        
        # Obtener conteo total
        query = f"""
        SELECT COUNT(*) as total_cases
        FROM `{databricks_service.catalog}`.`{databricks_service.schema}`.`{table_name}`
        """
        
        result = databricks_service.fetch_one(query)
        total_cases = result.get('total_cases', 0)
        
        # Intentar obtener métricas detalladas si existen las columnas
        try:
            detailed_query = f"""
            SELECT 
                COUNT(*) as total_cases,
                SUM(CASE WHEN outcome = 'Activo' THEN 1 ELSE 0 END) as active_cases,
                SUM(CASE WHEN outcome = 'Recuperado' THEN 1 ELSE 0 END) as recovered,
                SUM(CASE WHEN outcome = 'Fallecido' THEN 1 ELSE 0 END) as deaths
            FROM `{databricks_service.catalog}`.`{databricks_service.schema}`.`{table_name}`
            """
            
            detailed_result = databricks_service.fetch_one(detailed_query)
            
            return {
                "total_cases": detailed_result.get('total_cases', total_cases),
                "active_cases": detailed_result.get('active_cases', 0),
                "recovered": detailed_result.get('recovered', 0),
                "deaths": detailed_result.get('deaths', 0),
                "last_updated": datetime.now().isoformat(),
                "data_source": "databricks_real",
                "table_name": table_name
            }
        
        except:
            # Si no existen las columnas, retornar solo total
            return {
                "total_cases": total_cases,
                "active_cases": 0,
                "recovered": 0,
                "deaths": 0,
                "last_updated": datetime.now().isoformat(),
                "data_source": "databricks_real_simple",
                "table_name": table_name,
                "note": "Tabla sin columnas 'outcome'. Mostrando solo total."
            }
        
    except Exception as e:
        logger.error(f"Error en metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/timeseries")
async def get_timeseries_data(days: int = 30):
    """Series temporales"""
    try:
        if not databricks_service.is_configured():
            return {
                "data": [],
                "period_days": 0,
                "message": "Databricks no configurado"
            }
        
        table_name = get_active_table()
        
        if not table_name:
            return {
                "data": [],
                "period_days": 0,
                "message": "No hay tablas disponibles"
            }
        
        # Intentar con columna 'date'
        try:
            query = f"""
            SELECT 
                date,
                COUNT(*) as casos,
                SUM(CASE WHEN outcome = 'Fallecido' THEN 1 ELSE 0 END) as muertes,
                SUM(CASE WHEN vaccinated = true THEN 1 ELSE 0 END) as vacunados
            FROM `{databricks_service.catalog}`.`{databricks_service.schema}`.`{table_name}`
            WHERE date IS NOT NULL
            GROUP BY date
            ORDER BY date DESC
            LIMIT {days}
            """
            
            results = databricks_service.fetch_all(query)
            
            if results:
                timeseries = []
                for row in reversed(results):
                    timeseries.append({
                        "date": str(row["date"]),
                        "casos": row["casos"],
                        "muertes": row.get("muertes", 0),
                        "vacunados": row.get("vacunados", 0)
                    })
                
                return {
                    "data": timeseries,
                    "period_days": len(timeseries),
                    "data_source": "databricks_real"
                }
        
        except Exception as e:
            logger.warning(f"No hay columna 'date': {str(e)}")
        
        return {
            "data": [],
            "period_days": 0,
            "message": "Tabla sin columna 'date' para series temporales"
        }
        
    except Exception as e:
        logger.error(f"Error en timeseries: {str(e)}")
        return {
            "data": [],
            "period_days": 0,
            "error": str(e)
        }

@router.get("/severity-distribution")
async def get_severity_distribution():
    """Distribución por severidad"""
    try:
        if not databricks_service.is_configured():
            return []
        
        table_name = get_active_table()
        
        if not table_name:
            return []
        
        try:
            query = f"""
            SELECT 
                COALESCE(severity, 'Sin Clasificar') as severity,
                COUNT(*) as value
            FROM `{databricks_service.catalog}`.`{databricks_service.schema}`.`{table_name}`
            GROUP BY severity
            ORDER BY value DESC
            """
            
            results = databricks_service.fetch_all(query)
            
            color_map = {
                "Leve": "#4CAF50",
                "Moderado": "#FFC107",
                "Grave": "#FF5722",
                "Crítico": "#9C27B0",
                "Sin Clasificar": "#9E9E9E"
            }
            
            return [
                {
                    "name": row["severity"],
                    "value": row["value"],
                    "color": color_map.get(row["severity"], "#999999")
                }
                for row in results
            ]
        
        except:
            return []
        
    except Exception as e:
        logger.error(f"Error en severity: {str(e)}")
        return []

@router.get("/geographic")
async def get_geographic_data():
    """Datos geográficos"""
    try:
        if not databricks_service.is_configured():
            return {"data": [], "total_locations": 0}
        
        table_name = get_active_table()
        
        if not table_name:
            return {"data": [], "total_locations": 0}
        
        try:
            query = f"""
            SELECT 
                COALESCE(country, 'Unknown') as country,
                COALESCE(region, 'Unknown') as region,
                COUNT(*) as total_cases,
                SUM(CASE WHEN outcome = 'Fallecido' THEN 1 ELSE 0 END) as deaths
            FROM `{databricks_service.catalog}`.`{databricks_service.schema}`.`{table_name}`
            WHERE region IS NOT NULL AND region != 'Unknown'
            GROUP BY country, region
            ORDER BY total_cases DESC
            LIMIT 50
            """
            
            results = databricks_service.fetch_all(query)
            
            return {
                "data": results,
                "total_locations": len(results),
                "data_source": "databricks_real"
            }
        
        except:
            return {"data": [], "total_locations": 0}
        
    except Exception as e:
        logger.error(f"Error en geographic: {str(e)}")
        return {"data": [], "total_locations": 0}

@router.get("/age-distribution")
async def get_age_distribution():
    """Distribución por edad"""
    try:
        if not databricks_service.is_configured():
            return {"data": []}
        
        table_name = get_active_table()
        
        if not table_name:
            return {"data": []}
        
        try:
            query = f"""
            SELECT 
                CASE 
                    WHEN age < 18 THEN '0-17'
                    WHEN age < 30 THEN '18-29'
                    WHEN age < 45 THEN '30-44'
                    WHEN age < 60 THEN '45-59'
                    WHEN age < 75 THEN '60-74'
                    ELSE '75+'
                END as age_group,
                COUNT(*) as count
            FROM `{databricks_service.catalog}`.`{databricks_service.schema}`.`{table_name}`
            WHERE age IS NOT NULL AND age > 0 AND age < 120
            GROUP BY age_group
            ORDER BY MIN(age)
            """
            
            results = databricks_service.fetch_all(query)
            
            return {
                "data": results,
                "data_source": "databricks_real"
            }
        
        except:
            return {"data": []}
        
    except Exception as e:
        logger.error(f"Error en age: {str(e)}")
        return {"data": []}

@router.get("/vaccination-stats")
async def get_vaccination_stats():
    """Estadísticas de vacunación"""
    try:
        if not databricks_service.is_configured():
            return {
                "total": 0,
                "vaccinated": 0,
                "not_vaccinated": 0,
                "vaccination_rate": 0
            }
        
        table_name = get_active_table()
        
        if not table_name:
            return {
                "total": 0,
                "vaccinated": 0,
                "not_vaccinated": 0,
                "vaccination_rate": 0
            }
        
        try:
            query = f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN vaccinated = true THEN 1 ELSE 0 END) as vaccinated,
                SUM(CASE WHEN vaccinated = false OR vaccinated IS NULL THEN 1 ELSE 0 END) as not_vaccinated
            FROM `{databricks_service.catalog}`.`{databricks_service.schema}`.`{table_name}`
            """
            
            result = databricks_service.fetch_one(query)
            
            total = result.get("total", 0)
            vaccinated = result.get("vaccinated", 0)
            
            return {
                "total": total,
                "vaccinated": vaccinated,
                "not_vaccinated": result.get("not_vaccinated", 0),
                "vaccination_rate": round((vaccinated / total) * 100, 2) if total > 0 else 0,
                "data_source": "databricks_real"
            }
        
        except:
            return {
                "total": 0,
                "vaccinated": 0,
                "not_vaccinated": 0,
                "vaccination_rate": 0
            }
        
    except Exception as e:
        logger.error(f"Error en vaccination: {str(e)}")
        return {
            "total": 0,
            "vaccinated": 0,
            "not_vaccinated": 0,
            "vaccination_rate": 0
        }

@router.get("/kpis")
async def get_kpis():
    """KPIs principales"""
    try:
        if not databricks_service.is_configured():
            return {
                "total_cases": 0,
                "critical_cases": 0,
                "mortality_rate": 0.0,
                "average_age": 0.0,
                "updated_at": datetime.now().isoformat(),
                "data_source": "not_configured"
            }
        
        table_name = get_active_table()
        
        if not table_name:
            return {
                "total_cases": 0,
                "critical_cases": 0,
                "mortality_rate": 0.0,
                "average_age": 0.0,
                "updated_at": datetime.now().isoformat(),
                "data_source": "no_data"
            }
        
        try:
            query = f"""
            SELECT 
                COUNT(*) as total_cases,
                SUM(CASE WHEN severity = 'Crítico' THEN 1 ELSE 0 END) as critical_cases,
                ROUND((SUM(CASE WHEN outcome = 'Fallecido' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as mortality_rate,
                ROUND(AVG(CASE WHEN age > 0 AND age < 120 THEN age ELSE NULL END), 1) as average_age
            FROM `{databricks_service.catalog}`.`{databricks_service.schema}`.`{table_name}`
            """
            
            result = databricks_service.fetch_one(query)
            
            return {
                "total_cases": result.get("total_cases", 0),
                "critical_cases": result.get("critical_cases", 0),
                "mortality_rate": result.get("mortality_rate", 0.0) or 0.0,
                "average_age": result.get("average_age", 0.0) or 0.0,
                "updated_at": datetime.now().isoformat(),
                "data_source": "databricks_real"
            }
        
        except Exception as e:
            logger.warning(f"KPIs parciales: {str(e)}")
            
            # Intentar solo conteo total
            simple_query = f"""
            SELECT COUNT(*) as total_cases
            FROM `{databricks_service.catalog}`.`{databricks_service.schema}`.`{table_name}`
            """
            
            result = databricks_service.fetch_one(simple_query)
            
            return {
                "total_cases": result.get("total_cases", 0),
                "critical_cases": 0,
                "mortality_rate": 0.0,
                "average_age": 0.0,
                "updated_at": datetime.now().isoformat(),
                "data_source": "databricks_real_simple"
            }
        
    except Exception as e:
        logger.error(f"Error en kpis: {str(e)}")
        return {
            "total_cases": 0,
            "critical_cases": 0,
            "mortality_rate": 0.0,
            "average_age": 0.0,
            "updated_at": datetime.now().isoformat(),
            "data_source": "error"
        }