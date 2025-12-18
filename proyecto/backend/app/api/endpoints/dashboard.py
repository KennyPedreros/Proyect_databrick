from fastapi import APIRouter, HTTPException
from app.services.databricks_service import databricks_service
from datetime import datetime
import logging

router = APIRouter(prefix="/api/dashboard", tags=["Módulo 5: Dashboard"])
logger = logging.getLogger(__name__)

@router.get("/metrics")
async def get_dashboard_metrics():
    """Métricas principales"""
    try:
        # ✅ Verificar configuración primero
        if not databricks_service.is_configured():
            return {
                "total_cases": 0,
                "active_cases": 0,
                "recovered": 0,
                "deaths": 0,
                "message": "⚠️ Databricks no configurado"
            }
        
        # ✅ Conectar
        databricks_service.ensure_connected()
        
        # ✅ Obtener el nombre real de la tabla subida
        # Opción 1: Buscar todas las tablas
        query = f"""
        SHOW TABLES IN `{databricks_service.catalog}`.`{databricks_service.schema}`
        """
        tables = databricks_service.execute_query(query).fetchall()
        
        if not tables:
            return {
                "total_cases": 0,
                "message": "⚠️ No hay tablas. Sube archivos primero."
            }
        
        # Usar la primera tabla (o buscar por nombre específico)
        table_name = tables[0]['tableName']
        
        query = f"""
        SELECT COUNT(*) as total_cases
        FROM `{databricks_service.catalog}`.`{databricks_service.schema}`.`{table_name}`
        """
        
        result = databricks_service.execute_query(query).fetchone()
        
        return {
            "total_cases": result['total_cases'],
            "data_source": "databricks_real",
            "table_used": table_name
        }
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/timeseries")
async def get_timeseries_data(days: int = 30):
    """Series temporales - DATOS REALES agrupados por fecha"""
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        query = f"""
        SELECT 
            date,
            COUNT(*) as casos,
            SUM(CASE WHEN outcome = 'Fallecido' THEN 1 ELSE 0 END) as muertes,
            SUM(CASE WHEN vaccinated = true THEN 1 ELSE 0 END) as vacunados
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        WHERE date IS NOT NULL
        GROUP BY date
        ORDER BY date DESC
        LIMIT {days}
        """
        
        results = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        if results and len(results) > 0:
            timeseries = []
            for row in reversed(results):
                timeseries.append({
                    "date": str(row["date"]),
                    "casos": row["casos"],
                    "muertes": row["muertes"],
                    "vacunados": row["vacunados"]
                })
            
            return {
                "data": timeseries,
                "period_days": len(timeseries),
                "data_source": "databricks_real"
            }
        
        return {
            "data": [],
            "period_days": 0,
            "data_source": "empty",
            "message": "No hay datos de series temporales"
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo series: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/severity-distribution")
async def get_severity_distribution():
    """Distribución REAL por severidad"""
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        query = f"""
        SELECT 
            COALESCE(severity, 'Sin Clasificar') as severity,
            COUNT(*) as value
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        GROUP BY severity
        ORDER BY value DESC
        """
        
        results = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        if results and len(results) > 0:
            color_map = {
                "Leve": "#4CAF50",
                "Moderado": "#FFC107",
                "Grave": "#FF5722",
                "Crítico": "#9C27B0",
                "Sin Clasificar": "#9E9E9E"
            }
            
            distribution = []
            for row in results:
                distribution.append({
                    "name": row["severity"],
                    "value": row["value"],
                    "color": color_map.get(row["severity"], "#999999")
                })
            
            return distribution
        
        return []
        
    except Exception as e:
        logger.error(f"Error obteniendo distribución: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/geographic")
async def get_geographic_data():
    """Datos geográficos REALES"""
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        query = f"""
        SELECT 
            COALESCE(country, 'Unknown') as country,
            COALESCE(region, 'Unknown') as region,
            COUNT(*) as total_cases,
            SUM(CASE WHEN outcome = 'Fallecido' THEN 1 ELSE 0 END) as deaths
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        WHERE region IS NOT NULL AND region != 'Unknown'
        GROUP BY country, region
        ORDER BY total_cases DESC
        LIMIT 50
        """
        
        results = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        if results and len(results) > 0:
            return {
                "data": results,
                "total_locations": len(results),
                "data_source": "databricks_real"
            }
        
        return {
            "data": [],
            "total_locations": 0,
            "message": "No hay datos geográficos"
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo datos geográficos: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/age-distribution")
async def get_age_distribution():
    """Distribución REAL por edad"""
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
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
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        WHERE age IS NOT NULL AND age > 0 AND age < 120
        GROUP BY age_group
        ORDER BY MIN(age)
        """
        
        results = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        if results and len(results) > 0:
            return {
                "data": results,
                "data_source": "databricks_real"
            }
        
        return {"data": []}
        
    except Exception as e:
        logger.error(f"Error obteniendo distribución por edad: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/vaccination-stats")
async def get_vaccination_stats():
    """Estadísticas REALES de vacunación"""
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        query = f"""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN vaccinated = true THEN 1 ELSE 0 END) as vaccinated,
            SUM(CASE WHEN vaccinated = false OR vaccinated IS NULL THEN 1 ELSE 0 END) as not_vaccinated
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        """
        
        result = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        if result and result[0].get("total", 0) > 0:
            data = result[0]
            total = data.get("total", 1)
            vaccinated = data.get("vaccinated", 0)
            
            return {
                "total": total,
                "vaccinated": vaccinated,
                "not_vaccinated": data.get("not_vaccinated", 0),
                "vaccination_rate": round((vaccinated / total) * 100, 2) if total > 0 else 0,
                "data_source": "databricks_real"
            }
        
        return {
            "total": 0,
            "vaccinated": 0,
            "not_vaccinated": 0,
            "vaccination_rate": 0
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo vacunación: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/kpis")
async def get_kpis():
    """KPIs principales - DATOS REALES"""
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        query = f"""
        SELECT 
            COUNT(*) as total_cases,
            SUM(CASE WHEN severity = 'Crítico' THEN 1 ELSE 0 END) as critical_cases,
            ROUND((SUM(CASE WHEN outcome = 'Fallecido' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as mortality_rate,
            ROUND(AVG(CASE WHEN age > 0 AND age < 120 THEN age ELSE NULL END), 1) as average_age
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        """
        
        result = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        if result and result[0].get("total_cases", 0) > 0:
            kpis = result[0]
            return {
                "total_cases": kpis["total_cases"],
                "critical_cases": kpis["critical_cases"],
                "mortality_rate": kpis["mortality_rate"],
                "average_age": kpis["average_age"] or 0,
                "updated_at": datetime.now().isoformat(),
                "data_source": "databricks_real"
            }
        
        return {
            "total_cases": 0,
            "critical_cases": 0,
            "mortality_rate": 0.0,
            "average_age": 0.0,
            "updated_at": datetime.now().isoformat(),
            "data_source": "empty"
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo KPIs: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))