from fastapi import APIRouter, HTTPException
from app.services.databricks_service import databricks_service
from datetime import datetime, timedelta
import logging

router = APIRouter(prefix="/api/dashboard", tags=["Módulo 5: Dashboard"])
logger = logging.getLogger(__name__)


@router.get("/metrics")
async def get_dashboard_metrics():
    """
    Módulo 5: Métricas principales del dashboard
    
    Retorna KPIs principales:
    - Total de casos
    - Casos activos
    - Recuperados
    - Fallecidos
    """
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        # Query para métricas generales
        query = f"""
        SELECT 
            COUNT(*) as total_cases,
            SUM(CASE WHEN outcome = 'Activo' THEN 1 ELSE 0 END) as active_cases,
            SUM(CASE WHEN outcome = 'Recuperado' THEN 1 ELSE 0 END) as recovered,
            SUM(CASE WHEN outcome = 'Fallecido' THEN 1 ELSE 0 END) as deaths
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        """
        
        result = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        if result:
            data = result[0]
            return {
                "total_cases": data.get("total_cases", 0),
                "active_cases": data.get("active_cases", 0),
                "recovered": data.get("recovered", 0),
                "deaths": data.get("deaths", 0),
                "last_updated": datetime.now().isoformat()
            }
        
        return {
            "total_cases": 0,
            "active_cases": 0,
            "recovered": 0,
            "deaths": 0,
            "last_updated": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo métricas: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/timeseries")
async def get_timeseries_data(days: int = 30):
    """
    Datos de series temporales para gráficas
    
    Retorna evolución diaria de:
    - Casos nuevos
    - Muertes
    - Vacunados
    """
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
        WHERE date >= DATE_SUB(CURRENT_DATE(), {days})
        GROUP BY date
        ORDER BY date
        """
        
        results = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        # Formatear para el frontend
        timeseries = []
        for row in results:
            timeseries.append({
                "date": str(row["date"]),
                "casos": row["casos"],
                "muertes": row["muertes"],
                "vacunados": row["vacunados"]
            })
        
        return {
            "data": timeseries,
            "period_days": days
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo series temporales: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/severity-distribution")
async def get_severity_distribution():
    """
    Distribución de casos por severidad
    Para gráfica de pastel
    """
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        query = f"""
        SELECT 
            severity,
            COUNT(*) as value
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        WHERE severity IS NOT NULL
        GROUP BY severity
        """
        
        results = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        # Mapear colores
        color_map = {
            "Leve": "#4CAF50",
            "Moderado": "#FFC107",
            "Grave": "#FF5722",
            "Crítico": "#9C27B0"
        }
        
        distribution = []
        for row in results:
            distribution.append({
                "name": row["severity"],
                "value": row["value"],
                "color": color_map.get(row["severity"], "#999999")
            })
        
        return distribution
        
    except Exception as e:
        logger.error(f"Error obteniendo distribución: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/geographic")
async def get_geographic_data():
    """
    Datos por región/país para mapas
    """
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        query = f"""
        SELECT 
            country,
            region,
            COUNT(*) as total_cases,
            SUM(CASE WHEN outcome = 'Fallecido' THEN 1 ELSE 0 END) as deaths
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        GROUP BY country, region
        ORDER BY total_cases DESC
        LIMIT 50
        """
        
        results = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        return {
            "data": results,
            "total_locations": len(results)
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo datos geográficos: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/age-distribution")
async def get_age_distribution():
    """
    Distribución por grupos de edad
    """
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
        WHERE age IS NOT NULL
        GROUP BY age_group
        ORDER BY age_group
        """
        
        results = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        return {
            "data": results
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo distribución por edad: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/vaccination-stats")
async def get_vaccination_stats():
    """
    Estadísticas de vacunación
    """
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        query = f"""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN vaccinated = true THEN 1 ELSE 0 END) as vaccinated,
            SUM(CASE WHEN vaccinated = false THEN 1 ELSE 0 END) as not_vaccinated
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        """
        
        result = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        if result:
            data = result[0]
            total = data.get("total", 1)
            vaccinated = data.get("vaccinated", 0)
            
            return {
                "total": total,
                "vaccinated": vaccinated,
                "not_vaccinated": data.get("not_vaccinated", 0),
                "vaccination_rate": round((vaccinated / total) * 100, 2) if total > 0 else 0
            }
        
        return {
            "total": 0,
            "vaccinated": 0,
            "not_vaccinated": 0,
            "vaccination_rate": 0
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas de vacunación: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/kpis")
async def get_kpis():
    """
    KPIs principales del sistema
    """
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        # Obtener múltiples métricas
        queries = {
            "total": f"SELECT COUNT(*) as value FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed",
            "critical": f"SELECT COUNT(*) as value FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed WHERE severity = 'Crítico'",
            "mortality_rate": f"""
                SELECT 
                    ROUND((SUM(CASE WHEN outcome = 'Fallecido' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as value
                FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
            """,
            "avg_age": f"SELECT ROUND(AVG(age), 1) as value FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed WHERE age IS NOT NULL"
        }
        
        kpis = {}
        for key, query in queries.items():
            result = databricks_service.execute_query(query)
            kpis[key] = result[0]["value"] if result else 0
        
        databricks_service.disconnect()
        
        return {
            "total_cases": kpis["total"],
            "critical_cases": kpis["critical"],
            "mortality_rate": kpis["mortality_rate"],
            "average_age": kpis["avg_age"],
            "updated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo KPIs: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))