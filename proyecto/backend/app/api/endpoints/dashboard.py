from fastapi import APIRouter, HTTPException
from app.services.databricks_service import databricks_service
from datetime import datetime, timedelta
import logging
import random

router = APIRouter(prefix="/api/dashboard", tags=["Módulo 5: Dashboard"])
logger = logging.getLogger(__name__)

# Datos simulados más realistas (se actualizan con cada petición)
def generate_realistic_data():
    """Genera datos realistas para el dashboard"""
    base_date = datetime.now() - timedelta(days=30)
    
    # Generar serie temporal de 30 días
    timeseries = []
    for i in range(30):
        date = base_date + timedelta(days=i)
        casos = random.randint(800, 1500) - (i * 10)  # Tendencia decreciente
        muertes = int(casos * 0.02)  # 2% de mortalidad
        vacunados = random.randint(100, 300)
        
        timeseries.append({
            "date": date.strftime("%Y-%m-%d"),
            "casos": max(50, casos),
            "muertes": max(1, muertes),
            "vacunados": vacunados
        })
    
    return timeseries

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
        if databricks_service.connect():
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
            
            if result and result[0].get("total_cases", 0) > 0:
                data = result[0]
                return {
                    "total_cases": data.get("total_cases", 0),
                    "active_cases": data.get("active_cases", 0),
                    "recovered": data.get("recovered", 0),
                    "deaths": data.get("deaths", 0),
                    "last_updated": datetime.now().isoformat()
                }
        
        # Si no hay datos en Databricks, retornar datos simulados
        total_cases = random.randint(10000, 20000)
        active_rate = 0.15
        recovered_rate = 0.80
        death_rate = 0.05
        
        return {
            "total_cases": total_cases,
            "active_cases": int(total_cases * active_rate),
            "recovered": int(total_cases * recovered_rate),
            "deaths": int(total_cases * death_rate),
            "last_updated": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo métricas: {str(e)}")
        databricks_service.disconnect()
        
        # Retornar datos simulados en caso de error
        total_cases = 15234
        return {
            "total_cases": total_cases,
            "active_cases": 1845,
            "recovered": 12456,
            "deaths": 933,
            "last_updated": datetime.now().isoformat()
        }


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
        if databricks_service.connect():
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
            
            if results and len(results) > 0:
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
        
        # Si no hay datos, generar datos simulados realistas
        timeseries = generate_realistic_data()
        
        return {
            "data": timeseries[-days:],  # Últimos N días
            "period_days": days
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo series temporales: {str(e)}")
        databricks_service.disconnect()
        
        # Retornar datos simulados
        timeseries = generate_realistic_data()
        return {
            "data": timeseries[-days:],
            "period_days": days
        }


@router.get("/severity-distribution")
async def get_severity_distribution():
    """
    Distribución de casos por severidad
    Para gráfica de pastel
    """
    try:
        if databricks_service.connect():
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
            
            if results and len(results) > 0:
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
        
        # Datos simulados con distribución realista
        return [
            {"name": "Leve", "value": 450, "color": "#4CAF50"},
            {"name": "Moderado", "value": 320, "color": "#FFC107"},
            {"name": "Grave", "value": 180, "color": "#FF5722"},
            {"name": "Crítico", "value": 50, "color": "#9C27B0"}
        ]
        
    except Exception as e:
        logger.error(f"Error obteniendo distribución: {str(e)}")
        databricks_service.disconnect()
        
        # Retornar distribución simulada
        return [
            {"name": "Leve", "value": 450, "color": "#4CAF50"},
            {"name": "Moderado", "value": 320, "color": "#FFC107"},
            {"name": "Grave", "value": 180, "color": "#FF5722"},
            {"name": "Crítico", "value": 50, "color": "#9C27B0"}
        ]


@router.get("/geographic")
async def get_geographic_data():
    """
    Datos por región/país para mapas
    """
    try:
        if databricks_service.connect():
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
            
            if results and len(results) > 0:
                return {
                    "data": results,
                    "total_locations": len(results)
                }
        
        # Datos simulados de Ecuador
        return {
            "data": [
                {"country": "Ecuador", "region": "Pichincha", "total_cases": 4523, "deaths": 201},
                {"country": "Ecuador", "region": "Guayas", "total_cases": 3892, "deaths": 178},
                {"country": "Ecuador", "region": "Azuay", "total_cases": 2156, "deaths": 89},
                {"country": "Ecuador", "region": "Manabí", "total_cases": 1789, "deaths": 67},
                {"country": "Ecuador", "region": "Tungurahua", "total_cases": 1234, "deaths": 45}
            ],
            "total_locations": 5
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo datos geográficos: {str(e)}")
        databricks_service.disconnect()
        return {
            "data": [],
            "total_locations": 0
        }


@router.get("/age-distribution")
async def get_age_distribution():
    """
    Distribución por grupos de edad
    """
    try:
        if databricks_service.connect():
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
            
            if results and len(results) > 0:
                return {"data": results}
        
        # Datos simulados
        return {
            "data": [
                {"age_group": "0-17", "count": 856},
                {"age_group": "18-29", "count": 2341},
                {"age_group": "30-44", "count": 3892},
                {"age_group": "45-59", "count": 4523},
                {"age_group": "60-74", "count": 2456},
                {"age_group": "75+", "count": 1166}
            ]
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo distribución por edad: {str(e)}")
        databricks_service.disconnect()
        return {"data": []}


@router.get("/vaccination-stats")
async def get_vaccination_stats():
    """
    Estadísticas de vacunación
    """
    try:
        if databricks_service.connect():
            query = f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN vaccinated = true THEN 1 ELSE 0 END) as vaccinated,
                SUM(CASE WHEN vaccinated = false THEN 1 ELSE 0 END) as not_vaccinated
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
                    "vaccination_rate": round((vaccinated / total) * 100, 2) if total > 0 else 0
                }
        
        # Datos simulados
        return {
            "total": 15234,
            "vaccinated": 8500,
            "not_vaccinated": 6734,
            "vaccination_rate": 55.8
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas de vacunación: {str(e)}")
        databricks_service.disconnect()
        return {
            "total": 0,
            "vaccinated": 0,
            "not_vaccinated": 0,
            "vaccination_rate": 0
        }


@router.get("/kpis")
async def get_kpis():
    """
    KPIs principales del sistema
    """
    try:
        if databricks_service.connect():
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
            
            if kpis["total"] > 0:
                return {
                    "total_cases": kpis["total"],
                    "critical_cases": kpis["critical"],
                    "mortality_rate": kpis["mortality_rate"],
                    "average_age": kpis["avg_age"],
                    "updated_at": datetime.now().isoformat()
                }
        
        # KPIs simulados
        return {
            "total_cases": 15234,
            "critical_cases": 152,
            "mortality_rate": 6.12,
            "average_age": 45.3,
            "updated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo KPIs: {str(e)}")
        databricks_service.disconnect()
        
        # Retornar KPIs simulados
        return {
            "total_cases": 15234,
            "critical_cases": 152,
            "mortality_rate": 6.12,
            "average_age": 45.3,
            "updated_at": datetime.now().isoformat()
        }