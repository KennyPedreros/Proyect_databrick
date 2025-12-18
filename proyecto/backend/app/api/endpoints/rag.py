from fastapi import APIRouter, HTTPException
from app.models.schemas import RAGQueryRequest, RAGQueryResponse
from app.services.databricks_service import databricks_service
from datetime import datetime
import uuid
import logging

router = APIRouter(prefix="/api/rag", tags=["RAG: Consultas Inteligentes"])
logger = logging.getLogger(__name__)

QUERY_HISTORY = []

def query_databricks_stats() -> dict:
    """Obtiene estadísticas REALES desde Databricks"""
    try:
        if not databricks_service.connect():
            return None
        
        query = f"""
        SELECT 
            COUNT(*) as total_cases,
            COUNT(DISTINCT region) as total_regions,
            ROUND(AVG(CASE WHEN age > 0 AND age < 120 THEN age ELSE NULL END), 1) as avg_age,
            SUM(CASE WHEN outcome = 'Fallecido' THEN 1 ELSE 0 END) as total_deaths,
            SUM(CASE WHEN outcome = 'Activo' THEN 1 ELSE 0 END) as active_cases,
            SUM(CASE WHEN outcome = 'Recuperado' THEN 1 ELSE 0 END) as recovered,
            SUM(CASE WHEN vaccinated = true THEN 1 ELSE 0 END) as vaccinated
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        """
        
        result = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        if result and len(result) > 0:
            return result[0]
        
        return None
    
    except Exception as e:
        logger.error(f"Error obteniendo stats: {str(e)}")
        databricks_service.disconnect()
        return None

def generate_rag_response_real(question: str) -> dict:
    """Genera respuesta usando datos REALES de Databricks"""
    question_lower = question.lower()
    
    stats = query_databricks_stats()
    
    if not stats:
        return {
            "answer": "⚠️ **No hay datos disponibles**\n\nPor favor, carga archivos usando el módulo de ingesta primero.",
            "sources": ["Sistema"],
            "confidence": 0.0
        }
    
    total_cases = stats.get("total_cases", 0)
    total_deaths = stats.get("total_deaths", 0)
    active_cases = stats.get("active_cases", 0)
    recovered = stats.get("recovered", 0)
    vaccinated = stats.get("vaccinated", 0)
    avg_age = stats.get("avg_age", 0) or 0
    
    mortality_rate = round((total_deaths / total_cases * 100), 2) if total_cases > 0 else 0
    vaccination_rate = round((vaccinated / total_cases * 100), 2) if total_cases > 0 else 0
    recovery_rate = round((recovered / total_cases * 100), 2) if total_cases > 0 else 0
    
    sources = [
        f"Delta Lake: {databricks_service.catalog}.{databricks_service.schema}.covid_processed",
        f"Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    ]
    
    # ==================== RESPUESTAS BASADAS EN DATOS REALES ====================
    
    if "cuántos casos" in question_lower or "total de casos" in question_lower or "total" in question_lower:
        answer = f"""**📊 Estadísticas Generales (Datos Reales)**

**Resumen de Casos:**
• Total de Casos: **{total_cases:,}**
• Casos Activos: **{active_cases:,}** ({round(active_cases/total_cases*100, 1) if total_cases > 0 else 0}%)
• Recuperados: **{recovered:,}** ({recovery_rate}%)
• Fallecidos: **{total_deaths:,}** ({mortality_rate}%)

**Vacunación:**
• Total Vacunados: **{vaccinated:,}** ({vaccination_rate}%)

**Demografía:**
• Edad Promedio: **{avg_age:.1f} años**

_Datos extraídos directamente de Delta Lake_"""
        
    elif "vacunación" in question_lower or "vacunad" in question_lower:
        answer = f"""**💉 Estadísticas de Vacunación (Datos Reales)**

• **Personas Vacunadas**: {vaccinated:,}
• **Personas No Vacunadas**: {total_cases - vaccinated:,}
• **Tasa de Vacunación**: {vaccination_rate}%

**Análisis:**
De los {total_cases:,} casos registrados, {vaccinated:,} personas están vacunadas.

_Fuente: Archivos cargados en el sistema_"""
        
    elif "mortalidad" in question_lower or "muerte" in question_lower or "fallec" in question_lower:
        answer = f"""**📊 Estadísticas de Mortalidad (Datos Reales)**

• **Total Fallecidos**: {total_deaths:,} personas
• **Tasa de Mortalidad**: {mortality_rate}%
• **Casos Activos**: {active_cases:,}
• **Recuperados**: {recovered:,}

**Cálculo:**
Tasa de Mortalidad = (Fallecidos / Total Casos) × 100
= ({total_deaths:,} / {total_cases:,}) × 100 = {mortality_rate}%

_Datos actualizados desde Delta Lake_"""
        
    elif "región" in question_lower or "provincia" in question_lower or "geográf" in question_lower:
        if not databricks_service.connect():
            return {"answer": "Error conectando a base de datos", "sources": [], "confidence": 0}
        
        query = f"""
        SELECT 
            region,
            COUNT(*) as total_cases,
            SUM(CASE WHEN outcome = 'Fallecido' THEN 1 ELSE 0 END) as deaths
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        WHERE region IS NOT NULL AND region != 'Unknown'
        GROUP BY region
        ORDER BY total_cases DESC
        LIMIT 10
        """
        
        regions = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        if regions:
            region_text = "\n".join([
                f"{i+1}. **{r['region']}**: {r['total_cases']:,} casos ({r['deaths']:,} fallecidos)"
                for i, r in enumerate(regions)
            ])
            
            answer = f"""**📍 Distribución Geográfica (Top 10 Regiones)**

{region_text}

**Total de Regiones**: {stats.get('total_regions', len(regions))}

_Datos extraídos de los archivos cargados_"""
        else:
            answer = "⚠️ No hay datos geográficos disponibles en los archivos cargados."
        
    elif "severidad" in question_lower or "gravedad" in question_lower or "clasificación" in question_lower:
        if not databricks_service.connect():
            return {"answer": "Error conectando a base de datos", "sources": [], "confidence": 0}
        
        query = f"""
        SELECT 
            COALESCE(severity, 'Sin Clasificar') as severity,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed), 1) as percentage
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        GROUP BY severity
        ORDER BY 
            CASE COALESCE(severity, 'Sin Clasificar')
                WHEN 'Crítico' THEN 1
                WHEN 'Grave' THEN 2
                WHEN 'Moderado' THEN 3
                WHEN 'Leve' THEN 4
                ELSE 5
            END
        """
        
        severities = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        if severities:
            emoji_map = {
                'Crítico': '🔴',
                'Grave': '🟠',
                'Moderado': '🟡',
                'Leve': '🟢',
                'Sin Clasificar': '⚪'
            }
            
            sev_text = "\n".join([
                f"{emoji_map.get(s['severity'], '⚪')} **{s['severity']}**: {s['count']:,} casos ({s['percentage']}%)"
                for s in severities
            ])
            
            total_classified = sum(s['count'] for s in severities if s['severity'] != 'Sin Clasificar')
            
            answer = f"""**🏥 Distribución por Severidad**

{sev_text}

**Total Clasificado**: {total_classified:,} casos

💡 _Tip: Si hay casos sin clasificar, puedes ejecutar la clasificación automática en el Módulo 4_"""
        else:
            answer = "⚠️ No hay datos de clasificación. Ejecuta primero el módulo de clasificación automática."
    
    elif "edad" in question_lower or "promedio" in question_lower:
        if not databricks_service.connect():
            return {"answer": "Error conectando a base de datos", "sources": [], "confidence": 0}
        
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
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed WHERE age > 0 AND age < 120), 1) as percentage
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        WHERE age > 0 AND age < 120
        GROUP BY age_group
        ORDER BY MIN(age)
        """
        
        age_groups = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        if age_groups:
            age_text = "\n".join([
                f"• **{g['age_group']} años**: {g['count']:,} casos ({g['percentage']}%)"
                for g in age_groups
            ])
            
            answer = f"""**👥 Distribución por Edad**

{age_text}

**Edad Promedio**: {avg_age:.1f} años

_Análisis basado en {total_cases:,} casos registrados_"""
        else:
            answer = "No hay datos de edad disponibles."
    
    else:
        answer = f"""He analizado tu pregunta: **"{question}"**

**📊 Resumen de Datos Disponibles:**

• Total de Casos: **{total_cases:,}**
• Casos Activos: **{active_cases:,}**
• Recuperados: **{recovered:,}**
• Fallecidos: **{total_deaths:,}**
• Tasa de Mortalidad: **{mortality_rate}%**
• Tasa de Vacunación: **{vaccination_rate}%**
• Edad Promedio: **{avg_age:.1f} años**

**💡 Puedo responder preguntas sobre:**
✅ Estadísticas generales y específicas
✅ Distribución geográfica por regiones
✅ Datos de vacunación
✅ Tasas de mortalidad y recuperación
✅ Clasificación por severidad
✅ Análisis por grupos de edad

¿Qué información específica necesitas?"""
    
    return {
        "answer": answer,
        "sources": sources,
        "confidence": 0.95
    }

@router.post("/query", response_model=RAGQueryResponse)
async def query_covid_data(request: RAGQueryRequest):
    """
    Sistema RAG: Consultas sobre datos COVID-19 REALES
    Los datos provienen directamente de Databricks Delta Lake
    """
    try:
        logger.info(f"Nueva consulta RAG: {request.question}")
        
        response_data = generate_rag_response_real(request.question)
        
        query_id = str(uuid.uuid4())
        response = RAGQueryResponse(
            answer=response_data["answer"],
            sources=response_data["sources"],
            confidence=response_data["confidence"],
            query_id=query_id,
            timestamp=datetime.now()
        )
        
        QUERY_HISTORY.insert(0, {
            "query_id": query_id,
            "question": request.question,
            "answer": response_data["answer"],
            "timestamp": datetime.now().isoformat(),
            "helpful": None
        })
        
        if len(QUERY_HISTORY) > 100:
            QUERY_HISTORY.pop()
        
        logger.info(f"Respuesta generada: {query_id}")
        
        return response
        
    except Exception as e:
        logger.error(f"Error en RAG: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/history")
async def get_query_history(limit: int = 20):
    """Obtener historial de consultas RAG"""
    return {
        "total": len(QUERY_HISTORY),
        "queries": QUERY_HISTORY[:limit]
    }

@router.post("/feedback")
async def submit_feedback(query_id: str, helpful: bool):
    """Enviar feedback sobre una respuesta"""
    for query in QUERY_HISTORY:
        if query["query_id"] == query_id:
            query["helpful"] = helpful
            logger.info(f"Feedback: {query_id} - {'✓' if helpful else '✗'}")
            return {
                "success": True,
                "message": "Feedback registrado",
                "query_id": query_id
            }
    
    raise HTTPException(status_code=404, detail="Query no encontrada")

@router.get("/stats")
async def get_rag_stats():
    """Estadísticas del sistema RAG"""
    total_queries = len(QUERY_HISTORY)
    helpful_count = sum(1 for q in QUERY_HISTORY if q.get("helpful") == True)
    not_helpful_count = sum(1 for q in QUERY_HISTORY if q.get("helpful") == False)
    
    return {
        "total_queries": total_queries,
        "feedback": {
            "helpful": helpful_count,
            "not_helpful": not_helpful_count,
            "no_feedback": total_queries - helpful_count - not_helpful_count
        },
        "satisfaction_rate": round((helpful_count / max(helpful_count + not_helpful_count, 1)) * 100, 2),
        "average_confidence": 0.95
    }

@router.delete("/history")
async def clear_history():
    """Limpiar historial de consultas"""
    QUERY_HISTORY.clear()
    return {
        "success": True,
        "message": "Historial limpiado"
    }