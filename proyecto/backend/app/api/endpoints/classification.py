from app.services.monitoring_service import monitoring_service, LogLevel
from fastapi import APIRouter, HTTPException
from app.models.schemas import ClassificationResult, ModelMetrics
from app.services.databricks_service import databricks_service
import logging
import uuid
from datetime import datetime

router = APIRouter(prefix="/api/classify", tags=["Módulo 4: Clasificación"])
logger = logging.getLogger(__name__)


def classify_case_with_rules(age: int, symptoms: str) -> dict:
    """
    Clasificación usando Databricks SQL + Reglas de negocio
    (Sin necesidad de OpenAI - Todo en Databricks)
    """
    symptoms_lower = str(symptoms).lower()
    
    # Reglas de clasificación
    critical_keywords = ["ventilador", "uci", "oxígeno bajo", "saturación baja"]
    severe_keywords = ["neumonía", "hospitalización", "fiebre alta persistente"]
    moderate_keywords = ["fiebre", "tos persistente", "fatiga severa"]
    
    # Lógica de clasificación
    if any(k in symptoms_lower for k in critical_keywords) or age > 75:
        return {"severity": "Crítico", "confidence": 0.9}
    elif any(k in symptoms_lower for k in severe_keywords) or age > 65:
        return {"severity": "Grave", "confidence": 0.85}
    elif any(k in symptoms_lower for k in moderate_keywords) or age > 50:
        return {"severity": "Moderado", "confidence": 0.8}
    else:
        return {"severity": "Leve", "confidence": 0.75}


@router.post("/auto-label", response_model=ClassificationResult)
async def classify_cases():
    """
    Módulo 4: Clasificación automática usando Databricks
    
    Clasifica casos en la base de datos según:
    - Edad
    - Síntomas reportados
    - Condiciones médicas
    
    Categorías: Leve, Moderado, Grave, Crítico
    """
    try:
        logger.info("🚀 Iniciando clasificación automática...")
        
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        # 1. Obtener casos sin clasificar
        query_select = f"""
        SELECT case_id, age, symptoms
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        WHERE severity IS NULL OR severity = ''
        LIMIT 1000
        """
        
        cases = databricks_service.execute_query(query_select)
        
        if not cases:
            databricks_service.disconnect()
            return ClassificationResult(
                classification_id=str(uuid.uuid4()),
                total_classified=0,
                distribution={},
                samples=[]
            )
        
        # 2. Clasificar cada caso
        distribution = {"Leve": 0, "Moderado": 0, "Grave": 0, "Crítico": 0}
        samples = []
        
        for case in cases[:1000]:  # Procesar en lotes
            result = classify_case_with_rules(
                age=case.get("age", 0),
                symptoms=case.get("symptoms", "")
            )
            
            severity = result["severity"]
            confidence = result["confidence"]
            
            # Actualizar en Databricks
            query_update = f"""
            UPDATE {databricks_service.catalog}.{databricks_service.schema}.covid_processed
            SET severity = '{severity}',
                classification_confidence = {confidence}
            WHERE case_id = '{case["case_id"]}'
            """
            
            databricks_service.execute_query(query_update)
            
            # Estadísticas
            distribution[severity] += 1
            
            # Guardar muestras
            if len(samples) < 5:
                samples.append({
                    "text": case.get("symptoms", ""),
                    "age": case.get("age"),
                    "symptoms": case.get("symptoms"),
                    "predicted_severity": severity,
                    "confidence": confidence
                })
        
        databricks_service.disconnect()
        
        logger.info(f"✅ Clasificados {len(cases)} casos")
        
        monitoring_service.log_event(
            process="Clasificación",
            level=LogLevel.SUCCESS,
            message=f"Clasificados {len(cases)} casos",
            data={
                "total_classified": len(cases),
                "distribution": distribution
            }
)

        return ClassificationResult(
            classification_id=str(uuid.uuid4()),
            total_classified=len(cases),
            distribution=distribution,
            samples=samples
        )
        
    except Exception as e:
        logger.error(f"❌ Error en clasificación: {str(e)}")
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics", response_model=ModelMetrics)
async def get_classification_metrics():
    """Métricas del modelo de clasificación"""
    try:
        # Métricas simuladas (en producción vendrían de un proceso real)
        return ModelMetrics(
            accuracy=0.87,
            precision=0.85,
            recall=0.84,
            f1_score=0.845
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/distribution")
async def get_severity_distribution():
    """Distribución de casos por severidad"""
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        query = f"""
        SELECT severity, COUNT(*) as count
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        WHERE severity IS NOT NULL
        GROUP BY severity
        """
        
        results = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        distribution = {}
        total = 0
        for row in results:
            distribution[row["severity"]] = row["count"]
            total += row["count"]
        
        return {
            "distribution": distribution,
            "total": total
        }
        
    except Exception as e:
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/samples")
async def get_classified_samples(limit: int = 10):
    """Ejemplos de casos clasificados"""
    try:
        if not databricks_service.connect():
            raise HTTPException(status_code=500, detail="Error conectando a Databricks")
        
        query = f"""
        SELECT case_id, age, symptoms, severity, classification_confidence
        FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
        WHERE severity IS NOT NULL
        ORDER BY case_id DESC
        LIMIT {limit}
        """
        
        samples = databricks_service.execute_query(query)
        databricks_service.disconnect()
        
        return {
            "samples": samples,
            "total": len(samples)
        }
        
    except Exception as e:
        databricks_service.disconnect()
        raise HTTPException(status_code=500, detail=str(e))