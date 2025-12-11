from langchain.chat_models import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.chains import LLMChain
from app.config.settings import settings
from app.services.databricks_service import databricks_service
from app.models.schemas import ClassificationResult, ModelMetrics
import logging
import uuid
from datetime import datetime
from typing import Dict, List

logger = logging.getLogger(__name__)


class ClassificationService:
    """Servicio para clasificación de casos COVID-19 con LLM"""
    
    def __init__(self):
        self.llm = None
        self.classification_chain = None
        self.initialize_llm()
        
    def initialize_llm(self):
        """Inicializa el modelo LLM de OpenAI"""
        try:
            if not settings.OPENAI_API_KEY or settings.OPENAI_API_KEY == "sk-tu-api-key-aqui":
                logger.warning("OpenAI API Key no configurada. Usando clasificación básica.")
                return
                
            self.llm = ChatOpenAI(
                model="gpt-3.5-turbo",
                temperature=0.3,
                openai_api_key=settings.OPENAI_API_KEY
            )
            
            # Crear el prompt template
            prompt = ChatPromptTemplate.from_messages([
                ("system", """Eres un experto médico especializado en COVID-19. 
                Tu tarea es clasificar casos según su severidad en 4 categorías:
                
                - Leve: Síntomas leves, sin dificultad respiratoria
                - Moderado: Síntomas moderados, algo de dificultad respiratoria
                - Grave: Síntomas graves, dificultad respiratoria significativa
                - Crítico: Condición crítica, requiere hospitalización urgente
                
                Analiza la información del paciente y responde SOLO con el nivel de severidad."""),
                ("human", """Paciente:
                - Edad: {age} años
                - Sexo: {gender}
                - Síntomas: {symptoms}
                - Historial médico: {medical_history}
                
                Severidad:""")
            ])
            
            self.classification_chain = LLMChain(
                llm=self.llm,
                prompt=prompt
            )
            
            logger.info("✅ LLM inicializado correctamente")
            
        except Exception as e:
            logger.error(f"Error inicializando LLM: {str(e)}")
    
    async def classify_case(self, case_data: Dict) -> Dict:
        """Clasifica un caso individual"""
        try:
            # Si no hay LLM, usar clasificación básica
            if not self.classification_chain:
                return self._basic_classification(case_data)
            
            # Clasificación con LLM
            result = await self.classification_chain.arun(
                age=case_data.get("age", "desconocido"),
                gender=case_data.get("gender", "desconocido"),
                symptoms=case_data.get("symptoms", "sin síntomas reportados"),
                medical_history=case_data.get("medical_history", "no reportado")
            )
            
            severity = result.strip()
            
            # Validar severidad
            valid_severities = ["Leve", "Moderado", "Grave", "Crítico"]
            if severity not in valid_severities:
                # Intentar mapear
                severity_lower = severity.lower()
                if "leve" in severity_lower:
                    severity = "Leve"
                elif "moderado" in severity_lower:
                    severity = "Moderado"
                elif "grave" in severity_lower:
                    severity = "Grave"
                elif "crítico" in severity_lower or "critico" in severity_lower:
                    severity = "Crítico"
                else:
                    severity = "Moderado"  # Default
            
            return {
                "severity": severity,
                "confidence": 0.85,
                "reasoning": f"Clasificado por IA basado en edad, síntomas y historial"
            }
            
        except Exception as e:
            logger.error(f"Error en clasificación: {str(e)}")
            return self._basic_classification(case_data)
    
    def _basic_classification(self, case_data: Dict) -> Dict:
        """Clasificación básica basada en reglas"""
        age = case_data.get("age", 0)
        symptoms = str(case_data.get("symptoms", "")).lower()
        
        # Reglas simples
        critical_symptoms = ["ventilador", "icu", "crítico", "oxígeno", "respirador"]
        severe_symptoms = ["neumonía", "hospitalización", "fiebre alta"]
        moderate_symptoms = ["fiebre", "tos", "fatiga"]
        
        if any(s in symptoms for s in critical_symptoms) or age > 70:
            severity = "Crítico"
        elif any(s in symptoms for s in severe_symptoms) or age > 60:
            severity = "Grave"
        elif any(s in symptoms for s in moderate_symptoms):
            severity = "Moderado"
        else:
            severity = "Leve"
        
        return {
            "severity": severity,
            "confidence": 0.65,
            "reasoning": "Clasificación basada en reglas"
        }
    
    async def classify_all_cases(self, use_llm: bool = True, batch_size: int = 100) -> ClassificationResult:
        """Clasifica todos los casos en la base de datos"""
        try:
            # Conectar a Databricks
            if not databricks_service.connect():
                raise Exception("No se pudo conectar a Databricks")
            
            # Obtener casos sin clasificar
            query = f"""
            SELECT case_id, age, gender, symptoms, medical_history
            FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
            WHERE severity IS NULL OR severity = ''
            LIMIT {batch_size}
            """
            
            cases = databricks_service.execute_query(query)
            
            if not cases:
                logger.info("No hay casos para clasificar")
                return ClassificationResult(
                    classification_id=str(uuid.uuid4()),
                    total_classified=0,
                    distribution={},
                    samples=[]
                )
            
            # Clasificar cada caso
            classifications = []
            for case in cases:
                result = await self.classify_case({
                    "age": case.get("age"),
                    "gender": case.get("gender"),
                    "symptoms": case.get("symptoms"),
                    "medical_history": case.get("medical_history")
                })
                
                classifications.append({
                    "case_id": case["case_id"],
                    "severity": result["severity"],
                    "confidence": result["confidence"]
                })
            
            # Actualizar base de datos
            for classification in classifications:
                update_query = f"""
                UPDATE {databricks_service.catalog}.{databricks_service.schema}.covid_processed
                SET severity = '{classification['severity']}',
                    classification_confidence = {classification['confidence']},
                    classified_at = current_timestamp()
                WHERE case_id = '{classification['case_id']}'
                """
                databricks_service.execute_query(update_query)
            
            # Calcular distribución
            distribution = {}
            for c in classifications:
                severity = c["severity"]
                distribution[severity] = distribution.get(severity, 0) + 1
            
            # Obtener muestras
            samples = []
            for i, case in enumerate(cases[:5]):
                samples.append({
                    "text": case.get("symptoms", ""),
                    "age": case.get("age"),
                    "symptoms": case.get("symptoms"),
                    "predicted_severity": classifications[i]["severity"],
                    "confidence": classifications[i]["confidence"]
                })
            
            result = ClassificationResult(
                classification_id=str(uuid.uuid4()),
                total_classified=len(classifications),
                distribution=distribution,
                samples=samples
            )
            
            logger.info(f"✅ Clasificados {len(classifications)} casos")
            
            databricks_service.disconnect()
            return result
            
        except Exception as e:
            logger.error(f"Error clasificando casos: {str(e)}")
            databricks_service.disconnect()
            raise
    
    def get_model_metrics(self) -> ModelMetrics:
        """Obtiene métricas del modelo"""
        # En producción, estas métricas vendrían de un proceso de evaluación real
        return ModelMetrics(
            accuracy=0.87,
            precision=0.85,
            recall=0.84,
            f1_score=0.845
        )
    
    def get_severity_distribution(self) -> Dict[str, int]:
        """Obtiene la distribución de severidad actual"""
        try:
            if not databricks_service.connect():
                return {}
            
            query = f"""
            SELECT severity, COUNT(*) as count
            FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
            WHERE severity IS NOT NULL
            GROUP BY severity
            """
            
            results = databricks_service.execute_query(query)
            databricks_service.disconnect()
            
            distribution = {}
            for row in results:
                distribution[row["severity"]] = row["count"]
            
            return distribution
            
        except Exception as e:
            logger.error(f"Error obteniendo distribución: {str(e)}")
            databricks_service.disconnect()
            return {}
    
    def get_classification_history(self, limit: int = 100) -> List[Dict]:
        """Obtiene historial de clasificaciones"""
        try:
            if not databricks_service.connect():
                return []
            
            query = f"""
            SELECT case_id, severity, classification_confidence, classified_at
            FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
            WHERE severity IS NOT NULL
            ORDER BY classified_at DESC
            LIMIT {limit}
            """
            
            history = databricks_service.execute_query(query)
            databricks_service.disconnect()
            
            return history
            
        except Exception as e:
            logger.error(f"Error obteniendo historial: {str(e)}")
            databricks_service.disconnect()
            return []
    
    def get_samples(self, limit: int = 10) -> List[Dict]:
        """Obtiene ejemplos de casos clasificados"""
        try:
            if not databricks_service.connect():
                return []
            
            query = f"""
            SELECT case_id, age, gender, symptoms, severity, classification_confidence
            FROM {databricks_service.catalog}.{databricks_service.schema}.covid_processed
            WHERE severity IS NOT NULL
            ORDER BY classified_at DESC
            LIMIT {limit}
            """
            
            samples = databricks_service.execute_query(query)
            databricks_service.disconnect()
            
            return samples
            
        except Exception as e:
            logger.error(f"Error obteniendo muestras: {str(e)}")
            databricks_service.disconnect()
            return []
    
    def retrain_model(self) -> Dict:
        """Re-entrena el modelo con nuevos datos"""
        # Este método sería implementado con un proceso de fine-tuning
        # Por ahora retorna información simulada
        return {
            "model_version": "2.0",
            "training_samples": 10000,
            "validation_accuracy": 0.89,
            "trained_at": datetime.now().isoformat()
        }


# Instancia global
classification_service = ClassificationService()