from fastapi import APIRouter, HTTPException
from app.models.schemas import RAGQueryRequest, RAGQueryResponse
from app.services.databricks_service import databricks_service
from datetime import datetime
import uuid
import logging
import os
from openai import OpenAI

router = APIRouter(prefix="/api/rag", tags=["RAG: Consultas Inteligentes"])
logger = logging.getLogger(__name__)

QUERY_HISTORY = []

# Inicializar OpenAI client (opcional - solo si hay API key)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

def get_table_context() -> dict:
    """Obtiene contexto de la tabla activa de forma dinámica"""
    try:
        if not databricks_service.connect():
            return None

        # Obtener tabla activa
        active_table = databricks_service.get_active_table()
        if not active_table:
            databricks_service.disconnect()
            return None

        # Obtener esquema
        schema = databricks_service.get_table_schema(active_table)

        # Obtener estadísticas básicas
        query = f"""
        SELECT COUNT(*) as total_records
        FROM {databricks_service.catalog}.{databricks_service.schema}.{active_table}
        """
        result = databricks_service.execute_query(query)
        total_records = result[0]['total_records'] if result else 0

        # Obtener muestra de datos (primeras 5 filas)
        sample_query = f"""
        SELECT *
        FROM {databricks_service.catalog}.{databricks_service.schema}.{active_table}
        LIMIT 5
        """
        sample_data = databricks_service.execute_query(sample_query)

        databricks_service.disconnect()

        return {
            "table_name": active_table,
            "schema": schema,
            "total_records": total_records,
            "sample_data": sample_data
        }

    except Exception as e:
        logger.error(f"Error obteniendo contexto: {str(e)}")
        databricks_service.disconnect()
        return None

def generate_rag_response_with_openai(question: str, context: dict) -> dict:
    """Genera respuesta usando OpenAI GPT-4o-mini con contexto de Databricks"""
    try:
        # Preparar contexto para el prompt
        table_name = context.get("table_name", "N/A")
        total_records = context.get("total_records", 0)
        schema = context.get("schema", {})
        columns = schema.get("columns", [])
        sample_data = context.get("sample_data", [])

        # Crear descripción del esquema
        schema_description = "\n".join([
            f"- {col['name']} ({col['type']})"
            for col in columns
        ])

        # Crear muestra de datos
        sample_description = ""
        if sample_data and len(sample_data) > 0:
            sample_description = "Primeras filas de ejemplo:\n"
            for i, row in enumerate(sample_data[:3], 1):
                row_str = ", ".join([f"{k}: {v}" for k, v in list(row.items())[:5]])
                sample_description += f"{i}. {row_str}\n"

        # Crear prompt para OpenAI
        system_prompt = f"""Eres un asistente experto en análisis de datos.
Tienes acceso a una tabla de Databricks con la siguiente información:

**Tabla:** {table_name}
**Total de registros:** {total_records:,}

**Esquema (columnas):**
{schema_description}

{sample_description}

Tu tarea es responder preguntas sobre estos datos de manera clara, concisa y en español.
Si la pregunta requiere cálculos o agregaciones que no están disponibles, sugiere cómo obtenerlos.
Usa formato Markdown para las respuestas."""

        user_prompt = f"""Pregunta del usuario: {question}

Responde de manera clara y específica basándote en el esquema y datos disponibles."""

        # Llamar a OpenAI
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",  # Modelo pequeño y eficiente
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            max_tokens=500  # Limitar tokens para reducir costo
        )

        answer = response.choices[0].message.content
        sources = [
            f"Tabla: {table_name}",
            f"Total de registros: {total_records:,}",
            f"Modelo: GPT-4o-mini (OpenAI)"
        ]

        return {
            "answer": answer,
            "sources": sources,
            "confidence": 0.9
        }

    except Exception as e:
        logger.error(f"Error con OpenAI: {str(e)}")
        return {
            "answer": f"Error al procesar la pregunta con OpenAI: {str(e)}",
            "sources": ["Sistema"],
            "confidence": 0.0
        }


def generate_rag_response_fallback(question: str, context: dict) -> dict:
    """Respuesta fallback sin OpenAI (cuando no hay API key)"""
    table_name = context.get("table_name", "N/A")
    total_records = context.get("total_records", 0)
    schema = context.get("schema", {})
    columns = schema.get("columns", [])

    answer = f"""**📊 Información de la Tabla**

**Tabla activa:** `{table_name}`
**Total de registros:** {total_records:,}
**Columnas disponibles:** {len(columns)}

**Esquema:**
"""
    for col in columns[:10]:  # Mostrar primeras 10 columnas
        answer += f"\n• `{col['name']}` ({col['type']})"

    if len(columns) > 10:
        answer += f"\n• ... y {len(columns) - 10} columnas más"

    answer += f"""

**💡 Pregunta realizada:**
> {question}

⚠️ **Nota:** Para respuestas más inteligentes, configura tu API key de OpenAI en el archivo `.env`

**Consultas SQL disponibles:**
Puedes realizar consultas específicas sobre las columnas disponibles."""

    return {
        "answer": answer,
        "sources": [f"Tabla: {table_name}"],
        "confidence": 0.5
    }


