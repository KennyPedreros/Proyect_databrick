from fastapi import APIRouter, HTTPException
from app.services.databricks_service import databricks_service
from pydantic import BaseModel
from typing import Optional, List
import logging
from datetime import datetime
import json

router = APIRouter(prefix="/api/rag", tags=["Módulo 5: RAG con Llama 3.1"])
logger = logging.getLogger(__name__)


class QueryRequest(BaseModel):
    question: str
    table_name: Optional[str] = None  # Si no se provee, usa tabla más reciente


class QueryResponse(BaseModel):
    question: str
    answer: str
    sql_query: str
    table_used: str
    data_preview: Optional[List[dict]] = None
    execution_time: float


@router.post("/query", response_model=QueryResponse)
async def ask_question(request: QueryRequest):
    """
    Consulta RAG: Pregunta en lenguaje natural → SQL → Llama 3.1 → Respuesta
    100% Databricks - Sin OpenAI
    """
    try:
        start_time = datetime.now()

        if not databricks_service.is_configured():
            raise HTTPException(
                status_code=400,
                detail="Databricks no está configurado"
            )

        # Conectar a Databricks
        if not databricks_service.connect():
            raise HTTPException(
                status_code=500,
                detail="Error conectando a Databricks"
            )

        # Determinar tabla a consultar - SIEMPRE usa tabla ORIGINAL
        if request.table_name:
            table_name = request.table_name
        else:
            # Obtener todas las tablas disponibles
            query = f"SHOW TABLES IN {databricks_service.catalog}.{databricks_service.schema}"
            all_tables = databricks_service.execute_query(query)

            if not all_tables:
                raise HTTPException(
                    status_code=404,
                    detail="No hay tablas disponibles"
                )

            # Log de todas las tablas encontradas
            all_table_names = [t['tableName'] for t in all_tables]
            logger.info(f"📋 Tablas encontradas en Databricks: {all_table_names}")

            # Filtrar solo tablas originales (sin _clean ni _classified)
            # Y excluir audit_logs y raw_data
            original_tables = [
                t['tableName'] for t in all_tables
                if t['tableName'] not in ['audit_logs', 'raw_data']
                and not t['tableName'].endswith('_clean')
                and not t['tableName'].endswith('_classified')
            ]

            logger.info(f"📊 Tablas originales filtradas: {original_tables}")

            if not original_tables:
                raise HTTPException(
                    status_code=404,
                    detail=f"No hay tablas originales disponibles. Tablas encontradas: {all_table_names}"
                )

            # Usar la tabla más reciente (última en la lista)
            table_name = original_tables[-1]

            logger.info(f"✅ Usando tabla ORIGINAL: {table_name}")

        logger.info(f"🔍 Procesando pregunta: '{request.question}' en tabla: {table_name}")

        # 1. Generar SQL desde la pregunta
        sql_query = databricks_service.generate_sql_from_question(
            question=request.question,
            table_name=table_name
        )

        logger.info(f"📊 SQL generado: {sql_query}")

        # 2. Ejecutar query en Databricks
        query_results = databricks_service.execute_query(sql_query)

        if not query_results:
            query_results = []

        # Limitar resultados para contexto
        preview_data = query_results[:10] if query_results else []

        # 3. Formatear datos para contexto de Llama
        if query_results:
            # Convertir resultados a texto legible
            context_text = f"Tabla consultada: {table_name}\n"
            context_text += f"SQL ejecutado: {sql_query}\n\n"
            context_text += f"Resultados obtenidos ({len(query_results)} registros):\n"

            # Agregar primeras filas como contexto
            for i, row in enumerate(preview_data, 1):
                context_text += f"\n{i}. " + ", ".join([f"{k}: {v}" for k, v in row.items()])
        else:
            context_text = f"Tabla consultada: {table_name}\nNo se encontraron resultados para esta consulta."

        # 4. Consultar a Llama 3.1 para generar respuesta
        llama_response = databricks_service.query_llama(
            prompt=request.question,
            context=context_text
        )

        execution_time = (datetime.now() - start_time).total_seconds()

        # 5. Registrar en audit_logs
        databricks_service.insert_audit_log(
            process="RAG_Query",
            level="SUCCESS",
            message=f"Consulta RAG procesada: '{request.question[:50]}...'",
            metadata={
                "question": request.question,
                "table_used": table_name,
                "sql_query": sql_query,
                "results_count": len(query_results),
                "execution_time": execution_time
            }
        )

        databricks_service.disconnect()

        logger.info(f"✅ Consulta RAG completada en {execution_time:.2f}s")

        return QueryResponse(
            question=request.question,
            answer=llama_response,
            sql_query=sql_query,
            table_used=table_name,
            data_preview=preview_data,
            execution_time=execution_time
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en consulta RAG: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
async def get_query_history(limit: int = 10):
    """
    Obtiene el historial de consultas RAG desde audit_logs
    """
    try:
        if not databricks_service.is_configured():
            raise HTTPException(
                status_code=400,
                detail="Databricks no está configurado"
            )

        if not databricks_service.connect():
            raise HTTPException(
                status_code=500,
                detail="Error conectando a Databricks"
            )

        # Obtener logs de RAG
        query = f"""
            SELECT
                timestamp,
                level,
                message,
                metadata
            FROM {databricks_service.catalog}.{databricks_service.schema}.audit_logs
            WHERE process = 'RAG_Query'
            ORDER BY timestamp DESC
            LIMIT {limit}
        """

        logs = databricks_service.execute_query(query)
        databricks_service.disconnect()

        if not logs:
            return {"history": []}

        # Formatear logs
        history = []
        for log in logs:
            try:
                # Parsear metadata JSON de forma segura
                if isinstance(log['metadata'], str):
                    # Revertir el escape de backslashes si es necesario
                    metadata_str = log['metadata'].replace("\\\\", "\\")
                    metadata = json.loads(metadata_str)
                else:
                    metadata = log['metadata']

                history.append({
                    "timestamp": log['timestamp'].isoformat() if hasattr(log['timestamp'], 'isoformat') else str(log['timestamp']),
                    "question": metadata.get('question', 'N/A'),
                    "table_used": metadata.get('table_used', 'N/A'),
                    "results_count": metadata.get('results_count', 0),
                    "execution_time": metadata.get('execution_time', 0)
                })
            except json.JSONDecodeError as e:
                logger.warning(f"Error parseando metadata de log: {str(e)}")
                # Agregar entrada con datos por defecto si hay error
                history.append({
                    "timestamp": log['timestamp'].isoformat() if hasattr(log['timestamp'], 'isoformat') else str(log['timestamp']),
                    "question": "Error al parsear",
                    "table_used": "N/A",
                    "results_count": 0,
                    "execution_time": 0
                })

        return {"history": history}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error obteniendo historial RAG: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/available-tables")
async def get_available_tables_for_rag():
    """
    Obtiene las tablas disponibles para consultas RAG
    """
    try:
        if not databricks_service.is_configured():
            raise HTTPException(
                status_code=400,
                detail="Databricks no está configurado"
            )

        if not databricks_service.connect():
            raise HTTPException(
                status_code=500,
                detail="Error conectando a Databricks"
            )

        # Obtener todas las tablas
        query = f"SHOW TABLES IN {databricks_service.catalog}.{databricks_service.schema}"
        tables = databricks_service.execute_query(query)
        databricks_service.disconnect()

        if not tables:
            return {"tables": []}

        # Filtrar tablas de usuario (excluir audit_logs y raw_data)
        user_tables = [
            {
                "name": t['tableName'],
                "type": "classified" if "_classified" in t['tableName'] else "clean" if "_clean" in t['tableName'] else "original"
            }
            for t in tables
            if t['tableName'] not in ['audit_logs', 'raw_data']
        ]

        return {"tables": user_tables}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error obteniendo tablas: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
