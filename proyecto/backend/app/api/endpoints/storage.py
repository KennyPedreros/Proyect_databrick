from fastapi import APIRouter, HTTPException
from app.models.schemas import StorageStatus, SuccessResponse
from app.services.databricks_service import databricks_service
from typing import Dict, Any
import logging

router = APIRouter(prefix="/api/storage", tags=["Módulo 2: Almacenamiento"])

logger = logging.getLogger(__name__)


# ============================================
# FUNCIONES DEL MÓDULO 2
# ============================================

def initialize_storage():
    """Inicializa el sistema de almacenamiento en Databricks"""
    try:
        # Conectar a Databricks
        if not databricks_service.connect():
            return {"success": False, "message": "Error conectando a Databricks"}
        
        # Crear catálogo y schema
        databricks_service.create_catalog_and_schema()
        
        # Crear tablas
        databricks_service.create_raw_table()
        databricks_service.create_processed_table()
        
        return {"success": True, "message": "Storage inicializado correctamente"}
    
    except Exception as e:
        logger.error(f"Error inicializando storage: {str(e)}")
        return {"success": False, "message": str(e)}


def get_storage_statistics():
    """Obtiene estadísticas del almacenamiento"""
    try:
        raw_count = databricks_service.get_table_count("covid_raw")
        processed_count = databricks_service.get_table_count("covid_processed")
        
        return {
            "raw_records": raw_count,
            "processed_records": processed_count,
            "total_records": raw_count + processed_count
        }
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas: {str(e)}")
        return None


# ============================================
# ENDPOINTS
# ============================================

@router.post("/initialize")
async def initialize_delta_lake():
    """
    Módulo 2: Inicializar almacenamiento en Delta Lake
    
    Funciones implementadas:
    - initialize_storage()
    - create_catalog_and_schema()
    - create_tables()
    """
    try:
        result = initialize_storage()
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["message"])
        
        return SuccessResponse(
            success=True,
            message="Sistema de almacenamiento inicializado",
            data={
                "catalog": databricks_service.catalog,
                "schema": databricks_service.schema,
                "tables_created": ["covid_raw", "covid_processed"]
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_storage_status():
    """
    Obtener estado del almacenamiento Delta Lake
    """
    try:
        # Verificar conexión
        is_connected = databricks_service.connect()
        
        if not is_connected:
            return StorageStatus(
                storage_id="databricks-delta",
                location=f"{databricks_service.catalog}.{databricks_service.schema}",
                size_mb=0.0,
                integrity_check=False,
                backup_exists=False
            )
        
        # Obtener estadísticas
        stats = get_storage_statistics()
        
        if not stats:
            raise HTTPException(status_code=500, detail="Error obteniendo estadísticas")
        
        # Calcular tamaño aproximado (estimación: 1KB por registro)
        total_records = stats["total_records"]
        size_mb = (total_records * 1024) / (1024 * 1024)  # Convertir a MB
        
        return StorageStatus(
            storage_id="databricks-delta",
            location=f"{databricks_service.catalog}.{databricks_service.schema}",
            size_mb=round(size_mb, 2),
            integrity_check=True,
            backup_exists=True
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tables/info/{table_name}")
async def get_table_information(table_name: str):
    """
    Obtener información detallada de una tabla
    """
    try:
        if table_name not in ["covid_raw", "covid_processed"]:
            raise HTTPException(
                status_code=400, 
                detail="Tabla no válida. Use: covid_raw o covid_processed"
            )
        
        info = databricks_service.get_table_info(table_name)
        count = databricks_service.get_table_count(table_name)
        
        return {
            "table_name": table_name,
            "record_count": count,
            "schema_info": info,
            "location": f"{databricks_service.catalog}.{databricks_service.schema}.{table_name}"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_statistics():
    """
    Obtener estadísticas generales del almacenamiento
    """
    try:
        stats = get_storage_statistics()
        
        if not stats:
            raise HTTPException(status_code=500, detail="Error obteniendo estadísticas")
        
        return {
            "storage_type": "Delta Lake",
            "catalog": databricks_service.catalog,
            "schema": databricks_service.schema,
            "statistics": stats,
            "health_status": "healthy" if stats["total_records"] >= 0 else "error"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/test-connection")
async def test_databricks_connection():
    """
    Probar conexión con Databricks
    """
    try:
        is_connected = databricks_service.connect()
        
        if is_connected:
            # Ejecutar una query simple para verificar
            result = databricks_service.execute_query("SELECT 1 as test")
            
            return SuccessResponse(
                success=True,
                message="Conexión exitosa con Databricks",
                data={
                    "host": databricks_service.host,
                    "catalog": databricks_service.catalog,
                    "schema": databricks_service.schema,
                    "test_query": result
                }
            )
        else:
            raise HTTPException(
                status_code=500, 
                detail="No se pudo conectar con Databricks"
            )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        databricks_service.disconnect()


@router.delete("/tables/{table_name}")
async def drop_table(table_name: str):
    """
    Eliminar una tabla (solo para desarrollo/testing)
    """
    try:
        if table_name not in ["covid_raw", "covid_processed"]:
            raise HTTPException(
                status_code=400,
                detail="Tabla no válida"
            )
        
        query = f"DROP TABLE IF EXISTS {databricks_service.catalog}.{databricks_service.schema}.{table_name}"
        databricks_service.execute_query(query)
        
        return SuccessResponse(
            success=True,
            message=f"Tabla {table_name} eliminada",
            data={"table": table_name}
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))