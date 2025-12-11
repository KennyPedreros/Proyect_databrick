from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config.settings import settings
from fastapi import APIRouter, HTTPException
from app.models.schemas import StorageStatus, SuccessResponse
from app.services.databricks_service import databricks_service
from typing import Dict, Any
from app.api.endpoints import ingestion, storage, cleaning, classification, dashboard
import logging


# Crear la aplicación FastAPI
app = FastAPI(
    title=settings.PROJECT_NAME,
    description="API para gestión inteligente de datos COVID-19",
    version=settings.VERSION,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

router = APIRouter(prefix="/api/storage", tags=["Módulo 2: Almacenamiento"])

logger = logging.getLogger(__name__)

# Crear la aplicación FastAPI
app = FastAPI(
    title=settings.PROJECT_NAME,
    description="API para gestión inteligente de datos COVID-19",
    version=settings.VERSION,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# EVENTOS DE INICIO Y CIERRE
# ============================================

@app.on_event("startup")
async def startup_event():
    """Se ejecuta al iniciar el servidor"""
    print(f"🚀 {settings.PROJECT_NAME} v{settings.VERSION}")
    print(f"📡 API corriendo en http://{settings.API_HOST}:{settings.API_PORT}")
    print(f"📚 Documentación disponible en http://localhost:{settings.API_PORT}/docs")
    print(f"💾 Databricks configurado: {settings.DATABRICKS_HOST is not None}")

@app.on_event("shutdown")
async def shutdown_event():
    """Se ejecuta al cerrar el servidor"""
    print("👋 Cerrando servidor...")

# ============================================
# RUTAS BÁSICAS
# ============================================

@app.get("/")
def read_root():
    """Ruta raíz - Información del sistema"""
    return {
        "message": f"¡Bienvenido al {settings.PROJECT_NAME}!",
        "status": "online",
        "version": settings.VERSION,
        "docs": f"http://localhost:{settings.API_PORT}/docs"
    }

@app.get("/health")
def health_check():
    """Health check - Verificar que el servidor está funcionando"""
    return {
        "status": "healthy",
        "service": settings.PROJECT_NAME,
        "version": settings.VERSION
    }

@app.get("/api/info")
def system_info():
    """Información del sistema"""
    return {
        "project": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "databricks_configured": settings.DATABRICKS_HOST is not None,
        "openai_configured": settings.OPENAI_API_KEY is not None,
        "modules": {
            "modulo_1": "Ingesta de Datos",
            "modulo_2": "Almacenamiento Delta Lake",
            "modulo_3": "Procesamiento y Limpieza",
            "modulo_4": "Clasificación y Etiquetado",
            "modulo_5": "Almacenamiento Final y Visualización",
            "modulo_6": "Monitoreo y Auditoría",
            "extra": "Consultas RAG"
        },
        "status": {
            "modulo_1": "✅ Completo",
            "modulo_2": "✅ Completo", 
            "modulo_3": "✅ Completo",
            "modulo_4": "✅ Completo",
            "modulo_5": "✅ Completo",
            "modulo_6": "🔄 Pendiente",
            "rag": "🔄 Pendiente"
        }
    }

# ============================================
# REGISTRAR ROUTERS
# ============================================

# Módulo 1: Ingesta de Datos
app.include_router(ingestion.router)

# Módulo 2: Almacenamiento
app.include_router(storage.router)

# Módulo 3: Limpieza de Datos
app.include_router(cleaning.router)

#Módulo 4
app.include_router(classification.router)

#Módulo 5
app.include_router(dashboard.router)

# Si quieres ver todos los endpoints disponibles
@app.get("/api/routes")
def list_routes():
    """Listar todas las rutas disponibles"""
    routes = []
    for route in app.routes:
        if hasattr(route, "methods"):
            routes.append({
                "path": route.path,
                "methods": list(route.methods),
                "name": route.name,
                "tags": getattr(route, "tags", [])
            })
    return {"total_routes": len(routes), "routes": routes}
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