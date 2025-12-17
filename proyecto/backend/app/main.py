from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config.settings import settings
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from app.api.endpoints import (
    ingestion, 
    storage, 
    cleaning, 
    classification, 
    dashboard, 
    monitoring, 
    rag
)
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
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
    allow_origins=settings.CORS_ORIGINS + ["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=3600,
)

class LargeFileMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        request.scope["fastapi.request.max_body_size"] = 500 * 1024 * 1024  # 500MB
        return await call_next(request)

app.add_middleware(LargeFileMiddleware)
# ============================================
# EVENTOS DE INICIO Y CIERRE
# ============================================

@app.on_event("startup")
async def startup_event():
    """Se ejecuta al iniciar el servidor"""
    logger.info(f"🚀 {settings.PROJECT_NAME} v{settings.VERSION}")
    logger.info(f"📡 API corriendo en http://{settings.API_HOST}:{settings.API_PORT}")
    logger.info(f"📚 Documentación disponible en http://localhost:{settings.API_PORT}/docs")
    logger.info(f"💾 Databricks configurado: {settings.DATABRICKS_HOST is not None}")

@app.on_event("shutdown")
async def shutdown_event():
    """Se ejecuta al cerrar el servidor"""
    logger.info("👋 Cerrando servidor...")

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
            "modulo_6": "✅ Completo",
            "rag": "✅ Completo"
        }
    }

# ============================================
# REGISTRAR ROUTERS (LO MÁS IMPORTANTE)
# ============================================

# Módulo 1: Ingesta de Datos
app.include_router(ingestion.router)
logger.info("✅ Router de Ingesta registrado")

# Módulo 2: Almacenamiento
app.include_router(storage.router)
logger.info("✅ Router de Almacenamiento registrado")

# Módulo 3: Limpieza de Datos
app.include_router(cleaning.router)
logger.info("✅ Router de Limpieza registrado")

# Módulo 4: Clasificación y Etiquetado
app.include_router(classification.router)
logger.info("✅ Router de Clasificación registrado")

# Módulo 5: Dashboard y Visualización
app.include_router(dashboard.router)
logger.info("✅ Router de Dashboard registrado en /api/dashboard")

# Módulo 6: Monitoreo y Auditoría
app.include_router(monitoring.router)
logger.info("✅ Router de Monitoreo registrado")

# Extra: RAG - Consultas Inteligentes
app.include_router(rag.router)
logger.info("✅ Router de RAG registrado")

# ============================================
# UTILIDAD: LISTAR TODAS LAS RUTAS
# ============================================

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
    return {
        "total_routes": len(routes), 
        "routes": sorted(routes, key=lambda x: x["path"])
    }