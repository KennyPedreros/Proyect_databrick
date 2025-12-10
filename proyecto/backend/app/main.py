from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config.settings import settings
from app.api.endpoints import ingestion

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
        "modules": [
            "Módulo 1: Ingesta de Datos",
            "Módulo 2: Almacenamiento Inicial",
            "Módulo 3: Procesamiento y Limpieza",
            "Módulo 4: Clasificación y Etiquetado",
            "Módulo 5: Almacenamiento Final y Visualización",
            "Módulo 6: Monitoreo y Auditoría",
            "Extra: Consultas RAG"
        ]
    }
    # Registrar routers de cada módulo
app.include_router(ingestion.router)

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
                "name": route.name
            })
    return {"total_routes": len(routes), "routes": routes}