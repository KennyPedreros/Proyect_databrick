from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    """
    Configuración central del sistema
    Carga variables desde el archivo .env
    """
    
    # API Configuration
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_RELOAD: bool = True
    PROJECT_NAME: str = "Sistema COVID-19 ESPE"
    VERSION: str = "1.0.0"
    
    # CORS Origins (Frontend)
    CORS_ORIGINS: list = ["http://localhost:3000", "http://localhost:5173", "http://127.0.0.1:3000","https://proyect-databrick.vercel.app","https://proyect-databrick-git-main-kenny-pedreros-projects.vercel.app","https://proyect-databrick-75ganxylg-kenny-pedreros-projects.vercel.app"]
    
    # Databricks Configuration
    DATABRICKS_HOST: Optional[str] = None
    DATABRICKS_TOKEN: Optional[str] = None
    DATABRICKS_CLUSTER_ID: Optional[str] = None
    DATABRICKS_CATALOG: str = "covid_catalog"
    DATABRICKS_SCHEMA: str = "covid_schema"
    
    # OpenAI Configuration
    OPENAI_API_KEY: Optional[str] = None
    
    # ChromaDB Configuration
    CHROMA_PERSIST_DIRECTORY: str = "./chroma_db"
    
    # Logging
    LOG_LEVEL: str = "INFO"
    
    class Config:
        env_file = ".env"
        case_sensitive = True

# Instancia global de configuración
settings = Settings()