from databricks import sql
from databricks.sdk import WorkspaceClient
from app.config.settings import settings
import logging

logger = logging.getLogger(__name__)


class DatabricksService:
    """Servicio para interactuar con Databricks y Delta Lake"""
    
    def __init__(self):
        self.host = settings.DATABRICKS_HOST
        self.token = settings.DATABRICKS_TOKEN
        self.cluster_id = settings.DATABRICKS_CLUSTER_ID
        self.catalog = settings.DATABRICKS_CATALOG
        self.schema = settings.DATABRICKS_SCHEMA
        self.connection = None
        
    def connect(self):
        """Establece conexión con Databricks SQL Warehouse"""
        try:
            self.connection = sql.connect(
                server_hostname=self.host,
                http_path=f"/sql/1.0/warehouses/{self.cluster_id}",
                access_token=self.token
            )
            logger.info("✅ Conexión exitosa con Databricks")
            return True
        except Exception as e:
            logger.error(f"❌ Error conectando a Databricks: {str(e)}")
            return False
    
    def disconnect(self):
        """Cierra la conexión"""
        if self.connection:
            self.connection.close()
            logger.info("Conexión cerrada")
    
    def execute_query(self, query: str):
        """Ejecuta una consulta SQL y retorna resultados"""
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Obtener resultados
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            results = [dict(zip(columns, row)) for row in rows]
            cursor.close()
            
            return results
        except Exception as e:
            logger.error(f"Error ejecutando query: {str(e)}")
            raise
    
    def create_catalog_and_schema(self):
        """Crea el catálogo y schema si no existen"""
        queries = [
            f"CREATE CATALOG IF NOT EXISTS {self.catalog}",
            f"USE CATALOG {self.catalog}",
            f"CREATE SCHEMA IF NOT EXISTS {self.schema}",
            f"USE SCHEMA {self.schema}"
        ]
        
        for query in queries:
            try:
                self.execute_query(query)
                logger.info(f"✅ Ejecutado: {query}")
            except Exception as e:
                logger.error(f"Error en query: {query} - {str(e)}")
    
    def create_raw_table(self):
        """Crea la tabla RAW para datos sin procesar"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.covid_raw (
            ingestion_id STRING,
            filename STRING,
            raw_data STRING,
            uploaded_at TIMESTAMP,
            record_count INT
        )
        USING DELTA
        LOCATION 'dbfs:/mnt/covid/raw/'
        """
        
        try:
            self.execute_query(query)
            logger.info("✅ Tabla RAW creada/verificada")
        except Exception as e:
            logger.error(f"Error creando tabla RAW: {str(e)}")
            raise
    
    def create_processed_table(self):
        """Crea la tabla para datos procesados"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.covid_processed (
            case_id STRING,
            date DATE,
            country STRING,
            region STRING,
            age INT,
            gender STRING,
            symptoms STRING,
            severity STRING,
            vaccinated BOOLEAN,
            outcome STRING,
            processed_at TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (date)
        LOCATION 'dbfs:/mnt/covid/processed/'
        """
        
        try:
            self.execute_query(query)
            logger.info("✅ Tabla PROCESSED creada/verificada")
        except Exception as e:
            logger.error(f"Error creando tabla PROCESSED: {str(e)}")
            raise
    
    def insert_raw_data(self, ingestion_id: str, filename: str, 
                       raw_data: str, record_count: int):
        """Inserta datos crudos en la tabla RAW"""
        query = f"""
        INSERT INTO {self.catalog}.{self.schema}.covid_raw
        VALUES (
            '{ingestion_id}',
            '{filename}',
            '{raw_data}',
            current_timestamp(),
            {record_count}
        )
        """
        
        try:
            self.execute_query(query)
            logger.info(f"✅ Datos insertados en RAW: {ingestion_id}")
            return True
        except Exception as e:
            logger.error(f"Error insertando en RAW: {str(e)}")
            return False
    
    def get_table_info(self, table_name: str):
        """Obtiene información de una tabla"""
        query = f"""
        DESCRIBE EXTENDED {self.catalog}.{self.schema}.{table_name}
        """
        
        try:
            results = self.execute_query(query)
            return results
        except Exception as e:
            logger.error(f"Error obteniendo info de tabla: {str(e)}")
            return None
    
    def get_table_count(self, table_name: str) -> int:
        """Obtiene el conteo de registros de una tabla"""
        query = f"""
        SELECT COUNT(*) as count 
        FROM {self.catalog}.{self.schema}.{table_name}
        """
        
        try:
            results = self.execute_query(query)
            return results[0]['count'] if results else 0
        except Exception as e:
            logger.error(f"Error contando registros: {str(e)}")
            return 0


# Instancia global del servicio
databricks_service = DatabricksService()