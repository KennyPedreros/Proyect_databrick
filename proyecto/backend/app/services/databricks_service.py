from databricks import sql
from app.config.settings import settings
import logging
import pandas as pd
import json
from datetime import datetime
import uuid
import re
import os

logger = logging.getLogger(__name__)


class DatabricksService:
    """Servicio dinámico para Databricks - crea tablas basadas en CSV"""
    
    def __init__(self):
        # Cargar desde settings primero, luego intentar directo desde OS si falla
        self.host = settings.DATABRICKS_HOST or os.getenv('DATABRICKS_HOST')
        self.token = settings.DATABRICKS_TOKEN or os.getenv('DATABRICKS_TOKEN')
        self.cluster_id = settings.DATABRICKS_CLUSTER_ID or os.getenv('DATABRICKS_CLUSTER_ID')
        self.catalog = settings.DATABRICKS_CATALOG or os.getenv('DATABRICKS_CATALOG', 'covid_catalog')
        self.schema = settings.DATABRICKS_SCHEMA or os.getenv('DATABRICKS_SCHEMA', 'covid_schema')
        self.connection = None
        
        # Log de debug para verificar configuración al inicializar
        self._log_configuration_status()
    
    def _log_configuration_status(self):
        """Log de estado de configuración (solo para debug)"""
        if self.is_configured():
            logger.info(f"✅ Databricks configurado: {self.host[:20]}...")
        else:
            logger.error("❌ Databricks NO configurado correctamente")
            logger.error(f"   Host: {'✓' if self.host else '✗'}")
            logger.error(f"   Token: {'✓' if self.token else '✗'}")
            logger.error(f"   Cluster ID: {'✓' if self.cluster_id else '✗'}")
    
    def is_configured(self) -> bool:
        """
        Verifica si Databricks está configurado
        DEBE tener las 3 credenciales básicas
        """
        has_host = bool(self.host and self.host.strip() and self.host != 'None')
        has_token = bool(self.token and self.token.strip() and self.token != 'None')
        has_cluster = bool(self.cluster_id and self.cluster_id.strip() and self.cluster_id != 'None')
        
        configured = has_host and has_token and has_cluster
        
        if not configured:
            logger.warning("⚠️ Databricks no configurado completamente:")
            logger.warning(f"   - Host: {self.host if has_host else 'FALTA'}")
            logger.warning(f"   - Token: {'Configurado' if has_token else 'FALTA'}")
            logger.warning(f"   - Cluster ID: {self.cluster_id if has_cluster else 'FALTA'}")
        
        return configured
        
    def connect(self):
        """Establece conexión con Databricks SQL Warehouse"""
        # Verificar configuración ANTES de intentar conectar
        if not self.is_configured():
            logger.error("❌ No se puede conectar: Databricks no configurado")
            return False
        
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
            try:
                self.connection.close()
                logger.debug("Conexión cerrada")
            except Exception as e:
                logger.error(f"Error cerrando conexión: {str(e)}")
    
    def ensure_connected(self):
        """
        Asegura que hay conexión activa
        Si no hay, intenta reconectar
        """
        if not self.connection:
            return self.connect()
        return True
    
    def execute_query(self, query: str):
        """Ejecuta una consulta SQL y retorna resultados"""
        if not self.ensure_connected():
            return []
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Obtener resultados
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                results = [dict(zip(columns, row)) for row in rows]
            else:
                results = []
            
            cursor.close()
            return results
            
        except Exception as e:
            logger.error(f"Error ejecutando query: {str(e)}")
            logger.error(f"Query: {query}")
            raise
    
    def sanitize_column_name(self, column_name: str) -> str:
        """Limpia nombres de columnas para SQL"""
        clean = str(column_name).lower().strip()
        clean = re.sub(r'[^\w\s]', '_', clean)
        clean = re.sub(r'\s+', '_', clean)
        clean = re.sub(r'\+', '', clean)
        clean = clean.strip('_')
        
        if clean and clean[0].isdigit():
            clean = f'col_{clean}'
        
        return clean if clean else 'unnamed_column'
    
    def sanitize_table_name(self, filename: str) -> str:
        """Genera nombre de tabla válido desde nombre de archivo"""
        name = filename.replace('.csv', '').replace('.CSV', '')
        name = name.replace('.xlsx', '').replace('.xls', '').replace('.json', '')
        name = self.sanitize_column_name(name)
        return name
    
    def infer_sql_type(self, dtype, sample_values) -> str:
        """Infiere el tipo SQL desde pandas dtype"""
        dtype_str = str(dtype)
        
        non_null_samples = [v for v in sample_values if pd.notna(v)]
        
        if 'int' in dtype_str:
            return 'BIGINT'
        elif 'float' in dtype_str:
            return 'DOUBLE'
        elif 'bool' in dtype_str:
            return 'BOOLEAN'
        elif 'datetime' in dtype_str or 'date' in dtype_str:
            return 'TIMESTAMP'
        else:
            if non_null_samples:
                sample_str = str(non_null_samples[0])
                if re.match(r'\d{4}-\d{2}-\d{2}', sample_str):
                    return 'DATE'
                elif re.match(r'\d{2}/\d{2}/\d{4}', sample_str):
                    return 'DATE'
            
            return 'STRING'
    
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
        """Crea tabla RAW genérica"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.raw_data (
            ingestion_id STRING,
            table_name STRING,
            filename STRING,
            raw_data STRING,
            uploaded_at TIMESTAMP,
            record_count INT,
            column_info STRING
        )
        USING DELTA
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
            outcome STRING,
            vaccinated BOOLEAN,
            medical_history STRING,
            classification_confidence DOUBLE,
            classified_at TIMESTAMP,
            processed_at TIMESTAMP
        )
        USING DELTA
        """
        
        try:
            self.execute_query(query)
            logger.info("✅ Tabla PROCESSED creada/verificada")
        except Exception as e:
            logger.error(f"Error creando tabla PROCESSED: {str(e)}")
            raise
    
    def create_audit_table(self):
        """Crea tabla para logs de auditoría"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.audit_logs (
            event_id STRING,
            timestamp TIMESTAMP,
            process STRING,
            level STRING,
            message STRING,
            metadata STRING,
            user_id STRING
        )
        USING DELTA
        """
        
        try:
            self.execute_query(query)
            logger.info("✅ Tabla AUDIT creada/verificada")
        except Exception as e:
            logger.error(f"Error creando tabla AUDIT: {str(e)}")
            raise
    
    def setup_database(self):
        """
        ✅ MÉTODO FALTANTE #1
        Setup inicial completo de la base de datos
        """
        logger.info("🔧 Configurando base de datos...")
        try:
            self.create_catalog_and_schema()
            self.create_raw_table()
            self.create_processed_table()
            self.create_audit_table()
            logger.info("✅ Base de datos configurada exitosamente")
            return True
        except Exception as e:
            logger.error(f"Error en setup de BD: {str(e)}")
            return False
    
    def create_dynamic_table_from_df(self, df: pd.DataFrame, table_name: str, 
                                     drop_if_exists: bool = False) -> str:
        """
        Crea tabla dinámicamente basada en DataFrame
        
        Returns:
            str: Nombre limpio de la tabla creada
        """
        try:
            clean_table_name = self.sanitize_table_name(table_name)
            full_table_name = f"{self.catalog}.{self.schema}.{clean_table_name}"
            
            # Eliminar si se solicita
            if drop_if_exists:
                drop_query = f"DROP TABLE IF EXISTS {full_table_name}"
                self.execute_query(drop_query)
                logger.info(f"🗑️ Tabla {clean_table_name} eliminada")
            
            # Generar esquema dinámico
            columns_sql = []
            for col in df.columns:
                clean_col = self.sanitize_column_name(col)
                sample_values = df[col].head(100).tolist()
                sql_type = self.infer_sql_type(df[col].dtype, sample_values)
                columns_sql.append(f"{clean_col} {sql_type}")
            
            # Metadatos
            columns_sql.append("_ingestion_id STRING")
            columns_sql.append("_processed_at TIMESTAMP")
            
            create_query = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                {', '.join(columns_sql)}
            )
            USING DELTA
            """
            
            self.execute_query(create_query)
            logger.info(f"✅ Tabla '{clean_table_name}' creada con {len(df.columns)} columnas")
            
            return clean_table_name
            
        except Exception as e:
            logger.error(f"Error creando tabla dinámica: {str(e)}")
            raise
    
    def insert_raw_data(self, table_name: str, filename: str, 
                       df: pd.DataFrame, ingestion_id: str) -> bool:
        """Guarda muestra en tabla RAW"""
        try:
            column_info = {
                col: {
                    'dtype': str(df[col].dtype),
                    'sample': str(df[col].head(3).tolist())
                }
                for col in df.columns
            }
            
            column_info_json = json.dumps(column_info).replace("'", "''")
            raw_sample = df.head(10).to_json(orient='records').replace("'", "''")
            
            query = f"""
            INSERT INTO {self.catalog}.{self.schema}.raw_data
            VALUES (
                '{ingestion_id}',
                '{table_name}',
                '{filename}',
                '{raw_sample}',
                current_timestamp(),
                {len(df)},
                '{column_info_json}'
            )
            """
            
            self.execute_query(query)
            logger.info(f"✅ RAW guardado: {ingestion_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error insertando RAW: {str(e)}")
            return False
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        ingestion_id: str, batch_size: int = 1000) -> dict:
        """
        Inserta DataFrame completo en tabla
        """
        clean_table_name = self.sanitize_table_name(table_name)
        full_table_name = f"{self.catalog}.{self.schema}.{clean_table_name}"
        
        total_records = len(df)
        success_count = 0
        error_count = 0
        
        try:
            # Limpiar columnas
            df_clean = df.copy()
            df_clean.columns = [self.sanitize_column_name(col) for col in df_clean.columns]
            
            # Metadatos
            df_clean['_ingestion_id'] = ingestion_id
            df_clean['_processed_at'] = datetime.now()
            
            # Insertar por lotes
            for i in range(0, len(df_clean), batch_size):
                batch = df_clean.iloc[i:i+batch_size]
                
                try:
                    values_list = []
                    for _, row in batch.iterrows():
                        values = []
                        for col in batch.columns:
                            val = row[col]
                            
                            if pd.isna(val):
                                values.append('NULL')
                            elif isinstance(val, bool):
                                values.append('TRUE' if val else 'FALSE')
                            elif isinstance(val, (int, float)):
                                values.append(str(val))
                            elif isinstance(val, datetime):
                                values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                            else:
                                str_val = str(val).replace("'", "''").replace("\\", "\\\\")
                                values.append(f"'{str_val}'")
                        
                        values_list.append(f"({', '.join(values)})")
                    
                    insert_query = f"""
                    INSERT INTO {full_table_name}
                    VALUES {', '.join(values_list)}
                    """
                    
                    self.execute_query(insert_query)
                    success_count += len(batch)
                    
                    if (i + batch_size) % 5000 == 0:
                        logger.info(f"   Progreso: {success_count:,}/{total_records:,}")
                
                except Exception as e:
                    logger.error(f"Error en lote {i}: {str(e)}")
                    error_count += len(batch)
            
            logger.info(f"✅ {success_count:,} registros insertados")
            
            return {
                'total': total_records,
                'success': success_count,
                'errors': error_count,
                'table_name': clean_table_name
            }
            
        except Exception as e:
            logger.error(f"Error insertando DataFrame: {str(e)}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """Verifica si tabla existe"""
        try:
            clean_table_name = self.sanitize_table_name(table_name)
            query = f"DESCRIBE {self.catalog}.{self.schema}.{clean_table_name}"
            self.execute_query(query)
            return True
        except:
            return False
    
    def get_table_count(self, table_name: str) -> int:
        """Cuenta registros"""
        try:
            clean_table_name = self.sanitize_table_name(table_name)
            query = f"SELECT COUNT(*) as count FROM {self.catalog}.{self.schema}.{clean_table_name}"
            results = self.execute_query(query)
            return results[0]['count'] if results else 0
        except Exception as e:
            logger.error(f"Error contando: {str(e)}")
            return 0
    
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
    
    def insert_audit_log(self, process: str, level: str, message: str,
                        metadata: dict = None, user_id: str = None) -> bool:
        """Log de auditoría"""
        try:
            event_id = str(uuid.uuid4())
            metadata_str = json.dumps(metadata).replace("'", "''") if metadata else 'NULL'
            user_str = f"'{user_id}'" if user_id else 'NULL'
            message_clean = message.replace("'", "''")
            
            query = f"""
            INSERT INTO {self.catalog}.{self.schema}.audit_logs
            VALUES (
                '{event_id}',
                current_timestamp(),
                '{process}',
                '{level}',
                '{message_clean}',
                {f"'{metadata_str}'" if metadata else 'NULL'},
                {user_str}
            )
            """
            
            self.execute_query(query)
            return True
        except Exception as e:
            logger.error(f"Error audit log: {str(e)}")
            return False


# Instancia global
databricks_service = DatabricksService()