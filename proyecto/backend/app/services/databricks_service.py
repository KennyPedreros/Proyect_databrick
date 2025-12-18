from databricks import sql
from app.config.settings import settings
import logging
import pandas as pd
import json
from datetime import datetime
import uuid
import re
import io

logger = logging.getLogger(__name__)


class DatabricksService:
    """Servicio dinámico para Databricks - carga masiva optimizada"""
    
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
            if not self.host or not self.token or not self.cluster_id:
                logger.warning("Databricks credentials not configured")
                return False
                
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
    
    def is_configured(self) -> bool:
        """Verifica si Databricks está configurado"""
        return bool(self.host and self.token and self.cluster_id)

    def disconnect(self):
        """Cierra la conexión"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Conexión cerrada")
            except Exception as e:
                logger.error(f"Error cerrando conexión: {str(e)}")

    def ensure_connected(self):
        """Asegura que hay conexión activa"""
        try:
            if not self.connection:
                self.connect()
                return
            
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
        except:
            logger.warning("⚠️ Conexión inválida, reconectando...")
            self.connect()
    
    def execute_query(self, query: str, params=None):
        """Ejecuta query con reconexión automática"""
        try:
            self.ensure_connected()
            cursor = self.connection.cursor()
            cursor.execute(query, params or [])
            return cursor
        except Exception as e:
            logger.error(f"❌ Error: {e}")
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
        
        clean = clean[:128]
        
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
    
    def create_audit_table(self):
        """Crea tabla de auditoría"""
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
    
    def create_dynamic_table_from_df(self, df: pd.DataFrame, table_name: str, 
                                     drop_if_exists: bool = False) -> str:
        """Crea tabla dinámicamente basada en DataFrame"""
        self.ensure_connected()
        try:
            clean_table_name = self.sanitize_table_name(table_name)
            full_table_name = f"{self.catalog}.{self.schema}.{clean_table_name}"
            
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
                columns_sql.append(f"`{clean_col}` {sql_type}")
            
            # Agregar columnas de metadata
            columns_sql.append("`_ingestion_id` STRING")
            columns_sql.append("`_processed_at` TIMESTAMP")
            
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
    
    def get_uploaded_tables(self):
        """Lista tablas disponibles"""
        try:
            self.ensure_connected()
            query = f"SHOW TABLES IN `{self.catalog}`.`{self.schema}`"
            cursor = self.execute_query(query)
            return [row['tableName'] for row in cursor.fetchall()]
        except:
            return []

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
    
    def insert_dataframe(self, df, table_name, ingestion_id=None, batch_size=5000):
        """
        ⚡ CARGA MASIVA OPTIMIZADA - INSERT directo con VALUES múltiples
        
        Estrategia: Insertar lotes grandes directamente sin tablas temporales
        """
        try:
            self.ensure_connected()
            start_time = datetime.now()
            
            clean_table_name = self.sanitize_table_name(table_name)
            full_table = f"`{self.catalog}`.`{self.schema}`.`{clean_table_name}`"
            
            # Preparar DataFrame con metadata
            df_copy = df.copy()
            if ingestion_id:
                df_copy['_ingestion_id'] = ingestion_id
                df_copy['_processed_at'] = datetime.now()
            
            # Sanitizar nombres de columnas
            original_columns = df_copy.columns.tolist()
            df_copy.columns = [self.sanitize_column_name(col) for col in df_copy.columns]
            sanitized_columns = df_copy.columns.tolist()
            
            # Preparar query base
            cols_str = ', '.join([f'`{col}`' for col in sanitized_columns])
            
            total_rows = len(df_copy)
            inserted = 0
            failed = 0
            
            logger.info(f"📊 Insertando {total_rows:,} filas en lotes de {batch_size:,}...")
            
            # Insertar en lotes
            for i in range(0, total_rows, batch_size):
                try:
                    batch = df_copy.iloc[i:i + batch_size]
                    
                    # Construir VALUES múltiples
                    values_list = []
                    for _, row in batch.iterrows():
                        values = []
                        for val in row:
                            if pd.isna(val):
                                values.append('NULL')
                            elif isinstance(val, str):
                                # Escapar caracteres especiales
                                safe_val = str(val).replace("\\", "\\\\").replace("'", "''")
                                # Limitar longitud para evitar queries muy grandes
                                if len(safe_val) > 10000:
                                    safe_val = safe_val[:10000]
                                values.append(f"'{safe_val}'")
                            elif isinstance(val, (int, float)):
                                values.append(str(val))
                            elif isinstance(val, datetime):
                                values.append(f"TIMESTAMP '{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                            else:
                                safe_val = str(val).replace("'", "''")
                                values.append(f"'{safe_val}'")
                        
                        values_list.append(f"({', '.join(values)})")
                    
                    # INSERT masivo en un solo query
                    insert_query = f"""
                    INSERT INTO {full_table} ({cols_str})
                    VALUES {', '.join(values_list)}
                    """
                    
                    self.execute_query(insert_query)
                    inserted += len(batch)
                    
                    # Mostrar progreso
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = inserted / elapsed if elapsed > 0 else 0
                    progress = (inserted / total_rows) * 100
                    
                    logger.info(f"⚡ {inserted:,}/{total_rows:,} ({progress:.1f}%) - {rate:.0f} filas/s")
                    
                except Exception as batch_error:
                    logger.error(f"❌ Error en lote {i}-{i+batch_size}: {batch_error}")
                    failed += len(batch)
                    # Continuar con el siguiente lote
                    continue
            
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ {inserted:,} registros insertados en {elapsed:.1f}s")
            
            if failed > 0:
                logger.warning(f"⚠️ {failed:,} registros fallaron")
            
            avg_rate = inserted / elapsed if elapsed > 0 else 0
            logger.info(f"⚡ Velocidad promedio: {avg_rate:.0f} filas/s")
            
            return {
                "success": inserted, 
                "failed": failed, 
                "elapsed": elapsed,
                "rate": avg_rate
            }
            
        except Exception as e:
            logger.error(f"❌ Error en insert_dataframe: {e}", exc_info=True)
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
            cursor = self.execute_query(query)
            result = cursor.fetchone()
            return result['count'] if result else 0
        except Exception as e:
            logger.error(f"Error contando: {str(e)}")
            return 0

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
    
    def setup_database(self):
        """Setup inicial"""
        logger.info("🔧 Configurando BD...")
        try:
            self.create_catalog_and_schema()
            self.create_raw_table()
            self.create_audit_table()
            logger.info("✅ BD configurada")
            return True
        except Exception as e:
            logger.error(f"Error setup: {str(e)}")
            return False


# Instancia global
databricks_service = DatabricksService()