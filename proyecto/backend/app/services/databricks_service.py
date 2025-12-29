from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import files
from app.config.settings import settings
import logging
import pandas as pd
import json
import io
import tempfile
import os
from typing import Optional, Dict, Any
import uuid
import re
from datetime import datetime

logger = logging.getLogger(__name__)


class DatabricksService:
    """
    🚀 Servicio ULTRA-OPTIMIZADO con COPY INTO
    200,000 registros en ~30 segundos
    """
    
    def __init__(self):
        # Configuración básica
        self.host = settings.DATABRICKS_HOST or os.getenv('DATABRICKS_HOST')
        self.token = settings.DATABRICKS_TOKEN or os.getenv('DATABRICKS_TOKEN')
        self.cluster_id = settings.DATABRICKS_CLUSTER_ID or os.getenv('DATABRICKS_CLUSTER_ID')
        self.catalog = settings.DATABRICKS_CATALOG or os.getenv('DATABRICKS_CATALOG', 'covid_catalog')
        self.schema = settings.DATABRICKS_SCHEMA or os.getenv('DATABRICKS_SCHEMA', 'covid_schema')
        
        # Conexiones
        self.sql_connection = None
        self.workspace_client = None
        
        # Path para Volumes (mejor que DBFS)
        self.volume_path = f"/Volumes/{self.catalog}/{self.schema}/uploads"
        
        self._log_configuration_status()
    
    def _log_configuration_status(self):
        """Log de estado de configuración"""
        if self.is_configured():
            logger.info(f"✅ Databricks configurado: {self.host[:20]}...")
        else:
            logger.error("❌ Databricks NO configurado correctamente")
    
    def is_configured(self) -> bool:
        """Verifica si Databricks está configurado"""
        has_host = bool(self.host and self.host.strip() and self.host != 'None')
        has_token = bool(self.token and self.token.strip() and self.token != 'None')
        has_cluster = bool(self.cluster_id and self.cluster_id.strip() and self.cluster_id != 'None')
        return has_host and has_token and has_cluster
    
    def get_workspace_client(self) -> WorkspaceClient:
        """Obtiene cliente del Workspace"""
        if not self.workspace_client:
            try:
                self.workspace_client = WorkspaceClient(
                    host=f"https://{self.host}",
                    token=self.token
                )
                logger.info("✅ Workspace Client conectado")
            except Exception as e:
                logger.error(f"Error creando Workspace Client: {str(e)}")
                raise
        return self.workspace_client
    
    def connect(self):
        """Establece conexión SQL"""
        if not self.is_configured():
            logger.error("❌ No se puede conectar: Databricks no configurado")
            return False
        
        try:
            self.sql_connection = sql.connect(
                server_hostname=self.host,
                http_path=f"/sql/1.0/warehouses/{self.cluster_id}",
                access_token=self.token
            )
            logger.info("✅ Conexión SQL exitosa")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error conectando: {str(e)}")
            return False
    
    def disconnect(self):
        """Cierra la conexión SQL"""
        if self.sql_connection:
            try:
                self.sql_connection.close()
                logger.debug("Conexión SQL cerrada")
            except Exception as e:
                logger.error(f"Error cerrando conexión: {str(e)}")
    
    def ensure_connected(self):
        """Asegura que hay conexión SQL activa"""
        if not self.sql_connection:
            return self.connect()
        return True
    
    def execute_query(self, query: str):
        """Ejecuta una consulta SQL y retorna resultados"""
        if not self.ensure_connected():
            return []
        
        try:
            cursor = self.sql_connection.cursor()
            cursor.execute(query)
            
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                results = [dict(zip(columns, row)) for row in rows]
            else:
                results = []
            
            cursor.close()
            return results
            
        except Exception as e:
            # Solo log debug para queries que fallan (pueden ser errores esperados como columnas que no existen)
            logger.debug(f"Query falló: {str(e)}")
            raise
    
    def fetch_one(self, query: str):
        """Ejecuta query y retorna un solo resultado"""
        results = self.execute_query(query)
        return results[0] if results else {}
    
    def fetch_all(self, query: str):
        """Ejecuta query y retorna todos los resultados"""
        return self.execute_query(query)
    
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
    
    # ========== SETUP DE BASE DE DATOS ==========
    
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
    
    def create_volume(self):
        """Crea Volume para almacenar archivos temporales"""
        try:
            query = f"""
            CREATE VOLUME IF NOT EXISTS {self.catalog}.{self.schema}.uploads
            """
            self.execute_query(query)
            logger.info("✅ Volume 'uploads' creado/verificado")
        except Exception as e:
            logger.warning(f"⚠️ No se pudo crear Volume (puede no estar disponible): {str(e)}")
            # No es crítico, podemos usar DBFS como fallback
    
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
        """Setup inicial completo de la base de datos"""
        logger.info("🔧 Configurando base de datos...")
        try:
            self.create_catalog_and_schema()
            self.create_volume()
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
        """Crea tabla dinámicamente basada en DataFrame"""
        try:
            # Asegurar conexión activa
            if not self.connect():
                raise Exception("No se pudo conectar a Databricks")

            clean_table_name = self.sanitize_table_name(table_name)
            full_table_name = f"{self.catalog}.{self.schema}.{clean_table_name}"

            logger.info(f"🔨 Creando tabla: {full_table_name}")

            # Generar esquema dinámico con columnas SANITIZADAS
            # (Importante: debe coincidir con las columnas del CSV que se subirá)
            columns_sql = []
            for col in df.columns:
                clean_col = self.sanitize_column_name(col)
                sample_values = df[col].head(100).tolist()
                sql_type = self.infer_sql_type(df[col].dtype, sample_values)
                columns_sql.append(f"{clean_col} {sql_type}")

            # Metadatos
            columns_sql.append("_ingestion_id STRING")
            columns_sql.append("_processed_at TIMESTAMP")

            # Usar CREATE OR REPLACE para evitar problemas de merge en Delta
            if drop_if_exists:
                create_query = f"""
                CREATE OR REPLACE TABLE {full_table_name} (
                    {', '.join(columns_sql)}
                )
                USING DELTA
                """
                logger.info(f"🔄 Recreando tabla completa (CREATE OR REPLACE)")
            else:
                create_query = f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    {', '.join(columns_sql)}
                )
                USING DELTA
                """

            logger.info(f"📝 Ejecutando CREATE TABLE...")
            self.execute_query(create_query)

            # Verificar que la tabla realmente existe
            verify_query = f"DESCRIBE TABLE {full_table_name}"
            result = self.execute_query(verify_query)

            if result:
                logger.info(f"✅ Tabla '{clean_table_name}' creada y verificada con {len(result)} columnas")
            else:
                raise Exception(f"Tabla {full_table_name} no existe después de CREATE")

            return clean_table_name

        except Exception as e:
            logger.error(f"❌ Error creando tabla dinámica '{table_name}': {str(e)}")
            raise
    
    # ========== 🚀 MÉTODO ULTRA-RÁPIDO: COPY INTO ==========
    
    def upload_csv_to_volume(self, df: pd.DataFrame, filename: str) -> tuple[str, bool]:
        """
        Sube CSV a Databricks Volume/DBFS
        
        Returns:
            (path, use_volume) - Path del archivo y si se usó Volume o DBFS
        """
        try:
            # Convertir DataFrame a CSV en memoria
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            csv_bytes = csv_content.encode('utf-8')
            
            logger.info(f"📦 CSV generado en memoria: {len(csv_bytes) / (1024*1024):.2f} MB")
            
            # Intentar subir a Volume primero (más rápido y moderno)
            try:
                client = self.get_workspace_client()
                
                # Path en Volume
                volume_file_path = f"{self.volume_path}/{filename}"
                
                logger.info(f"📤 Subiendo a Volume: {volume_file_path}")
                
                # Usar Files API para subir
                client.files.upload(
                    file_path=volume_file_path,
                    contents=io.BytesIO(csv_bytes),
                    overwrite=True
                )
                
                logger.info(f"✅ Subido a Volume exitosamente")
                return volume_file_path, True
                
            except Exception as volume_error:
                logger.warning(f"⚠️ Volume no disponible: {str(volume_error)}")
                logger.info("🔄 Intentando con DBFS como fallback...")
                
                # Fallback: DBFS
                dbfs_path = f"/tmp/covid_ingestion/{filename}"
                
                # Guardar temporalmente
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp:
                    tmp.write(csv_content)
                    tmp_path = tmp.name
                
                try:
                    # Subir a DBFS usando workspace client
                    with open(tmp_path, 'rb') as f:
                        client.dbfs.upload(
                            path=dbfs_path,
                            contents=f,
                            overwrite=True
                        )
                    
                    logger.info(f"✅ Subido a DBFS: {dbfs_path}")
                    return f"dbfs:{dbfs_path}", False
                
                finally:
                    # Limpiar archivo temporal
                    try:
                        os.unlink(tmp_path)
                    except:
                        pass
        
        except Exception as e:
            logger.error(f"❌ Error subiendo archivo: {str(e)}")
            raise
    
    def insert_dataframe_ultra_fast(self, df: pd.DataFrame, table_name: str,
                                    ingestion_id: str) -> Dict[str, Any]:
        """
        📊 BULK INSERT OPTIMIZADO

        Proceso:
        1. Prepara DataFrame con metadatos
        2. Inserta en lotes de 20,000 registros
        3. ~200,000 filas en ~2-3 minutos

        Método confiable y probado ✅
        """
        # table_name ya viene sanitizado desde create_dynamic_table_from_df
        clean_table_name = table_name
        full_table_name = f"{self.catalog}.{self.schema}.{clean_table_name}"

        total_records = len(df)
        start_time = datetime.now()

        logger.info(f"📊 BULK INSERT: Procesando {total_records:,} registros")
        logger.info(f"   Tabla destino: {full_table_name}")

        # Preparar DataFrame
        df_clean = df.copy()
        df_clean.columns = [self.sanitize_column_name(col) for col in df_clean.columns]
        df_clean['_ingestion_id'] = ingestion_id
        df_clean['_processed_at'] = datetime.now()

        # Ejecutar BULK INSERT
        return self._insert_bulk_optimized(
            df_clean,
            full_table_name,
            clean_table_name,
            total_records,
            start_time
        )
    
    def _insert_bulk_optimized(self, df: pd.DataFrame, full_table_name: str,
                               clean_table_name: str, total_records: int,
                               start_time: datetime) -> Dict[str, Any]:
        """
        Fallback: INSERT con lotes grandes (20,000 registros)
        Más lento que COPY INTO pero aún eficiente
        """
        logger.info("📊 Usando BULK INSERT (20,000 filas por lote)")

        chunk_size = 20000  # Aumentado para reducir número de operaciones
        success_count = 0

        # Obtener nombres de columnas en el orden correcto
        column_names = ', '.join(df.columns.tolist())

        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]

            # Asegurar conexión activa antes de cada lote
            try:
                self.ensure_connected()
            except Exception as conn_error:
                logger.warning(f"⚠️ Reconectando: {str(conn_error)}")
                self.connect()

            # Construir VALUES de forma optimizada
            values_rows = []
            for _, row in chunk.iterrows():
                values = []
                for col in chunk.columns:
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

                values_rows.append(f"({','.join(values)})")

            # Insertar lote CON MAPEO EXPLÍCITO DE COLUMNAS
            insert_query = f"INSERT INTO {full_table_name} ({column_names}) VALUES {','.join(values_rows)}"

            try:
                self.execute_query(insert_query)
                success_count += len(chunk)

                progress_pct = (success_count / total_records) * 100
                logger.info(f"   📊 Progreso: {success_count:,}/{total_records:,} ({progress_pct:.1f}%)")

            except Exception as e:
                logger.error(f"❌ Error en lote {i}: {str(e)}")
                # Intentar reconectar y reintentar UNA vez
                logger.info("🔄 Intentando reconectar y reintentar...")
                try:
                    self.disconnect()
                    self.connect()
                    self.execute_query(insert_query)
                    success_count += len(chunk)
                    logger.info(f"✅ Lote {i} reintentado exitosamente")
                except Exception as retry_error:
                    logger.error(f"❌ Fallo reintento en lote {i}: {str(retry_error)}")
                    # Continuar con el siguiente lote
        
        elapsed = (datetime.now() - start_time).total_seconds()
        records_per_sec = success_count / elapsed if elapsed > 0 else 0
        
        logger.info(f"✅ BULK INSERT completado: {success_count:,} registros en {elapsed:.1f}s")
        
        return {
            'total': total_records,
            'success': success_count,
            'errors': total_records - success_count,
            'table_name': clean_table_name,
            'elapsed_seconds': elapsed,
            'records_per_second': records_per_sec,
            'method': 'bulk_insert'
        }
    
    def _cleanup_file(self, file_path: str, using_volume: bool):
        """Limpia archivo temporal después de COPY INTO"""
        try:
            client = self.get_workspace_client()
            
            if using_volume:
                # Limpiar de Volume
                client.files.delete(file_path)
                logger.info(f"🧹 Archivo limpiado de Volume")
            else:
                # Limpiar de DBFS
                dbfs_path = file_path.replace('dbfs:', '')
                client.dbfs.delete(dbfs_path)
                logger.info(f"🧹 Archivo limpiado de DBFS")
                
        except Exception as e:
            logger.warning(f"⚠️ No se pudo limpiar archivo: {str(e)}")
    
    # Alias para compatibilidad
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        ingestion_id: str, batch_size: int = 5000) -> Dict[str, Any]:
        """Usa automáticamente el método ultra-rápido"""
        return self.insert_dataframe_ultra_fast(df, table_name, ingestion_id)
    
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
    
    # ========== MÉTODOS DE UTILIDAD ==========
    
    def get_latest_table(self) -> Optional[str]:
        """Obtiene la tabla más reciente"""
        try:
            query = f"SHOW TABLES IN {self.catalog}.{self.schema}"
            tables = self.execute_query(query)
            
            if not tables:
                return None
            
            user_tables = [t for t in tables if not t.get('tableName', '').startswith('audit_') 
                          and t.get('tableName') not in ['raw_data', 'covid_processed']]
            
            if user_tables:
                return user_tables[-1].get('tableName')
            
            return None
            
        except Exception as e:
            logger.error(f"Error obteniendo última tabla: {str(e)}")
            return None
    
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
        query = f"DESCRIBE EXTENDED {self.catalog}.{self.schema}.{table_name}"
        try:
            results = self.execute_query(query)
            return results
        except Exception as e:
            logger.error(f"Error obteniendo info de tabla: {str(e)}")
            return None

    def get_table_schema(self, table_name: str) -> dict:
        """Obtiene el esquema de una tabla de forma estructurada"""
        try:
            full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
            logger.info(f"🔍 Obteniendo esquema de: {full_table_name}")

            query = f"DESCRIBE TABLE {full_table_name}"
            results = self.execute_query(query)

            if not results:
                logger.warning(f"⚠️ DESCRIBE TABLE no devolvió resultados para {full_table_name}")
                return {'table_name': table_name, 'columns': [], 'total_columns': 0}

            columns = []
            for row in results:
                col_name = row.get('col_name', '')
                # Filtrar metadatos y comentarios
                if not col_name.startswith('#') and col_name.strip():
                    columns.append({
                        'name': col_name,
                        'type': row.get('data_type', 'string'),
                        'comment': row.get('comment', '')
                    })

            logger.info(f"✅ Esquema obtenido: {len(columns)} columnas")
            return {
                'table_name': table_name,
                'columns': columns,
                'total_columns': len(columns)
            }
        except Exception as e:
            logger.error(f"❌ Error obteniendo esquema de {table_name}: {str(e)}")
            # Intentar obtener columnas con SELECT * LIMIT 0
            try:
                logger.info(f"🔄 Intentando obtener esquema con SELECT * LIMIT 0...")
                query = f"SELECT * FROM {self.catalog}.{self.schema}.{table_name} LIMIT 0"
                cursor = self.connection.cursor()
                cursor.execute(query)
                columns = [{'name': desc[0], 'type': 'string', 'comment': ''} for desc in cursor.description]
                cursor.close()
                logger.info(f"✅ Esquema obtenido con SELECT: {len(columns)} columnas")
                return {
                    'table_name': table_name,
                    'columns': columns,
                    'total_columns': len(columns)
                }
            except Exception as e2:
                logger.error(f"❌ Tampoco funcionó SELECT: {str(e2)}")
                return {'table_name': table_name, 'columns': [], 'total_columns': 0}

    def get_sample_data(self, table_name: str, limit: int = 5) -> list:
        """Obtiene datos de muestra de una tabla"""
        try:
            query = f"SELECT * FROM {self.catalog}.{self.schema}.{table_name} LIMIT {limit}"
            results = self.fetch_all(query)
            return results
        except Exception as e:
            logger.error(f"Error obteniendo muestra: {str(e)}")
            return []

    def get_active_table(self) -> str:
        """Obtiene la tabla más reciente con más registros (excluyendo audit_logs y tablas _clean/_classified)"""
        try:
            if not self.connect():
                return None

            # Obtener todas las tablas
            query = f"SHOW TABLES IN {self.catalog}.{self.schema}"
            tables = self.execute_query(query)

            if not tables:
                return None

            # Filtrar tablas (excluir audit_logs, raw_data, _clean y _classified)
            user_tables = [
                t['tableName'] for t in tables
                if t['tableName'] not in ['audit_logs', 'raw_data']
                and not t['tableName'].endswith('_clean')
                and not t['tableName'].endswith('_classified')
            ]

            if not user_tables:
                return None

            # Obtener tamaños de todas las tablas y ordenar por registros
            table_sizes = []
            for table in user_tables:
                try:
                    count_query = f"SELECT COUNT(*) as total FROM {self.catalog}.{self.schema}.{table}"
                    result = self.execute_query(count_query)
                    if result and len(result) > 0:
                        table_sizes.append({
                            'name': table,
                            'count': result[0]['total']
                        })
                except:
                    continue

            if not table_sizes:
                return user_tables[0] if user_tables else None

            # Ordenar por cantidad de registros (descendente) y retornar la más grande
            table_sizes.sort(key=lambda x: x['count'], reverse=True)
            return table_sizes[0]['name']

        except Exception as e:
            logger.error(f"Error obteniendo tabla activa: {str(e)}")
            return None

    def get_most_recent_table(self) -> str:
        """Obtiene la tabla MÁS RECIENTE (por timestamp de creación), excluyendo audit_logs y tablas _clean/_classified"""
        try:
            if not self.connect():
                return None

            # Obtener todas las tablas
            query = f"SHOW TABLES IN {self.catalog}.{self.schema}"
            tables = self.execute_query(query)

            if not tables:
                return None

            # Filtrar tablas (excluir audit_logs, raw_data, _clean y _classified)
            user_tables = [
                t['tableName'] for t in tables
                if t['tableName'] not in ['audit_logs', 'raw_data']
                and not t['tableName'].endswith('_clean')
                and not t['tableName'].endswith('_classified')
            ]

            if not user_tables:
                return None

            # Obtener información detallada de cada tabla (createdAt timestamp)
            table_info = []
            for table in user_tables:
                try:
                    # Verificar que la tabla no esté vacía
                    count_query = f"SELECT COUNT(*) as total FROM {self.catalog}.{self.schema}.{table}"
                    count_result = self.execute_query(count_query)

                    if not count_result or count_result[0]['total'] == 0:
                        continue  # Saltar tablas vacías

                    # Obtener timestamp de creación con DESCRIBE DETAIL
                    detail_query = f"DESCRIBE DETAIL {self.catalog}.{self.schema}.{table}"
                    detail = self.execute_query(detail_query)

                    if detail and len(detail) > 0:
                        table_info.append({
                            'name': table,
                            'created_at': detail[0].get('createdAt', ''),
                            'count': count_result[0]['total']
                        })
                except Exception as e:
                    logger.warning(f"No se pudo obtener info de tabla {table}: {str(e)}")
                    continue

            if not table_info:
                # Fallback: Si no hay info detallada, usar get_active_table (tabla más grande)
                logger.warning("No se pudo obtener timestamps, usando tabla más grande como fallback")
                return self.get_active_table()

            # Ordenar por timestamp de creación (descendente) y retornar la más reciente
            table_info.sort(key=lambda x: x['created_at'], reverse=True)
            most_recent = table_info[0]['name']

            logger.info(f"📅 Tabla más reciente: {most_recent} ({table_info[0]['count']:,} registros, creada: {table_info[0]['created_at']})")
            return most_recent

        except Exception as e:
            logger.error(f"Error obteniendo tabla más reciente: {str(e)}")
            return None

    def table_already_cleaned(self, table_name: str) -> bool:
        """Verifica si una tabla ya fue limpiada (existe tabla_clean)"""
        try:
            if not self.connect():
                return False

            # Verificar si existe tabla con sufijo _clean
            clean_table_name = f"{table_name}_clean"
            query = f"SHOW TABLES IN {self.catalog}.{self.schema} LIKE '{clean_table_name}'"
            result = self.execute_query(query)

            return len(result) > 0 if result else False

        except Exception as e:
            logger.error(f"Error verificando si tabla ya fue limpiada: {str(e)}")
            return False

    def insert_audit_log(self, process: str, level: str, message: str,
                        metadata: dict = None, user_id: str = None) -> bool:
        """Log de auditoría"""
        try:
            event_id = str(uuid.uuid4())

            # Serializar metadata a JSON y escapar caracteres especiales
            if metadata:
                # json.dumps ya escapa correctamente los caracteres de control
                metadata_json = json.dumps(metadata, ensure_ascii=False)
                # Escapar comillas simples para SQL
                metadata_str = metadata_json.replace("'", "''")
                # Escapar backslashes que puedan causar problemas
                metadata_str = metadata_str.replace("\\", "\\\\")
            else:
                metadata_str = None

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
                {f"'{metadata_str}'" if metadata_str else 'NULL'},
                {user_str}
            )
            """

            self.execute_query(query)
            return True
        except Exception as e:
            logger.error(f"Error audit log: {str(e)}")
            return False

    def analyze_column_for_classification(self, table_name: str, column_name: str, column_type: str) -> dict:
        """
        Analiza una columna y determina si es clasificable y cómo
        """
        try:
            if not self.connect():
                return None

            full_table = f"{self.catalog}.{self.schema}.{table_name}"

            # Obtener valores únicos y estadísticas básicas
            count_query = f"""
                SELECT
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT {column_name}) as unique_values,
                    COUNT({column_name}) as non_null_count
                FROM {full_table}
            """
            stats = self.execute_query(count_query)

            if not stats or len(stats) == 0:
                return None

            total_rows = stats[0]['total_rows']
            unique_values = stats[0]['unique_values']
            non_null_count = stats[0]['non_null_count']

            result = {
                "column_name": column_name,
                "column_type": column_type,
                "total_rows": total_rows,
                "unique_values": unique_values,
                "non_null_count": non_null_count,
                "is_classifiable": False,
                "classification_type": None,
                "suggestions": []
            }

            # 1. COLUMNAS NUMÉRICAS
            if column_type.lower() in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'double', 'float']:
                stats_query = f"""
                    SELECT
                        MIN({column_name}) as min_val,
                        MAX({column_name}) as max_val,
                        AVG({column_name}) as avg_val,
                        PERCENTILE({column_name}, 0.25) as q1,
                        PERCENTILE({column_name}, 0.50) as median,
                        PERCENTILE({column_name}, 0.75) as q3
                    FROM {full_table}
                    WHERE {column_name} IS NOT NULL
                """
                num_stats = self.execute_query(stats_query)

                if num_stats and len(num_stats) > 0:
                    min_val = float(num_stats[0]['min_val'])
                    max_val = float(num_stats[0]['max_val'])
                    q1 = float(num_stats[0]['q1'])
                    median = float(num_stats[0]['median'])
                    q3 = float(num_stats[0]['q3'])

                    # Crear rangos por cuartiles
                    result["is_classifiable"] = True
                    result["classification_type"] = "numeric_ranges"
                    result["suggestions"] = [
                        {
                            "name": f"{column_name}_rango",
                            "type": "cuartiles",
                            "ranges": [
                                {"label": "Muy bajo", "min": min_val, "max": q1},
                                {"label": "Bajo", "min": q1, "max": median},
                                {"label": "Medio", "min": median, "max": q3},
                                {"label": "Alto", "min": q3, "max": max_val}
                            ],
                            "stats": {
                                "min": min_val,
                                "max": max_val,
                                "median": median
                            }
                        }
                    ]

            # 2. COLUMNAS DE FECHA
            elif column_type.lower() in ['date', 'timestamp', 'datetime']:
                result["is_classifiable"] = True
                result["classification_type"] = "temporal"
                result["suggestions"] = [
                    {
                        "name": f"{column_name}_anio",
                        "type": "year",
                        "description": "Extraer año"
                    },
                    {
                        "name": f"{column_name}_mes",
                        "type": "month",
                        "description": "Extraer mes"
                    },
                    {
                        "name": f"{column_name}_trimestre",
                        "type": "quarter",
                        "description": "Extraer trimestre"
                    }
                ]

            # 3. COLUMNAS CATEGÓRICAS (< 50 valores únicos)
            elif column_type.lower() in ['string', 'varchar', 'char'] and unique_values < 50:
                # Obtener muestra de valores
                sample_query = f"""
                    SELECT {column_name} as value, COUNT(*) as count
                    FROM {full_table}
                    WHERE {column_name} IS NOT NULL
                    GROUP BY {column_name}
                    ORDER BY count DESC
                    LIMIT 10
                """
                sample_values = self.execute_query(sample_query)

                # Extraer valores de forma segura
                sample_list = []
                if sample_values:
                    for v in sample_values:
                        if isinstance(v, dict) and 'value' in v:
                            sample_list.append(v['value'])

                result["is_classifiable"] = True
                result["classification_type"] = "categorical"
                result["suggestions"] = [
                    {
                        "name": f"{column_name}_categoria",
                        "type": "direct",
                        "description": f"Usar valores directos ({unique_values} categorías)",
                        "sample_values": sample_list
                    }
                ]

            return result

        except Exception as e:
            logger.error(f"Error analizando columna {column_name}: {str(e)}")
            return None

    def execute_classification(self, source_table: str, classifications: list) -> dict:
        """
        Ejecuta clasificaciones en una tabla y crea tabla _classified

        classifications: [
            {
                "column": "edad",
                "new_column": "grupo_edad",
                "type": "numeric_ranges",
                "ranges": [...]
            },
            ...
        ]
        """
        try:
            if not self.connect():
                raise Exception("No se pudo conectar a Databricks")

            full_source = f"{self.catalog}.{self.schema}.{source_table}"
            classified_table = f"{source_table}_classified"
            full_classified = f"{self.catalog}.{self.schema}.{classified_table}"

            # Construir query con CASE statements para cada clasificación
            case_statements = []

            for classification in classifications:
                col = classification['column']
                new_col = classification['new_column']
                class_type = classification['type']

                if class_type == "numeric_ranges":
                    ranges = classification['ranges']
                    conditions = []
                    for i, r in enumerate(ranges):
                        # ARREGLO: El último rango debe usar <= en lugar de <
                        if i == len(ranges) - 1:  # Último rango
                            conditions.append(f"WHEN {col} >= {r['min']} AND {col} <= {r['max']} THEN '{r['label']}'")
                        else:
                            conditions.append(f"WHEN {col} >= {r['min']} AND {col} < {r['max']} THEN '{r['label']}'")
                    case_stmt = f"CASE {' '.join(conditions)} ELSE 'Desconocido' END as {new_col}"
                    case_statements.append(case_stmt)

                elif class_type == "year":
                    case_statements.append(f"YEAR({col}) as {new_col}")

                elif class_type == "month":
                    case_statements.append(f"DATE_FORMAT({col}, 'MMMM') as {new_col}")

                elif class_type == "quarter":
                    case_statements.append(f"CONCAT('Q', QUARTER({col}), ' ', YEAR({col})) as {new_col}")

                elif class_type == "direct":
                    case_statements.append(f"{col} as {new_col}")

            # Crear tabla clasificada
            all_columns = ", ".join(case_statements)

            create_query = f"""
                CREATE OR REPLACE TABLE {full_classified} AS
                SELECT
                    *,
                    {all_columns}
                FROM {full_source}
            """

            logger.info(f"🔄 Ejecutando clasificación COMPLETA:")
            logger.info(f"Query: {create_query}")
            self.execute_query(create_query)

            # Contar registros
            count_query = f"SELECT COUNT(*) as total FROM {full_classified}"
            result = self.execute_query(count_query)
            total_records = result[0]['total'] if result else 0

            logger.info(f"✅ Tabla clasificada creada: {classified_table} ({total_records:,} registros)")

            return {
                "success": True,
                "classified_table": classified_table,
                "total_records": total_records,
                "classifications_applied": len(classifications)
            }

        except Exception as e:
            logger.error(f"Error ejecutando clasificación: {str(e)}")
            raise

    def query_llama(self, prompt: str, context: str = "") -> str:
        """
        Consulta a Llama 3.1 usando Databricks Foundation Model API

        Args:
            prompt: Pregunta del usuario
            context: Contexto adicional (resultados de SQL, datos de tabla, etc.)

        Returns:
            Respuesta generada por Llama
        """
        try:
            import requests

            # Endpoint de Foundation Model API (Llama 3.1 8B Instruct)
            url = f"https://{self.host}/serving-endpoints/databricks-meta-llama-3-1-8b-instruct/invocations"

            # Construir mensaje con contexto
            full_prompt = f"""Eres un asistente analítico para datos de vacunación COVID-19 en Ecuador.

Contexto de datos:
{context}

Pregunta del usuario: {prompt}

Proporciona una respuesta clara, concisa y en español. Si hay datos numéricos, preséntalos de forma legible."""

            payload = {
                "messages": [
                    {
                        "role": "user",
                        "content": full_prompt
                    }
                ],
                "max_tokens": 500,
                "temperature": 0.3
            }

            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            }

            logger.info(f"🤖 Consultando Llama 3.1 con prompt: {prompt[:100]}...")

            response = requests.post(url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()

            result = response.json()

            # Extraer respuesta del modelo
            if "choices" in result and len(result["choices"]) > 0:
                answer = result["choices"][0]["message"]["content"]
                logger.info(f"✅ Respuesta de Llama generada ({len(answer)} chars)")
                return answer
            else:
                logger.warning("⚠️ Respuesta de Llama sin contenido esperado")
                return "No se pudo generar una respuesta del modelo."

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning("⚠️ Foundation Model API no disponible. Generando respuesta basada en datos...")
                return self._generate_response_from_data(prompt, context)
            else:
                logger.error(f"❌ Error HTTP consultando Llama: {str(e)}")
                return self._generate_response_from_data(prompt, context)
        except Exception as e:
            logger.warning(f"⚠️ Error consultando Llama: {str(e)}. Generando respuesta basada en datos...")
            return self._generate_response_from_data(prompt, context)

    def _generate_response_from_data(self, prompt: str, context: str) -> str:
        """
        Genera respuesta inteligente basada directamente en los datos
        (Fallback cuando Foundation Model API no está disponible)
        """
        try:
            # Extraer información del contexto
            lines = context.split('\n')

            # Buscar información clave
            table_name = ""
            sql_query = ""
            results_count = 0
            results_data = []

            for i, line in enumerate(lines):
                if "Tabla consultada:" in line:
                    table_name = line.split("Tabla consultada:")[-1].strip()
                elif "SQL ejecutado:" in line:
                    sql_query = line.split("SQL ejecutado:")[-1].strip()
                elif "Resultados obtenidos" in line:
                    # Extraer número de resultados
                    import re
                    match = re.search(r'(\d+)\s+registros', line)
                    if match:
                        results_count = int(match.group(1))
                elif line.strip() and line[0].isdigit() and ". " in line:
                    # Es una línea de resultado
                    results_data.append(line.strip())

            # Generar respuesta según el tipo de consulta
            prompt_lower = prompt.lower()

            # Tipo 1: Consultas de conteo
            if "cuántos" in prompt_lower or "cuántas" in prompt_lower or "total" in prompt_lower:
                if results_count > 0 and results_data:
                    # Extraer el valor del conteo
                    first_result = results_data[0] if results_data else ""
                    if "total:" in first_result.lower():
                        total_value = first_result.split(":")[-1].strip()
                        return f"""**Resultados de la consulta:**

El total registrado es de **{total_value}** registros en la tabla `{table_name}`.

📊 **Detalles:**
- Tabla consultada: {table_name}
- Tipo de consulta: Conteo total
- Registros encontrados: {results_count}

💡 Esta consulta se realizó directamente sobre los datos en Delta Lake."""
                    else:
                        return f"""**Resultados de la consulta:**

Se encontraron **{results_count}** registros en la tabla `{table_name}`.

📊 **Primeros resultados:**
{chr(10).join(results_data[:5])}

💡 Consulta ejecutada: `{sql_query}`"""
                else:
                    return f"""**No se encontraron resultados.**

La consulta no devolvió datos. Verifica que:
- La tabla `{table_name}` contenga datos
- Los filtros de la consulta sean correctos"""

            # Tipo 2: Consultas de agrupación
            elif "grupo" in prompt_lower or "distribución" in prompt_lower or "por" in prompt_lower:
                if results_count > 0 and results_data:
                    return f"""**Distribución encontrada:**

Se encontraron **{results_count}** categorías distintas.

📊 **Top resultados:**
{chr(10).join(results_data[:10])}

🔍 **Tabla analizada:** `{table_name}`

💡 Consulta SQL: `{sql_query}`"""
                else:
                    return f"""**No se encontraron datos para agrupar.**

Verifica que la tabla `{table_name}` contenga información en las columnas solicitadas."""

            # Tipo 3: Respuesta genérica
            else:
                if results_count > 0 and results_data:
                    return f"""**Resultados de tu consulta:**

📊 **Se encontraron {results_count} registros.**

**Muestra de datos:**
{chr(10).join(results_data[:10])}

🗃️ **Tabla:** `{table_name}`
🔍 **SQL ejecutado:** `{sql_query}`

💡 *Sistema RAG sin LLM - Respuesta generada directamente desde los datos*"""
                else:
                    return f"""**Consulta ejecutada correctamente.**

No se encontraron resultados para esta búsqueda en la tabla `{table_name}`.

🔍 **SQL:** `{sql_query}`"""

        except Exception as e:
            logger.error(f"Error generando respuesta desde datos: {str(e)}")
            return f"""**Respuesta basada en contexto:**

{context}

---
💡 *Nota: Foundation Model API no disponible. Mostrando datos directamente.*"""

    def generate_sql_from_question(self, question: str, table_name: str) -> str:
        """
        Usa Llama 3.1 para generar SQL query basado en pregunta en lenguaje natural

        Args:
            question: Pregunta del usuario
            table_name: Tabla a consultar

        Returns:
            SQL query generado por Llama 3.1
        """
        try:
            import requests

            # Obtener esquema de la tabla
            schema = self.get_table_schema(table_name)
            columns_info = schema.get('columns', []) if isinstance(schema, dict) else schema

            # Crear descripción del esquema para Llama
            schema_description = "Columnas disponibles:\n"
            for col in columns_info:
                schema_description += f"- {col['name']} ({col['type']})\n"

            full_table = f"{self.catalog}.{self.schema}.{table_name}"

            # Obtener valores de ejemplo para guiar a Llama (con manejo de errores)
            sample_text = ""
            try:
                sample_query = f"SELECT * FROM {full_table} LIMIT 3"
                sample_data = self.execute_query(sample_query)

                # Crear ejemplos de valores reales
                if sample_data and len(sample_data) > 0:
                    sample_text = "\nEJEMPLOS DE DATOS REALES:\n"
                    for i, row in enumerate(sample_data[:2], 1):
                        sample_text += f"{i}. " + ", ".join([f"{k}={v}" for k, v in row.items()]) + "\n"
            except Exception as e:
                logger.warning(f"No se pudieron obtener datos de ejemplo de {full_table}: {str(e)}")
                # Continuar sin ejemplos de datos

            # Prompt para que Llama genere SQL - Cada pregunta es independiente
            sql_generation_prompt = f"""Genera ÚNICAMENTE el SQL query para esta pregunta. No uses contexto previo.

TABLA COMPLETA (USAR SIEMPRE): {full_table}

COLUMNAS DISPONIBLES:
{schema_description}
{sample_text}
PREGUNTA DEL USUARIO: {question}

REGLAS CRÍTICAS:
1. DEBES usar el nombre COMPLETO de la tabla: {full_table} (NO solo {table_name})
2. TODAS las columnas son tipo STRING - NO uses CAST, EXTRACT, TO_DATE ni funciones de fecha
3. Para filtrar por sexo: usa 'Hombre' o 'Mujer' (con mayúscula inicial), NO 'masculino' ni 'femenino'
4. Para filtrar por mes en fechas STRING (formato M/D/YYYY o MM/DD/YYYY):
   - Enero = '1/', Febrero = '2/', Marzo = '3/', etc.
   - Usa: WHERE fecha_vacuna LIKE '2/%' para febrero
   - O usa: WHERE SUBSTRING(fecha_vacuna, 1, INSTR(fecha_vacuna, '/') - 1) = '2' para febrero
5. Para conteo simple: SELECT COUNT(*) FROM {full_table} WHERE condiciones LIMIT 100
6. NO uses GROUP BY si solo quieres un conteo total
7. Si agrupas por zona/provincia, usa ORDER BY COUNT(*) DESC para ordenar
8. LIMIT 100 al final
9. NO uses backticks, comillas invertidas ni markdown
10. SOLO genera el SQL query válido, sin markdown ni explicaciones

EJEMPLOS (FÍJATE EN EL NOMBRE COMPLETO DE LA TABLA):
- "cuántos hombres en febrero" → SELECT COUNT(*) FROM {full_table} WHERE sexo = 'Hombre' AND fecha_vacuna LIKE '2/%' LIMIT 100
- "provincia con más vacunas" → SELECT provincia, COUNT(*) as total FROM {full_table} GROUP BY provincia ORDER BY total DESC LIMIT 100
- "total de vacunas" → SELECT COUNT(*) FROM {full_table} LIMIT 100

SQL query:"""

            # Llamar a Llama para generar SQL - SIN memoria de conversaciones anteriores
            url = f"https://{self.host}/serving-endpoints/databricks-meta-llama-3-1-8b-instruct/invocations"

            payload = {
                "messages": [
                    {
                        "role": "system",
                        "content": "Eres un generador de SQL. Cada pregunta es INDEPENDIENTE. Respondes solo con SQL válido."
                    },
                    {
                        "role": "user",
                        "content": sql_generation_prompt
                    }
                ],
                "max_tokens": 200,
                "temperature": 0.0  # Temperatura 0 = más determinista
            }

            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            }

            logger.info(f"🤖 Generando SQL con Llama para: {question[:50]}...")

            response = requests.post(url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()

            result = response.json()

            if "choices" in result and len(result["choices"]) > 0:
                sql_query = result["choices"][0]["message"]["content"].strip()

                # Limpiar el SQL (remover markdown, comillas, etc.)
                sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
                sql_query = sql_query.replace("`", "")

                # Si hay múltiples líneas, tomar solo la primera query válida
                if "\n\n" in sql_query:
                    sql_query = sql_query.split("\n\n")[0]

                logger.info(f"✅ SQL generado por Llama: {sql_query}")
                return sql_query
            else:
                logger.warning("⚠️ Llama no generó SQL, usando fallback")
                return f"SELECT * FROM {full_table} LIMIT 100"

        except Exception as e:
            logger.error(f"❌ Error generando SQL con Llama: {str(e)}")
            # Fallback query
            return f"SELECT * FROM {self.catalog}.{self.schema}.{table_name} LIMIT 100"


# Instancia global
databricks_service = DatabricksService()