import sys
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Colores para terminal
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

def print_success(msg):
    print(f"{GREEN}✅ {msg}{RESET}")

def print_error(msg):
    print(f"{RED}❌ {msg}{RESET}")

def print_warning(msg):
    print(f"{YELLOW}⚠️  {msg}{RESET}")

def print_info(msg):
    print(f"{BLUE}ℹ️  {msg}{RESET}")

def print_header(msg):
    print("\n" + "="*70)
    print(f"{BLUE}{msg}{RESET}")
    print("="*70)

def check_environment_variables():
    """Verifica que las variables de entorno estén configuradas"""
    print_header("PASO 1: VERIFICAR VARIABLES DE ENTORNO")
    
    required_vars = {
        'DATABRICKS_HOST': os.getenv('DATABRICKS_HOST'),
        'DATABRICKS_TOKEN': os.getenv('DATABRICKS_TOKEN'),
        'DATABRICKS_CLUSTER_ID': os.getenv('DATABRICKS_CLUSTER_ID'),
        'DATABRICKS_CATALOG': os.getenv('DATABRICKS_CATALOG'),
        'DATABRICKS_SCHEMA': os.getenv('DATABRICKS_SCHEMA')
    }
    
    all_configured = True
    
    for var_name, var_value in required_vars.items():
        if var_value:
            masked_value = var_value if var_name not in ['DATABRICKS_TOKEN'] else f"{'*' * 20}{var_value[-4:]}"
            print_success(f"{var_name}: {masked_value}")
        else:
            print_error(f"{var_name}: NO CONFIGURADO")
            all_configured = False
    
    if not all_configured:
        print_error("\nFaltan variables de entorno. Edita el archivo .env")
        return False
    
    print_success("\nTodas las variables están configuradas")
    return True

def check_imports():
    """Verifica que las librerías necesarias estén instaladas"""
    print_header("PASO 2: VERIFICAR LIBRERÍAS")
    
    try:
        import databricks.sql
        print_success("databricks-sql-connector instalado")
    except ImportError:
        print_error("databricks-sql-connector NO instalado")
        print_info("Ejecuta: pip install databricks-sql-connector[pyarrow]")
        return False
    
    try:
        import pyarrow
        print_success("pyarrow instalado")
    except ImportError:
        print_warning("pyarrow NO instalado (recomendado pero no crítico)")
        print_info("Ejecuta: pip install pyarrow")
    
    try:
        from databricks.sdk import WorkspaceClient
        print_success("databricks-sdk instalado")
    except ImportError:
        print_warning("databricks-sdk NO instalado (opcional)")
    
    return True

def test_connection():
    """Prueba la conexión básica con Databricks"""
    print_header("PASO 3: PROBAR CONEXIÓN BÁSICA")
    
    try:
        from databricks import sql
        
        host = os.getenv('DATABRICKS_HOST')
        token = os.getenv('DATABRICKS_TOKEN')
        cluster_id = os.getenv('DATABRICKS_CLUSTER_ID')
        
        print_info(f"Conectando a: {host}")
        print_info(f"Usando warehouse: {cluster_id}")
        
        connection = sql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{cluster_id}",
            access_token=token
        )
        
        print_success("Conexión establecida exitosamente")
        
        # Ejecutar query simple
        print_info("Ejecutando query de prueba...")
        cursor = connection.cursor()
        cursor.execute("SELECT 1 as test, current_timestamp() as now")
        result = cursor.fetchall()
        
        print_success(f"Query ejecutada: {result[0]}")
        
        cursor.close()
        connection.close()
        
        return True
        
    except Exception as e:
        print_error(f"Error en conexión: {str(e)}")
        
        # Diagnóstico de errores comunes
        error_msg = str(e).lower()
        
        if 'authentication' in error_msg or 'token' in error_msg:
            print_warning("\n🔍 POSIBLE CAUSA: Token inválido o expirado")
            print_info("   1. Regenera el token en Databricks")
            print_info("   2. Actualiza DATABRICKS_TOKEN en .env")
            print_info("   3. El token debe empezar con 'dapi'")
        
        elif 'warehouse' in error_msg or 'cluster' in error_msg:
            print_warning("\n🔍 POSIBLE CAUSA: SQL Warehouse no encontrado o detenido")
            print_info("   1. Ve a SQL Warehouses en Databricks")
            print_info("   2. Asegúrate de que esté RUNNING (verde)")
            print_info("   3. Verifica el CLUSTER_ID en la URL")
        
        elif 'timeout' in error_msg or 'connection' in error_msg:
            print_warning("\n🔍 POSIBLE CAUSA: Problemas de red")
            print_info("   1. Verifica tu conexión a internet")
            print_info("   2. El HOST no debe tener https://")
            print_info("   3. Intenta desde otra red si es posible")
        
        return False

def create_catalog_and_schema():
    """Crea el catálogo y schema en Databricks"""
    print_header("PASO 4: CREAR CATÁLOGO Y SCHEMA")
    
    try:
        from databricks import sql
        
        host = os.getenv('DATABRICKS_HOST')
        token = os.getenv('DATABRICKS_TOKEN')
        cluster_id = os.getenv('DATABRICKS_CLUSTER_ID')
        catalog = os.getenv('DATABRICKS_CATALOG')
        schema = os.getenv('DATABRICKS_SCHEMA')
        
        connection = sql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{cluster_id}",
            access_token=token
        )
        
        cursor = connection.cursor()
        
        # Crear catálogo
        print_info(f"Creando catálogo: {catalog}")
        cursor.execute(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        print_success(f"Catálogo '{catalog}' creado/verificado")
        
        # Usar catálogo
        cursor.execute(f"USE CATALOG {catalog}")
        
        # Crear schema
        print_info(f"Creando schema: {schema}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        print_success(f"Schema '{schema}' creado/verificado")
        
        cursor.close()
        connection.close()
        
        return True
        
    except Exception as e:
        print_error(f"Error creando catálogo/schema: {str(e)}")
        return False

def create_tables():
    """Crea las tablas necesarias"""
    print_header("PASO 5: CREAR TABLAS")
    
    try:
        from databricks import sql
        
        host = os.getenv('DATABRICKS_HOST')
        token = os.getenv('DATABRICKS_TOKEN')
        cluster_id = os.getenv('DATABRICKS_CLUSTER_ID')
        catalog = os.getenv('DATABRICKS_CATALOG')
        schema = os.getenv('DATABRICKS_SCHEMA')
        
        connection = sql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{cluster_id}",
            access_token=token
        )
        
        cursor = connection.cursor()
        
        # Usar catálogo y schema
        cursor.execute(f"USE CATALOG {catalog}")
        cursor.execute(f"USE SCHEMA {schema}")
        
        # Crear tabla RAW
        print_info("Creando tabla covid_raw...")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.covid_raw (
                ingestion_id STRING,
                filename STRING,
                raw_data STRING,
                uploaded_at TIMESTAMP,
                record_count INT
            )
            USING DELTA
        """)
        print_success("Tabla 'covid_raw' creada/verificada")
        
        # Crear tabla PROCESSED
        print_info("Creando tabla covid_processed...")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.covid_processed (
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
        """)
        print_success("Tabla 'covid_processed' creada/verificada")
        
        # Verificar que las tablas existen
        cursor.execute(f"SHOW TABLES IN {catalog}.{schema}")
        tables = cursor.fetchall()
        
        print_info("\nTablas creadas:")
        for table in tables:
            print(f"   - {table[1]}")
        
        cursor.close()
        connection.close()
        
        return True
        
    except Exception as e:
        print_error(f"Error creando tablas: {str(e)}")
        return False

def verify_setup():
    """Verificación final"""
    print_header("PASO 6: VERIFICACIÓN FINAL")
    
    try:
        from databricks import sql
        
        host = os.getenv('DATABRICKS_HOST')
        token = os.getenv('DATABRICKS_TOKEN')
        cluster_id = os.getenv('DATABRICKS_CLUSTER_ID')
        catalog = os.getenv('DATABRICKS_CATALOG')
        schema = os.getenv('DATABRICKS_SCHEMA')
        
        connection = sql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{cluster_id}",
            access_token=token
        )
        
        cursor = connection.cursor()
        
        # Contar registros en cada tabla
        cursor.execute(f"SELECT COUNT(*) FROM {catalog}.{schema}.covid_raw")
        raw_count = cursor.fetchone()[0]
        print_success(f"Tabla covid_raw: {raw_count} registros")
        
        cursor.execute(f"SELECT COUNT(*) FROM {catalog}.{schema}.covid_processed")
        processed_count = cursor.fetchone()[0]
        print_success(f"Tabla covid_processed: {processed_count} registros")
        
        cursor.close()
        connection.close()
        
        return True
        
    except Exception as e:
        print_error(f"Error en verificación: {str(e)}")
        return False

def main():
    """Ejecuta todos los pasos"""
    print(f"\n{BLUE}{'='*70}")
    print("🧪 SCRIPT DE CONFIGURACIÓN DE DATABRICKS")
    print("   Sistema COVID-19 ESPE")
    print(f"{'='*70}{RESET}\n")
    
    # Paso 1: Variables de entorno
    if not check_environment_variables():
        print_error("\n❌ CONFIGURACIÓN INCOMPLETA")
        sys.exit(1)
    
    # Paso 2: Librerías
    if not check_imports():
        print_error("\n❌ FALTAN LIBRERÍAS")
        sys.exit(1)
    
    # Paso 3: Conexión
    if not test_connection():
        print_error("\n❌ ERROR DE CONEXIÓN")
        sys.exit(1)
    
    # Paso 4: Catálogo y Schema
    if not create_catalog_and_schema():
        print_error("\n❌ ERROR CREANDO CATÁLOGO/SCHEMA")
        sys.exit(1)
    
    # Paso 5: Tablas
    if not create_tables():
        print_error("\n❌ ERROR CREANDO TABLAS")
        sys.exit(1)
    
    # Paso 6: Verificación
    if not verify_setup():
        print_error("\n❌ ERROR EN VERIFICACIÓN")
        sys.exit(1)
    
    # ¡Éxito!
    print(f"\n{GREEN}{'='*70}")
    print("✅ ¡CONFIGURACIÓN COMPLETADA EXITOSAMENTE!")
    print(f"{'='*70}{RESET}\n")
    
    print_info("Siguiente paso:")
    print("   1. Inicia el servidor: uvicorn app.main:app --reload --port 8000")
    print("   2. Ve a: http://localhost:8000/docs")
    print("   3. Prueba: POST /api/storage/test-connection")
    print()

if __name__ == "__main__":
    main()