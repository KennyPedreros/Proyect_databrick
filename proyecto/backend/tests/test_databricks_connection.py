import sys
import os

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.databricks_service import databricks_service
from app.config.settings import settings


def test_connection():
    """Prueba la conexión con Databricks"""
    print("=" * 60)
    print("🧪 PROBANDO CONEXIÓN CON DATABRICKS")
    print("=" * 60)
    
    # Verificar configuración
    print("\n📋 Configuración:")
    print(f"   Host: {settings.DATABRICKS_HOST}")
    print(f"   Token: {'*' * 20}{settings.DATABRICKS_TOKEN[-4:] if settings.DATABRICKS_TOKEN else 'NO CONFIGURADO'}")
    print(f"   Cluster ID: {settings.DATABRICKS_CLUSTER_ID}")
    print(f"   Catalog: {settings.DATABRICKS_CATALOG}")
    print(f"   Schema: {settings.DATABRICKS_SCHEMA}")
    
    # Intentar conexión
    print("\n🔌 Conectando...")
    try:
        is_connected = databricks_service.connect()
        
        if not is_connected:
            print("❌ No se pudo establecer conexión")
            return False
        
        print("✅ Conexión establecida exitosamente")
        
        # Probar una query simple
        print("\n🔍 Ejecutando query de prueba...")
        result = databricks_service.execute_query("SELECT 1 as test, current_timestamp() as now")
        print(f"✅ Query ejecutada: {result}")
        
        # Intentar crear catálogo y schema
        print("\n📁 Creando catálogo y schema...")
        databricks_service.create_catalog_and_schema()
        print("✅ Catálogo y schema verificados/creados")
        
        # Crear tablas
        print("\n📊 Creando tablas...")
        databricks_service.create_raw_table()
        print("✅ Tabla RAW creada/verificada")
        
        databricks_service.create_processed_table()
        print("✅ Tabla PROCESSED creada/verificada")
        
        # Obtener conteos
        print("\n📈 Obteniendo estadísticas...")
        raw_count = databricks_service.get_table_count("covid_raw")
        processed_count = databricks_service.get_table_count("covid_processed")
        
        print(f"   Registros en RAW: {raw_count}")
        print(f"   Registros en PROCESSED: {processed_count}")
        
        print("\n" + "=" * 60)
        print("✅ TODAS LAS PRUEBAS PASARON EXITOSAMENTE")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        print("\n💡 Verifica:")
        print("   1. Que el archivo .env tiene las credenciales correctas")
        print("   2. Que el SQL Warehouse está running en Databricks")
        print("   3. Que el token tiene los permisos necesarios")
        return False
    
    finally:
        databricks_service.disconnect()
        print("\n🔌 Conexión cerrada")


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)