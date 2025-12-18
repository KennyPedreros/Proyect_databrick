from fastapi import APIRouter, UploadFile, File, HTTPException
from app.services.monitoring_service import monitoring_service, LogLevel
from app.services.databricks_service import databricks_service
from app.models.schemas import IngestionResponse, DataSourceInfo, SuccessResponse
from typing import List, Tuple, Dict, Any
import uuid
from datetime import datetime
import os
import pandas as pd
import io
import logging
import chardet

router = APIRouter(prefix="/api/ingest", tags=["Módulo 1: Ingesta de Datos"])
logger = logging.getLogger(__name__)

# Base de datos temporal en memoria
uploaded_files_db = []

def crear_id_ingesta() -> str:
    """Genera un ID único para la ingesta"""
    return f"ING-{uuid.uuid4().hex[:8].upper()}"

def validate_schema(filename: str, size: int) -> tuple[bool, str]:
    """Valida el esquema del archivo"""
    allowed_extensions = ['.csv', '.xlsx', '.xls', '.json']
    ext = os.path.splitext(filename)[1].lower()
    
    if ext not in allowed_extensions:
        return False, f"Extensión no permitida: {ext}. Solo se permiten: {', '.join(allowed_extensions)}"
    
    max_size = 500 * 1024 * 1024  # 500 MB
    if size > max_size:
        size_mb = size / (1024 * 1024)
        return False, f"Archivo demasiado grande: {size_mb:.2f}MB. Máximo: 500MB"
    
    if size == 0:
        return False, "El archivo está vacío"
    
    return True, "OK"

def log_ingestion_status(ingestion_id: str, status: str):
    """Registra el estado de la ingesta en logs"""
    logger.info(f"[INGESTION] {ingestion_id} - Status: {status} - {datetime.now()}")

def detect_encoding_smart(file_content: bytes, sample_size: int = 100000) -> str:
    """Detecta el encoding del archivo de forma inteligente"""
    try:
        sample = file_content[:sample_size]
        result = chardet.detect(sample)
        
        encoding = result.get('encoding', 'utf-8')
        confidence = result.get('confidence', 0)
        
        logger.info(f"Encoding detectado: {encoding} (confianza: {confidence:.0%})")
        
        if confidence < 0.7:
            common_encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            for enc in common_encodings:
                try:
                    sample.decode(enc)
                    logger.info(f"Usando encoding alternativo: {enc}")
                    return enc
                except:
                    continue
            return 'latin-1'
        
        return encoding if encoding else 'utf-8'
    
    except Exception as e:
        logger.warning(f"Error detectando encoding: {str(e)}")
        return 'latin-1'

def detect_csv_delimiter(content_sample: str) -> str:
    """Detecta el delimitador correcto para CSV"""
    delimiters = {
        ',': content_sample.count(','),
        ';': content_sample.count(';'),
        '\t': content_sample.count('\t'),
        '|': content_sample.count('|')
    }
    
    detected = max(delimiters, key=delimiters.get)
    count = delimiters[detected]
    
    logger.info(f"Delimitador detectado: '{detected}' (aparece {count} veces)")
    return detected

def read_file_universal(file_content: bytes, filename: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    FUNCIÓN UNIVERSAL: Lee CSV, Excel, JSON automáticamente
    
    Returns:
        (DataFrame, metadata_dict)
    """
    ext = os.path.splitext(filename)[1].lower()
    
    metadata = {
        "filename": filename,
        "extension": ext,
        "original_columns": [],
        "row_count": 0,
        "encoding": None,
        "delimiter": None,
        "null_counts": {},
        "dtypes": {},
        "issues": []
    }
    
    try:
        # ==================== CSV ====================
        if ext == '.csv':
            logger.info("📄 Procesando archivo CSV...")
            
            encoding = detect_encoding_smart(file_content)
            metadata["encoding"] = encoding
            
            try:
                first_lines = file_content.decode(encoding).split('\n')[:5]
                sample = '\n'.join(first_lines)
                delimiter = detect_csv_delimiter(sample)
                metadata["delimiter"] = delimiter
            except:
                delimiter = ','
                metadata["delimiter"] = ','
            
            df = pd.read_csv(
                io.BytesIO(file_content),
                encoding=encoding,
                delimiter=delimiter,
                low_memory=False,
                na_values=['', 'NULL', 'null', 'NA', 'N/A', 'nan', 'NaN', 'none', 'None'],
                keep_default_na=True,
                skip_blank_lines=True,
                on_bad_lines='skip',
                engine='python'
            )
            
            logger.info(f"✅ CSV leído: {len(df)} filas, encoding={encoding}, delimiter='{delimiter}'")
        
        # ==================== EXCEL ====================
        elif ext in ['.xlsx', '.xls']:
            logger.info("📊 Procesando archivo Excel...")
            
            df = pd.read_excel(
                io.BytesIO(file_content),
                engine='openpyxl' if ext == '.xlsx' else 'xlrd',
                na_values=['', 'NULL', 'null', 'NA', 'N/A', 'nan', 'NaN']
            )
            
            metadata["encoding"] = "N/A (Excel binario)"
            logger.info(f"✅ Excel leído: {len(df)} filas")
        
        # ==================== JSON ====================
        elif ext == '.json':
            logger.info("📋 Procesando archivo JSON...")
            
            try:
                df = pd.read_json(io.BytesIO(file_content), encoding='utf-8')
                metadata["encoding"] = "utf-8"
            except:
                encoding = detect_encoding_smart(file_content)
                json_str = file_content.decode(encoding)
                df = pd.read_json(io.StringIO(json_str))
                metadata["encoding"] = encoding
            
            logger.info(f"✅ JSON leído: {len(df)} filas")
        
        else:
            raise ValueError(f"Formato no soportado: {ext}")
        
        # ==================== POST-PROCESAMIENTO ====================
        
        metadata["original_columns"] = df.columns.tolist()
        metadata["row_count"] = len(df)
        metadata["null_counts"] = df.isnull().sum().to_dict()
        metadata["dtypes"] = df.dtypes.astype(str).to_dict()
        
        # Normalizar nombres de columnas
        df.columns = [
            str(col).strip().lower()
            .replace(' ', '_')
            .replace('á', 'a').replace('é', 'e').replace('í', 'i')
            .replace('ó', 'o').replace('ú', 'u').replace('ñ', 'n')
            for col in df.columns
        ]
        
        logger.info(f"📊 DataFrame: {len(df)} filas × {len(df.columns)} columnas")
        logger.info(f"📋 Columnas: {df.columns.tolist()}")
        
        # Detectar problemas
        unnamed = [col for col in df.columns if 'unnamed' in str(col).lower()]
        if unnamed:
            metadata["issues"].append(f"Columnas sin nombre: {unnamed}")
        
        for col, null_count in metadata["null_counts"].items():
            if null_count > 0:
                null_pct = (null_count / len(df)) * 100
                if null_pct > 50:
                    metadata["issues"].append(f"'{col}' tiene {null_pct:.0f}% nulos")
        
        dup_count = df.duplicated().sum()
        if dup_count > 0:
            metadata["issues"].append(f"{dup_count} filas duplicadas")
        
        if metadata["issues"]:
            for issue in metadata["issues"]:
                logger.warning(f"⚠️ {issue}")
        
        return df, metadata
    
    except Exception as e:
        logger.error(f"❌ Error leyendo archivo: {str(e)}")
        raise Exception(f"No se pudo leer {filename}: {str(e)}")

def bulk_insert_optimized(df: pd.DataFrame) -> int:
    """
    Inserción masiva optimizada: 1000 filas por query
    """
    try:
        if not databricks_service.connect():
            raise Exception("No se pudo conectar a Databricks")
        
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            values_list = []
            
            for _, row in batch.iterrows():
                case_id = str(row.get('case_id', row.get('id', f'CASE-{uuid.uuid4().hex[:8].upper()}')))
                date_val = str(row.get('date', row.get('fecha', datetime.now().strftime('%Y-%m-%d'))))
                country = str(row.get('country', row.get('pais', 'Ecuador')))
                region = str(row.get('region', row.get('provincia', row.get('ciudad', 'Unknown'))))
                
                try:
                    age = int(float(row.get('age', row.get('edad', 0))))
                except:
                    age = 0
                
                gender = str(row.get('gender', row.get('sexo', row.get('genero', 'Unknown'))))
                symptoms = str(row.get('symptoms', row.get('sintomas', 'Unknown'))).replace("'", "''")
                outcome = str(row.get('outcome', row.get('estado', 'Activo')))
                
                vaccinated_val = row.get('vaccinated', row.get('vacunado', False))
                vaccinated = 1 if vaccinated_val in [True, 1, '1', 'true', 'True', 'SI', 'Si', 'si'] else 0
                
                value_str = f"""(
                    '{case_id}',
                    '{date_val}',
                    '{country}',
                    '{region}',
                    {age},
                    '{gender}',
                    '{symptoms}',
                    NULL,
                    '{outcome}',
                    {vaccinated},
                    NULL,
                    NULL,
                    NULL,
                    current_timestamp()
                )"""
                values_list.append(value_str)
            
            query = f"""
            INSERT INTO {databricks_service.catalog}.{databricks_service.schema}.covid_processed
            (case_id, date, country, region, age, gender, symptoms, severity, outcome, 
             vaccinated, medical_history, classification_confidence, classified_at, processed_at)
            VALUES {', '.join(values_list)}
            """
            
            databricks_service.execute_query(query)
            total_inserted += len(batch)
            
            if total_inserted % 5000 == 0:
                logger.info(f"Progreso: {total_inserted:,}/{len(df):,} filas insertadas...")
        
        databricks_service.disconnect()
        logger.info(f"✅ Total insertado: {total_inserted:,} registros")
        
        return total_inserted
    
    except Exception as e:
        logger.error(f"❌ Error en bulk insert: {str(e)}")
        databricks_service.disconnect()
        raise

@router.post("/upload", response_model=IngestionResponse)
async def upload_covid_data(file: UploadFile = File(...)):
    """
    Módulo 1: Carga de archivos COVID-19 OPTIMIZADA
    
    Formatos: CSV, Excel (.xlsx, .xls), JSON
    Límite: 500MB por archivo
    
    MEJORAS:
    - Detección automática de encoding
    - Detección automática de delimitador
    - Bulk insert (1000 filas/query)
    - Soporte multi-formato
    """
    try:
        ingestion_id = crear_id_ingesta()
        
        logger.info(f"📥 Recibiendo archivo: {file.filename}")
        
        # Leer archivo en chunks
        contents = bytearray()
        chunk_size = 1024 * 1024  # 1MB chunks
        
        while chunk := await file.read(chunk_size):
            contents.extend(chunk)
        
        file_size = len(contents)
        logger.info(f"📊 Tamaño: {file_size / 1024 / 1024:.2f}MB")
        
        # Validar archivo
        is_valid, validation_message = validate_schema(file.filename, file_size)
        if not is_valid:
            logger.error(f"❌ Validación fallida: {validation_message}")
            raise HTTPException(status_code=400, detail=validation_message)
        
        # Leer y procesar archivo
        logger.info("🔍 Procesando archivo...")
        df, metadata = read_file_universal(bytes(contents), file.filename)
        
        if len(df) == 0:
            raise HTTPException(status_code=400, detail="No se encontraron registros válidos")
        
        records_count = len(df)
        logger.info(f"✅ {records_count:,} registros encontrados")
        logger.info(f"📋 Columnas: {metadata['original_columns']}")
        
        # Insertar en Databricks (OPTIMIZADO)
        if databricks_service.host and databricks_service.token:
            logger.info("💾 Guardando en Databricks...")
            start_time = datetime.now()
            
            total_inserted = bulk_insert_optimized(df)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ {total_inserted:,} registros guardados en {elapsed:.1f}s")
            logger.info(f"⚡ Velocidad: {total_inserted/elapsed:.0f} registros/segundo")
        else:
            logger.warning("⚠️ Databricks no configurado. Datos no persistidos.")
        
        # Guardar metadata
        file_info = {
            "ingestion_id": ingestion_id,
            "filename": file.filename,
            "size_bytes": file_size,
            "records_count": records_count,
            "uploaded_at": datetime.now(),
            "metadata": metadata
        }
        
        uploaded_files_db.append(file_info)
        
        # Log de monitoreo
        monitoring_service.log_event(
            process="Ingesta",
            level=LogLevel.SUCCESS,
            message=f"Archivo cargado: {file.filename} ({records_count:,} registros)",
            data={
                "ingestion_id": ingestion_id,
                "filename": file.filename,
                "records": records_count,
                "size_mb": round(file_size / 1024 / 1024, 2),
                "columns": metadata["original_columns"],
                "issues": metadata.get("issues", [])
            }
        )
        
        log_ingestion_status(ingestion_id, "SUCCESS")
        
        return IngestionResponse(
            ingestion_id=ingestion_id,
            filename=file.filename,
            records_count=records_count,
            status="success",
            message=f"✅ Archivo procesado: {records_count:,} registros insertados"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error inesperado: {str(e)}", exc_info=True)
        log_ingestion_status("ERROR", f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sources", response_model=List[DataSourceInfo])
def get_data_sources():
    """Listar fuentes de datos disponibles"""
    sources = [
        DataSourceInfo(
            source_id="OMS-001",
            name="Organización Mundial de la Salud",
            type="api",
            last_updated=datetime.now()
        ),
        DataSourceInfo(
            source_id="JHU-001",
            name="Johns Hopkins University",
            type="api",
            last_updated=datetime.now()
        ),
        DataSourceInfo(
            source_id="OWID-001",
            name="Our World in Data",
            type="csv",
            last_updated=datetime.now()
        )
    ]
    return sources

@router.get("/status/{ingestion_id}")
def get_ingestion_status(ingestion_id: str):
    """Obtener estado de una ingesta específica"""
    for file_info in uploaded_files_db:
        if file_info["ingestion_id"] == ingestion_id:
            return {
                "ingestion_id": ingestion_id,
                "status": "completed",
                "file_info": file_info
            }
    
    raise HTTPException(status_code=404, detail="Ingesta no encontrada")

@router.get("/history")
def get_ingestion_history():
    """Obtener historial de ingestas"""
    return {
        "total": len(uploaded_files_db),
        "uploads": uploaded_files_db
    }