from fastapi import APIRouter, UploadFile, File, HTTPException
from app.services.monitoring_service import monitoring_service, LogLevel
from app.services.databricks_service import databricks_service
from app.models.schemas import IngestionResponse, DataSourceInfo
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

uploaded_files_db = []

def crear_id_ingesta() -> str:
    return f"ING-{uuid.uuid4().hex[:8].upper()}"

def validate_schema(filename: str, size: int) -> tuple[bool, str]:
    allowed_extensions = ['.csv', '.xlsx', '.xls', '.json']
    ext = os.path.splitext(filename)[1].lower()
    
    if ext not in allowed_extensions:
        return False, f"Extensión no permitida: {ext}"
    
    max_size = 500 * 1024 * 1024
    if size > max_size:
        return False, f"Archivo > 500MB"
    
    if size == 0:
        return False, "Archivo vacío"
    
    return True, "OK"

def detect_encoding_smart(file_content: bytes, sample_size: int = 100000) -> str:
    try:
        sample = file_content[:sample_size]
        result = chardet.detect(sample)
        
        encoding = result.get('encoding', 'utf-8')
        confidence = result.get('confidence', 0)
        
        logger.info(f"Encoding: {encoding} ({confidence:.0%})")
        
        if confidence < 0.7:
            for enc in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
                try:
                    sample.decode(enc)
                    logger.info(f"Usando: {enc}")
                    return enc
                except:
                    continue
            return 'latin-1'
        
        return encoding if encoding else 'utf-8'
    except:
        return 'latin-1'

def detect_csv_delimiter(content_sample: str) -> str:
    delimiters = {
        ',': content_sample.count(','),
        ';': content_sample.count(';'),
        '\t': content_sample.count('\t'),
        '|': content_sample.count('|')
    }
    
    detected = max(delimiters, key=delimiters.get)
    logger.info(f"Delimitador: '{detected}'")
    return detected

def read_file_universal(file_content: bytes, filename: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Lee CSV, Excel, JSON automáticamente"""
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
        # CSV
        if ext == '.csv':
            logger.info("📄 Procesando CSV...")
            
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
                na_values=['', 'NULL', 'null', 'NA', 'N/A', 'nan', 'NaN'],
                keep_default_na=True,
                skip_blank_lines=True,
                on_bad_lines='skip',
                engine='python'
            )
            
            logger.info(f"✅ CSV: {len(df)} filas, {encoding}, '{delimiter}'")
        
        # Excel
        elif ext in ['.xlsx', '.xls']:
            logger.info("📊 Procesando Excel...")
            
            df = pd.read_excel(
                io.BytesIO(file_content),
                engine='openpyxl' if ext == '.xlsx' else 'xlrd',
                na_values=['', 'NULL', 'null', 'NA', 'N/A']
            )
            
            metadata["encoding"] = "N/A (Excel)"
            logger.info(f"✅ Excel: {len(df)} filas")
        
        # JSON
        elif ext == '.json':
            logger.info("📋 Procesando JSON...")
            
            try:
                df = pd.read_json(io.BytesIO(file_content), encoding='utf-8')
                metadata["encoding"] = "utf-8"
            except:
                encoding = detect_encoding_smart(file_content)
                json_str = file_content.decode(encoding)
                df = pd.read_json(io.StringIO(json_str))
                metadata["encoding"] = encoding
            
            logger.info(f"✅ JSON: {len(df)} filas")
        
        else:
            raise ValueError(f"Formato no soportado: {ext}")
        
        # Metadata
        metadata["original_columns"] = df.columns.tolist()
        metadata["row_count"] = len(df)
        metadata["null_counts"] = df.isnull().sum().to_dict()
        metadata["dtypes"] = df.dtypes.astype(str).to_dict()
        
        logger.info(f"📊 {len(df)} × {len(df.columns)} columnas")
        logger.info(f"📋 Columnas: {df.columns.tolist()}")
        
        # Detectar problemas
        dup_count = df.duplicated().sum()
        if dup_count > 0:
            metadata["issues"].append(f"{dup_count} duplicados")
        
        for col, null_count in metadata["null_counts"].items():
            if null_count > 0:
                null_pct = (null_count / len(df)) * 100
                if null_pct > 50:
                    metadata["issues"].append(f"'{col}': {null_pct:.0f}% nulos")
        
        if metadata["issues"]:
            for issue in metadata["issues"]:
                logger.warning(f"⚠️ {issue}")
        
        return df, metadata
    
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise Exception(f"Error leyendo {filename}: {str(e)}")


@router.post("/upload", response_model=IngestionResponse)
async def upload_covid_data(file: UploadFile = File(...)):
    """
    CARGA DINÁMICA: Crea tablas automáticamente según columnas del CSV
    
    ✅ Soporta: CSV, Excel, JSON
    ✅ Detecta encoding y delimitador automáticamente
    ✅ Crea tabla con el nombre del archivo
    ✅ No requiere esquema predefinido
    """
    try:
        ingestion_id = crear_id_ingesta()
        
        logger.info(f"📥 Archivo: {file.filename}")
        
        # Leer archivo
        contents = bytearray()
        chunk_size = 1024 * 1024
        
        while chunk := await file.read(chunk_size):
            contents.extend(chunk)
        
        file_size = len(contents)
        logger.info(f"📊 Tamaño: {file_size / 1024 / 1024:.2f}MB")
        
        # Validar
        is_valid, msg = validate_schema(file.filename, file_size)
        if not is_valid:
            raise HTTPException(status_code=400, detail=msg)
        
        # Procesar
        logger.info("🔍 Procesando...")
        df, metadata = read_file_universal(bytes(contents), file.filename)
        
        if len(df) == 0:
            raise HTTPException(status_code=400, detail="Sin registros válidos")
        
        records_count = len(df)
        logger.info(f"✅ {records_count:,} registros")
        logger.info(f"📋 Columnas: {metadata['original_columns']}")
        
        # GUARDAR EN DATABRICKS
        if databricks_service.host and databricks_service.token:
            logger.info("💾 Guardando en Databricks...")
            start_time = datetime.now()
            
            # 1. Setup inicial
            databricks_service.setup_database()
            
            # 2. Crear tabla dinámica
            table_name = databricks_service.create_dynamic_table_from_df(
                df=df,
                table_name=file.filename,
                drop_if_exists=False
            )
            
            logger.info(f"✅ Tabla '{table_name}' creada")
            
            # 3. Guardar RAW
            databricks_service.insert_raw_data(
                table_name=table_name,
                filename=file.filename,
                df=df,
                ingestion_id=ingestion_id
            )
            
            # 4. Insertar datos
            result = databricks_service.insert_dataframe(
                df=df,
                table_name=table_name,
                ingestion_id=ingestion_id,
                batch_size=1000
            )
            
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ {result['success']:,} registros en {elapsed:.1f}s")
            logger.info(f"⚡ Velocidad: {result['success']/elapsed:.0f} reg/s")
            
            # 5. Audit log
            databricks_service.insert_audit_log(
                process="ingestion_dynamic",
                level="INFO",
                message=f"Tabla '{table_name}' creada con {records_count:,} registros",
                metadata={
                    "table": table_name,
                    "file": file.filename,
                    "records": records_count,
                    "columns": metadata["original_columns"]
                }
            )
        else:
            logger.warning("⚠️ Databricks no configurado")
        
        # Guardar metadata
        file_info = {
            "ingestion_id": ingestion_id,
            "filename": file.filename,
            "table_name": table_name if databricks_service.host else None,
            "size_bytes": file_size,
            "records_count": records_count,
            "uploaded_at": datetime.now(),
            "metadata": metadata
        }
        
        uploaded_files_db.append(file_info)
        
        # Log monitoreo
        monitoring_service.log_event(
            process="Ingesta",
            level=LogLevel.SUCCESS,
            message=f"✅ {file.filename} ({records_count:,} registros)",
            data={
                "ingestion_id": ingestion_id,
                "table": table_name if databricks_service.host else "N/A",
                "records": records_count,
                "columns": metadata["original_columns"]
            }
        )
        
        return IngestionResponse(
            ingestion_id=ingestion_id,
            filename=file.filename,
            records_count=records_count,
            status="success",
            message=f"✅ Tabla '{table_name}' creada con {records_count:,} registros"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sources", response_model=List[DataSourceInfo])
def get_data_sources():
    """Fuentes disponibles"""
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
    """Estado de ingesta"""
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
    """Historial"""
    return {
        "total": len(uploaded_files_db),
        "uploads": uploaded_files_db
    }