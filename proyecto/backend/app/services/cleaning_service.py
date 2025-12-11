import pandas as pd
import numpy as np
from typing import Dict, Any, Optional
import uuid
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DataCleaningService:
    """Servicio para limpieza y validación de datos"""
    
    def __init__(self):
        self.cleaning_jobs = {}  # Almacena el estado de jobs en memoria
    
    def create_job_id(self) -> str:
        """Genera un ID único para el job de limpieza"""
        return f"CLEAN-{uuid.uuid4().hex[:8].upper()}"
    
    def remove_duplicates(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """
        Elimina registros duplicados
        Returns: (dataframe limpio, cantidad eliminada)
        """
        initial_count = len(df)
        df_clean = df.drop_duplicates()
        removed = initial_count - len(df_clean)
        
        logger.info(f"Duplicados eliminados: {removed}")
        return df_clean, removed
    
    def handle_missing_values(self, df: pd.DataFrame, strategy: str = "drop") -> tuple[pd.DataFrame, int]:
        """
        Maneja valores nulos según la estrategia especificada
        
        Estrategias:
        - drop: Elimina filas con nulos
        - fill_mean: Rellena con la media (columnas numéricas)
        - fill_median: Rellena con la mediana (columnas numéricas)
        - fill_zero: Rellena con cero
        """
        initial_count = len(df)
        
        if strategy == "drop":
            df_clean = df.dropna()
        
        elif strategy == "fill_mean":
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df_clean = df.copy()
            df_clean[numeric_cols] = df_clean[numeric_cols].fillna(df[numeric_cols].mean())
            df_clean = df_clean.fillna("Unknown")  # Para columnas no numéricas
        
        elif strategy == "fill_median":
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df_clean = df.copy()
            df_clean[numeric_cols] = df_clean[numeric_cols].fillna(df[numeric_cols].median())
            df_clean = df_clean.fillna("Unknown")
        
        elif strategy == "fill_zero":
            df_clean = df.fillna(0)
        
        else:
            raise ValueError(f"Estrategia no válida: {strategy}")
        
        affected = initial_count - len(df_clean)
        logger.info(f"Valores nulos manejados con estrategia '{strategy}': {affected} filas afectadas")
        
        return df_clean, affected
    
    def detect_outliers(self, df: pd.DataFrame, threshold: float = 3.0) -> tuple[pd.DataFrame, int, list]:
        """
        Detecta y marca outliers usando el método IQR (Interquartile Range)
        
        Returns: (dataframe, cantidad de outliers, lista de índices con outliers)
        """
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        outlier_indices = []
        
        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            # Definir límites
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            
            # Encontrar outliers
            col_outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)].index.tolist()
            outlier_indices.extend(col_outliers)
        
        # Eliminar duplicados en la lista de índices
        outlier_indices = list(set(outlier_indices))
        
        logger.info(f"Outliers detectados: {len(outlier_indices)}")
        return df, len(outlier_indices), outlier_indices
    
    def standardize_formats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Estandariza formatos de datos comunes
        - Fechas
        - Strings (lowercase, trim)
        - Booleanos
        """
        df_clean = df.copy()
        
        # Estandarizar strings: lowercase y trim
        string_cols = df_clean.select_dtypes(include=['object']).columns
        for col in string_cols:
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].str.lower().str.strip()
        
        # Intentar convertir columnas de fecha
        date_columns = ['date', 'fecha', 'timestamp', 'created_at', 'updated_at']
        for col in df_clean.columns:
            if any(date_keyword in col.lower() for date_keyword in date_columns):
                try:
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                except:
                    pass
        
        logger.info("Formatos estandarizados")
        return df_clean
    
    def validate_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida la calidad general de los datos
        """
        total_rows = len(df)
        total_cols = len(df.columns)
        
        # Contar nulos por columna
        null_counts = df.isnull().sum().to_dict()
        null_percentage = {col: (count / total_rows * 100) for col, count in null_counts.items()}
        
        # Columnas con más del 50% de nulos
        high_null_cols = [col for col, pct in null_percentage.items() if pct > 50]
        
        quality_report = {
            "total_rows": total_rows,
            "total_columns": total_cols,
            "null_counts": null_counts,
            "null_percentage": null_percentage,
            "high_null_columns": high_null_cols,
            "data_types": df.dtypes.astype(str).to_dict()
        }
        
        return quality_report
    
    def clean_covid_data(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Pipeline completo de limpieza de datos COVID-19
        
        Args:
            df: DataFrame con datos crudos
            config: Configuración de limpieza
                {
                    "remove_duplicates": bool,
                    "handle_missing": str,
                    "detect_outliers": bool,
                    "standardize_formats": bool
                }
        
        Returns:
            Dict con resultados y estadísticas
        """
        results = {
            "original_records": len(df),
            "duplicates_removed": 0,
            "null_handling": 0,
            "outliers_detected": 0,
            "outliers_removed": 0,
            "final_records": 0,
            "issues": []
        }
        
        df_clean = df.copy()
        
        # 1. Remover duplicados
        if config.get("remove_duplicates", True):
            df_clean, dup_count = self.remove_duplicates(df_clean)
            results["duplicates_removed"] = dup_count
            if dup_count > 0:
                results["issues"].append(f"Se encontraron {dup_count} duplicados")
        
        # 2. Manejar valores nulos
        handle_strategy = config.get("handle_missing", "drop")
        df_clean, null_count = self.handle_missing_values(df_clean, handle_strategy)
        results["null_handling"] = null_count
        if null_count > 0:
            results["issues"].append(f"Se manejaron {null_count} valores nulos con estrategia '{handle_strategy}'")
        
        # 3. Detectar outliers
        if config.get("detect_outliers", True):
            df_clean, outlier_count, outlier_indices = self.detect_outliers(df_clean)
            results["outliers_detected"] = outlier_count
            if outlier_count > 0:
                results["issues"].append(f"Se detectaron {outlier_count} outliers")
                # Opcionalmente remover outliers
                # df_clean = df_clean.drop(outlier_indices)
                # results["outliers_removed"] = outlier_count
        
        # 4. Estandarizar formatos
        if config.get("standardize_formats", True):
            df_clean = self.standardize_formats(df_clean)
        
        results["final_records"] = len(df_clean)
        results["quality_improvement"] = round(
            (results["original_records"] - results["final_records"]) / results["original_records"] * 100, 2
        )
        
        return df_clean, results
    
    def start_cleaning_job(self, job_id: str, config: Dict[str, Any]) -> None:
        """
        Inicia un job de limpieza (simulado para ahora, en producción sería async)
        """
        self.cleaning_jobs[job_id] = {
            "status": "pending",
            "progress": 0,
            "started_at": datetime.now(),
            "config": config,
            "results": None
        }
        
        logger.info(f"Job {job_id} iniciado")
    
    def update_job_progress(self, job_id: str, progress: int, status: str = "running"):
        """Actualiza el progreso de un job"""
        if job_id in self.cleaning_jobs:
            self.cleaning_jobs[job_id]["progress"] = progress
            self.cleaning_jobs[job_id]["status"] = status
    
    def complete_job(self, job_id: str, results: Dict[str, Any]):
        """Marca un job como completado"""
        if job_id in self.cleaning_jobs:
            self.cleaning_jobs[job_id]["status"] = "completed"
            self.cleaning_jobs[job_id]["progress"] = 100
            self.cleaning_jobs[job_id]["completed_at"] = datetime.now()
            self.cleaning_jobs[job_id]["results"] = results
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene el estado de un job"""
        return self.cleaning_jobs.get(job_id)


# Instancia global del servicio
cleaning_service = DataCleaningService()