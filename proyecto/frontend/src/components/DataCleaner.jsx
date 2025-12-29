import React, { useState, useEffect } from "react";
import { Sparkles, CheckCircle, AlertCircle, TrendingUp, XCircle, Clock, History } from "lucide-react";
import api from "../services/api";

function DataCleaner() {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [history, setHistory] = useState([]);
  const [loadingHistory, setLoadingHistory] = useState(false);

  // Cargar historial al montar el componente
  useEffect(() => {
    fetchHistory();
  }, []);

  const fetchHistory = async () => {
    setLoadingHistory(true);
    try {
      const response = await api.get("/api/clean/cleaning-history?limit=5");
      setHistory(response.data.history || []);
    } catch (err) {
      console.error("Error cargando historial:", err);
    } finally {
      setLoadingHistory(false);
    }
  };

  const handleCleanData = async () => {
    setLoading(true);
    setResult(null);
    setError(null);

    try {
      const response = await api.post("/api/clean/clean-databricks");
      setResult(response.data);
      // Recargar historial después de limpieza exitosa
      await fetchHistory();
    } catch (err) {
      console.error("Error limpiando datos:", err);
      setError(err.response?.data?.detail || err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-4xl mx-auto space-y-6 fade-in">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-espe-dark">Limpieza de Datos</h1>
        <p className="text-gray-600 mt-1">Módulo 2: Procesamiento y Limpieza</p>
      </div>

      {/* Main Card */}
      <div className="bg-white rounded-xl shadow-md p-8">
        <div className="text-center">
          <div className="inline-flex items-center justify-center w-20 h-20 bg-espe-green/10 rounded-full mb-4">
            <Sparkles size={40} className="text-espe-green" />
          </div>

          <h2 className="text-2xl font-bold text-espe-dark mb-2">
            Limpieza Automática
          </h2>

          <p className="text-gray-600 mb-6 max-w-2xl mx-auto">
            Este proceso automático limpiará tu <strong>tabla más reciente</strong> eliminando duplicados,
            valores nulos y outliers. Los datos limpios se guardarán en una nueva tabla con sufijo "_clean".
          </p>

          <button
            onClick={handleCleanData}
            disabled={loading}
            className="bg-espe-green text-white px-8 py-4 rounded-lg hover:bg-espe-green-light transition-colors disabled:opacity-50 disabled:cursor-not-allowed text-lg font-semibold shadow-lg"
          >
            {loading ? (
              <div className="flex items-center gap-3">
                <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
                <span>Limpiando datos...</span>
              </div>
            ) : (
              <div className="flex items-center gap-2">
                <Sparkles size={20} />
                <span>Limpiar Datos</span>
              </div>
            )}
          </button>
        </div>

        {/* What it does */}
        <div className="mt-8 grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="bg-blue-50 p-4 rounded-lg">
            <div className="flex items-center gap-2 mb-2">
              <CheckCircle className="text-blue-600" size={20} />
              <h4 className="font-semibold text-blue-900">Elimina Duplicados</h4>
            </div>
            <p className="text-sm text-blue-800">
              Identifica y elimina filas duplicadas para asegurar unicidad
            </p>
          </div>

          <div className="bg-green-50 p-4 rounded-lg">
            <div className="flex items-center gap-2 mb-2">
              <XCircle className="text-green-600" size={20} />
              <h4 className="font-semibold text-green-900">Elimina Nulos</h4>
            </div>
            <p className="text-sm text-green-800">
              Remueve filas con valores faltantes o nulos
            </p>
          </div>

          <div className="bg-purple-50 p-4 rounded-lg">
            <div className="flex items-center gap-2 mb-2">
              <TrendingUp className="text-purple-600" size={20} />
              <h4 className="font-semibold text-purple-900">Detecta Outliers</h4>
            </div>
            <p className="text-sm text-purple-800">
              Usa IQR para detectar y eliminar valores extremos
            </p>
          </div>
        </div>
      </div>

      {/* Results */}
      {result && (
        <div className="bg-white rounded-xl shadow-md p-6">
          <div className="flex items-center gap-3 mb-6">
            <CheckCircle className="text-green-600" size={32} />
            <div>
              <h3 className="text-xl font-bold text-espe-dark">
                ¡Limpieza Completada!
              </h3>
              <p className="text-sm text-gray-600">{result.message}</p>
            </div>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
            <div className="bg-blue-50 p-4 rounded-lg text-center">
              <p className="text-sm text-gray-600 mb-1">Originales</p>
              <p className="text-2xl font-bold text-espe-dark">
                {result.stats.original_records.toLocaleString()}
              </p>
            </div>

            <div className="bg-green-50 p-4 rounded-lg text-center">
              <p className="text-sm text-gray-600 mb-1">Limpios</p>
              <p className="text-2xl font-bold text-green-700">
                {result.stats.clean_records.toLocaleString()}
              </p>
            </div>

            <div className="bg-red-50 p-4 rounded-lg text-center">
              <p className="text-sm text-gray-600 mb-1">Duplicados</p>
              <p className="text-2xl font-bold text-red-700">
                {result.stats.duplicates_removed.toLocaleString()}
              </p>
            </div>

            <div className="bg-yellow-50 p-4 rounded-lg text-center">
              <p className="text-sm text-gray-600 mb-1">Nulos</p>
              <p className="text-2xl font-bold text-yellow-700">
                {result.stats.nulls_removed.toLocaleString()}
              </p>
            </div>

            <div className="bg-purple-50 p-4 rounded-lg text-center">
              <p className="text-sm text-gray-600 mb-1">Outliers</p>
              <p className="text-2xl font-bold text-purple-700">
                {result.stats.outliers_removed.toLocaleString()}
              </p>
            </div>
          </div>

          <div className="bg-espe-green/10 border border-espe-green rounded-lg p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="font-semibold text-espe-dark">Tabla Original:</p>
                <p className="text-sm text-gray-600">{result.original_table}</p>
              </div>
              <div>
                <p className="font-semibold text-espe-dark">Tabla Limpia:</p>
                <p className="text-sm text-gray-600">{result.clean_table}</p>
              </div>
              <div>
                <p className="font-semibold text-espe-dark">Calidad:</p>
                <p className="text-2xl font-bold text-espe-green">
                  {result.stats.quality_score}%
                </p>
              </div>
            </div>
          </div>

          <p className="text-xs text-gray-500 mt-4 text-center">
            Tiempo de procesamiento: {result.elapsed_seconds.toFixed(1)} segundos
          </p>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-6">
          <div className="flex items-start gap-3">
            <AlertCircle className="text-red-600 flex-shrink-0" size={24} />
            <div>
              <h4 className="font-semibold text-red-900">Error en la limpieza</h4>
              <p className="text-sm text-red-800 mt-1">{error}</p>
              <button
                onClick={handleCleanData}
                className="mt-3 text-sm text-red-600 underline"
              >
                Reintentar
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Important Info */}
      <div className="bg-yellow-50 border border-yellow-300 rounded-lg p-4 mb-4">
        <div className="flex gap-3">
          <AlertCircle className="text-yellow-600 flex-shrink-0" size={20} />
          <div>
            <h4 className="font-semibold text-yellow-900">Importante</h4>
            <ul className="text-sm text-yellow-800 mt-1 space-y-1">
              <li>• Se limpiará la <strong>tabla más reciente</strong> (última ingesta)</li>
              <li>• No se puede limpiar la misma tabla dos veces</li>
              <li>• Si ya existe una tabla "_clean", se mostrará un error</li>
            </ul>
          </div>
        </div>
      </div>

      {/* Info */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <div className="flex gap-3">
          <AlertCircle className="text-blue-600 flex-shrink-0" size={20} />
          <div>
            <h4 className="font-semibold text-blue-900">
              Proceso Automático y Dinámico
            </h4>
            <p className="text-sm text-blue-800 mt-1">
              La limpieza se ejecuta directamente en Databricks y funciona con
              cualquier estructura de datos. Los datos originales se mantienen
              intactos en la tabla original.
            </p>
          </div>
        </div>
      </div>

      {/* Historial de Limpiezas */}
      <div className="bg-white rounded-xl shadow-md p-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <History className="text-espe-dark" size={24} />
            <h3 className="text-xl font-bold text-espe-dark">
              Historial de Limpiezas
            </h3>
          </div>
          <button
            onClick={fetchHistory}
            disabled={loadingHistory}
            className="text-sm text-espe-green hover:text-espe-green-light transition-colors"
          >
            {loadingHistory ? "Cargando..." : "Actualizar"}
          </button>
        </div>

        {history.length === 0 ? (
          <p className="text-gray-500 text-center py-8">
            No hay limpiezas registradas aún
          </p>
        ) : (
          <div className="space-y-3">
            {history.map((item, index) => (
              <div
                key={index}
                className="border border-gray-200 rounded-lg p-4 hover:bg-gray-50 transition-colors"
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="flex items-center gap-2 mb-2">
                      <Clock size={16} className="text-gray-500" />
                      <span className="text-sm text-gray-600">
                        {new Date(item.timestamp).toLocaleString('es-ES', {
                          year: 'numeric',
                          month: 'short',
                          day: 'numeric',
                          hour: '2-digit',
                          minute: '2-digit'
                        })}
                      </span>
                    </div>
                    <p className="font-semibold text-espe-dark mb-1">
                      {item.original_table} → {item.clean_table}
                    </p>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-2 mt-2 text-xs">
                      <div className="bg-blue-50 px-2 py-1 rounded">
                        <span className="text-gray-600">Originales: </span>
                        <span className="font-semibold text-blue-700">
                          {item.original_records.toLocaleString()}
                        </span>
                      </div>
                      <div className="bg-green-50 px-2 py-1 rounded">
                        <span className="text-gray-600">Limpios: </span>
                        <span className="font-semibold text-green-700">
                          {item.clean_records.toLocaleString()}
                        </span>
                      </div>
                      <div className="bg-purple-50 px-2 py-1 rounded">
                        <span className="text-gray-600">Calidad: </span>
                        <span className="font-semibold text-purple-700">
                          {item.quality_score}%
                        </span>
                      </div>
                      <div className="bg-gray-50 px-2 py-1 rounded">
                        <span className="text-gray-600">Tiempo: </span>
                        <span className="font-semibold text-gray-700">
                          {item.elapsed_seconds.toFixed(1)}s
                        </span>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

export default DataCleaner;
