import React, { useState } from "react";
import { Play, Trash2, Filter, CheckCircle, AlertCircle } from "lucide-react";
import { runCleaningJob, getCleaningStatus } from "../services/cleaningAPI";

function DataCleaner() {
  const [jobId, setJobId] = useState(null);
  const [status, setStatus] = useState(null);
  const [running, setRunning] = useState(false);
  const [config, setConfig] = useState({
    removeDuplicates: true,
    handleMissing: "drop",
    detectOutliers: true,
    standardizeFormats: true,
  });

  const handleStartCleaning = async () => {
    setRunning(true);
    try {
      // Convertir config a snake_case para el backend
      const backendConfig = {
        remove_duplicates: config.removeDuplicates,
        handle_missing: config.handleMissing,
        detect_outliers: config.detectOutliers,
        standardize_formats: config.standardizeFormats,
      };
      const response = await runCleaningJob(backendConfig);
      setJobId(response.job_id);
      pollJobStatus(response.job_id);
    } catch (error) {
      console.error("Error starting cleaning:", error);
      setRunning(false);
    }
  };

  const pollJobStatus = async (id) => {
    const interval = setInterval(async () => {
      try {
        const statusResponse = await getCleaningStatus(id);
        setStatus(statusResponse);

        if (
          statusResponse.status === "completed" ||
          statusResponse.status === "failed"
        ) {
          clearInterval(interval);
          setRunning(false);
        }
      } catch (error) {
        clearInterval(interval);
        setRunning(false);
      }
    }, 2000);
  };

  return (
    <div className="max-w-6xl mx-auto space-y-6 fade-in">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-espe-dark">Limpieza de Datos</h1>
        <p className="text-gray-600 mt-1">Módulo 3: Procesamiento y Limpieza</p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Configuration Panel */}
        <div className="lg:col-span-1">
          <div className="bg-white rounded-xl shadow-md p-6">
            <h3 className="text-lg font-bold text-espe-dark mb-4 flex items-center gap-2">
              <Filter size={20} className="text-espe-green" />
              Configuración
            </h3>

            <div className="space-y-4">
              <label className="flex items-center gap-3">
                <input
                  type="checkbox"
                  checked={config.removeDuplicates}
                  onChange={(e) =>
                    setConfig({ ...config, removeDuplicates: e.target.checked })
                  }
                  className="w-5 h-5 text-espe-green rounded focus:ring-espe-green"
                />
                <span className="text-sm">Eliminar duplicados</span>
              </label>

              <label className="flex items-center gap-3">
                <input
                  type="checkbox"
                  checked={config.detectOutliers}
                  onChange={(e) =>
                    setConfig({ ...config, detectOutliers: e.target.checked })
                  }
                  className="w-5 h-5 text-espe-green rounded focus:ring-espe-green"
                />
                <span className="text-sm">Detectar outliers</span>
              </label>

              <label className="flex items-center gap-3">
                <input
                  type="checkbox"
                  checked={config.standardizeFormats}
                  onChange={(e) =>
                    setConfig({
                      ...config,
                      standardizeFormats: e.target.checked,
                    })
                  }
                  className="w-5 h-5 text-espe-green rounded focus:ring-espe-green"
                />
                <span className="text-sm">Estandarizar formatos</span>
              </label>

              <div>
                <label className="block text-sm font-medium mb-2">
                  Manejo de valores nulos
                </label>
                <select
                  value={config.handleMissing}
                  onChange={(e) =>
                    setConfig({ ...config, handleMissing: e.target.value })
                  }
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 focus:ring-2 focus:ring-espe-green focus:border-transparent"
                >
                  <option value="drop">Eliminar filas</option>
                  <option value="fill_mean">Rellenar con media</option>
                  <option value="fill_median">Rellenar con mediana</option>
                  <option value="fill_zero">Rellenar con cero</option>
                </select>
              </div>
            </div>

            <button
              onClick={handleStartCleaning}
              disabled={running}
              className="w-full mt-6 bg-espe-green text-white px-4 py-3 rounded-lg hover:bg-espe-green-light transition-colors disabled:opacity-50 flex items-center justify-center gap-2"
            >
              <Play size={20} />
              {running ? "Procesando..." : "Iniciar Limpieza"}
            </button>
          </div>
        </div>

        {/* Results Panel */}
        <div className="lg:col-span-2 space-y-6">
          {/* Status Card */}
          {status && (
            <div className="bg-white rounded-xl shadow-md p-6">
              <h3 className="text-lg font-bold text-espe-dark mb-4">
                Estado del Proceso
              </h3>

              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-gray-600">Job ID:</span>
                  <span className="font-mono text-sm">{jobId}</span>
                </div>

                <div className="flex items-center justify-between">
                  <span className="text-gray-600">Estado:</span>
                  <span
                    className={`px-3 py-1 rounded-full text-sm font-semibold ${
                      status.status === "completed"
                        ? "bg-green-100 text-green-800"
                        : status.status === "running"
                        ? "bg-blue-100 text-blue-800"
                        : status.status === "failed"
                        ? "bg-red-100 text-red-800"
                        : "bg-gray-100 text-gray-800"
                    }`}
                  >
                    {status.status}
                  </span>
                </div>

                {status.progress && (
                  <div>
                    <div className="flex justify-between text-sm mb-1">
                      <span className="text-gray-600">Progreso</span>
                      <span className="font-semibold">{status.progress}%</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div
                        className="bg-espe-green h-2 rounded-full transition-all duration-300"
                        style={{ width: `${status.progress}%` }}
                      ></div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Results */}
          {status?.results && (
            <div className="bg-white rounded-xl shadow-md p-6">
              <h3 className="text-lg font-bold text-espe-dark mb-4">
                Resultados
              </h3>

              <div className="grid grid-cols-2 gap-4">
                <div className="bg-blue-50 p-4 rounded-lg">
                  <p className="text-sm text-gray-600">Registros originales</p>
                  <p className="text-2xl font-bold text-espe-dark mt-1">
                    {status.results.original_records?.toLocaleString()}
                  </p>
                </div>

                <div className="bg-green-50 p-4 rounded-lg">
                  <p className="text-sm text-gray-600">Registros limpios</p>
                  <p className="text-2xl font-bold text-espe-dark mt-1">
                    {status.results.clean_records?.toLocaleString()}
                  </p>
                </div>

                <div className="bg-red-50 p-4 rounded-lg">
                  <p className="text-sm text-gray-600">Duplicados eliminados</p>
                  <p className="text-2xl font-bold text-espe-dark mt-1">
                    {status.results.duplicates_removed?.toLocaleString()}
                  </p>
                </div>

                <div className="bg-yellow-50 p-4 rounded-lg">
                  <p className="text-sm text-gray-600">Outliers detectados</p>
                  <p className="text-2xl font-bold text-espe-dark mt-1">
                    {status.results.outliers_detected?.toLocaleString()}
                  </p>
                </div>
              </div>

              {status.results.issues && (
                <div className="mt-6">
                  <h4 className="font-semibold mb-3">Problemas detectados:</h4>
                  <ul className="space-y-2">
                    {status.results.issues.map((issue, idx) => (
                      <li key={idx} className="flex items-start gap-2">
                        <AlertCircle
                          size={16}
                          className="text-yellow-600 flex-shrink-0 mt-1"
                        />
                        <span className="text-sm text-gray-700">{issue}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}

          {/* Info */}
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
            <div className="flex gap-3">
              <AlertCircle className="text-blue-600 flex-shrink-0" size={20} />
              <div>
                <h4 className="font-semibold text-blue-900">
                  Proceso automatizado
                </h4>
                <p className="text-sm text-blue-800 mt-1">
                  La limpieza se ejecuta en Databricks usando Apache Spark para
                  procesamiento masivo. Los datos limpios se almacenan en Delta
                  Lake.
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default DataCleaner;
