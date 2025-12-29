import React, { useState, useEffect } from "react";
import {
  Activity,
  AlertTriangle,
  CheckCircle,
  Clock,
  RefreshCw,
} from "lucide-react";
import { fetchLogs, fetchProcessStatus } from "../services/monitoringAPI";

function ProcessMonitor() {
  const [processes, setProcesses] = useState([]);

  const [logs, setLogs] = useState([]);

  const [loading, setLoading] = useState(false);

  // Leer estado inicial de localStorage (por defecto true)
  const [autoRefresh, setAutoRefresh] = useState(() => {
    const saved = localStorage.getItem('monitorAutoRefresh');
    return saved !== null ? saved === 'true' : true;
  });

  const [error, setError] = useState(null);

  // Guardar en localStorage cuando cambie autoRefresh
  useEffect(() => {
    localStorage.setItem('monitorAutoRefresh', autoRefresh);
  }, [autoRefresh]);

  useEffect(() => {
    loadData();

    if (autoRefresh) {
      const interval = setInterval(loadData, 5000);
      return () => clearInterval(interval);
    }
  }, [autoRefresh]);

  const loadData = async () => {
  setLoading(true);
  setError(null);
  try {
    // REEMPLAZAR con llamadas reales:
    const [processData, logsData] = await Promise.all([
      fetchProcessStatus(),
      fetchLogs()
    ]);
    
    setProcesses(processData.processes || []);
    setLogs(logsData.logs || []);
    
  } catch (error) {
    console.error("Error loading monitoring data:", error);
    setError(error.message);
  } finally {
    setLoading(false);
  }
};

  const statusColors = {
    running: "bg-blue-100 text-blue-800 border-blue-300",
    completed: "bg-green-100 text-green-800 border-green-300",
    failed: "bg-red-100 text-red-800 border-red-300",
    pending: "bg-yellow-100 text-yellow-800 border-yellow-300",
  };

  const statusIcons = {
    running: Activity,
    completed: CheckCircle,
    failed: AlertTriangle,
    pending: Clock,
  };

  return (
    <div className="max-w-7xl mx-auto space-y-6 fade-in">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-espe-dark">
            Monitoreo del Sistema
          </h1>
          <p className="text-gray-600 mt-1">Módulo 6: Monitoreo y Auditoría</p>
        </div>
        <div className="flex gap-3">
          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            className={`px-4 py-2 rounded-lg font-semibold transition-colors ${
              autoRefresh
                ? "bg-espe-green text-white"
                : "bg-gray-200 text-gray-700"
            }`}
          >
            Auto-refresh {autoRefresh ? "ON" : "OFF"}
          </button>
          <button
            onClick={loadData}
            disabled={loading}
            className="bg-espe-green text-white px-4 py-2 rounded-lg hover:bg-espe-green-light transition-colors flex items-center gap-2 disabled:opacity-50"
          >
            <RefreshCw size={18} className={loading ? "animate-spin" : ""} />
            Actualizar
          </button>
        </div>
      </div>

      {/* Process Status Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {processes.map((process, idx) => {
          const StatusIcon = statusIcons[process.status] || Activity;
          return (
            <div
              key={idx}
              className="bg-white rounded-xl shadow-md p-6 hover:shadow-lg transition-shadow"
            >
              <div className="flex items-start justify-between mb-4">
                <div>
                  <h3 className="font-bold text-espe-dark">{process.name}</h3>
                  <p className="text-sm text-gray-600 mt-1">
                    {process.description}
                  </p>
                </div>
                <StatusIcon
                  size={24}
                  className={statusColors[process.status].split(" ")[1]}
                />
              </div>

              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-gray-600">Estado:</span>
                  <span
                    className={`px-2 py-1 rounded-full text-xs font-semibold border ${
                      statusColors[process.status]
                    }`}
                  >
                    {process.status}
                  </span>
                </div>

                <div className="flex justify-between text-sm">
                  <span className="text-gray-600">Última ejecución:</span>
                  <span className="font-medium">{process.last_run}</span>
                </div>

                {process.duration && (
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-600">Duración:</span>
                    <span className="font-medium">{process.duration}</span>
                  </div>
                )}

                {process.progress !== undefined && process.progress > 0 && (
                  <div className="mt-3">
                    <div className="flex justify-between text-xs mb-1">
                      <span className="text-gray-600">Progreso</span>
                      <span className="font-semibold">
                        {Math.round(process.progress)}%
                      </span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div
                        className="bg-espe-green h-2 rounded-full transition-all duration-300"
                        style={{ width: `${process.progress}%` }}
                      ></div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {/* Logs Table */}
      <div className="bg-white rounded-xl shadow-md overflow-hidden">
        <div className="p-6 border-b border-gray-200">
          <h3 className="text-lg font-bold text-espe-dark">Logs del Sistema</h3>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-espe-gray">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                  Timestamp
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                  Proceso
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                  Nivel
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                  Mensaje
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {logs.map((log, idx) => (
                <tr key={idx} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600">
                    {log.timestamp}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-espe-dark">
                    {log.process}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm">
                    <span
                      className={`px-2 py-1 rounded-full text-xs font-semibold ${
                        log.level === "ERROR"
                          ? "bg-red-100 text-red-800"
                          : log.level === "WARNING"
                          ? "bg-yellow-100 text-yellow-800"
                          : log.level === "INFO"
                          ? "bg-blue-100 text-blue-800"
                          : "bg-green-100 text-green-800"
                      }`}
                    >
                      {log.level}
                    </span>
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-700">
                    {log.message}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {logs.length === 0 && (
          <div className="p-12 text-center text-gray-500">
            No hay logs disponibles
          </div>
        )}
      </div>

      {/* Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-green-50 border border-green-200 rounded-lg p-4">
          <div className="flex items-center gap-3">
            <CheckCircle className="text-green-600" size={24} />
            <div>
              <p className="text-sm text-gray-600">Procesos exitosos</p>
              <p className="text-2xl font-bold text-espe-dark">
                {processes.filter((p) => p.status === "completed").length}
              </p>
            </div>
          </div>
        </div>

        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
          <div className="flex items-center gap-3">
            <Activity className="text-blue-600" size={24} />
            <div>
              <p className="text-sm text-gray-600">En ejecución</p>
              <p className="text-2xl font-bold text-espe-dark">
                {processes.filter((p) => p.status === "running").length}
              </p>
            </div>
          </div>
        </div>

        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <div className="flex items-center gap-3">
            <AlertTriangle className="text-red-600" size={24} />
            <div>
              <p className="text-sm text-gray-600">Fallidos</p>
              <p className="text-2xl font-bold text-espe-dark">
                {processes.filter((p) => p.status === "failed").length}
              </p>
            </div>
          </div>
        </div>

        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
          <div className="flex items-center gap-3">
            <Clock className="text-yellow-600" size={24} />
            <div>
              <p className="text-sm text-gray-600">Pendientes</p>
              <p className="text-2xl font-bold text-espe-dark">
                {processes.filter((p) => p.status === "pending").length}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Info */}
      <div className="bg-purple-50 border border-purple-200 rounded-lg p-4">
        <div className="flex gap-3">
          <Activity className="text-purple-600 flex-shrink-0" size={20} />
          <div>
            <h4 className="font-semibold text-purple-900">
              Monitoreo en Tiempo Real
            </h4>
            <p className="text-sm text-purple-800 mt-1">
              Todos los procesos son auditados y registrados en Delta Lake. El
              sistema mantiene trazabilidad completa de cada operación.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default ProcessMonitor;
