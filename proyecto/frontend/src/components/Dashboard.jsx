import React, { useState, useEffect } from "react";
import {
  TrendingUp,
  Users,
  AlertCircle,
  Activity,
  Download,
  RefreshCw,
} from "lucide-react";
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import api from "../services/api";

function Dashboard() {
  const [metrics, setMetrics] = useState(null);
  const [timeSeries, setTimeSeries] = useState([]);
  const [severityData, setSeverityData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
  loadAllData();
  
  // AGREGAR: Auto-refresh cada 30 segundos
  const interval = setInterval(() => {
    loadAllData();
  }, 30000);
  
  return () => clearInterval(interval);
}, []);

  const loadAllData = async () => {
    setLoading(true);
    setError(null);
    try {
      // Cargar métricas principales
      const metricsRes = await api.get("/api/dashboard/metrics");
      setMetrics(metricsRes.data);

      // Cargar series temporales
      const timeSeriesRes = await api.get("/api/dashboard/timeseries?days=30");
      setTimeSeries(timeSeriesRes.data.data || []);

      // Cargar distribución de severidad
      const severityRes = await api.get("/api/dashboard/severity-distribution");
      const severityArray = severityRes.data || [];
      if (severityArray.length === 0) {
        setSeverityData([
          { name: "Sin datos", value: 1, color: "#999999" }
        ]);
      } else {
        setSeverityData(severityArray);
      }

    } catch (err) {
      console.error("Error cargando datos:", err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-espe-green"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6">
        <div className="flex items-center gap-3">
          <AlertCircle className="text-red-600" size={24} />
          <div>
            <h3 className="font-bold text-red-900">Error al cargar datos</h3>
            <p className="text-sm text-red-800 mt-1">{error}</p>
            <button
              onClick={loadAllData}
              className="mt-3 text-sm text-red-600 underline"
            >
              Reintentar
            </button>
          </div>
        </div>
      </div>
    );
  }

  const stats = [
    {
      title: "Total Casos",
      value: metrics?.total_cases?.toLocaleString() || "0",
      change: "-12%",
      icon: Users,
      color: "bg-blue-500",
    },
    {
      title: "Casos Activos",
      value: metrics?.active_cases?.toLocaleString() || "0",
      change: "-8%",
      icon: Activity,
      color: "bg-espe-green",
    },
    {
      title: "Recuperados",
      value: metrics?.recovered?.toLocaleString() || "0",
      change: "+5%",
      icon: TrendingUp,
      color: "bg-green-500",
    },
    {
      title: "Fallecidos",
      value: metrics?.deaths?.toLocaleString() || "0",
      change: "-3%",
      icon: AlertCircle,
      color: "bg-red-500",
    },
  ];

  return (
    <div className="space-y-6 fade-in">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-espe-dark">
            Visualización de Datos
          </h1>
          <p className="text-gray-600 mt-1">Módulo 5: Visualización de Datos</p>
          <p className="text-xs text-gray-500 mt-1">
            Última actualización: {metrics?.last_updated ? new Date(metrics.last_updated).toLocaleString() : "N/A"}
          </p>
        </div>
        <button 
          onClick={loadAllData}
          className="flex items-center gap-2 bg-espe-green text-white px-4 py-2 rounded-lg hover:bg-espe-green-light transition-colors"
        >
          <RefreshCw size={20} />
          Actualizar
        </button>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {stats.map((stat, index) => {
          const Icon = stat.icon;
          return (
            <div
              key={index}
              className="bg-white rounded-xl shadow-md p-6 hover:shadow-lg transition-shadow"
            >
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-gray-600 text-sm">{stat.title}</p>
                  <h3 className="text-2xl font-bold text-espe-dark mt-1">
                    {stat.value}
                  </h3>
                  <p
                    className={`text-sm mt-2 ${
                      stat.change.startsWith("+")
                        ? "text-green-600"
                        : "text-red-600"
                    }`}
                  >
                    {stat.change} vs mes anterior
                  </p>
                </div>
                <div className={`${stat.color} p-3 rounded-lg`}>
                  <Icon size={24} className="text-white" />
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Time Series Chart */}
        <div className="bg-white rounded-xl shadow-md p-6">
          <h3 className="text-lg font-bold text-espe-dark mb-4">
            Evolución Temporal (últimos 30 días)
          </h3>
          {timeSeries.length > 0 ? (
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={timeSeries}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Line
                  type="monotone"
                  dataKey="casos"
                  stroke="#2196F3"
                  strokeWidth={2}
                  name="Casos"
                />
                <Line
                  type="monotone"
                  dataKey="muertes"
                  stroke="#F44336"
                  strokeWidth={2}
                  name="Muertes"
                />
                <Line
                  type="monotone"
                  dataKey="vacunados"
                  stroke="#4CAF50"
                  strokeWidth={2}
                  name="Vacunados"
                />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-[300px] flex items-center justify-center text-gray-500">
              No hay datos de series temporales disponibles
            </div>
          )}
        </div>

        {/* Severity Distribution */}
        <div className="bg-white rounded-xl shadow-md p-6">
          <h3 className="text-lg font-bold text-espe-dark mb-4">
            Distribución por Severidad
          </h3>
          {severityData.some(d => d.value > 0) ? (
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={severityData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, percent }) =>
                    percent > 0 ? `${name} ${(percent * 100).toFixed(0)}%` : ''
                  }
                  outerRadius={100}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {severityData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-[300px] flex items-center justify-center text-gray-500">
              No hay casos clasificados todavía. <br />
              Ve al módulo de Clasificación para etiquetar casos.
            </div>
          )}
        </div>
      </div>

      {/* Bar Chart */}
      <div className="bg-white rounded-xl shadow-md p-6">
        <h3 className="text-lg font-bold text-espe-dark mb-4">
          Comparativa Mensual
        </h3>
        {timeSeries.length > 0 ? (
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={timeSeries}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="date" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Bar dataKey="casos" fill="#1B5E20" name="Casos" />
              <Bar dataKey="vacunados" fill="#4CAF50" name="Vacunados" />
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <div className="h-[300px] flex items-center justify-center text-gray-500">
            No hay datos disponibles para mostrar
          </div>
        )}
      </div>

      {/* Info */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <div className="flex gap-3">
          <AlertCircle className="text-blue-600 flex-shrink-0" size={20} />
          <div>
            <h4 className="font-semibold text-blue-900">Estado del Sistema</h4>
            <p className="text-sm text-blue-800 mt-1">
              {metrics?.total_cases > 0 
                ? `Hay ${metrics.total_cases} casos en el sistema. Datos actualizados desde Databricks.`
                : "No hay datos cargados todavía. Usa el módulo de Carga de Datos para subir un archivo CSV."
              }
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Dashboard;