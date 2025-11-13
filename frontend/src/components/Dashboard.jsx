import React, { useState, useEffect } from "react";
import {
  TrendingUp,
  Users,
  AlertCircle,
  Activity,
  Download,
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
import { fetchDashboardMetrics } from "../services/dashboardAPI";

function Dashboard() {
  const [metrics, setMetrics] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadMetrics();
  }, []);

  const loadMetrics = async () => {
    try {
      const data = await fetchDashboardMetrics();
      setMetrics(data);
    } catch (error) {
      console.error("Error loading metrics:", error);
    } finally {
      setLoading(false);
    }
  };

  // Datos de ejemplo
  const timeSeriesData = [
    { date: "2024-01", casos: 4000, muertes: 240, vacunados: 3000 },
    { date: "2024-02", casos: 3000, muertes: 180, vacunados: 4500 },
    { date: "2024-03", casos: 2000, muertes: 120, vacunados: 6000 },
    { date: "2024-04", casos: 2780, muertes: 167, vacunados: 7200 },
    { date: "2024-05", casos: 1890, muertes: 113, vacunados: 8500 },
  ];

  const severityData = [
    { name: "Leve", value: 400, color: "#4CAF50" },
    { name: "Moderado", value: 300, color: "#FFC107" },
    { name: "Grave", value: 200, color: "#FF5722" },
    { name: "Crítico", value: 100, color: "#9C27B0" },
  ];

  const stats = [
    {
      title: "Total Casos",
      value: "15,234",
      change: "-12%",
      icon: Users,
      color: "bg-blue-500",
    },
    {
      title: "Casos Activos",
      value: "1,845",
      change: "-8%",
      icon: Activity,
      color: "bg-espe-green",
    },
    {
      title: "Recuperados",
      value: "12,456",
      change: "+5%",
      icon: TrendingUp,
      color: "bg-green-500",
    },
    {
      title: "Fallecidos",
      value: "933",
      change: "-3%",
      icon: AlertCircle,
      color: "bg-red-500",
    },
  ];

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-espe-green"></div>
      </div>
    );
  }

  return (
    <div className="space-y-6 fade-in">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-espe-dark">
            Dashboard COVID-19
          </h1>
          <p className="text-gray-600 mt-1">Módulo 5: Visualización de Datos</p>
        </div>
        <button className="flex items-center gap-2 bg-espe-green text-white px-4 py-2 rounded-lg hover:bg-espe-green-light transition-colors">
          <Download size={20} />
          Exportar Reporte
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
            Evolución Temporal
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={timeSeriesData}>
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
              />
              <Line
                type="monotone"
                dataKey="muertes"
                stroke="#F44336"
                strokeWidth={2}
              />
              <Line
                type="monotone"
                dataKey="vacunados"
                stroke="#4CAF50"
                strokeWidth={2}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Severity Distribution */}
        <div className="bg-white rounded-xl shadow-md p-6">
          <h3 className="text-lg font-bold text-espe-dark mb-4">
            Distribución por Severidad
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={severityData}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, percent }) =>
                  `${name} ${(percent * 100).toFixed(0)}%`
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
        </div>
      </div>

      {/* Bar Chart */}
      <div className="bg-white rounded-xl shadow-md p-6">
        <h3 className="text-lg font-bold text-espe-dark mb-4">
          Comparativa Mensual
        </h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={timeSeriesData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="casos" fill="#1B5E20" />
            <Bar dataKey="vacunados" fill="#4CAF50" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

export default Dashboard;
