import React, { useState, useEffect } from "react";
import {
  TrendingUp,
  Users,
  AlertCircle,
  Activity,
  Download,
  RefreshCw,
  Database,
  Table,
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
  const [schema, setSchema] = useState(null);
  const [dataPreview, setDataPreview] = useState([]);
  const [columnStats, setColumnStats] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [availableTables, setAvailableTables] = useState([]);
  const [selectedTableType, setSelectedTableType] = useState('auto'); // 'auto', 'original', 'clean', 'classified'

  useEffect(() => {
    loadAllData();
  }, [selectedTableType]);

  useEffect(() => {
    loadAvailableTables();
  }, []);

  const loadAvailableTables = async () => {
    try {
      const response = await api.get("/api/dashboard/available-tables");
      console.log("Tablas disponibles:", response.data);
      setAvailableTables(response.data.tables || []);
    } catch (err) {
      console.error("Error cargando tablas disponibles:", err);
      // Si falla, asumir que al menos hay original
      setAvailableTables(['original']);
    }
  };

  const loadAllData = async () => {
    setLoading(true);
    setError(null);
    try {
      // Recargar tablas disponibles también
      await loadAvailableTables();

      // Determinar qué tabla cargar según selección
      const tableParam = selectedTableType !== 'auto' ? `?table_type=${selectedTableType}` : '';

      // 1. Cargar métricas principales (solo total_cases funciona dinámicamente)
      const metricsRes = await api.get(`/api/dashboard/metrics${tableParam}`);
      setMetrics(metricsRes.data);

      // 2. Cargar esquema de la tabla (DINÁMICO)
      const schemaRes = await api.get(`/api/dashboard/schema${tableParam}`);
      setSchema(schemaRes.data);

      // 3. Cargar vista previa de datos
      const previewRes = await api.get(`/api/dashboard/data-preview?limit=100${tableParam.replace('?', '&')}`);
      setDataPreview(previewRes.data.data || []);

      // 4. Cargar estadísticas INTELIGENTES según tipo de tabla
      if (schemaRes.data.columns && schemaRes.data.columns.length > 0) {
        const stats = {};

        // Determinar qué columnas analizar según el tipo de tabla
        let columnsToAnalyze = [];

        if (selectedTableType === 'classified' || schemaRes.data.table_name?.includes('_classified')) {
          // Si es tabla CLASIFICADA: Buscar columnas que terminen en _categoria, _grupo, _rango, etc.
          columnsToAnalyze = schemaRes.data.columns.filter(col =>
            col.name.includes('_categoria') ||
            col.name.includes('_grupo') ||
            col.name.includes('_rango') ||
            col.name.includes('_anio') ||
            col.name.includes('_mes') ||
            col.name.includes('_trimestre')
          ).slice(0, 3);

          // Si no hay columnas clasificadas, tomar las primeras 3
          if (columnsToAnalyze.length === 0) {
            columnsToAnalyze = schemaRes.data.columns.slice(0, 3);
          }
        } else {
          // Para tablas ORIGINAL o CLEAN: Tomar primeras 2 columnas (más rápido)
          columnsToAnalyze = schemaRes.data.columns.slice(0, 2);
        }

        for (const col of columnsToAnalyze) {
          try {
            // ARREGLO: Construir URL correctamente
            const columnStatsUrl = tableParam
              ? `/api/dashboard/column-stats/${encodeURIComponent(col.name)}${tableParam}`
              : `/api/dashboard/column-stats/${encodeURIComponent(col.name)}`;
            const statRes = await api.get(columnStatsUrl);
            stats[col.name] = statRes.data;
          } catch (err) {
            console.warn(`No se pudieron cargar stats para ${col.name}`);
          }
        }
        console.log("Estadísticas de columnas cargadas:", stats);
        setColumnStats(stats);
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

  // Detectar si los datos son limpios o no
  const isCleanData = schema?.table_name?.endsWith('_clean') || false;
  const tableDisplayName = schema?.table_name || "N/A";

  // Stats dinámicos basados en el esquema
  const stats = [
    {
      title: "Total Registros",
      value: metrics?.total_cases?.toLocaleString() || "0",
      icon: Database,
      color: "bg-blue-500",
    },
    {
      title: "Total Columnas",
      value: schema?.total_columns || "0",
      icon: Table,
      color: "bg-espe-green",
    },
    {
      title: "Tabla Activa",
      value: tableDisplayName.length > 20 ? tableDisplayName.substring(0, 20) + "..." : tableDisplayName,
      fullValue: tableDisplayName,
      icon: Activity,
      color: "bg-purple-500",
      isTableName: true,
    },
    {
      title: "Última Actualización",
      value: metrics?.last_updated ? new Date(metrics.last_updated).toLocaleTimeString() : "N/A",
      icon: RefreshCw,
      color: "bg-orange-500",
    },
  ];

  return (
    <div className="space-y-6 fade-in">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <div className="flex items-center gap-3">
            <h1 className="text-3xl font-bold text-espe-dark">
              Visualización de Datos
            </h1>
            {isCleanData ? (
              <span className="px-3 py-1 bg-green-100 text-green-800 text-sm font-semibold rounded-full border border-green-300 flex items-center gap-1">
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
                DATOS LIMPIOS
              </span>
            ) : (
              <span className="px-3 py-1 bg-yellow-100 text-yellow-800 text-sm font-semibold rounded-full border border-yellow-300 flex items-center gap-1">
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                </svg>
                DATOS ORIGINALES
              </span>
            )}
          </div>
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

      {/* Selector de Tabla */}
      <div className="bg-white rounded-xl shadow-md p-4">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-sm font-semibold text-gray-700">Ver tabla:</span>
          <div className="flex gap-2">
            <button
              onClick={() => setSelectedTableType('auto')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                selectedTableType === 'auto'
                  ? 'bg-espe-green text-white'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              Automático
            </button>
            {availableTables.includes('original') && (
              <button
                onClick={() => setSelectedTableType('original')}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                  selectedTableType === 'original'
                    ? 'bg-yellow-500 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                Datos Originales
              </button>
            )}
            {availableTables.includes('clean') && (
              <button
                onClick={() => setSelectedTableType('clean')}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                  selectedTableType === 'clean'
                    ? 'bg-green-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                Datos Limpios
              </button>
            )}
            {availableTables.includes('classified') && (
              <button
                onClick={() => setSelectedTableType('classified')}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                  selectedTableType === 'classified'
                    ? 'bg-purple-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                Datos Clasificados
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {stats.map((stat, index) => {
          const Icon = stat.icon;
          return (
            <div
              key={index}
              className="bg-white rounded-xl shadow-md p-6 hover:shadow-lg transition-shadow"
              title={stat.fullValue || stat.value}
            >
              <div className="flex items-center justify-between">
                <div className="flex-1 min-w-0">
                  <p className="text-gray-600 text-sm">{stat.title}</p>
                  <h3 className={`font-bold text-espe-dark mt-1 ${
                    stat.isTableName && stat.fullValue && stat.fullValue.length > 20
                      ? 'text-base break-words'
                      : 'text-2xl'
                  }`}>
                    {stat.fullValue || stat.value}
                  </h3>
                </div>
                <div className={`${stat.color} p-3 rounded-lg flex-shrink-0 ml-2`}>
                  <Icon size={24} className="text-white" />
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Gráficas Dinámicas */}
      {Object.keys(columnStats).length > 0 && (
        <div className="space-y-4">
          {/* Indicador de tipo de visualización */}
          {(selectedTableType === 'classified' || schema?.table_name?.includes('_classified')) && (
            <div className="bg-purple-50 border border-purple-200 rounded-lg p-3">
              <p className="text-sm text-purple-800 font-semibold">
                ✨ Mostrando columnas clasificadas automáticamente
              </p>
            </div>
          )}

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {Object.entries(columnStats).map(([colName, stats], idx) => {
              // Crear datos para gráfica de barras
              const chartData = stats.top_values?.slice(0, 10).map(item => ({
                name: String(item.value).substring(0, 20),
                value: item.count
              })) || [];

              const colors = ['#1B5E20', '#2E7D32', '#388E3C', '#43A047', '#4CAF50', '#66BB6A', '#81C784', '#A5D6A7', '#C8E6C9', '#E8F5E9'];

              // Detectar si es columna clasificada
              const isClassified = colName.includes('_categoria') ||
                                   colName.includes('_grupo') ||
                                   colName.includes('_rango') ||
                                   colName.includes('_anio') ||
                                   colName.includes('_mes') ||
                                   colName.includes('_trimestre');

              return (
                <div key={colName} className="bg-white rounded-xl shadow-md p-6">
                  <div className="flex items-center gap-2 mb-4">
                    <h3 className="text-lg font-bold text-espe-dark">
                      Distribución: {colName}
                    </h3>
                    {isClassified && (
                      <span className="px-2 py-1 bg-purple-100 text-purple-700 text-xs font-semibold rounded">
                        Clasificada
                      </span>
                    )}
                  </div>
                {chartData.length > 0 ? (
                  <div>
                    <ResponsiveContainer width="100%" height={300}>
                      <BarChart data={chartData}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis
                          dataKey="name"
                          angle={-45}
                          textAnchor="end"
                          height={100}
                          interval={0}
                        />
                        <YAxis />
                        <Tooltip />
                        <Bar dataKey="value" fill={colors[idx % colors.length]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                ) : (
                  <div className="h-[300px] flex items-center justify-center text-gray-500">
                    No hay datos suficientes para graficar
                  </div>
                )}
              </div>
            );
          })}
          </div>
        </div>
      )}

      {/* Data Preview Table - SOLO para datos originales */}
      {selectedTableType === 'original' && (
        <div className="bg-white rounded-xl shadow-md p-6">
          <h3 className="text-lg font-bold text-espe-dark mb-4">
            Vista Previa de Datos Originales (primeras 100 filas)
          </h3>
          {dataPreview.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    {schema?.columns?.slice(0, 10).map((col, idx) => (
                      <th
                        key={idx}
                        className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                      >
                        {col.name}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {dataPreview.slice(0, 10).map((row, rowIdx) => (
                    <tr key={rowIdx} className="hover:bg-gray-50">
                      {schema?.columns?.slice(0, 10).map((col, colIdx) => (
                        <td
                          key={colIdx}
                          className="px-4 py-2 whitespace-nowrap text-sm text-gray-900"
                        >
                          {String(row[col.name] || "").substring(0, 50)}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
              {schema?.columns?.length > 10 && (
                <p className="text-sm text-gray-500 mt-3 text-center">
                  Mostrando las primeras 10 columnas de {schema.columns.length} totales
                </p>
              )}
            </div>
          ) : (
            <div className="text-gray-500 text-center py-8">
              No hay datos para mostrar
            </div>
          )}
        </div>
      )}


      {/* Info */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <div className="flex gap-3">
          <AlertCircle className="text-blue-600 flex-shrink-0" size={20} />
          <div>
            <h4 className="font-semibold text-blue-900">Sistema Dinámico</h4>
            <p className="text-sm text-blue-800 mt-1">
              {metrics?.total_cases > 0
                ? `Hay ${metrics.total_cases.toLocaleString()} registros en la tabla "${schema?.table_name || 'N/A'}". El dashboard se adapta automáticamente a cualquier estructura de datos.`
                : "No hay datos cargados todavía. Usa el módulo de Carga de Datos para subir un archivo CSV, Excel o JSON."
              }
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Dashboard;