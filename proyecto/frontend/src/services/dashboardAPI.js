import api from "./api";

export const fetchDashboardMetrics = async () => {
  const response = await api.get("/api/dashboard/metrics");
  return response.data;
};

export const fetchTimeSeriesData = async (startDate, endDate) => {
  const response = await api.get("/api/dashboard/timeseries", {
    params: { start_date: startDate, end_date: endDate },
  });
  return response.data;
};

export const fetchGeographicData = async () => {
  const response = await api.get("/api/dashboard/geographic");
  return response.data;
};

export const exportDashboardData = async (format = "csv") => {
  const response = await api.get(`/api/dashboard/export?format=${format}`, {
    responseType: "blob",
  });
  return response.data;
};

export const fetchKPIs = async () => {
  const response = await api.get("/api/dashboard/kpis");
  return response.data;
};
