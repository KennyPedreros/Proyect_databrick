import api from "./api";

export const fetchProcessStatus = async () => {
  const response = await api.get("/api/monitoring/processes");
  return response.data;
};

export const fetchLogs = async (limit = 50, level = null) => {
  const response = await api.get("/api/monitoring/logs", {
    params: { limit, level },
  });
  return response.data;
};

export const fetchAlerts = async () => {
  const response = await api.get("/api/monitoring/alerts");
  return response.data;
};

export const generateAuditReport = async (startDate, endDate) => {
  const response = await api.post("/api/monitoring/audit-report", {
    start_date: startDate,
    end_date: endDate,
  });
  return response.data;
};

export const getSystemHealth = async () => {
  const response = await api.get("/api/monitoring/health");
  return response.data;
};
