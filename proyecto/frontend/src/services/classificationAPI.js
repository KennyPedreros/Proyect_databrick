import api from "./api";

export const runClassification = async (config = {}) => {
  const response = await api.post("/api/classify/auto-label", config);
  return response.data;
};

export const getModelMetrics = async () => {
  const response = await api.get("/api/classify/metrics");
  return response.data;
};

export const getClassificationHistory = async () => {
  const response = await api.get("/api/classify/history");
  return response.data;
};

export const trainModel = async (config) => {
  const response = await api.post("/api/classify/train", config);
  return response.data;
};

export const evaluateModel = async (modelId) => {
  const response = await api.post(`/api/classify/evaluate/${modelId}`);
  return response.data;
};
