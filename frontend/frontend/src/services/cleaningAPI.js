import api from "./api";

export const runCleaningJob = async (config) => {
  const response = await api.post("/clean/run", config);
  return response.data;
};

export const getCleaningStatus = async (jobId) => {
  const response = await api.get(`/clean/status/${jobId}`);
  return response.data;
};

export const getCleaningHistory = async () => {
  const response = await api.get("/clean/history");
  return response.data;
};

export const downloadCleanedData = async (jobId) => {
  const response = await api.get(`/clean/download/${jobId}`, {
    responseType: "blob",
  });
  return response.data;
};
