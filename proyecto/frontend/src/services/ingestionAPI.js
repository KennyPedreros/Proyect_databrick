import api from "./api";

export const uploadCovidData = async (file) => {
  const formData = new FormData();
  formData.append("file", file);

  const response = await api.post("/ingest/upload", formData, {
    headers: {
      "Content-Type": "multipart/form-data",
    },
  });

  return response.data;
};

export const fetchDataSources = async () => {
  const response = await api.get("/ingest/sources");
  return response.data;
};

export const validateSchema = async (fileId) => {
  const response = await api.post("/ingest/validate", { file_id: fileId });
  return response.data;
};

export const getIngestionStatus = async (ingestionId) => {
  const response = await api.get(`/ingest/status/${ingestionId}`);
  return response.data;
};
