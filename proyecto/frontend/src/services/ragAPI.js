import api from "./api";

export const queryRAG = async (question) => {
  const response = await api.post("/api/rag/query", { question });
  return response.data;
};

export const getQueryHistory = async () => {
  const response = await api.get("/api/rag/history");
  return response.data;
};

export const feedbackRAG = async (queryId, helpful) => {
  const response = await api.post("/api/rag/feedback", {
    query_id: queryId,
    helpful: helpful,
  });
  return response.data;
};
