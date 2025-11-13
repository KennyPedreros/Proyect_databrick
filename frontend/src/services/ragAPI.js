import api from "./api";

export const queryRAG = async (question) => {
  const response = await api.post("/rag/query", { question });
  return response.data;
};

export const getQueryHistory = async () => {
  const response = await api.get("/rag/history");
  return response.data;
};

export const feedbackRAG = async (queryId, helpful) => {
  const response = await api.post("/rag/feedback", {
    query_id: queryId,
    helpful: helpful,
  });
  return response.data;
};
