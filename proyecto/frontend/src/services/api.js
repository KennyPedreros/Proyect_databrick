import axios from "axios";

// IMPORTANTE: La URL base NO debe incluir /api porque ya está en vite.config.js
const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

const api = axios.create({
  baseURL: API_URL,
  headers: {
    "Content-Type": "application/json",
  },
  timeout: 300000, // 5 minutos de timeout (300000ms)
});

// Interceptor para requests
api.interceptors.request.use(
  (config) => {
    console.log(`🔵 REQUEST: ${config.method.toUpperCase()} ${config.baseURL}${config.url}`);
    return config;
  },
  (error) => {
    console.error("❌ REQUEST ERROR:", error);
    return Promise.reject(error);
  }
);

// Interceptor para responses
api.interceptors.response.use(
  (response) => {
    console.log(`✅ RESPONSE: ${response.config.method.toUpperCase()} ${response.config.url} - ${response.status}`);
    return response;
  },
  (error) => {
    if (error.response) {
      // El servidor respondió con un código de error
      console.error("❌ ERROR RESPONSE:", {
        status: error.response.status,
        url: error.config.url,
        data: error.response.data
      });
    } else if (error.request) {
      // La petición fue hecha pero no hubo respuesta
      console.error("❌ ERROR REQUEST (no response):", error.request);
    } else {
      // Algo pasó al configurar la petición
      console.error("❌ ERROR:", error.message);
    }
    return Promise.reject(error);
  }
);

export default api;