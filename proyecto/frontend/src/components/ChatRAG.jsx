import React, { useState, useRef, useEffect } from "react";
import { Send, Bot, User, Sparkles, Database, Clock, History } from "lucide-react";
import api from "../services/api";

function ChatRAG() {
  const [messages, setMessages] = useState([
    {
      role: "assistant",
      content:
        "¡Hola! Soy tu asistente de datos de vacunación COVID-19 en Ecuador. Puedes preguntarme sobre estadísticas, tendencias, vacunas por provincia, grupos de edad, etc. ¿En qué puedo ayudarte?",
    },
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [history, setHistory] = useState([]);
  const messagesEndRef = useRef(null);

  const scrollToBottom = () => {
    // Scroll solo dentro del contenedor de mensajes, no de toda la página
    if (messagesEndRef.current) {
      messagesEndRef.current.scrollIntoView({
        behavior: "smooth",
        block: "end",
        inline: "nearest"
      });
    }
  };

  useEffect(() => {
    // Solo hacer scroll si hay mensajes nuevos
    if (messages.length > 1) {
      scrollToBottom();
    }
  }, [messages]);

  useEffect(() => {
    fetchHistory();
    // Scroll al inicio de la página al cargar el componente
    window.scrollTo({ top: 0, behavior: 'instant' });
  }, []);

  const fetchHistory = async () => {
    try {
      const response = await api.get("/api/rag/history?limit=5");
      setHistory(response.data.history || []);
    } catch (err) {
      console.error("Error cargando historial:", err);
    }
  };

  const handleSend = async () => {
    if (!input.trim()) return;

    const userMessage = input;
    setInput("");

    setMessages((prev) => [...prev, { role: "user", content: userMessage }]);
    setLoading(true);

    try {
      const response = await api.post("/api/rag/query", {
        question: userMessage,
      });

      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: response.data.answer,
          sql_query: response.data.sql_query,
          table_used: response.data.table_used,
          execution_time: response.data.execution_time,
          data_preview: response.data.data_preview,
        },
      ]);

      // Actualizar historial
      await fetchHistory();
    } catch (error) {
      const errorMsg = error.response?.data?.detail || error.message;
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: `Lo siento, ocurrió un error: ${errorMsg}`,
        },
      ]);
    } finally {
      setLoading(false);
    }
  };

  const suggestedQuestions = [
    "¿Cuántas vacunas se aplicaron en total?",
    "¿Cuál provincia tiene más vacunaciones?",
    "Muestra la distribución por grupo de edad",
    "¿Cuántas dosis se aplicaron por mes?",
  ];

  return (
    <div className="max-w-5xl mx-auto fade-in pb-8">
      {/* Header */}
      <div className="mb-4">
        <h1 className="text-3xl font-bold text-espe-dark">
          Consultas Inteligentes
        </h1>
        <p className="text-gray-600 mt-1">
          RAG: Retrieval-Augmented Generation
        </p>
      </div>

      {/* Chat Container - Altura fija */}
      <div className="bg-white rounded-xl shadow-md flex flex-col overflow-hidden" style={{ height: '600px' }}>
        {/* Messages */}
        <div className="flex-1 overflow-y-auto p-6 space-y-4">
          {messages.map((message, idx) => (
            <div
              key={idx}
              className={`flex gap-3 ${
                message.role === "user" ? "flex-row-reverse" : "flex-row"
              }`}
            >
              <div
                className={`flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center ${
                  message.role === "user" ? "bg-espe-green" : "bg-purple-500"
                }`}
              >
                {message.role === "user" ? (
                  <User size={18} className="text-white" />
                ) : (
                  <Bot size={18} className="text-white" />
                )}
              </div>

              <div
                className={`flex-1 max-w-3xl ${
                  message.role === "user" ? "text-right" : "text-left"
                }`}
              >
                <div
                  className={`inline-block px-4 py-3 rounded-2xl ${
                    message.role === "user"
                      ? "bg-espe-green text-white"
                      : "bg-espe-gray text-espe-dark"
                  }`}
                >
                  <p className="text-sm whitespace-pre-wrap">
                    {message.content}
                  </p>
                </div>

                {message.role === "assistant" && message.table_used && (
                  <div className="mt-2 text-xs text-gray-600 space-y-1">
                    <div className="flex items-center gap-2">
                      <Database size={14} />
                      <span>Tabla: <strong>{message.table_used}</strong></span>
                    </div>
                    {message.execution_time && (
                      <div className="flex items-center gap-2">
                        <Clock size={14} />
                        <span>Tiempo: {message.execution_time.toFixed(2)}s</span>
                      </div>
                    )}
                    {message.sql_query && (
                      <details className="mt-2">
                        <summary className="cursor-pointer font-semibold">
                          Ver SQL generado
                        </summary>
                        <pre className="mt-1 p-2 bg-gray-100 rounded text-xs overflow-x-auto">
                          {message.sql_query}
                        </pre>
                      </details>
                    )}
                  </div>
                )}
              </div>
            </div>
          ))}

          {loading && (
            <div className="flex gap-3">
              <div className="flex-shrink-0 w-8 h-8 rounded-full bg-purple-500 flex items-center justify-center">
                <Bot size={18} className="text-white" />
              </div>
              <div className="bg-espe-gray px-4 py-3 rounded-2xl">
                <div className="flex gap-1">
                  <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce"></div>
                  <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce delay-100"></div>
                  <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce delay-200"></div>
                </div>
              </div>
            </div>
          )}

          <div ref={messagesEndRef} />
        </div>

        {/* Suggested Questions */}
        {messages.length === 1 && (
          <div className="px-6 py-3 border-t border-gray-200">
            <p className="text-sm text-gray-600 mb-2">Preguntas sugeridas:</p>
            <div className="flex flex-wrap gap-2">
              {suggestedQuestions.map((question, idx) => (
                <button
                  key={idx}
                  onClick={() => setInput(question)}
                  className="text-xs px-3 py-2 bg-espe-gray hover:bg-espe-green-lighter/20 rounded-full transition-colors"
                >
                  {question}
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Input */}
        <div className="p-4 border-t border-gray-200">
          <div className="flex gap-3">
            <input
              type="text"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyPress={(e) => e.key === "Enter" && handleSend()}
              placeholder="Pregunta sobre vacunación COVID-19..."
              className="flex-1 px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-espe-green focus:border-transparent"
            />
            <button
              onClick={handleSend}
              disabled={loading || !input.trim()}
              className="bg-espe-green text-white px-6 py-3 rounded-lg hover:bg-espe-green-light transition-colors disabled:opacity-50 flex items-center gap-2"
            >
              <Send size={20} />
            </button>
          </div>
        </div>
      </div>

      {/* Info - Compacto */}
      <div className="mt-4 bg-blue-50 border border-blue-200 rounded-lg p-3">
        <div className="flex gap-2 items-center">
          <Sparkles className="text-blue-600 flex-shrink-0" size={16} />
          <div>
            <h4 className="font-semibold text-blue-900 text-sm">
              Sistema RAG con Llama 3.1 - Generación Automática de SQL
            </h4>
            <p className="text-xs text-blue-800 mt-0.5">
              Llama 3.1 convierte tu pregunta en SQL → Ejecuta en Delta Lake → Genera respuesta natural
            </p>
          </div>
        </div>
      </div>

      {/* Historial - Crece hacia abajo con scroll de página */}
      {history.length > 0 && (
        <div className="mt-4 bg-white rounded-xl shadow-md p-4">
          <div className="flex items-center gap-2 mb-3">
            <History className="text-espe-dark" size={18} />
            <h3 className="font-bold text-espe-dark">Consultas Recientes</h3>
          </div>
          <div className="space-y-2">
            {history.map((item, index) => (
              <div
                key={index}
                className="text-sm p-3 bg-espe-gray rounded-lg cursor-pointer hover:bg-espe-green-lighter/20 transition-colors"
                onClick={() => setInput(item.question)}
              >
                <p className="font-semibold text-espe-dark">
                  {item.question}
                </p>
                <div className="flex gap-3 text-gray-600 text-xs mt-1">
                  <span>{item.results_count} resultados</span>
                  <span>{item.execution_time.toFixed(2)}s</span>
                  <span>{new Date(item.timestamp).toLocaleString("es-ES")}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default ChatRAG;
