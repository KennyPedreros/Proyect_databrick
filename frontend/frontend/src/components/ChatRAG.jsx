import React, { useState, useRef, useEffect } from "react";
import { Send, Bot, User, Sparkles } from "lucide-react";
import { queryRAG } from "../services/ragAPI";

function ChatRAG() {
  const [messages, setMessages] = useState([
    {
      role: "assistant",
      content:
        "¡Hola! Soy tu asistente de datos COVID-19. Puedes preguntarme sobre estadísticas, tendencias, casos por región, etc. ¿En qué puedo ayudarte?",
    },
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSend = async () => {
    if (!input.trim()) return;

    const userMessage = input;
    setInput("");

    setMessages((prev) => [...prev, { role: "user", content: userMessage }]);
    setLoading(true);

    try {
      const response = await queryRAG(userMessage);
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: response.answer,
          sources: response.sources,
        },
      ]);
    } catch (error) {
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content:
            "Lo siento, ocurrió un error al procesar tu consulta. Por favor intenta de nuevo.",
        },
      ]);
    } finally {
      setLoading(false);
    }
  };

  const suggestedQuestions = [
    "¿Cuántos casos de COVID hubo en Ecuador en 2024?",
    "¿Cuál es la tendencia de vacunación?",
    "Muestra los casos por provincia",
    "¿Cuál es la tasa de mortalidad actual?",
  ];

  return (
    <div className="max-w-5xl mx-auto h-[calc(100vh-12rem)] flex flex-col fade-in">
      {/* Header */}
      <div className="mb-4">
        <h1 className="text-3xl font-bold text-espe-dark">
          Consultas Inteligentes
        </h1>
        <p className="text-gray-600 mt-1">
          RAG: Retrieval-Augmented Generation
        </p>
      </div>

      {/* Chat Container */}
      <div className="flex-1 bg-white rounded-xl shadow-md flex flex-col overflow-hidden">
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

                {message.sources && (
                  <div className="mt-2 text-xs text-gray-500">
                    <p className="font-semibold mb-1">Fuentes consultadas:</p>
                    <ul className="list-disc list-inside">
                      {message.sources.map((source, i) => (
                        <li key={i}>{source}</li>
                      ))}
                    </ul>
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
              placeholder="Escribe tu pregunta sobre COVID-19..."
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

      {/* Info */}
      <div className="mt-4 bg-blue-50 border border-blue-200 rounded-lg p-4">
        <div className="flex gap-3">
          <Sparkles className="text-blue-600 flex-shrink-0" size={20} />
          <div>
            <h4 className="font-semibold text-blue-900">Sistema RAG</h4>
            <p className="text-sm text-blue-800 mt-1">
              Las respuestas se generan consultando la base de datos vectorial
              (ChromaDB) y los datos históricos en Delta Lake, garantizando
              información precisa y actualizada.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default ChatRAG;
