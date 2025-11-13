import React, { useState } from "react";
import { Sparkles, Tag, TrendingUp, Brain } from "lucide-react";
import {
  runClassification,
  getModelMetrics,
} from "../services/classificationAPI";

function Classifier() {
  const [running, setRunning] = useState(false);
  const [results, setResults] = useState(null);
  const [metrics, setMetrics] = useState(null);

  const handleClassify = async () => {
    setRunning(true);
    try {
      const response = await runClassification();
      setResults(response);
      loadMetrics();
    } catch (error) {
      console.error("Error in classification:", error);
    } finally {
      setRunning(false);
    }
  };

  const loadMetrics = async () => {
    try {
      const metricsData = await getModelMetrics();
      setMetrics(metricsData);
    } catch (error) {
      console.error("Error loading metrics:", error);
    }
  };

  const severityColors = {
    Leve: "bg-green-100 text-green-800 border-green-300",
    Moderado: "bg-yellow-100 text-yellow-800 border-yellow-300",
    Grave: "bg-orange-100 text-orange-800 border-orange-300",
    Crítico: "bg-red-100 text-red-800 border-red-300",
  };

  return (
    <div className="max-w-6xl mx-auto space-y-6 fade-in">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-espe-dark">
          Clasificación Inteligente
        </h1>
        <p className="text-gray-600 mt-1">
          Módulo 4: Clasificación y Etiquetado con IA
        </p>
      </div>

      {/* Action Card */}
      <div className="bg-gradient-to-r from-espe-green to-espe-green-light rounded-xl shadow-lg p-8 text-white">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-2xl font-bold mb-2">
              Clasificación Automática
            </h2>
            <p className="text-espe-white/90">
              Utiliza LangChain + LLMs para etiquetar casos por severidad
              automáticamente
            </p>
            <div className="flex gap-4 mt-4">
              <div className="flex items-center gap-2">
                <Brain size={20} />
                <span className="text-sm">OpenAI GPT-4</span>
              </div>
              <div className="flex items-center gap-2">
                <Sparkles size={20} />
                <span className="text-sm">LangChain Agents</span>
              </div>
            </div>
          </div>
          <button
            onClick={handleClassify}
            disabled={running}
            className="bg-white text-espe-green px-8 py-4 rounded-lg font-semibold hover:bg-espe-gray transition-colors disabled:opacity-50 flex items-center gap-2"
          >
            <Tag size={20} />
            {running ? "Clasificando..." : "Iniciar Clasificación"}
          </button>
        </div>
      </div>

      {/* Results */}
      {results && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Distribution */}
          <div className="bg-white rounded-xl shadow-md p-6">
            <h3 className="text-lg font-bold text-espe-dark mb-4 flex items-center gap-2">
              <Tag size={20} className="text-espe-green" />
              Distribución de Severidad
            </h3>

            <div className="space-y-3">
              {Object.entries(results.distribution || {}).map(
                ([severity, count]) => (
                  <div key={severity}>
                    <div className="flex justify-between mb-1">
                      <span className="font-medium">{severity}</span>
                      <span className="text-gray-600">{count} casos</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-3">
                      <div
                        className={`h-3 rounded-full ${
                          severity === "Leve"
                            ? "bg-green-500"
                            : severity === "Moderado"
                            ? "bg-yellow-500"
                            : severity === "Grave"
                            ? "bg-orange-500"
                            : "bg-red-500"
                        }`}
                        style={{ width: `${(count / results.total) * 100}%` }}
                      ></div>
                    </div>
                  </div>
                )
              )}
            </div>

            <div className="mt-6 pt-6 border-t border-gray-200">
              <div className="flex justify-between">
                <span className="font-semibold">Total Clasificado:</span>
                <span className="font-bold text-espe-green">
                  {results.total}
                </span>
              </div>
            </div>
          </div>

          {/* Model Performance */}
          <div className="bg-white rounded-xl shadow-md p-6">
            <h3 className="text-lg font-bold text-espe-dark mb-4 flex items-center gap-2">
              <TrendingUp size={20} className="text-espe-green" />
              Métricas del Modelo
            </h3>

            {metrics ? (
              <div className="space-y-4">
                <div className="bg-espe-gray p-4 rounded-lg">
                  <p className="text-sm text-gray-600 mb-1">
                    Precisión (Accuracy)
                  </p>
                  <div className="flex items-center justify-between">
                    <div className="flex-1 bg-gray-200 rounded-full h-2 mr-3">
                      <div
                        className="bg-espe-green h-2 rounded-full"
                        style={{ width: `${metrics.accuracy * 100}%` }}
                      ></div>
                    </div>
                    <span className="font-bold text-espe-dark">
                      {(metrics.accuracy * 100).toFixed(1)}%
                    </span>
                  </div>
                </div>

                <div className="bg-espe-gray p-4 rounded-lg">
                  <p className="text-sm text-gray-600 mb-1">F1-Score</p>
                  <div className="flex items-center justify-between">
                    <div className="flex-1 bg-gray-200 rounded-full h-2 mr-3">
                      <div
                        className="bg-espe-green-light h-2 rounded-full"
                        style={{ width: `${metrics.f1_score * 100}%` }}
                      ></div>
                    </div>
                    <span className="font-bold text-espe-dark">
                      {(metrics.f1_score * 100).toFixed(1)}%
                    </span>
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-3">
                  <div className="bg-blue-50 p-3 rounded-lg text-center">
                    <p className="text-xs text-gray-600">Precision</p>
                    <p className="text-xl font-bold text-espe-dark">
                      {(metrics.precision * 100).toFixed(1)}%
                    </p>
                  </div>
                  <div className="bg-purple-50 p-3 rounded-lg text-center">
                    <p className="text-xs text-gray-600">Recall</p>
                    <p className="text-xl font-bold text-espe-dark">
                      {(metrics.recall * 100).toFixed(1)}%
                    </p>
                  </div>
                </div>
              </div>
            ) : (
              <p className="text-gray-500 text-center py-8">
                Ejecuta la clasificación para ver métricas
              </p>
            )}
          </div>
        </div>
      )}

      {/* Sample Classifications */}
      {results?.samples && (
        <div className="bg-white rounded-xl shadow-md p-6">
          <h3 className="text-lg font-bold text-espe-dark mb-4">
            Ejemplos Clasificados
          </h3>

          <div className="space-y-3">
            {results.samples.slice(0, 5).map((sample, idx) => (
              <div
                key={idx}
                className="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow"
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <p className="text-sm text-gray-700">{sample.text}</p>
                    <div className="flex gap-2 mt-2">
                      <span className="text-xs text-gray-500">
                        Edad: {sample.age}
                      </span>
                      <span className="text-xs text-gray-500">
                        Síntomas: {sample.symptoms}
                      </span>
                    </div>
                  </div>
                  <span
                    className={`px-3 py-1 rounded-full text-sm font-semibold border ${
                      severityColors[sample.predicted_severity]
                    }`}
                  >
                    {sample.predicted_severity}
                  </span>
                </div>
                <div className="mt-2 text-xs text-gray-500">
                  Confianza: {(sample.confidence * 100).toFixed(1)}%
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Info */}
      <div className="bg-purple-50 border border-purple-200 rounded-lg p-4">
        <div className="flex gap-3">
          <Brain className="text-purple-600 flex-shrink-0" size={20} />
          <div>
            <h4 className="font-semibold text-purple-900">
              Clasificación con IA
            </h4>
            <p className="text-sm text-purple-800 mt-1">
              El sistema utiliza modelos de lenguaje (LLMs) para analizar
              síntomas, edad y otros factores, clasificando automáticamente cada
              caso en: Leve, Moderado, Grave o Crítico.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Classifier;
