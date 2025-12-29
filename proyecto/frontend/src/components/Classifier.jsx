import React, { useState, useEffect } from "react";
import {
  Brain,
  CheckCircle,
  AlertCircle,
  Sparkles,
  ChevronRight,
  History,
  Clock,
  TrendingUp,
  List,
} from "lucide-react";
import api from "../services/api";

function Classifier() {
  const [step, setStep] = useState(1);
  const [analyzing, setAnalyzing] = useState(false);
  const [executing, setExecuting] = useState(false);
  const [error, setError] = useState(null);
  const [analysisResult, setAnalysisResult] = useState(null);
  const [selectedClassifications, setSelectedClassifications] = useState([]);
  const [executionResult, setExecutionResult] = useState(null);
  const [history, setHistory] = useState([]);

  useEffect(() => {
    fetchHistory();
  }, []);

  const fetchHistory = async () => {
    try {
      const response = await api.get("/api/classify/classification-history?limit=5");
      setHistory(response.data.history || []);
    } catch (err) {
      console.error("Error cargando historial:", err);
    }
  };

  const handleAnalyze = async () => {
    setAnalyzing(true);
    setError(null);
    setAnalysisResult(null);

    try {
      const response = await api.post("/api/classify/analyze", {});
      setAnalysisResult(response.data);

      const defaultSelections = response.data.classifiable_columns.flatMap(
        (col) =>
          col.suggestions.map((sug) => ({
            column: col.column_name,
            new_column: sug.name,
            type: sug.type,
            ranges: sug.ranges || null,
          }))
      );
      setSelectedClassifications(defaultSelections);
      setStep(2);
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    } finally {
      setAnalyzing(false);
    }
  };

  const handleExecute = async () => {
    if (selectedClassifications.length === 0) {
      setError("Debes seleccionar al menos una clasificación");
      return;
    }

    setExecuting(true);
    setError(null);

    try {
      const response = await api.post("/api/classify/execute", {
        table_name: analysisResult.table_name,
        classifications: selectedClassifications.map((c) => ({
          column: c.column,
          new_column: c.new_column,
          type: c.type,
          ranges: c.ranges,
        })),
      });

      setExecutionResult(response.data);
      await fetchHistory();
      setStep(3);
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    } finally {
      setExecuting(false);
    }
  };

  const toggleClassification = (classification) => {
    const isSelected = selectedClassifications.some(
      (c) =>
        c.column === classification.column &&
        c.new_column === classification.new_column
    );

    if (isSelected) {
      setSelectedClassifications(
        selectedClassifications.filter(
          (c) =>
            c.column !== classification.column ||
            c.new_column !== classification.new_column
        )
      );
    } else {
      setSelectedClassifications([...selectedClassifications, classification]);
    }
  };

  const resetWizard = () => {
    setStep(1);
    setAnalysisResult(null);
    setSelectedClassifications([]);
    setExecutionResult(null);
    setError(null);
  };

  return (
    <div className="max-w-6xl mx-auto space-y-6 fade-in">
      <div>
        <h1 className="text-3xl font-bold text-espe-dark">
          Clasificación Automática
        </h1>
        <p className="text-gray-600 mt-1">Módulo 4: Clasificación de Datos</p>
      </div>

      <div className="bg-white rounded-xl shadow-md p-6">
        <div className="flex items-center justify-between">
          {[
            { num: 1, label: "Analizar" },
            { num: 2, label: "Configurar" },
            { num: 3, label: "Resultado" },
          ].map((s, idx) => (
            <React.Fragment key={s.num}>
              <div className="flex items-center gap-3">
                <div
                  className={`w-10 h-10 rounded-full flex items-center justify-center font-bold ${
                    step >= s.num
                      ? "bg-espe-green text-white"
                      : "bg-gray-200 text-gray-500"
                  }`}
                >
                  {step > s.num ? <CheckCircle size={20} /> : s.num}
                </div>
                <span
                  className={`font-semibold ${
                    step >= s.num ? "text-espe-dark" : "text-gray-400"
                  }`}
                >
                  {s.label}
                </span>
              </div>
              {idx < 2 && (
                <ChevronRight
                  className={step > s.num ? "text-espe-green" : "text-gray-300"}
                />
              )}
            </React.Fragment>
          ))}
        </div>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <div className="flex items-start gap-3">
            <AlertCircle className="text-red-600 flex-shrink-0" size={24} />
            <div>
              <h4 className="font-semibold text-red-900">Error</h4>
              <p className="text-sm text-red-800 mt-1">{error}</p>
            </div>
          </div>
        </div>
      )}

      {step === 1 && (
        <div className="bg-white rounded-xl shadow-md p-8">
          <div className="text-center">
            <div className="inline-flex items-center justify-center w-20 h-20 bg-espe-green/10 rounded-full mb-4">
              <Brain size={40} className="text-espe-green" />
            </div>

            <h2 className="text-2xl font-bold text-espe-dark mb-2">
              Análisis Inteligente
            </h2>

            <p className="text-gray-600 mb-6 max-w-2xl mx-auto">
              El sistema analizará automáticamente tu tabla más reciente y
              sugerirá clasificaciones basadas en el tipo de datos.
            </p>

            <button
              onClick={handleAnalyze}
              disabled={analyzing}
              className="bg-espe-green text-white px-8 py-4 rounded-lg hover:bg-espe-green-light transition-colors disabled:opacity-50 text-lg font-semibold shadow-lg"
            >
              {analyzing ? (
                <div className="flex items-center gap-3">
                  <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
                  <span>Analizando tabla...</span>
                </div>
              ) : (
                <div className="flex items-center gap-2">
                  <Sparkles size={20} />
                  <span>Analizar Tabla</span>
                </div>
              )}
            </button>
          </div>

          <div className="mt-8 grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="bg-blue-50 p-4 rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <TrendingUp className="text-blue-600" size={20} />
                <h4 className="font-semibold text-blue-900">Numéricos</h4>
              </div>
              <p className="text-sm text-blue-800">
                Crea rangos automáticos por cuartiles
              </p>
            </div>

            <div className="bg-green-50 p-4 rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <Clock className="text-green-600" size={20} />
                <h4 className="font-semibold text-green-900">Fechas</h4>
              </div>
              <p className="text-sm text-green-800">
                Extrae año, mes, trimestre
              </p>
            </div>

            <div className="bg-purple-50 p-4 rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <List className="text-purple-600" size={20} />
                <h4 className="font-semibold text-purple-900">Categorías</h4>
              </div>
              <p className="text-sm text-purple-800">
                Detecta valores únicos
              </p>
            </div>
          </div>
        </div>
      )}

      {step === 2 && analysisResult && (
        <div className="bg-white rounded-xl shadow-md p-6">
          <h2 className="text-xl font-bold text-espe-dark mb-4">
            Configurar Clasificaciones
          </h2>
          <p className="text-gray-600 mb-4">
            Tabla: <strong>{analysisResult.table_name}</strong> |{" "}
            {analysisResult.total_classifiable} columnas clasificables
          </p>

          <div className="space-y-4">
            {analysisResult.classifiable_columns.map((col) => (
              <div
                key={col.column_name}
                className="border border-gray-200 rounded-lg p-4"
              >
                <div className="mb-2">
                  <h3 className="font-semibold text-espe-dark">
                    {col.column_name}
                  </h3>
                  <p className="text-sm text-gray-500">
                    {col.classification_type} | {col.unique_values} valores
                  </p>
                </div>

                <div className="space-y-2">
                  {col.suggestions.map((sug, idx) => {
                    const classification = {
                      column: col.column_name,
                      new_column: sug.name,
                      type: sug.type,
                      ranges: sug.ranges || null,
                    };
                    const isSelected = selectedClassifications.some(
                      (c) =>
                        c.column === classification.column &&
                        c.new_column === classification.new_column
                    );

                    return (
                      <div
                        key={idx}
                        onClick={() => toggleClassification(classification)}
                        className={`p-3 rounded-lg border cursor-pointer ${
                          isSelected
                            ? "border-espe-green bg-espe-green/5"
                            : "border-gray-200 hover:border-espe-green/50"
                        }`}
                      >
                        <div className="flex items-center gap-3">
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={() => {}}
                            className="w-5 h-5"
                          />
                          <div className="flex-1">
                            <p className="font-medium">{sug.name}</p>
                            <p className="text-sm text-gray-600">
                              {sug.description || sug.type}
                            </p>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            ))}
          </div>

          <div className="flex gap-4 mt-6">
            <button
              onClick={resetWizard}
              className="px-6 py-3 border border-gray-300 rounded-lg hover:bg-gray-50"
            >
              Cancelar
            </button>
            <button
              onClick={handleExecute}
              disabled={executing || selectedClassifications.length === 0}
              className="flex-1 bg-espe-green text-white px-6 py-3 rounded-lg hover:bg-espe-green-light disabled:opacity-50 font-semibold"
            >
              {executing ? (
                <div className="flex items-center justify-center gap-3">
                  <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
                  <span>Clasificando...</span>
                </div>
              ) : (
                `Ejecutar ${selectedClassifications.length} Clasificaciones`
              )}
            </button>
          </div>
        </div>
      )}

      {step === 3 && executionResult && (
        <div className="bg-white rounded-xl shadow-md p-6">
          <div className="flex items-center gap-3 mb-6">
            <CheckCircle className="text-green-600" size={32} />
            <div>
              <h3 className="text-xl font-bold text-espe-dark">
                ¡Clasificación Completada!
              </h3>
              <p className="text-sm text-gray-600">{executionResult.message}</p>
            </div>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
            <div className="bg-blue-50 p-4 rounded-lg text-center">
              <p className="text-sm text-gray-600 mb-1">Registros</p>
              <p className="text-2xl font-bold text-espe-dark">
                {executionResult.total_records.toLocaleString()}
              </p>
            </div>

            <div className="bg-green-50 p-4 rounded-lg text-center">
              <p className="text-sm text-gray-600 mb-1">Clasificaciones</p>
              <p className="text-2xl font-bold text-green-700">
                {executionResult.classifications_applied}
              </p>
            </div>

            <div className="bg-purple-50 p-4 rounded-lg text-center">
              <p className="text-sm text-gray-600 mb-1">Tiempo</p>
              <p className="text-2xl font-bold text-purple-700">
                {executionResult.elapsed_seconds.toFixed(1)}s
              </p>
            </div>

            <div className="bg-orange-50 p-4 rounded-lg text-center">
              <p className="text-sm text-gray-600 mb-1">Nueva Tabla</p>
              <p className="text-xs font-bold text-orange-700 break-all">
                {executionResult.classified_table}
              </p>
            </div>
          </div>

          <button
            onClick={resetWizard}
            className="w-full bg-espe-green text-white px-6 py-3 rounded-lg hover:bg-espe-green-light font-semibold"
          >
            Clasificar Otra Tabla
          </button>
        </div>
      )}

      {history.length > 0 && (
        <div className="bg-white rounded-xl shadow-md p-6">
          <div className="flex items-center gap-2 mb-4">
            <History className="text-espe-dark" size={24} />
            <h3 className="text-xl font-bold text-espe-dark">
              Historial de Clasificaciones
            </h3>
          </div>

          <div className="space-y-3">
            {history.map((item, index) => (
              <div
                key={index}
                className="border border-gray-200 rounded-lg p-4"
              >
                <div className="flex items-center gap-2 mb-1">
                  <Clock size={16} className="text-gray-500" />
                  <span className="text-sm text-gray-600">
                    {new Date(item.timestamp).toLocaleString("es-ES")}
                  </span>
                </div>
                <p className="font-semibold text-espe-dark">
                  {item.source_table} → {item.classified_table}
                </p>
                <div className="flex gap-4 mt-2 text-xs text-gray-600">
                  <span>{item.total_records.toLocaleString()} registros</span>
                  <span>{item.classifications_applied} clasificaciones</span>
                  <span>{item.elapsed_seconds.toFixed(1)}s</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default Classifier;
