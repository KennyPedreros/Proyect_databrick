import React, { useState } from "react";
import {
  Upload,
  File,
  CheckCircle,
  XCircle,
  AlertTriangle,
} from "lucide-react";
import { uploadCovidData, fetchDataSources } from "../services/ingestionAPI";

function FileUploader() {
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [result, setResult] = useState(null);
  const [dragActive, setDragActive] = useState(false);

  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);

    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      setFile(e.dataTransfer.files[0]);
    }
  };

  const handleFileChange = (e) => {
    if (e.target.files && e.target.files[0]) {
      setFile(e.target.files[0]);
    }
  };

  const handleUpload = async () => {
    if (!file) return;

    setUploading(true);
    setResult(null);

    try {
      const response = await uploadCovidData(file);
      setResult({
        success: true,
        message: "Archivo cargado exitosamente",
        details: response,
      });
      setFile(null);
    } catch (error) {
      setResult({
        success: false,
        message: "Error al cargar el archivo",
        error: error.message,
      });
    } finally {
      setUploading(false);
    }
  };

  return (
    <div className="max-w-4xl mx-auto space-y-6 fade-in">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-espe-dark">Carga de Datos</h1>
        <p className="text-gray-600 mt-1">
          Módulo 1: Ingesta de Datos COVID-19
        </p>
      </div>

      {/* Upload Area */}
      <div className="bg-white rounded-xl shadow-md p-8">
        <div
          onDragEnter={handleDrag}
          onDragLeave={handleDrag}
          onDragOver={handleDrag}
          onDrop={handleDrop}
          className={`border-2 border-dashed rounded-xl p-12 text-center transition-all ${
            dragActive
              ? "border-espe-green bg-espe-green/5"
              : "border-gray-300 hover:border-espe-green"
          }`}
        >
          <Upload size={48} className="mx-auto text-espe-green mb-4" />
          <h3 className="text-xl font-semibold text-espe-dark mb-2">
            Arrastra tu archivo aquí
          </h3>
          <p className="text-gray-600 mb-4">o haz clic para seleccionar</p>
          <input
            type="file"
            id="file-upload"
            className="hidden"
            accept=".csv,.xlsx,.json"
            onChange={handleFileChange}
          />
          <label
            htmlFor="file-upload"
            className="inline-block bg-espe-green text-white px-6 py-3 rounded-lg cursor-pointer hover:bg-espe-green-light transition-colors"
          >
            Seleccionar Archivo
          </label>
          <p className="text-sm text-gray-500 mt-4">
            Formatos soportados: CSV, Excel (.xlsx), JSON
          </p>
        </div>

        {/* Selected File */}
        {file && (
          <div className="mt-6 flex items-center justify-between bg-espe-gray p-4 rounded-lg">
            <div className="flex items-center gap-3">
              <File size={24} className="text-espe-green" />
              <div>
                <p className="font-medium text-espe-dark">{file.name}</p>
                <p className="text-sm text-gray-600">
                  {(file.size / 1024 / 1024).toFixed(2)} MB
                </p>
              </div>
            </div>
            <button
              onClick={handleUpload}
              disabled={uploading}
              className="bg-espe-green text-white px-6 py-2 rounded-lg hover:bg-espe-green-light transition-colors disabled:opacity-50"
            >
              {uploading ? "Cargando..." : "Cargar"}
            </button>
          </div>
        )}

        {/* Result */}
        {result && (
          <div
            className={`mt-6 p-4 rounded-lg ${
              result.success
                ? "bg-green-50 border border-green-200"
                : "bg-red-50 border border-red-200"
            }`}
          >
            <div className="flex items-start gap-3">
              {result.success ? (
                <CheckCircle
                  size={24}
                  className="text-green-600 flex-shrink-0"
                />
              ) : (
                <XCircle size={24} className="text-red-600 flex-shrink-0" />
              )}
              <div>
                <p
                  className={`font-semibold ${
                    result.success ? "text-green-800" : "text-red-800"
                  }`}
                >
                  {result.message}
                </p>
                {result.details && (
                  <p className="text-sm text-gray-700 mt-1">
                    Registros procesados: {result.details.records_count}
                  </p>
                )}
                {result.error && (
                  <p className="text-sm text-red-700 mt-1">{result.error}</p>
                )}
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Info Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-white rounded-lg shadow p-4">
          <div className="flex items-center gap-3 mb-2">
            <CheckCircle className="text-espe-green" size={20} />
            <h4 className="font-semibold">Validación Automática</h4>
          </div>
          <p className="text-sm text-gray-600">
            Los datos se validan según el esquema definido
          </p>
        </div>
        <div className="bg-white rounded-lg shadow p-4">
          <div className="flex items-center gap-3 mb-2">
            <AlertTriangle className="text-espe-green" size={20} />
            <h4 className="font-semibold">Detección de Errores</h4>
          </div>
          <p className="text-sm text-gray-600">
            Identificamos inconsistencias automáticamente
          </p>
        </div>
        <div className="bg-white rounded-lg shadow p-4">
          <div className="flex items-center gap-3 mb-2">
            <File className="text-espe-green" size={20} />
            <h4 className="font-semibold">Almacenamiento Seguro</h4>
          </div>
          <p className="text-sm text-gray-600">
            Datos guardados en Delta Lake con versionado
          </p>
        </div>
      </div>
    </div>
  );
}

export default FileUploader;
