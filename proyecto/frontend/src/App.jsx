import React, { useState } from "react";
import {
  Menu,
  X,
  Home,
  Upload,
  Sparkles,
  Tag,
  BarChart3,
  Monitor,
} from "lucide-react";
import Dashboard from "./components/Dashboard";
import FileUploader from "./components/FileUploader";
import DataCleaner from "./components/DataCleaner";
import Classifier from "./components/Classifier";
import ChatRAG from "./components/ChatRAG";
import ProcessMonitor from "./components/ProcessMonitor";

function App() {
  const [activeModule, setActiveModule] = useState("dashboard");
  const [sidebarOpen, setSidebarOpen] = useState(true);

  const modules = [
    {
      id: "dashboard",
      name: "Visualización de Datos",
      icon: Home,
      number: 1,
      component: Dashboard,
    },
    {
      id: "upload",
      name: "Ingestar Datos",
      icon: Upload,
      number: 2,
      component: FileUploader,
    },
    {
      id: "clean",
      name: "Procesamiento y Limpieza",
      icon: Sparkles,
      number: 3,
      component: DataCleaner,
    },
    {
      id: "classify",
      name: "Clasificación de Datos",
      icon: Tag,
      number: 4,
      component: Classifier,
    },
    {
      id: "rag",
      name: "Agente Inteligente",
      icon: BarChart3,
      number: 5,
      component: ChatRAG,
    },
    {
      id: "monitor",
      name: "Monitoreo y Auditoría",
      icon: Monitor,
      number: 6,
      component: ProcessMonitor,
    },
  ];

  const ActiveComponent =
    modules.find((m) => m.id === activeModule)?.component || Dashboard;

  return (
    <div className="flex h-screen bg-espe-gray">
      {/* Sidebar */}
      <aside
        className={`${
          sidebarOpen ? "w-64" : "w-20"
        } bg-espe-green text-white transition-all duration-300 flex flex-col`}
      >
        {/* Header */}
        <div className="p-4 flex items-center justify-between border-b border-espe-green-light">
          {sidebarOpen && (
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 bg-white rounded-lg flex items-center justify-center">
                <span className="text-espe-green font-bold text-xl">E</span>
              </div>
              <div>
                <h1 className="font-bold text-sm">ESPE</h1>
                <p className="text-xs text-espe-green-lighter">COVID-19</p>
              </div>
            </div>
          )}
          <button
            onClick={() => setSidebarOpen(!sidebarOpen)}
            className="p-2 hover:bg-espe-green-light rounded-lg transition-colors"
          >
            {sidebarOpen ? <X size={20} /> : <Menu size={20} />}
          </button>
        </div>

        {/* Menu Items */}
        <nav className="flex-1 p-4 space-y-2">
          {modules.map((module) => {
            const Icon = module.icon;
            return (
              <button
                key={module.id}
                onClick={() => setActiveModule(module.id)}
                className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-all ${
                  activeModule === module.id
                    ? "bg-espe-green-light shadow-lg"
                    : "hover:bg-espe-green-light/50"
                }`}
              >
                <Icon size={20} />
                {sidebarOpen && (
                  <>
                    <span className="flex-1 text-left">{module.name}</span>
                    {module.number && (
                      <span className="bg-white text-espe-green text-xs px-2 py-1 rounded-full font-bold">
                        M{module.number}
                      </span>
                    )}
                  </>
                )}
              </button>
            );
          })}
        </nav>

        {/* Footer */}
        {sidebarOpen && (
          <div className="p-4 border-t border-espe-green-light text-xs text-espe-green-lighter">
            <p>Universidad de las Fuerzas Armadas</p>
            <p className="font-bold">ESPE - 2025</p>
          </div>
        )}
      </aside>

      {/* Main Content */}
      <main className="flex-1 overflow-auto">
        <div className="p-8">
          <ActiveComponent />
        </div>
      </main>
    </div>
  );
}

export default App;
