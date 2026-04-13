import { contextBridge, ipcRenderer } from "electron";

const desktopApi = {
  getConfig: () => ipcRenderer.invoke("desktop:get-config"),
  getState: () => ipcRenderer.invoke("desktop:get-state"),
  selectReport: (payload: { report_path: string }) => ipcRenderer.invoke("desktop:select-report", payload),
  deleteReport: (payload: { report_path: string }) => ipcRenderer.invoke("desktop:delete-report", payload),
  deleteAllReports: () => ipcRenderer.invoke("desktop:delete-all-reports"),
  runMode: (payload: Record<string, unknown>) => ipcRenderer.invoke("desktop:run-mode", payload),
  generateReport: (payload: Record<string, unknown>) => ipcRenderer.invoke("desktop:generate-report", payload),
  importTrades: (payload: Record<string, unknown>) => ipcRenderer.invoke("desktop:import-trades", payload),
  cashAdjust: (payload: Record<string, unknown>) => ipcRenderer.invoke("desktop:cash-adjust", payload),
  saveRuntimeConfig: (payload: Record<string, unknown>) => ipcRenderer.invoke("desktop:save-runtime-config", payload),
  saveSignalConfig: (payload: Record<string, unknown>) => ipcRenderer.invoke("desktop:save-signal-config", payload),
  pickCapitalXls: () => ipcRenderer.invoke("desktop:pick-capital-xls"),
  reloadApplication: () => ipcRenderer.invoke("desktop:reload-application"),
  closeApplication: () => ipcRenderer.invoke("desktop:close-application"),
};

contextBridge.exposeInMainWorld("desktopApi", desktopApi);
