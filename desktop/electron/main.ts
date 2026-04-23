/*
 * Copyright (c) 2026 Sheng-Hsin Tsai
 * SPDX-License-Identifier: MIT
 */

import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";

import { app, BrowserWindow, dialog, ipcMain } from "electron";
import type { IpcMainInvokeEvent } from "electron";

type OperationResult = {
  name: string;
  success: boolean;
  returncode: number;
  command: string;
  stdout: string;
  message: string;
  log_path: string;
  report_path: string;
  report_json_path: string;
};

type DesktopSession = {
  lastResult: OperationResult | null;
  selectedReportPath: string;
};

const WINDOW_TITLE = "Stock Trading Desktop";
const DEFAULT_WIDTH = 1440;
const DEFAULT_HEIGHT = 960;
const DESKTOP_ROOT = path.resolve(__dirname, "..");
const REPO_ROOT = path.resolve(DESKTOP_ROOT, "..");
const PRELOAD_PATH = path.join(__dirname, "preload.js");
const RENDERER_INDEX_PATH = path.join(DESKTOP_ROOT, "dist", "index.html");
const PYTHON_BRIDGE_PATH = path.join(REPO_ROOT, "gui_ipc.py");
const CONFIG_PATH = path.join(REPO_ROOT, "config.json");

let mainWindow: BrowserWindow | null = null;
let reloadTimer: NodeJS.Timeout | null = null;
const desktopSession: DesktopSession = {
  lastResult: null,
  selectedReportPath: "",
};

function resolvePythonCommand(): string {
  if (process.env.PYTHON && process.env.PYTHON.trim()) {
    return process.env.PYTHON.trim();
  }
  return process.platform === "win32" ? "python" : "python3";
}

function scheduleRendererReload(): void {
  if (!mainWindow || mainWindow.isDestroyed()) {
    return;
  }
  if (reloadTimer !== null) {
    clearTimeout(reloadTimer);
  }
  reloadTimer = setTimeout(() => {
    if (mainWindow && !mainWindow.isDestroyed()) {
      mainWindow.webContents.reloadIgnoringCache();
    }
  }, 180);
}

function watchRendererBundle(): void {
  const distDir = path.join(DESKTOP_ROOT, "dist");
  if (!fs.existsSync(distDir)) {
    return;
  }
  try {
    fs.watch(distDir, { recursive: true }, () => {
      scheduleRendererReload();
    });
  } catch {
    // Ignore environments that do not support recursive file watching.
  }
}

function invokePythonBridge<T>(action: string, payload: Record<string, unknown> = {}): Promise<T> {
  const python = resolvePythonCommand();

  return new Promise((resolve, reject) => {
    const child = spawn(
      python,
      [PYTHON_BRIDGE_PATH, "--action", action],
      {
        cwd: REPO_ROOT,
        env: process.env,
        stdio: ["pipe", "pipe", "pipe"],
      },
    );

    let stdout = "";
    let stderr = "";

    child.stdout.setEncoding("utf-8");
    child.stderr.setEncoding("utf-8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", (error) => {
      reject(error);
    });
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(stderr.trim() || `Python bridge exited with code ${code ?? 1}.`));
        return;
      }
      try {
        const parsed = JSON.parse(stdout) as T;
        resolve(parsed);
      } catch (error) {
        reject(
          new Error(
            `Failed to parse Python bridge response.${stderr ? ` stderr: ${stderr.trim()}` : ""}`,
          ),
        );
      }
    });

    child.stdin.write(
      JSON.stringify({
        ...payload,
        last_result: desktopSession.lastResult,
        selected_report_path: desktopSession.selectedReportPath,
      }),
    );
    child.stdin.end();
  });
}

async function invokeDesktopAction(action: string, payload: Record<string, unknown> = {}) {
  const response = await invokePythonBridge<{
    error?: string;
    ok: boolean;
    state: {
      last_result: OperationResult | null;
      ui: {
        selected_report_path: string;
      };
    };
  }>(action, payload);

  desktopSession.selectedReportPath = response.state.ui.selected_report_path || "";
  desktopSession.lastResult = response.state.last_result ?? null;
  return response;
}

function getWindowBounds(): Electron.Rectangle | null {
  try {
    if (fs.existsSync(CONFIG_PATH)) {
      const raw = fs.readFileSync(CONFIG_PATH, "utf-8");
      const cfg = JSON.parse(raw);
      const guiWin = cfg?.state_engine?.gui?.window;
      if (guiWin && typeof guiWin.width === "number" && typeof guiWin.height === "number") {
        return guiWin as Electron.Rectangle;
      }
    }
  } catch (e) {
    console.error("Failed to read window bounds from config.json", e);
  }
  return null;
}

function saveWindowBounds(bounds: Electron.Rectangle) {
  try {
    if (fs.existsSync(CONFIG_PATH)) {
      const raw = fs.readFileSync(CONFIG_PATH, "utf-8");
      const cfg = JSON.parse(raw);
      if (!cfg.state_engine) cfg.state_engine = {};
      if (!cfg.state_engine.gui) cfg.state_engine.gui = {};
      cfg.state_engine.gui.window = bounds;
      fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2) + "\n", "utf-8");
    }
  } catch (e) {
    console.error("Failed to save window bounds to config.json", e);
  }
}

async function createMainWindow(): Promise<void> {
  const bounds = getWindowBounds();
  const windowOptions: Electron.BrowserWindowConstructorOptions = {
    title: WINDOW_TITLE,
    width: bounds?.width ?? DEFAULT_WIDTH,
    height: bounds?.height ?? DEFAULT_HEIGHT,
    minWidth: 1180,
    minHeight: 760,
    backgroundColor: "#f3ecdf",
    webPreferences: {
      preload: PRELOAD_PATH,
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
  };

  if (bounds?.x !== undefined && bounds?.y !== undefined) {
    windowOptions.x = bounds.x;
    windowOptions.y = bounds.y;
  }

  const window = new BrowserWindow(windowOptions);

  mainWindow = window;
  window.on("close", () => {
    if (!window.isFullScreen() && !window.isMaximized()) {
      saveWindowBounds(window.getBounds());
    }
  });
  window.on("closed", () => {
    if (mainWindow === window) {
      mainWindow = null;
    }
  });

  await window.loadFile(RENDERER_INDEX_PATH);
}

ipcMain.handle("desktop:get-config", async () => {
  return {
    isElectron: true,
    transport: "ipc",
  };
});

ipcMain.handle("desktop:get-state", async () => {
  return invokeDesktopAction("get-state");
});

ipcMain.handle("desktop:select-report", async (_event: IpcMainInvokeEvent, payload: { report_path: string }) => {
  return invokeDesktopAction("select-report", payload);
});

ipcMain.handle("desktop:delete-report", async (_event: IpcMainInvokeEvent, payload: { report_path: string }) => {
  return invokeDesktopAction("delete-report", payload);
});

ipcMain.handle("desktop:delete-all-reports", async () => {
  return invokeDesktopAction("delete-all-reports");
});

ipcMain.handle("desktop:run-mode", async (_event: IpcMainInvokeEvent, payload: Record<string, unknown>) => {
  return invokeDesktopAction("run-mode", payload);
});

ipcMain.handle("desktop:generate-report", async (_event: IpcMainInvokeEvent, payload: Record<string, unknown>) => {
  return invokeDesktopAction("generate-report", payload);
});

ipcMain.handle("desktop:import-trades", async (_event: IpcMainInvokeEvent, payload: Record<string, unknown>) => {
  return invokeDesktopAction("import-trades", payload);
});

ipcMain.handle("desktop:cash-adjust", async (_event: IpcMainInvokeEvent, payload: Record<string, unknown>) => {
  return invokeDesktopAction("cash-adjust", payload);
});

ipcMain.handle("desktop:save-runtime-config", async (_event: IpcMainInvokeEvent, payload: Record<string, unknown>) => {
  return invokeDesktopAction("save-runtime-config", payload);
});

ipcMain.handle("desktop:save-signal-config", async (_event: IpcMainInvokeEvent, payload: Record<string, unknown>) => {
  return invokeDesktopAction("save-signal-config", payload);
});

ipcMain.handle("desktop:pick-capital-xls", async () => {
  const result = await dialog.showOpenDialog({
    title: "Select Capital XLS",
    properties: ["openFile"],
    filters: [
      { name: "Capital Trade Files", extensions: ["xls", "xlsx"] },
      { name: "All Files", extensions: ["*"] },
    ],
  });
  if (result.canceled || result.filePaths.length === 0) {
    return null;
  }
  return result.filePaths[0];
});

ipcMain.handle("desktop:pick-zip-file", async () => {
  const result = await dialog.showOpenDialog({
    title: "Select Data Zip",
    properties: ["openFile"],
    filters: [
      { name: "Zip Archives", extensions: ["zip"] },
      { name: "All Files", extensions: ["*"] },
    ],
  });
  if (result.canceled || result.filePaths.length === 0) {
    return null;
  }
  return result.filePaths[0];
});

ipcMain.handle("desktop:save-zip-path", async () => {
  const result = await dialog.showSaveDialog({
    title: "Export Data Zip",
    defaultPath: path.join(REPO_ROOT, `trading_backup_${new Date().toISOString().slice(0, 10)}.zip`),
    filters: [
      { name: "Zip Archives", extensions: ["zip"] },
      { name: "All Files", extensions: ["*"] },
    ],
  });
  if (result.canceled || !result.filePath) {
    return null;
  }
  return result.filePath;
});

ipcMain.handle("desktop:init-clean-env", async () => {
  return invokeDesktopAction("init-clean-env");
});

ipcMain.handle("desktop:export-zip", async (_event: IpcMainInvokeEvent, payload: Record<string, unknown>) => {
  return invokeDesktopAction("export-zip", payload);
});

ipcMain.handle("desktop:import-zip", async (_event: IpcMainInvokeEvent, payload: Record<string, unknown>) => {
  return invokeDesktopAction("import-zip", payload);
});

ipcMain.handle("desktop:reload-application", async () => {
  fs.writeFileSync(path.join(REPO_ROOT, ".restart_flag"), "");
  app.quit();
});

ipcMain.handle("desktop:close-application", async () => {
  app.quit();
});

app.whenReady().then(async () => {
  watchRendererBundle();
  await createMainWindow();

  app.on("activate", async () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      await createMainWindow();
    }
  });
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});
