const fs = require("node:fs");
const path = require("node:path");
const { spawn } = require("node:child_process");

const { app, BrowserWindow, ipcMain } = require("electron");

app.disableHardwareAcceleration();

const REPO_ROOT = path.resolve(__dirname, "..");
const DESKTOP_ROOT = path.join(REPO_ROOT, "desktop");
const PRELOAD_PATH = path.join(DESKTOP_ROOT, "dist-electron", "preload.js");
const RENDERER_INDEX_PATH = path.join(DESKTOP_ROOT, "dist", "index.html");
const PYTHON_BRIDGE_PATH = path.join(REPO_ROOT, "gui_ipc.py");
const OUT_DIR = path.join(REPO_ROOT, "docs", "images");
const WINDOW_WIDTH = 1440;
const WINDOW_HEIGHT = 960;
const PANEL_PADDING = 18;
const AFTER_ACTION_DELAY_MS = 350;

const desktopSession = {
  lastResult: null,
  selectedReportPath: "",
};

const SECTION_EXPR = {
  hero: "() => document.querySelector('section.hero-card')",
  generateReport:
    "() => [...document.querySelectorAll('section.rail-panel')].find((section) => section.querySelector('h2')?.textContent?.trim() === 'Generate Report')",
  importTrades:
    "() => [...document.querySelectorAll('section.rail-panel')].find((section) => section.querySelector('h2')?.textContent?.trim() === 'Import Trades')",
  cashAdjustment:
    "() => [...document.querySelectorAll('section.rail-panel')].find((section) => section.querySelector('h2')?.textContent?.trim() === 'Cash Adjustment')",
};

function resolvePythonCommand() {
  if (process.env.PYTHON && process.env.PYTHON.trim()) {
    return process.env.PYTHON.trim();
  }
  return process.platform === "win32" ? "python" : "python3";
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function writePng(filename, image) {
  fs.writeFileSync(path.join(OUT_DIR, filename), image.toPNG());
}

function invokePythonBridge(action, payload = {}) {
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
    child.on("error", (error) => reject(error));
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(stderr.trim() || `Python bridge exited with code ${code ?? 1}.`));
        return;
      }
      try {
        resolve(JSON.parse(stdout));
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

async function invokeDesktopAction(action, payload = {}) {
  const response = await invokePythonBridge(action, payload);
  desktopSession.selectedReportPath = response?.state?.ui?.selected_report_path || "";
  desktopSession.lastResult = response?.state?.last_result ?? null;
  return response;
}

function registerHandlers() {
  ipcMain.handle("desktop:get-config", async () => ({
    isElectron: true,
    transport: "ipc",
  }));
  ipcMain.handle("desktop:get-state", async () => invokeDesktopAction("get-state"));
  ipcMain.handle("desktop:select-report", async (_event, payload) => invokeDesktopAction("select-report", payload));
  ipcMain.handle("desktop:delete-report", async (_event, payload) => invokeDesktopAction("delete-report", payload));
  ipcMain.handle("desktop:delete-all-reports", async () => invokeDesktopAction("delete-all-reports"));
  ipcMain.handle("desktop:run-mode", async (_event, payload) => invokeDesktopAction("run-mode", payload));
  ipcMain.handle("desktop:generate-report", async (_event, payload) => invokeDesktopAction("generate-report", payload));
  ipcMain.handle("desktop:import-trades", async (_event, payload) => invokeDesktopAction("import-trades", payload));
  ipcMain.handle("desktop:cash-adjust", async (_event, payload) => invokeDesktopAction("cash-adjust", payload));
  ipcMain.handle("desktop:save-runtime-config", async (_event, payload) => invokeDesktopAction("save-runtime-config", payload));
  ipcMain.handle("desktop:save-signal-config", async (_event, payload) => invokeDesktopAction("save-signal-config", payload));
  ipcMain.handle("desktop:pick-capital-xls", async () => null);
  ipcMain.handle("desktop:reload-application", async () => null);
  ipcMain.handle("desktop:close-application", async () => app.quit());
}

async function execute(window, code) {
  return window.webContents.executeJavaScript(code);
}

async function waitForDashboard(window) {
  for (let attempt = 0; attempt < 60; attempt += 1) {
    const ready = await execute(
      window,
      `(() => {
        const hasAppShell = !!document.querySelector('.app-shell');
        const hasViewer = !!document.querySelector('.viewer-card, .config-layout');
        const hasReportPanel = [...document.querySelectorAll('h2')].some((node) => node.textContent?.trim() === 'Generate Report');
        const isLoading = document.body.textContent?.includes('Starting desktop workspace');
        return hasAppShell && hasViewer && hasReportPanel && !isLoading;
      })();`,
    ).catch(() => false);
    if (ready) {
      return;
    }
    await delay(250);
  }
  throw new Error("Timed out waiting for the desktop dashboard to render.");
}

async function scrollElementIntoView(window, expression) {
  await execute(
    window,
    `(() => {
      const node = (${expression})();
      if (node) {
        node.scrollIntoView({ block: 'center', inline: 'nearest' });
      }
    })();`,
  );
  await delay(AFTER_ACTION_DELAY_MS);
}

async function getElementRect(window, expression, padding = PANEL_PADDING) {
  const rect = await execute(
    window,
    `(() => {
      const node = (${expression})();
      if (!node) {
        return null;
      }
      const box = node.getBoundingClientRect();
      return {
        x: Math.max(0, Math.floor(box.left) - ${padding}),
        y: Math.max(0, Math.floor(box.top) - ${padding}),
        width: Math.ceil(box.width) + ${padding * 2},
        height: Math.ceil(box.height) + ${padding * 2},
      };
    })();`,
  );

  if (!rect) {
    throw new Error(`Failed to locate element for expression: ${expression}`);
  }
  return rect;
}

async function clickButtonByText(window, text) {
  await execute(
    window,
    `(() => {
      const button = [...document.querySelectorAll('button')].find((node) => node.textContent?.trim() === ${JSON.stringify(text)});
      if (button) {
        button.click();
      }
    })();`,
  );
  await delay(AFTER_ACTION_DELAY_MS);
}

async function setReportBasis(window, value) {
  await execute(
    window,
    `(() => {
      const input = document.querySelector('input[name="report-basis"][value="${value}"]');
      if (input && !input.checked) {
        input.click();
      }
    })();`,
  );
  await delay(AFTER_ACTION_DELAY_MS);
}

async function setDateInput(window, value) {
  await execute(
    window,
    `(() => {
      const input = document.querySelector('section.rail-panel input[type="date"]');
      if (!input) {
        return;
      }
      input.value = ${JSON.stringify(value)};
      input.dispatchEvent(new Event('input', { bubbles: true }));
      input.dispatchEvent(new Event('change', { bubbles: true }));
    })();`,
  );
  await delay(AFTER_ACTION_DELAY_MS);
}

async function captureElement(window, filename, expression) {
  const rect = await getElementRect(window, expression);
  const image = await window.webContents.capturePage(rect);
  writePng(filename, image);
}

const TARGETS = [
  {
    filename: "gui-overview-current.png",
    capture: async (window) => {
      await execute(window, "window.scrollTo(0, 0);");
      await delay(AFTER_ACTION_DELAY_MS);
      writePng("gui-overview-current.png", await window.webContents.capturePage());
    },
  },
  {
    filename: "gui-controls-current.png",
    capture: async (window) => {
      await execute(window, "window.scrollTo(0, 0);");
      await delay(AFTER_ACTION_DELAY_MS);
      await captureElement(window, "gui-controls-current.png", SECTION_EXPR.hero);
    },
  },
  {
    filename: "gui-generate-report-latest-current.png",
    capture: async (window) => {
      await setReportBasis(window, "latest");
      await scrollElementIntoView(window, SECTION_EXPR.generateReport);
      await captureElement(window, "gui-generate-report-latest-current.png", SECTION_EXPR.generateReport);
    },
  },
  {
    filename: "gui-generate-report-historical-current.png",
    capture: async (window) => {
      await setReportBasis(window, "selected");
      await setDateInput(window, "2026-04-10");
      await scrollElementIntoView(window, SECTION_EXPR.generateReport);
      await captureElement(window, "gui-generate-report-historical-current.png", SECTION_EXPR.generateReport);
      await setReportBasis(window, "latest");
    },
  },
  {
    filename: "gui-import-trades-current.png",
    capture: async (window) => {
      await scrollElementIntoView(window, SECTION_EXPR.importTrades);
      await captureElement(window, "gui-import-trades-current.png", SECTION_EXPR.importTrades);
    },
  },
  {
    filename: "gui-cash-adjustment-current.png",
    capture: async (window) => {
      await scrollElementIntoView(window, SECTION_EXPR.cashAdjustment);
      await captureElement(window, "gui-cash-adjustment-current.png", SECTION_EXPR.cashAdjustment);
    },
  },
  {
    filename: "gui-config-current.png",
    capture: async (window) => {
      await execute(window, "window.scrollTo(0, 0);");
      await delay(AFTER_ACTION_DELAY_MS);
      await clickButtonByText(window, "Config");
      await delay(800);
      writePng("gui-config-current.png", await window.webContents.capturePage());
      await clickButtonByText(window, "Report");
    },
  },
];

async function main() {
  registerHandlers();

  const window = new BrowserWindow({
    show: false,
    width: WINDOW_WIDTH,
    height: WINDOW_HEIGHT,
    backgroundColor: "#f3ecdf",
    webPreferences: {
      preload: PRELOAD_PATH,
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
  });

  await window.loadFile(RENDERER_INDEX_PATH);
  await waitForDashboard(window);
  await delay(1200);

  for (const target of TARGETS) {
    await target.capture(window);
  }

  await window.destroy();
  app.quit();
}

app.whenReady()
  .then(main)
  .catch((error) => {
    console.error(error);
    app.exit(1);
  });
