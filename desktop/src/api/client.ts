/*
 * Copyright (c) 2026 Sheng-Hsin Tsai
 * SPDX-License-Identifier: MIT
 */

import type { ApiStateResponse, DesktopShellConfig } from "@/types";

export async function loadDesktopConfig(): Promise<DesktopShellConfig> {
  if (!window.desktopApi) {
    throw new Error("The Electron desktop bridge is unavailable in this renderer.");
  }
  return window.desktopApi.getConfig();
}

export class GuiApiClient {
  async getState(): Promise<ApiStateResponse> {
    if (!window.desktopApi) {
      throw new Error("The Electron desktop bridge is unavailable in this renderer.");
    }
    return window.desktopApi.getState();
  }

  async invokeAction(action: string, payload: unknown): Promise<ApiStateResponse> {
    if (!window.desktopApi) {
      throw new Error("The Electron desktop bridge is unavailable in this renderer.");
    }
    switch (action) {
      case "select-report":
        return window.desktopApi.selectReport(payload as { report_path: string });
      case "delete-report":
        return window.desktopApi.deleteReport(payload as { report_path: string });
      case "delete-all-reports":
        return window.desktopApi.deleteAllReports();
      case "run-mode":
        return window.desktopApi.runMode(payload as Record<string, unknown>);
      case "generate-report":
        return window.desktopApi.generateReport(payload as Record<string, unknown>);
      case "import-trades":
        return window.desktopApi.importTrades(payload as Record<string, unknown>);
      case "cash-adjust":
        return window.desktopApi.cashAdjust(payload as Record<string, unknown>);
      case "save-runtime-config":
        return window.desktopApi.saveRuntimeConfig(payload as Record<string, unknown>);
      case "save-signal-config":
        return window.desktopApi.saveSignalConfig(payload as Record<string, unknown>);
      case "init-clean-env":
        return window.desktopApi.initCleanEnv();
      case "export-zip":
        return window.desktopApi.exportZip(payload as Record<string, unknown>);
      case "import-zip":
        return window.desktopApi.importZip(payload as Record<string, unknown>);
      default:
        throw new Error(`Unsupported desktop action: ${action}`);
    }
  }
}
