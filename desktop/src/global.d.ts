/*
 * Copyright (c) 2026 Sheng-Hsin Tsai
 * SPDX-License-Identifier: MIT
 */

import type { DesktopShellConfig } from "@/types";

export {};

declare global {
  interface Window {
    desktopApi?: {
      getConfig: () => Promise<DesktopShellConfig>;
      getState: () => Promise<import("@/types").ApiStateResponse>;
      selectReport: (payload: { report_path: string }) => Promise<import("@/types").ApiStateResponse>;
      deleteReport: (payload: { report_path: string }) => Promise<import("@/types").ApiStateResponse>;
      deleteAllReports: () => Promise<import("@/types").ApiStateResponse>;
      runMode: (payload: Record<string, unknown>) => Promise<import("@/types").ApiStateResponse>;
      generateReport: (payload: Record<string, unknown>) => Promise<import("@/types").ApiStateResponse>;
      importTrades: (payload: Record<string, unknown>) => Promise<import("@/types").ApiStateResponse>;
      cashAdjust: (payload: Record<string, unknown>) => Promise<import("@/types").ApiStateResponse>;
      saveRuntimeConfig: (payload: Record<string, unknown>) => Promise<import("@/types").ApiStateResponse>;
      saveSignalConfig: (payload: Record<string, unknown>) => Promise<import("@/types").ApiStateResponse>;
      pickCapitalXls: () => Promise<string | null>;
      reloadApplication: () => Promise<void>;
      closeApplication: () => Promise<void>;
    };
  }
}
