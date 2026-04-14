# Desktop Workspace

This directory contains the new GUI stack:

- `Electron` for the desktop shell
- `React + TypeScript` for the renderer
- direct Python invocation through `gui_ipc.py` and `gui/desktop_backend.py`

## Install

```bash
npm install
```

## Development

```bash
npm run dev
```

This starts Vite and Electron together. The Electron main process launches the Python backend automatically and passes the current `PYTHON` environment variable through to the backend startup.
This starts watch builds for the renderer and Electron entrypoints, then launches Electron against the built `dist/` output. No local HTTP server is started in this path.

## Production Build

```bash
npm run build
npm start
```

## Refresh README Screenshots

```bash
../capture_readme_screenshots.sh
```

This root-level command installs desktop dependencies if needed, rebuilds the current desktop bundle, then launches Electron in a hidden window and refreshes the GUI screenshots under `../docs/images/`.

If you only want the raw capture step from inside `desktop/`, you can still run:

```bash
npm run capture:docs
```

The capture flow is implemented in [capture_gui_screenshots.js](/Users/sht/Stock_Work/stock_trading/utils/capture_gui_screenshots.js).

## Notes

- Renderer code lives under `src/`.
- Electron entrypoints live under `electron/`.
- Electron talks to Python directly without a local GUI web server.
- The old server-rendered HTML GUI path has been removed from the repo.
