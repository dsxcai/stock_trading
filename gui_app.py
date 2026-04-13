from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Start the Electron desktop in development mode with Vite",
    )
    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="Do not auto-run npm install when node_modules is missing",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Force remove dist/ and dist-electron/ to rebuild the frontend",
    )
    return parser.parse_args()


def _desktop_dir(repo_root: Path) -> Path:
    return repo_root / "desktop"


def _require_binary(name: str) -> str:
    resolved = shutil.which(name)
    if not resolved:
        raise RuntimeError(f"{name} is not installed or is not on PATH.")
    return resolved


def _run_npm(desktop_dir: Path, *npm_args: str) -> int:
    env = os.environ.copy()
    env["PYTHON"] = sys.executable
    env.pop("ELECTRON_RUN_AS_NODE", None)
    npm_bin = shutil.which("npm") or "npm"
    completed = subprocess.run(
        [npm_bin, *npm_args],
        cwd=desktop_dir,
        env=env,
        check=False,
    )
    return int(completed.returncode or 0)


def _ensure_desktop_dependencies(desktop_dir: Path, *, skip_install: bool) -> None:
    if (desktop_dir / "node_modules").exists():
        return
    if skip_install:
        raise RuntimeError("desktop/node_modules is missing. Run `npm install` under desktop/ first.")
    exit_code = _run_npm(desktop_dir, "install")
    if exit_code != 0:
        raise RuntimeError("npm install failed for desktop/.")


def _is_build_stale(desktop_dir: Path) -> bool:
    renderer_index = desktop_dir / "dist" / "index.html"
    electron_main = desktop_dir / "dist-electron" / "main.js"
    if not renderer_index.exists() or not electron_main.exists():
        return True

    build_mtime = min(renderer_index.stat().st_mtime, electron_main.stat().st_mtime)
    source_paths = [
        desktop_dir / "index.html",
        desktop_dir / "package.json",
        desktop_dir / "vite.config.ts",
        desktop_dir / "tsconfig.json",
        desktop_dir / "tsconfig.node.json",
        desktop_dir / "src",
        desktop_dir / "electron",
    ]

    for source_path in source_paths:
        if not source_path.exists():
            continue
        if source_path.is_file():
            if source_path.stat().st_mtime > build_mtime:
                return True
            continue
        for root, _, files in os.walk(source_path):
            for name in files:
                if (Path(root) / name).stat().st_mtime > build_mtime:
                    return True
    return False


def _ensure_desktop_build(desktop_dir: Path, *, force_rebuild: bool = False) -> None:
    renderer_index = desktop_dir / "dist" / "index.html"
    electron_main = desktop_dir / "dist-electron" / "main.js"
    if force_rebuild or _is_build_stale(desktop_dir):
        shutil.rmtree(desktop_dir / "dist", ignore_errors=True)
        shutil.rmtree(desktop_dir / "dist-electron", ignore_errors=True)
    elif renderer_index.exists() and electron_main.exists():
        return
    exit_code = _run_npm(desktop_dir, "run", "build")
    if exit_code != 0:
        raise RuntimeError("npm run build failed for desktop/.")


def main() -> int:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parent
    desktop_dir = _desktop_dir(repo_root)
    try:
        _require_binary("node")
        _require_binary("npm")
    except RuntimeError as exc:
        print(f"[GUI] {exc}", file=sys.stderr)
        return 1

    if not desktop_dir.exists():
        print("[GUI] desktop/ workspace is missing.", file=sys.stderr)
        return 1

    restart_flag = repo_root / ".restart_flag"
    restart_flag.unlink(missing_ok=True)
    force_rebuild = bool(getattr(args, "rebuild", False))

    try:
        while True:
            _ensure_desktop_dependencies(desktop_dir, skip_install=bool(args.skip_install))
            if args.dev:
                exit_code = _run_npm(desktop_dir, "run", "dev")
            else:
                _ensure_desktop_build(desktop_dir, force_rebuild=force_rebuild)
                exit_code = _run_npm(desktop_dir, "start")

            if restart_flag.exists():
                restart_flag.unlink(missing_ok=True)
                force_rebuild = False
                print("[GUI] Reload requested. Restarting desktop launcher...", file=sys.stderr)
                continue
            return exit_code
    except RuntimeError as exc:
        print(f"[GUI] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
