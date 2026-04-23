@echo off
setlocal
cd /d "%~dp0"
uv run --with rich --with websockets --with numpy python retry_failed_tasks.py
pause
