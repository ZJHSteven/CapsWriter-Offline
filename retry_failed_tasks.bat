@echo off
setlocal
cd /d "%~dp0"
python retry_failed_tasks.py
pause
