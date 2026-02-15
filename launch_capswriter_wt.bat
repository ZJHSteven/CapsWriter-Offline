@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ==================================================================
REM Purpose:
REM 1) Start start_server.exe + start_client.exe in one Windows Terminal
REM    window using two tabs.
REM 2) Always kill old instances first so one click = full restart.
REM 3) Support --dry-run for quick validation before scheduler setup.
REM ==================================================================

REM Absolute folder where this script is located.
set "SCRIPT_DIR=%~dp0"

REM Executable paths (same folder as this script).
set "SERVER_EXE=%SCRIPT_DIR%start_server.exe"
set "CLIENT_EXE=%SCRIPT_DIR%start_client.exe"

REM Process names for cleanup.
set "SERVER_PROC=start_server.exe"
set "CLIENT_PROC=start_client.exe"

REM Reusable Windows Terminal window name.
set "WT_WINDOW_NAME=capswriter"

REM Dry-run flag.
set "DRY_RUN=0"
if /I "%~1"=="--dry-run" set "DRY_RUN=1"

REM Enter script folder to avoid relative-path issues.
pushd "%SCRIPT_DIR%" >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Cannot enter script folder: %SCRIPT_DIR%
    exit /b 1
)

REM Validate required executables.
if not exist "%SERVER_EXE%" (
    echo [ERROR] Missing file: %SERVER_EXE%
    popd >nul
    exit /b 1
)
if not exist "%CLIENT_EXE%" (
    echo [ERROR] Missing file: %CLIENT_EXE%
    popd >nul
    exit /b 1
)

REM Validate Windows Terminal command.
where wt.exe >nul 2>nul
if errorlevel 1 (
    echo [ERROR] wt.exe not found in PATH.
    echo [HINT] Install Windows Terminal first.
    popd >nul
    exit /b 1
)

echo [INFO] Restarting CapsWriter services...

REM Stop old server/client instances first.
call :KillProcessIfRunning "%SERVER_PROC%"
if errorlevel 1 (
    popd >nul
    exit /b 1
)
call :KillProcessIfRunning "%CLIENT_PROC%"
if errorlevel 1 (
    popd >nul
    exit /b 1
)

REM In dry-run mode, stop here.
if "%DRY_RUN%"=="1" (
    echo [DRY-RUN] Cleanup check passed.
    echo [DRY-RUN] Would open two tabs in WT window "%WT_WINDOW_NAME%".
    popd >nul
    exit /b 0
)

REM Use start /NORMAL to avoid minimized launch behavior.
REM Use cmd /c so tab closes automatically when process exits.
start "" /NORMAL wt.exe -w "%WT_WINDOW_NAME%" new-tab --title "CapsWriter Server" --suppressApplicationTitle cmd /c "cd /d ""%SCRIPT_DIR%"" && ""%SERVER_EXE%""" ; new-tab --title "CapsWriter Client" --suppressApplicationTitle cmd /c "cd /d ""%SCRIPT_DIR%"" && timeout /t 1 /nobreak >nul && ""%CLIENT_EXE%"""
if errorlevel 1 (
    echo [ERROR] Failed to start Windows Terminal tabs.
    popd >nul
    exit /b 1
)

echo [DONE] Server and client started in Windows Terminal.
popd >nul
exit /b 0

REM ==================================================================
REM Function: KillProcessIfRunning
REM Input : process image name (for example, start_server.exe)
REM Output: errorlevel 0 on success
REM ==================================================================
:KillProcessIfRunning
set "PROC_NAME=%~1"

tasklist /FI "IMAGENAME eq %PROC_NAME%" 2>nul | find /I "%PROC_NAME%" >nul
if errorlevel 1 (
    echo [INFO] Not running: %PROC_NAME%
    exit /b 0
)

if "%DRY_RUN%"=="1" (
    echo [DRY-RUN] Would stop process: %PROC_NAME%
    exit /b 0
)

echo [INFO] Stopping process: %PROC_NAME%
taskkill /F /T /IM "%PROC_NAME%" >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Failed to stop process: %PROC_NAME%
    exit /b 1
)

echo [INFO] Stopped process: %PROC_NAME%
exit /b 0
