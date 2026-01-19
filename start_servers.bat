@echo off
REM Startup script for Genizah Search servers (Windows)
REM Launches both the backend API and web interface

echo ============================================================
echo    Starting Genizah Search Servers
echo ============================================================
echo.

REM Set environment variables
set GENIZAH_BACKEND_PORT=8000
set GENIZAH_WEB_PORT=8081

echo Starting Backend API on http://localhost:%GENIZAH_BACKEND_PORT%
echo Starting Web Interface on http://localhost:%GENIZAH_WEB_PORT%
echo.

REM Start Backend API in new window
start "Genizah Backend API" cmd /k "set GENIZAH_PORT=%GENIZAH_BACKEND_PORT% && python -m backend.main"

REM Wait 2 seconds for backend to start
timeout /t 2 /nobreak >nul

REM Start Web Interface in new window
start "Genizah Web Interface" cmd /k "set GENIZAH_PORT=%GENIZAH_WEB_PORT% && python web/main.py"

echo.
echo ============================================================
echo    Servers are starting!
echo ============================================================
echo.
echo Backend API:      http://localhost:%GENIZAH_BACKEND_PORT%
echo API Docs:         http://localhost:%GENIZAH_BACKEND_PORT%/api/docs
echo.
echo Web Interface:    http://localhost:%GENIZAH_WEB_PORT%
echo.
echo Close the server windows to stop the servers.
echo ============================================================
echo.

pause
