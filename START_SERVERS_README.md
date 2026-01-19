# Starting Genizah Search Servers

This document explains how to start both the Backend API and Web Interface together.

## Overview

Genizah Search has two components that need to run simultaneously:

1. **Backend API (FastAPI)** - Port 8000
   - Handles user corrections, authentication, comments, etc.
   - API documentation at: http://localhost:8000/api/docs

2. **Web Interface (NiceGUI)** - Port 8081
   - The main user interface
   - Accessible at: http://localhost:8081

## Quick Start

### Windows

**Option 1: Python Script (Recommended)**
```bash
python start_servers.py
```
- Shows output from both servers in one terminal
- Press Ctrl+C to stop both servers

**Option 2: Batch File**
```bash
start_servers.bat
```
- Opens two separate windows for each server
- Close the windows to stop servers

### Linux / Mac

**Make script executable (first time only):**
```bash
chmod +x start_servers.sh
```

**Run the script:**
```bash
./start_servers.sh
```
- Logs written to `backend.log` and `web.log`
- Press Ctrl+C to stop both servers

## Manual Start (Alternative)

If you prefer to start servers manually in separate terminals:

**Terminal 1 - Backend API:**
```bash
set GENIZAH_PORT=8000
python -m backend.main
```

**Terminal 2 - Web Interface:**
```bash
set GENIZAH_PORT=8081
python web/main.py
```

## Ports Configuration

You can customize ports by editing the startup scripts:

- `GENIZAH_BACKEND_PORT` - Backend API port (default: 8000)
- `GENIZAH_WEB_PORT` - Web Interface port (default: 8081)

Or set environment variables before running:

```bash
# Windows
set GENIZAH_PORT=9000
python -m backend.main

# Linux/Mac
GENIZAH_PORT=9000 python -m backend.main
```

## Troubleshooting

### Port Already in Use

If you see "port already in use" error:

**Windows:**
```bash
netstat -ano | findstr :8000
netstat -ano | findstr :8081
```

**Linux/Mac:**
```bash
lsof -i :8000
lsof -i :8081
```

Then kill the process using that port, or change the port in the startup script.

### Backend Not Responding

1. Check if backend started successfully in its terminal/log
2. Verify the database initialized: `data/genizah_users.db` should exist
3. Check CORS settings in `backend/config.py`

### Web Interface Can't Connect to Backend

1. Ensure backend is running on port 8000
2. Check that CORS includes `http://localhost:8081`
3. Verify `web/pages/corrections.py` API URLs are correct

## First Time Setup

On first run, the backend will:
1. Create `data/` directory
2. Initialize SQLite database at `data/genizah_users.db`
3. Generate secret key at `data/.secret_key`

These are normal and expected!

## Accessing the Application

Once both servers are running:

- **Main Application**: http://localhost:8081
- **Corrections System**: http://localhost:8081/corrections
- **API Documentation**: http://localhost:8000/api/docs
- **API Health Check**: http://localhost:8000/api/health

## Stopping the Servers

- **Python script**: Press `Ctrl+C` in the terminal
- **Batch file**: Close the server windows
- **Shell script**: Press `Ctrl+C` in the terminal
- **Manual**: Press `Ctrl+C` in each terminal

---

For more information, see the main project README.
