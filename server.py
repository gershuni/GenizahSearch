#!/usr/bin/env python
"""
Simple CLI to manage the Genizah Search web server.

Usage:
    python server.py start     - Start the web server (port 8081)
    python server.py stop      - Stop the web server
    python server.py restart   - Restart the web server
    python server.py status    - Check if server is running
"""
import subprocess
import sys
import os
import signal
import time
from pathlib import Path

# Config
PID_FILE = Path(__file__).parent / ".server.pid"
PORT = 8081
SERVER_MODULE = "web.main"

def get_pid_on_port(port: int) -> int | None:
    """Find PID listening on the given port."""
    try:
        result = subprocess.run(
            ["netstat", "-ano"],
            capture_output=True, text=True, check=True
        )
        for line in result.stdout.splitlines():
            if f":{port}" in line and "LISTENING" in line:
                parts = line.split()
                return int(parts[-1])
    except Exception:
        pass
    return None

def is_running() -> tuple[bool, int | None]:
    """Check if server is running. Returns (is_running, pid)."""
    # First check PID file
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            # Verify process exists
            if sys.platform == 'win32':
                result = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}"],
                    capture_output=True, text=True
                )
                if str(pid) in result.stdout:
                    return True, pid
        except (ValueError, subprocess.SubprocessError):
            pass

    # Fallback: check port
    pid = get_pid_on_port(PORT)
    if pid:
        return True, pid

    return False, None

def start():
    """Start the web server."""
    running, pid = is_running()
    if running:
        print(f"Server already running (PID {pid}) on port {PORT}")
        return False

    print(f"Starting web server on port {PORT}...")

    # Set working directory
    os.chdir(Path(__file__).parent)

    env = os.environ.copy()
    env['GENIZAH_PORT'] = str(PORT)
    env['NICEGUI_RELOAD'] = 'false'  # Disable reload for cleaner process management

    # Start detached process
    if sys.platform == 'win32':
        # Windows: use CREATE_NEW_PROCESS_GROUP and DETACHED_PROCESS
        proc = subprocess.Popen(
            [sys.executable, "-m", SERVER_MODULE],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
        )
    else:
        # Unix: use nohup-style detachment
        proc = subprocess.Popen(
            [sys.executable, "-m", SERVER_MODULE],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True
        )

    # Save PID
    PID_FILE.write_text(str(proc.pid))

    # Wait a moment and verify it started
    time.sleep(3)
    running, pid = is_running()
    if running:
        print(f"Server started (PID {pid})")
        print(f"Web interface: http://localhost:{PORT}")
        return True
    else:
        print("Failed to start server")
        PID_FILE.unlink(missing_ok=True)
        return False

def stop():
    """Stop the web server."""
    running, pid = is_running()
    if not running:
        print("Server is not running")
        PID_FILE.unlink(missing_ok=True)
        return True

    print(f"Stopping server (PID {pid})...")

    try:
        if sys.platform == 'win32':
            # Windows: use taskkill to kill process tree
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True, check=False
            )
        else:
            # Unix: send SIGTERM, then SIGKILL
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    except Exception as e:
        print(f"Warning: {e}")

    # Clean up PID file
    PID_FILE.unlink(missing_ok=True)

    # Verify stopped
    time.sleep(1)
    running, _ = is_running()
    if not running:
        print("Server stopped")
        return True
    else:
        print("Server may still be running")
        return False

def restart():
    """Restart the web server."""
    print("Restarting server...")
    stop()
    time.sleep(2)
    return start()

def status():
    """Check server status."""
    running, pid = is_running()
    if running:
        print(f"Server is RUNNING (PID {pid}) on port {PORT}")
        print(f"Web interface: http://localhost:{PORT}")
    else:
        print("Server is STOPPED")

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "start":
        success = start()
        sys.exit(0 if success else 1)
    elif command == "stop":
        success = stop()
        sys.exit(0 if success else 1)
    elif command == "restart":
        success = restart()
        sys.exit(0 if success else 1)
    elif command == "status":
        status()
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)

if __name__ == "__main__":
    main()
