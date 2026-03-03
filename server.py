#!/usr/bin/env python
"""
CLI to manage the Genizah Search web server.

Usage:
    python server.py              - Interactive server management menu
    python server.py start        - Start the web server
    python server.py stop         - Stop the web server
    python server.py restart      - Restart the web server
    python server.py status       - Check server status
    python server.py check        - Quick port check
    python server.py kill         - Force kill server process on port
"""
import subprocess
import sys
import os
import signal
import time
from pathlib import Path
from typing import Optional, Tuple

# Config
PROJECT_DIR = Path(__file__).parent
PID_FILE = PROJECT_DIR / '.server.pid'
DEFAULT_PORT = 8081
MODULE = 'web.main'
SERVER_NAME = 'Web Server'


def get_pid_on_port(port: int) -> Optional[int]:
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
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f"Ignored exception: {e}")
    return None


def is_server_running() -> Tuple[bool, Optional[int]]:
    """Check if the web server is running. Returns (is_running, pid)."""
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
    pid = get_pid_on_port(DEFAULT_PORT)
    if pid:
        return True, pid

    return False, None


def start_server() -> bool:
    """Start the web server."""
    running, pid = is_server_running()

    if running:
        print(f"  {SERVER_NAME} already running (PID {pid}) on port {DEFAULT_PORT}")
        return True

    print(f"  Starting {SERVER_NAME} on port {DEFAULT_PORT}...")

    # Set working directory
    os.chdir(PROJECT_DIR)

    env = os.environ.copy()
    env['GENIZAH_PORT'] = str(DEFAULT_PORT)
    env['NICEGUI_RELOAD'] = 'false'

    # Start detached process
    if sys.platform == 'win32':
        proc = subprocess.Popen(
            [sys.executable, "-m", MODULE],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
        )
    else:
        proc = subprocess.Popen(
            [sys.executable, "-m", MODULE],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True
        )

    # Save PID
    PID_FILE.write_text(str(proc.pid))

    # Wait and verify
    time.sleep(2)
    running, pid = is_server_running()
    if running:
        print(f"  {SERVER_NAME} started (PID {pid})")
        return True
    else:
        print(f"  Failed to start {SERVER_NAME}")
        PID_FILE.unlink(missing_ok=True)
        return False


def stop_server() -> bool:
    """Stop the web server."""
    running, pid = is_server_running()

    if not running:
        print(f"  {SERVER_NAME} is not running")
        PID_FILE.unlink(missing_ok=True)
        return True

    print(f"  Stopping {SERVER_NAME} (PID {pid})...")

    try:
        if sys.platform == 'win32':
            # Method 1: taskkill with /F /T
            result = subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True, text=True, check=False
            )
            if result.returncode != 0:
                print(f"  taskkill returned: {result.stderr.strip()}")
                # Method 2: Try killing by port
                port_pid = get_pid_on_port(DEFAULT_PORT)
                if port_pid and port_pid != pid:
                    print(f"  Trying to kill process on port {DEFAULT_PORT} (PID {port_pid})...")
                    subprocess.run(
                        ["taskkill", "/F", "/T", "/PID", str(port_pid)],
                        capture_output=True, check=False
                    )
        else:
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    except Exception as e:
        print(f"  Warning: {e}")

    PID_FILE.unlink(missing_ok=True)

    # Wait and check multiple times
    for i in range(3):
        time.sleep(1)
        running, new_pid = is_server_running()
        if not running:
            print(f"  {SERVER_NAME} stopped")
            return True
        elif new_pid and new_pid != pid:
            # Different PID now - try to kill that too
            print(f"  New process detected (PID {new_pid}), killing...")
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(new_pid)],
                capture_output=True, check=False
            )

    # Final check
    running, _ = is_server_running()
    if not running:
        print(f"  {SERVER_NAME} stopped")
        return True
    else:
        print(f"  {SERVER_NAME} may still be running - try manually: taskkill /F /PID {pid}")
        return False


def start(target: str = 'all') -> bool:
    """Start the server."""
    print("Starting server...")
    return start_server()


def stop(target: str = 'all') -> bool:
    """Stop the server."""
    print("Stopping server...")
    return stop_server()


def restart(target: str = 'all') -> bool:
    """Restart the server."""
    print("Restarting server...")
    stop_server()
    time.sleep(2)
    return start_server()


def check():
    """Quick port check - show what's on the server port."""
    print()
    print("=== Port Check ===")
    pid = get_pid_on_port(DEFAULT_PORT)
    if pid:
        print(f"  Port {DEFAULT_PORT}: IN USE (PID {pid}) - {SERVER_NAME}")
    else:
        print(f"  Port {DEFAULT_PORT}: FREE - {SERVER_NAME}")
    print()


def kill():
    """Force kill process on the server port."""
    print()
    print("=== Force Kill ===")
    killed = False

    pid = get_pid_on_port(DEFAULT_PORT)
    if pid:
        print(f"  Killing process on port {DEFAULT_PORT} ({SERVER_NAME})...")
        if sys.platform == 'win32':
            # Method 1: Try taskkill first
            result = subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True, text=True, check=False
            )
            if result.returncode == 0:
                print(f"    Killed PID {pid} via taskkill")
                killed = True
            else:
                # Method 2: Use PowerShell to kill by port (more reliable)
                print(f"    taskkill failed, trying PowerShell...")
                ps_cmd = f'''
                    $connections = Get-NetTCPConnection -LocalPort {DEFAULT_PORT} -ErrorAction SilentlyContinue
                    foreach ($conn in $connections) {{
                        $proc = Get-Process -Id $conn.OwningProcess -ErrorAction SilentlyContinue
                        if ($proc) {{
                            Write-Host "Killing $($proc.ProcessName) (PID $($conn.OwningProcess))"
                            Stop-Process -Id $conn.OwningProcess -Force -ErrorAction SilentlyContinue
                        }}
                    }}
                '''
                result = subprocess.run(
                    ["powershell", "-Command", ps_cmd],
                    capture_output=True, text=True, check=False
                )
                if result.stdout:
                    print(f"    {result.stdout.strip()}")
                killed = True
        else:
            try:
                os.kill(pid, signal.SIGKILL)
                print(f"    Killed PID {pid}")
                killed = True
            except Exception as e:
                print(f"    Failed: {e}")
    else:
        print(f"  Port {DEFAULT_PORT} is already free ({SERVER_NAME})")

    # Clean up PID file
    PID_FILE.unlink(missing_ok=True)

    if killed:
        time.sleep(3)

    # Verify
    print()
    print("=== Verifying ===")
    check()


def status():
    """Check server status."""
    print()
    print("=== Server Status ===")

    running, pid = is_server_running()
    if running:
        print(f"  {SERVER_NAME}: [RUNNING] (PID {pid}) on port {DEFAULT_PORT}")
        print()
        print(f"  URL: http://localhost:{DEFAULT_PORT}")
    else:
        print(f"  {SERVER_NAME}: [STOPPED]")
    print()


def show_menu():
    """Show interactive menu and return chosen command, or None to exit."""
    os.system('cls' if os.name == 'nt' else 'clear')

    print("=" * 50)
    print("  Genizah Search - Server Management")
    print("=" * 50)

    status()

    print("Options:")
    print("  1. Start server")
    print("  2. Stop server")
    print("  3. Restart server")
    print("  4. Status")
    print("  5. Check ports")
    print("  6. Force kill")
    print("  7. Exit")
    print()

    try:
        choice = input("Choose option [1-7]: ").strip()
        if choice == "1":
            return "start"
        elif choice == "2":
            return "stop"
        elif choice == "3":
            return "restart"
        elif choice == "4":
            return "status"
        elif choice == "5":
            return "check"
        elif choice == "6":
            return "kill"
        elif choice == "7":
            return None
        else:
            print("Invalid choice")
            time.sleep(1)
            return "continue"
    except (KeyboardInterrupt, EOFError):
        print()
        return None


def interactive_loop():
    """Persistent interactive menu loop."""
    while True:
        command = show_menu()
        if command is None:
            print("Exiting.")
            break
        elif command == "continue":
            continue
        elif command == "start":
            start()
        elif command == "stop":
            stop()
        elif command == "restart":
            restart()
        elif command == "status":
            status()
        elif command == "check":
            check()
        elif command == "kill":
            kill()

        # Pause so user can see output before screen clears
        print()
        try:
            input("Press Enter to continue...")
        except (KeyboardInterrupt, EOFError):
            print()
            break


def main():
    if len(sys.argv) < 2:
        # No arguments: enter interactive loop
        interactive_loop()
        sys.exit(0)

    command = sys.argv[1].lower()

    if command == "start":
        success = start()
        status()
        sys.exit(0 if success else 1)
    elif command == "stop":
        success = stop()
        sys.exit(0 if success else 1)
    elif command == "restart":
        success = restart()
        status()
        sys.exit(0 if success else 1)
    elif command == "status":
        status()
    elif command == "check":
        check()
    elif command == "kill":
        kill()
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
