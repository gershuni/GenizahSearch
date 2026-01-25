#!/usr/bin/env python
"""
CLI to manage the Genizah Search servers (backend + frontend).

Usage:
    python server.py start     - Start both servers
    python server.py stop      - Stop both servers
    python server.py restart   - Restart both servers
    python server.py status    - Check server status
    python server.py check     - Quick port check
    python server.py kill      - Force kill all server processes

    python server.py start backend   - Start backend only
    python server.py start frontend  - Start frontend only
    python server.py stop backend    - Stop backend only
    python server.py stop frontend   - Stop frontend only
"""
import subprocess
import sys
import os
import signal
import time
from pathlib import Path
from typing import Optional, Tuple, Dict

# Config
PROJECT_DIR = Path(__file__).parent
PID_DIR = PROJECT_DIR

SERVERS = {
    'backend': {
        'port': 8000,
        'module': 'backend.main',
        'pid_file': PID_DIR / '.backend.pid',
        'name': 'Backend API'
    },
    'frontend': {
        'port': 8081,
        'module': 'web.main',
        'pid_file': PID_DIR / '.server.pid',
        'name': 'Web Frontend'
    }
}


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
    except Exception:
        pass
    return None


def is_server_running(server_key: str) -> Tuple[bool, Optional[int]]:
    """Check if a specific server is running. Returns (is_running, pid)."""
    config = SERVERS[server_key]
    pid_file = config['pid_file']
    port = config['port']

    # First check PID file
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
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
    pid = get_pid_on_port(port)
    if pid:
        return True, pid

    return False, None


def start_server(server_key: str) -> bool:
    """Start a specific server."""
    config = SERVERS[server_key]
    running, pid = is_server_running(server_key)

    if running:
        print(f"  {config['name']} already running (PID {pid}) on port {config['port']}")
        return True

    print(f"  Starting {config['name']} on port {config['port']}...")

    # Set working directory
    os.chdir(PROJECT_DIR)

    env = os.environ.copy()
    env['GENIZAH_PORT'] = str(config['port'])
    if server_key == 'frontend':
        env['NICEGUI_RELOAD'] = 'false'

    # Start detached process
    if sys.platform == 'win32':
        proc = subprocess.Popen(
            [sys.executable, "-m", config['module']],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
        )
    else:
        proc = subprocess.Popen(
            [sys.executable, "-m", config['module']],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True
        )

    # Save PID
    config['pid_file'].write_text(str(proc.pid))

    # Wait and verify
    time.sleep(2)
    running, pid = is_server_running(server_key)
    if running:
        print(f"  {config['name']} started (PID {pid})")
        return True
    else:
        print(f"  Failed to start {config['name']}")
        config['pid_file'].unlink(missing_ok=True)
        return False


def stop_server(server_key: str) -> bool:
    """Stop a specific server."""
    config = SERVERS[server_key]
    running, pid = is_server_running(server_key)

    if not running:
        print(f"  {config['name']} is not running")
        config['pid_file'].unlink(missing_ok=True)
        return True

    print(f"  Stopping {config['name']} (PID {pid})...")

    try:
        if sys.platform == 'win32':
            # Try multiple methods to kill the process on Windows
            # Method 1: taskkill with /F /T
            result = subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True, text=True, check=False
            )
            if result.returncode != 0:
                print(f"  taskkill returned: {result.stderr.strip()}")
                # Method 2: Try killing by port
                port_pid = get_pid_on_port(config['port'])
                if port_pid and port_pid != pid:
                    print(f"  Trying to kill process on port {config['port']} (PID {port_pid})...")
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

    config['pid_file'].unlink(missing_ok=True)

    # Wait and check multiple times
    for i in range(3):
        time.sleep(1)
        running, new_pid = is_server_running(server_key)
        if not running:
            print(f"  {config['name']} stopped")
            return True
        elif new_pid and new_pid != pid:
            # Different PID now - try to kill that too
            print(f"  New process detected (PID {new_pid}), killing...")
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(new_pid)],
                capture_output=True, check=False
            )

    # Final check
    running, _ = is_server_running(server_key)
    if not running:
        print(f"  {config['name']} stopped")
        return True
    else:
        print(f"  {config['name']} may still be running - try manually: taskkill /F /PID {pid}")
        return False


def start(target: str = 'all') -> bool:
    """Start servers."""
    print("Starting servers...")

    if target == 'all':
        # Start backend first, then frontend
        success_backend = start_server('backend')
        time.sleep(1)
        success_frontend = start_server('frontend')
        return success_backend and success_frontend
    elif target in SERVERS:
        return start_server(target)
    else:
        print(f"Unknown target: {target}")
        return False


def stop(target: str = 'all') -> bool:
    """Stop servers."""
    print("Stopping servers...")

    if target == 'all':
        success_frontend = stop_server('frontend')
        success_backend = stop_server('backend')
        return success_backend and success_frontend
    elif target in SERVERS:
        return stop_server(target)
    else:
        print(f"Unknown target: {target}")
        return False


def restart(target: str = 'all') -> bool:
    """Restart servers."""
    print("Restarting servers...")
    stop(target)
    time.sleep(2)
    return start(target)


def check():
    """Quick port check - just show what's on the ports."""
    print("\n=== Port Check ===")
    for key, config in SERVERS.items():
        port = config['port']
        pid = get_pid_on_port(port)
        if pid:
            print(f"  Port {port}: IN USE (PID {pid}) - {config['name']}")
        else:
            print(f"  Port {port}: FREE - {config['name']}")
    print()


def kill():
    """Force kill all processes on server ports."""
    print("\n=== Force Killing All Server Processes ===")
    killed = False

    for key, config in SERVERS.items():
        port = config['port']
        pid = get_pid_on_port(port)
        if pid:
            print(f"  Killing processes on port {port} ({config['name']})...")
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
                        $connections = Get-NetTCPConnection -LocalPort {port} -ErrorAction SilentlyContinue
                        foreach ($conn in $connections) {{
                            $proc = Get-Process -Id $conn.OwningProcess -ErrorAction SilentlyContinue
                            if ($proc) {{
                                Write-Host "Killing $($proc.ProcessName) (PID $($conn.OwningProcess))"
                                Stop-Process -Id $conn.OwningProcess -Force -ErrorAction SilentlyContinue
                            }}
                        }}
                        # Also kill all python processes as fallback
                        Get-Process python* -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
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
            print(f"  Port {port} is already free ({config['name']})")

        # Clean up PID file
        config['pid_file'].unlink(missing_ok=True)

    if killed:
        time.sleep(3)

    # Verify
    print("\n=== Verifying ===")
    check()


def status():
    """Check server status."""
    print("\n=== Server Status ===")

    all_running = True
    for key, config in SERVERS.items():
        running, pid = is_server_running(key)
        if running:
            print(f"  {config['name']}: RUNNING (PID {pid}) on port {config['port']}")
        else:
            print(f"  {config['name']}: STOPPED")
            all_running = False

    print()
    if all_running:
        print("URLs:")
        print(f"  Backend API:    http://localhost:{SERVERS['backend']['port']}")
        print(f"  API Docs:       http://localhost:{SERVERS['backend']['port']}/api/docs")
        print(f"  Web Interface:  http://localhost:{SERVERS['frontend']['port']}")
    print()


def show_menu():
    """Show interactive menu."""
    status()

    print("Options:")
    print("  1. Start all servers")
    print("  2. Stop all servers")
    print("  3. Restart all servers")
    print("  4. Check ports")
    print("  5. Force kill all")
    print("  6. Exit")
    print()

    try:
        choice = input("Choose option [1-6]: ").strip()
        if choice == "1":
            return "start"
        elif choice == "2":
            return "stop"
        elif choice == "3":
            return "restart"
        elif choice == "4":
            return "check"
        elif choice == "5":
            return "kill"
        elif choice == "6":
            return None
        else:
            print("Invalid choice")
            return None
    except (KeyboardInterrupt, EOFError):
        print()
        return None


def main():
    if len(sys.argv) < 2:
        command = show_menu()
        if not command:
            sys.exit(0)
        target = 'all'
    else:
        command = sys.argv[1].lower()
        target = sys.argv[2].lower() if len(sys.argv) > 2 else 'all'

    if command == "start":
        success = start(target)
        status()
        sys.exit(0 if success else 1)
    elif command == "stop":
        success = stop(target)
        sys.exit(0 if success else 1)
    elif command == "restart":
        success = restart(target)
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
