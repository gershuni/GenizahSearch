#!/usr/bin/env python
"""
Startup script for Genizah Search servers
Launches both the backend API and web interface

Controls:
  Ctrl+C  - Stop all servers and exit
  r       - Reboot all servers (restart)
"""
import subprocess
import sys
import os
import signal
import time
import threading
from pathlib import Path

# Windows-specific keyboard input
if sys.platform == 'win32':
    import msvcrt

# Set working directory to script location
os.chdir(Path(__file__).parent)

# Store process references
processes = []
reboot_requested = threading.Event()
shutdown_requested = threading.Event()

def stop_servers():
    """Stop all running servers"""
    global processes
    print("\n🛑 Stopping servers...")
    for proc in processes:
        if proc.poll() is None:  # Process is still running
            proc.terminate()

    # Wait for processes to terminate
    time.sleep(1)

    # Force kill if still running
    for proc in processes:
        if proc.poll() is None:
            proc.kill()

    processes = []
    print("✅ All servers stopped.")

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    shutdown_requested.set()
    stop_servers()
    sys.exit(0)

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

def start_servers():
    """Start both backend and web servers"""
    global processes
    processes = []  # Reset for fresh start

    print("=" * 60)
    print("🚀 Starting Genizah Search Servers")
    print("=" * 60)
    print()

    # Environment variables
    env = os.environ.copy()

    # Start Backend API on port 8000
    print("📡 Starting Backend API on http://localhost:8000")
    env['GENIZAH_PORT'] = '8000'
    backend_cmd = [sys.executable, "-m", "backend.main"]

    try:
        # Use CREATE_NEW_PROCESS_GROUP on Windows for better signal handling
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        backend_proc = subprocess.Popen(
            backend_cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=0,  # Unbuffered
            creationflags=creationflags
        )
        processes.append(backend_proc)
        print("   ✓ Backend API starting...")
    except Exception as e:
        print(f"   ✗ Failed to start backend: {e}")
        return False

    # Give backend time to start
    time.sleep(2)

    # Start Web Interface on port 8081
    print("🌐 Starting Web Interface on http://localhost:8081")
    env['GENIZAH_PORT'] = '8081'
    web_cmd = [sys.executable, "web/main.py"]

    try:
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        web_proc = subprocess.Popen(
            web_cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=0,  # Unbuffered
            creationflags=creationflags
        )
        processes.append(web_proc)
        print("   ✓ Web Interface starting...")
    except Exception as e:
        print(f"   ✗ Failed to start web interface: {e}")
        # Stop backend if web failed
        backend_proc.terminate()
        return

    print()
    print("=" * 60)
    print("✅ Servers are running!")
    print("=" * 60)
    print()
    print("📡 Backend API:      http://localhost:8000")
    print("   API Docs:         http://localhost:8000/api/docs")
    print()
    print("🌐 Web Interface:    http://localhost:8081")
    print()
    print("─" * 60)
    print("  Press 'r' to REBOOT servers  |  Ctrl+C to STOP and exit")
    print("─" * 60)
    print()

    # Start output reader threads
    import queue
    output_queue = queue.Queue()

    def read_output(proc, prefix):
        """Read output from process in a thread"""
        try:
            for line in iter(proc.stdout.readline, ''):
                if line:
                    output_queue.put((prefix, line.rstrip()))
                if proc.poll() is not None:
                    break
        except:
            pass

    # Start reader threads
    for i, proc in enumerate(processes):
        prefix = "[BACKEND]" if i == 0 else "[WEB]    "
        t = threading.Thread(target=read_output, args=(proc, prefix), daemon=True)
        t.start()

    # Monitor processes and check for keyboard input
    try:
        while True:
            # Check for keyboard input (Windows)
            if sys.platform == 'win32' and msvcrt.kbhit():
                key = msvcrt.getch().decode('utf-8', errors='ignore').lower()
                if key == 'r':
                    print("\n🔄 Reboot requested...")
                    reboot_requested.set()
                    return True  # Signal to reboot

            # Check if any process died
            for i, proc in enumerate(processes):
                if proc.poll() is not None:
                    server_name = "Backend API" if i == 0 else "Web Interface"
                    print(f"\n⚠️  {server_name} stopped unexpectedly!")
                    signal_handler(None, None)

            # Display queued output (non-blocking)
            try:
                while True:
                    prefix, line = output_queue.get_nowait()
                    print(f"{prefix} {line}")
            except queue.Empty:
                pass

            time.sleep(0.05)

    except KeyboardInterrupt:
        signal_handler(None, None)

    return False  # No reboot

def flush_keyboard_buffer():
    """Clear any pending keyboard input"""
    if sys.platform == 'win32':
        while msvcrt.kbhit():
            msvcrt.getch()

def main():
    """Main entry point with reboot support"""
    while True:
        should_reboot = start_servers()
        if should_reboot:
            stop_servers()
            reboot_requested.clear()
            flush_keyboard_buffer()  # Clear any buffered keystrokes
            print("\n" + "=" * 60)
            print("🔄 REBOOTING SERVERS...")
            print("=" * 60 + "\n")
            time.sleep(1)
            flush_keyboard_buffer()  # Clear again before restart
        else:
            break

if __name__ == "__main__":
    main()
