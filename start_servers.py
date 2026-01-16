#!/usr/bin/env python
"""
Startup script for Genizah Search servers
Launches both the backend API and web interface
"""
import subprocess
import sys
import os
import signal
import time
from pathlib import Path

# Set working directory to script location
os.chdir(Path(__file__).parent)

# Store process references
processes = []

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print("\n\n🛑 Shutting down servers...")
    for proc in processes:
        if proc.poll() is None:  # Process is still running
            proc.terminate()

    # Wait for processes to terminate
    time.sleep(1)

    # Force kill if still running
    for proc in processes:
        if proc.poll() is None:
            proc.kill()

    print("✅ All servers stopped.")
    sys.exit(0)

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

def start_servers():
    """Start both backend and web servers"""
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
        backend_proc = subprocess.Popen(
            backend_cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        processes.append(backend_proc)
        print("   ✓ Backend API starting...")
    except Exception as e:
        print(f"   ✗ Failed to start backend: {e}")
        return

    # Give backend time to start
    time.sleep(2)

    # Start Web Interface on port 8081
    print("🌐 Starting Web Interface on http://localhost:8081")
    env['GENIZAH_PORT'] = '8081'
    web_cmd = [sys.executable, "web/main.py"]

    try:
        web_proc = subprocess.Popen(
            web_cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
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
    print("Press Ctrl+C to stop both servers")
    print("=" * 60)
    print()

    # Monitor processes and show output
    try:
        while True:
            # Check if any process died
            for i, proc in enumerate(processes):
                if proc.poll() is not None:
                    server_name = "Backend API" if i == 0 else "Web Interface"
                    print(f"\n⚠️  {server_name} stopped unexpectedly!")

                    # Read any remaining output
                    output = proc.stdout.read()
                    if output:
                        print(output)

                    # Stop all servers
                    signal_handler(None, None)

            # Read and display output from both processes
            for i, proc in enumerate(processes):
                try:
                    line = proc.stdout.readline()
                    if line:
                        prefix = "[BACKEND]" if i == 0 else "[WEB]    "
                        print(f"{prefix} {line.rstrip()}")
                except:
                    pass

            time.sleep(0.1)

    except KeyboardInterrupt:
        signal_handler(None, None)

if __name__ == "__main__":
    start_servers()
