"""Wrapper to run translate_libraries_titles.py reliably on Windows."""
import subprocess
import sys
import os

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

proc = subprocess.Popen(
    [sys.executable, "scripts/translate_libraries_titles.py"] + sys.argv[1:],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
print(f"Running PID {proc.pid} — log: translate_libraries_log.txt")
proc.wait()
print(f"Exited with code {proc.returncode}")
