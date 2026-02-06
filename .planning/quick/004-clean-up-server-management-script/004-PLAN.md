---
phase: quick
plan: 004
type: execute
wave: 1
depends_on: []
files_modified:
  - server.py
  - start_servers.py
  - start_servers.bat
autonomous: true

must_haves:
  truths:
    - "Running `python server.py` opens a persistent interactive menu for managing the web server"
    - "User can start, stop, restart the web server and check status without leaving the script"
    - "No references to backend, backend.main, or port 8000 remain in server.py"
    - "start_servers.py and start_servers.bat no longer exist"
  artifacts:
    - path: "server.py"
      provides: "Interactive server management for web app only"
      contains: "web.main"
  key_links:
    - from: "server.py"
      to: "web/main.py"
      via: "subprocess launching python -m web.main"
      pattern: "web\\.main"
---

<objective>
Rewrite server.py as a standalone interactive server management script for the web app only, and remove the obsolete start_servers.py and start_servers.bat files.

Purpose: The FastAPI backend was removed in January 2026. server.py still references `backend.main` on port 8000, start_servers.py and start_servers.bat are fully redundant backend+frontend launchers. Clean all of this up into a single, useful interactive tool.

Output: A rewritten server.py that manages only the NiceGUI web server (port 8081) with a persistent interactive loop, and removal of two redundant files.
</objective>

<execution_context>
@C:\Users\gersh\.claude/get-shit-done/workflows/execute-plan.md
@C:\Users\gersh\.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@server.py
@start_servers.py
@start_servers.bat
@web/main.py
</context>

<tasks>

<task type="auto">
  <name>Task 1: Rewrite server.py as interactive web server manager</name>
  <files>server.py</files>
  <action>
Rewrite server.py to be a standalone interactive server management script for ONLY the NiceGUI web server. Remove ALL references to backend, backend.main, port 8000, API docs, and the dual-server architecture.

The new server.py should:

1. **Single server config** - Only manage the web server (module: `web.main`, default port: 8081, PID file: `.server.pid`). Remove the SERVERS dict with backend/frontend split. Use a simple config dict or constants.

2. **Keep existing utility functions** (adapted for single server):
   - `get_pid_on_port(port)` - unchanged, uses netstat
   - `is_server_running()` - simplified, no server_key parameter needed
   - `start_server()` - launch `python -m web.main` detached, save PID, verify startup. Keep GENIZAH_PORT env var. Keep NICEGUI_RELOAD=false. Keep both Windows (CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS) and Unix (start_new_session) paths.
   - `stop_server()` - stop by PID then by port fallback. Keep the Windows taskkill + port-based fallback. Keep retry loop.
   - `restart_server()` - stop then start
   - `status()` - show running/stopped + URL if running (only `http://localhost:{port}`)
   - `kill()` - force kill on port. Keep PowerShell fallback for Windows BUT remove the dangerous "kill all python*" line from the PowerShell command.

3. **Persistent interactive loop** - This is the key change. When run without args (`python server.py`), enter a LOOP that:
   - Shows current server status
   - Displays menu options: (1) Start, (2) Stop, (3) Restart, (4) Status, (5) Check ports, (6) Force kill, (7) Exit
   - After executing any command, return to the menu (do NOT exit)
   - Only exit on option 7, Ctrl+C, or EOF
   - Clear screen between iterations would be nice (use `os.system('cls' if os.name == 'nt' else 'clear')` before showing menu)

4. **CLI mode preserved** - `python server.py start`, `python server.py stop`, `python server.py restart`, `python server.py status`, `python server.py check`, `python server.py kill` should still work as one-shot commands (execute and exit). Only the no-args mode enters the interactive loop.

5. **Update docstring** at the top to reflect the new single-server usage. Remove all "backend" and "frontend" target references.

6. **No emojis** in output. Use plain text indicators like `[RUNNING]`, `[STOPPED]`, `[OK]`, `[FAIL]`, `===` dividers.
  </action>
  <verify>
    Run `python server.py --help` or inspect the file to confirm:
    - No references to "backend", "backend.main", or port 8000
    - Interactive loop exists in the no-args path
    - CLI one-shot commands still available
    - Only manages web.main on port 8081
  </verify>
  <done>
    server.py manages only the web server, has a persistent interactive menu loop, supports CLI one-shot commands, and contains zero backend references.
  </done>
</task>

<task type="auto">
  <name>Task 2: Remove redundant startup scripts</name>
  <files>start_servers.py, start_servers.bat</files>
  <action>
Delete the following files which are fully redundant now that server.py is the single management script:

1. Delete `start_servers.py` - This was the old dual-server launcher with threading, output queuing, and reboot support. All of its functionality (and more) is now in server.py.

2. Delete `start_servers.bat` - This was the Windows batch file launcher for both backend + frontend in separate cmd windows.

3. Clean up any leftover PID files if they exist: `.backend.pid`, `.server.pid` (check and delete if present, they are artifacts of the old system).

Use `git rm` for the tracked files (start_servers.py, start_servers.bat) so they are properly staged for removal. For PID files, just delete them with plain rm/del if they exist (they are gitignored or untracked).
  </action>
  <verify>
    Confirm `start_servers.py` and `start_servers.bat` no longer exist in the working directory.
    Run `git status` to confirm both files are staged for deletion.
  </verify>
  <done>
    start_servers.py and start_servers.bat are deleted. No stale PID files remain.
  </done>
</task>

</tasks>

<verification>
1. `python -c "import ast; ast.parse(open('server.py').read()); print('OK')"` - server.py is valid Python
2. Grep server.py for "backend" - should return zero matches
3. `start_servers.py` and `start_servers.bat` do not exist
4. server.py docstring mentions only the web server
</verification>

<success_criteria>
- server.py is a clean, single-server interactive management tool
- No backend/FastAPI references remain in any server management scripts
- start_servers.py and start_servers.bat are removed
- CLI one-shot mode works: `python server.py status`
- Interactive mode works: `python server.py` enters persistent menu loop
</success_criteria>

<output>
After completion, create `.planning/quick/004-clean-up-server-management-script/004-SUMMARY.md`
</output>
