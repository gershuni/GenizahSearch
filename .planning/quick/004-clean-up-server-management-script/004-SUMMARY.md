---
phase: quick
plan: 004
subsystem: infrastructure
tags: [server-management, cleanup, devtools]
dependency-graph:
  requires: []
  provides: [single-server-management-script]
  affects: []
tech-stack:
  added: []
  patterns: [interactive-cli-loop, single-server-config]
key-files:
  created: []
  modified: [server.py]
  deleted: [start_servers.py, start_servers.bat]
decisions:
  - Single server config with constants (no SERVERS dict)
  - Persistent interactive loop with clear-screen between iterations
  - CLI one-shot commands preserved for scripting
  - Removed dangerous "kill all python*" from PowerShell fallback
metrics:
  duration: 2 min
  completed: 2026-02-06
---

# Quick Task 004: Clean Up Server Management Script

**One-liner:** Rewrote server.py as single-server interactive manager, removed redundant dual-server launchers

## What Was Done

### Task 1: Rewrite server.py as interactive web server manager
- Removed all backend/FastAPI references (port 8000, backend.main, SERVERS dict, API docs URL)
- Replaced dual-server SERVERS dict with simple constants (MODULE, DEFAULT_PORT, PID_FILE, SERVER_NAME)
- Added persistent interactive menu loop (options 1-7) that clears screen between iterations
- Preserved all CLI one-shot commands (start/stop/restart/status/check/kill)
- Simplified all functions to remove server_key parameter (single server)
- Removed dangerous "kill all python*" fallback from PowerShell force-kill
- Changed output to plain text indicators ([RUNNING]/[STOPPED]) instead of emojis

### Task 2: Remove redundant startup scripts
- Deleted start_servers.py (213 lines - threading, output queuing, reboot support for dual servers)
- Deleted start_servers.bat (41 lines - Windows batch file launching backend+frontend in separate cmd windows)
- Confirmed no stale PID files existed

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Rewrite server.py as interactive web server manager | 8550302 | server.py |
| 2 | Remove redundant startup scripts | 6fb9cb2 | start_servers.py, start_servers.bat |

## Verification Results

| Check | Result |
|-------|--------|
| server.py valid Python | OK |
| Zero "backend" references in server.py | OK (0 matches) |
| start_servers.py removed | OK |
| start_servers.bat removed | OK |
| Docstring mentions only web server | OK |
| Interactive loop in no-args path | OK |
| CLI one-shot commands available | OK |

## Deviations from Plan

None - plan executed exactly as written.

## Self-Check: PASSED
