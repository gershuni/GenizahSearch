---
status: awaiting_human_verify
trigger: "Production genizah-web.service throws RuntimeError: File at path .../aggrid/dist is not a file when serving ag-grid static assets"
created: 2026-03-29T00:00:00Z
updated: 2026-03-29T00:00:00Z
---

## Current Focus

hypothesis: CONFIRMED - NiceGUI _get_esm handler checks exists() but not is_file(), so directory paths crash FileResponse
test: Monkey-patch applied to web/main.py, verified locally
expecting: Error should not recur after deploy
next_action: Awaiting human verification on production

## Symptoms

expected: ag-grid JavaScript assets should be served correctly to browser clients
actual: Starlette's FileResponse raises RuntimeError because the path points to a directory, not a file
errors: RuntimeError: File at path /home/ubuntu/GenizahSearch/venv/lib/python3.12/site-packages/nicegui/elements/aggrid/dist is not a file.
reproduction: Occurs on page loads that need ag-grid (search results tables, browse pages). Visible in systemd journal logs.
started: Logs show this happening Mar 28 21:14:25. Service running since Mar 27 14:04:09.

## Eliminated

## Evidence

- timestamp: 2026-03-29
  checked: NiceGUI 3.8.0 source nicegui/nicegui.py lines 108-117
  found: _get_esm route handler checks filepath.exists() but NOT filepath.is_file(). When path="" (empty string from bare directory URL), filepath resolves to the dist/ directory itself, passes exists(), and FileResponse crashes with RuntimeError.
  implication: This is a NiceGUI upstream bug affecting all ESM-based elements (aggrid, echart, mermaid, etc.)

- timestamp: 2026-03-29
  checked: Import map generation in dependencies.py lines 211-213
  found: Two entries generated per ESM module - one for 'nicegui-aggrid' -> index.js and one for 'nicegui-aggrid/' -> directory prefix. The trailing-slash entry enables sub-path resolution but also means bare directory URLs are valid import map targets.
  implication: Bots or malformed browser requests hitting the bare directory URL trigger the crash.

- timestamp: 2026-03-29
  checked: Local reproduction with pathlib
  found: Path('dist') / '' resolves to the dist directory itself. exists()=True, is_file()=False, is_dir()=True.
  implication: Adding is_file() check prevents the crash.

- timestamp: 2026-03-29
  checked: Monkey-patch in web/main.py
  found: Patch replaces _get_esm with _get_esm_patched that adds is_file() guard. Verified old route removed, new route installed. Empty path returns 404, valid file paths served normally.
  implication: Fix is safe and targeted.

## Resolution

root_cause: NiceGUI 3.8.0 _get_esm() handler (nicegui/nicegui.py:108-117) checks filepath.exists() but not filepath.is_file(). When a request hits /_nicegui/{version}/esm/{key}/ with empty trailing path, the resolved filepath is the dist/ directory itself. It passes exists() but Starlette's FileResponse raises RuntimeError because directories are not files.
fix: Added monkey-patch in web/main.py that replaces the _get_esm route with a patched version adding filepath.is_file() check. Directory paths now return HTTP 404 instead of crashing.
verification: Local test confirms patch replaces route handler, empty paths return 404, and valid file paths (index.js) serve correctly. Existing tests pass.
files_changed: [web/main.py]
