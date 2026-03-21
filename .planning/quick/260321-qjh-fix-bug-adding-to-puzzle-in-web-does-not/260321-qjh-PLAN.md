# Quick Task 260321-qjh: Fix puzzle add bug + wire Firefox addon

## Tasks

### Task 1: Fix session restore for external fragments
- **files**: web/pages/puzzle.py
- **action**: Replace `if not fl_id: continue` in init_canvas with dual-path restore (NLI + external). Fix load_pending counter for skipped fragments.
- **verify**: External library fragments persist across page reloads
- **done**: Session restore handles both NLI (fl_id) and external (external_provider) fragments

### Task 2: Wire Firefox AMO link into extension banner
- **files**: web/pages/puzzle.py
- **action**: Add browser detection (Firefox vs Chrome) and show appropriate store link with matching brand color
- **verify**: Firefox users see AMO link (orange), Chrome users see CWS link (blue)
- **done**: Banner links to correct store per browser

### Task 3: Improve error handling in auto_add
- **files**: web/pages/puzzle.py
- **action**: Replace silent `except Exception: pass` with logger.error in _after_delay
- **verify**: Server logs show errors when auto_add fails
- **done**: Errors logged instead of silently swallowed
