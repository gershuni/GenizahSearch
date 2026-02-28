# Bug: Catalog Browse Tab Empty After v6.1.0 Update

## Status: FIXED (2026-02-27)

## Symptoms
1. In-app update downloaded v6.1.0, closed app, showed UAC admin prompt, then nothing happened
2. On reopen: error about cannot overwrite pgp.db (file locked by running process)
3. After reboot: new "Browse by Identification" tab visible but completely empty — no domains, authors, or works

## Root Cause

SQLite connections to sidecar .db files (pgp.db, fjms_enrichment.db, nli_crossref.db) were held open by singleton services. On Windows, open file handles prevent overwriting. The `reset_*_service()` functions existed but were only called AFTER downloads completed — too late, since `shutil.move()` had already failed.

Same issue in the installer path: `execute_update()` launched the Inno Setup installer and called `QApplication.quit()` without closing DB connections first.

## Fix

1. **Sidecar download path**: Added `_reset_sidecar_connections()` helper. Called BEFORE starting downloads (in `_start_sidecar_download`) and again AFTER all downloads complete (to pick up new files).
2. **Installer update path**: Added `reset_*_service()` calls in `execute_update()` before launching the installer subprocess.

## Root Cause Investigation (resolved)

### Issue 1: Installer can't overwrite pgp.db
- The app process holds an open SQLite connection to pgp.db
- Inno Setup tries to replace the file but it's locked
- Need: CloseApplications=yes in .iss, or close DB connections before update

### Issue 2: Empty catalog browse tab
Key code locations:
- `genizah_app.py:11530` — `create_catalog_browse_tab()`
- `genizah_app.py:12390` — `_catalog_populate_tree()`
- `genizah_app.py:6796` — lazy load: tree populated on first tab switch
- `genizah_app.py:10315` — `_navigate_to_catalog_browse()`

Likely cause: `FjmsService` can't find or open `fjms_enrichment.db` after install.
The sidecar is resolved via LOCALAPPDATA or install directory. If the v5.0.0 sidecar
wasn't bundled in the installer or the path resolution fails, all queries return [].

### Files to check next session:
1. `genizah_app.py:12390` — `_catalog_populate_tree()` — does it handle empty FjmsService gracefully?
2. `shared/fjms_service.py` — `_has_persons_titles` flag — if sidecar is v4 (no genizah_persons table), falls back to legacy which may return empty for a fresh install
3. `CompileScriptGenizah.iss` — is fjms_enrichment.db bundled? Is the v5.0.0 version bundled?
4. `gui_threads.py` — SidecarUpdateThread — does it download the latest sidecar version?
5. Check if the bundled fjms_enrichment.db in the installer is v4 (old) not v5.0.0 (new with persons/titles tables)

### Most likely fix
The installer bundles the OLD fjms_enrichment.db (v4.0.0 without genizah_persons/genizah_titles).
The new catalog browse code uses `_has_persons_titles` which returns False on v4, falling back
to legacy AuthorText queries. But the domain tree population likely works even on v4.

**More likely**: the sidecar path resolution is failing entirely. Need to check:
- Where does `get_fjms_service()` look for the .db file?
- Is there a bundled copy in `_internal/` and a user copy in LOCALAPPDATA?
- After fresh install, which copy is found?

## Fix Priority: HIGH — blocks all desktop catalog browse functionality
