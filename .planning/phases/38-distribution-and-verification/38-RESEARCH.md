# Phase 38: Distribution and Verification - Research

**Researched:** 2026-02-18
**Domain:** Desktop/web sidecar bundling, offline verification, in-app update mechanism
**Confidence:** HIGH

## Summary

Phase 38 bundles three SQLite sidecar databases (pgp.db, fjms_enrichment.db, nli_crossref.db) into both the desktop installer and web server deployment, verifies that all three work offline from local data, and adds a sidecar-specific update mechanism. The codebase is extremely well-prepared for this phase -- all three services already use identical patterns (read-only URI mode, singleton factory, `is_available()` graceful degradation, `meta` table with version), and the build/deploy infrastructure already handles two of the three sidecars.

The main work is: (1) adding pgp.db to `build_app.bat` / `GenizahSearchPro.spec` with a `pgp_data/` entry, (2) updating `deploy.sh` documentation for pgp.db, (3) building a `SidecarUpdateChecker` that mirrors the existing `UpdateCheckerThread` pattern but reads a JSON manifest for sidecar versions, (4) writing automated verification tests that assert PGP/FJMS/NLI browse paths use only local SQLite with zero Supabase calls, and (5) updating the deployment docs.

**Primary recommendation:** Follow the exact patterns already established for fjms_enrichment.db and nli_crossref.db bundling. The sidecar update checker should use the existing GitHub Releases infrastructure (a JSON manifest asset attached to releases) with a dedicated `SidecarUpdateThread` modeled on `UpdateCheckerThread`.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Bundle ALL three sidecars: pgp.db, fjms_enrichment.db, nli_crossref.db
- Same set for both desktop installer and web server deployment -- consistent architecture
- No size concern -- 200MB+ is acceptable for a research tool with local data
- Silent operation -- no "Offline Mode" indicator; local features just work
- Online feature failure handling: Claude's discretion (match existing app patterns)
- Verification method: automated code-path verification that PGP browse paths use local SQLite with no Supabase calls
- Verification scope: all three sidecars, not just PGP -- verify FJMS and NLI features also work from local data
- In-app update check: auto-check on startup (non-blocking, silent unless update available)
- Download behavior: ask user first -- notification like "New data available (X MB). Download now?"
- User controls bandwidth; no silent large downloads
- Graceful degradation: features depending on a missing sidecar simply don't appear
- No error dialogs for missing sidecars -- they're optional enhancements
- Integrity check: version check only (read meta table version on startup) -- fast, catches stale files

### Claude's Discretion
- Sidecar file location on user's machine (app directory vs AppData)
- Online feature failure UX when offline (error on attempt vs disable controls)
- Update check hosting source (GitHub releases vs web endpoint)
- Web app Supabase fallback when sidecar missing
- Sidecar health display (About screen or implicit)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| DIST-01 | pgp.db bundled in desktop app installer (build_app.bat) | build_app.bat already bundles fjms/nli sidecars with `--add-data` pattern; add pgp_data/pgp.db with same pattern. Inno Setup `[Files]` section already recurses dist directory. |
| DIST-02 | pgp.db deployed alongside web server | deploy.sh + DEPLOYMENT_TECHNICAL.md document sidecar upload pattern. pgp_data/pgp.db already exists on dev machine; needs scp to server and docs update. |
| PERF-01 | Desktop PGP metadata/transcription browsing works without internet (images excluded) | PgpService already uses local SQLite exclusively. Verification tests need to assert no Supabase imports/calls in the PGP browse code paths. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| sqlite3 | stdlib | All sidecar access | Already used by all 3 services; read-only URI mode |
| PyInstaller | 6.x | Desktop bundling | Already used via build_app.bat; `--add-data` for sidecars |
| Inno Setup | 6.x | Windows installer | Already used via CompileScriptGenizah.iss |
| requests | 2.x | GitHub API for update check | Already used by UpdateCheckerThread |
| pytest | 9.x | Verification tests | Already used for 633 existing tests |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| json (stdlib) | - | Sidecar manifest parsing | Update checker reads manifest JSON |
| tempfile (stdlib) | - | Download staging | Sidecar download to temp before replacing |
| shutil (stdlib) | - | File operations | Atomic sidecar replacement |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| GitHub Releases manifest | Web endpoint on genizahsearch.com | GitHub is already in use for app updates; no server-side code needed |
| JSON manifest file | GitHub release tag metadata | JSON asset file is more flexible, can list individual sidecar versions and sizes |
| Separate sidecar update thread | Extend UpdateCheckerThread | Separate thread is cleaner; app updates and sidecar updates have different triggers and UI |

## Architecture Patterns

### Current Sidecar Architecture (Already Established)

```
Project Root (or _internal/ in PyInstaller)
├── fist_data/
│   └── fjms_enrichment.db     (687 MB, v4.0.0)
├── nli_data/
│   └── nli_crossref.db        (248 MB, v1.2.0)
├── pgp_data/                   ← NEW: add to build
│   └── pgp.db                  (147 MB, v1.0.0)
├── shared/
│   ├── fjms_service.py         (FjmsService, singleton, graceful degradation)
│   ├── nli_crossref_service.py (NliCrossrefService, singleton, graceful degradation)
│   └── document_service.py     (PgpService, singleton, graceful degradation)
└── libraries.csv               (anchor for _find_project_root())
```

### Pattern 1: Sidecar Service Initialization
**What:** All three services share identical patterns
**When to use:** This IS the pattern -- extend, don't reinvent

```python
# All three services use this exact pattern:
_SIDECAR_FILENAME = "pgp.db"      # or fjms_enrichment.db, nli_crossref.db
_SIDECAR_DIR = "pgp_data"          # or fist_data, nli_data

def _find_project_root() -> Optional[Path]:
    """Find project root by looking for libraries.csv up from this file."""
    current = Path(__file__).resolve().parent
    for _ in range(5):
        if (current / "libraries.csv").exists():
            return current
        current = current.parent
    return None

class XxxService:
    def __init__(self, db_path=None, thread_safe=False):
        if db_path is None:
            root = _find_project_root()
            if root:
                db_path = str(root / _SIDECAR_DIR / _SIDECAR_FILENAME)
        # ... open read-only, graceful None on failure

    def is_available(self) -> bool:
        return self._conn is not None

    def get_version(self) -> Optional[str]:
        # Read from meta table: key='version'

# Singleton factory:
_default_service = None
def get_xxx_service(thread_safe=False):
    global _default_service
    if _default_service is None:
        _default_service = XxxService(thread_safe=thread_safe)
    return _default_service
```

### Pattern 2: PyInstaller Data Bundling
**What:** `--add-data "source;dest"` in build_app.bat
**When to use:** Adding any data file to the desktop build

```bat
REM Current build_app.bat already has:
--add-data "fist_data\fjms_enrichment.db;fist_data" ^
--add-data "nli_data\nli_crossref.db;nli_data" ^
REM Add:
--add-data "pgp_data\pgp.db;pgp_data" ^
```

In the built distribution:
- `dist/GenizahSearchPro/_internal/fist_data/fjms_enrichment.db` (verified exists)
- `dist/GenizahSearchPro/_internal/nli_data/nli_crossref.db` (verified exists)
- `dist/GenizahSearchPro/_internal/pgp_data/pgp.db` (will be added)

The `_find_project_root()` walks from `_internal/shared/` up to `_internal/` and finds `libraries.csv` there. Sidecar dirs are siblings of `libraries.csv` under `_internal/`.

### Pattern 3: GitHub Releases Update Check (Existing)
**What:** `UpdateCheckerThread` already checks GitHub API `/repos/gershuni/GenizahSearch/releases/latest`
**When to use:** This pattern should be extended for sidecar updates

```python
# Existing: gui_threads.py line 334-383
class UpdateCheckerThread(QThread):
    def run(self):
        url = "https://api.github.com/repos/gershuni/GenizahSearch/releases/latest"
        resp = requests.get(url, timeout=5)
        data = resp.json()
        tag = data.get('tag_name', '')
        assets = data.get('assets', [])
        # Compares version, signals UI
```

### Pattern 4: Sidecar Update Check (New, Modeled on Existing)
**What:** Check a `sidecar-versions.json` manifest attached to GitHub releases
**When to use:** Startup auto-check for sidecar data updates

Proposed manifest format (attached as a release asset):
```json
{
  "pgp.db": {"version": "1.0.0", "size_mb": 147, "url": "https://github.com/gershuni/GenizahSearch/releases/download/data-v1/pgp.db"},
  "fjms_enrichment.db": {"version": "4.0.0", "size_mb": 687, "url": "https://..."},
  "nli_crossref.db": {"version": "1.2.0", "size_mb": 248, "url": "https://..."}
}
```

The `SidecarUpdateThread` reads this manifest, compares versions against local `meta` table values, and reports which sidecars have updates. This avoids encoding sidecar versions in the app release tag.

### Pattern 5: Offline Feature Failure UX (Existing)
**What:** Matching how Supabase-dependent features already behave when offline
**When to use:** Online-only features (corrections, comments, cloud sync)

```python
# Existing pattern in genizah_app.py:
server_available = self.corrections_client.is_server_available()
if not server_available:
    # Skip API calls, hide community UI elements
    self.btn_view_comments.setVisible(False)
    return
```

**Recommendation for Claude's Discretion:** Match this pattern -- hide/disable controls that require online access rather than showing errors on attempt. This is consistent with the existing UX.

### Anti-Patterns to Avoid
- **Building separate update infrastructure:** The GitHub Releases + requests pattern already works for app updates. Don't build a custom web endpoint.
- **Downloading sidecars to _internal/:** PyInstaller's _internal is a read-only bundled directory. Sidecar updates should go to a user-writable location (same level as Genizah_Index).
- **Blocking startup on network checks:** The app already does non-blocking update checks via QThread. Sidecar version check must follow the same pattern.
- **Replacing sidecars while SQLite connections are open:** Must close service singleton, replace file, re-initialize singleton.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Version comparison | Custom parser | Existing SemVer list comparison (already in UpdateCheckerThread) | Edge cases with pre-release versions |
| Download with progress | Custom HTTP client | `UpdateDownloaderThread` pattern (requests + streaming) | Already handles cancellation, errors, progress |
| File location resolution | Custom path logic | `_find_project_root()` pattern | Already works in dev, PyInstaller, and web server |
| Atomic file replacement | Direct overwrite | tempfile + shutil.move | Prevents corruption on crash during download |

**Key insight:** The entire infrastructure for checking updates, downloading files, and showing progress already exists for the app-level update system. The sidecar update feature is a smaller, parallel version of the same pattern.

## Common Pitfalls

### Pitfall 1: PyInstaller _internal is Read-Only for Updates
**What goes wrong:** Attempting to write updated sidecar files to the `_internal/` directory in a PyInstaller bundle fails because it's the installed application directory (often in Program Files, which requires admin privileges).
**Why it happens:** The bundled sidecars are immutable -- they came with the installer. Updates need a separate writable location.
**How to avoid:** Store sidecar updates in a user-writable data directory (e.g., `%LOCALAPPDATA%/GenizahSearchPro/data/`). At startup, check both locations -- prefer the user-data copy if its version is newer than the bundled copy.
**Warning signs:** `PermissionError` on file write in installed app.

### Pitfall 2: SQLite Connection Must Be Closed Before File Replacement
**What goes wrong:** Attempting to replace a sidecar .db file while the singleton service has an open read-only connection fails on Windows (file locking).
**Why it happens:** Windows locks files that have open handles. SQLite read-only connections hold a file handle.
**How to avoid:** Before replacing a sidecar: (1) call service.close(), (2) set module-level singleton to None, (3) replace file, (4) let lazy re-initialization create a new connection on next access.
**Warning signs:** `PermissionError: [WinError 32]` or `The process cannot access the file`.

### Pitfall 3: Inno Setup Already Copies Everything Recursively
**What goes wrong:** Over-engineering the installer config when the existing `[Files]` section already has `Flags: ignoreversion recursesubdirs createallsubdirs`.
**Why it happens:** The InnoSetup script copies the entire `dist/GenizahSearchPro/` tree recursively. Adding pgp.db to the PyInstaller build is sufficient -- Inno Setup picks it up automatically.
**How to avoid:** Only modify `build_app.bat` and `GenizahSearchPro.spec`. Do NOT modify `CompileScriptGenizah.iss` for pgp.db bundling.

### Pitfall 4: Service Singleton Caching
**What goes wrong:** After a sidecar update, the old singleton still serves stale data because the module-level `_default_service` was never reset.
**Why it happens:** Singletons are created once and cached forever.
**How to avoid:** Add a `reset_service()` function to each service module that sets `_default_service = None`. Call it after sidecar replacement.

### Pitfall 5: Web Deployment pgp.db Not in Git
**What goes wrong:** Running `deploy.sh` (git pull) doesn't bring pgp.db because it's in `.gitignore`.
**Why it happens:** All sidecars are excluded from git due to size. They must be uploaded manually.
**How to avoid:** Update DEPLOYMENT_TECHNICAL.md with pgp.db upload instructions matching the existing fjms/nli pattern. Add pgp_data/ to the scp commands.

### Pitfall 6: Installer Size Growth
**What goes wrong:** Adding 147MB pgp.db to the build increases the installer from ~96MB to ~120MB+ (with Inno Setup compression).
**Why it happens:** pgp.db is 147MB uncompressed. Inno Setup's solid LZMA compression should bring it down significantly.
**How to avoid:** This is expected and acceptable per user's decision ("200MB+ is acceptable"). Just note it in release notes.

## Code Examples

### Example 1: Adding pgp.db to build_app.bat

```bat
--add-data "pgp_data\pgp.db;pgp_data" ^
```

Add this line after the existing nli_crossref.db line in build_app.bat.

### Example 2: Sidecar Version Check on Startup

```python
class SidecarUpdateThread(QThread):
    """Check for sidecar data updates via GitHub release manifest."""

    update_available = pyqtSignal(list)  # list of {name, current, available, size_mb, url}
    error_signal = pyqtSignal(str)

    MANIFEST_URL = "https://api.github.com/repos/gershuni/GenizahSearch/releases/tags/data-latest"

    def run(self):
        try:
            resp = requests.get(self.MANIFEST_URL, timeout=5)
            if resp.status_code != 200:
                return  # Silent failure for auto-check

            data = resp.json()
            # Find sidecar-versions.json asset
            manifest_url = None
            for asset in data.get('assets', []):
                if asset['name'] == 'sidecar-versions.json':
                    manifest_url = asset['browser_download_url']
                    break
            if not manifest_url:
                return

            manifest = requests.get(manifest_url, timeout=5).json()

            # Compare local versions
            updates = []
            for sidecar_name, remote_info in manifest.items():
                local_version = self._get_local_version(sidecar_name)
                remote_version = remote_info['version']
                if self._is_newer(remote_version, local_version):
                    updates.append({
                        'name': sidecar_name,
                        'current': local_version or 'not installed',
                        'available': remote_version,
                        'size_mb': remote_info['size_mb'],
                        'url': remote_info['url'],
                    })

            if updates:
                self.update_available.emit(updates)

        except Exception as e:
            self.error_signal.emit(str(e))
```

### Example 3: Sidecar Update Location Strategy

```python
# For PyInstaller bundled app:
#   Bundled (read-only):  <exe_dir>/_internal/pgp_data/pgp.db
#   Updated (writable):   %LOCALAPPDATA%/GenizahSearchPro/data/pgp_data/pgp.db

# Modified _find_sidecar_path() priority:
# 1. User data dir (updated sidecar, if newer version)
# 2. Bundled location (original from installer)
# 3. Development location (project root)
```

### Example 4: Verification Test Pattern

```python
def test_pgp_browse_no_supabase_calls(monkeypatch):
    """PGP browse path uses only local SQLite, no Supabase."""
    import shared.document_service as ds

    # Monkeypatch to detect any Supabase import attempt
    original_import = __builtins__.__import__
    def guarded_import(name, *args, **kwargs):
        if 'supabase' in name.lower():
            raise AssertionError(f"Supabase import detected: {name}")
        return original_import(name, *args, **kwargs)

    # Create temp pgp.db with test data
    svc = PgpService(db_path=test_db_path)

    # Exercise all PGP browse functions
    svc.get_document_for_fragment("test_sys_id")
    svc.get_fragments_for_document(1234)
    svc.get_transcription_for_document(1234)
    svc.get_document_metadata(1234)
    svc.get_sources_for_document(1234)
    svc.get_all_sources_for_fragment("test_sys_id")
    # All returned data came from local SQLite -- no network calls made
```

### Example 5: Graceful Degradation When Sidecar Missing

```python
# This pattern ALREADY EXISTS in all three services:
svc = get_pgp_service()
if not svc.is_available():
    # Feature simply doesn't appear in UI
    return  # No error, no dialog

# For the desktop app, this manifests as:
# - No PGP badge on search results
# - No transcription tab content
# - No domain classifications
# But search, browse, and all other features work fine
```

## Claude's Discretion Recommendations

### 1. Sidecar File Location on User's Machine

**Recommendation: App directory (bundled) + AppData for updates**

- Bundled sidecars live in `_internal/` (read-only, from installer)
- Updated sidecars go to `%LOCALAPPDATA%/GenizahSearchPro/data/` (writable)
- Service initialization checks both, preferring the newer version
- Rationale: Matches how `Genizah_Index/` already uses `%LOCALAPPDATA%/GenizahSearchPro/Index/` as the AppData path (see genizah_core.py line 1697-1709)

### 2. Online Feature Failure UX When Offline

**Recommendation: Hide/disable controls (match existing pattern)**

- The app already uses `is_server_available()` to hide community features when offline
- Apply same pattern: hide buttons/tabs that need Supabase (corrections, comments, cloud sync)
- Local features (search, browse, PGP, FJMS, NLI) work silently
- No "Offline Mode" indicator -- per user decision

### 3. Update Check Hosting Source

**Recommendation: GitHub Releases with a manifest asset**

- Create a dedicated release tag `data-latest` (or `sidecar-v1`) on GitHub
- Attach a `sidecar-versions.json` manifest file as a release asset
- Attach the actual sidecar .db files as release assets
- The existing `UpdateCheckerThread` already uses GitHub Releases API; the sidecar checker mirrors this
- No server-side code needed; GitHub CDN handles downloads
- Rationale: The app already checks `api.github.com/repos/gershuni/GenizahSearch/releases/latest` for app updates. Using the same infrastructure for data updates minimizes new dependencies.

### 4. Web App Supabase Fallback When Sidecar Missing

**Recommendation: Require sidecar (no Supabase fallback)**

- The web app already uses PgpService (SQLite) exclusively for PGP data
- All PGP Supabase code paths have already been replaced by Phase 36
- The web server deployment guide already documents sidecar upload
- Adding a Supabase fallback would re-introduce a code path that was intentionally removed
- If pgp.db is missing, PGP features simply won't appear (graceful degradation)
- Rationale: Consistent architecture; the web app's fjms and nli features already degrade gracefully when their sidecars are missing.

### 5. Sidecar Health Display

**Recommendation: About screen "Data Sources" section**

- Add a small table to the About section showing installed sidecar versions
- Format: "PGP Data: v1.0.0 (35,839 documents) | FJMS: v4.0.0 | NLI: v1.2.0"
- Use the existing `meta` table data (already queried by `get_version()`)
- Show "Not installed" for missing sidecars
- This is lightweight and informative without being intrusive
- Rationale: Researchers occasionally need to know what data version they're working with for citations and reproducibility.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Supabase for PGP data | SQLite sidecar (pgp.db) | Phase 35-36, Feb 2026 | Desktop and web both use local SQLite |
| Manual sidecar upload only | Bundled in installer + manual web upload | Phase 25-29, Feb 2026 | Two sidecars already bundled |
| No sidecar update mechanism | App update only via GitHub Releases | Jan 2026 | Phase 38 adds sidecar-specific update |

**Deprecated/outdated:**
- Supabase PGP queries: Replaced by PgpService in Phase 36 (tables kept for legacy desktop users per prior decision, but new code doesn't use them)
- `fist_data/pgp.db` (0 bytes): Stale placeholder, real file is in `pgp_data/pgp.db`
- `fist_data/nli_crossref.db` (0 bytes): Stale placeholder, real file is in `nli_data/nli_crossref.db`

## Open Questions

1. **GitHub Release Asset Size Limits**
   - What we know: GitHub Releases supports assets up to 2GB per file. The largest sidecar (fjms_enrichment.db) is 687MB.
   - What's unclear: Whether GitHub's CDN provides reliable download speeds for 687MB files globally.
   - Recommendation: Use GitHub releases; 687MB is well within limits. If download reliability is a concern, it can be migrated to a different host later without changing the manifest format.

2. **Sidecar Update Atomicity**
   - What we know: SQLite connections must be closed before file replacement. Windows file locking is strict.
   - What's unclear: Whether users will encounter issues if they trigger a sidecar update while actively browsing data.
   - Recommendation: Show a brief "Updating data..." dialog that prevents interaction during the file swap (close service -> replace file -> re-init service). The swap itself is fast (file rename, not download).

3. **Stale Placeholder Files in fist_data/**
   - What we know: `fist_data/pgp.db` and `fist_data/nli_crossref.db` are 0-byte placeholder files.
   - What's unclear: Whether any code references these paths.
   - Recommendation: Clean up the 0-byte stale files as part of this phase to avoid confusion. The real files are in `pgp_data/` and `nli_data/` respectively.

## File Inventory

### Files to Modify
| File | Change | Purpose |
|------|--------|---------|
| `build_app.bat` | Add `--add-data "pgp_data\pgp.db;pgp_data"` | Bundle pgp.db in desktop build |
| `GenizahSearchPro.spec` | Add pgp_data entry to datas list | PyInstaller spec (auto-generated but should match) |
| `gui_threads.py` | Add `SidecarUpdateThread` class | Background sidecar version check |
| `genizah_app.py` | Add sidecar update check on startup, notification handling, About screen data | UI integration for sidecar updates |
| `docs/guides/DEPLOYMENT_TECHNICAL.md` | Add pgp.db to sidecar docs, update scp commands | Web deployment instructions |

### Files to Create
| File | Purpose |
|------|---------|
| `tests/test_offline_verification.py` | Automated verification that all three sidecar code paths use only local SQLite |
| `sidecar-versions.json` | Manifest for GitHub release (template/docs, not committed) |

### Existing Files (Reference Only, No Changes)
| File | Relevance |
|------|-----------|
| `shared/document_service.py` | PgpService -- already fully local SQLite |
| `shared/fjms_service.py` | FjmsService -- already fully local SQLite |
| `shared/nli_crossref_service.py` | NliCrossrefService -- already fully local SQLite |
| `CompileScriptGenizah.iss` | Inno Setup -- no changes needed (recursive copy) |
| `deploy.sh` | Web deploy -- no code changes (just docs) |

## Sidecar Data Summary

| Sidecar | Directory | Size | Version | Contents |
|---------|-----------|------|---------|----------|
| pgp.db | pgp_data/ | 147 MB | v1.0.0 | 35,839 documents, 9,364 sources, 22,757 footnotes, 36,155 fragments |
| fjms_enrichment.db | fist_data/ | 687 MB | v4.0.0 | 390K domains, 48K joins, 500K catalog + 8 child tables, 733K bibliography, 78K catalog_refs |
| nli_crossref.db | nli_data/ | 248 MB | v1.2.0 | 815K NLI images, 141K Cambridge manifests, 28K Manchester LUNA, 453 JTS DPUL |
| **Total** | | **~1.08 GB** | | All bundled in both desktop and web |

## Sources

### Primary (HIGH confidence)
- `build_app.bat` (line 22-23): Existing `--add-data` pattern for fjms and nli sidecars
- `GenizahSearchPro.spec` (line 4): PyInstaller spec confirming data bundling
- `CompileScriptGenizah.iss` (line 60): Inno Setup recursive copy from dist
- `shared/document_service.py`: PgpService implementation, `_find_project_root()`, singleton pattern
- `shared/fjms_service.py`: FjmsService implementation (identical pattern)
- `shared/nli_crossref_service.py`: NliCrossrefService implementation (identical pattern)
- `gui_threads.py` (line 334-458): `UpdateCheckerThread` and `UpdateDownloaderThread`
- `genizah_app.py` (line 20263-20346): App update check flow and UI
- `deploy.sh`: Web server deployment script
- `docs/guides/DEPLOYMENT_TECHNICAL.md`: Full deployment documentation
- `.gitignore`: Confirms sidecars are excluded from git
- Verified dist directory: `dist/GenizahSearchPro/_internal/` structure confirmed

### Secondary (MEDIUM confidence)
- GitHub Releases API: 2GB per-asset limit (based on GitHub documentation)
- Inno Setup solid LZMA compression: Typically achieves 2:1 to 4:1 on SQLite databases

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - All tools/patterns already in use in the codebase
- Architecture: HIGH - Extending established patterns, not introducing new ones
- Pitfalls: HIGH - Identified from actual codebase investigation (file locking, path resolution, PyInstaller behavior all verified against existing code)

**Research date:** 2026-02-18
**Valid until:** 2026-03-18 (stable patterns, no external dependencies changing)
