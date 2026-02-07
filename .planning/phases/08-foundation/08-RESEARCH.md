# Phase 8: Foundation - Research

**Researched:** 2026-02-07
**Domain:** Shared service layer extraction (Python module refactoring)
**Confidence:** HIGH

## Summary

Phase 8 extracts `web/document_service.py` (508 lines, 12 functions) into a new `shared/` package at the project root so both the NiceGUI web app and PyQt6 desktop app can consume PGP data through the same service. A unified `shared/supabase_provider.py` replaces three redundant Supabase client initialization sites. A re-export shim at `web/document_service.py` ensures zero breakage to 5 existing web import sites (3 top-level, 2 lazy).

This is a pure refactoring operation. No new external dependencies. No schema changes. No new features. The risk is LOW because the re-export shim pattern is standard Python and all consumers continue importing from the same path. The only way to break the web app is a typo in the shim or a missing function in the re-export list.

**Primary recommendation:** Use the re-export shim approach (not updating 15+ import sites). Create `shared/supabase_provider.py` as a module-level singleton. Move document_service to `shared/` with one import line changed. Replace `web/document_service.py` with a re-export shim. Verify with existing tests + new smoke tests.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Verification approach: Both automated smoke tests AND a manual walkthrough checklist
- Automated tests should catch import breakage and runtime issues (Claude decides appropriate depth)
- Manual checklist covers: transcriptions load, metadata displays, tags work, joins display

### Claude's Discretion
- **Import path strategy**: Whether to use a re-export shim at `web/document_service.py` or update all 15+ import sites -- Claude picks the safest approach based on codebase analysis
- **Smoke test depth**: Import-only checks vs import + live data calls -- Claude decides based on what's practical
- **Commit strategy**: Single commit vs incremental commits -- Claude picks based on risk level
- **Package naming**: `shared/` vs `core/` vs `services/` -- Claude decides based on existing conventions

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| supabase-py | 2.27.2 | Cloud database client | Already installed, used by all Supabase consumers |
| Python | 3.11 | Runtime | Existing project runtime |

### Supporting

No new libraries required. This phase uses only existing dependencies.

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Module-level `get_client()` singleton | Dependency injection (pass client param) | DI requires changing 12 function signatures + all call sites; module singleton requires zero signature changes |
| Re-export shim | Update all 15+ import sites | Updating import sites touches 5 web files + test file; shim touches 1 file |
| `shared/` package name | `services/` or `core/` | `services/` conflicts with `web/services.py`; `core/` conflicts with `genizah_core.py`; `shared/` is unambiguous |

**Installation:**
```bash
# No new packages needed
```

## Architecture Patterns

### Recommended Project Structure

```
GenizahSearch/
  shared/                         # NEW PACKAGE (3 files)
    __init__.py                   # Package marker (empty or minimal)
    supabase_provider.py          # Unified client factory (~35 lines)
    document_service.py           # Moved from web/ (~510 lines, 1 import changed)
  web/
    document_service.py           # MODIFIED: becomes re-export shim (~20 lines)
    supabase_client.py            # MODIFIED: delegates get_client to shared (~10 lines changed)
    pages/
      browse.py                   # NO CHANGE (shim handles it)
      search.py                   # NO CHANGE (shim handles it)
    components/
      joins_panel.py              # NO CHANGE (shim handles it)
  tests/
    test_document_service.py      # MODIFIED: patch targets updated + new shim tests
```

### Pattern 1: Re-export Shim for Backward Compatibility

**What:** After moving a module, leave a thin shim at the old location that re-exports all public names.

**When to use:** When a module has many consumers (5+ import sites) and you want zero consumer changes.

**Example:**
```python
# web/document_service.py (re-export shim)
"""
Backward-compatibility shim.
All document_service functions have moved to shared.document_service.
This module re-exports them so existing web imports continue to work.
"""
from shared.document_service import (
    get_document_for_fragment,
    get_fragments_for_document,
    get_transcription_for_document,
    get_document_metadata,
    parse_transcription_sections,
    get_section_for_page,
    get_sources_for_document,
    get_all_sources_for_fragment,
    get_editions_for_document,
    get_translations_for_document,
    get_sys_ids_with_transcriptions,
    get_fragments_by_tag,
)
```

### Pattern 2: Module-Level Singleton Client Provider

**What:** A single module provides the Supabase client via a `get_client()` function backed by a module-level `_client` variable.

**When to use:** When multiple modules need the same external client and all run synchronously in a single process.

**Example:**
```python
# shared/supabase_provider.py
import os
from typing import Optional
from supabase import create_client, Client

SUPABASE_URL = os.environ.get(
    'SUPABASE_URL',
    'https://ylcpglwxompwjcufdemz.supabase.co'
)
SUPABASE_ANON_KEY = os.environ.get(
    'SUPABASE_ANON_KEY',
    'eyJ...'  # Default anon key (same value currently hardcoded in 3 places)
)

_client: Optional[Client] = None

def get_client() -> Client:
    """Get or create the Supabase client singleton."""
    global _client
    if _client is None:
        if not SUPABASE_ANON_KEY:
            raise ValueError("SUPABASE_ANON_KEY not set")
        _client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    return _client

def reset_client():
    """Reset the client singleton (for testing or re-auth)."""
    global _client
    _client = None
```

### Pattern 3: Client Delegation in web/supabase_client.py

**What:** The web supabase client imports `get_client` from the shared provider instead of defining its own singleton. It re-exports the function so all existing `from web.supabase_client import get_client` calls continue working.

**When to use:** When consolidating duplicate singletons into a shared location.

**Example:**
```python
# web/supabase_client.py (modified top section only)
# BEFORE:
# SUPABASE_URL = os.environ.get('SUPABASE_URL', '...')
# SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY', '...')
# _client: Optional[Client] = None
# def get_client() -> Client: ...

# AFTER:
from shared.supabase_provider import get_client, reset_client, SUPABASE_URL, SUPABASE_ANON_KEY
# ... rest of file (auth, lists, corrections functions) unchanged
```

### Anti-Patterns to Avoid

- **Dependency injection on all 12 functions:** Adding `client` parameter to every function changes all signatures and all call sites. The module singleton is simpler and equally testable via `@patch`.
- **Moving web/supabase_client.py to shared/:** This 1,190-line file has web-specific auth concerns (OAuth, session management). Only `get_client()` and config constants are truly shared.
- **Using `from shared.document_service import *` in the shim:** Explicit re-exports are safer than wildcard. If shared module adds internal helpers later, they will not leak through the shim.
- **Updating all import sites instead of using a shim:** Higher risk (5 files to modify), higher review burden, no benefit. The shim approach is the standard Python migration pattern.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Supabase client management | Custom connection pooling or factory pattern | Module-level singleton (existing pattern from web/supabase_client.py) | supabase-py handles connection internally; a simple singleton is all that is needed |
| Import path migration | sys.path manipulation or importlib hacks | Re-export shim (standard Python pattern) | sys.path hacks are fragile, break with packaging, and are already a tech debt source in supabase_corrections_client.py |

## Common Pitfalls

### Pitfall 1: Missing Function in Re-export Shim

**What goes wrong:** The shim at `web/document_service.py` omits one of the 12 functions. A web page that imports that function gets `ImportError`.

**Why it happens:** The function list is typed manually. Easy to miss one, especially if functions were added after the original 4 (which grew to 12 during v1).

**How to avoid:** Enumerate all 12 functions from the current `web/document_service.py` using grep. Cross-check the shim re-export list against the actual function list. Add a smoke test that verifies all 12 are importable from `web.document_service`.

**Warning signs:** `ImportError` at web app startup. The app will fail to start entirely.

**The 12 functions that MUST be re-exported:**
1. `get_document_for_fragment`
2. `get_fragments_for_document`
3. `get_transcription_for_document`
4. `get_document_metadata`
5. `parse_transcription_sections`
6. `get_section_for_page`
7. `get_sources_for_document`
8. `get_all_sources_for_fragment`
9. `get_editions_for_document`
10. `get_translations_for_document`
11. `get_sys_ids_with_transcriptions`
12. `get_fragments_by_tag`

### Pitfall 2: Test Patch Targets Broken After Move

**What goes wrong:** Existing tests in `tests/test_document_service.py` use `@patch('web.document_service.get_client')`. After extraction, the real `get_client` is in `shared.document_service`, but the import goes through the shim. Patching the wrong target means the mock does not take effect -- real Supabase calls happen during tests.

**Why it happens:** Python's `@patch` patches the name in the module where it is USED, not where it is DEFINED. After extraction, `shared.document_service` uses `get_client` from `shared.supabase_provider`. The web shim re-imports the function objects. Patching `web.document_service.get_client` patches the shim's binding, but the actual function in `shared.document_service` still references the real `get_client`.

**How to avoid:** Update patch targets to `shared.document_service.get_client`. Existing tests continue importing from `web.document_service` (functions are the same objects), but the patch must target where `get_client` is looked up at call time, which is now `shared.document_service`.

**Warning signs:** Tests that previously passed now make real network calls, or tests that mock data now return unexpected results.

### Pitfall 3: web/supabase_client.py Auth Breaks After Client Delegation

**What goes wrong:** After `web/supabase_client.py` delegates `get_client()` to `shared.supabase_provider`, the auth functions (`sign_in`, `sign_up`, etc.) still work because they call `get_client()` which now returns the shared singleton. But if `reset_client()` is called in the web module, it resets the web module's local reference, not the shared singleton. Auth state becomes inconsistent.

**Why it happens:** If `web/supabase_client.py` keeps its own `reset_client()` that sets a local `_client = None`, but `get_client()` now comes from shared, the reset has no effect.

**How to avoid:** Ensure `reset_client` is also imported from shared (or delegates to `shared.supabase_provider.reset_client()`). The web module should NOT have its own `_client` variable after delegation.

**Warning signs:** After calling `reset_client()`, the next `get_client()` returns the old (stale) client instance instead of creating a new one.

### Pitfall 4: Circular Import Between shared and web

**What goes wrong:** If `shared/supabase_provider.py` accidentally imports anything from `web/`, or if `shared/document_service.py` imports from `web/supabase_client.py` instead of `shared/supabase_provider.py`, a circular import occurs and the app fails to start.

**Why it happens:** Copy-paste error when creating the shared module. The original import line is `from web.supabase_client import get_client` -- if this is not changed in the shared copy, it creates `shared -> web -> shared` cycles.

**How to avoid:** The shared package must depend on NOTHING internal except itself. The dependency graph is a clean DAG: `shared/supabase_provider` (no deps) -> `shared/document_service` -> `web/document_service` (shim). Verify with: `python -c "from shared.document_service import get_document_for_fragment; print('OK')"` before wiring web.

### Pitfall 5: Desktop Import Fails Due to Python Path

**What goes wrong:** Desktop app (`genizah_app.py`) tries `from shared.document_service import ...` and gets `ModuleNotFoundError` because `shared/` is not on `sys.path`.

**Why it happens:** The desktop app is typically launched as `python genizah_app.py` from the project root, which puts the root on `sys.path`. But if launched from a different working directory (e.g., from a shortcut or frozen executable), the root may not be on the path.

**How to avoid:** Verify that `genizah_app.py` already handles its working directory (check if it sets `sys.path` or uses `__file__` relative paths). Since `genizah_app.py` already imports `genizah_core` (which is at the same level), and that works, `from shared.X` will also work from the same launch context. Test by running `python -c "import sys; sys.path.insert(0, '.'); from shared.document_service import get_document_for_fragment; print('OK')"` from the project root.

## Code Examples

### Complete shared/supabase_provider.py

```python
# shared/supabase_provider.py
"""
Unified Supabase client provider for GenizahSearch.

Single source of truth for Supabase connection configuration.
Both web and desktop apps obtain their PGP data client from here.
"""

import os
from typing import Optional
from supabase import create_client, Client

# Single source of truth for Supabase configuration
SUPABASE_URL = os.environ.get(
    'SUPABASE_URL',
    'https://ylcpglwxompwjcufdemz.supabase.co'
)
SUPABASE_ANON_KEY = os.environ.get(
    'SUPABASE_ANON_KEY',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlsY3BnbHd4b21wd2pjdWZkZW16Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk3Njc0NzUsImV4cCI6MjA4NTM0MzQ3NX0.xKzlyKrBV0MxADYHqD0lyyymoVxTX91hyI4T6TGchpE'
)

_client: Optional[Client] = None


def get_client() -> Client:
    """Get or create the Supabase client singleton.

    This client uses the anon key and is suitable for:
    - Reading public data (documents, fragments, sources)
    - Operations that rely on RLS policies
    """
    global _client
    if _client is None:
        if not SUPABASE_ANON_KEY:
            raise ValueError("SUPABASE_ANON_KEY not set")
        _client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    return _client


def reset_client():
    """Reset the client singleton (for testing or re-auth)."""
    global _client
    _client = None
```

### Complete web/document_service.py (shim after extraction)

```python
# web/document_service.py
"""
Backward-compatibility shim.

All document_service functions have moved to shared.document_service.
This module re-exports them so existing web imports continue to work.
"""

from shared.document_service import (
    get_document_for_fragment,
    get_fragments_for_document,
    get_transcription_for_document,
    get_document_metadata,
    parse_transcription_sections,
    get_section_for_page,
    get_sources_for_document,
    get_all_sources_for_fragment,
    get_editions_for_document,
    get_translations_for_document,
    get_sys_ids_with_transcriptions,
    get_fragments_by_tag,
)
```

### Minimal change in shared/document_service.py

```python
# shared/document_service.py (only line 18 changes from the web/ version)

# BEFORE (web/document_service.py line 18):
# from web.supabase_client import get_client

# AFTER:
from shared.supabase_provider import get_client

# All 12 functions remain IDENTICAL. No signature changes. No logic changes.
```

### web/supabase_client.py modification (top section only)

```python
# web/supabase_client.py - MODIFIED top section

# REMOVE these lines (26-49):
# SUPABASE_URL = os.environ.get('SUPABASE_URL', '...')
# SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY', '...')
# _client: Optional[Client] = None
# def get_client() -> Client: ...
# def reset_client(): ...

# REPLACE WITH:
from shared.supabase_provider import get_client, reset_client, SUPABASE_URL, SUPABASE_ANON_KEY

# Everything else (lines 52-1190) stays EXACTLY the same.
# All auth, lists, corrections, comments, discoveries, joins functions untouched.
```

### Test patch target update

```python
# tests/test_document_service.py - MODIFIED patch targets

# BEFORE:
@patch('web.document_service.get_client')

# AFTER:
@patch('shared.document_service.get_client')

# The test imports can stay as-is (from web.document_service import X)
# because the shim re-exports the same function objects.
# But the PATCH target must point to where get_client is used at call time.
```

### Smoke test examples

```python
# tests/test_shared_service.py - NEW smoke tests

def test_shared_provider_import():
    """Verify shared.supabase_provider is importable."""
    from shared.supabase_provider import get_client, reset_client, SUPABASE_URL, SUPABASE_ANON_KEY
    assert SUPABASE_URL is not None
    assert SUPABASE_ANON_KEY is not None

def test_shared_document_service_import():
    """Verify all 12 functions importable from shared.document_service."""
    from shared.document_service import (
        get_document_for_fragment,
        get_fragments_for_document,
        get_transcription_for_document,
        get_document_metadata,
        parse_transcription_sections,
        get_section_for_page,
        get_sources_for_document,
        get_all_sources_for_fragment,
        get_editions_for_document,
        get_translations_for_document,
        get_sys_ids_with_transcriptions,
        get_fragments_by_tag,
    )

def test_web_shim_reexports():
    """Verify all 12 functions importable from web.document_service (shim)."""
    from web.document_service import (
        get_document_for_fragment,
        get_fragments_for_document,
        get_transcription_for_document,
        get_document_metadata,
        parse_transcription_sections,
        get_section_for_page,
        get_sources_for_document,
        get_all_sources_for_fragment,
        get_editions_for_document,
        get_translations_for_document,
        get_sys_ids_with_transcriptions,
        get_fragments_by_tag,
    )

def test_web_supabase_client_still_exports_get_client():
    """Verify web.supabase_client still exports get_client (used by 20+ files)."""
    from web.supabase_client import get_client
    assert callable(get_client)

def test_shared_and_web_get_client_are_same():
    """The shared provider and web shim should return the same singleton."""
    from shared.supabase_provider import get_client as shared_get_client
    from web.supabase_client import get_client as web_get_client
    # Both should reference the same function
    assert shared_get_client is web_get_client
```

## Detailed Analysis: Import Sites and Impact

### web/document_service.py consumers (5 import sites, all handled by shim)

| File | Line | Import Type | Functions Used |
|------|------|-------------|----------------|
| `web/pages/browse.py` | 25 | Top-level | `get_document_for_fragment`, `get_section_for_page`, `get_sources_for_document`, `get_all_sources_for_fragment` |
| `web/pages/browse.py` | 1891, 2051 | Lazy (inside function) | `get_transcription_for_document` |
| `web/pages/search.py` | 19 | Top-level | `get_sys_ids_with_transcriptions`, `get_all_sources_for_fragment`, `get_document_for_fragment`, `get_section_for_page`, `get_fragments_by_tag` |
| `web/components/joins_panel.py` | 106 | Lazy (inside function) | `get_document_for_fragment`, `get_fragments_for_document` |
| `tests/test_document_service.py` | multiple | Lazy (inside test methods) | All functions |

**All 5 sites import from `web.document_service`.** The re-export shim makes all of these work with zero changes.

### web/supabase_client.py consumers (20+ import sites, NOT affected by this phase)

These files import from `web.supabase_client` directly. They are unaffected by the document_service extraction because `web/supabase_client.py` stays in place -- only its internal `get_client()` implementation changes to delegate to shared.

Key consumers: `web/auth_state.py`, `web/pages/browse.py`, `web/pages/search.py`, `web/pages/discoveries.py`, `web/pages/admin.py`, `web/pages/corrections.py`, `web/pages/profile.py`, `web/components/joins_panel.py`, `web/components/version_selector.py`, `web/components/text_editor.py`, `web/components/notes_display.py`, `web/components/comment_dialog.py`, `web/user_lists.py`, `web/main.py`, `web/api.py`.

## INFRA-02: API Reshaping During Extraction

The requirement mentions "fix TODO, clean naming." Based on codebase analysis:

**TODO to fix:** Line 268 in `web/document_service.py`:
```python
# TODO: Enhance for multi-fragment documents in future
```
This is inside `get_section_for_page()`. For pages beyond 2, it returns the full transcription as a fallback. This TODO is about future enhancement, not a bug. It can be left as-is or annotated more clearly during extraction.

**Naming cleanup opportunities (Claude's discretion):**
- The functions have consistent naming already (`get_X_for_Y` pattern)
- No renaming is recommended during this phase to avoid breaking the shim. Any renaming would require updating all call sites, defeating the purpose of the shim.
- If the user wants naming changes, they should be done in a future phase after all consumers point to `shared.document_service` directly.

**Recommendation:** Keep the API surface identical during extraction. The "reshape" can be deferred to after desktop integration, when there is a clear picture of what both apps need. Changing function names now breaks the re-export shim pattern and requires touching all consumer files.

## Commit Strategy Recommendation

**Incremental commits (2 commits):**

1. **Commit 1: Create shared/ package** -- `shared/__init__.py`, `shared/supabase_provider.py`, `shared/document_service.py` (copy with 1 import changed). At this point, nothing depends on shared/ yet. Safe to commit.

2. **Commit 2: Wire everything + shim + tests** -- Replace `web/document_service.py` with shim, modify `web/supabase_client.py` to delegate, update test patch targets, add new smoke tests. This is the "cut-over" commit.

**Rationale:** If commit 2 causes issues, commit 1 is harmless (new files, nothing references them). Rollback is simple: revert commit 2. With a single commit, rollback loses the new files too.

## Manual Verification Checklist

After all automated tests pass, manually verify:

1. **Start web app:** `python -m web.main` -- should start without errors on port 8080/8081
2. **Browse a manuscript with PGP transcription:** Navigate to a fragment with a known PGP link (e.g., T-S 8J5.11). Transcription should display.
3. **Check metadata display:** PGP metadata (type, tags, dates, description) should appear in the browse panel.
4. **Check tag search:** Search by a PGP tag (e.g., "letter"). Results should appear.
5. **Check joins display:** Navigate to a multi-fragment document. Joined fragments should display correctly.
6. **Verify desktop import path:** `python -c "from shared.document_service import get_document_for_fragment; print('Desktop import OK')"`

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| 3 separate Supabase client singletons | 1 shared provider + app-specific auth | This phase | Eliminates config duplication |
| document_service in web/ only | document_service in shared/ with web shim | This phase | Desktop can access PGP data |

**Not deprecated/outdated:**
- The supabase-py 2.27.2 sync client is current and correct for this use case
- The module-level singleton pattern is the standard approach in this codebase (used by web/supabase_client.py already)

## Open Questions

1. **Should `lists_sync.py` also consume shared provider?**
   - What we know: `lists_sync.py` (line 29-30) has its own duplicate config. It uses a class-level `_client` but can accept an external client.
   - What's unclear: Whether unifying it now is in scope or should wait.
   - Recommendation: Include it as an optional cleanup step in Plan 08-01 (change 2 lines of config import). Low risk, eliminates the third duplicate. But it is not required by INFRA-01/02/03.

2. **Should `supabase_corrections_client.py` consume shared provider constants?**
   - What we know: It has hardcoded SUPABASE_URL and SUPABASE_ANON_KEY (same values, line 63-64). It also has `.env` loading fallback.
   - What's unclear: Whether changing its config source could affect desktop auth (it manages auth independently).
   - Recommendation: Defer to Phase 10 (Desktop PGP Core). For Phase 8, the desktop only needs to prove it can `import shared.document_service`. Actually wiring it into the desktop app is Phase 10's job.

## Sources

### Primary (HIGH confidence)
- Direct codebase analysis of all files involved (no external sources needed for internal refactoring)
- `web/document_service.py` -- 508 lines, 12 public functions
- `web/supabase_client.py` -- 1,190 lines, singleton get_client() + auth + CRUD
- `supabase_corrections_client.py` -- desktop Supabase client with own singleton
- `tests/test_document_service.py` -- 306 lines, 15 tests with `@patch` mocking
- `.planning/research/ARCHITECTURE.md` -- prior v5.6.0 research on shared service extraction

### Secondary (MEDIUM confidence)
- `.planning/research/PITFALLS.md` -- prior v5.6.0 pitfall analysis

### Tertiary (LOW confidence)
- None. This phase requires no external research.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new dependencies, verified existing versions
- Architecture: HIGH -- direct codebase analysis, follows existing patterns, prior research validated
- Pitfalls: HIGH -- all pitfalls derived from direct analysis of import chains and module structure
- Re-export shim: HIGH -- standard Python pattern, zero consumer changes, tested in many projects

**Research date:** 2026-02-07
**Valid until:** Indefinite (internal refactoring, no external dependencies to go stale)
