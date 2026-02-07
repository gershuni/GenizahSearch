# Architecture Patterns: Shared Service Layer Extraction

**Domain:** Cairo Genizah manuscript research platform
**Researched:** February 7, 2026
**Focus:** Extracting document_service to a shared location consumable by both NiceGUI web and PyQt6 desktop apps
**Confidence:** HIGH (based entirely on direct codebase analysis -- no external sources needed)

---

## Executive Summary

The PGP document service (`web/document_service.py`, 507 lines) currently lives inside the `web/` package and depends on `web/supabase_client.py`'s `get_client()` function. The desktop app has its own entirely separate Supabase client (`supabase_corrections_client.py`, 1834 lines) that creates its own `create_client()` instance with the same URL and anon key. Both apps also have a third Supabase-consuming module -- `lists_sync.py` -- that creates yet another independent client instance.

The fundamental architectural challenge is **not async vs sync** (both apps use the synchronous `supabase-py` client). It is **client instance management**: three separate modules each create their own Supabase `Client` singleton. Extracting the shared service requires a unified client provider that all three can consume.

The recommended approach is a **two-phase extraction**:
1. Create a `shared/` package at the project root with `supabase_provider.py` (client factory) and `document_service.py` (moved from web/)
2. Wire both apps to import from `shared/` instead of their current locations

This is a **refactoring-heavy, risk-low** change. No new external dependencies. No schema changes. No API changes. The existing tests for `web.document_service` can be adapted with minimal changes.

---

## Current Architecture: What Exists Today

### Three Independent Supabase Client Instances

```
Web App (NiceGUI)                Desktop App (PyQt6)
==================               ====================
web/supabase_client.py           supabase_corrections_client.py
  - _client singleton              - self._client instance
  - get_client() function           - _get_client() method
  - SUPABASE_URL (hardcoded)        - SUPABASE_URL (hardcoded, same value)
  - SUPABASE_ANON_KEY (hardcoded)   - SUPABASE_ANON_KEY (hardcoded, same value)
  - Auth: sign_up/in/out            - Auth: login/register/logout
  - Community: corrections,         - Community: same operations,
    comments, discoveries,            different API (returns dataclasses
    joins (returns dicts)             instead of dicts)
  - Lists: CRUD operations          [Lists handled separately]
       |                                    |
       v                                    |
web/document_service.py            lists_sync.py (3rd client!)
  - get_document_for_fragment()       - ANOTHER create_client()
  - get_fragments_for_document()      - SUPABASE_URL (hardcoded, same)
  - get_transcription_for_document()  - SUPABASE_ANON_KEY (hardcoded, same)
  - get_sources_for_document()
  - get_sys_ids_with_transcriptions()
  - get_fragments_by_tag()
  - parse_transcription_sections()
  - get_section_for_page()
```

### Key Observations

1. **All synchronous.** The supabase-py library used is the sync version. The web app calls document_service functions directly (no `await`). NiceGUI uses `run.io_bound()` for heavy operations in search.py, but document_service calls are made synchronously from UI event handlers.

2. **Same credentials, three places.** The exact same SUPABASE_URL and SUPABASE_ANON_KEY string are hardcoded in:
   - `web/supabase_client.py` (line 26-27)
   - `supabase_corrections_client.py` (line 63-64)
   - `lists_sync.py` (line 29-30)

3. **document_service has exactly one coupling point.** The only import is `from web.supabase_client import get_client` (line 18). Every function calls `get_client()` to obtain a Supabase Client, then uses it for table queries. The service itself is stateless -- no singletons, no class instances, just pure functions.

4. **Desktop app has NO PGP access today.** The desktop app (`genizah_app.py`) imports only `corrections_client.get_corrections_client()` (line 64), which delegates to `SupabaseCorrectionsClient`. There is zero PGP/document functionality in the desktop path.

5. **Auth sessions differ between apps.** Web app manages auth via browser cookies + Supabase session in `web/supabase_client.py`. Desktop app manages auth via keyring + local credentials file in `supabase_corrections_client.py`. PGP document queries do NOT require auth (anon key is sufficient for read access via RLS).

### Current Consumers of document_service

| Consumer | Import | Functions Used |
|----------|--------|----------------|
| `web/pages/browse.py` | `from web.document_service import ...` | `get_document_for_fragment`, `get_section_for_page`, `get_sources_for_document`, `get_all_sources_for_fragment` |
| `web/pages/search.py` | `from web.document_service import ...` | `get_sys_ids_with_transcriptions`, `get_all_sources_for_fragment`, `get_document_for_fragment`, `get_section_for_page`, `get_fragments_by_tag` |
| `tests/test_document_service.py` | `from web.document_service import ...` | All functions, via `@patch('web.document_service.get_client')` |

### Current Consumers of supabase_client (web)

20+ files import from `web.supabase_client`. The critical ones:

| Consumer | Key Imports |
|----------|-------------|
| `web/document_service.py` | `get_client` |
| `web/auth_state.py` | Auth functions |
| `web/pages/browse.py` | `create_correction`, `update_correction`, `get_corrections` |
| `web/pages/discoveries.py` | Discovery CRUD |
| `web/components/joins_panel.py` | `get_fragment_joins`, `create_fragment_join`, `get_client` |
| `web/user_lists.py` | List CRUD functions |
| `web/main.py` | `set_session_from_url`, `get_profile` |
| `web/api.py` | `set_session_from_url`, `get_profile` |

---

## Recommended Architecture: Shared Service Layer

### Module Structure

```
GenizahSearch/
  shared/                          <-- NEW PACKAGE
    __init__.py                    <-- Package marker
    supabase_provider.py           <-- Unified client factory
    document_service.py            <-- Moved from web/document_service.py
  web/
    supabase_client.py             <-- MODIFIED: delegates to shared.supabase_provider
    document_service.py            <-- MODIFIED: re-export shim for backward compatibility
    pages/
      browse.py                    <-- No change needed (re-export shim handles it)
      search.py                    <-- No change needed
  supabase_corrections_client.py   <-- MODIFIED: uses shared.supabase_provider
  lists_sync.py                    <-- MODIFIED: uses shared.supabase_provider
  genizah_app.py                   <-- MODIFIED: can now import shared.document_service
```

### Why `shared/` at the Project Root

1. **Both `web/` and root-level modules need it.** Placing it inside `web/` means root-level modules (like `supabase_corrections_client.py`) need `sys.path` hacks to reach it. Placing it at root means both can do `from shared.X import Y`.

2. **`genizah_core.py` is the precedent.** The project already has a shared module at the root (`genizah_core.py`) that both apps import. `shared/` follows this pattern but as a package (multiple files).

3. **No restructuring of `web/`.** The web package stays intact. A re-export shim in `web/document_service.py` means zero changes to web page imports.

4. **Clear naming convention.** `shared/` is self-documenting. It contains code shared between web and desktop.

### Alternative Considered: `services/` Package

A `services/` package was considered but rejected because:
- Ambiguous with `web/services.py` (which wraps genizah_core for web)
- `shared/` is more explicit about the cross-app purpose

### Alternative Considered: Move Everything to `genizah_core.py`

Rejected because:
- `genizah_core.py` is already 7K lines and focused on search/metadata
- PGP document access is a Supabase concern, not a search concern
- Would create a dependency from the core engine on Supabase (currently independent)

---

## Component Design

### 1. `shared/supabase_provider.py` -- Unified Client Factory

```python
"""
Unified Supabase client provider for GenizahSearch.

All Supabase consumers (web, desktop, scripts) should obtain their
client from this module instead of creating their own instances.
"""

import os
from typing import Optional
from supabase import create_client, Client

# Configuration (single source of truth)
SUPABASE_URL = os.environ.get(
    'SUPABASE_URL',
    'https://ylcpglwxompwjcufdemz.supabase.co'
)
SUPABASE_ANON_KEY = os.environ.get(
    'SUPABASE_ANON_KEY',
    'eyJ...'  # Default anon key
)

# Module-level singleton
_client: Optional[Client] = None


def get_client() -> Client:
    """Get or create the Supabase client singleton.

    This client uses the anon key and is suitable for:
    - Reading public data (documents, fragments, sources)
    - Operations that rely on RLS policies

    For authenticated operations, the caller should set the session
    on this client instance.
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

**Critical design decision: single client instance or multiple?**

The supabase-py `Client` object holds auth state (session tokens). If web and desktop share the same singleton, auth operations in one app could affect the other. However, since these apps run in **separate processes** (web server vs desktop executable), a module-level singleton is safe -- each process gets its own instance.

For the desktop app's `SupabaseCorrectionsClient`, which manages auth separately (keyring, saved credentials), the recommendation is:
- Use `shared.supabase_provider.get_client()` for **read-only PGP queries** (no auth needed)
- Keep the desktop's own `_client` instance for **authenticated community operations** (corrections, comments, etc.)

This means the desktop app will have **two** Supabase client references, but this is intentional and correct:
- One for anonymous PGP reads (from shared provider)
- One for authenticated writes (from SupabaseCorrectionsClient)

### 2. `shared/document_service.py` -- Extracted PGP Service

```python
"""
Document Service for PGP document-fragment relationships.

Shared between web and desktop apps.
Provides read-only access to PGP documents, fragments, and sources.
"""

import re
import json
from typing import Optional, List, Dict, Any, Set
from shared.supabase_provider import get_client

# All existing functions from web/document_service.py, unchanged:
# - get_document_for_fragment(sys_id, page_num) -> dict | None
# - get_fragments_for_document(pgpid) -> list[dict]
# - get_transcription_for_document(pgpid) -> str | None
# - get_document_metadata(pgpid) -> dict | None
# - parse_transcription_sections(transcription) -> dict
# - get_section_for_page(transcription, page_num) -> str | None
# - get_sources_for_document(pgpid) -> list[dict]
# - get_all_sources_for_fragment(sys_id) -> list[dict]
# - get_editions_for_document(pgpid) -> list[dict]
# - get_translations_for_document(pgpid) -> list[dict]
# - get_sys_ids_with_transcriptions(sys_ids) -> set[str]
# - get_fragments_by_tag(tag) -> list[dict]
```

The ONLY change: line 18 changes from `from web.supabase_client import get_client` to `from shared.supabase_provider import get_client`.

### 3. `web/document_service.py` -- Re-export Shim

```python
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

This means **zero changes** to any file in `web/pages/` or `web/components/`. Their `from web.document_service import X` continues to work.

### 4. `web/supabase_client.py` -- Modified to Use Shared Provider

The web supabase client keeps all its auth, community, and list functions unchanged. Only the `get_client()` function delegates:

```python
# BEFORE (lines 26-43):
SUPABASE_URL = os.environ.get('SUPABASE_URL', '...')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY', '...')
_client: Optional[Client] = None

def get_client() -> Client:
    global _client
    if _client is None:
        _client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    return _client

# AFTER:
from shared.supabase_provider import get_client  # Re-exported
from shared.supabase_provider import SUPABASE_URL, SUPABASE_ANON_KEY
# ... rest of file unchanged
```

**Important:** `web/supabase_client.py` also exports `get_client` by name, and many web modules import it: `from web.supabase_client import get_client`. By re-exporting from shared, these imports continue to work.

### 5. Desktop Integration Path

The desktop app (`genizah_app.py`) can import PGP functions directly:

```python
# In genizah_app.py or a new desktop PGP panel:
from shared.document_service import (
    get_document_for_fragment,
    get_sources_for_document,
    get_section_for_page,
)
```

No `sys.path` manipulation needed because `shared/` is at the project root alongside `genizah_app.py`.

---

## Data Flow After Extraction

### Web App Data Flow (unchanged behavior)

```
web/pages/browse.py
    |
    |-- from web.document_service import get_document_for_fragment
    |   (re-export shim)
    |       |
    |       v
    |   shared/document_service.py
    |       |
    |       |-- from shared.supabase_provider import get_client
    |       |       |
    |       |       v
    |       |   Supabase Client (singleton, same instance as web auth)
    |       |
    |       v
    |   Supabase tables: documents, document_fragments, document_sources
    |
    |-- from web.supabase_client import create_correction
    |   (uses same shared.supabase_provider.get_client under the hood)
    |       |
    |       v
    |   Supabase tables: corrections, comments, etc.
```

### Desktop App Data Flow (new capability)

```
genizah_app.py
    |
    |-- from shared.document_service import get_document_for_fragment
    |       |
    |       v
    |   shared/document_service.py
    |       |
    |       |-- from shared.supabase_provider import get_client
    |       |       |
    |       |       v
    |       |   Supabase Client (singleton, process-level, anon key only)
    |       |
    |       v
    |   Supabase tables: documents, document_fragments, document_sources
    |
    |-- from corrections_client import get_corrections_client
    |   (SupabaseCorrectionsClient with its own auth'd client)
    |       |
    |       v
    |   Supabase tables: corrections, comments, discoveries, joins
```

---

## Dependency Graph (Import Hierarchy)

```
                  shared/
                  supabase_provider.py    <-- FOUNDATION (no internal deps)
                  /         |         \
                 /          |          \
                v           v           v
     shared/           web/             supabase_
     document_         supabase_        corrections_
     service.py        client.py        client.py
         |                |
         v                v
     web/             web/pages/*
     document_         web/components/*
     service.py
     (re-export)
         |
         v
     web/pages/browse.py
     web/pages/search.py
```

**No circular dependencies.** The dependency graph is a clean DAG:
- `shared/supabase_provider.py` depends on nothing internal
- `shared/document_service.py` depends only on `shared/supabase_provider`
- `web/document_service.py` (shim) depends only on `shared/document_service`
- `web/supabase_client.py` depends only on `shared/supabase_provider`
- No module depends on both `web/supabase_client` and `shared/document_service` directly (the shim handles indirection)

---

## What Changes vs What Stays

### Files to CREATE (2 new files)

| File | Lines | Purpose |
|------|-------|---------|
| `shared/__init__.py` | 1 | Package marker |
| `shared/supabase_provider.py` | ~40 | Unified client factory |

### Files to MOVE (1 file, with shim left behind)

| From | To | Notes |
|------|-----|-------|
| `web/document_service.py` (507 lines) | `shared/document_service.py` | Change one import line |

### Files to MODIFY (3 files, minimal changes)

| File | Change | Scope |
|------|--------|-------|
| `web/document_service.py` | Replace with re-export shim | ~15 lines total |
| `web/supabase_client.py` | Replace `get_client()`/config with import from shared | ~10 lines changed |
| `supabase_corrections_client.py` | Import URL/key from shared instead of hardcoding | ~5 lines changed |

### Files with ZERO changes

| File | Why No Change |
|------|---------------|
| `web/pages/browse.py` | Re-export shim handles it |
| `web/pages/search.py` | Re-export shim handles it |
| `web/components/*.py` | Import from `web.supabase_client` unchanged |
| `web/auth_state.py` | Imports from `web.supabase_client` unchanged |
| `web/main.py` | Imports from `web.supabase_client` unchanged |
| `genizah_core.py` | No Supabase dependency |
| `genizah_app.py` | Modified ONLY when adding PGP features (later phase) |
| `corrections_client.py` | Delegates to `supabase_corrections_client` unchanged |

---

## Handling Auth State: The Nuance

### Problem

The Supabase `Client` object holds auth state in its `auth` attribute. When a user signs in via `client.auth.sign_in_with_password()`, the client stores access/refresh tokens. All subsequent queries from that client are authenticated.

If both `web/supabase_client.py` (auth management) and `shared/document_service.py` (PGP queries) use the same `get_client()` singleton, then:
- PGP queries will be authenticated when a user is logged in
- PGP queries will be unauthenticated when no user is logged in
- **This is fine** because the `documents`, `document_fragments`, and `document_sources` tables have RLS policies allowing anonymous reads

### Web App (no issue)

The web app already uses a single client instance for everything (auth + data). Sharing the singleton from `shared.supabase_provider` changes nothing.

### Desktop App (intentional separation)

The desktop app's `SupabaseCorrectionsClient` creates its own client and manages auth independently (keyring, saved credentials). For PGP reads, the desktop app will use `shared.supabase_provider.get_client()` which returns a **separate** singleton (same process, but the corrections client's instance is stored as `self._client`, not in the shared module's `_client`).

This means:
- Desktop PGP queries use the shared singleton (anon key, no auth session)
- Desktop community operations use the corrections client's instance (with auth)
- These are two separate `Client` objects in the same process
- **This is correct** -- PGP data is public, community operations need auth

### Future Convergence (Optional, Not Required for This Milestone)

Eventually, `supabase_corrections_client.py` could be refactored to use `shared.supabase_provider.get_client()` for its underlying client and add auth on top. This would reduce to one client per process. But this is a larger refactor that should be deferred -- it works correctly with two instances.

---

## Build Order

### Phase 1: Create shared/ Package (Foundation)

**Goal:** Establish the shared package without breaking anything.

1. Create `shared/__init__.py`
2. Create `shared/supabase_provider.py` -- extract `get_client()` and config constants
3. **Verify:** Both `from shared.supabase_provider import get_client` and `from web.supabase_client import get_client` work

**Risk:** LOW. Adding a new package. Nothing depends on it yet.

### Phase 2: Extract document_service (Core Migration)

**Goal:** Move PGP service to shared/ with backward-compatible shim.

1. Copy `web/document_service.py` to `shared/document_service.py`
2. Change the import in `shared/document_service.py`: `from shared.supabase_provider import get_client`
3. Replace `web/document_service.py` with a re-export shim
4. **Verify:** All existing web pages work unchanged. Run `tests/test_document_service.py`.

**Risk:** LOW. The re-export shim preserves all import paths. If anything breaks, it is a typo in the shim.

### Phase 3: Wire web/supabase_client.py to Shared Provider

**Goal:** Eliminate duplicated config constants.

1. Modify `web/supabase_client.py` to import `get_client`, `SUPABASE_URL`, `SUPABASE_ANON_KEY` from `shared.supabase_provider`
2. Remove the duplicated constant definitions and local `_client` singleton
3. **Verify:** All web auth, community features, lists continue to work

**Risk:** MEDIUM. The web supabase_client re-exports `get_client` and many web modules import it. Need to verify the re-export works. However, `web/supabase_client.py` has ~50 other functions that are unaffected.

### Phase 4: Wire Desktop to Shared Provider (Config Convergence)

**Goal:** Desktop uses shared config constants, retains its own auth client.

1. Modify `supabase_corrections_client.py` to import `SUPABASE_URL`, `SUPABASE_ANON_KEY` from `shared.supabase_provider`
2. Remove hardcoded config from `supabase_corrections_client.py`
3. Optionally modify `lists_sync.py` similarly
4. **Verify:** Desktop login/community features still work

**Risk:** LOW. Only changing where constants come from, not changing values.

### Phase 5: Add PGP Features to Desktop (New Functionality)

**Goal:** Desktop app can display PGP transcriptions and document metadata.

1. Import `shared.document_service` in desktop UI code
2. Add PGP transcription panel to desktop viewer
3. Add document metadata display
4. **Verify:** Desktop shows PGP data for linked fragments

**Risk:** MEDIUM. New UI work in PyQt6. Requires designing desktop-specific UI panels.

---

## Testing Strategy

### Existing Tests

`tests/test_document_service.py` patches `web.document_service.get_client`. After extraction:
- The patch target changes to `shared.document_service.get_client`
- OR keep patching `web.document_service.get_client` since the shim re-exports

Recommendation: Update patch targets to `shared.document_service.get_client` for clarity, but both work.

### New Tests Needed

| Test | What It Verifies |
|------|------------------|
| `test_shared_provider_singleton` | `get_client()` returns same instance on repeated calls |
| `test_shared_provider_reset` | `reset_client()` causes new instance creation |
| `test_web_shim_reexports` | All functions accessible via `from web.document_service import X` |
| `test_desktop_import_path` | `from shared.document_service import X` works from root |

---

## Anti-Patterns to Avoid

### Anti-Pattern 1: Passing Client as Parameter (Dependency Injection Overkill)

**What:** Refactoring every function to accept a `client` parameter: `get_document_for_fragment(sys_id, client=None)`
**Why bad:** 12 functions would need signature changes. All callers would need updating. The codebase is a single-project monolith, not a library -- DI is overhead.
**Instead:** Module-level singleton via `shared.supabase_provider.get_client()`. Tests use `@patch`.

### Anti-Pattern 2: Abstract Base Class for Supabase Client

**What:** Creating `class SupabaseClientBase(ABC)` with `class WebSupabaseClient(SupabaseClientBase)` and `class DesktopSupabaseClient(SupabaseClientBase)`
**Why bad:** Over-engineering. Both apps use the same `supabase-py` `Client` with the same API. The difference is auth management, not data access.
**Instead:** One `get_client()` for data access. Auth is a separate concern per app.

### Anti-Pattern 3: Making document_service a Class

**What:** Converting stateless functions to a `DocumentService` class with methods
**Why bad:** The current functions are stateless and pure (take input, call Supabase, return result). A class would add `self` to every signature for no benefit. No shared state to manage.
**Instead:** Keep as module-level functions. The module IS the namespace.

### Anti-Pattern 4: Moving web/supabase_client.py to shared/

**What:** Moving the entire 1190-line `web/supabase_client.py` to `shared/`
**Why bad:** The web supabase client has web-specific concerns (OAuth URL generation, session-from-URL handling, NiceGUI-specific auth state). The desktop doesn't need or want these. Only `get_client()` and the config constants are truly shared.
**Instead:** Extract only the minimal shared parts (`get_client`, config) to `shared/supabase_provider.py`. Keep web-specific functions in `web/supabase_client.py`.

### Anti-Pattern 5: Premature Desktop Client Unification

**What:** Merging `supabase_corrections_client.py` into `shared/` now
**Why bad:** The corrections client is 1834 lines with desktop-specific concerns (keyring, credential files, offline mode, dataclass conversions). Merging it is a major refactor that is not required for PGP access.
**Instead:** Desktop keeps its corrections client. PGP access comes through `shared/document_service` with the shared provider. Unification is a future consideration.

---

## Scalability and Future Considerations

### Adding More Shared Services

The `shared/` package can grow:

```
shared/
  __init__.py
  supabase_provider.py      # Client factory (Phase 1)
  document_service.py       # PGP documents (Phase 2)
  fragment_service.py       # Future: shared fragment operations
  search_enrichment.py      # Future: shared search result decoration
```

### NLI Integration

When NLI crossreference data (815K records) is imported, it will likely need a similar service. It can follow the same pattern:

```python
# shared/nli_service.py
from shared.supabase_provider import get_client

def get_nli_record(sys_id: str) -> Optional[Dict]:
    client = get_client()
    ...
```

### Desktop App Modernization

The desktop app's `SupabaseCorrectionsClient` could eventually be refactored to:
1. Use `shared.supabase_provider.get_client()` as its underlying client
2. Add auth management on top
3. Keep desktop-specific concerns (keyring, offline mode)

This is a separate milestone, not part of the current extraction.

---

## Confidence Assessment

| Area | Confidence | Reason |
|------|------------|--------|
| Module structure (`shared/`) | HIGH | Direct codebase analysis, follows existing patterns |
| Re-export shim approach | HIGH | Standard Python pattern, zero consumer changes needed |
| Client singleton safety | HIGH | Apps run in separate processes, no shared state |
| Build order | HIGH | Each phase is independently testable |
| Auth separation | HIGH | PGP data is public (anon key), community data needs auth |
| Desktop integration path | MEDIUM | PyQt6 UI work not yet designed; the import path is clear but UI panels are TBD |

---

## Summary for Roadmap

**Recommended phase structure:**

1. **Foundation** (shared/ package + supabase_provider) -- Low risk, enables everything else
2. **Extract** (document_service to shared/ + web shim) -- Core migration, backward compatible
3. **Converge** (web + desktop use shared config) -- Remove duplication
4. **Desktop PGP** (add PGP features to desktop app) -- New user-facing functionality

**Total scope:** ~2 new files, ~3 modified files, 0 breaking changes for web app
**Critical path:** Phase 1 and 2 unblock desktop PGP access. Phase 3 is cleanup. Phase 4 is new features.

---

*Research based on: direct analysis of web/document_service.py (507 lines), web/supabase_client.py (1190 lines), supabase_corrections_client.py (1834 lines), lists_sync.py, genizah_app.py, and all consumer modules. No external sources needed -- this is an internal architecture decision.*
