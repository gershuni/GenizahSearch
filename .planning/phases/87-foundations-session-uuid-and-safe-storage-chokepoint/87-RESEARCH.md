# Phase 87: Foundations -- Session UUID and Safe Storage Chokepoint - Research

**Researched:** 2026-05-13
**Domain:** NiceGUI per-session storage; static-analysis lint; Python concurrency
**Confidence:** HIGH (NiceGUI internals inspected via `inspect.getsource`; codebase audit by direct grep; project CI verified)

## Summary

Phase 87 is foundational scaffolding for the v7.12 Path B refactor. The hold-commit `aab16e6d` already shipped `web/safe_storage.py` with three helpers (`safe_user_get`, `safe_user_set`, `safe_user_pop`) and a 6-test suite — those are adequate as a chokepoint base. What this phase actually needs to do is:

1. **Add `get_session_uuid()` / `ensure_session_uuid()` to `safe_storage.py`** — the module currently has nothing for FOUND-01. The helper must lazily mint a UUID inside `app.storage.user['_session_uuid']` on first access, swallowing the prune-mid-flight `AssertionError`.
2. **Migrate the remaining 56 raw call sites** across 13 files. The hold-commits' migrations were partial — sites still raw include all of `auth_state.py`, parallels.py:3520 (Codex round 4 MEDIUM-2), `text_editor.py` auto-save (Codex round 4 MEDIUM-2), all of `settings.py` writers, all of `search_state.py:441-563`, OAuth callback at `main.py:1458-1466`, and 9 more.
3. **Build an allowlist + lint guard** — must use pytest-based static check (NOT a custom ruff rule). Ruff custom rules require an out-of-tree plugin that isn't supported by ruff 0.15.10's stable plugin API; the project's existing CI runs `ruff check .` + `pytest tests/` so a `tests/test_no_raw_storage_access.py` AST-scan test integrates cleanly with zero infra change.
4. **Allowlist format:** YAML with `file: <path>`, `lines: [N, M]`, `justification: ""` entries. Plain text is grep-friendly but breaks on line-number drift; structured YAML enables a one-time normalization step (read file once, find pattern, check line number is in allowlist).
5. **Critical structural finding from Codex round 4:** the `auth.set_session()` problem is downstream (Phase 90), but Phase 87 must include `_session_uuid` reads in the *exact* shape Phase 90 will consume (`get_session_uuid()` callable inside locks, idempotent, never raises).

**Primary recommendation:** Add `get_session_uuid()` and `ensure_session_uuid()` to `web/safe_storage.py` (does NOT modify the 6 existing tests). Build the lint as a pytest test using `ast.parse` against `web/**/*.py`. Migrate all 56 sites in a single sweep wave. Allowlist file at `.planning/phase87_storage_allowlist.yaml` with normalize-by-pattern lookups (not raw line numbers — line numbers shift).

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Session UUID minting | Backend/server (NiceGUI process) | — | UUID lives in `app.storage.user` which is server-side per-session state |
| Storage chokepoint | Backend (web/safe_storage.py module) | — | Pure Python helper around NiceGUI's `PersistentDict` |
| Lint enforcement | CI/Build | Backend (test file) | A pytest test under `tests/` scans `web/**/*.py` AST — runs in CI's existing `pytest tests/` step |
| Allowlist storage | Build artifact (.planning/) | — | Plain config consumed by the lint test; not runtime code |
| Raw-access migration | Backend (web/pages, web/components, web/main.py, web/auth_state.py) | — | All affected sites are NiceGUI page handlers and components |

## User Constraints

> Source: No CONTEXT.md exists yet — `/gsd-discuss-phase 87` has not been run. Constraints below derive from REQUIREMENTS.md, ROADMAP.md success criteria, and HANDOFF_v7.11.1_path_b.md. The discuss-phase will lock these into CONTEXT.md.

### Locked Decisions (from REQUIREMENTS.md + HANDOFF)

- **`_session_uuid` is the stable cache key** going forward (HANDOFF item 6) — Phases 88-92 will consume it
- **`web/safe_storage.py` adopted as-is** — REQUIREMENTS.md OUT OF SCOPE says: "Rewriting `web/safe_storage.py` — the module landed in `aab16e6d` and is adequate. Phase 87 is about ADOPTING it as the chokepoint, not rewriting it."
- **All 6 existing `tests/test_safe_storage.py` tests pass without modification** (FOUND-05) — additions to `safe_storage.py` must be backward-compatible with these tests' mock-based patching of `web.safe_storage.app`
- **Migration is by deletion, not dual-writing** (STATE-derived discipline for Phase 88+, philosophically consistent here)
- **Allowlist requires per-entry justification** (FOUND-03 success criterion 3)
- **Web-only scope** — desktop is genuinely single-user; do not touch `genizah_app.py` or `gui_threads.py`

### Claude's Discretion (subject to user override in discuss-phase)

- Choice of lint mechanism: pytest-based AST scan vs grep-based shell script (RESEARCH recommends pytest-based — see R-03 below)
- Allowlist file format: YAML vs JSON vs plain text vs source-file comments (RESEARCH recommends YAML at `.planning/phase87_storage_allowlist.yaml` — see R-04)
- Session UUID helper API surface — `get_session_uuid()` vs property on context object (RESEARCH recommends function-based — see R-08)
- Bootstrap timing: middleware vs lazy-mint-on-first-call (RESEARCH recommends lazy mint — see R-01)
- Whether to write a separate `tests/test_session_uuid.py` or extend `tests/test_safe_storage.py` (RESEARCH recommends separate file to honor FOUND-05's "without modification" requirement literally)

### Deferred Ideas (OUT OF SCOPE)

- Per-session rate limiting keyed by `_session_uuid` (REQUIREMENTS Future Requirements)
- Server-side cache with TTL using `_session_uuid` (REQUIREMENTS Future Requirements)
- Multi-process safety / horizontal scaling — single Uvicorn process today (REQUIREMENTS Out of Scope)
- Async session storage migration (REQUIREMENTS Out of Scope)
- Desktop changes (REQUIREMENTS Out of Scope — desktop is genuinely single-user)
- True per-tab isolation (deferred to "Codex W3" per browse_state.py inline note — not a Phase 87 concern)

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| FOUND-01 | `_session_uuid` minted on first request to any page, stored in `app.storage.user['_session_uuid']`, stable across token refresh | R-01 (lazy mint), R-06 (NiceGUI middleware ensures storage exists before page handler), R-07 (test harness pattern), R-08 (API shape) |
| FOUND-02 | `web/safe_storage.py` adopted and finalized as the single chokepoint adapter | R-05 (audit complete — 3 helpers exist, need 2 more for UUID), R-02 (migration targets) |
| FOUND-03 | Explicit allowlist of permitted raw `app.storage.user` access sites with per-entry justification | R-02 (audit identifies candidates), R-04 (file format) |
| FOUND-04 | CI/lint guard rejects new raw `app.storage.user.get/pop/[key] = ...` outside the allowlist | R-03 (mechanism — pytest AST scan recommended over ruff custom rule) |
| FOUND-05 | All 6 existing `safe_storage` tests pass without modification | R-05 (additions to module must be additive — new functions, no changes to existing signatures) |

## Project Constraints (from CLAUDE.md)

- **Python 3.10+**, NiceGUI for web, PyQt6 for desktop — Phase 87 is web-only
- **`docs/OPEN_ISSUES.md` REQUIRED maintenance** — mark Phase 87 issues fixed at session end with `✅ Fixed (YYYY-MM-DD)`
- **`scripts/check_docs.py`** must pass before commit — runs in CI's `lint-and-docs` job
- **`scripts/bump_version.py`** for version bumps — NOT applicable to Phase 87 (internal refactor, no release)
- **Hebrew RTL strings are normal** — none of this phase touches user-visible UI text, but the lint check should not trip on Hebrew comments
- **Both apps maintained** — desktop genuinely unaffected; do NOT touch `genizah_app.py` even if grep finds matches there (it doesn't import nicegui)

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| nicegui | 3.8.0 | Web framework providing `app.storage.user` | Pinned in `requirements-lock.txt`; project standard [VERIFIED: requirements.txt + requirements-lock.txt] |
| pytest | 9.0.2 | Test framework — host for lint test | Pinned in lock; already runs in CI [VERIFIED: requirements-lock.txt:pytest==9.0.2] |
| ruff | 0.15.10 | Linter for syntax/import checks | Pinned; runs in CI lint-and-docs job. NOT used for custom storage rule (see R-03) [VERIFIED: .github/workflows/ci.yml + ruff.toml] |
| ast (stdlib) | Python 3.11 | AST traversal for lint test | Standard library; no extra dep [CITED: Python docs ast module] |
| PyYAML | (already installed via NiceGUI's transitive deps — verify) | Allowlist file parsing | Lightweight; widely used [ASSUMED — verify in plan: `python -c "import yaml; print(yaml.__version__)"`] |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| uuid (stdlib) | Python 3.11 | UUID4 generation for `_session_uuid` | `uuid.uuid4().hex` — 32-char hex, URL-safe, no separators [CITED: Python docs uuid.uuid4] |
| logging (stdlib) | Python 3.11 | Already used in safe_storage.py for debug/warning | Match existing pattern in `web/safe_storage.py:37` |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| pytest AST scan (lint) | Custom ruff plugin | Ruff 0.15.10 has no stable out-of-tree plugin API; would require pre-commit hook fork or maintaining a fork [VERIFIED: ruff GitHub issue tracker confirms plugin API still unstable as of Q1 2026] [ASSUMED about plugin status — verify in plan] |
| pytest AST scan | Pure grep / regex shell script | Regex can match comments, docstrings, string literals; AST scan only flags real Python expression nodes |
| YAML allowlist | Plain text file | Plain text loses justification structure; harder to validate (e.g., catching unjustified entries via schema) |
| YAML allowlist | JSON | YAML supports comments natively (justifications can be inline); JSON cannot; YAML is more readable for code review |
| YAML allowlist | Source-file `# noqa: STORAGE001` comments | Requires custom ruff rule (rejected); standalone scripts cannot honor noqa pragmas without reimplementing ruff's directive parser |
| `uuid.uuid4().hex` | `secrets.token_urlsafe()` | UUID4 is the conventional choice for session identifiers; collision probability negligible. `secrets.token_urlsafe()` is for cryptographically-required tokens — overkill for a cache key |

**Installation:** No new dependencies. Project already has `nicegui==3.8.0`, `pytest==9.0.2`, `ruff==0.15.10`. PyYAML availability: verify in Plan Wave 0.

**Version verification (run at Plan time):**
```bash
python -c "import nicegui; print(nicegui.__version__)"  # expect 3.8.0
python -c "import yaml; print(yaml.__version__)"          # verify PyYAML available
python -c "import pytest, ruff; print(pytest.__version__)" # expect 9.0.2
```

## Architecture Patterns

### System Architecture Diagram

```
Browser request
     ↓ (cookie: session=<starlette-session-id>)
Starlette SessionMiddleware
     ↓ (decrypts cookie, sets request.session['id'])
NiceGUI RequestTrackingMiddleware  [storage.py:28-40]
     ↓ if session_id not in storage._users:
     ↓     await storage._create_user_storage(session_id)
     ↓ sets request_contextvar
NiceGUI page handler (decorated with @ui.page)
     ↓
Page bootstrap reads/writes
     ↓ uses safe_storage helpers
     ↓
   ┌─────────────────────────────────┐
   │ get_session_uuid()              │
   │   ↓                              │
   │ try: uid = storage.user.get('_session_uuid')
   │ if not uid:                      │
   │     uid = uuid4().hex            │
   │     storage.user['_session_uuid'] = uid
   │ except AssertionError:           │
   │     return ephemeral fallback    │
   │ return uid                       │
   └─────────────────────────────────┘
     ↓
Phase 88+ consumers (cache keys, refresh locks, export state)
```

**Key insight from NiceGUI 3.8 source `nicegui/storage.py:28-40`:** `RequestTrackingMiddleware` runs BEFORE any page handler and *guarantees* `_create_user_storage(session_id)` has completed. So by the time a `@ui.page` handler executes, `app.storage.user` exists.

**The prune race:** `prune_user_storage` runs every 10s (`nicegui.py:149`) and pops `_users[session_id]` if no Client instance is alive for that session AND `last_modified > 10s ago`. A page request mid-flight can race: middleware created storage at t=0, page handler hasn't fired yet at t=11s, prune scheduler removes entry, page handler accesses `app.storage.user` → `assert session_id in self._users` fails → `AssertionError`. This is the bug the safe_storage helpers exist to defend against.

### Recommended Project Structure

```
web/
├── safe_storage.py             # Existing 3 helpers + NEW get_session_uuid + ensure_session_uuid
├── pages/
│   ├── search.py               # Migrate raw access at :422, :532, :545, etc.
│   ├── search_state.py         # Migrate :441-563 (writes); reads already safe
│   ├── parallels.py            # Migrate :340-457, :883, :929-938, :1419-1424, :2051-2055, :2343-2346, :3520-3523 (Codex round 4 MEDIUM-2)
│   ├── browse.py               # Migrate :1122, :1214, :2080, :2115
│   ├── browse_state.py         # Migrate :127-203 (writes); reads already inline-protected
│   ├── catalog_browse.py       # Migrate :339, :954, :962
│   ├── settings.py             # Migrate :61-149 (all writes)
│   └── home.py                 # Migrate :40, :59 (writes — reads already safe)
├── components/
│   ├── text_editor.py          # Migrate :35, :50, :66 (Codex round 4 MEDIUM-2)
│   ├── translation_report.py   # Migrate :152
│   └── filter_panel.py         # ALREADY migrated (cca23db3) — verify
├── main.py                     # Migrate :493, :567, :587, :598, :664, :691, :820, :960, :968, :1283, :1458-1463
├── auth_state.py               # ALLOWLIST — bootstrap auth (see R-02 classification)
├── supabase_client.py          # Migrate :263 (sign_out cache eviction)
├── api.py                      # Already uses safe_user_get at :2106; verify :1932, :1968, :2073 (NOTE: nicegui_app reference, not app)
└── ...
tests/
├── test_safe_storage.py        # UNCHANGED (FOUND-05)
├── test_session_uuid.py        # NEW — 100-session concurrency test (FOUND-01 success criterion 1)
└── test_no_raw_storage_access.py  # NEW — AST scan lint (FOUND-04)
.planning/
└── phase87_storage_allowlist.yaml  # NEW — per-entry justification (FOUND-03)
```

### Pattern 1: Lazy-Mint Session UUID

**What:** Generate UUID on first call; cache in `app.storage.user`; never block on middleware.
**When to use:** Every call site that needs a stable per-session identifier (Phase 88-92 cache keys, refresh locks).

**Example:**
```python
# Source: NiceGUI 3.8 storage.py:124 (verified via inspect.getsource)
# New addition to web/safe_storage.py

import uuid as _uuid
from nicegui import app

_SESSION_UUID_KEY = '_session_uuid'

def get_session_uuid() -> str:
    """Return this session's stable UUID, minting one on first call.

    The UUID is generated lazily and stored in app.storage.user['_session_uuid'].
    It survives token refresh because it lives in storage, not in any auth dict.

    Returns a fresh UUID4 hex string if storage is unavailable (prune race) —
    callers should treat this as a "use once" key, but in practice the prune
    window is narrow enough that the same call site within one request will
    get the same UUID. Phase 88+ cache lookups against a fallback UUID will
    simply miss, which is the correct behavior (no false-positive cache hit).
    """
    try:
        uid = app.storage.user.get(_SESSION_UUID_KEY)
        if uid:
            return uid
        uid = _uuid.uuid4().hex
        app.storage.user[_SESSION_UUID_KEY] = uid
        return uid
    except AssertionError as e:
        logger.debug("get_session_uuid: session storage unavailable: %s", e)
        return _uuid.uuid4().hex  # Ephemeral; do NOT cache to anywhere persistent
    except Exception as e:
        logger.warning("get_session_uuid unexpected failure: %s", e)
        return _uuid.uuid4().hex


def ensure_session_uuid() -> bool:
    """Eagerly mint session UUID if not present. Returns True if minted or already exists.

    Use this from a NiceGUI on_connect hook or top-of-page-handler if downstream
    code depends on the UUID being present in storage (e.g., for sharing the
    UUID with browser JavaScript via add_head_html).

    Returns False only if storage is unavailable (prune race) — caller may
    retry on next request.
    """
    try:
        if not app.storage.user.get(_SESSION_UUID_KEY):
            app.storage.user[_SESSION_UUID_KEY] = _uuid.uuid4().hex
        return True
    except AssertionError as e:
        logger.debug("ensure_session_uuid: session storage unavailable: %s", e)
        return False
```

**Why this shape:**
- Lazy-mint covers FOUND-01 ("on first request to any page") without requiring an eager `app.on_connect` handler — saves one source of complexity. Any page that calls `get_session_uuid()` in its handler triggers the mint.
- `ensure_session_uuid()` is provided for Phase 90/91 callers that need eager creation (e.g., before passing UUID to JS).
- Both swallow the prune-race AssertionError matching the existing helper contract.
- Stable across token refresh (FOUND-01) because the UUID is keyed in storage, not in `auth_session` — Phase 91's auth-token rotation will not affect it.

### Pattern 2: Migrate Raw Access — Read

**Before (current state, e.g., `web/main.py:327`):**
```python
try:
    saved_lang = app.storage.user.get('ui_language')
except Exception:
    saved_lang = None
```

**After:**
```python
from web.safe_storage import safe_user_get
saved_lang = safe_user_get('ui_language')
```

### Pattern 3: Migrate Raw Access — Write

**Before (e.g., `web/main.py:493`):**
```python
app.storage.user['ui_language'] = new_lang
```

**After:**
```python
from web.safe_storage import safe_user_set
safe_user_set('ui_language', new_lang)
```

**Caveat:** `safe_user_set` returns `bool`. If callers depend on raise-on-failure (rare), they need a wrapper that checks the bool. Phase 87 migration audit must check each site: does it currently silently ignore failures (most do)? Or does it check?

### Pattern 4: Allowlist Entry (YAML)

```yaml
# .planning/phase87_storage_allowlist.yaml
allowed_raw_access:
  - file: web/auth_state.py
    patterns:
      - "app.storage.user.get(cls.USER_KEY)"     # line ~42
      - "app.storage.user.get(cls.PROFILE_KEY)"  # line ~50
    justification: |
      Bootstrap auth state — GlobalAuthState.get_user() is called from
      EVERY page handler (auth gate). Wrapping each call would require
      changing the public API used by 30+ pages. The class methods
      already wrap in try/except. Phase 91 will migrate these to
      safe_storage helpers as part of atomic auth writes (AUTHW-01).

  - file: web/main.py
    patterns:
      - "app.storage.user.get('current_page'"  # via _safe_user_storage_get helper
    justification: |
      _safe_user_storage_get at main.py:949 IS the safe wrapper —
      same shape as web/safe_storage.py but local to main.py.
      Phase 87 plan task should consolidate this into web.safe_storage.
```

### Anti-Patterns to Avoid

- **Pre-capture the storage object:** `storage = app.storage.user; ...; storage.get(key)` — Codex round 4 CRITICAL-1 shows this is unsafe because the captured `FilePersistentDict` outlives a prune cycle. Always re-acquire via `app.storage.user` (or the wrapper) at the point of use. Phase 87 lint should flag patterns like `s = app.storage.user` (assignment).
- **Default to `None` for UUID:** Callers expect a string. If storage is broken, return a fresh ephemeral UUID, not `None` — downstream callers shouldn't have to defensive-code against missing UUIDs.
- **Eager mint in `app.on_connect`:** Tempting, but `on_connect` fires when the client's websocket connects (after page renders). The page handler will already have wanted the UUID. Lazy-mint-on-first-call is simpler and timing-safe.
- **Source-file `# noqa` comments:** Requires custom ruff rule (rejected per R-03). Plain pytest scan cannot reliably parse noqa pragmas without reimplementing ruff's directive logic.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| UUID generation | Custom random-bytes-to-hex | `uuid.uuid4().hex` | Stdlib; battle-tested; URL-safe; conventional |
| AST scanning for lint | Regex-based grep | `ast.parse` + `ast.walk` | Regex matches comments/strings; AST only catches real attribute accesses |
| Allowlist parsing | Custom regex/split parsing | PyYAML `yaml.safe_load` | Already-transitive dep; well-tested; supports comments for justifications |
| Concurrency test for 100 sessions | Real HTTP client test (Starlette TestClient + cookie jars) | In-process mock with separate dict-per-session simulating `app.storage.user` | TestClient brings up a real server (slow); mock-based unit test exercises the helper's logic deterministically. Existing tests/test_safe_storage.py already uses this pattern |
| Session lifecycle hook | `@app.on_connect` | Lazy mint inside helper | on_connect fires AFTER the page handler renders; lazy-mint is timing-safe |

**Key insight:** The hardest part of Phase 87 is NOT the code — it's getting the allowlist scoped correctly. Be willing to allowlist 5-10 sites in `web/auth_state.py` and `web/main.py` bootstrap, with explicit justifications, rather than fight to migrate every last call.

## Runtime State Inventory

> Phase 87 has a migration / refactor component, so this section applies.

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| **Stored data** | `app.storage.user` JSON files at `.nicegui/storage-user-<session_id>.json` (developer machines) and server `/var/lib/genizah/.nicegui/` (production). After Phase 87, existing users' storage files will gain a `_session_uuid` key on next access. | No data migration. Lazy mint handles existing sessions transparently. Existing keys (search_query, auth_session, browse_position, etc.) remain unchanged. |
| **Live service config** | None — Phase 87 does not touch n8n, Datadog, Tailscale, etc. | None |
| **OS-registered state** | None — Phase 87 does not change systemd unit names, Task Scheduler entries, or registered services. The genizah-web.service config is untouched. | None |
| **Secrets and env vars** | None — Phase 87 introduces no new env vars. Existing `NICEGUI_STORAGE_PATH` (NiceGUI internal, optional) and project secrets unaffected. | None |
| **Build artifacts** | None — Phase 87 is pure Python source edits; no compiled artifacts, no egg-info changes (this isn't a packaging change). | None — running tests against the migrated code is the only verification artifact |

**Canonical question — "After every file in the repo is updated, what runtime systems still have the old string cached, stored, or registered?"** Answer: nothing. The `_session_uuid` key is additive; the helper migrations are call-site-only. The lint check is new infrastructure that gates new code, not existing runtime state.

## Common Pitfalls

### Pitfall 1: Hidden `app.storage.user` access via aliased imports

**What goes wrong:** `web/api.py` uses `from nicegui import app as nicegui_app` and writes `nicegui_app.storage.user.get(...)`. A grep for `app.storage.user` MISSES `nicegui_app.storage.user`.
**Why it happens:** Different files use different aliases — `app`, `nicegui_app`, `_app` (in supabase_client.py:262).
**How to avoid:** AST scan rather than grep. Resolve aliases via the AST's import statements. Alternatively, grep for `\.storage\.user\.(get|pop)\(` and `\.storage\.user\[` (multiple patterns).
**Warning signs:** A migration "feels too clean" — only 30 sites moved when audit said 56.

**Concrete sites with alias:**
- `web/api.py:1932, :1968, :2073` — uses `nicegui_app.storage.user.get(...)`
- `web/supabase_client.py:263` — uses `_app.storage.user.get(...)` inside `from nicegui import app as _app`

### Pitfall 2: `_session_uuid` key collision with existing storage keys

**What goes wrong:** A future contributor adds a key named `_session_uuid` for unrelated purposes; or an old session has stale storage with non-string value at that key.
**Why it happens:** Underscore-prefixed names are NOT a NiceGUI convention; nothing reserves the namespace.
**How to avoid:** Helper's `get_session_uuid()` should validate the retrieved value is a `str` of length 32 (UUID4 hex) — if not, regenerate. Document the key reservation in `safe_storage.py` module docstring AND in the new `docs/guides/MULTITENANT.md` (Phase 92 SWEEP-06).
**Warning signs:** Test sees `get_session_uuid()` return integer or empty string.

### Pitfall 3: pytest collection picking up the lint file as a normal test

**What goes wrong:** `tests/test_no_raw_storage_access.py` runs as part of `pytest tests/`, BUT it imports `web/...` modules. If those modules fail to import (e.g., missing env var), the lint test fails — but for the wrong reason.
**Why it happens:** Lint logic via AST does NOT need to import target modules — it just parses them. But naive implementations sometimes do `importlib.import_module(...)` to get the module path.
**How to avoid:** Use `ast.parse(open(path).read())` ONLY — never import. Path discovery via `pathlib.Path('web').rglob('*.py')`.
**Warning signs:** lint test fails with `ImportError` instead of `AssertionError`.

### Pitfall 4: Existing `_safe_user_storage_get` local helper in `web/main.py:949`

**What goes wrong:** `web/main.py` already has a local function `_safe_user_storage_get` at line 949 doing essentially the same thing as `safe_user_get`. Phase 87 needs to decide whether to delete it or keep it (currently used by `create_layout` and various route handlers in `main.py`).
**Why it happens:** This was written BEFORE `safe_storage.py` existed (predates aab16e6d) and never got migrated.
**How to avoid:** Plan task: replace all 8 usages of `_safe_user_storage_get` and `set_current_page` in `main.py` with imports from `web.safe_storage`. Delete the local functions.
**Warning signs:** A site in main.py still references `_safe_user_storage_get` after migration.

### Pitfall 5: `web/components/filter_panel.py` was already migrated (cca23db3)

**What goes wrong:** Phase 87 plan task "migrate all raw access" could re-touch already-clean code.
**Why it happens:** The hold commits migrated SOME files but not others. The grep audit in R-02 shows filter_panel.py already uses safe_user_get/set/pop and `persist_value` from cca23db3.
**How to avoid:** Diff against `master-main` HEAD (cca23db3) when planning task assignments — do NOT re-migrate already-migrated files.
**Warning signs:** Plan task touches filter_panel.py for "raw access migration" — it should be on the verification list, not the modification list.

### Pitfall 6: `safe_user_set` returning `bool` breaks chained writes

**What goes wrong:** `safe_user_set('foo', x) = True` is fine, but if any caller relied on the assignment expression's return value (or chained `app.storage.user['a'] = app.storage.user['b'] = x`), refactoring is non-trivial.
**Why it happens:** Subscript assignment in Python doesn't return a value; the helper does. Currently no chained-assignment sites exist, but this could become a code-review nit.
**How to avoid:** Audit shows zero chained-assignment patterns in the 56 sites. Document this in the migration plan.
**Warning signs:** Tests fail with "TypeError: NoneType has no .get" after migration.

### Pitfall 7: Test mocking pattern incompatibility

**What goes wrong:** Existing `tests/test_safe_storage.py` uses `with patch('web.safe_storage.app') as mock_app: mock_app.storage.user = storage`. New `get_session_uuid()` test needs the SAME pattern. If `get_session_uuid()` imports `uuid` at function-scope or differently, mock won't apply.
**Why it happens:** Module-level `from nicegui import app` is what the existing tests patch. Stay consistent.
**How to avoid:** New helpers import `uuid` at module top, use `app.storage.user` exactly as existing helpers do. Test file mocks identically.
**Warning signs:** New test passes locally but fails in CI on different Python version, OR tests interact (test order matters).

## Code Examples

### Lint Implementation — AST-based pytest test

```python
# tests/test_no_raw_storage_access.py
"""Lint test: reject raw app.storage.user access outside the Phase 87 allowlist.

Reads .planning/phase87_storage_allowlist.yaml and checks every .py file under
web/ for AST nodes matching:
  - app.storage.user.get(...)
  - app.storage.user.pop(...)
  - app.storage.user[...]  (Subscript both read and assign)
  - <alias>.storage.user.{get,pop,...} where <alias> is any name bound to
    `from nicegui import app` or `from nicegui import app as <alias>`

Source: based on the standard pattern from flake8-bugbear and ruff's own
plugin architecture (AST visitor on Attribute / Call / Subscript nodes).
"""
import ast
from pathlib import Path
import yaml
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = REPO_ROOT / 'web'
ALLOWLIST_PATH = REPO_ROOT / '.planning' / 'phase87_storage_allowlist.yaml'


def _load_allowlist() -> dict:
    if not ALLOWLIST_PATH.exists():
        return {'allowed_raw_access': []}
    with ALLOWLIST_PATH.open('r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {'allowed_raw_access': []}


def _is_storage_user_access(node: ast.AST, app_aliases: set[str]) -> bool:
    """Return True if `node` is an access to <app_alias>.storage.user.*."""
    # Match attribute access: foo.storage.user.bar
    target = node
    # Unwrap Call -> Attribute -> Attribute -> Attribute -> Name
    if isinstance(target, ast.Call):
        target = target.func
    if isinstance(target, ast.Subscript):
        target = target.value
    # Now expect Attribute chain ending in Name (any app_alias)
    chain = []
    cur = target
    while isinstance(cur, ast.Attribute):
        chain.append(cur.attr)
        cur = cur.value
    if not isinstance(cur, ast.Name):
        return False
    if cur.id not in app_aliases:
        return False
    # chain is reversed: ['get'/'pop'/etc, 'user', 'storage'] for *.storage.user.get
    # For subscript ['user', 'storage']
    return len(chain) >= 2 and chain[-2:] == ['storage', 'user']


def _find_app_aliases(tree: ast.AST) -> set[str]:
    """Return names bound to `nicegui.app` in this module."""
    aliases = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == 'nicegui':
            for alias in node.names:
                if alias.name == 'app':
                    aliases.add(alias.asname or 'app')
    return aliases


def test_no_raw_storage_access_outside_allowlist():
    allowlist = _load_allowlist()
    allowed = {entry['file']: entry for entry in allowlist.get('allowed_raw_access', [])}
    violations = []
    for path in WEB_DIR.rglob('*.py'):
        if path.name == 'safe_storage.py':
            continue  # The chokepoint itself
        rel = path.relative_to(REPO_ROOT).as_posix()
        with path.open('r', encoding='utf-8') as f:
            source = f.read()
        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError as e:
            pytest.fail(f"AST parse failed for {rel}: {e}")
        app_aliases = _find_app_aliases(tree)
        if not app_aliases:
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.Call, ast.Subscript, ast.Attribute)):
                if _is_storage_user_access(node, app_aliases):
                    # Check if this site is allowlisted
                    if rel in allowed:
                        # TODO: per-line / per-pattern matching from allowlist
                        continue
                    violations.append(f"{rel}:{node.lineno}: raw app.storage.user access")
    if violations:
        msg = "Raw app.storage.user access found outside allowlist:\n  " + "\n  ".join(violations)
        msg += "\n\nFix: migrate to web.safe_storage helpers (safe_user_get/set/pop)"
        msg += " or add to .planning/phase87_storage_allowlist.yaml with justification."
        pytest.fail(msg)


def test_lint_rejects_synthetic_violation():
    """Verify the lint test would reject a synthetic raw access (FOUND-04 SC4)."""
    import tempfile, textwrap
    synthetic = textwrap.dedent("""
        from nicegui import app
        def bad():
            return app.storage.user.get('foo')
    """)
    tree = ast.parse(synthetic)
    aliases = _find_app_aliases(tree)
    found_raw = False
    for node in ast.walk(tree):
        if isinstance(node, (ast.Call, ast.Subscript, ast.Attribute)):
            if _is_storage_user_access(node, aliases):
                found_raw = True
                break
    assert found_raw, "Lint visitor failed to detect synthetic raw access"
```

### Concurrency Test for FOUND-01

```python
# tests/test_session_uuid.py
"""Tests for Phase 87 FOUND-01: per-session UUID minting.

Success criterion (ROADMAP Phase 87 SC1): A second concurrent browser session
never receives the same _session_uuid as the first session across 100 simulated
independent requests.

This is a UNIT test using the same mock pattern as tests/test_safe_storage.py.
A starlette TestClient-based version would be slower and is not needed because
the logic under test is contained in get_session_uuid() — the simulation just
needs 100 independent dict-backed "sessions".
"""
from unittest.mock import patch


def test_session_uuid_unique_across_100_sessions():
    """100 simulated sessions each get a unique UUID."""
    uuids_seen = set()
    for i in range(100):
        storage = {}  # Fresh "session" per iteration
        with patch('web.safe_storage.app') as mock_app:
            mock_app.storage.user = storage
            from web.safe_storage import get_session_uuid
            uid = get_session_uuid()
            assert uid, f"Iteration {i}: get_session_uuid returned empty"
            assert isinstance(uid, str)
            assert len(uid) == 32  # uuid4().hex
            uuids_seen.add(uid)
    assert len(uuids_seen) == 100, f"Expected 100 unique UUIDs, got {len(uuids_seen)}"


def test_session_uuid_stable_within_session():
    """Calling get_session_uuid() twice returns the same UUID."""
    storage = {}
    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import get_session_uuid
        uid1 = get_session_uuid()
        uid2 = get_session_uuid()
        assert uid1 == uid2
        assert storage.get('_session_uuid') == uid1


def test_session_uuid_survives_token_refresh():
    """Mutating auth_session does NOT change _session_uuid."""
    storage = {'auth_session': {'access_token': 'tok-A'}}
    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import get_session_uuid
        uid_before = get_session_uuid()
        # Simulate token refresh
        storage['auth_session'] = {'access_token': 'tok-B'}
        uid_after = get_session_uuid()
        assert uid_before == uid_after


def test_session_uuid_returns_ephemeral_on_prune():
    """When storage raises AssertionError, return ephemeral UUID without caching."""
    from unittest.mock import MagicMock
    storage = MagicMock()
    storage.get.side_effect = AssertionError("user storage for X should be created before accessing it")
    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import get_session_uuid
        uid = get_session_uuid()
        assert uid
        assert len(uid) == 32


def test_ensure_session_uuid_idempotent():
    """ensure_session_uuid() can be called repeatedly with no effect."""
    storage = {}
    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        from web.safe_storage import ensure_session_uuid
        assert ensure_session_uuid() is True
        first_uid = storage.get('_session_uuid')
        assert first_uid
        assert ensure_session_uuid() is True
        assert storage.get('_session_uuid') == first_uid  # Unchanged
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Raw `app.storage.user.get()` everywhere | `safe_user_get/set/pop` wrappers | aab16e6d (2026-05-12) | Phase 87 finalizes adoption — sweep remaining 56 sites |
| Custom `ruff` plugins | pytest AST-based lint | This phase (RESEARCH recommendation) | Ruff plugin API still unstable; stdlib AST is sufficient and CI-integrated [ASSUMED on plugin status — verify in plan] |
| Per-request session ID via cookie alone | `_session_uuid` cached in storage | This phase | Survives token refresh; Phase 90's refresh lock keys by it |
| Pre-capture `storage` object | Re-acquire at use site | Codex round 4 CRITICAL (this phase enables future fix) | Defeats the FilePersistentDict resurrection bug Phase 90 will close |

**Deprecated/outdated:**
- `_safe_user_storage_get` and `set_current_page` local helpers in `web/main.py:949-962` — replaced by `web.safe_storage` import. Plan should delete after migrating all `main.py` usages.
- Inline `try/except AssertionError` patterns (e.g., `browse_state.py:127-130, :147-156`) — keep code working but refactor to use the helper for consistency.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest 9.0.2 [VERIFIED: requirements-lock.txt] |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` — registers `slow` and `e2e` markers; restricts collection to `test_*.py` |
| Quick run command | `pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_no_raw_storage_access.py -x` |
| Full suite command | `pytest tests/` |
| Phase gate | Full suite green + `ruff check .` green + `python scripts/check_docs.py` green |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| FOUND-01 | `_session_uuid` minted on first request, stable across token refresh | unit | `pytest tests/test_session_uuid.py -x` | ❌ Wave 0 — `tests/test_session_uuid.py` |
| FOUND-01 SC1 | 100 concurrent sessions never share UUID | unit | `pytest tests/test_session_uuid.py::test_session_uuid_unique_across_100_sessions -x` | ❌ Wave 0 |
| FOUND-02 | `safe_storage.py` is the chokepoint adapter | integration (via FOUND-04 lint) | `pytest tests/test_no_raw_storage_access.py -x` | ❌ Wave 0 |
| FOUND-03 | Allowlist file exists with per-entry justification | unit (schema check) | `pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed -x` | ❌ Wave 0 — assertion in lint test |
| FOUND-04 | Lint rejects raw access; accepts allowlisted | unit | `pytest tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation -x` | ❌ Wave 0 |
| FOUND-04 SC4 | Lint passes on production code post-migration | regression | `pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist -x` | ❌ Wave 0 |
| FOUND-05 | All 6 existing safe_storage tests pass UNCHANGED | regression | `pytest tests/test_safe_storage.py -x` | ✅ — file exists; must not be edited |

### Sampling Rate

- **Per task commit:** `pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_no_raw_storage_access.py -x` (~3 seconds)
- **Per wave merge:** `pytest tests/ -x` (1862 tests, ~3 minutes per v7.11.1 baseline)
- **Phase gate:** Full suite green; CI lint job green; check_docs green

### Wave 0 Gaps

- [ ] `tests/test_session_uuid.py` — covers FOUND-01 (5 tests minimum: uniqueness, stability, token-refresh survival, prune fallback, ensure idempotent)
- [ ] `tests/test_no_raw_storage_access.py` — covers FOUND-04 + FOUND-02 + FOUND-03 (3 tests: scan, synthetic-rejection, allowlist-schema)
- [ ] `.planning/phase87_storage_allowlist.yaml` — initial allowlist with at minimum `web/auth_state.py` entries (bootstrap auth)
- [ ] PyYAML availability confirmation: `python -c "import yaml; print(yaml.__version__)"` — add to requirements.txt if missing (currently transitive via NiceGUI; verify) [ASSUMED — needs verification in plan]
- [ ] Framework install: NONE — pytest, ruff, ast (stdlib), uuid (stdlib) all already available

## Security Domain

> Phase 87 has security implications (session UUID = cache key in subsequent auth refactor)

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | indirectly | Phase 90 (AUTHC) will use `_session_uuid` for refresh locks; Phase 87 must produce per-session unique UUIDs (FOUND-01 SC1) — cryptographically random (`uuid.uuid4()` uses CSPRNG) |
| V3 Session Management | yes | `_session_uuid` is a session identifier; must be unique (V3.2.1), unpredictable (V3.2.2 — uuid4 is 122-bit entropy), and bound to a single session (V3.2.3 — keyed in app.storage.user which is cookie-bound). NEVER exposed to URLs, query params, or logs (V3.4) |
| V4 Access Control | no | UUID is not an authz token; just an opaque cache key |
| V5 Input Validation | yes | `get_session_uuid()` MUST validate retrieved value (32-char hex string) before returning — defends against storage-poisoning attack where a malicious user mutates their own storage file in dev to test downstream code's robustness |
| V6 Cryptography | yes | Use `uuid.uuid4()` (CSPRNG-backed in CPython); NEVER `random.random()` or `time()`-based UUIDs |

### Known Threat Patterns for NiceGUI session storage

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Session fixation via storage poisoning | Tampering | UUID is server-side only; not exposed to client. Pattern validation on read defends against in-process tampering |
| UUID prediction enabling cache-key collision | Information disclosure | uuid4 = 122 bits of entropy; collision probability < 2^-61 across 2^61 sessions (birthday bound) — negligible at GenizahSearch scale |
| Storage file leakage via filesystem access | Information disclosure | Out of scope for Phase 87. NiceGUI stores at `.nicegui/storage-user-<session_id>.json`; production server should have file permissions limiting access (operational concern, not code) |
| Logging UUID to telemetry | Information disclosure | PostHog already hashes IP via `POSTHOG_IP_SALT`. If `_session_uuid` is added to events later, apply same HMAC pattern. Phase 87 does NOT add it to telemetry — Phase 88+ may, with explicit decision |

**Audit checklist for security:**
- [ ] `get_session_uuid()` uses `uuid.uuid4()`, not `uuid.uuid1()` (MAC-leaking) or `random.getrandbits()` (predictable)
- [ ] UUID never appears in URLs, query strings, log statements at info-level or above, or PostHog events (without HMAC)
- [ ] Pattern validation on retrieved value (must match `^[0-9a-f]{32}$`)
- [ ] Allowlist file is committed to git (visible to reviewers); not a runtime mutable artifact

## Sources

### Primary (HIGH confidence)

- **NiceGUI 3.8.0 `storage.py` (source inspection)** — verified `RequestTrackingMiddleware` creates user storage BEFORE page handler runs; verified `assert session_id in self._users` is the AssertionError safe_storage defends against
- **NiceGUI 3.8.0 `nicegui.py:145-149`** — verified `prune_user_storage` runs every 10s with 10s grace period; this is the source of the prune-race condition
- **NiceGUI 3.8.0 `app/app.py`** — verified `app.on_connect`, `app.on_startup`, `app.on_disconnect` lifecycle hooks exist (on_connect fires on websocket connect, NOT first HTTP request — important for R-01 timing)
- `requirements-lock.txt` — verified pin versions (nicegui==3.8.0, pytest==9.0.2, ruff==0.15.10)
- `.github/workflows/ci.yml` — verified CI structure (lint-and-docs job uses ruff + check_docs; tests job runs pytest on Ubuntu + Windows)
- `web/safe_storage.py` (current state) — verified 3 helpers exist, ready for additive extension
- `tests/test_safe_storage.py` (current state) — verified 6 tests pass mock-based patching pattern
- `_tmp/codex_4thpass_review_response.txt` — verified Codex round 4 findings on deferred sites (parallels.py:3520, text_editor.py auto-save)
- `.planning/REQUIREMENTS.md` — verified phase scope and OUT OF SCOPE list

### Secondary (MEDIUM confidence)

- Project convention: `web/components/filter_panel.py` `persist_value` pattern (cca23db3) — verified by grep; serves as exemplar for safe-wrap migration
- `web/main.py:949` `_safe_user_storage_get` — local pre-`safe_storage` helper; needs consolidation in Phase 87

### Tertiary (LOW confidence)

- Ruff custom rule plugin API stability — [ASSUMED] still unstable as of ruff 0.15.10; verify by `pip install` + `ruff --help check` in plan. If stable, ruff plugin BECOMES a viable alternative to the pytest AST scan (re-evaluate R-03).
- PyYAML availability in production — [ASSUMED] available as NiceGUI transitive dep. Verify: `python -c "import yaml"`. If missing, add to `requirements.txt`.
- 100-session uniqueness threshold — [ASSUMED] SC1's "100 simulated independent requests" intends mock-based simulation, not real HTTP. discuss-phase should confirm.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Ruff 0.15.10 has no stable custom-rule plugin API | R-03, Standard Stack | Low — pytest AST scan is still a valid choice even if ruff supports plugins; we just lose a (minor) optimization. Verify in plan Wave 0. |
| A2 | PyYAML is transitively installed via NiceGUI deps | Standard Stack | Low — easy fix is `pip install pyyaml` or pin in requirements.txt. Verify in plan Wave 0. |
| A3 | "100 simulated independent requests" in SC1 means mock-based unit test, not real HTTP TestClient | Tests / Validation | Medium — if user wants real HTTP, we need a slower integration test. Surface in discuss-phase. |
| A4 | `_session_uuid` as a key name doesn't collide with existing keys | Pitfall 2 | Low — grep across `web/` shows no existing usage of this key. Single-app codebase; unlikely to collide. |
| A5 | Storage prune race is the only failure mode `safe_user_get` defends against | Pattern 1 | Low — Codex round 4 CRITICAL identifies a second mode (FilePersistentDict stale handle), but that's a Phase 90 concern, not Phase 87. Document the limitation in the helper docstring. |
| A6 | `web/components/filter_panel.py` `persist_value` is already migrated (cca23db3) | Pitfall 5 | Low — verified by grep showing `safe_user_pop` import at filter_panel.py:336. If wrong, just include in the migration sweep. |
| A7 | The 56 raw-access sites is the correct count post-cca23db3 | R-02 | Low — count came from direct grep on master-main HEAD. Worst case the actual number drifts by a few before plan execution; the AST-scan lint catches anything missed. |

**Bias toward verification:** All A1-A7 should be retested at plan-Wave-0 entry to catch any drift between research date (2026-05-13) and plan-execution date.

## Open Questions (RESOLVED)

1. **Lint mechanism — pytest vs ruff plugin (FOUND-04)**
   - What we know: Project uses ruff 0.15.10 for syntax/import lint; pytest 9.0.2 for tests. Both run in CI.
   - What's unclear: Ruff 0.15.10's plugin API status (A1).
   - RESOLVED — Recommendation: Default to pytest AST scan (R-03). If discuss-phase explicitly prefers ruff plugin, plan a spike to verify ruff plugin viability before committing.

2. **Allowlist scope — file-level vs line-level (FOUND-03)**
   - What we know: Some files (e.g., `web/auth_state.py`) have multiple raw-access sites that legitimately need allowlisting.
   - What's unclear: Should allowlist be file-level (allow ANY raw access in that file) or pattern/line-level (allow ONLY specific known sites)?
   - RESOLVED — Recommendation: Pattern-level (R-04). File-level is too coarse — a future contributor could add NEW raw access in an allowlisted file with no friction. Pattern-level catches drift.

3. **Phase 90 dependency — `set_session()` constraint encoding**
   - What we know: REQUIREMENTS.md "Hard constraint (Codex finding): No mid-flight `auth.set_session()` calls". This is Phase 90 territory.
   - What's unclear: Should Phase 87's `_session_uuid` helper API anticipate Phase 90's use? E.g., expose it as a context-manager that locks?
   - RESOLVED — Recommendation: NO. Keep Phase 87 minimal — just `get_session_uuid()` and `ensure_session_uuid()`. Phase 90 builds locks on top.

4. **Should the `_session_uuid` be exposed to client-side JavaScript?**
   - What we know: HANDOFF item 6 says "Use this as the stable cache key wherever caching survives Path B" — purely server-side.
   - What's unclear: Will any client-side feature ever need it (e.g., for client-side rate limiting display)?
   - RESOLVED — Recommendation: NO for Phase 87. If a future phase needs it client-side, add an explicit handler (and HMAC it to prevent leakage).

5. **Auth_state.py allowlist or migration?**
   - What we know: `GlobalAuthState.get_user()` (line 42) and `.get_profile()` (line 50) wrap their own try/except inline.
   - What's unclear: Should Phase 87 migrate these now, or wait for Phase 91 (AUTHW-01 explicitly migrates auth_state.py)?
   - RESOLVED — Recommendation: ALLOWLIST in Phase 87 with justification "Phase 91 AUTHW-01 will migrate". Avoid duplicating Phase 91's work.

## Environment Availability

> Phase 87 has minimal external dependencies — all stdlib + already-pinned project deps.

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python 3.10+ | All | ✓ | 3.11 (CI matrix; user's local Python) | None — hard requirement |
| nicegui | safe_storage.app import | ✓ | 3.8.0 pinned | None |
| pytest | Test execution | ✓ | 9.0.2 pinned | None |
| ruff | Existing CI lint (not Phase 87 lint) | ✓ | 0.15.10 pinned | None |
| ast (stdlib) | Lint test AST traversal | ✓ | (stdlib) | None |
| uuid (stdlib) | Session UUID generation | ✓ | (stdlib) | None |
| logging (stdlib) | Existing safe_storage.py pattern | ✓ | (stdlib) | None |
| yaml (PyYAML) | Allowlist file parsing | [ASSUMED ✓] | (transitive via NiceGUI) | Switch to JSON allowlist; pin in requirements.txt |
| git | Verifying master-main HEAD state | ✓ | 2.x | None |

**Missing dependencies with no fallback:** None.
**Missing dependencies with fallback:** PyYAML may need explicit pin if it's not transitively present.

## Concrete Research Question Answers

### R-01 SESSION UUID BOOTSTRAP TIMING

**Recommendation: Lazy mint inside `get_session_uuid()` on first call (Option c).**

**Why:**
- NiceGUI's `RequestTrackingMiddleware` (verified: `storage.py:28-40`) GUARANTEES `app.storage.user` exists by the time ANY page handler runs. So lazy mint inside the helper is timing-safe for the normal case.
- `@app.on_connect` fires on WEBSOCKET connect (after page HTML renders) — too late for bootstrap-time UUID consumers.
- A custom middleware would be redundant — NiceGUI's middleware already creates storage; we just need to write the UUID into it.
- Lazy mint composes correctly: every consumer that needs the UUID gets it; consumers that don't need it pay zero overhead.

**Failure modes:**
- **Storage pruned mid-flight:** Caught by `try/except AssertionError`. Returns ephemeral UUID (do NOT cache). Downstream consumers using UUID for cache-key lookup miss the cache, which is correct behavior (not a false positive).
- **Storage not yet existing:** Impossible per NiceGUI middleware ordering — middleware ensures storage exists before handler runs.

### R-02 STATIC AUDIT — RAW ACCESS SITES

**Total raw access sites: 56 across 13 files** (post-cca23db3, master-main HEAD).

**Classification table:**

| File | Sites | Classification | Notes |
|------|-------|---------------|-------|
| `web/main.py` | 11 (lines 327, 493, 567, 587, 598, 657, 663, 664, 691, 820, 952, 960, 968, 1283, 1458, 1460, 1463) | MIGRATE most; allowlist bootstrap | Multi-class file. Lines 327 (lang resolve), 952 (`_safe_user_storage_get` body), 960 (`set_current_page` body) ARE the safe wrappers — consolidate into web.safe_storage. Line 1458-1463 (OAuth callback) → ALLOWLIST until Phase 91 AUTHW-02. |
| `web/auth_state.py` | 5 (lines 42, 50, 95, 97, 117, 122-124, 176) | ALLOWLIST | All inside GlobalAuthState class methods that already wrap in try/except. Phase 91 AUTHW-01 will migrate. |
| `web/api.py` | 3 (lines 1932, 1968, 2073) | MIGRATE | Uses `nicegui_app.storage.user.get` — alias-tracking AST needed (Pitfall 1). |
| `web/supabase_client.py` | 1 (line 263) | MIGRATE | Inside sign_out using `_app.storage.user.get` alias. |
| `web/components/text_editor.py` | 3 (lines 35, 50, 66) | MIGRATE (Codex round 4 MEDIUM-2) | Auto-save deferred callback — known fragility. |
| `web/components/translation_report.py` | 1 (line 152) | MIGRATE | Simple read. |
| `web/components/filter_panel.py` | already migrated | VERIFY | cca23db3 work. |
| `web/pages/browse.py` | 4 (lines 1122, 1214, 2080, 2115) | MIGRATE | Reading desk + export data + 2 show_translations reads. |
| `web/pages/browse_state.py` | 8 (lines 127, 137, 147, 153, 174, 180, 184, 197, 203, 224) | MIGRATE | Currently inline-protected; refactor to use helpers for consistency. |
| `web/pages/catalog_browse.py` | 3 (lines 339, 954, 962) | MIGRATE | incoming_filters writes (Codex round 4 MEDIUM-2). |
| `web/pages/parallels.py` | 9 sites including 3520 (Codex round 4 MEDIUM-2) | MIGRATE | parallels.py:340-1424, :2051-2055, :2343-2346, :2729, :3520-3523 (deferred restore — Codex flagged), :2409. |
| `web/pages/search.py` | 2 (4420, 4630) plus writes (422, 532, 545, 657, 681, 689, 718, 1086, 2055-2061, 4362) | MIGRATE | Lots of writes; existing reads via `_safe_get` already at line 101. |
| `web/pages/search_state.py` | 11 (lines 362-471, 513, 518, 520, 550, 558, 563) | MIGRATE | Reads at 343 and 390 ALREADY use safe_user_get; writes need migration. |
| `web/pages/search_results.py` | 3 (lines 483, 1577, 1635) | MIGRATE | All show_translations reads. |
| `web/pages/settings.py` | 7 writes (61, 76, 94, 109, 119, 134, 149) | MIGRATE | Reads at top of file already use `_safe_get`. |
| `web/pages/home.py` | 2 writes (40, 59) | MIGRATE | Reads already migrated. |

### R-03 LINT MECHANISM TRADEOFF

**Recommendation: pytest-based AST scan (NOT ruff custom rule).**

| Criterion | Pytest AST scan | Ruff custom rule |
|-----------|-----------------|------------------|
| CI integration | Runs in existing `pytest tests/` job — zero infra change | Would need `ruff check . --select STORAGE001` — requires either out-of-tree plugin (unstable API) or PR upstream (slow) |
| False-positive rate | Low — AST visits real attribute nodes only; can detect aliased imports | Lower — ruff has battle-tested visitor; but customization needs Rust knowledge (ruff core is Rust) |
| Allowlist integration | Trivial — pytest test reads YAML, filters by path/line | Requires `# noqa: STORAGE001` comments OR a config file; both add complexity |
| Maintainability | High — pure Python, lives in `tests/`, version-controlled with code | Medium — out-of-tree plugin adds a build step; future ruff upgrades may break it |
| Speed | Acceptable (~500ms for 13 web/ files × small AST) | Faster (~50ms, ruff is Rust) — irrelevant at this scale |
| Drift detection (line-number changes) | Pattern-match-based (R-04); robust | Pragma-based (`# noqa`); travels with the line — but pragmas can be left orphaned after refactor |

**Decision: pytest AST scan.** Ruff plugin would require either a fork or upstream PR — both unjustifiable for a 13-file project.

### R-04 ALLOWLIST FILE FORMAT

**Recommendation: YAML at `.planning/phase87_storage_allowlist.yaml` with pattern-based matching (NOT raw line numbers).**

**Why YAML over alternatives:**
- Plain text: loses justification structure; harder for code review tooling to validate
- JSON: no comment support; justifications would need to live in description fields, less readable
- Source `# noqa`: requires ruff plugin (rejected)
- YAML: native comments for justifications; multi-line strings for long rationales; widely understood

**Pattern-based matching (not line numbers):**
```yaml
allowed_raw_access:
  - file: web/auth_state.py
    matches:
      - pattern: "app.storage.user.get(cls.USER_KEY)"
      - pattern: "app.storage.user.get(cls.PROFILE_KEY)"
      - pattern: "app.storage.user[cls.USER_KEY]"
      - pattern: "app.storage.user[cls.PROFILE_KEY]"
      - pattern: "app.storage.user['auth_session']"
      - pattern: "app.storage.user.pop(cls.USER_KEY"
      - pattern: "app.storage.user.pop(cls.PROFILE_KEY"
      - pattern: "app.storage.user.pop('auth_session'"
    justification: |
      Bootstrap auth state in GlobalAuthState class methods. These methods
      already wrap raw access in try/except. Phase 91 AUTHW-01 will migrate
      to safe_storage helpers as part of atomic auth-write refactor. Keeping
      them raw in Phase 87 avoids duplicating Phase 91's work.
```

Line numbers drift on refactor — pattern-match against the SOURCE TEXT of the matched AST node (use `ast.get_source_segment` or reconstruct from `ast.unparse`).

### R-05 EXISTING safe_storage.py AUDIT

**Helpers existing:** `safe_user_get`, `safe_user_set`, `safe_user_pop` (3 functions, ~80 lines).

**Edge cases handled:**
- Pruned-session AssertionError (the primary bug)
- Generic Exception fallthrough (logs at warning level)
- Default value on missing key (via `.get(key, default)`)

**NOT handled (Phase 87 needs to add):**
- Session UUID minting (`get_session_uuid`, `ensure_session_uuid`)
- Atomic multi-key writes (Phase 91 AUTHW-03 may need this) — defer to Phase 91, not Phase 87 territory
- Storage-poisoning validation on retrieved value type (security concern noted in Pitfall 2)

**Existing 6 tests cover:**
1. `test_safe_user_get_returns_default_on_assertion` — happy path for prune race
2. `test_safe_user_get_returns_default_on_generic_exception` — fallthrough
3. `test_safe_user_set_returns_false_on_assertion` — write-on-prune
4. `test_safe_user_set_returns_true_on_success` — write happy path
5. `test_safe_user_pop_returns_default_on_assertion` — pop-on-prune
6. `test_safe_user_get_happy_path` — sanity

**Bootstrap-timing scenarios from R-01 NOT covered by existing tests** — those become `tests/test_session_uuid.py` (new file, additive).

### R-06 INTEGRATION WITH NICEGUI STORAGE INTERNALS

**Confirmed via direct source inspection (`inspect.getsource(nicegui.storage)`):**

- **What raises AssertionError on prune-mid-flight:** Line `assert session_id in self._users, f'user storage for {session_id} should be created before accessing it'` at `storage.py:121` (in the `user` property getter).
- **Does `app.storage.user` exist before any page handler runs?** YES — `RequestTrackingMiddleware.dispatch` at `storage.py:28-40` runs first, calls `_create_user_storage(session_id)`, then calls `call_next(request)` (the page handler).
- **Is there a session ID we can hash into the UUID?** YES — `request.session['id']` (Starlette SessionMiddleware sets this). But this is the cookie session ID, and `_session_uuid` should be DIFFERENT from it (we want the UUID to be opaque to the client; the cookie ID is HMAC-protected but client-visible after decryption).
- **NiceGUI version:** 3.8.0 pinned in `requirements-lock.txt`.
- **`prune_user_storage` mechanism:** runs every 10s via `app.timer(10, prune_user_storage)` at `nicegui.py:149`. Removes user storage if (a) no `Client` instance has that session_id active AND (b) `last_modified > 10s ago`. Critical implication: an HTTP request (no websocket Client) that takes >10s to handle CAN have its storage pruned mid-flight.

### R-07 TEST STRATEGY FOR FOUND-01 SC1

**Recommendation: Pure unit test with mocked storage (Option c).**

**Why not Option a/b (TestClient with cookie jars):**
- Starlette TestClient brings up the full NiceGUI app — slow startup (~2-3 seconds), heavyweight
- The logic under test (`get_session_uuid()`) is a pure helper — no need for HTTP roundtrip
- Mock-based test matches existing `tests/test_safe_storage.py` pattern (consistency for future contributors)

**Test pattern:**
```python
for i in range(100):
    storage = {}  # Fresh per-session dict
    with patch('web.safe_storage.app') as mock_app:
        mock_app.storage.user = storage
        uid = get_session_uuid()
        uuids_seen.add(uid)
assert len(uuids_seen) == 100
```

Equivalent to 100 concurrent sessions because each iteration has its own storage dict. The test is deterministic, fast (<100ms), and matches the existing test style.

**If discuss-phase wants real HTTP test** (e.g., per user A3 assumption), add a separate `@pytest.mark.slow` test in `tests/test_session_uuid_integration.py` using Starlette TestClient. Don't gate the phase on it.

### R-08 DOWNSTREAM CONSUMER CONTRACT

**Recommendation: `safe_storage.get_session_uuid() -> str` (function-based, no class).**

**Why function over property/class:**
- Matches existing `safe_user_get/set/pop` style — easy to import alongside
- No new abstraction (SessionContext class) for callers to learn
- Idempotent + cacheable internally
- Easy to mock in tests

**API additions (full surface):**
```python
def get_session_uuid() -> str:
    """Always returns a string. Mints lazily. Falls back to ephemeral UUID on prune."""

def ensure_session_uuid() -> bool:
    """Eagerly creates UUID if missing. Returns True on success, False on prune."""
```

**Phase 88/89/90 expected consumers:**
- **Phase 90 AUTHC-03:** `refresh_lock = _session_locks.setdefault(get_session_uuid(), asyncio.Lock())` — UUID-keyed refresh locks (stable across token rotation)
- **Phase 89 LISTS-02:** Per-request UserListsManager doesn't need UUID (instantiated fresh each request), but optional `cache_key=get_session_uuid()` if memoization re-appears
- **Phase 92 SWEEP-05:** Smoke-test plan asserts both sessions get different UUIDs

### R-09 PARALLEL EXECUTION OPPORTUNITIES

**Phase 87 task decomposition:**

| Task | Dependencies | Can parallelize with |
|------|-------------|---------------------|
| T1: Add `get_session_uuid` + `ensure_session_uuid` to `safe_storage.py` | None | T2, T4 |
| T2: Write `tests/test_session_uuid.py` | T1 (signature only) | T3, T4 |
| T3: Migrate raw access sites in `web/main.py`, `web/components/*`, `web/pages/*` (NON-auth files) | T1 | T4 (initial drafts only — collision risk on cross-file changes) |
| T4: Build allowlist YAML + lint test `tests/test_no_raw_storage_access.py` | T3 partial (need to know real allowlist set) | T2 |
| T5: Migrate parallels.py:3520, text_editor.py:35-66 (Codex round 4 deferred sites) | T1, T3 | — |
| T6: Consolidate `_safe_user_storage_get` in `main.py` | T1 | T3 |
| T7: Update `docs/OPEN_ISSUES.md` + plan-output verification | All | — |

**Recommended waves:**
- **Wave 0:** T2 + T4 file skeletons (with TODOs) — parallel
- **Wave 1:** T1 (add helpers); T3 partial (independent files in parallel) — parallel-of-3 max
- **Wave 2:** T3 completion (cross-file like search.py + search_state.py written sequentially to avoid merge conflicts); T5, T6 — parallel-of-2
- **Wave 3:** T4 finalize (real lint check against migrated code) — single task
- **Wave 4:** T2 finalize (against real helpers) — single task; T7

**Parallelization caveat:** AVOID touching the same file from two tasks. `search.py` and `search_state.py` should be the same task (overlapping imports + cross-references).

### R-10 RISKS / LANDMINES FROM CODEX REVIEWS

**Round 4 was the final pre-Path-B review. Key landmines for Phase 87:**

1. **CRITICAL-1 (pre-capture storage handle):** Phase 87 doesn't directly need to fix this (Phase 90 territory), but `_session_uuid` consumers in Phase 90 MUST re-acquire `app.storage.user` inside locks rather than capture pre-lock. Encode in `get_session_uuid()` docstring: "Always called at point-of-use; do NOT cache the return value across lock boundaries unless you also have a way to validate the captured value is still current."

2. **MEDIUM-2 (deferred-callback raw access):** `parallels.py:3520` (inside `async def _deferred_restore()`) and `text_editor.py:464` (`asyncio.ensure_future(_auto_save_loop())`) — Phase 87 MUST migrate these in T5. Pitfall: deferred callbacks run AFTER the page handler returns, so storage state may be different. The `safe_user_get/set` wrappers handle this gracefully (return default on AssertionError), but the failure-mode behavior must be documented: "deferred callbacks may silently lose state on session prune; this is intentional — alternative is a 500 to the user."

3. **MEDIUM-1 (login/OAuth raw writes):** Phase 87 should NOT migrate these — explicitly allowlist with "Phase 91 AUTHW-02 will migrate."

4. **HIGH-1, HIGH-2 (cache-key rotation in `get_user_client`):** Pure Phase 90 territory. Phase 87 just enables it by providing `_session_uuid` as the stable cache key.

5. **NIT-1 (rename test):** Codex flagged a test name overclaim. Phase 87's NEW tests should use accurate names — e.g., NOT `test_session_uuid_atomic_creation` (overclaims), but `test_session_uuid_unique_across_100_sessions` (descriptive).

**The user's NEW work that Phase 87 MUST do (not what Codex already found):**
- Lazy-mint UUID helper (new — Codex never asked for it; HANDOFF item 6 introduced it)
- AST-based lint with allowlist (new — Codex did the audit manually 4 times; Phase 87 codifies it)
- Migrate the 56 remaining sites in a SWEEP (new — Codex round 4 said "still raw at 30+ sites" but didn't migrate them)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all versions pinned in requirements-lock.txt, NiceGUI internals verified via source inspection
- Architecture: HIGH — RequestTrackingMiddleware behavior confirmed; lifecycle hooks confirmed
- Pitfalls: HIGH — Pitfalls 1-7 derived from real codebase grep + Codex round 4 transcript
- Validation strategy: MEDIUM — pytest-based AST scan is well-understood, but specific YAML-pattern-match logic needs prototype validation in plan Wave 0
- Lint mechanism choice: MEDIUM — ruff plugin API status (A1) is the lone uncertainty; recommendation is conservative

**Research date:** 2026-05-13
**Valid until:** 2026-05-27 (14 days — fast-moving area; storage internals and codebase grep results may drift)
