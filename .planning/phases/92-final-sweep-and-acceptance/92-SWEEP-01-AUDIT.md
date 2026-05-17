# Phase 92 SWEEP-01 -- 5-Surface Multitenant Audit

**Plan:** 92-01
**Date:** 2026-05-17
**Auditor:** Claude (executor)
**Status:** Complete

## Scope

Per Phase 92 D-03 (Gemini round-1 CRITICAL catch), multitenant safety extends beyond `app.storage.user` -- it covers any global state that touches disk or external APIs in a process shared across user sessions. This audit memo covers 5 surfaces:

1. `app.storage.user` in `web/` (the Phase 87 chokepoint scope)
2. `app.storage.browser` (NiceGUI cookie-backed store)
3. `app.storage.client` (NiceGUI connection-scoped store)
4. `shared/puzzle_service.py` + `joins.db` (local SQLite sidecar singleton)
5. `web/analytics.py` PostHog client (server-side analytics surface)

Also appended:
- SWEEP-02 spot-check: parallels.py:3520 + text_editor.py auto-save
- SWEEP-03 allowlist re-audit: post-Phase-91 empty state

## Surface 1: app.storage.user

**Audit method:** Independent AST walker scan of `web/` recursively, identifying `<app_alias>.storage.user.{get,pop}` and `<app_alias>.storage.user[...]` and bare `<app_alias>.storage.user` access. Reproduces `tests/test_no_raw_storage_access.py` logic standalone but bypasses the allowlist filter to produce raw unfiltered counts. Companion literal-grep evidence per Codex CM3 archived alongside.

**Evidence files:**
- `92-SWEEP-01-AST-SCAN.txt` (AST evidence — executable accesses only)
- `92-SWEEP-01-GREP.txt` (literal-grep evidence per CM3 — includes documentation references)

**Result (AST scan):** 0 raw `app.storage.user` accesses found in `web/` excluding `web/safe_storage.py`.

**Result (literal grep):** 36 total textual hits across `web/`; classification:
- 8 hits in `web/safe_storage.py` (the chokepoint — these are the canonical accesses)
- 10 hits in single-line comments (`# ...` referring to the legacy raw-access pattern in documentation)
- 18 hits inside module/class/function docstrings (e.g., `"""Uses NiceGUI's app.storage.user for persistence."""`)
- **0 hits in executable code outside `web/safe_storage.py`**

**Divergence note (AST vs. literal grep):** The plan author originally expected ALL literal-grep hits to fall inside `web/safe_storage.py`. In practice, 28 textual references to `app.storage.user` survive in `web/` outside `safe_storage.py` — every one of them in a COMMENT or DOCSTRING describing the migration (e.g., `# Phase 91 AUTHW-01: chokepoint helpers replace raw app.storage.user access`). These are documentation references, not executable accesses. The AST scan and the lint scanner (`tests/test_no_raw_storage_access.py`) BOTH report 0 executable violations — they agree, the literal grep does not contradict that verdict, the divergence is purely between "textual substring search" and "AST-aware code search." The artifact `92-SWEEP-01-GREP.txt` includes a per-line `[CODE|comment|docstring]` classification so the agreement is verifiable line-by-line.

**Files scanned:** 65 (`.py` files under `web/`)
**Repo HEAD at scan time:** b94eed74

**VERDICT: clean.** The Phase 87 lint scanner's enforcement is independently confirmed. Every per-user state access in `web/` routes through `web/safe_storage.py` helpers (`safe_user_get` / `safe_user_set` / `safe_user_pop` / `get_session_uuid` / `ensure_session_uuid`). The `.planning/phase87_storage_allowlist.yaml` is at final state `allowed_raw_access: []`.

## Surface 2: app.storage.browser

**Audit method (per Codex CM2 -- belt-and-suspenders):** TWO complementary scans:
1. **Literal grep** of `web/` for the substring `app.storage.browser` (matches the original CONTEXT.md wording + ROADMAP-style literal evidence)
2. **AST alias-aware scan** of `web/` using the same `_find_app_aliases` helper pattern from `tests/test_no_raw_storage_access.py` (catches aliases like `from nicegui import app as nicegui_app; nicegui_app.storage.browser.get(...)` that literal grep would miss)

NiceGUI's `app.storage.browser` is a cookie-backed per-browser store -- cookies are browser-scoped, not Python-process-scoped, so there is NO in-process cross-user leak risk by construction.

**Literal-grep call sites found (7 total):**

```
web/auth_state.py:31:  # `app.storage.browser.*` (lines ~382, 383, 408, 409, 411, 412) for the   [COMMENT]
web/auth_state.py:391: saved_email = app.storage.browser.get(REMEMBER_EMAIL_KEY, '')
web/auth_state.py:392: was_checked = app.storage.browser.get(REMEMBER_CHECKED_KEY, False)
web/auth_state.py:417: app.storage.browser[REMEMBER_EMAIL_KEY] = login_email.value
web/auth_state.py:418: app.storage.browser[REMEMBER_CHECKED_KEY] = True
web/auth_state.py:420: app.storage.browser.pop(REMEMBER_EMAIL_KEY, None)
web/auth_state.py:421: app.storage.browser.pop(REMEMBER_CHECKED_KEY, None)
```

**AST alias-aware call sites found (6 total -- 6 executable; the line-31 hit is a comment, not an AST node):**

```
AST: web/auth_state.py:391: app.storage.browser
AST: web/auth_state.py:392: app.storage.browser
AST: web/auth_state.py:417: app.storage.browser
AST: web/auth_state.py:418: app.storage.browser
AST: web/auth_state.py:420: app.storage.browser
AST: web/auth_state.py:421: app.storage.browser
```

**Agreement check:** Literal-grep returns 7 hits (6 code + 1 documentation comment at line 31). AST scan returns the same 6 executable accesses. The two methods agree on the set of executable accesses, and the AST scan correctly ignores the comment-only mention. CM2's belt-and-suspenders rationale is satisfied: no alias-only code path was missed by literal grep (because all 6 sites use the bare `app.storage.browser` name with no alias rebinding).

**Known site:** `web/auth_state.py:create_login_dialog` (lines 391-421) -- "Remember me" checkbox persistence (saved email + boolean checked flag, not PII beyond the user's own email). This was the Phase 91 NEW-H2 deviation documented in `.planning/STATE.md`.

**PII assessment:** Each call site reviewed:

- `web/auth_state.py:391` -- `saved_email = app.storage.browser.get(REMEMBER_EMAIL_KEY, '')` -- read of saved email for "Remember me" UI re-fill. **Per-browser scope (cookie-backed); the email belongs to the browser-owning user; no cross-user-in-same-process leak surface.**
- `web/auth_state.py:392` -- `was_checked = app.storage.browser.get(REMEMBER_CHECKED_KEY, False)` -- read of boolean "Remember me" toggle state. **Boolean preference, no PII.**
- `web/auth_state.py:417` -- `app.storage.browser[REMEMBER_EMAIL_KEY] = login_email.value` -- write of saved email when user opts in. **User's own email written to their own browser cookie store; not cross-user accessible.**
- `web/auth_state.py:418` -- `app.storage.browser[REMEMBER_CHECKED_KEY] = True` -- write of toggle state. **Boolean preference.**
- `web/auth_state.py:420` -- `app.storage.browser.pop(REMEMBER_EMAIL_KEY, None)` -- clear of saved email when user opts out. **Cookie pop; per-browser scope.**
- `web/auth_state.py:421` -- `app.storage.browser.pop(REMEMBER_CHECKED_KEY, None)` -- clear of toggle state. **Boolean preference.**

**VERDICT: documented (D-10).** All call sites carry the browser-owning user's own email or a boolean preference; no cross-user data exposure because cookies are by definition browser-scoped, not process-scoped. Per Phase 92 D-10: documented but NOT lint-enforced -- the Phase 87 scanner does not cover this surface and that is the correct posture. MULTITENANT.md §8 will explicitly note this surface as "outside the Phase 87 scanner; manually audit on any new code touching it."

## Surface 3: app.storage.client

**Audit method (per Codex CM2 -- belt-and-suspenders):** TWO complementary scans:
1. **Literal grep** of `web/` for the substring `app.storage.client`
2. **AST alias-aware scan** (same `_find_app_aliases` pattern; visitor scoped to `<alias>.storage.client.*` Attribute/Subscript nodes)

NiceGUI's `app.storage.client` is connection-scoped (per-WebSocket-connection) -- not the same surface as `app.storage.user`.

**Literal-grep call sites found (0 total):** No `app.storage.client` literal matches in `web/`.

**AST alias-aware call sites found (0 total):** No `app.storage.client` AST nodes in `web/`.

**Agreement check:** Literal-grep and AST scan agree: zero call sites of any kind. The surface is genuinely unused in `web/`.

**Per-call-site analysis:** N/A -- no call sites to analyze.

**VERDICT: clean.** No surface to audit -- the codebase does not use `app.storage.client` at all. If a future contributor introduces a call site, MULTITENANT.md §8 documents the audit expectation.

## Surface 4: shared/puzzle_service.py + joins.db

**Audit method (per Codex CH6 -- live DB inspection MANDATORY, not just source):** THREE complementary checks:
1. Read SQLite **source schema** in `shared/puzzle_service.py` (the `CREATE TABLE IF NOT EXISTS` statements that the code path runs at init)
2. Inspect the **live DB at `joins_data/joins.db`** via `PRAGMA table_info(join_documents)` and `PRAGMA table_info(join_document_fragments)` -- this catches divergence from column-add migrations that may exist outside the source CREATE
3. Check `PuzzleService` class for module-level singletons holding per-user state

### (1) Source schema (from `shared/puzzle_service.py` CREATE TABLE statements)

```sql
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS join_documents (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    join_type TEXT NOT NULL DEFAULT 'uncertain'
        CHECK (join_type IN ('physical', 'content', 'uncertain')),
    fragments_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS join_document_fragments (
    doc_id TEXT NOT NULL,
    fl_id TEXT NOT NULL,
    sys_id TEXT NOT NULL,
    FOREIGN KEY (doc_id) REFERENCES join_documents(id) ON DELETE CASCADE
);
```

**Per-user ownership columns found (source schema):** None. Regex over `shared/puzzle_service.py` for `(user_id|owner_id|created_by|user_uuid)` returns 0 hits.

### (2) Live DB inspection (`joins_data/joins.db`)

**Live DB state at audit time:** ABSENT at audit time -- the worktree (and the main checkout at b94eed74) does not have `joins_data/joins.db` or any equivalent at `joins.db` / `puzzle_data/joins.db`. This is consistent with a fresh checkout: the SQLite file is generated on first puzzle save (the CREATE TABLE IF NOT EXISTS pattern in `PuzzleService.__init__`); committing the DB to git is not the project convention (it's a runtime artifact like `Genizah_Index/`). Falling back to source schema verdict.

**`PRAGMA table_info(join_documents)`:**

```
N/A -- live DB file absent at audit time.
```

**`PRAGMA table_info(join_document_fragments)`:**

```
N/A -- live DB file absent at audit time.
```

**Per-user ownership columns found (live DB):** N/A (DB absent). The source `CREATE TABLE IF NOT EXISTS` statements are the only paths that create the schema; there are no `ALTER TABLE` migrations elsewhere in `shared/puzzle_service.py` (verified by regex `ALTER TABLE` over the file -- 0 hits), so the source schema IS the canonical schema for any DB this project creates from the current code.

### (3) Module-level state surface

`PuzzleService` -- instantiated per call (no module-level singleton in `shared/puzzle_service.py`; the `web/pages/puzzle.py` page calls `PuzzleService(db_path)` on demand). The class holds a single `sqlite3.Connection` instance scoped to the constructed object. The connection is process-shared in the sense that any caller that constructs `PuzzleService(<same path>)` opens a connection to the same on-disk file -- but there is no per-user data in that file by construction (Step 1 + Step 2 verify this), so the process-wide nature of the connection is not a multitenant leak vector.

### Verdict

**VERDICT: N/A community-share (D-04).** `joins.db` is the desktop/offline copy of community-share puzzle data. Per-user puzzle ownership lives in Supabase (RLS-protected cloud DB) -- NOT in the local sidecar. The local joins.db has no per-user columns -- confirmed via source CREATE only (live DB absent at audit time; source schema is the canonical schema because there are no `ALTER TABLE` migrations elsewhere in the module). No `user_id` / `owner_id` / `created_by` / `user_uuid` columns appear in either `join_documents` or `join_document_fragments` in the source schema. Smoke-test scenario R3 (concurrent puzzle write) is therefore marked **N/A** in `92-SWEEP-05-SMOKE.md` per D-08.

**Codex CH6 closed:** Source schema agrees on no per-user columns. The live DB is absent at audit time (the source-schema fallback path documented in the plan's Task 2 Step 1(b) applies). If a future phase adds per-user ownership to local joins.db, this audit re-opens.

## Surface 5: web/analytics.py PostHog client

**Audit method:** Read `web/analytics.py:posthog_capture` source; verify zero server-side per-user state caching (no module-level `distinct_id` cache, no Python-side state attached to user identifiers).

**Function source (excerpt):**

```python
def posthog_capture(event: str, properties: dict = None):
    """Send a custom PostHog event from the server side via JS injection.

    Safe to call even if PostHog isn't loaded (no-ops gracefully).
    Properties are JSON-serialized and passed to posthog.capture().
    """
    import json
    props_js = json.dumps(properties or {})
    try:
        ui.run_javascript(
            f"if(window.posthog)posthog.capture('{event}',{props_js})"
        )
    except Exception:
        pass  # No client connection or PostHog not loaded
```

**Server-side state assessment:** The function is purely a `ui.run_javascript(...)` injection that calls the BROWSER-loaded PostHog client. No Python-side state is created or cached. The `event` string and `properties` dict are JSON-serialized into the injected JS and forwarded to `posthog.capture()` which runs in the user's browser. The PostHog `distinct_id` is browser-resolved client-side (the JS client maintains its own per-browser cookie for `distinct_id`). The Python module has no module-level cache of `distinct_id`, `user_id`, or `session_id`. The exception handler silently no-ops on disconnected clients (no log, no state mutation).

**VERDICT: clean.** `posthog_capture` is `ui.run_javascript(...)` injection only -- the PostHog client is loaded into the BROWSER (per-tab) and called from the browser. There is no Python-side caching of per-user identifiers. The PostHog `distinct_id` is browser-resolved client-side. No cross-user attribution risk in the Python process.

---

## Appendix A: SWEEP-02 Spot-Check

**Requirement:** Confirm `parallels.py:3520` (deferred-restore callback flagged in Codex round 4 MEDIUM-2) and `text_editor.py` (auto-save callbacks flagged in Plan 87-03) are migrated to `safe_storage` helpers.

**Method:** Grep each file for `safe_user_*` usage AND raw `app.storage.user` usage. Both must show `safe_user_* > 0` and `app.storage.user == 0` (executable code; documentation comments allowed).

**`web/pages/parallels.py`:**
- `safe_user_*` calls: 40 (sites at L349, 353, 355, 385, 392, 396, 465, 889, 935-944, 1425-1430, 2053-2057, 2336, 2339, 2400, 2718, 3513-3516)
- raw `app.storage.user` accesses (executable): 0
- raw `app.storage.user` mentions (comments only): 1 (L326: `# NiceGUI query params are available via app.storage.user or client` -- documentation reference)
- Specific Codex MEDIUM-2 site (~lines 3510-3530, deferred-restore callback): `safe_user_get` confirmed at L3513-3516 (`stored_refs = safe_user_get('filter_sources_refs', [])`, `stored_enabled = set(safe_user_get('filter_sources_enabled', []))`, `stored_custom = safe_user_get('filter_sources_custom', {})`, `filter_sources['custom_count'] = safe_user_get('filter_sources_custom_count', 0)`)

**`web/components/text_editor.py`:**
- `safe_user_*` calls: 5 (sites at L17 import, L36, L51, L67)
- raw `app.storage.user` accesses (executable): 0
- raw `app.storage.user` mentions (comments only): 0
- Specific Plan 87-03 auto-save sites: `safe_user_set(LOCAL_EDITS_KEY, edits)` confirmed at L51 and L67; `safe_user_get(LOCAL_EDITS_KEY, {})` at L36; import at L17 (`from web.safe_storage import safe_user_get, safe_user_set`)

**VERDICT: SWEEP-02 satisfied.** Both files use the `safe_storage` chokepoint exclusively; no raw `app.storage.user` accesses (executable) remain in either file. The one comment-only reference in `parallels.py:326` is a documentation note about NiceGUI query parameter resolution and is correctly not a code access.

## Appendix B: SWEEP-03 Allowlist Re-Audit

**Requirement:** Phase 87 allowlist re-audited; every remaining entry has explicit justification; new entries require code-review approval.

**Method:** Read `.planning/phase87_storage_allowlist.yaml`; verify the `allowed_raw_access` field is the empty list; verify the preamble comment documents the 4 -> 3 -> 2 -> 0 progression; run `tests/test_no_raw_storage_access.py::test_allowlist_well_formed` to confirm the scanner accepts the empty state.

**State:**
- `allowed_raw_access: []` (final empty state after Phase 91 self-elimination)
- Preamble comment: present, 27 comment lines documenting the 4 -> 0 progression across phases 87/88/90/91
- `test_allowlist_well_formed`: PASS (`pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed -v` exits 0)

**VERDICT: SWEEP-03 satisfied.** The allowlist is at its terminal state (no exemptions). Any new raw access would require a PR re-adding an allowlist entry with justification AND expected_count (H1 schema). The Phase 87 lint scanner is the live enforcement layer.

---

*Generated by Plan 92-01 Task 2*
*Source-of-truth: `.planning/phases/92-final-sweep-and-acceptance/92-CONTEXT.md` D-03 (5-surface scope), D-04 (joins.db community-share), D-10 (storage.browser/client documentation-only)*
