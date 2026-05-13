---
phase: 88-state-separation-by-deletion
reviewed: 2026-05-13T00:00:00Z
depth: standard
files_reviewed: 13
files_reviewed_list:
  - tests/test_api_export_json.py
  - tests/test_api_legacy_unchanged.py
  - tests/test_export_cross_user_isolation.py
  - tests/test_export_state_selection.py
  - tests/test_no_appstate_export_fields.py
  - tests/test_no_deleted_state_references.py
  - web/api.py
  - web/export_state.py
  - web/pages/parallels.py
  - web/pages/search.py
  - web/pages/search_results.py
  - web/search_api.py
  - web/state.py
findings:
  critical: 0
  warning: 2
  info: 6
  total: 8
status: issues_found
---

# Phase 88: Code Review Report

**Reviewed:** 2026-05-13
**Depth:** standard
**Files Reviewed:** 13
**Status:** issues_found

## Summary

Phase 88 ("state separation by deletion") achieves its stated security objective. The
13 changed files cleanly migrate every writer/reader of the 10 deleted AppState
per-user mirror fields onto the per-session `web.export_state` payload routed through
the `safe_storage` chokepoint. The AppState class no longer carries any per-user
fields whose lifetime is request-scoped; what remains (`meta_mgr`, `var_mgr`,
`searcher`, etc.) is genuinely shared infrastructure.

**Security verdict (the headline concern):** the cross-user leak vector documented
in the 2026-05-12 incident is closed. The export handlers in `web/api.py` no longer
read from any process-wide singleton; both source_text paths now key exclusively on
the requesting cookie's session. The `parallels_source_text` reader-side fallback is
gone and the regression test (`test_parallels_source_text_cannot_leak_via_deleted_fallback`)
proves it on a POSITIVE export path (not just the 400-empty path).

**Defensive guards:** `web/export_state.py` getters/updaters now isinstance-guard
poisoned-shape payloads and copy-on-update before mutation. Coverage looks complete
for the 6 functions in that module.

**AST scanner robustness:** the static guard
(`tests/test_no_deleted_state_references.py`) tracks import aliases, chained
`module.state.field` access, and `setattr`/`getattr` with constant field names.
Found one notable gap (assignment-binding alias) -- see WR-01.

**Test coverage:** runtime + static guards cover all 10 deleted fields. The
sequential nature of the cross-user isolation test is acknowledged in its
docstring; true concurrent coverage is correctly deferred to Phase 92 SWEEP-05.

The findings below are non-blocking and most are stylistic.

## Warnings

### WR-01: AST scanner blind to assignment-bound state aliases

**File:** `tests/test_no_deleted_state_references.py:90-104`
**Issue:** `_DeletedStateAccessVisitor` extends `self.aliases` only via `visit_ImportFrom`.
A regression introduced through a plain assignment alias would bypass the scanner:
```python
from web.state import state          # canonical -- scanner catches s
s2 = state                            # NOT tracked -- visit_Assign is not overridden
s2.last_results = []                  # SCANNER FALSE NEGATIVE
```
Similarly, `web_state = state` (without using import-as) is also missed. The chained-attribute heuristic
(`<anything>.state.<field>`) catches some cases, but not the bare-name reassignment.
Since the scanner is a PERMANENT CI guard for a security-critical regression, the gap is worth
closing now -- a future contributor doing `s = state` to shorten a long block will silently disable
the guard for that file.

**Fix:** Add an `Assign` visitor that tracks `Name = Name-in-aliases` and `Name = Attribute-ending-in-.state`:
```python
def visit_Assign(self, node: ast.Assign):
    # Track: s = state, s = web_state.state, etc.
    if (isinstance(node.value, ast.Name) and node.value.id in self.aliases) or \
       (isinstance(node.value, ast.Attribute) and node.value.attr == 'state'):
        for target in node.targets:
            if isinstance(target, ast.Name):
                self.aliases.add(target.id)
    self.generic_visit(node)
```
Also add a seed-trap test (mirroring `test_scanner_catches_aliased_imports`) exercising
`s2 = state; s2.last_results = []`.

### WR-02: TOCTOU lost-update window in same-session concurrent payload patches

**File:** `web/export_state.py:74-91, 132-139`
**Issue:** The patch functions (`update_search_export_results`,
`update_search_export_selection`, `update_parallels_export_filtered`) follow
read-copy-write without serialization:
```python
payload = safe_user_get(_SEARCH_KEY, None)   # T0: read P0
if not isinstance(payload, dict): return
payload = dict(payload)                       # T1: copy
payload['results'] = results                  # T2: mutate
safe_user_set(_SEARCH_KEY, payload)           # T3: write
```
Two concurrent same-session requests (e.g., a slow `render_results` callback overlapping
with a checkbox toggle) can race:
- Request A: reads P0, copies, sets `results` → writes A.
- Request B: reads P0 (before A's T3), copies, sets `selected_uids` → writes B → A's `results`
  update is lost.

The docstring claims "copy-on-update defends against shared-reference races between
same-session requests," but that statement is narrow: it prevents *in-place mutation of a
dict another request is holding a reference to*, NOT lost-update via read-modify-write
interleaving. For the cross-USER case (the headline bug) this is irrelevant -- separate
sessions key on separate keys. For same-session concurrency it is a genuine but
low-impact data-loss window.

**Fix:** Either:
- (a) Document explicitly in the module docstring that updates are not serialized and
      the caller is responsible for ordering (acceptable for a single-user session);
- (b) Add a per-key lock (`threading.Lock` keyed off `safe_storage.get_session_uuid()`),
      acquired around the read-modify-write trio; or
- (c) Replace patch functions with full-payload setters at all call sites so the writer
      always has the fresh full payload in hand.

(a) is sufficient for v1 given NiceGUI's single-event-loop request handling typically
serializes same-session work, but make the assumption explicit.

## Info

### IN-01: Dead-on-arrival local variables in search.py migration

**File:** `web/pages/search.py:4093-4094, 4112, 4178-4179, 4202`
**Issue:** The writer-site migration introduced local variables that are assigned but
never read:
- Line 4093: `_current_search_query = clean_query` (never read; `clean_query` is passed to `set_search_export` directly)
- Line 4094: `_current_search_mode = mode` (never read)
- Line 4112: `_last_results = results` (never read)
- Line 4178: `_current_search_query = clean_query` (never read; happy path)
- Line 4179: `_current_search_mode = mode` (never read)
- Line 4202: `_last_results = results` (never read)

These appear to be leftover scaffolding from the mechanical `state.X = Y` →
`_X = Y` rewrite. The corresponding `set_search_export(...)` calls pass `clean_query`,
`mode`, and `results` directly, so the locals are dead.

**Fix:** Delete the 6 lines listed. The behavior is identical and the diff signals
intent more cleanly (no false "this name matters" cue to readers).

### IN-02: Unnecessary intermediate locals in history-restore branch

**File:** `web/pages/search.py:3782-3792`
**Issue:** `_results`, `_query`, `_mode`, `_filters_applied`, `_warnings` are each assigned
once and used exactly once on the next line in `set_search_export(...)`. `_gap` correctly
needs the local (try/except wrapper), but the others are unnecessary intermediates.
This is a style call -- not strictly dead, but adds noise.

**Fix:** Optional. If kept, leave as-is (the explicit local names document intent).
If cleaning up, inline all but `_gap`:
```python
set_search_export(
    results=state_snapshot['results'],
    query=entry.get('query', '') or '',
    mode=params.get('mode') or 'text',
    gap=_gap,
    filters=params.get('filters'),
    warnings=['restored-from-history'],
    selected_uids=None,
)
```

### IN-03: Stale line-number references in migration comments

**File:** `web/pages/search.py:3789-3790`, `web/pages/parallels.py:2305-2307`
**Issue:** Comments reference line numbers that drifted after the Phase 88 migration:
- `search.py:3789-3790` says "snapshot's filters dict already has the 10-key shape
  constructed at search.py:4232-4242" -- after deletion of the `state.X = ...` block,
  the actual filter-shape construction site is now around 4189-4200.
- `parallels.py:2305-2307` says "10-key shape as the live snapshot at
  parallels.py:2202-2213" -- that line range now points to a "no manuscripts match"
  early-return block, not the filter shape.

Line-number references in comments rot every refactor; they were already a code-smell
in the pre-Phase-88 source. Phase 88 inherits but does not introduce the smell.

**Fix:** Either delete the line-number citations (the surrounding prose is sufficient)
or replace with semantic anchors ("see the 10-key filter dict in the happy-path
completion handler below").

### IN-04: Empty bare `except Exception:` in parallels bootstrap

**File:** `web/pages/parallels.py:292-293, 315-316`
**Issue:** Both bootstrap blocks (active_snapshot restore and legacy-results restore)
wrap the entire payload-population block in `try: ... except Exception: pass`. While
the `pass # Snapshot restore failed; page falls back to empty` comment is informative,
the swallow is broad enough to hide bugs in `set_parallels_export` calls or in
`_active_snapshot.get(...)` chains. This matches the pre-existing pattern in the file,
so Phase 88 inherited rather than introduced the smell.

**Fix:** Optional. Narrow to `(KeyError, TypeError, AttributeError)` at minimum, or
log at debug level before the `pass` so an actual bug doesn't go silent:
```python
except Exception as e:
    logger.debug("parallels bootstrap restore failed: %s", e)
```

### IN-05: AST scanner chained-case docstring acknowledges false-positive risk but does not enforce filter

**File:** `tests/test_no_deleted_state_references.py:114-124`
**Issue:** The chained-attribute case (`<anything>.state.last_results`) is intentionally
conservative -- the comment says "False-positive risk: any object with a `.state.<field>`
chain matching. Acceptable in this codebase -- `.state` is overwhelmingly AppState."
This is a reasonable trade-off, but the test does not currently verify the assumption
empirically. A non-AppState `.state.last_results` reference (e.g., a Pydantic model
attribute) would silently fail the scanner.

**Fix:** Optional. Add a positive seed-trap that asserts the scanner DOES catch
chained access on a known-AppState binding, plus a negative seed-trap that exercises a
distinct `.state.X` chain to confirm the false-positive surface is bounded. Today
both behaviors are inferred from code review only.

### IN-06: Comment refers to deleted "Phase 77 (D-06)" semantics

**File:** `web/pages/parallels.py:2326`
**Issue:** Inline comment `'boundary_options': None,  # Phase 77: not yet exposed as
user-settable; placeholder for parity with /api/parallels API-02` references a deferred
Phase 77 commitment. Not a Phase 88 regression, but the `_parallels_search_meta`
construction now lives in a Phase-88-renamed block; the comment is mildly
context-confusing. Same for line 2327 (`# Phase 78 will populate`).

**Fix:** No action required; flagged for awareness when Phase 78 or the boundary-
options feature actually lands.

---

_Reviewed: 2026-05-13_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
