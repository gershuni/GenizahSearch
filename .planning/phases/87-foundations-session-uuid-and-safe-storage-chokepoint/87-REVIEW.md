---
phase: 87-foundations-session-uuid-and-safe-storage-chokepoint
reviewed: 2026-05-13T00:00:00Z
depth: standard
files_reviewed: 19
files_reviewed_list:
  - web/safe_storage.py
  - web/main.py
  - web/api.py
  - web/supabase_client.py
  - web/components/text_editor.py
  - web/components/translation_report.py
  - web/pages/home.py
  - web/pages/settings.py
  - web/pages/search_results.py
  - web/pages/browse.py
  - web/pages/browse_state.py
  - web/pages/catalog_browse.py
  - web/pages/parallels.py
  - web/pages/search.py
  - web/pages/search_state.py
  - tests/test_session_uuid.py
  - tests/test_no_raw_storage_access.py
  - tests/test_browse_state.py
  - tests/test_search_state.py
findings:
  critical: 0
  warning: 0
  info: 6
  total: 6
status: clean
---

# Phase 87: Code Review Report

**Reviewed:** 2026-05-13
**Depth:** standard
**Files Reviewed:** 19
**Status:** clean (no Critical or Warning issues; six Info-level observations)

## Summary

Phase 87 establishes `web/safe_storage.py` as the single chokepoint for
`app.storage.user` access across the web app, introduces a CSPRNG-backed
`_session_uuid` minted lazily via `get_session_uuid()` / eagerly via
`ensure_session_uuid()`, and migrates 131 raw access sites to the helper API.

The chokepoint module is correct and defensive. `_is_valid_uuid()` correctly
rejects all enumerated poisoning vectors (uppercase hex, non-string types,
malformed length, non-hex chars) with `^[0-9a-f]{32}$` `fullmatch`. The lazy
mint path through `uuid.uuid4().hex` is the only mint path. Read/write helpers
absorb AssertionError under prune-race and return safe defaults; broader
Exception is also caught with a warning-level log.

The B1 bootstrap wiring is complete: every `@ui.page` handler in `web/main.py`
either calls `create_layout()` (which begins with `ensure_session_uuid()` on
L346) or calls `ensure_session_uuid()` directly (`/reset-hints` L1251 and
`/auth/callback` L1413). The only exempt route is `/privacy-extension`
(L1214), AST-confirmed to perform zero storage access. The lint test
`test_every_ui_page_handler_mints_uuid` enforces this invariant going forward.

The AST lint scanner in `tests/test_no_raw_storage_access.py` correctly
implements the B2 chain semantics (inner-first `['user', 'storage']` ordering)
and the H1 expected-count schema. Production scan returns zero violations
outside the four allowlisted entries. The allowlist itself is well-scoped:
each entry is genuinely load-bearing for an explicitly deferred phase
(88/90/91), each pattern has a precise `expected_count`, and the
`test_allowlist_counts_exact` test prevents silent expansion.

All 38 tests under the four new test files plus `tests/test_safe_storage.py`
(SHA-256 prefix `e165bf0e` preserved per FOUND-05) pass. Test quality is
high: tests exercise real chokepoint behavior with `unittest.mock.patch` on
`web.safe_storage.app`; the synthetic-violation test guards against the B2
chain-order regression; the parent-tracking double-report test
covers the four AST shapes (Call.func, Subscript-read, Subscript-assign,
bare Attribute) at four distinct line numbers; the `test_search_state.py`
suite correctly dual-patches both `web.pages.search_state.app` (for tab
storage) and `web.safe_storage.app` (for user storage chokepoint).

No security regressions found. The UUID is treated as a session secret per
T-87-03 (no logging, no URLs, no telemetry without HMAC). Cross-user leak
protection in `supabase_client.get_user_client` was already addressed by the
Codex round 4 CRITICAL-1 fix (cache key bound to `access_token`, not
`id(storage)`); the lingering captured-handle pattern at L111 is explicitly
allowlisted with a Phase 90 deletion contract.

The Info-level items below are documentation drift between the review prompt
and the migrated source, and minor stylistic notes — none are defects.

## Info

### IN-01: Prompt line numbers drifted from migrated source

**File:** `web/main.py:346` (prompt said L349); `web/main.py:1251` (prompt said L1288); `web/main.py:1413` (prompt said L1450)
**Issue:** The review brief cites `ensure_session_uuid()` call sites at
L349 (create_layout), L1288 (reset_hints_route), L1450 (auth_callback_route).
Actual locations are L346, L1251, and L1413. The wiring itself is correct
and tested — only the prompt's line numbers are stale.
**Fix:** Cosmetic only — when next updating the Phase 87 plan docs or the
handoff note, refresh line numbers from the post-merge tree:
```bash
grep -n "ensure_session_uuid()" web/main.py
```

### IN-02: `safe_user_set('current_page', ...)` precedes `ensure_session_uuid()` in every page route

**File:** `web/main.py:938, 998, 1025, 1044, 1119, 1160, 1174, 1258, 1272, 1290, 1304, 1322, 1336, 1350, 1369, 1389`
**Issue:** Every `@ui.page` handler writes `current_page` via `safe_user_set`
BEFORE invoking `create_layout()` (which calls `ensure_session_uuid()` as its
first action at L346). Under prune-race the `current_page` write may silently
fail (safe_user_set returns False) while the subsequent
`ensure_session_uuid()` inside `create_layout()` may also fail to mint. The
page still renders (helpers degrade gracefully) but neither the navigation
breadcrumb nor the UUID is stamped on that request. Subsequent requests
will re-attempt minting. This is not a defect for Phase 87 because no
downstream code currently reads `_session_uuid` directly from storage —
they all go through `get_session_uuid()` which mints lazily on miss. But
when Phase 88+ introduces consumers that DO read storage directly, those
consumers will not be helped by the current `safe_user_set('current_page')`
preamble racing ahead of the mint.
**Fix:** Defer until Phase 88: when any code reads `_session_uuid` from
storage directly, restructure the route-handler preamble so
`ensure_session_uuid()` runs FIRST, before the `safe_user_set('current_page')`
write. Alternatively, hoist `ensure_session_uuid()` into an `app.middleware`
or NiceGUI startup hook that runs unconditionally before every page handler.

### IN-03: `safe_user_get`'s broad `except Exception` masks programming bugs

**File:** `web/safe_storage.py:58-60, 71-73, 83-85`
**Issue:** Each of `safe_user_get`, `safe_user_set`, `safe_user_pop`
has a broad `except Exception as e: logger.warning(...); return default`
fallback. This is intentional — under no circumstance should a storage
helper bubble an exception during page render. But the contract is wider
than strictly necessary: a `TypeError` from `app.storage.user[True]` (e.g.,
unhashable key passed in) or any other programmer error in caller code
silently returns the default, with only a warning-level log. Combined with
the broad `except Exception: pass` patterns at most call sites, real bugs
can be hidden from CI.
**Fix:** Acceptable for v1. For follow-up: consider narrowing to
`except (AssertionError, KeyError, AttributeError, RuntimeError) as e`,
which covers all observed prune-race and NiceGUI-internal failure modes
without absorbing `TypeError` / `NameError` indicating caller bugs.

### IN-04: Captured-handle pattern in `supabase_client.get_user_client` remains live for Phase 87

**File:** `web/supabase_client.py:111, 147, 160`
**Issue:** The allowlisted `storage = _app.storage.user` at L111 captures
a dict-like reference to the per-session storage object. Subsequent
operations at L115 (`storage.get(...)`), L147 (`storage.get(...)`), L160
(`storage['auth_session'] = {...}`), and L176 (`_clear_stale_auth(storage)`)
operate on that captured handle. If `prune_user_storage` GC's the underlying
`FilePersistentDict` between L111 and any of the downstream operations,
the writes go to a dangling dict and the reads return stale data. The
Codex round 4 CRITICAL-1 fix correctly addressed the cache-key risk
(access_token, not id(storage)), but the dangling-handle risk on the
storage object itself is still present. The allowlist YAML explicitly cites
Phase 90 AUTHC-01 as the planned deletion of `get_user_client()` entirely,
which self-eliminates this risk. For Phase 87, the captured handle is
correctly identified, scoped via `enclosing: get_user_client`, and
`expected_count: 1` prevents silent expansion.
**Fix:** No action this phase. When Phase 90 lands, verify
`get_user_client()`, `_client_cache`, `_session_locks`, `_locks_guard`,
and `_CLIENT_CACHE_TTL` are deleted in one atomic commit so the allowlist
entry self-removes.

### IN-05: `app.storage.tab` accesses in `search_state.py:248` and `search.py:195, 203` are correctly outside scope

**File:** `web/pages/search_state.py:248`; `web/pages/search.py:195, 203`
**Issue:** Three direct `app.storage.tab` accesses remain in the migrated
files. The AST scanner's `_matches_storage_user_access` only matches the
inner-first chain `['user', 'storage']`, so `app.storage.tab` is correctly
not flagged. This is the intended scope — tab storage is a separate
NiceGUI chokepoint with different prune semantics. Worth noting because
a future safe-storage extension to `app.storage.tab` would need to be
explicit (the current chokepoint does NOT cover it).
**Fix:** No action. If Phase 88+ ever introduces a per-tab UUID or a
`safe_tab_get/set`, the AST scanner's chain check will need parallel
wiring (`['tab', 'storage']`).

### IN-06: Class B preserved wrappers do their job and are well-documented

**File:** `web/pages/browse_state.py:187-213`; `web/pages/search_state.py:361-384, 398-417`
**Issue:** The M3 audit correctly preserves Class B `try/except Exception:`
wrappers in `persist_browse_snapshot`, `restore_search_snapshot`, and
`persist_search_snapshot`. Each preserved wrapper covers dict construction
on potentially-malformed `state.*` attributes, list-comprehension over
`reading_desk_entries` / `refinement_chain`, conditional logic, and
sub-call dispatch (`restore_search_active_snapshot`,
`persist_search_active_snapshot`). The chokepoint would NOT absorb
those non-storage-related exceptions — `safe_user_set` only catches
inside its single dict-assignment statement, not across the whole
caller block. The preserved wrappers are load-bearing, not masking bugs
the chokepoint would otherwise surface. The inline comments correctly
distinguish "Class A collapsed" from "Class B preserved" at each site,
giving future maintainers explicit guidance.
**Fix:** No action. Acceptance criterion met — Class B preservation is
justified and documented.

---

## Notes on Each Focus Area

### 1. Correctness of safe_storage chokepoint — PASS

- `ensure_session_uuid()` is idempotent: read existing → validate → return
  True if well-formed; mint+write only on miss/poison; returns False only
  on storage write AssertionError. Verified by
  `test_ensure_session_uuid_idempotent` and
  `test_ensure_session_uuid_returns_false_on_assertion`.
- `_is_valid_uuid()` correctly rejects: uppercase hex (regex is lowercase
  only), non-string types (`isinstance` guard), wrong length, non-hex chars,
  whitespace. Verified by `test_session_uuid_rejects_uppercase_hex`,
  `test_session_uuid_rejects_non_string`, `test_session_uuid_rejects_malformed_length`.
- All three helper paths (`safe_user_get/set/pop`) plus the two UUID
  functions absorb AssertionError → debug log; broader Exception → warning
  log. No exception escapes.
- The fallback path in `get_session_uuid` when storage read raises returns
  `_uuid.uuid4().hex` (ephemeral, not cached). Documented in docstring
  L107-111 — callers using the fallback UUID against a cache will simply
  miss, which is correct.

### 2. Migration integrity — PASS

- Inspected git diff for `573e14c8` (browse_state) and `9069e94d`
  (search_state): every `.get(k, d)` raw read became `safe_user_get(k, d)`;
  every `[k] = v` raw write became `safe_user_set(k, v)`; every
  `.pop(k, d)` became `safe_user_pop(k, d)`. No `[k]` (KeyError-raising)
  reads were silently converted to `.get()` (default-returning) semantics.
- Class A wrapper collapses verified: each was a `try/except (AssertionError,
  Exception)` around a single storage call with a default-fallback body.
  Those exception handlers were exactly what `safe_user_*` re-implements
  centrally, so collapsing is semantically equivalent.
- M2 independent-read invariant verified in `restore_browse_snapshot`:
  `browse_position` and `reading_desk_state` reads are sequential, not
  short-circuited. Same for `search_results` / `domain_exclusions` /
  `search_refinement_chain` / `search_exclusion_sources` in
  `restore_search_snapshot`.

### 3. B1 wiring completeness — PASS

- All 19 `@ui.page` decorators identified by `grep` in `web/main.py`. 17
  call `create_layout()` (which begins with `ensure_session_uuid()` at
  L346). The 2 that don't are `/reset-hints` (L1248-1254, calls
  `ensure_session_uuid()` directly at L1251) and `/auth/callback` (L1406-1488,
  calls `ensure_session_uuid()` directly at L1413).
- The single exempt route `/privacy-extension` (L1214-1246) is AST-confirmed
  to have zero `safe_user_*` or `app.storage.*` references in its body
  — it is a pure static info page.
- Regression guard: `test_every_ui_page_handler_mints_uuid` parses
  `web/main.py` and asserts every non-exempt page handler contains either
  `create_layout(` or `ensure_session_uuid(` in its source. Any new
  `@ui.page` that touches storage without one of these patterns fails CI.

### 4. Allowlist accuracy — PASS

- 4 entries, 13 patterns, 14 expected_count nodes (1+1+2+1+1+1+1+1 for
  auth_state.py + 1+1+1 for main.py + 1 for supabase_client.py + 1 for
  export_state.py).
- `web/auth_state.py`: deferred to Phase 91 AUTHW-01 atomic auth-write
  refactor. Each pattern is a distinct read/write of USER_KEY, PROFILE_KEY,
  or auth_session in `GlobalAuthState` methods.
- `web/main.py`: 3-key atomic OAuth callback writes at L1429, L1431, L1434.
  Each `[GlobalAuthState.USER_KEY]`, `[GlobalAuthState.PROFILE_KEY]`,
  `['auth_session']` is in the `complete_login` block. Deferred to Phase 91
  AUTHW-02 with test coverage in `test_auth_callback_resilience.py`.
- `web/supabase_client.py`: captured-handle pattern at L111
  (`storage = _app.storage.user`). The AST scanner records only the bare
  Attribute access, not the full assignment statement, so the pattern
  string `_app.storage.user` is substring-matched. `enclosing: get_user_client`
  prevents bare `_app.storage.user` elsewhere in the file from being
  silently legalized. Self-eliminating in Phase 90 AUTHC-01.
- `web/export_state.py`: production fallthrough `return app.storage.user`
  inside `_backend()` for `_TEST_BACKEND` shim. Self-eliminating in Phase 88
  STATE-04.
- `test_allowlist_counts_exact` enforces every pattern's `expected_count`.
  A new raw access matching a substring of an existing pattern would push
  the actual count above expected and fail CI.

### 5. Test quality — PASS

- `test_session_uuid.py`: 11 tests cover concurrency (100 simulated
  sessions, all unique UUIDs), stability within session, survival across
  simulated token refresh, ephemeral fallback on prune, idempotency of
  `ensure_session_uuid`, four poisoning vectors (uppercase, non-string,
  malformed length, AssertionError-during-write), route coverage
  (`test_every_ui_page_handler_mints_uuid`), and
  `create_layout`-wiring sanity (`test_create_layout_mints_session_uuid`).
- `test_no_raw_storage_access.py`: 6 tests cover schema (H1 dict format
  with source+expected_count), synthetic-violation detection (B2 chain
  regression guard), aliased-import resolution (`app`, `nicegui_app`, `_app`),
  parent-tracking (no double-reporting across Call+Subscript+Subscript+
  bare Attribute), exact-count enforcement (H1), and the big-gate
  production scan.
- `test_browse_state.py` + `test_search_state.py`: round-trip persist/restore,
  legacy-payload adoption (no version stamp), stale-version discard,
  partial clear (`keep_position`), and prune-race tolerance. The B3 fix
  is in place: `tests/test_browse_state.py` patches `web.safe_storage.app`
  (single chokepoint), `tests/test_search_state.py` dual-patches both
  `web.pages.search_state.app` (for tab storage) and `web.safe_storage.app`
  (for user storage chokepoint).
- All 38 tests pass on Python 3.11 + pytest 9.0.2.

### 6. Security — PASS

- CSPRNG path: `uuid.uuid4().hex` is the only mint path (lines 135, 141,
  144, 147, 174 in `web/safe_storage.py`). Python `uuid.uuid4` per docs
  uses `os.urandom` (CSPRNG). No other generation path exists.
- Session fixation: `_is_valid_uuid` rejects all poisoned stored values
  (uppercase hex, non-string, malformed length, non-hex chars) and
  regenerates fresh. A malicious user cannot force a known UUID by
  writing to their own session storage.
- T-87-03 (UUID secrecy): not logged at INFO+, not exposed in URLs/query
  strings, not sent to PostHog without HMAC. The docstring at L118-120
  encodes this requirement.
- Cross-user leak: pre-existing fix in `supabase_client.get_user_client`
  (cache key = access_token, not id(storage)) is preserved. Captured-handle
  pattern at L111 is documented and Phase-90-scheduled for deletion.

### 7. Defensive wrapper preservation — PASS

- The M3 Class A vs Class B audit is justified and well-documented at each
  preserved site (see IN-06).
- Spot-checked `persist_browse_snapshot` (L187-213): outer wrapper preserved
  because it wraps dict-construction (`{'sys_id': state.sys_id, 'p_num':
  getattr(page, 'p_num', 1), ...}`) which can raise `AttributeError`/
  `TypeError` on malformed state. The chokepoint helper would NOT absorb
  those because they happen before the storage call. Verified.
- Spot-checked `persist_search_snapshot` (L398-417): outer wrapper preserved
  because it wraps `_compact_result_rows((state.results or [])[:N])` (list
  iteration + dict.pop with arbitrary `display` values), plus
  `[s.to_dict() for s in state.refinement_chain]` (schema-drift risk on
  `RefinementStep`). The nested INNER wrapper (L409-414) is additionally
  preserved for the to_dict() loop alone, allowing partial save (other
  fields persist even if refinement_chain serialization fails).

---

_Reviewed: 2026-05-13_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
