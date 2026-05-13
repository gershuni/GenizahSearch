---
phase: 87-foundations-session-uuid-and-safe-storage-chokepoint
plan: 02
subsystem: storage
tags: [phase87, safe-storage, session-uuid, nicegui, storage-chokepoint, bootstrap-wiring, m5-strict-validation, b1-bootstrap, fix1-route-coverage]

# Dependency graph
requires:
  - phase: 87-01-validation-foundation
    provides: tests/test_session_uuid.py (10 failing tests — 5 base + 4 M5 + 1 route-coverage), tests/test_safe_storage.py (6 tests; FOUND-05 invariant SHA-256 e165bf0e... must remain byte-unchanged)
provides:
  - web/safe_storage.py:_is_valid_uuid (private regex validator; rejects uppercase hex, non-string, malformed)
  - web/safe_storage.py:get_session_uuid() -> str (lazy mint via uuid.uuid4().hex; CSPRNG-backed)
  - web/safe_storage.py:ensure_session_uuid() -> bool (eager mint; returns False on prune-race)
  - web/safe_storage.py:_SESSION_UUID_KEY constant ('_session_uuid')
  - web/safe_storage.py:_SESSION_UUID_RE compiled regex (^[0-9a-f]{32}$)
  - web/main.py bootstrap wiring at create_layout (covers 17 @ui.page handlers), /reset-hints, /auth/callback (Fix 1)
  - tests/test_session_uuid.py:test_create_layout_mints_session_uuid (B1 automated coverage; 11th test)
affects: [87-03-leaf-file-migrations, 87-04-main-and-alias-migrations, 87-05-browse-cluster-migrations, 87-06-search-cluster-migrations, 87-07-lint-finalization, 87-08-acceptance-and-docs, 88-state-separation, 89-lists-cache, 90-auth-caching, 91-atomic-auth-writes, 92-final-sweep]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Module-level constants `_SESSION_UUID_KEY` + `_SESSION_UUID_RE` for strict regex validation on read (T-87-02 mitigation)"
    - "Private validation helper `_is_valid_uuid()` consolidated and reused by both public helpers (DRY)"
    - "CSPRNG-backed UUID generation via `uuid.uuid4().hex` aliased as `_uuid.uuid4().hex` to flag module-internal usage"
    - "Bootstrap wiring at create_layout() covering the 17-of-19 standard route handlers; direct wiring at /reset-hints and /auth/callback for the 2 non-layout routes that touch storage"
    - "Defense-in-depth test pattern: textual AST verification (Part A) + functional mock verification (Part B) for bootstrap wiring"

key-files:
  created:
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-02-SESSION-UUID-HELPERS-SUMMARY.md
  modified:
    - web/safe_storage.py (additive — 3 new functions, 2 new module constants, 2 new imports; existing 3 helpers byte-identical)
    - web/main.py (additive — 1 new import + 1 call in create_layout + 1 call in reset_hints_route + 1 call in auth_callback_route)
    - tests/test_session_uuid.py (additive — 1 new test appended; previous 10 tests unchanged)

key-decisions:
  - "ensure_session_uuid() is wired at create_layout() (L349) + reset_hints_route (L1288) + auth_callback_route (L1450). The single intentionally-skipped route /privacy-extension is a pure static info page with zero app.storage.user accesses, verified via AST scan."
  - "_is_valid_uuid is a private helper (leading underscore) used by both public functions. Centralizing the regex+isinstance check guarantees consistent rejection semantics across get_session_uuid and ensure_session_uuid (T-87-02 mitigation)."
  - "import alias `uuid as _uuid` keeps the public API surface clean — only get/ensure_session_uuid are exported by intent, even though Python does not enforce module-internal imports."

patterns-established:
  - "Pattern: lazy-mint with validation-on-read — every read goes through _is_valid_uuid; storage-poisoned values trigger fresh mint + overwrite + WARNING log of the rejected value's type (not its content, per T-87-03)."
  - "Pattern: idempotent eager-mint — ensure_session_uuid is safe to call from multiple bootstrap entry points; second call is a no-op if storage already contains a well-formed UUID."

requirements-completed: [FOUND-01, FOUND-02, FOUND-05]

# Metrics
duration: ~3min 19sec
completed: 2026-05-13
---

# Phase 87 Plan 02: Session UUID Helpers Summary

**Foundational session-UUID API landed in `web/safe_storage.py` and wired into the page bootstrap path — all 11 tests in `tests/test_session_uuid.py` and all 6 in `tests/test_safe_storage.py` pass GREEN. The FOUND-05 byte-identical invariant on `tests/test_safe_storage.py` is preserved (SHA-256 `e165bf0e...` unchanged).**

## Performance

- **Duration:** ~3 min 19 sec
- **Started:** 2026-05-13T05:07:55Z
- **Completed:** 2026-05-13T05:11:14Z
- **Tasks:** 4 / 4 (Task 1, Task 2, Task 2b, Task 3)
- **Files modified:** 3 (1 production module, 1 web entry point, 1 test file)
- **Files created:** 1 (this SUMMARY)

## Accomplishments

### Task 1: Helpers added to `web/safe_storage.py`

Added 3 functions and 2 module constants — all additive, no existing code touched.

| Symbol | Signature | Purpose |
|---|---|---|
| `_SESSION_UUID_KEY` | `str = '_session_uuid'` | Canonical storage key |
| `_SESSION_UUID_RE` | `re.Pattern = re.compile(r"^[0-9a-f]{32}$")` | Pre-compiled regex (one-time module load cost) |
| `_is_valid_uuid(value: Any) -> bool` | private | Centralised M5 validator. Returns `True` iff `value` is a `str` matching `^[0-9a-f]{32}$`. Rejects uppercase hex, non-string types, malformed length, non-hex chars. |
| `get_session_uuid() -> str` | public | Returns this session's stable UUID, lazily minting via `uuid.uuid4().hex` on first call. Never `None`, never raises. Validates retrieved value via `_is_valid_uuid` (T-87-02 mitigation). Falls back to ephemeral UUID4 hex on `AssertionError` (prune race). |
| `ensure_session_uuid() -> bool` | public | Eagerly mints UUID if not present. Returns `True` if storage contains a well-formed UUID after the call (minted or pre-existing). Returns `False` only on prune-race `AssertionError`. |

New imports: `import re`, `import uuid as _uuid`.

**Existing helpers untouched** — `safe_user_get`, `safe_user_set`, `safe_user_pop` remain byte-identical to baseline. AST function-set scan confirms exactly 6 functions: `['_is_valid_uuid', 'ensure_session_uuid', 'get_session_uuid', 'safe_user_get', 'safe_user_pop', 'safe_user_set']`.

### Task 2: B1 bootstrap wiring at `create_layout()`

- **Import added at `web/main.py:29`:** `from web.safe_storage import ensure_session_uuid`
- **Call inserted at `web/main.py:349`** (first statement after docstring in `create_layout()`, preceded by 4-line comment block referencing Phase 87 FOUND-01 / 87-REVIEWS.md B1):

```python
def create_layout():
    """Create the main application layout with modern Header and Sidebar."""
    # Phase 87 FOUND-01 (B1 in 87-REVIEWS.md): mint _session_uuid on first
    # page render of every session. ensure_session_uuid() is idempotent and
    # returns False harmlessly on prune-race. Downstream code (Phases 88+)
    # can rely on _session_uuid being present in storage after this point.
    ensure_session_uuid()
```

Coverage at this point: 17 of 19 `@ui.page` handlers (every route that calls `create_layout()`). The 2 remaining storage-touching routes are wired by Task 2b.

### Task 2b: Fix 1 — direct wiring at `/reset-hints` and `/auth/callback`

Closes Codex round-2 B1-residual. Both routes now mint `_session_uuid` directly as their first statement after the docstring.

- **`reset_hints_route` at `web/main.py:1288`** — minted before the 3 storage `pop()` calls at line 1290.
- **`auth_callback_route` at `web/main.py:1450`** — minted before the OAuth atomic writes (USER_KEY, PROFILE_KEY, auth_session at L1458/1460/1463) and PostHog `login_success` telemetry.

**`/privacy-extension` intentionally skipped.** AST scan of `privacy_extension_route` confirms zero `app.storage.user` accesses — pure static info page with only `ui.add_head_html` + `ui.column` + `ui.label` calls. Hard-coded into `EXEMPT_ROUTES` in the route-coverage test.

### Task 3: B1 automated coverage test

Appended `test_create_layout_mints_session_uuid` (11th test in `tests/test_session_uuid.py`):

- **Part A — textual:** parses `web/main.py` source and asserts `from web.safe_storage import ... ensure_session_uuid` is present AND `ensure_session_uuid()` appears inside the `create_layout` body.
- **Part B — functional:** mocks `web.safe_storage.app`, invokes `ensure_session_uuid()` directly, asserts return `True` and that `storage['_session_uuid']` is a 32-char lowercase hex string matching `^[0-9a-f]{32}$`.

Test catches three regression modes: deleted call, deleted import, and broken implementation. Defense-in-depth alongside `test_ensure_session_uuid_idempotent` and `test_every_ui_page_handler_mints_uuid`.

## Task Commits

Each task was committed atomically with conventional-commit format.

1. **Task 1: Add helpers to web/safe_storage.py** — `b85213ee` (feat)
2. **Task 2: Wire ensure_session_uuid into create_layout (B1)** — `c71f5135` (feat)
3. **Task 2b: Wire ensure_session_uuid into /reset-hints and /auth/callback (Fix 1)** — `09f82efa` (feat)
4. **Task 3: Add test_create_layout_mints_session_uuid (B1 coverage)** — `1b0c5afc` (test)

**Plan metadata:** *(pending — added in final docs commit)*

## Test Results

| File | Total | Passing | Failing | Notes |
|---|---|---|---|---|
| `tests/test_safe_storage.py` | 6 | 6 | 0 | FOUND-05 invariant — file byte-unchanged |
| `tests/test_session_uuid.py` | 11 | 11 | 0 | 5 base + 4 M5 + 1 route-coverage + 1 bootstrap |
| **Total** | **17** | **17** | **0** | All 4 acceptance criteria met |

Verification runtime: 1.91 seconds (well under the validation strategy's 3-second quick-run target).

## T-87-01 Verification (UUID uniqueness — HIGH severity)

`test_session_uuid_unique_across_100_sessions` PASSED — 100 simulated independent sessions each receive a unique UUID, zero collisions. Implementation uses `_uuid.uuid4().hex` exclusively (CSPRNG-backed in CPython per Python `uuid` module docs; 122 bits of entropy). No use of `uuid.uuid1()` (MAC-leaking) or `random.*` (predictable) anywhere in the new code.

## T-87-02 Verification (storage-poisoning — MEDIUM-HIGH severity per M5)

4 dedicated regex-validation tests all PASSED:

| Test | Poisoned input | Implementation response |
|---|---|---|
| `test_session_uuid_rejects_uppercase_hex` | `'ABCDEF1234567890ABCDEF1234567890'` (32 uppercase hex) | Rejected by `_is_valid_uuid` (regex is `[0-9a-f]`, not `[0-9a-fA-F]`); fresh lowercase minted |
| `test_session_uuid_rejects_non_string` | `12345`, `None`, `{'malicious': 'dict'}`, `[1, 2, 3]`, `b'bytes'` | Rejected by `_is_valid_uuid` (isinstance check); fresh str minted |
| `test_session_uuid_rejects_malformed_length` | `'short'`, `'a'*31`, `'a'*33`, `'!'*32`, `'g'*32`, `'0'*31 + ' '` | Rejected by `_is_valid_uuid` (length + non-hex chars); fresh valid 32-hex minted |
| `test_ensure_session_uuid_returns_false_on_assertion` | `__setitem__` raises `AssertionError` (prune race during write) | Returns `False` cleanly; no exception bubbles |

WARNING log is emitted on poisoned-value detection — logging only the rejected value's TYPE (e.g., `'int'`, `'dict'`), never its content (T-87-03 mitigation).

## B1 Verification (bootstrap wiring)

- AST scan of `web/main.py` confirms: `create_layout`, `reset_hints_route`, `auth_callback_route` ALL contain `ensure_session_uuid()` calls.
- `test_create_layout_mints_session_uuid` PASSED — textual + functional 2-part check.
- `test_every_ui_page_handler_mints_uuid` PASSED — every `@ui.page` handler in `web/main.py` other than the explicitly-exempt `/privacy-extension` either calls `create_layout()` or `ensure_session_uuid()` directly.

## Fix 1 Verification (Codex B1-residual route coverage)

- 2 of 3 non-layout routes wired directly: `/reset-hints` and `/auth/callback`.
- `/privacy-extension` AST-confirmed to have zero `app.storage.user` accesses — exempt by design, encoded in the route-coverage test's `EXEMPT_ROUTES` set.
- The Wave 0 failure mode (route-coverage test naming both routes) is closed.

## FOUND-05 Invariant Verification

| Time | SHA-256 of `tests/test_safe_storage.py` | Match |
|---|---|---|
| Plan start | `e165bf0e1b71f94590e456b1197b5fcbb146d0aecad28551911e3d482e1ac75f` | baseline |
| Post-Task 1 | `e165bf0e1b71f94590e456b1197b5fcbb146d0aecad28551911e3d482e1ac75f` | ✓ |
| Post-Task 2 | (unchanged — only `web/main.py` edited) | ✓ |
| Post-Task 2b | (unchanged — only `web/main.py` edited) | ✓ |
| Post-Task 3 | `e165bf0e1b71f94590e456b1197b5fcbb146d0aecad28551911e3d482e1ac75f` | ✓ |

`git diff --stat tests/test_safe_storage.py` shows no changes. FOUND-05 hard constraint satisfied.

## Plan 01 Test Compatibility

The 4 Wave 0-passing tests in `tests/test_no_raw_storage_access.py` (`test_allowlist_well_formed`, `test_lint_rejects_synthetic_violation`, `test_lint_handles_aliased_imports`, `test_lint_does_not_double_report_nested_nodes`) all remain PASSING. Plan 02 made no edits to the lint scanner or allowlist; the schema-validation contract is intact.

The remaining 2 lint tests in that file (`test_no_raw_storage_access_outside_allowlist` and `test_allowlist_counts_exact`) are still expected RED at this point — they are gated on Plans 03–06 (raw-access migrations) and Plan 04 (supabase_client migration). Plan 02 does not touch them by design.

## Ruff Verification

`ruff check web/safe_storage.py web/main.py` → `All checks passed!`

No new lint errors introduced.

## Decisions Made

- **Use `re.compile` at module level**, then `_SESSION_UUID_RE.fullmatch(uid)` at call sites. Pre-compiled regex avoids per-call compilation cost in CPython's regex cache and makes the validation pattern auditable in one place.
- **`_is_valid_uuid` is private** (leading underscore) because callers should always go through `get_session_uuid` or `ensure_session_uuid`. Direct validation use suggests the caller is doing something the public API doesn't support, which is a smell.
- **`uuid` aliased as `_uuid`** to flag module-internal usage. Public callers should never need to import the `uuid` stdlib module via `web.safe_storage`; they call the helper instead.
- **Wiring placement at `create_layout()` first-statement-after-docstring** (line 349) — strictly before `_resolve_ui_language()` and `set_language()` calls. This guarantees `_session_uuid` is in storage before any other layout code reads or writes the session storage.
- **`auth_callback_route` wiring placed BEFORE the local imports** (`from web.supabase_client import ...`, `from web.auth_state import GlobalAuthState`) so that the UUID is minted before any module-level side effects of those imports run. Per plan spec.

## Deviations from Plan

None. All 4 tasks executed exactly as specified. The plan required only additive edits to one production module + one web entry point + one test file. No deviations, no auto-fixes (Rules 1–3), no architectural changes (Rule 4).

**Total deviations:** 0.
**Impact on plan:** No scope creep. Plan executed verbatim.

## Issues Encountered

None. All tests passed first run after each task commit. Ruff clean throughout. No Windows/cp1255 encoding issues in the code itself (one bash diagnostic command initially failed due to missing `encoding='utf-8'`, immediately corrected — no code impact).

## User Setup Required

None — Phase 87 helpers are runtime-only Python; no external service configuration, no DB migration, no env-var addition.

## Threat Flags

None. This plan introduces no new network endpoints, no new auth paths, no new file access, no new schema changes. It adds defensive validation to an existing storage chokepoint — strictly hardening, not expanding surface.

## Next Phase Readiness

**Plan 03 (Leaf File Migrations) is unblocked.** Plans 03–06 can now consume:
- `get_session_uuid()` as a stable cache key (per HANDOFF_v7.11.1_path_b.md item 6)
- `ensure_session_uuid()` knowing it's already wired at every storage-touching page entry — they can `assert safe_user_get('_session_uuid')` (after Plan 04's bulk migration) and find a value present.

**Wave 2 (Plans 03–06) will independently migrate raw `app.storage.user` access** at leaf files, components, main.py non-OAuth paths, browse cluster, and search cluster. Plan 07 closes with the lint scanner going fully GREEN.

**Phase 88 (State Separation by Deletion) depends on this plan** — it will use `_session_uuid` as the cache key for the post-`_TEST_BACKEND`-removal state model.

**Blockers/Concerns:** None.

## Self-Check: PASSED

- File `web/safe_storage.py` exists with new helpers. ✅ FOUND (6 functions, expected set)
- File `web/main.py` has B1 wiring at line 349. ✅ FOUND
- File `web/main.py` has Fix 1 wiring at lines 1288 + 1450. ✅ FOUND
- File `tests/test_session_uuid.py` has 11 tests including `test_create_layout_mints_session_uuid`. ✅ FOUND
- File `tests/test_safe_storage.py` SHA-256 unchanged (`e165bf0e...`). ✅ FOUND
- Commit `b85213ee` (Task 1) exists in git log. ✅ FOUND
- Commit `c71f5135` (Task 2) exists in git log. ✅ FOUND
- Commit `09f82efa` (Task 2b) exists in git log. ✅ FOUND
- Commit `1b0c5afc` (Task 3) exists in git log. ✅ FOUND

---
*Phase: 87-foundations-session-uuid-and-safe-storage-chokepoint*
*Plan: 02 - Session UUID Helpers*
*Completed: 2026-05-13*
