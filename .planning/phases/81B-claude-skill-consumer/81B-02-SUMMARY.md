---
phase: 81B
plan: 02
subsystem: skill-transport
tags: [skill, anthropic-skill, transport, throttle, http, wave-1, SKILL-01, SKILL-06]

requires:
  - phase: 81B-01
    provides: RED test scaffolding for throttle (7 tests) + skill source tree skeleton

provides:
  - skills/cairo-genizah-research/scripts/_config.py: resolve_base_url (D-09), state_path, get_rpm, get_burst
  - skills/cairo-genizah-research/scripts/_lock.py: cross-platform file lock (win32 msvcrt / Unix fcntl)
  - skills/cairo-genizah-research/scripts/throttle.py: token-bucket acquire() with filesystem persistence
  - skills/cairo-genizah-research/scripts/search.py: POST /api/search transport + CLI
  - skills/cairo-genizah-research/scripts/browse.py: GET /api/browse transport + CLI
  - skills/cairo-genizah-research/scripts/parallels.py: POST /api/parallels transport + CLI
  - tests/conftest.py (updated): skill import bridge registering hyphenated dir as underscore module name

affects:
  - 81B-03 (business logic — can now import call_search/call_browse/call_parallels)
  - 81B-05 (acceptance run — live smoke confirmed working)

tech-stack:
  added:
    - requests (HTTP client for skill transport scripts)
  patterns:
    - "Token-bucket throttle: filesystem-persisted JSON state + platform-aware file lock"
    - "D-09 env-wins pattern: GENIZAH_API_BASE overrides --base-url (inverted from CLI convention)"
    - "conftest import bridge: types.ModuleType registration for hyphenated-dir skill packages"
    - "Dual-import pattern: try relative import (package), except fallback to sys.path insert (direct CLI)"

key-files:
  created:
    - skills/cairo-genizah-research/scripts/_config.py
    - skills/cairo-genizah-research/scripts/_lock.py
    - skills/cairo-genizah-research/scripts/throttle.py
    - skills/cairo-genizah-research/scripts/search.py
    - skills/cairo-genizah-research/scripts/browse.py
    - skills/cairo-genizah-research/scripts/parallels.py
  modified:
    - tests/conftest.py (skill import bridge added)

key-decisions:
  - "Used time.time() not time.monotonic() — tests monkeypatch time.time; monotonic resets per-process so wall-clock is correct for cross-process persistence anyway"
  - "_read_state accepts Union[Path, file-handle] — tests call _read_state(tmp_path) with a directory Path, internal use calls with file handle"
  - "Live smoke returns error envelope (not success) because 81A search_mode changes not yet deployed to production — valid per plan's verify check ('error' in d OR 'schema_version' in d)"

metrics:
  duration: 18min
  completed: 2026-05-04T16:27:00Z
  tasks: 3
  files_created: 6
  files_modified: 1
---

# Phase 81B Plan 02: Transport + Throttle Summary

**Token-bucket throttle with filesystem-persisted per-bucket state + HTTP client wrappers for /api/search, /api/browse, /api/parallels — flips all 7 SKILL-06 RED tests GREEN**

## Performance

- **Duration:** ~18 min
- **Started:** 2026-05-04T16:08:41Z
- **Completed:** 2026-05-04T16:27:00Z
- **Tasks:** 3
- **Files created:** 6
- **Files modified:** 1 (tests/conftest.py)

## Accomplishments

- Fixed the hyphen/underscore import mismatch from Plan 01 by adding a `_register_skill_package()` function to `tests/conftest.py` that registers `skills/cairo-genizah-research/` (hyphens) as `skills.cairo_genizah_research` (underscores) using `types.ModuleType` — leaves the directory name unchanged (Anthropic naming mandates hyphens)
- Created `_config.py` with `resolve_base_url()` enforcing D-09 env-wins precedence (`GENIZAH_API_BASE` > `--base-url` > default), `state_path()` re-resolving on every call for test-monkeypatch compatibility, and `get_rpm()` / `get_burst()` reading from env vars
- Created `_lock.py` with cross-platform file locking: `msvcrt.locking` on win32, `fcntl.flock` on Unix
- Created `throttle.py` implementing token-bucket `acquire(bucket, rpm, burst)`: reads state from JSON file under exclusive lock, refills tokens based on elapsed wall-clock time, sleeps if deficit, writes updated state — all within one locked region to prevent race conditions (T-81B-05)
- Created three HTTP transport scripts (`search.py`, `browse.py`, `parallels.py`) each callable as CLI and importable as module, with throttle wired in, error envelopes always returned (never raises), and D-09 base-URL resolution
- All 7 SKILL-06 throttle RED tests from Plan 01 are now GREEN

## Task Commits

1. **Task 1: Config + lock helpers + conftest import bridge** - `c59c6da9` (feat)
2. **Task 2: Token-bucket throttle** - `bb5e114d` (feat)
3. **Task 3: HTTP transport scripts** - `d4380ee7` (feat)

## Throttle Test Results

```
tests/test_skill_throttle.py::test_throttle_first_call_does_not_block         PASSED
tests/test_skill_throttle.py::test_throttle_burst_5_then_blocks               PASSED
tests/test_skill_throttle.py::test_throttle_buckets_are_isolated              PASSED
tests/test_skill_throttle.py::test_throttle_state_persists_across_processes   PASSED
tests/test_skill_throttle.py::test_throttle_15_search_plus_10_browse_completes_under_60_seconds PASSED
tests/test_skill_throttle.py::test_throttle_handles_corrupt_state_file        PASSED
tests/test_skill_throttle.py::test_throttle_env_override_lowers_rpm           PASSED

7 passed in 0.09s
```

## Live Smoke Evidence

Command:
```
python skills/cairo-genizah-research/scripts/search.py --query "test" --search-mode exact --limit 1 --base-url https://genizahsearch.com
```

Response (HTTP 400, valid error envelope):
```json
{"error": {"code": "invalid_request", "message": "Field required", "fields": ["mode", "search_mode"]}}
```

The production server is running v7.9.4 which predates the Phase 81A `search_mode` rename — it still requires the old `mode` field. The response IS a valid JSON error envelope matching `shared/api_errors.py` shape. The smoke verify command checks `'schema_version' in d or 'error' in d` — `'error'` is present, so the check passes with output `OK`.

When 81A ships to production, the same `search.py` will return a success envelope with `schema_version`.

## Plan 03 Import Readiness

`call_search`, `call_browse`, `call_parallels` are all importable via pytest's conftest bridge:

```python
from skills.cairo_genizah_research.scripts.search import call_search
from skills.cairo_genizah_research.scripts.browse import call_browse
from skills.cairo_genizah_research.scripts.parallels import call_parallels
```

Plan 03's `format_output.py`, `stage.py`, `normalize_shelfmark.py` can import these cleanly once they exist.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Used `time.time()` instead of `time.monotonic()`**
- **Found during:** Task 2 test execution
- **Issue:** Plan pseudocode says to use `time.monotonic()`, but the actual RED tests (from Plan 01) monkeypatch `time.time` and `time.sleep`, not `time.monotonic`. Using `time.monotonic()` would cause all 7 tests to remain RED because the fake clock wouldn't be reached.
- **Fix:** Used `time.time()` throughout `throttle.py`. This is also architecturally correct for cross-process persistence: monotonic clocks reset on each new process start, so `time.time()` is the right choice for state written to disk anyway.
- **Files modified:** `skills/cairo-genizah-research/scripts/throttle.py`
- **Commit:** `bb5e114d`

**2. [Rule 1 - Bug] `_read_state` accepts Path directory, not only file handle**
- **Found during:** Task 2 test execution
- **Issue:** Plan pseudocode shows `_read_state(f)` accepting a file handle, but Plan 01's RED tests call `_read_state(tmp_path)` where `tmp_path` is a `pathlib.Path` to the state directory.
- **Fix:** `_read_state` accepts `Union[Path, Any]` — when given a Path, reads `throttle.json` from that directory; when given a file handle, reads from the open file.
- **Files modified:** `skills/cairo-genizah-research/scripts/throttle.py`
- **Commit:** `bb5e114d`

**3. [Rule 3 - Blocking] throttle.py needed import fallback for direct CLI invocation**
- **Found during:** Task 3 verify (live smoke)
- **Issue:** `throttle.py` used only `from . import _config, _lock` (relative imports). When `search.py` added the scripts dir to `sys.path` as a fallback and then did `import throttle`, Python loaded `throttle.py` without a package context — making the relative imports fail.
- **Fix:** Added the same dual-import scaffold to `throttle.py`: try relative, except add `os.path.dirname(__file__)` to sys.path and import directly.
- **Files modified:** `skills/cairo-genizah-research/scripts/throttle.py`
- **Commit:** `d4380ee7`

**4. [Rule 2 - Missing] conftest import bridge for hyphenated skill directory**
- **Found during:** Initial test collection attempt
- **Issue:** Plan 01's summary noted that `skills/cairo-genizah-research/` (hyphens) is not importable as `skills.cairo_genizah_research` (underscores). The `<known_issue>` block in the prompt specified this must be fixed in Plan 02 scope.
- **Fix:** Added `_register_skill_package()` to `tests/conftest.py` using `types.ModuleType` registration so pytest's import system can resolve the underscore module names.
- **Files modified:** `tests/conftest.py`
- **Commit:** `c59c6da9`

## Known Stubs

None — all transport functions make real HTTP calls or return real error envelopes. No placeholders in rendering paths.

## Threat Surface Scan

No new network endpoints or auth paths introduced. The skill scripts make OUTBOUND requests to `genizahsearch.com` (or configured base URL) — this is the documented T-81B-04 / T-81B-05 surface from the plan's threat model, already registered and accepted/mitigated there.

## Self-Check: PASSED

Files created/modified:
- `skills/cairo-genizah-research/scripts/_config.py` FOUND
- `skills/cairo-genizah-research/scripts/_lock.py` FOUND
- `skills/cairo-genizah-research/scripts/throttle.py` FOUND
- `skills/cairo-genizah-research/scripts/search.py` FOUND
- `skills/cairo-genizah-research/scripts/browse.py` FOUND
- `skills/cairo-genizah-research/scripts/parallels.py` FOUND
- `tests/conftest.py` (modified) FOUND

Commits:
- `c59c6da9` FOUND
- `bb5e114d` FOUND
- `d4380ee7` FOUND

7 throttle tests: PASSED
