---
phase: 81B
plan: 02
type: execute
wave: 1
depends_on: [81B-01]
files_modified:
  - skills/cairo-genizah-research/scripts/_config.py
  - skills/cairo-genizah-research/scripts/_lock.py
  - skills/cairo-genizah-research/scripts/throttle.py
  - skills/cairo-genizah-research/scripts/search.py
  - skills/cairo-genizah-research/scripts/browse.py
  - skills/cairo-genizah-research/scripts/parallels.py
autonomous: true
requirements: [SKILL-01, SKILL-06]
tags: [skill, transport, throttle, http, wave-1]
must_haves:
  truths:
    - "Calling `python skills/cairo-genizah-research/scripts/search.py --query test --limit 1` against the live production deployment returns valid JSON envelope to stdout (SKILL-01 base-URL configurable + runnable)"
    - "Calling 15 search + 10 browse acquires sequentially against the throttle stays under 60 seconds simulated time and never returns a self-induced 429 (SKILL-06 verification math)"
    - "Throttle state in `state/throttle.json` survives across separate Python process invocations (SKILL-06 cross-process persistence)"
    - "Per-endpoint buckets (`search`, `browse`, `parallels`) are independent — exhausting one does not block another"
    - "Base URL resolution honors D-09: env wins over CLI flag wins over default `https://genizahsearch.com`"
    - "All 7 throttle RED tests from Plan 01 are now GREEN"
  artifacts:
    - path: "skills/cairo-genizah-research/scripts/_config.py"
      provides: "resolve_base_url(cli_arg) helper enforcing D-09 precedence (env > CLI > default)"
      exports: ["resolve_base_url", "DEFAULT_BASE_URL", "STATE_DIR"]
    - path: "skills/cairo-genizah-research/scripts/_lock.py"
      provides: "Cross-platform file lock helper (fcntl on Unix, msvcrt on Windows)"
      exports: ["lock_file", "unlock_file"]
    - path: "skills/cairo-genizah-research/scripts/throttle.py"
      provides: "Token-bucket acquire(bucket, rpm, burst) with filesystem-persisted state"
      exports: ["acquire", "_read_state", "_write_state"]
    - path: "skills/cairo-genizah-research/scripts/search.py"
      provides: "POST /api/search transport; CLI: --query, --search-mode, --limit, --gap, --base-url, --filters-json"
      exports: ["call_search"]
    - path: "skills/cairo-genizah-research/scripts/browse.py"
      provides: "GET /api/browse transport; CLI: --uid OR --sys-id+--p-num+--volume-ie OR --fl-id"
      exports: ["call_browse"]
    - path: "skills/cairo-genizah-research/scripts/parallels.py"
      provides: "POST /api/parallels transport; CLI: --text, --chunk-size, --mode"
      exports: ["call_parallels"]
  key_links:
    - from: "skills/cairo-genizah-research/scripts/search.py"
      to: "skills/cairo-genizah-research/scripts/throttle.py"
      via: "throttle.acquire('search', rpm=...)"
      pattern: "throttle.acquire"
    - from: "skills/cairo-genizah-research/scripts/throttle.py"
      to: "skills/cairo-genizah-research/state/throttle.json"
      via: "filesystem read/write under file lock"
      pattern: "throttle.json"
    - from: "skills/cairo-genizah-research/scripts/_config.py"
      to: "GENIZAH_API_BASE env var"
      via: "os.environ.get"
      pattern: "GENIZAH_API_BASE"
---

<objective>
Build the transport layer for the Cairo Genizah Research skill: HTTP client wrappers around `/api/search`, `/api/browse`, `/api/parallels`, plus the token-bucket throttle that paces them under the server's 30 rpm per-bucket cap. Flips all 7 SKILL-06 throttle RED tests from Plan 01 GREEN.

Purpose: Per CONTEXT D-04, the skill exercises all three endpoints. Per SKILL-06, request pacing is a load-bearing requirement (server independently rate-limits each bucket; without client-side pacing, a 25-call workflow would hit 429s). Per RESEARCH §6, throttle state must persist across script invocations because each `python scripts/X.py` is a fresh process — solved by JSON state file with platform-aware file lock.

Output: 6 files. Pure transport — no business logic, no merging, no formatting (Plan 03 owns those). Each script is independently runnable as a CLI for the model's bash tool to invoke.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@.planning/phases/81B-claude-skill-consumer/81B-CONTEXT.md
@.planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md
@.planning/phases/81A-api-contract-expansion/81A-CONTEXT.md
@.planning/phases/79-api-browse-drill-down/79-CONTEXT.md
@.planning/phases/80-api-parallels/80-CONTEXT.md
@.planning/phases/81B-claude-skill-consumer/81B-01-PLAN.md
@web/search_api.py
@shared/api_errors.py
@tests/test_skill_throttle.py
@CLAUDE.md

<interfaces>
<!-- Locked from upstream phases. Skill code matches exactly. -->

POST /api/search request body (Phase 81A, web/search_api.py):
```python
{
  "search_mode": "exact" | "variants" | "regex" | "responsa" | "title" | "shelfmark",
  "query": str,                          # 1-1000 chars (256 if regex)
  "gap": int,                            # default 0
  "limit": int,                          # 1-100 (Phase 81A D-05)
  "filters": dict | None,
  "responsa_options": {                  # ONLY when search_mode='responsa'
    "variants": bool, "ja": bool, "flex_spacing": bool, "bidirectional": bool
  } | None
}
```
The OLD `mode` field is HARD-REJECTED with 400 `invalid_request`. Skill MUST use `search_mode`.

GET /api/browse query params (Phase 79):
- Preferred: `?uid=<UID>` (single param)
- Fallback A: `?sys_id=<SID>&p_num=<N>&volume_ie=<IE>`  (volume_ie optional)
- Fallback B: `?fl_id=<FL>`
- Optional: `?text_cap=<N>` (100..10000)

POST /api/parallels request body (Phase 80, NOT renamed by 81A — uses `mode` not `search_mode`):
```python
{
  "text": str,                           # ≤20000 chars
  "chunk_size": int,                     # default 5
  "mode": "exact" | "variants" | "fuzzy",   # NOTE: parallels keeps `mode` (81A D-07)
  "max_freq": int | None,
  "boundary_mode": "full" | "boundary" | "combined" | None,
  "filters": dict | None
}
```

Error envelope (shared/api_errors.py):
```python
{"error": {"code": "rate_limited" | "query_required" | "query_too_long" | "invalid_request"
                  | "invalid_combination" | "invalid_filter_value" | "filter_vocabulary_unavailable"
                  | "manuscript_page_not_found" | "core_timeout" | "locator_conflict"
                  | "composition_required" | "composition_too_long" | "regex_pattern_too_long"
                  | ...,
           "message": str}}
```
HTTP 429 carries `Retry-After` header (seconds, integer).

</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Config helpers + cross-platform file lock</name>
  <files>skills/cairo-genizah-research/scripts/_config.py, skills/cairo-genizah-research/scripts/_lock.py</files>
  <read_first>
    - .planning/phases/81B-claude-skill-consumer/81B-CONTEXT.md (D-09 base-URL precedence: env wins)
    - .planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md (§2 base-URL pattern; §6 STATE_FILE path; R6 env-wins surprise; R3 file lock)
    - tests/test_skill_throttle.py (test names referencing CAIRO_GENIZAH_STATE_DIR / GENIZAH_SKILL_REQ_PER_MIN env vars)
  </read_first>
  <behavior>
    - Test 1: `resolve_base_url(None)` returns `"https://genizahsearch.com"` when `GENIZAH_API_BASE` unset.
    - Test 2: `resolve_base_url("http://example/")` returns `"http://example"` (trailing slash stripped) when env unset.
    - Test 3: With env `GENIZAH_API_BASE=http://localhost:8080`, `resolve_base_url("http://other")` returns `"http://localhost:8080"` — env wins per D-09.
    - Test 4: `STATE_DIR` defaults to `Path(__file__).parent.parent / "state"` but is overridden by `CAIRO_GENIZAH_STATE_DIR` env var when set.
    - Test 5: `lock_file(f)` / `unlock_file(f)` execute without error on the current platform (Windows uses msvcrt; Unix uses fcntl).
  </behavior>
  <action>
    **`skills/cairo-genizah-research/scripts/_config.py`:**

    ```python
    """Configuration helpers for the cairo-genizah-research skill.

    D-09 precedence: GENIZAH_API_BASE env var > --base-url CLI flag > default.
    This INVERTS the typical CLI convention (CLI usually wins) — see SKILL.md
    for rationale.
    """
    from __future__ import annotations
    import os
    from pathlib import Path

    DEFAULT_BASE_URL = "https://genizahsearch.com"

    def resolve_base_url(cli_arg: str | None) -> str:
        env = os.environ.get("GENIZAH_API_BASE")
        if env:
            return env.rstrip("/")
        if cli_arg:
            return cli_arg.rstrip("/")
        return DEFAULT_BASE_URL

    def _state_dir() -> Path:
        override = os.environ.get("CAIRO_GENIZAH_STATE_DIR")
        if override:
            return Path(override)
        # Skill-relative: state/ sibling of scripts/
        return Path(__file__).resolve().parent.parent / "state"

    # Resolved at import time; tests that monkeypatch the env var must reload the module.
    STATE_DIR = _state_dir()

    def state_path(filename: str) -> Path:
        d = _state_dir()  # Re-resolve so tests can monkeypatch env var without reload
        d.mkdir(parents=True, exist_ok=True)
        return d / filename

    def get_rpm() -> int:
        """Effective requests-per-minute per bucket. Default 24 (6 rpm headroom under server's 30)."""
        return int(os.environ.get("GENIZAH_SKILL_REQ_PER_MIN", "24"))

    def get_burst() -> int:
        return int(os.environ.get("GENIZAH_SKILL_BURST", "5"))
    ```

    **`skills/cairo-genizah-research/scripts/_lock.py`:**

    ```python
    """Cross-platform file lock for throttle state.

    Unix: fcntl.flock (advisory, exclusive).
    Windows: msvcrt.locking (mandatory, byte-range).
    Both block until the lock is acquired.
    """
    from __future__ import annotations
    import sys

    if sys.platform == "win32":
        import msvcrt

        def lock_file(f) -> None:
            # Lock 1 byte at offset 0; LK_LOCK blocks (retries 10x at 1s intervals).
            f.seek(0)
            try:
                msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
            except OSError:
                # File too short to lock byte 0 — write a sentinel byte then retry.
                f.write(b"\0" if "b" in f.mode else "\0")
                f.flush()
                f.seek(0)
                msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)

        def unlock_file(f) -> None:
            f.seek(0)
            try:
                msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
    else:
        import fcntl

        def lock_file(f) -> None:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)

        def unlock_file(f) -> None:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    ```
  </action>
  <verify>
    <automated>python -c "import sys; sys.path.insert(0, 'skills/cairo-genizah-research'); from scripts._config import resolve_base_url, get_rpm, DEFAULT_BASE_URL; assert resolve_base_url(None) == 'https://genizahsearch.com'; assert resolve_base_url('http://x/') == 'http://x'; import os; os.environ['GENIZAH_API_BASE'] = 'http://localhost:8080'; assert resolve_base_url('http://other') == 'http://localhost:8080'; del os.environ['GENIZAH_API_BASE']; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `_config.py` exports `resolve_base_url`, `DEFAULT_BASE_URL`, `STATE_DIR`, `state_path`, `get_rpm`, `get_burst`.
    - `grep "GENIZAH_API_BASE" skills/cairo-genizah-research/scripts/_config.py` returns ≥1 line.
    - `grep "CAIRO_GENIZAH_STATE_DIR" skills/cairo-genizah-research/scripts/_config.py` returns ≥1 line.
    - `grep "GENIZAH_SKILL_REQ_PER_MIN" skills/cairo-genizah-research/scripts/_config.py` returns ≥1 line.
    - `_lock.py` defines BOTH `lock_file` and `unlock_file` (verify via `grep -E "^def (lock|unlock)_file" skills/cairo-genizah-research/scripts/_lock.py | wc -l` returns `2`).
    - `_lock.py` has a `sys.platform == "win32"` branch for msvcrt and an `else` branch for fcntl.
    - Verify command above prints `OK`.
    - D-09 inversion is documented in `_config.py` docstring (`grep "INVERTS" skills/cairo-genizah-research/scripts/_config.py` ≥1).
  </acceptance_criteria>
  <done>Config + lock helpers ready. Throttle (Task 2) and HTTP scripts (Task 3) import from these.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Token-bucket throttle with filesystem-persisted state</name>
  <files>skills/cairo-genizah-research/scripts/throttle.py</files>
  <read_first>
    - skills/cairo-genizah-research/scripts/_config.py (just-created; imports `state_path`, `get_rpm`, `get_burst`)
    - skills/cairo-genizah-research/scripts/_lock.py (just-created)
    - tests/test_skill_throttle.py (the 7 tests this module flips GREEN)
    - .planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md (§6 — full pseudocode + verification math)
  </read_first>
  <behavior>
    - All 7 RED tests from `tests/test_skill_throttle.py` flip GREEN:
      - first call doesn't block; burst-5 then blocks at #6 with wait ≥ 60/24 - epsilon
      - search/browse buckets isolated
      - state persists across importlib.reload (process-boundary proxy)
      - 15 search + 10 browse completes ≤60s simulated
      - corrupt state file does not raise (recovers as empty)
      - GENIZAH_SKILL_REQ_PER_MIN=12 halves throughput
  </behavior>
  <action>
    Implement per RESEARCH §6 pseudocode, with three concrete refinements:

    1. **Use `time.monotonic()` NOT `time.time()`** — wall-clock can jump backward (NTP, suspend/resume); monotonic guarantees `elapsed >= 0`. Tests monkeypatch `time.monotonic`.

    2. **State file format**: JSON `{bucket_name: {"tokens": float, "last_refill": float}}`. On corrupt JSON or unreadable file, treat as `{}` and rewrite (R3 mitigation).

    3. **Lock-then-read-then-modify-then-write-then-unlock** sequence; do NOT release the lock between read and write (race window).

    ```python
    """Token-bucket throttle with cross-process state persistence.

    Per SKILL-06: separate buckets per endpoint, default ≤24 req/min, burst 5.
    Each Python process invocation reads, decrements, and writes the shared
    state file under an exclusive file lock so sequential `python scripts/X.py`
    calls in the same skill run accumulate properly.

    Design notes:
    - time.monotonic() not time.time() — wall-clock jumps would corrupt math.
    - Block-by-sleep when tokens insufficient; return wait_seconds for caller observability.
    - Corrupt state recovers as empty (R3 mitigation).
    """
    from __future__ import annotations
    import json
    import time
    from typing import Any

    from . import _config
    from . import _lock

    STATE_FILENAME = "throttle.json"

    def _read_state(f) -> dict[str, Any]:
        f.seek(0)
        raw = f.read()
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}

    def _write_state(f, state: dict[str, Any]) -> None:
        f.seek(0)
        f.truncate()
        f.write(json.dumps(state))
        f.flush()

    def acquire(bucket: str, rpm: int | None = None, burst: int | None = None) -> float:
        """Block until one token is available for `bucket`. Returns total wait_seconds."""
        if rpm is None:
            rpm = _config.get_rpm()
        if burst is None:
            burst = _config.get_burst()

        path = _config.state_path(STATE_FILENAME)
        # Open in r+; create empty if absent.
        if not path.exists():
            path.write_text("{}", encoding="utf-8")

        with open(path, "r+", encoding="utf-8") as f:
            _lock.lock_file(f)
            try:
                state = _read_state(f)
                now = time.monotonic()
                b = state.get(bucket, {"tokens": float(burst), "last_refill": now})
                elapsed = max(0.0, now - b["last_refill"])
                # Refill: tokens += elapsed * (rpm / 60), capped at burst.
                b["tokens"] = min(float(burst), b["tokens"] + elapsed * (rpm / 60.0))
                b["last_refill"] = now

                wait = 0.0
                if b["tokens"] < 1.0:
                    wait = (1.0 - b["tokens"]) * 60.0 / rpm
                    time.sleep(wait)
                    # After sleep, account for the consumed token.
                    b["tokens"] = 0.0
                    b["last_refill"] = time.monotonic()
                else:
                    b["tokens"] -= 1.0

                state[bucket] = b
                _write_state(f, state)
                return wait
            finally:
                _lock.unlock_file(f)
    ```

    **CRITICAL — test-clock pattern**: tests monkeypatch `time.monotonic` AND `time.sleep`. The implementation calls `time.monotonic()` and `time.sleep()` via the unbound `time` module (NOT `from time import monotonic` as a local binding) so monkeypatch reaches them. Verify by `grep "from time import" skills/cairo-genizah-research/scripts/throttle.py` returns 0 lines and `grep "import time" skills/cairo-genizah-research/scripts/throttle.py` returns ≥1.
  </action>
  <verify>
    <automated>pytest tests/test_skill_throttle.py -x -v</automated>
  </verify>
  <acceptance_criteria>
    - `tests/test_skill_throttle.py` reports 7 passed.
    - `grep "import time" skills/cairo-genizah-research/scripts/throttle.py` returns ≥1 line.
    - `grep "from time import" skills/cairo-genizah-research/scripts/throttle.py` returns 0 lines (monkeypatch reaches `time.monotonic` / `time.sleep`).
    - `grep "time.monotonic" skills/cairo-genizah-research/scripts/throttle.py` returns ≥2 lines.
    - `grep "json.JSONDecodeError" skills/cairo-genizah-research/scripts/throttle.py` returns ≥1 line (corrupt-state recovery).
    - `grep -E "_lock\.(lock|unlock)_file" skills/cairo-genizah-research/scripts/throttle.py` returns ≥2 lines (lock used).
    - `acquire`, `_read_state`, `_write_state` are defined as top-level functions.
  </acceptance_criteria>
  <done>Throttle module flips all 7 RED tests GREEN. Filesystem state persists across processes; per-bucket isolation works; SKILL-06 verification math holds.</done>
</task>

<task type="auto">
  <name>Task 3: HTTP transport scripts (search.py, browse.py, parallels.py)</name>
  <files>skills/cairo-genizah-research/scripts/search.py, skills/cairo-genizah-research/scripts/browse.py, skills/cairo-genizah-research/scripts/parallels.py</files>
  <read_first>
    - skills/cairo-genizah-research/scripts/_config.py
    - skills/cairo-genizah-research/scripts/throttle.py
    - .planning/phases/81A-api-contract-expansion/81A-CONTEXT.md (search_mode enum + responsa_options shape + request echo)
    - .planning/phases/79-api-browse-drill-down/79-CONTEXT.md (browse query-param shape + locator round-trip)
    - .planning/phases/80-api-parallels/80-CONTEXT.md (parallels keeps `mode` not `search_mode` per 81A D-07)
    - shared/api_errors.py (error code list)
    - web/search_api.py (canonical request/response shapes)
  </read_first>
  <action>
    Each script: (a) parses CLI args via `argparse`, (b) acquires throttle token for its bucket, (c) makes one HTTP call via `requests` (per RESEARCH Open Q1 recommendation — sync, simpler debugging), (d) writes the JSON response (or error envelope) to stdout. Always exits 0 with a JSON body — even on HTTP errors — so the model's bash tool gets parseable output.

    **`scripts/search.py`** — POST /api/search:

    ```python
    """POST /api/search transport. Emits JSON envelope to stdout.

    Usage: python search.py --query "ויאמר" --search-mode exact --limit 10
                            [--gap N] [--filters-json '{"library":["CUL"]}']
                            [--responsa-options-json '{"variants":true}']
                            [--base-url URL]

    Per SKILL-01 / D-09: --base-url is overridden by GENIZAH_API_BASE env var.
    """
    from __future__ import annotations
    import argparse
    import json
    import sys

    import requests

    # Support both `python -m skills.cairo_genizah_research.scripts.search` and
    # `python skills/cairo-genizah-research/scripts/search.py` invocations.
    try:
        from . import _config, throttle
    except ImportError:
        import os
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")
        from scripts import _config, throttle  # type: ignore

    SEARCH_MODES = {"exact", "variants", "regex", "responsa", "title", "shelfmark"}

    def call_search(
        *,
        query: str,
        search_mode: str = "exact",
        limit: int = 10,
        gap: int = 0,
        filters: dict | None = None,
        responsa_options: dict | None = None,
        base_url: str | None = None,
        timeout: float = 30.0,
    ) -> dict:
        if search_mode not in SEARCH_MODES:
            return {"error": {"code": "invalid_request", "message": f"unknown search_mode '{search_mode}'"}}
        url = _config.resolve_base_url(base_url) + "/api/search"
        body: dict = {"search_mode": search_mode, "query": query, "gap": gap, "limit": limit}
        if filters:
            body["filters"] = filters
        if responsa_options:
            body["responsa_options"] = responsa_options
        throttle.acquire("search")
        try:
            resp = requests.post(url, json=body, timeout=timeout)
        except requests.exceptions.Timeout:
            return {"error": {"code": "core_timeout", "message": f"search request timed out after {timeout}s"}}
        except requests.exceptions.ConnectionError as e:
            return {"error": {"code": "connection_error", "message": str(e)}}
        # Pass through server response — error envelope OR success envelope.
        try:
            data = resp.json()
        except json.JSONDecodeError:
            return {"error": {"code": "invalid_response", "message": f"non-JSON response (HTTP {resp.status_code})"}}
        # On 429, surface Retry-After if present (skill output layer reads this).
        if resp.status_code == 429 and "Retry-After" in resp.headers:
            if isinstance(data, dict) and "error" in data:
                data["error"]["retry_after"] = resp.headers["Retry-After"]
        return data

    def _main(argv: list[str] | None = None) -> int:
        p = argparse.ArgumentParser(description="POST /api/search")
        p.add_argument("--query", required=True)
        p.add_argument("--search-mode", default="exact", choices=sorted(SEARCH_MODES))
        p.add_argument("--limit", type=int, default=10)
        p.add_argument("--gap", type=int, default=0)
        p.add_argument("--filters-json", default=None)
        p.add_argument("--responsa-options-json", default=None)
        p.add_argument("--base-url", default=None)
        args = p.parse_args(argv)
        filters = json.loads(args.filters_json) if args.filters_json else None
        ropts = json.loads(args.responsa_options_json) if args.responsa_options_json else None
        result = call_search(
            query=args.query,
            search_mode=args.search_mode,
            limit=args.limit,
            gap=args.gap,
            filters=filters,
            responsa_options=ropts,
            base_url=args.base_url,
        )
        json.dump(result, sys.stdout, ensure_ascii=False)
        sys.stdout.write("\n")
        return 0

    if __name__ == "__main__":
        sys.exit(_main())
    ```

    **`scripts/browse.py`** — GET /api/browse:

    Same dual-import scaffold + `argparse`. Public `call_browse(*, uid=None, sys_id=None, p_num=None, volume_ie=None, fl_id=None, text_cap=None, base_url=None, timeout=30.0)`.

    Validation: at least one of `uid`, `sys_id`, `fl_id` must be provided; otherwise return `{"error": {"code": "invalid_request", "message": "must provide uid, sys_id, or fl_id"}}`.

    Build query string preferring `uid`; else use `sys_id` (+ optional `p_num`, `volume_ie`); else `fl_id`. Append `text_cap` if set. Bucket name: `"browse"`. Otherwise pattern matches search.py.

    CLI: `--uid`, `--sys-id`, `--p-num`, `--volume-ie`, `--fl-id`, `--text-cap`, `--base-url`.

    **`scripts/parallels.py`** — POST /api/parallels:

    Same scaffold. Public `call_parallels(*, text, chunk_size=5, mode="exact", max_freq=None, boundary_mode=None, filters=None, base_url=None, timeout=60.0)`.

    PARALLELS_MODES = `{"exact", "variants", "fuzzy"}` — note this is the `mode` field, NOT `search_mode` (Phase 81A D-07 keeps the parallels field name as-is).

    Bucket name: `"parallels"`. Higher default timeout (60s — composition searches can be slow per Phase 80).

    CLI: `--text` (required; or `--text-file` reading from stdin/file), `--chunk-size`, `--mode`, `--max-freq`, `--boundary-mode`, `--filters-json`, `--base-url`.

    All three scripts: `if __name__ == "__main__": sys.exit(_main())` so they're directly runnable AND importable.
  </action>
  <verify>
    <automated>python skills/cairo-genizah-research/scripts/search.py --query "test" --search-mode exact --limit 1 --base-url https://genizahsearch.com 2>&1 | python -c "import sys, json; d = json.loads(sys.stdin.read()); assert isinstance(d, dict); assert 'schema_version' in d or 'error' in d; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - Three files exist with `call_search`, `call_browse`, `call_parallels` exported.
    - Each file has `if __name__ == "__main__":` block.
    - `grep -E "throttle\.acquire\(.search.\)" skills/cairo-genizah-research/scripts/search.py` returns 1 line.
    - `grep -E "throttle\.acquire\(.browse.\)" skills/cairo-genizah-research/scripts/browse.py` returns 1 line.
    - `grep -E "throttle\.acquire\(.parallels.\)" skills/cairo-genizah-research/scripts/parallels.py` returns 1 line.
    - `grep "search_mode" skills/cairo-genizah-research/scripts/search.py` returns ≥3 lines (request body uses `search_mode`, not legacy `mode`).
    - `grep -c "\"mode\"" skills/cairo-genizah-research/scripts/parallels.py` returns ≥1 (parallels uses `mode` per 81A D-07).
    - `grep "Retry-After" skills/cairo-genizah-research/scripts/search.py` returns ≥1 line.
    - `grep "GENIZAH_API_BASE\|resolve_base_url" skills/cairo-genizah-research/scripts/search.py` returns ≥1 line.
    - Verify command above prints `OK` (live smoke against production: returns either a valid envelope with `schema_version` or a graceful error envelope — never crashes).
    - At least one of `uid`, `sys_id`, `fl_id` validation present in browse.py: `grep "must provide uid" skills/cairo-genizah-research/scripts/browse.py` returns ≥1.
  </acceptance_criteria>
  <done>Three transport scripts callable via CLI and importable via Python. Live smoke against production returns a valid envelope. Throttle wired in. Plan 03 imports `call_search` / `call_browse` / `call_parallels` for the staged-discovery orchestrator.</done>
</task>

</tasks>

<threat_model>

## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Skill scripts → live API at `genizahsearch.com` | Outbound HTTPS; skill is the client; server enforces rate limit and validation. Skill must NOT trust response shape — error envelopes can arrive with any HTTP status. |
| User shell → CLI args | `--filters-json` and `--responsa-options-json` accept arbitrary JSON; passed to server which validates. Skill does not exec or eval the JSON locally. |
| `state/throttle.json` → multiple skill processes | Concurrent reads/writes via file lock. R3 captures race risk. |
| `GENIZAH_API_BASE` env var | User-controlled; can point to attacker-controlled host. Documented as a feature (D-09 supports local-dev override). |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-81B-04 | Spoofing | `GENIZAH_API_BASE` pointed to attacker host | accept | Per D-09 this is a feature (local dev override). User responsibility to set correctly. SKILL.md `--help` documents the precedence loudly per R6. |
| T-81B-05 | Tampering | Concurrent throttle state writes | mitigate | `_lock.lock_file` / `unlock_file` wraps all read-modify-write sequences in `throttle.acquire`. R3 mitigation: corrupt JSON recovers to empty state. |
| T-81B-06 | Information Disclosure | Server error messages echoed to stdout | accept | Server's error envelope (`shared/api_errors.py`) already filters internal details. Skill passes through verbatim; no PII risk because server controls the shape. |
| T-81B-07 | Denial of Service | Skill self-DOS via runaway loop | mitigate | Token-bucket throttle caps at 24 rpm per bucket; SKILL-06 verification math proves 25-call workflow stays under server's 30 rpm. Server-side rate limit (Phase 78 HARDEN-01) is the authoritative ceiling. |
| T-81B-08 | Elevation of Privilege | `responsa_options` cross-field validation bypass | mitigate | Client does not pre-validate; server enforces (81A D-04 invalid_combination). Skill passes through verbatim and surfaces server's 400. No client-side authority. |
| T-81B-09 | Repudiation | No request logging client-side | accept | Server logs via PostHog (Phase 78 HARDEN-05); skill is stateless transport, no local audit log needed for v7.10. |

</threat_model>

<verification>
- `pytest tests/test_skill_throttle.py -v` reports 7 passed.
- `pytest tests/test_skill_consumer.py --collect-only` STILL fails with `ModuleNotFoundError: ...format_output` (intended; Plan 03 owns those modules).
- Live smoke via Task 3 verify command returns `OK`.
- `python -c "from skills.cairo_genizah_research.scripts import search, browse, parallels, throttle, _config, _lock; print('imports OK')"` succeeds.
- Wider suite: `pytest -k "not skill_consumer and not skill_smoke"` reports the existing 1465 passed / 15 skipped baseline + 7 new throttle passes (i.e., 1472 passed / 15 skipped).
</verification>

<success_criteria>
- All 7 SKILL-06 throttle tests GREEN.
- Live smoke against production succeeds (search.py returns valid envelope or graceful error).
- D-09 base-URL precedence (env > CLI > default) enforced and tested.
- Per-bucket throttle isolation verified.
- Skill scripts callable both as CLI (`python search.py ...`) and as Python imports (`from scripts.search import call_search`).
</success_criteria>

<output>
After completion, create `.planning/phases/81B-claude-skill-consumer/81B-02-SUMMARY.md`:
- Files created, throttle test results
- Live smoke evidence (status code + first 200 chars of response)
- Any deviations from RESEARCH §6 pseudocode (e.g., `time.monotonic()` swap)
- Confirmation that Plan 03's `format_output.py`, `stage.py`, `normalize_shelfmark.py` can import `call_search`/`call_browse`/`call_parallels` cleanly
</output>
