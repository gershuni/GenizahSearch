---
id: SEED-016
status: shipped
planted: 2026-06-23
planted_during: 2026-06-23 product-quality fan-out audit (6 agents + Codex). Register: .planning/audit-2026-06-23-product-quality/MASTER.md
trigger_when: CLOUD-AUTO, but CAREFUL — changes shared-service signatures, so test the callers (web + desktop + tests) before push. Round-1 PARALLEL-SAFE: only touches shared/browse_service.py + shared/parallels_service.py (disjoint from 013/014/018-noncore). #3 should land before any future shared/ package extraction.
scope: medium (invert one layering dependency + bound one executor)
---

# SEED-016: Layering + browse executor

> From the 2026-06-23 audit. Both Codex-CONFIRMED. These harden the shared/ boundary and the browse
> enrichment concurrency model.

## Findings (file:line + fix direction)

### #3 — `shared/` imports `web/` (layering violation) (HIGH · MED)
- `shared/browse_service.py:117` `from web.services import get_service` (runtime, inside `_fetch_core`).
- `shared/parallels_service.py:120-122` and `200-202` `from web.state import state`.
The TYPE_CHECKING guard at `browse_service.py:55` shows this was a known smell. It makes shared/ untestable
without a full web context and blocks the planned package split.
**Fix direction:** invert the dependency — inject the service/state into the shared functions as a
parameter (or a small provider protocol), or move the genuinely state-free helpers into shared/ and let
web/ adapt. Keep behavior identical; this is a structural move.
**Risk:** every caller in `web/` (and any test) passes/relies on the implicit import. Enumerate callers
first (`grep -rl "browse_service\|parallels_service" web/ tests/`) and update them in the same change.

### #29 — Default `ThreadPoolExecutor` for browse enrichment fan-out (MED · MED)
`shared/browse_service.py:38-41` (comment already warns default-executor work can't be killed by
`wait_for`), `91-94` `loop.run_in_executor(None, ...)`, `128-129` wrapped in `asyncio.wait_for`, fan-out at
`250-264` / `326-329`. Under load, slow NLI pins the shared default pool; `wait_for` cancels the coroutine
but the thread keeps running.
**Fix direction:** use a bounded, named executor for browse enrichment; cap concurrent sources; make sync
fetchers honor a request-level timeout. Treat `wait_for` as a coroutine timeout, not thread cancellation.
(Relates to the keystone SEED-015 resilience theme but is self-contained here.)

## Tests required
- `tests/test_browse_service_layering.py`: shared services importable/usable WITHOUT importing `web.state`
  / `web.services` (e.g. import in a context where `web` is absent or the dependency is injected); existing
  browse/parallels behavior unchanged (mock the injected provider).
- Bounded-executor test: concurrency cap respected; a slow source times out without starving others.
- ⚠ Run the full set of tests that import these services before push (callers-first lesson).

## Done when
shared/ no longer imports web/ (verify with a guard test or `grep`), browse fan-out uses a bounded named
executor, all caller sites updated, tests green, ruff clean.

---

## Codex review corrections (2026-06-23) — apply before execution
- **Caller list (was missing):** `web/search_api.py` — imports at `:56` and `:59`, browse call at
  `:1333-1338`, parallels call at `:1543-1550`. Update these in the SAME change. (Doesn't collide with
  round-1 014/018-noncore, but the seed's file list was wrong — fixed in MASTER.)
- **#3 explicit signatures (not vague "inject"):** browse → pass a provider/service into
  `fetch_browse_bundle`/`_fetch_core`; parallels → pass `searcher` + `meta_mgr` (or a small provider object).
  **No hidden `web.state` fallback inside shared.** Keep return types STRUCTURAL — do NOT use a runtime
  `BrowsePage` annotation imported from `web.services` (that preserves the smell); keep it TYPE_CHECKING-only.
- **#29 do NOT broaden to parallels executor** — parallels already has a heavy semaphore in
  `web/search_api.py`; changing that concurrency model is a separate effort. Scope #29 to browse fan-out only.
- **Executor lifecycle:** module-level NAMED executor, max-workers from a constant/env, concurrency cap, and
  defined shutdown for tests/process exit.
- **Tests (stronger):** the "imports without web" guard is too weak (imports are already late) — instead call
  the shared functions with INJECTED fakes while poisoning/blocking `web.state`/`web.services`. Add a SOURCE
  guard: no `from web.`/`import web.` in `browse_service.py`/`parallels_service.py` outside TYPE_CHECKING.
  Add bounded-executor tests (cap respected; one slow source times out without starving others). Run
  `tests/test_browse_api.py`, `tests/test_parallels_api.py`, and `tests/test_search_api_v2.py` timeout/heavy
  tests (they monkeypatch internals → likely need signature updates).
