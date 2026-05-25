# Codex critique of INCIDENT-2026-05-25-nli-iiif-hang fix plan

**Verdict:** Not good as-is. Directionally right, but several changes needed before shipping.

## 1. Worst-case math is understated

`timeout=(3, 5)` is up to `3s connect + 5s read`, not just 5s. MARC is up to `3 + 3`. So the
first miss can be `5 sem + 8 IIIF + 6 MARC = 19s`, and `requests` read timeout is not a total
response deadline. Drop `NLI_SEMAPHORE_TIMEOUT` to `0` or `1`. Waiting on the semaphore burns
Starlette workers while doing nothing.

## 2. Circuit check must happen before semaphore acquisition

Check it at `web/api.py:647`, not only inside `_fetch_fl_ids_network`. Otherwise, when the
circuit is open and all 8 NLI slots are occupied, callers still block up to the semaphore
timeout before returning. Check it after cache lookup, before acquire, and recheck after
acquire.

## 3. `_nli_record_success()` placement

Call before `return fl_ids` at `web/api.py:705` and before `return unique_fl_ids` at
`web/api.py:729`, not "after the returns" as I had written.

## 4. Count more than exceptions as failures

Timeouts/connection errors should trip the breaker. `5xx` and `429` should probably count
too. `404`/empty manifest should be negative-cached per sys_id but should not trip the
global NLI outage breaker.

## 5. Lock the breaker; use monotonic time

`_nli_consecutive_failures += 1` and success resets are not atomic as a state machine. Add
a `threading.Lock` around breaker state. Use `time.monotonic()` rather than `time.time()`
for open-until windows. Current state in `genizah_core.py:3938` is race-prone.

## 6. Architecture: extract a module-level breaker

Sharing `GenizahCore` class attributes is acceptable for an emergency patch, but a poor
long-term ownership boundary. Cleaner: `shared/nli_circuit_breaker.py` with module-level
functions/object used by both `genizah_core.py` and `web/api.py`. Also note this is only
process-local — if uvicorn has multiple workers, each has its own breaker.

## 7. 3s connect timeout is fine

AWS us-west-2 to Israel should not need more than that under normal routing. Read timeout
and semaphore wait are the real concerns.

## 8. Other NLI/IIIF blockers I missed

The fix as scoped only covers `fetch_fl_ids_from_nli`. These additional paths block on the
same upstream and need either reduced timeouts or the same breaker:

- `web/api.py:771` `/api/nli_image/{fl_id}` — 15s IIIF + 15s Rosetta
- `web/api.py:834` `_fetch_nli_image_bytes` — 15s image fetches, can loop over every FL id on fallback
- `web/api.py:1994` `/api/proxy_image` — proxies NLI/Rosetta for 15s
- `shared/puzzle_image_service.py:172` and `:252` — 30s timeouts (!)
- `web/pages/puzzle.py:1991` — direct NLI manifest fetch with 15s timeout
- `genizah_core.py:4651` and `:4773` — MARC fetches; less dangerous if confined to the core
  executor, but should use the same breaker semantics

## 9. Tests insufficient

The single monkeypatch test is necessary but not enough. Add a small concurrency test using
`ThreadPoolExecutor`: saturate the NLI semaphore with hanging/timeout calls, assert
additional calls return within ~1s once the circuit is open and do not hit `_nli_session.get`.
Also test `5xx` trips, success resets, and circuit check-before-semaphore behavior.

---

## Minimum ship patch (per Codex)

1. Shorten timeouts
2. Circuit check **before** semaphore
3. Nonblocking or 1s semaphore wait (not 5s)
4. Locked, monotonic breaker
5. Count timeout/5xx/429 failures (not just exceptions)
6. Cover the puzzle/image/proxy NLI paths — at least reduce their timeouts, ideally also
   guard them with the same breaker

---

*Brief: `_tmp/codex-brief-nli-hang.md` | Codex run: 2026-05-25 | tokens: 79,791*
