# Incident 2026-05-25 — genizah-web hang due to synchronous NLI IIIF fetches

**Status:** Service restored by restart at 11:58 UTC. Root cause identified, code fix pending.
**Severity:** P1 (site unresponsive to external users for ~7 minutes).
**Owner:** Hillel.

---

## 1. Summary

genizah-web (PID 2705566) stopped responding to HTTP requests around 11:50 UTC on 2026-05-25.
`systemctl status` reported the service as `active (running)` and the Python process was alive,
but external `curl https://genizahsearch.com/` timed out at 15s. A user-initiated `systemctl restart`
at 11:57:12 UTC hung on SIGTERM for the full 90s grace period; systemd resorted to SIGKILL at
11:58:42. The replacement process started at 11:58:43 and the site recovered.

This was **not** a crash, **not** a memory leak (5.9G is the expected baseline for a service that
loads Tantivy indexes plus the pgp.db / fjms_enrichment.db / nli_crossref.db sidecars into RAM),
and **not** a NiceGUI bug. The `Client has been deleted but is still being used` warning that
appears in the journal at 11:23:15 is a downstream symptom (users disconnected because the site
was already slow) and is caught by the `except` clause at [web/pages/search.py:83](web/pages/search.py:83).

## 2. Root cause

Synchronous `requests.get()` calls to `https://iiif.nli.org.il/` with generous timeouts exhaust the
Starlette / FastAPI threadpool when NLI is slow.

The two hot lines are in [web/api.py](web/api.py):

| Line | Call | Timeout |
|------|------|---------|
| [web/api.py:680](web/api.py:680) | IIIF manifest fetch (`/IIIFv21/DOCID/PNX_MANUSCRIPTS{sys_id}-{suffix}/manifest`) | **15s** |
| [web/api.py:713](web/api.py:713) | MARC fallback (`/IIIFv21/marc/bib/{sys_id}`) | **10s** |

A single failing sys_id therefore blocks one threadpool worker for **up to 25 seconds**.
The endpoints that reach this code (`/api/fl_ids/{sys_id}`, `/api/browse`, puzzle folio resolution
at [web/api.py:1816](web/api.py:1816)) are declared with synchronous `def`, so each request
occupies a worker thread for its full duration. With Starlette's default threadpool of ~40
workers and the existing `NLI_MAX_CONCURRENT_FETCHES=8` semaphore in
[web/api.py:32](web/api.py:32), it takes only a few dozen concurrent NLI-touching requests
during an NLI slow-down to either:

1. Block all 8 semaphore slots, leaving the next 12 threads queued on
   `_nli_semaphore.acquire(timeout=20)` ([web/api.py:648](web/api.py:648)) — itself a 20s
   blocking call.
2. Cascade into the remaining threadpool capacity as additional request types stack up.

Result: from the outside, the service stops accepting new HTTP connections.

### Why SIGTERM timed out

A blocked `requests.get()` does not return until the socket times out. Python's default signal
handling will not interrupt a thread sitting inside a C-level socket recv. systemd waits
`TimeoutStopSec` (90s default) and SIGKILLs. Journal evidence:

```
11:57:12 systemd: Stopping genizah-web.service...
11:57:36 python : Failed to fetch FL IDs from IIIF manifest for 990001458630205171:
                  HTTPSConnectionPool(host='iiif.nli.org.il', port=443): Read timed out. (read timeout=15)
11:58:42 systemd: State 'stop-sigterm' timed out. Killing.
11:58:42 systemd: Killing process 2705566 (python) with signal SIGKILL.
```

### Existing mitigations that are working

These mitigations attenuate but do not eliminate the problem:

- **Negative cache** ([web/api.py:29](web/api.py:29)): `NLI_FAIL_CACHE_TTL=60s` — once a sys_id
  fails, subsequent calls return empty immediately for 60 seconds. Was added 2026-03-25
  (see [docs/OPEN_ISSUES.md:147](docs/OPEN_ISSUES.md)).
- **Concurrency cap** ([web/api.py:32](web/api.py:32), [web/api.py:41](web/api.py:41)):
  `NLI_MAX_CONCURRENT_FETCHES=8` via `threading.Semaphore`.
- **Persistent session** with connection pooling ([web/api.py:49-57](web/api.py:49)).

### Existing infrastructure that is NOT wired in

[genizah_core.py:3940-3961](genizah_core.py:3940) defines an NLI **circuit breaker**:

```python
_NLI_CIRCUIT_THRESHOLD = 3
_NLI_CIRCUIT_WINDOW = 60  # seconds
# _nli_circuit_is_open() / _nli_record_failure() / _nli_record_success()
```

This breaker is used by `genizah_core.py`'s own `fetch_marc_data` / IIIF paths
([genizah_core.py:4098](genizah_core.py:4098), [:4248](genizah_core.py:4248)), but
`web/api.py:fetch_fl_ids_from_nli` does **not** consult it. The two NLI paths fail
independently.

## 3. Evidence (journal excerpts)

```
May 25 10:49:21  WARNING: IIIF fetch failed for 990053470170205171 (suffix=1): Read timed out. (read timeout=5)
May 25 10:49:34  WARNING: IIIF fetch failed for 990052153770205171 (suffix=1): Read timed out. (read timeout=5)
May 25 10:49:35  WARNING: IIIF fetch failed for 990002090570205171 (suffix=1): Read timed out. (read timeout=5)
May 25 10:50:13  WARNING: IIIF fetch failed for 990001947120205171 (suffix=1): Read timed out. (read timeout=5)
May 25 11:23:15  NiceGUI: "Client has been deleted but is still being used"  (symptom of user disconnect)
May 25 11:50:33  Could not load storage file ...storage-user-d9dea360-....json   (last entry before silence)
May 25 11:57:12  systemd: Stopping genizah-web.service...                          (user-initiated restart)
May 25 11:57:36  Failed to fetch FL IDs from IIIF manifest for 990001458630205171: Read timed out. (read timeout=15)
May 25 11:58:01  Failed to fetch FL IDs from IIIF manifest for 990000845950205171: Read timed out. (read timeout=15)
May 25 11:58:42  systemd: State 'stop-sigterm' timed out. Killing.
May 25 11:58:43  systemd: Started genizah-web.service.
May 25 12:09:08  Failed to fetch FL IDs from IIIF manifest for 990001458630205171: Read timed out. (read timeout=15)
May 25 12:09:18  MARC fallback also failed for 990001458630205171: Read timed out. (read timeout=10)
```

The two distinct read-timeout values (`5` in the older log line from `genizah_core.py`, `15`+`10`
in `web/api.py`) confirm two independent NLI client paths in the codebase.

## 4. Remediation

Listed in priority order. (1) and (2) are the minimum required fix; (3)–(5) are hardening.

### (1) Shorten the NLI timeouts — `web/api.py`

Reduce the worst-case worker-block from 25s to ~8s.

```python
# web/api.py:680
resp = _nli_session.get(url, timeout=(3, 5), verify=True)        # (connect, read)
# web/api.py:713
resp = _nli_session.get(marc_url, timeout=(3, 3), verify=True)
```

Use a tuple so connect and read have independent budgets. Tune the read timeout downward —
the IIIF manifest is small JSON; a healthy NLI returns it in well under a second.

### (2) Wire `fetch_fl_ids_from_nli` to the existing circuit breaker

In `web/api.py:_fetch_fl_ids_network`, before the IIIF call:

```python
from genizah_core import GenizahCore  # or wherever _nli_circuit_is_open lives

if GenizahCore._nli_circuit_is_open():
    return []  # serve empty; negative-cache below will keep things calm
```

In the `except` handlers ([:707](web/api.py:707), [:731](web/api.py:731)):

```python
except Exception as e:
    logger.error(...)
    GenizahCore._nli_record_failure()
```

And on the success path ([:704](web/api.py:704), [:728](web/api.py:728)):

```python
GenizahCore._nli_record_success()
```

Effect: three consecutive NLI failures (across either web/api or genizah_core path) trip
a 60s shared cooldown. During cooldown, both paths short-circuit to empty without making
the network call at all.

### (3) Lower `NLI_SEMAPHORE_TIMEOUT`

[web/api.py:33](web/api.py:33) currently defaults to 20s. A thread waiting 20s for an NLI
slot is itself a blocked worker. Drop to 5s:

```python
NLI_SEMAPHORE_TIMEOUT = int(os.environ.get('NLI_SEMAPHORE_TIMEOUT', '5'))
```

Combined with (1), absolute worst case per request becomes `5 (sem wait) + 8 (IIIF+MARC) = 13s`,
down from the current `20 + 25 = 45s`.

### (4) Add an event-loop watchdog (optional, defensive)

A small background task that logs heartbeat every 60s, plus a check on each request that warns
if the request count stalls. The actionable signal is `time since last journal entry`. A
systemd timer that scrapes `journalctl -u genizah-web --since '1 minute ago'` and counts lines
would suffice; if zero for 5 minutes, alert.

### (5) Document the env knobs

Add the new defaults to [docs/guides/DEVELOPER_GUIDE.md:60](docs/guides/DEVELOPER_GUIDE.md) and
`CLAUDE.md` "Environment Variables" section. The relevant knobs:

| Variable | Old default | New default | Purpose |
|----------|-------------|-------------|---------|
| `NLI_FAIL_CACHE_TTL` | 60 (hardcoded) | 60 (no change) | Negative cache TTL |
| `NLI_MAX_CONCURRENT_FETCHES` | 8 | 8 (no change) | Semaphore slots |
| `NLI_SEMAPHORE_TIMEOUT` | 20 | **5** | Max wait for slot |
| *(new)* `NLI_IIIF_READ_TIMEOUT` | n/a | **5** | IIIF read timeout (seconds) |
| *(new)* `NLI_MARC_READ_TIMEOUT` | n/a | **3** | MARC fallback read timeout |

## 5. Verification plan

After applying (1) + (2):

1. **Unit test** — `tests/test_nli_circuit_breaker.py`: monkeypatch `_nli_session.get` to raise
   `requests.exceptions.ReadTimeout`. Call `fetch_fl_ids_from_nli` three times. Assert the
   fourth call returns `[]` without invoking `_nli_session.get` (circuit open). Sleep 61s
   (or monkeypatch time). Assert call 5 reaches the network again.

2. **Integration test** — `tests/test_fl_ids_endpoint_no_thread_block.py`: spawn 20 concurrent
   requests to `/api/fl_ids/990001458630205171` (a known-bad sys_id, or monkeypatched to fail).
   Measure total elapsed time. With the fix, total time should be ≲ 10s (one cohort of failures
   trips the breaker; subsequent requests return immediately). Without the fix it would be
   `20 / 8 * 25s = ~62s`.

3. **Production canary** — deploy, then `curl -w "%{time_total}\n" https://genizahsearch.com/api/fl_ids/990001458630205171`
   ten times in sequence. With the fix, after the first 1-3 calls return slowly, the rest
   should return in <0.1s (cache hit). Watch the journal: `Failed to fetch FL IDs` lines should
   appear at most 3 times in any 60-second window per sys_id, instead of one per request.

4. **Smoke** — confirm normal Cambridge/Oxford manuscript browse paths (which don't touch NLI)
   are unaffected.

## 6. Rollback

The fix is in `web/api.py` only — a single revert restores prior behavior. The new env vars all
have safe defaults that match or improve on current production behavior.

## 7. Out of scope (file as separate followups)

- **Async refactor** — moving `fetch_fl_ids_from_nli` to `httpx.AsyncClient` and rewriting the
  endpoints as `async def` would be the architecturally correct fix but is much larger. The
  shorter-timeout + circuit-breaker approach buys us the same outage resistance at ~1% of the
  code change.
- **`/api/browse` profiling** — the browse path also touches IIIF for image URL resolution.
  Worth a pass to confirm it benefits from the same circuit breaker.
- **`_refresh_user_session` token reuse error** at 12:09:14 — separate Supabase auth concern,
  not related to this incident.

## 8. Open question for Hillel

Should the circuit breaker state be **process-wide** (current `genizah_core.GenizahCore` class
attributes) or **per-sys_id**? Process-wide is simpler and matches the failure mode (NLI is
either reachable or not). Per-sys_id is more precise but mostly redundant with the existing
negative cache. Recommendation: keep it process-wide.
