---
title: "Verify memstat growth rate after export_search_payload cap fix"
created: 2026-05-18
area: web
priority: high
source: P1 memory leak hotfix (commit f2e456d4) — follow-up verification
verify_after: 2026-05-18T19:00:00Z   # ~5h after the fix shipped at 14:01 UTC
---

# Verify memstat growth rate after `export_search_payload` cap fix

## Context

The P1 web memory leak was traced to `web/export_state.py` persisting full
search/parallels result lists into `app.storage.user`. Fix shipped
2026-05-18T14:01 UTC (commit `f2e456d4`) capping payload at 5,000 results.

OPEN_ISSUES.md P1 entry was flipped to ✅ Fixed — but the fix is
**provisionally** closed pending growth-rate verification under real traffic.

## Baseline (fresh process at ~1 min uptime, 19 reconnected clients)

| Metric | Value |
|---|---|
| VmRSS | 1,784,268 kB (1.78 GB) |
| Top live user payload | 511,991 bytes (512 KB) |
| `.nicegui/` total | 198 MB |
| Largest user file on disk | 4,586,677 bytes (4.5 MB) |
| Prior unfixed growth rate | ~300 MB/hr |

## What to check

5+ hours after fix ship (so ≥ 2026-05-18T19:00 UTC):

```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com \
  'PID=$(systemctl show genizah-web.service -p MainPID --value);
   SECRET=$(sudo bash -c "tr \"\\0\" \"\\n\" < /proc/$PID/environ" \
     | grep -a "^MEMSTAT_SECRET=" | cut -d= -f2-);
   curl -s -H "X-Memstat-Secret: $SECRET" http://localhost:8081/_internal/memstat \
     | python3 -m json.tool'
```

Compute growth rate:
```
(VmRSS_kb_now - 1_784_268) / uptime_hours_since_restart
```

## Verdict thresholds

| Growth rate | Verdict |
|---|---|
| < 30 MB/hr | ✅ Fix is solid. Close the follow-up cleanly. |
| 30–100 MB/hr | 🟡 Acceptable but watch. Probably normal cache warmup + IIIF manifest accumulation. Re-check next day. |
| > 100 MB/hr | ⚠️ Secondary leak. Re-instrument with `/_internal/objgraph` + `tracemalloc` and attribute the next surface. The OPEN_ISSUES P1 entry lists candidate surfaces: NLI/IIIF manifest cache, csv_bank, detached `asyncio.ensure_future`/`ui.timer` callbacks, image-bytes buffers on puzzle/visual-similarity. |

## Output

If verdict is ✅: update `docs/OPEN_ISSUES.md` change log with the post-soak
RSS reading and "P1 closure verified by N-hour soak". Close this TODO.

If verdict is ⚠️: re-open the P1 entry with new findings, file a follow-up
phase, and capture a `/_internal/objgraph` baseline immediately so the next
attribution path has data.

---

## Verdict (executed 2026-05-19 by quick task 260519-9pk)

**Outcome: warning band -- secondary leak.**

| Metric | Value |
|---|---|
| Post-deploy baseline (2026-05-18 15:41:13 UTC, fresh process @ ~1 min) | VmRSS 1,784,268 kB = 1.78 GB |
| 11h soak reading (2026-05-19, `systemctl status genizah-web.service`) | Memory 6.3G, peak 6.8G, Tasks 31, PID 2332735 |
| Elapsed | ~11 hours |
| **Computed growth rate** | (6.3 GB - 1.78 GB) / 11 h ~= **411 MB/hr** |
| **Verdict band** | >100 MB/hr -- secondary leak; WORSE than the pre-fix 300 MB/hr observation |

**What the cap fix DID solve (capped surface confirmed closed):**

- Top live user payload: 498 MB -> 512 KB
- Top file on disk: 906 MB -> 4.5 MB
- `.nicegui/` total: 2.0 GB -> 198 MB
- Worst-case `export_search_payload` payload now ~80 MB; typical <5 MB

So `web/export_state.py` is no longer the dominant retention surface. A
different surface is leaking ~411 MB/hr.

### Actions executed per the "If verdict is warning" branch

1. **P1 OPEN_ISSUES entry re-opened** -- `docs/OPEN_ISSUES.md` line ~79 status
   cell flipped from `Fixed (2026-05-18)` -> `Partially Fixed (2026-05-18 export cap); Secondary leak Re-opened (2026-05-19)`.
   The Notes column got a "RE-OPENED 2026-05-19" preamble. Quick Summary counts
   updated: P1 Open 0->1; Total Open 33->34. Change log row appended. Last Updated
   stamp bumped to 2026-05-19.
2. **Follow-up phase scoped** at `.planning/todos/pending/2026-05-19-leak-attribution-phase.md`
   covering: candidate suspect surfaces (NLI/IIIF manifest cache, csv_bank,
   detached asyncio.ensure_future / ui.timer callbacks, image-byte buffers on
   puzzle/visual-similarity paths, image-adjustment LUTs, AND the new
   Phase 92.2 WeakKeyDictionary task-memo on get_user_client()); investigation
   approach (add `/_internal/objgraph` + `/_internal/tracemalloc` companion
   endpoints next to the existing `/_internal/memstat` to get live attribution
   data); done-criteria (RSS growth rate confirmed <30 MB/hr in a 24h soak).
3. **objgraph baseline NOT captured in this quick task** -- the existing todo
   text said "capture a `/_internal/objgraph` baseline immediately so the next
   attribution path has data", but that requires adding the endpoint first
   (the endpoint does not exist yet -- only `/_internal/memstat` does, at
   `web/main.py:~125`). Adding the endpoint is a code change scoped IN the
   v7.13-followup attribution phase, NOT in this quick docs-only task. The
   first action of that follow-up phase is to add the endpoint AND capture the
   first baseline in the same plan.

### Verdict outcome: TODO complete

Migrating this file from `.planning/todos/pending/` to `.planning/todos/done/`
after verdict has been recorded above.
