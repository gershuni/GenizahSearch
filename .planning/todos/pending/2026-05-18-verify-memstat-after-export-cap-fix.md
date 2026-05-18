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
