# tracemalloc live attribution — 2026-06-06 (INTERIM)

PID 3418627, depth=5. Baseline 19:28:22 UTC, snapshot 19:37:28 UTC (~9 min window).

## Window was QUIET — interim only
- VmRSS 13,521,880 → 13,521,944 kb (**+64 KB**, essentially flat).
- RssAnon 9,981,856 → 9,981,920 kb (+64 KB).
- nli_cache 50,980 → 51,017 (+37 entries; negligible bytes).
- ⇒ the leak is **bursty / traffic-correlated**; it did not fire in this evening
  window, so tracemalloc caught mostly transient render allocations. A LONGER
  soak over a busy period is needed for a definitive byte attribution.

## Top growth allocators in the window (~4.3 MB total)
| bytes | count | site |
|------:|------:|------|
| 1.33 MB | 5 | /usr/lib/python3.12/weakref.py:167 (WeakValueDictionary internal table — large) |
| 775 KB | 4616 | nicegui/observables.py:23 (ObservableDict/List wrapping) |
| 219 KB | 2492 | weakref.py:348 |
| 176 KB | 1464 | inspect.py:265 |
| 148 KB | 3 | nicegui/functions/html.py:17 |
| 147 KB | 4567 | nicegui/observables.py:25 |
| 142 KB | 1443 | shared/thread_local_db.py:112 (thread-local SQLite) |
| 137 KB | 3212 | nicegui/binding.py:234 |
| ~48–72 KB ea | ~750 ea | nicegui element.py / classes.py / props.py / style.py / slot.py |
| 52 KB | 3 | web/pages/browse.py:148 |
| 45 KB | 575 | json/decoder.py:353 |

## Reading
The churn signature is **NiceGUI UI-tree objects** (Element/Style/Classes/Props/
Slot/binding/ObservableDict/weakref) + large WeakValueDictionary tables — i.e.
per-client/per-element allocation. Combined with most_common_types (Style 5797,
Slot 5632, Classes 5602, Props 5601, EventListener 1846 retained for only 9 live
clients) this points at **NiceGUI client / element retention** (disconnected
clients' element trees not reclaimed / pruners blocked), NOT nli_cache and NOT
the export-payload ObservableDict-wrapping theory from 2026-05-19. Matches the
ORIGINAL 2026-04 OPEN_ISSUES hypothesis.

## SECOND WINDOW — 2026-06-07 02:16 (6.6 h, the decisive read)
Baseline 19:37:28 (06-06) → snapshot 02:16:37 (06-07), ~6.6 h, same PID (no restart):
- VmRSS 13,521,944 → 13,527,444 kb = **+5.5 MB over 6.6 h (<1 MB/hr — essentially FLAT)**.
- RssAnon +3.7 MB. nli_cache 51,017 → 52,611 (+1,594). clients 11 / tab_storage 123.
- tracemalloc diff (group_by=traceback): ~26 MB of *tracked* allocation churn but RSS
  net +5.5 MB ⇒ overwhelmingly TRANSIENT. Top growers are framework/runtime churn:
  nicegui/observables.py:52 (ObservableDict wrapping, +40K objs across 2 entries),
  concurrent/futures/thread.py:58/:92 (threadpool WorkItems, +90K objs),
  threading.py:1010, asyncio/runners.py:194, inspect.py:3054; app code only
  genizah_core.py:4055 (IIIF/MARC negative-cache region) + :4628 (`current_meta['images']`
  enrichment) — both transient per-enrichment, count-correlated with traffic.
  text_element.py:15 +1.31MB/+1 offset by button.py:36 −1.31MB/−1 (a single big element
  re-attributed, not growth).

## CONCLUSION (supersedes the "find the leak" framing)
**The process is NOT actively leaking in these windows — it has PLATEAUED around
13.5 GB.** Two windows (9 min + 6.6 h) both show flat RSS. The dramatic 1.78 GB →
13.5 GB rise over the first ~3 days was **front-loaded** (working set / caches /
NiceGUI sessions filling to steady state), now leveled off. No retained app-level
surface dominates; churn is normal NiceGUI + threadpool transients.

**Caveat:** both windows were OFF-PEAK (evening + overnight Israel time). One
daytime-peak RSS check (cheap `/_internal/memstat` polls, no tracemalloc needed)
would make the plateau conclusive vs. a possible peak-traffic-correlated climb.

## Actions
- tracemalloc STOPPED (overhead removed).
- nli_cache bound (committed `592c984e`) = hygiene; data confirms it is NOT the driver.
- Pragmatic fix for a high-but-plateaued working set: systemd `MemoryHigh`/`MemoryMax`
  + periodic (e.g. weekly) restart, rather than a code leak-hunt.
- Optional: lower baseline (csv_bank/translations resident set) only if 13 GB is too
  high for the box.
