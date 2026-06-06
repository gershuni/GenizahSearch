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

## Next
- Leave tracemalloc running (baseline now reset to 19:37:28). Snapshot again after
  a LONGER, busier window (hours / peak traffic) to catch the leak firing and pin
  the file:line. Stop tracemalloc (`?action=stop`) once captured to drop overhead.
- nli_cache bound (genizah_core.py `_BoundedLRUCache`) is implemented + tested but
  is hygiene — this data says it is NOT the multi-GB driver.
- Cross-check NiceGUI prune (`prune_user_storage`/client GC) + disconnect handlers.
