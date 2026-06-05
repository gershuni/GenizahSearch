---
phase: 108-desktop-join-workbench-query-builders-candidates-compare
reviewed: 2026-06-05T00:00:00Z
depth: standard
files_reviewed: 8
files_reviewed_list:
  - desktop/join_workbench.py
  - genizah_app.py
  - genizah_translations.py
  - shared/fjms_service.py
  - tests/test_fjms_service.py
  - tests/test_join_workbench_builder.py
  - tests/test_join_workbench_triage.py
  - tests/test_tabular_builder_rtl.py
findings:
  critical: 0
  warning: 4
  info: 5
  total: 9
status: issues_found
---

# Phase 108: Code Review Report

**Reviewed:** 2026-06-05T00:00:00Z
**Depth:** standard
**Files Reviewed:** 8
**Status:** issues_found

## Summary

Reviewed the Phase 108 Desktop Join Workbench work: the largely-new
`desktop/join_workbench.py` (query builders, candidate pane, compare dialog,
QThread workers), the `open_anchor_as_join` extension in `genizah_app.py`, the
`fjms_service.get_measurement_summaries_batch` reuse, and the four headless test
files. Scope notes for `genizah_translations.py` below.

Overall the code is careful and high-quality: generation-token latest-wins
semantics are consistently applied to the gen-carrying workers, `None`-page
guards are present at the `_enqueue_image_for_pane` boundary (RR-12), the
parser-hoist query composition is correct and matches the test-pinned contract
(RR-1/RR-13), and `open_anchor_as_join` preserves backward compatibility with
the Phase-107 no-partner call path (new kwargs default to `None`, and the B
field is only pre-filled when a partner is supplied). `RuntimeError` guards for
deleted widgets are applied pervasively.

The findings below are mostly correctness gaps in the candidate-pane search
flow. The most consequential (WR-01) is that the main `SearchThread` —
unlike every other worker in the module — is NOT generation-guarded, so a
stale search can clobber newer results. None are security issues; none are
release blockers on their own, but WR-01 and WR-02 are user-visible race
conditions worth addressing.

## Warnings

### WR-01: Main `SearchThread` is not generation-guarded — stale results can clobber newer ones

**File:** `desktop/join_workbench.py:1936-1954` (`do_search`), `:1956-2009` (`_on_results`)
**Issue:** Every gen-carrying worker in this module (`_AnchorLoadWorker`,
`_PageTextWorker`, `_KnownJoinsLoadWorker`, `ThumbBatchWorker`, image loaders)
correctly drops stale emissions via `if gen != self._gen: return`. The
candidate-pane `SearchThread`, however, carries no generation token and
`_on_results` performs no staleness check. `do_search` "cancels" the previous
search with:

```python
if self._search_thread is not None:
    try:
        self._search_thread.quit()   # only stops the event loop, NOT a blocking run()
    except Exception:
        pass
    self._search_thread = None
```

`QThread.quit()` only asks the thread's event loop to exit; it does NOT
interrupt the blocking `searcher.execute_search()` call inside `SearchThread.run`
(see `gui_threads.py:96-110` — the only interruption path is `cancel_flag`,
checked solely in the progress callback, and `do_search` never sets it). The old
thread keeps running and its `results_signal` stays connected to `_on_results`.
If a scholar edits the query and re-runs while the first (slower) search is still
in flight, the older search can finish second and overwrite `self._text_cands` /
`self.results` with results for the abandoned query.
**Fix:** Adopt the same gen-token pattern used everywhere else in the window.
Capture `gen = self.wb._gen` (or a pane-local counter) at search start, pass it
through, and drop stale emissions. Minimal version using a pane-local counter:

```python
def do_search(self):
    ...
    self._search_gen = getattr(self, "_search_gen", 0) + 1
    gen = self._search_gen
    if self._search_thread is not None:
        try:
            self._search_thread.cancel_flag = True   # actually signals run()
        except Exception:
            pass
    self._search_thread = SearchThread(...)
    self._search_thread.results_signal.connect(
        lambda raw, g=gen: self._on_results(g, raw)
    )
    self._search_thread.start()

def _on_results(self, gen, raw):
    if gen != self._search_gen:
        return  # stale search — drop
    ...
```

### WR-02: `_CrossSideWorker` and `_EnrichWorker` results are not generation-guarded

**File:** `desktop/join_workbench.py:2011-2014` (`_on_cross_done`), `:2061-2084` (`_on_enriched`)
**Issue:** Same class of race as WR-01, one layer down. `_on_cross_done` and
`_on_enriched` unconditionally adopt the worker payload (`self._text_cands = ...`,
`self._enrich = enrich`). The workers are cancelled best-effort
(`self._cross_worker.cancel()` / `self._enrich_worker.cancel()`), but
`_CrossSideWorker.run` checks no `_cancel` flag at all (it has the attribute but
never reads it), and `_EnrichWorker` only checks `_cancel` between candidates —
either can still emit after a newer search has started, replacing the current
candidate set with a stale one. Because these are chained off `_on_results`, the
WR-01 race compounds here.
**Fix:** Thread the same `gen` token through `_CrossSideWorker` and
`_EnrichWorker` (constructor arg + emit it back), and have `_on_cross_done` /
`_on_enriched` drop stale gens. At minimum, add a `_cancel` early-return at the
top of `_CrossSideWorker.run` so a cancelled cross-side worker does not emit.

### WR-03: `_remove_box` clears the active row even when removing a non-focused OR-box

**File:** `desktop/join_workbench.py:954-967` (`_remove_box`)
**Issue:** `_remove_box` nulls `self._active_row` whenever the removed box belongs
to the active row:

```python
if self._active_row is entry:
    self._active_row = None
```

But removing one OR-alternative does not de-focus the row — the row still exists
and other boxes in it remain. After this, `_on_modifier_changed` early-returns
(`self._active_row is None`), so toggling a modifier checkbox silently does
nothing until the user clicks back into a box, even though the row is visibly
still "the one being edited." The comment cites RR-16, but RR-16's intent is to
clear the reference when the row/box is *gone*; here the row survives. Compare
`_remove_row` (`:969-981`), where clearing is correct because the whole row
disappears.
**Fix:** Only clear `_active_row` in `_remove_box` if the box being removed was
the actually-focused widget (and ideally re-point `_active_row` to the surviving
row), e.g.:

```python
if self._active_row is entry and box["edit"].hasFocus():
    self._active_row = None
```

or simply do not clear it at all in `_remove_box` (the row is still valid), and
call `self._refresh_modifier_enabled()` which already re-evaluates the multi-box
state.

### WR-04: `apply_filters` size filter rejects candidates with no width, even when the range is full (0–200)

**File:** `desktop/join_workbench.py:2098-2131` (`apply_filters`)
**Issue:** When the opt-in size filter is enabled, candidates whose width is
unknown are dropped:

```python
if size_active:
    w = m.get("width_cm")
    if w is None or not (size_min <= w <= size_max):
        continue
```

With the default spinbox values (`size_min=0`, `size_max=200`) the *intent* is
"no narrowing," but every candidate lacking measurement data is silently
removed the moment the user merely expands the Size filter panel — even before
they touch the sliders. In a corpus where measurement coverage is partial
(~12K of the Genizah corpus per the FJMS notes), this can hide the majority of
real candidates with no visible cause. The Material/dimensions filters elsewhere
are explicit opt-ins (`need_dims`), but the size panel toggle alone shouldn't
behave like a hard "has dimensions" filter.
**Fix:** Treat a full-range window as "no width restriction," and/or only drop
`w is None` rows when the user has actually narrowed the bounds:

```python
if size_active and (size_min > 0 or size_max < 200):
    w = m.get("width_cm")
    if w is None or not (size_min <= w <= size_max):
        continue
```

Alternatively keep `w is None` rows unless a dedicated "has dimensions" checkbox
is on (consistent with `need_dims`).

## Info

### IN-01: `genizah_translations.py` change not visible in this diff slice

**File:** `genizah_translations.py`
**Issue:** Only the first 100 lines were in scope here and none of the many new
Phase-108 UI strings (`tr("+ Add Line")`, `tr("Find Candidates")`,
`tr("Visual similarities")`, `tr("⚓ anchor matches this query ✓  ·  ")`,
`tr("other side matched")`, the size/triage filter labels, etc.) appear in the
visible range. If those keys are absent from `TRANSLATIONS`, the dual `tr()`
implementation is language-gated so English users see the literal English (no
crash), but Hebrew users will see raw English strings leaking through. This
matches the known i18n-audit pattern in MEMORY.
**Fix:** Run the i18n audit (`_tmp/find_missing_tr2.py` per MEMORY) against the
new `desktop/join_workbench.py` literals and confirm HE keys exist for all
user-facing strings before release. Non-blocking, but worth a pass.

### IN-02: Several builder tooltips/options bypass `tr()` (English-only)

**File:** `desktop/join_workbench.py:1696-1698` (other-side tooltip), `:2700-2710` (zoom tooltips)
**Issue:** A handful of user-facing strings are hardcoded English rather than
wrapped in `tr()`:
- `self.other_enable.setToolTip("AND narrows: ...\nOR widens: ...")` (`:1695-1698`)
- Zoom/folio button tooltips and accessible names: `"Zoom out"`, `"Zoom in"`,
  `"Previous folio"`, `"Next folio"` (`:2700-2727`).

These will not localize to Hebrew. Most other strings in the file correctly use
`tr()`, so this is an inconsistency rather than a systemic gap.
**Fix:** Wrap these literals in `tr(...)` and add HE keys.

### IN-03: `_KnownJoinsLoadWorker` uses FJMS `alma_id` as a shelfmark fallback

**File:** `desktop/join_workbench.py:513-527`
**Issue:** When `get_meta_for_id(alma)` fails or returns `"Unknown"`, the code
falls back to using the raw alma/system id as the shelfmark
(`shelf = alma`), which then becomes `fragment_b` and is displayed to the
scholar as if it were a human-readable shelfmark. The 99000…-format id is not a
shelfmark and will look like noise in the joins panel. This is a graceful-
degradation choice, not a bug, but worth a comment or a clearer placeholder.
**Fix:** Use an explicit placeholder (e.g. `tr("(unresolved)")`) or skip
display of the shelfmark when resolution fails, rather than surfacing the alma
id as a shelfmark.

### IN-04: `_build_join_row` mislabels the member title using `fragment_b`

**File:** `desktop/join_workbench.py:3528-3534`
**Issue:** The secondary "title" line under each known-join member shelfmark is
populated from `row.get("fragment_b")`:

```python
title_text = (row.get("fragment_b") or "")[:60]
```

But `fragment_b` is a shelfmark string (set throughout `_KnownJoinsLoadWorker`),
not a title — so the row shows the shelfmark twice (once as `other_shelf` in
`shelf_label`, once truncated here as a pseudo-title). For PGP/FJMS rows where
the anchor is on side A, `fragment_b` is the member shelfmark and duplicates the
line above; for transitive edges not touching the anchor it may be an unrelated
fragment. No crash, but the second line is misleading/redundant.
**Fix:** Either drop the second label, or resolve and show the actual manuscript
title (via `meta_mgr`) instead of `fragment_b`.

### IN-05: `meta_brief` image count can be misleading when both image lists are non-empty

**File:** `desktop/join_workbench.py:103-109`
**Issue:** `meta_brief` computes the image count as
`len(meta.get("images_nli") or meta.get("images_ext") or [])`. Because `or`
short-circuits on the first truthy list, the count reflects only `images_nli`
when it is non-empty, even if the prioritized `images` list (ext-first per
must-fix #4 in `_AnchorLoadWorker`) would have a different length. The "N img"
summary can therefore disagree with the folio counter
(`len(self._anchor_images)`), which is derived from `meta.get("images")`. Minor
cosmetic inconsistency only.
**Fix:** Use `len(meta.get("images") or [])` for consistency with the
already-prioritized list the rest of the window uses.

---

_Reviewed: 2026-06-05T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
