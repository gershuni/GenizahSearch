---
status: resolved
created: 2026-03-29
updated: 2026-03-29
---

# Desktop Exclusion Not Filtering Results

## Symptom

After loading a list into the ExcludeDialog (via "From List" → "Load to Editor" → Apply), the desktop search results table still shows manuscripts that should be excluded. The same exclusion set works correctly on web.

Example: searching `הזן את העולם`, excluding ~125 manuscripts from a saved list. Web correctly hides them in the "Excluded manuscripts" collapsible section. Desktop shows them in the results table (e.g., `MS heb. e.96/1`, `T-S NS 111.138` still visible).

## Architecture Context

### How desktop results are rendered

1. `on_search_finished(results)` is called with the full result list
2. It sets `self.last_results = results` and calls `load_next_batch()`
3. `load_next_batch()` iterates `self.last_results[start:end]` and adds rows to `self.results_table` (QTableWidget)
4. There is NO exclusion filter in `load_next_batch()` — it renders everything in `self.last_results`

### How exclusions work (composition search — working)

In composition search, exclusion is applied DURING search:
- `excluded_ids` is passed to the search thread (`LabCompositionThread` / `SearchThread`)
- The search engine excludes them from results before returning
- `on_comp_finished()` also calls `_apply_manual_exclusions(main_res, main_appx)` which filters the results post-search

### How exclusions work (regular search — broken)

For regular search, exclusion was historically applied via `_apply_manual_exclusions()` which is called in `on_comp_finished` but NOT in `on_search_finished`. The regular search path never had post-search exclusion filtering in the rendering pipeline.

The old `_item_matches_exclusion()` approach checked `self.excluded_sys_ids` and `self.excluded_shelfmarks`, but it was only used in composition search display code, not regular search.

## What I Tried

### Attempt 1: Call `on_search_finished(filtered_results)`

Added `_rerender_with_exclusions()` which filters `self.last_results` by `self.excluded_sys_ids` and passes the filtered list to `on_search_finished()`.

**Problem:** `on_search_finished` sets `self._unfiltered_last_results = results` and `self.last_results = results`. So on second call, the "unfiltered" copy was already filtered, and excluded items reappeared.

### Attempt 2: Guard with `_rerendering_exclusions` flag

Added a flag to prevent `on_search_finished` from overwriting `_unfiltered_last_results` during exclusion re-renders.

**Problem:** Still not working. Likely because `on_search_finished` does much more than just render — it resets enrichment workers, domain data, printed IDs, refinement state, etc. Calling it as a "re-render" is a heavy hammer that has side effects. The enrichment workers may re-trigger `_render_with_filters` which doesn't know about exclusions, or the results may get overwritten by a background enrichment callback.

### Root Cause Analysis

The fundamental issue: **regular search has no exclusion hook in its rendering pipeline**. The composition search has `_apply_manual_exclusions()` wired into `on_comp_finished()` and `display_comp_results()`. But regular search renders results directly in `load_next_batch()` which reads `self.last_results` with no filtering step.

## Proposed Approaches

### Approach A: Filter in `load_next_batch` (Recommended — minimal change)

Add exclusion filtering directly in `load_next_batch()` at the point where it reads from `self.last_results`:

```python
def load_next_batch(self, batch_size=None):
    if self.results_loaded >= len(self.last_results):
        return
    start_idx = self.results_loaded
    end_idx = min(start_idx + (batch_size or BATCH_SIZE), len(self.last_results))
    batch = self.last_results[start_idx:end_idx]

    # Phase 56: Filter out excluded manuscripts
    if self.excluded_sys_ids:
        batch = [r for r in batch if r.get('display', {}).get('id') not in self.excluded_sys_ids]

    if not batch: return
    # ... rest of rendering
```

**Pros:** Minimal change, works for all paths that call `load_next_batch`, no need to re-call `on_search_finished`.
**Cons:** Result count in status label won't reflect exclusions (shows total, not filtered). The `results_loaded` counter may drift since filtered items are counted but not rendered.

To fix the count issue, also update the status label after `on_search_finished` completes:
```python
if self.excluded_sys_ids:
    visible_count = sum(1 for r in self.last_results if r.get('display', {}).get('id') not in self.excluded_sys_ids)
    self.status_label.setText(tr("Showing {} of {} results ({} excluded)").format(visible_count, len(self.last_results), len(self.last_results) - visible_count))
```

And when exclusions change, just clear and re-load the table:
```python
def _rerender_with_exclusions(self):
    self.results_loaded = 0
    self.results_table.setRowCount(0)
    self.load_next_batch()
    # Update status label with exclusion count
```

### Approach B: Pre-filter `self.last_results` before `on_search_finished`

Store the raw search results in a separate field (`_raw_search_results`) and always derive `self.last_results` from it:

```python
# In open_exclude_dialog after Apply:
if self._raw_search_results:
    if self.excluded_sys_ids:
        self.last_results = [r for r in self._raw_search_results
                             if r.get('display', {}).get('id') not in self.excluded_sys_ids]
    else:
        self.last_results = self._raw_search_results
    self.results_loaded = 0
    self.results_table.setRowCount(0)
    self.load_next_batch()
```

**Pros:** Clean separation of raw and filtered results.
**Cons:** Need to set `_raw_search_results` in `on_search_finished`, and ensure all code that reads `self.last_results` gets the filtered version. More fields to track.

### Approach C: Skip `on_search_finished`, directly manipulate table rows

Instead of re-rendering from scratch, hide/show existing table rows:

```python
def _rerender_with_exclusions(self):
    for row in range(self.results_table.rowCount()):
        item = self.results_table.item(row, self.COL_CHECKBOX)
        if item:
            result = item.data(Qt.ItemDataRole.UserRole)
            sid = result.get('display', {}).get('id') if result else None
            should_hide = sid and sid in self.excluded_sys_ids
            self.results_table.setRowHidden(row, should_hide)
    # Update count
    visible = sum(1 for row in range(self.results_table.rowCount()) if not self.results_table.isRowHidden(row))
    self.status_label.setText(...)
```

**Pros:** No re-rendering, instant, preserves enrichment state (domain badges, printed indicators, etc).
**Cons:** Hidden rows still in memory. Pagination counter may be wrong. Export features need to skip hidden rows. But for a first fix this is the simplest and least disruptive approach.

## Key Files

| File | Line | What |
|------|------|------|
| `genizah_app.py` | ~24455 | `load_next_batch()` — renders results to QTableWidget |
| `genizah_app.py` | ~24700 | `on_search_finished()` — sets last_results, calls load_next_batch |
| `genizah_app.py` | ~27405 | `_rerender_with_exclusions()` — current broken attempt |
| `genizah_app.py` | ~27333 | `open_exclude_dialog()` — calls _rerender after Apply |
| `genizah_app.py` | ~27470 | `_item_matches_exclusion()` — per-item check (used by composition only) |
| `genizah_app.py` | ~27496 | `_apply_manual_exclusions()` — filters main+appendix (composition only) |

## Recommendation

**Approach C (hide rows)** for immediate fix — it's 10 lines, no side effects, works with all existing enrichment.
Then optionally refactor to **Approach A** for a cleaner long-term solution if needed.
