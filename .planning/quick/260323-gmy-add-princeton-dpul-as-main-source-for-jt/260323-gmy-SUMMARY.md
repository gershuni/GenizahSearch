---
quick_id: 260323-gmy
status: completed
commit: 36ebe881
---

# Quick Task 260323-gmy: Summary

## What Changed

**web/pages/browse.py**: JTS manuscripts now auto-default to Princeton DPUL images instead of NLI.

### Implementation

Added `state.source_user_override` flag to `BrowseState`:
- **False** (default): auto-default to JTS source when DPUL images are available
- **True**: user explicitly clicked a source button — respect their choice
- Reset to **False** on every manuscript navigation (search, suggestion select, prev/next)

Auto-default logic inserted before source-active computation in the render function:
```python
if _has_jts_images and state.active_source == 'nli' and not state.source_user_override:
    state.active_source = 'jts'
```

### Behavior

- Browse JTS manuscript → DPUL images shown by default (orange "JTS" chip active)
- Click "NLI" chip → switches to NLI, stays on NLI (user override respected)
- Navigate to next manuscript → resets to DPUL default
- Non-JTS manuscripts → no change (NLI default)
- Desktop app already defaulted to external images — no change needed

### Tests

464 tests pass. 1 pre-existing failure in test_puzzle_model (unrelated default value mismatch).
