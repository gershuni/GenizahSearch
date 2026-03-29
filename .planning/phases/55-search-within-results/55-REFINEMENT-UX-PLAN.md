# Phase 55 UX Revision: Search-Within-Results Clarity & Same-Page Filter

**Date:** 2026-03-29
**Status:** Draft for review
**Scope:** Both web (NiceGUI) and desktop (PyQt6)

## Problem Statement

After initial Phase 55 implementation and user testing, three UX issues emerged:

### 1. Misleading unit in button/badge text
"Search within 178 results" implies restricting to those 178 page-level results. In reality, the restriction operates on **manuscripts** (sys_ids) — all pages of matching manuscripts are eligible. A manuscript with "הגמון" on page 1 shows up in results for "גמליאל" on page 5.

### 2. Results don't all contain earlier chain terms
Users expect every result to contain ALL chain terms. Because restriction is manuscript-level, results may show pages that only match the LAST term. This is confusing — "I searched הגמון then גמליאל, but many results don't contain הגמון."

### 3. Order-dependent result counts (non-commutative)
Searching A→B gives different counts than B→A because different manuscripts have different page densities per term. Users expect commutativity.

## Design Decision

**Keep manuscript-level restriction as the default** (broader, more useful for scholarship — manuscripts where both terms appear anywhere are relevant). Add an **opt-in page-level post-filter** for users who want stricter results.

## Proposed Changes

### A. Label Changes (both apps)

| Element | Current | Proposed EN | Proposed HE |
|---------|---------|-------------|-------------|
| Button | "Search within 178" | "Search within 120 manuscripts" | "חפש בתוך 120 כתבי יד" |
| Badge | "Refining within 178 results" | "Searching within 120 manuscripts" | "מחפש בתוך 120 כתבי יד" |
| Breadcrumb count | 178 (page count) | 97 (page count) | — |

The button/badge show **unique manuscript count** (`len(set(sys_ids))`), making it clear the unit is manuscripts. The breadcrumb still shows total page-level results (matches what's displayed).

### B. "Only results with all terms" Checkbox (both apps)

A checkbox on the refinement breadcrumb strip that post-filters displayed results:

```
[הגמון ✕] › [גמליאל ✕]  97  ☐ רק תוצאות עם כל המונחים  |  נקה הכל
```

**Behavior:**
- **Unchecked (default):** Show all results from matching manuscripts (current behavior)
- **Checked:** Post-filter displayed results to only keep pages whose `sys_id` appeared in EVERY text-search step's result set

**Implementation:**
1. Store per-step result sys_ids in `RefinementStep.result_sys_ids: set` (not persisted to session — rebuilt on replay)
2. When checkbox is checked, compute intersection of all steps' `result_sys_ids` → `common_sys_ids`
3. Filter `search_state.results` / `self.last_results` to only show results where `display.id in common_sys_ids`
4. Update displayed count, breadcrumb count, and "Search within N manuscripts" button
5. Checkbox state persisted in session

**Edge cases:**
- **Metadata modes (Title/Shelfmark):** These return manuscript-level results, not page-level. A Title search step has `result_sys_ids` = the matching manuscript IDs. Intersection still works correctly — it narrows to manuscripts matching ALL criteria.
- **Single-step chain:** Checkbox is hidden (no earlier terms to intersect with)
- **Zero intersection:** Show "0 results — no manuscripts match all terms" with recovery button
- **Cross-mode chains:** Intersection is always at sys_id level regardless of mode, so it works naturally

### C. Snippet Highlighting (already implemented)

The `enrich_snippet_with_chain_terms()` function already adds `*markers*` for earlier chain terms in snippets. This helps users see where earlier terms appear even in manuscript-level mode.

### D. Files Modified

| File | Changes |
|------|---------|
| `shared/refinement.py` | Add `result_sys_ids` field to RefinementStep (non-persisted), update `replay_chain` to populate it |
| `web/pages/search.py` | Label text changes, checkbox widget on refinement strip, post-filter logic in render_results |
| `genizah_app.py` | Same label/checkbox/filter changes for desktop |
| `genizah_translations.py` | New translation strings |
| `tests/test_refinement.py` | Tests for intersection filter logic |

### E. Implementation Detail: RefinementStep Changes

```python
@dataclass
class RefinementStep:
    query: str
    mode: str
    gap: int = 0
    exclude_words: list = field(default_factory=list)
    text_position: Optional[str] = None
    responsa_options: Optional[dict] = None
    result_count: int = 0
    # NEW: page-level result sys_ids for same-page filter (not serialized)
    _result_sys_ids: set = field(default_factory=set, repr=False)
```

`to_dict()` excludes `_result_sys_ids` (underscore prefix, not in serialization).
`replay_chain()` populates `_result_sys_ids` for each step during replay.
`_enter_refine_mode()` populates step 0's `_result_sys_ids` from current results.

### F. Post-Filter Logic

```python
def compute_all_terms_filter(chain: list[RefinementStep]) -> set | None:
    """Return sys_ids that appear in ALL steps' result sets, or None if filter disabled."""
    if len(chain) < 2:
        return None
    sets = [s._result_sys_ids for s in chain if s._result_sys_ids]
    if not sets:
        return None
    return set.intersection(*sets)
```

Both apps call this when checkbox is checked, then filter displayed results.

### G. What This Does NOT Change

- Search engine (`genizah_core.py`) — no changes needed
- `gui_threads.py` — no changes
- Session persistence format — `_result_sys_ids` excluded from serialization
- Restriction mechanism — still manuscript-level via `restrict_sys_ids`
- Chain replay logic — same, just also captures per-step sys_ids

## Risks

1. **Memory:** Storing per-step sys_id sets (up to ~200K IDs per step). At 8 bytes per int, a 10K-ID set is ~80KB. Negligible for expected chain depths (2-5 steps).
2. **Replay performance:** `replay_chain` already replays all steps. Capturing sys_ids is O(1) per result — no additional search calls.
3. **UX confusion:** Users might not understand why some results disappear when checkbox is toggled. Mitigation: show count change inline: "97 → 44 results (matching all terms)".
