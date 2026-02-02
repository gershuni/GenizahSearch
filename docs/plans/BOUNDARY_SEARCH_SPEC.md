# Technical Specification: Boundary-Crossing Parallel Search

> Date: February 2026
> Status: Draft v2 - Post Review
> Author: Claude Code

## Executive Summary

A new parallel search feature that identifies and prioritizes word sequences crossing paragraph boundaries in the source text. The rationale: such sequences are strong candidates for true literary parallels, as opposed to biblical quotations or formulaic phrases that typically appear within a single paragraph unit.

**Key Design Decision**: Rather than creating separate boundary chunks, the algorithm identifies which *existing* regular chunks cross user-defined boundaries and boosts their scores accordingly.

---

## 1. Goals and Rationale

### 1.1 Primary Goal
Enable users to find robust literary parallels by prioritizing matches on word sequences that span paragraph boundaries.

### 1.2 Key Insight
When two manuscripts share text that crosses a structural boundary (end of one paragraph + beginning of another), this is unlikely to be:
- A **biblical quotation** (which would be quoted as a complete unit)
- A **formulaic phrase** (which are typically short, self-contained expressions)
- A **coincidental match** (the probability of matching across boundaries is lower)

### 1.3 Problem-Solution Matrix

| Problem with Current Search | Boundary Search Solution |
|----------------------------|--------------------------|
| Biblical quotes appear as results | Complete quotes within paragraphs are excluded |
| Formulaic phrases create noise | Short in-paragraph phrases are filtered out |
| Hard to identify structural parallels | Cross-boundary match = structural similarity |
| Many low-quality matches | Fewer but higher-confidence results |

---

## 2. Three Search Modes

### 2.1 Full Mode (Default)
- **UI Label**: "Full search"
- **Description**: Searches all chunks, same as current behavior
- **Addition**: Option to filter results by minimum boundary-crossing matches (post-hoc filter)
- **Use case**: Comprehensive survey of all parallels

### 2.2 Cross-Paragraph Only Mode
- **UI Label**: "Cross-paragraph only"
- **Description**: Returns only results that matched on chunks crossing paragraph boundaries
- **Results**: Fewer results, but higher precision
- **Use case**: Finding clear literary dependencies
- **Tooltip**: "Show only matches where the matching text spans a paragraph break in your source"

### 2.3 Combined Mode
- **UI Label**: "Full + Cross-paragraph boost"
- **Description**: Full search with score boost for boundary-crossing matches
- **Use case**: Best balance between coverage and relevance
- **Tooltip**: "Search everything, but rank cross-paragraph matches higher"

---

## 3. Core Algorithm

### 3.1 Key Insight: Reuse Existing Chunks

The current chunking algorithm already creates overlapping chunks that may cross paragraph boundaries. Rather than creating separate "boundary chunks", we:

1. **Parse boundaries** from the source text based on user-selected delimiter
2. **Run regular chunk search** as usual
3. **Identify which chunks crossed boundaries** by checking word positions
4. **Apply boost** to results from boundary-crossing chunks
5. **Calculate boundary match quality** as average match strength

### 3.2 Boundary Detection

```python
def parse_boundaries(text: str, delimiter: str, min_distance: int = 3) -> list[int]:
    """
    Find word indices where boundaries occur.

    Args:
        text: Source text
        delimiter: Boundary marker (e.g., '\n\n', '.', ':')
        min_distance: Minimum words between boundaries (ignore closer ones)

    Returns:
        List of word indices where boundaries occur (boundary is AFTER this index)
    """
    # Split by delimiter
    parts = text.split(delimiter)

    boundaries = []
    word_count = 0
    last_boundary_pos = -min_distance  # Allow first boundary

    for i, part in enumerate(parts[:-1]):  # Skip last part (no boundary after it)
        words_in_part = len(part.split())
        word_count += words_in_part

        # Only add boundary if far enough from previous
        if word_count - last_boundary_pos >= min_distance:
            boundaries.append(word_count - 1)  # Boundary after last word of this part
            last_boundary_pos = word_count

    return boundaries
```

### 3.3 Identifying Boundary-Crossing Chunks

```python
def chunk_crosses_boundary(chunk_start: int, chunk_end: int,
                           boundaries: list[int]) -> bool:
    """
    Check if a chunk spans any boundary.

    A chunk crosses a boundary if the boundary index falls
    strictly between chunk_start and chunk_end.
    """
    for b in boundaries:
        if chunk_start <= b < chunk_end:
            return True
    return False
```

### 3.4 Boundary Match Quality Score

Instead of counting boundary matches, we calculate average match strength:

```python
def calculate_boundary_quality(boundary_chunk_scores: list[float]) -> float:
    """
    Calculate boundary match quality as average of match strengths.

    Args:
        boundary_chunk_scores: List of scores from chunks that crossed boundaries

    Returns:
        Average score (0 if no boundary matches)
    """
    if not boundary_chunk_scores:
        return 0.0
    return sum(boundary_chunk_scores) / len(boundary_chunk_scores)
```

### 3.5 Final Score Calculation

```python
def calculate_final_score(base_score: float,
                         boundary_quality: float,
                         has_boundary_matches: bool,
                         boundary_boost: float = 1.5) -> float:
    """
    Calculate final score with boundary boost.

    Formula: base_score * (1 + (boost - 1) * normalized_quality)

    Where normalized_quality = boundary_quality / base_score
    This ensures the boost is proportional to how good the boundary matches are
    relative to overall match quality.
    """
    if not has_boundary_matches or base_score == 0:
        return base_score

    # Normalize boundary quality relative to base score
    normalized_quality = min(boundary_quality / base_score, 1.0)

    multiplier = 1 + (boundary_boost - 1) * normalized_quality
    return base_score * multiplier
```

### 3.6 Score Examples (boost=1.5)

| Base Score | Boundary Quality | Normalized | Multiplier | Final Score |
|------------|------------------|------------|------------|-------------|
| 1000 | 0 (no matches) | 0 | ×1.00 | 1000 |
| 1000 | 500 (weak) | 0.5 | ×1.25 | 1250 |
| 1000 | 800 (good) | 0.8 | ×1.40 | 1400 |
| 1000 | 1000 (strong) | 1.0 | ×1.50 | 1500 |

---

## 4. Minimum Delimiter Distance

### 4.1 Problem
With period (`.`) as delimiter, text like `"Dr. Smith met Mr. Jones."` creates many false boundaries.

### 4.2 Solution
Ignore delimiters that occur within `min_distance` words of the previous delimiter.

```python
# Example: min_distance = 3

Text: "Hello. Hi. How are you today. Fine thanks."
       ^     ^    ^                ^
       0     1    2                6  (word positions)

Delimiters at word positions: 0, 1, 5, 7

With min_distance=3:
- Position 0: OK (first delimiter)
- Position 1: SKIP (only 1 word from previous)
- Position 5: OK (4 words from position 0)
- Position 7: SKIP (only 2 words from position 5)

Final boundaries: [0, 5]
```

### 4.3 Default Values

| Delimiter | Default min_distance | Rationale |
|-----------|---------------------|-----------|
| Paragraph (`\n\n`) | 1 | Paragraphs are intentional |
| Line (`\n`) | 1 | Lines are intentional |
| Period (`.`) | 3 | Avoid abbreviations |
| Colon (`:`) | 2 | Some colons are structural |
| Custom | 3 | Safe default |

---

## 5. Parameters

### 5.1 New Parameters

| Parameter | UI Label | Default | Range | Description |
|-----------|----------|---------|-------|-------------|
| `boundary_mode` | "Search Mode" | `'full'` | `'full'`, `'boundary'`, `'combined'` | Search mode selection |
| `boundary_delimiter` | "Paragraph separator" | `'\n\n'` | string | What marks a boundary |
| `boundary_boost` | "Cross-paragraph boost" | `1.5` | `1.0-3.0` | Score multiplier (combined mode) |
| `min_boundary_matches` | "Min. cross-paragraph matches" | `0` | `0-10` | Filter results (post-hoc) |
| `min_delimiter_distance` | (Advanced) | `3` | `1-10` | Min words between delimiters |

### 5.2 Existing Parameters (Unchanged)

| Parameter | Description | Relevance |
|-----------|-------------|-----------|
| `chunk_size` | Regular chunk size | Used in all modes |
| `mode` | exact/variants/fuzzy | Used in all modes |
| `deep_scan` | Exhaustive search | Used in all modes |
| `filter_text` | Text to exclude (Bible, etc.) | Used in all modes |

### 5.3 Parameter Behavior by Mode

| Mode | chunk_size | boundary_boost | min_boundary_matches |
|------|------------|----------------|---------------------|
| Full | Used | N/A | Post-hoc filter |
| Cross-paragraph only | Used | N/A | N/A (all results have matches) |
| Combined | Used | Applied | Post-hoc filter |

---

## 6. User Interface

### 6.1 Simple / Advanced Toggle

**Simple Mode** (default):
- Search mode selection (3 radio buttons)
- Delimiter selection (dropdown)
- Chunk size slider

**Advanced Mode** (expandable):
- All Simple options, plus:
- Cross-paragraph boost slider
- Min. cross-paragraph matches
- Min. delimiter distance

### 6.2 Web UI Layout

```
┌─────────────────────────────────────────────────────────────┐
│  Find Parallels                                             │
├─────────────────────────────────────────────────────────────┤
│  [textarea: Source text]                                    │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ ℹ️ 5 paragraph breaks detected, 12 chunks will be   │   │
│  │   checked for cross-paragraph matches               │   │  ← PRE-SEARCH STATS
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Search Mode: ○ Full  ○ Cross-paragraph only ⓘ  ○ Combined ⓘ│  ← TOOLTIPS
│                                                             │
│  Paragraph separator: [Blank line ▼]                        │
│  Chunk size: [====5====]                                    │
│  □ Deep Scan                                                │
│                                                             │
│  ▼ Advanced settings                                        │  ← COLLAPSED BY DEFAULT
│  ┌─────────────────────────────────────────────────────┐   │    AUTO-EXPANDS ON MODE CHANGE
│  │ Cross-paragraph boost: [====1.5====]                │   │
│  │ Min. matches to show:  [0 ▼]                        │   │
│  │ Min. words between separators: [3 ▼]                │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ⚠️ No paragraph breaks detected in text!                   │  ← WARNING (if applicable)
│                                                             │
│  [ 🔍 Find Parallels ]                                      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 6.3 Delimiter Menu

```
Blank line (paragraph)  ← default
Line break
Period (.)
Colon (:)
Custom...  → [input field]
```

### 6.4 Pre-Search Validation

Before search starts, display:
- Number of boundaries detected
- Number of chunks that will cross boundaries
- Warning if no boundaries found (for boundary/combined modes)

```python
def get_boundary_stats(text: str, delimiter: str, chunk_size: int) -> dict:
    boundaries = parse_boundaries(text, delimiter)
    total_words = len(text.split())

    # Estimate chunks that cross boundaries
    crossing_chunks = 0
    step = max(1, chunk_size // 2)
    for i in range(0, total_words - chunk_size + 1, step):
        if chunk_crosses_boundary(i, i + chunk_size, boundaries):
            crossing_chunks += 1

    return {
        'boundary_count': len(boundaries),
        'crossing_chunk_count': crossing_chunks,
        'total_chunks': (total_words - chunk_size) // step + 1
    }
```

### 6.5 Result Display

Results with boundary matches are highlighted:

**Web (light theme)**: Yellow background highlight
**Web (dark theme)**: Amber/gold background highlight
**Desktop**: Configurable (default: yellow background)

```
┌─────────────────────────────────────────────────────────────┐
│ T-S 12.345  [Score: 1250 → 1450]                           │  ← SHOWS BOOST
│ ─────────────────────────────────────────────────────────  │
│ ...text before ██ boundary ██ text after...                │  ← BOUNDARY HIGHLIGHTED
│                                                             │
│ 🔗 Cross-paragraph match (quality: 85%)                     │
└─────────────────────────────────────────────────────────────┘
```

For results WITHOUT boundary matches (in combined mode):
```
┌─────────────────────────────────────────────────────────────┐
│ T-S 67.890  [Score: 1100]                                  │
│ ─────────────────────────────────────────────────────────  │
│ ...matching text from manuscript...                         │
└─────────────────────────────────────────────────────────────┘
```

---

## 7. Implementation Changes

### 7.1 genizah_core.py

**New functions:**
```python
def parse_boundaries(text: str, delimiter: str, min_distance: int = 3) -> list[int]:
    """Parse boundary positions from text."""

def chunk_crosses_boundary(chunk_start: int, chunk_end: int, boundaries: list[int]) -> bool:
    """Check if chunk spans a boundary."""

def calculate_boundary_quality(scores: list[float]) -> float:
    """Calculate average boundary match quality."""

def get_boundary_stats(text: str, delimiter: str, chunk_size: int) -> dict:
    """Get pre-search statistics about boundaries."""
```

**Modified function signature:**
```python
def lab_composition_search(
    self,
    full_text: str,
    mode: str = 'exact',
    progress_callback=None,
    excluded_ids=None,
    chunk_size: int = None,
    filter_text: str = None,
    deep_scan: bool = False,
    scan_limit: int = None,
    # --- New parameters ---
    boundary_mode: str = 'full',           # 'full', 'boundary', 'combined'
    boundary_delimiter: str = '\n\n',
    boundary_boost: float = 1.5,
    min_boundary_matches: int = 0,
    min_delimiter_distance: int = 3
) -> dict:
```

**Extended result structure:**
```python
{
    'main': [
        {
            'uid': '...',
            'score': 1000,                    # Base score
            'final_score': 1400,              # After boost (if applicable)
            'boundary_quality': 0.85,         # Average quality (0-1)
            'has_boundary_matches': True,
            'boundary_match_count': 3,        # Number of crossing chunks matched
            # ... existing fields
        }
    ],
    'known': [...],
    'filtered': [...],
    'partial': bool,
    'boundary_stats': {                       # Pre-search stats
        'boundary_count': 5,
        'crossing_chunk_count': 12,
        'total_chunks': 45
    }
}
```

### 7.2 web/pages/parallels.py

- Add mode selection (radio buttons with tooltips)
- Add delimiter dropdown
- Add Simple/Advanced toggle
- Add pre-search statistics display
- Add warning for no boundaries
- Auto-expand advanced settings on mode change
- Update result cards with boundary highlighting
- Add score boost indicator

### 7.3 genizah_app.py (Desktop)

- Add ComboBox for mode selection
- Add boundary settings group (collapsible)
- Add pre-search stats label
- Add boundary highlighting in results tree
- Sync highlighting colors with theme

---

## 8. Execution Flow

### 8.1 Search Flow

```
1. User enters text and selects options
2. System parses boundaries from text
3. System displays pre-search stats
   - If boundary_mode != 'full' and boundary_count == 0:
     Show warning, optionally block search
4. User clicks "Find Parallels"
5. System generates regular chunks (existing logic)
6. For each chunk, mark if it crosses a boundary
7. Run search (existing logic)
8. For each result:
   a. Collect scores from boundary-crossing chunks
   b. Calculate boundary_quality
   c. If boundary_mode == 'combined': apply boost
   d. If boundary_mode == 'boundary': filter out non-crossing results
9. Apply min_boundary_matches filter (if set)
10. Sort by final_score
11. Return results with boundary metadata
```

### 8.2 Mode-Specific Behavior

**Full Mode:**
- Search all chunks
- Track boundary matches for display
- Filter by min_boundary_matches (post-hoc)
- No score modification

**Cross-paragraph Only Mode:**
- Search all chunks
- Return ONLY results with boundary matches
- No score modification

**Combined Mode:**
- Search all chunks
- Apply boost to results with boundary matches
- Filter by min_boundary_matches (post-hoc)
- Sort by final_score

---

## 9. Test Scenarios

### 9.1 Unit Tests

```python
def test_parse_boundaries_paragraph():
    text = "First paragraph.\n\nSecond paragraph.\n\nThird."
    boundaries = parse_boundaries(text, '\n\n')
    assert len(boundaries) == 2

def test_parse_boundaries_min_distance():
    text = "A. B. C. D. E. F."  # Periods every word
    boundaries = parse_boundaries(text, '.', min_distance=3)
    # Should skip some boundaries
    assert len(boundaries) < 6

def test_chunk_crosses_boundary():
    boundaries = [4, 10]  # Boundaries after words 4 and 10
    assert chunk_crosses_boundary(2, 6, boundaries) == True   # Crosses 4
    assert chunk_crosses_boundary(0, 3, boundaries) == False  # Before 4
    assert chunk_crosses_boundary(5, 9, boundaries) == False  # Between 4 and 10

def test_boundary_quality_calculation():
    scores = [800, 900, 850]
    quality = calculate_boundary_quality(scores)
    assert quality == 850.0

def test_no_boundaries_warning():
    text = "Continuous text without any paragraph breaks at all"
    stats = get_boundary_stats(text, '\n\n', chunk_size=5)
    assert stats['boundary_count'] == 0
```

### 9.2 Integration Tests

| Scenario | Input | Expected |
|----------|-------|----------|
| No boundaries, Full mode | Continuous text | Normal results, no boost |
| No boundaries, Boundary mode | Continuous text | Warning, empty results |
| Single boundary | "Para1\n\nPara2" | Chunks 2-4 cross boundary |
| Period delimiter | "Sentence one. Sentence two." | 1 boundary detected |
| Too-close periods | "Dr. Smith went to Mt. Everest." | Skipped (min_distance) |

### 9.3 UI Tests

- [ ] Mode selection updates stats display
- [ ] Delimiter change updates stats display
- [ ] Advanced panel auto-expands on mode change
- [ ] Warning shows when no boundaries detected
- [ ] Boundary highlighting visible in results
- [ ] Tooltips display on hover

---

## 10. Performance Considerations

### 10.1 Overhead Analysis

| Operation | Overhead | Notes |
|-----------|----------|-------|
| Parse boundaries | O(n) | Single pass through text |
| Check chunk crossing | O(b) per chunk | b = number of boundaries |
| Quality calculation | O(1) per result | Simple average |
| Score boost | O(1) per result | Single multiplication |

**Total overhead**: Negligible compared to existing search time.

### 10.2 No Additional Index Queries

Unlike the original design, this approach:
- Does NOT create additional chunks
- Does NOT run separate searches
- Uses existing search infrastructure

This means **no performance penalty** for Combined mode vs Full mode.

---

## 11. Implementation Phases

### Phase 1: Core Infrastructure
1. Add `parse_boundaries()` function
2. Add boundary crossing detection to chunk processing
3. Add boundary metadata to results
4. Add `get_boundary_stats()` function
5. Write unit tests
6. **Deliverable**: API supports boundary detection

### Phase 2: Web UI - Basic
1. Add mode selection radio buttons
2. Add delimiter dropdown
3. Add pre-search stats display
4. Add warning for no boundaries
5. **Deliverable**: Basic boundary search working

### Phase 3: Web UI - Enhanced
1. Add Simple/Advanced toggle
2. Add tooltips
3. Add boundary highlighting in results
4. Add score boost display
5. Auto-expand advanced on mode change
6. **Deliverable**: Full web UI complete

### Phase 4: Desktop App
1. Port all UI changes to PyQt6
2. Ensure feature parity
3. Test highlighting on different themes
4. **Deliverable**: Desktop version complete

### Phase 5: Tuning
1. Real-world testing with scholars
2. Tune default values (boost, min_distance)
3. Documentation update
4. **Deliverable**: Production-ready feature

---

## 12. Open Questions (Resolved)

| Question | Resolution |
|----------|------------|
| What counts as a "boundary match"? | Average quality of crossing-chunk scores |
| Overlap with regular chunks? | Reuse regular chunks, just identify which cross |
| Support period delimiter? | Yes, with min_distance protection |
| Too many options? | Simple/Advanced toggle |
| How to name modes? | Cross-paragraph only, Full + Cross-paragraph boost |
| User feedback? | Pre-search stats + warnings |
| Manual boundary editing? | Not needed - user defines delimiter |
| Panel auto-expand? | Yes, on mode change |

---

## 13. Future Enhancements

- [ ] **Library search**: Auto-detect verse/halakha boundaries
- [ ] **Auto-delimiter detection**: Guess delimiter from text structure
- [ ] **Visual boundary marking**: Show boundary positions in source textarea
- [ ] **Export enhancement**: Include boundary match info in Excel/Word exports
- [ ] **Statistics view**: Distribution of boundary vs non-boundary matches

---

## 14. Glossary

| Term | Definition |
|------|------------|
| **Boundary** | A user-defined break point in the source text (paragraph, sentence, etc.) |
| **Crossing chunk** | A regular search chunk that spans a boundary |
| **Boundary quality** | Average match score of crossing chunks (0-1 normalized) |
| **Boost** | Score multiplier applied based on boundary quality |
| **min_distance** | Minimum words between delimiters to avoid false boundaries |

---

*This document is ready for implementation.*
