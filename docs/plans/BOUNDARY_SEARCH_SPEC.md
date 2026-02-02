# Technical Specification: Boundary-Crossing Parallel Search

> Date: February 2026
> Status: Draft for Review
> Author: Claude Code

## Executive Summary

A new parallel search feature that focuses on word sequences crossing paragraph boundaries in the source text. The rationale: such sequences are strong candidates for true literary parallels, as opposed to biblical quotations or formulaic phrases that typically appear within a single paragraph unit.

---

## 1. Goals and Rationale

### 1.1 Primary Goal
Enable users to find robust literary parallels by searching for word sequences that span paragraph boundaries.

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
- **Description**: Searches all chunks, same as current behavior
- **Addition**: Option to filter results by minimum boundary-crossing matches
- **Use case**: Comprehensive survey of all parallels

### 2.2 Boundary-Only Mode
- **Description**: Searches only chunks that cross paragraph boundaries
- **Results**: Fewer results, but higher precision
- **Use case**: Finding clear literary dependencies

### 2.3 Combined Mode
- **Description**: Full search with score boost for boundary-crossing matches
- **Use case**: Best balance between coverage and relevance

---

## 3. Chunking Logic

### 3.1 Boundary Detection

```python
DELIMITERS = {
    'paragraph': '\n\n',      # Double newline (paragraph break)
    'newline': '\n',          # Single newline
    'period': '.',            # Period/full stop
    'colon': ':',             # Colon
    'custom': '<user_input>'  # User-defined
}
```

**Note on delimiter handling:**
- Multiple consecutive delimiters are collapsed (e.g., `\n\n\n\n` = one boundary)
- Empty paragraphs are ignored
- Whitespace is trimmed from paragraph edges

### 3.2 Sliding Window Around Boundaries

**Core principle**: Every chunk must contain at least one word from each side of the boundary.

```
Text: "...w5 w4 w3 w2 w1 |BOUNDARY| w1 w2 w3 w4 w5..."
                         ↑
                    paragraph break

With boundary_window_size=4, the following chunks are created:

  Chunk 1: [w3 w2 w1 | w1]     ← 3 from tail + 1 from head
  Chunk 2: [w2 w1 | w1 w2]     ← 2 + 2
  Chunk 3: [w1 | w1 w2 w3]     ← 1 + 3

Note: No 4+0 or 0+4 chunks - must have at least 1 word from each side!
```

### 3.3 Why Window Size of 4?

| Window Size | Chunks per Boundary | Trade-off |
|-------------|---------------------|-----------|
| 2 | 1 (1+1 only) | Too restrictive, misses near-boundary matches |
| 3 | 2 (2+1, 1+2) | Limited coverage |
| **4** | **3 (3+1, 2+2, 1+3)** | **Good balance: enough coverage, not too many chunks** |
| 5 | 4 | More coverage, more computation |
| 6 | 5 | Diminishing returns, overlaps with regular chunks |

**Recommendation**: Default to 4, allow user adjustment from 2-12.

### 3.4 Implementation

```python
def create_boundary_chunks(text: str, delimiter: str, window_size: int = 4) -> list:
    """
    Creates boundary-crossing chunks from text using a sliding window.

    Args:
        text: Source text to search
        delimiter: Character/sequence separating paragraphs
        window_size: Total chunk size (NOT per-side). Range: 2-12, default: 4

    Returns:
        List of chunk dictionaries with metadata
    """
    # Normalize delimiters (collapse multiple into one)
    import re
    normalized = re.sub(f'({re.escape(delimiter)})+', delimiter, text)

    paragraphs = normalized.split(delimiter)
    paragraphs = [p.strip() for p in paragraphs if p.strip()]

    if len(paragraphs) < 2:
        return []  # No boundaries to cross

    chunks = []

    for boundary_idx in range(len(paragraphs) - 1):
        tail_words = paragraphs[boundary_idx].split()
        head_words = paragraphs[boundary_idx + 1].split()

        # Generate all valid tail+head combinations
        # Constraint: tail_count >= 1, head_count >= 1, total = window_size
        for tail_count in range(1, window_size):
            head_count = window_size - tail_count

            # Skip if not enough words available
            if tail_count > len(tail_words) or head_count > len(head_words):
                continue

            chunk_tokens = tail_words[-tail_count:] + head_words[:head_count]

            chunks.append({
                'tokens': chunk_tokens,
                'text': ' '.join(chunk_tokens),
                'boundary_idx': boundary_idx,
                'tail_count': tail_count,
                'head_count': head_count,
                'para_indices': (boundary_idx, boundary_idx + 1),
                'is_boundary_chunk': True  # Flag for identification
            })

    return chunks
```

---

## 4. Parameters

### 4.1 New Parameters

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `boundary_mode` | `'full'` | `'full'`, `'boundary'`, `'combined'` | Search mode selection |
| `boundary_window_size` | `4` | `2-12` | Total words in boundary chunk |
| `boundary_delimiter` | `'\n\n'` | string | Paragraph separator |
| `boundary_boost` | `1.5` | `1.0-5.0` | Score multiplier for boundary matches (combined mode) |
| `min_boundary_matches` | `0` | `0-10` | Filter: minimum boundary matches to include result |

### 4.2 Existing Parameters (Unchanged)

| Parameter | Description | Relevance |
|-----------|-------------|-----------|
| `chunk_size` | Regular chunk size | Used in Full and Combined modes |
| `mode` | exact/variants/fuzzy | Used in all modes |
| `deep_scan` | Exhaustive search | Used in all modes |
| `filter_text` | Text to exclude (Bible, etc.) | Used in all modes |

### 4.3 Parameter Interactions

| Mode | chunk_size | boundary_window_size | boundary_boost |
|------|------------|---------------------|----------------|
| Full | Used | Used for filtering only | N/A |
| Boundary | N/A | Used | N/A |
| Combined | Used | Used | Used |

---

## 5. Scoring Algorithm (Combined Mode)

### 5.1 Design Considerations

The scoring formula must:
1. Reward boundary matches without overwhelming base scores
2. Show diminishing returns for many boundary matches (avoid runaway scores)
3. Be tunable via the `boundary_boost` parameter

### 5.2 Formula

```python
def calculate_combined_score(base_score: float,
                            boundary_match_count: int,
                            boundary_boost: float = 1.5) -> float:
    """
    Calculate final score in combined mode.

    Formula: base_score * (1 + (boost - 1) * log2(matches + 1))

    Properties:
    - 0 matches: base_score (no change)
    - 1 match: base_score * boost
    - Logarithmic growth prevents score explosion
    """
    if boundary_match_count == 0:
        return base_score

    import math
    multiplier = 1 + (boundary_boost - 1) * math.log2(boundary_match_count + 1)
    return base_score * multiplier
```

### 5.3 Score Examples (boost=1.5)

| Boundary Matches | Multiplier | Base 1000 → |
|------------------|------------|-------------|
| 0 | ×1.00 | 1000 |
| 1 | ×1.50 | 1500 |
| 2 | ×1.79 | 1792 |
| 3 | ×2.00 | 2000 |
| 5 | ×2.29 | 2292 |
| 10 | ×2.73 | 2730 |

**Note**: A manuscript with 10 boundary matches only scores ~2.7× base, not 10×. This prevents boundary-rich matches from completely dominating.

---

## 6. Combined Mode: Execution Strategy

### 6.1 Option A: Sequential (Simpler)
```
1. Run regular chunk search → regular_results
2. Run boundary chunk search → boundary_results
3. Merge results, applying boost to boundary matches
```
**Pros**: Simple to implement, easy to debug
**Cons**: ~2× search time

### 6.2 Option B: Unified (More Efficient)
```
1. Generate all chunks (regular + boundary) with flags
2. Run single search
3. Post-process: identify boundary matches, apply boost
```
**Pros**: Single search pass
**Cons**: More complex chunk management

### 6.3 Recommendation
Start with **Option A** for clarity. Optimize to Option B later if performance is an issue.

---

## 7. Result Deduplication

### 7.1 Problem
A manuscript may match both regular chunks and boundary chunks, potentially appearing twice in results.

### 7.2 Solution
```python
def merge_results(regular: list, boundary: list, boost: float) -> list:
    """
    Merge regular and boundary results, deduplicating by manuscript ID.

    For duplicates:
    - Keep highest base score
    - Add boundary_match_count from boundary results
    - Apply boost
    """
    results_by_uid = {}

    # Process regular results
    for r in regular:
        uid = r['uid']
        results_by_uid[uid] = {
            **r,
            'boundary_match_count': 0,
            'has_boundary_matches': False
        }

    # Process boundary results
    for r in boundary:
        uid = r['uid']
        if uid in results_by_uid:
            # Manuscript already found - add boundary info
            existing = results_by_uid[uid]
            existing['boundary_match_count'] += 1
            existing['has_boundary_matches'] = True
            # Keep higher base score
            existing['score'] = max(existing['score'], r['score'])
        else:
            # New manuscript from boundary search
            results_by_uid[uid] = {
                **r,
                'boundary_match_count': 1,
                'has_boundary_matches': True
            }

    # Apply boost and return
    results = list(results_by_uid.values())
    for r in results:
        r['final_score'] = calculate_combined_score(
            r['score'], r['boundary_match_count'], boost
        )

    return sorted(results, key=lambda x: x['final_score'], reverse=True)
```

---

## 8. User Interface

### 8.1 Web UI (NiceGUI)

```
┌─────────────────────────────────────────────────────────┐
│  Find Parallels                                         │
├─────────────────────────────────────────────────────────┤
│  [textarea: Source text]                                │
│                                                         │
│  Options:                                               │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Search Mode: ○ Full  ○ Boundary-Only  ○ Combined│   │
│  │ Chunk Size:  [====5====]                        │   │
│  │ □ Deep Scan                                     │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
│  ▼ Boundary Search Settings [collapsed by default]     │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Window Size:       [====4====]                  │   │
│  │ Delimiter:         [Paragraph break ▼]          │   │
│  │ Min. Matches:      [0 ▼] (result filter)        │   │
│  │ Score Boost:       [====1.5====] (combined)     │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
│  [ 🔍 Find Parallels ]                                  │
└─────────────────────────────────────────────────────────┘
```

### 8.2 Delimiter Menu

```
Paragraph break (blank line)  ← default
Line break
Period (.)
Colon (:)
Custom...  → [input field]
```

### 8.3 Result Display

Results with boundary matches show an indicator:

```
┌─────────────────────────────────────────────────────────┐
│ 🔗 T-S 12.345  [Score: 1250]                           │
│ ─────────────────────────────────────────────────────  │
│ ...highlighted *matching* text from manuscript...       │
│                                                         │
│ 🔗 3 boundary-crossing matches                          │
└─────────────────────────────────────────────────────────┘
```

---

## 9. Code Changes

### 9.1 genizah_core.py

**New functions:**
```python
def create_boundary_chunks(self, text: str, delimiter: str,
                          window_size: int) -> list:
    """Creates sliding window chunks around paragraph boundaries."""

def _merge_chunk_results(self, regular_results: dict,
                        boundary_results: dict, boost: float) -> dict:
    """Merges results from regular and boundary searches with boost."""
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
    boundary_mode: str = 'full',
    boundary_window_size: int = 4,
    boundary_delimiter: str = '\n\n',
    boundary_boost: float = 1.5,
    min_boundary_matches: int = 0
) -> dict:
```

**Extended result structure:**
```python
{
    'main': [
        {
            'uid': '...',
            'score': 1000,
            'final_score': 1500,           # NEW: after boost
            'boundary_match_count': 2,      # NEW
            'has_boundary_matches': True,   # NEW
            # ... existing fields
        }
    ],
    'known': [...],
    'filtered': [...],
    'partial': bool,
    'boundary_stats': {                     # NEW
        'total_boundaries': int,
        'total_boundary_chunks': int,
        'chunks_with_matches': int
    }
}
```

### 9.2 web/pages/parallels.py

- Add mode selection (radio buttons)
- Add collapsible boundary settings panel
- Update call to `lab_composition_search` with new parameters
- Add 🔗 icon to results with boundary matches
- Update results persistence to include boundary data

### 9.3 genizah_app.py (Desktop)

- Add ComboBox for mode selection
- Add boundary settings widget group
- Update search function call
- Add boundary indicator to results tree

---

## 10. Performance Considerations

### 10.1 Chunk Count Impact

| Scenario | Regular Chunks | Boundary Chunks | Total |
|----------|----------------|-----------------|-------|
| 100 words, no breaks | 20 | 0 | 20 |
| 100 words, 5 paragraphs | 20 | 12 (4 boundaries × 3) | 32 |
| 500 words, 20 paragraphs | 100 | 57 (19 × 3) | 157 |

**Observation**: Boundary chunks add ~30-60% overhead in typical texts.

### 10.2 Mitigation Strategies

1. **Boundary-only mode**: Searches only boundary chunks (much faster)
2. **Lazy evaluation**: Only compute boundary chunks if mode requires them
3. **Parallel processing**: Boundary and regular searches can run concurrently

---

## 11. Test Scenarios

### 11.1 Unit Tests

```python
def test_boundary_chunk_creation_basic():
    """Basic boundary chunk generation."""
    text = "word1 word2 word3\n\nword4 word5 word6"
    chunks = create_boundary_chunks(text, '\n\n', window_size=4)

    assert len(chunks) == 3  # 3+1, 2+2, 1+3
    assert all(c['is_boundary_chunk'] for c in chunks)
    assert chunks[0]['tail_count'] + chunks[0]['head_count'] == 4

def test_no_single_side_chunks():
    """Verify no chunks with 0 words on either side."""
    text = "a b c\n\nd e f"
    chunks = create_boundary_chunks(text, '\n\n', window_size=4)

    for chunk in chunks:
        assert chunk['tail_count'] >= 1
        assert chunk['head_count'] >= 1

def test_short_paragraphs():
    """Handle paragraphs shorter than window size."""
    text = "a b\n\nc d e f g"  # First para has only 2 words
    chunks = create_boundary_chunks(text, '\n\n', window_size=4)

    # Should create: 2+2, 1+3 (not 3+1 - not enough tail words)
    assert len(chunks) == 2

def test_multiple_boundaries():
    """Multiple paragraph boundaries."""
    text = "a b c\n\nd e f\n\ng h i"
    chunks = create_boundary_chunks(text, '\n\n', window_size=4)

    # 2 boundaries × 3 chunks each = 6
    assert len(chunks) == 6

def test_collapsed_delimiters():
    """Multiple consecutive delimiters should count as one boundary."""
    text = "a b c\n\n\n\nd e f"  # 4 newlines
    chunks = create_boundary_chunks(text, '\n\n', window_size=4)

    # Should still be 1 boundary
    assert len(chunks) == 3
```

### 11.2 Integration Tests

| Scenario | Input | Expected |
|----------|-------|----------|
| No boundaries | "continuous text" | Boundary mode: empty results |
| Single boundary | "para1\n\npara2" | 3 boundary chunks (with size=4) |
| Custom delimiter | "a:b:c" with delimiter=":" | 6 boundary chunks |
| Mixed search | Combined mode | Both regular and boundary matches |

### 11.3 UI Tests

- [ ] Mode selection works correctly
- [ ] Settings panel expands/collapses
- [ ] Boundary settings only enabled when relevant
- [ ] 🔗 icon displays correctly
- [ ] Min boundary filter works
- [ ] Results persist across page reloads

---

## 12. Implementation Phases

### Phase 1: Core Infrastructure
1. Add `create_boundary_chunks()` to genizah_core.py
2. Add new parameters to `lab_composition_search()`
3. Implement boundary-only mode
4. Write unit tests
5. **Deliverable**: Working boundary-only search via API

### Phase 2: Web UI
1. Add mode selection UI
2. Add boundary settings panel
3. Update search execution
4. Add result indicators
5. **Deliverable**: Fully functional web interface

### Phase 3: Combined Mode
1. Implement result merging
2. Implement boost scoring
3. Add deduplication logic
4. Test with real texts
5. **Deliverable**: Combined mode working

### Phase 4: Desktop App
1. Port UI changes to PyQt6
2. Ensure feature parity
3. Test thoroughly
4. **Deliverable**: Desktop version complete

### Phase 5: Tuning & Polish
1. Real-world testing with scholars
2. Tune default values
3. Optimize performance if needed
4. Documentation update
5. **Deliverable**: Production-ready feature

---

## 13. Open Questions and Future Work

### 13.1 Questions Requiring Testing
- [ ] What is the optimal default boost value? (Start with 1.5)
- [ ] What is the optimal default window size? (Start with 4)
- [ ] Does boundary search improve precision in practice?

### 13.2 Future Enhancements
- [ ] **Library search**: Auto-detect verse/halakha boundaries in source texts
- [ ] **Auto-delimiter detection**: Guess delimiter from text structure
- [ ] **Visual boundary marking**: Highlight boundary points in source display
- [ ] **Export enhancement**: Include boundary match info in exports
- [ ] **Statistics view**: Show boundary match distribution

### 13.3 Edge Cases to Monitor
- Very short paragraphs (1-2 words)
- Texts with inconsistent paragraph formatting
- Performance with many boundaries (>50)
- Interaction with filter text feature

---

## 14. Dependencies and Constraints

### 14.1 Dependencies
- No new external dependencies required
- Uses existing Tantivy index and fingerprinting infrastructure

### 14.2 Backwards Compatibility
- All new parameters have defaults matching current behavior
- Existing API calls continue to work unchanged
- No database schema changes required

### 14.3 Known Limitations
- If source text has no boundaries, boundary-only mode returns empty
- Combined mode approximately doubles search time
- Boundary detection is text-based only (no semantic understanding)

---

## 15. Glossary

| Term | Definition |
|------|------------|
| **Boundary** | The division point between two paragraphs in the source text |
| **Boundary chunk** | A word sequence that spans a boundary (has words from both sides) |
| **Window size** | Total number of words in a boundary chunk |
| **Tail** | Words from the end of the paragraph before the boundary |
| **Head** | Words from the beginning of the paragraph after the boundary |
| **Boost** | Score multiplier applied to manuscripts with boundary matches |

---

## Critical Review: Potential Issues for External Readers

### Issue 1: Ambiguous "Window Size" Definition
**Problem**: The spec says "window size" but it's unclear if this means total words or words per side.
**Resolution**: Clarified in Section 3.3 that it means TOTAL words, not per-side.

### Issue 2: Combined Mode Execution Not Specified
**Problem**: Original spec didn't explain whether combined mode runs one search or two.
**Resolution**: Added Section 6 explaining execution strategies.

### Issue 3: Deduplication Logic Missing
**Problem**: If a manuscript matches both regular and boundary chunks, how is it handled?
**Resolution**: Added Section 7 with explicit deduplication algorithm.

### Issue 4: Performance Impact Unquantified
**Problem**: No indication of how many extra chunks are created.
**Resolution**: Added Section 10 with concrete examples.

### Issue 5: Edge Cases Not Addressed
**Problem**: What happens with very short paragraphs? Consecutive delimiters?
**Resolution**: Added handling notes and test cases.

### Issue 6: Scoring Formula Inconsistency
**Problem**: Section 2.3 showed a simple formula, Section 7 showed logarithmic.
**Resolution**: Removed the simple formula, kept only the logarithmic one with full explanation.

---

*This document is a draft for discussion and approval before implementation.*
