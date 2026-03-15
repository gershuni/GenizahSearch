# Join Finder — Research Report: Algorithmic Identification of Cairo Genizah Fragment Joins

**Date**: March 2026
**Status**: Active research — POC/experimental

---

## Table of Contents

1. [Background and Problem Definition](#1-background-and-problem-definition)
2. [Taxonomy of Joins](#2-taxonomy-of-joins)
3. [Detection Challenges by Join Type](#3-detection-challenges-by-join-type)
4. [Technical Infrastructure](#4-technical-infrastructure)
5. [Summary of Approaches](#5-summary-of-approaches)
6. [Approach 1: Direct MSBERT (v1/PoC)](#6-approach-1-direct-msbert)
7. [Approach 2: Cross-Line MSBERT (v2)](#7-approach-2-cross-line-msbert)
8. [Approach 3: Generic Word Filtering (v3)](#8-approach-3-generic-word-filtering)
9. [Approach 4: Phrase-Based Parallel Lookup (v4)](#9-approach-4-phrase-based-parallel-lookup)
10. [Approach 5: TF-IDF Content Similarity (v5)](#10-approach-5-tf-idf-content-similarity)
11. [Approach 6: Long Phrase Text Identification (v6)](#11-approach-6-long-phrase-text-identification)
12. [Approach 7: Two-Hop via Parallels (v7) — The Breakthrough](#12-approach-7-two-hop-via-parallels)
13. [Approach 8: v7 + FIST Visual Candidates (v8)](#13-approach-8-v7--fist-visual-candidates)
14. [Sequential Join Finder](#14-sequential-join-finder)
15. [Batch Evaluation Results](#15-batch-evaluation-results)
16. [Automatic Tear Type Detection](#16-automatic-tear-type-detection)
17. [Candidate Pre-screening](#17-candidate-pre-screening)
18. [Line Length Estimation for Sequential and Corner Joins](#18-line-length-estimation)
19. [Known Limitations](#19-known-limitations)
20. [Future Directions](#20-future-directions)
21. [Script Index](#21-script-index)

---

## 1. Background and Problem Definition

The Cairo Genizah is a trove of ~400,000 manuscript fragments discovered in the Ben Ezra Synagogue in Fustat (Old Cairo). Many of these fragments are pieces of what were once complete leaves — torn apart over centuries and now scattered across library collections worldwide (Cambridge, JTS, Oxford, Manchester, St. Petersburg, etc.).

**The task**: Given a fragment A, find fragment B — the other half of the same physical leaf, or the direct textual continuation.

**Why this is hard**:
- ~217,000 records in the search index, with text in Hebrew, Judeo-Arabic, and Aramaic.
- Two halves of a vertical tear share almost **zero complete words** — every line is split in the middle.
- Classical IR approaches (word overlap, BM25) fail because the signal is _cross-line_: the end of line X on the LEFT half flows into the start of line X+1 on the RIGHT half.
- Many fragments are from the same literary works (liturgical poetry, Talmud, Bible), so content similarity alone produces thousands of false positives.
- Physical metadata (dimensions, ink color, ruling patterns) is not digitized at scale.

**Primary test cases**:
- **Or.1081** (Talmud): `990001403820205171` ↔ `990001403810205171` — vertical tear, many textual parallels available
- **PGPID 3433** (legal document): `990053958100205171` ↔ `990051342590205171` — unique document with few parallels

---

## 2. Taxonomy of Joins

Genizah "joins" (Hebrew: צירופים) fall into several distinct physical categories, each requiring different algorithmic strategies.

### 2.1 Vertical Tear Join (צירוף קרע אנכי)

A leaf was torn lengthwise into left and right halves. The **LEFT** half contains line endings (marked `]` in transcriptions — the text that survives from the torn-away right portion). The **RIGHT** half contains line beginnings (marked `[`).

```
   RIGHT half (beginnings)          LEFT half (endings)
   ┌──────────────────[             ]──────────────────┐
   │  start of line 1                end of line 1     │
   │  start of line 2    ←TEAR→     end of line 2     │
   │  start of line 3                end of line 3     │
   └──────────────────[             ]──────────────────┘
```

**Key signal**: The end of line X on the LEFT half flows directly into the start of line X+1 on the RIGHT half (cross-line continuity). This is the foundation of our algorithmic approach.

**Detection markers**: A high proportion of lines starting with `]` (left half) or ending with `[` (right half). Our algorithms require ≥15% of lines to be torn for classification.

**Variants**:
- **Clean vertical tear**: The tear follows a relatively straight vertical line. Line pairings are 1:1.
- **Irregular vertical tear**: The tear is jagged; some lines may be mostly intact on one half and almost entirely missing on the other.

### 2.2 Sequential Join (צירוף רציף — סוף→ראש)

Fragment A's text continues directly into fragment B. The last lines of A flow into the first lines of B. These are typically consecutive leaves of the same codex or scroll.

```
   Fragment A                    Fragment B
   ┌────────────────┐           ┌────────────────┐
   │  ...           │           │  ...continues  │
   │  last line     │  ────→    │  first line    │
   └────────────────┘           └────────────────┘
```

**Key signal**: Multi-word phrases from the end of A appear at the beginning of B, in order.

**Challenge**: The continuation text also appears in every other copy of the same work in the corpus. For liturgical poetry with dozens of surviving copies, the algorithm correctly identifies the _text_ but cannot isolate the specific join partner without additional physical evidence.

### 2.3 Corner Tear Join (צירוף קרע פינתי)

A corner of the leaf was torn off diagonally. The surviving fragment has both complete lines (away from the tear) and progressively shorter lines near the torn corner. The detached corner piece has very short lines that gradually lengthen.

```
   Main fragment                Corner piece
   ┌────────────────────┐      ┌──────┐
   │  full line          │      │ ╲    │
   │  full line          │      │  ╲   │
   │  partial lin[       │      │]ine  │
   │  short [            │      │]rest │
   └────────────────────┘      └──────┘
```

**Key signals**:
- **Decreasing line length** on the main fragment (lines get shorter approaching the corner).
- **Increasing line length** on the corner piece (lines get longer moving away from the corner).
- The tear angle means that the amount of missing text per line changes progressively.

**Detection strategy**: Analyze the gradient of line lengths. If lines consistently shorten toward one edge (top-right, bottom-right, top-left, or bottom-left), a corner tear is likely. The missing text length per line can be estimated from the gradient, enabling targeted search.

### 2.4 Horizontal Tear Join (צירוף קרע אופקי)

A leaf was torn horizontally — the top portion separated from the bottom. Unlike vertical tears, the text on each line is complete; the halves simply contain different lines of the same page.

**Key signal**: Content continuity — the bottom line of the upper half continues logically into the top line of the lower half (like a sequential join but within a single page).

**Detection markers**: Both fragments have complete lines (no `[` or `]` markers) but share the same script style, ink, and dimensions. Harder to detect algorithmically than vertical tears.

### 2.5 Codex Join (צירוף קודקס)

Leaves from the same codex (bound book) that are not necessarily textually adjacent. They share codicological features (same scribe, same ink, same ruling, same dimensions) but may be from different parts of the same work.

**Detection**: Primarily relies on paleographic and codicological analysis rather than textual continuity. Outside the scope of the current algorithmic approach, but FIST's SVM visual matching targets this category.

### 2.6 Material Join (צירוף חומרי)

Fragments from the same original sheet of parchment or paper, identified by matching physical properties (fiber patterns, watermarks, stains) rather than textual content. Requires image analysis.

---

## 3. Detection Challenges by Join Type

| Join Type | Textual Signal | Algorithmically Detectable? | Key Challenge |
|-----------|---------------|:---------------------------:|---------------|
| Vertical tear | Cross-line continuity via parallels | **Yes** (v7/v8) | Requires parallel manuscripts |
| Sequential | Multi-word phrase continuity | **Partially** | Cannot distinguish from other copies |
| Corner tear | Progressive line-length gradient | **Possible** (not yet implemented) | Estimating missing text length |
| Horizontal tear | Content continuity across halves | **Possible** | No torn-line markers to detect |
| Codex join | Shared codicological features | **No** (visual only) | Needs image analysis |
| Material join | Physical properties | **No** (visual only) | Needs image analysis |

### Automatic vs. Manual Tear Type Classification

**Current approach**: The algorithm auto-detects vertical tears by counting `]` and `[` line markers. If ≥15% of lines are torn, the fragment is classified as a tear candidate.

**Ideal workflow**: Let the researcher specify the tear type, or offer automatic detection with manual override:
1. **Auto-detect**: Analyze line markers (`]` / `[`), line-length gradients, and line completeness.
2. **Suggest**: "This fragment appears to be a LEFT half of a vertical tear (48 of 99 lines start with `]`)."
3. **Override**: Researcher can specify "this is a corner tear" or "search for sequential continuation" regardless of auto-detection.

**Not all fragments are join candidates**: Fragments with complete lines on all sides (no torn markers, no progressive shortening) are likely standalone leaves or complete bifolios. The algorithm should flag these: "This fragment does not appear to have torn edges — a join search may not be productive."

---

## 4. Technical Infrastructure

### 4.1 Tantivy Index

Full-text index built on Tantivy (Rust-based search engine) with ~217K documents.

| Field | Description |
|-------|-------------|
| `content` | Full text of the fragment (all lines) |
| `line_starts` | First words of each line (relevant for `[` fragments) |
| `line_ends` | Last words of each line |
| `unique_id` | Unique identifier: `sys:ALMA_ID` |
| `shelfmark` | Shelf mark (e.g., "T-S 12.123", "Or.1081 1.56") |

### 4.2 MSBERT

Multilingual Scribes BERT — a BERT model fine-tuned on Hebrew manuscript text. Runs as a local server on `localhost:5000`. Used for Masked Language Model predictions (predicting missing words). **Ultimately abandoned** for join finding due to overly generic predictions.

### 4.3 FIST.db / fjms_enrichment.db

Databases from the Friedberg Jewish Manuscript Society:

| Table | Database | Rows | Description | Role |
|-------|----------|------|-------------|------|
| `joins` | fjms_enrichment.db | 48,655 | Joins identified by scholars | **Ground truth only** — not used in scoring |
| `Image_BestMarkForJoin` | FIST.db | 35.9M | Visual similarity pairs from SVM | **Algorithmic signal** — used for re-ranking |
| `dbo_InventoryAlma` | FIST.db | — | AlmaId ↔ InventoryId mapping | Mapping chain |
| `dbo_ImgDigitalImage` | FIST.db | — | InventoryId ↔ FGPImageNumberId | Mapping chain |
| `Image_ImageDocument` | FIST.db | — | FGP ↔ DocumentId | Mapping chain |

**Critical distinction**: Scholarly joins from the `joins` table are confirmed by researchers and serve as evaluation ground truth. They are **never** used to boost algorithmic scores. SVM visual candidates from `Image_BestMarkForJoin` are algorithmically computed (not scholar-confirmed) and are used as an additional scoring signal.

The mapping chain from AlmaId (= sys_id in the Tantivy index) to FIST's internal DocumentId:
```
AlmaId → dbo_InventoryAlma.InventoryId
       → dbo_ImgDigitalImage.FGPImageNumberId
       → Image_ImageDocument.DocumentId
       → Image_BestMarkForJoin (35.9M visual pairs)
```

The `FistMapper` class in v8 implements this chain with caching.

**SVM Statistics**:
- 154,953 unique source documents
- Average 232 candidates per document
- SVMMark range: -0.219 to 20.172 (mean: 1.927)

---

## 5. Summary of Approaches

| Version | Approach | Or.1081 Rank | PGPID 3433 | Notes |
|---------|----------|:------------:|:----------:|-------|
| PoC (v1) | Direct MSBERT | untested | — | Initial PoC |
| v2 | Cross-Line MSBERT | ≈#181 | — | Generic predictions |
| v3 | v2 + generic word filter | ≈#181 | — | Minor improvement |
| v4 | 3-word phrase parallels | NOT FOUND | — | 33 parallels, target missed |
| v5 | TF-IDF content similarity | #834 | — | Only 7 shared words |
| v6 | Long phrases (4-word) | NOT FOUND | — | 0 shared phrases between halves |
| **v7** | **Two-Hop via parallels** | **#1** | **#1** | **Breakthrough** |
| **v8** | **v7 + FIST SVM visual** | **#1** | #1 (no FIST data) | Multi-source fusion |
| Sequential | End→start continuation | — | — | Finds text, not specific partner |

---

## 6. Approach 1: Direct MSBERT

**Script**: `scripts/join_finder_poc.py`

**Idea**: For each torn line, use MSBERT to predict the missing word(s) at the masked position, then search for those predictions in the Tantivy index.

**Algorithm**:
1. Take a torn line: `] visible text [MASK]`
2. MSBERT predicts top-K words for the mask
3. Search each prediction in `line_starts` of the index
4. Score candidates by number of matching predictions

**Result**: MSBERT consistently predicts generic Hebrew function words (של, את, על, הוא, כל...) that appear in virtually every manuscript. Zero discriminative power.

**Lesson**: Single-word masked language modeling is insufficient for this domain. The model's training on manuscript text makes it predict _common_ words, not _specific_ ones.

---

## 7. Approach 2: Cross-Line MSBERT

**Script**: `scripts/join_finder_v2.py`

**Insight**: The end of line X on the LEFT flows into the start of line X+1 on the RIGHT. So the prediction should be: "what is the first word of the NEXT line?"

**Algorithm**:
1. For each LEFT line j, construct: `"end_of_line_j_text [MASK]"`
2. MSBERT predicts top-K words
3. Search predictions in `line_starts`
4. Score by offset consistency: if LEFT line j matches RIGHT line k, then LEFT line j+1 should match RIGHT line k+1

**Result**: Or.1081 at rank ≈#181 — improvement over v1, but still weak. Predictions remain too generic.

---

## 8. Approach 3: Generic Word Filtering

**Script**: `scripts/join_finder_v3.py`

**Idea**: A word predicted for many different source lines is generic and undiscriminative. Filter it out.

**Key change**:
```python
MAX_WORD_LINES = 3
# Count how many source lines each predicted word appears in
word_line_count = defaultdict(int)
for j, wlist in predictions.items():
    seen_words = set()
    for word, score in wlist:
        if word not in seen_words:
            word_line_count[word] += 1
            seen_words.add(word)
# Keep only words appearing in ≤3 source lines
for j in predictions:
    predictions[j] = [(w, s) for w, s in predictions[j]
                       if word_line_count[w] <= MAX_WORD_LINES]
```

**Additional fixes**:
- Changed from first-match to all-positions matching in offset scoring
- Added debug output for target fragment analysis

**Result**: Minor improvement. The fundamental problem remains: MSBERT predictions lack specificity for Hebrew manuscript text.

**Conclusion**: MSBERT is not suitable for this task. A different source of continuation words is needed.

---

## 9. Approach 4: Phrase-Based Parallel Lookup

**Script**: `scripts/join_finder_v4.py`

**Core insight**: Instead of _predicting_ missing words with MSBERT, find **parallel manuscripts** containing the same text and _read_ the continuation from them.

**Algorithm**:
1. For each torn line in LEFT, take the last 3 words as a phrase
2. Search phrase in `content` → find parallel manuscripts
3. In each parallel, locate the phrase and extract the next words (continuation)
4. Search continuation words in `line_starts`

**Result**: 33 parallels found, 32 continuation words extracted, but **target NOT FOUND**. 3-word phrases were too short — they matched irrelevant passages, producing wrong continuation words.

---

## 10. Approach 5: TF-IDF Content Similarity

**Script**: `scripts/join_finder_v5.py`

**Idea**: Two halves of the same leaf share a _topic_ even if they share no complete lines. Rare words (scholar names, technical terms) should appear in both halves.

**Algorithm**:
1. Extract all words from the fragment, compute TF-IDF for each
2. For each candidate, compute weighted overlap score: `idf_sum * log(n_shared + 1)`

**Result**: Or.1081 at rank #834 with only 7 shared usable words. This proved that **two halves of a vertical tear share very few complete words** — each line is split in two, so word overlap is minimal.

---

## 11. Approach 6: Long Phrase Text Identification

**Script**: `scripts/join_finder_v6.py`

**Idea**: 4-word phrases should be unique enough to identify the text with certainty.

**Result**: **0 shared 4-word phrases** between the two halves of Or.1081. Further proof that the halves share no literal multi-word sequences. Only parallels (other manuscripts of the same text) share these phrases.

---

## 12. Approach 7: Two-Hop via Parallels

**Script**: `scripts/join_finder_v7.py`

### The Breakthrough

**Key insight**: Instead of matching LEFT↔RIGHT directly (which fails because they share no words), use a **bridge** — a parallel manuscript containing the full text of both halves.

```
    LEFT fragment ──(phrase search)──> PARALLEL manuscript
                                            │
                    "what comes next?"       │
                                            ▼
                                     continuation words
                                            │
                    (content search)         │
                                            ▼
                                     RIGHT fragment (found!)
```

The parallel manuscript has the complete, un-torn text. By finding the LEFT's line endings in the parallel, we can read ahead to discover what words appear on the RIGHT — words that are far more specific than any MSBERT prediction.

### Algorithm

**Parameters**:
```python
PHRASE_LEN = 2          # words per phrase for parallel search
CONTINUATION_WORDS = 8  # words to extract after each phrase match
MIN_LINES_MATCHED = 2   # minimum source lines matching a candidate
```

**Phase 1 — Find parallels** (<1 second):
- For each torn line (`]`) in LEFT, take the last 2 words
- Search as phrase in `content` (Tantivy phrase query, limit 50 hits)
- Keep UIDs matching ≥3 torn lines (fallback to ≥2)

**Phase 2 — Extract continuation words**:
- For each good parallel, locate each phrase in its content
- Extract the next 8 words after the phrase (regex extraction)
- Group by source line: `continuation_by_line[j] = {word1, word2, ...}`

**Phase 3 — Search continuation words in content** (~90 seconds):
- Search each unique continuation word in `content` of all fragments
- Skip self and known parallels
- Track: `uid_hits[uid] = {source line indices}`, `uid_words[uid] = {matched words}`

**Phase 4 — Score with physical complementarity check**:
```python
score = n_matched_lines * 100 + n_matched_words * 50 + idf_sum
```
- Filter: candidates must match words from ≥2 source lines
- Physical complementarity: source has `]` lines → candidate must have ≥15% `[` lines
- IDF precomputed for all continuation words

### Results

| Case | Rank | Lines | Matching Words | Parallels | Notes |
|------|:----:|:-----:|:--------------:|:---------:|-------|
| Or.1081 | **#1** | 10 | 15 | 30 | 541 continuation words extracted |
| PGPID 3433 | **#1** | — | — | — | With PHRASE_LEN=2 |

**Batch (15 cases)**:
- Recall@1: 6.7%, Recall@10: 20%, Recall@50: 33.3%
- MRR: 0.113

### Why PHRASE_LEN=2 outperforms PHRASE_LEN=3

| Setting | Or.1081 Parallels | PGPID 3433 |
|---------|:-----------------:|:----------:|
| PHRASE_LEN=3 | 6 parallels | NOT FOUND |
| PHRASE_LEN=2 | 30 parallels | **RANK #1** |

Shorter phrases find more parallels → more continuation words → better coverage. The tradeoff is more false-positive parallels, but the multi-line matching filter (Phase 4) handles this.

---

## 13. Approach 8: v7 + FIST Visual Candidates

**Script**: `scripts/join_finder_v8.py`

### Concept

Friedberg's SVM-based image matching system (`Image_BestMarkForJoin`) has computed 35.9M visual similarity pairs across 155K documents. These are **algorithmic** candidates — not scholar-confirmed joins. The idea: if a candidate appears in _both_ our textual algorithm (v7) _and_ Friedberg's visual SVM, it is very likely a true join.

Scholarly joins from the FJMS `joins` table are used **only as ground truth** for evaluation — they never affect scoring.

### Algorithm

```
Step 1: FIST lookups
  ├── Visual: Image_BestMarkForJoin → top 50 SVM candidates
  └── Scholarly: joins table → ground truth only (NOT for scoring)

Step 2: Run v7 algorithm
  └── complementary candidates (or all if none pass complementarity filter)

Step 3: Boost + Annotate
  ├── Candidate in algorithm + in SVM → +1500 score boost
  └── Candidate in algorithm + in scholarly joins → annotation only (no boost)

Step 4: FIST visual-only candidates
  └── Top 20 SVM candidates not found by algorithm → score = SVM * 800
```

### Scoring Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| `FIST_VISUAL_BOOST` | 1500 | Bonus when algorithm candidate also appears in SVM |
| `FIST_ONLY_SVM_SCALE` | 800 | Multiplier for SVM score when candidate is visual-only |
| `FIST_VISUAL_TOP_N` | 50 | How many visual candidates to retrieve |

### Results

**Or.1081**: RANK #1
- Algorithm score: 1820.7
- SVM match: Yes (score=1.639, rank #3 among SVM candidates)
- Scholar match: Yes (Physical Join) — annotation only, no score effect
- Final score: 3320.7 (= 1820.7 + 1500)

### User-Facing Mode

```bash
python scripts/join_finder_v8.py --find "T-S 12.338"
python scripts/join_finder_v8.py --find "Or.1081 2.74"
python scripts/join_finder_v8.py --find 990001403820205171
```

Accepts shelfmark or sys_id. Output in Hebrew with score breakdown, FIST annotations, and scholar confirmations.

---

## 14. Sequential Join Finder

**Script**: `scripts/join_finder_sequential.py`

### Concept

Find fragment B whose text continues directly after the end of fragment A.

### Algorithm

**Step 1**: Extract phrases from the last 5 lines of the source fragment
**Step 2**: Search in `content` → find parallel manuscripts (≥2 phrase matches)
**Step 3**: For each parallel, find the last phrases and extract what follows (15 continuation words)
**Step 4**: Build 3-word search phrases from the continuation text
**Step 5**: Search continuation phrases (**with orthographic variants**) in `content`
**Step 6**: Score — prioritize phrases appearing near the start of the candidate

### Orthographic Variant Generation

Hebrew manuscript text has significant spelling variation (plene vs. defective writing, scribal conventions). The variant generator handles:

```python
def generate_variants(phrase_words):
    # Plene/defective vav: וו→ו, insert/remove vav
    # Plene/defective yod: יי→י, insert/remove yod
    # He/Alef alternation at word end
    return variants[:10]  # cap at 10 variants per phrase
```

### Scoring

```python
score = phrases_at_start * 300    # phrases appearing at candidate's beginning
      + n_phrases * 50            # total matching phrases
      + start_bonus * 500         # fraction of phrases at start
      + idf_sum                   # rarity
```

### Core Challenge

The algorithm correctly identifies the **continuation text** — but cannot distinguish the actual join partner from hundreds of other manuscripts containing the same liturgical text. For example, if the source is a piyyut (liturgical poem) with 50 surviving copies, all 50 will score equally.

---

## 15. Batch Evaluation Results

### v7 vs. v8 — 15 Cases (PGP Hebrew, 2-fragment documents with vertical tears)

| Metric | v7 (algorithm only) | v8 (algorithm + SVM) | Delta |
|--------|:-------------------:|:--------------------:|:-----:|
| Recall@1 | 6.7% | **20.0%** | +13.3% |
| Recall@3 | 13.3% | **26.7%** | +13.3% |
| Recall@10 | 20.0% | **40.0%** | +20.0% |
| Recall@20 | 26.7% | **40.0%** | +13.3% |
| Recall@50 | 33.3% | **46.7%** | +13.3% |
| MRR | 0.113 | **0.251** | ×2.2 |

### Per-Case Analysis

| PGPID | Quality | v7 Rank | v8 Rank | FIST | Notes |
|-------|:-------:|:-------:|:-------:|:----:|-------|
| 7523 | 30 | — | — | | No parallels in index |
| 1488 | 28 | — | **#2** | V+S | **New!** SVM found it |
| 6576 | 28 | #328 | #347 | S | SVM didn't help |
| 29376 | 27 | **#2** | #22 | | Regressed (FIST-only candidates pushed ahead) |
| 12370 | 20 | #18 | **#1** | V+S | Jumped from #18 to #1 |
| 3433 | 18 | **#1** | **#1** | | No FIST data, algorithm alone |
| 7508 | 18 | #271 | #291 | S | Far, SVM didn't help |
| 1133 | 17 | — | — | | No parallels |
| 20063 | 17 | #46 | #66 | S | Regressed |
| 28433 | 17 | — | **#10** | V+S | **New!** |
| 28434 | 17 | — | **#10** | V+S | **New!** |
| 8422 | 16 | — | — | | No parallels |
| 16106 | 16 | — | — | | No parallels |
| 25308 | 16 | #9 | **#1** | V+S | Jumped from #9 to #1 |
| 1775 | 15 | — | — | | No parallels |

**Key observations**:
- SVM visual data added 3 new finds (1488, 28433, 28434) and improved 2 to #1 (12370, 25308)
- Case 29376 regressed: FIST-only candidates with no algorithmic support displaced the target
- 6/15 cases (40%) had no textual parallels at all → fundamentally unsolvable by the current approach

---

## 16. Automatic Tear Type Detection

### Current Implementation

The v8 `--find` mode auto-detects vertical tears by counting torn-line markers:

```python
n_left = sum(1 for l in lines if l.startswith("]"))   # left half markers
n_right = sum(1 for l in lines if l.endswith("["))     # right half markers
```

If both counts are < 3, the fragment is flagged as not having enough torn lines for vertical join search.

### Proposed Comprehensive Detection

A full tear-type classifier would analyze:

1. **Torn-line markers** (`]` and `[`):
   - High `]` count → LEFT half of vertical tear
   - High `[` count → RIGHT half of vertical tear
   - Both → fragment with tears on both sides (rare)
   - Neither → complete lines (not a vertical tear candidate)

2. **Line-length gradient** (for corner tears):
   - Measure character count per line
   - If lines shorten progressively toward one corner → corner tear
   - The gradient direction identifies which corner is missing

3. **Line completeness** (for horizontal tears):
   - All lines complete but the page appears to start/end mid-sentence → possible horizontal tear
   - Harder to detect without content analysis

4. **Recommended output**:
   ```
   Fragment analysis for T-S 12.338:
     99 lines total
     48 lines start with ] → LEFT half of vertical tear (49%)
     56 lines end with [   → also has right-torn lines (57%)
     Recommended search: VERTICAL TEAR (both directions)

   Fragment analysis for T-S NS 329.267:
     12 lines total, all complete (no torn markers)
     Line lengths: 45, 42, 38, 31, 22, 15 characters
     → Progressive shortening detected: CORNER TEAR (bottom-right)
     Estimated missing text: 0–30 characters per line
   ```

### Not Yet Implemented

The current system only handles vertical tears. Corner tear detection, horizontal tear detection, and comprehensive auto-classification remain future work.

---

## 17. Candidate Pre-screening

### Which Fragments Should Be Searched?

Not every fragment is a join candidate. A pre-screening step could filter the ~217K index to identify fragments likely to have join partners:

**Positive indicators** (likely has a join partner):
- Contains torn-line markers (`]` or `[`): 15%+ of lines are torn
- Very short lines suggesting text is missing
- Asymmetric content: text appears to start or end abruptly

**Negative indicators** (probably complete):
- All lines are full-length and complete
- Text begins and ends at natural boundaries (chapter starts, colophons)
- Fragment is a complete bifolio or intact leaf

### Scalability Consideration

Running v8 on all ~217K fragments is not feasible (~90 seconds each = ~226 days sequential). Practical approaches:
- Pre-filter to fragments with ≥5 torn lines (~10-20K candidates)
- Batch parallelize (the Tantivy searches are read-only)
- Cache the parallel-finding step (Phase 1) — same parallels serve multiple fragment analyses

---

## 18. Line Length Estimation

### For Sequential Joins

When searching for a continuation, the expected length of the first line on the partner fragment can be estimated from the source fragment's average line length. If the source consistently has ~60 characters per line, the continuation should start with a line of similar length.

This could help disambiguate between the true join partner and other copies:
- True partner: first line length matches source's average
- Other copies: line length varies based on their own scribal layout

### For Corner Tears

Corner tear line-length estimation is more complex:

```
Main fragment line lengths:     60, 58, 55, 50, 42, 30
Estimated missing text:          0,  2,  5, 10, 18, 30 characters

Corner piece line lengths:      30, 18, 10,  5,  2,  0
(mirror of what's missing)
```

The gradient of decreasing length on the main fragment predicts the gradient of increasing length on the corner piece. This could serve as a powerful filter: only candidates whose line-length gradient mirrors the source's gradient should be considered.

### Not Yet Implemented

Line-length estimation is a promising future direction but is not yet incorporated into any algorithm version.

---

## 19. Known Limitations

### Fundamental Limitations

1. **Parallel dependency**: The algorithm only works when parallel manuscripts exist in the index. For unique documents (personal letters, contracts, court records) with no parallels, there is no way to discover continuation words. This affects ~40% of test cases.

2. **Copy disambiguation**: In sequential joins, the algorithm finds the correct _text_ but cannot distinguish the actual join partner from other copies of the same work. This is especially problematic for liturgical texts with many surviving copies.

3. **PHRASE_LEN=2 fragility**: 2-word phrases produce false-positive parallels. 3-word phrases find fewer parallels. The optimal setting is content-dependent.

4. **Hebrew-only**: The algorithms assume Hebrew script. Judeo-Arabic content (Hebrew characters, Arabic language) would need adapted word-cleaning and variant generation.

### Technical Limitations

5. **Runtime**: ~90–150 seconds per fragment (Phase 3 word search dominates). Not suitable for batch processing of the entire corpus without parallelization.

6. **FIST coverage**: Not all fragments have DocumentIds in FIST (the mapping chain AlmaId→InventoryId→FGP→DocumentId has gaps).

7. **Complementarity threshold (15%)**: May filter out true joins that have few torn lines (e.g., fragments where only a small corner is torn).

### Data Limitations

8. **Index coverage**: Only ~217K of ~400K Genizah fragments are indexed. Missing fragments cannot be found.

9. **SVM quality**: Friedberg's SVM was trained on visual features — it sometimes scores visually similar but textually unrelated fragments highly.

10. **Transcription quality**: The algorithm depends on transcription accuracy. Errors in `]`/`[` marking or word tokenization degrade results.

---

## 20. Future Directions

### Near-term Improvements

1. **Combined v7 + Sequential**: Run both algorithms in parallel, merge and deduplicate results. A fragment could have both a vertical tear partner and a sequential continuation.

2. **Expanded batch evaluation**: Test against all ~14,906 FJMS JoinGroups as ground truth (currently only 15 PGP test cases).

3. **Metadata filtering**: Use domain classification, time period, script type, and library of origin to pre-filter candidates. Two fragments from the same domain (e.g., "Talmud") are more likely to join.

4. **IDF-weighted Phase 3**: Currently all continuation words are searched equally. Prioritizing rare words would reduce search time and improve precision.

5. **Corner tear detection**: Implement line-length gradient analysis to detect and search for corner tear joins.

### Medium-term Research

6. **Line-length scoring for sequential joins**: Use average line length as a discriminating feature between the true join partner and other copies.

7. **Cross-index linking**: Combine Tantivy textual data with NLI image metadata (dimensions, image features) for multi-modal scoring.

8. **Bidirectional search**: Currently we search from LEFT→RIGHT. Running RIGHT→LEFT as well and intersecting results could improve precision.

9. **Automatic score calibration**: Learn optimal weights for algorithm score vs. SVM score from ground truth data.

### Long-term Vision

10. **Full corpus scan**: Pre-compute parallel clusters and continuation word caches for all ~217K fragments, enabling near-instant join queries.

11. **Image-based features**: Integrate actual image analysis (ruling patterns, ink analysis, fiber patterns) alongside textual and SVM signals.

12. **Collaborative feedback loop**: Surface algorithmic candidates to researchers through the GenizahSearch UI, collect confirmations/rejections, and use them to improve the model iteratively.

### Recommended App-Oriented Implementation Plan (March 2026)

After reviewing the active scripts and validating the current behavior on the
two benchmark cases (Or.1081 and PGPID 3433), the strongest recommendation is
to treat v7/v8 as a research prototype and refactor the join finder into a
direction-aware retrieval + reranking service before embedding it in the app.

#### Key findings from the current implementation

1. **The current vertical algorithm is effectively LEFT-only**: the user-facing
   v8 tool reports both `]` and `[` counts, but the core search only processes
   lines starting with `]`. This means the current algorithm supports "LEFT half
   -> find RIGHT half" but not the mirror direction yet.

2. **Phase 3 is too slow for interactive use**: the current continuation-word
   fan-out over `content` dominates runtime. Live validation on March 15, 2026:
   Or.1081 took about 101 seconds, PGPID 3433 took about 83 seconds.

3. **The scripts ignore existing positional index fields**: the Tantivy index
   already stores `line_starts`, `line_ends`, and `L{n}:word` positional tokens,
   but v7/v8 currently search continuation words against full `content`.

4. **Mixed index scopes create duplicate/noisy candidates**: the index contains
   `page`, `system`, and `part` documents, but the join scripts do not restrict
   scope. This produces duplicate candidates for the same manuscript and wastes
   search effort.

5. **FIST visual-only candidates should be separated from textual ranking**:
   fixed-score visual-only candidates are useful, but should be shown as a
   separate evidence bucket or gated fallback instead of sharing the same rank
   scale as text-supported matches.

#### Recommended architecture

1. **Fragment profiler / router**
   - Auto-detect likely tear or continuation mode:
     - left tear, right tear
     - top/bottom continuation
     - corner tear
     - uncertain / run everything
   - Features should include torn-line markers, line-length gradients, abrupt
     starts/ends, and average line width.
   - The researcher should be able to override the automatic choice.

2. **Structured candidate generation**
   - Restrict retrieval to `scope="system"` and deduplicate by `sys_id`.
   - Use `line_ends` / `line_starts` and positional `L{n}:word` tokens instead
     of broad full-content word fan-out.
   - Run separate generators for:
     - LEFT -> RIGHT vertical joins
     - RIGHT -> LEFT vertical joins
     - sequential BEFORE -> AFTER
     - sequential AFTER -> BEFORE
     - corner tear / horizontal fallback

3. **Feature-based reranking**
   - Rank candidates using:
     - number of supported torn lines / phrases
     - rarity of matched continuation words
     - line-offset consistency
     - edge complementarity
     - line-length compatibility
     - FIST SVM support
     - metadata compatibility (domain, date, script, material, library)
     - bidirectional confirmation when both fragments retrieve each other

4. **Offline precomputation for app latency**
   - Precompute candidate pools, parallel caches, and IDF/DF statistics.
   - Store the results in a local sidecar cache so the app can show initial
     suggestions immediately and only do lightweight reranking live.

5. **Researcher-facing app workflow**
   - Add a "Find Joins" panel from the manuscript view.
   - UI controls should separate:
     - **search type**: physical join, sequential continuation, both
     - **direction / shape**: auto, left, right, upper, lower, corner, any
   - Results should be grouped by evidence:
     - text + visual
     - text only
     - visual fallback
     - already known in FJMS

For the detailed implementation roadmap, see
`docs/plans/JOIN_FINDER_IMPLEMENTATION_PLAN.md`.

---

## 21. Script Index

| File | Version | Status | Description |
|------|---------|--------|-------------|
| `scripts/join_finder_poc.py` | PoC | Abandoned | Direct MSBERT predictions |
| `scripts/join_finder_v2.py` | v2 | Abandoned | Cross-Line MSBERT |
| `scripts/join_finder_eval.py` | — | Abandoned | Batch eval for MSBERT approach |
| `scripts/join_finder_v3.py` | v3 | Abandoned | MSBERT + generic word filtering |
| `scripts/join_finder_v4.py` | v4 | Abandoned | 3-word phrase parallels |
| `scripts/join_finder_v5.py` | v5 | Abandoned | TF-IDF content similarity |
| `scripts/join_finder_v6.py` | v6 | Abandoned | Long phrases (4-word) |
| `scripts/join_finder_v7.py` | v7 | **Active** | Two-Hop via parallels — core algorithm |
| `scripts/join_finder_v8.py` | v8 | **Active** | v7 + FIST SVM visual candidates |
| `scripts/join_finder_sequential.py` | seq | **Active** | Sequential (end→start) continuation |

### Usage

```bash
# Find joins for a specific fragment (user-facing)
python scripts/join_finder_v8.py --find "T-S 12.338"
python scripts/join_finder_v8.py --find "Or.1081 2.74"
python scripts/join_finder_v8.py --find 990001403820205171

# Test cases
python scripts/join_finder_v7.py                      # Or.1081
python scripts/join_finder_v7.py --pgpid3433           # PGPID 3433
python scripts/join_finder_v8.py                       # v8 = v7 + FIST

# FIST data inspection
python scripts/join_finder_v8.py --fist-only ALMA_ID

# Batch evaluation
python scripts/join_finder_v7.py --batch --n=20
python scripts/join_finder_v8.py --batch --n=20

# Sequential joins
python scripts/join_finder_sequential.py --batch --n=15
```
