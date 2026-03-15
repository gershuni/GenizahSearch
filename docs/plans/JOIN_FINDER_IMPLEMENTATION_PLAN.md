# Join Finder Implementation Plan

> **Status:** Planning
> **Date:** 2026-03-15
> **Priority:** High

---

## Executive Summary

The current join finder (`v7` / `v8`) is a strong research prototype but is not
ready to be embedded directly into the manuscript view in GenizahSearch. The
best path is a hybrid system:

1. direction-aware fragment profiling
2. structured candidate retrieval using existing Tantivy position fields
3. feature-based reranking with textual, physical, visual, and metadata signals
4. offline precomputation and caching for app latency

The main goal is to let a researcher open a manuscript, click **Find Joins**,
choose a mode such as `before`, `after`, `left`, `right`, `upper`, `lower`,
`corner`, or `everything`, and receive useful results in seconds rather than
minutes.

---

## Current Findings

### What works today

- `v7` / `v8` can rank the true join at `#1` on the two known benchmark cases:
  - Or.1081
  - PGPID 3433
- The two-hop "via parallels" idea is the core breakthrough.
- FIST visual candidates are useful as supporting evidence.

### What blocks app embedding today

1. **Latency is too high**
   - Live validation on 2026-03-15:
     - Or.1081: about 101s
     - PGPID 3433: about 83s
   - Phase 3 continuation-word fan-out dominates runtime.

2. **Vertical search is only implemented in one direction**
   - Current v7/v8 processes lines starting with `]`.
   - This supports LEFT -> RIGHT, but not the mirror RIGHT -> LEFT flow.

3. **The scripts do not use the best available index fields**
   - The Tantivy index already contains:
     - `line_starts`
     - `line_ends`
     - positional `L{n}:word` tokens
     - `scope` (`page`, `system`, `part`)
     - `boundaries`
   - Current join scripts mostly search raw `content`.

4. **Mixed scopes produce duplicate candidates**
   - The same manuscript can appear multiple times as page-level and system-level
     hits, which creates duplicate/noisy results.

5. **FIST-only candidates are mixed too aggressively into the main ranking**
   - Visual-only candidates should be exposed separately or used as gated
     fallback, not promoted with a fixed linear scale into the same score space.

---

## Product Goal

From any manuscript view, the user should be able to:

1. open a **Find Joins** panel
2. accept an auto-detected join mode or override it
3. choose what they are looking for:
   - `before`
   - `after`
   - `left`
   - `right`
   - `upper`
   - `lower`
   - `corner`
   - `everything`
4. review ranked candidates with clear evidence labels
5. create or propose a join from the result list

---

## Recommended Architecture

### 1. Fragment Profiler / Router

Build a lightweight classifier that predicts which search mode should run.

#### Inputs

- proportion of lines starting with `]`
- proportion of lines ending with `[`
- line-length gradient by direction
- abrupt starts / abrupt endings
- average line length
- number of meaningful lines

#### Outputs

- vertical left-tear
- vertical right-tear
- sequential before -> after
- sequential after -> before
- corner tear
- uncertain / run multiple modes

#### Requirement

The classifier must support manual override in the UI.

---

### 2. Candidate Retrieval

#### A. Vertical Join Retrieval

Use the existing two-hop idea, but make retrieval structured:

- retrieve parallels using short phrase anchors from line endings or starts
- extract predicted continuation or preceding words from the parallels
- search candidate manuscripts using:
  - `scope="system"`
  - `line_starts` for expected right-half starts
  - `line_ends` for expected left-half endings
  - positional `L{n}:word` tokens when line alignment is strong

This replaces the current "search every continuation word against all content"
pattern.

#### B. Sequential Retrieval

Keep the current sequential algorithm as a base, but add:

- line-length compatibility between source end and candidate start
- metadata priors
- start-of-fragment / end-of-fragment penalties
- bidirectional confirmation

#### C. Corner / Horizontal Retrieval

Add a fallback mode driven by line-length gradients and visual candidates:

- estimate missing text per line from the slope of line lengths
- search for candidates with mirrored gradient behavior
- rely more heavily on FIST visual evidence and metadata filters

---

### 3. Feature-Based Reranking

Replace fixed heuristic scores with a richer feature vector.

#### Suggested features

- number of supporting torn lines
- number of supporting continuation phrases
- IDF sum / rarity of support words
- offset consistency across lines
- edge complementarity
- line-length compatibility
- same domain / author / work / time period
- same material / script when available
- FIST SVM score
- bidirectional agreement
- same manuscript retrieved through multiple modes

#### Training / tuning data

Use `fjms_enrichment.db` as ground truth:

- `48,655` join memberships
- `14,906` distinct join groups

Initial implementation can use hand-tuned weights; a later phase should learn
weights from the FJMS join data.

---

### 4. Precomputation and Caching

For app use, the join finder should not run as a cold full-corpus scan on every
click.

#### Recommended strategy

- precompute:
  - parallel clusters
  - document frequency / IDF values
  - top candidate pools per manuscript and per mode
  - FIST mapping caches
- store results in a local sidecar cache, for example:
  - `join_candidates.db`
  - or a versioned JSON/SQLite cache under app data

#### Runtime behavior

- initial results should load from cache in under 2s
- changing a mode or direction should rerank/filter a cached pool
- a deeper "search everything" option may run a slower background task

---

## UI Plan

### Entry Point

Add a **Find Joins** action from the manuscript view in both web and desktop.

### Controls

#### Search Type

- Auto
- Physical join
- Sequential continuation
- Both

#### Direction / Shape

- Auto
- Left
- Right
- Upper
- Lower
- Corner
- Any / Everything

#### Search Depth

- Fast
- Balanced
- Deep

### Result Buckets

- Text + Visual
- Text only
- Visual fallback
- Already known in FJMS

Each result should show:

- shelfmark
- score
- evidence tags
- why it matched
- quick action to open manuscript
- quick action to propose/create join

---

## Implementation Phases

### Phase 1: Correctness and Retrieval Refactor

- restrict join retrieval to `scope="system"`
- deduplicate by `sys_id`
- split LEFT -> RIGHT and RIGHT -> LEFT vertical modes
- replace Phase 3 word fan-out with `line_starts` / `line_ends` retrieval
- move FIST-only results into a separate result bucket

### Phase 2: Fragment Router and Additional Modes

- build fragment profiler
- add sequential before/after routing
- add corner-tear detection
- add "run everything" orchestration and merge logic

### Phase 3: Evaluation and Learned Calibration

- evaluate against all FJMS join groups
- collect metrics by join type
- tune or learn ranking weights
- define stable quality thresholds for surfacing candidates

### Phase 4: App Integration

- add shared join-finder service
- add local cache layer
- add manuscript-view UI
- add researcher feedback loop

---

## Success Metrics

### Quality

- higher Recall@10 on FJMS physical joins
- improved MRR over current `v8`
- fewer duplicate candidates in top results

### Latency

- cached result load: under 2s
- uncached focused search: under 10s
- deep search: background task with progress

### Product

- researcher can choose mode and direction explicitly
- candidates are interpretable
- confirmed joins can be fed back into the community join workflow

---

## Immediate Next Step

Implement Phase 1 first. That is the highest-value step for both accuracy and
latency, and it is the minimum required before the join finder can be embedded
into the manuscript view.
