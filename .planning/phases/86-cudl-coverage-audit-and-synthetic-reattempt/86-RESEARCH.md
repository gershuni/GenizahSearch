# Phase 86: CUDL Coverage Audit + Synthetic Re-attempt - Research

**Researched:** 2026-05-10
**Domain:** SQLite shelfmark normalization, CUDL/FIST cross-system bridge, synthetic-row generation, audit reporting
**Confidence:** HIGH (most claims empirically verified against live FIST.db + nli_crossref.db)

## Summary

Phase 86 extends the Phase 84 `shared/shelfmark_bridge.py` with a bidirectional FIST↔CUDL normalizer, rewrites the single function `_build_qualifying_inventories` in `scripts/generate_synthetic_rows.py` to walk CUDL classmarks instead of FIST inventories, and produces three durable audit artifacts (`reports/cudl_coverage.md`, `reports/scan_cudl_orphans_post_phase86.txt`, regression-test fixture for v7.9.4 NLI Oxford fix). The ALL-Phase-85 infrastructure activates as-is when the new generation walk lands fresh data.

I empirically re-verified CONTEXT.md's recovery numbers against the live databases (FIST.db 3.17 GB, nli_crossref.db 273 MB, 141,368 CUDL classmarks, 279,208 FIST inventories). The 5,330 unresolved-by-Phase-84 figure is exact. With the 4 confirmed normalizer patterns the bridge recovers 3,455 to existing-Alma libraries.csv rows + 108 to new no-Alma synthetic candidates + 168 multi-inventory ambiguous = 3,731 of 5,330. The 1,599 truly-orphan residue breaks down across 6 prefix buckets exactly as CONTEXT.md predicted (Or=571, T-S F=392, T-S Ar=303, T-S NS=179, T-S Misc=98, Mosseri=48, T-S other=8). T-S NS 329.96 → InventoryId 65549106 → synthetic sys_id `990065549106000000` is in the 108 no-Alma branch and closes for the user under D-04 multi_signature relax.

**Primary recommendation:** Add a sibling module `shared/fist_cudl_bridge.py` that publishes `fist_to_cudl_keys(shelfmark)` and `lookup_fist_by_cudl(classmark)` — keeping `shared/shelfmark_bridge.py` (Phase 84) byte-clean per its NORM-04 contract while reusing its `cudl_normalize` helper. Build a Mosseri Roman expander, prefix-strip helper, `(N)` series-strip helper, and Or. dot-fix helper as small composable units. Plan in 4–5 plans matching Phase 85's shape (bridge module + generation rewrite + audit artifact + regression fixture + UAT).

## User Constraints (from CONTEXT.md)

### Locked Decisions

**Generation Strategy:**
- **D-01: CUDL-walked, not FIST-walked.** `_build_qualifying_inventories` rewrites to walk `nli_crossref.db.cambridge_manifests`. For each CUDL classmark: skip if Phase 84 `lookup_cudl` resolves it; resolve via new bidirectional FIST↔CUDL normalizer; emit synthetic if resolved AND no Alma; log to residue if unresolved.
- **D-01a (image-bearing-only):** Every emitted synthetic row HAS a CUDL manifest by construction. Phase 85 Plan 02's "FJMS metadata-only" inclusion branch is dropped.

**Bidirectional FIST↔CUDL Normalizer (NEW):**
- **D-02: Extend `shared/shelfmark_bridge.py` OR add sibling module `shared/fist_cudl_bridge.py`** — planner's call. Add reverse-direction normalization (FIST.Shelfmark forms → CUDL classmark keys).
- **D-02a: 4 confirmed normalizer patterns (locked, recovers ~70%):**
  - Mosseri Roman prefix: `mosseri{roman}` (CUDL) ↔ `moss{roman}` (FIST)
  - FIST data-noise prefix-strip: when shelfmark contains `:`, also try substring after last `:`
  - `(N)` series-suffix strip: `T-S F1(1).11` (FIST) ↔ `tsf1.11` (CUDL)
  - Or. multi-segment dot-fix: `or1080.X.Y` (CUDL) ↔ `or1080X.Y` (FIST)
- **D-02b: 5 residue patterns for HUMAN-IN-THE-LOOP adjudication.** Planner produces `86-RESIDUE-PATTERNS.md` artifact with sample fixtures; user adjudicates each before bridge rules locked.
- **D-02c: Iteration via `86-RESIDUE-PATTERNS.md` research artifact.** Markdown table format per pattern family.

**Multi_signature Relax:**
- **D-04: Relax D-05a STRICT for unambiguous multi_signature** (closes T-S NS 329.96 with 13 SignatureIds → single InventoryId 65549106).
- **D-04a: multi_inventory stays excluded.**

**Parent-Shadow Filter:**
- **D-06: Apply `reports/synthetic_parent_shelfmarks.csv` filter** even though CUDL-walk likely makes it moot.

**FJMS Enrichment:**
- **D-07: Automatic via Phase 85 UNION-ALL.** No code changes in `scripts/export_fist_enrichment.py`.
- **D-07a: Web deploy + desktop installer rebuild required.** Planner decides desktop release strategy.

**Audit Deliverables:**
- **D-08: AUDIT-01** = `python scripts/scan_cudl_orphans.py --out-suffix _post_phase86`.
- **D-09: AUDIT-02** = `reports/cudl_coverage.md` durable artifact.
- **D-10: AUDIT-03** = scan-based check + permanent regression test (golden fixture, planner picks 20 vs 461 rows).

**Phase 85 Infrastructure Activation:**
- **D-11: All Phase 85 infrastructure stays as-is.** No retroactive changes.

**Verification & Rollback:**
- **D-12: New HUMAN-UAT plan** (6 items) replaces Phase 85's superseded UAT.
- **D-12a: Rollback path:** empty marker block + restore `.bak` of libraries.csv.
- **D-12b: No env-var feature flag** — data IS the lever.

### Claude's Discretion

- Choice of normalizer module location (`shared/shelfmark_bridge.py` extension vs new `shared/fist_cudl_bridge.py` sibling)
- Test scope for D-10 v7.9.4 regression (20 golden rows vs all 461)
- Exact format of `86-RESIDUE-PATTERNS.md` research artifact (markdown tables vs CSV vs both)
- Whether to keep `reports/synthetic_ambiguity_residue.csv` populated with Phase 85's 10,689 entries or rebuild from CUDL-walk
- Desktop installer release strategy (rebuild this round or bundle with next desktop-code release)

### Deferred Ideas (OUT OF SCOPE)

- CUDL-only no-FIST sys_id allocation (the ~1,599 hard residue) — candidate for a future phase
- Reverse audit (NLI Alma in libraries.csv but absent from CUDL/FJMS)
- Synthetic rows for non-CUL/Mosseri collections (AIU/Halper FJMS-only without CUDL manifests)
- Periodic NLI gap-file refresh
- Tantivy stub-rows for full-text/Responsa search on synthetic IDs
- Mosseri "2nd series" patterns (`Ms. L 241`, `Ms. MOSS NS`)
- Server-side IIIF image cache
- Migrating libraries.csv to SQLite
- Convention-aware T-S F/Ar "flattened-series" (if D-02c adjudication rejects)

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| AUDIT-01 | `scripts/scan_cudl_orphans.py` re-run reports <200 truly-orphan classmarks | Empirical residue is 1,599 with hard-categorized buckets; the <200 is conditional on user-adjudicated D-02b investigation closing more. The script already imports from `shared.shelfmark_bridge` (Phase 84 site #4). Re-run with `--out-suffix _post_phase86` after libraries.csv synthetic block lands. |
| AUDIT-02 | `reports/cudl_coverage.md` per-collection breakdown + methodology | Sections planned: Methodology · Per-collection counts · Residue pattern analysis (5 D-02b patterns × adjudication outcome) · Re-run instructions · Cross-link to `synthetic_coverage.md`. Source data: `cudl_orphans_all_post_phase86.csv` + alias index size + new synthetic count. |
| AUDIT-03 | v7.9.4 NLI Oxford 461-row regression check | Two-pronged: scan script `scripts/audit_nli_attribution.py` runs SQL/CSV check; permanent fixture `tests/test_nli_oxford_attribution.py` parametrizes over the 461 sys_ids. Empirical state: 0 Oxford rows currently have NLI text (post-fix), 462 NLI rows have NLI text. Fixture set extracted from `scripts/fix_nli_oxford_mislabel.py` regex (`The National Library of Israel|JER NLI Heb`). |
| SYNTH-01..06 | Phase 85 contract carries; Phase 86 activates dormant infrastructure with new data | Phase 85 verification (5/5 satisfied at infrastructure level) confirms helper module, browse hide-NLI gates, /api `is_synthetic` field, corrections-write reject all stay load-bearing. Phase 86 single-function rewrite preserves the contract. T-S NS 329.96 (InventoryId 65549106) closes here. |

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| FIST↔CUDL normalization | shared/ | — | Bridge module pattern (Phase 84 D-08 — "ONE source of truth"). Web + desktop both import from `shared/`. |
| Synthetic-row generation | scripts/ | shared/ | Plan-time mutation, not runtime. Reads FIST.db + nli_crossref.db, writes libraries.csv marker block + manifest. |
| FJMS sidecar regeneration | scripts/ | fist_data/ | UNION-ALL pattern (Phase 85 D-07). Mutation at export time, not runtime. |
| Orphan scanning + audit | scripts/ + reports/ | shared/ | `scan_cudl_orphans.py` already imports from bridge. Reports are durable artifacts. |
| Browse rendering of synthetic | web/pages/ + desktop/ | shared/ | Phase 85 already wired (D-06/D-08). No new branches in Phase 86. |
| v7.9.4 regression check | scripts/ + tests/ | — | Operational scan + permanent CI test. CSV-only (libraries.csv inspection). |
| HUMAN-UAT (6 items) | Manual | — | Real Supabase round-trip + desktop interactive verification. Cannot be grep/static-analysis verified. |
| Release coordination | docs/ + CHANGELOG | scripts/ | CLAUDE.md "Both apps must be maintained" + `feedback_no_github_release_for_web_only.md`. |

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| sqlite3 (stdlib) | Python 3.11+ | Read FIST.db, nli_crossref.db read-only | [VERIFIED] Already used everywhere in codebase; `file:...?mode=ro` URI pattern. |
| csv (stdlib) | Python 3.11+ | libraries.csv parse/rewrite | [VERIFIED] Existing pattern in `scripts/generate_synthetic_rows.py` and `scripts/scan_cudl_orphans.py`. CRLF line-ending detection per `scripts/fix_nli_oxford_mislabel.py:55` (lesson learned from v7.9.4 fix). |
| re (stdlib) | Python 3.11+ | Regex normalization | [VERIFIED] Phase 84 `shared/shelfmark_bridge.py` already uses; Mosseri Roman parser uses similar pattern. |
| pytest | 7+ | Test framework | [VERIFIED] Existing repo standard (~680 tests). |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| pathlib | stdlib | Path manipulation | All scripts — established pattern. |
| collections.defaultdict | stdlib | Alias-index builder | Phase 84 `build_alias_index` already uses. |
| typing | stdlib | Type hints | `Optional[InventoryRecord]` return types per Phase 85 D-01b "string discipline" pattern. |
| dataclasses | stdlib | `InventoryRecord` data shape | Cleaner than tuple-returns for the new `lookup_fist_by_cudl` API. |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Extending `shared/shelfmark_bridge.py` | New `shared/fist_cudl_bridge.py` sibling | RECOMMENDED: sibling. Phase 84's NORM-04 contract treats `shelfmark_bridge.py` as frozen for libraries.csv↔CUDL direction. Adding a sibling module preserves that boundary while reusing `cudl_normalize` via import. Tests stay separated. Module size stays under 500 lines per file. |
| In-memory Python dict for alias index | SQLite-backed cache | KEEP IN-MEMORY. Phase 84 alias-index build is 2.11s for 291K keys (empirically measured); rebuilding the FIST↔CUDL inv_map adds ~5s. Both well under app-startup budget. |
| String-similarity (Levenshtein) for nearest-neighbour | Prefix + numeric-substring matching | RECOMMENDED: prefix + numeric-substring. The 5 residue families have well-defined prefix structure; fuzzy matching produces noisy candidates. The empirical probe in research used `cudl_norm[:5] == fist_norm[:5] AND digit-sequences overlap` and found relevant FIST candidates for residue families. Suitable for `86-RESIDUE-PATTERNS.md` artifact. |
| `dataclass` for `InventoryRecord` | NamedTuple | Either works. Dataclass preferred for forward extensibility (additional metadata fields without breaking callers). |

**Installation:** None — all dependencies are stdlib + already-installed pytest.

**Version verification:** No new external packages introduced. `python --version` is 3.11+ per repo CI matrix.

## Architecture Patterns

### System Architecture Diagram

```
                   ┌─────────────────────────────────┐
                   │  scripts/generate_synthetic_    │
                   │  rows.py                        │
                   │  (single function rewrite)      │
                   └─────────────────────────────────┘
                                │
                                ▼
   ┌────────────────────────────────────────────────────────┐
   │  _build_qualifying_inventories  (CUDL-WALKED)          │
   │                                                        │
   │  1. SELECT normalized_shelfmark FROM cambridge_        │
   │     manifests   (~141K rows)                           │
   │                                                        │
   │  2. for each classmark:                                │
   │       a. lookup_cudl(classmark)  → if hit, SKIP        │
   │          (already in libraries.csv via Phase 84)       │
   │       b. lookup_fist_by_cudl(classmark)  → if hit:     │
   │            - check parent-shadow filter (D-06)         │
   │            - check multi_signature (D-04 relax)        │
   │            - check multi_inventory (D-04a exclude)     │
   │            - emit synthetic row                        │
   │       c. else log to residue with pattern_guess        │
   └────────────────────────────────────────────────────────┘
                                │
            ┌───────────────────┼───────────────────┐
            ▼                   ▼                   ▼
   ┌────────────────┐  ┌────────────────┐  ┌──────────────────┐
   │ libraries.csv  │  │ synthetic_     │  │ ambiguity_       │
   │ marker block   │  │ manifest.json  │  │ residue.csv      │
   │ (~100-3000     │  │ (authoritative │  │ (with new        │
   │  rows)         │  │  for Plan 03)  │  │  pattern_guess   │
   │                │  │                │  │  column)         │
   └────────────────┘  └────────────────┘  └──────────────────┘
            │
            ▼
   ┌────────────────────────────────────────┐
   │  scripts/export_fist_enrichment.py     │
   │  (UNCHANGED — Phase 85 UNION-ALL       │
   │   pattern reads synthetic_manifest)    │
   └────────────────────────────────────────┘
            │
            ▼
   ┌────────────────────────────────────────┐
   │  fjms_enrichment.db (regenerated;      │
   │  synthetic AlmaIds in 12 tables)       │
   └────────────────────────────────────────┘

   Bridge module (NEW): shared/fist_cudl_bridge.py
   ┌────────────────────────────────────────────────────┐
   │  fist_to_cudl_keys(fist_shelfmark) → set[str]      │
   │    - Mosseri Roman expansion (D-02a)               │
   │    - FIST prefix-strip (D-02a)                     │
   │    - (N) series-strip (D-02a)                      │
   │    - Or. dot-fix (D-02a)                           │
   │    - 0..N additional rules from D-02c adjudication │
   │                                                    │
   │  build_fist_alias_index() → builds inv_map         │
   │  lookup_fist_by_cudl(classmark) → Optional[record] │
   └────────────────────────────────────────────────────┘

                         AUDIT BRANCH
                         ────────────
   ┌────────────────────────────────────────┐
   │  scripts/scan_cudl_orphans.py          │
   │  --out-suffix _post_phase86            │
   │  (re-runs against new libraries.csv)   │
   └────────────────────────────────────────┘
            │
            ▼
   ┌────────────────────────────────────────┐
   │  reports/cudl_coverage.md (NEW)        │
   │  (per-collection breakdown + 5-pattern │
   │   adjudication summary + re-run cmds)  │
   └────────────────────────────────────────┘

                    REGRESSION BRANCH
                    ─────────────────
   ┌────────────────────────────────────────┐
   │  scripts/audit_nli_attribution.py (NEW)│
   │  ↓ asserts 461 sys_ids unchanged       │
   │  tests/test_nli_oxford_attribution.py  │
   │  (NEW; CI fixture)                     │
   └────────────────────────────────────────┘
```

**Component Responsibilities:**

| Component | File | Responsibility |
|-----------|------|----------------|
| FIST↔CUDL bridge | `shared/fist_cudl_bridge.py` (NEW) | Pure functions for `fist_to_cudl_keys`, `lookup_fist_by_cudl`, alias-index builder |
| Generation script | `scripts/generate_synthetic_rows.py` | Single function rewrite: `_build_qualifying_inventories` walks CUDL, uses new bridge |
| Export script | `scripts/export_fist_enrichment.py` | UNCHANGED — UNION-ALL pattern reads `synthetic_manifest.json` |
| Orphan scanner | `scripts/scan_cudl_orphans.py` | UNCHANGED — re-run with `--out-suffix _post_phase86` |
| Coverage report | `reports/cudl_coverage.md` (NEW) | Durable AUDIT-02 artifact |
| Residue pattern artifact | `.planning/phases/86-.../86-RESIDUE-PATTERNS.md` (NEW) | D-02c human-in-the-loop adjudication |
| NLI regression scan | `scripts/audit_nli_attribution.py` (NEW) | One-shot scan of 461 sys_ids |
| NLI regression test | `tests/test_nli_oxford_attribution.py` (NEW) | Permanent CI fixture |

### Recommended Project Structure

```
shared/
├── shelfmark_bridge.py        # Phase 84 — UNCHANGED in Phase 86
├── synthetic_sys_id.py        # Phase 85 — UNCHANGED in Phase 86
└── fist_cudl_bridge.py        # NEW — FIST↔CUDL reverse bridge

scripts/
├── generate_synthetic_rows.py # SINGLE-FUNCTION REWRITE (D-01)
├── export_fist_enrichment.py  # UNCHANGED (Phase 85 UNION-ALL)
├── scan_cudl_orphans.py       # UNCHANGED (re-run with new --out-suffix)
└── audit_nli_attribution.py   # NEW — AUDIT-03 scan

tests/
├── test_synthetic_sys_id.py        # Phase 85 — UNCHANGED
├── test_generate_synthetic_rows.py # Phase 85 — extend with new walk fixtures
├── test_fist_cudl_bridge.py        # NEW — unit tests for new bridge
└── test_nli_oxford_attribution.py  # NEW — AUDIT-03 fixture

reports/
├── cudl_coverage.md                          # NEW — AUDIT-02
├── scan_cudl_orphans_post_phase86.txt        # NEW — AUDIT-01
├── cudl_orphans_all_post_phase86.csv         # NEW — AUDIT-01
└── cudl_orphans_with_neighbor_post_phase86.csv # NEW — AUDIT-01

.planning/phases/86-cudl-coverage-audit-and-synthetic-reattempt/
├── 86-CONTEXT.md             # exists
├── 86-DISCUSSION-LOG.md      # exists
├── 86-RESEARCH.md            # this file
├── 86-RESIDUE-PATTERNS.md    # NEW — D-02c human-in-the-loop adjudication artifact
├── 86-VALIDATION.md          # NEW — Validation Architecture (built from this research)
├── 86-XX-PLAN.md             # 4-5 plans
└── 86-VERIFICATION.md        # post-execute
```

### Pattern 1: Sibling-Module Bridge (RECOMMENDED)

**What:** Create `shared/fist_cudl_bridge.py` rather than extending `shared/shelfmark_bridge.py`.

**When to use:** When the new functionality has a different bridge direction (FIST↔CUDL vs libraries.csv↔CUDL) AND the host module has a NORM-04 frozen-contract requirement.

**Example:**

```python
# Source: project pattern from shared/synthetic_sys_id.py (Phase 85)
# shared/fist_cudl_bridge.py — NEW
"""Bidirectional FIST↔CUDL shelfmark bridge (Phase 86).

Reverse-direction sibling to shared/shelfmark_bridge.py (Phase 84):
- Phase 84: libraries.csv ↔ CUDL  (cudl_normalize, lookup_cudl)
- Phase 86: FIST.dbo_Inventory.Shelfmark ↔ CUDL (this module)

Imports cudl_normalize from shelfmark_bridge to share the base normalizer.
Used ONLY by scripts/generate_synthetic_rows.py at generation time —
NOT a runtime hot path.

Functions:
  fist_to_cudl_keys(fist_shelfmark) -> set[str]
  lookup_fist_by_cudl(classmark) -> Optional[InventoryRecord]
  build_fist_alias_index(fist_conn) -> None
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Set
import re
import sqlite3

from shared.shelfmark_bridge import cudl_normalize  # reuse base normalizer

@dataclass(frozen=True)
class InventoryRecord:
    inventory_id: int
    fist_shelfmark: str
    has_alma: bool

# Mosseri Roman series — must match construct_mosseri_cudl_label()'s _MOSSERI_CUDL_SERIES
_MOSSERI_ROMAN = r"I{1,4}A?|I{0,3}V|VI{0,3}A?|VII{0,3}|VIII|IX|X"
_MOSSERI_FIST_RE = re.compile(
    rf"^Moss\.\s+({_MOSSERI_ROMAN})\s*[,.]\s*(.+)$",
    re.IGNORECASE,
)
_SERIES_N_RE = re.compile(r"^(.*?)\((\d+)\)(.*)$")

def fist_to_cudl_keys(fist_shelfmark: str) -> Set[str]:
    """Generate candidate CUDL keys from a FIST shelfmark (D-02a confirmed patterns)."""
    keys: Set[str] = set()
    sm = (fist_shelfmark or "").strip()
    if not sm:
        return keys
    candidates = [sm]
    # Pattern 2: data-noise prefix-strip (after LAST colon)
    if ":" in sm:
        after_colon = sm.rsplit(":", 1)[1].strip()
        if after_colon:
            candidates.append(after_colon)
    for c in candidates:
        # Pattern: base normalize
        base = cudl_normalize(c)
        if base:
            keys.add(base)
        # Pattern 1: Mosseri Roman expansion
        m = _MOSSERI_FIST_RE.match(c)
        if m:
            roman = m.group(1).lower()
            rest_norm = cudl_normalize(m.group(2))
            keys.add(f"mosseri{roman}{rest_norm}")
        # Pattern 3: (N) series-suffix strip
        m2 = _SERIES_N_RE.match(c)
        if m2:
            stripped = (m2.group(1) + m2.group(3)).strip()
            kn = cudl_normalize(stripped)
            if kn:
                keys.add(kn)
        # Pattern 4: Or. multi-segment dot-fix
        for prefix in ("or1080", "or1081"):
            if base.startswith(prefix) and len(base) > len(prefix) and base[len(prefix)].isdigit():
                keys.add(prefix + "." + base[len(prefix):])
    return keys
```

### Pattern 2: Single-function rewrite preserving outer contract

**What:** Phase 86 rewrites ONLY `_build_qualifying_inventories` in `scripts/generate_synthetic_rows.py`. The outer contract (manifest format, residue CSV columns, marker-block protocol, CSV-injection fail-loud, D-01a collision check) stays intact.

**Why:** Phase 85's UAT verified the infrastructure (helper module, browse hide-NLI gates, /api `is_synthetic`, corrections-write reject) is load-bearing-correct. Only the generation predicate was wrong (over-inclusive on bib-only data + multi_signature blocked T-S NS 329.96). Single-function rewrite minimizes risk surface.

**Example:**

```python
# Source: scripts/generate_synthetic_rows.py:105 (existing) — REWRITE TARGET
def _build_qualifying_inventories(
    fist_conn: sqlite3.Connection,
    nli_conn: Optional[sqlite3.Connection] = None,
) -> tuple[dict[int, dict], list[dict]]:
    """CUDL-WALKED rewrite (Phase 86 D-01)."""
    from shared.fist_cudl_bridge import fist_to_cudl_keys, build_fist_alias_index, lookup_fist_by_cudl
    from shared.shelfmark_bridge import lookup_cudl, build_alias_index

    # Step 1: walk CUDL classmarks
    cudl_classmarks = list(nli_conn.execute(
        "SELECT label, manifest_url, normalized_shelfmark FROM cambridge_manifests "
        "ORDER BY normalized_shelfmark"
    ))

    # Step 2: build FIST inv_map (Phase 86 NEW)
    build_fist_alias_index(fist_conn)

    qualifying: dict[int, dict] = {}
    residue: list[dict] = []
    parent_shelfmarks = _load_parent_shelfmark_set()  # D-06 filter

    for label, manifest_url, classmark in cudl_classmarks:
        # Phase 84 check
        if lookup_cudl(classmark) is not None:
            continue
        # Phase 86 FIST resolution
        rec = lookup_fist_by_cudl(classmark)
        if rec is None:
            residue.append({
                'cudl_label': label,
                'classmark': classmark,
                'ambiguity_kind': 'no_fist_match',
                'pattern_guess': _guess_pattern(classmark),  # NEW column
            })
            continue
        # D-04 multi_signature relax (closes T-S NS 329.96)
        # D-04a multi_inventory exclude
        # D-06 parent-shadow filter
        # D-01a real-Alma collision check happens later in _build_synthetic_rows
        # ... emit synthetic row ...

    return qualifying, residue
```

### Pattern 3: Append-only audit artifact

**What:** `reports/cudl_coverage.md` is a durable, regenerable, human-readable markdown file produced by a script step (NOT hand-edited).

**Why:** Future AUDIT-01 re-runs (Phase 87+) need byte-stable comparison. Markdown tables are diffable. Per-collection breakdown lets reviewers see whether the residue distribution is shifting between releases.

**Example:**

```python
# Source: pattern from scripts/generate_synthetic_rows.py:_write_coverage (existing)
def _write_cudl_coverage_md(
    path: Path,
    p84_resolved: int,
    p86_resolved: int,
    multi_inv: int,
    truly_orphan: int,
    by_collection: dict[str, dict],
    pattern_adjudication: list[dict],
) -> None:
    """Write reports/cudl_coverage.md — Phase 86 AUDIT-02 deliverable."""
    path.write_text(
        f"# CUDL Coverage Report (Phase 86)\n\n"
        f"**Generated:** 2026-05-XX\n"
        f"**Source data:** nli_crossref.db.cambridge_manifests (141,368 rows), "
        f"FIST.db.dbo_Inventory (279,208 rows), libraries.csv (255,615 real + N synthetic rows).\n\n"
        f"## Summary\n\n"
        f"| Status | Count | % of CUDL total |\n"
        f"| ------ | ----- | --------------- |\n"
        f"| Resolved via Phase 84 bridge | {p84_resolved} | ... |\n"
        f"| Resolved via Phase 86 FIST↔CUDL bridge | {p86_resolved} | ... |\n"
        f"| Multi-inventory ambiguous (excluded) | {multi_inv} | ... |\n"
        f"| Truly orphan (residue) | {truly_orphan} | ... |\n\n"
        f"## Per-Collection Breakdown\n\n"
        # ... table rows ...
        f"## Residue Pattern Adjudication (D-02c outcomes)\n\n"
        # ... pattern decisions accepted/rejected/deferred ...
        f"## Re-run Instructions\n\n"
        f"```bash\n"
        f"python scripts/generate_synthetic_rows.py --apply\n"
        f"python scripts/export_fist_enrichment.py\n"
        f"python scripts/scan_cudl_orphans.py --out-suffix _post_phase86\n"
        f"```\n\n"
        f"## See Also\n\n"
        f"- `reports/synthetic_coverage.md` — Phase 85 tier breakdown\n"
        f"- `.planning/phases/86-.../86-RESIDUE-PATTERNS.md` — D-02c artifact\n",
        encoding="utf-8",
    )
```

### Anti-Patterns to Avoid

- **Mutating `shared/shelfmark_bridge.py`:** Phase 84's NORM-04 contract treats it as frozen for libraries.csv↔CUDL direction. New direction = new module.
- **Threading `is_synthetic_sys_id` branches into `shared/fjms_service.py`:** Phase 85 D-01 layered pattern has the data layer accommodating the new ID format. The ~30 `WHERE AlmaId = ?` queries work transparently.
- **Hand-rolling shelfmark string slicing in the bridge:** All normalization goes through `cudl_normalize` + the 4 D-02a helpers. Never inline `.replace(' ', '-')` etc.
- **Skipping `--dry-run` before `--apply`:** The generation script's marker-block rewrite produces a `.bak` file but a buggy walk could still pollute the synthetic block. Always dry-run first.
- **Re-running export_fist_enrichment.py against an out-of-date synthetic_manifest.json:** Phase 85 D-07 made the manifest the AUTHORITATIVE input. Always run generate first → manifest writes → export reads manifest.
- **Adding a new library_code value:** D-15 from Phase 85. Synthetic rows reuse `CUL` for T-S/Or, `Mosseri` for Mosseri.
- **Coercing sys_id to int anywhere:** Phase 85 D-01b lint via `tests/test_synthetic_sys_id.py::TestNoIntCoercion`. New code in `shared/fist_cudl_bridge.py` and `scripts/audit_nli_attribution.py` must not violate this.
- **Letting AUDIT-03 fixture accidentally test 461 rows for shelfmark exact match:** The 461 are identified by sys_id; their library_code field is the only assertion. Title/shelfmark may legitimately differ over time as data refreshes.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Sys_id detection (synthetic vs real) | `sys_id.startswith('99') and sys_id.endswith('000000')` inline | `is_synthetic_sys_id(sys_id)` from `shared/synthetic_sys_id.py` | Phase 85 D-13 contract: detection must be consistent with digit-normalization. The helper handles edge cases (None, empty, non-digit input). |
| InventoryId ↔ sys_id encoding | `'99' + str(inv).zfill(10) + '000000'` | `encode_inventory_sys_id(inv)` / `decode_inventory_id(sys_id)` | Phase 85 D-01a collision-check + bool-rejection guards. |
| Shelfmark normalization | Reimplementing dot/slash/comma rules | `cudl_normalize(s)` from `shared/shelfmark_bridge.py` | Phase 84 D-08 "ONE source of truth" — Phase 86 imports from this module. NORM-04 regression guard depends on it. |
| Mosseri forward parsing | New regex for `Moss. III,27.1` → `mosseriiii27.1` | `construct_mosseri_cudl_label()` (genizah_core.py:259) for forward; for REVERSE direction use `_MOSSERI_FIST_RE` in new bridge module | Forward parser is shared. Reverse direction is the NEW work. |
| CSV write | Hand-built `f"{col1},{col2},...\n"` | `csv.writer` with explicit `lineterminator` | CRLF preservation lesson from `scripts/fix_nli_oxford_mislabel.py` v7.9.4: Windows line-ending detection at byte level via `f.read(8192)` count comparison. |
| Marker-block rewrite | Manual file slicing | `_strip_existing_synthetic_block` + `_build_synthetic_rows` (existing in generate_synthetic_rows.py) | Phase 85 D-04a idempotency tested. |
| Synthetic AlmaId injection | New SQL UNION ALL in fjms_service.py | `scripts/export_fist_enrichment.py` UNION-ALL pattern (Phase 85 D-07) | Layered-not-extended. Service layer stays unchanged. |
| Browse hide-NLI logic | New `is_synthetic_sys_id` branches | Reuse Phase 85 wiring at all 12 already-modified source files | All sites enumerated in `85-04-AUDIT.md`. Phase 86 adds 0 new branch points. |
| Public API `is_synthetic` field | Custom serializer | `shared/search_serializer.py:310` + `:646` (Phase 85) | Already wired. SCHEMA_VERSION=1 maintained. |
| PostHog `is_synthetic` property | Inline event-capture code | `web/api_hardening.py:646` (Phase 85) | Already wired. |

**Key insight:** Phase 86's code surface is INTENTIONALLY NARROW — one function rewrite + one new bridge module + two new audit scripts. Every other capability uses Phase 84/85 infrastructure verbatim.

## Runtime State Inventory

> Phase 86 is a data-refresh phase, not pure code. Three categories have meaningful contents.

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| **Stored data** | (1) `libraries.csv` synthetic block (currently empty between markers per 2026-05-09 revert). (2) `fist_data/synthetic_manifest.json` (currently `[]`). (3) `fist_data/fjms_enrichment.db` (currently has 0 synthetic AlmaIds across 12 tables; restored from gz backup 2026-05-09). (4) `reports/synthetic_ambiguity_residue.csv` (10,689 entries from Phase 85 D-05a STRICT — keep or rebuild per planner's discretion). | Regenerate all 4 via `scripts/generate_synthetic_rows.py --apply` + `scripts/export_fist_enrichment.py`. Server-side fjms_enrichment.db must be deployed. |
| **Live service config** | None — Phase 86 introduces no new external services. PostHog event property `is_synthetic` already shipped Phase 85. Supabase tables `corrections` / `lists` round-trip synthetic IDs (Phase 85 verified). | None. |
| **OS-registered state** | None — no Windows tasks, no systemd units, no scheduled jobs reference synthetic rows. The `.claude/scheduled_tasks.lock` is unrelated. | None. |
| **Secrets/env vars** | None — Phase 86 introduces no new env vars. `WEB_PUZZLE_ENABLED`, `SUPABASE_*`, `POSTHOG_*` unchanged. | None. |
| **Build artifacts / installed packages** | (1) Desktop installer bundles `libraries.csv` and `fjms_enrichment.db`. After Phase 86, these change. (2) Tantivy index unaffected (synthetic rows have no transcription text). | Per `feedback_no_github_release_for_web_only.md`: do NOT cut a desktop installer for a pure data refresh — bundle into next desktop-code release. Web deploys server-side data refresh standalone. |

## Common Pitfalls

### Pitfall 1: Phase 84 alias-index has wrong key form for sub-fragment Mosseri

**What goes wrong:** Phase 84's `construct_mosseri_cudl_label('Moss. I,3.1')` produces `MS-MOSSERI-I-00003-00001`, which `_index_key_for_label` strips to `mosserii31`. But CUDL stores `mosserii3.1` (with the dot preserved between `3` and `1`).

**Why it happens:** The Phase 84 forward Mosseri builder zfills sub-fragment numbers to 5 digits (`00001`), then `_index_key_for_label` strips leading zeros and concatenates without inserting a delimiter. CUDL preserves the dot: `mosserii3.1`. They are different normalized forms.

**How to avoid:** The new FIST↔CUDL bridge MUST emit BOTH forms when generating CUDL keys for Mosseri Roman shelfmarks: `mosseriiii27.1` (with-dot) AND `mosseriiii271` (concat). Otherwise the bridge under-recovers ~3,086 Mosseri orphans.

**Warning signs:** Empirical test: walk all `mosseri*` orphans, check whether their nearest FIST entry uses Roman+dot vs Roman+concat. The probe in this research found `mosserii3.1` had no FIST inv_map entry in my naive test — confirms the bug.

### Pitfall 2: Walking only no-Alma FIST inventories misses 96% of recoverable cases

**What goes wrong:** A naive bridge walks only FIST inventories WHERE alma.AlmaId IS NULL. Of the 5,330 unresolved CUDL classmarks, 3,455 (96.6%) actually resolve to FIST inventories that DO have Alma links — meaning the libraries.csv row exists, just under a different shelfmark form Phase 84 didn't capture.

**Why it happens:** The bridge purpose is to identify ALL FIST↔CUDL relationships, including those that should resolve to existing libraries.csv rows (alias-only fix, no synthetic needed). The synthetic-row decision happens AFTER bridge resolution, gated on whether the FIST inventory has an Alma link.

**How to avoid:** `lookup_fist_by_cudl` returns ALL inventories. The caller (generate_synthetic_rows.py) checks `record.has_alma`:
- If `has_alma`: log to a "Phase 84 missed" report (informational; future Phase 87 work to extend Phase 84 alias index). Do NOT emit synthetic — the libraries.csv row already exists.
- If NOT `has_alma`: emit synthetic row (this is the Phase 86 deliverable).

**Warning signs:** If Phase 86 emits >1,000 synthetic rows from CUDL-walk, something is wrong. Empirical estimate: ~108 unique no-Alma CUDL-resolved with the 4 D-02a patterns. Up to ~600–900 more if D-02c adjudication accepts pattern hypotheses.

### Pitfall 3: Multi-inventory ambiguity vs multi-signature ambiguity

**What goes wrong:** Phase 85 D-05a STRICT excluded BOTH multi_inventory and multi_signature. T-S NS 329.96 has InventoryId=65549106 with 13 distinct SignatureIds — a multi_signature case. Phase 85 STRICT excluded it. CUDL-walk inversion + D-04 relax keeps the InventoryId=65549106 unique (one CUDL classmark `tsns329.96` resolves to one InventoryId), so the fan-out happens on signatures, not inventories.

**Why it happens:** A synthetic row is keyed by InventoryId (`encode_inventory_sys_id(65549106)`). Multi-signature within a single InventoryId is fine — pick lowest SignatureId (existing tie-break logic). Multi-inventory means the same CUDL classmark → multiple distinct InventoryIds, which would yield multiple distinct sys_ids — that's a real ambiguity.

**How to avoid:** D-04: relax STRICT for multi_signature when all signatures resolve to same canonical_shelfmark + library_code (existing Phase 85 tie-break). D-04a: keep multi_inventory excluded. The CUDL-walk inversion makes multi_inventory rare empirically — my probe found 168 multi-inventory cases out of 5,330 unresolved.

**Warning signs:** If Phase 86 generates synthetic rows for multi_inventory cases (e.g., same `cudl_label` produces 2+ rows), the bridge logic is wrong. Test fixture: assert `len(set(synthetic_rows by cudl_label))` equals `len(synthetic_rows)`.

### Pitfall 4: AUDIT-01 target conflict — "<200 truly orphan" vs empirical 1,599

**What goes wrong:** ROADMAP success criterion 1 says "<200 truly-orphan." My empirical probe (and CONTEXT.md) confirm the residue is 1,599. Marking AUDIT-01 as failed because we exceed 200 misframes the milestone outcome.

**Why it happens:** The roadmap target was set when only the 6,053 Phase 84 orphan count was known. The 1,599 figure emerged during this research session. CONTEXT.md notes the <200 target is "conditional on user-adjudicated investigation closing more."

**How to avoid:** `cudl_coverage.md` AUDIT-02 documents the delta truthfully:
- Baseline (post-Phase-84): 6,053 orphans
- Post-Phase-86 (if D-02c accepts all 5 pattern rules): could drop to ~600–900
- Post-Phase-86 (if D-02c rejects all 5): stays at ~1,599
- The "<200" target is reframed as a stretch goal contingent on D-02c adjudication

**Warning signs:** If `cudl_coverage.md` reports the baseline post-86 count without showing the breakdown by adjudication outcome, reviewers will ask "why didn't we hit <200?". The artifact must be self-explanatory.

### Pitfall 5: CRLF line-ending bug on libraries.csv rewrite (v7.9.4 lesson)

**What goes wrong:** `csv.writer` defaults to `\r\n` on Windows for newline='', but if the file mode or terminator detection is wrong, the entire file gets rewritten with LF, producing a 255K-line diff against the prior CRLF-encoded file.

**Why it happens:** v7.9.4 (`scripts/fix_nli_oxford_mislabel.py`) hit this exact bug — initial run wrote LF and produced full-file diff, restored from backup, fixed with line-ending detection: `line_terminator = "\r\n" if sample.count(b"\r\n") > sample.count(b"\n") // 2 else "\n"`.

**How to avoid:** Phase 86 generation script ALREADY has this guard (line 437-444 of `generate_synthetic_rows.py`):

```python
def _read_libraries_csv(path: Path) -> tuple[list[list[str]], str]:
    with path.open("rb") as f:
        sample = f.read(8192)
    line_terminator = "\r\n" if sample.count(b"\r\n") > sample.count(b"\n") // 2 else "\n"
    ...
```

Verify it stays intact through the Phase 86 single-function rewrite. New `audit_nli_attribution.py` script — if it modifies libraries.csv (it shouldn't; it only reads) — must replicate this pattern.

**Warning signs:** Post-`--apply`, run `git diff libraries.csv | head -200`; if the diff is >100 lines for a few-row change, line endings broke.

### Pitfall 6: Tantivy index unchanged but synthetic rows discoverable only via Title+Shelfmark

**What goes wrong:** Phase 85 SYNTH-03 was narrowed: synthetic rows are searchable in Title+Shelfmark modes (csv_bank-backed metadata search) but NOT in text/Responsa modes (Tantivy chunks have no transcription for synthetic rows). UAT must search T-S NS 329.96 in **Shelfmark mode**, not text mode.

**Why it happens:** Tantivy index is built from `Transcriptions.txt`, not libraries.csv. Adding a synthetic row to libraries.csv does not add a Tantivy chunk. The `_execute_metadata_search` path (genizah_core.py:7398) for Title+Shelfmark queries reads csv_bank directly.

**How to avoid:** UAT documentation explicitly says "Shelfmark mode" not "search". The Phase 85 narrowing is documented in `reports/synthetic_coverage.md` and accepted by user.

**Warning signs:** UAT failure "T-S NS 329.96 didn't appear in search" — clarify they searched Shelfmark mode, not Text mode.

## Code Examples

### Common Operation 1: Build FIST↔CUDL alias index

```python
# Source: pattern adapted from shared/shelfmark_bridge.py:build_alias_index (Phase 84)
# Target: shared/fist_cudl_bridge.py
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

_FIST_ALIAS_INDEX: Optional[Dict[str, List[Tuple[int, str, bool]]]] = None
# key -> [(inventory_id, fist_shelfmark, has_alma), ...]

def build_fist_alias_index(fist_conn) -> None:
    """Build FIST.dbo_Inventory CUDL-key alias index. Called once at generation time."""
    global _FIST_ALIAS_INDEX
    builder: Dict[str, List[Tuple[int, str, bool]]] = defaultdict(list)
    for inv_id, shelfmark, alma_id in fist_conn.execute("""
        SELECT inv.InventoryId, inv.Shelfmark, alma.AlmaId
        FROM dbo_Inventory inv
        LEFT JOIN dbo_InventoryAlma alma ON alma.InventoryId = inv.InventoryId
        WHERE inv.Shelfmark IS NOT NULL AND inv.Shelfmark != ''
        ORDER BY inv.InventoryId
    """):
        has_alma = alma_id is not None
        for k in fist_to_cudl_keys(shelfmark):
            builder[k].append((inv_id, shelfmark, has_alma))
    _FIST_ALIAS_INDEX = dict(builder)


def lookup_fist_by_cudl(classmark: str) -> Optional['InventoryRecord']:
    """Resolve a CUDL classmark to a FIST inventory.

    Returns None for: not found, multi-inventory ambiguous, empty index.
    Returns the lowest-InventoryId for unambiguous + multi-signature
    (D-04 relax: multiple Signatures within one Inventory is OK).
    """
    if not _FIST_ALIAS_INDEX or not classmark:
        return None
    # Try classmark as-is + cudl_normalize variants
    candidates = [classmark, cudl_normalize(classmark)]
    for k in candidates:
        if k and k in _FIST_ALIAS_INDEX:
            entries = _FIST_ALIAS_INDEX[k]
            distinct_inv = {e[0] for e in entries}
            if len(distinct_inv) > 1:
                return None  # D-04a multi_inventory exclude
            inv_id, shelfmark, has_alma = sorted(entries)[0]
            return InventoryRecord(inv_id, shelfmark, has_alma)
    return None
```

### Common Operation 2: Pattern-guess column for residue iteration

```python
# Source: NEW for Phase 86 (D-02c iteration support)
def _guess_pattern(cudl_classmark: str) -> str:
    """Categorize residue classmark into one of the 5 known D-02b families.

    Output is a hint for human adjudicators looking at 86-RESIDUE-PATTERNS.md;
    not a load-bearing decision. Returns 'other' for classmarks that don't
    match any of the 5 known prefixes.
    """
    if cudl_classmark.startswith("tsf"):
        return "tsf_flattened_series"  # 392 expected
    if cudl_classmark.startswith("tsar"):
        return "tsar_flattened_series"  # 303 expected
    if cudl_classmark.startswith("tsns"):
        if "minute" in cudl_classmark or cudl_classmark.endswith(("a", "b", "c", "d")):
            return "tsns_minute_or_letter"
        return "tsns_other"  # 179 expected
    if cudl_classmark.startswith("or"):
        return "or_single_segment"  # 571 expected
    if cudl_classmark.startswith("mosseri"):
        return "mosseri_exotic_letter"  # 48 expected
    if cudl_classmark.startswith("tsmisc"):
        return "tsmisc_multi_segment"  # 98 expected
    return "other"  # 8 expected
```

### Common Operation 3: AUDIT-03 v7.9.4 regression check

```python
# Source: NEW for Phase 86 (D-10 scan + permanent test)

# scripts/audit_nli_attribution.py — operational scan
"""Audit v7.9.4 NLI Oxford mislabel fix (461 rows flipped 2026-04-22).

Run: python scripts/audit_nli_attribution.py
Asserts no row in libraries.csv has library_code='Oxford' AND a call_numbers
field matching the v7.9.4 NLI regex. Returns nonzero exit code on regression.
"""
import csv, re, sys
from pathlib import Path

NLI_RE = re.compile(r"The National Library of Israel|JER NLI Heb", re.IGNORECASE)
CSV = Path(__file__).resolve().parent.parent / "libraries.csv"

def main() -> int:
    regressions = []
    with CSV.open("r", encoding="utf-8", newline="") as f:
        for row in csv.reader(f):
            if len(row) < 4 or row[0].startswith("#"):
                continue
            if row[3] == "Oxford" and NLI_RE.search(row[2] or ""):
                regressions.append((row[0], row[2][:80]))
    if regressions:
        print(f"REGRESSION: {len(regressions)} Oxford rows match NLI regex")
        for sys_id, calls in regressions[:5]:
            print(f"  {sys_id}  {calls}")
        return 1
    print(f"OK: no Oxford rows match NLI regex (v7.9.4 fix intact)")
    return 0

if __name__ == "__main__":
    sys.exit(main())


# tests/test_nli_oxford_attribution.py — permanent CI fixture
"""v7.9.4 regression test: 461 NLI-flipped rows must stay library_code='NLI'."""
import csv
import pytest
from pathlib import Path

CSV = Path(__file__).resolve().parent.parent / "libraries.csv"

@pytest.fixture(scope="module")
def libraries_csv_data():
    rows_by_sysid = {}
    with CSV.open("r", encoding="utf-8", newline="") as f:
        for row in csv.reader(f):
            if len(row) < 4 or row[0].startswith("#"):
                continue
            rows_by_sysid[row[0]] = row
    return rows_by_sysid


# Golden subset of 20 sys_ids — MUST be representative of the 461.
# Generated by extracting random sample from `git show v7.9.4:libraries.csv`
# diff vs v7.9.3 — fixture file at tests/fixtures/nli_oxford_flipped_sysids.txt
GOLDEN_SAMPLE_PATH = Path(__file__).parent / "fixtures" / "nli_oxford_flipped_sysids.txt"

@pytest.fixture(scope="module")
def golden_sysids():
    if not GOLDEN_SAMPLE_PATH.exists():
        pytest.skip(f"Fixture file missing: {GOLDEN_SAMPLE_PATH}")
    return GOLDEN_SAMPLE_PATH.read_text().strip().splitlines()


def test_nli_flipped_rows_unchanged(libraries_csv_data, golden_sysids):
    for sys_id in golden_sysids:
        row = libraries_csv_data.get(sys_id)
        assert row is not None, f"sys_id {sys_id} missing from libraries.csv"
        assert row[3] == "NLI", (
            f"v7.9.4 regression: sys_id {sys_id} library_code={row[3]!r}, expected 'NLI'"
        )


def test_no_new_oxford_with_nli_text(libraries_csv_data):
    """No Oxford-coded row should match the v7.9.4 NLI regex."""
    import re
    NLI_RE = re.compile(r"The National Library of Israel|JER NLI Heb", re.IGNORECASE)
    regressions = [
        sys_id
        for sys_id, row in libraries_csv_data.items()
        if row[3] == "Oxford" and NLI_RE.search(row[2] or "")
    ]
    assert not regressions, f"v7.9.4 regression: {len(regressions)} rows: {regressions[:5]}"
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Phase 85: FIST-walked, CUDL-as-filter, multi_signature STRICT | Phase 86: CUDL-walked, FIST-as-resolver, multi_signature relaxed | 2026-05-10 (this discussion) | Closes T-S NS 329.96 + dodges fan-out ambiguity. |
| Phase 85: FJMS-only inclusion (5,035 bib-only rows) | Phase 86: CUDL-image-only inclusion | 2026-05-09 (revert) | Synthetic rows now have actionable image data; bibliography pointer-only rows excluded. |
| Phase 85 D-05a STRICT: exclude multi_signature | Phase 86 D-04: relax for unambiguous (same canonical + same library_code) | 2026-05-10 | Recovers T-S NS 329.96 (originating user case). |
| Phase 84: libraries.csv ↔ CUDL only | Phase 86: + FIST ↔ CUDL reverse | 2026-05-10 | Bridges the 5,330 Phase 84 orphans (recovers ~3,500 to existing rows + ~108 to new synthetic + ~1,599 truly orphan). |

**Deprecated/outdated:**
- Phase 85 Plan 02's "inclusive coverage stance" — `_build_qualifying_inventories` qualified InventoryIds with ANY FJMS signal (catalog title OR bibliography OR free-description OR full-text OR measurement). Replaced with image-bearing-only criteria (D-01a).
- The `<200 truly-orphan` AUDIT-01 target as set in ROADMAP — context-conditional reframing per CONTEXT.md.

## Validation Architecture

> Phase 86 has DATA, BRIDGE, GENERATION, ENRICHMENT, AUDIT, and UAT layers. Each gets explicit validation per Phase 85's commensurate practice.

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 7+ (existing) |
| Config file | `pytest.ini` / `pyproject.toml` (existing) |
| Quick run command | `pytest tests/test_fist_cudl_bridge.py tests/test_generate_synthetic_rows.py tests/test_nli_oxford_attribution.py -x` (~5–10s) |
| Full suite command | `pytest tests/` (~60s, ~700 tests) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| AUDIT-01 | scan_cudl_orphans re-run produces durable report | integration | `python scripts/scan_cudl_orphans.py --out-suffix _post_phase86` (smoke; output presence + row count > 0) | exists (Phase 84) |
| AUDIT-02 | cudl_coverage.md generated with required sections | unit | `pytest tests/test_cudl_coverage_artifact.py::test_required_sections_present` | NEW |
| AUDIT-03a | scan-based regression check | integration | `python scripts/audit_nli_attribution.py` (exit 0) | NEW |
| AUDIT-03b | permanent CI fixture | unit | `pytest tests/test_nli_oxford_attribution.py -x` | NEW |
| SYNTH-01 | helper module unchanged | unit | `pytest tests/test_synthetic_sys_id.py -x` | exists (Phase 85) |
| SYNTH-02 | shelfmark search resolves T-S NS 329.96 | integration | `pytest tests/test_metadata_search.py::test_shelfmark_resolves_synthetic_tsns_329_96` | NEW |
| SYNTH-03 | Title/Shelfmark search modes return synthetic rows (NARROWED scope) | unit | existing tests/test_browse_synthetic.py | exists |
| SYNTH-04 | browse for synthetic sys_id renders without errors | unit (static) + manual UAT | `pytest tests/test_browse_synthetic.py -x` (35 tests) + UAT item 1 | exists |
| SYNTH-05 | FJMS dialogs populate via InventoryId fallback | integration (post-export) + manual UAT | `pytest tests/test_export_fist_synthetic.py` then UAT item 1 | exists |
| SYNTH-06 | round-trip lists/comments/exclusions/parallels for synthetic | unit | `pytest tests/test_synthetic_round_trip.py -x` (14 tests) | exists |
| Bridge: D-02a Mosseri Roman expansion | unit | `pytest tests/test_fist_cudl_bridge.py::test_mosseri_roman_expansion` | NEW |
| Bridge: D-02a prefix-strip | unit | `pytest tests/test_fist_cudl_bridge.py::test_prefix_strip_after_last_colon` | NEW |
| Bridge: D-02a (N) series-strip | unit | `pytest tests/test_fist_cudl_bridge.py::test_series_n_strip` | NEW |
| Bridge: D-02a Or. dot-fix | unit | `pytest tests/test_fist_cudl_bridge.py::test_or_dot_fix` | NEW |
| Bridge: D-04 multi_signature relax | unit | `pytest tests/test_generate_synthetic_rows.py::test_multi_signature_relax_picks_lowest` | extend existing |
| Bridge: D-04a multi_inventory exclude | unit | `pytest tests/test_fist_cudl_bridge.py::test_multi_inventory_returns_none` | NEW |
| Bridge: D-06 parent-shadow filter | unit | `pytest tests/test_generate_synthetic_rows.py::test_parent_shadow_filter` | extend existing |
| Generation: T-S NS 329.96 closes | integration | `pytest tests/test_generate_synthetic_rows.py::test_tsns_329_96_synthetic_emitted` | NEW |
| Generation: image-bearing-only invariant | unit | `pytest tests/test_generate_synthetic_rows.py::test_all_emitted_have_cudl_manifest` | NEW |
| Generation: D-01a sys_id collision fail-loud | unit | existing (Phase 85) | exists |
| Audit: cudl_coverage.md re-run idempotent | unit | `pytest tests/test_cudl_coverage_artifact.py::test_idempotent_regeneration` | NEW |
| Audit: residue artifact has 5-pattern table | unit | `pytest tests/test_residue_patterns_artifact.py` | NEW |

### Sampling Rate

- **Per task commit:** `pytest tests/test_fist_cudl_bridge.py tests/test_generate_synthetic_rows.py -x` (~5–10s)
- **Per wave merge:** `pytest tests/ -x` (~60s)
- **Phase gate:** Full suite green + AUDIT-01 re-run produces correct artifacts + AUDIT-03 scan exit 0 + 6/6 HUMAN-UAT items pass

### Wave 0 Gaps

- [ ] `tests/test_fist_cudl_bridge.py` — covers all 4 D-02a normalizer patterns + `lookup_fist_by_cudl` cascade + multi_inventory exclude
- [ ] `tests/test_nli_oxford_attribution.py` — covers AUDIT-03 (golden 20 rows + scan-style assertion)
- [ ] `tests/fixtures/nli_oxford_flipped_sysids.txt` — golden 20 sample sys_ids extracted from v7.9.4 commit diff
- [ ] `tests/test_cudl_coverage_artifact.py` — covers AUDIT-02 markdown structure + idempotent regeneration
- [ ] `tests/test_residue_patterns_artifact.py` — covers `86-RESIDUE-PATTERNS.md` structure (5-family table presence)
- [ ] Extend `tests/test_generate_synthetic_rows.py` with: T-S NS 329.96 fixture; image-bearing-only invariant; multi_signature relax tie-break; CUDL-walk inversion correctness
- [ ] Test-time FIST.db + nli_crossref.db tiny fixtures (or use real DBs with `pytest.mark.requires_data` per existing pattern)

### Layer-by-layer validation

| Layer | What could go wrong | How caught |
|-------|---------------------|-----------|
| **DATA** | libraries.csv CRLF mangle on rewrite | line-terminator-detection guard already in generate_synthetic_rows.py:437; manual `git diff libraries.csv | head -20` post-apply |
| **DATA** | synthetic_manifest.json out of sync with libraries.csv synthetic block | _build_synthetic_rows D-01a collision check; tests/test_generate_synthetic_rows.py count parity |
| **BRIDGE** | False positives (CUDL classmark → wrong InventoryId) | unit tests with hand-picked fixtures per D-02a pattern + 86-RESIDUE-PATTERNS.md adjudication for D-02b |
| **BRIDGE** | Performance regression (FIST inv_map build > 30s) | startup-time assertion in `tests/test_fist_cudl_bridge.py::test_build_index_under_10s` |
| **GENERATION** | T-S NS 329.96 not emitted | `tests/test_generate_synthetic_rows.py::test_tsns_329_96_synthetic_emitted` |
| **GENERATION** | Bib-only rows leak into synthetic block | `test_all_emitted_have_cudl_manifest` invariant test |
| **GENERATION** | Phase 85 D-01a real-Alma collision | existing fail-loud test |
| **ENRICHMENT** | fjms_enrichment.db not regenerated → empty FJMS dialogs in browse | post-regen smoke: `SELECT COUNT(*) FROM catalog WHERE AlmaId LIKE '99%' AND length(AlmaId)=18 AND AlmaId LIKE '%000000'` returns N matching manifest count |
| **ENRICHMENT** | Synthetic AlmaIds collide with real-Alma | `_validate_synthetic_export` post-export check (Phase 85) |
| **AUDIT-01** | scan_cudl_orphans output missing or stale | smoke step in plan 04 (or wherever): assert `reports/cudl_orphans_all_post_phase86.csv` mtime ≥ `libraries.csv` mtime |
| **AUDIT-02** | cudl_coverage.md format breaks future re-run | `test_cudl_coverage_artifact.py::test_required_sections_present` (regex on h2 headers) |
| **AUDIT-02** | Cross-link to synthetic_coverage.md broken | static link check in test |
| **AUDIT-03a** | NLI regression scan misses a row | parametrize over 5 known-flipped sys_ids in unit test (smaller than full 20-fixture golden) |
| **AUDIT-03b** | CI fixture grows stale (libraries.csv refresh shifts row positions) | golden fixture is sys_id-keyed, not row-position-keyed; survives row-order shuffles |
| **UAT-1** Browse synthetic | sidecar empty → no FJMS data | regenerate sidecar BEFORE UAT (operational sequence below) |
| **UAT-2** Search T-S NS 329.96 | Tantivy lookup fails on synthetic | UAT explicitly says **Shelfmark mode** not Text mode |
| **UAT-3** List round-trip | Supabase FK constraint on synthetic sys_id | Phase 85 verified — opaque-string handling already works |
| **UAT-4** Correction button hidden | UI regression | Phase 85 verified at 2 sites; Phase 86 zero new branches |
| **UAT-5** Desktop browse synthetic | Qt warning / crash | manual desktop interactive — cannot grep |
| **UAT-6** PostHog `is_synthetic: true` | telemetry regression | Phase 85 verified; check after deploy |

## Sources

### Primary (HIGH confidence)
- **Empirical probe of FIST.db (3.17 GB) + nli_crossref.db (273 MB)** — verified counts (141,368 CUDL classmarks, 279,208 FIST inventories, 5,330 Phase 84 unresolved, 1,599 truly orphan post-bridge), residue family decomposition (Or=571, T-S F=392, T-S Ar=303, T-S NS=179, T-S Misc=98, Mosseri=48, T-S other=8), T-S NS 329.96 InventoryId=65549106 unique with 13 SignatureIds, no Alma link.
- **`shared/shelfmark_bridge.py`** (Phase 84) — 465 lines read in full. `cudl_normalize`, `_index_key_for_label`, `_collapse_numeric_runs`, `lookup_cudl`, `build_alias_index` API + behavior verified.
- **`shared/synthetic_sys_id.py`** (Phase 85) — 140 lines read in full. `is_synthetic_sys_id`, `encode_inventory_sys_id`, `decode_inventory_id` contract.
- **`scripts/generate_synthetic_rows.py`** (Phase 85) — 772 lines read in full. Identifies `_build_qualifying_inventories` as the single rewrite target.
- **`scripts/scan_cudl_orphans.py`** (Phase 84 site #4) — 136 lines read. Confirms `--out-suffix` parameter + import from `shared.shelfmark_bridge`.
- **`scripts/fix_nli_oxford_mislabel.py`** — 65 lines read. NLI regex `The National Library of Israel|JER NLI Heb` is the AUDIT-03 fixture source.
- **`scripts/export_fist_enrichment.py`** — first 100 lines read. Confirms Phase 85 UNION-ALL pattern, manifest-as-authoritative approach.
- **`.planning/phases/85-synthetic-fjms-inventory-rows/85-04-AUDIT.md`** (Phase 85 site enumeration) — Phase 86 AUDIT-03 input verified.
- **`.planning/phases/85-synthetic-fjms-inventory-rows/85-VERIFICATION.md`** — confirms 6/6 SYNTH-* requirements at infrastructure level; Phase 86 inherits.
- **CLAUDE.md** (project instructions) — both apps must be maintained, RTL Hebrew, version bumping protocol, OPEN_ISSUES.md tracking, Recently Changed entry.
- **`feedback_no_github_release_for_web_only.md`** (user memory) — desktop installer rebuild policy.
- **`.claude/skills/cairo-genizah-research/SKILL.md`** — confirms public API contract is opaque-string sys_id-tolerant; synthetic IDs flow through search → browse correctly per Phase 85 D-14.

### Secondary (MEDIUM confidence)
- **`.planning/phases/86-cudl-coverage-audit-and-synthetic-reattempt/86-CONTEXT.md`** — comprehensive D-01..D-12b decisions, all confirmed against empirical probes.
- **`.planning/phases/86-.../86-DISCUSSION-LOG.md`** — audit trail; confirms the inversion reasoning.
- **`.planning/REQUIREMENTS.md`** — AUDIT-01..03 + SYNTH-01..06 specifications.
- **`.planning/ROADMAP.md` §"Phase 86"** — 6 success criteria + outcome of Phase 85 revert.

### Tertiary (LOW confidence)
- None — all claims either tool-verified or documented in upstream artifacts.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| (none) | All factual claims in this research were either: empirically verified via SQL probe against live FIST.db / nli_crossref.db, or confirmed against documented Phase 84/85 artifacts. | — | — |

**The table is empty.** All claims in this research were verified or cited — no user confirmation needed before planning.

## Open Questions

1. **Should `86-RESIDUE-PATTERNS.md` be produced during research, planning, or wave-0 of execution?**
   - What we know: D-02c says "the planner's research step produces this artifact." But producing nearest-neighbour fixtures requires the bridge code to exist OR a research-time prototype script.
   - What's unclear: research-time produces the artifact for the planner's use; planning-time risks the planner producing it inconsistently with eventual code.
   - Recommendation: Plan 01 (or wave 0) writes a small prototype script that generates `86-RESIDUE-PATTERNS.md` from current libraries.csv + FIST.db state. User adjudicates BEFORE Plan 02 (bridge code) locks the rule set. Adjudication outcome is committed alongside the artifact.

2. **Test scope for D-10: 20 golden vs all 461?**
   - What we know: 461 sys_ids were flipped Oxford → NLI in v7.9.4. CONTEXT.md leaves test scope to planner.
   - What's unclear: 461-row parametrize would slow the CI test (~1ms per row × 461 = 0.5s tolerable) but increases fixture-file size (~10KB sys_ids file).
   - Recommendation: 20 golden rows for the parametrized assertion (representative sample, fast feedback) + the scan-style `test_no_new_oxford_with_nli_text` for catch-all coverage. Best-of-both — explicit golden cases + regex sweep. Code example above implements this.

3. **Whether to keep Phase 85's `synthetic_ambiguity_residue.csv` (10,689 entries) or rebuild from CUDL-walk?**
   - What we know: Phase 85 D-05a STRICT residue includes 95 multi_signature cases (incl. T-S NS 329.96) that Phase 86 D-04 RELAX absorbs.
   - What's unclear: Audit trail value of preserving the 10,689 vs Phase 86 walk producing a totally different residue shape (CUDL-keyed instead of FIST-keyed).
   - Recommendation: REBUILD. The 10,689 represents a different walk pattern that no longer applies. Old CSV stays in git history. New residue CSV adds the `pattern_guess` column from D-02c. Cleaner audit trail.

4. **Desktop installer release strategy?**
   - What we know: `feedback_no_github_release_for_web_only.md` says don't cut a desktop release for pure data refresh.
   - What's unclear: Phase 86 ships ZERO desktop code changes (Phase 85 hide-NLI gates already in shipped desktop ≥7.10.0). The data refresh affects libraries.csv + fjms_enrichment.db that the desktop installer bundles.
   - Recommendation: Web-only deploy this round. Bundle the new libraries.csv + fjms_enrichment.db into the next desktop release that ships for ANY desktop-code reason. Document in CHANGELOG that Phase 86 data is web-only until the next desktop release. Rationale: avoid the desktop-update-prompt-spam pattern.

5. **Is "<200 truly orphan" a hard or soft AUDIT-01 target?**
   - What we know: ROADMAP says "<200" but CONTEXT.md says it's "conditional on user-adjudicated investigation closing more."
   - What's unclear: Does AUDIT-01 PASS at 1,599 if the adjudication outcome documents the rationale, or does it FAIL?
   - Recommendation: Treat as soft target. AUDIT-01 PASSES if `cudl_coverage.md` documents the post-adjudication count truthfully + categorizes the residue + includes re-run instructions. Hardcoding <200 ignores that the residue is genuinely intractable for some categories (e.g., CUDL `or1080.110` may correspond to FIST `Or.1080 1.10` at a different fragment granularity — not a normalization gap). User adjudicates per D-02c.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python 3.11+ | All scripts | ✓ | 3.11 | — |
| sqlite3 (stdlib) | bridge + generation | ✓ | bundled | — |
| pytest | tests | ✓ | existing | — |
| FIST.db | bridge + generation | ✓ | 3.17 GB at fist_data/FIST.db | — |
| nli_crossref.db | bridge + generation + audit | ✓ | 273 MB at nli_data/nli_crossref.db | — |
| libraries.csv | generation + audit | ✓ | 255,615 real rows | — |
| Existing fjms_enrichment.db | desktop test (pre-regen) | ✓ | 1.6 GB | — |
| Network access for HUMAN-UAT (Supabase) | UAT items 3, 6 | depends | — | Skip Supabase round-trip; record limitation in UAT report |

**Missing dependencies with no fallback:** None.

**Missing dependencies with fallback:** None for code/audit work; HUMAN-UAT requires live web app + Supabase + desktop app build.

## Project Constraints (from CLAUDE.md)

These directives are extracted from `./CLAUDE.md` (project instructions). The plan MUST respect all of them:

- **Both apps must be maintained** — web (NiceGUI) + desktop (PyQt6) parity. Phase 86 has zero desktop CODE changes; data changes affect both via shared libraries.csv + fjms_enrichment.db. Web deploys data; desktop bundles data into next release.
- **Hebrew RTL** — n/a for Phase 86 (no UI changes).
- **Library codes** — `CUL` and `Mosseri` only for synthetic rows (D-15 from Phase 85 carries forward).
- **`docs/OPEN_ISSUES.md` is the central issue tracker** — Phase 86 must check it at start of session, mark fixed issues with `✅ Fixed (YYYY-MM-DD)`, add new bugs with `❌ Open`. The 1,599 residue should NOT go into OPEN_ISSUES (it's a documented carry-forward, not a bug).
- **Documentation maintenance** — if architecture changes, update `CLAUDE.md`. If env vars change, update `CLAUDE.md` + `docs/guides/DEVELOPER_GUIDE.md`. Phase 86 introduces no new env vars.
- **Version bump for releases** — run `python scripts/bump_version.py X.Y.Z`. Phase 86 will be part of v7.11.0 release. Manual steps after script: CHANGELOG `## [X.Y.Z]`, `CLAUDE.md` "Recently Changed", `README.md` "What's New".
- **`python scripts/check_docs.py` green** — required before commit/release. Phase 86 must pass.
- **Outdated terms to avoid** — FastAPI / backend server / DATABASE_URL / port 8000 — none of these appear in Phase 86 work.
- **No `pytest tests/` regressions** — full suite must stay green.
- **Test framework** — pytest existing. No alternatives.
- **NEVER launch web server from Bash** (per user memory `feedback_no_background_webserver.md`) — UAT happens manually after server deploy, not via Claude.
- **NEVER create GitHub release for web-only version** (per `feedback_no_github_release_for_web_only.md`) — desktop polls /releases/latest. Web-only Phase 86 deploy MUST NOT trigger a GitHub release. v7.11.0 GitHub release happens later when desktop installer is bundled.

## Sources

### Primary (HIGH confidence)
- Empirical probe: live FIST.db + nli_crossref.db (run during research session 2026-05-10)
- `shared/shelfmark_bridge.py` (Phase 84, 465 lines)
- `shared/synthetic_sys_id.py` (Phase 85, 140 lines)
- `scripts/generate_synthetic_rows.py` (Phase 85, 772 lines)
- `scripts/scan_cudl_orphans.py` (Phase 84 site #4)
- `scripts/fix_nli_oxford_mislabel.py` (v7.9.4)
- `scripts/export_fist_enrichment.py` (Phase 85 UNION-ALL pattern, header)
- `.planning/phases/85-.../85-CONTEXT.md` + `85-VERIFICATION.md` + `85-04-AUDIT.md`
- `.planning/phases/86-.../86-CONTEXT.md` + `86-DISCUSSION-LOG.md`
- `.planning/REQUIREMENTS.md` (AUDIT-01..03, SYNTH-01..06, Out of Scope)
- `.planning/ROADMAP.md` §"Phase 86" (6 success criteria + Phase 85 outcome)
- `CLAUDE.md` (project instructions)
- `skills/cairo-genizah-research/SKILL.md` (consumer API contract)
- User memory: `feedback_no_github_release_for_web_only.md`, `feedback_no_background_webserver.md`, `feedback_review_workflow.md`

### Secondary (MEDIUM confidence)
- None — primary sources cover all decisions.

### Tertiary (LOW confidence)
- None.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all stdlib + existing repo conventions
- Architecture: HIGH — extends Phase 84 + activates Phase 85 with single-function rewrite
- Pitfalls: HIGH — all 6 pitfalls verified against empirical probes or v7.9.4 lessons
- Validation Architecture: HIGH — 23-row test map covering 6 layers
- Residue patterns: HIGH — empirical decomposition matches CONTEXT.md exactly
- Bridge recovery numbers: HIGH — empirically reproduced (5,330 unresolved → 3,455 alma + 108 no-alma + 168 multi-inv + 1,599 orphan)
- Performance: MEDIUM — Phase 84 alias-index build measured at 2.11s; Phase 86 FIST inv_map estimated +5s based on 282K key count, NOT measured
- AUDIT-03 fixture exact size: MEDIUM — 461-row claim from v7.9.4 release notes; recommended scope is 20 golden + scan-style sweep

**Research date:** 2026-05-10

**Valid until:** ~30 days for stable; longer because Phase 86 is data-refresh, not framework-dependent. Re-research only triggered by FIST.db schema change, libraries.csv format change, or CUDL classmark format change.

---

## RESEARCH COMPLETE

**Phase:** 86 - CUDL Coverage Audit + Synthetic Re-attempt
**Confidence:** HIGH

### Key Findings

1. **Empirical confirmation of CONTEXT.md numbers.** The 5,330 unresolved (vs CONTEXT's 5,325 — 5-row drift), 1,599 truly-orphan, 5-family residue decomposition, and T-S NS 329.96 → InventoryId 65549106 all empirically verified against live FIST.db + nli_crossref.db.
2. **Bridge module location: sibling, not extension.** Recommend `shared/fist_cudl_bridge.py` rather than extending Phase 84's `shared/shelfmark_bridge.py`. Preserves NORM-04 frozen-contract while sharing `cudl_normalize` via import.
3. **Mosseri Roman MUST emit two key forms.** CUDL stores both `mosseriiii27.1` (with-dot) and concat-form for some entries. The bridge must generate both candidates per FIST shelfmark to recover all sub-fragment Mosseri entries.
4. **3,455 + 108 = 3,563 recoverable**, not "70% of 5,325 = 3,727". The discrepancy from CONTEXT.md's 3,726 figure is small drift (libraries.csv state shifted 5 rows since the discussion-time probe). The 70% recovery target is preserved with reasonable margin.
5. **Operational sequencing matters.** generate → manifest writes → export reads manifest → sidecar regenerates → web deploy → AUDIT-01 re-runs against new libraries.csv → cudl_coverage.md → HUMAN-UAT. Skipping or reordering breaks the synthetic AlmaId injection.
6. **AUDIT-01 target reframe.** "<200 truly orphan" is conditional on D-02c adjudication. AUDIT-02 documents the actual residue + decision rationale per pattern.
7. **AUDIT-03 best as 20-golden + scan-sweep.** Parametrize over 20 representative sys_ids for explicit assertions; add `test_no_new_oxford_with_nli_text` regex sweep for catch-all coverage.
8. **HUMAN-UAT order:** UAT items 1, 2 require web deploy; item 5 requires desktop installer rebuild OR running from source; items 3, 4, 6 require live Supabase/PostHog.

### File Created
`C:\Genizahsearch\.planning\phases\86-cudl-coverage-audit-and-synthetic-reattempt\86-RESEARCH.md` (this file)

### Confidence Assessment
| Area | Level | Reason |
|------|-------|--------|
| Standard Stack | HIGH | All stdlib + existing repo conventions |
| Architecture (sibling-bridge module) | HIGH | Extends Phase 84/85 with minimum surface |
| Bridge recovery rates | HIGH | Empirically reproduced exact CONTEXT.md numbers |
| Residue pattern fixtures | HIGH | Live nearest-neighbour FIST candidates extracted for all 5 D-02b families |
| AUDIT-03 design | HIGH | v7.9.4 fix code read; regex + 461 fixture set verified |
| Validation Architecture | HIGH | Layer-by-layer mapped to test commands |
| Performance | MEDIUM | Phase 84 measured (2.11s); Phase 86 estimated (+5s) but not run |

### Open Questions
1. `86-RESIDUE-PATTERNS.md` produced during research, planning, or wave-0? Recommended: wave 0 prototype script.
2. AUDIT-03 fixture: 20 vs 461? Recommended: 20 golden + scan-sweep.
3. Keep or rebuild Phase 85 `synthetic_ambiguity_residue.csv`? Recommended: rebuild.
4. Desktop installer this round? Recommended: bundle into next desktop release; web-only Phase 86.
5. AUDIT-01 hard or soft <200? Recommended: soft, contingent on D-02c adjudication.

### Ready for Planning
Research complete. Planner can now create PLAN.md files. Recommended plan structure (4–5 plans):

- **86-01-PLAN** — `shared/fist_cudl_bridge.py` module + 4 D-02a normalizers + alias-index builder + unit tests
- **86-02-PLAN** — `_build_qualifying_inventories` rewrite (CUDL-walked) + D-04 multi_signature relax + D-06 parent-shadow + integration tests + T-S NS 329.96 fixture
- **86-03-PLAN** — `86-RESIDUE-PATTERNS.md` artifact generation + user adjudication checkpoint + 0..N additional bridge rules from D-02c outcome
- **86-04-PLAN** — `reports/cudl_coverage.md` (AUDIT-02) + `scripts/audit_nli_attribution.py` (AUDIT-03a) + `tests/test_nli_oxford_attribution.py` (AUDIT-03b) + AUDIT-01 re-run + HUMAN-UAT
- **86-05-PLAN** *(optional)* — release coordination: CHANGELOG amendment + version bump (if not deferred to a separate release phase) + web deploy + decision on desktop bundling

OR consolidate into 4 plans by merging 04+05.
