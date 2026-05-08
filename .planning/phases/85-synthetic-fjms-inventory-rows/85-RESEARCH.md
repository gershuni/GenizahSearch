# Phase 85: Synthetic FJMS Inventory Rows - Research

**Researched:** 2026-05-08
**Domain:** Cross-system data integration — synthesise libraries.csv rows for FJMS-only inventories that have no NLI Alma record, with CUDL images when available, and round-trip them through search / browse / lists / corrections / parallels / public-API / PostHog.
**Confidence:** HIGH for the data layer + bridge wiring (Phase 84 precedent is solid, codebase verified). MEDIUM for the precise UI hide-list (estimated by grep, planner needs to enumerate during execution). MEDIUM for corrections-on-synthetic ship-now-vs-defer recommendation (depends on a Supabase question only the live DB can answer).

## User Constraints (from CONTEXT.md)

### Locked Decisions

**FJMS Lookup Architecture**
- **D-01:** Pre-populate AlmaId column + publish helpers (Option 4). At FJMS sidecar export time, write the synthetic sys_id directly INTO the `AlmaId` column for FJMS-only inventories. `shared/fjms_service.py` stays unchanged. Publish three helpers in shared code (e.g. `shared/synthetic_sys_id.py` or extend `shared/shelfmark_bridge.py`):
  - `is_synthetic_sys_id(s) -> bool`
  - `encode_inventory_sys_id(inventory_id: int) -> str`
  - `decode_inventory_id(sys_id: str) -> Optional[int]`
- **D-01a:** Export-time assertion that generated synthetic sys_ids do NOT collide with any real Alma-linked row in libraries.csv. Run as part of the regeneration script; fail-loud on collision.
- **D-01b:** Sys_ids stay as Python strings everywhere. No `int()` conversion at any call site.

**Synthetic-Row Scope**
- **D-02:** Generate a synthetic row for any classmark NOT already resolved by Phase 84's bridge that meets EITHER (a) has a CUDL manifest in `nli_crossref.db.cambridge_manifests`, OR (b) has substantive FJMS metadata in FIST.db — at minimum a catalog title, scholarly description, measurement record, OR bibliography entry.
- **D-03:** Plan-phase researcher must produce a coverage manifest breaking the synthetic-row population into three tiers: (1) CUDL+FJMS, (2) CUDL-only no-FJMS, (3) FJMS-only no-CUDL. Internal artifact (`reports/synthetic_coverage.md` or similar). NOT user-visible badging.

**Persistence**
- **D-04:** Synthetic rows append directly to `libraries.csv` via a regeneration script (e.g. `scripts/generate_synthetic_rows.py`). The script is the source-of-truth process; libraries.csv is the durable artifact. Re-runnable when inputs change. csv_bank loader treats them uniformly with real rows.
- **D-04a:** Regeneration must produce identical output on identical inputs. Planner picks mechanism — either a marked `# BEGIN SYNTHETIC` / `# END SYNTHETIC` block in libraries.csv that the script rewrites in place, or a separate manifest file (`fist_data/synthetic_manifest.json` or similar) that the script reads/writes. NO duplicate rows on rerun.

**Generation Source**
- **D-05:** Hybrid — cross-product of `nli_crossref.db.cambridge_manifests` × FIST.db (`dbo_Signature` for InventoryId resolution + linked tables for FJMS metadata harvest). NLI gap-file Excel excluded.
- **D-05a:** Match by normalized shelfmark key with ambiguity exclusion. If a CUDL classmark maps to multiple FIST signatures, exclude (don't fan out) and log to a residue file Phase 86 audit can pick up.

**Browse UX**
- **D-06:** Quiet degradation, no badge. Hide NLI-only UI elements when `is_synthetic_sys_id(sys_id)` is true: KTIV link, NLI source toggle option, NLI catalog references panel, NLI bibliography chips, NLI image source button, any `/api/nli_image_by_sysid` calls, any `/api/fl_ids` resolution attempts. No banner, no badge. Web + desktop parity.
- **D-07:** No visible source badge. Earlier "include with FJMS-only badge" answer was about whether to include the row, not about visual badging. D-06 stands.
- **D-08:** When `is_synthetic_sys_id(sys_id) AND has_cudl_manifest(sys_id)`: Cambridge IIIF is the default image source. `total_pages` driven by CUDL manifest canvas count. Browse next/prev navigates CUDL canvases. When no CUDL manifest exists, fall back to `total_pages=0` metadata-only behavior (Phase 53 precedent).

**Title & Shelfmark Shape**
- **D-09:** libraries.csv column 7 (`titles_non_placeholder`) precedence: FJMS TitleHeb → FJMS Title → FJMS GenizahTitle → shelfmark string.
- **D-12:** call_numbers shape — minimum is the FJMS canonical shelfmark form. If cheap, also include normalized variants. Planner picks variant generation strategy.

**Community Writes**
- **D-10:** Lists + comments allowed (sys_id-keyed, opaque-string handling). Corrections allowed in principle, deferrable if uid/p_num plumbing proves hard — plan-phase research must surface the complexity. Parallels: synthetic rows have empty Tantivy text → won't appear in composition-parallel results. Acceptable. Exclusions: sys_id-keyed; should round-trip transparently.

**Helper Contract**
- **D-13:** `is_synthetic_sys_id` must produce consistent detection regardless of whether input has been digit-normalized (`"".join(ch for ch in str(s) if ch.isdigit())`). Either (a) accept already-normalized input as canonical and document, OR (b) perform deterministic normalization itself.

**Cross-Sidecar Tolerance**
- **D-14:** Beyond fjms_enrichment.db, other Alma-keyed sidecars and external services should tolerate "no match": cambridge_manifests already joins via normalized_shelfmark (safe; verify); NLI Alma JSON callers must branch on `is_synthetic_sys_id()` BEFORE issuing the network call; PostHog adds `is_synthetic: true` event property; public `/api/browse` and `/api/search` should return synthetic rows cleanly.

**Library Code Attribution**
- **D-15:** Synthetic CUL rows reuse existing `library_code=CUL`. Synthetic Mosseri rows reuse `library_code=Mosseri`. No new codes.

### Claude's Discretion
- Internal organization of the helper module (`shared/synthetic_sys_id.py` standalone vs extending `shared/shelfmark_bridge.py`).
- Exact mechanism for D-04a idempotency (marker block in CSV vs separate manifest).
- Variant generation strategy for D-12 call_numbers (which subset of normalized forms to emit).
- Whether to add a `synthetic_manifest.json` audit file alongside the regenerated CSV block (recommended for diff/coverage tracking, but planner's call).

### Deferred Ideas (OUT OF SCOPE)
- NLI-publishes-real-Alma migration path (future phase, triggered by NLI publication event).
- Reverse audit (NLI Alma in libraries.csv but absent from CUDL/FJMS).
- Synthetic rows for non-CUL collections (AIU/Halper FJMS-only).
- Periodic NLI gap-file refresh.
- Tantivy incremental rebuild for synthetic-row updates.
- Mosseri "2nd series" patterns (`Ms. L 241`, `Ms. MOSS NS`).
- `is_synthetic: true` PostHog property surface — D-14 plan-time research item; may slip if complex.

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| SYNTH-01 | `is_synthetic_sys_id` helper + encode/decode for the 18-digit format | §Helper Module + §Synthetic ID Format Algebra; bridge module precedent (`shared/shelfmark_bridge.py:1-465`) |
| SYNTH-02 | User can search by FJMS-only shelfmark and get a synthetic-row result | §Search Path Trace; Phase 53 metadata-only precedent at `genizah_core.py:7284-7359` (`_execute_metadata_search`) is the unmodified target |
| SYNTH-03 | Tantivy index includes synthetic rows so all search modes return them (empty text OK) | §Tantivy Sidestep — no index rebuild needed; metadata modes route via `search_by_meta`+`_execute_metadata_search`, text/Responsa modes don't return them by design |
| SYNTH-04 | Browse page for synthetic sys_id shows CUDL images + FJMS catalogue/bib/measurements + clear UI signalling | §Browse Hide-NLI Audit + §CUDL Image Source Switching |
| SYNTH-05 | FJMS enrichment lookups resolve synthetic sys_ids via underlying InventoryId; web + desktop parity | §FJMS Pre-population Mechanics — D-01 means fjms_service.py needs zero changes |
| SYNTH-06 | Lists, exclusions, parallels, comments, corrections, external-link buttons all tolerate synthetic sys_ids | §Round-Trip Surface Audit (Lists/Comments OK, Parallels naturally absent, Corrections deferral recommendation) |

## Summary

Phase 85's mechanics are surprisingly clean BECAUSE Phase 84 already established the discipline: sys_ids are opaque strings, csv_bank is the source of truth, `library_code=CUL` is the universal taxonomy, and `_execute_metadata_search` already handles rows with empty Tantivy text (Phase 53 v7.1.0 precedent). The synthetic-row mechanism slots into this architecture mostly by writing data; the code surface area is small and concentrated.

**Three implementation kernels:**
1. **Data:** A regeneration script that walks FIST.db + cambridge_manifests, generates synthetic AlmaId-prefixed rows in `libraries.csv` AND injects synthetic AlmaId rows into `fjms_enrichment.db` at export time so all 11 AlmaId-keyed tables resolve via `WHERE AlmaId = ?` without service-layer changes.
2. **Helper module:** Three functions (`is_synthetic_sys_id`, `encode_inventory_sys_id`, `decode_inventory_id`) that ARE the architecture per D-01. Without them, hand-rolled string slicing will leak everywhere.
3. **UI hide-NLI branches:** ~12-15 call sites in `web/pages/browse.py` (verified: 7 KTIV/NLI references at lines 1708, 1973, 2430, 2898, 3442, 3568-3576, 3994-4032, 4266; plus active_source defaults at 606, 638, 682, 900, 3457-3556) and equivalent in desktop (`desktop/viewers.py:702-710` btn_ktiv + ~5 more in `genizah_app.py` per grep). Each must branch on `is_synthetic_sys_id()` to skip NLI elements and switch to CUDL-default semantics.

**Primary recommendation:** Plan in 5 plans — (1) helper module + format tests + golden fixtures; (2) regeneration script for libraries.csv with idempotent `# BEGIN/END SYNTHETIC` block; (3) `export_fist_enrichment.py` modification to inject synthetic AlmaId rows into all 11 tables via UNION-with-LEFT-JOIN; (4) UI hide-NLI branches (web + desktop parity, single PR with explicit call-site checklist); (5) public-API `is_synthetic` field + PostHog property + corrections-tolerance verification + scan-diff verification re-run. Recommend **defer corrections-write on synthetic rows to a Phase 87 small follow-up** — the Supabase `corrections` table accepts arbitrary `sys_id` strings (no FK constraint detected on `sys_id`, only on `author_id` to auth.users), but corrections need a `page_number` and the no-CUDL-no-FJMS subset has no images, no canvas count, and no clear page semantics. Read-side corrections viewing is already safe.

**Confidence sources:** Phase 84's bridge module is the architectural template (HIGH). The csv_bank loader's `"".join(ch for ch in str(s) if ch.isdigit())` pattern (genizah_core.py:3374) is the exact normalization that D-13 must accommodate (HIGH). FIST.db query template (LEFT JOIN `dbo_InventoryAlma`) is the export-time mutation point identified directly from `scripts/export_fist_enrichment.py` (HIGH).

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Synthetic ID encode/decode/detect | Shared (`shared/`) | — | Pure-function helper; both web + desktop import; same lifecycle as `shared/shelfmark_bridge.py` |
| libraries.csv regeneration | Build script (`scripts/`) | — | One-shot mutation, run before deploy; csv_bank loader stays read-only at runtime per Phase 84 D-08 contract |
| fjms_enrichment.db AlmaId pre-population | Sidecar export (`scripts/export_fist_enrichment.py`) | — | All FJMS mutation happens at export time; runtime queries unchanged (D-01 layered-not-extended) |
| Synthetic-row csv_bank load | Backend (`genizah_core.MetadataManager._load_csv_bank`) | — | Already loads any well-formed CSV row; no changes needed if CSV shape is correct |
| Search routing for synthetic rows | Backend (`genizah_core._execute_metadata_search`) | — | Phase 53 precedent path; metadata-only branch handles empty Tantivy text |
| Browse page resolution | Backend service (`web/services.WebDataService.get_browse_page`) | UI (`web/pages/browse.py` + `desktop/viewers.py`) | Service returns BrowsePage dataclass; UI renders + hides NLI elements based on `is_synthetic_sys_id()` |
| CUDL image source switching | Backend service + UI | Crossref sidecar | `cambridge_manifests` join via normalized_shelfmark (already library_code-agnostic); UI sets active_source='cambridge' default |
| FJMS dialog enrichment | Backend service (`shared/fjms_service.py`) | UI dialogs | Service unchanged per D-01; dialogs see populated data once AlmaId column has synthetic IDs |
| Public-API JSON shape | Backend (`shared/search_serializer.py`) | API routes (`web/search_api.py`) | Single source of truth per Phase 77 D-14; add `is_synthetic` field once, both /api/search and /api/browse inherit |
| PostHog event tagging | API hardening (`web/api_hardening.py:capture_api_event`) | — | Endpoint decorator owns event capture; add property via `captured_state` dict |
| KTIV link / NLI Alma calls | UI + backend | — | Branch on helper BEFORE issuing the network call (D-14) |
| Lists / exclusions / comments persistence | Supabase (`list_items`, `comments`) | UI | Schema already accepts opaque sys_id strings (verified in `lists_sync.py`, `supabase_corrections_client.py`) |
| Corrections persistence | Supabase (`corrections` table) | UI | sys_id is a string column; works in principle. Open question on `page_number` semantics for image-less synthetic rows — see §Corrections Subsystem Audit |
| Parallels (composition matching) | Backend (`shared/parallels_service.py`) | — | Reads Tantivy text — synthetic rows have none → naturally absent. No code change needed |

## Standard Stack

This phase doesn't add new third-party libraries; it leverages the existing stack. Versions verified locally.

### Core (existing, no version bump)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Python | 3.10+ | Runtime | Project standard |
| sqlite3 | stdlib | FIST.db / fjms_enrichment.db / nli_crossref.db / pgp.db / joins.db reads | All sidecars are SQLite |
| csv | stdlib | libraries.csv R/W | csv_bank loader uses `csv.reader` |
| supabase | (pinned in requirements-lock.txt) | Lists, comments, corrections, discoveries persistence | Already wired; community writes go through it |
| nicegui | (pinned) | Web UI | Existing |
| PyQt6 | (pinned) | Desktop UI | Existing |
| tantivy | (pinned) | Local full-text index | Read-only for this phase; synthetic rows have no transcription text |

### Supporting
| Module | Path | Purpose | When to Use |
|--------|------|---------|-------------|
| `shared.shelfmark_bridge` | existing | CUDL classmark ↔ libraries.csv normalization (Phase 84) | Synthetic-id helpers can extend it OR live in sibling module — Claude's discretion |
| `shared.fjms_service.FjmsService` | existing | All FJMS reads via `WHERE AlmaId = ?` | Untouched by this phase per D-01 |
| `shared.nli_crossref_service.NliCrossrefService` | existing | `cambridge_manifests` lookup with bridge | `get_cambridge_manifest_with_bridge` already library_code-agnostic; verify works for synthetic rows |
| `shared.search_serializer` | existing | `/api/search` + `/api/browse` JSON shape | Add `is_synthetic` field per D-14 |
| `web.api_hardening.capture_api_event` | existing | PostHog telemetry | Add `is_synthetic` property to `captured_state` dict |
| `scripts.export_fist_enrichment` | existing | FJMS sidecar build | Modify to UNION synthetic-AlmaId rows for inventories without Alma link |
| `tests.test_shelfmark_bridge*` | existing | Bridge module test suite (81 tests, all passing) | Template for synthetic-id helper test suite |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Modifying export_fist_enrichment.py to UNION synthetic rows | Post-export script that INSERTs synthetic AlmaId rows after each table is built | UNION integrates with the existing batch flow, post-process risks transactional drift |
| `# BEGIN/END SYNTHETIC` block in libraries.csv | Separate `synthetic_manifest.json` audit file | Marker block: simpler, fewer files to ship. Manifest: better diff visibility for the ~150-2K synthetic rows. **Recommendation: marker block + manifest BOTH (manifest as audit-only, marker block is the operational reload target).** See §Idempotent Regeneration Patterns. |
| Helper in `shared/synthetic_sys_id.py` | Extend `shared/shelfmark_bridge.py` | Standalone module wins on cohesion (synthetic-IDs are NOT classmark-normalization). Bridge already has its own surface. **Recommendation: standalone module.** |

**Installation:** None — no new dependencies.

**Version verification:** All packages verified pinned in requirements-lock.txt; no upgrade needed for Phase 85. [VERIFIED: `requirements-lock.txt` exists per CLAUDE.md "v7.8 Structural Foundation" milestone notes]

## Architecture Patterns

### System Architecture Diagram

```
                                 ┌─────────────────────────┐
                                 │ FIST_DB_BACKUP/FIST.db  │
                                 │ - dbo_Signature         │
                                 │ - dbo_Inventory         │
                                 │ - dbo_InventoryAlma     │
                                 │ - dbo_UnitCatalogRec    │
                                 │ - linked metadata tables│
                                 └─────────┬───────────────┘
                                           │ (read at export time)
                                           ▼
   ┌────────────────────────────────────────────────────┐
   │ Phase 85 Build-Time Pipeline (offline)             │
   │                                                     │
   │ 1. scripts/generate_synthetic_rows.py              │
   │    ├─ Read cambridge_manifests + FIST tables       │
   │    ├─ Match by normalized shelfmark (D-05a)         │
   │    ├─ Encode synthetic sys_ids (99+InvId+000000)    │
   │    ├─ D-01a collision check vs real Alma IDs        │
   │    └─ Write libraries.csv synthetic block           │
   │       (idempotent, marker-fenced)                   │
   │                                                     │
   │ 2. scripts/export_fist_enrichment.py (MODIFIED)    │
   │    └─ UNION synthetic AlmaId rows in 11 tables     │
   │       (catalog, domains, joins, bibliography,       │
   │        measurements, manuscript_measurements,       │
   │        catalog_free_desc, catalog_full_texts,       │
   │        catalog_sizes, extra_info, computed_meas)    │
   └─────────────┬───────────────────────────┬──────────┘
                 │                           │
                 ▼                           ▼
       ┌──────────────────┐        ┌────────────────────────┐
       │  libraries.csv   │        │ fjms_enrichment.db     │
       │  (with synthetic │        │ (AlmaId column         │
       │   block)         │        │  pre-populated with    │
       └────────┬─────────┘        │  synthetic IDs)        │
                │                  └────────┬───────────────┘
                │                           │
                ▼                           ▼
   ┌────────────────────────────────────────────────────────┐
   │ Runtime (web + desktop)                                │
   │                                                         │
   │ ┌──────────────────────────────────────────────────┐   │
   │ │ MetadataManager._load_csv_bank()                 │   │
   │ │ — synthetic rows load uniformly with real rows   │   │
   │ │ — sys_id digit-normalization preserves '99...'   │   │
   │ │ — build_alias_index() walks variants (Phase 84)  │   │
   │ └──────────────────────────────────────────────────┘   │
   │                  │                                      │
   │                  ▼                                      │
   │  ┌─────────────────────────────────────────────────┐   │
   │  │ shared.synthetic_sys_id (Phase 85, new)         │   │
   │  │ - is_synthetic_sys_id(s)                        │   │
   │  │ - encode_inventory_sys_id(inv_id)               │   │
   │  │ - decode_inventory_id(sys_id)                   │   │
   │  └────────────────┬────────────────────────────────┘   │
   │       Branch ON   │                                     │
   │   ┌───────────────┼─────────────────────────┐           │
   │   ▼               ▼                         ▼           │
   │ Search        Browse                  Public API       │
   │ (no branch)   (hide NLI elements)     (is_synthetic)   │
   │   │           (CUDL default)          (PostHog tag)    │
   │   ▼               │                         │           │
   │ search_by_meta    ▼                         ▼           │
   │ → metadata-only  cambridge_manifests    serialize_*    │
   │ path (Phase 53)  + fjms_service         _payload       │
   └────────────────────────────────────────────────────────┘
```

### Recommended Project Structure

```
scripts/
├── generate_synthetic_rows.py        # NEW: libraries.csv synthetic block writer
└── export_fist_enrichment.py         # MODIFIED: UNION synthetic AlmaId rows

shared/
├── synthetic_sys_id.py               # NEW: helper module (D-01 deliverable)
├── shelfmark_bridge.py               # EXISTING: untouched (Phase 84)
└── fjms_service.py                   # EXISTING: untouched per D-01

genizah_core.py                       # EXISTING: untouched (csv_bank already opaque)

web/pages/
├── browse.py                          # MODIFIED: ~12 call sites add is_synthetic branch
├── browse_state.py                    # EXISTING: state already opaque to sys_id
└── browse_enrichment.py               # MODIFIED: bibliography/catalog blocks gate marc_bib

web/
├── search_api.py                      # MODIFIED: pass is_synthetic to PostHog captured_state
├── api.py                             # MODIFIED: nli_image_by_sysid + fl_ids early-return for synthetic
└── api_hardening.py                   # MODIFIED: capture_api_event accepts is_synthetic property

desktop/
└── viewers.py                         # MODIFIED: btn_ktiv hide + active_source default

genizah_app.py                         # MODIFIED: ~3 KTIV/NLI call sites

shared/
├── search_serializer.py               # MODIFIED: add is_synthetic field to envelope items
└── browse_service.py                  # POSSIBLY UNTOUCHED: enrichment fan-out is library_code-agnostic

reports/
└── synthetic_coverage.md              # NEW: D-03 internal coverage manifest

fist_data/
├── synthetic_manifest.json            # NEW (recommended audit artifact)
└── fjms_enrichment.db                 # REGENERATED with synthetic AlmaId rows

tests/
├── test_synthetic_sys_id.py           # NEW: helper unit tests
├── test_synthetic_rows_search.py      # NEW: search-path round-trip
├── test_synthetic_rows_browse.py      # NEW: browse hide-NLI behavior
├── test_synthetic_rows_api.py         # NEW: public-API serializer
└── fixtures/
    └── synthetic_must_resolve.csv     # NEW: golden fixture (template: cudl_must_resolve.csv)
```

### Pattern 1: Helper Module as Public Contract (D-01)

**What:** Three pure functions live in one file. Every site that needs to differentiate synthetic vs real sys_ids imports the helper rather than slicing strings.

**When to use:** Anywhere a branch is needed on synthetic-vs-real (UI hides, network-call gating, PostHog property, public-API field, regeneration script self-collision check, tests).

**Example:**
```python
# Source: NEW shared/synthetic_sys_id.py
"""Phase 85 synthetic sys_id helpers.

The 18-digit format `99 + InventoryId-zfill(10) + 000000` is the only
publishable contract. All other code MUST consult these helpers; never
hand-roll string slicing or int() conversions.

Per D-01b: sys_ids are strings. Never int(). The 99 prefix preserves
numeric round-trip but we don't permit numeric semantics.

Per D-13: detection is consistent regardless of whether input has been
digit-normalized by the codebase's "".join(ch for ch in str(s) if ch.isdigit())
pattern (see genizah_core.py:3374). All-digit input + length-18 + leading-99
+ trailing-000000 is a sufficient and DETERMINISTIC test.
"""
from __future__ import annotations
from typing import Optional

_SYNTHETIC_PREFIX = "99"
_SYNTHETIC_SUFFIX = "000000"
_INVENTORY_PAD = 10
_TOTAL_LENGTH = 2 + _INVENTORY_PAD + 6  # 18


def is_synthetic_sys_id(s: object) -> bool:
    """Return True iff `s` represents a Phase-85 synthetic sys_id.

    Stable under digit-normalization: input may already have been passed
    through ``"".join(ch for ch in str(s) if ch.isdigit())`` — this helper
    accepts the canonical all-digit form. Any other input (None, empty,
    contains non-digits) returns False.

    Examples:
        >>> is_synthetic_sys_id("990000123456000000")  # InvId=123456
        True
        >>> is_synthetic_sys_id("990025143260205171")  # real Alma (NLI institution suffix)
        False
        >>> is_synthetic_sys_id("")
        False
    """
    if not s:
        return False
    s = str(s)
    if not s.isdigit():
        return False
    if len(s) != _TOTAL_LENGTH:
        return False
    return s.startswith(_SYNTHETIC_PREFIX) and s.endswith(_SYNTHETIC_SUFFIX)


def encode_inventory_sys_id(inventory_id: int) -> str:
    """Convert a FIST.db InventoryId into the 18-digit synthetic sys_id.

    Args:
        inventory_id: Positive InventoryId from dbo_Inventory. Must fit in
            10 digits (0 < inventory_id < 10**10).

    Returns:
        18-character all-digit string. Never an int (D-01b).

    Raises:
        ValueError: when inventory_id is non-positive or doesn't fit in 10 digits.
    """
    if not isinstance(inventory_id, int) or inventory_id <= 0:
        raise ValueError(f"inventory_id must be positive int; got {inventory_id!r}")
    if inventory_id >= 10 ** _INVENTORY_PAD:
        raise ValueError(f"inventory_id exceeds {_INVENTORY_PAD}-digit width: {inventory_id}")
    return f"{_SYNTHETIC_PREFIX}{inventory_id:0{_INVENTORY_PAD}d}{_SYNTHETIC_SUFFIX}"


def decode_inventory_id(sys_id: str) -> Optional[int]:
    """Extract the InventoryId from a synthetic sys_id, or None.

    Returns None for any non-synthetic input (so callers can do
    `inv = decode_inventory_id(s); if inv: ...` without prior is_synthetic check).
    """
    if not is_synthetic_sys_id(sys_id):
        return None
    return int(str(sys_id)[2 : 2 + _INVENTORY_PAD])
```

[ASSUMED: exact docstring wording — planner adjusts; algorithm is locked.]

### Pattern 2: Pre-populate AlmaId Column (D-01)

**What:** At sidecar export time, every InventoryId without an Alma row gets a synthetic AlmaId injected into ALL 11 enrichment tables. fjms_service.py's `WHERE AlmaId = ?` queries find them transparently.

**When to use:** This is THE mechanism for Phase 85 — by D-01, the only one. Alternatives (UNION at query time, separate InventoryId column, `synthetic_alma_to_inventory` mapping table) were all considered and rejected.

**Example (sketch — planner refines per-table):**
```python
# Source: scripts/export_fist_enrichment.py (current state, lines 230-286 for catalog)
# Current:
cursor = source.execute("""
    SELECT DISTINCT
        TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
        cat.UnitCatalogRecId,
        cat.Title,
        ...
    FROM dbo_InventoryAlma alma
    JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
    JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
    JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
    JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
    ...
""")

# Phase 85 modification — UNION ALL with synthetic AlmaId for inventories
# qualifying per D-02 (have CUDL manifest OR substantive FJMS metadata):
cursor = source.execute("""
    -- Existing real-Alma rows
    SELECT DISTINCT
        TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
        ...
    FROM dbo_InventoryAlma alma
    JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
    JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
    JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
    JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
    ...
    UNION ALL
    -- Synthetic rows: inventories qualifying per D-02 with no Alma link
    SELECT DISTINCT
        ('99' || printf('%010d', inv.InventoryId) || '000000') as AlmaId,
        ...
    FROM dbo_Inventory inv
    JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
    JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
    JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
    LEFT JOIN dbo_InventoryAlma alma ON alma.InventoryId = inv.InventoryId
    WHERE alma.AlmaId IS NULL
      AND inv.InventoryId IN (SELECT InventoryId FROM <qualifying_set>)
    ...
""")
```

[VERIFIED: source query template from `scripts/export_fist_enrichment.py:230-286`. The `<qualifying_set>` is the union of (a) inventories matched to CUDL manifests via the cross-product in `generate_synthetic_rows.py`, and (b) inventories with non-empty FJMS metadata fields per D-02b. Planner picks whether the qualifying set lives in a temp table, a CTE, or a generated IN-list.]

### Pattern 3: Layered Hide (D-06 Quiet Degradation)

**What:** Each NLI-only UI element is gated by `not is_synthetic_sys_id(state.sys_id)`. If true, the element renders normally. If synthetic, the element doesn't render at all — no banner, no badge, no console error.

**When to use:** Browse page (web + desktop), `web/api.py` NLI image proxy and fl_ids endpoints (early-return 404 or 204 with note), `genizah_core.fetch_iiif_manifest`/`fetch_marc_data` (skip the network call before issuing).

**Example:**
```python
# Source: web/pages/browse.py:1708 (KTIV link, current)
ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{page.sys_id}"
with ui.link(target=ktiv_url, new_tab=True).classes(...):
    ...

# Phase 85 modification — hide entirely when synthetic
from shared.synthetic_sys_id import is_synthetic_sys_id
if not is_synthetic_sys_id(page.sys_id):
    ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{page.sys_id}"
    with ui.link(target=ktiv_url, new_tab=True).classes(...):
        ...
# else: emit nothing — no element in the DOM, no console error
```

[VERIFIED: line 1708 from grep result above; pattern repeats for all NLI references.]

### Pattern 4: CUDL Default + Page Count From Manifest (D-08)

**What:** When `is_synthetic AND has_cudl_manifest`, browse switches to Cambridge IIIF as the active source by default, and `total_pages` is the canvas count from the manifest. When `is_synthetic AND no manifest`, fall back to Phase 53 metadata-only behavior (`total_pages=0`, no image panel).

**When to use:** `web/services.WebDataService.get_browse_page` — this is where `total_pages`, `external_provider`, and image source decisions are computed. Synthetic rows with manifests skip the NLI manifest fetch entirely.

[VERIFIED: `_BROWSE_PROXY_BY_LIBRARY` at `shared/search_serializer.py:70-76` already routes `library_code='CUL'` → `/api/cambridge_image`, but this is the SEARCH serializer's external-link picker; for the actual browse-page image source, the existing logic in `web/pages/browse.py:3457-3556` switches `state.active_source` based on `_has_cambridge_images`/`_has_oxford_images`/etc. — the synthetic-row branch needs to set `_cam_safe_default = True` explicitly when `is_synthetic_sys_id(page.sys_id)` AND the manifest is present.]

### Pattern 5: Public API Field Addition (D-14)

**What:** `shared/search_serializer.py:_serialize_item` adds `'is_synthetic': bool` to every search result item. `serialize_browse_payload` adds the same to the envelope. Single source of truth.

**When to use:** This is the contract for /api/search and /api/browse consumers (skill, external integrators). Per D-14 + skill consumer note in user constraints, this is required.

```python
# Source: shared/search_serializer.py:292-310 (current _serialize_item return)
return {
    'uid': result.get('uid', '') or '',
    'locator': {...},
    'score': score,
    'shelfmark': display.get('shelfmark', '') or '',
    'title': display.get('title', '') or '',
    'library': {'code': library_code, 'name': library_name},
    'domains': domains,
    'dating': dating,
    'snippet': snippet_clean,
    'excerpt': excerpt,
    'match_terms': match_terms,
    'image_url': _build_image_url(final_sys_id, parsed.get('p_num'), library_code),
}

# Phase 85 modification — add is_synthetic at top level (NOT under locator)
from shared.synthetic_sys_id import is_synthetic_sys_id
return {
    ...
    'is_synthetic': is_synthetic_sys_id(final_sys_id),
    ...
}
```

[VERIFIED: serializer source from `shared/search_serializer.py:292-310`. Schema version bump may be needed — current `SCHEMA_VERSION = 1`. Adding a new field is additive (per Phase 83 stability commitment "additive changes any time"), so schema_version stays at 1. Document in `docs/SEARCH_API.md` + CHANGELOG.]

### Anti-Patterns to Avoid

- **Hand-rolling synthetic detection:** `if sys_id.startswith('99') and len(sys_id) == 18:` — this skips the suffix check (D-13 contract) and breaks on real-Alma sys_ids that happen to be 18 digits and start with 99. Use the helper.
- **Calling `int(sys_id)`:** D-01b explicitly forbids this. Tests should grep for `int(sys_id)` and `int(s)` patterns near sys_id sources.
- **Library_code branching for synthetic behavior:** D-15 reuses CUL/Mosseri codes. Don't introduce `library_code='SYNTH'` or check for the synthetic marker via a new code value. Use `is_synthetic_sys_id`.
- **Adding a new column to fjms_service tables:** D-01 explicitly chose Option-4 (write synthetic IDs into AlmaId column) over Option-3 (add InventoryId column with COALESCE). The schema stays.
- **Putting synthetic-row generation logic in genizah_core.py:** Mutation belongs in build scripts (Phase 84 D-08 layered architecture). genizah_core.py loads from libraries.csv — that's it.
- **Silent fan-out when one CUDL classmark maps to multiple FIST signatures:** D-05a explicitly forbids this — exclude and log to a residue file. Phase 84 had to relearn this with D-06; don't repeat.
- **Issuing the NLI Alma JSON network call THEN handling the 404:** D-14 says branch BEFORE the call. Reduces external requests by ~93-2K per cold-cache cycle and avoids polluting NLI's logs with PNX_MANUSCRIPTS99... requests.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Detect synthetic sys_id | `s.startswith('99') and ...` | `is_synthetic_sys_id(s)` | D-13 contract: must be stable under digit normalization |
| Encode sys_id from InventoryId | `f'99{inv:010d}000000'` | `encode_inventory_sys_id(inv)` | Helper validates input range; raises on overflow |
| Decode sys_id to InventoryId | `int(s[2:12])` | `decode_inventory_id(s)` | Returns None on non-synthetic; no exception path |
| Match CUDL classmark to libraries.csv | Custom regex | `shared.shelfmark_bridge.lookup_cudl()` (existing) | Phase 84 already covers Mosseri/Or/T-S patterns |
| Match shelfmark to Cambridge manifest | Custom resolver | `nli_crossref_service.get_cambridge_manifest_with_bridge()` (existing) | 4-tier cascade already handles edge cases |
| FJMS metadata lookup | Direct SQL | `shared.fjms_service.FjmsService` methods | Pre-populated AlmaIds make this transparent (D-01) |
| Tantivy reindex on synthetic-row add | Custom indexer logic | Nothing — synthetic rows have no transcription text | Out of scope per CONTEXT.md "Tantivy reindex acceptable when synthetic rows change" |
| libraries.csv idempotent rewrite | Custom file-rewriter | `# BEGIN/END SYNTHETIC` marker block + Python read+rewrite | Pattern proven in Phase 84 reports — atomic block replacement |
| PostHog property addition | Manual `posthog.capture` calls | Pass via `captured_state['is_synthetic']` to existing decorator | `web/api_hardening.py:capture_api_event` owns event capture (Phase 78) |

**Key insight:** Phase 85's "build" is overwhelmingly DATA — generating the right rows in the right tables. The CODE surface is small (one helper module, ~20 call-site branches across 6 files, one serializer field, one PostHog property). The risk surface is correctness of the synthetic ID round-trip + the hide-list completeness, NOT algorithmic complexity.

## Runtime State Inventory

This phase is *additive* (new rows in CSV + DB) rather than a rename or migration. Per the GSD researcher protocol, here is the runtime-state audit anyway — it surfaces no blockers but documents the surface for the planner.

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| **Stored data** | (1) `libraries.csv` — append synthetic rows in marker block. (2) `fjms_enrichment.db` — pre-populate AlmaId in 11 tables at export time. (3) `nli_crossref.db.cambridge_manifests` — UNCHANGED, joins via `normalized_shelfmark` not AlmaId. (4) `pgp.db` — UNCHANGED, synthetic rows have no PGP records. (5) Tantivy index — UNCHANGED, synthetic rows have no transcription text. (6) `joins.db` — sys_id-keyed string column, accepts synthetic IDs as opaque strings. (7) Supabase: `corrections.sys_id`, `comments.sys_id`, `list_items.sys_id`, `discoveries.fragment_a_sys_id`/`fragment_b_sys_id` — all string columns; verified no FK on sys_id (only on `author_id` → `auth.users`). [VERIFIED via grep on `supabase_corrections_client.py` + `lists_sync.py`] | Build script writes; no migration of existing records needed |
| **Live service config** | None. Project doesn't have n8n/Datadog/Cloudflare-Tunnel-style runtime config touching sys_id semantics. | None |
| **OS-registered state** | None. No Windows Task Scheduler / launchd / systemd registrations reference sys_id. | None |
| **Secrets and env vars** | `POSTHOG_IP_SALT`, `SEARCH_API_*` — UNCHANGED. No env var encodes a sys_id. | None |
| **Build artifacts / installed packages** | (1) Desktop installer (`CompileScriptGenizah.iss`) bundles `libraries.csv` and `fjms_enrichment.db` and `nli_crossref.db` and `pgp.db`. (2) Web deploy script `deploy.sh` syncs same files to EC2. (3) `dist/GenizahSearchPro/_internal/` — local build directory (already has stale copies; will pick up new versions on next PyInstaller run). | Desktop installer rebuild needed; web `deploy.sh` ships new fjms_enrichment.db + libraries.csv. PostHog dashboards will start receiving `is_synthetic: true` events — no schema changes needed (PostHog accepts arbitrary properties). |

**Nothing found in category for live-service-config and OS-registered-state — verified by grep.**

## Common Pitfalls

### Pitfall 1: csv_bank's digit-normalization makes detection ambiguous

**What goes wrong:** `genizah_core.py:3374` does `sys_id = "".join(ch for ch in str(raw_sys_id) if ch.isdigit())`. If a synthetic sys_id is loaded with extra characters (e.g. quoting or whitespace), the normalized form must still pass `is_synthetic_sys_id()`. Conversely, if downstream code passes a non-normalized (e.g. with embedded dashes) sys_id to the helper, the helper must NOT match.

**Why it happens:** The codebase normalizes at many ingress points (CSV load, query parameters, HTTP route capture). Each ingress strips non-digits. By the time anything reaches `is_synthetic_sys_id`, the input MAY OR MAY NOT have been normalized.

**How to avoid:** Per D-13, choose option (a) — accept the canonical normalized form (all-digit) as the documented input shape. Reject anything with non-digit characters. The helper is fast (string slicing); callers can normalize once before the call. Document this in the helper's docstring.

**Warning signs:** Any test that passes a sys_id with `-` or `99-0001234560-000000` — should return False, not True.

### Pitfall 2: Real Alma sys_ids that happen to be 18 digits and start with 99

**What goes wrong:** The user reported case `T-S NS 329.96` is encoded as a synthetic 18-digit sys_id like `99XXXXXXXXXX000000`. But REAL Alma sys_ids exist that are 18 digits AND start with `99`. Example from search_serializer.py comment: `990025143260205171`. If detection only checks prefix + length, it would mis-classify real rows as synthetic.

**Why it happens:** NLI's Alma institution suffix is `205171`. Any 18-digit Alma sys_id with `99` prefix already matches that pattern UNLESS the suffix is `000000`.

**How to avoid:** ALL THREE checks: starts with `99`, length 18, ends with `000000`. The `000000` suffix is the actual discriminator. D-01a's collision check is the safety net — at export time, assert no real-Alma-keyed `libraries.csv` row produces `is_synthetic_sys_id(sys_id) == True`. Fail-loud on collision.

**Warning signs:** A test fixture should include `990025143260205171` and assert `is_synthetic_sys_id(...)` == False. Reverse: include synthetic and assert True.

### Pitfall 3: cambridge_manifests joins via normalized_shelfmark, not AlmaId

**What goes wrong:** Researchers (and Codex) might assume "every sidecar must accept synthetic AlmaIds." `nli_crossref.db.cambridge_manifests` does NOT — it stores `(label, manifest_url, normalized_shelfmark)`. Lookups go through `get_cambridge_manifest_with_bridge(shelfmark)`. If we redundantly try to inject a synthetic AlmaId into nli_crossref.db, we add data with no consumer.

**Why it happens:** D-14 mentions cross-sidecar tolerance generally — but `cambridge_manifests` was already library_code-agnostic by Phase 33's design.

**How to avoid:** Verify (don't modify) cambridge_manifests during planning. The lookup should "just work" because synthetic rows' `call_numbers_raw` includes the canonical CUDL classmark form (D-12) and `cudl_normalize` matches the cambridge_manifests `normalized_shelfmark`.

**Warning signs:** A planner task adding cambridge_manifests changes is a smell. There should be NONE.

### Pitfall 4: NLI Alma JSON callers fire the request before checking

**What goes wrong:** `genizah_core.py:3737` builds `https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{system_id}-{suffix}/manifest` and fires it. For synthetic sys_ids, this URL doesn't exist; NLI returns 404. The 404 pollutes NLI's access logs and adds latency to browse.

**Why it happens:** Pre-Phase 85, every sys_id was a real Alma id; the network call was always potentially valid. Adding synthetic rows changes this assumption.

**How to avoid:** Per D-14, branch on `is_synthetic_sys_id` BEFORE issuing the network call. `fetch_iiif_manifest`, `fetch_marc_data`, and any other NLI Alma JSON consumer should early-return an empty result for synthetic IDs. Same for `/api/fl_ids/{sys_id}` and `/api/nli_image_by_sysid/{sys_id}` — early-return 404 or 204 (planner picks; 204 is friendlier for client `<img>` error handlers).

**Warning signs:** PostHog should show `is_synthetic: true` events with `nli_404` errors AFTER deploy. If they appear, a hide-list site was missed.

### Pitfall 5: Search-result rendering shows "ID: 99..." instead of shelfmark

**What goes wrong:** `_execute_metadata_search` falls back to `f'ID: {sid}'` when `meta_info.get('shelfmark')` is empty. If the synthetic-row CSV write botches the call_numbers column, the metadata-only branch will display the raw 18-digit sys_id as a "shelfmark" — ugly and untranslatable.

**Why it happens:** The `_execute_metadata_search` fallback at `genizah_core.py:7322` is `f'ID: {sid}'`. It's defensive but unattractive when the actual write was incomplete.

**How to avoid:** Synthetic-row CSV generation MUST populate column 2 (`call_numbers`) with at least the FJMS canonical shelfmark form (D-12). Test the fallback path: assert `csv_bank[synthetic_sys_id]['shelfmark']` is non-empty.

**Warning signs:** Search results showing `ID: 99...000000` in any test or screenshot. Indicates the regeneration script skipped a row's shelfmark.

### Pitfall 6: Idempotent rewrite — duplicating the synthetic block on rerun

**What goes wrong:** First run appends synthetic rows. Second run appends them AGAIN, doubling the synthetic count.

**Why it happens:** Naive append-only mode in the regeneration script doesn't see the existing block.

**How to avoid:** Per D-04a, two viable approaches — see §Idempotent Regeneration Patterns. Recommendation: marker block in CSV (`# BEGIN SYNTHETIC` / `# END SYNTHETIC` as comment-prefixed lines), regeneration deletes everything between markers and rewrites. csv_bank loader must skip lines starting with `#`. **csv_bank loader as currently written (genizah_core.py:3368-3370) already does** `if not row or len(row) < 3: continue` — but a row with first-cell `# BEGIN SYNTHETIC` would fail the digit-normalization check (`sys_id = "".join(ch for ch in raw_sys_id if ch.isdigit())` returns empty string, then sys_id `''` becomes the dict key — overwritten on next iteration. Mostly harmless but ugly. **Cleaner: have the loader skip rows where `raw_sys_id.startswith('#')`.**

**Warning signs:** A test that runs the regeneration script twice and asserts `wc -l libraries.csv` is identical.

### Pitfall 7: D-05a ambiguity — silent fan-out

**What goes wrong:** A CUDL classmark normalizes to a key that maps to multiple FIST signatures. Naive cross-product fans out: one synthetic sys_id per FIST signature → multiple synthetic rows for the same physical manuscript.

**Why it happens:** Phase 84 D-06 had to learn this the hard way with the leading-zero collision audit. CUDL/FIST aren't 1:1.

**How to avoid:** Generate a residue file when ambiguity is detected; exclude (don't fan out) and log. Phase 86 audit picks it up. Mirror Phase 84's `reports/cudl_alias_collisions.csv` pattern.

**Warning signs:** Multiple synthetic rows with the same call_numbers field value (same physical shelfmark, different sys_ids) — should never happen by construction.

### Pitfall 8: Corrections page_number for image-less synthetic rows

**What goes wrong:** Supabase `corrections` table has `page_number` (1=recto, 2=verso, ...). For synthetic rows with no CUDL manifest, "page" has no defined meaning. Lists/comments work because they don't need page numbers. Corrections might.

**Why it happens:** Pre-Phase 85, every browseable manuscript had at least one image, hence ≥1 page. Synthetic + no-CUDL breaks this assumption.

**How to avoid:** Recommendation — defer corrections-write on synthetic rows to a future small phase per D-10's deferrability clause. The reads (browse showing existing corrections) work fine because no synthetic-row corrections will exist in the table on day one. Plan the write-side as a separate lane: the "Add correction" button is hidden when `is_synthetic_sys_id(state.sys_id)`. See §Corrections Subsystem Audit.

**Warning signs:** A correction submitted via desktop shows up under sys_id=`99...000000` with `page_number=null` — Supabase NOT NULL constraint will reject this; user sees an error.

## Code Examples

Verified patterns from official sources or directly readable in this codebase:

### Phase 84 Bridge Module (Architectural Template)

```python
# Source: shared/shelfmark_bridge.py:1-50 (existing, Phase 84)
"""Bridge module for CUDL shelfmark normalization (Phase 84).

Layered on top of genizah_core.normalize_shelfmark() — does NOT replace it.
Used only at the four cross-system lookup sites listed in Phase 84 D-08:
  1. Shelfmark search fallback (genizah_core.py shelfmark-mode search)
  2. Browse CUDL external-link builder (web/pages/browse.py)
  3. cambridge_manifests reverse lookup (shared/nli_crossref_service.py)
  4. Orphan-scanner unification (scripts/scan_cudl_orphans.py)
...
"""
```

This is the template for `shared/synthetic_sys_id.py` — same docstring discipline (decision references, call-site enumeration, contract notes).

### csv_bank Loading and Digit Normalization

```python
# Source: genizah_core.py:3354-3411 (existing)
def _load_csv_bank(self):
    """Load the massive CSV file into memory for instant lookup."""
    if not os.path.exists(Config.LIBRARIES_CSV):
        ...
    import csv
    try:
        with open(Config.LIBRARIES_CSV, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f, delimiter=',')
            next(reader, None)  # Skip header
            for row in reader:
                if not row or len(row) < 3:
                    continue
                raw_sys_id = row[0]
                sys_id = "".join(ch for ch in str(raw_sys_id) if ch.isdigit())  # ← THE NORMALIZATION
                ...
                self.csv_bank[sys_id] = {...}
        ...
        try:
            from shared.shelfmark_bridge import build_alias_index as _build_cudl_alias_index
            _build_cudl_alias_index(self.csv_bank)  # ← Phase 84 hook
        except ImportError as e:
            _warn_bridge_import_failed(e)
```

[VERIFIED: `genizah_core.py:3354-3411`]

**Implication for Phase 85:** synthetic CSV rows must encode the sys_id as an 18-digit all-digit string in column 0. Any quoting / dashes / whitespace gets stripped — the resulting string MUST still satisfy `is_synthetic_sys_id`. Test fixture: write `99-0001234560-000000` to column 0, load, assert csv_bank key is `990001234560000000`, assert `is_synthetic_sys_id(...)` is True.

### Metadata-Only Search Path (Phase 53 Precedent — Synthetic rows ride this unchanged)

```python
# Source: genizah_core.py:7284-7359 (existing, Phase 53)
def _execute_metadata_search(self, query_str, mode, progress_callback=None, restrict_sys_ids=None):
    """Search by title or shelfmark via csv_bank. Returns results even for metadata-only records."""
    ...
    sys_ids = self.meta_mgr.search_by_meta(query_str, target_field)
    ...
    for i, sid in enumerate(sys_ids):
        ...
        text, head, src, uid = '', '', '', ''
        if self.searcher:
            text, head, src, uid = self._get_best_text_for_id(sid)
        metadata_only = not text
        if metadata_only:
            meta_info = self.meta_mgr.get_meta_for_id(sid)
            ...
            display = {
                'shelfmark': meta_info.get('shelfmark', f'ID: {sid}'),
                'title': meta_info.get('title', ''),
                'img': '',
                'source': '',
                'id': sid,
                'library_code': ...,
            }
            results.append({
                'display': display,
                'snippet': '',
                'full_text': '',
                'uid': '',
                'raw_header': '',
                ...
                'metadata_only': True,
            })
```

[VERIFIED: `genizah_core.py:7284-7343`]

**Phase 85 implication:** SYNTH-02 is satisfied by this path with NO code change, provided the synthetic row's csv_bank entry has populated `shelfmark` and `title` fields. The path branches on `not text`; synthetic rows have no Tantivy text → `text=''` → `metadata_only=True` → display rendered from csv_bank metadata.

### cambridge_manifests Lookup (Synthetic-Friendly by Construction)

```python
# Source: shared/nli_crossref_service.py:350-411 (existing, Phase 84)
def get_cambridge_manifest_with_bridge(self, shelfmark: str) -> Optional[str]:
    """Phase 84: Try canonical normalized lookup, then CUDL-bridge fallbacks.
    cambridge_manifests.normalized_shelfmark is stored in CUDL classmark form,
    so the bridge's cudl_normalize() is the appropriate normalizer for queries
    against this table — different from the rest of the codebase.
    """
    if not shelfmark or self._conn is None:
        return None
    ...
    # 1. Existing canonical path (preserves pre-phase-84 behavior).
    url = self.get_cambridge_manifest(normalize_shelfmark(shelfmark))
    if url:
        return url
    # 2. cudl_normalize fallback (cambridge_manifests stores CUDL form).
    url = self.get_cambridge_manifest(cudl_normalize(shelfmark))
    if url:
        return url
    # 3. Mosseri-specific forward-label fallback ...
    # 4. Generic forward-label fallback via shelfmark_to_cudl_label (T-S / Add. / Or.).
    slug = shelfmark_to_cudl_label(shelfmark)
    if slug:
        url = self.get_cambridge_manifest(slug)
        if url:
            return url
    return None
```

[VERIFIED: `shared/nli_crossref_service.py:350-411`]

**Phase 85 implication:** Synthetic rows are passed in via their `shelfmark` field (e.g. `'T-S NS 329.96'`). The Tier-2 cudl_normalize fallback resolves to `tsns329.96` and matches the cambridge_manifests row. **Failure mode** (Q8 from research questions): if a synthetic row's `call_numbers` does NOT include a normalize-able CUDL classmark form, `get_cambridge_manifest_with_bridge` returns None → no CUDL image. Mitigation: D-12 says minimum is FJMS canonical shelfmark form, which IS the form CUDL uses (e.g. `T-S NS 329.96` ↔ `tsns329.96`).

### PostHog Event Capture (D-14 is_synthetic Property)

```python
# Source: web/api_hardening.py:586+ (existing)
def capture_api_event(
    *,
    endpoint_name: str,
    request: Request,
    status_code: int,
    error_code: Optional[str] = None,
    captured_state: dict,
    ...
):
    """Phase 78 — capture an API request as a PostHog event with hashed IP."""
    ...
    properties = {
        ...
        'mode': captured_state.get('mode'),
        'result_count': captured_state.get('result_count'),
        ...
    }
    posthog.capture(...)
```

[VERIFIED: structural pattern from `web/api_hardening.py:586+`. The decorator already passes `captured_state` dict; extension is additive.]

**Phase 85 modification:** Endpoint handlers in `web/search_api.py` set `captured_state['is_synthetic'] = is_synthetic_sys_id(sys_id)` after locator validation. Handler in `capture_api_event` reads it and adds to `properties`. Single source of truth.

## State of the Art

This phase doesn't depend on external library versions — it's project-internal data architecture. State of the art reasoning is internal:

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| sys_ids as ints, library-coded by integer prefix | sys_ids as opaque strings (Phase 84 D-08) | 2026-05 (Phase 84) | Phase 85 inherits — synthetic 18-digit format slots in without retrofit |
| FJMS sidecar = AlmaId-only (real Alma rows only) | FJMS sidecar = Alma + synthetic AlmaId for FJMS-only inventories | This phase | All 30 fjms_service methods unchanged (D-01 layered-not-extended) |
| Phase 53 metadata-only browse — `total_pages=0`, no image panel | Phase 85 D-08 metadata-only-with-CUDL — `total_pages=canvas_count`, Cambridge default source | This phase | Mostly affects synthetic+CUDL subset; no-CUDL synthetic falls back to Phase 53 behavior |
| KTIV/NLI image proxy/manifest fetch on every browse | Branch on `is_synthetic_sys_id` BEFORE network call | This phase | Saves ~93-2K requests per cold cache cycle; no NLI 404 spam |
| No sys_id-validity check on public-API responses | `is_synthetic: true` field on every response item | This phase | Skill consumer + external API users can branch on synthetic |

**Deprecated/outdated:**
- The "Inventory ID no exact match to Alma.xlsx" gap file (Feb 2026 NLI snapshot) — **CONTEXT.md D-05 explicitly excludes it** as a generation source for Phase 85. Hybrid is FIST.db + cambridge_manifests cross-product. The gap file is reference only.
- The notion that "FGP image number = NLI FL ID" — debunked in v5.9.0 milestone (Phase 30 lesson). Synthetic rows have no FL IDs; they don't go through the FL ID resolution path at all.

## Assumptions Log

> Claims tagged `[ASSUMED]` need confirmation before becoming locked decisions.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Helper module belongs in `shared/synthetic_sys_id.py` standalone, NOT extending `shared/shelfmark_bridge.py` | §Standard Stack Alternatives | LOW — D-clarity gain only. Both work; planner can choose. CONTEXT.md says Claude's discretion. |
| A2 | Marker block `# BEGIN SYNTHETIC` / `# END SYNTHETIC` in libraries.csv is the recommended idempotency mechanism (vs separate manifest file) | §Idempotent Regeneration Patterns | LOW — both work; planner picks. Recommendation is "marker block AS the operational target + manifest file AS audit artifact". |
| A3 | Public-API `is_synthetic: true` is added at the top level of each result item (NOT nested under `locator`) | §Pattern 5 | LOW — convention. Codex/skill consumers will adapt either way. Top-level wins on readability. |
| A4 | Defer corrections-write on synthetic rows to a future small phase (Phase 87 or follow-up) | §Corrections Subsystem Audit | MEDIUM — user explicitly accepted deferral as an outcome (D-10). Decision belongs to the planner after reviewing Supabase schema. |
| A5 | NLI image proxy endpoints (`/api/nli_image_by_sysid`, `/api/fl_ids`) should return 204 (not 404) for synthetic IDs | §Pattern 3 | LOW — both work; 204 is gentler on client `<img>` error handlers. Planner picks. |
| A6 | Schema version stays at 1 (additive change) | §Pattern 5 | LOW — Phase 83's stability commitment classifies new fields as additive. |
| A7 | Browse `total_pages` from CUDL manifest canvas count is sourced via existing `cambridge_images` array length, not a new manifest fetch | §Pattern 4 | MEDIUM — assumes existing browse pipeline already resolves canvas count. Planner verifies in `web/services.WebDataService.get_browse_page` impl. |
| A8 | csv_bank loader should be modified to skip rows where `raw_sys_id.startswith('#')` for marker-block cleanliness | §Pitfall 6 | LOW — current loader's digit-normalization makes marker rows "harmless garbage" (zero-key overwrite); explicit skip is hygiene. |
| A9 | Estimated 12-15 NLI/KTIV call sites in web browse + ~5 in desktop | §Summary | MEDIUM — grep estimate; planner's Plan 04 enumerates explicitly during execution (this is normal; the count drives task sizing). |
| A10 | `is_synthetic` PostHog property goes on browse + search + parallels API events; user-facing UI events too if cheap | §Pattern 5 | LOW — D-14 recommendation; can be staged across phases if complex. |

**If this table is empty:** It's not. 10 explicit assumptions, mostly LOW-risk planner-discretion choices. None compromise the locked decisions.

## Open Questions (RESOLVED)

These are gaps that the planner needs to resolve during the planning phase OR by a small experiment. Each entry below carries an explicit `RESOLVED:` line; recommendations have been incorporated into the corresponding plans.

### 1. Exact qualifying-set membership predicate for D-02
- **What we know:** D-02 says EITHER (a) CUDL manifest exists OR (b) substantive FJMS metadata (catalog title, scholarly description, measurement, OR bibliography). Two different scopes.
- **What's unclear:** SQL formulation. Should "substantive" be `EXISTS (SELECT 1 FROM dbo_UnitCatalogRec WHERE ...)` OR a UNION across all four tables? Planner specifies in Plan 02.
- **Recommendation:** Build a CTE `qualifying_inventories` that's a UNION of (a) inventories whose normalized shelfmark joins to cambridge_manifests, and (b) inventories with non-empty fields in any of {dbo_UnitCatalogRec.Title, dbo_UnitCatalogRec.BI_TextualFrameEng, dbo_UnitMeasurement.<any>, dbo_BibRef.<any>}. Iterate the count; if it's ~150-300, scope is right. If 5,000+, tighten the predicate.

### 2. CUDL ↔ FIST.db ambiguous-classmark count
- **What we know:** D-05a requires excluding ambiguous matches.
- **What's unclear:** Empirical count. Phase 84's `reports/cudl_alias_collisions.csv` is excluded keys for libraries.csv↔CUDL — the analog for cambridge_manifests↔FIST is new.
- **Recommendation:** Plan 02 produces `reports/synthetic_ambiguity_residue.csv` with columns (cudl_label, fist_signature_ids, fist_inventory_ids). Phase 86 audit picks it up.

### 3. Tantivy reindex required for SYNTH-03?
- **What we know:** SYNTH-03 says "Tantivy index includes synthetic rows so all standard search modes return them." But synthetic rows have no transcription text.
- **What's unclear:** Does "all search modes" cover Title and Shelfmark only, or also text/Responsa/composition?
- **Answer:** Title and Shelfmark route through `_execute_metadata_search` (genizah_core.py:7284) which doesn't need Tantivy — it queries csv_bank. **Tantivy reindex is NOT required.** Text and Responsa modes will not return synthetic rows by design (no chunks to match). REQUIREMENTS.md SYNTH-03 wording "transcription text is empty when FJMS has no full text, but the row is still discoverable" — discoverability is via Title/Shelfmark modes. Confirmed.
- **Plan implication:** Phase 85 has zero Tantivy infrastructure work. The Build pipeline (`build_index.py`) is untouched.

### 4. Public-API `is_synthetic` property on /api/parallels?
- **What we know:** D-14 mentions `/api/browse` and `/api/search` explicitly. `/api/parallels` not mentioned.
- **What's unclear:** Should parallels-result items also carry `is_synthetic`? Synthetic rows can't be in main_results (no Tantivy text), but they could theoretically appear in `filtered` if there's some edge case.
- **Recommendation:** Add the field anyway via the shared `_serialize_item` (single source of truth — Phase 77 D-14). Cost is zero; consistency wins. Verify with a `/api/parallels` smoke test that synthetic rows don't appear in main_results.

### 5. Multi-IE behavior for synthetic rows
- **What we know:** v7.7 Volume-Aware Browse adds IE infrastructure (3,193 multi-IE manuscripts). Synthetic rows are single-volume by definition (no IE registration).
- **What's unclear:** Does the IE lookup path handle synthetic rows gracefully (returns single IE, no error)?
- **Recommendation:** Plan 04 includes a smoke test: open a synthetic row's browse page and confirm no IE selector is shown. The IE lookup goes through `genizah_core.MetadataManager.get_volumes_for_id` (or similar); for synthetic sys_ids this returns an empty list → UI hides the selector.

### 6. Desktop installer rebuild?
- **What we know:** v7.10 was web-only. Phase 85 changes both `libraries.csv` AND `fjms_enrichment.db`, both bundled in the desktop installer.
- **What's unclear:** Is this release web+desktop or web-only?
- **Recommendation:** Web+desktop. Bundled SQLite files are user-visible "data" changes. Skipping desktop would mean desktop users miss synthetic rows entirely (search returns nothing for `T-S NS 329.96` on desktop). Planner notes this in Plan 05 / release plan.

## Environment Availability

> Phase has external-data dependencies: FIST.db (read), nli_crossref.db (read), libraries.csv (R/W), fjms_enrichment.db (write), Supabase (read for verifying constraints).

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `FIST_DB_BACKUP/FIST.db` | Plan 02 (regeneration) | ✓ | (committed in repo) | None — required to read inventory metadata |
| `nli_data/nli_crossref.db` | Plan 02 (CUDL match) | ✓ (per CLAUDE.md "deployed Jan 2026") | `cambridge_manifests` ~141K | Plan 02 also runs without it (CUDL-only synthetic rows just don't qualify), but coverage suffers |
| `fist_data/fjms_enrichment.db` | Plan 03 (verify export changes) | ✓ (1.6GB, regenerated 2026-04-21) | v5.0.0 schema | None — required to verify pre-populated AlmaIds |
| `libraries.csv` | Plan 02 + tests | ✓ | (committed) | None |
| Supabase access | Plan 05 (verify schema constraints) | Likely (production DB) | — | Static SQL inspection of `corrections`/`comments`/`list_items`/`discoveries` table definitions if live access unavailable |
| Python 3.10+ | All plans | ✓ | (project standard) | None |
| pytest | All plans | ✓ | (in requirements) | None |

**Missing dependencies with no fallback:** None. All required artifacts are in the repo or accessible via standard project tooling.

**Missing dependencies with fallback:** None.

**One operational note:** Phase 84 verification noted `nli_crossref.db not found` in the agent's working tree, which caused `TestScanDiffBaselineStillResolves` to be SKIPPED. For Phase 85 verification, the same risk exists — if the agent doesn't have `nli_crossref.db` locally, the synthetic-row CUDL-resolution rate test will skip. Planner notes this in Plan 05.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 8.x (per requirements-lock.txt; verified by `tests/test_shelfmark_bridge*.py` patterns) |
| Config file | `pytest.ini` (verified existing — Phase 84 ran 1492 tests successfully) |
| Quick run command | `pytest tests/test_synthetic_sys_id.py -x -q` |
| Full suite command | `pytest tests/ -q --ignore=tests/test_visual_similarity.py --ignore=tests/test_translation_service.py --ignore=tests/test_measurements.py` (Phase 84 baseline; pre-existing 15 unrelated failures) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SYNTH-01 | `is_synthetic_sys_id` returns True for valid synthetic, False for real Alma | unit | `pytest tests/test_synthetic_sys_id.py::test_helper_correctness -x` | ❌ Wave 0 — `tests/test_synthetic_sys_id.py` |
| SYNTH-01 | `encode_inventory_sys_id(123456)` → `'990001234560000000'` (round-trip with decode) | unit | `pytest tests/test_synthetic_sys_id.py::test_encode_decode_roundtrip -x` | ❌ Wave 0 |
| SYNTH-01 | Real Alma sys_id `990025143260205171` → False (collision-prevention guarantee) | unit | `pytest tests/test_synthetic_sys_id.py::test_real_alma_not_synthetic -x` | ❌ Wave 0 |
| SYNTH-01 | Detection stable under digit-normalization (D-13 contract) | unit | `pytest tests/test_synthetic_sys_id.py::test_d13_normalization_contract -x` | ❌ Wave 0 |
| SYNTH-01 | Encode raises ValueError on negative / overflow / non-int | unit | `pytest tests/test_synthetic_sys_id.py::test_encode_validation -x` | ❌ Wave 0 |
| SYNTH-02 | Search by FJMS-only shelfmark returns synthetic-row result with FJMS title | integration | `pytest tests/test_synthetic_rows_search.py::test_search_by_fjms_only_shelfmark -x` | ❌ Wave 0 |
| SYNTH-02 | Search by `T-S NS 329.96` returns at least one result (origin case) | integration | `pytest tests/test_synthetic_rows_search.py::test_origin_case_ts_ns_329_96 -x` | ❌ Wave 0 (requires pre-built csv_bank with synthetic rows) |
| SYNTH-03 | csv_bank loads synthetic rows; `csv_bank[synthetic_id]` has shelfmark + title populated | unit | `pytest tests/test_synthetic_rows_search.py::test_csv_bank_loads_synthetic -x` | ❌ Wave 0 |
| SYNTH-03 | csv_bank loader skips marker-block lines (`# BEGIN SYNTHETIC`) | unit | `pytest tests/test_synthetic_rows_csv.py::test_loader_skips_marker_lines -x` | ❌ Wave 0 |
| SYNTH-03 | Tantivy text-mode search returns 0 synthetic results (no chunks) | integration | `pytest tests/test_synthetic_rows_search.py::test_text_search_excludes_synthetic -x` | ❌ Wave 0 |
| SYNTH-04 | Browse-page resolution for synthetic+CUDL renders Cambridge image, no NLI elements | integration (Wave 0 + Wave N) | `pytest tests/test_synthetic_rows_browse.py::test_synthetic_with_cudl_renders -x` | ❌ Wave 0 |
| SYNTH-04 | Browse-page for synthetic+no-CUDL renders metadata-only (no image panel, no errors) | integration | `pytest tests/test_synthetic_rows_browse.py::test_synthetic_no_cudl_metadata_only -x` | ❌ Wave 0 |
| SYNTH-04 | KTIV link not rendered for synthetic rows | integration | `pytest tests/test_synthetic_rows_browse.py::test_no_ktiv_link_for_synthetic -x` | ❌ Wave 0 |
| SYNTH-04 | NLI image proxy `/api/nli_image_by_sysid/{synthetic_id}` returns 204 (or 404 — planner picks) | integration | `pytest tests/test_synthetic_rows_api.py::test_nli_proxy_skips_synthetic -x` | ❌ Wave 0 |
| SYNTH-05 | `FjmsService.get_catalog(synthetic_id)` returns populated record | integration | `pytest tests/test_synthetic_rows_fjms.py::test_get_catalog_returns_synthetic -x` | ❌ Wave 0 (requires pre-populated fjms_enrichment.db) |
| SYNTH-05 | `FjmsService.get_bibliography(synthetic_id)` returns list (possibly empty) without error | integration | `pytest tests/test_synthetic_rows_fjms.py::test_get_bibliography_synthetic -x` | ❌ Wave 0 |
| SYNTH-05 | All 11 fjms_service methods accept synthetic AlmaId without exception | integration | `pytest tests/test_synthetic_rows_fjms.py::test_all_methods_accept_synthetic -x` | ❌ Wave 0 |
| SYNTH-06 | Lists: add synthetic sys_id → list_items, retrieve, remove — round-trip | integration (Supabase mock) | `pytest tests/test_synthetic_rows_community.py::test_lists_round_trip -x` | ❌ Wave 0 |
| SYNTH-06 | Comments: add comment on synthetic — round-trip | integration (Supabase mock) | `pytest tests/test_synthetic_rows_community.py::test_comments_round_trip -x` | ❌ Wave 0 |
| SYNTH-06 | Exclusions: synthetic sys_id can be excluded from search | integration | `pytest tests/test_synthetic_rows_community.py::test_exclusions_round_trip -x` | ❌ Wave 0 |
| SYNTH-06 | Parallels: composition-search seeded by source text returns 0 synthetic-row results (no chunks) | integration | `pytest tests/test_synthetic_rows_community.py::test_parallels_excludes_synthetic -x` | ❌ Wave 0 |
| SYNTH-06 | Public API `/api/search` response items have `is_synthetic: false` for real, `true` for synthetic | integration | `pytest tests/test_synthetic_rows_api.py::test_search_envelope_is_synthetic_field -x` | ❌ Wave 0 |
| SYNTH-06 | Public API `/api/browse` envelope has `is_synthetic` field at top level | integration | `pytest tests/test_synthetic_rows_api.py::test_browse_envelope_is_synthetic_field -x` | ❌ Wave 0 |
| SYNTH-06 | Idempotent regeneration: run script twice, libraries.csv byte-identical | integration | `pytest tests/test_synthetic_rows_csv.py::test_idempotent_regeneration -x` | ❌ Wave 0 |
| SYNTH-06 | D-01a collision check: assert no real-Alma row resolves `is_synthetic` True | integration (uses real libraries.csv) | `pytest tests/test_synthetic_rows_csv.py::test_no_collision_with_real_alma -x` | ❌ Wave 0 |
| SYNTH-04 (regression) | Phase 84 scan-diff baseline: re-run `scripts/scan_cudl_orphans.py`; orphan count drops from 6,052 toward Phase 86 <200 target (T-S/Or residue closes) | integration / manual | `python scripts/scan_cudl_orphans.py && diff reports/cudl_orphans_post_phase84.csv reports/cudl_orphans_post_phase85.csv` | ❌ Wave 0 (script exists, expected output is new) |

### Sampling Rate
- **Per task commit:** `pytest tests/test_synthetic_sys_id.py tests/test_synthetic_rows_csv.py -x -q` (~5 sec, runs on every commit)
- **Per wave merge:** `pytest tests/test_synthetic*.py tests/test_shelfmark_bridge*.py -q` (~30 sec, includes Phase 84 regression)
- **Phase gate:** Full suite green before `/gsd-verify-work` (matches Phase 84 protocol; ~3 min including suite, manual scan-diff re-run)

### Wave 0 Gaps
- [ ] `tests/test_synthetic_sys_id.py` — covers SYNTH-01 (helper unit tests). Template: `tests/test_shelfmark_bridge_unit_index.py` (21 tests).
- [ ] `tests/test_synthetic_rows_search.py` — covers SYNTH-02, SYNTH-03 (search-path integration). Template: existing search tests.
- [ ] `tests/test_synthetic_rows_browse.py` — covers SYNTH-04 (browse hide-NLI behavior). New territory.
- [ ] `tests/test_synthetic_rows_fjms.py` — covers SYNTH-05 (FJMS dialog enrichment). Template: existing fjms_service tests.
- [ ] `tests/test_synthetic_rows_community.py` — covers SYNTH-06 lists/comments/exclusions/parallels round-trip. Template: existing supabase tests + parallels tests.
- [ ] `tests/test_synthetic_rows_api.py` — covers SYNTH-06 public-API serialization. Template: existing search_api/browse_api tests.
- [ ] `tests/test_synthetic_rows_csv.py` — covers SYNTH-03 csv_bank parsing, SYNTH-06 idempotency, D-01a collision check. New territory.
- [ ] `tests/fixtures/synthetic_must_resolve.csv` — golden fixture (template: `tests/fixtures/cudl_must_resolve.csv` 44 rows).
- [ ] No new framework install needed; pytest already in `requirements.txt`.

## Security Domain

> Required when `security_enforcement` is enabled (absent = enabled).

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | No new auth surface; lists/comments/corrections continue to use Supabase RLS |
| V3 Session Management | no | No new sessions |
| V4 Access Control | yes | Public-API endpoints (/api/search, /api/browse, /api/parallels) gain a new `is_synthetic` field. RLS policies on `corrections.sys_id`, `comments.sys_id`, `list_items.sys_id` apply unchanged — synthetic sys_ids are opaque strings, not a new access tier. |
| V5 Input Validation | yes | `is_synthetic_sys_id` is a STRICT validator (digit-only, exact length, exact prefix, exact suffix). Helper acts as defense in depth on inputs that may originate from URL paths or user input. |
| V6 Cryptography | no | No new crypto |
| V7 Errors & Logging | yes | Synthetic-row API responses must NOT leak internal InventoryId via error messages. `decode_inventory_id` is for internal tooling only; no error path exposes it. |
| V13 API & Web Service | yes | New `is_synthetic` field is additive. Document in `docs/SEARCH_API.md`. Backward compatible. |

### Known Threat Patterns for {Python web stack + SQLite + Supabase}

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| SQL injection in regen script `WHERE InventoryId IN ({list})` | Tampering | Parameterized SQL; never f-string interpolation. `sqlite3.Cursor.execute(query, params)` always. |
| sys_id passthrough to `/api/nli_image_by_sysid/{sys_id}` route | Spoofing | The proxy already validates `sys_id` is `digits_only`. Phase 85 adds an `is_synthetic` short-circuit BEFORE the proxy fetch — defense in depth. |
| Public-API `is_synthetic` field as oracle for InventoryId enumeration | Information disclosure | InventoryId space is bounded (FIST.db has ~1.5M signatures). Encoding maps directly. **Mitigation: D-01a collision check ensures the InventoryId is the value actually used; this is by-design exposure, not leak.** |
| Idempotent regen producing different output on race | Tampering | Single-writer; the regen script runs offline as part of build/deploy, not at runtime. No concurrent write risk. |
| User-submitted shelfmark search exploits the `lookup_cudl` fallback | Tampering | Phase 84 already hardened — alias index excludes ambiguous keys. Phase 85 inherits. |
| Marker-block injection in libraries.csv via user-controllable input | Tampering | libraries.csv is build-only artifact; never written from runtime. Marker block is comment-prefixed; csv loader filters by digit-normalization. |
| Supabase RLS bypass via synthetic sys_id | Elevation | RLS policies key on `author_id = auth.uid()`, not on sys_id. Synthetic sys_ids inherit existing access controls automatically. [VERIFIED via `shared/corrections_service.py:54-58` and `lists_sync.py` table operations.] |

## Browse Hide-NLI Audit (Specifics for Plan 04)

This is the empirical answer to research question 3: what call sites need a `is_synthetic_sys_id()` branch?

### `web/pages/browse.py` (verified call sites)

| Line | Pattern | Action |
|------|---------|--------|
| 1708 | `ktiv_url = f"https://www.nli.org.il/...PNX_MANUSCRIPTS{page.sys_id}"` (KTIV link in info panel) | Wrap in `if not is_synthetic_sys_id(page.sys_id):` |
| 1973 | `ktiv_url = ...` + `ui.link('NLI Ktiv', ktiv_url, new_tab=True)` (second KTIV link, different panel) | Wrap |
| 2430 | `img_src = f'/api/nli_image_by_sysid/{frag_sid}?page={pg_idx}'` (reading desk fragment image) | Wrap; for synthetic, skip the fragment or use cambridge_image proxy |
| 2898 | `frag_img_url = f'/api/nli_image_by_sysid/{frag_sid}...'` (another fragment image path) | Same as 2430 |
| 3442 | `img_url = f"/api/nli_image_by_sysid/{page.sys_id}?page={page_idx}..."` (main page image) | Wrap; switch to cambridge_image when synthetic+CUDL |
| 3457-3556 | `state.active_source = 'jts' / 'manchester' / 'oxford' / 'cambridge' / 'nli'` initialization | When synthetic, default to `'cambridge'` if manifest exists, else metadata-only mode |
| 3568-3576 | JS snippet calling `/api/fl_ids/{sys_id}` and opening NLI viewer | Wrap entire block; for synthetic, hide the NLI viewer button |
| 3994-4032 | `_nli_credit_url = ...PNX_MANUSCRIPTS{page.sys_id}...` (NLI credit attribution) | Wrap |
| 4001, 4266 | Image-source detection: `if '/api/nli_image_by_sysid/' in safe_img_url:` | Already library_code-agnostic; works because synthetic rows route to cambridge proxy |
| 606, 638, 682, 900 | `state.active_source = 'nli'` (reset on navigation) | When synthetic, set to `'cambridge'` if manifest, else `''` |

**Estimated: 12-14 call-site branches** in browse.py.

### `web/pages/browse_enrichment.py` (verified)

| Line | Pattern | Action |
|------|---------|--------|
| 503 | `marc_bib = cached.get('marc', {}).get('bibliography', [])` (NLI MARC bibliography) | Skip when synthetic (`marc_bib = []`) |
| 530-537 | `if marc_bib: ... ui.button(f'{tr("Bib. Ktiv")}...')` (NLI bibliography chip) | Already conditional on `marc_bib` non-empty; the skip above suffices |

**Estimated: 1 call-site branch** (early-return at 503 for synthetic).

### `web/api.py` (verified)

| Line | Pattern | Action |
|------|---------|--------|
| 467 | `@target_app.get('/api/fl_ids/{sys_id}')` | Early-return 204 for synthetic |
| 587 | `@target_app.get('/api/nli_image_by_sysid/{sys_id}')` | Early-return 204 for synthetic |
| 408, 441 | `iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{system_id}-{suffix}/manifest` (manifest fetch) and `marc/bib/{system_id}` | Early-return; or do not invoke at all (callers in genizah_core branch first) |

**Estimated: 2-3 endpoint branches** (with helpful logging).

### `genizah_core.py` (verified)

| Line | Pattern | Action |
|------|---------|--------|
| 3737 | `url = f"{Config.NLI_IIIF_BASE}/DOCID/PNX_MANUSCRIPTS{system_id}-{suffix}/manifest"` (`fetch_iiif_manifest`) | Early-return empty result `{'physical_desc': '', 'canvas_map': {}, 'attribution': ''}` for synthetic |
| 3783 | `def fetch_marc_data(self, system_id):` | Early-return empty for synthetic |
| 10383 | `ktiv_url = f"https://www.nli.org.il/...PNX_MANUSCRIPTS{sys_id}..."` (legacy/unknown context) | Wrap |

**Estimated: 3 call-site branches.**

### Desktop (`desktop/viewers.py` + `genizah_app.py`)

| File:Line | Pattern | Action |
|-----------|---------|--------|
| `desktop/viewers.py:702-710` | `self.btn_ktiv = QPushButton(...)`; `self.btn_ktiv.setVisible(False)`; `self.btn_ktiv.clicked.connect(self._open_ktiv_viewer)` | Add hide path for synthetic in `_detect_external_provider` or update call site |
| `desktop/viewers.py:856-861` | `self.btn_ktiv.setVisible(False); self._ktiv_sys_id = None; ... self.external_provider = self._detect_external_provider(meta)` | When synthetic, never set btn_ktiv visible; pre-set provider to `'cambridge'` if manifest |
| `genizah_app.py:12792` | `ktiv_url = f"https://www.nli.org.il/..."` | Wrap |
| `genizah_app.py:21717` | `QDesktopServices.openUrl(QUrl(f"https://www.nli.org.il/...{self.current_browse_sid}..."))` (KTIV button click handler) | Wrap |
| `genizah_app.py:8955-9340` | `fl_ids = meta.get('fl_ids', [])` and image list iteration | Skip iteration when synthetic (already iterates empty list naturally — verify) |

**Estimated: 4-5 call-site branches** in desktop.

**TOTAL: 22-26 call-site branches across 6 files.** Plan 04 enumerates explicitly during execution and tests each.

## CUDL Image Source Switching (Plan 04 Detail)

Per D-08, when `is_synthetic AND has_cudl_manifest`:
- Cambridge IIIF is the default image source (`state.active_source = 'cambridge'`)
- `total_pages` = canvas count from `cambridge_manifests.manifest_url`
- Browse next/prev navigates CUDL canvases

Existing infrastructure to reuse:
- `WebDataService.get_browse_page()` → returns `BrowsePage` with `cambridge_images: list[dict]` populated by Phase 33
- `cambridge_images[i]` has keys `url`, `fl_id`, `folio_label` (verified at `shared/search_serializer.py:516-533`)
- `state.active_source = 'cambridge'` triggers the existing cambridge-image-renderer path

Phase 85 modification: at lines `web/pages/browse.py:3457-3556` (the `_cam_safe_default` switching block), introduce a synthetic-aware default:

```python
# Sketch (planner finalizes)
from shared.synthetic_sys_id import is_synthetic_sys_id

_is_synth = is_synthetic_sys_id(page.sys_id)
_has_cambridge_images = bool(getattr(page, 'cambridge_images', None))

# Existing logic for real rows (unchanged):
# _cam_safe_default = ... existing computation ...
if _is_synth and _has_cambridge_images:
    _cam_safe_default = True   # synthetic+CUDL: Cambridge is THE source
elif _is_synth:
    _cam_safe_default = False  # synthetic-no-CUDL: metadata-only

if _cam_safe_default and state.active_source == 'nli' and not state.source_user_override:
    state.active_source = 'cambridge'
```

The `total_pages` value flows from `BrowsePage.total_pages` — service layer must populate this from `len(cambridge_images)` when sys_id is synthetic. **This requires a Plan 04 task in `web/services.WebDataService.get_browse_page` (or wherever BrowsePage is constructed) — branch on synthetic.**

## FJMS Pre-population Mechanics (Plan 03 Detail)

**Q1 from research questions (answered):**

`scripts/export_fist_enrichment.py` builds 11 enrichment tables. Every table starts the SQL with `FROM dbo_InventoryAlma alma JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId`. This INNER JOIN is the gate that drops inventories without an Alma row.

**Plan 03 modification** for each of the 11 tables — append a UNION ALL of the same query, but:
- Replace `dbo_InventoryAlma alma JOIN dbo_Inventory inv ON ...` with `dbo_Inventory inv LEFT JOIN dbo_InventoryAlma alma ON alma.InventoryId = inv.InventoryId`
- Filter `WHERE alma.AlmaId IS NULL`
- Filter `AND inv.InventoryId IN (SELECT InventoryId FROM <qualifying_set>)` — the qualifying_set CTE per D-02 (CUDL manifest exists OR substantive FJMS metadata)
- Replace `TRIM(CAST(alma.AlmaId AS TEXT))` with `('99' || printf('%010d', inv.InventoryId) || '000000')` to emit the synthetic AlmaId

**Each of 11 tables needs the same pattern** — they all key by AlmaId, all read inventory metadata, and the synthetic-row "data" is the same metadata read via the InventoryId path that bypasses the missing Alma link.

Do all 11 tables need parallel synthetic rows? Per D-01 contract ("every existing WHERE AlmaId = ? query just works"), YES — the runtime queries don't know which tables matter for which inventory. Pre-populating all 11 means `FjmsService.get_catalog`, `get_bibliography`, `get_measurements`, `get_source_names`, etc. all return data when called with a synthetic ID. Some tables will return empty for some synthetic rows (e.g. an inventory with only a catalog title and no bibliography); that's fine — empty is the correct result.

**Performance:** 11 tables × ~93 (origin scope) to ~2000 (D-02 expanded) synthetic inventories = 1K–22K extra rows total. Negligible relative to the existing ~3M-row enrichment DB.

**Schema:** No DDL changes. Just additional INSERTs at export time.

## Search Path Trace (Plan 02 Detail)

**Q2 from research questions (answered):**

`SearchEngine.execute_search(query_str, mode=...)` at `genizah_core.py:7361` branches:
- `mode == 'Title' or 'Shelfmark'` → `_execute_metadata_search` (line 7372-7373) — DOES NOT use Tantivy.
- `mode == 'Regex'` or text/Responsa modes — uses Tantivy index.

`_execute_metadata_search` (lines 7284-7359) — verified earlier:
1. Calls `meta_mgr.search_by_meta(query_str, target_field)` to get matching sys_ids from `csv_bank` (which has Phase 84's CUDL alias index built on top via `shelfmark_bridge.lookup_cudl`).
2. For each sys_id, attempts to fetch Tantivy text via `_get_best_text_for_id(sid)`.
3. If Tantivy returns nothing (`metadata_only = not text`), constructs the display from csv_bank alone.

**Synthetic rows ride this path with NO new branches** — they have csv_bank entries (post-Phase-85), and they have no Tantivy text → metadata_only branch → display from csv_bank metadata. The existing code is library_code-agnostic and Tantivy-text-tolerant.

**Confirm: no Tantivy reindex required for SYNTH-03.** Search discoverability comes via `search_by_meta`, which queries csv_bank and `nli_cache` (the latter is empty for synthetic rows by definition). The `lookup_cudl` fallback at line 4625-4634 also catches CUDL-form queries that don't normalize to a canonical libraries.csv shelfmark — synthetic rows benefit if their `call_numbers` includes the canonical FJMS shelfmark (D-12) which is what users type.

## Idempotent Regeneration Patterns (D-04a Detail)

**Q9 from research questions (answered):**

Two viable strategies:

### Strategy A: Marker block in libraries.csv

Pros:
- One file (libraries.csv) is the operational target — what the runtime loads.
- Diff visibility: `git diff libraries.csv` shows synthetic block changes.
- Pattern parallels Phase 84's report files.

Cons:
- The csv_bank loader needs to gracefully handle marker lines. Current loader's digit-normalization makes them harmless garbage (sys_id `''` overwrites in the dict — kept once at the end). Cleaner: explicit `if raw_sys_id.startswith('#'): continue` before normalization.
- Block boundaries embedded in the data file feel hacky.

### Strategy B: Separate manifest file

Pros:
- Clean separation: `fist_data/synthetic_manifest.json` lists synthetic rows; libraries.csv is purely real data + `# include synthetic_manifest.json` directive at end.
- The regen script reads the manifest, mutates libraries.csv by deleting old synthetic block + writing new from manifest.
- Manifest is JSON → easier to diff structurally.

Cons:
- Two files to coordinate; a missing manifest file leaves stale synthetic rows in libraries.csv.
- Loader needs to read both files (small added complexity).

### Recommendation (A4): **BOTH — Strategy A as the operational target + Strategy B as audit artifact.**

- libraries.csv has `# BEGIN SYNTHETIC ($timestamp, $count rows)` ... `# END SYNTHETIC` block. Real rows come first; synthetic block is fenced.
- `fist_data/synthetic_manifest.json` is written alongside as `[{inventory_id, synthetic_sys_id, source: "cudl_match" | "fjms_metadata", ...}, ...]` for diff visibility and Phase 86 audit.
- Regen script: (1) read existing libraries.csv, drop everything between markers, (2) read FIST.db + cambridge_manifests, generate new synthetic rows, (3) write libraries.csv with new fenced block, (4) write manifest.json with same row content + provenance.
- csv_bank loader: explicit `if raw_sys_id.startswith('#'): continue` (one-line addition).

This duplicates data slightly (synthetic rows in 2 files) but the manifest is purely diagnostic — runtime never reads it. The cost is negligible (~150-2K JSON lines).

## Corrections Subsystem Audit

**Q5 from research questions (answered):**

Verified table writes to Supabase `corrections` table at `supabase_corrections_client.py:797-815`:

```python
data = {
    'author_id': self.current_user._uuid,
    'sys_id': document_id,           # ← string column; accepts synthetic
    'shelfmark': shelfmark,
    'page_number': page_number,       # ← integer column
    'original_text': original_text,
    'corrected_text': corrected_text,
    'notes': notes or '',
    'status': 'draft' if save_as_draft else (status or 'pending')
}
if ie_id:
    data['ie_id'] = ie_id
response = client.table('corrections').insert(data).execute()
```

**Findings:**
- `sys_id` is a **string column** (varchar). 18-digit synthetic IDs fit. **No FK constraint detected on sys_id** — only `author_id` references `auth.users`.
- `page_number` is required (not nullable, based on usage). For synthetic+CUDL rows, `page_number` makes sense (canvas count). For synthetic+no-CUDL rows, no canvas → `page_number` is unclear.
- `shelfmark` accepts the FJMS canonical form.
- `original_text` is the text being corrected. Synthetic rows have no Tantivy text → no original text → corrections become "annotations" rather than "corrections" semantically.

**Recommendation:** **Defer corrections-write on synthetic rows to a Phase 87 follow-up.** Reasoning:
- Lists/comments work because they don't need `page_number` or `original_text` semantics.
- Corrections need both. The semantics for image-less synthetic rows is unclear (what page? what original text?).
- D-10 explicitly delegates this decision to research; the research finding is "the plumbing accepts the data but the user-facing semantics are awkward."
- **Read-side corrections viewing IS safe** — `get_pending_corrections_for_page(client, sys_id, page_number, user_id)` returns `[]` for any sys_id with no rows, including synthetic. No code change needed for read-side.
- **Write-side: hide the "Add correction" button when `is_synthetic_sys_id(state.sys_id)`** — one line in browse UI per app, defers the decision without breaking anything.

**This is a planner decision, not a research finding** — the planner reads this section and chooses ship-now (with awkward semantics) vs defer-now (clean semantics). Recommendation is defer.

## Round-Trip Surface Audit (Q7 + Q6 Answered)

**Lists** (`lists_sync.py`):
- `client.table('list_items').select('id, sys_id').eq(...)` and `.insert(...)` operations all treat sys_id as opaque string. Verified at lines 557-612.
- 18-digit synthetic IDs fit any varchar column.
- Local SQLite `lists.db` (or wherever the desktop list is stored) — if it has `sys_id TEXT`, accepts synthetic. Planner verifies.

**Comments** (`supabase_corrections_client.py:1066+`):
- `data = {'sys_id': document_id, ...}; client.table('comments').insert(data).execute()` — same pattern as corrections. String column, no FK.
- Read-side: `client.table('comments').select('*').eq('sys_id', document_id)` — works.

**Discoveries** (`supabase_corrections_client.py:1360+`):
- `fragment_a_sys_id` and `fragment_b_sys_id` columns. Synthetic IDs fit.

**Exclusions** (in-memory `excluded_sys_ids: set` per `genizah_app.py:2518`, persisted via session storage):
- Set of strings; opaque. Synthetic IDs work.

**Parallels** (`shared/parallels_service.py`):
- Reads via `SearchEngine.search_composition_logic` which iterates Tantivy chunks. Synthetic rows have no chunks → never appear in main_results.
- Confirmed by reading `shared/parallels_service.py:1-60` — the service is a thin wrapper around `search_composition_logic` from `genizah_core`.
- `/api/parallels` endpoint at `web/search_api.py:1145+` — accepts source text, returns ranked sys_id groups. Synthetic seed sys_id (if a future caller passes one) won't crash because the endpoint doesn't validate sys_id existence; it just returns 0 results.

**External-link buttons** (CUDL, Manchester, JTS, Oxford, KTIV) — already verified in §Browse Hide-NLI Audit. The CUDL button works for synthetic+manifest rows via `shelfmark_to_cudl_label` (Phase 84). Manchester/JTS/Oxford buttons should be hidden for synthetic rows because synthetic = CUL/Mosseri only and those collections don't have Manchester/JTS/Oxford catalog entries.

## Sources

### Primary (HIGH confidence — directly read in this codebase)
- `shared/shelfmark_bridge.py:1-465` — Phase 84 bridge module (architectural template)
- `shared/fjms_service.py:1-3500` — FJMS service contract; all queries `WHERE AlmaId = ?`
- `shared/search_serializer.py:1-875` — public-API JSON shape; `_serialize_item` and `serialize_browse_payload`
- `shared/nli_crossref_service.py:302-411` — `cambridge_manifests` lookup with bridge cascade
- `shared/corrections_service.py:1-66` — read-side corrections (already safe)
- `supabase_corrections_client.py:780-815, 1066+, 1360+` — write-side corrections, comments, discoveries
- `lists_sync.py:557-612` — list_items round-trip
- `genizah_core.py:3354-3411` — csv_bank loader and Phase 84 alias-index build hook
- `genizah_core.py:7284-7359` — `_execute_metadata_search` (Phase 53 metadata-only path)
- `genizah_core.py:4508-4636` — `search_by_meta` with `lookup_cudl` fallback
- `genizah_core.py:194-242` — `normalize_shelfmark` canonical
- `scripts/export_fist_enrichment.py:1-300` — FJMS export queries (modification target)
- `web/pages/browse.py:1708, 1973, 2430, 2898, 3442, 3568-3576, 3994-4032` — NLI/KTIV call sites
- `web/pages/browse_enrichment.py:490-565` — bibliography/catalog dialog wiring
- `web/api.py:408-587, 467, 1697` — NLI image proxy + fl_ids endpoints
- `web/search_api.py:1013-1142` — `/api/browse` handler
- `desktop/viewers.py:700-816` — desktop btn_ktiv + external_provider detection
- `genizah_app.py:12792, 21717` — desktop KTIV link sites
- `shared/browse_service.py:1-80` — pure-data enrichment fan-out for /api/browse
- `shared/parallels_service.py:1-60` — composition-search service
- `.planning/phases/84-cudl-shelfmark-normalization/VERIFICATION.md` — Phase 84 outcomes
- `.planning/phases/84-cudl-shelfmark-normalization/84-CONTEXT.md` — Phase 84 D-08 opaque-string contract
- `reports/cudl_orphans_post_phase84.csv` — origin-case T-S NS 329.96 (`MS-TS-NS-00329-00096`) verified present at line; 6,052 total orphans; 206 T-S NS rows; 1,332 total T-S rows; 837 Or rows; 3,883 Mosseri scanner-counted
- `reports/scan_cudl_orphans_post_phase84.txt` — scan summary (CUL rows: 140,170; CUDL manifests: 141,368)

### Secondary (MEDIUM confidence — inferred or reasoned from observed patterns)
- Estimated 12-14 NLI call sites in `web/pages/browse.py` (verified by grep, count subject to small variation)
- Estimated 4-5 NLI call sites in desktop (verified by grep)
- Wave 0 test file count: 7 new test files needed (extrapolated from Phase 84's testing pattern of 3 dedicated test files for the bridge module)

### Tertiary (LOW confidence — assumed without empirical confirmation in this session)
- Tantivy reindex acceptable when synthetic rows change (per CONTEXT.md "Future Deferred"; not empirically tested in this research session)
- Supabase `corrections.sys_id` accepts arbitrary 18-digit strings (inferred from string-typed column; not actually attempted at the database)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — no new libraries; everything in stack is verified existing.
- Architecture: HIGH — Phase 84's bridge precedent is exact template; csv_bank/search/browse paths read directly.
- Hide-list completeness: MEDIUM — grep-based estimates; planner enumerates explicitly during execution.
- Corrections-deferral recommendation: MEDIUM — based on schema reading + UX reasoning; planner makes final call.
- Idempotent regeneration mechanism: MEDIUM — both Strategy A and B are viable; recommendation is opinionated but not forced.
- Browse `total_pages` mechanism: MEDIUM — assumes `len(cambridge_images)` available on BrowsePage; needs verification in `web/services.py`.

**Research date:** 2026-05-08
**Valid until:** 2026-06-08 (estimate — stable codebase, mature Phase 84 foundation, low chance of upstream invalidation)
