# Phase 85: Synthetic FJMS Inventory Rows - Context

**Gathered:** 2026-05-08
**Status:** Ready for research/planning

<domain>
## Phase Boundary

Make CUDL-orphaned classmarks AND FJMS-only inventories appear as first-class
manuscripts in the app via synthetic libraries.csv rows using the locked
18-digit numeric sys_id format `99 + InventoryId-padded-10 + 000000`. They get
FJMS metadata when available, CUDL images when a manifest exists, and round-trip
through search / browse / lists / comments / parallels / corrections without
crashes or silent data loss.

In scope: SYNTH-01, SYNTH-02, SYNTH-03, SYNTH-04, SYNTH-05, SYNTH-06 from
`.planning/REQUIREMENTS.md`.

Out of scope (locked in REQUIREMENTS):
- Editing NLI Alma records or fabricating NLI-style metadata.
- New `library_code` values — synthetic rows reuse `CUL` for T-S, `Mosseri` for Mosseri.
- Changing the `sys_id starts with 99 + all digits` contract.
- Migrating libraries.csv to SQLite (project-level deferred).
- Server-side IIIF image cache (separate seed, separate trigger).
- Tantivy incremental rebuild for synthetic-row updates (future infra phase) —
  full reindex acceptable when synthetic rows change.

</domain>

<decisions>
## Implementation Decisions

### FJMS Lookup Architecture
- **D-01:** **Pre-populate AlmaId column + publish helpers (Option 4).** At FJMS
  sidecar export time, write the synthetic sys_id directly INTO the `AlmaId`
  column for FJMS-only inventories. `shared/fjms_service.py` stays unchanged —
  every existing `WHERE AlmaId = ?` query just works. Publish three helpers
  in shared code (e.g. `shared/synthetic_sys_id.py` or extend
  `shared/shelfmark_bridge.py`):
  - `is_synthetic_sys_id(s) -> bool`
  - `encode_inventory_sys_id(inventory_id: int) -> str`
  - `decode_inventory_id(sys_id: str) -> Optional[int]`
- **D-01a (Codex guardrail — collision check):** Export-time assertion that
  generated synthetic sys_ids do NOT collide with any real Alma-linked row in
  libraries.csv. Run as part of the regeneration script; fail-loud on collision.
- **D-01b (Codex guardrail — string discipline):** Sys_ids stay as Python strings
  everywhere. No `int()` conversion at any call site (the `99` prefix preserves
  numeric round-trip, but as a discipline we don't permit numeric semantics).

### Synthetic-Row Scope
- **D-02:** Generate a synthetic row for any classmark NOT already resolved by
  Phase 84's bridge that meets EITHER:
  - (a) Has a CUDL manifest in `nli_crossref.db.cambridge_manifests`, OR
  - (b) Has substantive FJMS metadata in FIST.db — at minimum a catalog title,
    scholarly description, measurement record, OR bibliography entry.
  Inclusive scope per user: "if it has CUDL image we want it" + "include with
  FJMS-only" for the no-image case + "image is enough" for the no-FJMS case.
- **D-03:** Plan-phase researcher must produce a coverage manifest breaking the
  synthetic-row population into three tiers: (1) CUDL+FJMS, (2) CUDL-only no-FJMS,
  (3) FJMS-only no-CUDL. Internal artifact (`reports/synthetic_coverage.md` or
  similar). NOT user-visible badging.

### Persistence
- **D-04:** Synthetic rows append directly to `libraries.csv` via a regeneration
  script (e.g. `scripts/generate_synthetic_rows.py`). The script becomes the
  source-of-truth process; libraries.csv is the durable artifact. Re-runnable
  whenever inputs change. csv_bank loader treats them uniformly with real rows.
- **D-04a (Codex guardrail — idempotency):** Regeneration must produce identical
  output on identical inputs. Planner picks mechanism — either a marked
  `# BEGIN SYNTHETIC` / `# END SYNTHETIC` block in libraries.csv that the script
  rewrites in place, or a separate manifest file (`fist_data/synthetic_manifest.json`
  or similar) that the script reads/writes and uses to delete-and-replace the
  synthetic block. Either way: NO duplicate rows on rerun.

### Generation Source
- **D-05:** Hybrid — cross-product of `nli_crossref.db.cambridge_manifests`
  (CUDL classmark inventory) × FIST.db (`dbo_Signature` for InventoryId resolution
  + linked tables for FJMS metadata harvest). NLI gap-file Excel
  (`Inventory ID no exact match to Alma.xlsx`) excluded — frozen Feb 2026 snapshot,
  BL/RNL/JTS heavy, out-of-scope per REQUIREMENTS.
- **D-05a (Codex guardrail — matching, not literal cross-product):** Match by
  normalized shelfmark key with ambiguity exclusion. If a CUDL classmark maps
  to multiple FIST signatures, exclude (don't fan out) and log to a residue file
  Phase 86 audit can pick up. Avoid the silent-merge anti-pattern Phase 84 D-06
  established.

### Browse UX
- **D-06:** **Quiet degradation, no badge.** Hide NLI-only UI elements when
  `is_synthetic_sys_id(sys_id)` is true: KTIV link, NLI source toggle option,
  NLI catalog references panel, NLI bibliography chips, NLI image source button,
  any `/api/nli_image_by_sysid` calls, any `/api/fl_ids` resolution attempts.
  No banner, no badge. The browse page renders only the data we have. Web +
  desktop parity. Matches Phase 53 metadata-only precedent.
- **D-07 [supersedes earlier "FJMS-only badge" answer]:** No visible source
  badge. Earlier "include with FJMS-only badge" answer in scope discussion was
  about whether to *include* the row, not about visual badging. D-06 stands.
- **D-08 (CUDL-only synthetic browse semantics):** When
  `is_synthetic_sys_id(sys_id) AND has_cudl_manifest(sys_id)`:
  - Cambridge IIIF is the **default** image source (no NLI attempted at all).
  - `total_pages` driven by CUDL manifest canvas count.
  - Browse next/prev navigates CUDL canvases.
  - When no CUDL manifest exists, fall back to `total_pages=0` metadata-only
    behavior (Phase 53 precedent).

### Title & Shelfmark Shape
- **D-09:** libraries.csv column 7 (`titles_non_placeholder`) precedence for
  synthetic rows: `FJMS TitleHeb → FJMS Title → FJMS GenizahTitle → shelfmark
  string`. Hebrew preferred (primary user base); deterministic so identical
  inputs always produce identical output (D-04a).
- **D-12 (call_numbers shape):** Match existing CUL row convention. **Minimum:**
  the FJMS canonical shelfmark form (e.g. `T-S NS 329.96`). If cheap to derive,
  also include the normalized variants the canonical normalizer produces (slash,
  dot, leading-zero forms). Planner picks variant generation strategy; outcome
  is "shelfmark-mode search resolves the synthetic row from any reasonable
  user input".

### Community Writes
- **D-10:** Round-trip safety:
  - **Lists + comments:** Allowed. They operate at sys_id level; opaque-string
    handling means synthetic IDs just work. Verify via plan-phase research.
  - **Corrections:** Allowed in principle, but **deferrable** if the
    uid/p_num plumbing proves hard. Plan-phase research must surface the
    corrections-on-synthetic-rows complexity (uid attachment for CUDL-only rows
    where uid='', Supabase FK constraints on document_corrections.fragment_uid
    or similar). Planner decides ship-now vs defer-to-future-phase based on
    findings.
  - **Parallels:** Synthetic rows have empty Tantivy text → won't appear in
    composition-parallel results (no chunks to match). Acceptable. Confirm in
    plan-phase research.
  - **Exclusions:** sys_id-keyed; should round-trip transparently.

### Helper Contract
- **D-13 (`is_synthetic_sys_id` contract):** The helper must produce consistent
  detection regardless of whether input has been digit-normalized by the
  codebase's existing `"".join(ch for ch in str(s) if ch.isdigit())` pattern.
  Either (a) accept already-normalized input as the canonical input shape and
  document this in the docstring, OR (b) perform deterministic normalization
  itself. Critical because sys_id normalization happens at many ingress points
  before the synthetic-vs-real branch; an inconsistent helper would yield
  different verdicts at different call sites.

### Cross-Sidecar Tolerance
- **D-14 (Codex guardrail — Alma-keyed sidecars):** Beyond fjms_enrichment.db
  (which gets synthetic IDs in AlmaId column per D-01), other Alma-keyed
  sidecars and external services should tolerate "no match" for synthetic IDs:
  - `nli_crossref.db.cambridge_manifests` joins via `normalized_shelfmark`,
    not AlmaId — already safe; verify.
  - NLI Alma JSON callers (KTIV link builders, etc.) must branch on
    `is_synthetic_sys_id()` BEFORE issuing the network call.
  - PostHog: add `is_synthetic: true` event property on browse/search/parallels
    interactions involving synthetic rows so analytics can separate them.
  - Public `/api/browse` and `/api/search` should return synthetic rows
    cleanly (no broken/empty image URLs in serialized output) — verify in
    plan-phase research.

### Library Code Attribution
- **D-15 (locked in REQUIREMENTS Out of Scope):** Synthetic CUL rows reuse
  existing `library_code=CUL`. Synthetic Mosseri rows reuse `library_code=Mosseri`.
  No new codes. Codex flagged that `library_code=CUL` currently implies
  CUL+NLI mixed behavior — the D-06 quiet-degradation hide-NLI-on-synthetic
  rule resolves this without library_code changes.

### Claude's Discretion
- Internal organization of the helper module (`shared/synthetic_sys_id.py` vs
  extending `shared/shelfmark_bridge.py`).
- Exact mechanism for D-04a idempotency (marker block in CSV vs separate manifest).
- Variant generation strategy for D-12 call_numbers (which subset of normalized
  forms to emit).
- Whether to add a `synthetic_manifest.json` audit file alongside the regenerated
  CSV block (recommended for diff/coverage tracking, but planner's call).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & Roadmap
- `.planning/REQUIREMENTS.md` §"Synthetic FJMS Inventory Rows" (SYNTH-01 …
  SYNTH-06) — locked acceptance criteria.
- `.planning/REQUIREMENTS.md` §"Out of Scope" — library_code reuse, sys_id
  contract, deferred items.
- `.planning/ROADMAP.md` §"Phase 85 -- Synthetic FJMS Inventory Rows" — goal +
  5 success criteria.
- `.planning/STATE.md` §"Investigation Summary (pre-milestone)" — origin case
  (T-S NS 329.96), gap-file context, Phase 84 outcomes.

### Phase 84 Foundation (Bridge Layer — Already Shipped)
- `.planning/phases/84-cudl-shelfmark-normalization/84-CONTEXT.md` — Phase 84
  decisions, especially D-08 ("treat sys_ids as opaque strings so Phase 85
  doesn't need to retrofit") and D-02 (`normalize_shelfmark` untouched).
- `.planning/phases/84-cudl-shelfmark-normalization/VERIFICATION.md` —
  Phase 84 outcomes including post-bridge orphan counts (T-S: 1,332; Or: 837;
  Mosseri scanner-counted: 3,883 but most resolved via alias index).
- `shared/shelfmark_bridge.py` — Phase 84 bridge module
  (`cudl_normalize`, `lookup_cudl`, `build_alias_index`,
  `shelfmark_to_cudl_label`). Synthetic rows must integrate cleanly with
  the bridge's lookup paths.

### FJMS Sidecar (Pre-population Target)
- `scripts/export_fist_enrichment.py` — FJMS sidecar exporter. D-01 requires
  modifying the export step that walks `dbo_InventoryAlma` to also emit
  synthetic AlmaId rows for inventories without Alma links.
- `shared/fjms_service.py` — ~30 methods all keyed by `WHERE AlmaId = ?`.
  Stays unchanged per D-01.
- `fist_data/fjms_enrichment.db` — 11 AlmaId-keyed tables: catalog, domains,
  joins, bibliography, measurements, manuscript_measurements, catalog_free_desc,
  catalog_full_texts, catalog_sizes, extra_info, computed_measurements.

### NLI Crossref (CUDL Manifest Source)
- `shared/nli_crossref_service.py` — `cambridge_manifests` table queries.
  D-08 requires that `get_cambridge_manifest_with_bridge()` resolve synthetic
  sys_ids correctly (Phase 84 already wired this for shelfmark-keyed lookup).
- `nli_data/nli_crossref.db.cambridge_manifests` — 141K rows, indexed by
  `normalized_shelfmark`. D-05 generation source.

### Existing Metadata-Only Browse Path (Phase 53 Precedent)
- `genizah_core.py:7285` — `search_by_meta` metadata-only search path.
  Synthetic rows ride this code path for SYNTH-03 search discoverability.
- `genizah_core.py` `_get_metadata_only_browse_page` — handles sys_ids without
  Tantivy text. D-08 extends this for synthetic rows with CUDL images.

### libraries.csv Loader
- `genizah_core.py:3357-3411` — `_load_csv_bank` parses libraries.csv into
  csv_bank dict. Synthetic rows must parse identically; D-04 requires the
  regeneration script to produce CSV rows in the same column shape.

### NLI Gap File (Reference Only — Excluded as Generation Source)
- `FIST_DB_BACKUP/gap_files/Inventory ID no exact match to Alma.xlsx` — frozen
  Feb 2026 NLI snapshot. ~3,406 no-match records. Reference only; D-05
  excludes as generation source (BL/RNL/JTS heavy, out-of-scope).
- `FIST_DB_BACKUP/gap_files/README.md` — gap-file methodology and AlmaId
  institution-code background.

### Reports (Reference Data from Phase 84)
- `reports/cudl_orphans_post_phase84.csv` — 6,052 rows including residue that
  D-02 will close: 1,332 T-S, 837 Or, 3,883 Mosseri (mostly scanner artifact).
- `reports/cudl_full_normalization_collisions.csv` — 529 rows; D-05a
  ambiguity-exclusion candidates.

### Tantivy Index Builder
- `build_index.py` — invokes `Indexer.create_index()`. Synthetic rows have
  empty transcription text and don't affect Tantivy ingestion (which reads
  Transcriptions.txt, not libraries.csv). Search discoverability comes
  through `search_by_meta`, NOT through Tantivy. Verify in plan-phase research
  that `execute_search` Title/Shelfmark/Responsa modes route synthetic-row
  matches via the metadata-only path.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **Phase 84 bridge** (`shared/shelfmark_bridge.py`) already establishes the
  pattern of a shared cross-system lookup module. Synthetic-id helpers can
  live here (extension) or in a sibling module (`shared/synthetic_sys_id.py`).
  Planner's call.
- **Phase 53 metadata-only search** (`genizah_core.py:7285`) already returns
  Title/Shelfmark/Responsa hits for csv_bank-only rows without crashing on
  empty Tantivy text. Synthetic rows fit this path with no new branches.
- **csv_bank loader** treats `library_code=CUL` and `library_code=Mosseri`
  rows uniformly. Synthetic rows reusing those codes need no loader changes.
- **`get_cambridge_manifest_with_bridge`** (`shared/nli_crossref_service.py`)
  already has a 4-tier cascade for shelfmark→manifest resolution. Synthetic
  rows with CUDL manifests resolve via the existing Tier 1 (canonical) path
  if their `call_numbers` includes the canonical CUDL classmark form.

### Established Patterns
- **Sidecars are read-only at runtime.** All mutation happens at export time.
  D-01's "write synthetic IDs into AlmaId column" follows this pattern —
  mutation in `scripts/export_fist_enrichment.py`, runtime queries unchanged.
- **csv_bank loaded once at startup**; all metadata lookups go through it.
  Synthetic rows append uniformly. No hot-path changes.
- **Phase 84 D-08 "opaque string" contract** for sys_ids — bridge code already
  tolerates 18-digit synthetic format. No retrofit needed.

### Integration Points
- **Browse page** (`web/pages/browse.py`, `desktop/browse_*.py`): Will branch
  on `is_synthetic_sys_id(sys_id)` to apply D-06 (hide NLI elements) and D-08
  (CUDL as default image source). Estimated 6–10 call sites in web,
  similar in desktop.
- **Search results renderer**: Synthetic rows are rendered like Phase 53
  metadata-only rows. No changes expected.
- **Public API** (`POST /api/search`, `GET /api/browse`): Codex flagged
  these emit weak/empty image data for metadata-only rows. Plan-phase research
  must surface what synthetic rows look like in the JSON serializer
  (`shared/search_serializer.py`) — may need `is_synthetic: true` field for
  consumer skill / external API users.
- **PostHog event capture**: New `is_synthetic` boolean property on browse,
  search, and parallels events when sys_id is synthetic.

### Constraints
- **Both apps must be maintained** (web + desktop parity). All branch points
  duplicate.
- **No int conversion of sys_ids anywhere** (D-01b). Codebase audit may surface
  paths that violate this.
- **Idempotent regeneration** (D-04a) — the script is the source-of-truth, not
  the CSV diff.
- **No new library_code values** (D-15) — synthetic rows reuse `CUL` and
  `Mosseri`. Behavior differentiation flows through `is_synthetic_sys_id`,
  not through library_code.
- **Tantivy reindex on synthetic-row changes** is acceptable per REQUIREMENTS
  Future Deferred. Synthetic rows have no transcription text → index isn't
  affected for them; reindex isn't actually triggered by adding rows.

</code_context>

<specifics>
## Specific Ideas

- **Inclusive coverage stance**: User explicitly chose maximum inclusion at
  every scope decision: include FJMS-only no-image rows, include CUDL-only
  no-FJMS rows, default to CUDL as image source. The premise is "if any
  external system holds something useful (image OR scholarly metadata),
  GenizahSearch should let researchers find and view it." This shapes downstream
  trade-offs toward "render what we have, hide what we don't, no apologies."

- **Codex review captured a key contradiction**: User's earlier "Include with
  FJMS-only badge" answer (the no-CUDL-image case) was about whether to
  *include* the row, NOT about visual badging. The later quiet-degradation
  decision (D-06) stands. Captured as D-07 explicitly to prevent re-litigation.

- **"Layered, not extended" carries from Phase 84**: D-01 keeps fjms_service
  unchanged (data layer accommodates the new ID format) rather than threading
  synthetic-detection branches through 30 service methods. Same reversibility
  lever — if the synthetic mechanism causes regression, regenerate the FJMS
  sidecar without synthetic rows and unwire the libraries.csv block.

- **Helper-as-public-contract**: D-01 explicitly publishes three helpers as
  the SYNTH-01 deliverable. Codex's reasoning: "without helpers, people will
  eventually hand-roll string slicing in link builders, analytics, tests, or
  import scripts." The helpers ARE the architecture, not an afterthought.

- **The `000000` suffix is the discriminator**: Real Alma sys_ids end in
  `205171` (or other institution codes). Synthetic ends in `000000`. D-01a's
  collision check is the safety net for when this discriminator becomes
  ambiguous in the future.

</specifics>

<deferred>
## Deferred Ideas

- **NLI-publishes-real-Alma migration**: If NLI later catalogs one of these
  inventories with a real Alma ID, we'd migrate the synthetic row to the
  real ID and keep an alias/redirect mapping. Defer to a future phase
  triggered by an actual NLI publication event.
- **Reverse audit (NLI Alma in libraries.csv but absent from CUDL/FJMS)**:
  Already in REQUIREMENTS Future Deferred. Different data sources,
  different scope, different milestone trigger.
- **Synthetic rows for non-CUL collections** (AIU/Halper FJMS-only inventories
  without CUDL manifests): Already in REQUIREMENTS Future Deferred. Phase 85
  scope is CUL/Mosseri only because those are the collections with CUDL coverage.
- **Periodic NLI gap-file refresh**: Already in REQUIREMENTS Future Deferred.
  No known NLI changelog feed; D-05 picks FIST.db as the refreshable source instead.
- **Tantivy incremental rebuild for synthetic-row updates**: Already in
  REQUIREMENTS Future Deferred. Synthetic rows don't actually trigger index
  rebuilds (no transcription text), but if we ever added FJMS scholarly
  descriptions to the index, this would matter.
- **Mosseri "2nd series" patterns** (`Ms. L 241`, `Ms. MOSS NS`): Carried
  from Phase 84 deferred. If they surface in Phase 86 residue, address in
  follow-up.
- **`is_synthetic: true` PostHog property on consumer-skill / external-API
  surface**: Captured as D-14 plan-time research item. If complex, may slip
  to a follow-up small phase.

</deferred>

---

*Phase: 85-synthetic-fjms-inventory-rows*
*Context gathered: 2026-05-08*
