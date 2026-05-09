# Requirements: v7.11 CUDL Coverage & Synthetic Inventories

> **Milestone:** v7.11
> **Goal:** Close the gap between CUDL's ~141K classmark catalogue and GenizahSearch's libraries.csv so users searching for any CUDL-catalogued shelfmark land on a usable record — through better cross-system normalization for shelfmarks that already exist in our data, and synthetic libraries.csv rows for the small residue of FJMS-only inventories that have no NLI Alma record.
> **Status:** Active

## Background

A user-reported case (`T-S NS 329.96`, missing in app, present in CUDL) triggered a deep scan against the CUDL classmark universe. Findings:

- **6,052 CUDL classmarks** were initially flagged as missing from libraries.csv.
- After fixing four normalization bugs (slash, comma, dot-after-letter, leading-zero), the residue dropped sharply.
- A Mosseri-aware normalizer recovers **3,828 of 3,883 (98.6%)** Mosseri classmarks already present in libraries.csv under `library_code=Mosseri` but in `Moss. III,27O` form (CUDL writes `mosseriiii27o`).
- A Cambridge Or. normalizer recovers **584 of 1,421 (41%)** Or. classmarks already present, with deeper work to push the rest.
- The NLI gap file (`Inventory ID no exact match to Alma.xlsx`, 42K rows) confirms ~93 T-S sub-series classmarks are **FJMS-only** — they have FJMS catalogue / bibliography / images but no NLI Alma record at all (NLI's own data confirms this). They cannot be aliased into a neighbour; they need independent rows.

So the milestone splits into a normalization pass (no schema change, recovers thousands of rows) and a synthetic-row mechanism (Option-2 numeric sys_id format, recovers ~150–250 truly-orphan FJMS inventories).

## v7.11 Requirements

### Normalization

CUDL classmark form ↔ libraries.csv shelfmark form mapping fixes. Lands in the bridge layer (`shared/nli_crossref_service.py` and any browse-side resolution) without changing libraries.csv content or schema.

- [ ] **NORM-01** — User can navigate from a CUDL Mosseri page (e.g. `mosseriiii27o`) to the matching libraries.csv row (`library_code=Mosseri`, `Moss. III,27O`). The Roman-numeral-collapsed form `mosseri{vol}{frag}` resolves to its `Moss. {VOL},{FRAG}` equivalent in both directions (browse external link → CUDL, and CUDL → app via shelfmark search).
- [ ] **NORM-02** — User can navigate from Cambridge Or. CUDL classmarks to libraries.csv rows. Both the `Or. 1080 J 15` ↔ `or1080j15` (letter-suffix) and `Or. 1080.1.1` ↔ `or1080.11` (numeric-collapse) patterns resolve correctly.
- [ ] **NORM-03** — Slash, comma, dot-after-letter, and leading-zero normalization fixes (`T-S F 8/002` ↔ `tsf8.2`, `Add. 863, 2` ↔ `add863.2`, `T-S Ar. 48.211` ↔ `tsar48.211`, `T-S NS 329/0014` ↔ `tsns329.14`) apply uniformly to all CUL/Cambridge collections.
- [ ] **NORM-04** — No regression on the 140K already-matching CUL rows or other library_code attributions; existing `shelfmark search` and browse-by-sys_id flows still resolve as before.

### Synthetic Inventories

New libraries.csv rows representing FJMS-only inventories that have no NLI Alma record. Keyed by Option-2 synthetic sys_id (18-digit, `99` + InventoryId-padded-10 + `000000`).

- [x] **SYNTH-01** — A `is_synthetic_sys_id(s)` helper plus encode/decode functions for the 18-digit format exist in shared code; all sites that branch on Alma vs FJMS metadata consult the helper rather than parsing the string ad-hoc.
- [x] **SYNTH-02** — User can search by an FJMS-only shelfmark (e.g. `T-S NS 329.96`) and get a result row backed by a synthetic libraries.csv entry with FJMS-derived title and matching `call_numbers`.
- [x] **SYNTH-03** — The Tantivy index includes synthetic rows so all standard search modes (text/title/shelfmark/Responsa) return them; transcription text is empty when FJMS has no full text, but the row is still discoverable.
- [x] **SYNTH-04** — User can open the browse page for a synthetic sys_id and see CUDL image panel (when a manifest exists), FJMS catalogue/bibliography/measurements, and clear UI signalling that no NLI metadata is available, without errors or empty fallbacks elsewhere on the page.
- [x] **SYNTH-05** — FJMS enrichment lookups (`fjms_service.py`) resolve synthetic sys_ids via their underlying InventoryId, so catalogue/bib/measurement/free-desc dialogs populate correctly. The fallback path is shared by both web and desktop apps.
- [x] **SYNTH-06** — Lists, exclusions, parallels, comments, corrections, and external-link buttons all tolerate synthetic sys_ids: round-trip add/remove/serialize works, and no path silently drops or crashes on the new ID format.

### Coverage Audit

Final pass that confirms the milestone closed the gap and produces a durable artifact for future re-runs.

- [ ] **AUDIT-01** — `scripts/scan_cudl_orphans.py` is re-run after Phases 84–85 land and produces a residual orphan list of fewer than 200 CUDL classmarks (target: most remaining are genuinely missing from both NLI Alma and FJMS, not normalization noise).
- [ ] **AUDIT-02** — `reports/cudl_coverage.md` documents the post-milestone state: matched-by-normalization count, synthetic-row count, residual unmatched count, with per-collection breakdown (T-S, Mosseri, Or, Add., etc.) and the methodology used.
- [ ] **AUDIT-03** — A regression check confirms the v7.9.4 NLI Oxford mislabel fix and existing library_code attributions are unchanged, and the 461 NLI-flipped rows still resolve correctly post-milestone.

## Future Requirements (Deferred)

- Reverse-direction audit: NLI Alma records present in libraries.csv but absent from CUDL/FJMS (different data sources, different scope).
- Periodic NLI gap-file refresh — Chico/Tzippora at NLI publish updates; for now the gap file is a one-shot snapshot.
- Synthetic rows for collections not currently in scope (e.g. AIU/Halper FJMS-only inventories that don't have CUDL manifests).
- Mosseri shelfmark format unification — keep legacy `Moss. III,27O` form; reform out of scope.
- Tantivy-index incremental rebuild for synthetic-row updates without a full rebuild — likely a future infra phase.

## Out of Scope

- Editing NLI Alma records to add the missing classmarks — NLI's responsibility, not ours.
- Adding fabricated NLI-style metadata (Hebrew titles_non_placeholder, etc.) to synthetic rows — they get FJMS-only metadata until/unless NLI publishes catalogue records.
- Migrating libraries.csv to SQLite (still deferred per project-level constraints).
- Server-side IIIF image cache (SEED-001 — separate seed, separate trigger conditions).
- Changing how the app keys browse/search by sys_id — Option-2 synthetic IDs satisfy the existing `starts with 99 + all digits` contract.
- Changing the `library_code` taxonomy — synthetic rows reuse the existing `CUL` code (or `Mosseri` for Mosseri synthetics) rather than introducing a new code.

## Traceability

Filled by the roadmap. See `ROADMAP.md` for phase-to-requirement mapping.
