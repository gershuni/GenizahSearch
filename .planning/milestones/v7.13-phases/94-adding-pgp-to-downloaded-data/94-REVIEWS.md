---
phase: 94
reviewers: [gemini, codex]
reviewed_at: 2026-05-19T18:30:07Z
plans_reviewed:
  - 94-01-PLAN.md
  - 94-02-PLAN.md
  - 94-03-PLAN.md
  - 94-04-PLAN.md
notes: |
  Claude CLI skipped (running inside Claude Code — review.md SELF_CLI rule).
  Gemini hit HTTP 429 after completing its full review; output is intact.
---

# Cross-AI Plan Review — Phase 94 (Research-Grade Export Metadata)

## Gemini Review

This review covers the 4-wave implementation plan for Phase 94: Research-Grade Export Metadata.

### Phase 94 Plan Review Summary

The plan is exceptionally thorough, structured logically into 4 waves that build from shared primitives to app-specific integration. It rigorously adheres to the multitenant invariants established in v7.12 and successfully incorporates all technical corrections from the preceding Codex critique. The use of a module-level bytes-builder for the desktop app is a significant architectural improvement that enables offline structural testing of a historically difficult-to-test Qt method.

---

### Plan 94-01: Wave 1 (shared/export_dossier.py)

**Summary**
Focuses on the "Scholarly Primitives" layer. It creates the 4 lookup helpers and 2 row-emitter functions that serve as the single source of truth for dossier metadata. It also fixes a latent bug in PGP language splitting and extracts the rich-text snippet helper for cross-app reuse.

**Strengths**
- **D-02 Enforcement:** The lookup helpers use explicit whitelists for return fields, making the "no transcription text in dossier" boundary structurally enforceable.
- **Exception Resilience:** Each helper is self-contained with its own `try/except` and logging, preventing a failure in one manuscript's lookup from crashing a 5000-row export.
- **API Tightening:** Adheres to Codex MUST-FIX items, specifically using real FJMS bibliography field names and the narrower `get_catalog_records` query for performance and safety.

**Concerns**
- **(LOW) Catalog Aggregation:** The "first-non-empty" strategy for catalog summary fields is sensible but might miss data if different catalogers provide different fields. However, for a summary column, this is an acceptable trade-off for brevity.

**Suggestions**
- Ensure the `MetaResolver` documentation explicitly mentions that the returned `library_name` should already be localized (or hard-pinned to English) by the caller.

**Risk Assessment: LOW**
The plan is highly modular and supported by 26+ net-new unit tests.

---

### Plan 94-02: Wave 2 (web state plumbing)

**Summary**
Bridges the gap between the live `/search` results and the background enrichment data. It plumbs `printed_ids` and `result_domains` into the persistent session payload and adds the 3 net-new flags to the JSON serializer.

**Strengths**
- **Atomic Updates:** The `update_search_export_enrichment` helper preserves the `copy-on-update` and `isinstance` guard patterns from Phase 88, ensuring thread safety and data integrity.
- **Background Sync:** The call sites in `search.py` correctly target both Stage-1 and Stage-2 enrichment completion, ensuring the export payload is kept in sync with async arrivals.
- **Negative Invariant:** The parallels-envelope regression test ensures no accidental schema leakage into the parallels API.

**Concerns**
- **(LOW) JSON `domains` field:** The plan notes `domains` already exists at `:365`. While true for the search result list, it must be verified that `transcription_sys_ids` and `printed_sys_ids` are correctly available in the scope of `_serialize_item` when called by `export_json`.

**Suggestions**
- None. The plumbing is standard and follows established patterns.

**Risk Assessment: LOW**
Plumbing is systematic and includes specific multitenant isolation tests.

---

### Plan 94-03: Wave 3 (web xlsx restructure)

**Summary**
The "Web Integration" wave. Restructures the Excel export into a 3-sheet workbook using the unified 12-column order. It activates the enrichment data from Wave 2 and implements conditional RTL.

**Strengths**
- **Unified Schema:** Implements the D-01 unified column order, ensuring researchers get identical artifacts from web and desktop.
- **Rich Snippets:** Extends red+bold highlighting to the web Excel output, increasing scholarly utility.
- **D-04 Compliance:** Correctly hard-pins English for library names even in Hebrew UI mode for the dossier path, while respecting the UI language for sheet direction.

**Concerns**
- **(LOW) IIIF Manifest Deferral:** The decision to keep the header but leave cells empty is a safe "soft scope" move, but should be clearly noted in the `CHANGELOG` to manage user expectations.

**Suggestions**
- Confirm that `sanitize_text_for_excel` is applied *inside* the row-lists before `ws.append` to ensure XML safety for all new metadata fields.

**Risk Assessment: LOW**
The multi-sheet logic is isolated and verified by rehydrating the produced bytes in a unit test.

---

### Plan 94-04: Wave 4 (desktop xlsx parity + docs)

**Summary**
Achieves desktop parity and closes the milestone. The extraction of the xlsx logic into a Qt-free helper is a major win for maintainability. Includes a comprehensive human smoke verification checklist.

**Strengths**
- **Testable Refactor:** Extracting `_build_search_results_xlsx_bytes` allows for automated regression testing of the desktop export without a GUI environment.
- **State Reading:** Directly reads the 3 enrichment signals from the existing desktop state machine, avoiding unnecessary plumbing.
- **Preservation:** Correctly preserves desktop-specific credits and search-info headers while adopting the shared dossier sheets.

**Concerns**
- **(LOW) Header Translations:** Desktop conventionally translates headers via `tr()`. The plan correctly allows this for the main sheet while keeping dossier sheets English-only (D-04).

**Suggestions**
- In the human smoke checkpoint, explicitly check that large result sets on desktop (e.g. 1000+ hits) do not cause a hang longer than ~10-15 seconds.

**Risk Assessment: LOW**
The desktop refactor is surgical and preserves existing CSV/TXT/DOCX paths. The blocking human checkpoint is a critical safety gate.

---

### Overall Phase 94 Risk Assessment: **LOW**

The 4-wave structure is extremely robust. The plan successfully navigates:
1. **Cross-App Parity:** via Wave 1 shared module.
2. **Multitenancy:** via Wave 2 safe storage.
3. **Data Integrity:** via Wave 3/4 unified column order.
4. **Scholarly Accuracy:** via Codex-tightened FJMS/NLI schemas.

The dependency ordering is correct, and the regression guards (both AST and behavioral) are comprehensive. The phase is ready for execution.

---

## Codex Review

**Overall**
The four-wave shape is sound: shared primitives, web state/JSON, web xlsx, desktop parity. The main risks are not conceptual, but integration-specific: compacted web export rows no longer carry `display.img/source/full_text`, JSON `domains` likely will not work reliably from compacted rows, and desktop parity is weakened by inconsistent sheet/header naming.

### Plan 94-01 — Shared Dossier Module

**Summary**
Good foundational plan. The API mostly reflects the prior Codex critique: narrow helpers, real FJMS bibliography fields, `get_catalog_records()` instead of `get_catalog_detail()`, no bibliography call from manuscript rows.

**Strengths**
- Clear narrow public API: 4 lookup helpers, 2 row builders, header constants.
- Good D-02 posture: whitelist projections, no PGP transcription fields, no FJMS `full_texts`.
- `_split_pgp_languages` directly addresses the comma-string bug.
- `meta_resolver` callable is the right cross-app seam.

**Concerns**
- **HIGH:** Test monkeypatch targets are inconsistent with the proposed implementation. If helpers import `get_fjms_service` / `get_nli_crossref_service` / `get_document_for_fragment` inside the function, `monkeypatch.setattr('shared.export_dossier.get_fjms_service', ...)` will not work unless those names exist at module scope.
- **LOW:** Grep acceptance around `transcription` conflicts with the requested module docstring documenting the D-02 prohibition.
- **LOW:** `Catalog Summary` collapses 4 fields into one string. That is acceptable if intentional, but it weakens machine-readability.

**Suggestions**
- Either import service factories at module scope for testability, or update tests to patch the source modules directly.
- Add one test asserting `catalog_summary_for_sys_id` never references `get_catalog_detail` by monkeypatching the service object, not by grepping only.
- Consider returning separate catalog summary fields internally and joining only in `build_manuscript_row`.

**Risk Assessment: LOW-MEDIUM**
Design is strong; main risk is test/implementation mismatch, not feature scope.

### Plan 94-02 — Web State Plumbing + JSON

**Summary**
This wave targets the right data path, but it has two important gaps: JSON `domains` may remain wrong/empty for compacted export rows, and the plan is internally inconsistent about whether `export_excel` passes the new kwargs in Wave 2.

**Strengths**
- Extending `set_search_export` and adding `update_search_export_enrichment` mirrors existing safe-storage patterns.
- Stage-1 and Stage-2 enrichment patch sites are the right general locations.
- `has_pgp` / `is_printed` as always-present booleans is correct.
- Explicit D-10 parallels negative test is valuable.

**Concerns**
- **HIGH:** `serialize_search_payload` currently batches domains only from `result['display']['id']`; stored export rows are compacted to top-level `sys_id`. Without changing sys-id extraction, JSON `domains` can be empty even though `result_domains` exists in the session payload.
- **HIGH:** History-restore `set_search_export(..., transcription_sys_ids=set(), printed_ids=set(), result_domains={})` means restored results export with false/empty metadata. That does not meet "same result set" expectations.
- **MEDIUM:** Wave 2 `must_haves` say `export_excel` passes kwargs, but Task 4 intentionally defers passing them to Wave 3. The plan should not claim both.
- **MEDIUM:** If users export before Stage-2 completes, only visible-page enrichment may be reflected.

**Suggestions**
- Add `result_domains` as an optional `serialize_search_payload` kwarg and prefer it over fresh FJMS lookup for JSON exports.
- Update serializer sys-id batching to use `display.id or result.sys_id or raw_header/uid parse`.
- Recompute or restore enrichment for history restore, or explicitly mark history-restored exports as metadata-incomplete.
- Update Plan 94-02 frontmatter/must-haves to say Excel kwarg activation is Wave 3.

**Risk Assessment: MEDIUM-HIGH**
State plumbing is close, but JSON `domains` and history restore are requirement-level risks.

### Plan 94-03 — Web Xlsx Restructure

**Summary**
The web workbook plan is mostly complete and should produce the intended 3-sheet artifact, but it overlooks the compacted-row shape used by `/api/export/excel`.

**Strengths**
- Correct 3-sheet order and main 12-column layout.
- Reuses `shared/export_dossier.py` for dossier sheets.
- Conditional RTL is scoped to view direction only.
- Rich-text snippet extraction gives good web/desktop parity.
- IIIF deferral is explicit and reasonable.

**Concerns**
- **HIGH:** Web export receives compacted rows from `web/export_state.py`; those rows do not carry `display.img` or `display.source`. The planned `Image/Page` and `Source` columns will often be empty unless rehydrated or preserved.
- **MEDIUM:** `search_terms = extract_search_terms(search_query)` becomes unused in the proposed rewrite, likely causing Ruff `F841`.
- **MEDIUM:** Main-sheet "identical structure" is weakened if web uses `Genizah Results` while desktop uses `Search Results`.
- **LOW:** Tests monkeypatch dossier leaf helpers, but the exporter imports row builders. That is okay only because row builders use module globals; keep that assumption explicit.

**Suggestions**
- Preserve `img` and `source` in `_compact_search_result_row`, or rehydrate them from `raw_header` / `uid` / `meta_mgr.parse_full_id_components`.
- Remove unused `search_terms` / highlight-fill leftovers.
- Add a test using compacted rows, not only full `display` rows.
- Lock sheet/header names across apps, or document the intentional exception in requirements.

**Risk Assessment: MEDIUM**
The workbook design is good; compacted-row rehydration is the main implementation trap.

### Plan 94-04 — Desktop Parity + Docs

**Summary**
Desktop parity is the riskiest wave. It correctly reads desktop state directly and avoids CSV/TXT/DOCX scope creep, but the proposed helper duplicates the web workbook builder and still diverges in sheet names, header translation, and credit/header placement.

**Strengths**
- Correctly targets only the xlsx branch of `export_results`.
- Reads `_pgp_transcription_sys_ids`, `_printed_sys_ids`, and `_result_domain_map` directly.
- Keeps non-xlsx desktop exports out of scope.
- Human smoke gate is appropriate for PyQt + Excel behavior.

**Concerns**
- **HIGH:** Desktop sheet name/test expects `Search Results`, while web and smoke checklist expect `Genizah Results`. This violates "identical structure" unless explicitly accepted.
- **HIGH:** Desktop main headers are translated via `tr()`, while the phase says English-only metadata and identical workbook structure. Hebrew UI could produce non-identical column headers.
- **MEDIUM:** `_build_search_results_xlsx_bytes` duplicates much of web's workbook logic. Future drift is likely.
- **MEDIUM:** Importing `genizah_app.py` in unit tests may be heavy/flaky because of PyQt imports and app-level initialization.
- **MEDIUM:** Desktop `Full Text` may be empty for selected table rows unless the stored result dict reliably carries `full_text`; add fallback to `self.searcher.get_full_text_by_id(uid)` if available.

**Suggestions**
- Use identical English sheet names and headers on both apps: `Genizah Results`, `Manuscripts`, `Bibliography`.
- Prefer a shared workbook-building helper for common sheet/header construction, while keeping app-specific main-row extraction separate.
- Add a cross-parity test comparing web and desktop workbook sheet names and header rows.
- Move the desktop pure xlsx helper outside `genizah_app.py` if feasible, to avoid PyQt import cost in tests.
- Keep docs closeout blocked until smoke passes, as planned.

**Risk Assessment: HIGH**
Desktop parity is achievable, but the current plan still encodes non-identical workbook structure and duplicates logic in the largest file.

---

## Consensus Summary

The two reviewers disagree sharply on overall risk: **Gemini rates the phase LOW** (modular waves, comprehensive guards, codex-tightened API); **Codex rates Wave 2 MEDIUM-HIGH and Wave 4 HIGH**, citing concrete integration traps the surface-level review missed. Where they agree, the design is sound (shared module shape, multitenant posture, D-02 enforcement, dependency ordering). The divergence is where Codex did deeper integration archaeology — it has called out specific data-shape mismatches that Gemini did not check.

### Agreed Strengths
- **Wave structure is sound** (both): shared primitives → web state → web xlsx → desktop parity is the right dependency ordering.
- **D-02 enforcement is structurally sound** (both): whitelisted projections in shared helpers; no PGP transcription text in new dossier surfaces; `get_catalog_records()` (narrow) over `get_catalog_detail()` (reads `full_texts`).
- **Multitenant invariants preserved** (both): chokepoint persistence, copy-on-update, isinstance guards inherited from Phase 88.
- **Rich-text snippet extraction for web/desktop parity is a real win** (both).
- **Wave 1 API tightening per the prior Codex critique is correctly folded in** (both): real FJMS bib field names, narrow helpers, `meta_resolver` callable as the cross-app seam, header constants.
- **Module-level bytes-builder for desktop xlsx enables offline testing** (Gemini explicit; implicit in Codex agreeing that Wave 4's xlsx-branch isolation is right).

### Agreed Concerns (highest priority — both flagged)
- **MEDIUM/LOW from both — `domains` JSON field reachability:** Gemini flags it as LOW ("verify scope in `_serialize_item`"), Codex flags it as HIGH ("compacted rows lack `display.id`, so JSON `domains` can be silently empty"). Codex's framing is the more diagnostic — needs verification before Wave 2 ships.
- **Desktop sheet-name / header-translation parity:** Codex HIGH ("Search Results" vs "Genizah Results"; `tr()` translation of main headers under Hebrew UI breaks "identical structure"). Gemini implicitly accepts the `tr()` translation as LOW; this disagreement reflects different readings of the EXPORT-META "identical structure" requirement. **The plan needs an explicit decision** — either tighten to English-only locked sheet/header names on desktop, or amend EXPORT-META-09 to formally accept the divergence.

### Divergent Views (worth investigating)
- **Compacted-row data shape on web (`/api/export/excel`)** — Codex HIGH: web's `_compact_search_result_row` strips `display.img` and `display.source`, so the planned Wave 3 `Image/Page` and `Source` columns will often be empty. Gemini did not surface this — likely it didn't read `web/export_state.py` deeply. **Likely real; resolve before Wave 3.** Either preserve those fields in compaction, or rehydrate from `raw_header` / `uid` / `meta_mgr.parse_full_id_components` at export time, plus a test that exercises compacted rows (not only full `display` rows).
- **History-restore enrichment loss** — Codex HIGH: when search history is restored, the 3 enrichment sets are restored as empty (`set()`, `set()`, `{}`), so exporting a restored result set produces false/empty metadata. Gemini did not flag this. **Likely real; needs explicit policy** — recompute enrichment on restore, OR mark restored exports as metadata-incomplete, OR scope the requirement to live (un-restored) result sets only.
- **Wave 2 self-consistency** — Codex MEDIUM: must_haves say `export_excel` passes new kwargs in Wave 2, but Task 4 defers activation to Wave 3. Wording fix, not a design fix.
- **Test monkeypatch targets vs in-function imports (Wave 1)** — Codex HIGH: if `shared/export_dossier.py` imports service factories inside helpers (lazy), monkeypatching at module scope won't intercept. Gemini did not check. **Cheap fix** — import factories at module scope OR patch the source modules directly.
- **Unused `search_terms`** (Codex MEDIUM, Wave 3): Ruff F841 if the rewrite leaves a stale binding. Trivial cleanup.
- **Overall risk rating disagreement** — Gemini LOW, Codex HIGH for Wave 4. The disagreement is mostly about how strictly to read "identical structure" + how much weight to give compacted-row / history-restore edge cases.

### Recommended Next Step

Run `/gsd-plan-phase 94 --reviews` to fold Codex's MUST-level concerns into the plans before execution:

1. **Wave 1**: import service factories at module scope (so monkeypatch targets work) OR change test patch targets to source modules; add a behavioral test that fails if `get_catalog_detail` is ever called.
2. **Wave 2**: settle the JSON `domains` sys-id extraction path; decide history-restore policy (recompute / mark incomplete / scope out); fix the must_haves/Task 4 inconsistency about when `export_excel` activates kwargs.
3. **Wave 3**: preserve or rehydrate `display.img` + `display.source` for compacted rows so `Image/Page` and `Source` aren't silently empty; add a compacted-row test path; remove dead `search_terms` binding.
4. **Wave 4**: lock sheet names and main-sheet header strings to English on desktop (override `tr()` for these specific strings) OR amend EXPORT-META-09 to formally accept the divergence; add a cross-parity test comparing web and desktop workbook sheet names and header rows; consider moving the pure-xlsx helper into `shared/` to avoid PyQt import cost in tests.

Gemini's LOW-risk rating is defensible *given the inputs it actually checked*; Codex did deeper integration archaeology and surfaced real traps. Both reviews should be honored — the Codex concerns are the ones that will bite in execution.
