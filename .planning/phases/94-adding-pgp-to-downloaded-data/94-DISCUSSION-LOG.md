# Phase 999.3: Adding PGP to downloaded data - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-15
**Phase:** 999.3-adding-pgp-to-downloaded-data
**Areas discussed:** Formats, Fields, Missing, Columns, Lists, Language, Repeat

---

## Which exports gain PGP fields

Multi-select. Investigated existing exports first: JSON already includes PGP via `_build_pgp_subset` at `shared/search_serializer.py:473`. Excel and Word currently have no PGP info. List and parallels exports also lack it.

| Option | Description | Selected |
|--------|-------------|----------|
| Excel search results | export_service.py:286 — 7 columns, no PGP | ✓ |
| Word search results | export_service.py:357 — shelfmark + title + snippet only |  |
| List exports | export_service.py:413 — user-saved lists |  |
| Parallels exports (Excel + Word) | export_service.py:477 / :563 |  |

**User's choice:** Excel search results only.

---

## Which PGP fields to include

Multi-select.

| Option | Description | Selected |
|--------|-------------|----------|
| PGP link (URL to princeton.edu) | pgp_url field | ✓ |
| PGP description (HE or EN per user language) | Free-text scholarly description | ✓ (English only — see Language area) |
| PGP transcription text (page-scoped) | Full transcription, can be long |  |
| PGP metadata (type, dates, languages, tags) | Structured fields | ✓ |

**User's choice:** Link + Description + Metadata. Transcription text excluded.
**Notes:** Final column set locked: PGP URL, PGP Description, PGP Type, PGP Date, PGP Languages, PGP Tags.

---

## Missing PGP data

| Option | Description | Selected |
|--------|-------------|----------|
| Empty cell / null | Matches existing convention. (Recommended) | ✓ |
| '—' or 'N/A' literal | Explicit marker; breaks sort |  |
| Skip rows that have no PGP | Filters export down to PGP-only |  |

**User's choice:** Empty cell / null.

---

## Column placement

| Option | Description | Selected |
|--------|-------------|----------|
| Appended at end after 'Full Text' | Preserves existing column order. (Recommended) | ✓ |
| Inserted after System ID (before Score) | Groups PGP with identification columns |  |
| Inserted after Title (before System ID) | Most prominent placement |  |

**User's choice:** Appended at end.

---

## List-valued field rendering

| Option | Description | Selected |
|--------|-------------|----------|
| Comma + space separated | Most human-readable. (Recommended) |  |
| Pipe separated ('Bible\|Letter\|Legal') | Round-trip safe | ✓ |
| Newline separated | Vertical readability in cell |  |

**User's choice:** Pipe separated.
**Notes:** Explicitly overrode the "Recommended" default. User wants research-script-friendly delimiter.

---

## Language fallback

| Option | Description | Selected |
|--------|-------------|----------|
| Fall back to other language with '(EN)' marker | Preserves data, signals gap. (Recommended) |  |
| Fall back silently | Cleaner cells, loses signal |  |
| Leave cell empty if requested language missing | Strict |  |
| (User override) | "PGP is always in English, so this is how the info will be presented" | ✓ |

**User's choice:** Override the entire question — PGP is canonical English, export always in English.
**Notes:** This is a strong directive. No `get_language()` call in the export path, no translation lookups, no fallback logic. The export is a citation artifact in the source language.

---

## Per-row repetition for multi-folio hits

| Option | Description | Selected |
|--------|-------------|----------|
| Repeat PGP fields on every row | Each row self-contained for sort/filter. (Recommended) | ✓ |
| Only fill PGP on first row for that manuscript | Cleaner scan but breaks sort |  |

**User's choice:** Repeat on every row.

---

## Claude's Discretion

- Exact column widths for the new columns.
- Whether to extract a shared `pgp_fields_for_export(sys_id, meta_mgr)` helper or inline the lookup.
- Whether to escape pipe characters that legitimately appear inside a tag value (extremely rare).
- Whether to render PGP URL as an Excel hyperlink (clickable) or plain text — default plain.

## Deferred Ideas

- Word search-results export PGP columns.
- List export PGP columns.
- Parallels exports PGP columns.
- PGP transcription text in exports (Excel cell-size limit concern).
- PGP source / scholar attribution columns (Goitein, V0.8, etc.).
- Hebrew-translated PGP descriptions as an opt-in export mode.
- Excel hyperlink rendering for PGP URL.

---

# Revision 3: Phase 94 Context — 2026-05-19

**Date:** 2026-05-19
**Phase:** 94-adding-pgp-to-downloaded-data (renumbered from 999.3 by /gsd-review-backlog on 2026-05-19)
**Trigger:** `/gsd-discuss-phase 94 --revise`
**Areas discussed:** Shared module shape, Desktop xlsx layout, Plan re-org strategy
**Context for revision:** Plans 94-01/02/03 are stale (web-only, drafted before the 2026-05-19 desktop-parity expansion `EXPORT-META-09`). Per v7.13 ROADMAP hand-off note: "The `94-CONTEXT.md` file itself should be updated during planning to record the desktop-parity scope (or `/gsd-discuss-phase 94` should be re-run to refresh CONTEXT.md before planning)."

---

## Area 1: Shared module shape (`shared/export_dossier.py`)

### Q1.1 — Module scope

| Option | Description | Selected |
|--------|-------------|----------|
| Helpers only | 4 lookup helpers; each app writes its own xlsx row code |  |
| Helpers + row builders (Recommended) | 4 lookup helpers + 2 row-emitter functions returning Python primitives |  |
| Full xlsx writer | Module owns the entire 3-sheet workbook writer |  |
| You decide | Claude picks the cleanest factoring | ✓ |

**Decision:** Helpers + row builders. Module exposes 4 lookup helpers, 2 row-emitter functions returning Python primitives, and 2 header-list constants. Each app handles styling/RTL itself; data shape is locked across both. (Codex critique tightened: row builders accept a `meta_resolver` callable instead of opaque `meta_mgr`.)

### Q1.2 — Error-resilience contract

| Option | Description | Selected |
|--------|-------------|----------|
| Swallow + return None/[] (Recommended) | Helpers wrap service calls in try/except, return None/[] on failure |  |
| Propagate | Helpers don't catch; xlsx-builder layer decides |  |
| You decide | Claude picks | ✓ |

**Decision:** Swallow + return None/[]. Each helper wraps its service call in try/except, logs warning, returns None (PGP/NLI/Catalog) or [] (Bibliography). Empty cells match D-06.

### Q1.3 — Batch shape

| Option | Description | Selected |
|--------|-------------|----------|
| Per-sys_id loops (Recommended) | Default loop pattern |  |
| Batch fetch + dict lookup | Add new batch methods to service modules |  |
| You decide based on what services already expose | Use batch where it exists |  |
| Take your recommendations and ask Codex | Run Codex critique on the recommendation | ✓ |

**Decision:** Per-sys_id loops with no new service-module batch methods in this phase. Validated by Codex (SHOULD-FIX 6 — SQLite point lookups cheap for 50-200 ids; the dangerous `get_catalog_detail()` call is replaced by `get_catalog_records()` per Codex MUST-FIX 3, which eliminates the actual performance footgun).
**Notes:** Codex critique saved as `94-CODEX-CRITIQUE.md`. 4 MUST-FIX + 6 SHOULD-FIX findings all folded into CONTEXT.md (D-01, D-02, D-08, D-14, D-15). Key catches: Bibliography schema mismatch with real FJMS bib field names; "NLI Description" mislabel (actually catalog-entry strings); `get_catalog_detail()` includes `full_texts` (D-02 risk); manuscript row builder should NOT call bibliography helper; rename public helpers without underscore prefix; expose header-list constants; metadata resolver instead of `meta_mgr`.

---

## Area 2: Desktop xlsx layout

### Q2.1 — Main-sheet column order

| Option | Description | Selected |
|--------|-------------|----------|
| Keep desktop's columns, APPEND new flags (Recommended) | Per-app column order differs |  |
| Unify both apps to web's column order | Lose desktop's Image/Page + Source |  |
| Unify both apps to desktop's column order | Web reshuffles + gains Image/Page + Source | ✓ |

**Decision:** Unify both apps to desktop's column order. Both apps end up with identical main-sheet structure.
**Follow-up:** What about web's Score + Full Text columns that desktop doesn't have?
- User chose: "Drop score (it's empty anyway), add full text". Final unified column order: `System ID | Library | Shelfmark | Title | Image/Page | Source | Snippet | Full Text | Has PGP | Is Printed | Domains | IIIF Manifest` (12 columns). Web drops `Score`; desktop gains `Full Text` + 4 new flag/URL columns.
**D-02 amendment:** Existing `Full Text` column is grandfathered (Tantivy-indexed page text already in payload, not a fresh PGP lookup). Strict D-02 prohibition applies only to NEW dossier surfaces.

### Q2.2 — Sub-sheets identical or per-app variations?

| Option | Description | Selected |
|--------|-------------|----------|
| Identical columns on both apps (Recommended) | Sourced from shared/export_dossier.py | ✓ |
| Desktop sub-sheets can vary slightly | Add complexity for app-specific signals |  |

**Decision:** Identical columns on both apps.

### Q2.3 — RTL sheet view

| Option | Description | Selected |
|--------|-------------|----------|
| RTL on all 3 desktop sheets, LTR on all 3 web sheets (Recommended) | Each app uses its own convention |  |
| RTL on desktop main sheet only | Visually inconsistent within desktop workbook |  |
| LTR everywhere on both apps | Regression for Hebrew-reading desktop users |  |
| You decide | Claude picks |  |
| RTL if downloaded from Heb UI, LTR from EN UI | Conditional on UI lang at export time | ✓ |

**Decision:** Conditional on UI language at export time. Hebrew UI → RTL on all 3 sheets; English UI → LTR on all 3 sheets. Web reads UI-lang from safe_storage; desktop reads from its locale state. D-04 amended: reading UI-lang for view direction is NOT a translation operation; the prohibition on `get_language()` applies only to content translation.

### Q2.4 — Rich-text snippet rendering

| Option | Description | Selected |
|--------|-------------|----------|
| Keep on desktop main sheet only, plain elsewhere (Recommended) | Preserve existing desktop UX |  |
| Extend rich-text formatting to web's main sheet too | Parity expansion | ✓ |
| Drop rich-text from desktop too | Regression for desktop users |  |

**Decision:** Extend desktop's `*` → red bold rich-text snippet rendering to web's main sheet. Sub-sheets stay plain text. Extract desktop's existing `write_rich_cell` helper (currently inner function at `genizah_app.py:18000`) into a shared helper.

---

## Area 3: Plan re-org strategy

### Q3.1 — Disposition of existing 3 plans

| Option | Description | Selected |
|--------|-------------|----------|
| Move all 3 to .SUPERSEDED + re-plan from scratch (Recommended) | Cleanest factoring | ✓ |
| Amend in-place — patch 94-01 + 94-02 + add 94-04 desktop | Risk: large patches |  |
| Hybrid — supersede 94-02 only, keep 94-01 and 94-03 | Mixed approach |  |

**Decision:** All 3 existing plans renamed to `*.SUPERSEDED-v2.md`. Original `94-01-PLAN.SUPERSEDED.md` (from 999.3 era) renamed to `*.SUPERSEDED-v1.md` for historical continuity. Re-plan from scratch via `/gsd-plan-phase 94`.

### Q3.2 — Wave structure

| Option | Description | Selected |
|--------|-------------|----------|
| 4 waves: dossier module → web state → web xlsx → desktop xlsx (Recommended) | Matches v7.13 ROADMAP suggestion | ✓ |
| 3 waves: dossier+state → both apps in parallel → verification | Faster but riskier |  |
| 5 waves: split verification into its own wave | Adds explicit verification wave |  |
| You decide | Claude picks |  |

**Decision:** 4 waves: (1) `shared/export_dossier.py` module + tests; (2) web state plumbing + JSON; (3) web xlsx restructure + unified columns + rich-text + RTL; (4) desktop xlsx parity + verification + closeout docs.

### Q3.3 — Cross-AI review posture

| Option | Description | Selected |
|--------|-------------|----------|
| Full /gsd-review --all per wave (Recommended) | Matches v7.12 pattern | ✓ |
| Codex-only review | Skip Gemini |  |
| Review the dossier module wave only | Save review time on boilerplate |  |
| Skip review, plan and execute directly | Risk-prone |  |

**Decision:** Full `/gsd-review --phase 94 --all` per wave. MUST + SHOULD revisions applied via `/gsd-plan-phase 94 --reviews` before execution. If Gemini quota-exhausted, proceed Codex-only and document gap.

---

## Codex Critique (delegated)

Run as part of Q1.3. Saved as `94-CODEX-CRITIQUE.md`. 4 MUST-FIX + 6 SHOULD-FIX + 2 OK findings — all folded into CONTEXT.md. See critique file for full details and disposition.

## Claude's Discretion (this revision)

- Whether the optional `build_dossier_rows` higher-level wrapper ships in Wave 1.
- Exact FJMS `get_catalog_records()` fields to surface in Catalog Summary (3-5, rationale required).
- Exact field on web result dict for `Image/Page` and `Source` columns.
- Exact field on desktop result dict for `Full Text`.
- Whether to extract `write_rich_cell` into `shared_export_utils` vs `shared/export_dossier.py`.
- Whether IIIF Manifest column lives on main sheet or only on Manuscripts sub-sheet (D-13 soft scope).

## Deferred Ideas (this revision)

- Batch fetches in `shared/export_dossier.py` — Codex flagged that the API should not block future batching/caching (SHOULD-FIX 5). Per-sys_id ships in Wave 1; prefetch-map support deferred to a follow-up phase if smoke testing reveals latency issues.
