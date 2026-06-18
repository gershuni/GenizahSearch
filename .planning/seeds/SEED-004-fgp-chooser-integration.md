---
id: SEED-004
status: dormant
planted: 2026-06-18
planted_during: FGP-transcriptions groundwork (prepared while v8.2.0 "Web Joins Lab" runs in parallel)
trigger_when: v8.2.0 ships AND a transcription-sources / scholarly-coverage milestone is next — surface this for `/gsd-new-milestone`
scope: Medium (3 phases, ~122–124; both apps)
related: docs/plans/FGP_CHOOSER_MILESTONE.md, docs/plans/FGP_TRANSCRIPTIONS_INTEGRATION_PLAN.md, fgp_data/README.md, memory project_fgp_transcriptions_sidecar
---

# SEED-004: FGP transcriptions as a selectable source in the version chooser (alongside PGP)

## Why This Matters

The Friedberg Genizah Project (FGP) transcription corpus is **already prepared** into a
gitignored sidecar `fgp_data/fgp_transcriptions.db` (387 MB, 45,034 rows, schema mirroring PGP
`document_sources`, `sys_id` resolved 99.94%). It is a large body of scholarly transcriptions
not currently surfaced anywhere in the apps. Wiring it into the existing version chooser — next
to PGP — gives users a second transcription witness per fragment with very low risk, because the
data already matches the `document_sources` shape the chooser consumes.

## Why It's a SEED (not started now)

A **separate GSD milestone (v8.2.0 "Web Joins Lab", phases 117–121) is being executed in
parallel.** This repo's GSD has no isolation (`use_worktrees:false`, `branching_strategy:"none"`,
no workstreams/workspaces), and `gsd-new-milestone` **replaces `REQUIREMENTS.md` and resets
`STATE.md`** — starting this milestone now would clobber the live v8.2.0 state. So the ground was
prepared **non-destructively**: a full readiness doc + this seed, with `gsd-new-milestone`
deferred until v8.2.0 ships.

## Scope (already specified)

- **IN:** FGP's *extracted transcription text* (a faithful PDF→text conversion already in the DB)
  shown as a distinct, selectable source in the transcription chooser, in **both** apps.
- **OUT / deferred:** (a) making FGP text *searchable* + dedup-vs-PGP/V0.8 — a later milestone;
  (b) displaying the original FGP **PDFs** — a possible later stage (the ~1.4 GB PDF tree is
  gitignored + not deployed; would reuse the v7.15 PDF-page renderer).

Full spec — draft requirements (FGP-01..12), the Codex-reviewed code-grounded integration recipe,
open decisions, the condensed 3-phase shape, and the instantiate-after-v8.2.0 runbook — lives in
**`docs/plans/FGP_CHOOSER_MILESTONE.md`**.

## When to Surface

Present during `/gsd-new-milestone` once v8.2.0 has shipped (run `/gsd-complete-milestone v8.2.0`
first). Phases start at **122**. Before `/gsd-plan-phase 122`, re-grep the wiring sites (line refs
stale while 119–121 land): `get_all_sources_for_fragment`, `create_version_selector`,
`_populate_pgp_combo`, `PGPSourceWorker`, `get_section_for_page`.

## Key landmines (from the readiness doc)

1. FGP rows share PGP's `doc_relation` values ('Digital Edition'/'Digital Translation') — without
   a `source='fgp'` discriminator they'd silently fold into the green "PGP" chooser group.
2. FGP `sections` are keyed `page_num`; `get_section_for_page()` matches `canvas_num` — normalize,
   or no-`page_info` rows show full text on both sides.
3. Chooser surfaces are **plural**: web = browse + browse_enrichment + search_results; desktop =
   reading desk + `_populate_pgp_combo` + `desktop/result_dialog.py`. Cover all of them.
4. Namespace merged IDs (`pgp:`/`fgp:`) — integer `source_id` collides across the two DBs.
5. Flag belongs in `shared/` (`FGP_TRANSCRIPTIONS_ENABLED`), not a web-only import; decide desktop
   default ON-after-ship or the bundled DB never surfaces.
