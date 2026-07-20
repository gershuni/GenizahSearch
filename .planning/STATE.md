---
gsd_state_version: 1.0
milestone: v9.0.0
milestone_name: Discovery — Same-Work Identification & Connection Atlas
status: executing
stopped_at: Completed 133-02-PLAN.md
last_updated: "2026-07-20T16:38:54.640Z"
last_activity: 2026-07-20
progress:
  total_phases: 7
  completed_phases: 0
  total_plans: 6
  completed_plans: 2
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-07-20)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 133 — visual-atlas-preview-early-quick-win

## Current Position

Phase: 133 (visual-atlas-preview-early-quick-win) — EXECUTING
Plan: 3 of 6
Status: Ready to execute
Last activity: 2026-07-20

Progress: [██░░░░░░░░] 17%

## Roadmap Summary (v9.0.0)

Condensed 7-phase roadmap: an early atlas quick win, then the REL-01 gate sequence (thin de-risk spine first):

| Phase | Goal | Requirements |
|-------|------|--------------|
| 133 Visual Atlas Preview (early quick win) | Static, canon-masked corpus-overview atlas on a standalone `/atlas` beta page; offline layout bake; FIRST deployable artifact under the REL-01 atlas-preview exception | ATLAS-01 |
| 134 Discovery Data Spine | Masked, versioned sidecar + async service + frozen-frame/budget artifacts | DATA-01..08, DATA-10, PERF-01 |
| 135 Precision Certificate & Confidence Bands | Data-driven band display + methods page + pre-registered tier-A measurement | BAND-01..05, CERT-01, CERT-02 |
| 136 Read Surfaces — Panel & Work→Witnesses | Browse connections panel + `/work/{id}` witness-map | PANEL-01..03, WORK-01, WORK-02 |
| 137 Community Judgments | Supabase migration + ✓/?/✗ voting layer (never affects bands) | JUDGE-01..05 |
| 138 Leads Queue | `/leads` R-B screening lane, uncertified, canon caveated | LEADS-01, LEADS-02 |
| 139 Atlas Drill-down, Homepage & Release Hardening | Server-bounded drill-down (absorbs the preview) + homepage band + SEO/i18n/RTL/a11y/obs + REL-01 flag-flip | ATLAS-02/03, SEO-01, I18N-01/02, A11Y-01/02, OBS-01/02, REL-01 |

## Accumulated Context

### Key Decisions (v9.0.0 roadmap)

- **OWNER REVISION (2026-07-20): Visual Atlas Preview is the milestone's FIRST deployable artifact** — new self-contained Phase 133 (ATLAS-01, offline bake from the research data via the `build_atlas_draft.py` prototype approach; no claim-model sidecar dependency), deployed early under the REL-01 ATLAS-PREVIEW EXCEPTION: no claim-level statements (no identifications/bands/numbers, cluster/shelfmark-level only), work labels only from reviewed neutral titles or omitted, asset-level masking scan, PERF/i18n basics, behind the feature flag. The full REL-01 gates still govern everything else.
- **7 theme-grouped phases (condensed):** one-phase-per-requirement rejected per the house condensed-roadmap preference.
- **REL-01 ordering is the spine (Phases 134-139):** claim model + masked schema → title map + sidecar + frozen-frame (both in 134) → certificate card draw (135) → read surfaces (136) → Supabase migration + security smoke → judgment UI (both in 137) → leads (138) → bounded atlas → public promotion (both in 139). No reorder across these gates.
- **CERT-01 is a parallel research track:** its frame freezes AFTER Phase 134 distillation stabilizes; cards drawn in Phase 135; owner grading runs in parallel with the Phase 136–138 UI build; the completed certificate gates the Phase 139 REL-01 public-promotion flag-flip.
- **Cross-cutting reqs homed in Phase 139** (I18N-01/02, A11Y-01/02, SEO-01, OBS-01/02, REL-01) as the comprehensive release-hardening gate — but translations/RTL/a11y are built into every UI surface from line one; 139 owns final verification.
- **Two hard blockers drive Phase 134:** M-source provenance masking (structural, at the sidecar-build boundary + permanent leak-vector CI scan) and event-loop safety (all sidecar/graph queries off the loop via the DiscoveryService, timeouts + concurrency cap).
- **UX discuss-phase precedes Phase 133/134 planning** and settles: ATLAS-02 graph primary object (before the Phase 133 layout bake), DATA-01 relation-vocabulary bilingual wording, BAND-04 per-surface disclaimer wording, final band-selection/row counts, neutral work-title curation workflow, atlas scope.

### Pending Todos

- Run the UX discuss-phase (atlas graph object / atlas scope / relation wording / disclaimer wording / band-selection / title curation), then `/gsd-plan-phase 133`.

### Blockers/Concerns

None at roadmap creation.

## Deferred Items

Carried forward from prior milestones (unchanged; see MILESTONES.md for full context):

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| FUT-01 | Text-reuse engine as `/parallels` (desktop: composition) backend | Future (v9-deferred by user) | v9.0.0 |
| FUT-02 | Public API endpoints for discovery (band-labeled, masked) + skill parity | Future | v9.0.0 |
| FUT-03 | Desktop parity for the discovery module | Future | v9.0.0 |
| FUT-04 | Refresh pipeline/cadence for the discovery snapshot | Future | v9.0.0 |
| FUT-05 | Live-interactive full-corpus WebGL atlas (sigma.js) + multi-hop | Future | v9.0.0 |
| FUT-06 | Public rendering of moderated free-text annotations | Future | v9.0.0 |
| FUT-07 | R-B / gen-2-at-scale certification; R-A independent audit (external gate) | Future | v9.0.0 |
| FUT-08 | New generalized discovery exports (xlsx/CSV) | Future | v9.0.0 |

Older cross-milestone deferrals (JSA/JWB Component B, DEFER-01..05 decomposition, D-F12, etc.) remain tracked in `docs/OPEN_ISSUES.md` and the v8.4.0 archive; not v9.0.0-relevant.

## Session Continuity

Last session: 2026-07-20T16:38:54.633Z
Stopped at: Completed 133-02-PLAN.md
Resume file: .planning/phases/133-visual-atlas-preview-early-quick-win/133-03-PLAN.md
Next step: UX discuss-phase, then `/gsd-plan-phase 133`.

## Performance Metrics

| Phase | Plan | Duration | Notes |
|-------|------|----------|-------|
| Phase 133 P01 | 55min | 1 tasks | 4 files |
| Phase 133 P02 | 40min | 3 tasks | 12 files |

## Decisions

- [Phase 133 P01]: scan_repo uses a fast literal-byte matcher (not the rich normalized/encoded matcher) to stay practical over a real working tree with large non-ignored untracked content — A full --scan-repo run against the actual working tree (~24GB unrelated ACL2026_papers/) exceeded 3 minutes with a shared rich matcher; splitting scan_repo (fast) from scan_asset (rich) matches the plan's own acceptance-criteria wording and completes in ~2 minutes
- [Phase 133 P01]: M-source leak in genizah_translations.py scrubbed via codename rename (M-source), not deletion, preserving the unwired Discovery Review deck glossary's structure
- [Phase 133]: [Phase 133 P02]: Small fixed/dynamic lookup tables (domain groups, library codes) live in manifest.json rather than the binary string heap; edge deltas are plain unsigned (no zigzag) via a group-reset rule; island-only clusters reuse the SAME force-layout/dust-ring code path as continuation clusters with MIN_CLUSTER=1
- [Phase 133 P01 round-3, fix(133-01) 02657578]: A round-2 adversarial re-review found the prior hardening only PARTIALLY closed its findings; round-3 closes all 9 HIGH + 1 MEDIUM COMPLETELY. ONE canonical fail-CLOSED pipeline (decode->unescape->casefold->match) runs on every surface. HIGH-1: non-ASCII case/normalization is covered on EVERY file via exhaustive precomputed case+NFC/NFD BYTE forms (bytes.lower folds only ASCII) — deliberately NOT by casefolding 25GB haystacks (that ran ~23min and is CI-prohibitive); wide/BOM + de-escaped text are additionally Unicode-casefolded. HIGH-2: BOM-less wide input scanned under EVERY codec (UTF-16LE/BE, UTF-32LE/BE), no tie-break guess. HIGH-3: escape decoding composes with wide decoding. HIGH-4: .br ALWAYS fully decompressed (never streamed raw) with compressed+decompressed caps; huge non-.br files stream with overlap=longest-match-span-1 + fail-closed if a pattern can't straddle. HIGH-5: URL decode is byte-level + form(+->space), fail-CLOSED on decoder failure, gated to NUL-free text. HIGH-7: HEAD probe distinguishes proven-unborn from operational error. HIGH-8: os.lstat/os.scandir with checked errors; symlinks scanned by link-text, never followed. HIGH-9: Issue.format() + every diagnostic route through a pattern-aware sanitizer; raw subprocess stderr / raw asset paths never printed. HIGH-10: --self-test un-bypassable (PRESENCE via `is not None`; empty --scan-asset rejected). Perf: html.unescape (pure-Python, pathological on binary) replaced by a C-scanned numeric-char-ref regex; URL decode skipped on binary (NUL) buffers — --scan-repo now 3m16s over the 24.9GB local scratch tree (~161MB/s on PDFs), seconds on a clean CI checkout. Tests: 81 pass / 1 skip (symlink priv on Windows); ruff clean.
- [Phase 133 P01 gap-fix, fix(133-01) 3922be48]: The masking scanner (scripts/check_atlas_masking.py) was hardened against an adversarial Codex review (9 HIGH + 1 MEDIUM). This SUPERSEDES the earlier "fast literal matcher on scan_repo, rich matcher on scan_asset" decision — that split was a coverage gap (normalized/encoded/multi-byte-encoding leaks could enter the repo undetected). ONE semantically-complete, fail-CLOSED matcher now runs on EVERY surface (repo + asset): literal + NFC/NFD+casefold + UTF-8/16/32 + URL/HTML/JS (incl. mixed literal+escaped) forms. Speed is preserved with the bytes.find primitive (~21GB/s), a single unquote_to_bytes URL buffer, and a windowed de-escape around sparse HTML/JS introducers — NOT by dropping coverage. git enumeration is now NUL-delimited (-z) + object-id cat-file batch; Brotli payloads are decompressed and scanned; leaky file NAMES are scanned and redacted; every git/read/decode/empty-pattern failure is fail-closed (non-zero exit). --scan-repo runs ~4.1min over the local 24.9GB untracked scratch tree (seconds on a clean CI checkout). Tests rebuilt to be load-bearing (real temp git repos, real Brotli, encoded/mixed forms, fail-closed sims): 59 pass; ruff clean.
- [Phase 133 P02 Wave-2 fix, fix(133-02) e4b66d1f]: Closed 3 HIGH + 2 MEDIUM correctness defects Codex found in the frozen atlas schema bake (the decode contract Wave-3's JS decoder/renderer build on). HIGH-1: EDGE_CLASS polarity was reversed vs the FROZEN schema doc — encoder now emits 0=continuation, 1=island per docs/specs/atlas-asset-schema-v1.md (doc is authoritative, left unchanged); added a semantic edge-class test. HIGH-2: canonicalize EVERY sys_id (pair endpoints, graph node keys, libraries.csv + FJMS domain metadata keys) through validate_sys_id() before baking so all lookups/set-ops use ONE int representation (mixed str/int keys were producing phantom/duplicate nodes and dropping title/domain/library); added a mixed-id-type metadata test. HIGH-3: EXACT eligible==placed set equality is now ENFORCED (raise before writing, no >= fudge) in run_bake + write paths + main() for every mode incl. --report, plus an encoded-node-set==eligible defense and the 62,414 real-bake regression floor; added a rejection test. MEDIUM-1: added an encoder-output-lock test (encode_asset reproduces the committed golden bytes) so encoder drift fails even a green decode-only test — this is exactly how HIGH-1 hid. MEDIUM-2: golden dataset sized (n=40) to yield a >=25-node cluster so CLUSTER_LABEL_* sections are exercised, with non-empty/field-correct assertions. HIGH-4 (shelfmark in heap) is a REJECTED false positive — shelfmark is an intended masking-safe per-star catalogue field, retained. Golden fixtures regenerated deterministically via --golden. Gates: 14 atlas_bake tests pass, smoke bake within byte budget, masking scan clean (exit 0), ruff clean.
- [Phase 133 P02 Wave-2 re-review fix, fix(133-02) 56bbeb9a]: A Codex re-review of the HIGH-2/HIGH-3 hardening above found the fixes were still incomplete on 3 points (1 real HIGH + 1 defense-in-depth HIGH + 1 MEDIUM). HIGH (real, metadata collision): `_canonicalize_meta()` was last-write-wins on a canonical-key collision (e.g. source keys `100` and `"100"`), silently DROPPING one source's Counter tallies — new `_merge_meta_values()` sums Counter elements and keeps-if-equal/raises-if-conflicting on scalar elements (fail-closed on our own per-sys_id catalogue data), order-independent; `load_ms_pairs_from_db()` also canonicalized pair endpoints via `validate_sys_id()` AFTER the raw `sa < sb` ordering compare, which could TypeError on a mixed int/str pair — canonicalize now happens BEFORE the compare. HIGH (defense-in-depth, writer boundary): `assert_bake_complete()` trusted the cached `missing`/`extra`/`placed_count` scalar fields on `BakeResult`, so a mutated/fabricated result could pass the writer gate even though the real node collection disagreed — it now RE-DERIVES missing/extra/duplicates from the actual `result.nodes` against a new `BakeResult.eligible_ids` field (the real eligible id set, not just its count). MEDIUM: the `REGRESSION_FLOOR` check lived only in `_write_production()`, so `main()`'s `--report` path returned before reaching it — extracted to `_enforce_regression_floor()` and enforced in `main()` before the `--report` early return (still smoke/golden-exempt), kept in `_write_production()` too for direct callers. Tests: added `test_metadata_canonical_key_collision_merges` (same sys_id as both int and str across metadata sources — Counter summed, scalar retained, conflicting scalar raises) and rewrote `test_bake_rejects_node_set_mismatch` to mutate the actual node collection (drop/append/duplicate a node) instead of the now-untrusted cached scalar fields, plus a confirmation that scalar-only mutation no longer bypasses or trips the gate. 15/15 atlas_bake tests pass, smoke bake succeeds, masking scan clean (exit 0), ruff clean; golden fixture bytes unchanged.
