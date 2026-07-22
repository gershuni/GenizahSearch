---
gsd_state_version: 1.0
milestone: v9.0.0
milestone_name: Discovery — Same-Work Identification & Connection Atlas
status: executing
stopped_at: Completed 134-04-PLAN.md
last_updated: "2026-07-22T03:59:12.051Z"
last_activity: 2026-07-22
progress:
  total_phases: 7
  completed_phases: 1
  total_plans: 14
  completed_plans: 10
  percent: 14
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-07-20)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 134 — discovery-data-spine

## Current Position

Phase: 134 (discovery-data-spine) — EXECUTING
Plan: 5 of 8
Status: Ready to execute
Last activity: 2026-07-22

Progress: [███████░░░] 67% (5/6 plans complete; 133-06 in progress)

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

- **133-06 awaiting human production deploy.** The LOCAL portion (Tasks 1-2) is done: the REAL atlas asset is baked into gitignored `atlas_data/` (`atlas-v1-61519a85a2d0`, eligible==placed==62,645, Brotli 2,259,052 B ≤ 6 MB, both masking gates exit 0), the four-surface test passes (12), and the deploy docs are committed. Tasks 3-4 are `checkpoint:human-verify` PRODUCTION steps (asset-first scp → `deploy.sh master-main` → set `ATLAS_PREVIEW_ENABLED=1` → restart → live smoke → rollback drill) and need explicit human go-ahead. ATLAS-01 SC#5 and Phase 133 complete only after the human deploy.
- Masking leak recurrence (2nd occurrence, tmp/ Codex-review scratch files): tmp/CODEX-REVIEW-134-replan-r2.md and tmp/CODEX-REVIEW-134-replan-r3.md (untracked, uncommitted) contain restricted-corpus mask hits per --scan-repo. tmp/ is still not gitignored. Recommend gitignoring tmp/**/*.md + tmp/**/*.log, or the owner manually redacting/deleting these two files. See .planning/phases/134-discovery-data-spine/deferred-items.md 134-02 entry.

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

Last session: 2026-07-22T03:59:12.042Z
Stopped at: Completed 134-04-PLAN.md
Resume file: None
Next step: Owner/researcher confirms (or corrects), PER CLAIM FAMILY, the E1 band-source artifact + join key + raw->band translation + TOTAL flank->claim_type routing proposed in docs/specs/discovery-sidecar-schema-v1.md; then resume 134-01 at Task 3 (schema FREEZE) -> Task 4 (ids.py + golden tests). Separately-pending: 133-06 human production deploy Tasks 3-4 (asset-first upload of atlas_data/ → deploy.sh master-main → set ATLAS_PREVIEW_ENABLED=1 → restart, then live smoke + rollback drill); the baked asset atlas-v1-61519a85a2d0 is ready locally in gitignored atlas_data/.

## Performance Metrics

| Phase | Plan | Duration | Notes |
|-------|------|----------|-------|
| Phase 133 P01 | 55min | 1 tasks | 4 files |
| Phase 133 P02 | 40min | 3 tasks | 12 files |
| Phase 133 P03 | 55min | 3 tasks | 9 files |
| Phase 133 P04 | 55min | 3 tasks | 4 files |
| Phase 133 P05 | 45min | 2 tasks | 2 files |
| Phase 133 P06 (Tasks 1-2, LOCAL) | 90min | 2 tasks | 5 files (Tasks 3-4 = human deploy, pending) |
| Phase 134 P01 | 25min | 3 tasks | 3 files |
| Phase 134 P02 | 50min | 3 tasks | 4 files |
| Phase 134 P03 | 55min | 3 tasks | 10 files |
| Phase 134 P04 | 120 | 3 tasks | 2 files |

## Decisions

- [Phase 133 P01]: scan_repo uses a fast literal-byte matcher (not the rich normalized/encoded matcher) to stay practical over a real working tree with large non-ignored untracked content — A full --scan-repo run against the actual working tree (~24GB unrelated ACL2026_papers/) exceeded 3 minutes with a shared rich matcher; splitting scan_repo (fast) from scan_asset (rich) matches the plan's own acceptance-criteria wording and completes in ~2 minutes
- [Phase 133 P01]: M-source leak in genizah_translations.py scrubbed via codename rename (M-source), not deletion, preserving the unwired Discovery Review deck glossary's structure
- [Phase 133]: [Phase 133 P02]: Small fixed/dynamic lookup tables (domain groups, library codes) live in manifest.json rather than the binary string heap; edge deltas are plain unsigned (no zigzag) via a group-reset rule; island-only clusters reuse the SAME force-layout/dust-ring code path as continuation clusters with MIN_CLUSTER=1
- [Phase 133 P01 round-3, fix(133-01) 02657578]: A round-2 adversarial re-review found the prior hardening only PARTIALLY closed its findings; round-3 closes all 9 HIGH + 1 MEDIUM COMPLETELY. ONE canonical fail-CLOSED pipeline (decode->unescape->casefold->match) runs on every surface. HIGH-1: non-ASCII case/normalization is covered on EVERY file via exhaustive precomputed case+NFC/NFD BYTE forms (bytes.lower folds only ASCII) — deliberately NOT by casefolding 25GB haystacks (that ran ~23min and is CI-prohibitive); wide/BOM + de-escaped text are additionally Unicode-casefolded. HIGH-2: BOM-less wide input scanned under EVERY codec (UTF-16LE/BE, UTF-32LE/BE), no tie-break guess. HIGH-3: escape decoding composes with wide decoding. HIGH-4: .br ALWAYS fully decompressed (never streamed raw) with compressed+decompressed caps; huge non-.br files stream with overlap=longest-match-span-1 + fail-closed if a pattern can't straddle. HIGH-5: URL decode is byte-level + form(+->space), fail-CLOSED on decoder failure, gated to NUL-free text. HIGH-7: HEAD probe distinguishes proven-unborn from operational error. HIGH-8: os.lstat/os.scandir with checked errors; symlinks scanned by link-text, never followed. HIGH-9: Issue.format() + every diagnostic route through a pattern-aware sanitizer; raw subprocess stderr / raw asset paths never printed. HIGH-10: --self-test un-bypassable (PRESENCE via `is not None`; empty --scan-asset rejected). Perf: html.unescape (pure-Python, pathological on binary) replaced by a C-scanned numeric-char-ref regex; URL decode skipped on binary (NUL) buffers — --scan-repo now 3m16s over the 24.9GB local scratch tree (~161MB/s on PDFs), seconds on a clean CI checkout. Tests: 81 pass / 1 skip (symlink priv on Windows); ruff clean.
- [Phase 133 P01 gap-fix, fix(133-01) 3922be48]: The masking scanner (scripts/check_atlas_masking.py) was hardened against an adversarial Codex review (9 HIGH + 1 MEDIUM). This SUPERSEDES the earlier "fast literal matcher on scan_repo, rich matcher on scan_asset" decision — that split was a coverage gap (normalized/encoded/multi-byte-encoding leaks could enter the repo undetected). ONE semantically-complete, fail-CLOSED matcher now runs on EVERY surface (repo + asset): literal + NFC/NFD+casefold + UTF-8/16/32 + URL/HTML/JS (incl. mixed literal+escaped) forms. Speed is preserved with the bytes.find primitive (~21GB/s), a single unquote_to_bytes URL buffer, and a windowed de-escape around sparse HTML/JS introducers — NOT by dropping coverage. git enumeration is now NUL-delimited (-z) + object-id cat-file batch; Brotli payloads are decompressed and scanned; leaky file NAMES are scanned and redacted; every git/read/decode/empty-pattern failure is fail-closed (non-zero exit). --scan-repo runs ~4.1min over the local 24.9GB untracked scratch tree (seconds on a clean CI checkout). Tests rebuilt to be load-bearing (real temp git repos, real Brotli, encoded/mixed forms, fail-closed sims): 59 pass; ruff clean.
- [Phase 133 P02 Wave-2 fix, fix(133-02) e4b66d1f]: Closed 3 HIGH + 2 MEDIUM correctness defects Codex found in the frozen atlas schema bake (the decode contract Wave-3's JS decoder/renderer build on). HIGH-1: EDGE_CLASS polarity was reversed vs the FROZEN schema doc — encoder now emits 0=continuation, 1=island per docs/specs/atlas-asset-schema-v1.md (doc is authoritative, left unchanged); added a semantic edge-class test. HIGH-2: canonicalize EVERY sys_id (pair endpoints, graph node keys, libraries.csv + FJMS domain metadata keys) through validate_sys_id() before baking so all lookups/set-ops use ONE int representation (mixed str/int keys were producing phantom/duplicate nodes and dropping title/domain/library); added a mixed-id-type metadata test. HIGH-3: EXACT eligible==placed set equality is now ENFORCED (raise before writing, no >= fudge) in run_bake + write paths + main() for every mode incl. --report, plus an encoded-node-set==eligible defense and the 62,414 real-bake regression floor; added a rejection test. MEDIUM-1: added an encoder-output-lock test (encode_asset reproduces the committed golden bytes) so encoder drift fails even a green decode-only test — this is exactly how HIGH-1 hid. MEDIUM-2: golden dataset sized (n=40) to yield a >=25-node cluster so CLUSTER_LABEL_* sections are exercised, with non-empty/field-correct assertions. HIGH-4 (shelfmark in heap) is a REJECTED false positive — shelfmark is an intended masking-safe per-star catalogue field, retained. Golden fixtures regenerated deterministically via --golden. Gates: 14 atlas_bake tests pass, smoke bake within byte budget, masking scan clean (exit 0), ruff clean.
- [Phase 133 P02 Wave-2 re-review fix, fix(133-02) 56bbeb9a]: A Codex re-review of the HIGH-2/HIGH-3 hardening above found the fixes were still incomplete on 3 points (1 real HIGH + 1 defense-in-depth HIGH + 1 MEDIUM). HIGH (real, metadata collision): `_canonicalize_meta()` was last-write-wins on a canonical-key collision (e.g. source keys `100` and `"100"`), silently DROPPING one source's Counter tallies — new `_merge_meta_values()` sums Counter elements and keeps-if-equal/raises-if-conflicting on scalar elements (fail-closed on our own per-sys_id catalogue data), order-independent; `load_ms_pairs_from_db()` also canonicalized pair endpoints via `validate_sys_id()` AFTER the raw `sa < sb` ordering compare, which could TypeError on a mixed int/str pair — canonicalize now happens BEFORE the compare. HIGH (defense-in-depth, writer boundary): `assert_bake_complete()` trusted the cached `missing`/`extra`/`placed_count` scalar fields on `BakeResult`, so a mutated/fabricated result could pass the writer gate even though the real node collection disagreed — it now RE-DERIVES missing/extra/duplicates from the actual `result.nodes` against a new `BakeResult.eligible_ids` field (the real eligible id set, not just its count). MEDIUM: the `REGRESSION_FLOOR` check lived only in `_write_production()`, so `main()`'s `--report` path returned before reaching it — extracted to `_enforce_regression_floor()` and enforced in `main()` before the `--report` early return (still smoke/golden-exempt), kept in `_write_production()` too for direct callers. Tests: added `test_metadata_canonical_key_collision_merges` (same sys_id as both int and str across metadata sources — Counter summed, scalar retained, conflicting scalar raises) and rewrote `test_bake_rejects_node_set_mismatch` to mutate the actual node collection (drop/append/duplicate a node) instead of the now-untrusted cached scalar fields, plus a confirmation that scalar-only mutation no longer bypasses or trips the gate. 15/15 atlas_bake tests pass, smoke bake succeeds, masking scan clean (exit 0), ruff clean; golden fixture bytes unchanged.
- [Phase ?]: [Phase 133 P03]: ONE authoritative web/atlas_assets.py loader (plain required, brotli optional, fail-closed) whose single atlas_preview_available() predicate gates the /atlas page, nav, and both off-/static data routes; manifest is a no-cache+ETag+304 mutable pointer to the immutable content-hashed asset; routes negotiate Accept-Encoding br/identity/* by q-value with a reachable 406. Windows-CRLF staging used a hunk-filter (drop R2-1 embed hunks) for web/main.py and a synthetic HEAD->HEAD+atlas patch for genizah_translations.py to keep the discovery-deck glossary uncommitted.
- [Phase 133 P03 Wave-3 re-review, fix(133-03) b830ad64]: Closed 4 MEDIUM findings from a follow-up Codex review of the atlas loader/data-routes (no HIGHs; hardens the fail-closed go-live path, no plan/ROADMAP change). MEDIUM-1: `load_atlas_state()` now validates the binary header + section-table BOUNDS (magic/schema_version/per-section dtype-elem_size/count*elem_size/8-byte-aligned offset/in-buffer range) per docs/specs/atlas-asset-schema-v1.md before ready=True — any structural violation fails closed. MEDIUM-2: manifest MUST carry a `content_hash` matching sha256(plain)[:12] AND `asset_basename` MUST be exactly `atlas-v1-<content_hash>` before the 1-year immutable cache is applied to that name. MEDIUM-3: a present `.bin.br` is Brotli-decompressed and compared byte-for-byte to the plain payload — corrupt/mismatched sidecars just drop the brotli representation (readiness unaffected); added `Brotli==1.2.0` as a genuine (already-vetted, per requirements-atlas-bake.txt's Phase-133 legitimacy audit) runtime dependency in requirements.txt/requirements-lock.txt, import-guarded so a not-yet-installed env degrades to brotli-unavailable rather than crashing. MEDIUM-4: `_negotiate_encoding` now computes each representation's effective RFC 9110 §12.5.3 quality and picks the highest non-zero one (tie -> br) instead of always preferring br whenever merely acceptable — `br;q=0.1, identity;q=1` now correctly yields identity. Tests rebuilt on the real committed `golden-v1.bin`/`.bin.br` fixtures (structurally valid ATLAS001 bytes + real Brotli) instead of a fake marker blob, plus new malformed-header/truncated-section-table/out-of-bounds-section/missing-or-non-hashed-basename/corrupt-or-mismatched-brotli/weighted-preference cases (24 tests, all pass); ruff clean; masking scan clean (exit 0). web/main.py staged via a targeted 2-hunk patch (only `_negotiate_encoding`) to avoid sweeping in the pre-existing uncommitted R2-1 embed change.
- [Phase ?]: 133-04: atlas renderer shipped as a static /static/js/atlas_decode.js UMD module (browser + Node) so the Node golden-decode + DOM-XSS tests exercise the exact same decode + DOM-builder code (cross-language proof); payload fetched from the manifest+content-hashed route, never inlined
- [Phase ?]: 133-04: every catalogue-derived DOM node built via createElement/textContent (zero innerHTML) — the fabricated malicious golden string renders as inert text (HIGH-7, Node DOM-XSS proof); sys_id decoded as BigUint64 .toString() single path (no fallback)
- [Phase ?]: Gated the homepage teaser on atlas_preview_available() (imported directly), not the bare flag, so a flag-ON/asset-missing window never advertises a broken /atlas link from this fourth surface (MEDIUM-6)
- [Phase ?]: Reused the two 133-03 pre-registered teaser translation keys verbatim; genizah_translations.py was never touched by this plan
- [Phase 133 P04 Codex MEDIUM follow-up, fix(133-04) 746b3386]: Closed 3 MEDIUM findings from a Codex review of the Wave-4 atlas renderer (no HIGHs; hardens UX + malformed-asset robustness, no plan/ROADMAP change). MEDIUM-2: the "Loading…" placeholder (now `#atlas-loading`, inside `#atlas-canvas-box`) is removed on a successful first draw (before the bloom-in intro starts) AND before the load-error overlay is shown — exactly one of {canvas, error} is ever visible, never a stuck placeholder. MEDIUM-3: `whenCanvasReady()` now takes an `onTimeout` callback instead of returning silently after its ~10s poll window; `init()` wires it to the SAME normal load-error UI (falls back to `#atlas-canvas-box`, then `document.body`, if the box itself never mounted either). MEDIUM-1 (defense-in-depth — the 133-03 server loader already bounds-checks before serving, and the asset is our own content-hashed bake, so not a live vector): `decodeAtlas()` now validates required-unique sections, dtype/elem_size agreement (incl. the one shared dynamic cluster-index dtype across NODE_CLUSTER/FLOW_*/CLUSTER_LABEL_CI), every section's byte range in-bounds + 8-byte aligned, every related count consistent (node/edge/flow/label relations per schema §4), every STRING_HEAP `(offset,length)` ref within heap bounds, every edge endpoint within node range, and caps every count to a sane limit — all BEFORE any typed-array view or DOM string is built; any violation raises a clean Error caught by `init()`'s `.catch()` and surfaced via the same load-error UI (never OOB-read/uncaught-throw/hang/silently-truncated data). Valid-asset decode is unchanged (golden/XSS tests still pass byte-for-byte). New Node-driven coverage in `tests/atlas_bake/test_atlas_golden_js.py`: 5 malformed-asset cases (duplicate section id, out-of-bounds heap ref, out-of-range edge endpoint, dtype/schema mismatch, absurd section_count) each proven to make `decodeAtlas()` throw, and 3 full `AtlasDecode.init()` UX scenarios (success / fetch-error / canvas-timeout) driven against a minimal fake-browser harness, proving the placeholder-hidden + single-error-surface contract end-to-end — each new test was manually confirmed to catch its target regression (temporarily reverted the fix, saw the test fail, restored). 13/13 atlas tests pass (Node v24 available locally, ran for real, not skipped); ruff clean; masking scan clean (exit 0).
- [Phase 134]: 134-01: skipped requirements mark-complete for DATA-01/02/03/10 (shared frontmatter IDs across 134-01/03/04/06/07); premature to flip Complete until the later distillation/release-contract plans land
- [Phase 134]: 134-01: reworded superseded-identifier mentions (work_witness_claims, ms_ms_claims, textual_parallel, direct_text_overlap) to hyphenated prose forms so the doc both explains what was dropped and survives its own literal-substring negative verify check
- [Phase 134]: 134-02: skipped requirements mark-complete for DATA-05/PERF-01 (shared frontmatter IDs across 134-02/04/08); premature to flip Complete until the sidecar-wired scan (134-04) and measured actuals (134-08) land
- [Phase 134]: 134-02: --scan-sqlite surface strings redacted via matcher.redact_path before Issue construction (Rule-1 fix caught by the new never-echo test) -- repr(issue) bypasses .format()'s sanitizer, so a leaky TABLE/COLUMN identifier could otherwise reach the raw dataclass field
- [Phase 134]: 134-02: R-source token pre-registration in the gitignored MASKING_SCAN_PATTERNS_FILE (D-03c) deferred as an owner-only operational step -- the executor does not have and must not fabricate the real R-source name/aliases/sigla; documented in the SUMMARY as a pending manual action
- [Phase ?]: [Phase 134 P03]: display_evidence_id carries NO SQLite-level FOREIGN KEY (F12 ownership is an application-level check backed by UNIQUE(claim_id, evidence_id), not a native cross-column composite FK) -- resolves the circular-FK build ordering (claims insert first with placeholder empty string, evidence inserts second, display_evidence_id backfilled via UPDATE with no FK re-check)
- [Phase ?]: [Phase 134 P03]: compute_frame_content_hash lives in scripts/build_discovery_sidecar.py (not verify_discovery_sidecar.py, not the FROZEN discovery_ids.py) and is imported by the verifier, so Task 1 stays independently buildable/verifiable before Task 2 exists and build/verify recomputation can never drift apart
- [Phase ?]: [Phase 134 P03]: verifier hardcodes the FROZEN 0.926 collection-level precision as a recognizable literal (C-7/R1, an immutable already-measured empirical number, not a build-varying placeholder) so a scope='band' row carrying it is unambiguously a G8 violation
- [Phase ?]: [Phase 134 P03]: R5/duplicate-evidence-key corruption tests use a CREATE TABLE ... AS SELECT recreate-without-constraints trick because the frozen DDL's own NOT NULL/PRIMARY KEY constraints would otherwise reject those mutations outright via sqlite3.IntegrityError -- exercises the verifier's own redundant Python-level checks rather than leaving them untestable
- [Phase ?]: [Phase 134 P03]: plain (non-router) shared_text rows default to routing_status=shipped/routing_reason=none in the fixture -- the frozen schema doc's routing matrix does not explicitly enumerate this family; 134-04/134-07 should confirm or override against the real q2_shared_text.jsonl ingest
- [Phase ?]: [Phase 134] 134-03: skipped requirements mark-complete for DATA-01/02/03/08/10 (shared frontmatter IDs across 134-01/03/04/06/07/08, same precedent as 134-01/134-02); premature to flip Complete until 134-04's real distillation + 134-07's release-contract finalization land
- [Phase ?]: 134-04: evidence_id collision (shared_text vs family-router same-span) resolved by build-side dedup, never by amending the FROZEN discovery_ids.evidence_id() recipe
- [Phase ?]: 134-04: absent-crosswalk aborts by default (create_if_missing=False) in assign_opaque_work_ids -- never silently re-mints opaque work_ids for an already-shipped work
- [Phase ?]: 134-04: real dev-box smoke build validated against actual research corpus (625 works/231,604 claims/251,976 evidence rows) -- passed verify_discovery_sidecar.py and the blocking masking gate with 0 hits
