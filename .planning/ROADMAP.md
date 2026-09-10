# Roadmap: GenizahSearch

## Milestones

- **v1 External Data Integration** -- Phases 1-7 (shipped 2026-02-07)
- **v5.6.0 Desktop Parity & PGP Integration** -- Phases 8-12 (shipped 2026-02-09)
- **v5.7.0 Responsa Search** -- Phases 14-17 (shipped 2026-02-10)
- **v5.7.2 Cleanup, Normalization & Sections** -- Phases 18-21 (shipped 2026-02-11)
- **v5.7.3 Pending Corrections Visibility** -- Phases 22-24 (shipped 2026-02-11)
- **v5.8.0 FJMS Integration** -- Phases 25-28 (shipped 2026-02-15)
- **v5.9.0 Multi-Source Image & Metadata Integration** -- Phases 29-34 (shipped 2026-02-16)
- **v6.0.0 Local Data Architecture** -- Phases 35-40 (shipped 2026-02-22)
- **v6.1.0 Catalog Browse & Navigation** -- Phase 41 (shipped 2026-02-27)
- **v6.5.0 Search UX & Filtered Search** -- Phases 42-46 (shipped 2026-03-14)
- **v7.0.0 Fragment Puzzle** -- Phases 47-52 (shipped 2026-03-17)
- **v7.1.0 FIST Gap Fill** -- Phase 53 (shipped 2026-03-19)
- **v7.6 Search Refinement & Scholarly Joins** -- Phases 54-57 (shipped 2026-03-31)
- **v7.7 Volume-Aware Browse** -- Phases 58-61 (shipped 2026-04-01)
- **v7.8 Structural Foundation** -- Phases 63-66 (shipped 2026-04-15)
- **v7.9 Decomposition** -- Phases 67-76 (complete 2026-04-17)
- **v7.10 Search API** -- Phases 77-83 (shipped 2026-05-05)
- **v7.11 CUDL Coverage & Synthetic Inventories** -- Phases 84-86 (shipped 2026-05-12)
- **v7.12 Multitenant Architecture (Path B)** -- Phases 87-92 + 92.1 + 92.2 + promoted 999.1/999.4 (shipped 2026-05-18)
- **v7.13 Research-Grade Downloads & PGP Filter** -- Phases 93-94 (shipped 2026-05-21)
- **v7.14 My Library — Local Document Search** -- Phases 95-98 (shipped 2026-05-24; closed 2026-05-27)
- **v7.15 My Library Visual** -- Phases 99-101 (shipped 2026-05-28). See `milestones/v7.15-ROADMAP.md`
- **v7.16 Hebrew PDF Text Quality** -- Phase 102 + no-phase quality work (shipped 2026-06-01). See `milestones/v7.16-ROADMAP.md`
- **v8.0.0 Dicta Rebrand & Joins Lab** -- BRAND (no-phase) + Phases 103, 105 (folded from v7.17; Phase 104 → EXP-F3) + Phases 106-110 Joins Lab (shipped 2026-06-09; closed 2026-06-11). Component B (JSA-01/02/03 + JWB-05) + web Joins Lab UI deferred post-v8.0.0. See `milestones/v8.0.0-ROADMAP.md`
- **v8.1.0 Desktop Telemetry** -- Phases 111-116 (shipped 2026-06-16; closed 2026-06-16). See `milestones/v8.1.0-ROADMAP.md`
- ✅ **v8.2.0 Web Joins Lab, FGP Transcriptions & Hebrew Search** -- Phases 117-121 (shipped 2026-06-23, both apps)
- ✅ **v8.3.0 God-File Decomposition + Search & Browse UX** -- Phases 122-129 (shipped 2026-06-29, both apps; closed 2026-06-30). Decomposition (122-127, zero behavior change) + SEED-025 Space-scroll + SEED-026 library filter. See `.planning/milestones/v8.3.0-ROADMAP.md`.
- ✅ **v8.4.0 Dual-Mode Library Filter** -- Phases 130-131 (shipped 2026-07-01, both apps; closed 2026-07-01). Evolved the v8.3.0 inclusion-only allowlist into a dual-mode (Show-only / Hide) library filter persisted across searches, at full web + desktop parity. Evolution of SEED-026. See `milestones/v8.4.0-ROADMAP.md`.
- ✅ **v8.4.1 Public API Dual-Mode** -- Phase 132 (shipped 2026-07-01, web; closed 2026-07-01). The public-API half of the dual-mode filter (DMF-11): `library_filter_mode` (include/exclude) on `POST /api/search` + `/api/parallels`, backward-compatible; skill clients gained `--library-mode`. Web point-release on the 8.4.0 tree (no version.py bump/tag). See `milestones/v8.4.1-ROADMAP.md`.
- ✅ **v9.0.0 Discovery — Same-Work Identification & Connection Atlas** -- Phases 133-136.2 built; 137-149 never ran (shipped web 2026-08-16, desktop 2026-08-26; **closed 2026-09-10 as an override_closeout**). **Read [`milestones/v9.0.0-MILESTONE-AUDIT.md`](milestones/v9.0.0-MILESTONE-AUDIT.md) before planning any follow-on work** — the milestone's own planning record froze on 2026-08-21 while 339 commits and two releases (v9.1.0, v9.2.0) shipped after it. See `milestones/v9.0.0-ROADMAP.md`.

---

## Phases

No active milestone. Start the next one with `/gsd-new-milestone`.

The v9.0.0 exit audit proposes an eight-phase successor, ranked by what gates what:

| # | Proposed phase | Goal |
|---|---|---|
| 1 | Ratify reality | The planning record stops contradicting the product |
| 2 | License the default | The passage-search default rests on something stated honestly |
| 3 | Append safety | No reference append can silently drop identifications |
| 4 | Gates that guard egress | The masking-adjacent gates actually run and can fail |
| 5 | Judgments: decide, then act | The reviews schema is hardened or accepted in writing |
| 6 | Honesty completion | Every claim-bearing surface carries its caveat |
| 7 | Passage surface debt | Pay for what shipped ahead of its gates |
| 8 | Accessibility & observability | A11Y-02 on the atlas; OBS-01/02 signals exist |

Full reasoning, the 81 verified findings each phase absorbs, and an explicit cut list
(Phase 138 leads queue, Phase 139b atlas drill-down, Phase 147) are in the audit.

---

## Archived Milestones

<details>
<summary>✅ v9.0.0 Discovery — Same-Work Identification &amp; Connection Atlas (Phases 133-136.2) — SHIPPED web 2026-08-16 / desktop 2026-08-26; CLOSED 2026-09-10</summary>

See: `.planning/milestones/v9.0.0-ROADMAP.md` — and, before trusting it,
`.planning/milestones/v9.0.0-MILESTONE-AUDIT.md`.

**6 phases built (133, 134, 135, 136, 136.1, 136.2), 45 plans, 108 tasks. 14 phases (137-149)
never ran.** Folded the SEED-029 corpus-wide same-work text-reuse map into genizahsearch.com as a
banded, masked, honesty-gated discovery module: the static Visual Atlas at `/atlas`, a masked
versioned `discovery.db` sidecar behind an async fail-closed service, a pre-registered tier-A
precision measurement (**0.9382, 95% CI [0.9084, 0.9644]**, owner-graded catalogue-blind against a
0.85 floor), the browse connections panel, the corpus-wide `/computed-identifications` findings
page, a frozen relation-precedence matrix, the `/start` launchpad, beta community identification
reviews, and an xlsx export. `DISCOVERY_ENABLED` went on in production 2026-08-08 — **ahead of the
REL-01 gate it was supposed to wait for**, with five gate items waived in writing afterwards.

**Closed as an `override_closeout`.** The planning record stopped tracking reality on 2026-08-21:
339 commits, the 98-commit PR #324 merge, and two tagged releases (v9.1.0 multi-witness RRF
fusion, v9.2.0 citations/PGP/repairs) landed with no phase number. Phase 140 — the bookkeeping
phase created to prevent exactly that, and sequenced first — never ran. 57 open artifacts were
acknowledged as deferred (see STATE.md § Deferred Items); ~26 were inherited from v7.14-v8.4.0.

**The exit audit is the authority on what actually shipped**: 24 units reconciled against the
repository, every finding put through two adversarial verifiers, 104 candidates in and **81 out**
(3 critical, 19 high, 33 medium, 26 low). The three criticals — the passage default flipped
without its pre-registered test, Phase 140 never running, and no gate against a repeat of the
103-identification append loss — are the seed of the next milestone.

Phase dirs archived to `.planning/milestones/v9.0.0-phases/`.

</details>

<details>
<summary>✅ v8.4.1 Public API Dual-Mode (Phase 132) — SHIPPED 2026-07-01, web; closed 2026-07-01</summary>

See: .planning/milestones/v8.4.1-ROADMAP.md

1 phase (132), 3 plans. The public-API half of the dual-mode library filter (DMF-11) — the API counterpart to v8.4.0. `POST /api/search` + `/api/parallels` accept an optional `filters.library_filter_mode` (`include` / `exclude`) alongside `filters.library`. One shared `FiltersModel.library_filter_mode` field (`Optional[Literal['include','exclude']]`, default=None); `exclude` resolves to the complement (single-pass `resolve_library_complement_sys_ids`) via `run_in_executor`, intersected into `restrict_sys_ids`. Byte-for-byte backward-compatible (`default=None` + `model_dump(exclude_none=True)`; omitted = include — Codex R1 caught that `default='include'` would break every caller's echo); invalid mode → 400 via Pydantic `Literal` + `extra='forbid'`. Skill clients (`search.py`/`parallels.py`) gained `--library` / `--library-mode`. Web-only point-release on the 8.4.0 tree (no `version.py` bump / git tag); live-verified on the real 255K corpus. Phase dir archived to `.planning/milestones/v8.4.1-phases/`.

</details>

<details>
<summary>✅ v8.4.0 Dual-Mode Library Filter (Phases 130-131) — SHIPPED 2026-07-01, both apps; closed 2026-07-01</summary>

See: .planning/milestones/v8.4.0-ROADMAP.md

2 phases (130-131), 13 plans. Evolved the v8.3.0 inclusion-only library allowlist (SEED-026) into a **dual-mode** UI filter — **Show-only** (allowlist) *or* **Hide** (denylist) — persisted so each intent survives across searches, at full web + desktop parity. **Phase 130** (lead) settled the shared `{'mode','codes'}` state shape + `safe_storage` persistence + legacy-allowlist migration + edge-state sentinels + 3-state button on web `/search`. **Phase 131** mirrored it on the desktop catalog `LibraryFilterDialog`, web Browse-by-Identification, and a NEW web `/parallels` control (scoping via `restrict_sys_ids`), plus UAT-driven per-library counts + type-to-find + sort + searchable English codes. `'LOCAL'` guard (DMF-10) held on every surface; 131 SECURED 24/24. Web deployed; desktop installer `GenizahSearchPro_V8.4.0_Setup.exe` published to GitHub Release latest @ `v8.4.0`. DMF-13 (zero-count exclusion) Partial on non-`/search` surfaces (behaviorally safe, fail-open). The public-API piece (DMF-11) shipped as v8.4.1 (above). Phase dirs archived to `.planning/milestones/v8.4.0-phases/`.

</details>

<details>
<summary>✅ v8.3.0 God-File Decomposition + Search & Browse UX (Phases 122-129) — SHIPPED 2026-06-29, both apps; closed 2026-06-30</summary>

See: .planning/milestones/v8.3.0-ROADMAP.md

8 phases (122-129). Two strands shipped together as the public 8.2.2→8.3.0 release: (1) god-file decomposition (122-127) — split genizah_app.py + genizah_core.py into cohesive shared/+desktop/ modules behind permanent re-export facades, zero behavior change; (2) Search & Browse UX (128-129) — SEED-025 Space-key results scroll + SEED-026 library filter (web /search + Browse-by-Identification + desktop catalog + filters.library API), both apps. Also shipped: SEED-017 Joins-Lab viewer rotate/fullscreen, SEED-024 desktop Joins-Lab parity + XLSX export, SEED-015 desktop image NLI breaker.
</details>

<details>
<summary>✅ v8.2.0 Web Joins Lab, FGP Transcriptions & Hebrew Search (Phases 117-121) — SHIPPED 2026-06-23, both apps</summary>

See: .planning/milestones/v8.2.0-ROADMAP.md

5 phases (117-121). Ported the desktop Joins Lab (Component A) to the web at `/joins-lab` at full parity — anchor pane + line-by-line builders for both leaf sides + deduped candidate grid/table + side-by-side Compare + Visual Similarity toggle + Add-as-Join/Puzzle/list; bilingual EN/HE + RTL, no login, server-side per-session state via `safe_storage`. Bundled beyond scope: FGP transcriptions go-live (both apps), SEED-006 Hebrew/Judeo-Arabic search, Responsa-operators-over-My-Library (desktop). Phase dirs archived to `.planning/milestones/v8.2.0-phases/`.

</details>

<details>
<summary>✅ v8.1.0 Desktop Telemetry (Phases 111-116) — SHIPPED 2026-06-16, closed 2026-06-16</summary>

See: .planning/milestones/v8.1.0-ROADMAP.md

6 phases (111-116), 20 plans, 32 tasks. Opt-in, privacy-preserving desktop telemetry for "Dicta Genizah Search Pro" — anonymous usage analytics, crash reports, and per-session performance summaries flow to the shared web PostHog project (id 134161, EU), identity-aligned with the web app (logged-in users → same Supabase `user.id`), split by `platform=desktop`. Default OFF until the user consents via a bilingual first-run dialog; never transmits search content or My Library data. Also bundled: desktop "Public API & AI Tools" advertising and web Search API enhancements (quick task 260616-p9x) + the `platform=web` super-property.

</details>

<details>
<summary>✅ v8.0.0 Dicta Rebrand & Joins Lab (Phases 103, 105 + 106-110) — SHIPPED 2026-06-09, closed 2026-06-11</summary>

See: .planning/milestones/v8.0.0-ROADMAP.md

7 phases — 103 + 105 (folded from the v7.17 cycle) + 106-110 (Joins Lab Component A). 25 requirements satisfied (BRAND 2 + LEXP 7 + EXPUX 4 + JWB 9 + COMP-LOC 2 + EXP-F3 1). Desktop Joins Lab: shared core (`shared/joins_lab.py`) + anchor pane + line-by-line query builders for both sides of the leaf + deduped candidate grid/table + side-by-side Compare + pairwise→group join model + Visual Similarity toggle. Component B (JSA-01/02/03 + JWB-05) and web Joins Lab UI deferred.

</details>
