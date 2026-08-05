# Phase 136 — Flag-on readiness attestation

**Written:** 2026-08-05 · **Plan:** `136-19` (wave 11, the last) · **Requirements:** NOVEL-02,
VIS-01, PANEL-01, PANEL-02

**This is the artifact the release gate reads instead of re-deriving Phase 136.** It records what
is done and what evidences it, how the four late owner rulings were dispositioned, what still gates
the public flip and who owns each of those, what is known and accepted, and — in its own section,
deliberately as prominent as the rest — **what this evidence does not cover.**

> **THE FLAG IS NOT FLIPPED AND MUST NOT BE FLIPPED BY THIS PLAN.** Production carries the code
> with `DISCOVERY_ENABLED` unset, so `discovery_available()` is False and the panel, the nav entry
> and `/computed-identifications` all clean-hide. Flipping is the owner's decision, at Phase 139,
> after the items in § 3 are closed. **Deployed and gated is not live**; the difference is the
> entire posture of this milestone.

**Why this document is sceptical of itself.** Phase 136 produced **six** instances of one defect —
a gate that reported success without performing its check. A masking test that skipped when its
pattern file was absent; a browser-DOM capture that exited 0 having run none of its interactions; an
executor-dispatch assertion that measured zero dispatches; a benchmark whose "non-empty" check
counted the rows of a `COUNT(*)`; a benchmark skip list that called a reachable state unreachable
and hid a real user-facing crash; and a masking capture whose "derived" coverage was a naming
convention, missing 78 executable lines including every surface painted by a click. Three were found
by an external reviewer, three by CI or by direct measurement. **All six failed in the same
direction: toward false confidence.** A seventh was found and closed while writing this plan (§ 5).
An attestation that reads as unconditionally clean, in that phase, would be worth less than one that
names its own limits.

---

## 1. Done, with evidence

One row per ROADMAP § Phase 136 success criterion **this phase owns**. Criteria 3 and 4 were moved
out to Phase 136.1 on 2026-08-02 (PANEL-03, WORK-01, WORK-02) and are listed for completeness with
their new owner.

| # | Criterion (abbreviated) | State | Evidence |
|---|---|---|---|
| 1 | The ONE authorized rebuild lands in production, flag-OFF, before any surface work | **MET** | `136-REBUILD-GATES.md` (full battery, 2026-08-03; `frame_content_hash` byte-identical to the pre-rebuild pin, so membership did not change and the rebuild is additive) · `136-COMPATIBILITY-ATTESTATION.md` (identity table, all gates pass) · `136-13-PLAN.md` · `136-05-SUMMARY.md` (the preservation gate pinned BEFORE the rebuild) · `136-12-SUMMARY.md` (twelve new registered release-verifier checks) |
| 2 | The browse "Computed identifications" panel, two buckets, match framing, no percentage, no review badge | **MET, built and gated** | `136-15-SUMMARY.md` (pure display model, 16-state arbitration cross-product) · `136-17-SUMMARY.md` (body renderer + browse seam, four service states, browse byte-for-byte unchanged when flag-off; cold p95 **4.2 ms** / warm **0.1 ms** against the 150 ms browse cap over the 20 heaviest pages) · `136-21-SUMMARY.md` (work expansion; largest work `w000112` at 5,684 distinct witness units; count latency p50 1.8 ms / p95 39 ms) · `tests/render_smoke/test_panel_render_smoke.py` (313 passed / 1 skipped; 12 UI-emitting functions, 245 of 246 required lines executed, one contract-unreachable exemption) |
| 3 | On-demand evidence view | **MOVED OUT** | Phase 136.1 criterion 1–2 (PANEL-03). Not owned here; nothing is claimed for it. |
| 4 | `/work/{id}` witness-map page | **MOVED OUT** | Phase 136.1 criterion 3 (WORK-01/02). Not owned here. |
| 5 | Findability: neutral-title search, reachable from the panel, from `/catalog-browse`, and from a new corpus-wide findings page with its own nav entry | **PARTIALLY MET — see the honest split below** | `136-16-SUMMARY.md` (page shell, route, nav entry, caveat, modes, filter bar, result bar, pager) · `136-18-SUMMARY.md` (three row units, the ruling-U headline, novelty badge, coverage clause, bucket name; 155 tests / 359 gate calls) · `136-09-SUMMARY.md` + `136-DOMAIN-CURATION.md` (the domain cascade's curated `works.genre` artifact and the author alias map) |
| 6 | The novelty axis is live and structurally orthogonal to the tier: ten-value shade, fail-closed, masked provenance, never feeding band assignment/ranking/precision/styling | **MET for what shipped; the production LLM run is UNAUTHORIZED and did not run** | `136-04-SUMMARY.md` + `136-NOVELTY-RUN.md` (the ruling-I re-measurement WAS executed against the real sidecars and the real pinned model; 60 shade cases, 78.3% agreement; the ~$301 production run was NOT executed) · `136-GATE1-DECISIONS.md` §§ K, L (ruling K keeps the run unauthorized; ruling L drops `divergence_correctness` from the model's job) · `136-12-SUMMARY.md` (the shade written on BOTH evidence families with masked provenance) · `shared/discovery_novelty.py::masked_provenance_label` |
| 7 | No precision percentage reachable from any surface; the methods page rewritten qualitatively; no human-review badge | **MET** — **one of the two flag-on gating items** | `136-02-SUMMARY.md` (the `web/pages/help.py` BAND-05 rewrite: zero percentage, interval, weighted estimate or strata table in either language) · `tests/render_smoke/test_help_methods_render_smoke.py` · `tests/render_smoke/discovery_honesty_gate.py::assert_surface_honesty` (SIX detectors — the five-detector `assert_discovery_honesty` is deliberately NOT what a surface calls) · `docs/specs/discovery-band-labels-v1.md` Amendment 2026-08-02 · D-13f: no surface claims human review |
| 8 | Every surface hides cleanly flag-off / sidecar-absent, stays inside the PERF-01 budgets, and passes the masking scan on rendered output, JSON payloads, copy/export paths and error paths | **MET** — **the other flag-on gating item** | `tests/render_smoke/test_discovery_masking_sweep.py` (this plan; § 2 below) · `136-20-SUMMARY.md` (the VIS-01 audience boundary and the extended readiness contract; 32 tests) · `docs/specs/discovery-budgets.md` § 4.4 · `scripts/bench_discovery.py` (483 combinations enumerated / 337 measured / 146 skipped / **0 over cap** on the 375 MB public artifact, built through the SHIPPED `_build_findings_query`) |

### Criterion 5, split honestly

* **Findings page with its own nav entry — MET.** `/computed-identifications` ships, gated on the
  single `discovery_available()` predicate (route AND nav entry, `web/main.py`).
* **Reachable from the panel — MET.** The panel names the works; titles render as plain text, not
  links, because `/work/{id}` moved to 136.1 (see § 4).
* **`/catalog-browse` carries computed identifications — NOT BUILT IN THIS PHASE.** It moved to
  Phase 136.1 criterion 4 in the 2026-08-02 re-scope. **The ROADMAP's Phase-136 criterion 5 text was
  never updated to say so** — it still reads as though this phase owns it. Recorded as a records
  discrepancy, not resolved here.
* **"Works are findable by neutral title (bilingual normalization + alias/duplicate handling)" —
  NOT BUILT as a search.** What exists is the findings page's **work facet**, cross-filtered by
  domain and author, with ruling-R curated display titles and the 136-09 author alias map behind the
  author facet. There is no neutral-title search box. The same re-scope is the reason, and the same
  ROADMAP text was not updated.

### The two flag-on gating items

Both are complete.

| Gating item | State | Evidence |
|---|---|---|
| The methods-page qualitative rewrite (D-06a / criterion 7) | **COMPLETE** | `136-02-SUMMARY.md`; `web/pages/help.py`; the D-06a limitations paragraph is digest-pinned in `test_panel_render_smoke.py` so the owner-approved wording cannot drift silently |
| The cross-surface masking sweep (criterion 8 / NOVEL-02) | **COMPLETE** | `tests/render_smoke/test_discovery_masking_sweep.py`, 31 tests; § 2 below |

**The masking item may be recorded as complete only because both of its preconditions held**, and
that is not a judgement call: `test_discovery_masking_sweep.py::masking_readiness` is the ONE
function that decides it, it requires a real pattern set to have loaded AND both database scan modes
to have run, and it refuses every partial outcome. An unavailable `MASKING_SCAN_PATTERNS_FILE` makes
the sweep FAIL and the item NOT MET — it is never a skip and never a pass. That is the direct
remedy for the first of the six defects above.

---

## 2. The masking sweep — what it covers, measured

Four egress classes, each written to its own capture file outside the working tree and scanned by
the existing DATA-05 gate (`scripts/check_atlas_masking.py`) rather than by a reimplementation of
its patterns.

| Class | Scanned | Measured |
|---|---|---|
| Rendered output | Panel + findings page, both languages, all four service states, all three row units, both buckets, plus every surface painted only after a click | **2,213,790 chars**; all 48 findings state combinations asserted present by name; **every executable line** of every UI-emitting function of `web/components/findings_rows.py` and `web/pages/findings.py` executed, with 5 exemptions that each paint nothing |
| JSON payloads | All **9** enveloped reads either surface consumes, each dumped both raw-UTF-8 and `\uXXXX`-escaped | **23,063 chars**; the read set is DERIVED by parsing every `web/` module that imports `web.discovery`, so an unscanned new read fails by name |
| Copy / export | The link targets a reader can copy, plus a derived inventory of the clipboard/download/export APIs the surfaces do **not** use | **470 chars**, **1** link target (`/browse?sys_id=…`); the absence of a clipboard/download path is derived from the source and asserted, not assumed |
| Error paths | 6 panel failure modes + 9 findings failure modes + forced REAL log lines + rendered error states, both languages | **27,566 chars** |

**Four positive controls, one per class**, each seeded with a marker generated at test time from the
pattern file, each asserting the SPECIFIC expected failure — the nonzero exit, the file the scan
named, and the three files it must not have named. What each raised is recorded in
`136-19-SUMMARY.md`.

**Databases: six, all clean, all with BOTH modes.** `--strict --scan-repo --scan-asset <db>
--scan-sqlite <db>` over every artifact any `discovery_data/manifest*.json` resolves. The
manifest-derived check found **three** the executor's remembered list had missed. The clean runs are
shown NON-VACUOUS: a value read out of the deployed artifact and fed back as the whole pattern set
is reported (11 / 26,480 / 1 hits from a cell, a deep column and a table name respectively), so the
48-second clean walk is a walk that happened. And `--scan-asset` alone is proved INSUFFICIENT by
construction: a value straddling a SQLite overflow-page boundary is invisible to the byte scan and
visible to the cell scan, with a non-straddling offset as the control that the byte scan works at
all.

---

## 3. Still gating the public flip — owned elsewhere, resolved NONE of them here

| # | Item | Owner |
|---|---|---|
| G1 | **D-06b — whether the public projection may inherit the all-source measurement, or needs its own pre-registered estimand.** CERT-01 measured the all-source population; the public projection is a structurally different one, and `135-09-CERT01-MEASUREMENT.md` records the Sefaria-only figure as descriptive, not pre-registered. Either pre-register and measure a public estimand before the public bake, or formally amend REL-01. | **Phase 139** (`136-CONTEXT.md` D-06b) |
| G2 | **REL-01 / CERT-02's "tier A goes public WITH its measured number" clause, which conflicts with D-06's no-numbers posture.** Must be amended or satisfied at the release gate. Phase 136 publishes nothing, so nothing is violated today. | **Phase 139** (`136-CONTEXT.md` D-06a "Still owed at 139") |
| G3 | **The correction and retraction policy**, which lost its home when the curated-surface exception was declined. — **RESOLVED by owner ruling 2026-08-05; no longer gates the flag. See the note below this table.** | ~~Phase 139~~ → owner, 2026-08-05 |
| G4 | **VIS-02's positive control and the public/private row-count reconciliation as a release-gate check.** | **Phase 139** (`136-CONTEXT.md` § Deferred) |
| G5 | **The ruling-T "more matches" browser check is recorded NOT MET in `136-16-SUMMARY.md`, while CI reports it green.** The `findings-browser-check` job passed for the first time on 2026-08-04 (CI run 30931268195) after four runs and three real defects, two of them product defects. The phase's own record was not updated, and `test_the_findings_deploy_is_blocked_until_the_browser_check_is_recorded_MET` reads that record — so the findings deploy gate currently reads BLOCKED on a criterion that is met. **This is a records correction, and it is deliberately not made here**: it should be made by whoever can verify the CI run, not by an executor quoting a dispatch. Until then the gate reads stale in the safe direction. | **Release gate / owner** |
| G6 | **`works.genre` is NULL on 58 public (181 private) works reachable through the review opt-in, and the release verifier FAILS on it today.** Owner decided 2026-08-04 to CURATE rather than backfill `Unassigned`. Worklists generated. | **A curation pass before the next bake** (`docs/OPEN_ISSUES.md`) |

### G3 — resolved by owner ruling, 2026-08-05

**A per-item retraction mechanism was the wrong tool, because the premise behind it was wrong.**
The policy was scoped as "a reader finds *an* error" — incidental, rare, worth taking down one row
at a time. The owner corrected this: wrong attributions are **systematic, not incidental**. The
measurement agrees — CERT-01's weighted precision is 0.9382 overall but the per-stratum spread runs
**1.000 down to 0.471**, so in the weakest stratum roughly half are wrong. Add the 12,664 rows that
contradict the catalogue (23.6% of the corpus) and the 25,872-row second pool, which by definition
did not meet the evidence rule. Being frequently wrong is not a failure mode on this surface; it is
the **stated premise**, and the tiering, the pools and the permanent caveat are what make it honest.

A withdrawal list against a systematic error rate would mean thousands of manual takedowns fighting
a problem that a better bake fixes wholesale. **The correction mechanism at scale is the next bake,
not a takedown list.** The ruling therefore splits what the old framing conflated:

* **Reports feed the next bake.** A reader who spots an error is supplying validation signal, worth
  far more aimed at the next artifact than at one row. `mailto` is sufficient — **owner ruling: "email
  us is enough"** — with the finding's id and the sidecar version prefilled, so a report is
  reproducible against the exact artifact that produced it. No schema, no moderation queue, no
  retraction path.
* **Withdrawal is reserved for a different class entirely** — harmful, defamatory, or a disclosure
  that should not have shipped. That list is short and its policy is not this one. "This match is
  wrong" is expected and must never route here.
* **The surface already carries most of the burden.** The permanent caveat says every row is a text
  match found by software and not a reviewed identification; the pools and tiers say where the
  evidence was thin. A reader who finds a wrong match in the second pool has found the system
  working exactly as described.

**Consequence for the flag:** G3 no longer gates it. What remains is a sentence saying what to do on
finding an error and where reports go, plus the prefilled `mailto` — an hour of work, not a phase.

---

None of G1, G2, G4, G5 or G6 is resolved by this attestation, and none should be read as resolved by the phase
being otherwise complete.

---

## 4. Known and accepted

* **The containment residue.** Stated exactly as the methods page states it: *"A work that contains
  another work's text can absorb matches that really belong to the contained work — a blessing or
  prayer embedded inside a larger legal code is the live example — so a small minority of the main
  pool, a low single-digit share, is misattributed for this reason. A two-page agreement is often the
  two sides of one physical leaf rather than two independent leaves. A composition date can rule out
  an implausible direction of borrowing, but it cannot settle identity by itself."* (`web/pages/help.py`,
  `_LIMITATIONS_TEXT`, digest-pinned.) The share is stated **in words** and never as a measured
  percentage; that is the D-06a exception and it is the only one.
* **The human-review provenance question keeps the badge off.** The provenance of the 121
  `human_confirmed` rows is unestablished (`e1_adjudicated_a.jsonl`, 174 individually-adjudicated
  cards; "internal deck vs owner" left open at Phase 134's closeout). Per D-13f no surface claims
  human review, and none does. If the owner turns out to have graded them, the badge can return with
  sourced wording and a dated amendment to `discovery-band-labels-v1.md` § 2.
* **Work titles render as plain text, not links, until Phase 136.1 ships `/work/{id}`.** Deliberate,
  and there is no dead end at either stage.
* **Coverage routing demotes correct identifications.** `low_coverage` accounts for 100,159
  review-only display rows; on Moss. V,374 it demoted six correct Rashi-on-Megillot identifications.
  Not fixable in Phase 136 — it is direct evidence for the deferred witness-vs-quoter / coverage work
  in discovery-v2.1.
* **The launch-scope reconciliation was REPORTED, not resolved** — by design. Gate 9 of the 136-13
  battery found the two ways of computing the public scope disagree on **36,989 of 297,415 evidence
  rows (12.4%)**; ruling S ships the two-axis conjunction and records `_vis01_shortcut` as a known-stale
  rule (see § 5 rulings).

### The launch figures — recorded WITH the artifact and audience that produced them

**These are properties of one artifact, not project constants, and they will move on the next
rebuild.** The deployed public artifact and the private rebuild already disagree (Codex measured
**9,523** in the deployed public artifact against **10,432** on the private rebuild), and a figure
quoted without its provenance is exactly how that turns into an apparent error.

| | |
|---|---|
| Artifact | `discovery-v1-e9365edcab27af7d0739ab1a07b1a187683993bcbff41ff88128c8fe4fbb7181.db` |
| `meta.audience` | **public** |
| `sidecar_version` | `discovery-v1-real` |
| `data_as_of` | 2026-08-03 |
| Content hash | matches `discovery_data/manifest.deploy.json` |
| Read through | `web.discovery.get_launch_stats_enveloped` (136-22), at request time — never a literal |

| Figure | Value | Basis |
|---|---|---|
| Identifications the finding aids did not already have | **9,523** | `main_pool = 1` |
| — no prior identification (`fills_gap`) | **4,152** | `main_pool = 1` |
| — finer than the aid (`refines_granularity`) | **3,873** | `main_pool = 1` |
| — aid named only a container (`container_predicts`) | **1,498** | `main_pool = 1` |
| Manuscripts carrying them | **6,755** | `main_pool = 1` |
| Context: fragments / pages | **38,431** fragments across **177,402** pages | corpus |
| All-bucket equivalent, stated as such in words wherever shown | **17,536** over **10,959** manuscripts | main pool + "more matches" |

The four headline numbers are on ONE basis and the shades sum exactly to the total; the total is
computed AS the sum, so the decomposition is structural rather than coincidental. **136-22's
no-literals guard scans source AND `genizah_translations.py` for every one of these figures as a
literal, including the retired 13,285**, with its forbidden list derived from the loaded artifact.
**No launch figure appears in `CHANGELOG.md`** — a number written there outlives the artifact
exactly as one written in a translation would, and a test in `tests/render_smoke/test_findings_render_smoke.py`
plus the guard keep it out of code and translations.

---

## 5. The four late rulings — disposition

Rulings R, S, T and U were recorded **2026-08-03, after** the Codex round-4 plan sign-off, so no plan
written before that date could reference them. This is the first artifact that states all four
dispositions in one place.

| Ruling | Disposition | Implemented in | Pinned by |
|---|---|---|---|
| **R** — curated display title for `w000176` (`משנה תורה, ספר אהבה / סידור` / `Mishneh Torah, Sefer Ahava / Siddur`); a DISPLAY-time relabel, corpus-wide, naming both readings and asserting neither | **APPLIED on all four surfaces.** Every surface that renders a work title routes through `display_work_title`; a surface formatting `neutral_title` directly would silently opt out | `shared/discovery_display_strings.py::CURATED_WORK_TITLES` + `display_work_title` | `tests/test_discovery_display_strings.py::test_curated_title_names_both_possibilities_never_asserts_one` (+ bilingual override, uncurated pass-through, bilingual-completeness) · `test_findings_render_smoke.py::test_the_curated_title_renders_and_the_raw_one_never_does_uncurated` (both languages) · `test_an_uncurated_title_passes_through` |
| **S** — the public artifact ships the two-axis conjunction **including the JA direct matches**; `_vis01_shortcut` is a known-stale rule that must NEVER be a publication gate | **SHIPPED as built; no rebuild, no re-projection.** The claim-integrity question was ANSWERED, not deferred: JA is ~20% of the 220 graded candidate cards while JA direct rows are ~10% of the shipped public evidence, so the certificate is not being stretched | `shared/discovery_visibility.py` (the per-row conjunction) · `scripts/project_discovery_public.py` · the reconciliation report in `136-REBUILD-GATES.md` gate 9 | `tests/test_vis01_projection.py` (the `is_public` fail-closed conjunction, no second implementation in `shared/`, `scripts/` or `web/` — verified by Codex against the live artifact: zero nonpublic assertion rows, zero nonpublic work identities, zero nonpublic identifications) |
| **T** — the "more matches" bucket carries ~half the non-Bible discovery value; it stays reachable, unnumbered, match-framed; the owner's "about half of them are right" is a **vibe-check that may never be quoted as a figure** | **APPLIED.** The second bucket is a first-class control, not a disclosure; its rows render with main-pool anatomy and carry no count. No number derived from the owner's impression reaches any surface | `web/pages/findings.py::_render_bucket_control` · `shared/discovery_main_pool.py::bucket_label` (the ONE bucket rule) · `web/components/findings_rows.py` | `test_findings_render_smoke.py::test_second_bucket_rows_render_on_a_populated_fixture`, `::test_a_second_bucket_row_has_the_SAME_anatomy_as_a_main_pool_row`, `::test_no_count_element_is_attached_to_the_bucket_control_or_its_rows`, `::test_the_second_bucket_section_carries_no_wording_implying_the_rows_are_wrong` · `tests/test_findings_page.py::test_real_browser_actionability_of_the_more_matches_control` (CI job `findings-browser-check`) — **see G5: the phase record still says NOT MET** |
| **U** — the launch leads with contribution; all four numbers on the single `main_pool = 1` basis; read from the artifact, never hardcoded; no precision percentage; match framing | **APPLIED.** The headline renders four numbers from the artifact being served, the shades sum exactly to the total, the basis is named in words in both languages, and an outage renders a retry rather than a zero | `web/discovery.py::get_launch_stats_enveloped` + `shared/discovery_service.py` (136-22) · `web/components/findings_rows.py::render_launch_headline` | 136-22's no-literals guard (source + translations, forbidden list derived from the artifact, including the retired 13,285) · `test_findings_render_smoke.py::test_every_rendered_headline_figure_equals_the_envelope_value` (on a SENTINEL fixture whose numbers appear in no artifact, so it proves the DATA PATH rather than agreeing with a coincidence) · `::test_the_headline_names_its_basis_in_words_and_frames_the_container_shade` · `::test_an_outage_headline_offers_a_retry_and_renders_no_zero` |

---

## 6. What this evidence does NOT cover

Stated here, at the same level as § 1, because in a phase whose evidence has been wrong six times
toward false confidence the limits of the evidence are part of the evidence.

1. **The database masking scans are executor-run, not a CI gate.** The six sidecars are gitignored
   and 390–470 MB each; no runner holds them. `test_every_discovery_database_present_on_this_machine_is_in_the_record`
   covers exactly zero databases on CI, and the run REPORTS that rather than passing quietly. The
   record (`PRODUCTION_DATABASE_SCANS`) carries the date, both modes, the result and the wall time
   for each. Re-run them before any public flip; the artifact may have changed.
2. **The masking sweep proves masking, not correctness.** It shows no restricted-corpus name is
   reachable by any of four egress classes. It says nothing about whether an identification is right.
3. **The panel is line-checked by its own suite, not by the sweep.** The sweep asserts the panel's
   UI-emitting FUNCTION coverage and re-uses 136-17's capture; the LINE-level requirement for
   `web/components/discovery_panel.py` lives in `test_panel_render_smoke.py`. Two suites, one
   property; if either is deleted the property is half-covered.
4. **Five lines of the findings modules are exempted from the line requirement.** Every one is a
   bare `return`, a `continue` or a defensive `except` that assigns a local — none paints. The
   exemption list is size-pinned and each entry's anchor must still match a line in the module, and
   a test refuses any exemption whose anchor contains `ui.`.
5. **The production LLM novelty run never happened.** Ruling K keeps the ~$301 run unauthorized.
   What shipped is the funnel, the heuristics and the re-measurement — not a full-corpus model pass.
   The false-novel rate, the axis question the owner cares most about, is **untested**.
6. **No end-to-end public read path has been exercised against a flag-ON production box.** 136-20
   proves the audience boundary against a private-audience artifact in tests; the browser check
   proves one control in CI against a synthetic fixture. Nobody has loaded
   `/computed-identifications` on production with the flag on, because the flag has never been on.
   **That is the single largest gap in this attestation**, and it is unavoidable at this point in
   the sequence: the first flag-ON load is itself part of the Phase 139 gate.
7. **A defect this plan introduced and closed, recorded because it is the seventh of its class.**
   The sweep's first positive control to go red printed the restricted pattern in clear text into the
   test log — `assert needle in text` is rewritten by pytest to show both operands. It was found by
   running the sweep's own mutation battery, not by review. Membership now goes through a helper that
   returns a bool, and an AST check refuses any assertion that could echo it again. **A masking test
   that leaks the thing it tests for, on its way to reporting a leak, is the worst possible shape for
   that file** — and nothing about the original line looked wrong.
8. **`docs/OPEN_ISSUES.md` carries the unfixed findings**, including two deferred `shared/discovery_service.py`
   cache-staleness defects (path-blind cache keys) that were left alone because two concurrent waves
   owned that file, a sixth event-loop-blocking call in `web/pages/browse.py`, the stored-vocabulary-on-an-exception
   egress in `shared/discovery_display_strings.py` that the panel path does not catch, and the mobile
   nav-drawer defect the browser check surfaced.

---

## 7. Verdict

**Phase 136's own work is complete and both flag-on gating items are done, with evidence.** The
phase is NOT a green light for the flag: six items still gate the public flip (§ 3), five of them
owned by Phase 139 and one a records correction, and the largest untested surface is the one nobody
can test until the flag goes on once (§ 6.6).

Read § 3 and § 6 before deciding anything.
