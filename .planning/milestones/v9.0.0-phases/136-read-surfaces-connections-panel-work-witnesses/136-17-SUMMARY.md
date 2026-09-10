---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 17
subsystem: discovery-read-surfaces
tags: [PANEL-01, PANEL-02, D-13, D-13i, D-06a, discovery, browse, honesty-gate, render-smoke]

requires:
  - "136-15: shared/discovery_panel_model.py -- PanelServiceBundle + build_panel_rows over the five-envelope bundle"
  - "136-21: web.discovery.get_work_expansion_enveloped + SURFACE_EXPANSION_FIELDS (four KNOWN_CARRIER_FLOOR members)"
  - "136-22: items[*].shade + LAUNCH_CONTRIBUTION_SHADES + SURFACE_LAUNCH_SHADE_FIELDS"
  - "136-14: make_envelope / the SURFACE_* allowlists / _ALL_ALLOWLISTS"
  - "136-10: shared/discovery_display_strings.py + the .gs-discovery CSS block"
  - "136-02: tests/render_smoke/discovery_honesty_gate.py (the five original detectors)"
provides:
  - "web/components/discovery_panel.py -- the panel body renderer over the pure display model"
  - "the browse seam: a fifth enrichment placeholder pair + fetch_discovery_panel_bundle()"
  - "the gate's CLASSIFICATION: MACHINE_VOCABULARY_FIELDS / READER_TEXT_FIELDS / META_* / KNOWN_CARRIER_FLOOR / ALLOWLIST_FIELD_UNION"
  - "the gate's SIXTH detector (accuracy/rate) + assert_envelope_honesty + assert_error_path_honesty + assert_surface_honesty"
  - "web/pages/help.py::_LIMITATIONS_PARAGRAPH_CLASS -- the D-06a exception's one registered element"
affects: ["136-18 (imports the same classification and the same six detectors)", "136-19"]

tech-stack:
  added: []
  patterns:
    - "completeness as an exact set EQUALITY against an independently recomputed ground truth, never an enumeration"
    - "declaring a machine carrier LITERALLY unions its vocabulary into the prohibited set (a derivation, not a second list to maintain)"
    - "an exception bound to ONE registered rendered ELEMENT rather than carved out of a lexicon"
    - "a hardcoded digest as an INDEPENDENT pin on owner-approved text"

key-files:
  created:
    - web/components/discovery_panel.py
    - tests/render_smoke/test_panel_render_smoke.py
    - tests/test_discovery_panel_render.py
    - tests/test_discovery_panel_browse_wiring.py
  modified:
    - web/pages/browse.py
    - web/pages/browse_enrichment.py
    - web/pages/help.py
    - tests/render_smoke/discovery_honesty_gate.py

decisions:
  - "DISCOVERY_ENABLED gates the panel's EXISTENCE; discovery_available() gates the envelope STATUS. Without that split, 'deployed with the flag off' would still put an outage card on every browse page."
  - "assert_discovery_honesty keeps its FIVE-detector contract; assert_surface_honesty is the six-detector entry point. Forced by a criterion pair in tension -- see 'The one criterion pair in tension'."
  - "meta is scanned with NO machine exemption at all: classification makes a key's value set checkable, it exempts nothing."
  - "D-13i is implemented as OMISSION plus an explicit scope note, not as a labelled catalogue line."
  - "The production deploy was NOT performed. Recorded as NOT MET with its reason."

metrics:
  duration: "~7h"
  completed: 2026-08-04
  tasks: 3
  commits: 4
  tests_added: 230
---

# Phase 136 Plan 17: The Reader-Facing Connections Panel Summary

The browse page can now open a manuscript's computed identifications from its existing toolbar and
see both panes with equal weight — over a honesty gate that classifies **every field of every
registered allowlist exactly once**, by exact set equality against a ground truth it recomputes, and
that catches an accuracy rate whether it arrives as prose, as an envelope string, as a float or in an
exception message.

## Task commits

| Task | Commit | What landed |
|---|---|---|
| 2 (part 1) | `e1db9fb6` | `web/components/discovery_panel.py` — the body renderer (488 lines) |
| 1 | `3fdbdc7d` | the browse seam, the four service states, the offload contract + 34 guards |
| 2 (part 2) | `4c3d2d60` | 30 renderer tests, proved by 12 mutations |
| 3 | `1ced6555` | the gate classification + the sixth detector + the D-06a hook + 166 tests |

**Commit ORDER deviates from task order, deliberately.** Task 1's AST offload guard READS
`web/components/discovery_panel.py`, so committing Task 1 first would have left a red tree for one
commit. The renderer module was therefore committed first, alone; every commit in the sequence is
green on its own.

## Performance — the measured enrichment delta

Measured against the REAL public artifact (`discovery_data/discovery-public-136rebuild.db`), through
the same async wrappers the browse seam awaits, over the 20 heaviest pages:

| | p50 | p95 | max |
|---|---|---|---|
| **cache-COLD** | 3.0 ms | **4.2 ms** | 9.8 ms |
| **cache-WARM** (a page turn within one manuscript) | 0.1 ms | **0.1 ms** | 0.2 ms |

Budget (`docs/specs/discovery-budgets.md` §1.1): added browse latency p95 ≤ 150 ms. Cold p95 is
**36× inside** it; warm is the number that describes normal browsing, and it is the version-keyed LRU
that makes the four-read shape affordable. The figures cover the three sidecar-backed eager reads plus
the manuscript-works read; `get_manuscript_page_ids` reads the browse map, which the browse page has
already loaded by the time the enrichment phase runs.

## Task 1 — the seam

`fetch_discovery_panel()` joins the existing `asyncio.gather`. Every read is a **direct `await`** on a
`web.discovery` async wrapper: no `run.io_bound`, no nested offload, no synchronous
`shared.discovery_service` call, no `web.discovery._service`. Three independent eager reads in one
round, then the manuscript-works read **only over a RESOLVED page scope** — the status is checked AND
`meta['resolved']` is compared to `True`, so an outage envelope (which carries no `resolved` key at
all) can never be read as a resolved scope.

**The dispatch-count table, derived from one rule** — *exactly one crossing per read that is ISSUED
and reaches `_run_off_loop`* — and asserted per service state:

| state | expected | observed |
|---|---|---|
| `ok`, resolved, cold | 4 | 4 |
| `ok`, resolved, page turn within one manuscript | 3 | 3 (manuscript-works HITS its stable-key entry) |
| `ok`, resolved, repeat of the same folio | 1 | 1 (only the uncached page-ID accessor) |
| `ok`, `meta['resolved'] is False`, cold | 3 | 3 |
| `unavailable` (`discovery_available()` False) | 0 | 0, **and the panel still renders its outage state** |
| `timeout` on the PAGE-ID read, cold | 3 | 3 |
| `timeout` on a DOWNSTREAM read, resolved, cold | 4 | 4 |
| `busy` INJECTED on the PAGE-ID read | 2 | 2 |
| `busy` INJECTED on a DOWNSTREAM read | 3 | 3 |
| opening the related-pages toggle | +1 cold, +0 on a repeat | asserted in `test_discovery_panel_render.py` |

The `busy` rows are injected **before** the executor dispatch, which is where the live gate
(`_acquire_heavy_slot`) raises — the panel's reads are `heavy=False`, so `DiscoveryOverload` is
unreachable through the live gate and an injection anywhere else would not describe what is tested.

**Which of the four offload modes the standing suite could NOT have caught.** `tests/test_no_await_sync_function.py` is an AST scan for `await <sync fn>()`. It cannot see **(i)** `run.io_bound` wrapped
around an *async* wrapper (the call is syntactically fine and the awaited object is a coroutine —
it simply never runs), **(ii)** a nested offload (both layers are individually legal), **(iii)** a
*synchronous* `shared.discovery_service` call from a page module (there is no `await` to flag), or
**(iv)** an import of `web.discovery._service` (an attribute access, not an await). All four are
pinned here, over the AST, and the guard has its own positive control.

**F-14's disposition is closed:** the four statuses survive from the service to the rendered state.
`web.discovery` names the failure, `PanelServiceBundle` refuses a bare list, the model's arbitration
table is total over (status × scope state), and the renderer branches on the model's field. No layer
collapses an outage to an empty list.

## Task 2 — the renderer

488 lines, and the interesting property is what is ABSENT: no `sorted`, no `bucket_label`, no band
comparison, no relation-keyed class, no bare `neutral_title` read outside the one guarded
`_expansion_work_title`. Both panes carry equal weight, the manuscript pane NAMES its works with page
counts, and the two lazy reads (related-page rows, per-work expansion) are issued by their own toggles.

**D-13i: implemented as OMISSION.** The panel renders no catalogue-derived text at all, and the
manuscript pane carries an explicit scope note ("Computed for this manuscript only. No catalogue
description is shown here." / the Hebrew twin). The alternative — labelling the catalogue line as
describing the SHELFMARK — was not taken: the browse header's line is outside this plan's files, and a
label there would be a change to a shipped surface for a defect this panel can avoid by not repeating
it.

**Ruling R, and a correction to how it can be asserted.** The HEBREW curated label for `w000176` is
`משנה תורה, ספר אהבה / סידור` — the raw recorded title PLUS the ruled disjunct. So "the raw title does
not appear anywhere in the expansion subtree" is a substring absence **no correct implementation can
satisfy in Hebrew**. The property actually asserted is that the raw title never appears UNCURATED:
`count(raw) == count(curated) if raw in curated else 0`, in both languages.

## Task 3 — the gate

### Gap A: the classification, as an exact partition

`ALLOWLIST_FIELD_UNION` is recomputed at test time from `_ALL_ALLOWLISTS` (**69 fields**).
`MACHINE_VOCABULARY_FIELDS` (**18**) and `READER_TEXT_FIELDS` (**51**) partition it exactly and
disjointly. There is no unclassified state and **`NEVER_POPULATED` is deleted**.

**The full machine/reader-text split.**

Machine carriers (field → vocabulary): `relation_kind`, `claim_type`, `anchor_claim_type` → `CLAIM_TYPES`;
`evidence_source`, `displayed_evidence_source` → `EVIDENCE_SOURCES`; `confidence_band`,
`displayed_confidence_band` → the `CONFIDENCE_BANDS_BY_SOURCE` union; `adjudication_status` →
`ADJUDICATION_STATUSES`; `routing_status` → `ROUTING_STATUSES`; `routing_reason` → `ROUTING_REASONS`;
`measurement_status` → `MEASUREMENT_STATUSES`; `novelty_status` → `NOVELTY_STATUSES`;
`main_pool_reason` → `MAIN_POOL_REASONS`; `coverage_status` → `COVERAGE_STATUSES` (local);
`eligibility_basis` → `ELIGIBILITY_BASES` (local); `shade` → `LAUNCH_CONTRIBUTION_SHADES`;
`unit` → `FINDINGS_UNITS`; `level` → `FACET_LEVELS`.

Reader-text (51), each with a KIND and a PRODUCER: 16 identity/digest (`page_id`, `sys_id`,
`claim_id`, `evidence_id`, `identification_id`, `work_id`, `canonical_work_id`, `display_work_id`,
`unit_id`, `representative_sys_id`, `representative_page_id`, `representative_claim_id`,
`related_page_id`, `member_sys_ids`, `value`, `parent`), 9 reader text (`neutral_title`, `author`,
`genre`, `domain`, `library_code`, `shelfmark_display`, `band_label`, `novelty_source_label`,
`label`), 15 numeric, 11 boolean.

**`unit` and `level` were moved to the MACHINE side** relative to the plan's prose, which lists `unit`
among fields "checked and not matching" the value rules. Both really are closed vocabularies
(`FINDINGS_UNITS`, `FACET_LEVELS`); classifying them machine subjects them to (h) and costs nothing,
because neither has an underscore-bearing member and so (c) prohibits nothing new. Recorded here as
the plan requires any field moved to the machine side to be.

**Declaring a carrier is LITERALLY what prohibits its vocabulary.** `_PROHIBITED_RAW_VOCAB_KEYS` is
rebound at the bottom of the module to union in every underscore-bearing member of every mapped
vocabulary. `ROUTING_REASONS` (`co_citation`, `later_shared_text`, `runner_up_conflict`),
`COVERAGE_STATUSES` (`no_denominator`, `not_applicable`) and `ELIGIBILITY_BASES` (`review_opt_in`)
enter the prohibited set through exactly that route — the explicit collector names none of them, and
assertion (c) would have failed if the derivation were absent (mutation G2).

**What the two value rules flagged, and the disposition of each.** Run over the whole corpus, rule (1)
SHAPE and rule (2) REGISTRY MEMBERSHIP flagged **nothing** under a `READER_TEXT_FIELDS` field or a
`META_FREE_TEXT_KEYS` key. `REGISTRY_MATCH_EXCLUSIONS` is therefore **EMPTY**, and
`NUMERIC_RULE_EXEMPTIONS` is empty. Values re-checked and confirmed not to collide: `genre` /
`domain` (FJMS strings with spaces and slashes), `novelty_source_label` (a masked sentence),
`library_code` (`CUL`), `band_label` (band prose), the id fields (sha256 digests and `w`-prefixed
keys), `shelfmark_display` (`T-S 12.123`).

**The DOCUMENTED RESIDUAL, stated rather than claimed closed:** a carrier whose vocabulary is
single-word AND exported nowhere AND declared reader-text is invisible to BOTH value rules. Ground
truth 1 still forces it to be CLASSIFIED, so it cannot pass unnoticed; what cannot be forced is the
classification being RIGHT. The only bound available is documentary, and it is enforced: every
`READER_TEXT_FIELDS` reason must name the code site, table or file that produces the values
(`test_b_every_reader_text_reason_names_a_kind_and_a_producer`).

**The `meta` partition.** `META_VOCABULARY_FIELDS` covers `reason`, `unit`, `sort`, `sort_basis`,
`bucket`, `basis`, `filter_basis`, `anchor_mode`, `level`, `lang`; `META_FREE_TEXT_KEYS` covers
`page_id`, `sys_id`, `volume_ie`, `work_id`, `domain`, `author`, `sidecar_version`, `audience`.
**`sidecar_version` and `audience` had NO authority to pin to** — both are read verbatim from the
artifact's own `meta` table, so a locally-declared value set would be a hand-copied set that cannot
fail when the builder changes it. Classification here **exempts nothing**: `meta` is scanned with no
machine exemption at all, and `meta['reason'] = 'direct_witness'` fails loudly (mutation G11 proves
the exemption path is the leak).

**Derived `CONSUMED_ALLOWLISTS`:** `SURFACE_CLAIM_FIELDS`, `SURFACE_WORK_SUMMARY_FIELDS`,
`SURFACE_RELATED_PAGE_FIELDS`, `SURFACE_EXPANSION_FIELDS`, `SURFACE_LAUNCH_SHADE_FIELDS` (**seeded**,
so 136-22's `shade` is exercised through the real controls rather than excused).
`SURFACE_FINDING_FIELDS` and `SURFACE_FACET_FIELDS` are outside the coverage domain **by derivation** —
this surface does not consume them — while still being fully CLASSIFIED, which is what lets 136-18
RUN the same check rather than edit a gate file it does not own.

**`_project` is TOTAL — CONFIRMED against the live code** (`shared/discovery_surface_projection.py`,
`_project` backfills every allowlisted key with `None`), so ground truth 3 uses the EXACT key-set
equality form, not the degraded subset form. It is a CONSTRUCTION check: it catches a hand-written
dict that never went through a projection, **not** the row that omitted a field. The omission is
caught one level up, by the derived non-null coverage check.

**No PRE-projection key assertion was added.** The producing queries build their rows from SELECT
lists whose columns vary by branch (`get_manuscript_works_enveloped` has two `ok` returns with
different `meta` shapes; the expansion's `manuscript_display` join is a LEFT JOIN whose columns are
legitimately NULL), so no producer contract requires every allowlisted key on every pre-projection
row. Inventing one so an old sentence becomes true is how a gate starts rejecting correct rows.

**Locally-declared vocabularies, pinned by READING their authority at test time:**
`COVERAGE_STATUSES` against the `CHECK (coverage_status IN (...))` literal in
`scripts/build_discovery_sidecar.py:255`; `ELIGIBILITY_BASES` against the `CASE ... END AS
eligibility_basis` literals in `shared/discovery_service.py:1592-1595`. **OWED FOLLOW-UP:** exporting
both properly to `shared/` was NOT done here — `shared/` and `scripts/` are outside this plan's
`files_modified`, which is true in every wave.

### Gap B: the sixth detector

Two rules — a RATE WORD within 48 chars of a RATE-SHAPED QUANTITY, and a bare decimal in `[0,1]` with
two or more places — plus, on envelopes, a float in `[0,1]` with more than one decimal place and a
rate-lexicon KEY NAME. Version syntax (`v`/`V` at a word boundary, or `version`/`גרסה`) is excluded
from both.

**One correction the plan did not anticipate, found by its own control.** The decimal pattern must
require an EXPLICIT integer part (`\d+\.\d+`, no lookbehind). With a lookbehind excluding a preceding
word character, `accuracy score0.9` escapes both rules — the round-9 defect. Without the integer-part
requirement, the shelfmark `MS Heb c.57` reads as the fraction `.57`, in `[0,1]` with two places, and
rule 2 rejects a legitimate shelfmark. Both boundaries are asserted.

### The one criterion pair in tension — REPORTED, not softened

Two acceptance criteria cannot both hold if the sixth detector is wired unconditionally into
`assert_discovery_honesty`:

* `FP-D06A-CARD-BOUNDARY` requires that the live limitations sentence, scoped to
  `_CONFIDENCE_SECTION_CLASS`, **FAILS**.
* `tests/render_smoke/test_help_methods_render_smoke.py` — outside this plan's `files_modified` and
  required to pass **UNEDITED** — makes exactly that call: `assert_discovery_honesty(fragment,
  scope_selector=_CONFIDENCE_SECTION_CLASS, ...)` over `_scoped_html_fragment(user)`, a **flattened,
  html-escaped** rendering of the whole methods card. The marker class does not survive that
  flattening, so no element-bound exemption can reach it either.

Resolution taken: `assert_discovery_honesty` keeps its FIVE-detector contract (`check_accuracy=False`),
and **`assert_surface_honesty`** is the six-detector entry point every Phase-136 SURFACE calls. Both
criteria then hold, the shipped help suite passes unedited, and the D-06a boundary is fully enforced by
this plan's own controls. **The residual cost, stated plainly:** a future surface that calls
`assert_discovery_honesty` directly gets five detectors, not six. The gate's docstring names
`assert_surface_honesty` as THE surface entry point and 136-18 must call it.

The alternative — defaulting `check_accuracy=True` — turns an owner-approved page's suite red and can
only be repaired by editing a file this plan does not own. That is a blocking finding, not a fix.

### D-06a's exception, bound to one element

`web/pages/help.py` gained **exactly one thing**: `_LIMITATIONS_PARAGRAPH_CLASS =
'discovery-methods-limitations'`, applied via `.classes(...)` to the single `ui.label` that renders
`_LIMITATIONS_TEXT[lk]`. `git diff web/pages/help.py` is **23 insertions, 1 deletion** — the deletion
is that one `ui.label(...)` line, replaced by the same call with `.classes(...)` inserted. The wording
in **both languages**, `_LIMITATIONS_HEADING`, `_CONFIDENCE_SECTION_CLASS` and its application to the
card are **untouched**. This is a markup hook placed AROUND owner-approved text, never a change to it.
`tests/render_smoke/test_help_methods_render_smoke.py` is **not edited** (`git diff` empty) and passes.

**The hardcoded per-language SHA-256 digests**, computed from the CURRENTLY SHIPPED wording over the
paragraph's normalised rendered text:

| lang | digest |
|---|---|
| en | `c209693ccdcbcc9b7548a091cdf3d22c7078591014cdfcc9424bbaa9302aef3a` |
| he | `43144bc0cfd79abb080bcddb4219202c39c267402e159a6aa42f5e6f20694487` |

Mutation H2 — an edit preserving every substring the sibling render test pins — fails **only** this
digest, which is the point of it being independent.

## Mutations run — and the two that could NOT fail

**Task 1 (6 mutations, all failed by name):** scope branching only on `meta['resolved']` defaulted to
True → 5 tests; the same with a falsy-but-present reading → 5; the post-works staleness check dropped →
1; `volume_ie` not passed → 1; the works read issued unconditionally → 7; `run.io_bound` on the panel
path → the AST guard; the entry control hiding on any empty claims list → 6.

*An earlier form of the first mutation (`.get('resolved', True)` while KEEPING the status check) was an
EQUIVALENT mutant and produced no failure — recorded because "no failure" there was a property of the
mutation, not a gap, and it took re-deriving the fixture semantics to see that.*

**Task 2 (12 mutations, 11 failed immediately; N12 could NOT fail).**

> **N12 — the expansion's own outage branch had no control.** Deleting `_render_expansion_envelope`'s
> `is_outage` branch left all 29 tests green, because the only outage test drove the RELATED-PAGES
> path. Two lazy reads, two branches, and only one was guarded. Fixed by adding
> `test_an_outage_on_the_expansion_read_renders_a_retry_not_an_empty_list`; N12 now fails by name.

Others: rows loading eagerly → 2; page length as the total → 1; the anchor chip dropped → 1; a blank
cell for `display_missing` → 1; a relation-keyed chip class → 1; the band label as visible text → 2;
the renderer sorting → 1; the gated chip losing its state → 1; the expansion title formatted rather
than routed → 1; a vote placeholder gaining a handler → 1; a bare count in the manuscript pane → 2.

**Task 3 (14 mutations + 1 combined; 13 failed by name, G1 could NOT fail alone).**

> **G1 — dropping `NOVELTY_STATUSES` and `MAIN_POOL_REASONS` from the explicit collector produced no
> failure.** Not a gap: the derivation from `MACHINE_VOCABULARY_FIELDS` re-adds them, which is the
> mechanism working. Confirmed by running **G1+G2 together** (collector short AND derivation removed):
> 5 tests fail, including controls 6 and 7 and assertion (c). So the property is load-bearing, and it
> is the derivation that carries it.

| mutation | failed |
|---|---|
| G2 prohibited set not derived from the mapping | (c) |
| G3 a carrier dropped from the classification | (a), (g), `FP-LIVE-VOCAB` |
| G4 `genre` → `frozenset()` (control 13, half 1) | (h) NON-EMPTY + (a) |
| G5 `genre` → an unrelated vocabulary (control 13, half 2) | (h) MEMBERSHIP + (a) |
| G6 the meta partition loses `filter_basis` | (e), both halves |
| G7 the accuracy detector dropped | 6 tests incl. controls 12/12a/12b, `FP-*` |
| G8 the D-06a exemption becomes a GLOBAL lexicon subtraction | control 12, 12b, `FP-D06A-CARD-BOUNDARY` |
| G9 the version exclusion widens to any word character | `FP-VERSION-BOUNDARY` |
| G10 the numeric envelope rule dropped | control 10 |
| G11 `meta` scanned WITH the machine exemption | the strict-meta test |
| G12 the raw-vocab detector dropped from the envelope scan | controls 6, 7, `FP-LIVE-VOCAB` |
| G13 `COVERAGE_STATUSES` drifts from its authority | the builder-pin test |
| G14 the D-06a scope registry grows a second entry | 4 tests |
| H1 the marker class on the CARD instead of the label | `FP-D06A-LIVE-PAGE` + 2 |
| H2 the owner wording edited, pinned substrings preserved | **only** the hardcoded digest |

Every mutation was applied to a `cp`-restored source and reverted from a scratchpad backup. **`git
checkout --`, `git stash`, `git clean` and `git reset` were not used at any point.**

## Positive and false-positive controls — every one watched

**Positive controls 1–13, each asserting its NAMED failure:** a precision figure in a row → the
percentage detector; a stored key in a chip → the raw-vocabulary detector; a review badge → the badge
detector; a percentage in `meta['reason']` → the envelope scan (with a companion showing the markup
scan cannot see it); a percentage in a forced exception → the error-path scan; `fills_gap` as
`band_label` → the NOVELTY vocabulary; `main_full_coverage` as `band_label` → the MAIN-POOL vocabulary;
`accuracy 0.91` in a row / in `meta['reason']` / as an envelope FLOAT / in an exception message → the
new detector's four wirings; the D-06a sentence taken from the LIVE render, seeded outside its scope in
**all three egress classes** plus **elsewhere in the same methods card**, with `accuracy 0.91` INSIDE
the registered scope still failing; `genre` moved to the machine side in two halves.

**False-positive controls, by identifier, all observed PASSING:** `FP-LIVE-VOCAB` (a fully-populated
live-vocabulary envelope passes; `direct_witness` in `band_label` fails), `FP-QUALIFIED-COVERAGE`
(both languages), `FP-RATE-DISCLAIMER` (both languages), `FP-SAMPLE-SIZE` (the pair),
`FP-VERSION-BOUNDARY` (four assertions), `FP-D06A-LIVE-PAGE` (both languages, over the LIVE `/help`
render, with the scope-exactness equality), `FP-D06A-CARD-BOUNDARY` (both halves).

## The suite, and the assertion count

| file | tests |
|---|---|
| `tests/render_smoke/test_panel_render_smoke.py` | **166** |
| `tests/test_discovery_panel_render.py` | **30** |
| `tests/test_discovery_panel_browse_wiring.py` | **34** |
| total added | **230** |

The 166 include **56 panel renders** (7 manuscripts × 2 languages × 4 service states), each running
the six-detector gate scoped per element — root, manuscript pane and every identification row — for
**176 element-scoped gate calls** across the matrix, plus 14 envelope scans (7 envelopes × 2
languages) and 12 error-path scans (6 modes × 2 languages).

The seven manuscripts are **fixture PROFILES** named for the standing regression set (clean,
commentary, Judeo-Arabic multi-register, expert-reviewed, problem siddur, page-relation-heavy,
427-identification), built through the live `surface_safe_*` projections. They are not the real
manuscripts: resolving those needs the browse map and the Tantivy index, which a render-smoke suite
must not load. **Deviation recorded.**

## Verification

| check | result |
|---|---|
| `pytest tests/render_smoke/ tests/test_no_await_sync_function.py tests/test_no_raw_storage_access.py` | **249 passed, 1 skipped** |
| plus the three new suites + display strings + panel model | **580 passed, 2 skipped** |
| `pytest tests/ -k "discovery"` AFTER the gate widening | **1494 passed, 8 skipped** |
| `pytest tests/ -k "browse_enrichment or discovery_panel"` | **276 passed, 4 skipped** |
| `ruff check` on all owned files | clean |
| `git diff --stat web/static/common.css` | **empty** (no CSS added) |
| `git diff tests/render_smoke/test_help_methods_render_smoke.py` | **empty** (unedited, green) |
| masking `--scan-repo` (`MASKING_SCAN_PATTERNS_FILE` SET to `.masking_patterns`) | **no matches — clean** |
| masking `--scan-asset` over the captured panel output | **no matches — clean** |

**No already-shipped surface went red after the gate widening.** The full render-smoke suite and the
1,494-test discovery suite were both re-run; nothing needed the "real leak" disposition and the ONE
named carve-out was not invoked (the D-06a boundary is handled by the entry-point split above, not by
narrowing the detector).

**Masking capture path:** `…/scratchpad/panel_capture.txt`, **outside the working tree**, so a stray
untracked capture cannot trip `--scan-repo` itself. `MASKING_SCAN_PATTERNS_FILE` was set explicitly;
the scan did NOT fall through to its fail-closed path, and no skip was taken.

## Acceptance criteria

### Task 1 — ALL MET

Fifth placeholder in the same shape as the four; `fetch_discovery_panel()` in the existing gather;
generation-token change mid-await paints nothing (two tests, before and after the works read);
client-liveness guard asserted on both the load and the retry path; four entry-control tests; the AST
guard over all four offload modes with its own positive control; the dispatch-count spy per service
state; the works read asserted NOT ISSUED for each of `timeout`/`busy`/`unavailable`;
`page.volume_ie` passed; the unresolved branch asserted to issue no query; the truncated marker;
`page_client` bound at render time and per-user state read before any await (source assertions); both
enrichment deltas recorded; the standing guards green with the four modes they could not have caught
stated above.

### Task 2 — ALL MET

Source assertion that no bucket/collapse/gating/ordering decision is made here (with a positive
control); exactly one guarded `neutral_title` read, inside `_expansion_work_title`; the bilingual
`w000176` fixture (in the corrected form); plain-text titles; the `1fr 1fr` grid and the two chip
states asserted against the shipped CSS; the band label as `title` and never visible; no
relation-keyed class; the manuscript pane naming works with counts and rendering a gated work dimmed;
the related-pages default asserted as NOT-REQUESTED rather than empty; the interaction test (issued
once, rows render, not issued before the toggle, no further read on re-open, outage → retry); the
expansion lazy, anchor-excluding, weaker-band; every carrier row named by library AND shelfmark;
`display_missing` → the explicit unnamed treatment; two chips on differing relations and one on
agreeing; the counted total, not the page length; no stored key and no colour-coding on either chip;
the match-framed heading; the catalogue treatment (omission, stated above); inert vote controls; no CSS
added.

### Task 3 — MET EXCEPT THE DEPLOY

Everything above is MET. **NOT MET:**

- **"The panel is deployed with the flag OFF for the public; the deployed commit is recorded."** —
  **NOT MET.**
- **"Post-deploy verification confirms the browse page is unchanged with the flag off and the panel
  renders with the flag on."** — **NOT MET** (it depends on the deploy).
- **"A rollback path is recorded as verified available before the deploy."** — **MET as far as it can
  be**: the code rollback is `git reset --hard <prior commit> && sudo systemctl restart genizah-web` on
  the box (`deploy.sh` is itself a `git reset --hard origin/<branch>`), and the sidecar rollback is the
  atomic manifest repoint in `docs/specs/discovery-deploy.md` §3. Neither was exercised, because
  neither deploy was performed.

**Why the deploy was not performed.** `deploy.sh` is `git fetch origin && git reset --hard
origin/master-main` **on the production box**, so deploying requires first pushing `master-main` to
origin and then hard-resetting production to whatever that branch then holds. Two conditions make that
unsafe to do unattended right now:

1. **This working tree is shared and another agent is active in it.** `tests/test_discovery_assets_audience.py`
   and `tests/test_discovery_composition.py` were clean at the start of this session and now carry
   substantial edits I did not make. A push plus a production `git reset --hard` would ship whatever
   lands on the branch between the push and the box's fetch — a set nobody verified together. This is
   precisely the contended-resource hazard the plan itself names when it explains why 136-18 deploys in
   a later wave.
2. **The deploy is a production mutation, and its verification half needs an environment flip**
   (`DISCOVERY_ENABLED=1` in the beta environment) that is an on-box change with no rollback drill run.

Prod was probed read-only and is healthy (`GET https://genizahsearch.com/browse` → **200**, 1.03 s).

**What is ready to deploy:** commit **`1ced6555`** on `master-main`, with `DISCOVERY_ENABLED` unset
(default OFF), which makes the browse page byte-for-byte what it is today —
`discovery_panel_enabled()` is False, so neither placeholder is created and no read is issued
(asserted by `test_the_panel_is_absent_entirely_when_the_flag_is_off`).

## Deviations from plan

**1. [Rule 3 — blocking] The commit order is 2 → 1 → 2 → 3, not 1 → 2 → 3.**
Task 1's AST offload guard reads `web/components/discovery_panel.py` from disk, so a Task-1-first
commit leaves a red tree. The renderer module was committed alone first. Every commit is green.

**2. [Rule 2 — missing critical] `DISCOVERY_ENABLED` gates the panel's EXISTENCE, separately from
`discovery_available()`.**
The plan's dispatch table says the `unavailable` state "still renders its outage state" while the
deploy criterion says the flag-off browse page must be **unchanged**. Both hold only if the FLAG gates
existence and `discovery_available()` (flag AND sidecar readiness) gates the envelope status. Without
the split, every browse page in production would carry a "temporarily unavailable" card.

**3. [Rule 3 — blocking] `assert_discovery_honesty` keeps five detectors; `assert_surface_honesty` is
the six-detector entry point.** See "The one criterion pair in tension".

**4. [Rule 1 — bug] The accuracy detector's decimal pattern needs an explicit integer part and no
lookbehind.** Both boundaries are asserted; without the first, `MS Heb c.57` is rejected as a rate.

**5. [Rule 2 — missing critical] The expansion's outage branch gained a control** (mutation N12).

**6. [Rule 3] Task-1 and Task-2 tests live in two new files outside `files_modified`.**
`tests/test_discovery_panel_browse_wiring.py` and `tests/test_discovery_panel_render.py`. The plan's
`files_modified` names two page modules, the component and two test modules, but Task 1's and Task 2's
acceptance criteria demand ~64 tests that cannot live in a render-smoke module (the verify command is
`-k "browse_enrichment or discovery_panel"`, which `test_panel_render_smoke.py` does not match). Both
files are NEW; no file owned by another plan was touched.

**7. [Rule 3] `unit` and `level` classified MACHINE**, against the plan's prose. Recorded above with
the reason.

**8. [Rule 3] The seven manuscripts are fixture PROFILES**, not the real regression manuscripts.
Recorded above with the reason.

**9. [Rule 2] The related-pages/expansion outage retry is a property of the SEAM.** The renderer draws
a retry only when given a handler, so `test_the_live_seam_always_supplies_a_retry_handler` asserts the
browse seam always passes one, and the render harness passes one exactly as the seam does. Found
because the first harness passed `None` and 44 outage renders went red.

**10. [Deliberate, from the plan] The composite-callable deviation stands.** Finding 4's remedy — one
synchronous bundle callable — is not implemented, because three of the four reads are served by
`_enveloped_off_loop(..., cache_name=…)`, a per-argument sidecar-version-keyed LRU. A composite would
be keyed on the whole argument tuple including `page_id`, invalidating the manuscript-works entry on
every folio turn. The measured warm p95 of **0.1 ms** is that cache working; the substance of finding
4 (no nesting, no private `_service`, `page.volume_ie` passed, explicit branching) is adopted in full.

## Owed follow-up

1. **Export `COVERAGE_STATUSES` and `ELIGIBILITY_BASES` to `shared/`.** They exist only as a CHECK
   constraint and a SQL `CASE` today, and are declared locally in the gate with test-time pins.
   `shared/` and `scripts/` are outside this plan's `files_modified`.
2. **136-18 must call `assert_surface_honesty`**, not `assert_discovery_honesty`, or it gets five
   detectors.
3. **The production deploy** and its post-deploy verification (above).

## Known Stubs

The **vote controls** are inert placeholders by design (PANEL-01 defers them to a later phase). They
carry no click handler at all, are marked `disable`, and the renderer contains no `ui.notify`, no
Supabase call and no vote-recording path — asserted by `test_vote_controls_are_inert`. Nothing else on
the panel is stubbed; every section is wired to a real read.

## Threat Flags

None. This plan adds no network endpoint, no auth path, no file access and no schema change. The two
lazy reads go through the existing enveloped wrappers, and `web/pages/help.py`'s only change is a
markup class.

## Self-Check: PASSED

Files claimed, verified present on disk:

- `web/components/discovery_panel.py` — FOUND
- `web/pages/browse.py` — FOUND (modified)
- `web/pages/browse_enrichment.py` — FOUND (modified)
- `web/pages/help.py` — FOUND (modified)
- `tests/render_smoke/discovery_honesty_gate.py` — FOUND (modified)
- `tests/render_smoke/test_panel_render_smoke.py` — FOUND
- `tests/test_discovery_panel_render.py` — FOUND
- `tests/test_discovery_panel_browse_wiring.py` — FOUND

Commits claimed, verified in `git log --oneline --all`:

- `e1db9fb6` — FOUND
- `3fdbdc7d` — FOUND
- `4c3d2d60` — FOUND
- `1ced6555` — FOUND

---
*Phase: 136-read-surfaces-connections-panel-work-witnesses*
*Completed: 2026-08-04*
