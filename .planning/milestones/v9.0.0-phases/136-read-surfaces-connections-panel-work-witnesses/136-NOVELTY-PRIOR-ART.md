# Phase 136 Novelty — Prior-Art Reconciliation (research pass, 2026-08-02)

**What this is.** A read-only investigation reconciling Phase 136's in-conversation novelty design
(`136-GATE1-DECISIONS.md` §§E/E′/F/G, amending `.planning/REQUIREMENTS.md` NOVEL-01/02) against the
substantial prior novelty work already sitting in this repo — most of it in the gitignored
`same_work_spike/probe/` research tree — which was not consulted when that in-conversation design was
produced. **No code, plan, spec or decision is changed by this file.** Every claim below is tagged
MEASURED (a number came out of running code), PLANNED (someone wrote down an intention), or ASSUMED
(carried forward without its own measurement). Where the evidence is ambiguous, options are presented
with trade-offs rather than resolved by fiat, per this pass's own brief.

**Headline finding, stated first because it is the most actionable one.** The nine-value shade
enum + separate `divergence_correctness` axis ratified in `136-GATE1-DECISIONS.md` §§E/E′/F/G and
folded into `.planning/REQUIREMENTS.md`'s NOVEL-01 amendments has **not been propagated into the three
documents that actually implement it**, even though `136-GATE1-DECISIONS.md` names all three by path:

1. `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-04-PLAN.md` (top-level,
   current — not the `superseded-2026-08-02/` copy) still writes, verbatim: `must_haves.truths[0]` =
   *"The novelty axis is tri-state and fail-closed"* (line 18); Task 1's action text (lines 98–101)
   defines `NOVELTY_STATUSES` as *"a frozenset of exactly `not_in_finding_aids`, `already_recorded`,
   `not_checked`"*; the `docs/specs/discovery-novelty-v1.md` artifact contract requires the string
   `"not_in_finding_aids"` (front-matter `artifacts[1].contains`). None of this reflects the nine-value
   enum. Yet `136-GATE1-DECISIONS.md` §E states outright: *"**Plans that must implement this
   ruling**... **136-04** — Novelty:... Must build the seven-value [now nine-value] shade classifier
   (not a boolean/tri-state one) and pin a NEW prompt hash."*
2. `136-12-PLAN.md` is the same story: `must_haves.truths` (line 17) says *"Every evidence row of every
   family carries a **tri-state** novelty status"*; Task 1 is literally named *"Novelty **tri-state**
   ingestion across all evidence families"* (line 69); the action text (line 41) and verification text
   (line 254) both say "tri-state." `136-GATE1-DECISIONS.md` §E names 136-12 as the plan that must
   enforce the widened CHECK and wire the new `divergence_correctness` column.
3. `docs/specs/discovery-sidecar-schema-v1.md` — the frozen **contract spec** itself — still states, in
   both places the enum appears: line 709, *"a TRI-STATE CLOSED vocabulary: `not_in_finding_aids` /
   `already_recorded` / `not_checked`"*, and lines 746–747, the literal SQL `CHECK (novelty_status IN
   ('not_in_finding_aids','already_recorded','not_checked'))`. `136-GATE1-DECISIONS.md` §E explicitly
   names this exact file and this exact CHECK constraint as one of the things the ruling amends.

   `.planning/ROADMAP.md` line 192 (Phase 136 success criterion 6) is a fourth instance of the same
   staleness, though ROADMAP is out of scope for this pass to edit: *"tri-state and fail-closed
   (`not_in_finding_aids` / `already_recorded` / `not_checked` — never novel by default)"* — not updated
   for §§E/E′/F/G either.

This is not a subtle drift — it is the literal artifact the objective describes: a nine-value
vocabulary was designed across a long conversation without the plan that owns novelty (136-04) or the
contract spec it must satisfy being re-read or updated in step. **A future session must not execute
136-04/136-06/136-12 as currently written** without first reconciling them to
`136-GATE1-DECISIONS.md` §§E/E′/F/G — otherwise the build will implement the RETIRED three-value
vocabulary that decisions E/E′/F/G explicitly superseded, silently regressing NOVEL-01 to a state the
owner already ruled inadequate twice over (once when D-23a replaced the original boolean with a
tri-state, and again when decision E replaced the tri-state with a shade enum).

---

## 1. Inventory — every prior novelty artifact

| Path | What it is | When / generation | Trustworthiness |
|---|---|---|---|
| `same_work_spike/probe/scripts/title_gate.py` (404 lines) | **MAPV2-9**, deterministic (no LLM) router comparing a claimed work against NLI/FJMS catalogue titles: `generic_or_absent` / `same_work` / `name_variant` / `known_quoter` / `different_specific`. Validated against 88 MAPV2-A gold annotations (its own `_validate()`). | Oldest generation ("MAPV2", gen-1 era) | **Superseded** by the LLM gate below for the discovery-scoring pipeline, but its `GENERIC_TOKENS`/`QUOTER_RULES` vocabularies are the direct ancestor of "generic catalogue = real find" and "known quoting relation" reasoning still embedded in the 5-way LLM prompt (`title_gate_llm.py`). Read-only reference; never imported by committed code. |
| `same_work_spike/probe/scripts/bib_gate.py` (347 lines) | **MAPV2-11**, `BibGate.classify()` — classifies a (sys_id, claimed work) pair against Friedberg bibliography rows into `known_bib` / `known_bib_genre` / `published_full` / `bib_partial` / `bib_mentions` / `bib_empty`. Its own docstring states the KILL rule design principle: *"Bib presence alone must NOT kill... TranscriptionType=Full rows likewise only inform, never kill."* | MAPV2, validated on "12 gold cases" | **Design principle sound; a later consumer violated it.** See §6 below — `gen2_novelty_gate.py` treats `published_full` as NAMED, which Codex flagged as a BLOCKER precisely because it contradicts this file's own documented rule. |
| `same_work_spike/probe/scripts/discovery_identified_gate.py` (242 lines) | **MAPV2-15l**, "the already-identified gate" — sweeps discovery-scored candidates and demotes any a source already NAMES, across `bib` / `catalog_refs` / `fgp` / `pgp`, all keyed `sys_id==AlmaId`. This is the file `136-CONTEXT.md` (lines 704–705) and 136-04's own `<read_first>` cite as "the pre-built NOVEL-01 funnel." | MAPV2 | **Reused verbatim, not re-validated, by `gen2_novelty_gate.py`** (below) — and that reuse is exactly what Codex flagged as broken (the `catalog_refs` field is mischaracterized; see §2/§6). The functions (`name_match`, `load_catalog_refs`, `load_fgp`, `load_pgp`) are sound in isolation; the INPUT CONTRACT a caller must honor (pass the candidate's actual title/author, not a collapsed representative) is where the later reuse failed. |
| `same_work_spike/probe/scripts/title_gate.py` → `same_work_spike/probe/scripts/title_gate_llm.py` (291 lines) + `discovery_llm_title_gate.py` (113 lines) | **MAPV2-15o**, the LLM title-understanding gate. Runs AFTER `discovery_identified_gate.py`, on rows STILL bucket2=='discovery'. Five verdicts: `known` / `witness` / `discovery` / `different` / `uncertain`. Model **`google/gemini-3.5-flash` via OpenRouter** (default arg, `title_gate_llm.py` line 210; docstring line 9 says "Gemini 3.5 Flash"). Cached by `(work_title, catalogue-title)` signature; an `ALIAS_SEED` list pins ~11 classic Judeo-Arabic⇄Hebrew title pairs before any model call. | MAPV2, dated docstring comment "v3 = v2 taxonomy + few-shot examples (2026-07-16)" | **The one MEASURED five-way result in the repo** (§3 below). Validated per its own docstring: "pilot-validated: 28x faster than per-card Opus, 0 container-witness buried, 84% agreement" (`title_gate_llm.py` line 9) and, in the later refined round, "validated 99% vs Hillel's 103 grades" (`discovery_llm_title_gate.py` line 8). **Caveat: validated under `gemini-3.5-flash`, NOT the `gemini-3.6-flash` + `reasoning:{effort:"low"}` combination 136-04 is pinned to use** — a different model generation. No Codex adversarial review of this specific file exists in the repo (unlike the heuristic funnel below, which got one and failed it). |
| `same_work_spike/probe/results/discovery_llm_title_gate_report.md` | The measured report: 26,476 discovery rows entering, 20,410 in gate scope (had a catalogue title), verdicts `{discovery: 6710, witness: 1327, different: 8032, known: 4339, uncertain: 2}`. | Same MAPV2 run | **A real, measured artifact — but scores a now-superseded pipeline generation's residual population** (the MAPV2 `discovery_scored_gated.jsonl` bucket, not the current live `discovery.db` v2 asset, and not the GEN2/`g_launch3.db` v2.1 evidence). Treat the counts as evidence that a "witness" class is real and non-trivial (6.5% of in-scope rows in THAT population), not as a base rate for the current or future corpus. |
| `same_work_spike/probe/rsource/scripts/gen2_novelty_gate.py` (197 lines) | **The GEN2/rsource "step 2a" heuristic funnel** — reuses `discovery_identified_gate.py`'s loaders VERBATIM over the newer `g_launch3.db` (discovery-v2.1) shipped claims. `CHECKED_SOURCES = ['bib(Friedberg)', 'catalog_refs', 'fgp', 'pgp']` (line 58). Grain: `(sys_id, canonical_work_id)`. Emits a 19,518-row residual for a planned "2b" LLM pass. | GEN2/rsource, 2026-07-28 | **REWORK'd by Codex — see §1a and §6.** This is the artifact `.planning/phases/136-.../136-04-PLAN.md`'s own `<read_first>` cites as *"Probe prototype: ... (heuristic-only, flawed per Codex — reference, not production)"* (HANDOFF-TO-135.md line 83). Its measured 84.0% is_new figure is explicitly disowned by the Codex review as untrustworthy (§6, item 6). |
| `same_work_spike/probe/rsource/CODEX-BRIEF-17-novelty-2a.md` + `CODEX-REVIEW-17-novelty-2a.log` (3,652 lines) | Adversarial review of `gen2_novelty_gate.py` by an external reviewer (`gpt-5.6-sol`, reasoning effort high). Ends `VERDICT: REWORK` (the log records the full review twice, at lines 3546–3597 and 3600–3651 — apparently two review passes concatenated in one log; both reach the identical verdict and near-identical text, so this is read as one authoritative REWORK, not two independent opinions). | GEN2/rsource, 2026-07-28 | **Authoritative negative finding.** 3 BLOCKER + 2 more BLOCKER + 2 HIGH (7 numbered findings, several tagged BLOCKER). This is the single most load-bearing document for "is the heuristic funnel usable as-is" — the answer is no, and HANDOFF-TO-135.md (below) already accepted that verdict rather than re-litigating it. |
| `same_work_spike/probe/rsource/data/gen2_novelty_residual_pairs.json` | The 19,518-row residual (confirmed by direct load: `len(json.load(...)) == 19518`). Sample row: `{"sys_id": "990051118260205171", "canonical_work_id": "M:Ytext1000", "work_title": "מקרא, דברים", "work_author": "מחבר לא ידוע", "catalog_titles": [...]}`. | GEN2/rsource | **Downstream of the REWORK'd funnel — inherits every defect Codex found** (wrong representative title on `M:Ytext1000`-style collapsed canonical ids; catalogue-VOLUME titles and entry numbers mixed in under the field name `catalog_titles`, stripped of provenance and truncated to 12 — Codex finding 5). Not fit to feed an LLM gate as-is; `HANDOFF-TO-135.md` §6.1 already says the residual needs to be rebuilt from source-specific evidence after fixing canonical identity. |
| `same_work_spike/probe/rsource/HANDOFF-TO-135.md` §6 item 1 ("Novelty gate") | The considered, POST-REWORK guidance handed to the milestone: compute at raw `ref_work` grain (not `canonical_work_id`); read FJMS `catalog.TitleHeb`/`GenizahTitleOrgTitle` (NOT `catalog_refs`, which "matched ZERO"); `published_full` (bib) and bare PGP descriptions over-demote; reuse `discovery_identified_gate.py` + `title_gate_llm.py` but **"rewire to `gemini-3.6-flash` + `reasoning:{effort:"low"}`, ~$27 one-time."** | GEN2/rsource → v9 milestone handoff, cited by `136-CONTEXT.md` line 652 as *"§6.1 is load-bearing for NOVEL-01"* | **Authoritative for DESIGN GUIDANCE; carries NO fresh measurement of its own** — it is a synthesis of the Codex REWORK findings plus a cost estimate reused from an earlier gate-cost reference, not a new validated run. Treat its prescriptions as a correctly-derived TODO list, not as evidence the prescribed fix has been built or tested. |
| `same_work_spike/probe/rsource/GEN2-HANDOFF.md` §6 | The fuller internal engine-track record; §6 items 3–4 log the (separate, DIFFERENT) "reference-granularity" and "witness-vs-quoter precision lever" future items — see §8 below. | GEN2/rsource | Internal working record; consistent with `HANDOFF-TO-135.md`, more verbose. |
| `.planning/phases/134-discovery-data-spine/134-VALIDATION.md` line 23 | References a corroboration predicate keying on `_bucket=='witness'` (a SEPARATE "witness" concept from the LLM gate's — see §4/§8) already baked into the LIVE `discovery.db` build (`docs/specs/discovery-sidecar-schema-v1.md` §4.2). | Phase 134, shipped | **Already shipped in production** — this is not prior art to reconcile; it is a fifth, currently-live meaning of "witness" that a future novelty implementer must not confuse with the LLM gate's `witness` verdict (§8). |
| `.planning/phases/136-.../136-GATE1-DECISIONS.md` §§A–G | The in-conversation ratification this pass was asked to reconcile: five gate-1 evidence decisions (A), the novelty-funnel spend authorization (B), the evaluation-set size (C), the needs-ruling domain posture (D), and the shade-enum design (E/E′/F/G). | 2026-08-02, same day, multiple dispatches | **Authoritative for what the OWNER ruled; not authoritative for whether the ruled design is buildable as specified** — that is exactly this pass's job, and §5/§6 below flag specific unresolved tensions with the prior art. |
| `.planning/phases/136-.../136-04-PLAN.md` | The plan that actually owns novelty implementation. | Current (top-level) | **Stale relative to `136-GATE1-DECISIONS.md` §§E/E′/F/G — see Headline finding.** |
| `.planning/phases/136-.../superseded-2026-08-02/136-04-PLAN.md` | An EARLIER, differently-numbered plan-04 (actually the ancestor of what is now plan 03 — the gate-1 evidence-gathering plan; confirmed by reading its own front-matter: `files_modified: scripts/discovery_gate1_evidence.py, ...136-GATE1-EVIDENCE.md, 136-GATE1-DECISIONS.md`, i.e., it is 136-03's predecessor, not 136-04's). Contains no mention of "tri-state" or "shade" at all. | Superseded 2026-08-02 | Historical only — the plan numbering shifted when the phase was replanned around the owner's 2026-08-02 re-scope; not itself a novelty artifact. |

---

## 2. Checked-source set: promised vs implemented

NOVEL-01 (base text, `.planning/REQUIREMENTS.md` line 37) promises: *"FJMS and NLI catalogue +
bibliography, titles, PGP, FGP, and M-source shelfmark attributions."* Six named source families.

| Promised source | Implemented where | Status |
|---|---|---|
| FJMS **bibliography** (Friedberg) | `bib_gate.py::BibGate` via `discovery_identified_gate.py` → `gen2_novelty_gate.py` | **Implemented, but incorrectly wired** — the heuristic funnel treats `published_full` as NAMED (Codex BLOCKER 1; 3,688 known pairs affected, 3,060 sole-source), directly contradicting `bib_gate.py`'s own documented design rule ("Bib presence alone must NOT kill"). |
| FJMS/NLI **catalogue** (structured identification) | Was implemented as `catalog_refs` (FJMS `catalog_refs` table, `CatalogEntry`/`CatalogTitle` columns) | **Implemented as the WRONG field, and it doesn't work.** Codex measured this directly: *"in the database these are entry numbers such as `2627` and titles of catalogue volumes such as *Catalogue of the Hebrew Manuscripts…*, not the catalogue entry's manuscript identification. Unsurprisingly, catalog matching produced **zero** known pairs"* (CODEX-REVIEW-17 line 3554/3608, finding 2). `HANDOFF-TO-135.md` §6.1 already states the correct field to use instead: FJMS **`catalog.TitleHeb` / `GenizahTitleOrgTitle`** — the catalogue's OWN identification, not `catalog_refs`. **136-04-PLAN.md's `<read_first>` already cites this correction** (line 92 of the current plan), so the plan-level TEXT is aware of the fix even though its Task-1 action text (a different section of the same file) still only defines the tri-state vocabulary, not source-reading logic. |
| **Titles** | The public `libraries.csv` column 7 (`titles_non_placeholder`, called "nli title" in `title_gate_llm.py`/`discovery_llm_title_gate.py`, "catalogue_text" in `discovery_gate1_evidence.py`) | **Implemented, exactly once, in the OLDER five-way LLM gate — and it is the ONLY source Phase 136's OWN hard-case selectors (`scripts/discovery_gate1_evidence.py`) read.** See §7 — this means the owner's ground-truth labelling sample has zero cases characterized by FJMS/bib/PGP/FGP evidence at all. |
| **PGP** | `discovery_identified_gate.py::load_pgp` → `gen2_novelty_gate.py` | **Implemented, but over-broad.** Codex BLOCKER: *"Any PGP description or transcription demotes every work claimed on that fragment... It proves that PGP describes the fragment, not necessarily that it names this work. PGP affects 2,014 known pairs and is the sole named source for 942"* (finding 1). `HANDOFF-TO-135.md` §6.1 already names this as a required fix ("bare PGP descriptions over-demote"). |
| **FGP** | `discovery_identified_gate.py::load_fgp` → `gen2_novelty_gate.py` | **Implemented and functioning as designed, but weak in practice.** Codex measured: *"FGP produces 1,177 known versus 9,373 studied/name-match failures"* (finding 6) — i.e. FGP text exists for far more fragments than the mechanical `name_match` token-bag test can actually connect to the claimed work. This is not a bug in the FGP loader itself; it is evidence that a purely mechanical name-match under-connects FGP text, which is exactly the gap the LLM gate (and the residual mechanism) exists to close — but the CURRENT residual (see above) is built wrong, so this gap is not yet closed in practice. |
| **M-source shelfmark attributions** | **Not found anywhere in the reviewed code.** Neither `discovery_identified_gate.py`, `bib_gate.py`, nor `gen2_novelty_gate.py` reads any M-source-specific shelfmark-attribution source. `HANDOFF-TO-135.md` and `GEN2-HANDOFF.md` do not mention it either. | **Not implemented, not designed, not even referenced in the read prior-art.** This is the one NOVEL-01 clause with literally zero prior engineering to inherit from — 136-04 would be starting from nothing on this specific source, not reconciling an existing (if flawed) implementation. |

**Direct answer to the owner's flagged concern** ("FGP, NLI catalogue/bibliography and M-source
textual witnesses appear unaccounted for"): **FGP and NLI bibliography (Friedberg) ARE accounted for
in prior code** — imperfectly (FGP under-connects via mechanical matching; bib over-demotes via
`published_full`) but genuinely present and measured. **The NLI/FJMS CATALOGUE proper (the structured
identification field) is accounted for only via the wrong field (`catalog_refs`), which measured ZERO
matches** — functionally unimplemented despite looking implemented. **M-source shelfmark attributions
are not accounted for anywhere in the read prior art** — this clause of NOVEL-01 has no prior
implementation to reconcile against; it needs to be designed from scratch, and 136-04's plan does not
currently name a source for it either (its `<read_first>` cites HANDOFF-TO-135 §6.1, which itself does
not mention M-source shelfmarks as a distinct data path — it only mentions "M-source shelfmark
attributions" in the requirement text it is quoting back, not as a described implementation).

---

## 3. The five-way vocabulary — exact definitions, provenance, cost, trust

The system prompt is in `same_work_spike/probe/scripts/title_gate_llm.py` lines 65–85
(`SYS_PROMPT`). Quoted verbatim (the decisive test sentence and the five verdict definitions):

> *"THE TEST: reading ONLY the manuscript's CATALOGUE title, could a cataloguer predict/expect this
> specific composition (work_title) here?"*
>
> - **`known`**: *"the catalogue EXPLICITLY names THIS composition — you'd know it's this exact
>   work."* Includes a direct name; a Judeo-Arabic⇄Hebrew alias of the same work; an
>   abbreviation/alt-title; a section/volume name of the same work; explicit author+work naming; OR a
>   Bible-section catalogue entry for the Aramaic Targum of a book in that section.
> - **`witness`**: *"the catalogue names a full standard RITE prayer-book or a SPECIFIC
>   ceremony/occasion whose content is predictable — you'd expect this exact unit there."* Examples
>   given: a standard-rite siddur/machzor predicting its standard yotzrot; a dirge collection
>   predicting a specific dirge; a named ceremony predicting its ritual text.
> - **`discovery`**: *"the catalogue is GENERIC and does NOT let you predict this specific content —
>   identifying it is a real find."* Examples: "collection of Genizah fragments," "prayer fragments,"
>   a bare "פיוט," a GENERIC "Arabic translation of X" that does not name the specific author/work, or
>   a broad rite+genre grab-bag ("Karaite selichot") that names only genre+rite, not a specific
>   occasion.
> - **`different`**: *"the catalogue names a genuinely DIFFERENT specific composition."*
> - **`uncertain`**: *"cannot tell."*
>
> Decisive rule (verbatim): *"a GENERIC collection title... is NEVER witness or known — it is
> 'discovery.' Reserve known/witness for titles that specifically name the work OR a specific
> predictive rite/ceremony/Bible-section."*

**Production/consumption:** `discovery_llm_title_gate.py` (MAPV2-15o) applies `get_verdict()` to every
row still `bucket2=='discovery'` after the mechanical `discovery_identified_gate.py` (15l) pass, using
the manuscript's NLI/library title (`libraries.csv` column 7) as the sole catalogue signal. `known`
verdicts are KEPT in the deck, flagged `llm_flag='likely_catalogued'`, deliberately **not demoted out**
so a misfire stays auditable (docstring line 11: *"AUDITABLE — NOT demoted out, so an LLM known-misfire
stays visible."*).

**Model, config, cost:** `google/gemini-3.5-flash` via OpenRouter, `temperature=0`, batches of 40
(`title_gate_llm.py` lines 107–121, 210–211). No per-run dollar cost is recorded anywhere in the
repo for this specific report — the only dollar figure on record anywhere in the project
(`reference_discovery_llm_gate_cost` memory, `136-GATE1-DECISIONS.md` §B) is **~$27 for
`gemini-3.6-flash` + `reasoning:{effort:"low"}`**, a DIFFERENT and NEWER model generation than the one
that actually produced these five-way counts. **Do not conflate the two: the measured 5-way counts are
a `gemini-3.5-flash` result; the pinned 136-04 contract is `gemini-3.6-flash` at low effort. No cost
and no accuracy number exists yet for the model 136-04 will actually pin, against ANY version of this
vocabulary.**

**Trustworthiness:** the vocabulary itself has two independent, MEASURED validation claims in the
code comments — *"pilot-validated: 28x faster than per-card Opus, 0 container-witness buried, 84%
agreement"* (`title_gate_llm.py` line 9, an earlier round) and *"validated 99% vs Hillel's 103 grades"*
(`discovery_llm_title_gate.py` line 8, a later, refined round with few-shot examples added
2026-07-16). Both are self-reported in code comments, not written up as a standalone validation
report the way the GEN2 track's E1-L/E1-R2/E1-R3 grading rounds were — treat the 84%/99% figures as
PLAUGHT genuine (they are specific, dated, and consistent with the project's general practice of
citing real numbers) but **not independently re-derivable from anything else in this repo**; no gold
label file or grading script for this specific gate was found in the areas searched. No adversarial
Codex review of this file exists (unlike the heuristic funnel, §6).

---

## 4. `witness` in depth

**What it covers:** a catalogue entry that names a *container* (a full standard-rite prayer book, a
named ceremony, a Bible-section-as-Targum-carrier) whose STANDARD, EXPECTED contents include the
specific unit being claimed — without the catalogue ever naming that specific unit itself. The prompt's
own worked examples: *"יוצרות לשבתות ⇐ סדור מנהג אשכנז המזרחי לכל השנה => witness (a full standard rite
siddur predicts its yotzrot)"*; *"יוצרות לארבע פרשיות ⇐ מחזור מנהג אשכנז לשלש רגלים => witness (a
standard machzor predicts its festival yotzrot)."* From the measured report's own samples
(`discovery_llm_title_gate_report.md` lines 39–46): `יוצר ח פסח` against `מחזור מנהג אשכנז לשלש רגלים`;
`אופן ליום טוב שני של שבועות` against `סדור מנהג אשכנז המזרחי לכל השנה`.

**How it is detected:** entirely by the LLM prompt's own trained judgment plus a small deterministic
`ALIAS_SEED` list for known Judeo-Arabic⇄Hebrew title pairs (which only ever fires `known`, never
`witness` — `title_gate_llm.py` lines 51–63, 132–139). There is no mechanical/string-based detection
path for `witness` anywhere in the codebase; it is a pure model judgment requiring liturgical-calendar
domain knowledge (which festival's machzor carries which class of piyyut).

**Measured population:** 1,327 of 20,410 in-scope rows (6.5%) in the MAPV2 residual population that
was actually scored. This is a real, non-trivial fraction — not a corner case — **in that population**;
no measurement exists of what fraction of the CURRENT live asset's or the future v2.1 asset's shipped
claims would land here, and no measurement exists of what fraction of the corpus's manuscripts are
themselves liturgical containers (siddurim/machzorim/piyyut anthologies) at all, though the
`GENERIC_TOKENS`/`GENRE_OF` vocabularies in `title_gate.py` (dozens of liturgical-genre Hebrew tokens:
`סידור`,`סדור`,`מחזור`,`יוצרות`,`קרובות`,`סליחות`,`קינות`,`הושענות` etc.) suggest liturgical material is
a large, well-represented genre in this corpus generally, which is itself circumstantial (not
measured) reason to expect the underlying scale to be material.

**What would happen to these rows under the nine-shade vocabulary, mapped mechanically:**

- Not `confirms` — the aid never names this specific unit, by the verdict's own definition.
- Not `refines_granularity` / `aid_more_specific` — both require the D-13d **author-gated** rule: same
  non-null `author` field AND an identical/prefix-shared normalized `title`. A machzor's catalogue
  entry and a claimed Yotzer's title share neither an author field (liturgical compilations are rarely
  author-attributed in the catalogue at all) nor a title-prefix relationship (a machzor's title is not
  a truncation of "Yotzer for Shavuot"). The relationship `witness` captures is CONTAINMENT-BY-RITE, a
  structurally different relation from D-13d's WORK-GRANULARITY relation (same underlying composition,
  catalogued at two levels of detail — e.g. Rashi-on-Torah vs Rashi-on-Genesis, same author field, title
  prefix shared). No existing predicate in the nine-shade design tests for rite-containment.
- Not `diverges_work` / `diverges_part` — there is no disagreement; the machzor's title does not
  contradict the claim.
- Not `alias_merge` or `extends` — neither applies (not an alias-of-catalogued-work situation, not an
  other-folio situation).
- **By elimination: `fills_gap`** — *"the aids identify F as nothing at all — the true 'previously
  unknown.'"* This is the failure mode the objective describes: a standard festival machzor absolutely
  predicts carrying a Yotzer for that festival, so identifying the Yotzer is emphatically not
  "previously unknown," yet the nine-shade enum has no other bucket for it to fall into.

**Is `witness` a shade, a separate axis, or a display rule?** Based on how the FIVE-way vocabulary
actually used it (a peer of `known`/`different`/`discovery`, not a modifier of them, and not a
correctness call), the closest fit under the nine-shade design's own internal logic is **a tenth
shade**, not a display rule and not folded into an existing axis — see the reconciliation proposal in
§5. It is explicitly NOT the same concept as `divergence_correctness` (there is no disagreement to
adjudicate) and NOT the same concept as the D-13d granularity relation (no shared author/title).

---

## 5. Reconciliation — mapping the two vocabularies, and a proposal

### 5a. Mapping table

| Five-way (measured, `gemini-3.5-flash`, over an older residual) | Nine-shade (ratified, `gemini-3.6-flash effort-low`, not yet run) | Relationship |
|---|---|---|
| `known` | `confirms` | **Same concept**, and ruling G's clarification (structured field coarse, free text precise ⇒ `confirms`) is a strict SUBSET refinement of `known`'s own definition, which already included section/volume names and alias/language variants. Clean fold. |
| `different` | `diverges_work` (scope=work; usually the whole different composition) | **Same concept at the WORK level.** The five-way vocabulary has no `diverges_part` analog — it never distinguished "different work" from "different, finer part of the SAME work." Folding `different` into `diverges_work` loses nothing new; the nine-shade's `diverges_part` split is a genuine refinement the five-way never had reason to make (Class 6 hard-case work surfaced it, not the title-gate). |
| `discovery` | `fills_gap` | **Same concept — a generic catalogue that predicts nothing.** Clean fold, PROVIDED `witness` is pulled out first (see below); otherwise this fold silently absorbs the witness population into false novelty, which is the exact risk this document exists to flag. |
| `uncertain` | `not_checked` (structured abstention) | **Same concept**, modulo the fact that `not_checked` also covers "unrun"/"failed"/"source unavailable," which `uncertain` never needed to (the five-way gate always ran to completion over its scope). A model-emitted `uncertain` should map to `not_checked`, not to a new value. |
| **`witness`** | **No mapping — this is the gap.** | See §4. Neither `confirms` (aid never names the unit) nor `fills_gap` (the content IS predictable, so it is not "previously unknown") is correct. |
| *(none)* | `refines_granularity` | Genuinely NEW relative to the five-way vocabulary — the five-way gate never tested a same-author/title-prefix granularity relationship; this is Class 3's contribution (D-13d), independently measured (276 of 1,367 identical-span groups collapse under the author-gated rule) and well-evidenced on its own terms. |
| *(none)* | `aid_more_specific` | NEW, E′'s direction-split of the above — likewise independently well-motivated (worked case: catalogue names `בראשית מד`, finer than either claim). |
| *(none)* | `alias_merge` | NEW — Class 2's situation (two catalogued works are actually one). GATE1-DECISIONS' own risk assessment (its "Outstanding" section) already flags this as the single value most likely to be confused with `refines_granularity`/`aid_more_specific` by a low-effort model, unresolved. |
| *(none)* | `extends` | NEW — "other folios of the same manuscript are tied to W, this folio isn't." No prior-art equivalent found; appears to require its own detection logic (same-manuscript, cross-folio join) that neither the five-way gate nor the heuristic funnel implements. |
| *(none)* | `divergence_correctness` (`catalogue_correct`/`claim_correct`/`unclear`) | NEW, orthogonal axis — no prior-art precedent; this is a genuinely fresh construct answering "which side is right," deliberately separated from the shade token per ruling F's own stated rationale (both directions occur under the identical shade). |

### 5b. Proposed reconciliation

Add a **tenth shade**, reusing the ALREADY-VALIDATED name and definition rather than inventing new
vocabulary: `witness` (or, if `witness` is judged too collision-prone against the four OTHER "witness"
meanings already live in this project — see §8 — a renamed but semantically identical
`container_predicts` / `predictable_context`). Condition: *"an aid names a broader
rite/cycle/ceremony/container whose standard, predictable content includes this specific unit, without
naming the unit itself."* Treatment: excluded from "Candidates for new finds" (same posture as
`confirms`/`refines_granularity`/`aid_more_specific`/`diverges_*`/`extends`) — it is clearly not a new
find — but, unlike `diverges_*`, there is no reason to hide it by default (no disagreement to be
cautious about; the objection to `diverges_*`'s default-hidden posture was specifically about
publishing rows the owner has measured reason to believe are OUR false positives, which does not apply
here).

**What should be dropped or left unresolved rather than forced:** nothing in the CURRENT nine-shade set
looks unsupportable enough to drop outright — each of `refines_granularity`/`aid_more_specific`
(D-13d, measured on 1,367 groups), `alias_merge` (Class 2, a real corpus phenomenon), and `extends`
(a real "same manuscript, different folio" situation) has its own independent, non-overlapping
motivation in the gate-1 evidence work, separate from anything the five-way gate or the heuristic
funnel ever measured. The one item this pass recommends NOT forcing a resolution on is
`alias_merge` vs `refines_granularity`/`aid_more_specific` — GATE1-DECISIONS' own risk section already
correctly identifies this as the most likely model-confusion pair and explicitly defers a possible
future collapse to the owner; this pass concurs that forcing that collapse now, without evidence,
would be premature.

### 5c. Can a `gemini-3.6-flash` effort-low gate reliably produce this proposal?

**Unknown — and the project's own infrastructure already says so, correctly.** The evidence available:

- The only MEASURED accuracy for ANY of this reasoning is the five-way vocabulary's 84%/99% agreement,
  under `gemini-3.5-flash` (a different, older model), against a 5-token (not 10-token) vocabulary, with
  a fundamentally different input contract (comparing a claimed title against ONE catalogue title
  string — never "read the aid's full free text alongside its structured field," which ruling G now
  requires as a PROMPT-DESIGN mandate).
- **⟨CORRECTED 2026-08-02, same day, a later dispatch — this bullet OVERSTATED the gap and is amended in
  place, not deleted, per the discipline this pass asks of others.⟩** The original text here read
  *"`gemini-3.6-flash effort-low` has **zero measured accuracy** against ANY version of this task in this
  repo."* That is WRONG and must not be propagated. `gemini-3.6-flash effort-low` **was measured at 100%
  verdict agreement (40/40) against `gemini-3.5-flash` running the fuller-thinking reference config**,
  and that reference config was itself independently validated at **99% against 103 human grades** —
  on the FIVE-way vocabulary, with the ORIGINAL one-title-string input contract (`reference_discovery_llm_gate_cost`
  memory; `136-GATE1-DECISIONS.md` §B cites this measurement, not merely a cost estimate, as the basis for
  "the validated cheap configuration... matches the validated quality"). The `~$27` figure on the SAME
  record is a SEPARATE, COST estimate carried forward by size-extrapolation — that half of the original
  sentence was correct and is unchanged. **The accurate scoping, stated plainly:** the pinned config DOES
  have a real, measured accuracy result — but that result was measured on the FIVE-way vocabulary and the
  one-title-string input contract. It does **NOT EXTEND** to the WIDENED shade enum (now ten values,
  E/E′/F/H) or to ruling G's free-text input contract (reading the aid's full free text alongside its
  structured field) — both of which the pinned config has never been measured against. This scoping gap,
  not a total absence of measurement, is what motivated owner ruling I (`136-GATE1-DECISIONS.md` § I):
  re-measure the pinned config against the owner-labelled evaluation set on the widened vocabulary and
  input contract BEFORE the production run. Wherever this document (or any other) cites the pinned
  config's accuracy, cite it with this scoping — "validated on the five-way vocabulary/one-title-string
  contract, not yet re-measured on the ten-value/free-text contract" — never as "zero measured accuracy"
  and never as "the ~$27 figure IS the accuracy evidence."
- `136-CONTEXT.md` D-23c already states, in its own words, that agreement on the OLD 40-card evaluation
  is *"too weak for an axis this reputationally loaded"* and calls for *"a substantially larger
  owner-labelled hard-case evaluation."* `136-GATE1-DECISIONS.md`'s own "RISK CHECK on the growing shade
  enum" section (in its "Outstanding" record) independently reaches the same posture for the WIDER
  enum, reporting three specific confusability risks rather than resolving them, and states plainly:
  *"this assessment is reported for the owner's decision... no collapse is applied in this plan."*
- Adding a TENTH shade (this pass's own proposal, §5b) would be a genuinely UNTESTED addition on top of
  an already-untested nine-value enum — it should not be treated as validated merely because the
  underlying CONCEPT (`witness`) was once measured; that measurement was under a different model, a
  different vocabulary size, and a different input contract.

**Recommendation, not a resolution:** before trusting `gemini-3.6-flash effort-low` on the full
proposed ten-value + correctness vocabulary in production, the owner-labelled hard-case set (currently
83–97 cases, entirely drawn from the catalogue-text axis — see §7) should be extended with a small,
liturgical-container-specific class (siddur/machzor/piyyut cases) built with the same zero-model-call,
reproducible selection discipline already used for Classes 4–6, so that the FIRST time this model
encounters the `witness`-shaped question is in a graded evaluation, not in the production run.

---

## 6. Codex REWORK blockers — status against 136-04's plan

Source: `same_work_spike/probe/rsource/CODEX-REVIEW-17-novelty-2a.log`, `VERDICT: REWORK`
(lines 3597, 3651 — the log contains the full review text twice; both instances agree). Against
`136-04-PLAN.md` (current, top-level) as it stands TODAY:

| Blocker (Codex) | Measured detail | Disposition |
|---|---|---|
| **1. `published_full` wrongly treated as NAMED** | Affects 3,688 known pairs, 3,060 sole-source. Contradicts `bib_gate.py`'s own documented KILL-rule design. | **Named as a required fix in 136-04's `<read_first>`** ("bibliography `published_full` and bare PGP descriptions are NON-decisive"), but the fix is a `<read_first>` NOTE, not yet expressed as a testable acceptance criterion anywhere in the plan's Task 1/2/3 text. **Open** — the plan states the intent, not a mechanism or a test. |
| **2. Reused functions, not the reused input contract** — `name_match`/loaders/BibGate mirror the validated gate, but the validated gate received the CANDIDATE's actual title/author; `gen2_novelty_gate.py` supplies an arbitrary canonical-representative title instead. Also: `catalog_refs` measured **zero** known pairs (wrong field). | 92,684 pairs affected in aggregate; catalog matching = 0. | **Partially addressed.** 136-04's `<read_first>` correctly redirects the catalogue source to FJMS `catalog.TitleHeb`/`GenizahTitleOrgTitle` (citing `HANDOFF-TO-135.md` §6.1) instead of `catalog_refs` — this specific sub-finding is resolved AT THE DESIGN-INTENT level. The "supply the raw candidate's own title, not a collapsed representative" half is addressed by 136-04's own instruction to "Compute at the raw `ref_work` grain and resolve to `novelty_work_key`, not to `canonical_work_id`" (Task 2 action text) — also resolved at the design-intent level, not yet built. |
| **3. Canonical→title mapping unsound** — `M:Ytext1000` collapses 39 Bible-book titles to one representative ("מקרא, דברים"), affecting 23,129 pairs; similar collapses for Mishnah (`M:Ytext31000`, 2,048 pairs) and Tosefta (`M:Ytext28000`, 522 pairs); author selection has the same flaw (529 Tur pairs get a blank author despite an available name). | 25,699 pairs (27.7% of the output) affected by the three named groups alone; 27,128 pairs have more than one possible title project-wide. | **Resolved by design, contingent on execution matching the design.** This is the SAME defect blocker 2's "raw `ref_work` grain, not `canonical_work_id`" fix addresses — 136-04's Task 2 explicitly calls this out. Not yet built or re-measured. |
| **4. Page→Alma mapping fine; effective work grain is not** | 198,238 distinct shipped pages, zero missing/null/conflicting mappings — the KEY JOIN is sound. The problem is entirely downstream (blocker 3). `page2sys.get()` silently drops future mapping failures rather than asserting zero loss. | Tagged HIGH, not BLOCKER. | **Not explicitly addressed.** 136-04's plan does not mention an explicit zero-loss assertion on the page→sys_id join. Low residual risk given the join itself measured clean, but worth a one-line acceptance criterion (`assert every page maps` or an explicit `not_checked` fallback with a logged count) when 136-04/136-12 are actually written. |
| **5. The residual is not a fit LLM-gate input** — excludes bib text for 37,643 bib-studied pairs; includes catalogue-VOLUME titles/entry numbers under the misleading field `catalog_titles`; never presents PGP descriptions (any description is deterministically NAMED under blocker 1's own bug); mixes FGP titles in with catalog metadata, stripped of provenance, truncated to 12. | 19,518-row residual, `gen2_novelty_residual_pairs.json`. | **Not addressed by any plan text read.** 136-04's Task 2 says "Read the catalogue's own identification field, not `catalog_refs`" (fixes half of this) but does not describe rebuilding a residual with per-source provenance preserved, or presenting bib rows / PGP descriptions to the judgment step at all. **Open**, and directly entangled with ruling G's NEW requirement (present the aid's FULL free text, not an id-only join) — 136-04 will need to design this residual/evidence-assembly step from scratch; nothing existing is fit to reuse as-is. |
| **6. The 84.0% is_new figure is not trustworthy** | Catalog: 0 matches. FGP: 1,177 known vs 9,373 fail. `M:Ytext1000` alone: 22,621 new pairs, 29.0% of all is_new=1, at a 97.8% new rate (almost certainly an artifact of blocker 3, not real novelty). `published_full`/PGP create SYSTEMATIC false-KNOWN in the other direction. | 92,684 total pairs (discovery-v2.1 population — see caveat below). | **This number must never be cited as evidence of anything about the current or future corpus.** It describes a REWORK'd method applied to a population (`g_launch3.db`/discovery-v2.1) that is itself not yet the population 136-04 will run against (see §7). Not "resolved" by 136-04 — simply inapplicable; there is no equivalent number yet for what 136-04 will actually build. |
| **7. Non-destructive/codename mostly sound, some hygiene gaps** — `_ensure_tables` DROPs whole tables rather than scoping to `RUN_ID`; the non-mutation check runs AFTER commit (detects, doesn't prevent); residual JSON write is not atomic with the DB commit; `checked_sources` metadata stores labels, not real version/hash/snapshot info. | — | **Partially superior in 136-04's own design.** 136-04's own acceptance criteria already require a checkpointed/resumable run and an explicit prompt/model/version pinning contract (`LLM_MODEL`, `LLM_MODEL_VERSION`, `PROMPT_SHA256`, `INPUT_NORMALIZATION_SHA256` as literal constants) — stronger than `gen2_novelty_gate.py`'s bare label list. The DROP-whole-table and post-commit-check issues are build-script hygiene concerns for whichever script ports this logic into `scripts/discovery_novelty_funnel.py`; not yet addressed because that script does not exist yet (confirmed: no `scripts/discovery_novelty_funnel.py` in the repo as of this pass). |

**Summary:** three of seven Codex blockers (2, 3, and — partially — 1) are addressed **at the
design-intent level** in 136-04's `<read_first>`/action text (they cite the correct fix). None are
addressed **at the acceptance-criteria/test level** — a future execution of 136-04 as currently written
could still ship the same defects if the implementer does not carefully translate the `<read_first>`
prose into concrete tests. Blockers 4, 5, and the residual-rebuild half of the work are **not yet
addressed anywhere in plan text.** Blocker 6 (the 84% figure) is moot — it is not a number about the
population 136-04 will actually score.

---

## 7. Eval population — what is actually being scored, and what should be labelled

**What the REWORK'd heuristic funnel scored:** ALL 92,684 shipped `(sys_id, canonical_work_id)` pairs
in `g_launch3.db` (discovery-v2.1) — not a residual. It then EMITTED a 19,518-row residual (rows scored
`is_new=1` where some source had SOME text that failed the mechanical `name_match`) as candidate input
for a planned, never-built "2b" LLM pass specific to that pipeline generation.

**What the measured five-way LLM report scored:** a DIFFERENT, OLDER pipeline generation's own residual
— 20,410 of 26,476 MAPV2 `discovery_scored_gated.jsonl` rows still `bucket2=='discovery'` after the
15l mechanical gate, restricted further to rows with a non-empty catalogue title. **These are two
non-overlapping residual populations from two different discovery-scoring engines — do not average or
compare their percentages as if they measure the same thing.**

**What 136-04 is currently planned to score:** Task 3's action text says *"Run the authorized funnel
over the full identification set"* (not "the residual") — i.e., ALL identifications in whichever
`discovery.db` asset is live at execution time (the current v2 asset per the plan's own Purpose
paragraph: *"today all 144,294 shipped direct evidence rows carry `is_new = 0`"*). **Whether the model
arm runs over ALL claims or only a heuristic-funnel residual is not yet decided in any plan text read**
— `GEN2-HANDOFF.md` §6 recommends *"heuristic funnel first to cut calls; scope the LLM pass to
shipped/same-work headline claims,"* but 136-04-PLAN.md does not commit to this design, and it matters
enormously for both cost (the $27 estimate's basis) and for which of the Codex-flagged defects actually
reach the model. **This is a genuinely open design question this pass flags rather than resolves.**

**Is the current 83-case hard-case pool (8 identity spot-check + 20 terse-catalogue + 25
generic-collection + 30 catalogue-divergence, per `136-NOVELTY-HARDCASES.md` and
`136-GATE1-DECISIONS.md`'s "Current state" section) the right ground-truth population?**

It is well-designed for what it tests: `scripts/discovery_gate1_evidence.py`'s selectors
(`select_terse_catalogue_candidates` line 1150, `select_generic_collection_candidates` line 1190,
`select_catalogue_divergence_candidates` line 1265) are all built from **exactly one field**:
`libraries.csv` column 7 (`load_libraries_csv`'s `catalogue_text`, line 760) — the SAME public field
`title_gate_llm.py`/`discovery_llm_title_gate.py` called "nli title." This is zero-cost, script-
reproducible, and deliberately adversarial (hard cases, not a random sample) — a reasonable design for
validating shade DEFINITIONS against genuinely ambiguous catalogue-text cases.

**But it structurally cannot surface the failure modes Codex measured as the biggest problem in prior
art.** None of the 83 cases were selected because of a bib/PGP/FGP/M-source signal — the selectors
never read those tables. This means:

- The exact populations Codex flagged as most damaging — 3,688 `published_full` false-knowns, 2,014 PGP
  false-knowns (942 sole-source), FGP's 1,177-known/9,373-fail split — have **zero representation** in
  the owner's labelling sample. A model gate could pass every one of the 83 labelled cases perfectly
  and still reproduce every one of those specific defects in production, with no empirical signal to
  catch it, because the label set never exercises those code paths.
- The `witness` scenario (§4) also has zero representation — no machzor/siddur/piyyut-container case
  exists among Classes 1–6.

**What population does the labelled number actually answer?** *"Does the owner agree with the proposed
shade labels on genuinely hard cases drawn from the catalogue-text axis"* — a vocabulary-validity
question. It does NOT answer *"how accurately will the pinned model classify the full, multi-source
checked-source funnel in production"* — that requires either broadening the sample to stratify by
SOURCE (bib-sole / PGP-sole / FGP-sole / catalogue-sole / container-predictable / multi-source
agreement) or explicitly documenting, in `docs/specs/discovery-novelty-v1.md`, that the pinned model's
measured agreement covers ONLY the catalogue-text axis and is UNMEASURED on the other four promised
sources.

**Recommendation (not a mandate — the owner's labelling time is the scarce resource here):** rather
than expanding Classes 4–6 further along the same axis, a SMALL supplementary class (10–15 cases,
built with the same zero-model-call, reproducible discipline) drawn specifically from (a) sole-
`published_full` known pairs, (b) sole-PGP known pairs, (c) FGP-present/name-match-failed pairs, and
(d) a handful of clear liturgical-container cases would directly test the four biggest measured risks
this document surfaces, for roughly the same per-case owner cost already accepted for Classes 4–6. This
is presented as an option with its trade-off (more owner time now) against the alternative (ship
without any empirical signal on the sources Codex found most broken), not as a resolution.

---

## 8. What else is being re-derived (sweep beyond novelty)

- **"Witness" is overloaded across at least five distinct, semantically DIFFERENT project concepts.**
  A future implementer grepping "witness" will find: (1) `docs/specs/discovery-sidecar-schema-v1.md`'s
  live, SHIPPED `claim_type`/`evidence_kind` enum member `direct_witness` (a span-competition rule:
  largest span on a page → `direct_witness`, a smaller embedded span → `quotes_this_work` — schema
  lines 324–331); (2) the SAME schema's `evidence_source=propagated` "witness family" (§4.2, lines
  374–416), itself built from a DIFFERENT upstream `_bucket=='witness'` router classification in the
  SEED-029/Q2 pipeline (`134-VALIDATION.md` line 23's `corroborated_predicate`), distinguishing a
  `witness` collection from `tafsir_targum`/`with_arabic` collections; (3) `HANDOFF-TO-135.md` §3's
  informal prose, *"the page **witnesses** the work"*, describing the GEN2 `coverage_route` surface
  label `same_work` (page-coverage ≥0.2984, AUC 0.874, Codex-APPROVED) — itself NOT YET consumed by
  Phase 136 (D-01 deferral, `136-CONTEXT.md` line 654: *"not consumed in 136"*); (4) the OLDER
  MAPV2-15o LLM title-gate's `witness` VERDICT (§3/§4 above) — a novelty-adjacent, catalogue-
  predictability judgment, never shipped; (5) the owner's own 10-way grading vocabulary used to
  validate discovery claims, `co-witness` (`GEN2-HANDOFF.md` lines 101–115: *"is the whole discrete
  work B present on the page?... TRUE (co-witness/partial)"*) — a QC label, not a stored field. **None
  of these five is the same thing, and a future novelty implementer adding a tenth shade (§5b) named
  `witness` would be introducing a SIXTH, different meaning of the same word into the same project.**
  This alone argues for the alternate name (`container_predicts`/`predictable_context`) floated in §5b,
  or at minimum a very deliberate, explicit disambiguation note wherever the new shade is defined.

- **Two DIFFERENT things are both called "granularity."** D-13d/E′'s WORK-granularity rule (same
  author, title-prefix relationship — e.g. Rashi-on-Torah vs Rashi-on-Genesis) is a completely separate
  axis from `HANDOFF-TO-135.md` §6 item 3 / `GEN2-HANDOFF.md` §6 item 3's EVIDENCE-LOCUS granularity
  stage (splitting collapsed mega-works to a real reference locus: Bible→chapter, Mishnah/Tosefta→
  tractate+chapter, Talmud→folio+amud) — explicitly logged as future, not-yet-built milestone work,
  and explicitly MEASURED to **not** raise same-work precision (*"coverage already uses the widest
  single unit → labels unchanged"*). Both are still open, both use the word "granularity," and nothing
  in the read Phase 136 material cross-references the other — a future session searching for
  "granularity" work could easily conflate or duplicate one while believing it addressed the other.

- **The "witness-vs-quoter precision lever"** (the Talmud/Bible mega-work precision gap — intrinsic
  quote-hardness, not a grain problem; needs a page-level witness-vs-quoter signal or co-claim
  structure) is explicitly logged as a SEPARATE, NOT-YET-STARTED future item in `HANDOFF-TO-135.md` §6
  item 4 and `GEN2-HANDOFF.md` §6 item 4, with the explicit instruction *"do NOT expect the granularity
  stage to deliver it."* No Phase 136 plan read references this. Flagged so a future precision-focused
  session on Talmud/Bible headlines does not re-discover this diagnosis from scratch.

- **The 0.8 single-page coverage floor (main-pool classification gate 4) is still explicitly
  unreviewed** — `136-GATE1-DECISIONS.md`'s own "Provisional-value / omission audit" section (after
  the five ratified decisions) states plainly that this value was NOT part of the gate-1 ruling and
  that the reviewers' own recommended ~300-case stratified hand review "before 0.8 becomes a constant"
  was NOT authorized. This is already correctly flagged in the decision record itself — re-flagged here
  only so a future session does not silently start treating 0.8 as settled because it appears
  unchallenged everywhere else.

- **A validated, Codex-approved page-coverage discriminator (`coverage_route`, AUC 0.874) already
  exists and is unused in the currently-live schema**, which instead derives `direct_witness` vs
  `quotes_this_work` from a span-competition rule. This is an ALREADY-CORRECTLY-FLAGGED deferral (D-01:
  discovery-v2.1 is "not consumed in 136" by deliberate decision, not oversight) — noted here only as
  context for this sweep, not as a fresh finding requiring action.

- **Plan renumbering churn.** The `superseded-2026-08-02/` directory holds an entire earlier
  01–31-numbered plan set for this phase; the live directory now runs 01–21. Confirmed by direct diff
  that the superseded `136-04-PLAN.md` is actually the ancestor of what is now `136-03-PLAN.md` (the
  gate-1 evidence plan), not of the current `136-04-PLAN.md` (the novelty-build plan) — the numbering
  shifted wholesale when the phase was replanned around the owner's 2026-08-02 re-scope. Not itself a
  case of redone WORK, but worth knowing so a future session doesn't cite a plan number from before
  2026-08-02 and land on the wrong file.

---

## 9. Where this knowledge should live

`136-04-PLAN.md` currently plans `docs/specs/discovery-novelty-v1.md` as the versioned novelty
contract. Recommended split:

- **`docs/specs/discovery-novelty-v1.md`** (the contract doc 136-04 will write): the FINAL, reconciled
  shade vocabulary and its exact conditions (once reconciled — see Headline finding); the checked-source
  set with per-source status (implemented-and-correct / implemented-and-flawed / not-yet-implemented,
  per §2's table, so a reader never has to re-derive that catalog_refs measured zero or that
  `published_full` over-demotes); the pinned LLM contract (model, version, effort, prompt hash, input
  normalization, cache key); the masking rule; the grading rule (owner-labels-only); and an explicit
  **"known limitations of the ground-truth set"** section stating plainly that the labelled hard cases
  are drawn only from the catalogue-text axis (§7) and that bib/PGP/FGP/M-source-specific failure modes
  are unmeasured against the shipped classifier, until/unless that gap is closed.
- **`.planning/REQUIREMENTS.md`** (already the right home, and already used correctly): the dated
  amendment trail for NOVEL-01/02 — this pass found it in excellent shape, precisely dated and
  cross-referenced to `136-GATE1-DECISIONS.md`. No change recommended to its role.
- **`136-CONTEXT.md`**: should gain a pointer to this file (`136-NOVELTY-PRIOR-ART.md`) alongside its
  existing `HANDOFF-TO-135.md` §6.1 citation (line 652), since this document is now the fuller
  prior-art account D-23 itself only partially summarizes. (Not made by this pass — this pass may only
  add the one new file.)
- **`docs/CODE_INDEX.md`**: currently has NO entry for `shared/discovery_novelty.py`,
  `scripts/discovery_novelty_funnel.py`, or `docs/specs/discovery-novelty-v1.md` (confirmed by grep —
  no matches for "novelty" anywhere in the file). Once 136-04 actually creates these files, CODE_INDEX
  should gain a short entry pointing at this document for "why the vocabulary looks the way it does" —
  CODE_INDEX is the wrong place for the reconciliation NARRATIVE itself (too long, too dated), but the
  right place for a one-line pointer to it.
- **This file itself** (`136-NOVELTY-PRIOR-ART.md`) is the right home for the RECONCILIATION NARRATIVE
  — the inventory, the gap tables, the Codex-blocker-to-plan-status mapping, and the "what else is
  being re-derived" sweep — because it is inherently a point-in-time investigation record, not a living
  contract. A future session should read it ONCE, before touching 136-04/136-06/136-12, and then work
  from the (by-then-reconciled) `docs/specs/discovery-novelty-v1.md` going forward.
