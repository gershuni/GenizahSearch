# Phase 110: Composition / Parallels Search — LOCAL Corpus Support (desktop) - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-08
**Phase:** 110-composition-parallels-search-local-corpus-support-desktop (REFRAMED mid-discussion)
**Areas discussed:** Phase reframe (defer parallels seeding), Component-B deferral scope, EXP-F3
inclusion, corpus-selector UI, Lab-Mode reconciliation

---

## Phase reframe (user-initiated)

The discussion opened on the original Phase 110 scope ("Search-Support — Parallels Seeding / JSA-01"
+ keep/spike/cut disposition for JSA-02/JSA-03/JWB-05). Before the gray-area selection was answered,
the user redirected:

> "I want to defer parallels seeding. We'll wire LOCAL corpus to parallel search and release."

Interpretation (confirmed by the subsequent answers): defer the Join Workbench parallels-seeding
(JSA-01) and the rest of Component B; repurpose Phase 110 to wire the LOCAL ("My Library") corpus into
the composition/parallels search (the `project_composition_no_local_corpus_path.md` pre-release
intention); then `/release` v8.0.0. Two grounding code-scouts were run before the substantive
questions.

---

## Component-B deferral scope

| Option | Description | Selected |
|--------|-------------|----------|
| Defer all of Component B | Push JSA-01 + JSA-02 + JSA-03 + JWB-05 to a post-v8.0.0 milestone | ✓ |
| Defer only JSA-01 | Still record keep/spike/cut for JSA-02/03 + JWB-05 | |

**User's choice:** Defer all of Component B.
**Notes:** v8.0.0 ships Component A (Join Workbench, Phases 106–109) + rebrand + LOCAL export + the
new LOCAL-composition wiring. ROADMAP.md + REQUIREMENTS.md amended in-session (JSA + JWB-05 → Future).

---

## EXP-F3 (composition-report LOCAL export)

| Option | Description | Selected |
|--------|-------------|----------|
| Include EXP-F3 in this phase | Wire LOCAL into composition AND make `export_comp_report` LOCAL-aware via Phase 103 helpers | ✓ |
| Wiring only; defer EXP-F3 | Add the selector + search path; leave export Genizah-only | |

**User's choice:** Include EXP-F3.
**Notes:** They're coupled — once LOCAL hits appear in composition results, an export that drops them
is a bug. EXP-F3 promoted out of Future into Phase 110 (it was gated on exactly this LOCAL
composition-search UI).

---

## Corpus-selector UI

| Option | Description | Selected |
|--------|-------------|----------|
| Pre-search dropdown + activate dormant post-search filter | Full Search-tab parity | |
| Pre-search dropdown only | Genizah/Local/ALL dropdown scopes the search; no post-search filter activation | ✓ |
| Activate post-search filter only | Search both corpora, filter in UI | |

**User's choice:** Pre-search dropdown only.
**Notes:** Composition is expensive, so pre-search scoping is the right lever; the dormant post-search
comp-filter scaffolding stays inactive (Claude's discretion: consider hiding the misleading control).

---

## Lab-Mode reconciliation

| Option | Description | Selected |
|--------|-------------|----------|
| Have the planner investigate, then reconcile | Flag Lab Mode's true semantics (deep_scan/scan_limit) as research; design the cleanest relationship | (basis) |
| Corpus selector subsumes Lab Mode | Retire the Lab-Mode checkbox; Local/ALL via the selector | |
| Keep Lab Mode as-is, independent | Selector only affects standard mode | |

**User's choice (free-text / "Other"):** "Lab mode should not include LOCAL by default, it should
search in the same fashion of regular mode, i.e. by choosing in which corpus the search will be
performed."

**Notes:** Decision (CONTEXT D-06): the corpus selector governs which corpus is searched for **both**
standard and Lab composition — corpus is **orthogonal to mode**, exactly like regular search. Lab Mode
is **not** hardwired to LOCAL. The planner still verifies (RF-1) what Lab Mode does beyond LOCAL
(deep_scan/scan_limit) and preserves any genuine extra scan semantics as an orthogonal toggle.

## Claude's Discretion

- Selector placement/label on the composition tab; staleness-signal styling; whether to hide the
  dormant post-search comp LOCAL-filter control; internal helper decomposition / parameterization.

## Deferred Ideas

- All of Component B (JSA-01/02/03 + JWB-05) → post-v8.0.0 milestone.
- Post-search LOCAL filter on the composition surface; web composition/parallels LOCAL.
