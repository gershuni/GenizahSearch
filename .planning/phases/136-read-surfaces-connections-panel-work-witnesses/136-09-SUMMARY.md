---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 09
subsystem: discovery / findings-page facet data
status: COMPLETE — the halt is resolved; rulings P and Q applied, `--validate --release` exits 0
tags: [curation, fjms-vocabulary, works.genre, author-alias, hash-pinned-artifact, data-quality]
requires:
  - "136-01 (works.genre contract, schema amendment 2026-08-02 (C))"
  - "136-03 (136-GATE1-DECISIONS.md § D — the needs-ruling posture)"
  - "136-GATE1-DECISIONS.md §§ Ruling P, Ruling Q — the 29 rulings (2026-08-03)"
  - "shared/fjms_service.py — the live 39-parent / 202-node domain tree"
  - "discovery-v1-33499c5b… (the deployed v2 asset)"
provides:
  - "discovery_data/work_domains-v1.json — 1,073 canonical works, domain-assigned, hash-pinned, RELEASE-GATE CLEAN"
  - "discovery_data/work_author_aliases-v1.json — the 96-row author alias map, hash-pinned"
  - "scripts/curate_work_domains.py — the curation harness + closed-vocabulary validator + release gate + the tracked OWNER_RULINGS input"
  - "136-DOMAIN-CURATION.md — the curation report, the 29-row needs-ruling list, and § 4.5 the applied rulings"
affects:
  - "136-12 (build wiring: loads works.genre from the pinned artifact; the release gate now passes, so it is unblocked — but must pin sha256:57393773…, NOT the pre-ruling sha256:4cc103ff…)"
  - "136-16 / 136-18 (the findings page's domain / author / work facet cascade)"
tech-stack:
  added: []
  patterns: ["hash-pinned curated build input", "DATA-04 fail-closed", "closed-vocabulary validator", "release gate separate from structural validation", "tracked rulings input so a curated artifact stays regenerable"]
key-files:
  created:
    - scripts/curate_work_domains.py
    - tests/test_work_domains.py
    - .planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-DOMAIN-CURATION.md
    - discovery_data/work_domains-v1.json          # gitignored; pinned by content hash
    - discovery_data/work_author_aliases-v1.json   # gitignored; pinned by content hash
  modified:
    - scripts/curate_work_domains.py               # OWNER_RULINGS + assert_rulings_are_answerable
    - tests/test_work_domains.py                   # 8 tests on the rulings path
decisions:
  - "needs-ruling rows are HELD (domain_leaf null + candidate leaves), never defaulted to Unassigned — the owner declined that default"
  - "structural --validate and the shipping --validate --release are SEPARATE gates, so a held row cannot reach the build"
  - "the FJMS vocabulary is read at runtime with no snapshot and no fallback; the rule table is decisions, and every node it names is checked against the live tree"
  - "a childless top-level FJMS node is itself a usable leaf (parent == leaf), which the feasibility sample's two-level model could not express"
  - "containment alias resolution prefers the LONGEST catalogue name, then the smallest person id"
  - "rulings live in a TRACKED OWNER_RULINGS table the emitter reads, so the artifact stays regenerable instead of hand-edited"
  - "a ruled row KEEPS confidence: needs-ruling — the owner_ruling citation, not the confidence token, is what unlocks release"
  - "a ruling may only pick one of that row's own candidate_leaves; a new option after the fact is a build error"
metrics:
  canonical_works: 1073
  high_confidence: 1012
  medium_confidence: 32
  needs_ruling_ruled: 29
  needs_ruling_held: 0
  needs_ruling_decisions: 23
  distinct_leaf_pairs_used: 61
  author_strings: 96
  tests: 49
  completed: 2026-08-03
---

# Phase 136 Plan 09: Work→Domain Curation and the Author Alias Map — Summary

A one-time curated FJMS domain for each of the 1,073 canonical works carrying a shipped
discovery claim, plus the 96-row author alias map — both hash-pinned, both validated —
and the 29-row list the owner ruled on, **now applied**.

## ✅ Status: the halt is RESOLVED

This plan was **required** to halt. `136-GATE1-DECISIONS.md` § D records the owner's
verdict verbatim: *"THE OWNER WILL RULE. The 'ship as Unassigned' default is explicitly
DECLINED."* The plan's job was to **produce** the work list, not to invent a default. It
did: `136-DOMAIN-CURATION.md` § 4 — 23 decisions covering 29 of 1,073 rows (2.7%).

**On 2026-08-03 the owner ruled on all 29**, in two sections of the same record:

- **§ Ruling P — 5 rows**, settled from FJMS's *own work-level* domain
  (`genizah_titles.DomainId`, a domain attached to a **title**, not to an AlmaId, so it
  does not breach the assignment axis). The owner was right that this source exists and
  the original pass had missed it. Yosippon ×3 → `Historiography and geographical
  descriptions`; Seder Olam ×2 → `Rabbinic Literature / Other`. "Follow FJMS" is scoped
  to this evidence, not a blanket override — two further candidates (מגילת אביתר n=6,
  ספר יצירה n=1) were declined as too thin.
- **§ Ruling Q — the remaining 24**, *delegated*: *"Go with your judgements, I trust
  you."* Recorded as delegated judgements rather than owner-authored ones, with the thin
  calls flagged ⚠ (lowest-confidence: `w001055` ספר הזיכרון, 3 claims, a prior rather
  than evidence).

**Applied and re-pinned in this continuation.** All 29 rows now carry `domain_parent`,
`domain_leaf` and an `owner_ruling` citation; **`--validate --release` exits 0**.

| | before | after |
|---|---|---|
| `content_hash` | `sha256:4cc103ff…` | **`sha256:57393773…`** |
| needs-ruling held | 29 | **0** |
| needs-ruling ruled | 0 | **29** |
| `--validate` | exit 0 | exit 0 |
| `--validate --release` | exit 1 | **exit 0** |
| tests | 41 | **49** |

## How the re-pin was done — the rulings are a TRACKED input, not a hand edit

`--emit-artifact` regenerates every row from the curation tables, so hand-editing the
29 rows into the JSON would have produced an artifact that the next emission silently
destroys. Instead the rulings live in a new **`OWNER_RULINGS`** table in
`scripts/curate_work_domains.py` — the same committed-decisions / gitignored-artifact
shape `CURATION_RULES` and `MANUAL_ASSIGNMENTS` already use, and the only tracked option
available, since `discovery_data/` is gitignored and cannot itself be the record of a
decision. `curate()` reads it, and the hash is re-pinned by
`compute_content_hash()` inside `build_artifact()` — never computed by hand.

**Three build errors guard the table** (`assert_rulings_are_answerable()`): a ruling on a
work that was never held; a ruled `(parent, leaf)` absent from the **live** FJMS tree; and
— the important one — a ruled leaf that was **not among that row's own
`candidate_leaves`**. A ruling answers the question that was put to the owner; it may not
introduce a fourth option after the fact. All 29 pass all three.

**Every ruled leaf validated against the closed vocabulary. None failed** — as Ruling Q
itself predicted, since its leaves were chosen from the artifact's own candidates.

**The change is provably confined to the 29 rows.** Re-running the identical pass with
the rulings table suppressed reproduces the pre-ruling hash `sha256:4cc103ff…`
byte-for-byte, and a row-by-row diff of the two emissions returns exactly the 29 ruled
ids and nothing else.

**`confidence` deliberately stays `needs-ruling` on a ruled row.** The row's provenance is
genuinely different from a rule-derived `high`/`medium` one, and it is the `owner_ruling`
citation — not the confidence token — that the release gate reads. Rewriting the
confidence would erase the only marker that these rows were ever contested. `provenance`
does change, to `owner-ruling:136-GATE1-DECISIONS.md § Ruling P|Q -- <class>`, and
`candidate_leaves` is kept so the artifact still records what the ruling chose between.

**Facet effect.** Distinct `(parent, leaf)` pairs in use rose 55 → 61: Ruling Q's
governing principle (*use the leaf the vocabulary carries for exactly this work*) filled
six nodes that would otherwise have had zero occupants — `Documentary / Documentary`,
`Kalam / Jewish Kalam`, `Medicine / Medical Works`, `Polemics / Polemics
Jewish-Christian`, `Polemics / Polemics Rabbinical`, `Secular Poetry / Other`.

**Carried forward, unresolved by the rulings** (both belong in `docs/OPEN_ISSUES.md`, per
Ruling Q's own note): `w000160` ערוגת הבושם is a title/author collision between
Archivolti's rhetoric and Abraham b. Azriel's piyyut commentary — the domain is ruled,
the collision is not; and `w000154`, a nineteenth-century maskilic memoir carrying claims
in a Genizah corpus, is a corpus-membership question a domain assignment cannot settle.
It was deliberately **not** sent to `Unassigned`, though that was on its candidate list,
because `Unassigned` would hide it.

Nothing else in the plan was ever blocked — all three tasks executed, both artifacts
exist and validate, and 49 tests are green.

## What was built

**`scripts/curate_work_domains.py`** — six modes (`--emit-worklist`, `--emit-artifact`,
`--validate`, `--report`, `--emit-aliases`, `--validate-aliases`), all documented in
`--help`. The closed FJMS domain vocabulary is read from `shared/fjms_service.py` **at
runtime**, with no snapshot and no fallback: an unreadable sidecar raises rather than
validating against stale data.

`--validate` rejects the five structural failure classes the plan names — a leaf outside
the tree, a leaf whose parent disagrees, a key that is not a canonical work id, a missing
or blank confidence/provenance, and a duplicate key — plus the artifact's own content
hash. `Unassigned` validates as a **real value with its own parent**, not as missing data.

**`discovery_data/work_domains-v1.json`** ·
`sha256:573937731e2e31f4ad3fccd6f84aadecc7e67210bf4cda82513dfc5c4d94f605`
— 1,073 canonical works: 1,012 `high`, 32 `medium`, 29 ruled `needs-ruling` (0 held); 61
`(parent, leaf)` pairs of the 202-node FJMS tree used. Every row carries a confidence and
a provenance. *(The pre-ruling pin was
`sha256:4cc103ff4523103c7799ab02ffbc50ec23d794f005b8eb0c72b6100f4a1af104` — superseded;
136-12 must not accept it.)*

**`discovery_data/work_author_aliases-v1.json`** ·
`sha256:acce47f67dcde456eb477fc092294ee42546963f5d977549f53e635da65f8a64`
— the 96 distinct `works.author` strings: 37 `exact`, 39 `containment`, 20 `unmatched`
(79.2% matched). Containment is labelled as containment; an unmatched author is retained,
never forced.

**`136-DOMAIN-CURATION.md`** — the report: coverage, the domain distribution, the full
needs-ruling list with candidate leaves, **§ 4.5 the applied rulings**, the applied
posture, five data-quality findings, threat-model coverage, and what 136-12 must do.

**`tests/test_work_domains.py`** — 49 tests. Pure except the six that read the gitignored
local artifacts. The eight added here cover the rulings path: a ruled row emits its leaf,
its citation and passes the release gate; the three build errors; the rulings table pairs
with the needs-ruling table it rules on (an injected test table never picks up this
module's 29 real rulings); every module ruling settles a module-held row with a leaf that
row offered and a P-or-Q citation; and the posture statement records what was applied.

## Assignment axis, stated explicitly

Every row was assigned from the **identified work's own neutral title and author**, at the
**canonical** grain (`works.canonical_work_id`, so a duplicate is never assigned twice).
The pass never opens `domains`, `catalog` or any `AlmaId`-keyed table — filtering on the
catalogue axis would hide exactly the findings that disagree with the catalogue.

## Independent cross-check

The rule table reproduces **88 of the 91** overlapping works in the independently-produced
93-work feasibility sample **exactly**, and the three disagreements are precisely the three
the sample itself flagged as low-confidence — all three are in the needs-ruling list. The
rule table was not fitted to the sample.

## Deviations from plan

### Auto-fixed

**1. [Rule 1 — Bug] Containment alias resolution picked the least specific catalogue name**
- **Found during:** Task 3, reviewing the first alias emission.
- **Issue:** the containment tie-break was "smallest `person_id`" — order-independent but
  blind to specificity. It resolved `שלמה בן יצחק (רש״י)` (Rashi, 39 works, the corpus's
  second most frequent author) onto FJMS person 147 `שלמה`, the bare given name, while
  person 152 `שלמה בן יצחק` sat in the same candidate list.
- **Fix:** longest catalogue name first, then smallest id — still fully deterministic and
  order-independent. Rashi now resolves to 152.
- **Files:** `scripts/curate_work_domains.py`, `tests/test_work_domains.py`
  (`test_alias_containment_prefers_the_most_specific_catalogue_name`).
- **Commit:** `db4d9c6c`.

### Documented interpretations

**2. [Rule 3] A fourth and fifth mode were added beyond the plan's three.** The plan names
`--emit-worklist`, `--validate` and `--report`, but the artifact has to be produced by
something; `--emit-artifact` and (for Task 3) `--emit-aliases` / `--validate-aliases` were
added. All six are documented in `--help`.

**3. [Rule 3] `--validate` was split into a structural gate and a release gate.** The plan
says `--validate` checks structure only, and threat T-136-09-06 is precisely that a
`needs-ruling` row then ships unreviewed. With the owner's `Unassigned` default declined,
a held row cannot be given any leaf at all — so the artifact must be structurally valid
and simultaneously not shippable. `--validate` passes on a held row; `--validate
--release` fails while any held row is unruled. A held row carrying a concrete leaf
without an `owner_ruling` citation is a validation error in **both** modes.

**4. The "grep finds no snapshotted domain list" criterion is met behaviourally, not by
literal absence of node names.** The rule table necessarily names ~55 nodes as *curation
decisions*. It is not a vocabulary: `assert_rules_within_vocabulary()` checks every one of
them against the live tree before a row is emitted, and two tests prove the vocabulary is
live — one swaps the tree and watches validation follow, one asserts no module-level object
reproduces the tree.

**5. The artifacts are not committed, by design.** `discovery_data/` is gitignored — as it
is for `novelty_hardcase_labels-v1.json` and `v2_canonical_merges.build.json`. The content
hashes recorded in `136-DOMAIN-CURATION.md` § 1 are the pin.

**6. `Unassigned` has zero occupants.** It remains a real, validating, reachable value (a
test asserts it), but the one work that reached the fallback (`w000846`) is a work the
vocabulary can place, so it was curated rather than left in the bucket.

## Data-quality findings — recorded, not fixed

1. **The wrong Bahya, confirmed on the live asset.** `w000022` (תורת חובות הלבבות,
   **981 shipped claims**) records its author as בחיי בן אשר; *Duties of the Hearts* is by
   Bahya ibn Paquda. The domain is unaffected; the row carries the finding as a note.
2. **`האיי גאון` — 59 works, unmatched on one letter.** The corpus's most frequent author
   string fails both alias tests only against a yod-doubling variant of FJMS person 683
   `האי גאון`. Forcing it would be exactly the outcome the plan forbids; this is the
   largest single lever on the alias map's coverage. Same shape, smaller:
   `סלמון בן ירוחים`, `שמעון קיארא`.
3. **The feasibility sample's "no history leaf" note is wrong.** The tree does carry
   `Historiography and geographical descriptions` — a childless top-level node, therefore
   itself a usable leaf, which the sample's two-level model could not see.
4. **Three works centuries later than the corpus** carry shipped claims: a nineteenth-century
   maskilic memoir (`w000154`), and two sixteenth-century works (`w000158`, `w000160`).
   Recorded as provenance questions about the reference corpus.
5. **Six author gaps a title-pattern fill would have closed were left open** — two of them
   name the *addressee* of a question, not its author.

## Masking

`check_atlas_masking.py --scan-asset` exits 0 on both artifacts, on the curation report,
on the script and on the tests. A **positive control** was run in the same session (a real
restricted pattern seeded into a scratch file) and correctly tripped, so the clean results
are true negatives rather than a misconfigured scanner. `MASKING_SCAN_PATTERNS_FILE` must
be set or the scan fails closed — that is an environment requirement, not a gate failure.
Restricted corpora appear nowhere in this plan's output under any name but "M-source".

## Commits

| Commit | Task | Contents |
|---|---|---|
| `a26e3e67` | 1 | harness + closed-vocabulary validator + 39 tests |
| `db4d9c6c` | 3 | author alias map + the containment-specificity fix (41 tests) |
| `0d448517` | 2 | **`136-DOMAIN-CURATION.md` — see the collision note below** |
| *(this continuation)* | — | `OWNER_RULINGS` + `assert_rulings_are_answerable` + 8 tests + the re-pinned artifact's report/summary updates |

**⚠ Commit collision, recorded rather than rewritten.** `136-DOMAIN-CURATION.md` was staged
by this plan and then swept into `0d448517` ("feat(136): batch the pinned novelty gate at
10 (owner ruling O)") by a **concurrent agent** committing in the same working tree at the
same moment. The file's content is byte-identical in `HEAD` and on disk
(`sha256:86bc2e77…`), so nothing is lost — but the commit message does not mention it and
attributes it to unrelated work. **History was deliberately NOT rewritten**: another agent
is actively committing here, and an amend or rebase would destroy its work. Recorded here
so the audit trail is reconstructable.

## What this plan deliberately did NOT do

- **It did not choose any of the 29 held assignments.** That was the owner's call, per
  ruling D — and the owner made it (§§ Ruling P, Ruling Q). This continuation **applied**
  those rulings verbatim; it did not author, extend or reinterpret any of them.
- **It did not default held rows to `Unassigned`.** That default was explicitly declined,
  and `w000154` was kept out of the bucket even though `Unassigned` was on its own
  candidate list.
- **It did not write `works.genre`** or touch the asset, the builder or the verifier — that
  is 136-12.
- **It did not correct the wrong-Bahya author, or any other data-quality finding**, in this
  artifact. An author correction belongs in the asset. Ruling Q's own two carried-forward
  items (`w000160`'s title/author collision, `w000154`'s corpus membership) are likewise
  recorded, not resolved.
- **It did not re-run `--emit-aliases`.** The alias artifact is untouched by the rulings;
  its pin `sha256:acce47f6…` stands.
- **It did not touch any concurrent agent's work** — not `scripts/build_discovery_sidecar.py`
  (plan 136-11), not `scripts/discovery_novelty_funnel.py` / `shared/discovery_novelty.py`,
  and not the detached production job's `discovery_data/novelty_production_*`. Every commit
  staged files explicitly by path.

## Next

1. ~~Owner rules on the 23 decisions~~ — **done, 2026-08-03** (§§ Ruling P, Ruling Q).
2. ~~Each held row gains its leaf plus an `owner_ruling` citation; the artifact is
   re-emitted and re-pinned~~ — **done**, `sha256:57393773…`.
3. ~~`--validate --release` must exit 0~~ — **it does**. 136-12 is unblocked, and must pin
   the new hash, not the pre-ruling `sha256:4cc103ff…`.

## Self-Check: PASSED

All five created files exist on disk; the four original commits (`a26e3e67`, `db4d9c6c`,
`0d448517`, `436251c7`) plus this continuation's commit are reachable in `git log`;
`tests/test_work_domains.py` 49/49 green; `ruff check` clean on both files;
`--validate` and `--validate --release` both exit 0 on the re-pinned artifact; the masking
scan is clean on the artifact, the script, the tests and the report, with a positive
control and a fail-closed check run in the same session. Verified 2026-08-03.
