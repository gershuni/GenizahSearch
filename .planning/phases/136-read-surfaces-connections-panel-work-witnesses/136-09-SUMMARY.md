---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 09
subsystem: discovery / findings-page facet data
status: HALTED — awaiting owner ruling (by design; 136-GATE1-DECISIONS.md § D)
tags: [curation, fjms-vocabulary, works.genre, author-alias, hash-pinned-artifact, data-quality]
requires:
  - "136-01 (works.genre contract, schema amendment 2026-08-02 (C))"
  - "136-03 (136-GATE1-DECISIONS.md § D — the needs-ruling posture)"
  - "shared/fjms_service.py — the live 39-parent / 202-node domain tree"
  - "discovery-v1-33499c5b… (the deployed v2 asset)"
provides:
  - "discovery_data/work_domains-v1.json — 1,073 canonical works, domain-assigned, hash-pinned"
  - "discovery_data/work_author_aliases-v1.json — the 96-row author alias map, hash-pinned"
  - "scripts/curate_work_domains.py — the curation harness + closed-vocabulary validator + release gate"
  - "136-DOMAIN-CURATION.md — the curation report and the 29-row needs-ruling work list"
affects:
  - "136-12 (build wiring: loads works.genre from the pinned artifact; must refuse while the release gate fails)"
  - "136-16 / 136-18 (the findings page's domain / author / work facet cascade)"
tech-stack:
  added: []
  patterns: ["hash-pinned curated build input", "DATA-04 fail-closed", "closed-vocabulary validator", "release gate separate from structural validation"]
key-files:
  created:
    - scripts/curate_work_domains.py
    - tests/test_work_domains.py
    - .planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-DOMAIN-CURATION.md
    - discovery_data/work_domains-v1.json          # gitignored; pinned by content hash
    - discovery_data/work_author_aliases-v1.json   # gitignored; pinned by content hash
  modified: []
decisions:
  - "needs-ruling rows are HELD (domain_leaf null + candidate leaves), never defaulted to Unassigned — the owner declined that default"
  - "structural --validate and the shipping --validate --release are SEPARATE gates, so a held row cannot reach the build"
  - "the FJMS vocabulary is read at runtime with no snapshot and no fallback; the rule table is decisions, and every node it names is checked against the live tree"
  - "a childless top-level FJMS node is itself a usable leaf (parent == leaf), which the feasibility sample's two-level model could not express"
  - "containment alias resolution prefers the LONGEST catalogue name, then the smallest person id"
metrics:
  canonical_works: 1073
  high_confidence: 1012
  medium_confidence: 32
  needs_ruling_held: 29
  needs_ruling_decisions: 23
  distinct_leaves_used: 55
  author_strings: 96
  tests: 41
  completed: 2026-08-03
---

# Phase 136 Plan 09: Work→Domain Curation and the Author Alias Map — Summary

A one-time curated FJMS domain for each of the 1,073 canonical works carrying a shipped
discovery claim, plus the 96-row author alias map — both hash-pinned, both validated —
and the **29-row list the owner must rule on before either can ship**.

## ⛔ Status: HALTED at the designed stopping point

This plan was **required** to halt. `136-GATE1-DECISIONS.md` § D records the owner's
verdict verbatim: *"THE OWNER WILL RULE. The 'ship as Unassigned' default is explicitly
DECLINED."* The plan's job was to **produce** the work list, not to invent a default.

**The work list is `136-DOMAIN-CURATION.md` § 4: 23 decisions covering 29 of 1,073 rows
(2.7%).** Until those rulings land, `--validate --release` fails closed and the artifact
is not shippable.

Nothing else in the plan is blocked — all three tasks executed, both artifacts exist and
validate structurally, and 41 tests are green.

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
`sha256:4cc103ff4523103c7799ab02ffbc50ec23d794f005b8eb0c72b6100f4a1af104`
— 1,073 canonical works: 1,012 `high`, 32 `medium`, 29 held `needs-ruling`; 55 of the 202
FJMS leaves used. Every row carries a confidence and a provenance.

**`discovery_data/work_author_aliases-v1.json`** ·
`sha256:acce47f67dcde456eb477fc092294ee42546963f5d977549f53e635da65f8a64`
— the 96 distinct `works.author` strings: 37 `exact`, 39 `containment`, 20 `unmatched`
(79.2% matched). Containment is labelled as containment; an unmatched author is retained,
never forced.

**`136-DOMAIN-CURATION.md`** — the report: coverage, the domain distribution, the full
needs-ruling list with candidate leaves, the applied posture, five data-quality findings,
threat-model coverage, and what 136-12 must do.

**`tests/test_work_domains.py`** — 41 tests. Pure except the five that read the gitignored
local artifacts.

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

**⚠ Commit collision, recorded rather than rewritten.** `136-DOMAIN-CURATION.md` was staged
by this plan and then swept into `0d448517` ("feat(136): batch the pinned novelty gate at
10 (owner ruling O)") by a **concurrent agent** committing in the same working tree at the
same moment. The file's content is byte-identical in `HEAD` and on disk
(`sha256:86bc2e77…`), so nothing is lost — but the commit message does not mention it and
attributes it to unrelated work. **History was deliberately NOT rewritten**: another agent
is actively committing here, and an amend or rebase would destroy its work. Recorded here
so the audit trail is reconstructable.

## What this plan deliberately did NOT do

- **It did not choose any of the 29 held assignments.** That is the owner's call, per
  ruling D.
- **It did not default held rows to `Unassigned`.** That default was explicitly declined.
- **It did not write `works.genre`** or touch the asset, the builder or the verifier — that
  is 136-12.
- **It did not correct the wrong-Bahya author, or any other data-quality finding**, in this
  artifact. An author correction belongs in the asset.
- **It did not touch the two files another agent had left modified** in the working tree
  (`scripts/discovery_novelty_funnel.py`, `shared/discovery_novelty.py`).

## Next

1. **Owner rules on the 23 decisions** in `136-DOMAIN-CURATION.md` § 4.
2. Rulings are recorded in `136-GATE1-DECISIONS.md`; each held row gains its leaf plus an
   `owner_ruling` citation; the artifact is re-emitted and re-pinned.
3. `--validate --release` must exit 0 before 136-12 may load `works.genre`.
