# The gen-2 bake is `discovery-v3` — a naming resolution

**Status:** proposed 2026-08-05 by the gen-2 bake session. **Needs owner acknowledgement**, because it
supersedes wording in an owner-ratified file (`discovery-coordination.md` §1, 2026-07-24). Nothing about a
built artifact changes; this renames an *unbuilt* one.

## The collision

Two different things were both called **discovery-v2.1**, three months apart, by two sessions that did not
cross-read each other's records:

| | what it is | when | state |
|---|---|---|---|
| **`discovery-frames-v2.1.md`** | Phase 136's **additive** rebuild over **unchanged v2 membership** — novelty axis, visibility axes, coverage + band rank, identification grain, curated `works.genre`, `kept_tie` repair | 2026-08-03 | **BUILT, DEPLOYED, SERVING** (`discovery-public-136rebuild`, content hash `e9365edc…`) |
| **the gen-2 evidence refresh** | a re-computed evidence base from a rebuilt engine — re-instrumented Track-1, two-pass shadow, coverage router, two surfaces | named 2026-07-24, still unbuilt | evidence artifact validated; **bake not started** |

**The damage this did to the records.** `.planning/ROADMAP.md` said the gen-2 refresh "becomes
discovery-v2.1 in its own later phase" (line 183) and that "PANEL-03's reference-side display additionally
waits on discovery-v2.1" (line 243). A reader who knows only `discovery-frames-v2.1.md` reads the second as
*blocked on an artifact that is already in production* — i.e. as no blocker at all. Both lines are corrected,
and line 55's "v2.1 candidate" with them.

## The resolution: `discovery-v3`

Four reasons, in order of weight:

1. **v2.1 names a deployed artifact.** Retroactively renaming something that is live, hash-pinned and cited
   by a gate (`tests/test_cert01_grading_validator.py` resolves the real artifact) is strictly worse than
   naming the thing that does not exist yet.
2. **The version step is semantically wrong, not just taken.** `discovery-frames-v2.1.md` says of itself:
   *"does NOT supersede `discovery-frames-v2.md` as a MEMBERSHIP frame — the membership is unchanged and the
   `frame_content_hash` is byte-identical."* The gen-2 bake is the opposite — a **membership replacement**:
   358,206 claims against 268,361, 4,160 distinct reference works against 1,269, a different engine, a
   different router, and a second (parallel/quotation) surface v2 does not have. A point release cannot mean
   both "changes no rows" and "changes every row".
3. **"v2.1" implies a wrong build order.** It reads as *additive on top of v2.1's new columns*, which would
   license migrating them. The opposite is required: novelty in particular is granularity-relative and must
   be **recomputed**, never carried across (see `project_novelty_is_granularity_relative`). A major-version
   name says that out loud.
4. **It leaves the ratified naming discipline intact.** `discovery-coordination.md` §2 reserves "v2" for the
   Phase-135 frame and requires gen-2 outputs be called "gen-2" until they land. That still holds: **"gen-2"
   stays the name of the evidence pipeline; `discovery-v3` is the name of the baked asset it produces.**

### What does NOT change

- **`schema_version` stays `discovery-v1`.** It is the loader's compatibility string, not a product version,
  and the fail-closed loader contract keys on it. It is not touched by this rename.
- Every existing artifact, hash, frame doc and manifest keeps its current identity. `discovery-frames-v2.1.md`
  keeps its name and its `v2.1` content — it is a correct record of what shipped.

### Records still carrying the old name (rename on next touch, not in a sweep)

Left deliberately un-swept: these are dated historical records, and rewriting them would falsify what each
session believed at the time. Each gets a pointer when it is next edited for another reason.

- `docs/specs/discovery-coordination.md` §1 — "It is the evidence refresh for **discovery-v2.1**". Owner-ratified;
  **needs the owner's acknowledgement to amend**, which is why this file is a proposal.
- `same_work_spike/probe/rsource/HANDOFF-TO-135.md` — titled "discovery-v2.1 evidence"; local-only repo.
- `docs/specs/discovery-forward-ledger.md` — "v2.1" in the *Suggested slot* column (items 1, 9).
- `docs/OPEN_ISSUES.md` — three entries say "the discovery-v2.1 refresh" (the coverage-routing demotion, the
  quoter-direction/compilation finding, and the liturgical-containment FP class).

## Masking

Tracked file. Restricted corpora appear here only as **M-source** / **R-source**, never by name.
