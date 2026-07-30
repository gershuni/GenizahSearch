# Discovery Milestone — Parallel-Session Coordination

**Status:** Owner-ratified 2026-07-24. This file is the authority both parallel
sessions cite for sequencing, naming, and handoffs. It supersedes any
conflicting framing in either session's working docs.

---

## 1. Decision of record (the fork adjudication)

**Phase 135 ships on the LIGHT bake** — `scripts/build_discovery_sidecar.py`
per the approved `docs/specs/discovery-v2-bake-plan.md` (owner-authorized
close, SHA-256 `a97d682c…`, see `135-BAKEPLAN-CODEX-REVIEW.md`), consuming
the EXISTING v1 evidence + the ratified census + the two date tables.

**The rsource gen-2 pipeline is NOT a Phase-135 dependency.** It is the
evidence refresh for **discovery-v2.1**: its direction-aware router,
register-matrix shadowing, and heavy Track-1 re-run land as a later
versioned rebuild through the same bake pipeline, fixing the documented
~6% high-coverage quotes-as-witness residual that v2 explicitly defers.

**One re-decision checkpoint:** immediately before the 135-07 real bake —
IF gen-2 has FULLY cleared its E2 post-run gate by then (cleared, not
"close"), the owner may reconsider, with the cascade costs on the table:
census re-verification against new evidence, works-list re-approval,
CERT-01 re-registration (population change), and a new frame version.
Absent that, no mid-phase evidence swap, ever.

**Rationale (recorded):** every production consumer (loader, DiscoveryService,
verifier, masking gate, golden fixture, band-labels module) targets the frozen
v1 schema; CERT-01 is pre-registered against the light bake's shipped
population; Phase 135 ships no public surface (surfaces are 136+, public flip
is 139), so the deferred residual is not user-visible during gen-2's maturation
window; and both tracks serialize on the same scarce resource — owner grading
time.

## 2. Session roles and naming discipline

| Session | Tree | Role |
|---|---|---|
| **135-SHIP** | main repo, `.planning/phases/135-*` | Executes 135-05..09 (light bake, CERT-01). Owns `discovery-sidecar-schema-v1.md` and all shipped vocabulary. |
| **GEN2** | `same_work_spike/probe/rsource/` | Builds the gen-2 evidence pipeline for discovery-v2.1. Owns the census emitter and date tables. |

- The name **"v2" is reserved** for the Phase-135 sidecar/frame. Gen-2 outputs
  are always called **"gen-2"** — never "v2", never "discovery-v2".
- Gen-2 vocabulary (e.g. its `contested` routing status, `gen2_*` schema)
  never enters the shipped sidecar except via a dated amendment to
  `discovery-sidecar-schema-v1.md`, which 135-SHIP owns.

## 3. Handoff interface (the only one)

Only **owner-ratified, hash-pinned artifacts** cross the boundary
(so far: the census, the two date tables; future: the slim census build
input, direction verdicts, gen-2 evidence snapshots). 135-SHIP never reads
live `rsource/` working files — it reads pinned snapshots whose SHA-256 it
records in `meta` + the frame doc.

**Delivered by GEN2 (2026-07-24):** the slim canonical-merges build input
`v2_canonical_merges.build.json` — **SHA-256
`cc054d111b9b4a76dd69912923ba50cd2b63f7820cb632617f645c12c207429a`**
(2382 bytes; 16 merges; top level EXACTLY `{dropped_by_135, merges}`; each merge
entry EXACTLY `{members_w, canonical_w, owner_verdict}`; `dropped_by_135=["w001239"]`;
pure-ASCII opaque `w#####` ids — no titles). A copy is staged at
`discovery_data/v2_canonical_merges.build.json`; regenerate deterministically from the
full census with `rsource/scripts/emit_canonical_merges_build.py`. 135-SHIP: point
`--canonical-merges` at this pinned file and record the SHA in `meta` + the frame doc.

## 4. Immediate tasks on resume

**GEN2, in order:**
1. Emit `v2_canonical_merges.build.json` — merge entries with EXACTLY the
   three frozen fields (`members_w`, `canonical_w`, `owner_verdict`) plus
   top-level `dropped_by_135` — and hand its SHA-256 to 135-SHIP. This closes
   the frozen-parser/real-file mismatch: the delivered census carries 9
   fields per merge entry (including masking-sensitive titles) and the
   approved plan's parser HALTS on extras. The slim file also keeps
   restricted titles out of the build input entirely.
2. Delete the 0-byte `same_work_spike/probe/data/fullcorpus_gen2.db` stub
   (name-collides with the real 4.49 GB DB in `rsource/data/`).
3. Strip the "Phase-135 unblock" framing from `14-SCOPING.md` §G and the E1
   briefs; re-label the target "gen-2 evidence refresh → discovery-v2.1".
4. Continue the E1-design review from its R2 REWORK (remaining blockers:
   deck estimand + survey inference). Round budget per §5.
5. Note: `same_work_spike/probe/` is now a LOCAL-ONLY git repo (initialized
   2026-07-24) — commit doc/script changes there; **never add a remote**.

**135-SHIP, in order:**
1. In 135-05/06, pin the SLIM census file's SHA-256 (`--canonical-merges`);
   do NOT point the build at `rsource/data/v2_canonical_merges.json`.
2. First test of the census parser in 135-06: **smoke-parse the real pinned
   file** — nine review rounds missed the field mismatch because no step
   ever touched the actual artifact.
3. Carry the dispositioned residuals into 135-06 as recorded in
   `135-BAKEPLAN-CODEX-REVIEW.md`: gate-13 becomes an `iff` contract;
   shipped-first display selection made unconditional.
4. At the 135-07 threshold, apply the §1 checkpoint, then bake.

## 5. Review-loop budget (both sessions)

Any Codex adversarial loop reaching **round 4 without APPROVE**: stop
iterating, compile a residual-disposition table, and request an
owner-authorized close per the `135-BAKEPLAN-CODEX-REVIEW.md` template.
No loop runs past round 5 without an explicit owner extension. (Lesson of
the 9-round bake-plan plateau.)

## 6. Preservation state (2026-07-24)

- Adjudication artifacts (verdicts, review CSVs, census, date tables,
  contested-study evidence) backed up to
  `Dropbox/GenizahSearch-backups/2026-07-24-discovery-adjudication/`.
- `same_work_spike/probe/` under local git (initial snapshot `5d763e4`).
- Main-repo `tmp/` is now gitignored (recurring masking-leak vector).

## Masking

This file is tracked: restricted corpora are referenced only as "M-source" /
"R-source"; works only by opaque `w000xxx` ids or public names.
