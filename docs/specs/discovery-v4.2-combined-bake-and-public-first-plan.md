# Discovery V4.2 — combined bake, container sources, and public-first identities

## Purpose and release boundary

V4.2 extends the reference corpus under a new `REF6` namespace and shares ONE
full-corpus bake with V4.1: one 667,411-page matcher run, one staging table,
one promotion, one candidate, one owner review packet. It exists because the
matcher run and the downstream candidate cycle are the dominant cost, and two
sequential bakes would pay them twice.

The work ends, exactly as V4.1's does, with a hash-verified loader-ready
candidate and an owner review packet. Uploading, manifest switching, or
restarting production requires a separate approval. Nothing in this plan
changes the release boundary.

This plan encodes the conditions of the 2026-08-16 Codex design review
(`COMBINE-WITH-CONDITIONS`; full text in the build depot at
`discovery_builds/discovery_v4/probe_v42/V42-CODEX-REVIEW.md`).

## Ratified amendment to V4.1 (owner, 2026-08-16)

The V4.1 acceptance gate "exactly ten new REF5 works" now means: **exactly ten
REF5 reference inputs, evaluated inside the final combined competitor set.**
A REF6 competitor may shadow a REF5 row; the separately-shadowed base+REF5
candidate the original V4.1 plan described is superseded and will not be
built. The V4.1 spec carries the same amendment note.

## Cohorts

`REF6` is a bake generation, not an identity mode. Every REF6 source-map
entry carries an explicit `identity_mode`:

1. **`private_sibling`** — maps to an existing private work identity, exactly
   like V4/V4.1 (`target_private_work_id`). Members:
   - The 15 Mishneh Torah book-grain works (w000174–w000188) as multi-text
     containers (below).
   - Six pipeline-ready additions from the 2026-08-16 probe round:
     w000463 (רבנו חננאל על מסכת בבא קמא), w000736/w000734/w000735 (three
     הלכות גדולות introduction loci mapped into one Sefaria work via a
     reviewed section-range rule), w000524 (ספרי זוטא במדבר),
     w001079 (תולדות בן סירא נוסח א, Wikisource).
   - NOT a member: w000775 (קניין תורה) — an owner locus decision on the
     existing Avot reference, not an acquisition. NOT a member: w000471
     (license fail-closed).
2. **`public_first`** — no private counterpart exists; the identity is minted
   from the public work itself under prior owner approval (below). The
   candidate list is owner-graded from a review packet in the restricted
   review location; it is not enumerated in tracked files.

## Conditions (all binding; from the Codex review)

### C1. Matcher run identity

Before the expensive run, the resume/stage state must be bound to ALL inputs:
a run ID derived from the final reference hash, masks hash, source-DB seed,
runner and calibration hashes, page frame, and page-batch geometry — stored
with the staging table and re-verified on `run`, `status`, and `promote`.
Today `pin_batch_geometry()` pins only `(tag, generation, page_batch)` and
`inspect_stage()` never compares the stored fingerprint against an expected
one, so a stale completed table could be accepted. A batch ledger proves every
batch, not only the last. Shadow ordering (append order, batch insertion
order, tie behavior) is pinned as part of the algorithm contract.

### C2. Manifest chain

The reference build chains append-only: base → +REF5 → +REF6, each stage
hash-pinned. Consumers (reconcile, excerpt bake, verifiers) take the complete
ORDERED manifest bundle (REF4→REF5→REF6), never only the final manifest. The
excerpt bake's acquisition manifests become explicit hash-pinned CLI inputs
(its recorded-absolute-path dereference is an open P2 defect). `WorkSources`
recognizes `REF5:`/`REF6:` ids.

### C3. Release contract v2

Per-namespace counts (`ref4_*`, `ref5_*`, `ref6_*`, and REF6 split by
identity_mode) enter a NEW contract schema version consumed by the sidecar
builder. The frozen `discovery-v4-track1-release-contract-v1` key set is not
mutated. The contract also records page_batch, expected batch count, pilot and
calibration hashes, and the bound run ID (C1).

### C4. Routing cohort registry

The sidecar builder's regrain/routing must consume a reviewed namespace/cohort
registry with at least three cohorts — REF5 private-target, REF6
private-target, REF6 public-first — each excluded from the legacy fullscan
path and routed through the frozen-threshold extrapolation, reported
separately. (Was an open P2 defect — only `REF4:` rows were excluded —
**fixed 2026-08-16**: `finalize_build` classifies every tier-A row through
`scripts/discovery_routing_cohorts.json`, routes each extrapolated namespace
separately, and hard-errors on unregistered `REF*` prefixes.)

**Status: C1, C2 (excerpt-bake side), C3 (emitter + consumer), and C4 were
implemented and committed 2026-08-16** with mutation-proven gates. Still open
from the conditions: C2's reconcile side (lands with C5), C5–C9, C10, C11's
handoff-artifact sweep wiring, and the remaining C12 gates tied to those.

### C5. Public-first identity artifact

A hash-pinned, pre-match owner-approval artifact keyed by a stable
`identity_key` (never the provider title, never a not-yet-minted opaque id)
carries the owner-approved Hebrew title, author, genre, domain assignment,
provider/source, and verdict. Reference metadata for public_first entries is
built exclusively from this artifact; the provider title is evidence shown at
approval time, not a mutable identifier. After matching, only live unshadowed
public_first references mint opaque ids; they are STANDALONE canonical works
(no singleton or synthetic two-member merge groups), validated as absent from
every merge group. Domains come from the artifact — no inheritance from a
nonexistent private identity. Approved-but-unmatched entries are reported and
mint nothing.

### C6. Provenance vocabulary

`source_label`/source-corpus vocabulary gains a reviewed open-public-reference
code (or a formally renamed existing one) so Wikisource-derived identities are
not recorded as "sefaria". With public_first, that label is identity-bearing.
The locus family hard-code ("sefaria" for every appended locus work) and the
`supplemental_structures` overwrite in `_extend_locus` are fixed in the same
change.

### C7. Container sources and the Mishneh Torah license ruling

The source-map schema gains a multi-text container record: a FROZEN ordered
child list (child key, provider ref, version title/source, provider-reported
license, license URL, attribution, raw/normalized hashes, offset interval in
the combined stream). Live ToC discovery may verify membership and order but
never silently redefine them. Locus grain "section", one locus per child.

**Owner ruling (2026-08-16): the Mishneh Torah is treated as Public Domain in
its entirety.** The acquisition manifest still records each child's
provider-reported license as fact (81 of 88 children report Public Domain,
7 report CC-BY-SA on the Wikisource presentation layer); a dated
`license_ruling` record in the source map sets the EFFECTIVE license of the
container to Public Domain, with the provider-reported values preserved
alongside. Attribution for excerpts is a single Public Domain notice. This
ruling is specific to the Mishneh Torah; any other mixed-license container
needs its own ruling or the full per-child attribution machinery.

### C8. Per-daf sources (Zohar-class)

Wikisource works paginated per daf/amud take their locus identity from PARSED
page names, never ordinal inference (`daf_bavli`'s Sefaria ordinal geometry
does not transfer). Missing pages fail the completeness gate; the fetcher's
`coverage_status="partial"` escape is not available to public_first sources.

### C9. Mask review gate for large Aramaic streams

The canonical-mask extension over REF6 reports per-work masked fraction,
largest masked intervals, and a manual sample before the matcher run.
Formulaic Aramaic (Zohar-class) may over-mask in ways the ten-work REF5 set
never exercised.

### C10. Measure before the run

Before the 667,411-page run: reference-index memory, staging DB growth
projection, candidate size, excerpt-bake peak memory, and masking runtime,
recorded in the depot. The match-row population risk (the restricted review
location's aggregate live-page counts) dominates raw source size.

### C11. Masking over handoff artifacts

The no-restricted-data scan runs over the ignored handoff artifacts (maps,
approvals, logs, reports) as well as the final SQLite projection. Tracked
REF6 files carry only independently acquired public facts — no restricted
ids, paths, hashes, rankings, or text-derived values.

### C12. CI mutation gates

New tests must be proven able to fail (mutate the defect back) for at least:
stale stage reuse, wrong reference/mask hash under the same tag, missing
batches, namespace-count drift, unknown REF namespaces, manifest-chain
discontinuity, public-first unmatched behavior, standalone canonical works,
container child-list drift, REF5/REF6 exclusion from the legacy routing path,
and multi-manifest excerpt replay. The existing exactly-ten REF5 test stays
unchanged; REF6 gets its own tests.

## Build sequence

1. **Parallel, now:** V4.1 step-2 fetch of the ten (running); fetcher
   container extension (C7) with the Mishneh Torah ruling encoded; the
   public-first identity packet for owner grading (C5, restricted location);
   the two P2 defect fixes (C2 path input, C4 registry) with C12 gates.
2. Owner grades the identity packet; section-range rule for the three
   Halakhot Gedolot loci reviewed at the same sitting.
3. REF6 acquisition (containers + six additions + approved public_first
   sources), immutable manifests, folded-stream verification per V4.1 rules.
4. Reference chain base → +REF5 → +REF6 with prefix-equality proofs at each
   stage; mask extension with the C9 gate; C10 measurements.
5. One matcher run under C1 identity binding; promotion only after all
   batches; frozen shadow over the combined competitor set (per the ratified
   amendment); release contract v2 (C3).
6. Reconcile (private_sibling per V4 rules; public_first per C5), routing per
   C4, locus and excerpts over the manifest chain (C2), public projection,
   masking (C11), real-loader proof, review packet. Separate owner decisions:
   promotion, and R-shadow policy (unchanged from the V4.1 plan, which still
   governs the R-shadow work).

No merge, production data change, or deployment follows from this plan alone.
