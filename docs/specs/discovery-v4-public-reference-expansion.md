# Discovery V4 public-reference expansion

## Scope

V4 adds public Sefaria and Hebrew Wikisource editions for private Discovery
work identities that already passed owner review. It does not infer new work
identity from title similarity: every acquired source is mapped explicitly to
one existing opaque private work id in `scripts/discovery_v4_sources.json`.

The 2026-08-14 audit selected 45 target identities from 43 source containers.
Forty-one containers were acquired, yielding 43 reference streams; two
too-short sources were quarantined. Generated text, audit snapshots, match
databases, and candidates stay in the ignored data depot. Only deterministic
transformations and the reviewed source map are tracked.

> **Depot location (2026-08-16).** That depot is `discovery_builds/discovery_v4/`
> — 623 files, 10.9 GB, gitignored. It was moved there from `_tmp/discovery_v4/`,
> because `_tmp/` is this repo's SCRATCH directory (`CLAUDE.md` and
> `scripts/init.ps1` both describe it as "working notes") and the depot includes
> the local twin of the artifact production serves, whose content hash matches its
> own `asset_basename`. The move is provenance-safe: the `reference_manifest.json`
> entries record absolute `_tmp/...` paths, but every consumer reads only
> `entries`, `schema_version` and the `*_sha256` fields and never dereferences
> those strings, so no manifest was rewritten and no pinned hash changed. Pass the
> new paths as the usual CLI arguments when re-running any stage.

## Frozen input chain

The V4 reference corpus is an append-only extension of `ref_corpus_v2.pkl`.
The complete V2 prefix must compare equal after unpickling. Matcher streams use
the established final-letter fold (`ךםןףץ` to `כמנפצ`); readable source text
keeps final letters and is checked against that folded stream before excerpts
can be baked.

The current input pins are:

- V2 reference corpus: `acb6b86f…6646de`
- V2 canonical masks: `14657b4e…7b5b81`
- V2 locus database: `aaac6f90…898263`
- V2 locus coverage report: `34f8cece…f4aaf`
- V4 acquisition manifest: `67eb11e1…7e30`
- V4 reference manifest: `9e3deab…944c`
- V4 reference corpus: `6c540e39…a3133`
- pristine copied research database: `75867026…e669`
- reviewed Track-1 pilot: `1d2b2695…0f6d`

Every consuming command verifies its relevant full SHA-256 before reading the
input. The reference verifier also proves that all readable public-source
texts reproduce their pickle streams exactly.

## Match and identity contract

The full 667,411-page corpus is matched. An old matched-page allowlist is not a
valid shortcut: a new reference may match a page absent from the V2 result.
The run is resumable by a pinned page-batch geometry and writes a staging table
until every batch is complete.

Promotion preserves the original 381,341-row table as
`track1_matches_v2_snapshot`, installs the reference-coordinate-aware result,
and reapplies the frozen `track1-shadow-v1` algorithm. A strict release
contract records and later recomputes the page, total/live, REF4, duplicate,
and missing-offset counts plus the matcher fingerprint and input hashes.

Only REF4 references with a live unshadowed match receive a new opaque id. The
reconciliation step copies the owner-approved author and curated domain from
the mapped private identity, then creates an approved two-member canonical
group whose public id is the representative. The existing 16 reviewed merge
groups and the D-14 decision remain required; V4 cannot be enabled by editing
the old expected merge count.

## Routing, locus, and excerpts

Existing Track-1 rows retain the fitted gen-2 decisions, including the
previous split-grain re-graining. REF4 rows did not exist in the fitted router,
so they are classified separately with its frozen coverage estimand and
threshold and are recorded as an extrapolated V4 population.

The locus database is extended in the same REF4 coordinate space. Missing
division tables remain honest whole-work fallbacks. The final public bake
recomputes claim and identification loci after projection, then loads excerpts
directly from the hash-verified public source text. A REF4 reference corpus
without the matching source manifest and normalized source directory is a
hard excerpt-bake error.

## Release posture

The 2026-08-14 owner ruling in
`discovery-public-projection-exclusions-v1.json` supersedes the earlier public
retention posture for both Yalkut Shimoni identities. They remain present in
the private research bake as routing competitors, but the public projection
drops their claims and evidence before recomputing works, identifications,
loci, excerpts, counts, and dependent graph closure.

V4 is first built as a loader-ready ignored candidate. Private verification,
public projection, public verification, masking scans, SQLite integrity and
foreign-key checks, excerpt replay, source attribution, and real-loader proof
must all pass. Its new frame hash is recorded as a candidate fact; it is not
silently compared with the pre-V4 frame as though the expansion were a
membership-preserving rebuild.

No upload, manifest swap, or production restart is part of the build. Those
remain a separate owner-approved deployment step after reviewing the candidate
counts and representative UI results.
