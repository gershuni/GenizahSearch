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
> own `asset_basename`. The move is provenance-safe for the pinned hashes: no
> manifest was rewritten and no pinned hash changed. **But the original claim that
> "every consumer never dereferences the recorded path strings" was wrong**
> (found 2026-08-16, Codex design review, line-verified):
> `scripts/bake_discovery_excerpts.py::load_v4_public_sources` reads
> `manifest["acquisition_manifest"]` as an absolute path, which now points at the
> deleted `_tmp/` location — the next excerpt bake fails loudly at startup until
> the acquisition manifest becomes an explicit CLI input. Tracked in
> `docs/OPEN_ISSUES.md` (P2, 2026-08-16). Every other stage takes its paths as
> CLI arguments; pass the new depot paths when re-running.

## Deployed artifact

Production has served the V4 public projection since **2026-08-15**:

| | |
|---|---|
| `asset_basename` | `discovery-v1-528f6d365ac642a7a6cbbfcf16d4d3fed7bc170d310e052fbbd81d70ae8dadaf` |
| size | 605,577,216 bytes |
| `meta.build_date` / `data_as_of` | `2026-08-14T14:43:05Z` / `2026-08-14` |
| `frame_content_hash` | `90a51e0b7ed1e1c107ee331dd92c011eed1838e956444121378fcf3c11a236f9` |
| identifications | 55,250 |
| works / manuscripts they reach | 596 / 39,341 — the identification grain, and the figures README.md publishes |
| `works` / `manuscript_display` table rows | 653 / 40,363 — the containing tables, which also hold rows no public identification points at |
| with a visible matched passage | 48,270 identifications (36,990 distinct manuscripts) |
| depot path | `discovery_builds/discovery_v4/guidefix/preview/` |

Verified 2026-08-16 rather than assumed: the streamed SHA-256 matches both the
filename and the manifest, `meta.audience` is `public`, the manifest and `meta`
frame hashes agree, `PRAGMA integrity_check` returns `ok`, and
`web/discovery_assets.py::load_discovery_state()` — the shipped fail-closed
loader, with its full validation matrix — reaches `ready=True` on it.

Two builds share these counts. `528f6d36…` supersedes `ef3a79fd…` (2026-08-14);
they are byte-different but count-identical, because the change between them was
the Judeo-Arabic range-picker label (`פרק יז` rather than `פרק יז, עמ' 219`) and
not the population. Both share `frame_content_hash 90a51e0b…`, so a figure read
from either is correct for the other. `528f6d36…` is the live one.

### Rollback target

**Not `e9365edc…`.** The beta-launch artifact (2026-08-03, 555 works / 53,581
identifications) is retained in `discovery_data/`, but the current code
*refuses* it: `web/discovery_assets.py::_REQUIRED_COLUMNS` requires
`discovery_identification.rendered_relation`, added by the 2026-08-12 C-track
batch, and that artifact predates it. Deploying it would leave the loader at
`ready=False` and clean-hide every discovery surface — measured, not inferred:
staged alone in a directory it logs `table 'discovery_identification' missing
required column(s): ['rendered_relation']`.

The rollback target is **`ef3a79fd07f375ccf1b6fcec4ad87576058bdc4d536155c44725952a630a071d`**
(2026-08-14, `discovery_builds/discovery_v4/preview/`), which carries the column
and the same 596 works / 55,250 identifications; it differs from the live build
only in the Judeo-Arabic range-picker label. It is not currently staged in
`discovery_data/live/` — that directory deliberately holds one artifact.

> **Unresolved.** Production served *something* between the C-track deploy and
> V4 that satisfied the `rendered_relation` requirement, and no copy of it is on
> this box. Either the batch's code and artifact shipped together or an
> intermediate rebake was promoted and never captured here. Worth settling
> before anyone plans a rollback to that window rather than to `ef3a79fd…`.

The three places that name the live artifact — `discovery_data/live/manifest.json`,
`discovery_data/manifest.deploy.json`, and `PRODUCTION_DATABASE_SCANS` in
`tests/render_smoke/test_discovery_masking_sweep.py` — must move together; see
`_tmp/BETA-LAUNCH-RUNBOOK.md` §2. The repo's own `discovery_data/manifest.json`
is deliberately NOT one of them: it resolves the frozen CERT-01 artifact for
`tests/test_cert01_grading_validator.py`.

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
