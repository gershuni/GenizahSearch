# Discovery V4.1 public-source expansion and R-shadow plan

## Purpose and release boundary

V4.1 is a candidate-only follow-up to merged Discovery V4 (PR #320). It adds
ten explicitly mapped, publicly licensed reference streams and evaluates the
private R-source only as an offline same-work competitor. It does not publish
R-source text, ids, titles, matches, excerpts, hashes, or source metadata.

The work ends with a hash-verified, loader-ready candidate and an owner review
packet. Uploading, manifest switching, or restarting production requires a
separate approval.

## Approved acquisition set

Add the following records to a versioned V4.1 source map. Each mapping is an
existing opaque work identity selected by review; title similarity is never an
identity-approval mechanism.

| Provider | Public source | Acquisition/locus rule | Target work |
| --- | --- | --- | --- |
| Sefaria | Midrash Tanchuma | `schema_leaves`, section | `w000926` |
| Sefaria | Duties of the Heart | `schema_leaves`, section | `w000195` |
| Sefaria | Sheiltot d'Rav Achai Gaon | default chapter rule | `w000732` |
| Sefaria | Avot DeRabbi Natan | default chapter rule | `w000788` |
| Sefaria | Avot DeRabbi Natan, Recension B | default chapter rule | `w000789` |
| Sefaria | Machberet Menachem | `schema_leaves`, section | `w000911` |
| Hebrew Wikisource | במדבר רבה | prefix `במדבר רבה `; chapters 1–14 | `w000496` |
| Hebrew Wikisource | מדרש פנים אחרים | default chapter rule | `w000986` |
| Hebrew Wikisource | אלפא ביתא דרבי עקיבא נוסח א | default chapter rule | `w001127` |
| Hebrew Wikisource | אלפא ביתא דרבי עקיבא נוסח ב | default chapter rule | `w000892` |

The fetcher must retain V4's licence allowlist (`Public Domain`, `CC0`,
`CC-BY`, `CC-BY-SA`), fail closed on an absent licence, enforce the 1,000
Hebrew-letter minimum, and persist raw source text only in the ignored data
depot. Record a dated acquisition manifest with the provider response,
normalised-source hash, text length, selector, and licence for every accepted
stream.

## Explicit exclusions and reconciliation queue

- Do not add Hebrew Wikisource's standard `מדרש תנחומא`: Sefaria's selected
  edition above is the reference stream.
- Do not add Seder Eliyahu Rabbah or Zuta: existing licensed Sefaria streams
  already cover them. Put the two private/public same-work pairs into the
  owner-review reconciliation queue; add no new reference unless that review
  rejects the existing edition.
- Quarantine Bamidbar Rabbah from Sefaria and Rabbeinu Chananel on Shabbat
  until their licence metadata is unambiguously allowlisted. Do not use the
  existing unknown-licence stream as an exception.
- Reject Midrash Tadshe, Seder Eliyahu Rabbah from Wikisource, and the standard
  Wikisource Tanchuma probe for being below the size threshold.
- Reject `עשרת הדיברות`: its probe has insufficient target coverage and must
  not be turned into an identity mapping.

## Build sequence

1. Copy the V4 source-map schema into a V4.1 manifest and add only the ten
   approved records. Introduce a `REF5:` namespace rather than altering any
   `REF4:` raw id or the byte-stable V2/V4 prefixes. Generalise V4-only
   validators where necessary so the namespace is an explicit input.
2. Fetch all ten sources, create immutable source/reference manifests, and
   verify every readable text reproduces its folded reference stream. A failed
   source is removed from the candidate and reported; it is never silently
   substituted.
3. Extend the reference corpus append-only. Prove equality of the V2 and V4
   prefixes after unpickling, uniqueness of raw ids, and exactly ten new REF5
   works.
4. Extend locus coverage in REF5 coordinates, retaining honest whole-work
   fallbacks where a provider lacks division metadata. Extend canonical masks
   only from reviewed rules; no mask is inferred from a title or a private
   source.
5. Run the full 667,411-page matcher with the pinned batch geometry, promote
   only after all batches complete, reapply the frozen shadow algorithm, and
   record total/live/REF4/REF5, duplicate, missing-offset, and fingerprint
   facts in the release contract.
6. Create the ordinary V4.1 candidate, rerun routing for the new REF5
   population, project public data, recompute dependent closure and loci, and
   run masking, SQLite integrity, foreign-key, source-attribution, excerpt
   replay, and real-loader checks.

## Ratified amendment (owner, 2026-08-16): combined bake with V4.2

The acceptance gate "exactly ten new REF5 works" now means **exactly ten REF5
reference inputs, evaluated inside the final combined competitor set** of the
shared V4.1+V4.2 bake (`docs/specs/discovery-v4.2-combined-bake-and-public-first-plan.md`).
A REF6 competitor may shadow a REF5 row. The separately-shadowed base+REF5
candidate this plan originally described is superseded and will not be built;
steps 5–6 below execute once, inside the combined bake, under the V4.2 plan's
conditions. Steps 2–4 (fetch, append, locus/mask extension) are unchanged and
remain REF5-scoped.

## Build-sequence status

**Step 1 is done** (`scripts/discovery_v4_1_sources.json`, namespace threading,
`tests/test_discovery_v4_1_sources.py`). **Step 2 (the ten-source fetch)
completed 2026-08-16** into `discovery_builds/discovery_v4_1/sources/`.
**Step 3 (the REF5 append) executed and verified 2026-08-16** — exactly 10 REF5
references (6 sefaria + 4 hewikisource, 2,658,587 normalized letters, 7 with
locus units + 3 whole-work fallbacks), chained byte-stably onto the pinned V4
corpus; outputs and hashes in
`discovery_builds/discovery_v4_1/REF5-APPEND-RUNBOOK.md` (corpus v5
`e6041360…27eae`, masks v5 `9333666c…bd17c`). Step 4 (REF6) awaits the owner
sitting; 5–6 are governed by the combined-bake amendment above.

The ten records were copied from the reviewed probe maps in
`discovery_builds/discovery_v4/{source,wikisource}_probe_map.json` rather than
re-derived from the table above, and each one's probe entry is `acquired` with an
allowlisted licence. The map also records the exclusions as data — quarantined,
reconciliation-queued, and rejected — so a later reader can see what was decided
against, not merely what is absent.

`REF5` is declared by the source map itself and threaded through
`discovery_v4_common`, `_build_reference`, `_extend_masks`, and
`_verify_reference`. A `--reference-namespace` flag that disagrees with the map
is refused rather than applied, so a V4 map cannot be run through a REF5 build.
Two fields stay conditional on the namespace being non-default —
`reference_namespace` in the emitted manifests, and the `v4_extension` coverage
key — because a REF4 rebuild must still reproduce the pinned V4 artifacts byte
for byte. Verified, not assumed: regenerating the V4 ids from the committed
source map reproduces the 43 `raw_reference_id` values in the built V4 reference
manifest exactly, in order.

**The V4 source-map hash pin was broken on this checkout and is now fixed.**
`scripts/discovery_v4_sources.json` is pinned by SHA-256 in the V4 acquisition
and reference manifests (`6f21efcd…`), but the repo had no `.gitattributes` and
the owner's git runs `core.autocrlf=true`, so the working tree held a CRLF copy
hashing `ba3f413a…` — `discovery_v4_build_reference.py` would have refused its
own inputs with "acquisition manifest source-map hash mismatch". A narrow
`.gitattributes` now holds both source maps at LF, and a test pins the V4 hash.

Two items to settle before or during the later steps:

- **`ref4_total_rows` / `ref4_live_rows` are frozen release-contract keys**
  (`_TRACK1_V4_CONTRACT_KEYS` in `scripts/build_discovery_sidecar.py`), so
  `discovery_v4_match.py` and `discovery_v4_reconcile.py` still carry `REF4:`
  literals. A V4.1 run needs REF4 *and* REF5 counts, which means widening that
  frozen key set — a coordinated change belonging to steps 5–6, not to the
  acquisition side.
- **`REF5:bamidbar_rabbah` has no `locus_title_he`**, matching its probe record,
  so its chapter labels will fall back to the private work's `neutral_title`.
  Worth an owner glance at step 4 if that title is not the expected
  `במדבר רבה`.

## Restricted R-source shadow

The R-source stays read-only in its restricted depot and is never an input to
the public reference corpus or candidate database. Run a second, offline
analysis that unions its Track-1 competitors only after canonical same-work
groups have been applied:

1. Same canonical work: retain the public match; do not call it a competing
   shadow.
2. Different canonical work: calculate whether the restricted competitor wins
   under the existing shadow ordering, but emit only aggregate counts and
   opaque public-side page/work buckets.
3. Produce a review-only delta with the affected public candidate rows and a
   `restricted_competitor` reason. It must contain no restricted identifiers,
   titles, excerpts, or text-derived hashes.
4. Keep the ordinary and restricted-shadow results separate. The latter is an
   owner decision aid, never an automatic public suppression or deployment
   rule.

The initial V4 diagnostic is the baseline for review, not a release invariant:
4,978 live REF4 rows became 4,037 under the raw restricted union, affecting
903 pages and removing 941 live rows. V4.1 must report its corresponding
numbers, stratify them by source and canonical status, and include a manual
sample of competitive and noncompetitive cases before any owner decision.

## Acceptance gates and handoff

- The source manifest has ten accepted records, each with allowlisted licence,
  explicit mapping, and reproducible normalised text hash.
- V2 and V4 prefixes are unchanged; REF5 adds exactly ten unique references;
  no candidate has duplicate raw ids or missing reference offsets.
- The full matcher completes all pages and the release contract, source
  verification, public projection, loader, and masking checks pass.
- The R-shadow output passes a no-restricted-data scan and is stored only in
  the restricted review location.
- The handoff packet contains candidate counts, affected-page aggregates,
  source attribution, known exclusions, representative UI checks, and the two
  separate owner decisions: public V4.1 promotion and any R-shadow policy.

No merge, production data change, or deployment follows from this plan alone.
