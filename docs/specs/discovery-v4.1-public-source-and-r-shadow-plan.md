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
