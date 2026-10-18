# Shelfmark resolver: keep the library a prefix names

Status: rev. 8 (2026-10-18): Codex rounds 1 (plan, 9 findings), 2 (plan + code, 5), 3 (4), 4 (3),
5 (3) and 6 (2), each DO NOT SHIP, folded in -- see the revision log. Per the owner's review budget
(the count fell 9-5-4-3-3-2, the last three rounds synthetic only), round 7 is for information:
what it finds is verified and logged in `docs/OPEN_ISSUES.md`. Implemented on
`feat/shelfmark-prefix-scope`, on top of PR #400's head `edb3ea61` (`feat/catalogue-shelfmark-forms`),
not committed. Builds on `docs/plans/CATALOGUE_SHELFMARK_FORMS_PLAN.md`, whose helpers it reuses.

## 1. Problem (open issue, P3, logged 2026-10-18)

`MetadataManager.resolve_system_by_shelfmark` ("T1") strips a leading library name or code with
`shared/browse_map_utils.py::_strip_library_prefix`, then exact-matches the rest **in every
library**. The library the user named is forgotten:

- `Halper 77` -> `77` -> 3 records numbered 77 (Senckenberg, HAS, Sofer). The record that stores
  `Halper 77` (`990001664240205171`, code **Katz**) is not offered. Of `Halper 1`-`499`, 484 go
  to other libraries' records this way.
- `The University of Manchester Library, Ms. P 45` -> `Ms. P 45` -> 4 records: the Manchester
  record `990053746560205171` and three Mosseri records.
- `GASTER 1775.5` -> JTS `990053098890205171` (`ENA 1775.5`, another collection), not the BL
  record that stores `GASTER 1775.5`.
- `The Bodleian Libraries, University of Oxford Arch. O.d. 8.1` (stored on `990053402960205171`)
  reaches only the stub `990053385670205171` (`arch. O.d.8/1`).

The catalogue-form fallback ("T2") keeps a named library, but runs only when T1 found no exact
match, so it never sees these inputs.

## 2. What exists (re-grep before editing)

- T1: strip prefix -> `normalize_shelfmark` -> one scan of `_get_shelfmark_index()` (csv variants
  and NLI-cache `shelfmark`/`shelfmark_alt`) collecting exact (key; dotless key for a dotless
  query) and substring matches -> a `lookup_cudl` bridge hit promoted to exact only if its record
  is not matched already -> dedup by `sys_id` keeping the shortest shelfmark -> one record / exact
  options / T2 / substring options.
- `shared/catalog_shelfmark.py` restates T1's exact stage (`_t1_exact_keys`, `t1_exact_entries`,
  `resolve_many`) for `resolve_catalog_form(s)` (exclusion lists, the Search-mode fallback).
- T2's institution for an alias-prefixed input: the removed prefix looked up in `alias_codes()`
  (exactly a `LIBRARY_CODES` code or name; `BnF`/`BNF` one class); a head-split input
  (`<head>, Ms. <rest>`) uses the head and records that store it.
- Library codes on the data often do not follow the alias: code `Halper` 0 records (the 17
  `Halper ...` texts are `Katz`), `Gaster` 0 (15,347 texts on BL/Manchester), `Adler` 2 (28,660
  texts on JTS), `Westminster` 163 (1,655 `Westminster College ...` texts on CUL), `Rylands` 0.

## 3. Rule

**R0. Scope.** The rule applies only to an input that (i) has a prefix naming a library (R1) and
(ii) T1 answers exactly today (key, dotless key or bridge). An input T1 does not answer exactly
goes to T2 as before. So exactness never changes: an input exact today stays exact (R3), and no
input becomes exact that was not.

**R1. Named codes.** `catalog_shelfmark.prefix_codes(q, sq)` = `alias_codes()[removed prefix]`,
the definition T2 already used, now shared; empty when the alias names no library (`cambridge`,
`bodleian`, `british library`, ...) or nothing is left to look up (`CUL`, `CUL .`).

**R2. The answer** is today's exact answer with the records of *other* libraries removed, plus
the records that store the whole input:
- (a) today's exact candidates (key, dotless key) that lie in the named library, and today's
  bridge entry if it does (added only when no kept candidate has its record, as today);
- (b) records that store the **whole input**: key equal to `normalize_shelfmark(q)` and the same
  integers as `q` (`catalog_shelfmark.numbers`), any library; no dotless rule, no bridge.

"In the named library" (`catalog_shelfmark.in_libraries`): the record's code is a named code;
**or** it stores `<head> Ms. <rest>` (the `Ms.` token in any case) whose head names a named code
(`head_codes`) and whose `<rest>` is this shelfmark -- same zero-insensitive key (`zkey`), same
integers -- collection provenance, per shelfmark, read from its call numbers and the NLI-cache
`shelfmark`/`shelfmark_alt` strings the index was built from (one frozen copy builds both the
index's cache entries and its `cached_variants`, carried by the index object itself, so a later
eviction, a bad cached value or a concurrent rebuild changes nothing);
**or** its library is unknown (a record known only from the NLI cache: unknown is not outside).

**R3. Fallback.** Today's answer, unchanged, when (a) and (b) are empty **or when nothing in them
genuinely carries the numbers** (no (b) record, no in-library bridge hit, and no (a) candidate
whose matched variant has the stripped text's integers): scoping must not leave a dotless-rule
conflation as the only answer.

**R4. One rule everywhere.** `resolve_system_by_shelfmark` and the restated T1 call the same
`named_library_exact`. T2's alias institution uses `in_libraries` (provenance on zero-insensitive
keys); T2 gains the whole-input key `w` = `zkey(q)` (any library, numeric check), so `Halper 077`
and `GASTER 01201.017` reach what `Halper 77` and `GASTER 1201.17` reach.

Not changed: `normalize_shelfmark` (SHA-pinned), `_strip_library_prefix` and its aliases,
`lookup_cudl`, substring matching, `search_by_meta`, inputs whose prefix names no library or that
have no prefix, and T2 for head-split inputs.

### Why each part (each was a measured wrong or lost record)

- **R0** (Codex round 1, HIGH 1). Promoting (b) to exact for inputs T2 answers today dropped
  same-library T2 candidates: `Mosseri, Jacques, Ms. Ch 2` offers two Mosseri records today and
  would have offered one (2,712 cases, 824 distinct records, measured by Codex on rev. 1).
- **Remove other libraries only.** Rev. 1 also number-checked (a) and so dropped in-library
  records that today's answer lists beside the right one (`JTS Ms. ENA NS 76.81` lost the JTS
  record stored `ENA NS 76.8.1`; 506-844 inputs per population in the rev. 2 measurement). The
  dotless rule is T1's own; this rule does not fix it, it only stops it from becoming the sole
  answer (R3).
- **R3's numbers condition.** Without it, `Westminster College Ms. L-G Glass 12` -- today the CUL
  record and the Westminster record stored `L-G Glass.1.2` -- became `L-G Glass.1.2` (Glass 1.2,
  a dotless-rule match) alone.
- **Provenance per shelfmark** (round 1, HIGH 4). Library codes miss collections (`Gaster`,
  `Halper`, `Adler` hold 0-2 records), so a record counts for a collection when it stores that
  collection's form *of this shelfmark*: the full BL record of `GASTER 1201.17` stores `Gaster,
  Moses Collection Ms. 1201.17`, so both it and the BL stub that stores `GASTER 1201.17` are
  offered (without provenance the stub alone replaced the full record: 2,075 inputs in rev. 1);
  the Schocken record that stores `Sofer, David Ms. 111.12` stays for `Sofer Ms. 111.12`. Per
  shelfmark, not per record: a record-wide reading made `adler MS 1512` the JTS record whose
  Adler number is 1873 (it also stores JTS `Ms. 1512`), and naming a (b) record's whole library
  made `Halper 275` add Katz `Ms. 275` (Penn Ms. 275, another manuscript).
- **Unknown library kept** (round 1, HIGH 3): a cache-only record stored `X 7` stays in `CUL X 7`.
- **Union of (a) and (b).** "(a) first" lost sources, "(b) first" lost 1,086 (rev. 1).
- **No dotless rule in (b).** With it, `Adler, Elkan Nathan Ms. 2162` also reached `Ms. 216.2`.
- **Bridge entry only if its record is not kept already** (round 1, LOW 8): otherwise 418
  baseline inputs changed only the shelfmark shown.
- **Empty remainder** (round 1, MEDIUM 9): `CUL .` finds nothing on both paths.

## 4. Measured on the real corpus (read-only, 2026-10-18)

Old = the pristine PR #400 head `edb3ea61`; new = this branch. Full responses from
`resolve_catalog_forms` -- tier, total, `sys_id`, shown shelfmark, and every option as (id, shown
shelfmark) in order -- for 3,067,059 inputs (2,022,705 distinct), per input and its source record.
Populations: P1 every stored variant that loses a prefix (261,574); P2 `<code> <v>` and
`<name>, <v>` for every short variant of every record whose code is in `LIBRARY_CODES` (453,455
each); P3 `<name>, Ms. <v>` (453,455); P4 every stored long form as NLI writes it, `<head>, Ms.
<rest>` (247,960); P5 every collection alias that stored texts use for records of another code,
typed before each short variant of that code's records (11 pairs, uncapped, 652,216); P6 each
real NLI head typed before each short variant of records of its library, including records that
store no long form (544,944).

| Population | Inputs | Unchanged | Narrowed to the source alone | Narrowed, source kept | Source added, today's answer kept | Other-library answer replaced by the source | T2 now answers (none before), source in it | One record -> two | **Source lost / in-library record dropped / single other record** |
|---|---|---|---|---|---|---|---|---|---|
| P1 | 261,574 | 250,093 | 757 | 251 | 2,177 | 41 | 8,255 | (2,075 of the 2,177) | **0 / 0 / 0** |
| P2 code | 453,455 | 449,848 | 1,942 | 1,665 | 0 | 0 | 0 | 0 | **0 / 0 / 0** |
| P2 name | 453,455 | 449,749 | 1,942 | 1,665 | 0 | 0 | 0 | 99 | **0 / 0 / 0** |
| P3 | 453,455 | 449,848 | 1,942 | 1,665 | 0 | 0 | 0 | 0 | **0 / 0 / 0** |
| P4 | 247,960 | 246,988 | 710 | 251 | 0 | 0 | 11 | 0 | **0 / 0 / 0** |
| P5 | 652,216 | 651,611 | 335 | 262 | 0 | 0 | 0 | 0 | **8 (see below) / 0 / 0** |
| P6 | 544,944 | 543,507 | 1,111 | 326 | 0 | 0 | 0 | 0 | **0 / 0 / 0** |

P1 is the index's variants (one per record and normalized key), so a literal spelling that
normalizes like another of the same record (`Mosseri, Jacques Ms. R 19`) is not a separate input.
These populations come from `libraries.csv` with an empty NLI cache (as every real-data test
here runs), so they say nothing about cache-only variants; those are pinned by synthetic tests
(a cache-only record, and a CSV record whose cached alternative carries the provenance).

- **No record of the named library is ever dropped**, no answer is emptied, no T2 answer loses a
  record, exactness never changes, nothing changes for an input whose prefix names no library,
  and no record present before and after changes its shown shelfmark (measured on every option).
- **P5's 8 "lost" sources are the rule working.** P5 pairs an alias with *another* record's
  number, so its source is not always the intended record: `adler Ms. 699` now gives the two
  records that store `Adler, Elkan Nathan Ms. 699` instead of also the record whose Adler number
  is 3273 and whose JTS number is 699; `adler Ms. 222` the record that stores `Adler, Elkan
  Nathan Ms. 222`, not the Sassoon 222 record.
- **The 41 replacements** are the intended fixes: `Halper 75/77/79` (14) to their Katz records,
  `GASTER 1775.N` (11) and `GASTER 633.1` to the BL records that store them instead of JTS
  `ENA ...`, `Bisno` (8), `Reinach` (6), `Corwin 1`.
- **One record -> two** (2,075 in P1, 99 in P2 name, plus `Halper 381`, whose one HAS record
  becomes the two Katz records): a stub and a full record carry the same shelfmark (Bodleian
  `arch. O.d.8/N`, `GASTER 1201.17`, JTS `S 407, membrane 10`); today the typed long form reaches
  only the stub.
- **T2 now answers** 8,255 stored P1 texts through the whole-input key -- 7,546 Gaster texts
  (`Gaster Printed series 61` -> its record; the bare `Gaster, Moses Collection` -> every record
  storing it, a list, as substring options listed before) and 709 others (`Austrian National
  Library Rainer Collection H 120` -> its Vienna record) -- and 11 in P4.
- `Halper 1`-`499`: every number that a record stores as `Halper N`/`Genizah N` now answers with
  those records (>= 480; today most took other libraries' records numbered N); `Halper 441` (no
  such record) keeps today's HAS record.
- Baseline fixture (1,219 inputs), regenerated on purpose and reviewed with
  `scripts/diff_shelfmark_resolve_baseline.py` against the pristine base: 23 entries change, all
  with a named prefix, 0 problems -- 7 stored variants now offer their source, 13 keep it in a
  smaller answer, 1 NLI form narrows to its record, and the 2 explicit ones `GASTER 1201.17`
  (both BL records) and `Halper 381` (the two Katz records), both given with `--expect`. All
  1,219 exactness flags are unchanged. (The tracked fixture's own `git diff` shows 126 changed
  rows: the previous fixture was recorded on master-main `ed05058f`, before PR #400's catalogue
  forms, so it also carries that PR's T2 changes.)

## 5. Changes

1. `shared/catalog_shelfmark.py`: `prefix_codes`, `named_prefix`, `in_libraries`,
   `named_library_exact`; `t1_exact_entries(..., scoped=True)` and `resolve_many`'s exact stage
   apply the rule (R0-R3); `_plan` uses `prefix_codes` and adds `w`; `_belongs` for an alias
   institution is `in_libraries` (provenance on zero-insensitive keys).
2. `shared/metadata_manager.py::resolve_system_by_shelfmark`: the named codes after the strip; one
   extra comparison per index entry only when a library is named; the bridge entry kept apart;
   `named_library_exact(...) or exact_matches` when there are exact matches.
3. `scripts/diff_shelfmark_resolve_baseline.py`: reviews a regenerated baseline (fails on a change
   to an input without a named library, a lost source, a single other record, or an explicit
   input changed without `--expect`).
4. Docs: this plan, `PLANS_INDEX.md`, `CODE_INDEX.md`, the `OPEN_ISSUES.md` entry archived.

## 6. Tests and gates (each shown able to fail)

1. **Synthetic** `tests/test_shelfmark_prefix_scope.py`: every row of section 3 as a corpus shape
   (Halper/Katz, Manchester/Mosseri, Bodleian and Gaster pairs, Glass 12 vs 1.2, 2162 vs 216.2,
   fallback, BnF/BNF, unmapped alias, no prefix), each round-1 finding (cache-only record, Sofer
   provenance, padded pair, empty remainder, bridge display), limits 0/1, shown shelfmark, the
   unscoped restatement, and resolver == single == batch.
2. **Real-data golden** `tests/test_shelfmark_prefix_scope_real.py` and the changed rows of
   `test_catalog_shelfmark_real.py::GOLDEN`, each listed with its reason.
3. **Population gate** (real data, `slow`): P1-P6, full responses of `resolve_many` against the
   same code with `prefix_rule=False` -- measured byte-identical to the pristine PR #400 code over
   all 2,022,705 distinct inputs, so the "before" is faithful. Asserted: no change at all for an
   input whose prefix names no library or for a head form's T2 answer; exactness unchanged; an
   exact answer drops no record that *independently* lies in the named library (its code, no csv
   row, or a stored `<prefix> ... Ms. <this shelfmark>` matched by the test's own regex) and adds
   only records that store the whole input; a T2 answer only grows, by records of the library or
   that store the input; no shown shelfmark changes for a record in both answers; no lost source
   outside P5's eight listed cases; class floors (full: measured minus 0.5%; CI's every-20th run:
   that slice's own measured counts minus 0.5% -- the first rule, 80% of a twentieth, failed on
   the slice because its classes are not spread evenly, e.g. 23 of P4's expected 35.5). With
   scoping disabled the floors fail.
4. **Adversarial gate**: R0 keeps its population as it was; it now says so by skipping the inputs
   the *unscoped* T1 answers exactly (rev. 1, which made inputs exact, would have moved 40,904
   comma and 5,707 no-comma cases out of it), same floors.
5. **Baseline** regenerated on purpose and reviewed with the diff script; old fixture kept in the
   PR description's numbers.
6. **Mutations**, each run separately and shown red (named test in the PR): no scoping; no (b);
   (a) first; (b) first; no numeric check on (b); (b)'s numeric check on the index's kept variant
   only; a dotless rule in (b); no numbers condition in R3; numeric check on (a)
   (rev. 1); provenance per record instead of per shelfmark; no provenance; unknown library
   dropped; a (b) record's whole library named; bridge always inserted; no R0; T2 without `w`;
   T2 alias without provenance; restatement not scoped; `w` for an empty remainder; provenance
   reading `Ms.` case-sensitively; provenance on plain keys; the query head split case-sensitive;
   the `prefix_rule` switch ignored; provenance ignoring the NLI cache; provenance reading the live
   cache instead of the snapshot the index carries; index entries built from the live cache
   instead of the frozen copy (26 in all, each red).
7. **Performance** (Codex round 2, on rev. 3): warm resolver median/p95 for prefixed inputs
   82/130 ms before, 83/128 ms after -- no change per request (the web resolves per request); the
   2M-input batch 162 -> 224 s (+38%), from per-candidate provenance; rev. 4 caches the parse per
   record. Re-measured for the PR.

**Inherited answers the rule leaves alone** (Codex round 7, verified; logged as P3 open issues):
`Halper 441` keeps today's HAS record stored `441` (no Halper 441 exists; R3); `adler MS 1512`
keeps today's list, which holds Mosseri `151/2` records and the record whose Adler number is 1873
(R3); `Westminster College Ms. L-G Glass 12` keeps `L-G Glass.1.2` beside the right record and
HAS `Ms. 36` keeps `Ms. 3, 6` (T1's dotless rule and number gluing, inside the named library);
`Gaster Ms. 1201.17` gives the full record but not the stub that stores `GASTER 1201.17` (the
whole-input key keeps the `Ms.`). None changes from today; the GOLDEN rows that pin them say so.

## 7. Owner questions (defaults in bold)

1. Two records carry the typed shelfmark (stub + full record): **offer both** / prefer the record
   that stores the typed text.
2. A named library with nothing matching, e.g. `Halper 441` (today: the HAS record stored `441`,
   another manuscript) or `BL T-S 12.123` (today: the Cambridge record): **today's answer** /
   offer it as a choice, never opened directly / not found. Codex recommends the last two for an
   explicit *institution* conflict and today's answer for collection aliases (`Gaster`, `Adler`,
   `Halper`), where today's answer keeps the right record in 89,297 measured cases.
3. `british library`, `national library of russia`, `jts library` name no `LIBRARY_CODES` code or
   name exactly: **leave them (as T2 does)** / map them in `alias_codes`.

## Revision log

- **Codex round 7 (finite audit, informational, SHIP WITH FIXES).** No newly introduced wrong or
  lost record in every GOLDEN row and 53 changed inputs across all 20 population/class pairs;
  every section-4 count confirmed by an exhaustive recount; the round-6 fix changes no answer on
  today's corpus (byte-identical responses). Corrections folded into section 4 (the 8,255 T2
  additions split, the 12 GASTER replacements, P1's scope, 2,076 one-to-two); the inherited
  answers are listed above and logged in `docs/OPEN_ISSUES.md`.
- **rev. 8 (2026-10-18), Codex round 6 (plan + code, DO NOT SHIP, 2 findings; round-5 fixes
  verified).** HIGH 1 (synthetic; no instance in today's CSV): the index keeps one variant per
  (record, key), so the numeric checks could test `GASTER 12 3` and miss `GASTER 123` of the same
  record -- (b) and R3's numbers condition now look at every stored variant of the record with
  that key (`_carries`). LOW 2: "the numeric check makes a dotless rule in (b) unreachable" was
  wrong (`21 62` and `21.62` share integers) -- a separate "dotless rule in (b)" mutation and its
  test now exist; 26 mutations.
- **rev. 7 (2026-10-18), Codex round 5 (plan + code, DO NOT SHIP, 3 findings; round-4 fixes
  verified for sequential use).** HIGH 1 and 2: the snapshot and the index read the cache at
  different moments and were published by two assignments, so a cache change mid-build or a
  concurrent rebuild could pair an index with other provenance -- the index is now a
  `_ShelfmarkIndex` (a list) that carries `cached_variants`; its cache entries are built from the
  same frozen copy (`_cached_shelfmarks`), one assignment publishes both, and every path passes
  the snapshot of the index it holds. LOW 3: the mutation list names what is mutated (no numeric
  check on (b)); 24 mutations.
- **rev. 6 (2026-10-18), Codex round 4 (plan + code, DO NOT SHIP, 3 findings; round-3 fixes
  verified).** HIGH 1: provenance read the live NLI cache while the index keeps what it was built
  from, so evicting a cache entry dropped a record the index still lists -- `_get_shelfmark_index`
  now snapshots the cached `shelfmark`/`shelfmark_alt` strings with the index
  (`_shelfmark_cached_variants`) and provenance reads that. MEDIUM 2: a non-string cached value
  crashed lookup -- only strings enter the snapshot. LOW 3: the mutation count is 23.
- **rev. 5 (2026-10-18), Codex round 3 (plan + code, DO NOT SHIP, 4 findings; round-2 fixes 1, 2,
  3, 5 verified).** HIGH 1: provenance read only csv call numbers, so a CSV record whose
  NLI-cache alternative is `Sofer, David Ms. 111.12` left `Sofer Ms. 111.12` -- `in_libraries`
  now also reads the record's cached `shelfmark`/`shelfmark_alt` (the resolver passes its cache on
  all three paths). MEDIUM 2: the gate's independent membership compared heads literally -- it now
  maps prefixes and stored heads to codes with its own restatement (case classes, `the`, first
  word), so `David Sofer Collection, 111/12` and `Gaster Collection 01201.017` are judged right.
  MEDIUM 3: the gate checks totals and the relative order of surviving records; the parity test
  compares every field at limits 0, 1 and unlimited. LOW 4: CODE_INDEX describes the current rule;
  the measurement's CSV-only population and the fixture's 126-row diff are stated.
- **rev. 4 (2026-10-18), Codex round 2 (plan + code, DO NOT SHIP, 5 findings).** HIGH 1: a
  lowercase `ms.` defeated the head split and `w` answered with one record -- the base now has
  PR #400's case-insensitive `QUERY_HEAD_RE` (`78ed2265`; branch fast-forwarded to `edb3ea61`), and
  provenance reads `Ms.` in any case too. HIGH 2: T1 provenance compared plain keys, so a
  zero-padded stored `Gaster ... Ms. 01201.017` lost the full record -- provenance now compares
  `zkey` on both paths (parse cached per record). MEDIUM 3: the padded Bodleian `Arch. O.d. 08.028`
  GOLDEN row now offers both records (and `GASTER 0967.03` both BL records); listed with reasons.
  MEDIUM 4: the population gate compares full responses with `resolve_many(prefix_rule=False)`,
  judges membership independently, gates T2, lists P5's eight cases, and has floors in both
  modes. MEDIUM 5: the measurement records every option's shown shelfmark and the total.
- **rev. 3 (2026-10-18), after measuring rev. 2 on full responses.** (a) is no longer
  number-checked: scoping removes other libraries' records only; R3 instead refuses to scope when
  nothing kept carries the numbers (rev. 2 dropped in-library dotless matches listed today beside
  the right record, 506-844 inputs per population). Provenance is per shelfmark (rev. 2's
  record-wide reading made `adler MS 1512` the record whose Adler number is 1873). A (b) record's
  whole library is no longer named, in T1 or T2 (it made `Halper 275` add Katz `Ms. 275`); per-
  shelfmark provenance keeps the full Gaster records instead.
- **rev. 2 (2026-10-18), Codex round 1 (DO NOT SHIP, 9 findings).** HIGH 1: R0 -- only inputs T1
  answers exactly are scoped. HIGH 2: T2's alias institution names the libraries of `w` hits.
  HIGH 3: unknown library kept. HIGH 4: collection provenance; the Sofer loss is gone; the
  institution-conflict choice is owner question 2. MEDIUM 5: GOLDEN changes enumerated
  (`Halper 381` is a fix, two Katz records; `Halper 441` is the fallback example). MEDIUM 6:
  exactness is now invariant (R0+R3), so the generator's flags stay correct; the diff script
  rejects out-of-scope and unexpected changes. MEDIUM 7: full-response measurement over all
  populations, uncapped P5, P6 short-only records. LOW 8: bridge entry only if not kept already.
  MEDIUM 9: empty remainder rejected in `prefix_codes`.
- **rev. 1 (2026-10-18).** First plan, measured on exact sets.
