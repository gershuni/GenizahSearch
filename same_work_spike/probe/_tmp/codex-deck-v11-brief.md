# Codex review brief — deck v11 (title-gate router + cite-gate v11)

## Context

`scripts/mapv2_deck.py` builds the Genizah "discovery deck" from tier-B
candidate rows (SQLite `track1_candidates`, ~1.34M rows) after a chain of
guards. A gold-standard annotation of the previous (v10) deck's 88 cards
proved: the catalog title is a near-perfect router (generic/absent title →
75% true discoveries; different-specific title → 85% shared-source leaks;
same/variant → 100% already-known). v11 implements:

1. NEW `scripts/title_gate.py` — deterministic classifier
   `TitleGate.classify(sys_id, claim_name) -> (class, evidence)` over
   NLI titles (libraries.csv, passed in) + FJMS catalog identifications
   (SQLite `catalog.GenizahTitleOrgTitle` per AlmaId, loaded read-only in
   __init__). Classes: generic_or_absent / same_work / name_variant /
   known_quoter / different_specific. Validated 71/88 vs gold with the two
   dangerous error directions at 1 and 3 (documented, accepted).
2. `mapv2_deck.py` section assembly rewritten: pools per title class →
   sections (discoveries P≥0.8 / P 0.5–0.8 / doubt+small-margin demotion /
   reversed-citation / catalog-says-otherwise / catalog-confirmations /
   known-dependence / statutory). `fill()` helper with per_work/per_ms
   caps, `used` dedup across sections, small-margin bands (m_003_010,
   m_0_003) excluded from top sections (small_any_p routes them to the
   doubt section).
3. Cite-formula gate v11: the v10 exemption (claims on works whose OWN
   stream contains citation formulas ANYWHERE) exempted the wrong side and
   re-admitted a known leak family. Now: a page-side formula in the window
   [-38,+30) around the span's raw start demotes UNLESS the claimed work's
   stream carries a formula at the ALIGNED position (partial_ratio_alignment
   of the normalized page span into the work stream, window [-45,+35) of
   dest_start). Kept rows recorded in `exempt_cite`.
4. NEW markers: 'ואמרו במדרש', 'אמרו במדרש', 'וגדסי', 'כמא קאל', 'לקולה',
   "לקו'", 'לקו׳' (Judeo-Arabic family + HTR garble; raw-text matching).
   Work-side normalized markers now len>=4 (was 5).
5. `reversed_citation(row)` at display time: work-side formula near
   dest_start, page side clean (and not in exempt_cite) → card diverted to
   a "reversed citation — potential find" section (cap 10).
6. Card JSON dump gains title_class/title_evidence.

## Files to review

- C:\Genizahsearch\same_work_spike\probe\scripts\title_gate.py (whole, new)
- C:\Genizahsearch\same_work_spike\probe\scripts\mapv2_deck.py — focus on:
  CITE_MARKERS block (~line 199), cite gate v11 (search "cite-formula
  demoter v11"), section assembly (search "title-gate routing, MAPV2-A"),
  report additions (search "Title-gate router (v11)").

## Review focus (rank findings BLOCKER / HIGH / MED / LOW)

1. Correctness of the cite-gate rewrite: window arithmetic (raw vs
   normalized coordinates — poffs_c maps stream index→raw offset; spans are
   stream coords), exempt/drop logic, any row class silently skipped.
2. The fill()/pools/used flow: can a row be double-displayed or wrongly
   dropped? per_work/per_ms caps not enforced for reversed items — is the
   cap-10 enough? card_no/document-order consistency of deck_cards vs
   rendered sections.
3. reversed_citation: false-positive risk (formula INSIDE the matched
   region because match starts mid-quote), pstream cache interaction,
   alignment score_cutoff=45 sanity.
4. title_gate.py: normalization pitfalls (geresh/gershayim stripped BEFORE
   tokenization — does that merge abbreviations wrongly?), _is_generic_token
   prefix stripping over-aggressive?, genre_conflict fail-open behavior,
   TitleGate SQLite lifecycle (connection closed, read-only), AlmaId type
   (int in DB vs str sys_id keys — verify the str() cast covers it).
5. New markers: false-positive risk of 'לקולה'/'כמא קאל'/'וגדסי' in raw
   HTR text; "לקו'" apostrophe variants coverage (U+05F3 vs ASCII).
6. Performance traps: per-row partial_ratio_alignment against full work
   streams in the cite gate — bounded by marker-firing rows only? wstreams
   lifetime vs the later `del`+gc block (search "free guard memory") — the
   v10 code filtered wstreams to kept_wids there; verify v11 still does and
   that reversed_citation/flank use it AFTER filtering only for kept rows.
7. Checkpoint fingerprint: guard params unchanged so old NDJSON verdicts
   load — is that still sound given the cite gate now admits DIFFERENT rows
   (exempted-aligned) that may never have been guard-tested? (The guard todo
   mechanism should catch them as new (pid,wid) keys — verify.)

Output: numbered findings with severity + one-line fix each, then a final
verdict line: APPROVE or REVISE.
