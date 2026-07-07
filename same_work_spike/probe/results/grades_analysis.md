# Grades analysis — 164 graded pairs (2026-07-07)

## Overall
- grades: {'same_text': 84, 'canonical': 20, 'shared_formula': 5, 'unrelated': 1, 'junk': 11, 'paraphrase': 1, 'duplicate_photo': 42}
- REAL shared text (same/paraphrase/formula/canonical): **110/164 = 67.1%**
- duplicate photography: 42 (25.6%) — removed by stage-0
- junk (microfilm title sheets): 11 — removed by stage-0
- ACTUALLY SPURIOUS: **1/164 = 0.6%**
- precision after stage-0 removes dup+junk: **110/111 = 99.1%**

## Per stratum
- **bh_boundary** (n=31): {'same_text': 29, 'shared_formula': 1, 'canonical': 1}
- **discovery** (n=40): {'duplicate_photo': 6, 'same_text': 34}
- **join_anomaly** (n=36): {'duplicate_photo': 36}
- **overlap_cross** (n=32): {'same_text': 4, 'canonical': 19, 'shared_formula': 3, 'unrelated': 1, 'junk': 5}
- **overlap_related** (n=17): {'shared_formula': 1, 'same_text': 15, 'paraphrase': 1}
- **short_span** (n=8): {'junk': 6, 'same_text': 2}

## Per ENGINE density band (excl. dup/junk — post-stage-0 view)
- density [0.00,0.30): n=28, real=28 (100%), detail={'same_text': 28}
- density [0.30,0.35): n=14, real=14 (100%), detail={'same_text': 14}
- density [0.35,0.40): n=30, real=30 (100%), detail={'canonical': 11, 'same_text': 18, 'paraphrase': 1}
- density [0.40,0.45): n=38, real=37 (97%), detail={'same_text': 23, 'canonical': 9, 'shared_formula': 5, 'unrelated': 1}
- density [0.45,0.51): n=1, real=1 (100%), detail={'same_text': 1}

## Discovery stratum (the headline capability)
- n=40: {'duplicate_photo': 6, 'same_text': 34}
- REAL discoveries (same_text, not dup/junk): **34**

## Line-agreement detector vs human duplicate grades
- human graded duplicate_photo: 42; detector flagged 31 of them (recall 74%)
- detector flagged 31 graded items; 31 are human-confirmed duplicates (precision 100%)

## Label semantics (Hillel's policy, binding for future annotation)
1. `junk` = microfilm opening-title sheets — identical images, not part
   of the manuscript; stage-0 filter class.
2. `canonical` = quotation embedded in a DIFFERENT work. Two Bible MSS
   of the same passage = `same_text` (both are witnesses of the work).
3. `same_text` is judged at the UNIT level: siddur-BH vs Haggadah-BH =
   same_text — the shared liturgical unit is the atom, not the
   codicological container. (=> same-work clustering must cluster
   UNITS, not manuscripts.)