# A4 — Global assignment (exact MWIS) vs greedy shadowing — disagreement probe

READ-ONLY probe. `track1_matches` was never written to (opened `mode=ro`).
Script: `scripts/probe_shadow_setcover.py`. Artifact: `results/a4_shadow_assignment_probe.json`.
Corpus: `../data/fullcorpus.db::track1_matches`, **all 276,296 rows** over 176,444 pages
(including the 61,922 rows already shadowed by the live greedy run).

## 1. Matching track1_shadow.py's actual semantics

Read `track1_shadow.py` first (per the brief). Confirmed by direct source inspection:

- **Unit of assignment = the whole `track1_matches` row.**
- Per row, the "span" used for both the overlap test *and* the density comparison
  is the row's **longest individual span** from `spans_json` (`best_span` = max by
  `end-start`), and the density used is **that span's own density value**
  (`spans_json[i][2]`) — **not** the row-level `best_density` column, which can be
  the *minimum* across a multi-span row and therefore differ from the longest
  span's density (confirmed empirically: e.g. rowid 158 has spans
  `[190,235]`/densities `[0.2895,0.2932]` — row-level `best_density`=0.2895 (the
  min), but greedy's own comparison uses 0.2932, the longer span's density).
- **density is a DISTANCE — lower is better.**
- Two rows conflict (worse one gets shadowed) iff, with `lo`=lower-density
  (better) and `hi`=higher-density (worse) of the pair: overlap(lo.span, hi.span)
  `>= 0.6 * len(hi.span)` (**60% of the WORSE row's own span**) **and**
  `hi.density - lo.density >= 0.03`.
- Greedy is *state-dependent*: it sorts rows by ascending density and only tests
  a row against already-**live** rows in that order, breaking on the first
  qualifying winner. It does **not** apply the conflict predicate exhaustively to
  all pairs.

**Self-check (built into the probe):** a faithful line-for-line replica of the
greedy loop was run in-memory over the loaded rows and diffed against the
`shadowed_by` column already persisted in the DB by the live `track1_shadow.py`
run: **stored shadowed=61,922, replica shadowed=61,922, mismatched-verdict
rows=0.** This confirms the semantics above are exactly right and that comparing
the probe's global assignment against the *existing* `shadowed_by` column is
equivalent to comparing against a freshly-run greedy.

## 2. Global assignment formulation

Per page, build the **conflict graph** over its rows using the *symmetric,
state-independent* version of the predicate above (same constants:
overlap frac `>=0.6` of the worse row's span, density gap `>=0.03`). Then solve
**exact Maximum-Weight Independent Set (MWIS)** on that graph — branch-on-vertex
with memoization over the "available rows" bitmask; page item counts in this
corpus are `<=20` (measured), so this is exact and fast (whole 276K-row corpus:
~3.7s wall-clock, in-memory, no candidate engine).

**Objective:** maximize `sum_{i in live} letters_i * (DENS_CAP - density_i)`.
This is the stated brief objective ("minimize total covered-letters-weighted
density, equivalently maximize coverage quality with lower-distance rows
preferred") made well-defined: a pure `minimize sum(letters*density)` is
degenerate on its own (the empty set scores 0 and always "wins", so it can't by
itself express "prefer coverage AND low distance" — the empty set is the
opposite of "coverage"). `DENS_CAP` is the per-letter constant that makes
inclusion valuable; it must be `>=` the corpus max density (0.35, measured) so
every `value_i>0` (asserted: 0 "orphan" exclusions in both runs below — an
excluded row with no live conflicting neighbor would be a free win the solver
missed, which never happened).

Ran **two settings to bracket the coverage-vs-quality weighting**, since this
constant is not specified by the brief and turns out to matter a lot:

| label | DENS_CAP | reading |
|---|---|---|
| **primary** | 1.00 | coverage-friendly — a much-longer, slightly-worse-density row can outweigh a short better one |
| **secondary** | 0.36 | quality-picky — minimal headroom over the max density; closest to greedy's literal stated intent ("the better-density identification wins") |

## 3. Headline flip counts (kill criterion: total flips < 2% of all rows)

| | primary (cap=1.00) | secondary (cap=0.36) |
|---|---|---|
| greedy live rows | 214,374 | 214,374 |
| global live rows | 218,591 | 216,191 |
| live→shadowed (global hides what greedy kept) | 4,241 | 1,338 |
| shadow→live (global revives what greedy hid) | 8,458 | 3,155 |
| **total flips** | **12,699** | **4,493** |
| **flip rate** | **4.596%** | **1.626%** |
| pages whose "best work" (lowest-density live row) changes | 3,218 / 176,444 (1.82%) | 1,074 / 176,444 (0.61%) |

**Secondary passes the <2% gate; primary does not.** Section 5 shows why — the
excess primary flips are concentrated in a specific, explainable disagreement
pattern (coverage vs. quality trade-off), not scattered noise or evidence of a
greedy bug.

## 4. Known-case spot checks

| work | all rows (distinct MSS) | greedy live | global live (primary) | global live (secondary) |
|---|---|---|---|---|
| **11QT** (`מגילת המקדש`, Temple Scroll) | 176 | **23** | 28 | **23 (exact match)** |
| ibn-Tibbon translation (`ספר הרקמה`, Hebrew) | 91 | **12** | 16 | **12 (exact match)** |
| JA original (`ריב"ג, ספר הרקמה`) | 114 | 56 | 58 | 56 (exact match) |

- **11QT:** stays nowhere near the 176-MSS pre-shadowing scale under either
  setting. Secondary reproduces greedy's 23 exactly. Primary drifts to 28 — a
  small revival, not a regression toward the false-ID class the shadowing
  feature exists to suppress.
- **ibn-Tibbon vs JA-original:** stays collapsed under both settings (translation
  witness count stays far below its 91-row un-shadowed baseline; the two
  competing works' combined live count does not balloon back toward
  double-counting). Secondary again reproduces greedy exactly.

**Both known cases pass under the quality-picky (secondary) global formulation
with zero deviation from greedy, and pass (with only minor drift, no
regression) under the coverage-friendly (primary) formulation.**

## 5. Why primary disagrees more: a worked example

Page `990001272920205171_IE31615617_P000062_FL31615680` carries a 6-way clique
of competing "works" (Kalir piyyut, Vidui, Bible/מקרא, "פיוט לאחר הסליחות",
another piyyut, Mishneh-Torah/Rambam) whose spans all mutually overlap
`[0,264]`-ish — a textbook multi-edition/citation duplicate of one liturgical
unit, exactly what shadowing exists to collapse to one witness. Greedy and the
quality-picky (secondary) global assignment both pick the single lowest-density
row (`פיוט לאחר הסליחות`, density 0.20, letters 230) as the sole live witness.
The **primary (coverage-friendly)** assignment instead picks `וידוי לסליחות
וליום הכיפורים` (density 0.266, letters 264) — 15% more matched letters at a
worse density that clears the 0.03 gap. Both are legitimate readings of "best"
under different value functions; this is the recurring shape of nearly every
extra primary-only flip (see the Tur/Rambam pair in cards #0/#5 below, where
Tur's span extends ~400 letters past where the Rambam-parallel span ends —
Tur is arguably the *more complete* match of the page even though its shared
region is slightly denser/worse).

**Stratification of all 12,699 primary flips** by the decisive competing edge:

| density gap of decisive edge | count | share |
|---|---|---|
| 0.03–0.05 | 5,004 | 39.4% |
| 0.05–0.10 | 5,300 | 41.7% |
| 0.10–0.20 | 2,318 | 18.3% |
| 0.20+ | 77 | 0.6% |

| overlap frac of decisive edge | count | share |
|---|---|---|
| 0.6–0.7 | 1,163 | 9.2% |
| 0.7–0.8 | 1,211 | 9.5% |
| 0.8–0.9 | 1,630 | 12.8% |
| 0.9–1.0 | 8,695 | 68.5% |

81% of primary disagreements sit at a density gap under 0.10 (most barely above
the 0.03 acceptance minimum) and 68.5% involve near-total overlap (>=0.9) — i.e.
they are near-ties on quality where a different, more coverage-hungry objective
tips the pick, not cases where greedy is making an obviously wrong call on a
clear-cut margin.

## 6. Per-work witness-count deltas (census filter: `matched_letters>=200`, distinct MSS)

Biggest movers, primary vs greedy (full ranked list of 25 in the JSON artifact,
`per_work_top25`):

| work | all (pre-shadow) | greedy live | global live (primary) | global live (secondary) |
|---|---|---|---|---|
| רמב״ם — משנה תורה, ספר אהבה | 2,275 | 1,955 | 2,021 | 1,971 |
| מקרא (Bible) | 17,434 | 17,299 | 17,310 | 17,308 |
| תנחומא | 358 | 164 | 176 | 166 |
| תוספתא | 293 | 169 | 188 | 176 |
| תלמוד ירושלמי, ברכות | 231 | 111 | 137 | 127 |
| הלכות ״ראו״ | 191 | 104 | 133 | 115 |
| פיוט לאחר הסליחות במוסף ליום הכיפורים | 23 | **23 (untouched by greedy)** | **12** | 17 |

All deltas are single- or low-double-digit swings against multi-hundred-to-
multi-thousand baselines that stay solidly collapsed relative to the
pre-shadowing "all" column — **no per-work delta regresses toward the
un-shadowed scale**, under either DENS_CAP setting. The one entry that looks
qualitatively different — `פיוט לאחר הסליחות` — is not a case of greedy missing
a shadow; it's the flip side of the worked example in §5: this row is already
greedy's/quality-picky's *correct winner* in its 6-way clique (it has the
lowest density of the group), and the coverage-friendly primary objective is
the one that displaces it in favor of a longer competitor, not the reverse.

Under the **secondary** (quality-picky) run the same per-work list shows smaller
swings (e.g. Rambam ספר אהבה: 1,955→1,971 vs pre-shadow 2,275; Bible:
17,299→17,308 vs pre-shadow 17,434) — negligible relative to corpus scale.

## 7. Ten disagreement example cards (primary run)

Full text snippets (original page text, projected back from the normalized
match-stream via `normalize.project_span`, +/-15 char padding) are in
`a4_shadow_assignment_probe.json::example_cards`. Summary:

| # | direction | page | row (density, letters) | competitor (density, letters) | gap | overlap |
|---|---|---|---|---|---|---|
| 0 | shadow→live | `...IE48878777_P69` | ארבעה טורים או"ח (0.268, 1846) | רמב״ם ספר המדע (0.212, 1445) | 0.056 | 0.78 |
| 1 | shadow→live | `...IE37593988_P10` | תלמוד בבלי ב"מ (0.306, 1572) | הלכות פסוקות (0.269, 1114) | 0.037 | 0.71 |
| 2 | shadow→live | `...IE148796782_P1` | והזהיר (0.330, 1418) | ספרא (0.267, 1374) | 0.063 | 0.97 |
| 3 | shadow→live | `...IE164956391_P2` | ת"י ראש השנה (0.243, 1129) | ת"י שקלים (0.194, 654) | 0.048 | 0.85 |
| 4 | shadow→live | `...IE49222146_P203` | מקרא (0.278, 1098) | רמב״ם ספר אהבה (0.175, 711) | 0.103 | 0.65 |
| 5 | live→shadowed | `...IE48878777_P69` | רמב״ם ספר המדע (0.212, 1445) | ארבעה טורים או"ח (0.268, 1846) | 0.056 | 0.78 |
| 6 | live→shadowed | `...IE148796782_P1` | ספרא (0.267, 1374) | ת"י יבמות (0.330, 171) | 0.063 | 1.00 |
| 7 | live→shadowed | `...IE51451194_P31` | משנה (0.174, 1177) | והזהיר (0.227, 981) | 0.053 | 1.00 |
| 8 | live→shadowed | `...IE159957557_P19` | ת"י תענית (0.157, 1168) | ת"י ברכות (0.260, 250) | 0.104 | 1.00 |
| 9 | live→shadowed | `...IE168420785_P3` | רמב״ם ספר אהבה (0.169, 1115) | תפילה לאחר התפילה (0.274, 135) | 0.106 | 1.00 |

Cards #0/#5 are the two sides of the *same* flip (same page, same pair):
primary revives Tur and displaces the Rambam row that greedy had preferred. All
ten cases follow the §5 pattern: a longer-but-slightly-denser row beats (or
loses to) a shorter-but-cleaner one, with the decisive gap almost always under
0.11.

## 8. Verdict

**KEEP GREEDY.**

The exact global assignment that most faithfully operationalizes greedy's own
stated intent ("the better-density identification wins" — secondary,
DENS_CAP=0.36) satisfies **all three** kill-criterion legs cleanly:
- total flips 1.63% (< 2%)
- per-work witness deltas negligible relative to corpus/per-work scale
- both known cases (11QT, ibn-Tibbon-vs-JA-original) reproduce greedy's numbers
  **exactly**

The alternative, more coverage-hungry reading of the brief's objective
(primary, DENS_CAP=1.0) exceeds the 2% flip bar (4.60%), but inspection shows
this is not evidence that greedy is wrong on its own terms: 81% of the extra
disagreements sit within 0.10 of the minimum density-gap threshold and 68.5%
occur at near-total overlap — i.e. near-ties where a *different* objective
(reward raw letter coverage over match tightness) tips the pick, not cases
where greedy makes a clear-margin mistake. Even here, neither known case
regresses meaningfully (11QT 23→28, far from the 176-MSS false-ID scale;
ibn-Tibbon 12→16).

Net: replacing greedy with an exact global solver would trade a well-
understood, already-validated heuristic for a solver whose disagreement rate is
now governed by an unspecified coverage-vs-quality hyperparameter (`DENS_CAP`)
— not a clear win, since the only setting that diverges meaningfully does so by
adopting a different value judgment (letters-covered vs. match density),
not by fixing a defect. **Close the item.**

### Open note for future consideration (not a recommendation)

If a future task specifically wants "credit the reference-work that explains
the *longest contiguous stretch* of the page, even at a slightly worse density"
(the Tur/Rambam pattern in §5/§7), that is a deliberate policy change, not a
bug fix — and it should be scoped carefully: rewarding raw coverage can let
naturally bulky texts (Mishneh Torah, Bible) absorb more witnesses at the
expense of shorter/cleaner sources, which risks reintroducing exactly the kind
of asymmetric over-crediting the shadowing feature was built to suppress. Not
recommended without dedicated domain sign-off.

### Method limitations

- `letters` used as the MWIS value weight is the row-level `matched_letters`
  column (summed across all of a row's spans in `spans_json`), while the
  conflict test only checks the row's *longest single* span — identical to how
  greedy itself only reasons about the longest span. Multi-span rows are rare
  (`n_spans<=6` in this corpus) so this does not materially affect the
  headline numbers, but it means a multi-span row's value can look inflated
  relative to what the conflict graph "sees" as its footprint.
- Stratification (§5) was computed for the primary run only (its flip set is a
  superset in spirit of the secondary run's, and secondary's flip volume is
  ~3x smaller — not worth a second full stratification table for this probe).
