# SEED-029 — 100K-Page Scale Rehearsal & First Text-Reuse Map (2026-07-07)

Follow-on to the separability probe (`PROBE-RESULTS.md`, method in `METHOD.md`).
Goal: (1) validate the **numpy sort-merge representation** at 6× pilot scale and measure
the candidate-volume scaling law; (2) run **stage-0 over the full corpus** and measure its
cut; (3) produce the **first corpus-map artifacts** (cluster census, library matrix, atlas).
All on the dev box, single-threaded Python+numpy.

## VERDICT: engineering GO for the full corpus — with ONE structural gate

The engine scales (14 min end-to-end at 102,568 pages, recall *improved*), stage-0 works,
precision holds. But the map exposes the predicted hairball empirically: **one giant
component of 15,969 manuscripts (89% of all connected MSS) — the Bible/liturgy/piyyut
continent — and flank-contrast alone does NOT break it.** Track-1 canonical masking is not
an enhancement; it is **the gating feature** for a legible works-census. Everything else is
mechanical.

## The corpus + stage-0 (full 948K-record pass, 131 s)

| Stage-0 class | Records |
|---|---|
| Streamed | 948,549 |
| Dropped: short/empty (<80 letters) | **231,679 (24.4%)** |
| Dropped: duplicate photograph (same FL image id) | **40,452 (4.3%)** |
| Dropped: microfilm target sheet / catalog card | 9,007 |
| **Effective corpus** | **≈667,400 pages** |

Sample: 100,000-page uniform reservoir + 2,568 tracer pages (1,211 BH-witness + 1,357
Tier-1-title pages) = **102,568 pages, 86,993,987 letters**.

## Engine at scale (engine_np.py — packed-uint64 sort/reduceat, no dicts)

Validation first: on the pilot corpus the numpy engine reproduces the dict engine exactly —
11,435,521 unique candidates (dict: 11.4M), tier-1 titles recall 0.977, in **81 s / ~2 GB**
vs the dict engine's 334 s / 15 GB.

100K run:

| Measurement | Value |
|---|---|
| positions indexed | 86,583,715 (sorted in 7 s) |
| grams kept (2 ≤ DF ≤ 100) | 2,233,319 (singletons 705K, DF-dropped 161K) |
| raw pair-hits | **653,611,484** (~10.5 GB emission, 9 chunks) |
| accumulator entries (pair × diagonal-bucket) | 590,944,966 |
| candidate segments → unique pairs | 40,549,024 → **38,232,433** |
| candidate stage wall-clock | **359 s** |
| verify (sloped boundary, score_cutoff early-exit) | 40.5M segments in **127 s** (~3 µs/pair) |
| accepted page pairs | **337,069** |
| end-to-end (incl. classify) | **~14 min single-threaded** |

**Recall at scale (tracers):** Tier-1 titles **0.993** (pilot 0.977 — recall *rises* with
corpus size: more witnesses present = more anchor paths); BH witness connectivity
**326/512 (64%)** vs 56% at pilot.

**Volume scaling law (the number the rehearsal existed to measure):** pages ×5.7, page
pairs ×32 — but raw hits only ×4.5 (146M → 654M). The absolute DF≤100 cap self-tightens
exactly as predicted; candidate volume grows ~linearly with corpus size, not quadratically.
DF sensitivity at 100K (probe_volume.py): DF≤50 → 238M, **DF≤100 → 654M**, DF≤200 → 1.57B,
DF≤400 → 3.30B. Extrapolating ~linearly, the full 667K-page corpus lands at **~4–5B raw
hits** → the same sort-merge with **disk-partitioned pair-hash spill** (partition by pair
key; each partition reduces independently; ~70 GB scratch disk) — a mechanical extension,
no algorithmic change.

## The first text-reuse map (102,568 pages)

After stage-0 filtering of accepted pairs (672 residual duplicates — extraction-time FL
dedup already did the heavy lifting): **336,397 clean page pairs → 244,020 manuscript
pairs over 17,994 manuscripts** (`results/rehearsal_100k_map.md`).

### The giant component (the structural finding)

| Layer | MS-pair edges | Components ≥2 | Largest |
|---|---|---|---|
| all | 244,020 | 780 | **15,969** then 17, 17, 14… |
| continuation (same-unit) | 85,598 | 862 | **11,920** then 51, 20, 18… |
| island (quotation/formula) | 166,470 | 504 | **12,914** then 31, 8, 7… |

One component holds 89% of connected manuscripts; its top titles are פיוט / אלמקדמאת /
מקרא — the canonical continent (Bible + liturgy + piyyut + exegesis, bridged; its
Louvain communities differentiate by domain: 41 Bible / 27 Liturgy / 19 Piyyut /
15 Exegesis / 10 Halakha — so "liturgical" alone would mislabel it). Flank-contrast
reduces but does not break it
(shared liturgical *sequences* legitimately continue across flanks). **Consequence: Track 1
(canon identification vs clean reference corpora + character-level masking before Track-2
indexing) is the prerequisite for the works census** — not Leiden tuning, not thresholds.

### Flank measurement correction (methodological)

Run-time flanks compared unequal lengths (`normalized_distance` floors at the length
ratio: 60-vs-150 letters ≥ 0.60 regardless of content) — fixed by equal-length clipping
(`fix_flanks.py`, L = min(150, avail) ≥ 60 both sides). Corrected distribution over clean
page pairs: continuation (≤0.52) 8,215 · ambiguous 3,606 · island 226,742 · edge (span
reaches page boundary, no flank) 97,834. The empirical random floor is **~0.80**, not the
~0.6 assumed in Round 3 — headroom for a future mid-band (0.52–0.70) reclassification.
Island dominance is real, not artifact: in this corpus most sharing IS bounded-passage
sharing (liturgy, quotations, formulae). The large `edge` class = spans running to the
page boundary — mostly same-work pages whose overlap covers the whole page; the full
pipeline should chain page-boundary spans across consecutive pages of the same manuscript.

### Beyond the blob: the works census is real (atlas, top clusters)

`review/rehearsal_100k_atlas.html` (120 clusters) + `results/rehearsal_100k_clusters.csv`.
Highlights — all machine-clustered from a *random* 100K sample, titles from the catalog:

- **C2 — 51 MSS across RNL/BL/CUL/JTS/Oxford**: חבור בדקדוק עברי / חבור על טעמי המקרא
  (Hebrew grammar treatises — one tradition connected across five collections)
- **C3 — 20 MSS**: פראיץ אלקלוב (JA, Duties of the Hearts) · **C4 — 18 MSS**: אלמרשד
  אלכאפי · **C9 — 12 MSS**: כתאב אלמחתוי (al-Basir's Kitāb al-Muḥtawī) · **C20 — 8 MSS**:
  קצור מלון אלפאסי — the Judeo-Arabic classics assemble themselves
- **C5 — 18 MSS**: Gittin formulas; **C18 — 8 RNL MSS**: Karaite ketubbot (dated Cairo
  1644/1693) — the formulaic-genre layer clusters as predicted
- **C7 — 14 MSS**: קצת חנה (Story of Hannah) · **C15 — 8 MSS**: Mishneh Torah introduction
  · **C14 — 9 MSS spanning Lehmann/RSL/Freer/Sassoon** — minor collections joined to the
  majors, several members with no catalog title (identification candidates)

### Library × library (clean MS pairs, top)

RNL–RNL 113,984 · CUL–RNL 63,861 · CUL–CUL 20,602 · JTS–RNL 11,099 · CUL–JTS 6,540 ·
Oxford–RNL 5,532 — the RNL (Firkovich) Bible density dominates; cross-collection edges
CUL↔RNL alone are 64K manuscript pairs.

## What this changes for the full-corpus run (ordered)

1. **Track 1 canon masking** — now empirically THE gate for map legibility (was "next
   structural gain"). Build against Maagarim/Sefaria + the liturgical corpus before the
   works-census map is produced.
2. **Disk-partitioned sort-merge** for ~4–5B raw hits (pair-hash partitions; mechanical).
3. **Page-chain extension** for `edge`-class spans (join consecutive pages of one MS).
4. Keep: DF≤100 (self-tightening confirmed), sloped boundary (recall rose at scale),
   equal-length flanks, extraction-time FL dedup (672 residual dups of 337K accepted).
5. Instrument peak RSS next run (not captured this run; design estimate ~35–40 GB at the
   final merge — the 63 GB box held comfortably).

## Track 1 (2026-07-07, same session): Maagarim/JA identification + canonical masking

Reference corpus (user-provided): **5,274 Maagarim works** (74.1M letters — full
Tanakh/Mishnah/Tosefta/Bavli/Yerushalmi + 5,182 edited works incl. piyyut, geonica,
letters) + **89 Friedberg JA works** (12.7M letters). Asymmetric matcher
(`track1_match.py`): reference gram index (DF cap 128 entries/code — drops 143K
formulaic codes carrying 60% of posting mass) → page-gram searchsorted → diagonal
two-hit per (page, 3.8K-letter segment) → one-sided verify (density ≤ 0.28 under
100 letters / 0.35 above; one-sided noise ⇒ tighter than Track 2's 0.42).

**Identification results (102,568 pages, 24 min):**

| Measurement | Result |
|---|---|
| pages identified (≥1 work) | **27,033 (26.4%)**; ≥100 matched letters: 25.9% |
| (page, work) rows | 42,679 (Bible 17,214 · Maagarim-other 17,585 · JA 4,096 · Bavli 2,357 · Mishnah 750 · Yerushalmi 480 · Tosefta 197) |
| Bible-domain pages matched (recall proxy) | **66.3%** |
| Liturgy-domain pages | 51.0% |
| Documents / Sciences pages (precision floor) | 3.7% / 1.1% |
| mesirah channel (matches to editions-of-Genizah-fragments) | 1,490 strong rows w/ source shelfmarks (`##המסירה##`) |

Top identifications read like a Genizah syllabus: Mishneh Torah (Sefer Ahavah 1,257
pages), Saadia (tafsir, פירוש דניאל, siddur, בקשה), רשב"ח Torah commentary, ibn Janah's
ספר הרקמה in BOTH the JA original AND ibn Tibbon's Hebrew, הלכות פסוקות, שאילתות,
Nethanel al-Fayyumi, Abraham Maimonides, ברכת המזון (118 pages).

**Masked Track-2 rerun** (`rehearsal_run.py … mask`: Track-1 spans dropped from the
gram index — "Track 2 never sees identified text"; 15.5M grams masked = 18%):

| | unmasked | masked |
|---|---|---|
| accepted page pairs | 337,069 | **72,741 (−78%)** |
| clean MS pairs / MSS | 244,020 / 17,994 | 48,570 / 9,588 |
| giant component (all / continuation) | 15,969 / 11,920 | **7,561 / 4,389** |
| tier-1 titles recall | 0.993 | 0.986 (non-canonical works unharmed) |
| library matrix head | RNL–RNL 114.0K, CUL–RNL 63.9K | RNL–RNL 39.2K (81% of map), CUL–RNL 2.9K |

Reading: masking absorbed the **Rabbanite canonical core** (Bible/Talmud/liturgy —
CUL–RNL cross-links collapse 63.9K→2.9K). The residual giant (4,389 MSS, 70% RNL,
top titles אלמקדמאת / piyyut / סדור מנהג קראים) is the **Karaite-liturgy + piyyut
continent — text shared across many MSS that is NOT in Maagarim/JA**: part missing
reference coverage (Genizah piyyut, Karaite siddur/exegesis), part genuinely
unedited. That residue is itself a discovery product: high-witness-count unidentified
units. Masked top clusters are markedly cleaner (Rif, ibn Shuaib's דרשות, Saadia's
siddur ×3, Targum Onkelos, shtarot/ketubbah formulas).

## Artifacts

| Path | What |
|---|---|
| `scripts/stage0.py` | stage-0 filters + dedup tiers (a)–(d) |
| `scripts/extract_rehearsal.py` → `data/rehearsal.db` | corpus (gitignored, regenerable) |
| `scripts/engine_np.py` | numpy sort-merge candidate generator (+ `masks=` support) |
| `scripts/probe_volume.py` | DF-sensitivity pre-flight |
| `scripts/rehearsal_run.py` → `results/rehearsal_100k_stats.json` | the run (3rd arg `mask` = masked rerun) |
| `scripts/fix_flanks.py` | equal-length flank recompute |
| `scripts/rehearsal_map.py` → `results/rehearsal_100k_map.md`, `_clusters.csv` | the map (3rd arg = pairs table) |
| `scripts/build_rehearsal_atlas.py` → `review/rehearsal_100k_atlas.html` | the atlas |
| `scripts/build_reuse_graph.py` → `review/rehearsal_100k{,_masked}_graph.html` | interactive graph |
| `scripts/track1_build_ref.py` → `data/ref_corpus.pkl` | Maagarim+JA reference (gitignored) |
| `scripts/track1_match.py` → `track1_matches`, `results/track1_100k_report.md` | Track-1 identification |
| `results/rehearsal_100k_masked_map.md` | the masked map |
