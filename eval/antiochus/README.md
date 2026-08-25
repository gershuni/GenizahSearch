# Megillat Antiochus — adjudicated recall benchmark

The measuring instrument behind every recall and precision figure in
[`docs/specs/passage-matching-algorithm.md`](../../docs/specs/passage-matching-algorithm.md)
sections 8.1 and 10.4. Built 2026-08-24 while evaluating the letter-level passage engine
against word-chunk matching on the Scroll of Antiochus.

It is committed because the numbers in that spec are worthless without it. "This policy
reaches 72% recall" is not a checkable statement unless the deck it was measured against
still exists, and a rebuilt deck would not be comparable to the recorded figures.

## Files

| file | what it is |
|---|---|
| `deck.json` | 1767 manuscripts, each with a verdict and the reason for it |
| `aliases.json` | 5 hand-verified shelfmark equivalences no rule can derive, plus 1 rejected |
| `query.source.txt` | the query text itself — the Aramaic scroll, verse-numbered |
| `query.normalized.txt` | its 5,979-letter normalized stream (repaired 2026-08-24: the first commit wrote 400 chars of cp1255 mojibake that normalized to zero letters, so every run scored against it came back empty) |
| `runs/*.json` | the five graded runs, by manuscript (rank, shelfmark, score, library) |

Scored by [`scripts/score_antiochus_deck.py`](../../scripts/score_antiochus_deck.py);
joined by [`scripts/shelfmark_join.py`](../../scripts/shelfmark_join.py).

## Using it

```bash
# reproduce the recorded table from the archived runs
python scripts/score_antiochus_deck.py --all

# score a new run — GUI xlsx export, delta CSV, or JSON rows
python scripts/score_antiochus_deck.py --run my_run.xlsx --show-missed
```

To generate a new letter-engine run to score, use
`scripts/compare_passage_policies.py --csv` with `query.normalized.txt` (re-normalizing an
already-normalized stream is idempotent, so it can be fed directly).

## The verdicts

| verdict | meaning | count |
|---|---|---|
| `WITNESS` | carries the scroll's text, or is catalogued as it | 69 |
| `INDIRECT` | quotes or cites it verbatim without being a copy of it | 14 |
| `NOISE` | everything else | 1684 |

**83 positives** = WITNESS + INDIRECT. Every recall figure below is out of 83.

## The recorded table

Reproduced by `--all`; these are the numbers the spec quotes.

| run | manuscripts | precision | recall | frontier |
|---|---|---|---|---|
| `chunks-linebreaks` | 43 | 53% | 28% | 0/20 |
| `letters-widest-40` | 56 | **100%** | 67% | 0/20 |
| `letters-max-40` | 67 | 90% | 72% | 0/20 |
| `chunks-3` | 297 | 18% | 64% | 0/20 |
| `chunks-2-filtered` | 1727 | 5% | **98%** | **20/20** |

Two later letter-engine runs were measured against this deck but are not archived here,
because they are regenerable from the index with a policy name:

| run | manuscripts | precision | recall | frontier |
|---|---|---|---|---|
| `widest-40` + `short` | 104 | 61% | 72% | 1/20 |
| `max-40` + `short` | 189 | 40% | 76% | 1/20 |

`widest-40 + short` is the interesting one: it surfaced **MS heb. e.45/36**, catalogued
מגילת אנטיוכוס, which *no* method had returned before — chunk-2 included. That manuscript
is therefore **not in this deck**, and a run that finds it will report it as UNGRADED.

## The frontier — what recall work is actually about

Twenty positives have only ever been returned by word-chunk matching at chunk size 2.
Letter-level search has never reached more than one:

```
ENA 1629.10          L-G Ar.II.151        L-G Ar.II.152        MS heb. d.37/71
MS heb. d.60/25      MS heb. e.30/56      MS heb. e.45/34      MS heb. f.18/35
MS heb. f.40/47      Ms. 10808.8          Ms. C 24             Ms. EVR ARAB I 4838
Ms. EVR II A 1225    Ms. EVR II A 922     Ms. G.F. vol. 2      Ms. VII C 12
T-S AS 171.65        T-S AS 67.25         T-S AS 72.94         T-S Ar.24.174
```

Most are Judeo-Arabic translations. Spec section 10.4 explains why the letter engine cannot
reach them and why a better threshold will not change that: an Arabic translation shares
essentially nothing with an Aramaic query beyond a few transliterated proper names. A tier
built to chase exactly this class was measured twice and failed both times (4% then 1%
precision) — read 10.4 before rebuilding it.

Progress on these twenty is the point. Progress on the other 63 is mostly re-finding what
`widest-40` already finds at 100% precision.

## UNGRADED is not noise

The scorer reports returned manuscripts absent from the deck as `UNGRADED`, separately from
`NOISE`, because they are one of two very different things:

* a **genuine new find** — the deck is the union of five runs, so a better method can exceed
  it (MS heb. e.45/36 is exactly this); or
* a **broken join** — the same manuscript written two ways, not colliding.

A join break looks like brilliant novelty and catastrophic recall *at the same time*. That
combination produced two wrong measurements in one session: the first reported 22% recall
for a policy whose real figure was 67%, with 37 rows called "ungraded" that were the deck's
own positives under other names. Treat any sizeable ungraded count as a join bug until
proven otherwise, and settle it with `--show-ungraded`.

## Known limits — read before trusting a number

1. **One query.** Everything here is Megillat Antiochus. The `short` profile's operating
   point (28, 12) is fitted to this text and has not been shown to generalize. A second
   graded query is the single highest-value addition to this benchmark.
2. **The deck is a union of runs, not the corpus.** It cannot prove a manuscript is absent
   from the corpus, only that no graded run found it. Recall is therefore *recall against
   what five methods collectively found*, an upper-bounded proxy for true recall.
3. **Grading was partly by sampling.** The 1402 chunks-2-only NOISE verdicts came from a
   marker scan plus a top-20 and 25-item sampled review, not an item-by-item reading. Each
   verdict carries its own basis in `reason`; check it before leaning on a single row.
4. **Verdicts were assigned by LLM adjudication** (Sonnet subagents reading catalogue titles
   and matched text), not by the owner. They were spot-checked, not audited. The WITNESS
   entries catalogued as מגילת אנטיוכוס are solid; borderline INDIRECT calls are the soft
   ones.
5. **`query.normalized.txt` is the normalized stream**, not the source text. Letters only,
   finals folded, spaces stripped — sufficient to re-run the engine, not to read.
