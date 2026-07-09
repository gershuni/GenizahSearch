# C1-prep report: Phase-0 JABERT training dataset
Generated 2026-07-08 22:39:54 by phase0_build_dataset.py against `C:\Genizahsearch\same_work_spike\probe\data\fullcorpus.db`.
## 1. JA page pool
- Live Track-1 JA-cat rows (`shadowed_by IS NULL`, PRAGMA compat gate): distinct pages = **15,693**
- Distinct primary JA works: **85**
- Pages with >1 live JA match row (secondary matches folded into the primary-work pick): 667
## 2. Split assignment (work-granular, see module docstring #1)
Greedy largest-remaining-room bin-pack of the 85 JA works into train/dev/test by page count (target 80/10/10):

| split | pages | pct |
|---|---|---|
| train | 12,553 | 80.0% |
| dev | 1,570 | 10.0% |
| test | 1,570 | 10.0% |

## 3. Graded positives (accepted_pairs_canonmask, JA-JA)
- Raw JA-JA accepted pairs (both endpoints in pool): **20,336**
- Same-primary-work pairs: 17,743 (87.2%); cross-work pairs: 2,593 (12.8%)
- Dropped for split inconsistency (page_a/page_b's primary works landed in different splits -- only possible for cross-work pairs): **804**
- Distinct (work_a,work_b) combos after the split filter: 226
- Combo cap = 150/combo (documented rationale: top-20 combos are 70% of raw pairs); combos capped: 30
- **Graded positives after split-filter + combo-cap: 7,829**

- Pre-computed norm_stream for 15,693/15,693 pool pages.

## 4. Negative mining
- hard_neg rows built: 7,829 (anchors with no available same-work candidate: 0)
- easy_neg rows built: 7,829 (anchors where sampling failed after 8 retries: 0)

## 5. Split sizes (rows per split x kind)

| split | graded | hard_neg | easy_neg | total |
|---|---|---|---|---|
| train | 5,752 | 5,752 | 5,752 | 17,256 |
| dev | 1,007 | 1,007 | 1,007 | 3,021 |
| test | 1,070 | 1,070 | 1,070 | 3,210 |

**Grand total rows: 23,487** (positives 7,829, negatives 15,658, hard:easy ratio 7829:7829)

## 6. Span-length stats (characters, projected text)

| field | min | p25 | median | p75 | p90 | max | mean |
|---|---|---|---|---|---|---|---|
| text_a (anchor span) | 58 | 179 | 335 | 546 | 805 | 3247 | 410.8 |
| text_b (graded partner span) | 57 | 179 | 336 | 546 | 811 | 3330 | 410.3 |

## 7. Similarity label distribution (graded positives)

- n=7,829, min=0.072, p25=0.332, median=0.461, p75=0.606, max=1.000, mean=0.472

## 8. Provenance / supplement check

- Positive pool is **not** tiny (7,829 >> 500 threshold) -- liturgy.db supplement NOT used per brief step 6.
- For the record: liturgy.db has 3,439 live JA-cat track1_matches rows (no `shadowed_by` column at all in that copy -> PRAGMA compat gate treats all rows as live), available as a future supplement if needed.


## 9. README -- how to run train_phase0_jabert.py

Not executed as part of this spike (hard rule: no training run). This
section is the operating manual for whoever runs it once a GPU box is
confirmed.

### Install

```
pip install -U sentence-transformers datasets scipy scikit-learn torch
```
(sentence-transformers >=3.0 for the SentenceTransformerTrainer/datasets
API the script uses; verified against sentence-transformers 5.7.0.dev0
source read 2026-07-08. numpy/scipy are already present in this repo's
env; datasets/scikit-learn/torch/sentence-transformers are NOT installed
here and were never imported for real -- the script was syntax-checked
(`python -m py_compile`) and its pure-Python data-joining logic was dry-run
against the actual dev.jsonl (0/1007 graded rows missing a hard_neg join;
the overlap/wasserstein numpy+scipy math was smoke-tested on synthetic
well-separated vs identical distributions) but sentence-transformers'
model/Trainer code paths themselves were never invoked.)

### Model

Default `--model-path` resolution: the local JABERT checkpoint at
`C:\Users\gersh\Dropbox\JA Models\JABert_ckpt86010` (description.txt
inside says `JA_FinalModel`, matching the brief's naming) if that folder
exists on the machine running the script, else the HF hub id
`MiDRASH-ERC/JABERT`. It's a raw `BertForMaskedLM` checkpoint (bert-base
shape, vocab_size=128000, no sentence-transformers modules.json) --
`SentenceTransformer(path)` auto-wraps a plain HF encoder path with a
mean-pooling head, so no manual `models.Transformer`/`models.Pooling`
wiring is needed.

### GPU / CPU notes

GPU availability on this box is UNVERIFIED (this spike never imports
torch for real). `--device auto` (default) picks `cuda` if
`torch.cuda.is_available()`, else falls back to `cpu`. BERT-base x 12
layers over ~17.3K training rows (cosine) or ~5.75K triplets (margin_mse,
train split only) for a few epochs is a CPU-feasible-but-slow job (no
benchmarked wall-clock here -- don't trust a made-up number); a GPU (even
a modest one) is strongly preferred. `--fp16` is wired through to
`SentenceTransformerTrainingArguments` for when a CUDA GPU is available.
`--max-train-examples` caps the training set for a quick smoke run before
committing to a full pass.

### Commands

1. Off-the-shelf baseline (the number the fine-tune must beat):
   ```
   python -X utf8 -u train_phase0_jabert.py --baseline-only --tag baseline
   ```
   Writes `results/phase0_eval_baseline.json` with dev+test separation
   metrics for the UNTRAINED model.

2. Fine-tune, cosine variant (MiqraBERT-parity regression):
   ```
   python -X utf8 -u train_phase0_jabert.py --loss cosine --tag cosine_ft ^
       --compare-to ../results/phase0_eval_baseline.json
   ```

3. Fine-tune, MarginMSE variant (triplet, our upgrade):
   ```
   python -X utf8 -u train_phase0_jabert.py --loss margin_mse --tag margin_ft ^
       --compare-to ../results/phase0_eval_baseline.json
   ```
   (`^` is the PowerShell/cmd line-continuation; use `\` on a POSIX shell.)

Each fine-tuning run saves the trained model to
`../data/phase0/model_<loss>/` and its own
`results/phase0_eval_<tag>.json`, then (if `--compare-to` is given) prints
a per-split PASS/FAIL against the baseline JSON.

### Phase 0 green-light gate

Fine-tuned separation on dev must beat the off-the-shelf JABERT baseline
on the SAME dev split. Operationalized in `compare_to_baseline()` as all
three of: AUC increases, Wasserstein distance increases, and overlap
coefficient decreases, versus the baseline JSON for that split. AUC is
the headline number; Wasserstein/overlap are corroborating (the brief's
"distribution separation, not top-k" framing -- there is no fixed
query/corpus retrieval task here, just graded pairwise reuse). If the
three disagree (e.g. AUC up but overlap also up), that is itself a
finding worth writing up, not just a PASS/FAIL.

### Design deviations already documented in the module docstring

See phase0_build_dataset.py's docstring (points 1-3) for: why splits are
work-granular rather than page-hash or connected-components; why the
per-(work_a,work_b) 150-pair cap exists; and why hard/easy negatives reuse
the anchor's EXACT matched span rather than a fresh window on both sides
(load-bearing for the MarginMSE triplet's shared anchor). train_phase0_jabert.py's
own docstring documents the MarginMSE-specific deviations (own density
label substituting for a teacher score; cosine not dot-product
similarity_fct; easy_neg unused in margin_mse training).

## 10. Open issues / honest caveats

- **No teacher model for MarginMSE.** True MarginMSELoss distills a
  cross-encoder teacher's score gap; we have none, so the target margin is
  our own Track-2 density-derived label (positive) minus 0.0 (negative).
  This is a reasonable proxy but an unvalidated one -- flag if margin_mse
  underperforms cosine and this substitution is the first suspect.
- **max_seq_length=320 is a guess**, not tuned against the JABERT
  tokenizer's actual subword rate on this JA corpus (p90 span length is
  ~810 characters, p99 unmeasured tail exists too -- see section 6).
  Should be checked with the real tokenizer before a real run.
- **Combo cap (150) and D_NORM (0.45) are v0 choices**, not swept. If
  Phase 0 results look off, these are the first two knobs to revisit
  (raising the cap trades diversity for volume; D_NORM shifts every
  graded label's magnitude uniformly, so it mostly matters for cosine
  loss's absolute regression target, less for margin_mse's relative
  ordering).
- **easy_neg candidates are drawn from the whole split-restricted JA
  pool without an explicit unrelatedness check beyond "different primary
  work"** -- two different JA works can still share substantial
  vocabulary/register (see section 3's cross-work combo list, e.g. the
  Rasag Targum/Perush companion pairs); an easy_neg could occasionally
  land on a companion work and be less "easy" than intended. Not
  corrected here; would show up as elevated easy_neg overlap in eval if
  it's a real problem.
- **12.8% of raw JA-JA positives are cross-work**, and 804 of those
  (3.95% of the raw 20,336) were dropped outright for landing in
  different splits under the work-granular assignment (section 3) --
  documented, not fixed; a future v1 could special-case the ~15 known
  companion-work links (Targum/Perush pairs etc.) into shared split
  buckets without triggering the giant-component collapse a full
  union-find hits (see module docstring point 1).
- **train_phase0_jabert.py has never actually imported
  sentence-transformers/torch/datasets/sklearn** in this environment (none
  are installed) -- only py_compile + a pure-Python dry run of the
  data-joining and numpy/scipy metric math (see README install note). A
  real first run may surface API drift even against the freshly-fetched
  5.7.0.dev0 source (e.g. SentenceTransformerTrainingArguments accepting
  all the kwargs used here) -- it's dev/pre-release, and the eventual
  training box may have an older pinned version instead.

Total wall time: 10.6s
