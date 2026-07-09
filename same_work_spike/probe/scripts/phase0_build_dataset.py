# -*- coding: utf-8 -*-
"""SEED-029 spike C1-prep: Phase-0 JABERT semantic-reuse training dataset.

Builds a JA-domain (Judeo-Arabic) semantic text-reuse dataset from
../data/fullcorpus.db for fine-tuning a sentence-transformers JABERT model
(see train_phase0_jabert.py). Three label classes, sharing one anchor span
per graded positive so downstream training can build (anchor, positive,
hard_negative) triplets for a MarginMSE loss as well as plain pairwise rows
for a CosineSimilarityLoss (MiqraBERT-parity) loss:

  graded   - accepted Track-2 pairs (both pages live Track-1 JA witnesses),
             density converted to a similarity label in [0,1].
  hard_neg - anchor page paired with ANOTHER page of the SAME primary JA
             work that is NOT in the accepted-pairs set (same vocabulary /
             style / topic, different passage of the work).
  easy_neg - anchor page paired with a random JA-pool page of a DIFFERENT
             primary work, also not accepted-paired.

No model code runs here -- this is SQLite reads + string slicing only.
Run: python -X utf8 -u phase0_build_dataset.py   (cwd = probe/scripts)

Design notes / deliberate deviations from the v0 brief (documented per the
brief's "adjust only with documented reasons" rule):

1. Split granularity is the JA WORK (primary work per page), not the raw
   page_id, and NOT full connected-components over cross-work pair links.
   Union-find over the 280 distinct cross-work pair links collapses 71/85
   works (15,239/15,693 pages, 97%) into a single component -- unusable for
   an 80/10/10 split. Raw independent per-page hashing would instead throw
   away ~34% of WITHIN-work positives purely by chance (same-work pages
   land in different splits ~34% of the time under independent hashing).
   Splitting by primary work keeps ~100% of same-work positives together
   (87% of the raw candidate pairs) and still guarantees the invariant that
   matters -- no single page's text is ever emitted into two splits, because
   split = f(page) = f(primary_work(page)) is a deterministic function.
   Cross-work positives (13%) are kept only if both works landed in the same
   split by the greedy bin-pack below; the rest are dropped (counted).

2. Per-(work_a,work_b) combo cap. Raw JA-JA accepted pairs are extremely
   concentrated (top 20 of 357 work-combos = 70% of all pairs, e.g. Bahya
   ibn Paquda x itself = 2,619 pairs). A per-combo cap keeps the dataset
   from being dominated by a few blockbuster works while still leaving
   "a few thousand" positives spanning all 85 works.

3. Hard/easy negatives reuse the anchor's EXACT matched-span text (a0:a1 of
   page_a from the graded positive) rather than sampling a fresh window
   from the anchor page too. Only the non-anchor side gets a fresh,
   comparable-length window. This is a deliberate deviation from a literal
   "sample windows from each" reading: keeping one shared anchor text per
   pair_id is what makes the (anchor, positive, hard_neg) triplet coherent
   for the MarginMSE trainer -- if the anchor text itself varied between the
   positive and negative rows there would be no single anchor embedding to
   compare margins against.
"""
import json
import os
import random
import sqlite3
import statistics
import time
from collections import Counter, defaultdict

from normalize import norm_stream, project_span

ROOT = r"C:\Genizahsearch\same_work_spike\probe"
DB_PATH = ROOT + r"\data\fullcorpus.db"
LITURGY_DB_PATH = ROOT + r"\data\liturgy.db"
OUT_DIR = ROOT + r"\data\phase0"
REPORT_PATH = ROOT + r"\results\c1_prep_report.md"

RNG_SEED = 42
SPLIT_RATIOS = {'train': 0.8, 'dev': 0.1, 'test': 0.1}
COMBO_CAP = 150            # max graded pairs kept per (work_a,work_b) combo
D_NORM = 0.45              # density->similarity denominator (see build_label)
MIN_SPAN = 25              # floor window length, matches rehearsal_run.py
NEG_CANDIDATE_RETRIES = 8  # attempts to find a usable neg-candidate window

rng = random.Random(RNG_SEED)

# Static report sections (kept here, not hand-edited into the .md output, so
# a re-run of this script never silently drops them).
README_SECTION = """
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
`C:\\Users\\gersh\\Dropbox\\JA Models\\JABert_ckpt86010` (description.txt
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
   (`^` is the PowerShell/cmd line-continuation; use `\\` on a POSIX shell.)

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
"""

OPEN_ISSUES_SECTION = """
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
"""


# ---------------------------------------------------------------- utilities

def live_filter_sql(con, table):
    """PRAGMA compat gate (mirrors track1_testimonies.py:113-118): only
    filter on shadowed_by if the column exists (liturgy.db may predate it,
    or a fresh Track-1 rebuild may have dropped it before shadowing reran)."""
    cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})")]
    return "WHERE shadowed_by IS NULL" if 'shadowed_by' in cols else ""


def build_label(density):
    """density is a Track-2 edit-DISTANCE (lower = closer). Map to a
    similarity label in [0,1]: sim = 1 - density/D_NORM, clipped.
    D_NORM=0.45 sits just above the observed max accepted JA-JA density
    (0.4178) and the length>=200 acceptance ceiling accept_density() uses
    in rehearsal_run.py (0.418) -- i.e. the least-similar accepted pair
    still gets a small positive similarity (~0.07), not exactly 0."""
    sim = 1.0 - density / D_NORM
    return max(0.0, min(1.0, round(sim, 4)))


# --------------------------------------------------------------- DB loading

def load_ja_pool(con):
    """Distinct live JA-cat page_ids + primary work assignment per page.

    Primary work = the JA row with the most matched_letters (tie -> lowest
    best_density, tie -> lowest work_id) among that page's live JA rows.
    Verified up front (see report) that title/author are a stable function
    of work_id, so a single lookup table is safe.
    """
    live = live_filter_sql(con, 'track1_matches')
    where = f"{live} AND cat='JA'" if live else "WHERE cat='JA'"
    rows = con.execute(f"""
        SELECT page_id, work_id, title, author, matched_letters, best_density
        FROM track1_matches {where}
        ORDER BY page_id, matched_letters DESC, best_density ASC, work_id ASC
    """).fetchall()
    primary_work = {}
    work_title = {}
    n_multi_match_pages = 0
    seen_pages = set()
    for pid, wid, title, author, ml, bd in rows:
        if pid in seen_pages:
            n_multi_match_pages += 1
            continue
        seen_pages.add(pid)
        primary_work[pid] = wid
        work_title.setdefault(wid, (title, author))
    return primary_work, work_title, n_multi_match_pages


def assign_work_splits(primary_work):
    """Greedy largest-remaining-room bin-pack of works into train/dev/test
    by page count (see module docstring point 1 for why work-granularity)."""
    pages_per_work = Counter(primary_work.values())
    total = sum(pages_per_work.values())
    targets = {s: r * total for s, r in SPLIT_RATIOS.items()}
    filled = {s: 0 for s in SPLIT_RATIOS}
    work_split = {}
    order = sorted(pages_per_work.items(), key=lambda kv: (-kv[1], kv[0]))
    for wid, n in order:
        best = max(filled, key=lambda s: targets[s] - filled[s])
        work_split[wid] = best
        filled[best] += n
    return work_split, filled, pages_per_work


def load_ja_ja_accepted_pairs(con, pool):
    """All accepted_pairs_canonmask rows with both endpoints in the JA pool."""
    con.execute("DROP TABLE IF EXISTS temp.ja_pool")
    con.execute("CREATE TEMP TABLE ja_pool(page_id TEXT PRIMARY KEY)")
    con.executemany("INSERT INTO ja_pool VALUES (?)", [(p,) for p in pool])
    rows = con.execute("""
        SELECT ap.page_a, ap.page_b, ap.sys_a, ap.sys_b,
               ap.a0, ap.a1, ap.b0, ap.b1, ap.aligned_len, ap.density,
               ap.flank_class
        FROM accepted_pairs_canonmask ap
        JOIN ja_pool pa ON pa.page_id = ap.page_a
        JOIN ja_pool pb ON pb.page_id = ap.page_b
    """).fetchall()
    return rows


def load_streams(con, page_ids):
    """Pre-fetch text + norm_stream + offsets for every pool page. ~15.7K
    pages, median raw length ~1.2K chars -- cheap to hold all in memory."""
    streams = {}
    q = "SELECT page_id, text FROM pages WHERE page_id IN ({})".format(
        ','.join('?' * len(page_ids)))
    # SQLite has a variable-count ceiling (~32K default); pool is ~15.7K so
    # one IN(...) is fine, but chunk defensively.
    ids = list(page_ids)
    CHUNK = 5000
    for i in range(0, len(ids), CHUNK):
        chunk = ids[i:i + CHUNK]
        qc = "SELECT page_id, text FROM pages WHERE page_id IN ({})".format(
            ','.join('?' * len(chunk)))
        for pid, text in con.execute(qc, chunk):
            stream, offs = norm_stream(text)
            streams[pid] = (text, stream, offs)
    return streams


# ------------------------------------------------------------- span helpers

def page_sys(page_id):
    """sys_id is the leading component of page_id (verified against the
    sys_a/sys_b columns already present on accepted_pairs_canonmask rows)."""
    return page_id.split('_', 1)[0]


def span_text(streams, page_id, a, b):
    text, stream, offs = streams[page_id]
    a = max(0, min(a, len(offs)))
    b = max(a, min(b, len(offs)))
    return project_span(offs, a, b, text)


def sample_window(streams, page_id, target_len, rng_):
    """Random contiguous window of ~target_len letters from page_id's
    normalized stream, projected back to original text. Returns
    (proj_text, start, end) or None if the stream is too short."""
    _, stream, offs = streams[page_id]
    n = len(offs)
    if n < MIN_SPAN:
        return None
    L = max(MIN_SPAN, min(target_len, n))
    start = rng_.randint(0, n - L)
    end = start + L
    return span_text(streams, page_id, start, end), start, end


# ---------------------------------------------------------------- main flow

def main():
    t_start = time.time()
    os.makedirs(OUT_DIR, exist_ok=True)
    con = sqlite3.connect(DB_PATH)

    report = []
    report.append("# C1-prep report: Phase-0 JABERT training dataset\n")
    report.append(f"Generated {time.strftime('%Y-%m-%d %H:%M:%S')} "
                   f"by phase0_build_dataset.py against `{DB_PATH}`.\n")

    # ---- 1. JA pool ----
    primary_work, work_title, n_multi = load_ja_pool(con)
    pool = sorted(primary_work)
    report.append("## 1. JA page pool\n")
    report.append(f"- Live Track-1 JA-cat rows (`shadowed_by IS NULL`, "
                   f"PRAGMA compat gate): distinct pages = **{len(pool):,}**\n"
                   f"- Distinct primary JA works: **{len(set(primary_work.values())):,}**\n"
                   f"- Pages with >1 live JA match row "
                   f"(secondary matches folded into the primary-work pick): "
                   f"{n_multi:,}\n")

    # ---- 2. split assignment (by primary work) ----
    work_split, split_page_fill, pages_per_work = assign_work_splits(primary_work)
    page_split = {pid: work_split[w] for pid, w in primary_work.items()}
    report.append("## 2. Split assignment (work-granular, see module docstring #1)\n")
    report.append("Greedy largest-remaining-room bin-pack of the 85 JA works "
                   "into train/dev/test by page count (target 80/10/10):\n\n")
    report.append("| split | pages | pct |\n|---|---|---|\n")
    total_pages = sum(split_page_fill.values())
    for s in ('train', 'dev', 'test'):
        report.append(f"| {s} | {split_page_fill[s]:,} | "
                       f"{100 * split_page_fill[s] / total_pages:.1f}% |\n")
    report.append("\n")

    # ---- 3. accepted JA-JA pairs -> graded candidates ----
    rows = load_ja_ja_accepted_pairs(con, pool)
    report.append("## 3. Graded positives (accepted_pairs_canonmask, JA-JA)\n")
    report.append(f"- Raw JA-JA accepted pairs (both endpoints in pool): "
                   f"**{len(rows):,}**\n")

    same_work_n = 0
    diff_work_n = 0
    dropped_cross_split = 0
    combo_groups = defaultdict(list)
    acc_keys = set()
    for (pa, pb, sa, sb, a0, a1, b0, b1, alen, dens, fclass) in rows:
        acc_keys.add(frozenset((pa, pb)))
        wa, wb = primary_work[pa], primary_work[pb]
        if wa == wb:
            same_work_n += 1
        else:
            diff_work_n += 1
        sa_split, sb_split = page_split[pa], page_split[pb]
        if sa_split != sb_split:
            dropped_cross_split += 1
            continue
        combo = (wa, wb) if wa <= wb else (wb, wa)
        combo_groups[combo].append(
            (pa, pb, sa, sb, a0, a1, b0, b1, alen, dens, fclass, sa_split))

    report.append(f"- Same-primary-work pairs: {same_work_n:,} "
                   f"({100 * same_work_n / len(rows):.1f}%); "
                   f"cross-work pairs: {diff_work_n:,} "
                   f"({100 * diff_work_n / len(rows):.1f}%)\n")
    report.append(f"- Dropped for split inconsistency (page_a/page_b's "
                   f"primary works landed in different splits -- only "
                   f"possible for cross-work pairs): "
                   f"**{dropped_cross_split:,}**\n")
    report.append(f"- Distinct (work_a,work_b) combos after the split "
                   f"filter: {len(combo_groups):,}\n")

    # per-combo cap
    survivors = []
    n_capped_combos = 0
    for combo, items in combo_groups.items():
        if len(items) > COMBO_CAP:
            n_capped_combos += 1
            items_sorted = sorted(items, key=lambda r: (r[0], r[1]))
            rng.shuffle(items_sorted)
            items = items_sorted[:COMBO_CAP]
        survivors.extend(items)
    report.append(f"- Combo cap = {COMBO_CAP}/combo (documented rationale: "
                   f"top-20 combos are 70% of raw pairs); combos capped: "
                   f"{n_capped_combos:,}\n")
    report.append(f"- **Graded positives after split-filter + combo-cap: "
                   f"{len(survivors):,}**\n\n")

    # ---- 4. load text streams for everything we might touch ----
    streams = load_streams(con, pool)
    report.append(f"- Pre-computed norm_stream for {len(streams):,}/"
                   f"{len(pool):,} pool pages.\n\n")

    # pool pages grouped by (work, split) for negative sampling
    pages_by_work = defaultdict(list)
    for pid, w in primary_work.items():
        pages_by_work[w].append(pid)
    pages_by_split = defaultdict(list)
    for pid, s in page_split.items():
        pages_by_split[s].append(pid)

    # ---- 5. build graded rows + anchored hard/easy negatives ----
    graded_out = []
    hard_out = []
    easy_out = []
    sim_labels = []
    span_len_a, span_len_b = [], []
    hard_neg_missing = 0
    easy_neg_missing = 0

    survivors.sort(key=lambda r: (r[0], r[1]))  # determinism before rng use
    for pair_id, (pa, pb, sa, sb, a0, a1, b0, b1, alen, dens, fclass, split
                  ) in enumerate(survivors):
        wa, wb = primary_work[pa], primary_work[pb]
        title_a = work_title[wa][0]
        title_b = work_title[wb][0]
        text_a = span_text(streams, pa, a0, a1)
        text_b = span_text(streams, pb, b0, b1)
        if not text_a or not text_b:
            continue
        sim = build_label(dens)
        sim_labels.append(sim)
        span_len_a.append(len(text_a))
        span_len_b.append(len(text_b))

        meta = {
            'kind': 'graded', 'pair_id': pair_id,
            'page_a': pa, 'page_b': pb, 'sys_a': sa, 'sys_b': sb,
            'work_a': wa, 'work_b': wb, 'title_a': title_a, 'title_b': title_b,
            'density': dens, 'aligned_len': alen, 'flank_class': fclass,
            'a0': a0, 'a1': a1, 'b0': b0, 'b1': b1, 'split': split,
        }
        graded_out.append({'text_a': text_a, 'text_b': text_b,
                            'label': sim, 'meta': meta})

        neg_target_len = max(MIN_SPAN, b1 - b0)

        # -- hard negative: same primary work as page_a, not accepted-paired --
        candidates = [p for p in pages_by_work[wa]
                       if p != pa and p != pb
                       and frozenset((pa, p)) not in acc_keys]
        rng.shuffle(candidates)
        hn = None
        for cand in candidates[:NEG_CANDIDATE_RETRIES]:
            win = sample_window(streams, cand, neg_target_len, rng)
            if win:
                win_text, ws, we = win
                hn = (cand, win_text, ws, we)
                break
        if hn is None:
            hard_neg_missing += 1
        else:
            cand, win_text, ws, we = hn
            hard_out.append({
                'text_a': text_a, 'text_b': win_text, 'label': 0.0,
                'meta': {'kind': 'hard_neg', 'pair_id': pair_id,
                         'page_a': pa, 'page_b': cand, 'sys_a': sa,
                         'sys_b': page_sys(cand),
                         'work_a': wa, 'work_b': wa,
                         'title_a': title_a, 'title_b': title_a,
                         'a0': a0, 'a1': a1, 'b0': ws, 'b1': we,
                         'split': split},
            })

        # -- easy negative: same split, DIFFERENT primary work, not paired --
        en = None
        pool_candidates = pages_by_split[split]
        for _ in range(NEG_CANDIDATE_RETRIES):
            cand = rng.choice(pool_candidates)
            if cand == pa or cand == pb:
                continue
            if primary_work[cand] == wa:
                continue
            if frozenset((pa, cand)) in acc_keys:
                continue
            win = sample_window(streams, cand, neg_target_len, rng)
            if win:
                win_text, ws, we = win
                en = (cand, win_text, ws, we)
                break
        if en is None:
            easy_neg_missing += 1
        else:
            cand, win_text, ws, we = en
            wcand = primary_work[cand]
            tcand, _ = work_title[wcand]
            easy_out.append({
                'text_a': text_a, 'text_b': win_text, 'label': 0.0,
                'meta': {'kind': 'easy_neg', 'pair_id': pair_id,
                         'page_a': pa, 'page_b': cand, 'sys_a': sa,
                         'sys_b': page_sys(cand),
                         'work_a': wa, 'work_b': wcand,
                         'title_a': title_a, 'title_b': tcand,
                         'a0': a0, 'a1': a1, 'b0': ws, 'b1': we,
                         'split': split},
            })

    # ---- 6. write JSONL per split ----
    counts = defaultdict(lambda: Counter())
    for kind, rowset in (('graded', graded_out), ('hard_neg', hard_out),
                         ('easy_neg', easy_out)):
        for r in rowset:
            counts[r['meta']['split']][kind] += 1

    files = {s: open(os.path.join(OUT_DIR, f"{s}.jsonl"), 'w', encoding='utf-8')
              for s in ('train', 'dev', 'test')}
    try:
        for rowset in (graded_out, hard_out, easy_out):
            for r in rowset:
                f = files[r['meta']['split']]
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    finally:
        for f in files.values():
            f.close()

    # ---- 7. stats ----
    report.append("## 4. Negative mining\n")
    report.append(f"- hard_neg rows built: {len(hard_out):,} "
                   f"(anchors with no available same-work candidate: "
                   f"{hard_neg_missing:,})\n")
    report.append(f"- easy_neg rows built: {len(easy_out):,} "
                   f"(anchors where sampling failed after "
                   f"{NEG_CANDIDATE_RETRIES} retries: {easy_neg_missing:,})\n\n")

    report.append("## 5. Split sizes (rows per split x kind)\n\n")
    report.append("| split | graded | hard_neg | easy_neg | total |\n"
                   "|---|---|---|---|---|\n")
    grand_total = 0
    for s in ('train', 'dev', 'test'):
        g, h, e = counts[s]['graded'], counts[s]['hard_neg'], counts[s]['easy_neg']
        tot = g + h + e
        grand_total += tot
        report.append(f"| {s} | {g:,} | {h:,} | {e:,} | {tot:,} |\n")
    report.append(f"\n**Grand total rows: {grand_total:,}** "
                   f"(positives {len(graded_out):,}, negatives "
                   f"{len(hard_out) + len(easy_out):,}, "
                   f"hard:easy ratio {len(hard_out)}:{len(easy_out)})\n\n")

    def pct(lst, p):
        if not lst:
            return float('nan')
        s = sorted(lst)
        return s[min(int(len(s) * p), len(s) - 1)]

    report.append("## 6. Span-length stats (characters, projected text)\n\n")
    report.append("| field | min | p25 | median | p75 | p90 | max | mean |\n"
                   "|---|---|---|---|---|---|---|---|\n")
    for name, lst in (('text_a (anchor span)', span_len_a),
                       ('text_b (graded partner span)', span_len_b)):
        if lst:
            report.append(f"| {name} | {min(lst)} | {pct(lst, .25)} | "
                           f"{pct(lst, .5)} | {pct(lst, .75)} | "
                           f"{pct(lst, .9)} | {max(lst)} | "
                           f"{statistics.mean(lst):.1f} |\n")
    report.append("\n")

    report.append("## 7. Similarity label distribution (graded positives)\n\n")
    if sim_labels:
        report.append(f"- n={len(sim_labels):,}, min={min(sim_labels):.3f}, "
                       f"p25={pct(sim_labels, .25):.3f}, "
                       f"median={pct(sim_labels, .5):.3f}, "
                       f"p75={pct(sim_labels, .75):.3f}, "
                       f"max={max(sim_labels):.3f}, "
                       f"mean={statistics.mean(sim_labels):.3f}\n\n")

    report.append("## 8. Provenance / supplement check\n\n")
    live_lit = live_filter_sql(sqlite3.connect(LITURGY_DB_PATH), 'track1_matches')
    lcon = sqlite3.connect(LITURGY_DB_PATH)
    lit_ja = lcon.execute(
        f"SELECT COUNT(*) FROM track1_matches {live_lit} "
        f"{'AND' if live_lit else 'WHERE'} cat='JA'").fetchone()[0]
    report.append(f"- Positive pool is **not** tiny ({len(survivors):,} >> "
                   f"500 threshold) -- liturgy.db supplement NOT used per "
                   f"brief step 6.\n"
                   f"- For the record: liturgy.db has {lit_ja:,} live JA-cat "
                   f"track1_matches rows (no `shadowed_by` column at all in "
                   f"that copy -> PRAGMA compat gate treats all rows as "
                   f"live), available as a future supplement if needed.\n\n")

    report.append(README_SECTION)
    report.append(OPEN_ISSUES_SECTION)
    report.append(f"\nTotal wall time: {time.time() - t_start:.1f}s\n")

    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.writelines(report)

    print("JA pool:", len(pool), "pages,", len(set(primary_work.values())), "works")
    print("Graded positives:", len(graded_out))
    print("Hard negatives:", len(hard_out), "missing:", hard_neg_missing)
    print("Easy negatives:", len(easy_out), "missing:", easy_neg_missing)
    for s in ('train', 'dev', 'test'):
        print(f"  {s}: graded={counts[s]['graded']} hard={counts[s]['hard_neg']} "
              f"easy={counts[s]['easy_neg']}")
    print("Report ->", REPORT_PATH)
    print("JSONL ->", OUT_DIR)


if __name__ == '__main__':
    main()
