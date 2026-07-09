# -*- coding: utf-8 -*-
"""SEED-029 C1-prep: Phase-0 JABERT semantic-reuse fine-tune script.

NOT executed as part of the C1-prep spike -- the deliverable is the dataset
(phase0_build_dataset.py -> ../data/phase0/{train,dev,test}.jsonl) plus this
script plus a README. GPU availability on this box is unverified; the
actual training run is deferred to whenever that's resolved. This file has
only been syntax-checked (`python -m py_compile`), never run.

Requires (not installed in the spike environment): sentence-transformers
(>=3.0, for the SentenceTransformerTrainer / datasets.Dataset API used
below -- verified against sentence-transformers 5.7.0.dev0 source on
2026-07-08), datasets, scipy, scikit-learn, torch.
    pip install -U sentence-transformers datasets scipy scikit-learn torch

Two loss variants (--loss):
  cosine     - CosineSimilarityLoss over ALL rows (graded + hard_neg +
               easy_neg) as independent (text_a, text_b, label) pairs.
               MiqraBERT-parity: continuous regression over the Track-2
               density-derived similarity label in [0,1] (negatives pinned
               at 0.0). Dataset columns: sentence1, sentence2, score.
  margin_mse - MarginMSELoss over (anchor, positive, hard_negative)
               triplets. Each graded row is joined to its 1:1 hard_neg row
               via meta.pair_id (phase0_build_dataset.py mines exactly one
               hard_neg per graded row, anchored at the SAME page_a span,
               so this join is total by construction -- see
               ../results/c1_prep_report.md, hard_neg_missing=0).
               Target margin = graded_label - 0.0 = graded_label itself:
               we substitute our own Track-2 density-derived similarity for
               the cross-encoder teacher score MarginMSE was designed
               around (there is no teacher model in this project). Uses
               COSINE similarity (util.pairwise_cos_sim), not the loss's
               default dot-product, so the learned margin is directly
               comparable to the cosine-based eval below and to the cosine
               loss variant. Dataset columns: query, passage1, passage2,
               label.
  easy_neg rows are NOT used by margin_mse training (only graded+hard_neg
  participate in triplets); they still appear in eval (see below).

Eval (phase0_eval_callback, run on dev+test before/after training): embeds
every text_a/text_b, takes the per-row cosine similarity, splits it into a
POSITIVE distribution (graded rows) and a NEGATIVE distribution
(hard_neg + easy_neg rows), and reports the three metrics that are this
project's PRIMARY evaluation criterion -- distribution separation, not
top-k retrieval (there is no fixed query/corpus retrieval task here):
  - Wasserstein-1 distance between the positive and negative cosine dists
  - overlap coefficient (shared-histogram-mass, [0,1], LOWER = more
    separated)
  - AUC of cosine similarity as a positive/negative classifier
A hard_neg-only and easy_neg-only breakdown is included too (hard_neg
should be the harder discrimination problem).

--baseline-only skips all training and evaluates the model exactly as
loaded (off-the-shelf JABERT, no fine-tuning) -- this produces the number
the fine-tuned run must beat (see README "Phase 0 green-light gate").
--compare-to lets a later fine-tuned run diff itself against a saved
baseline (or another run's) results JSON for an automatic PASS/FAIL read
of that gate.
"""
import argparse
import json
import os
import time

import numpy as np

# ROOT is derived from this file's location (probe/scripts/ -> probe/) so the
# script is portable across Windows and WSL/Linux (the 2026-07-09 GPU box is
# WSL; the Windows C:\... paths only resolve under the Git-Bash/Windows Python).
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR_DEFAULT = os.path.join(ROOT, 'data', 'phase0')
RESULTS_DIR = os.path.join(ROOT, 'results')
# local JABERT checkpoint: Windows path if present, else its WSL /mnt/c mount,
# else the HF hub id (resolve_model_path falls through in that order).
LOCAL_MODEL_DEFAULT = r"C:\Users\gersh\Dropbox\JA Models\JABert_ckpt86010"
LOCAL_MODEL_DEFAULT_WSL = "/mnt/c/Users/gersh/Dropbox/JA Models/JABert_ckpt86010"
HF_MODEL_FALLBACK = "MiDRASH-ERC/JABERT"


# --------------------------------------------------------------- resolution

def resolve_model_path(explicit):
    """--model-path override, else the local JABERT (JA_FinalModel)
    checkpoint if the Dropbox folder is present on this machine, else the
    HF hub id. The local dir is a raw BertForMaskedLM checkpoint (config.json
    architectures=["BertForMaskedLM"], vocab_size=128000, no sentence-
    transformers modules.json) -- SentenceTransformer(path) auto-wraps a
    plain HF encoder path with a mean-pooling head, so no manual
    models.Transformer/models.Pooling wiring is needed here."""
    if explicit:
        return explicit
    if os.path.isdir(LOCAL_MODEL_DEFAULT):
        return LOCAL_MODEL_DEFAULT
    if os.path.isdir(LOCAL_MODEL_DEFAULT_WSL):
        return LOCAL_MODEL_DEFAULT_WSL
    return HF_MODEL_FALLBACK


def resolve_device(explicit):
    if explicit and explicit != 'auto':
        return explicit
    import torch
    return 'cuda' if torch.cuda.is_available() else 'cpu'


# --------------------------------------------------------------- data glue

def read_jsonl(path):
    rows = []
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def split_by_kind(rows):
    by_kind = {'graded': [], 'hard_neg': [], 'easy_neg': []}
    for r in rows:
        by_kind[r['meta']['kind']].append(r)
    return by_kind


def build_cosine_dataset(rows, max_examples=None):
    from datasets import Dataset
    if max_examples:
        rows = rows[:max_examples]
    return Dataset.from_dict({
        'sentence1': [r['text_a'] for r in rows],
        'sentence2': [r['text_b'] for r in rows],
        'score': [float(r['label']) for r in rows],
    })


def build_margin_mse_dataset(rows, max_examples=None):
    """(query, passage1, passage2) triplets, one per graded row, joined to
    its hard_neg row by meta.pair_id. label = graded.label - 0.0 (see
    module docstring for why 0.0 -- hard_neg's own pinned label)."""
    from datasets import Dataset
    by_kind = split_by_kind(rows)
    hard_by_pair = {r['meta']['pair_id']: r for r in by_kind['hard_neg']}
    queries, pos, neg, labels = [], [], [], []
    missing = 0
    for g in by_kind['graded']:
        h = hard_by_pair.get(g['meta']['pair_id'])
        if h is None:
            missing += 1
            continue
        queries.append(g['text_a'])
        pos.append(g['text_b'])
        neg.append(h['text_b'])
        labels.append(float(g['label']))
    if missing:
        print(f"[phase0] margin_mse: {missing} graded rows had no "
              f"matching hard_neg (pair_id) -- skipped")
    if max_examples:
        queries, pos, neg, labels = (queries[:max_examples], pos[:max_examples],
                                      neg[:max_examples], labels[:max_examples])
    return Dataset.from_dict({'query': queries, 'passage1': pos,
                               'passage2': neg, 'label': labels})


def load_st_model(model_path, max_seq_length=None, device=None):
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(model_path, device=device)
    if max_seq_length:
        model.max_seq_length = max_seq_length
    return model


# ---------------------------------------------------------------- metrics

def overlap_coefficient(pos, neg, bins=50, value_range=(-1.0, 1.0)):
    """Shared-histogram-mass overlap coefficient in [0,1]. Both
    distributions are binned on the SAME fixed grid (cosine sim is always
    in [-1,1]) so the comparison is well-defined even with very different
    sample sizes. Lower = more separated."""
    hp, _ = np.histogram(pos, bins=bins, range=value_range)
    hn, _ = np.histogram(neg, bins=bins, range=value_range)
    hp = hp / max(1, hp.sum())
    hn = hn / max(1, hn.sum())
    return float(np.minimum(hp, hn).sum())


def compute_separation_metrics(pos_sims, neg_sims):
    from scipy.stats import wasserstein_distance
    from sklearn.metrics import roc_auc_score
    pos_sims = np.asarray(pos_sims, dtype=float)
    neg_sims = np.asarray(neg_sims, dtype=float)
    out = {
        'n_pos': int(len(pos_sims)), 'n_neg': int(len(neg_sims)),
        'pos_mean': float(pos_sims.mean()) if len(pos_sims) else None,
        'neg_mean': float(neg_sims.mean()) if len(neg_sims) else None,
        'pos_std': float(pos_sims.std()) if len(pos_sims) else None,
        'neg_std': float(neg_sims.std()) if len(neg_sims) else None,
    }
    if len(pos_sims) and len(neg_sims):
        out['wasserstein'] = float(wasserstein_distance(pos_sims, neg_sims))
        out['overlap_coefficient'] = overlap_coefficient(pos_sims, neg_sims)
        y = np.concatenate([np.ones_like(pos_sims), np.zeros_like(neg_sims)])
        scores = np.concatenate([pos_sims, neg_sims])
        out['auc'] = (float(roc_auc_score(y, scores))
                       if len(set(y.tolist())) > 1 else None)
    else:
        out['wasserstein'] = out['overlap_coefficient'] = out['auc'] = None
    return out


def phase0_eval_callback(model, rows, batch_size=64, show_progress=False):
    """The project's primary eval metric set. Embeds text_a/text_b for
    every row, computes per-row cosine similarity, buckets into the
    positive (graded) vs negative (hard_neg+easy_neg) distributions, and
    returns wasserstein/overlap/AUC for the combined negative pool plus a
    hard_neg-only / easy_neg-only breakdown for diagnosis."""
    by_kind = split_by_kind(rows)
    all_a = [r['text_a'] for r in rows]
    all_b = [r['text_b'] for r in rows]
    emb_a = model.encode(all_a, batch_size=batch_size,
                          show_progress_bar=show_progress,
                          convert_to_numpy=True)
    emb_b = model.encode(all_b, batch_size=batch_size,
                          show_progress_bar=show_progress,
                          convert_to_numpy=True)
    num = (emb_a * emb_b).sum(axis=1)
    denom = np.linalg.norm(emb_a, axis=1) * np.linalg.norm(emb_b, axis=1)
    cos = num / np.clip(denom, 1e-9, None)

    sims_by_kind = {'graded': [], 'hard_neg': [], 'easy_neg': []}
    for r, c in zip(rows, cos):
        sims_by_kind[r['meta']['kind']].append(float(c))

    pos = sims_by_kind['graded']
    neg_all = sims_by_kind['hard_neg'] + sims_by_kind['easy_neg']
    out = compute_separation_metrics(pos, neg_all)
    out['hard_neg_only'] = compute_separation_metrics(pos, sims_by_kind['hard_neg'])
    out['easy_neg_only'] = compute_separation_metrics(pos, sims_by_kind['easy_neg'])
    return out


def save_results(results, tag):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    out_path = os.path.join(RESULTS_DIR, f'phase0_eval_{tag}.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"[phase0] wrote {out_path}")
    for split, m in results.items():
        if isinstance(m, dict) and 'auc' in m:
            print(f"  {split}: AUC={_fmt(m['auc'])} "
                  f"wasserstein={_fmt(m['wasserstein'])} "
                  f"overlap={_fmt(m['overlap_coefficient'])} "
                  f"(pos_mean={_fmt(m['pos_mean'])} neg_mean={_fmt(m['neg_mean'])})")
    return out_path


def _fmt(x):
    return f"{x:.4f}" if isinstance(x, (int, float)) else str(x)


def compare_to_baseline(results, baseline_path):
    """Gate check: for each split shared with the baseline JSON, print
    whether AUC improved, wasserstein grew, and overlap shrank -- the three
    corroborating reads of "beat the off-the-shelf baseline" (README
    "Phase 0 green-light gate")."""
    with open(baseline_path, encoding='utf-8') as f:
        baseline = json.load(f)
    print(f"\n[phase0] gate check vs baseline: {baseline_path}")
    for split, m in results.items():
        if not (isinstance(m, dict) and 'auc' in m and split in baseline):
            continue
        b = baseline[split]
        if not (isinstance(b, dict) and 'auc' in b):
            continue
        d_auc = m['auc'] - b['auc'] if m['auc'] is not None and b['auc'] is not None else None
        d_was = (m['wasserstein'] - b['wasserstein']
                 if m['wasserstein'] is not None and b['wasserstein'] is not None else None)
        d_ovl = (m['overlap_coefficient'] - b['overlap_coefficient']
                 if m['overlap_coefficient'] is not None and b['overlap_coefficient'] is not None
                 else None)
        gate_pass = (d_auc is not None and d_auc > 0
                     and d_was is not None and d_was > 0
                     and d_ovl is not None and d_ovl < 0)
        print(f"  {split}: dAUC={_fmt(d_auc)} dWasserstein={_fmt(d_was)} "
              f"dOverlap={_fmt(d_ovl)} -> {'PASS' if gate_pass else 'FAIL/AMBIGUOUS'}")


# --------------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--data-dir', default=DATA_DIR_DEFAULT)
    ap.add_argument('--model-path', default=None,
                     help='local dir or HF hub id; default: local JABERT '
                          'if present, else MiDRASH-ERC/JABERT')
    ap.add_argument('--loss', choices=['cosine', 'margin_mse'], default='cosine')
    ap.add_argument('--baseline-only', action='store_true',
                     help='skip training; evaluate the model as-loaded')
    ap.add_argument('--output-dir', default=None,
                     help='fine-tuned model save dir; default '
                          '../data/phase0/model_<loss>')
    ap.add_argument('--epochs', type=int, default=3)
    ap.add_argument('--batch-size', type=int, default=16)
    ap.add_argument('--eval-batch-size', type=int, default=64)
    ap.add_argument('--lr', type=float, default=2e-5)
    ap.add_argument('--warmup-ratio', type=float, default=0.1)
    ap.add_argument('--max-seq-length', type=int, default=320,
                     help='covers most graded spans (median ~336 chars, '
                          'p90 ~810); raise if truncation looks bad')
    ap.add_argument('--fp16', action='store_true')
    ap.add_argument('--device', default='auto', help='auto | cpu | cuda')
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--eval-splits', default='dev,test')
    ap.add_argument('--max-train-examples', type=int, default=None,
                     help='debug: cap the training set size')
    ap.add_argument('--tag', default=None,
                     help='results/phase0_eval_<tag>.json filename stem; '
                          'default "baseline" or the loss name')
    ap.add_argument('--compare-to', default=None,
                     help='path to another phase0_eval_*.json to gate-check against')
    args = ap.parse_args()

    device = resolve_device(args.device)
    model_path = resolve_model_path(args.model_path)
    tag = args.tag or ('baseline' if args.baseline_only else args.loss)
    print(f"[phase0] model={model_path} device={device} loss={args.loss} "
          f"baseline_only={args.baseline_only} tag={tag}")

    import torch
    torch.manual_seed(args.seed)

    model = load_st_model(model_path, max_seq_length=args.max_seq_length, device=device)

    eval_splits = [s.strip() for s in args.eval_splits.split(',') if s.strip()]
    eval_rows = {s: read_jsonl(os.path.join(args.data_dir, f'{s}.jsonl'))
                 for s in eval_splits}

    if args.baseline_only:
        results = {}
        for s, rows in eval_rows.items():
            t0 = time.time()
            print(f"[phase0] evaluating baseline on {s} ({len(rows)} rows)...")
            results[s] = phase0_eval_callback(model, rows, batch_size=args.eval_batch_size)
            print(f"  ({time.time() - t0:.1f}s)")
        out_path = save_results(results, tag)
        if args.compare_to:
            compare_to_baseline(results, args.compare_to)
        return out_path

    # ---- training ----
    train_rows = read_jsonl(os.path.join(args.data_dir, 'train.jsonl'))
    print(f"[phase0] train rows available: {len(train_rows)}")

    from sentence_transformers import losses
    from sentence_transformers import SentenceTransformerTrainer, SentenceTransformerTrainingArguments
    from sentence_transformers import util as st_util

    if args.loss == 'cosine':
        train_ds = build_cosine_dataset(train_rows, args.max_train_examples)
        loss = losses.CosineSimilarityLoss(model)
    else:
        train_ds = build_margin_mse_dataset(train_rows, args.max_train_examples)
        # Explicit cosine similarity (not the loss's dot-product default) so
        # the learned margin matches the cosine-based eval below.
        loss = losses.MarginMSELoss(model, similarity_fct=st_util.pairwise_cos_sim)
    print(f"[phase0] training examples after dataset construction: {len(train_ds)}")

    output_dir = args.output_dir or os.path.join(
        ROOT, 'data', 'phase0', f'model_{args.loss}')
    train_args = SentenceTransformerTrainingArguments(
        output_dir=output_dir,
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch_size,
        learning_rate=args.lr,
        warmup_ratio=args.warmup_ratio,
        fp16=args.fp16,
        seed=args.seed,
        save_strategy='epoch',
        logging_steps=50,
        report_to=[],
    )
    trainer = SentenceTransformerTrainer(model=model, args=train_args,
                                          train_dataset=train_ds, loss=loss)

    t0 = time.time()
    trainer.train()
    print(f"[phase0] training done in {time.time() - t0:.1f}s")
    model.save(output_dir)
    print(f"[phase0] model saved -> {output_dir}")

    results = {}
    for s, rows in eval_rows.items():
        print(f"[phase0] evaluating fine-tuned model on {s} ({len(rows)} rows)...")
        results[s] = phase0_eval_callback(model, rows, batch_size=args.eval_batch_size)
    out_path = save_results(results, tag)
    if args.compare_to:
        compare_to_baseline(results, args.compare_to)
    return out_path


if __name__ == '__main__':
    main()
