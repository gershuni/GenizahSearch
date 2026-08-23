# -*- coding: utf-8 -*-
"""The depth probe: how far down the OTHER method's list an exclusive find
actually sits, at both grains.

Codex review: "no reproducible depth-probe implementation is named or
frozen." C4 (complementarity, docs/specs/parallels-holdout-prereg.md) needs
the depth probe to be a committed, deterministic script rather than a
one-off used in the 2026-08-21 re-examination and never checked in.

Method. Take every GRADED card whose deck_key.json entry has exactly one
method (an "exclusive" find -- the other method's ranked list for that
query never surfaced this record at all, at least not within per_method_k).
Run that card's query through the OTHER method's retriever, built EXACTLY
as `scripts/eval_methods.py::build_retrievers` builds it (imported, not
reimplemented, so the probe can never quietly drift from what the eval
harness actually runs), with NO extra slicing -- the retriever's own full
returned list, under its own internal caps. Report, at both grains:

  page grain         is entry['record_id'] anywhere in that list, and at
                     what (0-based) rank?
  manuscript grain   is ANY record sharing entry['record_id']'s sys_id
                     prefix anywhere in that list, and at what best rank?

"ABSENT" (never "unretrievable"): passage has candidate/verification score
caps and chunk has a per-chunk hit cap, so a record's absence from the
returned list reflects those caps, not some hypothetical worst case beyond
them. Every ABSENT card carries an explicit note saying so, verbatim.

Determinism: cards are grouped by relation grade (alphabetical) and sorted
by card id within each grade; identical inputs give identical output bytes.

This script is SLOW to run for real -- the prereg's own estimate is ~13s per
chunk query -- so it is not exercised end-to-end by the test suite. Instead
`probe()`, the pure core (card selection, grain computation, report
shaping), is unit-tested against a STUBBED retriever
(tests/test_probe_exclusive_depth.py); only `run_probe()`/`main()` touch a
real index, via `--limit N` for a deliberately small smoke run.

Usage:
  python scripts/probe_exclusive_depth.py --deck-key deck_key.json \
      --verdicts export.json --queries fgp_queries.jsonl --index IDX \
      --configs 'passage:standard-40,chunk:3:exact:100' --out report.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts import eval_methods  # noqa: E402
from scripts import score_grading_deck as sgd  # noqa: E402

NOT_RETURNED_NOTE = (
    'not returned by this configured retriever under its internal caps')


def load_key(path: str) -> dict:
    with open(path, encoding='utf-8') as fh:
        key_list = json.load(fh)
    return {k['id']: k for k in key_list}


def load_verdicts(path: str) -> list:
    with open(path, encoding='utf-8') as fh:
        payload = json.load(fh)
    return payload.get('verdicts', payload)


def load_query_texts(path: str) -> dict:
    out = {}
    with open(path, encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            out[d['query_id']] = d['text']
    return out


def select_exclusive_graded(key: dict, verdicts: list,
                            valid_grades: frozenset) -> list:
    """Graded, EXCLUSIVE (single-method) cards, deduplicated and sorted.

    Returns a deterministic list of (card_id, grade) tuples, sorted by
    card_id. A verdict row for an id absent from the key, a grade outside
    `valid_grades`, or a duplicate id is silently EXCLUDED here (not fatal):
    this is a read-only diagnostic over an already-scored export, not the
    tamper/format gate -- that gate is scripts/score_grading_deck.py's job,
    and it already ran (or should have) before this probe does.
    """
    out = []
    seen = set()
    for v in verdicts:
        cid, g = v.get('id'), v.get('grade')
        if cid in seen or cid not in key or g not in valid_grades:
            continue
        entry = key[cid]
        if len(entry.get('methods') or []) != 1:
            continue
        seen.add(cid)
        out.append((cid, g))
    return sorted(out, key=lambda t: t[0])


def rank_or_absent(record_id: str, ranked_list: list):
    """0-based rank of `record_id` in `ranked_list`, or the string 'ABSENT'."""
    try:
        return ranked_list.index(record_id)
    except ValueError:
        return 'ABSENT'


def manuscript_rank_or_absent(sys_id: str, ranked_list: list):
    """Best (lowest) 0-based rank among records sharing `sys_id`, or 'ABSENT'."""
    best = None
    for i, rid in enumerate(ranked_list):
        if rid.split('_', 1)[0] == sys_id and (best is None or i < best):
            best = i
    return best if best is not None else 'ABSENT'


def probe_card(card_id: str, grade: str, key_entry: dict, other_spec: str,
              other_retriever, query_text: str) -> dict:
    """Depth-probe one exclusive card against the OTHER method's retriever.

    `other_retriever` need only implement `.retrieve(text) -> list[str]`
    (record ids in rank order) -- a real eval_methods retriever in
    production, a stub with a canned list in tests.
    """
    ranked = other_retriever.retrieve(query_text)
    record_id = key_entry['record_id']
    sys_id = record_id.split('_', 1)[0]
    page_rank = rank_or_absent(record_id, ranked)
    ms_rank = manuscript_rank_or_absent(sys_id, ranked)
    return {
        'card_id': card_id,
        'grade': grade,
        'query_id': key_entry['query_id'],
        'own_method': key_entry['methods'][0],
        'other_method': other_spec,
        'record_id': record_id,
        'sys_id': sys_id,
        'page_rank': page_rank,
        'page_note': NOT_RETURNED_NOTE if page_rank == 'ABSENT' else None,
        'manuscript_rank': ms_rank,
        'manuscript_note': (
            NOT_RETURNED_NOTE if ms_rank == 'ABSENT' else None),
    }


def build_report(cards: list) -> dict:
    """Deterministic aggregation by relation grade (alphabetical), cards
    sorted by card_id within each grade."""
    by_grade: dict = {}
    for c in cards:
        by_grade.setdefault(c['grade'], []).append(c)
    ordered = {}
    for g in sorted(by_grade):
        cs = sorted(by_grade[g], key=lambda c: c['card_id'])
        ordered[g] = {
            'n': len(cs),
            'page_absent': sum(1 for c in cs if c['page_rank'] == 'ABSENT'),
            'manuscript_absent': sum(
                1 for c in cs if c['manuscript_rank'] == 'ABSENT'),
            'cards': cs,
        }
    return {'n_exclusive_graded': len(cards), 'by_grade': ordered}


def probe(key: dict, verdicts: list, qtext: dict, specs: list,
         retrievers_by_spec: dict, limit: int = 0) -> dict:
    """The pure core: selection -> per-card depth probe -> report.

    Takes already-built retrievers (`retrievers_by_spec`, one per entry in
    `specs`) rather than constructing them, so this function never needs a
    real index and is exactly what the unit tests call with stubs.

    Fatal (SystemExit), never a silent skip or a guess: `specs` not a pair;
    a card's own method not among `specs` (the probe configs must match the
    deck that produced the key); a card's query_id absent from `qtext`.
    """
    if len(specs) != 2:
        raise SystemExit(
            f'depth probe is pairwise: --configs must name exactly 2 '
            f'methods, got {len(specs)}: {specs}')
    exclusive = select_exclusive_graded(key, verdicts, sgd.ALL_GRADES)
    if limit:
        exclusive = exclusive[:limit]

    cards = []
    for cid, grade in exclusive:
        entry = key[cid]
        own = entry['methods'][0]
        if own not in specs:
            raise SystemExit(
                f'card {cid!r} was found by method {own!r}, which is not '
                f'in --configs {specs} -- the probe configs must match the '
                f'deck that produced this key')
        other = specs[1] if own == specs[0] else specs[0]
        qid = entry['query_id']
        if qid not in qtext:
            raise SystemExit(
                f'query {qid!r} (card {cid!r}) not found in the supplied '
                f'--queries file')
        cards.append(probe_card(cid, grade, entry, other,
                                retrievers_by_spec[other], qtext[qid]))
    return build_report(cards)


def run_probe(deck_key_path: str, verdicts_path: str, queries_path: str,
             index_dir: str, configs: str, limit: int = 0) -> dict:
    """I/O + real-retriever wiring around `probe()`.

    Retrievers are built by `eval_methods.build_retrievers` -- imported,
    never reimplemented -- so this probe can never quietly diverge from
    what the eval harness itself runs.
    """
    key = load_key(deck_key_path)
    verdicts = load_verdicts(verdicts_path)
    qtext = load_query_texts(queries_path)
    specs = [s.strip() for s in configs.split(',') if s.strip()]
    retrievers = eval_methods.build_retrievers(configs, index_dir)
    retrievers_by_spec = dict(zip(specs, retrievers))
    return probe(key, verdicts, qtext, specs, retrievers_by_spec,
                limit=limit)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--deck-key', required=True)
    ap.add_argument('--verdicts', required=True)
    ap.add_argument('--queries', required=True)
    ap.add_argument('--index', required=True)
    ap.add_argument('--configs',
                    default='passage:standard-40,chunk:3:exact:100',
                    help='exactly 2 comma-separated method specs, matching '
                         'the --configs the deck itself was built with')
    ap.add_argument('--limit', type=int, default=0,
                    help='cap the number of exclusive graded cards probed '
                         '(0 = no cap). Probing is SLOW -- chunk queries '
                         'run ~13s each -- use a small --limit for a smoke '
                         'run before committing to the full probe.')
    ap.add_argument('--out', default=None)
    args = ap.parse_args()

    report = run_probe(args.deck_key, args.verdicts, args.queries,
                       args.index, args.configs, limit=args.limit)

    print(f"exclusive graded cards probed: {report['n_exclusive_graded']}")
    for grade, block in sorted(report['by_grade'].items()):
        print(f"  {grade}: n={block['n']}  "
              f"page-absent={block['page_absent']}  "
              f"manuscript-absent={block['manuscript_absent']}")

    if args.out:
        with open(args.out, 'w', encoding='utf-8') as fh:
            json.dump(report, fh, ensure_ascii=False, indent=1,
                      sort_keys=True)
        print(f'\nwrote {args.out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
