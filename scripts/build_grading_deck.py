# -*- coding: utf-8 -*-
"""Build a BLINDED relation-grading deck comparing two retrieval methods.

Adapts the SEED-029 review tool (`same_work_spike/probe/scripts/
prep_review_tool.py`): same 8-option relation vocabulary, same bilingual
keyboard grading, same RTL side-by-side card with the matched span marked and
context in gray, same localStorage + JSON export.

Four things change, and each exists to stop the deck from answering its own
question:

1. THE UNIT. A card is one (query, returned manuscript) pair, not one
   machine-detected pair. That is what a precision measurement of a SEARCH
   needs: of what a method handed the reader, how much was real?

2. BLINDING. No method name, no rank, no score, no density on the card, and
   the pooled order is a deterministic shuffle. The method behind each card
   lives only in the key file, which the deck HTML never sees.

3. METHOD-NEUTRAL EVIDENCE. The highlighted span is recomputed here by ONE
   shared alignment for every card, never taken from the method that returned
   it. Otherwise span style leaks provenance and the blinding is decorative --
   the two engines mark different things.

4. PRE-REGISTRATION. Strata, allocation and draw are fixed in a prereg file
   before any card exists; the deck manifest pins its own content hash and the
   prereg's. The verdicts file is separate, so grading cannot silently edit
   the deck it grades.

Precision here is RELATION-AWARE, not binary: the vocabulary distinguishes
same-text from paraphrase, shared formula, canonical quotation and duplicate
photography. A returned manuscript that is not the query's own source may be a
genuine parallel -- which is exactly why this deck exists and why the
self-retrieval instrument could not produce precision.

Usage:
  python scripts/build_grading_deck.py --queries Q.jsonl --index IDX \
      --out-dir DIR --queries-n 60 --per-method-k 3
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import unicodedata

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_index import open_index  # noqa: E402
from shared.passage_normalize import (  # noqa: E402
    K, gram_codes, norm_stream,
)

DECK_SCHEMA_VERSION = 1
CONTEXT_CHARS = 260


def sha(obj) -> str:
    blob = json.dumps(obj, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(blob).hexdigest()


def neutral_span(query_text: str, record_text: str) -> dict:
    """The one alignment every card uses, whichever method produced it.

    Returns display-space offsets for both sides. Taking each method's own
    span would let the grader tell them apart by how the highlight sits.
    """
    q_stream, q_off = norm_stream(query_text)
    r_stream, r_off = norm_stream(record_text)
    blank = {
        'q': ('', unicodedata.normalize('NFC', query_text)[:CONTEXT_CHARS], ''),
        'r': ('', unicodedata.normalize('NFC', record_text)[:CONTEXT_CHARS], ''),
        'score': 0.0, 'letters': 0, 'aligned': False,
    }
    if not q_stream or not r_stream:
        return blank
    # NO SCORE CUTOFF, deliberately. A cutoff of 40 silently dropped the
    # WORST-aligning results -- precisely the false positives a precision
    # measurement exists to count. A pair a method RETURNED is part of what it
    # returned; the grader decides whether it is junk, not this builder.
    found = precise_shared_span(q_stream, r_stream)
    if found is None:
        return blank
    qs, qe, rs, re_ = found
    q_nfc = unicodedata.normalize('NFC', query_text)
    r_nfc = unicodedata.normalize('NFC', record_text)

    def cut(offs, text, s, e):
        s = max(0, min(s, len(offs) - 1))
        e = max(s + 1, min(e, len(offs)))
        a0, b0 = offs[s], offs[e - 1] + 1
        return (text[max(0, a0 - CONTEXT_CHARS):a0], text[a0:b0],
                text[b0:b0 + CONTEXT_CHARS])

    return {
        'q': cut(q_off, q_nfc, qs, qe),
        'r': cut(r_off, r_nfc, rs, re_),
        'score': 0.0,
        'letters': int(re_ - rs),
        'aligned': True,
    }


# The SEED-029 relation vocabulary, verbatim. The numbering has a gap at 2
# because that scale merged verbatim/near_verbatim into same_text; the key
# bindings preserve the original aliasing so muscle memory carries over.
GRADES = [
    ("same_text", "1 · אותו טקסט — כולל שינויי סופרים"),
    ("paraphrase", "3 · ניסוח שונה"),
    ("shared_formula", "4 · נוסחה משותפת"),
    ("topical", "5 · דמיון נושאי בלבד"),
    ("unrelated", "6 · לא קשור"),
    ("junk", "7 · דף פסול"),
    ("canonical", "8 · ציטוט מקרא/חז\"ל/תפילה"),
    ("duplicate_photo", "9 · אותו דף שצולם פעמיים"),
]

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             'templates', 'grading_deck.html')

MAX_POSTINGS_PER_CODE = 40


def precise_shared_span(q_stream: str, r_stream: str):
    """Locate the actual shared passage, not merely a good window.

    partial_ratio_alignment returns the best-scoring WINDOW of the needle
    length, which on a 400-letter needle marked 74% of the query pane and
    localized nothing -- the grader could not see where the two texts agree,
    and cards carrying a real but short shared formula looked like noise.

    This finds the strongest alignment DIAGONAL instead: shared 5-grams vote
    for their offset (qpos - rpos), the busiest diagonal wins, and the span is
    that diagonal's extent. It is the same seeding idea the engine uses, but
    applied identically to every card whichever method produced it, so the
    evidence stays method-neutral.

    Returns (q0, q1, r0, r1) in normalized-stream coordinates, or None.
    """
    qc = gram_codes(q_stream)
    rc = gram_codes(r_stream)
    if not len(qc) or not len(rc):
        return None
    where = {}
    for i, c in enumerate(rc.tolist()):
        lst = where.setdefault(c, [])
        if len(lst) < MAX_POSTINGS_PER_CODE:
            lst.append(i)
    diag = {}
    for qi, c in enumerate(qc.tolist()):
        for ri in where.get(c, ()):  # noqa: B007
            d = qi - ri
            e = diag.get(d)
            if e is None:
                diag[d] = [1, qi, qi, ri, ri]
            else:
                e[0] += 1
                if qi < e[1]:
                    e[1] = qi
                if qi > e[2]:
                    e[2] = qi
                if ri < e[3]:
                    e[3] = ri
                if ri > e[4]:
                    e[4] = ri
    if not diag:
        return None
    # Merge adjacent diagonals (a scribal insertion shifts the offset by a
    # letter or two); then take the busiest cluster.
    best_d = max(diag, key=lambda d: diag[d][0])
    n, q0, q1, r0, r1 = diag[best_d]
    for d in (best_d - 2, best_d - 1, best_d + 1, best_d + 2):
        e = diag.get(d)
        if e:
            n += e[0]
            q0, q1 = min(q0, e[1]), max(q1, e[2])
            r0, r1 = min(r0, e[3]), max(r1, e[4])
    if n < 2:
        return None
    return q0, q1 + K, r0, r1 + K


def draw_queries(all_queries: list, n: int, salt: str) -> list:
    """Deterministic, spread draw. Recorded in the prereg before any card."""
    ordered = sorted(all_queries, key=lambda q: hashlib.sha256(
        f'{salt}|{q["query_id"]}'.encode()).hexdigest())
    return ordered[:n]


def build(args) -> int:
    from shared.passage_policy import get_preset
    from shared.retrieval_adapters import (
        ChunkRetriever, PassageRetriever, eligible_record_ids)
    from shared.retrieval_eval import EvalQuery, split_queries

    os.makedirs(args.out_dir, exist_ok=True)
    raw = [json.loads(l) for l in open(args.queries, encoding='utf-8')]
    evq = [EvalQuery(d['query_id'], d['text'], frozenset(d['positives']),
                     d.get('strata') or {}) for d in raw]
    keep = {q.query_id for q in split_queries(evq)[args.split]}
    pool_src = [d for d in raw if d['query_id'] in keep]
    drawn = draw_queries(pool_src, args.queries_n, args.salt)
    print(f'{len(raw):,} queries; split={args.split} -> {len(pool_src):,}; '
          f'drawn {len(drawn)}', flush=True)

    # ---- PRE-REGISTRATION: fixed before a single card is rendered ----------
    prereg = {
        'deck_schema_version': DECK_SCHEMA_VERSION,
        'purpose': 'blinded relation grading, two retrieval methods pooled',
        'split': args.split,
        'exploratory': args.split != 'holdout',
        'salt': args.salt,
        'queries_n': args.queries_n,
        'per_method_k': args.per_method_k,
        'query_set': os.path.basename(args.queries),
        'query_ids': [d['query_id'] for d in drawn],
        'methods': args.configs.split(','),
        'vocabulary': [g[0] for g in GRADES],
        'ground_truth_grain': 'manuscript (sys_id)',
        'notes': (
            'A returned manuscript that is not the query source may still be '
            'a genuine parallel; the vocabulary, not a binary, decides.'),
    }
    prereg['prereg_id'] = sha(prereg)[:16]
    with open(os.path.join(args.out_dir, 'prereg.json'), 'w',
              encoding='utf-8') as fh:
        json.dump(prereg, fh, ensure_ascii=False, indent=1, sort_keys=True)
    print(f'prereg_id={prereg["prereg_id"]}', flush=True)

    # ---- run both methods --------------------------------------------------
    idx = open_index(args.index)
    if idx is None:
        raise SystemExit(f'index will not open: {args.index}')
    # EQUAL ELIGIBILITY. The Tantivy corpus holds 948,549 page records; the
    # passage index holds 702,466, because Stage-0 removes 246,083 short
    # pages, target sheets and ownership stamps. Unfiltered, the incumbent
    # returns records the passage engine structurally cannot, and the deck
    # would grade two different corpora rather than two methods.
    eligible = eligible_record_ids(idx)
    print(f'equal-eligibility set: {len(eligible):,} records', flush=True)
    text_of = {}
    retrievers = {}
    for spec in args.configs.split(','):
        kind, _, rest = spec.strip().partition(':')
        if kind == 'passage':
            retrievers[spec] = PassageRetriever(index=idx,
                                                policy=get_preset(rest))
        else:
            from shared.metadata_manager import MetadataManager
            from shared.search_engine import SearchEngine
            from shared.variants import VariantManager
            eng = SearchEngine(MetadataManager(), VariantManager())
            if eng.searcher is None:
                raise SystemExit('Tantivy index failed to open')
            size, mode, freq = (rest.split(':') + ['exact', '100'])[:3]
            retrievers[spec] = ChunkRetriever(engine=eng, chunk_size=int(size),
                                              mode=mode, max_freq=int(freq),
                                              eligible=eligible)

    rid_index = {idx.record_id(i): i for i in range(idx.n_records)}
    pairs: dict = {}
    original_text: dict = {}
    for n, d in enumerate(drawn, 1):
        for spec, r in retrievers.items():
            for rid in r.retrieve(d['text'])[:args.per_method_k]:
                pairs.setdefault((d['query_id'], rid), set()).add(spec)
        if n % 10 == 0:
            print(f'  {n}/{len(drawn)} queries, {len(pairs)} pairs',
                  flush=True)

    # ---- original transcription text for the records on the deck ----------
    # The index stores the NORMALIZED stream -- space-free, finals folded --
    # which is what the matcher compares but NOT something a human can grade.
    # An earlier deck rendered that stream straight into the manuscript pane
    # and it came out as unreadable letter soup. One pass over the corpus
    # fetches the real text for just the records that appear on a card.
    wanted = {rid for _q, rid in pairs}
    print(f'fetching original text for {len(wanted):,} records...', flush=True)
    from shared.passage_corpus import iter_records as _iter
    for rid, txt in _iter(args.transcriptions):
        if rid in wanted:
            original_text[rid] = txt
            if len(original_text) == len(wanted):
                break
    missing = len(wanted) - len(original_text)
    print(f'  found {len(original_text):,}, missing {missing}', flush=True)

    # ---- cards: neutral evidence, no provenance ---------------------------
    qtext = {d['query_id']: d for d in drawn}
    cards, key = [], []
    for (qid, rid), methods in sorted(pairs.items()):
        ri = rid_index.get(rid)
        if ri is None:
            continue
        rec_text = original_text.get(rid) or idx.stream(ri)
        span = neutral_span(qtext[qid]['text'], rec_text)
        card_id = sha({'q': qid, 'r': rid})[:16]
        cards.append({
            'id': card_id,
            'q_before': span['q'][0], 'q_match': span['q'][1],
            'q_after': span['q'][2],
            'r_before': span['r'][0], 'r_match': span['r'][1],
            'r_after': span['r'][2],
            'letters': span['letters'],
            'aligned': span['aligned'],
            'r_sys': rid.split('_', 1)[0],
            'is_source_ms': rid.split('_', 1)[0] == qtext[qid]['meta']['sys_id']
            if 'meta' in qtext[qid] else None,
        })
        key.append({'id': card_id, 'query_id': qid, 'record_id': rid,
                    'methods': sorted(methods)})
    # Deterministic shuffle by card id: pooled order must not encode method.
    cards.sort(key=lambda c: c['id'])
    for c in cards:
        c.pop('is_source_ms', None)        # would leak the answer

    manifest = {
        'prereg_id': prereg['prereg_id'],
        'n_cards': len(cards),
        'n_queries': len(drawn),
        'cards_hash': sha(cards),
        'key_hash': sha(key),
    }
    for name, obj in (('deck_key.json', key), ('deck_manifest.json', manifest)):
        with open(os.path.join(args.out_dir, name), 'w',
                  encoding='utf-8') as fh:
            json.dump(obj, fh, ensure_ascii=False, indent=1, sort_keys=True)

    with open(TEMPLATE_PATH, encoding='utf-8') as fh:
        html = fh.read()
    html = html.replace('__DATA__', json.dumps(cards, ensure_ascii=False))
    html = html.replace('__GRADES__', json.dumps(GRADES, ensure_ascii=False))
    html = html.replace('__DECKID__', manifest['cards_hash'][:16])
    out_html = os.path.join(args.out_dir, 'grading_deck.html')
    with open(out_html, 'w', encoding='utf-8') as fh:
        fh.write(html)
    n_unaligned = sum(1 for c in cards if not c['aligned'])
    print()
    print(f'pairs returned   {len(pairs)}')
    print(f'cards rendered   {len(cards)}')
    print(f'  unrenderable   {len(pairs) - len(cards)}'
          f'  (must be 0 under equal eligibility)')
    print(f'  unmarked       {n_unaligned}'
          f'  (no alignment computed; still graded)')
    print(f'deck  {out_html} ({len(html)//1024} KB)')
    print(f'key   deck_key.json (hash {manifest["key_hash"][:16]}) '
          f'-- do NOT open before grading')
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--queries', required=True)
    ap.add_argument('--index', required=True)
    ap.add_argument('--out-dir', required=True)
    ap.add_argument('--configs',
                    default='passage:standard-40,chunk:3:exact:100')
    ap.add_argument('--split', default='tune', choices=['tune', 'holdout'])
    ap.add_argument('--queries-n', type=int, default=60)
    ap.add_argument('--per-method-k', type=int, default=3)
    ap.add_argument('--salt', default='deck-v1')
    ap.add_argument('--transcriptions',
                    default=r'C:\GenizahSearch\Transcriptions.txt')
    return build(ap.parse_args())


if __name__ == '__main__':
    raise SystemExit(main())
