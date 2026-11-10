# -*- coding: utf-8 -*-
"""Measure "close spellings" for the Blau lookup against pages with a human edition.

The idea (owner, 2026-11-04): when a clicked word reaches no entry, try it with
ONE letter swapped, using the letter pairs the automatic transcription confuses
most (``shared/unified_variants.py``, the table the Variants search uses), and
show what the swapped spellings reach. This script carries that logic
(``close_swaps``, ``close_spellings``) and asks how often it helps and how often
it misleads, on real pages. Its first run (2026-11-04; 1,200 documents) found
about 1 offer in 20 right at every word length and pair count, and the owner
dropped the feature: shared/blau_service.py answers only what a word's own
spelling reaches. The script stays so a later idea (a corpus-frequency filter,
say) can be measured the same way before anything ships.

How it measures:

1. The Princeton Geniza Project's Judeo-Arabic digital editions
   (``pgp_data/pgp.db``, ``document_sources.source_language = 'jrb'``) give, for
   a document, what a scholar read.
2. ``Transcriptions.txt`` gives the automatic text of the same manuscripts'
   pages (``document_fragments`` maps a document to its sys_ids).
3. Each automatic page is aligned to the edition word by word
   (``difflib.SequenceMatcher`` on the normalized words); a page is used only
   when at least ``--min-coverage`` of its words fall in matching runs, and a
   pair is taken only from a run of equal length of at most two words between
   two matching runs, so the pair is the same word read two ways.

What is counted (by occurrence, with distinct pairs beside):

- **A. misread, edition word in Blau, automatic word not**: the case close
  spellings exist for. ``right first`` -- the first close entry is one the
  edition word reaches; ``right in the cards`` -- one of the first
  ``MAX_CLOSE_CARDS`` is; ``wrong only`` -- close entries, none of them right;
  ``none`` -- nothing within one swap.
- **A2. misread onto another entry**: the automatic word itself reaches an
  entry the edition word does not. Close spellings are not tried (an exact hit
  wins), so the reader sees the wrong entry; counted for scale.
- **B. read right, not in Blau**: the automatic word equals the edition word
  and has no entry. Any close entry here is a false offer.
- **C. every automatic word without an entry** on the used pages: how often a
  click on such a word shows close spellings at all.

Each figure is reported for several pair counts (``--pair-counts``),
``CLOSE_PAIR_COUNT`` among them. Read-only on every input; writes only
``--out`` (JSON) and prints a summary.

Usage::

    python scripts/measure_blau_close_spellings.py --out _tmp/blau_close_measure.json
"""
from __future__ import annotations

import argparse
import difflib
import functools
import json
import os
import random
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable, Iterator, Optional

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from shared.blau_normalize import candidate_stems, fold_finals, normalize_ja  # noqa: E402
from shared.blau_service import HEADWORD_KINDS  # noqa: E402

HEADER_PREFIX = "==> "
HEADER_SUFFIX = " <=="
#: The variant measured first: the top 30 one-letter pairs, keys of at least
#: three letters, the first six entries shown.
CLOSE_PAIR_COUNT = 30
MIN_CLOSE_LETTERS = 3
MAX_CLOSE_CARDS = 6


@functools.lru_cache(maxsize=8)
def close_swaps(count: int = CLOSE_PAIR_COUNT) -> dict[str, tuple[tuple[str, int], ...]]:
    """letter -> ((the letter it may have been, rank), ...): the top ``count``
    one-letter pairs of ``shared.unified_variants.UNIFIED_VARIANT_PAIRS`` (ranked
    by frequency), finals folded the way ``normalize_ja`` folds them, each pair
    once, both directions. Rank 0 is the most frequent confusion."""
    from shared.unified_variants import UNIFIED_VARIANT_PAIRS

    ranked: list[tuple[str, str]] = []
    seen: set[frozenset[str]] = set()
    for source, target, _frequency in UNIFIED_VARIANT_PAIRS:
        if len(source) != 1 or len(target) != 1:
            continue
        a, b = fold_finals(source), fold_finals(target)
        pair = frozenset((a, b))
        if a == b or pair in seen:
            continue
        seen.add(pair)
        ranked.append((a, b))
        if len(ranked) >= count:
            break
    table: dict[str, list[tuple[str, int]]] = {}
    for rank, (a, b) in enumerate(ranked):
        table.setdefault(a, []).append((b, rank))
        table.setdefault(b, []).append((a, rank))
    return {letter: tuple(alts) for letter, alts in table.items()}


def close_spellings(key: str, keys: frozenset[str] | set[str], exclude: tuple[str, ...] = (),
                    count: int = CLOSE_PAIR_COUNT) -> list[tuple[str, str]]:
    """(stem, spelling) pairs one swapped letter away from ``key`` that reach a
    key in ``keys``: every letter is swapped for each letter it is confused
    with, the swapped spelling goes through ``candidate_stems`` (a swap inside a
    proclitic still peels), and a stem of at least ``MIN_CLOSE_LETTERS`` letters
    in ``keys`` (not in ``exclude``) is kept with the spelling that reached it.
    Best first: the more frequent confusion, the less peeled stem, the earlier
    letter. Pure."""
    if not key or " " in key or len(key) < MIN_CLOSE_LETTERS:
        return []
    swaps = close_swaps(count)
    best: dict[str, tuple[tuple[int, int, int], str]] = {}
    for pos, letter in enumerate(key):
        for alt, rank in swaps.get(letter, ()):
            spelling = key[:pos] + alt + key[pos + 1:]
            for order, stem in enumerate(candidate_stems(spelling)):
                if len(stem) < MIN_CLOSE_LETTERS or stem in exclude or stem not in keys:
                    continue
                score = (rank, order, pos)
                if stem not in best or score < best[stem][0]:
                    best[stem] = (score, spelling)
    return [(stem, spelling) for stem, (_score, spelling) in sorted(best.items(), key=lambda kv: kv[1][0])]


def _ro(path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{Path(path).resolve().as_posix()}?mode=ro", uri=True)


def load_key_roots(blau_db: str) -> dict[str, frozenset[str]]:
    """single-word headword-like key -> the entries it reaches (a cross-reference
    root counted as its target, as the panel shows it)."""
    conn = _ro(blau_db)
    try:
        xref = {str(rid): str(target) for rid, is_x, target in
                conn.execute("SELECT id, is_cross_reference, target_id FROM roots") if is_x and target}
        marks = ",".join("?" * len(HEADWORD_KINDS))
        out: dict[str, set[str]] = defaultdict(set)
        for norm, root_id in conn.execute(
                f"SELECT norm, root_id FROM forms_norm WHERE word_count = 1 AND kind IN ({marks})", HEADWORD_KINDS):
            out[str(norm)].add(xref.get(str(root_id), str(root_id)))
        return {k: frozenset(v) for k, v in out.items()}
    finally:
        conn.close()


def load_editions(pgp_db: str) -> tuple[dict[int, list[str]], dict[str, set[int]]]:
    """pgpid -> the normalized words of its Judeo-Arabic editions; sys_id -> pgpids."""
    conn = _ro(pgp_db)
    try:
        words: dict[int, list[str]] = defaultdict(list)
        for pgpid, content in conn.execute(
                "SELECT pgpid, content FROM document_sources WHERE source_language = 'jrb' "
                "AND doc_relation = 'Digital Edition' ORDER BY pgpid, sequence_order, id"):
            words[int(pgpid)].extend(normalize_ja(content or "").split())
        by_sys: dict[str, set[int]] = defaultdict(set)
        for pgpid, sys_id in conn.execute("SELECT document_id, sys_id FROM document_fragments"):
            if int(pgpid) in words:
                by_sys[str(sys_id)].add(int(pgpid))
        return dict(words), dict(by_sys)
    finally:
        conn.close()


def iter_pages(path: str, wanted: set[str]) -> Iterator[tuple[str, str, str]]:
    """(sys_id, header, text) for every page of a wanted sys_id, streaming."""
    sys_id = header = None
    lines: list[str] = []
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if line.startswith(HEADER_PREFIX) and line.rstrip().endswith(HEADER_SUFFIX):
                if sys_id is not None:
                    yield sys_id, header, "".join(lines)
                header = line.strip()[len(HEADER_PREFIX):-len(HEADER_SUFFIX)]
                candidate = header.split("_", 1)[0]
                sys_id = candidate if candidate in wanted else None
                lines = []
            elif sys_id is not None:
                lines.append(line)
    if sys_id is not None:
        yield sys_id, header, "".join(lines)


def aligned_pairs(page: list[str], edition: list[str], min_coverage: float) -> Optional[list[tuple[str, str]]]:
    """(automatic, edition) word pairs of one page, or None when the page does
    not follow the edition (fewer than ``min_coverage`` of its words matched)."""
    matcher = difflib.SequenceMatcher(None, page, edition, autojunk=False)
    ops = matcher.get_opcodes()
    matched = sum(i2 - i1 for tag, i1, i2, _j1, _j2 in ops if tag == "equal")
    if not page or matched / len(page) < min_coverage:
        return None
    pairs: list[tuple[str, str]] = []
    for k, (tag, i1, i2, j1, j2) in enumerate(ops):
        if tag == "equal":
            pairs.extend((page[i], page[i]) for i in range(i1, i2))
        elif tag == "replace" and i2 - i1 == j2 - j1 <= 2:
            before = ops[k - 1][0] if k > 0 else ""
            after = ops[k + 1][0] if k + 1 < len(ops) else ""
            if before == "equal" and after == "equal":
                pairs.extend(zip(page[i1:i2], edition[j1:j2]))
    return pairs


class Measure:
    def __init__(self, key_roots: dict[str, frozenset[str]], pair_counts: Iterable[int]):
        self.key_roots = key_roots
        self.keys = frozenset(key_roots)
        self.pair_counts = sorted(set(pair_counts))
        self._roots: dict[str, frozenset[str]] = {}
        self._close: dict[tuple[str, int], tuple[str, ...]] = {}

    def roots(self, word: str) -> frozenset[str]:
        """The entries a word reaches exactly (its candidates, as the lookup tries them)."""
        if word not in self._roots:
            found: set[str] = set()
            for stem in candidate_stems(word):
                found |= self.key_roots.get(stem, frozenset())
            self._roots[word] = frozenset(found)
        return self._roots[word]

    def close(self, word: str, count: int) -> tuple[str, ...]:
        """The entries the close spellings reach, best first, each once."""
        key = (word, count)
        if key not in self._close:
            ordered: list[str] = []
            for stem, _spelling in close_spellings(word, self.keys, exclude=tuple(candidate_stems(word)),
                                                   count=count):
                for root in sorted(self.key_roots.get(stem, ())):
                    if root not in ordered:
                        ordered.append(root)
            self._close[key] = tuple(ordered)
        return self._close[key]


def _empty_counts(pair_counts: list[int]) -> dict:
    a = {n: Counter() for n in pair_counts}
    return {"A": a, "A_distinct": {n: Counter() for n in pair_counts}, "A2": Counter(),
            "B": {n: Counter() for n in pair_counts}, "C": {n: Counter() for n in pair_counts}}


def run(args: argparse.Namespace) -> dict:
    started = time.monotonic()
    key_roots = load_key_roots(args.blau_db)
    editions, by_sys = load_editions(args.pgp_db)
    if args.limit_docs:
        rng = random.Random(args.seed)
        keep = set(rng.sample(sorted(editions), min(args.limit_docs, len(editions))))
        by_sys = {s: {p for p in ps if p in keep} for s, ps in by_sys.items()}
        by_sys = {s: ps for s, ps in by_sys.items() if ps}
    measure = Measure(key_roots, args.pair_counts)
    counts = _empty_counts(measure.pair_counts)
    seen_a: set[tuple[str, str]] = set()
    samples: dict[str, list[dict]] = {"right": [], "wrong": [], "none": [], "false_offer": []}
    pages_seen = pages_used = pairs_total = 0
    docs_used: set[int] = set()
    for sys_id, header, text in iter_pages(args.transcriptions, set(by_sys)):
        page = normalize_ja(text).split()
        if len(page) < args.min_words:
            continue
        pages_seen += 1
        best: Optional[list[tuple[str, str]]] = None
        best_doc = None
        for pgpid in sorted(by_sys[sys_id]):
            pairs = aligned_pairs(page, editions[pgpid], args.min_coverage)
            if pairs is not None and (best is None or len(pairs) > len(best)):
                best, best_doc = pairs, pgpid
        if best is None:
            continue
        pages_used += 1
        docs_used.add(best_doc)
        pairs_total += len(best)
        for auto, edited in best:
            if len(auto) < MIN_CLOSE_LETTERS or len(edited) < MIN_CLOSE_LETTERS:
                continue
            auto_roots = measure.roots(auto)
            if auto == edited:
                if not auto_roots:
                    for n in measure.pair_counts:
                        offered = bool(measure.close(auto, n))
                        counts["B"][n]["offered" if offered else "quiet"] += 1
                        counts["C"][n]["offered" if offered else "quiet"] += 1
                        if offered and n == CLOSE_PAIR_COUNT and len(samples["false_offer"]) < args.samples:
                            samples["false_offer"].append({"word": auto, "close": measure.close(auto, n)[:3]})
                continue
            edited_roots = measure.roots(edited)
            if not auto_roots:
                for n in measure.pair_counts:
                    counts["C"][n]["offered" if measure.close(auto, n) else "quiet"] += 1
            if not edited_roots:
                continue
            if auto_roots:
                if not (auto_roots & edited_roots):
                    counts["A2"]["occurrences"] += 1
                continue
            first_time = (auto, edited) not in seen_a
            seen_a.add((auto, edited))
            for n in measure.pair_counts:
                close = measure.close(auto, n)
                shown = close[:MAX_CLOSE_CARDS]
                if not close:
                    outcome = "none"
                elif close[0] in edited_roots:
                    outcome = "right_first"
                elif set(shown) & edited_roots:
                    outcome = "right_in_cards"
                else:
                    outcome = "wrong_only"
                counts["A"][n][outcome] += 1
                if first_time:
                    counts["A_distinct"][n][outcome] += 1
                if n == CLOSE_PAIR_COUNT and first_time:
                    bucket = {"right_first": "right", "right_in_cards": "right", "wrong_only": "wrong",
                              "none": "none"}[outcome]
                    if len(samples[bucket]) < args.samples:
                        samples[bucket].append({"automatic": auto, "edition": edited, "close": shown[:3],
                                                "edition_entries": sorted(edited_roots)[:3]})
    return {
        "inputs": {"pgp_db": args.pgp_db, "transcriptions": args.transcriptions, "blau_db": args.blau_db,
                   "min_coverage": args.min_coverage, "min_words": args.min_words,
                   "limit_docs": args.limit_docs, "seed": args.seed},
        "first_variant_pair_count": CLOSE_PAIR_COUNT, "max_close_cards": MAX_CLOSE_CARDS,
        "documents_with_jrb_edition": len(editions), "documents_used": len(docs_used),
        "pages_seen": pages_seen, "pages_used": pages_used, "aligned_pairs": pairs_total,
        "A": {n: dict(c) for n, c in counts["A"].items()},
        "A_distinct": {n: dict(c) for n, c in counts["A_distinct"].items()},
        "A2_misread_onto_another_entry": dict(counts["A2"]),
        "B_read_right_not_in_blau": {n: dict(c) for n, c in counts["B"].items()},
        "C_every_word_without_an_entry": {n: dict(c) for n, c in counts["C"].items()},
        "samples": samples,
        "seconds": round(time.monotonic() - started, 1),
    }


def _pct(part: int, whole: int) -> str:
    return f"{100.0 * part / whole:.1f}%" if whole else "n/a"


def summary(result: dict) -> str:
    lines = [f"documents used {result['documents_used']:,} of {result['documents_with_jrb_edition']:,}; "
             f"pages used {result['pages_used']:,} of {result['pages_seen']:,}; "
             f"aligned word pairs {result['aligned_pairs']:,}; {result['seconds']} s",
             f"A2 misread onto another entry (close not tried): "
             f"{result['A2_misread_onto_another_entry'].get('occurrences', 0):,}"]
    for n, a in result["A"].items():
        total = sum(a.values())
        b = result["B_read_right_not_in_blau"][n]
        c = result["C_every_word_without_an_entry"][n]
        mark = "  <- first variant" if int(n) == result["first_variant_pair_count"] else ""
        lines.append(
            f"pairs {n:>3}: A n={total:,} right-first {_pct(a.get('right_first', 0), total)}, "
            f"right-in-cards {_pct(a.get('right_first', 0) + a.get('right_in_cards', 0), total)}, "
            f"wrong-only {_pct(a.get('wrong_only', 0), total)}, none {_pct(a.get('none', 0), total)} | "
            f"B false offers {_pct(b.get('offered', 0), sum(b.values()))} of {sum(b.values()):,} | "
            f"C offered {_pct(c.get('offered', 0), sum(c.values()))} of {sum(c.values()):,}{mark}")
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--pgp-db", default=str(REPO / "pgp_data" / "pgp.db"))
    parser.add_argument("--transcriptions", default=str(REPO / "Transcriptions.txt"))
    parser.add_argument("--blau-db", default=str(REPO / "blau_data" / "blau.db"))
    parser.add_argument("--pair-counts", type=int, nargs="+", default=[10, 20, CLOSE_PAIR_COUNT, 50])
    parser.add_argument("--min-coverage", type=float, default=0.5)
    parser.add_argument("--min-words", type=int, default=15)
    parser.add_argument("--limit-docs", type=int, default=0, help="a seeded sample of documents (0 = all)")
    parser.add_argument("--seed", type=int, default=20261104)
    parser.add_argument("--samples", type=int, default=12)
    parser.add_argument("--out", default="")
    args = parser.parse_args(argv)
    for name in ("pgp_db", "transcriptions", "blau_db"):
        path = getattr(args, name)
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            print(f"missing or empty input: {path}", file=sys.stderr)
            return 2
    result = run(args)
    print(summary(result))
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"written: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
