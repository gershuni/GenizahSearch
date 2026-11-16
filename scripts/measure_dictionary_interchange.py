# -*- coding: utf-8 -*-
"""Measure what the צ/ט exchange gains and costs in the Judeo-Arabic dictionaries' lookups.

Judeo-Arabic writes ض (צ̇) and ظ (ט̇) for one another, so the lookup and the expression search
also try a word with its צ and ט exchanged (``shared.blau_normalize.interchange_variants``,
``shared.ja_expression.interchange_candidates``) and show what that reaches after what the word
as written reaches, as a possible spelling. Two rules are compared:

- **any letter**: every exchanged spelling the dictionary has is shown.
- **dotted only** (what ships, ``shared.ja_roots.INTERCHANGE_DOTTED_ONLY``): an exchanged spelling
  is shown only when the dictionary prints the letter it reaches with the dot (ט̇ for a word's צ,
  צ̇ for its ט), i.e. the ض/ظ pair itself. Each word and run goes through the services twice, the
  rule off and on, with the display caps lifted (``NO_CAP``): the counts are what each rule shows.

The corpus drops dots often, on both sides, so neither the word's dot nor the dictionary's can be
taken as proof. What is counted:

- **gain**: a word that reaches no entry as written (nor with a yod or waw added or dropped) and
  reaches one with the exchange; under each rule.
- **added**: cards an exchange adds to a word that already reaches entries as written; under
  each rule. These are the cost to a reader who finds the word's own entry first anyway.
- **Blau's Arabic**: Blau names each root in Arabic too. An exchanged card whose Arabic root has
  ظ/ض at the letter is the interchange itself; one with only ط/ص is not. Crossed with whether
  the rule keeps the card, this says how often "dotted only" drops a real ض/ظ (the dictionary
  left the dot out) and how often "any letter" shows a ط/ص root.
- every figure split by whether the corpus word carries a dot or an apostrophe on its צ/ט.
- **expressions**: the ``--expressions`` most frequent two-word runs with צ or ט on the same pages,
  through each dictionary's expression search.

Words come from the automatic text (``Transcriptions.txt``), every ``--page-step``-th page up to
``--lines`` lines: the most frequent ``--top`` distinct words with צ or ט, and ``--tail`` more drawn
at random from the rest. Read-only on every input; writes ``--out`` (JSON) and ``--examples``
(the gains and the Blau ط/ص cards, for reading) and prints a summary.

Usage::

    python scripts/measure_dictionary_interchange.py --blau blau_data/blau.db \\
        --friedman friedman_data/friedman.db --out _tmp/interchange.json --examples _tmp/interchange_examples.tsv
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import random
import sys
import time
from pathlib import Path
from typing import Iterator, Optional

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from shared.blau_normalize import normalize_ja, spelling_variant  # noqa: E402
from shared.ja_roots import dot_pattern  # noqa: E402

SWAP = {"צ": "ט", "ט": "צ"}
#: The Arabic letters of the interchange itself, by the Hebrew letter a word's letter is exchanged for.
ARABIC_PAIR = {"ט": "ظ", "צ": "ض"}
#: The Arabic letters the same Hebrew letter writes without the dot.
ARABIC_PLAIN = {"ט": "ط", "צ": "ص"}
#: The services' display caps are lifted: an exchanged card ranks after the word's own, so a capped
#: page would hide it.
NO_CAP = 10_000
STRIP = "[](){}<>.,:;!?/\\|־–-\"״“”"


def iter_lines(path: str, page_step: int, max_lines: int) -> Iterator[str]:
    page = -1
    taken = 0
    with open(path, encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = raw.rstrip("\r\n")
            if line.startswith("==> "):
                page += 1
                continue
            if page % page_step:
                continue
            yield line
            taken += 1
            if taken >= max_lines:
                return


def corpus_words(lines: Iterator[str]) -> tuple[collections.Counter, list[tuple[str, str]]]:
    """(word -> occurrences, every adjacent pair of words) for words with Hebrew letters."""
    words: collections.Counter = collections.Counter()
    pairs: list[tuple[str, str]] = []
    for line in lines:
        tokens = [t.strip(STRIP) for t in line.split()]
        tokens = [t for t in tokens if normalize_ja(t)]
        words.update(tokens)
        pairs.extend(zip(tokens, tokens[1:]))
    return words, pairs


def has_letter(word: str) -> bool:
    key = normalize_ja(word)
    return " " not in key and len(key) >= 3 and any(ch in SWAP for ch in key)


def word_dotted(word: str) -> bool:
    return "צ*" in dot_pattern(word) or "ט*" in dot_pattern(word)


def targets(word: str) -> set[str]:
    return {SWAP[ch] for ch in normalize_ja(word) if ch in SWAP}


#: The Hebrew letter each Arabic letter of the pair and its plain partner is written with.
_HEBREW_OF = {"ظ": "ט", "ط": "ט", "ض": "צ", "ص": "צ"}


def arabic_class(arabic: str, word: str, matched: str) -> str:
    """"pair" when the Arabic root has ظ/ض at the letter the exchange reached (by its occurrence,
    ``shared.ja_roots.exchanged_letters``), "plain" for ط/ص there, "none" when the root has no
    such letter at that occurrence. Without a known exchange, any letter of the kinds exchanged for."""
    from shared.ja_roots import exchanged_letters
    places = exchanged_letters(word, matched) if matched else None
    if not places:
        letters = targets(word)
        if any(ARABIC_PAIR[letter] in arabic for letter in letters):
            return "pair"
        return "plain" if any(ARABIC_PLAIN[letter] in arabic for letter in letters) else "none"
    seen: dict[str, int] = {}
    at: dict[tuple[str, int], str] = {}
    for ch in arabic:
        hebrew = _HEBREW_OF.get(ch)
        if hebrew:
            at[(hebrew, seen.get(hebrew, 0))] = ch
            seen[hebrew] = seen.get(hebrew, 0) + 1
    found = [at.get(place) for place in places]
    if any(ch is None for ch in found):
        return "none"
    return "pair" if all(ch in ARABIC_PAIR.values() for ch in found) else "plain"


def exchanged(record) -> bool:
    paths = getattr(record, "analyses", ())
    return bool(paths) and all("interchange" in p for p in paths)


def weak(record) -> bool:
    paths = getattr(record, "analyses", ())
    return bool(paths) and all(spelling_variant(p) for p in paths)


def _blank() -> dict:
    return collections.defaultdict(int)


def under(rule: bool, call):
    """``call()`` with the shipped dotted rule on (True) or off (False)."""
    import shared.ja_roots
    before = shared.ja_roots.INTERCHANGE_DOTTED_ONLY
    shared.ja_roots.INTERCHANGE_DOTTED_ONLY = rule
    try:
        return call()
    finally:
        shared.ja_roots.INTERCHANGE_DOTTED_ONLY = before


def measure_lookups(name: str, lookup, ident, spellings_of, arabic_of, chosen: list[tuple[str, int]],
                    examples: list) -> dict:
    """Each word through the service twice, the rule off and on (what ships), with no display cap."""
    counts = {"all": _blank(), "word dotted": _blank(), "word plain": _blank()}
    started = time.time()
    for word, freq in chosen:
        records = under(False, lambda: lookup(word))
        letters = targets(word)
        own = [r for r in records if not weak(r)]
        swapped = [r for r in records if exchanged(r)]
        kept = {ident(r) for r in under(True, lambda: lookup(word)) if exchanged(r)}
        dotted = [r for r in swapped if ident(r) in kept]
        for part in ("all", "word dotted" if word_dotted(word) else "word plain"):
            c = counts[part]
            c["words"] += 1
            c["occurrences"] += freq
            if own:
                c["words with own"] += 1
                c["occ with own"] += freq
                c["added cards, any"] += len(swapped)
                c["added cards, dotted only"] += len(dotted)
                c["words given added, any"] += bool(swapped)
                c["words given added, dotted only"] += bool(dotted)
            elif not records or all(exchanged(r) for r in records):
                c["words with nothing as written"] += 1
                c["occ with nothing as written"] += freq
                if swapped:
                    c["gain words, any"] += 1
                    c["gain occ, any"] += freq
                if dotted:
                    c["gain words, dotted only"] += 1
                    c["gain occ, dotted only"] += freq
            for r in swapped:
                if arabic_of is not None:
                    cls = arabic_class(arabic_of(r), word, getattr(r, "candidate", ""))
                    rule = "kept" if ident(r) in kept else "dropped"
                    c[f"cards: arabic {cls}, rule {rule}"] += 1
        for r in swapped:
            examples.append((name, "gain" if not own else "added", word, freq,
                             " / ".join(s for s in spellings_of(r) if s)[:60],
                             "kept" if ident(r) in kept else "dropped",
                             arabic_of(r) if arabic_of else ""))
    counts["seconds"] = round(time.time() - started, 1)
    return {k: dict(v) if isinstance(v, collections.defaultdict) else v for k, v in counts.items()}


def measure_expressions(expression, ident, chosen: list[tuple[str, str]]) -> dict:
    """Each run through the service twice, the rule off and on (what ships), with no display cap."""
    c = _blank()
    started = time.time()
    for first, second in chosen:
        text = f"{first} {second}"
        result = under(False, lambda: expression(text))
        if result.problem:
            c["problem " + result.problem] += 1
            continue
        shipped = under(True, lambda: expression(text))
        own = [h for h in result.hits if not h.possible_spelling]
        added = [h for h in result.hits if h.possible_spelling]
        dotted = [h for h in shipped.hits if h.possible_spelling]
        c["runs"] += 1
        c["runs with own"] += bool(own)
        c["runs given added, any"] += bool(added) and bool(own)
        c["runs given added, dotted only"] += bool(dotted) and bool(own)
        c["gain runs, any"] += bool(added) and not own
        c["gain runs, dotted only"] += bool(dotted) and not own
        c["added hits, any"] += len(added)
        c["added hits, dotted only"] += len(dotted)
    c["seconds"] = round(time.time() - started, 1)
    return dict(c)


def run(args: argparse.Namespace) -> dict:
    os.environ["BLAU_DICTIONARY_ENABLED"] = "1"
    os.environ["FRIEDMAN_DICTIONARY_ENABLED"] = "1"
    from shared.blau_service import BlauService
    from shared.friedman_service import FriedmanService

    words, pairs = corpus_words(iter_lines(args.transcriptions, args.page_step, args.lines))
    with_letter = [(w, n) for w, n in words.most_common() if has_letter(w)]
    rng = random.Random(args.seed)
    top = with_letter[:args.top]
    rest = with_letter[args.top:]
    tail = rng.sample(rest, min(args.tail, len(rest)))
    total_occ = sum(n for _, n in with_letter)
    pair_counts = collections.Counter(p for p in pairs if has_letter(p[0]) or has_letter(p[1]))
    # The most frequent runs: a run drawn at random is almost never a dictionary's expression.
    run_sample = [p for p, _ in pair_counts.most_common(args.expressions)]

    blau = BlauService(args.blau)
    friedman = FriedmanService(args.friedman)
    if not blau.is_ready() or not friedman.is_available():
        raise SystemExit(f"not served: blau={blau.unavailable_reason!r} friedman={friedman.is_available()}")

    blau_spellings = lambda card: [card.name, card.root_label]  # noqa: E731
    friedman_spellings = lambda m: [m.unit.head_text, m.unit.root, m.unit.entry_root, m.root]  # noqa: E731
    blau_card = lambda card: (card.root_id, card.sub_entry)  # noqa: E731
    friedman_unit = lambda record: record.unit.id  # noqa: E731
    examples: list = []
    result = {
        "corpus": {"lines": args.lines, "page_step": args.page_step, "distinct words with the letters": len(with_letter),
                   "their occurrences": total_occ, "top": len(top), "tail": len(tail), "tail drawn from": len(rest),
                   "top covers occurrences": sum(n for _, n in top)},
        "lookup": {},
        "expressions": {},
    }
    for label, sample in (("top", top), ("tail", tail)):
        result["lookup"][label] = {
            "blau": measure_lookups("blau", lambda w: blau.lookup(w, limit=NO_CAP).cards, blau_card, blau_spellings,
                                    lambda c: c.arabic, sample, examples),
            "friedman": measure_lookups("friedman", lambda w: friedman.lookup(w, limit=NO_CAP).matches, friedman_unit,
                                        friedman_spellings, None, sample, examples),
        }
    result["expressions"] = {
        "runs drawn": len(run_sample),
        "blau": measure_expressions(lambda t: blau.expression(t, limit=NO_CAP), blau_card, run_sample),
        "friedman": measure_expressions(lambda t: friedman.expression(t, limit=NO_CAP), friedman_unit, run_sample),
    }
    blau.close()
    friedman.close()
    if args.examples:
        Path(args.examples).parent.mkdir(parents=True, exist_ok=True)
        with open(args.examples, "w", encoding="utf-8", newline="\n") as fh:
            fh.write("dictionary\tkind\tword\toccurrences\treached\thebrew dot\tarabic\n")
            for row in examples:
                fh.write("\t".join(str(x) for x in row) + "\n")
    return result


def summary(result: dict) -> str:
    lines = [json.dumps(result["corpus"], ensure_ascii=False)]
    for label, by_dict in result["lookup"].items():
        for name, parts in by_dict.items():
            for part, c in parts.items():
                if part == "seconds":
                    continue
                lines.append(f"lookup {label} {name} [{part}]: " + ", ".join(f"{k}={v}" for k, v in sorted(c.items())))
    for name in ("blau", "friedman"):
        lines.append(f"expressions {name}: " + ", ".join(f"{k}={v}" for k, v in sorted(result['expressions'][name].items())))
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--transcriptions", default=str(REPO / "Transcriptions.txt"))
    parser.add_argument("--blau", default=str(REPO / "blau_data" / "blau.db"))
    parser.add_argument("--friedman", default=str(REPO / "friedman_data" / "friedman.db"))
    parser.add_argument("--lines", type=int, default=300_000)
    parser.add_argument("--page-step", type=int, default=10)
    parser.add_argument("--top", type=int, default=1500)
    parser.add_argument("--tail", type=int, default=1000)
    parser.add_argument("--expressions", type=int, default=600)
    parser.add_argument("--seed", type=int, default=20261116)
    parser.add_argument("--out", required=True)
    parser.add_argument("--examples", default="")
    args = parser.parse_args(argv)
    result = run(args)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(result, fh, ensure_ascii=False, indent=1)
    print(summary(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
