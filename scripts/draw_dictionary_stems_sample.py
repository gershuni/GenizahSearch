# -*- coding: utf-8 -*-
"""Draw the graded click sample for the cards found by verb form (the private dictionary lookup plan,
section 6.1, decided 2026-11-11 before any draw). Nothing here ships and nothing is changed: the script
reads the corpus's editions and the two dictionary sidecars and writes the sample and the grading
batches to ``--out`` (the private repository: they hold edition text and dictionary glosses).

The population: the words of the Judaeo-Arabic digital editions in ``pgp_data/pgp.db``, drawn as
running text is read (token sampling: a frequent word is drawn as often as it occurs), with a
recorded seed; function words, words under three letters and words in editorial brackets left out.
A drawn word is kept when it gets at least one card found by verb form in either dictionary, with
the kinds graded now switched on (A, B, D; C and E stay hidden): the lookup's own cards, in the
panel's order (``BlauService.lookup`` / ``FriedmanService.lookup`` with ``verb_forms``). A word's
kind in a dictionary is its first such card's. Drawing stops when every graded kind has
``QUOTA`` words in each dictionary; a word counts in both dictionaries where it has a kind.

What the grader sees, per word: the word in its sentence (``CONTEXT`` words each side) and every
root the two dictionaries propose for it, each with the stems the word fits and each stem's gloss
from the dictionaries (the stem's paragraph: Blau's senses, Friedman's definition). Not which
dictionary proposed it, nor its kind. The grader marks each root right / wrong / cannot tell, and
which of its stems is right. ``scripts/score_dictionary_stems_sample.py`` applies the pass rule.

Usage::

    python -m scripts.draw_dictionary_stems_sample --blau-db blau_data/blau-1.1.0.db --out <dir>
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from shared.ja_expression import is_function_word, lookup_key, word_keys
from shared.ja_roots import dot_pattern, stem_label
from shared.ja_verb_forms import VERB_FORMS_VERSION, verb_form_cards

DRAW_VERSION = 1
GRADED_KINDS = frozenset("ABD")
DICTIONARIES = ("blau", "friedman")
QUOTA = 100
CONTEXT = 8
BATCH = 25
SEED = 20261111
MIN_LETTERS = 3
LANGUAGE = "Judaeo-Arabic"
BROWSE_URL = "https://genizahsearch.com/browse?sys_id="
GLOSSES_PER_STEM = 3
#: Around a printed word: punctuation the edition sets next to it.
_EDGE = ".,:;!?()\"'״׳-–—|/"
#: Inside a word: an editorial mark (a restored or doubtful letter, a gap) leaves it out.
_EDITORIAL = re.compile(r"[\[\]{}<>?…\d]|[A-Za-z]")
_HEBREW = re.compile(r"[א-ת]")


# ---------------------------------------------------------------------------
# The words
# ---------------------------------------------------------------------------

def clean_word(token: str) -> str:
    """A printed token as a reader clicks it: the punctuation around it dropped; '' when it carries
    an editorial mark (brackets, a question mark, a gap, digits, Latin letters) or no Hebrew letter."""
    word = token.strip(_EDGE)
    if not word or _EDITORIAL.search(word) or not _HEBREW.search(word):
        return ""
    return word


def eligible(word: str) -> bool:
    """A word the sample may draw: three letters or more, one word, not a function word."""
    key = lookup_key(word)
    if len(key) < MIN_LETTERS or " " in key:
        return False
    keys = word_keys(word)
    return bool(keys) and not is_function_word(keys)


def editions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """The Judaeo-Arabic digital editions: id, the document, a manuscript of it, its printed tokens."""
    out = []
    for row in conn.execute(
            "SELECT s.id, s.pgpid, s.content, (SELECT f.sys_id FROM document_fragments f WHERE f.document_id = s.pgpid "
            "ORDER BY f.sequence_order, f.id LIMIT 1) AS sys_id FROM document_sources s "
            "WHERE s.doc_relation = 'Digital Edition' AND s.language = ? ORDER BY s.id", (LANGUAGE,)):
        out.append({"source": int(row[0]), "pgpid": int(row[1]), "sys_id": str(row[3] or ""),
                    "tokens": str(row[2] or "").split()})
    return out


def occurrences(texts: list[dict[str, Any]]) -> list[tuple[int, int]]:
    """Every (edition, token) position whose word the sample may draw, in reading order."""
    return [(t, n) for t, text in enumerate(texts) for n, token in enumerate(text["tokens"])
            if eligible(clean_word(token))]


def context(tokens: list[str], n: int, width: int = CONTEXT) -> tuple[str, str]:
    """The printed words before and after position ``n`` (as printed, brackets and all)."""
    return " ".join(tokens[max(0, n - width):n]), " ".join(tokens[n + 1:n + 1 + width])


# ---------------------------------------------------------------------------
# The cards found by verb form, as the panel shows them
# ---------------------------------------------------------------------------

def blau_answer(service: Any, word: str, kinds: frozenset[str] = GRADED_KINDS) -> tuple[list[dict[str, Any]], int]:
    """Blau's cards found by verb form for the word, in the panel's order (entry, root, the stems the
    word fits, the stem a card would open on, the kind), and how many cards the word itself reached."""
    result = service.lookup(word, verb_forms=kinds, verb_form_stems=frozenset())
    found = {c.entry_id: c for c in verb_form_cards(result.candidates, service.verb_form_index())}
    out = []
    for card in result.cards:
        if card.verb_form and card.root_id in found:
            vf = found[card.root_id]
            out.append({"entry": card.root_id, "root": card.root_label or vf.root, "stems": list(vf.stems),
                        "stem": vf.stem, "kind": card.kind, "form": vf.form})
    return out, sum(1 for card in result.cards if not card.verb_form)


def friedman_answer(service: Any, word: str, kinds: frozenset[str] = GRADED_KINDS) -> tuple[list[dict[str, Any]], int]:
    """Friedman's matches found by verb form for the word, and its other matches, as ``blau_answer``."""
    result = service.lookup(word, verb_forms=kinds, verb_form_stems=frozenset())
    found = {c.entry_id: c for c in verb_form_cards(result.candidates, service.verb_form_index())}
    out = []
    for match in result.matches:
        entry = match.unit.id if match.unit.kind == "entry" else (match.unit.parent or match.unit.id)
        if match.kind and entry in found:
            vf = found[entry]
            out.append({"entry": entry, "root": match.root or vf.root, "stems": list(vf.stems),
                        "stem": vf.stem, "kind": match.kind, "form": vf.form})
    return out, sum(1 for match in result.matches if not match.kind)


def blau_cards(service: Any, word: str, kinds: frozenset[str] = GRADED_KINDS) -> list[dict[str, Any]]:
    return blau_answer(service, word, kinds)[0]


def friedman_cards(service: Any, word: str, kinds: frozenset[str] = GRADED_KINDS) -> list[dict[str, Any]]:
    return friedman_answer(service, word, kinds)[0]


def roots_to_grade(cards: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """The roots both dictionaries propose, once each (by its letters and dots), with the union of
    the stems the word fits, in the order they first appear (Blau's cards, then Friedman's)."""
    roots: dict[str, dict[str, Any]] = {}
    for name in DICTIONARIES:
        for card in cards.get(name, ()):
            key = dot_pattern(card["root"])
            entry = roots.setdefault(key, {"root": card["root"], "stems": []})
            for stem in card["stems"]:
                if stem not in entry["stems"]:
                    entry["stems"].append(stem)
    for entry in roots.values():
        entry["stems"].sort()
    return list(roots.values())


# ---------------------------------------------------------------------------
# The draw
# ---------------------------------------------------------------------------

def draw(order: Iterable[tuple[int, int]], texts: list[dict[str, Any]],
         cards_of: Callable[[str], dict[str, list[dict[str, Any]]]], quota: int = QUOTA,
         max_draws: Optional[int] = None) -> tuple[list[dict[str, Any]], Counter, int]:
    """Walk the drawn positions in ``order`` and keep each word that has a graded kind in a dictionary
    whose quota for that kind is not yet full, until every quota is. Returns the kept items, the
    count per (dictionary, kind), and how many positions were drawn."""
    counts: Counter = Counter()
    items: list[dict[str, Any]] = []
    drawn = 0
    goal = {(name, kind) for name in DICTIONARIES for kind in GRADED_KINDS}
    for t, n in order:
        if all(counts[g] >= quota for g in goal) or (max_draws is not None and drawn >= max_draws):
            break
        drawn += 1
        text = texts[t]
        word = clean_word(text["tokens"][n])
        cards = cards_of(word)
        kinds = {name: cards[name][0]["kind"] for name in DICTIONARIES if cards.get(name)}
        if not any(counts[(name, kind)] < quota for name, kind in kinds.items()):
            continue
        for name, kind in kinds.items():
            counts[(name, kind)] += 1
        left, right = context(text["tokens"], n)
        items.append({"id": f"s{len(items) + 1:04d}", "draw": drawn, "word": word, "left": left, "right": right,
                      "source": text["source"], "pgpid": text["pgpid"], "sys_id": text["sys_id"],
                      "link": BROWSE_URL + text["sys_id"] if text["sys_id"] else "",
                      "kinds": kinds, "cards": {name: cards.get(name, []) for name in DICTIONARIES},
                      "own": dict(cards.get("own") or {}), "roots": roots_to_grade(cards)})
    return items, counts, drawn


# ---------------------------------------------------------------------------
# Glosses and the grading batches
# ---------------------------------------------------------------------------

def stem_glosses(blau: Any, friedman: Any, root: str, stem: int, cache: dict) -> list[str]:
    """What the dictionaries say a root's stem means: Blau's first senses of that stem's paragraph and
    Friedman's definition of it ("<root> <stem>", the typed shortcut), without naming either."""
    key = (root, stem)
    if key not in cache:
        query, dots = f"{root} {stem_label(stem)}", dot_pattern(root)
        out: list[str] = []
        for card in blau.lookup(query, verb_forms=frozenset()).cards if blau else ():
            if card.stem == stem and dot_pattern(card.root_label or card.name) == dots:
                for sense in card.senses[:GLOSSES_PER_STEM]:
                    gloss = sense.gloss_en or sense.gloss_he
                    if gloss and gloss not in out:
                        out.append(gloss)
        for match in friedman.lookup(query, verb_forms=frozenset()).matches if friedman else ():
            if (match.stem == stem and dot_pattern(match.root) == dots and match.unit.definition
                    and match.unit.definition not in out):
                out.append(match.unit.definition[:240])
        cache[key] = out
    return cache[key]


def grading_item(item: dict[str, Any], glosses: Callable[[str, int], list[str]]) -> dict[str, Any]:
    """What the grader sees of one drawn word: no dictionary, no kind, no link."""
    return {"id": item["id"], "word": item["word"], "before": item["left"], "after": item["right"],
            "roots": [{"rid": f"{item['id']}-r{n}", "root": entry["root"],
                       "stems": [{"stem": stem_label(stem), "glosses": glosses(entry["root"], stem)}
                                 for stem in entry["stems"]]}
                      for n, entry in enumerate(item["roots"], 1)]}


def batches(items: list[dict[str, Any]], size: int = BATCH) -> list[list[dict[str, Any]]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--pgp-db", default="pgp_data/pgp.db")
    parser.add_argument("--friedman-db", default="friedman_data/friedman.db")
    parser.add_argument("--blau-db", default="blau_data/blau.db")
    parser.add_argument("--out", required=True, help="the folder for the sample (private repository)")
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--quota", type=int, default=QUOTA)
    args = parser.parse_args(argv)
    os.environ["GENIZAH_BLAU_DB_PATH"] = args.blau_db
    os.environ["GENIZAH_FRIEDMAN_DB_PATH"] = args.friedman_db
    os.environ["FRIEDMAN_DICTIONARY_ENABLED"] = "1"
    from shared.blau_service import BlauService
    from shared.friedman_service import FriedmanService
    blau, friedman = BlauService(args.blau_db), FriedmanService(args.friedman_db)
    if not blau.is_available() or not friedman.is_available():
        print(f"a dictionary is not available: blau={blau.unavailable_reason!r} friedman={friedman.status!r}",
              file=sys.stderr)
        return 2
    conn = sqlite3.connect(f"file:{args.pgp_db}?mode=ro", uri=True)
    texts = editions(conn)
    order = occurrences(texts)
    random.Random(args.seed).shuffle(order)
    cache: dict[str, dict[str, list[dict[str, Any]]]] = {}

    def cards_of(word: str) -> dict[str, Any]:
        if word not in cache:
            (b_cards, b_own), (f_cards, f_own) = blau_answer(blau, word), friedman_answer(friedman, word)
            cache[word] = {"blau": b_cards, "friedman": f_cards, "own": {"blau": b_own, "friedman": f_own}}
        return cache[word]

    items, counts, drawn = draw(order, texts, cards_of, args.quota)
    gloss_cache: dict = {}
    graded = [grading_item(item, lambda root, stem: stem_glosses(blau, friedman, root, stem, gloss_cache))
              for item in items]
    out = Path(args.out)
    (out / "batches").mkdir(parents=True, exist_ok=True)
    _write_jsonl(out / "sample.jsonl", items)
    for n, batch in enumerate(batches(graded), 1):
        _write_jsonl(out / "batches" / f"batch_{n:02d}.jsonl", batch)
    meta = {"draw_version": DRAW_VERSION, "verb_forms_version": VERB_FORMS_VERSION, "seed": args.seed,
            "quota": args.quota, "graded_kinds": sorted(GRADED_KINDS), "language": LANGUAGE,
            "editions": len(texts), "eligible_positions": len(order), "drawn": drawn, "kept": len(items),
            "counts": {f"{name}:{kind}": counts[(name, kind)] for name in DICTIONARIES for kind in sorted(GRADED_KINDS)},
            "distinct_words": len({item["word"] for item in items}),
            "roots_to_grade": sum(len(item["roots"]) for item in items)}
    (out / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(json.dumps(meta, ensure_ascii=False, indent=1))
    short = [k for k, v in meta["counts"].items() if v < args.quota]
    if short:
        print(f"short of the quota: {short}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
