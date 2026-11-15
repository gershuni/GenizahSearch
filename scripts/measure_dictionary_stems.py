# -*- coding: utf-8 -*-
"""Measure the generated verb forms against the dictionaries' own answers (the private dictionary
lookup plan, section 6: the gate of PR 2). Nothing here ships and nothing is changed: the script
reads the two sidecars and reports.

The forms are ``shared/ja_verb_forms.py``'s: for every (root, stem) pair a dictionary lists
(``FriedmanService.stem_pairs``, ``BlauService.stem_pairs``), every way the stem may be written. A
word reaches a pair when one of its keys (its own key, then its clitics peeled: ``candidate_stems``)
is one of those forms; the pair's entry is then a card "found by verb form".

What is reported, for each dictionary (plan section 6):

1. **Reach on Friedman's index of forms**, the book's own list of forms with their roots, the index
   route switched off so the index is the answer key and not a source. A card is right when it
   names the index's root: by its dotted spelling in Friedman, by its letters in Blau (whose roots
   the index does not spell). ``reach`` = a right card among the cards, ``first`` = the first card
   right (the order of plan 4.4: the word's own key before a peeled one, a prefixed stem -- III, V
   to X -- before I, II and IV, a longer form before a shorter), ``wrong_only`` = cards and none
   right, ``nothing`` = no card. On every form, and on the forms whose root the dictionary lists in
   some stem. Today's lookup on the same forms is the baseline (Friedman: its heads and base forms;
   Blau: its lookup), the Friedman baseline split by whether a sub-unit's head matched.
2. **Quotations under a listed stem** that hold a generated form of that very pair, and for those
   that do not, why (``miss_reasons``): a form of another stem of the root, a word of the root that
   no rule generates (its shape counted), no word of the root at all; and the flags: the stem row is
   one whose column and printed head disagree (Blau), the citation sits under a sub-entry.
3. **The share of right hits by stem, by key (own or peeled) and by form length.**
4. **The offer rate on running text**: the distinct words of both dictionaries' quotations, how
   many get a card by verb form and how many cards.
5. **The mutation runs**, each with the numbers that moved: weak-root rules off, two-letter forms
   on, stem VIII assimilation off (on the forms a stem VIII card got right), and the listed-pair
   filter off (every root of the dictionary in every stem).
6. **Build time and memory** of each service's ``verb_form_index`` (plan 4.3).

The thresholds are not fixed here: they are agreed on these numbers (plan section 6,
an ``/effort xhigh`` call), and the graded click sample is the only measure of wrong cards on real
pages. Output: a summary on stdout; ``--out`` writes the full result as JSON (numbers and shapes
only; ``--samples N`` adds up to N example forms per bucket, dictionary text, for the private
repository only).

Usage::

    python scripts/measure_dictionary_stems.py
    python scripts/measure_dictionary_stems.py --out <private repo>/docs/plans/dictionary_stems_gate.json
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import tracemalloc
from collections import Counter, defaultdict
from typing import Any, Callable, Iterable, Optional

from shared.blau_normalize import candidate_stems, normalize_ja
from shared.ja_expression import lookup_key
from shared.ja_roots import StemPair, dot_pattern, stem_numbers
from shared.ja_verb_forms import PREFIXED, VERB_FORMS_VERSION, VerbFormCard, stem_forms, verb_form_cards  # noqa: F401 - PREFIXED: importable from here, as before

#: 2 (PR 3): the cards are the lookup's own (``verb_form_cards``), which count a hit once per entry,
#: stem, key and length (two paragraphs of one stem, or an entry filed under two roots, were two
#: hits): the shares by stem, key and length moved by up to three points, nothing else did.
MEASURE_VERSION = 2
#: The letters a root may lose or change in writing (hollow, final weak, first ו or א).
WEAK_LETTERS = "ויא"


# ---------------------------------------------------------------------------
# The words, the cards
# ---------------------------------------------------------------------------

def word_keys(word: str) -> list[str]:
    """A word's keys: its own (``lookup_key``) first, then its clitics peeled (``candidate_stems``)."""
    own = lookup_key(word)
    if not own or " " in own:
        return [own] if own else []
    return list(dict.fromkeys([own, *candidate_stems(own)]))


#: One entry a word reached by verb form, with the hits that reached it: the lookup's own card
#: (``shared.ja_verb_forms.VerbFormCard``, PR 3), so the gate measures what the panel shows.
Card = VerbFormCard


def restricted_index(pairs: Iterable[StemPair], wanted: set[str], **rules: Any) -> dict[str, list[StemPair]]:
    """The verb form index (``shared.ja_verb_forms.verb_form_index``) kept to the keys ``wanted``: the
    same answer for those keys, in a fraction of the memory (the filter-off run has every root in
    every stem)."""
    index: dict[str, list[StemPair]] = defaultdict(list)
    for pair in pairs:
        for form in stem_forms(pair.root_key, pair.stem, **rules) & wanted:
            index[form].append(pair)
    return index


def cards_for(keys: list[str], index: dict[str, list[StemPair]]) -> list[Card]:
    """The entries the keys reach, in the order plan 4.4 shows them (stable on first reach):
    ``shared.ja_verb_forms.verb_form_cards``, the lookup's own."""
    return verb_form_cards(keys, index)


def has_root_letters(key: str, root_key: str) -> Optional[bool]:
    """Does ``key`` hold the root's strong letters (not ו, י, א) in order? None when the root has
    fewer than two strong letters (no word can be told to be of it)."""
    strong = [ch for ch in root_key if ch not in WEAK_LETTERS]
    if len(strong) < 2:
        return None
    position = 0
    for ch in key:
        if position < len(strong) and ch == strong[position]:
            position += 1
    return position == len(strong)


def shape(key: str, root_key: str) -> str:
    """The key with the root's letters, where they first fall in order, written 1, 2, 3 (מ1א23ה):
    a form no rule generates, counted by its shape, carries no dictionary text."""
    out, position = [], 0
    for ch in key:
        if position < len(root_key) and ch == root_key[position]:
            out.append(str(position + 1))
            position += 1
        else:
            out.append(ch)
    return "".join(out)


# ---------------------------------------------------------------------------
# Reading the sidecars (read-only)
# ---------------------------------------------------------------------------

def _ro(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def friedman_answers(conn: sqlite3.Connection) -> dict[str, set[tuple[str, str]]]:
    """Friedman's index of forms: each form (its spelling) and the roots it gives, (dotted, key)."""
    answers: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for row in conn.execute("SELECT form, root_dotted, root_key FROM index_forms "
                            "WHERE root_dotted IS NOT NULL AND root_dotted <> '' ORDER BY id"):
        answers[str(row["form"])].add((str(row["root_dotted"]), str(row["root_key"])))
    return dict(answers)


def running_text_words(conns: Iterable[sqlite3.Connection]) -> set[str]:
    """The distinct words of the dictionaries' quotations and heads (``tokens``, unpeeled keys)."""
    words: set[str] = set()
    for conn in conns:
        words.update(str(r[0]) for r in conn.execute("SELECT DISTINCT key FROM tokens WHERE peeled = 0"))
    return {w for w in words if w and " " not in w}


# ---------------------------------------------------------------------------
# The measures
# ---------------------------------------------------------------------------

def _rates(counter: Counter, total: int) -> dict[str, Any]:
    out: dict[str, Any] = {"n": total}
    for name in ("reach", "first", "wrong_only", "nothing"):
        out[name] = counter[name]
        out[name + "_pct"] = round(100.0 * counter[name] / total, 1) if total else 0.0
    return out


def score_forms(answers: dict[str, set[tuple[str, str]]], index: dict[str, list[StemPair]],
                right: Callable[[Card, set[tuple[str, str]]], bool], listed: Callable[[set[tuple[str, str]]], bool],
                samples: int = 0) -> dict[str, Any]:
    """Item 1 and item 3 of plan section 6 for one dictionary's index."""
    counts = {"all": Counter(), "listed": Counter()}
    totals = Counter()
    by: dict[str, Counter] = {"stem": Counter(), "stem_right": Counter(), "key": Counter(), "key_right": Counter(),
                              "length": Counter(), "length_right": Counter()}
    examples: dict[str, list[str]] = defaultdict(list)
    for form, roots in answers.items():
        cards = cards_for(word_keys(form), index)
        good = [card for card in cards if right(card, roots)]
        subsets = ["all"] + (["listed"] if listed(roots) else [])
        for subset in subsets:
            totals[subset] += 1
            if good:
                counts[subset]["reach"] += 1
            if cards and right(cards[0], roots):
                counts[subset]["first"] += 1
            if cards and not good:
                counts[subset]["wrong_only"] += 1
            if not cards:
                counts[subset]["nothing"] += 1
        if samples:
            bucket = "right" if good else ("wrong_only" if cards else "nothing")
            if len(examples[bucket]) < samples:
                examples[bucket].append(f"{form} -> {[c.root for c in cards[:3]]}")
        for card in cards:
            ok = right(card, roots)
            for stem, own, length in card.hits:
                for name, value in (("stem", stem), ("key", "own" if own else "peeled"),
                                    ("length", min(length, 7))):
                    by[name][value] += 1
                    by[name + "_right"][value] += ok
    shares = {name: {str(k): round(100.0 * by[name + "_right"][k] / v, 1) for k, v in sorted(by[name].items())}
              for name in ("stem", "key", "length")}
    hits = {name: {str(k): v for k, v in sorted(by[name].items())} for name in ("stem", "key", "length")}
    out = {"all": _rates(counts["all"], totals["all"]), "listed": _rates(counts["listed"], totals["listed"]),
           "right_share_by": shares, "hits_by": hits}
    if samples:
        out["examples"] = dict(examples)
    return out


def score_baseline(answers: dict[str, set[tuple[str, str]]], cards_of: Callable[[str], list[tuple[str, str, bool]]],
                   right: Callable[[tuple[str, str], set[tuple[str, str]]], bool],
                   listed: Callable[[set[tuple[str, str]]], bool]) -> dict[str, Any]:
    """Today's lookup on the same forms: ``cards_of(form)`` gives (root dotted, root key, reached by a
    sub-unit's head) per card, in the lookup's order. Its wrong-only (plan section 5: "not understood
    yet") is split: a card reached through a sub-unit's head; an entry filed under the word itself
    (its "root" is one of the word's keys: a noun entered under its own spelling, which the index
    files under a verbal root -- not a wrong card, a different filing); the rest."""
    counts = {"all": Counter(), "listed": Counter()}
    totals = Counter()
    split = Counter()
    for form, roots in answers.items():
        cards = cards_of(form)
        good = [c for c in cards if right((c[0], c[1]), roots)]
        for subset in ["all"] + (["listed"] if listed(roots) else []):
            totals[subset] += 1
            counts[subset]["reach"] += bool(good)
            counts[subset]["first"] += bool(cards) and right((cards[0][0], cards[0][1]), roots)
            counts[subset]["wrong_only"] += bool(cards) and not good
            counts[subset]["nothing"] += not cards
        if cards and not good:
            keys = set(word_keys(form))
            if any(c[1] in keys for c in cards):
                split["entry_filed_under_the_word_itself"] += 1
            elif any(c[2] for c in cards):
                split["through_a_sub_unit_head"] += 1
            else:
                split["another_root"] += 1
    return {"all": _rates(counts["all"], totals["all"]), "listed": _rates(counts["listed"], totals["listed"]),
            "wrong_only_split": dict(split)}


def quotation_coverage(items: Iterable[tuple[StemPair, str, dict[str, bool]]], pair_stems: dict[str, set[int]],
                       shapes: int = 15) -> dict[str, Any]:
    """Item 2: (pair, quotation text, flags) for every quotation under a listed stem. A quotation holds
    the pair when one of its words' keys is a generated form of it; else the first reason that fits:
    a form of another stem of the root (``other_stem``), a word of the root no rule generates
    (``root_word_not_generated``, its shape counted), no word of the root (``no_root_word``), a root
    whose words cannot be told (``weak_root_unknown``). Each miss also counts its flags."""
    total = held = 0
    reasons, flags, shape_counts = Counter(), Counter(), Counter()
    for pair, text, item_flags in items:
        keys = {k for word in text.split() for k in word_keys(word)}
        if not keys:
            continue
        total += 1
        if keys & stem_forms(pair.root_key, pair.stem):
            held += 1
            continue
        others = pair_stems.get(pair.root_key, set()) | set(range(1, 11))
        if any(keys & stem_forms(pair.root_key, other) for other in others if other != pair.stem):
            reason = "other_stem"
        else:
            of_root = [k for k in keys if has_root_letters(k, pair.root_key)]
            if has_root_letters("", pair.root_key) is None:
                reason = "weak_root_unknown"
            elif of_root:
                reason = "root_word_not_generated"
                shape_counts.update(shape(k, pair.root_key) for k in of_root)
            else:
                reason = "no_root_word"
        reasons[reason] += 1
        for name, on in item_flags.items():
            if on:
                flags[name] += 1
    return {"quotations": total, "held": held, "held_pct": round(100.0 * held / total, 1) if total else 0.0,
            "miss_reasons": dict(reasons.most_common()), "miss_flags": dict(flags.most_common()),
            "missing_shapes": dict(shape_counts.most_common(shapes))}


def offer_rate(words: set[str], index: dict[str, list[StemPair]]) -> dict[str, Any]:
    """Item 4: of the distinct running-text words, how many get a card by verb form, and how many."""
    offered = cards_total = one_root = 0
    for word in words:
        cards = cards_for(word_keys(word), index)
        if cards:
            offered += 1
            cards_total += len(cards)
            one_root += len({c.root_key for c in cards}) == 1
    return {"words": len(words), "offered": offered,
            "offered_pct": round(100.0 * offered / len(words), 1) if words else 0.0,
            "cards_per_offered_word": round(cards_total / offered, 2) if offered else 0.0,
            "one_root_pct_of_offered": round(100.0 * one_root / offered, 1) if offered else 0.0}


def build_cost(build: Callable[[], dict]) -> dict[str, Any]:
    """Item 6: the time and the memory one service's ``verb_form_index`` takes to build."""
    tracemalloc.start()
    started = time.perf_counter()
    index = build()
    seconds = time.perf_counter() - started
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {"seconds": round(seconds, 2), "keys": len(index), "held_mb": round(current / 1e6, 1),
            "peak_mb": round(peak / 1e6, 1)}


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

def _friedman_right(card: Card, roots: set[tuple[str, str]]) -> bool:
    return any(dot_pattern(card.root) == dot_pattern(dotted) for dotted, _key in roots)


def _blau_right(card: Card, roots: set[tuple[str, str]]) -> bool:
    return any(card.root_key == normalize_ja(dotted) for dotted, _key in roots)


def measure(friedman_db: str, blau_db: str, samples: int = 0) -> dict[str, Any]:
    from shared.blau_service import BlauService
    from shared.friedman_service import FriedmanService

    friedman, blau = FriedmanService(friedman_db), BlauService(blau_db)
    if not friedman.is_ready() or not blau.is_ready():
        raise SystemExit(f"a sidecar is not served: friedman {friedman.unavailable_reason!r}, "
                         f"blau {blau.unavailable_reason!r}")
    fconn, bconn = _ro(friedman_db), _ro(blau_db)
    result: dict[str, Any] = {"measure_version": MEASURE_VERSION, "verb_forms_version": VERB_FORMS_VERSION}

    result["build"] = {"friedman": build_cost(FriedmanService(friedman_db).verb_form_index),
                       "blau": build_cost(BlauService(blau_db).verb_form_index)}
    pairs = {"friedman": [p for p in friedman._stems() if len(p.root_key) == 3 and " " not in p.root_key],
             "blau": [p for p in blau._stems()[0] if len(p.root_key) == 3 and " " not in p.root_key]}
    result["pairs"] = {name: {"pairs": len(found), "roots": len({p.root_key for p in found})}
                       for name, found in pairs.items()}

    answers = friedman_answers(fconn)
    words = running_text_words((fconn, bconn))
    wanted = {k for form in answers for k in word_keys(form)} | {k for w in words for k in word_keys(w)}
    listed_dotted = {dot_pattern(p.root) for p in pairs["friedman"]}
    listed_keys = {p.root_key for p in pairs["blau"]}
    rules = {"friedman": (_friedman_right, lambda roots: any(dot_pattern(d) in listed_dotted for d, _k in roots)),
             "blau": (_blau_right, lambda roots: any(normalize_ja(d) in listed_keys for d, _k in roots))}
    result["answer_key"] = {"forms": len(answers), "running_text_words": len(words)}

    indexes = {name: restricted_index(found, wanted) for name, found in pairs.items()}
    for name in ("friedman", "blau"):
        right, listed = rules[name]
        result[name] = {"forms": score_forms(answers, indexes[name], right, listed, samples),
                        "offer": offer_rate(words, indexes[name])}

    # Today's lookup as the baseline (no index route in either).
    def friedman_today(form: str) -> list[tuple[str, str, bool]]:
        out, seen = [], set()
        for key in word_keys(form):
            for row in fconn.execute("SELECT u.id, u.kind, u.parent FROM units u JOIN unit_keys k ON k.unit_id = u.id "
                                     "WHERE k.key = ? ORDER BY u.ord", (key,)):
                entry = row["id"] if row["kind"] == "entry" else row["parent"]
                if entry in seen:
                    continue
                seen.add(entry)
                for r in fconn.execute("SELECT dotted FROM unit_roots WHERE unit_id = ?", (entry,)):
                    out.append((str(r[0]), lookup_key(str(r[0])), row["kind"] != "entry"))
        return out

    def blau_today(form: str) -> list[tuple[str, str, bool]]:
        # The lookup without the cards found by verb form (they are what is measured, not the
        # baseline), whatever BLAU_VERB_FORM_KINDS says.
        return [(card.root_label or card.name, normalize_ja(card.root_label or card.name), bool(card.sub_entry))
                for card in blau.lookup(form, verb_forms=frozenset()).cards]

    result["friedman"]["baseline"] = score_baseline(
        answers, friedman_today, lambda c, roots: any(dot_pattern(c[0]) == dot_pattern(d) for d, _k in roots),
        rules["friedman"][1])
    result["blau"]["baseline"] = score_baseline(
        answers, blau_today, lambda c, roots: any(c[1] == normalize_ja(d) for d, _k in roots), rules["blau"][1])

    # Item 2: quotations under a listed stem.
    stems_of: dict[str, dict[str, set[int]]] = {name: defaultdict(set) for name in pairs}
    for name, found in pairs.items():
        for p in found:
            stems_of[name][p.root_key].add(p.stem)

    def friedman_quotes() -> Iterable[tuple[StemPair, str, dict[str, bool]]]:
        for p in pairs["friedman"]:
            for row in fconn.execute("SELECT quote FROM quotations WHERE unit_id = ?", (p.unit_id,)):
                yield p, str(row[0] or ""), {}

    disagree = set()
    for row in bconn.execute("SELECT id, binyan, head_json FROM stems"):
        column = set(stem_numbers(row["binyan"]))
        try:
            segments = (json.loads(row["head_json"] or "{}") or {}).get("segments") or []
        except ValueError:
            segments = []
        printed = {n for seg in segments if seg.get("text_type") == "binyan" for n in stem_numbers(seg.get("text"))}
        if column and printed and column != printed:
            disagree.add(str(row["id"]))

    def blau_quotes() -> Iterable[tuple[StemPair, str, dict[str, bool]]]:
        for p in pairs["blau"]:
            for row in bconn.execute("SELECT c.ja_text, f.sub_entry_id FROM citations c JOIN forms f ON f.id = c.form_id "
                                     "WHERE f.stem_id = ? AND c.ja_text IS NOT NULL AND c.ja_text <> ''", (p.unit_id,)):
                yield p, str(row[0]), {"column_disagrees_with_head": p.unit_id in disagree,
                                       "under_a_sub_entry": bool(row[1])}

    result["friedman"]["quotations"] = quotation_coverage(friedman_quotes(), stems_of["friedman"])
    result["blau"]["quotations"] = quotation_coverage(blau_quotes(), stems_of["blau"])
    result["blau"]["stem_rows_disagreeing"] = len(disagree)

    # Item 5: the mutation runs, on the forms whose root the dictionary lists.
    every_root = {
        "friedman": [StemPair(r, lookup_key(r), n, "", e) for e, roots in friedman._entry_roots.items() for r in roots
                     if len(lookup_key(r)) == 3 for n in range(1, 11)],
        "blau": [StemPair(_label, normalize_ja(_label), n, "", rid)
                 for rid, _label in ((str(r["id"]), str(r["name_ja"]).split("|")[0].strip())
                                     for r in bconn.execute("SELECT id, name_ja FROM roots"))
                 if len(normalize_ja(_label)) == 3 for n in range(1, 11)],
    }
    mutations: dict[str, Any] = {}
    for name in ("friedman", "blau"):
        right, listed = rules[name]
        runs = {
            "weak_rules_off": restricted_index(pairs[name], wanted, weak=False),
            "two_letter_forms_on": restricted_index(
                pairs[name], wanted | {k for form in answers for k in word_keys(form) if len(k) == 2}, min_letters=2),
            "listed_pair_filter_off": restricted_index(every_root[name], wanted),
        }
        mutations[name] = {run: score_forms(answers, index, right, listed)["listed"] for run, index in runs.items()}
        # Stem VIII assimilation, on the forms a stem VIII card got right with the rules on.
        viii = {form: roots for form, roots in answers.items()
                if any(right(card, roots) and any(stem == 8 for stem, _own, _len in card.hits)
                       for card in cards_for(word_keys(form), indexes[name]))}
        off = restricted_index(pairs[name], wanted, assimilation=False)
        mutations[name]["stem_viii_set"] = {
            "forms": len(viii),
            "on": score_forms(viii, indexes[name], right, lambda _r: True)["all"],
            "assimilation_off": score_forms(viii, off, right, lambda _r: True)["all"],
        }
    result["mutations"] = mutations
    return result


def summary(result: dict[str, Any]) -> str:
    lines = [f"dictionary stems gate, measure v{result['measure_version']}, verb forms v{result['verb_forms_version']}",
             f"answer key: {result['answer_key']['forms']} Friedman index forms; "
             f"{result['answer_key']['running_text_words']} running-text words"]
    for name in ("friedman", "blau"):
        r = result[name]
        build = result["build"][name]
        lines.append(f"\n== {name}: {result['pairs'][name]['pairs']} pairs over {result['pairs'][name]['roots']} roots; "
                     f"index {build['keys']} keys, {build['seconds']} s, held {build['held_mb']} MB (peak {build['peak_mb']})")
        for subset in ("all", "listed"):
            f, b = r["forms"][subset], r["baseline"][subset]
            lines.append(f"  {subset:6} n={f['n']}: reach {f['reach_pct']}%  first {f['first_pct']}%  "
                         f"wrong-only {f['wrong_only_pct']}%  nothing {f['nothing_pct']}%   | today: reach {b['reach_pct']}%  "
                         f"wrong-only {b['wrong_only_pct']}%")
        lines.append(f"  today's wrong-only split: {r['baseline']['wrong_only_split']}")
        lines.append(f"  right share by stem: {r['forms']['right_share_by']['stem']}")
        lines.append(f"  right share by key: {r['forms']['right_share_by']['key']}; by length: "
                     f"{r['forms']['right_share_by']['length']}")
        q = r["quotations"]
        lines.append(f"  quotations under a listed stem holding its form: {q['held']} of {q['quotations']} ({q['held_pct']}%)")
        lines.append(f"    misses: {q['miss_reasons']}; flags: {q['miss_flags']}")
        lines.append(f"    shapes not generated: {q['missing_shapes']}")
        o = r["offer"]
        lines.append(f"  running text: {o['offered']} of {o['words']} words get a card ({o['offered_pct']}%), "
                     f"{o['cards_per_offered_word']} cards each, one root {o['one_root_pct_of_offered']}%")
        m = result["mutations"][name]
        for run in ("weak_rules_off", "two_letter_forms_on", "listed_pair_filter_off"):
            lines.append(f"  mutation {run}: reach {m[run]['reach_pct']}%  wrong-only {m[run]['wrong_only_pct']}%  "
                         f"first {m[run]['first_pct']}%")
        v = m["stem_viii_set"]
        lines.append(f"  stem VIII set ({v['forms']} forms): reach {v['on']['reach_pct']}% -> "
                     f"{v['assimilation_off']['reach_pct']}% with assimilation off")
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    from shared.blau_service import default_sidecar_path as blau_path
    from shared.friedman_service import default_sidecar_path as friedman_path

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--friedman-db", default=friedman_path())
    parser.add_argument("--blau-db", default=blau_path())
    parser.add_argument("--out", help="write the full result as JSON here")
    parser.add_argument("--samples", type=int, default=0,
                        help="example forms per bucket in the JSON (dictionary text: private repository only)")
    args = parser.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8")
    result = measure(args.friedman_db, args.blau_db, args.samples)
    print(summary(result))
    if args.out:
        with open(args.out, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(result, handle, ensure_ascii=False, indent=1)
        print(f"\nwritten: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
