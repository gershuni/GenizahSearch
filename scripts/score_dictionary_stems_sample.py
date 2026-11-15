# -*- coding: utf-8 -*-
"""Score the graded click sample of the cards found by verb form (the private dictionary lookup plan,
section 6.1, decided 2026-11-11 before any draw). Reads the draw (``sample.jsonl``, written by
``scripts/draw_dictionary_stems_sample.py``), the grader's answers (``grading/answers/*.json``) and the
second review (``review.json``: a root id's verdict that replaces the grader's), and reports, for each
dictionary and each graded kind:

- **wrong-only**: words whose cards found by verb form name no right root (none graded right, and not
  every one cannot-tell), with the 95% upper bound (Wilson);
- **cannot tell**: words every one of whose roots is cannot-tell; they are not gradable;
- **first right**: the first card's root right;
- **stem**: of the words whose root is right and whose right card would open on a stem (the word fits
  one listed stem), the share where that stem is right; the words that fit several stems open on the
  whole entry and are counted apart;
- the same split by whether the dictionary also found the word itself.

The pass rule: a kind is shown in a dictionary when at most 5 in 100 of its
gradable words are wrong-only and cannot-tell words are at most 25% of those drawn; a kind that passes
opens on the stem only if the stem is right in at least 85% of its words whose root is right.

Two rulings after the first results (recall counts more than precision): a word is a win
when any root proposed for it is right, whichever dictionary proposed it (``pooled``, the default; the
reader sees both tabs); and Hebrew and Aramaic words are irrelevant (``excluded.json`` in the sample
folder: item id -> why; they are left out of every count). ``--strict`` scores each dictionary on its
own cards only.

    python -m scripts.score_dictionary_stems_sample <sample folder> [--out score.json] [--strict]
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, Iterable, Optional

from shared.ja_roots import dot_pattern, stem_label

#: 2: pooled wins and excluded words.
SCORE_VERSION = 2
DICTIONARIES = ("blau", "friedman")
GRADED_KINDS = ("A", "B", "D")
MAX_WRONG_ONLY = 0.05
MAX_CANNOT_TELL = 0.25
MIN_STEM_RIGHT = 0.85


def wilson_upper(k: int, n: int, z: float = 1.959964) -> float:
    """The upper end of the 95% Wilson interval for k successes in n."""
    if n == 0:
        return 1.0
    p = k / n
    centre = p + z * z / (2 * n)
    spread = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return min(1.0, (centre + spread) / (1 + z * z / n))


def verdicts(sample: Iterable[dict[str, Any]], answers: Iterable[dict[str, Any]],
             review: Optional[dict[str, dict[str, str]]] = None) -> dict[str, dict[str, dict[str, str]]]:
    """Per item id, per root (its letters and dots), the verdict {'root': ..., 'stem': ...}: the
    grader's, replaced by the second review where it gives one (by root id)."""
    by_rid = {r["rid"]: r for item in answers for r in item.get("roots", ())}
    review = review or {}
    out: dict[str, dict[str, dict[str, str]]] = {}
    for item in sample:
        roots: dict[str, dict[str, str]] = {}
        for n, entry in enumerate(item["roots"], 1):
            rid = f"{item['id']}-r{n}"
            verdict = dict(review.get(rid) or by_rid.get(rid) or {})
            if verdict:
                roots[dot_pattern(entry["root"])] = {"root": verdict.get("root", ""), "stem": verdict.get("stem", "")}
        out[item["id"]] = roots
    return out


def judge(item: dict[str, Any], name: str, roots: dict[str, dict[str, str]],
          pooled: bool = True) -> Optional[dict[str, Any]]:
    """One word in one dictionary: None when not graded (no kind there, or a root not graded yet).
    ``pooled``: the word is a win when any root proposed for it, by either dictionary, is right."""
    cards = item["cards"].get(name) or []
    if not cards or name not in item.get("kinds", {}):
        return None
    graded = [roots.get(dot_pattern(card["root"])) for card in cards]
    if any(g is None for g in graded):
        return None
    right = [card for card, g in zip(cards, graded) if g["root"] == "right"]
    cannot = all(g["root"] == "cannot_tell" for g in graded)
    win = bool(right)
    if pooled:
        everything = [roots.get(dot_pattern(entry["root"])) for entry in item["roots"]]
        if any(g is None for g in everything):
            return None   # a root the other dictionary proposed is not graded yet: the word waits
        win = any(g["root"] == "right" for g in everything)
        cannot = all(g["root"] == "cannot_tell" for g in everything)
    out = {"kind": item["kinds"][name], "cannot_tell": cannot, "wrong_only": not win and not cannot,
           "first_right": graded[0]["root"] == "right", "root_right": bool(right),
           "own": bool((item.get("own") or {}).get(name)), "stem_right": False, "stem_whole": False}
    if right:
        card = right[0]
        stem = roots[dot_pattern(card["root"])]["stem"]
        out["stem_whole"] = not card["stem"]
        out["stem_right"] = bool(card["stem"]) and stem == stem_label(card["stem"])
    return out


def tally(judged: list[dict[str, Any]]) -> dict[str, Any]:
    drawn = len(judged)
    cannot = sum(j["cannot_tell"] for j in judged)
    gradable = drawn - cannot
    wrong = sum(j["wrong_only"] for j in judged)
    root_right = sum(j["root_right"] for j in judged)
    stem_right = sum(j["stem_right"] for j in judged)
    stem_whole = sum(j["stem_whole"] for j in judged)
    # The stem is judged where a card would open on one: a word that fits several stems opens on the
    # whole entry whatever the switch says, so it neither helps nor counts against the stem.
    guessed = root_right - stem_whole
    out = {"drawn": drawn, "gradable": gradable, "cannot_tell": cannot, "wrong_only": wrong,
           "wrong_only_pct": round(100.0 * wrong / gradable, 1) if gradable else None,
           "wrong_only_upper95_pct": round(100.0 * wilson_upper(wrong, gradable), 1) if gradable else None,
           "cannot_tell_pct": round(100.0 * cannot / drawn, 1) if drawn else None,
           "first_right": sum(j["first_right"] for j in judged), "root_right": root_right,
           "stem_guessed": guessed, "stem_right": stem_right, "stem_whole": stem_whole,
           "stem_right_pct": round(100.0 * stem_right / guessed, 1) if guessed else None}
    out["passes"] = bool(gradable) and wrong / gradable <= MAX_WRONG_ONLY and cannot / drawn <= MAX_CANNOT_TELL
    out["opens_on_stem"] = out["passes"] and bool(guessed) and stem_right / guessed >= MIN_STEM_RIGHT
    return out


def score(sample: list[dict[str, Any]], answers: list[dict[str, Any]],
          review: Optional[dict[str, dict[str, str]]] = None, excluded: Optional[dict[str, str]] = None,
          pooled: bool = True) -> dict[str, Any]:
    """The tallies per dictionary and kind; ``excluded`` items (id -> why) are left out of every count."""
    excluded = excluded or {}
    kept = [item for item in sample if item["id"] not in excluded]
    roots = verdicts(kept, answers, review)
    result: dict[str, Any] = {"score_version": SCORE_VERSION, "items": len(kept), "excluded": len(sample) - len(kept),
                              "pooled": pooled,
                              "graded_items": sum(1 for item in kept if roots[item["id"]]
                                                  and len(roots[item["id"]]) == len(item["roots"]))}
    for name in DICTIONARIES:
        judged = [j for item in kept if (j := judge(item, name, roots[item["id"]], pooled)) is not None]
        result[name] = {}
        for kind in GRADED_KINDS:
            mine = [j for j in judged if j["kind"] == kind]
            result[name][kind] = tally(mine)
            result[name][kind]["also_found_itself"] = tally([j for j in mine if j["own"]])
            result[name][kind]["found_only_by_verb_form"] = tally([j for j in mine if not j["own"]])
    return result


def _jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def load(folder: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, str]],
                                dict[str, str]]:
    """The sample, the grader's answers, the second review and the excluded items of a sample folder."""
    sample = _jsonl(folder / "sample.jsonl")
    answers = [item for path in sorted((folder / "grading" / "answers").glob("batch_*.json"))
               if ".rejected" not in path.name
               for item in json.loads(path.read_text(encoding="utf-8"))["items"]]

    def optional(name: str) -> dict:
        path = folder / name
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    return sample, answers, optional("review.json"), optional("excluded.json")


def summary(result: dict[str, Any]) -> str:
    lines = [f"items {result['items']} ({result['excluded']} excluded), fully graded {result['graded_items']}, "
             f"{'pooled: a right root from either dictionary is a win' if result['pooled'] else 'strict: each dictionary on its own cards'}"]
    for name in DICTIONARIES:
        for kind in GRADED_KINDS:
            t = result[name][kind]
            lines.append(
                f"{name:8s} {kind}: {t['gradable']:3d} gradable, wrong-only {t['wrong_only']} ({t['wrong_only_pct']}%, "
                f"95% up to {t['wrong_only_upper95_pct']}%), cannot tell {t['cannot_tell']} ({t['cannot_tell_pct']}%), "
                f"stem right {t['stem_right_pct']}% (whole entry {t['stem_whole']}) -> "
                f"{'PASS' if t['passes'] else 'FAIL'}{', opens on the stem' if t['opens_on_stem'] else ''}")
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("folder")
    parser.add_argument("--out", default="")
    parser.add_argument("--strict", action="store_true", help="each dictionary on its own cards only")
    args = parser.parse_args(argv)
    result = score(*load(Path(args.folder)), pooled=not args.strict)
    print(summary(result))
    if args.out:
        Path(args.out).write_text(json.dumps(result, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
