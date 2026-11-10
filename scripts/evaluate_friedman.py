#!/usr/bin/env python3
"""Evaluate the Friedman dictionary's search before any release (plan step 5).

Two kinds of measure, both READ-ONLY on ``friedman_data/friedman.db``:

* **Automatic**, with the book itself as the truth. Every multiword head key (an expression
  the book prints as a head) must be found by the expression search in its own head; the
  book's own quotations under that head are searched for the expression at every gap; every
  form in the book's index must reach an entry of its root by the single-word lookup; a head
  key shared by several entries must bring them all. These say what the machinery misses.
  They cannot say whether a hit is RIGHT: that is the scholar's.
* **The scholar's sheet**: a stratified random sample (fixed seed) of the cases the plan names,
  with the text, the marked words, the printed page and the PDF link, and two empty columns:
  ``verdict`` and ``note``. ``grade`` reads the filled sheet back and gives the rates for
  missed expressions and false suggestions, separately from the single-word lookup.

    python scripts/evaluate_friedman.py run [--per-stratum 12] [--seed 1]
        -> friedman_data/evaluation/report.json, report.md, sheet.csv, sheet_guide.md
    python scripts/evaluate_friedman.py grade [--sheet friedman_data/evaluation/sheet.csv]
        -> friedman_data/evaluation/grades.json; exit 2 when the sheet is not graded

No threshold is decided here: the numbers are reported, the decision is the owner's. No
production coverage claim follows from them.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import sqlite3
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.friedman_service import (  # noqa: E402
    DEFAULT_GAP, ENV_ENABLED, MAX_EXPRESSION_HITS, MAX_EXPRESSION_WORDS, MAX_GAP, MAX_MATCHES,
    SCOPE_HEAD, SCOPE_QUOTATION, FriedmanService, head_scope_map, lookup_key, ordered_match, word_keys,
)

EVALUATION_DIR = ROOT / "friedman_data" / "evaluation"
VIEWER_BASE = "https://ja.genizah.org/JA_Dictionary_pdfjs/web/viewer.html?file=PDF/"

#: The strata of the scholar's sheet, each with the verdicts it takes.
POSITIVE_VERDICTS = ("correct", "false", "unsure")
MISSED_VERDICTS = ("inflection", "reordered", "extraction", "not_there", "unsure")
STRATA: dict[str, tuple[str, tuple[str, ...]]] = {
    "head_missed": ("A head key the expression search did not find in its own head", MISSED_VERDICTS),
    "quotation_missed": ("An own quotation where the head's expression was not found at the widest gap",
                         MISSED_VERDICTS),
    "discontinuous": ("An own quotation where the expression was found only with a gap", POSITIVE_VERDICTS),
    "clitic": ("An own quotation where a word was reached through a clitic alternative", POSITIVE_VERDICTS),
    "other_unit": ("A quotation of ANOTHER unit holding the expression (a suggestion)", POSITIVE_VERDICTS),
    "reversed": ("A quotation holding a two-word expression in the REVERSED order", POSITIVE_VERDICTS),
    "continuation": ("An own quotation of a unit that continues across a page", POSITIVE_VERDICTS),
    "index_missed": ("An index form whose root the lookup did not reach", MISSED_VERDICTS),
    "ambiguous_root": ("An index form the book gives several roots", POSITIVE_VERDICTS),
    "homograph": ("A head key shared by several entries", POSITIVE_VERDICTS),
}
SHEET_COLUMNS = ("case", "stratum", "search", "gap", "unit", "kind", "head", "parent_head", "where", "page",
                 "link", "text", "components", "verdict", "note")


# ---------------------------------------------------------------------------
# The automatic measures
# ---------------------------------------------------------------------------

def open_service(db_path: Optional[str]) -> tuple[FriedmanService, sqlite3.Connection]:
    os.environ[ENV_ENABLED] = "1"
    service = FriedmanService(db_path)
    if not service.is_ready():
        raise SystemExit(f"the sidecar is not ready: {service.status}")
    conn = sqlite3.connect(f"file:{service.db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return service, conn


def multiword_keys(conn: sqlite3.Connection) -> list[tuple[str, str, str]]:
    """(key, unit id, kind) for every head key of two to MAX_EXPRESSION_WORDS words."""
    out = []
    for row in conn.execute("SELECT k.key, k.unit_id, u.kind FROM unit_keys k JOIN units u ON u.id = k.unit_id "
                            "ORDER BY u.ord, k.rowid"):
        if 2 <= len(row["key"].split()) <= MAX_EXPRESSION_WORDS:
            out.append((row["key"], row["unit_id"], row["kind"]))
    return out


def _tokens(conn: sqlite3.Connection, unit_id: str, scope: str, n: int) -> list[tuple[int, str]]:
    return [(int(r["pos"]), str(r["key"])) for r in conn.execute(
        "SELECT pos, key FROM tokens WHERE unit_id = ? AND scope = ? AND n = ? ORDER BY pos, rowid",
        (unit_id, scope, n))]


def _unit_pages(conn: sqlite3.Connection) -> dict[str, int]:
    """Distinct PDF pages a unit lies on: a paragraph split on one page is not a page crossing."""
    return {row["id"]: len({(loc["source_id"], loc["pdf_page"]) for loc in json.loads(row["locations"] or "[]")})
            for row in conn.execute("SELECT id, locations FROM units")}


def evaluate_expressions(service: FriedmanService, conn: sqlite3.Connection, gap: int = DEFAULT_GAP) -> dict[str, list]:
    """One expression search per multiword head key at the reader's default gap, and the
    head's own quotations searched directly at every gap."""
    pages = _unit_pages(conn)
    heads, quotations, others = [], [], []
    for key, unit_id, kind in multiword_keys(conn):
        words = tuple(key.split())
        result = service.expression(key, gap=gap, limit=10 ** 6)
        by_unit = {hit.unit.id: hit for hit in result.hits}
        rank = next((i for i, hit in enumerate(result.hits) if hit.unit.id == unit_id), None)
        own = by_unit.get(unit_id)
        head_place = next((p for p in own.places if p.scope == SCOPE_HEAD), None) if own else None
        heads.append({
            "key": key, "unit": unit_id, "kind": kind, "words": len(words), "continued": pages.get(unit_id, 1) > 1,
            "found": head_place is not None,
            "contiguous": bool(head_place) and head_place.span == len(words) - 1,
            "rank": rank, "shown": rank is not None and rank < MAX_EXPRESSION_HITS,
            "hits": len(result.hits),
        })
        for hit in result.hits:
            if hit.unit.id == unit_id:
                continue
            for place in hit.places:   # the places the reader sees (MAX_PLACES per unit)
                others.append({"key": key, "unit": unit_id, "other": hit.unit.id, "scope": place.scope, "n": place.n,
                               "positions": list(place.positions), "span": place.span,
                               "stem": any(c.via == "stem" for c in place.components)})
        accepts = [frozenset(word_keys(w)) for w in words]
        for q in conn.execute("SELECT n, quote FROM quotations WHERE unit_id = ? ORDER BY n", (unit_id,)):
            printed = str(q["quote"] or "").split()
            items = _tokens(conn, unit_id, SCOPE_QUOTATION, int(q["n"]))
            first = None
            for g in range(MAX_GAP + 1):
                match = ordered_match(items, accepts, g)
                if match is not None:
                    first = g
                    break
            record = {"key": key, "unit": unit_id, "n": int(q["n"]), "first_gap": first,
                      "continued": pages.get(unit_id, 1) > 1, "positions": None, "stem": False}
            if match is not None:
                record["positions"] = [items[i][0] for i in match]
                # A clitic alternative on either side: the typed word's own key is not the
                # matched key, or the matched key is not the printed word's own key.
                record["stem"] = any(
                    items[i][1] != word_keys(w)[0]
                    or (items[i][0] < len(printed) and items[i][1] not in lookup_key(printed[items[i][0]]).split())
                    for i, w in zip(match, words))
            quotations.append(record)
    return {"heads": heads, "quotations": quotations, "others": others}


def evaluate_reversed(service: FriedmanService, conn: sqlite3.Connection, gap: int = DEFAULT_GAP) -> list[dict]:
    """Two-word head keys searched in the reversed order: every place that is not the own head."""
    out = []
    for key, unit_id, _kind in multiword_keys(conn):
        words = key.split()
        if len(words) != 2:
            continue
        result = service.expression(" ".join(reversed(words)), gap=gap, limit=10 ** 6)
        for hit in result.hits:
            for place in hit.places:
                if hit.unit.id == unit_id and place.scope == SCOPE_HEAD:
                    continue
                out.append({"key": key, "unit": unit_id, "other": hit.unit.id, "scope": place.scope, "n": place.n,
                            "positions": list(place.positions), "own": hit.unit.id == unit_id})
    return out


def evaluate_index(service: FriedmanService, conn: sqlite3.Connection) -> list[dict]:
    """Every index form with a root: does the lookup reach an entry of that root? ``how`` says
    by what: ``dotted`` (an entry whose dotted root is the index's), ``plain`` (an entry of the
    same root letters, the dots disagreeing or absent), ``head`` (the form is itself an entry
    head, which the index files under a root label no entry carries), or ``none``."""
    dotted_of = defaultdict(set)
    for row in conn.execute("SELECT unit_id, dotted FROM unit_roots"):
        dotted_of[row["unit_id"]].add(row["dotted"])
    keys_of = defaultdict(set)
    for row in conn.execute("SELECT unit_id, key FROM unit_keys"):
        keys_of[row["unit_id"]].add(row["key"])
    root_keys_of = {row["id"]: lookup_key(row["root"] or "") for row in conn.execute("SELECT id, root FROM units")}
    by_form: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in conn.execute("SELECT form, form_key, root_text, root_dotted, root_key FROM index_forms ORDER BY id"):
        if row["root_key"]:
            by_form[row["form"]].append(row)
    out = []
    for form, rows in by_form.items():
        result = service.lookup(form, limit=10 ** 6)
        reached_dotted, reached_keys, reached_roots, hows = set(), set(), set(), {}
        for match in result.matches:
            reached_dotted |= dotted_of.get(match.unit.id, set())
            reached_keys |= keys_of.get(match.unit.id, set())
            reached_roots.add(root_keys_of.get(match.unit.id, ""))
            hows.setdefault(match.unit.id, match.via)
        for row in rows:
            if row["root_dotted"] and row["root_dotted"] in reached_dotted:
                how = "dotted"
            elif row["root_key"] in reached_keys or row["root_key"] in reached_roots:
                how = "plain"
            elif row["form_key"] in reached_keys:
                how = "head"
            else:
                how = "none"
            out.append({"form": form, "root": row["root_text"], "dotted": row["root_dotted"], "found": how != "none",
                        "how": how, "ambiguous": len(rows) > 1, "matches": len(result.matches),
                        "shown": len(result.matches) <= MAX_MATCHES,
                        "via": sorted(set(hows.values()))})
    return out


#: The letters Judeo-Arabic dots (ג̇ ד̇ ט̇ כ̇ צ̇ ת̇ for ج ذ ظ خ ض ث; plain ג is غ); a dot above any other letter
#: whose NEXT letter is one of these sits on the wrong base (the PDF's glyph order put it there).
DOTTABLE = frozenset("גדטכךצץת")  # the letters the book dots: ج ذ ظ خ (also final) ض (also final) ث; plain ג is غ
DOT_ABOVE = "̇"


def shifted_dots(text: str) -> int:
    """How many dots above sit on a letter that takes none while the next letter takes one and
    carries none: a dot that moved from its base onto the letter before it. An abbreviation or a
    numeral dots every letter (נ̇ע̇, י̇ג̇) and is not counted."""
    bases: list[tuple[str, list[str]]] = []
    for ch in text:
        if unicodedata.category(ch).startswith("M") and bases:
            bases[-1][1].append(ch)
        else:
            bases.append((ch, []))
    return sum(1 for i, (ch, marks) in enumerate(bases)
               if DOT_ABOVE in marks and ch not in DOTTABLE and i + 1 < len(bases)
               and bases[i + 1][0] in DOTTABLE and DOT_ABOVE not in bases[i + 1][1])


def evaluate_conservation(conn: sqlite3.Connection) -> dict:
    """Technical conservation of the recovered text: the dot above on the wrong letter, counted
    in heads, quotations and the dotted roots the index route keys on."""
    out = {}
    for name, sql in (("heads", "SELECT head_text FROM units WHERE head_text != ''"),
                      ("quotations", "SELECT quote FROM quotations"),
                      ("unit_roots", "SELECT dotted FROM unit_roots"),
                      ("index_roots", "SELECT DISTINCT root_dotted FROM index_forms WHERE root_dotted != ''")):
        texts = [row[0] for row in conn.execute(sql)]
        dotted = [t for t in texts if DOT_ABOVE in t]
        out[name] = {"total": len(texts), "with_a_dot": len(dotted), "dot_shifted": sum(1 for t in dotted if shifted_dots(t))}
    index_dotted = {row[0] for row in conn.execute("SELECT DISTINCT root_dotted FROM index_forms WHERE root_dotted != ''")}
    unit_dotted = {row[0] for row in conn.execute("SELECT DISTINCT dotted FROM unit_roots")}
    out["index_dotted_roots_with_a_unit_root"] = len(index_dotted & unit_dotted)
    out["index_dotted_roots"] = len(index_dotted)
    return out


def evaluate_homographs(service: FriedmanService, conn: sqlite3.Connection) -> list[dict]:
    """A head key shared by several entries: the lookup must bring every one of them."""
    entries = defaultdict(list)
    for row in conn.execute("SELECT k.key, u.id FROM unit_keys k JOIN units u ON u.id = k.unit_id "
                            "WHERE u.kind = 'entry' ORDER BY u.ord"):
        entries[row["key"]].append(row["id"])
    out = []
    for key, ids in entries.items():
        if len(ids) < 2:
            continue
        result = service.lookup(key, limit=10 ** 6)
        got = [m.unit.id for m in result.matches]
        out.append({"key": key, "entries": ids, "returned": got, "complete": all(i in got for i in ids),
                    "shown": len(got) <= MAX_MATCHES})
    return out


def summarize(expr: dict[str, list], reversed_hits: list[dict], index: list[dict], homographs: list[dict],
              conservation: Optional[dict] = None) -> dict:
    heads, quotations, others = expr["heads"], expr["quotations"], expr["others"]
    by_gap = Counter(q["first_gap"] for q in quotations)
    cumulative = {}
    running = 0
    for g in range(MAX_GAP + 1):
        running += by_gap.get(g, 0)
        cumulative[str(g)] = running
    units_with_quotations = {q["unit"] for q in quotations}
    found_units = {q["unit"] for q in quotations if q["first_gap"] is not None}
    continued = [h for h in heads if h["continued"]]
    continued_q = [q for q in quotations if q["continued"]]
    return {
        "expressions": {
            "head_keys": len(heads),
            "head_found": sum(h["found"] for h in heads),
            "head_found_contiguous": sum(h["contiguous"] for h in heads),
            "head_missed": [h["key"] + " @" + h["unit"] for h in heads if not h["found"]],
            "head_shown_in_first_results": sum(h["shown"] for h in heads),
            "searches_truncated_for_the_reader": sum(h["hits"] > MAX_EXPRESSION_HITS for h in heads),
            "hits_per_search": {"median": _median([h["hits"] for h in heads]), "max": max((h["hits"] for h in heads), default=0)},
            "own_quotations": len(quotations),
            "own_quotations_found_by_gap_cumulative": cumulative,
            "own_quotations_missed_at_max_gap": by_gap.get(None, 0),
            "own_quotations_found_through_a_clitic": sum(q["stem"] for q in quotations),
            "units_with_quotations": len(units_with_quotations),
            "units_found_in_some_own_quotation": len(found_units),
            "other_unit_places": len(others),
            "other_unit_quotation_places": sum(o["scope"] == SCOPE_QUOTATION for o in others),
            "other_unit_head_places": sum(o["scope"] == SCOPE_HEAD for o in others),
            "reversed_two_word_places": len(reversed_hits),
            "reversed_in_own_quotations": sum(r["own"] for r in reversed_hits),
            "continued_units": {"head_keys": len(continued), "head_found": sum(h["found"] for h in continued),
                                "own_quotations": len(continued_q),
                                "own_quotations_found": sum(q["first_gap"] is not None for q in continued_q)},
        },
        "lookup": {
            "index_forms_with_a_root": len(index),
            "index_found": sum(i["found"] for i in index),
            "index_found_how": dict(Counter(i["how"] for i in index)),
            "index_missed": [f"{i['form']} -> {i['root']}" for i in index if not i["found"]],
            "index_ambiguous_forms": len({i["form"] for i in index if i["ambiguous"]}),
            "index_ambiguous_roots_found": sum(i["found"] for i in index if i["ambiguous"]),
            "index_ambiguous_roots": sum(1 for i in index if i["ambiguous"]),
            "index_lookups_truncated_for_the_reader": len({i["form"] for i in index if not i["shown"]}),
            "homograph_keys": len(homographs),
            "homograph_complete": sum(h["complete"] for h in homographs),
            "homograph_truncated_for_the_reader": sum(not h["shown"] for h in homographs),
        },
        "conservation": conservation or {},
    }


def _median(values: list[int]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    mid = len(s) // 2
    return float(s[mid]) if len(s) % 2 else (s[mid - 1] + s[mid]) / 2


def render_report(report: dict) -> str:
    e, lk = report["expressions"], report["lookup"]
    lines = ["# Friedman search evaluation: the automatic measures", "",
             "The book is the truth here; whether a hit is RIGHT is the scholar's (sheet.csv).", "",
             "## Expressions (multiword head keys)", "",
             f"- Head keys of 2..{MAX_EXPRESSION_WORDS} words: {e['head_keys']}; found in their own head: "
             f"{e['head_found']} ({e['head_found_contiguous']} contiguous); shown among the first "
             f"{MAX_EXPRESSION_HITS} results: {e['head_shown_in_first_results']}; searches with more hits than the "
             f"reader is shown: {e['searches_truncated_for_the_reader']} (median hits {e['hits_per_search']['median']}, "
             f"max {e['hits_per_search']['max']}).",
             f"- Own quotations under those heads: {e['own_quotations']}; holding the expression, cumulative by gap: "
             + ", ".join(f"gap {g}: {n}" for g, n in e["own_quotations_found_by_gap_cumulative"].items())
             + f"; not found at gap {MAX_GAP}: {e['own_quotations_missed_at_max_gap']}; found through a clitic "
             f"alternative: {e['own_quotations_found_through_a_clitic']}.",
             f"- Units with quotations: {e['units_with_quotations']}; found in at least one own quotation: "
             f"{e['units_found_in_some_own_quotation']}.",
             f"- Places in OTHER units at the default gap {DEFAULT_GAP}: {e['other_unit_places']} "
             f"({e['other_unit_quotation_places']} quotations, {e['other_unit_head_places']} heads).",
             f"- Two-word keys in the reversed order: {e['reversed_two_word_places']} places "
             f"({e['reversed_in_own_quotations']} in own quotations).",
             f"- Units continuing across a page: {e['continued_units']['head_found']}/{e['continued_units']['head_keys']} "
             f"head keys found; {e['continued_units']['own_quotations_found']}/{e['continued_units']['own_quotations']} "
             f"own quotations found.",
             "", "## Single-word lookup (the book's index of forms)", "",
             f"- Index forms with a root: {lk['index_forms_with_a_root']}; root reached: {lk['index_found']} "
             f"(by the dotted root: {lk['index_found_how'].get('dotted', 0)}, by the plain letters only: "
             f"{lk['index_found_how'].get('plain', 0)}, the form itself an entry head: "
             f"{lk['index_found_how'].get('head', 0)}); "
             f"ambiguous forms: {lk['index_ambiguous_forms']} ({lk['index_ambiguous_roots_found']}/"
             f"{lk['index_ambiguous_roots']} of their roots reached); lookups with more matches than the reader is "
             f"shown: {lk['index_lookups_truncated_for_the_reader']}.",
             f"- Head keys shared by several entries: {lk['homograph_keys']}; all entries returned: "
             f"{lk['homograph_complete']}; truncated for the reader: {lk['homograph_truncated_for_the_reader']}.",
             ""]
    cons = report.get("conservation") or {}
    if cons:
        lines += ["## Technical conservation: the dot above", "",
                  "A dot above a letter that takes none (not ג ד ט כ ך צ ץ ת) while the next letter takes one and carries none: the "
                  "PDF's glyph order put it on the letter before its base. Marks are stripped from every lookup key, "
                  "so only the DISPLAY and the dotted-root route of the index are affected.", ""]
        for name in ("heads", "quotations", "unit_roots", "index_roots"):
            c = cons[name]
            lines.append(f"- {name}: {c['with_a_dot']} of {c['total']} carry a dot; shifted in {c['dot_shifted']}.")
        lines += [f"- Index dotted roots with a unit root spelled the same: {cons['index_dotted_roots_with_a_unit_root']} "
                  f"of {cons['index_dotted_roots']}.", ""]
    if e["head_missed"]:
        lines += ["### Head keys missed", ""] + [f"- {k}" for k in e["head_missed"]] + [""]
    if lk["index_missed"]:
        lines += ["### Index forms whose root was not reached", ""] + [f"- {k}" for k in lk["index_missed"]] + [""]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# The scholar's sheet
# ---------------------------------------------------------------------------

def _mark(words: list[str], positions: Optional[list[int]]) -> str:
    marked = set(positions or [])
    return " ".join(f"[[{w}]]" if i in marked else w for i, w in enumerate(words))


def _links(conn: sqlite3.Connection, unit_id: str) -> str:
    row = conn.execute("SELECT locations, pdf_url FROM units WHERE id = ?", (unit_id,)).fetchone()
    try:
        locations = json.loads(row["locations"] or "[]")
    except (TypeError, ValueError):
        locations = []
    links = [f"{VIEWER_BASE}{loc['source_id']}.pdf#page={loc['pdf_page']}" for loc in locations
             if loc.get("source_id") and loc.get("pdf_page")]
    return " ; ".join(dict.fromkeys(links)) or (row["pdf_url"] or "")


def _components(conn: sqlite3.Connection, key: str, unit_id: str, scope: str, n: int,
                positions: Optional[list[int]]) -> str:
    if not positions:
        return ""
    if scope == SCOPE_HEAD:
        text = conn.execute("SELECT head_text FROM units WHERE id = ?", (unit_id,)).fetchone()["head_text"]
        order = head_scope_map(text)
        positions = [order[p] if p < len(order) else p for p in positions]   # the printed order
    else:
        text = conn.execute("SELECT quote FROM quotations WHERE unit_id = ? AND n = ?", (unit_id, n)).fetchone()["quote"]
    printed = text.split()
    parts = []
    for typed, pos in zip(key.split(), positions):
        word = printed[pos] if pos < len(printed) else "?"
        via = "word" if lookup_key(typed) in lookup_key(word).split() else "stem"
        parts.append(f"{typed} <- {word} ({via})")
    return "; ".join(parts)


def _row(conn: sqlite3.Connection, stratum: str, search: str, gap: Any, unit_id: str, scope: str, n: int,
         positions: Optional[list[int]], text: Optional[str] = None) -> dict:
    unit = conn.execute("SELECT kind, head_text, parent, printed_page FROM units WHERE id = ?", (unit_id,)).fetchone()
    parent = ""
    if unit["parent"]:
        prow = conn.execute("SELECT head_text FROM units WHERE id = ?", (unit["parent"],)).fetchone()
        parent = prow["head_text"] if prow else ""
    if text is None:
        if scope == SCOPE_HEAD:
            order = head_scope_map(unit["head_text"])
            text = _mark(unit["head_text"].split(), [order[p] if p < len(order) else p for p in positions] if positions else positions)
        else:
            q = conn.execute("SELECT quote, translation, locator FROM quotations WHERE unit_id = ? AND n = ?",
                             (unit_id, n)).fetchone()
            text = f"{_mark(q['quote'].split(), positions)} {q['translation']} {q['locator']}".strip()
    where = "head" if scope == SCOPE_HEAD else f"quotation {n}"
    return {"case": "", "stratum": stratum, "search": search, "gap": gap, "unit": unit_id, "kind": unit["kind"],
            "head": unit["head_text"], "parent_head": parent, "where": where, "page": unit["printed_page"],
            "link": _links(conn, unit_id), "text": text,
            "components": _components(conn, search, unit_id, scope, n, positions) if scope else "",
            "verdict": "", "note": ""}


def service_lookup_heads(conn: sqlite3.Connection, form: str) -> str:
    """The heads of the entries whose root letters are the form's root letters: what the
    scholar sees beside a missed index form."""
    key = lookup_key(form)
    rows = conn.execute("SELECT DISTINCT u.id, u.head_text, u.printed_page FROM units u JOIN unit_keys k ON k.unit_id = u.id "
                        "WHERE k.key = ? AND u.kind = 'entry' ORDER BY u.ord LIMIT 6", (key,)).fetchall()
    return " | ".join(f"{r['id']} {r['head_text']} (p. {r['printed_page']})" for r in rows)


def build_sheet(conn: sqlite3.Connection, expr: dict[str, list], reversed_hits: list[dict], index: list[dict],
                homographs: list[dict], per_stratum: int, seed: int) -> list[dict]:
    rng = random.Random(seed)

    def sample(population: list, n: int = per_stratum) -> list:
        return rng.sample(population, min(n, len(population))) if population else []

    rows: list[dict] = []
    for h in sample([h for h in expr["heads"] if not h["found"]]):
        rows.append(_row(conn, "head_missed", h["key"], DEFAULT_GAP, h["unit"], SCOPE_HEAD, 0, None))
    # The two strata the plan's rates rest on (missed expressions, false suggestions) get twice the rows.
    for q in sample([q for q in expr["quotations"] if q["first_gap"] is None], 2 * per_stratum):
        rows.append(_row(conn, "quotation_missed", q["key"], MAX_GAP, q["unit"], SCOPE_QUOTATION, q["n"], None))
    for q in sample([q for q in expr["quotations"] if q["first_gap"]]):
        rows.append(_row(conn, "discontinuous", q["key"], q["first_gap"], q["unit"], SCOPE_QUOTATION, q["n"], q["positions"]))
    for q in sample([q for q in expr["quotations"] if q["stem"]]):
        rows.append(_row(conn, "clitic", q["key"], q["first_gap"], q["unit"], SCOPE_QUOTATION, q["n"], q["positions"]))
    for o in sample([o for o in expr["others"] if o["scope"] == SCOPE_QUOTATION], 2 * per_stratum):
        row = _row(conn, "other_unit", o["key"], DEFAULT_GAP, o["other"], SCOPE_QUOTATION, o["n"], o["positions"])
        row["note"] = f"searched from the head of {o['unit']}"
        rows.append(row)
    for r in sample(reversed_hits):
        row = _row(conn, "reversed", " ".join(reversed(r["key"].split())), DEFAULT_GAP, r["other"], r["scope"], r["n"],
                   r["positions"])
        row["note"] = f"the head reads {r['key']} ({r['unit']})"
        rows.append(row)
    for q in sample([q for q in expr["quotations"] if q["continued"] and q["first_gap"] is not None]):
        rows.append(_row(conn, "continuation", q["key"], q["first_gap"], q["unit"], SCOPE_QUOTATION, q["n"], q["positions"]))
    for i in sample([i for i in index if not i["found"]]):
        result = service_lookup_heads(conn, i["form"])
        rows.append({"case": "", "stratum": "index_missed", "search": i["form"], "gap": "", "unit": "", "kind": "index",
                     "head": i["root"], "parent_head": "", "where": "index of forms", "page": "", "link": "",
                     "text": f"{i['form']} -> {i['root']}: {i['matches']} matches, none of this root"
                             + (f": {result}" if result else ""),
                     "components": "", "verdict": "", "note": ""})
    ambiguous = sorted({i["form"] for i in index if i["ambiguous"]})
    for form in sample(ambiguous):
        roots = [i for i in index if i["form"] == form]
        rows.append({"case": "", "stratum": "ambiguous_root", "search": form, "gap": "", "unit": "", "kind": "index",
                     "head": " / ".join(r["root"] for r in roots), "parent_head": "", "where": "index of forms",
                     "page": "", "link": "",
                     "text": "; ".join(f"{r['root']}: {'reached' if r['found'] else 'NOT reached'}" for r in roots),
                     "components": "", "verdict": "", "note": ""})
    for h in sample(homographs):
        heads = []
        for uid in h["entries"]:
            u = conn.execute("SELECT head_text, printed_page FROM units WHERE id = ?", (uid,)).fetchone()
            heads.append(f"{uid} {u['head_text']} (p. {u['printed_page']}){'' if uid in h['returned'] else ' MISSING'}")
        rows.append({"case": "", "stratum": "homograph", "search": h["key"], "gap": "", "unit": "", "kind": "entry",
                     "head": h["key"], "parent_head": "", "where": "lookup", "page": "", "link": "",
                     "text": " | ".join(heads), "components": "", "verdict": "", "note": ""})
    for i, row in enumerate(rows, 1):
        row["case"] = f"c{i:03d}"
    return rows


def render_guide(rows: list[dict], report: dict) -> str:
    counts = Counter(r["stratum"] for r in rows)
    lines = ["# The scholar's sheet: how to grade", "",
             "Open `sheet.csv` (UTF-8), fill `verdict` for every row, add a `note` where it helps, save it as CSV, "
             "then run `python scripts/evaluate_friedman.py grade`. Matched words are marked `[[so]]`; "
             "`components` says which typed word was carried by which printed word and by what (word, stem).", "",
             "| stratum | rows | what it is | verdicts |", "|---|---|---|---|"]
    for name, (what, verdicts) in STRATA.items():
        lines.append(f"| {name} | {counts.get(name, 0)} | {what} | {', '.join(verdicts)} |")
    lines += ["", "Verdicts: `correct` = the place is a genuine instance of the expression (or the lookup's answer is "
              "right); `false` = it is not; for the missed strata say why: `inflection` (the expression is there in "
              "another form), `reordered` (its words in another order), `extraction` (the text was recovered wrongly "
              "from the PDF: check the link), `not_there` (the quotation or index row does not contain it, so nothing "
              "was missed); `unsure` when you cannot tell.", "",
              "Populations the rates are projected onto (from report.json):", "",
              f"- own quotations under multiword heads: {report['expressions']['own_quotations']}, of which not found at "
              f"gap {MAX_GAP}: {report['expressions']['own_quotations_missed_at_max_gap']}",
              f"- places in other units at the default gap: {report['expressions']['other_unit_quotation_places']} quotations",
              f"- index forms with a root: {report['lookup']['index_forms_with_a_root']}, of which not reached: "
              f"{len(report['lookup']['index_missed'])}", ""]
    return "\n".join(lines)


def write_sheet(rows: list[dict], path: Path) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=SHEET_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def read_sheet(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------------------
# Grading the filled sheet
# ---------------------------------------------------------------------------

def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float, float]:
    """(rate, low, high): the Wilson 95% interval of k successes in n."""
    if n == 0:
        return 0.0, 0.0, 0.0
    p = k / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return p, max(0.0, centre - half), min(1.0, centre + half)


def grade_sheet(rows: list[dict], report: Optional[dict] = None) -> dict:
    """Counts per stratum and verdict, the rates the plan asks for, and the problems found."""
    problems = []
    by_stratum: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        stratum, verdict = row.get("stratum", ""), (row.get("verdict") or "").strip().lower()
        if stratum not in STRATA:
            problems.append(f"{row.get('case')}: unknown stratum {stratum!r}")
            continue
        if not verdict:
            problems.append(f"{row.get('case')}: no verdict")
            continue
        if verdict not in STRATA[stratum][1]:
            problems.append(f"{row.get('case')}: verdict {verdict!r} is not one of {STRATA[stratum][1]}")
            continue
        by_stratum[stratum][verdict] += 1

    def rate(stratum: str, bad: tuple[str, ...]) -> dict:
        c = by_stratum.get(stratum, Counter())
        n = sum(v for k, v in c.items() if k != "unsure")
        k = sum(c.get(b, 0) for b in bad)
        p, lo, hi = wilson(k, n)
        return {"graded": n, "unsure": c.get("unsure", 0), "bad": k, "rate": round(p, 3), "low": round(lo, 3),
                "high": round(hi, 3)}

    out = {
        "problems": problems,
        "counts": {s: dict(c) for s, c in by_stratum.items()},
        "expressions": {
            "missed_among_quotations_not_found": rate("quotation_missed", ("inflection", "reordered", "extraction")),
            "false_among_other_unit_places": rate("other_unit", ("false",)),
            "false_among_discontinuous": rate("discontinuous", ("false",)),
            "false_among_clitic": rate("clitic", ("false",)),
            "same_expression_among_reversed": rate("reversed", ("correct",)),
            "wrong_text_among_continuations": rate("continuation", ("false",)),
            "head_missed_real": rate("head_missed", ("inflection", "reordered", "extraction")),
        },
        "lookup": {
            "missed_among_index_not_reached": rate("index_missed", ("extraction", "inflection", "reordered")),
            "false_among_ambiguous_roots": rate("ambiguous_root", ("false",)),
            "false_among_homographs": rate("homograph", ("false",)),
        },
    }
    if report:
        e = report["expressions"]
        missed = out["expressions"]["missed_among_quotations_not_found"]
        if e["own_quotations"]:
            projected = e["own_quotations_missed_at_max_gap"] * missed["rate"]
            out["expressions"]["projected_missed_quotations"] = round(projected, 1)
            out["expressions"]["projected_missed_rate_of_own_quotations"] = round(projected / e["own_quotations"], 4)
        false = out["expressions"]["false_among_other_unit_places"]
        out["expressions"]["projected_false_other_unit_places"] = round(e["other_unit_quotation_places"] * false["rate"], 1)
        lk = report["lookup"]
        idx = out["lookup"]["missed_among_index_not_reached"]
        if lk["index_forms_with_a_root"]:
            out["lookup"]["projected_missed_rate_of_index_forms"] = round(
                len(lk["index_missed"]) * idx["rate"] / lk["index_forms_with_a_root"], 4)
    return out


# ---------------------------------------------------------------------------

def run(args: argparse.Namespace) -> int:
    service, conn = open_service(args.db)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    expr = evaluate_expressions(service, conn)
    reversed_hits = evaluate_reversed(service, conn)
    index = evaluate_index(service, conn)
    homographs = evaluate_homographs(service, conn)
    report = summarize(expr, reversed_hits, index, homographs, evaluate_conservation(conn))
    report["sidecar"] = {"path": service.db_path, "status": service.status,
                         "built_at": service.meta("built_at"), "units_sha256": service.meta("units_sha256")}
    (out / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    (out / "report.md").write_text(render_report(report), encoding="utf-8")
    rows = build_sheet(conn, expr, reversed_hits, index, homographs, args.per_stratum, args.seed)
    write_sheet(rows, out / "sheet.csv")
    (out / "sheet_guide.md").write_text(render_guide(rows, report), encoding="utf-8")
    print(render_report(report))
    print(f"sheet: {len(rows)} rows in {out / 'sheet.csv'}; guide in {out / 'sheet_guide.md'}")
    return 0


def grade(args: argparse.Namespace) -> int:
    sheet = Path(args.sheet)
    report_path = sheet.parent / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else None
    result = grade_sheet(read_sheet(sheet), report)
    (sheet.parent / "grades.json").write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=1))
    if result["problems"]:
        print(f"{len(result['problems'])} rows not graded or not gradable", file=sys.stderr)
        return 2
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default=None, help="the sidecar (default: where the service looks)")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("run", help="the automatic measures and the scholar's sheet")
    p.add_argument("--out", default=str(EVALUATION_DIR))
    p.add_argument("--per-stratum", type=int, default=12)
    p.add_argument("--seed", type=int, default=1)
    p.set_defaults(func=run)
    g = sub.add_parser("grade", help="rates from the filled sheet")
    g.add_argument("--sheet", default=str(EVALUATION_DIR / "sheet.csv"))
    g.set_defaults(func=grade)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
