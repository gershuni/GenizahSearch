#!/usr/bin/env python3
"""Build ``friedman_data/friedman.db``, the Friedman dictionary sidecar, from the verified candidates.

Inputs, each behind its own verify gate: ``candidate/units.json`` (entries, sub-entries and
binyan paragraphs cut from the PDFs by ``friedman_units.py``) and ``candidate/index_forms.json``
(the book's own index of forms, ``friedman_index.py``). The sidecar keeps one Friedman
namespace (unit ids ``f000001``…), the printed and PDF location of every unit, the source
hashes, the parent relation, every original reading, the lookup keys made by the shared
normalizer, the dotted roots (ג̇ distinct from ג) for the index, and the index rows.

The sidecar's ``meta.status`` says what it is: a CANDIDATE. ``shared/friedman_service.py``
serves it read-only and shows that status; nothing deploys it.

    python scripts/build_friedman_sidecar.py build
    python scripts/build_friedman_sidecar.py verify
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.dictionary_search_indexes import CONSTRUCTION_DDL, expression_material, rebuild_expression_tokens, unit_form_rows
from scripts import friedman_index, friedman_units  # noqa: E402
from scripts.prepare_friedman_dictionary import CREDIT_HE, VIEWER_BASE, read_json, sha256  # noqa: E402
from shared.blau_normalize import NORMALIZER_VERSION, SPELLING_DDL, build_spelling_index, normalize_ja  # noqa: E402
from shared.ja_expression import KEYING_VERSION
from shared.friedman_service import SCOPE_HEAD, SCOPE_QUOTATION, head_scope_text, scope_keys  # noqa: E402

SIDECAR_SCHEMA = "friedman-sidecar-6"
SIDECAR_VERSION = 8  # 7 (2026-11-13): head verbs carry their stems; words carry their verb root whatever the kind of hit; 8 (2026-11-14): a unit's references keep its cross-references ("ראה הערך חפי")
STATUS = "candidate_not_reviewed_not_for_production"
SIDECAR_FILENAME = "friedman.db"

DDL = SPELLING_DDL + CONSTRUCTION_DDL + """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE units (
    id TEXT PRIMARY KEY, ord INTEGER NOT NULL UNIQUE, kind TEXT NOT NULL, letter TEXT NOT NULL,
    parent TEXT REFERENCES units(id), head_text TEXT, head_plain TEXT, root TEXT, root_source TEXT,
    root_candidate TEXT, lemma TEXT, binyan TEXT, definition TEXT NOT NULL, refs TEXT NOT NULL,
    gloss_foreign TEXT NOT NULL, text TEXT NOT NULL, printed_page TEXT, pdf_url TEXT NOT NULL,
    flags TEXT NOT NULL, locations TEXT NOT NULL, quotations INTEGER NOT NULL);
CREATE INDEX units_parent ON units(parent, ord);
CREATE TABLE unit_keys (key TEXT NOT NULL, unit_id TEXT NOT NULL REFERENCES units(id),
    PRIMARY KEY (key, unit_id));
CREATE INDEX unit_keys_unit ON unit_keys(unit_id);
CREATE TABLE unit_forms (key TEXT NOT NULL, unit_id TEXT NOT NULL REFERENCES units(id),
    literal TEXT NOT NULL, PRIMARY KEY (key, unit_id, literal));
CREATE TABLE unit_roots (dotted TEXT NOT NULL, unit_id TEXT NOT NULL REFERENCES units(id),
    PRIMARY KEY (dotted, unit_id));
CREATE TABLE quotations (unit_id TEXT NOT NULL REFERENCES units(id), n INTEGER NOT NULL,
    quote TEXT NOT NULL, quote_key TEXT NOT NULL, translation TEXT NOT NULL, locator TEXT NOT NULL,
    foreign_text TEXT NOT NULL, PRIMARY KEY (unit_id, n));
CREATE TABLE index_forms (id INTEGER PRIMARY KEY, form TEXT NOT NULL, form_key TEXT NOT NULL,
    root_text TEXT NOT NULL, root_dotted TEXT NOT NULL, root_key TEXT NOT NULL, alternatives INTEGER NOT NULL,
    pdf_page INTEGER NOT NULL, printed_page TEXT, url TEXT NOT NULL, line TEXT NOT NULL);
CREATE INDEX index_forms_key ON index_forms(form_key);
CREATE INDEX index_forms_root ON index_forms(root_dotted);
CREATE INDEX index_forms_root_key ON index_forms(root_key);
CREATE TABLE tokens (unit_id TEXT NOT NULL REFERENCES units(id), scope TEXT NOT NULL, n INTEGER NOT NULL,
    pos INTEGER NOT NULL, key TEXT NOT NULL, peeled INTEGER NOT NULL);
CREATE INDEX tokens_key ON tokens(key, unit_id, scope, n);
CREATE INDEX tokens_scope ON tokens(unit_id, scope, n, pos);
"""


#: The words that open a cross-reference, at the end of the text before the bold entry it names
#: ("ראה הערך ", "השווה הערכים ", "ראה עוד הערך ").
XREF_OPEN_RE = re.compile(r"(?:ראה|השווה)[^.()]*$")
_BODY_CLASSES = frozenset({"ja", "he", "loc", "shelf", "bullet", "other", "foreign"})
#: A reference to the sub-entry before or after ("ראה ערך המשנה הבא", "השווה ערכי המשנה הקודמים"): no
#: bold name follows it, and the recovery files it with a locator.
NEIGHBOUR_RE = re.compile(r"(?:ראה|השווה)[^.()]{0,12}ער(?:ך|כי) המשנה (?:הבא|הקוד)[^\s.,;)]*")


def cross_references(unit: dict) -> list[str]:
    """A unit's cross-references as the book prints them: the opening words and the entries named
    in bold after them, a stem numeral kept ("ראה הערך נסי IV", "השווה הערכים סَלَף, קُדَמא").
    The recovery files the bold names apart from the text (``gloss.xrefs``, an example's
    ``xrefs``) and the opening words with the locator before them, so the references column lost
    both (the entry חפו showed ". ." for "ראה הערך חפי."). Read from the unit's
    segments: bold in the body, after words that open a reference; bold after such a group and a
    joining word is one more name of it. [] for a unit with none."""
    segments = unit.get("segments") or []
    found: list[str] = []
    body = False
    i = 0
    while i < len(segments):
        cls, text = segments[i]
        if cls in _BODY_CLASSES:
            body = True
        if cls != "head" or not body:
            i += 1
            continue
        before = segments[i - 1][1].strip() if i else ""
        opening = XREF_OPEN_RE.search(before)
        names: list[str] = []
        while i < len(segments) and segments[i][0] in ("head", "binyan"):
            names.append(segments[i][1])
            i += 1
        name = " ".join("".join(names).split()).strip(" ,;")
        if not name:
            continue
        if opening:
            found.append(f"{' '.join(opening.group(0).split())} {name}")
        elif found and len(before) <= 6:   # ", " / "וכן ": one more entry of the reference before it
            found[-1] += f", {name}"
    body = False
    for cls, text in segments:   # the sub-entry before or after: the words alone, where no name follows
        body = body or cls in _BODY_CLASSES
        if body and cls in ("loc", "he", "shelf", "other"):
            for near in NEIGHBOUR_RE.findall(text):
                near = " ".join(near.split())
                if not any(near in ref for ref in found):
                    found.append(near)
    return found


def unit_references(unit: dict) -> str:
    """The references column: the gloss's references, cleaned of the full stops the cross-references
    left behind, then the unit's cross-references (``cross_references``), each a sentence."""
    plain = re.sub(r"^[.\s]+", "", (unit["gloss"].get("references") or "").strip())
    see = cross_references(unit)   # of a unit in printed order: those after its quotations only
    if unit.get("printed"):
        see = [ref for ref in see if ref not in plain]
    if not see:
        return plain   # as the book prints them, their own full stop kept
    plain = re.sub(r"(?:\s*\.)+\s*$", "", plain).strip()
    parts = ([plain + "."] if plain and see else [plain] if plain else []) + [f"{ref}." for ref in see]
    return " ".join(parts)


def quotation_locator(ex: dict) -> str:
    """A quotation's locator without the opening words of a cross-reference that follows the
    quotation ("(ב2, ע״ב, שורה 13). ראה הערך"): the reference is the unit's (``unit_references``)."""
    locator = ex.get("locator") or ""
    if ex.get("xrefs"):
        locator = re.sub(r"[.\s]*(?:ראה|השווה)[^.()]*$", "", locator).strip()
    near = NEIGHBOUR_RE.search(locator)
    if near and ")" in locator[:near.start()]:   # after the locator's own bracket: the unit's reference
        locator = locator[:near.start()].rstrip(" .;,")
    return locator


def quotation_foreign(unit, n):
    """Recover Arabic example text across font classes, retaining its written order.

    The archived segments retain Arabic incorrectly classified as "other". Use the
    same bullet/locator boundaries as parse_body; never join separate examples.
    """
    zones = []
    current = None
    closed = False
    for cls, text in unit.get("segments", []):
        if cls == "bullet":
            current = []
            zones.append(current)
            closed = False
            continue
        if current is None:
            continue
        if closed and cls == "ja" and text.strip():
            current = []
            zones.append(current)
            closed = False
        if cls in ("loc", "shelf") and ")" in text:
            closed = True
        current.append((cls, text))
    original = unit["examples"][n - 1]["foreign"]
    if n <= len(zones) and any(cls == "other" and any("ء" <= ch <= "ي" for ch in text)
                              for cls, text in zones[n - 1]):
        return " ".join("".join(text for cls, text in zones[n - 1]
                                if cls in ("ja", "foreign", "other")).split())
    return original


#: A glyph of the ligature for the last two letters of אללה, read as another script's sign.
_GLYPHS = {"૮": "לה"}
_LOCATOR_CLASSES = ("loc", "shelf")


def _join(segments) -> str:
    text = "".join(text for _cls, text in segments)
    for glyph, letters in _GLYPHS.items():
        text = text.replace(glyph, letters)
    return re.sub(r"\s+([.,;:)\]])", r"\g<1>", re.sub(r"([(\[])\s+", r"\g<1>", " ".join(text.split())))


def _letters(text: str) -> bool:
    return any(ch.isalpha() for ch in text)


def _body_start(segments, gloss: dict):
    """Where a unit's body begins: the first piece that opens one of the gloss's fields as the
    recovery cut them (a head keeps words in the definition's typeface, "(ולא רק …)", so the
    typeface alone does not tell), else the first quotation's bullet."""
    fields = {"ja": gloss.get("definition"), "he": gloss.get("references"), "loc": gloss.get("locator"),
              "shelf": gloss.get("locator"), "foreign": gloss.get("foreign")}
    for i, (cls, text) in enumerate(segments):
        if cls == "bullet":
            return i
        piece, field = "".join(text.split()), "".join((fields.get(cls) or "").split())
        if piece and field.startswith(piece):
            return i
    return None


def _references_start(zone) -> int:
    """The first piece of a gloss in the references' typeface that opens the references: after
    words of the definition, outside a bracket, and not itself a bracketed note."""
    depth, defined = 0, False
    for i, (cls, text) in enumerate(zone):
        if cls in ("he", "loc", "shelf") and _letters(text) and defined and depth <= 0 and not text.lstrip(" .").startswith("("):
            return i
        defined = defined or (cls == "ja" and _letters(text))
        depth += text.count("(") - text.count(")")
    return len(zone)


def printed_order(unit: dict) -> dict:
    """The unit with its definition, references, quotations and translations read in printed order.

    The recovery files each typeface apart (``friedman_units._zone``), so a word in a second
    typeface left the sentence it is printed in: a bold word inside a definition ("(בלי עَלَיْהِ ובלי
    פועל שני") was dropped, an editor's note inside a quotation went to its translation, Arabic
    inside the references was filed alone. The unit's segments keep the printed order: the
    definition is the gloss up to its last piece in the definition's typeface, the references the
    rest; a quotation runs to its last piece in the quotation's typeface, its translation is the
    rest, the locator as the recovery has it. A unit without segments is returned as it is."""
    segments = [(cls, text) for cls, text in unit.get("segments") or []]
    start = _body_start(segments, unit["gloss"])
    if start is None:
        return unit
    zones: list[list[tuple[str, str]]] = [[]]
    quoting = closed = False
    first_bullet = None
    for i in range(start, len(segments)):
        cls, text = segments[i]
        if cls == "bullet":
            first_bullet = i if first_bullet is None else first_bullet
            zones.append([])
            quoting, closed = True, False
            continue
        if quoting and closed and cls == "ja" and text.strip():
            zones.append([])
            closed = False
        if cls in _LOCATOR_CLASSES and ")" in text:
            closed = True
        zones[-1].append((cls, text))
    if len(zones) - 1 != len(unit["examples"]):
        return unit   # the recovery cut its quotations otherwise: its fields stand

    def last_of(zone, cls="ja") -> int:
        return max((i for i, (c, text) in enumerate(zone) if c == cls and text.strip()), default=-1)

    gloss_zone = zones[0]
    opens = _references_start(gloss_zone)
    cut = min(last_of(gloss_zone) + 1, opens)
    # What the definition ends with in another typeface is still the definition ("אחריות לסיכון
    # (hendiadys)", a closing stop or bracket): up to the piece that opens the references, or a
    # bold name that words of a reference introduce.
    while 0 < cut < opens:
        cls, text = gloss_zone[cut]
        if cls in ("he", *_LOCATOR_CLASSES) and _letters(text):
            break
        if cls in ("head", "binyan") and XREF_OPEN_RE.search(_join(gloss_zone[:cut])):
            break
        cut += 1
    definition, rest = gloss_zone[:cut], gloss_zone[cut:]
    if definition and rest and rest[0][0] == "head":
        # "… ראה הערך" + a bold name: the opening words belong with the name, in the references.
        opening = XREF_OPEN_RE.search(definition[-1][1])
        if opening and definition[-1][0] == "ja":
            cls, text = definition[-1]
            definition[-1] = (cls, text[:opening.start()])
            rest.insert(0, (cls, text[opening.start():]))
    gloss = dict(unit["gloss"], definition=_join(definition), references=_join(rest), foreign="")
    examples = []
    for ex, zone in zip(unit["examples"], zones[1:]):
        cut = last_of(zone) + 1
        if not cut:   # a quotation in Arabic script: as the recovery has it (``quotation_foreign``)
            examples.append(ex)
            continue
        # The translation runs to the locator, every typeface in place (a stem numeral, a bold word);
        # after the locator only a note in the translation's typeface is kept: the bold names there
        # are the unit's cross-references (``cross_references``), a lone stop the sentence's end.
        located = next((i for i in range(cut, len(zone)) if zone[i][0] in _LOCATOR_CLASSES), len(zone))
        after = [seg for seg in zone[located:] if seg[0] not in (*_LOCATOR_CLASSES, "head", "binyan")]
        note = _join(after) if any(_letters(text) for _cls, text in after) else ""
        examples.append(dict(
            ex, quote=_join(seg for seg in zone[:cut] if seg[0] not in _LOCATOR_CLASSES),
            translation=" ".join(part for part in (_join(zone[cut:located]), note) if part)))
    return dict(unit, gloss=gloss, examples=examples, printed=True,
                segments=[list(seg) for seg in segments[first_bullet:]] if first_bullet is not None else [])


def token_rows(unit: dict):
    """The ``tokens`` rows of one unit: every key of every printed word of its head and of each
    of its quotations (``shared.friedman_service.scope_keys``: the search keys the same way)."""
    head = unit["head"] or {}
    for pos, key, peeled in scope_keys(head_scope_text(head.get("text") or "")):
        yield (unit["id"], SCOPE_HEAD, 0, pos, key, peeled)
    for n, ex in enumerate(unit["examples"], 1):
        for pos, key, peeled in scope_keys(ex["quote"]):
            yield (unit["id"], SCOPE_QUOTATION, n, pos, key, peeled)


def write_sidecar(units_result: dict, index_result: dict, bindings: dict, path: Path) -> dict:
    """Write the sidecar atomically from the two candidates; returns its meta."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".building", dir=path.parent)
    os.close(fd)
    tmp = Path(name)
    conn = sqlite3.connect(tmp)
    try:
        conn.executescript(DDL)
        units = units_result["units"]
        for order, unit in enumerate(units):
            unit = printed_order(unit)
            head = unit["head"] or {}
            gloss = unit["gloss"]
            first = unit["locations"][0]
            conn.execute(
                "INSERT INTO units VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (unit["id"], order, unit["kind"], unit["letter"], unit["parent"], head.get("text"), head.get("plain"),
                 head.get("root"), head.get("root_source"), head.get("root_candidate"), head.get("lemma"), unit["binyan"],
                 gloss["definition"], unit_references(unit), gloss["foreign"], unit["text"], first["printed_page"],
                 f"{VIEWER_BASE}{first['source_id']}.pdf#page={first['pdf_page']}", json.dumps(unit["flags"], ensure_ascii=False),
                 json.dumps(unit["locations"], ensure_ascii=False), len(unit["examples"])))
            conn.executemany("INSERT INTO unit_keys VALUES (?,?)", ((key, unit["id"]) for key in head.get("keys", [])))
            conn.executemany("INSERT INTO unit_roots VALUES (?,?)",
                             ((dotted, unit["id"]) for dotted in dict.fromkeys(head.get("root_dotted", []))))
            conn.executemany("INSERT INTO quotations VALUES (?,?,?,?,?,?,?)",
                             ((unit["id"], n, ex["quote"], normalize_ja(ex["quote"]), ex["translation"], quotation_locator(ex), quotation_foreign(unit, n))
                              for n, ex in enumerate(unit["examples"], 1)))
            conn.executemany("INSERT INTO tokens VALUES (?,?,?,?,?,?)", token_rows(unit))
        conn.executemany("INSERT INTO unit_forms VALUES (?,?,?)", unit_form_rows(conn))
        tokens = rebuild_expression_tokens(conn, "friedman")
        rows = 0
        for row in index_result["rows"]:
            for root in row["roots"] or [{"text": "", "dotted": "", "key": "", "alternatives": 0}]:
                rows += 1
                conn.execute("INSERT INTO index_forms VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                             (rows, row["form"], row["form_key"], root["text"], root["dotted"], root["key"],
                              root["alternatives"], row["pdf_page"], row["printed_page"], row["url"], row["line"]))
        build_spelling_index(conn, [r[0] for r in conn.execute("SELECT key FROM unit_keys UNION SELECT key FROM unit_forms UNION SELECT form_key FROM index_forms")])
        meta = {"keying_version": str(KEYING_VERSION), "schema": SIDECAR_SCHEMA, "sidecar_version": str(SIDECAR_VERSION), "status": STATUS,
                "credit_he": CREDIT_HE, "built_at": datetime.now(timezone.utc).isoformat(),
                "normalizer_version": str(NORMALIZER_VERSION), "recovery_version": str(units_result["recovery_version"]),
                "index_version": str(index_result["index_version"]), "viewer_base": VIEWER_BASE,
                "units": str(len(units)), "entries": str(sum(u["kind"] == "entry" for u in units)),
                "index_rows": str(rows), "tokens": str(tokens), **bindings}
        conn.executemany("INSERT INTO meta VALUES (?,?)", meta.items())
        conn.commit()
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok" or conn.execute("PRAGMA foreign_key_check").fetchall():
            raise ValueError("Sidecar failed its integrity checks")
        conn.close()
        os.replace(tmp, path)
        return meta
    finally:
        conn.close()
        tmp.unlink(missing_ok=True)


def build(directory: Path, path: Path | None = None) -> dict:
    friedman_units.verify(directory)
    friedman_index.verify(directory)
    candidate = directory / "candidate"
    bindings = {"units_sha256": sha256((candidate / "units.json").read_bytes()),
                "index_sha256": sha256((candidate / "index_forms.json").read_bytes()),
                "manifest_sha256": sha256((directory / "source_manifest.json").read_bytes())}
    return write_sidecar(read_json(candidate / "units.json"), read_json(candidate / "index_forms.json"), bindings,
                         path or directory / SIDECAR_FILENAME)


def verify(directory: Path, path: Path | None = None) -> dict:
    """The sidecar is bound to the verified candidates and reproduces their keys and counts."""
    path = path or directory / SIDECAR_FILENAME
    candidate = directory / "candidate"
    conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok" or conn.execute("PRAGMA foreign_key_check").fetchall():
            raise ValueError("Sidecar failed its integrity checks")
        meta = dict(conn.execute("SELECT key, value FROM meta"))
        if meta.get("schema") != SIDECAR_SCHEMA or meta.get("sidecar_version") != str(SIDECAR_VERSION):
            raise ValueError("Sidecar must be rebuilt with the current sidecar version")
        if meta.get("status") != STATUS or meta.get("credit_he") != CREDIT_HE:
            raise ValueError("Sidecar status or credit missing")
        for key, file in (("units_sha256", candidate / "units.json"), ("index_sha256", candidate / "index_forms.json"),
                          ("manifest_sha256", directory / "source_manifest.json")):
            if meta.get(key) != sha256(file.read_bytes()):
                raise ValueError(f"Sidecar is not bound to the current {file.name}")
        units = read_json(candidate / "units.json")["units"]
        if conn.execute("SELECT COUNT(*) FROM units").fetchone()[0] != len(units) or meta.get("units") != str(len(units)):
            raise ValueError("Unit count differs from the candidate")
        stored_keys = {}
        for key, unit_id in conn.execute("SELECT key, unit_id FROM unit_keys"):
            stored_keys.setdefault(unit_id, set()).add(key)
        quoted = dict(conn.execute("SELECT unit_id, COUNT(*) FROM quotations GROUP BY unit_id"))
        for unit in units:
            expected = set((unit["head"] or {}).get("keys", []))
            if stored_keys.get(unit["id"], set()) != expected or quoted.get(unit["id"], 0) != len(unit["examples"]):
                raise ValueError(f"Keys or quotations of {unit['id']} differ from the candidate")
        rows = read_json(candidate / "index_forms.json")["rows"]
        expected_rows = sum(max(1, len(row["roots"])) for row in rows)
        if conn.execute("SELECT COUNT(*) FROM index_forms").fetchone()[0] != expected_rows:
            raise ValueError("Index row count differs from the candidate")
        expected_tokens = len(expression_material(conn, "friedman")[0])
        if conn.execute("SELECT COUNT(*) FROM tokens").fetchone()[0] != expected_tokens or meta.get("tokens") != str(expected_tokens):
            raise ValueError("Token rows differ from the candidate's words")
        return {"verified": True, "units": len(units), "index_rows": expected_rows, "tokens": expected_tokens,
                "status": meta["status"]}
    finally:
        conn.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("build", "verify"))
    parser.add_argument("--directory", type=Path, default=ROOT / "friedman_data")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args(argv)
    try:
        result = build(args.directory, args.output) if args.command == "build" else verify(args.directory, args.output)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, KeyError, sqlite3.Error) as exc:
        print(f"Friedman sidecar failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
