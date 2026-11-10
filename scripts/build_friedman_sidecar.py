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
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import friedman_index, friedman_units  # noqa: E402
from scripts.prepare_friedman_dictionary import CREDIT_HE, VIEWER_BASE, read_json, sha256  # noqa: E402
from shared.blau_normalize import NORMALIZER_VERSION, normalize_ja  # noqa: E402
from shared.friedman_service import SCOPE_HEAD, SCOPE_QUOTATION, head_scope_text, scope_keys  # noqa: E402

SIDECAR_SCHEMA = "friedman-sidecar-2"
SIDECAR_VERSION = 2
STATUS = "candidate_not_reviewed_not_for_production"
SIDECAR_FILENAME = "friedman.db"

DDL = """
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
CREATE INDEX tokens_key ON tokens(key);
CREATE INDEX tokens_scope ON tokens(unit_id, scope, n, pos);
"""


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
            head = unit["head"] or {}
            gloss = unit["gloss"]
            first = unit["locations"][0]
            conn.execute(
                "INSERT INTO units VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (unit["id"], order, unit["kind"], unit["letter"], unit["parent"], head.get("text"), head.get("plain"),
                 head.get("root"), head.get("root_source"), head.get("root_candidate"), head.get("lemma"), unit["binyan"],
                 gloss["definition"], gloss["references"], gloss["foreign"], unit["text"], first["printed_page"],
                 f"{VIEWER_BASE}{first['source_id']}.pdf#page={first['pdf_page']}", json.dumps(unit["flags"], ensure_ascii=False),
                 json.dumps(unit["locations"], ensure_ascii=False), len(unit["examples"])))
            conn.executemany("INSERT INTO unit_keys VALUES (?,?)", ((key, unit["id"]) for key in head.get("keys", [])))
            conn.executemany("INSERT INTO unit_roots VALUES (?,?)",
                             ((dotted, unit["id"]) for dotted in dict.fromkeys(head.get("root_dotted", []))))
            conn.executemany("INSERT INTO quotations VALUES (?,?,?,?,?,?,?)",
                             ((unit["id"], n, ex["quote"], normalize_ja(ex["quote"]), ex["translation"], ex["locator"], ex["foreign"])
                              for n, ex in enumerate(unit["examples"], 1)))
            conn.executemany("INSERT INTO tokens VALUES (?,?,?,?,?,?)", token_rows(unit))
        tokens = conn.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
        rows = 0
        for row in index_result["rows"]:
            for root in row["roots"] or [{"text": "", "dotted": "", "key": "", "alternatives": 0}]:
                rows += 1
                conn.execute("INSERT INTO index_forms VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                             (rows, row["form"], row["form_key"], root["text"], root["dotted"], root["key"],
                              root["alternatives"], row["pdf_page"], row["printed_page"], row["url"], row["line"]))
        meta = {"schema": SIDECAR_SCHEMA, "sidecar_version": str(SIDECAR_VERSION), "status": STATUS,
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
        expected_tokens = sum(1 for unit in units for _ in token_rows(unit))
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
