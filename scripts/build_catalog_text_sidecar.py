#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build ``catalog_data/catalog_text.db``, the catalogue text search sidecar.

Plan: ``docs/plans/CATALOGUE_TEXT_SEARCH_PLAN.md`` (§4.1). It is a DERIVED file. Every row comes
from four read-only inputs the apps already carry, and the build changes none of them:

- the FJMS sidecar (``fist_data/fjms_enrichment.db``): work titles, titles, comments, colophons,
  authors, places, running titles, free descriptions, full catalogue texts, textual frames,
  mentions, and the automatic translations (``fjms_translations``, except the person
  descriptions, which describe people, not manuscripts). The work-title and author translations
  are keyed by the title id and the author's text, not by a manuscript, and go to every
  manuscript whose catalogue record carries that title or author (``ENTITY_KEYED_TRANSLATIONS``);
- the PGP sidecar (``pgp_data/pgp.db``): each document's English description, for every
  manuscript the document lies on (``document_fragments.sys_id``). PGP's Hebrew translations are
  NOT read: they are withheld by the owner's decision of 2026-09-21
  (``docs/plans/PGP_TRANSLATION_QUALITY.md``);
- ``libraries.csv``: the Hebrew title (column 7, ``titles_non_placeholder``);
- ``libraries_translations.db``: the automatic English title.

One row per (sys_id, source, field, language, automatic, text) in ``records``, and the same row's
normalized words (``shared.catalog_text_normalize.key``) in the contentless FTS5 table
``catalog_fts``. Each row's words go in one of three columns by weight: ``title``, ``description``
or ``fulltext`` (``COLUMN_OF``). A text identical, after normalization, to one already stored for
the same manuscript is stored once. Originals are read before translations, so an automatic
translation that repeats an original adds nothing. Placeholder titles and authors ("Cannot be
determined from the catalog", לא ידוע, לא מזוהה, "Unknown", ``PLACEHOLDER_KEYS``) and rows whose
sys_id is not a corpus sys_id are left out and counted.

    python scripts/build_catalog_text_sidecar.py
    python scripts/verify_catalog_text_sidecar.py --db catalog_data/catalog_text.db --expect-rows <N>

The defaults are the repository's own paths; ``--fjms``, ``--pgp``, ``--libraries-csv``,
``--title-translations`` and ``--out`` override them. The build FAILS (exit 2) on a missing or
empty input, and REFUSES (exit 3) to replace an existing sidecar holding more rows unless
``--force`` is given. The file is written beside the target under a temporary name and renamed
into place only once complete, so a failed build leaves the served file as it was.
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import hashlib
import os
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.catalog_text_normalize import NORMALIZER_VERSION, is_hebrew_text, key  # noqa: E402
from shared.sys_id_patterns import CORPUS_SYS_ID_RE  # noqa: E402

#: The schema this builder writes into ``meta.version``; ``shared.catalog_text_service`` reads
#: only this version (tests/test_catalog_text_service.py pins the two equal).
SCHEMA_VERSION = "1.0.0"
#: The FTS5 tokenizer. The keys are already folded, so the tokenizer only splits on the spaces.
TOKENIZER = "unicode61 remove_diacritics 0"
FTS_COLUMNS = ("title", "description", "fulltext")

DEFAULT_FJMS = ROOT / "fist_data" / "fjms_enrichment.db"
DEFAULT_PGP = ROOT / "pgp_data" / "pgp.db"
DEFAULT_LIBRARIES_CSV = ROOT / "libraries.csv"
DEFAULT_TITLE_TRANSLATIONS = ROOT / "libraries_translations.db"
DEFAULT_OUT = ROOT / "catalog_data" / "catalog_text.db"

EXIT_OK = 0
EXIT_SOURCE = 2
EXIT_REFUSED = 3
EXIT_OUTPUT = 4

#: (source, field) -> the FTS column its words go in. A field not listed is a build error.
COLUMN_OF = {
    ("fjms", "GenizahTitle"): "title",
    ("fjms", "GenizahTitleEng"): "title",
    ("fjms", "GenizahTitleEngTitle"): "title",
    ("fjms", "Title"): "title",
    ("fjms", "TitleHeb"): "title",
    ("fjms", "RunningTitle"): "title",
    ("fjms", "FreeDesc"): "description",
    ("fjms", "TextualFrame"): "description",
    ("fjms", "Comment"): "description",
    ("fjms", "Colophon"): "description",
    ("fjms", "AuthorText"): "description",
    ("fjms", "CopyPlace"): "description",
    ("fjms", "Mention"): "description",
    ("fjms", "FullText"): "fulltext",
    ("pgp", "description"): "description",
    ("libraries", "title"): "title",
    ("libraries", "title_en"): "title",
}
SOURCES = ("fjms", "pgp", "libraries")
#: Translated FJMS fields left out: they describe people, not manuscripts.
EXCLUDED_TRANSLATION_FIELDS = ("PersonEngDesc", "PersonHebDesc")
#: Title and author segments that only say the catalogue could not decide (compared by key, any
#: case or period). FJMS writes the unknown work as GenizahTitleId -1 (לא ידוע) or -999 (לא מקודד /
#: Not Coded), the unknown author as לא מזוהה, translated "Unknown": indexed, they would put
#: "Unknown" on about a thousand manuscripts.
PLACEHOLDER_KEYS = frozenset({
    key("Cannot be determined from the catalog"),
    key("לא ניתן להכרעה מתוך הקטלוג"),
    key("לא ידוע"),
    key("לא ניתן לדעת"),
    key("לא מזוהה"),
    key("לא מקודד"),
    key("Not Coded"),
    key("Unknown"),
})
#: The fields whose values may be placeholders (``PLACEHOLDER_KEYS``): titles and authors.
PLACEHOLDER_FIELDS = frozenset({
    "GenizahTitle", "GenizahTitleEng", "GenizahTitleEngTitle", "Title", "TitleHeb", "AuthorText",
    "title", "title_en",
})
#: Translated FJMS fields whose ``fjms_translations.alma_id`` is NOT a manuscript: the work title
#: is keyed by ``GenizahTitleId`` and the author by the author's text
#: (scripts/translate_fjms_catalog.py). Each translation goes to every manuscript whose catalogue
#: record carries that title or author.
ENTITY_KEYED_TRANSLATIONS = frozenset({"GenizahTitleEngTitle", "AuthorText"})
#: Columns of FJMS ``catalog`` read per record, with the field name each is stored under.
CATALOG_COLUMNS = (
    ("GenizahTitleOrgTitle", "GenizahTitle"),
    ("GenizahTitleEngTitle", "GenizahTitleEng"),
    ("Title", "Title"),
    ("TitleHeb", "TitleHeb"),
    ("TextualFrameHeb", "TextualFrame"),
    ("TextualFrameEng", "TextualFrame"),
    ("Comment", "Comment"),
    ("Colophon", "Colophon"),
    ("AuthorText", "AuthorText"),
    ("CopyPlace", "CopyPlace"),
)


class SourceError(Exception):
    """An input is missing, empty or not shaped as expected."""


def _open_ro(path: Path) -> sqlite3.Connection:
    if not path.is_file() or path.stat().st_size == 0:
        raise SourceError(f"missing or empty input: {path}")
    return sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _meta(conn: sqlite3.Connection) -> dict[str, str]:
    try:
        return {str(k): "" if v is None else str(v) for k, v in conn.execute("SELECT key, value FROM meta")}
    except sqlite3.Error:
        return {}


def drop_placeholders(title: str) -> str:
    """``title`` without the segments that only say the catalogue could not decide."""
    segments = [s for s in title.split(";") if s.strip() and key(s) not in PLACEHOLDER_KEYS]
    return ";".join(segments).strip()


class Writer:
    """Collects rows into the sidecar being built, deduplicated per manuscript."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.seen: set[bytes] = set()
        self.next_id = 0
        self.batch: list[tuple] = []
        self.counts: Counter = Counter()
        self.skipped: Counter = Counter()

    def add(self, sys_id, source: str, field: str, text, automatic: bool) -> None:
        if text is None:
            return
        text = str(text).strip()
        if not text:
            return
        sid = str(sys_id or "").strip()
        if not CORPUS_SYS_ID_RE.fullmatch(sid):
            self.skipped[f"{source}_bad_sys_id"] += 1
            return
        column = COLUMN_OF[(source, field)]
        if field in PLACEHOLDER_FIELDS:
            text = drop_placeholders(text)
            if not text:
                self.skipped[f"{source}_placeholder"] += 1
                return
        norm = key(text)
        if not norm:
            self.skipped[f"{source}_no_words"] += 1
            return
        # 16 bytes of blake2b over (sys_id, key): one text per manuscript, whatever field said it.
        digest = hashlib.blake2b(f"{sid}\x00{norm}".encode("utf-8"), digest_size=16).digest()
        if digest in self.seen:
            self.skipped[f"{source}_duplicate"] += 1
            return
        self.seen.add(digest)
        self.next_id += 1
        lang = "he" if is_hebrew_text(text) else "en"
        self.batch.append((self.next_id, sid, source, field, lang, 1 if automatic else 0, text, column, norm))
        self.counts[f"{source}_{'automatic' if automatic else 'original'}"] += 1
        if len(self.batch) >= 20000:
            self.flush()

    def flush(self) -> None:
        if not self.batch:
            return
        cur = self.conn.cursor()
        cur.executemany(
            "INSERT INTO records (id, sys_id, source, field, lang, automatic, text) VALUES (?,?,?,?,?,?,?)",
            [row[:7] for row in self.batch],
        )
        for column in FTS_COLUMNS:
            rows = [(row[0], row[8]) for row in self.batch if row[7] == column]
            if rows:
                cur.executemany(f"INSERT INTO catalog_fts (rowid, {column}) VALUES (?, ?)", rows)
        self.conn.commit()
        self.batch.clear()


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(f"""
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE records (
            id INTEGER PRIMARY KEY,
            sys_id TEXT NOT NULL,
            source TEXT NOT NULL,
            field TEXT NOT NULL,
            lang TEXT NOT NULL,
            automatic INTEGER NOT NULL,
            text TEXT NOT NULL
        );
        CREATE VIRTUAL TABLE catalog_fts USING fts5(
            {", ".join(FTS_COLUMNS)}, content='', tokenize='{TOKENIZER}'
        );
    """)


def read_fjms(path: Path, writer: Writer, facts: dict[str, str]) -> None:
    conn = _open_ro(path)
    try:
        meta = _meta(conn)
        facts["input_fjms_version"] = meta.get("version", "")
        facts["input_fjms_created"] = meta.get("created", "")
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        needed = {"catalog", "catalog_running_titles", "catalog_free_desc", "catalog_full_texts",
                  "catalog_textual_frames", "catalog_mentions", "fjms_translations"}
        if needed - tables:
            raise SourceError(f"{path}: missing table(s) {sorted(needed - tables)}")
        columns = ", ".join(c for c, _ in CATALOG_COLUMNS)
        for row in conn.execute(f"SELECT AlmaId, {columns} FROM catalog"):
            for (_, field), value in zip(CATALOG_COLUMNS, row[1:]):
                writer.add(row[0], "fjms", field, value, automatic=False)
        for sid, value in conn.execute("SELECT AlmaId, RunningTitle FROM catalog_running_titles"):
            writer.add(sid, "fjms", "RunningTitle", value, automatic=False)
        for sid, value in conn.execute("SELECT AlmaId, FreeDesc FROM catalog_free_desc"):
            writer.add(sid, "fjms", "FreeDesc", value, automatic=False)
        for sid, value in conn.execute("SELECT AlmaId, FullText FROM catalog_full_texts"):
            writer.add(sid, "fjms", "FullText", value, automatic=False)
        for sid, heb, eng in conn.execute("SELECT AlmaId, TextualFrameHeb, TextualFrameEng FROM catalog_textual_frames"):
            writer.add(sid, "fjms", "TextualFrame", heb, automatic=False)
            writer.add(sid, "fjms", "TextualFrame", eng, automatic=False)
        for sid, mention, desc in conn.execute("SELECT AlmaId, Mention, MentionDesc FROM catalog_mentions"):
            writer.add(sid, "fjms", "Mention", " ".join(str(x) for x in (mention, desc) if x), automatic=False)
    finally:
        conn.close()


def read_fjms_translations(path: Path, writer: Writer) -> None:
    conn = _open_ro(path)
    try:
        # The work-title and author translations are keyed by the title id and the author's text
        # (ENTITY_KEYED_TRANSLATIONS); these maps lead each one to its manuscripts. A negative
        # title id is FJMS's "unknown" / "not coded" and leads nowhere.
        by_title: dict[str, list[str]] = defaultdict(list)
        for title_id, alma in conn.execute(
            "SELECT DISTINCT GenizahTitleId, AlmaId FROM catalog "
            "WHERE GenizahTitleId IS NOT NULL AND GenizahTitleId >= 0 ORDER BY 1, 2"
        ):
            by_title[str(title_id)].append(alma)
        by_author: dict[str, list[str]] = defaultdict(list)
        for author, alma in conn.execute(
            "SELECT DISTINCT AuthorText, AlmaId FROM catalog "
            "WHERE AuthorText IS NOT NULL AND AuthorText <> '' ORDER BY 1, 2"
        ):
            by_author[author].append(alma)
        placeholders = ",".join("?" * len(EXCLUDED_TRANSLATION_FIELDS))
        for entity, field, value in conn.execute(
            f"SELECT alma_id, field_name, translated_text FROM fjms_translations "
            f"WHERE field_name NOT IN ({placeholders}) ORDER BY id",
            EXCLUDED_TRANSLATION_FIELDS,
        ):
            if ("fjms", field) not in COLUMN_OF:
                raise SourceError(f"{path}: fjms_translations field {field!r} has no column; add it to COLUMN_OF")
            if field == "GenizahTitleEngTitle":
                targets = by_title.get(str(entity or "").strip(), [])
            elif field == "AuthorText":
                targets = by_author.get(entity or "", [])
            else:
                targets = [entity]
            if not targets:
                writer.skipped[f"fjms_{field}_unmapped"] += 1
                continue
            for sys_id in targets:
                writer.add(sys_id, "fjms", field, value, automatic=True)
    except sqlite3.Error as exc:
        raise SourceError(f"{path}: {exc}") from exc
    finally:
        conn.close()


def read_pgp(path: Path, writer: Writer, facts: dict[str, str]) -> None:
    conn = _open_ro(path)
    try:
        meta = _meta(conn)
        facts["input_pgp_version"] = meta.get("version", "")
        facts["input_pgp_upstream_commit"] = meta.get("upstream_commit", "")
        facts["input_pgp_upstream_committed"] = meta.get("upstream_committed", "")
        facts["input_pgp_documents_count"] = meta.get("documents_count", "")
        for sid, description in conn.execute(
            "SELECT DISTINCT df.sys_id, d.description FROM documents d "
            "JOIN document_fragments df ON df.document_id = d.pgpid "
            "WHERE d.description IS NOT NULL AND d.description <> '' ORDER BY d.pgpid, df.sys_id"
        ):
            writer.add(sid, "pgp", "description", description, automatic=False)
    except sqlite3.Error as exc:
        raise SourceError(f"{path}: {exc}") from exc
    finally:
        conn.close()


def read_libraries_csv(path: Path, writer: Writer, facts: dict[str, str]) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        raise SourceError(f"missing or empty input: {path}")
    facts["input_libraries_csv_sha256"] = _sha256(path)
    rows = 0
    with path.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.reader(fh):
            if not row or not CORPUS_SYS_ID_RE.fullmatch(row[0].strip()):
                continue  # the header, or a blank line
            rows += 1
            if len(row) > 7 and row[7].strip():
                writer.add(row[0], "libraries", "title", drop_placeholders(row[7]), automatic=False)
    facts["input_libraries_csv_records"] = str(rows)


def read_title_translations(path: Path, writer: Writer, facts: dict[str, str]) -> None:
    facts["input_title_translations_sha256"] = _sha256(path) if path.is_file() else ""
    conn = _open_ro(path)
    try:
        meta = _meta(conn)
        facts["input_title_translations_created"] = meta.get("created_at", "")
        for sid, english in conn.execute(
            "SELECT system_number, english_title FROM title_translations "
            "WHERE english_title IS NOT NULL AND english_title <> '' ORDER BY system_number"
        ):
            writer.add(sid, "libraries", "title_en", drop_placeholders(english), automatic=True)
    except sqlite3.Error as exc:
        raise SourceError(f"{path}: {exc}") from exc
    finally:
        conn.close()


def existing_rows(path: Path) -> int | None:
    """``meta.rows_total`` of an existing sidecar at ``path``, or None when there is none."""
    if not path.is_file() or path.stat().st_size == 0:
        return None
    try:
        conn = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
        try:
            value = dict(conn.execute("SELECT key, value FROM meta").fetchall()).get("rows_total", "")
        finally:
            conn.close()
    except sqlite3.Error:
        return None
    return int(value) if str(value).isdigit() else None


def build(out: Path, fjms: Path, pgp: Path, libraries_csv: Path, title_translations: Path) -> dict[str, str]:
    """Write the sidecar to ``out`` (which must not exist) and return its ``meta``."""
    started = time.time()
    conn = sqlite3.connect(out)
    try:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        create_schema(conn)
        writer = Writer(conn)
        facts: dict[str, str] = {}
        # Originals first, every source; then the automatic translations, so a translation that
        # repeats an original of the same manuscript is the one left out.
        read_fjms(fjms, writer, facts)
        read_pgp(pgp, writer, facts)
        read_libraries_csv(libraries_csv, writer, facts)
        read_fjms_translations(fjms, writer)
        read_title_translations(title_translations, writer, facts)
        writer.flush()
        conn.execute("INSERT INTO catalog_fts (catalog_fts) VALUES ('optimize')")
        conn.execute("CREATE INDEX idx_records_sys_id ON records (sys_id)")
        manuscripts = conn.execute("SELECT COUNT(DISTINCT sys_id) FROM records").fetchone()[0]
        meta = {
            "version": SCHEMA_VERSION,
            "normalizer_version": str(NORMALIZER_VERSION),
            "tokenizer": TOKENIZER,
            "fts_columns": ",".join(FTS_COLUMNS),
            "built_at": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
            "build_seconds": str(round(time.time() - started)),
            "rows_total": str(writer.next_id),
            "manuscripts_total": str(manuscripts),
            "excluded": "PGP Hebrew translations (withheld 2026-09-21); FJMS person descriptions; placeholder titles",
            **facts,
        }
        for source in SOURCES:
            for kind in ("original", "automatic"):
                meta[f"rows_{source}_{kind}"] = str(writer.counts.get(f"{source}_{kind}", 0))
        for name, count in sorted(writer.skipped.items()):
            meta[f"skipped_{name}"] = str(count)
        conn.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", sorted(meta.items()))
        conn.commit()
        return meta
    finally:
        conn.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--fjms", default=str(DEFAULT_FJMS))
    ap.add_argument("--pgp", default=str(DEFAULT_PGP))
    ap.add_argument("--libraries-csv", default=str(DEFAULT_LIBRARIES_CSV))
    ap.add_argument("--title-translations", default=str(DEFAULT_TITLE_TRANSLATIONS))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--force", action="store_true", help="replace an existing sidecar even when it holds more rows")
    args = ap.parse_args(argv)

    out = Path(args.out)
    tmp = out.with_name(f"{out.name}.building-{os.getpid()}")
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        if tmp.exists():
            tmp.unlink()
        meta = build(tmp, Path(args.fjms), Path(args.pgp), Path(args.libraries_csv), Path(args.title_translations))
    except SourceError as exc:
        tmp.unlink(missing_ok=True)
        print(f"FAIL: {exc}")
        return EXIT_SOURCE
    except (sqlite3.Error, OSError) as exc:
        tmp.unlink(missing_ok=True)
        print(f"FAIL: could not write {out}: {exc}")
        return EXIT_OUTPUT

    old = existing_rows(out)
    new = int(meta["rows_total"])
    if old is not None and new < old and not args.force:
        tmp.unlink(missing_ok=True)
        print(f"REFUSED: {out} holds {old:,} rows and this build {new:,}; pass --force to replace it")
        return EXIT_REFUSED
    try:
        os.replace(tmp, out)
    except OSError as exc:
        tmp.unlink(missing_ok=True)
        print(f"FAIL: could not replace {out}: {exc}")
        return EXIT_OUTPUT
    print(f"wrote    {out} ({out.stat().st_size:,} bytes) in {meta['build_seconds']} s")
    print(f"rows     {new:,} over {int(meta['manuscripts_total']):,} manuscripts")
    for k in sorted(meta):
        if k.startswith(("rows_", "skipped_", "input_")):
            print(f"  {k} = {meta[k]}")
    print("RESULT: OK")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
