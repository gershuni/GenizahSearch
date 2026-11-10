#!/usr/bin/env python3
"""Archive and inspect Friedman's PDFs; build an OFFLINE page-search candidate.

This is preparation, not a semantic dictionary edition or a production sidecar.
The original PDFs, raw extraction, typography and source positions are retained.
No root, binyan, sense or phrase boundaries are inferred. Extracted text and
typographic candidates require review before use in a dictionary reader.

    python scripts/prepare_friedman_dictionary.py fetch
    python scripts/prepare_friedman_dictionary.py build
    python scripts/prepare_friedman_dictionary.py verify
    python scripts/prepare_friedman_dictionary.py search 'לא אבד' --gap 3

Only build requires PyMuPDF (pip install pymupdf). Fetch/verify/search use stdlib
and the project's existing JA normalizer. Source files are never overwritten.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.blau_normalize import NORMALIZER_VERSION, normalize_ja  # noqa: E402
from shared.friedman_service import ordered_match as _ordered_match  # noqa: E402

SITE_URL = "https://ja.genizah.org/JA_Dictionary_pdfjs/JA_Dictionary.html"
PDF_BASE = "https://ja.genizah.org/JA_Dictionary_pdfjs/web/PDF/"
VIEWER_BASE = "https://ja.genizah.org/JA_Dictionary_pdfjs/web/viewer.html?file=PDF/"
BODY_IDS = tuple("1 2 3 3_ 4 4_ 5 6 7 8 9 9_ 10 20 20_ 30 40 50 60 70 80 90 90_ 100 200 300 400 400_".split())
AUX_IDS = tuple("BookTitle English Introduction Shortcuts_Signs Appendix Shortcuts_Bibl Value_Keys Certificates MissingManuscripts Articles".split())
SOURCE_IDS = BODY_IDS + AUX_IDS
SCHEMA_VERSION = "friedman-preparation-1"
EXTRACTOR_VERSION = 3
INDEX_RECOVERY_SHA256 = "05ffed28f64dca8c0568e80713399eede911a9d3d0672949b7adeba92ca56155"
# The inspected Advhadasa alphabet in this index (C224..C250 with legacy glyph
# aliases). This is NOT a universal conversion for fonts named Hadassah.
INDEX_GLYPHS = (
    "C224 AE C226 ordfeminine C228 C229 C230 C231 Lslash Oslash OE ordmasculine "
    "C236 C237 C238 C239 C240 ae C242 C243 C244 dotlessi C246 C247 lslash oslash oe"
).split()
INDEX_LETTERS = {glyph: chr(0x05D0 + i) for i, glyph in enumerate(INDEX_GLYPHS)}
CREDIT_HE = (
    'מרדכי עקיבא פרידמן, מילון הערבית-היהודית מימי הביניים לתעודות הגניזה של ספר הודו '
    'ולטקסטים אחרים, ירושלים: מכון בן-צבי לחקר קהילות ישראל במזרח '
    'וקרן הרב דוד משה ועמליה רוזן, תשע"ו'
)
DISCLAIMER = (
    "Extraction candidate: page matches, not identified lexical senses. Hebrew definitions "
    "and quotations share the search index. Check the linked original page. No lemmatization."
)
MAX_DOWNLOAD = 64 * 1024 * 1024
MAX_QUERY_WORDS = 12


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value) -> None:
    """Atomically publish a report; preserve the previous copy on failure."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def download(url: str) -> bytes:
    with urlopen(url, timeout=45) as response:
        data = response.read(MAX_DOWNLOAD + 1)
    if len(data) > MAX_DOWNLOAD:
        raise ValueError(f"Source exceeds the download limit: {url}")
    return data


def store_source(path: Path, data: bytes) -> None:
    if path.exists():
        if path.read_bytes() != data:
            raise ValueError(f"Source changed; use a new --directory to preserve this snapshot: {path}")
        return
    # Exclusive creation prevents an interrupted fetch from replacing a source.
    with path.open("xb") as stream:
        stream.write(data)


def fetch(directory: Path) -> dict:
    download_dir = directory / "download"
    download_dir.mkdir(parents=True, exist_ok=True)
    html = download(SITE_URL)
    # Check the declared inventory, but never turn HTML-supplied names into URLs/paths.
    names = set(re.findall(r"LoadPDF\('([A-Za-z0-9_]+)'", html.decode("utf-8-sig")))
    if names != set(SOURCE_IDS):
        raise ValueError(f"Source inventory changed: missing={set(SOURCE_IDS) - names}, extra={names - set(SOURCE_IDS)}")
    store_source(download_dir / "JA_Dictionary.html", html)
    rows = []
    for source_id in SOURCE_IDS:
        url = f"{PDF_BASE}{source_id}.pdf"
        data = download(url)
        if not data.startswith(b"%PDF-"):
            raise ValueError(f"Not a PDF: {url}")
        store_source(download_dir / f"{source_id}.pdf", data)
        rows.append({"id": source_id, "kind": "body" if source_id in BODY_IDS else "auxiliary",
                     "url": url, "bytes": len(data), "sha256": sha256(data)})
        print(f"archived {source_id}.pdf ({len(data):,} bytes)", flush=True)
    manifest = {"schema": SCHEMA_VERSION, "fetched_at": datetime.now(timezone.utc).isoformat(),
                "site_url": SITE_URL, "site_sha256": sha256(html), "credit_he": CREDIT_HE,
                "sources": rows}
    write_json(directory / "source_manifest.json", manifest)
    return manifest


def checked_sources(directory: Path) -> dict:
    manifest = read_json(directory / "source_manifest.json")
    if manifest.get("schema") != SCHEMA_VERSION or manifest.get("credit_he") != CREDIT_HE:
        raise ValueError("Incompatible source manifest or missing credit")
    sources = manifest["sources"]
    if [row["id"] for row in sources] != list(SOURCE_IDS):
        raise ValueError("Incomplete or reordered source manifest")
    for row in sources:
        source_id = row["id"]
        if row["url"] != f"{PDF_BASE}{source_id}.pdf":
            raise ValueError(f"Unexpected source URL: {source_id}")
        if row["kind"] != ("body" if source_id in BODY_IDS else "auxiliary"):
            raise ValueError(f"Incorrect source kind: {source_id}")
        data = (directory / "download" / f"{source_id}.pdf").read_bytes()
        if not data.startswith(b"%PDF-") or len(data) != row["bytes"] or sha256(data) != row["sha256"]:
            raise ValueError(f"Source checksum mismatch: {source_id}")
    html = (directory / "download" / "JA_Dictionary.html").read_bytes()
    if sha256(html) != manifest["site_sha256"]:
        raise ValueError("Source HTML checksum mismatch")
    return manifest


def inspect_page(page) -> dict:
    """Keep source order verbatim; separately collect typographic heading candidates.

    Hadassah-Bold marks heads but also cross-references. These are explicitly
    candidates, never roots/entries. Vowel-font spaces are not copied into the
    bare heading key: that splits אَ בَ ד into three words in ordinary extraction.
    Spans are reordered geometrically only in the separately labelled candidate.
    """
    raw = page.get_text("text", sort=False)
    blocks = page.get_text("dict", sort=False)["blocks"]
    headings = []
    for block in blocks:
        for line in block.get("lines", []):
            if line["bbox"][1] < page.rect.height * 0.15 or line["bbox"][3] > page.rect.height * 0.88:
                continue  # running headings and page numbers, retained in raw extraction
            spans = [s for s in line["spans"] if "Hadassah-Bold" in s["font"] and s["size"] >= 13]
            spans.sort(key=lambda s: s["bbox"][2], reverse=True)
            text = "".join(s["text"] for s in spans).strip()
            if normalize_ja(text):
                headings.append({"text": text, "key": normalize_ja(text), "bbox": line["bbox"],
                                 "status": "typographic_candidate"})
    controls = sum(ord(ch) < 32 and ch not in "\n\t\r" for ch in raw)
    letters = sum(ch.isalpha() for ch in raw)
    hebrew = sum("א" <= ch <= "ת" for ch in raw)
    # Deliberately coarse: this detects the corrupt index, NOT transcription correctness.
    searchable = bool(hebrew >= 20 and hebrew / max(letters, 1) >= 0.5 and controls / max(len(raw), 1) < 0.01)
    footer = []
    for block in blocks:
        for line in block.get("lines", []):
            if line["bbox"][1] > page.rect.height * 0.88:
                # Even pages extract the bracket pair as ]12[; the digits themselves
                # stay in numerical order. Do not reverse the number or infer an offset.
                text = "".join(s["text"] for s in line["spans"])
                for match in re.finditer(r"\[(\d{1,4})\]|\](\d{1,4})\[", text):
                    footer.append(match.group(1) or match.group(2))
    # Keep every span, font and bbox, excluding embedded image bytes (PDF is retained).
    text_blocks = [block for block in blocks if block.get("type") == 0]
    return {"raw_text": raw, "blocks": text_blocks, "heading_candidates": headings,
            "printed_page": footer[0] if len(footer) == 1 else None,
            "text_searchable": searchable, "hebrew_letters": hebrew, "control_characters": controls}


def recover_index_letters(doc, source_sha256: str) -> dict:
    """Recover the pinned index's Hebrew letters in memory, AFTER raw extraction.

    Replace only the letter mappings in the fonts' ToUnicode streams. The PDF
    drawing/glyphs stay untouched. Nonletter mappings, including unresolved
    diacritic shapes, stay as supplied. No entry/root assignments are inferred.
    """
    if source_sha256 != INDEX_RECOVERY_SHA256:
        return {"status": "not_attempted_unreviewed_source"}
    fonts = {font[0]: font for page in doc for font in page.get_fonts()
             if "Advhadasa-" in font[3]}
    if not fonts:
        raise ValueError("Pinned root index no longer has the inspected fonts")
    recovered = []
    for xref, font in fonts.items():
        _, encoding_ref = doc.xref_get_key(xref, "Encoding")
        encoding = doc.xref_object(int(encoding_ref.split()[0]))
        difference = re.search(r"/Differences\s*\[([^]]+)\]", encoding, re.S)
        if not difference:
            raise ValueError("Missing encoding differences in pinned index")
        mapping = {}
        code = None
        for token in re.findall(r"\d+|/[^\s/]+", difference.group(1)):
            if token.isdigit():
                code = int(token)
            else:
                if code is None:
                    raise ValueError("Malformed index font encoding")
                if token[1:] in INDEX_LETTERS:
                    mapping[code] = ord(INDEX_LETTERS[token[1:]])
                code += 1
        _, unicode_ref = doc.xref_get_key(xref, "ToUnicode")
        cmap_xref = int(unicode_ref.split()[0])
        cmap = doc.xref_stream(cmap_xref).decode("ascii")
        # Rebuild bfchar blocks so no code appears twice with conflicting values.
        pairs = {}
        for block in re.findall(r"\d+\s+beginbfchar(.*?)endbfchar", cmap, re.S):
            for source, target in re.findall(r"<([0-9a-fA-F]+)>\s*<([0-9a-fA-F]+)>", block):
                pairs[int(source, 16)] = target
        pairs.update({code: f"{value:04X}" for code, value in mapping.items()})
        cmap = re.sub(r"\d+\s+beginbfchar.*?endbfchar", "", cmap, flags=re.S)
        # C224-mapped letters can have low character codes outside the old range.
        cmap = re.sub(r"begincodespacerange.*?endcodespacerange",
                      "begincodespacerange <00> <FF> endcodespacerange", cmap, flags=re.S)
        block = f"{len(pairs)} beginbfchar\n" + "\n".join(
            f"<{code:02X}> <{value}>" for code, value in sorted(pairs.items())) + "\nendbfchar\n"
        cmap = cmap.replace("endcmap", block + "endcmap")
        doc.update_stream(cmap_xref, cmap.encode("ascii"))
        recovered.append({"font": font[3], "letters_mapped": len(mapping)})
    # Reopen to discard MuPDF's cached font decoding; the source file is never saved.
    import fitz

    with fitz.open(stream=doc.tobytes(), filetype="pdf") as corrected:
        pages = [{"pdf_page": n, "text": page.get_text("text", sort=False),
                  "blocks": [b for b in page.get_text("dict", sort=False)["blocks"] if b.get("type") == 0]}
                 for n, page in enumerate(corrected, 1)]
    return {"status": "letter_recovery_candidate_not_lexical_mapping", "fonts": recovered,
            "unresolved": "Diacritic glyphs and three-column reading order need review; not indexed or served.",
            "pages": pages}


def pagination_summary(records: list[dict]) -> dict:
    """Report overlaps/gaps without silently removing source pages or inventing labels."""
    by_label = {}
    for row in records:
        if row["printed_page"] is not None:
            by_label.setdefault(int(row["printed_page"]), []).append(
                {"source_id": row["source_id"], "pdf_page": row["pdf_page"]})
    low, high = (min(by_label), max(by_label)) if by_label else (None, None)
    return {"first": low, "last": high, "distinct_printed_pages": len(by_label),
            "unlabelled_pdf_pages": sum(row["printed_page"] is None for row in records),
            "missing_within_range": [n for n in range(low, high + 1) if n not in by_label] if by_label else [],
            "overlaps": [{"printed_page": label, "locations": rows}
                         for label, rows in sorted(by_label.items()) if len(rows) > 1]}


def build(directory: Path) -> dict:
    import fitz

    manifest = checked_sources(directory)
    candidate = directory / "candidate"
    candidate.mkdir(parents=True, exist_ok=True)
    target = candidate / "pages.sqlite"
    fd, name = tempfile.mkstemp(prefix="pages.", suffix=".building", dir=candidate)
    os.close(fd)
    tmp = Path(name)
    conn = sqlite3.connect(tmp)
    summaries = []
    extracted = []
    pagination = []
    try:
        conn.executescript("""
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE pages (
                id INTEGER PRIMARY KEY, source_id TEXT NOT NULL, pdf_page INTEGER NOT NULL,
                printed_page TEXT, url TEXT NOT NULL, raw_text TEXT NOT NULL,
                normalized TEXT NOT NULL, heading_text TEXT NOT NULL,
                UNIQUE(source_id, pdf_page));
            CREATE TABLE positions (
                token TEXT NOT NULL, page_id INTEGER NOT NULL REFERENCES pages(id),
                position INTEGER NOT NULL, PRIMARY KEY(page_id, position));
            CREATE INDEX positions_token ON positions(token, page_id, position);
        """)
        conn.execute("PRAGMA foreign_keys=ON")
        metadata = {"schema": SCHEMA_VERSION, "normalizer_version": str(NORMALIZER_VERSION),
                    "extractor_version": str(EXTRACTOR_VERSION), "pymupdf_version": fitz.VersionBind,
                    "manifest_sha256": sha256((directory / "source_manifest.json").read_bytes()),
                    "credit_he": CREDIT_HE, "status": "offline_candidate_not_for_production",
                    "disclaimer": DISCLAIMER}
        conn.executemany("INSERT INTO meta VALUES (?, ?)", metadata.items())
        for source in manifest["sources"]:
            source_id = source["id"]
            with fitz.open(directory / "download" / f"{source_id}.pdf") as doc:
                pages = []
                for number, page in enumerate(doc, 1):
                    record = inspect_page(page)
                    record.update({"pdf_page": number,
                                   "url": f"{VIEWER_BASE}{source_id}.pdf#page={number}"})
                    pages.append(record)
                    if source_id in BODY_IDS:
                        pagination.append({"source_id": source_id, "pdf_page": number,
                                           "printed_page": record["printed_page"]})
                    if source_id not in BODY_IDS or not record["text_searchable"]:
                        continue
                    norm = normalize_ja(record["raw_text"])
                    heading_text = "\n".join(h["text"] for h in record["heading_candidates"])
                    cursor = conn.execute("INSERT INTO pages (source_id,pdf_page,printed_page,url,raw_text,normalized,heading_text) "
                                          "VALUES (?,?,?,?,?,?,?)", (source_id, number, record["printed_page"],
                                          record["url"], record["raw_text"], norm, heading_text))
                    conn.executemany("INSERT INTO positions VALUES (?,?,?)",
                                     ((token, cursor.lastrowid, pos) for pos, token in enumerate(norm.split())))
                result = {"source_id": source_id, "source_sha256": source["sha256"],
                          "extractor_version": EXTRACTOR_VERSION, "pages": pages}
                if source_id == "Value_Keys":
                    result["letter_recovery"] = recover_index_letters(doc, source["sha256"])
                output = candidate / f"{source_id}.json"
                write_json(output, result)
                extracted.append({"id": source_id, "sha256": sha256(output.read_bytes())})
                summaries.append({"id": source_id, "kind": source["kind"], "pages": len(pages),
                                  "searchable_text_pages": sum(p["text_searchable"] for p in pages),
                                  "heading_candidates": sum(len(p["heading_candidates"]) for p in pages),
                                  "unreadable_pages": [p["pdf_page"] for p in pages if not p["text_searchable"]],
                                  "missing_printed_page": [p["pdf_page"] for p in pages if p["printed_page"] is None]})
            print(f"extracted {source_id}: {len(pages)} pages", flush=True)
        conn.commit()
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("Candidate database integrity check failed")
        indexed = conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
        tokens = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
        conn.close()
        os.replace(tmp, target)
    finally:
        conn.close()
        tmp.unlink(missing_ok=True)
    report = {**metadata, "built_at": datetime.now(timezone.utc).isoformat(), "sources": summaries,
              "extractions": extracted, "database_sha256": sha256(target.read_bytes()),
              "indexed_body_pages": indexed, "indexed_tokens": tokens,
              "body_pagination": pagination_summary(pagination),
              "total_pages": sum(s["pages"] for s in summaries)}
    write_json(candidate / "audit.json", report)
    return report


def verify(directory: Path) -> dict:
    """Bind archive, extractions, index and audit by hash, then check every token."""
    manifest = checked_sources(directory)
    candidate = directory / "candidate"
    report = read_json(candidate / "audit.json")
    if report["schema"] != SCHEMA_VERSION or report["manifest_sha256"] != sha256((directory / "source_manifest.json").read_bytes()):
        raise ValueError("Audit is not bound to this source archive")
    if report["extractor_version"] != str(EXTRACTOR_VERSION) or report["normalizer_version"] != str(NORMALIZER_VERSION):
        raise ValueError("Candidate must be rebuilt with the current extractor/normalizer")
    if [row["id"] for row in report["extractions"]] != list(SOURCE_IDS):
        raise ValueError("Incomplete extraction inventory")
    if report["database_sha256"] != sha256((candidate / "pages.sqlite").read_bytes()):
        raise ValueError("Candidate database checksum mismatch")
    expected = {}
    pagination = []
    for source, extraction in zip(manifest["sources"], report["extractions"]):
        data = (candidate / f"{source['id']}.json").read_bytes()
        if sha256(data) != extraction["sha256"]:
            raise ValueError(f"Extraction checksum mismatch: {source['id']}")
        records = json.loads(data)
        if records["source_sha256"] != source["sha256"]:
            raise ValueError("Extraction is not bound to its PDF")
        if [p["pdf_page"] for p in records["pages"]] != list(range(1, len(records["pages"]) + 1)):
            raise ValueError("Missing or duplicate PDF page")
        for page in records["pages"]:
            if source["id"] in BODY_IDS:
                pagination.append({"source_id": source["id"], "pdf_page": page["pdf_page"],
                                   "printed_page": page["printed_page"]})
            if source["id"] in BODY_IDS and page["text_searchable"]:
                expected[(source["id"], page["pdf_page"])] = page
    if report.get("body_pagination") != pagination_summary(pagination):
        raise ValueError("Printed-page coverage differs from retained extraction")
    conn = sqlite3.connect((candidate / "pages.sqlite").resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok" or conn.execute("PRAGMA foreign_key_check").fetchall():
            raise ValueError("Candidate database failed integrity checks")
        metadata = dict(conn.execute("SELECT key,value FROM meta"))
        for key in ("schema", "normalizer_version", "extractor_version", "manifest_sha256", "credit_he", "status"):
            if metadata.get(key) != report[key]:
                raise ValueError(f"Candidate metadata mismatch: {key}")
        seen = set()
        token_count = 0
        for row in conn.execute("SELECT * FROM pages"):
            key = (row["source_id"], row["pdf_page"])
            page = expected.get(key)
            if page is None or row["raw_text"] != page["raw_text"] or row["url"] != page["url"] or row["printed_page"] != page["printed_page"]:
                raise ValueError(f"Page provenance mismatch: {key}")
            norm = normalize_ja(page["raw_text"])
            positions = [(r["token"], r["position"]) for r in conn.execute(
                "SELECT token,position FROM positions WHERE page_id=? ORDER BY position", (row["id"],))]
            if row["normalized"] != norm or positions != [(t, i) for i, t in enumerate(norm.split())]:
                raise ValueError(f"Search tokens differ from source extraction: {key}")
            token_count += len(positions)
            seen.add(key)
        if seen != set(expected) or len(seen) != report["indexed_body_pages"] or token_count != report["indexed_tokens"]:
            raise ValueError("Page/token counts differ from audit")
    finally:
        conn.close()
    return {"verified": True, "sources": len(SOURCE_IDS), "indexed_body_pages": len(expected),
            "indexed_tokens": token_count, "status": report["status"]}


def ordered_match(tokens: list[str], query: list[str], gap: int) -> tuple[int, ...] | None:
    """First ordered match with at most gap intervening tokens BETWEEN each pair.

    The matcher itself lives in ``shared.friedman_service`` (the sidecar's expression search
    uses it with keyed alternatives); this prototype matches plain page tokens with it.
    Repeated query words need distinct occurrences. This matches extracted page text only: it
    can cross lexical senses and is NOT automatic idiom recognition.
    """
    return _ordered_match(tokens, query, gap)


def search(directory: Path, query: str, gap: int = 0, limit: int = 10) -> dict:
    if not 0 <= gap <= 20 or not 1 <= limit <= 50:
        raise ValueError("gap must be 0..20; limit must be 1..50")
    words = normalize_ja(query).split()
    if not 1 <= len(words) <= MAX_QUERY_WORDS:
        raise ValueError(f"Use 1..{MAX_QUERY_WORDS} Hebrew-letter words")
    path = directory / "candidate" / "pages.sqlite"
    conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        metadata = dict(conn.execute("SELECT key,value FROM meta"))
        if metadata.get("schema") != SCHEMA_VERSION or metadata.get("normalizer_version") != str(NORMALIZER_VERSION):
            raise ValueError("Incompatible candidate database")
        unique = list(dict.fromkeys(words))
        marks = ",".join("?" for _ in unique)
        rows = conn.execute(f"SELECT p.* FROM pages p JOIN (SELECT page_id FROM positions WHERE token IN ({marks}) "
                            "GROUP BY page_id HAVING COUNT(DISTINCT token)=?) hits ON p.id=hits.page_id ORDER BY p.id",
                            (*unique, len(unique)))
        hits = []
        for row in rows:
            tokens = row["normalized"].split()
            positions = ordered_match(tokens, words, gap)
            if positions is None:
                continue
            hits.append({"source_id": row["source_id"], "pdf_page": row["pdf_page"],
                         "printed_page": row["printed_page"], "url": row["url"],
                         "normalized_context": " ".join(tokens[max(0, positions[0] - 6):positions[-1] + 7]),
                         "positions": positions, "match_type": "extracted_page_text"})
            if len(hits) > limit:
                break
        return {"query": query, "gap_between_words": gap, "disclaimer": DISCLAIMER,
                "credit_he": metadata["credit_he"], "hits": hits[:limit], "more": len(hits) > limit}
    finally:
        conn.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("fetch", "build", "verify", "search"))
    parser.add_argument("query", nargs="?")
    parser.add_argument("--directory", type=Path, default=ROOT / "friedman_data")
    parser.add_argument("--gap", type=int, default=0)
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args(argv)
    try:
        if args.command == "fetch":
            result = fetch(args.directory)
            print(f"Archived {len(result['sources'])} PDFs with SHA-256 checksums.")
        elif args.command == "build":
            result = build(args.directory)
            print(f"Candidate: {result['indexed_body_pages']} body pages; {result['total_pages']} total pages.")
        elif args.command == "verify":
            print(json.dumps(verify(args.directory), ensure_ascii=False, indent=2))
        else:
            if not args.query:
                parser.error("search requires a query")
            print(json.dumps(search(args.directory, args.query, args.gap, args.limit), ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, KeyError, sqlite3.Error) as exc:
        print(f"Friedman preparation failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
