# -*- coding: utf-8 -*-
"""A tiny, self-consistent PGP refresh tree for driving the pipeline scripts end to end.

Not a test module (no ``test_`` prefix). Imported by the tests that run the REAL ``main()``
of ``pgp_transcriptions_export.py``, ``import_pgp_full.py`` and ``update_doc_relation.py``
against a throwaway project root -- because every guard on this branch that was only
tested through its helpers turned out to be deletable at the call site with the suite
still green.

The tree mirrors what the scripts expect under a project root: ``libraries.csv`` at the
root, the three upstream CSVs plus their manifest and the FIST supplement under
``pgp_data/``, and (optionally) a derived ``transcriptions_linked.csv`` with its stamp.
Every CSV column a script reads is present; content is minimal but realistic enough for
the shelfmark matching to link one document and leave another unmatched.
"""
from __future__ import annotations

import hashlib
import json
import pathlib

COMMIT = "c0ffee" + "0" * 34

LIBRARIES_CSV = (
    "system_number,oxford_part_id,call_numbers,library_code,,,,titles_non_placeholder\n"
    "9900000001,,T-S 12.123,CUL,,,,Letter\n"
)

# 60+ characters: extract_transcriptions() drops content shorter than 50.
EDITION_TEXT = "בשם רחמן. כתאבי אליך יא סידי ומולאי אטאל אללה בקאך ואדאם עזך ותאיידך"
# No commas in either: the fixture CSVs are written as plain text, not csv-quoted.
TRANSLATION_TEXT = ("In the name of the Merciful. My letter to you my lord and master; may God "
                    "prolong your life and preserve your honour.")

DOCUMENTS_CSV = (
    "pgpid,shelfmark,type,description,languages_primary,url,has_transcription,has_translation,tags\n"
    "1001,T-S 12.123,Letter,A letter.,Judaeo-Arabic,https://geniza.princeton.edu/documents/1001/,Y,Y,\n"
    "1002,Moss. IX 1.1,Legal,A deed.,Hebrew,https://geniza.princeton.edu/documents/1002/,N,N,\n"
)

FRAGMENTS_CSV = (
    "shelfmark,pgpids,collection,library,library_abbrev,url,iiif_url\n"
    "T-S 12.123,1001,Taylor-Schechter,Cambridge University Library,CUL,,https://example.org/iiif/1\n"
    "Moss. IX 1.1,1002,Mosseri,Alliance Israelite Universelle,AIU,,\n"
)

FOOTNOTES_CSV = (
    "document_id,document,doc_relation,source,source_slug,location,url,content,emendations,notes\n"
    "1001,https://geniza.princeton.edu/documents/1001/,Digital Edition,S. D. Goitein,goitein,,,"
    + EDITION_TEXT + ",,\n"
    "1001,https://geniza.princeton.edu/documents/1001/,Digital Translation,A Translator,translator,,,"
    + TRANSLATION_TEXT + ",,\n"
    "1002,https://geniza.princeton.edu/documents/1002/,Discussion,Someone,someone,p. 1,,,,\n"
)

SUPPLEMENT_CSV = "shelfmark,alma_id\nMoss. IX 1.1,9900000002\n"

LINKED_FIELDS = ["sys_id", "pgpid", "shelfmark", "matched_part", "doc_type", "languages",
                 "source_scholar", "doc_relation", "content_length", "pgp_url", "content"]

# What scripts/pgp_transcriptions_export.py writes for the tree above: both footnote rows
# of 1001 link to sys_id 9900000001; the 1002 row is a Discussion and is dropped.
LINKED_ROWS = [
    {"sys_id": "9900000001", "pgpid": "1001", "shelfmark": "T-S 12.123",
     "matched_part": "T-S 12.123", "doc_type": "Letter", "languages": "Judaeo-Arabic",
     "source_scholar": "S. D. Goitein", "doc_relation": "Digital Edition",
     "content_length": str(len(EDITION_TEXT)),
     "pgp_url": "https://geniza.princeton.edu/documents/1001/", "content": EDITION_TEXT},
    {"sys_id": "9900000001", "pgpid": "1001", "shelfmark": "T-S 12.123",
     "matched_part": "T-S 12.123", "doc_type": "Letter", "languages": "Judaeo-Arabic",
     "source_scholar": "A Translator", "doc_relation": "Digital Translation",
     "content_length": str(len(TRANSLATION_TEXT)),
     "pgp_url": "https://geniza.princeton.edu/documents/1001/", "content": TRANSLATION_TEXT},
]


def write_linked(path: pathlib.Path, rows=None) -> None:
    """transcriptions_linked.csv exactly as the exporter writes it (utf-8-sig, csv-quoted)."""
    import csv

    with open(path, "w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=LINKED_FIELDS)
        writer.writeheader()
        writer.writerows(LINKED_ROWS if rows is None else rows)


def _fingerprint(path: pathlib.Path):
    if not path.exists():
        return "absent"
    raw = path.read_bytes()
    return {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def write_upstream_manifest(pgp_data: pathlib.Path, commit: str = COMMIT) -> None:
    """upstream_provenance.json as scripts/fetch_pgp_metadata.py writes it."""
    files = {}
    for name in ("documents.csv", "fragments.csv", "footnotes.csv"):
        raw = (pgp_data / name).read_bytes()
        files[name] = {
            "rows": raw.count(b"\n") - 1,
            "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
    (pgp_data / "upstream_provenance.json").write_text(
        json.dumps({
            "upstream_repo": "princetongenizalab/pgp-metadata",
            "upstream_commit": commit,
            "upstream_committed": "2026-09-21T19:31:40Z",
            "files": files,
        }, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def write_derived_stamp(root: pathlib.Path, commit: str = COMMIT) -> None:
    """derived_provenance.json as scripts/pgp_transcriptions_export.py writes it."""
    pgp_data = root / "pgp_data"
    (pgp_data / "derived_provenance.json").write_text(
        json.dumps({
            "derived_from_commit": commit,
            "derived_by": "scripts/pgp_transcriptions_export.py",
            "files": {
                "transcriptions_linked.csv": _fingerprint(pgp_data / "transcriptions_linked.csv"),
            },
            "inputs": {
                "libraries.csv": _fingerprint(root / "libraries.csv"),
                "fist_shelfmarks_supplement.csv": _fingerprint(
                    pgp_data / "fist_shelfmarks_supplement.csv"
                ),
            },
        }, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def build_tree(root: pathlib.Path, commit: str = COMMIT, derived: bool = True,
               supplement: bool = True) -> pathlib.Path:
    """Lay the whole tree down under `root`. Returns root/pgp_data."""
    root = pathlib.Path(root)
    pgp_data = root / "pgp_data"
    pgp_data.mkdir(parents=True, exist_ok=True)
    (root / "scripts").mkdir(exist_ok=True)  # so Path(__file__).parent.parent resolves here
    (root / "libraries.csv").write_text(LIBRARIES_CSV, encoding="utf-8")
    (pgp_data / "documents.csv").write_text(DOCUMENTS_CSV, encoding="utf-8")
    (pgp_data / "fragments.csv").write_text(FRAGMENTS_CSV, encoding="utf-8")
    (pgp_data / "footnotes.csv").write_text(FOOTNOTES_CSV, encoding="utf-8")
    if supplement:
        (pgp_data / "fist_shelfmarks_supplement.csv").write_text(SUPPLEMENT_CSV, encoding="utf-8")
    write_upstream_manifest(pgp_data, commit)
    if derived:
        write_linked(pgp_data / "transcriptions_linked.csv")
        write_derived_stamp(root, commit)
    return pgp_data


def tamper(path: pathlib.Path) -> None:
    """Change a file's content without changing its length or its structure.

    Swaps the case of the first ASCII letter after the header line, so only the hash
    notices -- the CSV still parses, which is what the override paths need.
    """
    raw = bytearray(path.read_bytes())
    i = raw.index(b"\n") + 1
    while raw[i] > 127 or not chr(raw[i]).isalpha():
        i += 1
    raw[i] = ord(chr(raw[i]).swapcase())
    path.write_bytes(bytes(raw))
