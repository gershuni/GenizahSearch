#!/usr/bin/env python3
"""
PGP Transcriptions Export Script

Exports transcriptions from Princeton Geniza Project (PGP) metadata
and links them to GenizahSearch system_numbers.

Input:
  - pgp_data/documents.csv (PGP document metadata with shelfmarks)
  - pgp_data/footnotes.csv (PGP footnotes containing transcriptions)
  - libraries.csv (GenizahSearch shelfmark → sys_id mapping)

Output:
  - pgp_data/transcriptions_linked.csv (transcriptions with sys_id linkage)
  - pgp_data/transcriptions_unmatched.csv (transcriptions without linkage)
  - pgp_data/export_report.txt (statistics and diagnostics)

Usage:
  python scripts/pgp_transcriptions_export.py
"""

import csv
import re
import sys
import hashlib
import io
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path


def normalize_shelfmark(shelf: str) -> str:
    """
    Normalize a shelfmark for comparison.

    Handles variations like:
    - "Cambridge University Library Ms. T-S 13J35.3" → "t-s 13j35.3"
    - "T-S 13 J 15.3" → "t-s 13j15.3"
    - "Ms. Or. 1080 J 35" → "or.1080 j35"
    - "CUL Or.1080 J70" → "or.1080 j70"
    - "Bodl. MS heb. a 2/22" → "ms heb. a 2.22"
    """
    if not shelf:
        return ""

    # Remove common prefixes
    shelf = re.sub(r'^Cambridge University Library\s*', '', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^The University of Manchester Library\s*', '', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^Freer Gallery of Art,?\s*Smithsonian Institution\s*', '', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^The Jewish Theological Seminary of America\s*', '', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^CUL\s*', '', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^Bodl\.?\s*', '', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^AIU\s*', '', shelf, flags=re.IGNORECASE)  # AIU VII.D.69 → VII.D.69
    # Normalize em-dash to regular dash in AIU shelfmarks
    shelf = shelf.replace('–', '-').replace('—', '-')
    shelf = re.sub(r'^RNL\s*', '', shelf, flags=re.IGNORECASE)  # RNL Yevr → Yevr
    shelf = re.sub(r'^NLI\s*', '', shelf, flags=re.IGNORECASE)  # NLI 577.3/3 → 577.3/3
    shelf = re.sub(r'^HUC\s*', '', shelf, flags=re.IGNORECASE)  # HUC 1037 → 1037
    shelf = re.sub(r'^BL\s+', '', shelf, flags=re.IGNORECASE)  # BL OR 10126 → OR 10126
    # BL Or. 2570 → OR 2570 (remove period, uppercase) - only if followed by space+number (not Or.1080)
    shelf = re.sub(r'^Or\.\s+(\d+)$', r'OR \1', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^Or\.\s+(\d+)\s', r'OR \1 ', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^PER\s+', '', shelf, flags=re.IGNORECASE)  # PER H 130 → H 130 (Vienna)
    shelf = re.sub(r'^UPenn\s+', '', shelf, flags=re.IGNORECASE)  # UPenn E 16510 → E 16510

    # IOM (Institute of Oriental Manuscripts, St Petersburg) normalization
    # PGP: "IOM D 55.13" → FIST: "D 55/13"
    shelf = re.sub(r'^IOM\s+D\s*(\d+)\.(\d+)', r'D \1/\2', shelf, flags=re.IGNORECASE)

    # JRL (John Rylands Library) → Ms. A/B/C/L format
    # "JRL A 316" → "Ms. A 316", "JRL L 128" → "Ms. L 128"
    shelf = re.sub(r'^JRL\s+([ABCL])\s+(\d+)', r'Ms. \1 \2', shelf, flags=re.IGNORECASE)
    # JRL Gaster → Gaster (remove JRL prefix)
    # "JRL Gaster heb. ms 1760/18" → "Gaster heb. ms 1760/18"
    shelf = re.sub(r'^JRL\s+Gaster\s+', r'Gaster ', shelf, flags=re.IGNORECASE)
    # JRL P 213 → P 213 (remove JRL prefix)
    shelf = re.sub(r'^JRL\s+P\s+(\d+)', r'P \1', shelf, flags=re.IGNORECASE)
    # JRL AF 255 → AF  255 (FIST uses double space!)
    shelf = re.sub(r'^JRL\s+AF\s+(\d+)', r'AF  \1', shelf, flags=re.IGNORECASE)

    # JTS Schechter/Krengel → Ms. Schechter./Krengel. format
    # "JTS Schechter 4" → "Ms. Schechter.4", "JTS: Schechter 1" → "Ms. Schechter.1"
    shelf = re.sub(r'^JTS:?\s*Schechter\s+(\d+)', r'Schechter.\1', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^JTS:?\s*Krengel\s+(\d+)', r'Krengel.\1', shelf, flags=re.IGNORECASE)
    # JTSA MS 4391 → MS 4391 (remove JTSA prefix)
    shelf = re.sub(r'^JTSA\s+', r'', shelf, flags=re.IGNORECASE)

    shelf = re.sub(r'^Ms\.?\s*', '', shelf, flags=re.IGNORECASE)

    # Normalize whitespace
    shelf = shelf.strip()
    shelf = re.sub(r'\s+', ' ', shelf)

    # Normalize T-S series formatting
    # "T-S 13 J 35" → "T-S 13J35"
    shelf = re.sub(r'T-S\s+(\d+)\s*J\s*(\d+)', r'T-S \1J\2', shelf, flags=re.IGNORECASE)

    # "T-S K 7" → "T-S K7"
    shelf = re.sub(r'T-S\s+([A-Z]+)\s+(\d+)', r'T-S \1\2', shelf, flags=re.IGNORECASE)

    # Normalize Or.1080 and Or.1081 variants
    # "Or. 1080 J 70" → "Or.1080 J70"
    shelf = re.sub(r'Or\.?\s*1080\s*J\s*(\d+)', r'Or.1080 J\1', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'Or\.?\s*1080\s+(\d+)', r'Or.1080 \1', shelf, flags=re.IGNORECASE)
    # "Or. 1081 2.25" → "Or.1081 2.25"
    shelf = re.sub(r'Or\.?\s*1081\s+', r'Or.1081 ', shelf, flags=re.IGNORECASE)

    # Normalize Bodleian: "heb. a 2/22" → "heb. a.2.22"
    # PGP: "heb. a 2/22" → libraries: "heb. a.2.22"
    shelf = re.sub(r'heb\.\s*([a-z])\s+(\d+)', r'heb. \1.\2', shelf, flags=re.IGNORECASE)
    shelf = shelf.replace('/', '.')

    # Normalize L-G (Lewis-Gibson): "L-G Ar. I.105" → "L-G Ar.I.105"
    # Remove space between type and Roman numeral
    # "L-G Ar. I.105" → "L-G Ar.I.105"
    # "L-G Misc. 58" → "L-G Misc .58" (FIST uses space before number!)
    # "L-G Lit.II.118" already has no space
    shelf = re.sub(r'L-G\s+(Ar)\.\s+([IVX]+)', r'L-G \1.\2', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'L-G\s+(Misc|Lit)\.\s*(\d+)', r'L-G \1 .\2', shelf, flags=re.IGNORECASE)

    # Normalize Yevr. (RNL): Various formats to standard "Yevr.-Arab. I 19"
    # Handle "RNL Yevr.-Arab I 19" → "Yevr.-Arab. I 19" (add period after Arab)
    # Note: RNL prefix was already removed above
    # "Yevr.-Arab I 86" → "Yevr.-Arab. I 86" (ensure period after Arab)
    shelf = re.sub(r'Yevr\.-Arab\s+([IVX]+)', r'Yevr.-Arab. \1', shelf, flags=re.IGNORECASE)
    # Also handle "Yevr. Arab." vs "Yevr.-Arab."
    shelf = re.sub(r'Yevr\.\s*Arab\.?\s+', r'Yevr.-Arab. ', shelf, flags=re.IGNORECASE)
    # Handle "Yevr. II C1" → "Yevr. II C 1" (add space before number in C/B series)
    shelf = re.sub(r'Yevr\.\s+([IVX]+)\s+([CB])(\d+)', r'Yevr. \1 \2 \3', shelf, flags=re.IGNORECASE)
    # Handle "Ms Yevr." → "Yevr." (remove "Ms" prefix)
    shelf = re.sub(r'^Ms\s+Yevr\.', r'Yevr.', shelf, flags=re.IGNORECASE)
    # Handle "Yevr. I B 19" → "Yevr. I B 19" (already correct format)

    # Normalize Mosseri: "Moss. V, 39.6" → "Moss. V,39.6" (remove space after comma)
    shelf = re.sub(r'(Moss\.\s*[IVX]+),\s+', r'\1,', shelf, flags=re.IGNORECASE)

    return shelf.lower()



# Set by _require_verified_inputs(), read by _record_derived_provenance(). They run in
# different functions, and the stamp must never certify inputs that failed their manifest.
#
# False until the guard has actually run and passed. It used to start True, which meant
# any path that skipped the guard -- or a single dropped `verified=` kwarg at the call
# site -- produced a fully certified stamp for inputs nobody had checked. The unsafe
# state must be unreachable by omission.
_INPUTS_VERIFIED = False


def _require_verified_inputs(pgp_data_dir, contents=None) -> bool:
    """Refuse to derive from CSVs that do not match what the fetch recorded.

    Otherwise the stamp certifies inputs it never checked: swap footnotes.csv for an
    altered copy, run this, restore the original, and the derived file is wrong while
    verification reports clean. Hashing only the OUTPUT would faithfully hash the wrong
    derivation, so the inputs have to be verified before generation, not after.
    """
    import sys as _sys

    _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    global _INPUTS_VERIFIED
    try:
        from fetch_pgp_metadata import verify_against_provenance
    except ImportError:
        # Cannot verify => must not certify. Returning None here read as "carry on"
        # while being falsy downstream, which is the worst of both.
        print("WARNING: could not import the provenance verifier; the derived file "
              "will carry no upstream commit.")
        _INPUTS_VERIFIED = False
        return False

    problems = verify_against_provenance(str(pgp_data_dir), check_derived=False,
                                         contents=contents)
    if not problems:
        _INPUTS_VERIFIED = True
        return True
    if os.environ.get('PGP_ALLOW_UNVERIFIED_INPUTS') == '1':
        print("WARNING: deriving from unverified CSVs "
              "(PGP_ALLOW_UNVERIFIED_INPUTS=1):")
        for problem in problems:
            print("  %s" % problem)
        print("  No upstream commit will be recorded for the derived file.")
        _INPUTS_VERIFIED = False
        # The override means "derive from these anyway", not "and vouch for them".
        # Without this the stamp copies the rejected manifest's commit, and once the
        # original CSVs are restored everything verifies clean while the derived output
        # came from different inputs.
        return False

    # No override, and the inputs do not match their manifest: this is fatal. A stray
    # `return True` used to sit here and made everything below unreachable, so a
    # mismatch silently reported success and the derived file was stamped with the
    # rejected commit.
    _INPUTS_VERIFIED = False
    print("ERROR: the CSVs in pgp_data/ do not match upstream_provenance.json:",
          file=__import__("sys").stderr)
    for problem in problems:
        print("  %s" % problem, file=__import__("sys").stderr)
    print("", file=__import__("sys").stderr)
    print("Re-run:  python scripts/fetch_pgp_metadata.py", file=__import__("sys").stderr)
    print("Set PGP_ALLOW_UNVERIFIED_INPUTS=1 to derive from them anyway.",
          file=__import__("sys").stderr)
    raise SystemExit(1)


def _invalidate_derived_stamp(pgp_data_dir) -> None:
    """Remove derived_provenance.json. Called BEFORE the derived file is touched.

    The stamp describes a specific transcriptions_linked.csv. From the moment this run
    starts rewriting that file, the old stamp describes nothing that exists -- and if the
    run dies between the write and the re-stamp (an empty footnotes.csv used to do that,
    via ZeroDivisionError in the report), a stale stamp would vouch for a header-only file.
    """
    stale = os.path.join(str(pgp_data_dir), 'derived_provenance.json')
    if os.path.exists(stale):
        os.remove(stale)


def _read_bytes(path):
    """The file's bytes, or None when there is no such file (or no path was given)."""
    if not path or not os.path.exists(path):
        return None
    with open(path, 'rb') as fh:
        return fh.read()


def _fingerprint_bytes(raw):
    """{bytes, sha256} of bytes already in hand, or 'absent' for None.

    The derivation fingerprints the very bytes it parses. Fingerprinting the FILE -- even
    before reading it -- left a window: fingerprint A, swap in B, let the loader read B,
    restore A, and the output came from B while the stamp described A and verified clean.
    With one read there is no second read for a swap to land between.
    """
    if raw is None:
        return 'absent'
    return {'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}


def _fingerprint(path):
    """{bytes, sha256} of a file, or the string 'absent' when there is no such file."""
    return _fingerprint_bytes(_read_bytes(path))


def _record_derived_provenance(pgp_data_dir, verified: bool = False, inputs=()) -> None:
    """Stamp transcriptions_linked.csv with its upstream commit AND its own checksum.

    The commit alone was not enough: replacing the file with two lines of CSV while
    leaving the stamp alone passed verification, because nothing read the content.

    `inputs` are the NON-upstream files the derivation read -- libraries.csv and the FIST
    supplement -- as (label, fingerprint) pairs, where the fingerprint was taken BEFORE the
    file was read (see export_transcriptions). They decide which manuscript a transcription
    is attributed to, and they were covered by no checksum: editing one line of
    libraries.csv re-attributed a transcription to the wrong manuscript while every check
    stayed clean. Fingerprinting them here, after the derivation, was not enough either: a
    change between the read and the stamp produced output from the old file stamped with
    the new file's hash, which then verified clean. Taken before the read, any later change
    leaves the file on disk disagreeing with the stamp, and the import refuses.

    `verified` defaults to False on purpose: forgetting to pass it must produce no stamp,
    never a certified one.
    """
    import hashlib
    import json

    # Invalidate FIRST, before any early return. A stale derived_provenance.json left
    # behind by a previous run would otherwise vouch for output this run produced from
    # unverified -- or unknown -- inputs.
    _invalidate_derived_stamp(pgp_data_dir)

    upstream_path = os.path.join(str(pgp_data_dir), 'upstream_provenance.json')
    if not os.path.exists(upstream_path):
        return
    try:
        with open(upstream_path, 'r', encoding='utf-8') as fh:
            upstream = json.load(fh) or {}
    except (OSError, ValueError):
        return

    commit = upstream.get('upstream_commit')
    if not commit:
        return
    if not verified:
        # Derived from CSVs that failed their manifest; the stamp was already removed
        # above, so there is simply nothing for the import to trust.
        print("  No derived provenance recorded (inputs were not verified).")
        return

    linked = os.path.join(str(pgp_data_dir), 'transcriptions_linked.csv')
    if not os.path.exists(linked):
        return
    with open(linked, 'rb') as fh:
        raw = fh.read()

    out = os.path.join(str(pgp_data_dir), 'derived_provenance.json')
    # Invalidate the old stamp before writing the new one, so an interrupted write
    # cannot leave a stamp vouching for a file it no longer describes.
    if os.path.exists(out):
        os.remove(out)
    with open(out, 'w', encoding='utf-8') as fh:
        json.dump({
            'derived_from_commit': commit,
            'derived_by': 'scripts/pgp_transcriptions_export.py',
            'files': {
                'transcriptions_linked.csv': {
                    'bytes': len(raw),
                    'sha256': hashlib.sha256(raw).hexdigest(),
                },
            },
            'inputs': dict(inputs),
        }, fh, indent=2, sort_keys=True)
        fh.write('\n')
    print("  Recorded derived provenance (upstream %s, sha %s)"
          % (commit[:12], hashlib.sha256(raw).hexdigest()[:12]))


def require_fist_supplement(path, allow_missing: bool) -> None:
    """Refuse to run without the FIST shelfmark supplement.

    It contributes ~35,600 shelfmarks that libraries.csv does not carry. Without it the
    PGP fragment match rate drops from 94.5% to 87.5% -- roughly 2,900 fragments that
    quietly fail to link, taking their IIIF image URLs with them. The old behaviour was
    to shrug and continue, so the loss showed up only as a slightly worse number in a
    report nobody diffed.
    """
    import os as _os
    if _os.path.exists(path):
        return
    if allow_missing:
        print("WARNING: proceeding WITHOUT the FIST supplement "
              "(PGP_ALLOW_MISSING_FIST_SUPPLEMENT=1).")
        print("         Expect a materially lower fragment match rate.")
        print()
        return
    print("ERROR: FIST shelfmark supplement not found:", file=__import__("sys").stderr)
    print("         %s" % path, file=__import__("sys").stderr)
    print("", file=__import__("sys").stderr)
    print("Regenerate it with:  python scripts/fist_shelfmarks_export.py",
          file=__import__("sys").stderr)
    print("(it needs fist_data/FIST.db). Without it the fragment match rate falls from",
          file=__import__("sys").stderr)
    print("94.5% to 87.5%, so thousands of fragments lose their IIIF image links.",
          file=__import__("sys").stderr)
    print("Set PGP_ALLOW_MISSING_FIST_SUPPLEMENT=1 if you really mean to run without it.",
          file=__import__("sys").stderr)
    raise SystemExit(1)


def load_genizahsearch_shelfmarks_from_bytes(libraries_raw: bytes,
                                             supplement_raw: bytes = None) -> dict:
    """Build the shelfmark -> sys_id mapping from bytes already read.

    The derivation reads libraries.csv and the FIST supplement exactly once, fingerprints
    those bytes for derived_provenance.json, and parses the SAME bytes here -- so the stamp
    can only ever describe what was actually consumed.

    Returns dict: normalized_shelfmark -> system_number
    """
    gs_lookup = {}

    # Main libraries.csv. newline=None: the text-mode open() this replaced translated
    # bare CR and CRLF to LF before the csv module saw them; StringIO does not unless told.
    reader = csv.reader(io.StringIO(libraries_raw.decode('utf-8'), newline=None))
    next(reader, None)  # Skip header

    for row in reader:
        if len(row) < 3:
            continue

        sys_id = row[0]
        call_numbers = row[2]

        # Split pipe-separated variants and index all
        for variant in call_numbers.split('|'):
            normalized = normalize_shelfmark(variant)
            if normalized:
                # Keep first match (most specific)
                if normalized not in gs_lookup:
                    gs_lookup[normalized] = sys_id

    # FIST supplement, if it was there
    if supplement_raw is not None:
        reader = csv.DictReader(io.StringIO(supplement_raw.decode('utf-8-sig'), newline=None))
        fist_count = 0
        for row in reader:
            shelfmark = row.get('shelfmark', '')
            alma_id = row.get('alma_id', '')
            if shelfmark and alma_id:
                normalized = normalize_shelfmark(shelfmark)
                if normalized and normalized not in gs_lookup:
                    gs_lookup[normalized] = alma_id
                    fist_count += 1
        print(f"  Added {fist_count} shelfmarks from FIST supplement")

    return gs_lookup


def load_genizahsearch_shelfmarks(libraries_path: str, fist_supplement_path: str = None) -> dict:
    """
    Load GenizahSearch libraries.csv and create shelfmark → sys_id mapping.
    Optionally supplement with FIST shelfmarks for better coverage.

    Path-based convenience for callers that do not stamp provenance (import_pgp_full.py).
    The derivation itself must NOT use this: see export_transcriptions.

    Returns dict: normalized_shelfmark → system_number
    """
    with open(libraries_path, 'rb') as fh:
        libraries_raw = fh.read()
    supplement_raw = _read_bytes(fist_supplement_path)
    return load_genizahsearch_shelfmarks_from_bytes(libraries_raw, supplement_raw)


def load_pgp_documents(documents_path: str) -> dict:
    """Path form of load_pgp_documents_from_bytes(), for callers that stamp no provenance.

    The pipeline's main() must NOT use this: it verifies a file's bytes and then parses
    the SAME bytes, so nothing can be swapped between the two. Re-opening by path here is
    exactly the window that closes.
    """
    with open(documents_path, 'rb') as fh:
        return load_pgp_documents_from_bytes(fh.read())


def load_pgp_documents_from_bytes(raw: bytes) -> dict:
    """
    Load PGP documents.csv and create pgpid → document info mapping.

    Returns dict: pgpid → {shelfmark, type, tags, ...}
    """
    pgp_docs = {}

    with io.StringIO(raw.decode('utf-8'), newline=None) as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Handle BOM in first column
            pgpid = row.get('\ufeffpgpid') or row.get('pgpid')
            if pgpid:
                pgp_docs[pgpid] = {
                    'shelfmark': row.get('shelfmark', ''),
                    'type': row.get('type', ''),
                    'tags': row.get('tags', ''),
                    'description': row.get('description', ''),
                    'languages_primary': row.get('languages_primary', ''),
                    'url': row.get('url', ''),
                }

    return pgp_docs


def extract_transcriptions(footnotes_path: str) -> list:
    """Path form of extract_transcriptions_from_bytes(), for callers that stamp no provenance.

    The pipeline's main() must NOT use this: it verifies a file's bytes and then parses
    the SAME bytes, so nothing can be swapped between the two. Re-opening by path here is
    exactly the window that closes.
    """
    with open(footnotes_path, 'rb') as fh:
        return extract_transcriptions_from_bytes(fh.read())


def extract_transcriptions_from_bytes(raw: bytes) -> list:
    """
    Extract transcriptions from PGP footnotes.csv.

    Filters for:
    - doc_relation containing 'Edition' or 'Digital'
    - content length > 50 characters

    Returns list of dicts with transcription data
    """
    transcriptions = []

    with io.StringIO(raw.decode('utf-8'), newline=None) as f:
        reader = csv.DictReader(f)

        for row in reader:
            # The document_id column contains the PGPID
            doc_id = row.get('document_id', '')
            doc_relation = row.get('doc_relation', '')
            content = row.get('content', '')

            # Filter for edition/transcription content
            if not ('Edition' in doc_relation or 'Digital' in doc_relation):
                continue

            # Filter for substantial content
            if not content or len(content) < 50:
                continue

            transcriptions.append({
                'pgpid': doc_id,
                'source': row.get('source', ''),
                'source_slug': row.get('source_slug', ''),
                'doc_relation': doc_relation,
                'location': row.get('location', ''),
                'url': row.get('url', ''),
                'content': content,
                'emendations': row.get('emendations', ''),
                'notes': row.get('notes', ''),
            })

    return transcriptions


def match_to_genizahsearch(shelfmark: str, gs_lookup: dict) -> tuple:
    """
    Try to match a PGP shelfmark to GenizahSearch sys_id.

    Handles multi-fragment shelfmarks like "T-S 13J35.3 + AIU VII.A.23"

    Returns: (sys_id or None, matched_part or None)
    """
    # Handle multi-fragment shelfmarks
    parts = [p.strip() for p in shelfmark.split('+')]

    for part in parts:
        normalized = normalize_shelfmark(part)
        if normalized in gs_lookup:
            return gs_lookup[normalized], part

    # Try without sub-part (e.g., "T-S 13J35" instead of "T-S 13J35.3")
    for part in parts:
        normalized = normalize_shelfmark(part)
        # Remove trailing .number
        base = re.sub(r'\.\d+$', '', normalized)
        if base in gs_lookup:
            return gs_lookup[base], part
        # Also try removing just trailing letter (e.g., .10a -> .10)
        base_no_letter = re.sub(r'([a-z])$', '', normalized)
        if base_no_letter in gs_lookup:
            return gs_lookup[base_no_letter], part

    return None, None


def export_transcriptions(
    libraries_path: str,
    documents_path: str,
    footnotes_path: str,
    output_dir: str,
    fist_supplement_path: str = None
):
    """
    Main export function.
    """
    print("=" * 60)
    print("PGP Transcriptions Export")
    print("=" * 60)
    print()

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Read the mapping inputs ONCE. The stamp fingerprints these bytes and the loader
    # parses these bytes, so the stamp can only describe what was consumed. Fingerprinting
    # the FILE was not enough in either order: after the read, an edit during the run was
    # stamped as consumed; before the read, a swap-and-restore between the fingerprint and
    # the loader's own open() produced output from one file stamped as the other.
    libraries_raw = _read_bytes(libraries_path)
    if libraries_raw is None:
        raise FileNotFoundError(libraries_path)
    supplement_raw = _read_bytes(fist_supplement_path)
    documents_raw = _read_bytes(documents_path)
    footnotes_raw = _read_bytes(footnotes_path)
    for path, raw in ((documents_path, documents_raw), (footnotes_path, footnotes_raw)):
        if raw is None:
            raise FileNotFoundError(path)

    # Verify the upstream CSVs -- THESE bytes, not the files on disk. Verifying by path and
    # then letting the loaders re-open the files left the same swap-and-restore window
    # the mapping inputs had: swapped content in the output, the original commit in the
    # stamp, and a clean verification afterwards.
    _require_verified_inputs(output_dir, contents={
        'documents.csv': documents_raw,
        'footnotes.csv': footnotes_raw,
    })

    input_fingerprints = (
        ('libraries.csv', _fingerprint_bytes(libraries_raw)),
        ('fist_shelfmarks_supplement.csv', _fingerprint_bytes(supplement_raw)),
    )

    # Load data -- every loader parses the bytes read above.
    print("Loading GenizahSearch shelfmarks...")
    gs_lookup = load_genizahsearch_shelfmarks_from_bytes(libraries_raw, supplement_raw)
    print(f"  Loaded {len(gs_lookup):,} normalized shelfmarks")

    print("Loading PGP documents...")
    pgp_docs = load_pgp_documents_from_bytes(documents_raw)
    print(f"  Loaded {len(pgp_docs):,} documents")

    print("Extracting transcriptions from footnotes...")
    transcriptions = extract_transcriptions_from_bytes(footnotes_raw)
    print(f"  Found {len(transcriptions):,} transcription records")
    print()

    # Match and export
    linked = []
    unmatched = []
    stats = defaultdict(int)

    for trans in transcriptions:
        pgpid = trans['pgpid']
        doc_info = pgp_docs.get(pgpid, {})
        shelfmark = doc_info.get('shelfmark', '')

        sys_id, matched_part = match_to_genizahsearch(shelfmark, gs_lookup)

        record = {
            'sys_id': sys_id or '',
            'pgpid': pgpid,
            'shelfmark': shelfmark,
            'matched_part': matched_part or '',
            'doc_type': doc_info.get('type', ''),
            'languages': doc_info.get('languages_primary', ''),
            'source_scholar': trans['source'],
            'doc_relation': trans['doc_relation'],
            'content': trans['content'],
            'content_length': len(trans['content']),
            'pgp_url': doc_info.get('url', ''),
        }

        if sys_id:
            linked.append(record)
            stats['linked'] += 1
        else:
            unmatched.append(record)
            stats['unmatched'] += 1

        # Track by doc_relation
        stats[f"relation:{trans['doc_relation']}"] += 1

    # Count unique documents
    linked_pgpids = set(r['pgpid'] for r in linked)
    unmatched_pgpids = set(r['pgpid'] for r in unmatched) - linked_pgpids

    print("Matching results:")
    print(f"  Linked records: {stats['linked']:,}")
    print(f"  Unmatched records: {stats['unmatched']:,}")
    print(f"  Unique linked documents: {len(linked_pgpids):,}")
    print(f"  Unique unmatched documents: {len(unmatched_pgpids):,}")
    print()

    # A derivation that links nothing is not a result, it is a broken input: an upstream
    # column rename or a truncated footnotes.csv used to replace a 10,000-row file with a
    # header and then crash in the report (ZeroDivisionError), leaving the old stamp
    # behind. Refuse before touching the output.
    if not transcriptions or not linked:
        print("ERROR: the derivation produced %d transcription records and %d linked rows; "
              "refusing to overwrite transcriptions_linked.csv with an empty result."
              % (len(transcriptions), len(linked)), file=sys.stderr)
        print("  footnotes.csv: %s" % footnotes_path, file=sys.stderr)
        print("  libraries.csv: %s" % libraries_path, file=sys.stderr)
        raise SystemExit(1)

    # From here on the previous transcriptions_linked.csv is being replaced, so the stamp
    # that described it must go NOW -- before the write, not after it, where a crash in
    # between (the report's ZeroDivisionError on an empty footnotes.csv) left a stale
    # stamp vouching for a header-only file. Not earlier either: a derivation refused
    # above leaves the old file intact, and its stamp still describes it.
    _invalidate_derived_stamp(output_dir)

    # Write linked transcriptions
    linked_path = os.path.join(output_dir, 'transcriptions_linked.csv')
    print(f"Writing {linked_path}...")

    fieldnames = ['sys_id', 'pgpid', 'shelfmark', 'matched_part', 'doc_type',
                  'languages', 'source_scholar', 'doc_relation', 'content_length',
                  'pgp_url', 'content']

    with open(linked_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(linked)

    print(f"  Wrote {len(linked):,} records")

    # Write unmatched transcriptions
    unmatched_path = os.path.join(output_dir, 'transcriptions_unmatched.csv')
    print(f"Writing {unmatched_path}...")

    with open(unmatched_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unmatched)

    print(f"  Wrote {len(unmatched):,} records")

    # Write report
    report_path = os.path.join(output_dir, 'export_report.txt')
    print(f"Writing {report_path}...")

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("PGP Transcriptions Export Report\n")
        f.write("=" * 60 + "\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n")

        f.write("Input Files:\n")
        f.write(f"  libraries.csv: {len(gs_lookup):,} shelfmarks\n")
        f.write(f"  documents.csv: {len(pgp_docs):,} documents\n")
        f.write(f"  footnotes.csv: {len(transcriptions):,} transcription records\n\n")

        f.write("Matching Results:\n")
        f.write(f"  Linked records: {stats['linked']:,}\n")
        f.write(f"  Unmatched records: {stats['unmatched']:,}\n")
        f.write(f"  Match rate: {stats['linked']/max(len(transcriptions), 1)*100:.1f}%\n\n")

        f.write("Unique Documents:\n")
        f.write(f"  Linked: {len(linked_pgpids):,}\n")
        f.write(f"  Unmatched: {len(unmatched_pgpids):,}\n\n")

        f.write("By doc_relation:\n")
        for key, count in sorted(stats.items()):
            if key.startswith('relation:'):
                f.write(f"  {key[9:]}: {count:,}\n")

        # Sample unmatched shelfmarks for debugging
        f.write("\n\nSample Unmatched Shelfmarks (first 50):\n")
        f.write("-" * 60 + "\n")
        seen_shelfs = set()
        for record in unmatched[:200]:
            shelf = record['shelfmark']
            if shelf and shelf not in seen_shelfs:
                seen_shelfs.add(shelf)
                f.write(f"  {shelf}\n")
                if len(seen_shelfs) >= 50:
                    break

    print()
    # Record which upstream commit this DERIVED file was built from. The fetch step
    # checksums the three downloaded CSVs, but transcriptions_linked.csv is generated
    # from them and is what actually carries transcription content -- so fetching a new
    # commit and keeping an old derived file passed verification while the importer
    # consumed the old text.
    _record_derived_provenance(
        output_dir,
        verified=_INPUTS_VERIFIED,
        inputs=input_fingerprints,
    )

    print("Export complete!")
    print(f"  Linked: {linked_path}")
    print(f"  Unmatched: {unmatched_path}")
    print(f"  Report: {report_path}")

    return {
        'linked_count': len(linked),
        'unmatched_count': len(unmatched),
        'linked_docs': len(linked_pgpids),
        'unmatched_docs': len(unmatched_pgpids),
    }


def main():
    # Determine paths relative to script location
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent

    libraries_path = project_dir / 'libraries.csv'
    fist_supplement_path = project_dir / 'pgp_data' / 'fist_shelfmarks_supplement.csv'
    require_fist_supplement(
        str(fist_supplement_path),
        os.environ.get('PGP_ALLOW_MISSING_FIST_SUPPLEMENT') == '1',
    )
    # ONE directory for the verification, the reads and the writes. They already
    # agreed, but deriving them separately is how a check ends up validating a different
    # directory than the one actually consumed. The verification itself happens inside
    # export_transcriptions(), against the bytes it parses.
    pgp_data_dir = project_dir / 'pgp_data'
    documents_path = pgp_data_dir / 'documents.csv'
    footnotes_path = project_dir / 'pgp_data' / 'footnotes.csv'
    output_dir = pgp_data_dir

    # Verify input files exist
    for path in [libraries_path, documents_path, footnotes_path]:
        if not path.exists():
            print(f"ERROR: Input file not found: {path}")
            return 1

    export_transcriptions(
        str(libraries_path),
        str(documents_path),
        str(footnotes_path),
        str(output_dir),
        str(fist_supplement_path) if fist_supplement_path.exists() else None
    )

    return 0


if __name__ == '__main__':
    exit(main())
