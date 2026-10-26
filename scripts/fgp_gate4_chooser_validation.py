# -*- coding: utf-8 -*-
"""Gate 4 of the FGP source-folders plan: the version chooser validated against the larger sidecar.

Why
---
Gates 1-3 (``scripts/fgp_association_inventory.py``, ``scripts/fgp_gate2_rules.py``,
``scripts/fgp_gate3_text_pipeline.py``) decided, row by row, which FGP texts may be published,
under which credit and with which text. Nothing yet proves that the two apps' version chooser
(``shared/fgp_service.py``: ``FgpService.get_fgp_sources_for_fragment`` -> ``dedupe_fgp_sources``,
``filter_sources_for_page`` / ``_select_fgp_sources_for_page``, ``choose_default_source``; rendered
by ``web/components/version_selector.py``, ``web/pages/browse.py`` and ``genizah_app.py``) behaves
with the sidecar those gates produce. This gate builds that sidecar as a CANDIDATE beside today's
(never over it) and drives the chooser's shared functions over both, with both apps' argument
shapes, on every manuscript and page by page on a declared reference set.

Rules (declared before the run; ``docs/plans/FGP_CHOOSER_VALIDATION_RULES.md``)
----------------------------------------------------------------------------------
Candidate sidecar ``fgp_transcriptions.candidate.db`` (today's schema + ``origin``,
``text_provenance``, ``fist_unit_table``, ``fist_unit_id``, ``durable_key``):
  R1 every today row keeps its ``id``; new rows take ids above today's maximum, in row-key order.
  R2 a today row is carried iff gate 2 did not retire it and it is gate-2 publishable, or its only
     gate-2 hold is ``xml_unverified`` and gate 3 verified the XML text by the row's own record; and
     it has a text that is not a test or dummy file (``test_or_dummy``: at most 40 characters that
     are a transcription without a Hebrew or Arabic letter, test words and digits only, or carry
     "טסט" / "נא להתעלם") and, when gate 3 replaced it, not a legacy-font DOCX (``legacy_font``:
     at least 10 combining marks that follow no letter, and at least a tenth as many as letters);
     and it is not the stub of a same-image pair (``stub_retired``: another unit's row on the same
     image, relation, language and manuscript has more than three times its letters and CONTAINS
     it -- at least 80% of its letters in order, in runs of six or more letters of the longer text;
     a short row that is another text stays). R2d: a row listed by durable key in
     ``scripts/fgp_gate4_exclusions.json`` is excluded under its listed reason (``non_manuscript``,
     ``keyboard_dummy``). R2e: a text with three or more telephone numbers or an e-mail address
     is excluded as ``personal_data``. R2f: a unit whose FIST signature has ``RecordStatus`` 5 is an
     older version FGP superseded (``superseded_version``). R2g: a new translation whose signature has
     ``RecordStatus`` 1 is held (``status1_translation``); status-1 transcriptions enter.
  R3 its text is gate 3's when gate 3 replaced it (status accepted / approved), else today's;
     ``sections`` (the PDF page split) is cleared on a replaced text.
  R4 its credit is gate 2's (``credit_he`` / ``credit_en``; the generic Friedberg attribution where
     gate 2 ruled generic).
  R5 a new (unit, AlmaId) row read by gate 3 enters under ``--new-rows-policy plain`` iff its text
     is non-blank, not a superseded version (R2f), not a status-1 translation (R2g), not listed (R2d), without personal data (R2e), not a test or dummy file, not a
     legacy-font DOCX, its FIST unit comment does not say
     it must not be published until read against the source (``fist_comment_no_publish``, gate 2's rule), and it
     comes from folder B, or
     from a DOCX whose rendering gate 3 checked clean against FGP's PDF of the file (or that has no
     PDF text) and whose tables are one-column line blocks -- the owner's rulings of 2026-10-26
     (FGP_DOCX_STRUCTURE_RULES.md): hidden runs, numbering, footnotes and tracked changes are
     rendered, a real table holds the row (``docx_multicolumn_table``) and so does a difference
     gate 3 could not classify against the PDF (``pdf_held``); a folder-B row carries the image number of its folder as
     ``c_number`` only when the NLI gallery lists that image under the row's manuscript (else held
     ``image_alma_mismatch`` / ``image_unknown``); a row today's chooser could never show is held:
     a whole-document row on a manuscript whose candidate already has per-image rows of the same
     relation and language (``whole_doc_shadowed``), a per-image row on an image another unit
     already serves (``image_already_served``), or a per-image row whose manuscript has a
     whole-document row of the same relation and language (``shadows_whole_doc_row``). Under
     ``hold`` no new row enters. New rows are credited by gate 2's resolver on their FIST unit.
  R6 ``fgp_meta.version`` is set (``2.0.0``) and the build is recorded there.

Checks (a failing check exits 1; a missed expectation too)
  C1 two runs identical: the digest over candidate rows and every disposition equals the previous
     run's (``--previous-summary``), or ``--allow-single-run`` was passed.
  C2 selectability, every manuscript, through the real ``FgpService`` on both DBs: every source
     the chooser lists today is listed from the candidate, or carries one of the dispositions
     ``retired_alias`` (a same-meaning successor is listed), ``gate2_hold``, ``xml_unverified``,
     an R2 / R2c / R2d / R2e exclusion, ``same_text_swap`` (its image group is now won by a row of
     the same text, ratio >= 0.95 over the whole text); every new row is listed; a same-meaning
     successor is listed.
  C3 identity: every carried row keeps id, uid, doc_relation and language -- the only keys a saved
     choice can rest on (the reading desk keys choices by list position; see the rules document).
  C4 page assignment: every carried row keeps sys_id, c_number, image_side, page_info and
     folio_num, unless an approval in ``scripts/fgp_gate4_approvals.json`` names the change; every
     new per-image row's image is confirmed for its manuscript by ``nli_crossref.db``.
  C5 default selection on the reference set, page by page: the web shape
     (``filter_sources_for_page``) and the desktop shape (``_select_fgp_sources_for_page`` with
     ``displayed_*`` resolvers) agree on both DBs, and every change of ``choose_default_source``'s
     decision between the DBs carries a disposition: ``new_edition``, ``row_excluded``,
     ``text_replaced``, ``same_text_swap``; else ``unexplained`` fails.
  C6 latency within the declared budgets (``BUDGETS``) on the reference set, and the candidate's
     p95 within 1.5x today's (+20 ms).
  C7 memory within the declared budgets: the build's peak working set, the app-side cost of opening
     the candidate and reading the full sys_id sets, the heaviest manuscript's load; the file size.
  C8 the service contract: the source table is discovered, ``get_version()`` answers, every row has
     a sys_id and text, counts reconcile.
  C9 every durable key listed in the exclusions file names a row of today's sidecar or of gate 3's
     new rows.

Outputs (``--out-dir``, default ``fgp_data/inventory``): ``fgp_transcriptions.candidate.db``,
``gate4.db`` (``candidate_rows``, ``row_dispositions``, ``selectability``, ``page_assignment``,
``default_decisions``, ``reference_set``, ``latency``, ``memory``, ``checks``, ``counts``,
``gate4_meta``), ``gate4_summary.json``, ``GATE4_REPORT.md``.

Usage
  python scripts/fgp_gate4_chooser_validation.py --allow-single-run
  python scripts/fgp_gate4_chooser_validation.py --previous-summary fgp_data/inventory/gate4_run1/gate4_summary.json \
      --expect-file scripts/fgp_gate4_chooser_validation.expect.json
  --htr-json PATH replaces the search engine as the HTR source (tests); --new-rows-policy hold|plain.
"""
from __future__ import annotations

import argparse
import collections
import csv
import difflib
import hashlib
import json
import os
import random
import re
import sqlite3
import sys
import time
import unicodedata

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from scripts.fgp_association_inventory import DEFAULT_FIST, DEFAULT_OUT, parse_expectations, ro  # noqa: E402
from scripts.fgp_fill_credits_bilingual import build_book_map, build_catalog_map, build_credit_parts  # noqa: E402
from scripts.fgp_gate2_rules import (  # noqa: E402
    BIBREF_SOURCE, GENERIC_CREDIT, NO_PUBLISH_COMMENT_RE, build_bibref_map, credit_for_unit, load_unit_comments,
)
from scripts.fgp_rebuild_text_and_credit import heb_ratio  # noqa: E402

os.environ.setdefault("FGP_TRANSCRIPTIONS_ENABLED", "1")
from shared import fgp_service as fs  # noqa: E402
import shared.nli_crossref_service as nli_mod  # noqa: E402

DEFAULT_FGP_DB = os.path.join(ROOT, "fgp_data", "fgp_transcriptions.db")
DEFAULT_GATE2 = os.path.join(DEFAULT_OUT, "gate2.db")
DEFAULT_GATE3 = os.path.join(DEFAULT_OUT, "gate3.db")
DEFAULT_INVENTORY = os.path.join(DEFAULT_OUT, "inventory.db")
DEFAULT_NLI = os.path.join(ROOT, "nli_data", "nli_crossref.db")
DEFAULT_LIBRARIES = os.path.join(ROOT, "libraries.csv")
DEFAULT_APPROVALS = os.path.join(ROOT, "scripts", "fgp_gate4_approvals.json")
DEFAULT_EXCLUSIONS = os.path.join(ROOT, "scripts", "fgp_gate4_exclusions.json")
EXCLUSION_REASONS = frozenset({"non_manuscript", "keyboard_dummy"})
CANDIDATE_NAME = "fgp_transcriptions.candidate.db"
CANDIDATE_VERSION = "2.0.0"
MS_HEB_G2 = "990053489970205171"
REFERENCE_SEED = 20261026

# Declared before the first run (undeclared = fail). Latency is per call on this machine, after one
# warm-up call per DB; memory is the process working set as psutil reports it.
BUDGETS = {
    "latency.load.p95_ms": 500.0,      # FgpService.get_fgp_sources_for_fragment, candidate
    "latency.load.max_ms": 2000.0,
    "latency.page.p95_ms": 250.0,      # filter_sources_for_page + choose_default_source, candidate
    "latency.page.max_ms": 2000.0,
    "latency.ratio_vs_today": 1.5,     # candidate p95 <= today p95 * ratio + floor
    "latency.ratio_floor_ms": 20.0,
    "memory.build_peak_mb": 3072.0,    # peak working set while building the candidate
    "memory.app_open_mb": 128.0,       # open the candidate + the two full sys_id sets both apps read
    "memory.heaviest_manuscript_mb": 64.0,
    "size.candidate_mb": 1024.0,
}
EXTRA_COLUMNS = ("origin", "text_provenance", "fist_unit_table", "fist_unit_id", "durable_key")
PAGE_FIELDS = ("sys_id", "c_number", "image_side", "page_info", "folio_num")
MULTICOLUMN_MIN = 2  # a table whose widest row has this many cells is a real table, not a line block
SAME_TEXT_RATIO = 0.95
# The only reasons a source the chooser lists today may be missing from the candidate's list.
SELECTABILITY_DISPOSITIONS = frozenset({"retired_alias", "gate2_hold", "xml_unverified", "empty_text", "test_or_dummy",
                                        "legacy_font", "stub_retired", "same_text_swap", "personal_data", "superseded_version"}
                                       | EXCLUSION_REASONS)
# Legacy-font DOCX (owner's ruling of 2026-10-26): a file typed in a pre-Unicode Hebrew font reads as vowel points
# standing where letters should be. In real Unicode text a combining mark always follows a letter; a mark that
# follows a space, a digit, punctuation or the start of the text ("orphan") is the font's letter. A vocalised
# piyyut has none; the garbled files have one per word. Held when there are at least LEGACY_ORPHAN_MIN orphan marks
# and they are at least LEGACY_ORPHAN_SHARE of the letters. Judged on gate 3's texts only (DOCX, folder B): today's
# PDF text detaches marks by itself, which gate 3's classes handle.
LEGACY_ORPHAN_MIN = 10
LEGACY_ORPHAN_SHARE = 0.10
# Stubs (owner's ruling of 2026-10-26, corrected the same evening): when two FIST units serve the same image, relation
# and language of one manuscript, today's chooser lists only the longer text; a shorter one with under STUB_SHARE of
# the longer's letters is a stub of a transcription filed twice, and is retired, only when it is a PART of the longer
# text: at least STUB_CONTAINED of its letters lie, in order, in runs of STUB_RUN or more letters of the longer one.
# Length alone retired 56 of today's rows that are other texts (team 200's "גנוזות" rows on a different photograph of
# the same C-number; none above 0.33 by this measure, a real excerpt 0.996). Rows that are not contained stay.
STUB_SHARE = 0.30
STUB_RUN = 6
STUB_CONTAINED = 0.80


def legacy_font_stats(text: str) -> tuple:
    """(Hebrew and Arabic letters, orphan combining marks) of ``text``.

    A mark attaches to any letter (a Latin letter with a decomposed diacritic, "Muḥammad", is not an orphan).
    An orphan counts only when it starts a word -- the next character is a letter or another mark -- which is
    how a legacy font's letters arrive; a mark standing alone between spaces is a transcriber's placeholder for
    an illegible letter (T-S AS 141.44d: "] ָ ְ ָ (שם)") and is left alone.
    """
    text = text or ""
    letters = orphan = 0
    after_letter = False
    for i, c in enumerate(text):
        cat = unicodedata.category(c)
        if "א" <= c <= "ת" or "ؠ" <= c <= "ي":
            letters += 1
            after_letter = True
        elif cat.startswith("L"):
            after_letter = True
        elif cat == "Mn":
            if not after_letter:
                nxt = text[i + 1] if i + 1 < len(text) else ""
                if nxt and (unicodedata.category(nxt).startswith("L") or unicodedata.category(nxt) == "Mn"):
                    orphan += 1
        else:
            after_letter = False
    return letters, orphan


def legacy_font(text: str) -> bool:
    letters, orphan = legacy_font_stats(text)
    return orphan >= LEGACY_ORPHAN_MIN and orphan >= LEGACY_ORPHAN_SHARE * letters
# Test and dummy files (owner's ruling of 2026-10-26, after the drafts check): a text of at most 40 characters that
# is a transcription without a Hebrew or Arabic letter ("test", "ggg", "fgfghfg", "241241"), or consists of test
# words and digits only ("בדיקה", "נסיון", "test"), or carries "טסט" or "נא להתעלם" ("please ignore") anywhere.
TEST_TEXT_MAX = 40
TEST_WORDS = frozenset({"test", "טסט", "בדיקה", "נסיון"})
TEST_TOKEN_RE = re.compile(r"[\w']+", re.UNICODE)


def test_or_dummy(text: str, relation: str) -> bool:
    t = (text or "").strip()
    if not t or len(t) > TEST_TEXT_MAX:
        return False
    if "טסט" in t or "נא להתעלם" in t:
        return True
    if relation == "Digital Edition" and not LETTERS_RE.search(unicodedata.normalize("NFD", t)):
        return True
    tokens = [w.lower() for w in TEST_TOKEN_RE.findall(t)]
    return bool(tokens) and all(w in TEST_WORDS or w.isdigit() for w in tokens)
# Personal data (owner's ruling of 2026-10-26 evening): a manuscript transcription has no telephone numbers or e-mail
# addresses; team 850 filed a contact list (107 numbers) as a transcription. Held at PERSONAL_PHONES_MIN numbers -- a
# phone-shaped run never starts inside a longer number (image and file ids) -- or at any Latin e-mail address with a
# known top-level domain (the transcribers' "@...@" marks are not one).
PERSONAL_PHONES_MIN = 3
PHONE_RE = re.compile(r"(?<!\d)(?:05\d-?\d{7}|0[2-9]-?\d{7}|\+972[-\s]?\d[\d-]{7,})(?!\d)")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+\.(?:com|org|net|il|ac\.il|co\.il|edu|gov)\b", re.I)


def personal_data_counts(text: str) -> tuple:
    """(telephone numbers, e-mail addresses) in ``text``."""
    return len(PHONE_RE.findall(text or "")), len(EMAIL_RE.findall(text or ""))


def personal_data(text: str) -> bool:
    phones, emails = personal_data_counts(text)
    return phones >= PERSONAL_PHONES_MIN or emails > 0
# Superseded versions (owner's go-ahead of 2026-10-26 evening): FIST keeps every version of a unit as its own signature
# (same SourceId, SubId, UnitNumInSource; Version 1, 2, ...; the newer one's VersionSignature names the older). The
# export has no code table for dbo_Signature.RecordStatus, but 3,926 of the 3,945 new rows with status 5 have a newer
# status-0 version of the same unit and 3,920 of those newer versions are already in the candidate, 83% of the sampled
# pairs with identical letters -- status 5 is "replaced by a newer version". Status 1 (1,982 new rows, no newer
# version) has no meaning in the data; the owner looked it up on FJMS (2026-10-26): FGP shows the status-1
# transcriptions sampled from teams 200, 107 and 500 with the same text, and neither status-1 translation sampled (one
# of them not the fragment's text). Status-1 transcriptions enter; the 26 status-1 translations are held (R2g).
SUPERSEDED_STATUS = 5
STATUS_ONE = 1


def load_superseded(fist: sqlite3.Connection) -> dict:
    """{(unit_table, unit_id) -> the newest status-0 SignatureId of the same unit, or None} for every unit whose
    signature has RecordStatus SUPERSEDED_STATUS; {} on a FIST without the version columns."""
    try:
        sigs = fist.execute("SELECT SignatureId, SourceId, SubId, UnitNumInSource, Version, RecordStatus "
                            "FROM dbo_Signature").fetchall()
    except sqlite3.OperationalError:
        return {}
    versions = collections.defaultdict(list)
    for sid, src, sub, num, ver, status in sigs:
        if status == 0:
            versions[(src, sub, num)].append((ver or 0, sid))
    old = {}
    for sid, src, sub, num, ver, status in sigs:
        if status == SUPERSEDED_STATUS:
            newer = sorted(v for v in versions[(src, sub, num)] if v[0] > (ver or 0))
            old[sid] = newer[-1][1] if newer else None
    out = {}
    for table, id_col in (("dbo_UnitTranscription", "UnitTranscriptionId"), ("dbo_UnitTranslation", "UnitTranslationId")):
        for uid, sid in fist.execute(f"SELECT {id_col}, SignatureId FROM {table}"):
            if sid in old:
                out[(table, uid)] = old[sid]
    return out


def load_status_one(fist: sqlite3.Connection) -> set:
    """{(unit_table, unit_id)} of every unit whose FIST signature has RecordStatus STATUS_ONE; empty on a FIST
    without the column."""
    try:
        sigs = {sid for (sid,) in fist.execute("SELECT SignatureId FROM dbo_Signature WHERE RecordStatus = ?", (STATUS_ONE,))}
    except sqlite3.OperationalError:
        return set()
    out = set()
    for table, id_col in (("dbo_UnitTranscription", "UnitTranscriptionId"), ("dbo_UnitTranslation", "UnitTranslationId")):
        out |= {(table, uid) for uid, sid in fist.execute(f"SELECT {id_col}, SignatureId FROM {table}") if sid in sigs}
    return out
LETTERS_RE = re.compile(r"[א-תؠ-ي]+")
NEW_UID_RE = re.compile(r"^(dbo_\w+):(\d+)\|(\d+)$")


def log(msg: str) -> None:
    print(msg, flush=True)


def _j(v) -> str:
    return json.dumps(v, ensure_ascii=False, sort_keys=True)


def sha(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def letters(text: str) -> str:
    return "".join(LETTERS_RE.findall(unicodedata.normalize("NFD", text or "")))


def text_ratio(a: str, b: str) -> float:
    """Similarity of the two texts' letters over their whole length (a prefix would call two texts with one
    beginning and different endings the same)."""
    la, lb = letters(a), letters(b)
    if not la or not lb:
        return 0.0
    return difflib.SequenceMatcher(None, la, lb, autojunk=False).ratio()


def contained_share(short: str, long: str) -> float:
    """Share of ``short``'s letters that lie, in order, in runs of STUB_RUN or more letters of ``long``."""
    ls, ll = letters(short), letters(long)
    if not ls or not ll:
        return 0.0
    blocks = difflib.SequenceMatcher(None, ls, ll, autojunk=False).get_matching_blocks()
    return sum(b.size for b in blocks if b.size >= STUB_RUN) / len(ls)


def pct(values: list, q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(len(s) - 1, int(round(q * (len(s) - 1))))]


# --------------------------------------------------------------------------- inputs


def table_columns(conn: sqlite3.Connection, table: str) -> list:
    return [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')]


def load_today(conn: sqlite3.Connection) -> tuple:
    cols = table_columns(conn, "fgp_transcriptions")
    rows = {}
    for r in conn.execute("SELECT * FROM fgp_transcriptions ORDER BY id"):
        d = dict(zip(cols, r))
        rows[d["id"]] = d
    return rows, cols


def load_gate2(conn: sqlite3.Connection) -> tuple:
    cols = table_columns(conn, "row_outcomes")
    outcomes = {r[0]: dict(zip(cols, r)) for r in conn.execute("SELECT * FROM row_outcomes")}
    aliases = {r[0]: {"successor_id": r[1], "reason": r[2], "same_meaning": r[3]}
               for r in conn.execute("SELECT old_id, successor_id, reason, same_meaning FROM aliases")}
    return outcomes, aliases


def load_gate3(conn: sqlite3.Connection) -> tuple:
    cols = table_columns(conn, "text_rows")
    today, new = {}, []
    for r in conn.execute("SELECT * FROM text_rows ORDER BY row_key"):
        d = dict(zip(cols, r))
        if d["today_id"] is not None:
            today[d["today_id"]] = d
        else:
            new.append(d)
    return today, new


def load_texts(conn: sqlite3.Connection, keys: set) -> dict:
    """Texts of the units in ``keys`` only -- unit_texts is read once, streaming."""
    out = {}
    for ut, uid, text in conn.execute("SELECT unit_table, unit_id, text FROM unit_texts"):
        if (ut, uid) in keys:
            out[(ut, uid)] = text or ""
    return out


def load_inventory(conn: sqlite3.Connection) -> tuple:
    b_numbers = {r[0]: set(json.loads(r[1] or "[]")) for r in conn.execute("SELECT number, nli_alma_ids FROM b_numbers")}
    units = {}
    for ut, uid, so, su, bn in conn.execute("SELECT unit_table, unit_id, source_id, sub_id, basename FROM file_units"):
        units.setdefault((ut, uid), (so, su, bn))
    return b_numbers, units


def load_libraries(path: str) -> dict:
    out = {}
    if not path or not os.path.exists(path):
        return out
    with open(path, encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        next(reader, None)
        for row in reader:
            if len(row) > 3 and row[0]:
                shelf = (row[2] or "").split("|")[0].strip()
                out[row[0]] = (row[3] or "", shelf)
    return out


def load_approvals(path: str) -> dict:
    if not path or not os.path.exists(path):
        return {"page": {}, "default": {}}
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    page = {(int(a["id"]), a["field"]): a for a in data.get("page_assignment", [])}
    default = {(str(a["sys_id"]), int(a["p_num"])): a for a in data.get("default_selection", [])}
    return {"page": page, "default": default}


def load_exclusions(path: str) -> dict:
    """durable key -> reason, from the owner-ruled exclusions file (R2d); absent = none."""
    if not path or not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    out = {}
    for e in data.get("exclusions", []):
        if e["reason"] not in EXCLUSION_REASONS:
            raise ValueError(f"exclusion {e['durable_key']}: reason {e['reason']!r} is not one of {sorted(EXCLUSION_REASONS)}")
        out[e["durable_key"]] = e["reason"]
    return out


def image_number_of(reader: str, source_path: str) -> str | None:
    """The FGP image number a folder-B unit text belongs to: B2 ``C<n>\\data.js`` -> n; B1 ``<n>.xml`` -> n."""
    if reader == "b2_rows":
        return (source_path or "").replace("/", "\\").split("\\")[0].lstrip("Cc") or None
    if reader == "b1_xml":
        return os.path.splitext(os.path.basename(source_path or ""))[0] or None
    return None


# --------------------------------------------------------------------------- the candidate


def plan_candidate(today: dict, g2: dict, aliases: dict, g3_today: dict, g3_new: list, texts_conn: sqlite3.Connection,
                   b_numbers: dict, units: dict, libraries: dict, credit_ctx: dict, policy: str,
                   comments: dict | None = None, exclusions: dict | None = None, superseded: dict | None = None,
                   status_one: set | None = None) -> dict:
    comments = comments or {}
    exclusions = exclusions or {}
    superseded = superseded or {}
    status_one = status_one or set()
    known_keys = {(g3_today.get(rid) or g2.get(rid) or {}).get("durable_key") for rid in today} | {n["durable_key"] for n in g3_new}
    kept, excluded = [], []
    for rid in sorted(today):
        o = g2.get(rid)
        g = g3_today.get(rid)
        if o is None or o["outcome"] == "retired-with-alias":
            excluded.append((rid, "retired_alias" if rid in aliases else "retired", _j(aliases.get(rid))))
            continue
        holds = json.loads(o["hold_reasons"] or "[]")
        if not o["publishable"]:
            if holds == ["xml_unverified"] and g and g["xml_verified"]:
                pass
            elif holds == ["xml_unverified"]:
                excluded.append((rid, "xml_unverified", _j(holds)))
                continue
            else:
                excluded.append((rid, "gate2_hold", _j(holds)))
                continue
        replaced = bool(g and g["text_replaced"])
        kept.append({
            "id": rid, "replaced": replaced,
            "unit": (g["unit_table"], g["unit_id"]) if g else (o.get("unit_table"), o.get("unit_id")),
            "reader": g["reader"] if g else None, "durable_key": (g or o).get("durable_key"),
            "credit_he": o["credit_he"], "credit_en": o["credit_en"],
        })
    # texts needed: replaced today rows + every new row that may enter
    needed = {k["unit"] for k in kept if k["replaced"]}
    pre = []
    for n in g3_new:
        if n["status"] not in ("no_comparator", "checked", "pdf_held"):
            continue
        pre.append(n)
        needed.add((n["unit_table"], n["unit_id"]))
    # per-image (folder-B) rows first: the chooser keeps per-image rows and drops a whole-document row beside
    # them (dedupe_fgp_sources), so when both are new on one manuscript the per-image row enters and the
    # whole-document row is held whole_doc_shadowed -- never the reverse by accident of row-key order
    pre.sort(key=lambda n: (0 if n["reader"] in ("b1_xml", "b2_rows") else 1, n["row_key"]))
    texts = load_texts(texts_conn, needed) if needed else {}
    # R2: a carried row must have a text -- today's 13 text-less PDF rows (their DOCX text awaits a gate-3 approval)
    with_text = []
    for k in kept:
        text = texts.get(k["unit"], "") if k["replaced"] else (today[k["id"]].get("content") or "")
        if not text.strip():
            excluded.append((k["id"], "empty_text", "gate3:" + str((g3_today.get(k["id"]) or {}).get("hold_reason") or "")))
        elif k["unit"] in superseded:
            excluded.append((k["id"], "superseded_version", _j({"newer_signature": superseded[k["unit"]]})))
        elif k["durable_key"] in exclusions:
            excluded.append((k["id"], exclusions[k["durable_key"]], _j({"listed": k["durable_key"]})))
        elif personal_data(text):
            excluded.append((k["id"], "personal_data", _j(dict(zip(("phones", "emails"), personal_data_counts(text))))))
        elif test_or_dummy(text, today[k["id"]].get("doc_relation") or "Digital Edition"):
            excluded.append((k["id"], "test_or_dummy", text.strip()[:TEST_TEXT_MAX]))
        elif k["replaced"] and legacy_font(text):
            excluded.append((k["id"], "legacy_font", "%d orphan marks / %d letters" % legacy_font_stats(text)[::-1]))
        else:
            with_text.append(k)
    kept = with_text
    # R2c: retire the stub of a same-image pair of two units
    by_image = collections.defaultdict(list)
    for k in kept:
        t = today[k["id"]]
        if t.get("c_number"):
            by_image[(t["c_number"], t.get("doc_relation"), t.get("language"), t.get("sys_id"))].append(k)
    stubs = set()
    for group in by_image.values():
        if len({k["unit"] for k in group}) < 2:
            continue
        text_of = {k["id"]: (texts.get(k["unit"], "") if k["replaced"] else (today[k["id"]].get("content") or "")) for k in group}
        longest = max(group, key=lambda k: (len(letters(text_of[k["id"]])), -k["id"]))
        long_text = text_of[longest["id"]]
        for k in group:
            if k is longest or k["unit"] == longest["unit"]:
                continue
            short_text = text_of[k["id"]]
            if len(letters(short_text)) >= STUB_SHARE * len(letters(long_text)):
                continue
            share = contained_share(short_text, long_text)
            if share >= STUB_CONTAINED:
                stubs.add(k["id"])
                excluded.append((k["id"], "stub_retired", _j({"longer": f"fgp:{longest['id']}", "letters": len(letters(short_text)),
                                                                "longer_letters": len(letters(long_text)),
                                                                "contained": round(share, 3)})))
    kept = [k for k in kept if k["id"] not in stubs]
    # new rows
    new_rows, held, yardstick = [], [], {}
    kept_ids = {k["id"] for k in kept}
    per_image = collections.defaultdict(set)      # (image, relation, language, sys_id) -> units serving it (candidate today rows)
    whole_doc = collections.defaultdict(set)      # (relation, language, sys_id) -> whole-document candidate rows
    per_image_any = collections.defaultdict(set)  # (relation, language, sys_id) -> per-image candidate rows
    for rid in kept_ids:
        t = today[rid]
        key = (t.get("doc_relation"), t.get("language"), t.get("sys_id"))
        cn = (t.get("c_number") or "").lstrip("Cc")
        unit = g3_today[rid]["unit_id"] if rid in g3_today else g2[rid].get("unit_id")
        if cn:
            per_image[(cn,) + key].add(unit)
            per_image_any[key].add(rid)
        else:
            whole_doc[key].add(rid)
    next_id = (max(today) if today else 0) + 1
    for n in pre:
        rk, ut, uid, sid = n["row_key"], n["unit_table"], n["unit_id"], n["sys_id"]
        if policy != "plain":
            held.append((rk, "policy_hold", policy))
            continue
        if not sid:
            held.append((rk, "no_sys_id", ""))
            continue
        text = texts.get((ut, uid), "")
        if not text.strip():
            held.append((rk, "blank_text", ""))
            continue
        if (ut, uid) in superseded:
            held.append((rk, "superseded_version", _j({"newer_signature": superseded[(ut, uid)]})))
            continue
        if n["kind"] == "translation" and (ut, uid) in status_one:
            held.append((rk, "status1_translation", ""))
            continue
        if n["durable_key"] in exclusions:
            held.append((rk, exclusions[n["durable_key"]], _j({"listed": n["durable_key"]})))
            continue
        if personal_data(text):
            held.append((rk, "personal_data", _j(dict(zip(("phones", "emails"), personal_data_counts(text))))))
            continue
        comment = comments.get((ut, uid))
        if comment and NO_PUBLISH_COMMENT_RE.search(comment):
            held.append((rk, "fist_comment_no_publish", comment[:80]))
            continue
        relation = "Digital Translation" if n["kind"] == "translation" else "Digital Edition"
        if test_or_dummy(text, relation):
            held.append((rk, "test_or_dummy", text.strip()[:TEST_TEXT_MAX]))
            continue
        if legacy_font(text):
            held.append((rk, "legacy_font", "%d orphan marks / %d letters" % legacy_font_stats(text)[::-1]))
            continue
        structure = json.loads(n["structure"] or "{}") if n["structure"] else {}
        if n["reader"] == "docx":
            if (structure.get("table_max_columns") or 0) >= MULTICOLUMN_MIN:
                held.append((rk, "docx_multicolumn_table", _j({"columns": structure["table_max_columns"],
                                                               "tables": structure.get("tables", 0)})))
                continue
            if n.get("status") == "pdf_held":
                held.append((rk, "pdf_held", _j({"held_blocks": n.get("held_blocks"),
                                                 "classes": sorted(json.loads(n.get("classes") or "{}"))[:8]})))
                continue
        language = n["language"]
        key = (relation, language, sid)
        image = image_number_of(n["reader"], n["source_path"])
        if n["reader"] in ("b1_xml", "b2_rows"):
            if image not in b_numbers:
                held.append((rk, "image_unknown", image or ""))
                continue
            if sid not in b_numbers[image]:
                held.append((rk, "image_alma_mismatch", _j({"image": image, "gallery": sorted(b_numbers[image])})))
                continue
            served = per_image.get((image,) + key)
            if served and served != {uid}:
                held.append((rk, "image_already_served", _j({"image": image, "units": sorted(served)})))
                continue
            if whole_doc.get(key):
                held.append((rk, "shadows_whole_doc_row", _j(sorted(whole_doc[key]))))
                continue
        else:
            image = None
            if per_image_any.get(key):
                held.append((rk, "whole_doc_shadowed", _j(sorted(per_image_any[key])[:10])))
                continue
        so, su, basename = units.get((ut, uid), (None, None, n["basename"]))
        he, en, cat = (None, None, "unresolved")
        if so is not None:
            he, en, cat = credit_for_unit({"source": so, "sub": su}, sid, credit_ctx["parts"], credit_ctx["catalog"],
                                          credit_ctx["book"], credit_ctx["bibref"])
        if not he:
            he, en, cat = GENERIC_CREDIT[0], GENERIC_CREDIT[1], "generic"
        lib, shelf = libraries.get(sid, ("", ""))
        m = re.match(r"^\d{14}", basename or "")
        src = (n["source_path"] or "").replace("\\", "/")
        yardstick[rk] = "pdf" if n.get("yardstick") else "none"
        new_rows.append({
            "id": next_id, "row_key": rk, "collection": lib or "FIST", "shelfmark": shelf or (basename or ""),
            "c_number": f"C{image}" if image else None, "image_id": m.group(0) if m else (basename or ""),
            "source_scholar": "FGP", "doc_relation": relation, "language": language, "content": text,
            "content_length": sum(1 for c in text if not c.isspace()), "n_pages": 0, "sections": None,
            "heb_ratio": heb_ratio(text), "size_bytes": len(text.encode("utf-8")),
            "rel_path": f"fist/{src}#{ut}:{uid}|{sid}", "filename": basename or "", "sys_id": sid, "mms_id": sid,
            "sysid_method": "fist_inventory_alma", "image_side": None, "page_info": None, "folio_num": None,
            "source_credit": he, "source_credit_he": he, "source_credit_en": en, "text_source": f"gate3:{n['reader']}",
            "origin": "new", "text_provenance": f"gate3:{n['reader']}", "fist_unit_table": ut, "fist_unit_id": uid,
            "durable_key": n["durable_key"], "credit_category": cat,
        })
        next_id += 1
        # a later new row on the same image / manuscript sees this one
        if image:
            per_image[(image,) + key].add(uid)
            per_image_any[key].add(next_id - 1)
        else:
            whole_doc[key].add(next_id - 1)
    return {"kept": kept, "excluded": excluded, "new_rows": new_rows, "held": held, "texts": texts, "yardstick": yardstick,
            "exclusions_unmatched": sorted(k for k in exclusions if k not in known_keys)}


def write_candidate(path: str, today_db: str, today_cols: list, plan: dict, meta: dict) -> None:
    if os.path.exists(path):
        old = ro(path)
        try:
            names = {r[0] for r in old.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            mine = "fgp_meta" in names and old.execute("SELECT 1 FROM fgp_meta WHERE key='candidate'").fetchone()
        finally:
            old.close()
        if not mine:
            raise SystemExit(f"{path} exists and is not a gate-4 candidate; refusing to overwrite it")
        os.remove(path)
    con = sqlite3.connect(path)
    con.execute("ATTACH DATABASE ? AS today", (today_db,))
    sql = con.execute("SELECT sql FROM today.sqlite_master WHERE type='table' AND name='fgp_transcriptions'").fetchone()[0]
    con.execute(sql)
    for col in EXTRA_COLUMNS:
        if col not in today_cols:
            con.execute(f'ALTER TABLE fgp_transcriptions ADD COLUMN "{col}" {"INTEGER" if col == "fist_unit_id" else "TEXT"}')
    cols = today_cols + [c for c in EXTRA_COLUMNS if c not in today_cols]
    con.execute("CREATE TEMP TABLE keep (id INTEGER PRIMARY KEY)")
    con.executemany("INSERT INTO keep VALUES (?)", [(k["id"],) for k in plan["kept"]])
    quoted = ", ".join(f'"{c}"' for c in today_cols)
    con.execute(f"INSERT INTO fgp_transcriptions ({quoted}) SELECT {quoted} FROM today.fgp_transcriptions "
                "WHERE id IN (SELECT id FROM keep)")
    has = set(cols)
    # provenance, unit and credit of every carried row
    con.executemany(
        'UPDATE fgp_transcriptions SET origin=?, text_provenance=?, fist_unit_table=?, fist_unit_id=?, durable_key=? WHERE id=?',
        [("today", f"gate3:{k['reader']}" if k["replaced"] else "today", k["unit"][0], k["unit"][1], k["durable_key"], k["id"])
         for k in plan["kept"]])
    credit_cols = [c for c in ("source_credit", "source_credit_he", "source_credit_en") if c in has]
    if credit_cols:
        sets = ", ".join(f'"{c}"=?' for c in credit_cols)
        con.executemany(f"UPDATE fgp_transcriptions SET {sets} WHERE id=?",
                        [tuple(k["credit_en"] if c == "source_credit_en" else k["credit_he"] for c in credit_cols) + (k["id"],)
                         for k in plan["kept"]])
    text_cols = [c for c in ("content", "content_length", "heb_ratio", "size_bytes", "sections", "text_source") if c in has]
    if text_cols:
        sets = ", ".join(f'"{c}"=?' for c in text_cols)
        params = []
        for k in plan["kept"]:
            if not k["replaced"]:
                continue
            text = plan["texts"][k["unit"]]
            values = {"content": text, "content_length": sum(1 for c in text if not c.isspace()),
                      "heb_ratio": heb_ratio(text), "size_bytes": len(text.encode("utf-8")), "sections": None,
                      "text_source": f"gate3:{k['reader']}"}
            params.append(tuple(values[c] for c in text_cols) + (k["id"],))
        con.executemany(f"UPDATE fgp_transcriptions SET {sets} WHERE id=?", params)
    if plan["new_rows"]:
        new_cols = [c for c in cols if c in plan["new_rows"][0]]
        names = ", ".join(f'"{c}"' for c in new_cols)
        marks = ", ".join("?" * len(new_cols))
        con.executemany(f"INSERT INTO fgp_transcriptions ({names}) VALUES ({marks})",
                        [tuple(r[c] for c in new_cols) for r in plan["new_rows"]])
    for table in ("fgp_shelfmark_meta", "fgp_cnumber_info"):
        if con.execute("SELECT 1 FROM today.sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
            con.execute(f'CREATE TABLE "{table}" AS SELECT * FROM today."{table}"')
    for (isql,) in con.execute("SELECT sql FROM today.sqlite_master WHERE type='index' AND sql IS NOT NULL").fetchall():
        try:
            con.execute(isql)
        except sqlite3.OperationalError:
            pass
    con.execute("CREATE TABLE IF NOT EXISTS fgp_meta (key TEXT PRIMARY KEY, value TEXT)")
    if con.execute("SELECT 1 FROM today.sqlite_master WHERE type='table' AND name='fgp_meta'").fetchone():
        con.execute("INSERT OR IGNORE INTO fgp_meta SELECT key, value FROM today.fgp_meta")
    con.executemany("INSERT OR REPLACE INTO fgp_meta VALUES (?, ?)", [
        ("version", CANDIDATE_VERSION), ("candidate", "gate 4 chooser validation"),
        ("built_at", meta["run_at"]), ("built_from_gate3_digest", meta.get("gate3_digest") or ""),
        ("new_rows_policy", meta["new_rows_policy"]),
        ("rows", str(len(plan["kept"]) + len(plan["new_rows"]))),
        ("rows_today_carried", str(len(plan["kept"]))), ("rows_new", str(len(plan["new_rows"]))),
    ])
    con.commit()
    con.execute("DETACH DATABASE today")
    con.close()


# --------------------------------------------------------------------------- chooser drives


def source_rows(svc: fs.FgpService, sid: str) -> list:
    return svc.get_fgp_sources_for_fragment(sid) or []


def row_text(db: sqlite3.Connection, rid: int) -> str:
    r = db.execute("SELECT content FROM fgp_transcriptions WHERE id=?", (rid,)).fetchone()
    return r[0] if r else ""


def check_selectability(svc_t: fs.FgpService, svc_c: fs.FgpService, db_t: sqlite3.Connection, db_c: sqlite3.Connection,
                        sys_ids: list, excluded: dict, aliases: dict, max_today_id: int) -> tuple:
    rows, C = [], collections.Counter()
    new_total = db_c.execute("SELECT COUNT(*) FROM fgp_transcriptions WHERE id > ?", (max_today_id,)).fetchone()[0]
    for sid in sys_ids:
        a = source_rows(svc_t, sid)
        b = source_rows(svc_c, sid)
        uids_b = {s["uid"]: s for s in b}
        ids_c = {r[0] for r in db_c.execute("SELECT id FROM fgp_transcriptions WHERE sys_id=?", (sid,))}
        order_changed = [s["uid"] for s in a if s["uid"] in uids_b] != [s["uid"] for s in b if s["uid"] in {x["uid"] for x in a}]
        if order_changed:
            C["selectability.order_changed_manuscripts"] += 1
        for s in a:
            uid = s["uid"]
            if uid in uids_b:
                C["selectability.kept"] += 1
                continue
            rid = s["id"]
            if rid not in ids_c:
                reason, detail = excluded.get(rid, ("missing", ""))
                disp = reason
                if reason == "retired_alias":
                    al = aliases.get(rid) or {}
                    if al.get("same_meaning") and al.get("successor_id") is not None:
                        succ = f"fgp:{al['successor_id']}"
                        disp = "retired_alias" if succ in uids_b else "successor_unselectable"
                        detail = succ
            else:
                # carried, but its image group is now won by another row: same text or not?
                key = (s.get("fgp_c_number"), s.get("doc_relation"), s.get("language"))
                winner = next((w for w in b if (w.get("fgp_c_number"), w.get("doc_relation"), w.get("language")) == key), None)
                ratio = text_ratio(row_text(db_c, rid), row_text(db_c, winner["id"])) if winner else 0.0
                disp = "same_text_swap" if ratio >= SAME_TEXT_RATIO else "unexplained"
                detail = _j({"winner": winner["uid"] if winner else None, "ratio": round(ratio, 3)})
            C[f"selectability.lost.{disp}"] += 1
            rows.append((sid, uid, 1, 0, disp, detail))
        for s in b:
            if s["id"] > max_today_id:
                C["selectability.new_listed"] += 1
                rows.append((sid, s["uid"], 0, 1, "new_listed", ""))
    C["selectability.new_unlisted"] = new_total - C["selectability.new_listed"]
    return rows, C


def check_page_assignment(db_t: sqlite3.Connection, db_c: sqlite3.Connection, cols_t: list, kept_ids: list,
                          new_rows: list, nli: sqlite3.Connection | None, approvals: dict) -> tuple:
    fields = [f for f in PAGE_FIELDS if f in cols_t]
    rows, C = [], collections.Counter()
    sel = ", ".join(f'"{f}"' for f in fields)
    today_vals = {r[0]: r[1:] for r in db_t.execute(f"SELECT id, {sel} FROM fgp_transcriptions")}
    cand_vals = {r[0]: r[1:] for r in db_c.execute(f"SELECT id, {sel} FROM fgp_transcriptions")}
    for rid in kept_ids:
        tv, cv = today_vals.get(rid), cand_vals.get(rid)
        if tv == cv:
            C["page_assignment.unchanged"] += 1
            continue
        for f, a, b in zip(fields, tv or (), cv or ()):
            if a == b:
                continue
            approved = (rid, f) in approvals["page"]
            C["page_assignment.approved" if approved else "page_assignment.unapproved"] += 1
            rows.append((rid, f, str(a), str(b), int(approved)))
    for r in new_rows:
        if not r["c_number"]:
            C["page_assignment.new_whole_doc"] += 1
            continue
        ok = False
        if nli is not None:
            ok = nli.execute("SELECT 1 FROM nli_images WHERE FGPImageNumberId=? AND NLI_AlmaId=? LIMIT 1",
                             (r["c_number"].lstrip("Cc"), r["sys_id"])).fetchone() is not None
        C["page_assignment.new_image_confirmed" if ok else "page_assignment.new_image_unconfirmed"] += 1
        if not ok:
            rows.append((r["id"], "c_number", "", r["c_number"], 0))
    return rows, C


class HtrSource:
    """HTR (V0.8) pages per manuscript: the search engine the apps use, or a JSON file (tests)."""

    def __init__(self, json_path: str | None):
        self._json = None
        self._engine = None
        if json_path:
            with open(json_path, encoding="utf-8") as fh:
                self._json = json.load(fh)

    def pages(self, sid: str) -> list:
        if self._json is not None:
            return [{"p_num": int(p["p_num"]), "text": p.get("text") or ""} for p in self._json.get(sid, [])]
        if self._engine is None:
            from shared.metadata_manager import MetadataManager
            from shared.variants import VariantManager
            import shared.search_engine as se
            self._engine = se.SearchEngine(MetadataManager(), VariantManager(), worker_mode=True, open_local=False)
        pages = self._engine.get_full_manuscript(sid) or []
        return sorted(({"p_num": p["p_num"], "text": p.get("text") or ""} for p in pages), key=lambda p: p["p_num"])


def reference_set(today_by_sid: dict, new_by_sid: dict, replaced_sids: set, excluded_sids: set, swap_sids: set,
                  multi_unit_sids: set, limit: int | None) -> dict:
    groups = collections.defaultdict(set)
    rng = random.Random(REFERENCE_SEED)
    for sid, _ in sorted(today_by_sid.items(), key=lambda kv: (-kv[1], kv[0]))[:10]:
        groups[sid].add("A_most_today_rows")
    for sid, _ in sorted(new_by_sid.items(), key=lambda kv: (-kv[1], kv[0]))[:10]:
        groups[sid].add("B_most_new_rows")
    if MS_HEB_G2 in today_by_sid:
        groups[MS_HEB_G2].add("C_ms_heb_g2")
    for sid in rng.sample(sorted(replaced_sids), min(20, len(replaced_sids))):
        groups[sid].add("D_replaced_text")
    both = sorted(set(today_by_sid) & set(new_by_sid))
    for sid in rng.sample(both, min(20, len(both))):
        groups[sid].add("E_today_and_new")
    for sid in sorted(excluded_sids)[:20]:
        groups[sid].add("F_excluded_row")
    for sid in sorted(swap_sids):
        groups[sid].add("G_same_image_swap")
    for sid in sorted(multi_unit_sids - swap_sids)[:10]:
        groups[sid].add("G_same_image_units")
    if limit:
        groups = dict(sorted(groups.items())[:limit])
    return dict(groups)


def check_defaults(svc_t: fs.FgpService, svc_c: fs.FgpService, db_t: sqlite3.Connection, db_c: sqlite3.Connection,
                   nli_svc, htr: HtrSource, refs: dict, max_today_id: int, excluded: dict, approvals: dict) -> tuple:
    decisions, ref_rows, lat_load_t, lat_load_c, lat_page_t, lat_page_c = [], [], [], [], [], []
    C = collections.Counter()
    heaviest = (None, -1)
    first = True
    for sid in sorted(refs):
        pages = htr.pages(sid)
        imgs = nli_svc.get_folio_images(sid) if nli_svc is not None else []
        n = len(pages)
        if first:  # one warm-up per DB so cold connections do not count as latency
            source_rows(svc_t, sid)
            source_rows(svc_c, sid)
            first = False
        t0 = time.perf_counter()
        a = source_rows(svc_t, sid)
        t1 = time.perf_counter()
        b = source_rows(svc_c, sid)
        t2 = time.perf_counter()
        lat_load_t.append((t1 - t0) * 1000)
        lat_load_c.append((t2 - t1) * 1000)
        if len(b) > heaviest[1]:
            heaviest = (sid, len(b))
        ref_rows.append((sid, _j(sorted(refs[sid])), len(a), len(b), n, len(imgs)))
        if not pages:
            C["reference.without_htr"] += 1
            pages = [{"p_num": 1, "text": ""}]
            n = 1
        full = "\n".join(p["text"] for p in pages if p["text"])
        a_by_uid = {s["uid"]: s for s in a}
        for p in pages:
            pn, text = p["p_num"], p["text"]
            folio = fs.folio_label_for_displayed_page(imgs, pn, n)
            image = fs.fgp_image_number_for_displayed_page(imgs, pn, n)
            # desktop shape: the module resolvers over the crossref singleton
            if nli_svc is not None:
                d_folio, d_image = fs.displayed_folio_label(sid, pn, n), fs.displayed_fgp_image_number(sid, pn, n)
                if (d_folio or "") != (folio or "") or (d_image or "") != (image or ""):
                    C["defaults.shape_disagreement"] += 1
            res = {}
            for name, src, lat in (("today", a, lat_page_t), ("cand", b, lat_page_c)):
                t0 = time.perf_counter()
                web = fs.filter_sources_for_page(list(src), pn, folio, image, page_text=text)
                dec = fs.choose_default_source(web, text, full_htr_getter=lambda full=full: full)
                lat.append((time.perf_counter() - t0) * 1000)
                desk = fs._select_fgp_sources_for_page(list(src), folio, image, text)
                if [s["uid"] for s in web] != [s["uid"] for s in desk]:
                    C["defaults.shape_disagreement"] += 1
                res[name] = (dec, [s["uid"] for s in web])
            (dt, set_t), (dc, set_c) = res["today"], res["cand"]
            ut = dt["source"]["uid"] if dt.get("source") else None
            uc = dc["source"]["uid"] if dc.get("source") else None
            changed = (ut, dt["reason"], bool(dt["eligible"])) != (uc, dc["reason"], bool(dc["eligible"]))
            disp, detail = "unchanged", ""
            if changed:
                cid = dc["source"]["id"] if dc.get("source") else None
                tid = dt["source"]["id"] if dt.get("source") else None
                replaced = [u for u in set_t if u in set_c and sha(row_text(db_t, int(u.split(":")[1]))) !=
                            sha(row_text(db_c, int(u.split(":")[1])))]
                gone = [u for u in set_t if int(u.split(":")[1]) in excluded]
                if (sid, pn) in approvals["default"]:
                    disp, detail = "approved", approvals["default"][(sid, pn)].get("by", "")
                elif cid is not None and cid > max_today_id:
                    disp, detail = "new_edition", uc or ""
                elif (tid is not None and tid in excluded) or (gone and all(u in set_t for u in set_c)):
                    # a row of today's page set left the candidate (chosen or not: a demoted edition's absence
                    # changes the reason too)
                    disp, detail = "row_excluded", _j(sorted({excluded[int(u.split(':')[1])][0] for u in gone}
                                                             | ({excluded[tid][0]} if tid in excluded else set())))
                elif ut and ut not in set_c and set_c:
                    # the page's edition changed hands: the same text, a new row, a text gate 3 replaced on
                    # either side (which moves a whole-document row's page coverage), or unexplained
                    other = set_c[0]
                    oid = int(other.split(":")[1])
                    ratio = text_ratio(row_text(db_t, tid), row_text(db_c, oid))
                    if ratio >= SAME_TEXT_RATIO:
                        disp = "same_text_swap"
                    elif oid > max_today_id:
                        disp = "new_edition"
                    elif any(sha(row_text(db_t, i)) != sha(row_text(db_c, i)) for i in (tid, oid)):
                        disp = "text_replaced"
                    else:
                        disp = "unexplained"
                    detail = _j({"now": other, "ratio": round(ratio, 3)})
                elif replaced:
                    disp, detail = "text_replaced", _j(replaced[:5])
                elif set_t != set_c and any(u not in a_by_uid for u in set_c):
                    disp, detail = "new_edition", _j([u for u in set_c if u not in a_by_uid][:5])
                else:
                    disp, detail = "unexplained", _j({"today": set_t[:5], "cand": set_c[:5]})
                C[f"defaults.changed.{disp}"] += 1
            else:
                C["defaults.unchanged"] += 1
            C["defaults.pages"] += 1
            decisions.append((sid, pn, ut, dt["reason"], uc, dc["reason"], int(changed), disp, detail))
    lat = {"load.today": lat_load_t, "load.cand": lat_load_c, "page.today": lat_page_t, "page.cand": lat_page_c}
    return decisions, ref_rows, lat, heaviest, C


def measure_memory(cand_path: str, svc_c: fs.FgpService, heaviest_sid: str | None, build_peak_mb: float) -> list:
    try:
        import psutil
    except ImportError:  # pragma: no cover - the environment has psutil; without it the check fails
        return [("memory.build_peak_mb", build_peak_mb, BUDGETS["memory.build_peak_mb"], 0, "psutil missing")]
    proc = psutil.Process()
    rows = [("memory.build_peak_mb", round(build_peak_mb, 1), BUDGETS["memory.build_peak_mb"],
             int(build_peak_mb <= BUDGETS["memory.build_peak_mb"]), "peak working set while building the candidate")]
    before = proc.memory_info().rss
    svc = fs.FgpService(db_path=cand_path)
    eds = svc.get_sys_ids_with_fgp_editions()
    all_sids = [r[0] for r in svc._conn.execute('SELECT DISTINCT sys_id FROM fgp_transcriptions WHERE sys_id IS NOT NULL')]
    srcs = svc.get_sys_ids_with_fgp_sources(all_sids)
    delta = (proc.memory_info().rss - before) / 2**20
    rows.append(("memory.app_open_mb", round(delta, 1), BUDGETS["memory.app_open_mb"], int(delta <= BUDGETS["memory.app_open_mb"]),
                 f"open + editions set ({len(eds):,}) + sources set ({len(srcs):,})"))
    svc.close()
    if heaviest_sid:
        before = proc.memory_info().rss
        rows_h = source_rows(svc_c, heaviest_sid)
        delta = (proc.memory_info().rss - before) / 2**20
        rows.append(("memory.heaviest_manuscript_mb", round(delta, 1), BUDGETS["memory.heaviest_manuscript_mb"],
                     int(delta <= BUDGETS["memory.heaviest_manuscript_mb"]), f"{heaviest_sid}: {len(rows_h)} chooser rows"))
    size = os.path.getsize(cand_path) / 2**20
    rows.append(("size.candidate_mb", round(size, 1), BUDGETS["size.candidate_mb"], int(size <= BUDGETS["size.candidate_mb"]), ""))
    return rows


def latency_rows(lat: dict) -> list:
    rows = []
    for metric in ("load", "page"):
        t, c = lat[f"{metric}.today"], lat[f"{metric}.cand"]
        p95_t, p95_c = pct(t, 0.95), pct(c, 0.95)
        rows.append((f"latency.{metric}.today", len(t), round(pct(t, 0.5), 2), round(p95_t, 2), round(max(t or [0]), 2), None, 1, ""))
        ok = (p95_c <= BUDGETS[f"latency.{metric}.p95_ms"] and max(c or [0]) <= BUDGETS[f"latency.{metric}.max_ms"]
              and p95_c <= p95_t * BUDGETS["latency.ratio_vs_today"] + BUDGETS["latency.ratio_floor_ms"])
        rows.append((f"latency.{metric}.cand", len(c), round(pct(c, 0.5), 2), round(p95_c, 2), round(max(c or [0]), 2),
                     BUDGETS[f"latency.{metric}.p95_ms"], int(ok),
                     f"max budget {BUDGETS[f'latency.{metric}.max_ms']} ms; <= {BUDGETS['latency.ratio_vs_today']}x today's p95 + "
                     f"{BUDGETS['latency.ratio_floor_ms']} ms"))
    return rows


# --------------------------------------------------------------------------- checks and outputs


def run_checks(C: collections.Counter, latency: list, memory: list, contract: dict, digest: str, previous: str | None,
               allow_single: bool, expectations: dict) -> list:
    checks = []

    def add(name, passed, expected, actual, note=""):
        checks.append({"name": name, "passed": int(bool(passed)), "expected": str(expected), "actual": str(actual), "note": note})

    if previous:
        add("C1_two_runs_identical", digest == previous, previous[:16], digest[:16], "digest of the candidate rows and dispositions")
    else:
        add("C1_two_runs_identical", allow_single, "previous digest", "single run allowed" if allow_single else "no previous summary",
            "pass --previous-summary or --allow-single-run")
    bad = sum(v for k, v in C.items() if k.startswith("selectability.lost.")
              and k.split(".")[-1] not in SELECTABILITY_DISPOSITIONS)
    add("C2_selectability_dispositioned", bad == 0, 0, bad, "today-listed sources lost without a declared disposition")
    add("C2_new_rows_listed", C.get("selectability.new_unlisted", 0) == 0, 0, C.get("selectability.new_unlisted", 0),
        "new rows the chooser does not list")
    add("C3_identity", contract["identity_mismatch"] == 0, 0, contract["identity_mismatch"],
        "carried rows whose id / uid / relation / language changed")
    add("C4_page_assignment", C.get("page_assignment.unapproved", 0) == 0, 0, C.get("page_assignment.unapproved", 0),
        "carried rows whose page keys changed without an approval")
    add("C4_new_images_confirmed", C.get("page_assignment.new_image_unconfirmed", 0) == 0, 0,
        C.get("page_assignment.new_image_unconfirmed", 0), "new per-image rows nli_crossref.db does not confirm")
    add("C5_defaults_dispositioned", C.get("defaults.changed.unexplained", 0) == 0, 0, C.get("defaults.changed.unexplained", 0),
        "default-selection changes on the reference set without a disposition")
    add("C5_both_shapes_agree", C.get("defaults.shape_disagreement", 0) == 0, 0, C.get("defaults.shape_disagreement", 0),
        "web and desktop page filters or resolvers disagreed")
    for r in latency:
        if r[0].endswith(".cand"):
            add(f"C6_{r[0]}", r[6], f"p95 <= {r[5]} ms", f"p95 {r[3]} ms, max {r[4]} ms", r[7])
    for r in memory:
        add(f"C7_{r[0]}", r[3], f"<= {r[2]} MB", f"{r[1]} MB", r[4])
    add("C8_service_contract", contract["ok"], "table found, version set, every row with sys_id and text",
        contract["detail"], "")
    add("C9_exclusions_matched", C.get("exclusions.unmatched", 0) == 0, 0, C.get("exclusions.unmatched", 0),
        "durable keys listed in the exclusions file that name no row")
    for key, exp in sorted(expectations.items()):
        actual = C.get(key)
        add(f"E_{key}", actual == exp, exp, actual if actual is not None else "missing", "declared expectation")
    return checks


def _create(conn, table, columns, rows, pk=None):
    cols = ", ".join(f'"{c}"' + (" PRIMARY KEY" if c == pk else "") for c in columns)
    conn.execute(f'CREATE TABLE "{table}" ({cols})')
    conn.executemany(f'INSERT INTO "{table}" VALUES ({", ".join("?" * len(columns))})', rows)


def write_outputs(out_dir: str, meta: dict, plan: dict, cand_rows: list, sel: list, page: list, decisions: list,
                  refs: list, latency: list, memory: list, checks: list, C: dict) -> str:
    db_path = os.path.join(out_dir, "gate4.db")
    if os.path.exists(db_path):
        old = ro(db_path)
        try:
            names = {r[0] for r in old.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            old.close()
        if "gate4_meta" not in names:
            raise SystemExit(f"{db_path} exists and is not a gate-4 output; refusing to overwrite it")
        os.remove(db_path)
    con = sqlite3.connect(db_path)
    _create(con, "gate4_meta", ("key", "value"), [(k, str(v)) for k, v in meta.items()], pk="key")
    _create(con, "candidate_rows", ("id", "origin", "row_key", "durable_key", "sys_id", "c_number", "doc_relation", "language",
                                    "text_provenance", "text_hash", "letters", "credit_he", "credit_category"), cand_rows, pk="id")
    _create(con, "row_dispositions", ("key", "origin", "disposition", "detail"),
            [(str(rid), "today", reason, detail) for rid, reason, detail in plan["excluded"]] +
            [(rk, "new", reason, detail) for rk, reason, detail in plan["held"]])
    _create(con, "selectability", ("sys_id", "uid", "today_listed", "candidate_listed", "disposition", "detail"), sel)
    _create(con, "page_assignment", ("id", "field", "today_value", "candidate_value", "approved"), page)
    _create(con, "default_decisions", ("sys_id", "p_num", "today_uid", "today_reason", "candidate_uid", "candidate_reason",
                                       "changed", "disposition", "detail"), decisions)
    _create(con, "reference_set", ("sys_id", "groups", "today_rows", "candidate_rows", "htr_pages", "gallery_images"), refs)
    _create(con, "latency", ("metric", "n", "p50_ms", "p95_ms", "max_ms", "budget_p95_ms", "passed", "note"), latency)
    _create(con, "memory", ("metric", "value_mb", "budget_mb", "passed", "note"), memory)
    _create(con, "checks", ("name", "passed", "expected", "actual", "note"),
            [(c["name"], c["passed"], c["expected"], c["actual"], c["note"]) for c in checks], pk="name")
    _create(con, "counts", ("key", "value"), sorted((k, v) for k, v in C.items()), pk="key")
    con.commit()
    con.close()
    summary = {"meta": meta, "counts": dict(sorted(C.items())), "checks": checks, "latency": latency, "memory": memory,
               "budgets": BUDGETS}
    with open(os.path.join(out_dir, "gate4_summary.json"), "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=1)
    with open(os.path.join(out_dir, "GATE4_REPORT.md"), "w", encoding="utf-8") as fh:
        fh.write(render_report(meta, plan, sel, decisions, refs, latency, memory, checks, C))
    return db_path


def render_report(meta, plan, sel, decisions, refs, latency, memory, checks, C) -> str:
    failed = [c for c in checks if not c["passed"]]
    L = [f"# FGP gate 4 -- chooser validation with the larger sidecar ({meta['run_at']})", "",
         f"**{'FAIL' if failed else 'PASS'}** -- {len(checks) - len(failed)} / {len(checks)} checks; digest `{meta['digest'][:16]}`; "
         f"new-rows policy `{meta['new_rows_policy']}`.", "",
         "## The candidate sidecar", "",
         f"- today's rows carried {len(plan['kept']):,} (text replaced by gate 3 on {sum(1 for k in plan['kept'] if k['replaced']):,}), "
         f"excluded {len(plan['excluded']):,}: " +
         ", ".join(f"{k} {v:,}" for k, v in sorted(collections.Counter(r for _, r, _ in plan['excluded']).items())),
         f"- new rows entered {len(plan['new_rows']):,}, held {len(plan['held']):,}: " +
         ", ".join(f"{k} {v:,}" for k, v in sorted(collections.Counter(r for _, r, _ in plan['held']).items())),
         f"- rows {len(plan['kept']) + len(plan['new_rows']):,}; `fgp_meta.version` {CANDIDATE_VERSION}", "",
         "## Selectability (every manuscript, the real service on both DBs)", ""]
    for k, v in sorted(C.items()):
        if k.startswith("selectability."):
            L.append(f"- `{k}` {v:,}")
    L += ["", "## Page assignment", ""]
    for k, v in sorted(C.items()):
        if k.startswith("page_assignment."):
            L.append(f"- `{k}` {v:,}")
    L += ["", f"## Default selection on the reference set ({len(refs)} manuscripts)", ""]
    for k, v in sorted(C.items()):
        if k.startswith("defaults.") or k.startswith("reference."):
            L.append(f"- `{k}` {v:,}")
    changed = [d for d in decisions if d[6]]
    if changed:
        L += ["", "| manuscript | page | today | candidate | disposition | detail |", "|---|---|---|---|---|---|"]
        for d in changed[:40]:
            L.append(f"| {d[0]} | {d[1]} | {d[2] or 'V0.8'} ({d[3]}) | {d[4] or 'V0.8'} ({d[5]}) | {d[7]} | {d[8][:80]} |")
    L += ["", "| manuscript | groups | today rows | candidate rows | HTR pages | gallery images |", "|---|---|---|---|---|---|"]
    for r in refs:
        L.append(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | {r[5]} |")
    L += ["", "## Latency (ms)", "", "| metric | n | p50 | p95 | max | budget p95 | ok |", "|---|---|---|---|---|---|---|"]
    for r in latency:
        L.append(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | {r[5] or ''} | {'yes' if r[6] else 'NO'} |")
    L += ["", "## Memory and size (MB)", "", "| metric | value | budget | ok | note |", "|---|---|---|---|---|"]
    for r in memory:
        L.append(f"| {r[0]} | {r[1]} | {r[2]} | {'yes' if r[3] else 'NO'} | {r[4]} |")
    L += ["", "## Checks", "", "| check | ok | expected | actual |", "|---|---|---|---|"]
    for c in checks:
        L.append(f"| {c['name']} | {'yes' if c['passed'] else 'NO'} | {c['expected']} | {c['actual']} |")
    return "\n".join(L) + "\n"


# --------------------------------------------------------------------------- main


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--fgp-db", default=DEFAULT_FGP_DB, help="today's sidecar (read only)")
    ap.add_argument("--gate2", default=DEFAULT_GATE2)
    ap.add_argument("--gate3", default=DEFAULT_GATE3)
    ap.add_argument("--inventory", default=DEFAULT_INVENTORY)
    ap.add_argument("--fist", default=DEFAULT_FIST)
    ap.add_argument("--nli", default=DEFAULT_NLI, help="nli_crossref.db (gallery; page assignment)")
    ap.add_argument("--libraries-csv", default=DEFAULT_LIBRARIES)
    ap.add_argument("--approvals", default=DEFAULT_APPROVALS)
    ap.add_argument("--exclusions", default=DEFAULT_EXCLUSIONS, help="owner-ruled exclusions by durable key (R2d)")
    ap.add_argument("--out-dir", default=DEFAULT_OUT)
    ap.add_argument("--new-rows-policy", choices=("hold", "plain"), default="plain")
    ap.add_argument("--htr-json", help="HTR pages per sys_id (tests); default: the search engine")
    ap.add_argument("--reference-limit", type=int, help="cap the reference set (smoke runs only)")
    ap.add_argument("--previous-summary", help="gate4_summary.json of the previous run (two-run check)")
    ap.add_argument("--allow-single-run", action="store_true")
    ap.add_argument("--expect", action="append", metavar="KEY=VALUE")
    ap.add_argument("--expect-file", metavar="PATH")
    args = ap.parse_args(argv)
    try:
        expectations = parse_expectations(args.expect, args.expect_file)
    except (SystemExit, OSError, ValueError) as ex:
        print(ex, file=sys.stderr)
        return 2
    previous = None
    if args.previous_summary:
        with open(args.previous_summary, encoding="utf-8") as fh:
            previous = json.load(fh)["meta"]["digest"]
    t0 = time.time()
    os.makedirs(args.out_dir, exist_ok=True)
    try:
        import psutil
        proc = psutil.Process()
    except ImportError:  # pragma: no cover
        proc = None
    meta = {"run_at": time.strftime("%Y-%m-%d %H:%M:%S"), "fgp_db": args.fgp_db, "gate2": args.gate2, "gate3": args.gate3,
            "inventory": args.inventory, "nli": args.nli, "approvals": args.approvals, "exclusions": args.exclusions,
            "new_rows_policy": args.new_rows_policy,
            "htr": args.htr_json or "search engine", "budgets": _j(BUDGETS)}

    log("loading today's sidecar, gates 2 and 3, the inventory ...")
    db_t = ro(args.fgp_db)
    today, cols_t = load_today(db_t)
    g2_conn = ro(args.gate2)
    g2, aliases = load_gate2(g2_conn)
    g2_conn.close()
    g3_conn = ro(args.gate3)
    g3_today, g3_new = load_gate3(g3_conn)
    meta["gate3_digest"] = (g3_conn.execute("SELECT value FROM gate3_meta WHERE key='digest'").fetchone() or [""])[0]
    inv = ro(args.inventory)
    b_numbers, units = load_inventory(inv)
    inv.close()
    libraries = load_libraries(args.libraries_csv)
    approvals = load_approvals(args.approvals)
    exclusions = load_exclusions(args.exclusions)
    fist = ro(args.fist)
    bibref_ids = {units[k][1] for k in units if units[k][0] == BIBREF_SOURCE}
    credit_ctx = {"parts": build_credit_parts(db_t), "catalog": build_catalog_map(fist), "book": build_book_map(fist),
                  "bibref": build_bibref_map(fist, bibref_ids)}
    comments = load_unit_comments(fist)
    superseded = load_superseded(fist)
    status_one = load_status_one(fist)
    fist.close()
    log(f"  {len(today):,} today rows, {len(g3_new):,} new rows read by gate 3, {len(libraries):,} manuscripts in libraries.csv "
        f"({time.time() - t0:.0f}s)")

    log("planning and writing the candidate sidecar ...")
    plan = plan_candidate(today, g2, aliases, g3_today, g3_new, g3_conn, b_numbers, units, libraries, credit_ctx,
                          args.new_rows_policy, comments=comments, exclusions=exclusions, superseded=superseded,
                          status_one=status_one)
    g3_conn.close()
    cand_path = os.path.join(args.out_dir, CANDIDATE_NAME)
    write_candidate(cand_path, args.fgp_db, cols_t, plan, meta)
    build_peak = 0.0
    if proc is not None:
        mi = proc.memory_info()
        build_peak = getattr(mi, "peak_wset", mi.rss) / 2**20
    log(f"  carried {len(plan['kept']):,} (replaced {sum(1 for k in plan['kept'] if k['replaced']):,}), excluded "
        f"{len(plan['excluded']):,}, new {len(plan['new_rows']):,}, held {len(plan['held']):,} ({time.time() - t0:.0f}s)")

    C = collections.Counter()
    C["rows.today"] = len(today)
    C["rows.carried"] = len(plan["kept"])
    C["rows.replaced"] = sum(1 for k in plan["kept"] if k["replaced"])
    C["rows.excluded"] = len(plan["excluded"])
    for _, reason, _ in plan["excluded"]:
        C[f"excluded.{reason}"] += 1
    C["rows.new"] = len(plan["new_rows"])
    C["rows.new_held"] = len(plan["held"])
    for _, reason, _ in plan["held"]:
        C[f"new_held.{reason}"] += 1
    for r in plan["new_rows"]:
        C[f"new.credit.{r['credit_category']}"] += 1
        C["new.per_image" if r["c_number"] else "new.whole_doc"] += 1
    C["rows.candidate"] = len(plan["kept"]) + len(plan["new_rows"])
    for y in plan["yardstick"].values():
        C[f"new.yardstick.{y}"] += 1
    C["exclusions.listed"] = len(exclusions)
    C["exclusions.unmatched"] = len(plan["exclusions_unmatched"])
    for k in plan["exclusions_unmatched"]:
        log(f"  listed exclusion names no row: {k}")

    db_c = ro(cand_path)
    svc_t, svc_c = fs.FgpService(db_path=args.fgp_db), fs.FgpService(db_path=cand_path)
    max_today_id = max(today) if today else 0
    excluded = {rid: (reason, detail) for rid, reason, detail in plan["excluded"]}

    # C8 identity and contract
    cand_cols = table_columns(db_c, "fgp_transcriptions")
    sel_cols = [c for c in ("id", "doc_relation", "language") if c in cand_cols]
    cand_keys = {r[0]: r for r in db_c.execute(f"SELECT {', '.join(sel_cols)} FROM fgp_transcriptions WHERE id <= ?", (max_today_id,))}
    mismatch = sum(1 for k in plan["kept"] if cand_keys.get(k["id"]) != tuple(today[k["id"]].get(c) for c in sel_cols))
    n_rows = db_c.execute("SELECT COUNT(*) FROM fgp_transcriptions").fetchone()[0]
    n_bad = db_c.execute("SELECT COUNT(*) FROM fgp_transcriptions WHERE sys_id IS NULL OR sys_id='' OR content IS NULL OR content=''").fetchone()[0]
    contract_ok = svc_c.is_available() and svc_c.get_version() == CANDIDATE_VERSION and n_rows == C["rows.candidate"] and n_bad == 0
    contract = {"identity_mismatch": mismatch, "ok": contract_ok,
                "detail": f"table {svc_c._table}, version {svc_c.get_version()}, rows {n_rows:,}, rows without sys_id or text {n_bad}"}
    C["contract.rows_without_sys_id_or_text"] = n_bad

    log("selectability over every manuscript ...")
    sys_ids = sorted({r["sys_id"] for r in today.values() if r["sys_id"]} | {r["sys_id"] for r in plan["new_rows"]})
    sel, Cs = check_selectability(svc_t, svc_c, db_t, db_c, sys_ids, excluded, aliases, max_today_id)
    C.update(Cs)
    C["selectability.manuscripts"] = len(sys_ids)
    log(f"  {len(sys_ids):,} manuscripts; lost: " + ", ".join(f"{k.split('.')[-1]} {v:,}" for k, v in sorted(Cs.items()) if ".lost." in k) +
        f"; new unlisted {Cs.get('selectability.new_unlisted', 0)} ({time.time() - t0:.0f}s)")

    log("page assignment ...")
    nli = ro(args.nli) if args.nli and os.path.exists(args.nli) else None
    page, Cp = check_page_assignment(db_t, db_c, cols_t, [k["id"] for k in plan["kept"]], plan["new_rows"], nli, approvals)
    C.update(Cp)
    if nli is not None:
        nli.close()

    log("default selection on the reference set ...")
    today_by_sid = collections.Counter(r["sys_id"] for r in today.values() if r["sys_id"])
    new_by_sid = collections.Counter(r["sys_id"] for r in plan["new_rows"])
    replaced_sids = {today[k["id"]]["sys_id"] for k in plan["kept"] if k["replaced"] and today[k["id"]]["sys_id"]}
    excluded_sids = {today[rid]["sys_id"] for rid, _, _ in plan["excluded"] if today[rid]["sys_id"]}
    swap_sids = {r[0] for r in sel if r[4] == "same_text_swap"}
    multi = collections.defaultdict(set)
    for k in plan["kept"]:
        t = today[k["id"]]
        if t.get("c_number"):
            multi[(t["sys_id"], t["c_number"], t.get("doc_relation"), t.get("language"))].add(k["unit"][1])
    multi_unit_sids = {key[0] for key, us in multi.items() if len(us) > 1}
    refs = reference_set(today_by_sid, new_by_sid, replaced_sids, excluded_sids, swap_sids, multi_unit_sids, args.reference_limit)
    nli_svc = None
    if args.nli and os.path.exists(args.nli):
        nli_svc = nli_mod.NliCrossrefService(db_path=args.nli)
        nli_mod._default_service = nli_svc  # the desktop-shape resolvers read the singleton
    htr = HtrSource(args.htr_json)
    decisions, ref_rows, lat, heaviest, Cd = check_defaults(svc_t, svc_c, db_t, db_c, nli_svc, htr, refs, max_today_id, excluded, approvals)
    C.update(Cd)
    C["reference.manuscripts"] = len(refs)
    latency = latency_rows(lat)
    log(f"  {len(refs)} manuscripts, {Cd.get('defaults.pages', 0):,} pages, changed: " +
        ", ".join(f"{k.split('.')[-1]} {v}" for k, v in sorted(Cd.items()) if k.startswith("defaults.changed.")) +
        f" ({time.time() - t0:.0f}s)")

    log("memory ...")
    memory = measure_memory(cand_path, svc_c, heaviest[0], build_peak)

    cand_rows = []
    for r in db_c.execute("SELECT id, origin, durable_key, sys_id, c_number, doc_relation, language, text_provenance, content, "
                          "source_credit_he FROM fgp_transcriptions ORDER BY id" if "source_credit_he" in cand_cols else
                          "SELECT id, origin, durable_key, sys_id, c_number, doc_relation, language, text_provenance, content, "
                          "NULL FROM fgp_transcriptions ORDER BY id"):
        rk = next((n["row_key"] for n in plan["new_rows"] if n["id"] == r[0]), f"today:{r[0]}") if r[1] == "new" else f"today:{r[0]}"
        cat = next((n["credit_category"] for n in plan["new_rows"] if n["id"] == r[0]), None) if r[1] == "new" else None
        cand_rows.append((r[0], r[1], rk, r[2], r[3], r[4], r[5], r[6], r[7], sha(r[8]), len(letters(r[8])), r[9], cat))
    h = hashlib.sha256()
    for row in cand_rows:
        h.update(_j([row[0], row[1], row[4], row[5], row[9], row[11], row[8]]).encode("utf-8"))
    for row in sorted(sel):
        h.update(_j(row).encode("utf-8"))
    for row in decisions:
        h.update(_j(row[:8]).encode("utf-8"))
    for row in plan["excluded"] + plan["held"]:
        h.update(_j(row).encode("utf-8"))
    digest = h.hexdigest()
    meta["digest"] = digest
    meta["candidate"] = cand_path
    meta["elapsed_s"] = round(time.time() - t0, 1)

    checks = run_checks(C, latency, memory, contract, digest, previous, args.allow_single_run, expectations)
    db_path = write_outputs(args.out_dir, meta, plan, cand_rows, sel, page, decisions, ref_rows, latency, memory, checks, C)
    db_t.close()
    db_c.close()
    svc_t.close()
    svc_c.close()
    if nli_svc is not None:
        nli_svc.close()
        nli_mod._default_service = None
    failed = [c for c in checks if not c["passed"]]
    for c in checks:
        log(f"  [{'ok  ' if c['passed'] else 'FAIL'}] {c['name']}: expected {c['expected']}, got {c['actual']}")
    log(f"wrote {db_path} and {cand_path} ({time.time() - t0:.0f}s); digest {digest[:16]}")
    log(f"RESULT: {'FAIL' if failed else 'PASS'} ({len(checks) - len(failed)}/{len(checks)})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
