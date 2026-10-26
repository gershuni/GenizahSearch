#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Gate 2 of the FGP source-folders plan: identity, credit, visibility and refresh rules.

WHY THIS EXISTS (docs/plans/FGP_SOURCE_FOLDERS_ASSESSMENT.md, section 8, gate 2)
-------------------------------------------------------------------------------
Every one of today's ``fgp_transcriptions`` rows is public: its integer id is the
``fgp:<id>`` the web chooser saves as a reader's selection and the MCP detail section
lists. Before the sidecar is rebuilt from the FIST file store, each row needs an explicit
outcome -- kept, re-sourced or retired-with-alias -- under rules that never let a public
id change meaning, never publish a row whose credit is unresolved, whose manuscript
association is ambiguous, or whose FIST visibility flag is set, and say how a later
delivery refreshes the rows. The rules are written out in
docs/plans/FGP_IDENTITY_CREDIT_VISIBILITY_RULES.md; this script applies them to the gate-1
inventory and today's sidecar and fails when any row escapes them.

THE RULES, IN SHORT
-------------------
Identity. The meaning of ``fgp:<id>`` is the tuple (kind, language, sys_id, c_number) read
from the row plus the FIST source record the row's file belongs to. The durable key of a
row is ``<unit_table>:<unit_id>|<sys_id>|<c_number>``; the FIST unit is found by the row's
file basename and chosen among the file's units by the one whose inventory chain reaches
the row's sys_id. A row is *kept* when exactly that holds; *re-sourced* when its identity
holds but its credit or its text must come from a different source than today (credit from
FIST instead of the file-name prefix, XML text from the record's own position folder);
*retired-with-alias* when its meaning cannot stand: an exact duplicate of another row
(successor = the lowest id, same meaning), a sys_id the FIST chain contradicts (no
successor; the pipeline creates the correct row under a new id), or a row no reader can
reach (sys_id NULL). Ids are never reused; a retired id resolves through ``aliases``.

Credit. By the unit's FIST SourceId/SubId: a verified team-head credit (with the
manuscript's DataSource part for that team when present), the Genuzos / Herzog
infrastructure credit, the catalogue (CODE_Catalog) or the book (CODE_Title); for the
sources in ``DATASOURCE_PART_TOKENS`` the single DataSource part that names the
contributor; anything else is *unresolved* (a DataSource aggregate is recorded as
``datasource_only`` but counts as unresolved until the owner rules). FIST copyright text on the unit is carried verbatim
beside the credit as an acknowledgment, never as the credit. The generic "Friedberg
Genizah Project" attribution is reported as a second, looser publishable count, never
used by the strict flag.

Visibility. Publishable (strict) = transcription or translation, not retired, association
basis ``fist_chain`` (or the two-registration case), no ``tmp_DoubleNotToDisplay``, credit
resolved, and XML text verified to come from the row's own record. Descriptions and
unmatched files never reach the chooser.

Refresh. Match a new delivery by FIST unit id, then by FileName; a text change is a new
revision of the same id; a unit whose chain no longer reaches the row's sys_id retires
the row with an alias; a unit gone from FIST retires it without one; new units get new ids.

OUTPUTS
-------
``<out-dir>/gate2.db`` (``row_outcomes``, ``aliases``, ``credit_rulings``, ``checks``,
``counts``), ``gate2_summary.json``, ``GATE2_REPORT.md``. Read-only on every input.

USAGE
-----
    powershell -File scripts/run_fgp_gate2_rules.ps1
    python scripts/fgp_gate2_rules.py --expect-file scripts/fgp_gate2_rules.expect.json

Exit codes: 0 every check passed, 1 a check failed (fail-closed), 2 usage.
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import re
import sqlite3
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from scripts.fgp_association_inventory import (  # noqa: E402
    B_REGION_RE, DEFAULT_FGP_DB, DEFAULT_FIST, DEFAULT_OUT, parse_expectations, ro,
    today_basename, today_kind,
)
from scripts.fgp_fill_credits_bilingual import (  # noqa: E402
    INFRA_CREDITS, TEAM_CREDITS, build_book_map, build_catalog_map, build_credit_parts,
    resolve_team_credit,
)

DEFAULT_INVENTORY = os.path.join(DEFAULT_OUT, "inventory.db")
GENERIC_CREDIT = ("פרויקט הגניזה של פרידברג", "Friedberg Genizah Project")

OUTCOMES = ("kept", "re-sourced", "retired-with-alias")
CREDIT_RESOLVED = ("team", "team_individual", "infra", "catalog", "book", "datasource_part", "bibliographic_reference",
                   "site_user", "personal_list")

# FIST source 850: the unit's SubId is a site-user id that this FIST copy does not carry. The names
# were read off the live FGP site (the transcription's "Source: Site User - ..." line) on 2026-10-25,
# one shelfmark per id, by the owner's Chrome session. Hebrew and English as FJMS shows them.
SITE_USER_IDS = {
    221: ("משתמש באתר - Prof. Robert Brody", "Site user - Prof. Robert Brody"),        # T-S 10F1
    493: ("משתמש באתר - David Oren", "Site user - David Oren"),                        # T-S AS 119.57
    187: ("משתמש באתר - Chaya Ber", "Site user - Chaya Ber"),                          # ENA 149.4
    7: ("משתמש באתר - מרים גרינשטיין", "Site user - Miriam Greenstein"),               # ENA NS 3.27
    346: ("משתמש באתר - שלמה אילן", "Site user - Shlomo Ilan"),                        # Or.1080 1.57
    80: ("משתמש באתר - Rabbi Tuvia Katzman", "Site user - Rabbi Tuvia Katzman"),      # T-S G1.82
    34: ("משתמש באתר - ד\"ר הלל גרשוני", "Site user - Dr. Hillel Gershuni"),            # T-S 12.863
    645: ("משתמש באתר - daniel ventura", "Site user - daniel ventura"),                # T-S AS 69.186
}
# FIST source 300 (רשימות אישיות): the SubId is a personal-list id. 51 is Ben-Sasson's (36 rows carry
# it in their DataSource); 53 was read off the live site (T-S AS 74.11: "Chaim Milikowsky, Personal Handlist").
PERSONAL_LIST_IDS = {
    51: ("בן ששון מנחם, רשימות אישיות", "Menahem Ben-Sasson, personal handlist"),
    53: ("חיים מיליקובסקי, רשימות אישיות", "Chaim Milikowsky, personal handlist"),
}
CREDIT_UNRESOLVED = ("datasource_only", "unresolved")

# FIST sources with no verified team credit whose contributor FJMS's per-manuscript DataSource
# names in a recognisable part. The part is taken only when exactly one matches (14 manuscripts
# list several site users). Confirmed on the live FGP site by the owner, 2026-10-25: BL OR
# 10578S.16 (source 100 -> the named site user) and AIU III.B.109 (161 -> Friedberg's line).
DATASOURCE_PART_TOKENS = {
    100: "משתמש באתר",                        # a site user (CODE_Source 100 = צוות אורחים)
    850: "משתמש באתר",
    700: "משתמש באתר",
    300: "רשימות אישיות",                     # a personal list (CODE_Source 300); Ben-Sasson's in practice
}
# 161 (Emanuel Friedberg's team) is a verified TEAM_CREDITS entry since 2026-10-25.

# FIST source 601 = הפניות ביבליוגרפיות: the unit's SubId is a dbo_UnitBibliographyReference id,
# which FJMS renders as "Bibliographical Reference - <author>, <journal> <date>, עמוד <page>"
# (owner's screenshot of Or.1080 6.17, 2026-10-25). Built here from the same FIST rows.
BIBREF_SOURCE = 601

UNIT_TABLES = {
    "transcription": ("dbo_UnitTranscription", "UnitTranscriptionId"),
    "translation": ("dbo_UnitTranslation", "UnitTranslationId"),
    "description": ("dbo_UnitFreeDescription", "UnitFreeDescriptionId"),
}


def log(msg: str) -> None:
    print(msg, flush=True)


def _j(v) -> str:
    return json.dumps(sorted(v) if isinstance(v, set) else v, ensure_ascii=False)


# --------------------------------------------------------------------------- inputs


def load_inventory(conn: sqlite3.Connection) -> dict:
    files = {}
    for r in conn.execute("SELECT basename, kind, multi_class, status, double_not_display FROM files"):
        files[r[0]] = {"kind": r[1], "multi_class": r[2], "status": r[3], "held": r[4]}
    units = collections.defaultdict(list)
    itf_owner = collections.defaultdict(list)
    for r in conn.execute(
        "SELECT basename, unit_table, unit_id, kind, signature_id, set_signature_id, source_id, sub_id, "
        "image_transcription_folder, double_not_display, heb_copyright, eng_copyright FROM file_units"
    ):
        u = {"basename": r[0], "table": r[1], "unit_id": r[2], "kind": r[3], "sig": r[4], "set": r[5],
             "source": r[6], "sub": r[7], "itf": r[8], "held": r[9] == 1, "heb_cr": bool(r[10]), "eng_cr": bool(r[11])}
        units[r[0]].append(u)
        if u["itf"]:
            itf_owner[u["itf"]].append(u)
    almas = collections.defaultdict(lambda: collections.defaultdict(set))
    for b, sig, _st, _inv, alma, _n, _t in conn.execute("SELECT * FROM file_almas"):
        almas[b][sig].add(alma)
    db_only = {}
    for rid, reason in conn.execute("SELECT id, db_only_reason FROM today_rows WHERE in_nopos = 0"):
        db_only[rid] = reason
    return {"files": files, "units": units, "almas": almas, "itf_owner": itf_owner, "db_only": db_only}


def fist_units_for_absent_file(fist: sqlite3.Connection, basename: str) -> tuple:
    """Units + chain for a FIST FileName whose file is not in the delivery (the 19 sidecar-only rows)."""
    units, almas = [], collections.defaultdict(set)
    for kind, (table, id_col) in UNIT_TABLES.items():
        extra = ", tmp_DoubleNotToDisplay, HebCopyRight, EngCopyRight" if kind != "description" else ", NULL, NULL, NULL"
        itf = ", ImageTranscriptionFolder" if kind == "transcription" else ", NULL"
        for uid, sid, dnd, heb, eng, itf_v in fist.execute(
            f"SELECT {id_col}, SignatureId{extra}{itf} FROM {table} WHERE FileName = ?", (basename,)
        ):
            s = fist.execute("SELECT SetSignatureId, SourceId, SubId FROM dbo_Signature WHERE SignatureId = ?", (sid,)).fetchone()
            units.append({"basename": basename, "table": table, "unit_id": uid, "kind": kind, "sig": sid,
                          "set": s[0] if s else None, "source": s[1] if s else None, "sub": s[2] if s else None,
                          "itf": itf_v, "held": dnd == 1, "heb_cr": bool(heb), "eng_cr": bool(eng)})
            if s and s[0] is not None:
                for (inv_id,) in fist.execute("SELECT InventoryId FROM dbo_InventorySignature WHERE SetSignatureId = ?", (s[0],)):
                    for (alma,) in fist.execute("SELECT AlmaId FROM dbo_InventoryAlma WHERE InventoryId = ? AND AlmaId IS NOT NULL", (inv_id,)):
                        almas[sid].add(str(alma))
    return units, almas


def build_bibref_map(fist: sqlite3.Connection, ref_ids) -> dict:
    """{UnitBibliographyReferenceId: (he, en)} for FIST source 601 units; {} when the tables are absent."""
    ref_ids = sorted({int(i) for i in ref_ids if i is not None})
    if not ref_ids:
        return {}
    tables = {r[0] for r in fist.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"dbo_UnitBibliographyReference", "CODE_Title"} <= tables:
        return {}
    title_cols = [c for c in ("RunningTitleHeb", "FullTitleHeb", "RunningTitleEng")
                  if c in {r[1] for r in fist.execute("PRAGMA table_info(CODE_Title)")}]
    out = {}
    for ref_id in ref_ids:
        r = fist.execute(
            "SELECT TitleId, JournalDate, MentionPage, FromPage, ToPage FROM dbo_UnitBibliographyReference "
            "WHERE UnitBibliographyReferenceId = ?", (ref_id,)
        ).fetchone()
        if not r:
            continue
        title_id, date, page, from_p, to_p = r
        t = fist.execute(f"SELECT {', '.join(title_cols)} FROM CODE_Title WHERE TitleId = ?",
                         (title_id,)).fetchone() if title_id is not None and title_cols else None
        journal = next((str(v).strip() for v in (t or ()) if v and str(v).strip()), "")
        authors = []
        if {"dbo_BibMultiArticleAuthor", "CODE_Author"} <= tables:
            authors = [a[0] for a in fist.execute(
                "SELECT COALESCE(b.ArticleAuthorDisplay, ca.HebDesc, ca.EngDesc) FROM dbo_BibMultiArticleAuthor b "
                "LEFT JOIN CODE_Author ca ON ca.AuthorId = b.ArticleAuthorId "
                "WHERE b.UnitBibliographyReferenceId = ? ORDER BY b.AuthorOrder", (ref_id,)) if a[0]]
        pages = (page or "").strip() or (f"{from_p}-{to_p}" if from_p and to_p else "")
        body = ", ".join(x for x in ("; ".join(authors), " ".join(x for x in (journal, (date or "").strip()) if x)) if x)
        if not body:
            continue
        if pages:
            body = f"{body}, עמוד {pages}"
        out[ref_id] = (f"הפניה ביבליוגרפית – {body}", f"Bibliographical reference – {body}")
    return out


# A FIST unit whose free-text Comment says it must not be published until read against the source
# ("אין לפרסם עד לקריאה מול מקור", "לא לפרסם") is held. A comment that only says the transcription was
# not checked against the source ("לא נבדק מול המקור", usually with a writing stage) is a quality note
# on a finished transcription FGP itself shows, not an instruction: the owner first held both
# (2026-10-26, after the drafts check) and narrowed the hold to the explicit instruction the same day.
NO_PUBLISH_COMMENT_RE = re.compile(r"אין לפרסם|לא לפרסם")


def load_unit_comments(fist: sqlite3.Connection) -> dict:
    """{(unit_table, unit_id) -> Comment} for every unit with a non-blank Comment; {} on a FIST without the column."""
    out = {}
    for table, id_col in (("dbo_UnitTranscription", "UnitTranscriptionId"), ("dbo_UnitTranslation", "UnitTranslationId")):
        try:
            rows = fist.execute(f"SELECT {id_col}, Comment FROM {table} WHERE Comment IS NOT NULL AND TRIM(Comment) != ''").fetchall()
        except sqlite3.OperationalError:
            continue
        for uid, comment in rows:
            out[(table, uid)] = comment.strip()
    return out


def load_copyright(fist: sqlite3.Connection) -> dict:
    out = {}
    for table, id_col in (("dbo_UnitTranscription", "UnitTranscriptionId"), ("dbo_UnitTranslation", "UnitTranslationId")):
        for uid, heb, eng in fist.execute(
            f"SELECT {id_col}, HebCopyRight, EngCopyRight FROM {table} WHERE HebCopyRight IS NOT NULL OR EngCopyRight IS NOT NULL"
        ):
            out[(table, uid)] = ((heb or "").strip() or None, (eng or "").strip() or None)
    return out


def load_today(conn: sqlite3.Connection) -> list:
    cols = ("id", "filename", "collection", "shelfmark", "c_number", "image_id", "doc_relation", "language",
            "sys_id", "mms_id", "sysid_method", "text_source", "source_credit_he", "source_credit_en",
            "content_length", "rel_path")
    rows = [dict(zip(cols, r)) for r in conn.execute(f"SELECT {', '.join(cols)} FROM fgp_transcriptions ORDER BY id")]
    return rows


def content_hashes(conn: sqlite3.Connection, ids: list) -> dict:
    out = {}
    for rid in ids:
        c = conn.execute("SELECT content FROM fgp_transcriptions WHERE id = ?", (rid,)).fetchone()
        out[rid] = hashlib.sha256((c[0] or "").encode("utf-8")).hexdigest() if c else None
    return out


# --------------------------------------------------------------------------- credit


def credit_for_unit(unit: dict, mms_id, credit_parts: dict, catalog_map: dict, book_map: dict,
                    bibref_map: dict | None = None) -> tuple:
    """(he, en, category) from the unit's FIST source, mirroring fgp_fill_credits_bilingual but keyed on FIST."""
    so, su = unit["source"], unit["sub"]
    parts = credit_parts.get(str(mms_id), []) if mms_id is not None else []
    if so == BIBREF_SOURCE:
        ref = (bibref_map or {}).get(su)
        return (ref[0], ref[1], "bibliographic_reference") if ref else (None, None, "unresolved")
    if so == 500:
        s = catalog_map.get(su)
        return (s, s, "catalog") if s else (None, None, "unresolved")
    if so == 600:
        s = book_map.get(su)
        return (s, s, "book") if s else (None, None, "unresolved")
    if so in TEAM_CREDITS:
        he, en, cat = resolve_team_credit(so, parts)
        return he, en, ("team_individual" if cat.endswith("matched") else "team")
    if so in INFRA_CREDITS:
        he, en = INFRA_CREDITS[so]
        return he, en, "infra"
    token = DATASOURCE_PART_TOKENS.get(so)
    if token:
        matched = [(e, h) for (e, h) in parts if h and token in h]
        if len(matched) == 1:
            e, h = matched[0]
            return h, (e or h), "datasource_part"
    if so == 850 and su in SITE_USER_IDS:
        he, en = SITE_USER_IDS[su]
        return he, en, "site_user"
    if so == 300 and su in PERSONAL_LIST_IDS:
        he, en = PERSONAL_LIST_IDS[su]
        return he, en, "personal_list"
    if parts:
        he = "; ".join(dict.fromkeys(h for (_e, h) in parts if h)) or None
        en = "; ".join(dict.fromkeys(e for (e, _h) in parts if e)) or None
        return he, (en or he), "datasource_only"
    return None, None, "unresolved"


# --------------------------------------------------------------------------- outcomes


def xml_text_status(row: dict, unit: dict, inv: dict) -> tuple:
    """How today's XML text relates to the row's own FIST record: (status, owner relation)."""
    if row["text_source"] != "xml":
        return "ok", None
    num = (row["c_number"] or "").lstrip("C")
    itf = unit.get("itf") or ""
    if itf == num:
        return "ok", "own_folder"
    if itf and itf.lstrip("C") == num and itf.startswith("C"):
        return "xml_other_edition", "own_record_is_B2"
    if itf and B_REGION_RE.sub("", itf) == num:
        return "xml_region", "own_record_is_region"
    owners = inv["itf_owner"].get(num, [])
    if not owners:
        return "xml_unverified", "no_fist_owner"
    same_src = any(o["source"] == unit["source"] for o in owners)
    same_alma = any(row["sys_id"] and row["sys_id"] in inv["almas"][o["basename"]].get(o["sig"], set()) for o in owners)
    if same_src and same_alma:
        return "xml_unverified", "owner_same_source_same_manuscript"
    if same_src:
        return "xml_unverified", "owner_same_source_other_manuscript"
    return "xml_other_edition", "owner_other_source"


def build_outcomes(rows: list, inv: dict, fist: sqlite3.Connection, credit_parts: dict,
                   catalog_map: dict, book_map: dict, copyright: dict, hashes: dict,
                   credit_policy: str = "generic", bibref_map: dict | None = None, comments: dict | None = None) -> tuple:
    C = collections.Counter()
    bibref_map = bibref_map or {}
    comments = comments or {}
    outcomes, aliases = [], []
    rulings = collections.defaultdict(lambda: {"rows": 0, "today_credits": collections.Counter()})
    # exact duplicates: same (filename, sys_id, c_number) -> keep the lowest id. Rows without a
    # sys_id are unreachable and retire on their own; they never serve as a successor.
    groups = collections.defaultdict(list)
    for r in rows:
        if r["sys_id"]:
            groups[(r["filename"], r["sys_id"], r["c_number"])].append(r["id"])
    dup_successor = {}
    for ids in groups.values():
        if len(ids) > 1:
            keep = min(ids)
            for rid in ids:
                if rid != keep:
                    dup_successor[rid] = keep
    absent_cache = {}
    for r in rows:
        rid = r["id"]
        base = today_basename(r["filename"])
        kind = today_kind(r["filename"])
        sid = r["sys_id"] or None
        in_export = base in inv["files"]
        if in_export:
            units = inv["units"].get(base, [])
            almas = inv["almas"].get(base, {})
        else:
            if base not in absent_cache:
                absent_cache[base] = fist_units_for_absent_file(fist, base)
            units, almas = absent_cache[base]
        units = [u for u in units if u["kind"] == kind] or units
        hits = [u for u in units if sid and sid in almas.get(u["sig"], set())]
        hit_sigs = {u["sig"] for u in hits}
        file_almas = {a for s in almas.values() for a in s}
        out = {
            "id": rid, "basename": base, "filename": r["filename"], "kind": kind, "language": r["language"],
            "doc_relation": r["doc_relation"], "sys_id": sid, "sysid_method": r["sysid_method"],
            "c_number": r["c_number"], "text_source": r["text_source"], "in_export": int(in_export),
            "unit_table": None, "unit_id": None, "signature_id": None, "set_signature_id": None,
            "source_id": None, "sub_id": None, "unit_kind": None, "multi_class": inv["files"].get(base, {}).get("multi_class"),
            "association_basis": None, "outcome": None, "outcome_reason": None, "successor_id": None,
            "credit_category": None, "credit_status": None, "credit_he": None, "credit_en": None,
            "copyright_he": None, "copyright_en": None, "credit_changed": None,
            "text_status": None, "xml_owner_relation": None, "visibility_hold": 0, "fist_comment": None,
            "publishable_strict": 0, "publishable_generic": 0, "publishable": 0, "hold_reasons": None,
            "durable_key": None,
        }
        reasons = []
        unit = None
        if not units:
            basis = "none"
        elif sid is None:
            basis = "none"
        elif len(hit_sigs) == 1:
            unit = min(hits, key=lambda u: u["unit_id"])
            basis = "fist_chain"
        elif len(hit_sigs) > 1:
            srcs = {(u["source"], u["sub"]) for u in hits}
            unit = min(hits, key=lambda u: u["unit_id"])
            basis = "fist_chain_multi_registration" if len(srcs) == 1 else "ambiguous"
        elif file_almas:
            basis = "conflict"
        else:
            basis = "unconfirmed"
        if unit is None and units and basis in ("conflict", "unconfirmed", "none"):
            unit = min(units, key=lambda u: u["unit_id"])  # the record the file belongs to, for credit / durable key
        out["association_basis"] = basis
        if unit:
            out.update({"unit_table": unit["table"], "unit_id": unit["unit_id"], "signature_id": unit["sig"],
                        "set_signature_id": unit["set"], "source_id": unit["source"], "sub_id": unit["sub"],
                        "unit_kind": unit["kind"]})
            out["durable_key"] = f"{unit['table']}:{unit['unit_id']}|{sid or ''}|{r['c_number'] or ''}"
        # ---- outcome
        if rid in dup_successor:
            outcome, reason, succ = "retired-with-alias", "exact_duplicate_row", dup_successor[rid]
            if hashes.get(rid) != hashes.get(succ):
                reason = "exact_duplicate_row_content_differs"
        elif sid is None:
            outcome, reason, succ = "retired-with-alias", "no_sys_id_unreachable", None
        elif basis == "conflict":
            outcome, reason, succ = "retired-with-alias", "sys_id_not_in_fist_chain", None
        elif basis == "ambiguous":
            outcome, reason, succ = "kept", "association_ambiguous", None
        elif basis == "none":
            outcome, reason, succ = "kept", "no_fist_unit_of_this_kind", None
        else:
            outcome, reason, succ = "kept", "", None
        # ---- credit (from the unit; today's for comparison)
        if unit:
            he, en, cat = credit_for_unit(unit, r["mms_id"], credit_parts, catalog_map, book_map, bibref_map)
            cr = copyright.get((unit["table"], unit["unit_id"]))
            if cr:
                # An acknowledgment or permission line is shown beside the credit, never as the credit.
                out["copyright_he"], out["copyright_en"] = cr
            out.update({"credit_category": cat, "credit_he": he, "credit_en": en})
            out["credit_status"] = "resolved" if cat in CREDIT_RESOLVED else "unresolved"
            # "changed" is meaningful only for a resolved credit: an unresolved one replaces nothing.
            out["credit_changed"] = int(cat in CREDIT_RESOLVED
                                        and (he or None, en or None) != (r["source_credit_he"] or None, r["source_credit_en"] or None))
            if cat in CREDIT_UNRESOLVED:
                key = f"source {unit['source']}" + (f" sub {unit['sub']}" if unit["source"] in (500, 600) else "")
                rulings[key]["rows"] += 1
                rulings[key]["today_credits"][(r["source_credit_he"] or "(none)")[:60]] += 1
        else:
            out.update({"credit_category": "unresolved", "credit_status": "unresolved", "credit_changed": None})
        # ---- text
        if unit and in_export:
            ts, rel = xml_text_status(r, unit, inv)
        elif unit:
            ts, rel = "file_absent_in_delivery", None
        else:
            ts, rel = "no_record", None
        out["text_status"], out["xml_owner_relation"] = ts, rel
        # ---- visibility
        if unit and unit["held"]:
            out["visibility_hold"] = 1
            reasons.append("tmp_DoubleNotToDisplay")
        if unit:
            out["fist_comment"] = comments.get((unit["table"], unit["unit_id"]))
            if out["fist_comment"] and NO_PUBLISH_COMMENT_RE.search(out["fist_comment"]) and outcome != "retired-with-alias":
                reasons.append("fist_comment_no_publish")
        if outcome == "retired-with-alias":
            reasons.append("retired")
        if basis not in ("fist_chain", "fist_chain_multi_registration") and outcome != "retired-with-alias":
            reasons.append(f"association_{basis}")
        if ts != "ok" and outcome != "retired-with-alias":
            reasons.append(ts)
        credit_hold = out["credit_status"] != "resolved"
        strict = not reasons and not credit_hold
        generic = not reasons
        if credit_hold and outcome != "retired-with-alias":
            if credit_policy == "generic":
                # The owner's ruling (2026-10-25): publish under the generic attribution; the
                # unresolved category and today's DataSource text stay recorded for refinement.
                out["credit_he"], out["credit_en"] = GENERIC_CREDIT
                out["credit_status"] = "generic"
            else:
                reasons.append("credit_" + (out["credit_category"] or "unresolved"))
        # re-sourced: identity kept, but credit or text comes from elsewhere than today
        if outcome == "kept" and ((out["credit_changed"] or 0) == 1 or ts.startswith("xml_")):
            outcome = "re-sourced"
            reason = ";".join(x for x in (reason, "credit_from_fist" if out["credit_changed"] else "", ts if ts.startswith("xml_") else "") if x)
        out.update({"outcome": outcome, "outcome_reason": reason or None, "successor_id": succ,
                    "publishable_strict": int(strict), "publishable_generic": int(generic),
                    "publishable": int(generic if credit_policy == "generic" else strict),
                    "hold_reasons": _j(reasons) if reasons else None})
        if outcome == "retired-with-alias":
            aliases.append({"old_id": rid, "successor_id": succ, "reason": reason,
                            "same_meaning": int(succ is not None), "retired_on": time.strftime("%Y-%m-%d")})
        outcomes.append(out)
        # ---- counts
        C[f"outcome.{outcome}"] += 1
        C[f"basis.{basis}"] += 1
        C[f"credit.{out['credit_category']}"] += 1
        C[f"text.{ts}"] += 1
        if rel:
            C[f"xml_owner.{rel}"] += 1
        for x in reasons:
            C[f"hold.{x}"] += 1
    C["rows"] = len(rows)
    C["rows_in_export"] = sum(o["in_export"] for o in outcomes)
    C["rows_db_only"] = len(rows) - C["rows_in_export"]
    C["retired.exact_duplicate"] = sum(1 for a in aliases if a["reason"].startswith("exact_duplicate"))
    C["retired.no_sys_id"] = sum(1 for a in aliases if a["reason"] == "no_sys_id_unreachable")
    C["retired.sys_id_conflict"] = sum(1 for a in aliases if a["reason"] == "sys_id_not_in_fist_chain")
    C["retired.with_successor"] = sum(1 for a in aliases if a["successor_id"] is not None)
    C["credit.changed_rows"] = sum(1 for o in outcomes if o["credit_changed"] == 1)
    C["credit.resolved_rows"] = sum(1 for o in outcomes if o["credit_status"] == "resolved")
    C["credit.generic_rows"] = sum(1 for o in outcomes if o["credit_status"] == "generic")
    C["credit.unresolved_rows"] = sum(1 for o in outcomes if o["credit_status"] != "resolved")
    C["credit.copyright_rows"] = sum(1 for o in outcomes if o["copyright_he"] or o["copyright_en"])
    C["visibility.held_rows"] = sum(o["visibility_hold"] for o in outcomes)
    C["publishable.strict"] = sum(o["publishable_strict"] for o in outcomes)
    C["publishable.generic_credit_allowed"] = sum(o["publishable_generic"] for o in outcomes)
    C["publishable.under_policy"] = sum(o["publishable"] for o in outcomes)
    C["publishable.not_under_policy"] = len(rows) - C["publishable.under_policy"]
    C["publishable.not_strict"] = len(rows) - C["publishable.strict"]
    C["durable_keys_distinct"] = len({o["durable_key"] for o in outcomes if o["durable_key"]})
    C["kind_disagrees_with_unit"] = sum(1 for o in outcomes if o["unit_kind"] and o["unit_kind"] != o["kind"])
    ruling_rows = []
    for key in sorted(rulings, key=lambda k: -rulings[k]["rows"]):
        ruling_rows.append({"source": key, "rows": rulings[key]["rows"],
                            "today_credits": _j(rulings[key]["today_credits"].most_common(4))})
    return outcomes, aliases, ruling_rows, C


# --------------------------------------------------------------------------- checks


def run_checks(outcomes: list, aliases: list, C: dict, expectations: dict) -> list:
    checks = []

    def add(name, passed, expected, actual, note):
        checks.append({"name": name, "passed": int(bool(passed)), "expected": str(expected), "actual": str(actual), "note": note})

    by_id = {o["id"]: o for o in outcomes}
    n = sum(C.get(f"outcome.{k}", 0) for k in OUTCOMES)
    bad = sum(1 for o in outcomes if o["outcome"] not in OUTCOMES)
    add("G01_every_row_has_one_outcome", n == C["rows"] and bad == 0, f"{C['rows']} rows, 0 other", f"{n} rows, {bad} other",
        "kept + re-sourced + retired-with-alias == rows; no other outcome")
    viol = sum(1 for o in outcomes if o["outcome"] != "retired-with-alias"
               and o["association_basis"] not in ("fist_chain", "fist_chain_multi_registration") and o["publishable_strict"])
    kind_bad = C["kind_disagrees_with_unit"]
    add("G02_meaning_preserved", viol == 0 and kind_bad == 0, "0 and 0", f"{viol} and {kind_bad}",
        "a kept / re-sourced row is publishable only when its FIST unit's chain reaches its sys_id; the unit's kind equals the row's")
    bad_alias = 0
    for a in aliases:
        if not a["reason"]:
            bad_alias += 1
        s = a["successor_id"]
        if s is not None:
            t, o = by_id.get(s), by_id[a["old_id"]]
            if t is None or t["outcome"] == "retired-with-alias" or (t["kind"], t["language"], t["sys_id"], t["c_number"]) != (o["kind"], o["language"], o["sys_id"], o["c_number"]):
                bad_alias += 1
    add("G03_aliases_keep_meaning", bad_alias == 0 and len(aliases) == C["outcome.retired-with-alias"], f"0 bad, {C['outcome.retired-with-alias']} aliases",
        f"{bad_alias} bad, {len(aliases)} aliases", "every retired row has a reason; a successor exists, is not retired, and has the same meaning")
    viol = sum(1 for o in outcomes if o["publishable"] and (
        o["credit_status"] not in ("resolved", "generic") or o["visibility_hold"] or o["outcome"] == "retired-with-alias"
        or o["association_basis"] not in ("fist_chain", "fist_chain_multi_registration") or o["text_status"] != "ok"))
    viol_strict = sum(1 for o in outcomes if o["publishable_strict"] and o["credit_status"] != "resolved")
    add("G04_no_publishable_row_with_a_hold", viol == 0 and viol_strict == 0, "0 and 0", f"{viol} and {viol_strict}",
        "no publishable row has an unresolved credit (generic only under the ruled policy), a visibility flag, "
        "an ambiguous association, unverified text, or is retired; no strictly publishable row has a generic credit")
    viol = sum(1 for o in outcomes if o["publishable"] and not (o["credit_he"] and o["credit_en"]))
    add("G05_publishable_rows_have_bilingual_credit", viol == 0, 0, viol, "every publishable row carries a Hebrew and an English credit")
    dk = collections.Counter(o["durable_key"] for o in outcomes if o["outcome"] != "retired-with-alias" and o["durable_key"])
    dup_keys = sum(1 for v in dk.values() if v > 1)
    add("G06_durable_keys_unique_among_live_rows", dup_keys == 0, 0, dup_keys,
        "no two live rows share (unit, sys_id, c_number); exact duplicates are retired")
    add("G07_every_unpublishable_row_has_a_reason", all(o["hold_reasons"] for o in outcomes if not o["publishable"]),
        C["publishable.not_under_policy"], sum(1 for o in outcomes if not o["publishable"] and o["hold_reasons"]),
        "every row that is not publishable under the policy names at least one hold reason")
    for key, want in sorted(expectations.items()):
        have = C.get(key)
        add(f"E_{key}", have == want, want, have, "declared expectation (--expect / --expect-file)")
    return checks


# --------------------------------------------------------------------------- outputs


def _create(conn, table, rows, pk=None):
    if not rows:
        conn.execute(f'CREATE TABLE "{table}" (empty INTEGER)')
        return
    cols = list(rows[0].keys())
    defs = ", ".join(f'"{c}"' + (" PRIMARY KEY" if c == pk else "") for c in cols)
    conn.execute(f'CREATE TABLE "{table}" ({defs})')
    conn.executemany(f'INSERT INTO "{table}" VALUES ({",".join("?" * len(cols))})', ([r[c] for c in cols] for r in rows))


def write_outputs(outcomes, aliases, rulings, checks, C, out_dir, meta) -> str:
    os.makedirs(out_dir, exist_ok=True)
    db_path = os.path.join(out_dir, "gate2.db")
    if os.path.exists(db_path):
        try:
            old = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            ours = old.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='gate2_meta'").fetchone()[0] == 1
            old.close()
        except sqlite3.DatabaseError:
            ours = False
        if not ours:
            raise SystemExit(f"refusing to overwrite {db_path}: not a gate-2 database this script wrote")
        os.remove(db_path)
    tmp = db_path + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)
    conn = sqlite3.connect(tmp)
    with conn:
        _create(conn, "gate2_meta", [{"key": k, "value": str(v)} for k, v in meta.items()], pk="key")
        _create(conn, "row_outcomes", outcomes, pk="id")
        _create(conn, "aliases", aliases, pk="old_id")
        _create(conn, "credit_rulings", rulings, pk="source")
        _create(conn, "checks", checks, pk="name")
        _create(conn, "counts", [{"key": k, "value": v} for k, v in sorted(C.items())], pk="key")
        conn.execute('CREATE INDEX "ix_ro_base" ON "row_outcomes"("basename")')
        conn.execute('CREATE INDEX "ix_ro_sys" ON "row_outcomes"("sys_id")')
        conn.execute('CREATE INDEX "ix_ro_key" ON "row_outcomes"("durable_key")')
    conn.close()
    os.replace(tmp, db_path)
    with open(os.path.join(out_dir, "gate2_summary.json"), "w", encoding="utf-8") as fh:
        json.dump({"meta": meta, "counts": dict(sorted(C.items())), "checks": checks}, fh, ensure_ascii=False, indent=1)
    with open(os.path.join(out_dir, "GATE2_REPORT.md"), "w", encoding="utf-8") as fh:
        fh.write(render_report(outcomes, aliases, rulings, checks, C, meta))
    return db_path


def render_report(outcomes, aliases, rulings, checks, C, meta) -> str:
    failed = [c for c in checks if not c["passed"]]
    out = [f"# FGP identity, credit and visibility rules — gate 2 ({meta.get('run_at', '')})", "",
           f"**Result: {'FAIL' if failed else 'PASS'}** — {len(checks) - len(failed)} / {len(checks)} checks passed.", "",
           "| Check | Result | Expected | Actual | What it guards |", "|---|---|---|---|---|"]
    for c in checks:
        out.append(f"| `{c['name']}` | {'pass' if c['passed'] else '**FAIL**'} | {c['expected']} | {c['actual']} | {c['note']} |")
    out += ["", "## Outcomes of today's rows", "", "| Outcome | Rows |", "|---|---|"]
    for k in OUTCOMES:
        out.append(f"| `{k}` | {C.get('outcome.' + k, 0):,} |")
    out += ["", f"Credit policy: **{meta.get('credit_policy', 'generic')}** — publishable {C['publishable.under_policy']:,} "
            f"(strict {C['publishable.strict']:,}; {C['credit.generic_rows']:,} rows under the generic attribution).",
            "", "## Rows whose FIST source has no verified credit (generic under the ruling; refine from the live FGP site)", "",
            "| FIST source | Rows | Today's credits (top) |", "|---|---|---|"]
    for r in rulings:
        out.append(f"| {r['source']} | {r['rows']:,} | {r['today_credits']} |")
    out += ["", "## All counts", "", "| Key | Value |", "|---|---|"]
    for k, v in sorted(C.items()):
        out.append(f"| `{k}` | {v:,} |" if isinstance(v, int) else f"| `{k}` | {v} |")
    out += ["", "## Inputs", ""] + [f"- `{k}`: {v}" for k, v in meta.items()] + [""]
    return "\n".join(out)


# --------------------------------------------------------------------------- main


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--inventory", default=DEFAULT_INVENTORY, help="gate-1 inventory.db")
    ap.add_argument("--fgp-db", default=DEFAULT_FGP_DB)
    ap.add_argument("--fist", default=DEFAULT_FIST)
    ap.add_argument("--out-dir", default=DEFAULT_OUT)
    ap.add_argument("--credit-policy", choices=("strict", "generic"), default="generic",
                    help="generic (the owner's ruling, 2026-10-25): a row whose FIST source has no verified credit "
                         "is published under the generic Friedberg Genizah Project attribution; strict: it is held")
    ap.add_argument("--expect", action="append", metavar="KEY=VALUE")
    ap.add_argument("--expect-file", metavar="PATH")
    args = ap.parse_args(argv)
    try:
        expectations = parse_expectations(args.expect, args.expect_file)
    except (SystemExit, OSError, ValueError) as ex:
        print(ex, file=sys.stderr)
        return 2
    t0 = time.time()
    meta = {"run_at": time.strftime("%Y-%m-%d %H:%M:%S"), "inventory": args.inventory, "fgp_db": args.fgp_db,
            "fist": args.fist, "credit_policy": args.credit_policy}
    log("loading the gate-1 inventory ...")
    with ro(args.inventory) as conn:
        inv = load_inventory(conn)
    log(f"  {len(inv['files']):,} files, {sum(len(v) for v in inv['units'].values()):,} unit rows ({time.time() - t0:.0f}s)")
    log("loading today's rows and the credit sources ...")
    fgp = ro(args.fgp_db)
    rows = load_today(fgp)
    credit_parts = build_credit_parts(fgp)
    groups = collections.defaultdict(list)
    for r in rows:
        if r["sys_id"]:
            groups[(r["filename"], r["sys_id"], r["c_number"])].append(r["id"])
    dup_ids = [i for ids in groups.values() if len(ids) > 1 for i in ids]
    hashes = content_hashes(fgp, dup_ids)
    fgp.close()
    fist = ro(args.fist)
    catalog_map, book_map, copyright = build_catalog_map(fist), build_book_map(fist), load_copyright(fist)
    comments = load_unit_comments(fist)
    bibref_ids = {u["sub"] for units in inv["units"].values() for u in units if u["source"] == BIBREF_SOURCE}
    bibref_map = build_bibref_map(fist, bibref_ids)
    log(f"  {len(rows):,} rows, {len(credit_parts):,} DataSource manuscripts, {len(catalog_map):,} catalogues, "
        f"{len(book_map):,} books, {len(bibref_map):,} bibliographic references, {len(copyright):,} copyright units, "
        f"{len(comments):,} unit comments ({time.time() - t0:.0f}s)")
    log("applying the rules ...")
    outcomes, aliases, rulings, C = build_outcomes(rows, inv, fist, credit_parts, catalog_map, book_map, copyright, hashes,
                                                   credit_policy=args.credit_policy, bibref_map=bibref_map, comments=comments)
    fist.close()
    checks = run_checks(outcomes, aliases, C, expectations)
    db_path = write_outputs(outcomes, aliases, rulings, checks, C, args.out_dir, meta)
    failed = [c for c in checks if not c["passed"]]
    for c in checks:
        log(f"  [{'ok  ' if c['passed'] else 'FAIL'}] {c['name']}: expected {c['expected']}, got {c['actual']}")
    log(f"wrote {db_path} ({time.time() - t0:.0f}s)")
    log(f"RESULT: {'FAIL' if failed else 'PASS'} ({len(checks) - len(failed)}/{len(checks)})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
