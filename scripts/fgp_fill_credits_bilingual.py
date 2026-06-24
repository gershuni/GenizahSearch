# -*- coding: utf-8 -*-
"""Bilingual FGP credits + FIST fallback for the NULL ``source_credit`` rows.

Background
----------
``scripts/fgp_rebuild_text_and_credit.py`` already populates ``source_credit``
from FGP's own ``fgp_shelfmark_meta.DataSource`` (rich, per-item: individual
transcribers, team heads, catalogs, multi-source). That covers ~27,070 rows.
~17,964 rows are NULL — they have no DataSource credit AND no visual XML
(e.g. CUL shelfmarks whose MetadataOnShelfmark was never imported, incl.
T-S Ar.50.45). Those are filled here, deterministically, from FIST (FileName /
image_id prefix -> team / catalog / book), credited at the **team-head** level
(the per-item transcriber is not recoverable locally — see the session notes).

This script ALSO splits the credit into two columns so the web/desktop UI can
show "Hebrew in the Hebrew UI, English in the English UI" (Hillel, 2026-06-24):

    source_credit_he   source_credit_en

For DataSource rows the two languages come from the ``{eng:.., heb:..}`` source
(usually identical for credits); for catalog/book rows there is one citation
used in both UIs (the source's own language); for the FIST team fallback we have
a verified HE + EN per team.

Legacy ``source_credit`` is left untouched (back-compat).

Run on a COPY first, validate the --report sample, then swap.

Usage:
    python scripts/fgp_fill_credits_bilingual.py <db_path> [--report] [--limit N]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Reuse the existing in-house-team constant so the Genuzos credit stays identical.
from scripts.fgp_rebuild_text_and_credit import XML_DEFAULT_CREDIT  # noqa: E402

FIST_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "fist_data", "FIST.db"
)

# In-house "Genuzos" team — HE matches the existing DB form (no space before
# גנוזות); EN is the FJMS English form (Hillel-verified).
GENUZOS_HE = XML_DEFAULT_CREDIT  # "צוות FGP להעתקות -גנוזות"
GENUZOS_EN = "FGP Transcriptions Team - Genuzos"

# ── Verified team-head credits (Hillel via FJMS, 2026-06-24): {team: (he, en)} ──
# Head-led for pattern-A teams; team+head form for the 5 individual-led teams.
TEAM_CREDITS = {
    101: ("אהרן ממן, ראש צוות FGP לחכמת הלשון",
          "Aharon Maman, Head of FGP Linguistics team"),
    102: ("איילה מאיר אליהו, ראש צוות FGP לפילוסופיה, תיאולוגיה ופולמוס",
          "Ayala Meyer Eliyahu, Head of FGP Philosophy, Theology and Polemics team"),
    103: ("מנחם כהנא, ראש צוות FGP למדרשי הלכה",
          "Menahem Kahana, Head of FGP Halakhic Midrashim team"),
    105: ("צוות FGP לספרות ההלכה בערבית-יהודית (דוד סקליר, ראש הצוות)",
          "FGP Judeo-Arabic Halakhic Literature team (David Sklare, Head)"),
    106: ("צוות FGP לפרשנות המקרא בערבית-יהודית (דוד סקליר, ראש הצוות)",
          "FGP Judeo-Arabic Biblical Exegesis team (David Sklare, Head)"),
    107: ("דוד סקליר, ראש צוות FGP לאוספי פירקוביץ'",
          "David Sklare, Head of FGP Firkovitch Collections team"),
    108: ("חיים מיליקובסקי, ראש צוות FGP למדרשי אגדה",
          "Chaim Milikowsky, Head of FGP Aggadic Midrashim team"),
    109: ("צוות FGP לשו\"ת (מרדכי עקיבא פרידמן, ראש הצוות)",
          "FGP Responsa team (Mordechai A. Friedman, Head)"),
    130: ("אורי ארליך, ראש צוות FGP לתפילה",
          "Uri Erlich, Head of FGP Liturgy team"),
    131: ("אברהם דוד, ראש צוות FGP לחומר תיעודי מאוחר (עברית)",
          "Avraham David, Head of FGP Late Documentary Material (Hebrew) team"),
    132: ("צוות FGP לחומר תיעודי (גויטין) (מרק כהן, ראש הצוות)",
          "FGP Princeton Documentary Material (Goitein) team (Mark Cohen, Head)"),
    133: ("צוות FGP לפרסית-יהודית (שאול שקד, ראש הצוות)",
          "FGP Judeo-Persian team (Shaul Shaked, Head)"),
    135: ("יעקב זוסמן, ראש צוות FGP לתלמוד ירושלמי",
          "Yaacov Sussmann, Head of FGP Yerushalmi team"),
    136: ("פינחס דוד מנדל, ראש צוות FGP למדרש איכה רבא",
          "Paul Mandel, Head of FGP Midrash Eikha Rabba team"),
    151: ("שמואל גליק, ראש צוות FGP לשרידי תשובות – מכון שוקן",
          "Shmuel Glick, Head of FGP Seride Teshuvot Team: Shocken Institute"),
    220: ("אלדינה קינטנה, ראש צוות FGP ללאדינו",
          "Aldina Quintana, Head of FGP Ladino team"),
}
INFRA_CREDITS = {
    200: (GENUZOS_HE, GENUZOS_EN),
    400: ("יד הרב הרצוג בחסות צוות FGP", "Yad Harav Herzog - FGP sponsored team"),
}


# ── DataSource bilingual parse (keeps BOTH languages; the existing script
#    collapsed to Hebrew). Same {eng:.., heb:..} pseudo-dict format. ──
def _kv(block: str, key: str, other: str):
    m = re.search(rf"{key}\s*:\s*(.*?)(?:\s*,\s*{other}\s*:|$)", block, re.DOTALL)
    return m.group(1).strip() if m else None


def _datasource_parts(ds):
    """List of (en, he) per source block, alignment preserved (one per source)."""
    if not ds:
        return []
    s = ds if isinstance(ds, str) else json.dumps(ds, ensure_ascii=False)
    out = []
    for block in re.findall(r"\{([^{}]*)\}", s):
        e = _kv(block, "eng", "heb")
        h = _kv(block, "heb", "eng")
        if e or h:
            out.append(((e or "").strip() or None, (h or "").strip() or None))
    return out


def build_credit_parts(conn):
    """{mms_id -> [(en, he), ...]} — the manuscript's DataSource source-parts,
    deduped in stable order, alignment preserved (so a team part can be matched
    by its Hebrew text and its English counterpart kept). NOT pre-joined — the
    caller selects only the parts matching a transcription's own source."""
    acc = {}
    try:
        rows = conn.execute(
            "SELECT mms_id, raw_json FROM fgp_shelfmark_meta "
            "WHERE mms_id IS NOT NULL ORDER BY mms_id, rowid"
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    for mms_id, raw in rows:
        if not raw:
            continue
        try:
            j = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        lst = acc.setdefault(str(mms_id), [])
        for part in _datasource_parts(j.get("DataSource")):
            if part not in lst:
                lst.append(part)
    return acc


def _team_token(he_credit: str) -> str:
    """The team-name token from a verified HE credit, used to match the right
    DataSource part (e.g. 105 -> 'ספרות ההלכה בערבית-יהודית', 151 ->
    'שרידי תשובות'). Everything after 'צוות FGP ל', trimmed at ' (' / ' –'."""
    m = re.search(r"צוות FGP ל(.+)", he_credit)
    if not m:
        return he_credit
    return re.split(r" \(| –", m.group(1))[0].strip()


TEAM_TOKENS = {t: _team_token(he) for t, (he, _en) in TEAM_CREDITS.items()}


# ── FIST catalog (500) and book (600) credit maps ──
def _table_columns(fi, table: str) -> set:
    """Column names present in ``table`` (empty if the table is absent)."""
    return {r[1] for r in fi.execute(f"PRAGMA table_info({table})")}


def build_catalog_map(fi):
    """{CatalogId -> '<CatAcronym> Catalog, <Publisher>, <Year>'} (fields optional).

    ``Publisher``/``YearOfPublishing`` exist in the full FIST.db but not in every
    FIST schema variant (e.g. the synthetic test fixture). Select only the
    columns that are present so a leaner schema yields the acronym-only credit
    instead of raising ``OperationalError`` and aborting the whole credit fill.
    """
    cols = _table_columns(fi, "CODE_Catalog")
    if not {"CatalogId", "CatAcronym"} <= cols:
        return {}
    has_pub = "Publisher" in cols
    has_yr = "YearOfPublishing" in cols
    select = "CatalogId, CatAcronym, " + (
        "Publisher" if has_pub else "NULL"
    ) + ", " + ("YearOfPublishing" if has_yr else "NULL")
    out = {}
    for cid, acr, pub, yr in fi.execute(f"SELECT {select} FROM CODE_Catalog"):
        if not acr:
            continue
        parts = [f"{acr} Catalog"]
        if pub and str(pub).strip():
            parts.append(str(pub).strip())
        if yr and str(yr).strip():
            parts.append(str(yr).strip())
        out[cid] = ", ".join(parts)
    return out


def build_book_map(fi):
    """{TitleId -> '<authors; >, <RunningTitleHeb>'} (title-only when no authors).

    Defensive against FIST schema variants: requires ``CODE_Title`` with at least
    one title column; ``CODE_TitleAuthor``/``CODE_Author`` are optional (no author
    join when absent), and ``IsCanceledCode`` is filtered only when present.
    """
    title_cols = _table_columns(fi, "CODE_Title")
    if "TitleId" not in title_cols:
        return {}
    avail_titles = [c for c in ("RunningTitleHeb", "FullTitleHeb", "AcronymHeb") if c in title_cols]
    if not avail_titles:
        return {}

    authors = {}
    ta_cols = _table_columns(fi, "CODE_TitleAuthor")
    if {"TitleId", "AuthorId"} <= ta_cols:
        where = "WHERE IsCanceledCode=0 " if "IsCanceledCode" in ta_cols else ""
        for tid, aid in fi.execute(
            f"SELECT TitleId, AuthorId FROM CODE_TitleAuthor {where}ORDER BY TitleId, rowid"
        ):
            authors.setdefault(tid, []).append(aid)
    aname = {}
    a_cols = _table_columns(fi, "CODE_Author")
    if {"AuthorId", "HebDesc"} <= a_cols:
        aname = {
            r[0]: r[1]
            for r in fi.execute("SELECT AuthorId, HebDesc FROM CODE_Author WHERE HebDesc IS NOT NULL")
        }

    select = "TitleId, " + ", ".join(avail_titles)
    out = {}
    for row in fi.execute(f"SELECT {select} FROM CODE_Title"):
        tid = row[0]
        title = next((str(v).strip() for v in row[1:] if v and str(v).strip()), "")
        if not title:
            continue
        names = [aname[a] for a in authors.get(tid, []) if a in aname]
        out[tid] = (f"{'; '.join(names)}, {title}" if names else title)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("db_path")
    ap.add_argument("--report", action="store_true", help="don't write; print coverage + samples")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    fi = sqlite3.connect(f"file:{FIST_DB}?mode=ro", uri=True)

    cols = {r[1] for r in conn.execute("PRAGMA table_info(fgp_transcriptions)")}
    if not args.report:
        for col in ("source_credit_he", "source_credit_en"):
            if col not in cols:
                conn.execute(f"ALTER TABLE fgp_transcriptions ADD COLUMN {col} TEXT")
        conn.commit()

    print("Building indexes ...")
    credit_parts = build_credit_parts(conn)
    catalog_map = build_catalog_map(fi)
    book_map = build_book_map(fi)
    print(f"  DataSource mms_ids: {len(credit_parts)} | catalogs: {len(catalog_map)} "
          f"| books: {len(book_map)}")

    def credit_for(row):
        """(he, en, category) keyed on the transcription's OWN source (image_id),
        NOT the per-manuscript DataSource aggregate (which mixes in every catalog
        a manuscript appears in — the VII.E.18 'Schwab; Gil' bug).

        * 500/600 -> the precise catalog/book the image_id points at (these have
          no FGP individual; the DataSource aggregate is dropped);
        * scholarly team -> the verified team-head credit, but if the manuscript's
          DataSource carries a part matching THIS team (often with the real
          transcriber, e.g. אילה אליהו), the HE keeps that richer part;
        * 200/400 -> the fixed in-house / sponsored credit (drops catalog noise);
        * else -> the manuscript's DataSource as-is (rare unmapped teams) or null.
        EN is always the clean verified team credit (we have no English transcriber
        names); HE carries the individual where matched."""
        image_id = row["image_id"]
        prefix = int(image_id[:3]) if image_id and image_id[:3].isdigit() else None
        sub = int(image_id[3:7]) if image_id and image_id[3:7].isdigit() else None
        parts = credit_parts.get(str(row["mms_id"]), []) if row["mms_id"] is not None else []

        if prefix == 500:
            s = catalog_map.get(sub)
            return (s, s, "catalog") if s else (None, None, "catalog:unresolved")
        if prefix == 600:
            s = book_map.get(sub)
            return (s, s, "book") if s else (None, None, "book:unresolved")
        if prefix in TEAM_CREDITS:
            tok = TEAM_TOKENS[prefix]
            matched_he = [he for (_en, he) in parts if he and tok in he]
            he = "; ".join(dict.fromkeys(matched_he)) if matched_he else TEAM_CREDITS[prefix][0]
            en = TEAM_CREDITS[prefix][1]
            return he, en, f"team:{prefix}:{'matched' if matched_he else 'fallback'}"
        if prefix in INFRA_CREDITS:
            he, en = INFRA_CREDITS[prefix]
            return he, en, f"infra:{prefix}"
        # Unmapped team / other: keep the manuscript's DataSource (rare), else null.
        if parts:
            he = "; ".join(dict.fromkeys(h for (_e, h) in parts if h)) or None
            en = "; ".join(dict.fromkeys(e for (e, _h) in parts if e)) or None
            return he, (en or he), "datasource-other"
        return None, None, "unknown"

    sel = "SELECT id, mms_id, c_number, image_id, shelfmark, collection, source_credit FROM fgp_transcriptions"
    if args.limit:
        sel += f" LIMIT {args.limit}"
    rows = conn.execute(sel).fetchall()

    cat = Counter()
    filled_null = Counter()
    samples = {}
    updates = []
    for r in rows:
        he, en, category = credit_for(r)
        cat[category.split(":")[0]] += 1
        was_null = r["source_credit"] is None
        if was_null and (he or en):
            filled_null[category.split(":")[0]] += 1
            samples.setdefault(category.split(":")[0], [])
            if len(samples[category.split(":")[0]]) < 3:
                samples[category.split(":")[0]].append(
                    (r["collection"], r["shelfmark"], he, en))
        updates.append((he, en, r["id"]))

    print(f"\nRows: {len(rows)}")
    print("category distribution (all rows):", dict(cat))
    print("NEWLY-filled (were NULL) by category:", dict(filled_null),
          "=> total", sum(filled_null.values()))
    null_after = sum(1 for he, en, _ in updates if not (he or en))
    print(f"still-null after fill: {null_after}")
    print("\n--- samples of newly-filled rows (verify on FJMS) ---")
    for c, items in samples.items():
        print(f"[{c}]")
        for coll, sm, he, en in items:
            print(f"   {coll} {sm}")
            print(f"      HE: {he}")
            print(f"      EN: {en}")

    if args.report:
        print("\nREPORT (no write).")
    else:
        print("\nWriting source_credit_he / source_credit_en ...")
        conn.executemany(
            "UPDATE fgp_transcriptions SET source_credit_he=?, source_credit_en=? WHERE id=?",
            updates,
        )
        conn.commit()
        print(f"Updated {len(updates)} rows.")
    conn.close()
    fi.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
