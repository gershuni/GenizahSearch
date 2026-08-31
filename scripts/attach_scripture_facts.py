# -*- coding: utf-8 -*-
"""Attach a `scripture_fact` table to a v5 review artifact.

WHAT IT ANSWERS. A match between a Genizah page and a reference work can rest
on text BOTH sides are quoting -- scripture, most often. The hardmask already
blanks the quotations it detected before matching, but quotes escape it three
ways: variant wording, inline citations breaking the run, and quotes below its
threshold. The reviewer then sees a Bible page "matching" Seder Olam at exactly
the verse Seder Olam quotes, with nothing on the row saying so.

THREE DETECTORS, each recorded separately so the viewer can say WHY:
  * bible_share / canon_share -- the fraction of the matched span's 20-grams
    found verbatim in the Bible (resp. Mishnah/Tosefta/Bavli/Yerushalmi/Targum)
    streams of the pinned v4.2 reference corpus. Catches verbatim quotes.
  * flank_cite -- a citation formula or a parenthesized biblical citation
    within FLANK chars of the match in the reference display text. Catches
    VARIANT quotes the gram test misses (the Seder Olam example scores low on
    grams -- its wording differs -- but carries its citation in the flank).
  * mask_distance / mask_overlap -- letters from the matched span to the
    nearest hardmask interval of the same work, and the fraction of the span
    lying INSIDE such intervals. A quote that escaped the mask usually sits
    beside quotes that were caught; a span mostly inside caught intervals is
    itself quotation. Only the fraction flags -- contact alone does not.

`flagged` is the union at the thresholds recorded in `meta`. It is a REVIEW
HEURISTIC, not a relation verdict: it never demotes a row, it labels one.

Scope (v3, 2026-08-30): ALL corpora -- EXCEPT works that are themselves
canonical scripture (Bible/Targum/Tafsir, Mishnah, Bavli, Yerushalmi, Tosefta,
Massorah, by domain). For those works "the span is verbatim scripture" IS the
identification, not a contaminant -- the first, R-source-only pass proved the
point by mis-flagging 911 of 1,053 rows on the R-source Tosefta file itself.
The mask detectors still fire only where a mask artifact exists (R-source);
other rows carry NULL there, which the union treats as "did not fire".

Run (with the review server STOPPED -- this writes into the artifact):
    python -X utf8 scripts/attach_scripture_facts.py \
        --db discovery_data/discovery-v5-REVIEW.db
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
import unicodedata
from bisect import bisect_left

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

GRAM = 20
FLANK = 90            # chars of reference display text scanned on each side
EDGE = 30             # chars of the match's own edge joined to each flank
SHARE_FLAG = 0.5      # >= this fraction of grams in scripture -> flagged
MASK_FRAC = 0.5       # >= this fraction of the span inside mask intervals -> flagged
FLANK_MAX_LETTERS = 150  # a citation flank flags only a match SHORTER than this

FINALS = {"ך": "כ", "ם": "מ", "ן": "נ", "ף": "פ", "ץ": "צ"}
_HEB = re.compile("[א-ת]")

# Books as they appear inside a parenthesized citation. Written here rather
# than imported: the review artifact travels without the repo, and this script
# must describe itself to whoever re-runs it.
_BOOKS = ("בראשית|שמות|ויקרא|במדבר|דברים|יהושע|שופטים|שמואל|מלכים|ישעיה|ישעיהו|"
          "ירמיה|ירמיהו|יחזקאל|הושע|יואל|עמוס|עובדיה|יונה|מיכה|נחום|חבקוק|צפניה|"
          "חגי|זכריה|מלאכי|תהלים|תהילים|משלי|איוב|שיר השירים|רות|איכה|קהלת|אסתר|"
          "דניאל|עזרא|נחמיה|דברי הימים")
CITE_PAREN = re.compile(r"\((?:%s)[^)]{0,25}\)" % _BOOKS)
CITE_FORMULA = re.compile(r"שנאמר|שנא'|דכתיב|כדכתיב|וגו'|וכתיב|ככתוב|תלמוד לומר|ת\"ל")


def fold(text):
    """Display text -> the letter-stream alphabet (norm_stream semantics:
    NFC, Hebrew base letters only, finals folded). Niqqud and te'amim are
    combining marks outside [א-ת], so the letter class drops them."""
    t = unicodedata.normalize("NFC", text or "")
    return "".join(FINALS.get(c, c) for c in _HEB.findall(t))


def flank_kind(before, match, after):
    """None, or which citation signal sits AT THE MATCH BOUNDARY
    ('paren'/'formula'/'both').

    Two windows, one per boundary, each spanning FLANK chars of context plus
    EDGE chars of the match itself. The match's edge is scanned because inline
    citations are Hebrew letters -- part of the letter stream -- so the matched
    span often swallows a citation's opening: the Seder Olam example ends
    "...בירושלם (דברי" with the rest in the after-flank, and only a window
    straddling the boundary sees that citation whole.

    The match's MIDDLE is deliberately not scanned. A genuine witness of a
    midrashic or halachic work matches long spans of text that is itself full
    of שנאמר and citations -- that is the work's own voice, not contamination
    -- and scanning whole matches flagged 54% of same_work rows. A quotation
    signal is only a signal about the match when it touches the match's edge."""
    m = match or ""
    win1 = (before or "")[-FLANK:] + m[:EDGE]
    win2 = m[-EDGE:] + (after or "")[:FLANK]
    p = bool(CITE_PAREN.search(win1)) or bool(CITE_PAREN.search(win2))
    f = bool(CITE_FORMULA.search(win1)) or bool(CITE_FORMULA.search(win2))
    if p and f:
        return "both"
    return "paren" if p else ("formula" if f else None)


class MaskIndex:
    def __init__(self, intervals_by_work):
        self.ivs = {k: sorted(v) for k, v in intervals_by_work.items()}
        self.starts = {k: [a for a, _b in v] for k, v in self.ivs.items()}

    def distance(self, raw_id, s, e):
        """Letters from [s,e) to the nearest interval of raw_id; 0 = overlap;
        None = the work has no mask intervals."""
        v = self.ivs.get(raw_id)
        if not v:
            return None
        i = bisect_left(self.starts[raw_id], s) - 1
        best = None
        for j in (i, i + 1, i + 2):
            if 0 <= j < len(v):
                a, b = v[j]
                d = 0 if (a < e and b > s) else (a - e if a >= e else s - b)
                best = d if best is None else min(best, d)
        return best

    def overlap_frac(self, raw_id, s, e):
        """Fraction of [s,e) covered by raw_id's intervals; None = no mask.

        The FRACTION, not mere contact: a 1,000-letter witness span that
        brushes a 60-letter quotation does not "rest on" it, while a span
        mostly inside known quotation intervals very likely does."""
        v = self.ivs.get(raw_id)
        if not v:
            return None
        if e <= s:
            return 0.0
        tot = 0
        for a, b in v:
            if a >= e:
                break
            if b > s:
                tot += min(b, e) - max(a, s)
        return tot / (e - s)


def gram_sets(corpus_path, say=print):
    import pickle
    say("loading reference corpus (streams for the gram sets)...")
    corpus = pickle.load(open(corpus_path, "rb"))

    def grams(cats):
        s = set()
        for x in corpus:
            if x.get("cat") in cats:
                st = x["stream"]
                for i in range(len(st) - GRAM + 1):
                    s.add(hash(st[i:i + GRAM]))
        return s

    bible = grams({"Bible"})
    canon = grams({"Mishnah", "Tosefta", "Bavli", "Yerushalmi", "Targum"})
    say("  bible %d-grams: %d   rabbinic-canon: %d" % (GRAM, len(bible), len(canon)))
    return bible, canon


def share(stream, gramset):
    """Fraction of `stream`'s GRAM-grams present in `gramset` (0.0 if too short)."""
    if len(stream) < GRAM:
        return 0.0
    grams = [hash(stream[i:i + GRAM]) for i in range(len(stream) - GRAM + 1)]
    return sum(g in gramset for g in grams) / len(grams)


# Works in these domains ARE the canon; a verbatim-scripture span there is the
# identification itself. They get no fact row and the facet shows "not
# computed", with the sidebar card saying why.
CANONICAL_DOMAIN_SQL = (
    "(r.domain LIKE 'Bible:%' OR r.domain LIKE 'Mishnah:%' "
    "OR r.domain LIKE 'Talmud Bavli:%' OR r.domain LIKE 'Massorah%' "
    "OR r.domain = 'Rabbinic Literature / Tosefta' "
    "OR r.domain = 'Rabbinic Literature / Talmud Yerushalmi')")

SCOPE_WHERE = "(r.domain IS NULL OR NOT %s)" % CANONICAL_DOMAIN_SQL


def compute_rows(con, bible, canon, mask, say=print):
    """Yield one scripture_fact tuple per in-scope review row (all corpora,
    canonical-scripture works exempted)."""
    t0 = time.time()
    n = 0
    cur = con.execute(
        "SELECT r.evidence_id, r.ref_before, r.ref_match, r.ref_after, "
        "r.w_start, r.w_end, r.matched_letters, rw.raw_id "
        "FROM review_row r "
        "LEFT JOIN reference_witness rw ON rw.witness_id = r.witness_id "
        "WHERE " + SCOPE_WHERE)
    for r in cur:
        n += 1
        if n % 50000 == 0:
            say("  %d rows scored (%.0fs)" % (n, time.time() - t0))
        s = fold(r["ref_match"])
        b = share(s, bible)
        c = share(s, canon)
        fk = flank_kind(r["ref_before"], r["ref_match"], r["ref_after"])
        have_span = r["raw_id"] and r["w_start"] is not None
        dist = mask.distance(r["raw_id"], r["w_start"], r["w_end"]) \
            if have_span else None
        frac = mask.overlap_frac(r["raw_id"], r["w_start"], r["w_end"]) \
            if have_span else None
        # The union. A citation flank flags only a SHORT match: long same_work
        # spans in quotation-dense works end at mask boundaries, which parks
        # citations at their edges -- the work's own voice, not contamination
        # (unconditioned, this one detector flagged 54% of same_work rows).
        short = (r["matched_letters"] or 0) < FLANK_MAX_LETTERS
        flagged = int(max(b, c) >= SHARE_FLAG
                      or (frac is not None and frac >= MASK_FRAC)
                      or (fk is not None and short))
        yield (r["evidence_id"], round(b, 4), round(c, 4),
               int(fk is not None), fk, dist,
               None if frac is None else round(frac, 4), flagged)


DDL = """CREATE TABLE scripture_fact(
  evidence_id  TEXT PRIMARY KEY REFERENCES review_row(evidence_id),
  bible_share  REAL NOT NULL,     -- matched span's 20-grams found in the Bible
  canon_share  REAL NOT NULL,     -- ... in Mishnah/Tosefta/Bavli/Yerushalmi/Targum
  flank_cite   INTEGER NOT NULL,  -- citation formula/paren-citation at the match boundary
  flank_kind   TEXT,              -- 'paren' | 'formula' | 'both' | NULL
  mask_distance INTEGER,          -- letters to nearest hardmask interval; NULL = no mask
  mask_overlap REAL,              -- fraction of the span inside mask intervals; NULL = no mask
  flagged      INTEGER NOT NULL   -- union verdict at the thresholds in meta
)"""


def attach(db_path, corpus_path, mask_path, say=print):
    masks = json.load(open(mask_path, encoding="utf-8"))
    mask = MaskIndex(masks)
    bible, canon = gram_sets(corpus_path, say)

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = list(compute_rows(con, bible, canon, mask, say))
        total = con.execute("SELECT COUNT(*) FROM review_row r "
                            "WHERE " + SCOPE_WHERE).fetchone()[0]
        if len(rows) != total:
            raise RuntimeError("scored %d rows but the artifact has %d "
                               "in-scope rows -- refusing to publish a "
                               "partial table" % (len(rows), total))
        flagged = sum(r[-1] for r in rows)
        con.execute("BEGIN")
        con.execute("DROP TABLE IF EXISTS scripture_fact")
        con.execute(DDL)
        con.executemany("INSERT INTO scripture_fact VALUES (?,?,?,?,?,?,?,?)",
                        rows)
        for k, v in (
                ("scripture_fact.version", "3"),
                ("scripture_fact.scope",
                 "all corpora except works in canonical-scripture domains"),
                ("scripture_fact.thresholds", json.dumps(
                    {"gram": GRAM, "share_flag": SHARE_FLAG,
                     "flank_chars": FLANK, "match_edge_chars": EDGE,
                     "flank_max_letters": FLANK_MAX_LETTERS,
                     "mask_frac": MASK_FRAC})),
                ("scripture_fact.rows", str(len(rows))),
                ("scripture_fact.flagged", str(flagged)),
                ("scripture_fact.built_at", time.strftime("%Y-%m-%d %H:%M:%S")),
                # The viewer reads every doc.* key from the artifact's own meta.
                ("doc.scripture_flag",
                 "A computed review label, never a relation verdict. Flagged "
                 "when any of these holds: (1) at least half the matched "
                 "span's 20-letter grams occur verbatim in the Bible or in "
                 "Mishnah/Tosefta/Talmud/Targum; (2) a citation formula or "
                 "parenthesized biblical citation sits at the match boundary "
                 "(%d chars of context plus %d chars of the match's own edge "
                 "-- never its middle, which is the work's own voice) AND the "
                 "match is under %d letters -- long witness spans in "
                 "quotation-dense works end at mask boundaries, parking "
                 "citations at their edges; (3) at least half the span lies "
                 "inside quotation intervals the pre-matching hardmask "
                 "already caught (mask data exists for R-source only). A "
                 "flagged match may rest on text both sides are quoting "
                 "rather than on the work itself. Computed for every corpus "
                 "EXCEPT works that are themselves canonical scripture "
                 "(Bible/Targum, Mishnah, Talmud, Tosefta, Massorah) -- "
                 "there, a verbatim-scripture span IS the identification."
                 % (FLANK, EDGE, FLANK_MAX_LETTERS))):
            con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
        # The facet projection now needs a scripture_flagged column; dropping it
        # makes the viewer rebuild on next start (it is a cache by design).
        con.execute("DROP TABLE IF EXISTS facet_row")
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        con.close()
        raise
    con.close()
    say("scripture_fact: %d rows, %d flagged (%.1f%%)"
        % (len(rows), flagged, 100.0 * flagged / max(1, len(rows))))
    return len(rows), flagged


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"))
    ap.add_argument("--corpus", default=os.path.join(
        REPO_ROOT, "discovery_builds", "discovery_v4_2", "build",
        "ref_corpus_v6.pkl"))
    ap.add_argument("--masks", default=os.path.join(
        REPO_ROOT, "same_work_spike", "probe", "rsource", "data",
        "mask2_hardmask.json"))
    args = ap.parse_args(argv)
    for p in (args.db, args.corpus, args.masks):
        if not os.path.exists(p):
            raise SystemExit("missing: %s" % p)
    attach(args.db, args.corpus, args.masks)
    return 0


if __name__ == "__main__":
    sys.exit(main())
