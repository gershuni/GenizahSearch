"""Local review server over the full v3 quote-identification DB.

WHY A SERVER AND NOT A PAGE. The review set is 254,612 rows carrying both
sides of every match; as one HTML file that is over a gigabyte and no browser
opens it. So the DB stays a DB and this serves slices of it. Stdlib only
(`http.server` + `sqlite3`) -- a teammate needs the DB, this file, and Python.

A file:// page was also rejected for a second reason: browsers discard its
localStorage without warning, and grading work would evaporate on restart. Here
every grade is written to disk immediately.

WHAT IT GRADES, and why that is the point. `divergence_correctness`
(`catalogue_correct` / `claim_correct` / `unclear`) is HUMAN-ONLY by owner
ruling L, 2026-08-03: the model scored 8/28 on it -- at or below chance for a
three-way choice -- on questions the owner answered 31/32. So the column is
empty by design in every artifact, and the only way it is ever filled is a human
looking at both sides. That is exactly what this tool is for.

GRADES LIVE IN THEIR OWN FILE (`<db>.grades.db`, ATTACHed), never in the review
DB itself, so re-baking the review projection cannot destroy grading work.

PRESENTATION is a deliberate clone of the public `/computed-identifications`
findings page: the same tokens, the same `.gs-discovery` scope, the same class
names and the same row anatomy, so the two surfaces stay comparable and the
reference docs stay accurate. Every reader-facing CLAIM sentence from the public
page is replaced by a grading instruction; the launch headline, the mode strip,
the second-pool invitation and the bilingual UI are not ported at all. See
docs/specs/v3-review-viewer-spec.md.

Run:
    python scripts/serve_v3_review.py --db discovery_data/discovery-v3-REVIEW.db
    # then open http://127.0.0.1:8777

    # a reviewer already running the web app locally gets a first-party session,
    # their own language and theme, and none of the production cost:
    python scripts/serve_v3_review.py --site http://127.0.0.1:8080 --preview frame
"""
from __future__ import annotations

import argparse
import html
import json
import os
import re
import sqlite3
import threading
import unicodedata
import urllib.parse
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DIVERGENCE_VALUES = ("catalogue_correct", "claim_correct", "unclear")
# Stands in for SQL NULL on the wire, so a genuinely-absent value stays
# selectable and keeps a name of its own instead of collapsing into a neighbour.
NULL_TOKEN = "__null__"
PAGE_SIZES = (25, 50, 100)
# 25 because the text pane is OPEN by default -- a 100-row first paint would be
# ~25,000 lines of Hebrew before the reader has filtered anything.
PAGE_SIZE = 25

DEFAULT_SITE = "https://genizahsearch.com"
PREVIEW_MODES = ("image", "frame", "off")

# THE REDACTION MAP. `source_corpus` is the ONE column on this grain that can
# leak a restricted corpus name, so no raw value ever reaches the client: the
# row payload is rewritten server-side (`_public_row`) and the raw column is
# deleted from it, which makes a raw render impossible in the browser rather
# than merely discouraged. An unmapped code renders as a neutral placeholder --
# fail-closed, never the raw string.
CORPUS_LABELS = {
    "sefaria": "Sefaria (open texts)",
    "ja": "Judeo-Arabic",
    "msource": "M-source",
    "rsource": "R-source",
}
CORPUS_UNKNOWN = "other reference corpus"


def corpus_label(code) -> str:
    """The only function allowed to turn a `source_corpus` code into words."""
    if code is None:
        return "no reference corpus recorded"
    return CORPUS_LABELS.get(str(code), CORPUS_UNKNOWN)


# The chip/label vocabulary this tool is REQUIRED to use and the public page is
# forbidden from using: the public surface may not say witness / copy of /
# quotes (a greppable honesty gate), because it is not the axis it ships. Here
# it is exactly the axis being graded, so it must be said in full. Deliberately
# NOT imported from `shared/discovery_display_strings.py`, and this wording must
# never travel back into anything under `web/`.
RELATION_LABELS = {
    "same_work": "Witness",
    "parallel": "Quotation",
    "not_shipped": "Held back by the router",
    "shared_text": "Shared text",
}
RELATION_CARD_LABELS = {
    "same_work": "Witness — this page is a copy of the work",
    "parallel": "Quotation — this page quotes the work",
    "not_shipped": "Held back by the router",
    "shared_text": "Shared text",
}
CLAIM_LABELS = {
    "direct_witness": "Largest span on page",
    "quotes_this_work": "Smaller span on page",
    "shared_text": "Shared wording",
}
ADJUDICATION_LABELS = {
    "provisional": "provisional",
    "human_confirmed": "human confirmed (earlier pass)",
}

# `matched_letters` below this gets the known-weakness prompt on the row.
SHORT_MATCH_LETTERS = 150


# ---------------------------------------------------------------------------
# THE GROUPED NOVELTY VIEW -- the public page's sidebar card 1, mirrored.
#
# `/computed-identifications` does NOT offer the raw shade list. It offers FOUR
# GROUPS under the header "Which findings", and until now this tool offered only
# the eight raw shades -- so a reader could not ask the private surface the
# question the public one is built around, and "what does the public page show
# here?" had to be answered by hand-unioning chips. This clone exists so the two
# surfaces stay comparable; a grouping that differed would end that.
#
# THE AUTHORITY is `shared/discovery_service.py::_NOVELTY_VIEW_SHADES`, itself
# derived from `shared/discovery_novelty.py::HIDDEN_BY_DEFAULT_SHADES` -- so a
# shade joining or leaving the hidden-by-default policy moves the public
# grouping automatically. It is RESTATED here rather than imported because this
# file is stdlib-only by contract (a teammate needs the DB, this file, and
# Python -- not a checkout). A restatement that can drift silently is worth
# nothing, so `check_novelty_views` below re-derives the mapping from the real
# module WHENEVER the repo happens to be importable and says so loudly on a
# mismatch; outside the repo it is a no-op and the tool still runs.
#
# `None` means NO PREDICATE, the same convention the service uses: an empty
# filter is not a filter. An unrecognised view widens to `all` rather than
# raising, mirroring `novelty_view_shades` -- a vocabulary that moved between
# releases must degrade to showing everything, never to a narrower set the
# reader did not choose.
#
# THE RAW 8-SHADE FACET STAYS (card 5 below). The grader is grading the gate,
# and hiding its outputs makes it ungradeable. The two COMPOSE: the view is an
# independent WHERE clause, so selecting one narrows the shade facet beneath it
# to that view's members and their counts.
# ---------------------------------------------------------------------------

NOVELTY_VIEW_DEFAULT = "all"

NOVELTY_VIEW_SHADES = {
    "all": None,
    "candidates": ("fills_gap",),
    "divergent": ("diverges_work", "diverges_part"),
    "either": ("fills_gap", "diverges_work", "diverges_part"),
}

# The public page's own words, in the public page's own order: `novelty_view_*`
# from `web/pages/findings.py`, except the candidacy option, which reuses the
# ratified `novelty_strings()['toggle']` name there and does so here too.
NOVELTY_VIEW_LABELS = (
    ("all", "All findings"),
    ("candidates", "Candidates for new finds"),
    ("divergent", "Do not correspond to the catalogue"),
    ("either", "Candidates or non-correspondence"),
)

# `divergence_warning()`, verbatim -- ruling F's control is an explicitly WARNED
# one, so the warning travels with the grouping rather than being left behind on
# the public page.
NOVELTY_VIEW_WARNING = (
    "These findings do not correspond to an existing catalogue identification. "
    "Neither side has been adjudicated — read them with that in mind.")


# ---------------------------------------------------------------------------
# "View on Sefaria" -- a citation link for the canonical works
# ---------------------------------------------------------------------------
# A CONVENIENCE BESIDE THE ADDRESS, never instead of it (owner, 2026-09-01: the
# important thing is the exact char mapping file to file). The link locates the
# chapter or folio; it cannot locate the matched span, because our offsets index
# our own files and Sefaria serves a different edition with its own numbering.
#
# Only the three families whose address maps EXACTLY are linked. Yerushalmi,
# Tosefta and Mishneh Torah are left unlinked on purpose -- their numbering or
# their section names do not correspond 1:1, and a link to the wrong passage is
# worse than no link.
_SEF_TANAKH = {
    "בראשית": "Genesis", "שמות": "Exodus", "ויקרא": "Leviticus",
    "במדבר": "Numbers", "דברים": "Deuteronomy", "יהושע": "Joshua",
    "שופטים": "Judges", "שמואל א": "I_Samuel", "שמואל ב": "II_Samuel",
    "מלכים א": "I_Kings", "מלכים ב": "II_Kings", "ישעיהו": "Isaiah",
    "ירמיהו": "Jeremiah", "יחזקאל": "Ezekiel", "הושע": "Hosea",
    "יואל": "Joel", "עמוס": "Amos", "עובדיה": "Obadiah", "יונה": "Jonah",
    "מיכה": "Micah", "נחום": "Nahum", "חבקוק": "Habakkuk",
    "צפניה": "Zephaniah", "חגי": "Haggai", "זכריה": "Zechariah",
    "מלאכי": "Malachi", "תהלים": "Psalms", "משלי": "Proverbs",
    "איוב": "Job", "שיר השירים": "Song_of_Songs", "רות": "Ruth",
    "איכה": "Lamentations", "קהלת": "Ecclesiastes", "אסתר": "Esther",
    "דניאל": "Daniel", "עזרא": "Ezra", "נחמיה": "Nehemiah",
    "דברי הימים א": "I_Chronicles", "דברי הימים ב": "II_Chronicles",
}
# tractate names shared by Bavli and Mishnah; Mishnah takes a "Mishnah_" prefix
_SEF_TRACTATE = {
    "ברכות": "Berakhot", "שבת": "Shabbat", "עירובין": "Eruvin",
    "פסחים": "Pesachim", "שקלים": "Shekalim", "יומא": "Yoma",
    "סוכה": "Sukkah", "ביצה": "Beitzah", "ראש השנה": "Rosh_Hashanah",
    "תענית": "Taanit", "מגילה": "Megillah", "מועד קטן": "Moed_Katan",
    "חגיגה": "Chagigah", "יבמות": "Yevamot", "כתובות": "Ketubot",
    "נדרים": "Nedarim", "נזיר": "Nazir", "סוטה": "Sotah",
    "גיטין": "Gittin", "קידושין": "Kiddushin", "בבא קמא": "Bava_Kamma",
    "בבא מציעא": "Bava_Metzia", "בבא בתרא": "Bava_Batra",
    "סנהדרין": "Sanhedrin", "מכות": "Makkot", "שבועות": "Shevuot",
    "עבודה זרה": "Avodah_Zarah", "הוריות": "Horayot", "זבחים": "Zevachim",
    "מנחות": "Menachot", "חולין": "Chullin", "בכורות": "Bekhorot",
    "ערכין": "Arakhin", "תמורה": "Temurah", "כריתות": "Keritot",
    "מעילה": "Meilah", "תמיד": "Tamid", "מידות": "Middot",
    "קינים": "Kinnim", "נידה": "Niddah", "אבות": "Pirkei_Avot",
    "פאה": "Peah", "דמאי": "Demai", "כלאים": "Kilayim",
    "שביעית": "Sheviit", "תרומות": "Terumot", "מעשרות": "Maasrot",
    "מעשר שני": "Maaser_Sheni", "חלה": "Challah", "ערלה": "Orlah",
    "ביכורים": "Bikkurim", "עדיות": "Eduyot", "כלים": "Kelim",
    "אהלות": "Oholot", "נגעים": "Negaim", "פרה": "Parah",
    "טהרות": "Tahorot", "מקואות": "Mikvaot", "מכשירין": "Makhshirin",
    "זבים": "Zavim", "טבול יום": "Tevul_Yom", "ידים": "Yadayim",
    "עוקצין": "Oktzin",
}
_GEM = {"א": 1, "ב": 2, "ג": 3, "ד": 4, "ה": 5, "ו": 6, "ז": 7, "ח": 8,
        "ט": 9, "י": 10, "כ": 20, "ך": 20, "ל": 30, "מ": 40, "ם": 40,
        "נ": 50, "ן": 50, "ס": 60, "ע": 70, "פ": 80, "ף": 80, "צ": 90,
        "ץ": 90, "ק": 100, "ר": 200, "ש": 300, "ת": 400}


def _gematria(tok):
    """A Hebrew numeral to an int, or None.

    "Every letter is a numeral letter" is NOT a usable test -- EVERY Hebrew
    letter has a value, so that rule read \u05e9\u05dc\u05de\u05d4 as 375 and would have invented
    an address. A Hebrew numeral is written in NON-INCREASING order of value,
    so that is what is enforced: \u05e7\u05d9\u05d8 (100, 10, 9) parses; \u05e9\u05dc\u05de\u05d4 (300, 30, 40, 5
    -- 40 follows 30) does not. Call sites bound the result to what a chapter
    or a folio can actually be.
    """
    tok = (tok or "").replace("\u05f3", "").replace("'", "").replace(
        "\u05f4", "").replace('"', "").strip()
    if not tok:
        return None
    n, prev = 0, None
    for ch in tok:
        v = _GEM.get(ch)
        if v is None:
            return None
        if prev is not None and v > prev:
            return None      # a word made of numeral letters, not a numeral
        prev = v
        n += v
    return n or None


#: Beyond these a "numeral" was a word that happened to descend. Psalms 150 is
#: the largest chapter in the canon; Bavli folios stop well under 200.
_MAX_CHAPTER = 150
_MAX_FOLIO = 200


def sefaria_ref(work_title, locus_label):
    """(url, human_label) for a canonical work with a resolvable address, else
    (None, None). Never guesses: an unmapped book or an unparsable locus yields
    no link at all."""
    if not work_title or not locus_label:
        return None, None
    title, locus = work_title.strip(), locus_label.strip()
    if ", " not in title:
        return None, None
    fam, book = title.split(", ", 1)
    fam, book = fam.strip(), book.strip()
    slug = chap = None
    if fam in ("\u05ea\u05e0\u05f4\u05da", '\u05ea\u05e0"\u05da'):
        slug = _SEF_TANAKH.get(book)
        if slug and locus.startswith("\u05e4\u05e8\u05e7 "):
            # a range ("פרק א–ב") links to its first chapter
            first = locus[4:].split("\u2013")[0].split("-")[0]
            chap = _gematria(first)
            if chap and chap > _MAX_CHAPTER:
                chap = None
    elif fam in ("\u05ea\u05dc\u05de\u05d5\u05d3 \u05d1\u05d1\u05dc\u05d9",):
        slug = _SEF_TRACTATE.get(book)
        parts = locus.split()
        if slug and len(parts) == 2:
            folio = _gematria(parts[0])
            if folio and folio > _MAX_FOLIO:
                folio = None
            side = parts[1].replace("\u05f4", '"')
            if folio and side in ('\u05e2"\u05d0', '\u05e2"\u05d1'):
                return ("https://www.sefaria.org/%s.%d%s"
                        % (slug, folio, "a" if side.endswith("\u05d0") else "b"),
                        "%s %d%s" % (slug.replace("_", " "), folio,
                                     "a" if side.endswith("\u05d0") else "b"))
        return None, None
    elif fam == "\u05de\u05e9\u05e0\u05d4":
        t = _SEF_TRACTATE.get(book)
        slug = ("Mishnah_" + t) if t and t != "Pirkei_Avot" else t
        if slug and locus.startswith("\u05e4\u05e8\u05e7 "):
            chap = _gematria(locus[4:].split("\u2013")[0].split("-")[0])
            if chap and chap > _MAX_CHAPTER:
                chap = None
    if not slug or not chap:
        return None, None
    return ("https://www.sefaria.org/%s.%d" % (slug, chap),
            "%s %d" % (slug.replace("_", " "), chap))


# ---------------------------------------------------------------------------
# Structural markers inside the text
# ---------------------------------------------------------------------------
# Owner, 2026-09-01: the live site already handles these. Its rule lives in
# `scripts/bake_discovery_excerpts.py::clean_ja_markers` (owner ruling
# 2026-08-13) and is REPEATED here rather than imported, because this file ships
# to a reviewer as a single stdlib-only script and cannot import the repo.
# `tests/test_review_marker_cleaning.py` asserts the two agree, so the copy
# cannot drift silently.
#
# TWO families are removed, both structural, neither ever content:
#   +פסוק~ +כב~   J-corpus label/value markers (9,148 rows). `+` and `~` are
#                 never content characters in that corpus.
#   >>            the verse/paragraph start marker carried by the M-source text
#                 (197,982 rows, most of them Bible), and its `<<` partner.
#
# THREE families are deliberately KEPT, because they are philology and a
# scholar needs to see them:
#   <יָצֹא ...>   editorial restoration / ketiv-qere brackets (31,561 rows)
#   {בשמ' רחמ'}  editorial restoration (27,354 rows)
#   ?קר?          an uncertain reading in the transcription
#
# Display-only, applied AFTER slicing: the stored offsets index the ORIGINAL
# text and are never touched, so an address printed on the row still lands on
# the same characters in the source file.
_JA_MARKER_RE = re.compile(r"\+[^+~\n]{0,40}~")
_SECTION_MARK_RE = re.compile(r"(?:^|(?<=\s))(?:>>|<<)(?=\s|$)", re.M)


def clean_display_markers(text):
    """Strip structural markers from ONE already-cut display piece."""
    if not text or not isinstance(text, str):
        return text
    s = text
    if "+" in s or "~" in s:
        s = _JA_MARKER_RE.sub("", s)
        # a marker the slice cut in half, at either end of the piece
        s = re.sub(r"^[^+~\n]{0,40}~", "", s)
        s = re.sub(r"\+[^+~\n]{0,40}$", "", s)
    if ">>" in s or "<<" in s:
        s = _SECTION_MARK_RE.sub("", s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    s = re.sub(r" ?\n ?", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip(" ")


def check_novelty_views(say=print) -> bool:
    """Re-derive the grouping from the service and report any drift.

    Compares MEMBERSHIP (sets), not tuple order: the mapping becomes an
    `IN (...)` predicate, so a reordering of `DIVERGENCE_SHADE_ORDER` is not a
    difference and must not be reported as one. Returns True only when the
    check actually ran and matched.
    """
    try:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from shared.discovery_service import NOVELTY_VIEWS, novelty_view_shades
    except Exception:
        return False                      # no checkout -- nothing to compare to
    real = {v: novelty_view_shades(v) for v in NOVELTY_VIEWS}
    norm = lambda m: {k: (None if v is None else frozenset(v)) for k, v in m.items()}
    if norm(real) == norm(NOVELTY_VIEW_SHADES):
        return True
    say("! novelty views DRIFTED from shared/discovery_service.py -- this tool "
        "no longer groups the shades the way /computed-identifications does, so "
        "the two surfaces are not comparable.")
    say("!   public: %r" % (real,))
    say("!   here  : %r" % (NOVELTY_VIEW_SHADES,))
    return False


# ---------------------------------------------------------------------------
# The slim facet projection
# ---------------------------------------------------------------------------

# EVERY filter, count and facet runs against `facet_row`; only the 25-row page
# body reads `review_row`. review_row carries ~6 KB of both-sides text per row
# (1.4 GB); facet_row is ~40 MB. A facets response slow enough for the browser
# to cancel leaves every control empty with nothing saying why -- that has
# happened here, and it is why the sort keys live in this table too rather than
# in an ORDER BY over the fat one.
FACET_COLS = ("evidence_id", "sys_id", "shelfmark", "domain", "work_id",
              "work_title", "work_author", "novelty_status", "main_pool",
              "claim_type", "router_verdict", "routing_status",
              # added for the private controls (spec 6.3): corpus / band /
              # prior-review / demotion-reason filters and the three evidence
              # sort keys. Without these, every one of those controls would
              # have had to scan review_row.
              "source_corpus", "confidence_band", "adjudication_status",
              "main_pool_reason", "matched_letters", "coverage_ppm", "n_spans",
              "owner_ruling",
              # the citable-address filter ("locus contains") scans this, and
              # the From/To range control orders loci by w_start
              "locus_label", "w_start")

# DERIVED at build time, not per request. "Fewest matched pages first" needs the
# number of rows in each identification (sys_id x work_id); computing that with
# a window function per page turn is a full scan of the projection, so it is
# materialised once instead.
FACET_DERIVED = (("id_pages", "COUNT(*) OVER (PARTITION BY sys_id, work_id)"),
                 # 0/1 from `scripture_fact` where that table is attached
                 # (scripts/attach_scripture_facts.py); NULL where it is not,
                 # or for rows outside its scope. The expression is swapped for
                 # a literal NULL at build time when the table is absent -- see
                 # `ensure_facet_table` -- so a v3-era file still opens.
                 ("scripture_flagged",
                  "(SELECT sf.flagged FROM scripture_fact sf "
                  "WHERE sf.evidence_id = review_row.evidence_id)"),
                 # the liturgy/formulary label (scripts/attach_formula_flags.py);
                 # NULL fallback when the table is absent, like scripture
                 ("formula_kind",
                  "(SELECT ff.kind FROM formula_fact ff "
                  "WHERE ff.evidence_id = review_row.evidence_id)"),
                 # the LLM adjudication verdicts (scripts/attach_gate_verdicts.py)
                 # -- PAIR grain, so every row of an identification carries its
                 # pair's verdict; NULL fallback when the table is absent
                 ("gate_divergence",
                  "(SELECT gv.verdict FROM gate_verdict_fact gv "
                  "WHERE gv.sys_id = review_row.sys_id "
                  "AND gv.work_id = review_row.work_id "
                  "AND gv.task = 'divergence')"),
                 ("gate_new_finds",
                  "(SELECT gv.verdict FROM gate_verdict_fact gv "
                  "WHERE gv.sys_id = review_row.sys_id "
                  "AND gv.work_id = review_row.work_id "
                  "AND gv.task = 'new_finds')"),
                 # the ONE model column the primary sidebar filters on. The two
                 # tasks ran on DISJOINT novelty populations (divergent vs
                 # fills_gap pairs), so a row has at most one verdict and a
                 # COALESCE is lossless -- the per-task columns above stay for
                 # the Advanced knobs.
                 ("model_verdict",
                  "(SELECT gv.verdict FROM gate_verdict_fact gv "
                  "WHERE gv.sys_id = review_row.sys_id "
                  "AND gv.work_id = review_row.work_id "
                  "ORDER BY gv.task LIMIT 1)"),
                 # filled by an UPDATE right after the build (it reads the
                 # derived columns above) -- see TRIAGE_SQL
                 ("triage", "NULL"))

# THE POOLS (owner, 2026-08-30): the one sorting a reviewer actually needs --
# main pool (worth grading first), a citation relationship, or two sides
# merely sharing the same quoted text (near-useless). Bucket NAMES commit to
# nothing (owner: not "probably the work" -- "main pool"); the rule is
# deterministic over stored signals and is what the headline filter and the
# row's first chip show.
#   main          -- witness verdict, not scripture-flagged, and the match
#                    covers >= 85% of the page (owner ruling 2026-08-30,
#                    tuned on the owner's own 80-card blind deck over THIS
#                    artifact: 85-100 graded 40/40 clean, 70-85 graded 82.5%). The owner's key: a page whose
#                    text mostly does NOT match the work is probably not a
#                    copy of it, so mere clearance of the router's validated
#                    29.8% line is not enough for the main pool.
#   citation      -- quotation verdict, not scripture-flagged
#   shared_quotes -- the matched text itself is scripture both sides could be
#                    quoting (the flag), or the router said shared_text
#   unclear       -- everything else: held-back rows, 29.8-85% witnesses,
#                    flagged witnesses
TRIAGE_SQL = """UPDATE facet_row SET triage = CASE
  WHEN scripture_flagged = 1 AND (router_verdict != 'same_work'
                                  OR router_verdict IS NULL)
       THEN 'shared_quotes'
  WHEN formula_kind = 'embedded_section' THEN 'shared_quotes'
  WHEN router_verdict = 'shared_text' THEN 'shared_quotes'
  WHEN router_verdict = 'same_work' AND COALESCE(scripture_flagged, 0) = 0
       -- documentary_page is deliberately NOT here: it is a catalogue-derived
       -- label, and the catalogue never judges an identification (owner,
       -- 2026-08-30). It renders as context only.
       AND COALESCE(formula_kind, '') NOT IN ('embedded_section',
                                              'standalone_unit')
       AND COALESCE(owner_ruling, '') NOT IN
           ('dropped_as_identification_reference',
            'excluded_from_public_identities')
       AND (coverage_ppm >= 850000
            OR (source_corpus = 'rsource' AND coverage_ppm >= 750000))
       THEN 'main'
  WHEN router_verdict = 'parallel' AND COALESCE(scripture_flagged, 0) = 0
       THEN 'citation'
  ELSE 'unclear'
END"""

DOC_TRIAGE = (
    "A deterministic sorting rule over the stored signals, never a verdict -- "
    "the bucket names deliberately claim nothing. MAIN POOL = the router "
    "called it a witness, the matched text is not flagged as shared "
    "scripture, the work is not owner-ruled out as an identification "
    "reference, and the match covers at least 85% of the page's letters -- "
    "75% for R-source, whose letter-exact coverage against printed editions "
    "tops out at 83.5% (both bars owner-graded blind: base 85-100 scored "
    "40/40, R-source >=75 scored 37/40, every miss formulaic text). The key "
    "is how much of the manuscript does NOT match the work: a page mostly "
    "unmatched is probably not a copy, however real the match. "
    "CITATION = the router called it a quotation and it is not "
    "scripture-flagged. SHARED QUOTATIONS = the matched text itself is a "
    "third text both sides could be quoting -- near-useless for "
    "identification. UNCLEAR = everything else: held-back rows, witnesses "
    "between the router's validated 29.8% line and the 85% main-pool bar, "
    "scripture-flagged witnesses.")

FACET_INDEXES = ("domain", "work_id", "work_author", "novelty_status",
                 "main_pool", "claim_type", "router_verdict", "routing_status",
                 "evidence_id", "sys_id", "source_corpus", "confidence_band",
                 "adjudication_status", "main_pool_reason", "matched_letters",
                 "coverage_ppm", "id_pages", "scripture_flagged", "formula_kind",
                 "gate_divergence", "gate_new_finds", "model_verdict", "triage")


def ensure_facet_table(db_path, say=print):
    """Make sure the DB carries a current slim facet projection. Returns its rows.

    A SLIM TABLE FOR FACETS. Each review_row carries ~6 KB of both-sides text, so
    ANY facet scan drags that payload through memory for columns it never reads --
    which is why a filtered facets call still took seconds even with the right
    indexes. This projection is the filterable columns only (~40 MB against
    1.4 GB), so a facet scan touches a fraction of the data.

    Done here rather than only in the builder so an artifact already on a
    teammate's disk gains it without a 1.5 GB rebuild; it is a cache, and dropping
    it costs only speed.
    """
    con = sqlite3.connect(db_path)
    try:
        # ENSURE INDEXES. `routing_status` had none, and its GROUP BY cost ~1s per
        # facet over 254,612 rows -- seven facets made the response slow enough for
        # the browser to cancel it, which is what left every dropdown empty.
        for name, col in (("ix_rr_routing", "routing_status"),
                          ("ix_rr_band", "confidence_band")):
            try:
                con.execute("CREATE INDEX IF NOT EXISTS %s ON review_row(%s)"
                            % (name, col))
            except sqlite3.OperationalError:
                pass          # older artifact without the column -- not fatal

        have = con.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
                           "AND name='facet_row'").fetchone()[0]
        if have:
            same = (con.execute("SELECT COUNT(*) FROM facet_row").fetchone()[0] ==
                    con.execute("SELECT COUNT(*) FROM review_row").fetchone()[0])
            # ALSO CHECK THE COLUMNS, not just the row count. A facet_row built by
            # an older copy of this script has no `router_verdict` -- the relation
            # axis every filter and chip now reads -- and a row-count check calls
            # that table current. It would serve, and the relation filter would be
            # silently dead, which is precisely the failure this artifact was
            # shipped to a teammate to avoid. This is also the auto-migration:
            # every column added to FACET_COLS or FACET_DERIVED since a teammate
            # last ran the tool fires this rebuild on their own on-disk copy.
            cols = {r[1] for r in con.execute("PRAGMA table_info(facet_row)")}
            derived = {name for name, _expr in FACET_DERIVED}
            if not same or not set(FACET_COLS) <= cols or not derived <= cols:
                con.execute("DROP TABLE facet_row")     # stale against a rebuild
                have = 0
        if not have:
            say("facets    : building the facet index table (one time)...")
            # A db without `scripture_fact` (v3-era, or the attach script never
            # run) still builds -- the column exists and is NULL everywhere,
            # which the filter renders as "not computed", never as an error.
            has = {n: con.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (tbl,)).fetchone()
                for n, tbl in (("scripture_flagged", "scripture_fact"),
                               ("formula_kind", "formula_fact"),
                               ("gate_divergence", "gate_verdict_fact"),
                               ("gate_new_finds", "gate_verdict_fact"),
                               ("model_verdict", "gate_verdict_fact"))}
            exprs = tuple((name, "NULL" if (name in has and not has[name])
                           else expr)
                          for name, expr in FACET_DERIVED)
            select = ", ".join(FACET_COLS + tuple(
                "%s AS %s" % (expr, name) for name, expr in exprs))
            con.execute("CREATE TABLE facet_row AS SELECT %s FROM review_row"
                        % select)
            con.execute(TRIAGE_SQL)
            con.execute("INSERT OR REPLACE INTO meta VALUES "
                        "('doc.triage', ?)", (DOC_TRIAGE,))
            for col in FACET_INDEXES:
                con.execute("CREATE INDEX ix_fr_%s ON facet_row(%s)" % (col, col))
        n = con.execute("SELECT COUNT(*) FROM facet_row").fetchone()[0]
        con.commit()
        return n
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Author normalisation (spec 4.6)
# ---------------------------------------------------------------------------

_PAREN = re.compile(r"\([^)]*\)")


def author_key(surface: str) -> str:
    """Collapse the surface forms of ONE person onto one filter entry.

    Three collision groups cover 30,501 rows here, and the largest splits one
    author across 574 / 3,486 / 12,674 -- so picking one entry hid 82% of that
    author's rows with nothing saying so. The differences are: a gershayim glyph
    (U+05F4 vs an ASCII quote), a trailing parenthetical acronym present on one
    form and absent on another, and one plene/defective spelling variant
    (doubled yod). Each fold below is one of those, and nothing more: an
    aggressive key merges two different people, which is a worse failure than
    the one being fixed.
    """
    s = unicodedata.normalize("NFC", surface)
    for ch in ("״", "″", "''", '""'):
        s = s.replace(ch, '"')
    s = _PAREN.sub(" ", s)            # "... (רמב״ם)" vs the bare name
    s = s.replace("יי", "י").replace("וו", "ו")
    return " ".join(s.split()).casefold()


# ---------------------------------------------------------------------------
# The stylesheet: copied, not reinvented (spec 1.1)
# ---------------------------------------------------------------------------

# 1. THE TOKENS, from web/static/common.css. LIGHT IS THE DEFAULT -- that is
#    what makes this read as a clone of the public page rather than as a
#    different tool that happens to share a layout. The dark block is the
#    existing `[data-theme="dark"]` override, values unchanged (none invented),
#    and the one-click toggle writes `data-theme` on <html> because grading is a
#    hours-long job and the tool this replaces was dark.
CSS_TOKENS = r"""
:root{
  --primary-50:#ecfdf5; --primary-300:#6ee7b7; --primary-400:#34d399;
  --primary-500:#10b981; --primary-600:#059669; --primary-700:#047857;
  --accent-gold:#d4a574; --accent-amber:#f59e0b;
  --bg-primary:#ffffff; --bg-secondary:#f8fafc; --bg-tertiary:#f1f5f9;
  --bg-card:#ffffff; --bg-hover:#f1f5f9; --bg-active:#ecfdf5;
  --text-primary:#1e293b; --text-secondary:#475569; --text-tertiary:#64748b;
  --text-muted:#475569; --text-inverse:#ffffff;
  --border-light:#e2e8f0; --border-medium:#cbd5e1; --border-focus:#059669;
  --shadow-sm:0 1px 2px 0 rgb(0 0 0 / 0.05);
  --shadow-md:0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
}
[data-theme="dark"]{
  --bg-primary:#0f172a; --bg-secondary:#1e293b; --bg-tertiary:#334155;
  --bg-card:#1e293b; --bg-hover:#334155; --bg-active:#064e3b;
  --text-primary:#f1f5f9; --text-secondary:#cbd5e1; --text-tertiary:#cbd5e1;
  --text-muted:#94a3b8; --text-inverse:#0f172a;
  /* Lighter primary shades for dark mode -- emerald-400/300 for AA on dark bg. */
  --primary-600:#34d399; --primary-700:#6ee7b7;
  --border-light:#334155; --border-medium:#475569; --border-focus:#10b981;
  --shadow-sm:0 1px 2px 0 rgb(0 0 0 / 0.3);
  --shadow-md:0 4px 6px -1px rgb(0 0 0 / 0.4);
}
:focus-visible{outline:2px solid var(--primary-600);outline-offset:2px}
"""

# 2. THE DISCOVERY BLOCK, web/static/common.css lines 1593-1957, copied AS-IS
#    including its `.gs-discovery` scope and its comments -- the comments carry
#    the measured contrast ratios and the two hard rules, which is the whole
#    reason the spec says copy rather than reinvent. Keeping the sketch class
#    names exactly (.row .chip .rel .nov .fg .fchip .dnote .needs .caveat .dnode
#    .here) is what keeps the two surfaces comparable and the reference docs
#    accurate.
#
#    ONE DELIBERATE DEVIATION, marked at its rule: `.fg.novgrp` is renamed to
#    `.fg.relgrp`. On the public page the headline axis is novelty; here it is
#    the relation, and that rule is what pins the headline card first.
CSS_DISCOVERY = r"""
/* discovery surfaces — ONE block serving BOTH the browse-page panel and the
   corpus-wide findings page (Phase 136, plan 136-10, PANEL-01 / PANEL-02 /
   NOVEL-01). Sources: the validated sketches in
   .claude/skills/sketch-findings-genizahsearch/references/
   discovery-panel-layout.md § "CSS Patterns" and findings-page.md
   § "CSS Patterns".

   ============================ HOW TO USE ============================
   EVERY rule below is scoped under `.gs-discovery`. The panel's root
   element and the findings page's root element MUST carry that class, or
   none of this applies. The scope is not decoration: the sketches use very
   generic class names (`.row`, `.chip`, `.mode`, `.c`), and this file is a
   GLOBAL stylesheet loaded beside Quasar — an unscoped `.row` rule here
   would restyle every NiceGUI page in the app at phone width. The sketch
   class names are kept EXACTLY as the reference docs record them so those
   docs stay accurate; only the scope is added.

   ============================ TWO HARD RULES ========================
   1. EVERY directional property is LOGICAL — `border-inline-start`,
      `padding-inline-start`, `margin-inline-start`, `text-align: start/end`.
      Both surfaces render LTR and RTL; physical properties break RTL.
   2. The relation chip stays VISUALLY NEUTRAL. Colour-coding it by relation
      kind reintroduces per-tier confidence styling through the back door,
      which is exactly what D-24 prohibits. There is NO confidence scale on
      either surface and no per-tier chip rule exists anywhere in this file.

   Phone-first: comparable surfaces on this site run ~68% mobile, so the
   stacked layout is the base and the wide layout is the media query.

   Contrast: every new text-on-background pair was measured in light,
   parchment and dark. Three sketch values were CORRECTED because they fail
   WCAG AA in at least one theme; each correction is noted at its rule.
   ==================================================================== */

/* ---- panel: the even two-pane grid (the selected variant D layout) ----
   Mobile stacks page-pane first, then manuscript-pane, with nothing hidden;
   at >=900px the two panes carry EQUAL weight. */
.gs-discovery .dpanes { display: block; }

@media (min-width: 900px) {
    .gs-discovery .dpanes {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 16px;
    }
    /* RTL-safe divider between the panes. */
    .gs-discovery .dpanes > div + div {
        border-inline-start: 1px solid var(--border-light);
        padding-inline-start: 16px;
    }
}

/* ---- panel: a bucket that must NOT read as an identification ----
   D-13e keeps "Also shares text with" as a distinct third disclosure level
   and requires it be explicitly never presented as an identification:
   tinted summary, inset inline-start rule, italic qualifier.
   Contrast — text-primary on bg-tertiary: light 13.35, parchment 13.28,
   dark 9.45. */
.gs-discovery .disc.notid > summary {
    background: var(--bg-tertiary);
    color: var(--text-primary);
    border-radius: 6px;
    padding: 4px 8px;
}
.gs-discovery .disc.notid .dbody {
    border-inline-start: 3px solid var(--border-medium);
    margin-inline-start: 12px;
    padding-inline-start: 10px;
}
/* Contrast — text-secondary on bg-primary: light 7.58, parchment 8.80,
   dark 12.02. */
.gs-discovery .disc.notid .dnote {
    font-style: italic;
    color: var(--text-secondary);
    text-align: start;
}

/* ---- panel: the disclosure arrow, which MUST flip for RTL ---- */
.gs-discovery .disc > summary {
    cursor: pointer;
    list-style: none;
    text-align: start;
}
.gs-discovery .disc > summary::-webkit-details-marker { display: none; }
.gs-discovery .disc > summary::before {
    content: '\25B8';                 /* small right-pointing triangle */
    display: inline-block;
    margin-inline-end: 6px;
    transition: transform .15s ease;
}
.gs-discovery .disc[open] > summary::before { transform: rotate(90deg); }
[dir="rtl"] .gs-discovery .disc > summary::before {
    content: '\25C2';                 /* small left-pointing triangle */
}
[dir="rtl"] .gs-discovery .disc[open] > summary::before { transform: rotate(-90deg); }

/* ---- panel: work chips in the manuscript pane ----
   D-13h requires NAMED works rather than a bare count, so these carry real
   titles and must stay legible in both states.
   `.here`  — a work present on THIS folio: solid border + tinted fill.
   `.gated` — a work reachable only behind the screening toggle.

   CORRECTION to the sketch: `.chip.gated` used `opacity: .7`, which drops
   the label to 3.50 (light) / 4.03 (parchment) — below the 4.5 AA floor for
   a real, readable label. Raised to .85: 4.97 / 5.86 / 7.54. The dashed
   border and the recessed fill still carry the distinction.
   Contrast — .chip.here text-primary on bg-active: light 13.89,
   parchment 13.09, dark 8.87. */
/* ---- the entry control's LAUNCH HIGHLIGHT (owner, 2026-08-07) ----
   The panel arrives on a browse toolbar already carrying seven controls, every
   one of them `flat dense` — so the one new surface in the release looked
   exactly like the six things a reader already knows how to ignore. This gives
   it a border and a tint until the reader opens it once.

   PRESENTATIONAL ONLY. It marks the CONTROL as new; it says nothing about the
   matches behind it, and there is no per-tier or per-confidence rule here (D-24
   forbids confidence styling on this surface, and a "look at this" treatment
   keyed on findings would be exactly that through the back door).

   `--accent-gold` deliberately, NOT `--primary-600`: the green primary is the
   site's ACTION colour (Search, Add to Reading Desk), and this is a "notice me",
   not a call to act. Gold is already the caveat rule's accent on this same
   surface, so the panel keeps one accent rather than acquiring a second.

   Contrast — .gs-discovery .discovery-panel-entry-new-badge text on
   bg-tertiary: light 13.35, parchment 13.28, dark 9.45 (the same pair the
   caveat rule measures, which is why it reuses those two variables). */
.gs-discovery.discovery-panel-entry-new {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    border: 1px solid var(--accent-gold);
    border-radius: 6px;
    background: var(--bg-tertiary);
    padding-inline: 4px;
}
.gs-discovery .discovery-panel-entry-new-badge {
    font-size: 10px;
    font-weight: 700;
    line-height: 1.4;
    letter-spacing: .04em;
    text-transform: uppercase;
    padding: 1px 6px;
    border-radius: 999px;
    background: var(--accent-gold);
    /* Near-black on gold in EVERY theme: --accent-gold is a fixed light value
       (#d4a574) that does not change per theme, so a theme-following text
       colour would go white-on-gold in dark and fail AA. Contrast 8.19. */
    color: #1a1a1a;
    text-align: start;
}

.gs-discovery .chip {
    display: inline-block;
    font-size: 12px;
    line-height: 1.4;
    padding: 2px 8px;
    border: 1px solid var(--border-light);
    border-radius: 999px;
    background: var(--bg-secondary);
    color: var(--text-secondary);
    text-align: start;
}
.gs-discovery .chip.here {
    border-color: var(--primary-600);
    background: var(--bg-active);
    color: var(--text-primary);
    font-weight: 700;
}
.gs-discovery .chip.gated {
    border-style: dashed;
    border-color: var(--border-medium);
    opacity: .85;
}

/* ---- findings page: the novelty switch is FIRST in the filter bar,
   regardless of DOM order (it is the page's headline axis). ----

   PRIVATE DEVIATION (spec §4): renamed `.fg.novgrp` -> `.fg.relgrp` and
   repointed. On the public page the headline axis is novelty; on a grading tool
   the headline axis is the RELATION, because witness-vs-quotation is the thing
   being graded. Same rule, same job, one class name. */
.gs-discovery .fg.relgrp { order: -1; }

/* ---- findings page: a filter rendered but BLOCKED on missing data ----
   Never silently absent: a filter that vanishes is indistinguishable from a
   filter that never existed. Dimmed + dashed + an amber tag naming the
   reason. `.fg.blocked` and `.mode.future` are INACTIVE user-interface
   components, which WCAG 1.4.3 exempts from the contrast minimum; their
   effective ratios at opacity .55 are 2.59 / 2.87 / 4.45 and that is
   deliberate — they must read as unavailable. */
.gs-discovery .fg.blocked { opacity: .55; }
.gs-discovery .fg.blocked .fchip {
    cursor: not-allowed;
    border-style: dashed;
}
.gs-discovery .fg.blocked .fchip:hover { border-color: var(--border-medium); }

/* The amber "needs the rebuild" tag.
   CORRECTION to the sketch: it set `color: var(--text-inverse)` on the amber
   fill, which is white in light AND parchment — 2.15 against #f59e0b, a
   clear AA failure. The amber token is theme-INVARIANT (defined only in
   :root), so a theme-varying foreground token cannot work here; a fixed
   dark foreground is correct and measures 6.81 in all three themes. */
.gs-discovery .needs {
    font-size: 9px;
    text-transform: uppercase;
    letter-spacing: .03em;
    background: var(--accent-amber);
    color: #1e293b;
    border-radius: 999px;
    padding: 1px 6px;
    margin-inline-start: 6px;
}

/* ---- both surfaces: the relation chip on a row ----
   NEUTRAL BY DESIGN. Do NOT add a per-kind colour rule here or anywhere
   else; see hard rule 2 at the top of this block. The frozen band label
   rides on the element's `title` attribute, never as visible text.
   Contrast — text-secondary on bg-secondary: light 7.24, parchment 8.53,
   dark 9.85. */
.gs-discovery .rel {
    font-size: 10px;
    padding: 1px 7px;
    border-radius: 999px;
    border: 1px solid var(--border-light);
    background: var(--bg-secondary);
    color: var(--text-secondary);
    white-space: nowrap;
}

/* ---- both surfaces: the novelty badge ----
   Solid presence for a candidate; the `unknown` variant is the fail-closed
   "not checked" state and reads as muted through WEIGHT and STYLE, not
   through a colour that fails contrast.
   CORRECTION to the sketch: `.nov.unknown` used `--text-muted` on
   bg-tertiary — 4.49 (parchment) and 4.04 (dark), both under AA. Switched
   to --text-secondary: 6.92 / 8.27 / 6.97, with italic + normal weight
   carrying the "muted" reading instead.
   Contrast — .nov primary-700 on bg-active: light 5.21, parchment 6.37,
   dark 6.38. */
.gs-discovery .nov {
    font-size: 10px;
    font-weight: 700;
    padding: 1px 7px;
    border-radius: 999px;
    border: 1px solid var(--primary-600);
    color: var(--primary-700);
    background: var(--bg-active);
}
.gs-discovery .nov.unknown {
    border-color: var(--border-medium);
    color: var(--text-secondary);
    background: var(--bg-tertiary);
    font-weight: 400;
    font-style: italic;
}

/* ---- both surfaces: the PERMANENT caveat slot ----
   A designed element between header and body — not fine print, not a
   warning banner. Gold inline-start rule, RTL-safe.
   Contrast — text-primary on bg-tertiary: light 13.35, parchment 13.28,
   dark 9.45. */
.gs-discovery .phead .caveat {
    background: var(--bg-tertiary);
    color: var(--text-primary);
    border-inline-start: 3px solid var(--accent-gold);
    padding: 8px 12px;
    border-radius: 0 6px 6px 0;
    text-align: start;
}
[dir="rtl"] .gs-discovery .phead .caveat { border-radius: 6px 0 0 6px; }

/* ---- findings page: the domain facet tree ----
   Leaf indent and the trailing count both flip for RTL. */
.gs-discovery .dnode { text-align: start; }
.gs-discovery .dnode.leaf { padding-inline-start: 22px; }
.gs-discovery .dnode .c {
    margin-inline-start: auto;
    color: var(--text-secondary);
    font-variant-numeric: tabular-nums;
}

/* ---- findings page: the mode strip ----
   Phase 137's saved judgments and Phase 138's leads ship VISIBLE but inert
   and phase-tagged, so those phases add a tab rather than a page. */
.gs-discovery .mode.future {
    opacity: .55;
    cursor: not-allowed;
    pointer-events: none;
}

/* ---- both surfaces: the low-coverage note under a row ----
   Contrast — text-secondary on bg-primary: light 7.58, parchment 8.80,
   dark 12.02. */
.gs-discovery .dnote {
    font-size: 11px;
    color: var(--text-secondary);
    text-align: start;
}

/* ---- findings page: the RESULT ROW's meta line wraps below 700px ----

   Scoped to findings-row anatomy by CLASS, never by descending from the generic
   NiceGUI row class. The previous rule did the latter and set flex-direction to
   column, which matched EVERY NiceGUI row element inside the page -- 24 of them,
   including the pool segment, the result toolbar, the active-filter chips, the
   launch figures and the pager -- and turned each into a vertical stack on any
   screen under 700px. On a surface whose sibling atlas page takes ~68% of its
   traffic from phones, that is the majority experience.

   It also could not have been doing its stated job: the ONE element carrying
   that class is ROW_CLASS, which is a column element and therefore already
   vertical, and nothing on either surface carries the side class the rule's
   second half targeted. So it stacked everything except the thing it named.

   What a narrow screen actually needs is for the row's META LINE -- relation
   chip, page count, coverage, pool name, actions -- to wrap instead of
   overflowing, which `flex-wrap` does without destroying any horizontal group.
   `gap` is set in logical terms so it needs no RTL mirror.

   EVERY SELECTOR STAYS UNDER `.gs-discovery`, even though the `gs-findings-*`
   class names are already unique enough not to collide. That is a standing rule
   for this whole block (`tests/test_discovery_display_strings.py::
   test_discovery_css_block_is_scoped_and_carries_the_required_treatments`), and
   the reason is that this file is a GLOBAL stylesheet loaded beside Quasar: the
   guard cannot tell a specific-looking name from a generic one, so it requires
   the page scope on all of them rather than adjudicating per selector. Ignoring
   it here would also mean the rule applied on a page that never opted in. */
@media (max-width: 700px) {
    .gs-discovery .gs-findings-row-meta {
        flex-wrap: wrap;
        row-gap: 4px;
    }
    /* The two grouped-row affordances get a full-width target, which is the
       phone-sized fix the owner asked for on the preview. */
    .gs-discovery .gs-findings-row-expander,
    .gs-discovery .gs-findings-row-preview-toggle {
        width: 100%;
        justify-content: flex-start;
    }
}

/* ---- findings page: "How to read this page" is BODY TEXT, not a footnote ----
   Owner, 2026-08-06: "the font of the text inside 'How to read this page' is too
   small".

   The panel holds the page's honesty-critical prose — the recall disclaimer, the
   candidacy sub-line, the checked-source list, the staleness warning and the
   two-bucket rule. All of it inherited `.dnote` (11px), which is the right size
   for a note sitting BESIDE a row and the wrong size for four paragraphs a reader
   has deliberately opened in order to read. Prose set at footnote size is prose
   that gets skipped, which is the same failure the collapse was introduced to
   fix — the owner's earlier verdict was that this text "read as a draft and
   therefore got read by nobody".

   Scoped to the panel's own class, so `.dnote` keeps its 11px everywhere else
   (the row meta line, the facet notes, the launch basis lines). 14px is the
   site's body size; `line-height` is raised with it because 11px leading under
   14px text reads as cramped.

   ONE RULE RATHER THAN SIX INLINE OVERRIDES. The size belongs to the container,
   not to each label in it, and an inline `text-sm` on every child is six places
   to forget when a seventh line is added. Every property here is
   direction-neutral — no RTL mirror needed. */
.gs-discovery .gs-findings-howto .dnote,
.gs-discovery .gs-findings-howto .gs-findings-novelty-help {
    font-size: 14px;
    line-height: 1.55;
}
"""

# 3. PRIVATE-ONLY ADDITIONS, each in its own commented block.
#
#    The public page gets its card shape, its utility classes and its controls
#    from Quasar + NiceGUI's Tailwind-ish helpers. Stdlib-only means none of that
#    is here, so the classes the copied block keys on (`.fg`, `.row`, `.chip`
#    users, `w-full`, `gap-2`, `p-4`) have to be defined. These are
#    RE-IMPLEMENTATIONS of the public geometry, not new design: same 80rem
#    column, same `flex:1 1 280px` / `flex:999 1 420px` sidebar split, same
#    hairline row separator, same pill chips.
CSS_PRIVATE = r"""
/* --- private A: page frame ------------------------------------------------ */
*{box-sizing:border-box}
:root{color-scheme:light}
:root[data-theme="dark"]{color-scheme:dark}
body{margin:0;background:var(--bg-primary);color:var(--text-primary);
     font:14px/1.55 system-ui,"Segoe UI",Arial,sans-serif}
a{color:var(--primary-700)}
[data-theme="dark"] a{color:var(--primary-600)}
mark{border-radius:3px;padding:0 2px}

/* The sticky top bar is the ONE element outside `.gs-discovery`: grading is a
   scrolling job, so search / reset / export / theme / the live count stay
   reachable, while the sidebar deliberately scrolls away with the page (it is
   nine cards tall and a sticky column that tall is a scroll trap). */
.gs-topbar{position:sticky;top:0;z-index:20;background:var(--bg-card);
  border-block-end:1px solid var(--border-light);padding:8px 16px;
  display:flex;flex-wrap:wrap;gap:8px;align-items:center;
  box-shadow:var(--shadow-sm)}
.gs-topbar .grow{margin-inline-start:auto}
.gs-topbar b{font-variant-numeric:tabular-nums}
input,select,textarea,button{font:inherit;color:var(--text-primary);
  background:var(--bg-primary);border:1px solid var(--border-medium);
  border-radius:6px;padding:5px 8px}
button{cursor:pointer;background:var(--bg-secondary)}
button:hover{border-color:var(--primary-600)}

/* --- private B: the page column and the two-column body ------------------- */
.gs-discovery{max-width:80rem;margin:0 auto;padding:16px;
  display:flex;flex-direction:column;gap:16px}
.gs-discovery .phead{display:flex;flex-direction:column;gap:8px}
.gs-discovery h1{font-size:22px;line-height:1.3;margin:0;font-weight:700}
.gs-discovery .body{display:flex;flex-wrap:wrap;gap:16px;align-items:flex-start}
.gs-discovery .sidebar{flex:1 1 280px;min-width:240px;
  display:flex;flex-direction:column;gap:16px}
.gs-discovery .results{flex:999 1 420px;min-width:0;
  display:flex;flex-direction:column;gap:16px}

/* --- private C: the utility classes the copied markup expects -------------- */
.gs-discovery .w-full{width:100%}
.gs-discovery .gap-1{gap:4px}
.gs-discovery .gap-2{gap:8px}
.gs-discovery .gap-4{gap:16px}
.gs-discovery .p-2{padding:8px}
.gs-discovery .p-4{padding:16px}
.gs-discovery .items-center{align-items:center}
.gs-discovery .flex-wrap{flex-wrap:wrap}
.gs-discovery .font-bold{font-weight:700}
.gs-discovery .text-xs{font-size:11px}
.gs-discovery .mono{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;
  font-size:11px}
/* `.row` is a COLUMN element on the public page (NiceGUI's ROW_CLASS is a
   column); `.side` is the horizontal group inside it. Do not swap them -- the
   copied 700px media query assumes exactly this. */
.gs-discovery .row{display:flex;flex-direction:column}
.gs-discovery .side{display:flex;flex-direction:row}

/* --- private D: sidebar cards --------------------------------------------- */
.gs-discovery .fg{background:var(--bg-card);border:1px solid var(--border-light);
  border-radius:8px;padding:16px;display:flex;flex-direction:column;gap:8px;
  box-shadow:var(--shadow-sm)}
.gs-discovery .gs-findings-card-header{font-size:11px;font-weight:700;
  letter-spacing:.04em;text-transform:uppercase;color:var(--text-secondary);
  text-align:start}
.gs-discovery .fchip{display:inline-flex;align-items:center;gap:6px;
  font-size:12px;line-height:1.4;padding:3px 10px;border-radius:999px;
  border:1px solid var(--border-light);background:var(--bg-secondary);
  color:var(--text-secondary);cursor:pointer;text-align:start}
.gs-discovery .fchip:hover{border-color:var(--border-medium)}
/* Selected state = the public page's own: aria-pressed + --bg-active + 700.
   Deliberately NOT the old tool's blue -- a second accent on a surface whose
   only accents are the green primary and the gold caveat rule. */
.gs-discovery .fchip.here{border-color:var(--primary-600);
  background:var(--bg-active);color:var(--text-primary);font-weight:700}
.gs-discovery .fchip .c{margin-inline-start:auto;
  font-variant-numeric:tabular-nums;font-weight:400;font-size:11px}
.gs-discovery .fg .stack{display:flex;flex-direction:column;gap:4px}
.gs-discovery .fg .wrap{display:flex;flex-wrap:wrap;gap:4px}
.gs-discovery .fg input[type=text],.gs-discovery .fg select{width:100%}
/* The domain tree: the public shape exactly -- parent button plus a SEPARATE
   round chevron, indented leaves, a bounded scroll box. The chevron glyph is
   vertical (expand_more / expand_less) so nothing flips for RTL. */
.gs-discovery .dtree{max-height:340px;overflow-y:auto;display:flex;
  flex-direction:column;gap:2px}
.gs-discovery .dnode{display:flex;align-items:center;gap:4px}
.gs-discovery .dnode>button.n{flex:1 1 auto;text-align:start}
.gs-discovery .chev{flex:0 0 auto;width:24px;height:24px;padding:0;
  border-radius:999px;line-height:1;display:flex;align-items:center;
  justify-content:center;color:var(--text-secondary)}

/* the always-visible file+offsets line (provLine): quiet, monospaced, one
   line per side, sitting directly under the two text panes */
.gs-discovery .pvline{display:flex;flex-wrap:wrap;gap:4px 16px;font-size:11.5px;
  color:var(--text-secondary);padding:4px 0}
.gs-discovery .pvline .pvside{display:inline-flex;gap:6px;align-items:baseline}
.gs-discovery .pvline b{font-weight:700;color:var(--text-primary)}
.gs-discovery .pvline .weak{opacity:.75}

/* --- private E2: the CARD grain -------------------------------------------
   A card is a QUESTION ("is this page this work?"), so it reads as one block
   with its evidence indented inside it. Deliberately no new accent colour: the
   witness strip's states are text, because a colour for "no returned
   alignment" would read as a verdict on the witness. */
.gs-discovery .kwcard{background:var(--bg-card);border:1px solid var(--border-light);
  border-radius:8px;padding:12px 14px;display:flex;flex-direction:column;gap:8px;
  box-shadow:var(--shadow-sm)}
.gs-discovery .kwcard>.kwhead{display:flex;flex-wrap:wrap;gap:4px 10px;
  align-items:baseline;justify-content:space-between}
.gs-discovery .kwcard .kwtitle{font-size:15px;font-weight:700}
.gs-discovery .kwcard .kwnums{font-size:12px;color:var(--text-secondary);
  font-variant-numeric:tabular-nums}
.gs-discovery .kwcard .wstrip{display:flex;flex-wrap:wrap;gap:6px}
.gs-discovery .kwcard .wit{font-size:11.5px;line-height:1.45;padding:3px 8px;
  border-radius:6px;border:1px solid var(--border-light);
  background:var(--bg-secondary);color:var(--text-secondary)}
.gs-discovery .kwcard .wit.on{color:var(--text-primary);
  border-color:var(--border-medium);font-weight:600}
.gs-discovery .kwcard .wit .sc{opacity:.8}
.gs-discovery .kwcard .kwev{display:flex;flex-direction:column;gap:8px;
  padding-inline-start:10px;border-inline-start:2px solid var(--border-light)}
.gs-discovery .kwcard .kwev>summary{cursor:pointer;font-size:12px;
  color:var(--text-secondary)}

/* --- private E: the result bar, chip bar, rows, pager --------------------- */
.gs-discovery .rbar{display:flex;flex-direction:column;gap:8px;
  background:var(--bg-card);border:1px solid var(--border-light);
  border-radius:8px;padding:8px 12px}
.gs-discovery .rbar .n{font-variant-numeric:tabular-nums;font-weight:700}
.gs-discovery .chipbar{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
.gs-discovery .achip{display:inline-flex;align-items:center;gap:6px;
  font-size:12px;padding:2px 6px 2px 10px;border-radius:999px;
  border:1px solid var(--border-light);background:var(--bg-secondary);
  color:var(--text-secondary)}
.gs-discovery .achip button{width:18px;height:18px;padding:0;line-height:1;
  border-radius:999px;font-size:11px;display:flex;align-items:center;
  justify-content:center}
.gs-discovery .clearall{border:0;background:none;color:#dc2626;padding:2px 6px}
[data-theme="dark"] .gs-discovery .clearall{color:#fca5a5}
.gs-discovery .rows{display:flex;flex-direction:column;gap:8px}
/* Hairline separator on EVERY row identically. No per-tier, per-band or
   per-confidence row treatment exists here -- a grading tool that visually
   pre-judges a row biases the grader before they read the text. The one
   non-uniform mark on a row is the amber disagreement badge, and that flags a
   two-column DATA condition, not a quality. */
.gs-discovery .gs-findings-row{background:var(--bg-card)}
.gs-discovery .pager{display:flex;gap:8px;align-items:center;
  justify-content:center;padding:8px}
.gs-discovery .empty,.gs-discovery .errbox{display:flex;flex-direction:column;
  align-items:center;gap:8px;padding:32px 16px;color:var(--text-secondary);
  text-align:center}
.gs-discovery .glyph{font-size:28px;line-height:1;opacity:.7}
/* The outage state is REPLACED by a SQLite-error state: exception class, the
   name of the query that failed, and a Retry. Never an empty list -- an empty
   list is indistinguishable from "no rows match", which is the wrong reading. */
.gs-discovery .errbox{border:1px solid var(--accent-amber);border-radius:8px;
  background:var(--bg-tertiary);color:var(--text-primary)}
.gs-discovery .errbox code{font-family:ui-monospace,Consolas,monospace;
  font-size:12px}

/* --- private F: the help panel ------------------------------------------- */
.gs-discovery details.gs-findings-howto{background:var(--bg-card);
  border:1px solid var(--border-light);border-radius:8px}
.gs-discovery details.gs-findings-howto>summary{cursor:pointer;padding:10px 14px;
  font-weight:700}
.gs-discovery .helpbody{padding:0 16px 14px;max-width:62rem;
  display:flex;flex-direction:column;gap:4px}
.gs-discovery .helpbody h4{margin:12px 0 2px;font-size:13px}
/* The two inline-start rules kept from the tool this replaces -- same treatment
   as the caveat plate, gold for "this will mislead you" and green for "this one
   was actually validated". */
.gs-discovery .warn{border-inline-start:3px solid var(--accent-gold);
  padding-inline-start:9px}
.gs-discovery .ok{border-inline-start:3px solid var(--primary-600);
  padding-inline-start:9px}

/* --- private G: pane A, source vs manuscript ------------------------------ */
/* The same 900px breakpoint the public panel's `.dpanes` uses. Each pane
   scrolls INSIDE itself; the page never scrolls horizontally. */
.gs-discovery .cols{display:grid;grid-template-columns:1fr 1fr;gap:12px;
  margin-block-start:8px}
@media(max-width:900px){.gs-discovery .cols{grid-template-columns:1fr}}
.gs-discovery .pane h4{margin:0 0 6px;font-size:13px;font-weight:600;
  color:var(--text-secondary);display:flex;align-items:center;gap:8px}
.gs-discovery .pane h4 button{font-size:11px;padding:1px 7px}
.gs-discovery .txt{direction:rtl;text-align:right;white-space:pre-wrap;
  font-size:15px;line-height:1.9;max-height:320px;overflow:auto;
  background:var(--bg-tertiary);border:1px solid var(--border-light);
  border-radius:8px;padding:10px}
.gs-discovery .ctx{color:var(--text-secondary)}
/* --accent-gold is theme-INVARIANT, so a theme-following foreground would go
   white-on-gold in dark and fail AA. Fixed near-black, 8.19 in every theme. */
.gs-discovery .txt mark{background:var(--accent-gold);color:#1a1a1a}
.gs-discovery .stream{font-size:11px;color:var(--text-secondary)}

/* --- private G2: pane C, what the novelty gate read ----------------------- */
/* Label and text sit in ONE grid so the labels line up down the block and a
   long Hebrew catalogue string cannot push the label out of view. Collapses to
   one column on the same 900px breakpoint the panes use. */
.gs-discovery .readwrap{margin-block-start:6px}
.gs-discovery .readbody{margin-block-start:6px;padding:9px 10px;
  background:var(--bg-tertiary);border:1px solid var(--border-light);
  border-radius:8px;max-height:340px;overflow:auto}
.gs-discovery .rdrow{display:grid;grid-template-columns:170px 1fr;gap:10px;
  padding-block:4px;border-block-end:1px solid var(--border-light)}
.gs-discovery .rdrow:last-child{border-block-end:0}
@media(max-width:900px){.gs-discovery .rdrow{grid-template-columns:1fr}}
.gs-discovery .rdlab{font-size:11px;color:var(--text-secondary);
  text-transform:uppercase;letter-spacing:.03em;padding-block-start:2px}
.gs-discovery .rdtxt{font-size:13px;line-height:1.7;white-space:pre-wrap}
/* The per-work count is dimmed when it is not about this manuscript, so the
   eye does not read it as evidence for the row it is sitting on. */
.gs-discovery .rdrow.weak .rdtxt{color:var(--text-secondary)}
.gs-discovery .rdwarn{font-size:12px;line-height:1.6;margin-block-end:6px;
  padding:6px 8px;border-radius:6px;border:1px solid var(--border-medium);
  background:var(--bg-secondary)}
.gs-discovery .fchip.warnchip{border-color:var(--border-medium);
  color:var(--text-secondary)}

/* --- private H: pane B, the folio preview -------------------------------- */
.gs-discovery .prev{margin-block-start:10px;border:1px solid var(--border-light);
  border-radius:8px;overflow:hidden;background:var(--bg-tertiary);display:none}
.gs-discovery .prev.on{display:block}
.gs-discovery .prev .bar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;
  padding:6px 9px;border-block-end:1px solid var(--border-light);
  color:var(--text-secondary);font-size:12px}
/* Locked height: there is no same-origin JS in either direction, so the parent
   cannot measure the embedded page. See the module docstring and spec 6.1. */
.gs-discovery .prev iframe{width:100%;height:62vh;border:0;background:#fff;
  display:block}
.gs-discovery .prev img{width:100%;max-height:62vh;object-fit:contain;
  display:block;background:#fff}

/* --- private I: the grade bar -------------------------------------------- */
.gs-discovery .grade{margin-block-start:6px;display:flex;gap:8px;
  align-items:center;flex-wrap:wrap;padding-block-start:6px}
.gs-discovery .grade .lbl{font-size:12px;color:var(--text-secondary)}
.gs-discovery .grade input[type=text]{flex:1 1 220px;min-width:160px;
  font-size:12px}

/* --- private J: the 2026-08-31 rework (Codex 1-6) ------------------------- */
/* J1: the fixed three-slot assessment strip. dir=ltr chrome; grayscale only.
   Amber (.attn) marks REVIEWER ATTENTION, never model confidence. */
.gs-discovery .slots{display:grid;
  grid-template-columns:repeat(auto-fit,minmax(200px,1fr));
  gap:8px;margin-block:6px;direction:ltr}
.gs-discovery .sdot{display:inline-block;width:9px;height:9px;
  border-radius:50%;margin-inline-end:6px}
.gs-discovery .slot details>summary{cursor:pointer;list-style:none}
.gs-discovery .slot details>summary::after{content:" ▸";color:var(--text-secondary)}
.gs-discovery .slot details[open]>summary::after{content:" ▾"}
.gs-discovery .slot .sbody{font-size:12px;color:var(--text-secondary);
  line-height:1.5;margin-block-start:4px}
@media(max-width:900px){.gs-discovery .slots{grid-template-columns:1fr}}
.gs-discovery .slot{border:1px solid var(--border-light);border-radius:8px;
  padding:5px 9px;background:var(--bg-tertiary);min-width:0}
.gs-discovery .slot .slbl{font-size:10px;letter-spacing:.06em;
  text-transform:uppercase;color:var(--text-secondary)}
.gs-discovery .slot .sval{font-size:13px;line-height:1.4}
.gs-discovery .slot .ssub{font-size:11px;color:var(--text-secondary);
  font-family:monospace}
.gs-discovery .slot.attn{border-color:#b8860b;background:
  color-mix(in srgb, #b8860b 12%, var(--bg-tertiary))}
.gs-discovery .slot.attn .slbl::after{content:" · REVIEW REQUIRED";color:#b8860b}
/* J2: clamped rationale lines */
.gs-discovery .rationale{font-size:12px;color:var(--text-secondary);
  line-height:1.5;margin-block:2px;display:-webkit-box;-webkit-line-clamp:2;
  -webkit-box-orient:vertical;overflow:hidden}
.gs-discovery .rationale b{color:var(--text-primary);font-weight:600}
/* J3: cautions line -- separate from ordinary metadata */
.gs-discovery .cautions{display:flex;gap:8px;flex-wrap:wrap;align-items:center;
  margin-block:2px}
/* J4: labeled disclosures replacing chip piles */
.gs-discovery details.mdet{margin-block-start:6px}
.gs-discovery details.mdet>summary{cursor:pointer;font-size:12px;
  color:var(--text-secondary)}
.gs-discovery details.mdet .mrows>*{display:block;margin-block:3px;
  width:fit-content;max-width:100%}
/* J5: checkbox-dropdown filters */
.gs-discovery .dd{position:relative;display:block;margin-block:4px}
.gs-discovery .dd .ddbtn{width:100%;text-align:start;display:flex;
  justify-content:space-between;align-items:center;gap:6px}
/* expands IN PLACE (a floated popover is clipped by the sidebar's own
   scroll container) */
.gs-discovery .dd .ddpop{margin-block-start:2px;max-height:320px;
  overflow:auto;background:var(--bg-card);border:1px solid var(--border-medium);
  border-radius:8px;padding:6px;box-shadow:0 4px 14px rgba(0,0,0,.25)}
.gs-discovery .ddopt{display:flex;gap:8px;align-items:center;padding:4px 6px;
  border-radius:6px;cursor:pointer;font-size:12.5px}
.gs-discovery .ddopt:hover{background:var(--bg-tertiary)}
.gs-discovery .ddopt .c{margin-inline-start:auto;color:var(--text-secondary);
  font-size:11px}
/* J6: global texts toggle */
.gs-discovery .rows.notext .cols,.gs-discovery .rows.notext #txtnote{display:none}
.gs-discovery .rows.notext [id^="txt-"]{display:none}
.gs-discovery .rows.notext [id^="txtnote-"]{display:none}
"""

# ---------------------------------------------------------------------------
# The page shell (spec 1.2, 1.3, 1.4)
# ---------------------------------------------------------------------------

# DIRECTION: the document is dir="ltr" -- this tool has an English-only UI, and
# the layout is never mirrored. Direction is set LOCALLY on DATA only: work
# titles, catalogue titles and the two text panes. The copied CSS uses logical
# properties throughout, so its [dir="rtl"] mirror rules simply never fire.
#
# WHAT IS DELIBERATELY ABSENT (spec 5): the launch headline and its stats band
# (its centrepiece is a contribution CLAIM, and a grading tool that opens with
# the conclusion has pre-registered its answer), the beta line, the mode strip
# with its "Coming soon" pills, the second-pool invitation, the report mailto,
# the admin suppression control, and the bilingual EN/HE UI.
PAGE_HTML = r"""<!doctype html><html lang="en" dir="ltr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>Computed identifications — private review (v5)</title>
<style>__CSS__</style></head><body>

<div class="gs-topbar">
  <input id="q" type="text" placeholder="shelfmark or sys_id contains…" size="22"
         oninput="typed()" onkeydown="if(event.key==='Enter')apply()">
  <button onclick="reset()">Reset</button>
  <button id="textsbtn" onclick="toggleAllTexts()">Hide all texts</button>
  <button onclick="exportGrades()">Export grades</button>
  <button id="themebtn" onclick="toggleTheme()" title="light / dark">◐ dark</button>
  <span class="grow"></span>
  <span id="count">…</span>
</div>

<div class="gs-discovery gs-findings">

  <div class="phead">
    <h1>Computed identifications — private review (v5)</h1>

    <div class="dnote">The point of this tool is the TEXT REUSE itself — read
      the aligned panes. Machine judgments are labels to steer your minutes,
      never verdicts. Dot colours mark a signal&#39;s DIRECTION, not its
      confidence: green = the computed identification looks right here,
      orange = it does not, amber = needs a person, gray = no answer.
      Grading is optional (each row&#39;s &ldquo;grade&rdquo; disclosure).</div>

    <div id="sessionstrip" class="dnote" style="display:none"></div>
  </div>

  <details id="help" class="gs-findings-howto">
    <summary>What these columns mean — read before grading</summary>
    <div class="helpbody"><!--__HELP__--></div>
  </details>

  <div class="body">
    <div class="sidebar" id="sidebar"></div>
    <div class="results">
      <div id="chipbar" class="chipbar"></div>
      <div id="rbar"></div>
      <div id="rows" class="rows"></div>
      <div class="pager">
        <button onclick="prev()">&larr; prev</button>
        <span id="pageno" class="dnote"></span>
        <button onclick="next()">next &rarr;</button>
      </div>
    </div>
  </div>
</div>

<script>__JS__</script></body></html>"""


# ---------------------------------------------------------------------------
# The client
# ---------------------------------------------------------------------------

PAGE_JS = r"""
const DIV = ["catalogue_correct","claim_correct","unclear"];
// Button LABELS only -- the stored values above are the grades-file contract
// and must not move. "claim" confused readers who had just been told about
// claim_type ("span rank"): the grade is about OUR IDENTIFICATION.
const DIV_LABELS = {catalogue_correct: "the catalogue is right",
                    claim_correct: "our identification is right",
                    unclear: "unclear"};
const NULL_TOKEN = "__null__";
const SITE = "__SITE__";
const PREVIEW = "__PREVIEW__";
// whether THIS db carries a current card projection (built by
// scripts/attach_review_cards.py and matching today's review_row and
// registry). False keeps the tool exactly as it was: no toggle, row grain.
const CARDS_OK = __CARDS_OK__;
// true only when htr_page exists AND its per-row stamps agree with their own
// meta count -- a half-run hides the HTR pane rather than showing a stale one
const HTR_OK = __HTR_OK__;
const DOCS = /*__DOCS__*/{};
const NUMS = /*__NUMS__*/{};
// The public page's four grouped views, [key, label] in ITS order, injected
// from NOVELTY_VIEW_LABELS so the two surfaces cannot drift apart in the
// browser after agreeing on the server.
const VIEWS = /*__VIEWS__*/[];
// A JSON-encoded string, not a raw `__TOKEN__` splice: the warning is prose
// with an em dash and could one day gain a quote or an apostrophe, and a splice
// into a literal would then be a syntax error rather than a visible typo.
const VIEW_WARN = /*__VIEWWARN__*/"";
// One threshold, injected from SHORT_MATCH_LETTERS. The row's known-weakness
// prompt and the sidebar's length filter must name the SAME number -- a chip
// that warns at 150 beside a filter that cuts at 100 is two facts.
const SHORT_MATCH = __SHORT__;
const $ = id => document.getElementById(id);
let LAST = [], total = 0, shown = 0, sessionGraded = 0, lastFacets = null;

// QUOTES MUST BE ESCAPED, not just `&` and `<`. This value is interpolated into
// HTML ATTRIBUTES (`<option value="...">`, `title="..."`), and Hebrew titles
// carry gershayim as a plain double quote -- `תנ"ך, תהלים` closed the attribute
// early and the option rendered as `תנ`, silently losing the rest of every such
// title (197 work titles and 6,310 catalogue titles). Escaping only the
// text-content characters is the wrong rule for an attribute.
// COERCE, do not assume a string. `main_pool` arrives from SQLite as the INTEGER
// 1, and `(1 || "")` is the number 1 -- `.replace` does not exist on it, so esc()
// threw and took facets() down with it. Novelty populated first, then pool threw,
// so pool/relation/shown+review silently kept only their "all" option. `?? ""`
// rather than `|| ""` for the same reason: `0` is a real value here and `||`
// would erase it.
const esc = s => String(s ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;")
                        .replace(/>/g,"&gt;").replace(/"/g,"&quot;")
                        .replace(/'/g,"&#39;");
const num = n => Number(n || 0).toLocaleString();

// The reader-facing bucket names are the site's own ("main pool" / "more
// matches"), never "more findings". The second bucket means the evidence did not
// meet the rule -- it is NOT a statement that the identification is wrong.
function poolLabel(id, v){
  if (id === "pool") {
    if (v === NULL_TOKEN || v === null) return "no identification record";
    return String(v) === "1" ? "main pool" : "more matches";
  }
  // THE RELATION comes from the ROUTER -- the only witness-vs-quoter signal
  // gen-2 actually graded, and the axis this tool exists to grade. claim_type is
  // a frozen v1 heuristic about which span is biggest on the page, so it is
  // named for that and never as a relation.
  if (id === "relation") return {same_work:"Witness", parallel:"Quotation",
    not_shipped:"Held back by the router", shared_text:"Shared text"}[v]
    || (v === NULL_TOKEN ? "not routed" : String(v));
  if (id === "claim") return {direct_witness:"Largest span on page",
    quotes_this_work:"Smaller span on page", shared_text:"Shared wording"}[v]
    || String(v);
  if (id === "routing") return {shipped:"shown on site",
    review_only:"review only"}[v] || String(v);
  if (id === "adjudication") return {provisional:"provisional",
    human_confirmed:"human confirmed (earlier pass)"}[v] || String(v);
  return String(v);
}
const RELCARD = {same_work:"Witness — this page is a copy of the work",
  parallel:"Quotation — this page quotes the work",
  not_shipped:"Held back by the router", shared_text:"Shared text"};

// ---- state ---------------------------------------------------------------
// An EMPTY multi-select set means "no restriction", which is how card 1 ships
// with all four boxes ticked without sending four values on every request. A
// selection that empties itself (the reader unticks the last box) resets to
// no-restriction rather than to zero rows: an all-unticked control that shows
// every row would be a control lying about what it did.
// `view` is the public page's grouped novelty selector and `novelty` is this
// tool's raw shade facet. They are SEPARATE keys on purpose: the view is the
// question the public surface asks, the shades are the gate being graded, and
// the server ANDs them, so a view narrows the shade list to its members.
// THE DEFAULT VIEW IS NOT "everything" (owner rulings, 2026-08-30): a grader
// opens onto "probably the work itself" -- the population worth human minutes
// first. The chip shows as a selection and clears normally; Reset returns
// HERE, and the "everything" state is one click away.
const DEFAULT_TRIAGE = ["main"];
const TRIAGE_LABELS = {
  main: "Main pool — witness candidates, most of the page matches",
  citation: "Citation relationship — one quotes the other",
  shared_quotes: "Only shared quotations — near-useless",
  unclear: "Unclear / borderline"};
const TRIAGE_ORDER = ["main", "citation", "shared_quotes", "unclear"];
// LLM adjudication verdicts (gate_verdict_fact) -- shared by the two advanced
// cards and the row chips, so a label can never say two different things.
const GATE_LABELS = {
  catalogue_right_match_is_quotation: "Catalogue right — the match is a quotation",
  catalogue_right_claim_mistaken: "Catalogue right — computed ID mistaken",
  both_right_multiple_works: "Both right — the page carries several works",
  catalogue_too_general: "Catalogue too general — computed ID compatible, more specific",
  computed_right_catalogue_mismatch: "ID right, catalogue entry may not fit — NEEDS HUMAN REVIEW",
  overlapping_works: "Two overlapping works — the matched text cannot decide",
  credible_new_identification: "Credible new identification",
  plausible_needs_expert_check: "Plausible — needs an expert check",
  weak_match_generic_text: "Weak match — generic/shared text",
  actually_recorded: "Actually recorded — an aid already names it",
  wrong_identification: "Wrong identification",
  not_checked: "Model abstained",
};
// short forms for the ROW chips -- the long forms stay on the filter cards
const GATE_SHORT = {
  catalogue_right_match_is_quotation: "catalogue right — quotation",
  catalogue_right_claim_mistaken: "catalogue right — ID mistaken",
  both_right_multiple_works: "both right — multi-work page",
  catalogue_too_general: "catalogue too general",
  computed_right_catalogue_mismatch: "catalogue entry may not fit this page — verify",
  overlapping_works: "two overlapping works — the text cannot decide",
  credible_new_identification: "credible new find",
  plausible_needs_expert_check: "plausible — needs check",
  weak_match_generic_text: "weak — generic text",
  actually_recorded: "already recorded",
  wrong_identification: "wrong ID",
  not_checked: "abstained",
};
const TRIAGE_SHORT = {main: "Main pool", citation: "Citation",
                      shared_quotes: "Shared quotations only", unclear: "Unclear"};
// the two PRIMARY grouped controls (owner, 2026-08-31): "is it new/divergent?"
// and "what does the model say?" -- each group toggles its RAW values, so the
// wire and the Advanced knobs stay untouched.
const NOV_GROUPS = [
  ["New — nothing in the aids identifies it", ["fills_gap", "extends"]],
  ["Diverges from the catalogue", ["diverges_work", "diverges_part"]],
  ["Aligned with the catalogue",
   ["confirms", "aid_more_specific", "container_predicts",
    "refines_granularity", "alias_merge"]],
  ["Not checked", ["not_checked"]],
];
const MODEL_GROUPS = [
  ["Backs the identification",
   ["credible_new_identification", "plausible_needs_expert_check",
    "both_right_multiple_works", "catalogue_too_general"]],
  ["Backs the catalogue / rejects it",
   ["catalogue_right_match_is_quotation", "catalogue_right_claim_mistaken",
    "weak_match_generic_text", "wrong_identification"]],
  ["ID right, catalogue entry may not fit — needs human review",
   ["computed_right_catalogue_mismatch"]],
  ["Two overlapping works — cannot decide", ["overlapping_works"]],
  ["Already recorded after all", ["actually_recorded"]],
  ["Not judged", ["__null__", "not_checked"]],
];
// the FULL vocabularies, static. toggleGroup's set math needs allVals to hold
// every possible value; deriving it from the CURRENT facet broke the control —
// a zero-count value missing from the facet made a fully-selected group read
// as unselected, so a click ADDED values instead of removing them and the
// dropdown snapped back to select-all.
const NOV_ALL = NOV_GROUPS.flatMap(g => g[1]);
const MODEL_ALL = MODEL_GROUPS.flatMap(g => g[1]);
const S = {relation:new Set(), novelty:new Set(), pool:new Set(),
           corpus:new Set(), scripture:new Set(),
           gatediv:new Set(), gatenew:new Set(), model:new Set(),
           triage:new Set(DEFAULT_TRIAGE),
           poolreason:"", claim:"", disagree:false,
           domain:"", author:"", work:"", locus:"", locus_from:"", locus_to:"",
           coverage:"", nontiera:false, adjudicated:false,
           letters:"", graded:"", q:"", view:"all", sort:"work", size:25, off:0,
           // CARD grain (one question per page x known work) or the raw
           // EVIDENCE grain (one row per alignment). Cards are the default
           // when the projection is present; the row grain stays reachable
           // because an offset or a text pane is per-alignment, not per card.
           grain:"card"};
// "" is what clearAxis leaves behind and "all" is what the control ships with;
// both mean the unfiltered state, and neither is ever sent on the wire.
const curView = () => S.view || "all";
const DOPEN = new Set();          // which domain parents are expanded
let OTHER_NOV = false;            // card 4's "other" group expanded
let ADV_OPEN = false;             // the Advanced raw-signals block

function params(extra){
  const p = new URLSearchParams();
  for (const k of ["relation","novelty","pool","corpus","scripture",
                   "gatediv","gatenew","model","triage"])
    for (const v of S[k]) p.append(k, v);
  for (const k of ["poolreason","claim","domain","author","work","locus",
                   "locus_from","locus_to","coverage","graded","q","sort"])
    if (S[k]) p.set(k, S[k]);
  if (curView() !== "all") p.set("view", curView());
  if (S.disagree) p.set("disagree", "1");
  if (S.nontiera) p.set("nontiera", "1");
  if (S.adjudicated) p.set("adjudicated", "1");
  // The three length chips are two BOUNDS on the wire; the endpoint takes any
  // range, so a teammate can ask for a window the sidebar has no chip for.
  if (S.letters === "short") p.set("maxletters", String(SHORT_MATCH - 1));
  else if (S.letters === "long") p.set("minletters", String(SHORT_MATCH));
  Object.entries(extra||{}).forEach(([k,v]) => p.set(k,v));
  return p;
}
function apply(){ load(0); }
function isOn(key, v){ return S[key].size ? S[key].has(v) : true; }
function toggleMulti(key, v, allVals){
  const eff = S[key].size ? new Set(S[key]) : new Set(allVals);
  if (eff.has(v)) eff.delete(v); else eff.add(v);
  S[key] = (eff.size === 0 || eff.size === allVals.length) ? new Set() : eff;
  apply();
}
function setSingle(key, v){ S[key] = (S[key] === v) ? "" : v; apply(); }
// A grouped chip toggles ALL its raw values as one unit -- same set semantics
// as toggleMulti (empty set = everything), so it composes with the raw
// Advanced chips over the same axis instead of fighting them.
function toggleGroup(key, values, allVals){
  const eff = S[key].size ? new Set(S[key]) : new Set(allVals);
  const on = values.every(v => eff.has(v));
  for (const v of values) { if (on) eff.delete(v); else eff.add(v); }
  S[key] = (eff.size === 0 || eff.size === allVals.length) ? new Set() : eff;
  apply();
}
function groupOn(key, values){
  return S[key].size ? values.every(v => S[key].has(v)) : true;
}
function groupCount(facet, values){
  let n = 0, seen = false;
  for (const t of (facet || [])) {
    const key = t[0] === null ? "__null__" : String(t[0]);
    if (values.includes(key)) { n += t[2]; seen = true; }
  }
  return seen ? n : 0;
}

// ---- checkbox-dropdown filters (Codex 2026-08-31, point 1) -----------------
// A closed dropdown shows "label · k of n" so an active hidden filter is
// always visible; open state survives the re-render every apply() causes.
const DD_OPEN = new Set();
function ddToggle(key){
  if (DD_OPEN.has(key)) DD_OPEN.delete(key); else DD_OPEN.add(key);
  if (lastFacets) renderSidebar(lastFacets);
}
document.addEventListener("click", ev => {
  if (!ev.target.closest(".dd") && DD_OPEN.size) {
    DD_OPEN.clear();
    if (lastFacets) renderSidebar(lastFacets);
  }
});
// items: [{on, lab, n, onchange}] -- each row is a checkbox; `onchange` is the
// same toggle the old chip ran, so set semantics are unchanged.
function ddMulti(key, label, items){
  const open = DD_OPEN.has(key);
  const onN = items.filter(i => i.on).length;
  const state = onN === items.length ? "all" : (onN + " of " + items.length);
  return `<div class="dd" data-dd="${key}">
    <button class="fchip ddbtn${onN !== items.length ? " here" : ""}"
      onclick="event.stopPropagation();ddToggle('${key}')"
      aria-expanded="${open}">${esc(label)} · ${state}<span>▾</span></button>
    ${open ? `<div class="ddpop" onclick="event.stopPropagation()">` +
      items.map(i => `<label class="ddopt"><input type="checkbox"
          ${i.on ? "checked" : ""} onchange="${i.onchange}">
        <span>${i.lab}</span>` +
        (i.n === null ? `` : `<span class="c">${num(i.n)}</span>`) +
        `</label>`).join("") + `</div>` : ``}
  </div>`;
}

// ---- global texts toggle (grading is secondary; the texts are the point) ---
let TEXTS_ON = true;
function toggleAllTexts(){
  TEXTS_ON = !TEXTS_ON;
  const r = document.getElementById("rows");
  if (r) r.classList.toggle("notext", !TEXTS_ON);
  const b = $("textsbtn");
  if (b) b.textContent = TEXTS_ON ? "Hide all texts" : "Show all texts";
}

// TYPEABLE FILTERS. `datalist` gives native type-ahead over hundreds of options
// (1,269 works make a plain <select> unusable), but its value is the LABEL, so
// each combo keeps its own label->value map. A label the reader has not matched
// yet is simply not a filter -- never a silent no-match.
const COMBOS = ["author","work"];
const MAP = {author:{}, work:{}};
function picked(k){
  const t = ($(k + "_t").value || "").trim();
  // a From/To range belongs to ONE work; changing or clearing the work
  // clears it, or the range would silently constrain the next work.
  if (k === "work") { S.locus_from = ""; S.locus_to = ""; }
  if (!t) { S[k] = ""; apply(); return; }
  if (t in MAP[k]) { S[k] = MAP[k][t]; apply(); }   // only query on a real pick
}
// Choosing a filter APPLIES it. An Apply button made every change a two-step
// action and, worse, let the controls show a state the rows below did not match.
// The text box debounces so a shelfmark search does not fire a query per keystroke.
let typeTimer = null;
function typed(){
  clearTimeout(typeTimer);
  typeTimer = setTimeout(() => { S.q = $("q").value.trim(); apply(); }, 300);
}
let locusTimer = null;
function locusTyped(){
  clearTimeout(locusTimer);
  locusTimer = setTimeout(() => {
    S.locus = $("locus_t").value.trim(); apply();
  }, 300);
}
function setLocusRange(which, v){
  if (which === "from") S.locus_from = v; else S.locus_to = v;
  apply();
}
function setCoverage(v){ S.coverage = (S.coverage === v) ? "" : v; apply(); }
function reset(){
  // Back to the DEFAULT view ("probably the work itself"), not to
  // "everything" -- Reset means "as I opened it".
  S.relation = new Set(); S.novelty = new Set(); S.pool = new Set();
  S.corpus = new Set(); S.scripture = new Set();
  S.gatediv = new Set(); S.gatenew = new Set(); S.model = new Set();
  S.triage = new Set(DEFAULT_TRIAGE);
  S.poolreason = ""; S.claim = ""; S.disagree = false; S.domain = "";
  S.author = ""; S.work = ""; S.locus = ""; S.locus_from = ""; S.locus_to = "";
  S.coverage = ""; S.nontiera = false; S.adjudicated = false;
  S.letters = ""; S.graded = ""; S.q = ""; S.view = "all";
  S.sort = "work"; S.off = 0;
  $("q").value = "";
  const lc = $("locus_t"); if (lc) lc.value = "";
  for (const k of COMBOS) { const el = $(k + "_t"); if (el) el.value = ""; }
  load(0);
}
function toggleTheme(){
  const cur = document.documentElement.getAttribute("data-theme") === "dark";
  setTheme(cur ? "light" : "dark");
}
function setTheme(t){
  document.documentElement.setAttribute("data-theme", t);
  try { localStorage.setItem("v3review.theme", t); } catch (e) {}
  const b = $("themebtn");
  if (b) b.textContent = t === "dark" ? "◐ light" : "◐ dark";
}

// ---- facets --------------------------------------------------------------
let facetSeq = 0;
async function facets(){
  // A failed or SUPERSEDED facets fetch must not blank the controls. Previously
  // any error here threw out of facets(), every control kept only its default
  // option, and nothing said why. The sequence guard also stops a slow early
  // response from overwriting a newer one.
  const mine = ++facetSeq;
  let f;
  try {
    const r = await fetch("/api/facets?" + params());
    if (!r.ok) throw new Error("HTTP " + r.status);
    f = await r.json();
    if (f.error) throw new Error(f.error.cls + " in " + f.error.query);
  } catch (e) {
    $("count").textContent += "  · filter lists unavailable (" + e.message + ")";
    return;
  }
  if (mine !== facetSeq) return;
  lastFacets = f;
  renderSidebar(f);
  renderChipBar(f);
}

const L = (list) => (list || []).map(t => [t[0], t[1], t[2]]);
const findN = (list, v) => {
  const hit = (list || []).find(t => String(t[0]) === String(v));
  return hit ? hit[2] : 0;
};
function card(cls, header, bodyHtml, prose){
  // The explanation is REAL but collapsed (owner, 2026-08-31: paragraphs on
  // every filter made the sidebar unreadable). One click on "what is this?"
  // opens it; nothing is lost, nothing is inline by default.
  return `<div class="fg ${cls}">
    <div class="gs-findings-card-header">${header}</div>
    ${bodyHtml}
    ${prose ? `<details class="whyd"><summary class="dnote"
        style="cursor:pointer">what is this?</summary>
        <div class="dnote">${prose}</div></details>` : ``}</div>`;
}
function chipBtn(on, label, count, onclick, title){
  return `<button class="fchip${on?" here":""}" aria-pressed="${on?"true":"false"}"
    onclick="${onclick}"${title?` title="${esc(title)}"`:``}>${label}` +
    (count === null ? `` : `<span class="c">${num(count)}</span>`) + `</button>`;
}

function renderSidebar(f){
  const relVals = ["same_work","parallel","not_shipped","shared_text"];
  const out = [];
  const adv = [];   // the raw signals, collapsed under "Advanced" at the end

  // In CARD grain the counts beside each control still count EVIDENCE ROWS,
  // and a card can hold several. Making them count cards means a
  // COUNT(DISTINCT card_id) per axis: measured at 4.1 s for one axis over
  // 519,382 rows, so ~15 axes would take a minute and the browser would
  // cancel the response -- which leaves every control empty with nothing
  // saying why. The list header above carries the EXACT card total, so the
  // honest fix is to say which grain these numbers are in rather than to
  // approximate them.
  if (CARDS_OK && S.grain === "card")
    out.push(`<div class="dnote" style="padding:0 2px 4px">The counts beside
      each control below count <b>evidence rows</b>; the header above counts
      <b>cards</b>. A card holds one or more evidence rows, so the two differ
      wherever a work has several witnesses on one page.</div>`);

  // CARD 0 -- THE POOLS (owner, 2026-08-30): the one sorting every other
  // control was circling. Bucket names deliberately claim nothing; the rule
  // is deterministic, recorded in the file, and shown in the tooltip.
  out.push(card("relgrp", "The pools — where should your minutes go?",
    `<div class="stack">` + TRIAGE_ORDER.map(v => chipBtn(isOn("triage", v),
        esc(TRIAGE_LABELS[v]), findN(f.triage, v),
        `toggleMulti('triage','${v}',${JSON.stringify(TRIAGE_ORDER).replace(/"/g,"&quot;")})`,
        DOCS["doc.triage"] || "")).join("") + `</div>`,
    "A sorting rule over the signals below — the router's relation, the " +
    "shared-scripture detectors, and page coverage — never a verdict. The " +
    "main pool demands that nearly the WHOLE page match the work (≥85%): a page " +
    "mostly unmatched is probably not a copy, however real the match. " +
    "&lsquo;Shared quotations&rsquo; means the matched text is a third text " +
    "both sides quote — near-useless. The raw signals stay available under " +
    "&lsquo;Advanced&rsquo;."));

  // CARD 0b -- "is it new or divergent?" (owner, 2026-08-31): the grouped
  // catalogue-relationship control. Each chip toggles its RAW novelty shades,
  // so it composes with the full ten-shade card under Advanced.
  out.push(card("", "Vs the catalogue — is it new or divergent?",
    ddMulti("novelty", "Vs the catalogue", NOV_GROUPS.map(([lab, vals]) => ({
      on: groupOn("novelty", vals), lab: esc(lab),
      n: groupCount(f.novelty, vals),
      onchange: `toggleGroup('novelty',${JSON.stringify(vals).replace(/"/g,"&quot;")},NOV_ALL)`}))),
    "Groups of the ten raw novelty shades (all ten stay under Advanced). " +
    "&lsquo;New&rsquo; means none of the checked finding aids identify this " +
    "fragment; &lsquo;diverges&rsquo; means an aid names a DIFFERENT work; " +
    "&lsquo;aligned&rsquo; covers confirms and the granularity variants."));

  // CARD 0c -- "what does the model say?" One control over BOTH gates'
  // verdicts (they ran on disjoint populations, so a row has at most one);
  // the per-task raw cards stay under Advanced.
  out.push(card("", "What does the model say?",
    ddMulti("model", "Model's reading", MODEL_GROUPS.map(([lab, vals]) => ({
      on: groupOn("model", vals), lab: esc(lab),
      n: groupCount(f.model, vals),
      onchange: `toggleGroup('model',${JSON.stringify(vals).replace(/"/g,"&quot;")},MODEL_ALL)`}))),
    "The LLM adjudication (2026-08-31, main+unclear pairs): for divergent " +
    "pairs, who is right — the catalogue or the computed identification; for " +
    "unrecorded pairs, is the proposed NEW identification credible. Verdicts " +
    "are labels, never pool gates. &lsquo;Claims the catalogue is wrong&rsquo; " +
    "went 0-for-4 when the owner contested it — treat as a review queue, not " +
    "a finding. Each row shows the model's reason under its chips; the " +
    "per-verdict raw controls are under Advanced."));

  // CARD 1 -- the router's own axis.
  // Its labels say witness and quotes in full sentences precisely so it cannot
  // be confused with card 2, whose labels contain neither word.
  adv.push(card("",
    "Relation — is this page a copy of the work, or does it quote it?",
    ddMulti("relation", "Relation", relVals.map(v => ({
      on: isOn("relation", v), lab: esc(RELCARD[v]), n: findN(f.relation, v),
      onchange: `toggleMulti('relation','${v}',${JSON.stringify(relVals).replace(/"/g,"&quot;")})`}))),
    "The only witness-vs-quotation axis that was validated — ~1,400 blind cards " +
    "plus 400 more, graded by hand. Decided by how much of the page the match covers."));

  // CARD 2 -- span rank. NOT a relation, and labelled so it cannot be read as one.
  adv.push(card("", "Span rank — NOT a relation",
    `<div class="stack">` +
    chipBtn(!S.claim, "Any", null, `setClaim('')`) +
    chipBtn(S.claim === "direct_witness", "Largest span on page",
            findN(f.claim, "direct_witness"), `setClaim('direct_witness')`,
            DOCS["doc.claim_type"]) +
    chipBtn(S.claim === "quotes_this_work", "Smaller span on page",
            findN(f.claim, "quotes_this_work"), `setClaim('quotes_this_work')`,
            DOCS["doc.claim_type"]) +
    chipBtn(S.disagree, "Only rows where span rank disagrees with the router",
            f.disagree_n, `toggleDisagree()`) + `</div>`,
    "Says only which matched span is largest on this page. No minimum length, " +
    "never reads the text, and a page with a single match gets &lsquo;largest&rsquo; " +
    "by default however short. Shown so you can see where it disagrees with the router."));

  // CARD 2a -- page coverage. The router's own quantity, as a filter: 29.8%
  // page coverage is the validated witness threshold, so 30-40% is the
  // "barely a witness" band -- the population where a quotation of a long
  // passage most often slips over the line (the Harkavy-responsum case).
  const covVals = [["lt10", "under 10%"], ["10to30", "10–30%"],
                   // the band the handoff note calls the population most worth
                   // suspicion had no control of its own, so the note pointed at
                   // something the reader could not select
                   ["30to40", "30–40% — where a long quotation slips over the line"],
                   ["30to60", "30–60% — over the router's witness line"],
                   ["60to85", "60–85% — under the main-pool bar"],
                   ["ge85", "85%+ — main-pool grade"]];
  adv.push(card("", "Page coverage — how much of the page the match covers",
    `<div class="stack">` +
    chipBtn(!S.coverage, "Any", null, `setCoverage('')`) +
    covVals.map(([v, lab]) => chipBtn(S.coverage === v, esc(lab), null,
      `setCoverage('${v}')`)).join("") + `</div>`,
    "The quantity the lines are drawn on: the router calls a witness at " +
    "≥29.8%; the main pool starts at 85% — 75% for R-source, whose " +
    "letter-exact coverage against printed editions tops out at 83.5%, so " +
    "the 85%+ bucket itself never holds R-source rows."));

  // CARD 2b -- shared scripture. Computed by scripts/attach_scripture_facts.py;
  // on a db without the table every row is "not computed" and the card says so.
  const scrVals = ["1","0",NULL_TOKEN];
  const scrLabel = v => v === "1"
      ? "Flagged — may rest on text both sides quote"
      : (v === "0" ? "Checked, not flagged" : "Not computed");
  adv.push(card("", "Shared scripture — is the matched TEXT itself scripture?",
    ddMulti("scripture", "Shared scripture", scrVals.map(v => ({
      on: isOn("scripture", v), lab: esc(scrLabel(v)), n: findN(f.scripture, v),
      onchange: `toggleMulti('scripture','${v}',${JSON.stringify(scrVals).replace(/"/g,"&quot;")})`}))),
    "A DIFFERENT question from the Relation card: the relation says how this " +
    "page relates to this work (by page coverage — witness or quotation); " +
    "this asks whether the matched text is a THIRD text both could be " +
    "quoting independently — a verse, a mishnah. A &lsquo;quotation&rsquo; " +
    "relation can be genuine while this is clean, and a &lsquo;witness&rsquo; " +
    "row can still be flagged. Detectors: the matched text found verbatim in " +
    "scripture; a citation formula at the boundary of a short match; a span " +
    "mostly inside quotations the pre-matching mask caught. Each row's chip " +
    "names which fired. Computed for EVERY corpus except works that are " +
    "themselves canonical scripture (Bible, Mishnah, Talmud, Tosefta…) — " +
    "there a verbatim-scripture span IS the identification, and those rows " +
    "read &lsquo;not computed&rsquo;. The triage card above already folds this " +
    "flag in; use these chips to work with the raw signal itself."));

  // CARD 2c -- the LLM adjudication verdicts (gate_verdict_fact; absent on a
  // db where the attach script never ran -- every row is then "not judged").
  const gdivVals = ["catalogue_right_match_is_quotation",
                    "catalogue_right_claim_mistaken", "both_right_multiple_works",
                    "catalogue_too_general", "overlapping_works",
                    "computed_right_catalogue_mismatch", NULL_TOKEN];
  const gnewVals = ["credible_new_identification", "plausible_needs_expert_check",
                    "weak_match_generic_text", "actually_recorded",
                    "wrong_identification", NULL_TOKEN];
  const gateLabel = v => GATE_LABELS[v] || (v === NULL_TOKEN ? "Not judged" : v);
  adv.push(card("", "Model adjudication — catalogue vs computed (divergent pairs)",
    ddMulti("gatediv", "Raw divergence verdicts", gdivVals.map(v => ({
      on: isOn("gatediv", v), lab: esc(gateLabel(v)), n: findN(f.gatediv, v),
      onchange: `toggleMulti('gatediv','${v}',${JSON.stringify(gdivVals).replace(/"/g,"&quot;")})`}))),
    DOCS["doc.gate_divergence"] || ""));
  adv.push(card("", "Model check — candidate NEW identifications",
    ddMulti("gatenew", "Raw new-find verdicts", gnewVals.map(v => ({
      on: isOn("gatenew", v), lab: esc(gateLabel(v)), n: findN(f.gatenew, v),
      onchange: `toggleMulti('gatenew','${v}',${JSON.stringify(gnewVals).replace(/"/g,"&quot;")})`}))),
    DOCS["doc.gate_new_finds"] || ""));

  // CARD 3 -- pool. THREE states plus All; the third is the never-evaluated block.
  const poolVals = ["1","0",NULL_TOKEN];
  let poolBody = `<div class="stack">` + poolVals.map(v => chipBtn(isOn("pool", v),
      esc(poolLabel("pool", v === NULL_TOKEN ? NULL_TOKEN : v)), findN(f.pool, v),
      `toggleMulti('pool','${v}',${JSON.stringify(poolVals).replace(/"/g,"&quot;")})`,
      DOCS["doc.main_pool"])).join("") + `</div>`;
  // The nested control appears ONLY when "more matches" is the sole selection --
  // a demotion reason has no meaning for a row that was never demoted.
  if (S.pool.size === 1 && S.pool.has("0")) {
    const reasons = (f.poolreason || []).filter(t =>
        t[0] && !String(t[0]).startsWith("main_"));
    poolBody += `<div class="gs-findings-card-header" style="margin-block-start:6px">
      Why it was demoted</div><select onchange="setReason(this.value)">
      <option value="">any reason</option>` + reasons.map(t =>
        `<option value="${esc(t[0])}"${S.poolreason===String(t[0])?" selected":""}>` +
        `${esc(t[0])} (${num(t[2])})</option>`).join("") + `</select>`;
  }
  adv.push(card("", "Public-site display rule (its own ‘main pool’)", poolBody,
    "&lsquo;More matches&rsquo; means the evidence did not meet the rule — " +
    "not that the identification is wrong. &lsquo;No identification record&rsquo; " +
    "is a third state: the rule was never evaluated. <code>shared_wording</code> " +
    "and <code>insufficient_length</code> are the population the known-weakness " +
    "note tells you to distrust."));

  // CARD 4 -- THE PUBLIC PAGE'S GROUPED VIEW, immediately above the raw shade
  // facet it narrows. A <select>, like the public control and for its reasons:
  // four prose labels do not fit a chip row, and a reader sees all four at once
  // instead of discovering them by clicking.
  //
  // NO COUNTS ON THE OPTIONS -- also the public page's rule (a number inside a
  // filter reads as a finding). The counts a grader needs are right below, on
  // the shades themselves, and they already narrow to the chosen view.
  adv.push(card("", "Which findings — the public page's grouping",
    `<select onchange="setView(this.value)">` + VIEWS.map(t =>
      `<option value="${esc(t[0])}"${curView()===String(t[0])?" selected":""}>` +
      `${esc(t[1])}</option>`).join("") + `</select>`,
    "Exactly the four groups /computed-identifications offers, from the same " +
    "source of truth — so &lsquo;what does the public page show here?&rsquo; is " +
    "one click, not a hand-union of chips. It composes with the shades below: " +
    "choosing a view leaves only that view&rsquo;s shades in the next card. " +
    esc(VIEW_WARN)));

  // CARD 5 -- novelty. ALL ten raw shade names, never the public page's gated
  // subset: the grader is grading the gate, and hiding eight of its outputs
  // makes it ungradeable.
  const novAll = L(f.novelty).map(t => String(t[0]));
  const novBody = ddMulti("novelty_raw", "Raw novelty shades",
    L(f.novelty).map(t => ({
      on: isOn("novelty", String(t[0])), lab: esc(String(t[0])), n: t[2],
      onchange: `toggleMulti('novelty','${t[0]}',${JSON.stringify(novAll).replace(/"/g,"&quot;")})`})));
  adv.push(card("", "What this adds to the catalogue — all ten raw shades", novBody,
    "<code>not_checked</code> is an honest &lsquo;no answer&rsquo;, never a guess — and " +
    num(NUMS.never_evaluated) + " of its " + num(NUMS.not_checked) +
    " rows are the never-evaluated block, i.e. the rule never ran."));

  // CARD 6 -- the domain tree, public shape exactly.
  out.push(card("", "Domain of the identified work", domainTree(f.domains), ""));

  // CARD 7 -- author, normalised. See author_key() in the server: one person
  // appeared as up to three separate entries and picking one hid 82% of them.
  out.push(card("", "Author", combo("author", "author: all", f.authors),
    "Surface spellings of one person are merged into a single entry; " +
    "the filter still matches every form."));

  // CARD 8 -- work, keyed on work_id. 43 titles are shared by more than one
  // work_id, so a title alone does not name a work. When a work is chosen,
  // the Part-of-work From/To appears below it -- the live app's control, fed
  // by this work's loci in stream order (f.loci from the server).
  let workBody = combo("work", "work: all", f.works);
  if (S.work && (f.loci || []).length > 1) {
    const opt = (sel) => `<option value=""></option>` + f.loci.map(l =>
      `<option value="${esc(l)}"${sel === l ? " selected" : ""}>${esc(l)}</option>`).join("");
    workBody += `<div class="gs-findings-card-header" style="margin-block-start:6px">
        Part of work</div>
      <div class="side gap-2">
        <select dir="auto" style="max-width:47%" onchange="setLocusRange('from', this.value)">
          ${opt(S.locus_from)}</select>
        <select dir="auto" style="max-width:47%" onchange="setLocusRange('to', this.value)">
          ${opt(S.locus_to)}</select>
      </div>
      <div class="dnote">From … to, in the order the work runs. Leave one side
        empty for an open range.</div>`;
  }
  out.push(card("", "Work", workBody, ""));

  // CARD 8b -- locus contains. A plain substring over the citable address:
  // type a tractate, a chapter, a siman. No wildcards, no surprises.
  out.push(card("", "Locus — the citable address",
    `<input id="locus_t" dir="auto" placeholder="locus contains…"
       value="${esc(S.locus)}" oninput="locusTyped()">`,
    "Matches anywhere inside the address — e.g. ברכות, פרק ג, or a folio."));

  // CARD 9 -- reference corpus, ALWAYS through the redaction map.
  const corpVals = L(f.corpus).map(t => String(t[0]));
  out.push(card("", "Reference corpus",
    `<div class="stack">` + L(f.corpus).map(t => chipBtn(isOn("corpus", t[0]),
      esc(t[1]), t[2],
      `toggleMulti('corpus','${t[0]}',${JSON.stringify(corpVals).replace(/"/g,"&quot;")})`
      )).join("") + `</div>`, ""));

  // CARD 10 -- escape hatches, all off by default.
  const gradeVals = [["","any"],["no","ungraded only"],["yes","graded only"]];
  // Match length. The row already warns that a match under SHORT_MATCH letters
  // may rest on shared scripture; this is the control that lets the grader ASK
  // for that population instead of paging a sort to its end. Both directions,
  // because "skip the short ones" is the other half of the same working day.
  const lenVals = [["","Any"],
                   ["short", "Short matches only — under " + SHORT_MATCH + " letters"],
                   ["long", SHORT_MATCH + " letters or more"]];
  adv.push(card("", "Narrow further",
    `<div class="stack">` +
    chipBtn(S.nontiera, "Only bands below the top confidence tier", f.nontiera_n,
            `toggleFlag('nontiera')`) +
    chipBtn(S.adjudicated, "Only rows a person already looked at", f.adjudicated_n,
            `toggleFlag('adjudicated')`,
            "The earlier adjudication pass. Grades entered here do not update it.") +
    `<div class="dnote">"Already looked at" is an EARLIER review pass, frozen —
      a different process from the grading you do here, and your grades do not
      update it.</div>` +
    `<div class="gs-findings-card-header" style="margin-block-start:6px">Match length</div>` +
    lenVals.map(([v,lab]) => chipBtn(S.letters === v, esc(lab),
        v === "" ? null : (v === "short" ? f.short_n : f.long_n),
        `setLetters('${v}')`,
        v === "short" ? DOCS["doc.known_weakness"] : "")).join("") +
    `<div class="gs-findings-card-header" style="margin-block-start:6px">Grading</div>` +
    gradeVals.map(([v,lab]) => chipBtn(S.graded === v, lab,
        v === "" ? f.graded_total_here : (v === "yes" ? f.graded_n : f.ungraded_n),
        `setGraded('${v}')`)).join("") + `</div>`, ""));

  // The raw signals behind the triage, one collapsed block. `ADV_OPEN` keeps
  // the reader's open/closed choice across re-renders (every filter click
  // rebuilds the sidebar; a details element that snapped shut each time would
  // be unusable).
  out.push(`<details class="gs-findings-howto"${ADV_OPEN ? " open" : ""}
      ontoggle="ADV_OPEN=this.open">
    <summary>Advanced — the raw signals behind the triage</summary>
    ${adv.join("")}</details>`);
  $("sidebar").innerHTML = out.join("");
  fillCombo("author", f.authors);
  fillCombo("work", f.works);
}
// VALIDATED, not trusted, exactly as the public handler does it: an unknown
// value widens to "all" rather than reaching the server. (The server widens
// too -- this only keeps the control's own label honest.)
function setView(v){
  S.view = VIEWS.some(t => String(t[0]) === String(v)) ? String(v) : "all";
  apply();
}
function setClaim(v){ S.claim = v; apply(); }
function setReason(v){ S.poolreason = v; apply(); }
function setGraded(v){ S.graded = v; apply(); }
function setLetters(v){ S.letters = v; apply(); }
function toggleDisagree(){ S.disagree = !S.disagree; apply(); }
function toggleFlag(k){ S[k] = !S[k]; apply(); }
function toggleOtherNov(){ OTHER_NOV = true; if (lastFacets) renderSidebar(lastFacets); }

function combo(k, ph, list){
  return `<span class="combo"><input id="${k}_t" type="text" list="${k}_l"
     placeholder="${esc(ph)}" oninput="picked('${k}')"><datalist id="${k}_l"></datalist></span>`;
}
function fillCombo(k, list){
  const el = $(k + "_l");
  if (!el) return;
  MAP[k] = {};
  const seen = {};
  // NO SILENT CAP. An earlier version stopped at 400 of 1,269 works with nothing
  // saying so -- a filter that could not reach a third of its own domain. Facet
  // lists are small (tens to low thousands), so they are returned whole.
  el.innerHTML = L(list).map(([v, lab, n]) => {
    let label = String(lab || v || "(none)");
    if (seen[label]) label += "  · " + v;      // keep duplicates distinct
    seen[label] = 1;
    MAP[k][label] = v;
    return `<option value="${esc(label)}">${num(n)}</option>`;
  }).join("");
  const cur = $(k + "_t");
  if (cur && !cur.value && S[k]) {
    const back = Object.keys(MAP[k]).find(lab => String(MAP[k][lab]) === String(S[k]));
    if (back) cur.value = back;
  }
}

// Parent selection is a STRICT SUPERSET (the server does
// `domain = ? OR domain LIKE ? || ' / %'`). All 61 leaves carry ' / ', so the
// parent split is uniform. Chevron is a SEPARATE round button using a vertical
// glyph, so nothing flips for RTL.
function domainTree(list){
  const parents = new Map();
  let nullN = null;
  for (const [v, lab, n] of L(list)) {
    if (v === NULL_TOKEN) { nullN = n; continue; }
    const s = String(v), i = s.indexOf(" / ");
    const p = i < 0 ? s : s.slice(0, i);
    const tail = i < 0 ? s : s.slice(i + 3);
    if (!parents.has(p)) parents.set(p, {n:0, leaves:[]});
    const e = parents.get(p);
    e.n += n; e.leaves.push([s, tail, n]);
  }
  const rows = [];
  for (const [p, e] of [...parents.entries()].sort((a,b) => b[1].n - a[1].n)) {
    const selHere = S.domain === p;
    const inBranch = selHere || (S.domain && S.domain.startsWith(p + " / "));
    const open = DOPEN.has(p) || inBranch;
    rows.push(`<div class="dnode">
      <button class="n fchip${selHere?" here":""}" aria-pressed="${selHere?"true":"false"}"
        onclick="pickDomain('${escJs(p)}')" title="${esc(p)}">${esc(p)}
        <span class="c">${num(e.n)}</span></button>
      <button class="chev fchip" aria-label="${open?"Collapse":"Expand"}"
        onclick="toggleBranch('${escJs(p)}')">${open?"⌃":"⌄"}</button></div>`);
    if (open) {
      for (const [full, tail, n] of e.leaves.sort((a,b) => b[2] - a[2])) {
        const on = S.domain === full;
        rows.push(`<div class="dnode leaf"><button class="n fchip${on?" here":""}"
          aria-pressed="${on?"true":"false"}" onclick="pickDomain('${escJs(full)}')"
          title="${esc(full)}">${esc(tail)}<span class="c">${num(n)}</span></button></div>`);
      }
    }
  }
  if (nullN !== null) {
    const on = S.domain === NULL_TOKEN;
    rows.push(`<div class="dnode"><button class="n fchip${on?" here":""}"
      aria-pressed="${on?"true":"false"}" onclick="pickDomain('${NULL_TOKEN}')">
      no domain recorded<span class="c">${num(nullN)}</span></button></div>`);
  }
  return `<div class="dtree">${rows.join("")}</div>`;
}
// A value going into a single-quoted JS string inside an HTML attribute passes
// through TWO decoders, so it needs both escapes: the backslash for the JS
// string and esc() for the attribute. Domain names are ASCII here, but a name
// with an apostrophe would otherwise break the handler silently.
function escJs(s){ return esc(String(s).replace(/\\/g,"\\\\").replace(/'/g,"\\'")); }
function pickDomain(v){ S.domain = (S.domain === v) ? "" : v; apply(); }
function toggleBranch(p){
  if (DOPEN.has(p)) DOPEN.delete(p); else DOPEN.add(p);
  if (lastFacets) renderSidebar(lastFacets);
}
// ---- the row (spec 2) ----------------------------------------------------
function chip(cls, text, title){
  return `<span class="${cls}"${title?` title="${esc(title)}"`:``}>${esc(text)}</span>`;
}
// The LIVE site's bare viewer. `embed=1` is the route built for exactly this --
// no site chrome, and it does NOT persist/restore browse state, so previewing
// here cannot overwrite wherever the reader left /browse in their own tab.
// ABSOLUTE, never relative: browse_url() in the app returns `/browse?...`, which
// on 127.0.0.1 resolves to a local 404.
// page and volume_ie travel TOGETHER or not at all: a folio number without its
// volume is a DIFFERENT folio in each volume of a multi-volume manuscript, so a
// half address looks targeted and lands somewhere else.
function browseUrl(x, embed){
  if (!x.sys_id) return null;
  let u = SITE + "/browse?sys_id=" + encodeURIComponent(x.sys_id);
  if (embed) u += "&embed=1";
  if (x.page_num && x.volume_ie)
    u += "&page=" + x.page_num + "&volume_ie=" + encodeURIComponent(x.volume_ie);
  return u;
}
// The image endpoint needs NO session, no cookie and no client script, so an
// <img> has none of the iframe's five consequences. It is NLI-routed, so Oxford
// manuscripts 404 on it -- and `onerror` is a reliable signal, which is exactly
// what the iframe could never give us.
function imageUrl(x){
  return SITE + "/api/nli_image_by_sysid/" + encodeURIComponent(x.sys_id) +
         "?page=" + encodeURIComponent(x.page_num || 0);
}

// which primary group a raw novelty shade belongs to -- the slot's big label
function novGroupOf(shade){
  if (shade === "fills_gap" || shade === "extends") return "New";
  if (shade === "diverges_work" || shade === "diverges_part") return "Diverges";
  if (shade === "not_checked" || !shade) return "Not checked";
  return "Aligned";
}

// ---- the CARD grain ------------------------------------------------------
// A card states what it knows and REFUSES to average what it does not: a
// column whose rows disagree reads "mixed" (the builder wrote it that way),
// and a card whose rows carry different loci shows how many rather than
// picking one. The witness strip is the honesty surface: a witness with no
// evidence on this page reads "no returned alignment", never "not applicable"
// -- that stronger claim is made ONLY where the projection could prove it.
const WIT_LABEL = {aligned: "aligned here",
                   no_returned_alignment: "no returned alignment",
                   not_applicable: "not applicable here"};
function witHtml(w){
  const on = w.status === "aligned";
  const scope = w.scope === "whole" ? "whole work" : w.scope;
  return `<span class="wit${on ? " on" : ""}"${w.why ? ` title="${esc(w.why)}"` : ``}>
    ${esc(scope)} <span class="sc">· ${esc(WIT_LABEL[w.status] || w.status)}` +
    (on ? ` (${num(w.rows_here)})` : ``) + `</span></span>`;
}
// the model's answers across a card's rows, summarised WITHOUT a vote: one
// verdict if they agree, otherwise every verdict with its count
function verdictMix(rows){
  const c = {};
  rows.forEach(r => { const v = r.gate_divergence || r.gate_new_finds;
                      if (v) c[v] = (c[v] || 0) + 1; });
  const keys = Object.keys(c);
  if (!keys.length) return ``;
  const one = keys.length === 1;
  return `<span class="dnote" title="An LLM's unverified answers for this ` +
    `card's evidence rows.">Model: ` +
    (one ? esc(GATE_SHORT[keys[0]] || keys[0])
         : "mixed — " + keys.map(k =>
             `${c[k]}× ${esc(GATE_SHORT[k] || k)}`).join(", ")) +
    `</span>`;
}
function kwCardHtml(c){
  const rows = c.rows || [];
  // A CARD MUST EARN ITS FRAME. For 327,058 cards (75.4%) the known work has
  // exactly one witness and the page has one row from it, so every line a card
  // would add -- title, shelfmark, pool, verdict, "1 of 1 witness aligned" --
  // is already on the row beneath it, and the reader sees one box inside
  // another saying the same thing. Those render as the row alone. The card
  // stays wherever it says something the row cannot: several rows to
  // aggregate, or a witness of this work that did NOT align here.
  if (rows.length === 1 && c.kw_witnesses === 1) {
    const differs = c.kw_title && c.kw_title !== rows[0].work_title;
    const soloBasis = c.kw_title_basis && c.kw_title_basis !== "singleton";
    if (!differs && !c.provisional && !soloBasis) return rowHtml(rows[0]);
    // the two things a bare card can still be needed for
    const head = [];
    if (differs)
      head.push(`<div class="kwnums">known work:
        <b dir="auto" style="unicode-bidi:isolate">${esc(c.kw_title)}</b></div>`);
    if (c.provisional)
      head.push(chip("needs", "provisional identity",
        "This known work was minted from a routing the owner has not yet " +
        "ruled on; its name may change."));
    return `<div class="kwcard">${head.join("")}${rowHtml(rows[0])}</div>`;
  }
  const mix = v => v === "mixed"
    ? `<b title="This card's evidence rows disagree; neither value is the card's answer.">mixed</b>`
    : esc(v || "");
  const locus = c.locus_label
    ? esc(c.locus_label)
    : (c.locus_variants > 1
        ? `<span title="Its evidence rows cite different addresses, so the card states none.">${c.locus_variants} addresses</span>`
        : `—`);
  const prov = c.provisional
    ? chip("needs", "provisional identity",
           "This known work was minted from a routing the owner has not yet " +
           "ruled on; its name may change.")
    : ``;
  const author = c.kw_author ? ` · ${esc(c.kw_author)}` : ``;
  // with one row, the shelfmark/folio/locus line below is the row's own line
  // repeated a box further out; the identity and the witness count are what
  // the card contributes
  // the card-level shelfmark can be null while a member row carries it (1,584
  // of 2,021 such cards) -- fall back the way the row path already does
  const shelf = c.shelfmark || (rows.find(r => r.shelfmark) || {}).shelfmark
                || c.sys_id || "";
  const where = rows.length === 1 ? `` : `<div class="kwnums">${esc(shelf)}
          · folio ${esc(rows.length ? (rows[0].page_num || "?") : "?")}
          · ${locus}</div>`;
  const head = `<div class="kwhead">
      <div><div class="kwtitle">${esc(c.kw_title || "")}${author}</div>
        ${where}</div>
      <div class="kwnums">${num(c.evidence_rows)} evidence
        ${c.evidence_rows === 1 ? "row" : "rows"} ·
        ${num(c.witnesses)} of ${num(c.kw_witnesses)}
        ${c.kw_witnesses === 1 ? "witness" : "witnesses"} aligned</div>
    </div>`;
  // the aggregate line is for AGGREGATES: with a single row it would only
  // restate that row's own pool/verdict chips, one box further out
  // THE POOL A CARD SHOWS IS ITS OWN ROWS' POOL. It used to come from
  // review_row.main_pool -- the public site's display rule -- while every row
  // chip below showed facet_row.triage, the four pools this tool sorts by. On
  // 112,844 cards those two disagreed, and the card's "pool: yes" sat directly
  // above chips reading "Unclear". Deriving it from the rows makes disagreement
  // impossible, and the site's own rule is named separately where it appears.
  const pools = [...new Set(rows.map(r => r.triage).filter(Boolean))];
  const poolTxt = !pools.length ? `\u2014`
    : pools.length === 1 ? esc(TRIAGE_SHORT[pools[0]] || pools[0])
    : `<b title="This card's evidence rows fall in different pools; the rows below say which.">mixed</b> \u2014 `
      + pools.map(t => esc(TRIAGE_SHORT[t] || t)).join(", ");
  // what DECIDED this identity: an owner ruling for this artifact reads very
  // differently from the production reference contract, and both used to render
  // as nothing at all
  const BASIS_LABEL = {
    owner_merge: "identity: owner ruling (this artifact)",
    work_group: "identity: owner ruling (two halves are one work)",
    census_canonical: "identity: production reference contract",
    cluster: "identity: cross-corpus same-work link",
    family: "identity: container file and its parts",
    mint: "identity: minted from a division name",
  };
  const basis = (c.kw_title_basis && c.kw_title_basis !== "singleton")
    ? chip("chip", BASIS_LABEL[c.kw_title_basis] || ("identity: " + c.kw_title_basis),
           "How this known work's identity was decided. An owner ruling applies "
           + "to this review artifact; the production contract is what the "
           + "public corpus already says.")
    : ``;
  const summary = rows.length === 1 && !basis ? `` : `<div class="side items-center gap-2 flex-wrap kwnums">
      <span>pool: ${poolTxt}</span>` +
    (rows.length === 1 ? `` : `
      <span>vs catalogue: ${mix(c.novelty_status)}</span>
      <span>relation: ${mix(c.router_verdict)}</span>
      <span>corpus: ${esc(c.source_corpora || "")}</span>
      ${verdictMix(rows)}`) + `
      ${basis} ${prov}
    </div>`;
  const strip = `<div class="wstrip">` +
    (c.witness_strip || []).map(witHtml).join("") + `</div>`;
  // the evidence stays one click away: a card is the question, the rows are
  // the answer's raw material, and each row keeps its own grade control
  // Open by default up to three rows: that is 98.4% of cards (379,331 hold one
  // row, 35,916 two, 11,542 three), so the file+offsets line under each row is
  // there without a click. Above that a card would flood the page -- the
  // largest holds 22 rows -- so it stays collapsed with its count in the
  // summary.
  const open = rows.length <= 3 ? " open" : "";
  const ev = `<details class="kwev"${open}><summary>${num(rows.length)}
      ${rows.length === 1 ? "evidence row" : "evidence rows"} ·
      ${num(c.graded_rows || 0)} graded</summary>` +
    rows.map(rowHtml).join("") + `</details>`;
  return `<div class="kwcard">${head}${summary}${strip}${ev}</div>`;
}

function rowHtml(x){
  const id = x.evidence_id;
  // THE THREE-SLOT ASSESSMENT STRIP (Codex 2026-08-31): same three questions
  // in the same position on every row -- pool, vs-catalogue, model reading.
  // Grayscale by design; amber marks REVIEWER ATTENTION only, never model
  // confidence.
  const mVerdict = x.gate_divergence || x.gate_new_finds;
  const mLlm = x.llm && (x.llm.divergence || x.llm.new_finds);
  const attn = mVerdict === "computed_right_catalogue_mismatch";
  // colours mark DIRECTION, not confidence (owner, 2026-08-31): green = the
  // computed identification looks right here, orange = it does not,
  // amber = needs human review, gray = no answer. Stated once in the page note.
  const NOVG_DOT = {"New": "#2e9e6b", "Diverges": "#8a6bbf",
                    "Aligned": "#5a7ca8", "Not checked": "#8a8f98"};
  // the model slot's HEADLINE is the owner's question -- "is the computed ID
  // right on this page?" -- because the catalogue being right does NOT make
  // our identification wrong (multi-work pages, general entries). The raw
  // verdict is the second line; the rationale expands from the answer.
  const YES_VERDICTS = new Set(["credible_new_identification",
    "plausible_needs_expert_check", "both_right_multiple_works",
    "catalogue_too_general", "computed_right_catalogue_mismatch",
    "actually_recorded"]);
  const mAnswer = !mVerdict ? null :
    mVerdict === "not_checked" ? ["no answer", "#8a8f98"] :
    mVerdict === "overlapping_works" ? ["Cannot tell", "#8a8f98"] :
    YES_VERDICTS.has(mVerdict) ? ["Yes", "#2e9e6b"] : ["No", "#c77b21"];
  const dot = c => c ? `<span class="sdot" style="background:${c}"></span>` : ``;
  // each slot carries its OWN one-line tooltip; the long docs live in the
  // sidebar cards' "what is this?"
  const slot = (lbl, val, sub, cls, tip, body) =>
    `<div class="slot${cls ? " " + cls : ""}"${tip ? ` title="${esc(tip)}"` : ``}>
      <div class="slbl">${lbl}</div>` +
    (body ? `<details><summary class="sval">${val}</summary>
               <div class="sbody">${body}</div></details>`
          : `<div class="sval">${val}</div>`) +
    (sub ? `<div class="ssub">${sub}</div>` : ``) + `</div>`;
  // the model's rationale expands FROM its verdict (owner, 2026-08-31)
  const mBody = mLlm && (mLlm.reason || mLlm.doubt)
    ? `<b>Rationale (unverified):</b> ${esc(mLlm.reason || "")}` +
      (mLlm.doubt ? `<br><b>Suggested check:</b> ${esc(mLlm.doubt)}` : ``)
    : ``;
  const slots = `<div class="slots">` +
    slot("Pool", esc(TRIAGE_SHORT[x.triage] || "—"), "", "",
         "Where this row's minutes rank: a deterministic sort over the " +
         "relation, the flags and page coverage — never a verdict.") +
    slot("Vs catalogue",
         dot(NOVG_DOT[novGroupOf(x.novelty_status)]) +
         esc(novGroupOf(x.novelty_status)),
         esc(x.novelty_status || ""), "",
         "How this identification relates to the finding aids: new, " +
         "divergent, or aligned.") +
    // no slot at all when the pair was never judged (owner, 2026-08-31)
    (mVerdict ? slot("Model: computed ID right here?",
         dot(mAnswer[1]) + esc(mAnswer[0]) +
           (attn ? " — but verify" : ""),
         esc(GATE_SHORT[mVerdict] || mVerdict), attn ? "attn" : "",
         "An LLM's unverified answer — click it to read the rationale. " +
         "The second line is the model's specific verdict.",
         mBody) : ``) +
    `</div>`;
  // -- cautions: separate from ordinary metadata, only when they fire --------
  const cautions = [];
  if (x.owner_ruling)
    cautions.push(chip("needs", "owner ruling: " + x.owner_ruling +
      (x.owner_ruling_date ? " (" + x.owner_ruling_date + ")" : ""),
      x.owner_ruling_note || ""));
  if (x.compilation_risk === "high")
    cautions.push(chip("needs", "compilation risk: high",
      "A computed suspicion that this work is an anthology quoting other " +
      "texts, so a match may witness the SOURCE it compiled. It excludes " +
      "nothing by itself."));
  if (x.formula_kind === "embedded_section")
    cautions.push(chip("needs", "fixed prayer / formulary section",
      DOCS["doc.formula_kind"] || ""));
  else if (x.formula_kind === "standalone_unit")
    cautions.push(chip("needs", "standalone liturgy unit",
      DOCS["doc.formula_kind"] || ""));
  let scrWhy = "";
  if (x.scripture && x.scripture.flagged) {
    const s = x.scripture, why = [];
    const pct = v => Math.round(v * 100) + "%";
    if (Math.max(s.bible, s.canon) >= 0.5)
      why.push(s.bible >= s.canon
        ? pct(s.bible) + " of the matched text is verbatim Bible"
        : pct(s.canon) + " of the matched text is verbatim Mishnah/Talmud/Targum");
    if (s.flank && Number(x.matched_letters) < 150)
      why.push("a citation " + (s.flank === "formula" ? "formula" : "sits") +
               " at the match boundary, and the match is short");
    if (s.mask_overlap !== null && s.mask_overlap >= 0.5)
      why.push(pct(s.mask_overlap) +
               " of the span lies inside quotations the mask caught");
    scrWhy = why.join("; ");
    cautions.push(chip("needs", "may rest on shared scripture",
      (scrWhy ? scrWhy + "\n\n" : "") + (DOCS["doc.scripture_flag"] || "")));
  }
  if (Number(x.matched_letters) < SHORT_MATCH && !x.scripture)
    cautions.push(`<span class="dnote text-xs" title="${esc(DOCS["doc.known_weakness"])}">` +
           `short match — check whether this rests on shared scripture</span>`);
  if (x.router_verdict === "parallel" && x.claim_type === "direct_witness")
    cautions.push(chip("needs", "span rank disagrees with the router",
                num(NUMS.disagree) + " rows are in this state"));
  // -- evidence line ---------------------------------------------------------
  let ev = num(x.matched_letters) + " letters";
  if (x.n_spans > 1) ev += " · " + num(x.n_spans) + " spans";
  ev += x.coverage_ppm === null || x.coverage_ppm === undefined
      ? " · page coverage not recorded"
      : " · covers " + (Number(x.coverage_ppm) / 10000).toFixed(1) +
        "% of this page's letters";
  // -- everything else: "Machine details", nothing dropped ------------------
  const more = [];
  more.push(chip("rel", poolLabel("relation", x.router_verdict),
    DOCS["doc.router_verdict"] + (x.relation_kind
      ? "\n\nearlier relation verdict (superseded): " + x.relation_kind : "")));
  more.push(chip("chip", poolLabel("claim", x.claim_type), DOCS["doc.claim_type"]));
  more.push(chip("chip", x.main_pool===null ? "no identification record"
                                            : poolLabel("pool", x.main_pool),
              DOCS["doc.main_pool"]));
  if (x.main_pool === 0 && x.main_pool_reason)
    more.push(`<span class="dnote text-xs">${esc(x.main_pool_reason)}</span>`);
  if (x.corpus_label === "R-source")
    more.push(chip("chip", "not on the live site — this whole corpus is review-only",
                DOCS["doc.routing_status"]));
  else if (x.routing_status !== "shipped")
    more.push(chip("chip", "review only — not shown on the site",
                DOCS["doc.routing_status"]));
  if (x.compilation_risk === "medium")
    more.push(chip("chip", "compilation risk: medium",
      "A computed suspicion that this work is an anthology quoting other " +
      "texts. It excludes nothing by itself."));
  if (x.formula_kind === "documentary_page")
    // CONTEXT ONLY -- catalogue-derived, so it never moves a row between pools
    more.push(chip("chip", "page catalogued as a legal document only — its " +
      "formula may be what the work quotes", DOCS["doc.formula_kind"] || ""));
  if (x.title_provenance === "collection_retitle")
    more.push(chip("chip", "a collection file — the locus names the actual work",
      "This source file holds more than one work; the file-level title now " +
      "says so, and each row's locus names the sub-work the match is in."));
  else if (x.title_provenance && x.title_provenance !== "both_agreed_correct")
    more.push(`<span class="dnote text-xs">title: ${esc(x.title_provenance)}</span>`);
  if (x.confidence_band && x.confidence_band !== "tier_a")
    more.push(chip("chip", x.confidence_band));
  if (x.adjudication_status && x.adjudication_status !== "unreviewed")
    more.push(chip("chip", poolLabel("adjudication", x.adjudication_status),
      "The earlier adjudication pass. Grades entered here do not update it."));
  if (scrWhy)
    more.push(`<span class="dnote text-xs">shared-scripture detectors: ${esc(scrWhy)}</span>`);
  const full = browseUrl(x, false);
  let previewBtn = ``;
  if (PREVIEW !== "off" && x.sys_id)
    previewBtn = `<button class="fchip gs-findings-row-preview-toggle" id="pv-${id}"
        onclick="preview('${id}',this)">Preview the manuscript</button>`;
  else if (full)
    previewBtn = `<a class="fchip gs-findings-row-preview-toggle" href="${esc(full)}"
        target="_blank" rel="noopener">open the manuscript in a tab ↗</a>`;

  // Identity lines. Titles are NEVER machine-translated -- discovery work titles
  // are Hebrew-only -- and `dir="auto"` + isolation keeps a Hebrew title from
  // reordering the Latin text beside it.
  const title = `<span class="font-bold" dir="auto" style="unicode-bidi:isolate">` +
    esc(x.work_title || x.work_id) + `</span>`;
  // The work-id chip is ALWAYS shown: 43 titles are shared by more than one
  // work_id, so a title alone does not name a work.
  const wid = `<span class="chip mono">${esc(x.work_id)}</span>`;
  // Never "Unknown author" -- that would be a claim about the work. And no
  // provenance annotation beside the name: the only provenance that ever
  // reached this line was "derived from the title", and an author that is
  // already spelled inside the title tells a reader nothing the title did not
  // -- so those authors are no longer stored at all (owner, 2026-09-01;
  // scripts/drop_title_derived_authors.py). An annotation for a value that
  // adds nothing is noise twice over.
  const author = x.work_author
    ? `<div class="dnote" dir="auto" style="unicode-bidi:isolate">` +
      `${esc(x.work_author)}</div>`
    : ``;
  // Both library_code and shelfmark are NULL on 14,349 rows (5.6%); fall back to
  // the sys_id in a monospace chip, never to a blank line.
  let shelf;
  if (x.shelfmark || x.library_code) {
    shelf = (x.library_code ? `<span class="chip">${esc(x.library_code)}</span> ` : ``) +
      (full ? `<a href="${esc(full)}" target="_blank" rel="noopener">${esc(x.shelfmark || x.sys_id)}</a>`
            : esc(x.shelfmark || x.sys_id));
  } else {
    shelf = `<span class="chip mono">${esc(x.sys_id)}</span>`;
  }
  // Two separate elements, never one concatenated string, and the title verbatim
  // in one language. Absent on 19,786 rows -- render nothing at all.
  // 44,432 rows have NO catalogue line. Rendering nothing made that
  // indistinguishable from a field that was never captured -- and the whole
  // novelty axis is measured against the catalogue, so its silence is a fact
  // the reader needs.
  const cat = x.catalogue_title
    ? `<div class="side gap-2 items-center flex-wrap"><span class="dnote">Catalogued as:</span>
       <span dir="auto" style="unicode-bidi:isolate">${esc(x.catalogue_title)}</span></div>`
    : `<div class="side gap-2 items-center flex-wrap"><span class="dnote"
         title="No finding-aid entry for this manuscript reached this file. It is an absence of a record, not an unfilled column.">Catalogued as:
         no catalogue record on file</span></div>`;
  // The citable address, on the card itself (the live findings page shows
  // its locus the same way). `whole_work` is not shown here -- its label is
  // the work title, which the card already leads with.
  // right under the title, no label (owner, 2026-08-30) -- the address IS
  // self-describing
  const locus = (x.locus_label && x.locus_status === "resolved")
    ? `<div class="dnote" dir="auto" style="unicode-bidi:isolate">${esc(x.locus_label)}</div>`
    : ``;

  // Codex point 4: catalogue text right under its comparison slot; scattered
  // metadata merged into one compact line; cautions on their own line;
  // Sources & provenance and Machine details as labeled disclosures (point 6);
  // grading minimized behind a disclosure (owner: the texts are the point).
  return `<div class="row gs-findings-row w-full gap-1 p-2"
      style="border-block-end:1px solid var(--border-light)">
    <div class="side gap-2 items-center flex-wrap">${title}${wid}
      ${x.grade ? chip("nov", "graded: " + x.grade) : ``}</div>
    ${locus}
    ${author}
    <div class="side gap-2 items-center flex-wrap">${shelf}
      ${chip("chip", x.corpus_label)}
      <span class="dnote text-xs">${esc(ev)}</span>
      ${x.alias_twin && x.alias_twin.length
        ? chip("chip", "same work in another corpus: " + x.alias_twin.join(" · "),
               DOCS["doc.work_alias"] || "") : ``}</div>
    ${slots}
    ${cat}
    ${cautions.length ? `<div class="cautions">${cautions.join("")}</div>` : ``}
    <div class="cols" id="txt-${id}">${panes(x)}</div>
    <div class="dnote" id="txtnote-${id}">${esc(DOCS["doc.ms_match_vs_ref_match"])}</div>
    ${provLine(x)}
    ${previewBtn ? `<div style="margin-block-start:6px">${previewBtn}</div>` : ``}
    <div class="prev" id="prev-${id}"></div>
    <details class="mdet"><summary>Sources &amp; provenance — files and
        character offsets, both sides</summary>
      ${readBlock(x)}${provBlock(x)}</details>
    <details class="mdet"><summary>Machine details (${more.length})</summary>
      <div class="mrows">${more.join("")}</div></details>
    <details class="mdet"><summary>Grade${x.grade ? ": " + esc(x.grade) : ""} —
        optional</summary>${gradeBar(x)}</details>
  </div>`;
}

// ---- pane A: source vs manuscript text, DEFAULT OPEN (spec 3.1) ----------
// Two independent panes, two buttons, two regions. Both may be open at once and
// neither closes the other: the text pane is the grading instrument and the
// folio pane is the confirmation step, so sharing one region would hide the
// manuscript text at exactly the moment the reviewer is comparing them.
// the citation link sits with the REFERENCE pane's heading, next to the
// address, so it reads as one more way to reach the text -- not as the address
function sefariaLink(x){
  if (!x.sefaria_url) return ``;
  return ` <a href="${esc(x.sefaria_url)}" target="_blank" rel="noopener"
    title="Open ${esc(x.sefaria_label)} on Sefaria. It locates the chapter or
folio, NOT the matched span: the character offsets on this row index our own
source file, and Sefaria serves a different edition with its own numbering."
    >Sefaria \u2197</a>`;
}
// `extra` is TRUSTED HTML built server-side or by sefariaLink() -- the title
// itself is still escaped, so a corpus label can never inject markup
function pane(kind, id, title, b, m, a, isStream, extra){
  return `<div class="pane"><h4><span>${esc(title)}</span>${extra || ``}
    ${isStream?'<span class="stream">[unspaced letter stream]</span>':''}
    <button class="fchip" onclick="copyPane('${id}','${kind}',this)">copy</button></h4>
    <div class="txt" dir="rtl"><span class="ctx">${esc(b)}</span><mark>${esc(m)}</mark><span class="ctx">${esc(a)}</span></div></div>`;
}
function panes(x){
  return pane("ms", x.evidence_id, "Manuscript",
              x.ms_before, x.ms_match, x.ms_after, false) +
         pane("ref", x.evidence_id, "Reference edition — " + x.corpus_label,
              x.ref_before, x.ref_match, x.ref_after, x.ref_is_stream,
              sefariaLink(x));
}
// ---- pane C: what the novelty gate read, DEFAULT CLOSED ------------------
// The row's "Catalogued as:" line is libraries.csv column 7 ALONE; the gate
// judged on a combined catalogue text plus bibliography, PGP, FGP and an
// M-source witness count, and on 83% of `confirms` rows the two differ -- so a
// correct label looked absurd. Every string here is built server-side
// (`_attach_gate`), including the corpus name, which is produced BY the
// redaction map rather than trusted to already agree with it.
function readBlock(x){
  const id = x.evidence_id, r = x.read;
  // The BUTTON carries the warning, because it is what a grader sees without
  // expanding: a bundle resting on nothing but a per-work count must not be
  // discoverable only by clicking.
  let flag = "", body;
  if (!r) {
    flag = " — nothing: never put to the gate";
    body = `<div class="dnote">This identification was never put to the novelty
      gate (novelty <b>not_checked</b>), so there is nothing it read. The line
      above is the catalogue title only.</div>`;
  } else if (r.missing_table) {
    flag = " — not recorded in this file";
    body = `<div class="dnote">This file does not carry the gate's own reading
      material (it was not extracted for this build). The verdict on the row is
      still real; what is missing is only the ability to see WHAT the gate read
      when it judged. Nothing you need for grading depends on it.</div>`;
  } else {
    const parts = [];
    if (r.nothing_else)
      parts.push(`<div class="rdwarn">This identification rests on NOTHING about
        this manuscript — only a count of the work's other witnesses, below.</div>`);
    if (r.empty)
      parts.push(`<div class="dnote">The gate found no catalogue, bibliography,
        PGP or FGP text for this manuscript.</div>`);
    (r.items || []).forEach(it => {
      parts.push(`<div class="rdrow"><span class="rdlab">${esc(it.label)}</span>
        <span class="rdtxt" dir="auto" style="unicode-bidi:isolate">${esc(it.text)}</span></div>`);
    });
    if (r.thin_title)
      parts.push(`<div class="dnote">The row's “Catalogued as:” line shows only
        <span dir="auto" style="unicode-bidi:isolate">${esc(r.thin_title)}</span> —
        the gate read the fuller catalogue text above.</div>`);
    if (r.msrc) {
      // NOT about this manuscript unless a witness actually matched. Said in
      // the LABEL, not buried in a footnote.
      const lab = r.msrc.about_this_ms
        ? esc(r.msrc.label) + " — this manuscript matched" +
          (r.msrc.conf ? " (" + esc(r.msrc.conf) + " confidence)" : "")
        : esc(r.msrc.label) + " — about the WORK, not this manuscript";
      parts.push(`<div class="rdrow${r.msrc.about_this_ms?``:` weak`}">
        <span class="rdlab">${lab}</span>
        <span class="rdtxt" dir="auto" style="unicode-bidi:isolate">${esc(r.msrc.text)}</span></div>`);
      if (!r.msrc.about_this_ms)
        parts.push(`<div class="dnote">A per-work, count-only statement: how many
          OTHER witnesses that corpus records for this work. Nothing tied this
          manuscript to it.</div>`);
    }
    if (r.reason)
      parts.push(`<div class="dnote">gate's own reason: <code>${esc(r.reason)}</code></div>`);
    // The count is of sources ABOUT THIS MANUSCRIPT. A per-work witness count
    // is named separately and never totted up with them -- adding it would put
    // the "rests on nothing" rows at "1 source", which is the false impression
    // this whole block exists to remove.
    const n = (r.items || []).length;
    if (r.nothing_else) flag = " — only a per-work count";
    else if (r.empty) flag = " — nothing found";
    else {
      flag = " — " + n + (n === 1 ? " source" : " sources");
      if (r.msrc) flag += r.msrc.about_this_ms
        ? " + an " + r.msrc.label + " match" : " + a per-work count";
    }
    body = parts.join("");
  }
  const cls = (r && (r.nothing_else || !r.items || !r.items.length)) || !r
    ? "fchip warnchip" : "fchip";
  return `<div class="readwrap">
    <button class="${cls}" id="rd-${esc(id)}" onclick="toggleRead('${escJs(id)}',this)"
      >What the software read${esc(flag)}</button>
    <div class="readbody" id="readb-${esc(id)}" style="display:none">${body}</div>
  </div>`;
}
function toggleRead(id, btn){
  const box = $("readb-" + id);
  if (!box) return;
  const hidden = box.style.display === "none";
  box.style.display = hidden ? "block" : "none";
  btn.classList.toggle("here", hidden);
}
// ---- pane D: where each side of the match came from, DEFAULT CLOSED -------
// New in the v5 artifact: file + character offsets on BOTH sides. All offsets
// count characters of the NFC-normalized text, 0-based, end exclusive. A v3 db
// has none of these columns; the block then says "not recorded" and nothing
// else changes.
function fmtN(v){ return (v === null || v === undefined) ? "?" : Number(v).toLocaleString("en-US"); }
// The FILE AND CHARACTER RANGE of both sides, always visible on the row. This
// is the thing the v5 artifact exists to carry, and it used to sit two clicks
// down (a <details> plus a toggle) -- three in card grain, where the row is
// itself inside the card's evidence <details>. The full block below still holds
// the statuses, the witness id and the NFC caveat; this line holds the address.
//
// A masked corpus shows its CODENAME (RS:10.2.3), never a filename: the real
// basename exists only in the local key file, outside this artifact.
function provLine(x){
  const n = v => (v === null || v === undefined) ? "?" : Number(v).toLocaleString("en-US");
  const parts = [];
  if (x.ms_provenance_status === "ok") {
    parts.push(`<span class="pvside"><b>MS</b> <span class="mono">Transcriptions.txt
      · chars ${n(x.file_char_start)}–${n(x.file_char_end)}</span></span>`);
  } else if (x.ms_provenance_status === "offsets_missing" &&
             x.page_char_start !== null && x.page_char_start !== undefined) {
    // A substituted page: the matcher searched a human transcription (FGP/PGP)
    // of it. When the span was re-aligned onto the HTR text, THAT address is
    // the one the reader can open; its status and score travel with it. Only
    // 'offsets_missing' means substituted -- 'nfc_shift' is a different fact
    // and gets its own line below.
    const st = x.htr_align_status;
    const hasFile = st && x.htr_file_char_start !== null && x.htr_file_char_start !== undefined;
    if (hasFile && (st === "exact" || st === "realigned_htr" || st === "realign_uncertain")) {
      const how = st === "exact" ? "verbatim"
        : st === "realigned_htr" ? `re-aligned, score ${n(x.htr_align_score)}`
        : `best window, score ${n(x.htr_align_score)} — uncertain`;
      parts.push(`<span class="pvside"><b>MS</b> <span class="mono">Transcriptions.txt
        · chars ${n(x.htr_file_char_start)}–${n(x.htr_file_char_end)}</span>
        <span class="${st === "realign_uncertain" ? "weak" : ""}">· HTR, ${how}; the
        searched text was FGP/PGP</span></span>`);
    } else {
      parts.push(`<span class="pvside"><b>MS</b> <span class="mono">searched text (FGP/PGP):
        chars ${n(x.page_char_start)}–${n(x.page_char_end)}</span>
        <span class="weak">· no address in Transcriptions.txt${st ? ` (${esc(st)})` : ``}</span></span>`);
    }
  } else if (x.page_char_start !== null && x.page_char_start !== undefined) {
    // nfc_shift (or any other non-ok status): page offsets only, and say why
    parts.push(`<span class="pvside"><b>MS</b> <span class="mono">this page (NFC text):
      chars ${n(x.page_char_start)}–${n(x.page_char_end)}</span>
      <span class="weak">· file offsets withheld (${esc(x.ms_provenance_status || "?")})</span></span>`);
  }
  const src = x.src;
  const name = src ? (src.file || src.ref_id) : null;
  if (name) {
    const range = (x.ref_char_start !== null && x.ref_char_end !== null &&
                   x.ref_char_start !== undefined)
      ? `· chars ${n(x.ref_char_start)}–${n(x.ref_char_end)}`
      : `· letter-stream ${n(x.w_start)}–${n(x.w_end)}`;
    parts.push(`<span class="pvside"><b>Ref</b> <span class="mono">${esc(name)}
      ${range}</span>` +
      (src.masked ? ` <span class="weak">(codename)</span>` : ``) + `</span>`);
  }
  if (!parts.length) return ``;
  return `<div class="pvline" title="The file each side was taken from and the
    character range of the match inside it. Offsets count characters of the
    NFC-normalized text, 0-based, end exclusive.">${parts.join("")}</div>`;
}
// ---- the HTR side of a substituted page -----------------------------------
// On 18,982 corpus pages the matcher searched a human transcription (FGP/PGP)
// instead of the HTR; the HTR text still stands in Transcriptions.txt, and
// scripts/attach_htr_realignment.py located each matched span in it by
// alignment. The status says how far to trust that address: a low score is
// shown, never hidden, and 'ambiguous' / 'unalignable' get no address at all.
function htrAddressRow(x){
  const st = x.htr_align_status;
  if (!st) return `<div class="rdrow weak"><span class="rdlab">HTR</span>
    <span class="rdtxt">not re-aligned onto the HTR text (this file predates that pass)</span></div>`;
  const sc = x.htr_align_score;
  const hasFile = x.htr_file_char_start !== null && x.htr_file_char_start !== undefined;
  const hasPage = x.htr_page_char_start !== null && x.htr_page_char_start !== undefined;
  const addr = `<span class="mono">Transcriptions.txt · chars ${fmtN(x.htr_file_char_start)}–${fmtN(x.htr_file_char_end)}</span>`;
  let txt, weak = false;
  if (!hasFile) {
    // no file address: say which of the known reasons, never print "?–?"
    weak = true;
    if (st === "ambiguous")
      txt = `the matched letters occur more than once in the HTR page — no single
        address, on purpose`;
    else if (st === "unalignable")
      txt = `the matched span is too short to locate in the HTR text`;
    else if (hasPage)
      txt = `HTR page chars ${fmtN(x.htr_page_char_start)}–${fmtN(x.htr_page_char_end)} —
        file address withheld: this page's raw form differs from its NFC form`;
    else
      txt = `no HTR address recorded for this row (status <code>${esc(String(st))}</code>)`;
  } else if (st === "exact")
    txt = `${addr} — the matched letters occur verbatim in the HTR page`;
  else if (st === "realigned_htr")
    txt = `${addr} — re-aligned onto the HTR text, score ${fmtN(sc)}`;
  else if (st === "realign_uncertain") { weak = true;
    txt = `${addr} — best HTR window, score ${fmtN(sc)}: the HTR is noisy here,
      treat the boundaries as approximate`; }
  else { weak = true;
    txt = `${addr} — status <code>${esc(String(st))}</code> is not one this viewer
      knows; treat the address as unverified`; }
  const btn = HTR_OK ? ` <button class="fchip" id="htrb-${esc(x.evidence_id)}"
      onclick="htrPane('${escJs(x.evidence_id)}',this)">HTR text of this page</button>` : ``;
  return `<div class="rdrow${weak ? ` weak` : ``}"><span class="rdlab">HTR</span>
    <span class="rdtxt">${txt}${btn}</span></div>
    <div id="htrp-${esc(x.evidence_id)}" style="display:none"></div>`;
}
const HTRP = {};   // page_id -> fetched htr_page payload, once per page
async function htrPane(id, btn){
  const box = $("htrp-" + id);
  if (!box) return;
  if (box.style.display !== "none") {
    box.style.display = "none"; btn.classList.remove("here"); return;
  }
  const x = LAST.find(r => r.evidence_id === id);
  if (!x) return;
  let p = HTRP[x.page_id];
  if (!p) {
    btn.textContent = "loading…";
    try {
      const r = await fetch("/api/htr_page?page_id=" + encodeURIComponent(x.page_id));
      p = await r.json();
    } catch (e) { p = {error: String(e)}; }
    btn.textContent = "HTR text of this page";
    if (p && !p.error) HTRP[x.page_id] = p;
  }
  if (!p || p.error) {
    box.innerHTML = `<div class="dnote">HTR text unavailable: ${esc((p && p.error) || "no response")}</div>`;
    box.style.display = "block"; return;
  }
  // the page text is shown EXACTLY as stored -- no display cleaning, because
  // the row's page offsets index this very string
  const t = String(p.text || "");
  let b = t, m = "", a = "";
  if (x.htr_page_char_start !== null && x.htr_page_char_start !== undefined &&
      x.htr_page_char_end !== null && x.htr_page_char_end !== undefined) {
    b = t.slice(0, x.htr_page_char_start);
    m = t.slice(x.htr_page_char_start, x.htr_page_char_end);
    a = t.slice(x.htr_page_char_end);
  }
  const where = (p.file_start !== null && p.file_start !== undefined)
    ? ` · Transcriptions.txt chars ${fmtN(p.file_start)}–${fmtN(p.file_end)}`
    : ` · no file address (NFC shift)`;
  box.innerHTML = pane("htr", id, `Manuscript — HTR text of this page${where}`, b, m, a, false,
    `<span class="weak"> the matcher searched a ${esc(String(p.source || "?")).toUpperCase()}
     transcription of this page instead (substitution score ${esc(String(p.score ?? "?"))});
     ${m ? "the marked span is where the match re-aligned" : "no span could be marked"}</span>`);
  box.style.display = "block"; btn.classList.add("here");
}
function provBlock(x){
  const id = x.evidence_id;
  const hasMs = x.ms_provenance_status !== undefined && x.ms_provenance_status !== null;
  const hasRef = x.ref_provenance_status !== undefined && x.ref_provenance_status !== null;
  if (!hasMs && !hasRef)
    return `<div class="readwrap">
      <button class="fchip" disabled>Where this came from — not recorded (a v3-era file)</button></div>`;
  const parts = [];
  // -- manuscript side ------------------------------------------------------
  if (hasMs) {
    if (x.ms_provenance_status === "ok") {
      parts.push(`<div class="rdrow"><span class="rdlab">Manuscript</span>
        <span class="rdtxt mono">Transcriptions.txt · chars ${fmtN(x.file_char_start)}–${fmtN(x.file_char_end)}
        (this page: ${fmtN(x.page_char_start)}–${fmtN(x.page_char_end)})</span></div>`);
    } else if (x.ms_provenance_status === "offsets_missing") {
      parts.push(`<div class="rdrow weak"><span class="rdlab">Manuscript</span>
        <span class="rdtxt">the text the matcher searched on this page was a human
        transcription (FGP/PGP), not the HTR in Transcriptions.txt` +
        (x.page_char_start != null ? `; within that text: chars ${fmtN(x.page_char_start)}–${fmtN(x.page_char_end)}` : ``) +
        `</span></div>`);
      parts.push(htrAddressRow(x));
    } else if (x.ms_provenance_status === "nfc_shift") {
      parts.push(`<div class="rdrow weak"><span class="rdlab">Manuscript</span>
        <span class="rdtxt">file offsets withheld: this page's raw bytes differ from
        their NFC form, so a file address would be off by a character or two.
        Within the NFC page text: chars ${fmtN(x.page_char_start)}–${fmtN(x.page_char_end)}</span></div>`);
    } else {
      parts.push(`<div class="rdrow weak"><span class="rdlab">Manuscript</span>
        <span class="rdtxt mono">${esc(x.ms_provenance_status)}</span></div>`);
    }
  }
  // -- reference side -------------------------------------------------------
  if (hasRef) {
    const s = x.src;
    let fileLine;
    if (s && s.file) fileLine = esc(s.file);
    else if (s && s.masked) fileLine = `<span class="chip mono">${esc(s.ref_id)}</span> — a masked
      source: this id resolves to a real file only through a key file kept by
      the project, on purpose. You are not missing anything, and grading does
      not need it`;
    else fileLine = `<span class="chip mono">${esc(x.witness_id || "?")}</span>`;
    if (x.ref_provenance_status === "ok") {
      parts.push(`<div class="rdrow"><span class="rdlab">Reference</span>
        <span class="rdtxt" dir="auto" style="unicode-bidi:isolate">${fileLine}</span></div>`);
      parts.push(`<div class="rdrow"><span class="rdlab"></span>
        <span class="rdtxt mono">chars ${fmtN(x.ref_char_start)}–${fmtN(x.ref_char_end)} ·
        letter stream ${fmtN(x.w_start)}–${fmtN(x.w_end)}</span></div>`);
    } else {
      parts.push(`<div class="rdrow weak"><span class="rdlab">Reference</span>
        <span class="rdtxt">no per-file address (<code>${esc(x.ref_provenance_status)}</code>) —
        the reference pane shows the work's letter stream, positions ${fmtN(x.w_start)}–${fmtN(x.w_end)}</span></div>`);
    }
    if (x.locus_label)
      parts.push(`<div class="rdrow"><span class="rdlab">Citable locus</span>
        <span class="rdtxt" dir="auto" style="unicode-bidi:isolate">${esc(x.locus_label)}</span></div>`);
  }
  parts.push(`<div class="dnote">Offsets count characters of the NFC-normalized
    text (not bytes), 0-based, end exclusive. Every one was re-derived
    independently and matched before this file shipped.</div>`);
  // A substituted page whose span was re-aligned onto the HTR does have a
  // manuscript-side file address; the chip says so, and says how it got one.
  const msViaHtr = hasMs && x.ms_provenance_status === "offsets_missing" &&
    x.htr_file_char_start !== null && x.htr_file_char_start !== undefined &&
    (x.htr_align_status === "exact" || x.htr_align_status === "realigned_htr");
  const msHtrUncertain = hasMs && x.ms_provenance_status === "offsets_missing" &&
    x.htr_align_status === "realign_uncertain";
  const flag = (hasMs && x.ms_provenance_status === "ok" && x.ref_provenance_status === "ok")
    ? " — file + offsets, both sides"
    : (msViaHtr && x.ref_provenance_status === "ok")
      ? " — file + offsets, both sides (MS via HTR re-alignment)"
      : msHtrUncertain
        ? " — partial (HTR address uncertain, see why)"
        : " — partial (see why)";
  return `<div class="readwrap">
    <button class="fchip" id="pv2-${esc(id)}" onclick="toggleProv('${escJs(id)}',this)"
      >Where this came from${flag}</button>
    <div class="readbody" id="provb-${esc(id)}" style="display:none">${parts.join("")}</div>
  </div>`;
}
function toggleProv(id, btn){
  const box = $("provb-" + id);
  if (!box) return;
  const hidden = box.style.display === "none";
  box.style.display = hidden ? "block" : "none";
  btn.classList.toggle("here", hidden);
}
function toggleText(id, btn){
  const box = $("txt-" + id), note = $("txtnote-" + id);
  const hidden = box.style.display === "none";
  box.style.display = hidden ? "grid" : "none";
  if (note) note.style.display = hidden ? "" : "none";
  btn.textContent = hidden ? "Hide the texts" : "Compare the texts";
}
function copyPane(id, kind, btn){
  const x = LAST.find(r => r.evidence_id === id);
  if (!x) return;
  const t = kind === "ms" ? [x.ms_before, x.ms_match, x.ms_after]
          : kind === "htr" ? [(HTRP[x.page_id] || {}).text]
                           : [x.ref_before, x.ref_match, x.ref_after];
  const s = t.map(v => String(v ?? "")).join("");
  try { navigator.clipboard.writeText(s); btn.textContent = "copied"; }
  catch (e) { btn.textContent = "copy failed"; }
  setTimeout(() => { btn.textContent = "copy"; }, 1500);
}

// ---- pane B: the folio preview, DEFAULT CLOSED, lazy, capped at one -------
let openFrame = null;             // evidence_id of the one live iframe
const watchdogs = {};
function preview(id, btn){
  const box = $("prev-" + id);
  if (box.classList.contains("on")) return closePreview(id, btn);
  const x = LAST.find(r => r.evidence_id === id);
  if (!x) return;
  const full = browseUrl(x, false), emb = browseUrl(x, true);
  const tab = `<a href="${esc(full)}" target="_blank" rel="noopener">open in a tab ↗</a>`;
  if (PREVIEW === "image") {
    box.innerHTML = `<div class="bar"><span>folio ${esc(x.page_num||"?")} — image only,
        no folio navigation and no transcription</span>${tab}</div>
      <img src="${esc(imageUrl(x))}" loading="lazy" alt="folio image"
        onerror="imgFailed('${id}')">`;
  } else {
    // EVERY OPEN MINTS A NEW SERVER-SIDE SESSION on production: the third-party
    // session cookie is SameSite=Lax, so it is neither sent nor accepted
    // cross-site. Hence the cap at one and the destroy on close -- the public
    // page latches `loaded` and leaves N live iframes and N websockets, which
    // is exactly what a day of grading must not do.
    if (openFrame && openFrame !== id) {
      const other = $("prev-" + openFrame), ob = $("pv-" + openFrame);
      if (other) { other.classList.remove("on"); other.innerHTML = ""; }
      if (ob) ob.textContent = "Preview the manuscript";
      clearTimeout(watchdogs[openFrame]);
    }
    openFrame = id;
    box.innerHTML = `<div class="bar">
        <span>live ${esc(SITE.replace(/^https?:\/\//,""))} — folio ${esc(x.page_num||"?")}</span>
        ${tab}
        <span class="dnote">the embedded viewer opens in Hebrew — it has no access
        to your site settings</span></div>
      <div class="dnote" id="wd-${id}"></div>
      <iframe src="${esc(emb)}" title="Live manuscript viewer" loading="lazy"
        referrerpolicy="no-referrer" sandbox="allow-scripts allow-same-origin"></iframe>`;
    // THE ONLY AVAILABLE SIGNAL. The parent cannot read contentDocument and the
    // `load` event fires on the 32 KB spinner shell, so "rendered" and "spinner
    // forever" are indistinguishable from here. Safari ITP and any block-all-
    // cookies setting throw SecurityError inside nicegui.js at evaluation and
    // the spinner never resolves; this timer is what surfaces that.
    watchdogs[id] = setTimeout(() => {
      const wd = $("wd-" + id);
      if (wd) wd.innerHTML = `<span class="needs">the embedded viewer did not
        paint — open it in a tab</span>`;
    }, 8000);
  }
  box.classList.add("on");
  btn.textContent = "Close";
}
function closePreview(id, btn){
  const box = $("prev-" + id);
  box.classList.remove("on");
  box.innerHTML = "";                       // DESTROY, do not merely hide
  clearTimeout(watchdogs[id]);
  if (openFrame === id) openFrame = null;
  if (btn) btn.textContent = "Preview the manuscript";
}
function imgFailed(id){
  const box = $("prev-" + id);
  const x = LAST.find(r => r.evidence_id === id);
  if (!box || !x) return;
  box.innerHTML = `<div class="bar"><span class="needs">no image at this endpoint
      (it is NLI-routed; Oxford manuscripts are not served by it)</span>
      <a href="${esc(browseUrl(x,false))}" target="_blank" rel="noopener">open in a tab ↗</a></div>`;
}
// Interacting with the frame blurs the parent window; that is the closest thing
// to a paint signal available without a postMessage channel in browse.py, which
// is out of scope for a private tool.
try { window.addEventListener("blur", () => {
  if (openFrame) { clearTimeout(watchdogs[openFrame]); }
}); } catch (e) {}

// ---- the grade bar (spec 2.4) --------------------------------------------
function gradeBar(x){
  const id = x.evidence_id;
  return `<div class="grade">
    <span class="lbl">Which is right?</span>
    ${DIV.map(v => `<button class="fchip${x.grade===v?" here":""}"
       aria-pressed="${x.grade===v?"true":"false"}"
       onclick="grade('${id}','${v}',this)">${esc(DIV_LABELS[v] || v.replace("_"," "))}</button>`).join("")}
    <button class="fchip" onclick="grade('${id}','',this)">clear</button>
    <input type="text" id="note-${id}" value="${esc(x.note)}"
      placeholder="why? (saved as you type)" oninput="noteTyped('${id}')">
  </div>`;
}
async function grade(id, val, btn){
  const r = await fetch("/api/grade", {method:"POST",
    headers:{"Content-Type":"application/json"},
    body: JSON.stringify({evidence_id:id, divergence_correctness:val})});
  if (!r.ok) { btn.textContent = "not saved"; return; }
  const row = btn.parentElement;
  [...row.querySelectorAll("button")].forEach(b => {
    b.classList.remove("here"); b.setAttribute("aria-pressed", "false");
  });
  if (val) { btn.classList.add("here"); btn.setAttribute("aria-pressed", "true"); }
  const x = LAST.find(r2 => r2.evidence_id === id);
  if (x) x.grade = val;
  sessionGraded++;
  const j = await r.json().catch(() => ({}));
  renderSession(j.graded_total);
}
const noteTimers = {};
// The field says "saved as you type", and a 500 ms debounce means the last
// keystrokes are still in the browser when the reviewer closes the tab or
// switches away -- so the claim was false for exactly the words they typed last.
// Every pending note is now flushed on the way out, with sendBeacon (which the
// browser delivers even as the page unloads) and a synchronous XHR fallback.
const noteText = {};
function flushNotes(){
  const ids = Object.keys(noteTimers);
  for (const id of ids) {
    if (noteTimers[id] === undefined) continue;
    clearTimeout(noteTimers[id]);
    delete noteTimers[id];
    const el = $("note-" + id);
    const body = JSON.stringify({evidence_id: id,
                                 note: el ? el.value : (noteText[id] || "")});
    let sent = false;
    try {
      if (navigator.sendBeacon)
        sent = navigator.sendBeacon("/api/grade",
                 new Blob([body], {type: "application/json"}));
    } catch (e) {}
    if (!sent) {
      try {                       // last resort: synchronous, unload-safe
        const x = new XMLHttpRequest();
        x.open("POST", "/api/grade", false);
        x.setRequestHeader("Content-Type", "application/json");
        x.send(body);
      } catch (e) {}
    }
  }
}
function noteTyped(id){
  clearTimeout(noteTimers[id]);
  const el0 = $("note-" + id);
  if (el0) noteText[id] = el0.value;   // survives the row being re-rendered
  noteTimers[id] = setTimeout(async () => {
    const el = $("note-" + id);
    delete noteTimers[id];
    await fetch("/api/grade", {method:"POST",
      headers:{"Content-Type":"application/json"},
      body: JSON.stringify({evidence_id:id, note: el ? el.value : ""})});
  }, 500);
}
// pagehide fires on close/navigate; visibilitychange catches a switched tab or a
// closed laptop, which is where an unsaved note would otherwise sit for hours
window.addEventListener("pagehide", flushNotes);
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "hidden") flushNotes();
});
// The safeguard against the worst outcome: an hour of grading that never reached
// disk. It is written per click, so the number must go up as the reviewer works.
function renderSession(totalGraded){
  const el = $("sessionstrip");
  if (!el) return;
  if (!sessionGraded && !totalGraded) { el.style.display = "none"; return; }
  el.style.display = "";
  el.innerHTML = `${num(sessionGraded)} graded this session · ` +
    `${num(totalGraded)} graded in total · ` +
    `<a href="/api/export">export</a>`;
}

// ---- result bar, active-filter chips, pager (spec 4.10, 4.11) ------------
const SORTS = [["work","Work, then manuscript"],
               ["letters","Most matched letters first"],
               ["coverage","Highest page coverage first"],
               ["pages","Fewest matched pages first"]];
// the card grain sorts over the card's OWN aggregates: "fewest matched pages"
// is an identification-level key with no card equivalent, and offering it here
// would silently fall back to the default sort
const CARD_SORTS = [["work","Known work, then manuscript"],
                    ["letters","Most matched letters first"],
                    ["coverage","Highest page coverage first"],
                    ["witnesses","Most evidence on the page first"]];
function sortList(){ return (CARDS_OK && S.grain === "card") ? CARD_SORTS : SORTS; }
function grainToggle(){
  if (!CARDS_OK) return ``;
  return `<span class="dnote">Grain</span>` +
    chipBtn(S.grain === "card", "Cards", null, `setGrain('card')`,
      "One card per page × known work: the question asked once, with " +
      "every witness's evidence beneath it.") +
    chipBtn(S.grain === "row", "Evidence rows", null, `setGrain('row')`,
      "One row per alignment — the same page and work appear once per " +
      "witness of the work.");
}
function renderRbar(d){
  // The real pre-LIMIT total, EXACT and never capped. A grading tool's totals
  // must be exact; a capped total reported as exact is a correctness defect.
  // THREE NAMED NUMBERS in card grain: cards, evidence rows, manuscripts are
  // different things and are never collapsed into one "total".
  const head = S.grain === "card"
    ? `<span>Showing <span class="n">${num((d.cards||[]).length)}</span> of
       <span class="n">${num(d.total)}</span> cards</span>
       <span class="dnote">${num(d.evidence_rows)} evidence rows in them</span>
       <span class="dnote">of ${num(d.grain ? d.grain.cards : 0)} cards over
       ${num(d.grain ? d.grain.manuscripts : 0)} manuscripts in the file</span>`
    : `<span>Showing <span class="n">${num((d.rows||[]).length)}</span> of
       <span class="n">${num(d.total)}</span> rows</span>
       <span class="dnote">${num(d.identifications)} identifications</span>
       <span class="dnote">${num(d.graded_here)} of these are graded</span>`;
  $("rbar").innerHTML = `<div class="rbar gs-findings-rbar w-full gap-2">
    <div class="side items-center gap-2 flex-wrap">${head}</div>
    <div class="side items-center gap-2 flex-wrap">${grainToggle()}</div>
    <div class="side items-center gap-2 flex-wrap">
      <span class="dnote">Sort by</span>
      <select onchange="setSort(this.value)">${sortList().map(([v,lab]) =>
        `<option value="${v}"${S.sort===v?" selected":""}>${esc(lab)}</option>`).join("")}</select>
      <span class="dnote">Page size</span>
      <select onchange="setSize(this.value)">${[25,50,100].map(n =>
        `<option value="${n}"${S.size===n?" selected":""}>${n}</option>`).join("")}</select>
    </div></div>`;
}
function setSort(v){ S.sort = v; apply(); }
function setGrain(v){
  if (S.grain === v) return;
  S.grain = v;
  // a sort key that does not exist in the other grain would silently fall back
  if (v === "card" && S.sort === "pages") S.sort = "work";
  if (v === "row" && S.sort === "witnesses") S.sort = "work";
  load(0);
}
function setSize(v){ S.size = parseInt(v, 10) || 25; apply(); }

// Unlike the public page, the POOL gets a chip -- and so do relation, span rank,
// corpus and the escape hatches. The public control has no neutral state, so a
// removable chip would promise one; these all have one, so it can be kept.
function renderChipBar(f){
  const out = [];
  const add = (label, clear) => out.push(`<span class="achip">${esc(label)}
    <button aria-label="Remove" onclick="${clear}">✕</button></span>`);
  if (S.triage.size) add("pool: " + [...S.triage].map(v =>
      (TRIAGE_LABELS[v] || v).split(" — ")[0]).join(", "), `clearAxis('triage')`);
  if (S.relation.size) add("relation: " + [...S.relation].map(v =>
      poolLabel("relation", v)).join(", "), `clearAxis('relation')`);
  if (S.claim) add("span rank: " + poolLabel("claim", S.claim), `clearAxis('claim')`);
  if (S.disagree) add("span rank disagrees with the router", `clearAxis('disagree')`);
  if (S.pool.size) add("pool: " + [...S.pool].map(v =>
      poolLabel("pool", v)).join(", "), `clearAxis('pool')`);
  if (S.poolreason) add("demoted: " + S.poolreason, `clearAxis('poolreason')`);
  // The view gets a chip because it HAS a neutral state ("All findings") for a
  // chip to return it to -- the same test every other chip here passes and the
  // public control fails. Named by its reader-facing label, not its key.
  if (curView() !== "all") add("which findings: " + viewLabel(curView()),
                               `clearAxis('view')`);
  if (S.novelty.size) add("novelty: " + [...S.novelty].join(", "), `clearAxis('novelty')`);
  if (S.domain) add("domain: " + (S.domain === NULL_TOKEN ? "no domain recorded" : S.domain),
                    `clearAxis('domain')`);
  if (S.author) add("author: " + authorLabel(f), `clearAxis('author')`);
  if (S.work) add("work: " + workLabel(f), `clearAxis('work')`);
  if (S.locus) add("locus contains: " + S.locus, `clearAxis('locus')`);
  if (S.locus_from || S.locus_to)
    add("part of work: " + (S.locus_from || "start") + " → " + (S.locus_to || "end"),
        `S.locus_from='';S.locus_to='';apply()`);
  if (S.coverage) add("page coverage: " + ({lt10:"under 10%","10to30":"10–30%",
      "30to40":"30–40%",
      "30to60":"30–60%","60to85":"60–85%",ge85:"85%+"}[S.coverage] || S.coverage),
      `clearAxis('coverage')`);
  if (S.corpus.size) add("corpus: " + [...S.corpus].map(v =>
      corpusLabel(f, v)).join(", "), `clearAxis('corpus')`);
  if (S.scripture.size) add("shared scripture: " + [...S.scripture].map(v =>
      v === "1" ? "flagged" : (v === "0" ? "not flagged" : "not computed"))
      .join(", "), `clearAxis('scripture')`);
  if (S.nontiera) add("only non-tier-A bands", `clearAxis('nontiera')`);
  if (S.adjudicated) add("only rows a person looked at", `clearAxis('adjudicated')`);
  if (S.letters) add("match length: " + (S.letters === "short"
      ? "under " + SHORT_MATCH + " letters" : SHORT_MATCH + " letters or more"),
      `clearAxis('letters')`);
  if (S.graded) add("grading: " + (S.graded === "yes" ? "graded only" : "ungraded only"),
                    `clearAxis('graded')`);
  if (S.q) add("text: " + S.q, `clearAxis('q')`);
  $("chipbar").innerHTML = out.length
    ? out.join("") + `<button class="clearall" onclick="clearAll()">Clear All</button>` : ``;
}
const lookup = (list, v, i) => {
  const hit = (list || []).find(t => String(t[0]) === String(v));
  return hit ? String(hit[i]) : String(v);
};
const viewLabel = v => {
  const hit = VIEWS.find(t => String(t[0]) === String(v));
  return hit ? String(hit[1]) : String(v);
};
const authorLabel = f => lookup(f && f.authors, S.author, 1);
const workLabel = f => lookup(f && f.works, S.work, 1);
const corpusLabel = (f, v) => lookup(f && f.corpus, v, 1);
function clearAxis(k){
  if (k === "q") { S.q = ""; $("q").value = ""; }
  else if (S[k] instanceof Set) S[k] = new Set();
  else if (typeof S[k] === "boolean") S[k] = false;
  else S[k] = "";
  if (k === "author" || k === "work") { const el = $(k + "_t"); if (el) el.value = ""; }
  apply();                                   // clearing any axis resets to page 1
}
function clearAll(){ reset(); }

// ---- load ----------------------------------------------------------------
function errBox(e, retry){
  return `<div class="errbox"><div class="glyph">⚠</div>
    <div><b>The database refused a query.</b></div>
    <div><code>${esc(e.cls || "error")}</code> in <code>${esc(e.query || "?")}</code></div>
    <div class="dnote">${esc(e.msg || "")}</div>
    <button class="fchip" onclick="${retry}">Retry</button></div>`;
}
async function load(newOff){
  if (newOff !== undefined) S.off = newOff;
  const cardGrain = CARDS_OK && S.grain === "card";
  const ep = cardGrain ? "/api/cards" : "/api/rows";
  let d;
  try {
    const r = await fetch(ep + "?" + params({offset: S.off, size: S.size}));
    d = await r.json();
    if (!r.ok || d.error) throw d.error || {cls:"HTTPError", query:"rows",
                                            msg:"HTTP " + r.status};
  } catch (e) {
    $("rows").innerHTML = errBox(e.cls ? e : {cls:"NetworkError", query:"rows",
                                              msg:String(e.message || e)}, "load()");
    $("count").textContent = "query failed";
    return;
  }
  // in card grain the payload's units are CARDS; `LAST` keeps carrying evidence
  // rows, because everything downstream of it (grading, panes) is per-row
  const units = cardGrain ? (d.cards || []) : (d.rows || []);
  total = d.total; shown = units.length;
  LAST = cardGrain ? units.reduce((a, c) => a.concat(c.rows || []), []) : units;
  $("count").textContent = `${num(d.total)} ${cardGrain ? "cards" : "rows"}` +
    (d.total ? ` · showing ${num(S.off+1)}-${num(S.off+shown)}` : ``);
  // typeable page number (owner, 2026-08-31)
  $("pageno").innerHTML = d.total
    ? `<input type="number" min="1" max="${Math.ceil(d.total / S.size)}"
         value="${Math.floor(S.off / S.size) + 1}" style="width:5.5em"
         onchange="gotoPage(this.value)"
         onkeydown="if(event.key==='Enter')gotoPage(this.value)">
       / ${num(Math.ceil(d.total / S.size))}` : ``;
  renderRbar(d);
  renderSession(d.graded_total);
  // The empty state keeps the public shape and drops the pool invitation inside
  // it -- there is no second pool to sell here, only a filter to loosen.
  $("rows").innerHTML = units.length
    ? units.map(cardGrain ? kwCardHtml : rowHtml).join("")
    : `<div class="empty"><div class="glyph">⌕</div><div><b>No results found</b></div>
       <div class="dnote">Loosen a filter — the counts beside each control show
       what is reachable.</div></div>`;
  facets();
}
// paging returns the reader to the top of the new page (owner, 2026-08-31)
function pageTo(off){ load(off); window.scrollTo({top: 0}); }
function next(){ if (S.off + S.size < total) pageTo(S.off + S.size); }
function prev(){ if (S.off > 0) pageTo(Math.max(0, S.off - S.size)); }
function gotoPage(p){
  const pages = Math.max(1, Math.ceil(total / S.size));
  const n = Math.min(pages, Math.max(1, parseInt(p, 10) || 1));
  pageTo((n - 1) * S.size);
}
function exportGrades(){ window.location = "/api/export"; }

function boot(){
  try {
    let t = null;
    try { t = localStorage.getItem("v3review.theme"); } catch (e) {}
    setTheme(t === "dark" ? "dark" : "light");
  } catch (e) {}
}
boot();
load(0);
"""

# ---------------------------------------------------------------------------
# The help panel: read from `meta` at REQUEST time, never retyped (spec 1.4)
# ---------------------------------------------------------------------------

# (meta key, heading, sub-heading, plate class). If the artifact's own
# documentation changes, this panel changes with it -- which is the entire point
# of sourcing it from the DB rather than from a string in this file. `.warn` is
# the gold rule (this will mislead you), `.ok` the green one (this axis was
# actually validated); both are the same treatment as the caveat plate.
HELP_SECTIONS = (
    ("doc.router_verdict", "relation", "the router's verdict — the axis you are grading", "ok"),
    ("doc.claim_type", "span rank", "NOT a relation — read this one carefully", "warn"),
    ("doc.main_pool", "pool", "", ""),
    ("doc.routing_status", "shown / review-only", "", ""),
    ("doc.novelty_status", "novelty", "what this adds to the catalogue", ""),
    ("doc.divergence_correctness", "Which is right?",
     "the buttons — this is the job", ""),
    ("doc.ms_match_vs_ref_match", "the two text panes will not match closely",
     "and that is not an error", "warn"),
    ("doc.known_weakness", "a short match on a famous passage is a known weakness",
     "", "warn"),
    ("doc.scripture_flag", "may rest on shared scripture",
     "a computed label — three detectors, named on the chip", "warn"),
)


def build_help_html(docs) -> str:
    out = []
    for key, head, sub, cls in HELP_SECTIONS:
        body = docs.get(key)
        out.append("<h4>%s%s</h4>" % (
            html.escape(head),
            ' <span class="dnote">(%s)</span>' % html.escape(sub) if sub else ""))
        if body:
            out.append('<div class="dnote %s">%s</div>' % (cls, html.escape(body)))
        else:
            # NEVER silently absent. A definition that vanishes is
            # indistinguishable from one that never existed, and this panel is
            # the only place a teammate learns what the columns mean.
            out.append('<div class="dnote warn">This artifact carries no '
                       '<code>%s</code> row in <code>meta</code>. It was built by an '
                       'older builder, or the row was dropped.</div>'
                       % html.escape(key))
    out.append('<div class="dnote">Every definition above is read out of the '
               'review DB&#39;s own <code>meta</code> table at request time, not '
               'copied into this tool.</div>')
    return "\n".join(out)


def render_page(docs, nums, site, preview_mode, cards_ok=False,
                htr_ok=False) -> str:
    css = CSS_TOKENS + CSS_DISCOVERY + CSS_PRIVATE
    js = (PAGE_JS
          .replace("__SITE__", site.rstrip("/"))
          .replace("__PREVIEW__", preview_mode)
          .replace("__CARDS_OK__", "true" if cards_ok else "false")
          .replace("__HTR_OK__", "true" if htr_ok else "false")
          .replace("__SHORT__", str(int(SHORT_MATCH_LETTERS)))
          .replace("/*__DOCS__*/{}", _js_json(docs))
          .replace("/*__NUMS__*/{}", _js_json(nums))
          .replace("/*__VIEWS__*/[]", _js_json([list(t) for t in NOVELTY_VIEW_LABELS]))
          .replace('/*__VIEWWARN__*/""', _js_json(NOVELTY_VIEW_WARNING)))
    return (PAGE_HTML
            .replace("__CSS__", css)
            .replace("<!--__HELP__-->", build_help_html(docs))
            .replace("__JS__", js))


def _js_json(obj) -> str:
    """JSON safe to drop inside a <script> element.

    `ensure_ascii=True` (the default) escapes every non-ASCII code point,
    which incidentally neutralises U+2028/U+2029 -- valid in JSON but line
    TERMINATORS in JavaScript, so an unescaped one inside a string literal is
    a syntax error rather than a visible bug. `</` is rewritten because a
    `</script` sequence anywhere in the payload would close the element early.
    """
    return json.dumps(obj).replace("</", "<\\/")


# ---------------------------------------------------------------------------
# The server
# ---------------------------------------------------------------------------

# Multi-value axes. A NULL member CANNOT ride in an `IN (...)`, so the wire
# carries `__null__` and the clause becomes `(col IN (...) OR col IS NULL)`.
# Getting this wrong silently drops the 64,406-row never-evaluated block --
# exactly the failure the three-state pool control exists to prevent.
MULTI_FILTERS = (("relation", "router_verdict"),
                 ("novelty", "novelty_status"),
                 ("pool", "main_pool"),
                 ("corpus", "source_corpus"),
                 ("poolreason", "main_pool_reason"),
                 ("scripture", "scripture_flagged"),
                 ("gatediv", "gate_divergence"),
                 ("gatenew", "gate_new_finds"),
                 ("model", "model_verdict"),
                 ("triage", "triage"))

SINGLE_FILTERS = (("work", "work_id"),
                  ("claim", "claim_type"),
                  # `routing_status` gets NO CONTROL (spec 4.1-4.9): it is 100%
                  # redundant with router_verdict (shipped <=> same_work, zero
                  # exceptions), and a control on it would let a reader build
                  # `shipped AND parallel` and see 0 rows with no explanation.
                  # It stays reachable as an API parameter -- review-only rows
                  # are where the quotations live, and a teammate driving the
                  # endpoint directly must be able to name that population --
                  # and it survives on the row as chip 7.
                  ("routing", "routing_status"))

SORT_SQL = {
    # Default: keeps an identification's pages adjacent, which is how a grader
    # reads them.
    "work": "r.work_id, r.sys_id, r.evidence_id",
    "letters": "r.matched_letters DESC, r.evidence_id",
    "coverage": "r.coverage_ppm DESC, r.evidence_id",
    # Surfaces the 69,723 single-row identifications -- the population the
    # multi-folio signal cannot speak for.
    "pages": "r.id_pages ASC, r.work_id, r.sys_id, r.evidence_id",
}

# The facet axes: (out key, value column, label column, the filter key to
# EXCLUDE when computing it). A facet computed WITH its own selection applied
# contains exactly one option -- the thing already chosen -- which forces the
# reader back through "all" before switching. Every other filter still applies,
# so the counts stay honest about the rest of the query.
FACET_AXES = (("relation", "router_verdict", "router_verdict", "relation"),
              ("claim", "claim_type", "claim_type", "claim"),
              ("pool", "main_pool", "main_pool", "pool"),
              ("poolreason", "main_pool_reason", "main_pool_reason", "poolreason"),
              ("novelty", "novelty_status", "novelty_status", "novelty"),
              ("domains", "domain", "domain", "domain"),
              ("works", "work_id", "work_title", "work"),
              ("corpus", "source_corpus", "source_corpus", "corpus"),
              ("routing", "routing_status", "routing_status", "routing"),
              ("band", "confidence_band", "confidence_band", "nontiera"),
              ("adjudication", "adjudication_status", "adjudication_status",
               "adjudicated"),
              ("scripture", "scripture_flagged", "scripture_flagged",
               "scripture"),
              ("gatediv", "gate_divergence", "gate_divergence", "gatediv"),
              ("gatenew", "gate_new_finds", "gate_new_finds", "gatenew"),
              ("model", "model_verdict", "model_verdict", "model"),
              ("triage", "triage", "triage", "triage"))


class QueryFailed(Exception):
    """A SQLite error, tagged with WHICH query failed.

    The public page's outage state is an empty list; that is the wrong shape
    here, because an empty list is indistinguishable from "no rows match" and a
    grader would read a broken query as a finished filter.
    """

    def __init__(self, name, exc):
        super().__init__(str(exc))
        self.name = name
        self.cls = type(exc).__name__
        self.msg = str(exc)

    def payload(self):
        return {"error": {"cls": self.cls, "query": self.name, "msg": self.msg}}


class Handler(BaseHTTPRequestHandler):
    db_path = None
    site = DEFAULT_SITE
    preview_mode = "image"
    # Facets are pure functions of the filter state, and the reader re-issues the
    # same state constantly (every page turn calls facets()). Bounded so a long
    # session cannot grow it without limit. The GRADING counts are deliberately
    # NOT cached with the rest -- they change as the reviewer works, and a work
    # queue whose numbers do not move is a work queue nobody trusts.
    _facet_cache = {}
    _facet_lock = threading.Lock()
    _authors = None
    _nums = None
    _docs = None

    def log_message(self, *a):
        pass

    def handle_one_request(self):
        # A reader who navigates or re-filters mid-response aborts the socket.
        # That is normal client behaviour, not a fault, and printing a traceback
        # for it buries real errors in noise.
        try:
            super().handle_one_request()
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            self.close_connection = True

    # -- plumbing ----------------------------------------------------------
    def _conn(self):
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        # GRADES LIVE IN THEIR OWN FILE. Re-baking the review projection must not
        # destroy grading work, so nothing here ever writes to the review DB.
        con.execute("ATTACH DATABASE ? AS g", (self.db_path + ".grades.db",))
        con.execute("""CREATE TABLE IF NOT EXISTS g.human_grade (
                         evidence_id TEXT PRIMARY KEY,
                         divergence_correctness TEXT,
                         note TEXT,
                         graded_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        return con

    def _send(self, obj, ctype="application/json", raw=None, status=200):
        body = raw if raw is not None else json.dumps(
            obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    @staticmethod
    def _one(q, key, default=""):
        return (q.get(key) or [default])[0]

    @staticmethod
    def _many(q, key):
        out = []
        for raw in q.get(key) or []:
            for part in str(raw).split(","):
                part = part.strip()
                if part:
                    out.append(part)
        return out

    def _query(self, con, name, sql, params=()):
        try:
            return con.execute(sql, params)
        except sqlite3.Error as e:
            raise QueryFailed(name, e)

    # -- author normalisation ---------------------------------------------
    def _author_groups(self, con):
        """key -> {'forms': [...surface...], 'display': surface}.

        Built from the 108 distinct surface values, so it is cheap and cached for
        the process. The filter expands a key to `work_author IN (<every surface
        form>)`, which keeps the index usable -- a SQL-side normalisation
        function would force a full scan of the projection on every request.
        """
        if Handler._authors is None:
            rows = self._query(con, "facets.authors.groups",
                               "SELECT work_author AS a, COUNT(*) AS n FROM facet_row "
                               "WHERE work_author IS NOT NULL GROUP BY 1").fetchall()
            groups = {}
            for r in rows:
                groups.setdefault(author_key(r["a"]), []).append((r["a"], r["n"]))
            Handler._authors = {
                k: {"forms": [a for a, _n in v],
                    # the most frequent surface form is the one shown
                    "display": max(v, key=lambda t: t[1])[0]}
                for k, v in groups.items()}
        return Handler._authors

    # -- WHERE -------------------------------------------------------------
    def _where(self, con, q, exclude=None):
        """`exclude` drops ONE axis from the clause (see FACET_AXES)."""
        cl, pr = [], {}

        for key, col in MULTI_FILTERS:
            if key == exclude:
                continue
            vals = self._many(q, key)
            if not vals:
                continue
            want_null = NULL_TOKEN in vals
            rest = [v for v in vals if v != NULL_TOKEN]
            if key in ("pool", "scripture"):
                # INTEGER columns; bind integers so the comparison never
                # depends on affinity coercion of a text literal.
                rest = [int(v) for v in rest if v in ("0", "1")]
            parts = []
            if rest:
                names = []
                for i, v in enumerate(rest):
                    nm = "%s_%d" % (key, i)
                    pr[nm] = v
                    names.append(":" + nm)
                parts.append("r.%s IN (%s)" % (col, ",".join(names)))
            if want_null:
                parts.append("r.%s IS NULL" % col)
            if parts:
                cl.append("(" + " OR ".join(parts) + ")")

        for key, col in SINGLE_FILTERS:
            if key == exclude:
                continue
            v = self._one(q, key)
            if v == NULL_TOKEN:
                cl.append("r.%s IS NULL" % col)
            elif v:
                cl.append("r.%s = :%s" % (col, key))
                pr[key] = v

        # The GROUPED novelty view. A second, independent clause on the same
        # column as the raw shade facet, which is what makes the two compose:
        # `view=candidates` plus a shade selection is the intersection, and a
        # shade outside the view simply has no rows -- which is the truth, not a
        # swallowed filter. `None` (the `all` view, or any value not in the
        # vocabulary) adds no clause at all; an empty filter is not a filter.
        if exclude != "view":
            shades = NOVELTY_VIEW_SHADES.get(self._one(q, "view"))
            if shades:
                names = []
                for i, shade in enumerate(shades):
                    nm = "__view_%d" % i
                    pr[nm] = shade
                    names.append(":" + nm)
                cl.append("r.novelty_status IN (%s)" % ",".join(names))

        # Domain: a parent selection is a strict SUPERSET of its leaves. All 61
        # non-null values carry ' / ', so a value without it is always a parent.
        if exclude != "domain":
            d = self._one(q, "domain")
            if d == NULL_TOKEN:
                cl.append("r.domain IS NULL")
            elif d and " / " in d:
                cl.append("r.domain = :domain")
                pr["domain"] = d
            elif d:
                cl.append("(r.domain = :domain OR r.domain LIKE :domain_pfx)")
                pr["domain"] = d
                pr["domain_pfx"] = d + " / %"

        # Locus contains. `instr`, not LIKE: the reader types a fragment of a
        # citation address (a tractate, a chapter) and wildcards would only be
        # a way to be surprised.
        if exclude != "locus":
            v = self._one(q, "locus")
            if v:
                cl.append("instr(r.locus_label, :locus) > 0")
                pr["locus"] = v

        # Locus RANGE (From/To), meaningful only with a work selected. The
        # bounds are stream positions: From = the first occurrence of that
        # locus in the work's letter stream, To = the last -- so the range is
        # "from where locus A starts to where locus B ends", which is what the
        # live app's Part-of-work control means.
        if exclude != "locusrange":
            lf, lt = self._one(q, "locus_from"), self._one(q, "locus_to")
            w = self._one(q, "work")
            if w and (lf or lt):
                use_lu = self._has_locus_units(con)
                if lf:
                    row = self._query(
                        con, "where.locus_from",
                        "SELECT MIN(start_offset) AS p FROM locus_unit "
                        "WHERE work_id=:w AND label_he=:l" if use_lu else
                        "SELECT MIN(w_start) AS p FROM facet_row "
                        "WHERE work_id=:w AND locus_label=:l",
                        {"w": w, "l": lf}).fetchone()
                    if row and row["p"] is not None:
                        cl.append("r.w_start >= :lr0")
                        pr["lr0"] = row["p"]
                if lt:
                    if use_lu:
                        # up to (not including) the unit AFTER the last unit
                        # carrying the To label; the last unit of the work has
                        # no successor -> open upper bound
                        row = self._query(
                            con, "where.locus_to",
                            "SELECT MIN(start_offset) AS p FROM locus_unit "
                            "WHERE work_id=:w AND unit_ord > ("
                            "  SELECT MAX(unit_ord) FROM locus_unit"
                            "  WHERE work_id=:w AND label_he=:l)",
                            {"w": w, "l": lt}).fetchone()
                        if row and row["p"] is not None:
                            cl.append("r.w_start < :lr1")
                            pr["lr1"] = row["p"]
                    else:
                        row = self._query(
                            con, "where.locus_to",
                            "SELECT MAX(w_start) AS p FROM facet_row "
                            "WHERE work_id=:w AND locus_label=:l",
                            {"w": w, "l": lt}).fetchone()
                        if row and row["p"] is not None:
                            cl.append("r.w_start <= :lr1")
                            pr["lr1"] = row["p"]

        # Page coverage buckets. 29.8% is the validated witness threshold, so
        # 30-40% is the "barely a witness" band worth suspicion.
        if exclude != "coverage":
            v = self._one(q, "coverage")
            bounds = {"lt10": "r.coverage_ppm < 100000",
                      "10to30": "r.coverage_ppm >= 100000 AND r.coverage_ppm < 300000",
                      "30to40": "r.coverage_ppm >= 300000 AND r.coverage_ppm < 400000",
                      "30to60": "r.coverage_ppm >= 300000 AND r.coverage_ppm < 600000",
                      "60to85": "r.coverage_ppm >= 600000 AND r.coverage_ppm < 850000",
                      "ge85": "r.coverage_ppm >= 850000"}
            if v == NULL_TOKEN:
                cl.append("r.coverage_ppm IS NULL")
            elif v in bounds:
                cl.append("(" + bounds[v] + ")")

        if exclude != "author":
            a = self._one(q, "author")
            if a == NULL_TOKEN:
                cl.append("r.work_author IS NULL")
            elif a:
                grp = self._author_groups(con).get(a)
                if grp:
                    names = []
                    for i, form in enumerate(grp["forms"]):
                        nm = "author_%d" % i
                        pr[nm] = form
                        names.append(":" + nm)
                    cl.append("r.work_author IN (%s)" % ",".join(names))
                else:
                    # An unknown key selects NOTHING. Falling through to "no
                    # clause" would silently widen the query to the whole corpus
                    # while the control still showed a name.
                    cl.append("1 = 0")

        # Match length. A RANGE, not a preset: the sidebar's three chips are two
        # bounds under the hood, and the endpoint takes either bound alone.
        #
        # This axis exists because the row already tells the grader that a match
        # under SHORT_MATCH_LETTERS may rest on shared scripture (chip 12,
        # `doc.known_weakness`) -- and until now there was no way to ASK for that
        # population. 58,461 rows carry the warning and the only route to them
        # was paging a descending sort to its end.
        if exclude != "letters":
            lo = self._int(self._one(q, "minletters"), None)
            hi = self._int(self._one(q, "maxletters"), None)
            if lo is not None:
                cl.append("r.matched_letters >= :__minletters")
                pr["__minletters"] = lo
            if hi is not None:
                cl.append("r.matched_letters <= :__maxletters")
                pr["__maxletters"] = hi

        if exclude != "disagree" and self._one(q, "disagree") == "1":
            cl.append("(r.router_verdict = 'parallel' "
                      "AND r.claim_type = 'direct_witness')")
        if exclude != "nontiera" and self._one(q, "nontiera") == "1":
            cl.append("(r.confidence_band IS NOT NULL "
                      "AND r.confidence_band <> 'tier_a')")
        if exclude != "adjudicated" and self._one(q, "adjudicated") == "1":
            cl.append("r.adjudication_status IN ('provisional', 'human_confirmed')")

        s = self._one(q, "q")
        if s:
            cl.append("(r.shelfmark LIKE :q OR r.sys_id LIKE :q)")
            pr["q"] = "%" + s + "%"

        if exclude != "graded":
            g = self._one(q, "graded")
            if g == "yes":
                cl.append("hg.divergence_correctness IS NOT NULL "
                          "AND hg.divergence_correctness <> ''")
            elif g == "no":
                cl.append("(hg.divergence_correctness IS NULL "
                          "OR hg.divergence_correctness = '')")
        return ("WHERE " + " AND ".join(cl) if cl else ""), pr

    @staticmethod
    def _and(where, *conds):
        """AND extra conditions onto a `_where` clause that may be EMPTY."""
        conds = [c for c in conds if c]
        if not conds:
            return where
        return ((where + " AND ") if where else "WHERE ") + " AND ".join(conds)

    # -- counting ----------------------------------------------------------
    # An EMPTY STRING is not a grade. The `clear` button writes one, so every
    # graded/ungraded test below has to say `<> ''` as well as `IS NOT NULL`;
    # testing only for NULL counts a cleared row as graded and the work queue
    # then never empties.
    _GRADED = ("hg.divergence_correctness IS NOT NULL "
               "AND hg.divergence_correctness <> ''")
    _UNGRADED = ("(hg.divergence_correctness IS NULL "
                 "OR hg.divergence_correctness = '')")

    def _grade_join(self, q, exclude=None, force=False):
        """The join onto the grades DB, added ONLY when the query reads it.

        Carrying it on every facet query cost seconds over 254,612 rows for a
        column most of those queries never read -- and a facets response slow
        enough for the browser to cancel leaves every control empty with nothing
        saying why. That is a real failure this tool has already had.
        """
        wanted = force or (exclude != "graded"
                           and self._one(q, "graded") in ("yes", "no"))
        return ("LEFT JOIN g.human_grade hg ON hg.evidence_id = r.evidence_id"
                if wanted else "")

    def _count(self, con, name, q, exclude=None, extra=None, force_join=False):
        """COUNT over the SLIM projection. Never over review_row.

        `extra` is a literal predicate this file wrote (never reader input); the
        reader's values all arrive through `_where` as bound parameters.
        """
        where, pr = self._where(con, q, exclude=exclude)
        join = self._grade_join(q, exclude, force=force_join)
        if extra:
            where = (where + " AND " + extra) if where else "WHERE " + extra
        return self._query(con, name,
                           "SELECT COUNT(*) AS c FROM facet_row r %s %s"
                           % (join, where), pr).fetchone()["c"]

    @staticmethod
    def _int(v, default):
        try:
            return int(v)
        except (TypeError, ValueError):
            return default

    # -- the row payload ---------------------------------------------------
    @staticmethod
    def _public_row(row):
        """The ONE place a stored row becomes a payload the browser may see.

        `source_corpus` is DELETED, not merely left unrendered: the label is
        computed here and the raw code never crosses the wire, which makes a raw
        render impossible in the client rather than merely discouraged. An
        unmapped code becomes a neutral placeholder -- fail-closed.

        The six text pieces also lose their STRUCTURAL markers here -- the same
        display-only cleaning the live site applies to its own excerpt panes.
        """
        d = dict(row)
        d["corpus_label"] = corpus_label(d.pop("source_corpus", None))
        for k in ("ms_before", "ms_match", "ms_after",
                  "ref_before", "ref_match", "ref_after"):
            if k in d:
                d[k] = clean_display_markers(d[k])
        url, label = sefaria_ref(d.get("work_title"), d.get("locus_label"))
        if url:
            d["sefaria_url"], d["sefaria_label"] = url, label
        return d

    # -- what the novelty gate actually read --------------------------------
    #
    # THE BUG THIS CLOSES. The row's "Catalogued as:" line is `libraries.csv`
    # column 7 and nothing else, but the gate judged on a COMBINED catalogue
    # text plus bibliography, PGP, FGP and an M-source witness count. On 83% of
    # `confirms` rows the displayed title differs from what was read, so a
    # CORRECT label looks absurd. `gate_fact` (attached by
    # scripts/attach_gate_facts.py) carries the gate's own inputs, and this
    # joins them onto the 25 rows ON SCREEN only -- filters, facets and counts
    # still run entirely on the slim `facet_row`, which is the whole reason
    # this tool stays fast.
    _GATE_SOURCES = (
        ("gate_catalogue", "catalogue"),
        ("bib_text", "bibliography"),
        ("pgp_text", "PGP"),
        ("fgp_text", "FGP"),
    )
    # `gate_fact` is ADDITIVE and may simply not be there (an older review DB,
    # or the attach script never run). Resolved once per process; a missing
    # table degrades to a block that says so, never to a 500 on the rows
    # endpoint -- the grading tool must keep painting.
    _gate_table = None

    def _has_gate(self, con):
        if Handler._gate_table is None:
            Handler._gate_table = bool(self._query(
                con, "rows.gate_probe",
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='gate_fact'").fetchone())
        return Handler._gate_table

    # ---- the CARD grain (scripts/attach_review_cards.py) --------------------
    # One card per (page_id, known_work): the unit of the question "is this page
    # this work?", with every witness's evidence beneath it. Additive, like
    # every other satellite -- and FAIL-CLOSED: card mode is offered only when
    # the projection covers exactly today's review_row AND was built from the
    # registry now in the file. A stale card table must never be served as
    # current, so the probe compares both numbers rather than trusting presence.
    _card_grain = None

    def _has_cards(self, con):
        if Handler._card_grain is None:
            ok = bool(self._query(
                con, "rows.card_probe",
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='card_member'").fetchone())
            if ok:
                m = {x["k"]: x["v"] for x in self._query(
                    con, "rows.card_meta",
                    "SELECT key AS k, value AS v FROM meta WHERE key IN "
                    "('card_grain.members','card_grain.registry_pins_sha256',"
                    "'work_registry.pins_sha256')").fetchall()}
                live = self._query(con, "rows.card_rowcount",
                                   "SELECT COUNT(*) AS c FROM review_row"
                                   ).fetchone()["c"]
                ok = (m.get("card_grain.members") == str(live)
                      and m.get("card_grain.registry_pins_sha256")
                      == m.get("work_registry.pins_sha256"))
            Handler._card_grain = ok
        return Handler._card_grain

    # `htr_page` + the per-row `htr_*` stamps are attached by
    # scripts/attach_htr_realignment.py. Fail-closed like the card grain: the
    # feature is on only when the table exists AND the number of stamped rows
    # equals the count the pass itself recorded -- a half-run or a later row
    # edit hides the pane rather than serving a stale address.
    _htr_table = None

    def _has_htr(self, con):
        if Handler._htr_table is None:
            ok = bool(self._query(
                con, "rows.htr_probe",
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='htr_page'").fetchone())
            if ok:
                m = {x["k"]: x["v"] for x in self._query(
                    con, "rows.htr_meta",
                    "SELECT key AS k, value AS v FROM meta WHERE key="
                    "'htr_realign.rows'").fetchall()}
                live = self._query(
                    con, "rows.htr_rowcount",
                    "SELECT COUNT(*) AS c FROM review_row WHERE "
                    "htr_align_status IS NOT NULL").fetchone()["c"]
                ok = live > 0 and m.get("htr_realign.rows") == str(live)
            Handler._htr_table = ok
        return Handler._htr_table

    def _api_htr_page(self, q):
        """The HTR text of ONE substituted page, exactly as stored: the row's
        htr_page_char_* index this string, so no display cleaning is applied."""
        pid = self._one(q, "page_id", "").strip()
        con = self._conn()
        try:
            if not pid or not self._has_htr(con):
                return self._send({"error": "this file carries no HTR page text"})
            r = self._query(
                con, "htr.page",
                "SELECT page_id, search_text_source AS source, "
                "substitution_score AS score, htr_text AS text, "
                "htr_n_chars AS n, htr_file_char_start AS file_start, "
                "htr_file_char_end AS file_end, nfc_ok FROM htr_page "
                "WHERE page_id=:p", {"p": pid}).fetchone()
            if not r:
                return self._send({"error": "this page was not substituted; its "
                                   "text is already the one in Transcriptions.txt"})
            return self._send(dict(r))
        finally:
            con.close()

    # `reference_witness`/`source_file` exist only in schema-v2 artifacts (the
    # v5 file); a v3 db must keep working with the block saying "not recorded".
    # Same probe-once pattern as `gate_fact`.
    _witness_tables = None

    def _has_witness(self, con):
        if Handler._witness_tables is None:
            Handler._witness_tables = bool(self._query(
                con, "rows.witness_probe",
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='reference_witness'").fetchone())
        return Handler._witness_tables

    def _attach_witness(self, con, rows):
        """Hang the reference-side source file on each payload as `src`.

        `display_ref` is NULL on every masked source BY CONSTRUCTION (the
        builder stores the real path only in the local key file, outside the
        repo), so the fallback to the opaque `ref_id` here is showing a
        codename, never a filename. One IN(...) query for the <=25 witnesses
        on screen -- the pattern `_attach_gate` set.
        """
        if not rows or not self._has_witness(con):
            return
        wids = sorted({r["witness_id"] for r in rows if r.get("witness_id")})
        if not wids:
            return
        names = {"w%d" % i: w for i, w in enumerate(wids)}
        info = {}
        for x in self._query(
                con, "rows.witness",
                "SELECT rw.witness_id AS w, sf.display_ref AS f, "
                "sf.ref_id AS rid, sf.masked AS m, sf.kind AS k "
                "FROM reference_witness rw "
                "JOIN source_file sf ON sf.id = rw.source_file_id "
                "WHERE rw.witness_id IN (%s)"
                % ",".join(":" + n for n in names), names).fetchall():
            info[x["w"]] = {"file": x["f"], "ref_id": x["rid"],
                            "masked": bool(x["m"]), "kind": x["k"]}
        for r in rows:
            r["src"] = info.get(r.get("witness_id"))

    # `scripture_fact` (scripts/attach_scripture_facts.py) is additive, like
    # `gate_fact`: absent on a v3-era file or before the attach script ran.
    _scripture_table = None

    def _has_scripture(self, con):
        if Handler._scripture_table is None:
            Handler._scripture_table = bool(self._query(
                con, "rows.scripture_probe",
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='scripture_fact'").fetchone())
        return Handler._scripture_table

    def _attach_scripture(self, con, rows):
        """Hang the shared-scripture detectors on each payload as `scripture`."""
        if not rows or not self._has_scripture(con):
            return
        names = {"e%d" % i: r["evidence_id"] for i, r in enumerate(rows)}
        info = {}
        for x in self._query(
                con, "rows.scripture",
                "SELECT evidence_id, bible_share, canon_share, flank_cite, "
                "flank_kind, mask_distance, mask_overlap, flagged "
                "FROM scripture_fact WHERE evidence_id IN (%s)"
                % ",".join(":" + n for n in names), names).fetchall():
            info[x["evidence_id"]] = {
                "bible": x["bible_share"], "canon": x["canon_share"],
                "flank": x["flank_kind"], "mask_distance": x["mask_distance"],
                "mask_overlap": x["mask_overlap"],
                "flagged": bool(x["flagged"])}
        for r in rows:
            r["scripture"] = info.get(r["evidence_id"])

    # `work_alias_fact` (scripts/attach_work_alias.py) is additive too.
    _alias_table = None

    def _has_alias(self, con):
        if Handler._alias_table is None:
            Handler._alias_table = bool(self._query(
                con, "rows.alias_probe",
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='work_alias_fact'").fetchone())
        return Handler._alias_table

    def _attach_alias_twin(self, con, rows):
        """When the SAME page also matches a linked twin of this row's work
        (the same work under another corpus), name the twin on the payload --
        the duplication is visible instead of puzzling. One indexed query per
        on-screen row."""
        if not rows or not self._has_alias(con):
            return
        for d in rows:
            titles = [x["t"] for x in self._query(
                con, "rows.alias_twin",
                "SELECT DISTINCT b.work_title AS t FROM work_alias_fact w "
                "JOIN review_row b ON b.page_id = :page AND b.work_id = "
                "  CASE WHEN w.rs_work = :wid THEN w.base_work ELSE w.rs_work END "
                "WHERE :wid IN (w.rs_work, w.base_work)",
                {"page": d.get("page_id"), "wid": d.get("work_id")}).fetchall()]
            if titles:
                d["alias_twin"] = titles[:3]

    # `gate_verdict_fact` (scripts/attach_gate_verdicts.py) is additive too.
    _llm_table = None

    def _has_llm(self, con):
        if Handler._llm_table is None:
            Handler._llm_table = bool(self._query(
                con, "rows.llm_probe",
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='gate_verdict_fact'").fetchone())
        return Handler._llm_table

    def _attach_llm_verdicts(self, con, rows):
        """Hang the LLM adjudication's reason/doubt on each payload as `llm`
        -- {task: {verdict, reason, doubt}} at pair grain, for the <=25 rows
        on screen. The VERDICT itself also rides the facet columns (filterable);
        this carries the prose the chip tooltip shows."""
        if not rows or not self._has_llm(con):
            return
        pairs, params, clauses = [], [], []
        for d in rows:
            key = (d.get("sys_id"), d.get("work_id"))
            if key in pairs or not key[0]:
                continue
            pairs.append(key)
            clauses.append("(sys_id = ? AND work_id = ?)")
            params.extend(key)
        if not pairs:
            return
        got = {}
        for x in self._query(
                con, "rows.llm",
                "SELECT sys_id, work_id, task, verdict, reason, doubt "
                "FROM gate_verdict_fact WHERE " + " OR ".join(clauses),
                params).fetchall():
            got.setdefault((x["sys_id"], x["work_id"]), {})[x["task"]] = {
                "verdict": x["verdict"],
                "reason": self._route_corpus_mentions(x["reason"]),
                "doubt": self._route_corpus_mentions(x["doubt"])}
        for d in rows:
            d["llm"] = got.get((d.get("sys_id"), d.get("work_id")))

    @staticmethod
    def _route_corpus_mentions(text):
        """Every corpus name in gate text goes out through `corpus_label()`.

        The stored codename is already the masked one, so this is a no-op on
        today's data -- which is the point: the rendered string is produced BY
        the redaction map rather than merely believed to agree with it, so a
        source that ever ships a raw name cannot reach the browser unnoticed.
        """
        label = corpus_label("msource")
        out = str(text or "")
        for token in ("M-source", "M-SOURCE", "m-source"):
            out = out.replace(token, label)
        return out

    def _attach_gate(self, con, rows):
        """Hang the gate's own evidence on each payload as `read`.

        `read` is None when the pair was never put to the gate (novelty
        `not_checked`); the client says that plainly rather than drawing an
        empty block.
        """
        for d in rows:
            d["read"] = None
        if not self._has_gate(con):
            for d in rows:
                d["read"] = {"missing_table": True}
            return
        pairs, params, clauses = [], [], []
        for d in rows:
            key = (d.get("sys_id"), d.get("work_id"))
            if key in pairs or not key[0]:
                continue
            pairs.append(key)
            clauses.append("(sys_id = ? AND ref_work_id = ?)")
            params.extend(key)
        if not pairs:
            return
        got = {}
        for x in self._query(
                con, "rows.gate",
                "SELECT * FROM gate_fact WHERE " + " OR ".join(clauses),
                params).fetchall():
            got[(x["sys_id"], x["ref_work_id"])] = x
        for d in rows:
            x = got.get((d.get("sys_id"), d.get("work_id")))
            if x is None:
                continue
            items = []
            for col, label in self._GATE_SOURCES:
                text = (x[col] or "").strip()
                if text:
                    items.append({"label": label,
                                  "text": self._route_corpus_mentions(text)})
            read = {"items": items, "reason": x["heuristic_reason"] or ""}
            # The thin title is worth naming ONLY when it differs from what was
            # read -- that difference IS the bug the block exists to explain.
            thin = (x["displayed_title"] or "").strip()
            combined = (x["gate_catalogue"] or "").strip()
            read["thin_title"] = thin if (thin and thin != combined) else ""
            msrc = (x["msrc_text"] or "").strip()
            if msrc:
                # PER-WORK and COUNT-ONLY. `witness_conf IS NULL` means nothing
                # tied THIS manuscript to that corpus -- the line is then not
                # about this manuscript at all, and 725 `confirms` rows rest on
                # nothing else. Both facts travel with the text, never inferred
                # in the client.
                read["msrc"] = {
                    "text": self._route_corpus_mentions(msrc),
                    "label": corpus_label("msource"),
                    "about_this_ms": x["witness_conf"] is not None,
                    "conf": x["witness_conf"] or "",
                }
            else:
                read["msrc"] = None
            read["nothing_else"] = bool(
                not items and read["msrc"] and not read["msrc"]["about_this_ms"])
            read["empty"] = not items and not read["msrc"]
            d["read"] = read

    # -- facets ------------------------------------------------------------
    def _facets(self, con, q):
        out = {}
        for key, valcol, labcol, own in FACET_AXES:
            fw, fp = self._where(con, q, exclude=own)
            join = self._grade_join(q, own)
            # NO SILENT CAP. An earlier version stopped at 400 of 1,269 works
            # with nothing saying so. Facet lists are small; they go back whole.
            rows = self._query(
                con, "facets." + key,
                "SELECT r.%s AS v, MAX(r.%s) AS lab, COUNT(*) AS n "
                "FROM facet_row r %s %s GROUP BY 1 ORDER BY n DESC"
                % (valcol, labcol, join, fw), fp).fetchall()
            items = []
            for x in rows:
                # The VALUE stays the raw code -- it is what the filter compares
                # -- and only the LABEL goes through the redaction map.
                lab = corpus_label(x["v"]) if key == "corpus" else x["lab"]
                items.append([NULL_TOKEN if x["v"] is None else x["v"], lab, x["n"]])
            out[key] = items
        out["authors"] = self._author_facet(con, q)

        # Works sharing one display title (13 R-source groups: two works both
        # called רש״י, three called רא״ש...) get their DOMAIN leaf appended, so
        # the dropdown separates them by what they are, not by an opaque id.
        works = out.get("works") or []
        lab_n = {}
        for it in works:
            lab_n[it[1]] = lab_n.get(it[1], 0) + 1
        dups = [it[0] for it in works
                if lab_n[it[1]] > 1 and it[0] != NULL_TOKEN]
        if dups:
            names = {"w%d" % i: v for i, v in enumerate(dups)}
            dmap = dict(self._query(
                con, "facets.workdomain",
                "SELECT work_id, MAX(domain) FROM facet_row "
                "WHERE work_id IN (%s) GROUP BY 1"
                % ",".join(":" + n for n in names), names).fetchall())
            for it in works:
                d = dmap.get(it[0])
                if lab_n[it[1]] > 1 and d:
                    it[1] = "%s — %s" % (it[1], d.split(" / ")[-1])

        # The From/To range control: this work's ATOMIC loci in reading order,
        # from the embedded `locus_unit` table (the live app's own list) --
        # never from the rows' labels, which on the base corpora are RANGE
        # labels ("פרק ב–ג") whenever a claim spans units. Falls back to the
        # row labels only on an artifact without the table.
        w = self._one(q, "work")
        if w:
            if self._has_locus_units(con):
                loci = [x[0] for x in self._query(
                    con, "facets.loci",
                    "SELECT label_he FROM locus_unit WHERE work_id=:w "
                    "ORDER BY unit_ord", {"w": w}).fetchall()]
                # consecutive duplicates collapse (a label can repeat when a
                # unit splits); order is preserved
                out["loci"] = [l for i, l in enumerate(loci)
                               if i == 0 or l != loci[i - 1]]
            else:
                out["loci"] = [x[0] for x in self._query(
                    con, "facets.loci_fallback",
                    "SELECT locus_label FROM facet_row WHERE work_id=:w "
                    "AND locus_label IS NOT NULL AND locus_label != '' "
                    "GROUP BY 1 ORDER BY MIN(w_start)", {"w": w}).fetchall()]
        return out

    _locus_units = None

    def _has_locus_units(self, con):
        if Handler._locus_units is None:
            Handler._locus_units = bool(self._query(
                con, "facets.lu_probe",
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='locus_unit'").fetchone())
        return Handler._locus_units

    def _author_facet(self, con, q):
        """`[key, display, n]` -- one entry per PERSON, not per surface spelling.

        Three collision groups covered 30,501 rows and the largest split one
        author across 574 / 3,486 / 12,674, so picking one entry hid 82% of that
        author's rows with nothing saying so. The counts are summed over the
        folded group, so the number beside a name is the number the filter
        returns.
        """
        fw, fp = self._where(con, q, exclude="author")
        join = self._grade_join(q, "author")
        rows = self._query(con, "facets.authors",
                           "SELECT r.work_author AS v, COUNT(*) AS n "
                           "FROM facet_row r %s %s GROUP BY 1" % (join, fw),
                           fp).fetchall()
        groups = self._author_groups(con)
        folded, nulls = {}, 0
        for x in rows:
            if x["v"] is None:
                nulls += x["n"]
                continue
            k = author_key(x["v"])
            e = folded.setdefault(k, [0, (groups.get(k) or {}).get("display")
                                      or x["v"]])
            e[0] += x["n"]
        items = [[k, disp, n] for k, (n, disp) in folded.items()]
        items.sort(key=lambda t: -t[2])
        if nulls:
            items.append([NULL_TOKEN, "no author recorded", nulls])
        return items

    def _grading_counts(self, con, q):
        """Deliberately NOT cached: they change as the reviewer works, and a work
        queue whose numbers do not move is a work queue nobody trusts."""
        return {
            "graded_total_here": self._count(con, "facets.graded_total_here", q,
                                             exclude="graded"),
            "graded_n": self._count(con, "facets.graded_n", q, exclude="graded",
                                    extra=self._GRADED, force_join=True),
            "ungraded_n": self._count(con, "facets.ungraded_n", q,
                                      exclude="graded", extra=self._UNGRADED,
                                      force_join=True),
        }

    # -- the page ----------------------------------------------------------
    def _read_docs(self, con):
        """The column definitions, read from the artifact's OWN `meta` table.

        Never retyped into this file: if the artifact's documentation changes,
        the help panel and every tooltip change with it.
        """
        rows = self._query(con, "page.docs",
                           "SELECT key, value FROM meta "
                           "WHERE key LIKE 'doc.%'").fetchall()
        docs = {r["key"]: r["value"] for r in rows}
        Handler._docs = docs
        return docs

    def _read_nums(self, con):
        """The three corpus-wide numbers the prose quotes. Cached: they are three
        scans of the projection and they cannot move while the process runs."""
        if Handler._nums is None:
            r = self._query(
                con, "page.nums",
                "SELECT SUM(novelty_status = 'not_checked') AS not_checked, "
                "SUM(novelty_status = 'not_checked' AND main_pool IS NULL) "
                "  AS never_evaluated, "
                "SUM(router_verdict = 'parallel' "
                "    AND claim_type = 'direct_witness') AS disagree "
                "FROM facet_row").fetchone()
            Handler._nums = {k: (r[k] or 0) for k in
                             ("not_checked", "never_evaluated", "disagree")}
        return Handler._nums

    def _page(self):
        con = self._conn()
        cards_ok = htr_ok = False
        try:
            docs, nums = self._read_docs(con), self._read_nums(con)
            cards_ok = self._has_cards(con)
            htr_ok = self._has_htr(con)
        except QueryFailed:
            # The page must still paint. The help panel says so itself when a
            # definition is missing, and the row endpoint reports the real
            # failure in full -- a blank window would say nothing at all.
            docs, nums = Handler._docs or {}, Handler._nums or {}
        finally:
            con.close()
        body = render_page(docs, nums, self.site, self.preview_mode,
                           cards_ok=cards_ok, htr_ok=htr_ok).encode("utf-8")
        self._send(None, "text/html; charset=utf-8", body)

    # -- endpoints ---------------------------------------------------------
    def _api_facets(self, q):
        ckey = tuple(sorted((k, tuple(v)) for k, v in q.items()
                            if k not in ("offset", "size")))
        with self._facet_lock:
            hit = self._facet_cache.get(ckey)
        con = self._conn()
        try:
            if hit is None:
                hit = self._facets(con, q)
                # The three escape-hatch counts, each computed with its OWN axis
                # excluded -- otherwise every one of them reads as its own
                # current selection and the chip can never be turned back on.
                hit["disagree_n"] = self._count(
                    con, "facets.disagree_n", q, exclude="disagree",
                    extra="(r.router_verdict = 'parallel' "
                          "AND r.claim_type = 'direct_witness')")
                hit["nontiera_n"] = self._count(
                    con, "facets.nontiera_n", q, exclude="nontiera",
                    extra="(r.confidence_band IS NOT NULL "
                          "AND r.confidence_band <> 'tier_a')")
                hit["adjudicated_n"] = self._count(
                    con, "facets.adjudicated_n", q, exclude="adjudicated",
                    extra="r.adjudication_status IN "
                          "('provisional', 'human_confirmed')")
                # The two sides of the short-match split, each computed with the
                # length axis excluded so the chip the reader is standing on
                # still shows the size of the other side.
                hit["short_n"] = self._count(
                    con, "facets.short_n", q, exclude="letters",
                    extra="r.matched_letters < %d" % SHORT_MATCH_LETTERS)
                hit["long_n"] = self._count(
                    con, "facets.long_n", q, exclude="letters",
                    extra="r.matched_letters >= %d" % SHORT_MATCH_LETTERS)
                with self._facet_lock:
                    if len(self._facet_cache) > 200:
                        self._facet_cache.clear()
                    self._facet_cache[ckey] = hit
            out = dict(hit)
            out.update(self._grading_counts(con, q))
        finally:
            con.close()
        self._send(out)

    def _api_rows(self, q):
        off = max(0, self._int(self._one(q, "offset", "0"), 0))
        size = self._int(self._one(q, "size", ""), PAGE_SIZE)
        if size not in PAGE_SIZES:
            size = PAGE_SIZE
        order = SORT_SQL.get(self._one(q, "sort", "work"), SORT_SQL["work"])
        con = self._conn()
        try:
            where, pr = self._where(con, q)
            join = self._grade_join(q)
            total = self._query(con, "rows.total",
                                "SELECT COUNT(*) AS c FROM facet_row r %s %s"
                                % (join, where), pr).fetchone()["c"]
            # EXACT, never capped. A grading tool's totals must be exact; a
            # capped total reported as exact is a correctness defect.
            ident = self._query(
                con, "rows.identifications",
                "SELECT COUNT(*) AS c FROM (SELECT DISTINCT r.sys_id, r.work_id "
                "FROM facet_row r %s %s)" % (join, where), pr).fetchone()["c"]
            graded_here = self._count(con, "rows.graded_here", q,
                                      extra=self._GRADED, force_join=True)
            # THE PAGE ADDRESSES, off the slim table: filter, sort and paginate
            # all run over 40 MB. Only the 25 addresses that survive are then
            # looked up in the 1.4 GB table that carries both sides of the text.
            ids = self._query(
                con, "rows.page",
                "SELECT r.evidence_id AS e FROM facet_row r %s %s "
                "ORDER BY %s LIMIT :__lim OFFSET :__off" % (join, where, order),
                dict(pr, __lim=size, __off=off)).fetchall()
            evs = [x["e"] for x in ids]
            rows = []
            if evs:
                names = {"e%d" % i: e for i, e in enumerate(evs)}
                fat = self._query(
                    con, "rows.body",
                    "SELECT r.*, fr.triage AS triage, fr.formula_kind AS formula_kind, "
                    "fr.gate_divergence AS gate_divergence, "
                    "fr.gate_new_finds AS gate_new_finds, "
                    "hg.divergence_correctness AS grade, "
                    "hg.note AS note FROM review_row r "
                    "LEFT JOIN facet_row fr ON fr.evidence_id = r.evidence_id "
                    "LEFT JOIN g.human_grade hg ON hg.evidence_id = r.evidence_id "
                    "WHERE r.evidence_id IN (%s)"
                    % ",".join(":" + n for n in names), names).fetchall()
                by_id = {x["evidence_id"]: x for x in fat}
                # Re-ordered in Python: an IN(...) lookup has no order of its own,
                # and the sort the reader chose is the order they are grading in.
                for e in evs:
                    if e not in by_id:
                        continue
                    d = self._public_row(by_id[e])
                    d["grade"] = d.get("grade") or ""
                    d["note"] = d.get("note") or ""
                    rows.append(d)
                # ONE extra query each, for the 25 addresses already on screen.
                self._attach_gate(con, rows)
                self._attach_witness(con, rows)
                self._attach_scripture(con, rows)
                self._attach_llm_verdicts(con, rows)
                self._attach_alias_twin(con, rows)
            graded_total = self._query(
                con, "rows.graded_total",
                "SELECT COUNT(*) AS c FROM g.human_grade WHERE "
                "divergence_correctness IS NOT NULL "
                "AND divergence_correctness <> ''").fetchone()["c"]
        finally:
            con.close()
        self._send({"total": total, "rows": rows, "identifications": ident,
                    "graded_here": graded_here, "graded_total": graded_total})

    # cards a grader can page through: sorts over the card's OWN aggregates, so
    # a page turn never scans the fat table
    CARD_SORT_SQL = {
        "work": "c.kw_id, c.sys_id, c.card_id",
        "letters": "c.best_matched_letters DESC, c.card_id",
        "coverage": "c.best_coverage_ppm DESC, c.card_id",
        "witnesses": "c.evidence_rows DESC, c.witnesses DESC, c.card_id",
    }

    def _witness_strip(self, con, cards):
        """Every witness of the card's known work, aligned here or not.

        Honesty rule. A witness with no evidence on this page reads "no
        returned alignment" -- the engine ran and returned nothing, which is
        NOT the same as the witness being irrelevant. The only case where
        "not applicable" is provable is a division-scoped witness of a
        container whose rows on THIS page routed to a DIFFERENT division: the
        prefixes partition that container's rows, so this division demonstrably
        has none here. Everything else keeps the humbler label.
        """
        if not cards:
            return
        kws = sorted({c["kw_id"] for c in cards})
        names = {"k%d" % i: k for i, k in enumerate(kws)}
        members = defaultdict(list)
        for x in self._query(
                con, "cards.members",
                "SELECT kw_id AS kw, work_id AS w, scope AS s, "
                "scope_prefix AS p, basis AS b, route_basis AS rb, "
                "evidence_rows AS n FROM known_work_member WHERE kw_id IN (%s)"
                % ",".join(":" + n for n in names), names).fetchall():
            members[x["kw"]].append(dict(work_id=x["w"], scope=x["s"],
                                         scope_prefix=x["p"], basis=x["b"],
                                         route_basis=x["rb"], work_rows=x["n"]))
        for c in cards:
            here = {}
            for r in c.get("rows", ()):
                key = (r.get("card_work_id"), r.get("card_scope"))
                here[key] = here.get(key, 0) + 1
            strip = []
            for m in sorted(members.get(c["kw_id"], ()),
                            key=lambda m: (m["work_id"], m["scope"])):
                key = (m["work_id"], m["scope"])
                d = dict(m, rows_here=here.get(key, 0))
                if d["rows_here"]:
                    d["status"] = "aligned"
                elif m["scope_prefix"] and any(
                        k[0] == m["work_id"] and k[1] != m["scope"]
                        for k in here):
                    d["status"] = "not_applicable"
                    d["why"] = ("this page's rows of the same container belong "
                                "to another division")
                else:
                    d["status"] = "no_returned_alignment"
                strip.append(d)
            c["witness_strip"] = strip

    def _api_cards(self, q):
        """The card grain: one card per (page_id, known_work).

        A card is returned when ANY of its evidence rows passes the filter --
        the grader asked to see identifications of that kind, and hiding the
        card's other witnesses would misrepresent what the page carries. Rows
        that matched are flagged `matched: true` so the card can say which of
        its evidence the filter selected.
        """
        off = max(0, self._int(self._one(q, "offset", "0"), 0))
        size = self._int(self._one(q, "size", ""), PAGE_SIZE)
        if size not in PAGE_SIZES:
            size = PAGE_SIZE
        order = self.CARD_SORT_SQL.get(self._one(q, "sort", "work"),
                                       self.CARD_SORT_SQL["work"])
        con = self._conn()
        try:
            if not self._has_cards(con):
                self._send({"error": "card grain unavailable",
                            "detail": "run scripts/attach_review_cards.py "
                                      "against this db (and rebuild it after "
                                      "any registry rebuild)"}, status=409)
                return
            where, pr = self._where(con, q)
            join = self._grade_join(q)
            # addressing ONE card (or one page's cards) -- a grader linking a
            # colleague to the card they are asking about. Parameter names are
            # prefixed so they cannot collide with a filter's own bindings, and
            # conditions are ANDed through `_and` because `_where` returns an
            # EMPTY string when nothing is filtered.
            pr = dict(pr)
            conds = []
            if self._one(q, "card"):
                conds.append("c.card_id = :__cid")
                pr["__cid"] = self._one(q, "card")
            if self._one(q, "page"):
                conds.append("c.page_id = :__pid")
                pr["__pid"] = self._one(q, "page")
            where = self._and(where, *conds)
            sel = ("FROM card c JOIN card_member cm ON cm.card_id = c.card_id "
                   "JOIN facet_row r ON r.evidence_id = cm.evidence_id %s %s"
                   % (join, where))
            total = self._query(
                con, "cards.total",
                "SELECT COUNT(*) AS c FROM (SELECT c.card_id %s "
                "GROUP BY c.card_id)" % sel, pr).fetchone()["c"]
            ids = self._query(
                con, "cards.page",
                "SELECT c.card_id AS id %s GROUP BY c.card_id ORDER BY %s "
                "LIMIT :__lim OFFSET :__off" % (sel, order),
                dict(pr, __lim=size, __off=off)).fetchall()
            cids = [x["id"] for x in ids]
            cards, rows = [], []
            if cids:
                names = {"c%d" % i: c for i, c in enumerate(cids)}
                inlist = ",".join(":" + n for n in names)
                by_id = {}
                # the known work's own identity travels WITH the card: the
                # header asks "is this page <this work>?", and `provisional`
                # plus the title basis are how the reader knows how firm that
                # name is (Codex round-3/4: never hide the provisional flag)
                for x in self._query(
                        con, "cards.body",
                        "SELECT c.*, k.title AS kw_title, k.author AS kw_author, "
                        "k.title_basis AS kw_title_basis, "
                        "k.author_basis AS kw_author_basis, "
                        "k.main_witness_work AS kw_main_work, "
                        "k.main_witness_scope AS kw_main_scope "
                        "FROM card c JOIN known_work k ON k.kw_id = c.kw_id "
                        "WHERE c.card_id IN (%s)" % inlist,
                        names).fetchall():
                    by_id[x["card_id"]] = {k: x[k] for k in x.keys()}
                    by_id[x["card_id"]]["rows"] = []
                # the card's OWN evidence -- every row of it, not only the rows
                # the filter matched, else a card would misreport its witnesses
                mem = self._query(
                    con, "cards.members_rows",
                    "SELECT cm.card_id AS cid, cm.evidence_id AS e, "
                    "cm.work_id AS w, cm.scope AS s, cm.scope_prefix AS p, "
                    "cm.member_basis AS mb, cm.route_basis AS rb "
                    "FROM card_member cm WHERE cm.card_id IN (%s)" % inlist,
                    names).fetchall()
                evs = [x["e"] for x in mem]
                fat = {}
                if evs:
                    enames = {"e%d" % i: e for i, e in enumerate(evs)}
                    for x in self._query(
                            con, "cards.rows",
                            "SELECT r.*, fr.triage AS triage, "
                            "fr.formula_kind AS formula_kind, "
                            "fr.gate_divergence AS gate_divergence, "
                            "fr.gate_new_finds AS gate_new_finds, "
                            "hg.divergence_correctness AS grade, "
                            "hg.note AS note FROM review_row r "
                            "LEFT JOIN facet_row fr "
                            "ON fr.evidence_id = r.evidence_id "
                            "LEFT JOIN g.human_grade hg "
                            "ON hg.evidence_id = r.evidence_id "
                            "WHERE r.evidence_id IN (%s)"
                            % ",".join(":" + n for n in enames),
                            enames).fetchall():
                        fat[x["evidence_id"]] = x
                    matched = {x["e"] for x in self._query(
                        con, "cards.matched",
                        "SELECT cm.evidence_id AS e FROM card c "
                        "JOIN card_member cm ON cm.card_id = c.card_id "
                        "JOIN facet_row r ON r.evidence_id = cm.evidence_id "
                        "%s %s" % (join, self._and(
                            where, "cm.card_id IN (%s)" % inlist)),
                        dict(pr, **names)).fetchall()}
                    for m in mem:
                        x = fat.get(m["e"])
                        if x is None or m["cid"] not in by_id:
                            continue
                        d = self._public_row(x)
                        d["grade"] = d.get("grade") or ""
                        d["note"] = d.get("note") or ""
                        d["card_work_id"] = m["w"]
                        d["card_scope"] = m["s"]
                        d["card_scope_prefix"] = m["p"]
                        d["member_basis"] = m["mb"]
                        d["route_basis"] = m["rb"]
                        d["matched"] = m["e"] in matched
                        by_id[m["cid"]]["rows"].append(d)
                        rows.append(d)
                    self._attach_gate(con, rows)
                    self._attach_witness(con, rows)
                    self._attach_scripture(con, rows)
                    self._attach_llm_verdicts(con, rows)
                    self._attach_alias_twin(con, rows)
                cards = [by_id[c] for c in cids if c in by_id]
                for c in cards:
                    c["rows"].sort(key=lambda d: (d["card_work_id"],
                                                  d["card_scope"],
                                                  d["evidence_id"]))
                    c["graded_rows"] = sum(1 for d in c["rows"] if d["grade"])
                self._witness_strip(con, cards)
            # THREE NAMED NUMBERS: cards are not evidence rows, and neither is
            # a manuscript count -- reporting one total for all three is how a
            # reader ends up quoting the wrong one.
            nums = {x["k"]: x["v"] for x in self._query(
                con, "cards.nums",
                "SELECT key AS k, value AS v FROM meta WHERE key LIKE "
                "'card_grain.%'").fetchall()}
            # the SAME join as the card selection, so a card/page filter is
            # respected here too; one evidence row belongs to exactly one card
            # (gated at build time), so this count is exact, not inflated
            ev_total = self._query(
                con, "cards.ev_total",
                "SELECT COUNT(*) AS c %s" % sel, pr).fetchone()["c"]
            graded_total = self._query(
                con, "cards.graded_total",
                "SELECT COUNT(*) AS c FROM g.human_grade WHERE "
                "divergence_correctness IS NOT NULL "
                "AND divergence_correctness <> ''").fetchone()["c"]
        finally:
            con.close()
        self._send({"total": total, "cards": cards,
                    "evidence_rows": ev_total,
                    "graded_total": graded_total,
                    "grain": {"cards": nums.get("card_grain.cards"),
                              "evidence_rows": nums.get("card_grain.members"),
                              "manuscripts": nums.get("card_grain.manuscripts"),
                              "pages": nums.get("card_grain.pages")}})

    # The export is the ONE artifact that leaves this machine -- a teammate
    # forwards the file. Spec 6.2 restricts it to these six fields and no
    # others: identifiers plus the grade. Titles, authors, catalogue titles,
    # shelfmarks, domains and above all the corpus are all context the grader
    # already had on screen; in a forwarded file they are an unmasked copy of
    # the artifact travelling under the name "my grades".
    #
    # This tuple is the whole contract. Adding a column here is adding it to
    # every file anyone forwards, so the SELECT is written from the tuple
    # rather than beside it -- the two cannot drift.
    EXPORT_FIELDS = (("r", "evidence_id"), ("r", "sys_id"), ("r", "work_id"),
                     ("hg", "divergence_correctness"), ("hg", "note"),
                     ("hg", "graded_at"))

    def _api_export(self):
        """Every grade entered so far, from the GRADES file, keyed to the row
        it grades. Unfiltered on purpose: it is the backup.

        `facet_row` is joined for `sys_id` / `work_id` only -- the identifiers
        that make a grade re-attachable after a rebuild of the projection.
        """
        con = self._conn()
        try:
            rows = self._query(
                con, "export",
                # A NOTE IS WORK TOO. Filtering on the verdict alone dropped
                # every row where the reviewer typed a note while still
                # deciding -- silent loss in exactly the "partial progress"
                # flow the handoff note recommends this button for.
                "SELECT %s FROM g.human_grade hg "
                "JOIN facet_row r ON r.evidence_id = hg.evidence_id "
                "WHERE (hg.divergence_correctness IS NOT NULL "
                "       AND hg.divergence_correctness <> '') "
                "   OR (hg.note IS NOT NULL AND TRIM(hg.note) <> '') "
                "ORDER BY hg.graded_at"
                % ", ".join("%s.%s" % f for f in self.EXPORT_FIELDS)).fetchall()
        finally:
            con.close()
        # NOT `_public_row`: that adds `corpus_label`, and the export carries no
        # corpus at all -- neither the raw code nor its redacted label.
        allowed = {c for _t, c in self.EXPORT_FIELDS}
        body = json.dumps([{k: x[k] for k in x.keys() if k in allowed}
                           for x in rows],
                          ensure_ascii=False, indent=1).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        # named after the db it came from: three round sidecars coexist, and a
        # file called v3-human-grades.json from the v5 artifact is a trap
        stem = os.path.splitext(os.path.basename(self.db_path))[0]
        self.send_header("Content-Disposition",
                         "attachment; filename=%s.grades.json" % stem)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        try:
            if u.path == "/":
                return self._page()
            if u.path == "/api/facets":
                return self._api_facets(q)
            if u.path == "/api/rows":
                return self._api_rows(q)
            if u.path == "/api/cards":
                return self._api_cards(q)
            if u.path == "/api/export":
                return self._api_export()
            if u.path == "/api/htr_page":
                return self._api_htr_page(q)
        except QueryFailed as e:
            # 200 with the failure IN THE BODY, deliberately. The client reads
            # `d.error` and paints the query name and the SQLite class; a bare
            # status code would reduce that to "HTTP 500", and an empty list --
            # the public page's outage shape -- is indistinguishable from "no
            # rows match", which a grader would read as a finished filter.
            return self._send(e.payload())
        self.send_error(404)

    def do_POST(self):
        if urllib.parse.urlparse(self.path).path != "/api/grade":
            return self.send_error(404)
        try:
            n = self._int(self.headers.get("Content-Length"), 0)
            d = json.loads(self.rfile.read(n) or b"{}")
        except ValueError:
            return self.send_error(400, "unreadable request body")
        if not isinstance(d, dict):
            return self.send_error(400, "unreadable request body")
        ev = str(d.get("evidence_id") or "").strip()
        if not ev:
            return self.send_error(400, "no evidence_id")

        # THE TWO HALVES ARE INDEPENDENT. The verdict buttons post only
        # `divergence_correctness` and the note box posts only `note`, so a
        # request must touch exactly the key it carries: reading an absent
        # `divergence_correctness` as "clear" made every keystroke in the note
        # box delete the grade beside it.
        val = None
        if "divergence_correctness" in d:
            val = str(d.get("divergence_correctness") or "")
            if val and val not in DIVERGENCE_VALUES:
                return self.send_error(400, "value outside the closed vocabulary")
        note = None
        if "note" in d:
            note = str(d.get("note") or "")[:4000]
        if val is None and note is None:
            return self.send_error(400, "nothing to save")

        con = self._conn()
        try:
            if val is not None:
                con.execute(
                    "INSERT INTO g.human_grade(evidence_id, divergence_correctness) "
                    "VALUES(?, ?) ON CONFLICT(evidence_id) DO UPDATE SET "
                    "divergence_correctness = excluded.divergence_correctness, "
                    "graded_at = CURRENT_TIMESTAMP", (ev, val))
                # `clear` must not throw away a reason the reviewer typed, so the
                # row only disappears once BOTH halves are empty.
                if not val:
                    con.execute("DELETE FROM g.human_grade WHERE evidence_id = ? "
                                "AND (note IS NULL OR note = '')", (ev,))
            if note is not None:
                con.execute(
                    "INSERT INTO g.human_grade(evidence_id, note) VALUES(?, ?) "
                    "ON CONFLICT(evidence_id) DO UPDATE SET note = excluded.note",
                    (ev, note))
            con.commit()
            total = con.execute(
                "SELECT COUNT(*) FROM g.human_grade WHERE "
                "divergence_correctness IS NOT NULL "
                "AND divergence_correctness <> ''").fetchone()[0]
        finally:
            con.close()
        self._send({"ok": True, "graded_total": total})


class ReviewServer(ThreadingHTTPServer):
    # MUST be False on Windows. `HTTPServer` defaults it to 1, and Windows honours
    # SO_REUSEADDR by letting a SECOND process bind a port another process already
    # holds -- so this server would start, print its URL, and quietly lose every
    # request to whatever was already listening. That is not hypothetical: a stale
    # `http.server` on 8777 served its own directory listing to a reader who had
    # just started this one, and nothing anywhere reported a conflict.
    allow_reuse_address = False


def _port_is_taken(port: int) -> bool:
    """Someone already listening on loopback? Ask by connecting, not by binding:
    on Windows a bind can succeed against an in-use port, which is the whole bug."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.35)
        return s.connect_ex(("127.0.0.1", port)) == 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Local review server over the full v3 quote-identification DB.")
    ap.add_argument("--db", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "discovery_data", "discovery-v3-REVIEW.db"))
    ap.add_argument("--port", type=int, default=8777)
    ap.add_argument("--strict-port", action="store_true",
                    help="fail if --port is busy instead of moving to a free one")
    ap.add_argument("--site", default=DEFAULT_SITE,
                    help="where the manuscript preview points; a reviewer already "
                         "running the web app locally can pass "
                         "http://127.0.0.1:8080 and get their own session")
    # Default "frame" (owner ruling, 2026-08-30): the preview pane is the live
    # web viewer, folio navigation and transcription included. --preview image
    # remains for a reviewer who wants the sessionless <img>.
    ap.add_argument("--preview", choices=PREVIEW_MODES, default="frame",
                    help="frame: the live bare viewer (default). "
                         "image: a folio <img>, no session and no script. "
                         "off: a plain link.")
    # An alias, not a fourth mode: the same dest, and declared AFTER --preview so
    # the "frame" default is the one that lands on the namespace.
    ap.add_argument("--no-preview", dest="preview", action="store_const",
                    const="off", default=argparse.SUPPRESS,
                    help="alias for --preview off")
    args = ap.parse_args(argv)

    if not os.path.exists(args.db):
        raise SystemExit("review DB not found: %s" % args.db)
    Handler.db_path = args.db
    Handler.site = args.site.rstrip("/")
    Handler.preview_mode = args.preview
    _fr = ensure_facet_table(args.db, say=lambda m: print(m, flush=True))
    # Cheap (a constants-only import, ~0.1s) and worth doing at every launch:
    # the whole value of the grouped view is that it matches the public page,
    # and a mismatch that nobody is told about is worse than no control at all.
    _views_ok = check_novelty_views(say=lambda m: print(m, flush=True))

    # WHICH SIDECAR, AND WHAT IS ALREADY IN IT. The grades file is derived from
    # the db path, so relaunching against a moved or renamed copy silently
    # attaches a FRESH, empty sidecar -- an hour of grading apparently gone,
    # with nothing saying why. Name the path and the counts at every launch, so
    # "starting fresh" can never be mistaken for "found my work".
    _side = args.db + ".grades.db"
    try:
        _g = sqlite3.connect(_side)
        _has = _g.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
            "AND name='human_grade'").fetchone()[0]
        _graded = _noted = 0
        if _has:
            _graded = _g.execute(
                "SELECT COUNT(*) FROM human_grade WHERE divergence_correctness "
                "IS NOT NULL AND divergence_correctness <> ''").fetchone()[0]
            _noted = _g.execute(
                "SELECT COUNT(*) FROM human_grade WHERE note IS NOT NULL "
                "AND TRIM(note) <> ''").fetchone()[0]
        _g.close()
        if _graded or _noted:
            print("grades    : %s -- %d graded, %d with notes"
                  % (_side, _graded, _noted), flush=True)
        else:
            print("grades    : %s -- STARTING FRESH (nothing graded in it yet)"
                  % _side, flush=True)
    except sqlite3.Error as _e:
        print("grades    : could not read %s (%s)" % (_side, _e), flush=True)

    port = args.port
    if _port_is_taken(port):
        if args.strict_port:
            raise SystemExit(
                "port %d is already serving something else (a stale http.server?). "
                "Stop it, or re-run without --strict-port to use the next free port."
                % port)
        print("! port %d is already in use by another server -- moving on" % port)
        for cand in range(port + 1, port + 40):
            if not _port_is_taken(cand):
                port = cand
                break
        else:
            raise SystemExit("no free port in %d-%d" % (port + 1, port + 39))

    try:
        srv = ReviewServer(("127.0.0.1", port), Handler)
    except OSError as e:
        raise SystemExit("could not bind 127.0.0.1:%d -- %s" % (port, e))

    # SAY WHICH. Silence looked the same whether the facet table was built,
    # found, or failed to appear -- and an absent one is the difference between
    # sub-second filters and a response slow enough for the browser to cancel.
    print("review DB : %s (%.0f MB)" % (args.db, os.path.getsize(args.db) / 1e6))
    print("facets    : ready, %s rows indexed" % format(_fr, ","))
    print("grades    : %s.grades.db  (nothing is ever written to the review DB)"
          % args.db)
    print("views     : %s" % ("verified against shared/discovery_service.py"
                              if _views_ok else "NOT verified (no checkout, or "
                              "drift reported above)"))
    print("site      : %s" % Handler.site)
    print("preview   : %s" % Handler.preview_mode)
    print("")
    print("   OPEN:   http://127.0.0.1:%d" % port)
    print("")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("stopped.")
    finally:
        srv.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
