#!/usr/bin/env python3
"""Curate an FJMS domain for every canonical work carrying a shipped discovery
claim, plus the author alias map -- as hash-pinned artifacts, never as hand
edits to a database (plan 136-09, requirements NOVEL-01 / PANEL-02).

WHY THIS EXISTS
---------------
``works.genre`` is NULL on all 1,269 rows of the deployed discovery asset, so
the findings page's domain facet is inert without a one-time curation pass.
The work facet needs nothing (the works being identified ARE the discovery
works) and bridging discovery titles to the catalogue's own title vocabulary
matches only ~5%, so this pass is the only new data the whole facet cascade
needs.

THE CLOSED VOCABULARY IS READ AT RUNTIME, NEVER SNAPSHOTTED
-----------------------------------------------------------
``load_vocabulary()`` reads the domain tree from ``shared.fjms_service``
(``FjmsService.get_domain_hierarchy``) every time it runs.  This module
contains NO copy of the vocabulary and no fallback list: if the FJMS sidecar
cannot be read, every mode fails closed rather than validating against a stale
snapshot (threat T-136-09-01 -- a domain invented outside the closed
vocabulary silently widening the facet tree).

``CURATION_RULES`` below IS NOT A VOCABULARY.  It is the ordered table of
curation DECISIONS -- "a title of this shape denotes a work of this kind" --
and every node name it names is checked against the LIVE tree by
``assert_rules_within_vocabulary()`` before a single row is emitted.  A rule
naming a node the live tree does not contain is a build error, not a new
domain.

ARTIFACT SCHEMA (``discovery_data/work_domains-v1.json``)
--------------------------------------------------------
Top level:

  ``artifact``            -- fixed string ``"work_domains"``.
  ``artifact_version``    -- fixed string ``"v1"``.
  ``generated_by``        -- this script's repo-relative path.
  ``generated_utc``       -- ISO-8601 UTC timestamp of the emit run.
  ``vocabulary_source``   -- where the closed vocabulary was read from.
  ``asset_basename``      -- the discovery asset the worklist was drawn from.
  ``assignment_axis``     -- the axis every row was assigned on, stated so a
                             reader cannot mistake it for the manuscript's
                             catalogue domain.
  ``needs_ruling_posture``-- which posture was applied to unsettled rows.
  ``rules``               -- the rule table actually used, name + description,
                             so a row's ``provenance`` resolves to a sentence.
  ``counts``              -- coverage summary (total / by confidence /
                             Unassigned / needs-ruling).
  ``content_hash``        -- ``sha256:<hex>`` over
                             ``json.dumps(assignments, sort_keys=True,
                             ensure_ascii=False)`` -- the ``assignments`` array
                             ONLY, so the hash is stable under any later change
                             to the header fields above.
  ``assignments``         -- the rows.

Each row of ``assignments``:

  ``canonical_work_id``   -- REQUIRED.  The CANONICAL work id (a work whose
                             ``works.canonical_work_id`` is itself), so a
                             duplicate is never assigned twice.  A raw,
                             non-canonical source work id is rejected.
  ``domain_parent``       -- REQUIRED.  The immediate parent node of
                             ``domain_leaf`` in the live FJMS tree; equal to
                             ``domain_leaf`` when the leaf is a childless
                             top-level node; ``"Unassigned"`` for the
                             Unassigned bucket; ``null`` ONLY on a held
                             ``needs-ruling`` row.
  ``domain_leaf``         -- REQUIRED.  A node of the live FJMS tree, or the
                             sentinel ``"Unassigned"``; ``null`` ONLY on a held
                             ``needs-ruling`` row.
  ``confidence``          -- REQUIRED.  One of ``high`` / ``medium`` /
                             ``needs-ruling``.
  ``provenance``          -- REQUIRED.  ``rule:<name>`` (a row assigned by the
                             named rule) or ``manual:<short reason>`` (a row
                             assigned individually).  Never empty.
  ``note``                -- OPTIONAL.  Free text; used for data-quality
                             findings recorded rather than silently fixed.
  ``candidate_leaves``    -- REQUIRED, non-empty, ONLY on a ``needs-ruling``
                             row: the leaves the ruling must choose between,
                             each ``{"domain_parent", "domain_leaf", "case"}``.
  ``owner_ruling``        -- OPTIONAL.  Present ONLY when the owner has ruled
                             on a ``needs-ruling`` row; a citation of the
                             record that carries the ruling (e.g.
                             ``"136-GATE1-DECISIONS.md D"``).  A
                             ``needs-ruling`` row may carry a concrete
                             ``domain_leaf`` ONLY together with this field.

``Unassigned`` IS A REAL VALUE, NOT MISSING DATA -- a work the vocabulary
cannot place stays visible in the corpus view (the catalogue itself ships an
"Unspecified Domain" bucket with 19,709 rows).  It validates like any other
assignment.

THE ``needs-ruling`` POSTURE
----------------------------
``--validate`` checks STRUCTURE only, so a ``needs-ruling`` row would
otherwise ship into the asset whether or not anyone looked at it (threat
T-136-09-06).  The owner recorded the posture in ``136-GATE1-DECISIONS.md``
group D: *"THE OWNER WILL RULE.  The 'ship as Unassigned' default is
explicitly DECLINED."*  So a ``needs-ruling`` row is HELD -- it carries
``domain_leaf: null`` plus its candidate leaves -- and:

  * ``--validate``            passes (the artifact is structurally sound);
  * ``--validate --release``  FAILS while any held row remains unruled.

A ``needs-ruling`` row may NEVER carry a guessed leaf: a concrete
``domain_leaf`` on such a row is rejected unless it also carries
``owner_ruling``.

THE RULINGS ARE A TRACKED INPUT, NOT A HAND EDIT
------------------------------------------------
When the owner rules, the ruling is recorded in ``OWNER_RULINGS`` below -- a
COMMITTED table in this committed module, exactly like ``CURATION_RULES`` and
``MANUAL_ASSIGNMENTS``, and for the same reason: ``discovery_data/`` is
gitignored, so the artifact itself can never be the record of a decision.
``--emit-artifact`` reads ``OWNER_RULINGS`` and emits the ruled rows with
their ``domain_leaf`` and their ``owner_ruling`` citation, so **re-emitting
reproduces the rulings instead of discarding them** and the artifact stays
regenerable rather than hand-edited.  Three build errors guard the table:

  * a ruling on a work that is NOT held is rejected (nothing to rule on);
  * a ruled ``(domain_parent, domain_leaf)`` outside the LIVE FJMS tree is
    rejected (threat T-136-09-01);
  * a ruled leaf that is not one of THAT row's own ``candidate_leaves`` is
    rejected -- a ruling settles the question that was actually put to the
    owner, it does not introduce a new option after the fact.

MODES
-----
  ``--emit-worklist``   the canonical works needing assignment, at the
                        canonical grain.
  ``--emit-artifact``   run the curation pass and write the pinned artifact.
  ``--validate PATH``   check an artifact against the live closed vocabulary
                        and the schema contract (add ``--release`` for the
                        fail-closed shipping gate).
  ``--report PATH``     coverage, confidence distribution, and the rows
                        needing an owner ruling.
  ``--emit-aliases``    build the author alias map artifact.
  ``--validate-aliases PATH``  check an alias artifact.

MASKING
-------
Both artifacts carry work titles.  Run
``python scripts/check_atlas_masking.py --scan-asset <path>`` on each, and on
the curation report, with ``MASKING_SCAN_PATTERNS_FILE`` set.  Restricted
corpora are named ONLY as "M-source" / "R-source" anywhere in this module, its
output, its logs and its error paths (D-25).
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import re
import sqlite3
import sys
import unicodedata
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

DISCOVERY_DATA_DIR = os.path.join(_REPO_ROOT, "discovery_data")
DEFAULT_MANIFEST = os.path.join(DISCOVERY_DATA_DIR, "manifest.json")
DEFAULT_DOMAINS_ARTIFACT = os.path.join(DISCOVERY_DATA_DIR, "work_domains-v1.json")
DEFAULT_ALIASES_ARTIFACT = os.path.join(DISCOVERY_DATA_DIR, "work_author_aliases-v1.json")

ARTIFACT_NAME = "work_domains"
ARTIFACT_VERSION = "v1"
ALIAS_ARTIFACT_NAME = "work_author_aliases"

#: The sentinel a work the vocabulary cannot place is assigned to.  A REAL
#: value with its own parent -- never a null, never a silent disappearance.
UNASSIGNED = "Unassigned"

#: The closed confidence vocabulary.
CONFIDENCE_TOKENS: Tuple[str, ...] = ("high", "medium", "needs-ruling")

#: The closed alias-match-label vocabulary (Task 3).
ALIAS_MATCH_TOKENS: Tuple[str, ...] = ("exact", "containment", "unmatched")

ASSIGNMENT_AXIS = (
    "the IDENTIFIED WORK's own neutral title and author -- never any "
    "manuscript's catalogue domain (a court-records shelfmark can carry a "
    "correct Rashi finding; filtering on the catalogue axis would hide exactly "
    "the findings that disagree with the catalogue)"
)

NEEDS_RULING_POSTURE_HELD = (
    "held-for-owner: a needs-ruling row carries domain_leaf=null plus its "
    "candidate leaves and is NOT shippable; `--validate --release` fails while "
    "any such row is unruled. The 'ship as Unassigned' default was explicitly "
    "DECLINED by the owner (136-GATE1-DECISIONS.md group D)."
)


class CurationError(RuntimeError):
    """Any fail-closed condition in this module."""


# ---------------------------------------------------------------------------
# The live closed vocabulary -- read from shared/fjms_service.py at runtime.
# ---------------------------------------------------------------------------


class Vocabulary:
    """The FJMS browse domain tree, as an edge set.

    ``nodes``      every node name that exists anywhere in the tree.
    ``edges``      every ``(parent, child)`` pair that exists in the tree.
    ``top_level``  the names that sit at the top of the tree.

    A ``(parent, leaf)`` pair is valid iff it is an edge, or ``parent == leaf``
    and ``leaf`` is a top-level node (a childless top-level node -- e.g. a
    historiography or belles-lettres node -- is itself a usable leaf).
    """

    def __init__(
        self,
        nodes: Set[str],
        edges: Set[Tuple[str, str]],
        top_level: Set[str],
        source: str,
    ) -> None:
        self.nodes = nodes
        self.edges = edges
        self.top_level = top_level
        self.source = source

    # -- queries ----------------------------------------------------------
    def has_node(self, name: str) -> bool:
        return name in self.nodes

    def has_pair(self, parent: str, leaf: str) -> bool:
        if (parent, leaf) in self.edges:
            return True
        return parent == leaf and leaf in self.top_level

    def parents_of(self, leaf: str) -> List[str]:
        out = sorted({p for (p, c) in self.edges if c == leaf})
        if leaf in self.top_level:
            out.append(leaf)
        return out

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self.nodes)


def vocabulary_from_hierarchy(hierarchy: Mapping[str, Any], source: str) -> Vocabulary:
    """Build a :class:`Vocabulary` from a ``get_domain_hierarchy()`` mapping.

    Pure -- takes the tree as data so tests can inject a tiny one.
    """
    nodes: Set[str] = set()
    edges: Set[Tuple[str, str]] = set()
    top_level: Set[str] = set()
    for parent, info in hierarchy.items():
        if not parent:
            continue
        nodes.add(parent)
        top_level.add(parent)
        for child in (info or {}).get("children", []) or []:
            name = child.get("domain")
            if not name:
                continue
            nodes.add(name)
            edges.add((parent, name))
            for sub in child.get("children", []) or []:
                sub_name = sub.get("domain")
                if not sub_name:
                    continue
                nodes.add(sub_name)
                edges.add((name, sub_name))
    if not nodes:
        raise CurationError(
            "FJMS domain vocabulary is empty -- refusing to validate against "
            "an empty tree (fail closed)"
        )
    return Vocabulary(nodes=nodes, edges=edges, top_level=top_level, source=source)


def load_vocabulary() -> Vocabulary:
    """Read the closed domain vocabulary from ``shared.fjms_service`` LIVE.

    No snapshot, no fallback: an unreadable FJMS sidecar raises rather than
    silently validating against stale data.
    """
    from shared.fjms_service import get_fjms_service  # local import: fail loudly here

    svc = get_fjms_service()
    hierarchy = svc.get_domain_hierarchy()
    if not hierarchy:
        raise CurationError(
            "shared.fjms_service returned an empty domain hierarchy -- the FJMS "
            "sidecar (fist_data/fjms_enrichment.db) is missing or unreadable"
        )
    return vocabulary_from_hierarchy(
        hierarchy,
        source=(
            "shared.fjms_service.FjmsService.get_domain_hierarchy "
            "(fist_data/fjms_enrichment.db :: domains)"
        ),
    )


# ---------------------------------------------------------------------------
# The discovery asset -- the worklist, at the canonical grain.
# ---------------------------------------------------------------------------


def resolve_asset_path(manifest_path: str = DEFAULT_MANIFEST) -> str:
    if not os.path.isfile(manifest_path):
        raise CurationError(f"discovery manifest not found: {manifest_path}")
    with open(manifest_path, "r", encoding="utf-8") as fh:
        manifest = json.load(fh)
    basename = manifest.get("asset_basename")
    if not basename:
        raise CurationError("discovery manifest carries no asset_basename")
    path = os.path.join(DISCOVERY_DATA_DIR, basename + ".db")
    if not os.path.isfile(path):
        raise CurationError(f"discovery asset not found: {path}")
    return path


def _open_ro(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def load_worklist(asset_path: str) -> List[Dict[str, Any]]:
    """Every CANONICAL work carrying at least one shipped claim.

    Keyed on ``works.canonical_work_id`` so a duplicate work (the same work
    reachable under two source ids) is never assigned twice.  The canonical
    work's OWN title and author are carried (D-13a: the canonical work's own
    title wins).
    """
    conn = _open_ro(asset_path)
    try:
        works = {r["work_id"]: dict(r) for r in conn.execute("SELECT * FROM works")}
        rows = conn.execute(
            """
            SELECT w.canonical_work_id AS cw,
                   COUNT(DISTINCT dc.claim_id) AS shipped_claims
              FROM discovery_claim dc
              JOIN works w ON w.work_id = dc.work_id
              JOIN discovery_evidence e ON e.claim_id = dc.claim_id
             WHERE e.routing_status = 'shipped'
             GROUP BY 1
            """
        ).fetchall()
    finally:
        conn.close()

    out: List[Dict[str, Any]] = []
    for row in rows:
        cw = row["cw"]
        canonical = works.get(cw)
        if canonical is None:
            raise CurationError(
                f"canonical_work_id {cw!r} has no row in works -- refusing to "
                "curate a work the asset cannot describe (fail closed)"
            )
        out.append(
            {
                "canonical_work_id": cw,
                "neutral_title": canonical.get("neutral_title"),
                "author": canonical.get("author"),
                "source_corpus": canonical.get("source_corpus"),
                "shipped_claims": int(row["shipped_claims"]),
            }
        )
    out.sort(key=lambda r: r["canonical_work_id"])
    return out


def load_canonical_ids(asset_path: str) -> Set[str]:
    """The set of ids that ARE canonical (``canonical_work_id == work_id``)."""
    conn = _open_ro(asset_path)
    try:
        return {
            r["work_id"]
            for r in conn.execute(
                "SELECT work_id FROM works WHERE canonical_work_id = work_id"
            )
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Title normalization shared by the rules.
# ---------------------------------------------------------------------------

_QUOTE_MARKS = "\"'׳״‘’“”"
_QUOTE_RE = re.compile("[" + re.escape(_QUOTE_MARKS) + "]")
_WS_RE = re.compile(r"\s+")


def normalize_title(value: Optional[str]) -> str:
    """NFC, quote/geresh/gershayim marks stripped, whitespace collapsed.

    Deliberately the SAME normalization ``shared/discovery_grouping.py``
    already applies to work titles, so the two notions of "the same title" do
    not diverge.
    """
    if not value:
        return ""
    text = unicodedata.normalize("NFC", value)
    text = _QUOTE_RE.sub("", text)
    return _WS_RE.sub(" ", text).strip()


# ---------------------------------------------------------------------------
# CURATION DECISIONS -- NOT A VOCABULARY.
#
# Each entry says "a work whose title has this shape is a work of this kind".
# Every (parent, leaf) named here is checked against the LIVE tree by
# assert_rules_within_vocabulary() before any row is emitted; a name the live
# tree does not carry is a build error, never a new domain.
#
# Ordered: the FIRST matching rule wins, so the specific precedes the general.
# ---------------------------------------------------------------------------

_TALMUD_TRACTATE_COMMENTARY_HINT = (
    "בבא|ברכות|שבת|עירובין|פסחים|יומא|סוכה|ביצה|ראש השנה|תענית|מגילה|מועד קטן|"
    "חגיגה|יבמות|כתובות|נדרים|נזיר|סוטה|גיטין|קידושין|סנהדרין|מכות|שבועות|"
    "עבודה זרה|הוריות|זבחים|מנחות|חולין|בכורות|ערכין|תמורה|כריתות|מעילה|נדה|"
    "נידה|תמיד|מדות|קינים|ה?וריות"
)


def _rule(name: str, description: str, parent: str, leaf: str, confidence: str, test):
    return {
        "name": name,
        "description": description,
        "domain_parent": parent,
        "domain_leaf": leaf,
        "confidence": confidence,
        "test": test,
    }


def _starts(prefix: str):
    def _t(title: str, author: str) -> bool:
        return title.startswith(prefix)

    return _t


def _matches(pattern: str):
    rx = re.compile(pattern)

    def _t(title: str, author: str) -> bool:
        return bool(rx.search(title))

    return _t


def _exact(*titles: str):
    wanted = {normalize_title(t) for t in titles}

    def _t(title: str, author: str) -> bool:
        return title in wanted

    return _t


def _all(*tests):
    def _t(title: str, author: str) -> bool:
        return all(t(title, author) for t in tests)

    return _t


def _not(test):
    def _t(title: str, author: str) -> bool:
        return not test(title, author)

    return _t


def _author_is(*names: str):
    wanted = {normalize_title(n) for n in names}

    def _t(title: str, author: str) -> bool:
        return author in wanted

    return _t


CURATION_RULES: List[Dict[str, Any]] = [
    # -- Bible: texts, targumim, tafsir ---------------------------------
    _rule(
        "tanakh_book",
        "title names a book of the Hebrew Bible under the Tanakh prefix",
        "Bible: Texts and Translations", "Bible: Texts", "high",
        _starts("תנך, "),
    ),
    _rule(
        "targum_aramaic",
        "an Aramaic Targum (Onqelos / Jonathan / Pseudo-Jonathan / Fragmentary / "
        "the Writings targumim)",
        "Bible: Texts and Translations", "Aramaic Targumim", "high",
        _all(_starts("תרגום"), _not(_matches("תרגום$"))),
    ),
    _rule(
        "saadia_tafsir",
        "Saadia Gaon's Judaeo-Arabic Bible translation (tafsir)",
        "Bible: Texts and Translations", "Arabic Tafsir", "high",
        _all(_matches("^רסג, "), _matches("תרגום")),
    ),
    _rule(
        "karaite_arabic_bible_translation",
        "a Judaeo-Arabic Bible translation transmitted in the Karaite orbit",
        "Bible: Texts and Translations", "Arabic Tafsir", "high",
        _exact("מאור עין, תרגום", "ספר הפתרון, תרגום"),
    ),
    _rule(
        "masorah_terms",
        "a Masoretic treatise on the terminology and rules of the Masorah",
        "Massorah", "Diqduqe ha-Te'amim and Qunterese ha-Masorah", "high",
        _exact("דקדוקי הטעמים", "מונחי המסורה וכלליה"),
    ),
    _rule(
        "masorah_lists",
        "a Masoretic list of defective and plene spellings",
        "Massorah", "Lists and Counts", "high",
        _exact("מדרש חסרות ויתרות"),
    ),
    _rule(
        "masorah_arabic",
        "a Judaeo-Arabic treatment of the cantillation accents",
        "Massorah", "Masorah in Arabic", "high",
        _exact("טעמי המקרא, תרגום"),
    ),
    # -- Biblical exegesis ----------------------------------------------
    _rule(
        "rashi_talmud_commentary",
        "Rashi on a Talmudic tractate -- a Talmud commentary, NOT Bible exegesis",
        "Halakhic Literature and Talmudic Commentaries", "Talmud Bavli Commentaries", "high",
        _all(_starts("רשי על "), _matches("רשי על (" + _TALMUD_TRACTATE_COMMENTARY_HINT + ")")),
    ),
    _rule(
        "rabbanite_bible_commentary_by_named_exegete",
        "a Rabbanite commentary on a book of the Bible by a named medieval exegete",
        "Biblical Exegesis", "Biblical Exegesis- Rabbanite", "high",
        _matches(
            "^(רשי על |רדק על |אבן עזרא על |רמבן על |פירוש בן בלעם על |"
            "פירוש תנחום הירושלמי ל|ראבש, |פירוש ראבם לתורה|"
            "פירוש התורה לרשבח|פירוש בראשית לדוד הנגיד|פירוש קהלת לריץ גיאת|"
            "מדרש הבאור|שיר השירים פירוש עתיק)"
        ),
    ),
    _rule(
        "saadia_bible_commentary",
        "Saadia Gaon's own commentary on a book of the Bible",
        "Biblical Exegesis", "Biblical Exegesis- Rabbanite", "high",
        _all(_matches("^רסג, "), _matches("פירוש")),
    ),
    _rule(
        "rabbenu_hananel_bible_commentary",
        "Rabbenu Hananel on a book of the Torah (not on a tractate)",
        "Biblical Exegesis", "Biblical Exegesis- Rabbanite", "high",
        _matches("^רבנו חננאל על (בראשית|שמות|ויקרא|במדבר|דברים)$"),
    ),
    _rule(
        "karaite_bible_commentary",
        "a Karaite commentary on a book of the Bible",
        "Biblical Exegesis", "Biblical Exegesis- Karaite", "high",
        _exact(
            "פתרון שנים עשר",
            "פירוש לעשרת הדיברות(؟)",
            "פירוש ליחזקאל ותרי עשר",
            "איגרות ושונות",
        ),
    ),
    _rule(
        "anonymous_bible_commentary",
        "an anonymous commentary on a biblical passage or book",
        "Biblical Exegesis", "Biblical Exegesis- Rabbanite", "medium",
        _exact(
            "פירוש לבראשית",
            "פירוש למלאכי",
            "פירוש לעשרת הדיברות",
            "שאלות על מקומות קשים בתורה",
            "מאור האפלה",
        ),
    ),
    # -- Rabbinic literature: Mishnah / Tosefta / Talmud ----------------
    _rule(
        "mishnah_tractate",
        "a tractate of the Mishnah",
        "Mishnah: Texts and Translations", "Mishnah: Texts", "high",
        _starts("משנה, "),
    ),
    _rule(
        "mishnah_addenda",
        "addenda transmitted with the Mishnah text",
        "Mishnah: Texts and Translations", "Mishnah: Texts", "high",
        _exact("תוספות למשנה"),
    ),
    _rule(
        "tosefta_tractate",
        "a tractate of the Tosefta",
        "Rabbinic Literature", "Tosefta", "high",
        _starts("תוספתא, "),
    ),
    _rule(
        "talmud_bavli_tractate",
        "a tractate of the Babylonian Talmud",
        "Talmud Bavli: Texts and Anthologies", "Talmud Bavli", "high",
        _starts("תלמוד בבלי, "),
    ),
    _rule(
        "talmud_yerushalmi_tractate",
        "a tractate of the Jerusalem Talmud",
        "Rabbinic Literature", "Talmud Yerushalmi", "high",
        _starts("תלמוד ירושלמי, "),
    ),
    _rule(
        "minor_tractate",
        "one of the minor (extra-canonical) tractates",
        "Rabbinic Literature", "Minor Tractates", "high",
        _matches(
            "^(מסכת סופרים|מסכת כלה|מסכת כלה רבתי|מסכת שמחות|מסכת שמחות דרבי חייא|"
            "מסכת אבות דרבי נתן|מסכת דרך ארץ|פרקי דרך ארץ|שבע מסכתות קטנות|"
            "חיבור כעין אבות דרבי נתן|קניין תורה|פרק שירה)"
        ),
    ),
    # -- Midrash --------------------------------------------------------
    _rule(
        "halakhic_midrash",
        "a tannaitic (halakhic) midrash",
        "Midrash", "Halakhic Midrashim", "high",
        _matches("^(מכילתא|ספרא$|ספרי |ספרי זוטא )"),
    ),
    _rule(
        "aggadic_midrash",
        "an aggadic midrash or midrashic anthology",
        "Midrash", "Aggadic Midrashim", "high",
        _matches(
            "^(בראשית רבה|בראשית רבתי|שמות רבה|ויקרא רבה|במדבר רבה|דברים רבה|"
            "איכה רבה|איכה זוטא|אסתר רבה|קהלת רבה|רות רבה|רות זוטא|שיר השירים רבה|"
            "שיר השירים זוטא|מדרש |תנחומא|פסיקתא|ילקוט שמעוני|תוספות לבראשית רבה|"
            "תוספות למדרש משלי|אגדת |אגדות |מדרש$)"
        ),
    ),
    _rule(
        "later_midrash",
        "a post-classical midrashic or homiletic composition",
        "Derashot and Later Midrashim", "Later Midrashim", "high",
        _matches(
            "^(פרקי רבי אליעזר|פרקי רבי יאשיה|תנא דבי אליהו|סדר אליהו|"
            "ברייתא דשלושים ושתיים מידות|ברייתא דמלאכת המשכן|חופת אליהו|"
            "פרק זרעים|פרק מעשים|פרק צדקות|מעשה רבי מאיר|מעשה רבן יוחנן בן זכאי|"
            "ראויות יחזקאל|והזהיר|ספר והזהיר|לקט מדרשים ופירושים)"
        ),
    ),
    _rule(
        "derasha",
        "a sermon or homiletic address",
        "Derashot and Later Midrashim", "Derashot", "high",
        _matches("^(דרשה |פירוש מדרשי ל)"),
    ),
    # -- Halakhah: codes, Rif, Mishneh Torah, Geonic halakhah ----------
    _rule(
        "mishneh_torah",
        "a book of Maimonides' Mishneh Torah (or its introduction)",
        "Halakhic", "Mishneh Torah and its Commentaries", "high",
        _starts("משנה תורה, "),
    ),
    _rule(
        "rif",
        "Alfasi's Halakhot and the works transmitted with them",
        "Halakhic", "Halakhot ha-Rif and its Commentaries", "high",
        _matches("^(ריף |הלכות הריף|הלכות קטנות לריף)"),
    ),
    _rule(
        "geonic_halakhah",
        "a Geonic halakhic code, collection or digest",
        "Halakhic", "Halakhic- Gaonim", "high",
        _matches(
            "^(הלכות גדולות|הלכות פסוקות|הלכות קצובות|הלכות ראו|הלכות רב אבא|"
            "שאילתות|הלכות גאונים למסכת|חיבור כעין הלכות פסוקות|מתיבות|"
            "ספר המקצועות|הלכות ותשובות מסידור|הלכות מסידור|"
            "הלכות תפילין לרב האיי גאון|לקט הלכות ופסקים|"
            "שערי ברכות לרב שמואל גאון|שערי שחיטה ובדיקה לרב שמואל גאון|"
            "משפטי שבועות|מגילת סתרים|ספר השטרות|ספר ברכות ושטרות|"
            "ספר הבגרות לרשבח|ספר הגירושין לרשבח|ספר דיני מצות הציצית לרשבח)"
        ),
    ),
    _rule(
        "eretz_israel_halakhah",
        "a halakhic collection of the Palestinian (Eretz-Israel) rite",
        "Halakhic", "Halakhic- Gaonim", "high",
        _matches(
            "^(הלכות ארץ ישראליות|הלכות טרפות של בני ארץ ישראל|"
            "הלכות עריות של בני ארץ ישראל|מעשים לבני ארץ ישראל)"
        ),
    ),
    _rule(
        "rishonim_halakhah",
        "a halakhic code or manual of the Rishonim / Aharonim",
        "Halakhic", "Halakhic- Rishonim and Aharonim", "high",
        _exact("טור אורח חיים", "ספר החינוך"),
    ),
    _rule(
        "minhagim",
        "a record of the differing customs of two rites",
        "Halakhic Literature and Talmudic Commentaries", "Minhagim", "medium",
        _exact("רשימת החילוקים שבין בני ארץ ישראל ובני בבל"),
    ),
    _rule(
        "karaite_halakhah",
        "a Karaite legal code or book of precepts",
        "Halakhic", "Halakhic- Karaite", "high",
        _exact(
            "ספר מצוות ללוי בן יפת הלוי, תרגום",
            "ספר דינים",
            "תכריך של שטרות קראיים",
        ),
    ),
    _rule(
        "sifrei_mitzvot",
        "a book of precepts (sefer ha-mitzvot)",
        "Halakhic Literature and Talmudic Commentaries", "Sifrei Mitzvot (Rabbinical)", "high",
        _matches("^(רמבם, ספר המצוות|ספר המצוות ל)"),
    ),
    # -- Talmudic & Mishnaic commentary ---------------------------------
    _rule(
        "geonic_talmud_commentary",
        "a Geonic or early commentary on a Talmudic tractate",
        "Halakhic Literature and Talmudic Commentaries", "Talmud Bavli Commentaries", "high",
        _matches(
            "^(פירושי גאונים וקדמונים לתלמוד|רבנו חננאל על |תוספות על |"
            "האיי גאון על |שרירא גאון על |נסים גאון על |יצחק על |"
            "פירוש לתוספת סבוראית|לקט פירושי גאונים למילים קשות בתלמוד)"
        ),
    ),
    _rule(
        "geonic_taharot_commentary",
        "the Geonic commentary on Seder Tohorot",
        "Halakhic Literature and Talmudic Commentaries", "Mishnaic Commentaries", "high",
        _exact("פירוש הגאונים לסדר טהרות"),
    ),
    _rule(
        "mishnah_commentary",
        "a commentary on the Mishnah",
        "Halakhic Literature and Talmudic Commentaries", "Mishnaic Commentaries", "high",
        _matches("^(פירוש המשנה|פירוש אבות לדוד הנגיד)"),
    ),
    _rule(
        "talmud_introduction",
        "an introduction to, or set of rules for, the Talmud",
        "Halakhic Literature and Talmudic Commentaries", "Talmud – Introductions and Rules", "high",
        _exact("מבוא התלמוד"),
    ),
    # -- Responsa --------------------------------------------------------
    _rule(
        "geonic_responsa",
        "a Geonic responsum or responsa collection",
        "Responsa and Halakhic Decisions", "Responsa- Gaonim", "high",
        _matches(
            "^(תשובות הגאונים|תשובות האיי גאון|תשובות שרירא גאון|"
            "תשובות נטרונאי גאון|תשובות עמרם גאון|תשובות פלטוי גאון|"
            "תשובות צמח גאון|תשובות שמואל גאון|תשובות שר שלום גאון|"
            "תשובות מתתיה גאון|תשובות משרשיה|תשובות יצחק|תשובות$|"
            "תשובה בעניין|תשובות בעניין|תשובה אל |תשובה שנייה בעניין|"
            "שאלה בעניין|שאלה אל |פתיחות לתשובות|"
            "עשר שאלות לרשבח|תשובות יוסף בן אביתור)"
        ),
    ),
    _rule(
        "rishonim_responsa",
        "a responsum or responsa collection of the Rishonim",
        "Responsa and Halakhic Decisions", "Responsa- Rishonim and Aharonim", "high",
        _matches(
            "^(תשובות הרמבם|תשובות ראבם|תשובות משולם|תשובות קלונימוס)"
        ),
    ),
    # -- Liturgy ---------------------------------------------------------
    _rule(
        "passover_haggadah",
        "the Passover Haggadah",
        "Liturgy and Brakhot", "Passover Haggadah", "high",
        _exact("הגדה של פסח"),
    ),
    _rule(
        "common_prayers",
        "a fixed prayer of the daily / Sabbath liturgy",
        "Liturgy and Brakhot", "Common Prayers", "high",
        _matches("^(עמידה ל|ברכות שמע|ברכת המזון|קידוש ליל שבת|שבעתא דאליהו)"),
    ),
    _rule(
        "karaite_prayers",
        "the Karaite prayer rite",
        "Liturgy and Brakhot", "Karaite Prayers", "high",
        _exact("סדר תפילה קראי"),
    ),
    _rule(
        "brakhot",
        "a benediction text outside the fixed prayer order",
        "Liturgy and Brakhot", "Brakhot", "high",
        _exact("הבדלה דרבי עקיבא"),
    ),
    _rule(
        "hymn",
        "a liturgical hymn",
        "Piyut and its Interpretation", "Piyyut", "high",
        _exact("הימנון", "כתר מלכות (רשבג/אבן גבירול)"),
    ),
    # -- Mysticism, Heikhalot, magic ------------------------------------
    _rule(
        "heikhalot",
        "a Heikhalot / Merkavah composition",
        "Kabbalah", "Heikhalot", "high",
        _matches(
            "^(היכלות |מסכת היכלות|מעשה מרכבה|מרכבה רבה|שיעור קומה|"
            "אותיות דרבי עקיבא|מעשה בראשית)"
        ),
    ),
    _rule(
        "magic_recipes",
        "a book of adjurations, recipes or magical praxis",
        "Occult Sciences", "Magic Recipes", "high",
        _matches("^(ספר הרזים|השבעות|ספר המלבוש)"),
    ),
    _rule(
        "physiognomy",
        "a physiognomic manual",
        "Predicting the Future", "Physiognomy", "high",
        _exact("הכרת פנים"),
    ),
    _rule(
        "astrology",
        "an astrological treatise",
        "Occult Sciences", "Astrology", "high",
        _exact("ברייתא דמזלות"),
    ),
    # -- Sciences --------------------------------------------------------
    _rule(
        "calendar",
        "a treatise on the calendar and the intercalation cycle",
        "Astronomy", "Calendar", "high",
        _matches(
            "^(סוד העיבור|חיבור בעניין חשבון העיבור|ברייתא דשמואל|"
            "רשימת החילופים שבין רב סעדיה גאון ובן מאיר)"
        ),
    ),
    _rule(
        "logic",
        "a treatise on logic",
        "Philosophy, Theology, Ethical literature", "Logic", "high",
        _exact("באור מלאכת ההגיון"),
    ),
    # -- Philosophy, theology, ethics -----------------------------------
    _rule(
        "philosophy",
        "a work of philosophy",
        "Philosophy, Theology, Ethical literature", "Philosophy", "high",
        _matches(
            "^(מורה נבוכים|הכוזרי|ספר הכוזרי|רסג, הנבחר באמונות ודעות|"
            "נתנאל בן פיומי, גן השכלים)"
        ),
    ),
    _rule(
        "ethical_literature",
        "a work of ethical / pietist literature",
        "Philosophy, Theology, Ethical literature", "Ethical Literature", "high",
        _matches("^(חובות הלבבות|תורת חובות הלבבות|שערי תשובה)"),
    ),
    _rule(
        "apocalyptic",
        "an apocalyptic or messianic composition",
        "Philosophy, Theology, Ethical literature", "Apocalyptic Literature", "high",
        _matches(
            "^(אותות המשיח|פרק המשיח|ספר זרובבל|מלכות ישמעאל|"
            "נסתרות רבי שמעון בן יוחאי|המעלות לדרגות ימות המשיח|"
            "ברייתא דישועה)"
        ),
    ),
    # -- Polemics --------------------------------------------------------
    _rule(
        "polemics_karaite_rabbanite",
        "a Karaite-Rabbanite polemic",
        "Polemics", "Polemics Karaite-Rabbanite", "high",
        _matches("^(חיבור נגד הקראים|חיבור נגד אל-קומסי|מלחמות אדוני|השגות על רסג)"),
    ),
    _rule(
        "polemics_other",
        "a polemic against sectarians",
        "Polemics", "Other", "high",
        _exact("חיבור נגד המינים"),
    ),
    # -- Philology -------------------------------------------------------
    _rule(
        "grammar",
        "a work of Hebrew grammar or linguistic theory",
        "Philology", "Grammar", "high",
        _matches(
            "^(ספר הרקמה|ספר ההשוואה|ספר המאזניים|יהודה חיוג|"
            "רסג, כתאב אלפציח|תשובות על מנחם|חיבור בעניין אותיות החילוף)"
        ),
    ),
    _rule(
        "dictionaries",
        "a dictionary or lexicon",
        "Philology", "Dictionaries", "high",
        _matches("^(ספר הערוך|אלמרשד אלכאפי)"),
    ),
    # -- Documentary ------------------------------------------------------
    _rule(
        "karaite_letters",
        "a Karaite epistle addressed to a named recipient",
        "Documentary", "Letters", "high",
        _matches("^(איגרת קראית|איגרת אל דוד הבבלי|איגרת ליהודה אבן קריש)"),
    ),
    _rule(
        "communal_documents",
        "a communal document or writ of appointment",
        "Documentary", "Communal Documents", "high",
        _exact("תעודת הנגיד"),
    ),
    # -- Ancillaries / unclassifiable text types --------------------------
    _rule(
        "table_of_contents",
        "an index or list of pericopes",
        "Ancillaries to the Main Work", "Indices", "high",
        _exact("פתרון תורה"),
    ),
]


def assert_rules_within_vocabulary(vocab: Vocabulary, rules=CURATION_RULES) -> None:
    """Every rule's ``(domain_parent, domain_leaf)`` must exist in the LIVE tree.

    A rule naming a node the tree does not carry is a BUILD ERROR -- the whole
    point of the closed vocabulary (threat T-136-09-01).
    """
    bad: List[str] = []
    for rule in rules:
        parent = rule["domain_parent"]
        leaf = rule["domain_leaf"]
        if not vocab.has_node(leaf):
            bad.append(f"rule {rule['name']}: leaf {leaf!r} is not in the FJMS tree")
        elif not vocab.has_pair(parent, leaf):
            bad.append(
                f"rule {rule['name']}: {leaf!r} is not a child of {parent!r} "
                f"(actual parents: {vocab.parents_of(leaf)})"
            )
        if rule["confidence"] not in CONFIDENCE_TOKENS:
            bad.append(f"rule {rule['name']}: confidence {rule['confidence']!r} out of vocabulary")
    if bad:
        raise CurationError(
            "curation rules name node(s) outside the live FJMS vocabulary:\n  "
            + "\n  ".join(bad)
        )


def apply_rules(title: str, author: str, rules=CURATION_RULES) -> Optional[Dict[str, Any]]:
    """First matching rule wins.  Returns ``None`` when no rule fires."""
    for rule in rules:
        if rule["test"](title, author):
            return rule
    return None


# ---------------------------------------------------------------------------
# Validation.
# ---------------------------------------------------------------------------

_ROW_KEYS_REQUIRED = {"canonical_work_id", "domain_parent", "domain_leaf", "confidence", "provenance"}
_ROW_KEYS_OPTIONAL = {"note", "candidate_leaves", "owner_ruling"}
_W_ID_RE = re.compile(r"^w\d{6}$")


def compute_content_hash(assignments: Sequence[Mapping[str, Any]]) -> str:
    """``sha256`` over the ``assignments`` array only.

    Stable under any later change to the artifact's own header fields -- the
    same recipe ``discovery_data/novelty_hardcase_labels-v1.json`` already uses
    for its ``cases`` array.
    """
    payload = json.dumps(assignments, sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_artifact(
    doc: Mapping[str, Any],
    vocab: Vocabulary,
    canonical_ids: Optional[Set[str]] = None,
    *,
    release: bool = False,
) -> List[str]:
    """Return a list of error strings; empty means the artifact validates.

    The five structural failure classes, each rejected here:

      1. a domain not in the tree;
      2. a leaf whose parent disagrees;
      3. an assignment keyed on a non-canonical work id;
      4. a missing confidence or provenance;
      5. a duplicate canonical key.

    ``release=True`` additionally applies the fail-closed shipping gate: any
    ``needs-ruling`` row still held (no ``owner_ruling``) fails.
    """
    errors: List[str] = []

    if doc.get("artifact") != ARTIFACT_NAME:
        errors.append(f"artifact must be {ARTIFACT_NAME!r}")
    if doc.get("artifact_version") != ARTIFACT_VERSION:
        errors.append(f"artifact_version must be {ARTIFACT_VERSION!r}")

    assignments = doc.get("assignments")
    if not isinstance(assignments, list) or not assignments:
        errors.append("assignments must be a non-empty list")
        return errors

    declared = doc.get("content_hash")
    actual = compute_content_hash(assignments)
    if not declared:
        errors.append("content_hash is missing -- an unpinned artifact is not pinned")
    elif declared != actual:
        errors.append(f"content_hash mismatch: declared {declared}, actual {actual}")

    if not doc.get("needs_ruling_posture"):
        errors.append(
            "needs_ruling_posture is missing -- the posture must be stated, "
            "never implicit (threat T-136-09-06)"
        )

    seen: Set[str] = set()
    for i, row in enumerate(assignments):
        where = f"assignments[{i}]"
        if not isinstance(row, dict):
            errors.append(f"{where} must be an object")
            continue

        keys = set(row)
        extra = keys - _ROW_KEYS_REQUIRED - _ROW_KEYS_OPTIONAL
        if extra:
            errors.append(f"{where} has key(s) outside the closed schema: {sorted(extra)}")
        missing = _ROW_KEYS_REQUIRED - keys
        if missing:
            # failure class 4 (and the id/leaf shape checks below need them)
            errors.append(f"{where} is missing required field(s): {sorted(missing)}")
            continue

        cw = row["canonical_work_id"]
        if not isinstance(cw, str) or not _W_ID_RE.match(cw):
            errors.append(f"{where} canonical_work_id {cw!r} is not w000000-shaped")
            continue

        # failure class 5 -- duplicate canonical key
        if cw in seen:
            errors.append(f"{where} duplicate canonical_work_id {cw!r}")
        seen.add(cw)

        # failure class 3 -- keyed on a non-canonical work id
        if canonical_ids is not None and cw not in canonical_ids:
            errors.append(
                f"{where} canonical_work_id {cw!r} is not a CANONICAL work id "
                "(a raw source work id would assign a duplicate twice)"
            )

        # failure class 4 -- missing/blank confidence or provenance
        confidence = row["confidence"]
        if confidence not in CONFIDENCE_TOKENS:
            errors.append(
                f"{where} confidence {confidence!r} outside {list(CONFIDENCE_TOKENS)}"
            )
        provenance = row["provenance"]
        if not isinstance(provenance, str) or not provenance.strip():
            errors.append(f"{where} provenance is missing or blank")

        parent = row["domain_parent"]
        leaf = row["domain_leaf"]
        held = confidence == "needs-ruling" and leaf is None and parent is None

        if held:
            candidates = row.get("candidate_leaves")
            if not isinstance(candidates, list) or not candidates:
                errors.append(
                    f"{where} is a held needs-ruling row but carries no "
                    "candidate_leaves -- the owner cannot rule on nothing"
                )
            else:
                for j, cand in enumerate(candidates):
                    if not isinstance(cand, dict):
                        errors.append(f"{where}.candidate_leaves[{j}] must be an object")
                        continue
                    cp, cl = cand.get("domain_parent"), cand.get("domain_leaf")
                    if cl == UNASSIGNED and cp == UNASSIGNED:
                        continue
                    if not vocab.has_node(cl):
                        errors.append(
                            f"{where}.candidate_leaves[{j}] leaf {cl!r} is not in the FJMS tree"
                        )
                    elif not vocab.has_pair(cp, cl):
                        errors.append(
                            f"{where}.candidate_leaves[{j}] {cl!r} is not a child of {cp!r}"
                        )
            continue

        if confidence == "needs-ruling" and not row.get("owner_ruling"):
            # A needs-ruling row may NEVER carry a guessed leaf.
            errors.append(
                f"{where} is needs-ruling and carries a concrete domain_leaf "
                f"({leaf!r}) without an owner_ruling citation -- a guessed leaf "
                "on an unsettled row is exactly what the posture forbids"
            )

        if leaf == UNASSIGNED:
            # Unassigned is a REAL value with its own parent, not missing data.
            if parent != UNASSIGNED:
                errors.append(
                    f"{where} Unassigned must carry domain_parent {UNASSIGNED!r}, got {parent!r}"
                )
            continue

        if leaf is None or parent is None:
            errors.append(
                f"{where} domain_parent/domain_leaf may only be null on a HELD "
                "needs-ruling row (both null together)"
            )
            continue

        # failure class 1 -- a domain not in the tree
        if not vocab.has_node(leaf):
            errors.append(
                f"{where} domain_leaf {leaf!r} is not in the closed FJMS vocabulary"
            )
            continue
        # failure class 2 -- a leaf whose parent disagrees
        if not vocab.has_pair(parent, leaf):
            errors.append(
                f"{where} domain_leaf {leaf!r} is not a child of domain_parent "
                f"{parent!r} (actual parents: {vocab.parents_of(leaf)})"
            )

    if release:
        held_rows = [
            r
            for r in assignments
            if isinstance(r, dict)
            and r.get("confidence") == "needs-ruling"
            and not r.get("owner_ruling")
        ]
        if held_rows:
            errors.append(
                f"RELEASE GATE: {len(held_rows)} needs-ruling row(s) are still "
                "held for the owner's ruling; this artifact is NOT shippable "
                "(the 'ship as Unassigned' default was explicitly DECLINED -- "
                "136-GATE1-DECISIONS.md group D)"
            )

    return errors


# ---------------------------------------------------------------------------
# The curation pass.
# ---------------------------------------------------------------------------

#: Individually-curated works, keyed on canonical work id.  Each entry states
#: WHY, so the row's provenance resolves to a sentence.  Every (parent, leaf)
#: here is validated against the live tree exactly like a rule's.
MANUAL_ASSIGNMENTS: Dict[str, Dict[str, Any]] = {}

#: Works the closed vocabulary cannot settle without the owner.  HELD --
#: `domain_leaf` stays null and the candidate leaves are carried for the
#: ruling.  `case` names which of the three classes the row is.
NEEDS_RULING: Dict[str, Dict[str, Any]] = {}


def _manual(work_id: str, parent: str, leaf: str, confidence: str, reason: str, note=None):
    MANUAL_ASSIGNMENTS[work_id] = {
        "domain_parent": parent,
        "domain_leaf": leaf,
        "confidence": confidence,
        "provenance": "manual:" + reason,
        "note": note,
    }


def _ruling(work_id: str, case: str, question: str, candidates: List[Tuple[str, str, str]], note=None):
    NEEDS_RULING[work_id] = {
        "case": case,
        "question": question,
        "candidate_leaves": [
            {"domain_parent": p, "domain_leaf": leaf, "case": why} for (p, leaf, why) in candidates
        ],
        "note": note,
    }


# -- individually curated works -----------------------------------------
# Each of these is a work the ordered rule table places badly or not at all.
# Assigned from the work's OWN title and author; `medium` wherever a second
# reading is genuinely available and named in the note.

# Geonic monographs and codes the generic rules mis-file.
_manual("w001192", "Halakhic Literature and Talmudic Commentaries",
        "Talmud – Introductions and Rules", "medium",
        "Sherira Gaon's epistle is the classical account of how the Oral Law and "
        "the Talmud were transmitted; catalogued as an introduction to the Talmud",
        note="an epistle in form; a historiography reading is also available")
_manual("w000071", "Halakhic Literature and Talmudic Commentaries",
        "Talmud Bavli Commentaries", "medium",
        "R. Nissim Gaon's five books, whose core (the Key to the Talmud) is a "
        "commentary on the Talmud",
        note="the collection is mixed; the Talmud commentary dominates it")
_manual("w001136", "Philology", "Grammar", "medium",
        "Abramson's edition of three of Ibn Balaam's works, which are linguistic "
        "and lexicographic, not Talmudic",
        note="the rule table would otherwise file it as a Talmud commentary")
_manual("w000511", "Halakhic Literature and Talmudic Commentaries",
        "Mishnaic Commentaries", "high",
        "a Geonic commentary on a chapter of Mishnah Parah -- a Mishnah "
        "commentary, not a halakhic code")
_manual("w001052", "Halakhic", "Halakhic- Rishonim and Aharonim", "medium",
        "R. Hananel of Kairouan is conventionally counted among the Rishonim, "
        "not the Geonim")

# The Ben Meir calendar controversy -- all three texts are about the calendar.
_manual("w001054", "Astronomy", "Calendar", "medium",
        "Saadia's Book of Festivals is his intervention in the calendar "
        "controversy",
        note="also readable as polemics")
_manual("w001059", "Astronomy", "Calendar", "medium",
        "a responsum on the calendar reckoning in the Ben Meir controversy",
        note="responsum in form, calendrical in subject")

# Philosophy / pietism / polemic the rules mis-file.
_manual("w000052", "Biblical Exegesis", "Biblical Exegesis- Rabbanite", "medium",
        "Ibn Aqnin's Revelation of Secrets is a philosophical commentary on the "
        "Song of Songs -- exegesis, not ethical literature")
_manual("w001139", "Philosophy, Theology, Ethical literature", "Sufi Literature", "medium",
        "Obadyah Maimonides' Treatise of the Pool is a Jewish-Sufi pietist work")
_manual("w001134", "Philosophy, Theology, Ethical literature", "Sufi Literature", "medium",
        "David b. Joshua ha-Nagid's Guide to Detachment is a Jewish-Sufi pietist work")
_manual("w000834", "Polemics", "Polemics Karaite-Rabbanite", "high",
        "Sahl b. Masliah's Open Rebuke is a Karaite polemic against the Rabbanites")
_manual("w000009", "Philosophy, Theology, Ethical literature", "Philosophy", "medium",
        "title denotes a treatise on truths / first principles")

# Philology.
_manual("w000911", "Philology", "Dictionaries", "high",
        "Menahem b. Saruq's Mahberet is a biblical lexicon")
_manual("w001053", "Philology", "Dictionaries", "medium",
        "the introduction to Saadia's Agron, the first Hebrew dictionary",
        note="the introduction itself is a treatise on the language; Grammar is "
             "the alternative leaf")
_manual("w001056", "Philology", "Grammar", "medium",
        "Saadia's Sefer ha-Galui is transmitted with his linguistic works",
        note="polemical in purpose")

# Narrative, chronicle and travel.
_manual("w000158", "Historiography and geographical descriptions",
        "Historiography and geographical descriptions", "high",
        "Gedaliah ibn Yahya's Shalshelet ha-Qabbalah is a chronicle",
        note="DATA QUALITY: a sixteenth-century printed chronicle sitting in a "
             "Genizah discovery corpus -- recorded, not fixed here")
_manual("w001133", "Historiography and geographical descriptions",
        "Historiography and geographical descriptions", "medium",
        "al-Harizi's Travels of Judah is a travel narrative -- the node name "
        "itself covers geographical description")
_manual("w000897", "Stories and Belles Lettres", "Stories and Belles Lettres", "medium",
        "the Alexander Romance is narrative belles-lettres")
_manual("w000944", "Derashot and Later Midrashim", "Later Midrashim", "medium",
        "the Chronicles of Moses is a medieval narrative midrash",
        note="a belles-lettres reading is also available")
_manual("w000850", "Derashot and Later Midrashim", "Later Midrashim", "medium",
        "a legendary description of Solomon's throne, transmitted midrashically",
        note="a belles-lettres reading is also available")
_manual("w000757", "Rabbinic Literature", "Other", "medium",
        "the scholion on Megillat Ta'anit -- a tannaitic-era text with no closer leaf")

# Eldad ha-Dani's epistles: the work's OWN title names the genre ("stories").
for _eldad in ("w000842", "w000843", "w000844", "w000845"):
    _manual(_eldad, "Stories and Belles Lettres", "Stories and Belles Lettres", "medium",
            "the work's own title names its genre (stories) -- a literary "
            "epistle, not a documentary letter",
            note="Documentary / Letters is the alternative leaf; the title's own "
                 "genre word drove this")

# Eldad ha-Dani's laws of ritual slaughter -- a halakhic code of the Geonic
# period, transmitted with his legendary material.
_manual("w000846", "Halakhic", "Halakhic- Gaonim", "medium",
        "a ninth-century halakhic code on ritual slaughter",
        note="transmitted with Eldad ha-Dani's legendary material, so a "
             "belles-lettres reading of the corpus it travels in also exists")

# Karaite legal works.
_manual("w000827", "Halakhic", "Halakhic- Karaite", "medium",
        "Yeshua b. Judah's Sefer ha-Yashar is a Karaite legal work")

# Mysticism.
_manual("w000811", "Kabbalah", "Other", "medium",
        "Donnolo's Hakhmoni is a commentary on Sefer Yetzirah",
        note="astrological in content; filed with the Sefer Yetzirah tradition")
for _otiyot in ("w000892", "w001127"):
    _manual(_otiyot, "Kabbalah", "Heikhalot", "medium",
            "the Alphabet of R. Aqiva is transmitted in the Heikhalot orbit",
            note="an aggadic-midrash reading is also available")

# A data-quality finding recorded, not silently fixed.
_manual("w000022", "Philosophy, Theology, Ethical literature", "Ethical Literature", "high",
        "Duties of the Hearts is the classical work of Jewish ethical literature",
        note="DATA QUALITY: the asset records the author as Bahya ben Asher; "
             "Duties of the Hearts is by Bahya ibn Paquda. The domain is "
             "unaffected -- recorded here rather than corrected in this artifact")


# -- held for the owner's ruling ----------------------------------------
# `case` is one of the three classes plan 136-09 names:
#   (a) literary genre, documentary surface form;
#   (b) no plausible leaf in the closed vocabulary at all;
#   (c) between two adjacent leaves on a real scholarly judgement.

_LETTERS = ("Documentary", "Letters", "the documentary leaf the surface form suggests")

_ruling(
    "w001140", "(a) literary genre, documentary surface form",
    "Maimonides' epistles (Shailat's edition) are literary and halakhic "
    "treatises cast as letters. Do they file under the documentary Letters "
    "leaf, or with responsa / ethical literature?",
    [_LETTERS,
     ("Responsa and Halakhic Decisions", "Responsa- Rishonim and Aharonim",
      "several of the epistles answer halakhic questions"),
     ("Philosophy, Theology, Ethical literature", "Ethical Literature",
      "the Epistle to Yemen and the Epistle on Martyrdom are read as ethical works")],
)
_ruling(
    "w000079", "(a) literary genre, documentary surface form",
    "The letters of Samuel b. Ali are a literary letter collection. Documentary "
    "/ Letters, or a literary parent? (The feasibility sample recorded this as "
    "one of its three low-confidence cases.)",
    [_LETTERS,
     ("Responsa and Halakhic Decisions", "Responsa- Rishonim and Aharonim",
      "the Gaon of Baghdad's correspondence is largely responsive"),
     ("Stories and Belles Lettres", "Stories and Belles Lettres",
      "the collection is transmitted as literature")],
)
_ruling(
    "w000081", "(a) literary genre, documentary surface form",
    "The Silencing Epistle is a philosophical treatise addressed as a letter.",
    [("Philosophy, Theology, Ethical literature", "Philosophy",
      "the content is a philosophical argument"),
     _LETTERS,
     ("Polemics", "Other", "the treatise is polemical in purpose")],
)
_ruling(
    "w000058", "(c) between two adjacent leaves",
    "An account of a disputation with a priest: a polemic, or a narrative about "
    "one?",
    [("Polemics", "Polemics Jewish-Christian", "the subject is a Jewish-Christian dispute"),
     ("Stories and Belles Lettres", "Stories and Belles Lettres",
      "the text is cast as a narrative of an event")],
)
_ruling(
    "w000154", "(b) no plausible leaf in the closed vocabulary",
    "A nineteenth-century Hebrew memoir. The FJMS vocabulary, built for the "
    "Genizah corpus, has no leaf for modern autobiography.",
    [("Historiography and geographical descriptions",
      "Historiography and geographical descriptions", "memoir as historical writing"),
     ("Stories and Belles Lettres", "Stories and Belles Lettres", "memoir as literature"),
     (UNASSIGNED, UNASSIGNED, "the vocabulary genuinely cannot place it")],
    note="DATA QUALITY: a nineteenth-century maskilic memoir carrying shipped "
         "claims in a Genizah discovery corpus is itself worth checking",
)
_ruling(
    "w000065", "(b) no plausible leaf in the closed vocabulary",
    "Judah Rosh ha-Seder's Sefer ha-Shanim: the title reads as a book of years "
    "(a calendar treatise), but the author is known as a Talmudic lexicographer.",
    [("Astronomy", "Calendar", "reading the title as a calendrical treatise"),
     ("Philology", "Dictionaries", "the author's known lexicographic work"),
     ("Halakhic Literature and Talmudic Commentaries", "Talmud Bavli Commentaries",
      "the author's known Talmudic work")],
)
for _yos, _yos_why in (
    ("w000853", "the Hebrew Yosippon"),
    ("w000855", "an alternative ending of the Hebrew Yosippon"),
    ("w001152", "the Judaeo-Arabic Yosippon"),
):
    _ruling(
        _yos, "(c) between two adjacent leaves",
        f"Yosippon ({_yos_why}) is a historical narrative written as a romance. "
        "The feasibility sample recorded it as unplaceable; note that the "
        "vocabulary DOES carry a historiography node, contrary to that note.",
        [("Historiography and geographical descriptions",
          "Historiography and geographical descriptions", "Yosippon as history"),
         ("Stories and Belles Lettres", "Stories and Belles Lettres",
          "Yosippon as a historical romance")],
    )
for _seder in ("w000164", "w001066"):
    _ruling(
        _seder, "(c) between two adjacent leaves",
        "Seder Olam is a rabbinic chronography: a work of the rabbinic corpus, "
        "or a work of historiography?",
        [("Rabbinic Literature", "Other", "a tannaitic composition"),
         ("Historiography and geographical descriptions",
          "Historiography and geographical descriptions", "a chronography"),
         ("Derashot and Later Midrashim", "Later Midrashim", "transmitted midrashically")],
    )
_ruling(
    "w001055", "(b) no plausible leaf in the closed vocabulary",
    "Saadia's Sefer ha-Zikkaron: the subject of this title is not determinable "
    "from the title and author alone.",
    [("Astronomy", "Calendar", "if it belongs with his calendar works"),
     ("Halakhic", "Halakhic- Gaonim", "if it is a halakhic monograph"),
     ("Polemics", "Other", "if it belongs with his polemical works")],
)
_ruling(
    "w000160", "(c) between two adjacent leaves",
    "Arugat ha-Bosem names two different works: Archivolti's Hebrew rhetoric "
    "(the author the asset records) and Abraham b. Azriel's piyyut commentary "
    "(the better-known work of that name).",
    [("Philology", "Grammar", "Archivolti's rhetoric, per the recorded author"),
     ("Piyut and its Interpretation", "Piyyut Commentaries",
      "Abraham b. Azriel's piyyut commentary")],
    note="DATA QUALITY: the recorded author and the better-known work of this "
         "title disagree -- the ruling settles which work the claims are about",
)
_ruling(
    "w000057", "(c) between two adjacent leaves",
    "Dawud al-Muqammis' Twenty Chapters: kalam or theology? (The feasibility "
    "sample recorded this as one of its three low-confidence cases.)",
    [("Kalam", "Jewish Kalam", "the work is the founding Jewish kalam text"),
     ("Philosophy, Theology, Ethical literature", "Theology",
      "the work is read as systematic theology")],
)
_ruling(
    "w001149", "(b) no plausible leaf in the closed vocabulary",
    "A scholarly edition of the documents of Sicilian Jewry mixes several "
    "documentary kinds; no single documentary leaf covers it.",
    [("Documentary", "Documentary",
      "the parent node itself, deliberately coarse, covering the mixture"),
     ("Documentary", "Communal Documents", "if the edition is predominantly communal"),
     _LETTERS],
)
_ruling(
    "w000820", "(c) between two adjacent leaves",
    "Meshivat Nefesh is the title of Yeshua b. Judah's Karaite Torah commentary "
    "and also reads as a title for a devotional or remedial text.",
    [("Biblical Exegesis", "Biblical Exegesis- Karaite",
      "Yeshua b. Judah's commentary of this name"),
     ("Occult Sciences", "Shimmush Tehillim", "reading the title devotionally"),
     ("Philosophy, Theology, Ethical literature", "Ethical Literature",
      "reading the title as pietist")],
    note="assigning this without a ruling would be a guess; the two readings "
         "put it in different parents",
)
_ruling(
    "w000818", "(c) between two adjacent leaves",
    "Marpe la-Etzem reads as a medical title and is also transmitted with "
    "magical recipe material.",
    [("Medicine", "Medical Works", "reading the title medically"),
     ("Occult Sciences", "Magic Recipes", "if it is a recipe / praxis text")],
)
_ruling(
    "w000001", "(c) between two adjacent leaves",
    "Moses ibn Ezra's Kitab al-Muhadara is a treatise on Hebrew POETRY and "
    "rhetoric, not a philosophical work.",
    [("Secular Poetry", "Other", "a treatise on poetry"),
     ("Philology", "Grammar", "a treatise on rhetoric and language")],
)
for _kifaya in ("w000007", "w000036", "w000038"):
    _ruling(
        _kifaya, "(c) between two adjacent leaves",
        "Abraham Maimonides' Kifayat al-Abidin is the classical Jewish-Sufi "
        "pietist work: ethical literature, or the Sufi Literature leaf?",
        [("Philosophy, Theology, Ethical literature", "Ethical Literature",
          "the work's own genre is pietist ethics"),
         ("Philosophy, Theology, Ethical literature", "Sufi Literature",
          "the work is the central Jewish-Sufi text")],
    )
_ruling(
    "w000444", "(c) between two adjacent leaves",
    "Megillat Evyatar is a partisan historical account of the Palestinian "
    "gaonate, preserved as a communal document.",
    [("Historiography and geographical descriptions",
      "Historiography and geographical descriptions", "a historical narrative"),
     ("Documentary", "Communal Documents", "a communal record"),
     ("Polemics", "Polemics Rabbinical", "a partisan attack")],
)
_ruling(
    "w000040", "(b) no plausible leaf in the closed vocabulary",
    "Hoter b. Solomon is a fifteenth-century Yemenite philosopher; his "
    "'questions' are philosophical, though the surface form reads as responsa.",
    [("Philosophy, Theology, Ethical literature", "Philosophy",
      "the author's known philosophical work"),
     ("Responsa and Halakhic Decisions", "Responsa- Rishonim and Aharonim",
      "the surface form")],
)
_ruling(
    "w001004", "(c) between two adjacent leaves",
    "Chapters for the Ninth of Av: a midrashic composition, or an occasional "
    "liturgy for the fast?",
    [("Derashot and Later Midrashim", "Later Midrashim", "a midrashic composition"),
     ("Liturgy and Brakhot", "Occasional prayer", "a liturgy for the fast day"),
     ("Secular Poetry", "Dirges", "if the chapters are dirges")],
)
_ruling(
    "w001058", "(c) between two adjacent leaves",
    "The Life of Rabbenu ha-Qadosh is a hagiographic narrative about R. Judah "
    "ha-Nasi.",
    [("Stories and Belles Lettres", "Stories and Belles Lettres", "hagiographic narrative"),
     ("Historiography and geographical descriptions",
      "Historiography and geographical descriptions", "a biography")],
)
_ruling(
    "w001079", "(c) between two adjacent leaves",
    "The Alphabet of Ben Sira is a satirical narrative transmitted in midrashic "
    "dress.",
    [("Stories and Belles Lettres", "Stories and Belles Lettres", "satirical narrative"),
     ("Derashot and Later Midrashim", "Later Midrashim", "midrashic transmission")],
)
_ruling(
    "w001132", "(c) between two adjacent leaves",
    "al-Harizi's Kitab al-Durar is a Judaeo-Arabic literary anthology.",
    [("Stories and Belles Lettres", "Stories and Belles Lettres", "literary prose"),
     ("Secular Poetry", "Other", "the anthology carries poetry")],
)
for _yetzirah, _yetzirah_why in (
    ("w000522", "Sefer Yetzirah itself"),
    ("w000021", "Saadia's commentary on Sefer Yetzirah"),
):
    _ruling(
        _yetzirah, "(b) no plausible leaf in the closed vocabulary",
        f"{_yetzirah_why}: the vocabulary has no Sefer Yetzirah leaf, and the "
        "work sits between cosmological speculation, philosophy and mysticism.",
        [("Kabbalah", "Other", "the mystical tradition"),
         ("Philosophy, Theology, Ethical literature", "Philosophy",
          "the cosmological / philosophical reading"),
         ("Occult Sciences", "Theoretical Works", "the speculative-science reading")],
    )


# -- the owner's rulings on the held rows --------------------------------
# A TRACKED input the emitter reads, so a re-emission carries the rulings
# rather than discarding them (see the module docstring).  Every entry cites
# the section of `136-GATE1-DECISIONS.md` that settles it; every ruled leaf is
# checked against the LIVE FJMS tree AND against that row's own
# `candidate_leaves` before a row is emitted.
#
# The 29 held rows were ruled on 2026-08-03 in two sections:
#   Ruling P -- 5 rows settled from FJMS's OWN work-level domain
#               (`fjms_enrichment.db::genizah_titles.DomainId`, decoded via
#               AlmaIds carrying exactly one title and exactly one domain row).
#   Ruling Q -- the remaining 24, delegated by the owner ("Go with your
#               judgements, I trust you") and therefore DELEGATED judgements,
#               not owner-authored ones.  Ruling Q's governing principle:
#               where the closed vocabulary carries a leaf for exactly this
#               work, use it -- falling back to a broader leaf leaves the
#               specific node empty and destroys the information the facet
#               exists to expose.

RULING_P = "136-GATE1-DECISIONS.md § Ruling P"
RULING_Q = "136-GATE1-DECISIONS.md § Ruling Q"

#: work id -> the ruled assignment.  ``why`` is recorded on the row's ``note``.
OWNER_RULINGS: Dict[str, Dict[str, Any]] = {}


def _ruled(work_id: str, parent: str, leaf: str, citation: str, why: str) -> None:
    OWNER_RULINGS[work_id] = {
        "domain_parent": parent,
        "domain_leaf": leaf,
        "owner_ruling": citation,
        "why": why,
    }


# --- Ruling P: settled from FJMS's own work-level domain ----------------
for _yosippon in ("w001152", "w000853", "w000855"):
    _ruled(_yosippon,
           "Historiography and geographical descriptions",
           "Historiography and geographical descriptions", RULING_P,
           "FJMS's own work-level domain for this title (DomainId 180000, 100% "
           "concentration, n=98, exact normalized-title match) reads as "
           "historiography -- history, not romance")
for _seder_olam in ("w000164", "w001066"):
    _ruled(_seder_olam, "Rabbinic Literature", "Other", RULING_P,
           "FJMS's own work-level domain for this title (DomainId 120000, 100% "
           "concentration, n=87) files Seder Olam with the rabbinic corpus, not "
           "with historiography")

# --- Ruling Q: the remaining 24, delegated ------------------------------
for _kifayat in ("w000007", "w000036", "w000038"):
    _ruled(_kifayat, "Philosophy, Theology, Ethical literature",
           "Sufi Literature", RULING_Q,
           "the paradigmatic Jewish-Sufi text; the dedicated leaf exists for it, "
           "and falling back to Ethical Literature would leave it empty")
_ruled("w000001", "Secular Poetry", "Other", RULING_Q,
       "Kitab al-Muhadara is Hebrew POETICS; Grammar is about language structure")
_ruled("w001149", "Documentary", "Documentary", RULING_Q,
       "a deliberately MIXED documentary edition; the coarse parent avoids "
       "mislabelling its non-letter documents. The catalogue's 55% Letters "
       "describes the individual fragments, not the edition")
_ruled("w001140", "Philosophy, Theology, Ethical literature",
       "Ethical Literature", RULING_Q,
       "Yemen / Martyrdom / Resurrection are theological-ethical treatises in "
       "letter form; Documentary/Letters means archival correspondence and would "
       "be a category error")
for _yetzirah_ruled in ("w000021", "w000522"):
    _ruled(_yetzirah_ruled, "Kabbalah", "Other", RULING_Q,
           "the vocabulary has no Sefer Yetzirah leaf; the mystical tradition is "
           "its conventional home, and FJMS's own (n=1, unusable) signal also "
           "pointed mystical. Text and commentary are kept together so they do "
           "not split across parents. THIN: Saadia's commentary is genuinely "
           "philosophical-cosmological, and Philosophy is a defensible "
           "alternative for w000021 alone")
_ruled("w000058", "Polemics", "Polemics Jewish-Christian", RULING_Q,
       "catalogue 64% (n=69); its subject IS the disputation")
_ruled("w001132", "Secular Poetry", "Other", RULING_Q,
       "catalogue 38% (n=100); al-Harizi is a poet and the anthology carries his "
       "verse. THIN: Belles Lettres is arguable for a prose anthology")
_ruled("w000057", "Kalam", "Jewish Kalam", RULING_Q,
       "al-Muqammis' 'Ishrun Maqala is the FOUNDING Jewish kalam text; the "
       "dedicated leaf exists for it")
_ruled("w000820", "Biblical Exegesis", "Biblical Exegesis- Karaite", RULING_Q,
       "the catalogue is 64% Karaite contexts (n=64), which selects Yeshua b. "
       "Judah's commentary over the devotional readings")
_ruled("w000079", "Documentary", "Letters", RULING_Q,
       "unlike the Maimonidean epistles these are ACTUAL letters of the Baghdad "
       "Gaon preserved as correspondence")
_ruled("w000081", "Philosophy, Theology, Ethical literature", "Philosophy", RULING_Q,
       "this row's own note leads with 'a philosophical treatise addressed as a "
       "letter'")
_ruled("w000040", "Philosophy, Theology, Ethical literature", "Philosophy", RULING_Q,
       "catalogue DELIBERATELY overridden (51% Responsa, n=37): Hoter b. Solomon "
       "is a known Yemenite philosopher and this row records the responsa reading "
       "as surface form only. Not to be 'corrected' later")
_ruled("w000444", "Polemics", "Polemics Rabbinical", RULING_Q,
       "follows FJMS's own work-level domain. THIN: n=6, and it is a partisan "
       "account, so Historiography is arguable")
_ruled("w000160", "Philology", "Grammar", RULING_Q,
       "the RECORDED author is Archivolti and the catalogue agrees (42%). THIN: "
       "n=12, and this row is a title/author collision with Abraham b. Azriel's "
       "piyyut commentary -- a data-quality item this ruling does NOT settle")
_ruled("w000065", "Astronomy", "Calendar", RULING_Q,
       "the title states the subject; the lexicographic reading comes only from "
       "the author's OTHER work. THIN")
_ruled("w000818", "Medicine", "Medical Works", RULING_Q,
       "the title is explicit; magical recipes are a transmission context, not "
       "the work's subject")
_ruled("w000154", "Historiography and geographical descriptions",
       "Historiography and geographical descriptions", RULING_Q,
       "memoir as historical writing. Unassigned was DELIBERATELY not used -- it "
       "would hide a row that may not belong in the corpus at all; assigning it "
       "keeps it visible. THIN: the real question is corpus membership, recorded "
       "as data-quality")
_ruled("w001079", "Stories and Belles Lettres", "Stories and Belles Lettres", RULING_Q,
       "the Alphabet of Ben Sira is a satirical folk narrative in midrashic dress")
_ruled("w001055", "Halakhic", "Halakhic- Gaonim", RULING_Q,
       "THIN -- the LOWEST-CONFIDENCE call in this set. This row itself says the "
       "subject is not determinable from title+author; Saadia's monographs are "
       "predominantly halakhic, so this is a prior, not evidence. 3 claims")
_ruled("w001004", "Derashot and Later Midrashim", "Later Midrashim", RULING_Q,
       "'פרקי' marks a midrashic composition (cf. Pirkei de-Rabbi Eliezer). THIN")
_ruled("w001058", "Stories and Belles Lettres", "Stories and Belles Lettres", RULING_Q,
       "hagiography sits closer to narrative than to historiography")


def assert_rulings_are_answerable(
    vocab: Vocabulary,
    needs_ruling: Mapping[str, Mapping[str, Any]],
    rulings: Mapping[str, Mapping[str, Any]],
) -> None:
    """Every ruling must settle a HELD row, with a leaf that row actually offered.

    Three build errors, in the same fail-closed shape as
    :func:`assert_rules_within_vocabulary`:

      1. a ruling on a work that is not held -- there is nothing to rule on;
      2. a ruled ``(parent, leaf)`` outside the LIVE FJMS tree (T-136-09-01);
      3. a ruled leaf that is not one of THAT row's own ``candidate_leaves`` --
         a ruling answers the question that was put to the owner; it does not
         introduce a new option after the fact.
    """
    bad: List[str] = []
    for wid, spec in sorted(rulings.items()):
        parent = spec["domain_parent"]
        leaf = spec["domain_leaf"]
        if not spec.get("owner_ruling"):
            bad.append(f"ruling {wid}: carries no owner_ruling citation")
        held = needs_ruling.get(wid)
        if held is None:
            bad.append(
                f"ruling {wid}: is not a held needs-ruling row -- there is "
                "nothing here to rule on"
            )
            continue
        if not vocab.has_pair(parent, leaf) and not (
            parent == UNASSIGNED and leaf == UNASSIGNED
        ):
            bad.append(
                f"ruling {wid}: {leaf!r} is not under {parent!r} in the live "
                f"FJMS tree (actual parents: {vocab.parents_of(leaf)})"
            )
            continue
        offered = {
            (c["domain_parent"], c["domain_leaf"]) for c in held["candidate_leaves"]
        }
        if (parent, leaf) not in offered:
            bad.append(
                f"ruling {wid}: {parent!r} / {leaf!r} was not among the candidate "
                f"leaves put to the owner ({sorted(offered)})"
            )
    if bad:
        raise CurationError(
            "owner rulings do not settle the questions that were asked:\n  "
            + "\n  ".join(bad)
        )


def curate(
    worklist: Sequence[Mapping[str, Any]],
    vocab: Vocabulary,
    rules: Optional[Sequence[Mapping[str, Any]]] = None,
    manual: Optional[Mapping[str, Mapping[str, Any]]] = None,
    needs_ruling: Optional[Mapping[str, Mapping[str, Any]]] = None,
    rulings: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Assign every canonical work in ``worklist`` a domain.

    Order of precedence: an explicit owner-held ``needs-ruling`` row (carrying
    the owner's ruling when one has been recorded for it), then an
    individually-curated manual assignment, then the ordered rule table, then
    ``Unassigned`` (a REAL, visible bucket) for a work no rule places.

    ``rules`` / ``manual`` / ``needs_ruling`` / ``rulings`` default to this
    module's own curation tables; they are injectable so the behaviour can be
    exercised against a small vocabulary without the FJMS sidecar.  ``rulings``
    PAIRS with ``needs_ruling``: injecting a needs-ruling table without a
    rulings table means "no rulings", never "this module's 29 rulings against a
    two-row test table".
    """
    rules = CURATION_RULES if rules is None else rules
    manual = MANUAL_ASSIGNMENTS if manual is None else manual
    if rulings is None:
        rulings = OWNER_RULINGS if needs_ruling is None else {}
    needs_ruling = NEEDS_RULING if needs_ruling is None else needs_ruling
    assert_rules_within_vocabulary(vocab, rules)
    assert_rulings_are_answerable(vocab, needs_ruling, rulings)
    bad: List[str] = []
    for wid, spec in manual.items():
        if not vocab.has_pair(spec["domain_parent"], spec["domain_leaf"]):
            bad.append(f"manual {wid}: {spec['domain_leaf']!r} not under {spec['domain_parent']!r}")
    for wid, spec in needs_ruling.items():
        for cand in spec["candidate_leaves"]:
            if cand["domain_leaf"] == UNASSIGNED:
                continue
            if not vocab.has_pair(cand["domain_parent"], cand["domain_leaf"]):
                bad.append(
                    f"needs-ruling {wid}: candidate {cand['domain_leaf']!r} not "
                    f"under {cand['domain_parent']!r}"
                )
    if bad:
        raise CurationError(
            "curated assignments name node(s) outside the live FJMS vocabulary:\n  "
            + "\n  ".join(bad)
        )

    out: List[Dict[str, Any]] = []
    for entry in worklist:
        wid = entry["canonical_work_id"]
        title = normalize_title(entry.get("neutral_title"))
        author = normalize_title(entry.get("author"))

        if wid in needs_ruling:
            spec = needs_ruling[wid]
            row: Dict[str, Any] = {
                "canonical_work_id": wid,
                "domain_parent": None,
                "domain_leaf": None,
                "confidence": "needs-ruling",
                "provenance": "manual:held for owner ruling -- " + spec["case"],
                "candidate_leaves": spec["candidate_leaves"],
            }
            note = spec.get("note") or spec.get("question")
            ruling = rulings.get(wid)
            if ruling is not None:
                # RULED.  `confidence` deliberately STAYS `needs-ruling`: the
                # row was held and then settled by a ruling, which is not the
                # same provenance as a rule-derived `high`/`medium` row, and it
                # is the `owner_ruling` CITATION -- not the confidence value --
                # that the release gate reads.  `candidate_leaves` is kept so
                # the artifact still records what the ruling chose between.
                row["domain_parent"] = ruling["domain_parent"]
                row["domain_leaf"] = ruling["domain_leaf"]
                row["owner_ruling"] = ruling["owner_ruling"]
                row["provenance"] = (
                    "owner-ruling:" + ruling["owner_ruling"] + " -- " + spec["case"]
                )
                why = ruling.get("why")
                if why:
                    note = (note + "  ") if note else ""
                    note += "RULED (" + ruling["owner_ruling"] + "): " + why
            if note:
                row["note"] = note
            out.append(row)
            continue

        if wid in manual:
            spec = manual[wid]
            row = {
                "canonical_work_id": wid,
                "domain_parent": spec["domain_parent"],
                "domain_leaf": spec["domain_leaf"],
                "confidence": spec["confidence"],
                "provenance": spec["provenance"],
            }
            if spec.get("note"):
                row["note"] = spec["note"]
            out.append(row)
            continue

        rule = apply_rules(title, author, rules)
        if rule is not None:
            out.append(
                {
                    "canonical_work_id": wid,
                    "domain_parent": rule["domain_parent"],
                    "domain_leaf": rule["domain_leaf"],
                    "confidence": rule["confidence"],
                    "provenance": "rule:" + rule["name"],
                }
            )
            continue

        out.append(
            {
                "canonical_work_id": wid,
                "domain_parent": UNASSIGNED,
                "domain_leaf": UNASSIGNED,
                "confidence": "medium",
                "provenance": "manual:no rule placed this work; Unassigned is a "
                "visible bucket, not a silent disappearance",
            }
        )

    out.sort(key=lambda r: r["canonical_work_id"])
    return out


def build_artifact(
    assignments: Sequence[Mapping[str, Any]],
    vocab: Vocabulary,
    asset_basename: str,
) -> Dict[str, Any]:
    rows = [dict(r) for r in assignments]
    by_confidence: Dict[str, int] = {}
    for r in rows:
        by_confidence[r["confidence"]] = by_confidence.get(r["confidence"], 0) + 1
    ruled = sorted(
        {r["owner_ruling"] for r in rows if r.get("owner_ruling")}
    )
    n_ruled = sum(1 for r in rows if r.get("owner_ruling"))
    n_held = sum(
        1 for r in rows if r["confidence"] == "needs-ruling" and not r.get("owner_ruling")
    )
    posture = NEEDS_RULING_POSTURE_HELD
    if ruled:
        posture += (
            f" APPLIED: {n_ruled} row(s) carry an owner_ruling citation "
            f"({'; '.join(ruled)}) and {n_held} remain held."
        )
    doc = {
        "artifact": ARTIFACT_NAME,
        "artifact_version": ARTIFACT_VERSION,
        "generated_by": "scripts/curate_work_domains.py",
        "generated_utc": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "vocabulary_source": vocab.source,
        "asset_basename": asset_basename,
        "assignment_axis": ASSIGNMENT_AXIS,
        "needs_ruling_posture": posture,
        "rules": [
            {"name": r["name"], "description": r["description"],
             "domain_parent": r["domain_parent"], "domain_leaf": r["domain_leaf"]}
            for r in CURATION_RULES
        ],
        "counts": {
            "total": len(rows),
            "by_confidence": dict(sorted(by_confidence.items())),
            "unassigned": sum(1 for r in rows if r["domain_leaf"] == UNASSIGNED),
            "needs_ruling_held": n_held,
            "needs_ruling_ruled": n_ruled,
        },
        "content_hash": compute_content_hash(rows),
        "assignments": rows,
    }
    return doc


# ---------------------------------------------------------------------------
# Task 3 -- the author alias map.
# ---------------------------------------------------------------------------


def load_fjms_persons() -> List[Dict[str, Any]]:
    """The catalogue's own person vocabulary.

    Read through the same FJMS sidecar ``shared/fjms_service.py`` opens; the
    alias map bridges the discovery ``works.author`` strings to THESE ids, and
    ``FjmsService.get_browse_authors()`` is the accessor the facet cascade
    queries them back through.
    """
    from shared.fjms_service import get_fjms_service

    svc = get_fjms_service()
    conn = getattr(svc, "_conn", None)
    if conn is None:
        raise CurationError(
            "FJMS sidecar is not open -- cannot build the author alias map "
            "(fail closed rather than emitting an empty map)"
        )
    rows = conn.execute(
        "SELECT GenizahPersonId, EngDesc, HebDesc FROM genizah_persons "
        "WHERE GenizahPersonId > 0 AND HebDesc IS NOT NULL AND HebDesc != ''"
    ).fetchall()
    persons = [
        {
            "person_id": int(r["GenizahPersonId"]),
            "eng_desc": r["EngDesc"],
            "heb_desc": r["HebDesc"],
        }
        for r in rows
    ]
    if not persons:
        raise CurationError("FJMS person vocabulary is empty -- fail closed")
    return persons


def resolve_author_alias(
    author: str,
    persons: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Resolve ONE discovery author string against the catalogue person list.

    Deterministic and ORDER-INDEPENDENT: an exact match beats every
    containment match regardless of the order ``persons`` arrives in; among
    containment matches the LONGEST (most specific) catalogue name wins; and
    every remaining tie is broken by the smallest ``person_id`` -- the same
    "deterministic, order-independent representative" discipline
    ``shared/discovery_novelty.py::novelty_work_key`` already uses for its
    alias groups.

    The longest-first containment rule is load-bearing, not cosmetic: the
    catalogue carries both a bare given name and the full name of the same
    person, and a smallest-id-only tie-break resolved the corpus's second most
    frequent author (39 works) onto the bare given name while the full name sat
    in the same candidate list.

    Returns ``{"match": <exact|containment|unmatched>, "person_id", "heb_desc",
    "eng_desc", "candidates"}``.  An unmatched author is RETAINED as unmatched,
    never forced onto a near-neighbour and never dropped.
    """
    key = normalize_title(author)
    exact: List[Mapping[str, Any]] = []
    contains: List[Mapping[str, Any]] = []
    for p in persons:
        heb = normalize_title(p.get("heb_desc"))
        if not heb:
            continue
        if heb == key:
            exact.append(p)
        elif key and (heb in key or key in heb):
            contains.append(p)

    if exact:
        winner = min(exact, key=lambda p: int(p["person_id"]))
        return {
            "match": "exact",
            "person_id": int(winner["person_id"]),
            "heb_desc": winner.get("heb_desc"),
            "eng_desc": winner.get("eng_desc"),
            "candidates": sorted(int(p["person_id"]) for p in exact),
        }
    if contains:
        # Longest (most specific) catalogue name first, then smallest id.
        winner = min(
            contains,
            key=lambda p: (-len(normalize_title(p.get("heb_desc"))), int(p["person_id"])),
        )
        return {
            "match": "containment",
            "person_id": int(winner["person_id"]),
            "heb_desc": winner.get("heb_desc"),
            "eng_desc": winner.get("eng_desc"),
            "candidates": sorted(int(p["person_id"]) for p in contains),
        }
    return {
        "match": "unmatched",
        "person_id": None,
        "heb_desc": None,
        "eng_desc": None,
        "candidates": [],
    }


def build_alias_artifact(
    worklist: Sequence[Mapping[str, Any]],
    persons: Sequence[Mapping[str, Any]],
    asset_basename: str,
) -> Dict[str, Any]:
    """The alias map over every DISTINCT ``works.author`` string in the worklist."""
    by_author: Dict[str, List[str]] = {}
    for entry in worklist:
        author = entry.get("author")
        if not author:
            continue
        by_author.setdefault(author, []).append(entry["canonical_work_id"])

    aliases: List[Dict[str, Any]] = []
    for author in sorted(by_author):
        resolved = resolve_author_alias(author, persons)
        aliases.append(
            {
                "author": author,
                "normalized": normalize_title(author),
                "works": sorted(by_author[author]),
                "work_count": len(by_author[author]),
                "match": resolved["match"],
                "fjms_person_id": resolved["person_id"],
                "fjms_heb_desc": resolved["heb_desc"],
                "fjms_eng_desc": resolved["eng_desc"],
                "fjms_candidate_person_ids": resolved["candidates"],
            }
        )

    counts: Dict[str, int] = {t: 0 for t in ALIAS_MATCH_TOKENS}
    for a in aliases:
        counts[a["match"]] += 1

    total_works = len(worklist)
    with_author = sum(1 for e in worklist if e.get("author"))
    doc = {
        "artifact": ALIAS_ARTIFACT_NAME,
        "artifact_version": ARTIFACT_VERSION,
        "generated_by": "scripts/curate_work_domains.py",
        "generated_utc": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "asset_basename": asset_basename,
        "person_vocabulary_source": (
            "fist_data/fjms_enrichment.db :: genizah_persons "
            "(the list shared.fjms_service.FjmsService.get_browse_authors reads back)"
        ),
        "author_gap_rule": (
            "an author gap is filled ONLY from the work's own metadata. An "
            "author is NEVER inferred from a title pattern -- that is how the "
            "wrong Bahya gets attributed, which this corpus already "
            "demonstrates."
        ),
        "counts": {
            "canonical_works": total_works,
            "works_with_author": with_author,
            "distinct_authors": len(aliases),
            "by_match": counts,
            "gaps_left_unfilled": total_works - with_author,
        },
        "content_hash": compute_content_hash(aliases),
        "aliases": aliases,
    }
    return doc


def validate_alias_artifact(doc: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    if doc.get("artifact") != ALIAS_ARTIFACT_NAME:
        errors.append(f"artifact must be {ALIAS_ARTIFACT_NAME!r}")
    aliases = doc.get("aliases")
    if not isinstance(aliases, list) or not aliases:
        errors.append("aliases must be a non-empty list")
        return errors
    declared = doc.get("content_hash")
    actual = compute_content_hash(aliases)
    if not declared:
        errors.append("content_hash is missing")
    elif declared != actual:
        errors.append(f"content_hash mismatch: declared {declared}, actual {actual}")
    seen: Set[str] = set()
    for i, row in enumerate(aliases):
        where = f"aliases[{i}]"
        if not isinstance(row, dict):
            errors.append(f"{where} must be an object")
            continue
        author = row.get("author")
        if not isinstance(author, str) or not author.strip():
            errors.append(f"{where} author is missing or blank")
            continue
        if author in seen:
            errors.append(f"{where} duplicate author key {author!r}")
        seen.add(author)
        match = row.get("match")
        if match not in ALIAS_MATCH_TOKENS:
            errors.append(f"{where} match {match!r} outside {list(ALIAS_MATCH_TOKENS)}")
            continue
        pid = row.get("fjms_person_id")
        if match == "unmatched":
            if pid is not None:
                errors.append(f"{where} unmatched author must carry a null fjms_person_id")
        elif not isinstance(pid, int):
            errors.append(f"{where} {match} match must carry an integer fjms_person_id")
    return errors


# ---------------------------------------------------------------------------
# Reporting.
# ---------------------------------------------------------------------------


def build_report(doc: Mapping[str, Any], worklist_index: Mapping[str, Mapping[str, Any]]) -> str:
    rows = doc["assignments"]
    lines: List[str] = []
    lines.append("WORK -> DOMAIN CURATION REPORT")
    lines.append("=" * 70)
    lines.append(f"artifact          : {doc.get('artifact')} {doc.get('artifact_version')}")
    lines.append(f"content_hash      : {doc.get('content_hash')}")
    lines.append(f"vocabulary        : {doc.get('vocabulary_source')}")
    lines.append(f"assignment axis   : {doc.get('assignment_axis')}")
    lines.append("")
    counts = doc.get("counts", {})
    lines.append(f"total works       : {counts.get('total')}")
    for token in CONFIDENCE_TOKENS:
        lines.append(f"  {token:<14}: {counts.get('by_confidence', {}).get(token, 0)}")
    lines.append(f"Unassigned        : {counts.get('unassigned')}")
    lines.append(f"needs-ruling held : {counts.get('needs_ruling_held')}")
    lines.append(f"needs-ruling ruled: {counts.get('needs_ruling_ruled', 0)}")
    lines.append("")

    by_leaf: Dict[Tuple[str, str], int] = {}
    for r in rows:
        if r["domain_leaf"] is None:
            continue
        key = (r["domain_parent"], r["domain_leaf"])
        by_leaf[key] = by_leaf.get(key, 0) + 1
    lines.append("DOMAIN DISTRIBUTION")
    lines.append("-" * 70)
    for (parent, leaf), n in sorted(by_leaf.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"{n:>5}  {parent} / {leaf}")
    lines.append("")

    held = [r for r in rows if r["confidence"] == "needs-ruling" and not r.get("owner_ruling")]
    lines.append(f"NEEDS RULING ({len(held)})")
    lines.append("-" * 70)
    for r in held:
        meta = worklist_index.get(r["canonical_work_id"], {})
        lines.append(f"{r['canonical_work_id']}  {meta.get('neutral_title', '')}")
        if meta.get("author"):
            lines.append(f"            author: {meta['author']}")
        lines.append(f"            claims: {meta.get('shipped_claims', '?')}")
        if r.get("note"):
            lines.append(f"            question: {r['note']}")
        for cand in r.get("candidate_leaves", []):
            lines.append(
                f"              - {cand['domain_parent']} / {cand['domain_leaf']}"
                f"   ({cand['case']})"
            )
        lines.append("")

    ruled = [r for r in rows if r.get("owner_ruling")]
    lines.append(f"RULED BY THE OWNER ({len(ruled)})")
    lines.append("-" * 70)
    for r in ruled:
        meta = worklist_index.get(r["canonical_work_id"], {})
        lines.append(
            f"{r['canonical_work_id']}  {meta.get('neutral_title', '')}"
            f"   ->   {r['domain_parent']} / {r['domain_leaf']}"
        )
        lines.append(f"            ruling: {r['owner_ruling']}")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------------


def _write_json(path: str, doc: Mapping[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=1, sort_keys=False)
        fh.write("\n")
    os.replace(tmp, path)


def _read_json(path: str) -> Dict[str, Any]:
    if not os.path.isfile(path):
        raise CurationError(f"artifact not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="curate_work_domains.py",
        description=(
            "Curate an FJMS domain for every canonical work carrying a shipped "
            "discovery claim, plus the author alias map. The closed vocabulary "
            "is read from shared/fjms_service.py at RUNTIME -- never snapshotted."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "modes:\n"
            "  --emit-worklist          the canonical works needing assignment, at the canonical grain\n"
            "  --emit-artifact          run the curation pass and write the hash-pinned artifact\n"
            "  --validate PATH          check an artifact against the closed vocabulary and the schema\n"
            "  --report PATH            coverage, confidence distribution, and the needs-ruling rows\n"
            "  --emit-aliases           build the author alias map artifact\n"
            "  --validate-aliases PATH  check an alias artifact\n"
            "\n"
            "needs-ruling posture:\n"
            "  A needs-ruling row is HELD (domain_leaf null + candidate leaves) and may never\n"
            "  carry a guessed leaf. --validate passes on a held row; --validate --release FAILS\n"
            "  while any held row is unruled. The 'ship as Unassigned' default was explicitly\n"
            "  DECLINED by the owner (136-GATE1-DECISIONS.md group D).\n"
            "  When the owner rules, the ruling goes in this module's OWNER_RULINGS table --\n"
            "  a TRACKED input --emit-artifact reads -- so re-emitting reproduces the ruled\n"
            "  rows instead of discarding them, and the artifact is never hand-edited.\n"
        ),
    )
    p.add_argument("--emit-worklist", action="store_true",
                   help="print the canonical works needing assignment (JSON)")
    p.add_argument("--emit-artifact", action="store_true",
                   help="run the curation pass and write the hash-pinned domain artifact")
    p.add_argument("--validate", metavar="PATH", nargs="?", const=DEFAULT_DOMAINS_ARTIFACT,
                   help="validate a domain artifact (default: %(const)s)")
    p.add_argument("--report", metavar="PATH", nargs="?", const=DEFAULT_DOMAINS_ARTIFACT,
                   help="print the coverage / confidence / needs-ruling report")
    p.add_argument("--emit-aliases", action="store_true",
                   help="build the author alias map artifact")
    p.add_argument("--validate-aliases", metavar="PATH", nargs="?", const=DEFAULT_ALIASES_ARTIFACT,
                   help="validate an author alias artifact (default: %(const)s)")
    p.add_argument("--release", action="store_true",
                   help="with --validate: apply the fail-closed shipping gate "
                        "(any unruled needs-ruling row fails)")
    p.add_argument("--out", metavar="PATH", default=None,
                   help="output path for --emit-artifact / --emit-aliases")
    p.add_argument("--asset", metavar="PATH", default=None,
                   help="discovery asset path (default: resolved from discovery_data/manifest.json)")
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    if not any(
        [args.emit_worklist, args.emit_artifact, args.validate, args.report,
         args.emit_aliases, args.validate_aliases]
    ):
        build_parser().print_help()
        return 2

    try:
        if args.emit_worklist:
            asset = args.asset or resolve_asset_path()
            worklist = load_worklist(asset)
            json.dump(worklist, sys.stdout, ensure_ascii=False, indent=1)
            sys.stdout.write("\n")
            return 0

        if args.emit_artifact:
            asset = args.asset or resolve_asset_path()
            vocab = load_vocabulary()
            worklist = load_worklist(asset)
            assignments = curate(worklist, vocab)
            basename = os.path.splitext(os.path.basename(asset))[0]
            doc = build_artifact(assignments, vocab, basename)
            out = args.out or DEFAULT_DOMAINS_ARTIFACT
            _write_json(out, doc)
            print(f"wrote {out}")
            print(f"content_hash={doc['content_hash']}")
            print(f"counts={json.dumps(doc['counts'], ensure_ascii=False)}")
            return 0

        if args.validate:
            doc = _read_json(args.validate)
            vocab = load_vocabulary()
            asset = args.asset or resolve_asset_path()
            canonical = load_canonical_ids(asset)
            errors = validate_artifact(doc, vocab, canonical, release=args.release)
            if errors:
                print(f"INVALID: {len(errors)} error(s)")
                for e in errors[:50]:
                    print("  - " + e)
                if len(errors) > 50:
                    print(f"  ... and {len(errors) - 50} more")
                return 1
            print(
                f"VALID: {len(doc['assignments'])} assignment(s), "
                f"content_hash={doc.get('content_hash')}"
                + (" [RELEASE GATE PASSED]" if args.release else "")
            )
            return 0

        if args.report:
            doc = _read_json(args.report)
            asset = args.asset or resolve_asset_path()
            worklist = load_worklist(asset)
            index = {e["canonical_work_id"]: e for e in worklist}
            sys.stdout.write(build_report(doc, index) + "\n")
            return 0

        if args.emit_aliases:
            asset = args.asset or resolve_asset_path()
            worklist = load_worklist(asset)
            persons = load_fjms_persons()
            doc = build_alias_artifact(worklist, persons, os.path.splitext(os.path.basename(asset))[0])
            out = args.out or DEFAULT_ALIASES_ARTIFACT
            _write_json(out, doc)
            print(f"wrote {out}")
            print(f"content_hash={doc['content_hash']}")
            print(f"counts={json.dumps(doc['counts'], ensure_ascii=False)}")
            return 0

        if args.validate_aliases:
            doc = _read_json(args.validate_aliases)
            errors = validate_alias_artifact(doc)
            if errors:
                print(f"INVALID: {len(errors)} error(s)")
                for e in errors[:50]:
                    print("  - " + e)
                return 1
            print(f"VALID: {len(doc['aliases'])} alias row(s), content_hash={doc.get('content_hash')}")
            return 0
    except CurationError as exc:
        print(f"FAIL CLOSED: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
