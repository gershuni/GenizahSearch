"""The Genuzot team's transcription marks: the legend shown beside their texts.

FGP's Genuzot team ("צוות FGP להעתקות -גנוזות" / "FGP Transcriptions Team -
Genuzos", 35,639 of the 59,056 rows in the 2026-10 source-folders sidecar)
types its marks into the transcription itself: ``?ב?`` for a doubtful letter,
``@...@`` around a piyyut, ``<...>`` for a line filler. Both apps show the text
as typed. Beside a Genuzot transcription they show a "?" that opens this
table, so a reader can tell a mark from the text (GS-274, option A). Nothing
here changes the text: search, copy and export are untouched. Drawing the marks
is GS-274 option B, deferred until FGP confirms the meanings this list cannot
give.

THE SOURCE OF THE MEANINGS
--------------------------
``MARKS`` follows the team's own list of marks, "סיגלה אקדמית" (2024), one row
per mark, in our own words. The team's 2014 list for its Talmud work differs in
places; ``OLDER_LIST`` gives the three meanings from it that a reader of older
transcriptions may need, labelled as such. ``NOT_IN_LIST`` names marks that
occur in the texts but are in neither list; the legend says so rather than
guessing (the census of 2026-10-26 is in ``docs/ISSUE_DETAILS.md`` GS-274).

Like ``shared/transcription_credits.py`` this takes ``lang`` explicitly and
imports nothing beyond the standard library and ``shared.fgp_service``.
"""

from __future__ import annotations

import html as _html
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

from shared.fgp_service import source_relation_kind

#: Substrings that identify the team in each credit column (``source_credit_en``,
#: ``source_credit_he`` and the legacy Hebrew ``source_credit``).
_GENUZOT_CREDIT_MARKERS = ("Genuzos", "גנוזות")


@dataclass(frozen=True)
class Mark:
    """One mark: how it looks in the text, and what it means in each language."""
    form: str
    en: str
    he: str

    def meaning(self, lang: str) -> str:
        return self.he if _is_hebrew(lang) else self.en


#: The team's list (2024), in its order. ``form`` is how the mark appears in a
#: transcription; ``…`` stands for the marked text.
MARKS: Tuple[Mark, ...] = (
    Mark("(…)", "deleted text", "טקסט מחוק"),
    Mark("^…^", "between the lines", "בין השיטין"),
    Mark("|…|", "added in the margin or between the lines", "תוספת בגליון או בין השיטין"),
    Mark("=…=", "in the margin", "בגליון"),
    Mark("[....]", "torn or blurred", "קרוע או מטושטש"),
    Mark("{…}", "a correction", "תיקון"),
    Mark("{א//ב}", "a correction where it is unclear which is the original and which the correction",
         "תיקון, כשלא ברור מה המקור ומה התיקון"),
    Mark("((…))", "an erasure", "גרידה"),
    Mark("?…?", "a doubtful reading", "קריאה מסופקת"),
    Mark("?א/ב?", "doubtful letters: either reading is possible", "אותיות מסופקות: כל אחת מהקריאות אפשרית"),
    Mark("%…%", "a catchword", "שומר דף"),
    Mark("<…>", "a line filler", "מילוי שורה"),
    Mark("*…*", "a heading", "כותרת"),
    Mark("-…-", "a page number", "מספר עמוד"),
    Mark("$…$", "laws and instructions", "הלכות והוראות"),
    Mark("@…@", "a piyyut", "פיוט"),
    Mark("«…»", "additions in special circumstances", "תוספות בנסיבות מיוחדות"),
    Mark("&…&", "Judeo-Arabic", "ערבית יהודית"),
    Mark("¿…¿", "a foreign language", "שפה זרה"),
    Mark("##", "dots above a word", "נקודות מעל מילה"),
    Mark("#", "a dot above a letter", "נקודה מעל אות"),
    Mark("ב+", "an abbreviation of \"ברוך אתה ה'\"", "ברוך אתה ה'"),
    Mark("°", "a sign pointing to the margin", "סימון לגליון"),
)

#: Meanings from the team's 2014 list that the 2024 list gives differently or
#: not at all. Shown apart, because a transcription may follow either.
OLDER_LIST: Tuple[Mark, ...] = (
    Mark("[…]", "in the margin or between the lines", "בגליון או בין השיטין"),
    Mark("§…§", "a variant reading that the manuscript marks as one", "נוסח אחר שכתב היד מציין במפורש"),
    Mark("²", "a paragraph end of an unusual form", "סוף פסקה שאינו בצורה הרגילה"),
)

#: Marks that occur in the texts but that neither list explains.
NOT_IN_LIST: Tuple[str, ...] = (
    "@ (around one to three letters inside a word)",
    "¶",
    "\\",
    "[[…]]",
    "[|…|]",
    "¦…¦",
    "◌",
    "??",
)

_NOT_IN_LIST_HE: Tuple[str, ...] = (
    "@ (סביב אות אחת עד שלוש בתוך מילה)",
    "¶",
    "\\",
    "[[…]]",
    "[|…|]",
    "¦…¦",
    "◌",
    "??",
)

_LABELS_EN: Dict[str, str] = {
    "title": "Transcription marks",
    "button_tooltip": "What the marks in this transcription mean",
    "intro": ("The FGP Genuzot team marks what it sees in the manuscript with signs typed "
              "into the transcription. Their meanings, following the team's list of marks (2024):"),
    "col_mark": "Mark",
    "col_meaning": "Meaning",
    "older_heading": "In the team's earlier list (2014)",
    "older_intro": "Some older transcriptions may follow this earlier list, which differs here:",
    "unexplained_heading": "Not in the team's lists",
    "unexplained_intro": "These marks also occur in the transcriptions, and we do not know their meaning:",
    "question_mark_note": "A ? may also be an ordinary question mark.",
}

_LABELS_HE: Dict[str, str] = {
    "title": "סימני התעתוק",
    "button_tooltip": "מה פירוש הסימנים בתעתוק זה",
    "intro": ("צוות גנוזות של פרויקט פרידברג מסמן את מה שהוא רואה בכתב היד בסימנים "
              "שבתוך התעתוק. פירושם, על פי רשימת הסימנים של הצוות (2024):"),
    "col_mark": "סימן",
    "col_meaning": "פירוש",
    "older_heading": "ברשימה הקודמת של הצוות (2014)",
    "older_intro": "תעתוקים ישנים אחדים עשויים ללכת לפי הרשימה הקודמת, השונה בסימנים אלה:",
    "unexplained_heading": "סימנים שאינם ברשימות הצוות",
    "unexplained_intro": "גם הסימנים האלה מופיעים בתעתוקים, ופירושם אינו ידוע לנו:",
    "question_mark_note": "? יכול להיות גם סימן שאלה רגיל.",
}


def _is_hebrew(lang: Optional[str]) -> bool:
    return str(lang or "").lower().startswith("he")


def labels(lang: str) -> Dict[str, str]:
    """The legend's frame (title, headings, notes) in ``lang``."""
    return _LABELS_HE if _is_hebrew(lang) else _LABELS_EN


def not_in_list(lang: str) -> Tuple[str, ...]:
    """The unexplained marks, described in ``lang``."""
    return _NOT_IN_LIST_HE if _is_hebrew(lang) else NOT_IN_LIST


def legend_html(lang: str) -> str:
    """The whole legend as one HTML fragment, for both apps.

    Plain tables and paragraphs only, so Qt's rich-text subset renders it the
    way a browser does. Every string is escaped: the marks are `<`, `&`, `$`.
    """
    lab = labels(lang)
    direction = "rtl" if _is_hebrew(lang) else "ltr"
    align = "right" if direction == "rtl" else "left"
    esc = _html.escape
    cell = "padding: 3px 10px; border-bottom: 1px solid #ddd;"
    mark_cell = (f"{cell} font-family: Consolas, 'Courier New', monospace; "
                 "white-space: nowrap; direction: ltr; unicode-bidi: embed;")

    def table(marks: Tuple[Mark, ...]) -> str:
        head = (f"<tr><th style='{cell} text-align: {align};'>{esc(lab['col_mark'])}</th>"
                f"<th style='{cell} text-align: {align};'>{esc(lab['col_meaning'])}</th></tr>")
        rows = "".join(
            f"<tr><td style=\"{mark_cell}\" dir='ltr'>{esc(m.form)}</td>"
            f"<td style='{cell}'>{esc(m.meaning(lang))}</td></tr>"
            for m in marks)
        return (f"<table width='100%' cellspacing='0' style='border-collapse: collapse; width: 100%;'>"
                f"{head}{rows}</table>")

    unexplained = "".join(f"<li>{esc(item)}</li>" for item in not_in_list(lang))
    return (
        f"<div dir='{direction}' style='text-align: {align};'>"
        f"<p>{esc(lab['intro'])}</p>"
        f"{table(MARKS)}"
        f"<p>{esc(lab['question_mark_note'])}</p>"
        f"<h4>{esc(lab['older_heading'])}</h4>"
        f"<p>{esc(lab['older_intro'])}</p>"
        f"{table(OLDER_LIST)}"
        f"<h4>{esc(lab['unexplained_heading'])}</h4>"
        f"<p>{esc(lab['unexplained_intro'])}</p>"
        f"<ul>{unexplained}</ul>"
        "</div>"
    )


def uses_genuzot_marks(source: Optional[Mapping[str, Any]]) -> bool:
    """Is ``source`` a transcription by the Genuzot team, which uses these marks?

    ``source`` is a chooser source dict (``shared.fgp_service._row_to_fgp_source``).
    Translations are excluded: the marks belong to the transcription.
    """
    if not source:
        return False
    if not (source.get("is_fgp") or source.get("source") == "fgp"):
        return False
    if source_relation_kind(dict(source)) == "translation":
        return False
    credits = (source.get("source_credit_en"), source.get("source_credit_he"),
               source.get("source_credit"))
    return any(marker in (credit or "")
               for credit in credits for marker in _GENUZOT_CREDIT_MARKERS)
