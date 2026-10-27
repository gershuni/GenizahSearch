"""The Genuzot team's transcription marks: the legend shown beside their texts.

FGP's Genuzot team ("צוות FGP להעתקות -גנוזות" / "FGP Transcriptions Team -
Genuzos", 35,639 of the 59,056 rows in the 2026-10 source-folders sidecar)
types its marks into the transcription itself: ``?ב?`` for a doubtful letter,
``@...@`` around a piyyut, ``<...>`` for a line filler. Both apps show the text
as typed. Beside a Genuzot transcription they show a "?" that opens this
legend, so a reader can tell a mark from the text (GS-274, option A). Nothing
here changes the text: search, copy and export are untouched. Drawing the marks
is GS-274 option B, deferred.

THE TWO LISTS (owner, 2026-10-27)
---------------------------------
The team has a list for each kind of fragment, and the legend shows both,
headed and nothing else: the sidecar cannot tell which a row follows (23,218 of
the 35,639 Genuzot rows have no domain, and the Talmud list's marks occur
outside the rows marked Talmud too).

* ``PRAYER_MARKS`` -- prayer and piyyut fragments: "סיגלה אקדמית" (2024), an
  image-only PDF the owner holds, row by row. The owner added the second use of
  ``@`` (around letters inside a word); a ``?`` is never a plain question mark.
* ``TALMUD_MARKS`` -- Talmud fragments, transcribed for Hachi Garsinan: the
  2014 Word table "סיגלה עיונית וסיגלה חזותית", its academic column, every row
  with a mark that plain text can carry (rows 1-2 are fonts only).

The meanings follow each list's own wording. Like ``shared/transcription_credits.py``
this takes ``lang`` explicitly and imports nothing beyond the standard library
and ``shared.fgp_service``.
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


#: Prayer and piyyut fragments (2024), in the list's order. ``form`` is how the
#: mark appears in a transcription; ``…`` stands for the marked text.
PRAYER_MARKS: Tuple[Mark, ...] = (
    Mark("(…)", "deleted text", "טקסט מחוק"),
    Mark("^…^", "between the lines", "בין השיטין"),
    Mark("|…|", "an addition in the margin or between the lines", "תוספת בגליון או בין השיטין"),
    Mark("=…=", "margin", "גליון"),
    Mark("[....]", "torn or blurred", "קרוע או מטושטש"),
    Mark("{…}", "a correction", "תיקון"),
    Mark("{א//ב}", "a correction, where it is unclear which is the original and which the correction",
         "תיקון, כשלא ברור מה המקור ומה התיקון"),
    Mark("((…))", "an erasure", "גרידה"),
    Mark("?…?", "a doubtful reading", "קריאה מסופקת"),
    Mark("?א/ב?", "doubtful letters", "אותיות מסופקות"),
    Mark("%…%", "a catchword", "שומר דף"),
    Mark("<…>", "a line filler", "מילוי שורה"),
    Mark("*…*", "a heading", "כותרת"),
    Mark("-…-", "a page number", "מספר עמוד"),
    Mark("$…$", "laws and instructions", "הלכות והוראות"),
    Mark("@…@", "a piyyut; around letters inside a word: something unusual about the letters",
         "פיוט; סביב אותיות בתוך מילה: משהו חריג באותיות"),
    Mark("«…»", "additions in special circumstances", "תוספות בנסיבות מיוחדות"),
    Mark("&…&", "Judeo-Arabic", "ערבית יהודית"),
    Mark("¿…¿", "foreign languages", "שפות זרות"),
    Mark("##", "dots above a word", "נקודות מעל מילה"),
    Mark("#", "dots above a letter", "נקודות מעל אות"),
    Mark("ב+", "ברוך אתה ה'", "ברוך אתה ה'"),
    Mark("°", "a sign pointing to the margin", "סימון לגליון"),
)

#: Talmud fragments, for Hachi Garsinan (2014), in the table's order (its row
#: numbers in the comments; 4, 5 and 7 share the bracket, 22 and 24 have two forms).
TALMUD_MARKS: Tuple[Mark, ...] = (
    Mark("(…)", "deletion", "מחיקה"),                                                       # 3
    Mark("[…]", "margin; between the lines; added after the word or line was completed",  # 4, 5, 7
         "גליון; בין השיטין; נוספה לאחר השלמת התיבה או השורה"),
    Mark("¦…¦", "an addition inside a margin or interlinear addition", "הוספה בתוך גליון או בין השיטין"),  # 6
    Mark("(…){…}", "a correction in the body of the manuscript or over an erasure",       # 8
         "תיקון בגוף כתב היד או על גרידה"),
    Mark("[2]…[1]…", "correcting the order of the words or the letters", "תיקון סדר המילים או סדר האותיות"),  # 9
    Mark("ᵔ", "joining two words into one", "חיבור של שתי תיבות לאחת"),                    # 10
    Mark("ᵕ", "separating two words", "הפרדה בין שתי תיבות"),                              # 11
    Mark("א֗", "an almost certain reading", "קריאה כמעט ודאית"),                             # 12
    Mark("א֯", "a doubtful reading", "קריאה מסופקת"),                                        # 13
    Mark("א֯/ב֯", "two possible readings", "שתי אפשרויות לקריאה"),                            # 14
    Mark("(א֯){ב֯}/(ב֯){א֯}", "a correction, where it is unclear what was corrected to what",  # 15
         "תיקון, כשלא ברור מה תוקן למה"),
    Mark("....", "torn or blurred", "קריעה או טשטוש"),                                      # 16
    Mark("*…*", "anything not part of the text of the Talmud or Mishnah: headings, hadran, colophon, "
                "commentary and the like",                                                 # 17
         "כל דבר שאינו שייך לגוף הטקסט של התלמוד או המשנה: כותרות, הדרן, סליק, קולופון, פירוש וכיוצא בזה"),
    Mark("א?", "a sign pointing to the margin", "סימון לגליון"),                            # 18
    Mark("§…§", "another reading, only where it is written explicitly (נ\"א and the like)",  # 19
         "נוסח אחר, רק שכתוב במפורש (נ\"א וכדומה)"),
    Mark("[1]", "a note", "הערה"),                                                          # 20
    Mark("?…?", "something unidentified around the word or the letter",                    # 21
         "דברים לא מזוהים סביב המילה או האות"),
    Mark("#", "a decoration above a letter", "קישוט מעל אות"),                              # 22
    Mark("##", "a decoration or a line above a word", "קישוט או קו מעל מילה"),               # 22
    Mark("-א-", "page numbering", "מספור עמוד"),                                            # 23
    Mark("<…>", "a catchword (<ראובן>) or a line filler (<רא>)", "שומר דף (<ראובן>) או מילוי שורה (<רא>)"),  # 24
    Mark("<~>", "undefined", "לא מוגדר"),                                                   # 24
    Mark("<(…)>", "a catchword or line filler that differs from the next line or page",    # 25
         "שומר דף או מילוי שורה השונים מהשורה או מהעמוד הבא"),
    Mark("<<…>>", "the start of the next line or page is torn or missing",                  # 26
         "תחילת השורה הבאה או העמוד הבא קרועה או איננה"),
    Mark("²", "a non-standard end of paragraph", "סוף פסקה לא סטנדרטי"),                    # 27
)

_LABELS_EN: Dict[str, str] = {
    "title": "Transcription marks",
    "button_tooltip": "What the marks in this transcription mean",
    "col_mark": "Mark",
    "col_meaning": "Meaning",
    "prayer_heading": "Genuzot team sigla - prayer and piyyut fragments",
    "talmud_heading": "Genuzot team sigla - Talmud fragments",
}

_LABELS_HE: Dict[str, str] = {
    "title": "סימני התעתוק",
    "button_tooltip": "מה פירוש הסימנים בתעתוק זה",
    "col_mark": "סימן",
    "col_meaning": "פירוש",
    "prayer_heading": "סיגלה צוות גנוזות - קטעי תפילה ופיוט",
    "talmud_heading": "סיגלה צוות גנוזות - קטעי תלמוד",
}


def _is_hebrew(lang: Optional[str]) -> bool:
    return str(lang or "").lower().startswith("he")


def labels(lang: str) -> Dict[str, str]:
    """The legend's frame (title, headings, column names) in ``lang``."""
    return _LABELS_HE if _is_hebrew(lang) else _LABELS_EN


def legend_html(lang: str) -> str:
    """The whole legend as one HTML fragment, for both apps: two headed tables.

    Plain tables only, so Qt's rich-text subset renders it the way a browser
    does. Every string is escaped: the marks are `<`, `&`, `$`. Qt ignores the
    direction of a wrapping `<div>`, so every block carries its own `dir` and
    `align`. Qt also never mirrors a table's columns, whatever its `dir`, so the
    tables are laid out left to right in both languages and a Hebrew row lists
    its cells in reverse: the mark still ends up on the right.
    """
    lab = labels(lang)
    direction = "rtl" if _is_hebrew(lang) else "ltr"
    align = "right" if direction == "rtl" else "left"
    block = f"dir='{direction}' align='{align}'"
    esc = _html.escape
    cell = "padding: 3px 10px; border-bottom: 1px solid #ddd;"
    mark_cell = (f"{cell} font-family: Consolas, 'Courier New', monospace; "
                 "white-space: nowrap; direction: ltr; unicode-bidi: embed;")

    def tr_(mark_cell_html: str, meaning_cell_html: str) -> str:
        cells = (mark_cell_html, meaning_cell_html)
        return "<tr>" + "".join(reversed(cells) if direction == "rtl" else cells) + "</tr>"

    def table(marks: Tuple[Mark, ...]) -> str:
        # Bold <td>, not <th>: Qt aligns a <th> on its own terms, off its column.
        head = tr_(f"<td align='{align}' style='{cell}'><b>{esc(lab['col_mark'])}</b></td>",
                   f"<td {block} style='{cell}'><b>{esc(lab['col_meaning'])}</b></td>")
        rows = "".join(
            tr_(f"<td style=\"{mark_cell}\" dir='ltr' align='{align}'>{esc(m.form)}</td>",
                f"<td {block} style='{cell}'>{esc(m.meaning(lang))}</td>")
            for m in marks)
        return (f"<table dir='ltr' width='100%' cellspacing='0' "
                f"style='border-collapse: collapse; width: 100%;'>{head}{rows}</table>")

    def heading(text: str) -> str:
        return f"<h4 {block}>{esc(text)}</h4>"

    return (
        f"<div dir='{direction}' style='text-align: {align};'>"
        f"{heading(lab['prayer_heading'])}"
        f"{table(PRAYER_MARKS)}"
        f"{heading(lab['talmud_heading'])}"
        f"{table(TALMUD_MARKS)}"
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
    # The per-row credits decide. The legacy `source_credit` is only a fallback
    # for a sidecar without them: in the 2026-06 sidecar it aggregates every
    # team that touched the shelfmark, so row 38 (Yad Harav Herzog) names
    # Genuzot there too (Codex round 1, 2026-10-27).
    credits = [source.get("source_credit_en"), source.get("source_credit_he")]
    if not any(credits):
        credits = [source.get("source_credit")]
    return any(marker in (credit or "")
               for credit in credits for marker in _GENUZOT_CREDIT_MARKERS)
