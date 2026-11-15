# -*- coding: utf-8 -*-
"""The search text of a scholarly edition (editions search plan, private repository, §3.2).

An FGP or PGP edition is indexed under a derived text: the editor's apparatus is taken out
so the words read as the manuscript has them, and the stored edition is never touched. Two
promises hold for every rule:

* **Deletions and one-for-one replacements only.** A rule deletes characters or whole lines,
  or replaces one sign with one space. Nothing is inserted or reordered, so every character
  of the search text maps back to one character of the edition (``SearchText.to_original``):
  a match found in the search text is shown on the edition itself, signs and line numbers
  included.
* **No reading is made up.** Letters on either side of a sign are joined only when the sign
  marks something inside one reading (a supralinear letter, a doubtful letter, a correction's
  new text). Alternatives (``א/ב``) are never joined, and letters the edition marks as
  deleted inside a word leave the word as the scribe finally wrote it.

The sign meanings come from the FGP legends (``shared/fgp_marks_legend.py``) and the FGP text
pipeline rules (§10, a team's Word markings). Hebrew points and accents are never stripped or
reordered here; ``content_search`` folds them, as for the main index.

``CLEANER_VERSION`` changes whenever any rule changes: the editions index records it, and a
search text is only mapped back to an edition by the version that made it.
"""
from __future__ import annotations

import bisect
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import List, Optional, Sequence, Tuple

CLEANER_VERSION = 3   # 2: PGP side headers with a qualifier; 3: adjacent deleted lines (Codex, PR 1)

SOURCE_FGP = "fgp"
SOURCE_PGP = "pgp"

_HEB = "א-ת"
# A character that belongs to a written word for the purpose of "inside a word": Hebrew
# letters and points, square brackets (restorations), gershayim and geresh.
_WORDISH = "֐-׿\\[\\]'\"׳״"

# --- line rules -------------------------------------------------------------------------

#: The first line of an FGP Word file's catalogue block ("Cambridge, University Library, ...").
_LIBRARY_LINE_RE = re.compile(
    r"^\s*(Cambridge|Oxford|Bodleian|New York|Jewish Theological|JTS|Manchester|John Rylands|"
    r"London|British Library|Paris|Alliance|Philadelphia|Budapest|St\.? ?Petersburg|"
    r"Jerusalem|National Library|Washington|Vienna|Birmingham|Strasbourg|Geneva|Frankfurt|"
    r"Heidelberg|Kiev|Moscow|Westminster|Mosseri|Antonin|Firkovi)",
    re.IGNORECASE)
_HEADER_MAX_LINES = 6

#: A line that is only a folio or side label: ``1r``, ``2v``, ``Recto``, ``Verso:``,
#: ``(verso)``, ``צד א'``, ``ע"ב``, ``fol. 3v``.
_FOLIO_LINE_RE = re.compile(
    r"^\s*\(?\s*(?:"
    r"(?:fol\.?\s*)?\d{1,4}\s*[rvab]"
    r"|recto|verso|r|v"
    r"|צד\s+[אב]['׳]?(?:\s*\([^)\n]{0,20}\))?"
    r"|ע[\"״][אב]"
    r")\s*\)?\s*:?\s*$",
    re.IGNORECASE)

#: A PGP side header with a qualifier, in the forms ``document_service.parse_transcription_sections``
#: recognises: ``Recto - right margin``, ``Verso. Address.``, ``Verso (upside down)``,
#: ``Recto Margin``, ``verso, bottom margin``.
_SIDE_HEADER_LINE_RE = re.compile(
    r"^\s*\(?([Rr]ecto|[Vv]erso)\b"
    r"(?:[.\s]*(?:[-,]\s*[^\n]{0,40}|\([^\n)]{0,40}\)|[A-Z][a-z]*(?:\.[^\n]{0,30})?))?"
    r"\s*\)?\s*:?\s*$")

#: Editorial headings and notes that stand on a line of their own.
_HEADING_LINE_RE = re.compile(r"^\s*(?:ציטטות|לפני הגהה אחרונה)\s*:?\s*$")
_INLINE_NOTE_RE = re.compile(r"\[פסקה חדשה\]")

#: An editor's line number at the start of a line: ``01 ``, ``1. ``, ``(3) ``, ``12: ``.
_LINE_NUMBER_RE = re.compile(r"^[ \t]*\(?\d{1,3}[.):]?\)?(?=[ \t]|$)[ \t]*")
_NUMBERED_SHARE = 0.5
_NUMBERED_MIN_LINES = 3

# --- sign rules -------------------------------------------------------------------------

#: A correction pair, the old reading in (single or double) parentheses and the new one in
#: braces: ``א(נ){ס}א``, ``((ל)){י}``. The old reading is dropped, the new one kept.
_CORRECTION_PAIR_RE = re.compile(r"\(\(?([^(){}\n]{0,40})\)\)?\{([^{}\n]{0,40})\}")
#: An interlinear addition between ``\`` and ``/`` (a team's Word markings, rules §10).
_INTERLINEAR_PAIR_RE = re.compile(r"\\([^\\/\n]{1,40})/")
#: An editor's query mark.
_QUERY_RE = re.compile(r"\(\?\)")
#: Deleted text: FGP's ``(…)`` and ``((…))``, and ``⟦…⟧`` in every source.
_FGP_DELETED_RE = re.compile(r"\(\(?([^(){}\n]{1,40})\)\)?")
_STRUCK_RE = re.compile(r"⟦([^⟦⟧\n]{1,40})⟧")
#: Signs whose letters stay and whose marks go (supralinear, doubtful, corrections, margin,
#: headings, catchwords, fillers, PGP's expansions and line breaks inside a word, …).
_DROP_SIGNS = set("^?$@<>{}()|=#°*%&¿«»‹›¦⸤⸥§+\\ᵔ")
#: Signs that separate readings or words: replaced by a space, so nothing is joined.
_SPACE_SIGNS = set("/ᵕ·")
#: Control characters left by PDF extraction.
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")


@dataclass
class SearchText:
    """The search text of one edition, and the way back to the edition's characters."""

    text: str
    original: str
    rules: Counter = field(default_factory=Counter)
    _maps: List[Tuple[Tuple[int, ...], Tuple[int, ...], Tuple[int, ...]]] = field(default_factory=list)

    def to_original(self, index: int) -> int:
        """The edition's character index for search-text character *index*."""
        for out_starts, orig_starts, lengths in reversed(self._maps):
            k = bisect.bisect_right(out_starts, index) - 1
            if k < 0:
                raise IndexError(index)
            offset = index - out_starts[k]
            if offset >= lengths[k]:
                raise IndexError(index)
            index = orig_starts[k] + offset
        return index

    def originals(self) -> List[int]:
        """The edition's index of every search-text character, in order (increasing: no
        rule reorders)."""
        cached = getattr(self, "_originals_cache", None)
        if cached is None:
            cached = [self.to_original(i) for i in range(len(self.text))]
            self._originals_cache = cached
        return cached

    def span_from_original(self, start: int, end: int) -> Optional[Tuple[int, int]]:
        """The search-text span of the characters kept from the edition's ``[start, end)``,
        or None when the search text kept none of them."""
        origs = self.originals()
        s = bisect.bisect_left(origs, start)
        e = bisect.bisect_left(origs, end)
        return (s, e) if e > s else None

    def span_to_original(self, start: int, end: int) -> Tuple[int, int]:
        """The edition's span for the search-text span ``[start, end)`` (end > start):
        from the first matched character to just after the last, everything between
        included (the signs and numbers the search text left out)."""
        if end <= start:
            raise ValueError("empty span")
        return self.to_original(start), self.to_original(end - 1) + 1


class _Editor:
    """Applies one stage's edits and keeps the map from the new text to the old."""

    def __init__(self, text: str):
        self.text = text
        self.maps: List[Tuple[Tuple[int, ...], Tuple[int, ...], Tuple[int, ...]]] = []

    def apply(self, edits: Sequence[Tuple[int, int, str]]) -> None:
        """*edits*: ``(start, end, replacement)`` on the current text, replacement ``''``
        (delete) or ``' '`` (one character replaced by a space). Overlapping edits keep the
        first."""
        if not edits:
            return
        text = self.text
        parts: List[str] = []
        out_starts: List[int] = []
        orig_starts: List[int] = []
        lengths: List[int] = []
        pos = 0
        out = 0

        def keep(a: int, b: int) -> None:
            nonlocal out
            if b > a:
                parts.append(text[a:b])
                out_starts.append(out)
                orig_starts.append(a)
                lengths.append(b - a)
                out += b - a

        for start, end, repl in sorted(edits):
            if start < pos:
                continue
            if repl not in ("", " ") or (repl == " " and end - start != 1):
                raise ValueError("an edit deletes, or replaces one character by a space")
            keep(pos, start)
            if repl:
                parts.append(repl)
                out_starts.append(out)
                orig_starts.append(start)
                lengths.append(1)
                out += 1
            pos = end
        keep(pos, len(text))
        self.text = "".join(parts)
        self.maps.append((tuple(out_starts), tuple(orig_starts), tuple(lengths)))


def _line_spans(text: str) -> List[Tuple[int, int]]:
    """``(start, end)`` of every line, ``end`` excluding the newline."""
    spans = []
    start = 0
    for m in re.finditer("\n", text):
        spans.append((start, m.start()))
        start = m.end()
    spans.append((start, len(text)))
    return spans


def _delete_lines(text: str, spans: Sequence[Tuple[int, int]], indices) -> List[Tuple[int, int, str]]:
    """Edits that remove the lines *indices* (of *spans*) with their newlines.

    Adjacent lines go as ONE span: a line's own span would reach back over the newline the
    line before it already removes when the run ends the text, and the editor keeps only
    the first of two overlapping edits (Codex review of PR 1: ``foo\\nRecto\\nVerso`` kept
    ``Verso``). A run that ends the text takes the newline before it instead of after."""
    edits: List[Tuple[int, int, str]] = []
    run: List[int] = []
    for i in sorted(set(indices)) + [None]:
        if run and (i is None or i != run[-1] + 1):
            first, last = run[0], run[-1]
            if last + 1 < len(spans):
                edits.append((spans[first][0], spans[last + 1][0], ""))
            elif first > 0:
                edits.append((spans[first][0] - 1, len(text), ""))
            else:
                edits.append((0, len(text), ""))
            run = []
        if i is not None:
            run.append(i)
    return edits


def _has_hebrew(s: str) -> bool:
    return re.search(f"[{_HEB}]", s) is not None


def _inside_word(text: str, start: int, end: int) -> bool:
    """True when the span ``[start, end)`` is glued to a word on either side."""
    before = text[start - 1] if start > 0 else ""
    after = text[end] if end < len(text) else ""
    return bool(re.match(f"[{_WORDISH}]", before or " ")) or bool(re.match(f"[{_WORDISH}]", after or " "))


# --- the stages -------------------------------------------------------------------------

def _stage_characters(ed: _Editor, rules: Counter) -> None:
    edits = []
    for m in re.finditer("\r", ed.text):
        edits.append((m.start(), m.end(), ""))
    for m in re.finditer("\t", ed.text):
        edits.append((m.start(), m.end(), " "))
    for m in _CONTROL_RE.finditer(ed.text):
        if m.group(0) not in "\t\n\r":
            edits.append((m.start(), m.end(), ""))
            rules["control_characters"] += 1
    ed.apply(edits)


def _stage_lines(ed: _Editor, source: str, rules: Counter) -> None:
    text = ed.text
    spans = _line_spans(text)
    edits = []
    dropped = set()
    # The catalogue block of an FGP Word file: its first line names a library, and the
    # block runs to the first blank line within six lines.
    if source == SOURCE_FGP:
        first = next((i for i, (a, b) in enumerate(spans) if text[a:b].strip()), None)
        if first is not None and _LIBRARY_LINE_RE.match(text[spans[first][0]:spans[first][1]]):
            blank = next((i for i in range(first + 1, min(len(spans), first + 1 + _HEADER_MAX_LINES))
                          if not text[spans[i][0]:spans[i][1]].strip()), None)
            last = blank if blank is not None else first
            for i in range(first, last + 1):
                dropped.add(i)
            rules["header_block"] += 1
    for i, (a, b) in enumerate(spans):
        if i in dropped:
            continue
        line = text[a:b]
        if _FOLIO_LINE_RE.match(line) or (_SIDE_HEADER_LINE_RE.match(line) and not _has_hebrew(line)):
            dropped.add(i)
            rules["folio_lines"] += 1
        elif _HEADING_LINE_RE.match(line):
            dropped.add(i)
            rules["heading_lines"] += 1
    # The editor's line numbers, in texts that number most of their lines.
    content_lines = [i for i, (a, b) in enumerate(spans) if i not in dropped and text[a:b].strip()]
    numbered = [i for i in content_lines if _LINE_NUMBER_RE.match(text[spans[i][0]:spans[i][1]])]
    if len(content_lines) >= _NUMBERED_MIN_LINES and len(numbered) >= _NUMBERED_SHARE * len(content_lines):
        rules["numbered_texts"] += 1
        for i in numbered:
            a, b = spans[i]
            m = _LINE_NUMBER_RE.match(text[a:b])
            if not text[a + m.end():b].strip():
                dropped.add(i)          # a line that is only its number
            else:
                edits.append((a, a + m.end(), ""))
            rules["line_numbers"] += 1
    edits += _delete_lines(text, spans, dropped)
    for m in _INLINE_NOTE_RE.finditer(text):
        edits.append((m.start(), m.end(), ""))
        rules["inline_notes"] += 1
    ed.apply(edits)


def _stage_paired_signs(ed: _Editor, genuzot_marks: bool, rules: Counter) -> None:
    text = ed.text
    edits = []
    if genuzot_marks:
        for m in _CORRECTION_PAIR_RE.finditer(text):
            # Keep the new reading: delete "(old){" or "((old)){", and the closing brace.
            edits.append((m.start(), m.start(2), ""))
            edits.append((m.end() - 1, m.end(), ""))
            rules["correction_pairs"] += 1
    for m in _INTERLINEAR_PAIR_RE.finditer(text):
        edits.append((m.start(), m.start() + 1, ""))
        edits.append((m.end() - 1, m.end(), ""))
        rules["interlinear_pairs"] += 1
    for m in _QUERY_RE.finditer(text):
        edits.append((m.start(), m.start() + 1, " "))
        edits.append((m.start() + 1, m.end(), ""))
        rules["query_marks"] += 1
    ed.apply(edits)

    # Deleted letters: kept when they stand as words of their own (so they can be found),
    # dropped inside a word (the word reads as the scribe finally wrote it). ``⟦…⟧`` means
    # deleted in every source; parentheses only in the Genuzot team's marks (both its
    # lists), since other editors use them for restorations and expansions.
    text = ed.text
    edits = []
    patterns = [_STRUCK_RE] + ([_FGP_DELETED_RE] if genuzot_marks else [])
    for pattern in patterns:
        for m in pattern.finditer(text):
            if not _has_hebrew(m.group(1)):
                continue
            if _inside_word(text, m.start(), m.end()):
                edits.append((m.start(), m.end(), ""))
                rules["deleted_in_word"] += 1
            else:
                edits.append((m.start(), m.start(1), ""))
                edits.append((m.end(1), m.end(), ""))
                rules["deleted_words"] += 1
    ed.apply(edits)


_SIGN_RE = re.compile(
    "(?P<drop>[" + re.escape("".join(sorted(_DROP_SIGNS))) + "])"
    "|(?P<space>[" + re.escape("".join(sorted(_SPACE_SIGNS | {"⟦", "⟧"}))) + "]"
    f"|(?<=[{_HEB}])[:-](?=[{_HEB}]))")


def _stage_signs(ed: _Editor, rules: Counter) -> None:
    edits = []
    for m in _SIGN_RE.finditer(ed.text):
        if m.group("drop"):
            edits.append((m.start(), m.end(), ""))
            rules["dropped_signs"] += 1
        else:
            edits.append((m.start(), m.end(), " "))
            rules["spaced_signs"] += 1
    ed.apply(edits)


def _stage_empty_lines(ed: _Editor, rules: Counter) -> None:
    text = ed.text
    spans = _line_spans(text)
    ed.apply(_delete_lines(text, spans, [i for i, (a, b) in enumerate(spans) if not text[a:b].strip()]))


def search_text(original: Optional[str], source: str, *, genuzot_marks: bool = False) -> SearchText:
    """The search text of *original*, an edition from *source* (``"fgp"`` or ``"pgp"``),
    already decoded the way the chooser shows it (``decode_stored_entities`` for FGP).

    *genuzot_marks*: the text is the Genuzot team's (``fgp_marks_legend.uses_genuzot_marks``),
    whose parentheses mark deleted letters and whose ``(old){new}`` marks a correction."""
    if source not in (SOURCE_FGP, SOURCE_PGP):
        raise ValueError(f"unknown edition source {source!r}")
    if genuzot_marks and source != SOURCE_FGP:
        raise ValueError("the Genuzot marks belong to FGP texts")
    original = original or ""
    rules: Counter = Counter()
    ed = _Editor(original)
    _stage_characters(ed, rules)
    _stage_lines(ed, source, rules)
    _stage_paired_signs(ed, genuzot_marks, rules)
    _stage_signs(ed, rules)
    _stage_empty_lines(ed, rules)
    return SearchText(text=ed.text, original=original, rules=rules, _maps=ed.maps)
