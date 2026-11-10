# -*- coding: utf-8 -*-
"""Per-work unit tables: the shapes, and the builders that need no source format.

A locus unit table lists a work's citable divisions as ascending start offsets in
the work's normalized letter stream (`shared/discovery_locus.norm_stream`), the
same coordinates the matcher's work-side offsets are measured in. This module
holds the parts of that job that do not depend on how any source file is laid
out:

  * the record shapes (`Unit`, `Edition`, `WorkUnits`);
  * `chapter_units`, which turns parsed addresses into units;
  * `build_staged_ja_chapters`, the chapter table of one staged open text.

The builders that read the reference corpora's own files are kept in the
private repository; they import these pieces from here rather than keeping a
copy of their own.
"""
from __future__ import annotations

import os
import re
from typing import Dict, Iterable, List, NamedTuple, Optional, Sequence, Tuple

from shared.discovery_locus import (
    LocusAddress,
    heb_numeral,
    norm_stream,
    parse_unit_numeral,
    stream_offset_for_raw,
)

#: Windows long-path escape: some source filenames push past MAX_PATH.
_LONG = "\\\\?\\"


class Unit(NamedTuple):
    """One citable division of one work.

    `source_address` is the address the source states, level by level and
    coarsest first, carried through unrendered. The other fields are derived;
    keeping the stated address beside them lets a check compare the two (two
    places that render one label must still state two addresses). It is not
    persisted: the artifact's schema carries only the derived fields.
    """

    unit_ord: int              #: position in the table; ASCENDING in stream offset
    start: int                 #: offset into the work's normalized stream
    part_key: str              #: stable machine key -- never rendered, never Hebrew
    label_he: str              #: what a reader sees
    citation_pos: Optional[int]  #: position in the work's OWN citation order, or None
    #: the levels of the address the SOURCE states, coarsest first
    source_address: Tuple[str, ...] = ()


class Edition(NamedTuple):
    """The published edition a page address belongs to, and the names the work
    goes by. `title_original` is the work's own name as the source records it
    (in Hebrew letters); it is not a translation."""

    title_he: str
    title_original: str        #: the work's original-language name; "" when none
    author_short: str          #: as the source abbreviates it
    author_full: str           #: as the source spells it out
    publisher: str
    publisher_city: str
    publisher_year: str
    editor: str
    edition: str


class WorkUnits(NamedTuple):
    ref_id: str
    family: str
    grain: str
    units: List[Unit]
    stream_len: int
    edition: Optional[Edition] = None


def read_source(path: str) -> str:
    """Read a source file, surviving the ones that push past Windows MAX_PATH.

    The `\\\\?\\` escape only works on a fully normalized absolute path with
    backslashes, so the path is normalized first and the prefix is applied only
    where it is needed.
    """
    if os.name == "nt":
        native = os.path.abspath(path)
        if len(native) > 240 and not native.startswith("\\\\"):
            native = _LONG + native
        path = native
    return open(path, encoding="utf-8", errors="replace").read()


def dedupe_ascending(units: Iterable[Unit]) -> List[Unit]:
    """Drop units that start where the previous one did, and renumber.

    Two markers can land on the same stream offset when nothing but punctuation
    separates them. A zero-width unit is not citable and would make `part_key`
    ambiguous, so the FIRST wins -- it is the one whose label opens the text.
    """
    out: List[Unit] = []
    for unit in units:
        if out and unit.start == out[-1].start:
            continue
        out.append(unit._replace(unit_ord=len(out)))
    return out


#: The word a sub-unit is displayed with. `משנה` is displayed as `הלכה`; only the
#: word changes -- the ordinal beside it, the unit's offset, its part key and its
#: citation position are the source's own. Read only when `chapter_units` is
#: called with `with_sub`.
SUB_KIND_LABEL = {"משנה": "הלכה", "הלכה": "הלכה", "פסוק": "פסוק"}

#: Numbering bases for the dense citation position. A chapter's sub-units stay
#: below SUB_BASE, and `parse_unit_numeral` accepts nothing above 999, so neither
#: base can be overrun. That is a coupling: widening `parse_unit_numeral` (or the
#: `heb_numeral` round trip it relies on) means widening these too. Both are wide
#: enough that a DIVISION boundary is never a successor, so a span that crosses
#: one renders as two pieces rather than as one range.
SUB_BASE = 100
DIVISION_BASE = 1000


def chapter_units(
    marks: Sequence[Tuple[Optional[LocusAddress], int]], with_sub: bool = False
) -> List[Unit]:
    """[(parsed address, stream offset)] -> one unit per RUN of the same address.

    Runs collapse because one address can be stated many times in a row; each
    repeat is the same place, not a new unit.

    The enclosing division is carried in the label exactly when the source
    states more than one division, so a chapter numeral that restarts under
    each division never names two places. Where the division is left out of the
    label it is still recorded in `source_address`.

    The counting word is always the source's own (`LocusAddress.chapter_kind`),
    and a range repeats it only once (`shorten_range_tail`).

    With `with_sub`, the sub-unit is rendered too, its word mapped through
    `SUB_KIND_LABEL`.
    """
    addressed = [(a, o) for a, o in marks if a is not None and a.chapter]
    stated = list(dict.fromkeys(a.division for a, _ in addressed if a.division))
    with_division = len(stated) > 1

    # One slot per numbering system, and the system is the division AND the word
    # it counts by: two counting words are two numberings, just as two divisions
    # are. The empty division gets a slot of its own, so text that opens a work
    # before its first named division never shares the first division's slot.
    systems = list(dict.fromkeys(
        (a.division, a.chapter_kind or "פרק") for a, _ in addressed))
    division_ord = {system: index for index, system in enumerate(systems)}

    units: List[Unit] = []
    for address, offset in addressed:
        chapter = address.chapter
        chapter_value = parse_unit_numeral(chapter)
        chapter_word = address.chapter_kind or "פרק"
        ordinal = division_ord[(address.division, chapter_word)]

        parts = [address.division] if with_division else []
        if with_sub:
            kind_word = SUB_KIND_LABEL.get(address.sub_kind, address.sub_kind)
            sub = address.sub
            sub_value = (parse_unit_numeral(sub) or 0) if sub else 0
            parts.append(f"{chapter_word} {chapter}"
                         + (f", {kind_word} {sub}" if sub and kind_word else ""))
            key = f"ch:{ordinal}.{chapter_value if chapter_value is not None else chapter}" \
                  f".{sub_value}"
            position = ((ordinal * DIVISION_BASE + (chapter_value or 0)) * SUB_BASE
                        + sub_value)
        else:
            sub_value = 0
            parts.append(f"{chapter_word} {chapter}")
            key = f"ch:{ordinal}.{chapter_value if chapter_value is not None else chapter}"
            # A chapter whose label is not a numeral has no place in the citation
            # ORDER, so it gets no position and can never merge with a neighbour.
            position = (None if chapter_value is None
                        else ordinal * DIVISION_BASE + chapter_value)
        rendered = ", ".join(p for p in parts if p)
        # The address the SOURCE stated, level by level -- never the rendered
        # string. The division, the sub-unit's kind and the chapter's counting
        # word are all levels of it: each one can tell apart two places that
        # render, or are positioned, alike.
        source = (
            address.division,
            str(chapter_value) if chapter_value is not None else chapter,
            str(sub_value) if with_sub and address.sub else "",
            address.sub_kind if with_sub else "",
            chapter_word,
        )
        # Collapse on the stated address, not on the rendered label: two
        # divisions whose adjacent chapters share a numeral would otherwise fold
        # into one unit wherever the label omits the division.
        if units and units[-1].source_address == source:
            continue
        units.append(Unit(len(units), offset, key, rendered, position, source))
    return dedupe_ascending(units)


# The staged Judeo-Arabic text of the Guide for the Perplexed marks its own
# chapters with `פצל # <numeral>` lines, numbered afresh in each of its three
# parts (76, 48 and 54 chapters). No other staged text uses this marker, so the
# builder is keyed to that one entry, and a chapter sequence that differs from
# the expected one fails the whole work closed rather than guessing.
STAGED_GUIDE_KEY = "ja2_rambam_moreh"
STAGED_GUIDE_PART_CHAPTERS = (76, 48, 54)
STAGED_JA_CHAPTER_RE = re.compile(r"^פצל\s*#\s*(\S+)\s*$", re.MULTILINE)


def build_staged_ja_chapters(
    key: str, staging_dir: str, body_file: str, ref_id: str,
    shipped: Dict[str, str],
) -> Optional[WorkUnits]:
    """Build the Guide's three-part chapter table from its own chapter lines."""
    if key != STAGED_GUIDE_KEY:
        return None
    body_path = os.path.join(staging_dir, body_file)
    if not os.path.exists(body_path):
        return None
    raw = read_source(body_path)
    stream, offsets = norm_stream(raw)
    if shipped.get(ref_id) != stream:
        return None

    markers = list(STAGED_JA_CHAPTER_RE.finditer(raw))
    if len(markers) != sum(STAGED_GUIDE_PART_CHAPTERS):
        return None

    marks: List[Tuple[Optional[LocusAddress], int]] = []
    cursor = 0
    for part, chapter_count in enumerate(STAGED_GUIDE_PART_CHAPTERS, 1):
        for chapter in range(1, chapter_count + 1):
            marker = markers[cursor]
            stated = parse_unit_numeral(marker.group(1))
            # The text twice writes sixteen as יו; the citation renders the
            # canonical טז, and no other mismatch is accepted.
            if stated is None and marker.group(1) == "יו":
                stated = 16
            if stated != chapter:
                return None
            marks.append((
                LocusAddress(
                    division=f"חלק {heb_numeral(part)}",
                    chapter=heb_numeral(chapter),
                    chapter_kind="פרק",
                    sub="",
                    sub_kind="",
                ),
                stream_offset_for_raw(offsets, marker.start()),
            ))
            cursor += 1

    chapters = chapter_units(marks)
    if len(chapters) != len(markers):
        return None
    # Text before Part I chapter 1 is a real introduction. Each numbered part
    # gets its own thousand-wide citation block and the introduction stays at 1,
    # so every part boundary (including introduction -> Part I) is a real break:
    # a span crossing it renders as two pieces, never as one synthetic range.
    units = [Unit(0, 0, "intro", "הקדמה", 1, ("הקדמה",))]
    units.extend(
        unit._replace(
            unit_ord=index,
            citation_pos=(unit.citation_pos + 1000
                          if unit.citation_pos is not None else None),
        )
        for index, unit in enumerate(chapters, 1)
    )
    return WorkUnits(ref_id, "ja", "chapter", units, len(stream))
