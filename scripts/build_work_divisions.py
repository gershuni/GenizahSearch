# -*- coding: utf-8 -*-
"""Build the per-work unit tables that give an identification a place.

An identification names a work and stops. `shared/discovery_locus.py` turns a
stored work-side offset into a citation, but only if it is handed the work's
address vocabulary as ascending start offsets in the SAME normalized stream the
offsets were measured in. Producing that vocabulary is this script's whole job.

Four families, four mechanisms, one output shape:

  canonical headers   `##division, פרק N, ...##` line markers          (M-source)
  inline daf markers  `<דף X, עמ' Y>` inside the text                  (M-source)
  Judeo-Arabic        `+kind~ +numeral~` line markers                  (M-source/JA)
  staged versemaps    a sidecar JSON indexing the body by chapter:verse

FAIL CLOSED, PER WORK. A work whose stream does not re-derive byte-for-byte from
its source gets NO units at all. It is not close enough, and a work that is
mis-derived by even one character has every one of its offsets shifted, so every
citation it produces would be confidently wrong. Skipped works are counted and
named in the coverage report rather than passed over quietly.

ONE DIVISION AUTHORITY, NAMED. Every label emitted here is the indexed edition's
OWN division label, copied verbatim. Nothing is recomputed, renumbered, or mapped
onto another edition's system, and no such mapping is implied by the output.

That is a commitment rather than an implementation detail, because the systems
genuinely disagree. Yerushalmi halakhah numbering differs between editions; so do
paragraph numbering in the midrashim and siman numbering in the codes. A citation
built by mixing two authorities, or by silently renumbering into a third, would be
wrong in a way no gate here could detect -- every offset would be right and the
address would still send a reader to the wrong place. So the rule is: whatever the
source header says IS the address, and where two editions of one work exist they
are two works with two unit tables, never one reconciled table.

Worth knowing what that commits us to for the Yerushalmi specifically: the indexed
edition labels its sub-unit `משנה`, not `הלכה`, so its citations read
`פרק ח, משנה א`. That is the edition indexing by the mishnah a sugya comments on,
which is both self-consistent and more stable across editions than a halakhah
count -- but it is still ONE edition's system, and the surface should say so
rather than let a reader assume their own volume agrees.

MASKING. No source path appears here. The reference corpora are restricted, so
their directories arrive through the environment (see `--help`) exactly as the
masking pattern file does. A canonical work's SECTION LABEL is a universal
citation and is emitted; the source-manuscript provenance field carried in the
same header is cut at the first `|` and never reaches the output. Note the tension
this creates with the paragraph above: the label is publishable, the edition it
came from is not, so a reader cannot check the address against a named volume.
Carrying the printed folio alongside is the mitigation -- a folio is shared across
editions where an internal numbering is not.

Usage:
    python -X utf8 -u scripts/build_work_divisions.py --out _tmp/locus
    python -X utf8 -u scripts/build_work_divisions.py --out _tmp/locus --family ja
"""
from __future__ import annotations

import argparse
import collections
import functools
import hashlib
import html
import json
import os
import pickle
import re
import sqlite3
import sys
import time
from typing import Dict, Iterable, List, NamedTuple, Optional, Sequence, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.discovery_locus import (  # noqa: E402
    amud_ordinal,
    citation_seq_for_daf,
    daf_label_he,
    heb_numeral,
    norm_stream,
    parse_canonical_header,
    parse_unit_numeral,
    sefaria_daf,
    stream_offset_for_raw,
)

# --------------------------------------------------------------------------
# Inputs. Every one is a restricted or gitignored location, so every one is an
# environment variable -- the same posture as MASKING_SCAN_PATTERNS_FILE.
# --------------------------------------------------------------------------
ENV_REF_PKL = "GENIZAH_REF_CORPUS_PKL"
ENV_MSOURCE_DIR = "GENIZAH_MSOURCE_DIR"
ENV_JA_DIR = "GENIZAH_JA_DIR"
ENV_STAGING_DIR = "GENIZAH_REFS_STAGING_DIR"
ENV_CROSSWALK = "GENIZAH_REF_CROSSWALK"

#: Windows long-path escape: some source filenames push past MAX_PATH.
_LONG = "\\\\?\\"

#: `##...##`, never spanning `##` or a newline. This is the pipeline's own form;
#: a looser `##[^#]*##` exists elsewhere in the research tree and is NOT the same
#: regex -- on one work it deletes 30,677 letters the stream is supposed to keep.
HEADER_RE = re.compile(r"##(?:[^#\n]|#(?!#))*##")
_HEADER_LINE_RE = re.compile(r"^##(.*?)##\s*$")
_DAF_RE = re.compile(r"<דף ([^,>]{1,12}), עמ' ([^>]{1,6})>")
_YTEXT_RE = re.compile(r"Ytext(\d+)")
#: `+kind~` / `+kind~ +numeral~` at the head of a Judeo-Arabic line.
#
# The terminator is `~`, NOT whitespace. Stopping at the first space truncates every
# multi-word heading, and the damage is invisible without an outside reference:
# `+1. [פ, מ, לא, לו, ליא]~` became `1. פ`, which still looks like a plausible
# label. Checked against the publisher's own partition tree, that one work has 314
# divisions under both readings -- identical boundaries, 313 of 314 labels wrong.
_JA_MARKER_RE = re.compile(r"\+([^~\n]*)~")
#: The verse-analogue tier: too fine to cite, 76.3% of all JA markers.
JA_LEAF_KINDS = frozenset({"פסוק", "פס'", "משנה"})
#: How many levels of the enclosing chain a citation may carry.
_JA_MAX_DEPTH = 3
#: Editorial delimiters round a heading -- `{הקדמה}`, `<שופטים>`, some of them
#: arriving HTML-escaped. They mark the heading, they are not part of its name.
#:
#: SQUARE brackets are deliberately NOT in this set. They are not a delimiter: they
#: enclose the manuscripts that witness a section, `1. [פ, מ, לא, לו, ליא]`, and the
#: publisher's own tree renders them. Stripping them lost the distinction between a
#: witness list and the section number it qualifies. They are also safe to print --
#: the surface's interval scanner rejects a bracketed pair of DECIMALS, and a list
#: of Hebrew sigla is not one.
_MARKUP_CHARS = str.maketrans({c: None for c in "{}<>"})


def _clean_marker_text(text: str) -> str:
    """Strip the editorial delimiters a heading arrives wrapped in.

    Entities are unescaped FIRST: some headings carry `&lt;…&gt;` rather than the
    literal characters, and a citation reading `&lt;שופטים&gt;` is worse than one
    reading `<שופטים>` -- it exposes the transport encoding to a reader.
    """
    cleaned = html.unescape(text).translate(_MARKUP_CHARS)
    return cleaned.strip().strip(",:;.").strip()


class Unit(NamedTuple):
    """One citable division of one work."""

    unit_ord: int              #: position in the table; ASCENDING in stream offset
    start: int                 #: offset into the work's normalized stream
    part_key: str              #: stable machine key -- never rendered, never Hebrew
    label_he: str              #: what a reader sees
    citation_pos: Optional[int]  #: position in the work's OWN citation order, or None


class WorkUnits(NamedTuple):
    ref_id: str
    family: str
    grain: str
    units: List[Unit]
    stream_len: int


# --------------------------------------------------------------------------
# Shared helpers
# --------------------------------------------------------------------------

def _read(path: str) -> str:
    """Read a source file, surviving the ones that push past Windows MAX_PATH.

    The `\\\\?\\` escape only works on a fully normalized absolute path with
    backslashes -- handed a forward slash it fails to open a file that is plainly
    there -- so normalize first and apply the prefix only where it is needed.
    """
    if os.name == "nt":
        native = os.path.abspath(path)
        if len(native) > 240 and not native.startswith("\\\\"):
            native = _LONG + native
        path = native
    return open(path, encoding="utf-8", errors="replace").read()


def _sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _dedupe_ascending(units: Iterable[Unit]) -> List[Unit]:
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


# --------------------------------------------------------------------------
# Family 1 + 2: M-source canonical headers, and the inline daf markers
# --------------------------------------------------------------------------

@functools.lru_cache(maxsize=4)
def _msource_files(msource_dir: str) -> Dict[str, str]:
    """Ytext number -> filename. EXACT number match, never a substring.

    `resolve_ref_file` in the research tree matches with `ref_key in filename`,
    which is wrong twice over: the `M:` prefix is in no filename, so a split child
    resolves to zero files, and 60 of the 8,233 Ytext numbers are a strict prefix
    of another, so a bare number can resolve to the wrong edition entirely.

    Cached because the directory holds 8,233 files and the standalone pass asks
    once per work: re-listing it each time makes the build quadratic in the corpus.
    """
    found: Dict[str, str] = {}
    for name in os.listdir(msource_dir):
        if not name.endswith(".txt"):
            continue
        match = _YTEXT_RE.search(name)
        if match:
            found.setdefault(match.group(1), name)
    return found


def _split_divisions(raw: str) -> List[Tuple[str, List[str], List[Optional[str]]]]:
    """A monolith -> [(division, payload lines, chapter label per line)].

    Reproduces the research tree's split EXACTLY -- payload-only, `>>` lines joined
    with a single space -- because that is the recipe the shipped child streams were
    built with, and a stream that differs by one character invalidates every offset.
    """
    order: List[str] = []
    payloads: Dict[str, List[str]] = {}
    chapters: Dict[str, List[Optional[str]]] = {}
    current: Optional[str] = None
    current_chapter: Optional[str] = None
    for line in raw.split("\n"):
        text = line.strip()
        header = _HEADER_LINE_RE.match(text)
        if header:
            inner = header.group(1)
            current = inner.split(",")[0].strip()
            if current not in payloads:
                payloads[current] = []
                chapters[current] = []
                order.append(current)
            address = parse_canonical_header(inner)
            current_chapter = address.chapter if address else None
            continue
        if text.startswith(">>"):
            payload = text[2:].strip()
            if not payload or current is None:
                continue
            payloads[current].append(payload)
            chapters[current].append(current_chapter)
    return [(d, payloads[d], chapters[d]) for d in order]


#: The finest header field a citation may use, per grain. `halakhah` addresses the
#: Yerushalmi the way it is actually cited; `chapter` is right for Mishnah and
#: Tosefta, whose chapters are already close to the size of a stored span.
_SUB_KIND_LABEL = {"משנה": "משנה", "הלכה": "הלכה", "פסוק": "פסוק"}


def _chapter_units(
    labels_in_order: Sequence[Tuple[Optional[str], int]], with_sub: bool = False
) -> List[Unit]:
    """[(address, stream offset)] -> one unit per RUN of the same address.

    Each entry's address is `(chapter, sub, sub_kind)` when `with_sub`, otherwise
    the chapter label alone.

    Collapsing runs is not tidying. The Yerushalmi interleaves a main-text segment
    and a variant segment under one address -- y.Berakhot carries 1,151 headers for
    9 chapters -- so without the collapse a work gets hundreds of duplicate-labelled
    units and its citations become unusable.
    """
    units: List[Unit] = []
    for label, offset in labels_in_order:
        if label is None:
            continue
        if with_sub:
            chapter, sub, sub_kind = label
            if not chapter:
                continue
            kind_word = _SUB_KIND_LABEL.get(sub_kind, sub_kind)
            rendered = f"פרק {chapter}, {kind_word} {sub}" if sub and kind_word else f"פרק {chapter}"
            chapter_value = parse_unit_numeral(chapter) or 0
            sub_value = parse_unit_numeral(sub) or 0 if sub else 0
            key = f"ch:{chapter_value}.{sub_value}"
            # Dense so that consecutive halakhot are successors: a chapter never
            # runs past 99 halakhot anywhere in this corpus (measured max 45).
            position = chapter_value * 100 + sub_value
        else:
            rendered = label
            position = parse_unit_numeral(label)
            key = f"ch:{position if position else label}"
        if units and units[-1].label_he == rendered:
            continue
        units.append(Unit(len(units), offset, key, rendered, position))
    return _dedupe_ascending(units)


def build_msource_children(
    monolith_ref_id: str, msource_dir: str, shipped: Dict[str, str]
) -> List[WorkUnits]:
    """Bible / Mishnah / Tosefta: one child work per division, chapter grain."""
    number = _YTEXT_RE.search(monolith_ref_id).group(1)
    name = _msource_files(msource_dir).get(number)
    if not name:
        return []
    raw = _read(os.path.join(msource_dir, name))
    out: List[WorkUnits] = []
    for index, (division, payload_lines, chapter_labels) in enumerate(_split_divisions(raw)):
        stream = norm_stream(" ".join(payload_lines))[0]
        if not stream:
            continue
        ref_id = f"{monolith_ref_id}_{index:02d}"
        if shipped.get(ref_id) != stream:
            continue                       # fail closed: not byte-exact, no units
        offset = 0
        marks: List[Tuple[Optional[str], int]] = []
        for payload, label in zip(payload_lines, chapter_labels):
            marks.append((label, offset))
            offset += len(norm_stream(payload)[0])
        units = _chapter_units(marks)
        if units:
            out.append(WorkUnits(ref_id, "msource_header", "chapter", units, len(stream)))
    return out


def build_msource_standalone(
    ref_id: str, msource_dir: str, shipped: Dict[str, str], prefer: str = "auto"
) -> Optional[WorkUnits]:
    """A per-tractate M-source file: chapter units from headers, daf units from markers.

    `prefer` picks the grain. "auto" applies what the measurements support:

      BAVLI -> daf+amud. Its mean chapter is 21,179 stream letters against a median
      stored span of 542, so a chapter citation names a block forty times the thing
      it is locating -- and daf+amud is the only address a reader of the Bavli uses.

      YERUSHALMI -> the header's chapter+halakhah. Its printed column is 2,945
      letters, so coarse that 90.8% of spans land inside one and the citation
      narrows nothing; the header address is 1,398 letters, which is the true
      analogue of a Bavli amud. The column is still recorded, to be shown beside it.
    """
    number_match = _YTEXT_RE.search(ref_id)
    if not number_match:
        return None
    name = _msource_files(msource_dir).get(number_match.group(1))
    if not name:
        return None
    raw = _read(os.path.join(msource_dir, name))
    stripped = HEADER_RE.sub(" ", raw)
    stream, offsets = norm_stream(stripped)
    if shipped.get(ref_id) != stream:
        return None                        # fail closed

    folios = [(m.start(), m.group(1).strip(), m.group(2).strip())
              for m in _DAF_RE.finditer(stripped)]
    columns = 4 if any(amud_ordinal(a) > 2 for _, _, a in folios) else 2
    # A four-column leaf is the Yerushalmi, which is cited by chapter and halakhah;
    # its printed column is 2,945 letters, so coarse that 90.8% of stored spans sit
    # inside one and the citation narrows nothing. Its header address is 1,398 --
    # the real analogue of a Bavli amud -- so the header carries the citation and
    # the column rides along beside it.
    with_sub = columns == 4
    header_units = _standalone_header_units(raw, with_sub=with_sub)

    grain = prefer
    if prefer == "auto":
        grain = "daf" if folios and columns == 2 else ("header" if header_units else "daf")
    if grain == "daf":
        units = _daf_units(folios, offsets, columns)
        if units:
            return WorkUnits(ref_id, "msource_daf", f"daf{columns}", units, len(stream))
    if header_units:
        return WorkUnits(ref_id, "msource_header",
                         "chapter_halakhah" if with_sub else "chapter",
                         header_units, len(stream))
    return None


def _standalone_header_units(raw: str, with_sub: bool = False) -> List[Unit]:
    """Header units for a whole-file work, offsets measured in the STRIPPED stream."""
    offset = 0
    marks: List[Tuple[Optional[object], int]] = []
    for line in raw.split("\n"):
        header = _HEADER_LINE_RE.match(line.strip())
        if header:
            address = parse_canonical_header(header.group(1))
            if address is None:
                marks.append((None, offset))
            elif with_sub:
                marks.append(((address.chapter, address.sub, address.sub_kind), offset))
            else:
                marks.append((address.chapter, offset))
            continue
        offset += len(norm_stream(line)[0])
    return _chapter_units(marks, with_sub=with_sub)


def _daf_units(
    folios: Sequence[Tuple[int, str, str]], offsets: Sequence[int], columns: int
) -> List[Unit]:
    units: List[Unit] = []
    for raw_pos, daf_label, amud_label in folios:
        daf, amud = parse_unit_numeral(daf_label), amud_ordinal(amud_label)
        start = stream_offset_for_raw(offsets, raw_pos)
        if daf is None or not 1 <= amud <= columns:
            # An unreadable folio still divides the text; it just cannot be cited.
            units.append(Unit(len(units), start, f"daf:?{len(units)}", "", None))
            continue
        units.append(Unit(len(units), start, f"daf:{daf}.{amud}",
                          daf_label_he(daf, amud), None))
    units = _dedupe_ascending(units)
    sequence = citation_seq_for_daf(
        [(parse_unit_numeral(u.label_he.split(" ")[0]) if u.label_he else None,
          amud_ordinal(u.label_he[-1]) if u.label_he else 0) for u in units],
        columns,
    )
    return [u._replace(citation_pos=p) for u, p in zip(units, sequence)]


# --------------------------------------------------------------------------
# Family 3: Judeo-Arabic
# --------------------------------------------------------------------------

def build_ja(path: str, ref_id: str, shipped: Dict[str, str]) -> Optional[WorkUnits]:
    """Judeo-Arabic: one unit per DIVISION marker line.

    THE MARKERS ARE NOT STRIPPED, and must not be. The ingest runs the normalizer
    over the whole raw file, preamble and markers included, so their letters are
    part of the stream every stored offset was measured against -- 140,754 letters,
    1.1% of the corpus. Removing them first changes the stream and invalidates every
    offset: measured, 89 of 89 documents rebuild byte-exactly with them and 0 of 89
    without. (That the marker letters are indexed AS CONTENT is a real hygiene
    defect, but it can only be fixed on a run that recomputes the offsets too.)

    The verse tier is excluded. It is 76.3% of all markers at a median 72 letters --
    finer than the median stored span -- so a flat table would cite 'verses 3 to 47'
    for an ordinary hit. Excluding it leaves a median unit of 552 letters, and 79.8%
    of real spans then land in exactly one.
    """
    raw = _read(path)
    stream, offsets = norm_stream(raw)
    if shipped.get(ref_id) != stream:
        return None                        # fail closed

    marked: List[Tuple[int, str, Optional[str]]] = []
    position = 0
    for line in raw.split("\n"):
        tokens = _JA_MARKER_RE.findall(line)
        if tokens and line.lstrip().startswith("+"):
            kind, numeral = _split_ja_heading(tokens)
            marked.append((position + len(line) - len(line.lstrip()), kind, numeral))
        position += len(line) + 1

    divisions = [(p, k, n) for p, k, n in marked if k not in JA_LEAF_KINDS]
    if len(divisions) < 3:
        divisions = marked                 # a document with no coarse tier is flat
    if not divisions:
        return None

    containers = _infer_containers(divisions)
    units: List[Unit] = []
    open_context: List[Tuple[str, str]] = []        # (kind, its rendered label)
    for raw_pos, kind, numeral in divisions:
        own = _clean_marker_text(f"{kind} {numeral}".strip() if numeral else kind)
        # Only a kind something is actually nested INSIDE stays open as context.
        while open_context and open_context[-1][0] == kind:
            open_context.pop()
        parts = [label for _, label in open_context[-(_JA_MAX_DEPTH - 1):]] + [own]
        units.append(Unit(len(units), stream_offset_for_raw(offsets, raw_pos),
                          f"ja:{len(units)}", ", ".join(p for p in parts if p),
                          len(units)))
        if kind in containers:
            open_context.append((kind, own))
    units = _dedupe_ascending(units)
    units = _disambiguate_labels(units)
    units = [u._replace(citation_pos=i) for i, u in enumerate(units)]
    return WorkUnits(ref_id, "ja", "division", units, len(stream))


def _split_ja_heading(tokens: Sequence[str]) -> Tuple[str, Optional[str]]:
    """The `+...~` tokens of one line -> (kind, numeral) for a whole heading.

    A heading is not two tokens. One line carries as many as it needs:

        +1.~ +[פ,~ +מ,~ +לא,~ +לו,~ +ליא]~

    is a single division whose label the publisher renders `1. [פ, מ, לא, לו, ליא]`
    -- a section number followed by the manuscripts that witness it. Reading only
    the first two tokens gave `1. פ`, which is not obviously wrong on the page and
    is wrong on 313 of that work's 314 divisions. It took the publisher's own tree
    to see it, which is the argument for having fetched the tree.

    A leading token that is itself a numeral means the section is bare-numbered and
    has no kind word; otherwise the first token is the kind and the rest qualify it.
    """
    if not tokens:
        return "", None
    head = tokens[0].strip()
    rest = " ".join(t.strip() for t in tokens[1:]).strip()
    if parse_unit_numeral(head.rstrip(".")) is not None:
        return "", (f"{head} {rest}".strip() if rest else head)
    return head, (rest or None)


def _infer_containers(divisions: Sequence[Tuple[int, str, Optional[str]]]) -> set:
    """Which marker kinds genuinely CONTAIN another kind, read off the numbering.

    A nested sequence restarts; a top-level one does not. That is the whole test,
    and it is a fact about the document rather than a guess about its vocabulary.

    Rarity is the guess it replaces, and rarity produces confidently WRONG output
    rather than merely vague output. Saadia on Job carries one `{הקדמה}` at the
    top, forty-two `פרק` running א to מב without a break, and three `{פתיחה}`
    sitting BETWEEN chapters. Ranked by rarity those nest, and chapter 35 is cited
    `הקדמה, פתיחה, פרק לה` -- a chain asserting that the chapter is inside a
    preface which is inside the introduction, when the three are siblings in a
    linear commentary and none contains any other. An ambiguous label is visibly
    unhelpful; a false containment claim reads as information.

    Under the numbering test that document yields no containers at all, which is
    correct, while the documents that really are nested still resolve: `פצל`
    restarts at every gate of Hovot ha-Levavot, `פרק` restarts at every discourse
    of Emunot ve-De'ot, and `פרק` restarts at each of the twelve book headings in
    Ibn Balaam -- headings that are a singleton kind apiece, which is exactly what
    a frequency rule cannot see. The parent need not be numbered; it need only be
    the thing the restart happens at.
    """
    positions: Dict[str, List[Tuple[int, Optional[int]]]] = collections.defaultdict(list)
    for index, (_, kind, numeral) in enumerate(divisions):
        positions[kind].append((index, parse_unit_numeral(numeral) if numeral else None))

    containers: set = set()
    for kind, entries in positions.items():
        numbered = [(i, v) for i, v in entries if v is not None]
        if len(numbered) < 4:
            continue                       # too short to tell a restart from noise
        restarts = [numbered[j][0] for j in range(1, len(numbered))
                    if numbered[j][1] <= numbered[j - 1][1]]
        if len(restarts) < 2:
            continue                       # monotonic: this kind is top level
        own_positions = [i for i, _ in entries]
        opened_by: List[str] = []
        for restart in restarts:
            # The gap is everything between the END of the previous block and this
            # restart. The container is the FIRST marker in it, not the nearest:
            # Hovot ha-Levavot opens each gate `אלבאב`, then `פתיחה`, then `פצל א`,
            # so the nearest heading to the restart is the preface -- a SIBLING of
            # the chapters -- while the gate is what actually opens the block.
            previous = max((p for p in own_positions if p < restart), default=-1)
            gap = [divisions[i][1] for i in range(previous + 1, restart)
                   if divisions[i][1] != kind]
            if gap:
                opened_by.append(gap[0])
        # MOST restarts must be opened by a heading, not all of them. Requiring all
        # was too strict to survive real text: one gate transition in Hovot
        # ha-Levavot has no heading between the blocks, and abandoning the kind on
        # that single anomaly discarded a hierarchy that is plainly there in the
        # other nine. A sequence with no explained restart at all is still refused,
        # which is what keeps a linear commentary from acquiring a false parent.
        if len(opened_by) * 2 >= len(restarts):
            containers |= set(opened_by)
    return containers


def _disambiguate_labels(units: Sequence[Unit]) -> List[Unit]:
    """Guarantee that no two units of one work render the same citation.

    The enclosing chain removes most collisions, but it cannot remove all of them:
    a document whose headings simply repeat, with nothing coarser between them, has
    no context to borrow. What is left gets its occurrence number, which is ugly and
    honest -- a citation that silently points at up to eleven indistinguishable
    places is neither.

    Parentheses are safe here: the surface's interval scanner rejects a bracketed
    DECIMAL PAIR, and a lone ordinal is not one.

    WHY A FALLBACK IS NEEDED AT ALL, stated plainly rather than tuned away: rarity
    is a proxy for depth and it sometimes inverts. Saadia's Emunot ve-De'ot has ten
    discourses and six prefaces, so the preface reads as the COARSER kind on counts
    while it actually sits inside a discourse -- and the book's own opening preface
    sits outside all of them, so no containment rule recovers the hierarchy either.
    That is a real irregularity in the text, not a gap in the heuristic. 15% of JA
    units land here. They are numbered rather than guessed at, and the occurrence
    numbers are deliberately visible in the scholar audit so the ruling on them is
    made by someone who can read the manuscript.
    """
    counts = collections.Counter(u.label_he for u in units)
    seen: Dict[str, int] = collections.Counter()
    out: List[Unit] = []
    for unit in units:
        if counts[unit.label_he] == 1:
            out.append(unit)
            continue
        seen[unit.label_he] += 1
        out.append(unit._replace(label_he=f"{unit.label_he} ({seen[unit.label_he]})"))
    return out


# --------------------------------------------------------------------------
# Family 4: staged versemaps
# --------------------------------------------------------------------------

#: Keys whose `chapter` is an amud index in the tractate's own foliation.
_DAF_KEY_RE = re.compile(r"^(?:sef|b2)_(?:tosafot|rabbeinu_chananel)_")
#: Keys whose `chapter` is an amud index in the RIF's separate foliation.
_RIF_KEY_RE = re.compile(r"^(?:sef|b2)_rif_")
_RIF_PREFIX = 'רי"ף'


def sefaria_render_kind(key: str, source_ref: str) -> str:
    """Which of the three things a versemap's integer `chapter` actually means.

    Classified by KEY, never by the book or tractate the title names. A suffix
    classifier reads a midrash's paragraph number as a biblical chapter and emits a
    'chapter 93' for a book with 50 -- the one overshoot an adversarial pass found
    in an otherwise clean 162-work oracle.
    """
    if _RIF_KEY_RE.match(key):
        return "daf_rif"
    if _DAF_KEY_RE.match(key) and not re.search(r"_(genesis|exodus|leviticus|numbers|"
                                                r"deuteronomy)$", key):
        return "daf_bavli"
    return "chapter"


def build_sefaria(
    key: str, staging_dir: str, body_file: str, versemap_file: Optional[str],
    ref_id: str, shipped: Dict[str, str], source_ref: str = "",
) -> Optional[WorkUnits]:
    """Staged works: the division lives in a sidecar JSON, not in the body.

    The sidecar indexes the RAW body; the stored offsets index the normalized
    stream. Bridging them is the bisect the ingest throws away, and skipping it is
    not a subtle error -- resolving a stored offset as a raw position puts 84% of
    rows in the wrong chapter.
    """
    if not versemap_file:
        return None
    body_path = os.path.join(staging_dir, body_file)
    if not os.path.exists(body_path):
        return None
    stream, offsets = norm_stream(_read(body_path))
    if shipped.get(ref_id) != stream:
        return None                        # fail closed

    sidecar = json.load(open(os.path.join(staging_dir, versemap_file), encoding="utf-8"))
    kind = sefaria_render_kind(key, source_ref or sidecar.get("source_ref", ""))
    records = sidecar.get("units") or []
    if records:
        return _sefaria_verse_units(ref_id, kind, records, offsets, len(stream))
    sections = sidecar.get("sections") or []
    if sections:
        return _sefaria_section_units(ref_id, sections, offsets, len(stream))
    return None


def _sefaria_verse_units(
    ref_id: str, kind: str, records: Sequence[dict], offsets: Sequence[int], stream_len: int
) -> Optional[WorkUnits]:
    """One unit per RUN of the same `chapter`; `verse` is the sub-field, not the grain.

    The sub-index is deliberately NOT rendered for the daf kinds. Only 25.8% of
    daf-family spans fall inside a single numbered comment, so a `2a §1` would be
    wrong or misleading for roughly three rows in four.
    """
    units: List[Unit] = []
    for record in records:
        chapter = record.get("chapter")
        if chapter is None:
            continue
        if units and units[-1].part_key.endswith(f":{chapter}"):
            continue
        start = stream_offset_for_raw(offsets, record.get("start", 0))
        if kind in ("daf_bavli", "daf_rif"):
            daf, amud = sefaria_daf(int(chapter))
            label = daf_label_he(daf, amud, _RIF_PREFIX if kind == "daf_rif" else "")
            position = daf * 2 + amud - 1
        else:
            label = heb_numeral(int(chapter)) if 1 <= int(chapter) <= 999 else str(chapter)
            position = int(chapter)
        units.append(Unit(len(units), start, f"{kind}:{chapter}", label, position))
    units = _dedupe_ascending(units)
    if not units:
        return None
    return WorkUnits(ref_id, "sefaria", kind, units, stream_len)


def _sefaria_section_units(
    ref_id: str, sections: Sequence[dict], offsets: Sequence[int], stream_len: int
) -> Optional[WorkUnits]:
    """Short liturgical texts, cited by section NAME rather than by number."""
    units: List[Unit] = []
    for index, section in enumerate(sections):
        label = (section.get("section_he") or "").strip()
        start = stream_offset_for_raw(offsets, section.get("start", 0))
        units.append(Unit(len(units), start, f"sec:{index}", label, index))
    units = _dedupe_ascending(units)
    if not units:
        return None
    return WorkUnits(ref_id, "sefaria", "section", units, stream_len)


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE locus_work (
  locus_ref_id  TEXT PRIMARY KEY,
  family        TEXT NOT NULL,
  grain         TEXT NOT NULL,
  stream_len    INTEGER NOT NULL,
  unit_count    INTEGER NOT NULL);

CREATE TABLE locus_unit (
  locus_ref_id  TEXT NOT NULL REFERENCES locus_work(locus_ref_id),
  unit_ord      INTEGER NOT NULL,
  start_offset  INTEGER NOT NULL,
  part_key      TEXT NOT NULL,
  label_he      TEXT NOT NULL,
  citation_pos  INTEGER,
  PRIMARY KEY (locus_ref_id, unit_ord));

-- part_key is NOT unique within a work, and that is a measured decision rather
-- than a slack constraint: 46 of 87 marker-bearing works visit the same folio
-- twice, so two units legitimately share one citation. The index keeps them
-- apart by unit_ord; the citation folds them together by part_key.
CREATE INDEX ix_locus_unit_part ON locus_unit(locus_ref_id, part_key);
"""


def write_artifact(path: str, works: Sequence[WorkUnits]) -> None:
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    conn.executescript(_SCHEMA)
    conn.executemany(
        "INSERT INTO locus_work VALUES (?,?,?,?,?)",
        [(w.ref_id, w.family, w.grain, w.stream_len, len(w.units)) for w in works],
    )
    conn.executemany(
        "INSERT INTO locus_unit VALUES (?,?,?,?,?,?)",
        [(w.ref_id, u.unit_ord, u.start, u.part_key, u.label_he, u.citation_pos)
         for w in works for u in w.units],
    )
    conn.commit()
    conn.close()


def check_invariants(works: Sequence[WorkUnits]) -> List[str]:
    """Structural gates. These are what a wrong table looks like from the outside."""
    problems: List[str] = []
    for work in works:
        starts = [u.start for u in work.units]
        if starts != sorted(starts):
            problems.append(f"{work.ref_id}: unit starts are not ascending")
        if len(set(starts)) != len(starts):
            problems.append(f"{work.ref_id}: duplicate start offsets")
        if any(u.start > work.stream_len for u in work.units):
            problems.append(f"{work.ref_id}: a unit starts past the end of the stream")
        if [u.unit_ord for u in work.units] != list(range(len(work.units))):
            problems.append(f"{work.ref_id}: unit ordinals are not 0..n-1")
        positions = [u.citation_pos for u in work.units if u.citation_pos is not None]
        if work.grain.startswith("daf") and len(set(positions)) < 2 and len(positions) > 2:
            problems.append(f"{work.ref_id}: every unit shares one citation position")

        # A citation must name ONE place. Two units may legitimately share a folio
        # -- the edition revisits it -- and those fold together at render time by
        # citation position. What is never acceptable is two units at DIFFERENT
        # citation positions rendering the same string, because then the reader is
        # handed one label pointing at several indistinguishable places.
        by_label: Dict[str, set] = collections.defaultdict(set)
        for unit in work.units:
            if unit.label_he:
                by_label[unit.label_he].add(unit.citation_pos)
        ambiguous = sorted(lab for lab, places in by_label.items() if len(places) > 1)
        if ambiguous:
            problems.append(
                f"{work.ref_id}: {len(ambiguous)} label(s) name more than one place, "
                f"e.g. {ambiguous[0]!r}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""Inputs arrive through the environment because the reference
corpora are restricted:
  {ENV_REF_PKL}      the reference-corpus pickle (the SHIPPED streams)
  {ENV_MSOURCE_DIR}    the M-source edition directory
  {ENV_JA_DIR}         the Judeo-Arabic per-document directory
  {ENV_STAGING_DIR}  the staged bodies + versemap sidecars
  {ENV_CROSSWALK}   crosswalk.json (optional; reference id -> work id)""")
    parser.add_argument("--out", required=True, help="output directory")
    parser.add_argument("--family", action="append",
                        choices=["msource", "daf", "ja", "sefaria"],
                        help="build only these families (default: all)")
    args = parser.parse_args()

    families = set(args.family or ["msource", "daf", "ja", "sefaria"])
    started = time.time()

    ref_pkl = os.environ.get(ENV_REF_PKL)
    if not ref_pkl or not os.path.exists(ref_pkl):
        parser.error(f"set {ENV_REF_PKL} to the reference-corpus pickle")
    print(f"pinning the reference corpus ... ", end="", flush=True)
    corpus_hash = _sha256(ref_pkl)
    print(corpus_hash[:16])
    shipped = {w["id"]: w["stream"] for w in pickle.load(open(ref_pkl, "rb"))}
    print(f"  {len(shipped):,} shipped streams  ({time.time() - started:.0f}s)", flush=True)

    works: List[WorkUnits] = []
    skipped: Dict[str, int] = collections.Counter()

    msource_dir = os.environ.get(ENV_MSOURCE_DIR)
    if msource_dir and os.path.isdir(msource_dir):
        if "msource" in families:
            for monolith in sorted({r.split("_")[0] for r in shipped
                                    if r.startswith("M:") and "_" in r}):
                built = build_msource_children(monolith, msource_dir, shipped)
                works.extend(built)
                print(f"  {monolith}: {len(built)} children", flush=True)
        if "daf" in families:
            standalone = sorted(r for r in shipped
                                if r.startswith("M:") and "_" not in r.split(":", 1)[1])
            print(f"  standalone M-source: {len(standalone):,} candidates ...", flush=True)
            built_count = 0
            for index, ref_id in enumerate(standalone, 1):
                built = build_msource_standalone(ref_id, msource_dir, shipped)
                if built:
                    works.append(built)
                    built_count += 1
                else:
                    skipped["msource_standalone"] += 1
                if index % 500 == 0:
                    print(f"    {index:,}/{len(standalone):,}  {built_count} with units "
                          f"({time.time() - started:.0f}s)", flush=True)
            print(f"  standalone M-source: {built_count} built, "
                  f"{skipped['msource_standalone']} without units", flush=True)
    elif families & {"msource", "daf"}:
        print(f"  SKIPPED: {ENV_MSOURCE_DIR} is not set or not a directory")

    ja_dir = os.environ.get(ENV_JA_DIR)
    if "ja" in families and ja_dir and os.path.isdir(ja_dir):
        names = sorted(f for f in os.listdir(ja_dir) if f.endswith(".txt"))
        for name in names:
            # The reference id is `J:` + the filename stem, which is what the ingest
            # minted. Numbering by position instead silently mismatches every work
            # and the whole family fails closed with nothing to point at.
            ref_id = "J:" + os.path.splitext(name)[0]
            built = build_ja(os.path.join(ja_dir, name), ref_id, shipped)
            if built:
                works.append(built)
            else:
                skipped["ja"] += 1
        print(f"  JA: {sum(1 for w in works if w.family == 'ja')} built, "
              f"{skipped['ja']} skipped", flush=True)
    elif "ja" in families:
        print(f"  SKIPPED: {ENV_JA_DIR} is not set or not a directory")

    staging = os.environ.get(ENV_STAGING_DIR)
    if "sefaria" in families and staging and os.path.isdir(staging):
        manifest_path = os.path.join(staging, "manifest.json")
        entries = (json.load(open(manifest_path, encoding="utf-8")).get("entries", [])
                   if os.path.exists(manifest_path) else [])
        for entry in entries:
            if entry.get("guard_only"):
                continue
            key = entry["key"]
            versemap = entry.get("versemap_file") or (
                f"{key}.versemap.json"
                if os.path.exists(os.path.join(staging, f"{key}.versemap.json")) else None)
            built = build_sefaria(key, staging, entry.get("body_file", f"{key}.txt"),
                                  versemap, f"REF2:{key}", shipped,
                                  entry.get("source_ref", ""))
            if built:
                works.append(built)
            else:
                skipped["sefaria"] += 1
        print(f"  staged: {sum(1 for w in works if w.family == 'sefaria')} built, "
              f"{skipped['sefaria']} skipped", flush=True)
    elif "sefaria" in families:
        print(f"  SKIPPED: {ENV_STAGING_DIR} is not set or not a directory")

    problems = check_invariants(works)
    os.makedirs(args.out, exist_ok=True)
    artifact = os.path.join(args.out, "work_divisions.db")
    write_artifact(artifact, works)

    by_family = collections.Counter(w.family for w in works)
    by_grain = collections.Counter(w.grain for w in works)
    coverage = {
        "reference_corpus_sha256": corpus_hash,
        "works_with_units": len(works),
        "units_total": sum(len(w.units) for w in works),
        "by_family": dict(by_family),
        "by_grain": dict(by_grain),
        "skipped_fail_closed": dict(skipped),
        "invariant_problems": problems,
        "seconds": round(time.time() - started, 1),
    }
    with open(os.path.join(args.out, "coverage.json"), "w", encoding="utf-8") as handle:
        json.dump(coverage, handle, ensure_ascii=False, indent=2)

    print()
    print(f"works with units : {len(works):,}")
    print(f"units            : {coverage['units_total']:,}")
    print(f"by family        : {dict(by_family)}")
    print(f"by grain         : {dict(by_grain)}")
    print(f"skipped closed   : {dict(skipped)}")
    print(f"artifact         : {artifact}")
    if problems:
        print()
        print(f"INVARIANT PROBLEMS: {len(problems)}")
        for problem in problems[:20]:
            print(f"  {problem}")
        return 1
    print("invariants       : clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
