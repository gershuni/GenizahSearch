# -*- coding: utf-8 -*-
"""Display-time collapse, deterministic lead attribution, and granularity
separation for the discovery panel / work page / findings page (Phase 136,
plan 136-07, PANEL-01/PANEL-02).

Three pure functions live here, each implementing ONE ratified owner
decision from `.planning/phases/136-read-surfaces-connections-panel-work-
witnesses/136-GATE1-DECISIONS.md` § A and `136-CONTEXT.md`:

- `collapse_canonical` -- D-13a: two claims recording the SAME
  `canonical_work_id` (but a different `work_id`, because dedup historically
  ran per `(page_id, work_id)` claim key) collapse to one displayed row, the
  canonical work's own title winning.
- `lead_attribution` -- D-13b: several works claiming a byte-identical span
  on one page nest under ONE lead attribution, chosen by the SAME
  deterministic total order `scripts.discovery_ids.select_display_evidence`
  already implements (band rank, then its existing tie-break lattice, whose
  FINAL key is the lexicographic `evidence_id`) -- reused verbatim, per the
  ratified decision, never a fresh tie-break.
- `separate_granularity` -- D-13d: for an identical-span group carrying >=2
  DIFFERENT canonical works, decides whether the group is the SAME work at
  two catalogued granularities (collapses like a D-13a duplicate) or
  genuinely different works sharing one passage (D-13e's "Also shares text
  with" bucket). Implements `works_related_by_title`, ported VERBATIM from
  `scripts/discovery_gate1_evidence.py` (the module the ratified rule was
  measured and validated against), per the decision record's own citation
  instruction.

This is a DISPLAY-TIME module: it never imports anything from `web/`, never
reads a table, and never reads the alignment's normalized edit-distance
field (the coverage-vs-match-quality trap documented in
`shared/discovery_main_pool.py`) -- grouping logic here operates purely on
already-materialized claim/evidence/work rows the caller supplies.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import scripts.discovery_ids as ids

# ---------------------------------------------------------------------------
# D-13a (136-CONTEXT.md, 136-GATE1-DECISIONS.md § A "D-13a"): collapse
# duplicate canonical works at display time.
# ---------------------------------------------------------------------------


def collapse_canonical(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Collapse `rows` that share the same `canonical_work_id` into ONE
    displayed row per canonical group, with the canonical work's own title
    winning -- the OTHER row(s) in the group are dropped from view entirely
    (never merged, never averaged): "the mockup's real page showed the SAME
    work twice under two titles... even though canonical_work_id... records
    the merge, because claims key on (page_id, work_id) and dedup runs per
    claim key. Owner decision: collapse by canonical_work_id, and the
    canonical work's own title wins."

    Each row in `rows` must carry at least `work_id` and `canonical_work_id`;
    every OTHER key on the surviving row is copied unchanged from whichever
    single row wins (never a mix of two different rows' fields). The
    returned list's order follows first-occurrence order of each
    `canonical_work_id` in `rows` (stable, deterministic over input order).

    When no member of a group IS the canonical anchor itself (`work_id ==
    canonical_work_id` -- a corpus corner case where the anchor work carries
    no claim on this page/manuscript), the group's lexicographically
    SMALLEST `work_id` wins instead, deterministically -- never "whichever
    row the caller happened to list first".
    """
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        canon = row["canonical_work_id"]
        groups.setdefault(canon, []).append(dict(row))

    collapsed: List[Dict[str, Any]] = []
    for canon, members in groups.items():
        anchor = next((m for m in members if m["work_id"] == canon), None)
        winner = anchor if anchor is not None else min(members, key=lambda m: m["work_id"])
        collapsed.append(winner)
    return collapsed


# ---------------------------------------------------------------------------
# D-13b (136-CONTEXT.md, 136-GATE1-DECISIONS.md § A "D-13b"): deterministic
# lead attribution for an identical-span group.
# ---------------------------------------------------------------------------


def lead_attribution(
    group: Sequence[Mapping[str, Any]]
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Deterministic lead attribution for an identical-span group -- several
    works' evidence rows claiming the SAME byte-identical span on one page.

    Returns `(lead, ordered_remainder)`: `lead` is the row
    `scripts.discovery_ids.select_display_evidence` would pick as this
    group's own display evidence (band rank ascending, then that function's
    existing full total order, whose FINAL tie-break is the lexicographic
    `evidence_id` -- "the exact tie-break already implemented at
    scripts/discovery_ids.py... No new tie-break concept is introduced; the
    rule is reused verbatim"). `ordered_remainder` is every OTHER row in the
    group, in that SAME total order (so "the same passage also matches..."
    always renders in a stable sequence).

    Implemented by applying `select_display_evidence` repeatedly over a
    shrinking candidate set, rather than reaching into its private sort key
    directly -- so the ordering is PROVABLY the same total order the
    existing selector already implements, never a parallel reimplementation
    of it.

    Each item in `group` must be evidence-row-shaped: at minimum
    `evidence_id`, `evidence_source`, `confidence_band` (the fields
    `select_display_evidence`'s sort key reads); `adjudication_status` /
    `routing_status` are optional and default exactly as they already do
    inside `scripts.discovery_ids`.
    """
    remaining = list(group)
    ordered: List[Dict[str, Any]] = []
    while remaining:
        winner_id = ids.select_display_evidence(remaining)
        winner = next(r for r in remaining if r.get("evidence_id") == winner_id)
        ordered.append(winner)
        remaining = [r for r in remaining if r is not winner]
    lead, remainder = ordered[0], ordered[1:]
    return lead, remainder


# ---------------------------------------------------------------------------
# D-13d (136-CONTEXT.md, 136-GATE1-DECISIONS.md § A "D-13d"): the
# granularity-separation predicate, ported VERBATIM from
# scripts/discovery_gate1_evidence.py (the module it was measured and
# validated against; 276/1,367 groups, 20.2%, collapse under this exact
# rule). Reads ONLY works.author and works.neutral_title -- the fields the
# ratified decision names -- never a heuristic invented here.
# ---------------------------------------------------------------------------

SAME_WORK_GRANULARITY = "same_work_granularity"
GENERIC_SHARED_TEXT = "generic_shared_text"
UNDECIDABLE = "undecidable"

_TITLE_PUNCT_RE = re.compile(r"[\"'׳״‘’“”]")
_WS_RE = re.compile(r"\s+")


def normalize_title(title) -> str:
    """NFC + strip quote/geresh/gershayim marks + collapse whitespace.

    Ported verbatim from `scripts/discovery_gate1_evidence.py`
    (`normalize_title`). Deliberately does NOT strip nikud/te'amim
    (`works.neutral_title` is plain text with no vocalization in the
    shipped asset) and does NOT touch the maqaf `־` (a real
    word-joining character, not punctuation to discard)."""
    if not title:
        return ""
    t = unicodedata.normalize("NFC", title)
    t = _TITLE_PUNCT_RE.sub("", t)
    t = _WS_RE.sub(" ", t).strip()
    return t


def titles_share_prefix(title_a: str, title_b: str, min_len: int = 4) -> bool:
    """True when the two (already-normalized) titles share a >= min_len
    leading-character-run prefix. Ported verbatim from
    `scripts/discovery_gate1_evidence.py` (`titles_share_prefix`) --
    deliberately crude (a literal prefix match, not tokenized morphology):
    "the 'concrete, testable' half of D-13d's proposed separation rule:
    cheap, auditable, and explicitly a DISPLAY heuristic, never a data fix."
    """
    if len(title_a) < min_len or len(title_b) < min_len:
        return False
    return title_a[:min_len] == title_b[:min_len]


def works_related_by_title(work_a: Mapping[str, Any], work_b: Mapping[str, Any]) -> bool:
    """D-13d's ratified separation predicate (136-GATE1-DECISIONS.md § A
    "D-13d", verbatim owner ruling): SAME non-null `author` field AND
    (identical normalized `neutral_title` -- an undetected alias -- OR a
    shared >= 4-character normalized-title prefix, e.g. a common "<author>
    on ..." commentary marker). Ported VERBATIM from
    `scripts/discovery_gate1_evidence.py` (`works_related_by_title`),
    intentionally conservative (author-gated) precisely because an
    ungated title-prefix match alone is corpus-noisy (many M-source
    responsa collections share one generic title -- e.g. "Responsa of the
    Geonim" -- across dozens of genuinely distinct items with no author
    recorded; gating on a matching author removes that entire noise class:
    the largest such author-gated cluster this rule deliberately does NOT
    collapse tops out at 43 members).

    ⚠ DORMANT GAP, flagged -- do NOT fix here (136-03's own investigation,
    136-GATE1-DECISIONS.md § A "D-13d", the paragraph beginning "What the
    owner's skip note is actually evidence of"): this exact predicate would
    ALSO collapse three genuinely distinct catalogued VOLUMES of one
    multi-volume opus -- `w000007` ("...כרך ב חלק ב"), `w000036` ("...כרך ט
    חלק ב"), `w000038` ("...כרך ט חלק א"), all authored by אברהם בן הרמב"ם
    and sharing the 4-character prefix "כרך " -- if any two of them ever
    claimed a byte-identical span. Confirmed (136-03, a corpus-wide query
    against the live asset) that this NEVER currently occurs -- the gap is
    dormant, not an active defect in the shipped 276-group collapse
    population. This function implements the RATIFIED rule EXACTLY as
    decided; it must NOT be changed to work around this flaw -- a future
    gate must rule on it explicitly first (per 136-GATE1-DECISIONS.md's own
    "not an authorized code change" framing).
    """
    author_a = work_a.get("author")
    author_b = work_b.get("author")
    if not author_a or not author_b or author_a != author_b:
        return False
    na = normalize_title(work_a.get("neutral_title"))
    nb = normalize_title(work_b.get("neutral_title"))
    if not na or not nb:
        return False
    if na == nb:
        return True
    return titles_share_prefix(na, nb, min_len=4)


def separate_granularity(group: Sequence[Mapping[str, Any]]) -> str:
    """For an identical-span group (>=2 works claiming a byte-identical
    span on one page, each item carrying at least `canonical_work_id`,
    `author`, `neutral_title`), decide whether the group is the SAME work
    recorded at two catalogued granularities (collapses like a D-13a
    duplicate and stays a standalone identification) or genuinely DIFFERENT
    works sharing one passage (D-13e's "Also shares text with" bucket,
    generic shared text).

    Returns one of three values:

    - `SAME_WORK_GRANULARITY` -- at least one pair of members with
      DIFFERENT `canonical_work_id` satisfies `works_related_by_title`.
    - `GENERIC_SHARED_TEXT` -- the group carries >=2 distinct
      `canonical_work_id` members and NO such pair satisfies the predicate.
    - `UNDECIDABLE` -- the group does not even carry >=2 distinct
      `canonical_work_id` members (this predicate's own precondition,
      D-13d, is about identical-span groups with >=2 DIFFERENT canonical
      works -- a group failing that precondition is a malformed/degenerate
      input this function was never asked to decide). The CALLER must map
      this conservatively to the SAME bucket as `GENERIC_SHARED_TEXT`
      ("A group the predicate cannot decide is classified conservatively as
      generic and is never silently promoted") -- this function itself
      never guesses at a collapse.
    """
    members = list(group)
    distinct_canon = {m["canonical_work_id"] for m in members}
    if len(distinct_canon) < 2:
        return UNDECIDABLE

    for i, work_a in enumerate(members):
        for work_b in members[i + 1:]:
            if work_a["canonical_work_id"] == work_b["canonical_work_id"]:
                continue
            if works_related_by_title(work_a, work_b):
                return SAME_WORK_GRANULARITY
    return GENERIC_SHARED_TEXT
