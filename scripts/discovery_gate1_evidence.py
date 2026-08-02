#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Gate-1 evidence measurement for Phase 136 plan 03 (PANEL-01, PANEL-02, NOVEL-01).

A READ-ONLY measurement script over the deployed discovery sidecar
(``discovery-v1-33499c5b...``, the LIVE asset — the trimmed rebuild has not
run yet). It computes every number the owner needs to rule on the five
"Still OPEN by design" wave-1 decisions named in
``136-CONTEXT.md`` (D-13e, D-16, D-13c, D-13b, D-13d), plus the zero-cost
novelty hard-case candidate set that feeds plan 136-03 Task 3 — originally
three D-23c classes (near-miss titles, alias pairs, granularity), extended
per an OWNER-AUTHORIZED gate-1 ruling (recorded in ``136-GATE1-DECISIONS.md``
item C) with two additional classes: terse/missing catalogue identification
text, and generic collection works. A SIXTH class -- catalogue divergence --
was added per owner decision E (``136-GATE1-DECISIONS.md`` item E, which also
amends the tri-state novelty flag into a seven-value shade enum): real
shipped claims where an available finding aid ties the SAME fragment to a
DIFFERENT work that is not a D-13d granularity variant -- the shade decision
E calls ``diverges``, with zero representation in Classes 1-5.

**Correction E′** (``136-GATE1-DECISIONS.md`` § E′, a same-day correction to
decision E, not a new ruling): decision E's ``refines_granularity`` shade
conflated two opposite directions of a granularity relationship into one
value. The shade enum now widens from SEVEN to EIGHT values by splitting it:
``refines_granularity`` (OUR claim is the finer one -- informative) versus
``aid_more_specific`` (the AID's identification is the finer one -- we add
nothing). ``aid_more_specific`` is the least-novel shade and is excluded from
the candidate toggle alongside ``confirms`` / ``refines_granularity`` /
``diverges`` / ``extends``; ``fills_gap`` remains the sole candidate
selector.

**Rulings F and G** (``136-GATE1-DECISIONS.md`` §§ F/G, a later continuation
after the owner read the actual 97 candidate cases): (F) ``diverges`` is
RETIRED and replaced by a SCOPE split -- ``diverges_work`` (a genuinely
different work) / ``diverges_part`` (a different or finer part of the SAME
work) -- widening the shade enum from EIGHT to NINE values, plus a NEW
sibling ``divergence_correctness`` question (``catalogue_correct`` /
``claim_correct`` / ``unclear``) recorded separately from the shade, because
the owner's own review of the real cases found the catalogue is often right
when it disagrees with a claim -- a fact this module must surface to the
user (hidden by default, explicit warning) without ever letting the system
itself adjudicate on the catalogue's say-so (the catalogue-never-evidence
discipline, applied to a new axis). (G) A boundary correction: when the
catalogue's STRUCTURED identification is generic but its own FREE TEXT
already states our specific identification in prose, the shade is
``confirms``, not ``refines_granularity`` -- with a systemic consequence
that a novelty check keying only on structured ids manufactures false
novelty at scale; this module's OWN Class-6 selector
(``select_catalogue_divergence_candidates``) demonstrates the failure
(over-fires on roughly half of its candidates per the owner's own
characterization) and is left UNCORRECTED here deliberately, per the
"measure it, do not quietly fix it away" instruction.

**Labelling restructure** (same continuation, owner ruling): Classes 1-3
(near-miss / alias / granularity) compare two of OUR OWN claims -- an A↔B
"is this the same work" identity judgment, not a claim-vs-aid novelty
judgment -- and were found, on reading the actual cases, to bake "same work"
into their own selection (same-author + common-title-stem is the selection
criterion itself), so full labelling of all 52 was REPLACED by an ~8-case
IDENTITY SPOT-CHECK per class (``same_work`` / ``different_works`` /
``unsure`` / ``skip``) that tests the constant-answer assumption rather than
building ground truth. Classes 4-6 (terse catalogue / generic collection /
catalogue divergence) are where the answer genuinely varies and carry the
NOVELTY SHADE question; they are EXPANDED from 45 to ~75 candidates
(20/25/30), proportioned to how genuinely hard/consequential each family is
(Class 6 -- the most consequential and, per ruling G, measurably flawed --
gets the largest expansion; Class 5's collection-level ambiguity is second;
Class 4's terse/absent text is the most mechanical of the three). Each row
now carries the ONE question type it was actually constructed to support
(identity XOR shade), fixing an incoherence in the pre-restructure worksheet
where Decision E's shade vocabulary was applied uniformly to all six classes
even though Classes 1-3 have no claim-vs-aid relationship to judge.

This module also emits an XLSX labelling workbook (``write_hardcases_xlsx``)
alongside the Markdown, per the owner's request for something easier to work
with than Hebrew RTL in Markdown; it now carries THREE sheets -- "Identity
Spot-Check", "Novelty Shades" (with its own Correctness column) and
"Vocabulary & Instructions".

Mirrors the shape of ``scripts/bench_discovery.py``: open the live asset
read-only, drive real queries against it, print a table per measurement, and
NEVER present a silent zero as a finding — every count this script reports
that should be nonzero is asserted nonzero, and the script exits 1 if any
such assertion fails.

Usage:
    python scripts/discovery_gate1_evidence.py <asset.db>
    python scripts/discovery_gate1_evidence.py <asset.db> --research-db <fullcorpus.db> \\
        --libraries-csv libraries.csv --evidence-out 136-GATE1-EVIDENCE.md \\
        --hardcases-out 136-NOVELTY-HARDCASES.md --hardcases-xlsx-out 136-NOVELTY-HARDCASES.xlsx

Determinism: every SQL query below is either naturally total (aggregate
COUNT) or carries an explicit ORDER BY over a stable key, and every Python
container built from query results is either a dict keyed by a stable id or
a list sorted by an explicit key before being sliced/rendered — re-running
this script against the same asset produces byte-identical Markdown output.

Masking discipline: this script renders real ``works.neutral_title`` /
``works.author`` values (already the shipped, DATA-04-cleared neutral
titles — the same strings the live product already displays) and real
public catalogue metadata from ``libraries.csv`` (shelfmark / non-placeholder
title — already public, displayed throughout the product). It never reads or
prints a raw M-source/J-source identifier, a restricted corpus name, or
reference text. The caller is expected to run
``scripts/check_atlas_masking.py --scan-asset`` over both written files
before presenting them to the owner (acceptance criteria, not enforced by
this script itself — this script has no import-time dependency on the
masking-gate module so it stays a lightweight read path, mirroring
``web/discovery_assets.py``'s own comment on why it does not import
``scripts.build_discovery_sidecar``).
"""

from __future__ import annotations

import argparse
import csv
import difflib
import os
import re
import sqlite3
import sys
import unicodedata
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PHASE_DIR = os.path.join(
    REPO_ROOT, ".planning", "phases",
    "136-read-surfaces-connections-panel-work-witnesses",
)
DEFAULT_RESEARCH_DB = os.path.join(REPO_ROOT, "same_work_spike", "probe", "data", "fullcorpus.db")
DEFAULT_LIBRARIES_CSV = os.path.join(REPO_ROOT, "libraries.csv")
DEFAULT_EVIDENCE_OUT = os.path.join(PHASE_DIR, "136-GATE1-EVIDENCE.md")
DEFAULT_HARDCASES_OUT = os.path.join(PHASE_DIR, "136-NOVELTY-HARDCASES.md")
DEFAULT_HARDCASES_XLSX_OUT = os.path.join(PHASE_DIR, "136-NOVELTY-HARDCASES.xlsx")

# ---------------------------------------------------------------------------
# Verdict vocabulary -- the SINGLE source of truth for every owner-facing
# shade token, shared verbatim by the Markdown worksheet, the XLSX dropdown
# validation and the XLSX vocabulary sheet, so the three surfaces can never
# drift out of sync. Order matches the owner's own E/E′/F shade table
# (decision E's six original shades, E′'s aid_more_specific inserted right
# after its sibling refines_granularity, F's diverges_work/diverges_part
# replacing the single diverges token, then not_checked's owner-facing alias
# `unsure` and the `skip` non-answer). ``not_checked`` itself is NOT
# owner-facing -- only its owner-facing alias `unsure` is offered as an
# answer token. THIS vocabulary is used ONLY on Class 4-6 ("shade") rows --
# see IDENTITY_VOCABULARY below for the separate Class 1-3 question.
# ---------------------------------------------------------------------------
SHADE_VOCABULARY: Tuple[Tuple[str, str], ...] = (
    ("confirms", "an aid already ties this fragment to this work"),
    (
        "refines_granularity",
        "OUR claim is MORE SPECIFIC (finer) than what an aid says -- e.g. the catalogue names the "
        "whole work, our claim names a specific book/chapter of it (the D-13d same-author/related-"
        "title rule); the OPPOSITE direction from `aid_more_specific` -- we ADD precision here. "
        "OWNER RULING G: if the aid's own FREE TEXT already states this identification in ANY form "
        "(even under a coarser structured work-id), the correct shade is `confirms`, not this one -- "
        "reserve `refines_granularity` for information the aid contains in NO form, structured or free.",
    ),
    (
        "aid_more_specific",
        "an AID names a MORE SPECIFIC (finer) variant of this fragment's work than our claim does "
        "-- e.g. the catalogue names a chapter/book, our claim names the whole work (the D-13d "
        "same-author/related-title rule); the OPPOSITE direction from `refines_granularity` -- we "
        "add NOTHING here, the aid already knew more (owner correction E′; the LEAST novel shade)",
    ),
    (
        "diverges_work",
        "an aid ties this fragment to a genuinely DIFFERENT WORK (not a granularity variant of ours) "
        "-- the aid and the claim contradict each other on WHICH WORK this is (owner ruling F, "
        "replacing the single `diverges` token). Owner: reading real cases, USUALLY the catalogue is "
        "right and this is OUR false positive -- but not always; record which side is correct in the "
        "separate Correctness column. Hidden by default on every surface, behind an explicit warned "
        "toggle -- never silently shown, never silently suppressed.",
    ),
    (
        "diverges_part",
        "an aid ties this fragment to a DIFFERENT OR FINER PART of the SAME work (owner ruling F -- "
        "\"more delicate and essentially less important\" than diverges_work) -- e.g. the aid names a "
        "specific chapter/section of the work while we name a different one, or the whole work, or "
        "vice versa. Same Correctness column and same hidden-by-default posture as diverges_work.",
    ),
    (
        "fills_gap",
        "the aids identify this fragment as nothing at all -- the genuine \"previously unknown\" case",
    ),
    (
        "extends",
        "aids tie OTHER folios of the SAME manuscript to this work, but not this specific folio",
    ),
    (
        "alias_merge",
        "the two work_ids shown ARE the same underlying work, not yet canonically merged (Class 2's "
        "situation)",
    ),
    (
        "unsure",
        "you cannot judge this case from the information shown -- maps to `not_checked`, costs "
        "nothing, is a real and useful answer",
    ),
    (
        "skip",
        "you choose not to judge this case at all -- recorded as skipped, NEVER filled from a draft "
        "`PROPOSAL`",
    ),
)
# The real SHADE tokens only (excludes the two non-shade answer tokens
# `unsure` / `skip`) -- this is the XLSX DataValidation list together with
# `unsure` / `skip` appended (ten tokens total): eight real shades that are
# owner-facing (the ninth stored value, `not_checked`, is a fail-closed
# system default never picked directly -- `unsure` is its owner-facing
# alias) plus the two non-shade answers.
SHADE_TOKENS: Tuple[str, ...] = tuple(tok for tok, _ in SHADE_VOCABULARY)

# ---------------------------------------------------------------------------
# Correctness vocabulary (owner ruling F, 136-GATE1-DECISIONS.md § F) -- a
# SEPARATE axis from the shade, applicable ONLY when the shade verdict is
# `diverges_work` or `diverges_part`. A divergence shade records only THAT
# the aid and the claim disagree (and at what scope); it cannot also record
# WHICH side is right, because the owner's own review of the real cases
# found BOTH directions occur under the identical shade. Blank is allowed
# (meaning "not applicable / not yet answered" -- the same convention as a
# blank Verdict cell) and is the correct answer for every non-divergence row.
# ---------------------------------------------------------------------------
CORRECTNESS_VOCABULARY: Tuple[Tuple[str, str], ...] = (
    (
        "catalogue_correct",
        "the catalogue/aid is right; our claim is the false positive -- owner ruling F: reading the "
        "real cases, this is the COMMON outcome",
    ),
    (
        "claim_correct",
        "our claim is right; the aid is wrong, thinner, or itself mistaken",
    ),
    (
        "unclear",
        "cannot tell which side is correct from the information shown",
    ),
)
CORRECTNESS_TOKENS: Tuple[str, ...] = tuple(tok for tok, _ in CORRECTNESS_VOCABULARY)

# ---------------------------------------------------------------------------
# Identity vocabulary (owner-authorized labelling restructure, same
# continuation as rulings F/G) -- the question Classes 1-3 (near-miss /
# alias / granularity) were ACTUALLY constructed to support: are the two
# claims/works shown (A and B) the same underlying work, or genuinely
# different works? This is NOT a novelty shade -- these rows compare two of
# OUR OWN claims against each other, never a claim against a finding aid.
# ---------------------------------------------------------------------------
IDENTITY_VOCABULARY: Tuple[Tuple[str, str], ...] = (
    (
        "same_work",
        "A and B are the same underlying work (or two parts/granularities of the same work)",
    ),
    (
        "different_works",
        "A and B are genuinely different works",
    ),
    (
        "unsure",
        "you cannot judge this pair from the information shown -- a real and useful answer",
    ),
    (
        "skip",
        "you choose not to judge this pair at all -- recorded as skipped",
    ),
)
IDENTITY_TOKENS: Tuple[str, ...] = tuple(tok for tok, _ in IDENTITY_VOCABULARY)

# ---------------------------------------------------------------------------
# D-13c / gate-4 page-coverage normalizer -- a faithful, standalone port of
# scripts/build_discovery_sidecar.py::norm_stream_letter_count /
# compute_page_coverage (itself a port of
# same_work_spike/probe/scripts/normalize.py::norm_stream). Re-implemented
# here rather than imported so this measurement script stays free of
# build_discovery_sidecar's masking-gate + argparse import surface (mirrors
# web/discovery_assets.py's stated reason for not importing that module).
# ---------------------------------------------------------------------------
_HEB_MIN, _HEB_MAX = 0x05D0, 0x05EA
_FINAL_FOLD = {
    0x05DA: chr(0x05DB),  # final kaf   -> kaf
    0x05DD: chr(0x05DE),  # final mem   -> mem
    0x05DF: chr(0x05E0),  # final nun   -> nun
    0x05E3: chr(0x05E4),  # final pe    -> pe
    0x05E5: chr(0x05E6),  # final tsadi -> tsadi
}


def norm_stream_letter_count(text: Optional[str]) -> int:
    """Space-free normalized Hebrew base-letter stream length (page_norm_letters)."""
    if not text:
        return 0
    n = 0
    for ch in unicodedata.normalize("NFC", text):
        folded = _FINAL_FOLD.get(ord(ch))
        code = ord(ch) if folded is None else ord(folded)
        if _HEB_MIN <= code <= _HEB_MAX:
            n += 1
    return n


def compute_page_coverage(matched_letters: Optional[int], page_norm_letters: Optional[int]) -> Optional[float]:
    if matched_letters is None:
        return None
    if not page_norm_letters:
        return 0.0
    return min(1.0, matched_letters / page_norm_letters)


# ---------------------------------------------------------------------------
# Band rank -- mirrors the ORDER of scripts/discovery_ids.py's
# _BAND_RANK_GROUPS (strongest first). Reproduced as a small local table
# (rather than reaching into that module's underscore-prefixed private
# lattice) so this script depends only on the PUBLIC discovery_ids surface
# it actually needs (none, at this granularity) and stays trivially
# auditable against the cited source.
# ---------------------------------------------------------------------------
_BAND_RANK_GROUPS: List[List[Tuple[str, str]]] = [
    [("track1_direct", "expert_verified"), ("track1_direct", "high_confidence_algorithmic")],
    [("track1_direct", "tier_a")],
    [("propagated", "corroborated")],
    [("track1_direct", "screening_rb")],
    [("track1_direct", "screening_canon")],
    [("propagated", "weak")],
    [("propagated", "not_evaluated")],
]
_BAND_RANK_INDEX: Dict[Tuple[str, str], int] = {
    pair: i for i, group in enumerate(_BAND_RANK_GROUPS) for pair in group
}
_UNRANKED_BAND = len(_BAND_RANK_GROUPS)
_SCREENING_CANON_RANK = _BAND_RANK_INDEX[("track1_direct", "screening_canon")]
_WEAK_RANK = _BAND_RANK_INDEX[("propagated", "weak")]


def band_rank(evidence_source: str, confidence_band: str) -> int:
    return _BAND_RANK_INDEX.get((evidence_source, confidence_band), _UNRANKED_BAND)


# ---------------------------------------------------------------------------
# Title-relationship machinery (D-13d granularity separation rule; reused
# verbatim, per the plan's instruction, for the novelty hard-case selection
# in Task 1). Pure string/Unicode operations -- no model call, ever.
# ---------------------------------------------------------------------------
_TITLE_PUNCT_RE = re.compile(r"[\"'׳״‘’“”]")
_WS_RE = re.compile(r"\s+")


def normalize_title(title: Optional[str]) -> str:
    """NFC + strip quote/geresh/gershayim marks + collapse whitespace.

    Deliberately does NOT strip nikud/te'amim (works.neutral_title is plain
    text with no vocalization in the shipped asset) and does NOT touch the
    maqaf ``־`` (a real word-joining character, not punctuation to
    discard) -- mirrors the same discipline documented for
    ``norm_stream_letter_count`` above.
    """
    if not title:
        return ""
    t = unicodedata.normalize("NFC", title)
    t = _TITLE_PUNCT_RE.sub("", t)
    t = _WS_RE.sub(" ", t).strip()
    return t


def titles_share_prefix(title_a: str, title_b: str, min_len: int = 4) -> bool:
    """True when the two (already-normalized) titles share a >= min_len prefix.

    Deliberately crude (a literal leading-character-run match, not tokenized
    morphology) -- this is the "concrete, testable" half of D-13d's proposed
    separation rule: cheap, auditable, and explicitly a DISPLAY heuristic,
    never a data fix.
    """
    if len(title_a) < min_len or len(title_b) < min_len:
        return False
    return title_a[:min_len] == title_b[:min_len]


def works_related_by_title(work_a: Dict[str, Any], work_b: Dict[str, Any]) -> bool:
    """D-13d's proposed separation rule: SAME non-null author AND (identical
    normalized title -- an undetected alias/duplicate -- OR a shared >=4-char
    normalized-title prefix, e.g. a shared "<author> on " commentary marker).

    This is the display-time alias/containment test the plan asks for. It is
    intentionally conservative (author-gated) precisely because an
    ungated title-prefix match alone is corpus-noisy (verified empirically
    against this asset: many M-source responsa collections share a single
    generic collection title -- e.g. "Responsa of the Geonim" -- across
    dozens of genuinely distinct items with NO author recorded; gating on a
    matching author removes that entire noise class).
    """
    author_a = work_a.get("author")
    author_b = work_b.get("author")
    if not author_a or not author_b or author_a != author_b:
        return False
    na, nb = normalize_title(work_a.get("neutral_title")), normalize_title(work_b.get("neutral_title"))
    if not na or not nb:
        return False
    if na == nb:
        return True
    return titles_share_prefix(na, nb, min_len=4)


def title_similarity_ratio(work_a: Dict[str, Any], work_b: Dict[str, Any]) -> float:
    na, nb = normalize_title(work_a.get("neutral_title")), normalize_title(work_b.get("neutral_title"))
    if not na or not nb:
        return 0.0
    return difflib.SequenceMatcher(None, na, nb).ratio()


# ---------------------------------------------------------------------------
# sys_id derivation. page_id = "{sys_id}_IE{ie_id}_P{page_num}_FL{fl_id}"
# (docs/specs/discovery-sidecar-schema-v1.md; canonical_refs "Measured
# facts"). Verified against a live sample before being trusted as a
# structural invariant (see the self-test in __main__ guard below is NOT
# present -- verification instead happens via the nonzero-nonNone assertion
# in load_claims()).
# ---------------------------------------------------------------------------
_PAGE_ID_SYS_RE = re.compile(r"^(\d+)_IE")


def sys_id_from_page_id(page_id: str) -> Optional[str]:
    m = _PAGE_ID_SYS_RE.match(page_id)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# A tiny nonzero-assertion ledger -- every measurement that SHOULD be
# nonzero registers itself here; main() exits 1 if anything failed, so a
# silent zero can never reach the owner as a finding (per-task acceptance
# criteria + the T-136-03-02 threat mitigation).
# ---------------------------------------------------------------------------
class NonzeroLedger:
    def __init__(self) -> None:
        self.failures: List[str] = []

    def check(self, label: str, value: int) -> int:
        if value == 0:
            self.failures.append(label)
        return value

    def ok(self) -> bool:
        return not self.failures


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def connect_readonly(path: str) -> sqlite3.Connection:
    if not os.path.exists(path):
        raise FileNotFoundError(f"asset not found: {path}")
    uri = f"file:{path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def load_works(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    rows = conn.execute(
        "SELECT work_id, canonical_work_id, neutral_title, author, genre, source_corpus "
        "FROM works ORDER BY work_id"
    ).fetchall()
    return {r["work_id"]: dict(r) for r in rows}


def load_claims(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """One row per claim, carrying its DISPLAY evidence's presentation fields.

    ``ORDER BY dc.claim_id`` makes downstream iteration order (and therefore
    any Python dict built by insertion) reproducible run-to-run.
    """
    rows = conn.execute(
        """
        SELECT dc.page_id, dc.work_id, dc.claim_id, dc.claim_type, dc.display_evidence_id,
               de.evidence_source, de.confidence_band, de.adjudication_status,
               de.routing_status, de.matched_letters, de.span_start, de.span_end,
               de.aligned_len
        FROM discovery_claim dc
        JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
        ORDER BY dc.claim_id
        """
    ).fetchall()
    claims = [dict(r) for r in rows]
    for c in claims:
        sid = sys_id_from_page_id(c["page_id"])
        if sid is None:
            raise ValueError(
                f"page_id does not match the expected '{{sys_id}}_IE...' shape: {c['page_id']!r}"
            )
        c["sys_id"] = sid
    return claims


def load_human_confirmed_claim_ids(conn: sqlite3.Connection) -> set:
    return {
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT claim_id FROM discovery_evidence "
            "WHERE adjudication_status = 'human_confirmed' ORDER BY claim_id"
        )
    }


def load_kept_tie_pages(conn: sqlite3.Connection, works: Dict[str, Dict[str, Any]]) -> set:
    """(page_id, canonical_work_id) pairs where the routing audit recorded an
    unresolved tie retained FOR that work on that page. ``demoted_work_id`` is
    NULL on every kept_tie row in this asset (D-02b's documented flaw), so
    only the kept-side of the tie is reconstructable here -- see the
    Methodology note in the rendered brief."""
    out = set()
    for row in conn.execute(
        "SELECT page_id, kept_work_id FROM discovery_routing_audit "
        "WHERE decision = 'kept_tie' ORDER BY id"
    ):
        w = works.get(row["kept_work_id"])
        if w is not None:
            out.add((row["page_id"], w["canonical_work_id"]))
    return out


def build_near_tie_competition(
    claims: List[Dict[str, Any]], works: Dict[str, Dict[str, Any]]
) -> set:
    """(page_id, canonical_work_id) pairs where at least one track1_direct
    claim for that (page, canonical work) has an overlapping, near-equal-
    length competing span from a DIFFERENT canonical work on the same page
    -- the other half of gate 3 ("an overlapping near-tie span from another
    canonical work"). Overlap >= 70% of the shorter span AND a length ratio
    >= 0.7 -- a stated, reproducible threshold, not a tuned constant."""
    page_spans: Dict[str, List[Tuple[str, int, int]]] = defaultdict(list)
    for c in claims:
        if c["evidence_source"] == "track1_direct" and c["span_start"] is not None and c["span_end"] is not None:
            w = works.get(c["work_id"])
            if w is None:
                continue
            page_spans[c["page_id"]].append((w["canonical_work_id"], c["span_start"], c["span_end"]))

    out = set()
    for c in claims:
        if c["evidence_source"] != "track1_direct" or c["span_start"] is None:
            continue
        w = works.get(c["work_id"])
        if w is None:
            continue
        canon = w["canonical_work_id"]
        s0, s1 = c["span_start"], c["span_end"]
        my_len = s1 - s0
        if my_len <= 0:
            continue
        for other_canon, os0, os1 in page_spans.get(c["page_id"], []):
            if other_canon == canon:
                continue
            olen = os1 - os0
            if olen <= 0:
                continue
            overlap = min(s1, os1) - max(s0, os0)
            if overlap <= 0:
                continue
            ratio = min(my_len, olen) / max(my_len, olen)
            overlap_frac = overlap / min(my_len, olen)
            if ratio >= 0.7 and overlap_frac >= 0.7:
                out.add((c["page_id"], canon))
                break
    return out


# ---------------------------------------------------------------------------
# Main-pool-rule classification (references/main-pool-rule.md). Unit =
# identification = (sys_id, canonical_work_id). Gates evaluated in order;
# ANY gate sends the identification to "show more" (show_more); passing all
# four (or a human_confirmed override) -> "main".
# ---------------------------------------------------------------------------

def classify_identifications(
    claims: List[Dict[str, Any]],
    works: Dict[str, Dict[str, Any]],
    human_confirmed_claim_ids: set,
    kept_tie_pages: set,
    near_tie_pages: set,
    page_norm_letters: Dict[str, int],
) -> Tuple[Dict[Tuple[str, str], List[Dict[str, Any]]], Dict[Tuple[str, str], Tuple[str, str]]]:
    """Returns (identifications, classification) where ``identifications``
    maps (sys_id, canonical_work_id) -> its usable claims, and
    ``classification`` maps the same key -> (bucket, reason).

    "Usable" claims (surface-visible, current product behaviour): the claim's
    DISPLAY evidence has ``routing_status == 'shipped'``, OR ANY evidence row
    for the claim is ``adjudication_status == 'human_confirmed'`` (the D-13g
    fix folded in here -- a human-confirmed row must never be invisible to
    this measurement merely because routing demoted it)."""
    identifications: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for c in claims:
        is_hc = c["claim_id"] in human_confirmed_claim_ids
        if c["routing_status"] != "shipped" and not is_hc:
            continue
        w = works.get(c["work_id"])
        if w is None:
            continue
        c = dict(c)
        c["canonical_work_id"] = w["canonical_work_id"]
        c["is_human_confirmed"] = is_hc
        identifications[(c["sys_id"], w["canonical_work_id"])].append(c)

    classification: Dict[Tuple[str, str], Tuple[str, str]] = {}
    for key, cl in sorted(identifications.items()):
        sys_id, canon = key
        if any(x["is_human_confirmed"] for x in cl):
            classification[key] = ("main", "human_confirmed_override")
            continue
        if not any(x["claim_type"] == "direct_witness" for x in cl):
            classification[key] = ("show_more", "gate1_no_same_work_claim")
            continue
        best = min(band_rank(x["evidence_source"], x["confidence_band"]) for x in cl)
        if best in (_SCREENING_CANON_RANK, _WEAK_RANK):
            classification[key] = ("show_more", "gate2_best_band_weak")
            continue
        pages = {x["page_id"] for x in cl}
        if pages and all(
            (p, canon) in kept_tie_pages or (p, canon) in near_tie_pages for p in pages
        ):
            classification[key] = ("show_more", "gate3_unresolved_competition")
            continue
        if len(pages) == 1:
            page = next(iter(pages))
            mls = [x["matched_letters"] for x in cl if x["evidence_source"] == "track1_direct" and x["matched_letters"] is not None]
            if not mls:
                classification[key] = ("show_more", "gate4_no_coverage_data")
                continue
            pnl = page_norm_letters.get(page)
            cov = compute_page_coverage(max(mls), pnl)
            if cov is None or cov < 0.8:
                classification[key] = ("show_more", "gate4_low_single_page_coverage")
            else:
                classification[key] = ("main", "gate4_single_page_full_coverage")
            continue
        classification[key] = ("main", "multi_folio_agreement")

    return identifications, classification


def pages_needing_coverage(
    claims: List[Dict[str, Any]],
    works: Dict[str, Dict[str, Any]],
    human_confirmed_claim_ids: set,
    kept_tie_pages: set,
    near_tie_pages: set,
) -> List[str]:
    """Pre-pass identical to the gate1-3 portion of classify_identifications,
    used ONLY to determine which single pages need a page_norm_letters
    lookup before the real classification runs (so we query fullcorpus.db
    for exactly the pages needed, never the whole 667K-row table)."""
    identifications: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for c in claims:
        is_hc = c["claim_id"] in human_confirmed_claim_ids
        if c["routing_status"] != "shipped" and not is_hc:
            continue
        w = works.get(c["work_id"])
        if w is None:
            continue
        c = dict(c)
        c["canonical_work_id"] = w["canonical_work_id"]
        c["is_human_confirmed"] = is_hc
        identifications[(c["sys_id"], w["canonical_work_id"])].append(c)

    needed = set()
    for key, cl in identifications.items():
        sys_id, canon = key
        if any(x["is_human_confirmed"] for x in cl):
            continue
        if not any(x["claim_type"] == "direct_witness" for x in cl):
            continue
        best = min(band_rank(x["evidence_source"], x["confidence_band"]) for x in cl)
        if best in (_SCREENING_CANON_RANK, _WEAK_RANK):
            continue
        pages = {x["page_id"] for x in cl}
        if pages and all(
            (p, canon) in kept_tie_pages or (p, canon) in near_tie_pages for p in pages
        ):
            continue
        if len(pages) == 1:
            page = next(iter(pages))
            mls = [x["matched_letters"] for x in cl if x["evidence_source"] == "track1_direct" and x["matched_letters"] is not None]
            if mls:
                needed.add(page)
    return sorted(needed)


def load_page_norm_letters(research_db_path: str, page_ids: List[str]) -> Dict[str, int]:
    if not page_ids:
        return {}
    conn = sqlite3.connect(f"file:{research_db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    result: Dict[str, int] = {}
    CHUNK = 400
    try:
        for i in range(0, len(page_ids), CHUNK):
            chunk = page_ids[i : i + CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            for row in conn.execute(
                f"SELECT page_id, text FROM pages WHERE page_id IN ({placeholders})",  # noqa: S608 -- fixed table name, params bound
                chunk,
            ):
                result[row["page_id"]] = norm_stream_letter_count(row["text"])
    finally:
        conn.close()
    return result


# ---------------------------------------------------------------------------
# Identical-span groups (D-13b / D-13d). Population = the "shipped direct
# set": evidence_kind='witness', evidence_source='track1_direct',
# routing_status='shipped', grouped by (page_id, span_start, span_end).
# ---------------------------------------------------------------------------

def load_identical_span_groups(
    conn: sqlite3.Connection, works: Dict[str, Dict[str, Any]]
) -> Dict[Tuple[str, int, int], List[Dict[str, Any]]]:
    rows = conn.execute(
        """
        SELECT de.evidence_id, de.claim_id, dc.page_id AS page_id, dc.work_id,
               de.span_start, de.span_end, de.matched_letters,
               de.evidence_source, de.confidence_band
        FROM discovery_evidence de
        JOIN discovery_claim dc ON dc.claim_id = de.claim_id
        WHERE de.evidence_kind = 'witness' AND de.evidence_source = 'track1_direct'
          AND de.routing_status = 'shipped'
        ORDER BY dc.page_id, de.span_start, de.span_end, de.evidence_id
        """
    ).fetchall()
    groups: Dict[Tuple[str, int, int], List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        d = dict(r)
        w = works.get(d["work_id"])
        d["canonical_work_id"] = w["canonical_work_id"] if w else None
        groups[(d["page_id"], d["span_start"], d["span_end"])].append(d)
    return {k: v for k, v in groups.items() if len(v) >= 2}


def load_libraries_csv(path: str) -> Dict[str, Dict[str, str]]:
    """sys_id -> {shelfmark, library_code, catalogue_text}. Public catalogue
    metadata already displayed throughout the product (masking-safe)."""
    out: Dict[str, Dict[str, str]] = {}
    if not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        for row in reader:
            if len(row) < 8:
                continue
            sys_id = row[0].strip()
            if not sys_id:
                continue
            call_numbers = row[2]
            shelfmark = call_numbers.split("|")[0].strip() if call_numbers else ""
            out[sys_id] = {
                "shelfmark": shelfmark,
                "library_code": row[3].strip(),
                "catalogue_text": row[7].strip(),
            }
    return out


def best_claim_for_work(
    claims: List[Dict[str, Any]], work_id: str
) -> Optional[Dict[str, Any]]:
    """Deterministic representative shipped claim for a work: highest
    matched_letters first, then lexicographically smallest page_id (a total
    order, never "whichever row the query happens to return first")."""
    candidates = [
        c for c in claims
        if c["work_id"] == work_id and c["routing_status"] == "shipped"
    ]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda c: (-(c["matched_letters"] or 0), c["page_id"]),
    )


# ---------------------------------------------------------------------------
# D-13e: the middle-bucket "not otherwise reachable" count.
# ---------------------------------------------------------------------------

def compute_d13e(
    conn: sqlite3.Connection,
    span_groups: Dict[Tuple[str, int, int], List[Dict[str, Any]]],
    classification: Dict[Tuple[str, str], Tuple[str, str]],
) -> Dict[str, Any]:
    # D-13d population within the span groups: groups with >= 2 DIFFERENT
    # canonical works (a group whose members share ONE canonical id is a
    # D-13a duplicate-rendering case, not a D-13d generic-shared-text case).
    generic_claims: List[Tuple[str, str]] = []  # (sys_id, canonical_work_id) per affected claim
    for key, members in span_groups.items():
        canons = {m["canonical_work_id"] for m in members if m["canonical_work_id"]}
        if len(canons) < 2:
            continue
        for m in members:
            if not m["canonical_work_id"]:
                continue
            sid = sys_id_from_page_id(m["page_id"])
            generic_claims.append((sid, m["canonical_work_id"]))

    not_reachable_generic = 0
    overlap_generic = 0
    for k in generic_claims:
        bucket, _reason = classification.get(k, ("show_more", "not_classified"))
        if bucket == "main":
            not_reachable_generic += 1
        else:
            overlap_generic += 1

    # Related-pages population (D-11a): shipped shared_text evidence never
    # maps onto a work identification at all, so it can NEVER be reached via
    # the per-work "show more matches" toggle -- the entire population is
    # "not otherwise reachable". Counted as DIRECTED (anchor, opposite) page
    # pairs, matching D-11a's own reported figure.
    (related_pairs,) = conn.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT a_page_id, other_page_id FROM discovery_evidence "
        "WHERE evidence_kind = 'shared_text' AND routing_status = 'shipped')"
    ).fetchone()

    total_middle = len(generic_claims) + related_pairs
    not_reachable = not_reachable_generic + related_pairs
    overlap = overlap_generic

    return {
        "generic_claims_total": len(generic_claims),
        "generic_not_reachable": not_reachable_generic,
        "generic_overlap": overlap_generic,
        "related_pairs": related_pairs,
        "total_middle_bucket": total_middle,
        "not_reachable_total": not_reachable,
        "overlap_total": overlap,
    }


# ---------------------------------------------------------------------------
# D-16: relation distribution.
# ---------------------------------------------------------------------------

def compute_d16(
    conn: sqlite3.Connection,
    claims: List[Dict[str, Any]],
    classification: Dict[Tuple[str, str], Tuple[str, str]],
) -> Dict[str, Any]:
    (corpus_wide,) = ([dict(r) for r in conn.execute(
        "SELECT claim_type, COUNT(*) AS n FROM discovery_claim GROUP BY claim_type ORDER BY claim_type"
    )],)
    shipped = [
        dict(r)
        for r in conn.execute(
            """
            SELECT dc.claim_type, COUNT(*) AS n
            FROM discovery_claim dc
            JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
            WHERE de.routing_status = 'shipped'
            GROUP BY dc.claim_type ORDER BY dc.claim_type
            """
        )
    ]

    main_pool_relation: Dict[str, int] = defaultdict(int)
    for c in claims:
        sid, wid = c["sys_id"], c.get("canonical_work_id")
        if wid is None:
            continue
        bucket, _ = classification.get((sid, wid), ("show_more", "n/a"))
        if bucket == "main":
            main_pool_relation[c["claim_type"]] += 1

    return {
        "corpus_wide": corpus_wide,
        "shipped": shipped,
        "main_pool": dict(sorted(main_pool_relation.items())),
    }


# ---------------------------------------------------------------------------
# D-13c: short-evidence thresholds.
# ---------------------------------------------------------------------------

def compute_d13c(
    conn: sqlite3.Connection,
    classification: Dict[Tuple[str, str], Tuple[str, str]],
    claims: List[Dict[str, Any]],
) -> Dict[str, Any]:
    thresholds = (50, 100, 150, 200)

    direct_lengths = [
        r[0]
        for r in conn.execute(
            "SELECT matched_letters FROM discovery_evidence "
            "WHERE evidence_source = 'track1_direct' AND routing_status = 'shipped' "
            "AND matched_letters IS NOT NULL"
        )
    ]
    propagated_lengths = [
        r[0]
        for r in conn.execute(
            "SELECT aligned_len FROM discovery_evidence "
            "WHERE evidence_source = 'propagated' AND evidence_kind = 'shared_text' "
            "AND routing_status = 'shipped' AND aligned_len IS NOT NULL"
        )
    ]

    direct_cumulative = {t: sum(1 for v in direct_lengths if v < t) for t in thresholds}
    propagated_cumulative = {t: sum(1 for v in propagated_lengths if v < t) for t in thresholds}

    thinnest_direct = min(direct_lengths) if direct_lengths else None

    # Short direct rows (< 150 matched letters) that are nonetheless part of
    # a MAIN identification via multi-folio agreement specifically (the
    # honest counter-argument the owner already accepted: a short liturgical
    # passage may be exactly the correct identification for a prayer book).
    short_in_main_multi_folio = 0
    for c in claims:
        if c["evidence_source"] != "track1_direct" or c["matched_letters"] is None:
            continue
        if c["matched_letters"] >= 150:
            continue
        sid, wid = c["sys_id"], c.get("canonical_work_id")
        if wid is None:
            continue
        bucket, reason = classification.get((sid, wid), ("show_more", "n/a"))
        if bucket == "main" and reason == "multi_folio_agreement":
            short_in_main_multi_folio += 1

    return {
        "thresholds": thresholds,
        "direct_total": len(direct_lengths),
        "direct_cumulative": direct_cumulative,
        "propagated_total": len(propagated_lengths),
        "propagated_cumulative": propagated_cumulative,
        "thinnest_direct": thinnest_direct,
        "short_in_main_multi_folio": short_in_main_multi_folio,
    }


# ---------------------------------------------------------------------------
# D-13b: lead-attribution tie residual.
# ---------------------------------------------------------------------------

def compute_d13b(span_groups: Dict[Tuple[str, int, int], List[Dict[str, Any]]]) -> Dict[str, Any]:
    total_groups = len(span_groups)
    total_claims = sum(len(v) for v in span_groups.values())
    tied_groups = 0
    tied_claims = 0
    for members in span_groups.values():
        ranks = [band_rank(m["evidence_source"], m["confidence_band"]) for m in members]
        best = min(ranks)
        n_at_best = sum(1 for r in ranks if r == best)
        if n_at_best >= 2:
            tied_groups += 1
            tied_claims += n_at_best
    return {
        "total_groups": total_groups,
        "total_claims": total_claims,
        "tied_after_band_rank_groups": tied_groups,
        "tied_after_band_rank_claims": tied_claims,
    }


# ---------------------------------------------------------------------------
# D-13d: granularity separation quantification + worked example.
# ---------------------------------------------------------------------------

WORKED_EXAMPLE_WORK_IDS = ("w000171", "w001281")
# The SPECIFIC instance named in 136-CONTEXT.md D-13d (T-S Misc. 12.31.14,
# sys_id 990051079570205171) -- verified directly against libraries.csv
# during this plan's research. Preferred over any OTHER co-occurrence of the
# same work pair (this pair also co-occurs across dozens of pages of a
# separate, much larger Rashi-commentary manuscript, sys_id
# 990000852430205171 -- a real finding in its own right, but not the
# documented worked example the owner has already reviewed in the mockup).
WORKED_EXAMPLE_SPAN = (0, 962)


def compute_d13d(
    span_groups: Dict[Tuple[str, int, int], List[Dict[str, Any]]],
    works: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    diff_canon_groups = []
    for key, members in span_groups.items():
        canons = {m["canonical_work_id"] for m in members if m["canonical_work_id"]}
        if len(canons) >= 2:
            diff_canon_groups.append((key, members))

    collapse_candidate_groups = []
    generic_groups = []
    for key, members in diff_canon_groups:
        work_ids = sorted({m["work_id"] for m in members})
        found = False
        for i in range(len(work_ids)):
            for j in range(i + 1, len(work_ids)):
                wa, wb = works.get(work_ids[i]), works.get(work_ids[j])
                if wa and wb and works_related_by_title(wa, wb):
                    found = True
                    break
            if found:
                break
        if found:
            collapse_candidate_groups.append((key, members))
        else:
            generic_groups.append((key, members))

    worked_example = None
    exact_example_entry = None
    fallback_example_entry = None
    for key, members in diff_canon_groups:
        work_ids = {m["work_id"] for m in members}
        if not set(WORKED_EXAMPLE_WORK_IDS).issubset(work_ids):
            continue
        if fallback_example_entry is None:
            fallback_example_entry = (key, work_ids, members)
        _page_id, span_start, span_end = key
        if (span_start, span_end) == WORKED_EXAMPLE_SPAN:
            exact_example_entry = (key, work_ids, members)
            break
    chosen_entry = exact_example_entry or fallback_example_entry

    if chosen_entry is not None:
        key, work_ids, members = chosen_entry
        page_id, span_start, span_end = key
        worked_example = {
            "page_id": page_id,
            "sys_id": sys_id_from_page_id(page_id),
            "span_start": span_start,
            "span_end": span_end,
            "matched_letters": members[0]["matched_letters"],
            "works": [
                {
                    "work_id": wid,
                    "canonical_work_id": works[wid]["canonical_work_id"],
                    "neutral_title": works[wid]["neutral_title"],
                    "author": works[wid]["author"],
                }
                for wid in sorted(work_ids)
                if wid in works
            ],
        }

    return {
        "diff_canon_groups_total": len(diff_canon_groups),
        "diff_canon_claims_total": sum(len(v) for _, v in diff_canon_groups),
        "collapse_candidate_groups": len(collapse_candidate_groups),
        "collapse_candidate_claims": sum(len(v) for _, v in collapse_candidate_groups),
        "generic_groups": len(generic_groups),
        "generic_claims": sum(len(v) for _, v in generic_groups),
        "worked_example": worked_example,
        "collapse_candidate_keys": [k for k, _ in collapse_candidate_groups],
    }


# ---------------------------------------------------------------------------
# Novelty hard-case candidate set (feeds Task 3). Zero model calls -- pure
# string/title comparison, reusing works_related_by_title /
# title_similarity_ratio from the D-13d machinery above.
# ---------------------------------------------------------------------------

def _evenly_spaced_indices(n: int, k: int) -> List[int]:
    """``k`` evenly-spaced 0-based indices across a list of length ``n``,
    inclusive of the first and last positions when ``k >= 2``. Deterministic
    (round-half-to-even via Python's ``round``, but every input here is a
    fixed, already-sorted pool, so re-running against the same asset always
    yields the same indices) -- used to build the Class 1-3 IDENTITY
    spot-check (owner-authorized labelling restructure) by sampling spread
    positions across each class's existing deterministic candidate order,
    rather than hand-picking specific cases -- so the selection stays a
    reproducible function of the data, never a manually pinned case list."""
    if k <= 0 or n <= 0:
        return []
    if k == 1:
        return [0]
    if k >= n:
        return list(range(n))
    seen: List[int] = []
    for i in range(k):
        idx = round(i * (n - 1) / (k - 1))
        if idx not in seen:
            seen.append(idx)
    return seen

def select_alias_pair_candidates(works: Dict[str, Dict[str, Any]]) -> List[Tuple[Dict, Dict]]:
    """Class 2 (alias pairs): cluster works by (author, normalized title);
    a cluster of EXACTLY 2 members with DIFFERENT canonical_work_id is a
    clean alias/duplicate candidate. Clusters of 3+ are excluded: verified
    empirically against this asset that large clusters are generic
    multi-item collection titles (e.g. many distinct M-source responsa
    items sharing one collector's name as both "author" and title stem),
    which is corpus noise for this purpose, not an alias signal."""
    clusters: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for w in sorted(works.values(), key=lambda w: w["work_id"]):
        author = w.get("author")
        nt = normalize_title(w.get("neutral_title"))
        if author and nt:
            clusters[(author, nt)].append(w)
    pairs = []
    for key in sorted(clusters.keys()):
        members = clusters[key]
        if len(members) == 2 and members[0]["canonical_work_id"] != members[1]["canonical_work_id"]:
            pairs.append((members[0], members[1]))
    return pairs


def select_near_miss_candidates(
    works: Dict[str, Dict[str, Any]], min_ratio: float = 0.85, max_ratio: float = 0.999
) -> List[Tuple[Dict, Dict, float]]:
    """Class 1 (near-miss titles): same-author pairs whose normalized titles
    are highly similar (SequenceMatcher ratio in [min_ratio, max_ratio)) but
    NOT identical -- a string-comparison-based check can be fooled either
    way (treating them as the same, or missing that they are related)."""
    items = sorted(works.values(), key=lambda w: w["work_id"])
    out = []
    n = len(items)
    for i in range(n):
        wa = items[i]
        na = normalize_title(wa.get("neutral_title"))
        if not na or not wa.get("author"):
            continue
        for j in range(i + 1, n):
            wb = items[j]
            if wa["canonical_work_id"] == wb["canonical_work_id"]:
                continue
            if wb.get("author") != wa.get("author"):
                continue
            nb = normalize_title(wb.get("neutral_title"))
            if not nb or na == nb:
                continue
            ratio = difflib.SequenceMatcher(None, na, nb).ratio()
            if min_ratio <= ratio < max_ratio:
                out.append((wa, wb, ratio))
    out.sort(key=lambda t: (-t[2], t[0]["work_id"], t[1]["work_id"]))
    return out


# ---------------------------------------------------------------------------
# Classes 4 and 5 -- OWNER-AUTHORIZED scope extension recorded in
# 136-GATE1-DECISIONS.md (item C). Added AFTER the original 52 candidates,
# at the gate, because the original three classes were selected adversarially
# to a STRING heuristic, not to an LLM -- these two classes exist so the
# measured error rate is not flattered by cases an LLM finds easy. Same
# discipline as classes 1-3: zero model calls, pure metadata/string
# comparison, fully deterministic (every selection below is either a total
# order over a stable key or an explicit sort before slicing).
# ---------------------------------------------------------------------------

def select_terse_catalogue_candidates(
    claims: List[Dict[str, Any]],
    libraries: Dict[str, Dict[str, str]],
    cap: int = 15,
    max_len: int = 20,
) -> List[Dict[str, Any]]:
    """Class 4 (terse or missing catalogue identification text): shipped
    ``direct_witness`` claims on a manuscript (sys_id) whose OWN
    ``libraries.csv`` ``catalogue_text`` field is either entirely absent
    (empty string, or no row at all) or so short (<= ``max_len`` characters,
    nonzero) that a title comparison has almost nothing to work with.

    One representative claim per sys_id: the highest ``matched_letters``,
    then lexicographically smallest ``page_id``, then ``work_id`` -- a total
    order, never "whichever row the query happens to return first".
    Candidate sys_ids are then sorted by (catalogue-text length ascending,
    sys_id) so the emptiest cases surface first, and capped at ``cap``.
    """
    best_by_sys: Dict[str, Dict[str, Any]] = {}
    for c in claims:
        if c.get("claim_type") != "direct_witness" or c.get("routing_status") != "shipped":
            continue
        sid = c["sys_id"]
        cat = libraries.get(sid, {}).get("catalogue_text", "")
        if len(cat) > max_len:
            continue
        key = (-(c["matched_letters"] or 0), c["page_id"], c["work_id"])
        prev = best_by_sys.get(sid)
        if prev is None or key < prev["_key"]:
            d = dict(c)
            d["_key"] = key
            best_by_sys[sid] = d

    ordered_sys_ids = sorted(
        best_by_sys.keys(),
        key=lambda sid: (len(libraries.get(sid, {}).get("catalogue_text", "")), sid),
    )
    return [{"sys_id": sid, "claim": best_by_sys[sid]} for sid in ordered_sys_ids[:cap]]


def select_generic_collection_candidates(
    claims: List[Dict[str, Any]],
    works: Dict[str, Dict[str, Any]],
    cap: int = 15,
    min_cluster_size: int = 3,
) -> List[Dict[str, Any]]:
    """Class 5 (generic collection works): (author, normalized-title)
    clusters of >= ``min_cluster_size`` works carrying >= 2 distinct
    ``canonical_work_id``s -- precisely the large generic-collection-title
    clusters ``select_alias_pair_candidates`` (above) explicitly EXCLUDES as
    corpus noise (its own docstring: "large clusters are generic multi-item
    collection titles -- e.g. many distinct M-source responsa items sharing
    one collector's name as both author and title stem"). Here they ARE the
    signal, not the noise: for "already recorded" to mean anything for a
    single witness of such a collection is genuinely ill-defined, not merely
    hard to string-match.

    For each such cluster this returns REAL shipped ``direct_witness``
    manuscripts (best claim per sys_id, by matched_letters descending then
    page_id then work_id -- a total order), round-robin across clusters
    (largest cluster first, ties broken by the cluster key) one at a time so
    no single collection crowds out the others, capped at ``cap``.
    """
    clusters: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for w in sorted(works.values(), key=lambda w: w["work_id"]):
        author = w.get("author")
        nt = normalize_title(w.get("neutral_title"))
        if author and nt:
            clusters[(author, nt)].append(w)

    big_clusters: List[Tuple[int, Tuple[str, str], List[Dict[str, Any]]]] = []
    for key, members in clusters.items():
        if len(members) >= min_cluster_size:
            canon_ids = {m["canonical_work_id"] for m in members}
            if len(canon_ids) >= 2:
                big_clusters.append((len(members), key, members))
    big_clusters.sort(key=lambda t: (-t[0], t[1]))

    claims_by_workid: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for c in claims:
        if c.get("claim_type") == "direct_witness" and c.get("routing_status") == "shipped":
            claims_by_workid[c["work_id"]].append(c)

    per_cluster_reps: List[Tuple[Tuple[str, str], List[Dict[str, Any]], List[Dict[str, Any]]]] = []
    for _size, key, members in big_clusters:
        work_ids = sorted({m["work_id"] for m in members})
        by_sys: Dict[str, Dict[str, Any]] = {}
        for wid in work_ids:
            for c in claims_by_workid.get(wid, []):
                sid = c["sys_id"]
                cand_key = (-(c["matched_letters"] or 0), c["page_id"], c["work_id"])
                prev = by_sys.get(sid)
                if prev is None or cand_key < prev["_key"]:
                    d = dict(c)
                    d["_key"] = cand_key
                    by_sys[sid] = d
        reps = sorted(by_sys.values(), key=lambda d: d["_key"])
        per_cluster_reps.append((key, members, reps))

    out: List[Dict[str, Any]] = []
    round_idx = 0
    while len(out) < cap:
        added_this_round = False
        for key, members, reps in per_cluster_reps:
            if round_idx < len(reps):
                out.append({"cluster_key": key, "members": members, "claim": reps[round_idx]})
                added_this_round = True
                if len(out) >= cap:
                    break
        if not added_this_round:
            break
        round_idx += 1
    return out[:cap]


def select_catalogue_divergence_candidates(
    claims: List[Dict[str, Any]],
    works: Dict[str, Dict[str, Any]],
    libraries: Dict[str, Dict[str, str]],
    cap: int = 15,
    min_divergent_title_len: int = 6,
) -> List[Dict[str, Any]]:
    """Class 6 (catalogue divergence, owner decision E -- 136-GATE1-DECISIONS.md
    item E): shipped ``direct_witness`` claims on a manuscript whose OWN
    ``libraries.csv`` catalogue-identification text, once normalized, contains
    the normalized title of a DIFFERENT work than the one this claim
    identifies, where that other work is NOT a D-13d granularity variant of
    the claimed work (``works_related_by_title`` returns False for the pair)
    -- decision E's ``diverges`` shade: "an aid ties F to a different work
    that is NOT a granularity variant". This class has ZERO representation
    across Classes 1-5 and is exactly the shade decision E's rationale names
    as currently inflating the novelty (``not_in_finding_aids``) count.

    Selection is pure deterministic string containment over data already
    loaded for Classes 1-5 -- ZERO model calls, same discipline as every
    other class. A manuscript/work pair is skipped as a candidate when the
    claimed work's OWN normalized title is already found in the catalogue
    text (that is agreement, however partial, not divergence). Only work
    titles at least ``min_divergent_title_len`` normalized characters long
    are searched for containment -- a short title would match too much
    catalogue prose to be a trustworthy divergence signal (the same
    reasoning as D-13d's >=4-char PREFIX bar, raised here because this is
    FULL-title containment against free-text catalogue prose, not a
    title-to-title prefix comparison).

    One candidate per manuscript (sys_id): across all of that manuscript's
    shipped claimed works, the one whose best divergent match has the
    LONGEST normalized divergent title wins (a longer matched title is less
    likely to be a spurious substring hit). Candidates are then GROUPED by
    the divergent work they name and round-robined across groups (largest
    group first, ties broken by work_id -- the identical discipline
    ``select_generic_collection_candidates`` above uses for its clusters):
    a single widely-quoted title (e.g. a popular ethical/halakhic work named
    in passing across many unrelated catalogue entries) would otherwise
    crowd out every other divergence pattern, exactly the diversity problem
    that function's own docstring names. Deterministic, no randomness, no
    sampling; re-running against the same asset reproduces the identical
    candidate list byte-for-byte.
    """
    best_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for c in claims:
        if c.get("claim_type") != "direct_witness" or c.get("routing_status") != "shipped":
            continue
        key = (c["sys_id"], c["work_id"])
        cand_key = (-(c["matched_letters"] or 0), c["page_id"])
        prev = best_by_key.get(key)
        if prev is None or cand_key < prev["_key"]:
            d = dict(c)
            d["_key"] = cand_key
            best_by_key[key] = d

    title_index: List[Tuple[Dict[str, Any], str]] = []
    for w in sorted(works.values(), key=lambda w: w["work_id"]):
        nt = normalize_title(w.get("neutral_title"))
        if len(nt) >= min_divergent_title_len:
            title_index.append((w, nt))

    sys_ids = sorted({sid for sid, _wid in best_by_key.keys()})
    per_sys_best: List[Dict[str, Any]] = []
    for sid in sys_ids:
        cat_text = libraries.get(sid, {}).get("catalogue_text", "")
        if not cat_text:
            continue
        cat_norm = normalize_title(cat_text)
        if not cat_norm:
            continue
        work_ids_for_sys = sorted({wid for s, wid in best_by_key.keys() if s == sid})
        best_case: Optional[Tuple[int, str, Dict[str, Any], Dict[str, Any], Dict[str, Any]]] = None
        for wid in work_ids_for_sys:
            claim = best_by_key[(sid, wid)]
            w_claimed = works.get(wid)
            if w_claimed is None:
                continue
            claimed_norm = normalize_title(w_claimed.get("neutral_title"))
            if claimed_norm and claimed_norm in cat_norm:
                continue  # catalogue text already seems to name this exact claim -- not a divergence
            for o, nt in title_index:
                if o["canonical_work_id"] == w_claimed["canonical_work_id"]:
                    continue
                if nt not in cat_norm:
                    continue
                if works_related_by_title(w_claimed, o):
                    continue  # D-13d granularity variant, not a genuine divergence
                cand = (len(nt), wid, w_claimed, o, claim)
                if best_case is None or cand[0] > best_case[0]:
                    best_case = cand
        if best_case is not None:
            _mlen, _wid, w_claimed, o, claim = best_case
            per_sys_best.append({
                "sys_id": sid,
                "claim": claim,
                "claimed_work": w_claimed,
                "divergent_work": o,
            })

    # Round-robin across DISTINCT divergent-work groups (largest group first,
    # ties by work_id) -- same discipline as select_generic_collection_candidates
    # above, so one frequently-quoted title cannot crowd out every other
    # divergence pattern in the capped list.
    by_divergent: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for entry in per_sys_best:
        by_divergent[entry["divergent_work"]["work_id"]].append(entry)
    groups: List[Tuple[int, str, List[Dict[str, Any]]]] = []
    for wid, entries in by_divergent.items():
        entries.sort(key=lambda e: (e["sys_id"], e["claim"]["work_id"]))
        groups.append((len(entries), wid, entries))
    groups.sort(key=lambda t: (-t[0], t[1]))

    out: List[Dict[str, Any]] = []
    round_idx = 0
    while len(out) < cap:
        added_this_round = False
        for _size, _wid, entries in groups:
            if round_idx < len(entries):
                out.append(entries[round_idx])
                added_this_round = True
                if len(out) >= cap:
                    break
        if not added_this_round:
            break
        round_idx += 1
    return out[:cap]


# ---------------------------------------------------------------------------
# Class-6 owner scope/shade annotations (rulings F/G, 136-GATE1-DECISIONS.md
# §§ F/G). The owner read all 15 of the ORIGINAL Class-6 candidates directly
# (not merely the shade's abstract definition) and issued explicit verdicts
# on 12 of them, keyed here by sys_id (stable across a case-number
# renumbering, unlike the "Case N" labels in the pre-restructure worksheet
# those verdicts were originally phrased against). This dict does NOT decide
# the owner's Task-3 verdict for these cases -- it only upgrades their
# PROPOSAL text (still explicitly marked PROPOSAL, still not a label) to
# reflect what the owner has ALREADY told this project about that specific
# manuscript/work pair, so the regenerated worksheet does not silently
# regress to a stale generic "plausibly diverges" proposal for cases the
# owner has already spoken to directly. The 3 original candidates NOT named
# in either ruling (sys_ids not present here) are left with a generic,
# undetermined-scope proposal -- genuinely open, not silently resolved.
# ---------------------------------------------------------------------------
_CLASS6_OWNER_SCOPE: Dict[str, Dict[str, str]] = {
    # --- ruling F: diverges_work ("usually our claim is wrong") ---
    "990001004230205171": {"shade": "diverges_work", "ruling": "F"},  # case 92: ילקוט שמעוני / תנחומא
    "990000413480205171": {"shade": "diverges_work", "ruling": "F"},  # case 84: משנה תורה ספר זמנים / הגדה של פסח
    "990001398690205171": {"shade": "diverges_work", "ruling": "F"},  # case 86: משנה תורה ספר אהבה / ברכת המזון
    "990000621960205171": {"shade": "diverges_work", "ruling": "F"},  # case 95: תנ"ך בראשית / שאילתות
    "990051080280205171": {"shade": "diverges_work", "ruling": "F"},  # case 97: ויקרא רבה / חובות הלבבות
    "990000905560205171": {"shade": "diverges_work", "ruling": "F"},  # case 91: מכילתא דרשב"י / מכילתא דרבי ישמעאל
    "990000555810205171": {"shade": "diverges_work", "ruling": "F"},  # case 85: הלכות פסוקות / הלכות גדולות
    # --- ruling F: diverges_part ("more delicate and essentially less important") ---
    "990001935160205171": {"shade": "diverges_part", "ruling": "F"},  # case 90: הלכות פסוקות...קידושין / הלכות פסוקות
    "990051173260205171": {"shade": "diverges_part", "ruling": "F"},  # case 94: משנה תורה הקדמה ומניין המצוות / הלכות ציצית
    "990051150540205171": {"shade": "diverges_part", "ruling": "F"},  # case 96: בראשית רבה צה-צו תוספת / בראשית רבה
    # --- ruling G: NOT a divergence at all -- the aid's own FREE TEXT
    # already states the claimed identification; only the structured
    # work-id keying differed (the selector's own over-fire failure mode) ---
    "990000555880205171": {
        "shade": "confirms",
        "ruling": "G",
        "free_text_quote": "שאלות ותשובות מאת האי בן שרירא גאון",
    },  # case 83: תשובות האיי גאון (structured match: תשובות)
    "990001394270205171": {
        "shade": "confirms",
        "ruling": "G",
        "free_text_quote": "יוסיפון בערבית",
    },  # case 87: ספר יוסיפון (ערבי) (structured match: יוסיפון)
}


def build_hardcases(
    claims: List[Dict[str, Any]],
    works: Dict[str, Dict[str, Any]],
    d13d: Dict[str, Any],
    libraries: Dict[str, Dict[str, str]],
    class1_spotcheck_cap: int = 3,
    class2_spotcheck_cap: int = 2,
    class3_spotcheck_cap: int = 3,
    cap_per_class: int = 20,
    class4_cap: int = 20,
    class5_cap: int = 25,
    class6_cap: int = 30,
) -> List[Dict[str, Any]]:
    cases: List[Dict[str, Any]] = []

    # --- Class 3: granularity -- IDENTITY spot-check (owner-authorized
    # labelling restructure, same continuation as rulings F/G). Full
    # labelling of the class's candidate pool was REPLACED by an
    # ~class3_spotcheck_cap-case spread, testing the constant-answer
    # assumption ("A and B are the same work at two granularities") rather
    # than building ground truth over all of it -- see the module docstring
    # and 136-GATE1-DECISIONS.md's labelling-restructure note. ---
    # Dedupe to ONE representative group per manuscript (sys_id) first --
    # a single large manuscript can contribute dozens of near-duplicate
    # span-groups (verified: sys_id 990000852430205171 alone supplies 11 of
    # the 276 collapse candidates), which would otherwise crowd out
    # diversity in the candidate pool. Keep each manuscript's LARGEST (most
    # matched_letters) group as its representative.
    by_sys_id: Dict[str, Tuple[Tuple[str, int, int], int]] = {}
    for key in d13d["collapse_candidate_keys"]:
        page_id, s0, s1 = key
        sid = sys_id_from_page_id(page_id)
        matched = s1 - s0
        prev = by_sys_id.get(sid)
        if prev is None or matched > prev[1]:
            by_sys_id[sid] = (key, matched)
    granularity_keys = [v[0] for v in by_sys_id.values()]
    # Worked example first if present, then the rest in stable key order.
    worked_page_id = (d13d["worked_example"] or {}).get("page_id")
    granularity_keys.sort(key=lambda k: (k[0] != worked_page_id, k))
    granularity_pool = granularity_keys[:cap_per_class]
    for pool_idx in _evenly_spaced_indices(len(granularity_pool), class3_spotcheck_cap):
        page_id, s0, s1 = granularity_pool[pool_idx]
        sid = sys_id_from_page_id(page_id)
        cat = libraries.get(sid, {})
        # find the two related work_ids on this exact span
        span_group_works = sorted({
            c["work_id"]
            for c in claims
            if c["page_id"] == page_id and c["span_start"] == s0 and c["span_end"] == s1
        })
        titles = [
            f"{works[w]['neutral_title']} ({w})"
            for w in span_group_works
            if w in works
        ]
        cases.append({
            "class": "granularity",
            "question_type": "identity",
            "sys_id": sid,
            "shelfmark": cat.get("shelfmark", ""),
            "catalogue_text": cat.get("catalogue_text", ""),
            "work_titles": titles,
            "reason": (
                f"Byte-identical span {s0}-{s1} on this page is claimed by {len(span_group_works)} "
                "works sharing the same author and a common title stem -- a title-containment/alias "
                "relationship a plain string comparison cannot resolve on its own (same underlying "
                "commentary at two catalogued granularities, or two genuinely distinct works?)."
            ),
            "proposal": (
                "PROPOSAL (draft, not a label): plausibly `same_work` (the SAME underlying work at two "
                "granularities) -- confirm or correct."
            ),
        })

    # --- Class 2: alias pairs -- IDENTITY spot-check (same restructure) ---
    alias_pairs = select_alias_pair_candidates(works)
    alias_pool = alias_pairs[:cap_per_class]
    for pool_idx in _evenly_spaced_indices(len(alias_pool), class2_spotcheck_cap):
        wa, wb = alias_pool[pool_idx]
        rep = best_claim_for_work(claims, wa["work_id"]) or best_claim_for_work(claims, wb["work_id"])
        sid = rep["sys_id"] if rep else None
        cat = libraries.get(sid, {}) if sid else {}
        cases.append({
            "class": "alias",
            "question_type": "identity",
            "sys_id": sid,
            "shelfmark": cat.get("shelfmark", ""),
            "catalogue_text": cat.get("catalogue_text", ""),
            "work_titles": [
                f"{wa['neutral_title']} ({wa['work_id']}, {wa['source_corpus']})",
                f"{wb['neutral_title']} ({wb['work_id']}, {wb['source_corpus']})",
            ],
            "reason": (
                "Two DIFFERENT work_ids share both the same author and an identical normalized title "
                "(a two-member cluster, not a large generic-collection cluster) -- a likely un-merged "
                "cross-corpus alias/duplicate."
            ),
            "proposal": (
                "PROPOSAL (draft, not a label): plausibly `same_work` (an alias pair, not yet "
                "canonically merged) -- confirm or correct."
            ),
        })

    # --- Class 1: near-miss titles -- IDENTITY spot-check (same restructure) ---
    near_miss = select_near_miss_candidates(works)
    near_miss_pool = near_miss[:cap_per_class]
    for pool_idx in _evenly_spaced_indices(len(near_miss_pool), class1_spotcheck_cap):
        wa, wb, ratio = near_miss_pool[pool_idx]
        rep = best_claim_for_work(claims, wa["work_id"]) or best_claim_for_work(claims, wb["work_id"])
        sid = rep["sys_id"] if rep else None
        cat = libraries.get(sid, {}) if sid else {}
        cases.append({
            "class": "near_miss",
            "question_type": "identity",
            "sys_id": sid,
            "shelfmark": cat.get("shelfmark", ""),
            "catalogue_text": cat.get("catalogue_text", ""),
            "work_titles": [
                f"{wa['neutral_title']} ({wa['work_id']})",
                f"{wb['neutral_title']} ({wb['work_id']})",
            ],
            "reason": (
                f"Same author; normalized titles are {ratio:.1%} similar (SequenceMatcher) but NOT "
                "identical -- genuinely different works (e.g. different books/chapters/parts) whose "
                "titles a string comparison could easily conflate in EITHER direction."
            ),
            "proposal": None,
        })

    # --- Class 4: terse or missing catalogue identification text (owner-
    # authorized scope extension, 136-GATE1-DECISIONS.md item C; EXPANDED
    # 15->class4_cap in the labelling restructure -- see module docstring
    # for the proportioning rationale) -- NOVELTY SHADE question ---
    terse = select_terse_catalogue_candidates(claims, libraries, cap=class4_cap)
    for entry in terse:
        sid = entry["sys_id"]
        c = entry["claim"]
        w = works.get(c["work_id"])
        cat = libraries.get(sid, {})
        cat_text = cat.get("catalogue_text", "")
        title = f"{w['neutral_title']} ({c['work_id']})" if w else c["work_id"]
        if not cat_text:
            reason = (
                "This manuscript's own catalogue identification field is EMPTY -- there is no "
                "catalogue text at all for a title comparison to work with, only the identified "
                "work's title itself."
            )
            display_cat_text = "_(none on file -- explicit marker of absence, not an omission)_"
        else:
            reason = (
                f"This manuscript's own catalogue identification field is only {len(cat_text)} "
                "characters -- too short/generic for a title comparison to have anything meaningful "
                "to compare against."
            )
            display_cat_text = cat_text
        cases.append({
            "class": "terse_catalogue",
            "question_type": "shade",
            "sys_id": sid,
            "shelfmark": cat.get("shelfmark", ""),
            "catalogue_text": display_cat_text,
            "work_titles": [title],
            "reason": reason,
            "proposal": None,
        })

    # --- Class 5: generic collection works (owner-authorized scope
    # extension, 136-GATE1-DECISIONS.md item C; EXPANDED 15->class5_cap in
    # the labelling restructure) -- NOVELTY SHADE question ---
    generic = select_generic_collection_candidates(claims, works, cap=class5_cap)
    for entry in generic:
        author, cluster_title = entry["cluster_key"]
        members = entry["members"]
        c = entry["claim"]
        sid = c["sys_id"]
        cat = libraries.get(sid, {})
        claimed_work = works.get(c["work_id"])
        claimed_title = f"{claimed_work['neutral_title']} ({c['work_id']})" if claimed_work else c["work_id"]
        sibling_ids = sorted({m["work_id"] for m in members} - {c["work_id"]})
        sample_siblings = ", ".join(sibling_ids[:5]) + ("..." if len(sibling_ids) > 5 else "")
        cases.append({
            "class": "generic_collection",
            "question_type": "shade",
            "sys_id": sid,
            "shelfmark": cat.get("shelfmark", ""),
            "catalogue_text": cat.get("catalogue_text", ""),
            "work_titles": [
                f"{claimed_title} -- one of {len(members)} works sharing author {author!r} and "
                f"title stem {cluster_title!r} (siblings incl. {sample_siblings})"
            ],
            "reason": (
                f"This work belongs to a {len(members)}-member same-author/same-title-stem collection "
                "(a generic responsa/piyyut/collection title recurring across many distinct catalogued "
                "items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is "
                "'already recorded' is genuinely ill-defined at the collection level, not merely hard "
                "for a string comparison to settle."
            ),
            "proposal": (
                "PROPOSAL (draft, not a label): a generic collection member -- confirm whether this "
                "specific witness/passage is already recorded, or correct."
            ),
        })

    # --- Class 6: catalogue divergence (owner decision E, 136-GATE1-DECISIONS.md
    # item E; SHADE SPLIT BY SCOPE + a separate correctness axis per owner
    # ruling F, 136-GATE1-DECISIONS.md item F; EXPANDED 15->class6_cap in the
    # labelling restructure). The underlying selector's structured-id-vs-
    # free-text conflation is left DELIBERATELY UNCORRECTED (owner ruling G,
    # § G -- "measure it, do not quietly fix it away"): it over-fires on
    # roughly half of the original 15 candidates, and expanding the pool
    # with the SAME heuristic is expected to keep surfacing that same
    # failure mode, which is itself the measured signal a future hardening
    # pass should be built against. NOVELTY SHADE question, PLUS a
    # Correctness sub-question (see CORRECTNESS_VOCABULARY) -- neither
    # forced by this script; _CLASS6_OWNER_SCOPE only upgrades the PROPOSAL
    # text for the 12 of 15 original candidates the owner has already ruled
    # on directly (still a PROPOSAL, never a label). ---
    divergence = select_catalogue_divergence_candidates(claims, works, libraries, cap=class6_cap)
    for entry in divergence:
        sid = entry["sys_id"]
        cat = libraries.get(sid, {})
        w_claimed = entry["claimed_work"]
        w_divergent = entry["divergent_work"]
        c = entry["claim"]
        claimed_title = f"{w_claimed['neutral_title']} ({c['work_id']})"
        divergent_title = f"{w_divergent['neutral_title']} ({w_divergent['work_id']})"
        owner_scope = _CLASS6_OWNER_SCOPE.get(sid)
        if owner_scope is None:
            proposal = (
                "PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope "
                "not yet distinguished by owner rulings F/G for this specific case) -- the catalogue "
                "and this claim name different works or parts; per ruling G, first check whether the "
                "catalogue's own FREE TEXT already states this claim's identification under a "
                "different spelling/phrasing before confirming a divergence -- confirm the shade, the "
                "scope, and (if divergent) the Correctness call, or correct."
            )
        elif owner_scope["shade"] == "confirms":
            proposal = (
                "PROPOSAL (draft, not a label; owner ruling G -- see 136-GATE1-DECISIONS.md § G): "
                "plausibly `confirms`, NOT a divergence -- the catalogue's own free text "
                f"({owner_scope['free_text_quote']!r}) already states this claim's identification; "
                "only the structured work-id keying differed (this class's own selector over-fired "
                "here) -- confirm or correct."
            )
        else:
            proposal = (
                f"PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): "
                f"plausibly `{owner_scope['shade']}` -- confirm the shade AND supply the separate "
                "Correctness call (catalogue_correct / claim_correct / unclear), or correct."
            )
        cases.append({
            "class": "catalogue_divergence",
            "question_type": "shade",
            "correctness_applicable": True,
            "sys_id": sid,
            "shelfmark": cat.get("shelfmark", ""),
            "catalogue_text": cat.get("catalogue_text", ""),
            "work_titles": [
                f"CLAIMED (this identification): {claimed_title}",
                f"CATALOGUE NAMES (found in the identification text): {divergent_title}",
            ],
            "reason": (
                "This manuscript's own catalogue identification text names a DIFFERENT work "
                f"({w_divergent['neutral_title']!r}) than the one this claim identifies "
                f"({w_claimed['neutral_title']!r}); the two are NOT a granularity variant under the "
                "D-13d author-gated rule (different author, or an unrelated title) -- a genuine "
                "catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into "
                "`diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can "
                "over-fire when the catalogue's free text actually already agrees with the claim under "
                "a different structured key -- see the Vocabulary sheet/table)."
            ),
            "proposal": proposal,
        })

    return cases


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _fmt_pct(numerator: int, denominator: int) -> str:
    if not denominator:
        return "n/a"
    return f"{100.0 * numerator / denominator:.1f}%"


def render_evidence_brief(
    *,
    asset_path: str,
    identifications_total: int,
    main_total: int,
    show_more_total: int,
    reason_counts: Dict[str, int],
    d13e: Dict[str, Any],
    d16: Dict[str, Any],
    d13c: Dict[str, Any],
    d13b: Dict[str, Any],
    d13d: Dict[str, Any],
) -> str:
    lines: List[str] = []
    a = lines.append
    a("# Phase 136 Plan 03 -- Gate-1 Decision Evidence")
    a("")
    a(f"Measured read-only against the deployed asset `{os.path.basename(asset_path)}` "
      "(the LIVE v2 sidecar -- the trimmed rebuild has not run). Re-running "
      "`scripts/discovery_gate1_evidence.py` against the same file reproduces every number below "
      "exactly (no sampling, no randomness).")
    a("")
    a("**Population note.** The main-pool-rule classification below is computed over \"usable\" claims: "
      "a claim's display evidence has `routing_status='shipped'`, OR any evidence row for the claim is "
      "`adjudication_status='human_confirmed'` (the D-13g fix folded in, so a human-confirmed row is "
      "never invisible to this measurement merely because routing demoted it). This reproduces "
      f"**{identifications_total:,}** identifications, main **{main_total:,}** / show-more "
      f"**{show_more_total:,}** -- within ~2% of the design-pass figures already on record in "
      "`main-pool-rule.md` (36,152 / 28,357), which is the expected order of agreement given this pass "
      "implements gate 3 slightly differently (see the D-13e section below) and a slightly different "
      "human-confirmed rule.")
    a("")
    a("Gate breakdown (identification count by classification reason):")
    a("")
    a("| Reason | Bucket | Count |")
    a("|---|---|---|")
    reason_bucket = {
        "human_confirmed_override": "main",
        "multi_folio_agreement": "main",
        "gate4_single_page_full_coverage": "main",
        "gate1_no_same_work_claim": "show_more",
        "gate2_best_band_weak": "show_more",
        "gate3_unresolved_competition": "show_more",
        "gate4_low_single_page_coverage": "show_more",
        "gate4_no_coverage_data": "show_more",
    }
    for reason in sorted(reason_counts):
        a(f"| {reason} | {reason_bucket.get(reason, '?')} | {reason_counts[reason]:,} |")
    a("")

    # --- D-13e ---
    a("## D-13e -- does the middle \"Also shares text with\" bucket survive as a THIRD level?")
    a("")
    a(f"- D-13d generic identical-span-group claims (>=2 DIFFERENT canonical works on one byte-identical "
      f"span): **{d13e['generic_claims_total']:,}**.")
    a(f"  - Of those, **{d13e['generic_not_reachable']:,}** belong to an identification that this pass "
      "classifies MAIN under the main-pool rule -- i.e. NOT otherwise reachable via \"show more matches\" "
      "(that toggle only ever renders show-more-classified identifications).")
    a(f"  - The remaining **{d13e['generic_overlap']:,}** belong to an identification ALREADY classified "
      "show-more -- these rows are already reachable via \"show more matches\"; giving them a second, "
      "separate middle-bucket home would be a duplicate view.")
    a(f"- Related-pages population (D-11a, shared-text page relations -- these never map onto a WORK "
      "identification at all, so they can never be reached via the per-work \"show more matches\" toggle): "
      f"**{d13e['related_pairs']:,}** directed (anchor, opposite) page pairs. All of it is, by "
      "construction, not otherwise reachable.")
    a("")
    a(f"**Total middle-bucket population: {d13e['total_middle_bucket']:,}.** "
      f"**Not otherwise reachable: {d13e['not_reachable_total']:,}** "
      f"({_fmt_pct(d13e['not_reachable_total'], d13e['total_middle_bucket'])} of the middle bucket). "
      f"**Overlap with \"show more matches\": {d13e['overlap_total']:,}** "
      f"({_fmt_pct(d13e['overlap_total'], d13e['total_middle_bucket'])}).")
    a("")
    a("**Methodology note (gate 3):** this pass implements \"unresolved competition\" as EITHER a "
      "`discovery_routing_audit` `kept_tie` page (direct, non-heuristic) OR an overlapping, near-equal-"
      "length competing span from another canonical work on the same page (overlap >= 70% of the shorter "
      "span AND a length ratio >= 0.7 -- a stated, reproducible threshold). `demoted_work_id` is NULL on "
      "every `kept_tie` row in this asset (a known, already-documented flaw -- D-02b), so the audit table "
      "alone cannot fully reconstruct every tie; the near-tie span test is this pass's way of recovering "
      "most of the gap, and is the main source of any remaining difference from the design-pass figures.")
    a("")
    a("**Question for the owner:** given the numbers above, does the panel keep a distinct THIRD "
      "disclosure level (\"Also shares text with\"), or does it collapse into \"more matches\"? No "
      "recommendation is made here -- D-13e is open by design.")
    a("")

    # --- D-16 ---
    a("## D-16 / PANEL-01 -- does the findings page also get the relation filter?")
    a("")
    a("Relation distribution (claim_type), corpus-wide (all claims, any routing status):")
    a("")
    a("| Relation | Count |")
    a("|---|---|")
    for row in d16["corpus_wide"]:
        a(f"| {row['claim_type']} | {row['n']:,} |")
    a("")
    a("Relation distribution restricted to SHIPPED display claims:")
    a("")
    a("| Relation | Count |")
    a("|---|---|")
    for row in d16["shipped"]:
        a(f"| {row['claim_type']} | {row['n']:,} |")
    a("")
    a("Relation distribution restricted to this pass's MAIN pool (identifications classified `main`):")
    a("")
    a("| Relation | Count |")
    a("|---|---|")
    for relation, n in d16["main_pool"].items():
        a(f"| {relation} | {n:,} |")
    a("")
    a("**Question for the owner:** does a relation filter on the main pool meaningfully narrow the "
      "default view, or does it mostly restate the bucket (since the main pool is already "
      "overwhelmingly `direct_witness`)? No recommendation is made here -- D-16 is open by design.")
    a("")

    # --- D-13c ---
    a("## D-13c -- the short-evidence threshold")
    a("")
    a(f"Thinnest shipped direct match in the whole asset: **{d13c['thinnest_direct']} matched letters**.")
    a("")
    a("Cumulative row counts below each candidate threshold:")
    a("")
    a("| Threshold (matched letters) | Direct family (of {:,}) | Propagated family (of {:,}) |".format(
        d13c["direct_total"], d13c["propagated_total"]))
    a("|---|---|---|")
    for t in d13c["thresholds"]:
        dn = d13c["direct_cumulative"][t]
        pn = d13c["propagated_cumulative"][t]
        a(f"| < {t} | {dn:,} ({_fmt_pct(dn, d13c['direct_total'])}) | "
          f"{pn:,} ({_fmt_pct(pn, d13c['propagated_total'])}) |")
    a("")
    a("**Methodology note (propagated family):** the propagated family's length metric is "
      "`aligned_len` on shipped `shared_text` evidence rows (propagated `witness`-kind rows -- "
      "`corroborated`/`weak` -- carry no length field in this asset at all). This is a slightly "
      "different population than an earlier design-pass count (which counted DISPLAY claims rather "
      "than evidence rows); the counting unit is stated here so the two are never silently conflated.")
    a("")
    a(f"Short direct rows (< 150 matched letters) that are nonetheless part of a MAIN identification "
      f"via multi-folio agreement (the honest counter-argument the owner already accepted -- for a "
      f"prayer book, a short liturgical passage may be exactly the correct identification): "
      f"**{d13c['short_in_main_multi_folio']:,}**.")
    a("")
    a("**Question for the owner:** what is the short-evidence threshold, in matched letters? A "
      "defensible default exists: **150** (the figure the owner has already reviewed counts against, "
      "per `main-pool-rule.md` / `136-CONTEXT.md` D-13c) -- kept as the recommended default unless the "
      "table above changes the owner's mind.")
    a("")

    # --- D-13b ---
    a("## D-13b -- the lead-attribution tie-break")
    a("")
    a(f"Identical-span groups (>=2 shipped direct claims on one byte-identical span): "
      f"**{d13b['total_groups']:,}** groups / **{d13b['total_claims']:,}** claims.")
    a(f"Of those, **{d13b['tied_after_band_rank_groups']:,}** groups "
      f"({_fmt_pct(d13b['tied_after_band_rank_groups'], d13b['total_groups'])}) are STILL tied after "
      f"ordering by band rank alone (**{d13b['tied_after_band_rank_claims']:,}** claims involved) -- "
      "band rank alone cannot pick a lead attribution for the overwhelming majority of these groups.")
    a("")
    a("**Question for the owner:** what breaks a tie after band rank when several works claim one "
      "passage? A defensible default exists: fall back to the existing TOTAL claim ordering already "
      "used elsewhere in the build (`discovery_ids.py`'s `evidence_id` lexicographic tie-break) -- "
      "deterministic, already in the codebase, and requires no new concept.")
    a("")

    # --- D-13d ---
    a("## D-13d -- the granularity separation rule (KNOWN FLAW)")
    a("")
    a(f"Identical-span groups with >=2 DIFFERENT canonical works: "
      f"**{d13d['diff_canon_groups_total']:,}** groups / **{d13d['diff_canon_claims_total']:,}** claims.")
    a("")
    we = d13d["worked_example"]
    if we:
        a("**Worked example** (T-S Misc. 12.31.14):")
        a("")
        a(f"- Page: `{we['page_id']}` (sys_id `{we['sys_id']}`)")
        a(f"- Span: offsets {we['span_start']}-{we['span_end']} ({we['matched_letters']} matched letters)")
        for w in we["works"]:
            a(f"  - `{w['work_id']}` (canonical `{w['canonical_work_id']}`): **{w['neutral_title']}** "
              f"-- author: {w['author']}")
        a("")
        a("Both works share the same author and a common title prefix (\"<author> on ...\") -- the same "
          "underlying commentary recorded at two catalogued granularities (a general work covering the "
          "whole Torah, and a specific work covering only Genesis), carrying DIFFERENT `canonical_work_id`s. "
          "Under the CURRENT rule (D-13d as originally stated) this whole pair is swept into the generic "
          "\"also shares text with\" bucket and neither title renders as a stand-alone identification for "
          "this page, even though the two titles denote a real, nameable commentary.")
        a("")
    a("**Proposed separation rule** (display-time only, not a data fix): treat two works in an identical-"
      "span group as the SAME work at different granularity (collapse like a duplicate, per D-13a) when "
      "they share a non-null `author` field AND EITHER their normalized titles are identical (an "
      "undetected alias) OR share a >= 4-character normalized-title prefix (e.g. a common "
      "\"<author> on ...\" commentary marker). Groups where no such pair exists remain genuinely generic "
      "shared text.")
    a("")
    a(f"**Measured effect:** of the {d13d['diff_canon_groups_total']:,} different-canonical-work identical-"
      f"span groups, **{d13d['collapse_candidate_groups']:,}** groups "
      f"({_fmt_pct(d13d['collapse_candidate_groups'], d13d['diff_canon_groups_total'])}, "
      f"{d13d['collapse_candidate_claims']:,} claims) contain a same-author/related-title pair and are "
      f"candidates for the collapse rule; **{d13d['generic_groups']:,}** groups "
      f"({_fmt_pct(d13d['generic_groups'], d13d['diff_canon_groups_total'])}, "
      f"{d13d['generic_claims']:,} claims) contain no such pair and remain genuinely generic shared text "
      "under the proposed rule.")
    a("")
    a("**Question for the owner:** does this separation rule (same author + identical/prefix-shared "
      "normalized title) correctly draw the line? A defensible default exists: adopt it as stated, since "
      "it is conservative (author-gated, so it never over-collapses the large generic-collection-title "
      "clusters measured separately in the novelty hard-case selection below) and directly resolves the "
      "worked example.")
    a("")

    return "\n".join(lines) + "\n"


# Classes 1-3 ask the IDENTITY question (same_work / different_works /
# unsure / skip) -- an owner-authorized labelling restructure (same
# continuation as rulings F/G): these rows compare two of OUR OWN claims and
# have no claim-vs-aid relationship to judge, so decision E's shade
# vocabulary never applied to them coherently in the first place. Classes
# 4-6 ask the NOVELTY SHADE question. See the module docstring and
# 136-GATE1-DECISIONS.md's labelling-restructure note for the full rationale.
_IDENTITY_CLASSES: frozenset = frozenset({"granularity", "alias", "near_miss"})

# Per-class guidance for which of the shade vocabulary's tokens are actually
# plausible answers for that class's kind of hard case (136-03 Task 4 --
# "say plainly, per class, which shades are plausible answers for that
# class"). ONLY meaningful for the three SHADE classes (4-6) -- every case
# can still receive ANY shade, `unsure`, or `skip`; this is a reading aid for
# the owner, never a constraint enforced by this script.
_PLAUSIBLE_SHADES_BY_CLASS: Dict[str, Tuple[str, ...]] = {
    "terse_catalogue": ("fills_gap", "confirms"),
    "generic_collection": ("fills_gap", "confirms", "extends"),
    # F replaces `diverges` with the scope-split pair; G adds `confirms` as
    # a live possibility here (the class's own selector can over-fire a
    # genuine confirms case into a divergence candidate -- see ruling G).
    "catalogue_divergence": (
        "diverges_work", "diverges_part", "aid_more_specific", "refines_granularity", "confirms",
    ),
}

# Per-class guidance for the IDENTITY classes (1-3) -- always the same two
# substantive answers (same_work / different_works) plus unsure/skip; kept
# as a dict (rather than a single shared tuple) so a future class-specific
# note can be added without touching the render/xlsx code that reads it.
_PLAUSIBLE_IDENTITY_BY_CLASS: Dict[str, Tuple[str, ...]] = {
    "granularity": ("same_work", "different_works"),
    "alias": ("same_work", "different_works"),
    "near_miss": ("same_work", "different_works"),
}


_CLASS_TITLES: Dict[str, str] = {
    "granularity": "Class 3 -- catalogue entry naming a different granularity of the same work (IDENTITY spot-check)",
    "alias": "Class 2 -- alias pairs (IDENTITY spot-check)",
    "near_miss": "Class 1 -- near-miss titles (IDENTITY spot-check)",
    "terse_catalogue": "Class 4 -- terse or missing catalogue identification text (NOVELTY SHADE, owner-authorized extension)",
    "generic_collection": "Class 5 -- generic collection works (NOVELTY SHADE, owner-authorized extension)",
    "catalogue_divergence": "Class 6 -- catalogue divergence (NOVELTY SHADE, owner rulings E/E′/F/G)",
}
_CLASS_ORDER: Tuple[str, ...] = (
    "granularity", "alias", "near_miss", "terse_catalogue", "generic_collection",
    "catalogue_divergence",
)


def assign_case_numbers(cases: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Assigns a stable, sequential ``case_num`` to each case in the SAME
    ``_CLASS_ORDER`` / within-class order the Markdown worksheet renders in,
    so 136-NOVELTY-HARDCASES.md and 136-NOVELTY-HARDCASES.xlsx agree
    case-for-case -- both render from this ONE pre-numbered list, never two
    independent iterations that could silently drift apart."""
    by_class: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for c in cases:
        by_class[c["class"]].append(c)
    numbered: List[Dict[str, Any]] = []
    n = 0
    for cls in _CLASS_ORDER:
        for item in by_class.get(cls, []):
            n += 1
            item = dict(item)
            item["case_num"] = n
            numbered.append(item)
    return numbered


def render_hardcases_brief(cases: List[Dict[str, Any]]) -> str:
    """``cases`` must already carry ``case_num`` (see ``assign_case_numbers``)."""
    lines: List[str] = []
    a = lines.append
    identity_total = sum(1 for c in cases if c["class"] in _IDENTITY_CLASSES)
    shade_total = len(cases) - identity_total
    a("# Phase 136 Plan 03 -- Novelty Hard-Case Candidates")
    a("")
    a(f"**{len(cases)} candidates total: {identity_total} IDENTITY spot-check cases (Classes 1-3) + "
      f"{shade_total} NOVELTY SHADE cases (Classes 4-6).** Restructured per an owner-authorized "
      "labelling-restructure ruling (`136-GATE1-DECISIONS.md`, the note appended after items F/G) "
      "made AFTER the owner read the original 97-case worksheet directly: Classes 1-3 compare two of "
      "OUR OWN claims to each other (an A↔B \"is this the same work\" identity judgment) and were "
      "found, on reading the actual cases, to bake \"same work\" into their own selection criterion "
      "(same author + common title stem) -- so full labelling of all 52 is REPLACED by this "
      f"{identity_total}-case SPOT-CHECK that TESTS the constant-answer assumption rather than "
      "building ground truth. Classes 4-6 are where the answer genuinely varies and carry the "
      "NOVELTY SHADE question (a claim-vs-finding-aid judgment); they are EXPANDED from the original "
      f"45 to {shade_total} candidates. Every case in both groups is selected entirely by "
      "string/title/metadata comparison over the works and manuscripts already in the deployed asset "
      "-- **zero model calls, measured cost $0.00**. Any attached draft verdict below is explicitly "
      "marked `PROPOSAL` and is a reading aid only, never a label -- it is NOT filled in by this "
      "script as an owner answer. This worksheet is also emitted as `136-NOVELTY-HARDCASES.xlsx` "
      "(same phase directory, THREE sheets: \"Identity Spot-Check\", \"Novelty Shades\", "
      "\"Vocabulary & Instructions\") for owners who find Hebrew RTL easier to work with in a "
      "spreadsheet; both files render the SAME cases in the SAME order, from the same pre-numbered "
      "case list, so the two agree case-for-case.")
    a("")
    a("## Part A -- IDENTITY spot-check (Classes 1-3)")
    a("")
    a("**Question type: IDENTITY, not a novelty shade.** These rows compare two of OUR OWN claims "
      "(A and B) -- there is no finding aid in this judgment at all. Answer ONE of:")
    a("")
    a("| Answer | Choose this when... |")
    a("|---|---|")
    for token, description in IDENTITY_VOCABULARY:
        a(f"| `{token}` | {description} |")
    a("")
    a("**How to read the result (recorded in `136-GATE1-DECISIONS.md` so the interpretation is fixed "
      "BEFORE the answers come in, not chosen afterward to fit them):** if ALL cases below come back "
      "`same_work`, that is a measured fact and an argument that the D-13d author-gated collapse rule "
      "(currently collapsing only 276 of 1,367 identical-span groups, 20.2% -- see this plan's D-13d "
      "section) is TOO CONSERVATIVE and should collapse more aggressively. If even ONE case comes back "
      "`different_works`, the constant-answer assumption FAILS and the full 52-case pool needs real "
      "labelling after all, not just a spot-check.")
    a("")
    by_class: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for c in cases:
        by_class[c["class"]].append(c)

    for cls in _CLASS_ORDER:
        if cls not in _IDENTITY_CLASSES:
            continue
        items = by_class.get(cls, [])
        a(f"### {_CLASS_TITLES[cls]} ({len(items)} candidates)")
        a("")
        for item in items:
            a(f"#### Case {item['case_num']}")
            a("")
            if item.get("shelfmark"):
                a(f"- **Manuscript:** {item['shelfmark']} (sys_id `{item['sys_id']}`)")
            elif item.get("sys_id"):
                a(f"- **Manuscript:** sys_id `{item['sys_id']}` (no shelfmark on file)")
            else:
                a("- **Manuscript:** (no shipped claim instance found for either work)")
            a(f"- **A vs B:** {' / '.join(item['work_titles'])}")
            if item.get("catalogue_text"):
                a(f"- **Catalogue's own identification text:** {item['catalogue_text']}")
            a(f"- **Why this pair is adversarial to a STRING heuristic:** {item['reason']}")
            if item.get("proposal"):
                a(f"- **{item['proposal']}**")
            a("- **Identity verdict:** _(pending Task 3 -- `same_work` / `different_works` / "
              "`unsure` / `skip`)_")
            a("")

    a("## Part B -- NOVELTY SHADE cases (Classes 4-6)")
    a("")
    a("**Question type: NOVELTY SHADE, a claim-vs-finding-aid judgment (never A↔B identity).** For "
      "EACH case below, answer with the shade that best describes what an enumerable finding aid "
      "(the catalogue's own identification field, bibliography, titles, PGP, FGP, M-source shelfmark "
      "attributions) actually says about THIS fragment and THIS work -- or `unsure` / `skip`. "
      "Amended 2026-08-02 by owner decisions E / E′ / F / G (`136-GATE1-DECISIONS.md` items E, E′, "
      "F, G): the tri-state (`already_recorded` / `not_in_finding_aids` / `unsure`) collapsed "
      "materially different findings into one bucket -- a catalogue CONTRADICTION and a genuine "
      "\"previously unknown\" both scored the same way, a granularity refinement that helps and one "
      "that adds nothing also scored the same way (E′), and a flat wrong-work divergence and a "
      "same-work-wrong-part divergence also scored the same way, with no way to record WHICH SIDE is "
      "actually correct (F) -- so the shade enum now carries NINE values.")
    a("")
    a("| Shade | Choose this when... |")
    a("|---|---|")
    for token, description in SHADE_VOCABULARY:
        a(f"| `{token}` | {description} |")
    a("")
    a("`not_checked` (the fail-closed system default for an unrun/failed/abstained check) is not a "
      "verdict the owner picks directly -- `unsure` is its owner-facing equivalent.")
    a("")
    a("### Correctness (Class 6 ONLY -- answer ONLY if your shade verdict is `diverges_work` or "
      "`diverges_part`, owner ruling F)")
    a("")
    a("A divergence shade records only THAT the aid and the claim disagree, never WHICH SIDE is "
      "right -- the owner's own review of the real cases found BOTH directions occur under the "
      "identical shade. Leave blank / not applicable for every non-divergence shade.")
    a("")
    a("| Correctness | Choose this when... |")
    a("|---|---|")
    for token, description in CORRECTNESS_VOCABULARY:
        a(f"| `{token}` | {description} |")
    a("")

    for cls in _CLASS_ORDER:
        if cls in _IDENTITY_CLASSES:
            continue
        items = by_class.get(cls, [])
        a(f"### {_CLASS_TITLES[cls]} ({len(items)} candidates)")
        a("")
        plausible = _PLAUSIBLE_SHADES_BY_CLASS[cls]
        plausible_str = ", ".join(f"`{s}`" for s in plausible)
        a(f"**Plausible shades for this class:** {plausible_str} (any other shade from the "
          "vocabulary table above is still a valid answer if the case warrants it; `unsure` / "
          "`skip` are always available).")
        a("")
        for item in items:
            a(f"#### Case {item['case_num']}")
            a("")
            if item.get("shelfmark"):
                a(f"- **Manuscript:** {item['shelfmark']} (sys_id `{item['sys_id']}`)")
            elif item.get("sys_id"):
                a(f"- **Manuscript:** sys_id `{item['sys_id']}` (no shelfmark on file)")
            else:
                a("- **Manuscript:** (no shipped claim instance found for either work)")
            a(f"- **Work(s):** {' / '.join(item['work_titles'])}")
            if item.get("catalogue_text"):
                a(f"- **Catalogue's own identification text:** {item['catalogue_text']}")
            a(f"- **Why it is hard:** {item['reason']}")
            if item.get("proposal"):
                a(f"- **{item['proposal']}**")
            a(f"- **Shade verdict:** _(pending Task 3 -- {plausible_str}, any other shade from the "
              "vocabulary table above, or `unsure` / `skip`)_")
            if item.get("correctness_applicable"):
                a("- **Correctness (only if `diverges_work` / `diverges_part` above):** "
                  "_(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if "
                  "not applicable)_")
            a("")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# XLSX labelling workbook (owner-requested, 2026-08-02: Hebrew RTL is hard to
# work with in Markdown). Same ``cases`` data as the Markdown worksheet above
# (pre-numbered via ``assign_case_numbers`` so the two files agree
# case-for-case), reissued via THIS script -- never hand-edited -- per the
# same discipline as the Markdown. openpyxl is an existing project
# dependency (see ``shared/export_dossier.py`` / ``web/export_service.py``
# for the house style this mirrors: ``sheet_view.rightToLeft``, bold-white-
# on-blue header row, ``sanitize_text_for_excel`` on every cell).
#
# MASKING NOTE (measured during this plan): ``.xlsx`` is a ZIP archive whose
# inner XML parts are DEFLATE-compressed by default -- a raw byte-level scan
# of the OUTER file (e.g. a naive ``check_atlas_masking.py --scan-asset`` on
# the ``.xlsx`` path) cannot see a literal string that is only present in the
# compressed inner XML. The masking scan for this artifact must be run
# against the DECOMPRESSED inner content (e.g. via ``zipfile`` extraction
# into a scratch file passed as a single explicit ``--scan-asset`` path) --
# see ``136-GATE1-DECISIONS.md``'s "Outstanding (pending Task 3)" section for
# the full methodology note. This module does not perform that extraction
# itself (it stays a lightweight read/write path, mirroring this file's own
# stated reason for not importing the masking-gate module directly); the
# caller is expected to do it before presenting the workbook to the owner.
# ---------------------------------------------------------------------------

def write_hardcases_xlsx(cases: List[Dict[str, Any]], path: str) -> None:
    """``cases`` must already carry ``case_num`` (see ``assign_case_numbers``).

    Writes a THREE-sheet workbook (owner-authorized labelling restructure,
    same continuation as rulings F/G -- each sheet carries the ONE question
    type the class was actually built to support):

    - "Identity Spot-Check" (Classes 1-3, the ~8-case A↔B spot-check): an
      Identity dropdown (`same_work` / `different_works` / `unsure` / `skip`).
    - "Novelty Shades" (Classes 4-6, ~75 cases): a Verdict dropdown (the full
      nine-shade vocabulary) PLUS a Correctness dropdown (owner ruling F --
      `catalogue_correct` / `claim_correct` / `unclear`, meaningful only on
      `diverges_work` / `diverges_part` rows; blank elsewhere).
    - "Vocabulary & Instructions" (both vocabularies + per-class hints + the
      "blank is not a label" note).

    Case #s are GLOBAL across both data sheets (assigned once by
    ``assign_case_numbers``, never renumbered per-sheet), so Task 4's
    round-trip can locate a case by number regardless of which sheet it came
    from. Deterministic at the cell-value / validation-list / sheet-structure
    level -- NOT claimed byte-for-byte on the saved ``.xlsx`` file itself,
    since openpyxl embeds a save timestamp in the workbook's
    ``docProps/core.xml`` on every save.
    """
    # Deferred heavy import (mirrors scripts/bench_discovery.py's own
    # documented pattern): keeps this module importable/usable via --help
    # and --no-write without an openpyxl dependency on those paths.
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter
    from openpyxl.worksheet.datavalidation import DataValidation

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from shared.export_utils import sanitize_text_for_excel as _san

    def _s(value: Optional[str]) -> str:
        return _san(value or "")

    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
    header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    wrap_top = Alignment(horizontal="right", vertical="top", wrap_text=True, readingOrder=2)
    wrap_top_center = Alignment(horizontal="center", vertical="top", wrap_text=True)

    identity_cases = [c for c in cases if c["class"] in _IDENTITY_CLASSES]
    shade_cases = [c for c in cases if c["class"] not in _IDENTITY_CLASSES]

    wb = Workbook()

    def _manuscript_str(item: Dict[str, Any]) -> str:
        return (
            item["shelfmark"] if item.get("shelfmark")
            else (f"sys_id {item['sys_id']} (no shelfmark on file)" if item.get("sys_id")
                  else "(no shipped claim instance found for either work)")
        )

    def _apply_header(ws, headers: List[str]) -> None:
        ws.append(headers)
        for col_idx in range(1, len(headers) + 1):
            cell = ws.cell(row=1, column=col_idx)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_align

    def _apply_body_alignment(ws, n_cols: int) -> None:
        last_row = ws.max_row
        for row_idx in range(2, last_row + 1):
            ws.cell(row=row_idx, column=1).alignment = wrap_top_center
            for col_idx in range(2, n_cols + 1):
                ws.cell(row=row_idx, column=col_idx).alignment = wrap_top

    # --- Sheet 1: Identity Spot-Check (Classes 1-3) ---
    ws1 = wb.active
    ws1.title = "Identity Spot-Check"
    ws1.sheet_view.rightToLeft = True
    headers1 = [
        "Case #", "Identity verdict", "Class",
        "Manuscript", "sys_id", "A vs B (claim pair)",
        "Catalogue's own identification text", "Why adversarial to a string heuristic",
        "PROPOSAL (draft -- NOT a label)",
    ]
    _apply_header(ws1, headers1)
    widths1 = {"A": 9, "B": 20, "C": 40, "D": 32, "E": 22, "F": 48, "G": 46, "H": 55, "I": 46}
    for col_letter, width in widths1.items():
        ws1.column_dimensions[col_letter].width = width
    for item in identity_cases:
        ws1.append([
            item["case_num"],
            "",  # Identity verdict -- left blank; owner fills in
            _s(_CLASS_TITLES[item["class"]]),
            _s(_manuscript_str(item)),
            _s(item.get("sys_id")),
            _s(" / ".join(item["work_titles"])),
            _s(item.get("catalogue_text")),
            _s(item["reason"]),
            _s(item.get("proposal")),
        ])
    _apply_body_alignment(ws1, len(headers1))
    last_row1 = ws1.max_row
    ws1.freeze_panes = "C2"
    ws1.auto_filter.ref = f"A1:{get_column_letter(len(headers1))}{last_row1}"
    dv_identity = DataValidation(
        type="list", formula1='"' + ",".join(IDENTITY_TOKENS) + '"',
        allow_blank=True, showErrorMessage=True,
        errorTitle="Invalid identity verdict",
        error="Choose 'same_work', 'different_works', 'unsure' or 'skip'. Free text is rejected.",
        promptTitle="Identity verdict",
        prompt="Are A and B the same underlying work? Blank = not yet answered.",
    )
    ws1.add_data_validation(dv_identity)
    if last_row1 >= 2:
        dv_identity.add(f"B2:B{last_row1}")

    # --- Sheet 2: Novelty Shades (Classes 4-6) ---
    ws2 = wb.create_sheet(title="Novelty Shades")
    ws2.sheet_view.rightToLeft = True
    headers2 = [
        "Case #", "Shade verdict", "Correctness (diverges_work/diverges_part only)", "Class",
        "Plausible shades", "Manuscript", "sys_id", "Claimed work(s)",
        "Catalogue's own identification text", "Why it is hard",
        "PROPOSAL (draft -- NOT a label)",
    ]
    _apply_header(ws2, headers2)
    widths2 = {
        "A": 9, "B": 20, "C": 30, "D": 46, "E": 30, "F": 32, "G": 22, "H": 48, "I": 46, "J": 55, "K": 46,
    }
    for col_letter, width in widths2.items():
        ws2.column_dimensions[col_letter].width = width
    for item in shade_cases:
        ws2.append([
            item["case_num"],
            "",  # Shade verdict -- left blank; owner fills in
            "",  # Correctness -- left blank; only meaningful on divergence rows
            _s(_CLASS_TITLES[item["class"]]),
            _s(", ".join(_PLAUSIBLE_SHADES_BY_CLASS[item["class"]])),
            _s(_manuscript_str(item)),
            _s(item.get("sys_id")),
            _s(" / ".join(item["work_titles"])),
            _s(item.get("catalogue_text")),
            _s(item["reason"]),
            _s(item.get("proposal")),
        ])
    _apply_body_alignment(ws2, len(headers2))
    last_row2 = ws2.max_row
    ws2.freeze_panes = "D2"
    ws2.auto_filter.ref = f"A1:{get_column_letter(len(headers2))}{last_row2}"

    # Shade Verdict dropdown -- the FULL vocabulary (eight real shades +
    # `unsure` + `skip`, i.e. every token in SHADE_VOCABULARY -- ten total).
    # Blank is allowed ("not yet answered" is a legitimate transient state);
    # the Vocabulary sheet explains a blank is NOT a label.
    dv_shade = DataValidation(
        type="list", formula1='"' + ",".join(SHADE_TOKENS) + '"',
        allow_blank=True, showErrorMessage=True,
        errorTitle="Invalid verdict",
        error="Choose one of the listed shades, or 'unsure' / 'skip'. Free text is rejected.",
        promptTitle="Shade verdict",
        prompt="Pick the shade that best fits, or 'unsure' / 'skip'. Blank = not yet answered.",
    )
    ws2.add_data_validation(dv_shade)
    if last_row2 >= 2:
        dv_shade.add(f"B2:B{last_row2}")

    # Correctness dropdown (owner ruling F) -- applied to EVERY row for
    # simplicity (Excel data validation is range-based, not conditional on
    # another cell's value); the Vocabulary sheet + column header explain it
    # is meaningful ONLY on diverges_work/diverges_part rows and should stay
    # blank otherwise.
    dv_correctness = DataValidation(
        type="list", formula1='"' + ",".join(CORRECTNESS_TOKENS) + '"',
        allow_blank=True, showErrorMessage=True,
        errorTitle="Invalid correctness call",
        error="Choose 'catalogue_correct', 'claim_correct' or 'unclear', or leave blank.",
        promptTitle="Correctness (divergence rows only)",
        prompt="Only answer if the Shade verdict is diverges_work or diverges_part. Otherwise leave blank.",
    )
    ws2.add_data_validation(dv_correctness)
    if last_row2 >= 2:
        dv_correctness.add(f"C2:C{last_row2}")

    # --- Sheet 3: Vocabulary & Instructions ---
    ws3 = wb.create_sheet(title="Vocabulary & Instructions")
    ws3.sheet_view.rightToLeft = True
    ws3.column_dimensions["A"].width = 24
    ws3.column_dimensions["B"].width = 90

    def _note(text: str) -> None:
        ws3.append([_s(text)])
        ws3.cell(row=ws3.max_row, column=1).alignment = Alignment(
            horizontal="right", vertical="top", wrap_text=True, readingOrder=2
        )
        ws3.merge_cells(start_row=ws3.max_row, start_column=1, end_row=ws3.max_row, end_column=2)

    def _table_header(a_text: str, b_text: str) -> None:
        ws3.append([a_text, b_text])
        ws3.cell(row=ws3.max_row, column=1).font = header_font
        ws3.cell(row=ws3.max_row, column=2).font = header_font
        ws3.cell(row=ws3.max_row, column=1).fill = header_fill
        ws3.cell(row=ws3.max_row, column=2).fill = header_fill

    def _table_row(a_text: str, b_text: str) -> None:
        ws3.append([a_text, _s(b_text)])
        ws3.cell(row=ws3.max_row, column=1).alignment = Alignment(
            horizontal="right", vertical="top", wrap_text=True, readingOrder=2
        )
        ws3.cell(row=ws3.max_row, column=2).alignment = Alignment(
            horizontal="right", vertical="top", wrap_text=True, readingOrder=2
        )

    _note(
        "This workbook is a labelling instrument for the novelty hard-case evaluation set "
        "(plan 136-03 Task 3). The Markdown file 136-NOVELTY-HARDCASES.md in the same phase "
        f"directory remains the authoritative human-readable record of these {len(cases)} candidate "
        "cases and the reasoning for why each is hard; this workbook renders the SAME cases, in the "
        "SAME order, from one shared pre-numbered case list, split across two data sheets by the "
        "question type each case was actually built to support."
    )
    ws3.append([])
    _note(
        "\"Identity Spot-Check\" (Classes 1-3): these rows compare two of OUR OWN claims to each "
        "other -- an A vs B \"same underlying work?\" judgment, no finding aid involved at all. "
        "\"Novelty Shades\" (Classes 4-6): these rows judge ONE claim against the finding aids -- the "
        "question genuinely varies here, which is why it carries the fuller shade vocabulary plus a "
        "Correctness column for divergence rows."
    )
    ws3.append([])
    _note(
        "IMPORTANT: a BLANK verdict cell (Identity OR Shade) is NOT a label -- it means \"not yet "
        "answered\". If you cannot judge a case, enter `unsure` explicitly (it costs nothing and is a "
        "real, useful answer) rather than leaving the cell blank. If you choose not to judge a case at "
        "all, enter `skip` explicitly -- it is recorded as skipped, never silently filled from the "
        "case's own draft PROPOSAL. The Correctness column is meaningful ONLY when the Shade verdict "
        "is `diverges_work` or `diverges_part` -- leave it blank on every other row."
    )
    ws3.append([])

    _table_header("Identity answer", "Choose this when...")
    for token, description in IDENTITY_VOCABULARY:
        _table_row(token, description)
    ws3.append([])

    _table_header("Shade", "Choose this when...")
    for token, description in SHADE_VOCABULARY:
        _table_row(token, description)
    ws3.append([])
    _note(
        "`not_checked` (the fail-closed system default for an unrun/failed/abstained check) is not "
        "a verdict you pick directly -- `unsure` is its owner-facing equivalent."
    )
    ws3.append([])

    _table_header("Correctness (divergence rows only)", "Choose this when...")
    for token, description in CORRECTNESS_VOCABULARY:
        _table_row(token, description)
    ws3.append([])

    _table_header("Class", "Plausible answers (a reading aid -- any answer above is still valid)")
    for cls in _CLASS_ORDER:
        if cls in _IDENTITY_CLASSES:
            plausible_str = ", ".join(_PLAUSIBLE_IDENTITY_BY_CLASS[cls])
        else:
            plausible_str = ", ".join(_PLAUSIBLE_SHADES_BY_CLASS[cls])
        _table_row(_CLASS_TITLES[cls], plausible_str)

    os.makedirs(os.path.dirname(path), exist_ok=True)
    wb.save(path)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Phase 136 plan 03 -- gate-1 decision evidence + novelty hard-case candidates "
                    "(read-only measurement over the deployed discovery sidecar)."
    )
    parser.add_argument("asset", help="path to the discovery-v1-*.db sidecar to measure")
    parser.add_argument(
        "--research-db", default=DEFAULT_RESEARCH_DB,
        help="path to the (gitignored, local-only) fullcorpus.db research DB used for the D-13c/gate-4 "
             "page-coverage lookup (default: same_work_spike/probe/data/fullcorpus.db)",
    )
    parser.add_argument(
        "--libraries-csv", default=DEFAULT_LIBRARIES_CSV,
        help="path to libraries.csv (public catalogue metadata for hard-case manuscripts)",
    )
    parser.add_argument("--evidence-out", default=DEFAULT_EVIDENCE_OUT)
    parser.add_argument("--hardcases-out", default=DEFAULT_HARDCASES_OUT)
    parser.add_argument("--hardcases-xlsx-out", default=DEFAULT_HARDCASES_XLSX_OUT)
    parser.add_argument(
        "--no-write", action="store_true",
        help="print console tables only; do not write the Markdown/XLSX artifacts",
    )
    args = parser.parse_args(argv)

    ledger = NonzeroLedger()

    conn = connect_readonly(args.asset)
    try:
        works = load_works(conn)
        ledger.check("works", len(works))

        claims = load_claims(conn)
        ledger.check("claims", len(claims))

        human_confirmed_claim_ids = load_human_confirmed_claim_ids(conn)
        ledger.check("human_confirmed_claim_ids", len(human_confirmed_claim_ids))

        kept_tie_pages = load_kept_tie_pages(conn, works)
        ledger.check("kept_tie_pages", len(kept_tie_pages))

        near_tie_pages = build_near_tie_competition(claims, works)
        ledger.check("near_tie_pages", len(near_tie_pages))

        needed_pages = pages_needing_coverage(
            claims, works, human_confirmed_claim_ids, kept_tie_pages, near_tie_pages
        )
        ledger.check("pages_needing_coverage", len(needed_pages))
        page_norm_letters = load_page_norm_letters(args.research_db, needed_pages)
        ledger.check("page_norm_letters_resolved", len(page_norm_letters))

        identifications, classification = classify_identifications(
            claims, works, human_confirmed_claim_ids, kept_tie_pages, near_tie_pages, page_norm_letters
        )
        ledger.check("identifications", len(identifications))

        # Fold canonical_work_id back onto each claim dict for the D-16/D-13c
        # main-pool lookups below (classify_identifications built its own
        # copies internally; re-derive here once, cheaply, via `works`).
        for c in claims:
            w = works.get(c["work_id"])
            c["canonical_work_id"] = w["canonical_work_id"] if w else None

        main_total = sum(1 for b, _ in classification.values() if b == "main")
        show_more_total = sum(1 for b, _ in classification.values() if b == "show_more")
        ledger.check("main_total", main_total)
        ledger.check("show_more_total", show_more_total)
        reason_counts: Dict[str, int] = defaultdict(int)
        for _bucket, reason in classification.values():
            reason_counts[reason] += 1

        span_groups = load_identical_span_groups(conn, works)
        ledger.check("span_groups", len(span_groups))

        d13e = compute_d13e(conn, span_groups, classification)
        ledger.check("d13e_total_middle_bucket", d13e["total_middle_bucket"])

        d16 = compute_d16(conn, claims, classification)
        ledger.check("d16_corpus_wide_rows", len(d16["corpus_wide"]))

        d13c = compute_d13c(conn, classification, claims)
        ledger.check("d13c_direct_total", d13c["direct_total"])
        ledger.check("d13c_propagated_total", d13c["propagated_total"])

        d13b = compute_d13b(span_groups)
        ledger.check("d13b_total_groups", d13b["total_groups"])

        d13d = compute_d13d(span_groups, works)
        ledger.check("d13d_diff_canon_groups_total", d13d["diff_canon_groups_total"])

        libraries = load_libraries_csv(args.libraries_csv)
        ledger.check("libraries_csv_rows", len(libraries))

        hardcases = build_hardcases(claims, works, d13d, libraries)
        ledger.check("hardcases", len(hardcases))
        ledger.check("hardcases_class1_near_miss_spotcheck", sum(1 for c in hardcases if c["class"] == "near_miss"))
        ledger.check("hardcases_class2_alias_spotcheck", sum(1 for c in hardcases if c["class"] == "alias"))
        ledger.check("hardcases_class3_granularity_spotcheck", sum(1 for c in hardcases if c["class"] == "granularity"))
        ledger.check("hardcases_class4_terse_catalogue", sum(1 for c in hardcases if c["class"] == "terse_catalogue"))
        ledger.check("hardcases_class5_generic_collection", sum(1 for c in hardcases if c["class"] == "generic_collection"))
        ledger.check("hardcases_class6_catalogue_divergence", sum(1 for c in hardcases if c["class"] == "catalogue_divergence"))
        ledger.check("hardcases_identity_total", sum(1 for c in hardcases if c["class"] in _IDENTITY_CLASSES))
        ledger.check("hardcases_shade_total", sum(1 for c in hardcases if c["class"] not in _IDENTITY_CLASSES))
        # Acceptance criteria: any attached draft verdict MUST be explicitly
        # marked PROPOSAL and separable from an owner's answer -- assert it
        # on every case that carries one (a case may also carry none at all,
        # e.g. the near-miss class below, which is a valid choice per the
        # plan's "MAY attach a draft" wording).
        for case in hardcases:
            proposal = case.get("proposal")
            if proposal is not None and not proposal.startswith("PROPOSAL"):
                raise AssertionError(
                    f"hard-case draft verdict is not marked PROPOSAL: {proposal!r}"
                )

        # Assign stable case numbers ONCE, in the same class order both
        # rendered artifacts (Markdown + XLSX) share -- see assign_case_numbers.
        hardcases = assign_case_numbers(hardcases)
        ledger.check("hardcases_numbered", len(hardcases))

    finally:
        conn.close()

    # ---- console summary ----
    print("=" * 78)
    print("Discovery gate-1 evidence (Phase 136 plan 03)")
    print("=" * 78)
    print(f"asset            : {args.asset}")
    print(f"identifications  : {len(identifications):,}  (main {main_total:,} / show-more {show_more_total:,})")
    print(f"span-groups      : {len(span_groups):,}")
    print(f"D-13e middle pop : {d13e['total_middle_bucket']:,} "
          f"(not-reachable {d13e['not_reachable_total']:,} / overlap {d13e['overlap_total']:,})")
    print(f"D-13b tied groups: {d13b['tied_after_band_rank_groups']:,} of {d13b['total_groups']:,}")
    print(f"D-13d collapse   : {d13d['collapse_candidate_groups']:,} of {d13d['diff_canon_groups_total']:,}")
    print(f"D-13c thinnest   : {d13c['thinnest_direct']} matched letters")
    print(f"hard-cases       : {len(hardcases):,}")
    print("=" * 78)

    if not ledger.ok():
        print(
            "FAIL: the following measurements returned an unexpected ZERO "
            f"(never presenting a silent zero as a finding): {ledger.failures}",
            file=sys.stderr,
        )
        return 1

    if not args.no_write:
        evidence_md = render_evidence_brief(
            asset_path=args.asset,
            identifications_total=len(identifications),
            main_total=main_total,
            show_more_total=show_more_total,
            reason_counts=dict(reason_counts),
            d13e=d13e,
            d16=d16,
            d13c=d13c,
            d13b=d13b,
            d13d=d13d,
        )
        os.makedirs(os.path.dirname(args.evidence_out), exist_ok=True)
        with open(args.evidence_out, "w", encoding="utf-8") as fh:
            fh.write(evidence_md)
        print(f"wrote {args.evidence_out}")

        hardcases_md = render_hardcases_brief(hardcases)
        os.makedirs(os.path.dirname(args.hardcases_out), exist_ok=True)
        with open(args.hardcases_out, "w", encoding="utf-8") as fh:
            fh.write(hardcases_md)
        print(f"wrote {args.hardcases_out}")

        write_hardcases_xlsx(hardcases, args.hardcases_xlsx_out)
        print(f"wrote {args.hardcases_xlsx_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
