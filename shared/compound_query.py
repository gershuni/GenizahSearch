"""Composition Search conditions: the model and the judgement.

A compound query is the main text (searched as today, letter-level) plus up to
four CONDITIONS. Each condition is a text in one or more versions (any one
counts), a requirement -- must appear / must not appear -- and a place relative
to each page the main text matched: the same image, an adjacent image (P+-1 in
the same manuscript and image set), or anywhere in the same manuscript.
Design, decisions and review: docs/plans/COMPOSITION_CONDITIONS_PLAN.md.

This module is pure: no index, no Qt. It parses page identities, says which
pages a condition must be searched over, and judges every main-text page from
the condition searches' verified hits. The searching itself is
`shared.passage_parallels.PassageSearcher.search_condition`.

What a judgement means:

    condition         search complete         search cut / stopped / failed
    must, found       satisfied               satisfied (a verified match stands)
    must, absent      FAILED (page dropped)   UNRESOLVED
    must not, found   REMOVED                 REMOVED
    must not, absent  satisfied               UNRESOLVED

A page is DROPPED if any condition failed, else REMOVED if any condition
removed it, else UNRESOLVED if any condition is unresolved, else MAIN. "Absent"
means absent from the searchable pages (owner ruling 2026-10-14): pages the
index left out are not searched, and the caller reports how many there were.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from shared.passage_normalize import norm_stream_fast
from shared.sys_id_patterns import ANY_SYS_ID_RE

REQ_MUST = 'must'
REQ_MUST_NOT = 'must_not'
REQUIREMENTS = (REQ_MUST, REQ_MUST_NOT)

WHERE_SAME_IMAGE = 'same_image'
WHERE_ADJACENT = 'adjacent_image'
WHERE_MANUSCRIPT = 'manuscript'
PLACES = (WHERE_SAME_IMAGE, WHERE_ADJACENT, WHERE_MANUSCRIPT)

MAX_CONDITIONS = 4
MAX_VERSIONS = 10
MAX_SEARCHES = 25           # main-text witnesses + condition versions
MAX_VERSION_LETTERS = 10_000

# "Own text" (owner ruling 2026-10-14): a must_not condition's quoting text
# often quotes the main text itself (the Rif quoting the Talmud), so a page
# must not be removed merely because the quoting text matched there -- only
# when the quoting text contributes letters of its OWN. See own_text_runs.
OWN_TEXT_MIN = 40          # minimum contiguous uncovered run, page letters
EDGE_ALLOWANCE = 12        # HTR edge jitter: widens every main-text window
                           # by this many letters on each side before it is
                           # subtracted from the quoting text's coverage.

# Per page, per condition.
SATISFIED = 'satisfied'
FAILED = 'failed'
REMOVED = 'removed'
UNRESOLVED = 'unresolved'
STATES = (SATISFIED, FAILED, REMOVED, UNRESOLVED)

# Where a page goes.
CAT_MAIN = 'main'
CAT_REMOVED = 'removed'
CAT_UNRESOLVED = 'unresolved'
CAT_DROPPED = 'dropped'
CATEGORIES = (CAT_MAIN, CAT_REMOVED, CAT_UNRESOLVED, CAT_DROPPED)

# Why a version's search does not prove absence.
CUT_NONE = ''
CUT_CANDIDATES = 'candidates_truncated'
CUT_VERIFY = 'verify_truncated'
CUT_BELOW_FLOOR = 'below_floor'
CUT_STOPPED = 'stopped'
CUT_FAILED = 'failed'
CUT_NOT_RUN = 'not_run'
CUT_UNIDENTIFIED = 'unidentified_page'
CUT_REASONS = (CUT_CANDIDATES, CUT_VERIFY, CUT_BELOW_FLOOR, CUT_STOPPED,
               CUT_FAILED, CUT_NOT_RUN, CUT_UNIDENTIFIED)


# -- page identity -----------------------------------------------------------

@dataclass(frozen=True, order=True)
class PageKey:
    """One image: manuscript (sys_id), image set (IE, or F for a LOCAL file)
    and page number P."""
    sys_id: str
    ie: str
    p: int

    def neighbours(self) -> tuple:
        return (PageKey(self.sys_id, self.ie, self.p - 1),
                PageKey(self.sys_id, self.ie, self.p + 1))


_IE_RE = re.compile(r'(IE\d+)')
_LOCAL_F_RE = re.compile(r'_(F\d{3,5})(?!\d)')
_P_RE = re.compile(r'_P(\d+)(?=_|$)')


def parse_page_key(header: str) -> Optional[PageKey]:
    """The page identity in a record id / row `raw_header`, or None.

    Corpus ids are `{sys_id}_IE{n}_P{######}_FL{n}`; LOCAL ("My Library")
    headers are `{97...}_LOCAL_P{n}_F{nnnn}`, where the F number is the file
    and plays the IE's part. Order-agnostic, like
    MetadataManager.parse_full_id_components."""
    if not header:
        return None
    s = str(header)
    m_sys = ANY_SYS_ID_RE.search(s)
    m_p = _P_RE.search(s)
    if not m_sys or not m_p:
        return None
    m_ie = _IE_RE.search(s) or _LOCAL_F_RE.search(s)
    if not m_ie:
        return None
    return PageKey(m_sys.group(1), m_ie.group(1), int(m_p.group(1)))


# -- the condition -----------------------------------------------------------

def split_versions(text: str) -> list:
    """Versions are separated by a blank line (the Witnesses dialog's rule)."""
    parts = re.split(r'\n\s*\n', (text or '').replace('\r\n', '\n'))
    return [p.strip() for p in parts if p.strip()]


@dataclass(frozen=True)
class Condition:
    requirement: str
    where: str
    versions: tuple
    label: str = ''

    def to_dict(self) -> dict:
        return {'requirement': self.requirement, 'where': self.where,
                'versions': list(self.versions), 'label': self.label}

    @classmethod
    def from_dict(cls, d) -> 'Condition':
        """Strict: anything malformed raises ValueError (a restored query is
        never half-applied)."""
        if not isinstance(d, dict):
            raise ValueError('condition is not a mapping')
        req, where = d.get('requirement'), d.get('where')
        versions, label = d.get('versions'), d.get('label', '')
        if req not in REQUIREMENTS:
            raise ValueError(f'unknown requirement {req!r}')
        if where not in PLACES:
            raise ValueError(f'unknown place {where!r}')
        if (not isinstance(versions, list) or not versions
                or not all(isinstance(v, str) and v.strip() for v in versions)):
            raise ValueError('versions must be a non-empty list of texts')
        if not isinstance(label, str):
            raise ValueError('label must be text')
        return cls(req, where, tuple(versions), label)


@dataclass(frozen=True)
class Problem:
    """A reason the query cannot run. `condition` is 0-based, or None."""
    code: str
    condition: Optional[int] = None
    version: Optional[int] = None
    detail: int = 0


def validate(conditions: Sequence[Condition], main_searches: int,
             min_span: int) -> list:
    """Every reason this compound query must not run; empty means it may.

    `main_searches` is the number of main-text searches (1, or the witness
    count); `min_span` the active length floor in letters."""
    problems = []
    if len(conditions) > MAX_CONDITIONS:
        problems.append(Problem('too_many_conditions', detail=len(conditions)))
    total = int(main_searches)
    for ci, cond in enumerate(conditions):
        if cond.requirement not in REQUIREMENTS:
            problems.append(Problem('bad_requirement', ci))
        if cond.where not in PLACES:
            problems.append(Problem('bad_place', ci))
        if not cond.versions:
            problems.append(Problem('no_text', ci))
        if len(cond.versions) > MAX_VERSIONS:
            problems.append(Problem('too_many_versions', ci, detail=len(cond.versions)))
        total += len(cond.versions)
        for vi, text in enumerate(cond.versions):
            letters = len(norm_stream_fast(text or ''))
            if letters < min_span:
                problems.append(Problem('version_too_short', ci, vi, letters))
            elif letters > MAX_VERSION_LETTERS:
                problems.append(Problem('version_too_long', ci, vi, letters))
    if total > MAX_SEARCHES:
        problems.append(Problem('too_many_searches', detail=total))
    return problems


# -- what a condition is searched over ---------------------------------------

def search_scope(where: str, main_keys: Iterable[PageKey]):
    """(pages, sys_ids) a condition must be searched over, for all the main
    text's pages: the pages themselves, their neighbours (literal P+-1, never
    "the next record present"), or every page of their manuscripts."""
    keys = {k for k in main_keys if k is not None}
    if where == WHERE_SAME_IMAGE:
        return {(k.sys_id, k.ie, k.p) for k in keys}, set()
    if where == WHERE_ADJACENT:
        return {(n.sys_id, n.ie, n.p) for k in keys for n in k.neighbours()
                if n.p >= 0}, set()
    if where == WHERE_MANUSCRIPT:
        return set(), {k.sys_id for k in keys}
    raise ValueError(f'unknown place {where!r}')


def protection_scopes(conditions: Sequence[Condition],
                      main_keys: Iterable[PageKey]) -> dict:
    """{where: (pages, sys_ids)} for every DISTINCT scope among the
    must_not conditions -- what the "own text" rule's protection search (the
    main text as typed, never a witness) must be searched over, once per
    scope, shared across every must_not condition that scope applies to
    (owner, 2026-10-14; docs/plans/COMPOSITION_CONDITIONS_PLAN.md section
    12). Pure scope planning, nothing about the search call itself -- shared
    between the desktop worker (desktop/gui_threads.py::
    CompoundCompositionThread.run, which adds leases, Stop and generation
    checks around the actual search) and the Qt-free gate
    (scripts/verify_compound_conditions.py::run_compound) so the two cannot
    drift apart on WHICH pages get searched."""
    wheres = sorted({c.where for c in conditions if c.requirement == REQ_MUST_NOT})
    return {where: search_scope(where, main_keys) for where in wheres}


def seed_protection_windows(rows: Iterable[dict]) -> dict:
    """{record_id: windows} from the main text's own matched-core windows on
    its OWN result rows (`page_windows`, shared.passage_parallels) -- half of
    a must_not page's protection union (see must_not_version_search's
    `seed_windows`). A row without the field (older code, or a row from
    somewhere other than `search_composition_logic`) is simply skipped, not
    an error: this is additive, and most rows have no `page_windows` at
    all."""
    out: dict = {}
    for row in rows:
        wins = row.get('page_windows')
        if not wins:
            continue
        rec_id = row.get('raw_header')
        if not rec_id:
            continue
        out[rec_id] = out.get(rec_id, ()) + tuple(wins)
    return out


# -- searching results in, judgements out ------------------------------------

@dataclass(frozen=True)
class VersionSearch:
    """What one version's search established: its verified hits and, when it
    does not prove absence, why (a CUT_* reason; CUT_NONE when complete)."""
    hit_record_ids: tuple = ()
    cut: str = CUT_NONE


def cut_reason(report, *, stopped: bool = False, failed: bool = False) -> str:
    """Why a search with this QueryReport (object or as_dict()) does not prove
    absence, or CUT_NONE. A stop or failure outranks the report."""
    if failed:
        return CUT_FAILED
    if stopped:
        return CUT_STOPPED
    get = report.get if isinstance(report, dict) else (lambda k: getattr(report, k, None))
    if get('below_min_span_query'):
        return CUT_BELOW_FLOOR
    if get('candidates_truncated'):
        return CUT_CANDIDATES
    if get('verify_truncated'):
        return CUT_VERIFY
    return CUT_NONE


# -- must_not "own text": the quoting work's own letters, not the main text's -

def _merge_windows(windows: Iterable) -> list:
    """Sorted, merged (lo, hi) half-open intervals -- touching or
    overlapping windows become one. `windows` holds (r0, r1) pairs exactly
    as `shared.passage_search.PassageHit.windows` carries them (page-letter
    offsets, UNMERGED -- never the coarser `spans`/`hit_spans`)."""
    ivs = sorted((int(lo), int(hi)) for lo, hi in windows if hi > lo)
    merged: list = []
    for lo, hi in ivs:
        if merged and lo <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
        else:
            merged.append((lo, hi))
    return merged


def _subtract_windows(covered: list, protected: list) -> list:
    """`covered` with every `protected` interval removed. Both are already
    merged, sorted and disjoint. O(len(covered) * len(protected)) -- the
    window counts on one page are a handful at most, never a hot path."""
    frags = list(covered)
    for p_lo, p_hi in protected:
        nxt = []
        for lo, hi in frags:
            if p_hi <= lo or p_lo >= hi:
                nxt.append((lo, hi))
                continue
            if p_lo > lo:
                nxt.append((lo, p_lo))
            if p_hi < hi:
                nxt.append((p_hi, hi))
        frags = nxt
    return frags


def own_text_runs(cond_windows: dict, protect_windows: dict, *,
                  min_run: int, edge: int) -> dict:
    """record id -> the longest contiguous run (page letters) of a must_not
    version's own verified windows that the main text's windows -- each
    widened by `edge` letters on both sides, for HTR edge jitter -- do NOT
    cover.

    Only records whose longest run is >= `min_run` are returned -- several
    islands under the floor, split apart by a protected stretch, are never
    added together (owner ruling 2026-10-14, "own text":
    docs/plans/COMPOSITION_CONDITIONS_PLAN.md section 12).

    `cond_windows` / `protect_windows` are {record_id: windows}, each
    `windows` a sequence of (r0, r1) page-letter half-open intervals exactly
    as `PassageHit.windows` / `ConditionSearch.hit_windows` carry them -- the
    UNMERGED accepted windows. A record missing from `protect_windows` has
    no protection at all (the main text never matched that page), so every
    one of the quoting text's windows there counts toward its run."""
    out = {}
    for rec_id, wins in cond_windows.items():
        covered = _merge_windows(wins)
        if not covered:
            continue
        widened = [(max(0, lo - edge), hi + edge)
                  for lo, hi in protect_windows.get(rec_id, ())]
        protected = _merge_windows(widened)
        frags = _subtract_windows(covered, protected)
        longest = max((hi - lo for lo, hi in frags), default=0)
        if longest >= min_run:
            out[rec_id] = longest
    return out


def must_not_version_search(cond_search, protections: Sequence, *,
                            seed_windows: Optional[dict] = None,
                            min_run: int = OWN_TEXT_MIN,
                            edge: int = EDGE_ALLOWANCE) -> VersionSearch:
    """The VersionSearch to judge for one must_not version, applying the
    "own text" rule.

    `cond_search` is the quoting text's own `ConditionSearch`
    (shared.passage_parallels) -- what this version's own search found.
    `protections` is the main text's own ConditionSearch(es), searched over
    the SAME scope as this condition (same pages / sys_ids) -- normally a
    single one, searched once per distinct scope and shared across every
    must_not condition that scope applies to. It is always the main text AS
    TYPED (the seed), never another witness: a witness is a stand-in
    manuscript, not the user's own text, and "own text" means the user's.

    `seed_windows` is the main text's OWN matched-core windows on the pages
    it found as a RESULT of its own search -- {record_id: windows}, normally
    `seed_protection_windows(main_rows + filtered_rows)`. Protection for a
    page is the UNION of these and whatever `protections` re-found there: a
    SECOND search of the main text, over only a must_not condition's scope,
    can find FEWER matches on a page than the main search itself already
    established there (measured on noisy or long pages) -- the main search's
    own windows must never be lost to that. A page the main search did not
    itself match (an adjacent image, a manuscript page outside the main
    results) has no `seed_windows` entry and is protected by `protections`
    alone, which is exactly what they are for.

    If ANY protection search does not prove its own completeness (stopped,
    failed before it ever ran, truncated), nothing can safely be called
    "found": this returns `VersionSearch((), <that reason>)` so the
    must_not condition removes NOTHING from the pages it covers, and every
    one of them is judged "not fully checked" instead of main or removed --
    conservative, and documented so a reviewer does not mistake it for a bug:
    the one way this rule can go wrong is toward KEEPING a page, never
    toward losing one.
    """
    p_cuts = [c for c in (cut_reason(p.report, stopped=p.stopped)
                         for p in protections) if c]
    if p_cuts:
        return VersionSearch((), p_cuts[0])
    protect_windows: dict = {}
    for rec_id, wins in (seed_windows or {}).items():
        protect_windows[rec_id] = protect_windows.get(rec_id, ()) + tuple(wins)
    for p in protections:
        for rec_id, wins in p.hit_windows.items():
            protect_windows[rec_id] = protect_windows.get(rec_id, ()) + tuple(wins)
    v_cut = cut_reason(cond_search.report, stopped=cond_search.stopped)
    runs = own_text_runs(cond_search.hit_windows, protect_windows,
                        min_run=min_run, edge=edge)
    return VersionSearch(tuple(sorted(runs)), v_cut)


@dataclass(frozen=True)
class PageOutcome:
    category: str
    states: tuple            # one STATES value per condition
    evidence: tuple          # per condition: tuple of record ids where found
    reasons: tuple           # per condition: CUT_* reason when UNRESOLVED

    def to_dict(self) -> dict:
        return {'category': self.category, 'states': list(self.states),
                'evidence': [list(e) for e in self.evidence],
                'reasons': list(self.reasons)}

    @classmethod
    def from_dict(cls, d, n_conditions: int) -> 'PageOutcome':
        """Strict: a stored outcome that does not match the query's shape
        raises ValueError (a snapshot is never shown half-judged)."""
        if not isinstance(d, dict):
            raise ValueError('outcome is not a mapping')
        cat, states = d.get('category'), d.get('states')
        evidence, reasons = d.get('evidence'), d.get('reasons')
        if cat not in CATEGORIES:
            raise ValueError(f'unknown category {cat!r}')
        for name, seq in (('states', states), ('evidence', evidence),
                          ('reasons', reasons)):
            if not isinstance(seq, list) or len(seq) != n_conditions:
                raise ValueError(f'{name} does not match the conditions')
        if not all(s in STATES for s in states):
            raise ValueError('unknown state')
        if not all(isinstance(e, list) and all(isinstance(r, str) for r in e)
                   for e in evidence):
            raise ValueError('malformed evidence')
        if not all(r == CUT_NONE or r in CUT_REASONS for r in reasons):
            raise ValueError('unknown reason')
        if _category(states) != cat:
            raise ValueError('category does not follow from the states')
        for state, ev, reason in zip(states, evidence, reasons):
            # A reason is exactly the mark of an unresolved condition, and
            # evidence goes with "found": a stored outcome that says otherwise
            # would show an unproven absence as clean.
            if (state == UNRESOLVED) != bool(reason):
                raise ValueError('a reason without an unresolved state, or the reverse')
            if state == REMOVED and not ev:
                raise ValueError('removed without evidence')
            if state in (FAILED, UNRESOLVED) and ev:
                raise ValueError('evidence on a condition that was not found')
        return cls(cat, tuple(states), tuple(tuple(e) for e in evidence),
                   tuple(reasons))


def _category(states) -> str:
    if FAILED in states:
        return CAT_DROPPED
    if REMOVED in states:
        return CAT_REMOVED
    if UNRESOLVED in states:
        return CAT_UNRESOLVED
    return CAT_MAIN


class _ConditionHits:
    """One condition's verified hits from all its versions (any version
    counts), indexed for the three places."""

    def __init__(self, versions: Sequence[VersionSearch]):
        self.by_page: dict = {}
        self.by_sys: dict = {}
        cuts = [v.cut for v in versions if v.cut]
        self.cut = cuts[0] if cuts else CUT_NONE
        for v in versions:
            for rid in v.hit_record_ids:
                key = parse_page_key(rid)
                if key is None:
                    # A verified hit we cannot place: absence is no longer
                    # proven anywhere.
                    self.cut = self.cut or CUT_UNIDENTIFIED
                    continue
                self.by_page.setdefault(key, []).append(rid)
                self.by_sys.setdefault(key.sys_id, []).append(rid)

    def found(self, where: str, key: PageKey) -> tuple:
        if where == WHERE_SAME_IMAGE:
            rids = self.by_page.get(key, [])
        elif where == WHERE_ADJACENT:
            a, b = key.neighbours()
            rids = self.by_page.get(a, []) + self.by_page.get(b, [])
        else:
            rids = self.by_sys.get(key.sys_id, [])
        return tuple(sorted(set(rids)))


def judge(main_headers: Iterable[str], conditions: Sequence[Condition],
          results: Sequence[Sequence[VersionSearch]]) -> dict:
    """header -> PageOutcome for every main-text page.

    `results[i]` holds condition i's version searches (a version that never
    ran must be passed as VersionSearch(cut=CUT_NOT_RUN), never omitted). A
    main page whose identity cannot be parsed is UNRESOLVED on every
    condition."""
    if len(results) != len(conditions):
        raise ValueError('one result list per condition')
    hits = []
    for cond, versions in zip(conditions, results):
        if len(versions) != len(cond.versions):
            raise ValueError('one search per version')
        hits.append(_ConditionHits(versions))
    out = {}
    for header in main_headers:
        key = parse_page_key(header)
        states, evidence, reasons = [], [], []
        for cond, h in zip(conditions, hits):
            if key is None:
                states.append(UNRESOLVED)
                evidence.append(())
                reasons.append(CUT_UNIDENTIFIED)
                continue
            found = h.found(cond.where, key)
            if cond.requirement == REQ_MUST:
                state = SATISFIED if found else (UNRESOLVED if h.cut else FAILED)
            else:
                state = REMOVED if found else (UNRESOLVED if h.cut else SATISFIED)
            states.append(state)
            evidence.append(found)
            reasons.append(h.cut if state == UNRESOLVED else CUT_NONE)
        out[header] = PageOutcome(_category(states), tuple(states),
                                  tuple(evidence), tuple(reasons))
    return out


def fingerprint(main_texts: Sequence[str], conditions: Sequence[Condition],
                settings: Optional[dict] = None) -> str:
    """Identity of a dispatched compound query: the main text(s), every
    condition, and the settings that change what a search finds."""
    payload = {'main': list(main_texts),
               'conditions': [c.to_dict() for c in conditions],
               'settings': settings or {}}
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()


def merge_patterns(a, b) -> str:
    """One highlight pattern for two texts matched on the same page (each is
    a regex alternation of literals); either may be empty."""
    a, b = a or '', b or ''
    if not a or a == b:
        return b if not a else a
    if not b:
        return a
    return f'(?:{a})|(?:{b})'


@dataclass
class Routed:
    """Main-text rows sorted by their judgement, capped AFTER judging.

    Every row is a copy carrying `compound` (its PageOutcome as a dict) and
    `compound_bucket` (the bucket the main-text search put it in: 'main' or
    'filtered'). `failed_counts[i]` is how many pages condition i dropped."""
    main: list
    filtered: list
    removed: list
    unresolved: list
    failed_counts: tuple
    main_truncated: bool = False
    filtered_truncated: bool = False


def route(main_rows: Sequence[dict], filtered_rows: Sequence[dict],
          outcomes: dict, n_conditions: int, *, cap: int, sys_parser,
          order_key: Optional[str] = None) -> Routed:
    """Send each judged main-text row to Main, Filtered, Removed or Not fully
    checked, or drop it; then apply the display cap to Main and to Filtered
    -- never before judging, and never to Removed or Not fully checked.

    Every row must have an outcome (keyed by `raw_header`); a missing one is
    a defect and raises, rather than letting an unjudged row through."""
    from shared.parallels_service import _cap_main_results_by_group

    kept = {'main': [], 'filtered': []}
    removed, unresolved = [], []
    failed = [0] * n_conditions
    for bucket, rows in (('main', main_rows), ('filtered', filtered_rows)):
        for row in rows:
            oc = outcomes.get(row.get('raw_header'))
            if oc is None:
                raise ValueError(f'unjudged row {row.get("raw_header")!r}')
            if len(oc.states) != n_conditions:
                raise ValueError('outcome does not match the conditions')
            if oc.category == CAT_DROPPED:
                for ci, state in enumerate(oc.states):
                    if state == FAILED:
                        failed[ci] += 1
                continue
            r = dict(row)
            r['compound'] = oc.to_dict()
            r['compound_bucket'] = bucket
            if oc.category == CAT_MAIN:
                kept[bucket].append(r)
            elif oc.category == CAT_REMOVED:
                removed.append(r)
            else:
                unresolved.append(r)
    main, filtered = kept['main'], kept['filtered']
    main_trunc = filt_trunc = False
    if cap and cap > 0:
        main, main_trunc = _cap_main_results_by_group(
            main, sys_parser, cap=cap, order_key=order_key)
        filtered, filt_trunc = _cap_main_results_by_group(
            filtered, sys_parser, cap=cap, order_key=order_key)
    return Routed(main, filtered, removed, unresolved, tuple(failed),
                  main_trunc, filt_trunc)
