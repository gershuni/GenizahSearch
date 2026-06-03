# -*- coding: utf-8 -*-
"""
Joins Lab shared core — pure domain logic + SearchExecutor adapter contract.

Provides: BuilderRow / SideQuery / Candidate / MergeResult frozen dataclasses,
SearchExecutor Protocol, normalize_candidate(), page_of(), compose().

Plans 02 and 03 append further functions (dedup_candidates, merge_candidates,
detect_self_match, cross_side_membership, resolve_other_side_pages,
snippet_html, snippet_plain) to this same module.

No Qt bindings. No direct database connections. All data access via SearchExecutor
adapter or the existing shared services (visual_similarity_service,
fjms_service).
"""
from __future__ import annotations

import dataclasses
import html
import re
from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable


# ── Frozen domain model ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class BuilderRow:
    """One row in the Joins Lab line-by-line query builder.

    Fields:
      term         -- the Hebrew search term (clean text, no bracket tokens)
      line_start   -- True → this row's first word must be at the START of a line
                      (RTL right edge; compose() prepends '|' to toks[0])
      line_end     -- True → this row's last word must be at the END of a line
                      (compose() appends '|' to toks[-1])
      gap_to_next  -- number of lines to skip to the next row; 0 = consecutive
    """
    term: str
    line_start: bool = False
    line_end: bool = False
    gap_to_next: int = 0


@dataclass(frozen=True)
class SideQuery:
    """A complete query specification for one side of a join search.

    Fields:
      rows           -- ordered tuple of BuilderRows (top to bottom)
      variants       -- True → engine uses variant expansion ('variants' mode)
      page_position  -- 'start' | 'end' | None (D-08, whole-query page anchor)
                        'start': first line of the manuscript page must contain
                        the first row's match (engine text_position='start').
                        'end': last line must contain the last row's match.
                        None: no page-level position constraint.

    Value domain: page_position must be None, 'start', or 'end'.
    Placement constraint (first/last row must have non-empty content) is
    enforced in compose(), not here — it depends on row content at call time.
    """
    rows: tuple
    variants: bool = False
    page_position: Optional[str] = None

    def __post_init__(self):
        # Validate VALUE DOMAIN only.  Placement constraint is in compose().
        if self.page_position not in (None, "start", "end"):
            raise ValueError(
                f"page_position must be 'start'|'end'|None, got {self.page_position!r}"
            )


@dataclass(frozen=True)
class Candidate:
    """A normalized search result candidate for the Joins Lab.

    Produced by normalize_candidate() — the SINGLE dict→Candidate source of truth.
    frozen=True forbids mutation; downstream merge uses dataclasses.replace()
    (Plan 02 adds `import dataclasses` for that usage).

    Fields:
      sys_id            -- Alma/long system-number ID (99000… format)
      page              -- 1-based page/image number; None for VS-only rows (uid='sid|vs')
      uid               -- canonical result identifier from the engine
      shelfmark         -- human-readable shelfmark (e.g. 'T-S 12.100')
      title             -- manuscript title (may be empty)
      library_code      -- abbreviated library code ('CUL', 'JTS', ...)
      full_text         -- transcription text (may be empty)
      snippet           -- highlighted text fragment from engine
      highlight_pattern -- regex pattern string for client-side re-highlight
      score             -- Tantivy relevance score; None when result carries no score
                          (line-break results carry no score — Codex VERIFIED)
      scope             -- result scope string ('page' | 'system' | 'genizah' | ...)
      via_text          -- True if result came from a text search
      via_vs            -- True if result came from or matched VS look-alikes
      via_other_side    -- True if result came from the cross-side OR path
      is_anchor_self    -- True if this result is the anchor fragment itself
      vs_rank           -- VS look-alike rank (integer position); None if not via VS
      vs_score          -- VS SVM score; None = no VS data (NOT 0.0 dissimilar) — Pitfall 6
    """
    sys_id: str
    page: Optional[int]
    uid: str = ""
    shelfmark: str = "?"
    title: str = ""
    library_code: str = ""
    full_text: str = ""
    snippet: str = ""
    highlight_pattern: Optional[str] = None
    score: Optional[float] = None   # None == result carried no score (line-break results) — Codex VERIFIED
    scope: str = ""
    via_text: bool = False
    via_vs: bool = False
    via_other_side: bool = False
    is_anchor_self: bool = False
    vs_rank: Optional[int] = None
    vs_score: Optional[float] = None   # None == "no VS data" (NOT 0.0 dissimilar) — RESEARCH Pitfall 6

    @property
    def key(self) -> tuple:
        """Canonical per-image dedup key: (sys_id, page).

        page is None for VS-only rows (uid='{sid}|vs') — dedup key is (sys_id, None),
        so only one VS-only candidate per sys_id survives.
        """
        return (self.sys_id, self.page)


@dataclass(frozen=True)
class MergeResult:
    """Result of a text/VS candidate merge operation (Plan 02).

    frozen=True: downstream annotation uses dataclasses.replace(), never
    attribute assignment.

    Fields:
      candidates -- merged, ordered, de-duplicated tuple of Candidates
      note       -- optional diagnostic note (empty string = no note)
    """
    candidates: tuple
    note: str = ""


# ── SearchExecutor Protocol ───────────────────────────────────────────────────


@runtime_checkable
class SearchExecutor(Protocol):
    """Narrow adapter Protocol over the search engine.

    Implementations: desktop wraps SearchEngine + MetadataManager (Phase 107+);
    web wraps the web engine or its API (later phase).  All four methods are
    thin passthroughs — no per-app normalizer.

    Callers of execute_search for Joins Lab pass corpus_scope='genizah' explicitly
    (the default 'all' is the live engine default and is preserved here for
    structural compatibility).
    """

    def execute_search(
        self,
        query_str: str,
        mode: str,
        gap: int,
        progress_callback=None,
        exclude_words=None,
        responsa_options: dict | None = None,
        restrict_sys_ids: set | None = None,
        text_position: str | None = None,
        corpus_scope: str = "all",
    ) -> list[dict]: ...
    # Joins Lab adapter callers pass corpus_scope='genizah' explicitly.

    def get_browse_page(
        self,
        sys_id: str,
        p_num: int | None = None,
        next_prev: int = 0,
        absolute_index: int | None = None,
        allow_cross: bool = False,
        volume_ie: str | None = None,
    ) -> dict | None: ...
    # Returns dict with 'uid','p_num','full_header','text','total_pages',
    # 'current_idx','internal_index','sys_id','volume_ie', or None at boundaries.

    def get_meta_for_id(self, sys_id: str) -> tuple[str, str]: ...
    # Returns (shelfmark, title).

    def get_library_for_id(self, sys_id: str) -> str: ...
    # Returns library_code string ('CUL', 'JTS', ...) or ''.


# ── Module-level pure helpers ─────────────────────────────────────────────────


def _to_int(v) -> Optional[int]:
    """Coerce v to int; return None on any conversion failure."""
    try:
        return int(str(v).strip())
    except (TypeError, ValueError, AttributeError):
        return None


def _r_sid(res: dict) -> str:
    """Extract sys_id from a result dict (sketch L61 accessor)."""
    return (res.get("display") or {}).get("id") or res.get("sys_id") or ""


def _r_shelf(res: dict) -> str:
    """Extract shelfmark from a result dict (sketch L65 accessor)."""
    d = res.get("display") or {}
    return d.get("shelfmark") or res.get("shelfmark") or res.get("uid") or "?"


def _r_title(res: dict) -> str:
    """Extract title from a result dict (sketch L68 accessor)."""
    return (res.get("display") or {}).get("title") or ""


def _r_lib(res: dict) -> str:
    """Extract library_code from a result dict (sketch L73 accessor)."""
    d = res.get("display") or {}
    return d.get("library_code") or d.get("library") or ""


def page_of(res: dict) -> Optional[int]:
    """Best-effort 1-based page/image number for a result dict.

    Extraction order (sketch L84-95):
    1. display.img via _to_int if truthy.
    2. Re-search '_P0*(\\d+)' in uid field.
    3. None (covers VS-only uid='{sid}|vs' and missing data).
    """
    p = _to_int((res.get("display") or {}).get("img"))
    if p:
        return p
    m = re.search(r"_P0*(\d+)", res.get("uid") or "")
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def normalize_candidate(res: dict) -> Candidate:
    """Normalize a raw engine result dict to a Candidate.

    Single source of truth for dict→Candidate conversion (D-02, D-04).
    All fields are read via .get() with safe defaults so a partial/malformed
    dict cannot raise KeyError or produce a wrong-typed page (T-106-02 mitigation).

    'score' is read via .get('score') with default None — line-break results
    carry no score key, so Candidate.score stays None for those (Codex VERIFIED).
    """
    d = res.get("display") or {}
    return Candidate(
        sys_id=_r_sid(res),
        page=page_of(res),
        uid=res.get("uid") or "",
        shelfmark=d.get("shelfmark") or res.get("shelfmark") or res.get("uid") or "?",
        title=d.get("title") or "",
        library_code=d.get("library_code") or d.get("library") or "",
        full_text=res.get("full_text") or "",
        snippet=res.get("snippet") or "",
        highlight_pattern=res.get("highlight_pattern"),
        score=res.get("score"),          # .get() → None when key absent (line-break results)
        scope=res.get("scope") or "",
        via_text=bool(res.get("_via_text")),
        via_vs=bool(res.get("_via_vs")),
        via_other_side=bool(res.get("_via_other_side")),
        is_anchor_self=bool(res.get("_is_anchor_self")),
        vs_rank=res.get("vs_rank"),
        vs_score=res.get("svm_score"),
    )


# ── Cross-side membership helpers (SC#2) ─────────────────────────────────────


def resolve_other_side_pages(page: int, total_pages: Optional[int]) -> frozenset:
    """Return the set of neighbor page numbers that form the 'other side' of a leaf.

    For a recto/verso leaf, the other side is the adjacent image:
      first page  → {p+1}
      last page   → {p-1}
      middle page → {p-1, p+1}
      single-page doc → empty set

    total_pages=None means unknown upper bound; the lower clamp (drop < 1) still applies.

    Pure function — no I/O (D-06).
    """
    neighbors = set()
    for n in (page - 1, page + 1):
        if n < 1:
            continue
        if total_pages is not None and n > total_pages:
            continue
        neighbors.add(n)
    return frozenset(neighbors)


def cross_side_membership(
    base_keys: set,
    b_set: set,
    combine: str,
    totals: dict,
) -> set:
    """Pure AND/OR set logic for cross-side filtering (transplanted from sketch L400-428).

    base_keys: set of (sys_id, page) tuples from the this-side search.
    b_set:     set of (sys_id, page) tuples from the other-side search.
    combine:   'AND' or 'OR'.
    totals:    {sys_id: total_pages | None} — used by OR to clamp neighbor synthesis.

    AND: keep only base entries where a neighbor (sid, p±1) appears in b_set.
    OR:  start from base_keys; for each (sid, q) in b_set add neighbor pages (q±1)
         that are in-bounds and not already in the set.

    Pure function — no I/O (D-06).
    """
    if combine == "AND":
        return {
            (sid, p)
            for (sid, p) in base_keys
            if (sid, p - 1) in b_set or (sid, p + 1) in b_set
        }
    # OR: accumulate base + synthesized neighbors
    result = set(base_keys)
    for (sid, q) in b_set:
        t = totals.get(sid)
        for n in (q - 1, q + 1):
            if n < 1:
                continue
            if t is not None and n > t:
                continue
            result.add((sid, n))
    return result


def apply_cross_side(
    executor: "SearchExecutor",
    base: list,
    b_query: str,
    b_responsa_options: dict,
    combine: str,
    anchor_pattern: Optional[str] = None,
) -> "MergeResult":
    """I/O-bound orchestrator for cross-side AND/OR membership (SC#2).

    Runs query B through the injected SearchExecutor, builds a b_set of (sys_id, page)
    pairs, then applies AND/OR logic to filter/extend the base Candidate list.

    AND: returns only base candidates whose (sys_id, page±1) appears in b_set.
    OR:  returns base candidates plus synthesized neighbor Candidates for each b_set
         page whose adjacent page is not already represented.

    Failure in any executor call degrades gracefully to fewer/no candidates rather than
    raising (T-106-05 mitigation). corpus_scope='genizah' is passed explicitly.

    Returns MergeResult(candidates=tuple, note=str).
    """
    # 1) Run query B through the engine → b_set of (sid, page)
    try:
        bres = (
            executor.execute_search(
                b_query,
                "exact",
                0,
                responsa_options=b_responsa_options,
                corpus_scope="genizah",
            )
            or []
        )
    except Exception:
        bres = []

    b_set = set()
    for r in bres:
        c = normalize_candidate(r)
        if c.sys_id and c.page is not None:
            b_set.add((c.sys_id, c.page))

    # 2) total_pages cache: per-sid, fetched lazily from get_browse_page(sid, 1)
    _totals_cache: dict = {}

    def _page_total(sid: str) -> Optional[int]:
        if sid not in _totals_cache:
            t = None
            try:
                page_data = executor.get_browse_page(sid, 1)
                if page_data is not None:
                    t = page_data.get("total_pages")
            except Exception:
                t = None
            _totals_cache[sid] = t
        return _totals_cache[sid]

    # 3) AND: filter base to those with a b_set neighbor
    if combine == "AND":
        out = [
            c
            for c in base
            if c.sys_id
            and c.page is not None
            and ((c.sys_id, c.page - 1) in b_set or (c.sys_id, c.page + 1) in b_set)
        ]
        note = f"B matched {len(b_set)} pages"
        return MergeResult(candidates=tuple(out), note=note)

    # 4) OR: start from base, synthesize neighbors for each b_set page
    out = list(base)
    seen = {c.key for c in base}
    added = 0
    for (sid, q) in b_set:
        t = _page_total(sid)
        for n in (q - 1, q + 1):
            if n < 1:
                continue
            if t is not None and n > t:
                continue
            if (sid, n) in seen:
                continue
            seen.add((sid, n))
            # Synthesize a neighbor result dict (sketch _make_neighbor_result shape)
            txt = ""
            try:
                page_data = executor.get_browse_page(sid, n)
                if page_data is not None:
                    txt = page_data.get("text", "") or ""
            except Exception:
                txt = ""
            shelf, title = "", ""
            try:
                shelf, title = executor.get_meta_for_id(sid)
            except Exception:
                pass
            lib = ""
            try:
                lib = executor.get_library_for_id(sid) or ""
            except Exception:
                pass
            neighbor_res = {
                "display": {
                    "id": sid,
                    "shelfmark": shelf,
                    "title": title,
                    "library_code": lib,
                    "img": n,
                },
                "full_text": txt,
                "uid": f"{sid}|{n}",
                "highlight_pattern": anchor_pattern,
                "_via_other_side": True,
            }
            out.append(normalize_candidate(neighbor_res))
            added += 1

    note = f"B matched {len(b_set)} pages · +{added} via other side"
    return MergeResult(candidates=tuple(out), note=note)


# ── Candidate dedup / compaction (SC#3) ──────────────────────────────────────


def dedup_candidates(
    raw: list,
    anchor_sid: str,
    include_self: bool = False,
) -> tuple:
    """Deduplicate raw result dicts to one Candidate per (sys_id, page).

    Transplanted from sketch _on_results dedup block (L1102-1114).

    - Keyed on Candidate.key == (sys_id, page). VS-only rows (uid='{sid}|vs')
      get page=None, so only one VS-only candidate per sys_id survives.
    - Every surviving Candidate has via_text=True (it came from a text search).
    - Candidates whose sys_id == anchor_sid are flagged as is_anchor_self.
      When include_self=False (default) they are excluded from the output.
    - Returns (deduped_list, anchor_matched) where anchor_matched=True means
      at least one raw result belonged to the anchor fragment itself.

    Pure function — no I/O (D-06).
    """
    seen: set = set()
    out: list = []
    anchor_matched = False
    for r in raw:
        sid = _r_sid(r)
        is_self = sid == anchor_sid
        if is_self:
            anchor_matched = True
        if is_self and not include_self:
            continue
        cand = normalize_candidate(r)
        key = cand.key
        if key in seen:
            continue
        seen.add(key)
        cand = dataclasses.replace(cand, via_text=True, is_anchor_self=is_self)
        out.append(cand)
    return (out, anchor_matched)


# ── Text/VS merge ordering with provenance (SC#4) ────────────────────────────


def merge_candidates(text_cands: list, vs_cands: list) -> list:
    """Merge text and VS candidate lists into a stable, provenance-tagged ordering.

    Transplanted from sketch _maybe_assemble (L1156-1174).

    Takes already-fetched lists (D-05/D-06 — VS is fetched by the caller via
    get_vs_service(), never by this function). Frozen Candidates are annotated
    via dataclasses.replace() — never attribute mutation (T-106-06 mitigation).

    Ordering: tier 0 (both via_text AND via_vs) → tier 1 (text-only) →
    tier 2 (VS-only). Within tier 2, sorted by vs_rank ascending.
    Within tiers 0 and 1 the relative order from text_cands is preserved (stable sort).

    Pure function — no I/O (D-06).
    """
    if not vs_cands:
        return list(text_cands)
    if not text_cands:
        return list(vs_cands)

    vs_by_sid = {v.sys_id: v for v in vs_cands}

    # Annotate text candidates that also appear in the VS set
    annotated_text = []
    for r in text_cands:
        v = vs_by_sid.get(r.sys_id)
        if v is not None:
            # Carry vs_score through too (WR-02): dropping it would re-stamp the
            # candidate as via_vs=True with vs_score=None, which the Candidate
            # docstring defines as "no VS data (NOT 0.0)" — Pitfall 6.
            r = dataclasses.replace(r, via_vs=True, vs_rank=v.vs_rank, vs_score=v.vs_score)
        annotated_text.append(r)

    text_sids = {r.sys_id for r in annotated_text}
    vs_only = [v for v in vs_cands if v.sys_id not in text_sids]

    merged = annotated_text + vs_only

    def _k(c: "Candidate"):
        both = c.via_text and c.via_vs
        tier = 0 if both else (1 if c.via_text else 2)
        return (tier, c.vs_rank if c.vs_rank is not None else 99999)

    merged.sort(key=_k)
    return merged


# ── Self-match detection + first-hit locator (SC#5) ─────────────────────────


def detect_self_match(raw_results: list, anchor_sid: str) -> bool:
    """Report whether the anchor's own sys_id appears in a fetched result list.

    This is a sys_id SET-MEMBERSHIP check over an already-fetched result list (D-06).
    It reports whether the anchor's own sys_id appears among the results the engine
    returned for the composed query — i.e. the anchor itself satisfied the query as
    evidenced by the engine returning it.

    It is bracket-AGNOSTIC by construction: it keys on sys_id and NEVER re-runs
    the line-start position regex, so a leading tear-bracket token in the anchor's
    text cannot affect this boolean either way.

    It does NOT, and must not be read to, prove that the line-break engine path
    returns bracket-prefixed line-start hits — the line-break Tantivy candidate
    expansion does not add bracket variants, so that end-to-end guarantee is OUT
    of Phase 106's scope (RESEARCH R-02, corrected 2026-06-03 per Codex review).

    The caller obtains raw_results from execute_search; the "include anchor itself"
    UI toggle (Phase 108) consumes this boolean together with dedup_candidates'
    include_self parameter.

    Pure function — no I/O (D-06). Transplanted from sketch _anchor_matched (L1100).
    """
    return any(_r_sid(r) == anchor_sid for r in raw_results)


def _match_line(lines: list, pattern: Optional[str]) -> int:
    """Return the index of the first line whose content matches the pattern.

    Returns -1 if pattern is None/empty, if no line matches, or if the
    pattern is malformed (re.error swallowed — no raise).

    Case-insensitive match (re.IGNORECASE).

    Pure function — no I/O (D-06). Transplanted from sketch L113-123.
    """
    if not pattern:
        return -1
    try:
        rx = re.compile(pattern, re.IGNORECASE)
    except re.error:
        return -1
    for i, ln in enumerate(lines):
        if rx.search(ln):
            return i
    return -1


# ── HTML snippet helpers (SC#5) ──────────────────────────────────────────────

# Internal sentinel tokens used to bracket highlighted matches before HTML-escaping.
# These characters (SOH/STX) are not valid in corpus text and survive html.escape() unchanged.
MARK_A = "\x01"   # marks the start of a regex highlight region
MARK_B = "\x02"   # marks the end of a regex highlight region


def htmlify(text: str, pattern: Optional[str] = None) -> str:
    """Escape corpus text to HTML, optionally highlighting pattern matches.

    Processing order (T-106-08 XSS mitigation):
    1. Substitute match regions with NUL-byte sentinels (MARK_A/MARK_B).
    2. html.escape() the whole text (including any '<'/'>'/'&') — corpus content
       is fully escaped and cannot inject markup.
    3. Replace '\\n' with '<br>'.
    4. Replace sentinels with the fixed '<b style=...>' tag — only this module's
       own tag survives the escape pass.
    5. Wrap in a right-to-left div.

    Pure function — no I/O (D-06). Transplanted from sketch L98-110.
    """
    text = text or ""
    if pattern:
        try:
            rx = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
            text = rx.sub(lambda m: MARK_A + m.group(0) + MARK_B, text)
        except re.error:
            pass
    t = html.escape(text)
    t = t.replace("\n", "<br>")
    t = t.replace(MARK_A, "<b style='color:#dc2626'>").replace(MARK_B, "</b>")
    return f"<div dir='rtl' style='text-align:right'>{t}</div>"


def snippet_html(text: str, pattern: Optional[str], max_lines: int = 8) -> str:
    """HTML snippet centered on the first regex match.

    The matched line is always visible in the output window (so the caller does
    not need to scroll to see why this result was included). When no match is
    found, the first non-blank lines are returned instead.

    Returns an RTL-wrapped, HTML-escaped string with the match highlighted in
    red-bold (via htmlify). max_lines controls the window size (default 8).

    Pure function — no I/O (D-06). Transplanted from sketch L126-135.
    """
    lines = (text or "").split("\n")
    hit = _match_line(lines, pattern)
    if hit < 0:
        chosen = [ln for ln in lines if ln.strip()][:max_lines]
    else:
        lo = max(0, hit - 2)
        chosen = lines[lo:lo + max_lines]
    return htmlify("\n".join(chosen), pattern)


def snippet_plain(text: str, pattern: Optional[str], max_chars: int = 220) -> str:
    """Plain-text snippet centered on the first regex match, for table cells.

    Joins up to 3 lines around the first match with '  /  ', stripped of
    whitespace. Truncates to max_chars with a trailing '…' when over the cap.
    When no match is found, uses the first 3 non-blank lines instead.

    Pure function — no I/O (D-06). Transplanted from sketch L138-148.
    """
    lines = (text or "").split("\n")
    hit = _match_line(lines, pattern)
    if hit < 0:
        parts = [ln.strip() for ln in lines if ln.strip()][:3]
    else:
        lo = max(0, hit - 1)
        parts = [ln.strip() for ln in lines[lo:lo + 3] if ln.strip()]
    s = "  /  ".join(parts)
    return (s[:max_chars] + "…") if len(s) > max_chars else s


# ── compose() — line-break query composition (SC#1) ─────────────────────────


def compose(side: SideQuery) -> tuple:
    """Compose a SideQuery into the engine's line-break query syntax.

    Returns (query_str, responsa_options, page_position) — a 3-tuple.

    query_str:         engine line-break syntax with '|'-groups and '[|N]' gaps.
                       None when all rows have empty terms (benign empty-input case,
                       only when page_position is None — see ValueError rules below).
    responsa_options:  dict for execute_search(responsa_options=...).
    page_position:     'start' | 'end' | None — pass as execute_search(text_position=...).
                       The caller is responsible for forwarding this to execute_search.
                       Placement constraint enforced here (D-08):
                       'start' requires a non-empty first row;
                       'end' requires a non-empty last row.

    RTL orientation (Pitfall 1 / iteration E):
    line_start=True prepends '|' to toks[0] (LEFT-SIDE token in the string, which is
    the RIGHT-SIDE / start position in RTL Hebrew).
    line_end=True appends '|' to toks[-1].

    Raises ValueError:
      - page_position='start' and first row has no non-empty content
      - page_position='end'   and last  row has no non-empty content
      - (SideQuery.__post_init__ handles the value-domain check for page_position)

    Pure function — no I/O (D-06).
    """
    # Step 1: D-08 placement constraint — validate BEFORE the empty short-circuit.
    if side.page_position == "start":
        if not side.rows or not side.rows[0].term.strip():
            raise ValueError(
                "page_position 'start' requires a non-empty first row to anchor"
            )
    if side.page_position == "end":
        if not side.rows or not side.rows[-1].term.strip():
            raise ValueError(
                "page_position 'end' requires a non-empty last row to anchor"
            )

    # Step 2: filter to non-empty rows.
    rows = [r for r in side.rows if r.term.strip()]
    if not rows:
        # Benign empty-input case (only reachable when page_position is None —
        # the guards above already raised on anchored-empty).
        return (None, None, None)

    # Step 3: build responsa_options (sketch L565 _responsa_opts equivalent).
    ro = {
        "responsa_mode": True,
        "variants": side.variants,
        "ja": False,
        "flex_spacing": False,
        "bidirectional": False,
        "variant_mode": "variants" if side.variants else "exact",
    }

    # Step 4: single-row / no-anchor short-circuit.
    multiline = len(rows) > 1 or any(r.line_start or r.line_end for r in rows)
    if not multiline:
        return (rows[0].term.strip(), ro, side.page_position)

    # Step 5: multi-row — build parts list with pipe tokens and gap markers.
    parts = []
    for i, row in enumerate(rows):
        toks = row.term.strip().split()
        if not toks:
            continue
        if row.line_start:
            toks[0] = "|" + toks[0]      # leading pipe = line START, RTL right edge
        if row.line_end:
            toks[-1] = toks[-1] + "|"    # trailing pipe = line END
        parts.append(" ".join(toks))
        if i < len(rows) - 1:
            parts.append(f"[|{row.gap_to_next}]")

    return (" ".join(parts), ro, side.page_position)
