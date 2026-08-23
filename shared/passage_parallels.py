# -*- coding: utf-8 -*-
"""CompositionSearcher-shaped wrapper over shared.passage_search (Phase 145).

Contract: docs/specs/passage-matching-algorithm.md sections 6-8; the plan's
"Relationship to the incumbent" (a passage-matching parallels search web
surface). This is the ONLY place a passage-matching result becomes a row
shaped like `shared/parallels_service.py::CompositionSearcher` expects --
`{uid, raw_header, src_lbl, source_ctx, text, score, final_score,
chunk_count, chunk_hits}` -- so both serializers, both UIs, exports and the
public envelope keep working unmodified against it, exactly as they do
against `SearchEngine.search_composition_logic`.

Design decisions this module makes, so they are not re-litigated per call
site:

* Each ACCEPTED SPAN on a record is one "chunk hit". `chunk_count` is the
  number of merged spans (`PassageHit.n_spans`), not a re-derived count --
  unlike the incumbent, whose `chunk_count` is deduped post-hoc from raw
  Tantivy hits (search_engine.py::_count_unique_chunks). Passage spans are
  already merged and distinct by construction.
* `score` / `final_score` are BOTH `PassageHit.score` (matched letters,
  merged-span length). Passage never computes a boundary-boosted
  `final_score`: there is no boundary-crossing concept over a letter stream
  with no token boundaries.

  NOT COMPARABLE TO THE INCUMBENT'S SCORE, and an earlier version of this
  docstring wrongly said it was "directly comparable" (adversarial review
  round 2). The two numbers are measured on OPPOSITE SIDES of the match:
  passage sums QUERY-stream spans (`sum(q1 - q0)`,
  shared/passage_search.py::search), while the incumbent sums merged spans
  in the CANDIDATE's raw `content` (shared/search_engine.py, `base_score`).
  They are not the same quantity in different units, so no conversion
  factor exists to reconcile them -- and three further gaps compound it:
  the passage stream has whitespace, marks, punctuation and digits removed
  by `norm_stream`; passage alignment is approximate and may merge spans
  differently; and raw-character inflation varies with each manuscript's
  orthography and pointing.

  Consequence for any surface combining the two methods: rank WITHIN a
  method and append, never pool the raw scores into one sorted list. This
  matches the evaluation rule in docs/specs/parallels-method-comparison.md
  ("stratify by per-method rank/quantile, never by pooled raw score").
  `boundary_mode` accordingly must be `'full'` -- anything else raises
  ValueError rather than silently degrading (adversarial review finding #2;
  web/search_api.py rejects 'boundary'/'combined' with a 400 BEFORE ever
  calling this searcher, and web/pages/parallels.py disables the boundary
  controls while passage mode is selected, so this ValueError is a
  structural backstop, not the primary enforcement point).
* RENDERED ROWS == KEPT ROWS, always (adversarial review finding #1, "THE
  BIG ONE"). Earlier code rendered highlights for the first `render_cap`
  hits by RAW HIT RANK, but `shared.parallels_service._cap_main_results_by_
  group` (what the API path applies downstream, and what the page's direct
  call path does NOT apply at all) keeps rows by a DIFFERENT rule: top
  `render_cap` distinct sys_id GROUPS by summed score, uncapped when group
  count <= render_cap. With >render_cap raw hits spread across
  <=render_cap manuscripts, rows past raw rank `render_cap` were kept by
  the group rule but never rendered -- silently blank `text`/`source_ctx`/
  `chunk_hits`. Fixed by applying the SAME group-cap function INSIDE this
  searcher (via `_RegexSysIdParser`, a minimal stand-in for the
  `UidComponentParser` the real function needs -- passage has no
  MetadataManager dependency, so it derives sys_id the same way it already
  does for `restrict_sys_ids` filtering, `_extract_sys_id`), rendering
  highlights ONLY for the surviving rows, and RETURNING ONLY those rows.
  This makes the API path's own downstream call to the same function a
  no-op (already <= cap groups) rather than a second, different selection,
  and makes the page's direct call path bounded for the first time (it
  applied no cap before).
* Codex review finding #16(a): `filtered_results` is now group-capped TOO,
  by the SAME function and the SAME cap, not left unbounded. The incumbent
  (shared/parallels_service.py's documented v7.10 decision) leaves ITS
  filtered bucket uncapped on the stated assumption that it is "typically
  small, driven by the user's max_freq threshold" -- passage has no
  max_freq-shaped signal at all; its filtered bucket is driven by
  `filter_text` substring matches, which has no such size guarantee (a
  large pasted "known source" blob could match most of the corpus). Without
  a cap, EVERY verified candidate up to `policy.verify_cap` (default 3,000)
  could land in `filtered` and get its own live, uncapped Tantivy lookup --
  exactly the unbounded-cost risk the render cap exists to prevent for
  `main`. So filtered rows go through the identical
  `_cap_main_results_by_group` call, capped and rendered the same way.
* Codex review finding #16(b): a FAILED text lookup no longer produces a
  silently blank row. Previously a row that survived the cap but whose
  `get_full_text_by_header` call returned nothing (or raised) still made it
  into the output with `text=''`/`source_ctx=''`/`chunk_hits=[]` --
  indistinguishable from "this row survived the cap but genuinely has
  nothing to highlight." Now `_render_highlights` reports success/failure;
  a failed row is DROPPED from the returned list entirely and counted in
  the new `dropped_text_lookup_failures` key of this method's return dict
  (mirrors this project's "no silent truncation -- count every exclusion"
  rule). `shared/parallels_service.py::fetch_parallels_results` reads this
  count into `ParallelsResultBundle.dropped_text_lookup_failures`, and
  web/search_api.py surfaces it as a `passage_text_lookup_failed` warning
  in the public envelope when non-zero. web/pages/parallels.py's direct
  call path receives the same count in its own result dict but does not
  yet display it -- a documented, deliberately out-of-scope gap for this
  fix (no UI surface for a warnings array exists on that page today).
* `filter_text` (the page's "Filter Sources" text) is honored with the SAME
  semantics `SearchEngine.search_composition_logic` uses (adversarial
  review finding #3) -- if the query-side text of ANY span matched on a
  record also appears (letter-stream-normalized) inside `filter_text`, the
  WHOLE row is routed to `filtered` rather than `main` (per-RECORD
  granularity, matching `rec['is_filtered']` in search_engine.py, not
  per-span). `max_freq` has no passage equivalent (passage's own posting
  budget already bounds retrieval internally) and stays unused.
* `chunk_hits[i][0]` (`chunk_index`) is the ordinal of the span's query-side
  start offset among ALL distinct start offsets across every surviving hit
  for this query -- computed ONCE, shared across every record (adversarial
  review finding #4). This mirrors the incumbent's `chunk_index`, which is
  a position in the PASTED COMPOSITION comparable across records (the
  sliding-window index), not a per-record-local counter: two different
  records whose spans start at the same query offset carry the SAME
  `chunk_index` here, exactly as they would if the same sliding-window
  chunk matched both records in the incumbent.
* `_highlight_span` sanitizes a literal `'*'` out of the highlighted window
  before inserting `*marker*` syntax (adversarial review finding #5) --
  mirrors `SearchEngine.highlight` / `_highlight_by_span`'s
  `snippet.replace('*', ' ')` precedent (shared/search_engine.py, the
  general-purpose highlight helper; the composition path's OWN inline
  snippet builder happens to skip this, but the established pattern does
  not, and a literal asterisk in manuscript text would otherwise be
  indistinguishable from marker syntax downstream).
* Highlight text (`text`, `source_ctx`, and each `chunk_hits` tuple's
  `manuscript_snippet`) is built for exactly the rows this method RETURNS
  (both `main` and `filtered`, each capped independently to `render_cap`
  groups) -- a BOUNDED re-normalization per the plan's display-span
  contract. Row count per bucket is itself bounded upstream by
  `policy.verify_cap` (default 3,000 -- shared/passage_policy.py: at most
  one hit per verified candidate); at a measured ~1.3 ms/row render cost, a
  worst-case full cap on BOTH buckets is a handful of seconds, comfortably
  inside `SEARCH_API_PASSAGE_TIMEOUT` (30s default). This is an ACCEPTED
  cost, not further sub-capped, specifically so "rendered == kept" holds
  even in that worst case.
* Two costs are deliberately NOT optimized away (adversarial review,
  "deliberately skipped" items): (1) `nfc()` runs twice per rendered row's
  manuscript text -- once explicitly here, once again inside
  `norm_stream()` -- fixing it would mean changing the versioned
  normalizer's own contract (shared/passage_normalize.py), which is out of
  proportion to the cost; (2) `get_full_text_by_header` issues one live
  Tantivy query PER rendered row rather than a batch fetch -- bounded by
  the same render-cap/verify-cap chain as the render cost above (and now
  ALSO applied to `filtered`, finding #16(a)), so it is documented rather
  than restructured.
* `mode` (exact/variants/fuzzy), `progress_callback`, `boundary_delimiter`,
  `boundary_boost` and `min_delimiter_distance` are accepted (for
  call-shape compatibility with the real `SearchEngine.search_composition_
  logic`, which both `web/pages/parallels.py` and
  `shared/parallels_service.py` invoke with the full keyword set) but
  IGNORED -- none has a passage-matching equivalent: character-level
  Levenshtein matching has no morphological variant expansion and no
  paragraph-token boundary concept. `min_boundary_matches` (the "min. chunk
  matches" UI control in 'full' boundary_mode) IS honored, since it is a
  trivial post-filter on `n_spans` and skipping it would silently ignore a
  visible control. `corpus_scope` is accepted but ignored: the passage
  index never holds Local-corpus records regardless of its value, and
  disabling `passage` for non-Genizah scope is the CALLER's job (API
  validation / the page's method selector), not this searcher's -- see
  web/search_api.py.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Optional, Protocol

from shared.parallels_service import PARALLELS_GROUP_CAP, _cap_main_results_by_group
from shared.passage_hygiene import is_duplicate_photography
from shared.passage_index import PassageIndex
from shared.passage_normalize import nfc, norm_stream, norm_stream_fast, project_span
from shared.passage_policy import PassagePolicy, get_preset
from shared.passage_search import search_passage

# How many score-adjacent SURVIVORS each rendered row is compared against in
# the duplicate-photography pass. Duplicate photographs of one page score
# near-identically (same text matched), so they land adjacent in score order
# -- a small window catches the class the measurement found (36/36 join
# anomalies were adjacent duplicates) while keeping the pass O(window x rows)
# instead of the O(rows^2) the plan's "Bounded Stage-0" section forbids.
DUP_SCAN_WINDOW = 3

logger = logging.getLogger(__name__)

# Letters of context on each side of a highlighted span, matching
# shared/search_engine.py's ms_snip / src_snippets windows exactly (both use
# a +-60 character window around each merged match).
HIGHLIGHT_CONTEXT_PAD = 60

# Same header shape the corpus builder emits and the Tantivy `full_header`
# field stores verbatim (shared/passage_corpus.py's HEADER_RE captures the
# whole `{sys_id}_{IE..}_{P######}_{FL..}` token; this narrows to just the
# IE/P/FL portion, matching shared/metadata_manager.py::extract_unique_id's
# own first-attempt regex so `uid` is byte-identical to what the Tantivy
# `unique_id` field would hold for the SAME page).
_UID_RE = re.compile(r'(IE\d+_P\d+_FL\d+)')
_SYS_ID_RE = re.compile(r'((?:99|97)\d{8,})')


def _derive_uid(record_id: str) -> str:
    m = _UID_RE.search(record_id)
    return m.group(1) if m else record_id


def _extract_sys_id(record_id: str) -> Optional[str]:
    m = _SYS_ID_RE.search(record_id)
    return m.group(1) if m else None


class _RegexSysIdParser:
    """Minimal `UidComponentParser` (shared/parallels_service.py's Protocol)
    for PassageSearcher's OWN group cap. PassageSearcher has no
    MetadataManager dependency and does not need the full parser -- only the
    sys_id it can already derive from a record_id (raw_header) via
    `_extract_sys_id`, the SAME extraction `restrict_sys_ids` filtering
    already uses. Passed into `shared.parallels_service._cap_main_results_
    by_group` so grouping/capping is the EXACT SAME function the API path
    applies to the incumbent's rows -- never a forked reimplementation of
    the grouping logic (adversarial review finding #1)."""

    def parse_full_id_components(self, uid_or_header: str) -> dict:
        return {'sys_id': _extract_sys_id(uid_or_header)}


def _highlight_span(orig_text: str, offsets, start: int, end: int,
                     pad: int = HIGHLIGHT_CONTEXT_PAD) -> str:
    """Build a `*match*`-marked snippet around normalized-stream span
    [start, end) inside `orig_text`, mirroring
    shared/search_engine.py's ms_snip formula
    (``content[start:s] + f"*{content[s:e]}*" + content[e:end]``), PLUS the
    literal-`'*'` sanitization `SearchEngine.highlight` /
    `_highlight_by_span` apply (`snippet.replace('*', ' ')`) that the
    composition path's own inline builder happens to skip -- a manuscript
    page containing a literal asterisk must not produce a spurious marker
    (adversarial review finding #5).

    `offsets` maps normalized-stream index -> index in `orig_text` (from
    `shared.passage_normalize.norm_stream`). Returns '' on an out-of-range
    span (mirrors `project_span`'s own bounds handling) rather than raising
    -- a display helper must never crash a search that otherwise succeeded.
    """
    if not len(offsets) or start >= len(offsets) or end <= start:
        return ''
    end = min(end, len(offsets))
    s = offsets[start]
    e = min(len(orig_text), offsets[end - 1] + 1)
    win_start = max(0, s - pad)
    win_end = min(len(orig_text), e + pad)
    # Sanitize the WINDOW (not the whole orig_text) -- .replace() preserves
    # length, so indices relative to the window stay valid post-sanitize.
    window = orig_text[win_start:win_end].replace('*', ' ')
    rel_s = s - win_start
    rel_e = e - win_start
    return window[:rel_s] + f"*{window[rel_s:rel_e]}*" + window[rel_e:]


class PageTextFetcher(Protocol):
    """Structural type for the injected text source (SEED-016 #3 style).

    Satisfied by the process-singleton SearchEngine
    (`shared/search_engine.py::SearchEngine.get_full_text_by_header`, added
    alongside the pre-existing `get_full_text_by_id`) without importing it
    here -- `shared/` stays framework-agnostic; the caller
    (`web/passage_assets.py`) injects the real engine. PUBLIC (not
    underscore-prefixed) so other modules needing the same shape import
    THIS one rather than redeclaring it (adversarial review finding #9).
    """

    def get_full_text_by_header(self, full_header: str) -> Optional[str]:
        ...


@dataclass
class PassageSearcher:
    """A `CompositionSearcher` (shared/parallels_service.py) backed by the
    passage-matching engine (`shared.passage_search.search_passage`).

    Constructed only when `web.passage_assets.passage_available()` is True
    (the index opened successfully at startup) -- callers must never build
    one against an index that failed to load; there is no internal
    fallback, by design (a searcher that silently no-ops on a bad index is
    exactly the failure mode the rest of this project's fail-closed loaders
    exist to prevent).
    """
    index: PassageIndex
    text_fetcher: PageTextFetcher
    policy: Optional[PassagePolicy] = None
    render_cap: int = PARALLELS_GROUP_CAP

    def __post_init__(self) -> None:
        if self.policy is None:
            self.policy = get_preset('standard-40')

    # -- CompositionSearcher protocol ---------------------------------------

    def search_composition_logic(
        self,
        full_text: str,
        chunk_size: int = 5,
        max_freq: float = 100.0,
        mode: str = 'exact',
        filter_text: Optional[str] = None,
        progress_callback=None,
        boundary_mode: str = 'full',
        boundary_delimiter: str = '\n',
        boundary_boost: float = 1.5,
        min_boundary_matches: int = 0,
        min_delimiter_distance: int = 3,
        restrict_sys_ids: Optional[set] = None,
        corpus_scope: str = 'genizah',
        **_ignored,
    ) -> dict:
        """Same parameter names/order as
        `SearchEngine.search_composition_logic` so both existing call sites
        (the page's positional `text` + full keyword set, and
        `shared/parallels_service.py`'s all-keyword subset) bind correctly.
        See the module docstring for which parameters are honored vs
        accepted-but-ignored.

        Returns:
            ``{'main': [...], 'filtered': [...],
            'dropped_text_lookup_failures': int}`` -- the third key is
            additive (existing consumers that only read 'main'/'filtered'
            are unaffected); it counts rows that survived the group cap but
            were DROPPED (never rendered, never returned) because
            `get_full_text_by_header` failed for them (finding #16(b)).

        Raises:
            ValueError: if `boundary_mode != 'full'` -- passage-matching has
                no cross-paragraph/token-boundary concept over a letter
                stream, and silently degrading to 'full' would be exactly
                the silent-degradation failure mode this project's
                fail-closed posture exists to prevent (adversarial review
                finding #2). Callers must reject this BEFORE invoking this
                searcher (web/search_api.py does, at request-validation
                time, before any concurrency slot is acquired;
                web/pages/parallels.py disables the boundary controls while
                passage mode is selected).
        """
        if boundary_mode != 'full':
            raise ValueError(
                f"PassageSearcher only supports boundary_mode='full' (got "
                f"{boundary_mode!r}); passage-matching has no cross-"
                f"paragraph/token-boundary concept over a letter stream."
            )

        text = full_text or ''
        # PR #324 round 5: the restriction goes INTO the engine so the
        # candidate/verify caps are spent only on records the caller can
        # receive. Filtering hits afterwards (the old shape, kept below as
        # belt-and-braces) let out-of-set candidates consume the caps and
        # produced false negatives on filtered searches over common texts.
        _allowed = (None if restrict_sys_ids is None else
                    (lambda rid: _extract_sys_id(rid) in restrict_sys_ids))
        hits, report = search_passage(self.index, text, self.policy,
                                      record_allowed=_allowed)

        if min_boundary_matches:
            hits = [h for h in hits if h.n_spans >= min_boundary_matches]

        if restrict_sys_ids is not None:
            hits = [h for h in hits
                    if _extract_sys_id(h.record_id) in restrict_sys_ids]

        # Query-side offset map, computed ONCE per query (not per hit/span --
        # this is O(query length), never O(corpus)) so every rendered span's
        # source_ctx/chunk_hits text can be projected back onto the pasted
        # composition exactly like the manuscript side.
        q_nfc = nfc(text)
        _q_stream, q_offsets = norm_stream(text)
        filter_stream = norm_stream_fast(filter_text) if filter_text else ''

        # Finding #4: chunk_index is a position in the PASTED COMPOSITION,
        # comparable across records -- the ordinal of a span's query-side
        # start offset among ALL distinct start offsets across every
        # surviving hit, computed once and shared by every record. Two
        # different records whose spans start at the same query offset get
        # the SAME chunk_index, mirroring the incumbent's sliding-window
        # index exactly.
        all_q_starts = sorted({span[0] for h in hits for span in h.spans})
        span_index_of_q0 = {q0: i for i, q0 in enumerate(all_q_starts)}

        eligible_rows: list = []
        filtered_candidate_rows: list = []
        hit_by_header: dict = {}
        for hit in hits:
            hit_by_header[hit.record_id] = hit
            row = {
                'uid': _derive_uid(hit.record_id),
                'raw_header': hit.record_id,
                'src_lbl': '',
                'source_ctx': '',
                'text': '',
                'score': float(hit.score),
                'final_score': float(hit.score),
                'chunk_count': int(hit.n_spans),
                'chunk_hits': [],
            }
            if filter_stream and self._is_source_text_filtered(
                hit, filter_stream, q_nfc, q_offsets,
            ):
                # Matches SearchEngine.search_composition_logic's filter_text
                # semantics: a record is routed to `filtered` (never `main`)
                # when the composition text it matched is itself known/
                # printed source text.
                filtered_candidate_rows.append(row)
            else:
                eligible_rows.append(row)

        # Finding #1 ("THE BIG ONE") + finding #16(a): apply the SAME
        # group-cap rule to BOTH buckets, so rendered rows == kept rows
        # always -- see the module docstring for the full rationale on why
        # `filtered` is capped too (unlike the incumbent's own filtered
        # bucket, which is documented as "typically small" on an assumption
        # passage's filter_text mechanism does not share).
        capped_main_candidates, main_truncated = _cap_main_results_by_group(
            eligible_rows, _RegexSysIdParser(), cap=self.render_cap)
        capped_filtered_candidates, _truncated_f = _cap_main_results_by_group(
            filtered_candidate_rows, _RegexSysIdParser(), cap=self.render_cap)

        # Finding #16(b): a row whose text lookup fails is DROPPED and
        # COUNTED, never returned half-blank.
        dropped = 0
        # Raw page text captured during rendering, for the duplicate-
        # photography pass below -- the render already fetched it, so the
        # hygiene pass costs no extra Tantivy lookups.
        raw_text_by_header: dict = {}

        main_results = []
        for row in capped_main_candidates:
            hit = hit_by_header[row['raw_header']]
            if self._render_highlights(row, hit, q_nfc, q_offsets,
                                       span_index_of_q0,
                                       raw_text_out=raw_text_by_header):
                main_results.append(row)
            else:
                dropped += 1

        filtered_results = []
        for row in capped_filtered_candidates:
            hit = hit_by_header[row['raw_header']]
            if self._render_highlights(row, hit, q_nfc, q_offsets,
                                       span_index_of_q0,
                                       raw_text_out=raw_text_by_header):
                filtered_results.append(row)
            else:
                dropped += 1

        # Post-verify Stage-0: duplicate-photography suppression (PR #324
        # round 3 -- the algorithm spec section 9 marks this MANDATORY, and
        # until this pass the helper existed only at its definition). The
        # false-positive class it removes is the one that looks most like a
        # discovery: the same physical page photographed under two catalogue
        # records, measured 36 of 36 among "join anomaly" pairs. Bounded
        # exactly as the plan's "Bounded Stage-0" section requires: each row
        # is compared only against the last DUP_SCAN_WINDOW SURVIVORS in
        # score order (a cluster of N photographs of one page collapses to
        # its best-scored copy with N-1 window-1 comparisons, not N^2), and
        # the demoted count ships in the return dict -- never a silent
        # removal. Demote, not delete: the row moves to `filtered`, already
        # rendered, so a skeptical user can still inspect it.
        dup_demoted = 0
        if len(main_results) > 1:
            survivors: list = []
            for row in main_results:
                dup_of = None
                tb = raw_text_by_header.get(row['raw_header'])
                if tb:
                    for prev in survivors[-DUP_SCAN_WINDOW:]:
                        ta = raw_text_by_header.get(prev['raw_header'])
                        if ta and is_duplicate_photography(ta, tb):
                            dup_of = prev
                            break
                if dup_of is not None:
                    row['filter_reason'] = 'duplicate_photography'
                    filtered_results.append(row)
                    dup_demoted += 1
                else:
                    survivors.append(row)
            main_results = survivors

        return {
            'main': main_results,
            'filtered': filtered_results,
            # PR #324 round 5: this flag used to be computed and DISCARDED.
            # The API path then re-applied the same cap downstream to an
            # already-capped list, concluded nothing was truncated, and
            # omitted truncated_to_200 -- queries with over render_cap
            # manuscript groups silently looked complete.
            'truncated_to_200': bool(main_truncated),
            'dropped_text_lookup_failures': dropped,
            'duplicate_photography_demoted': dup_demoted,
            # The budget/truncation report. QueryReport's own contract says
            # it "ships in the result envelope -- a truncated search that
            # does not say so is a correctness defect"; until PR #324 round 3
            # this method bound it to `_report` and threw it away, so a
            # capped search looked complete to users AND to the evaluation
            # instruments.
            'query_report': report.as_dict(),
        }

    # -- filter_text parity (finding #3) -------------------------------------

    def _is_source_text_filtered(self, hit, filter_stream: str,
                                  q_nfc: str, q_offsets) -> bool:
        """True when the query-side text of ANY span matched on `hit` also
        appears (letter-stream-normalized) inside the caller's filter text.

        Mirrors `SearchEngine.search_composition_logic`'s filter_text
        semantics (shared/search_engine.py): the check is per-RECORD (any
        one matching span is enough to route the WHOLE record to
        `filtered`), not per-span -- same granularity as the incumbent's
        `rec['is_filtered']` flag, which is set once a record accumulates
        ANY text-filtered chunk hit.
        """
        for q0, q1, _r0, _r1, _density in hit.spans:
            span_text = project_span(q_offsets, q0, q1, q_nfc)
            span_stream = norm_stream_fast(span_text)
            if span_stream and span_stream in filter_stream:
                return True
        return False

    # -- bounded re-normalization (display path only) -----------------------

    def _render_highlights(self, row: dict, hit, q_nfc: str, q_offsets,
                            span_index_of_q0: dict,
                            raw_text_out: Optional[dict] = None) -> bool:
        """Fill in `text` / `source_ctx` / `chunk_hits` for ONE row (either a
        surviving `main` row or a surviving `filtered` row -- both are
        group-capped identically, finding #16(a)).

        Per-render work, deliberately -- the display-span contract
        (docs/specs/passage-matching-algorithm.md) requires re-normalizing
        only the pages actually shown, reading original text the same way
        the incumbent path does (Tantivy's stored `content`, via
        `SearchEngine.get_full_text_by_header`). One live Tantivy query per
        row (not batched) and one redundant nfc() pass (`norm_stream` below
        calls it again internally) are both accepted costs -- see the
        module docstring's "deliberately NOT optimized away" note.

        Returns:
            True if the row was successfully rendered (caller should keep
            it); False if the text lookup failed (caller must DROP the row
            and count it -- finding #16(b): never return a silently blank
            row that survived the cap but has no real content).
        """
        orig_text = None
        try:
            orig_text = self.text_fetcher.get_full_text_by_header(hit.record_id)
        except Exception:
            logger.warning(
                'passage_parallels: text fetch raised for record %s',
                hit.record_id, exc_info=True,
            )
        if not orig_text:
            logger.warning(
                'passage_parallels: dropping record %s -- text lookup failed '
                '(counted, not returned as a blank row)', hit.record_id,
            )
            return False

        if raw_text_out is not None:
            # For the duplicate-photography pass: line_agreement needs the
            # ORIGINAL text with its line breaks (line breaks are a property
            # of the physical page), and this is the one place it is fetched.
            raw_text_out[hit.record_id] = orig_text

        r_nfc = nfc(orig_text)  # redundant with norm_stream's internal nfc() -- accepted cost
        _r_stream, r_offsets = norm_stream(orig_text)

        src_snips, ms_snips, chunk_hits = [], [], []
        for q0, q1, r0, r1, _density in hit.spans:
            src_snips.append(_highlight_span(q_nfc, q_offsets, q0, q1))
            ms_snip = _highlight_span(r_nfc, r_offsets, r0, r1)
            ms_snips.append(ms_snip)
            plain_query_text = project_span(q_offsets, q0, q1, q_nfc)
            chunk_index = span_index_of_q0.get(q0, 0)
            chunk_hits.append((chunk_index, plain_query_text, float(q1 - q0), ms_snip))

        row['source_ctx'] = "\n\n".join(src_snips)
        row['text'] = "\n...\n".join(ms_snips)
        row['chunk_hits'] = chunk_hits
        return True
