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
  contract. Row count per bucket is bounded upstream by `policy.verify_cap`
  (shared/passage_policy.py: at most one hit per verified candidate), at a
  measured ~1.3 ms/row render cost. This is an ACCEPTED cost, not further
  sub-capped, specifically so "rendered == kept" holds even in the worst
  case.

  **That bound is 3,000 only at `normal` depth.** This paragraph used to
  say "default 3,000 ... comfortably inside SEARCH_API_PASSAGE_TIMEOUT
  (30s)", which was true when written and stopped being true when the
  search-depth axis raised `verify_cap` to 50,000 for `deep` and `deepest`
  -- a 16x larger bound argued as safe on the smaller one (Codex review,
  PR #328).

  Measured on the real index (759,224 records) with a real 7,562-char query
  at `max-40+short`. `hits` and `search` are observed; `render` is the hit
  count times the ~1.3 ms/row constant above, so it is arithmetic rather
  than a fourth measurement:

      depth     verify_cap    hits   search   render*   total
      normal         3,000     272     1.2s     0.4s     1.5s
      deep          50,000     725     6.3s     0.9s     7.3s
      deepest       50,000   2,431    15.5s     3.2s    18.6s

  Acceptance runs at roughly 5% of `verify_cap`, not 100%, so the ~65s
  render a full cap would imply does not occur. What HAS gone is the
  margin: `deepest` spends about two thirds of the 30s budget where this
  paragraph once described a tenth of it.

  The breach arrives at roughly **11,150 rendered rows** -- (30s - 15.5s
  search) / 1.3 ms -- about 4.6x anything measured.

  Three corrections to an earlier version of this note, all of which made
  the margin look different than it is (Codex review, PR #328):

  * It said ~8,000 rows. That was (30 - 18.6) / 1.3 ms, the rows that fit
    ON TOP of the 2,431 already rendered, written as though it were the
    total. Subtract the search cost, not the whole measured run.
  * It said a `filter_text` halves the threshold "since both buckets
    render", and then that the pre-existing "worst-case full cap on BOTH
    buckets" was 2x too pessimistic. Both statements are wrong, because
    the answer depends on `render_cap` and neither said which regime it
    meant (Codex, again, on the test written to pin the first correction):

      - `render_cap == 0` -- the PAGE path, and the one this threshold is
        about. No cap, so each hit lands in `filtered_candidate_rows` OR
        `eligible_rows` (one `if/else`, one loop), the buckets PARTITION
        the hits, and every row renders exactly once. A `filter_text`
        redistributes the cost and adds none.
      - `render_cap > 0` -- the API path. The two buckets are capped
        INDEPENDENTLY, so a filter splitting hits across both raises the
        rendered total from one cap to as many as two. Measured on a
        4-group fixture at `render_cap=2`: 6 rows unfiltered, 12 with a
        mixed filter. The pre-existing "full cap on BOTH buckets" was
        RIGHT for this path.

    `tests/test_passage_parallels.py::test_the_two_buckets_partition_the_hits`
    now pins both regimes, because this paragraph has been corrected three
    times and been wrong in a different direction each time.
  * 11,150 assumes `search` stays at 15.5s while accepting 4.6x more rows,
    and verification is what PRODUCES rows -- so a query returning 11,000
    hits spends longer searching too, and the real breach comes earlier.
    Treat it as an upper bound, not an estimate.

  A render sub-cap was considered and NOT added: it would partially undo
  the owner's 2026-08-23 ruling that `render_cap=0` exists so found
  manuscripts stop being hidden (198 shown of 497 found on Birkat Hamazon),
  and the measurement does not justify that price. Anyone widening
  `verify_cap` further should redo the table above rather than trust this
  one -- which is exactly the mistake this note records.
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
from shared.passage_fusion import fuse, tag_rows, witness_id_for
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

# A witness reference is a page `raw_header` -- the exact value carried on
# every result row and stored verbatim in Tantivy's `full_header` field, e.g.
# `990001234560205171_IE12345_P00001_FL678`. Alphanumerics and underscores,
# nothing else. THIS regex is the authoritative one (see
# `PassageSearcher._resolve_witnesses`); web/search_api.py's copy is fail-fast
# UX for a caller's benefit, not a second boundary.
_WITNESS_REF_RE = re.compile(r'^[A-Za-z0-9_]+$')


class NoWitnessesResolved(ValueError):
    """Every witness in a non-empty witness list failed to resolve.

    Carries the same `witness_report` the success path returns, so the caller
    can tell the user WHICH witness failed and why. An empty result set would
    be indistinguishable from an honest "no matches" -- the one thing a
    search must never be ambiguous about.
    """

    def __init__(self, report: dict):
        super().__init__('no witness resolved to searchable text')
        self.report = report


@dataclass
class _WitnessRun:
    """One witness's search plus everything its rows need to be rendered.

    `q_nfc`, `q_offsets` and `span_index_of_q0` are PER WITNESS and are only
    meaningful against that witness's own text -- which is why a fused row
    records the witness that produced it and the renderer looks the context
    up by that id rather than reusing whichever one happens to be in scope.
    """
    wid: str
    label: str
    hits: list
    q_nfc: str
    q_offsets: object
    span_index_of_q0: dict
    report: dict


def _synthesize_query_report(reports: list) -> dict:
    """Collapse N witnesses' QueryReports into the ONE `query_report` the
    envelope contract exposes.

    Booleans are OR-ed and counters summed, so a truncation on witness 7
    still reaches the caller. Passing through only the first witness's report
    -- the obvious shape -- would under-report exactly the case the report
    exists for: "a truncated search that does not say so is a correctness
    defect" (shared/passage_search.py::QueryReport).

    A single report is returned UNCHANGED (identity, not a rebuild), which is
    what keeps the single-witness result byte-identical to the pre-witness
    behaviour.
    """
    if not reports:
        return {}
    if len(reports) == 1:
        return reports[0]
    out: dict = {}
    for key, value in reports[0].items():
        if isinstance(value, bool):
            out[key] = any(bool(r.get(key)) for r in reports)
        elif isinstance(value, (int, float)):
            out[key] = sum((r.get(key) or 0) for r in reports)
        else:
            # policy_id / policy_name: identical across witnesses (one policy
            # per request), so the first is the whole truth.
            out[key] = value
    return out


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
    # <= 0 means UNCAPPED (owner ruling 2026-08-23, the Birkat Hamazon
    # session): return every group, fully rendered. The page passes 0 -- its
    # display layer already batches 50 groups per "Load more" click, and the
    # export layer caps at 5,000 rows, so the 200-group cap here was hiding
    # 299 of 497 found manuscripts from BOTH surfaces for no benefit. The
    # API path keeps the default: its envelope contract is 200 groups, and
    # rendering rows the service would immediately discard is pure waste.
    # Cost bound when uncapped: verify_cap (3,000 records) bounds the render
    # at ~4s worst case, inside every timeout that guards this path.
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
        *,
        witnesses: Optional[list] = None,
        witness_text_cap: Optional[int] = None,
        **_ignored,
    ) -> dict:
        """Same parameter names/order as
        `SearchEngine.search_composition_logic` so both existing call sites
        (the page's positional `text` + full keyword set, and
        `shared/parallels_service.py`'s all-keyword subset) bind correctly.
        See the module docstring for which parameters are honored vs
        accepted-but-ignored.

        `witnesses` (keyword-only, additive) is the multi-witness entry
        point: a list of `{'id', 'label', 'text' | 'raw_header'}` dicts, each
        searched SEPARATELY and fused by rank. Omitted or empty, this method
        behaves byte-for-byte as it did before the parameter existed -- which
        is what makes every existing caller and test safe. `witnesses`
        resolving to exactly ONE text short-circuits into the single-witness
        path too: RRF over one list is a `1/(k+rank)` rescale that carries no
        information, and `score` must keep meaning "matched letters" for the
        common case however it arrived.

        Note the `*` -- `witnesses` is keyword-only. The chunk engine's
        `SearchEngine.search_composition_logic` has a fixed parameter list
        with no `**kwargs`, so a `witnesses=` aimed at the wrong searcher
        raises TypeError rather than being silently swallowed.

        Returns:
            ``{'main': [...], 'filtered': [...], 'truncated_to_200': bool,
            'dropped_text_lookup_failures': int,
            'duplicate_photography_demoted': int, 'query_report': {...}}``,
            plus -- ONLY when `witnesses` was passed at all --
            ``'per_witness_query_reports'`` and ``'witness_report'``. The
            gate is "witnesses were requested", NOT "two or more resolved":
            asking for seventeen and searching sixteen is precisely the case
            a caller has to be able to see, and it would be hidden by an
            N>=2 gate whenever the drop left a single survivor. Omitting
            `witnesses` leaves the result byte-identical to what it was
            before this parameter existed, which is the parity that protects
            every existing consumer.

            For N>=2 `query_report` is SYNTHESISED across the witnesses
            (booleans OR-ed, counters summed) rather than being one witness's
            report. Reporting only the first witness's would under-report a
            truncation that happened on witness 7, and "a truncated search
            that does not say so is a correctness defect" (QueryReport's own
            contract).

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
            ValueError: if BOTH `full_text` and `witnesses` are non-empty.
                Never silently pick one: the query the user believes was
                searched would differ from the one that ran.
            NoWitnessesResolved: if `witnesses` was non-empty but not one
                entry resolved to searchable text. Carries `.report`, so the
                caller can say WHICH failed and why instead of returning an
                empty result indistinguishable from "no matches".
        """
        if boundary_mode != 'full':
            raise ValueError(
                f"PassageSearcher only supports boundary_mode='full' (got "
                f"{boundary_mode!r}); passage-matching has no cross-"
                f"paragraph/token-boundary concept over a letter stream."
            )

        queries, witness_report = self._resolve_witnesses(
            witnesses, full_text, witness_text_cap)
        multi = len(queries) > 1

        runs = [self._run_one_witness(wid, label, text, restrict_sys_ids,
                                      min_boundary_matches)
                for wid, label, text in queries]

        filter_stream = norm_stream_fast(filter_text) if filter_text else ''

        # Per-witness buckets. `hit_by_key` is keyed by (witness, record) --
        # NOT by record alone, which is the shape the single-witness code
        # used and which COLLIDES the moment a second witness matches the
        # same manuscript. Last-witness-wins there would silently reduce
        # every witness_count to 1 while looking entirely healthy.
        eligible_by_witness: dict = {}
        filtered_by_witness: dict = {}
        hit_by_key: dict = {}
        run_by_witness: dict = {}
        for run in runs:
            run_by_witness[run.wid] = run
            rows: list = []
            for hit in run.hits:
                hit_by_key[(run.wid, hit.record_id)] = hit
                rows.append({
                    'uid': _derive_uid(hit.record_id),
                    'raw_header': hit.record_id,
                    'src_lbl': '',
                    'source_ctx': '',
                    'text': '',
                    'score': float(hit.score),
                    'final_score': float(hit.score),
                    'chunk_count': int(hit.n_spans),
                    'chunk_hits': [],
                })
            if multi:
                # Tag BEFORE the filter split: a witness's rank is its
                # position in that witness's FULL result list. A rank derived
                # from the post-filter list would shift with the caller's
                # filter text, changing the fusion for a reason that has
                # nothing to do with match quality.
                tag_rows(rows, run.wid, run.label)
            eligible, source_filtered = [], []
            for row, hit in zip(rows, run.hits):
                if filter_stream and self._is_source_text_filtered(
                    hit, filter_stream, run.q_nfc, run.q_offsets,
                ):
                    # Matches SearchEngine.search_composition_logic's
                    # filter_text semantics: a record is routed to `filtered`
                    # (never `main`) when the composition text it matched is
                    # itself known/printed source text.
                    source_filtered.append(row)
                else:
                    eligible.append(row)
            eligible_by_witness[run.wid] = eligible
            filtered_by_witness[run.wid] = source_filtered

        if multi:
            eligible_rows = fuse([(r.wid, eligible_by_witness[r.wid])
                                  for r in runs])
            fused_filtered = fuse([(r.wid, filtered_by_witness[r.wid])
                                   for r in runs])
            # A record is `filtered` only when EVERY witness that matched it
            # filtered it. One witness matching it on text the caller did NOT
            # declare as a known source is a real result, and suppressing it
            # would make the filter STRICTER the more witnesses you add --
            # the opposite of what the control says it does.
            in_main = {row['raw_header'] for row in eligible_rows}
            filtered_candidate_rows = [r for r in fused_filtered
                                       if r['raw_header'] not in in_main]
            # The cap must ORDER by the key the rows were SELECTED by, or it
            # discards exactly the groups the fusion promoted. It must NOT
            # change what aggregate_score reports -- that becomes the public
            # `score`, which stays matched letters (review finding).
            order_key = 'fusion_score'
        else:
            only = runs[0]
            eligible_rows = eligible_by_witness[only.wid]
            filtered_candidate_rows = filtered_by_witness[only.wid]
            order_key = None

        # Finding #1 ("THE BIG ONE") + finding #16(a): apply the SAME
        # group-cap rule to BOTH buckets, so rendered rows == kept rows
        # always -- see the module docstring for the full rationale on why
        # `filtered` is capped too (unlike the incumbent's own filtered
        # bucket, which is documented as "typically small" on an assumption
        # passage's filter_text mechanism does not share).
        if self.render_cap and self.render_cap > 0:
            capped_main_candidates, main_truncated = _cap_main_results_by_group(
                eligible_rows, _RegexSysIdParser(), cap=self.render_cap,
                order_key=order_key)
            capped_filtered_candidates, _truncated_f = _cap_main_results_by_group(
                filtered_candidate_rows, _RegexSysIdParser(), cap=self.render_cap,
                order_key=order_key)
        else:
            # Uncapped: every group survives, so nothing is truncated by
            # definition and "rendered == kept" holds over the full set.
            capped_main_candidates = eligible_rows
            capped_filtered_candidates = filtered_candidate_rows
            main_truncated = False

        # Finding #16(b): a row whose text lookup fails is DROPPED and
        # COUNTED, never returned half-blank.
        dropped = 0
        # Raw page text captured during rendering, for the duplicate-
        # photography pass below -- the render already fetched it, so the
        # hygiene pass costs no extra Tantivy lookups.
        raw_text_by_header: dict = {}

        def _render(row: dict) -> bool:
            # The WINNING witness supplies the evidence. A span offset is a
            # position in ONE witness's text, so projecting it through
            # another witness's offset map would highlight the wrong letters
            # -- and would still look entirely plausible on screen.
            run = run_by_witness.get(row.get('witness_id') or runs[0].wid,
                                     runs[0])
            hit = hit_by_key[(run.wid, row['raw_header'])]
            return self._render_highlights(row, hit, run.q_nfc, run.q_offsets,
                                           run.span_index_of_q0,
                                           raw_text_out=raw_text_by_header)

        main_results = []
        for row in capped_main_candidates:
            if _render(row):
                main_results.append(row)
            else:
                dropped += 1

        filtered_results = []
        for row in capped_filtered_candidates:
            if _render(row):
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

        result = {
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
            'query_report': _synthesize_query_report([r.report for r in runs]),
        }
        if witness_report:
            # Present whenever `witnesses` was PASSED -- not merely when two
            # or more resolved. Requesting seventeen and searching sixteen is
            # exactly the case a caller must be able to see, and gating this
            # on N>=2 would hide it whenever the drop left one survivor.
            # Absent when `witnesses` was omitted, which is the parity that
            # protects every existing consumer.
            result['per_witness_query_reports'] = [
                {'witness_id': r.wid, 'witness_label': r.label,
                 'report': r.report} for r in runs
            ]
            result['witness_report'] = witness_report
        return result

    # -- multi-witness plumbing ---------------------------------------------

    def _resolve_witnesses(self, witnesses, full_text: str,
                           text_cap: Optional[int]):
        """Turn the caller's witness list into `[(id, label, text), ...]`.

        THE single place a `raw_header` reference is validated and resolved.
        `web/search_api.py` also shape-checks the field, but that is
        fail-fast UX and never a second security boundary -- two copies of a
        validation rule drift, and the copy running closest to the fetch is
        the one that has to be authoritative.

        Resolution happens HERE, inside the searcher, because it is Tantivy
        work: doing it in the route handler would block the single uvicorn
        event loop, and this method already runs inside a bounded executor.

        The length cap is re-checked AFTER resolution, because twenty-five
        tiny references can resolve to twenty-five 20,000-character pages --
        a payload-only cap bounds the REQUEST, not the WORK.

        An unresolvable witness is SKIPPED AND REPORTED, never fatal:
        rejecting a seventeen-witness request over one stale reference wastes
        the sixteen searches the user asked for and can still have.
        """
        if not witnesses:
            return [('seed', '', full_text or '')], {}
        if (full_text or '').strip():
            raise ValueError(
                'PassageSearcher: pass EITHER full_text OR witnesses, never '
                'both -- silently searching one of them would make the query '
                'that actually ran invisible to the caller.'
            )

        queries: list = []
        entries: list = []
        for i, w in enumerate(witnesses):
            w = w if isinstance(w, dict) else {'text': w}
            wid = str(w.get('id') or witness_id_for(i))
            label = str(w.get('label') or '')
            text = w.get('text')
            kind = 'pasted'
            reason = None
            if text is None or not str(text).strip():
                kind = 'manuscript'
                text = None
                ref = str(w.get('raw_header') or '').strip()
                if not ref:
                    reason = 'empty'
                elif not _WITNESS_REF_RE.match(ref):
                    reason = 'bad_ref'
                else:
                    try:
                        text = self.text_fetcher.get_full_text_by_header(ref)
                    except Exception:
                        logger.warning(
                            'passage_parallels: witness ref lookup raised for '
                            '%s', ref, exc_info=True,
                        )
                        text = None
                    if not text:
                        reason = 'not_found'
            if reason is None and text_cap and len(text) > text_cap:
                reason = 'too_long'
            entries.append({
                'id': wid,
                'label': label,
                'kind': kind,
                'resolved': reason is None,
                'reason': reason,
                'letters': len(text or '') if reason is None else 0,
            })
            if reason is None:
                queries.append((wid, label, text))

        report = {
            'requested': len(witnesses),
            'searched': len(queries),
            'witnesses': entries,
            'unresolved': [e for e in entries if not e['resolved']],
        }
        if not queries:
            raise NoWitnessesResolved(report)
        return queries, report

    def _run_one_witness(self, wid: str, label: str, text: str,
                         restrict_sys_ids, min_boundary_matches: int):
        """Search ONE witness and build everything its rendering will need.

        Never call this with two witnesses joined into one string. The
        passage engine spends a per-query POSTING BUDGET, so a concatenated
        query starves: the 17 Birkat Hamazon witnesses joined into one query
        admit 2.4% of their own postings and reach 48.2% of the reachable
        census -- WORSE than the best single witness (56.7%) and well under
        the 74.1% the same 17 reach fused. See
        shared/passage_fusion.py for the numbers, and for why this does not
        generalise to the chunk engine.
        """
        text = text or ''
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

        # Query-side offset map, computed ONCE per witness (not per hit/span
        # -- this is O(query length), never O(corpus)) so every rendered
        # span's source_ctx/chunk_hits text can be projected back onto that
        # witness's own text exactly like the manuscript side.
        q_nfc = nfc(text)
        _q_stream, q_offsets = norm_stream(text)

        # Finding #4: chunk_index is a position in the PASTED COMPOSITION,
        # comparable across records -- the ordinal of a span's query-side
        # start offset among ALL distinct start offsets across every
        # surviving hit, computed once and shared by every record. Two
        # different records whose spans start at the same query offset get
        # the SAME chunk_index, mirroring the incumbent's sliding-window
        # index exactly.
        #
        # It is PER WITNESS, and comparable only WITHIN one: offset 200 of
        # witness A and offset 200 of witness B are different places in
        # different texts. Every fused row records which witness produced it
        # (`witness_id`), so the index is never read out of its context.
        all_q_starts = sorted({span[0] for h in hits for span in h.spans})
        return _WitnessRun(
            wid=wid, label=label, hits=hits, q_nfc=q_nfc, q_offsets=q_offsets,
            span_index_of_q0={q0: i for i, q0 in enumerate(all_q_starts)},
            report=report.as_dict(),
        )

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
