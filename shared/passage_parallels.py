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
  merged-span character count) -- directly comparable to the incumbent's
  merged-span character score (`base_score` in search_engine.py), which is
  why passage never computes a boundary-boosted `final_score`: there is no
  boundary-crossing concept over a letter stream with no token boundaries.
* Highlight text (`text`, `source_ctx`, and each `chunk_hits` tuple's
  `manuscript_snippet`) is built ONLY for the top-N rendered rows (N =
  `shared.parallels_service.PARALLELS_GROUP_CAP`, the same cap the
  incumbent's results are ultimately capped to before a client ever sees
  them) -- a BOUNDED re-normalization, per the plan's display-span
  contract. Rows beyond the cap still carry every other field (so a caller
  that groups/sorts/caps AFTER this call, as `parallels_service` does, sees
  a complete row set) but with empty `text`/`source_ctx`/`chunk_hits`.
* `filtered` is always `[]`. The incumbent's filtered bucket exists because
  a Tantivy chunk can exceed a frequency ceiling (`max_freq`); passage has
  no equivalent per-chunk frequency signal (its own posting budget already
  bounds retrieval internally, invisibly to the caller), so every accepted
  record is a "main" result.
* `mode` (exact/variants/fuzzy), `max_freq`, `filter_text`,
  `progress_callback`, `boundary_delimiter`, `boundary_boost` and
  `min_delimiter_distance` are accepted (for call-shape compatibility with
  the real `SearchEngine.search_composition_logic`, which both
  `web/pages/parallels.py` and `shared/parallels_service.py` invoke with
  the full keyword set) but IGNORED -- none has a passage-matching
  equivalent: character-level Levenshtein matching has no morphological
  variant expansion, no per-chunk frequency signal, and no paragraph-token
  boundary concept. `min_boundary_matches` (the "min. chunk matches" UI
  control in 'full' boundary_mode) IS honored, since it is a trivial
  post-filter on `n_spans` and skipping it would silently ignore a visible
  control. `corpus_scope` is accepted but ignored: the passage index never
  holds Local-corpus records regardless of its value, and disabling
  `passage` for non-Genizah scope is the CALLER's job (API validation / the
  page's method selector), not this searcher's -- see web/search_api.py.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Optional, Protocol

from shared.parallels_service import PARALLELS_GROUP_CAP
from shared.passage_index import PassageIndex
from shared.passage_normalize import nfc, norm_stream, project_span
from shared.passage_policy import PassagePolicy, get_preset
from shared.passage_search import search_passage

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


def _highlight_span(orig_text: str, offsets, start: int, end: int,
                     pad: int = HIGHLIGHT_CONTEXT_PAD) -> str:
    """Build a `*match*`-marked snippet around normalized-stream span
    [start, end) inside `orig_text`, mirroring
    shared/search_engine.py's ms_snip formula exactly:
    ``content[start:s] + f"*{content[s:e]}*" + content[e:end]``.

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
    return orig_text[win_start:s] + f"*{orig_text[s:e]}*" + orig_text[e:win_end]


class PageTextFetcher(Protocol):
    """Structural type for the injected text source (SEED-016 #3 style).

    Satisfied by the process-singleton SearchEngine
    (`shared/search_engine.py::SearchEngine.get_full_text_by_header`, added
    alongside the pre-existing `get_full_text_by_id`) without importing it
    here -- `shared/` stays framework-agnostic; the caller
    (`web/passage_assets.py`) injects the real engine.
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
        """
        text = full_text or ''
        hits, _report = search_passage(self.index, text, self.policy)

        if boundary_mode == 'full' and min_boundary_matches:
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

        main_results = []
        for rank, hit in enumerate(hits):
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
            if rank < self.render_cap:
                self._render_highlights(row, hit, q_nfc, q_offsets)
            main_results.append(row)

        return {'main': main_results, 'filtered': []}

    # -- bounded re-normalization (display path only) -----------------------

    def _render_highlights(self, row: dict, hit, q_nfc: str, q_offsets) -> None:
        """Fill in `text` / `source_ctx` / `chunk_hits` for ONE rendered row.

        Per-render work, deliberately -- the display-span contract
        (docs/specs/passage-matching-algorithm.md) requires re-normalizing
        only the pages actually shown, reading original text the same way
        the incumbent path does (Tantivy's stored `content`, via
        `SearchEngine.get_full_text_by_header`). Never called for rows
        beyond `render_cap`.
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
            return

        r_nfc = nfc(orig_text)
        _r_stream, r_offsets = norm_stream(orig_text)

        src_snips, ms_snips, chunk_hits = [], [], []
        for i, (q0, q1, r0, r1, _density) in enumerate(hit.spans):
            src_snips.append(_highlight_span(q_nfc, q_offsets, q0, q1))
            ms_snip = _highlight_span(r_nfc, r_offsets, r0, r1)
            ms_snips.append(ms_snip)
            plain_query_text = project_span(q_offsets, q0, q1, q_nfc)
            chunk_hits.append((i, plain_query_text, float(q1 - q0), ms_snip))

        row['source_ctx'] = "\n\n".join(src_snips)
        row['text'] = "\n...\n".join(ms_snips)
        row['chunk_hits'] = chunk_hits
