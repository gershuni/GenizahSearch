# -*- coding: utf-8 -*-
"""Uniform `retrieve(text) -> [record_id, ...]` wrappers for both methods.

The comparison is only meaningful if both methods are asked the same question
and answered in the same units, so every difference between them lives here
and nowhere else in the harness.

Two things both adapters must do, and neither does by default:

* PAGE-SCOPE. The Tantivy index also holds continuous `sys:`/`part:`
  pseudo-documents, and composition search does not filter them out --
  `parse_query(t_query, ["content"])` has no scope clause. The passage index
  is page-only by construction. Comparing them unscoped would measure the
  difference in document sets, not in methods.

* RECORD-ID UNITS. A page hit's `raw_header` is already the corpus record id
  verbatim; the passage index returns record ids directly. Verified, not
  assumed.

`max_freq` is a real incumbent tuning axis, not a constant: at the desktop
default of 10, a verbatim 60-word query of a real page returned NOTHING in
`main` because every chunk exceeded the frequency ceiling and was routed to
`filtered`. At 100 the same query returns the correct page at rank 0. The
comparison sweeps it, exactly as it sweeps the passage policy -- tuning one
method against the other's defaults would be a rigged result.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

PSEUDO_DOC_PREFIXES = ('sys:', 'part:')


def eligible_record_ids(index) -> frozenset:
    """The shared document set: every record the passage index actually holds.

    Applying this to the incumbent is what makes the two methods answer over
    the same corpus. Without it the incumbent searches 246,083 records the
    other cannot see.
    """
    return frozenset(index.record_id(i) for i in range(index.n_records))


def is_page_row(row: dict) -> bool:
    """False for continuous pseudo-document hits.

    Their `raw_header` names the first page of the aggregate, not the matched
    location, so counting them as page hits would credit a match to the wrong
    record.
    """
    return not str(row.get('uid', '')).startswith(PSEUDO_DOC_PREFIXES)


@dataclass
class ChunkRetriever:
    """The incumbent: SearchEngine.search_composition_logic, page-scoped.

    `eligible` is the EQUAL-ELIGIBILITY control and it matters more than it
    looks. The Tantivy corpus holds 948,549 page records; the passage index
    holds 702,466, because Stage-0 excludes 246,083 (short pages, microfilm
    target sheets, library ownership stamps). Unfiltered, the incumbent can
    return records the passage engine structurally cannot, which makes any
    comparison a measurement of two different document sets rather than of two
    methods.

    Two legitimate framings, and they answer different questions:
      * eligible set applied   -> compares the METHODS (the plan's primary)
      * eligible set omitted   -> compares the PRODUCTS as they ship today
    Whichever is used must be declared, because the difference is 26% of the
    corpus.
    """
    engine: object
    chunk_size: int = 5
    mode: str = 'exact'
    max_freq: int = 100
    include_filtered: bool = False
    eligible: Optional[frozenset] = None

    @property
    def config_id(self) -> str:
        return (f'chunk-c{self.chunk_size}-{self.mode}-f{self.max_freq}'
                f'{"-incl" if self.include_filtered else ""}'
                f'{"-elig" if self.eligible is not None else "-alldocs"}')

    def retrieve(self, text: str) -> list:
        res = self.engine.search_composition_logic(
            text, self.chunk_size, self.max_freq, self.mode)
        rows = list(res.get('main') or [])
        if self.include_filtered:
            rows += list(res.get('filtered') or [])
        out, seen = [], set()
        for r in rows:
            if not is_page_row(r):
                continue
            rid = str(r.get('raw_header') or '')
            if not rid or rid in seen:
                continue
            if self.eligible is not None and rid not in self.eligible:
                continue
            seen.add(rid)
            out.append(rid)
        return out


@dataclass
class PassageRetriever:
    """The new engine: search_passage over the corpus-resident index."""
    index: object
    policy: object
    top_k: Optional[int] = None

    @property
    def config_id(self) -> str:
        return f'passage-{self.policy.name}-{self.policy.policy_id}'

    def retrieve(self, text: str) -> list:
        from shared.passage_search import search_passage
        hits, _report = search_passage(self.index, text, self.policy)
        if self.top_k is not None:
            hits = hits[:self.top_k]
        return [h.record_id for h in hits]
