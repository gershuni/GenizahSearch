# -*- coding: utf-8 -*-
"""The ONE builder for a `/browse` link off a discovery surface.

Three surfaces link a reader from a computed identification to the manuscript it
is about -- the findings page's preview iframe, the connections panel's
"other manuscripts carrying this work" expansion, and the panel's unevaluated
candidate alignments -- and every one of them has to answer the same two
questions: which manuscript, and which folio. Before this module they answered
them three times, in three modules, and drifted:

* the findings preview named no folio at all, so it opened folio 1 of every
  manuscript (owner report, 2026-08-08);
* the expansion rows named no folio either;
* the candidate-alignment rows named a folio and NOT its volume, which is worse
  than naming neither -- see `browse_url` below.

`web/components/discovery_panel.py` already recorded the gap in a comment
("there is no shared builder to call, and this is not the place to invent a
second one"). This is that builder. It is deliberately dependency-free -- no
NiceGUI, no app state, no service -- so every consumer can import it and a test
can exercise it without a client.

NOTHING HERE PARSES A PAGE ID. The composite
`{sys_id}_{ie_id}_P{n:06d}_{fl_id}` is the service's to take apart
(`shared/discovery_service.py::_browse_address_from_page_id`); these functions
receive the two components already separated, precisely so no surface has a
reason to hold the composite id -- the defect `get_related_pages_enveloped`
carries a scar from, when the panel rendered
`990051620920205171_IE167198813_P000003_FL167198817` where a shelfmark belongs.
"""

from __future__ import annotations

from typing import Any, Optional
from urllib.parse import quote

__all__ = ["browse_target_is_a_folio", "browse_url"]


def browse_target_is_a_folio(page: Any, volume_ie: Any) -> bool:
    """Whether `(page, volume_ie)` is a COMPLETE folio address.

    The single predicate every consumer asks, so a surface's PROSE and its LINK
    cannot answer it differently. That is not hypothetical: on the findings page
    the note asked "is there a folio?" while the URL asked "is there a folio AND
    a volume?", so a row carrying one without the other printed a sentence
    promising the matched folio above a link that opened the manuscript (Codex
    review, 2026-08-08).
    """
    return isinstance(page, int) and page > 0 and bool(volume_ie)


def browse_url(sys_id: Any, *, page: Any = None, volume_ie: Any = None,
               embed: bool = False) -> Optional[str]:
    """A `/browse` address for `sys_id`, at a folio when one is fully known.

    `None` when there is no `sys_id`: a link is WITHHELD rather than pointed at a
    page that cannot resolve.

    `page` AND `volume_ie` ARE ADDED TOGETHER OR NOT AT ALL, and this is the
    property the whole module exists to hold. A MULTI-VOLUME manuscript numbers
    its folios PER VOLUME, so `page=7` with no volume is not an approximate
    address -- it is a different page in each volume, and `/browse` resolves it
    against whichever volume it happens to open. 988 of the served artifact's
    53,581 identifications span more than one volume. Half an address is
    therefore worse than none: it looks targeted and lands somewhere else, and
    a reader who cannot see that they are in the wrong volume concludes the
    identification is wrong rather than the link.

    `embed=True` adds the `?embed=1` route built for the discovery-review
    iframe and reused by `/atlas`: a bare viewer that ALSO disables browse
    snapshot restore/persist (`web/pages/browse.py`, `embedded=True`). That
    second property is why it exists here -- a preview rendered inside another
    page must not overwrite wherever the reader had left `/browse`. It is NOT
    set for ordinary shelfmark links, which are navigations the reader meant.
    """
    if not sys_id:
        return None
    url = f"/browse?sys_id={quote(str(sys_id))}"
    if embed:
        url += "&embed=1"
    if browse_target_is_a_folio(page, volume_ie):
        url += f"&page={int(page)}&volume_ie={quote(str(volume_ie))}"
    return url
