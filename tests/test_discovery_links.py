# -*- coding: utf-8 -*-
"""The ONE `/browse` link builder the three discovery surfaces share.

Its whole job is a property that is invisible in any single call and only shows
up across surfaces: a folio number and its volume are HALF AN ADDRESS EACH.
Before this module the three link sites answered that question three different
ways -- the findings preview named no folio, the panel's expansion named none
either, and the panel's candidate alignments named a folio with NO volume, which
for a multi-volume manuscript is a real page in the wrong place.
"""

from __future__ import annotations

import pytest

from web.components.discovery_links import browse_target_is_a_folio, browse_url

_SYS = "990000895680205171"


def test_no_sys_id_means_no_link_at_all():
    """A link is WITHHELD rather than pointed at a page that cannot resolve.

    `/browse?sys_id=None` is a dead end dressed as a destination, and it is what
    a naive f-string produces -- the panel's candidate-alignment row emitted
    exactly that shape before this builder existed.
    """
    for empty in (None, "", 0, False):
        assert browse_url(empty) is None, empty
        assert browse_url(empty, page=7, volume_ie="IE1") is None, empty


def test_the_manuscript_alone_is_a_complete_and_honest_target():
    assert browse_url(_SYS) == f"/browse?sys_id={_SYS}"


@pytest.mark.parametrize("page,volume", [
    (7, None),            # a folio with no volume -- the panel's shipped defect
    (None, "IE47974133"),  # a volume with no folio
    (0, "IE47974133"),     # a folio number /browse would clamp to page 1
    (-3, "IE47974133"),
    ("7", "IE47974133"),   # a string, which would build `page=7` and typecheck later
    (None, None),
    (7, ""),
])
def test_HALF_an_address_is_refused_and_degrades_to_the_manuscript(page, volume):
    """The single property this module exists for.

    Half an address is worse than none: it LOOKS targeted and lands somewhere
    else. Folio numbering is per volume, so `page=7` with no volume is a
    different page in each volume of a multi-volume manuscript and `/browse`
    resolves it against whichever one it happens to open -- and a reader who
    cannot see that they are in the wrong volume concludes the identification is
    wrong rather than the link.
    """
    assert browse_url(_SYS, page=page, volume_ie=volume) == (
        f"/browse?sys_id={_SYS}")
    assert browse_target_is_a_folio(page, volume) is False


def test_a_COMPLETE_address_carries_both_components():
    assert browse_url(_SYS, page=7, volume_ie="IE47974133") == (
        f"/browse?sys_id={_SYS}&page=7&volume_ie=IE47974133")
    assert browse_target_is_a_folio(7, "IE47974133") is True


def test_embed_is_opt_in_and_survives_the_folio():
    """`?embed=1` is the bare-viewer route that ALSO disables browse snapshot
    restore/persist -- the property that keeps a preview rendered inside another
    page from overwriting wherever the reader had left `/browse`. It is opt-in
    because an ordinary shelfmark link is a navigation the reader meant."""
    assert "embed=1" not in browse_url(_SYS)
    assert browse_url(_SYS, embed=True) == f"/browse?sys_id={_SYS}&embed=1"
    assert browse_url(_SYS, page=7, volume_ie="IE1", embed=True) == (
        f"/browse?sys_id={_SYS}&embed=1&page=7&volume_ie=IE1")


def test_every_component_is_URL_QUOTED():
    """These values come out of an artifact, not out of this codebase, so the
    builder quotes rather than trusting their shape."""
    url = browse_url("a b&c", page=7, volume_ie="IE 1&x")
    assert "a%20b%26c" in url and "IE%201%26x" in url, url
    assert url.count("&") == 2, f"an unescaped separator reached the query: {url}"


def test_the_three_discovery_surfaces_all_route_through_this_builder():
    """A STRUCTURAL guard, because the defect this module fixes was three
    hand-built URLs drifting apart -- and a fourth would drift the same way
    without failing any behavioural test that exists today.

    Asserts on the SOURCE because the alternative -- rendering every surface and
    comparing hrefs -- proves only that today's outputs happen to agree, which
    is exactly what was true before the panel's row started emitting a folio
    with no volume.
    """
    import ast
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[1]
    for relative in ("web/pages/findings.py",
                     "web/components/discovery_panel.py"):
        source = (root / relative).read_text(encoding="utf-8")
        tree = ast.parse(source)
        handbuilt = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
            and node.value.startswith("/browse?sys_id=")
        ]
        assert not handbuilt, (
            f"{relative} builds a /browse target by hand at line(s) "
            f"{[n.lineno for n in handbuilt]} -- use "
            "web.components.discovery_links.browse_url, which is the only place "
            "the folio/volume atomicity rule is stated")
        assert "browse_url" in source, (
            f"{relative} no longer calls the shared builder at all")
