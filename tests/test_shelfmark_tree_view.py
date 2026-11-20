# -*- coding: utf-8 -*-
"""The shelf views both apps draw (shared/shelfmark_tree_view.py) and the website's switch
(web/shelfmark_tree_assets.py), on the fixture tree of tests/shelfmark_tree_fixture.py."""
from __future__ import annotations

import pytest

from shared import shelfmark_tree_service as service_module
from shared.shelfmark_tree_service import ShelfmarkTreeService
from shared.shelfmark_tree_view import (
    Position,
    crumbs,
    direct_index,
    display_name,
    is_top_library,
    place_of,
    position,
    start_at,
)
from tests.shelfmark_tree_fixture import SYS, build_fixture


@pytest.fixture
def svc(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    yield s
    s.close()


def test_display_name_prefers_hebrew_where_there_is_one(svc):
    cul = next(n for n in svc.children() if n.short == "CUL")
    assert display_name(cul, "he") == "ספריית אוניברסיטת קיימברידג'"
    assert display_name(cul, "en") == "Cambridge University Library"
    no_hebrew = next(n for n in svc.children() if n.id == "code:Lutzki")
    assert no_hebrew.name_he == "" and display_name(no_hebrew, "he") == no_hebrew.name_en   # the English one
    no_code = next(n for n in svc.children() if n.id == "code:-")
    assert (display_name(no_code, "en"), display_name(no_code, "he")) == ("Location unknown", "מקום לא ידוע")
    other = svc.node("other:lib:1")
    assert display_name(other, "he") == "מספרי מדף אחרים" and display_name(other, "en") == "Other shelfmarks"


def test_start_at_a_manuscript_a_placement_a_node_and_a_prefix(svc):
    s = start_at(svc, SYS["as2"][6])
    assert s.path == ("lib:1", "col:10", "vol:100") and s.leaf.sys_id == SYS["as2"][6]
    leaf = svc.path_of(SYS["two_places"])[1].leaf
    s = start_at(svc, leaf.id)
    assert s.leaf == leaf and s.path[-1] == leaf.node_id
    assert start_at(svc, "col:12").path == ("lib:1", "col:12")
    # a library of one collection opens at that collection, as a click on it does (PR review)
    assert start_at(svc, "lib:2").path == ("lib:2", "col:20")
    assert start_at(svc, "T-S AS 2.4").path == ("lib:1", "col:10", "vol:100")
    assert start_at(svc, "").path == () and start_at(svc, "nothing here").path == ()
    assert start_at(svc, "990009999999999999").path == ()       # a manuscript with no place


def test_place_of_keeps_the_placement_the_reader_arrived_by(svc):
    first, second = svc.path_of(SYS["two_places"])
    assert place_of(svc, SYS["two_places"]).leaf.id == first.leaf.id
    assert place_of(svc, SYS["two_places"], second.leaf.id).leaf.id == second.leaf.id
    assert place_of(svc, SYS["two_places"], "inv:0:nothing").leaf.id == first.leaf.id
    assert place_of(svc, "") is None


def test_crumbs_and_position(svc):
    place = place_of(svc, SYS["as2"][6])
    assert [n.name_en for n in crumbs(place)] == ["Cambridge University Library", "T-S AS", "T-S AS 2"]
    pos = position(svc, place.leaf)
    leaves, total = svc.leaves("vol:100", 0, 100)
    assert pos.total == total and leaves[pos.index - 1].id == place.leaf.id
    assert direct_index(svc, place.leaf) == pos.index - 1


def test_top_libraries(svc, monkeypatch):
    cul = next(n for n in svc.children() if n.short == "CUL")
    assert not is_top_library(cul, svc)                          # the fixture's floor is 1,000
    monkeypatch.setattr(svc, "meta", lambda key: "7")
    assert is_top_library(cul, svc)


def test_the_website_serves_the_tree_only_when_switched_on_and_opened(svc, monkeypatch):
    from web.shelfmark_tree_assets import shelf_tree_available, shelf_tree_service
    monkeypatch.setattr(service_module, "_default_service", None)
    assert shelf_tree_service() is None                           # never opened: not served
    monkeypatch.setattr(service_module, "_default_service", svc)
    assert shelf_tree_service() is svc and shelf_tree_available()
    monkeypatch.setenv("SHELFMARK_TREE_ENABLED", "0")
    assert shelf_tree_service() is None                           # switched off
    monkeypatch.setenv("SHELFMARK_TREE_ENABLED", "1")
    svc.close()
    assert shelf_tree_service() is None                           # refused or closed


def test_a_placement_asked_for_is_spent_once_another_manuscript_is_opened():
    # PR review: a pick of A, then B opened before A's place was read, left A's request waiting;
    # a later, ordinary search for A then opened the old placement
    from shared.shelfmark_tree_view import placement_request_is_spent
    assert placement_request_is_spent(("A", "inv:1:A"), "B")
    assert not placement_request_is_spent(("A", "inv:1:A"), "A")       # its own manuscript: still wanted
    assert not placement_request_is_spent(None, "B")
    assert not placement_request_is_spent(("A", "inv:1:A"), "")        # the manuscript is not known yet


def test_a_step_from_a_manuscript_whose_place_is_not_read_yet_follows_the_shelf(svc):
    # PR review: a second Previous / Next while the first was still opening its manuscript fell
    # back to the transcription file's order
    from shared.shelfmark_tree_view import step_from
    two = svc.path_of(SYS["two_places"])                    # on T-S AS 2.3 and on T-S NS 5.3
    assert len(two) == 2
    for place in two:                                       # by the placement it was opened with
        prev, nxt = step_from(svc, SYS["two_places"], (SYS["two_places"], place.leaf.id))
        assert (prev, nxt) == svc.neighbours(place.leaf.id)
    # a request for another manuscript says nothing here: its first place
    assert step_from(svc, SYS["two_places"], ("other", "x")) == svc.neighbours(two[0].leaf.id)
    assert step_from(svc, SYS["two_places"]) == svc.neighbours(two[0].leaf.id)
    assert step_from(svc, "990009999990205171") is None     # no place on the shelf


def test_a_library_name_is_never_cut():
    # PR review: a row's name is cut after two lines, but not in the libraries' column: a long
    # institution name wraps whole (a phone has no tooltip to show the rest)
    import re

    from web.components.shelfmark_tree_picker import _CSS
    rule = re.search(r"\.stp-libs \.stp-name\s*\{([^}]*)\}", _CSS)
    assert rule and "-webkit-line-clamp: unset" in rule.group(1) and "display: block" in rule.group(1)
    assert _CSS.index(".stp-libs .stp-name") > _CSS.index(".stp-name {")     # after the clamp, so it wins


def test_the_picker_follows_the_site_theme():
    # owner, 2026-11-20: in the dark theme the lists stayed white. Every theme colour the picker uses
    # is one the site's dark theme sets (an unknown name falls back to its light default).
    import re
    from pathlib import Path

    from web.components.shelfmark_tree_picker import _CSS
    css = Path(__file__).resolve().parents[1].joinpath("web", "static", "common.css").read_text(encoding="utf-8")
    dark = "".join(re.findall(r'\[data-theme="dark"\]\s*\{([^}]*)\}', css))
    used = set(re.findall(r"var\((--[\w-]+)", _CSS)) - {"--stp-leaf"}
    assert used and all(f"{name}:" in dark for name in used), sorted(n for n in used if f"{n}:" not in dark)
    assert re.search(r"\.stp-list \{[^}]*color: var\(--text-primary", _CSS)      # text follows the background


def test_a_position_counts_the_list_it_is_shown_in(svc):
    # PR review: "T-S AS" holds its volumes and two shelfmarks of its own, shown as their own list:
    # "1 of 2", "2 of 2", not counted after the volumes' shelfmarks ("16 of 17")
    node = svc.node("col:10")
    assert node.child_nodes and node.direct_leaves == 2
    places = [place_of(svc, SYS[k]).leaf for k in ("inactive_volume", "boundary")]
    assert [lf.node_id for lf in places] == ["col:10", "col:10"]
    assert [position(svc, lf) for lf in places] == [Position(1, 2), Position(2, 2)]


def test_the_picker_shows_one_level_at_a_time_before_its_columns_overflow():
    # PR review: between 700 and 900px the columns' least widths did not fit the card and the
    # shelfmarks were cut off; the breakpoint covers the widest least layout, from the CSS itself
    import re

    from web.components.shelfmark_tree_picker import _CSS

    def least(selector):
        return int(re.search(re.escape(selector) + r" \{[^}]*min-width: (\d+)px", _CSS).group(1))

    gap = int(re.search(r"\.stp-body \{[^}]*gap: (\d+)px", _CSS).group(1))
    pad = int(re.search(r"\.stp-card \{[^}]*padding: \d+px (\d+)px", _CSS).group(1))
    card = int(re.search(r"\.stp-card \{[^}]*max-width: (\d+)vw", _CSS).group(1)) / 100
    widest = least(".stp-col.stp-libs") + 2 * least(".stp-col") + least(".stp-leaves") + 3 * gap + 2 * pad
    breakpoint = int(re.search(r"@media \(max-width: (\d+)px\)", _CSS).group(1))
    assert breakpoint * card >= widest, (breakpoint, widest)
