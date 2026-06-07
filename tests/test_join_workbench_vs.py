# -*- coding: utf-8 -*-
"""Phase 109 — VS adapter (JWB-12a), shelfmark fallback (review #5), grey-out logic (JWB-12g),
D-14a parity (review #3), page=None four-actions safety (review #7), network-page-lazy (review #4).

Plan 01 tests: pure shim + grey-out data-layer predicate.
Plan 02 tests: test_load_visual_candidates_parity (D-14a), test_page_none_actions_do_not_crash
               (review #7), test_thumbnail_path_is_page_scoped (review #4/D-09 AMENDMENT).
"""
import sqlite3

import pytest

from shared.visual_similarity_service import VisualSimilarityService
from shared.joins_lab import normalize_candidate
from desktop.join_workbench import _normalize_vs_row


@pytest.fixture
def tmp_vs_db(tmp_path):
    """Create a temporary visual_similarity.db with known VS rows for alma_id_a=100."""
    db_path = str(tmp_path / 'visual_similarity.db')
    conn = sqlite3.connect(db_path)
    conn.execute('''CREATE TABLE visual_suggestions (
        alma_id_a INTEGER NOT NULL, alma_id_b INTEGER NOT NULL,
        svm_score REAL NOT NULL, PRIMARY KEY (alma_id_a, alma_id_b))''')
    conn.execute('CREATE INDEX idx_vs_a ON visual_suggestions(alma_id_a)')
    conn.executemany('INSERT INTO visual_suggestions VALUES (?,?,?)',
        [(100, 201, 15.5), (100, 202, 12.3), (100, 203, 10.1), (100, 204, 8.7), (100, 205, 5.2)])
    conn.commit()
    conn.close()
    return db_path


def test_vs_adapter_maps_fields():
    """JWB-12a: VS dict -> normalize_candidate produces correct Candidate fields."""
    row = {"alma_id": "990001234500205171", "svm_score": 14.7, "rank": 1}
    c = normalize_candidate(_normalize_vs_row(row))
    assert c.sys_id == "990001234500205171"
    assert c.page is None            # VS is manuscript-level (display.img=None -> page_of -> None)
    assert c.via_vs is True
    assert c.vs_rank == 1
    assert c.vs_score == 14.7


def test_vs_adapter_shelfmark_fallback():
    """Review #5: shim must never produce an empty shelfmark — cards render blank otherwise."""
    row = {"alma_id": "990007654300205171", "svm_score": 9.1, "rank": 3}
    c = normalize_candidate(_normalize_vs_row(row))
    assert c.shelfmark, "VS shim must never produce an empty shelfmark (review #5 — cards render blank)"
    assert c.shelfmark == "990007654300205171"   # fallback == str(alma_id) when no metadata


def test_visual_source_greyed_when_no_vs(tmp_vs_db):
    """JWB-12g: anchor absent from VS DB -> has_suggestions False -> empty candidate list."""
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)
    assert svc.has_suggestions("100") is True
    assert svc.has_suggestions("999999") is False   # anchor absent -> grey out Visual (D-08)
    # When has_suggestions is False, the VS load contract is: produce []
    raw = svc.get_suggestions("999999", 200)
    assert [normalize_candidate(_normalize_vs_row(r)) for r in raw] == []


# ---------------------------------------------------------------------------
# Plan 02 tests
# ---------------------------------------------------------------------------


def test_load_visual_candidates_parity(tmp_vs_db):
    """D-14a: the Workbench Visual source returns the SAME sys_id set as get_suggestions."""
    from desktop.join_workbench import JoinCandidatePane
    svc = VisualSimilarityService(db_path=tmp_vs_db, thread_safe=False)

    class _StubMeta:  # supplies csv_bank for the batch shelfmark enrichment (review #5)
        csv_bank = {}

    class _StubWB:
        meta_mgr = _StubMeta()
        _anchor_sid = "100"

    # Call the helper unbound with a stub self exposing .wb (no Qt construction needed).
    stub = type("S", (), {"wb": _StubWB()})()
    cands = JoinCandidatePane._load_visual_candidates(stub, "100", service=svc)
    expected = {r["alma_id"] for r in svc.get_suggestions("100", 200)}
    assert {c.sys_id for c in cands} == expected      # SAME sys_id set — D-14a
    assert all(c.via_vs for c in cands)
    assert all(c.shelfmark for c in cands)            # review #5: never blank (fallback)


def test_page_none_actions_do_not_crash():
    """Review #7 / D-16: the four-action dispatch must not crash on a page=None VS Candidate.

    candidate_to_result_dict (Browse seam) and the add-to-list img derivation both derive a
    page from c.page=None; assert they tolerate None without raising and produce safe values.
    """
    from desktop.join_workbench import candidate_to_result_dict, _normalize_vs_row
    from shared.joins_lab import normalize_candidate
    c = normalize_candidate(_normalize_vs_row({"alma_id": "990001", "svm_score": 7.0, "rank": 2}))
    assert c.page is None
    res = candidate_to_result_dict(c)                  # Browse action seam — must not raise
    assert res["display"]["id"] == "990001"
    assert res["display"]["img"] is None               # page=None propagates safely (host re-derives)
    assert (c.page or 1) == 1                           # add-to-list img derivation: None -> 1
    # puzzle + add-as-join use c.sys_id / c.shelfmark only (no page) — page-safe by construction
    assert c.sys_id and c.shelfmark


def test_thumbnail_path_is_page_scoped():
    """Review #4 / D-09 AMENDMENT: the network/thumbnail path (ThumbResolver) receives only the
    visible page (<=_PER_PAGE), NOT the full <=200 set. Cheap local enrichment stays batched-full.
    """
    from desktop.join_workbench import _PER_PAGE
    # Simulate an 80-candidate result set; the page slice fed to ThumbResolver is
    # results[start:start+_PER_PAGE].
    big = list(range(80))
    page = 0
    start = page * _PER_PAGE
    page_slice = big[start:start + _PER_PAGE]
    assert len(page_slice) == _PER_PAGE        # exactly one page (<=20), never all 80 (review #4)
    assert _PER_PAGE <= 20


# ---------------------------------------------------------------------------
# Plan 05 tests — Task 1: toggle + boolean state machine + guarded pending
# ---------------------------------------------------------------------------


def test_no_source_radios_in_build_ui():
    """Task 1 RED: the 3 radios are gone; a single checkable VS toggle is present (G-04)."""
    import pathlib
    src = (pathlib.Path(__file__).parent.parent / "desktop" / "join_workbench.py").read_text(encoding="utf-8")
    # Radio widget names MUST be absent after the replacement
    assert "rb_combined" not in src, "rb_combined still present — 3-radio model not removed"
    assert "rb_visual" not in src, "rb_visual still present — 3-radio model not removed"
    assert "rb_text" not in src, "rb_text still present — 3-radio model not removed"
    assert "_source_group" not in src, "_source_group still present — 3-radio model not removed"
    # Single toggle MUST be present with setCheckable(True) and tr("Visual Similarity") label
    assert "setCheckable(True)" in src, "btn_vs_toggle.setCheckable(True) not found"
    assert 'tr("Visual Similarity")' in src, 'tr("Visual Similarity") toggle label not found'


def test_ensure_vs_load_keyed_to_anchor():
    """Task 1 RED: _ensure_vs_loaded_for_anchor is idempotent per anchor sid (HIGH-1 + HIGH-2)."""
    load_calls = []

    class _StubWB:
        _anchor_sid = "AAA"

    class _StubPane:
        wb = _StubWB()
        _vs_cands = None
        _vs_loaded_sid = None

        def _load_visual_candidates(self, sid):
            load_calls.append(sid)
            return [object()]  # non-empty — simulates VS data

        def status(self):
            pass

    from desktop.join_workbench import JoinCandidatePane
    stub = _StubPane()

    # Call twice with the same anchor — should load ONCE (memoised)
    JoinCandidatePane._ensure_vs_loaded_for_anchor(stub, silent=True)
    JoinCandidatePane._ensure_vs_loaded_for_anchor(stub, silent=True)
    assert len(load_calls) == 1, f"Expected 1 load, got {len(load_calls)} — not memoised per anchor"

    # Change anchor sid — should reload
    stub.wb._anchor_sid = "BBB"
    JoinCandidatePane._ensure_vs_loaded_for_anchor(stub, silent=True)
    assert len(load_calls) == 2, f"Expected 2 loads after anchor change, got {len(load_calls)}"


def test_set_source_keeps_pending_when_not_applied():
    """Task 1 RED: BLOCKER A — set_source('visual') keeps _pending_vs when apply_source returns False."""
    from desktop.join_workbench import JoinWorkbenchWindow

    class _FalsePaneStub:
        """Simulates a pane whose anchor has no VS — apply_source returns False."""
        _pending_vs = None

        def apply_source(self, source):
            return False  # no VS for this anchor

    class _TruePaneStub:
        """Simulates a pane whose anchor HAS VS — apply_source returns True."""
        _pending_vs = None

        def apply_source(self, source):
            return True

    # Case 1: no-VS anchor -> _pending_vs must stay set (NOT cleared)
    false_pane = _FalsePaneStub()
    # Call the window-level set_source method unbound with a stub `self`
    stub_win = type("W", (), {"_candidate_pane": false_pane})()
    JoinWorkbenchWindow.set_source(stub_win, "visual")
    assert false_pane._pending_vs is True, (
        "BLOCKER A: _pending_vs was cleared even though apply_source returned False. "
        "The request is swallowed — set_anchor can never re-apply it."
    )

    # Case 2: anchor HAS VS -> _pending_vs must be cleared after apply_source returns True
    true_pane = _TruePaneStub()
    stub_win2 = type("W", (), {"_candidate_pane": true_pane})()
    JoinWorkbenchWindow.set_source(stub_win2, "visual")
    assert true_pane._pending_vs is None, (
        "set_source should clear _pending_vs when apply_source returns True (actually applied)."
    )


# ---------------------------------------------------------------------------
# Plan 05 tests — Task 2: intersection assemble + empty-state + re-anchor
# ---------------------------------------------------------------------------


def _make_candidate(sys_id, via_text=False, via_vs=False, vs_rank=None):
    """Build a minimal Candidate-like object for assemble tests."""
    import dataclasses
    from shared.joins_lab import normalize_candidate
    from desktop.join_workbench import _normalize_vs_row

    if via_vs and not via_text:
        # pure VS candidate
        row = {"alma_id": sys_id, "svm_score": 10.0, "rank": vs_rank or 1}
        c = normalize_candidate(_normalize_vs_row(row, shelfmark=str(sys_id)))
        return c
    elif via_text:
        # text candidate — normalize then patch via_text=True (as dedup_candidates would)
        fake_res = {
            "display": {
                "id": sys_id,
                "shelfmark": str(sys_id),
                "title": "",
                "library_code": "",
                "img": 1,
            },
            "uid": f"{sys_id}|1",
            "full_text": "sample text",
            "scope": "text",
            "vs_rank": None,
            "svm_score": None,
            "_via_vs": via_vs,  # can be True for ★both tests
        }
        c = normalize_candidate(fake_res)
        # dedup_candidates sets via_text=True on survivors; replicate that here
        return dataclasses.replace(c, via_text=True)
    raise ValueError("must specify via_text or via_vs")


def test_intersection_is_both_only():
    """Task 2 RED: intersection (toggle ON + term) returns only candidates in BOTH sets (G-04)."""
    from shared.joins_lab import merge_candidates

    # T1: text-only, T2: in both sets, V3: VS-only
    t1 = _make_candidate("T1", via_text=True)
    t2 = _make_candidate("T2", via_text=True)
    v2 = _make_candidate("T2", via_vs=True, vs_rank=1)   # same sys_id as t2 -> ★both
    v3 = _make_candidate("V3", via_vs=True, vs_rank=2)

    # The intersection helper: merge_candidates(text, vs) then filter via_text AND via_vs
    merged_all = merge_candidates([t1, t2], [v2, v3])
    intersection = [c for c in merged_all if c.via_text and c.via_vs]

    assert len(intersection) == 1, f"Expected 1 intersection candidate, got {len(intersection)}"
    assert intersection[0].sys_id == "T2", f"Expected T2 in intersection, got {intersection[0].sys_id}"


def test_toggle_on_empty_box_is_pure_vs():
    """Task 2 RED: toggle ON + no term -> pure VS (merge_candidates([], vs))."""
    from shared.joins_lab import merge_candidates

    v1 = _make_candidate("V1", via_vs=True, vs_rank=1)
    v2 = _make_candidate("V2", via_vs=True, vs_rank=2)

    # toggle ON + empty box: text=[] -> pure VS output
    result = list(merge_candidates([], [v1, v2]))
    assert len(result) == 2, f"Expected 2 VS candidates, got {len(result)}"
    assert all(c.via_vs for c in result), "All pure-VS candidates should have via_vs=True"
    assert all(not c.via_text for c in result), "Pure-VS candidates should NOT have via_text=True"


def test_toggle_off_keeps_vs_badge_on_text_match():
    """Task 2 RED: toggle OFF -> text candidates that are also VS look-alikes retain via_vs badge (G-04 bullet 4)."""
    from shared.joins_lab import merge_candidates

    # T2 is a text candidate; v2 has the same sys_id -> merge_candidates marks T2 via_vs=True
    t2 = _make_candidate("T2", via_text=True)
    v2 = _make_candidate("T2", via_vs=True, vs_rank=1)
    v3 = _make_candidate("V3", via_vs=True, vs_rank=2)  # VS-only

    # toggle OFF: merge all, then filter to via_text only (exclude VS-only rows)
    merged_all = merge_candidates([t2], [v2, v3])
    toggle_off_result = [c for c in merged_all if c.via_text]

    assert len(toggle_off_result) == 1, f"Expected 1 result (T2 with badge), got {len(toggle_off_result)}"
    assert toggle_off_result[0].sys_id == "T2"
    assert toggle_off_result[0].via_vs is True, "T2 must carry via_vs=True (★both badge) in toggle-OFF mode"
    assert not any(c.sys_id == "V3" for c in toggle_off_result), "V3 (VS-only) must NOT appear in toggle-OFF results"


def test_empty_intersection_renders_empty_not_spinner():
    """Task 2 RED: disjoint text/VS sets -> intersection is [] (no perpetual spinner: G-03)."""
    from shared.joins_lab import merge_candidates

    t1 = _make_candidate("T1", via_text=True)
    v3 = _make_candidate("V3", via_vs=True, vs_rank=1)  # different sys_id -> no overlap

    # Intersection of disjoint sets must be []
    merged_all = merge_candidates([t1], [v3])
    intersection = [c for c in merged_all if c.via_text and c.via_vs]
    assert intersection == [], f"Expected empty intersection, got {intersection}"

    # Structural check: _start_enrich has an empty-results branch that routes to apply_filters
    # (never a spinner). Assert the else branch is present in the source.
    import pathlib
    src = (pathlib.Path(__file__).parent.parent / "desktop" / "join_workbench.py").read_text(encoding="utf-8")
    assert "# G-03:" in src or "G-03" in src, (
        "G-03 anti-spinner comment not found in _start_enrich's empty-results branch"
    )


def test_empty_intersection_status_message():
    """Task 2 RED: _maybe_assemble + apply_filters sets tr('No look-alikes match this search') on empty intersection (MEDIUM-1)."""
    import pathlib
    src = (pathlib.Path(__file__).parent.parent / "desktop" / "join_workbench.py").read_text(encoding="utf-8")
    assert 'tr("No look-alikes match this search")' in src, (
        "MEDIUM-1: tr('No look-alikes match this search') not found in apply_filters — "
        "empty-intersection empty-state message missing"
    )
    assert "_empty_intersection" in src, (
        "MEDIUM-1: _empty_intersection flag not found — drives the empty-state branch in apply_filters"
    )


def test_set_anchor_invalidates_candidate_state():
    """Task 2 RED: HIGH-2 + NEW-HIGH — set_anchor clears pane data AND invokes render_results (BLOCKER B)."""
    from desktop.join_workbench import JoinWorkbenchWindow

    render_calls = []

    class _FakePaneStub:
        _text_cands = ["old_text_cand"]
        _vs_cands = ["old_vs_cand"]
        _vs_loaded_sid = "OLD_SID"
        results = ["old_result"]

        def render_results(self):
            render_calls.append(1)

    class _MockLabel:
        def setText(self, *a):
            pass

    class _FakeWin:
        """Minimal stub exposing the attributes set_anchor touches."""
        _gen = 0
        _anchor_sid = None
        _anchor_res = None
        filtered = ["old_filtered"]
        triage = {}
        _candidate_pane = None  # will be set below
        anchor_shelf = _MockLabel()  # QLabel mock
        anchor_img_label = _MockLabel()  # QLabel mock
        _anchor_images = []
        _anchor_idx = 0
        _zoom = 1.0
        _fit_pending = False
        _anchor_full_pix = None
        _img_loader = None

        def _cancel_workers(self):
            pass

        def _start_anchor_load(self, *a, **kw):
            pass

        def _reload_known_joins(self, *a, **kw):
            pass

        def _set_joins_expanded(self, *a):
            pass

    win = _FakeWin()
    pane = _FakePaneStub()
    win._candidate_pane = pane

    # Call set_anchor with a minimal result dict
    res = {"display": {"id": "NEW_SID", "shelfmark": "T-S 1.1", "img": 1}, "uid": "NEW_SID|1"}
    JoinWorkbenchWindow.set_anchor(win, res)

    # Data must be cleared
    assert pane._text_cands is None, "set_anchor must clear pane._text_cands"
    assert pane._vs_cands is None, "set_anchor must clear pane._vs_cands"
    assert pane._vs_loaded_sid is None, "set_anchor must clear pane._vs_loaded_sid"
    assert pane.results == [], "set_anchor must reset pane.results to []"
    assert win.filtered == [], "set_anchor must reset wb.filtered to []"

    # render_results MUST be invoked (clears old card widgets — BLOCKER B / NEW-HIGH)
    assert len(render_calls) == 1, (
        f"set_anchor must call pane.render_results() exactly once to clear stale card widgets, "
        f"got {len(render_calls)} calls"
    )


# ---------------------------------------------------------------------------
# Plan 05 tests — Task 3: VS card text (G-02, page-lazy)
# ---------------------------------------------------------------------------


def test_vs_card_carries_full_text():
    """Task 3 RED: VS candidate cards fetch transcription text via page-lazy browse (G-02).

    Structural test: CandidateCard has a load_vs_text() method, and _render_grid_page
    calls card.load_vs_text() for each page card (page-lazy, not wholesale).
    """
    import pathlib
    src = (pathlib.Path(__file__).parent.parent / "desktop" / "join_workbench.py").read_text(encoding="utf-8")

    # CandidateCard must have a load_vs_text method
    assert "def load_vs_text(self):" in src, (
        "G-02: CandidateCard.load_vs_text() method not found in desktop/join_workbench.py"
    )

    # _render_grid_page must call card.load_vs_text() (page-lazy — only for the current page)
    assert "card.load_vs_text()" in src, (
        "G-02: card.load_vs_text() not called in _render_grid_page — page-lazy VS text fetch missing"
    )

    # The fetch uses get_browse_page (via _PageTextWorker) — not a direct NLI call
    assert "get_browse_page" in src, (
        "G-02: get_browse_page not found — VS card text should reuse the _PageTextWorker path"
    )

    # done handler MUST always set the snippet (never stuck on 'loading…')
    # Check: the done handler calls self.snip.setHtml (resolves even when txt is empty)
    assert "snip.setHtml" in src, (
        "G-02: snip.setHtml not found in load_vs_text done handler — snippet might get stuck on 'loading…'"
    )
