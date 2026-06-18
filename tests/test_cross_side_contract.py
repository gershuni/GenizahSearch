"""CR HIGH-3 / HIGH-4: cross-side ('other side of the leaf') contract.

HIGH-4: total_pages<=0 must be treated as UNKNOWN (None), not a hard cap of zero
        (which dropped EVERY OR neighbor); volume_ie must be forwarded to
        get_browse_page so multi-IE manuscripts resolve the right volume.
HIGH-3: a progress_callback must reach the query-B execute_search so a superseded
        cross-side scan is cooperatively cancellable.

Candidate.key stays a 2-tuple (sys_id, page) — volume_ie is additive, NOT in the
key (documented contract relied on by the desktop merge + existing tests).
"""

from __future__ import annotations

from shared.joins_lab import apply_cross_side, normalize_candidate


def _res(sid, page, **extra):
    d = {
        "display": {"id": sid, "shelfmark": "T-S 1", "title": "", "library_code": "CUL", "img": page},
        "uid": f"{sid}_P{page:04d}",
        "full_text": "",
    }
    d.update(extra)
    return d


class _FakeExec:
    def __init__(self, results, browse_pages=None):
        self._results = results
        self._browse = browse_pages or {}
        self.exec_kwargs = None
        self.browse_calls = []  # (sid, p_num, volume_ie)

    def execute_search(self, query_str, mode, gap, **kwargs):
        self.exec_kwargs = kwargs
        return self._results

    def get_browse_page(self, sys_id, p_num=None, **kwargs):
        self.browse_calls.append((sys_id, p_num, kwargs.get("volume_ie")))
        return self._browse.get((sys_id, p_num))

    def get_meta_for_id(self, sys_id):
        return ("T-S 1", "t")

    def get_library_for_id(self, sys_id):
        return "CUL"


# ---------------------------------------------------------------------------
# Candidate.volume_ie (additive; key unchanged)
# ---------------------------------------------------------------------------

def test_normalize_reads_volume_ie():
    c = normalize_candidate(_res("A", 3, volume_ie="v1"))
    assert c.volume_ie == "v1"


def test_key_stays_two_tuple_without_volume_ie():
    c = normalize_candidate(_res("A", 3, volume_ie="v1"))
    assert c.key == ("A", 3)  # volume_ie deliberately NOT in the dedup key


# ---------------------------------------------------------------------------
# HIGH-4: total_pages<=0 + volume_ie forwarding
# ---------------------------------------------------------------------------

def test_total_pages_zero_does_not_clamp_or_neighbors():
    ex = _FakeExec(
        [_res("B", 3)],
        browse_pages={
            ("B", 1): {"total_pages": 0},
            ("B", 2): {"text": "x", "total_pages": 0},
            ("B", 4): {"text": "y", "total_pages": 0},
        },
    )
    mr = apply_cross_side(ex, [], "q", {}, "OR")
    pages = {c.page for c in mr.candidates if c.sys_id == "B"}
    # total_pages=0 means "unknown" → both neighbors synthesized (was: all dropped)
    assert 2 in pages and 4 in pages


def test_volume_ie_forwarded_to_get_browse_page():
    ex = _FakeExec(
        [_res("B", 3, volume_ie="vol-9")],
        browse_pages={
            ("B", 1): {"total_pages": 5},
            ("B", 2): {"text": "x", "total_pages": 5},
            ("B", 4): {"text": "y", "total_pages": 5},
        },
    )
    apply_cross_side(ex, [], "q", {}, "OR")
    assert ex.browse_calls, "expected get_browse_page calls"
    assert all(v == "vol-9" for (_s, _p, v) in ex.browse_calls)


# ---------------------------------------------------------------------------
# HIGH-3: progress_callback forwarded to the query-B scan
# ---------------------------------------------------------------------------

def test_progress_callback_forwarded_to_execute_search():
    def sentinel(*_a, **_k):
        return None

    ex = _FakeExec([])
    apply_cross_side(ex, [], "q", {}, "AND", progress_callback=sentinel)
    assert ex.exec_kwargs is not None
    assert ex.exec_kwargs.get("progress_callback") is sentinel


def test_other_side_text_position_forwarded():
    # The other side now has its OWN Text Position; apply_cross_side forwards it.
    ex = _FakeExec([])
    apply_cross_side(ex, [], "q", {}, "AND", text_position="line_start")
    assert ex.exec_kwargs.get("text_position") == "line_start"
