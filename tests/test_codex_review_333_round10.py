"""Regressions for the two Codex findings in round 10 on PR #333 (2026-09-02).

Both refine fixes made earlier in this PR.

P2 `desktop/viewers.py` — the remembered per-source index was returned
unconditionally, so after navigating within the destination source, switching back
restored a stale index instead of mapping from the image on screen.

P2 `web/pages/browse.py` — the search-scope key `(sys_id, p_num)` omits the
volume. Multi-volume manuscripts restart `p_num` per volume and the volume
selector loads page 1, so another volume's page 1 matched the matched folio's key.
"""
from __future__ import annotations


def _read(path):
    return open(path, encoding="utf-8").read()


OX = [{"label": f"{f}{s}", "folio_num": f} for f in range(1, 83) for s in ("a", "b")]  # 164
NLI = [{"label": "FL168181477"}, {"label": "FL168181478"}]


class _Viewer:
    def __init__(self):
        import desktop.viewers as viewers
        self.current_source = None
        self._last_idx_by_source = {}
        self._last_switch_landed_at = None
        self._index_for_source_switch = (
            viewers.ManuscriptViewerWidget._index_for_source_switch.__get__(self))


class TestRememberedIndexIsInvalidatedByNavigation:
    def test_immediate_switch_back_is_still_reversible(self):
        v = _Viewer()
        v.current_source = "ext"
        v._last_idx_by_source["ext"] = 53           # folio 27b
        # the previous switch landed on NLI index 1, and the reader has not moved
        v.current_source = "nli"
        v._last_switch_landed_at = ("nli", 1)
        assert v._index_for_source_switch(NLI, 1, OX, "ext") == 53

    def test_navigating_first_re_maps_from_the_current_image(self):
        v = _Viewer()
        v._last_idx_by_source["ext"] = 53           # stale: folio 27b
        v.current_source = "nli"
        v._last_switch_landed_at = ("nli", 1)       # switch landed on verso...
        # ...but the reader paged to NLI index 0 (recto) before switching back
        idx = v._index_for_source_switch(NLI, 0, OX, "ext")
        assert idx != 53, "a stale remembered index must not survive navigation"

    def test_first_visit_to_a_source_maps_by_side(self):
        v = _Viewer()
        v.current_source = "ext"
        v._last_switch_landed_at = None
        assert v._index_for_source_switch(OX, 53, NLI, "nli") == 1   # 27b -> verso
        assert v._index_for_source_switch(OX, 52, NLI, "nli") == 0   # 27a -> recto

    def test_out_of_range_memory_is_ignored(self):
        v = _Viewer()
        v.current_source = "ext"
        v._last_idx_by_source["nli"] = 99
        v._last_switch_landed_at = ("ext", 52)
        assert v._index_for_source_switch(OX, 52, NLI, "nli") == 0

    def test_both_switch_paths_record_where_they_land(self):
        src = _read("desktop/viewers.py")
        assert src.count("self._last_switch_landed_at = ") >= 4   # init + reset + 2 paths
        assert '_last_switch_landed_at = ("nli", new_idx)' in src

    def test_a_new_manuscript_resets_the_marker(self):
        import ast
        src = _read("desktop/viewers.py")
        fn = next(n for n in ast.walk(ast.parse(src))
                  if isinstance(n, ast.FunctionDef) and n.name == "load_images")
        body = ast.get_source_segment(src, fn)
        assert "_last_switch_landed_at = None" in body


class TestScopeKeyIncludesTheVolume:
    def test_browse_key_has_three_parts(self):
        src = _read("web/pages/browse.py")
        i = src.index("current = (getattr(page, 'sys_id'")
        window = src[i:i + 400]
        assert "volume_ie" in window, "another volume's page 1 would match otherwise"
        assert "p_num" in window

    def test_enrichment_key_matches(self):
        src = _read("web/pages/browse_enrichment.py")
        i = src.index("state.highlight_scope in (None,")
        window = src[i:i + 500]
        assert "volume_ie" in window
        assert "p_num" in window

    def test_scope_helper_distinguishes_volumes(self):
        # Behavioural: same sys_id and p_num, different volume -> not the same folio.
        src = _read("web/pages/browse.py")
        i = src.index("    def _search_scope_phrase(page):")
        j = src.index("    def _clear_search_scope_for_new_manuscript(", i)
        body = "\n".join(line[4:] if line.startswith("    ") else line
                         for line in src[i:j].split("\n"))

        class _State:
            highlight_terms = "phrase"
            highlight_scope = None
            volume_ie = None

        ns = {"state": _State}
        exec(body, ns)
        f = ns["_search_scope_phrase"]

        class _Page:
            def __init__(self, sys_id, volume_ie, p_num):
                self.sys_id = sys_id
                self.volume_ie = volume_ie
                self.p_num = p_num

        assert f(_Page("A", "IE1", 1)) == "phrase"          # the matched folio
        assert f(_Page("A", "IE2", 1)) is None, (
            "volume 2 page 1 is a different folio from volume 1 page 1"
        )
        assert f(_Page("A", "IE1", 1)) == "phrase"          # back to the original
