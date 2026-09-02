"""Regressions for the four Codex findings in round 2 on PR #333 (2026-09-02).

P1 `desktop/viewers.py` — `_apply_attribution_for_source()` read
`self.current_source` on the first `load_images()`, but `__init__` never defined
it and `load_images` only assigns it further down: an `AttributeError` before any
image could load.

P1 `desktop/viewers.py` / `desktop/widgets/__init__.py` — switching back from the
two-entry NLI list to Oxford's whole-codex list fell through to proportional
scaling (`1 / (2 - 1)`), landing on image 163 of 164 instead of the folio just
left. The switch is now reversible.

P2 `web/pages/browse_enrichment.py` — `nli_attribution` copied the primary
credit, so the browser's Oxford→NLI fallback still labelled the NLI image with
the Bodleian credit.

P1 `web/api.py` — the Rosetta fallback used bare `requests.get(verify=True)`;
`rosetta.nli.org.il` has a legacy certificate chain and `shared/nli_fetch.py`
owns the host-scoped policy for it.
"""
from __future__ import annotations

import ast
import inspect

from desktop.widgets import map_matching_image_index


def _read(path):
    return open(path, encoding="utf-8").read()


OX = [{"label": f"{f}{s}", "folio_num": f} for f in range(1, 83) for s in ("a", "b")]  # 164
NLI = [{"label": "FL168181477"}, {"label": "FL168181478"}]


class _Viewer:
    """The two switch behaviours, unbound from the real widget (no QApplication)."""

    def __init__(self):
        import desktop.viewers as viewers
        self.current_source = None
        self._last_idx_by_source = {}
        self._attr_ext = ""
        self._attr_nli = ""
        self._label_text = None
        self._label_visible = None
        self._index_for_source_switch = viewers.ManuscriptViewerWidget._index_for_source_switch.__get__(self)
        self._apply = viewers.ManuscriptViewerWidget._apply_attribution_for_source.__get__(self)

    # minimal QLabel stand-in
    @property
    def lbl_attribution(self):
        outer = self

        class _L:
            def setText(self, t):
                outer._label_text = t

            def setVisible(self, v):
                outer._label_visible = v
        return _L()


class TestCurrentSourceIsInitialized:
    def test_init_defines_current_source_before_any_load(self):
        src = _read("desktop/viewers.py")
        tree = ast.parse(src)
        cls = next(n for n in ast.walk(tree)
                   if isinstance(n, ast.ClassDef) and n.name == "ManuscriptViewerWidget")
        init = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "__init__")
        assigned = {
            t.attr for st in ast.walk(init) if isinstance(st, ast.Assign)
            for t in st.targets
            if isinstance(t, ast.Attribute) and isinstance(t.value, ast.Name) and t.value.id == "self"
        }
        assert "current_source" in assigned, "read by _apply_attribution_for_source on the first load"
        assert "_last_idx_by_source" in assigned

    def test_attribution_is_applied_after_the_source_is_decided(self):
        src = _read("desktop/viewers.py")
        tree = ast.parse(src)
        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == "load_images")
        body = ast.get_source_segment(src, fn)
        apply_at = body.index("self._apply_attribution_for_source()")
        combo_at = body.index("self.combo_source.blockSignals(False)")
        assert apply_at > combo_at, "the credit must be chosen after the source is"

    def test_helper_runs_with_a_freshly_constructed_state(self):
        v = _Viewer()          # current_source is None, as after __init__
        v._attr_ext = "Bodleian Libraries"
        v._apply()             # must not raise
        assert v._label_text == "Bodleian Libraries"
        assert v._label_visible is True


class TestSourceSwitchIsReversible:
    def test_returning_to_oxford_restores_the_folio_just_left(self):
        v = _Viewer()
        v.current_source = "ext"
        v._last_idx_by_source["ext"] = 53          # folio 27b of the 164-image codex
        idx = v._index_for_source_switch(NLI, 1, OX, "ext")
        assert idx == 53, f"expected the remembered Oxford index, got {idx}"
        assert idx != len(OX) - 1, "the proportional branch would land on the last image"

    def test_first_visit_to_a_source_still_maps_by_side(self):
        v = _Viewer()
        v.current_source = "ext"
        assert v._index_for_source_switch(OX, 53, NLI, "nli") == 1   # 27b -> verso
        assert v._index_for_source_switch(OX, 52, NLI, "nli") == 0   # 27a -> recto

    def test_out_of_range_memory_is_ignored(self):
        v = _Viewer()
        v._last_idx_by_source["nli"] = 99          # stale, from a longer list
        assert v._index_for_source_switch(OX, 52, NLI, "nli") == 0

    def test_pure_helper_unchanged_for_the_two_entry_case(self):
        assert map_matching_image_index(OX, 53, NLI) == 1
        assert map_matching_image_index(NLI, 1, OX) == len(OX) - 1, (
            "the pure helper is still proportional here — that is exactly why the "
            "viewer must not rely on it for a round trip"
        )

    def test_both_switch_paths_use_the_helper(self):
        src = _read("desktop/viewers.py")
        for name in ("_on_source_changed", "_on_image_load_failed"):
            fn = next(n for n in ast.walk(ast.parse(src))
                      if isinstance(n, ast.FunctionDef) and n.name == name)
            body = ast.get_source_segment(src, fn)
            assert "_index_for_source_switch(" in body, name
            assert "_last_idx_by_source[" in body, name


class TestWebFallbackCredit:
    def test_enrichment_reads_the_nli_specific_credit(self):
        src = _read("web/pages/browse_enrichment.py")
        assert "nli_attribution = cached_meta.get('attribution_nli', '')" in src
        assert "nli_attribution = attribution\n" not in src


class TestRosettaUsesNliTlsPolicy:
    def test_fallback_goes_through_the_shared_wrapper(self):
        # NB: web/api.py has TWO Rosetta thumbnail fetches. The one at the
        # `nli_image` route is pre-existing and still uses requests.get(verify=True)
        # (unchanged here, flagged as a follow-up); the one this PR added lives in
        # `_fetch_nli_image_bytes` and must use the shared wrapper.
        src = _read("web/api.py")
        assert "from shared.nli_fetch import" in src and "nli_image_get" in src
        fn = next(n for n in ast.walk(ast.parse(src))
                  if isinstance(n, ast.FunctionDef) and n.name == "_fetch_nli_image_bytes")
        body = ast.get_source_segment(src, fn)
        assert "dps_func=thumbnail" in body
        i = body.index("dps_func=thumbnail")
        window = body[i:i + 900]
        assert "nli_image_get(" in window
        assert "requests.get(" not in window.split("nli_image_get(")[0]

    def test_rosetta_is_a_known_nli_host(self):
        from shared.nli_fetch import nli_verify_for
        assert nli_verify_for(
            "https://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_func=thumbnail&dps_pid=FL1"
        ) is False, "the wrapper must disable verification for this host"

    def test_wrapper_signature_accepts_what_the_call_site_passes(self):
        from shared.nli_fetch import nli_image_get
        params = inspect.signature(nli_image_get).parameters
        for kw in ("headers", "timeout"):
            assert kw in params, kw
