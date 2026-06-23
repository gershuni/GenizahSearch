# -*- coding: utf-8 -*-
"""SEED-014 — Accessibility & RTL/bidi audit guards.

Source/DOM-level assertions for the fixes applied to the search toolbar/results,
the anchor viewer, and the shared filter panel. These are static (AST / source
text) checks plus one async-state behavioral test for the filter-count recompute.

Headless pytest cannot see *computed* bidi reordering or computed element height,
so a manual Hebrew-UI visual + keyboard pass is still required before merge. What
these tests DO pin:

  #14  aria-label present on icon-only buttons in the search toolbar/panel,
        AND exhaustively on EVERY icon-only ui.button(...) in search_results.py.
  #15  shelfmark/title renders carry dir="auto" + unicode-bidi:isolate (or <bdi>).
  M3/#26 the result expand/collapse toggle exposes role=button / tabindex /
        aria-expanded / aria-controls and keyboard activation; a visible Collapse
        control exists in the expanded panel.
  #25  expansion is driven via NiceGUI set_visibility(), not imperative
        display:none/block on the expansion ref.
  #22  the chip-bar updater is gated on an explicit readiness flag, not a broad
        `except NameError`.
  #11  recompute_filter_count surfaces pending / done / error via on_state.
  #41  anchor-viewer folio arrows are direction-aware (is_rtl()).
"""
from __future__ import annotations

import ast
import asyncio
import pathlib
import re
from types import SimpleNamespace

import pytest

REPO_ROOT = pathlib.Path(__file__).parent.parent
SEARCH_PY = REPO_ROOT / "web" / "pages" / "search.py"
RESULTS_PY = REPO_ROOT / "web" / "pages" / "search_results.py"
ANCHOR_PY = REPO_ROOT / "web" / "components" / "anchor_viewer.py"
FILTER_PY = REPO_ROOT / "web" / "components" / "filter_panel.py"


def _read(p: pathlib.Path) -> str:
    return p.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# #14 — aria-labels on icon-only search toolbar/panel buttons
# ---------------------------------------------------------------------------

class TestFinding14AriaLabels:
    def test_search_toolbar_icon_buttons_have_aria_labels(self):
        """Every icon-only button flagged in the audit carries aria-label.

        We assert presence of the aria-label text near each known icon so a
        regression that drops the prop is caught. The icons cover the collapsed
        panel, expand/collapse, New Search, History, bulk/filter/export row.
        """
        src = _read(SEARCH_PY)
        # Each tuple: a fragment that identifies the button + the aria-label string
        # that must appear in the same props(...) call (we check both are present
        # and that the file uses the aria-label form for that control's row).
        required = [
            "Expand search options",
            "Collapse search panel",
            "New Search",
            "Search History",
            "Add Selected to List",
            "Copy Selected Text",
            "Toggle Filters",
            "Export Word",
            "Export Excel",
            "Export JSON",
        ]
        for label in required:
            assert f'aria-label="{{tr("{label}")}}"' in src or f'aria-label="{label}"' in src, (
                f"Missing aria-label for control '{label}' in search.py (#14)"
            )

    def test_no_icon_only_button_without_aria_label_in_toolbar_region(self):
        """The icon-only ui.button(...) controls in the New-Search / History /
        bulk / export rows must each pair their icon with an aria-label in their
        props string. We match the BUTTON construction specifically (not chips /
        icons / badges that may reuse the same Material icon name elsewhere).
        """
        src = _read(SEARCH_PY)
        for icon in ("restart_alt", "history", "playlist_add", "content_copy",
                     "filter_list", "description", "table_view", "data_object"):
            needle = f"ui.button(icon='{icon}'"
            idx = src.find(needle)
            assert idx != -1, f"expected ui.button(icon='{icon}') in search.py"
            window = src[idx:idx + 400]
            assert "aria-label=" in window, (
                f"icon-only button icon={icon!r} lacks aria-label within its props (#14)"
            )

    def test_every_icon_only_button_in_results_has_accessible_name(self):
        """REGRESSION GUARD (Codex REQUEST-CHANGES #14): EVERY icon-only
        ``ui.button(...)`` in ``search_results.py`` must expose an accessible
        name via ``aria-label=`` somewhere in its ``.props(...)`` chain.

        An icon-only button is a ``ui.button(...)`` call with NO first positional
        (text) argument — i.e. the label is conveyed only by ``icon=``. Buttons
        with a positional text label (e.g. ``ui.button(tr('Collapse'), ...)``)
        already have an accessible name from their visible text, so they are
        exempt. A tooltip alone is NOT an accessible name (it is not announced as
        the button's label by screen readers), so it does not satisfy this guard.

        This is intentionally exhaustive (not a hand-picked subset) so a future
        icon-only button that ships without aria-label is caught here.
        """
        src = _read(RESULTS_PY)
        tree = ast.parse(src)

        def _is_ui_button_call(node: ast.Call) -> bool:
            f = node.func
            return (
                isinstance(f, ast.Attribute)
                and f.attr == "button"
                and isinstance(f.value, ast.Name)
                and f.value.id == "ui"
            )

        def _is_icon_only(call: ast.Call) -> bool:
            # Icon-only == no positional args (the label, if any, is positional).
            # ``ui.button(icon=..., on_click=...)`` -> icon-only.
            # ``ui.button(tr('Save'), icon='save')`` -> has a text label, exempt.
            if call.args:
                return False
            return any(kw.arg == "icon" for kw in call.keywords)

        def _props_chain_has_aria_label(button_call: ast.Call) -> bool:
            """Return True iff a ``.props(...)`` call that decorates THIS button
            instance carries ``aria-label=`` in its (string/f-string) argument.

            A ``.props(...)`` decorates ``button_call`` when the button call's
            source span is nested inside the props call's source span AND the
            button's own source text appears within the props call's source text
            (so sibling buttons on adjacent lines are not confused)."""
            found = {"aria": False}
            b_seg = ast.get_source_segment(src, button_call) or ""

            class _OwnerVisitor(ast.NodeVisitor):
                def visit_Call(self, node: ast.Call):
                    func = node.func
                    if (
                        isinstance(func, ast.Attribute)
                        and func.attr == "props"
                        and node.args
                        and button_call.lineno >= node.lineno
                        and button_call.end_lineno <= (node.end_lineno or node.lineno)
                    ):
                        seg = ast.get_source_segment(src, node) or ""
                        # Confirm this props() decorates *this* button instance
                        # (its full segment contains the button's own segment).
                        if b_seg and b_seg in seg and "aria-label=" in (
                            ast.get_source_segment(src, node.args[0]) or ""
                        ):
                            found["aria"] = True
                    self.generic_visit(node)

            _OwnerVisitor().visit(tree)
            return found["aria"]

        offenders = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and _is_ui_button_call(node) and _is_icon_only(node):
                if not _props_chain_has_aria_label(node):
                    offenders.append(node.lineno)

        assert not offenders, (
            "icon-only ui.button(...) in search_results.py missing aria-label at "
            f"line(s): {sorted(offenders)} (#14)"
        )


# ---------------------------------------------------------------------------
# #15 — bidi isolation of shelfmarks + mixed-script titles
# ---------------------------------------------------------------------------

class TestFinding15BidiIsolation:
    def test_isolated_label_helper_exists_and_uses_bdi_and_isolate(self):
        src = _read(RESULTS_PY)
        assert "def _isolated_label(" in src, "expected _isolated_label helper (#15)"
        # Helper renders a <bdi dir="auto"> and applies unicode-bidi: isolate.
        assert "<bdi dir=\"auto\">" in src or "<bdi dir='auto'>" in src
        assert "unicode-bidi: isolate" in src
        # Text must be escaped before being put into ui.html.
        assert "html.escape(" in src

    def test_shelfmark_renders_go_through_isolation(self):
        """The result-card + excluded-list + advanced-dialog shelfmark renders
        use the bidi-isolated helper rather than a bare ui.label."""
        src = _read(RESULTS_PY)
        # The main card shelfmark must be isolated.
        assert "_isolated_label(shelfmark," in src, (
            "main result-card shelfmark must be bidi-isolated (#15)"
        )
        # Excluded-list shelfmarks isolated.
        assert "_isolated_label(excl_shelfmark," in src
        # Advanced dialog display_shelfmark isolated.
        assert "_isolated_label(display_shelfmark," in src

    def test_title_labels_carry_unicode_bidi_isolate(self):
        """Mixed-script title labels (which keep a mutable ui.label for the
        original/translated toggle) must add unicode-bidi: isolate to their
        direction-styled spans."""
        src = _read(RESULTS_PY)
        # Count direction:{...} styles that are accompanied by isolate.
        dir_styles = re.findall(r"direction:\s*\{[^}]+\}[^']*", src)
        assert dir_styles, "expected direction:{..} title styles in results"
        # Every title direction style we touched should also carry isolate.
        for s in dir_styles:
            assert "unicode-bidi: isolate" in s, (
                f"title direction style lacks unicode-bidi: isolate: {s!r} (#15)"
            )


# ---------------------------------------------------------------------------
# M3 + #26 — semantic expansion toggle + visible collapse control
# ---------------------------------------------------------------------------

class TestFinding26SemanticExpansion:
    def test_toggle_exposes_button_semantics(self):
        src = _read(RESULTS_PY)
        # role=button + tabindex + aria-expanded + aria-controls on the toggle.
        assert "role=button" in src
        assert "tabindex=0" in src
        assert "aria-expanded=false" in src
        assert "aria-controls=" in src

    def test_toggle_has_keyboard_activation(self):
        src = _read(RESULTS_PY)
        assert "keydown.enter" in src, "Enter must activate the expansion toggle (#26)"
        assert "keydown.space" in src, "Space must activate the expansion toggle (#26)"

    def test_aria_expanded_kept_in_sync(self):
        src = _read(RESULTS_PY)
        assert "_set_expansion_aria(" in src
        # The sync helper flips aria-expanded between true/false based on state.
        assert 'aria-expanded={"true" if expanded else "false"}' in src
        # And the toggle element is created with aria-expanded=false.
        assert "aria-expanded=false" in src

    def test_expanded_panel_has_region_and_collapse_control(self):
        src = _read(RESULTS_PY)
        assert "role=region" in src, "expanded panel should be a labeled region (#26)"
        # Explicit Collapse control inside the expansion.
        assert 'tr(\'Collapse\')' in src or 'tr("Collapse")' in src


# ---------------------------------------------------------------------------
# #25 — NiceGUI visibility, not imperative display:none/block
# ---------------------------------------------------------------------------

class TestFinding25Visibility:
    def test_toggle_expansion_uses_set_visibility(self):
        src = _read(RESULTS_PY)
        assert "def toggle_expansion(" in src
        # Pull the toggle_expansion body and assert it uses set_visibility and
        # does NOT imperatively set display:none/block on the ref.
        start = src.index("def toggle_expansion(")
        end = src.index("\ndef ", start + 1)
        body = src[start:end]
        assert "set_visibility(False)" in body
        assert "set_visibility(True)" in body
        assert "display: none" not in body, (
            "toggle_expansion must not imperatively set display:none (#25)"
        )
        assert "display: block" not in body

    def test_expansion_container_created_hidden_via_state(self):
        src = _read(RESULTS_PY)
        # The expansion container is created and hidden via set_visibility(False),
        # not a literal display:none style.
        assert "expand_container.set_visibility(False)" in src


# ---------------------------------------------------------------------------
# #22 — explicit readiness flag, not `except NameError`
# ---------------------------------------------------------------------------

class TestFinding22ReadinessFlag:
    def test_no_except_nameerror_masking_update_chip_bar(self):
        src = _read(SEARCH_PY)
        assert "except NameError" not in src, (
            "broad `except NameError` around _update_chip_bar masks real bugs (#22)"
        )

    def test_chip_bar_ready_flag_present(self):
        src = _read(SEARCH_PY)
        assert "_chip_bar_ready" in src
        assert "_chip_bar_ready['value'] = True" in src
        assert "if _chip_bar_ready['value']:" in src


# ---------------------------------------------------------------------------
# #41 — direction-aware folio arrows in anchor viewer
# ---------------------------------------------------------------------------

class TestFinding41RtlFolioArrows:
    def test_anchor_viewer_imports_is_rtl(self):
        src = _read(ANCHOR_PY)
        assert "is_rtl" in src
        assert "from web.translations import" in src

    def test_folio_arrows_are_direction_aware(self):
        src = _read(ANCHOR_PY)
        # Prev/next icons chosen by is_rtl(), mirroring browse.py.
        assert 'icon="chevron_right" if _rtl else "chevron_left"' in src
        assert 'icon="chevron_left" if _rtl else "chevron_right"' in src
        # The hard-coded direction:ltr override on the controls bar is gone.
        assert 'anchor-controls-bar w-full justify-between").style(\n                "direction: ltr;"' not in src


# ---------------------------------------------------------------------------
# #11 — recompute_filter_count pending / done / error feedback (async behavior)
# ---------------------------------------------------------------------------

def _make_filter_state(active=True):
    """Minimal state stub for recompute_filter_count."""
    ns = SimpleNamespace(
        filter_domains=['Bible'] if active else [],
        filter_authors=[],
        filter_works=[],
        filter_date_from=None,
        filter_date_to=None,
        filter_material_exclude=None,
        filter_text_all=None,
        filter_text_any=None,
        filter_text_not=None,
        filter_include_mode=True,
        filter_manuscript_count=None,
        restrict_sys_ids=None,
    )
    return ns


class TestFinding11FilterRecomputeState:
    def test_signature_accepts_on_state(self):
        import inspect
        from web.components.filter_panel import recompute_filter_count
        params = inspect.signature(recompute_filter_count).parameters
        assert "on_state" in params, "recompute_filter_count must accept on_state (#11)"
        assert params["on_state"].default is None, "on_state must be optional (backward compat)"

    def test_pending_then_done_on_success(self, monkeypatch):
        import web.components.filter_panel as fp

        async def _fake_io_bound(fn, *a, **k):
            # Simulate a successful FJMS lookup returning a sys_id set.
            return ['s1', 's2', 's3']

        monkeypatch.setattr(fp.run, "io_bound", _fake_io_bound)

        states = []
        chip_calls = []

        async def _drive():
            st = _make_filter_state(active=True)
            await fp.recompute_filter_count(
                st, lambda: chip_calls.append(True), on_state=states.append
            )
            return st

        st = asyncio.run(_drive())
        assert states[0] == "pending"
        assert states[-1] == "done"
        assert "error" not in states
        assert st.filter_manuscript_count == 3
        assert chip_calls, "chip bar updater should run on success"

    def test_error_state_on_failure(self, monkeypatch):
        import web.components.filter_panel as fp

        async def _boom(fn, *a, **k):
            raise RuntimeError("fjms exploded")

        monkeypatch.setattr(fp.run, "io_bound", _boom)

        states = []

        async def _drive():
            st = _make_filter_state(active=True)
            await fp.recompute_filter_count(
                st, lambda: None, on_state=states.append
            )
            return st

        st = asyncio.run(_drive())
        assert "pending" in states
        assert states[-1] == "error", f"expected terminal 'error', got {states}"
        assert st.filter_manuscript_count is None
        assert st.restrict_sys_ids is None

    def test_no_active_filters_emits_done_without_io(self, monkeypatch):
        import web.components.filter_panel as fp

        async def _should_not_run(fn, *a, **k):  # pragma: no cover - must not be hit
            raise AssertionError("io_bound should not run with no active filters")

        monkeypatch.setattr(fp.run, "io_bound", _should_not_run)

        states = []

        async def _drive():
            st = _make_filter_state(active=False)
            await fp.recompute_filter_count(st, lambda: None, on_state=states.append)

        asyncio.run(_drive())
        assert states == ["done"]

    def test_backward_compatible_without_on_state(self, monkeypatch):
        """Callers that omit on_state (e.g. parallels.py) must still work."""
        import web.components.filter_panel as fp

        async def _fake_io_bound(fn, *a, **k):
            return ['s1']

        monkeypatch.setattr(fp.run, "io_bound", _fake_io_bound)

        async def _drive():
            st = _make_filter_state(active=True)
            await fp.recompute_filter_count(st, lambda: None)  # no on_state
            return st

        st = asyncio.run(_drive())
        assert st.filter_manuscript_count == 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
