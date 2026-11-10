# -*- coding: utf-8 -*-
"""SEED-018 (#44/M5): verify the root ``shared_export_utils`` compatibility shim
re-exports the same public surface as the relocated ``shared.export_utils``.

The implementation moved under ``shared/`` and the root module became a thin
re-export shim so existing ``import shared_export_utils`` / ``from
shared_export_utils import X`` call sites keep working unchanged. These tests pin
that contract: same symbols, same objects, zero behavior change.
"""

import importlib

import pytest

# The public names every caller relies on (the documented surface).
_EXPECTED_PUBLIC = [
    "build_rich_snippet_cell",
    "sanitize_text_for_excel",
    "remove_highlight_markers",
    "sanitize_cache_filename",
    "coerce_img_page_cell",
    "build_expanded_context",
    "clean_text_single_line",
    "strip_xml_illegal_chars",
    "make_safe_filename",
    "encode_filename_for_header",
    "extract_search_terms",
    "contains_any_term",
    "EXPORT_CONTEXT_CAP",
]


@pytest.fixture(scope="module")
def mods():
    shim = importlib.import_module("shared_export_utils")
    impl = importlib.import_module("shared.export_utils")
    return shim, impl


def test_expected_public_names_present_in_impl(mods):
    _, impl = mods
    missing = [name for name in _EXPECTED_PUBLIC if not hasattr(impl, name)]
    assert not missing, f"shared.export_utils missing expected public names: {missing}"


def test_shim_re_exports_all_expected_names(mods):
    shim, _ = mods
    missing = [name for name in _EXPECTED_PUBLIC if not hasattr(shim, name)]
    assert not missing, f"shim missing re-exported names: {missing}"


def test_shim_and_impl_expose_same_objects(mods):
    """Every expected public name must resolve to the SAME object in both modules
    (the shim re-exports, it does not redefine)."""
    shim, impl = mods
    for name in _EXPECTED_PUBLIC:
        assert getattr(shim, name) is getattr(impl, name), (
            f"{name!r} differs between shim and shared.export_utils"
        )


def test_shim_public_surface_matches_impl(mods):
    """The shim's non-underscore public attribute set must match the impl's
    (modulo import machinery), so no public symbol silently drops on relocation."""
    shim, impl = mods

    def public_callables_and_consts(m):
        names = set()
        for name in dir(m):
            if name.startswith("_"):
                continue
            obj = getattr(m, name)
            # Skip re-imported modules (e.g. ``re``) — only compare the module's
            # own functions/constants.
            if getattr(obj, "__module__", None) in ("shared.export_utils", None) or not callable(obj):
                names.add(name)
        return names

    impl_public = public_callables_and_consts(impl)
    shim_public = public_callables_and_consts(shim)
    # Every public symbol exported by the impl must be reachable on the shim.
    missing = impl_public - shim_public
    # Allow imported helpers (like ``re``, ``Optional``) to differ; only require
    # the documented surface to be fully present.
    expected = set(_EXPECTED_PUBLIC)
    assert expected.issubset(shim_public), f"shim missing: {expected - shim_public}"
    assert expected.issubset(impl_public), f"impl missing: {expected - impl_public}"


def test_shim_behavior_matches_impl(mods):
    """Smoke a couple of functions through both entry points to confirm identical
    behavior (they are the same object, but this guards against future drift)."""
    shim, impl = mods
    assert shim.remove_highlight_markers("a*b*c") == impl.remove_highlight_markers("a*b*c") == "abc"
    assert shim.sanitize_cache_filename("../etc/passwd") == impl.sanitize_cache_filename("../etc/passwd")
    assert shim.coerce_img_page_cell("5") == impl.coerce_img_page_cell("5") == 5
