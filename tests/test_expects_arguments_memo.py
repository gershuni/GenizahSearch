# -*- coding: utf-8 -*-
"""``web/framework_patches.py::_patch_expects_arguments_memo``: NiceGUI's per-element
``inspect.signature`` question is answered once per function.

NiceGUI 3.18 calls ``helpers.expects_arguments`` for the change handler of the
Classes, Style and Props of every element it builds (observables.py) and for
every event handler it registers; each call runs ``inspect.signature``, and the
calls were 31% of a 1,204-element build on the dev box. Those three handlers
are bound methods of three functions, so the patch remembers the answer for
bound methods by ``__func__`` and passes everything else through, so no lambda
closure (and no element one captures) is kept alive by the cache.
"""

from __future__ import annotations

import functools
import gc
import itertools
import types
import weakref

import pytest
from nicegui import helpers, ui
from nicegui.client import Client
from nicegui.testing.general import prepare_simulation
from packaging.version import Version

import web.framework_patches as patches

# NiceGUI's own, whether or not web.main already applied the patch in this process.
_ORIGINAL = patches._nicegui_expects_arguments or helpers.expects_arguments
_paths = itertools.count()


@pytest.fixture
def patched(monkeypatch):
    """The patch applied on NiceGUI's own binding, with an empty memo; undone afterwards."""
    monkeypatch.setattr(helpers, 'expects_arguments', _ORIGINAL)
    monkeypatch.setattr(patches, '_nicegui_expects_arguments', None)
    monkeypatch.setattr(patches, '_expects_arguments_by_function', {})
    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', 'x' * 40)
    prepare_simulation()
    patches._patch_expects_arguments_memo()
    yield


def _plain(a, b=1):
    return a, b


class _Thing:
    def no_args(self):
        pass

    def one_arg(self, e):
        pass

    def optional(self, e=None):
        pass

    def star(self, *args, **kwargs):
        pass


def _build(n):
    with Client(ui.page(f'/_memo_probe_{next(_paths)}')) as client:
        for i in range(n):
            with ui.row().classes('items-center gap-2'):
                ui.label(str(i)).classes('text-sm').style('color: red;')
                ui.button('go', on_click=lambda: None).props('flat dense')
    return client


def test_the_patch_is_applied_on_the_pinned_nicegui(patched):
    assert Version(patches._nicegui.__version__) <= patches._PATCH_AUDIT_THRESHOLD
    assert helpers.expects_arguments is patches._expects_arguments_memo
    assert patches._nicegui_expects_arguments is _ORIGINAL


def test_memoized_answers_equal_the_original_for_bound_methods_and_everything_else(patched):
    client = _build(1)
    label = next(e for e in client.elements.values() if isinstance(e, ui.label))
    thing = _Thing()
    cases = [
        label._classes._update, label._style._update, label._props._update,  # NiceGUI's three
        thing.no_args, thing.one_arg, thing.optional, thing.star, _Thing.no_args,
        lambda: None, lambda e: None, lambda e=None: None, _plain, functools.partial(_plain, 1),
    ]
    for func in cases:
        expected = _ORIGINAL(func)
        assert helpers.expects_arguments(func) is expected, func
        assert helpers.expects_arguments(func) is expected, func  # the remembered answer too
    assert [_ORIGINAL(f) for f in (thing.no_args, thing.one_arg, thing.optional)] == [False, True, False]
    client.delete()


def test_the_memo_holds_functions_only_and_stays_small_after_a_hundred_elements(patched):
    client = _build(100)
    memo = patches._expects_arguments_by_function
    assert memo, 'nothing was remembered while building 100 elements'
    assert all(isinstance(key, types.FunctionType) for key in memo), [type(k) for k in memo]
    assert all(isinstance(value, bool) for value in memo.values())
    assert len(memo) <= 8, len(memo)
    names = {key.__qualname__ for key in memo}
    assert {'Classes._update', 'Style._update', 'Props._update'} <= names, names
    # ...and nothing in it keeps an element alive once its page is gone.
    button = next(e for e in client.elements.values() if isinstance(e, ui.button))
    gone = weakref.ref(button)
    del button
    client.delete()
    del client
    gc.collect()
    assert gone() is None, 'an element outlived its page'


def test_the_patch_is_idempotent(patched):
    patches._patch_expects_arguments_memo()
    patches._patch_expects_arguments_memo()
    assert helpers.expects_arguments is patches._expects_arguments_memo
    assert patches._nicegui_expects_arguments is _ORIGINAL, 'the patch wrapped itself'


def test_it_stands_down_on_an_unaudited_nicegui(monkeypatch):
    monkeypatch.setattr(helpers, 'expects_arguments', _ORIGINAL)
    monkeypatch.setattr(patches, '_nicegui_expects_arguments', None)
    monkeypatch.setattr(patches, '_NV', Version('99.0.0'))
    patches._patch_expects_arguments_memo()
    assert helpers.expects_arguments is _ORIGINAL
    assert patches._nicegui_expects_arguments is None


def test_apply_all_patches_carries_it():
    import inspect

    assert '_patch_expects_arguments_memo' in inspect.getsource(patches.apply_all_patches)
