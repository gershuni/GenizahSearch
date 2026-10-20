# -*- coding: utf-8 -*-
"""``web/framework_patches.py::_patch_binding_remove``: element cleanup no
longer walks every property marker in the process.

NiceGUI 3.8's ``binding.remove`` (called by every ``element.clear()``) loops
over ``bindable_properties``, one marker per bindable property of every live
element on every open page, so a page's cleanup got slower with each other tab
open (tracker: "A website restart can set off reload loops from open pages").
The patch removes the same binding links and leaves the weak markers to expire
with their objects, as NiceGUI 3.15 does.
"""

from __future__ import annotations

import gc
import random
import weakref

import pytest
from nicegui import binding
from packaging.version import Version

import web.framework_patches as patches

# NiceGUI's own remove, whether or not web.main already applied the patch in this process.
_ORIGINAL_REMOVE = (patches._nicegui_binding_remove
                    or (binding.remove if binding.remove is not patches._remove_bindings_without_marker_scan
                        else None))


class _Model:
    text = binding.BindableProperty()
    visible = binding.BindableProperty()

    def __init__(self):
        self.text = ''
        self.visible = True


class _Plain:
    """Not bindable: a link from it is an active (polled) link."""

    def __init__(self):
        self.value = 0


@pytest.fixture
def clean_bindings():
    saved = (dict(binding.bindings), list(binding.active_links), dict(binding.bindable_properties))
    binding.bindings.clear()
    binding.active_links.clear()
    binding.bindable_properties.clear()
    yield
    binding.bindings.clear()
    binding.bindings.update(saved[0])
    binding.active_links[:] = saved[1]
    binding.bindable_properties.clear()
    binding.bindable_properties.update(saved[2])


def _snapshot():
    return (
        sorted((key, [(id(s), id(t), n) for s, t, n, _ in lst]) for key, lst in binding.bindings.items()),
        [(id(s), sn, id(t), tn) for s, sn, t, tn, _ in binding.active_links],
    )


def _random_graph(rng):
    models = [_Model() for _ in range(12)]
    plains = [_Plain() for _ in range(4)]
    for _ in range(25):
        a, b = rng.sample(models, 2)
        kind = rng.choice(('to', 'from', 'both'))
        if kind == 'to':
            binding.bind_to(a, 'text', b, 'text', None)
        elif kind == 'from':
            binding.bind_from(a, 'visible', b, 'visible', None)
        else:
            binding.bind(a, 'text', b, 'text')
    for plain in plains:
        binding.bind_from(rng.choice(models), 'text', plain, 'value', None)  # an active link
    return models, plains


def test_the_patch_is_applied_on_the_pinned_nicegui():
    if Version(patches._nicegui.__version__) >= patches._BINDING_REMOVE_FIXED_UPSTREAM:
        pytest.skip('NiceGUI has the upstream fix; the patch stands down')
    patches._patch_binding_remove()
    assert binding.remove is patches._remove_bindings_without_marker_scan


@pytest.mark.skipif(_ORIGINAL_REMOVE is None, reason='NiceGUI has the upstream fix; no patch to compare')
@pytest.mark.parametrize('seed', range(20))
def test_it_removes_exactly_the_links_the_original_removes(clean_bindings, seed):
    rng = random.Random(seed)
    models, plains = _random_graph(rng)
    doomed = rng.sample(models, 4) + rng.sample(plains, 1)

    baseline_bindings = {k: list(v) for k, v in binding.bindings.items()}
    baseline_links = list(binding.active_links)
    _ORIGINAL_REMOVE(doomed)
    expected = _snapshot()

    binding.bindings.clear()
    binding.bindings.update({k: list(v) for k, v in baseline_bindings.items()})
    binding.active_links[:] = baseline_links
    patches._remove_bindings_without_marker_scan(doomed)
    assert _snapshot() == expected


def test_it_never_walks_the_property_markers(clean_bindings, monkeypatch):
    models, _ = _random_graph(random.Random(7))

    class _NoWalk(weakref.WeakValueDictionary):
        def __iter__(self):
            raise AssertionError('binding.remove walked every property marker in the process')

        keys = items = values = __iter__

    markers = _NoWalk()
    markers.update(binding.bindable_properties)
    monkeypatch.setattr(binding, 'bindable_properties', markers)
    patches._remove_bindings_without_marker_scan(models[:3])


def test_markers_of_removed_objects_expire_with_them(clean_bindings):
    model = _Model()
    other = _Model()
    binding.bind(model, 'text', other, 'text')
    key = (id(model), 'text')
    assert key in binding.bindable_properties
    patches._remove_bindings_without_marker_scan([model])
    assert all(id(s) != id(model) and id(t) != id(model)
               for lst in binding.bindings.values() for s, t, _, _ in lst)
    del model
    gc.collect()
    assert key not in binding.bindable_properties


def test_removing_nothing_is_a_no_op(clean_bindings):
    _random_graph(random.Random(3))
    before = _snapshot()
    patches._remove_bindings_without_marker_scan([])
    assert _snapshot() == before
