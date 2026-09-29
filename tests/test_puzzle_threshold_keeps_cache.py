# -*- coding: utf-8 -*-
"""Fragment Puzzle: changing the threshold keeps other cached images.

The web threshold slider calls ``web.pages.puzzle._invalidate_and_refetch``.
Every (size, threshold) has its own cache file, so moving the slider must
only add the new threshold's image, never remove the ones already cached
(which other visitors may be using).
"""
from __future__ import annotations

import pytest

FL = '34567890'


@pytest.fixture
def service(tmp_path, monkeypatch):
    import shared.puzzle_image_service as pis
    pis.reset_puzzle_image_service()
    svc = pis.get_puzzle_image_service(cache_dir=tmp_path / 'puzzle')
    monkeypatch.setattr(svc, '_fetch_iiif_image', lambda fl_id, size: None)
    yield svc
    pis.reset_puzzle_image_service()


def test_moving_the_threshold_slider_keeps_every_cached_image(service):
    from web.pages.puzzle import _invalidate_and_refetch
    seeded = {
        service.get_cache_path(FL, 800, 30.0, True, False): b'\x89PNG-800-30',
        service.get_cache_path(FL, 1200, 30.0, True, False): b'\x89PNG-1200-30',
        service.get_cache_path(FL, 800, 30.0, False, False): b'\xff\xd8-800-original',
    }
    for path, data in seeded.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    _invalidate_and_refetch(FL, 45.0)

    for path, data in seeded.items():
        assert path.exists(), path.name
        assert path.read_bytes() == data
