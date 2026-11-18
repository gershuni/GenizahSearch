# -*- coding: utf-8 -*-
"""The free-rotation angle (2026-11-18): the 90° buttons land in the slider's -180..180."""
from __future__ import annotations

import math

import pytest

from web.pages.browse_state import ROTATION_MAX, ROTATION_MIN, clamp_rotation, normalize_rotation


@pytest.mark.parametrize('given, angle', [
    (0, 0), (90, 90), (-90, -90), (180, 180), (-180, 180), (270, -90), (-270, 90),
    (360, 0), (450, 90), (-450, -90), (7, 7), (-7, -7), (353, -7), (12.6, 13),
    ('45', 45), (None, 0), ('abc', 0), (math.nan, 0), (math.inf, 0),
])
def test_normalize_rotation(given, angle):
    assert normalize_rotation(given) == angle


def test_ninety_degree_steps_go_round_inside_the_slider():
    seen, angle = [], 0
    for _ in range(8):
        angle = normalize_rotation(angle - 90)
        seen.append(angle)
        assert ROTATION_MIN <= angle <= ROTATION_MAX
    assert seen == [-90, 180, 90, 0, -90, 180, 90, 0]


@pytest.mark.parametrize('given, angle', [
    (-180, -180), (180, 180), (-181, -180), (500, 180), (3.4, 3), ('12', 12), (None, 0), (math.nan, 0),
])
def test_clamp_rotation_keeps_the_slider_end_it_was_dragged_to(given, angle):
    assert clamp_rotation(given) == angle
