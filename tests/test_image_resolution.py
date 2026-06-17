# -*- coding: utf-8 -*-
"""
Phase 117 Plan 03 — Tests for web/components/image_resolution.py.

Covers:
- resolve_image_url: all 5 providers, synthetic-sys_id guard, no-direct-NLI-IIIF,
  Oxford direct-vs-proxy fork, multi-IE Manchester volume offset, active_source
  auto-default.
- resolve_external_images: cache-hit (no enrich_metadata call), empty-cache path
  (enrich_metadata called once), enrich-raises degrades to empty fields (new-HIGH).
"""
from __future__ import annotations

import pytest
from types import SimpleNamespace

from web.components.image_resolution import resolve_image_url, resolve_external_images


# ─── Helpers ───────────────────────────────────────────────────────────────────

REAL_SYS_ID = "990025143260205171"    # real Alma NLI (NOT synthetic)
SYNTH_SYS_ID = "990001234560000000"   # synthetic (is_synthetic_sys_id → True)

# Shelfmarks for Oxford tests
OXFORD_SHELFMARK_BODLEIAN = "MS Heb. e.93/58"   # derivable → direct Bodleian URL
OXFORD_SHELFMARK_NODIRECT = "T-S 12.123"         # NOT Bodleian-derivable → proxy fallback


def _nli_result(sys_id=REAL_SYS_ID, p_num=1):
    """Minimal kwargs for a plain NLI manuscript."""
    return resolve_image_url(
        sys_id=sys_id,
        p_num=p_num,
        is_oxford=False,
        shelfmark='',
        active_source='nli',
        source_user_override=False,
    )


# ─── NLI default branch ────────────────────────────────────────────────────────

def test_resolve_image_url_nli_returns_proxy_url():
    """Plain NLI sys_id → /api/nli_image_by_sysid proxy URL, has_image True."""
    result = _nli_result(p_num=3)
    assert result['has_image'] is True
    assert result['img_url'].startswith(f'/api/nli_image_by_sysid/{REAL_SYS_ID}')
    assert 'page=2' in result['img_url']   # p_num=3 → page_idx=2


def test_resolve_image_url_nli_no_direct_iiif():
    """NLI branch NEVER produces a URL containing iiif.nli.org.il (ANC-02 / HIGH-2)."""
    for p in range(1, 5):
        result = _nli_result(p_num=p)
        assert 'iiif.nli.org.il' not in result['img_url'], (
            f"ANC-02 violated at p_num={p}: img_url={result['img_url']!r}"
        )


# ─── Cambridge branch ──────────────────────────────────────────────────────────

def test_resolve_image_url_cambridge():
    """active_source='cambridge' with cambridge_images present → /api/cambridge_image."""
    cam_images = [{'url': 'https://cudl.lib.cam.ac.uk/iiif/1/canvas/1'}]
    result = resolve_image_url(
        sys_id=REAL_SYS_ID,
        p_num=2,
        is_oxford=False,
        shelfmark='T-S 12.123',
        cambridge_images=cam_images,
        external_provider='',
        active_source='cambridge',
        source_user_override=True,
    )
    assert result['has_image'] is True
    assert result['img_url'].startswith(f'/api/cambridge_image/{REAL_SYS_ID}')
    assert 'page=1' in result['img_url']
    assert 'iiif.nli.org.il' not in result['img_url']


# ─── Manchester branch + multi-IE volume offset ────────────────────────────────

def test_resolve_image_url_manchester_no_volume_offset():
    """Manchester active_source, volume_suffix=1 → page index unchanged."""
    manch_images = [{'url': 'manch-1'}]
    result = resolve_image_url(
        sys_id=REAL_SYS_ID,
        p_num=3,
        is_oxford=False,
        shelfmark='',
        cambridge_images=manch_images,
        external_provider='manchester',
        active_source='manchester',
        source_user_override=True,
        volume_suffix=1,
        volumes=[],
    )
    assert result['has_image'] is True
    # page_idx = max(0, 3-1) = 2; no vol offset
    assert 'page=2' in result['img_url']
    assert result['img_url'].startswith(f'/api/manchester_image/{REAL_SYS_ID}')
    assert 'iiif.nli.org.il' not in result['img_url']


def test_resolve_image_url_manchester_multi_ie_volume_offset():
    """Manchester multi-IE: volume_suffix=2 applies preceding volume's transcription_pages."""
    manch_images = [{'url': 'manch-1'}]
    # Volume 1 has 50 transcription pages, Volume 2 is the active one.
    volumes = [
        {'suffix': 1, 'transcription_pages': 50},
        {'suffix': 2, 'transcription_pages': 30},
    ]
    result = resolve_image_url(
        sys_id=REAL_SYS_ID,
        p_num=3,   # page 3 in volume 2 → page_idx=2, vol_offset=50, manch_page_idx=52
        is_oxford=False,
        shelfmark='',
        cambridge_images=manch_images,
        external_provider='manchester',
        active_source='manchester',
        source_user_override=True,
        volume_suffix=2,
        volumes=volumes,
    )
    assert result['has_image'] is True
    assert 'page=52' in result['img_url']   # 2 (page_idx) + 50 (vol_offset)
    assert result['img_url'].startswith(f'/api/manchester_image/{REAL_SYS_ID}')
    assert 'iiif.nli.org.il' not in result['img_url']


# ─── JTS branch ────────────────────────────────────────────────────────────────

def test_resolve_image_url_jts():
    """active_source='jts' with JTS images → /api/jts_image."""
    jts_images = [{'url': 'jts-1'}]
    result = resolve_image_url(
        sys_id=REAL_SYS_ID,
        p_num=1,
        is_oxford=False,
        shelfmark='',
        cambridge_images=jts_images,
        external_provider='jts',
        active_source='jts',
        source_user_override=True,
    )
    assert result['has_image'] is True
    assert result['img_url'].startswith(f'/api/jts_image/{REAL_SYS_ID}')
    assert 'page=0' in result['img_url']
    assert 'iiif.nli.org.il' not in result['img_url']


# ─── Synthetic sys_id guard ────────────────────────────────────────────────────

def test_resolve_image_url_synthetic_no_cambridge_images():
    """Synthetic sys_id with no cambridge_images → has_image False, img_url empty."""
    result = resolve_image_url(
        sys_id=SYNTH_SYS_ID,
        p_num=1,
        is_oxford=False,
        shelfmark='',
        cambridge_images=[],
        external_provider='',
        active_source='nli',
        source_user_override=False,
    )
    assert result['has_image'] is False
    assert result['img_url'] == ''
    assert 'iiif.nli.org.il' not in result['img_url']


def test_resolve_image_url_synthetic_with_cambridge_images():
    """Synthetic sys_id + cambridge_images present → auto-defaults to cambridge, has_image True."""
    cam_images = [{'url': 'https://cudl.lib.cam.ac.uk/iiif/MS-FOO/canvas/1'}]
    result = resolve_image_url(
        sys_id=SYNTH_SYS_ID,
        p_num=1,
        is_oxford=False,
        shelfmark='',
        cambridge_images=cam_images,
        external_provider='',
        active_source='nli',
        source_user_override=False,
    )
    # Auto-default: synthetic + cambridge_images → active_source set to 'cambridge'
    assert result['active_source'] == 'cambridge'
    assert result['has_image'] is True
    assert result['img_url'].startswith(f'/api/cambridge_image/{SYNTH_SYS_ID}')
    assert 'iiif.nli.org.il' not in result['img_url']


# ─── Oxford direct-Bodleian exception (MEDIUM-5) ──────────────────────────────

def test_resolve_image_url_oxford_bodleian_derivable():
    """Oxford with a Bodleian-derivable shelfmark → direct Bodleian URL (MEDIUM-5 exception)."""
    result = resolve_image_url(
        sys_id=REAL_SYS_ID,
        p_num=1,
        is_oxford=True,
        shelfmark=OXFORD_SHELFMARK_BODLEIAN,  # "MS Heb. e.93/58" — derivable
        active_source='oxford',
        source_user_override=True,
    )
    assert result['has_image'] is True
    # Must be a Bodleian URL, NOT a proxy URL and NOT iiif.nli.org.il
    assert 'bodleian.ox.ac.uk' in result['img_url'], (
        f"Expected direct Bodleian URL (bodleian.ox.ac.uk); got: {result['img_url']!r}"
    )
    assert '/api/oxford_image/' not in result['img_url']
    assert 'iiif.nli.org.il' not in result['img_url']


def test_resolve_image_url_oxford_no_bodleian_shelfmark():
    """Oxford with non-Bodleian-derivable shelfmark → /api/oxford_image proxy fallback."""
    result = resolve_image_url(
        sys_id=REAL_SYS_ID,
        p_num=2,
        is_oxford=True,
        shelfmark=OXFORD_SHELFMARK_NODIRECT,  # "T-S 12.123" — NOT derivable
        active_source='oxford',
        source_user_override=True,
    )
    assert result['has_image'] is True
    assert result['img_url'].startswith(f'/api/oxford_image/{REAL_SYS_ID}')
    assert 'iiif.nli.org.il' not in result['img_url']


# ─── No direct NLI IIIF across all branches ───────────────────────────────────

def test_resolve_image_url_no_direct_nli_iiif_across_providers():
    """ANC-02 / HIGH-2: no iiif.nli.org.il URL in any provider branch output."""
    cam_images = [{'url': 'cam-1'}]
    manch_images = [{'url': 'manch-1'}]
    jts_images = [{'url': 'jts-1'}]

    cases = [
        # NLI default
        dict(sys_id=REAL_SYS_ID, p_num=1, is_oxford=False, shelfmark='', active_source='nli'),
        # Cambridge
        dict(sys_id=REAL_SYS_ID, p_num=1, is_oxford=False, shelfmark='', active_source='cambridge',
             cambridge_images=cam_images, external_provider='', source_user_override=True),
        # Manchester
        dict(sys_id=REAL_SYS_ID, p_num=1, is_oxford=False, shelfmark='', active_source='manchester',
             cambridge_images=manch_images, external_provider='manchester', source_user_override=True),
        # JTS
        dict(sys_id=REAL_SYS_ID, p_num=1, is_oxford=False, shelfmark='', active_source='jts',
             cambridge_images=jts_images, external_provider='jts', source_user_override=True),
    ]
    for kwargs in cases:
        result = resolve_image_url(**kwargs)
        assert 'iiif.nli.org.il' not in result['img_url'], (
            f"ANC-02 violated for active_source={kwargs.get('active_source')!r}: "
            f"img_url={result['img_url']!r}"
        )


# ─── resolve_external_images ───────────────────────────────────────────────────

class FakeMetaMgr:
    """Minimal fake MetadataManager for testing resolve_external_images."""

    def __init__(self, cache_data=None):
        self.nli_cache = cache_data or {}
        self.enrich_calls: list[str] = []

    def enrich_metadata(self, sys_id: str) -> None:
        self.enrich_calls.append(sys_id)
        # Simulate populating the cache
        self.nli_cache[sys_id] = {
            'images_ext': [{'url': 'cam-fake'}],
            'external_provider': 'cambridge',
            'cambridge_alignment': {'verdict': 'aligned'},
        }


class FakeMetaMgrRaises:
    """MetadataManager whose enrich_metadata raises."""

    def __init__(self):
        self.nli_cache: dict = {}
        self.enrich_calls: list[str] = []

    def enrich_metadata(self, sys_id: str) -> None:
        self.enrich_calls.append(sys_id)
        raise RuntimeError("NLI unreachable")


def test_resolve_external_images_cache_hit_no_enrich():
    """Cache-hit: nli_cache already has images_ext → does NOT call enrich_metadata."""
    mgr = FakeMetaMgr(cache_data={
        REAL_SYS_ID: {
            'images_ext': [{'url': 'cached-img'}],
            'external_provider': 'jts',
            'cambridge_alignment': {'verdict': 'misaligned'},
        }
    })
    result = resolve_external_images(REAL_SYS_ID, meta_mgr=mgr)

    assert mgr.enrich_calls == [], "enrich_metadata must NOT be called on cache hit"
    assert result['cambridge_images'] == [{'url': 'cached-img'}]
    assert result['external_provider'] == 'jts'
    assert result['cambridge_alignment'] == {'verdict': 'misaligned'}


def test_resolve_external_images_empty_cache_calls_enrich():
    """Empty cache: calls enrich_metadata exactly once, then returns populated fields."""
    mgr = FakeMetaMgr(cache_data={})  # nothing in cache
    result = resolve_external_images(REAL_SYS_ID, meta_mgr=mgr)

    assert mgr.enrich_calls == [REAL_SYS_ID], (
        f"enrich_metadata must be called exactly once for empty cache; got {mgr.enrich_calls!r}"
    )
    assert result['cambridge_images'] == [{'url': 'cam-fake'}]
    assert result['external_provider'] == 'cambridge'
    assert result['cambridge_alignment'] == {'verdict': 'aligned'}


def test_resolve_external_images_enrich_raises_degrades_to_empty():
    """enrich_metadata raises → returns empty fields without re-raising (new-HIGH)."""
    mgr = FakeMetaMgrRaises()
    result = resolve_external_images(REAL_SYS_ID, meta_mgr=mgr)

    assert mgr.enrich_calls == [REAL_SYS_ID], "enrich_metadata must be attempted once"
    # Degrade to empty fields — must NOT raise
    assert result['cambridge_images'] == []
    assert result['external_provider'] == ''
    assert result['cambridge_alignment'] is None
