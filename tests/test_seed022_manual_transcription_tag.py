"""SEED-022 — source-agnostic "has manual transcription" indicator.

Covers the new union primitive (PGP readable-text ∪ FGP) and proves it is DISTINCT
from the existing PGP link-presence helper that feeds the unchanged PGP badge.
DB-independent: the source helpers are monkeypatched so these run in CI without the
PGP/FGP sidecars. Two optional tests exercise the real pgp.db when present.
"""

from __future__ import annotations

import os

import pytest

import shared.document_service as ds
import shared.fgp_service as fgp
import shared.transcription_service as ts


# --- union logic (DB-independent) -----------------------------------------

def test_union_is_pgp_text_or_fgp(monkeypatch):
    monkeypatch.setattr(ds, 'get_sys_ids_with_pgp_text', lambda ids: {'a', 'b'})
    monkeypatch.setattr(fgp, 'get_sys_ids_with_fgp_sources', lambda ids: {'b', 'c'})
    assert ts.get_sys_ids_with_manual_transcriptions(['a', 'b', 'c', 'd']) == {'a', 'b', 'c'}


def test_union_degrades_to_pgp_when_fgp_absent(monkeypatch):
    """FGP returns set() when its sidecar/flag is absent -> union == PGP-text set."""
    monkeypatch.setattr(ds, 'get_sys_ids_with_pgp_text', lambda ids: {'x', 'y'})
    monkeypatch.setattr(fgp, 'get_sys_ids_with_fgp_sources', lambda ids: set())
    assert ts.get_sys_ids_with_manual_transcriptions(['x', 'y', 'z']) == {'x', 'y'}


def test_fgp_only_sys_id_surfaces(monkeypatch):
    """An FGP-only manuscript (translation-only, no PGP text) still gets the tag."""
    monkeypatch.setattr(ds, 'get_sys_ids_with_pgp_text', lambda ids: set())
    monkeypatch.setattr(fgp, 'get_sys_ids_with_fgp_sources', lambda ids: {'fgp1'})
    assert ts.get_sys_ids_with_manual_transcriptions(['fgp1', 'other']) == {'fgp1'}


def test_pure_union_helper_no_io():
    assert ts.union_manual_transcriptions({'a'}, {'b'}) == {'a', 'b'}
    assert ts.union_manual_transcriptions(set(), set()) == set()


def test_signature_accepts_list_and_is_none_empty_safe(monkeypatch):
    """Mirrors the underlying List[str] helpers; None/empty short-circuit to set()."""
    called = {'n': 0}

    def _pgp(ids):
        called['n'] += 1
        assert isinstance(ids, list)  # cast happened before dispatch
        return set()

    monkeypatch.setattr(ds, 'get_sys_ids_with_pgp_text', _pgp)
    monkeypatch.setattr(fgp, 'get_sys_ids_with_fgp_sources', lambda ids: set())
    assert ts.get_sys_ids_with_manual_transcriptions(None) == set()
    assert ts.get_sys_ids_with_manual_transcriptions([]) == set()
    assert called['n'] == 0  # empty inputs never hit the source helpers
    ts.get_sys_ids_with_manual_transcriptions(('s1', 's2'))  # tuple cast to list
    assert called['n'] == 1


def test_include_user_flag_is_accepted_noop(monkeypatch):
    monkeypatch.setattr(ds, 'get_sys_ids_with_pgp_text', lambda ids: {'a'})
    monkeypatch.setattr(fgp, 'get_sys_ids_with_fgp_sources', lambda ids: set())
    # No user store yet -> include_user must not change the result.
    assert ts.get_sys_ids_with_manual_transcriptions(['a'], include_user=True) == {'a'}
    assert ts.get_sys_ids_with_manual_transcriptions(['a'], include_user=False) == {'a'}


# --- web FGP kill-switch gating (GitHub Codex #303 P2) ---------------------

def test_web_manual_ids_respect_web_fgp_killswitch(monkeypatch):
    """On web, the manual-transcription union must drop FGP when WEB_FGP_ENABLED=0,
    even if the shared FGP flag/sidecar is present — otherwise FGP-only manuscripts
    get badged but cannot be opened in the chooser."""
    import web.feature_flags as ff
    import web.pages.search as wsearch

    monkeypatch.setattr(wsearch, 'get_sys_ids_with_pgp_text', lambda ids: {'pgp1'})
    monkeypatch.setattr(wsearch, 'get_sys_ids_with_fgp_sources', lambda ids: {'fgp1'})

    # FGP disabled on web -> FGP set dropped; union is PGP-text only.
    monkeypatch.setattr(ff, 'web_fgp_enabled', lambda: False)
    assert wsearch._web_fgp_sys_ids(['x']) == set()
    assert wsearch._web_manual_transcription_ids(['x']) == {'pgp1'}

    # FGP enabled on web -> FGP set included.
    monkeypatch.setattr(ff, 'web_fgp_enabled', lambda: True)
    assert wsearch._web_fgp_sys_ids(['x']) == {'fgp1'}
    assert wsearch._web_manual_transcription_ids(['x']) == {'pgp1', 'fgp1'}


# --- i18n ------------------------------------------------------------------

def test_tooltip_key_present_en_he():
    import genizah_translations as gt
    key = 'scholarly transcription/translation available'
    assert key in gt.TRANSLATIONS
    assert gt.TRANSLATIONS[key] == 'תעתיק/תרגום מדעי זמין'


# --- real-DB distinction (PGP text-presence != link-presence) --------------

_PGP_DB = 'pgp_data/pgp.db'


@pytest.mark.skipif(not os.path.exists(_PGP_DB), reason='pgp.db sidecar absent')
def test_pgp_text_predicate_is_stricter_than_link_presence():
    """The new text-predicate must be a STRICT subset of the link helper: there
    exists at least one sys_id that is PGP-linked but has no readable text, and it
    must be in the link set but NOT the text set."""
    import sqlite3
    c = sqlite3.connect(_PGP_DB)
    link_only = c.execute(
        "SELECT f.sys_id FROM document_fragments f JOIN documents d ON d.pgpid=f.document_id "
        "WHERE COALESCE(d.has_transcription,0)=0 AND COALESCE(d.has_translation,0)=0 LIMIT 1"
    ).fetchone()
    has_text = c.execute(
        "SELECT f.sys_id FROM document_fragments f JOIN documents d ON d.pgpid=f.document_id "
        "WHERE d.has_transcription=1 OR d.has_translation=1 LIMIT 1"
    ).fetchone()
    c.close()
    assert link_only and has_text, 'fixture corpus lacks both cases'
    link_only, has_text = link_only[0], has_text[0]

    assert link_only in ds.get_sys_ids_with_transcriptions([link_only])      # PGP badge would show
    assert link_only not in ds.get_sys_ids_with_pgp_text([link_only])        # manual tag correctly hidden
    assert has_text in ds.get_sys_ids_with_pgp_text([has_text])              # manual tag shows
    assert has_text in ts.get_sys_ids_with_manual_transcriptions([has_text])  # union includes it
