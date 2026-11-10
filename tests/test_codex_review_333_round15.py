"""Regression for the Codex finding in round 15 on PR #333 (2026-09-03).

P2 `shared/metadata_manager.py::enrich_metadata` — `current_meta` IS the cached
dict (`self.nli_cache.get(system_id, {})`, mutated in place) and the method is
re-run for already-cached manuscripts (`web/api.py`, `image_resolution.py`, the
desktop join workbench). The unconditional
`current_meta['attribution_nli'] = nli_iiif_data.get('attribution', '') or ''`
therefore erased a good cached credit whenever the refresh's IIIF manifest timed
out or came back empty, and the Oxford->NLI fallback showed the generic NLI label.

Behavioural: the whole method runs with every network/sidecar call stubbed.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

import shared.metadata_manager as mm_mod
from shared.metadata_manager import MetadataManager


SID = "990053489970205171"
CACHED_CREDIT = "National Library of Israel, from the manifest"


def _manager(cached: dict):
    mm = MetadataManager.__new__(MetadataManager)
    mm.nli_cache = {SID: dict(cached)}
    mm.csv_bank = {}
    return mm


def _run(mm, iiif_result):
    """Run enrich_metadata with MARC empty, no sidecars, and the given manifest outcome."""
    def _iiif(self, system_id, suffix=1):
        if isinstance(iiif_result, BaseException):
            raise iiif_result
        return iiif_result

    with patch.object(MetadataManager, "fetch_marc_data", lambda self, sid: {}), \
         patch.object(MetadataManager, "fetch_iiif_manifest", _iiif), \
         patch.object(MetadataManager, "get_part_for_folio", lambda self, sid: None), \
         patch.object(mm_mod, "_get_crossref_service", lambda: None), \
         patch.object(mm_mod, "_get_fjms_service", lambda: None):
        return mm.enrich_metadata(SID)


class TestCachedNliCreditSurvivesAFailedRefresh:
    def test_empty_manifest_keeps_the_cached_credit(self):
        mm = _manager({"attribution_nli": CACHED_CREDIT, "shelfmark": "MS heb. g. 2"})
        meta = _run(mm, {})
        assert meta["attribution_nli"] == CACHED_CREDIT
        assert mm.nli_cache[SID]["attribution_nli"] == CACHED_CREDIT

    def test_timed_out_manifest_keeps_the_cached_credit(self):
        mm = _manager({"attribution_nli": CACHED_CREDIT, "shelfmark": "MS heb. g. 2"})
        meta = _run(mm, TimeoutError("manifest"))
        assert meta["attribution_nli"] == CACHED_CREDIT

    def test_a_refreshed_credit_still_replaces_the_cached_one(self):
        mm = _manager({"attribution_nli": "stale", "shelfmark": "MS heb. g. 2"})
        meta = _run(mm, {"attribution": CACHED_CREDIT})
        assert meta["attribution_nli"] == CACHED_CREDIT

    def test_first_run_initialises_the_key_even_when_the_manifest_is_empty(self):
        # Downstream readers (`attribution` fallback, the web/desktop credit
        # switch) index the key directly -- it must always exist afterwards.
        mm = _manager({"shelfmark": "MS heb. g. 2"})
        meta = _run(mm, {})
        assert meta["attribution_nli"] == ""

    def test_the_attribution_fallback_still_reads_the_kept_credit(self):
        # No MARC credit, no external provider, so `attribution` falls through
        # to `attribution_nli` -- and must see the KEPT value, not ''.
        mm = _manager({"attribution_nli": CACHED_CREDIT, "shelfmark": "MS heb. g. 2"})
        meta = _run(mm, {})
        assert meta["attribution"] == CACHED_CREDIT


@pytest.mark.parametrize("stub", [{}, {"attribution": ""}, {"attribution": None}])
def test_every_empty_shape_of_the_manifest_credit_preserves_the_cache(stub):
    mm = _manager({"attribution_nli": CACHED_CREDIT, "shelfmark": "x"})
    assert _run(mm, stub)["attribution_nli"] == CACHED_CREDIT
