# -*- coding: utf-8 -*-
"""
Static integration guards for the FGP transcription chooser wiring (FGP-05/06/07).

The web (NiceGUI) and desktop (PyQt6) UI modules can't be imported in CI without
their heavy GUI deps, so — following the project's existing static-guard pattern
(tests/test_pgp_filter_cascade.py, tests/test_no_server_side_stop_propagation.py)
— these tests assert the FGP integration is present at every chooser surface by
inspecting source. They catch a refactor that silently drops FGP wiring. The
behavior of the shared logic those surfaces depend on is tested in
tests/test_fgp_service.py.
"""

import os

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(rel_path: str) -> str:
    with open(os.path.join(REPO, rel_path), encoding="utf-8") as fh:
        return fh.read()


# ── Web surfaces ──────────────────────────────────────────────────


class TestWebChooserWiring:
    def test_version_selector_renders_distinct_fgp_group(self):
        src = _read("web/components/version_selector.py")
        assert "group_transcription_sources" in src
        assert "get_fgp_sources" in src           # FGP getter used in the menu
        assert "FGP Transcriptions" in src         # own group label
        assert "'fgp'" in src or '"fgp"' in src    # source discriminator emitted
        # Own badge, not the green PGP badge.
        assert "create_version_badge" in src
        assert "deep-purple" in src

    def test_browse_enrichment_merges_and_filters_fgp(self):
        src = _read("web/pages/browse_enrichment.py")
        assert "get_fgp_sources_for_fragment" in src
        assert "filter_sources_for_page" in src
        assert "web_fgp_enabled" in src

    def test_search_results_merges_and_filters_fgp(self):
        src = _read("web/pages/search_results.py")
        assert "get_fgp_sources_for_fragment" in src
        assert "filter_sources_for_page" in src
        assert "web_fgp_enabled" in src

    def test_browse_reading_desk_merges_and_labels_fgp(self):
        src = _read("web/pages/browse.py")
        assert "_merge_fgp_sources" in src
        assert "get_fgp_section_for_page" in src
        assert "FGP Transcription" in src          # distinct dropdown label


# ── Desktop surfaces ──────────────────────────────────────────────


class TestDesktopChooserWiring:
    def test_worker_merges_and_splits_fgp(self):
        src = _read("gui_threads.py")
        assert "get_fgp_sources_for_fragment" in src
        assert "get_fgp_section_for_page" in src
        # Merged in BOTH the per-fragment worker and the reading-desk batch worker.
        assert src.count("get_fgp_sources_for_fragment(") >= 2

    def test_populate_combo_has_distinct_fgp_group(self):
        src = _read("genizah_app.py")
        assert "source_provider" in src
        assert '"fgp_edition"' in src
        assert '"fgp_translation"' in src
        assert "-- FGP --" in src                  # distinct combo group header

    def test_browse_version_loader_handles_fgp(self):
        src = _read("genizah_app.py")
        assert "'fgp_edition'" in src
        assert "'fgp_translation'" in src
        # Auto-select falls back to FGP when no PGP edition.
        assert "'pgp_edition', 'fgp_edition'" in src

    def test_browse_save_guard_excludes_fgp(self):
        # FGP combo items must not be saved-as-corrections (would duplicate them).
        src = _read("genizah_app.py")
        assert "'fgp_edition', 'fgp_translation', None" in src

    def test_result_dialog_handles_fgp(self):
        src = _read("desktop/result_dialog.py")
        assert "fgp_edition" in src
        assert "fgp_translation" in src


# ── i18n ──────────────────────────────────────────────────────────


class TestFgpI18n:
    def test_hebrew_strings_present(self):
        src = _read("genizah_translations.py")
        assert '"FGP Transcription"' in src
        assert '"FGP Transcriptions"' in src
