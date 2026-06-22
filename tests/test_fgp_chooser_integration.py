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
import re

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(rel_path: str) -> str:
    with open(os.path.join(REPO, rel_path), encoding="utf-8") as fh:
        return fh.read()


def _method_body(src: str, name: str) -> str:
    """Source of a top-level (4-space-indented) method ``name`` up to the next def."""
    m = re.search(rf"\n    def {re.escape(name)}\(.*?(?=\n    def )", src, re.S)
    assert m, f"method {name} not found"
    return m.group(0)


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
        # An FGP translation must be labeled a Translation, never "Transcription"
        # (the chooser used to call every FGP row a transcription).
        assert "source_relation_kind" in src
        assert "is_translation" in src

    def test_browse_enrichment_merges_and_filters_fgp(self):
        src = _read("web/pages/browse_enrichment.py")
        assert "get_fgp_sources_for_fragment" in src
        assert "filter_sources_for_page" in src
        assert "web_fgp_enabled" in src
        # FGP aligned to the displayed image by folio: the folio label resolved
        # from folio_images is passed into the filter.
        assert "folio_label" in src
        # Exact per-image key (c_number ↔ fgp_image_number_id) is resolved and
        # passed into the filter — preferred over the coincidental folio label.
        assert "fgp_image_number_for_displayed_page" in src
        assert "fgp_image_number" in src
        # V0.8 page text passed so FGP editions align to it by similarity.
        assert "page_text" in src

    def test_search_results_merges_and_filters_fgp(self):
        src = _read("web/pages/search_results.py")
        assert "get_fgp_sources_for_fragment" in src
        assert "filter_sources_for_page" in src
        assert "web_fgp_enabled" in src
        # Advanced view resolves the displayed folio for FGP alignment.
        assert "displayed_folio_label" in src
        # ...and the exact per-image FGP number, passed into the filter.
        assert "displayed_fgp_image_number" in src
        # ...and the V0.8 page text for similarity alignment.
        assert "page_text" in src

    def test_browse_reading_desk_merges_and_labels_fgp(self):
        src = _read("web/pages/browse.py")
        assert "_merge_fgp_sources" in src
        assert "FGP Transcription" in src          # distinct dropdown label
        assert "folio_label" in src                # per-image rows distinguished (1r, 1v…)
        # Round-2 contract: FGP is a full-content navigable list, never side-split
        # onto the fragment's recto/verso (which hid rows on the non-matching page).
        assert "get_fgp_section_for_page" not in src


# ── Desktop surfaces ──────────────────────────────────────────────


class TestDesktopChooserWiring:
    def test_worker_merges_and_aligns_fgp_by_folio(self):
        src = _read("gui_threads.py")
        assert "get_fgp_sources_for_fragment" in src
        # Merged in BOTH the per-fragment worker and the reading-desk batch worker.
        assert src.count("get_fgp_sources_for_fragment(") >= 2
        # FGP chosen via the shared selection (folio match + V0.8 similarity).
        assert "_select_fgp_sources_for_page" in src
        # The folio label, the exact FGP image number, AND the V0.8 page text are
        # resolved on the MAIN thread and passed in — the worker must NOT query
        # the (non-thread-safe) crossref service itself.
        assert "self.folio_label" in src
        assert "self.image_number" in src
        assert "self.page_text" in src
        assert "displayed_folio_label" not in src
        # Never the old recto/verso side-splitter (it hid rows on later pages).
        assert "get_fgp_section_for_page" not in src

    def test_desktop_passes_fgp_image_number_to_worker(self):
        # The exact per-image FGP key is resolved on the main thread and passed
        # into PGPSourceWorker alongside the folio label (Geneva / Manchester /
        # NLI-Heb alignment fix).
        src = _read("genizah_app.py")
        assert "_displayed_fgp_image_number_for_pgp" in src
        assert "image_number=self._displayed_fgp_image_number_for_pgp()" in src
        assert "fgp_image_number_for_displayed_page" in src

    def test_populate_combo_has_distinct_fgp_group(self):
        src = _read("genizah_app.py")
        assert "source_provider" in src
        assert '"fgp_edition"' in src
        assert '"fgp_translation"' in src
        assert "-- FGP --" in src                  # distinct combo group header
        # Caller resolves the displayed folio on the main thread and passes it to
        # the PGP worker so FGP rows are aligned to the displayed image.
        assert "_displayed_folio_label_for_pgp" in src
        assert "folio_label=self._displayed_folio_label_for_pgp()" in src

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

    def test_initial_worker_starts_after_page_combo_repopulated(self):
        # Codex HIGH: the initial PGP/FGP worker reads the displayed folio from the
        # page combo, so browse_load_page() (which repopulates that combo for the
        # new manuscript) MUST run before the worker is constructed — otherwise the
        # worker captures the previous/empty combo and resolves the wrong folio.
        body = _method_body(_read("genizah_app.py"), "on_browse_enriched_loaded")
        i_load = body.find("self.browse_load_page()")
        i_worker = body.find("PGPSourceWorker(")
        assert i_load != -1 and i_worker != -1
        assert i_load < i_worker, "browse_load_page() must precede PGPSourceWorker(...)"

    def test_fgp_only_manuscripts_refresh_on_page_change(self):
        # Codex HIGH: page-change refresh must NOT bail when there is no PGP
        # document — FGP-only manuscripts still need FGP re-aligned to the new
        # folio. The freezing early-return must be gone and a worker still started.
        body = _method_body(_read("genizah_app.py"), "_browse_refresh_pgp_for_page")
        assert "if not self._browse_pgp_doc:" not in body
        assert "PGPSourceWorker(" in body

    def test_result_dialog_handles_fgp(self):
        src = _read("desktop/result_dialog.py")
        assert "fgp_edition" in src
        assert "fgp_translation" in src

    def test_result_dialog_aligns_fgp_to_displayed_image(self):
        # The ResultDialog must align FGP to the displayed image too (not show
        # every transcription). It resolves the keys on the MAIN thread from the
        # enriched folio_images in nli_cache and passes them into the worker.
        src = _read("desktop/result_dialog.py")
        assert "_rd_displayed_image_keys" in src
        assert "fgp_image_number_for_displayed_page" in src
        assert "image_number=_rd_imgnum" in src
        # Must read already-loaded data, NOT query the (bg-thread-bound) service.
        assert "nli_cache" in src

    def test_v08_original_bypasses_document_cache(self):
        # Regression: switching FGP/PGP -> V0.8 on a later page jumped back to
        # page 1. "original" (V0.8) is per-PAGE and read from the live
        # browse_original_page_text (refreshed by browse_render_page), so it must
        # NOT be served from the per-DOCUMENT _browse_versions_cache (seeded once
        # with page 1). The cache-hit shortcut must exclude source == "original".
        body = _method_body(_read("genizah_app.py"), "_browse_load_version")
        m = re.search(r"\n( +)if .*cache_key in self\._browse_versions_cache:", body)
        assert m, "cache-hit shortcut not found in _browse_load_version"
        assert 'source != "original"' in m.group(0), (
            "cache-hit shortcut must exclude source == 'original' so V0.8 "
            "renders the current page, not the cached page-1 text"
        )


# ── i18n ──────────────────────────────────────────────────────────


class TestFgpI18n:
    def test_hebrew_strings_present(self):
        src = _read("genizah_translations.py")
        assert '"FGP Transcription"' in src
        assert '"FGP Transcriptions"' in src
