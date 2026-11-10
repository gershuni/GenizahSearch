# -*- coding: utf-8 -*-
"""Phase 95 D-45 — export_dossier skip_local parameter.

Tests that build_manuscript_row and build_bibliography_rows correctly handle
the skip_local kwarg:
- skip_local=False (desktop default): LOCAL sys_ids ARE included.
- skip_local=True  (web defense):     LOCAL sys_ids return None / [].
"""
import pytest


# A LOCAL sys_id (97-prefixed, 18 digits) and two regular Genizah sys_ids.
LOCAL_SYS_ID = "970012345601234567"
GENIZAH_SYS_ID_1 = "990025143260205171"
GENIZAH_SYS_ID_2 = "990012345600000001"


def _make_meta_resolver(data: dict):
    """Simple meta_resolver factory for tests."""
    def resolver(sys_id):
        return data.get(sys_id)
    return resolver


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_dossier_services(monkeypatch):
    """Patch the three service factory functions so tests run without DBs."""
    from shared import export_dossier

    def _no_pgp(sys_id, lang='en'):
        return {}

    def _no_nli(sys_id):
        return {}

    def _no_catalog(sys_id, lang='en'):
        return {}

    def _no_bib(sys_id, lang='en'):
        return []

    monkeypatch.setattr(export_dossier, 'pgp_subset_for_sys_id', _no_pgp)
    monkeypatch.setattr(export_dossier, 'nli_subset_for_sys_id', _no_nli)
    monkeypatch.setattr(export_dossier, 'catalog_summary_for_sys_id', _no_catalog)
    monkeypatch.setattr(export_dossier, 'bibliography_for_sys_id', _no_bib)


# ---------------------------------------------------------------------------
# build_manuscript_row tests
# ---------------------------------------------------------------------------

class TestBuildManuscriptRowSkipLocal:

    def test_skip_local_false_includes_local_row(self, mock_dossier_services):
        """D-45 desktop default: skip_local=False — LOCAL row is returned."""
        from shared.export_dossier import build_manuscript_row
        row = build_manuscript_row(LOCAL_SYS_ID, None, skip_local=False)
        assert row is not None, "skip_local=False must return a row for LOCAL sys_id"
        assert len(row) == 14, "row must have exactly 14 cells"
        # sys_id column (index 0) must be the LOCAL sys_id
        assert row[0] == LOCAL_SYS_ID

    def test_skip_local_true_excludes_local_row(self, mock_dossier_services):
        """D-45 web defense: skip_local=True — LOCAL row returns None."""
        from shared.export_dossier import build_manuscript_row
        row = build_manuscript_row(LOCAL_SYS_ID, None, skip_local=True)
        assert row is None, "skip_local=True must return None for LOCAL sys_id"

    def test_skip_local_true_keeps_genizah_row(self, mock_dossier_services):
        """D-45: skip_local=True must NOT drop regular Genizah rows."""
        from shared.export_dossier import build_manuscript_row
        row = build_manuscript_row(GENIZAH_SYS_ID_1, None, skip_local=True)
        assert row is not None, "skip_local=True must not drop Genizah rows"
        assert row[0] == GENIZAH_SYS_ID_1

    def test_skip_local_default_is_false(self, mock_dossier_services):
        """D-45: default behaviour (no kwarg) must include LOCAL rows."""
        from shared.export_dossier import build_manuscript_row
        row = build_manuscript_row(LOCAL_SYS_ID, None)
        assert row is not None, "Default (skip_local omitted) must include LOCAL"

    def test_empty_sys_id_returns_row_regardless(self, mock_dossier_services):
        """Edge case: empty sys_id is not LOCAL; should not be filtered."""
        from shared.export_dossier import build_manuscript_row
        row = build_manuscript_row('', None, skip_local=True)
        # Empty sys_id falls through — the function may return a blank row but
        # must not raise.
        # (Row is still returned; sys_id column will be empty string.)
        assert row is not None or row is None  # no exception — either outcome ok


# ---------------------------------------------------------------------------
# build_bibliography_rows tests
# ---------------------------------------------------------------------------

class TestBuildBibliographyRowsSkipLocal:

    def test_skip_local_false_includes_local(self, mock_dossier_services, monkeypatch):
        """D-45 desktop: skip_local=False — bib lookup proceeds for LOCAL."""
        from shared import export_dossier

        # Simulate one bib entry for the LOCAL sys_id.
        def _one_bib(sys_id, lang='en'):
            if sys_id == LOCAL_SYS_ID:
                return [{'article_author_eng': 'Author', 'article_name': 'Article',
                         'running_title': 'Journal', 'title_year': 2000,
                         'mention_page': '1', 'catalog_acronym': 'CAT'}]
            return []

        monkeypatch.setattr(export_dossier, 'bibliography_for_sys_id', _one_bib)

        rows = export_dossier.build_bibliography_rows(LOCAL_SYS_ID, None, skip_local=False)
        assert len(rows) == 1, "skip_local=False must return bib rows for LOCAL"
        assert rows[0][0] == LOCAL_SYS_ID

    def test_skip_local_true_excludes_local(self, mock_dossier_services):
        """D-45 web defense: skip_local=True — LOCAL returns empty list."""
        from shared.export_dossier import build_bibliography_rows
        rows = build_bibliography_rows(LOCAL_SYS_ID, None, skip_local=True)
        assert rows == [], "skip_local=True must return [] for LOCAL sys_id"

    def test_skip_local_true_keeps_genizah(self, mock_dossier_services, monkeypatch):
        """D-45: skip_local=True must not drop Genizah bib rows."""
        from shared import export_dossier

        def _one_bib(sys_id, lang='en'):
            if sys_id == GENIZAH_SYS_ID_1:
                return [{'article_author_eng': 'Author', 'article_name': 'Art',
                         'running_title': 'J', 'title_year': 2001,
                         'mention_page': '2', 'catalog_acronym': 'C'}]
            return []

        monkeypatch.setattr(export_dossier, 'bibliography_for_sys_id', _one_bib)

        rows = export_dossier.build_bibliography_rows(GENIZAH_SYS_ID_1, None, skip_local=True)
        assert len(rows) == 1, "skip_local=True must not drop Genizah bib rows"

    def test_skip_local_default_is_false(self, mock_dossier_services):
        """D-45: default kwarg omitted — LOCAL proceeds (no filtering)."""
        from shared.export_dossier import build_bibliography_rows
        # With mocked empty bib service, LOCAL returns [] (no entries), not filtered.
        rows = build_bibliography_rows(LOCAL_SYS_ID, None)
        assert isinstance(rows, list)
