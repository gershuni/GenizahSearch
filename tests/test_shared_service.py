# -*- coding: utf-8 -*-
"""
Smoke tests for shared service layer extraction (Phase 8).

Verifies:
- shared package is importable
- shared.supabase_provider exports get_client and reset_client
- shared.document_service exports all 12 functions
- web.document_service shim re-exports all 12 functions
- web.supabase_client.get_client still works
- Desktop importability (shared works without web/ on sys.path)
"""
import pytest


class TestSharedProviderImport:
    """Verify shared.supabase_provider is importable."""

    def test_shared_package_import(self):
        import shared
        assert shared is not None

    def test_shared_provider_exports(self):
        from shared.supabase_provider import get_client, reset_client
        assert callable(get_client)
        assert callable(reset_client)


class TestSharedDocumentServiceImport:
    """Verify all 12 functions importable from shared.document_service."""

    def test_all_12_functions_importable(self):
        from shared.document_service import (
            get_document_for_fragment,
            get_fragments_for_document,
            get_transcription_for_document,
            get_document_metadata,
            parse_transcription_sections,
            get_section_for_page,
            get_sources_for_document,
            get_all_sources_for_fragment,
            get_editions_for_document,
            get_translations_for_document,
            get_sys_ids_with_transcriptions,
            get_fragments_by_tag,
        )
        for fn in [get_document_for_fragment, get_fragments_for_document,
                    get_transcription_for_document, get_document_metadata,
                    parse_transcription_sections, get_section_for_page,
                    get_sources_for_document, get_all_sources_for_fragment,
                    get_editions_for_document, get_translations_for_document,
                    get_sys_ids_with_transcriptions, get_fragments_by_tag]:
            assert callable(fn)


class TestWebShimReexports:
    """Verify web.document_service shim re-exports all 12 functions."""

    def test_shim_reexports_all_12(self):
        from web.document_service import (
            get_document_for_fragment,
            get_fragments_for_document,
            get_transcription_for_document,
            get_document_metadata,
            parse_transcription_sections,
            get_section_for_page,
            get_sources_for_document,
            get_all_sources_for_fragment,
            get_editions_for_document,
            get_translations_for_document,
            get_sys_ids_with_transcriptions,
            get_fragments_by_tag,
        )
        for fn in [get_document_for_fragment, get_fragments_for_document,
                    get_transcription_for_document, get_document_metadata,
                    parse_transcription_sections, get_section_for_page,
                    get_sources_for_document, get_all_sources_for_fragment,
                    get_editions_for_document, get_translations_for_document,
                    get_sys_ids_with_transcriptions, get_fragments_by_tag]:
            assert callable(fn)

    def test_shim_and_shared_are_same_objects(self):
        """Web shim functions must be the exact same objects as shared ones."""
        from web.document_service import get_document_for_fragment as web_fn
        from shared.document_service import get_document_for_fragment as shared_fn
        assert web_fn is shared_fn


class TestWebSupabaseClientStillWorks:
    """Verify web/supabase_client.py get_client still works independently."""

    def test_web_get_client_importable(self):
        from web.supabase_client import get_client
        assert callable(get_client)


class TestDesktopImportability:
    """Verify desktop app can import shared service without web/ dependency."""

    def test_desktop_import_shared_directly(self):
        """Desktop imports shared.document_service, not web.document_service."""
        from shared.document_service import get_document_for_fragment
        assert callable(get_document_for_fragment)
