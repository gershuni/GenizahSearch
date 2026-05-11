# -*- coding: utf-8 -*-
"""
Browse Enrichment Functions

Extracted from web/pages/browse.py (Phase 73).
Plan 01 creates this stub with BrowsePageRefs dataclass.
Plan 02 adds the three enrichment function bodies
(load_enrichment, update_enrichment_sections, populate_bib_catalog_buttons).

Each function that was a closure in create_browse_page() now takes explicit
state and refs parameters instead of capturing them via closure.

IMPORTANT: This module must NOT import from web.pages.browse to avoid
circular imports. All shared types come from web.pages.browse_state.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Dict

from nicegui import ui, run

from web.translations import tr
from web.document_service import get_document_for_fragment, get_section_for_page, get_all_sources_for_fragment
from web.pages.browse_state import BrowseState, _crossref_cache
from shared.synthetic_sys_id import is_synthetic_sys_id

logger = logging.getLogger(__name__)


@dataclass
class BrowsePageRefs:
    """UI element references and callbacks needed by extracted browse_enrichment functions.

    Populated in create_browse_page() after all UI elements and callbacks are defined.
    These are ephemeral per-page references, NOT persistent state (per D-06).

    Population timing:
    - enrichment_refs: alias assigned at construction, populated incrementally
      during update_content() calls (lines ~4189, ~4313-4341, ~4617-4618)
    - load_generation: default {'value': 0} at construction, used as-is
    - page_client: set immediately after construction (ui.context.client)
    - content_container: set when main content area is created (~line 4210+)
    - slider_refs: alias assigned at construction, populated in update_content()
    - enter_joined_view: set after enter_joined_view() defined (~line 1450)
    - update_content: set after update_content() defined (~line 2083)

    All callbacks are set BEFORE any asyncio.ensure_future(load_page(...)) call,
    which happens at the end of create_browse_page() (~lines 4982-5041).
    """
    enrichment_refs: Dict[str, Any] = field(default_factory=dict)
    # SAFETY NOTE: load_generation MUST remain a mutable dict container (not an int).
    # Local aliases in browse.py bind to this dict object -- if changed to a primitive,
    # the alias would be a copy and stale-generation checks would silently break.
    load_generation: Dict[str, int] = field(default_factory=lambda: {'value': 0})
    page_client: Any = None
    content_container: Any = None
    slider_refs: Dict[str, Any] = field(default_factory=dict)

    # Callback functions (set after definition in create_browse_page)
    enter_joined_view: Any = None
    update_content: Any = None


async def load_enrichment(state: BrowseState, refs: BrowsePageRefs, page, generation):
    """Phase B: Load PGP + FJMS enrichment data in background."""
    state.enrichment_loading = True

    async def fetch_pgp():
        _page_sys_id = page.sys_id
        _page_p_num = page.p_num

        def _pgp_sync():
            all_sources = get_all_sources_for_fragment(_page_sys_id)
            pgp_doc = get_document_for_fragment(_page_sys_id, _page_p_num)
            return all_sources, pgp_doc

        try:
            return await run.io_bound(_pgp_sync)
        except Exception as e:
            logger.error(f"Failed to fetch PGP data: {e}")
            return None, None

    async def fetch_fjms():
        _page_sys_id = page.sys_id

        def _fjms_sync():
            from shared.fjms_service import get_fjms_service
            fjms = get_fjms_service(thread_safe=True)
            if fjms.is_available():
                result = {
                    'catalog_records': fjms.get_catalog_records(_page_sys_id),
                    'domains': fjms.get_domains(_page_sys_id),
                    'bibliography': fjms.get_bibliography(_page_sys_id),
                    'source_names': fjms.get_source_names(_page_sys_id),
                    'catalog_refs': fjms.get_catalog_refs(_page_sys_id),
                    'has_measurements': fjms.has_measurements(_page_sys_id),
                }
                # Phase 57: Visual Similarity availability check
                try:
                    from shared.visual_similarity_service import get_vs_service
                    vs_svc = get_vs_service(thread_safe=True)
                    if vs_svc.is_available():
                        _vs_count = vs_svc.get_suggestion_count(_page_sys_id)
                        result['has_visual_suggestions'] = _vs_count > 0
                        result['visual_suggestion_count'] = _vs_count
                    else:
                        result['has_visual_suggestions'] = False
                        result['visual_suggestion_count'] = 0
                except Exception:
                    result['has_visual_suggestions'] = False  # Feature detection failed; assume not available
                    result['visual_suggestion_count'] = 0
                return result
            return None

        try:
            return await run.io_bound(_fjms_sync)
        except Exception as e:
            logger.error(f"Failed to fetch FJMS data: {e}")
            return None

    async def fetch_crossref():
        _page_sys_id = page.sys_id
        # Check session cache first
        if _page_sys_id in _crossref_cache:
            return _crossref_cache[_page_sys_id]

        def _crossref_sync():
            from shared.nli_crossref_service import get_nli_crossref_service
            svc = get_nli_crossref_service(thread_safe=True)
            if svc.is_available() and _page_sys_id:
                return svc.get_crossref_metadata(_page_sys_id)
            return {}

        try:
            result = await run.io_bound(_crossref_sync)
            _crossref_cache[_page_sys_id] = result  # Cache for session
            return result
        except Exception as e:
            logger.error(f"Failed to fetch crossref data: {e}")
            return {}

    async def fetch_browse_enrichment():
        """Fetch crossref + Oxford + Cambridge + attribution data deferred from Phase A."""
        _page = page
        _sys_id = _page.sys_id
        _shelfmark = _page.shelfmark
        _library_code = _page.library_code
        _is_oxford = _page.is_oxford
        _p_num = _page.p_num
        _total_pages = _page.total_pages

        def _browse_enrich_sync():
            from web.services import ATTRIBUTION_BY_LIBRARY, _get_library_attribution
            from web.state import state as state_mod
            result = {}

            # Attribution cascade
            attribution = ''
            if _sys_id and hasattr(state_mod.meta_mgr, 'nli_cache'):
                cached_meta = state_mod.meta_mgr.nli_cache.get(_sys_id, {})
                attribution = cached_meta.get('attribution', '')
            if _library_code in ATTRIBUTION_BY_LIBRARY:
                lib_attr = _get_library_attribution(_library_code)
                if lib_attr is not None:
                    attribution = lib_attr
            elif _is_oxford:
                attribution = 'Bodleian Libraries, University of Oxford \u00b7 CC BY-NC 4.0'
            if attribution:
                result['attribution'] = attribution

            # Oxford codicological
            if _is_oxford and _sys_id:
                if hasattr(state_mod.meta_mgr, 'get_part_for_folio'):
                    part_id = state_mod.meta_mgr.get_part_for_folio(_sys_id)
                    if part_id:
                        result['oxford_part_id'] = part_id
                        if hasattr(state_mod.meta_mgr.codico_mgr, 'get_part_display_name'):
                            result['oxford_part_display'] = state_mod.meta_mgr.codico_mgr.get_part_display_name(part_id)
                        part_meta = state_mod.meta_mgr.get_part_metadata(part_id)
                        if part_meta:
                            ox_meta = {}
                            for key in ['title', 'contents', 'provenance']:
                                if part_meta.get(key):
                                    ox_meta[key] = part_meta[key]
                            result['oxford_part_metadata'] = ox_meta
                            if part_meta.get('direct_link'):
                                result['external_url'] = part_meta['direct_link']

            # Cambridge MARC
            if 'external_url' not in result:
                marc_data = {}
                if hasattr(state_mod.meta_mgr, 'nli_cache') and _sys_id in state_mod.meta_mgr.nli_cache:
                    marc_data = state_mod.meta_mgr.nli_cache[_sys_id].get('marc', {})
                ext_link = marc_data.get('external_iiif_link')
                if ext_link and "cudl.lib.cam.ac.uk" in ext_link:
                    result['is_cambridge'] = True
                    result['external_url'] = ext_link.replace("/iiif/", "/view/")

            # NLI crossref queries
            try:
                from shared.nli_crossref_service import get_nli_crossref_service
                crossref_svc = get_nli_crossref_service(thread_safe=True)
                if crossref_svc.is_available() and _sys_id:
                    from genizah_core import normalize_shelfmark
                    norm_shelf = normalize_shelfmark(_shelfmark) if _shelfmark else None
                    # Phase 84 follow-up: pass raw shelfmark so the service can
                    # fall through to the bridge cascade when the canonical-form
                    # direct query misses (Mosseri label, T-S slash + leading-zero,
                    # Or.-numeric-collapse). Without this, _has_cambridge stays
                    # False in browse and the Cambridge button + IIIF images are
                    # suppressed despite a valid manifest URL upstream.
                    result['image_source_info'] = crossref_svc.get_image_sources(
                        _sys_id, normalized_shelfmark=norm_shelf, shelfmark=_shelfmark
                    )
                    if not result.get('is_cambridge') and result['image_source_info'].get('cambridge'):
                        result['is_cambridge'] = True
                    folio_images = crossref_svc.get_folio_images(_sys_id)
                    result['folio_images'] = folio_images
                    if (folio_images
                            and len(folio_images) == _total_pages
                            and 0 < _p_num <= len(folio_images)):
                        result['folio_label'] = folio_images[_p_num - 1].get('folio_label', '')

                    # Skip get_physical_metadata here -- fetch_crossref already gets it
                    # via get_crossref_metadata. We read it from crossref_data after gather.

                    result['library_viewer_url'] = crossref_svc.get_library_viewer_url(_sys_id)

                    # Derive fl_id for metadata-only records (no Tantivy fl_id)
                    if not _page.fl_id and folio_images:
                        first_fl = folio_images[0].get('fl_id')
                        if first_fl:
                            result['derived_fl_id'] = first_fl
            except Exception as e:
                logger.error("Browse enrichment crossref error: %s", e)

            # External images from nli_cache (populated by enrich_metadata)
            if _sys_id and hasattr(state_mod.meta_mgr, 'nli_cache'):
                cached = state_mod.meta_mgr.nli_cache.get(_sys_id, {})
                if not cached.get('images_ext'):
                    # nli_cache not yet populated — call enrich_metadata to resolve
                    # Manchester/Cambridge/JTS images from crossref sidecar + IIIF
                    try:
                        state_mod.meta_mgr.enrich_metadata(_sys_id)
                        cached = state_mod.meta_mgr.nli_cache.get(_sys_id, {})
                    except Exception as e:
                        logger.warning("Browse enrichment enrich_metadata error: %s", e)
                result['cambridge_images'] = cached.get('images_ext', [])
                result['external_provider'] = cached.get('external_provider', '')
                result['cambridge_alignment'] = cached.get('cambridge_alignment')
                # Phase 84 follow-up: propagate external_url back from nli_cache when
                # the MARC branch (line 192-199 above) didn't set one. enrich_metadata
                # may have resolved it via the bridge supplement (Cambridge for
                # CUDL-only rows like Mosseri). Without this, pg.external_url stays
                # empty for Mosseri/CUL-CUDL rows and the browse "View on CUDL" link
                # falls through to a lossy shelfmark-slug fallback that 404s.
                if not result.get('external_url') and cached.get('external_url'):
                    cached_url = cached.get('external_url') or ''
                    if 'cudl.lib.cam.ac.uk' in cached_url.lower():
                        result['is_cambridge'] = True
                    result['external_url'] = cached_url

            return result

        try:
            return await run.io_bound(_browse_enrich_sync)
        except Exception as e:
            logger.error(f"Failed to fetch browse enrichment: {e}")
            return {}

    try:
        (all_sources, pgp_doc), fjms_data, crossref_data, browse_enrich = await asyncio.gather(
            fetch_pgp(), fetch_fjms(), fetch_crossref(), fetch_browse_enrichment()
        )
    except Exception as e:
        logger.error(f"Enrichment fetch failed: {e}")
        state.enrichment_loaded = True
        state.enrichment_loading = False
        return

    # Stale check
    if generation != refs.load_generation['value']:
        return

    # Process PGP sources
    if all_sources:
        current_page_info = 'recto' if page.p_num == 1 else 'verso'
        page_sources = []
        for source in all_sources:
            source_page = source.get('page_info')
            if source_page == current_page_info or not source_page:
                is_translation = 'Translation' in (source.get('doc_relation') or '')
                if source.get('content'):
                    if not is_translation and not source_page:
                        source['content'] = get_section_for_page(source['content'], page.p_num, source.get('sections'))
                page_sources.append(source)
        state.all_sources = page_sources if page_sources else None
    else:
        state.all_sources = None

    # Process PGP document metadata
    if pgp_doc:
        state.pgp_metadata = {
            'document_type': pgp_doc.get('document_type'),
            'tags': pgp_doc.get('tags', []),
            'description': pgp_doc.get('description'),
            'languages_primary': pgp_doc.get('languages_primary'),
            'languages_secondary': pgp_doc.get('languages_secondary'),
            'doc_date_original': pgp_doc.get('doc_date_original'),
            'doc_date_standard': pgp_doc.get('doc_date_standard'),
            'inferred_date_display': pgp_doc.get('inferred_date_display'),
            'inferred_date_standard': pgp_doc.get('inferred_date_standard'),
            'inferred_date_rationale': pgp_doc.get('inferred_date_rationale'),
            'pgp_url': pgp_doc.get('pgp_url'),
            'pgpid': pgp_doc.get('pgpid'),
        }

        pgpid = pgp_doc.get('pgpid')
        doc_relation = pgp_doc.get('doc_relation', '')
        is_edition = 'Edition' in doc_relation or not doc_relation
        page_content = get_section_for_page(pgp_doc['transcription'], page.p_num, fragment_page_info=pgp_doc.get('_fragment_page_info')) if pgp_doc.get('transcription') else None

        if is_edition and page_content:
            state.pgp_transcription = {
                'full_content': pgp_doc['transcription'],
                'content': page_content,
                'attribution': pgp_doc.get('transcription_source', 'PGP'),
                'pgp_url': pgp_doc.get('pgp_url'),
                'pgpid': pgpid
            }
        else:
            state.pgp_transcription = None
    else:
        state.pgp_transcription = None
        state.pgp_metadata = None

    state.fjms_data = fjms_data
    state.crossref_data = crossref_data

    # Apply browse enrichment (crossref + Oxford + Cambridge + attribution) to page
    if state.current_page:
        pg = state.current_page
        if browse_enrich:
            if browse_enrich.get('attribution'):
                pg.attribution = browse_enrich['attribution']
            if browse_enrich.get('oxford_part_id'):
                pg.oxford_part_id = browse_enrich['oxford_part_id']
                pg.oxford_part_display = browse_enrich.get('oxford_part_display', '')
                pg.oxford_part_metadata = browse_enrich.get('oxford_part_metadata', {})
            if browse_enrich.get('external_url'):
                pg.external_url = browse_enrich['external_url']
            if browse_enrich.get('is_cambridge'):
                pg.is_cambridge = True
            if browse_enrich.get('image_source_info'):
                pg.image_source_info = browse_enrich['image_source_info']
            if browse_enrich.get('folio_images'):
                pg.folio_images = browse_enrich['folio_images']
            if browse_enrich.get('folio_label'):
                pg.folio_label = browse_enrich['folio_label']
            if browse_enrich.get('library_viewer_url'):
                pg.library_viewer_url = browse_enrich['library_viewer_url']
            if browse_enrich.get('cambridge_images'):
                pg.cambridge_images = browse_enrich['cambridge_images']
            if browse_enrich.get('external_provider'):
                pg.external_provider = browse_enrich['external_provider']
            if browse_enrich.get('cambridge_alignment') is not None:
                pg.cambridge_alignment = browse_enrich['cambridge_alignment']
            # Derived fl_id for metadata-only records
            if browse_enrich.get('derived_fl_id') and not pg.fl_id:
                from web.services import get_thumbnail_url, get_full_image_url
                pg.fl_id = browse_enrich['derived_fl_id']
                pg.thumb_url = get_thumbnail_url(pg.fl_id)
                pg.image_url = get_full_image_url(pg.fl_id)
                pg.p_num = 1
                state.page_input_value = 1
                if browse_enrich.get('folio_images'):
                    pg.folio_label = browse_enrich['folio_images'][0].get('folio_label', '')
                    pg.total_pages = len(browse_enrich['folio_images'])
                    pg.current_idx = 1
            # Phase 86: synthetic rows (no NLI crossref fl_id) have empty
            # folio_images but cambridge_images populated via the bridge.
            # Derive total_pages from cambridge_images so pagination works,
            # and clamp p_num/current_idx into [1, total_pages].
            if not pg.total_pages and pg.cambridge_images:
                pg.total_pages = len(pg.cambridge_images)
                clamped = pg.p_num if pg.p_num and 1 <= pg.p_num <= pg.total_pages else 1
                pg.p_num = clamped
                pg.current_idx = clamped
                state.page_input_value = clamped

        # Physical metadata from crossref_data (independent of browse_enrich success)
        if crossref_data and crossref_data.get('physical_metadata'):
            pg.physical_metadata = crossref_data['physical_metadata']

        # Oxford translations (deferred from Phase A since oxford_part_metadata was empty)
        if pg.oxford_part_metadata:
            _ox_meta = pg.oxford_part_metadata
            def _fetch_oxford_translations():
                try:
                    from shared.translation_service import TranslationService
                    svc = TranslationService(thread_safe=True)
                    result = {}
                    if svc.oxford_available():
                        texts = [_ox_meta.get(f, '').strip() for f in ('title', 'contents', 'provenance') if _ox_meta.get(f, '').strip()]
                        if texts:
                            result = svc.get_oxford_translations_batch(texts)
                    svc.close()
                    return result
                except Exception:
                    return {}  # Lookup failed; return empty dict
            try:
                ox_result = await run.io_bound(_fetch_oxford_translations)
                # Re-check generation after await to avoid stale state on rapid navigation
                if generation == refs.load_generation['value']:
                    state.oxford_translations = ox_result
            except Exception:
                pass  # Translation lookup failed; continue without translation

    # Re-check generation before committing final state (guards Oxford await above)
    if generation != refs.load_generation['value']:
        return

    state.enrichment_loaded = True
    state.enrichment_loading = False

    # Re-render with full enrichment data (attribution, source badges, folio labels,
    # external links, Oxford part, physical metadata) then update PGP/FJMS containers
    refs.update_content()
    update_enrichment_sections(state, refs)


def update_enrichment_sections(state: BrowseState, refs: BrowsePageRefs):
    """Update enrichment placeholder containers after Phase B completes."""
    # PGP link button in header
    pgp_container = refs.enrichment_refs.get('pgp_link_container')
    if pgp_container:
        pgp_container.clear()
        if state.pgp_metadata and state.pgp_metadata.get('pgp_url'):
            with pgp_container:
                with ui.link(target=state.pgp_metadata['pgp_url'], new_tab=True).classes(
                    'flex items-center gap-1 px-2 py-1 rounded'
                ).style(
                    'text-decoration: none; '
                    'color: #ffffff !important; '
                    'background: rgba(255, 255, 255, 0.2);'
                ):
                    ui.icon('open_in_new', size='sm').style('color: #ffffff !important;')
                    ui.label('PGP').classes('text-sm font-semibold').style('color: #ffffff !important;')

    # Version selector
    version_container = refs.enrichment_refs.get('version_container')
    if version_container and (state.pgp_transcription or state.all_sources):
        version_container.clear()
        handler = refs.enrichment_refs.get('version_change_handler')
        page = state.current_page
        if handler and page and page.text:
            with version_container:
                from web.components import create_version_selector
                create_version_selector(
                    document_id=page.sys_id,
                    page_number=page.p_num,
                    original_text=page.text,
                    on_version_change=handler,
                    pgp_transcription=state.pgp_transcription,
                    all_sources=state.all_sources
                )

    # Joins button
    joins_container = refs.enrichment_refs.get('joins_container')
    if joins_container:
        joins_container.clear()
        page = state.current_page
        if page and page.shelfmark:
            pgpid_for_joins = state.pgp_metadata.get('pgpid') if state.pgp_metadata else None
            navigate_fn = refs.enrichment_refs.get('navigate_to_shelfmark')
            with joins_container:
                from web.components import create_joins_button
                create_joins_button(
                    shelfmark=page.shelfmark,
                    document_id=page.sys_id,
                    pgpid=pgpid_for_joins,
                    on_navigate=navigate_fn,
                    on_view_all=refs.enter_joined_view
                )

    # Bibliography & Catalog buttons
    bib_catalog_container = refs.enrichment_refs.get('bib_catalog_container')
    if bib_catalog_container:
        populate_bib_catalog_buttons(bib_catalog_container, state, state.current_page)


def populate_bib_catalog_buttons(container, state: BrowseState, page):
    """Populate bibliography and catalog buttons in the page navigation pane.

    Called both inline (if enrichment already loaded) and from
    update_enrichment_sections (deferred enrichment Phase B).
    """
    if not page:
        return
    container.clear()

    fjms_data = state.fjms_data or {}
    fjms_bib = fjms_data.get('bibliography', [])

    # NLI/Ktiv bibliography from crossref cache
    marc_bib = []
    # Phase 85 D-06: synthetic sys_ids have no NLI MARC record — skip the cache
    # read entirely so the bibliography panel doesn't render a "Bibliography Ktiv"
    # chip with empty contents.
    if not is_synthetic_sys_id(page.sys_id):
        try:
            from web.state import state as app_state
            if app_state.meta_mgr and hasattr(app_state.meta_mgr, 'nli_cache'):
                cached = app_state.meta_mgr.nli_cache.get(page.sys_id, {})
                marc_bib = cached.get('marc', {}).get('bibliography', [])
        except Exception:
            pass  # Cache operation failed; continue without cached data

    catalog_source_count = len(fjms_data.get('source_names', []))

    has_meas = fjms_data.get('has_measurements', False)
    has_vs = fjms_data.get('has_visual_suggestions', False)
    if not fjms_bib and not marc_bib and catalog_source_count == 0 and not has_meas and not has_vs:
        return

    from web.components.bibliography_dialog import create_fjms_bibliography_dialog, create_nli_bibliography_dialog
    from web.components.catalog_dialog import show_catalog_dialog

    bib_chip_style = 'border: 1.5px solid #7e57c2; border-radius: 12px; min-height: 22px; color: #7e57c2; font-weight: 600;'
    catalog_chip_style = 'border: 1.5px solid #5c6bc0; border-radius: 12px; min-height: 22px; color: #5c6bc0; font-weight: 600;'

    with container:
        if fjms_bib:
            fjms_dlg = create_fjms_bibliography_dialog(
                fjms_bib, page.sys_id,
                shelfmark=page.shelfmark or '',
            )
            ui.button(
                f'{tr("Bib. FJMS")} ({len(fjms_bib)})',
                on_click=fjms_dlg.open,
            ).props('flat dense size=sm no-caps').classes('text-xs px-2 py-0').style(bib_chip_style)
        if marc_bib:
            nli_dlg = create_nli_bibliography_dialog(
                marc_bib, page.sys_id,
                shelfmark=page.shelfmark or '',
            )
            ui.button(
                f'{tr("Bib. Ktiv")} ({len(marc_bib)})',
                on_click=nli_dlg.open,
            ).props('flat dense size=sm no-caps').classes('text-xs px-2 py-0').style(bib_chip_style)
        if catalog_source_count > 0:
            ui.button(
                f'{tr("Catalog rec.")} ({catalog_source_count})',
                on_click=lambda s=page.sys_id, sm=page.shelfmark or '': show_catalog_dialog(s, sm),
            ).props('flat dense size=sm no-caps').classes('text-xs px-2 py-0').style(catalog_chip_style)

        # Measurements button (teal chip, distinct from catalog indigo and bib purple)
        if has_meas:
            from web.components.measurements_dialog import show_measurements_dialog
            measurements_chip_style = 'border: 1.5px solid #00897b; border-radius: 12px; min-height: 22px; color: #00897b; font-weight: 600;'
            _meas_side = 'recto' if page.p_num == 1 else 'verso'
            ui.button(
                f'{tr("Measurements")}',
                on_click=lambda s=page.sys_id, sm=page.shelfmark or '', side=_meas_side: show_measurements_dialog(s, sm, image_side=side),
            ).props('flat dense size=sm no-caps').classes('text-xs px-2 py-0').style(measurements_chip_style)

        # Phase 57: Visual Similarity chip (orange, distinct from other enrichment colors)
        has_vs = fjms_data.get('has_visual_suggestions', False)
        if has_vs:
            from web.components.visual_similarity_dialog import show_visual_similarity_dialog
            vs_chip_style = 'border: 1.5px solid #ef6c00; border-radius: 12px; min-height: 22px; color: #ef6c00;'
            vs_count = fjms_data.get('visual_suggestion_count', 0)
            ui.button(
                f'{tr("Visual Similarity")} ({vs_count})',
                on_click=lambda s=page.sys_id, sm=page.shelfmark or '': show_visual_similarity_dialog(s, sm),
            ).props('flat dense size=sm no-caps').classes('text-xs px-2 py-0').style(vs_chip_style)
