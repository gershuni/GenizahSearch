"""SC#3: Permanent genizah_core facade identity contract.

genizah_core is a permanent compatibility facade that re-exports symbols from
the shared/ modules extracted during v8.3.0. This file asserts the identity
contract (genizah_core.X is shared.Y.X) for all 27 names across 13 shared
modules. The facade is NEVER removed (contrast: the genizah_app D1 shims ARE
retired in Phase 127).

These identity assertions are also present in tests/test_no_back_edges_core.py
(where they serve as GUARD-01 regression tests). Both files should pass.
This file provides the NAMED, dedicated home for the SC#3 facade contract —
unambiguous documentation of what the permanent facade guarantees.

Phase map:
  Phase 122: shared/config.py                              (CORE-02)
  Phase 123: browse_map_utils, text_normalize, variants,
             responsa, codicological, joins_manager, lists_manager
  Phase 124: metadata_manager, indexer
  Phase 125: lab_settings, lab_engine, search_engine
"""


# ---------------------------------------------------------------------------
# Phase 122: config (CORE-02)
# ---------------------------------------------------------------------------

def test_config_facade_identity():
    """CONFIG-01: genizah_core.Config is the same class object as shared.config.Config."""
    import shared.config
    import genizah_core

    assert shared.config.Config is genizah_core.Config, (
        "genizah_core.Config is not the same object as shared.config.Config. "
        "The re-export shim in genizah_core.py must be: "
        "from shared.config import Config  # noqa: F401"
    )


# ---------------------------------------------------------------------------
# Phase 123: browse_map_utils (CORE-06)
# ---------------------------------------------------------------------------

def test_browse_map_utils_facade_identity():
    """CORE-06: browse_map_utils facade — 5 names."""
    import shared.browse_map_utils
    import genizah_core

    assert shared.browse_map_utils.normalize_shelfmark is genizah_core.normalize_shelfmark, (
        "genizah_core.normalize_shelfmark is not the same object as "
        "shared.browse_map_utils.normalize_shelfmark."
    )
    assert shared.browse_map_utils.natural_sort_key is genizah_core.natural_sort_key, (
        "genizah_core.natural_sort_key is not the same object as "
        "shared.browse_map_utils.natural_sort_key."
    )
    assert shared.browse_map_utils.get_library_display is genizah_core.get_library_display, (
        "genizah_core.get_library_display is not the same object as "
        "shared.browse_map_utils.get_library_display."
    )
    assert shared.browse_map_utils.LIBRARY_CODES is genizah_core.LIBRARY_CODES, (
        "genizah_core.LIBRARY_CODES is not the same object as "
        "shared.browse_map_utils.LIBRARY_CODES."
    )
    assert shared.browse_map_utils.dedupe_browse_map is genizah_core.dedupe_browse_map, (
        "genizah_core.dedupe_browse_map is not the same object as "
        "shared.browse_map_utils.dedupe_browse_map."
    )


# ---------------------------------------------------------------------------
# Phase 123: text_normalize (CORE-07)
# ---------------------------------------------------------------------------

def test_text_normalize_facade_identity():
    """CORE-07: text_normalize facade — 4 names."""
    import shared.text_normalize
    import genizah_core

    assert shared.text_normalize.strip_nikud is genizah_core.strip_nikud, (
        "genizah_core.strip_nikud is not the same object as shared.text_normalize.strip_nikud."
    )
    assert shared.text_normalize.strip_search_diacritics is genizah_core.strip_search_diacritics, (
        "genizah_core.strip_search_diacritics is not the same object as "
        "shared.text_normalize.strip_search_diacritics."
    )
    assert shared.text_normalize.NIKUD_PATTERN is genizah_core.NIKUD_PATTERN, (
        "genizah_core.NIKUD_PATTERN is not the same object as shared.text_normalize.NIKUD_PATTERN."
    )
    assert shared.text_normalize.COMBINING_DIACRITICALS_PATTERN is genizah_core.COMBINING_DIACRITICALS_PATTERN, (
        "genizah_core.COMBINING_DIACRITICALS_PATTERN is not the same object as "
        "shared.text_normalize.COMBINING_DIACRITICALS_PATTERN."
    )


# ---------------------------------------------------------------------------
# Phase 123: variants (CORE-08)
# ---------------------------------------------------------------------------

def test_variants_facade_identity():
    """CORE-08: variants facade — 1 name."""
    import shared.variants
    import genizah_core

    assert shared.variants.VariantManager is genizah_core.VariantManager, (
        "genizah_core.VariantManager is not the same object as shared.variants.VariantManager."
    )


# ---------------------------------------------------------------------------
# Phase 123: responsa (CORE-01)
# ---------------------------------------------------------------------------

def test_responsa_facade_identity():
    """CORE-01: responsa facade — 5 names."""
    import shared.responsa
    import genizah_core

    assert shared.responsa.ResponsaComponent is genizah_core.ResponsaComponent, (
        "genizah_core.ResponsaComponent is not the same object as "
        "shared.responsa.ResponsaComponent."
    )
    assert shared.responsa.parse_responsa_query is genizah_core.parse_responsa_query, (
        "genizah_core.parse_responsa_query is not the same object as "
        "shared.responsa.parse_responsa_query."
    )
    assert shared.responsa._apply_explosion_guard is genizah_core._apply_explosion_guard, (
        "genizah_core._apply_explosion_guard is not the same object as "
        "shared.responsa._apply_explosion_guard."
    )
    assert shared.responsa._count_expanded_terms is genizah_core._count_expanded_terms, (
        "genizah_core._count_expanded_terms is not the same object as "
        "shared.responsa._count_expanded_terms."
    )
    assert shared.responsa.GRAMMATICAL_PREFIXES is genizah_core.GRAMMATICAL_PREFIXES, (
        "genizah_core.GRAMMATICAL_PREFIXES is not the same object as "
        "shared.responsa.GRAMMATICAL_PREFIXES."
    )


# ---------------------------------------------------------------------------
# Phase 123: codicological (CORE-03)
# ---------------------------------------------------------------------------

def test_codicological_facade_identity():
    """CORE-03: codicological facade — 1 name."""
    import shared.codicological
    import genizah_core

    assert shared.codicological.CodicologicalManager is genizah_core.CodicologicalManager, (
        "genizah_core.CodicologicalManager is not the same object as "
        "shared.codicological.CodicologicalManager."
    )


# ---------------------------------------------------------------------------
# Phase 123: joins_manager (CORE-04)
# ---------------------------------------------------------------------------

def test_joins_manager_facade_identity():
    """CORE-04: joins_manager facade — 1 name."""
    import shared.joins_manager
    import genizah_core

    assert shared.joins_manager.JoinsManager is genizah_core.JoinsManager, (
        "genizah_core.JoinsManager is not the same object as "
        "shared.joins_manager.JoinsManager."
    )


# ---------------------------------------------------------------------------
# Phase 123: lists_manager (CORE-05)
# ---------------------------------------------------------------------------

def test_lists_manager_facade_identity():
    """CORE-05: lists_manager facade — 1 name."""
    import shared.lists_manager
    import genizah_core

    assert shared.lists_manager.ListsManager is genizah_core.ListsManager, (
        "genizah_core.ListsManager is not the same object as "
        "shared.lists_manager.ListsManager."
    )


# ---------------------------------------------------------------------------
# Phase 124: metadata_manager (CORE-09)
# ---------------------------------------------------------------------------

def test_metadata_manager_facade_identity():
    """CORE-09: metadata_manager facade — 4 names."""
    import shared.metadata_manager
    import genizah_core

    assert shared.metadata_manager.MetadataManager is genizah_core.MetadataManager, (
        "genizah_core.MetadataManager is not the same object as "
        "shared.metadata_manager.MetadataManager."
    )
    assert shared.metadata_manager._BoundedLRUCache is genizah_core._BoundedLRUCache, (
        "genizah_core._BoundedLRUCache is not the same object as "
        "shared.metadata_manager._BoundedLRUCache."
    )
    assert shared.metadata_manager.MARC_FUTURE_TIMEOUT is genizah_core.MARC_FUTURE_TIMEOUT, (
        "genizah_core.MARC_FUTURE_TIMEOUT is not the same object as "
        "shared.metadata_manager.MARC_FUTURE_TIMEOUT."
    )
    assert shared.metadata_manager._NLI_CACHE_MAX_ENTRIES is genizah_core._NLI_CACHE_MAX_ENTRIES, (
        "genizah_core._NLI_CACHE_MAX_ENTRIES is not the same object as "
        "shared.metadata_manager._NLI_CACHE_MAX_ENTRIES."
    )


# ---------------------------------------------------------------------------
# Phase 124: indexer (CORE-10)
# ---------------------------------------------------------------------------

def test_indexer_facade_identity():
    """CORE-10: indexer facade — 1 name."""
    import shared.indexer
    import genizah_core

    assert shared.indexer.Indexer is genizah_core.Indexer, (
        "genizah_core.Indexer is not the same object as shared.indexer.Indexer."
    )


# ---------------------------------------------------------------------------
# Phase 125: lab_settings (CORE-11)
# ---------------------------------------------------------------------------

def test_lab_settings_facade_identity():
    """CORE-11: lab_settings facade — 1 name."""
    import shared.lab_settings
    import genizah_core

    assert shared.lab_settings.LabSettings is genizah_core.LabSettings, (
        "genizah_core.LabSettings is not the same object as shared.lab_settings.LabSettings."
    )


# ---------------------------------------------------------------------------
# Phase 125: lab_engine (CORE-12)
# ---------------------------------------------------------------------------

def test_lab_engine_facade_identity():
    """CORE-12: lab_engine facade — 1 name."""
    import shared.lab_engine
    import genizah_core

    assert shared.lab_engine.LabEngine is genizah_core.LabEngine, (
        "genizah_core.LabEngine is not the same object as shared.lab_engine.LabEngine."
    )


# ---------------------------------------------------------------------------
# Phase 125: search_engine (CORE-13)
# ---------------------------------------------------------------------------

def test_search_engine_facade_identity():
    """CORE-13: search_engine facade — 1 name."""
    import shared.search_engine
    import genizah_core

    assert shared.search_engine.SearchEngine is genizah_core.SearchEngine, (
        "genizah_core.SearchEngine is not the same object as shared.search_engine.SearchEngine."
    )
