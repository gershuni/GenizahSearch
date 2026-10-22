from web.search_bootstrap import resolve_search_bootstrap


def test_restores_saved_state_without_route_context():
    state = resolve_search_bootstrap(
        initial_query=None,
        initial_tag=None,
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='Title',
        saved_query='ישן',
        saved_results_count=0,
        use_slider=False,
    )

    assert state == {
        'mode': 'Title',
        'query': 'ישן',
        'restore_saved_results': True,
        'restore_saved_filters': True,
        'restore_saved_exclusions': True,
    }


def test_query_route_uses_clean_default_state():
    state = resolve_search_bootstrap(
        initial_query='בלי ירח',
        initial_tag=None,
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='Title',
        saved_query='ישן',
        saved_results_count=0,
        use_slider=False,
    )

    assert state == {
        'mode': 'exact',
        'query': 'בלי ירח',
        'restore_saved_results': False,
        'restore_saved_filters': False,
        'restore_saved_exclusions': False,
    }


def test_explicit_route_mode_wins_and_slider_maps_extended_variants():
    state = resolve_search_bootstrap(
        initial_query='שלום',
        initial_tag=None,
        initial_mode='variants_extended',
        initial_domain=None,
        from_browse=None,
        saved_mode='Title',
        saved_query='ישן',
        saved_results_count=0,
        use_slider=True,
    )

    assert state['mode'] == 'variants'
    assert state['query'] == 'שלום'
    assert state['restore_saved_results'] is False


def test_tag_route_forces_tag_mode_and_clears_saved_query():
    state = resolve_search_bootstrap(
        initial_query=None,
        initial_tag='Merchant',
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='Regex',
        saved_query='ישן',
        saved_results_count=0,
        use_slider=False,
    )

    assert state == {
        'mode': 'pgp_tags',
        'query': '',
        'restore_saved_results': False,
        'restore_saved_filters': False,
        'restore_saved_exclusions': False,
    }


def test_browse_navigation_disables_saved_state_restore():
    state = resolve_search_bootstrap(
        initial_query=None,
        initial_tag=None,
        initial_mode=None,
        initial_domain='Letters',
        from_browse=1,
        saved_mode='Regex',
        saved_query='ישן',
        saved_results_count=0,
        use_slider=False,
    )

    assert state == {
        'mode': 'exact',
        'query': '',
        'restore_saved_results': False,
        'restore_saved_filters': False,
        'restore_saved_exclusions': False,
    }


def test_back_navigation_from_browse_restores_saved_results():
    """Browser Back from /browse to /search?q=... must restore the saved
    snapshot when URL query matches saved_query and snapshot has results.

    What IS restored on back-nav:
      - cached results (restore_saved_results=True)
      - saved mode (restore_saved_results path uses saved_mode)
      - refinement chain (via restore_saved_results path in search.py)
      - exclusions (restore_saved_exclusions=True)

    What is NOT restored on back-nav:
      - pre-search filters (material/domain/printed sliders) — restore_saved_filters=False.
        Rationale: history.replaceState only stamps q/tag/mode/variants into the URL,
        never filters. There is no authoritative round-trip signal that filter state
        belongs to the URL. 829cd7cf (2026-03-27) intent preserved.

    Regression test for 75-UAT.md surface 1 blocker (back-navigation
    state loss). See 75-03-PLAN.md.
    """
    state = resolve_search_bootstrap(
        initial_query='שלום',
        initial_tag=None,
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='exact',
        saved_query='שלום',
        saved_results_count=12,
        use_slider=False,
    )

    assert state['restore_saved_results'] is True, (
        "Back-navigation must restore cached results"
    )
    assert state['restore_saved_exclusions'] is True, (
        "Back-navigation must restore exclusions (they were part of the snapshot)"
    )
    assert state['restore_saved_filters'] is False, (
        "Back-navigation must NOT restore filters "
        "(history.replaceState did not stamp them; 829cd7cf intent preserved)"
    )
    assert state['query'] == 'שלום'
    assert state['mode'] == 'exact'


def test_fresh_query_route_with_different_saved_query_still_uses_clean_state():
    """Genuinely-fresh /search?q=X (different query than saved) must still
    use clean deterministic state — do not inherit stale snapshot.

    Preserves commit 829cd7cf (2026-03-27) intent.
    """
    state = resolve_search_bootstrap(
        initial_query='שלום',
        initial_tag=None,
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='exact',
        saved_query='לילה',  # different query than URL
        saved_results_count=12,
        use_slider=False,
    )

    assert state['restore_saved_results'] is False
    assert state['restore_saved_filters'] is False
    assert state['restore_saved_exclusions'] is False
    assert state['query'] == 'שלום'
    assert state['mode'] == 'exact'


def test_query_route_with_matching_saved_but_empty_snapshot_does_not_falsely_restore():
    """URL query matches saved_query but snapshot has zero results —
    nothing to restore, fall through to clean state.
    """
    state = resolve_search_bootstrap(
        initial_query='שלום',
        initial_tag=None,
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='exact',
        saved_query='שלום',
        saved_results_count=0,  # empty snapshot
        use_slider=False,
    )

    assert state['restore_saved_results'] is False
    assert state['restore_saved_filters'] is False
    assert state['restore_saved_exclusions'] is False


def test_back_navigation_restores_saved_mode_when_saved_mode_is_title():
    """When user was searching in 'Title' mode and hits Back, restore 'Title'
    mode — not reset to 'exact'. Verifies Edit 1(d)'s is_back_navigation
    branch honors saved_mode beyond the default.

    Added per checker warning 4: the mode-restoration branch was untested
    when saved_mode != 'exact'.
    """
    state = resolve_search_bootstrap(
        initial_query='שלום',
        initial_tag=None,
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='Title',
        saved_query='שלום',
        saved_results_count=42,
        use_slider=False,
    )

    assert state['restore_saved_results'] is True
    assert state['mode'] == 'Title'


# --- Back to a Responsa search (2026-10-22) ---------------------------------
# A finished Responsa search stamps q=..&mode=responsa plus its option flags into
# the URL, so its Back carries a mode. It was treated as a fresh link and the whole
# search ran again (production 2026-10-22: one 45 s search ran three times). Back
# is now recognised by the saved run: same query, same options.

_RESPONSA_QUERY = '(מאמתי/מאימתי) (קורין/קוריין)'


def _responsa_run(query=_RESPONSA_QUERY, mode='responsa', **options):
    return {
        'query': query, 'mode': mode, 'gap': 0, 'exclude_words': [], 'text_position': None,
        'responsa_options': {
            'responsa_mode': True,
            'variants': options.get('variants', True),
            'ja': options.get('ja', False),
            'flex_spacing': options.get('flex_spacing', True),
            'bidirectional': options.get('bidirectional', False),
            'variant_mode': 'variants' if options.get('variants', True) else 'exact',
        },
        'corpus_scope': 'genizah', 'variant_settings': {},
    }


def _responsa_back(*, initial_query=_RESPONSA_QUERY, initial_responsa=None, saved_run=None,
                   saved_results_count=81, saved_mode='exact', saved_query=_RESPONSA_QUERY):
    # The URL search.py stamps for variants + flexible spacing: &variants=1&flex_spaces=1
    # (the route's ints, by the engine's option names).
    if initial_responsa is None:
        initial_responsa = {'variants': 1, 'ja': None, 'flex_spacing': 1, 'bidirectional': None}
    return resolve_search_bootstrap(
        initial_query=initial_query,
        initial_tag=None,
        initial_mode='responsa',
        initial_domain=None,
        from_browse=None,
        saved_mode=saved_mode,
        saved_query=saved_query,
        saved_results_count=saved_results_count,
        use_slider=False,
        initial_responsa=initial_responsa,
        saved_run=_responsa_run() if saved_run is None else saved_run,
    )


def test_back_to_a_responsa_search_restores_its_results():
    state = _responsa_back()

    assert state == {
        'mode': 'responsa',
        'query': _RESPONSA_QUERY,
        'restore_saved_results': True,
        'restore_saved_filters': False,
        'restore_saved_exclusions': True,
    }


def test_responsa_back_does_not_depend_on_the_saved_mode_preference():
    """A Responsa search started from a /search?mode=responsa link sets the mode in
    code, which writes no `search_mode`: the saved run decides, not that preference."""
    assert _responsa_back(saved_mode='Title', saved_query='')['restore_saved_results'] is True


def test_responsa_link_with_other_options_than_the_saved_run_searches_again():
    for name in ('variants', 'ja', 'flex_spacing', 'bidirectional'):
        url = {'variants': 1, 'ja': None, 'flex_spacing': 1, 'bidirectional': None}
        url[name] = None if url[name] else 1
        state = _responsa_back(initial_responsa=url)
        assert state['restore_saved_results'] is False, name
        assert state['restore_saved_exclusions'] is False, name
        assert state['mode'] == 'responsa'


def test_responsa_link_with_another_query_searches_again():
    state = _responsa_back(initial_query='מאימתי קורין')

    assert state['restore_saved_results'] is False
    assert state['query'] == 'מאימתי קורין'


def test_responsa_link_when_the_saved_run_was_not_responsa_searches_again():
    state = _responsa_back(saved_run=_responsa_run(mode='literal'))

    assert state['restore_saved_results'] is False


def test_responsa_link_without_a_saved_run_searches_again():
    """A snapshot saved before searches were saved with their run has nothing to
    match the options against: a fresh search, as before."""
    for saved_run in ({}, {'query': _RESPONSA_QUERY, 'mode': 'responsa'},
                      {'query': _RESPONSA_QUERY, 'mode': 'responsa', 'responsa_options': None}):
        assert _responsa_back(saved_run=saved_run)['restore_saved_results'] is False, saved_run


def test_responsa_link_with_no_saved_results_searches_again():
    assert _responsa_back(saved_results_count=0)['restore_saved_results'] is False


def test_other_explicit_modes_in_the_url_still_search_again():
    """Only Responsa stamps a mode; any other mode in the URL came from a link."""
    state = resolve_search_bootstrap(
        initial_query='שלום',
        initial_tag=None,
        initial_mode='variants',
        initial_domain=None,
        from_browse=None,
        saved_mode='variants',
        saved_query='שלום',
        saved_results_count=12,
        use_slider=False,
        saved_run={'query': 'שלום', 'mode': 'variants', 'responsa_options': None},
    )

    assert state['restore_saved_results'] is False
    assert state['mode'] == 'variants'


def _search_page_tree():
    import ast
    from pathlib import Path
    source = (Path(__file__).resolve().parents[1] / 'web' / 'pages' / 'search.py').read_text(encoding='utf-8')
    return ast.parse(source)


def test_search_page_passes_the_stamped_responsa_options_and_the_saved_run():
    """The call site: the four URL flags reach the bootstrap under the names the
    saved run uses, and the saved run is passed. A renamed key would quietly turn
    Back to Responsa into a fresh search again."""
    import ast
    from web.search_bootstrap import RESPONSA_URL_OPTIONS

    calls = [node for node in ast.walk(_search_page_tree())
             if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'resolve_search_bootstrap']
    assert len(calls) == 1, 'expected one bootstrap call in search.py; update this test'
    keywords = {kw.arg: kw.value for kw in calls[0].keywords}
    assert 'saved_run' in keywords
    passed = keywords['initial_responsa']
    assert isinstance(passed, ast.Dict)
    mapping = {key.value: value.id for key, value in zip(passed.keys, passed.values)}
    assert mapping == {
        'variants': 'initial_variants', 'ja': 'initial_ja',
        'flex_spacing': 'initial_flex_spaces', 'bidirectional': 'initial_bidirectional',
    }
    assert set(mapping) == set(RESPONSA_URL_OPTIONS)


def test_the_saved_run_names_every_stamped_responsa_option():
    """The saved run's options come from _responsa_options_from_controls; each
    option the URL carries must be one of its keys, or nothing ever matches."""
    import ast
    from web.search_bootstrap import RESPONSA_URL_OPTIONS

    funcs = [node for node in ast.walk(_search_page_tree())
             if isinstance(node, ast.FunctionDef) and node.name == '_responsa_options_from_controls']
    assert len(funcs) == 1
    returned = [node.value for node in ast.walk(funcs[0])
                if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict)]
    assert len(returned) == 1
    keys = {key.value for key in returned[0].keys}
    assert set(RESPONSA_URL_OPTIONS) <= keys
