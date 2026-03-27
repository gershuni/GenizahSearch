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
        use_slider=False,
    )

    assert state == {
        'mode': 'exact',
        'query': '',
        'restore_saved_results': False,
        'restore_saved_filters': False,
        'restore_saved_exclusions': False,
    }
