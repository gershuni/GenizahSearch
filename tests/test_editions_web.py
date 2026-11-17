# -*- coding: utf-8 -*-
"""The scholarly transcriptions on the website's pages (editions plan section 3.4, PR 3b): the
result card's labels and links, the count line, opening a hit on /browse (the edition chosen,
its match marked on its own characters, a stale hit told), Quick View's default, copy and
exports reading the edition, the stored rows compacted, and the deploy's promote/restore/check.

The real engine on a small main index and an editions generation (tests/test_editions_search.py's
``world``); the pages' helpers are called directly (no browser)."""
import os
import subprocess
import sys
from types import SimpleNamespace

import pytest

pytest.importorskip("tantivy")

from shared.edition_search_text import SOURCE_FGP, SOURCE_PGP, search_text  # noqa: E402
from tests.test_editions_search import DOCS, MS1, MS2, P11, P12, world  # noqa: E402,F401

TEXTS = {f"{d.kind}:{d.edition_id}": d.search.original for d in DOCS}


class _Resolver:
    """The sidecars' texts as EditionResolver.resolve answers (it reads real files in its own
    test); *changed* versions answer a text other than the indexed one."""
    def __init__(self, changed=()):
        self.changed = set(changed)

    def resolve(self, version):
        if version not in TEXTS:
            return None
        kind, eid = version.split(':')
        text = TEXTS[version] + (' added later' if version in self.changed else '')
        st = search_text(text, SOURCE_FGP if kind == 'fgp' else SOURCE_PGP)
        return {'kind': kind, 'id': eid, 'version': version, 'original': text, 'search': st,
                'search_text': st.text, 'credit': {'en': f'Team {eid}', 'he': f'Team {eid}'}}


@pytest.fixture
def web(world, monkeypatch):  # noqa: F811 -- the imported fixture
    """The web process's state over the real engine, editions on, the stand-in resolver, in
    English (this machine's desktop profile may be Hebrew)."""
    from web import editions_assets
    from web.state import state
    from web.translations import using_language
    monkeypatch.setattr(state, 'searcher', world['eng'])
    monkeypatch.setattr(state, 'meta_mgr', world['eng'].meta_mgr)
    monkeypatch.setattr(editions_assets, '_resolver', _Resolver())
    monkeypatch.setenv('EDITIONS_SEARCH_ENABLED', '1')
    with using_language('en'):
        yield world


def _rows(w, query='שלום עליכם', sources='all'):
    return w['eng'].execute_search(query, 'literal', 0, corpus_scope='genizah', sources=sources)


# --- the result card and the count line ----------------------------------------------------------

def test_the_card_names_every_text_that_matched(web):
    from web.pages.search_editions import matched_in, member_label, page_not_identified
    rows = {r['uid']: r for r in _rows(web)}
    line = matched_in(rows[P11])
    assert line.startswith('Matched in: ')
    assert 'Automatic transcription' in line and 'FGP edition (Team 1)' in line and 'PGP edition (Team 10)' in line
    # A page found in its automatic transcription alone says nothing more than today.
    plain = {r['uid']: r for r in _rows(web, 'ואחר כך')}
    assert all(matched_in(r) is None for r in plain.values())
    # A long credit is cut on the card.
    long = {'edition': {'kind': 'fgp', 'credit': {'en': 'x' * 80}}}
    assert len(member_label(long)) < 60 and member_label(long).endswith('…)')
    loose = [r for r in rows.values() if page_not_identified(r)]
    assert loose and loose[0]['uid'].startswith('ED:fgp:4')


def test_an_unlocated_hit_links_no_folio_and_quick_view_shows_its_edition(web):
    """A hit whose page could not be identified: the spreadsheet's Image URL stays empty (its
    blank page number linked the first folio). Quick View shows the edition that matched with
    the page it opens on, as Browse and the desktop's viewer do, and says in its header that
    the page is not identified; Next and Previous browse on as usual."""
    from web.export_service import _image_url_cell
    from web.export_state import _compact_search_result_row
    from web.pages.search_results import _hit_scope_version, _quick_view_result_label
    rows = _rows(web)
    loose = next(r for r in rows if r.get('page_identified') is False)
    placed = next(r for r in rows if r.get('edition') and r.get('page_identified') is not False)
    for row in (loose, _compact_search_result_row(loose)[0]):
        assert _image_url_cell(row, MS2, 'CUL', '') == ''
    assert _image_url_cell(placed, MS1, 'CUL', '2').endswith('?page=1')
    page = SimpleNamespace(sys_id=MS2, p_num=1)
    adv = SimpleNamespace(hit_scope=None, volume_ie=None)
    assert _hit_scope_version(loose, adv, page) == loose['edition']['version'] == 'fgp:4'
    assert _hit_scope_version(loose, adv, SimpleNamespace(sys_id=MS2, p_num=2)) is None   # Next
    assert _hit_scope_version(loose, adv, page) == 'fgp:4'                               # back
    assert (_hit_scope_version(placed, SimpleNamespace(hit_scope=None, volume_ie=None), page)
            == placed['edition']['version'])
    assert _quick_view_result_label(2, 50, loose) == 'Result 3 / 50 · Page not identified'
    assert _quick_view_result_label(2, 50, placed) == 'Result 3 / 50'


def test_a_card_without_a_page_number_still_renders():
    """A card's image index from its page number: 0 when there is none or it is not a number
    (int('Unknown') stopped a whole page of cards rendering at the first such hit)."""
    from web.pages.search_results import _page_index
    assert _page_index('3') == 2 and _page_index('1') == 0 and _page_index(4) == 3
    assert _page_index('') == 0 and _page_index(None) == 0
    assert _page_index('Unknown') == 0 and _page_index('0') == 0


def test_the_count_line_says_how_many_matched_in_an_edition(web):
    from web.pages.search_editions import editions_count_suffix
    rows = _rows(web)
    with_edition = sum(1 for r in rows if r.get('edition_match'))
    state = SimpleNamespace(results=rows, editions_skipped=False)
    assert editions_count_suffix(state) == f' · {with_edition} in scholarly transcriptions'
    state.editions_skipped = True
    assert editions_count_suffix(state).endswith('The scholarly transcriptions could not be searched this time')
    # Today's rows: nothing added.
    plain = web['plain'].execute_search('שלום עליכם', 'literal', 0, corpus_scope='genizah')
    assert editions_count_suffix(SimpleNamespace(results=plain, editions_skipped=False)) == ''


def test_a_hit_links_to_its_edition_on_its_page(web):
    from urllib.parse import parse_qs, urlparse
    from web.pages.search_editions import browse_url
    rows = {r['uid']: r for r in _rows(web)}
    fgp1 = next(m for m in rows[P11]['members'] if m['uid'].startswith('ED:fgp:1'))
    q = parse_qs(urlparse(browse_url(fgp1, web['eng'].meta_mgr)).query)
    assert q['sys_id'] == [MS1] and q['version'] == ['fgp:1'] and q['page'] == ['1']
    a, b = (int(x) for x in q['span'][0].split('-'))
    assert fgp1['full_text'][a:b] == TEXTS['fgp:1'][:TEXTS['fgp:1'].index(' רבותי')]   # pointed, as indexed
    # The verso of the same PGP text: its own page.
    pgp_verso = next(m for m in rows[P12]['members'] if m.get('edition'))
    assert parse_qs(urlparse(browse_url(pgp_verso, web['eng'].meta_mgr)).query)['page'] == ['2']
    # A page that was not identified opens the manuscript: no page, never page zero.
    loose = next(r for r in rows.values() if r.get('page_identified') is False)
    q = parse_qs(urlparse(browse_url(loose, web['eng'].meta_mgr)).query)
    assert q['sys_id'] == [MS2] and 'page' not in q and q['version'] == ['fgp:4']


def test_copy_and_the_inline_text_read_the_edition_not_the_page(web):
    from web.pages.search_editions import edition_text
    rows = {r['uid']: r for r in _rows(web, sources='editions')}
    assert edition_text(rows[P11]) == TEXTS[rows[P11]['edition']['version']]
    automatic = {r['uid']: r for r in _rows(web, sources='automatic')}
    assert edition_text(automatic[P11]) is None


# --- opening a hit on /browse ---------------------------------------------------------------------

def test_the_link_is_parsed_strictly():
    from web.pages.browse_editions import parse_target
    assert parse_target('fgp:1', '3-9') == {'version': 'fgp:1', 'span': (3, 9)}
    assert parse_target('pgp:10', 'junk') == {'version': 'pgp:10', 'span': None}
    assert parse_target('fgp:1', '9-3')['span'] is None
    for bad in (None, '', 'fgp:x', 'abc:1', 'fgp:1;drop'):
        assert parse_target(bad, '1-2') is None


def test_an_unplaced_hit_opens_its_edition_and_says_its_page_is_not_known():
    """A link with no page (a hit whose page could not be identified) still opens the edition
    with its match, and the reader is told, once (Codex review of PR #18); a changed edition
    says that instead."""
    from web.pages.browse_editions import edition_target_notice, parse_target
    unplaced = parse_target('fgp:4', '0-4', placed=False)
    assert unplaced['unplaced'] is True and unplaced['version'] == 'fgp:4'
    assert edition_target_notice(unplaced) is None                 # not resolved yet
    unplaced.update(resolved=True, stale=False)
    assert edition_target_notice(unplaced).startswith('The page of this match could not be identified')
    unplaced['notified'] = True
    assert edition_target_notice(unplaced) is None                 # once
    placed = dict(parse_target('fgp:1', '0-4'), resolved=True, stale=False)
    assert edition_target_notice(placed) is None
    placed['stale'] = True
    assert edition_target_notice(placed).startswith('The scholarly transcription has changed')
    stale_unplaced = dict(parse_target('fgp:4', '0-4', placed=False), resolved=True, stale=True)
    assert edition_target_notice(stale_unplaced).startswith('The scholarly transcription has changed')


def test_the_match_is_marked_on_the_editions_own_characters(web):
    from web.pages.browse_editions import mark_html, resolve_mark
    # The pointed edition: the search text has no points; the mark carries them.
    text = TEXTS['fgp:1']
    matched = text[:text.index(' רבותי')]                     # the pointed "shalom aleikhem"
    got = resolve_mark(MS1, 'fgp:1', (0, len(matched)))
    assert got['stale'] is False
    assert got['mark'] == matched
    html = mark_html(text, got['mark'], got['occurrence'])
    assert html.startswith('<span class="highlight-term">') and got['mark'] in html
    # Which occurrence: the second "שלום" of an edition marks the second one.
    text4 = TEXTS['fgp:4']
    second = search_text(text4, SOURCE_FGP).text.index('שלום שלום') + len('שלום ')
    got = resolve_mark(MS2, 'fgp:4', (second, second + len('שלום')))
    assert got['occurrence'] == 2
    marked = mark_html(text4, got['mark'], got['occurrence'])
    assert marked.count('highlight-term') == 1
    assert marked.endswith('<span class="highlight-term">' + got['mark'] + '</span>')   # the last of three
    assert mark_html('nothing here', 'שלום', 0) is None
    # Across a line break: each line's part is marked on its own line.
    two = mark_html('a b\nc d', 'b\nc', 0)
    assert two == 'a <span class="highlight-term">b</span>\n<span class="highlight-term">c</span> d'


def test_a_changed_or_missing_edition_is_a_stale_hit(web, monkeypatch):
    from web import editions_assets
    from web.pages.browse_editions import resolve_mark
    monkeypatch.setattr(editions_assets, '_resolver', _Resolver(changed={'fgp:1'}))
    assert resolve_mark(MS1, 'fgp:1', (0, 4))['stale'] is True
    assert resolve_mark(MS1, 'fgp:99', (0, 4))['stale'] is True        # gone from the index
    assert resolve_mark(MS2, 'fgp:1', (0, 4))['stale'] is True         # not of this manuscript


def test_a_link_marks_its_span_only_while_it_holds_what_the_search_read(web):
    """The link carries a fingerprint of its matched stretch: after the sidecar and the index are
    rebuilt together the version and the index still agree, so only that fingerprint shows the
    edition changed under the span (Codex review of PR #18). A link without one is read as
    before."""
    from urllib.parse import parse_qs, urlparse
    from shared.editions_search import span_fingerprint
    from web.pages.browse_editions import parse_target, resolve_mark
    from web.pages.search_editions import browse_url
    rows = {r['uid']: r for r in _rows(web)}
    fgp1 = next(m for m in rows[P11]['members'] if m['uid'].startswith('ED:fgp:1'))
    span = tuple(fgp1['edition']['span'])
    assert fgp1['edition']['span_fp'] == span_fingerprint(fgp1['full_text'], span)
    q = parse_qs(urlparse(browse_url(fgp1, web['eng'].meta_mgr)).query)
    assert q['fp'] == [fgp1['edition']['span_fp']]
    target = parse_target(q['version'][0], q['span'][0], fp=q['fp'][0])
    assert target['fp'] == fgp1['edition']['span_fp']
    assert parse_target('fgp:1', '0-4', fp='not-a-fingerprint').get('fp') is None
    # The same text: marked. Another text under the same span: stale, not the old offsets marked.
    assert resolve_mark(MS1, 'fgp:1', span, target['fp'])['mark']
    other = resolve_mark(MS1, 'fgp:1', span, 'f' * 12)
    assert other == {'stale': True, 'mark': None, 'occurrence': 0}
    # A span the edition is now too short for, with a fingerprint: stale too.
    assert resolve_mark(MS1, 'fgp:1', (10 ** 6, 10 ** 6 + 4), 'f' * 12)['stale'] is True
    # An old link (no fingerprint): as before.
    assert resolve_mark(MS1, 'fgp:1', span)['mark']


def test_the_linked_edition_survives_the_page_filter_and_is_the_default():
    from web.components.version_selector import decision_for_version
    from web.pages.browse_editions import keep_target, source_version
    fgp = {'source': 'fgp', 'is_fgp': True, 'id': 1, 'uid': 'fgp:1', 'content': 'x', 'doc_relation': 'Digital Edition'}
    pgp = {'id': 10, 'pgpid': 3, 'source_scholar': 'Gil', 'content': 'y', 'doc_relation': 'Digital Edition'}
    assert source_version(fgp) == 'fgp:1' and source_version(pgp) == 'pgp:10'
    # The page filter left the edition out: it is the text the search matched, so it stays.
    assert keep_target([fgp, pgp], [pgp], 'fgp:1') == [pgp, fgp]
    assert keep_target([fgp, pgp], [pgp], None) == [pgp]
    assert keep_target([fgp, pgp], [fgp, pgp], 'fgp:1') == [fgp, pgp]
    # The chooser opens on it, ahead of PGP-first.
    decision = decision_for_version([pgp, fgp], 'fgp:1')
    assert decision['source'] is fgp and decision['provider'] == 'fgp' and decision['eligible']
    assert decision_for_version([pgp, fgp], 'pgp:10')['provider'] == 'pgp'
    assert decision_for_version([pgp, fgp], 'fgp:2') is None and decision_for_version([pgp], None) is None


def test_only_the_folio_the_link_arrived_for_opens_on_the_edition(web):
    from web.pages.browse_editions import (claim_target, on_target_folio, parse_target, resolve_target,
                                           target_mark_for, target_version_for)
    state = SimpleNamespace(edition_target=parse_target('fgp:1', '0-4'), volume_ie=None)
    page1 = SimpleNamespace(sys_id=MS1, volume_ie='IE11', p_num=1)
    page2 = SimpleNamespace(sys_id=MS1, volume_ie='IE11', p_num=2)
    assert on_target_folio(state, page2) is True                    # unclaimed: nothing decided yet
    # A worker never claims it: unclaimed, resolving does nothing.
    resolve_target(state, page2)
    assert state.edition_target.get('resolved') is None and state.edition_target.get('scope') is None
    # The arrival folio claims it on the loop when its enrichment starts; a folio reached while it
    # is still loading cannot, and its worker resolves nothing (Codex review of #18).
    assert claim_target(state, page1) is True
    assert claim_target(state, page2) is False
    resolve_target(state, page2)
    assert state.edition_target.get('resolved') is None
    resolve_target(state, page1)
    # The address keeps version/span on page 1 only: a reload on page 2 must not claim the edition
    # (Codex review of PR #18).
    assert on_target_folio(state, page1) is True and on_target_folio(state, page2) is False
    assert target_version_for(state, page1) == 'fgp:1'
    assert target_version_for(state, page2) is None
    assert target_mark_for(state, page1, {'source': 'fgp', 'uid': 'fgp:1'})['mark']
    assert target_mark_for(state, page1, {'source': 'fgp', 'uid': 'fgp:2'}) is None
    assert target_mark_for(state, page1, {'source': 'original'}) is None


# --- stored rows and exports ------------------------------------------------------------------------

def test_stored_rows_drop_the_members_texts(web):
    from web.pages.search_state import compact_result_rows
    rows = compact_result_rows(_rows(web))
    for r in rows:
        for m in r.get('members') or []:
            assert 'full_text' not in m
    assert any(m.get('full_text_excerpt') for r in rows for m in r.get('members') or [])


def test_exports_keep_what_matched_and_read_the_edition(web):
    from web.export_service import _resolve_result_full_text, _sources_cell
    from web.export_state import _compact_search_result_row
    row = next(r for r in _rows(web) if r['uid'] == P11)
    kept, _changed = _compact_search_result_row(row)
    assert kept['sources'] == row['sources']
    assert {m['uid'] for m in kept['members']} == {m['uid'] for m in row['members']}
    assert all(set(m) == {'uid', 'raw_header', 'snippet', 'edition'} for m in kept['members'])
    cell = _sources_cell(kept)
    # An edition hit with no page named keeps saying so in the export (Codex review of #18).
    loose = next(r for r in _rows(web) if r.get('page_identified') is False)
    assert _compact_search_result_row(loose)[0]['page_identified'] is False
    assert 'page_identified' not in kept
    assert 'Automatic transcription' in cell and 'FGP edition (Team 1)' in cell
    assert _sources_cell(kept, 'he').startswith('תעתיק')
    led = next(r for r in _rows(web, sources='editions') if r['uid'] == P11)
    kept, _ = _compact_search_result_row(led)
    assert _resolve_result_full_text(kept) == TEXTS[led['edition']['version']]
    # Today's rows: no new keys, no Source change.
    plain, _ = _compact_search_result_row(web['plain'].execute_search('שלום', 'literal', 0, corpus_scope='genizah')[0])
    assert not {'sources', 'members', 'edition'} & set(plain) and _sources_cell(plain) is None


def test_an_export_after_the_edition_changed_does_not_carry_its_new_text(web, monkeypatch):
    """A stored row keeps its match's span and fingerprint; exported after the sidecar was
    rebuilt with other text under the same version, it exports none of that text -- the
    spreadsheet's Full Text and the JSON excerpt stay empty, a live row falls back to the text
    it was found in (Codex review of PR #18). The JSON still writes the public fields alone."""
    from shared.search_serializer import serialize_search_payload
    from web import editions_assets
    from web.export_service import _resolve_result_full_text, with_excerpts
    from web.export_state import _compact_search_result_row
    led = next(r for r in _rows(web, sources='editions') if r['uid'] == P11)
    kept, _ = _compact_search_result_row(led)
    assert kept['edition']['span'] == led['edition']['span']
    assert kept['edition']['span_fp'] == led['edition']['span_fp']
    assert _resolve_result_full_text(kept) == TEXTS[led['edition']['version']]   # unchanged: read

    class _Revised(_Resolver):
        def resolve(self, version):
            got = super().resolve(version)
            if got is None:
                return None
            text = 'נוסף בראש. ' + got['original']                 # the same version, other text
            st = search_text(text, SOURCE_FGP if got['kind'] == 'fgp' else SOURCE_PGP)
            return dict(got, original=text, search=st, search_text=st.text)

    monkeypatch.setattr(editions_assets, '_resolver', _Revised())
    assert _resolve_result_full_text(kept) == ''
    assert 'full_text_excerpt' not in with_excerpts([kept])[0]
    assert _resolve_result_full_text(led) == led['full_text']          # the text it was found in
    out = serialize_search_payload([kept], meta_mgr=web['eng'].meta_mgr, query='q', mode='text')
    assert not {'span', 'span_fp'} & set(out['results'][0]['edition'])


def test_quick_view_and_copy_never_read_a_changed_edition(web, monkeypatch):
    """Quick View's text and Copy read an edition again only while it is the text the hit was
    found in; changed under the match (a restored search after a rebuild), they show the text
    the row kept, never the new one (Codex review of PR #18)."""
    from web import editions_assets
    from web.pages.search_editions import edition_text
    led = next(r for r in _rows(web, sources='editions') if r['uid'] == P11)
    version = led['edition']['version']
    assert edition_text(led) == TEXTS[version]                           # unchanged: the sidecar's

    class _Revised(_Resolver):
        def resolve(self, version):
            got = super().resolve(version)
            if got is None:
                return None
            text = 'נוסף בראש. ' + got['original']
            st = search_text(text, SOURCE_FGP if got['kind'] == 'fgp' else SOURCE_PGP)
            return dict(got, original=text, search=st, search_text=st.text)

    monkeypatch.setattr(editions_assets, '_resolver', _Revised())
    assert edition_text(led) == led['full_text']                         # the text it was found in
    restored = dict(led, full_text_excerpt=led['full_text'][:20])
    restored.pop('full_text')
    assert edition_text(restored) == led['full_text'][:20]
    assert 'נוסף בראש' not in (edition_text(led) or '')


def test_an_edition_link_is_read_only_for_a_reader_it_is_served_to(monkeypatch):
    """With EDITIONS_SEARCH_ENABLED=editors, an editor's /browse link opened by another reader
    opens the manuscript as usual: no edition chosen or marked from the editions index (Codex
    review of PR #18)."""
    from web import editions_assets
    from web.auth_state import GlobalAuthState
    from web.pages.browse_editions import link_target
    monkeypatch.setattr(GlobalAuthState, 'is_editor', classmethod(lambda cls: False))
    seen = []
    monkeypatch.setattr(editions_assets, 'served_to', lambda is_editor: seen.append(is_editor) or False)
    assert link_target('fgp:1', '0-4', fp='a' * 12) is None
    monkeypatch.setattr(editions_assets, 'served_to', lambda is_editor: True)
    assert link_target('fgp:1', '0-4', placed=False, fp='a' * 12) == {
        'version': 'fgp:1', 'span': (0, 4), 'unplaced': True, 'fp': 'a' * 12}
    assert seen == [False]


def test_the_json_export_excerpt_is_the_editions_text(web):
    """A stored edition-led row has no text; the JSON export's excerpt is read from the edition
    again, not left empty (Codex review of PR #18). A row that kept its text is left as is."""
    from shared.search_serializer import serialize_search_payload
    from web.export_service import with_excerpts
    from web.export_state import _compact_search_result_row
    led = next(r for r in _rows(web, sources='editions') if r['uid'] == P11)
    kept, _ = _compact_search_result_row(led)
    assert 'full_text' not in kept and 'full_text_excerpt' not in kept
    plain = {'uid': 'u', 'snippet': 'x', 'full_text': 'kept'}
    out = with_excerpts([kept, plain])
    assert out[0]['full_text_excerpt'] == TEXTS[led['edition']['version']][:500]
    assert out[1] is plain and 'full_text_excerpt' not in kept          # the stored row untouched
    payload = serialize_search_payload(out[:1], meta_mgr=web['eng'].meta_mgr, query='q', mode='text')
    assert payload['results'][0]['excerpt'] == TEXTS[led['edition']['version']][:500]


# --- the deploy: promote, restore, check -------------------------------------------------------------

def _build_script(*args, env):
    return subprocess.run([sys.executable, os.path.join('scripts', 'build_editions_index.py'), *args],
                          cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          capture_output=True, text=True, env=env)


def test_promote_restore_and_check_live(web, tmp_path):
    import shutil
    live = tmp_path / 'Editions'
    env = dict(os.environ, GENIZAH_EDITIONS_DIR=str(live))
    assert _build_script('--check-live', env=env).returncode == 1          # nothing there yet
    shutil.copytree(web['gen'], tmp_path / 'one')
    r = _build_script('--promote-only', str(tmp_path / 'one'), env=env)
    assert r.returncode == 0, r.stdout + r.stderr
    assert live.is_dir() and not (tmp_path / 'one').exists()
    assert _build_script('--check-live', env=env).returncode == 0
    # A second generation, then a rollback: the first is back, the second kept as .failed.
    shutil.copytree(web['gen'], tmp_path / 'two')
    (tmp_path / 'two' / 'marker').write_text('second', encoding='utf-8')
    assert _build_script('--promote-only', str(tmp_path / 'two'), env=env).returncode == 0
    assert (live / 'marker').exists() and (tmp_path / 'Editions.prev').is_dir()
    assert _build_script('--restore-previous', env=env).returncode == 0
    assert not (live / 'marker').exists() and (tmp_path / 'Editions.failed' / 'marker').exists()
    assert _build_script('--restore-previous', env=env).returncode == 1    # no .prev left
    # A first install has no .prev: its rollback sets the new generation aside, and the site
    # serves without editions (Codex review of PR #18).
    assert _build_script('--set-aside-live', env=env).returncode == 0
    assert not live.exists() and (tmp_path / 'Editions.failed').is_dir()
    assert _build_script('--check-live', env=env).returncode == 1
    assert _build_script('--set-aside-live', env=env).returncode == 1     # nothing left to set aside
    # An incoherent generation is never promoted.
    shutil.copytree(web['gen'], tmp_path / 'bad')
    os.remove(tmp_path / 'bad' / 'editions_meta.json')
    assert _build_script('--promote-only', str(tmp_path / 'bad'), env=env).returncode == 1
    assert (tmp_path / 'bad').is_dir()


def test_drift_is_found_when_an_input_changed(web, monkeypatch):
    import json
    from web import editions_assets
    from shared import editions_indexer
    monkeypatch.setattr(editions_assets, '_resolver', SimpleNamespace(_fgp_db=None, _pgp_db=None))
    with open(os.path.join(web['gen'], 'editions_meta.json'), encoding='utf-8') as f:
        recorded = json.load(f)['inputs']
    monkeypatch.setattr(editions_indexer, 'input_stamp',
                        lambda *a: dict(recorded, pgp=dict(recorded['pgp'], file={'sha256': 'other'})))
    editions_assets._check_drift(web['eng'])
    assert editions_assets.editions_drift() == ['the PGP sidecar']
    monkeypatch.setattr(editions_indexer, 'input_stamp', lambda *a: dict(recorded))
    editions_assets._check_drift(web['eng'])
    assert editions_assets.editions_drift() == []


def test_quick_view_opens_the_edition_on_the_hits_page_only():
    """Quick View's chooser and its Browse links take the edition leading a result on the
    page the hit belongs to; Next/Previous pages browse as usual (Codex review of #18)."""
    from web.pages.search_results import _hit_scope_version
    result = {'edition': {'version': 'fgp:1'}}
    adv = SimpleNamespace(hit_scope=None, volume_ie='IE11')
    page1 = SimpleNamespace(sys_id=MS1, p_num=1)
    page2 = SimpleNamespace(sys_id=MS1, p_num=2)
    assert _hit_scope_version(result, adv, page1) == 'fgp:1'
    assert _hit_scope_version(result, adv, page2) is None
    assert _hit_scope_version(result, adv, page1) == 'fgp:1'
    assert _hit_scope_version({'edition': None}, SimpleNamespace(hit_scope=None, volume_ie=None), page1) is None


def test_quick_view_shows_an_unplaced_hits_edition_on_the_page_it_opens_on():
    """The chain Quick View runs (_render_advanced_view) for a hit whose page could not be
    identified -- a short excerpt of a long manuscript, say: the hit claims the page Quick View
    opens on, the page filter left the excerpt out there and it is kept, and the text shown is
    the excerpt, not that page's automatic text (often empty on such a page)."""
    from web.components.version_selector import decision_for_version
    from web.pages.browse_editions import keep_target
    from web.pages.search_results import _hit_scope_version
    excerpt = {'source': 'fgp', 'is_fgp': True, 'id': 4, 'uid': 'fgp:4', 'content': 'x',
               'doc_relation': 'Digital Edition'}
    result = {'edition': {'version': 'fgp:4'}, 'page_identified': False}
    version = _hit_scope_version(result, SimpleNamespace(hit_scope=None, volume_ie=None),
                                 SimpleNamespace(sys_id=MS2, p_num=1))
    assert version == 'fgp:4'
    sources = keep_target([excerpt], [], version)
    decision = decision_for_version(sources, version)
    assert decision['source'] is excerpt and decision['eligible']


def test_browses_search_for_parallels_sends_the_version_on_screen():
    """Browse's Search for Parallels reads the page as it is on screen, from the pair
    handle_version_change writes (the Word export's pair): an edition chosen, else the
    automatic transcription; a translation sends the page's own text."""
    from web.pages.browse import _parallels_page_text
    state = SimpleNamespace(current_page=SimpleNamespace(text='the automatic transcription'))
    assert _parallels_page_text(state, {}) == 'the automatic transcription'
    chosen = {'current_version_info': {'source': 'fgp', 'is_fgp': True}, 'current_version_text': 'the edition'}
    assert _parallels_page_text(state, chosen) == 'the edition'
    translated = {'current_version_info': {'source': 'translation'}, 'current_version_text': 'English'}
    assert _parallels_page_text(state, translated) == 'the automatic transcription'
    # A text with no info beside it was never chosen: not trusted.
    assert _parallels_page_text(state, {'current_version_text': 'stale'}) == 'the automatic transcription'
    # Editing: the editor's text, not the version chosen before (Codex review of #33).
    editing = SimpleNamespace(current_page=state.current_page, edit_mode=True, edit_text='my draft')
    assert _parallels_page_text(editing, chosen) == 'my draft'
    assert _parallels_page_text(SimpleNamespace(current_page=state.current_page, edit_mode=True, edit_text=''),
                                chosen) == 'the automatic transcription'


def test_a_restored_search_drops_a_choice_the_reader_is_no_longer_served():
    """An editor's preview restored after they lost it: its last run and its search-within
    steps replay without the editions (Codex review of #18)."""
    from shared.refinement import RefinementStep
    from web.pages.search_editions import drop_source_choice
    state = SimpleNamespace(last_run={'query': 'x', 'sources': 'editions'},
                            refinement_chain=[RefinementStep(query='x', mode='literal', sources='all'),
                                              RefinementStep(query='y', mode='literal')])
    drop_source_choice(state)
    assert 'sources' not in state.last_run
    assert [s.sources for s in state.refinement_chain] == [None, None]
