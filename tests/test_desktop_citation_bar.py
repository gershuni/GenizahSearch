# -*- coding: utf-8 -*-
"""The desktop citation bar must credit whoever actually wrote the text.

THE DEFECT THIS FIXES, so it is not re-introduced. Until 2026-09-04 the bar at
the bottom of every desktop tab showed ONE hardcoded MiDRASH/Zenodo citation
with no awareness of the version combo, and "Copy Citation" handed that to the
reader. `_auto_select_pgp_edition` is PGP-FIRST unconditionally, so a reader on
the Browse tab is usually looking at a Princeton or Friedberg edition -- meaning
the wrong-attribution case was the DEFAULT path, not an edge case. It is the
same defect the web printed sheet and citation footer had, and it is fixed from
the same shared decision (`shared/transcription_credits`) so the two apps cannot
drift on who is owed what.

HOW THESE TESTS RUN WITHOUT Qt
------------------------------
`_credit_version_info` is a method on the main window class, and instantiating
that needs a QApplication -- which would put these tests in the `gui` lane that
CI excludes (`-m "not gui and not render_smoke and not atlas_bake"`), i.e. the
guard on a licensing defect would never run in CI.

So the method is lifted out of `genizah_app.py` by AST and executed standalone
against a stub `self`. That is not a source-text assertion dressed up: the real
function body runs, and its output is fed to the REAL shared module, so what is
asserted is the citation a reader would actually get. The only thing stubbed is
the one attribute it touches (`_browse_pgp_url`).
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from shared.transcription_credits import (
    KIND_FGP,
    KIND_HTR,
    KIND_PENDING,
    KIND_PGP,
    KIND_TRANSLATION,
    KIND_USER,
    page_citation,
    site_citation,
)

REPO = Path(__file__).resolve().parents[1]
APP = REPO / 'genizah_app.py'


# ---------------------------------------------------------------------------
# Lifting the real function out of a 25,000-line Qt module
# ---------------------------------------------------------------------------

def _code_only(path: Path) -> str:
    """Source with comments and docstrings blanked.

    Source assertions in this file have twice been satisfied by the very prose
    explaining the rule they check.
    """
    import io as _io
    import tokenize as _tok

    src = path.read_text(encoding='utf-8')
    drop = set()
    prev = None
    for tok in _tok.generate_tokens(_io.StringIO(src).readline):
        if tok.type == _tok.COMMENT:
            drop.update(range(tok.start[0], tok.end[0] + 1))
        elif tok.type == _tok.STRING and (
                prev is None or prev.type in (_tok.INDENT, _tok.NEWLINE,
                                              _tok.NL, _tok.DEDENT)):
            drop.update(range(tok.start[0], tok.end[0] + 1))
        if tok.type not in (_tok.NL, _tok.COMMENT):
            prev = tok
    return '\n'.join('' if i + 1 in drop else ln
                     for i, ln in enumerate(src.splitlines()))


def _load_method(name: str, **attrs):
    """Return `genizah_app`'s method `name` as a plain callable, plus a stub self.

    Compiled from the file's own AST, so the body under test is the shipped one
    -- if it is edited, this runs the edit.
    """
    tree = ast.parse(APP.read_text(encoding='utf-8'))
    found = [n for n in ast.walk(tree)
             if isinstance(n, ast.FunctionDef) and n.name == name]
    assert len(found) == 1, (
        'expected exactly one def %s in genizah_app.py, found %d'
        % (name, len(found)))

    fn = found[0]
    fn.decorator_list = []          # @staticmethod etc. are meaningless here
    module = ast.Module(body=[fn], type_ignores=[])
    ast.fix_missing_locations(module)
    namespace: dict = {}
    exec(compile(module, str(APP), 'exec'), namespace)      # noqa: S102

    class _Stub:
        pass

    stub = _Stub()
    for key, value in attrs.items():
        setattr(stub, key, value)
    return namespace[name], stub


@pytest.fixture(scope='module')
def adapter():
    """`_credit_version_info` bound to a stub with a PGP url available."""
    fn, stub = _load_method('_credit_version_info',
                            _browse_pgp_url='https://geniza.princeton.edu/x')

    def call(version_data):
        return fn(stub, version_data)

    return call


# ---------------------------------------------------------------------------
# 1. THE DEFECT ITSELF
# ---------------------------------------------------------------------------

def test_a_princeton_edition_is_not_credited_to_midrash(adapter):
    """THE BUG, stated as a test. This is the default Browse view.

    `_auto_select_pgp_edition` prefers a PGP edition unconditionally, so this is
    what a reader normally has on screen -- and the bar used to hand them the
    MiDRASH citation for it.
    """
    info = adapter({'source': 'pgp_edition', 'scholar': 'Goitein, S. D.',
                    'content': '...'})
    citation = page_citation(info, lang='en', library='Cambridge University Library',
                             shelfmark='T-S Ar.50.74', folio='1r',
                             retrieved_on='2026-09-04')
    assert citation.kind == KIND_PGP
    assert citation.credits_midrash is False, (
        'a Princeton edition is being credited to MiDRASH -- the exact defect '
        'this bar was fixed to remove')
    assert 'MiDRASH' not in citation.text
    assert 'Goitein, S. D.' in citation.text
    assert 'T-S Ar.50.74' in citation.text


def test_the_automatic_transcription_still_credits_midrash(adapter):
    """The CONTROL for the test above.

    Without it, "no MiDRASH in the citation" would be satisfied by a bar that
    credits MiDRASH for nothing at all -- which is a licence breach in the other
    direction, and the easier mistake to make while fixing the first.
    """
    for version_data in (None, {}, {'source': 'original'}):
        info = adapter(version_data)
        assert info is None, version_data
        citation = page_citation(info, lang='en', shelfmark='T-S 12.123',
                                 retrieved_on='2026-09-04')
        assert citation.kind == KIND_HTR
        assert citation.credits_midrash is True
        assert 'MiDRASH' in citation.text


def test_the_hardcoded_citation_is_gone_from_the_app_module():
    """The bar carried the full author list as a literal -- TWICE.

    The visible label read `SettingsDialog.FULL_CITATION` and `copy_citation`
    re-typed the same 346 characters inline; they were byte-identical, so
    changing one would have silently made the bar show something other than what
    it copied. Both are replaced by the shared module.

    `desktop/settings_dialogs.py` keeps `FULL_CITATION` deliberately: the About
    tab is genuinely about MiDRASH, and citing it there is correct.

    SCOPED TO THE BAR. `_get_credit_header` -- the credit on desktop's DOCX/TXT
    exports -- still holds its own hardcoded MiDRASH text, and that copy has
    already drifted from the canonical `shared/export_utils.MIDRASH_CREDIT_LINES`
    it mirrors ("Dataset available at:" for "Dataset:", and the full author line
    dropped). That is a real, separate gap on a surface this change did not
    touch, and it is named here so a later reader does not mistake it for
    something this test failed to notice.
    """
    src = APP.read_text(encoding='utf-8')
    bar = src[src.index('def _create_citation_bar'):
              src.index('def _show_citation_reminder')]
    assert 'Stoekl Ben Ezra' not in bar, (
        'the MiDRASH author list is hardcoded in the citation bar again; it '
        'belongs to shared/export_utils, which the shared credit module reads')
    assert 'FULL_CITATION' not in bar, (
        'the bar reads the About tab\'s static citation again, which is not '
        'aware of what is on screen')
    # And the byte-identical SECOND copy that `copy_citation` re-typed is gone.
    assert 'def copy_citation(self)' not in src, (
        'the old source-blind copy_citation is back')


# ---------------------------------------------------------------------------
# 2. Every source the combo can emit, mapped
# ---------------------------------------------------------------------------

CASES = [
    ({'source': 'pgp_edition', 'scholar': 'Goitein, S. D.'}, KIND_PGP, False),
    ({'source': 'fgp_edition', 'source_credit': 'FGP team'}, KIND_FGP, False),
    ({'source': 'pgp_translation', 'scholar': 'Goitein, S. D.',
      'language': 'English'}, KIND_TRANSLATION, False),
    ({'source': 'fgp_translation', 'source_credit': 'FGP team',
      'language': 'Hebrew'}, KIND_TRANSLATION, False),
    # A community-submitted VERSION -- a whole alternative transcription by a
    # named contributor, which is a DIFFERENT combo source from a correction and
    # was missed by the first cut of the adapter (found by the completeness test
    # below). It fell through to the MiDRASH default: the same defect, on a path
    # nobody had looked at.
    ({'source': 'user', 'version_id': 7, 'user_name': 'Ada'}, KIND_USER, True),
    ({'source': 'correction', 'status': 'approved', 'user_name': 'Ada'},
     KIND_USER, True),
    ({'source': 'correction', 'status': 'pending', 'user_name': 'Ada'},
     KIND_PENDING, True),
    ({'source': 'correction', 'status': 'draft', 'user_name': 'Ada'},
     KIND_PENDING, True),
    ({'source': 'correction', 'status': 'rejected', 'user_name': 'Ada'},
     KIND_PENDING, True),
    ({'source': 'header'}, KIND_HTR, True),
    ({'source': 'original'}, KIND_HTR, True),
]


@pytest.mark.parametrize('version_data,kind,midrash', CASES)
@pytest.mark.parametrize('lang', ['en', 'he'])
def test_each_combo_source_gets_the_right_credit(adapter, version_data, kind,
                                                 midrash, lang):
    citation = page_citation(adapter(version_data), lang=lang,
                            shelfmark='T-S 12.123', retrieved_on='2026-09-04')
    assert citation.kind == kind, version_data
    assert citation.credits_midrash is midrash, version_data


def test_an_unapproved_correction_does_not_read_as_an_accepted_one(adapter):
    """`draft`, `pending` and `rejected` are all unapproved.

    Only `approved` has been accepted by anyone. Collapsing the four into one
    "community correction" heading would print an unreviewed edit as a settled
    reading -- the same class of false claim as mis-crediting a transcriber.
    """
    approved = page_citation(
        adapter({'source': 'correction', 'status': 'approved',
                 'user_name': 'Ada'}), lang='en', retrieved_on='2026-09-04')
    pending = page_citation(
        adapter({'source': 'correction', 'status': 'pending',
                 'user_name': 'Ada'}), lang='en', retrieved_on='2026-09-04')
    assert approved.text != pending.text
    assert 'Ada' in approved.text
    # Both are edits OF the automatic transcription, so both still owe MiDRASH.
    assert approved.credits_midrash and pending.credits_midrash


def test_the_adapter_covers_every_source_the_desktop_actually_emits(adapter):
    """COMPLETENESS, derived from the app rather than from this file's memory.

    Collects every `{'source': <literal>}` the desktop builds into combo item
    data and checks the adapter classifies each one deliberately. A seventh
    source added to `_populate_pgp_combo` later fails HERE rather than silently
    falling through to the MiDRASH default -- which is the failure mode that
    would quietly re-create the original defect for the new provider.
    """
    # Companion keys that only a VERSION item carries. Used to recognise a
    # combo item by its SHAPE, not by how it happens to reach the widget.
    VERSION_KEYS = {'version_id', 'source_id', 'content', 'corrected_text',
                    'correction_id', 'user_name', 'is_default', 'scholar',
                    'source_credit'}

    def _source_of(dict_node):
        source = None
        keys = set()
        for key, value in zip(dict_node.keys, dict_node.values):
            if not isinstance(key, ast.Constant):
                continue
            keys.add(key.value)
            if (key.value == 'source' and isinstance(value, ast.Constant)
                    and isinstance(value.value, str)):
                source = value.value
        return source, keys

    tree = ast.parse(APP.read_text(encoding='utf-8'))

    # The adapter's OWN output dicts are excluded: it PRODUCES the shared
    # module's vocabulary ({'source': 'fgp', 'source_credit': ...}) rather than
    # consuming desktop's, and those carry version companion keys too, so the
    # shape test above claims them as unmapped desktop sources otherwise.
    adapter_defs = [n for n in ast.walk(tree)
                    if isinstance(n, ast.FunctionDef)
                    and n.name == '_credit_version_info']
    assert len(adapter_defs) == 1
    produced = {id(n) for n in ast.walk(adapter_defs[0])}

    emitted = set()
    for node in ast.walk(tree):
        if id(node) in produced:
            continue
        # TWO ways in, deliberately, because each alone has a blind spot.
        #
        # (a) the dict handed straight to `addItem(label, {...})` -- how every
        #     combo item is written today; and
        # (b) any dict literal carrying `source` PLUS a version companion key,
        #     which still catches an item built into a variable first.
        #
        # (b) is what makes the check hold under refactoring, and it was added
        # after mutation testing: a NEW provider written as
        # `_new = {...}; combo.addItem(label, _new)` slips past (a) entirely and
        # would fall through to the MiDRASH default in silence. With (b) that
        # mutation goes red. (Moving an ALREADY-mapped source into a variable
        # stays green, correctly -- it is still mapped.)
        #
        # Walking every dict with a `source` key was the first cut and was too
        # wide the other way -- it swept in result-row `display` dicts, whose
        # unrelated `source` field carries '' and 'LOCAL'. Those have none of
        # VERSION_KEYS, so the shape test excludes them.
        if (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == 'addItem'
                and len(node.args) == 2
                and isinstance(node.args[1], ast.Dict)):
            source, _ = _source_of(node.args[1])
            if source is not None:
                emitted.add(source)
            continue
        if isinstance(node, ast.Dict):
            source, keys = _source_of(node)
            if source is not None and (keys & VERSION_KEYS):
                emitted.add(source)

    assert emitted, (
        'no version-combo item dicts were found at all, so this test is '
        'asserting nothing -- the addItem shape it looks for has changed')

    # The vocabulary the adapter is written against. Anything else is new.
    # 'V0.7' and 'V0.8' are here because they ARE combo items; the adapter sends
    # them to the HTR credit by asking the shared module's HTR_SOURCES.
    known = {'pgp_edition', 'fgp_edition', 'pgp_translation', 'fgp_translation',
             'correction', 'user', 'header', 'original', 'V0.7', 'V0.8'}
    unexpected = emitted - known
    assert not unexpected, (
        'genizah_app.py builds version dicts with source(s) %r that '
        '_credit_version_info does not classify; they would fall through to the '
        'MiDRASH default and mis-credit that provider' % sorted(unexpected))

    # And each consumed literal really is handled -- not merely listed above.
    for source in ('pgp_edition', 'fgp_edition', 'pgp_translation',
                   'fgp_translation', 'correction', 'user'):
        info = adapter({'source': source, 'status': 'approved'})
        assert info is not None, (
            '%r falls through to the HTR/MiDRASH default' % source)


def test_no_metadata_manager_yields_no_page_citation_rather_than_a_crash():
    """`self.meta_mgr` starts as None and is only assigned conditionally.

    `_browse_page_citation` reads `meta_mgr.nli_cache`, and it runs inside
    `menu.aboutToShow` -- so on a start-up where metadata never loaded, an
    unguarded read would take down the whole menu, including the site citation,
    which is always available. Withholding the page citation is the right
    failure; showing a wrong one is not.
    """
    fn, stub = _load_method('_browse_page_citation')
    stub.current_browse_sid = '99001'
    stub.meta_mgr = None
    assert fn(stub, retrieved_on='2026-09-05') is None

    # And the ordinary "nothing open" case, which is the common one.
    fn2, stub2 = _load_method('_browse_page_citation')
    stub2.current_browse_sid = None
    stub2.meta_mgr = object()
    assert fn2(stub2, retrieved_on='2026-09-05') is None


# ---------------------------------------------------------------------------
# 3. The strip, and what gets copied
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('lang', ['en', 'he'])
def test_the_site_citation_names_this_site_as_well_as_midrash(lang):
    """The old bar credited MiDRASH and never mentioned Dicta or the address.

    RENAMED from `test_the_strip_names_...`: it calls `site_citation` directly,
    so it tests the shared formatter and says nothing about what the strip
    displays. Replacing the strip's text with a literal left it green (Codex
    review, confirmed by mutation). The strip's own wiring is asserted in
    `test_the_strip_is_built_from_the_site_citation_with_no_date` below.
    """
    text = site_citation(lang=lang).text
    assert 'genizahsearch.com' in text
    assert 'MiDRASH' in text
    assert ('Dicta Genizah Search' in text if lang == 'en'
            else 'דיקטה' in text)


def test_the_site_citation_omits_a_date_unless_one_is_passed():
    """RENAMED, for the same reason as the test above: this is the formatter.

    `site_citation` must not invent a date -- the module never reads the clock,
    the surface decides. What the STRIP does with that is asserted below.
    """
    on_screen = site_citation(lang='en').text
    copied = site_citation(lang='en', retrieved_on='2026-09-04').text
    assert 'Sept. 4, 2026' in copied
    assert '2026' not in on_screen.replace('2025', ''), (
        'site_citation invented a date nobody passed it')


def test_the_strip_is_built_from_the_site_citation_with_no_date():
    """THE TWO CLAIMS THE RENAMED TESTS ABOVE DO NOT MAKE.

    Both of those call `site_citation` directly, so a strip showing a literal
    string, or a strip stamped with a start-up date, left them green -- found by
    the Codex review and confirmed by running both mutations.

    Why the date matters: the bar is built ONCE, at start-up, and the app may
    stay open for days. A date painted then is quietly wrong by the next
    morning, and it is the reason the strip can be built once and never
    repainted. The copied string is stamped at copy time instead.
    """
    src = APP.read_text(encoding='utf-8')
    bar = src[src.index('def _create_citation_bar'):
              src.index('def _copy_citation_text')]
    line = [ln for ln in bar.splitlines()
            if 'cit_lbl = ' in ln and not ln.lstrip().startswith('#')]
    assert len(line) == 1, 'expected one strip-label assignment, got %r' % line
    assignment = line[0]
    assert 'self._site_citation_text()' in assignment, (
        'the strip is not built from the site citation: %r' % assignment)
    for stamp in ('retrieved_on', '_citation_stamp'):
        assert stamp not in assignment, (
            'the strip is stamped with a date at build time, which is stale by '
            'the next morning: %r' % assignment)


def test_the_strip_elides_visibly_and_never_hands_out_the_cut_string():
    """CI-visible half of the ElidingLabel guard.

    `tests/test_eliding_label.py` exercises the widget itself, but it is
    `gui`-marked and CI runs `-m "not gui ..."`. These two properties need no Qt
    and are the ones that go wrong at the CALL site: using a plain QLabel again
    (which clips in silence), or copying `text()` -- the elided form -- instead
    of `full_text`.

    With the owner's full 17-author citation the strip is ~428 characters on one
    fixed 30px line, so it cannot all fit at any ordinary window width; the only
    question is whether the reader can SEE that.
    """
    src = APP.read_text(encoding='utf-8')
    bar = src[src.index('def _create_citation_bar'):
              src.index('def _copy_citation_text')]
    assert 'ElidingLabel(' in bar, (
        'the citation strip is a plain QLabel again, which does not elide -- it '
        'stops painting, so a cut citation looks like a short one')
    assert 'cit_lbl.setToolTip(cit_lbl.full_text)' in bar, (
        'the tooltip shows the ELIDED text, so the whole citation is nowhere '
        'reachable by reading')
    assert 'TextSelectableByMouse' not in bar, (
        'the strip is selectable again: `text()` is the elided form, so a mouse '
        'selection yields a citation that is cut but looks whole')


def test_the_bar_cites_the_SITE_and_offers_no_page_citation():
    """THE SPLIT (owner, 2026-09-05).

    REPLACES a test asserting the bar carried a two-entry menu. That design was
    wrong for a reason the owner spotted: this bar spans the whole window, so a
    "Citation for this page" entry on it was offered on the Search tab, on
    Personal Lists, on Community -- for a folio the reader had left.

    Gating the entry on the active tab would have worked. Moving it is better:
    "Cite this page" now lives on the Browse tab's own toolbar, where it cannot
    be reached from a surface that has no page. A control that only exists where
    its subject exists cannot describe the wrong thing.
    """
    src = APP.read_text(encoding='utf-8')
    bar = src[src.index('def _create_citation_bar'):
              src.index('def _copy_citation_text')]

    assert 'self.copy_site_citation' in bar, (
        'the bar no longer copies the site citation')
    assert 'copy_page_citation' not in bar, (
        'the bar offers a page citation again -- it is visible on every tab, so '
        'that is an offer to cite a folio the reader is not looking at')
    assert 'Citation for this page' not in bar


def test_cite_this_page_is_on_the_browse_toolbar():
    """Its other half: the page citation exists, on the surface that has a page."""
    code = _code_only(APP)
    assert 'self.btn_b_cite = QPushButton(' in code, (
        'the Browse toolbar has no cite button, so the page citation removed '
        'from the bar has nowhere to live')
    assert 'nav_bar.addWidget(self.btn_b_cite)' in code, (
        'the cite button is not on the Browse toolbar')
    assert 'self.btn_b_cite.clicked.connect(self.copy_page_citation)' in code
    assert 'self.btn_b_cite.setEnabled(False)' in code, (
        'the cite button starts enabled, before any manuscript is loaded')


def test_the_cite_button_is_disabled_for_a_readers_own_scan():
    """A LOCAL "97" document is savable but not citable.

    `_sync_browse_cite_button` runs wherever `btn_b_save` is enabled, because
    "there is something to save" and "there is something to cite" are the same
    condition with that one exception.
    """
    fn, stub = _load_method('_sync_browse_cite_button')

    class _Btn:
        enabled = None

        def setEnabled(self, value):
            self.enabled = value

    for sid, expected in (('99001', True),                     # corpus
                          ('970012345601234567', False),       # LOCAL
                          (None, False)):                      # nothing loaded
        stub.btn_b_cite = _Btn()
        stub.current_browse_sid = sid
        fn(stub)
        assert stub.btn_b_cite.enabled is expected, (
            'sys_id %r gave enabled=%r' % (sid, stub.btn_b_cite.enabled))


def test_the_cite_sync_does_not_consult_the_tab_gate():
    """It must not call `_browse_page_citation`.

    That method refuses whenever another tab is in front -- which is the
    ordinary state while a manuscript is still loading from a search result, so
    consulting it here would leave the button disabled on arrival.
    """
    body = _code_only(APP).split('def _sync_browse_cite_button', 1)[1] \
                          .split('\n    def ', 1)[0]
    assert '_browse_page_citation' not in body, (
        'the enable check consults the tab-gated citation, so the button is '
        'disabled whenever the manuscript loads while another tab is in front')


# ---------------------------------------------------------------------------
# 4. The desktop cites SOFTWARE, not a website (owner, 2026-09-05)
# ---------------------------------------------------------------------------

def test_the_desktop_does_not_cite_the_website():
    """THE DEFECT the owner reported.

    Both desktop citations named the site, its address and a retrieval date --
    all three false for a reader who never opened a browser. A reader working in
    the desktop application ran a PROGRAM, and what identifies a program is its
    version.
    """
    from shared.export_utils import desktop_software_clause
    from shared.transcription_credits import site_citation

    for lang in ('en', 'he'):
        text = site_citation(lang=lang,
                             software=desktop_software_clause('9.1.0')).text
        assert 'genizahsearch.com' not in text, (
            'the desktop citation points at a website the reader never visited')
        assert 'retrieved' not in text.lower()
        assert 'נצפה בתאריך' not in text, 'the desktop citation carries a date'
        assert 'Dicta Genizah Search Pro V9.1.0' in text
        # The MiDRASH credit is unaffected -- it is about the TEXT, not the app.
        assert 'MiDRASH' in text


def test_the_product_name_is_the_same_in_both_languages():
    """Not a missing translation -- the established convention for this product.

    The window title, the export credit header and the Hebrew consent dialog all
    keep the Latin name. The WEB site name translates because it describes a
    website; a product name does not.
    """
    from shared.export_utils import DESKTOP_APP_NAME, desktop_software_clause
    from shared.transcription_credits import site_citation

    assert DESKTOP_APP_NAME == 'Dicta Genizah Search Pro'
    clause = desktop_software_clause('9.1.0')
    he = site_citation(lang='he', software=clause).text
    en = site_citation(lang='en', software=clause).text
    assert DESKTOP_APP_NAME in he and DESKTOP_APP_NAME in en
    # ...and the Hebrew WEB site name must not appear on a desktop citation.
    assert 'אתר הגניזה של דיקטה' not in he, (
        'the desktop citation calls itself the website, in Hebrew')


def test_the_web_citation_is_unchanged():
    """The CONTROL. `software` is opt-in; a change that made every citation a
    software citation would satisfy the two tests above and break the site."""
    from shared.transcription_credits import site_citation

    text = site_citation(lang='en', retrieved_on='2026-09-05').text
    assert 'genizahsearch.com' in text
    assert 'retrieved Sept. 5, 2026' in text
    assert 'Dicta Genizah Search Pro' not in text, (
        'the WEB citation now names the desktop product')


def test_a_version_is_required_for_the_clause_to_carry_one():
    """The owner asked for the version specifically. A blank one must degrade to
    the bare name rather than printing a dangling "V"."""
    from shared.export_utils import desktop_software_clause

    assert desktop_software_clause('9.1.0') == 'Dicta Genizah Search Pro V9.1.0'
    assert desktop_software_clause('') == 'Dicta Genizah Search Pro'
    assert desktop_software_clause(None) == 'Dicta Genizah Search Pro'


def test_the_desktop_reads_the_real_app_version():
    """A hardcoded version would go stale at the next release and nothing would
    say so -- the citation would name a version the reader is not running."""
    from version import APP_VERSION

    body = _code_only(APP).split('def _software_clause', 1)[1] \
                          .split('\n    def ', 1)[0]
    assert 'APP_VERSION' in body, (
        'the software clause does not read the real version')
    assert APP_VERSION not in body, (
        'the version is hardcoded into the clause instead of read from version.py')


def test_both_desktop_citation_paths_pass_the_software_clause():
    """The bar, the Browse toolbar and the ResultDialog all cite the app.

    A path that missed it would silently fall back to the WEB form -- naming a
    site and a date -- which is the defect, not an error.
    """
    code = _code_only(APP)
    for name in ('_site_citation_text', '_browse_page_citation'):
        body = code.split('def %s' % name, 1)[1].split('\n    def ', 1)[0]
        assert 'software=self._software_clause()' in body, (
            '%s does not pass the software clause, so it cites the website'
            % name)

    rd = _code_only(REPO / 'desktop' / 'result_dialog.py')
    body = rd.split('def _rd_page_citation', 1)[1].split('\n    def ', 1)[0]
    assert 'software=self._app._software_clause()' in body, (
        'the ResultDialog citation still names the website')
