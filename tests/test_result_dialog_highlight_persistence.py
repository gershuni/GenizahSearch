# -*- coding: utf-8 -*-
"""Debug session oxford-fgp-image-mismatch — sub-issue B.

Bug: the search-hit highlight (rendered as *...* markers -> <b style='color:
red;'> by ResultDialog._htmlify) survived the INITIAL page render, but was
silently lost when the user switched the Version combo away from "V0.8" and
then back to it.

Mechanism (confirmed by reading result_dialog.py): the "V0.8"/"original"
combo entry is populated with a version-cache key that is ALWAYS pre-seeded
in `_rd_load_versions()` (`self._rd_versions_cache = {'original': ...}`), so
`_rd_load_version_content`'s cache-hit branch — not the separate
`if source == "original":` branch further down — is what actually redisplays
V0.8 text on every switch-back. The value that gets cached was
`self.text_ms.toPlainText()`, taken AFTER `load_page()` had already rendered
the highlighted HTML: `*...*` markers were consumed by `_htmlify` into <b>
tags, so plain-text extraction returns text with NO trace of where the hit
was. Switching back re-displayed that de-highlighted snapshot forever after.

Fix: `load_page()` now stashes the *...*-marked raw text (the exact string
BEFORE `_htmlify` converts the markers) in `self._rd_original_marked_text`.
`_rd_load_versions()` seeds the cache from THAT instead of
`toPlainText()`. `self._rd_original_text` (still toPlainText()-derived) is
left untouched — other callers use it as clean HTR text for PGP-edition
coverage-ratio matching (SEED-030 htr_text= sites), which must not see
literal '*' characters.

Qt-FREE, in the pattern of tests/test_result_dialog_manuscript_nav.py: every
method under test is bound unbound onto a lightweight stub, and the one Qt
dependency (`apply_line_numbered_text`, which needs a real QTextEdit/
QTextBrowser to render a line-number gutter) is monkeypatched to a
side-effect-free stand-in that mimics exactly what the real function does to
`self.text_ms` (setHtml/setPlainText) -- no QApplication, no event loop.
"""
from __future__ import annotations

import ast
import io
import os

import desktop.result_dialog as result_dialog

RD = result_dialog.ResultDialog
RD_PY = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'desktop', 'result_dialog.py')


def _method_source(name):
    """The source of ONE method of ResultDialog, by name."""
    tree = ast.parse(io.open(RD_PY, encoding='utf-8').read())
    cls = next(n for n in tree.body
               if isinstance(n, ast.ClassDef) and n.name == 'ResultDialog')
    fn = next(n for n in cls.body
              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
              and n.name == name)
    lines = io.open(RD_PY, encoding='utf-8').read().splitlines()
    return '\n'.join(lines[fn.lineno - 1:fn.end_lineno])


# ---------------------------------------------------------------------------
# Stubs.
# ---------------------------------------------------------------------------

class _Button:
    def __init__(self):
        self.visible = True

    def setVisible(self, b):
        self.visible = bool(b)


class _TextMs:
    """Stand-in for the real QTextBrowser `self.text_ms`."""

    def __init__(self, plain=""):
        self._plain = plain
        self.html = None
        self.layout_direction = None

    def toPlainText(self):
        return self._plain

    def setLayoutDirection(self, direction):
        self.layout_direction = direction

    def setHtml(self, html):
        self.html = html

    def setPlainText(self, text):
        self._plain = text


class _CorrectionsClient:
    def __init__(self, server_available=False):
        self._server_available = server_available

    def is_server_available(self, force_check=False):
        return self._server_available


class _App:
    def __init__(self, server_available=False):
        self.corrections_client = _CorrectionsClient(server_available)


def _fake_apply_line_numbered_text(widget, rendered, *, source_text=None, pages=None, is_html=True):
    """Mirrors the ONE thing `_rd_display_text`/`load_page` rely on from the
    real `apply_line_numbered_text`: writing the body onto the widget. Skips
    the gutter/line-number machinery, which needs a real Qt widget and is
    unrelated to sub-issue B."""
    if is_html:
        widget.setHtml(rendered)
    else:
        widget.setPlainText(rendered)


class _RD:
    _rd_load_versions = RD._rd_load_versions
    _rd_load_version_content = RD._rd_load_version_content
    _rd_display_text = RD._rd_display_text
    # 260903: _rd_display_text now re-derives the *...* markers from the
    # result's own highlight_pattern, so the stub must carry the method.
    # These stubs set no `self.data`, hence no pattern, hence the
    # re-derivation is inert here and every expectation below still holds.
    _mark_search_hits = RD._mark_search_hits
    _htmlify = RD._htmlify

    def __init__(self, *, server_available=False):
        self.text_ms = _TextMs()
        self._app = _App(server_available=server_available)
        self.current_sys_id = "990053489970205171"
        self.current_p_num = 1
        self.btn_view_comments = _Button()


def _patch(monkeypatch):
    monkeypatch.setattr(result_dialog, "apply_line_numbered_text", _fake_apply_line_numbered_text)


# ---------------------------------------------------------------------------
# _rd_load_versions: cache seeding (the actual mechanism the bug lived in)
# ---------------------------------------------------------------------------

def test_cache_seeded_from_marked_text_when_available(monkeypatch):
    _patch(monkeypatch)
    rd = _RD()
    rd.text_ms._plain = "before HIT after"  # what toPlainText() returns post-render (markers stripped)
    rd._rd_original_marked_text = "before *HIT* after"  # captured pre-_htmlify by load_page

    rd._rd_load_versions()

    assert rd._rd_versions_cache["original"] == "before *HIT* after"


def test_rd_original_text_stays_plain_for_htr_matching_consumers(monkeypatch):
    """self._rd_original_text must NOT gain '*' markers -- it's used
    elsewhere (SEED-030 sites) as clean HTR text for PGP-edition
    coverage-ratio matching."""
    _patch(monkeypatch)
    rd = _RD()
    rd.text_ms._plain = "before HIT after"
    rd._rd_original_marked_text = "before *HIT* after"

    rd._rd_load_versions()

    assert rd._rd_original_text == "before HIT after"
    assert "*" not in rd._rd_original_text


def test_cache_falls_back_to_plain_text_when_load_page_has_not_run(monkeypatch):
    """Defensive fallback: no _rd_original_marked_text attribute at all
    (e.g. called before load_page ever ran) must not crash, and falls back
    to the old toPlainText()-derived value rather than raising."""
    _patch(monkeypatch)
    rd = _RD()
    rd.text_ms._plain = "fallback plain text"
    assert not hasattr(rd, "_rd_original_marked_text")

    rd._rd_load_versions()

    assert rd._rd_versions_cache["original"] == "fallback plain text"


# ---------------------------------------------------------------------------
# _rd_load_version_content: the reachable redisplay path (cache-hit branch)
# ---------------------------------------------------------------------------

def test_switching_back_to_v08_reapplies_highlight(monkeypatch):
    """The exact reported symptom: after the cache holds the marked text,
    selecting {'source': 'original'} (what the 'V0.8' combo item carries)
    must re-render the search hit as <b style='color:red;'>...</b>, not
    plain unmarked text."""
    _patch(monkeypatch)
    rd = _RD()
    rd._rd_versions_cache = {"original": "before *HIT* after"}

    rd._rd_load_version_content({"source": "original"})

    assert rd.text_ms.html == "<div dir='rtl'>before <b style='color:red;'>HIT</b> after</div>"


def test_highlight_spanning_a_line_break_still_renders(monkeypatch):
    """Regression guard for the reported case where the hit itself spans a
    newline ('רבה' / 'דיניך' on adjacent lines)."""
    _patch(monkeypatch)
    rd = _RD()
    rd._rd_versions_cache = {"original": "x\n*HIT\nSPAN*\ny"}

    rd._rd_load_version_content({"source": "original"})

    assert "<b style='color:red;'>HIT<br>SPAN</b>" in rd.text_ms.html


def test_no_marked_text_before_fix_loses_highlight_pre_regression_baseline(monkeypatch):
    """Pins the OLD (buggy) behavior as a named baseline: a cache seeded
    with plain (unmarked) text — what _rd_load_versions used to store before
    this fix — renders with no highlight span at all. If this test starts
    failing, `_htmlify`'s *...* handling itself changed, not sub-issue B."""
    _patch(monkeypatch)
    rd = _RD()
    rd._rd_versions_cache = {"original": "before HIT after"}  # no '*' markers

    rd._rd_load_version_content({"source": "original"})

    assert "<b style='color:red;'>" not in rd.text_ms.html
    assert rd.text_ms.html == "<div dir='rtl'>before HIT after</div>"


# ---------------------------------------------------------------------------
# _rd_load_version_content: defensive `source == "original"` branch
# (unreachable in practice since the cache key is always pre-seeded, but
# kept in sync for defense-in-depth -- see result_dialog.py comment).
# ---------------------------------------------------------------------------

def test_explicit_original_branch_uses_marked_text_on_cache_miss(monkeypatch):
    _patch(monkeypatch)
    rd = _RD()
    rd._rd_versions_cache = {}  # simulate a cache miss for 'original'
    rd._rd_original_marked_text = "x *Y* z"
    rd._rd_original_text = "x Y z"

    rd._rd_load_version_content({"source": "original"})

    assert "<b style='color:red;'>Y</b>" in rd.text_ms.html


def test_explicit_original_branch_falls_back_when_no_marked_text(monkeypatch):
    _patch(monkeypatch)
    rd = _RD()
    rd._rd_versions_cache = {}
    rd._rd_original_text = "plain only"
    assert not hasattr(rd, "_rd_original_marked_text")

    rd._rd_load_version_content({"source": "original"})

    assert rd.text_ms.html == "<div dir='rtl'>plain only</div>"


# ---------------------------------------------------------------------------
# load_page: pins the capture site itself (too many unrelated dependencies
# -- searcher.get_browse_page, image sync, PGP worker teardown -- to drive
# the full method here; source-text assertion pins the exact wiring line
# instead, in the style already used by this file's sibling tests).
# ---------------------------------------------------------------------------

def test_load_page_captures_marked_text_before_htmlify():
    src = _method_source('load_page')
    assert 'self._rd_original_marked_text = raw_text' in src, (
        "load_page must stash the *...*-marked raw_text BEFORE it is passed "
        "through _htmlify, so _rd_load_versions can seed the redisplay "
        "cache with the highlight intact."
    )
    # The capture must happen AFTER the highlight-pattern substitution
    # (raw_text = highlighted_text) and BEFORE _rd_load_versions() runs --
    # otherwise it would capture the pre-highlight text.
    capture_pos = src.index('self._rd_original_marked_text = raw_text')
    highlight_sub_pos = src.index('raw_text = highlighted_text')
    load_versions_pos = src.index('self._rd_load_versions()')
    assert highlight_sub_pos < capture_pos < load_versions_pos
