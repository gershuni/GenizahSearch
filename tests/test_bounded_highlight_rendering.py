"""Expensive saved query patterns must not hang result viewers."""
import html
import time

from shared.joins_lab import htmlify, _match_line, snippet_html, snippet_plain
from shared import search_regex


def test_highlight_timeout_keeps_escaped_text(monkeypatch):
    monkeypatch.setenv('GENIZAH_REGEX_TIMEOUT_SECONDS', '0.001')
    text = 'a' * 10000 + '!<script>'
    rendered = htmlify(text, r'(a+)+$')
    assert html.escape(text) in rendered
    assert '<script>' not in rendered


def test_snippet_line_search_is_bounded(monkeypatch):
    monkeypatch.setenv('GENIZAH_REGEX_TIMEOUT_SECONDS', '0.001')
    assert _match_line(['a' * 10000 + '!'], r'(a+)+$') == -1


def test_snippets_survive_expired_compilation_budget(monkeypatch):
    monkeypatch.setattr(search_regex.time, 'monotonic', lambda: 2.0)
    token = search_regex._deadline.set(1.0)
    try:
        text = 'first <line>\nsecond line'
        assert _match_line(text.splitlines(), r'\w+') == -1
        assert snippet_plain(text, r'\w+') == snippet_plain(text, None)
        assert snippet_html(text, r'\w+') == snippet_html(text, None)
        assert '&lt;line&gt;' in snippet_html(text, r'\w+')
    finally:
        search_regex._deadline.reset(token)


def test_hebrew_highlight_and_escaping_preserved():
    rendered = htmlify('א < שלום & עולם', 'שלום')
    assert "<b style='color:#dc2626'>שלום</b>" in rendered
    assert '&lt;' in rendered and '&amp;' in rendered


def test_viewer_keeps_short_budget_with_research_defaults(monkeypatch):
    monkeypatch.delenv('GENIZAH_REGEX_TIMEOUT_SECONDS', raising=False)
    text = 'a' * 10000 + '!<script>'
    started = time.monotonic()
    rendered = htmlify(text, r'(a|aa)+$')
    assert time.monotonic() - started < 2
    assert html.escape(text) in rendered
