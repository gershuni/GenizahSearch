"""A refused Responsa query is a message in Joins Lab, never zero results; and the web's
full-text highlight matches before escaping (docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md,
Phase 1). The engine and the API sides are tests/test_responsa_api_refusal.py.
"""
import pytest

from shared.responsa_match import ResponsaQueryRefused


class _RefusingSearcher:
    def __init__(self):
        self.calls = []

    def execute_search(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        raise ResponsaQueryRefused()


def test_the_web_joins_lab_executor_lets_a_refusal_through(monkeypatch):
    from web.state import state
    from web.joins_executor import WebSearchExecutor
    monkeypatch.setattr(state, 'searcher', _RefusingSearcher())
    with pytest.raises(ResponsaQueryRefused):
        WebSearchExecutor().execute_search('כ*נים', 'exact', 0)

    class _Broken:
        def execute_search(self, *a, **k):
            raise RuntimeError('index gone')
    monkeypatch.setattr(state, 'searcher', _Broken())
    assert WebSearchExecutor().execute_search('שלום', 'exact', 0) == []   # unchanged


def test_the_other_side_of_the_leaf_lets_a_refusal_through():
    from shared.joins_lab import apply_cross_side, normalize_candidate
    base = [normalize_candidate({'display': {'id': '990000000010205171', 'img': 3}, 'uid': 'u', 'full_text': ''})]
    with pytest.raises(ResponsaQueryRefused):
        apply_cross_side(_RefusingSearcher(), base, 'כ*נים', {'responsa_mode': True}, 'AND', None)


def test_the_web_full_text_highlight_matches_before_escaping():
    """`"` was `&quot;` when matched, so `ר"ת` was never marked (2026-10-21)."""
    from shared.search_engine import make_mark_tolerant_pattern
    import re
    from web.pages.search_results import highlight_full_text_html
    pattern = make_mark_tolerant_pattern(re.escape('רת'))
    html_text, stopped = highlight_full_text_html('אמר ר"ת <b> כאן', pattern)
    assert not stopped
    assert '<span class="highlight-match">ר&quot;ת</span>' in html_text
    assert '&lt;b&gt;' in html_text                  # the text itself is escaped
    plain, _ = highlight_full_text_html('a\x01b<', '')
    assert plain == 'ab&lt;'                         # no sentinel survives, no pattern


@pytest.mark.gui
def test_the_desktop_other_side_worker_reports_the_refusal():
    from desktop.join_workbench import _CrossSideWorker
    from shared.joins_lab import BuilderRow, SideQuery, normalize_candidate
    base = [normalize_candidate({'display': {'id': '990000000010205171', 'img': 3}, 'uid': 'u', 'full_text': ''})]
    worker = _CrossSideWorker(_RefusingSearcher(), base, SideQuery(rows=(BuilderRow(term='כ*נים'),)), {}, 'AND', None)
    refused, done = [], []
    worker.refused.connect(refused.append)
    worker.done.connect(done.append)
    worker.run()
    assert refused and '*' in refused[0]
    assert [c.page for c in done[0].candidates] == [3]   # this side's rows still show
