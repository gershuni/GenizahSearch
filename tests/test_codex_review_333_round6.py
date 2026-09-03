"""Regression for the Codex finding in round 6 on PR #333 (2026-09-02).

P2 `web/pages/search_results.py` — the Advanced dialog picked the hit-containing
text for its initial render (passing `must_contain`), but its
`create_version_selector(...)` call omitted it. The selector's delayed loader then
re-ran the chooser under plain PGP-first precedence and, about 0.1 s later,
replaced the correct text with a transcription that does not contain the hit.

Both decisions must be made with the same phrase.
"""
from __future__ import annotations

import ast


SRC = "web/pages/search_results.py"


def _read(path):
    return open(path, encoding="utf-8").read()


def _render_content_body():
    src = _read(SRC)
    fn = next(n for n in ast.walk(ast.parse(src))
              if isinstance(n, ast.FunctionDef) and n.name == "render_content")
    return ast.get_source_segment(src, fn)


class TestAdvancedSelectorAgreesWithTheInitialRender:
    def test_selector_receives_must_contain(self):
        body = _render_content_body()
        i = body.index("create_version_selector(")
        call = body[i:i + 1200]
        assert "must_contain=" in call, (
            "the delayed loader would otherwise re-decide under PGP-first and "
            "overwrite the hit-containing text"
        )

    def test_it_is_the_same_phrase_the_initial_decision_used(self):
        body = _render_content_body()
        # the initial display_text decision
        first = body.index("choose_default_source(")
        first_call = body[first:first + 600]
        # round 11 wrapped the extraction in a page-scoping helper
        assert "must_contain=_hit_scope_phrase(snippet, adv_state, page)" in first_call
        # the selector call
        i = body.index("create_version_selector(")
        call = body[i:i + 1200]
        assert "must_contain=_hit_scope_phrase(snippet, adv_state, page)" in call

    def test_no_selector_call_in_this_module_omits_the_phrase(self):
        src = _read(SRC)
        start = 0
        seen = 0
        while True:
            i = src.find("create_version_selector(", start)
            if i == -1:
                break
            seen += 1
            call = src[i:i + 1200]
            assert "must_contain=" in call, f"call #{seen} at offset {i} omits must_contain"
            start = i + 1
        assert seen >= 1, "no create_version_selector call found — module refactored?"

    def test_selector_accepts_the_parameter(self):
        import inspect
        from web.components.version_selector import create_version_selector
        assert "must_contain" in inspect.signature(create_version_selector).parameters
