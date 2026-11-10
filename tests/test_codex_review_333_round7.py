"""Regressions for the two Codex findings in round 7 on PR #333 (2026-09-02).

P2 — `_snippet_match_phrase` kept the results table's line-break sentinel
(`SearchEngine.highlight` renders `\\n` as ` \\u2016 `). Source texts contain no
such character, so a match spanning a line break could never satisfy
`must_contain` -- precisely the multi-line hits that most need it. (The original
report, `תקום רבה / דיניך`, is exactly this shape.)

P2 — the Advanced view's `_adv_credit_nli` was the empty string: the round-5
patch lifted `attribution = ''` instead of the NLI credit literal, and
`switchImageCredit()` skips a falsy value, so the Bodleian label survived the
fallback anyway. The round-5 test asserted the WIRING but not the VALUE.
"""
from __future__ import annotations

import re

SRC = "web/pages/search_results.py"
SENTINEL = "‖"


def _read(path):
    return open(path, encoding="utf-8").read()


def _load_snippet_helper():
    """The pure helper, executed without importing the NiceGUI page module."""
    src = _read(SRC)
    i = src.index("def _snippet_match_phrase")
    j = src.index("# ---", i)
    ns = {"re": re, "_SNIPPET_MATCH_RE": re.compile(r"\*([^*]+)\*")}
    exec(src[i:j], ns)
    return ns["_snippet_match_phrase"]


class TestLineBreakSentinelIsStripped:
    def test_a_match_spanning_a_line_break(self):
        f = _load_snippet_helper()
        # the reported hit: "תקום רבה" / "דיניך" on consecutive lines
        snippet = f"קדושי שי *תקום רבה {SENTINEL} דיניך* וכהדרי אלצדקת"
        phrase = f(snippet)
        assert SENTINEL not in phrase
        assert phrase == "תקום רבה דיניך"

    def test_multiple_sentinels(self):
        f = _load_snippet_helper()
        phrase = f(f"*a {SENTINEL} b {SENTINEL} c*")
        assert phrase == "a b c"

    def test_plain_single_line_match_unchanged(self):
        f = _load_snippet_helper()
        assert f("foo *עצים עליו למודה* bar") == "עצים עליו למודה"

    def test_no_markers_returns_empty(self):
        f = _load_snippet_helper()
        assert f("no markers here") == ""
        assert f("") == ""

    def test_the_phrase_can_actually_match_a_source(self):
        """End to end: the extracted phrase must satisfy the shared chooser."""
        from shared.fgp_service import choose_default_source
        f = _load_snippet_helper()
        folio_text = "קדושי\nשי\nתקום רבה\nדיניך וכהדרי אלצדקת עיניך ולבך מביט"
        phrase = f(f"*תקום רבה {SENTINEL} דיניך*")
        pgp = {"id": 1, "content": "خدمة تعرض على مجلس المولا", "doc_relation": "Digital Edition",
               "language": "Arabic"}
        d = choose_default_source([pgp], folio_text, must_contain=phrase)
        assert d["provider"] == "v08", (
            "the V0.8 text contains the hit across a line break; the sentinel "
            f"used to make this miss (phrase={phrase!r}, reason={d['reason']})"
        )
        assert d["must_contain_matched"] is True


class TestAdvancedNliCreditIsNonEmpty:
    def test_the_nli_variant_has_a_real_value(self):
        src = _read(SRC)
        m = re.search(r"_adv_credit_nli = (.+)", src)
        assert m, "_adv_credit_nli not found"
        value = m.group(1).split("#")[0].strip()
        assert value not in ("''", '""'), (
            "switchImageCredit() skips a falsy value, so an empty credit leaves "
            "the Bodleian label in place after the NLI fallback"
        )
        assert "National Library of Israel" in value or "הספרייה הלאומית" in value

    def test_the_two_variants_differ(self):
        src = _read(SRC)
        i = src.index("# Attribution footer")
        block = src[i:i + 2400]
        assert "OXFORD_IMAGE_CREDIT_EN" in block
        assert "_adv_credit_nli" in block
        m = re.search(r"_adv_credit_nli = (.+)", src)
        assert "OXFORD" not in m.group(1)

    def test_js_skips_a_falsy_credit(self):
        # Documents WHY the value must be non-empty.
        js = _read("web/static/manuscript_viewer.js")
        i = js.index("function switchImageCredit(")
        body = js[i:i + 900]
        assert "if (text)" in body
