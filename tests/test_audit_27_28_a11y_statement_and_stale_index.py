# -*- coding: utf-8 -*-
"""Audit items #27 (a11y statement wording) + SEED-019 #28 (stale-index diagnostics).

#27 — The accessibility statement (`web/pages/accessibility.py`) over-claimed
("fully navigable", "clearly visible on ALL interactive elements", "have
appropriate alternative text") and carried a stale "February 2025" date. The
fix softens those absolute claims to match reality (after SEED-014's a11y work)
and bumps the date. Because the page renders every string through the shared
``tr()`` dict, each reworded English string must also be re-keyed in
``genizah_translations.TRANSLATIONS`` or Hebrew users fall back to English.

#28 — The SEED-006 ``content_search`` compat gate (`_index_has_field`) silently
degrades Hebrew punctuation/diacritic retrieval against an index built before the
field existed. SEED-006 M3 logged a one-shot WARNING for the GENIZAH index;
SEED-019 #28 extends that visibility: a reusable message helper, a queryable
``SearchEngine.index_staleness_report()``, and LOCAL-side parity.
"""
from __future__ import annotations

import pathlib

REPO_ROOT = pathlib.Path(__file__).parent.parent
A11Y_PY = REPO_ROOT / "web" / "pages" / "accessibility.py"
CORE_PY = REPO_ROOT / "genizah_core.py"


def _read(p: pathlib.Path) -> str:
    return p.read_text(encoding="utf-8")


# =========================================================================== #
# #27 — accessibility statement: softened wording + fresh date + i18n sync
# =========================================================================== #


class TestFinding27AccessibilityStatement:
    # The absolute over-claims the audit flagged must be gone from the page.
    OVERCLAIMS = (
        "The site is fully navigable using a keyboard.",
        "Focus indicators are clearly visible on all interactive elements.",
        "Controls and images have appropriate alternative text or labels.",
    )
    # Their softened replacements (must appear verbatim so the tr() key matches).
    SOFTENED = (
        "Most of the site can be navigated using a keyboard, "
        "and we continue to expand keyboard support.",
        "Focus indicators are visible on interactive elements.",
        "Controls and images are given alternative text or labels where applicable.",
    )

    def test_overclaims_removed_from_page(self):
        src = _read(A11Y_PY)
        for claim in self.OVERCLAIMS:
            assert claim not in src, f"over-claim still present in page: {claim!r}"

    def test_softened_strings_present_in_page(self):
        src = _read(A11Y_PY)
        for soft in self.SOFTENED:
            assert soft in src, f"softened string missing from page: {soft!r}"

    def test_softened_strings_have_hebrew_translations(self):
        # Each reworded English string is a new tr() key; without a HE entry the
        # Hebrew (default) UI would silently fall back to English.
        from genizah_translations import TRANSLATIONS

        for soft in self.SOFTENED:
            assert soft in TRANSLATIONS, f"no HE translation for new key: {soft!r}"
            # The HE value must actually be Hebrew (not an accidental EN copy).
            assert any("֐" <= ch <= "׿" for ch in TRANSLATIONS[soft]), (
                f"translation for {soft!r} is not Hebrew: {TRANSLATIONS[soft]!r}"
            )

    def test_old_overclaim_keys_not_left_dangling(self):
        # The renamed keys should not linger in the page (the dict may keep them
        # harmlessly, but the page must not reference the old wording).
        from genizah_translations import TRANSLATIONS

        # Sanity: at least the page no longer references the old keys (covered
        # above); confirm the dict was actually re-keyed for the new strings.
        for soft in self.SOFTENED:
            assert TRANSLATIONS.get(soft, "") != ""

    def test_stale_date_bumped(self):
        src = _read(A11Y_PY)
        assert "February 2025" not in src, "stale 'February 2025' date still on page"
        assert "פברואר 2025" not in src, "stale Hebrew date still on page"
        # The refreshed bilingual date is present.
        assert "June 2026" in src
        assert "יוני 2026" in src


# =========================================================================== #
# SEED-019 #28 — content_search_staleness_messages (pure helper)
# =========================================================================== #


class TestStalenessMessages:
    def test_both_fresh_returns_no_messages(self):
        from genizah_core import content_search_staleness_messages

        assert content_search_staleness_messages(True, True) == []

    def test_genizah_stale_no_local(self):
        from genizah_core import content_search_staleness_messages

        msgs = content_search_staleness_messages(False, None)
        assert len(msgs) == 1
        assert "GENIZAH" in msgs[0]
        # Carries an actionable remediation hint.
        assert "Rebuild" in msgs[0] or "rebuild" in msgs[0]

    def test_local_stale_only(self):
        from genizah_core import content_search_staleness_messages

        msgs = content_search_staleness_messages(True, False)
        assert len(msgs) == 1
        assert "LOCAL" in msgs[0]
        assert "Re-index" in msgs[0]

    def test_both_stale_returns_two(self):
        from genizah_core import content_search_staleness_messages

        msgs = content_search_staleness_messages(False, False)
        assert len(msgs) == 2
        assert any("GENIZAH" in m for m in msgs)
        assert any("LOCAL" in m for m in msgs)

    def test_local_none_never_emits_local_message(self):
        from genizah_core import content_search_staleness_messages

        # local_present=None means "no LOCAL index" -> never a LOCAL message.
        assert content_search_staleness_messages(True, None) == []
        assert content_search_staleness_messages(False, None) == [
            content_search_staleness_messages(False, None)[0]
        ]
        for m in content_search_staleness_messages(False, None):
            assert "LOCAL" not in m


# =========================================================================== #
# SEED-019 #28 — SearchEngine.index_staleness_report (queryable verdict)
# =========================================================================== #


class TestIndexStalenessReport:
    """Calls the method unbound with a lightweight stand-in `self` so we don't
    pay SearchEngine.__init__ (which loads Tantivy)."""

    @staticmethod
    def _report(genizah, local_flag, has_local_index):
        from types import SimpleNamespace

        from genizah_core import SearchEngine

        fake = SimpleNamespace(
            _has_content_search=genizah,
            _local_has_content_search=local_flag,
            local_index=object() if has_local_index else None,
        )
        return SearchEngine.index_staleness_report(fake)

    def test_genizah_stale_no_local_index(self):
        r = self._report(genizah=False, local_flag=False, has_local_index=False)
        assert r["genizah_content_search"] is False
        assert r["local_content_search"] is None  # no LOCAL index open
        assert r["stale"] is True
        assert len(r["messages"]) == 1 and "GENIZAH" in r["messages"][0]

    def test_both_fresh(self):
        r = self._report(genizah=True, local_flag=True, has_local_index=True)
        assert r["genizah_content_search"] is True
        assert r["local_content_search"] is True
        assert r["stale"] is False
        assert r["messages"] == []

    def test_both_stale(self):
        r = self._report(genizah=False, local_flag=False, has_local_index=True)
        assert r["local_content_search"] is False
        assert r["stale"] is True
        assert len(r["messages"]) == 2

    def test_local_index_absent_forces_none_regardless_of_flag(self):
        # Even if the (stale) flag is False, no open LOCAL index => None, no msg.
        r = self._report(genizah=True, local_flag=False, has_local_index=False)
        assert r["local_content_search"] is None
        assert r["stale"] is False


# =========================================================================== #
# SEED-019 #28 — wiring source guards
# =========================================================================== #


class TestStalenessWiring:
    def test_reload_index_logs_via_shared_helper(self):
        src = _read(CORE_PY)
        # The inline M3 warning text was replaced by the centralized helper.
        assert "content_search_staleness_messages(False, None)" in src
        # The old bespoke wording is gone (kept the SEED-006 tag in a comment only).
        assert "retrieval fix is INERT until" not in src

    def test_local_open_sites_warn(self):
        src = _read(CORE_PY)
        # Both LOCAL open sites call the parity warning helper.
        assert src.count("self._warn_if_local_index_stale()") >= 2
        assert "def _warn_if_local_index_stale" in src

    def test_report_method_present(self):
        src = _read(CORE_PY)
        assert "def index_staleness_report" in src
