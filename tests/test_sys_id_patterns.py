# -*- coding: utf-8 -*-
"""Drift guard for sys_id extraction (2026-08-25).

Background: ~20 sites hand-rolled ``re.search(r'(99\\d{8,})', raw_header)`` while
4 hand-rolled the wider ``((?:99|97)\\d{8,})``. The two dialects drifted because
nothing tied them together. ``shared/sys_id_patterns.py`` is now the single
definition and this module is the enforcement:

  * ``TestNoHandRolledPatterns`` -- repo-grep lint; a new inline pattern fails CI
    unless it carries an explicit ``sys-id-pattern-exempt:`` annotation.
  * ``TestSiteWiring`` -- the production constants ARE the shared objects, so a
    site cannot silently swap in its own compiled pattern.
  * ``TestCorpusNeverMatchesLocal`` -- the property that makes the narrowing
    safe, and the one that fails if anyone re-widens or un-anchors.
  * ``TestLocalParsersStillAgnostic`` -- Phase 95 D-13 regression: the desktop
    parsers must KEEP accepting 97.
"""
from __future__ import annotations

import io
import random
import re
import subprocess
from pathlib import Path

import pytest

from shared.sys_id_patterns import (
    ANY_SYS_ID_RE,
    CORPUS_SYS_ID_PATTERN,
    CORPUS_SYS_ID_RE,
    extract_any_sys_id,
    extract_corpus_sys_id,
)

REPO_ROOT = Path(__file__).resolve().parent.parent

#: The canonical definition lives here; it is the one file allowed to spell the
#: pattern out. This test module is allowed too (it must name what it forbids).
_DEFINING_FILES = {
    "shared/sys_id_patterns.py",
    "tests/test_sys_id_patterns.py",
}

#: A line that spells a sys_id regex by hand. Matches the literal two-char
#: regex token ``\d`` right after a ``99``/``97`` prefix, in either dialect.
_HAND_ROLLED = re.compile(r"(?:99|97)\\d|\(\?:99\|97\)")

#: Opt-out marker. A site that genuinely must spell its own pattern puts this on
#: the same line or within the 5 lines above, WITH a reason.
_EXEMPT = "sys-id-pattern-exempt"


def _tracked_python_files():
    out = subprocess.run(
        ["git", "ls-files", "*.py"], cwd=REPO_ROOT,
        capture_output=True, text=True, check=True).stdout
    return [p for p in out.splitlines() if p.strip()]


class TestNoHandRolledPatterns:
    """No site may spell a sys_id regex by hand without an annotated exemption."""

    def test_no_unannotated_hand_rolled_sys_id_regex(self):
        offenders = []
        for rel in _tracked_python_files():
            if rel in _DEFINING_FILES:
                continue
            path = REPO_ROOT / rel
            try:
                lines = io.open(path, encoding="utf-8").read().splitlines()
            except (OSError, UnicodeDecodeError):
                continue
            for i, line in enumerate(lines):
                if not _HAND_ROLLED.search(line):
                    continue
                window = lines[max(0, i - 5): i + 1]
                if any(_EXEMPT in w for w in window):
                    continue
                offenders.append(f"{rel}:{i + 1}: {line.strip()}")
        assert not offenders, (
            "Hand-rolled sys_id regex found. Import from shared/sys_id_patterns.py "
            "(CORPUS_SYS_ID_RE for Genizah records, ANY_SYS_ID_RE only where a "
            "LOCAL 'My Library' header can genuinely arrive). If a site truly needs "
            "its own pattern, add a 'sys-id-pattern-exempt: <reason>' comment.\n  "
            + "\n  ".join(offenders))


class TestSiteWiring:
    """Each migrated site must reference the shared object, not a private copy."""

    def test_export_state_uses_corpus_pattern(self):
        import web.export_state as m
        assert m._SYS_ID_RE is CORPUS_SYS_ID_RE

    def test_search_serializer_uses_corpus_pattern(self):
        import shared.search_serializer as m
        assert m._SYS_ID_REGEX is CORPUS_SYS_ID_RE

    def test_passage_parallels_uses_corpus_pattern(self):
        import shared.passage_parallels as m
        assert m._SYS_ID_RE is CORPUS_SYS_ID_RE

    def test_discovery_page_id_is_built_from_corpus_pattern(self):
        import web.services as m
        assert m._DISCOVERY_PAGE_ID_RE.pattern.startswith(CORPUS_SYS_ID_PATTERN)


class TestCorpusNeverMatchesLocal:
    """The property that makes narrowing safe.

    An UNANCHORED corpus pattern does not merely miss a LOCAL header -- it can
    match a ``99`` sitting inside the LOCAL id's random digits and return a
    truncated, wrong sys_id (measured ~6.4% of LOCAL ids). These tests fail if
    the anchor or the 99-only restriction is removed.
    """

    @staticmethod
    def _local_ids(n, seed=1234):
        rnd = random.Random(seed)
        return ["97" + f"{rnd.randrange(10 ** 8):08d}" + f"{rnd.randrange(10 ** 8):08d}"
                for _ in range(n)]

    def test_corpus_pattern_never_matches_any_local_id(self):
        bad = []
        for sid in self._local_ids(20000):
            got = extract_corpus_sys_id(f"{sid}_LOCAL_P3_F0042")
            if got is not None:
                bad.append((sid, got))
        assert not bad, (
            f"CORPUS pattern mis-matched inside {len(bad)} LOCAL sys_ids "
            f"(first: real={bad[0][0]} -> extracted={bad[0][1]}). A LOCAL header "
            "must yield None, never a truncated id.")

    def test_known_mis_match_case_is_fixed(self):
        # Regression pin: this exact id mis-matched under the old unanchored
        # r'(99\d{8,})', which returned '993169503583183'.
        # sys-id-pattern-exempt: the string above names the historical defect.
        header = "970993169503583183_LOCAL_P3_F0042"
        assert extract_corpus_sys_id(header) is None
        assert extract_any_sys_id(header) == "970993169503583183"

    @pytest.mark.parametrize("header,expected", [
        ("990051620920205171_IE167198813_P000003_FL167198817", "990051620920205171"),
        ("990000000000000944_IE1_P000002_FL3", "990000000000000944"),
        ("header_9912345678901234_IE99_P7", "9912345678901234"),
        ("==> 990030907670205171_IE1_P000001_FL2 <==", "990030907670205171"),
    ])
    def test_corpus_headers_still_resolve(self, header, expected):
        assert extract_corpus_sys_id(header) == expected

    def test_empty_and_junk_are_none(self):
        for junk in ("", None, "no digits here", "IE1_P3_FL4"):
            assert extract_corpus_sys_id(junk) is None

    def test_discovery_page_id_rejects_local_header(self):
        from web.services import discovery_page_id_from_header
        assert discovery_page_id_from_header("970012345601234567_LOCAL_P3_F0042") is None


class TestLocalParsersStillAgnostic:
    """Phase 95 D-13: the desktop parsers must KEEP accepting 97."""

    @pytest.fixture
    def mgr(self):
        from genizah_core import MetadataManager
        return MetadataManager.__new__(MetadataManager)

    def test_parse_header_smart_still_accepts_local(self, mgr):
        assert mgr.parse_header_smart("970012345601234567_LOCAL_P3_F0042")[0] == "970012345601234567"

    def test_parse_full_id_components_still_accepts_local(self, mgr):
        assert mgr.parse_full_id_components(
            "970012345601234567_LOCAL_P3_F0042")["sys_id"] == "970012345601234567"

    def test_any_pattern_covers_both_namespaces(self):
        assert ANY_SYS_ID_RE.search("990025143260205171_IE1_P5_FL2").group(1) == "990025143260205171"
        assert ANY_SYS_ID_RE.search("970012345601234567_LOCAL_P3_F0042").group(1) == "970012345601234567"
