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
  * ``TestPrefixCheckScript`` -- runs scripts/check_sys_id_prefixes.py the way its
    docstring documents it, over a REAL Tantivy index. Added after a review found
    two defects on that branch (a missing repo root on ``sys.path``, and a
    ``Searcher.segment_readers()`` call the Python binding does not expose): both
    survived because nothing ever executed the index path.
"""
from __future__ import annotations

import importlib.util
import io
import random
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

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


class TestPrefixCheckScript:
    """The verification script must actually run, over a real index.

    Both defects a review found here were invisible to every other test because
    the Tantivy branch is skipped unless ``--index`` is passed and the directory
    exists. These tests pass it a real index.
    """

    @staticmethod
    def _build_index(path, headers, field="full_header"):
        tantivy = pytest.importorskip("tantivy")
        path.mkdir(parents=True, exist_ok=True)
        builder = tantivy.SchemaBuilder()
        builder.add_text_field(field, stored=True)
        index = tantivy.Index(builder.build(), path=str(path))
        writer = index.writer()
        for header in headers:
            writer.add_document(tantivy.Document(**{field: [header]}))
        writer.commit()
        writer.wait_merging_threads()
        return path

    def _run(self, index_dir, cwd):
        return subprocess.run(
            [sys.executable, str(REPO_ROOT / "scripts" / "check_sys_id_prefixes.py"),
             "--index", str(index_dir)],
            cwd=cwd, capture_output=True, text=True)

    def test_documented_invocation_succeeds_on_a_corpus_index(self, tmp_path):
        """Guards the sys.path defect: run exactly as the docstring documents."""
        idx = self._build_index(tmp_path / "idx", [
            "990051620920205171_IE167198813_P000003_FL167198817",
            "990000000000000944_IE1_P000002_FL3",
            "990030907670205171_IE1_P000001_FL2",
        ])
        proc = self._run(idx, REPO_ROOT)
        assert proc.returncode == 0, f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        assert "ModuleNotFoundError" not in proc.stderr
        assert "AttributeError" not in proc.stderr
        # Guards the enumeration defect: every document must actually be walked,
        # and every one of them must have been genuinely classified.
        assert "3 classified, 0 unreadable, 0 with no sys_id" in proc.stdout
        assert "RESULT: OK" in proc.stdout

    def test_runs_from_an_unrelated_cwd(self, tmp_path):
        """The repo root must be found regardless of where the script is invoked."""
        idx = self._build_index(tmp_path / "idx2", ["990051620920205171_IE1_P1_FL2"])
        proc = self._run(idx, tmp_path)
        assert proc.returncode == 0, f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        assert "RESULT: OK" in proc.stdout

    def test_CONTROL_a_97_in_the_index_fails_the_run(self, tmp_path):
        """The check must be able to FAIL, or it proves nothing when it passes."""
        idx = self._build_index(tmp_path / "idx3", [
            "990051620920205171_IE1_P000003_FL2",
            "970012345601234567_LOCAL_P3_F0042",
        ])
        proc = self._run(idx, REPO_ROOT)
        assert proc.returncode == 1, f"stdout:\n{proc.stdout}"
        assert "RESULT: FAIL" in proc.stdout
        assert "970012345601234567" in proc.stdout

    def test_walk_pages_and_reports_completeness(self, tmp_path):
        """Multi-page walk: every document is seen when paging more than once."""
        pytest.importorskip("tantivy")
        headers = [f"9900000000000{i:05d}_IE1_P1_FL2" for i in range(25)]
        idx = self._build_index(tmp_path / "idx4", headers)
        spec = importlib.util.spec_from_file_location(
            "_chk", REPO_ROOT / "scripts" / "check_sys_id_prefixes.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.TANTIVY_PAGE = 4  # 25 docs -> 7 pages
        assert mod.scan_tantivy(str(idx)) == {}, "a clean corpus index must report clean"

    def test_an_incomplete_walk_never_reports_clean(self, tmp_path, monkeypatch):
        """A short walk must fail loudly, not look like 'no 97 found'."""
        pytest.importorskip("tantivy")
        idx = self._build_index(tmp_path / "idx5",
                                [f"9900000000000{i:05d}_IE1_P1_FL2" for i in range(10)])
        spec = importlib.util.spec_from_file_location(
            "_chk2", REPO_ROOT / "scripts" / "check_sys_id_prefixes.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        import tantivy as _t
        real_open = _t.Index.open

        class _Truncating:
            """A searcher that silently returns only the first page."""
            def __init__(self, inner):
                self._inner = inner
                self.num_docs = inner.num_docs
            def search(self, query, limit=10, offset=0, count=True):
                if offset:
                    return SimpleNamespace(hits=[], count=0)
                return self._inner.search(query, limit=2, offset=0, count=False)
            def doc(self, address):
                return self._inner.doc(address)

        class _Idx:
            def __init__(self, inner):
                self._inner = inner
            def searcher(self):
                return _Truncating(self._inner.searcher())

        monkeypatch.setattr(_t.Index, "open",
                            staticmethod(lambda d: _Idx(real_open(d))))
        result = mod.scan_tantivy(str(idx))
        assert "__walk_problems__" in result, (
            "a truncated walk reported a clean result -- that is indistinguishable "
            "from a corpus with no 97 in it")
        assert "incomplete walk" in result["__walk_problems__"]

    # --- second review round: the completeness guard had two holes of its own.

    def test_CONTROL_an_index_whose_schema_lacks_the_header_field_fails(self, tmp_path):
        """A walk that inspected NO header must not come back clean.

        The first version incremented its scanned counter BEFORE reading the
        document, so `scanned == total` held even when every read yielded
        nothing -- and it printed RESULT: OK off zero inspected headers.
        """
        idx = self._build_index(tmp_path / "wrong", ["990051620920205171_IE1_P1_FL2"],
                                field="other_field")
        proc = self._run(idx, REPO_ROOT)
        assert proc.returncode == 1, f"stdout:\n{proc.stdout}"
        assert "RESULT: FAIL" in proc.stdout
        assert "nothing classified" in proc.stdout

    def test_CONTROL_a_brand_new_prefix_is_detected(self, tmp_path):
        """THE point of this script: find a prefix nobody has seen before.

        Detection must not run through the constants under test. A 99/97-only
        pattern cannot match a 98, so the document contributed nothing, the
        counters still balanced, and an index of nothing but 98s reported
        RESULT: OK -- the check blind to its own subject.
        """
        idx = self._build_index(tmp_path / "p98", ["980051620920205171_IE1_P1_FL2"])
        proc = self._run(idx, REPO_ROOT)
        assert proc.returncode == 1, f"stdout:\n{proc.stdout}"
        assert "RESULT: FAIL" in proc.stdout
        assert "'98'" in proc.stdout, "the new prefix was not named in the report"
        assert "980051620920205171" in proc.stdout

    def test_an_unreadable_document_is_not_counted_as_inspected(self, tmp_path,
                                                                monkeypatch):
        """A raising `doc()` must land in `unreadable`, not in the inspected count."""
        pytest.importorskip("tantivy")
        idx = self._build_index(tmp_path / "raise",
                                ["990051620920205171_IE1_P1_FL2"] * 3)
        spec = importlib.util.spec_from_file_location(
            "_chk3", REPO_ROOT / "scripts" / "check_sys_id_prefixes.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        import tantivy as _t
        real_open = _t.Index.open

        class _Raising:
            def __init__(self, inner):
                self._inner = inner
                self.num_docs = inner.num_docs
            def search(self, *a, **kw):
                return self._inner.search(*a, **kw)
            def doc(self, address):
                raise RuntimeError("simulated unreadable document")

        monkeypatch.setattr(_t.Index, "open",
                            staticmethod(lambda d: SimpleNamespace(
                                searcher=lambda: _Raising(real_open(d).searcher()))))
        result = mod.scan_tantivy(str(idx))
        assert "__walk_problems__" in result, (
            "documents that could not be read were counted as inspected")
        assert "unreadable documents" in result["__walk_problems__"]

    def test_a_clean_corpus_index_still_passes(self, tmp_path):
        """The guards must not have made a good index un-passable."""
        idx = self._build_index(tmp_path / "clean", [
            "990051620920205171_IE167198813_P000003_FL167198817",
            "990030907670205171_IE1_P000001_FL2",
        ])
        proc = self._run(idx, REPO_ROOT)
        assert proc.returncode == 0, f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        assert "2 classified, 0 unreadable, 0 with no sys_id" in proc.stdout
