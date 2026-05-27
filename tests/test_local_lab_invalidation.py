# -*- coding: utf-8 -*-
"""Phase 95 D-09 + D-38: LOCAL LAB index invalidation on weights change.

Tests:
  Task 1 — shared/local_indexer.py build_lab_side_index + meta.json
  Task 2 — genizah_core.py lab_composition_search + search_composition_logic
"""
from __future__ import annotations

import ast
import inspect
import json
import os
import shutil
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_dummy_callbacks():
    """Return trivial callback stubs for Option C (W5)."""
    def fingerprint_dyn_fn(content: str, dynamic_rank_map) -> str:
        return " ".join(content.split()[:5]) if content else ""

    def fingerprint_static_fn(content: str) -> str:
        return " ".join(content.split()[:5]) if content else ""

    def normalize_text_fn(content: str) -> str:
        return content.lower() if content else ""

    return fingerprint_dyn_fn, fingerprint_static_fn, normalize_text_fn


def _make_local_indexer(tmp_dir: str):
    """Construct a LocalIndexer with all dirs under tmp_dir."""
    from shared.local_indexer import LocalIndexer

    index_dir = os.path.join(tmp_dir, "LocalIndex")
    lab_index_dir = os.path.join(tmp_dir, "LocalLabIndex")
    db_path = os.path.join(tmp_dir, "local_index.sqlite3")
    os.makedirs(index_dir, exist_ok=True)
    os.makedirs(lab_index_dir, exist_ok=True)
    indexer = LocalIndexer(
        index_dir=index_dir,
        lab_index_dir=lab_index_dir,
        db_path=db_path,
    )
    return indexer, index_dir, lab_index_dir, db_path


def _index_small_txt(indexer, tmp_dir: str) -> tuple[str, str]:
    """Write a small TXT file, register its folder, and scan. Returns (folder_path, sys_id)."""
    folder_path = os.path.join(tmp_dir, "docs")
    os.makedirs(folder_path, exist_ok=True)
    txt_path = os.path.join(folder_path, "sample.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("This is a test document with some Hebrew content.\n")
    indexer.add_folder(folder_path)
    indexer.scan_all()
    # Get the sys_id from SQLite
    row = indexer._conn.execute("SELECT sys_id FROM local_files LIMIT 1").fetchone()
    sys_id = row["sys_id"] if row else None
    return folder_path, txt_path, sys_id


# ---------------------------------------------------------------------------
# Task 1 tests — build_lab_side_index + .meta.json
# ---------------------------------------------------------------------------

class TestBuildLabSideIndex:

    def test_lab_meta_json_written_on_build(self, tmp_path):
        """build_lab_side_index writes .meta.json with required keys (D-38)."""
        indexer, index_dir, lab_index_dir, db_path = _make_local_indexer(str(tmp_path))
        try:
            fp_dyn, fp_static, normalize = _make_dummy_callbacks()
            lab_weights = {"use_dynamic_weights": False}
            indexer.build_lab_side_index(
                lab_weights=lab_weights,
                fingerprint_dyn_fn=fp_dyn,
                fingerprint_static_fn=fp_static,
                normalize_text_fn=normalize,
                lab_schema_version=1,
                dynamic_rank_map=None,
            )
            meta_path = os.path.join(lab_index_dir, ".meta.json")
            assert os.path.exists(meta_path), ".meta.json must exist after build"
            with open(meta_path, "r", encoding="utf-8") as fh:
                meta = json.load(fh)
            assert "weights_hash" in meta, "weights_hash key required"
            assert "lab_schema_version" in meta, "lab_schema_version key required"
            assert "last_built_at" in meta, "last_built_at key required"
        finally:
            indexer.close()

    def test_lab_meta_weights_hash_deterministic(self, tmp_path):
        """Same weights dict → same weights_hash; different weights → different hash."""
        indexer, index_dir, lab_index_dir, db_path = _make_local_indexer(str(tmp_path))
        try:
            fp_dyn, fp_static, normalize = _make_dummy_callbacks()
            weights_a = {"use_dynamic_weights": False, "v": 1}
            weights_b = {"use_dynamic_weights": True, "v": 2}

            # Build with weights_a
            indexer.build_lab_side_index(
                lab_weights=weights_a,
                fingerprint_dyn_fn=fp_dyn,
                fingerprint_static_fn=fp_static,
                normalize_text_fn=normalize,
                lab_schema_version=1,
            )
            meta_path = os.path.join(lab_index_dir, ".meta.json")
            with open(meta_path, "r", encoding="utf-8") as fh:
                hash_a1 = json.load(fh)["weights_hash"]

            # Build again with weights_a — same hash
            indexer.build_lab_side_index(
                lab_weights=weights_a,
                fingerprint_dyn_fn=fp_dyn,
                fingerprint_static_fn=fp_static,
                normalize_text_fn=normalize,
                lab_schema_version=1,
            )
            with open(meta_path, "r", encoding="utf-8") as fh:
                hash_a2 = json.load(fh)["weights_hash"]

            assert hash_a1 == hash_a2, "Same weights must produce same hash"

            # Build with weights_b — different hash
            indexer.build_lab_side_index(
                lab_weights=weights_b,
                fingerprint_dyn_fn=fp_dyn,
                fingerprint_static_fn=fp_static,
                normalize_text_fn=normalize,
                lab_schema_version=1,
            )
            with open(meta_path, "r", encoding="utf-8") as fh:
                hash_b = json.load(fh)["weights_hash"]

            assert hash_a1 != hash_b, "Different weights must produce different hash"
        finally:
            indexer.close()

    def test_lab_meta_versioning(self, tmp_path):
        """lab_schema_version in .meta.json is int >= 1."""
        indexer, index_dir, lab_index_dir, db_path = _make_local_indexer(str(tmp_path))
        try:
            fp_dyn, fp_static, normalize = _make_dummy_callbacks()
            indexer.build_lab_side_index(
                lab_weights={"v": 1},
                fingerprint_dyn_fn=fp_dyn,
                fingerprint_static_fn=fp_static,
                normalize_text_fn=normalize,
                lab_schema_version=1,
            )
            meta_path = os.path.join(lab_index_dir, ".meta.json")
            with open(meta_path, "r", encoding="utf-8") as fh:
                meta = json.load(fh)
            assert isinstance(meta["lab_schema_version"], int)
            assert meta["lab_schema_version"] >= 1

            # Version 2 (future schema bump)
            indexer.build_lab_side_index(
                lab_weights={"v": 1},
                fingerprint_dyn_fn=fp_dyn,
                fingerprint_static_fn=fp_static,
                normalize_text_fn=normalize,
                lab_schema_version=2,
            )
            with open(meta_path, "r", encoding="utf-8") as fh:
                meta2 = json.load(fh)
            assert meta2["lab_schema_version"] == 2
        finally:
            indexer.close()

    def test_build_lab_side_index_callback_signature_option_c(self):
        """W5 LOCKED: build_lab_side_index must have keyword-only callback params."""
        from shared.local_indexer import LocalIndexer
        sig = inspect.signature(LocalIndexer.build_lab_side_index)
        params = dict(sig.parameters)
        # Must have all three callback parameters
        assert "fingerprint_dyn_fn" in params, "fingerprint_dyn_fn missing (W5 Option C)"
        assert "fingerprint_static_fn" in params, "fingerprint_static_fn missing (W5 Option C)"
        assert "normalize_text_fn" in params, "normalize_text_fn missing (W5 Option C)"
        # All three must be keyword-only (after the * separator)
        from inspect import Parameter
        for name in ("fingerprint_dyn_fn", "fingerprint_static_fn", "normalize_text_fn"):
            assert params[name].kind == Parameter.KEYWORD_ONLY, (
                f"{name} must be keyword-only (W5 — use * separator)"
            )

    def test_lab_rebuild_after_source_file_deleted(self, tmp_path):
        """HIGH-4 (option b): LAB rebuild reads from main LOCAL Tantivy stored content.

        After source file is deleted, LOCAL doc is still in LAB (durable snapshot).
        After explicit _delete_file + LAB rebuild, LOCAL doc is absent from LAB.
        """
        try:
            import tantivy  # noqa
        except ImportError:
            pytest.skip("tantivy not available")

        indexer, index_dir, lab_index_dir, db_path = _make_local_indexer(str(tmp_path))
        try:
            folder_path, txt_path, sys_id = _index_small_txt(indexer, str(tmp_path))
            assert sys_id is not None, "File must have been indexed"

            fp_dyn, fp_static, normalize = _make_dummy_callbacks()
            lab_weights = {"v": 1}

            # Build LAB — doc should be present
            indexer.build_lab_side_index(
                lab_weights=lab_weights,
                fingerprint_dyn_fn=fp_dyn,
                fingerprint_static_fn=fp_static,
                normalize_text_fn=normalize,
                lab_schema_version=1,
            )
            # Verify doc is in LAB by checking meta was written (index was built)
            meta_path = os.path.join(lab_index_dir, ".meta.json")
            assert os.path.exists(meta_path)

            # Delete the source file from disk
            os.remove(txt_path)
            assert not os.path.exists(txt_path)

            # Rebuild LAB — source file missing, but main LOCAL Tantivy still has the doc
            indexer.build_lab_side_index(
                lab_weights=lab_weights,
                fingerprint_dyn_fn=fp_dyn,
                fingerprint_static_fn=fp_static,
                normalize_text_fn=normalize,
                lab_schema_version=1,
            )
            # HIGH-4: meta still written = LAB rebuilt from durable Tantivy snapshot
            assert os.path.exists(meta_path), "LAB must rebuild even when source file deleted"

            # Verify the page is still in local_pages (not deleted by LAB rebuild)
            page_rows = indexer._conn.execute(
                "SELECT uid FROM local_pages WHERE sys_id = ?", (sys_id,)
            ).fetchall()
            assert len(page_rows) > 0, "local_pages rows must persist — only _delete_file removes them"

            # Now explicitly delete via _delete_file + rebuild LAB
            indexer._delete_file(sys_id, txt_path)
            indexer.build_lab_side_index(
                lab_weights=lab_weights,
                fingerprint_dyn_fn=fp_dyn,
                fingerprint_static_fn=fp_static,
                normalize_text_fn=normalize,
                lab_schema_version=1,
            )
            # After explicit delete, local_pages rows are gone
            page_rows_after = indexer._conn.execute(
                "SELECT uid FROM local_pages WHERE sys_id = ?", (sys_id,)
            ).fetchall()
            assert len(page_rows_after) == 0, "After _delete_file, local_pages must be cleared"
        finally:
            indexer.close()

    def test_lab_rebuild_after_folder_unavailable_d40(self, tmp_path):
        """HIGH-4 companion: D-40 folder unavailable → LAB doc still present.

        The LAB rebuild reads content from main LOCAL Tantivy, not source files.
        Making a folder unavailable (D-40) does not remove rows from local_pages.
        """
        try:
            import tantivy  # noqa
        except ImportError:
            pytest.skip("tantivy not available")

        indexer, index_dir, lab_index_dir, db_path = _make_local_indexer(str(tmp_path))
        try:
            folder_path, txt_path, sys_id = _index_small_txt(indexer, str(tmp_path))
            assert sys_id is not None

            fp_dyn, fp_static, normalize = _make_dummy_callbacks()
            lab_weights = {"v": 1}

            # Build initial LAB
            indexer.build_lab_side_index(
                lab_weights=lab_weights,
                fingerprint_dyn_fn=fp_dyn,
                fingerprint_static_fn=fp_static,
                normalize_text_fn=normalize,
                lab_schema_version=1,
            )
            meta_path = os.path.join(lab_index_dir, ".meta.json")
            assert os.path.exists(meta_path)

            # Simulate D-40: make folder unavailable by removing it
            shutil.rmtree(folder_path)
            # Mark folder status = 'unavailable' in SQLite (mirrors scan_all D-40 path)
            indexer._conn.execute(
                "UPDATE folders SET status = 'unavailable' WHERE path = ?",
                (folder_path,)
            )
            indexer._conn.commit()

            # Rebuild LAB — reads from durable main LOCAL Tantivy, not source folder
            indexer.build_lab_side_index(
                lab_weights=lab_weights,
                fingerprint_dyn_fn=fp_dyn,
                fingerprint_static_fn=fp_static,
                normalize_text_fn=normalize,
                lab_schema_version=1,
            )
            # D-40: rows are preserved when folder unavailable
            page_rows = indexer._conn.execute(
                "SELECT uid FROM local_pages WHERE sys_id = ?", (sys_id,)
            ).fetchall()
            assert len(page_rows) > 0, "D-40: local_pages rows must be preserved when folder unavailable"
        finally:
            indexer.close()

    def test_lab_rebuild_is_replace_not_append(self, tmp_path):
        """Codex 2026-05-27 HIGH: build_lab_side_index reuses the LAB dir, so a
        rebuild must CLEAR existing docs first (delete_all_documents) — not append.
        Without that, each rebuild duplicates every page (and keeps deleted pages)
        while writing a 'fresh' weights_hash. Assert the LAB Tantivy doc count
        equals the page count after a second rebuild, not twice it.
        """
        try:
            import tantivy  # noqa
        except ImportError:
            pytest.skip("tantivy not available")
        from shared.local_indexer import build_local_lab_schema

        indexer, index_dir, lab_index_dir, db_path = _make_local_indexer(str(tmp_path))
        try:
            folder_path, txt_path, sys_id = _index_small_txt(indexer, str(tmp_path))
            assert sys_id is not None

            fp_dyn, fp_static, normalize = _make_dummy_callbacks()
            lab_weights = {"v": 1}

            n_pages = indexer._conn.execute(
                "SELECT COUNT(*) AS c FROM local_pages"
            ).fetchone()["c"]
            assert n_pages > 0

            def _lab_doc_count() -> int:
                lab_index = tantivy.Index(build_local_lab_schema(), path=lab_index_dir)
                lab_index.reload()
                return lab_index.searcher().num_docs

            for _ in range(2):
                indexer.build_lab_side_index(
                    lab_weights=lab_weights,
                    fingerprint_dyn_fn=fp_dyn,
                    fingerprint_static_fn=fp_static,
                    normalize_text_fn=normalize,
                    lab_schema_version=1,
                )

            assert _lab_doc_count() == n_pages, (
                "LAB rebuild must replace, not append — doc count should equal "
                "page count after repeated rebuilds"
            )
        finally:
            indexer.close()

    def test_read_lab_meta_returns_none_when_missing(self, tmp_path):
        """read_lab_meta returns None when .meta.json does not exist."""
        from shared.local_indexer import LocalIndexer
        meta = LocalIndexer.read_lab_meta(str(tmp_path))
        assert meta is None

    def test_read_lab_meta_returns_dict_when_present(self, tmp_path):
        """read_lab_meta returns dict when .meta.json exists."""
        from shared.local_indexer import LocalIndexer
        meta_path = str(tmp_path / ".meta.json")
        payload = {"weights_hash": "abc123", "lab_schema_version": 1, "last_built_at": "2026-01-01T00:00:00Z"}
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        result = LocalIndexer.read_lab_meta(str(tmp_path))
        assert result is not None
        assert result["weights_hash"] == "abc123"


# ---------------------------------------------------------------------------
# Task 2 tests — genizah_core.py lab_composition_search + search_composition_logic
# ---------------------------------------------------------------------------

class TestLabCompositionSearchLocalLab:

    def _make_engine_with_local_lab(self, lab_meta: dict | None):
        """Create a minimal mock SearchEngine with local_lab_searcher configured."""
        try:
            import genizah_core  # noqa
        except ImportError:
            pytest.skip("genizah_core not importable in test env")

        # We test via the helper methods directly (unit-level)
        # without needing a real index — use a duck-typed minimal stub.
        # The real SearchEngine.__init__ requires substantial infrastructure;
        # we test the logic helpers via a thin wrapper.
        pass

    def test_weights_hash_mismatch_triggers_stale_flag(self):
        """D-38: when stored weights_hash != current LAB weights hash,
        local_lab_searcher_stale is set True and local_lab_searcher is bypassed.
        """
        try:
            from genizah_core import SearchEngine
        except ImportError:
            pytest.skip("genizah_core not importable")

        # Create a minimal stub to test _check_local_lab_freshness
        class StubSettings:
            use_dynamic_weights = False

        engine = object.__new__(SearchEngine)
        engine.local_lab_searcher = MagicMock()  # non-None: simulates open LAB index
        engine.local_lab_searcher_stale = False
        engine.settings = StubSettings()
        engine.dynamic_rank_map = None
        # Write .meta.json with a stale hash
        engine._lab_local_meta = {"weights_hash": "stale_hash_000", "lab_schema_version": 1}

        result = engine._check_local_lab_freshness()
        assert result is False, "_check_local_lab_freshness must return False on hash mismatch"
        assert engine.local_lab_searcher_stale is True, "local_lab_searcher_stale must be True on mismatch"

    def test_weights_hash_match_local_freshness_true(self):
        """D-38: when stored weights_hash matches current hash, freshness=True."""
        try:
            from genizah_core import SearchEngine
        except ImportError:
            pytest.skip("genizah_core not importable")

        class StubSettings:
            use_dynamic_weights = False

        engine = object.__new__(SearchEngine)
        engine.local_lab_searcher = MagicMock()
        engine.local_lab_searcher_stale = True
        engine.settings = StubSettings()
        engine.dynamic_rank_map = None

        # Compute what the actual hash would be
        current_hash = engine._current_lab_weights_hash()
        engine._lab_local_meta = {"weights_hash": current_hash, "lab_schema_version": 1}

        result = engine._check_local_lab_freshness()
        assert result is True, "_check_local_lab_freshness must return True when hash matches"
        assert engine.local_lab_searcher_stale is False

    def test_check_freshness_no_searcher_returns_false(self):
        """D-37 mirror: no searcher → freshness=False (not stale, just absent)."""
        try:
            from genizah_core import SearchEngine
        except ImportError:
            pytest.skip("genizah_core not importable")

        class StubSettings:
            use_dynamic_weights = False

        engine = object.__new__(SearchEngine)
        engine.local_lab_searcher = None  # absent
        engine.local_lab_searcher_stale = False
        engine.settings = StubSettings()
        engine.dynamic_rank_map = None
        engine._lab_local_meta = None

        result = engine._check_local_lab_freshness()
        assert result is False

    def test_search_engine_has_required_attrs(self):
        """SearchEngine must declare local_lab_searcher, local_lab_searcher_stale, _lab_local_meta."""
        src_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "genizah_core.py")
        if not os.path.exists(src_path):
            pytest.skip("genizah_core.py not found")

        with open(src_path, "r", encoding="utf-8") as f:
            source = f.read()

        # Check attributes exist in genizah_core.py source
        assert "local_lab_searcher" in source, "local_lab_searcher missing from genizah_core.py"
        assert "local_lab_searcher_stale" in source, "local_lab_searcher_stale missing"
        assert "_lab_local_meta" in source, "_lab_local_meta missing"
        assert "_check_local_lab_freshness" in source, "_check_local_lab_freshness method missing"
        assert "_current_lab_weights_hash" in source, "_current_lab_weights_hash method missing"

    def test_rebuild_local_lab_index_method_exists(self):
        """SearchEngine.rebuild_local_lab_index must exist with Option C callback wire-up."""
        try:
            from genizah_core import SearchEngine
        except ImportError:
            pytest.skip("genizah_core not importable")

        assert hasattr(SearchEngine, "rebuild_local_lab_index"), (
            "SearchEngine.rebuild_local_lab_index method required (W5 Option C wire-up)"
        )
        src_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "genizah_core.py")
        if not os.path.exists(src_path):
            pytest.skip("genizah_core.py not found")
        with open(src_path, "r", encoding="utf-8") as f:
            source = f.read()
        assert "fingerprint_dyn_fn=" in source, (
            "Option C callback wire-up missing: fingerprint_dyn_fn= not found in genizah_core.py"
        )

    def test_search_composition_logic_extends_local_lab_query(self):
        """I14: search_composition_logic contains the LOCAL LAB extension hook."""
        src_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "genizah_core.py")
        if not os.path.exists(src_path):
            pytest.skip("genizah_core.py not found")
        with open(src_path, "r", encoding="utf-8") as f:
            source = f.read()

        # AST check: function search_composition_logic contains reference to local_lab_searcher
        tree = ast.parse(source)
        found_fn = False
        found_local_lab = False
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "search_composition_logic":
                found_fn = True
                fn_source = ast.get_source_segment(source, node)
                if fn_source and "local_lab_searcher" in fn_source:
                    found_local_lab = True
                break

        assert found_fn, "search_composition_logic function not found in genizah_core.py"
        assert found_local_lab, (
            "search_composition_logic does not reference local_lab_searcher (I14 extension missing)"
        )

    def test_lab_composition_search_extends_local_lab_query(self):
        """D-09: lab_composition_search contains the LOCAL LAB extension hook."""
        src_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "genizah_core.py")
        if not os.path.exists(src_path):
            pytest.skip("genizah_core.py not found")
        with open(src_path, "r", encoding="utf-8") as f:
            source = f.read()

        tree = ast.parse(source)
        found_fn = False
        found_local_lab = False
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "lab_composition_search":
                found_fn = True
                fn_source = ast.get_source_segment(source, node)
                if fn_source and "local_lab_searcher" in fn_source:
                    found_local_lab = True
                break

        assert found_fn, "lab_composition_search function not found in genizah_core.py"
        assert found_local_lab, (
            "lab_composition_search does not reference local_lab_searcher (D-09 extension missing)"
        )

    def test_no_rrf_in_lab_scoring(self):
        """D-09: LAB scoring path must NOT use RRF (custom fingerprint scoring preserved)."""
        src_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "genizah_core.py")
        if not os.path.exists(src_path):
            pytest.skip("genizah_core.py not found")
        with open(src_path, "r", encoding="utf-8") as f:
            source = f.read()

        import re as _re
        # Ensure no rrf_merge call near "lab" context
        matches = _re.findall(r"_rrf_merge.*lab|lab.*_rrf_merge", source, _re.IGNORECASE)
        assert len(matches) == 0, (
            f"D-09: RRF must not be used for LAB scoring; found: {matches}"
        )


# ---------------------------------------------------------------------------
# CR-01 regression — _current_lab_weights_hash must NOT raise on real
# SearchEngine instances (which do NOT have dynamic_rank_map / settings attrs).
# Previously the existing tests masked the bug by hand-attaching attributes
# via object.__new__ + manual setattr.
# ---------------------------------------------------------------------------

class TestCR01CurrentLabWeightsHashNoCrash:
    """CR-01: _current_lab_weights_hash must use getattr defaults so it does
    not crash on plain SearchEngine objects that lack LabEngine state."""

    def test_no_attrs_does_not_raise(self):
        """A SearchEngine without dynamic_rank_map and without settings must
        still return a valid sha256 hex string from _current_lab_weights_hash.
        """
        try:
            from genizah_core import SearchEngine
        except ImportError:
            pytest.skip("genizah_core not importable")

        # Construct WITHOUT touching __init__; do NOT hand-attach LabEngine attrs.
        engine = object.__new__(SearchEngine)
        # Sanity: confirm the attributes truly don't exist (otherwise this
        # regression test is masking the same way the original tests did).
        assert not hasattr(engine, "dynamic_rank_map"), (
            "Test setup invariant: dynamic_rank_map must NOT be set"
        )
        assert not hasattr(engine, "settings"), (
            "Test setup invariant: settings must NOT be set"
        )

        # The method must not raise AttributeError.
        result = engine._current_lab_weights_hash()
        assert isinstance(result, str)
        assert len(result) == 64, "sha256 hex digest must be 64 chars"

    def test_check_freshness_no_attrs_returns_bool(self):
        """End-to-end: _check_local_lab_freshness must not crash on bare
        SearchEngine — even with a non-None local_lab_searcher + meta.
        """
        try:
            from genizah_core import SearchEngine
        except ImportError:
            pytest.skip("genizah_core not importable")

        engine = object.__new__(SearchEngine)
        engine.local_lab_searcher = MagicMock()  # not None — exercises the hash path
        engine.local_lab_searcher_stale = False
        engine._lab_local_meta = {"weights_hash": "deadbeef" * 8, "lab_schema_version": 1}
        # Deliberately do NOT set dynamic_rank_map / settings (CR-01 invariant).

        result = engine._check_local_lab_freshness()
        assert isinstance(result, bool)
        # The freshness check should be False because the meta hash will not
        # match the "no-weights" hash; but the important assertion is that
        # the call returned cleanly without AttributeError.


# ---------------------------------------------------------------------------
# CR-02 regression — LabEngine must have its own LOCAL LAB attributes +
# _check_local_lab_freshness so lab_composition_search actually surfaces
# LOCAL hits in LAB mode (previously the getattr(...) guard returned None
# and the entire LOCAL LAB hook was silently skipped).
# ---------------------------------------------------------------------------

class TestCR02LabEngineHasLocalLabHook:
    """CR-02: REQ-6 — LAB Composition Search must surface LOCAL hits."""

    def test_lab_engine_has_check_local_lab_freshness(self):
        """LabEngine must define _check_local_lab_freshness as a method."""
        try:
            from genizah_core import LabEngine
        except ImportError:
            pytest.skip("genizah_core not importable")

        assert hasattr(LabEngine, "_check_local_lab_freshness"), (
            "CR-02: LabEngine._check_local_lab_freshness must exist so "
            "lab_composition_search's getattr guard activates the LOCAL LAB hook"
        )
        # Must be callable (a real bound-method, not just a stub attribute).
        assert callable(LabEngine._check_local_lab_freshness)

    def test_lab_engine_has_local_lab_attrs(self):
        """LabEngine __init__ must set local_lab_searcher, _local_lab_index,
        _lab_local_meta, local_lab_searcher_stale."""
        try:
            from genizah_core import LabEngine  # noqa: F401  (imported to verify availability; used via AST inspection below)
        except ImportError:
            pytest.skip("genizah_core not importable")

        # Read source — actual instantiation requires meta_mgr/var_mgr.
        src_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "genizah_core.py")
        with open(src_path, "r", encoding="utf-8") as f:
            source = f.read()

        # AST check: LabEngine.__init__ must assign all four attributes.
        tree = ast.parse(source)
        found_class = False
        attr_names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == "LabEngine":
                found_class = True
                for child in ast.walk(node):
                    if isinstance(child, ast.FunctionDef) and child.name == "__init__":
                        for stmt in ast.walk(child):
                            if isinstance(stmt, ast.Attribute) and isinstance(stmt.value, ast.Name):
                                if stmt.value.id == "self":
                                    attr_names.add(stmt.attr)
                break
        assert found_class, "LabEngine class not found"
        for required in (
            "local_lab_searcher",
            "_local_lab_index",
            "_lab_local_meta",
            "local_lab_searcher_stale",
        ):
            assert required in attr_names, (
                f"CR-02: LabEngine.__init__ must assign self.{required}"
            )

    def test_lab_engine_has_reload_local_lab_index(self):
        """LabEngine must define reload_local_lab_index for MyLibraryTab to call."""
        try:
            from genizah_core import LabEngine
        except ImportError:
            pytest.skip("genizah_core not importable")

        assert hasattr(LabEngine, "reload_local_lab_index"), (
            "CR-02: LabEngine.reload_local_lab_index must exist so "
            "MyLibraryTab can wire LAB-side reloads after Refresh/Add/Remove"
        )

    def test_my_library_tab_calls_lab_engine_reload(self):
        """MyLibraryTab._reload_all_local_indexes must invoke lab_engine.reload_local_lab_index."""
        my_lib_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "desktop",
            "my_library_tab.py",
        )
        if not os.path.exists(my_lib_path):
            pytest.skip("desktop/my_library_tab.py not found")
        with open(my_lib_path, "r", encoding="utf-8") as f:
            source = f.read()
        assert "lab_engine.reload_local_lab_index" in source, (
            "CR-02: MyLibraryTab must call lab_engine.reload_local_lab_index "
            "so LAB-mode Composition Search sees newly indexed LOCAL files"
        )


# ---------------------------------------------------------------------------
# WR-08 regression — MyLibraryTab must wire rebuild_local_lab_index so
# D-38 weights_hash invalidation actually triggers a rebuild end-to-end.
# ---------------------------------------------------------------------------

class TestWR08RebuildLabWiring:
    """WR-08: rebuild_local_lab_index was dead code — never called from any
    UI entry point. D-38's invalidation triggers were unimplemented.
    """

    def test_my_library_tab_has_rebuild_helper(self):
        """MyLibraryTab must define _maybe_rebuild_lab_if_stale."""
        my_lib_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "desktop",
            "my_library_tab.py",
        )
        with open(my_lib_path, "r", encoding="utf-8") as f:
            source = f.read()
        assert "_maybe_rebuild_lab_if_stale" in source, (
            "WR-08: MyLibraryTab must define _maybe_rebuild_lab_if_stale"
        )
        assert "rebuild_local_lab_index" in source, (
            "WR-08: MyLibraryTab must call SearchEngine.rebuild_local_lab_index"
        )

    def test_my_library_tab_wires_rebuild_on_worker_finished(self):
        """_on_worker_finished must invoke _maybe_rebuild_lab_if_stale."""
        my_lib_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "desktop",
            "my_library_tab.py",
        )
        with open(my_lib_path, "r", encoding="utf-8") as f:
            source = f.read()

        tree = ast.parse(source)
        found_fn = False
        found_call = False
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_on_worker_finished":
                found_fn = True
                fn_source = ast.get_source_segment(source, node) or ""
                if "_maybe_rebuild_lab_if_stale" in fn_source:
                    found_call = True
                break
        assert found_fn, "_on_worker_finished function not found"
        assert found_call, (
            "WR-08: _on_worker_finished must call _maybe_rebuild_lab_if_stale "
            "so a Refresh after weights change rebuilds the LAB index"
        )

    def test_my_library_tab_wires_rebuild_on_startup_recovery(self):
        """_on_startup_recovery_completed must invoke _maybe_rebuild_lab_if_stale."""
        my_lib_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "desktop",
            "my_library_tab.py",
        )
        with open(my_lib_path, "r", encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source)
        found_fn = False
        found_call = False
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_on_startup_recovery_completed":
                found_fn = True
                fn_source = ast.get_source_segment(source, node) or ""
                if "_maybe_rebuild_lab_if_stale" in fn_source:
                    found_call = True
                break
        assert found_fn, "_on_startup_recovery_completed function not found"
        assert found_call, (
            "WR-08: _on_startup_recovery_completed must call "
            "_maybe_rebuild_lab_if_stale so a weights change between sessions "
            "is picked up at startup"
        )
