# -*- coding: utf-8 -*-
"""Tests for web/joins_executor.py — WebSearchExecutor Protocol compliance.

Covers:
  - isinstance(WebSearchExecutor(), SearchExecutor) (runtime @runtime_checkable check)
  - inspect.signature compatibility with the Protocol for all four methods (LOW-7)
  - Graceful fallback to []/None/('','')/'' when the engine raises
  - Graceful fallback when execute_search returns None (the `or []` guard)
  - Happy-path keyword-arg passthrough (corpus_scope, text_position, responsa_options)

All tests are pure-Python (no NiceGUI runtime — the adapter never touches app.storage or ui).
State is monkeypatched at the module-level `state` singleton (web.state.state).
"""

import inspect


from shared.joins_lab import SearchExecutor
from web.joins_executor import WebSearchExecutor
import web.state as _state_module


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _BrokenSearcher:
    """Fake searcher whose every method raises a generic Exception."""

    def execute_search(self, *args, **kwargs):
        raise Exception("engine down")

    def get_browse_page(self, *args, **kwargs):
        raise Exception("engine down")


class _BrokenMetaMgr:
    """Fake meta manager whose every method raises a generic Exception."""

    def get_meta_for_id(self, sys_id):
        raise Exception("meta down")

    def get_library_for_id(self, sys_id):
        raise Exception("meta down")


class _NoneSearcher:
    """Fake searcher whose execute_search returns None (exercises the `or []` guard)."""

    def execute_search(self, *args, **kwargs):
        return None

    def get_browse_page(self, *args, **kwargs):
        return {"uid": "test_uid"}


class _RecordingSearcher:
    """Fake searcher that records kwargs and returns canned values."""

    def __init__(self, results=None):
        self._results = results if results is not None else [{"display": {"id": "99"}}]
        self.last_kwargs: dict = {}

    def execute_search(
        self,
        query_str,
        mode,
        gap,
        progress_callback=None,
        exclude_words=None,
        responsa_options=None,
        restrict_sys_ids=None,
        text_position=None,
        corpus_scope="all",
    ):
        self.last_kwargs = {
            "query_str": query_str,
            "mode": mode,
            "gap": gap,
            "progress_callback": progress_callback,
            "exclude_words": exclude_words,
            "responsa_options": responsa_options,
            "restrict_sys_ids": restrict_sys_ids,
            "text_position": text_position,
            "corpus_scope": corpus_scope,
        }
        return self._results

    def get_browse_page(self, sys_id, **kwargs):
        return None


class _RecordingMetaMgr:
    """Fake meta manager with canned values."""

    def get_meta_for_id(self, sys_id) -> tuple:
        return ("T-S 12.1", "Test Title")

    def get_library_for_id(self, sys_id) -> str:
        return "CUL"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestProtocolCompliance:
    """isinstance + inspect.signature compliance (LOW-7)."""

    def test_isinstance_satisfies_protocol(self):
        """WebSearchExecutor() satisfies the @runtime_checkable SearchExecutor Protocol."""
        executor = WebSearchExecutor()
        assert isinstance(executor, SearchExecutor), (
            "WebSearchExecutor() must satisfy isinstance(executor, SearchExecutor)"
        )

    def test_execute_search_signature_matches_protocol(self):
        """execute_search parameter names must match the Protocol (LOW-7)."""
        adapter_sig = inspect.signature(WebSearchExecutor.execute_search)
        protocol_sig = inspect.signature(SearchExecutor.execute_search)

        adapter_params = [
            p for p in adapter_sig.parameters if p != "self"
        ]
        protocol_params = [
            p for p in protocol_sig.parameters if p != "self"
        ]
        assert adapter_params == protocol_params, (
            f"execute_search parameter mismatch:\n"
            f"  adapter : {adapter_params}\n"
            f"  protocol: {protocol_params}"
        )

    def test_get_browse_page_signature_matches_protocol(self):
        """get_browse_page parameter names must match the Protocol (LOW-7)."""
        adapter_sig = inspect.signature(WebSearchExecutor.get_browse_page)
        protocol_sig = inspect.signature(SearchExecutor.get_browse_page)

        adapter_params = [
            p for p in adapter_sig.parameters if p != "self"
        ]
        protocol_params = [
            p for p in protocol_sig.parameters if p != "self"
        ]
        assert adapter_params == protocol_params, (
            f"get_browse_page parameter mismatch:\n"
            f"  adapter : {adapter_params}\n"
            f"  protocol: {protocol_params}"
        )

    def test_get_meta_for_id_signature_matches_protocol(self):
        """get_meta_for_id parameter names must match the Protocol (LOW-7)."""
        adapter_sig = inspect.signature(WebSearchExecutor.get_meta_for_id)
        protocol_sig = inspect.signature(SearchExecutor.get_meta_for_id)

        adapter_params = [
            p for p in adapter_sig.parameters if p != "self"
        ]
        protocol_params = [
            p for p in protocol_sig.parameters if p != "self"
        ]
        assert adapter_params == protocol_params, (
            f"get_meta_for_id parameter mismatch:\n"
            f"  adapter : {adapter_params}\n"
            f"  protocol: {protocol_params}"
        )

    def test_get_library_for_id_signature_matches_protocol(self):
        """get_library_for_id parameter names must match the Protocol (LOW-7)."""
        adapter_sig = inspect.signature(WebSearchExecutor.get_library_for_id)
        protocol_sig = inspect.signature(SearchExecutor.get_library_for_id)

        adapter_params = [
            p for p in adapter_sig.parameters if p != "self"
        ]
        protocol_params = [
            p for p in protocol_sig.parameters if p != "self"
        ]
        assert adapter_params == protocol_params, (
            f"get_library_for_id parameter mismatch:\n"
            f"  adapter : {adapter_params}\n"
            f"  protocol: {protocol_params}"
        )


class TestGracefulFailure:
    """Return [] / None / ('','') / '' when the engine raises or returns None."""

    def test_execute_search_raises_returns_empty_list(self, monkeypatch):
        """execute_search returns [] when state.searcher raises Exception."""
        monkeypatch.setattr(_state_module.state, "searcher", _BrokenSearcher())
        executor = WebSearchExecutor()
        result = executor.execute_search("שלום", mode="exact", gap=0)
        assert result == [], f"Expected [], got {result!r}"

    def test_execute_search_returns_none_becomes_empty_list(self, monkeypatch):
        """execute_search returns [] when state.searcher.execute_search returns None."""
        monkeypatch.setattr(_state_module.state, "searcher", _NoneSearcher())
        executor = WebSearchExecutor()
        result = executor.execute_search("שלום", mode="exact", gap=0)
        assert result == [], f"Expected [] from None return, got {result!r}"

    def test_get_browse_page_raises_returns_none(self, monkeypatch):
        """get_browse_page returns None when state.searcher raises Exception."""
        monkeypatch.setattr(_state_module.state, "searcher", _BrokenSearcher())
        executor = WebSearchExecutor()
        result = executor.get_browse_page("990001234560205171")
        assert result is None, f"Expected None, got {result!r}"

    def test_get_meta_for_id_raises_returns_empty_tuple(self, monkeypatch):
        """get_meta_for_id returns ('','') when state.meta_mgr raises Exception."""
        monkeypatch.setattr(_state_module.state, "meta_mgr", _BrokenMetaMgr())
        executor = WebSearchExecutor()
        result = executor.get_meta_for_id("990001234560205171")
        assert result == ("", ""), f"Expected ('',''), got {result!r}"

    def test_get_library_for_id_raises_returns_empty_string(self, monkeypatch):
        """get_library_for_id returns '' when state.meta_mgr raises Exception."""
        monkeypatch.setattr(_state_module.state, "meta_mgr", _BrokenMetaMgr())
        executor = WebSearchExecutor()
        result = executor.get_library_for_id("990001234560205171")
        assert result == "", f"Expected '', got {result!r}"


class TestHappyPathForward:
    """Keyword arguments reach the engine unchanged."""

    def test_execute_search_passes_corpus_scope_and_text_position(self, monkeypatch):
        """corpus_scope and text_position keywords reach the fake searcher unchanged."""
        recording_searcher = _RecordingSearcher()
        monkeypatch.setattr(_state_module.state, "searcher", recording_searcher)
        executor = WebSearchExecutor()

        result = executor.execute_search(
            "קהלת",
            mode="exact",
            gap=0,
            corpus_scope="genizah",
            text_position="recto",
            responsa_options={"strict": True},
        )

        # Results should be the canned list
        assert result == recording_searcher._results

        # All kwargs must have been forwarded unchanged
        assert recording_searcher.last_kwargs["corpus_scope"] == "genizah", (
            "corpus_scope was not forwarded to the engine"
        )
        assert recording_searcher.last_kwargs["text_position"] == "recto", (
            "text_position was not forwarded to the engine"
        )
        assert recording_searcher.last_kwargs["responsa_options"] == {"strict": True}, (
            "responsa_options was not forwarded to the engine"
        )
        assert recording_searcher.last_kwargs["query_str"] == "קהלת"
        assert recording_searcher.last_kwargs["mode"] == "exact"
        assert recording_searcher.last_kwargs["gap"] == 0

    def test_get_meta_for_id_happy_path(self, monkeypatch):
        """get_meta_for_id returns the engine's (shelfmark, title) when healthy."""
        monkeypatch.setattr(_state_module.state, "meta_mgr", _RecordingMetaMgr())
        executor = WebSearchExecutor()
        shelf, title = executor.get_meta_for_id("990001234560205171")
        assert shelf == "T-S 12.1"
        assert title == "Test Title"

    def test_get_library_for_id_happy_path(self, monkeypatch):
        """get_library_for_id returns the engine's library_code when healthy."""
        monkeypatch.setattr(_state_module.state, "meta_mgr", _RecordingMetaMgr())
        executor = WebSearchExecutor()
        lib = executor.get_library_for_id("990001234560205171")
        assert lib == "CUL"
