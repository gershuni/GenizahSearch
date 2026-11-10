"""Regression coverage for off-loop version-selector database reads."""

import asyncio
import inspect


def test_page_versions_async_passes_captured_client_to_worker(monkeypatch):
    from web.components import version_selector

    reader_client = object()
    calls = []

    monkeypatch.setattr(version_selector, "get_user_client", lambda: reader_client)
    monkeypatch.setattr(
        version_selector,
        "fetch_page_versions",
        lambda sys_id, page_num, client=None: calls.append(
            (sys_id, page_num, client)
        )
        or {"total": 0},
    )

    async def io_bound(callback):
        return callback()

    monkeypatch.setattr(version_selector.run, "io_bound", io_bound)

    result = asyncio.run(
        version_selector.fetch_page_versions_async("doc-1", 4)
    )

    assert result == {"total": 0}
    assert calls == [("doc-1", 4, reader_client)]


def test_version_menu_data_captures_authenticated_context(monkeypatch):
    from web.components import version_selector

    reader_client = object()
    auth_reads = []
    approved_calls = []
    pending_calls = []

    monkeypatch.setattr(
        version_selector.GlobalAuthState,
        "is_logged_in",
        classmethod(lambda cls: True),
    )

    def get_user_id(cls):
        auth_reads.append("read")
        return "user-1"

    monkeypatch.setattr(
        version_selector.GlobalAuthState, "get_user_id", classmethod(get_user_id)
    )
    monkeypatch.setattr(version_selector, "get_user_client", lambda: reader_client)
    monkeypatch.setattr(
        version_selector,
        "fetch_document_corrections",
        lambda document_id, page_number, client=None: approved_calls.append(
            (document_id, page_number, client)
        )
        or [{"id": "approved"}],
    )
    monkeypatch.setattr(
        version_selector,
        "get_pending_corrections_for_page",
        lambda **kwargs: pending_calls.append(kwargs) or [{"id": "pending"}],
    )

    async def io_bound(callback):
        return callback()

    monkeypatch.setattr(version_selector.run, "io_bound", io_bound)

    approved, pending = asyncio.run(
        version_selector.fetch_version_menu_data_async("doc-2", 5)
    )

    assert auth_reads == ["read"]
    assert approved == [{"id": "approved"}]
    assert pending == [{"id": "pending"}]
    assert approved_calls == [("doc-2", 5, reader_client)]
    assert pending_calls == [{
        "client": reader_client,
        "sys_id": "doc-2",
        "page_number": 5,
        "user_id": "user-1",
    }]


def test_version_selector_ui_awaits_database_reads():
    from web.components import version_selector

    source = inspect.getsource(version_selector.create_version_selector)

    assert "versions_data = await fetch_page_versions_async(" in source
    assert "await fetch_version_menu_data_async(" in source
    assert "lambda: asyncio.create_task(_safe_load())" in source
