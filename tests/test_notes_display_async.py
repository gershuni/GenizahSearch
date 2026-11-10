"""Regression coverage for off-loop comment reads in the notes UI."""

import asyncio
import inspect


def test_async_comment_read_captures_authenticated_context(monkeypatch):
    from web.components import notes_display

    reader_client = object()
    auth_reads = []
    query_calls = []
    offloads = []

    def get_user_id(cls):
        auth_reads.append("read")
        return "user-1"

    def get_comments(**kwargs):
        query_calls.append(kwargs)
        if kwargs.get("is_public"):
            return [{"id": "public", "created_at": "2026-01-01", "profiles": {}}]
        return [{"id": "private", "created_at": "2026-01-02", "profiles": {}}]

    async def io_bound(callback):
        offloads.append(True)
        return callback()

    monkeypatch.setattr(
        notes_display.GlobalAuthState, "get_user_id", classmethod(get_user_id)
    )
    monkeypatch.setattr(notes_display, "get_user_client", lambda: reader_client)
    monkeypatch.setattr(notes_display, "get_comments", get_comments)
    monkeypatch.setattr(notes_display.run, "io_bound", io_bound)

    comments = asyncio.run(
        notes_display.fetch_document_comments_async("doc-1", 3, ie_id="ie-1")
    )

    assert auth_reads == ["read"]
    assert offloads == [True]
    assert [comment["id"] for comment in comments] == ["private", "public"]
    assert len(query_calls) == 2
    assert all(call["client"] is reader_client for call in query_calls)
    assert query_calls[1]["author_id"] == "user-1"


def test_async_comment_read_keeps_anonymous_query_public_only(monkeypatch):
    from web.components import notes_display

    reader_client = object()
    query_calls = []

    monkeypatch.setattr(
        notes_display.GlobalAuthState,
        "get_user_id",
        classmethod(lambda cls: None),
    )
    monkeypatch.setattr(notes_display, "get_user_client", lambda: reader_client)
    monkeypatch.setattr(
        notes_display,
        "get_comments",
        lambda **kwargs: query_calls.append(kwargs) or [],
    )

    async def io_bound(callback):
        return callback()

    monkeypatch.setattr(notes_display.run, "io_bound", io_bound)

    assert asyncio.run(notes_display.fetch_document_comments_async("doc-2")) == []
    assert len(query_calls) == 1
    assert query_calls[0]["is_public"] is True
    assert query_calls[0]["client"] is reader_client


def test_notes_ui_has_no_direct_synchronous_comment_reads():
    from web.components import notes_display

    source = inspect.getsource(notes_display)

    # One definition plus the one worker-thread call inside the async wrapper.
    assert source.count("fetch_document_comments(") == 2
    assert "comments = await fetch_document_comments_async(" in source
    assert "lambda: asyncio.create_task(_safe_check())" in source
