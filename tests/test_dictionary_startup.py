# -*- coding: utf-8 -*-
"""web/main.py::open_blau_sidecar_on_startup opens EVERY dictionary sidecar at startup, each off
the event loop: Blau's, the Arabic (Lane) one and Friedman's, all shown in Blau's panel. Otherwise
the first /browse render would run a sidecar's quick_check and table loads on the loop (Codex on #480)."""
from __future__ import annotations

import asyncio


def test_the_startup_hook_opens_every_dictionary_off_the_loop(monkeypatch):
    import shared.arabic_service as arabic
    import shared.blau_service as blau
    import shared.friedman_service as friedman
    import web.main as wm

    opened, offloaded = [], []

    class Ready:
        unavailable_reason = ""

        def is_ready(self):
            return True

    def opener(name):
        def get():
            opened.append(name)
            return Ready()
        return get

    async def io_bound(function, *args, **kwargs):
        offloaded.append(function)
        return function(*args, **kwargs)

    monkeypatch.setattr(blau, "get_blau_service", opener("blau"))
    monkeypatch.setattr(arabic, "get_arabic_service", opener("lane"))
    monkeypatch.setattr(friedman, "get_friedman_service", opener("friedman"))
    monkeypatch.setattr(wm.run, "io_bound", io_bound)
    asyncio.run(wm.open_blau_sidecar_on_startup())
    assert opened == ["blau", "lane", "friedman"], opened
    assert len(offloaded) == 3, "each sidecar is opened through run.io_bound"


def test_a_failing_lane_sidecar_cannot_stop_the_app(monkeypatch, capsys):
    import shared.arabic_service as arabic
    import shared.blau_service as blau
    import web.main as wm

    class Ready:
        unavailable_reason = ""

        def is_ready(self):
            return True

    def broken():
        raise RuntimeError("disk gone")

    async def io_bound(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(blau, "get_blau_service", lambda: Ready())
    monkeypatch.setattr(arabic, "get_arabic_service", broken)
    monkeypatch.setattr(wm.run, "io_bound", io_bound)
    asyncio.run(wm.open_blau_sidecar_on_startup())
    assert "Lane sidecar check failed (non-fatal): disk gone" in capsys.readouterr().out
