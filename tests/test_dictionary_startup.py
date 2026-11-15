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


def test_the_verb_forms_are_built_in_the_background_where_a_kind_is_shown(monkeypatch, capsys):
    """Blau's and Friedman's verb forms (one to two seconds, about 75 MB) are built after startup, in
    the background, so the first reader's click does not wait for them. Only for a dictionary that is
    served and shows a kind; a failing build is reported and left to the first lookup."""
    import shared.arabic_service as arabic
    import shared.blau_service as blau
    import shared.friedman_service as friedman
    import web.main as wm

    built: list[str] = []

    class Served:
        unavailable_reason = ""

        def __init__(self, name, available=True, broken=False, preview=False):
            self.name, self.available, self.broken, self.preview = name, available, broken, preview

        def is_ready(self):
            return True

        def is_available(self):
            return self.available

        def served_to(self, editor):
            # Served to everyone, or to editors only (a preview): both get the forms.
            return self.available or (self.preview and editor)

        def verb_form_index(self):
            if self.broken:
                raise RuntimeError("no memory")
            built.append(self.name)
            return {"אסתדלו": ()}

    async def io_bound(function, *args, **kwargs):
        return function(*args, **kwargs)

    def run(blau_service, friedman_service, kinds="A,B,D"):
        built.clear()
        monkeypatch.setattr(blau, "get_blau_service", lambda: blau_service)
        monkeypatch.setattr(arabic, "get_arabic_service", lambda: Served("lane"))
        monkeypatch.setattr(friedman, "get_friedman_service", lambda: friedman_service)
        monkeypatch.setenv(blau.ENV_VERB_FORM_KINDS, kinds)
        monkeypatch.setenv(friedman.ENV_VERB_FORM_KINDS, kinds)

        async def startup():
            await wm.open_blau_sidecar_on_startup()
            await asyncio.gather(*wm._VERB_FORM_PREWARM)

        asyncio.run(startup())
        return list(built)

    monkeypatch.setattr(wm.run, "io_bound", io_bound)
    assert run(Served("blau"), Served("friedman")) == ["blau", "friedman"]
    assert "Blau verb forms built in the background (1 forms" in capsys.readouterr().out
    assert run(Served("blau"), Served("friedman", available=False)) == ["blau"], "Friedman not served"
    assert run(Served("blau"), Served("friedman", available=False, preview=True)) == ["blau", "friedman"], \
        "a dictionary switched to editors only is served to them: its forms are built too"
    assert run(Served("blau"), Served("friedman"), kinds="") == [], "no kind shown: nothing to build"
    assert run(Served("blau", broken=True), Served("friedman")) == ["friedman"]
    assert "Blau verb forms not built at startup (non-fatal): no memory" in capsys.readouterr().out


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
