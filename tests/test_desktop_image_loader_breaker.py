# -*- coding: utf-8 -*-
"""SEED-015 — desktop image fetches wire into the shared NLI circuit breaker
and the shared NLI host TLS policy.

Covers audit findings:
- #1  : the desktop image loader bypassed the Phase-98 NLI circuit breaker,
        waited the full 10/30s read timeout on every image during an outage,
        and never fed the breaker on failure.
- M2  : blanket ``verify=False`` disabled TLS for *any* host on the path and
        suppressed ``InsecureRequestWarning`` non-host-scoped.

The fuller image-loading unification (#2 / M1 / M4 — 4 divergent impls,
browser-side direct-NLI fallback, unified failure taxonomy) is DEFERRED to a
later milestone and intentionally NOT exercised here.

Breaker module state is reset before/after each test by the autouse fixture in
tests/conftest.py.

Style: no QApplication / widget construction. ``_download_bytes`` is a pure
method, so we build the loader via ``__new__`` to avoid the QThread teardown
race that CI-skips the other desktop Qt tests.
"""
from __future__ import annotations

import pathlib
import warnings

import pytest
import requests

import shared.nli_circuit_breaker as br
from shared import nli_fetch

# desktop.image_loader pulls PyQt6.QtGui (QImage). On a headless box where Qt is
# installed but QtGui can't load libGL.so.1, that import fails — so only the
# loader-dependent class below is guarded; the pure host-policy / redirect /
# source-guard tests run regardless. (Mirrors the repo's QT_AVAILABLE pattern.)
try:
    from desktop.image_loader import ImageLoaderThread

    IMAGE_LOADER_AVAILABLE = True
except Exception:  # pragma: no cover - environment-dependent (headless QtGui)
    ImageLoaderThread = None
    IMAGE_LOADER_AVAILABLE = False

NLI_URL = "https://iiif.nli.org.il/IIIFv21/FL12345/full/2000,/0/default.jpg"
ROSETTA_URL = (
    "https://rosetta.nli.org.il/delivery/DeliveryManagerServlet"
    "?dps_func=stream&dps_pid=FL12345"
)
EXT_URL = "https://cudl.lib.cam.ac.uk/content/images/foo.jpg"


class _Resp:
    """Minimal stand-in for requests.Response."""

    def __init__(self, status_code=200, content=b"IMG"):
        self.status_code = status_code
        self.content = content


# ---------------------------------------------------------------------------
# shared/nli_fetch.py — host detection + TLS policy
# ---------------------------------------------------------------------------
class TestHostPolicy:
    def test_is_nli_host(self):
        assert nli_fetch.is_nli_host(NLI_URL) is True
        assert nli_fetch.is_nli_host(ROSETTA_URL) is True
        assert nli_fetch.is_nli_host(EXT_URL) is False

    def test_is_nli_host_rejects_garbage(self):
        assert nli_fetch.is_nli_host("") is False
        assert nli_fetch.is_nli_host("not a url") is False

    def test_is_nli_host_exact_match_not_suffix(self):
        # A spoofed host that merely contains the NLI host as a prefix must NOT
        # be treated as NLI (this drives the TLS-verify decision — M2 security).
        assert nli_fetch.is_nli_host("https://iiif.nli.org.il.evil.com/x") is False

    def test_host_of_lowercases(self):
        assert nli_fetch.host_of("https://IIIF.NLI.ORG.IL/x") == "iiif.nli.org.il"
        assert nli_fetch.host_of("") == ""

    def test_verify_policy(self):
        assert nli_fetch.nli_verify_for(NLI_URL) is False
        assert nli_fetch.nli_verify_for(ROSETTA_URL) is False
        assert nli_fetch.nli_verify_for(EXT_URL) is True


# ---------------------------------------------------------------------------
# shared/nli_fetch.py — nli_image_get verify + host-scoped warning suppression
# ---------------------------------------------------------------------------
class TestNliImageGet:
    def _patch_get(self, monkeypatch):
        captured = {}

        def fake_get(url, **kw):
            captured["url"] = url
            captured["verify"] = kw.get("verify")
            # Snapshot the warning filters active *during* the call.
            captured["filters"] = list(warnings.filters)
            return _Resp(200)

        monkeypatch.setattr(nli_fetch.requests, "get", fake_get)
        return captured

    def test_nli_host_verify_false(self, monkeypatch):
        cap = self._patch_get(monkeypatch)
        nli_fetch.nli_image_get(NLI_URL, headers={}, timeout=(3, 5))
        assert cap["verify"] is False

    def test_non_nli_host_verify_true(self, monkeypatch):
        cap = self._patch_get(monkeypatch)
        nli_fetch.nli_image_get(EXT_URL, headers={}, timeout=(3, 5))
        assert cap["verify"] is True

    @pytest.mark.skipif(
        nli_fetch.InsecureRequestWarning is None,
        reason="InsecureRequestWarning unavailable in this packaging",
    )
    def test_warning_suppression_is_host_scoped_not_global(self, monkeypatch):
        cap = self._patch_get(monkeypatch)
        before = list(warnings.filters)
        nli_fetch.nli_image_get(NLI_URL, headers={}, timeout=(3, 5))
        # During the NLI call an ignore filter for InsecureRequestWarning is active...
        assert any(
            f[0] == "ignore" and f[2] is nli_fetch.InsecureRequestWarning
            for f in cap["filters"]
        ), "InsecureRequestWarning must be suppressed during the NLI fetch"
        # ...and it is torn down afterwards (catch_warnings restores filters) —
        # the suppression is NOT a global process-wide mutation.
        assert warnings.filters == before

    @pytest.mark.skipif(
        nli_fetch.InsecureRequestWarning is None,
        reason="InsecureRequestWarning unavailable in this packaging",
    )
    def test_no_suppression_for_non_nli_host(self, monkeypatch):
        cap = self._patch_get(monkeypatch)
        nli_fetch.nli_image_get(EXT_URL, headers={}, timeout=(3, 5))
        assert not any(
            f[0] == "ignore" and f[2] is nli_fetch.InsecureRequestWarning
            for f in cap["filters"]
        ), "non-NLI hosts must keep TLS verification and not suppress warnings"


class _RedirectResp:
    """Stand-in for a requests redirect response (allow_redirects=False)."""

    def __init__(self, location, status_code=302):
        self.status_code = status_code
        self.headers = {"location": location}
        self.is_redirect = True
        self.content = b""
        self.closed = False
        # requests sets resp.next to the PreparedRequest for the next hop.
        self.next = type("_PR", (), {"url": location})()

    def close(self):
        self.closed = True


class _OkResp:
    """Stand-in for a terminal 200 requests response (allow_redirects=False)."""

    def __init__(self, content=b"IMG"):
        self.status_code = 200
        self.headers = {}
        self.is_redirect = False
        self.next = None
        self.content = content

    def close(self):
        pass


class TestNliImageGetRedirects:
    """The TLS allowlist must not be escapable via a redirect (Codex bot P2)."""

    def _script_get(self, monkeypatch, script):
        calls = []

        def fake_get(url, **kw):
            calls.append({"url": url, "verify": kw.get("verify"),
                          "allow_redirects": kw.get("allow_redirects")})
            return script[url]

        monkeypatch.setattr(nli_fetch.requests, "get", fake_get)
        return calls

    def test_redirect_from_nli_to_external_reverifies_per_hop(self, monkeypatch):
        ext = "https://evil.example.com/y"
        calls = self._script_get(
            monkeypatch, {NLI_URL: _RedirectResp(ext), ext: _OkResp(b"DATA")}
        )
        resp = nli_fetch.nli_image_get(NLI_URL, headers={}, timeout=(3, 5), stream=True)
        assert resp.content == b"DATA"
        # Manual following: each hop fetched with allow_redirects=False...
        assert all(c["allow_redirects"] is False for c in calls)
        # ...the NLI hop with verify=False, the external redirect target with
        # verify=True (the allowlist is NOT escaped by the 30x).
        assert calls[0]["url"] == NLI_URL and calls[0]["verify"] is False
        assert calls[1]["url"] == ext and calls[1]["verify"] is True

    def test_redirect_nli_to_nli_stays_verify_false(self, monkeypatch):
        target = "https://rosetta.nli.org.il/delivery/img"
        calls = self._script_get(
            monkeypatch, {NLI_URL: _RedirectResp(target), target: _OkResp(b"X")}
        )
        nli_fetch.nli_image_get(NLI_URL, headers={}, timeout=(3, 5))
        assert calls[0]["verify"] is False
        assert calls[1]["verify"] is False

    def test_allow_redirects_false_returns_the_30x(self, monkeypatch):
        ext = "https://evil.example.com/y"
        calls = self._script_get(monkeypatch, {NLI_URL: _RedirectResp(ext)})
        resp = nli_fetch.nli_image_get(
            NLI_URL, headers={}, timeout=(3, 5), allow_redirects=False
        )
        assert resp.status_code == 302
        assert len(calls) == 1  # not followed

    def test_redirect_cap_raises_too_many_redirects(self, monkeypatch):
        # A self-referential redirect loop must terminate via TooManyRedirects.
        self._script_get(monkeypatch, {NLI_URL: _RedirectResp(NLI_URL)})
        with pytest.raises(requests.exceptions.TooManyRedirects):
            nli_fetch.nli_image_get(NLI_URL, headers={}, timeout=(3, 5))


# ---------------------------------------------------------------------------
# desktop/image_loader.py::_download_bytes — breaker wiring + timeouts
# ---------------------------------------------------------------------------
@pytest.mark.skipif(
    not IMAGE_LOADER_AVAILABLE,
    reason="desktop.image_loader unavailable (PyQt6 QtGui could not load)",
)
class TestDownloadBytesBreaker:
    def _loader(self):
        # _download_bytes is a pure method (uses only self._cancelled); build
        # the instance without QThread.__init__ to avoid Qt teardown.
        loader = ImageLoaderThread.__new__(ImageLoaderThread)
        loader._cancelled = False
        return loader

    def _trip_breaker(self):
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure(failure_type="timeout", path="preload")
        assert br.is_open()

    def test_breaker_open_short_circuits_without_network(self, monkeypatch):
        import desktop.image_loader as il

        self._trip_breaker()
        calls = {"n": 0}

        def boom(*a, **k):
            calls["n"] += 1
            raise AssertionError("network must not be hit when breaker is open")

        monkeypatch.setattr(il, "nli_image_get", boom)
        loader = self._loader()
        assert loader._download_bytes(NLI_URL, {}) is None
        assert calls["n"] == 0

    def test_200_returns_content_and_records_success(self, monkeypatch):
        import desktop.image_loader as il

        monkeypatch.setattr(il, "nli_image_get", lambda *a, **k: _Resp(200, b"DATA"))
        loader = self._loader()
        assert loader._download_bytes(NLI_URL, {}) == b"DATA"
        assert br._state_snapshot()["consecutive_failures"] == 0

    def test_5xx_records_5xx_failure(self, monkeypatch):
        import desktop.image_loader as il

        monkeypatch.setattr(il, "nli_image_get", lambda *a, **k: _Resp(503, b""))
        loader = self._loader()
        assert loader._download_bytes(NLI_URL, {}) is None
        assert br._state_snapshot()["consecutive_failures"] == 1

    def test_429_records_failure(self, monkeypatch):
        import desktop.image_loader as il

        monkeypatch.setattr(il, "nli_image_get", lambda *a, **k: _Resp(429, b""))
        loader = self._loader()
        assert loader._download_bytes(NLI_URL, {}) is None
        assert br._state_snapshot()["consecutive_failures"] == 1

    def test_timeout_records_failure(self, monkeypatch):
        import desktop.image_loader as il

        def raise_timeout(*a, **k):
            raise requests.exceptions.Timeout()

        monkeypatch.setattr(il, "nli_image_get", raise_timeout)
        loader = self._loader()
        assert loader._download_bytes(NLI_URL, {}) is None
        assert br._state_snapshot()["consecutive_failures"] == 1

    def test_connection_error_records_failure(self, monkeypatch):
        import desktop.image_loader as il

        def raise_conn(*a, **k):
            raise requests.exceptions.ConnectionError()

        monkeypatch.setattr(il, "nli_image_get", raise_conn)
        loader = self._loader()
        assert loader._download_bytes(NLI_URL, {}) is None
        assert br._state_snapshot()["consecutive_failures"] == 1

    def test_404_does_not_trip_breaker(self, monkeypatch):
        # 4xx other than 429 is the caller's concern (D-07); must NOT count.
        import desktop.image_loader as il

        monkeypatch.setattr(il, "nli_image_get", lambda *a, **k: _Resp(404, b""))
        loader = self._loader()
        assert loader._download_bytes(NLI_URL, {}) is None
        assert br._state_snapshot()["consecutive_failures"] == 0

    def test_non_nli_failure_does_not_trip_breaker(self, monkeypatch):
        import desktop.image_loader as il

        monkeypatch.setattr(il, "nli_image_get", lambda *a, **k: _Resp(503, b""))
        loader = self._loader()
        assert loader._download_bytes(EXT_URL, {}) is None
        assert br._state_snapshot()["consecutive_failures"] == 0
        assert br.is_open() is False

    def test_non_nli_not_short_circuited_when_breaker_open(self, monkeypatch):
        import desktop.image_loader as il

        self._trip_breaker()
        calls = {"n": 0}

        def fake(*a, **k):
            calls["n"] += 1
            return _Resp(200, b"EXT")

        monkeypatch.setattr(il, "nli_image_get", fake)
        loader = self._loader()
        # An NLI outage must not block Cambridge/Oxford/JTS images.
        assert loader._download_bytes(EXT_URL, {}) == b"EXT"
        assert calls["n"] == 1

    def test_timeout_tuple_short_connect_generous_read(self, monkeypatch):
        import desktop.image_loader as il
        from shared.nli_circuit_breaker import (
            NLI_CONNECT_TIMEOUT,
            NLI_IMAGE_READ_TIMEOUT,
        )

        cap = {}

        def fake(url, **k):
            cap[url] = k.get("timeout")
            return _Resp(200, b"x")

        monkeypatch.setattr(il, "nli_image_get", fake)
        loader = self._loader()
        loader._download_bytes(NLI_URL, {})
        loader._download_bytes(ROSETTA_URL, {})
        loader._download_bytes(EXT_URL, {})
        # Always a (connect, read) tuple — short connect so a dead host fails
        # in ~NLI_CONNECT_TIMEOUT s, not the old blanket 10/30s.
        assert cap[NLI_URL] == (NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT)
        assert cap[ROSETTA_URL] == (NLI_CONNECT_TIMEOUT, 30)  # full-res TIFF read
        assert cap[EXT_URL] == (NLI_CONNECT_TIMEOUT, 10)

    def test_cancelled_returns_none(self, monkeypatch):
        import desktop.image_loader as il

        monkeypatch.setattr(il, "nli_image_get", lambda *a, **k: _Resp(200, b"DATA"))
        loader = self._loader()
        loader._cancelled = True
        assert loader._download_bytes(NLI_URL, {}) is None


# ---------------------------------------------------------------------------
# Source guards — the two desktop fetch sites no longer use a blanket
# verify=False and DO route through the shared breaker + host policy.
# (join_workbench's ThumbBatchWorker is a QThread whose run() is CI-skipped to
#  construct; its logic mirrors _download_bytes through the same shared helper.)
# ---------------------------------------------------------------------------
class TestSourceGuards:
    def _src(self, rel):
        return pathlib.Path(rel).read_text(encoding="utf-8")

    def test_image_loader_no_blanket_verify_false(self):
        src = self._src("desktop/image_loader.py")
        assert "verify=False" not in src, (
            "M2: image_loader must route TLS via shared nli_fetch policy, "
            "not a blanket verify=False"
        )
        assert "nli_image_get" in src
        assert "_nli_circuit_is_open" in src

    def test_join_workbench_thumb_breaker_wired(self):
        src = self._src("desktop/join_workbench.py")
        assert "verify=False" not in src, (
            "M2: join_workbench thumb fetch must route TLS via shared nli_fetch policy"
        )
        assert "nli_image_get" in src
        assert "is_nli_host" in src
        assert "join_workbench_thumb" in src
