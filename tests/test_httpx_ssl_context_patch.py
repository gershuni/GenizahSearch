# -*- coding: utf-8 -*-
"""httpx's default SSL context is built from the CA bundle read once, not re-read per client.

``get_user_client()`` builds a supabase client for each signed-in task, and that
builds about four httpx clients. Each called ``ssl.create_default_context(cafile=
certifi.where())``, and OpenSSL 3.0 loads a CA *file* slowly (about 190 ms per
context on the dev box; cpython#95031 reports 40-60 ms on Ubuntu with OpenSSL
3.0.x), all on the event loop: about 1.2 s per signed-in call measured locally
on 2026-10-20. The same certificates loaded from text give the same context much
faster. Every client must still get its OWN context (httpcore sets ALPN on a
context for each connection), and certificate checking must stay on.
"""

from __future__ import annotations

import ssl

import certifi
import httpx
import pytest

import web.framework_patches as fp


@pytest.fixture
def patched():
    fp._patch_httpx_ssl_context()


def _context(client: httpx.Client) -> ssl.SSLContext:
    return client._transport._pool._ssl_context


def test_the_default_context_never_reads_the_ca_file(patched, monkeypatch):
    loads = []
    real = ssl.SSLContext.load_verify_locations

    def spy(self, cafile=None, capath=None, cadata=None):
        loads.append((cafile, capath, cadata is not None))
        return real(self, cafile, capath, cadata)

    monkeypatch.setattr(ssl.SSLContext, 'load_verify_locations', spy)
    monkeypatch.delenv('SSL_CERT_FILE', raising=False)
    monkeypatch.delenv('SSL_CERT_DIR', raising=False)
    with httpx.Client():
        pass
    assert loads and all(cafile is None and capath is None and has_data
                         for cafile, capath, has_data in loads), loads


def test_it_trusts_the_same_certificates_and_still_verifies(patched):
    reference = ssl.create_default_context(cafile=certifi.where())
    with httpx.Client() as client:
        ctx = _context(client)
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True
    assert len(ctx.get_ca_certs()) == len(reference.get_ca_certs()) > 100


def test_each_client_gets_its_own_context(patched):
    with httpx.Client() as a, httpx.Client() as b:
        assert _context(a) is not _context(b)


def test_an_explicit_ca_setting_keeps_httpx_behaviour(patched, monkeypatch):
    loads = []
    real = ssl.SSLContext.load_verify_locations

    def spy(self, cafile=None, capath=None, cadata=None):
        loads.append(cafile)
        return real(self, cafile, capath, cadata)

    monkeypatch.setattr(ssl.SSLContext, 'load_verify_locations', spy)
    monkeypatch.setenv('SSL_CERT_FILE', certifi.where())
    with httpx.Client():
        pass
    assert loads == [certifi.where()]
    with httpx.Client(verify=False) as insecure:
        assert _context(insecure).verify_mode == ssl.CERT_NONE


def test_the_patch_is_idempotent(patched):
    import httpx._transports.default as transports

    first = transports.create_ssl_context
    fp._patch_httpx_ssl_context()
    assert transports.create_ssl_context is first
