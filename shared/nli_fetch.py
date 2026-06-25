"""SEED-015 — shared NLI image-fetch host policy (TLS + host detection).

The NLI image-delivery hosts (``iiif.nli.org.il``, ``rosetta.nli.org.il``)
present a legacy TLS certificate chain that fails Python's default
verification. Historically the desktop image loaders worked around this with a
blanket ``requests.get(..., verify=False)`` — which (a) disabled TLS for *any*
host that flowed through the same code path (Cambridge / Oxford / JTS images),
and (b) emitted/suppressed ``InsecureRequestWarning`` globally (M2, audit
2026-06-23).

This module centralises a *minimal* host policy shared by the web and desktop
NLI fetch sites:

- :func:`is_nli_host` — used to gate the shared NLI circuit breaker so a
  non-NLI library failure never trips it.
- :func:`nli_verify_for` — ``verify=False`` ONLY for the known NLI image hosts,
  ``verify=True`` everywhere else.
- :func:`nli_image_get` — a thin ``requests.get`` wrapper that applies the
  verify policy and suppresses the ``InsecureRequestWarning`` *host-scoped*
  (only around the actual NLI call, never installed as a global filter).

Deliberately tiny and dependency-free (only ``requests`` / ``urllib`` /
``warnings``) so both ``web/`` and ``desktop/`` can import it without a cycle.
The fuller image-loading unification (audit findings #2 / M1 / M4) is DEFERRED
to a later milestone — do NOT grow this module into that.
"""

from __future__ import annotations

import warnings
from urllib.parse import urlparse

import requests

# urllib3 ships InsecureRequestWarning; modern `requests` re-exports it via the
# top-level urllib3. Fall back to the vendored path, then to None, so an
# unexpected packaging change degrades to a still-host-scoped broad ignore
# rather than crashing the import.
try:  # pragma: no cover - import shape varies by environment
    from urllib3.exceptions import InsecureRequestWarning
except Exception:  # pragma: no cover
    try:
        from requests.packages.urllib3.exceptions import InsecureRequestWarning  # type: ignore
    except Exception:
        InsecureRequestWarning = None  # type: ignore[assignment]


# The known NLI image-delivery hosts. Kept explicit (not a suffix match) because
# this set drives a SECURITY decision (which hosts skip TLS verification, M2).
NLI_IMAGE_HOSTS = frozenset({
    'iiif.nli.org.il',
    'rosetta.nli.org.il',
})


def host_of(url: str) -> str:
    """Return the lowercased hostname of ``url`` ('' if unparseable)."""
    try:
        return (urlparse(url).hostname or '').lower()
    except Exception:
        return ''


def is_nli_host(url: str) -> bool:
    """True iff ``url`` points at a known NLI image-delivery host.

    Used to (a) gate the shared NLI circuit breaker — only NLI failures should
    count toward it — and (b) decide the TLS verify policy.
    """
    return host_of(url) in NLI_IMAGE_HOSTS


def nli_verify_for(url: str) -> bool:
    """TLS ``verify`` value to use for ``url``.

    ``False`` only for the known NLI image hosts (legacy cert chain, M2);
    ``True`` for every other host.
    """
    return not is_nli_host(url)


def nli_image_get(url, *, headers=None, timeout=None, stream=False, **kwargs):
    """``requests.get`` with the NLI host TLS policy applied.

    For NLI image hosts, TLS verification is disabled and the resulting
    ``InsecureRequestWarning`` is suppressed *host-scoped* — the ignore filter
    is installed inside a :func:`warnings.catch_warnings` block around this one
    call and torn down afterwards, so it never leaks to the rest of the process.
    For all other hosts, ``verify=True`` and no filter is touched.

    Does NOT consult or feed the circuit breaker — the caller records
    success/failure so it can attach a call-site ``path``.
    """
    verify = nli_verify_for(url)
    if verify:
        return requests.get(
            url, headers=headers, timeout=timeout, stream=stream, verify=True, **kwargs
        )
    # NLI host -> verify=False, but keep the InsecureRequestWarning suppression
    # scoped to exactly this call (catch_warnings restores the prior filters).
    with warnings.catch_warnings():
        if InsecureRequestWarning is not None:
            warnings.simplefilter('ignore', InsecureRequestWarning)
        else:  # pragma: no cover - defensive packaging fallback
            warnings.simplefilter('ignore')
        return requests.get(
            url, headers=headers, timeout=timeout, stream=stream, verify=False, **kwargs
        )


__all__ = [
    'NLI_IMAGE_HOSTS',
    'host_of',
    'is_nli_host',
    'nli_verify_for',
    'nli_image_get',
]
