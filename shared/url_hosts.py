# -*- coding: utf-8 -*-
"""Which library a URL belongs to, decided by its host name.

A test like ``'cudl.lib.cam.ac.uk' in url`` also matches
``https://elsewhere.example/?cudl.lib.cam.ac.uk``. ``is_on_host`` parses the
URL and compares its host name with each domain, or a subdomain of it.
"""
from __future__ import annotations

from urllib.parse import urlsplit

CAMBRIDGE_HOSTS = ('cudl.lib.cam.ac.uk',)
OXFORD_HOSTS = ('bodleian.ox.ac.uk',)  # also hebrew.bodleian.ox.ac.uk, digital.bodleian...
MANCHESTER_HOSTS = ('luna.manchester.ac.uk',)
PRINCETON_HOSTS = ('iiif-cloud.princeton.edu', 'figgy.princeton.edu')


def url_host(url) -> str:
    """The lower-case host name of ``url``; '' when there is none.

    A URL written without its scheme (``cudl.lib.cam.ac.uk/view/...``) is read
    as if it had one.
    """
    text = str(url or '').strip()
    if not text:
        return ''
    try:
        host = urlsplit(text).hostname
        if not host and '://' not in text and not text.startswith('/'):
            host = urlsplit('//' + text).hostname
    except ValueError:
        return ''
    host = (host or '').lower().rstrip('.')
    return '' if any(c.isspace() for c in host) else host


def is_on_host(url, domains) -> bool:
    """True when ``url``'s host is one of ``domains`` or a subdomain of one."""
    host = url_host(url)
    return bool(host) and any(host == d or host.endswith('.' + d) for d in domains)
