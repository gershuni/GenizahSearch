# -*- coding: utf-8 -*-
"""shared/url_hosts.py: a library is recognised by the URL's host name, never by
its text appearing somewhere in the URL; and no caller tests the text again."""
from __future__ import annotations

import os
import re
import subprocess

import pytest

from shared.url_hosts import (CAMBRIDGE_HOSTS, MANCHESTER_HOSTS, OXFORD_HOSTS, PRINCETON_HOSTS,
                              is_on_host, url_host)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.mark.parametrize('url, domains, expected', [
    ('https://cudl.lib.cam.ac.uk/view/MS-TS-00012-00123/1', CAMBRIDGE_HOSTS, True),
    ('HTTPS://CUDL.LIB.CAM.AC.UK/view/x', CAMBRIDGE_HOSTS, True),
    ('cudl.lib.cam.ac.uk/view/x', CAMBRIDGE_HOSTS, True),          # no scheme
    ('https://cudl.lib.cam.ac.uk./view/x', CAMBRIDGE_HOSTS, True),  # trailing dot
    ('https://cudl.lib.cam.ac.uk:443/view/x', CAMBRIDGE_HOSTS, True),
    ('https://hebrew.bodleian.ox.ac.uk/x', OXFORD_HOSTS, True),
    ('https://digital.bodleian.ox.ac.uk/objects/x', OXFORD_HOSTS, True),
    ('https://luna.manchester.ac.uk/luna/servlet/x', MANCHESTER_HOSTS, True),
    ('https://iiif-cloud.princeton.edu/iiif/x', PRINCETON_HOSTS, True),
    ('https://figgy.princeton.edu/concern/x', PRINCETON_HOSTS, True),
    # The domain's text elsewhere in the URL is not the host.
    ('https://elsewhere.example/?u=cudl.lib.cam.ac.uk', CAMBRIDGE_HOSTS, False),
    ('https://elsewhere.example/cudl.lib.cam.ac.uk/x', CAMBRIDGE_HOSTS, False),
    ('https://cudl.lib.cam.ac.uk.elsewhere.example/x', CAMBRIDGE_HOSTS, False),
    ('https://notcudl.lib.cam.ac.uk/x', CAMBRIDGE_HOSTS, False),
    ('https://cudl.lib.cam.ac.uk@elsewhere.example/x', CAMBRIDGE_HOSTS, False),
    ('', CAMBRIDGE_HOSTS, False),
    (None, CAMBRIDGE_HOSTS, False),
    ('http://[bad', CAMBRIDGE_HOSTS, False),
])
def test_is_on_host(url, domains, expected):
    assert is_on_host(url, domains) is expected


def test_url_host():
    assert url_host('https://Hebrew.Bodleian.ox.ac.uk/x') == 'hebrew.bodleian.ox.ac.uk'
    assert url_host('/relative/path') == ''
    assert url_host('not a url') == ''


@pytest.mark.parametrize('url, provider', [
    ('https://cudl.lib.cam.ac.uk/iiif/MS-TS-1', 'cambridge'),
    ('https://digital.bodleian.ox.ac.uk/iiif/image/x', 'oxford'),
    ('https://luna.manchester.ac.uk/luna/x', 'manchester'),
    ('https://iiif-cloud.princeton.edu/iiif/2/x', 'jts'),
    ('https://elsewhere.example/?cudl.lib.cam.ac.uk', None),
])
def test_desktop_viewer_names_the_library_by_host(url, provider):
    pytest.importorskip('PyQt6')
    from desktop.viewers import detect_external_provider
    assert detect_external_provider({'images_ext': [{'url': url}]}) == provider


_HOST_TEXT_TEST = re.compile(
    r"""['"][a-z0-9.-]*(cam\.ac\.uk|bodleian|manchester\.ac\.uk|princeton\.edu)[a-z0-9.-]*['"]\s+(not\s+)?in\b""")


def test_no_caller_tests_a_library_host_as_text():
    files = subprocess.run(['git', 'ls-files', '*.py'], cwd=REPO, capture_output=True,
                           encoding='utf-8', check=True).stdout.split()
    found = []
    for path in files:
        if path.startswith('tests/') or path == 'shared/url_hosts.py':
            continue
        with open(os.path.join(REPO, path), encoding='utf-8', errors='replace') as fh:
            for number, line in enumerate(fh, 1):
                if _HOST_TEXT_TEST.search(line):
                    found.append(f'{path}:{number}: {line.strip()}')
    assert not found, 'use shared.url_hosts.is_on_host:\n' + '\n'.join(found)
