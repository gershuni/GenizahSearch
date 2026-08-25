"""Client-side error-reporting contract (2026-08-25 PostHog error-digest triage).

The weekly digest for the week to 2026-08-25 held 33 exceptions in four issues, and
32 of them were foreign to this codebase:

  18  DOMException  SecurityError: Failed to read the 'cssRules' property ...
  11  TypeError     Failed to fetch
   3  TypeError     Failed to fetch   (a second fingerprint, new that week)
   1  Error         Script error.

Exception capture is switched on at the PostHog PROJECT level, not in ``posthog.init``
here, so it is a global ``window.onerror`` / ``unhandledrejection`` hook with no
deny-list: it reports whatever runs in the tab, ours or not. Two consequences are
pinned by the tests below.

1. The ``cssRules`` read is provably not ours -- hence
   ``test_no_stylesheet_rules_reader_in_client_code``, which is what licenses dropping
   that fingerprint in the browser. If a legitimate stylesheet-rules reader is ever
   added, that test fails and the filter must be reconsidered BEFORE the new code's
   real errors start getting swallowed.

2. "Script error." is opaque precisely BECAUSE a cross-origin script tag lacked
   ``crossorigin="anonymous"``, so it can be hiding one of our own failures. The fix is
   the attribute, never a filter -- hence the fabric.js assertions and the
   "keeps everything else" case for that message.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# Directories whose contents execute in a visitor's browser.
CLIENT_ROOTS = ('web', 'extension')
CLIENT_SUFFIXES = {'.py', '.js', '.html', '.css'}


# ---------------------------------------------------------------------------
# The premise that licenses the filter.
# ---------------------------------------------------------------------------

def test_no_stylesheet_rules_reader_in_client_code():
    """No first-party code reads stylesheet rules, so the SecurityError is foreign.

    Tokens are deliberately dot-/paren-anchored so they match a READ
    (``sheet.cssRules``, ``document.styleSheets``) and not the filter's own bare
    string comparison or the prose explaining it.
    """
    tokens = ('.cssRules', '.styleSheets', 'insertRule(')
    offenders = []
    for root_name in CLIENT_ROOTS:
        root = REPO_ROOT / root_name
        if not root.is_dir():
            continue
        for path in sorted(root.rglob('*')):
            if not path.is_file() or path.suffix not in CLIENT_SUFFIXES:
                continue
            text = path.read_text(encoding='utf-8', errors='replace')
            for token in tokens:
                if token in text:
                    offenders.append(f'{path.relative_to(REPO_ROOT)}: {token}')
    assert not offenders, (
        'First-party code now reads stylesheet rules, so a SecurityError on cssRules '
        'may be OUR bug rather than injected third-party CSS. Re-examine the drop in '
        'web.main._PH_BEFORE_SEND_JS before keeping it:\n  ' + '\n  '.join(offenders)
    )


# ---------------------------------------------------------------------------
# Rendering of the PostHog snippet.
# ---------------------------------------------------------------------------

def test_before_send_is_wired_into_posthog_init():
    """The filter must actually reach posthog.init, not merely be defined above it."""
    from web.main import _build_posthog_script

    script = _build_posthog_script('phc_test_key')
    assert 'function _phBeforeSend(event)' in script
    assert 'before_send: _phBeforeSend' in script


def test_build_posthog_script_uses_its_argument_not_the_module_key():
    """Guards a real defect caught in review: the f-string interpolated the global.

    POSTHOG_API_KEY is unset in CI, so that bug would make every assertion in this
    module vacuous while production still looked correct.
    """
    from web.main import _build_posthog_script

    rendered = _build_posthog_script('phc_distinctive_key')
    assert "posthog.init('phc_distinctive_key'" in rendered


def test_build_posthog_script_is_empty_without_a_key():
    from web.main import _build_posthog_script

    assert _build_posthog_script('') == ''


def test_before_send_js_carries_no_f_string_escapes():
    """The filter lives in a plain string, so doubled braces would ship verbatim."""
    from web.main import _PH_BEFORE_SEND_JS

    assert '{{' not in _PH_BEFORE_SEND_JS
    assert '}}' not in _PH_BEFORE_SEND_JS


def test_rendered_posthog_script_has_no_unrendered_braces():
    from web.main import _build_posthog_script

    assert '{{' not in _build_posthog_script('phc_test_key')


# ---------------------------------------------------------------------------
# Behaviour of the filter, executed as the JavaScript it actually ships as.
# ---------------------------------------------------------------------------

def _run_filter(event_json: str) -> str:
    """Run the shipped ``_PH_BEFORE_SEND_JS`` in node; return 'dropped'/'kept'/'mutated'.

    Executes the real source rather than a Python paraphrase -- a paraphrase would
    keep passing while the deployed JS was broken.
    """
    node_bin = shutil.which('node') or shutil.which('nodejs')
    if not node_bin:
        pytest.skip('node not installed; this case needs a JS engine to run the filter')

    from web.main import _PH_BEFORE_SEND_JS

    harness = (
        _PH_BEFORE_SEND_JS
        + f'\nconst ev = {event_json};'
        + '\nconst out = _phBeforeSend(ev);'
        + "\nconsole.log(out === null ? 'dropped' : (out === ev ? 'kept' : 'mutated'));"
    )
    with tempfile.NamedTemporaryFile(
        'w', suffix='.js', delete=False, encoding='utf-8'
    ) as handle:
        handle.write(harness)
        script_path = Path(handle.name)
    try:
        proc = subprocess.run(
            [node_bin, str(script_path)], capture_output=True, text=True, timeout=60
        )
    finally:
        script_path.unlink(missing_ok=True)
    assert proc.returncode == 0, f'filter raised in node: {proc.stderr.strip()}'
    return proc.stdout.strip()


def _exception_event(exc_type: str, value: str) -> str:
    return json.dumps({
        'event': '$exception',
        'properties': {'$exception_list': [{'type': exc_type, 'value': value}]},
    })


def test_before_send_drops_cross_origin_cssrules_error():
    """The digest's top issue, verbatim -- 18 of that week's 33 exceptions."""
    verbatim = (
        "SecurityError: Failed to read the 'cssRules' property from 'CSSStyleSheet': "
        'Cannot access rules'
    )
    assert _run_filter(_exception_event('DOMException', verbatim)) == 'dropped'


def test_before_send_scans_every_frame_not_just_the_first():
    """rrweb wraps the SecurityError, so the match can sit behind an outer frame."""
    nested = json.dumps({
        'event': '$exception',
        'properties': {'$exception_list': [
            {'type': 'Error', 'value': 'style mutation failed'},
            {'type': 'DOMException', 'value': "cannot read 'cssRules'"},
        ]},
    })
    assert _run_filter(nested) == 'dropped'


@pytest.mark.parametrize('exc_type,value', [
    # Both "Failed to fetch" issues in the same digest: unattributed, so they stay.
    ('TypeError', 'Failed to fetch'),
    # Opaque BECAUSE of a missing crossorigin attribute -- may be hiding our own bug.
    ('Error', 'Script error.'),
    # A representative first-party error must never be filtered.
    ('TypeError', "Cannot read properties of null (reading 'canvas')"),
])
def test_before_send_keeps_everything_else(exc_type, value):
    assert _run_filter(_exception_event(exc_type, value)) == 'kept'


@pytest.mark.parametrize('event_json', [
    '{"event": "$pageview", "properties": {}}',
    '{"event": "$autocapture"}',
    '{"event": "$exception"}',
    '{"event": "$exception", "properties": {"$exception_list": "not-an-array"}}',
    '{"event": "$exception", "properties": {"$exception_list": [null]}}',
])
def test_before_send_passes_through_other_and_malformed_events(event_json):
    """A filter that throws or over-drops loses real telemetry silently."""
    assert _run_filter(event_json) == 'kept'


# ---------------------------------------------------------------------------
# The one first-party cause in the digest: "Script error." from fabric.js.
# ---------------------------------------------------------------------------

def test_fabric_cdn_tag_is_cors_enabled():
    """Without crossorigin=anonymous a fabric exception is an unreadable Script error."""
    from web.pages.puzzle import FABRIC_JS_CDN

    assert 'cdn.jsdelivr.net' in FABRIC_JS_CDN
    assert 'crossorigin="anonymous"' in FABRIC_JS_CDN


# Hosts allowed to ship a cross-origin <script> WITHOUT crossorigin="anonymous".
#
# The attribute is not free: if the response carries no Access-Control-Allow-Origin,
# the browser refuses to execute the script at all. So it may only be added to a host
# whose CORS headers have been confirmed. Trading an opaque "Script error." for a
# silently dead script is a bad trade.
#
# googletagmanager.com: gtag.js CORS headers UNVERIFIED (the sandbox this was triaged
# in blocks the host, so it could not be checked). Google also does not support SRI on
# gtag.js, since the content changes. Losing site-wide analytics is strictly worse than
# one unreadable exception per week, so this stays exempt until someone confirms the
# header from a machine with open egress -- at which point delete the entry and add the
# attribute.
_CORS_ATTRIBUTE_EXEMPT_HOSTS = ('www.googletagmanager.com',)


def test_every_cross_origin_script_tag_is_cors_enabled():
    """Any third-party ``<script src="https://...">`` must opt into CORS.

    Same reasoning as the fabric tag: without it, every exception the script raises
    arrives as a contentless "Script error." with no message, file or line.
    """
    pattern = re.compile(r'<script[^>]*\bsrc="https?://[^"]+"[^>]*>', re.IGNORECASE)
    offenders = []
    exempt_seen = set()
    for path in sorted((REPO_ROOT / 'web').rglob('*')):
        if not path.is_file() or path.suffix not in {'.py', '.html', '.js'}:
            continue
        text = path.read_text(encoding='utf-8', errors='replace')
        for tag in pattern.findall(text):
            if 'crossorigin' in tag.lower():
                continue
            exempt = next(
                (host for host in _CORS_ATTRIBUTE_EXEMPT_HOSTS if host in tag), None
            )
            if exempt:
                exempt_seen.add(exempt)
                continue
            offenders.append(f'{path.relative_to(REPO_ROOT)}: {tag[:120]}')
    assert not offenders, (
        'Cross-origin <script> tags without crossorigin="anonymous" -- their runtime '
        'errors reach PostHog as a bare "Script error." with no message, file or line. '
        'Confirm the host sends Access-Control-Allow-Origin, then add the attribute; '
        'if it does not, add the host to _CORS_ATTRIBUTE_EXEMPT_HOSTS with the '
        'reason:\n  ' + '\n  '.join(offenders)
    )
    stale = set(_CORS_ATTRIBUTE_EXEMPT_HOSTS) - exempt_seen
    assert not stale, (
        'Exemption(s) in _CORS_ATTRIBUTE_EXEMPT_HOSTS no longer match any tag, so the '
        f'list is now telling a story about code that is gone -- remove them: {stale}'
    )
