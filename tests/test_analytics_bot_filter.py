"""Automated browsers are kept out of Google Analytics and PostHog (web/analytics_bot_filter.py).

The user-agent pattern is run in Python's ``re`` (it is plain enough to behave the same as in
JavaScript), and the wiring is checked on the strings ``web/main.py`` actually adds to <head>.
"""
import re
from pathlib import Path

from web import analytics_bot_filter as abf

ROOT = Path(__file__).resolve().parents[1]


def _ua_pattern() -> re.Pattern:
    m = re.search(r'return (/.+/)i\s*\n\s*\.test\(navigator\.userAgent', abf.AUTOMATED_JS)
    assert m, 'user-agent test not found in AUTOMATED_JS'
    body = m.group(1)[1:-1].replace(r'\/', '/')
    return re.compile(body, re.I)


BOTS = [
    'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
    'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; Googlebot/2.1) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)',
    'Mozilla/5.0 (Linux; Android 11; moto g power) Chrome-Lighthouse',
    'Mozilla/5.0 (compatible; GoogleOther)',
    'Mozilla/5.0 (compatible; Google-InspectionTool/1.0)',
    'AdsBot-Google (+http://www.google.com/adsbot.html)',
    # Seen in the server log 2026-11-26, browsing /browse page by page.
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.53 Safari/537.36 Google-Agent',
]

PEOPLE = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 13; CUBOT_X30 Build/TP1A) AppleWebKit/537.36 Chrome/131.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0',
]


def test_crawler_user_agents_are_automated():
    pat = _ua_pattern()
    for ua in BOTS:
        assert pat.search(ua), ua


def test_people_are_not_automated():
    pat = _ua_pattern()
    for ua in PEOPLE:
        assert not pat.search(ua), ua


def test_webdriver_is_checked():
    assert 'navigator.webdriver' in abf.AUTOMATED_JS


def test_google_tag_loads_only_when_not_automated():
    html = abf.ga_head_html()
    assert html.startswith(abf.AUTOMATED_JS)
    # No static <script src=gtag>: it would load before the check.
    assert '<script async src=' not in html
    gate = html.index('if (!window.__ghAutomated)')
    assert gate < html.index('googletagmanager.com/gtag/js')
    assert gate < html.index("gtag('config'")


def test_main_uses_the_module_for_the_google_tag():
    src = (ROOT / 'web' / 'main.py').read_text(encoding='utf-8')
    assert 'ANALYTICS_SCRIPT = _ga_head_html()' in src
    assert 'G-LXT1PTKG3E' not in src


def _head_scripts() -> list:
    """The <script> bodies web/main.py puts in <head> for both tags, as a browser gets them.

    POSTHOG_SCRIPT is an f-string that is empty without POSTHOG_API_KEY, so it is rebuilt from
    main.py's own source text rather than imported (importing web.main starts the app)."""
    src = (ROOT / 'web' / 'main.py').read_text(encoding='utf-8')
    ph = src[src.index("POSTHOG_SCRIPT = f'''") + len("POSTHOG_SCRIPT = f'''"):
             src.index("''' if _posthog_key else ''")]
    ph = ph.replace('{{', '{').replace('}}', '}')
    scrub = (ROOT / 'web' / 'static' / 'js' / 'posthog_scrub.js').read_text(encoding='utf-8')
    ph = (ph.replace('{_AUTOMATED_JS}', abf.AUTOMATED_JS)
            .replace('{_POSTHOG_SCRUB_JS}', scrub).replace('{_posthog_key}', 'phc_test'))
    html = abf.ga_head_html() + ph
    return re.findall(r'<script>(.*?)</script>', html, re.S)


# Runs the head scripts in a mock page, then the calls the server and the static scripts make
# (web/analytics.py, web/auth_state.py, hebrew_keyboard.js): none may throw, and an automated
# browser must load neither tag.
_NODE_HARNESS = r'''
const vm = require('vm');
const scripts = JSON.parse(require('fs').readFileSync(0, 'utf8'));
function run(webdriver) {
  const loaded = [];
  const el = () => ({ parentNode: { insertBefore: (s) => loaded.push(s.src) } });
  const document = {
    createElement: () => ({}),
    getElementsByTagName: () => [el()],
    head: { appendChild: (s) => loaded.push(s.src) },
  };
  const window = { navigator: { webdriver, userAgent: 'Mozilla/5.0 (Windows NT 10.0) Chrome/131.0' },
                   location: { hostname: 'genizahsearch.com' },
                   requestIdleCallback: (f) => f() };
  window.window = window; window.document = document;
  const ctx = vm.createContext(Object.assign(window, { console }));
  for (const s of scripts) vm.runInContext(s, ctx);
  vm.runInContext("if(window.posthog)posthog.capture('search_executed',{})", ctx);
  vm.runInContext("if(window.posthog)posthog.identify('u',{})", ctx);
  vm.runInContext("if(window.posthog){window.posthog.capture('hebrew_keyboard_used',{})}", ctx);
  return { automated: !!ctx.__ghAutomated, posthog: typeof ctx.posthog,
           loaded: loaded.map(String), dataLayer: (ctx.dataLayer || []).length };
}
console.log(JSON.stringify({ person: run(false), bot: run(true) }));
'''


def test_head_scripts_run_in_a_page():
    import json
    import os
    import shutil
    import subprocess

    import pytest
    node = shutil.which('node')
    if not node:
        if os.environ.get('CI'):
            pytest.fail('node is required for the analytics head-script check on CI')
        pytest.skip('node is not installed')
    out = subprocess.run([node, '-e', _NODE_HARNESS], input=json.dumps(_head_scripts()),
                         capture_output=True, text=True, encoding='utf-8', timeout=30)
    assert out.returncode == 0, out.stdout + out.stderr
    res = json.loads(out.stdout)
    person, bot = res['person'], res['bot']
    assert not person['automated'] and person['posthog'] == 'object'
    assert any('gtag/js' in s for s in person['loaded']), person
    assert any('array.js' in s for s in person['loaded']), person
    assert person['dataLayer'] == 2
    assert bot['automated'] and bot['posthog'] == 'undefined'
    assert bot['loaded'] == [] and bot['dataLayer'] == 0, bot
