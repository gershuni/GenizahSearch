"""Keep automated browsers out of Google Analytics and PostHog.

In the 28 days to 2026-11-26, 1,441 of the 1,919 "Direct" users in Google Analytics (and
1,529 PostHog persons) came from a data centre in The Dalles, Oregon: desktop Chrome on
Linux, one page, zero seconds, never returning. They were three in four of the site's
"active users". Counting them hides the real audience (about 700 a month).

The page itself stays open to them: a crawler that renders /browse may be how a search
engine indexes the manuscript pages. Only the two analytics tags are skipped.

``AUTOMATED_JS`` sets ``window.__ghAutomated`` once per page. It is included in both
tags' head HTML, so a route that adds only one of them still has it.
"""

GA_MEASUREMENT_ID = 'G-LXT1PTKG3E'

# navigator.webdriver is true in a browser driven by WebDriver / Puppeteer / Playwright
# (headless or not). The user-agent test catches crawlers that announce themselves
# (Googlebot, GoogleOther, Google-InspectionTool, bingbot, HeadlessChrome, Lighthouse).
AUTOMATED_JS = r'''<script>
window.__ghAutomated = window.__ghAutomated || (function () {
  try {
    if (navigator.webdriver) return true;
    return /bot\/|\bbot\b|crawl|spider|slurp|headless|lighthouse|googleother|google-inspectiontool|google-agent|mediapartners-google|adsbot|apis-google|feedfetcher/i
      .test(navigator.userAgent || '');
  } catch (e) { return false; }
})();
</script>
'''


def ga_head_html() -> str:
    """The Google tag, loaded only when the browser is not automated."""
    return AUTOMATED_JS + f'''<!-- Google tag (gtag.js), skipped for automated browsers -->
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){{dataLayer.push(arguments);}}
  if (!window.__ghAutomated) {{
    var _gs = document.createElement('script');
    _gs.async = true;
    _gs.src = 'https://www.googletagmanager.com/gtag/js?id={GA_MEASUREMENT_ID}';
    document.head.appendChild(_gs);
    gtag('js', new Date());
    gtag('config', '{GA_MEASUREMENT_ID}');
  }}
</script>
'''
