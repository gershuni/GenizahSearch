# Quick Task Plan: SEO Improvements Round 2

---
task_id: 260413-eqk
type: execute
autonomous: true
files_modified:
  - web/main.py
  - web/pages/home.py
  - genizah_translations.py
  - CHANGELOG.md
  - version.py
  - version_info.txt
  - CompileScriptGenizah.iss
  - README.md
  - docs/OPEN_ISSUES.md

must_haves:
  truths:
    - "Homepage default title leads with Hebrew intent phrase, then Hebrew brand 'אתר הגניזה של דיקטה', then English brand"
    - "Homepage default description leads with Hebrew, under 160 chars"
    - "Homepage JSON-LD includes SearchAction with correct /search?q= target"
    - "Homepage JSON-LD includes Organization with logo and Dicta URL"
    - "Per-page titles on /browse and /catalog-browse lead with Hebrew"
    - "Homepage visible h1 contains target Hebrew phrase for crawlers"
    - "Existing noindex policy on search/lists/settings/corrections/admin/profile is preserved"
    - "Version bumped and CHANGELOG updated"
  artifacts:
    - path: "web/main.py"
      provides: "Rewritten meta defaults, extended JSON-LD, Hebrew-leading per-page titles"
    - path: "web/pages/home.py"
      provides: "Hebrew-leading h1 translation key update"
    - path: "genizah_translations.py"
      provides: "Updated Hebrew translation for homepage h1"
---

<objective>
Improve SEO discoverability for Hebrew-language queries targeting the Cairo Genizah.
An SEO expert flagged: (1) slow load, (2) Hebrew semantic queries don't surface the site,
(3) meta title/description quality is weak for the Hebrew audience.

This plan addresses D1 (measure then fix), D2 (meta refresh + structured data), and D3
(version bump + deploy) from CONTEXT.md.

Purpose: Make genizahsearch.com discoverable for Hebrew queries like "לחפש בגניזה הקהירית"
Output: Updated meta tags, enriched JSON-LD, perf quick-wins, patch release
</objective>

<execution_context>
@.planning/quick/260413-eqk-seo-improvements-based-on-expert-feedbac/CONTEXT.md
</execution_context>

<context>
@web/main.py (lines 88-132: page_meta + defaults; lines 785-815: homepage route + JSON-LD)
@web/pages/home.py (h1 at line 46 uses tr('Welcome to Dicta Genizah Search'))
@genizah_translations.py (line 1466: Hebrew translation for that key)
@version.py (current: 7.7.0)

Key facts from codebase audit:
- Search route: `/search?q={query}` (confirmed at main.py:294, 825)
- Static assets: og-image.png, favicon.ico, common.css, manuscript_viewer.js
- No custom @font-face in common.css (NiceGUI uses system/CDN fonts)
- noindex pages: /search, /parallels, /lists, /settings, /corrections, /admin, /profile
- Existing JSON-LD: WebSite only (no SearchAction, no Organization)
- BreadcrumbList: not yet implemented anywhere
- Homepage h1 in Hebrew: "ברוכים הבאים לאתר הגניזה של דיקטה" (welcome phrasing, not search-intent)
</context>

<tasks>

<task type="auto">
  <name>Task 1: Lighthouse baseline measurement</name>
  <files>.planning/quick/260413-eqk-seo-improvements-based-on-expert-feedbac/lighthouse-baseline.md</files>
  <action>
Attempt to run Lighthouse CLI against production:
```
npx lighthouse https://genizahsearch.com --output=json --output-path=.planning/quick/260413-eqk-seo-improvements-based-on-expert-feedbac/lighthouse-baseline.json --chrome-flags="--headless --no-sandbox" --only-categories=performance,seo
```

If npx/lighthouse is not available or fails on this Windows box, fall back to:
1. Use curl to fetch https://www.googleapis.com/pagespeedonline/v5/runPagespeedTest?url=https://genizahsearch.com&category=PERFORMANCE&category=SEO&strategy=MOBILE
2. Parse the JSON response for: Performance score, LCP, TBT, CLS, SEO score, and any failing audits.
3. If BOTH fail, document that measurement must be done manually via https://pagespeed.web.dev/ and proceed to tasks 2-6 using known NiceGUI bottleneck patterns (large inline JS from PostHog/GA, WebSocket overhead, no font-display:swap if CDN fonts lack it).

Save a markdown summary to lighthouse-baseline.md with:
- Performance score, SEO score
- Top 3 performance bottlenecks with estimated impact
- Any SEO audit failures
- Recommended quick-wins vs out-of-scope items

Per D1: this is the "measure first" step. Do NOT speculate; report actual data or document fallback.
  </action>
  <verify>File .planning/quick/260413-eqk-seo-improvements-based-on-expert-feedbac/lighthouse-baseline.md exists and contains either real metrics or a documented fallback with rationale.</verify>
  <done>Baseline measurement captured or fallback documented. Top bottlenecks identified.</done>
</task>

<task type="auto">
  <name>Task 2: Rewrite default meta + per-page Hebrew-leading titles</name>
  <files>web/main.py, web/pages/home.py, genizah_translations.py</files>
  <action>
**A. Rewrite _DEFAULT_TITLE and _DEFAULT_DESCRIPTION in web/main.py (line 89-91). Per D2 meta layer:**

Replace:
```python
_DEFAULT_DESCRIPTION = 'Dicta Genizah Search - חיפוש גניזת קהיר. Advanced research platform with full-text search across 500,000+ Cairo Genizah manuscript fragments.'
_DEFAULT_TITLE = 'Dicta Genizah Search | Cairo Genizah Manuscript Research Platform'
```

With:
```python
_DEFAULT_TITLE = 'חיפוש בגניזה הקהירית — אתר הגניזה של דיקטה | Dicta Genizah Search'
_DEFAULT_DESCRIPTION = 'חיפוש מלא בכתבי יד מהגניזה הקהירית — טקסטים, תמונות, קטלוג ומטא-דאטה מ-255,000 קטעי גניזת קהיר. אתר הגניזה של דיקטה.'
```

Title: ~67 chars (slightly over 60-char snippet limit; full brand chain is intentional per user decision). Hebrew intent phrase first, then Hebrew brand "אתר הגניזה של דיקטה", then English brand.
Description: ~145 chars Hebrew + brand. Covers: full-text search, manuscripts, images, catalog, metadata, 255K fragments.

Also update _DEFAULT_KEYWORDS to lead with Hebrew terms:
```python
_DEFAULT_KEYWORDS = 'חיפוש בגניזה הקהירית, חיפוש גניזה, כתבי יד גניזת קהיר, גניזה קהירית, מחקר גניזה, Cairo Genizah search, Genizah manuscripts, Jewish manuscripts, Dicta Genizah Search'
```

**B. Update the homepage ui.page title (line 785):**
Change `title='Dicta Genizah Search | חיפוש גניזת קהיר'` to `title='חיפוש בגניזה הקהירית — אתר הגניזה של דיקטה | Dicta Genizah Search'` (matches new _DEFAULT_TITLE).

**C. Update per-page titles to lead with Hebrew (per D2 audit). Modify these in web/main.py:**

- `/search` (line 823, 835): `'חיפוש טקסט מלא בגניזה | Full-Text Search — Dicta Genizah Search'`
- `/browse` generic (line 898): `'עיון בכתב יד מהגניזה | Manuscript Browser — Dicta Genizah Search'`
- `/browse?sys_id=X` (line 888, 892): `f'{_shelfmark_display} — כתב יד | Dicta Genizah Search'`
- `/catalog-browse` generic (line 938): `'עיון בקטלוג הגניזה | Catalog Browse — Dicta Genizah Search'`
- `/parallels` (line 861): `'מקבילות טקסטואליות | Textual Parallels — Dicta Genizah Search'`
- `/puzzle` (line 978): `'פאזל קטעים | Fragment Puzzle — Dicta Genizah Search'`
- `/help` (line 1074): `'עזרה ומדריך | Help — Dicta Genizah Search'`
- `/discoveries` (line 1106): `'מרכז גילויים | Discoveries — Dicta Genizah Search'`
- `/about` (line 1171): `'מהי גניזת קהיר? | About the Cairo Genizah — Dicta Genizah Search'`
- `/download` (line 1191): `'הורדת תוכנה | Download — Dicta Genizah Search'`
- `/accessibility` (line 1152): `'נגישות | Accessibility — Dicta Genizah Search'`

Also update per-page descriptions on key indexable routes to lead with Hebrew:
- `/browse` generic description: `'עיון בכתבי יד מגניזת קהיר — תמונות ברזולוציה גבוהה, תעתוקים, ניווט בין דפים ומטא-דאטה מדעית מ-FJMS ו-PGP.'`
- `/catalog-browse` generic description: `'עיון ב-255,000+ כתבי יד מגניזת קהיר לפי תחום, מחבר ויצירה. קטלוג מבוסס נתוני פרידברג.'`
- `/browse?sys_id=X` description: `f'צפייה בכתב יד {_shelfmark_display} מגניזת קהיר — תמונות, תעתוק, קטלוג, ביבליוגרפיה ומטא-דאטה מדעית.'`

IMPORTANT: Do NOT change noindex on any page. search and parallels stay noindex=True.

**D. Update homepage h1 for crawlers (per D2 — visible above-the-fold content):**

In web/pages/home.py line 46, change the tr() key from:
`tr('Welcome to Dicta Genizah Search')` to `tr('Genizah Search — Full-Text Manuscript Search')`

In genizah_translations.py, add a new translation entry:
`"Genizah Search — Full-Text Manuscript Search": "אתר הגניזה של דיקטה — חיפוש בגניזה הקהירית",`

This makes the Hebrew h1 contain the exact target phrases for crawlers: "חיפוש בגניזה הקהירית" and "כתבי יד".

Keep the old translation for backward compat but the old key is no longer referenced so it's harmless.

**E. Update the ui.page title on search route (line 823):**
Match the new title from the page_meta call so the browser tab and meta tag agree.
  </action>
  <verify>
Run: python -c "from web.main import _DEFAULT_TITLE, _DEFAULT_DESCRIPTION; print(len(_DEFAULT_TITLE), _DEFAULT_TITLE); print(len(_DEFAULT_DESCRIPTION), _DEFAULT_DESCRIPTION); assert len(_DEFAULT_TITLE) < 65; assert len(_DEFAULT_DESCRIPTION) < 170; assert 'חיפוש' in _DEFAULT_TITLE; assert 'חיפוש' in _DEFAULT_DESCRIPTION; print('OK')"

Run: python -c "from web.main import page_meta; html = page_meta('/'); assert 'noindex' not in html; print('Homepage not noindex: OK')"
Run: python -c "from web.main import page_meta; html = page_meta('/search', noindex=True); assert 'noindex' in html; print('Search still noindex: OK')"
  </verify>
  <done>
- _DEFAULT_TITLE leads with Hebrew, under 65 chars
- _DEFAULT_DESCRIPTION leads with Hebrew, under 170 chars
- All indexable page titles lead with Hebrew
- All indexable page descriptions lead with Hebrew
- Homepage h1 contains target search phrases in Hebrew
- noindex policy unchanged on search/parallels/lists/settings/corrections/admin/profile
  </done>
</task>

<task type="auto">
  <name>Task 3: Extend JSON-LD with SearchAction and Organization</name>
  <files>web/main.py</files>
  <action>
**A. Add SearchAction to existing WebSite JSON-LD (per D2 structured data layer).**

Replace the existing JSON-LD block at lines 794-811 with an expanded version that includes potentialAction. The search URL target is `/search?q={search_term_string}` (confirmed from codebase: line 294 uses `/search?q=`, line 825 accepts `q: str`).

```python
    ui.add_head_html('''
    <script type="application/ld+json">
    {
        "@context": "https://schema.org",
        "@type": "WebSite",
        "name": "Dicta Genizah Search",
        "alternateName": "חיפוש בגניזה הקהירית",
        "url": "https://genizahsearch.com",
        "description": "חיפוש מלא בכתבי יד מהגניזה הקהירית — טקסטים, תמונות, קטלוג ומטא-דאטה מ-255,000 קטעי גניזת קהיר.",
        "inLanguage": ["he", "en"],
        "potentialAction": {
            "@type": "SearchAction",
            "target": {
                "@type": "EntryPoint",
                "urlTemplate": "https://genizahsearch.com/search?q={search_term_string}"
            },
            "query-input": "required name=search_term_string"
        },
        "publisher": {
            "@type": "Organization",
            "name": "Dicta — The Israel Center for Text Analysis",
            "url": "https://dicta.org.il"
        }
    }
    </script>
    ''')
```

Note: Updated alternateName to match new Hebrew target phrase. Updated description to Hebrew-leading.

**B. Add Organization JSON-LD as a separate script block, immediately after the WebSite block:**

```python
    # Structured data: Organization schema
    ui.add_head_html('''
    <script type="application/ld+json">
    {
        "@context": "https://schema.org",
        "@type": "Organization",
        "name": "Dicta Genizah Search",
        "alternateName": "חיפוש בגניזה הקהירית — דיקטה",
        "url": "https://genizahsearch.com",
        "logo": "https://genizahsearch.com/static/og-image.png",
        "parentOrganization": {
            "@type": "Organization",
            "name": "Dicta — The Israel Center for Text Analysis",
            "url": "https://dicta.org.il"
        }
    }
    </script>
    ''')
```

No sameAs field — the project has no verified social profiles. Using og-image.png as logo
(confirmed it exists in web/static/).

**C. BreadcrumbList on browse routes (per D2):**

In the browse_page_route function (around line 890), when sys_id is present, add a BreadcrumbList JSON-LD after the page_meta call:

```python
        # Structured data: BreadcrumbList for manuscript pages
        ui.add_head_html(f'''
        <script type="application/ld+json">
        {{
            "@context": "https://schema.org",
            "@type": "BreadcrumbList",
            "itemListElement": [
                {{
                    "@type": "ListItem",
                    "position": 1,
                    "name": "Home",
                    "item": "https://genizahsearch.com/"
                }},
                {{
                    "@type": "ListItem",
                    "position": 2,
                    "name": "Browse",
                    "item": "https://genizahsearch.com/browse"
                }},
                {{
                    "@type": "ListItem",
                    "position": 3,
                    "name": "{_html.escape(_shelfmark_display)}",
                    "item": "https://genizahsearch.com/browse?sys_id={sys_id}"
                }}
            ]
        }}
        </script>
        ''')
```

Import html as _html is already available at the top of page_meta. Use the same _html reference (it's imported inside page_meta; for the browse route, add `import html as _html` at the point of use or confirm it's available in scope — it is, since page_meta imports it at line 102). Actually, use the standard `html` module import that's already at the top of main.py or add a scoped import. The safest approach: use `_shelfmark_display.replace('"', '&quot;').replace('<', '&lt;')` inline to avoid injection without needing the html module import in route scope.

Similarly add BreadcrumbList for catalog-browse when filters are active (domain/author/work).
  </action>
  <verify>
Run: python -c "
import json
# Verify JSON-LD is valid JSON by extracting from the page_meta output
from web.main import dashboard_page
# Just verify the module loads without syntax errors
from web import main
print('Module loads OK')
"

Manually verify by searching the modified main.py for 'SearchAction' and 'Organization' and 'BreadcrumbList' strings.
  </verify>
  <done>
- WebSite JSON-LD includes potentialAction with SearchAction targeting /search?q=
- Organization JSON-LD present on homepage with name, url, logo, parentOrganization
- BreadcrumbList JSON-LD on /browse?sys_id=X pages (Home > Browse > Shelfmark)
- All JSON-LD blocks are valid JSON (no trailing commas, proper escaping)
  </done>
</task>

<task type="auto">
  <name>Task 4: Apply safe performance quick-wins</name>
  <files>web/main.py</files>
  <action>
Based on the codebase audit (no custom @font-face, large inline PostHog/GA scripts, NiceGUI WebSocket architecture), apply these bounded quick-wins:

**A. Defer analytics scripts.**
The PostHog script (POSTHOG_SCRIPT, ~40 lines of inline JS) and Google Analytics script (ANALYTICS_SCRIPT) are render-blocking when added via ui.add_head_html. They are not needed for first paint.

For ANALYTICS_SCRIPT (line 135-144): The gtag script tag already has `async` — good. No change needed.

For POSTHOG_SCRIPT: The inline script runs synchronously. Wrap the PostHog init in a `setTimeout(() => { ... }, 0)` or use `requestIdleCallback` to defer it past first paint:

Change the PostHog inline script to wrap the init call:
```javascript
if (window.requestIdleCallback) {
    requestIdleCallback(function() { posthog.init(...) });
} else {
    setTimeout(function() { posthog.init(...) }, 2000);
}
```

This defers PostHog initialization past LCP without losing any events (PostHog queues them).

**B. Add dns-prefetch for PostHog and Google Analytics CDNs.**
In page_meta(), add after the existing preconnect hints:
```html
<link rel="dns-prefetch" href="https://eu.i.posthog.com">
<link rel="dns-prefetch" href="https://www.googletagmanager.com">
```

These are already partially covered (PostHog CDN loads a script from `eu.i.posthog.com`). Adding dns-prefetch reduces DNS lookup time.

**C. Add meta viewport if missing.**
Check if NiceGUI injects viewport meta automatically. If NOT present, add:
```html
<meta name="viewport" content="width=device-width, initial-scale=1">
```
(NiceGUI likely handles this, but verify. If already present, skip.)

IMPORTANT: Do NOT restructure the asset pipeline, split JS bundles, or change how NiceGUI serves its framework JS/CSS. Those are out of scope per CONTEXT.md.

Log any out-of-scope bottlenecks found in Task 1 to the lighthouse-baseline.md file (append a "Deferred to future phase" section).
  </action>
  <verify>
Run: python -c "from web.main import POSTHOG_SCRIPT; assert 'requestIdleCallback' in POSTHOG_SCRIPT or 'setTimeout' in POSTHOG_SCRIPT or POSTHOG_SCRIPT == ''; print('PostHog deferred: OK')"
Run: python -c "from web.main import page_meta; html = page_meta('/'); assert 'dns-prefetch' in html; print('DNS prefetch present: OK')"
  </verify>
  <done>
- PostHog init deferred past first paint via requestIdleCallback/setTimeout
- dns-prefetch hints added for analytics CDNs
- No structural changes to NiceGUI asset pipeline
  </done>
</task>

<task type="auto">
  <name>Task 5: Update CHANGELOG, OPEN_ISSUES, version bump</name>
  <files>CHANGELOG.md, docs/OPEN_ISSUES.md, version.py, version_info.txt, CompileScriptGenizah.iss, README.md</files>
  <action>
**A. Version bump (per D3).**
Run: `python scripts/bump_version.py 7.7.1`
This is a patch bump (iterative SEO polish, not a new feature).

**B. Update CHANGELOG.md.**
Add a new section at the top (after the `---` separator), following the existing tone/format:

```markdown
## [7.7.1] - SEO Round 2 - 2026-04-13

### Improvements
- **Hebrew-leading meta tags**: Default title and description rewritten to lead with Hebrew intent phrases ("חיפוש בגניזה הקהירית") for better discoverability on Hebrew-language searches (web)
- **Per-page Hebrew titles**: All indexable page titles (homepage, browse, catalog, puzzle, discoveries, help, about, download) now lead with Hebrew before English (web)
- **SearchAction JSON-LD**: Homepage structured data extended with Sitelinks Search Box markup targeting `/search?q=` for potential in-SERP search (web)
- **Organization JSON-LD**: Added Organization schema with logo and parent organization (Dicta) on homepage (web)
- **BreadcrumbList JSON-LD**: Browse manuscript pages now emit breadcrumb structured data (Home > Browse > Shelfmark) (web)
- **Performance**: PostHog analytics deferred past first paint via requestIdleCallback, dns-prefetch hints for analytics CDNs (web)
- **Homepage h1**: Updated to contain target search phrases ("חיפוש בגניזה הקהירית — חיפוש טקסט מלא בכתבי יד") for crawler visibility (web)
```

**C. Update docs/OPEN_ISSUES.md.**
Read the file first. Add under the appropriate section:
- If an "SEO" section exists, add entries there
- Otherwise add a brief note: "SEO Round 2 shipped (7.7.1): Hebrew meta, SearchAction, Organization, BreadcrumbList, perf quick-wins"
- Mark the original SEO expert feedback items as addressed

**D. Manual steps reminder (do NOT execute these):**
- Update CLAUDE.md "Recently Changed" section with v7.7.1 entry
- Deploy to production: this is a web-only deploy (per D3 and /release skill)
  </action>
  <verify>
Run: python -c "from version import APP_VERSION; assert APP_VERSION == '7.7.1', f'Expected 7.7.1, got {APP_VERSION}'; print('Version OK')"
Run: grep -c "7.7.1" CHANGELOG.md (should be >= 1)
  </verify>
  <done>
- version.py reads 7.7.1
- CHANGELOG.md has [7.7.1] section with SEO round 2 notes
- OPEN_ISSUES.md updated
- All bump_version.py targets updated (version_info.txt, .iss, README.md)
  </done>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <what-built>
SEO Round 2: Hebrew-leading meta tags, SearchAction + Organization + BreadcrumbList JSON-LD,
PostHog deferral, homepage h1 update, version 7.7.1 bump.
  </what-built>
  <how-to-verify>
1. Run locally: `python -m web.main` — open http://localhost:8081
2. View page source on homepage:
   - Title tag should start with "חיפוש בגניזה הקהירית"
   - meta description should start with "חיפוש מלא בכתבי יד"
   - Find TWO `application/ld+json` blocks: WebSite (with SearchAction) and Organization
   - Confirm NO `noindex` on homepage
3. Navigate to /browse with a sys_id — view source:
   - Find BreadcrumbList JSON-LD with 3 items
   - Title should be "{shelfmark} — כתב יד | Dicta Genizah Search"
4. Navigate to /search — view source:
   - Confirm `noindex` is still present
   - Title leads with Hebrew
5. Check the visible h1 on homepage says "חיפוש בגניזה הקהירית — חיפוש טקסט מלא בכתבי יד" (in Hebrew mode)
6. Verify CHANGELOG.md has the 7.7.1 entry
7. If all looks good, deploy to production per /release skill (web-only)
  </how-to-verify>
  <resume-signal>Type "approved" to proceed with deploy, or describe issues to fix</resume-signal>
</task>

</tasks>

<verification>
- All Hebrew copy reads naturally and contains target search phrases
- noindex policy preserved on: /search, /parallels, /lists, /settings, /corrections, /admin, /profile
- No desktop files modified
- No new external dependencies added
- JSON-LD blocks are valid JSON (no syntax errors)
- Version is 7.7.1 across all tracked files
</verification>

<success_criteria>
- Homepage title and description lead with Hebrew intent phrases
- "חיפוש בגניזה הקהירית" appears in: title, description, h1, JSON-LD alternateName
- SearchAction JSON-LD points to /search?q={search_term_string}
- Organization JSON-LD present with logo and Dicta parent org
- BreadcrumbList on manuscript browse pages
- PostHog deferred past first paint
- Version 7.7.1 bumped and CHANGELOG updated
- Site deployed to production with changes live
</success_criteria>

<output>
After completion, verify live site with:
```bash
curl -s https://genizahsearch.com | grep -o '<title>[^<]*</title>'
curl -s https://genizahsearch.com | grep -c 'SearchAction'
curl -s https://genizahsearch.com | grep -c 'Organization'
```
</output>
