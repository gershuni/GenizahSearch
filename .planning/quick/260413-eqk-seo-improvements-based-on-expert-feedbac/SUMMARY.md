# Quick Task 260413-eqk: SEO Improvements Round 2 -- Summary

## One-liner
Bilingual English-leading meta tags, JSON-LD structured data, PostHog deferral, and client/server title consistency fix for Hebrew discoverability on genizahsearch.com.

## Commits

### Original execution (5 commits)
| # | Hash | Message |
|---|------|---------|
| 1 | c42c4fa5 | Lighthouse baseline measurement (manual fallback) |
| 2 | 99018426 | Hebrew-leading meta tags + per-page titles + homepage h1 |
| 3 | ddc2bc53 | SearchAction + Organization + BreadcrumbList JSON-LD |
| 4 | 48282467 | PostHog deferral + dns-prefetch hints |
| 5 | e0cea8f2 | Version bump 7.7.1 + CHANGELOG + OPEN_ISSUES |

### Revision commits (3 commits, pre-deploy)
| # | Hash | Message |
|---|------|---------|
| 6 | aba02f6b | Revise titles to English-leading bilingual per Codex review |
| 7 | 945723ad | Unify client-side document.title with server-side browse title format |
| 8 | 06b808db | Correct overclaimed SEO wins and add honest limitations |

## Files Modified (revision commits only)
- web/main.py
- web/pages/browse.py
- CHANGELOG.md
- docs/OPEN_ISSUES.md
- .planning/quick/260413-eqk-seo-improvements-based-on-expert-feedbac/lighthouse-baseline.md

## Revision Log

### What the original execution did wrong
Mass Hebrew-first overcorrection to all page titles and descriptions. Every route had its title rewritten to lead with Hebrew text, based on CONTEXT.md D2 ("Hebrew-first audience"). Codex review identified this makes the English brand invisible in truncated SERP snippets, adds noise to low-intent pages, and hides shelfmarks on manuscript pages.

### What Codex flagged
- Title language strategy: too Hebrew-first, should be English-leading bilingual
- Client-side title override bug: browse.py:598 produced different format than server
- CHANGELOG overclaims: "Hebrew-leading" framing, "potential in-SERP search" for deprecated feature
- lighthouse-baseline.md: stale SEO assertions framed as data

### What was changed
- Commit 6: All per-page titles revised to English-leading bilingual (or English-only for low-intent/noindex)
- Commit 7: Client-side browse title bug fixed to match server format
- Commit 8: CHANGELOG rewritten honestly, OPEN_ISSUES fixed, lighthouse file replaced

### Final title strategy
- Homepage: Bilingual (English brand + Hebrew search phrase + Hebrew brand)
- Indexable pages (/browse, /catalog-browse, /about): English-leading bilingual
- Manuscript pages (/browse?sys_id=X): Shelfmark-first, English brand suffix
- Low-intent pages (/help, /download, /accessibility): Concise English
- noindex pages (/search, /parallels): Plain English

### Items kept from original (not reverted)
- _DEFAULT_KEYWORDS (Hebrew-led) -- meta keywords ignored by Google
- All JSON-LD (WebSite + SearchAction + Organization + BreadcrumbList)
- Homepage h1 change (Hebrew search phrase for crawlers)
- PostHog requestIdleCallback deferral + dns-prefetch hints
- _DEFAULT_DESCRIPTION -- already bilingual

### Items deferred to future phases
- Real Lighthouse / PSI measurement on production
- Search Console URL Inspection
- Per-language URL architecture (/he/ vs /en/ + hreflang)
- Server-side rendered browse body content (WebSocket hydration gap)
- Core Web Vitals optimization based on real data
