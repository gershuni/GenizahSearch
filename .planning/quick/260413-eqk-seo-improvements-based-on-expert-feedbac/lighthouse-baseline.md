# Lighthouse Baseline — 2026-04-13

## Measurement Status: Manual Fallback

Both automated approaches failed:
- **Lighthouse CLI**: Not installed on this Windows dev machine (no `npx lighthouse`).
- **PageSpeed Insights API**: Returns 404 — requires API key (not configured).

**Manual verification required:** Visit https://pagespeed.web.dev/ and run against `https://genizahsearch.com` for mobile + desktop.

## Known Bottleneck Analysis (from codebase audit)

Based on the NiceGUI architecture and codebase inspection:

### Top 3 Performance Bottlenecks (estimated)

1. **NiceGUI WebSocket framework JS** — NiceGUI injects ~200-400KB of framework JavaScript (Vue.js, Quasar, socket.io). This is the dominant render-blocking payload and is NOT addressable without switching frameworks. **Out of scope.**

2. **PostHog inline script (~40 lines)** — Runs synchronously before first paint. The loader creates a script element and inserts it, but `posthog.init()` runs immediately. Deferring init past LCP is a safe quick-win. **Estimated impact: 50-200ms TBT reduction.**

3. **Google Analytics** — The gtag script already has `async` attribute. Minimal additional optimization possible. **No change needed.**

### SEO Audit Issues (known from codebase)

- Default title is English-leading: "Dicta Genizah Search | Cairo Genizah..." — weak for Hebrew semantic queries
- Default description is English-leading — same issue
- Homepage JSON-LD lacks SearchAction (no Sitelinks Search Box eligibility)
- No Organization schema
- No BreadcrumbList on browse pages
- Homepage h1 uses welcome phrasing, not search-intent phrasing

### Quick-Wins (in scope for this task)

1. Defer PostHog init via `requestIdleCallback` / `setTimeout` fallback
2. Add `dns-prefetch` hints for analytics CDNs
3. Rewrite meta tags to lead with Hebrew intent phrases
4. Add SearchAction, Organization, BreadcrumbList JSON-LD
5. Update homepage h1 for crawler relevance

### Deferred to Future Phase

- NiceGUI framework JS bundle size — architectural constraint, not addressable
- Image optimization (WebP/AVIF conversion) — NLI IIIF serves JPEG; would require proxy layer
- Server-side rendering — NiceGUI is WebSocket-based; SSR not applicable
- Critical CSS extraction — NiceGUI controls CSS injection
- Service Worker / PWA caching — out of scope for SEO quick task
