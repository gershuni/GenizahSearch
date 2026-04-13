# Lighthouse Baseline — NOT MEASURED

## Status: Placeholder

**No real measurement was performed** for the v7.7.1 release. The Lighthouse CLI was unavailable on the Windows dev machine, and the PageSpeed Insights API call without an API key returned errors.

## What was actually done
A codebase audit identified likely bottlenecks based on architectural patterns (NiceGUI WebSocket-only hydration, inline analytics scripts, no font-display:swap on potential CDN fonts). Two safe quick-wins were applied in commit `48282467`:
- PostHog analytics deferred past first paint via `requestIdleCallback`
- `dns-prefetch` hints added for analytics CDNs

## What was NOT done
- No actual LCP / INP / CLS / TBT measurements
- No SEO audit pass count
- No targeted fixes based on real bottlenecks
- No before/after comparison

## Required follow-up after deploy
1. Run https://pagespeed.web.dev/ against production homepage and `/browse?sys_id=X`
2. Run Search Console URL Inspection on at least the homepage, `/about`, and one manuscript page
3. File real perf issues in `docs/OPEN_ISSUES.md` based on actual data
4. Open a follow-up phase if Core Web Vitals fail thresholds
