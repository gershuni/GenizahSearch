# Quick Task 260413-eqk: SEO Improvements (Round 2)

## Trigger
SEO expert spent 60 seconds on https://genizahsearch.com and reported:
1. **Slow load** — site loads very slowly
2. **Semantic search miss** — querying "לחפש בגניזה הקהירית" (Hebrew semantic phrasing) does not surface the site
3. **Meta title/description quality** — flagged as problematic

This is feedback on top of phase 7.3.1 ("SEO Foundation & Shareable Browse URLs") which already shipped per-page meta tags, sitemap, JSON-LD on homepage, canonical, OG/Twitter cards, preconnect hints, and noindex policy.

## Discussion Decisions

### D1 — Performance investigation approach
**Decision:** Measure first, then fix. Run Lighthouse + PageSpeed Insights on production homepage and 1–2 representative routes (search, browse) to identify concrete bottlenecks (LCP, INP, TBT, payload size, render-blocking resources). Plan must be data-driven, not speculative.

**Why:** User has not measured; "slow" is subjective. Fixing the wrong thing wastes effort. NiceGUI + WebSocket architecture has unusual perf characteristics — assumptions from typical SPA SEO advice may not apply.

**How to apply:** Step 1 of the plan is "measure"; step 2+ is "fix top N bottlenecks within scope of a quick task". Out-of-scope perf work gets logged to MEMORY/notes for a future phase.

### D2 — Semantic discoverability scope
**Decision:** Both meta-tag refresh AND structured-data enrichment, in that order of priority.

**Meta layer:**
- Rewrite `_DEFAULT_TITLE` and `_DEFAULT_DESCRIPTION` to lead with strong Hebrew query-matching phrasing ("חיפוש בגניזה הקהירית", "חיפוש מלא בכתבי יד מהגניזה הקהירית", etc.) before the brand name.
- Audit per-page titles/descriptions on search.py, browse.py, lists.py for keyword/intent alignment.
- Ensure homepage `<h1>` and visible above-the-fold content contains the target phrases (not just meta).

**Structured data layer:**
- Add `WebSite` JSON-LD with `potentialAction`/`SearchAction` (Sitelinks Search Box) so Google can offer in-SERP search.
- Add `Organization` JSON-LD (name, url, logo, sameAs).
- Add basic `BreadcrumbList` on browse routes.

**Why:** User said "doesn't know" priority — picked best-practice combo. Meta tags are the foundation; structured data unlocks rich results and AI-overview eligibility, which directly addresses "didn't find the site for semantic queries".

**How to apply:** Bundle into the same plan; meta-tag work is 1 file (`web/main.py`), structured-data is additive head HTML on homepage.

### D3 — Deploy scope
**Decision:** Full release flow at end — version bump (`scripts/bump_version.py`), CHANGELOG entry, server deploy.

**Why:** User explicitly said "גם לפרוס ל-prod בסוף". SEO changes have zero value until live and re-indexed by Google.

**How to apply:** Last task in plan is the release. Use `/release web-only` or equivalent (no desktop changes here).

## Locked Constraints
- Web-only (no desktop changes — desktop has no SEO surface).
- No new external dependencies.
- Must not regress existing per-page metadata or noindex policy from phase 7.3.1.
- All Hebrew copy must be RTL-correct and natural (this is a Hebrew-first audience).
- Atomic commits per task.

## Out of Scope
- Major perf refactors (JS bundle splitting, CDN migration, image format conversion at scale) — log as future phase if Lighthouse surfaces them.
- Sitelinks Search Box requires Google verification + crawl cycle; we ship the markup, results take weeks.
- Desktop app SEO (N/A).
- Backlink / off-page SEO.

## Existing SEO State (verified)
- `web/main.py:88-132` — `page_meta()` builds per-page tags with canonical + OG + Twitter
- `web/main.py:790-810` — homepage `WebSite` JSON-LD already exists (need to extend with SearchAction)
- `_DEFAULT_TITLE` and `_DEFAULT_DESCRIPTION` — English-leading; needs Hebrew-leading rewrite for the target audience
- Preconnect hints already present for NLI IIIF + Cambridge CUDL
- Sitemap shipped in 7.3.1 (manuscript URLs in 40K chunks)
