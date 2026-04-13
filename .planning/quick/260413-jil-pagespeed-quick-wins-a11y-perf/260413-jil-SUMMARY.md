---
phase: quick
plan: 260413-jil
subsystem: web-frontend
tags: [a11y, perf, pagespeed, lighthouse, wcag]
dependency_graph:
  requires: []
  provides: [a11y-compliance, font-display-swap, conditional-preconnect]
  affects: [web/main.py, web/pages/home.py, web/static/common.css]
tech_stack:
  added: []
  patterns: [starlette-middleware-css-injection, nicegui-template-patch]
key_files:
  created: []
  modified:
    - web/main.py
    - web/pages/home.py
    - web/static/common.css
decisions:
  - "Patch NiceGUI template in-memory at startup for html lang (idempotent, re-applies on boot)"
  - "Use Starlette middleware for font-display injection rather than copying/overriding fonts.css"
  - "Light theme --text-muted changed from #94a3b8 (2.34:1) to #64748b (4.63:1 on white)"
  - "Global link color override with --primary-700 instead of Quasar default #5898d4"
metrics:
  duration: ~3 min
  completed: 2026-04-13
  tasks_completed: 2
  tasks_total: 3
  files_modified: 3
---

# Quick Task 260413-jil: PageSpeed Quick Wins (A11y + Perf) Summary

Six surgical PageSpeed Insights fixes -- 4 accessibility (html lang, aria-labels, color contrast, heading hierarchy) and 2 performance (font-display: swap, conditional iiif preconnect) to push Lighthouse scores toward a11y >= 95 and perf >= 93.

## Completed Tasks

### Task 1: A11y fixes (3937384c)

**Fix 1 -- html lang attribute:**
- Added `_patch_html_lang_attribute()` startup function that patches NiceGUI's `index.html` template to include `lang="he"` on the `<html>` tag
- Idempotent: re-applies on every boot (survives pip upgrades)
- JS in `apply_theme_immediately()` still overrides per user preference at runtime

**Fix 2 -- aria-labels on icon-only buttons:**
- Help button in header: `aria-label="Help"`
- What's New close button: `aria-label="Dismiss"`
- Theme toggle buttons (light/parchment/dark): descriptive aria-labels
- Citation footer copy/close buttons (full + compact rows): aria-labels
- OCR banner close button (home.py): `aria-label="Dismiss"`
- Hero search button (home.py): `aria-label="Search"`

**Fix 3 -- Color contrast:**
- Light theme `--text-muted` changed from `#94a3b8` (slate-400, 2.34:1 on white) to `#64748b` (slate-500, 4.63:1 on white)
- Dark and parchment theme values unchanged (already sufficient)
- Global link color override: `a { color: var(--primary-700); }` replaces Quasar default `#5898d4` (3.06:1) with `#047857` (5.44:1)
- Dark theme links use `--primary-300` for adequate contrast on dark backgrounds
- Explicit inline/class colors preserved via `a[style*="color"], .text-white a, .citation-link { color: inherit; }`

**Fix 4 -- Heading order:**
- "What is the Cairo Genizah?" promoted from `h3` to `h2` in `web/pages/home.py`
- Heading hierarchy now: h1 -> h2 ("What is...") -> h2 ("Research Tools") -> h3s (cards)

### Task 2: Perf fixes (1fcbe6b2)

**Fix 5 -- font-display: swap:**
- Added Starlette HTTP middleware `_inject_font_display_swap` that intercepts `fonts.css` responses
- Injects `font-display: swap;` into each `@font-face` block
- Path check short-circuits all non-font requests (zero overhead)
- Prevents invisible text during font loading (~1200ms on slow connections)

**Fix 6 -- Conditional iiif preconnect:**
- Added `needs_iiif: bool = False` parameter to `page_meta()`
- Homepage no longer emits `<link rel="preconnect" href="https://iiif.nli.org.il">`
- Routes that show manuscript images opt in: `/search`, `/browse`, `/puzzle`
- All other routes (help, about, lists, settings, corrections, discoveries, etc.) skip the IIIF preconnect

### Task 3: Human verification (checkpoint)

**Status:** Awaiting manual Lighthouse audit

**What to verify:**
1. Start the web app: `python -m web.main`
2. Open Chrome DevTools on homepage (localhost:8081)
3. Run Lighthouse audit (desktop, a11y + perf categories)
4. Verify: a11y score >= 95, perf score >= 93
5. Specific checks:
   - Elements panel: `<html lang="he">` present in initial DOM
   - Inspect hero search button: has aria-label attribute
   - Inspect theme toggle buttons in sidebar: have aria-labels
   - View Source: no `iiif.nli.org.il` preconnect on homepage
   - Navigate to `/browse/CUL/T-S+12.1`: iiif preconnect IS present
   - Toggle to dark mode: verify text is still readable, no regressions
   - Toggle to parchment mode: verify text readable
   - Check heading hierarchy: h1 -> h2 ("What is...") -> h2 ("Research Tools") -> h3s
6. Check font loading: Network tab, filter fonts -- should show "swap" in @font-face

## Deviations from Plan

None -- plan executed exactly as written.

## Known Stubs

None.

## Self-Check: PENDING

Self-check deferred until human verification completes (Task 3 checkpoint).
