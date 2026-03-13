---
status: testing
phase: 46-dicta-translation
source: [46-01-SUMMARY.md, 46-02-SUMMARY.md, 46-03-SUMMARY.md, 46-04-SUMMARY.md, 46-05-SUMMARY.md]
started: 2026-03-13T12:00:00Z
updated: 2026-03-13T12:00:00Z
---

## Current Test
<!-- OVERWRITE each test - shows where we are -->

number: 1
name: Web Translation Toggle in Sidebar
expected: |
  In the web app sidebar, between the language and theme toggles, there is a "Show translations" toggle. Clicking it ON persists the preference. When ON, translated text appears in search results and browse views. When OFF, only original-language text is shown.
awaiting: user response

## Tests

### 1. Web Translation Toggle in Sidebar
expected: In the web app sidebar, between the language and theme toggles, there is a "Show translations" toggle. Clicking it ON persists the preference. When ON, translated text appears in search results and browse views. When OFF, only original-language text is shown.
result: [pending]

### 2. Clickable Translated/Original Badge (Search)
expected: With translation toggle ON, perform a search. Results with PGP descriptions show a clickable "Translated" badge (light blue). Clicking the badge toggles the text inline between the Hebrew translation (RTL) and the English original (LTR). The badge label switches between "Translated" and "Original".
result: [pending]

### 3. Clickable Translated/Original Badge (Browse)
expected: With translation toggle ON, open a manuscript in the browse page that has a PGP description. The description shows with a clickable "Translated"/"Original" badge that toggles between Hebrew translation and English original inline.
result: [pending]

### 4. Subtitle Display for Short Hebrew Titles
expected: With translation toggle ON, manuscripts with very short Hebrew titles (< 15 chars) show an additional subtitle line with the EN→HE translation, separated by an em dash. Visible in both search results and browse view.
result: [pending]

### 5. Browse Page Shelfmark/sys_id URL Support
expected: Navigate to the browse page with a shelfmark in the URL (e.g., ?shelfmark=T-S 12.123). The manuscript loads directly. Also, entering a sys_id (starts with 99, all digits) in the browse search box loads that record directly.
result: [pending]

### 6. Dicta-Powered Translate Button
expected: On-demand translate buttons (for community content like corrections/comments) use the Dicta Translation API. Clicking a translate button produces a translation — it should NOT show MyMemory errors or use the old API.
result: [pending]

### 7. FJMS Catalog Dialog — RunningTitle/FullText Translations
expected: Open an FJMS catalog detail dialog (from search or browse). With translation toggle ON, RunningTitle and FullText fields show translated versions where available. The dialog displays the translation alongside or in place of the original.
result: [pending]

### 8. Desktop Translation Toggle
expected: In the desktop app (PyQt6), there is a translation toggle setting. When enabled, search results and browse views show translated text with clickable toggle badges, matching the web app behavior.
result: [pending]

### 9. No Translated-Match Badges in Main Search
expected: Perform a search in the main search. There should be NO "Translated match" badges appearing on search results. Translation search was intentionally removed from main search — it only applies in browse catalog text filter.
result: [pending]

## Summary

total: 9
passed: 0
issues: 0
pending: 9
skipped: 0

## Gaps

[none yet]
