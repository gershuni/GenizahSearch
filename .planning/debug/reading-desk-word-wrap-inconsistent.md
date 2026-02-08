---
status: diagnosed
trigger: "Word wrap in reading desk text pane works for Goitein's text but not for other texts"
created: 2026-02-08T12:00:00Z
updated: 2026-02-08T12:30:00Z
---

## Current Focus

hypothesis: Right pane flex item missing min-width:0, so content with long unbroken lines prevents container from constraining width
test: Examined all CSS in layout chain from flex container to text label
expecting: Missing min-width:0 on right pane card, causing texts without frequent newlines to overflow
next_action: Report root cause

## Symptoms

expected: All text versions (PGP editions, translations, V0.8) should wrap within the reading desk text pane
actual: Goitein's edition text wraps, but other texts (V0.8 HTR, other editions, translations) overflow or don't wrap
errors: None
reproduction: Open reading desk, switch between text versions via version selector
started: After 11-08 fix removed overflow:hidden -- that fix unmasked this deeper issue

## Eliminated

- hypothesis: Text labels missing word-wrap CSS
  evidence: All three rendering paths (initial at 2795-2801, PGP version change at 2713-2718, V0.8 at 2733-2738) consistently apply white-space:pre-wrap, overflow-wrap:break-word, word-break:break-word
  timestamp: 2026-02-08T12:10:00Z

- hypothesis: overflow:hidden on text container clipping text
  evidence: Already fixed in 11-08 (commit f5cb3e6). Line 2774 now has no overflow:hidden.
  timestamp: 2026-02-08T12:12:00Z

- hypothesis: Different rendering paths for different text types
  evidence: All text types go through identical ui.label().style() with same CSS properties
  timestamp: 2026-02-08T12:15:00Z

## Evidence

- timestamp: 2026-02-08T12:05:00Z
  checked: All word-wrap CSS in reading desk text rendering (lines 2713-2718, 2733-2738, 2795-2801)
  found: All three code paths apply identical CSS: white-space:pre-wrap; overflow-wrap:break-word; word-break:break-word
  implication: CSS on the text labels is correct and consistent

- timestamp: 2026-02-08T12:08:00Z
  checked: Layout chain for right pane: flex container -> card -> scroll_area -> column -> label
  found: |
    Line 2482-2483: Outer flex container: display:flex; flex-direction:row; gap:16px; width:100%
    Line 2487: Left pane card: flex: 0 0 50% (fixed 50% width)
    Line 2610: Right pane card: flex: 1 1 auto (NO min-width:0, NO overflow constraint)
    Line 2614: Scroll area: w-full, flex:1
    Line 2774: Text column: w-full px-3 py-2 (NO overflow constraint)
  implication: Right pane card has flex-basis:auto and default min-width:auto

- timestamp: 2026-02-08T12:12:00Z
  checked: CSS flexbox min-width behavior
  found: In CSS flexbox, the default min-width is 'auto', meaning a flex item cannot shrink below its content's minimum intrinsic width. This prevents overflow-wrap from triggering on child elements because the container expands to fit the content rather than constraining it.
  implication: Without min-width:0 on the flex item, long text lines make the card expand instead of wrapping

- timestamp: 2026-02-08T12:15:00Z
  checked: Why Goitein's text wraps but others don't
  found: Goitein's PGP editions are scholarly transcriptions with line-by-line formatting. Each manuscript line is its own text line with newlines. white-space:pre-wrap respects these newlines, keeping lines short. V0.8 HTR text and other editions may have longer paragraphs or continuous text without frequent newlines.
  implication: Goitein's text never exceeds container width because it has natural line breaks. Other texts rely on container width to trigger wrapping, which fails because container expands instead.

- timestamp: 2026-02-08T12:18:00Z
  checked: Full manuscript view path (lines 2192-2195)
  found: This path also lacks overflow-wrap and word-break CSS, but is not in a flex row so less affected
  implication: Secondary issue in view_all path

- timestamp: 2026-02-08T12:20:00Z
  checked: Single-page view render_text_content (lines 3310-3314)
  found: Has overflow-wrap:break-word and word-break:break-word (fixed in 11-08), and its parent is also flex-based but has text_panel_flex with flex:1 1 auto (same pattern at line 3235)
  implication: Single-page view may have the same latent bug

- timestamp: 2026-02-08T12:22:00Z
  checked: Single-page layout (lines 3151-3158)
  found: |
    Line 3151-3152: viewer-panels: display:flex; flex-direction:row; gap:16px; width:100%
    Line 3157-3158: Image panel: flex: 0 0 50%
    Line 3235: Text panel: flex: 1 1 auto (same pattern as reading desk)
  implication: Same min-width:auto issue exists in single-page view

## Resolution

root_cause: |
  The reading desk right pane card (line 2610) uses `flex: 1 1 auto` without
  `min-width: 0`. In CSS flexbox, the default `min-width: auto` prevents flex
  items from shrinking below their content's intrinsic minimum width.

  When text content has long unbroken lines (V0.8 HTR, non-Goitein editions,
  translations with long paragraphs), the card expands to accommodate the
  content rather than constraining it, making `overflow-wrap: break-word` on
  the label ineffective because the container never reaches a point where
  wrapping is needed.

  Goitein's text appears to wrap correctly only because his scholarly
  transcription format uses frequent newlines (one per manuscript line),
  keeping lines naturally short. The wrapping is actually happening at
  the `\n` characters via `white-space: pre-wrap`, NOT at the container
  boundary via `overflow-wrap`.

  The same pattern exists in the single-page view (line 3235).

fix: |
  1. Reading desk right pane card (line 2610): Add `min-width: 0;` to style
  2. Single-page text panel (line 3235): Add `min-width: 0;` to text_panel_flex
  3. Full manuscript view text labels (line 2192-2195): Add overflow-wrap and word-break CSS
  4. Reading desk text container column (line 2774): Add `min-width: 0;` for safety

verification: []
files_changed: []
