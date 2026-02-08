---
status: resolved
trigger: "Investigate why text pane word wrap is not working in the web reading desk"
created: 2026-02-08T00:00:00Z
updated: 2026-02-08T00:20:00Z
---

## Current Focus

hypothesis: Text wrapping CSS applied to labels but container width not constrained
test: Read browse.py to find all text label creation and container styling
expecting: Find width constraints or overflow settings preventing wrap
next_action: Read browse.py completely

## Symptoms

expected: Long Hebrew text should wrap within text pane width
actual: Long Hebrew text runs off right edge without wrapping
errors: None reported
reproduction: View reading desk with long Hebrew text
started: After fix 11-06 W5 that added overflow-wrap/word-break CSS

## Eliminated

## Evidence

- timestamp: 2026-02-08T00:05:00Z
  checked: browse.py lines 2667-2757 (reading desk text pane)
  found: Three locations have overflow-wrap/word-break CSS on ui.label (lines 2670, 2690, 2753)
  implication: The text labels have correct wrapping CSS

- timestamp: 2026-02-08T00:06:00Z
  checked: browse.py line 2728 (text container)
  found: Text container uses `ui.column().classes('w-full px-3 py-2').style('overflow: hidden;')`
  implication: Container has overflow:hidden which might hide wrapped text

- timestamp: 2026-02-08T00:07:00Z
  checked: browse.py line 2568 (scroll area parent)
  found: Scroll area uses `.classes('rd-text-pane w-full')`
  implication: Parent container has w-full class for width constraint

- timestamp: 2026-02-08T00:08:00Z
  checked: browse.py line 2564 (right pane card)
  found: Card uses `style('flex: 1 1 auto; min-height: 70vh; display: flex; flex-direction: column;')`
  implication: Card flexes to fill available space

- timestamp: 2026-02-08T00:10:00Z
  checked: Compared to render_text_content (line 3258-3267, single-page viewer)
  found: Single-page viewer has NO overflow:hidden on scroll area, only on inner container. Text label at line 3264-3267 is MISSING overflow-wrap/word-break CSS entirely
  implication: Single-page viewer has same issue but worse (missing wrapping CSS)

- timestamp: 2026-02-08T00:12:00Z
  checked: Understanding of overflow:hidden behavior
  found: CSS overflow:hidden clips content that exceeds container bounds, both horizontally and vertically. When text exceeds container width, it's hidden rather than wrapped.
  implication: Even with word-break CSS on the label, parent overflow:hidden prevents proper display

- timestamp: 2026-02-08T00:13:00Z
  checked: Purpose of overflow:hidden at line 2728
  found: Text container uses ui.column().classes('w-full px-3 py-2') with overflow:hidden. The w-full class should constrain width to parent.
  implication: overflow:hidden seems unnecessary - the scroll area parent should handle overflow, not the text container

- timestamp: 2026-02-08T00:14:00Z
  checked: Image viewer container styling (line 2536)
  found: Image viewer ALSO uses overflow:hidden but this is appropriate for clipping draggable/zoomable images
  implication: overflow:hidden serves a purpose for images (clip dragged content) but NOT for text (prevents wrapping)

- timestamp: 2026-02-08T00:15:00Z
  checked: Width constraint mechanism
  found: Text container has w-full class (100% width of parent). Parent scroll area has w-full. Parent card has flex:1. This creates proper width constraint.
  implication: Width is already constrained by w-full class. The overflow:hidden is NOT needed for width constraint and actively breaks text wrapping.

## Resolution

root_cause: Text container at line 2728 has `style('overflow: hidden;')` which clips horizontally overflowing text instead of allowing it to wrap. The ui.label has correct wrapping CSS (overflow-wrap/word-break), but the parent container's overflow:hidden prevents proper text reflow.

The issue is NOT the label styling - the word-wrap CSS is correctly applied to the ui.label elements at lines 2670, 2690, and 2753. The problem is the parent container at line 2728 (.classes('w-full px-3 py-2').style('overflow: hidden;')) which hides overflow instead of allowing wrapped text to expand vertically.

**Root Cause Mechanism:**
1. ui.label has overflow-wrap: break-word (correct)
2. Parent ui.column has overflow: hidden (incorrect)
3. When text tries to wrap, it expands vertically
4. Parent clips the vertically expanded content
5. Result: text appears to run off edge because wrapped portion is hidden

**Why overflow:hidden was added:** Likely copied from image viewer pattern (line 2536) where it serves a purpose (clip draggable images). But for text containers, it's harmful.

**Secondary Issue:** Single-page viewer (render_text_content at line 3264-3267) is MISSING the overflow-wrap/word-break CSS entirely. This affects the non-reading-desk single manuscript view.

fix:
1. Reading desk: Remove `style('overflow: hidden;')` from line 2728 text container
2. Single-page viewer: Add overflow-wrap/word-break CSS to ui.label at line 3264

verification:
1. View reading desk with long Hebrew text, verify it wraps within container width
2. View single manuscript page with long Hebrew text, verify wrapping there too

files_changed: [web/pages/browse.py]
