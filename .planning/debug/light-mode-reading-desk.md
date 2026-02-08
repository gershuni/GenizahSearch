---
status: resolved
trigger: "Light Mode reading desk: Back to Page View button, header icons, fragment count badge are white-on-white (invisible). 11-08 fix supposedly applied but still broken."
created: 2026-02-08T00:00:00Z
updated: 2026-02-08T00:01:00Z
---

## Current Focus

hypothesis: CONFIRMED - Global .q-card CSS rule with !important overrides the header card's inline green gradient background to white
test: CSS specificity analysis of .q-card rule vs inline style
expecting: !important in stylesheet beats inline style without !important
next_action: Return root cause

## Symptoms

expected: Back to Page View button, header icons, and fragment count badge should be visible in Light Mode
actual: These elements are white-on-white (invisible) in Light Mode
errors: No errors - cosmetic visibility issue
reproduction: Open reading desk in Light Mode
started: Persists despite 11-08 fix plan

## Eliminated

- hypothesis: "11-08 fixes were not applied or were reverted"
  evidence: "Git commit 65e963e confirmed applied. Current code at browse.py:2426,2431,2434,2442 has the fixes (inline !important styles and text-color=white prop)"
  timestamp: 2026-02-08

- hypothesis: "text-color=white prop or color: white !important on text elements doesn't work"
  evidence: "These approaches DO work for text color. The problem is not text color -- it's the BACKGROUND being overridden to white."
  timestamp: 2026-02-08

## Evidence

- timestamp: 2026-02-08
  checked: browse.py lines 2421-2442 (reading desk header card)
  found: Card uses ui.card().style('background: linear-gradient(135deg, #15803d 0%, #166534 100%);') -- inline style WITHOUT !important
  implication: This inline background is vulnerable to CSS rules with !important

- timestamp: 2026-02-08
  checked: main.py line 595-596 (global .q-card CSS)
  found: ".q-card { background: var(--bg-card) !important; }" -- applies to ALL q-card elements with !important
  implication: In light mode, --bg-card = #ffffff, so ALL cards get white background, overriding any inline style

- timestamp: 2026-02-08
  checked: CSS specificity rules
  found: !important in a stylesheet class rule (.q-card) beats a regular inline style. Only an inline style WITH !important can override a class !important.
  implication: The header card's green gradient is overridden to white, creating white-on-white for all child elements

- timestamp: 2026-02-08
  checked: Dialog headers (browse.py:2043-2047 and 2256-2260)
  found: Dialogs put the green gradient on a ui.row() INSIDE the card, not on the card itself. The row is a div, not a .q-card, so its background is not overridden.
  implication: This explains why dialog headers might work while the reading desk header doesn't -- different element hierarchy

- timestamp: 2026-02-08
  checked: Git commit 65e963e (11-08 fix diff)
  found: The fix changed text color approaches but did NOT address the card background override. The fix was correct about text color but misdiagnosed the actual problem.
  implication: 11-08 fixed the wrong layer -- text color was not the issue, background color was

## Resolution

root_cause: Global CSS rule `.q-card { background: var(--bg-card) !important; }` in web/main.py:596 overrides the reading desk header card's inline green gradient background to white (#ffffff) in Light Mode. Since all text/icons/badge in the header are styled white, they become invisible (white on white). The 11-08 fix correctly addressed text color styling but misdiagnosed the root issue -- the BACKGROUND was being overridden, not the text color.

fix: Add !important to the card's inline background style, OR change the structure to use a non-card element (ui.element/ui.row) for the header bar, OR apply the green gradient on a child row inside the card (matching the pattern used by dialog headers).

verification:
files_changed: []
