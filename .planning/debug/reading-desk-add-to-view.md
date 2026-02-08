---
status: diagnosed
trigger: "Add to View button only shows 1 fragment; second manuscript replaces first instead of adding alongside"
created: 2026-02-08T00:00:00Z
updated: 2026-02-08T00:00:00Z
---

## Current Focus

hypothesis: browse_load() called during reading desk mode destroys the visual state and creates inconsistency
test: trace the full Add to View -> Navigate -> Add to View flow
expecting: state accumulates but rendering is overwritten by navigate
next_action: document root cause

## Symptoms

expected: Clicking "Add to View" on manuscript A starts reading desk. Navigating to B and clicking "Add to View" again should ADD B alongside A (cumulative).
actual: Only B is shown (replaces A). Both Bug 1 (Test 11) and Bug 2 (Test 12).
errors: No errors; just wrong behavior (replacement instead of accumulation)
reproduction: 1) View manuscript A, click "Add to View" 2) Navigate to B via Go button 3) Click "Add to View" again -- only B shows
started: Since initial implementation (plan 11-04)

## Eliminated

(none)

## Evidence

- timestamp: 2026-02-08
  checked: _browse_add_to_view (line 7454)
  found: Correctly branches: if desk not active -> enter_reading_desk(); else -> rd_add_entry()
  implication: Logic for the button click itself is correct

- timestamp: 2026-02-08
  checked: _browse_enter_reading_desk (line 7318)
  found: Creates brand new ReadingDeskState() at line 7332, always replaces self.browse_reading_desk_state at line 7364
  implication: Entering reading desk always starts fresh (correct for first entry)

- timestamp: 2026-02-08
  checked: browse_load() (line 16021) -- called by Go button
  found: Does NOT check browse_reading_desk_active. Overwrites browse_text at line 16031/16379. Clears viewer at line 16032. Changes current_browse_sid at line 16137. Renders single-page in text pane at line 16159.
  implication: Navigation during reading desk mode destroys the reading desk visual rendering without exiting the reading desk state

- timestamp: 2026-02-08
  checked: on_browse_enriched_loaded (line 7071)
  found: At line 7142, loads images into normal browse_viewer -- overwriting reading desk image layout
  implication: Normal image enrichment runs on top of reading desk mode

- timestamp: 2026-02-08
  checked: browse_render_page (line 16342)
  found: At line 16379, sets browse_text HTML to single-page content. At line 16359, may re-show browse_viewer.
  implication: Text pane reading desk HTML is overwritten by normal page render

- timestamp: 2026-02-08
  checked: _browse_rd_add_entry (line 7472)
  found: Correctly appends entry to state.entries (line 7511), checks duplicates, calls _browse_rd_render()
  implication: The add-entry logic itself is sound

- timestamp: 2026-02-08
  checked: _browse_exit_reading_desk callers
  found: Only called from: Exit button (line 6855), rd-navigate link (line 7270), remove last entry (line 7971). NOT called from browse_load.
  implication: browse_reading_desk_active stays True during navigation, but visual state is destroyed

## Resolution

root_cause: |
  TWO root causes that combine to produce both bugs:

  ROOT CAUSE 1 (PRIMARY): browse_load() (line 16021) does not guard against reading desk mode.
  When the user navigates to a new manuscript via the Go button while the reading desk is active,
  browse_load() runs its full normal flow:
    - Line 16031: Overwrites browse_text with "Loading metadata..."
    - Line 16032: Clears browse_viewer images
    - Line 16137: Changes current_browse_sid to the new manuscript
    - Line 16159: Calls browse_render_page() which replaces the reading desk HTML in browse_text
      with single-page content (line 16379)

  This destroys the reading desk visual rendering but does NOT reset browse_reading_desk_active
  (it stays True) or browse_reading_desk_state (entries are preserved).

  ROOT CAUSE 2 (SECONDARY): on_browse_enriched_loaded() (line 7071) also runs without a
  reading desk guard. When the enrichment thread completes:
    - Line 7142: Loads images into normal browse_viewer, overriding the reading desk image scroll
    - This clobbers the reading desk image layout

  NET EFFECT for Bug 1 + Bug 2:
  Step 1: User views A, clicks "Add to View" -> reading desk enters with A (works correctly)
  Step 2: User navigates to B via Go -> browse_load() runs:
    - browse_text overwritten with B's single-page HTML (reading desk HTML for A is gone from screen)
    - browse_viewer images overwritten with B's images
    - BUT browse_reading_desk_active remains True, and state still has entry A
  Step 3: User clicks "Add to View" again -> _browse_add_to_view sees desk IS active ->
    calls _browse_rd_add_entry(B) -> appends B to state -> _browse_rd_render() renders both A and B

  HOWEVER: The on_browse_enriched_loaded from step 2 may fire AFTER step 3's render,
  clobbering images again. And the toolbar/viewer visibility gets confused.

  Additionally, the problem may be MORE severe if browse_load() triggers intermediate
  state changes that cause the reading desk rendering to appear as only B. The key issue
  is that browse_load() should either:
  (a) Be blocked/intercepted during reading desk mode (don't allow normal navigation), or
  (b) Not overwrite the text/image panes when reading desk is active

fix: (not applied -- diagnosis only)
verification: (not verified -- diagnosis only)
files_changed: []
