---
phase: quick
plan: 003
type: execute
wave: 1
depends_on: []
files_modified:
  - web/main.py
  - web/pages/parallels.py
autonomous: true

must_haves:
  truths:
    - "User sees progress bar immediately when clicking any sidebar navigation item (Parallels, Browse, Lists, etc.)"
    - "User sees progress bar immediately when clicking ui.navigate.to() triggered buttons (e.g., Browse from parallels results)"
    - "User sees the top page loading bar animate during a parallels search operation"
    - "Progress bar hides when new page finishes loading or parallels search completes"
  artifacts:
    - path: "web/main.py"
      provides: "Fixed progress bar JavaScript that intercepts all navigation methods"
      contains: "pageLoadingBar"
    - path: "web/pages/parallels.py"
      provides: "Top loading bar integration during parallels search"
      contains: "pageLoadingBar"
  key_links:
    - from: "web/main.py JavaScript"
      to: "progress bar CSS"
      via: "beforeunload event + classList toggle"
      pattern: "beforeunload.*pageLoadingBar"
    - from: "web/pages/parallels.py execute_parallels"
      to: "pageLoadingBar JavaScript"
      via: "ui.run_javascript"
      pattern: "run_javascript.*pageLoadingBar"
---

<objective>
Fix the page loading progress bar to show immediately for ALL navigation methods (not just `<a>` link clicks), and add it to the parallels search operation.

Purpose: Currently the progress bar only triggers on `<a href>` clicks and Enter key presses. But sidebar navigation and many buttons use NiceGUI's `ui.navigate.to()` which changes `window.location` programmatically -- this is NOT an `<a>` click so the progress bar never shows. Users wait several minutes with no visual feedback when navigating to Parallels, Browse, or Lists pages. Additionally, the parallels search can take minutes but the top page bar is not used.

Output: Progress bar works for all navigation methods and shows during parallels search.
</objective>

<execution_context>
@C:\Users\gersh\.claude\get-shit-done\workflows\execute-plan.md
@C:\Users\gersh\.claude\get-shit-done\templates\summary.md
</execution_context>

<context>
@web/main.py (lines 1330-1435: progress bar CSS and JavaScript in create_layout)
@web/pages/parallels.py (lines 1099-1262: execute_parallels function)
</context>

<tasks>

<task type="auto">
  <name>Task 1: Fix progress bar to trigger on all navigation methods</name>
  <files>web/main.py</files>
  <action>
In `create_layout()` (around line 1392-1435), replace the existing progress bar JavaScript with an improved version that catches ALL navigation triggers.

The current JS only listens for `<a href>` click events and Enter keydown. This misses `ui.navigate.to()` calls which set `window.location.href` programmatically (used by sidebar nav items, button click handlers throughout the app).

**Replace the existing `ui.add_head_html('''<script>...</script>''')` block (lines ~1392-1435) with:**

```python
ui.add_head_html('''<script>
(function() {
    function showLoadingBar() {
        var bar = document.getElementById('pageLoadingBar');
        if (bar) {
            bar.classList.remove('complete');
            bar.classList.add('active');
        }
    }
    function hideLoadingBar() {
        var bar = document.getElementById('pageLoadingBar');
        if (bar) {
            bar.classList.remove('active');
            bar.classList.add('complete');
        }
    }

    // Expose globally so Python can call via ui.run_javascript
    window.__showLoadingBar = showLoadingBar;
    window.__hideLoadingBar = hideLoadingBar;

    // 1. Trigger on <a href> clicks (original behavior)
    document.addEventListener('click', function(e) {
        var link = e.target.closest('a[href]');
        if (!link) return;
        var href = link.getAttribute('href');
        if (href && href.startsWith('/') && !href.startsWith('//') && !link.target) {
            showLoadingBar();
        }
    });

    // 2. Trigger on Enter key in text inputs (search/shelfmark navigation)
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Enter' && e.target.tagName === 'INPUT') {
            var skipTypes = ['submit', 'button', 'checkbox', 'radio', 'file'];
            if (skipTypes.indexOf(e.target.type) === -1) {
                showLoadingBar();
            }
        }
    });

    // 3. KEY FIX: Trigger on beforeunload - catches ALL navigation methods
    // This fires when ui.navigate.to() sets window.location, when <a> links navigate,
    // when the user uses back/forward, etc. It's the universal navigation event.
    window.addEventListener('beforeunload', function() {
        showLoadingBar();
    });

    // 4. Hide on page load (new page finished rendering)
    window.addEventListener('load', hideLoadingBar);

    // 5. Fallback: hide after 15 seconds if page didn't navigate
    // (for Enter key searches that update in-page instead of navigating)
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            setTimeout(hideLoadingBar, 15000);
        }
    });
})();
</script>''')
```

Key changes from the old version:
- Added `window.addEventListener('beforeunload', showLoadingBar)` which fires for ALL navigation, including `ui.navigate.to()` which sets `window.location.href`
- Exposed `window.__showLoadingBar` and `window.__hideLoadingBar` globally so Python code can control the bar via `ui.run_javascript('window.__showLoadingBar()')` for long operations like parallels search
- Increased fallback timeout from 10s to 15s
- Kept all existing triggers (click on `<a>`, Enter key) for early activation before beforeunload fires

Do NOT change the progress bar CSS (lines 1330-1377) or the HTML div injection (line 1391). Only replace the `<script>` block.
  </action>
  <verify>
1. Run `python -m web.main` from the project root
2. Click sidebar "Find Parallels" navigation item -- progress bar should show immediately at top
3. Click sidebar "Browse" navigation item -- progress bar should show immediately
4. Click sidebar "My Lists" navigation item -- progress bar should show immediately
5. Use the quick search in header (Enter key) -- progress bar should show
6. Click any `<a href>` link -- progress bar should still show (original behavior preserved)
7. Progress bar should hide when the destination page finishes loading
  </verify>
  <done>
- Progress bar shows immediately for sidebar navigation clicks (ui.navigate.to)
- Progress bar shows for all `<a href>` link clicks (preserved original behavior)
- Progress bar shows for Enter key searches (preserved original behavior)
- Progress bar hides when destination page loads
  </done>
</task>

<task type="auto">
  <name>Task 2: Show page loading bar during parallels search</name>
  <files>web/pages/parallels.py</files>
  <action>
In the `execute_parallels()` async function (around line 1099), add calls to show and hide the top page loading bar at the start and end of the search.

**At the START of the search** (after the existing immediate feedback section, around line 1144 after `progress_bar.set_value(0)`), add:

```python
# Show top page loading bar during search
ui.run_javascript('if (window.__showLoadingBar) window.__showLoadingBar();')
```

**At the END of the search** (after `p_state.is_running = False` on line 1221, before the result processing), add:

```python
# Hide top page loading bar
ui.run_javascript('if (window.__hideLoadingBar) window.__hideLoadingBar();')
```

This makes the thin green bar at the very top of the page animate during the entire parallels search, in addition to the existing in-page progress bar and spinner. The top bar provides an immediately recognizable "something is happening" signal, especially valuable since the parallels search can take several minutes.

Also add the same hide call in the `cancel_search()` function (around line 1095-1097), after setting `p_state.is_cancelled = True`:

```python
def cancel_search():
    p_state.is_cancelled = True
    p_state.status = tr('Cancelling...')
    # Hide top page loading bar on cancel
    ui.run_javascript('if (window.__hideLoadingBar) window.__hideLoadingBar();')
```

Note: The `if (window.__showLoadingBar)` guard ensures no error if the function isn't defined for some reason.
  </action>
  <verify>
1. Navigate to the Parallels page
2. Enter some Hebrew text (at least 3 words)
3. Click "Find Parallels"
4. Verify the thin green progress bar appears at the top of the page AND the in-page spinner/progress bar also shows
5. Wait for search to complete -- top bar should hide/complete
6. Start another search and click "Stop" -- top bar should hide on cancel
  </verify>
  <done>
- Top page loading bar animates during parallels search operation
- Top bar hides when search completes
- Top bar hides when search is cancelled
- Existing in-page progress bar and spinner still function normally
  </done>
</task>

</tasks>

<verification>
1. Navigation: Click each sidebar item (Home, Search, Parallels, Browse, Community, Lists) -- progress bar shows immediately for each
2. Navigation: Click a `ui.navigate.to()` button (e.g., Browse button on a parallels result card) -- progress bar shows
3. Navigation: Click an `<a href>` link -- progress bar shows (original behavior)
4. Navigation: Use header quick search (Enter key) -- progress bar shows
5. Parallels: Start a search -- top bar shows alongside in-page progress
6. Parallels: Cancel a search -- top bar hides
7. Parallels: Complete a search -- top bar hides
8. All pages: Progress bar hides when destination page loads, no stuck bars
</verification>

<success_criteria>
- Progress bar triggers for ALL navigation methods (sidebar clicks, ui.navigate.to buttons, <a> links, Enter key)
- Progress bar shows during parallels search operations
- Progress bar hides properly on page load and search completion/cancellation
- No regressions: existing progress bar behavior for <a> links and Enter key preserved
- No JavaScript errors in browser console
</success_criteria>

<output>
After completion, create `.planning/quick/003-fix-progress-bar-navigation-parallels/003-SUMMARY.md`
</output>
