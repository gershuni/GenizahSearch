---
phase: quick
plan: 002
type: execute
wave: 1
depends_on: []
files_modified:
  - web/main.py
autonomous: true

must_haves:
  truths:
    - "User sees animated progress bar at top of page during navigation"
    - "Progress bar appears when clicking any internal link"
    - "Progress bar hides when new page fully loads"
  artifacts:
    - path: "web/main.py"
      provides: "Global page loading progress bar CSS and JavaScript"
      contains: "page-loading-bar"
  key_links:
    - from: "JavaScript click handler"
      to: "CSS animation"
      via: "classList toggle on progress bar element"
      pattern: "page-loading-bar"
---

<objective>
Add a GitHub/YouTube-style thin animated progress bar at the top of the page that shows during page navigation.

Purpose: Provide immediate visual feedback when user clicks a link, especially when navigating to the browse page with a shelfmark lookup that takes time to load.

Output: A thin animated progress bar visible at the very top of the viewport during page transitions.
</objective>

<execution_context>
@C:\Users\gersh\.claude\get-shit-done\workflows\execute-plan.md
@C:\Users\gersh\.claude\get-shit-done\templates\summary.md
</execution_context>

<context>
@.planning/STATE.md
@web/main.py (COMMON_STYLES section around line 89-157, header section around line 1435-1445)
</context>

<tasks>

<task type="auto">
  <name>Task 1: Add global page loading progress bar</name>
  <files>web/main.py</files>
  <action>
Add the page loading progress bar CSS and JavaScript to COMMON_STYLES in web/main.py.

**CSS (add to COMMON_STYLES after the existing styles, around line 157 before the closing style tag or as a new section):**

```css
/* Page Loading Progress Bar - GitHub/YouTube style */
.page-loading-bar {
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 3px;
    z-index: 9999;
    pointer-events: none;
    background: linear-gradient(90deg,
        var(--primary-400) 0%,
        var(--primary-500) 50%,
        var(--primary-400) 100%);
    background-size: 200% 100%;
    transform: translateX(-100%);
    opacity: 0;
    transition: opacity 0.2s ease;
}

.page-loading-bar.active {
    opacity: 1;
    animation: loading-progress 1.5s ease-in-out infinite,
               loading-shimmer 1s linear infinite;
}

@keyframes loading-progress {
    0% { transform: translateX(-100%); }
    50% { transform: translateX(-30%); }
    100% { transform: translateX(-10%); }
}

@keyframes loading-shimmer {
    0% { background-position: 200% 0; }
    100% { background-position: -200% 0; }
}

.page-loading-bar.complete {
    animation: loading-complete 0.3s ease-out forwards;
}

@keyframes loading-complete {
    0% { transform: translateX(-10%); opacity: 1; }
    100% { transform: translateX(0%); opacity: 0; }
}
```

**JavaScript (add to COMMON_STYLES after the CSS, before the closing </style> tag - actually add it as a separate script section):**

Add after COMMON_STYLES definition (after the closing `'''` for COMMON_STYLES), create a new constant:

```python
PAGE_LOADING_SCRIPT = '''
<div class="page-loading-bar" id="pageLoadingBar"></div>
<script>
(function() {
    const bar = document.getElementById('pageLoadingBar');
    if (!bar) return;

    // Show loading bar when clicking internal links
    document.addEventListener('click', function(e) {
        const link = e.target.closest('a[href]');
        if (!link) return;

        const href = link.getAttribute('href');
        // Only trigger for internal navigation (not external links or anchors)
        if (href && href.startsWith('/') && !href.startsWith('//') && !link.target) {
            bar.classList.remove('complete');
            bar.classList.add('active');
        }
    });

    // Also trigger on NiceGUI navigation calls via custom event
    window.addEventListener('nicegui-navigate', function() {
        bar.classList.remove('complete');
        bar.classList.add('active');
    });

    // Hide loading bar when page fully loads
    window.addEventListener('load', function() {
        bar.classList.remove('active');
        bar.classList.add('complete');
    });

    // Also hide on DOMContentLoaded as fallback
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function() {
            setTimeout(function() {
                bar.classList.remove('active');
                bar.classList.add('complete');
            }, 100);
        });
    }
})();
</script>
'''
```

Then in each page route (or better, in `create_layout()` function), add:
```python
ui.add_head_html(PAGE_LOADING_SCRIPT)
```

Find `create_layout()` function (around line 1335) and add the PAGE_LOADING_SCRIPT after the other head_html additions in the function, or add it once at the top level where COMMON_STYLES is added.

**Simpler approach:** Just append the CSS directly to COMMON_STYLES and add the HTML/JS script in create_layout.

Look for where `ui.add_head_html(COMMON_STYLES)` is called in each page route and ensure PAGE_LOADING_SCRIPT is also added. Since create_layout is not where head_html is added, and each route adds it individually, the simplest fix is to concatenate PAGE_LOADING_SCRIPT content into each page route.

**Best approach:** Since COMMON_STYLES is already added to every page route, add the CSS to COMMON_STYLES, then add a single line to create_layout() at the start (before the header) to inject the progress bar div:

In create_layout() function, at the very beginning after `rtl_mode = is_rtl()`, add:
```python
# Page loading progress bar element (CSS in COMMON_STYLES)
ui.html('<div class="page-loading-bar" id="pageLoadingBar"></div>')
```

And add the JavaScript as a separate head_html call in the same function:
```python
ui.add_head_html('''<script>
(function() {
    document.addEventListener('click', function(e) {
        const link = e.target.closest('a[href]');
        if (!link) return;
        const href = link.getAttribute('href');
        if (href && href.startsWith('/') && !href.startsWith('//') && !link.target) {
            const bar = document.getElementById('pageLoadingBar');
            if (bar) {
                bar.classList.remove('complete');
                bar.classList.add('active');
            }
        }
    });
    window.addEventListener('load', function() {
        const bar = document.getElementById('pageLoadingBar');
        if (bar) {
            bar.classList.remove('active');
            bar.classList.add('complete');
        }
    });
})();
</script>''')
```
  </action>
  <verify>
1. Run the web app: `python -m web.main`
2. Navigate to any page (home, search, browse)
3. Click an internal link - should see thin green progress bar animate at top
4. When new page loads, bar should complete animation and fade out
5. External links (new tab) should NOT trigger the bar
  </verify>
  <done>
- Thin animated progress bar appears at top of page when navigating
- Bar animates with shimmer effect during load
- Bar completes and fades when page loads
- Works for all internal navigation links
  </done>
</task>

</tasks>

<verification>
1. Visual: Progress bar visible at very top during navigation
2. Animation: Smooth shimmer/progress animation while loading
3. Completion: Bar fades out when page finishes loading
4. Coverage: Works on home, search, browse, lists, and all other pages
5. Edge cases: External links (target="_blank") do not trigger bar
</verification>

<success_criteria>
- Green animated progress bar visible at top during page transitions
- Bar provides immediate visual feedback (appears within 50ms of click)
- Bar completes smoothly when destination page loads
- No visual artifacts or flashing
- Works consistently across all pages
</success_criteria>

<output>
After completion, create `.planning/quick/002-browse-loading-progress-bar/002-SUMMARY.md`
</output>
