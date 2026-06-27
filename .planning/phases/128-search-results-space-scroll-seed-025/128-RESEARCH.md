# Phase 128: Search Results Space-Scroll (SEED-025) - Research

**Researched:** 2026-06-27
**Domain:** Browser keyboard event handling (NiceGUI/Quasar), PyQt6 table key routing
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Space scrolls the results UNLESS one of these holds keyboard focus / is active: a result's **checkbox**, an **expand/collapse** toggle, an **open-detail** control (link/button that opens the result), or an **open detail dialog/accordion**. Everything else falls through to scroll.
- **D-02:** Space = one viewport page down; Shift+Space = one viewport page up. Native PageDown/PageUp must keep working.
- **D-03 (web):** On web, Space scrolls only the results pane — the existing `.results-scroll-area` container (`web/pages/search.py` near line 1763) — not the document body. Integrate with the existing global `ui.keyboard(on_key=...)` handler (near line 1959, `ignore=['input','textarea']`); add a Space branch with a focus guard + `preventDefault` + container `scrollBy(±viewport)`. Do NOT `preventDefault` when a control legitimately wants Space (a11y intact).
- **D-04 (desktop):** In the desktop results `QTableWidget` (`genizah_app.py` near line 4828, checkbox column `COL_CHECKBOX` near line 4851), Space toggles the checkbox **only when that checkbox cell has focus**; otherwise Space routes to the table's **page-down** (Shift+Space page-up). Preserves today's checkbox-toggle behavior for the focused-cell case.

### Claude's Discretion
- Exact mechanism for detecting "actionable focus" on each platform (web: `document.activeElement` class/role test inside the keydown `js_handler`; desktop: `focusWidget()` / current cell + checkbox-column test) is the planner's/implementer's call, as long as D-01 membership holds and a11y is preserved.
- Smooth vs instant scroll animation — implementer's choice (lean native/instant for predictability).

### Deferred Ideas (OUT OF SCOPE)
- Space-scroll on other scrollable surfaces (browse, reading desk, catalog, Joins Lab) — out of this phase's scope.
- Library filter (SEED-026) — Phase 129.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| SCROLL-01 | Web: Space page-scrolls the results container; Shift+Space scrolls up; no `preventDefault` when actionable control has focus; suppression set enumerated + tested | See §§ Web Implementation Mechanism, Actionable Focus Suppression Set |
| SCROLL-02 | Desktop: Space routes to page-down/page-up when no checkable/actionable focus state; otherwise Space toggles/activates as today | See §§ Desktop Implementation Mechanism, Desktop Checkbox Behavior |
| GUARD-02 | Zero regression to existing behavior (existing keyboard shortcuts, checkbox toggle, PageUp/PageDown) | See §§ Regression Surface, Existing Keyboard Shortcuts |
</phase_requirements>

---

## Summary

This phase adds a Space-to-page-scroll affordance on both apps. The implementation is small but requires understanding the precise event model on each platform.

**Web (NiceGUI/Quasar):** The existing `ui.keyboard` handler fires a server round-trip and CANNOT call `preventDefault` — the event is already processed in the browser by the time the Python callback executes. The correct mechanism is a pure client-side `document.addEventListener('keydown', ...)` block injected via `ui.run_javascript`, following the exact same deferred-setup pattern as the existing `setup_scroll_collapse` async function. The focus guard uses `document.activeElement` properties and the presence of a `q-dialog` overlay. The scroll target is the `q-scrollarea__container` element nested inside `.results-scroll-area`, consistent with how the existing scroll-collapse JS finds it.

**Desktop (PyQt6):** The results table checkbox is a `QTableWidgetItem` with `Qt.ItemFlag.ItemIsUserCheckable` (NOT a cell widget), so Qt's default Space handling toggles the check state on the current item. D-04 is implemented cleanest via a `QTableWidget` subclass (or `keyPressEvent` override directly on the main app's `eventFilter`) that intercepts `Key_Space` before the default handler. When `currentColumn() != COL_CHECKBOX`, consume the event and trigger `verticalScrollBar().triggerAction(SliderPageStepAdd/Sub)`; otherwise pass through to `super()`.

**Testing:** Both platforms use static AST source guards (already proven in this codebase) for the "don't hand-roll" path. The web keyboard path cannot be exercised in the NiceGUI `User` headless driver without a real browser; test via source-structure assertions + pure-logic unit tests. The desktop can be tested via a `QApplication` widget construction smoke test in the `gui` marker group.

**Primary recommendation:** Inject a single self-contained `(function(){...})()` block via `ui.run_javascript` in a new `async def setup_space_scroll()` function, deferred with `asyncio.ensure_future(_after_delay(1.0, setup_space_scroll))`, placed adjacent to the existing `setup_scroll_collapse` call. On the desktop, subclass `QTableWidget` or add a `Key_Space` branch to the existing `GenizahGUI.eventFilter`.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Space-scroll, web | Browser (client-side JS) | — | `preventDefault` must fire synchronously before browser default; no server round-trip possible |
| Focus guard, web | Browser (client-side JS) | — | `document.activeElement` is only available client-side at event time |
| Space-scroll, desktop | Desktop app (Qt event layer) | — | QTableWidget key routing is entirely in-process Qt |
| Checkbox toggle preservation | Desktop app (Qt event layer via super()) | — | Default `QAbstractItemView` behavior handles it when not intercepted |

---

## Standard Stack

This phase adds NO new libraries. All capabilities are built from existing platform primitives.

### Core (existing, verified in codebase)
| Primitive | Where | Purpose in Phase |
|-----------|-------|-----------------|
| `ui.run_javascript(js_code)` | `web/pages/search.py` (already used at multiple sites) | Inject the keydown handler JS [VERIFIED: grep search.py] |
| `asyncio.ensure_future(_after_delay(...))` | `web/pages/search.py:2064` | Defer JS setup until DOM is ready (Cat-2 pattern already used for `setup_scroll_collapse`) [VERIFIED: grep search.py] |
| `document.addEventListener('keydown', ...)` | `web/pages/browse.py:4706` | Client-side keyboard handler, same pattern as browse page arrow-key nav [VERIFIED: grep browse.py] |
| `q-scrollarea__container` | `web/pages/search.py:1996` | Actual scrollable element inside Quasar scroll-area; `.scrollBy(0, height)` target [VERIFIED: existing JS in setup_scroll_collapse] |
| `QTableWidget.verticalScrollBar().triggerAction(...)` | — | Qt scroll-bar action trigger; standard PyQt6 pattern [ASSUMED] |
| `QAbstractSlider.SliderAction.SliderPageStepAdd/Sub` | — | Page-step scroll actions [ASSUMED] |
| `GenizahGUI.eventFilter` | `genizah_app.py:17891` | Existing event filter on the main window; already intercepts keys for results_table [VERIFIED: grep genizah_app.py] |

### Package Legitimacy Audit

No new packages are installed in this phase.

---

## Architecture Patterns

### System Architecture Diagram

```
[User presses Space]
        |
        +--[Web: browser keydown event]--+
        |                                |
   [activeElement is actionable?]        |
        |                                |
       YES -> let event through (a11y)   |
        |                                |
        NO -> preventDefault()           |
             scrollBy(±clientHeight)     |
             on q-scrollarea__container  |
                                         |
        +--[Desktop: Qt Key_Space]-------+
                |
        [currentColumn() == COL_CHECKBOX?]
                |
               YES -> super().keyPressEvent() -> Qt toggles checkbox
                |
                NO -> consume event
                      verticalScrollBar().triggerAction(SliderPageStepAdd/Sub)
```

### Recommended File Changes
```
web/pages/search.py
  └── new async def setup_space_scroll()     # adjacent to setup_scroll_collapse (near line 2064)
      └── asyncio.ensure_future(...)         # deferred 1.0s setup

genizah_app.py
  └── eventFilter (near line 17891)          # new Key_Space branch for results_table
      └── OR: subclass QTableWidget          # if eventFilter branch grows too long
```

---

## Web Implementation Mechanism (Critical Research Findings)

### Finding W-1: `ui.keyboard` CANNOT `preventDefault` — Do Not Use It for Space-Scroll

The NiceGUI `Keyboard` component (`nicegui/elements/keyboard.js`) does:
```javascript
document.addEventListener(event, (evt) => {
  // ... ignore check ...
  this.$emit("key", {...});  // round-trips to Python server
});
```
[VERIFIED: read C:/Users/gersh/AppData/Local/Programs/Python/Python311/Lib/site-packages/nicegui/elements/keyboard.js]

By the time the Python `on_key` callback fires, the browser has already processed the keydown event. There is no way to call `evt.preventDefault()` after a server round-trip. The existing `/` shortcut does NOT need `preventDefault` (text input focus does not require it). Space-scroll DOES require `preventDefault` to suppress browser default scrolling behavior.

**Conclusion:** Space-scroll MUST use a pure client-side `document.addEventListener('keydown', ...)` handler injected via `ui.run_javascript`. Do NOT add a Space branch to `handle_keyboard_shortcut`.

### Finding W-2: The Expand-Toggle Column ALREADY Calls `preventDefault` on Space

The result card expand-toggle (`_content_col`) is bound with Vue's `.prevent` modifier:
```python
_content_col.on('keydown.space.self.prevent', lambda idx=index, ...: toggle_expansion(...))
```
[VERIFIED: `web/pages/search_results.py` near line 454]

When focus is on the expand-toggle element, the Vue handler fires synchronously client-side, calls `evt.preventDefault()`, and stops the scroll. This means the expand-toggle case is ALREADY handled — the global Space handler only needs to add scrolling for the "nothing actionable focused" case.

### Finding W-3: The Scroll Target is `q-scrollarea__container`, Not `.results-scroll-area`

Quasar's `ui.scroll_area()` renders the outer element with class `.results-scroll-area`, but the INNER container that actually scrolls has class `q-scrollarea__container`. The existing JS in `setup_scroll_collapse` already shows the correct pattern:
```javascript
const scrollAreaEl = document.querySelector('.results-scroll-area');
const inner = scrollAreaEl.querySelector('.q-scrollarea__container');
// inner.scrollTop / inner.scrollBy() — this is the actual scroll target
```
[VERIFIED: `web/pages/search.py` near lines 1993-1999]

`scrollBy(0, inner.clientHeight)` is one viewport down; `scrollBy(0, -inner.clientHeight)` is one viewport up.

### Finding W-4: The Actionable-Focus Suppression Set (Web, D-01)

Per the rendered HTML structure in `create_result_card` (`web/pages/search_results.py`):

| Control | HTML element | Detection |
|---------|-------------|-----------|
| Checkbox (`ui.checkbox`) | `<input type="checkbox">` — tagName `INPUT` | `activeElement.tagName === 'INPUT'` |
| Expand-toggle (`_content_col`) | `<div role="button" tabindex="0">` | `activeElement.getAttribute('role') === 'button'` |
| Action buttons (Browse, QuickView, star, catalog, joins, VS toggle) | `<button>` | `activeElement.tagName === 'BUTTON'` |
| Open detail dialog | Quasar `q-dialog` overlay | `document.querySelector('.q-dialog') !== null` |

The existing `keyboard.js` `ignore` array for `ui.keyboard` includes `'input'` (so `input[type=checkbox]` is already ignored for the server-side on_key handler). In the new client-side handler, the same tagName checks apply — but we add `role=button` and the dialog check.

**Complete guard expression (JavaScript):**
```javascript
const ae = document.activeElement;
const isActionable = (
    ae.tagName === 'INPUT' ||
    ae.tagName === 'BUTTON' ||
    ae.tagName === 'TEXTAREA' ||
    ae.tagName === 'SELECT' ||
    ae.getAttribute('role') === 'button' ||
    ae.isContentEditable ||
    document.querySelector('.q-dialog') !== null
);
```

Note: `ae.tagName === 'INPUT'` covers both checkboxes and text inputs (though text inputs are usually tagged INPUT and NiceGUI's keyboard `ignore` already suppresses their events — the client-side handler is an independent listener and must guard independently).

### Finding W-5: The Deferred Setup Pattern (Cat-2)

The identical `_after_delay(1.0, setup_fn)` + `asyncio.ensure_future(...)` pattern is already used for `setup_scroll_collapse` at line 2064. The Space-scroll setup should be a parallel async function called the same way, sharing the `_after_delay` helper (defined at line 111 in `search.py`).

The setup function only needs to inject the listener once per page load. Unlike `setup_scroll_collapse`, it does NOT need to wait for a specific DOM element to appear — the listener can be installed immediately (but `_after_delay(1.0, ...)` is fine for consistency and safety).

### Finding W-6: Composition with Existing `keydown.space.self.prevent` on Expand-Toggle

The expand-toggle's Vue modifier `.self.prevent` means:
- `.self` — only fires when the event TARGET is exactly the `_content_col` element (not a nested child)
- `.prevent` — calls `event.preventDefault()` before calling the Python handler

When the expand-toggle HAS focus and the user presses Space, the Vue handler fires first (on the element), calls `preventDefault()`, and the event does NOT bubble to the global `document.addEventListener`. No conflict.

When the expand-toggle does NOT have focus (e.g., no focus at all), the global handler fires, checks `activeElement`, finds it is not actionable, and scrolls.

### Finding W-7: `select_all_checkbox` Is NOT in a Result Card

The "Select All" checkbox at the top (near line 1452 of `search.py`) is a `ui.checkbox` in the header — it renders as `<input type="checkbox">`. The global guard already covers it (`ae.tagName === 'INPUT'`).

---

## Desktop Implementation Mechanism (Critical Research Findings)

### Finding D-1: Checkbox Column Is `ItemIsUserCheckable` — Qt Default Toggles It on Space

The results table checkbox is:
```python
item_chk = QTableWidgetItem()
item_chk.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
item_chk.setCheckState(Qt.CheckState.Unchecked)
self.results_table.setItem(row_idx, self.COL_CHECKBOX, item_chk)
```
[VERIFIED: `genizah_app.py` near line 16111]

This is NOT a `cellWidget` — it is a `QTableWidgetItem`. Qt's `QAbstractItemView` default key handling toggles the check state of the current item when Space is pressed. When `currentColumn() == COL_CHECKBOX`, the toggled item IS the checkbox. When `currentColumn() != COL_CHECKBOX`, the toggled item is a non-checkable item (typically no-op, but still consumes the Space).

### Finding D-2: The Existing `eventFilter` Is the Right Hook

`GenizahGUI.eventFilter` (near line 17891) already handles:
- Key_Down on `query_input` → history menu
- Key navigation inside history menus
- ToolTip smart-show for results_table viewport
- Leave events for hover-action widgets

The filter already has `source == self.results_table` checks for the ToolTip case. Adding a `Key_Space` branch here is the least-invasive approach — no new subclass needed.

The new branch:
```python
if (hasattr(self, 'results_table') and source is self.results_table
        and event.type() == QEvent.Type.KeyPress
        and event.key() == Qt.Key.Key_Space):
    col = self.results_table.currentColumn()
    if col != self.COL_CHECKBOX:
        # Not on checkbox — route to page-scroll
        mod = event.modifiers()
        bar = self.results_table.verticalScrollBar()
        if mod & Qt.KeyboardModifier.ShiftModifier:
            bar.triggerAction(QAbstractSlider.SliderAction.SliderPageStepSub)
        else:
            bar.triggerAction(QAbstractSlider.SliderAction.SliderPageStepAdd)
        return True  # consume — do NOT let Qt toggle a non-checkbox item
    # col == COL_CHECKBOX: fall through to super() for checkbox toggle
```

### Finding D-3: `installEventFilter(self)` Already Wired on results_table

At line 4879 (grep, confirm):
```python
self.results_table.installEventFilter(self)
self.results_table.viewport().installEventFilter(self)
```
[VERIFIED: grep genizah_app.py line 4879]

The filter is already installed. Only a new `Key_Space` branch inside `eventFilter` is needed.

### Finding D-4: `QAbstractSlider.SliderAction` Import Check

`QAbstractSlider` is imported in PyQt6 from `PyQt6.QtWidgets`. Confirm the imports already available:
```python
from PyQt6.QtWidgets import (..., QAbstractSlider, ...)
```
Check the existing imports at the top of `genizah_app.py` — if `QAbstractSlider` is not imported, add it to the existing `from PyQt6.QtWidgets import (...)` block.

[ASSUMED: `QAbstractSlider` may or may not be in the existing import list — grep before writing the plan task]

### Finding D-5: COL_CHECKBOX = 0 Is Stable

`COL_CHECKBOX = 0` is defined at `genizah_app.py:4814` and never changes. The eventFilter branch can reference `self.COL_CHECKBOX` directly.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Client-side keydown + preventDefault | Custom NiceGUI element or server-side logic | `ui.run_javascript` injected IIFE (same as scroll-collapse) | Only way to `preventDefault` synchronously; proven pattern already in codebase |
| Quasar scroll area programmatic scroll | Parsing DOM manually | `.q-scrollarea__container` `.scrollBy()` | Already used by existing scroll-collapse JS; Quasar's scroll-area wraps a real scrollable div |
| Desktop page-scroll calculation | Computing pixel offsets | `QAbstractSlider.SliderPageStepAdd/Sub` via `triggerAction()` | Platform-native page-step is what PageDown uses; single call, no pixel arithmetic |

---

## Common Pitfalls

### Pitfall 1: Adding Space Branch to `handle_keyboard_shortcut` Instead of Client-Side JS
**What goes wrong:** The Python `on_key` handler fires AFTER the browser has already processed the event. Calling `ui.run_javascript("...")` inside the handler is asynchronous and runs too late to suppress the browser's default Space behavior. The results pane does not scroll, or scrolls the wrong element.
**Why it happens:** The CONTEXT.md says "integrate with the existing global `ui.keyboard(on_key=...)` handler" — this is misleading if interpreted as "add a Space case to `handle_keyboard_shortcut`." It means the handler wires the Space branch; the ACTUAL action (preventDefault + scroll) must be client-side.
**How to avoid:** The Space branch must be a pure client-side listener installed by `ui.run_javascript`, NOT a Python server-side handler call.
**Warning signs:** Results don't scroll on Space, or the page body scrolls instead of `.results-scroll-area`.

### Pitfall 2: Scrolling `.results-scroll-area` Instead of `.q-scrollarea__container`
**What goes wrong:** `.results-scroll-area` is the Quasar `q-scroll-area` outer wrapper; its `overflow` may not be `auto`/`scroll`. `.scrollBy()` on it has no effect. The actual scrollable div is `.q-scrollarea__container`.
**Why it happens:** The obvious target class name is `.results-scroll-area`, but Quasar wraps the actual scrollable content in an inner div.
**How to avoid:** Use the two-step find pattern from the existing JS: `querySelector('.results-scroll-area').querySelector('.q-scrollarea__container')`. The `setup_scroll_collapse` code already proves this works.
**Warning signs:** Space press shows no scroll effect.

### Pitfall 3: Forgetting the `.q-dialog` Guard on Web
**What goes wrong:** User opens the Quick View dialog (full-screen), presses Space, and the results pane scrolls behind the dialog. Or Space triggers unexpected behavior inside the dialog.
**Why it happens:** `document.activeElement` may point to the dialog's close button or a non-actionable element inside it; the guard passes and the results scroll.
**How to avoid:** Check `document.querySelector('.q-dialog') !== null` as part of the suppression set.
**Warning signs:** Space scrolls results through the Quick View dialog overlay.

### Pitfall 4: Desktop — Not Consuming the Event When Routing to Page-Scroll
**What goes wrong:** `eventFilter` triggers the scroll but also passes the event to `super().eventFilter()`, which then lets Qt toggle the current item's checkbox (or try to). Spurious checkbox state changes appear.
**Why it happens:** Returning `False` (or not returning `True`) from `eventFilter` passes the event downstream to Qt's default handling.
**How to avoid:** Return `True` from the eventFilter branch when routing Space to the scroll bar, so Qt does NOT see the Key_Space event further.

### Pitfall 5: Desktop — Intercepting Space on Wrong Source
**What goes wrong:** Typing Space in the search input bar (`query_input`) is suppressed or routed to scroll.
**Why it happens:** The eventFilter is installed on `self.results_table` specifically, but if the source check is wrong, inputs caught by other paths could be affected.
**How to avoid:** The branch must be gated on `source is self.results_table` (object identity), consistent with how the ToolTip branch does it.

### Pitfall 6: Web — Injecting the Listener Multiple Times
**What goes wrong:** If `setup_space_scroll` is called more than once (e.g., on a search or render refresh), multiple `document.addEventListener('keydown', ...)` calls accumulate, and each Space press scrolls multiple times.
**Why it happens:** `asyncio.ensure_future(_after_delay(1.0, setup_space_scroll))` at page-load time should only run once per page client. However, if the function is accidentally called elsewhere (e.g., in `render_results`), it accumulates.
**How to avoid:** Call `setup_space_scroll` exactly once, in the same place `setup_scroll_collapse` is called (at page setup time, not on result render). Alternatively, use a JS flag: `if (window._gsSpaceScrollInstalled) return; window._gsSpaceScrollInstalled = true;` at the top of the IIFE.

---

## Code Examples

### Web: The Complete `setup_space_scroll` Function

Based on the existing `setup_scroll_collapse` pattern [VERIFIED: `web/pages/search.py` near lines 1980-2064]:

```javascript
// Source: mirrors pattern from setup_scroll_collapse in web/pages/search.py
(function() {
    // Guard: only install once per page load
    if (window._gsSpaceScrollInstalled) return;
    window._gsSpaceScrollInstalled = true;

    document.addEventListener('keydown', function(e) {
        if (e.key !== ' ') return;

        // Suppression set (D-01):
        // 1. Any open dialog
        if (document.querySelector('.q-dialog')) return;

        // 2. Actionable active element
        const ae = document.activeElement;
        if (!ae) return;
        const tag = ae.tagName;
        if (tag === 'INPUT' || tag === 'BUTTON' || tag === 'TEXTAREA' || tag === 'SELECT') return;
        if (ae.getAttribute('role') === 'button') return;
        if (ae.isContentEditable) return;

        // Scroll the results pane (D-03)
        const outer = document.querySelector('.results-scroll-area');
        if (!outer) return;
        const inner = outer.querySelector('.q-scrollarea__container') || outer;
        const delta = e.shiftKey ? -inner.clientHeight : inner.clientHeight;
        inner.scrollBy({ top: delta, behavior: 'instant' });
        e.preventDefault();
    });
})();
```

**Python wrapper (same pattern as `setup_scroll_collapse`):**
```python
async def setup_space_scroll():
    """Install client-side Space key scroll handler for the results pane (D-03)."""
    js_code = '''
    (function() {
        if (window._gsSpaceScrollInstalled) return;
        window._gsSpaceScrollInstalled = true;
        document.addEventListener('keydown', function(e) {
            if (e.key !== ' ') return;
            if (document.querySelector('.q-dialog')) return;
            const ae = document.activeElement;
            if (!ae) return;
            const tag = ae.tagName;
            if (tag === 'INPUT' || tag === 'BUTTON' || tag === 'TEXTAREA' || tag === 'SELECT') return;
            if (ae.getAttribute('role') === 'button') return;
            if (ae.isContentEditable) return;
            const outer = document.querySelector('.results-scroll-area');
            if (!outer) return;
            const inner = outer.querySelector('.q-scrollarea__container') || outer;
            const delta = e.shiftKey ? -inner.clientHeight : inner.clientHeight;
            inner.scrollBy({{ top: delta, behavior: 'instant' }});
            e.preventDefault();
        });
    })();
    '''
    try:
        await ui.run_javascript(js_code, timeout=5.0)
    except TimeoutError:
        pass  # JS still executes

# In the page setup code, after the existing setup_scroll_collapse call:
asyncio.ensure_future(_after_delay(1.0, setup_space_scroll))
```

### Desktop: `eventFilter` Space Branch

Based on the existing `eventFilter` structure [VERIFIED: `genizah_app.py` near line 17891]:

```python
# Source: extends existing GenizahGUI.eventFilter in genizah_app.py
# Add this BEFORE the ToolTip/Leave branches (early return is cleaner)
if (hasattr(self, 'results_table')
        and source is self.results_table
        and event.type() == QEvent.Type.KeyPress
        and event.key() == Qt.Key.Key_Space):
    col = self.results_table.currentColumn()
    if col != self.COL_CHECKBOX:
        # Space on non-checkbox column → page-scroll (D-04)
        bar = self.results_table.verticalScrollBar()
        if event.modifiers() & Qt.KeyboardModifier.ShiftModifier:
            bar.triggerAction(QAbstractSlider.SliderAction.SliderPageStepSub)
        else:
            bar.triggerAction(QAbstractSlider.SliderAction.SliderPageStepAdd)
        return True  # consumed — do NOT let Qt toggle the current item
    # col == COL_CHECKBOX: fall through to super() for default checkbox toggle

return super().eventFilter(source, event)
```

**Required import check:** Verify `QAbstractSlider` is in the existing `from PyQt6.QtWidgets import (...)` block at the top of `genizah_app.py`. If not, add it.

---

## Regression Surface (GUARD-02)

### Existing Web Keyboard Shortcuts on `/search`
| Key | Handler | Risk |
|-----|---------|------|
| `Escape` | `handle_keyboard_shortcut` → `toggle_search_panel()` | None — Space branch is gated on `e.key !== ' '` |
| `/` | `handle_keyboard_shortcut` → `query_input.run_method('focus')` | None — independent key check |
| `Enter` on search input | `query_input.on('keydown.enter', ...)` | None — Input tag is in suppression set |
| Space on expand-toggle | `_content_col.on('keydown.space.self.prevent', ...)` | None — `.prevent` fires before global listener; activeElement will have `role=button` |
| Space on result action buttons | Standard button behavior (click) | None — `tag === 'BUTTON'` in suppression set |
| Space on result checkbox | Quasar checkbox toggle | None — `tag === 'INPUT'` in suppression set |
| PageUp/PageDown | Browser native scroll on `q-scrollarea__container` | None — the Space handler only intercepts `e.key === ' '` |

### Existing Desktop Keyboard Shortcuts
| Key | Handler | Risk |
|-----|---------|------|
| Down arrow on `query_input` | `eventFilter` → `_show_search_history_menu()` | None — different source/key |
| Up/Down in history menu | `eventFilter` → `super().eventFilter` | None — different source/key |
| Return/Delete in history menu | `eventFilter` | None — different source/key |
| Space on COL_CHECKBOX cell | Qt default (checkbox toggle via `itemChanged` signal) | Preserved — new branch falls through to `super()` when `col == COL_CHECKBOX` |
| PageDown/PageUp on results_table | Qt default (scroll bar page step) | Preserved — only `Key_Space` is intercepted, not `Key_PageDown`/`Key_PageUp` |
| Double-click → `show_full_text` | `doubleClicked` signal | None — mouse event, unaffected |
| `check_scroll_load` on scroll | `verticalScrollBar().valueChanged.connect(...)` | Preserved — `triggerAction` fires the `valueChanged` signal normally |

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `QAbstractSlider` is or is not already in `genizah_app.py`'s imports | Desktop Code Example | Compile error if missing — low risk, easy fix: add to import list |
| A2 | `QAbstractSlider.SliderAction.SliderPageStepAdd/Sub` is the correct enum path in PyQt6 | Desktop Code Example | Wrong enum path → AttributeError. Verify: `from PyQt6.QtWidgets import QAbstractSlider; QAbstractSlider.SliderAction.SliderPageStepAdd` |
| A3 | `behavior: 'instant'` is supported in all browsers GenizahSearch targets | Web Code Example | Safari < 15.4 does not support `scrollBehavior` in `scrollBy` options — fallback: use `scrollTop +=` assignment instead |

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 7.x (existing) |
| Config file | none — standard `pytest tests/` |
| Quick run command | `python -m pytest tests/test_space_scroll.py -x -q` |
| Full suite command | `python -m pytest tests/ -m "not gui and not render_smoke" -x -q` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | Notes |
|--------|----------|-----------|-------------------|-------|
| SCROLL-01 | Web: Space NOT stolen when `activeElement` is `INPUT` | unit (pure-logic JS string scan) | `pytest tests/test_space_scroll.py::test_web_space_scroll_js_installed -x` | AST/source guard verifying JS is present in search.py output |
| SCROLL-01 | Web: Space NOT stolen when `activeElement.role === 'button'` | unit (pure-logic) | `pytest tests/test_space_scroll.py::test_web_suppression_set_complete -x` | Static check: JS contains all required suppression conditions |
| SCROLL-01 | Web: Space NOT stolen when `.q-dialog` present | unit (pure-logic) | `pytest tests/test_space_scroll.py::test_web_dialog_guard -x` | Static check: JS contains `document.querySelector('.q-dialog')` |
| SCROLL-01 | Web: expand-toggle still gets Space when focused | source guard | `pytest tests/test_space_scroll.py::test_expand_toggle_space_prevent_intact -x` | Confirms `keydown.space.self.prevent` still present in search_results.py |
| SCROLL-02 | Desktop: Space on non-checkbox column triggers page-scroll | unit (widget stub) | `pytest tests/test_space_scroll.py -m gui -x` | Creates QTableWidget stub, verifies triggerAction called |
| SCROLL-02 | Desktop: Space on COL_CHECKBOX falls through to super() | unit (widget stub) | `pytest tests/test_space_scroll.py::test_desktop_space_checkbox_passthrough -m gui` | Verifies `super().eventFilter()` called, not consumed |
| GUARD-02 | Desktop: Shift+Space triggers page-up (SliderPageStepSub) | unit (widget stub) | `pytest tests/test_space_scroll.py::test_desktop_shift_space_page_up -m gui` | Direction check |
| GUARD-02 | Web: `window._gsSpaceScrollInstalled` guard present (no double-install) | source guard | `pytest tests/test_space_scroll.py::test_web_no_double_install_guard -x` | Static check: JS contains the flag |
| GUARD-02 | Existing Escape/slash keyboard shortcuts still work (no removed code) | source guard | `pytest tests/test_space_scroll.py::test_existing_shortcuts_preserved -x` | Confirms handle_keyboard_shortcut still has Escape and / cases |

### Observable Behaviors (for manual smoke / Nyquist validation)
1. **Web — basic scroll:** Load `/search`, run a query, press Space → results pane scrolls down ~1 viewport. Press Shift+Space → scrolls up.
2. **Web — checkbox not stolen:** Tab to a result checkbox, press Space → checkbox toggles, results do NOT scroll.
3. **Web — expand not stolen:** Tab to a result card body (role=button), press Space → card expands/collapses, results do NOT scroll.
4. **Web — dialog not stolen:** Click Quick View to open the fullscreen dialog, press Space → results do NOT scroll (dialog stays in front, no background scroll).
5. **Web — action button not stolen:** Tab to a Browse/QuickView button, press Space → button activates, results do NOT scroll.
6. **Desktop — basic scroll:** Run a search, click anywhere in results to focus the table, press Space → page scrolls down. Shift+Space → page scrolls up.
7. **Desktop — checkbox preserved:** Click on the checkbox column cell for a result, press Space → checkbox toggles (not a scroll).
8. **Desktop — PageDown/PageUp unaffected:** Press PageDown in the results table → native page-scroll (not broken by this change).

### Sampling Rate
- **Per task commit:** `python -m pytest tests/test_space_scroll.py -x -q` (< 5s)
- **Per wave merge:** `python -m pytest tests/ -m "not gui and not render_smoke" -x -q`
- **Phase gate:** Full suite green (`bulk` + `gui` split) before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `tests/test_space_scroll.py` — create this file with all test cases above. Mix of AST/static guards (no QApplication needed) and gui-marked widget tests (need `QApplication.instance() or QApplication([])`).
- [ ] Add `"test_space_scroll.py"` to `_GUI_TEST_FILES` set in `tests/conftest.py` for the widget-level desktop tests (the ones that construct QTableWidget and dispatch key events).

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python / PyQt6 | Desktop tests | ✓ | (existing install) | — |
| NiceGUI | Web JS injection | ✓ | (existing install) | — |
| pytest | Tests | ✓ | (existing, 5146 tests collected) | — |

No missing dependencies. This phase is purely additive client-side JS + eventFilter Python logic.

---

## Security Domain

SCROLL-01/SCROLL-02 add a keyboard scroll affordance. No authentication, no data access, no user input passed to the server, no new API surface. ASVS V5 input validation is not applicable (the JS reads `e.key` to check for `' '` and does not pass it to any backend). Security impact is negligible.

---

## State of the Art

| Old Approach | Current Approach | Impact |
|--------------|------------------|--------|
| `ui.keyboard on_key` for all shortcuts | Pure client-side `document.addEventListener` for shortcuts requiring `preventDefault` | Correct separation: server-side for logic, client-side for event suppression |
| `scrollBy({behavior: 'smooth'})` | `scrollBy({behavior: 'instant'})` or `scrollTop +=` | Instant is more predictable for keyboard navigation; smooth feels sluggish |

---

## Open Questions

1. **`behavior: 'instant'` vs `scrollTop +=` for scroll compatibility**
   - What we know: `behavior: 'instant'` is part of the CSSOM View spec but older Safari (< 15.4) may fall back to instant anyway or ignore the options object.
   - What's unclear: Whether the target browsers include old Safari.
   - Recommendation: Use `scrollTop += delta` assignment as the fallback: `inner.scrollTop += delta; e.preventDefault();` — this works universally and is already what old browsers do.

2. **`COL_CHECKBOX` visibility when no row is selected (desktop)**
   - What we know: When the table has items but none is currently selected/focused, `currentColumn()` may return -1 or the last column. Qt's `currentColumn()` returns -1 when there is no current item.
   - What's unclear: Whether `currentColumn() == -1` should route to scroll or fall through.
   - Recommendation: Treat `currentColumn() != COL_CHECKBOX` (including -1) as "scroll." This is correct — if no item is focused on the checkbox column, Space should scroll.

---

## Sources

### Primary (HIGH confidence)
- `web/pages/search.py` — direct read: keyboard handler at ~line 1959, scroll area at ~1763, setup_scroll_collapse at ~1980, _after_delay at ~111
- `web/pages/search_results.py` — direct read: create_result_card showing checkbox (ui.checkbox, line 428), expand-toggle with `keydown.space.self.prevent` (line 454), action buttons (lines 683-762)
- `nicegui/elements/keyboard.js` — direct read: confirms round-trip `$emit`, no `preventDefault` possible from Python callback
- `nicegui/elements/keyboard.py` — direct read: confirms `ignore` behavior, `KeyEventArguments` structure
- `genizah_app.py` — direct read: `results_table` setup (~4828), `COL_CHECKBOX` (~4814), `eventFilter` (~17891), `installEventFilter` (~4879), checkbox item creation (~16111)
- `tests/conftest.py` — direct read: `_GUI_TEST_FILES`, `gui` marker, `collect_ignore_glob` pattern
- `web/pages/browse.py:4706` — direct read: existing `document.addEventListener('keydown', ...)` client-side keyboard pattern in NiceGUI page

### Secondary (MEDIUM confidence)
- `web/pages/home.py:316` — `keydown.space` Vue modifier pattern on `role=button` elements (confirms `.prevent` is not needed when the button navigates)
- `tests/test_no_server_side_stop_propagation.py` — AST guard pattern for client-side-only propagation control (confirms project convention)
- `tests/test_join_workbench_rotate.py` — desktop widget test pattern without pytest-qt (confirms `QApplication.instance() or QApplication([])` pattern and gui marker)

### Tertiary (LOW confidence / ASSUMED)
- `QAbstractSlider.SliderAction.SliderPageStepAdd/Sub` API — [ASSUMED] standard PyQt6 enum path; verify import before writing plan task
- `scrollBy({behavior: 'instant'})` browser compatibility — [ASSUMED] widely supported, but recommend `scrollTop +=` fallback for Safari < 15.4

---

## Metadata

**Confidence breakdown:**
- Web mechanism (preventDefault via client-side JS): HIGH — verified from keyboard.js source and existing browse.py pattern
- Web scroll target (`q-scrollarea__container`): HIGH — verified from existing setup_scroll_collapse JS
- Web suppression set membership: HIGH — verified by reading all result card controls in search_results.py
- Desktop eventFilter hook: HIGH — verified from existing eventFilter structure
- Desktop checkbox is ItemIsUserCheckable (not cell widget): HIGH — verified from result population code
- Desktop QAbstractSlider API path: MEDIUM — standard PyQt6, not yet confirmed via import check
- Testing approach: HIGH — mirrors existing test patterns exactly

**Research date:** 2026-06-27
**Valid until:** 2026-07-27 (stable APIs, no fast-moving dependencies)
