---
phase: quick
plan: 260326-jwi
type: execute
wave: 1
depends_on: []
files_modified:
  - genizah_app.py
autonomous: true
requirements: []
must_haves:
  truths:
    - "Rapidly clicking prev/next in browse tab does not crash the application"
    - "Image displayed always corresponds to the currently selected page"
    - "No stale image from a previous navigation appears after switching pages"
  artifacts:
    - path: "genizah_app.py"
      provides: "Race-condition-safe ManuscriptViewerWidget image loading"
      contains: "_load_generation"
  key_links:
    - from: "ManuscriptViewerWidget.display_image"
      to: "_load_generation"
      via: "generation guard rejects stale thread callbacks"
      pattern: "_load_generation"
---

<objective>
Fix desktop browse tab crash (exit code 0xC0000409 / STATUS_STACK_BUFFER_OVERRUN) when rapidly navigating forward/backward through manuscript images.

Purpose: Prevent Qt C++ runtime abort caused by overlapping async image load threads delivering stale callbacks to the ManuscriptViewerWidget during rapid navigation.

Output: Stable browse tab image navigation under rapid clicking.
</objective>

<execution_context>
@.planning/quick/260326-jwi-fix-desktop-browse-tab-crash-when-rapidl/260326-jwi-PLAN.md
</execution_context>

<context>
@genizah_app.py (ManuscriptViewerWidget class, lines ~1966-2490, and browse navigation lines ~28243-28500)

Root cause analysis — there are 5 race conditions in ManuscriptViewerWidget:

1. **`display_image` (line 2430) has NO generation guard** — `_load_generation` is incremented in `set_page` and checked in `_on_thumbnail_ready`, but the main `display_image` slot connected to `loader_thread.image_loaded` does NOT check generation. A stale ImageLoaderThread that completes after navigation changed will overwrite the current image.

2. **`loader_thread.wait(500)` blocks UI thread** (line 2414) — each `set_page` call blocks up to 500ms waiting for the previous thread to finish. Rapid clicking queues these waits, causing UI freeze and potential Qt event loop corruption.

3. **`_load_thumbnail_async` spawns untracked `threading.Thread`** (line 2386) — these raw Python threads are never cancelled or tracked. Multiple thumbnail fetches pile up, and any can deliver a stale pixmap via the `_thumbnail_ready` signal (though this one does have a generation guard).

4. **`_preload` replaces `preload_worker` without cancelling previous** (line 2341) — the old preload thread loses its reference and may be garbage collected while running, or complete and emit to a disconnected signal.

5. **Signal connections accumulate on `loader_thread`** (lines 2422-2424) — each `set_page` creates a new `ImageLoaderThread` and connects `image_loaded`/`load_failed`, but the old thread (if still running after the 500ms wait) retains its connected slots. The old thread reference is replaced, but if it finishes between `cancel()` and the new assignment, its signal fires into the still-connected `display_image`.

The 0xC0000409 crash is most likely from: (a) a stale ImageLoaderThread emitting `image_loaded` with a QImage after the widget context has moved on, combined with rapid `wait()` blocking causing Qt event loop re-entrancy; or (b) Python GC collecting a QThread that's still running (the preload_worker case).
</context>

<tasks>

<task type="auto">
  <name>Task 1: Add generation guard to ManuscriptViewerWidget image loading and eliminate blocking waits</name>
  <files>genizah_app.py</files>
  <action>
Fix the 5 race conditions in ManuscriptViewerWidget (class starts at line ~1966):

**Fix 1: Generation guard on `display_image` (CRITICAL)**
In `set_page` (line 2388), capture `_load_generation` into a local variable before creating the loader thread. Instead of directly connecting `loader_thread.image_loaded` to `self.display_image`, connect to a lambda/wrapper that checks generation:

```python
gen = self._load_generation
self.loader_thread.image_loaded.connect(
    lambda img, g=gen: self.display_image(img) if g == self._load_generation and not self._closing else None
)
```

Similarly guard the `load_failed` lambda.

**Fix 2: Replace blocking `wait(500)` with non-blocking cancel**
In `set_page` (lines 2411-2414), replace:
```python
if self.loader_thread and self.loader_thread.isRunning():
    self.loader_thread.cancel()
    self.loader_thread.wait(500)
```
With:
```python
if self.loader_thread and self.loader_thread.isRunning():
    self.loader_thread.cancel()
    # Disconnect all signals to prevent stale delivery — do NOT block with wait()
    try:
        self.loader_thread.image_loaded.disconnect()
        self.loader_thread.load_failed.disconnect()
    except (TypeError, RuntimeError):
        pass
```
The generation guard on the new connection ensures correctness even if the old thread eventually emits. No need to wait.

**Fix 3: Cancel previous preload worker properly**
In `_preload` (line 2334), before creating new preload_worker, cancel and disconnect the old one:
```python
if self.preload_worker and self.preload_worker.isRunning():
    self.preload_worker.cancel()
    try:
        self.preload_worker.image_loaded.disconnect()
        self.preload_worker.load_failed.disconnect()
    except (TypeError, RuntimeError):
        pass
```

**Fix 4: Track thumbnail threads for cleanup**
Add `self._thumb_threads = []` to `__init__` (after line 1978). In `_load_thumbnail_async`, append each threading.Thread to `_thumb_threads`. In `stop_threads`, set `_closing = True` first (already done), then clean up the list. The generation guard in `_on_thumbnail_ready` already handles stale delivery, so this is mainly for reference tracking to prevent GC issues.

**Fix 5: Apply same pattern to `cancel_browse_image_thread` (line 28549)**
The main app's `cancel_browse_image_thread` also has `browse_img_thread.wait(500)`. Replace with disconnect-based cancellation:
```python
def cancel_browse_image_thread(self):
    if getattr(self, 'browse_img_thread', None) and self.browse_img_thread.isRunning():
        self.browse_img_thread.cancel()
        try:
            self.browse_img_thread.image_loaded.disconnect()
            self.browse_img_thread.load_failed.disconnect()
        except (TypeError, RuntimeError):
            pass
```

**Fix 6: Apply same pattern to `stop_threads` (line 2344)**
Replace `wait(2000)` and `wait(1000)` with disconnect + shorter non-blocking timeout:
```python
def stop_threads(self):
    self._closing = True
    if self.loader_thread and self.loader_thread.isRunning():
        self.loader_thread.cancel()
        try:
            self.loader_thread.image_loaded.disconnect()
            self.loader_thread.load_failed.disconnect()
        except (TypeError, RuntimeError):
            pass
        self.loader_thread.wait(500)  # Keep short wait only on widget destruction
    if self.preload_worker and self.preload_worker.isRunning():
        self.preload_worker.cancel()
        self.preload_worker.wait(500)
```

Also apply the same disconnect-before-wait pattern to `ResultDialog.closeEvent` (line 8352) for `browse_img_thread.wait()` — add a short timeout (500ms) instead of indefinite wait.

**IMPORTANT**: Do NOT change the browse_navigate or navigate_manuscript flow. Only fix the image loading layer in ManuscriptViewerWidget and the browse thumbnail thread.
  </action>
  <verify>
    <automated>cd C:/genizahsearch && python -c "from genizah_app import ManuscriptViewerWidget; print('Import OK')" 2>&1 | head -5</automated>
    Verify: grep for `wait(500)` in set_page — should NOT have blocking wait in the navigation path.
    Verify: grep for `_load_generation` in display_image connection — should have generation guard.
  </verify>
  <done>
    - ManuscriptViewerWidget.set_page does NOT block UI with wait() during normal navigation
    - display_image connection has generation guard matching _on_thumbnail_ready pattern
    - preload_worker is cancelled before replacement
    - cancel_browse_image_thread uses disconnect instead of blocking wait
    - All changes are in the image loading layer only — no browse navigation logic changed
  </done>
</task>

<task type="auto">
  <name>Task 2: Add navigation debounce to prevent rapid-fire set_page calls</name>
  <files>genizah_app.py</files>
  <action>
Add a debounce mechanism to ManuscriptViewerWidget.set_page to coalesce rapid navigation clicks into a single image load. This is a defense-in-depth measure — even with the generation guards from Task 1, spawning dozens of threads per second is wasteful.

In ManuscriptViewerWidget.__init__ (after line 1978), add:
```python
self._nav_debounce_timer = None  # QTimer for debouncing rapid set_page calls
self._pending_page_idx = None    # Deferred page index
```

Modify `set_page` to use a short debounce (150ms):
```python
def set_page(self, index):
    if not self.active_list:
        self.scroll_area.set_image(None)
        self.scroll_area.set_status_message(tr("No images available"))
        return

    # Bounds check
    if index < 0: index = 0
    if index >= len(self.active_list): index = len(self.active_list) - 1

    self.current_idx = index
    self._load_generation += 1  # Invalidate any in-flight callbacks immediately

    # Update status text immediately for responsiveness
    self.scroll_area.set_status_message(tr("Loading..."))

    # Cancel any pending debounced load
    if self._nav_debounce_timer is not None:
        self._nav_debounce_timer.stop()

    # Store pending index and schedule actual load
    self._pending_page_idx = index
    self._nav_debounce_timer = QTimer()
    self._nav_debounce_timer.setSingleShot(True)
    self._nav_debounce_timer.timeout.connect(self._execute_set_page)
    self._nav_debounce_timer.start(150)  # 150ms debounce
```

Add `_execute_set_page` method right after `set_page`:
```python
def _execute_set_page(self):
    """Actually load the image after debounce settles."""
    index = self._pending_page_idx
    if index is None or self._closing:
        return

    # Re-check bounds (active_list may have changed)
    if not self.active_list:
        return
    if index < 0: index = 0
    if index >= len(self.active_list): index = len(self.active_list) - 1

    self.current_idx = index
    self._load_generation += 1  # Fresh generation for actual load
    gen = self._load_generation

    img_data = self.active_list[index]
    base_url = img_data['url']

    # Thumbnail preview
    thumb_url = img_data.get('thumb_url', '')
    if not thumb_url and 'iiif.nli.org.il' in base_url:
        thumb_url = f"{base_url}/full/400,/0/default.jpg"

    # Cancel previous loader (non-blocking)
    if self.loader_thread and self.loader_thread.isRunning():
        self.loader_thread.cancel()
        try:
            self.loader_thread.image_loaded.disconnect()
            self.loader_thread.load_failed.disconnect()
        except (TypeError, RuntimeError):
            pass

    # Load thumbnail first
    if thumb_url:
        self._load_thumbnail_async(thumb_url)

    # Load full image
    final_url = self._resolve_url(base_url)
    self.loader_thread = ImageLoaderThread(final_url)
    self.loader_thread.image_loaded.connect(
        lambda img, g=gen: self.display_image(img) if g == self._load_generation and not self._closing else None
    )
    self.loader_thread.load_failed.connect(
        lambda g=gen: None if g != self._load_generation or self._closing else self.scroll_area.set_status_message(tr("No Image"))
    )
    self.loader_thread.start()

    # Preload next
    self._preload(index + 1)
```

Also add cleanup in `stop_threads`:
```python
if self._nav_debounce_timer is not None:
    self._nav_debounce_timer.stop()
```

**IMPORTANT**: The debounce only affects the image thread spawning. `current_idx` and `_load_generation` are updated immediately so the UI state (page number display) stays responsive. The 150ms delay only affects when the network request fires.
  </action>
  <verify>
    <automated>cd C:/genizahsearch && python -c "
from genizah_app import ManuscriptViewerWidget
v = ManuscriptViewerWidget.__init__
import inspect
src = inspect.getsource(v)
assert '_nav_debounce_timer' in src, 'Missing debounce timer init'
print('Debounce timer found in __init__')
src2 = inspect.getsource(ManuscriptViewerWidget.set_page)
assert '_nav_debounce_timer' in src2 or '_execute_set_page' in src2, 'Missing debounce in set_page'
print('Debounce wired in set_page')
print('All checks passed')
" 2>&1 | tail -5</automated>
  </verify>
  <done>
    - Rapid clicking prev/next coalesces into a single image load after 150ms of idle
    - Page number UI updates immediately (responsive feel)
    - Only the final destination page triggers a network request
    - Timer cleanup in stop_threads prevents post-destruction callbacks
  </done>
</task>

</tasks>

<verification>
1. Launch desktop app, open browse tab, navigate to a multi-page manuscript
2. Rapidly click Next 10+ times in quick succession — app should NOT crash
3. Rapidly click Prev 10+ times — app should NOT crash
4. Rapidly alternate Next/Prev — app should NOT crash
5. Final displayed image should match the page indicated in the combo box
6. Navigate between different manuscripts (prev/next manuscript) rapidly — no crash
</verification>

<success_criteria>
- No 0xC0000409 crash on rapid image navigation
- Image always matches current page selection
- No UI freeze during rapid navigation (no blocking wait)
- No stale images flash after navigation settles
</success_criteria>

<output>
After completion, create `.planning/quick/260326-jwi-fix-desktop-browse-tab-crash-when-rapidl/260326-jwi-SUMMARY.md`
</output>
