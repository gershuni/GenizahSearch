---
status: diagnosed
trigger: "Mouse wheel zooms images instead of scrolling in Virtual Reading Desk"
created: 2026-02-19T00:00:00Z
updated: 2026-02-19T00:00:00Z
---

## Current Focus

hypothesis: CONFIRMED - ZoomableScrollArea.wheelEvent unconditionally zooms without checking modifier keys
test: Read wheelEvent implementation at line 1477
expecting: No modifier check present
next_action: Return diagnosis

## Symptoms

expected: Mouse wheel scrolls the Virtual Reading Desk view (standard behavior)
actual: Mouse wheel zooms images instead of scrolling
errors: None (behavioral issue)
reproduction: Open VRD, use mouse wheel
started: Unknown - may have always been this way

## Eliminated

## Evidence

- timestamp: 2026-02-19
  checked: ZoomableScrollArea.wheelEvent at line 1477-1482
  found: |
    def wheelEvent(self, event):
        self._auto_fit_enabled = False
        delta = event.angleDelta().y()
        factor = 1.1 if delta > 0 else 0.9
        self._apply_zoom(factor)
        event.accept()
    No modifier key check. ALL wheel events are consumed for zoom.
    event.accept() prevents propagation to parent QScrollArea.
  implication: Plain mouse wheel always zooms, never scrolls. Standard pattern is Ctrl+wheel = zoom, plain wheel = scroll.

- timestamp: 2026-02-19
  checked: VRD layout hierarchy
  found: |
    VRD stacks multiple ZoomableScrollArea widgets inside a QScrollArea (_browse_rd_image_scroll).
    Each ZoomableScrollArea has setMinimumHeight(400), setMaximumHeight(600).
    Scrollbars are hidden (ScrollBarAlwaysOff at lines 1400-1401).
    Because wheelEvent calls event.accept(), the parent QScrollArea never receives wheel events.
  implication: Users cannot scroll through stacked images in the VRD at all using mouse wheel.

- timestamp: 2026-02-19
  checked: Second wheelEvent at line 1990
  found: Belongs to a different class (horizontal text scroll widget), unrelated to VRD
  implication: Only the ZoomableScrollArea wheelEvent needs modification

## Resolution

root_cause: ZoomableScrollArea.wheelEvent (line 1477-1482 of genizah_app.py) unconditionally intercepts ALL mouse wheel events and applies zoom, with no check for modifier keys. It also calls event.accept() which prevents the parent QScrollArea from receiving wheel events. This means (1) in the VRD, users cannot scroll through stacked manuscript images using the mouse wheel, and (2) in all uses of ZoomableScrollArea, the standard Ctrl+wheel=zoom / plain-wheel=scroll convention is violated.
fix: Modify wheelEvent to check for Ctrl modifier. Plain wheel should propagate to parent (scroll). Ctrl+wheel should zoom.
verification:
files_changed: []
