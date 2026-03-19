---
phase: quick
plan: 260319-dt1
subsystem: image-viewer
tags: [image-controls, brightness, contrast, gamma, invert, web, desktop]
dependency_graph:
  requires: []
  provides: [image-adjustment-controls]
  affects: [web/pages/browse.py, web/pages/search.py, genizah_app.py, genizah_translations.py]
tech_stack:
  added: []
  patterns: [CSS-filter-chain, SVG-feComponentTransfer-gamma, LUT-pixel-processing, QTimer-debounce]
key_files:
  created: []
  modified:
    - web/pages/browse.py
    - web/pages/search.py
    - genizah_app.py
    - genizah_translations.py
decisions:
  - CSS filter approach for web (brightness/contrast/invert native + SVG gamma)
  - LUT-based QImage pixel processing for desktop with 80ms debounce
  - Per-viewer SVG gamma filters to isolate reading desk images
  - Export (copy/save) applies adjustments via same LUT pipeline
metrics:
  duration: 30min
  completed: 2026-03-19
---

# Quick Task 260319-dt1: Image Adjustment Controls Summary

CSS-filter image controls (brightness, contrast, gamma, invert) across all web and desktop image viewers with LUT-based desktop export.

## Tasks Completed

| # | Task | Commit | Key Changes |
|---|------|--------|-------------|
| 1 | Web image controls -- all viewers | 4a508f10 | browse.py (standard + fullscreen + reading desk), search.py (advanced viewer), translations |
| 2 | Desktop image controls -- ManuscriptViewerWidget | 6e7636ff | ZoomableScrollArea LUT filters, toolbar sliders, export with adjustments |

## Implementation Details

### Web (CSS Filters)
- **manuscriptViewer** (browse standard): brightness/contrast/gamma/invert state on viewer object, `_applyFilters()` builds CSS filter string, SVG `#gamma-main` for gamma
- **fsViewer** (browse fullscreen): Same pattern with dedicated `#gamma-fs` SVG filter
- **rdViewers** (reading desk): Per-image `rdSetBrightness/rdSetContrast/rdSetGamma/rdToggleInvert` JS functions, dynamically created `#gamma-{viewerId}` SVG filters
- **advViewer** (search advanced): Same pattern with `#gamma-adv` SVG filter
- Controls toolbar: `ui.slider` for B/C/G, `ui.button` for invert/reset, all labels translated
- Image navigation reset: sliders reset to defaults on page change

### Desktop (QImage LUT Processing)
- `ZoomableScrollArea`: Added `_brightness`, `_contrast`, `_gamma`, `_invert` state
- `_build_lut()`: 256-entry lookup table combining contrast (multiply around midpoint), brightness (offset), gamma (power), invert (complement)
- `_apply_display_filters()`: Applies LUT to all RGB channels via `QImage.bits()` direct byte access
- `_schedule_filter_update()`: QTimer 80ms debounce prevents excessive reprocessing during slider drag
- `_apply_adjustments_to_pixmap()`: Same LUT pipeline used by `_get_rotated_pixmap()` for export
- `ManuscriptViewerWidget.init_ui()`: Second toolbar row with QSlider (B: -100..+100, C: -100..+100, G: 0.20..3.00) + QPushButton (Invert toggle, Reset)
- `display_image()`: Resets all adjustment sliders on new image load

### Translations
- Added to TRANSLATIONS dict: Brightness, Contrast, Gamma, Invert, Reset Image (Hebrew)

## Post-Plan Fixes

| Commit | Fix |
|--------|-----|
| `ecb80438` | Desktop race condition: stale thumbnail/timer overwrites new image (cancel `_adj_timer` in `set_image()`, `_load_generation` counter) |
| `02d42151` | Browse page crash: `ui.run_javascript` in async `load_page` needs NiceGUI slot context (`with content_container:` wrapper) |
| `bf740084` | Replace text labels with icons (brightness_6, contrast, timeline, exposure, restart_alt) |
| `004e80a4` | Rename tooltip to "Invert Colors" / "הפוך צבעים" |
| `a4764fd7` | Use `exposure` icon (±) for Invert Colors — distinct from contrast half-circle |

## Self-Check: PASSED

User verified all viewers working. Approved 2026-03-19.
