## [Unreleased] -- Free image rotation at every screen width (web, branch `claude/image-rotation-control-813739`)

### Fixed
- **Free rotation was gone below 1440px.** Since the compact layout of 2026-10-22 the /browse
  rotation slider sat in the image toolbar and was hidden on any screen narrower than 1440px, so
  a laptop or a phone could only turn the image by 90°. The slider now sits behind the
  **Image adjustments** (tune) button with brightness, contrast and gamma, at every width, so the
  toolbar stays one row and the image keeps its full height; on a wide screen the toolbar is one
  control shorter than before.

### Changed
- **The slider runs from -180° to 180°**, upright in the middle, so a small tilt either way is
  beside the centre (it was 0-360, where a slight turn to the left meant dragging to the far
  end). The angle shows beside it; the arrow keys move it one degree at a time, and the image
  turns in the browser while it is dragged. The 90° buttons land in the same range and move the
  slider with them; Reset Image now zeroes the rotation as well, keeping the zoom.
- **The full-screen editor and Quick View** have the same slider. In Quick View the 90° buttons
  and Reset View go through it, Reset View now resets the brightness, contrast and gamma sliders
  too, and a library image that arrives after the dialog opened keeps the reader's angle.
