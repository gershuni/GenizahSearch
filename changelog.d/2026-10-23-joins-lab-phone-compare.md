## [Unreleased] -- Joins Lab on a phone: Compare shows the two images, and the anchor stays in view (web, branch `ccr-d332c10d-xiyooi`)

The Lab's job on a phone is "anchor vs candidate", and the one screen built for that was the
one that failed at it (owner, 2026-10-23): with the Compare window's panes stacked, each pane
was a head, an image capped at 40vh and the whole transcription, so the candidate's image sat
several screens below the anchor's and the two were never on the screen together.

### Fixed

- **Compare on a phone shows the two images alone.** Below 641px each pane's head keeps only
  the role, the shelfmark and the badges on one row; the title, details and info rows and both
  transcriptions hide; and each image is capped at 32vh (31dvh), so the anchor and the candidate
  are on the screen together with the verdict bar. A new "Show text" button in the window header
  brings the transcriptions and details back for as long as the window is open. The header
  stays one row on a phone: the "Compare" title goes, and Add as Join and Add to Puzzle go
  icon-only and dense.
- **A phone on its side compares side by side.** On any screen under 521px tall the panes sit
  beside each other (two 420px panes on an 844px phone, wider than the desktop's 380px anchor
  pane; two 312px panes on a 640px-wide phone, where the phone rule had stacked them), the text
  hides the same way, and each image takes the height the window header, the head, the control
  bar and the verdict bar leave.
- **The anchor stays in view while candidates scroll.** Under 1024px the anchor pane stacks above
  the work column and scrolled away with the candidates. Once it has left the screen, a strip
  under the header shows the anchor's thumbnail and shelfmark, moves to the top while the
  header's reveal has hidden it, and a tap scrolls back up
  (`web/static/js/joins_lab_anchor_strip.js`). The narrow-screen strip the Phase-119 design named
  was never built; this is it.
- The hooks are pinned in `tests/test_phone_layout_rules.py`.
