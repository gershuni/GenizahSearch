## [Unreleased] -- Joins Lab and Personal Lists on a phone (web, branch `ccr-bb516a2e-5hnjn2`)

Owner's screenshots, 2026-10-22, at 390px: the Joins Lab builder's controls lay across the
anchor's transcription while the page scrolled under it, and the Personal Lists tree was a
97px column with its heading wrapped and its three buttons stacked.

### Fixed

- **Joins Lab under 1024px.** The anchor pane (image, transcription, known joins) was a 380px
  sticky column with its own scroll at every width, which left the builder and the candidates
  10px on a phone. The pane now sits above the work column, each at the full width and the pane
  no longer sticky; on a phone the transcription keeps a bounded scroll (45vh) so the builder
  stays within reach, the viewer's control bar is one row of 34px buttons (in the page and in
  Compare), and the Compare window's two panes stack, anchor above candidate. The
  label the page rendered below everything for a narrow-screen anchor strip that was never
  built (empty, then the sys_id in monospace) is gone.
- **Personal Lists under 769px.** The My Lists tree was the splitter's 25% panel. The splitter
  now stacks, the tree above the list at the full width with a bounded scroll, the page scrolls
  instead of a fixed-height shell, choosing a list scrolls it into view, and an item card's
  action icons are a row under the text (as a column beside it they were one icon per line
  and the card 700px tall). On a small phone
  (480px) dialog cards also drop their 400px / 500px minimum width, which made the create-list,
  trash and edit dialogs wider than the screen.
- The hooks are pinned in `tests/test_phone_layout_rules.py`.
