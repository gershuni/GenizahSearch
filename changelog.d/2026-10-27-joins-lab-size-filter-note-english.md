## [Unreleased] -- Joins Lab: the width-filter note reads as a sentence in English (desktop, branch `claude/sweet-noether-6fdtsi`)

### Fixes
- **Joins Lab "Filter ▾" dialog: the note under the width filter showed the literal text "Size filter note"
  in the English UI.** The string was looked up by a placeholder key, and `tr()` returns the key itself in
  English. The key is now the English sentence ("Size filtering is optional — dimensions are shown as a hint
  only."), following the convention that keys are the English text; the Hebrew text is unchanged.
