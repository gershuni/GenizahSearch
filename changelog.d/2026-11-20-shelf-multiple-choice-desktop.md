## [Unreleased] -- Exclude from the shelf, several at once, on the desktop (desktop, branch `claude/shelf-picker-desktop-multi`)

### Added
- **Exclude manuscripts from the shelf** on the desktop, in the Search and the Composition tab's
  Exclude Manuscripts dialog: "Pick from the shelf" opens the shelf picker in its multiple-choice
  mode, where whole volumes (or collections, groups, ranges) and single shelfmarks from any library
  are ticked. A whole volume is one chip; unticking a shelfmark inside it lowers its count. A bar of
  chips shows what is chosen, with "+N more" opening the full list, beside a running count, Clear,
  Save as a list and "Exclude them". The choice comes back as a source of its own above the dialog's
  tabs, "From the shelf: <volumes>", and applies with the dialog's Apply; it can be removed there
  like any source (`desktop/widgets/shelfmark_tree_picker.py`, `desktop/dialogs_filter.py`).
- **What is chosen is kept** for each tab (Search, Composition) until it is applied or cleared,
  across closing the picker and restarting the app, whatever the restore setting (the session
  file's `shelf_selections`); a Cancel in the Exclude dialog applies nothing and keeps it.

### Fixed
- **Exclude Manuscripts' Clear All** also clears the lists selected in the From List tab, which
  came back on Apply.
- **An edit after Resolve Shelfmarks** is no longer lost: Apply used the earlier resolution and
  dropped ids added afterwards (from a list, a file or typed); an edit now forgets it.

### Notes
- The picker's ids are excluded as they are: the shelfmarks shown are never looked up again, so a
  second record sharing a shelfmark is not excluded with it.
