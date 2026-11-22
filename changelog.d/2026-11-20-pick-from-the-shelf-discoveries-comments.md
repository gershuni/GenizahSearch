## [Unreleased] -- Pick from the shelf in Share a Discovery and in comments (web, branch `claude/shelf-picker-web-discovery`)

### Added
- **Pick from the shelf when sharing a discovery**: the linked document can be chosen on the shelf
  (library > collection > volume > shelfmark), beside Recent Activity and My Lists, in the new and
  in the edit dialog; and "Additional shelfmarks" can be added from the shelf, each keeping the
  manuscript it names (typing over its text makes it a typed shelfmark again)
  (`web/components/shelf_pick.py`, `web/pages/discoveries.py`).
- **Pick from the shelf for a comment's shelfmark reference**, beside "Add shelfmark reference":
  the same `[[shelfmark:…|id:…]]` reference the lists insert; the comment is not sent
  (`web/components/comment_dialog.py`).
- The buttons are shown only while the website serves the shelf tree file.

### Fixed
- **A typed additional shelfmark of a discovery was dropped**: the box's text was never kept, so a
  shelfmark typed under "Additional shelfmarks" was left out when the discovery was shared; and
  removing one entry could remove another.
