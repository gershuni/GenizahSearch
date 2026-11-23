## [Unreleased] -- Desktop Reading Desk: the open desk comes back after a restart (desktop, branch `claude/determined-borg-0825ee`)

### Fixed
- **A Reading Desk left open is open again on the next start.** The session restore used to bring
  back only the manuscript Browse showed, so after a restart Browse showed one manuscript instead
  of the desk. The desk now comes back over it as it was left: its manuscripts in their order, the
  PGP document it was opened for, the text chosen for each, and the image view, the image on show
  and the puzzle images on show. A manuscript that is no longer in the catalogue or the index is
  left out without a message; a session with no desk restores as before.
- The desk opens once the app's own startup loading has finished (at most 30 seconds), so the
  window does not freeze while it is drawn; closing the app before then still keeps the desk.
- **Leaving the desk shows Browse's image again** when that image arrived while the desk was open
  (which is always so for a restored desk): Browse's viewer used to stay empty.
