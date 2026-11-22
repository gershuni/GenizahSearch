## [Unreleased] -- The shelf picker reads your lists without holding the site (web, branch `claude/shelf-picker-lists-off-loop`)

### Fixed
- **Add to List from the shelf picker's multiple choice read the reader's lists on the server's
  event loop.** The lists manager's `data` is a fresh database read, so a slow answer held every
  reader on the server until it came. The lists are now read in a worker thread
  (`web/components/shelfmark_tree_picker.py::_load_list_options`). The reader's sign-in and database
  client are resolved first, on the loop (`UserListsManager._worker_context`), because a worker
  cannot read the visitor's storage. A failed read offers a new list only.

### Tests
- `tests/render_smoke/test_shelf_picker_round3_render_smoke.py::test_the_list_chooser_reads_the_lists_off_the_event_loop`:
  the read runs off the loop thread with the context resolved on it. It fails with the read moved
  back onto the loop, and with the context left unresolved.
