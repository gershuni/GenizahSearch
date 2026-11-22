## P3 desktop: A shelfmark opened in the first seconds after start can fail with "dictionary changed size during iteration"
Where: `shared/metadata_manager.py::_iter_shelfmark_sources` (iterates `self.csv_bank` while the catalogue is still being loaded), reached from `MetadataManager.resolve_system_by_shelfmark` -> `_get_shelfmark_index`
Opened: 2026-11-20

Seen while driving the desktop window for the shelf picker PR (gershuni/GenizahSearch-dev#68):
`GenizahGUI.go_to("T-S AS 39.7")` called 4.7 s after `on_startup_finished`, while `libraries.csv`
was still filling `csv_bank` on another thread, raised `RuntimeError: dictionary changed size
during iteration` out of `browse_load`. The same call a few seconds later works. A reader who types
a shelfmark in Browse or the Go to box right after the window opens can hit it; the error escapes
the slot. Fix sketch: build the shelfmark index from a snapshot (`list(self.csv_bank.items())`), or
wait for the catalogue's ready flag that `_check_shelfmark_completer_ready` already polls, and say
"still loading" until then. Unrelated to the shelf picker; found by it.
