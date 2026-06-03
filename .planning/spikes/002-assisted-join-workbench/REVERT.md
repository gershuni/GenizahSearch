# Join Workbench sketch — how to revert (it's a throwaway)

The whole feature is a SKETCH. Two reversal paths depending on whether it has been committed.

## Path A — still uncommitted (the situation as of 2026-06-02)

Everything lives as (a) one **untracked** new file and (b) **purely additive** uncommitted
hunks in two tracked files (verified: +93 lines, 0 deletions). One-shot revert:

```powershell
git restore desktop/result_dialog.py genizah_app.py   # drops every production-file hook
Remove-Item desktop/join_workbench.py                  # drops the sketch itself
```

⚠ `git restore` discards ALL uncommitted changes in those two files. Confirmed safe today
because their only uncommitted changes ARE the joins sketch — re-check `git diff --stat`
before running if other work has since landed in them.

## Path B — after it has been committed

Every production-file intrusion is tagged with the marker `JOINS-SKETCH`. Find them all:

```powershell
git grep -n "JOINS-SKETCH"
```

Remove each tagged block, then delete `desktop/join_workbench.py`. The 8 hook sites:

| File | What | Marker line |
|---|---|---|
| `genizah_app.py` | result-row 🔗 button | `joins_btn = self._create_action_button(... )  # JOINS-SKETCH` |
| `genizah_app.py` | Browse button (creation) | `# JOINS-SKETCH: anchor the currently-browsed fragment …` (block ⇒ `self.btn_b_find_joins`) |
| `genizah_app.py` | Browse button (layout) | `ext_info_row.addWidget(self.btn_b_find_joins)  # JOINS-SKETCH` |
| `genizah_app.py` | `open_joins_workbench(self, res)` method | `def open_joins_workbench(self, res):  # JOINS-SKETCH` |
| `genizah_app.py` | `_browse_open_join_workbench(self)` method | `def _browse_open_join_workbench(self):  # JOINS-SKETCH` |
| `desktop/result_dialog.py` | action-row button (creation) | `# JOINS-SKETCH: pin the currently-viewed fragment …` (block ⇒ `self.btn_rd_find_joins`) |
| `desktop/result_dialog.py` | action-row button (layout) | `action_row.addWidget(self.btn_rd_find_joins)  # JOINS-SKETCH` |
| `desktop/result_dialog.py` | `_open_join_workbench(self)` method | `def _open_join_workbench(self):  # JOINS-SKETCH` |

## Why this is low-risk regardless

`desktop/join_workbench.py` is imported **lazily inside `try/except`** at both entry points
(`open_joins_workbench`, and the ResultDialog/Browse buttons route through it). If the sketch
file is deleted but a hook is somehow left behind, the button shows
"Join Workbench unavailable: …" instead of crashing the app. Deleting the file alone already
neuters the feature; removing the hooks is just cleanup.

## All sketch surface (inventory)

- **NEW (untracked):** `desktop/join_workbench.py` — the entire workbench (anchor pane,
  search bar with Line START/END + responsa mode, thumbnail grid, responsa-style table view,
  CompareDialog, material/VS helpers). Delete = gone.
- **Tracked, additive hooks:** the 8 `JOINS-SKETCH` sites above.
- **Planning docs (safe to keep):** this folder — `CODEX-CRITIQUE.md`,
  `DESKTOP-INTEGRATION-NOTES.md`, `REVERT.md`.
