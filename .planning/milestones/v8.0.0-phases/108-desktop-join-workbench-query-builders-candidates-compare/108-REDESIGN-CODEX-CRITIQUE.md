I could not complete the requested local code/sketch read: PowerShell and Node file reads both failed with `windows sandbox: spawn setup refresh`, and no MCP file resources were exposed. The review below is therefore based on the detailed code map, invariants, and approved decisions in your brief.

**Highest-Risk Issues**

The builder plan is parser-risky unless “remove” means “remove from visible UI, not necessarily remove state.” Keep `chk_opt_*` widgets/attrs or replace every `_responsa_opts` and `_merge_globals` dependency deliberately. If the global options dialog creates local checkboxes only when opened, searches can break before the dialog is ever shown.

For per-line options, do not bind the dialog directly to `row["mods"]` if the dialog has Cancel. Use a local copy, commit on OK, then call `_sync()` once. Also, wildcard-prefix-disabled-when-`>1` OR boxes must clear stale prefix state, not just disable the checkbox. Otherwise an older true value can still leak into `build_side_query`.

Removing `eventFilter` / `_on_row_focus` is probably correct conceptually, but risky in one pass. Safer: first stop depending on it visually, keep the methods/attrs harmless, and remove only after verifying parser tests and construction. It may still be touched by tests or by preview synchronization.

The typed-sign behavior is under-specified. If word boxes already pass raw text through, the change may only need tooltip/legend text. If code currently strips or normalizes signs, adding parsing could break RR-1/RR-13. Define precedence between typed signs and gear options, especially for negation and wildcard prefix with multiple OR boxes.

The table checkbox column will shift existing table column indices. `_table_double_clicked`, result mapping, action buttons, and any hidden `UserRole` data must be adjusted. Double-clicking the checkbox column should not open compare.

The master checkbox is not trivial in `QTableWidget`. A real header checkbox usually needs a custom `QHeaderView` or a clear toolbar-level select-all control. If the approved sketch insists on a header checkbox, plan that explicitly.

**Candidate Selection**

Use a transient `selected_keys: set[...]` distinct from triage. Triage remains persisted/sys_id-keyed; selection is UI-only and should clear on new search/anchor change.

For grid/table/pagination, the clean pattern is:

- one `_candidate_key(c)` helper
- checkbox init reads `key in selected_keys`
- render code blocks checkbox signals while setting initial state
- checkbox changes call `_set_selected(key, checked)`
- bulk bar state updates from `selected_keys`
- table items store the key/candidate in `Qt.UserRole`

Decide whether filtering prunes selection. I would prune selection to the current filtered/result universe to avoid bulk actions on hidden rows.

**Puzzle Action**

Varargs on `open_anchor_in_puzzle(sys_id, *extra_sys_ids)` is backward-compatible, but a new public host method like `open_anchors_in_puzzle(sys_ids)` is cleaner semantically. Either is fine if workbench calls only public host methods.

Anchor-first ordering is correct. Also dedupe before calling the host if anchor and candidate can ever overlap. Do not put `_vs_` even in workbench comments or strings if the no-private test is a simple scan.

**Compare / Browse Results**

No conflict: keep all three entry points.

- `Browse results ▶`: opens `CompareDialog` over `filtered`, starting at the single selected item if exactly one, otherwise first result.
- card `⇄`: opens compare at that card.
- table double-click: opens compare at that row, except checkbox column.

Bulk 📖 browse is a different action and should remain enabled only for exactly one selected item.

**i18n / Qt Pitfalls**

Every new tooltip, context menu item, dialog title, filter label, shortcut, and relabeled other-side text needs a `tr()` key registered in translations. Avoid dynamic `tr(f"...")` keys.

New dialogs should be parented and should not set dialog-level RTL. Keep chrome LTR; only content text/layout should follow existing local pattern.

Construction order is a real risk here: initialize selection sets, persistent option widgets/dialogs, filter controls, and `_preview_edit` before any signal or sync path can reference them.

**Safest Sequencing**

1. Builder first, parser-neutral: hide/move controls but keep `build_side_query`, `_responsa_opts`, row defaults, and existing attrs stable. Run builder/i18n tests.
2. Add per-row options dialog using copy-on-OK, then consider removing active-row machinery only after tests.
3. Candidate pane cleanup: remove tags/self-match/include-anchor UI, hardcode self-exclusion, relabel other-side.
4. Move filters into a persistent dialog while preserving the existing `apply_filters` data path.
5. Add selection set, grid checkboxes, table checkbox column, and bulk bar together.
6. Wire compare toolbar and puzzle anchor+candidate behavior.
7. Final pass for i18n, no-private scan, and construction smoke.

I would split typed-sign parsing and table master-header polish if schedule is tight; both are easy places to break otherwise stable behavior.
