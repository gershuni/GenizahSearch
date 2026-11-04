## P2 ops: A forced worktree removal walks junctions into the main checkout's data folders
Where: `.claude/worktrees/*` cleanup (`git worktree remove --force`, `rm -rf`); `scripts/init.ps1`, `scripts/check_worktree_links.ps1`
Opened: 2026-11-04

What happened. On 2026-11-03 a worktree session made directory junctions inside its worktree
(`cmd /c mklink /J nli_data C:\GenizahSearch\nli_data`, likewise `pgp_data`, `fist_data` and
`fgp_data`) so its dev server would find the gitignored sidecars a worktree does not carry. On
2026-11-04 at 00:11 a cleanup session working in the main checkout ran `git worktree remove --force`
and then `rm -rf` on that worktree. Git for Windows and MSYS `rm` walk a junction as a directory, so
the main checkout's `nli_data/` and `fgp_data/` were emptied (`pgp_data/` and `fist_data/`
survived; why is not established). Nothing reached the Recycle Bin, and the session reported only
that the worktrees were gone.

Lost: `nli_data/nli_crossref.db` (272,809,984 bytes) and all of `fgp_data/`: the served
`fgp_transcriptions.db` (v2.0.0, 59,062 rows), the kept June sidecar
`fgp_transcriptions.pre_9110.db`, `nuscha_hidden_layer.db`, `inventory/` (inventory.db,
pdf_text_cache.db, fgp_transcriptions.candidate.db, the glyph re-read rounds r1-r6) and
`metadata/`. Recovered the same day: the two served files, from the 2026-11-03 desktop build
output `dist/GenizahSearchPro/_internal/nli_data/` and `.../fgp_data/` (same sizes, quick_check
ok, row counts match; the server's copies were never touched, and it keeps the June file as
`fgp_transcriptions.db.prev-<stamp>`). Still lost: the FGP inventory candidates, the hidden layer
and the local June sidecar, which existed nowhere else; rebuilding them means re-running the FGP
inventory gates (`docs/plans/FGP_SOURCE_FOLDERS_ASSESSMENT.md`), an owner decision.

What the 2026-11-04 change adds, and what it does not. The rule in `CLAUDE.md` ("How to work
here": a worktree never links to the main checkout's data; copy a sidecar under
`%LOCALAPPDATA%\GenizahSearchPro\data\<dir>\` or set `GENIZAH_FJMS_DB_PATH`),
`scripts/check_worktree_links.ps1` (names every reparse point anywhere under a worktree, never
stepping into one, exit 1; a folder it cannot read makes the scan fail closed, exit 2) and `scripts/init.ps1` running it in every worktree and, from the main
checkout, over `.claude/worktrees/*`. Nothing BLOCKS `git worktree remove --force` or `rm -rf` on a
worktree that still holds a junction: the guard is advisory, and a session that skips `init.ps1`
sees no warning. Closing this row = a removal path that refuses while a reparse point exists
(a wrapper the cleanup step must use, or a hook), plus the local-only FGP artifacts rebuilt or
written off.
