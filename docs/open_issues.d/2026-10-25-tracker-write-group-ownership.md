## P3 build-ci: The tracker scripts' atomic write does not keep a POSIX file's group
Where: `scripts/open_issues_model.py::write_text`
Opened: 2026-10-25

`write_text` writes a temporary file (`tempfile.mkstemp`) and renames it over the target. Since
PR #449 (Codex round 9) it copies the target's mode, but not its group: on a POSIX checkout shared
through a group other than the invoking user's primary group, a group-owned `0664`
`OPEN_ISSUES.md`, `BACKLOG.md` or `ISSUE_DETAILS.md` comes back owned by the user's default
group after `collect_open_issues.py --apply` or `archive_closed_issues.py --apply`, and the other
members can no longer update it. No such checkout exists today: the scripts run on the owner's
Windows machine and in CI. Fix sketch: `os.chown(tmp, -1, os.stat(path).st_gid)` before the
rename, raising rather than writing when that is not permitted. Found by the Codex review of
PR #449 (round 10) and logged instead of fixed under that PR's review budget.
