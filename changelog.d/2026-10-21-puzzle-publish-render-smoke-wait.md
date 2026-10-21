## [Unreleased] -- Fragment Puzzle publish test waits for its dialogs (ci, branch `claude/puzzle-publish-render-smoke-wait`)

`tests/render_smoke/test_puzzle_export_render_smoke.py`: the Save and Publish
steps slept a fixed 0.3 s before clicking the button in the dialog they open.
Both dialogs are built only after the page's handler has been to a worker
thread, and on a slow Windows runner that took longer: the publish test failed
with `AssertionError: 0` (no Publish button yet) on PR #424, which changed no web
code. The test now waits, up to 15 s, for the dialog's button to exist, and for
the save to have recorded the document's id before it clicks Publish. Clicking
still requires exactly one such button. No product change.
