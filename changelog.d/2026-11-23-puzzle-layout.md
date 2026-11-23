## [Unreleased] -- The Fragment Puzzle: toolbars that keep their height, a canvas as wide as its room, a way back (web, branch `claude/web-puzzle-layout`)

- In a short window the Puzzle's toolbars shrank to 32 px and their wrapped lines lay over the next row (the sliders over the buttons). They keep their height now, as does the extension's notice; the canvas gives way, down to 240 px.
- The canvas kept the width it was drawn at when its room changed without the window (the side menu opened or closed, the notice dismissed), and could fill half of it. It follows its room now.
- A Back button first on the Puzzle's toolbar, to the page it was opened from (the Reading Desk, Browse, a list, ...): shown when the reader came from another page of the site.
- `web/pages/puzzle.py`; tests `tests/render_smoke/test_puzzle_layout_render_smoke.py`.
