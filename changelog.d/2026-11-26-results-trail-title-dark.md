## [Unreleased] -- Browse's results list: the title's beginning, and the current row in the dark theme (web, branch `fix/results-trail-title-dark`)

- `web/components/results_trail.py`: each row of the bar's list shows the beginning of the result's
  title (up to 40 characters, `short_title`) between the shelfmark and the matched words.
- The current row was a light green under light text in the dark theme; there it takes the theme's
  own selected-row colour (`--bg-active`), and in both themes it is marked by a bar at its start.
- Opening the list scrolls only the list to the current result, so "Back to the search results"
  stays in view; the list no longer overflows the menu sideways on a narrow window.
