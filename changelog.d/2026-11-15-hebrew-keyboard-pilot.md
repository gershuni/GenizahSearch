## [Unreleased] -- On-screen Hebrew keyboard on the search box (pilot) (web, branch `claude/hebrew-keyboard-pilot`)

- A keyboard button inside the /search box opens Hebrew keys under it, for visitors with no Hebrew layout: the letters in alphabet order, each final form beside its letter, space, `*` (any letters), `'` (geresh, as the dictionary box reads it), `"` (abbreviation) and delete. A key types at the cursor; a mouse click leaves the cursor in the box, and a tap moves it to the keys so a phone's own keyboard closes (`web/hebrew_keyboard.py`, `web/static/js/hebrew_keyboard.js`).
- Pilot scope, after a design review: the main search box only, one layout, no `?` key (`?` and a space switch the mode), no typing with the physical keys.
- PostHog, with no characters and no query text: `hebrew_keyboard_opened` and `hebrew_keyboard_used` (once per search attempt); the page address PostHog adds to these two loses its query in `before_send` (`web/static/js/posthog_scrub.js`).
