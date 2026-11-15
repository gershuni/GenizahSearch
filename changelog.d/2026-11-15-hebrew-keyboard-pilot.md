## [Unreleased] -- On-screen Hebrew keyboard on the search box (pilot) (web, branch `ccr-bf29ace5-33digb`)

- A keyboard button inside the /search box opens Hebrew keys under it, for visitors with no Hebrew layout: the letters in alphabet order, each final form beside its letter, space, `*` (any letters), `'` (geresh, as the dictionary box reads it), `"` (abbreviation) and delete. A key types at the cursor; a mouse click leaves the cursor in the box, and a tap moves it to the keys so a phone's own keyboard closes (`web/hebrew_keyboard.py`, `web/static/js/hebrew_keyboard.js`).
- Pilot scope, after a design review: the main search box only, one layout, no `?` key (`?` and a space switch the mode), no typing with the physical keys.
- PostHog, with no characters: `hebrew_keyboard_opened`, `hebrew_keyboard_used` (once per search attempt) and `hebrew_keyboard` on `search_executed`.
