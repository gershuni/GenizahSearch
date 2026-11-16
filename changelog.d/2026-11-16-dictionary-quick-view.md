## [Unreleased] -- Dictionaries in Quick View (web, branch `claude/dictionary-quick-view`)

The dictionary lookup (Blau, Friedman, Lane) works in the search page's Quick View as on /browse:
click a word of the transcription, select words, press D or M, or on a phone tap "Look up" under
selected words; the same switch beside the version chooser, and the same panel. Fullscreen Quick
View takes the lookup too. Moving to another result or page, changing the text version, or closing
Quick View closes the panel. Esc closes the panel first, and Quick View on the next press.

The page side moved out of `web/pages/browse.py` into `web/components/dictionary_lookup.py`, which
both pages use; /browse is unchanged. `web/static/js/blau_lookup.js` now stands down only for a menu
or a dialog that does not hold the panel, and inside Quick View keeps the phone chip beside the panel
and uses up the Esc that closes it. No sidecar or setting changes: Quick View offers the lookup
wherever /browse does.
