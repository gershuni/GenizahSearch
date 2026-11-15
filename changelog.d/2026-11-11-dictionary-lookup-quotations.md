## [Unreleased] -- Dictionaries: a word is also found inside the quotations (web, branch `dictionary-lookup-quotations`)

A one-word lookup in Blau's and Friedman's dictionaries now also reads the quotations and the
longer headwords. When an entry quotes the word, the panel lists that entry under "Also in the
examples under", with the quotation and the word marked. Before, a word that was no headword was
answered with "Nothing in the dictionary for this word" even when the dictionary printed it.

- `shared/blau_service.py`, `shared/friedman_service.py`: `BlauLookup.quotes` and
  `FriedmanLookup.quotes`, read from each sidecar's `tokens` table (`_quotes`). The entries that
  print the exact word most often come first. Five entries are shown and the rest counted
  (`MAX_QUOTE_HITS`), with two places each (`MAX_QUOTE_PLACES`).
- Not listed: the entries already shown as cards, a function word (פי), a word held by more than
  25 entries (`MAX_QUOTE_UNITS`), and several words (those are the expression search).
- A Blau 1.0.0 sidecar has no `tokens` table and is served as before, without this list.
- `web/components/blau_panel.py`, `web/components/friedman_panel.py`: the list uses the existing
  label and the expression search's rendering. No new public string.

This is step A of the dictionary lookup plan. It does not yet lead from an inflected verb to its
root: a form is found only where a quotation prints that same form.
