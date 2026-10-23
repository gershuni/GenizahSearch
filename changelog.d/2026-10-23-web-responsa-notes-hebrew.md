## [Unreleased] -- Responsa notes with numbers in the visitor's language (web, branch `master-main`)

- A Hebrew results line no longer ends in English. The engine writes its notes in English on
  the website (the API and MCP want them so), and the page looked each note up as it was: a
  note whose numbers were already in it ("a query with * reached 5,252 manuscripts, more than
  the 2,000 ...") never matched its key, and "Matches across a page break could not be
  searched; the list may be incomplete." was split at its own "; ". The page now recognises
  the engine's numbered messages (`shared/search_engine.py::FILLED_MESSAGE_TEMPLATES`: the two
  page-break notes and the Responsa guard's refusal) and writes them in the visitor's language
  with the same numbers (`web/translations.py::tr_engine_message`), in the results line and in
  the refusal message. The desktop translated these before filling them in and is unchanged.
- Pinned by `tests/test_engine_message_translation.py`, which also fails a new numbered
  message that is not registered.
