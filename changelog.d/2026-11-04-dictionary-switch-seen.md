## [Unreleased] -- The dictionaries switch on /browse says what it is (web, branch `claude/arabic-lane-cards`)

The owner found the click-a-word dictionaries too hidden: a grey `translate` icon in the version
row, named only in a hover tooltip a phone never shows. Option A of the discussion
(`docs/plans/BLAU_DICTIONARY_PLAN.md` §6.2.1); ruling (c) stands: the reader still turns the
dictionaries on, and nothing is guessed from the text or the catalogue.

- **The letters עع** (Hebrew ʿayin beside Arabic ʿayn, the owner's choice) replace the icon, drawn
  as outlines from Noto Sans Hebrew Bold and Noto Naskh Arabic Bold (SIL OFL), so they look the same
  on every device and need no Arabic font (`web/components/blau_panel.py::GLYPH_SVG`).
- **A label from 640px**, the owner's wording: "חדש! מילונים לערבית־יהודית!" / "New! Judeo-Arabic
  dictionaries!", highlighted in amber, until the visitor first turns the dictionaries on; then
  "מילונים לערבית־יהודית" / "Judeo-Arabic dictionaries" for good (`ui.dictionaries_seen`, cleared by
  `/reset-hints`). On a phone, the letters alone: amber with a dot while new. The label is also
  the switch's spoken name.
- **The state colours show.** The switch was a Quasar `primary` button, whose `text-primary` is
  `!important`, so its grey "off" rule never applied; it is created without a colour now.
- **PostHog**, beside `browse_manuscript`: `dictionary_switch` {on, was_new}, `dictionary_lookup`
  {how: word | box, blau, lane: found something}, `dictionary_tab` {tab}. The word is never sent.
