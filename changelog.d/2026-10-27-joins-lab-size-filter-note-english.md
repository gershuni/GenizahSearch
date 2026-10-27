## [Unreleased] -- Joins Lab: the width-filter note says what turning the filter on does (desktop, branch `claude/sweet-noether-6fdtsi`)

### Fixes
- **Joins Lab "Filter ▾" dialog: the note under "Filter by width (cm)" now says what the filter does.** The
  English UI showed the literal text "Size filter note": the string was looked up by a placeholder key, and
  `tr()` returns the key itself in English. The Hebrew note ("סינון גודל הוא אופציונאלי — מידות מוצגות כרמז
  בלבד.") was unclear. Both now give the one fact a user needs before turning the filter on: "With this on,
  fragments with no recorded width are hidden too." / "כשהסינון פעיל, מוסתרים גם קטעים שאין להם רוחב רשום."
  The key is the English sentence, following the convention that keys are the English text.
