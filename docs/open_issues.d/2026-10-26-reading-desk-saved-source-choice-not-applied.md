## P3 web: The reading desk saves a source choice it never applies, keyed by list position
Where: `web/pages/browse.py` (reading-desk version `ui.select`, `state.reading_desk_selected_sources`), `web/pages/browse_state.py`
Opened: 2026-10-26

The reading desk's per-fragment version dropdown stores the chosen option under
`state.reading_desk_selected_sources[sys_id]` and the browse snapshot persists it as `selected_sources`, but
the dropdown is always built with `value=default_key` (first PGP edition, else first translation, else V0.8):
the saved value is restored into state on `enter_joined_view` and never read again, so a choice does not
survive a page rebuild or a session. The key is also the option's list position (`src_<i>`), which changes
whenever the fragment's PGP or FGP source list changes (a sidecar refresh adds or removes rows; gate 4,
`docs/plans/FGP_CHOOSER_VALIDATION_RULES.md` §5, measured 282 manuscripts whose FGP list order changes with the
larger sidecar). Found while validating the chooser against the larger FGP sidecar (gate 4 of
`docs/plans/FGP_SOURCE_FOLDERS_ASSESSMENT.md`), whose check C3 keeps `fgp:<id>` stable for exactly this reason.
Fix sketch: key the option by the source's stable id (`fgp:<id>` / `pgp:<id>`, `v08`), pass the saved key as the
select's initial value when it is still among the options, and fall back to the default otherwise.
