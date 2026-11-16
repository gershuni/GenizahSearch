## [Unreleased] -- PGP editions of both sides shown in the version chooser (web + desktop, branch `claude/pgp-recto-and-verso-editions`)

A PGP edition linked to a manuscript as "recto and verso" never appeared in the version chooser:
the page filter kept a PGP source only when its side was the shown page's ('recto' on page 1,
'verso' on any other) or was empty. It now shows on every page, its text narrowed to the page by
its Recto/Verso headings or per-image sections, as an edition with no side already was; where
its sections skip a page, its headings decide that page. A text that cannot be split for a page
-- no heading and no section for it -- is shown whole on both pages (owner, 2026-11-16). Measured on the local `pgp.db`: 982 edition texts carry the label;
688 manuscripts had no PGP edition in the chooser because all of theirs do; 49 texts on 42
manuscripts are the whole-text case. With PGP first, these editions become the default text on
those pages.

The desktop PGP source worker (`desktop/gui_threads.py`) kept its own copy of the rule; it now
calls `shared/fgp_service.py::filter_sources_for_page`, with `translations_every_page=True` so a
PGP translation still shows on every page in the desktop as before. Pinned by
`tests/test_pgp_recto_and_verso.py` and `tests/test_pgp_recto_and_verso_desktop.py`.
