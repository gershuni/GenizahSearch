## [Unreleased] -- A library is recognised by the link's host name (web + desktop, branch `fix/url-host-checks`)

- `shared/url_hosts.py::is_on_host`: whether a link is Cambridge's, Oxford's, Manchester's or
  Princeton's is decided by its host name, no longer by the name appearing anywhere in the link.
  Used by the desktop viewer and library button, `genizah_app.py`, `shared/metadata_manager.py`,
  `shared/cudl_identity.py` and `web/pages/browse_enrichment.py`.
- `tests/test_url_hosts.py` fails on any new text test of a library host outside `tests/`.
