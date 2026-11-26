## [Unreleased] -- PGP tag results: the fragments of a join are labelled (web + desktop, branch `fix/pgp-tag-join-labels`)

A PGP tag search lists one row per fragment, so the fragments of one joined
document all showed the same description or transcription and looked like
duplicates. The rows of a document now come together, in fragment order, and each
says which document and fragment it is ("PGP 7403 · 1/4"): a small label on the
web card, at the start of the desktop row's text. Single-fragment documents are
unchanged. `shared/document_service.py::get_fragments_by_tag` orders by document
and fragment and adds `fragment_index` / `fragment_count`; `pgp_join_label` builds
the label for both apps.

The web tag results also look up the Hebrew description by the row's own PGP
document, not by fragment: a fragment in two documents could show the other
document's description.
