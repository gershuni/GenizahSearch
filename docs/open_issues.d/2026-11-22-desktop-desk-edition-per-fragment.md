## P2 desktop: The desktop Reading Desk shows a document's edition under every fragment, sometimes another document's
Where: `genizah_app.py::_browse_rd_render_text`, `desktop/gui_threads.py::ReadingDeskWorker`
Opened: 2026-11-22

The desktop desk asks for sources fragment by fragment (`get_all_sources_for_fragment`, which
returns the sources of every PGP document a fragment belongs to) and shows, under each fragment,
the first edition with text among them. Replayed on the local `pgp.db` (2026-11-22): a desk of
PGP 712 (ENA 2740.3 + ENA 2805.2) shows edition 558 under both fragments; a desk of PGP 1702
(T-S 13J16.11, T-S 13J22.21, T-S 8J22.14) shows edition 5171 under all three, and 5171 belongs to
PGP 1246. The website fixed the same fault in v10.1.0 with its document mode
(`web/pages/browse_desk_document.py`: the document's own edition once, Edition / Translation /
By fragment), measured there on 548 documents shown repeated and 128 opening on another
document's edition.

Fix sketch: move `document_desk` and its rules from `web/pages/browse_desk_document.py` to
`shared/`, and give the desktop desk the same text panel (a source selector over the document's
own editions, translations apart, "By fragment" for each fragment's own transcriptions). The
image views of GS-294 (one image at a time, puzzle images) are in; this is the text half.
