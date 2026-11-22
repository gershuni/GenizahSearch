## P3 community: The discovery dialogs never offer the image of a linked document
Where: `web/pages/discoveries.py::create_new_discovery_dialog`, `open_edit_discovery_dialog` (`select_document`)
Opened: 2026-11-20

Both dialogs build their "Image number" selector from `state.meta_mgr.get_browse_page(sys_id,
p_num=1)`, but `state.meta_mgr` is a `MetadataManager` (`web/main.py`, `state.meta_mgr =
MetadataManager(...)`), which has no `get_browse_page` (`shared/metadata_manager.py`; the method is
on `web/services.py` and `shared/search_engine.py`). The `AttributeError` is swallowed by the
surrounding `except Exception: pass`, so no image can be chosen and a new discovery's
`page_number` is never set from the dialog. Found by Codex's pre-flight of picker PR 7 (web) and
confirmed by grep. Fix sketch: ask the page count through the web service's `get_browse_page` off
the event loop (it is blocking), then build the selector.

## P3 community: The feed reads a discovery's document and additional shelfmarks from keys the web never writes
Where: `web/supabase_client.py::create_discovery`, `get_community_feed`
Opened: 2026-11-20
Status: ⏳ Needs a check of the live `discoveries` table

`create_discovery` stores the linked document, its page and the additional shelfmarks as
dictionaries in the `shelfmarks` JSONB array (the only such column in `supabase_setup.sql`), while
`get_community_feed` reads top-level `document_id`, `shelfmark`, `page_number` and
`additional_shelfmarks` and never unpacks `shelfmarks`; the edit dialog writes the top-level
columns through `update_discovery`. Unless the live table has those columns filled by a trigger
(not in any tracked SQL), a discovery shared on the web shows no linked document and no additional
shelfmarks in the feed. Not checked against the live database. Fix sketch: in the feed, fall back
to `shelfmarks[0]` and `shelfmarks[1:]` when the top-level keys are empty.
