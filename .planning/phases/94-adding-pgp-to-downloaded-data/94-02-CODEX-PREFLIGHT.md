**Q1 — PROBLEM**

Item-level sys_id rehydration is OK, but `domains` still breaks. `export_json` passes compacted session rows into `serialize_search_payload` (`web/api.py:2240`), and `_serialize_item` can recover `final_sys_id` from top-level `result['sys_id']` via the SEED-002 block (`shared/search_serializer.py:272`, `:274`, `:306`, `:312`) when `meta_mgr` is present. The break is earlier: `serialize_search_payload` builds `sys_ids` only from `result['display']['id']` (`shared/search_serializer.py:450`), so compacted rows with no `display` produce an empty `domain_batch`; `domain_batch.get(final_sys_id)` then returns nothing (`shared/search_serializer.py:329`).  
Fix: make the `serialize_search_payload` batch sys_id builder use the same fallback chain as `_serialize_item`, including top-level `sys_id`.

**Q2 — PROBLEM**

`domain_batch` is built from `[(r.get('display') or {}).get('id', '') ...]` only (`shared/search_serializer.py:450`-`:454`). It does not inspect top-level `sys_id`, `raw_header`, or `uid`, even though `_compact_search_result_row` synthesizes `sys_id` before dropping `display` (`web/export_state.py:196`, `:212`, `:218`). So the bilateral concern is real specifically at the batch-build step.  
Fix: add a small shared resolver used by both `serialize_search_payload` batch collection and `_serialize_item`.

**Q3 — PROBLEM**

The only history-restore branch that also calls `set_search_export` is `web/pages/search.py:3902`-`:3911`; live code has no new kwargs yet, but Plan 94-02 instructs adding empty `transcription_sys_ids=set()`, `printed_ids=set()`, and `result_domains={}` there (`94-02-PLAN.md:706`). At restore time the code restores only `results`, `domain_exclusions`, and `printed_filter` (`web/pages/search.py:3881`-`:3885`), not the three enrichment sets; current history entries also do not store result rows (`web/pages/search.py:4478`-`:4481`, `web/pages/search_state.py:534`-`:535`). Session restore similarly restores results without these enrichment fields (`web/pages/search_state.py:378`-`:390`) and only recomputes PGP later, not printed/domains or export payload (`web/pages/search.py:4830`-`:4867`).  
Fix: mark restored exports as metadata-incomplete, e.g. add an explicit warning and document that restored-result exports are outside the complete EXPORT-META guarantee unless recomputed.

**Q4 — PROBLEM**

The plan intends Wave 2 to read Excel enrichment fields but not pass them to `export_search_results_excel` until Wave 3. Task 4 explicitly chooses deferral (`94-02-PLAN.md:1181`-`:1186`) and shows the Wave-2 implementation as local reads plus a `TODO(Wave 3)` (`94-02-PLAN.md:1203`-`:1210`, `:1260`). But `must_haves` still says `export_excel` “reads ... and passes them as kwargs” while also saying activation waits for Wave 3 (`94-02-PLAN.md:28`), and the overview repeats “reads + passes” (`94-02-PLAN.md:86`).  
Fix: change those lines to “reads into local variables only; Wave 3 passes the kwargs.”

**Q5 — PROBLEM**

With the planned Stage-1 update call, an export clicked after Stage 1 but before Stage 2 gets the partial visible-page metadata, not nothing. Live code shows Stage 1 enriches only `all_sys_ids[:PAGE_SIZE]` (`web/pages/search.py:4605`-`:4608`) and assigns `transcription_sys_ids` / `printed_ids` from that batch (`web/pages/search.py:4620`-`:4622`); Stage 2 later processes `all_sys_ids[PAGE_SIZE:]` (`web/pages/search.py:4642`-`:4645`) and unions the remaining ids (`web/pages/search.py:4662`-`:4664`). Since the planned booleans are always `False` when a sys_id is absent from the set (`94-02-PLAN.md:887`-`:890`), rows not yet enriched can export false negatives for `has_pgp` / `is_printed`.  
Fix: either update export enrichment only after full Stage 2 completes, or mark the payload as metadata-incomplete until Stage 2 finishes/recompute on export.

**Q6 — OK**

Yes, `serialize_parallels_payload` routes through `_to_parallels_envelope_item` (`shared/search_serializer.py:907`-`:918`), and `_to_parallels_envelope_item` calls `_serialize_item` directly (`shared/search_serializer.py:805`-`:810`). So unconditional additions in `_serialize_item` would leak into parallels unless stripped. The plan handles this correctly: it explicitly requires `_to_parallels_envelope_item` to remove `has_pgp` and `is_printed` after the shared serializer call (`94-02-PLAN.md:944`-`:960`) and adds the negative regression test (`94-02-PLAN.md:1046`-`:1080`).

**Q7 — PROBLEM**

One additional HIGH trap: `/api/search` also calls `serialize_search_payload` (`web/search_api.py:940`-`:950`), and the serializer module states that changes to `_serialize_item` affect both downloads and `/api/search` in lockstep (`shared/search_serializer.py:11`-`:13`). If Wave 2 adds always-present booleans with default-false behavior (`94-02-PLAN.md:847`-`:855`) but only `web/api.py:export_json` passes the sets, stateless `/api/search` will emit false `has_pgp` / `is_printed` for every item. The monkeypatch/testability pieces look OK: `_serialize_item` accepts `domain_batch` directly (`shared/search_serializer.py:232`-`:238`) and `_safe_library_name` is a module-global call (`shared/search_serializer.py:325`); the planned enrichment helper preserves the `isinstance` guard and copy-on-update pattern (`94-02-PLAN.md:505`-`:515`).  
Fix: either wire real enrichment sets into `web/search_api.py` before serialization, or make the new flags opt-in for export JSON only.
