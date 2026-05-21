# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 D-20: delete Tantivy docs by uid (via local_pages sidecar).

Real implementation: shared/local_indexer.py (Wave 1, Plan 95-03).
"""


def test_delete_by_uid_with_raw_tokenizer():
    """D-20 Codex P1: Tantivy unique_id is a text field; deletion routes through
    local_pages sidecar to collect all page-level UIDs for a given sys_id,
    then issues writer.delete_documents(Term('unique_id', uid)) per UID.
    """
    raise NotImplementedError(
        "Wave 0 stub for D-20 delete-by-uid — implemented in Wave 1 plan 95-03"
    )
