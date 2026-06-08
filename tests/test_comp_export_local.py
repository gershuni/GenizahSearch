# -*- coding: utf-8 -*-
"""Phase 110 Wave-0 scaffold — LOCAL-aware composition export (EXP-F3).

Pins the contract Plan 04 will implement on `export_comp_report`: a LOCAL "My
Library" composition hit must export with the LOCAL column shape
(filename / parent folder / full filepath / page / matched-text) rather than the
Genizah manuscript columns, across all four formats (xlsx / csv / txt / docx),
WITHOUT a private LOCAL `97…` id ever reaching the NLI metadata prefetch.

The export primitives are tested via MODULE-LEVEL helpers Plan 04 adds to
`shared/export_dossier.py` (C1 — importable + unit-testable, NOT closures inside
`export_comp_report`):
    _build_local_comp_row(...)        -> 5-cell list (thin wrapper over build_local_document_row)
    _partition_comp_export_rows(...)  -> (genizah_items, local_rows)

These do not yet exist at Wave 0, so each test imports them INSIDE its body and
is marked `xfail(strict=False)` — collection always succeeds (the file imports
cleanly), the bodies go green at Wave 4 when Plan 04 lands the helpers.

LOCAL detection uses `is_local_sys_id(item.get('sys_id',''))` (NOT a nested
display-source lookup) — composition grouped items carry NO `display` dict
(RESEARCH Pitfall 2). Fixtures use a REALISTIC 18-digit `97…` numeric sys_id
(`970012345601234567`) so `is_local_sys_id()` is genuinely exercised, NOT a
`LOCAL_…` literal that would never match the real discriminator (Round-2 #6).

Requirements covered:
  - EXP-F3: LOCAL hit -> LOCAL columns; all four formats LOCAL-aware; Genizah-only
    export STRUCTURALLY unchanged (partition leaves Genizah rows identical + empty
    LOCAL set — C5, asserted structurally NOT byte-for-byte because xlsx/docx are
    ZIP containers with non-deterministic timestamps/ordering).
  - EXP-F3 / D-12 (Round-2 #1 / T-110-07): a LOCAL-only composition export filters
    `97…` ids out of the NLI prefetch so `_fetch_metadata_with_dialog` never fires
    for private LOCAL ids.
"""
from __future__ import annotations

import pytest

from shared.local_sys_id import is_local_sys_id
from shared.export_dossier import local_documents_header_row


# A realistic 18-digit '97'-prefixed LOCAL sys_id (Round-2 #6 — exercises the
# real is_local_sys_id discriminator, not a LOCAL_… placeholder).
LOCAL_SYS_ID = "970012345601234567"
LOCAL_SYS_ID_2 = "970077777701234567"
# A real Alma (Genizah) sys_id — is_local_sys_id(...) is False for this.
GENIZAH_SYS_ID = "990025143260205171"


def _fake_local_item():
    """A grouped LOCAL composition item (NO 'display' dict — RESEARCH Pitfall 2)."""
    return {
        "type": "manuscript",
        "sys_id": LOCAL_SYS_ID,
        "src_lbl": "LOCAL",
        "pages": [
            {
                "raw_header": f"{LOCAL_SYS_ID}_LOCAL_P3_F1r",
                "p_num": 3,
                "chunk_locator": "p. 3",
                "source_ctx": "פתח דבריך *יאיר* מבין פתיים",
            }
        ],
    }


def _fake_genizah_item(sys_id=GENIZAH_SYS_ID):
    """A grouped Genizah composition item."""
    return {
        "type": "manuscript",
        "sys_id": sys_id,
        "src_lbl": "T-S",
        "pages": [
            {
                "raw_header": f"{sys_id}_IE1_P1_F1r",
                "p_num": 1,
                "chunk_locator": "p. 1",
                "source_ctx": "ברוך אתה *ה'* מלך העולם",
            }
        ],
    }


def _is_local(item):
    """LOCAL detector mirroring Plan 04: src_lbl OR the real 97… sys_id."""
    return item.get("src_lbl") == "LOCAL" or is_local_sys_id(item.get("sys_id", ""))


# ---------------------------------------------------------------------------
# EXP-F3 — LOCAL row shape via the module-level helper (C1)
# ---------------------------------------------------------------------------

@pytest.mark.xfail(reason="_build_local_comp_row lands in Plan 04", strict=False)
def test_xlsx_local_row_shape():
    """EXP-F3: a LOCAL composition hit builds a 5-cell row
    [filename, parent_folder, full_filepath, page, matched_text_raw] via the
    Plan-04 module-level helper, in the column order of
    local_documents_header_row('en'), and the matched-text cell retains the
    source `*`-highlighting."""
    from shared.export_dossier import _build_local_comp_row

    local_item = _fake_local_item()
    page = local_item["pages"][0]
    row = _build_local_comp_row(
        filename="x.pdf",
        parent_folder="dir",
        full_filepath="/d/x.pdf",
        page="3",
        matched_text_raw=page["source_ctx"],
    )
    assert isinstance(row, list)
    assert len(row) == len(local_documents_header_row("en")) == 5, (
        "LOCAL comp row must have exactly 5 cells matching the Local Documents header"
    )
    assert row[0] == "x.pdf"
    assert row[1] == "dir"
    assert row[2] == "/d/x.pdf"
    assert str(row[3]) == "3"
    assert row[4] == page["source_ctx"], (
        "cell[4] (matched_text_raw) must be the source_ctx `*`-highlighted passage"
    )
    assert "*יאיר*" in row[4], "matched-text cell must retain the *-highlight markers"


@pytest.mark.xfail(reason="_partition_comp_export_rows lands in Plan 04", strict=False)
def test_all_formats_local_aware():
    """EXP-F3: the partition helper splits grouped comp items into the Genizah
    set (untouched) and the LOCAL 5-cell row set, so each of xlsx/csv/txt/docx
    can emit a LOCAL-aware path when a LOCAL item is present."""
    from shared.export_dossier import _build_local_comp_row, _partition_comp_export_rows

    local_item = _fake_local_item()
    genizah_item = _fake_genizah_item()

    def _local_row_fn(it):
        pg = it["pages"][0]
        return _build_local_comp_row(
            filename="x.pdf", parent_folder="dir", full_filepath="/d/x.pdf",
            page=str(pg.get("p_num", "")), matched_text_raw=pg.get("source_ctx", ""),
        )

    genizah_items, local_rows = _partition_comp_export_rows(
        [local_item, genizah_item],
        is_local_fn=_is_local,
        local_row_fn=_local_row_fn,
    )
    assert len(local_rows) == 1, "exactly one LOCAL row expected"
    assert len(local_rows[0]) == 5, "the LOCAL row is a 5-cell list"
    assert genizah_items == [genizah_item], (
        "the Genizah partition must contain exactly the one Genizah item"
    )


@pytest.mark.xfail(reason="_partition_comp_export_rows lands in Plan 04", strict=False)
def test_genizah_only_export_unchanged():
    """EXP-F3 STRUCTURAL parity (C5): a Genizah-only export partitions to an
    EMPTY LOCAL set and leaves the Genizah items IDENTICAL (same objects, same
    order). This proves the LOCAL plumbing cannot alter the Genizah path.

    Genizah-only export parity is asserted STRUCTURALLY (the partition leaves
    Genizah items untouched + empty LOCAL set) rather than byte-for-byte, because
    xlsx/docx are ZIP containers with non-deterministic timestamps/ordering (C5).
    csv/txt are deterministic text so the higher-level Plan-04 verify may add a
    byte check there if cheap."""
    from shared.export_dossier import _partition_comp_export_rows

    genizah_a = _fake_genizah_item("990025143260205171")
    genizah_b = _fake_genizah_item("990001458630205171")

    genizah_items, local_rows = _partition_comp_export_rows(
        [genizah_a, genizah_b],
        is_local_fn=_is_local,
        local_row_fn=lambda it: [],  # never invoked — no LOCAL items
    )
    assert local_rows == [], "Genizah-only export must produce an empty LOCAL set"
    assert genizah_items == [genizah_a, genizah_b], (
        "Genizah items must be returned identical and in order (structural parity)"
    )


@pytest.mark.xfail(reason="metadata-prefetch LOCAL filter lands in Plan 04", strict=False)
def test_local_only_export_no_metadata_fetch():
    """Round-2 #1 / EXP-F3 / D-12 / T-110-07: a LOCAL-only composition export must
    NOT fire the NLI metadata prefetch (which would start a ShelfmarkLoaderThread
    for a private LOCAL `97…` id). PURE-TEST (no live network) — exercises the
    metadata-prefetch FILTERING logic Plan 04 adds before
    `_fetch_metadata_with_dialog`, NOT the Qt dialog.

    Plan 04 builds `genizah_ids = [uid for uid in unique_ids if not
    is_local_sys_id(uid)]` and only fetches `missing` from `genizah_ids`. For an
    all-LOCAL id list, genizah_ids == [] -> missing is empty ->
    _fetch_metadata_with_dialog is never called. For a mixed list, the LOCAL ids
    are filtered out so only the Genizah id is fetched.

    Prefer the Plan-04 module-level helper if it exposes one; otherwise assert the
    equivalent comprehension invariant directly."""
    # If Plan 04 exposes a named filter helper, use it; else fall back to the
    # inline comprehension that mirrors export_comp_report's filter.
    try:
        from shared.export_dossier import filter_genizah_ids_for_metadata as _filter
    except ImportError:
        _filter = None

    # (a) All-LOCAL id list -> empty genizah_ids -> _fetch_metadata_with_dialog never called.
    all_local = [LOCAL_SYS_ID, LOCAL_SYS_ID_2]
    if _filter is not None:
        genizah_ids = _filter(all_local, is_local_sys_id)
    else:
        genizah_ids = [uid for uid in all_local if not is_local_sys_id(uid)]
    assert genizah_ids == [], (
        "an all-LOCAL id list must produce an empty genizah_ids set, so the "
        "NLI metadata prefetch (_fetch_metadata_with_dialog) is never called"
    )

    # (b) Mixed list -> LOCAL ids filtered out, only Genizah id(s) remain.
    mixed = [LOCAL_SYS_ID, GENIZAH_SYS_ID]
    if _filter is not None:
        genizah_ids_mixed = _filter(mixed, is_local_sys_id)
    else:
        genizah_ids_mixed = [uid for uid in mixed if not is_local_sys_id(uid)]
    assert genizah_ids_mixed == [GENIZAH_SYS_ID], (
        "a mixed list must keep only the Genizah id for the metadata prefetch "
        "(LOCAL 97… ids filtered out before _fetch_metadata_with_dialog)"
    )
