# -*- coding: utf-8 -*-
"""The audience boundary and the extended readiness contract for
``web/discovery_assets.py`` (Phase 136, plan 136-20).

**How this closes F-05's disposition.** VIS-01 requires that the exclusion of
private material be STRUCTURAL -- "private rows are ABSENT from the public
artifact, never merely hidden by a UI filter or a query predicate". Plan 136-08
delivers half of that: the public projection rebuilds a separate artifact from
which private rows are simply missing. But the runtime resolves ONE
manifest-named database and has, until this plan, no notion of what that
database is ALLOWED to contain -- so the last deployment step could still point
a public route at the private artifact and defeat every earlier control. The
audience gate closes that half: the private artifact is UNLOADABLE by a public
loader, not merely unselected. The two together are what "structural, never
merely hidden" means once deployment is included.

Two independent gates, deliberately:

  * ``_resolve_versioned_db()`` still selects EXACTLY the manifest-named file
    and still ignores siblings (the rollback-safety property, T-134-rollback).
    This plan does not touch it.
  * The readiness path now additionally checks the CONTENT of whatever the
    manifest selected -- its ``meta.audience``, its required tables, its
    required COLUMNS and its release-contract row counts.

**Read paths that do not exist yet.** At this plan's execution time
``web/discovery.py`` exposes the page-claims path, the work/manuscript scope,
the related-page path, the evidence expansion, and the band-precision readers.
The related-page COUNT wrapper and the corpus-wide findings reader are added by
later Phase-136 plans and so cannot be named here. Rather than freeze a list
that would go stale, the refusal test SWEEPS every public async reader in
``web/discovery.py`` dynamically and REFUSES to skip one whose required
parameter it does not recognise -- so a later plan adding a reader is forced to
register it rather than quietly escaping this proof. Plan 136-19's sweep carries
the follow-up assertion for the two named-but-absent paths.

Masking discipline (D-25): every value in this module is fabricated/synthetic;
no restricted corpus is named anywhere (they appear only as "M-source" /
"R-source" repo-wide), and the refusal-log test asserts positively that no cell
value from the refused artifact reaches the log.
"""
from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import logging
import sqlite3

import pytest

import web.discovery_assets as da
from tests.fixtures.discovery_v2_fixture import (
    GOLDEN_BASENAME,
    GOLDEN_DB,
    materialize_pre_rebuild_sidecar,
    materialize_sidecar,
    write_manifest,
)

# A distinctive synthetic marker seeded into the refused artifact's own rows,
# so the refusal-log test can assert positively that NO cell value escaped into
# the log line. Fabricated -- it corresponds to nothing in any corpus.
_CELL_MARKER = "SYNTHETIC-CELL-MARKER-7QX42"
_AUDIENCE_MARKER = "SYNTHETIC-AUDIENCE-MARKER-9Q7"


@pytest.fixture(autouse=True)
def _restore_state():
    """Every test ends with a bare load_discovery_state() restore call so
    module state never leaks across tests (the atlas/discovery test idiom).

    Module IDENTITY needs no restore here: `_reimport_web_discovery` reloads
    in place rather than popping `sys.modules`, so only one `web.discovery`
    object ever exists. See that helper for why the difference matters.
    """
    yield
    da.load_discovery_state()


def _seed_cell_marker(db_path) -> None:
    """Write the synthetic marker into an ordinary content cell of the sidecar
    so the log assertion has something concrete to look for."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("UPDATE works SET neutral_title = ?", (_CELL_MARKER,))
        conn.commit()
    finally:
        conn.close()


def _point_loader_at(monkeypatch, directory) -> None:
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(directory))


# ===========================================================================
# Task 1 -- the audience boundary
# ===========================================================================


def test_public_audience_asset_loads_and_readiness_proceeds(tmp_path, monkeypatch):
    """Behavior 1: an asset whose meta.audience is `public` loads and readiness
    proceeds exactly as before."""
    materialize_sidecar(tmp_path, audience="public")
    _point_loader_at(monkeypatch, tmp_path)

    assert da.load_discovery_state() is True

    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)
    assert da.discovery_available() is True
    assert da.discovery_meta("audience") == "public"


def test_private_audience_asset_is_refused_by_the_public_loader(tmp_path, monkeypatch):
    """Behavior 2: an asset declaring a PRIVATE audience is refused outright --
    readiness stays False and discovery_available() is False even with the flag
    ON. The artifact is otherwise completely valid, so nothing but the audience
    can be responsible for the refusal."""
    materialize_sidecar(tmp_path, audience="private")
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)

    assert da.load_discovery_state() is False
    assert da.discovery_available() is False
    assert da._state.ready is False
    assert da.discovery_db_path() is None


def test_missing_audience_key_fails_closed_never_treated_as_public(tmp_path, monkeypatch):
    """Behavior 3a: a MISSING audience key is refused. The default is closed --
    an artifact that says nothing about its audience is never assumed public."""
    materialize_sidecar(tmp_path, audience=None)
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)

    assert da.load_discovery_state() is False
    assert da.discovery_available() is False


@pytest.mark.parametrize(
    "audience_value",
    ["", "   ", "PUBLIC", "Public", "public ", "unrestricted", _AUDIENCE_MARKER],
)
def test_unrecognised_audience_value_fails_closed(tmp_path, monkeypatch, audience_value):
    """Behavior 3b: an EMPTY or UNRECOGNISED audience value is refused. The
    comparison is exact -- no case-folding, no stripping, no near-miss accepted
    (the same reject-incompatible idiom the schema_version check uses)."""
    materialize_sidecar(tmp_path, audience=audience_value)
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)

    assert da.load_discovery_state() is False
    assert da.discovery_available() is False


def test_refusal_is_logged_with_reason_and_no_row_content(tmp_path, monkeypatch, caplog):
    """Behavior 4: the refusal is logged with the REASON but without echoing any
    row content -- neither an ordinary cell value nor the (attacker-controllable)
    raw audience token itself."""
    db_path = materialize_sidecar(tmp_path, audience=_AUDIENCE_MARKER)
    _seed_cell_marker(db_path)
    write_manifest(tmp_path, db_path)  # re-hash AFTER seeding
    _point_loader_at(monkeypatch, tmp_path)

    with caplog.at_level(logging.INFO, logger="web.discovery_assets"):
        assert da.load_discovery_state() is False

    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "audience" in text, "the refusal must name its reason"
    assert _CELL_MARKER not in text, "a row's cell value leaked into the refusal log"
    assert _AUDIENCE_MARKER not in text, "the raw audience token was echoed back into the log"


def test_manifest_pointing_at_private_asset_leaves_loader_unresolvable(tmp_path, monkeypatch):
    """Behavior 5 (the <done> criterion): with manifest.json pointing AT a
    private-audience database -- the deployment mistake this gate exists for --
    discovery_available() is False and the loader reports not-ready, so no
    public route can resolve it. Task 3 below carries the same assertion through
    every actual public read path."""
    materialize_sidecar(tmp_path, audience="private")
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)

    # The manifest genuinely names the private file -- the failure is a content
    # refusal, not a resolution failure.
    assert (tmp_path / f"{GOLDEN_BASENAME}.db").exists()
    assert da.load_discovery_state() is False
    assert da.discovery_available() is False
    assert da.discovery_sidecar_version() is None
    assert da.discovery_meta("audience") is None


def test_audience_enum_is_a_module_level_closed_set():
    """The audience vocabulary is a module-level CLOSED set, not an inline
    literal comparison, and the public loader serves exactly one member of it."""
    assert isinstance(da._AUDIENCES, frozenset)
    assert da._AUDIENCES == frozenset({"public", "private"})
    assert da._PUBLIC_LOADER_AUDIENCE == "public"
    assert da._PUBLIC_LOADER_AUDIENCE in da._AUDIENCES


def test_resolved_audience_is_exposed_without_reopening_the_database(tmp_path, monkeypatch):
    """The resolved audience is reachable through the module's existing state
    accessor, so a later diagnostic/admin surface can report which artifact is
    live without opening the sidecar again."""
    materialize_sidecar(tmp_path, audience="public")
    _point_loader_at(monkeypatch, tmp_path)

    assert da.load_discovery_state() is True
    assert da.discovery_meta("audience") == "public"
    assert da._state.audience == "public"


def test_sibling_ignoring_resolution_is_unchanged(tmp_path, monkeypatch):
    """T-136-20-06: adding the audience gate must not loosen
    ``_resolve_versioned_db``'s sibling-ignoring behaviour. A perfectly valid
    PUBLIC sidecar sitting under a name the manifest does not name is still
    never picked up."""
    materialize_sidecar(tmp_path, audience="public")
    (tmp_path / f"{GOLDEN_BASENAME}.db").rename(tmp_path / "stale-rollback-sibling.db")
    write_manifest(tmp_path, tmp_path / "stale-rollback-sibling.db",
                   asset_basename="discovery-v1-current")
    _point_loader_at(monkeypatch, tmp_path)

    assert da.load_discovery_state() is False
    assert da._state.path is None


# ===========================================================================
# Task 2 -- the extended readiness contract: the two new tables, their
# release-contract row counts, and the amendment's required COLUMNS.
# ===========================================================================

# The Amendment 2026-08-02 column set, restated here INDEPENDENTLY of the
# loader so the mapping test is a real pin rather than a tautology. Sourced
# from docs/specs/discovery-sidecar-schema-v1.md § Amendment 2026-08-02:
#   (A) discovery_evidence / works additions
#   (B) the two new tables' DDL
#   (C) works.genre -- an EXISTING column the amendment populates and constrains
#   (F) discovery_routing_audit.demoted_work_id -- made contractual by the
#       amendment's kept_tie rule
_AMENDMENT_COLUMNS = {
    "discovery_evidence": frozenset({
        "coverage_ppm",
        "coverage_status",
        "band_rank",
        "novelty_status",
        "novelty_source_label",
        # ADDED 2026-08-02 (owner ruling F) by the schema doc's own
        # 136-03-continuation amendment, AFTER plan 136-20 was drafted -- so it
        # is absent from the plan's inline enumeration but squarely inside
        # "every column the Amendment 2026-08-02 adds". Omitting it would leave
        # open exactly the partial-builder hole this contract exists to close.
        "divergence_correctness",
        "assertion_visibility",
    }),
    "works": frozenset({"genre", "identity_visibility"}),
    "discovery_routing_audit": frozenset({"demoted_work_id"}),
    "discovery_identification": frozenset({
        "identification_id", "sys_id", "canonical_work_id", "display_work_id",
        "main_pool", "main_pool_reason", "best_band_rank", "page_count",
        "max_coverage_ppm", "relation_kind", "novelty_status",
        "divergence_correctness", "assertion_visibility", "identity_visibility",
    }),
    "manuscript_display": frozenset({
        "sys_id", "library_code", "library_sort_key",
        "shelfmark_display", "shelfmark_sort_key",
    }),
}

#: Columns from a LATER amendment that the loader nevertheless requires of EVERY
#: asset, because a read path SELECTs them unconditionally. Kept as its own set
#: rather than folded into `_AMENDMENT_COLUMNS` above (Codex review, 2026-08-12):
#: that set is the independent restatement of Amendment 2026-08-02, and quietly
#: adding a 2026-08-12 column to it would make the pin claim to check one thing
#: while checking another. The required-column contract is the UNION, and saying
#: so is what keeps both halves auditable.
#:
#: `rendered_relation` is here because three read paths select it (the claims
#: query, the manuscript-summary query, the findings query), so an asset without
#: it does not degrade -- it loads, reports itself available, and answers every
#: panel and findings read with `unavailable / query_failed`. Measured on the
#: served pre-batch asset.
_PROMOTED_RUNTIME_COLUMNS = {
    "discovery_identification": frozenset({"rendered_relation"}),
}


def _expected_required_columns():
    """Amendment 2026-08-02 ∪ the promoted runtime columns."""
    merged = {table: set(cols) for table, cols in _AMENDMENT_COLUMNS.items()}
    for table, cols in _PROMOTED_RUNTIME_COLUMNS.items():
        merged.setdefault(table, set()).update(cols)
    return {table: frozenset(cols) for table, cols in merged.items()}


def test_fully_valid_post_rebuild_public_asset_passes(tmp_path, monkeypatch):
    """Behavior 8: the positive control. Without it every failure assertion
    below could be passing for an unrelated reason."""
    materialize_sidecar(tmp_path)
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)

    assert da.load_discovery_state() is True
    assert da.discovery_available() is True


def _add_locus_display_contract(db_path, *, count_drift=False):
    conn = sqlite3.connect(db_path)
    for table in ("discovery_claim", "discovery_identification"):
        conn.execute(
            f"ALTER TABLE {table} ADD COLUMN locus_status "
            "TEXT NOT NULL DEFAULT 'unavailable'"
        )
        conn.execute(f"ALTER TABLE {table} ADD COLUMN locus_work_id TEXT")
        conn.execute(f"ALTER TABLE {table} ADD COLUMN locus_label TEXT")
    rows = [("locus_display_version", "locus-display-v1")]
    for grain, table in (
        ("claim", "discovery_claim"),
        ("identification", "discovery_identification"),
    ):
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        rows.extend([
            (f"expected_locus_{grain}_resolved", "0"),
            (f"expected_locus_{grain}_whole_work", "0"),
            (f"expected_locus_{grain}_unavailable", str(total)),
        ])
    if count_drift:
        rows = [
            (key, "999" if key == "expected_locus_claim_unavailable" else value)
            for key, value in rows
        ]
    conn.executemany("INSERT OR REPLACE INTO meta(key,value) VALUES (?,?)", rows)
    conn.commit()
    conn.close()


def test_locus_display_marker_accepts_a_complete_additive_contract(tmp_path, monkeypatch):
    db_path = materialize_sidecar(tmp_path)
    _add_locus_display_contract(db_path)
    write_manifest(tmp_path, db_path)
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)
    assert da.load_discovery_state() is True


def test_locus_display_count_drift_fails_readiness(tmp_path, monkeypatch):
    db_path = materialize_sidecar(tmp_path)
    _add_locus_display_contract(db_path, count_drift=True)
    write_manifest(tmp_path, db_path)
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)
    assert da.load_discovery_state() is False


def _add_locus_filter_contract(db_path, *, count_drift=False):
    _add_locus_display_contract(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE discovery_locus_piece ("
        "identification_id TEXT NOT NULL, locus_work_id TEXT NOT NULL, "
        "piece_ord INTEGER NOT NULL, start_unit_ord INTEGER NOT NULL, "
        "end_unit_ord INTEGER NOT NULL)"
    )
    conn.executemany(
        "INSERT OR REPLACE INTO meta(key,value) VALUES (?,?)",
        [
            ("locus_filter_version", "locus-filter-v1"),
            ("expected_rows_discovery_locus_piece", "1" if count_drift else "0"),
        ],
    )
    conn.commit()
    conn.close()


def test_locus_filter_marker_accepts_a_complete_additive_contract(tmp_path, monkeypatch):
    db_path = materialize_sidecar(tmp_path)
    _add_locus_filter_contract(db_path)
    write_manifest(tmp_path, db_path)
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)
    assert da.load_discovery_state() is True


def test_locus_filter_count_drift_fails_readiness(tmp_path, monkeypatch):
    db_path = materialize_sidecar(tmp_path)
    _add_locus_filter_contract(db_path, count_drift=True)
    write_manifest(tmp_path, db_path)
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)
    assert da.load_discovery_state() is False


def test_missing_discovery_identification_table_fails_readiness(tmp_path, monkeypatch):
    """Behavior 1."""
    materialize_sidecar(tmp_path, omit_tables=["discovery_identification"])
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)

    assert da.load_discovery_state() is False
    assert da.discovery_available() is False


def test_missing_manuscript_display_table_fails_readiness(tmp_path, monkeypatch):
    """Behavior 2."""
    materialize_sidecar(tmp_path, omit_tables=["manuscript_display"])
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)

    assert da.load_discovery_state() is False
    assert da.discovery_available() is False


@pytest.mark.parametrize(
    "meta_key",
    ["expected_rows_discovery_identification", "expected_rows_manuscript_display"],
)
def test_new_table_row_count_disagreement_fails_readiness(tmp_path, monkeypatch, meta_key):
    """Behavior 3: both tables present, but a release-contract count disagrees
    with the actual row count -- on EITHER new table."""
    materialize_sidecar(tmp_path, meta_overrides={meta_key: "7"})
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)

    assert da.load_discovery_state() is False
    assert da.discovery_available() is False


def test_missing_required_column_on_a_new_table_fails_readiness(tmp_path, monkeypatch):
    """Behavior 4a: both tables present with correct row counts, but a required
    column on a NEW table is absent. Tables and counts alone would have passed
    this asset, exposed the nav entry, and failed on the first query."""
    materialize_sidecar(
        tmp_path, omit_columns=[("discovery_identification", "max_coverage_ppm")]
    )
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)

    assert da.load_discovery_state() is False
    assert da.discovery_available() is False


def test_missing_required_column_on_an_existing_table_fails_readiness(tmp_path, monkeypatch):
    """Behavior 4b: the same, for a column this phase adds to a PRE-EXISTING
    table. This is the case `_REQUIRED_TABLES` structurally cannot catch."""
    materialize_sidecar(tmp_path, omit_columns=[("discovery_evidence", "coverage_ppm")])
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)

    assert da.load_discovery_state() is False
    assert da.discovery_available() is False


def test_extra_unexpected_column_does_not_fail_readiness(tmp_path, monkeypatch):
    """The column check is a SUBSET check: a column the contract does not name
    is not a failure, so a future additive build is not gratuitously rejected."""
    materialize_sidecar(
        tmp_path,
        extra_columns=[
            ("manuscript_display", "some_future_additive_column", "TEXT"),
            ("discovery_evidence", "another_future_column", "INTEGER"),
        ],
    )
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)

    assert da.load_discovery_state() is True
    assert da.discovery_available() is True


def test_required_columns_mapping_is_the_amendment_plus_promoted_runtime_columns(tmp_path):
    """Behavior 5: the required-column set covers every column the Amendment
    2026-08-02 adds -- including the ones on PRE-EXISTING tables, not only the
    new tables' own columns -- PLUS every column promoted because a read path
    selects it unconditionally.

    ⟨RENAMED 2026-08-12, Codex review⟩ It was
    `..._matches_the_amendment_exactly`, and after `rendered_relation` was
    promoted that name was false: the mapping is deliberately a SUPERSET of the
    2026-08-02 amendment. The two halves are now separate sets and the assertion
    is their union, so neither can absorb the other silently.
    """
    assert isinstance(da._REQUIRED_COLUMNS, dict)
    assert da._REQUIRED_COLUMNS == _expected_required_columns()
    # The promotion is a real addition, not a restatement -- if a future edit
    # folded it into the amendment set, the union above would still pass while
    # the pin stopped meaning what it says.
    for table, columns in _PROMOTED_RUNTIME_COLUMNS.items():
        assert not (columns & _AMENDMENT_COLUMNS.get(table, frozenset())), (
            f"{table}: a promoted runtime column was folded into the "
            "Amendment 2026-08-02 set, which is the one thing this split exists "
            "to prevent")
    for table, columns in da._REQUIRED_COLUMNS.items():
        assert isinstance(columns, frozenset), f"{table} must map to a frozenset"

    # The nine columns the plan enumerates for EXISTING tables must all be
    # present, wherever they live.
    existing_table_columns = (
        da._REQUIRED_COLUMNS["discovery_evidence"]
        | da._REQUIRED_COLUMNS["works"]
        | da._REQUIRED_COLUMNS["discovery_routing_audit"]
    )
    assert {
        "coverage_ppm", "coverage_status", "band_rank", "novelty_status",
        "novelty_source_label", "assertion_visibility", "identity_visibility",
        "genre", "demoted_work_id",
    } <= existing_table_columns


def test_missing_new_meta_key_fails_readiness(tmp_path, monkeypatch):
    """Behavior 6: a sidecar missing a new release-contract meta key fails
    readiness rather than silently skipping the count check for that table."""
    materialize_sidecar(
        tmp_path, omit_meta_keys=["expected_rows_discovery_identification"]
    )
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)

    assert da.load_discovery_state() is False
    assert da.discovery_available() is False


def test_pre_rebuild_asset_fails_readiness_so_a_rollback_hides_cleanly(tmp_path, monkeypatch):
    """Behavior 7 -- THE case that is easiest to skip. Deploying forward is
    tested by everything downstream; rolling BACK to the pre-rebuild asset is
    tested by nothing unless it is tested here.

    The committed golden fixture IS the pre-rebuild shape: no `meta.audience`,
    no `discovery_identification`/`manuscript_display`, none of the amendment's
    new columns. Under the new contract it must leave the surfaces HIDDEN rather
    than half-working -- and it must do so without raising.
    """
    materialize_pre_rebuild_sidecar(tmp_path)
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)

    assert da.load_discovery_state() is False
    assert da.discovery_available() is False
    assert da.discovery_db_path() is None


def test_pre_rebuild_asset_fails_on_the_structural_checks_alone(tmp_path, monkeypatch):
    """The evidence behind the retain-`discovery-v1` decision.

    That decision rests on the required-table / required-COLUMN / row-count
    checks carrying the whole weight -- so it must be shown that they do, with
    the audience gate taken out of the picture. Stamp a `public` audience marker
    onto the otherwise untouched PRE-REBUILD asset: the audience gate now passes,
    and the asset is still refused, by structure alone.
    """
    materialize_pre_rebuild_sidecar(tmp_path, audience="public")
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)

    assert da.load_discovery_state() is False
    assert da.discovery_available() is False


def test_new_tables_are_in_the_required_set_with_count_pairs():
    """Both new tables are required, and each has a release-contract
    (meta_key, table) pair so the EXISTING count loop covers them without a new
    mechanism."""
    assert "discovery_identification" in da._REQUIRED_TABLES
    assert "manuscript_display" in da._REQUIRED_TABLES

    pairs = dict((table, meta_key) for meta_key, table in da._RELEASE_CONTRACT_COUNTS)
    assert pairs["discovery_identification"] == "expected_rows_discovery_identification"
    assert pairs["manuscript_display"] == "expected_rows_manuscript_display"

    assert "expected_rows_discovery_identification" in da._REQUIRED_META_KEYS
    assert "expected_rows_manuscript_display" in da._REQUIRED_META_KEYS


def test_schema_marker_is_not_bumped():
    """The retain-`discovery-v1` decision, pinned. The amendment is purely
    ADDITIVE, and the required-table / required-COLUMN / count checks carry the
    whole weight deterministically -- which is only honest because columns are
    actually checked (see the dropped-column tests above)."""
    assert da._EXPECTED_SCHEMA_VERSION == "discovery-v1"


# ===========================================================================
# Task 3 -- end to end: no PUBLIC READ PATH can reach a private artifact.
#
# The predicate-level assertions above prove `discovery_available()` is False.
# That is necessary but not what VIS-01 actually asks for. What it asks for is
# that no public read path returns a row. These tests exercise the REAL wrappers
# in `web/discovery.py` -- the ones a page renders through -- and assert each
# returns its unavailable envelope, then assert the SAME paths do return rows
# against a valid public artifact so the refusal cannot pass vacuously.
# ===========================================================================

# Arguments for the public read wrappers, keyed by PARAMETER NAME rather than
# by function, so the sweep below automatically covers a reader a later plan
# adds. Values are ids that genuinely exist in the golden fixture, so the same
# map drives the inverse (non-empty) control.
def _golden_claim_id(page_id: str) -> str:
    """A REAL claim id from the golden fixture. Claim ids are content hashes, so
    resolving one at import time is the only way to keep this map honest across
    a fixture rebuild."""
    conn = sqlite3.connect(f"file:{GOLDEN_DB}?mode=ro", uri=True)
    try:
        row = conn.execute(
            "SELECT claim_id FROM discovery_claim WHERE page_id = ? ORDER BY claim_id LIMIT 1",
            (page_id,),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None, f"golden fixture has no claim on page {page_id!r}"
    return row[0]


def _must_not_be_reached(*_args, **_kwargs):
    """Injected where a private artifact must never reach the renderer."""
    raise AssertionError(
        "a private-audience artifact reached the export's workbook builder -- "
        "the audience gate let rows past before rendering")


_READ_PATH_ARGS = {
    "page_id": "p012",
    "claim_id": _golden_claim_id("p012"),
    "work_id": "w000005",
    "sys_id": "s001",
    "evidence_source": "track1_direct",
    "confidence_band": "expert_verified",
    "collection_id": "propagated_witness_collection_v1",
    "enabled_bands": None,
    # 136-14: the manuscript scope is served by `page_id IN (...)` over the
    # browse page's own page list, so its read path takes a page LIST.
    "page_ids": ["p012"],
    # 136-14: the findings facet cascade. `domain` is the first level, and the
    # only one that needs no level above it selected.
    "level": "domain",
    # excerpt-v1: the text-vs-text read is keyed by the identification.
    "identification_id": "i-nonexistent",
    # phase 136.2: the findings export takes the workbook builder as an
    # INJECTION. The stub RAISES rather than returning a harmless value, and
    # that is the registration doing real work: on a private-audience artifact
    # the walk must fail before anything is rendered, so if the audience gate
    # ever let it through, the builder would be reached and this proof would
    # fail loudly instead of passing on an empty workbook.
    "build_fn": _must_not_be_reached,
}

# The unavailable envelope each wrapper shape returns. Anything else is data.
_EMPTY_ENVELOPES = (None, [], {}, (), False, 0)


def _is_empty_read_result(result) -> bool:
    """True when NO ROW came back, in either wrapper shape.

    Two shapes coexist deliberately (136-14): the legacy list/None wrappers,
    which fail open to the values in `_EMPTY_ENVELOPES`, and the enveloped
    wrappers, which return `{status, items, total, meta}` so an outage is
    distinguishable from a genuine zero (D-13). For THIS proof only one
    question matters -- did a row escape the audience boundary -- so an
    envelope counts as empty when it carries no item and a zero total.
    """
    if isinstance(result, dict) and "status" in result and "items" in result:
        return not result["items"] and result["total"] == 0
    return result in _EMPTY_ENVELOPES


def _reimport_web_discovery():
    """Force a FRESH import of web.discovery so its module-level
    DiscoveryService re-resolves against the currently loaded state (the
    tests/test_discovery_composition.py idiom).

    RELOAD, never `sys.modules.pop()` + re-import. Popping and re-importing
    builds a SECOND module object, and restoring `sys.modules` afterwards does
    not undo it: the import machinery also rebinds the submodule as an
    ATTRIBUTE of the parent package, and `import web.discovery as x` reads that
    package attribute. So the popper leaves `sys.modules["web.discovery"]` and
    `web.discovery` pointing at different objects.

    That was a real cross-suite failure. `web/pages/findings.py` from-imports
    `get_findings_enveloped`, whose `__globals__` IS the dict of whichever
    object was live when the page was first imported. With two objects around,
    tests/test_findings_page.py patched `discovery_available` on one while the
    page read the other -- so the page took the unavailable branch and issued
    ZERO executor dispatches, and the second module-level execution of
    web/discovery.py cascaded `Event loop is closed` into unrelated suites.

    `importlib.reload` re-executes the module in its EXISTING namespace, so
    exactly one object stays live in `sys.modules`, on the package, and as the
    page's `__globals__`, while module-level state (notably the
    `DiscoveryService` construction this helper exists to re-run) is rebuilt.
    Note the conftest.py precedent at the `shared.local_indexer` fixture: a
    reload does NOT rebind names another module already from-imported, so a
    caller that needs the new FUNCTION OBJECT (rather than fresh module state)
    must rebind it itself.
    """
    import web.discovery as disc  # noqa: PLC0415 -- reloaded deliberately below

    return importlib.reload(disc)


#: Public async wrappers in `web/discovery.py` that are NOT read paths, and so
#: are excluded from the VIS-01 sweep by name.
#:
#: EXCLUDED, NOT REGISTERED, and the distinction is the point. This sweep proves
#: that no read returns a row from a private-audience artifact; it does that by
#: CALLING every public coroutine it finds. A WRITE has no row to leak -- it
#: touches Supabase, not the sidecar, and the audience boundary is not a thing it
#: can cross -- so registering its parameter would not extend the proof. It would
#: make the sweep attempt a real Supabase insert on every run, which is both a
#: side effect a test must not have and a call whose failure would say nothing
#: about the audience boundary.
#:
#: Each entry needs a REASON, and the guard below fails on an entry whose name no
#: longer exists -- so this cannot become a quiet escape hatch for a read that
#: someone found inconvenient.
_NON_READ_PATHS = {
    "suppress_identification":
        "a WRITE (the admin hide list, in Supabase). It reads no sidecar row, so "
        "there is nothing here for a private artifact to leak; calling it in this "
        "sweep would attempt a real Supabase insert.",
    "suppressed_identification_ids":
        "reads SUPABASE, not the sidecar -- the admin hide list. Its failure mode "
        "is fail-open to an empty tuple, and it can no more leak a private "
        "sidecar row than the write above can.",
}


#: The exclusion set, PINNED EXACTLY. Not a bound, not a shape check.
#:
#: An earlier revision asserted `len(...) <= 4` and `len(reason) > 40`, which Codex
#: review (2026-08-07) correctly called not a guard at all: a future coroutine that
#: DOES read the sidecar could be added with any forty-character sentence, stay
#: under the bound, and silently drop out of a sweep whose entire purpose is to
#: prove no read returns a row from a private-audience artifact.
#:
#: Equality is the mechanism. Excluding anything else fails HERE, by name, and the
#: only way past it is to edit this constant -- which is a diff a reviewer sees.
_REVIEWED_NON_READ_PATHS = frozenset({
    "suppress_identification",
    "suppressed_identification_ids",
})


def test_the_non_read_exclusions_are_exactly_the_two_reviewed_paths():
    """An exclusion whose function is gone is a dead excuse that reads as though it
    covers something -- and an exclusion added for a real READ would silently shrink
    the VIS-01 sweep, which is the one thing this list must not enable."""
    import web.discovery as disc

    assert frozenset(_NON_READ_PATHS) == _REVIEWED_NON_READ_PATHS, (
        "the VIS-01 non-read exclusion set changed. Every entry is a public "
        "coroutine the audience proof no longer sweeps, so adding one is a "
        "reviewed decision: if the new path really touches Supabase and not the "
        "sidecar, add it to `_REVIEWED_NON_READ_PATHS` in the same commit and say "
        "why. If it reads the sidecar, it belongs in the sweep instead.")
    for name, reason in _NON_READ_PATHS.items():
        assert hasattr(disc, name), (
            f"web.discovery.{name} no longer exists -- delete the exclusion")
        assert len(reason) > 40, f"{name} is excluded with no stated reason"


def _public_async_read_paths(disc):
    """Every public async read wrapper in web/discovery.py, with a call-ready
    kwargs dict. A wrapper whose required parameter is not in `_READ_PATH_ARGS`
    raises here rather than being silently skipped -- so a later plan adding a
    reader with a new parameter is FORCED to register it instead of quietly
    escaping this proof."""
    paths = {}
    for name, fn in sorted(vars(disc).items()):
        if name.startswith("_") or not inspect.iscoroutinefunction(fn):
            continue
        if getattr(fn, "__module__", None) != disc.__name__:
            continue
        if name in _NON_READ_PATHS:
            continue
        kwargs = {}
        for param_name, param in inspect.signature(fn).parameters.items():
            if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                continue
            if param.default is not inspect.Parameter.empty:
                continue
            if param_name not in _READ_PATH_ARGS:
                raise AssertionError(
                    f"web.discovery.{name}() takes an unregistered required parameter "
                    f"{param_name!r} -- add it to _READ_PATH_ARGS so the VIS-01 "
                    "end-to-end refusal proof covers this read path"
                )
            kwargs[param_name] = _READ_PATH_ARGS[param_name]
        paths[name] = (fn, kwargs)
    return paths


def _first_claim_id(disc):
    async def _run():
        claims = await disc.get_claims_for_page(_READ_PATH_ARGS["page_id"])
        return claims[0]["claim_id"] if claims else None

    return asyncio.run(_run())


def test_private_artifact_returns_no_row_on_any_public_read_path(tmp_path, monkeypatch):
    """The VIS-01 end-to-end refusal: manifest pointing at a PRIVATE-audience
    database, flag ON, and NOT ONE public read path returns a row.

    Asserted per path by name, so a failure says which surface leaked.
    """
    materialize_sidecar(tmp_path, audience="private")
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)
    assert da.load_discovery_state() is False

    disc = _reimport_web_discovery()
    paths = _public_async_read_paths(disc)

    # The four wrappers the plan names explicitly must be among them -- a
    # regression that deleted one would otherwise shrink the sweep silently.
    for required in (
        "get_claims_for_page",          # the page claims path
        "get_work_witnesses",           # the work / manuscript scope
        "get_pages_related_to_page",    # the related-page path
        "get_evidence",                 # on-demand evidence expansion
    ):
        assert required in paths, f"{required} is no longer a public read path"

    async def _run():
        for name, (fn, kwargs) in paths.items():
            result = await fn(**kwargs)
            assert _is_empty_read_result(result), (
                f"web.discovery.{name}() returned data from a PRIVATE-audience "
                "artifact -- the audience boundary leaked"
            )

    asyncio.run(_run())

    # The sync predicate too: nothing to noindex when nothing is available.
    assert disc.discovery_methods_noindex() is False


def test_public_paths_do_return_rows_against_a_valid_public_artifact(tmp_path, monkeypatch):
    """The inverse control. Without it the refusal test above would also pass
    against an empty database, and would prove nothing."""
    materialize_sidecar(tmp_path, audience="public")
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)
    assert da.load_discovery_state() is True

    disc = _reimport_web_discovery()

    async def _run():
        assert await disc.get_version() is not None
        claims = await disc.get_claims_for_page(_READ_PATH_ARGS["page_id"])
        assert len(claims) > 0, "the page claims path returned nothing"
        related = await disc.get_pages_related_to_page("p004")
        assert len(related) > 0, "the related-page path returned nothing"
        witnesses = await disc.get_work_witnesses(_READ_PATH_ARGS["work_id"])
        assert len(witnesses) > 0, "the work/manuscript scope returned nothing"
        evidence = await disc.get_evidence(claims[0]["claim_id"])
        assert len(evidence) > 0, "the evidence path returned nothing"

        # 136-14: the ENVELOPED paths need the same inverse control, or their
        # refusal above would pass merely because they always return an empty
        # envelope.
        enveloped = await disc.get_claims_for_page_enveloped(
            _READ_PATH_ARGS["page_id"])
        assert enveloped["status"] == "ok" and enveloped["total"] > 0
        scope = await disc.get_manuscript_works_enveloped(_READ_PATH_ARGS["page_ids"])
        assert scope["status"] == "ok" and scope["total"] > 0
        count = await disc.get_related_page_count_enveloped("p004")
        assert count["status"] == "ok" and count["total"] > 0
        rows = await disc.get_related_pages_enveloped("p004")
        assert rows["status"] == "ok" and len(rows["items"]) > 0
        # The two findings paths get a WEAKER control here, stated plainly:
        # this fixture CREATES `discovery_identification` but seeds no rows
        # (see tests/fixtures/discovery_v2_fixture.py), so there is nothing for
        # them to return. What IS provable -- and what distinguishes them from
        # the private-artifact case above, where they answer `unavailable` -- is
        # that against a valid public artifact they reach the database and
        # report SUCCESS. Their row-level non-vacuity is proved in
        # tests/test_discovery_findings_query.py against a populated fixture.
        findings = await disc.get_findings_enveloped(bucket="all")
        assert findings["status"] == "ok"
        facets = await disc.get_findings_facets_enveloped(
            _READ_PATH_ARGS["level"], bucket="all")
        assert facets["status"] == "ok"

    asyncio.run(_run())


def test_registered_read_path_arguments_resolve_to_real_fixture_rows(tmp_path, monkeypatch):
    """Keeps `_READ_PATH_ARGS` honest: the refusal sweep must never start
    passing merely because it is asking for ids that do not exist. Each id used
    by the sweep is proved to resolve to a real row against the PUBLIC artifact.
    """
    materialize_sidecar(tmp_path, audience="public")
    _point_loader_at(monkeypatch, tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)
    assert da.load_discovery_state() is True

    disc = _reimport_web_discovery()
    assert _first_claim_id(disc) == _READ_PATH_ARGS["claim_id"]

    async def _run():
        assert len(await disc.get_evidence(_READ_PATH_ARGS["claim_id"])) > 0
        assert len(await disc.get_work_witnesses(_READ_PATH_ARGS["work_id"])) > 0

    asyncio.run(_run())


# ===========================================================================
# Untrusted artifact values must never reach the log
# (Codex code review 2026-08-03, finding 1 -- BLOCKER)
# ===========================================================================
#
# The audience check was written never to interpolate its raw value, and said so
# in a comment. The checks immediately above and below it did interpolate:
# schema_version, claim_type, and the (evidence_source, confidence_band) pair
# were all echoed into a ValueError that the fail-closed handler then logs in
# full. A restricted name sitting in any of those fields would land in a log.
#
# `test_refusal_is_logged_with_reason_and_no_row_content` above cannot catch
# this: it seeds an AUDIENCE marker, so the loader refuses at the audience gate
# and returns before any of these branches execute. These controls therefore use
# a **public** audience so the loader gets past that gate, and each asserts three
# things -- the load failed, the log names THIS branch's reason (proving the
# branch was actually reached, not short-circuited earlier), and the sentinel is
# absent.

_FIELD_MARKER = "SYNTHETIC-FIELD-MARKER-4KJ81"


def _rehash(tmp_path, db_path):
    """Re-write the manifest so the content-hash gate does not refuse the
    artifact before the check under test runs."""
    write_manifest(tmp_path, db_path)


def _drop_constraints(conn, table):
    """CREATE TABLE ... AS SELECT copies data but not CHECK/NOT NULL/PK, letting
    a control reach a state the frozen DDL rejects outright.

    The DDL blocking the mutation is defense in depth and a GOOD thing -- an
    out-of-vocabulary `claim_type` cannot be written through the real schema.
    But it also means the loader's OWN runtime re-check can only be exercised
    against a table recreated without those constraints, i.e. a non-conforming
    producer. Same idiom as tests/test_discovery_release_contract.py.
    """
    conn.execute(f'ALTER TABLE "{table}" RENAME TO "{table}__bak"')
    conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM "{table}__bak"')
    conn.execute(f'DROP TABLE "{table}__bak"')


@pytest.mark.parametrize("field,sql,expected_reason", [
    (
        "meta.schema_version",
        "UPDATE meta SET value = ? WHERE key = 'schema_version'",
        "incompatible schema_version",
    ),
    (
        "discovery_claim.claim_type",
        "UPDATE discovery_claim SET claim_type = ?",
        "outside the frozen vocabulary",
    ),
    (
        "discovery_evidence.confidence_band",
        "UPDATE discovery_evidence SET confidence_band = ?",
        "outside the frozen vocabulary",
    ),
    # The band check keys on the PAIR, so a sentinel in evidence_source reaches
    # the same refusal by the other half of the lookup. Code review 2A found the
    # original three cases never touched this side.
    (
        "discovery_evidence.evidence_source",
        "UPDATE discovery_evidence SET evidence_source = ?",
        "outside the frozen vocabulary",
    ),
    # meta values reached by the release-contract count loop. `int(expected)`
    # used to name the raw value for us inside "invalid literal for int() with
    # base 10: '<value>'" -- no interpolation anywhere, so an f-string audit
    # could not see it (code review 2A, finding 1).
    (
        "meta.expected_rows_claims",
        "UPDATE meta SET value = ? WHERE key = 'expected_rows_claims'",
        "is missing or not an integer",
    ),
    (
        "meta.expected_rows_discovery_identification",
        "UPDATE meta SET value = ? WHERE key = 'expected_rows_discovery_identification'",
        "is missing or not an integer",
    ),
])
def test_rejected_field_value_never_reaches_the_log(
    tmp_path, monkeypatch, caplog, field, sql, expected_reason
):
    db_path = materialize_sidecar(tmp_path, audience="public")
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        table = field.split(".")[0]
        if table != "meta":
            _drop_constraints(conn, table)
        conn.execute(sql, (_FIELD_MARKER,))
        conn.commit()
    finally:
        conn.close()
    _rehash(tmp_path, db_path)
    _point_loader_at(monkeypatch, tmp_path)

    with caplog.at_level(logging.INFO):
        ready = da.load_discovery_state()

    text = caplog.text
    assert ready is False, f"a sentinel in {field} did not refuse the artifact"
    assert expected_reason in text, (
        f"the refusal of {field} did not come from its own check -- the loader "
        f"failed earlier, so this control proves nothing about that branch. "
        f"Log:\n{text}"
    )
    assert _FIELD_MARKER not in text, (
        f"the raw value of {field} was echoed into the log. Any restricted name "
        f"sitting in that field would be written to disk. Log:\n{text}"
    )


def test_public_audience_control_reaches_past_the_audience_gate(tmp_path, monkeypatch):
    """Proves the parametrized controls above are not silently refused at the
    audience gate (which is what made the older marker test unable to reach
    these branches): the same fixture, unmutated, loads successfully."""
    materialize_sidecar(tmp_path, audience="public")
    _point_loader_at(monkeypatch, tmp_path)
    assert da.load_discovery_state() is True


def test_manifest_asset_basename_never_reaches_the_log(tmp_path, monkeypatch, caplog):
    """The basename is manifest-supplied, and the loader builds a filesystem path
    out of it. Nothing interpolates it -- `OSError` names it for us, which is why
    an audit for f-strings missed this (code review 2A, finding 1)."""
    db_path = materialize_sidecar(tmp_path, audience="public")
    manifest_path = tmp_path / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    # A well-formed bare stem that simply does not exist: it must clear the
    # traversal guard so the refusal comes from the READ, not from the shape
    # check, which would prove nothing about the OSError path.
    manifest["asset_basename"] = _FIELD_MARKER
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    _point_loader_at(monkeypatch, tmp_path)
    assert db_path.exists()  # the real asset is still there; only the name moved

    with caplog.at_level(logging.INFO):
        ready = da.load_discovery_state()

    text = caplog.text
    assert ready is False
    assert "name withheld" in text, (
        "the refusal did not come from the file-read branch, so this control "
        f"proves nothing about it. Log:\n{text}"
    )
    assert _FIELD_MARKER not in text, (
        "the manifest's asset_basename was echoed into the log. A restricted "
        f"name in that field would be written to disk. Log:\n{text}"
    )


def test_release_count_mismatch_withholds_the_expected_value(tmp_path, monkeypatch, caplog):
    """The numeric-but-wrong case takes a DIFFERENT branch from the
    not-an-integer case above, and used to interpolate `expected!r` directly."""
    db_path = materialize_sidecar(tmp_path, audience="public")
    conn = sqlite3.connect(str(db_path))
    try:
        # A real integer, so int() succeeds and the comparison is what refuses.
        conn.execute(
            "UPDATE meta SET value = '424242' WHERE key = 'expected_rows_claims'"
        )
        conn.commit()
    finally:
        conn.close()
    _rehash(tmp_path, db_path)
    _point_loader_at(monkeypatch, tmp_path)

    with caplog.at_level(logging.INFO):
        ready = da.load_discovery_state()

    text = caplog.text
    assert ready is False
    assert "row-count mismatch" in text, f"wrong branch refused it. Log:\n{text}"
    assert "424242" not in text, (
        "the expected row count came from the artifact and was echoed. A count "
        f"is harmless; the interpolation that carried it is not. Log:\n{text}"
    )


def test_an_unexpected_exception_type_logs_no_message_text(tmp_path, monkeypatch, caplog):
    """The load-bearing half of the fix: the rule must hold for raise sites
    nobody audited, including a library's.

    Without this, `_LoaderRefusal` is just a rename -- the catch-all would still
    log whatever text an sqlite3 error or a future `raise` carried out of an
    untrusted artifact."""
    materialize_sidecar(tmp_path, audience="public")
    _point_loader_at(monkeypatch, tmp_path)

    def _explode(_path):
        raise RuntimeError(f"library detail carrying {_FIELD_MARKER}")

    monkeypatch.setattr(da, "_sha256_file", _explode)

    with caplog.at_level(logging.INFO):
        ready = da.load_discovery_state()

    text = caplog.text
    assert ready is False
    assert "RuntimeError" in text, (
        f"the exception type is the whole diagnostic; it must be logged. Log:\n{text}"
    )
    assert "detail withheld" in text, f"the withheld-detail branch did not run. Log:\n{text}"
    assert _FIELD_MARKER not in text, (
        "an unaudited exception's message text reached the log. That is the "
        f"class this fix exists to close, not the individual sites. Log:\n{text}"
    )
