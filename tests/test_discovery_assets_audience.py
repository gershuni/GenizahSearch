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

Masking discipline (D-25): every value in this module is fabricated/synthetic;
no restricted corpus is named anywhere (they appear only as "M-source" /
"R-source" repo-wide), and the refusal-log test asserts positively that no cell
value from the refused artifact reaches the log.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import sqlite3
import sys

import pytest

import web.discovery_assets as da
from tests.fixtures.discovery_v2_fixture import (
    ADDED_COLUMNS_ON_EXISTING_TABLES,
    GOLDEN_BASENAME,
    NEW_TABLE_COLUMNS,
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
    module state never leaks across tests (the atlas/discovery test idiom)."""
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
