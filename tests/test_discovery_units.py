# -*- coding: utf-8 -*-
"""DATA-10 witness-unit projection tests (Phase 134, plan 134-03, Task 3).
"""
import sqlite3
from pathlib import Path

from scripts import discovery_ids as ids

FIXTURE_DB = (
    Path(__file__).resolve().parent / "fixtures" / "discovery" / "discovery-v1-fixture.db"
)

# The "same scribe" pair the fixture deliberately keeps UNMERGED (DATA-10:
# merge_basis is NEVER 'scribe').
_SCRIBE_PAIR_SYS_IDS = ("990000000000000017", "990000000000000018")


def _connect_ro():
    return sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)


def test_oxford_part_and_physical_join_pairs_are_merged():
    conn = _connect_ro()
    try:
        rows = conn.execute(
            "SELECT unit_id, sys_id, merge_basis FROM witness_unit_members ORDER BY unit_id, sys_id"
        ).fetchall()
    finally:
        conn.close()
    assert rows, "fixture must carry >=1 witness_unit"

    by_unit = {}
    for unit_id, sys_id, merge_basis in rows:
        by_unit.setdefault(unit_id, []).append((sys_id, merge_basis))

    bases_seen = {mb for members in by_unit.values() for _, mb in members}
    assert ids.MERGE_BASIS_OXFORD_PART in bases_seen
    assert ids.MERGE_BASIS_PHYSICAL_JOIN in bases_seen
    assert "scribe" not in bases_seen, "merge_basis must NEVER be 'scribe' (DATA-10)"

    for unit_id, members in by_unit.items():
        member_bases = {mb for _, mb in members}
        assert len(member_bases) == 1, (
            f"unit {unit_id} mixes merge_basis values {member_bases} -- "
            "each unit is homogeneous per the fixture design"
        )


def test_scribe_pair_not_merged():
    conn = _connect_ro()
    try:
        rows = conn.execute(
            "SELECT unit_id FROM witness_unit_members WHERE sys_id IN (?, ?)",
            _SCRIBE_PAIR_SYS_IDS,
        ).fetchall()
    finally:
        conn.close()
    assert rows == [], "the 'same scribe' pair must NEVER co-occur in a witness_unit"


def test_unit_id_deterministic_via_discovery_ids():
    conn = _connect_ro()
    try:
        unit_ids = [r[0] for r in conn.execute("SELECT unit_id FROM witness_units").fetchall()]
        by_unit = {}
        for unit_id, sys_id in conn.execute(
            "SELECT unit_id, sys_id FROM witness_unit_members"
        ).fetchall():
            by_unit.setdefault(unit_id, []).append(sys_id)
    finally:
        conn.close()

    assert unit_ids, "fixture must carry >=1 witness_unit"
    for unit_id in unit_ids:
        members = by_unit[unit_id]
        recomputed = ids.unit_id(members)
        assert recomputed == unit_id, "unit_id must recompute identically via scripts.discovery_ids.unit_id"
        # Order-invariance: shuffling the member list must still recompute the same id.
        assert ids.unit_id(list(reversed(members))) == unit_id


def test_each_sys_id_in_at_most_one_unit():
    conn = _connect_ro()
    try:
        rows = conn.execute("SELECT sys_id, COUNT(*) FROM witness_unit_members GROUP BY sys_id").fetchall()
    finally:
        conn.close()
    for sys_id, count in rows:
        assert count == 1, f"sys_id {sys_id} appears in {count} witness_units (must be <=1)"
