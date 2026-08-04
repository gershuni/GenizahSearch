# -*- coding: utf-8 -*-
"""Materialize a loader-ready SYNTHETIC discovery sidecar for CI.

Phase 136. Used by the ``findings-browser-check`` job in
``.github/workflows/ci.yml``.

WHY THIS EXISTS
---------------
The findings page (``/computed-identifications``) is gated on
``web/discovery_assets.py::discovery_available()`` = ``DISCOVERY_ENABLED`` AND a
startup-validated sidecar. A CI runner has neither: ``discovery_data/`` is
gitignored, so the route clean-hides and any browser check against it would fail
for a reason that has nothing to do with what it is checking.

This script builds the synthetic post-rebuild sidecar from the committed golden
fixture into a directory of the caller's choosing, so the app can be served
against it via ``GENIZAH_DISCOVERY_DATA_DIR``.

WHY IT DOES NOT USE ``materialize_sidecar()``
---------------------------------------------
``tests/fixtures/discovery_v2_fixture.py::materialize_sidecar`` first STRIPS the
golden back to the v1 shape (so its ``omit_*`` defect knobs have something to
withhold) and then rebuilds the Amendment 2026-08-02 tables EMPTY. That is
exactly right for loader-readiness tests, and exactly wrong here: it drops the
golden's 18 ``discovery_identification`` rows, and a findings page with zero
rows in both buckets can only ever prove that a re-render happened.

This script therefore uses the same module's other two entry points --
``upgrade_db_to_post_rebuild`` (adds ``meta.audience`` plus the two
release-contract count keys; the tables and columns are already present in the
refreshed golden, so it adds nothing else) and ``write_manifest`` (recomputes
the content hash AFTER the mutation). Same fixture module, same contract, rows
preserved: 2 identifications in the main pool and 16 in the second bucket, which
is what makes "the results region is replaced" a statement about content.

NOTHING HERE TOUCHES REAL RESEARCH DATA. Every value in the golden fixture is
fabricated, and no restricted corpus is named anywhere in this tree -- masked
corpora appear only as "M-source" / "R-source".

The repo's real ``discovery_data/manifest.json`` is deliberately NOT repointed:
``tests/test_cert01_grading_validator.py`` resolves the REAL artifact through it,
so mutating it in place would silently redirect an unrelated gate.

Usage::

    python scripts/ci_materialize_discovery_fixture.py --dest /tmp/discovery-ci
"""
from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def build(dest_dir: Path) -> Path:
    """Build the sidecar + manifest in ``dest_dir``. Returns the sidecar path."""
    from tests.fixtures.discovery_v2_fixture import (
        GOLDEN_BASENAME,
        GOLDEN_DB,
        upgrade_db_to_post_rebuild,
        write_manifest,
    )

    dest_dir.mkdir(parents=True, exist_ok=True)
    db_path = dest_dir / f"{GOLDEN_BASENAME}.db"
    shutil.copyfile(GOLDEN_DB, db_path)
    upgrade_db_to_post_rebuild(db_path)
    write_manifest(dest_dir, db_path)
    return db_path


def preflight(dest_dir: Path, db_path: Path) -> None:
    """Fail LOUDLY here rather than as an opaque browser timeout later.

    Two independent checks:

    1. The sidecar carries identifications in BOTH pools. This is a property of
       the fixture alone -- no service code involved -- so a failure means the
       fixture changed, not that the page regressed.
    2. The loader accepts it and the findings service returns `ok` with rows in
       both buckets. This is the one that proves the page will have something to
       switch BETWEEN.

    Neither is the browser check. Both exist so that when the browser check
    fails, it is because of the control.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        pools = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT main_pool, COUNT(*) FROM discovery_identification GROUP BY main_pool"
            )
        }
    finally:
        conn.close()
    if not pools.get(1) or not pools.get(0):
        raise SystemExit(
            "PRE-FLIGHT FAILED: the fixture sidecar does not carry identifications in "
            f"BOTH pools (main_pool counts: {pools!r}). The findings page needs rows on "
            "each side of the bucket control for a bucket switch to mean anything."
        )

    os.environ["GENIZAH_DISCOVERY_DATA_DIR"] = str(dest_dir)
    os.environ.setdefault("DISCOVERY_ENABLED", "1")

    import asyncio

    import web.discovery_assets as da

    if da.DISCOVERY_DATA_DIR != str(dest_dir):
        raise SystemExit(
            "PRE-FLIGHT FAILED: web/discovery_assets.py did not honour "
            f"GENIZAH_DISCOVERY_DATA_DIR (reads {da.DISCOVERY_DATA_DIR!r})."
        )
    if not da.load_discovery_state():
        raise SystemExit(
            "PRE-FLIGHT FAILED: the fixture sidecar did not pass the loader's "
            "readiness contract. Run scripts/verify_discovery_sidecar.py against "
            f"{db_path} for detail; the loader itself withholds artifact content by design."
        )

    from web.discovery import get_findings_enveloped

    for bucket in ("main", "more"):
        envelope = asyncio.run(get_findings_enveloped(bucket=bucket))
        status = envelope.get("status")
        total = int(envelope.get("total") or 0)
        if status != "ok" or total <= 0:
            raise SystemExit(
                f"PRE-FLIGHT FAILED: the findings service returned status={status!r} "
                f"total={total} for the {bucket!r} bucket. The browser check needs BOTH "
                "buckets populated; a change to the surface projection can cause this, "
                "and it is not a regression of the control being checked."
            )
        print(f"  pre-flight {bucket:>4} bucket: status=ok total={total}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dest", required=True, help="directory to materialize into")
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="build only (no loader/service verification)",
    )
    args = parser.parse_args(argv)

    dest_dir = Path(args.dest).resolve()
    db_path = build(dest_dir)
    print(f"materialized discovery fixture sidecar: {db_path}")
    if not args.skip_preflight:
        preflight(dest_dir, db_path)
    print(f"GENIZAH_DISCOVERY_DATA_DIR={dest_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
