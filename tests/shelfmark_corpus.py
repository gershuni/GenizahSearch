"""Load a real MetadataManager over the tracked libraries.csv for shelfmark tests.

Shared by the catalogue-shelfmark real-data tests and
``scripts/build_shelfmark_resolve_baseline.py``. Two guarantees:

* nothing is written into the repository: the CUDL alias builder's collision
  report goes to ``report_dir`` (``build_alias_index`` otherwise writes
  ``reports/cudl_alias_collisions.csv``);
* the answer does not depend on the machine: the per-user NLI cache that
  ``MetadataManager.__init__`` loads is replaced by an empty one.

A corpus below ``MIN_ROWS`` means the data is not there (a sparse checkout);
callers skip on that. Any other failure propagates, so a real-data gate fails
rather than skipping when the corpus is present.
"""
from pathlib import Path

MIN_ROWS = 100_000


def load_real_manager(report_dir):
    import shared.shelfmark_bridge as bridge
    from shared.metadata_manager import MetadataManager

    mm = MetadataManager()
    mm.nli_cache = type(mm.nli_cache)()
    original = bridge.build_alias_index
    report = Path(report_dir) / "cudl_alias_collisions.csv"

    def _redirected(csv_bank, report_path=None):
        return original(csv_bank, report_path=report)

    bridge.build_alias_index = _redirected
    try:
        mm._load_csv_bank()
    finally:
        bridge.build_alias_index = original
    return mm
