"""Baseline gate for the shared shelfmark resolver (catalogue-shelfmark plan, gate 2).

tests/fixtures/shelfmark_resolve_baseline.json was written by
scripts/build_shelfmark_resolve_baseline.py on master-main ed05058f, BEFORE the
catalogue-form fallback existed. The changed resolver must give:

* every answer that was exact (one record, or exact options) back identically,
  order and selected shelfmark included -- the fallback runs only after T1 found no
  exact match;
* every other answer identically, or now its source record, or a list that still
  contains its source record. A single record other than the source fails.

`slow`: CI's main job (which runs `slow` tests too) checks every simulated and
explicit input and every 10th stored variant; `pytest -m slow` or
GENIZAH_SHELFMARK_FULL=1 checks all 1,219.

The fixture is tied to the libraries.csv it was built from (sha256 in its header).
After a data refresh, regenerate it on purpose with the script; from this PR on it
records the combined resolver.

Regenerated on purpose for the library-prefix rule (docs/plans/SHELFMARK_PREFIX_SCOPE_PLAN.md):
only inputs whose prefix names a library changed, each reviewed with
scripts/diff_shelfmark_resolve_baseline.py against the previous fixture.
"""
import hashlib
import json
import os
from pathlib import Path

import pytest

from tests.shelfmark_corpus import MIN_ROWS, load_real_manager

REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "shelfmark_resolve_baseline.json"
# Explicit inputs that change on purpose, with the record they must now reach (or list).
EXPLICIT_SOURCES = {
    'Cambridge University Library, Ms. T-S K 08.026': '990051208260205171',
    'Mosseri, Jacques, Ms. VIII.402': '990053809560205171',
}


def _load():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def test_fixture_belongs_to_this_libraries_csv():
    header = _load()["header"]
    assert header["libraries_csv_sha256"] == _sha256(REPO / "libraries.csv"), (
        "libraries.csv changed since the baseline was recorded: regenerate it with "
        "`python scripts/build_shelfmark_resolve_baseline.py` and review the diff")


@pytest.fixture(scope="module")
def mm(tmp_path_factory):
    m = load_real_manager(tmp_path_factory.mktemp("cudl_report"))
    if len(m.csv_bank) < MIN_ROWS:
        pytest.skip(f"libraries.csv has only {len(m.csv_bank)} rows (corpus not present)")
    return m


@pytest.mark.slow
def test_resolver_holds_the_baseline(mm, request):
    full = (os.environ.get('GENIZAH_SHELFMARK_FULL') == '1'
            or (request.config.option.markexpr or '').strip() == 'slow')
    entries = _load()["entries"]
    stored = [e for e in entries if e["kind"] == "stored"]
    others = [e for e in entries if e["kind"] != "stored"]
    selected = others + (stored if full else stored[::10])
    failures = []
    for e in selected:
        r = mm.resolve_system_by_shelfmark(e["query"], limit=e["limit"])
        got = {"sys_id": r.get("sys_id"), "options": [o["sys_id"] for o in r.get("options") or []],
               "selected_shelfmark": r.get("selected_shelfmark")}
        base = e["response"]
        if got == base:
            continue
        if e["exact"]:
            failures.append(("exact answer changed", e["query"], base, got))
            continue
        source = e["source"] or EXPLICIT_SOURCES.get(e["query"])
        if not source:
            failures.append(("changed, no source to check against", e["query"], base, got))
        elif got["sys_id"] and got["sys_id"] != source:
            failures.append(("now a single OTHER record", e["query"], source, got))
        elif not got["sys_id"] and source not in got["options"]:
            failures.append(("list lost its source record", e["query"], source, got))
    assert not failures, failures[:10]
    assert len(selected) >= (len(entries) if full else 300)
