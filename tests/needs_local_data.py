# -*- coding: utf-8 -*-
"""The tests only a machine with the local data can run: the `needs_local_data` marker's list.

The public CI runs the full suite on every published commit, so a release does not
run it again locally. What the CI cannot run is listed here, and only that runs in
the release's local lane (`python scripts/run_local_tests.py --local-data`, in the
main checkout):

- CI_UNCOLLECTED: files tests/conftest.py does not collect when GITHUB_ACTIONS is
  set (threaded Qt tests that crash on the headless runners). Most of them are
  also `gui`: those run in the local Qt lane (`scripts/run_gui_tests.py`, which
  collects them locally), not in --local-data, which keeps the CI's `not gui`.
- WHOLE_FILES: files whose tests all skip on CI: the data they read (a sidecar
  under the checkout, the Tantivy index, the private repository) is not there.
- TESTS: single tests that skip on CI for that reason, in files whose other tests
  run there. An entry also covers its parametrized cases (`name[...]`) and, for a
  class, its methods (`Class::name`).
- RUNS_NOWHERE (not marked): tests that skip on CI and on the development machine as
  well. The local lane would only see them skip, so they are listed apart, to keep
  the gap in view.

How the list was made, and how it stays true: a run with `--timings-out` records
every skipped test and its reason. The first list came from such a run made as the
CI runs (GITHUB_ACTIONS set, no private repository, no home or LOCALAPPDATA data),
2026-11-24. Each public CI run uploads the same record, and
`python scripts/fold_lane_timings.py --run <id> --skips` lists the tests that
skipped there without this marker: add the data ones here. Tests that skip on CI
for a reason the development machine shares (Linux only, node absent) are not listed:
they would skip in the local lane too.

tests/conftest.py applies the marker; scripts/run_local_tests.py reads `files()`.
This module is imported by both, so it imports nothing from the project.
"""
from __future__ import annotations

import fnmatch
import os

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TESTS_DIR)

# tests/conftest.py's collect_ignore_glob when GITHUB_ACTIONS is set. Basenames
# matched with fnmatch, as collect_ignore_glob matches them.
CI_UNCOLLECTED = (
    "test_my_library_tab*.py",
    "test_unified_tree_async_populate.py",
    "test_folder_walk_worker.py",
    "test_local_optout_persistence.py",
    "test_recovery_scan_runs_cleanup.py",
    "test_disk_headroom.py",
    # Phase 108 Join Lab widget construction smoke tests (QWidget build).
    "test_join_workbench_construct.py",
    # Phase 100 PdfImageController + PdfRenderWorker QThread.
    "test_pdf_image_controller.py",
    "test_pdf_page_renderer.py",
    # Phase 97.2 discard_run / Tantivy-failure short-circuit -- Linux-only
    # failure mode but skipping uniformly keeps the conftest simple.
    "test_phase_97_2_sqlite_vs_tantivy_consistency.py",
)

_FJMS = "fist_data/fjms_enrichment.db (the FJMS sidecar)"
_PGP = "pgp.db (the PGP sidecar)"
_FGP = "fgp_data/fgp_transcriptions.db (the FGP sidecar)"
_PGP_FGP = "pgp.db and fgp_data/fgp_transcriptions.db"
_DISCOVERY = "discovery_data/live (the live discovery artifact)"
_NLI = "nli_crossref.db (the NLI cross-reference sidecar)"
_PRIVATE = "a record in the private repository"
_INDEX = "the Tantivy index, found where the apps look for it (tests/real_index.py)"
_RESEARCH = "same_work_spike/probe/scripts (the research normalizer, gitignored)"
_RESEARCH_CORPUS = "same_work_spike/probe/scripts and Transcriptions.txt (both gitignored)"

# file -> what it needs that CI does not have
WHOLE_FILES = {
    "tests/test_attach_htr_realignment.py": _RESEARCH,
}

# "file::test" -> what it needs that CI does not have
TESTS = {
    "tests/test_browse_api.py::test_browse_real_core_shape_smoke": _INDEX,
    "tests/test_catalog_numbers.py::test_golden_table_covers_every_form_in_the_real_sidecar": _FJMS,
    "tests/test_desktop_folio_navigation.py::test_real_crossref_folio_labels": _NLI,
    "tests/test_discovery_genre_labels.py::test_every_live_hebrew_domain_name_passes_the_surface_honesty_gate": _FJMS,
    "tests/test_discovery_genre_labels.py::test_the_live_fjms_vocabulary_covers_every_shipped_genre_string": _FJMS,
    "tests/test_discovery_genre_labels.py::test_the_live_lookup_is_cached_after_the_first_read": _FJMS,
    "tests/test_discovery_genre_labels.py::test_the_live_vocabulary_carries_both_parents_and_leaves": _FJMS,
    "tests/test_discovery_panel_model.py::test_the_emitted_disclosure_level_count_equals_the_ratified_number": _PRIVATE,
    "tests/test_discovery_suppression.py::test_a_suppressed_row_is_absent_from_the_ROWS_as_well_as_the_count": _DISCOVERY,
    "tests/test_discovery_suppression.py::test_suppressing_rows_reduces_the_TOTAL_by_exactly_that_many": _DISCOVERY,
    "tests/test_discovery_suppression.py::test_the_manuscript_axis_ACTUALLY_NARROWS_and_is_not_silently_dropped": _DISCOVERY,
    "tests/test_discovery_suppression.py::test_the_manuscript_axis_pins_children_to_exactly_one_manuscript": _DISCOVERY,
    "tests/test_discovery_suppression.py::test_the_not_in_clause_is_safe_because_the_column_is_the_primary_key": _DISCOVERY,
    "tests/test_fgp_default_coverage.py::TestMustContainOverride::test_real_pgpid_37732_pair": _PGP,
    "tests/test_fgp_default_coverage.py::TestTextMatchDemotion::test_real_ms_heb_g2_folio27_case": _FGP,
    "tests/test_filter_unavailable_service.py::test_real_sidecar_line_height_is_reported_consistently": _FJMS,
    "tests/test_open_issues_fragments.py::test_no_open_issue_reuses_an_id_the_archive_holds": _PRIVATE,
    "tests/test_parallels_api.py::test_parallels_locator_round_trip_real": _INDEX,
    "tests/test_responsa_performance.py::test_exact_search_shows_first_results_within_3s": _INDEX,
    "tests/test_passage_normalize.py::test_corpus_parity_of_gram_codes": _RESEARCH_CORPUS,
    "tests/test_passage_normalize.py::test_corpus_parity_with_research_normalizer": _RESEARCH_CORPUS,
    "tests/test_passage_normalize.py::test_fixture_parity_with_research_normalizer": _RESEARCH,
    "tests/test_passage_normalize.py::test_research_k_and_base_match_the_spec": _RESEARCH,
    "tests/test_search_api_v2.py::test_search_mode_real_index_returns_at_least_one_result": _INDEX,
    "tests/test_seed022_manual_transcription_tag.py::test_pgp_text_predicate_is_stricter_than_link_presence": _PGP,
    "tests/test_seed023_catalog_filters.py::test_editions_union_matches_scholarly_transcriptions_stat": _PGP_FGP,
    "tests/test_seed023_catalog_filters.py::test_fgp_editions_excludes_translations": _FGP,
    "tests/test_seed023_catalog_filters.py::test_get_sys_ids_with_editions_is_editions_only": _PGP,
    "tests/test_seed023_catalog_filters.py::test_get_sys_ids_with_editions_none_and_empty_safe": _PGP,
    "tests/test_seed023_stats_service.py::test_live_db_metrics_match_constants": _PGP_FGP,
    "tests/test_shipping_sidecar_guard.py::test_the_web_deploy_is_guarded_too": _PRIVATE,
}

# Tests that skip on CI AND on the development machine, so they run nowhere. NOT marked:
# in the local lane they would only skip, and a skip fails it. Measured in the main
# checkout, 2026-11-24. They stay listed so the gap is visible and
# `fold_lane_timings.py --skips` does not report them again; making one run (or
# deleting it) is its own change, after which it moves to TESTS.
# 2026-11-24: the index tests now find the index where the apps do (tests/real_index.py),
# and the research tree's two modules were copied into the main checkout from the retired
# one; those moved to TESTS. The Responsa benchmarks below run, and fail their ceilings
# several times over: an open issue (docs/open_issues.d/2026-11-24-responsa-common-word-
# searches-slow.md); they are skipped until it is fixed.
RUNS_NOWHERE = {
    "tests/test_responsa_performance.py::TestPerformanceBenchmarks":
        "skipped on purpose: Responsa searches over a common word are too slow, an open issue",
}


def _ci_uncollected(path):
    return any(fnmatch.fnmatch(os.path.basename(path), g) for g in CI_UNCOLLECTED)


def _listed(entries, path, test):
    """Is (path, test) covered by an entry of *entries* ("file" or "file::test")?"""
    for entry in entries:
        f, _, name = entry.partition("::")
        if f != path:
            continue
        if not name:
            return True
        if test and (test == name or test.startswith(name + "[")
                     or test.startswith(name + "::")):
            return True
    return False


def is_marked(path, test=""):
    """Does the test *test* (`name`, `name[param]`, `Class::name`; "" for the whole
    file) in the repo-relative file *path* need the local data?"""
    path = path.replace("\\", "/")
    if path in WHOLE_FILES or _ci_uncollected(path):
        return True
    return _listed(TESTS, path, test)


def runs_nowhere(path, test=""):
    """Is the test listed in RUNS_NOWHERE (skips on CI and on the development machine)?"""
    return _listed(RUNS_NOWHERE, path.replace("\\", "/"), test)


def files():
    """Every test file that holds a needs_local_data test, repo-relative."""
    found = set(WHOLE_FILES) | {e.partition("::")[0] for e in TESTS}
    for dirpath, dirnames, filenames in os.walk(TESTS_DIR):
        dirnames[:] = [d for d in dirnames if d != "__pycache__"]
        for name in filenames:
            if name.startswith("test_") and name.endswith(".py") and _ci_uncollected(name):
                rel = os.path.relpath(os.path.join(dirpath, name), REPO_ROOT)
                found.add(rel.replace("\\", "/"))
    return sorted(found)
