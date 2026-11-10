# atlas_bake package — Phase 133 (Visual Atlas Preview, ATLAS-01) bake
# invariant tests for scripts/build_atlas_asset.py.
#
# Requires the pinned bake-time deps in requirements-atlas-bake.txt
# (networkx/python-louvain/Brotli) -- NOT installed in the main `tests` CI
# job. Every test module here MUST `pytest.importorskip(...)` those deps
# before importing scripts.build_atlas_asset, so collection is a clean SKIP
# (not a hard error) when the main job runs `-m "not atlas_bake"`.
