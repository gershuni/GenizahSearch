# Deferred Items - Phase 40

## Pre-existing Test Failures

1. `tests/test_responsa_core.py::TestApplyExplosionGuard::test_suffixes_counted_in_explosion_guard` - Asserts English "suffix" in Hebrew warning message
2. `tests/test_responsa_edge_cases.py::TestExplosionGuardEndToEnd::test_prefix_plus_suffix_cascades_down_instead_of_error` - Same root cause: asserts English text in Hebrew warning

**Root cause:** Warning messages were translated to Hebrew, but tests still assert English substrings. Not related to performance changes.
