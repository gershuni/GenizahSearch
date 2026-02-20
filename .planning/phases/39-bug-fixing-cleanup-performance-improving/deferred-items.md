# Deferred Items - Phase 39

## Pre-existing Test Failures (found during 39-06)

1. **test_msviewer_ktiv_button_exists** (`tests/test_desktop_folio_navigation.py:107`)
   - Asserts `border: 1.5px solid #4caf50` in genizah_app.py source
   - Button styling likely changed; test not updated

2. **test_suffixes_counted_in_explosion_guard** (`tests/test_responsa_core.py:643`)
   - Expects English "suffix" in warning message, but warnings are now in Hebrew

3. **test_prefix_plus_suffix_cascades_down_instead_of_error** (`tests/test_responsa_edge_cases.py:354`)
   - Same issue: expects English "suffix" or "Grammatical suffix" in Hebrew warning message
