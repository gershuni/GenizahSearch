---
status: resolved
trigger: "E2E tests fail with ModuleNotFoundError: No module named 'selenium' instead of skipping"
created: 2026-02-19T00:00:00Z
updated: 2026-02-19T00:00:00Z
---

## Current Focus

hypothesis: Module-level `from selenium.webdriver...` imports in test files execute before conftest.py skip logic runs
test: Read import order and pytest collection sequence
expecting: conftest.py pytest_collection_modifyitems runs AFTER module imports, confirming the race
next_action: Document root cause and recommended fix

## Symptoms

expected: Running `pytest tests/e2e/ -x -q` should skip all E2E tests gracefully when selenium is not installed
actual: Tests fail with `ModuleNotFoundError: No module named 'selenium'` at collection time
errors: ModuleNotFoundError: No module named 'selenium'
reproduction: `pytest tests/e2e/ -x -q` on a machine without selenium installed
started: Since E2E test infrastructure was added (phase 39-05)

## Eliminated

(none needed -- root cause identified on first hypothesis)

## Evidence

- timestamp: 2026-02-19
  checked: conftest.py skip mechanism
  found: conftest.py has TWO skip mechanisms -- (1) pytest_collection_modifyitems at line 38 that tries to import selenium and marks items for skip, and (2) a try/except ImportError block at line 57-202 that guards fixture definitions
  implication: The conftest.py skip logic is correctly designed but cannot help if test MODULE imports fail first

- timestamp: 2026-02-19
  checked: test_browse_flow.py line 14
  found: `from selenium.webdriver.common.by import By` at module top level, outside any try/except
  implication: This import executes during module collection, BEFORE pytest_collection_modifyitems runs

- timestamp: 2026-02-19
  checked: test_search_flow.py lines 17-19
  found: Three bare selenium imports: `from selenium.webdriver.common.by import By`, `from selenium.webdriver.common.keys import Keys`, `from selenium.common.exceptions import NoSuchElementException`
  implication: Same problem in this file

- timestamp: 2026-02-19
  checked: test_performance.py lines 15-16
  found: Two bare selenium imports: `from selenium.webdriver.common.by import By`, `from selenium.webdriver.common.keys import Keys`
  implication: Same problem in this file

- timestamp: 2026-02-19
  checked: pytest collection order
  found: pytest collection sequence is: (1) discover test files, (2) import test modules to find test items, (3) THEN run pytest_collection_modifyitems hook. Step 2 fails because selenium imports crash the module import.
  implication: pytest_collection_modifyitems NEVER gets a chance to run -- the ImportError happens during step 2

## Resolution

root_cause: The three E2E test files (test_browse_flow.py, test_search_flow.py, test_performance.py) have bare `from selenium.webdriver...` imports at module top level. When pytest collects these files, it must import them to discover test functions/classes. The module import fails with ModuleNotFoundError before pytest_collection_modifyitems ever runs. The conftest.py skip logic in pytest_collection_modifyitems is correct in principle but is bypassed because the test modules themselves cannot be imported.

fix: Use `pytest.importorskip("selenium")` at the top of each test file, or wrap the selenium imports in try/except blocks that set fallback values. See recommended fix below.

verification: (pending implementation)
files_changed: []
