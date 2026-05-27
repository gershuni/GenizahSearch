---
phase: 95
plan: 04
type: execute
wave: 1
depends_on: [01, 02]
files_modified:
  - shared/search_serializer.py
  - corrections_client.py
  - lists_sync.py
  - tests/test_local_namespace_no_api_leak.py
  - tests/test_local_namespace_no_corrections_leak.py
  - tests/test_local_namespace_no_lists_leak.py
autonomous: true
requirements: [REQ-9]
must_haves:
  truths:
    - "shared.search_serializer.serialize_search_payload drops any LOCAL row before serialization (REQ-9 defense-in-depth)"
    - "corrections_client returns 'local_corrections_disabled' error for LOCAL document_id WITHOUT making an HTTP call"
    - "lists_sync.sync_item_to_cloud returns False for LOCAL sys_id WITHOUT calling _get_client() or sync_list_to_cloud() (Codex D-30 P0)"
    - "lists_sync.sync_list_to_cloud returns False if ANY item belonging to the list has a LOCAL sys_id (Codex D-30 P0) — iterates self.lists_manager.data['items'] checking 'lists' membership"
    - "All three gates log at INFO level when triggered (expected behavior, not error)"
  artifacts:
    - path: "shared/search_serializer.py"
      provides: "LOCAL filter before _serialize_item (REQ-9 defense-in-depth)"
      contains: "is_local_sys_id"
    - path: "corrections_client.py"
      provides: "Extended SYNTH-06 gate with LOCAL OR-clause"
      contains: "local_corrections_disabled"
    - path: "lists_sync.py"
      provides: "Top-of-function LOCAL gates in sync_item_to_cloud + sync_list_to_cloud (D-30 P0)"
      contains: "local-only item, not synced"
  key_links:
    - from: "lists_sync.py:sync_item_to_cloud"
      to: "shared.local_sys_id.is_local_sys_id"
      via: "first-statement gate BEFORE self._get_client() or self.sync_list_to_cloud()"
      pattern: "is_local_sys_id"
    - from: "tests/test_local_namespace_no_lists_leak.py"
      to: "lists_sync.LocalListsManager._get_client"
      via: "MagicMock + assert call_count == 0"
      pattern: "_get_client"
---

<objective>
Implement and pin the three cloud-write boundaries (REQ-9) that prevent LOCAL sys_ids from EVER reaching cloud surfaces. Codex P0 finding: the original CONTEXT proposed "gate at the natural sys_id lookup site" — that's TOO LATE in `lists_sync.sync_item_to_cloud` because `_get_client()` (line 742) and `sync_list_to_cloud(list_id)` (line 753) BOTH fire BEFORE the natural lookup at line 762. Gate MUST move to the TOP.

Three gates:
1. **`shared/search_serializer.py`** — filter LOCAL items BEFORE `_serialize_item` runs (REQ-9 defense-in-depth; web Tantivy has no LOCAL today but the helper is shared).
2. **`corrections_client.py`** — extend existing `is_synthetic_sys_id` gate at `:619-623` with parallel LOCAL OR-clause; distinct error code `local_corrections_disabled`.
3. **`lists_sync.py`** — INSERT gate as FIRST STATEMENT of `sync_item_to_cloud` (before `is_sync_available()` at line 738) AND at TOP of `sync_list_to_cloud()`. Lookup `item_data` from local `self.lists_manager.data` (in-memory only — no network).

**B2 RESOLUTION — `sync_list_to_cloud` item-iteration field name PINNED:**

The planner has READ `lists_sync.py:697-734` (`sync_list_to_cloud` body) and `:461-695` (`sync_to_cloud` body, which contains the canonical bulk-iteration pattern). Findings:

1. `sync_list_to_cloud(self, list_id)` at line 697 currently does NOT iterate items at all — it only syncs the list-level metadata to `user_lists` (lines 711-728). Items are NOT pushed by this function.

2. The canonical item iteration pattern is in `sync_to_cloud` at lines 619-635:
   ```python
   items = self.lists_manager.data.get('items', {})                   # line 619
   ...
   for item_id, item_data in items.items():                           # line 631
       if list_id not in item_data.get('lists', []):                  # line 632 — list membership check
           continue
       sys_id = item_data.get('sys_id', item_id)                      # line 635
   ```

3. Items are stored as a FLAT DICT at `self.lists_manager.data['items']`. Each item dict has a `'lists'` LIST field holding the list_ids it belongs to. The sys_id field is `'sys_id'` (fallback to `item_id`).

4. **Pinned gate for `sync_list_to_cloud`:** iterate `self.lists_manager.data.get('items', {}).items()`, check `if list_id in item_data.get('lists', [])` for membership, then check `is_local_sys_id(item_data.get('sys_id', item_id))`. NO call-your-own placeholder — exact field names pinned: `'items'` (dict), `'lists'` (list field), `'sys_id'` (string, fallback `item_id`).

Output: 3 modified files + 3 GREEN test files that mock the cloud client and assert ZERO calls when LOCAL sys_id is present.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/phases/95-my-library/95-CONTEXT.md
@.planning/phases/95-my-library/95-PATTERNS.md
@shared/local_sys_id.py
@shared/synthetic_sys_id.py
@shared/search_serializer.py
@corrections_client.py
@lists_sync.py

<interfaces>
<!-- Existing gate templates -->

From corrections_client.py:619-623 (existing SYNTH-06 gate — extend with LOCAL):
```python
# Phase 85 SYNTH-06 / D-10 — REVIEWS-MODE iteration 1 B1 gate.
# Reject synthetic sys_ids at the WRITE entry point.
if is_synthetic_sys_id(document_id):
    return (
        None,
        "synthetic_corrections_disabled: corrections cannot be added to synthetic sys_ids",
    )
```

From lists_sync.py:736-762 (sync_item_to_cloud body — Codex P0 placement target):
```python
def sync_item_to_cloud(self, item_id: str, list_id: str) -> bool:
    """Push a specific item to cloud."""
    if not self.is_sync_available():                # line 738 — gate must be BEFORE this
        return False
    try:
        client = self._get_client()                  # line 742 — LEAKS if gate placed at line 762
        if not client:
            return False
        list_data = self.lists_manager.data.get('lists', {}).get(list_id)
        if not list_data:
            return False
        cloud_list_id = list_data.get('cloud_id')
        if not cloud_list_id:
            self.sync_list_to_cloud(list_id)         # line 753 — ALSO LEAKS
            cloud_list_id = list_data.get('cloud_id')
            if not cloud_list_id:
                return False
        item_data = self.lists_manager.data.get('items', {}).get(item_id)
        if not item_data:
            return False
        sys_id = item_data.get('sys_id', item_id)    # line 762 — WRONG gate placement (too late)
        ...
```

From lists_sync.py:697-734 (sync_list_to_cloud body — confirmed: does NOT iterate items):
```python
def sync_list_to_cloud(self, list_id: str) -> bool:
    """Push a specific list and its items to cloud."""
    if not self.is_sync_available():
        return False

    try:
        client = self._get_client()                   # line 703 — would LEAK without gate
        if not client:
            return False

        list_data = self.lists_manager.data.get('lists', {}).get(list_id)
        if not list_data or list_data.get('is_system'):
            return False
        # ... only list-level metadata sync follows; NO item iteration here.
```

From lists_sync.py:619-635 (sync_to_cloud — canonical item-iteration pattern; field names PINNED from this):
```python
items = self.lists_manager.data.get('items', {})                          # line 619
# ... per-list loop ...
for item_id, item_data in items.items():                                   # line 631
    if list_id not in item_data.get('lists', []):                          # line 632 — list membership
        continue
    sys_id = item_data.get('sys_id', item_id)                              # line 635
```

From shared/search_serializer.py:565-580 (filter site for /api/search payload):
```python
items = [
    _serialize_item(
        r,
        meta_mgr=meta_mgr,
        domain_batch=domain_batch,
        catalog_batch=catalog_batch,
        transcription_sys_ids=transcription_sys_ids,
        printed_sys_ids=printed_sys_ids,
    )
    for r in results
]
```
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Insert LOCAL gates at TOP of sync_item_to_cloud and sync_list_to_cloud (D-30 Codex P0 + HIGH-2 review fix) — field names PINNED (B2)</name>
  <read_first>
    - lists_sync.py:697-734 (sync_list_to_cloud — confirmed: no item iteration)
    - lists_sync.py:619-635 (sync_to_cloud — canonical iteration pattern; PIN source for the gate)
    - lists_sync.py:736-770 (sync_item_to_cloud body — Codex P0 placement target)
    - .planning/phases/95-my-library/95-PATTERNS.md ("lists_sync.py modifications (D-30 Codex P0)")
    - .planning/phases/95-my-library/95-CONTEXT.md (D-30 Codex revision — the FOUR-STEP gate)
    - .planning/phases/95-my-library/95-REVIEWS.md (HIGH-2 finding: sys_id derivation must run BEFORE the `if item_data:` branch so a LOCAL `item_id` with missing `item_data` is ALSO gated)
  </read_first>
  <behavior>
    Test `test_sync_item_to_cloud_zero_get_client_calls_for_local`:
    - Patch `LocalListsManager._get_client` with `MagicMock(return_value=None)`.
    - Patch `LocalListsManager.sync_list_to_cloud` with `MagicMock(return_value=False)`.
    - Add to `manager.lists_manager.data['items']` a fake item: `{'sys_id': '970012345601234567', 'lists': ['fake-list-id'], 'fl_id': '...', ...}`.
    - Call `sync_item_to_cloud(item_id='fake-item-id', list_id='fake-list-id')`.
    - Assert return value is `False`.
    - Assert `_get_client.call_count == 0` (the load-bearing assertion — without P0 fix this would be ≥ 1).
    - Assert `sync_list_to_cloud.call_count == 0` (same reason).
    - Verify the gate logged at INFO level: `caplog.text` contains `"local-only item, not synced"`.

    Test `test_sync_list_to_cloud_aborts_if_any_item_local`:
    - Patch `_get_client` + Supabase calls.
    - Add 2 items to `lists_manager.data['items']` (the flat items dict per the B2-pinned schema):
      - Genizah item: `{'sys_id': '990025143260205171', 'lists': ['fake-list-id']}`
      - LOCAL item: `{'sys_id': '970012345601234567', 'lists': ['fake-list-id']}`
    - Add a list at `lists_manager.data['lists']['fake-list-id']` with any non-system metadata.
    - Call `sync_list_to_cloud(list_id='fake-list-id')`.
    - Assert return value is `False`.
    - Assert `_get_client.call_count == 0`.
    - Verify log: `"list contains LOCAL items, not synced"`.

    Test `test_sync_list_to_cloud_no_local_items_proceeds`:
    - REGRESSION: items dict contains ONLY Genizah sys_ids associated with `'fake-list-id'`. Gate must NOT short-circuit; existing flow continues.

    Test `test_sync_item_to_cloud_synthetic_unchanged`:
    - REGRESSION: synthetic 99-prefix sys_id (`'990001234560000000'`) should NOT trigger the LOCAL gate. The natural lookup at line 762 (or wherever the existing flow ended up) still applies.
    - Assert function does NOT short-circuit at the LOCAL gate (set up the rest of the mocks so the call proceeds further).

    Test `test_sync_item_to_cloud_missing_item_data_non_local_item_id`:
    - HIGH-2 review fix — this is the non-LOCAL "missing item_data" path; asserts the function does NOT regress in the OTHER direction.
    - `item_id="some-non-local-id"` not in `lists_manager.data['items']`. The gate's lookup yields None. After HIGH-2 fix the function still derives `sys_id = item_id`, but `is_local_sys_id("some-non-local-id")` is False, so the gate does NOT short-circuit.
    - Function should NOT crash; should proceed to existing flow (which will eventually return False for OTHER reasons — `_get_client` may be called, `sync_list_to_cloud` may be called, etc.).
    - Assert no exceptions raised.
    - Assert the LOCAL-gate `return False` was NOT taken — the function reached at least `is_sync_available()`.

    Test `test_sync_item_to_cloud_local_item_id_missing_data`:
    - **HIGH-2 review fix — NEW LOAD-BEARING TEST.** This pins the HIGH-2 regression: when `item_data` is None AND `item_id` itself is a LOCAL sys_id, the function MUST still short-circuit BEFORE any cloud touch.
    - Patch `LocalListsManager._get_client` with `MagicMock(return_value=None)`.
    - Patch `LocalListsManager.sync_list_to_cloud` with `MagicMock(return_value=False)`.
    - Patch Supabase client construction (any cloud-touching method on the manager) as a MagicMock recording call_count.
    - Do NOT add anything to `manager.lists_manager.data['items']` — the lookup MUST return None.
    - Call `sync_item_to_cloud(item_id="970012345601234567", list_id="fake-list-id")` (LOCAL sys_id supplied as the item_id itself, no item_data).
    - Assert return value is `False`.
    - Assert `_get_client.call_count == 0` (LOAD-BEARING — without the HIGH-2 fix, the function would fall through `if item_data:` and reach `_get_client()` because `is_sync_available()` returns True under the test fixture).
    - Assert `sync_list_to_cloud.call_count == 0`.
    - Assert NO method on the mocked Supabase client was called (e.g., `.from_().insert().execute()` chain — verify via the mock's recorded calls).
    - Verify the gate logged at INFO level: `caplog.text` contains `"local-only item, not synced"` with the LOCAL sys_id.
    - Why: HIGH-2 fix — LOCAL items must never touch the cloud even when `item_data` is missing from the in-memory store (e.g., race condition where the item was removed locally between the caller looking it up and `sync_item_to_cloud` running).
  </behavior>
  <action>
    1. Locate `def sync_item_to_cloud(self, item_id: str, list_id: str) -> bool:` in `lists_sync.py` (verified line 736). Insert the LOCAL gate AS THE FIRST STATEMENTS of the function body, BEFORE `if not self.is_sync_available():`.

    **HIGH-2 REVIEW FIX (load-bearing):** The `sys_id` derivation MUST run BEFORE the `if item_data:` branch so that when `item_data` is None and `item_id` itself is a LOCAL sys_id, the gate STILL fires. The previous draft hid the `sys_id` derivation INSIDE `if item_data:` — that path let a LOCAL `item_id` with missing `item_data` slip past the gate into the cloud flow.

    Per CONTEXT D-30 Codex revision four-step protocol + HIGH-2 fix:
    ```python
    def sync_item_to_cloud(self, item_id: str, list_id: str) -> bool:
        """Push a specific item to cloud."""
        # ===== Phase 95 LOCAL gate (D-30 Codex P0 + HIGH-2 review fix, REQ-9) =====
        # MUST run BEFORE _get_client() and sync_list_to_cloud() — both leak
        # cloud activity even though the natural sys_id lookup is at line ~762.
        # HIGH-2: derive sys_id BEFORE the `if item_data:` branch so a LOCAL
        # item_id with missing item_data is ALSO gated (the previous draft
        # nested the derivation INSIDE the `if item_data:` body which let this
        # case slip through).
        # Lookup from in-memory self.lists_manager.data only (no network).
        item_data = self.lists_manager.data.get('items', {}).get(item_id)
        sys_id = item_data.get('sys_id', item_id) if item_data else item_id
        if is_local_sys_id(sys_id):
            logger.info("[local-only item, not synced] item_id=%s sys_id=%s", item_id, sys_id)
            return False
        # ===========================================================================
        if not self.is_sync_available():   # existing body continues unchanged
            return False
        try:
            ...
    ```

    **Sequence requirements (the executor MUST preserve exactly):**
    - LINE 1 of the function body (after the docstring): `item_data = self.lists_manager.data.get('items', {}).get(item_id)`.
    - LINE 2: `sys_id = item_data.get('sys_id', item_id) if item_data else item_id` (the HIGH-2 line).
    - LINE 3: `if is_local_sys_id(sys_id):` followed by the logger.info + `return False`.
    - There MUST be NO `if item_data:` branch wrapping the `sys_id` derivation — flatten the sequence so `sys_id` is always defined and `is_local_sys_id(sys_id)` is always evaluated, regardless of whether `item_data` is None.
    - The pre-existing `if not self.is_sync_available()` line is the FIRST cloud-touching call and MUST come AFTER the gate above.

    2. Add the import at module top (after existing imports):
    ```python
    from shared.local_sys_id import is_local_sys_id
    ```

    3. Locate `def sync_list_to_cloud(self, list_id: str) -> bool:` at line 697. Insert the LOCAL gate as the FIRST STATEMENTS of its body. **B2 RESOLUTION — exact field names pinned from `sync_to_cloud:619-635`:**

    ```python
    def sync_list_to_cloud(self, list_id: str) -> bool:
        """Push a specific list and its items to cloud."""
        # ===== Phase 95 LOCAL gate (D-30 Codex P0, REQ-9) =====
        # Abort entire list sync if any item belonging to this list has a LOCAL sys_id.
        # B2 — field names pinned from sync_to_cloud:619-635 canonical pattern.
        # Items are stored as a flat dict at self.lists_manager.data['items'].
        # Each item dict has a 'lists' list field holding the list_ids it belongs to.
        # The sys_id is in 'sys_id' (fallback item_id).
        items_map = self.lists_manager.data.get('items', {})
        for iid, item_data in items_map.items():
            if list_id not in (item_data.get('lists') or []):
                continue  # item not in this list
            if is_local_sys_id(item_data.get('sys_id', iid)):
                logger.info("[list contains LOCAL items, not synced] list_id=%s", list_id)
                return False
        # ======================================================
        if not self.is_sync_available():   # existing body continues unchanged (line 699)
            return False
        # ... rest of existing function body unchanged
    ```

    **NO planner-must-read placeholder. The field names are PINNED:**
    - Items collection: `self.lists_manager.data.get('items', {})` (flat dict keyed by `item_id`)
    - List membership field on each item: `item_data.get('lists', [])` (list of list_ids)
    - Sys_id field on each item: `item_data.get('sys_id', iid)` (string, fallback to item_id)

    4. Ensure `logger` is imported in `lists_sync.py` (already present per line 733: `logger.error(f"Error syncing list to cloud: {e}")` — verify via `grep -n "^import logging\\|^logger" lists_sync.py | head`).
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_namespace_no_lists_leak.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "Phase 95 LOCAL gate" lists_sync.py` returns ≥ 2 (one per function).
    - `grep -c "from shared.local_sys_id import is_local_sys_id" lists_sync.py` returns 1.
    - `grep -c "local-only item, not synced\\|list contains LOCAL items" lists_sync.py` returns ≥ 2.
    - The exact iteration uses `self.lists_manager.data.get('items', {})` AND `item_data.get('lists')`. Verify: `grep -c "item_data.get('lists'" lists_sync.py` returns ≥ 1 (the new gate).
    - The LOCAL gate appears BEFORE `_get_client()` and `sync_list_to_cloud(` calls. Verify by reading the function bodies — the FIRST executable statement after the docstring is the `item_data = ...` lookup, followed by `sys_id = ...` derivation, followed by `if is_local_sys_id(sys_id):` gate, followed by `return False`. NO cloud-touching call appears before the gate.
    - HIGH-2 review fix — the sys_id derivation runs BEFORE (and OUTSIDE of) any `if item_data:` branch: read the source body and confirm the sequence is `item_data = self.lists_manager.data.get('items', {}).get(item_id)` then `sys_id = item_data.get('sys_id', item_id) if item_data else item_id` then `if is_local_sys_id(sys_id):`. Static check: `grep -c "sys_id = item_data.get(.sys_id., item_id) if item_data else item_id" lists_sync.py` returns 1.
    - HIGH-2 review fix — line-order static check: the `sys_id = item_data.get(...) if item_data else item_id` line MUST appear BEFORE the FIRST `if item_data:` line in `lists_sync.py`. Verify by comparing line numbers from two greps: `grep -nE "sys_id = item_data\.get\(.*\) if item_data else item_id" lists_sync.py | head -1` returns a line number STRICTLY LESS THAN the line number from `grep -nE "^[[:space:]]*if item_data:" lists_sync.py | head -1` (use the FIRST occurrence of each). If `if item_data:` does not appear at all (the executor flattened it away), that ALSO satisfies the AC.
    - HIGH-2 review fix — load-bearing test: `python -m pytest tests/test_local_namespace_no_lists_leak.py::test_sync_item_to_cloud_local_item_id_missing_data -x -q` exits 0. The test asserts `_get_client.call_count == 0` when `item_data` is None AND `item_id="970012345601234567"`.
    - HIGH-2 review fix — the regression test for the non-LOCAL path also passes: `python -m pytest tests/test_local_namespace_no_lists_leak.py::test_sync_item_to_cloud_missing_item_data_non_local_item_id -x -q` exits 0. (Verifies the gate did NOT over-fire — non-LOCAL item_id with missing item_data still proceeds to the existing flow.)
    - `python -m pytest tests/test_local_namespace_no_lists_leak.py -x -q` exits 0 with all tests PASSED (including the new HIGH-2 tests).
    - The critical assertion `_get_client.call_count == 0` passes for BOTH item_data-present AND item_data-absent LOCAL paths.
    - REGRESSION: `python -m pytest tests/ -k "lists_sync or sync_item or sync_list" -x -q` exits 0.
    - `python -m ruff check lists_sync.py tests/test_local_namespace_no_lists_leak.py` exits 0.
  </acceptance_criteria>
  <done>Both functions gated at top BEFORE any cloud call; sys_id derivation handles missing item_data per HIGH-2 review fix (flattened — no `if item_data:` wrapper around the derivation); iteration uses pinned field names `data['items']` + `item_data['lists']` + `item_data['sys_id']`; tests assert zero `_get_client` invocations for LOCAL sys_ids in BOTH item_data-present and item_data-absent scenarios.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Extend corrections_client.py gate with parallel LOCAL OR-clause</name>
  <read_first>
    - corrections_client.py:619-623 (existing SYNTH-06 gate)
    - .planning/phases/95-my-library/95-PATTERNS.md ("corrections_client.py modifications (REQ-9)" — extend not replace)
    - .planning/phases/95-my-library/95-CONTEXT.md (REQ-9: distinct error code `local_corrections_disabled`)
  </read_first>
  <behavior>
    Test `test_corrections_submit_returns_local_corrections_disabled`:
    - Mock the HTTP client (requests/Supabase) — provide a MagicMock for whatever the existing test pattern uses.
    - Call the corrections submit path (probably `submit_correction(document_id="970012345601234567", ...)` or equivalent — exact function name from corrections_client).
    - Assert the return value's error code is `"local_corrections_disabled"` (exact string per REQ-9 acceptance).
    - Assert the HTTP mock was NEVER called (assert `mock_client.call_count == 0` AND no `requests.post` / `requests.get` invocation).

    Test `test_corrections_submit_synthetic_still_disabled`:
    - REGRESSION: synthetic sys_id `"990001234560000000"` still returns `"synthetic_corrections_disabled"` (NOT the new LOCAL code).

    Test `test_corrections_submit_real_alma_still_passes_gate`:
    - REGRESSION: real Alma sys_id (e.g., `"990025143260205171"`) passes both gates and reaches the existing flow.
  </behavior>
  <action>
    1. In `corrections_client.py`, add the import at module top (after existing `from shared.synthetic_sys_id import is_synthetic_sys_id`):
    ```python
    from shared.local_sys_id import is_local_sys_id
    ```

    2. Locate the existing gate at `:619-623`:
    ```python
    if is_synthetic_sys_id(document_id):
        return (
            None,
            "synthetic_corrections_disabled: corrections cannot be added to synthetic sys_ids",
        )
    ```

    3. ADD (do NOT merge into a single OR) a parallel LOCAL gate IMMEDIATELY AFTER the synthetic gate:
    ```python
    if is_synthetic_sys_id(document_id):
        return (
            None,
            "synthetic_corrections_disabled: corrections cannot be added to synthetic sys_ids",
        )
    # Phase 95 REQ-9 — LOCAL sys_ids never reach the corrections cloud surface.
    if is_local_sys_id(document_id):
        return (
            None,
            "local_corrections_disabled: corrections cannot be added to LOCAL sys_ids",
        )
    ```

    DO NOT collapse into a single `if is_synthetic_sys_id(...) or is_local_sys_id(...):` — the error code must differ (REQ-9 acceptance pins `local_corrections_disabled` distinctly).
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_namespace_no_corrections_leak.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "local_corrections_disabled" corrections_client.py` returns ≥ 1.
    - `grep -c "synthetic_corrections_disabled" corrections_client.py` returns ≥ 1 (unchanged).
    - `grep -c "from shared.local_sys_id import is_local_sys_id" corrections_client.py` returns 1.
    - `python -m pytest tests/test_local_namespace_no_corrections_leak.py -x -q` exits 0 with all tests PASSED.
    - `python -m pytest tests/ -k "corrections_client or correction_submit" -x -q` exits 0 (no regressions).
    - The synthetic gate is UNCHANGED — `grep -A 3 "synthetic_corrections_disabled" corrections_client.py` shows the original 4-line block intact.
    - `python -m ruff check corrections_client.py tests/test_local_namespace_no_corrections_leak.py` exits 0.
  </acceptance_criteria>
  <done>corrections_client extended with parallel LOCAL gate; tests green; error code matches REQ-9 acceptance.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Add LOCAL filter to shared/search_serializer.py (REQ-9 defense-in-depth)</name>
  <read_first>
    - shared/search_serializer.py (locate `serialize_search_payload` and any `serialize_parallels_payload`)
    - .planning/phases/95-my-library/95-PATTERNS.md ("shared/search_serializer.py modifications (REQ-9 defense-in-depth)")
    - .planning/phases/95-my-library/95-CONTEXT.md (REQ-9: filter LOCAL items, drop them from /api/search payload)
  </read_first>
  <behavior>
    Test `test_serialize_search_payload_drops_local`:
    - Construct a `results` list with 3 fake search hits: 2 normal (Genizah V0.8 sys_ids), 1 LOCAL (`{'sys_id': '970012345601234567', 'display': {'source': 'LOCAL', 'library_code': 'LOCAL', 'id': '970012345601234567'}, ...}`).
    - Call `serialize_search_payload(results=..., ...)`.
    - Assert `len(envelope['results']) == 2` (LOCAL row dropped).
    - Assert no item in `envelope['results']` has `display.library_code == 'LOCAL'` or `display.source == 'LOCAL'` or a 97-prefix sys_id.

    Test `test_serialize_search_payload_no_local_unchanged`:
    - REGRESSION: when no LOCAL items present, output length == input length; no items dropped.

    Test `test_serialize_parallels_payload_drops_local` (if `serialize_parallels_payload` exists):
    - Same as above but for the parallels payload.
  </behavior>
  <action>
    1. In `shared/search_serializer.py`, add import at module top (alongside the existing `is_synthetic_sys_id` import around line 52):
    ```python
    from shared.local_sys_id import is_local_sys_id
    ```

    2. Define a helper function near the top (before `_serialize_item`):
    ```python
    def _is_local_item(result: dict) -> bool:
        """Return True if a result row is a LOCAL hit (REQ-9 defense-in-depth)."""
        display = result.get('display', {}) or {}
        sys_id = display.get('id', '') or result.get('sys_id', '') or ''
        library_code = display.get('library_code', '') or ''
        return library_code == 'LOCAL' or is_local_sys_id(sys_id)
    ```

    3. Locate `serialize_search_payload` — find the line `items = [_serialize_item(...) for r in results]` (around `:568`). Insert a filter step IMMEDIATELY BEFORE that listcomp:
    ```python
    # Phase 95 REQ-9 defense-in-depth — drop LOCAL items before serializing.
    # Web Tantivy has no LOCAL data today, but the helper is shared and a
    # belt-and-suspenders gate avoids future regressions.
    results = [r for r in results if not _is_local_item(r)]
    items = [
        _serialize_item(r, meta_mgr=..., ...)
        for r in results
    ]
    ```

    4. If `serialize_parallels_payload` (or equivalent) also constructs items from a `results` list, apply the same filter there. Run `grep -nE "def serialize_" shared/search_serializer.py` to find all candidates.
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_namespace_no_api_leak.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "def _is_local_item" shared/search_serializer.py` returns 1.
    - `grep -c "from shared.local_sys_id import is_local_sys_id" shared/search_serializer.py` returns 1.
    - `grep -c "REQ-9 defense-in-depth" shared/search_serializer.py` returns ≥ 1.
    - `python -m pytest tests/test_local_namespace_no_api_leak.py -x -q` exits 0 with all tests PASSED.
    - `python -m pytest tests/ -k "search_serializer or serialize_search or serialize_parallels" -x -q` exits 0 (no regressions).
    - Filter applied in `serialize_search_payload` AND any other serialize_* function with a `results` parameter.
    - W10 — verify NO unrecursive `grep -c "skip_local=True" web/` in any test description (use `grep -rc` if scanning a directory tree).
    - `python -m ruff check shared/search_serializer.py tests/test_local_namespace_no_api_leak.py` exits 0.
  </acceptance_criteria>
  <done>Serializer drops LOCAL items defensively; tests green; no regressions.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| LOCAL sys_id (in-memory) → `lists_sync._get_client()` (Supabase cloud) | **CRITICAL** — D-30 P0 fix moves gate to TOP of function before any cloud touch |
| LOCAL sys_id → `corrections_client` HTTP submit | Existing SYNTH-06 gate extended with parallel LOCAL OR-clause |
| LOCAL sys_id → `shared.search_serializer` payload | Defense-in-depth filter before `_serialize_item` |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-95-13 | Information disclosure | LOCAL sys_id leaks via `/api/search` JSON response to external HTTP consumers | mitigate | `shared/search_serializer.py:_is_local_item` filter drops the row pre-serialization; pinned by `tests/test_local_namespace_no_api_leak.py` (T1 from phase-level threat list) |
| T-95-14 | Information disclosure | LOCAL sys_id leaks via Lists sync to Supabase | mitigate | D-30 Codex P0 fix + HIGH-2 review fix — gate at TOP of `sync_item_to_cloud` AND `sync_list_to_cloud` BEFORE `_get_client()`; HIGH-2: sys_id derivation runs OUTSIDE the `if item_data:` branch (`sys_id = item_data.get('sys_id', item_id) if item_data else item_id`) so a LOCAL item_id with missing item_data is ALSO gated. Pinned by `tests/test_local_namespace_no_lists_leak.py::test_sync_item_to_cloud_zero_get_client_calls_for_local` AND `::test_sync_item_to_cloud_local_item_id_missing_data` (HIGH-2 load-bearing test). B2 — field names pinned from `sync_to_cloud:619-635` canonical pattern: `data['items']` flat dict, `item_data['lists']` membership, `item_data['sys_id']` |
| T-95-15 | Information disclosure | LOCAL sys_id leaks via corrections submit | mitigate | Existing SYNTH-06 gate extended; distinct error code `local_corrections_disabled` per REQ-9 acceptance; pinned by `tests/test_local_namespace_no_corrections_leak.py` (T3) |
| T-95-16 | Tampering | Future contributor adds new cloud-write surface without LOCAL gate | accept (partially mitigated) | Plan 09 adds `tests/test_web_library_options_no_local.py` static AST guard for `LIBRARY_CODES` consumers; cloud-write boundaries are a more open surface, but the helper `is_local_sys_id` is the single source of truth — code review catches new gates |
| T-95-17 | Repudiation | Silent gate trigger leaves no audit trail | mitigate | All three gates log at INFO level with sys_id; ops can grep logs for `"local-only"` / `"list contains LOCAL"` to verify gate triggered |
</threat_model>

<verification>
- `python -m pytest tests/test_local_namespace_no_api_leak.py tests/test_local_namespace_no_corrections_leak.py tests/test_local_namespace_no_lists_leak.py -x -q` exits 0.
- `python -m pytest tests/ -q` exits 0 (full suite — no regressions in corrections_client / lists_sync / search_serializer existing tests).
- `python -m ruff check shared/search_serializer.py corrections_client.py lists_sync.py tests/test_local_namespace_no_*.py` exits 0.
- The CRITICAL load-bearing assertion (`_get_client.call_count == 0` after `sync_item_to_cloud` with LOCAL sys_id) passes — verifying the Codex P0 fix is correctly placed.
- B2 — verify the `sync_list_to_cloud` gate iterates `data['items']` (flat dict), checks `item_data['lists']` membership, and reads `item_data['sys_id']`.
</verification>

<success_criteria>
- 3 cloud-write boundaries gated with `is_local_sys_id` check.
- `lists_sync.sync_item_to_cloud` gate is the FIRST STATEMENT of the function (BEFORE `is_sync_available()` at line 738) — verified by reading the source. The `sys_id` derivation runs OUTSIDE any `if item_data:` branch (HIGH-2 review fix).
- `lists_sync.sync_list_to_cloud` aborts entire list sync if ANY item belonging to the list has LOCAL sys_id. **B2 — iteration uses pinned field names: `data.get('items', {})` flat dict, `item_data.get('lists', [])` membership, `item_data.get('sys_id', iid)` lookup.**
- `corrections_client` returns `local_corrections_disabled` error code (distinct from `synthetic_corrections_disabled`).
- `shared/search_serializer.py` filters LOCAL items before `_serialize_item`.
- 3 Wave-0 stub files turned GREEN with the load-bearing zero-call assertions.
- All three gates log at INFO level on trigger.
- No regressions in existing corrections/lists/serializer test suites.
</success_criteria>

<output>
After completion, create `.planning/phases/95-my-library/95-04-SUMMARY.md` documenting:
- Exact line numbers where each gate was inserted in each file
- Confirmation that the `sync_list_to_cloud` gate iterates `data['items']` flat dict and checks `item_data['lists']` membership (B2 resolution)
- Confirmation that the `_get_client.call_count == 0` assertion passes for BOTH item_data-present AND item_data-absent LOCAL paths (HIGH-2 review fix)
</output>
