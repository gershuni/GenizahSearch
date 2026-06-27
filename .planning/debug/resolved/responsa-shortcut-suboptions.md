---
status: diagnosed
trigger: "R+Space keyboard shortcut activates Responsa mode but doesn't show sub-options row"
created: 2026-02-10T00:00:00Z
updated: 2026-02-10T00:01:00Z
---

## Current Focus

hypothesis: CONFIRMED - shortcut handler sets mode_select.value programmatically, which does NOT trigger the on('update:model-value') event in NiceGUI, so on_mode_change() never runs
test: Compared three code paths: dropdown, shortcut, URL restoration
expecting: Shortcut handler missing visibility call
next_action: Report root cause

## Symptoms

expected: When user types "R " in the search field, Responsa mode activates AND sub-options row (Variants, JA, Flex Spacing) appears
actual: "R " activates Responsa mode but sub-options row stays hidden
errors: None (visual bug, not crash)
reproduction: Type "R " in web search field, observe sub-options row does not appear
started: Since Phase 15 added the R shortcut

## Eliminated

## Evidence

- timestamp: 2026-02-10T00:00:30Z
  checked: Shortcut handler (on_query_input_change) at lines 338-352
  found: Handler sets mode_select.value = target_mode (line 350) but does NOT call on_mode_change() or responsa_sub_row.set_visibility()
  implication: Programmatic .value assignment is the only action taken

- timestamp: 2026-02-10T00:00:35Z
  checked: on_mode_change handler at lines 573-600
  found: Contains responsa_sub_row.set_visibility(is_responsa) at line 594, registered via mode_select.on('update:model-value', on_mode_change) at line 600
  implication: This handler is only triggered by the Vue/Quasar update:model-value event

- timestamp: 2026-02-10T00:00:40Z
  checked: URL restoration code at lines 2989-3007
  found: After setting mode_select.value = 'responsa' (line 2991), it MANUALLY calls responsa_sub_row.set_visibility(True) (line 2992). Same pattern for pgp_tags at lines 3003-3006.
  implication: Developer already knew programmatic .value does NOT fire the event. Applied workaround in URL restoration but not in shortcut handler.

- timestamp: 2026-02-10T00:00:45Z
  checked: Whether on_mode_change is ever called explicitly
  found: on_mode_change appears only at line 573 (definition) and line 600 (event registration). Never called directly.
  implication: The only way on_mode_change runs is via the update:model-value event from user interaction with the dropdown

- timestamp: 2026-02-10T00:00:50Z
  checked: NiceGUI behavior for update:model-value on programmatic .value changes
  found: Known NiceGUI limitation - programmatic Python .value changes do NOT fire client-side update:model-value events. Only user-initiated UI interactions fire these events.
  implication: Confirms root cause - shortcut handler's mode_select.value assignment never triggers on_mode_change

## Resolution

root_cause: The shortcut handler on_query_input_change (line 338) sets mode_select.value programmatically (line 350), but in NiceGUI, programmatic .value assignment does NOT fire the 'update:model-value' event. Therefore on_mode_change() (line 573) never executes, and responsa_sub_row.set_visibility() (line 594) is never called. The dropdown works because user interaction with the select widget fires the Vue/Quasar event natively.
fix:
verification:
files_changed: []
