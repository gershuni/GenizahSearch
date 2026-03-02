---
status: awaiting_human_verify
trigger: "Sub-sub-domain was added to the domain hierarchy, but the Select All / Select None buttons in the desktop domain filter only toggle the first two levels (domain, sub-domain). Sub-sub-domains are not affected."
created: 2026-03-01T00:00:00Z
updated: 2026-03-01T00:00:00Z
---

## Current Focus

hypothesis: CONFIRMED - _check_all, _uncheck_all, _handle_item_changed, _restore_exclusions, and get_excluded_domains all only iterate 2 levels (parent + child) and never recurse into 3rd-level children.
test: Read code at lines 5042-5133 of genizah_app.py
expecting: Missing iteration over child_item.child(k) sub-sub-domain items
next_action: Apply fix to all 5 methods

## Symptoms

expected: Select All / Select None in the desktop domain filter should check/uncheck all three levels: domain, sub-domain, AND sub-sub-domain
actual: Select All / Select None only toggles the first two levels (domain and sub-domain). Sub-sub-domains remain unchanged.
errors: No error messages - logic/UI bug
reproduction: Open desktop app -> domain filter -> click Select All or Select None -> sub-sub-domain checkboxes don't change
started: Since sub-sub-domain support was added

## Eliminated

## Evidence

- timestamp: 2026-03-01T00:01:00Z
  checked: DomainFilterDialog._populate_tree (lines 4944-5020)
  found: Tree IS correctly populated with 3 levels (parent -> child -> sub-child at lines 4993-5008)
  implication: The tree structure is correct; the bug is in the methods that iterate over tree items

- timestamp: 2026-03-01T00:02:00Z
  checked: _check_all (lines 5060-5070), _uncheck_all (lines 5072-5082)
  found: Both only iterate 2 levels: root.child(i) -> parent_item.child(j). No 3rd-level loop.
  implication: CONFIRMED BUG - sub-sub-domains are never toggled by Select All / Select None

- timestamp: 2026-03-01T00:02:30Z
  checked: _handle_item_changed (lines 5042-5058)
  found: Only propagates parent->child (1 level down), not parent->child->grandchild
  implication: Clicking a parent checkbox doesn't propagate to sub-sub-domains either

- timestamp: 2026-03-01T00:03:00Z
  checked: _restore_exclusions (lines 5084-5103), get_excluded_domains (lines 5105-5121)
  found: Both only iterate 2 levels. Sub-sub-domain exclusions are never restored or collected.
  implication: Even if you could manually uncheck a sub-sub-domain, it wouldn't be saved or detected

- timestamp: 2026-03-01T00:03:30Z
  checked: _filter_tree (lines 5022-5040)
  found: Only iterates 2 levels for search filtering too. Sub-sub-domains are never hidden/shown by search.
  implication: All iteration methods in DomainFilterDialog have the same 2-level-only bug

- timestamp: 2026-03-01T00:04:00Z
  checked: Web implementation at web/pages/search.py line 305-310
  found: domainFilterSelectAll uses querySelectorAll('input[type="checkbox"]') which gets ALL checkboxes regardless of nesting depth
  implication: Web works because it doesn't iterate by level - it selects all checkboxes in container. Desktop needs explicit recursion.

## Resolution

root_cause: All 6 iteration methods in DomainFilterDialog only loop 2 levels deep (parent + child). The _populate_tree method correctly creates 3 levels (lines 4993-5008), but _check_all, _uncheck_all, _handle_item_changed, _restore_exclusions, get_excluded_domains, and _filter_tree only iterate over parent.child(j) without descending into child.child(k) for sub-sub-domains.
fix: Add 3rd-level iteration loops to all 6 methods
verification: Syntax check passes. 76/76 unit tests pass (1 pre-existing failure unrelated to change). Awaiting human verification in desktop app.
files_changed: [genizah_app.py]
