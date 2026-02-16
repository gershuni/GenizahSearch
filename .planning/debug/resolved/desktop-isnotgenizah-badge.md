---
status: resolved
trigger: "Desktop IsNotGenizah badge not visible for Allony Ms. 113 (sys_id 990000465700205171)"
created: 2026-02-16T00:00:00Z
updated: 2026-02-16T20:00:00Z
---

## Current Focus

hypothesis: CONFIRMED - browse_render_page overwrites the enriched label that contains the badge
test: Traced execution flow from on_browse_enriched_loaded through browse_load_page
expecting: N/A - root cause confirmed
next_action: Return diagnosis

## Symptoms

expected: Orange "Not Genizah" badge near shelfmark in desktop browse view for Allony Ms. 113
actual: No badge visible in desktop app; web app shows it correctly
errors: None reported
reproduction: Browse to Allony Ms. 113 (sys_id 990000465700205171) in desktop app
started: Unknown - may never have worked in desktop

## Eliminated

- hypothesis: IsNotGenizah data missing from crossref DB for this sys_id
  evidence: nli_crossref.db has 2 rows with IsNotGenizah='True' for sys_id 990000465700205171
  timestamp: 2026-02-16T00:00:30Z

- hypothesis: NliCrossrefService.get_is_not_genizah returns wrong value
  evidence: Direct test returns True for this sys_id
  timestamp: 2026-02-16T00:00:30Z

- hypothesis: enrich_metadata doesn't set is_not_genizah in meta dict
  evidence: genizah_core.py line 3378 sets current_meta['is_not_genizah'] unconditionally
  timestamp: 2026-02-16T00:00:30Z

- hypothesis: HTML rendering broken (setText vs setHtml)
  evidence: label_text starts with <b> tag, AutoText detects rich text. But moot because label is overwritten.
  timestamp: 2026-02-16T00:00:45Z

## Evidence

- timestamp: 2026-02-16T00:00:20Z
  checked: nli_data/nli_crossref.db for sys_id 990000465700205171
  found: 6 rows total, 2 with IsNotGenizah='True' (AIU collection III.C.12)
  implication: Data is present and correct

- timestamp: 2026-02-16T00:00:25Z
  checked: NliCrossrefService.get_is_not_genizah('990000465700205171')
  found: Returns True
  implication: Service works correctly

- timestamp: 2026-02-16T00:00:30Z
  checked: genizah_core.py line 3378
  found: current_meta['is_not_genizah'] = crossref_svc.get_is_not_genizah(system_id) - set unconditionally
  implication: Meta dict will have the flag

- timestamp: 2026-02-16T00:00:35Z
  checked: on_browse_enriched_loaded (line 9046) badge rendering
  found: Lines 9130-9135 correctly add HTML badge to label_text, set at line 9137
  implication: Badge IS added to the label

- timestamp: 2026-02-16T00:00:40Z
  checked: on_browse_enriched_loaded lines 9236-9240
  found: Calls self.browse_load_page() when _browse_nav_rendered is False
  implication: browse_load_page is called after badge is set

- timestamp: 2026-02-16T00:00:45Z
  checked: browse_load_page (line 10635) -> browse_render_page (line 19508)
  found: browse_render_page rebuilds info label from scratch at line 19604 WITHOUT badge
  implication: Badge set at line 9137 is OVERWRITTEN at line 19604

- timestamp: 2026-02-16T00:00:50Z
  checked: _browse_nav_rendered flag usage
  found: Only set True in browse_navigate (line 19489) for arrow navigation. NOT set for initial browse.
  implication: On initial browse, badge is always overwritten. On arrow navigation, badge survives.

## Resolution

root_cause: on_browse_enriched_loaded (line 9046) correctly sets the badge in browse_info_lbl at line 9137, but then immediately calls browse_load_page() at line 9240, which calls browse_render_page() (line 19508), which rebuilds the info label from scratch at line 19604 WITHOUT the IsNotGenizah badge. The badge is set and then immediately overwritten.
fix:
verification:
files_changed: []
