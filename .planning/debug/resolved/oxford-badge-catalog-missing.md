---
status: resolved
trigger: "Investigate why the desktop app doesn't show the IsNotGenizah badge or Neubauer-Cowley catalog number for Oxford MS heb. a.1/1 (sys_id 990053385780205171)."
created: 2026-02-16T00:00:00Z
updated: 2026-02-16T00:07:00Z
---

## Current Focus

hypothesis: CONFIRMED - Oxford Part manuscripts take _browse_load_part path which doesn't append badge/catalog and uses setText() instead of setHtml()
test: Complete
expecting: N/A
next_action: Return diagnosis report

## Symptoms

expected: Desktop app should show orange "Not Genizah" HTML badge and Neubauer-Cowley catalog number (e.g., "2613.1") for Oxford manuscript sys_id 990053385780205171
actual: User reports "don't see" for both features
errors: None reported
reproduction: Browse to Oxford MS heb. a.1/1 in desktop app
started: Unknown (code added in commit be2352ab)

## Eliminated

## Evidence

- timestamp: 2026-02-16T00:01:00Z
  checked: genizah_app.py lines 8701-8714
  found: Line 8714 uses `self.browse_info_lbl.setText(label_text)` but label_text contains HTML including `<span>` tags for the badge (lines 8708-8712)
  implication: setText() treats HTML as plain text and doesn't render it. HTML badges and formatting are displayed as literal text strings instead of being rendered.

- timestamp: 2026-02-16T00:02:00Z
  checked: genizah_app.py lines 8665-8690
  found: label_text was ALREADY using HTML tags before commit be2352ab -- `<b>` for shelfmark (line 8679), `<br/>` for line breaks (line 8683), `<span style='font-size: 11px;'>` for styled titles (line 8683)
  implication: The setText() bug pre-existed the badge addition. All HTML formatting (bold, line breaks, font sizes) has never rendered correctly in browse_info_lbl.

- timestamp: 2026-02-16T00:03:00Z
  checked: git show be2352ab
  found: Commit added HTML badge (`<span style='background:#fff3e0...'>`) and plain text catalog_entry to label_text, then called setText() which was already there
  implication: The commit correctly added the badge HTML and catalog text, but setText() prevents rendering of all HTML including the new badge and pre-existing formatting.

- timestamp: 2026-02-16T00:04:00Z
  checked: Grep for setHtml in genizah_app.py
  found: 20+ other locations correctly use setHtml() for HTML content (browse_text, txt_extended_info, etc.)
  implication: setHtml() is the established pattern in this codebase. Line 8714 is an outlier.

- timestamp: 2026-02-16T00:05:00Z
  checked: genizah_app.py lines 18911-19001 (_browse_load_part function)
  found: Oxford Part manuscripts (like MS heb. a.1/1) take a DIFFERENT code path than regular manuscripts. _browse_load_part (line 18911) sets info label at line 19000 using setText(), builds info_text with HTML (lines 18989-18998), but does NOT append catalog_entry or is_not_genizah badge. NO EnrichMetadataThread is started in this function.
  implication: Oxford Part manuscripts NEVER reach on_browse_enriched_loaded (line 8627) which is where the badge/catalog code exists (lines 8701-8712). The badge and catalog features were added to the wrong code path.

- timestamp: 2026-02-16T00:06:00Z
  checked: Browse flow for Oxford Parts vs regular manuscripts
  found: browse_load (line 18759) checks if folio belongs to Oxford Part at line 18872-18875. If yes, calls _browse_load_part and returns. If no, continues to line 18893 where EnrichMetadataThread starts, which eventually calls on_browse_enriched_loaded.
  implication: Two code paths exist. Commit be2352ab only added badge/catalog to the on_browse_enriched_loaded path (regular manuscripts), missing the _browse_load_part path (Oxford Parts).

## Resolution

root_cause: **TWO issues prevent badge and catalog from showing on Oxford manuscripts:**

1. **Wrong code path:** The badge and catalog code (lines 8701-8712 in on_browse_enriched_loaded) was added to the regular manuscript flow, but Oxford Part manuscripts (like MS heb. a.1/1 with sys_id 990053385780205171) take a completely different path through _browse_load_part (line 18911). The _browse_load_part function:
   - Builds info_text at lines 18989-18998 (with HTML)
   - Sets label at line 19000: `self.browse_info_lbl.setText(info_text)`
   - NEVER appends catalog_entry or is_not_genizah badge
   - Does NOT trigger EnrichMetadataThread, so on_browse_enriched_loaded is never called

2. **setText() vs setHtml():** Both code paths use setText() instead of setHtml() (lines 8714, 19000), causing all HTML (including badges) to render as literal text instead of styled markup.

**The fix requires:**
- Add badge/catalog appending logic to _browse_load_part function (after line 18998)
- Change setText() to setHtml() in both locations (lines 8714, 19000)
- Consider also fixing lines 10429, 15778, 19188 which have same setText() issue
fix:
verification:
files_changed: []
