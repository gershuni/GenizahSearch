---
phase: quick-19
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - genizah_app.py
  - CHANGELOG.md
  - README.md
autonomous: true
requirements: [SESSION-COMP, SESSION-BROWSE]
must_haves:
  truths:
    - "Composition search results display correctly after session restore (summary text, grouped results, exclusion state)"
    - "Browse by Shelfmark tab restores the last viewed manuscript on session restore"
    - "Browse by Identification tab restores domain/author/work/date/text filters on session restore"
    - "Active tab at close time is restored on session restore"
    - "CHANGELOG.md 6.5.1 section includes session restore improvements"
  artifacts:
    - path: "genizah_app.py"
      provides: "Enhanced _save_session and _restore_session with browse + composition improvements"
    - path: "CHANGELOG.md"
      provides: "Updated 6.5.1 release notes"
  key_links:
    - from: "genizah_app.py:_save_session"
      to: "shared/session_persistence.py:save_session_state"
      via: "state_dict with browse_shelfmark, browse_catalog, active_tab keys"
      pattern: "save_session_state.*state_dict"
    - from: "genizah_app.py:_restore_session"
      to: "genizah_app.py:browse_load"
      via: "Restoring browse state triggers browse_load"
      pattern: "browse_load"
---

<objective>
Fix desktop session restore to persist and restore: (1) composition/parallel search results with full fidelity (summary text, grouped display), (2) Browse by Shelfmark tab state (last viewed manuscript), (3) Browse by Identification tab state (domain/author/work/date/text filters), and (4) the active tab. Then update CHANGELOG.md and README.md for v6.5.1.

Purpose: Users lose their browse context and composition search state when restarting the app. The session persistence infrastructure exists but only covers regular search fully. Composition restore works but loses the summary text and grouping. Browse tabs are not persisted at all.

Output: Updated genizah_app.py with complete session persistence, updated CHANGELOG.md and README.md.
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@genizah_app.py (lines 25570-25820: _save_session, _restore_session, closeEvent)
@genizah_app.py (lines 8554-8563: tab setup with addTab)
@genizah_app.py (lines 14065-14086: catalog browse internal state variables)
@genizah_app.py (lines 24328-24377: browse_load method)
@shared/session_persistence.py (save/load/clear functions)
@CHANGELOG.md
@README.md
</context>

<tasks>

<task type="auto">
  <name>Task 1: Extend session save/restore with browse state, composition summary, and active tab</name>
  <files>genizah_app.py</files>
  <action>
**In `_save_session` (around line 25579):**

Add these new keys to `state_dict`:

1. **`active_tab`** — Save `self.tabs.currentIndex()` so we restore the user's last active tab.

2. **`browse_shelfmark`** — Save the Browse by Shelfmark state:
   ```python
   'browse_shelfmark': {
       'sys_id': self.browse_sys_input.text().strip(),
       'shelfmark': self.browse_shelf_input.text().strip(),
       'fl_id': self.browse_fl_input.text().strip(),
       'last_field': getattr(self, 'last_browse_field', 'shelf'),
   }
   ```

3. **`browse_catalog`** — Save the Browse by Identification state:
   ```python
   'browse_catalog': {
       'domain': getattr(self, '_catalog_current_domain', None),
       'author': getattr(self, '_catalog_current_author', None),
       'work': getattr(self, '_catalog_current_work', None),
       'date_from': getattr(self, '_catalog_date_from', None),
       'date_to': getattr(self, '_catalog_date_to', None),
       'include_undated': getattr(self, '_catalog_include_undated', False),
       'text_all': getattr(self, '_catalog_text_all', []),
       'text_any': getattr(self, '_catalog_text_any', []),
       'text_not': getattr(self, '_catalog_text_not', []),
   }
   ```

4. **In `composition_search` dict**, add:
   ```python
   'summary_text': getattr(self, 'comp_summary_text', ''),
   ```

**In `_restore_session` (around line 25631):**

1. **After restoring composition results** (after `display_comp_results` call ~line 25758), restore the summary text:
   ```python
   if comp.get('summary_text'):
       self.comp_summary_text = comp['summary_text']
       self.comp_progress.setVisible(True)
       self.comp_progress.setRange(0, 1)
       self.comp_progress.setValue(1)
       self.comp_progress.setFormat(self.comp_summary_text)
   ```

2. **Restore Browse by Shelfmark state** — After restoring composition state, add a block:
   ```python
   # Restore Browse by Shelfmark
   browse = state.get('browse_shelfmark', {})
   if browse.get('sys_id') or browse.get('shelfmark') or browse.get('fl_id'):
       if browse.get('sys_id'):
           self.browse_sys_input.setText(browse['sys_id'])
       if browse.get('shelfmark'):
           self.browse_shelf_input.setText(browse['shelfmark'])
       if browse.get('fl_id'):
           self.browse_fl_input.setText(browse['fl_id'])
       self.last_browse_field = browse.get('last_field', 'shelf')
       # Defer browse_load to after UI is settled
       QTimer.singleShot(300, self.browse_load)
   ```

3. **Restore Browse by Identification state** — After Browse by Shelfmark:
   ```python
   # Restore catalog browse filters
   cat = state.get('browse_catalog', {})
   if any([cat.get('domain'), cat.get('author'), cat.get('work'),
           cat.get('date_from'), cat.get('date_to'),
           cat.get('text_all'), cat.get('text_any'), cat.get('text_not')]):
       self._catalog_current_domain = cat.get('domain')
       self._catalog_current_author = cat.get('author')
       self._catalog_current_work = cat.get('work')
       self._catalog_date_from = cat.get('date_from')
       self._catalog_date_to = cat.get('date_to')
       self._catalog_include_undated = cat.get('include_undated', False)
       self._catalog_text_all = cat.get('text_all', [])
       self._catalog_text_any = cat.get('text_any', [])
       self._catalog_text_not = cat.get('text_not', [])
       # Populate date input fields
       if cat.get('date_from') and hasattr(self, '_catalog_date_from_input'):
           self._catalog_date_from_input.setText(str(cat['date_from']))
       if cat.get('date_to') and hasattr(self, '_catalog_date_to_input'):
           self._catalog_date_to_input.setText(str(cat['date_to']))
       # Defer catalog refresh to after UI is settled
       QTimer.singleShot(400, self._catalog_refresh_results)
       QTimer.singleShot(450, self._catalog_update_chips)
   ```
   Note: `_catalog_refresh_results` and `_catalog_update_chips` are existing methods -- verify their exact names by grepping. If the chip update method is named differently, use the correct name.

4. **Restore active tab** — At the very end, before hiding the progress bar:
   ```python
   # Restore active tab
   active_tab_idx = state.get('active_tab')
   if active_tab_idx is not None and 0 <= active_tab_idx < self.tabs.count():
       self.tabs.setCurrentIndex(active_tab_idx)
   ```

5. **Update `has_data` check** (~line 25650) to also consider browse state:
   ```python
   browse = state.get('browse_shelfmark', {})
   cat = state.get('browse_catalog', {})
   has_data = (reg.get('results') or comp.get('results') or comp.get('source_text')
               or browse.get('sys_id') or browse.get('shelfmark')
               or cat.get('domain') or cat.get('author') or cat.get('work'))
   ```

**Important:** Verify exact method names before using them:
- Grep for `def _catalog_refresh` and `def _catalog_update_chip` to confirm names
- Grep for `_catalog_include_undated` checkbox widget to ensure we also update the checkbox state if there is one
  </action>
  <verify>
    <automated>python -c "import genizah_app; print('Import OK')" 2>&1 | head -5</automated>
    Manual: Launch desktop app, perform a regular search, browse a manuscript, set catalog filters, switch to composition tab. Close app. Reopen. Verify all state restored.
  </verify>
  <done>
    - _save_session persists browse_shelfmark, browse_catalog, active_tab, and composition summary_text
    - _restore_session restores all of these on startup
    - has_data check considers browse state for the restore prompt
    - Active tab at close time is the active tab after restore
  </done>
</task>

<task type="auto">
  <name>Task 2: Update CHANGELOG.md and README.md for v6.5.1</name>
  <files>CHANGELOG.md, README.md</files>
  <action>
**CHANGELOG.md:** In the existing `## [6.5.1]` section, add a new bullet under `### Bug Fixes` (or create `### Improvements` subsection):

```markdown
### Improvements

- **Desktop session persistence — browse tabs**: Browse by Shelfmark now restores the last viewed manuscript on restart. Browse by Identification restores domain, author, work, date, and text filters
- **Desktop session persistence — composition search**: Composition search now restores the results summary bar showing elapsed time and match counts
- **Desktop session persistence — active tab**: The last active tab is restored on restart (previously always returned to Search tab)
```

**README.md:** Find the "What's New" or equivalent section. Under v6.5.1, add a brief mention:
```
- Session restore: browse tabs and composition search state now fully persist across restarts
```

If the README doesn't have a v6.5.1 section or "What's New" doesn't list patch versions, just update the existing "Recently Changed" line for v6.5.1 in CLAUDE.md instead. Check `CLAUDE.md` "Recently Changed" section and update the March 2026 v6.5.1 entry to mention session restore improvements.
  </action>
  <verify>
    <automated>grep -c "session persistence" CHANGELOG.md</automated>
  </verify>
  <done>
    - CHANGELOG.md 6.5.1 section documents browse tab persistence, composition summary restore, and active tab restore
    - README.md or CLAUDE.md updated with brief mention of session restore improvements
  </done>
</task>

</tasks>

<verification>
1. Desktop app launches without import errors
2. Regular search session restore still works (regression check)
3. Composition search restores with summary text visible
4. Browse by Shelfmark restores the manuscript that was being viewed
5. Browse by Identification restores filters and re-runs the query
6. Active tab is preserved across restart
7. CHANGELOG.md has session restore entries under 6.5.1
</verification>

<success_criteria>
- All 5 session state categories persist: regular search, composition search (with summary), browse shelfmark, browse catalog, active tab
- No regression in existing session restore functionality
- Documentation updated for v6.5.1
</success_criteria>

<output>
After completion, create `.planning/quick/19-fix-desktop-session-restore-parallel-sea/19-SUMMARY.md`
</output>
