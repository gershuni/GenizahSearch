---
phase: quick-16
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - CompileScriptGenizah.iss
autonomous: true
requirements: []
must_haves:
  truths:
    - "Installer always shows directory selection page, even during upgrades"
    - "Installer output filename matches current version v6.2.0"
  artifacts:
    - path: "CompileScriptGenizah.iss"
      provides: "Inno Setup installer script"
      contains: "DisableDirPage=no"
  key_links: []
---

<objective>
Fix Inno Setup installer script to always show the directory selection page (even on upgrades) and update the output filename to match v6.2.0.

Purpose: Prevent installer failure when upgrading from a previous install whose path is no longer accessible (e.g., v5.9.3 on a different drive). Inno Setup 6.x defaults DisableDirPage=auto which hides the directory page on upgrades, causing "drive not accessible" errors when the registry-stored path is stale.

Output: Updated CompileScriptGenizah.iss
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@CompileScriptGenizah.iss
</context>

<tasks>

<task type="auto">
  <name>Task 1: Add DisableDirPage and fix OutputBaseFilename</name>
  <files>CompileScriptGenizah.iss</files>
  <action>
In CompileScriptGenizah.iss [Setup] section, make two changes:

1. Add `DisableDirPage=no` after the `DisableProgramGroupPage=yes` line (line 32). This forces the directory selection page to always appear, even during upgrades when a previous install path exists in the registry.

2. Change line 36 from `OutputBaseFilename=GenizahSearchPro_V6.1.1_Setup` to `OutputBaseFilename=GenizahSearchPro_V6.2.0_Setup` to match the current MyAppVersion defined on line 6.

No other changes needed. The rest of the script is correct.
  </action>
  <verify>
    <automated>grep -n "DisableDirPage=no" CompileScriptGenizah.iss && grep -n "V6.2.0_Setup" CompileScriptGenizah.iss && echo "PASS"</automated>
  </verify>
  <done>CompileScriptGenizah.iss contains DisableDirPage=no in [Setup] section and OutputBaseFilename references V6.2.0</done>
</task>

</tasks>

<verification>
- `DisableDirPage=no` present in [Setup] section
- `OutputBaseFilename` references V6.2.0
- No other lines modified
</verification>

<success_criteria>
Installer script will show directory selection on all installs (fresh and upgrade), and output filename matches v6.2.0.
</success_criteria>

<output>
After completion, create `.planning/quick/16-fix-desktop-installer-add-directory-sele/16-SUMMARY.md`
</output>
