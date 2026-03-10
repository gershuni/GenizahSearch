# Quick Task 17: Summary

## What changed

1. **scripts/bump_version.py** (new) -- automated version bump across 4 files:
   - `version.py` (APP_VERSION)
   - `version_info.txt` (filevers, prodvers, FileVersion, ProductVersion)
   - `CompileScriptGenizah.iss` (#define MyAppVersion + OutputBaseFilename)
   - `README.md` (header line)
   - Supports `--dry-run`, validates X.Y.Z format, prints manual steps reminder

2. **version_info.txt** -- fixed 6.1.1 -> 6.2.0 (was out of sync with version.py)

3. **CLAUDE.md** -- added "Version Bumping" section documenting the script and which files to manually update (CHANGELOG.md, CLAUDE.md Recently Changed, README.md What's New)

## Usage
```bash
python scripts/bump_version.py 6.3.0          # apply
python scripts/bump_version.py 6.3.0 --dry-run # preview
```
