# In-App Software Update Implementation Plan

**Date:** 2026-02-04
**Status:** ✅ Implemented (2026-02-04)
**Priority:** Medium

---

## Overview

Implement automatic in-app software updates for the GenizahSearch desktop application (PyQt6). Currently, the app can detect new versions via GitHub Releases API but requires users to manually download and install updates. This plan adds the ability to download and install updates directly from within the application.

---

## Current State

| Component | Status | Location |
|-----------|--------|----------|
| Version checking | ✅ Working | `gui_threads.py:344-383` |
| Update notification bar | ✅ Working | `genizah_app.py:228-282` |
| Manual check button | ✅ Working | `genizah_app.py:15147-15155` |
| Download & install | ❌ Not implemented | - |
| Auto-restart | ❌ Not implemented | - |

### Current Flow
```
App starts → Check GitHub API → Show notification bar → User clicks "Download" → Opens browser → User downloads installer → User runs installer manually
```

### Target Flow
```
App starts → Check GitHub API → Show notification bar → User clicks "Update Now" → Download installer with progress → Run installer silently → App restarts automatically
```

---

## Technical Approach

### Why Custom Solution (Not tufup/PyUpdater)

| Factor | Custom | tufup |
|--------|--------|-------|
| Infrastructure needed | GitHub Releases (existing) | Separate server/S3 |
| Security | HTTPS + GitHub trust | TUF cryptographic signing |
| Patch updates | No (full download) | Yes (delta patches) |
| Implementation time | Low | High |
| Maintenance | Simple | Complex |
| User base size | Small (~100 users) | Enterprise scale |

**Decision:** Custom solution is appropriate for this project's scale and existing infrastructure.

---

## Implementation Details

### Phase 1: Download Thread

**New Class:** `UpdateDownloaderThread` in `gui_threads.py`

```python
class UpdateDownloaderThread(QThread):
    """Download update ZIP from GitHub Releases."""

    progress_signal = pyqtSignal(int, int)  # downloaded_bytes, total_bytes
    finished_signal = pyqtSignal(bool, str)  # success, file_path_or_error

    def __init__(self, download_url: str, target_path: str):
        super().__init__()
        self.download_url = download_url
        self.target_path = target_path

    def run(self):
        try:
            # 1. Get asset download URL from release
            # 2. Stream download with progress updates
            # 3. Save to temp directory
            # 4. Emit success with file path
        except Exception as e:
            self.finished_signal.emit(False, str(e))
```

**Key Features:**
- Stream download to avoid memory issues (~150MB ZIP)
- Progress reporting every 1% or 1MB
- Timeout handling (10 min max)
- Resume support (nice-to-have, not required)

---

### Phase 2: Update Dialog UI

**New Class:** `UpdateProgressDialog` in `genizah_app.py`

```python
class UpdateProgressDialog(QDialog):
    """Shows download progress and handles update installation."""

    def __init__(self, parent, version: str, download_url: str):
        # Progress bar (0-100%)
        # Status label ("Downloading...", "Extracting...", etc.)
        # Cancel button
        # Size info label ("45 MB / 150 MB")
```

**UI Mockup:**
```
┌─────────────────────────────────────────┐
│  Updating to version 5.5.0              │
├─────────────────────────────────────────┤
│                                         │
│  ████████████░░░░░░░░░░░░  48%         │
│                                         │
│  Downloading: 72 MB / 150 MB            │
│                                         │
│              [ Cancel ]                 │
└─────────────────────────────────────────┘
```

---

### Phase 3: File Replacement Strategy

**The Challenge:** On Windows, the running `.exe` file is locked and cannot be replaced.

**Solution:** Use the existing Inno Setup installer in silent mode.

**Installer Settings Added:** (`dist/CompileScriptGenizah.iss`)

```ini
[Setup]
CloseApplications=force      ; Auto-close running app
CloseApplicationsFilter=*.exe
RestartApplications=yes      ; Restart app after update
```

**Silent Install Command:**
```batch
GenizahSearchPro_V5.5.0_Setup.exe /VERYSILENT /RESTARTAPPLICATIONS
```

**Process Flow:**
```
1. Download installer to %TEMP%\GenizahSearchPro_v5.5.0_Setup.exe
2. Run installer with /VERYSILENT /RESTARTAPPLICATIONS flags
3. Installer auto-closes running app (CloseApplications=force)
4. Installer updates files in place
5. Installer restarts app (RestartApplications=yes)
```

**Advantages over batch script approach:**
- Inno Setup handles all the tricky file locking issues
- UAC elevation handled properly
- No need for complex batch scripts
- Same installer used for fresh installs and updates

---

### Phase 4: Integration with Existing Code

#### Changes to `gui_threads.py`

| Change | Lines | Description |
|--------|-------|-------------|
| Add `UpdateDownloaderThread` | New (after line 383) | Download with progress |
| Modify `UpdateCheckerThread` | 358-378 | Also fetch asset download URL |

**Modified `UpdateCheckerThread.run()`:**
```python
def run(self):
    # ... existing code ...
    if resp.status_code == 200:
        data = resp.json()
        tag = data.get('tag_name', '').strip()
        html_url = data.get('html_url', '')

        # Get installer (.exe) asset URL
        assets = data.get('assets', [])
        installer_url = ''
        for asset in assets:
            asset_name = asset.get('name', '').lower()
            if asset_name.endswith('.exe') and ('setup' in asset_name or 'install' in asset_name):
                installer_url = asset.get('browser_download_url', '')
                break

        # ... version comparison ...
        if remote_v > curr_v:
            self.finished_signal.emit(True, tag, html_url, installer_url, self.is_manual)
```

**Signal Change:**
```python
# Old
finished_signal = pyqtSignal(bool, str, str, bool)  # found, version, url, is_manual

# New
finished_signal = pyqtSignal(bool, str, str, str, bool)  # found, version, html_url, installer_url, is_manual
```

---

#### Changes to `genizah_app.py`

| Change | Lines | Description |
|--------|-------|-------------|
| Add `UpdateProgressDialog` | New (after line 282) | Progress UI |
| Modify `UpdateNotificationBar` | 248-251 | Change button to "Update Now" |
| Modify `on_update_result` | 15157-15181 | Store installer_url, show dialog |
| Add `start_in_app_update()` | New | Initiate download |
| Add `execute_update()` | New | Run installer silently |

**Key Method: `execute_update()`**
```python
def execute_update(self):
    """Run the installer in silent mode (Windows only)."""
    import subprocess

    # Run the installer with silent mode
    # The installer will:
    # 1. Close this running app (CloseApplications=force)
    # 2. Install the update
    # 3. Restart the app (RestartApplications=yes)
    subprocess.Popen(
        [self.downloaded_path, '/VERYSILENT', '/RESTARTAPPLICATIONS'],
        creationflags=subprocess.DETACHED_PROCESS
    )

    # Close the dialog and quit the application
    self.accept()
    QApplication.quit()
```

---

## File Changes Summary

| File | Type | Changes |
|------|------|---------|
| `gui_threads.py` | Modify | Add `UpdateDownloaderThread`, modify `UpdateCheckerThread` signals |
| `genizah_app.py` | Modify | Add `UpdateProgressDialog`, modify notification bar, add update methods |
| `dist/CompileScriptGenizah.iss` | Modify | Add `CloseApplications` and `RestartApplications` settings |
| `version.py` | No change | - |
| `build_app.bat` | No change | - |

---

## Error Handling

| Scenario | Handling |
|----------|----------|
| Network error during download | Show retry button, keep partial download |
| Disk full | Show error, clean up partial files |
| ZIP corrupted | Verify ZIP before extraction, show error |
| Update script fails | Keep backup, show manual instructions |
| User cancels mid-download | Clean up temp files, return to normal state |
| No write permission | Show error with admin instructions |

---

## Security Considerations

1. **HTTPS Only** - All downloads via HTTPS (GitHub enforces this)
2. **Domain Verification** - Only download from `github.com/gershuni/GenizahSearch`
3. **No Code Signing** - Acceptable for this user base; Windows Defender may flag
4. **Backup Before Replace** - Keep `.backup` folder until successful restart

**Future Enhancement (Optional):**
- Add SHA256 checksum verification (publish checksums in release notes)

---

## User Experience Flow

### Automatic Update (App Startup)

```
1. App starts normally
2. Background check finds v5.5.0 available
3. Blue notification bar appears at top:
   "New version available: 5.5.0  [Update Now] [✕]"
4. User clicks "Update Now"
5. Progress dialog appears
6. Download completes → "Restarting..."
7. App closes, updates, reopens
8. User sees new version running
```

### Manual Update (Settings)

```
1. User clicks "Check for Updates" in Settings
2. Dialog: "Version 5.5.0 is available. Update now?"
3. User clicks "Yes"
4. Same progress flow as above
```

### Dismissal

```
1. User clicks ✕ on notification bar
2. Bar disappears
3. Won't show again until NEXT version (stored in config)
```

---

## Testing Checklist

### Pre-Release Testing

- [ ] Download completes successfully
- [ ] Progress bar updates smoothly
- [ ] Cancel button stops download and cleans up
- [ ] ZIP extraction works correctly
- [ ] App restarts after update
- [ ] Old version files are removed
- [ ] User data (config, index) preserved
- [ ] Network failure shows appropriate error
- [ ] Disk space error handled gracefully

### Edge Cases

- [ ] Update while background tasks running
- [ ] Update with unsaved changes
- [ ] Update from very old version (e.g., 5.0.0 → 5.5.0)
- [ ] Update when app installed in Program Files (admin required)
- [ ] Update on slow/unreliable network
- [ ] Update interrupted by system shutdown

---

## Rollback Plan

If update fails mid-process:

1. **Backup exists** (`GenizahSearchPro.backup/`):
   - User can manually rename back
   - Or re-download from website

2. **No backup** (deleted too early):
   - User must download fresh from website
   - User data in `%APPDATA%` is preserved

---

## Implementation Order

1. **Phase 1:** `UpdateDownloaderThread` with progress signals
2. **Phase 2:** `UpdateProgressDialog` UI
3. **Phase 3:** Batch script generation and execution
4. **Phase 4:** Integration with existing notification bar
5. **Phase 5:** Testing on Windows 10/11
6. **Phase 6:** Documentation update

---

## Timeline Estimate

| Phase | Effort |
|-------|--------|
| Phase 1-2 | ~2 hours |
| Phase 3 | ~1 hour |
| Phase 4 | ~1 hour |
| Phase 5 | ~2 hours |
| Phase 6 | ~30 min |
| **Total** | **~6-7 hours** |

---

## References

- [GitHub Releases API](https://docs.github.com/en/rest/releases/releases)
- [PyInstaller onedir structure](https://pyinstaller.org/en/stable/operating-mode.html)
- [Windows batch scripting](https://ss64.com/nt/)
- Current implementation: `gui_threads.py:344-383`, `genizah_app.py:228-282`, `genizah_app.py:15141-15197`

---

## Appendix: Alternative Approaches Considered

### A. tufup Library
- **Pros:** Cryptographic security, delta patches
- **Cons:** Requires separate update server, complex setup
- **Verdict:** Overkill for current user base

### B. NSIS/Inno Setup Installer
- **Pros:** Professional installer UI, handles UAC
- **Cons:** Requires building separate installer, larger download
- **Verdict:** Could be added later if needed

### C. Windows Store Distribution
- **Pros:** Auto-updates handled by Windows
- **Cons:** Store certification process, restrictions
- **Verdict:** Not suitable for niche academic software

### D. Squirrel.Windows
- **Pros:** Mature, used by Electron apps
- **Cons:** Designed for .NET/Electron, complex integration
- **Verdict:** Not a good fit for PyInstaller apps
