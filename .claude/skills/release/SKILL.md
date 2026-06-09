---
name: release
description: "Release GenizahSearch — version bump, What's New drafting, code review, build, deploy, GitHub release. Supports: web-only, desktop-only, or both."
user-invocable: true
---

# /release — GenizahSearch Release Skill

You are orchestrating a release of GenizahSearch. This is a multi-step, partially interactive process. Follow each phase in order. Do NOT skip phases. Mark each phase clearly in your output.

## Phase 0: Gather Release Intent

Ask the user:
1. **What version?** (suggest next patch/minor/major based on current `version.py`)
2. **What type?** — `web`, `desktop`, or `both`
3. **One-line summary** of the release theme (e.g., "Image adjustment controls")

Read `version.py` to show the current version. Read recent `git log --oneline -20` and `docs/OPEN_ISSUES.md` to understand what's changed since last release.

## Phase 1: Pre-Flight Code Review

Send an agent (subagent_type=Explore) to verify the code is ready:

**Checks:**
- `git status` — working tree clean? Uncommitted changes?
- `python -m pytest tests/ --tb=short -q` — all tests pass?
- **`python -m ruff check .` — explicit ruff pass** (per project memory: v7.12.0 CI failed on F401 unused imports; pre-flight must run ruff as its own line item, not implied by pytest)
- `python scripts/check_docs.py` — documentation health OK? (NOTE: on Windows console may fail with UnicodeEncodeError on emoji — that's environment-only, not a blocker)
- **`requirements.txt` vs `requirements-lock.txt` consistency** — every runtime dep in `requirements.txt` must have a matching pin in `requirements-lock.txt`. CI installs from the lock file, so a `requirements.txt` addition that's not lock-pinned breaks CI on the release commit. Diff check:
  ```bash
  # Extract package names (lowercase) from both files and compare
  comm -23 \
    <(grep -v '^#' requirements.txt | sed 's/[<>=!].*//' | tr '[:upper:]' '[:lower:]' | sort -u) \
    <(grep -v '^#' requirements-lock.txt | sed 's/==.*//' | tr '[:upper:]' '[:lower:]' | sort -u)
  ```
  If anything prints, those packages are in `requirements.txt` but absent from the lock file — block the release until they're added to `requirements-lock.txt`. (Phase 95 / v7.14.0 hit this: pymupdf was in `requirements.txt` but missing from the lock, so CI failed post-push.)
- No `TODO(release)` or `FIXME(urgent)` markers in recently changed files
- `OPEN_ISSUES.md` — any critical/blocking issues still open?
- Grep for `print(` debug statements in recently committed code (last 5 commits)
- Version consistency: `version.py`, `CompileScriptGenizah.iss`, `version_info.txt`, `README.md` all match current version (not yet bumped)
- **`tests/test_release_artifacts.py` `_TARGET_VERSION`** — per project memory, `bump_version.py` does NOT update this constant. Check its current value matches the pre-bump version; you'll need to bump it manually in Phase 3.

**Report** findings to the user. If there are blockers, stop and ask how to proceed. If there are warnings, list them and ask for confirmation to continue.

## Phase 2: Interactive "What's New" Drafting (OPTIONAL)

**Ask the user first:** "Do you want to draft What's New / release texts now? You can do all, some, or skip entirely."

The user may choose any subset of the items below, or skip this phase entirely. Only draft what they ask for.

This is a multi-round interactive process. Do NOT rush it.

### Step 2a: Gather material
- Read `git log --oneline` since the last version tag
- Read recent entries in `CHANGELOG.md`
- Identify the key user-facing changes (not internal refactors)

### Step 2b: Draft requested release texts
Present drafts for whichever items the user wants. The full menu is:

1. **CHANGELOG.md entry** — Full detailed changelog section with `### New Features`, `### Improvements`, `### Bug Fixes` subsections as appropriate. Technical but readable.

2. **README.md "What's New" section** — 1-2 paragraphs + bullet points for the version. User-facing, not too technical.

3. **Web What's New banner** (`web/main.py` near `WHATS_NEW_VERSION`) — Single concise line, bilingual (English + Hebrew). This is what users see in the dismissible banner. Must be compelling but brief.

4. **Desktop What's New bar** (`genizah_app.py` `WhatsNewBar.show_whats_new`) — Single line Hebrew summary shown in the notification bar.

5. **Desktop What's New dialog** (`genizah_app.py` `WhatsNewDialog`) — 3-5 bullet points in Hebrew (`<li>` items), shown when user clicks "Learn More".

6. **GitHub Release draft** — *(skip for web-only releases — no GitHub release will be created)*. Title + body for the GitHub release. Include:
   - Release title: `vX.Y.Z: <theme>`
   - Summary paragraph
   - Key changes (bullet points)
   - Download links placeholder for desktop installer

7. **Help page updates** — If new features need documentation:
   - `web/pages/help.py` — both `_create_english_content()` and `_create_hebrew_content()`
   - `Help.html` — desktop help file

### Step 2c: Review loop
Present all requested drafts to the user at once. Ask:
> "Please review these drafts. Tell me what to change — wording, emphasis, additions, removals. We can do as many rounds as you need."

Iterate until the user approves. Pay attention to:
- Hebrew quality (the user is a native speaker)
- Consistency across chosen text locations
- Proper bilingual coverage (EN + HE)

## Phase 3: Version Bump + Apply Texts

Once texts are approved (or Phase 2 was skipped):

1. Run `python scripts/bump_version.py X.Y.Z` — updates version.py, version_info.txt, CompileScriptGenizah.iss, README.md
2. Apply all approved text changes (only those drafted in Phase 2):
   - Edit `CHANGELOG.md` — add the approved section at top
   - Edit `README.md` — update "What's New" section
   - Edit `web/main.py` — update the What's New banner text
   - Edit `genizah_app.py` — update WhatsNewBar message + WhatsNewDialog content
   - Edit `CLAUDE.md` "Recently Changed" section — add entry
   - Edit help files if applicable
3. Run `python scripts/check_docs.py` to verify docs are still healthy
4. Update `docs/OPEN_ISSUES.md` — mark any fixed issues, update timestamp

## Phase 4: "Did You Forget?" Checklist

Present this checklist BEFORE building or deploying. Adapt it based on what was done in Phase 2 (mark N/A for skipped items):

```
RELEASE CHECKLIST — vX.Y.Z
==============================
[ ] Version bumped in all files (version.py, .iss, version_info.txt, README)
[ ] tests/test_release_artifacts.py _TARGET_VERSION manually bumped (bump_version.py misses it)
[ ] CHANGELOG.md has new section with correct date
[ ] README.md "What's New" updated (or N/A)
[ ] CLAUDE.md "Recently Changed" updated
[ ] Web What's New banner text updated (or N/A)
[ ] Desktop What's New bar + dialog updated (or N/A)
[ ] Help pages updated (or N/A)
[ ] OPEN_ISSUES.md up to date
[ ] All tests passing
[ ] ruff explicit pass (python -m ruff check .)
[ ] requirements.txt ↔ requirements-lock.txt diff is empty (CI uses the lock file; missing deps break CI)
[ ] No uncommitted changes (besides release changes)
[ ] Translations present for new UI strings

FOR DESKTOP:
[ ] Sidecar databases checkpointed (build_app.bat does this)

FOR WEB:
[ ] No breaking changes to web-only features
```

Ask: "Anything else you want to verify before we build and deploy?"

## Phase 5: Release Summary + Confirmation

Present a human-readable summary of what this release contains and what will happen. This is the "read it aloud to a colleague" version — plain English, no file paths or technical jargon.

**Part A — What's in this release:**
Summarize the user-facing changes in plain language. Group by:
- **New features** — what can users do now that they couldn't before?
- **Improvements** — what got better/faster/easier?
- **Bug fixes** — what was broken and is now fixed?
- **Internal/infrastructure** — anything non-user-facing worth noting (briefly)

Keep each item to one sentence. Write as if explaining to a non-technical stakeholder.

Example:
> **New features:** Users can now adjust brightness and contrast on manuscript images directly in the viewer.
> **Bug fixes:** Fixed an issue where Oxford metadata would incorrectly appear on Russian National Library manuscripts.

**Part B — What will happen next:**
```
ACTIONS — vX.Y.Z: <theme>
===================================
Commit & tag:  vX.Y.Z on master-main
Desktop build: PyInstaller + Inno Setup installer   (or "skipped — web only")
Web deploy:    deploy.sh on EC2                      (or "skipped — desktop only")
GitHub release: with installer upload                (or "skipped — web only")
```

Ask: "Does this look right? Proceed with build and deploy?"

## Phase 6: Build (Desktop only — skip for web-only releases)

> **Running `build_app.bat` reliably (learned the hard way — v8.0.0 burned ~5 failed
> attempts).** A bare `cmd /c build_app.bat` from the PowerShell/Bash tool fails with
> `'build_app.bat' is not recognized` for THREE compounding reasons, all defeated by the
> invocation below:
> 1. **Background tasks don't inherit the project CWD** — they start elsewhere, not the repo root.
> 2. **PowerShell `Set-Location` only moves the *provider* location, NOT the process working
>    directory** a spawned `cmd` inherits — you MUST also set `[Environment]::CurrentDirectory`.
>    (`Start-Process -WorkingDirectory` is *ignored* when combined with output redirection —
>    don't rely on it either.)
> 3. This machine has **`NoDefaultCurrentDirectoryInExePath`** set, so `cmd` refuses to search
>    the current dir for the batch file — you MUST pass the **explicit full path** to the `.bat`.
>    (The `.bat` still uses relative paths internally — genizah_app.py, icon.ico, scripts\… —
>    so the process CWD must ALSO be the repo root; that's why both pieces are needed.)
>
> **Proven invocation (PowerShell tool; OK to run in background):**
> ```powershell
> Set-Location -LiteralPath 'C:\Genizahsearch'; [Environment]::CurrentDirectory = 'C:\Genizahsearch'; cmd /c "C:\Genizahsearch\build_app.bat"; Write-Output "BUILD_EXIT=$LASTEXITCODE"
> ```

1. Run `build_app.bat` (via the invocation above) — PyInstaller build (several minutes)
   - Verify `dist/GenizahSearchPro/GenizahSearchPro.exe` exists after build
   - **`build_app.bat` REGENERATES (clobbers) `GenizahSearchPro.spec`** every run (command-line
     PyInstaller writes a fresh minimal spec, stripping the maintained `collect_all('pymupdf')`/
     `collect_all('zstandard')`/`collect_all('lxml')` + `fitz`/`openpyxl`/`defusedxml`
     hidden-imports). The build still works (PyInstaller contrib hooks collect those deps), but
     **after the build run `git restore GenizahSearchPro.spec`** so the maintained spec is never
     committed clobbered.
2. Run Inno Setup CLI to create installer (same CWD caveat — full paths):
   ```powershell
   Set-Location -LiteralPath 'C:\Genizahsearch'; [Environment]::CurrentDirectory = 'C:\Genizahsearch'; & 'C:\Program Files (x86)\Inno Setup 6\ISCC.exe' 'C:\Genizahsearch\CompileScriptGenizah.iss'; Write-Output "ISCC_EXIT=$LASTEXITCODE"
   ```
   - Verify the output `.exe` installer was created (`dist/GenizahSearchPro_VX.Y.Z_Setup.exe`)
   - (Inno compresses the ~2.3 GB payload with LZMA — allow ~5 min.)
3. Report build output sizes to user

### Desktop Installer Test Gate

**Do NOT proceed to deploy until the user confirms they tested the installer.**

Ask: "Please install and launch the built installer to verify it works. Confirm when ready to continue."

Wait for the user's confirmation before moving to Phase 7.

## Phase 7: Commit, Tag & Deploy

After user confirms:

1. **Stage and commit** all release changes:
   ```
   git add -A
   git commit -m "release: vX.Y.Z — <theme>"
   ```

2. **Tag** the release:
   ```
   git tag -a vX.Y.Z -m "vX.Y.Z: <theme>"
   ```

3. **Push** (ask for confirmation first):
   ```
   git push origin master-main --tags
   ```

3.5. **REQUIRED — Watch CI on the release commit.** Never assume CI will pass just because the local pytest run passed. CI installs from `requirements-lock.txt` (not `requirements.txt`), runs on Ubuntu-latest with Python 3.11, and exercises a different subset of the suite. The local test run can be green while CI fails to even collect tests (the v7.14.0 release hit exactly this: 8 collection errors on `ModuleNotFoundError: No module named 'fitz'` because `pymupdf` was missing from the lock file).

   ```bash
   # Find the latest CI run for the release commit
   gh run list --branch master-main --limit 3
   # Once you know the run ID, watch until it finishes
   gh run watch <run-id>  # blocks until completion, streams logs
   ```

   Alternatively, use the Monitor tool with a polling `gh run view` loop until the run reaches `completed` status.

   **If CI passes:** continue to step 4 (deploy web).

   **If CI fails:** STOP. Do NOT deploy to production with a red CI run on the release commit. Read the failure logs (`gh run view <id> --log-failed`), diagnose root cause, push a hotfix commit, and re-watch CI. Only proceed to deploy once CI is green. (For the v7.14.0 incident the user accepted that web+desktop were already deployed and live, but the right default is to gate deploy on CI — not parallel.)

4. **Deploy web** (if web or both):
   - Run: `ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "cd /home/ubuntu/GenizahSearch && ./deploy.sh"`
   - Verify service restarted successfully

5. **Web smoke test** (if web or both):
   - Use WebFetch to GET `https://genizahsearch.com` — verify HTTP 200
   - Check that the response body contains the new version string (X.Y.Z)
   - If the smoke test fails, alert the user immediately and suggest rollback (see Phase 9)

6. **Create GitHub Release** — **DESKTOP OR BOTH ONLY. NEVER for web-only releases.**

   **Why this matters:** the installed desktop app polls `https://api.github.com/repos/gershuni/GenizahSearch/releases/latest` (`UpdateCheckerThread` in `gui_threads.py:445`) and prompts every desktop user to update whenever a new tag becomes `latest`. A web-only GitHub release has no installer attached, so the prompt sends users to a release page they cannot install from. **Past incident: v7.9.3 (web-only) created a release, every desktop user was prompted to "update" to a no-installer page.**

   - **If web-only:** SKIP this step entirely. Do NOT run `gh release create`. The git tag itself is fine (tags do not appear in `/releases/latest`); only the GitHub Release object triggers the desktop update prompt.
   - **If desktop or both:**
     ```
     gh release create vX.Y.Z --title "vX.Y.Z: <theme>" --notes-file -
     ```
     - Then upload the installer (required — a release without one strands desktop users):
     ```
     gh release upload vX.Y.Z <installer-path>
     ```

7. **Post-deploy verification**:
   - For web: confirm smoke test passed, suggest user also checks manually
   - For desktop: remind to verify the installer download works from GitHub releases

## Phase 8: Wrap Up

- Summarize what was released
- **Final CI confirmation:** run `gh run list --branch master-main --limit 3` and confirm the latest run (release commit + any hotfixes) is `completed success`. If still in-progress, set a Monitor and don't declare the release done until it lands green.
- List any deferred items or known issues for next release
- Remind about any follow-up tasks (e.g., "announce to users", "monitor error tracking")

## Phase 9: Rollback (only if something goes wrong)

This phase is NOT part of the normal flow. Only use if the deploy fails or the user reports a critical issue.

**Web rollback:**
```bash
# SSH to server, revert to previous commit
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "cd /home/ubuntu/GenizahSearch && git checkout HEAD~1 && sudo systemctl restart genizah-web"
```

**Desktop rollback:**
- The previous installer is still on GitHub releases — users can download it
- No action needed server-side

**Git rollback (if needed):**
```bash
# Revert the release commit (creates a new commit, safe)
git revert HEAD --no-edit
git push origin master-main
# Do NOT delete the tag yet — discuss with user first
```

Always explain what happened and why before executing any rollback.
