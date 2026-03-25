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
- `python scripts/check_docs.py` — documentation health OK?
- No `TODO(release)` or `FIXME(urgent)` markers in recently changed files
- `OPEN_ISSUES.md` — any critical/blocking issues still open?
- Grep for `print(` debug statements in recently committed code (last 5 commits)
- Version consistency: `version.py`, `CompileScriptGenizah.iss`, `version_info.txt`, `README.md` all match current version (not yet bumped)

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

6. **GitHub Release draft** — Title + body for the GitHub release. Include:
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
[ ] CHANGELOG.md has new section with correct date
[ ] README.md "What's New" updated (or N/A)
[ ] CLAUDE.md "Recently Changed" updated
[ ] Web What's New banner text updated (or N/A)
[ ] Desktop What's New bar + dialog updated (or N/A)
[ ] Help pages updated (or N/A)
[ ] OPEN_ISSUES.md up to date
[ ] All tests passing
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

1. Run `build_app.bat` — PyInstaller build (this takes several minutes)
   - Run with timeout of 600000ms (10 min)
   - Verify `dist/GenizahSearchPro/GenizahSearchPro.exe` exists after build
2. Run Inno Setup CLI to create installer:
   ```
   "/c/Program Files (x86)/Inno Setup 6/ISCC.exe" CompileScriptGenizah.iss
   ```
   - Verify the output .exe installer was created
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

4. **Deploy web** (if web or both):
   - Run: `ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "cd /home/ubuntu/GenizahSearch && ./deploy.sh"`
   - Verify service restarted successfully

5. **Web smoke test** (if web or both):
   - Use WebFetch to GET `https://genizahsearch.com` — verify HTTP 200
   - Check that the response body contains the new version string (X.Y.Z)
   - If the smoke test fails, alert the user immediately and suggest rollback (see Phase 9)

6. **Create GitHub Release** (if desktop or both):
   ```
   gh release create vX.Y.Z --title "vX.Y.Z: <theme>" --notes-file -
   ```
   - If desktop installer was built, upload it:
   ```
   gh release upload vX.Y.Z <installer-path>
   ```

7. **Post-deploy verification**:
   - For web: confirm smoke test passed, suggest user also checks manually
   - For desktop: remind to verify the installer download works from GitHub releases

## Phase 8: Wrap Up

- Summarize what was released
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
