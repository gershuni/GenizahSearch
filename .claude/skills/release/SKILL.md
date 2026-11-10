---
name: release
description: "Release GenizahSearch — version bump, What's New drafting, code review, release PR, publish, build, GitHub release, deploy. Supports: web-only, desktop-only, or both."
user-invocable: true
---

# /release — GenizahSearch Release Skill

You are orchestrating a release of GenizahSearch. This is a multi-step, partially interactive process. Follow each phase in order. Do NOT skip phases. Mark each phase clearly in your output.

## Where a release happens

- **This checkout is the development repository** (`origin`). All release changes reach its
  `master-main` through a pull request that the owner squash-merges in the browser. Never commit on
  `master-main`, never push it, and never run `gh pr merge`.
- **The public repository receives only `master-main` and tags, through the publishing step kept in
  the private repository.** The GitHub Release and the website deploy also go only through the
  wrappers kept there. Never push to the public repository and never run `gh release create` or
  `gh release upload` yourself. The exact commands are in the private repository's
  `docs/guides/RELEASE_PROCEDURE.md`; read it before Phase 7.
- **Order:** release PR merged -> publish that commit -> the full public CI passes on exactly that
  commit -> (desktop or both) build the installer from exactly that commit and test it -> tag the
  commit and publish the tag -> GitHub Release -> deploy that commit. The tag comes last of the
  checks, because a public tag can never be moved: anything that fails before it (the publishing
  step, the public CI, the packaging smoke, the installer test) is fixed through a new PR, and the
  fixed commit becomes the release commit under the same version.
- **The owner approves each outward step** (opening the PR, the real publish, the release, the
  deploy) in that moment. The owner runs the deploy.

## Phase 0: Gather Release Intent

First stand on the current development `master-main`, so that everything below is read from the
commit being released (not from a stale or feature branch):

```bash
git fetch origin
git switch --detach origin/master-main
```

Ask the user:
1. **What version?** (suggest next patch/minor/major based on current `version.py`)
2. **What type?** — `web`, `desktop`, or `both`
3. **One-line summary** of the release theme (e.g., "Image adjustment controls")

Read `version.py` to show the current version. Read recent `git log --oneline -20` and `docs/OPEN_ISSUES.md` to understand what's changed since last release.

Then start the release branch at that same commit:

```bash
git switch -c release/vX.Y.Z
```

## Phase 1: Pre-Flight Code Review

Send an agent (subagent_type=Explore) to verify the code is ready:

**Checks:**
- `git status` — working tree clean? Uncommitted changes?
- `python scripts/run_local_tests.py` — all tests pass? (the bounded runner; never one `pytest tests/`
  process — see `tests/README.md`). Its default expression excludes the lanes CI runs as separate
  jobs, so add them explicitly: `python scripts/run_local_tests.py -m render_smoke` (the NiceGUI
  render-smoke lane; REQUIRED for a web release) and `python scripts/run_gui_tests.py` (the Qt lane;
  REQUIRED for a desktop release). If the render-smoke lane includes a content-safety sweep
  maintained in the private repository, follow its own setup instructions there before running it --
  it is out of scope for this public skill. A web
  release that touches the Atlas builder or its baked asset also needs the atlas-bake lane CI
  keeps separate (`requirements-atlas-bake.txt` deps): `pip install -r requirements-atlas-bake.txt`
  then `pytest tests/atlas_bake -m atlas_bake`; the default runner deselects it.
- **`python -m ruff check .` — explicit ruff pass** (v7.12.0 CI failed on F401 unused imports; pre-flight must run ruff as its own line item, not implied by pytest)
- `python scripts/check_docs.py` — documentation health OK? (NOTE: on Windows console may fail with UnicodeEncodeError on emoji — that's environment-only, not a blocker)
- **`requirements.txt` vs `requirements-lock.txt` consistency** — every runtime dep in `requirements.txt` must have a matching pin in `requirements-lock.txt`. CI installs from the lock file, so a `requirements.txt` addition that's not lock-pinned breaks CI on the release commit. Diff check:
  ```bash
  # Extract package names (lowercase) from both files and compare
  comm -23 \
    <(grep -v '^#' requirements.txt | sed 's/[<>=!].*//' | tr '[:upper:]' '[:lower:]' | sort -u) \
    <(grep -v '^#' requirements-lock.txt | sed 's/==.*//' | tr '[:upper:]' '[:lower:]' | sort -u)
  ```
  If anything prints, those packages are in `requirements.txt` but absent from the lock file — block the release until they're added to `requirements-lock.txt`. (v7.14.0 hit this: pymupdf was in `requirements.txt` but missing from the lock, so CI failed post-push.)
- No `TODO(release)` or `FIXME(urgent)` markers in recently changed files
- `OPEN_ISSUES.md` — any critical/blocking issues still open?
- Grep for `print(` debug statements in recently committed code (last 5 commits)
- Version consistency: `version.py`, `CompileScriptGenizah.iss`, `version_info.txt`, `README.md` all match current version (not yet bumped)
- **`tests/test_release_artifacts.py` `_TARGET_VERSION`** — `bump_version.py` does NOT update this constant. Check its current value matches the pre-bump version; you'll need to bump it manually in Phase 3.

The development repository runs only the light CI check; the full suite runs in the public
repository after publishing (Phase 7). The local runs above are what stands between a mistake and a
red public CI, so do not skip them.

**Report** findings to the user. If there are blockers, stop and ask how to proceed. If there are warnings, list them and ask for confirmation to continue.

## Phase 2: Interactive "What's New" Drafting (OPTIONAL)

**Ask the user first:** "Do you want to draft What's New / release texts now? You can do all, some, or skip entirely."

The user may choose any subset of the items below, or skip this phase entirely. Only draft what they ask for.

This is a multi-round interactive process. Do NOT rush it.

### Step 2a: Gather material
- Read `git log --oneline` since the last version tag
- Run `python scripts/collect_changelog.py --apply` first: PRs write their entries as files in `changelog.d/`, and this folds them into `CHANGELOG.md` as `[Unreleased]` entries (and deletes the files)
- Read recent entries in `CHANGELOG.md`
- Identify the key user-facing changes (not internal refactors)

### Step 2b: Draft requested release texts
Present drafts for whichever items the user wants. The full menu is:

1. **CHANGELOG.md entry** — Full detailed changelog section with `### New Features`, `### Improvements`, `### Bug Fixes` subsections as appropriate. Technical but readable. Cite versions, not PR numbers: development PR numbers do not exist in the public repository.

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

Every text that goes public (changelog, README, GitHub Release notes, tag message) describes only
the change and its observable effects for users.

### Step 2c: Review loop
Present all requested drafts to the user at once. Ask:
> "Please review these drafts. Tell me what to change — wording, emphasis, additions, removals. We can do as many rounds as you need."

Iterate until the user approves. Pay attention to:
- Hebrew quality (the user is a native speaker)
- Consistency across chosen text locations
- Proper bilingual coverage (EN + HE)

## Phase 3: Version Bump + Apply Texts

Once texts are approved (or Phase 2 was skipped), on the release branch:

1. Run `python scripts/bump_version.py X.Y.Z` — updates version.py, version_info.txt, CompileScriptGenizah.iss, README.md
2. Apply all approved text changes (only those drafted in Phase 2):
   - Edit `CHANGELOG.md` — add the approved section at top
   - Edit `README.md` — update "What's New" section
   - Edit `web/main.py` — update the What's New banner text
   - Edit `genizah_app.py` — update WhatsNewBar message + WhatsNewDialog content
   - Edit `CLAUDE.md` "Recently Changed" section — add entry
   - Add the version's entry to `shared/whats_new_data.py` (newest first: title and items, each
     a feature / improvement / fix, EN + HE, with the apps it is in), from the approved texts. It
     feeds the website's `/whats-new`, `GET /api/whats-new` (the desktop update notice reads it) and
     the desktop's "What's New in every version"; `tests/test_whats_new_history.py` fails while the
     bumped version has no entry
   - Edit help files if applicable
3. Run `python scripts/check_docs.py` to verify docs are still healthy
4. Update `docs/OPEN_ISSUES.md` — mark any fixed issues, update timestamp

## Phase 4: "Did You Forget?" Checklist

Present this checklist BEFORE the release PR. Adapt it based on what was done in Phase 2 (mark N/A for skipped items):

```
RELEASE CHECKLIST — vX.Y.Z
==============================
[ ] On a release branch from origin/master-main (not on master-main)
[ ] Version bumped in all files (version.py, .iss, version_info.txt, README)
[ ] tests/test_release_artifacts.py _TARGET_VERSION manually bumped (bump_version.py misses it)
[ ] CHANGELOG.md has new section with correct date
[ ] README.md "What's New" updated (or N/A)
[ ] CLAUDE.md "Recently Changed" updated
[ ] Web What's New banner text updated (or N/A)
[ ] Desktop What's New bar + dialog updated (or N/A)
[ ] Help pages updated (or N/A)
[ ] OPEN_ISSUES.md up to date
[ ] All tests passing (default, render_smoke for web, gui for desktop)
[ ] ruff explicit pass (python -m ruff check .)
[ ] requirements.txt ↔ requirements-lock.txt diff is empty (CI uses the lock file; missing deps break CI)
[ ] No uncommitted changes (besides release changes)
[ ] Translations present for new UI strings

FOR DESKTOP:
[ ] Sidecar databases checkpointed (build_app.bat does this)

FOR WEB:
[ ] No breaking changes to web-only features
```

Ask: "Anything else you want to verify before the release PR?"

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
Release PR:     release/vX.Y.Z -> master-main in the development repository (you merge it)
Publish:        the merged commit through the publishing step, then the full public CI on it
Desktop build:  PyInstaller + Inno Setup from exactly that commit, then your installer test
                                                                    (or "skipped — web only")
Tag:            vX.Y.Z on that commit, published on its own
GitHub release: through the release wrapper, with the installer    (or "skipped — web only")
Web deploy:     the deploy step, exactly that commit (you run it)   (or "skipped — desktop only")
```

Ask: "Does this look right? Proceed with the release PR?"

## Phase 6: Release PR (development repository)

1. **Stage the release changes by name** and commit. Never `git add -A` or `git add .`: the
   checkout holds data and local files that must never be committed.
   ```bash
   git status --porcelain
   git add version.py version_info.txt CompileScriptGenizah.iss README.md CHANGELOG.md CLAUDE.md
   git add <every other file this release changed, by name>
   git commit -F <message file>     # first line: release: vX.Y.Z — <theme>
   ```
2. **Push the branch and open the PR** (ask first). The squash-merge uses the PR title as the
   commit message, so the title is the release commit's message: `release: vX.Y.Z — <theme>`.
   ```bash
   git push -u origin HEAD         # the release branch you are on (release/vX.Y.Z, or a -fixN branch)
   gh pr create --base master-main --title "release: vX.Y.Z — <theme>" --body-file <body file>
   ```
3. **The owner squash-merges it in the browser.** Note the merge commit's FULL id (40 characters)
   and wait until the development repository's light CI check on EXACTLY that commit has
   completed successfully (a run for another commit proves nothing):
   ```bash
   git fetch origin
   git log -1 --format=%H origin/master-main
   gh run list --commit <full merge commit id> --json headSha,workflowName,status,conclusion
   ```
   A red light check stops the release; fix it as described in "Fixing a failure before the
   tag" (after Phase 8), never on this already merged branch. From the merge until the tag is
   published (Phase 9), nothing else may be merged into the development `master-main`: the release
   tag must point at its tip. Keep the recorded id: every later phase uses exactly it.

## Phase 7: Publish, Public CI

1. **Publish the merged commit** through the publishing step in the private repository: a dry run
   first, then the real run with the merged commit as the expected candidate (the owner approves
   the real run). No tag yet. The commands are in `docs/guides/RELEASE_PROCEDURE.md` there.
   - If it **refuses**, nothing was published. Report what it names and stop; never work around it.
2. **REQUIRED — the full public CI must pass on exactly that commit.** Only the public repository
   runs the heavy jobs (Ubuntu, Python 3.11, installs from `requirements-lock.txt`). The local
   run can be green while CI fails to even collect tests (v7.14.0: 8 collection errors on
   `ModuleNotFoundError: No module named 'fitz'`). Watch the public CI run for the published
   commit (the procedure document shows how). **If it fails:** STOP. No tag, no release, no
   deploy. Fix it as described in "Fixing a failure before the tag" (after Phase 8).

## Phase 8: Build (Desktop only — skip for web-only releases)

Build from a checkout that is EXACTLY the published commit, with a clean `git status` and the data
folders the installer bundles (the main checkout, not a worktree: worktrees have no data). Check out
the recorded id itself, never "the newest `master-main`":

```bash
git switch --detach <full published commit id>
git rev-parse HEAD                 # must equal the published commit
git status --porcelain             # must print nothing
```

If anything in this phase fails (the build, the packaging smoke, Inno Setup, the installer test)
and the fix needs a change in the repository, follow "Fixing a failure before the tag" below.
Nothing is tagged yet, so the version number stays.

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
> $repo = (git rev-parse --show-toplevel) -replace '/', '\'; Set-Location -LiteralPath $repo; [Environment]::CurrentDirectory = $repo; cmd /c "$repo\build_app.bat"; Write-Output "BUILD_EXIT=$LASTEXITCODE"
> ```

1. Run `build_app.bat` (via the invocation above) — PyInstaller build (several minutes)
   - Verify `dist/GenizahSearchPro/GenizahSearchPro.exe` exists after build
   - **REQUIRED -- run the packaging smoke against the fresh build**, with the flag that turns a
     missing EXE into a failure instead of a skip:
     `$env:GENIZAH_PACKAGING_SMOKE='1'; python -m pytest tests/test_local_pyinstaller_smoke.py`
     (Tier 2 runs the EXE's `--self-test-pymupdf`; from 2026-04 to 2026-09-18 this test looked for
     the EXE at the wrong path and skipped on every release, which is how a frozen self-test that
     could never pass went unnoticed. It must be green before the installer is compiled.)
   - `build_app.bat` builds from the **checked-in** `GenizahSearchPro.spec`
     (`python -m PyInstaller --noconfirm --clean GenizahSearchPro.spec`, `build_app.bat:13-21`), so the
     maintained `collect_all('pymupdf')`/`collect_all('zstandard')`/`collect_all('lxml')` calls and the
     `fitz`/`openpyxl`/`defusedxml` hidden imports are used as committed. The build does not rewrite
     the spec; a spec change goes through the release PR like any other file.
2. Run Inno Setup CLI to create installer (same CWD caveat — full paths):
   ```powershell
   $repo = (git rev-parse --show-toplevel) -replace '/', '\'; Set-Location -LiteralPath $repo; [Environment]::CurrentDirectory = $repo; & "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe" "$repo\CompileScriptGenizah.iss"; Write-Output "ISCC_EXIT=$LASTEXITCODE"
   ```
   - Verify the output `.exe` installer was created (`dist/GenizahSearchPro_VX.Y.Z_Setup.exe`)
   - (Inno compresses the ~2.3 GB payload with LZMA — allow ~5 min.)
3. Report build output sizes to user. `git status --porcelain` must still print nothing.

### Desktop Installer Test Gate

**Do NOT proceed until the user confirms they tested the installer.**

Ask: "Please install and launch the built installer to verify it works. Confirm when ready to continue."

Wait for the user's confirmation before moving to Phase 9.

### Fixing a failure before the tag

A failure after the release PR is merged — the light check (Phase 6), the public CI (Phase 7), or
the build, packaging smoke or installer test (Phase 8) — is fixed under the same version, because
nothing is tagged yet. The checkout is then on the merged release branch
or detached at the published commit, and neither is a place for the fix: start a NEW branch from
the current development `master-main` first (the freeze means it is still the published commit):

```bash
git fetch origin
git switch -c release/vX.Y.Z-fix1 origin/master-main
```

Make the fix there, then run Phase 6 again with this branch (push it, open its PR titled
`release: vX.Y.Z — fix: <what>`; the owner merges it; its light check on the new merge commit),
then Phases 7-8 for that merge commit, which becomes the release commit. Number further fixes `-fix2`, `-fix3`.

## Phase 9: Tag

Only after the full public CI passed on the recorded commit (Phase 7) and, for a desktop or both
release, after the user confirmed the installer test (Phase 8). Tag exactly that commit
(annotated; the tagger is this checkout's configured identity), push the tag to the development
repository, and publish the tag on its own through the publishing step: a dry run, then the real
run naming the tag as the release tag and the same commit as the expected candidate (owner
approves):

```bash
git tag -a vX.Y.Z <full published commit id> -m "vX.Y.Z: <theme>"
git push origin vX.Y.Z
```

Once the tag is public, other PRs may be merged again.

## Phase 10: GitHub Release — DESKTOP OR BOTH ONLY. NEVER for web-only releases.

**Why this matters:** the installed desktop app polls `https://api.github.com/repos/gershuni/GenizahSearch/releases/latest` (`UpdateCheckerThread` in `desktop/gui_threads.py`) and prompts every desktop user to update whenever a new tag becomes `latest`. A web-only GitHub release has no installer attached, so the prompt sends users to a release page they cannot install from. **Past incident: v7.9.3 (web-only) created a release, every desktop user was prompted to "update" to a no-installer page.**

- **If web-only:** SKIP this phase entirely. The tag itself is fine (tags do not appear in
  `/releases/latest`); only the GitHub Release object triggers the desktop update prompt.
- **If desktop or both:** make the release only through the release wrapper in the private
  repository (see its `docs/guides/RELEASE_PROCEDURE.md`): it checks the build output and the
  notes, freezes the installer, and creates the release only for a published tag whose public CI
  passed. Run its check first and show the owner what it reports; the owner approves the frozen
  installer by its hash before the real run. If it refuses, report what it names and stop.

## Phase 11: Deploy (web or both)

1. **Deploy exactly the published commit** with the deploy step in the private repository (its
   procedure document): it checks that the id is published and that the full public CI passed on
   it, then the server deploys that commit and nothing else. **The owner runs it**; give the
   command and wait for the output.
   - Sidecar databases that the release needs go to the server with their own deploy scripts,
     in the order the release notes say (code first, or data first).
2. **Web smoke test:**
   - Use WebFetch to GET `https://genizahsearch.com` — verify HTTP 200
   - Check that the response body contains the new version string (X.Y.Z)
   - If the smoke test fails, alert the user immediately and suggest rollback (see Phase 13)
3. **Post-deploy verification**:
   - For web: confirm smoke test passed, suggest user also checks manually
   - For desktop: remind to verify the installer download works from GitHub releases

## Phase 12: Wrap Up

- Summarize what was released
- Put the main checkout back on its branch (`git switch master-main`, then
  `git pull --ff-only origin master-main`); a desktop build left it on the release commit.
- **Final CI confirmation:** the public CI run on the published commit is `completed success`.
  If still in progress, don't declare the release done until it lands green.
- List any deferred items or known issues for next release
- Remind about any follow-up tasks (e.g., "announce to users", "monitor error tracking")

## Phase 13: Rollback (only if something goes wrong)

This phase is NOT part of the normal flow. Only use if the deploy fails or the user reports a critical issue.

**Web rollback:** deploy the previous published commit with the same deploy step (the server
deploys any published commit, and only those). Never change files or check out commits on the
server by hand.

**Desktop rollback:**
- The previous installer is still on GitHub releases — users can download it
- No action needed server-side

**Code rollback (if needed):** revert the release commit through a new PR in the development
repository, then publish it like any other change. Never delete, move or re-point a public tag:
public tags never change.

Always explain what happened and why before executing any rollback.
