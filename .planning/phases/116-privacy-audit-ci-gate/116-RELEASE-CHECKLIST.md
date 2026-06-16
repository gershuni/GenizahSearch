# v8.1.0 Desktop Telemetry — Milestone Close + Release Checklist

**Created:** 2026-06-16 (end of the phase-116 session) · **For:** next session
**Milestone:** v8.1.0 Desktop Telemetry (Phases 111–116). Phase 116 is the FINAL phase.
**Release type:** BOTH — desktop binary (the telemetry milestone) **and** a web change
(`platform=web` tag). A real desktop installer ships → a **GitHub release IS appropriate**
(the "no GitHub release for web-only" rule does NOT apply here).

---

## State at hand-off (already done this session — do NOT redo)

- Phase 116 plans executed + committed: 116-01 (PRIV-04 scrubber tests + `_safe_context`
  hardening), 116-02 (`send_selftest_event_sync` + `--telemetry-selftest` CLI), 116-03
  (TELEMETRY_RUNBOOK.md + INFRA-06 wording amend + milestone-exit gate doc).
- Phase 116 verified `human_needed` (`116-VERIFICATION.md`); code-review clean
  (`116-REVIEW.md`); **Codex cross-AI review addressed** (`116-CODEX-CODE-REVIEW.md` — HIGH
  phc_ self-test fix + MEDIUM test-determinism fix + BLOCKER-1 extension-set expansion;
  BLOCKER-2 rebutted as out-of-scope, covered by the D-17 guard).
- **UAT Test 1 (live delivery + on-the-wire privacy) PASSED** via production PostHog 134161:
  the 2026-06-16 frozen-exe session delivered `desktop_*` events over TLS with ZERO forbidden
  content (no query text / paths / filenames / traceback). Closes Phase 114 live-delivery UAT.
  See `116-HUMAN-UAT.md`.
- `platform=web` super-property added to the web posthog-js init (`web/main.py`, commit
  `36ae3fe7`) — **committed, NOT yet deployed**.
- PostHog dashboards built in project 134161: Desktop (id 752803), Web (752805),
  Comparison (752806).
- `GenizahSearchPro.spec` was clobbered by a local `build_app.bat` run this session and
  **restored** to the maintained version (do not commit a bare spec — see build note below).

---

## What's left to close + release (ordered)

### 1. Pre-flight gates (must be green on the release commit)
- [ ] `python -m ruff check .` — **run explicitly** (pre-flight has missed ruff before → CI F401 failures).
- [ ] Milestone-exit regression (the D-10 gate): run with GUI suppression locally
      `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen PYTHONUTF8=1 python -m pytest tests/test_telemetry*.py tests/test_no_dynamic*.py tests/test_no_raw_storage_access.py -m "not gui"`
      (this session: **241 passed**). CI runs the full suite on ubuntu + windows (SC#1); GUI tests run via the `gui-tests` marker split, NOT `-n auto`.
- [ ] `PYTHONIOENCODING=utf-8 python scripts/check_docs.py` (needs UTF-8 or it crashes on emoji under cp1255).

### 2. Milestone verification — flip the deferred requirement statuses
Per `116-VERIFICATION.md`'s deferral note, PRIV-04 + INFRA-06 were intentionally left `Pending`
until the UAT landed. UAT Test 1 (delivery + privacy) has now PASSED.
- [ ] Decide on Test 2 (strict clean-no-Python-VM `SSL_OK` + offline arm): the user accepted the
      current state. It naturally happens during the release build (step 4) — run it then to fully
      close SC#3, OR flip on the practical evidence already gathered.
- [ ] In `.planning/REQUIREMENTS.md`: flip **PRIV-04** and **INFRA-06** checklist boxes `[ ]→[x]`
      AND their Traceability rows `Pending→Complete` (PRIV-03 is already Complete). This is the
      "milestone verification pass" the deferral note hands off to.
- [ ] Mark Phase 116 complete: `gsd-sdk query phase.complete 116` (updates ROADMAP/STATE) — or via
      `/gsd-complete-milestone`.

### 2.5 Desktop advertising: public API + AI skill (NEW — added 2026-06-16, bundle into this release)
User decision (2026-06-16): advertise the public Search API + the `cairo-genizah-research` AI
skill **to desktop users**, folded into THIS v8.1.0 release (NOT a standalone commit — bundling
here avoids What's-New version-coordination churn and ships the Help content in the same exe).
Author this BEFORE the build (step 4) so it ships. Bilingual EN + HE, **skill-first** angle.
- [ ] **Persistent Help section** — add a "Public API & AI Tools" section to the desktop Help
      (bundled Help.html via `HelpDialog` / `open_help_center`, ~`genizah_app.py:1456`/`:16248`).
      MIRROR the EXISTING **web** Help "Public API & AI Tools" section (search `web/` — likely
      `web/pages/help.py`/help component) so desktop ↔ web copy + public URLs stay consistent.
      Match the Help file's existing section/anchor structure + bilingual mechanism.
- [ ] **One-time What's New line** — fold a single line into the **v8.1.0** What's New (the same
      block step 3 drafts; do NOT create a separate version for it). Bilingual.
- [ ] **Angle = SKILL-FIRST:** lead with "Use Dicta Genizah Search from Claude / AI agents" —
      the `cairo-genizah-research` skill (conversationally find parallels / candidate witnesses /
      piyyut-responsa matches with browse-grounded justifications; audience = non-developer
      researchers). THEN a shorter "For developers" API pointer: `POST /api/search`,
      `POST /api/parallels`, `GET /api/browse`; OpenAPI at `/api/openapi.json`; contract in
      `docs/SEARCH_API.md` + the public docs page the web Help links to. Do NOT invent copy/URLs —
      pull from the web Help section. Provide REAL Hebrew, not placeholders. Display name
      "Dicta Genizah Search Pro"; do NOT rename the binary.
- [ ] **Out of scope (do NOT do here):** any web-side advertising; any anti-bot / rate-limit /
      Cloudflare / API-key work. Site is already behind Cloudflare → edge bot-fight + a rate rule
      on `/api/*` is a future toggle if the API is ever advertised publicly; light vs heavy-mode
      (fuzzy/parallels) rate differentiation is the natural next web-side step (see quick task
      260616-p9x heavy-tier work).
- [ ] **Verify:** `HelpDialog` still constructs/loads (offscreen test:
      `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen ...`); the new Help section + What's New line
      present in BOTH languages; `ruff` clean on touched `.py`; `check_docs` passes (PYTHONUTF8=1).

### 3. Version bump (REQUIRED — this is a milestone release)
- [ ] `python scripts/bump_version.py 8.1.0` (updates version.py, version_info.txt,
      CompileScriptGenizah.iss, README header). Current version is **8.0.0**.
- [ ] **MANUAL (bumper misses these):**
  - [ ] `tests/test_release_artifacts.py` → bump `_TARGET_VERSION = "8.0.0"` → `"8.1.0"` (else CI fails on the release commit).
  - [ ] `CHANGELOG.md` → add `## [8.1.0]` Desktop Telemetry section.
  - [ ] `CLAUDE.md` "Recently Changed" → one-line v8.1.0 entry.
  - [ ] `README.md` "What's New" → telemetry feature blurb.

### 4. Desktop build + the clean-VM SSL gate (closes 116-HUMAN-UAT Test 2 / SC#3)
- [ ] Build `GenizahSearchPro.exe`. **Build gotchas (from prior releases):**
  - `build_app.bat` **clobbers `GenizahSearchPro.spec`** → `git restore GenizahSearchPro.spec`
    after building (the maintained spec carries `collect_all('pymupdf'/'zstandard'/'lxml')` +
    `fitz/pymupdf/openpyxl/defusedxml` hidden imports; a bare spec ships a broken exe).
  - `CompileScriptGenizah.iss` has hardcoded `C:\GenizahSearch\dist\...` paths → junction the
    dist folder before `ISCC.exe` so a worktree build doesn't use stale binaries.
  - Invoking `build_app.bat`: bare `cmd /c build_app.bat` fails — set
    `[Environment]::CurrentDirectory` + full `.bat` path.
  - The embedded `phc_` key is ALREADY baked in source (`desktop/telemetry.py`
    `_TELEMETRY_KEY_DEFAULT`), so the frozen exe carries it — no extra step.
- [ ] On a CLEAN Windows VM with NO Python: `GenizahSearchPro.exe --telemetry-selftest` → `SSL_OK`
      exit 0 (proves bundled certifi). `--telemetry-selftest-offline` → `OFFLINE_OK` fast; normal
      offline launch silent. Record in `116-HUMAN-UAT.md` Test 2.

### 5. Web deploy (ship the `platform=web` tag)
- [ ] Deploy the web app so commit `36ae3fe7` (platform=web) goes live. Code-only change (no DB),
      so standard deploy — but follow the project deploy flow (scp DBs first only if a DB changed;
      none here). Dashboards already work without it (they separate by event name).

### 6. Release mechanics
- [ ] Run `/release` (handles version bump if not done, What's New, code review, build, deploy,
      GitHub release). Select **both** (desktop + web).
- [ ] Run **`/gsd-complete-milestone`** EXPLICITLY — `/release` skips the GSD milestone-close
      ritual (MILESTONES.md / REQUIREMENTS.md archival drifted on v7.13/v7.14 without it).

---

## Optional / deferred (not blockers)
- Server-side web events (`search_api_request`, `nli_breaker_*`) remain `platform=(not set)` — not
  tagged `web`. Tag them only if you want the API surface attributed to web; future.
- Comparison dashboard scale mismatch (web ≫ desktop): toggle "Show multiple Y axes" or log scale
  on the two comparison line charts for readability.
- Add the three dashboard links to `docs/guides/TELEMETRY_RUNBOOK.md` for discoverability.
- BLOCKER-2 future hardening (runtime per-key enum value validators at the chokepoint) — logged in
  `116-CODEX-CODE-REVIEW.md`; covered today by the D-17 producer AST guard; not a v8.1.0 blocker.
- GeoIP note: PostHog derives city-level location from IP server-side (same as web). Consider a one
  line in the privacy disclosure ("approximate location inferred from IP, as on the website").
