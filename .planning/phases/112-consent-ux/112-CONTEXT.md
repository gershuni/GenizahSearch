# Phase 112: Consent UX - Context

**Gathered:** 2026-06-14
**Status:** Ready for planning

<domain>
## Phase Boundary

Build the **consent surface** for desktop telemetry on top of the Phase 111 engine: a one-time bilingual first-run consent dialog (`show_first_run_prompt()`), a Settings toggle bound to the same `set_consent()` source of truth, and a standalone bilingual privacy disclosure reachable from both. This is **pure UI composition + wiring** — the engine already exists in `desktop/telemetry.py` (consent persistence, UUID mint, audit fields, opt-out queue-drain).

Requirements (from ROADMAP/REQUIREMENTS): CONSENT-02, CONSENT-03, CONSENT-04, CONSENT-08, PRIV-05.

**Already complete from Phase 111 (do NOT re-implement):**
- **CONSENT-08** (opt-out drains/discards queued events) — done inside `set_consent(False)` via `_drain_and_discard()`. Marked Complete in REQUIREMENTS.md.
- The consent **audit record** (timestamp / app version / consent-UI version `'1'`) is written by `set_consent(True)` — CONSENT-03's audit-trail fields already persist on opt-in.
- All `config.pkl` key constants: `TELEMETRY_ENABLED_KEY`, `FIRST_RUN_SHOWN_KEY`, `TELEMETRY_INSTALL_ID_KEY`, `CONSENT_TIMESTAMP_KEY`, `CONSENT_APP_VERSION_KEY`, `CONSENT_UI_VERSION_KEY`, `IDENTIFIED_USER_KEY`.

**Locked by Success Criteria (not open for discussion):** dialog shown at most once (write `FIRST_RUN_SHOWN_KEY=True` unconditionally after any choice); two equal-weight buttons with **no** pre-selection and **no** Enter-key default (Enter without reading ⇒ opted out); default OFF until explicit consent.

**Out of phase (later):** exception/crash hooks (113), usage events (114), perf events (115), CI privacy gate (116). No events fire in this phase either — producers begin in 113+.
</domain>

<decisions>
## Implementation Decisions

### First-run dialog — language presentation
- **D-01:** The first-run consent dialog shows **both EN and HE text stacked** in the one dialog (English block + Hebrew block, or two columns — layout is planner/UI-spec discretion). Rationale: first launch defaults to English (`load_language()` returns `'en'` when no language file exists) before the user has touched the language switch, so a `tr()`-gated single-language dialog would show a Hebrew-first user an English-only consent prompt. "Bilingual EN/HE modal" (SC#1) is taken literally. **Do NOT build the dialog with `tr()` alone** — both languages must be visible regardless of `CURRENT_LANG`.
- **D-02:** Same bilingual-stacked treatment applies to the privacy disclosure (D-07) for consistency (satisfies PRIV-05's "bilingual EN/HE").

### First-run dialog — timing in startup
- **D-03:** The modal appears **after the main window paints** (queue it post-`show()`, e.g. `QTimer.singleShot(0, ...)` or equivalent after the window's startup work). The user sees the real app behind the prompt; it never blocks heavy startup (index load, recovery).
- **D-04:** It must **not stack on top of** the interrupted-indexing recovery modal ("התאוששות מאינדוקס שהופסק") or the sync prompt — those first-launch modals resolve **first**, then the consent modal shows. (Both are `QDialog.exec()` nested loops; a naïve `singleShot(0)` can fire *inside* a recovery modal's loop and stack two modals — planner must sequence so consent shows only after they close.)
- **D-05:** Shown **exactly once**, gated on `FIRST_RUN_SHOWN_KEY`. After any choice (Accept, Decline, or close/Escape), `FIRST_RUN_SHOWN_KEY=True` is written unconditionally; later launches skip it. Closing the dialog without choosing = opted out (consent stays False/default).

### Settings toggle — placement + apply semantics
- **D-06:** Toggle lives in **SettingsDialog → General tab → Preferences group**, as a checkbox alongside Desktop Notifications / Show Translations (e.g. "Help improve the app — send anonymous usage data"), with a **"Privacy details" link** beside it that opens the PrivacyDialog (D-07).
- **D-07a:** **Confirm-on-change → immediate-apply.** Flipping the checkbox first shows a small confirm ("telemetry will start / stop now"); on confirm, it calls `set_consent(...)` **immediately** (not staged). On cancel-of-confirm, the checkbox reverts to its prior visual state with no `set_consent` call.
- **D-07b:** Because it applies immediately, the telemetry consent keys are **exempt from `SettingsDialog`'s `_config_snapshot` / `_on_cancel` raw `save_app_config` restore** — Cancel must NOT silently rewrite `telemetry_enabled` out of sync with the in-memory `_enabled` cache + transport wiring that `set_consent()` already mutated. Planner must exclude the telemetry keys from the snapshot/restore (or re-apply them after restore).
- **D-08:** Both the toggle and the dialog route consent changes **only through `set_consent()`** — never raw `save_app_config({'telemetry_enabled': ...})`. `set_consent()` is the sole source of truth (mints UUID, drains queue, wires/revokes transport, stamps audit fields).

### Privacy disclosure — form + reach
- **D-09:** A **standalone bilingual `PrivacyDialog`** (QDialog, EN+HE stacked) is the single source of truth. Opened from BOTH the first-run dialog's **"Learn more"** button AND the Settings **"Privacy details"** link. Decoupled from `SettingsDialog`'s lifecycle so the first-run flow (which runs before the user opens Settings) can open it directly.
- **D-10:** Disclosure content must cover, bilingually: **what is collected** (anonymous usage/feature counts, version/OS, performance buckets, crash signals — counts only), **what is NOT** (no search/query content, no My Library file paths or filenames, no email/name beyond the bare Supabase user id when logged in), **who processes it** (PostHog, EU region + Dicta), **how to opt out** (the Settings toggle), and that the **install id is a pseudonymous identifier**. Consistent with the existing "Local Index Cache Privacy" posture already in the About tab.

### Claude's Discretion (within the locked SC + decisions above)
- Exact EN/HE button labels (must be equal-weight, no default — e.g. "Enable" / "Not now"); the one-line summary shown **inline** in the first-run dialog vs deferred to "Learn more" (SC#5 wants enough in-dialog that the disclosure is *reachable*, not necessarily fully inline).
- Stacked-vertical vs two-column visual layout of the bilingual text.
- The exact `PrivacyDialog` copy (within D-10's required points).
- **PRIV-05 satisfaction breadth:** the canonical full text lives in `PrivacyDialog`; planner should also add a brief telemetry-privacy pointer + link in the **About tab** (and optionally a Help.html section) so the existing disclosure posture stays consistent — discretion on exactly where, as long as a logged-in user reading About/Help can discover the telemetry disclosure.
- Whether to stamp a fresh consent-audit record on **opt-out** (engine currently stamps audit fields on opt-in only) — optional nicety, not required by SC.
</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & roadmap (locked)
- `.planning/REQUIREMENTS.md` — Phase 112 covers CONSENT-02/03/04/08, PRIV-05. Note CONSENT-08 is already **Complete** (Phase 111). MUST read.
- `.planning/ROADMAP.md` §"Phase 112: Consent UX" — goal + 5 success criteria (the locked invariants in `<domain>`).

### Prior-phase context (the engine this phase drives)
- `.planning/phases/111-telemetry-foundation/111-CONTEXT.md` — D-01..D-10 of Phase 111 (shared web project, identity alignment, consent-gate placement). The identity/queue decisions still govern.
- `.planning/phases/111-telemetry-foundation/111-PATTERNS.md` — pattern map for new telemetry files.
- `.planning/research/POSTHOG-PROJECT-DECISION.md` — the reversed-decision rationale (one shared project; identity-aligned). Background only.

### Code to read / extend
- `desktop/telemetry.py` — the engine. **Implement the `show_first_run_prompt()` stub (line 712).** Reuse `set_consent()`, `is_enabled()`, `get_install_id()`, and the `*_KEY` constants. NOTE: `desktop/telemetry.py` is the ONLY `desktop/` file allowed to import `shared.posthog_server` (PRIV-03 AST guard) — the dialog/toggle UI must go through `desktop/telemetry.py`'s public API, never the transport directly.
- `genizah_app.py:2145-2513` — `SettingsDialog` (General + About tabs, `_config_snapshot` / `_on_cancel` restore, the Preferences `_pref_row` helper + existing checkboxes). Toggle lands here (D-06).
- `genizah_app.py` startup path — `SettingsDialog` is instantiated once at `:3438`; the "What's New" inline bar wiring is `:3338-3551`. First-run modal hooks into the post-`show()` startup (D-03/D-04). Verify exact `MainWindow.__init__` / `showEvent` site during research.
- `genizah_app.py:284` `WhatsNewDialog` + `:1447` `HelpDialog` — existing bilingual-dialog precedents to mirror for `PrivacyDialog`'s structure (RTL handling via `setLayoutDirection`, palette-aware colors).
- `genizah_core.py:2852-2900` — `load_language()` (defaults `'en'`), `save_language`, `load_app_config`/`save_app_config`, `tr()` (language-gated — confirms D-01's "tr() alone won't show both languages"), `CURRENT_LANG`.
- Recovery-modal sequencing precedent — `desktop/my_library_tab.py` `_show_recovery_modal` / `GenizahGUI.closeEvent` `sweep_running_scan_runs()` (per the 2026-05-27 recovery-modal-recurrence fix) — relevant to D-04 not-stacking-on-recovery-modal.

### Disclosure-posture precedent
- `genizah_app.py:2472-2476` — the existing "Local Index Cache Privacy" About-tab block; the new telemetry disclosure should match its tone/placement posture (PRIV-05).
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `desktop/telemetry.set_consent(bool)` — single source of truth; handles UUID mint, audit fields, queue drain, transport wiring. The toggle + dialog call this.
- `desktop/telemetry.is_enabled()` — to set the toggle's initial checked state.
- `desktop/telemetry.FIRST_RUN_SHOWN_KEY` + `load_app_config()`/`save_app_config()` — the once-only gate for the first-run dialog.
- `SettingsDialog._pref_row()` + existing checkbox rows (`chk_notifications`, `chk_show_translations`) — copy the row pattern for the telemetry toggle (D-06).
- `WhatsNewDialog` / `HelpDialog` — QDialog precedents (RTL, palette-aware colors, `QTextBrowser` for HTML) to model `PrivacyDialog` on.

### Established Patterns
- **Consent routed only through `desktop/telemetry.py`** — PRIV-03 AST guard forbids any other `desktop/` file from touching `shared.posthog_server`. UI calls the public API.
- **Bilingual dialogs** elsewhere choose one language via `CURRENT_LANG`/`tr()`; this phase deliberately diverges for the consent dialog + disclosure (both languages always visible — D-01/D-02).
- **`config.pkl` (NOT QSettings, NOT session.json)** — session.json is wiped by crash recovery; consent must persist in `config.pkl` (already enforced engine-side).

### Integration Points
- `show_first_run_prompt()` stub in `desktop/telemetry.py` → implemented to build/exec the bilingual modal (or delegate to a `desktop/`-side dialog class it imports — planner decides module placement; keep `posthog_server` import confined to `telemetry.py`).
- `SettingsDialog._build_general_tab()` → add the telemetry checkbox + "Privacy details" link; exclude its keys from `_config_snapshot`/`_on_cancel` (D-07b).
- Startup (`MainWindow.__init__` / post-show) → call `show_first_run_prompt()` once, after recovery/sync modals resolve (D-03/D-04).
- New `PrivacyDialog` (likely `desktop/`) → opened from both entry points (D-09).
</code_context>

<specifics>
## Specific Ideas

- First-run dialog: two equal-weight buttons, no default, no Enter-shortcut; Enter/Escape/close ⇒ opted out. "Learn more" opens `PrivacyDialog`.
- Toggle label tenor: "Help improve the app — send anonymous usage data" (EN) + Hebrew equivalent; "Privacy details" link beside it.
- Confirm-on-change microcopy: a short "telemetry will start/stop now" confirm before `set_consent()` fires.
- Disclosure must explicitly state: no search/query content, no My Library paths/filenames; PostHog (EU) + Dicta as processors; opt-out via Settings; pseudonymous install id.
</specifics>

<deferred>
## Deferred Ideas

- **CONSENT-F1** ("Reset telemetry id" affordance in Settings) — already deferred to Future in STATE.md; user chose keep-id-on-opt-out. Not in 112.
- **Web consent gate** — the web app identifies real users (email/name) with no opt-in gate; harmonizing is out of v8.1.0 (flagged in 111-CONTEXT.md).
- Producers (crash/usage/perf events) — Phases 113-115. No event fires from 112.

None — discussion stayed within phase scope (no new capabilities raised).
</deferred>

---

*Phase: 112-consent-ux*
*Context gathered: 2026-06-14*
