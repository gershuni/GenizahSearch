# Phase 112: Consent UX - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-14
**Phase:** 112-consent-ux
**Areas discussed:** Dialog language presentation, First-run timing in startup, Settings toggle (place + apply), Privacy disclosure (form + reach)

---

## Dialog language presentation

| Option | Description | Selected |
|--------|-------------|----------|
| Both stacked (EN + HE) | Show both languages in the one dialog. Readable regardless of UI language; matches "bilingual" literally; avoids Hebrew-first user getting an English-only consent prompt. | ✓ |
| Follow UI language (tr()) | Show one language matching CURRENT_LANG, like About/Help. Cleaner but first-launch default is English. | |
| Pick language first | Tiny language toggle inside the dialog; consent text re-renders. More interactive, more to build. | |

**User's choice:** Both stacked (EN + HE)
**Notes:** Same bilingual-stacked treatment carried to the privacy disclosure for consistency (PRIV-05).

---

## First-run timing in startup

| Option | Description | Selected |
|--------|-------------|----------|
| After window paints | Window shows + finishes startup work, then modal pops (queued post-show). Least disruptive; doesn't block index load/recovery. | ✓ |
| Before window shows | Block at startup; window only after a choice. Most prominent but stacks on other first-launch flows and delays the app. | |
| Defer / low priority | Let other first-launch dialogs resolve, then show after a delay/idle. Gentlest, but risk of dismissal-without-reading. | |

**User's choice:** After window paints
**Notes:** Constraint added — must not stack on the interrupted-indexing recovery modal or sync prompt; those resolve first (relevant given the recovery-modal recurrence history). Shown once, gated on FIRST_RUN_SHOWN_KEY.

---

## Settings toggle — apply semantics

| Option | Description | Selected |
|--------|-------------|----------|
| Immediate-apply, exempt from snapshot | Toggle calls set_consent() on flip; keys excluded from Cancel snapshot/restore. Matches set_consent's side-effects. | |
| Honor OK/Cancel | Stage; only set_consent() on OK, revert on Cancel. Tidy but snapshot restore bypasses queue-drain/transport/UUID side-effects. | |
| Confirm-on-change | Flip triggers a small confirm, then immediate-apply. Extra friction, makes the irreversible-ish nature explicit. | ✓ |

**User's choice:** Confirm-on-change (→ immediate-apply via set_consent(); therefore exempt from the Cancel snapshot)

---

## Settings toggle — placement

| Option | Description | Selected |
|--------|-------------|----------|
| General → Preferences group | Checkbox alongside Desktop Notifications / Show Translations + "Privacy details" link. Most discoverable, consistent. | ✓ |
| New 'Privacy' subsection | Dedicated Privacy section/separator in General tab holding toggle + link. | |
| About tab | Toggle next to the existing "Local Index Cache Privacy" disclosure text. | |

**User's choice:** General → Preferences group

---

## Privacy disclosure — form + reach

| Option | Description | Selected |
|--------|-------------|----------|
| Standalone PrivacyDialog | One bilingual (EN+HE stacked) QDialog as single source of truth; opened from both first-run "Learn more" and Settings "Privacy details". Decoupled, reusable. | ✓ |
| About-tab section | Fold into the existing About tab; entry points open Settings on the About tab. Reuses surface but couples first-run flow to SettingsDialog. | |
| Help.html anchor | Add a privacy section to Help.html; open HelpDialog scrolled to an anchor. Consistent with other help, heavier to render/edit. | |

**User's choice:** Standalone PrivacyDialog
**Notes:** Content must cover what's collected / what's not (no search content, no My Library paths/filenames) / who processes (PostHog EU + Dicta) / how to opt out / pseudonymous install-id.

---

## Claude's Discretion

- Exact EN/HE button labels (equal-weight, no default).
- One-line inline summary in the first-run dialog vs deferred to "Learn more".
- Stacked-vertical vs two-column visual layout of the bilingual text.
- Exact PrivacyDialog copy (within the required content points).
- Where to add the brief telemetry-privacy pointer + link in About / Help.html for PRIV-05 posture consistency.
- Whether to stamp a fresh consent-audit record on opt-out (optional; engine stamps on opt-in only).

## Deferred Ideas

- CONSENT-F1 ("Reset telemetry id" affordance) — already Future per STATE.md; not in 112.
- Web consent gate (web identifies real users with no opt-in) — out of v8.1.0.
- Crash/usage/perf producers — Phases 113-115; no event fires from 112.
