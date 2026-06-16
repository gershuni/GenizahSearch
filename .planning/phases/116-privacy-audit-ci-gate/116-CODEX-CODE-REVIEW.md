VERDICT: CHANGES-REQUESTED

Privacy invariant: DOES NOT HOLD. There are concrete inputs that can reach `shared.posthog_server.enqueue_event` with forbidden content through the current `desktop.telemetry` chokepoint.

## BLOCKER

### `desktop/telemetry.py:355-386`, `desktop/telemetry.py:707-709` -- `_safe_context` still leaks filename-shaped context values outside the curated extension set

`_safe_context()` rejects only dotted values whose final segment appears in `_CONTEXT_FILE_EXTENSIONS`. Any real filename with an unlisted extension still matches `_CONTEXT_RE` and is returned verbatim, then `_emit()` restores it into `scrubbed['context']` from the pre-scrub value and calls `enqueue_event()`.

Examples that currently pass the regex and are not in the curated set: `local_index.sqlite3`, `genizah.log`, `private_notes.markdown`, `manifest.jp2`, `sources.bib`, `zotero.ris`. These are filenames, and filenames are explicitly forbidden content.

Why it matters: the generic `_PATH_RE` bare-filename redactor would catch many of these, but `context` intentionally bypasses that redacted value. A denylist of extensions cannot prove the PRIV-04 invariant.

Concrete fix: replace the extension denylist with an explicit allowlist of known static context labels, or reject all dotted filename-shaped values unless they are exact members of a `_KNOWN_CONTEXT_LABELS` allowlist. Add tests for unlisted real extensions plus existing legitimate labels such as `search_tab.run`, `app.crash`, and `search_tab.run_query`.

### `desktop/telemetry.py:295-331`, `desktop/telemetry.py:248-265`, `desktop/telemetry.py:679-709` -- allowlisted keys are not value-validated, so query/prose can ride through non-`context` keys

The property allowlist is key-only. For allowed keys other than `context`, `_scrub_value()` redacts paths and Hebrew but does not reject English free text. A call such as `track(DesktopEvent.SEARCH_EXECUTED, action='Maimonides rent letter')`, `search_mode='Maimonides rent letter'`, `feature_name='gersh private notes'`, or `$set={'q': 'Maimonides rent letter'}` survives validation and scrubbing and reaches `enqueue_event()`.

Why it matters: this violates the stated structural invariant. The system currently relies on producer discipline for low-cardinality enum values, but Phase 116 is supposed to prove forbidden search/query/user content cannot reach the network-bound queue.

Concrete fix: add per-key value validators at the chokepoint after `_validate_props()`: fixed enum sets for `tab_name`, `search_mode`, `corpus_scope`, `feature_name`, `dialog_name`, `action`, `operation_kind`, and bucket fields; strict UUID/version/numeric validators for IDs and counts; and either remove `$set` from public `track()` payloads or constrain it to internally generated identity fields only. Add raw-needle absence tests for every string allowlisted key, not only forbidden-key drops and `context`.

## HIGH

### `shared/posthog_server.py:164-168`, `shared/posthog_server.py:434-449`, `desktop/telemetry.py:481-487` -- self-test can POST a non-`phc_` env key despite the `NO_KEY` contract

`send_selftest_event_sync()` treats any non-empty `_resolve_api_key()` result as usable. `_resolve_api_key()` falls back to `POSTHOG_API_KEY` whenever `_api_key_override` is `None`. In the CLI path, `_wire_transport_config()` correctly rejects a non-`phc_` key by calling `set_capture_api_key(None)`, but the sync helper then falls back to the same invalid env var and performs a network POST.

Why it matters: the helper documents `NO_KEY` as "no phc_ key is configured; returns WITHOUT any network call" (`shared/posthog_server.py:417`). With `POSTHOG_API_KEY=phx_...` or any other non-ingestion token, this contract is false and key material is sent in the JSON payload.

Concrete fix: validate `api_key.startswith('phc_')` inside `send_selftest_event_sync()` before resolving URL or building the payload, and return `NO_KEY` without a POST when it fails. Add tests in `tests/test_telemetry_selftest.py` for `POSTHOG_API_KEY='phx_secret'`, `POSTHOG_API_KEY='not-a-key'`, and `set_capture_api_key('not_phc')`, all with `requests.post` set to explode.

## MEDIUM

### `tests/test_telemetry_priv04.py:54-71`, `shared/posthog_server.py:244` -- privacy tests can still race with an already-started drain daemon

The new fixture monkeypatches `ph._event_queue` and `_start_drain_thread_once`, but `_reset_for_tests()` does not stop an already-running daemon thread. That daemon loop reads the module-level `_event_queue` on each iteration, so a daemon started by an earlier test can later consume this file's fresh queue before the assertions call `.get()`.

Why it matters: the comment says the event cannot be stolen, but that is not guaranteed. The network guard prevents production POSTs once patched, but it does not make the captured-payload assertions deterministic.

Concrete fix: patch the imported `desktop.telemetry.enqueue_event` in this fixture to put directly into the private `fresh_q`, bypassing `shared.posthog_server.enqueue_event` and the daemon entirely. Keep the `requests.post` hard-fail as a belt-and-suspenders network guard.

## LOW

None found in the phase-116 diff.

## Additional Notes

I did not find a CLI ordering/restoration issue in the `genizah_app.py` block: it runs before `QApplication`, uses an in-memory `_enabled` toggle, avoids `set_consent()`, and restores `_enabled` in `finally`.

I could not run the focused tests here: `pytest` is not on PATH, and both `venv` and `.venv` point at missing base Python installations.

---

## Claude triage + response (2026-06-16)

Investigated each finding against the live codebase + the LOCKED phase CONTEXT (D-01 "lightweight",
`<domain>`: "adds NO new producers and **no new chokepoint machinery**") and machinery NOT in the
reviewed diff. Net: **1 HIGH fixed, 1 MEDIUM fixed, BLOCKER-1 partially fixed + documented,
BLOCKER-2 rebutted.** Full telemetry+guard regression green (241 passed) after fixes.

**Key context Codex could not see (diff-only review):**
- **`context` has ZERO production producers.** `grep` confirms no `track_error(...)` /
  `track(..., context=...)` callsite anywhere outside `desktop/telemetry.py`'s own docstrings.
  `_safe_context` is forward-looking DEFENSE-IN-DEPTH for a key nothing populates yet.
- **The PRIMARY guard against dynamic user content is the D-17 producer-layer AST guard**
  (`tests/test_no_dynamic_telemetry_strings.py`), which forbids `text()` / `selectedFiles()` /
  `currentText()` / `tabText()` / `windowTitle()` / `toPlainText()` as arguments to telemetry
  calls. Real query text and real My-Library filenames are RUNTIME values pulled from exactly
  these accessors → structurally blocked at the producer layer before the chokepoint.

### HIGH (self-test can POST a non-`phc_` key) — FIXED ✅
Valid; the code contradicted its own docstring. `send_selftest_event_sync()` now returns `NO_KEY`
(no network call) when the resolved key is empty OR not `phc_`-prefixed, so a stray
`POSTHOG_API_KEY=phx_...` personal key can never be POSTed. Tests added
(`test_phx_env_key_*`, `test_junk_env_key_*`, `test_non_phc_override_*`). Commit below.

### MEDIUM (priv04 tests could race a pre-existing drain daemon) — FIXED ✅
Correct: the per-test `_event_queue` monkeypatch could not isolate a daemon a prior test file
left running. Adopted the suggested fix: the autouse fixture now intercepts
`desktop.telemetry.enqueue_event` and captures the (already-scrubbed) payload into a PRIVATE queue
the daemon never reads — fully deterministic regardless of daemon state. The `requests.post`
hard-fail stays as a belt-and-suspenders network guard. Commit below.

### BLOCKER-1 (`_safe_context` denylist leaks unlisted extensions) — PARTIALLY FIXED + DOCUMENTED ⚠️
Valid as a defense-in-depth strengthening; reachability is low (zero producers; dynamic values are
D-17-blocked). The proposed pure label-allowlist is NOT viable here: `context` has no production
labels to enumerate, and an allowlist would either be empty (defeating the key's purpose) or would
have to hardcode the TEST-invented labels (`search_tab.run`, `app.crash`, …) into production —
backwards. A structural rule cannot separate code labels (`app.crash`) from filenames
(`genizah.log`) — same shape — and the established
`test_safe_context_preserves_code_labels` contract REQUIRES dotted labels to survive. **Action:**
expanded `_CONTEXT_FILE_EXTENSIONS` to cover all of Codex's named cases (`sqlite3`, `log`, `bib`,
`jp2`, `markdown`, `ris`) plus a comprehensive document/data/image/archive/media/code/log set, and
documented the defense-in-depth posture + the accepted residual (a hardcoded-literal filename with
an unlisted extension on a future `context` producer) in the code comment. Verified Codex's examples
now collapse and all code labels still survive.

### BLOCKER-2 (allowlisted keys are key-only validated, prose can ride non-`context` keys) — REBUTTED ❌ (not fixed)
Out-of-scope and the leak vector is already structurally closed:
- Adding per-key value validators (enum sets for `tab_name`/`search_mode`/`feature_name`/… +
  UUID/numeric validators + `$set` constraints) is **new chokepoint machinery**, explicitly OUT of
  scope per the locked CONTEXT `<domain>` and the D-01 "lightweight" decision.
- Codex's example `track(..., action='Maimonides rent letter')` is a hardcoded literal no developer
  would write; the realistic vector — a runtime user query/filename — is a dynamic value pulled from
  a UI/file accessor, which the **D-17 AST guard blocks at the producer callsite** (Codex couldn't
  see this guard; it isn't in the diff). The recursive `_scrub_value` redactor also covers paths +
  Hebrew on every key except `context`.
- Logged as a candidate FUTURE hardening (runtime per-key enum validation as belt-and-suspenders),
  not a v8.1.0 blocker. The PRIV-04 invariant holds via producer discipline + D-17 + scrubber, which
  is the design D-01 locked.

**Verdict reconciliation:** Codex's "privacy invariant DOES NOT HOLD" rests on inputs that the
zero-producer reality (`context`) and the D-17 producer-layer guard (dynamic values) actually
prevent. With the HIGH + MEDIUM fixes and the BLOCKER-1 expansion landed, and BLOCKER-2 covered by
existing structural guards, the invariant holds for v8.1.0's scope. Fix commits: see `git log --grep "116.*codex"`.
