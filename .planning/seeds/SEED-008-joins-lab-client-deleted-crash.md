---
id: SEED-008
status: dormant
planted: 2026-06-19
planted_during: v8.2.0 / Phase 119 (Web Joins Lab) live UAT
trigger_when: Phase 120 (Actions & Persistence) — fold this hardening fix into that phase alongside the other close-out items. Small, well-precedented bug fix; not a standalone phase.
scope: small (fold into existing Phase 120)
---

> **ROUTING:** Pre-existing Phase 118 async-lifecycle bug surfaced during the 2026-06-19 live UAT of the
> Web Joins Lab. NOT a Phase-119 gap-closure regression (the 119 gap plans 09/10/11 did not touch the
> affected code). User directed (2026-06-19): "add it to the next phase with all other things to close."
> Pull into `/gsd:discuss-phase 120` / Phase 120 plans as a stability fix.

# SEED-008: Web Joins Lab — fire-and-forget tasks crash on client/tab deletion

> Captured as a seed (NOT implemented inline, per the mid-phase-fix preference). A live crash the user hit
> while testing Compare during Phase 119 close-out.

## Symptom (observed 2026-06-19)

Opening the Joins Lab (and Compare) and then navigating away / closing the tab while a background fetch is
in flight throws an unretrieved-task exception in the server log:

```
Task exception was never retrieved
future: <Task finished name='Task-254' coro=<create_joins_lab_page.<locals>._load_known_joins() ...
exception=RuntimeError('The client this element belongs to has been deleted.')>
  File "web/pages/joins_lab.py", line 1176, in _load_known_joins
    known_joins_container.clear()
  ...
RuntimeError: The client this element belongs to has been deleted.
```

## Root cause

`_load_known_joins` is a fire-and-forget task (`asyncio.ensure_future` at `web/pages/joins_lab.py:1309`).
It guards against a **newer anchor** superseding the fetch (the `anchor_gen != _anchor_generation['value']`
checks), but it does **not** guard against the **client/tab being deleted** while the `run.io_bound`
(Supabase + SQLite `fetch_connected_fragments`) call is in flight. When the tab disconnects mid-fetch,
`_anchor_generation['value']` is unchanged, so the guard passes and the post-await
`known_joins_container.clear()` (lines 1140 / 1156 / 1176) raises
`RuntimeError: The client this element belongs to has been deleted.`

## Scope of the fix

Two fire-and-forget tasks on this page share the unguarded post-await UI-mutation pattern:
1. **`_load_known_joins`** (`web/pages/joins_lab.py` ~1122–1184; dispatched at ~1309) — the reported crash.
2. **`_do_vs_fetch_and_update`** (dispatched at ~1257 and ~1515; clears `candidates_container` at ~1474) —
   same vulnerability on the VS-candidate fetch path (Phase 119 territory; harden it in the same pass).

Audit any other `asyncio.ensure_future(...)` on this page that mutates UI (`.clear()` / render) after an
`await`, and apply the same guard.

## Fix pattern (already established in this codebase)

Wrap the post-await UI mutations in `except RuntimeError:` and bail out — the client is gone, there is
nothing to render. Precedents in-repo:
- `web/pages/joins_lab.py:2122` (timer `parent_slot has been deleted` guard — same class)
- `web/components/joins_panel.py:512`
- `web/components/notes_display.py:427`
- `web/components/version_selector.py:194`

```python
# inside _load_known_joins / _do_vs_fetch_and_update, around each post-await container mutation:
try:
    known_joins_container.clear()
    with known_joins_container:
        render_known_joins_group(...)
except RuntimeError:
    return  # client/tab deleted mid-fetch — nothing to update
```

## Origin / blame

- Introduced: `bd2579c0` feat(118-04): integrate builder + known-joins (ANC-04)
- Last touched: `095b6437` fix(118): cross-side cancellable + page-contract + 2 MED fixes (CR)
- Phase 119 gap plans (119-09/10/11) did NOT modify `_load_known_joins` — confirmed via `git log -L`.

## Notes / pointers

- Page: `web/pages/joins_lab.py`; off-loop dispatch via `web/joins_executor.py`
  (statically enforced by `tests/test_joins_lab_off_loop.py`).
- A render-smoke test that simulates client disconnect mid-fetch would pin this (the existing harness does
  not exercise the disconnect path — same blind-spot class as the original Phase-119 render-smoke gap).
- Related deferred items from the same UAT: HUMAN-UAT re-run for R2-3 (Compare image height) and R2-6
  (single shelfmark with real data) — see `119-HUMAN-UAT.md`.
