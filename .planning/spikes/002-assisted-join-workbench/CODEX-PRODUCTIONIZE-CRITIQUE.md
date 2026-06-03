# Codex critique — Join Workbench: replan vs tweak vs hybrid (2026-06-02)

> Brief: `_tmp/codex-joins-productionize-brief.md`. Raw: `_tmp/codex-joins-productionize-response.txt`.
> Codex gpt-5.5, reasoning xhigh. (Codex's own shell sandbox failed mid-run, so it answered from
> the brief + partial repo reads — verdict still well-reasoned.)

## Verdict: **C (hybrid), but stricter** — treat the sketch as an EXECUTABLE SPEC, not the
implementation base. Tweaking-to-production violates the repo contract in exactly the risky places
(i18n, shared services, desktop/web split, persistence, tests, private methods). Blank-page wastes
validated UX discovery.

> "The danger is letting 'hybrid' become 'clean up the sketch until it feels production-ish.' Don't.
> Extract the domain behavior, test it, build the desktop UI on top, and make web parity an
> architectural constraint rather than a same-phase deliverable."

## Sub-decisions
1. **Process** → real GSD plan, **timeboxed**. Requirements around the *validated behaviors* +
   explicit deferrals + verification matrix + rollback path. (Lighter than blank-page; heavier than
   an inline harden-plan — this is v8.0.0 scope touching persistence/search/VS/bilingual/architecture.)
2. **Web parity** → **desktop-first UI, shared logic now, web later.** Don't build two UIs this
   phase (doubles QA before the model stabilizes), but the extracted logic must be web-usable now:
   no PyQt types, no desktop callbacks, no direct sqlite path hacks.
3. **VS dialog** → **soft-retire.** Reach parity in the workbench → reroute VS entry points → mark
   the old dialog removable after one verification pass. Not "two surfaces forever" (drift).

## Where the hybrid will hurt
- **Line-break search dependency**: composing the query is easy; *executing* it cleanly may not be.
  Stop the shared module at a clean **`SearchExecutor` adapter** boundary. Keep pure parts pure
  (query model, syntax rendering, side-A/B plan, result normalization, membership logic).
- **PyQt coupling worse than it looks**: the four actions are app methods today → need **public,
  named action APIs**, not `_vs_*` calls, or the workbench is just another privileged glued dialog.
- **Join-persist path = architectural red flag** (see risks).
- **"Shared" must mean domain/service logic, NOT premature cross-platform UI abstraction** (a
  view-model later is fine; a UI framework abstraction now is overengineering).

## Risks I was underweighting
- **2-fragment join model vs N-fragment clusters** (TOP product risk): "Add as Join" persists a
  2-fragment relationship, but the workbench UX encourages discovering *clusters*; scholars may want
  N-fragment groups / uncertain joins / per-relationship evidence+notes. Don't solve it this phase
  unless required, but **document it as an explicit deferred data-model risk** so v8 doesn't
  accidentally canonize a too-small model.
- **Per-candidate perf**: 80 VS candidates × (get_browse_page + enrichment + thumbnail + dimensions
  + snippet + side-membership) = death by per-candidate calls. **Batch everything.**
- **Reversibility drops once tracked** — use feature flag / isolated entry points until VS is
  rerouted; don't half-land monkey patches.
- **i18n is acceptance criteria, not cleanup.**
- **Dedup identity needs a canonical candidate key** (sys_id, page, side image, adjacent-side
  membership) or triage/joins/self-match become untrustworthy.

## Recommended sequence (Codex)
1. Freeze the sketch as spec (extract behaviors into REQUIREMENTS/MILESTONES, not copy-paste).
2. Define the shared **domain model** (candidate/anchor identity, side query, rows/gaps, provenance,
   triage, self-match, badges, merge semantics).
3. **Extract pure logic first + unit-test it** before touching UI (compose, cross-side membership,
   dedup/compaction, merge ordering, provenance, snippet/page helpers).
4. Add **service adapters** (shared search / VS / FJMS-measurement / metadata-image) — no direct
   `fist_data/*.db`.
5. Create **public desktop actions** (replace `_vs_*`).
6. Port the desktop UI onto the shared layer, `tr()` from the start, keep old VS dialog until parity.
7. Verify perf + bilingual + RTL + dark mode + join persistence.
8. Reroute VS entry points; deprecate the old dialog.
9. Defer web UI deliberately, with the shared API already usable.

## My synthesis (Claude)
Agree with C-stricter. Two things I'm elevating: (a) the **N-fragment join-model question is a
SCHOLAR/domain decision for Hillel** and is foundational — settle it (even as "explicitly deferred,
2-fragment for v8") before locking the data model; (b) run this as the **deferred v8.0.0 Joins Lab
phase** (timeboxed GSD plan, sketch = spec) following the extract-pure-logic-first order.
