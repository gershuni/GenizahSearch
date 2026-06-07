# Phase 109: Visual-Similarity Merge & Soft-Retire - Context

**Gathered:** 2026-06-07
**Status:** Ready for planning

<domain>
## Phase Boundary

Plug the **visual-similarity (VS) look-alike source** into the desktop Join Workbench candidate
surface that Phase 108 already scaffolded (the Text/Visual/Combined source selector +
provenance-badge + both-first merge plumbing exist; Visual/Combined are stubbed/disabled —
108 D-14). This phase: (1) wires the shared VS service
(`get_vs_service().get_suggestions(anchor_sys_id)` → manuscript-level look-alikes) into the
**Visual** and **Combined** sources; (2) merges text + VS via `shared/joins_lab.merge_candidates`
(★both → ✎text → ⊙VS-by-rank, locked Phase 106); (3) **reroutes the standalone VS dialog's
normal-mode entry points** into the Workbench and **marks the old normal-mode path removable** after
a parity pass — **while keeping the JoinsDialog pick-mode partner-picker working**.

**Largely UI wiring + a few interaction decisions** — the merge/dedup/provenance logic already
exists and is unit-tested in `shared/joins_lab.py` (Phase 106), and the VS service is mature
(`shared/visual_similarity_service.py`).

**In scope (JWB-12 VS source + combined view + desktop soft-retire):**
- Visual source = anchor's VS look-alikes (auto-loaded, no query); Combined = builder text query
  merged with the anchor's VS look-alikes; both-first provenance ordering + badges live.
- Reroute desktop Browse + ResultDialog normal-mode VS entry points into the Workbench (Visual).
- Mark the old standalone normal-mode dialog removable (retain one cycle); preserve pick-mode.
- Parity verification (automated test + manual UAT) gating the deprecation marker.

**Explicitly OUT of 109:**
- Web Joins Lab / web VS-dialog soft-retire (no web Workbench yet — later phase; D-13).
- Routing JoinsDialog **pick-mode** into the Workbench (kept as-is — D-12).
- Physical deletion of the old dialog code (a later cleanup phase — D-11).
- JSA / parallels seeding (Phase 110); tear-side `[`/`]` assist (Phase 110 disposition).
- Any new VS scoring / index work — rides the existing `visual_similarity.db` + service.

</domain>

<decisions>
## Implementation Decisions

### Source semantics (JWB-12 source selector)
- **D-01:** **Visual source auto-loads on select.** Switching the source selector to **Visual**
  immediately fetches + shows the anchor's look-alikes — no "Find Candidates" press. VS is keyed on
  the anchor's `sys_id` (manuscript-level), so no text query is needed. Matches the old dialog's
  "show me this fragment's look-alikes" feel.
- **D-02:** **Combined ("Search + visual") = builder text query merged with the anchor's VS
  look-alikes** via `merge_candidates` (★both → ✎text → ⊙VS). When the builder query is **empty,
  Combined degrades gracefully to Visual-only** — no error, no block.
- **D-03:** **The VS source ignores BOTH builders** (the anchor-side builder and the other-side
  `p±1` cross-side builder). The other-side AND-narrow / OR-widen toggle constrains only the
  **Text/Combined text half**. VS look-alikes are always the anchor manuscript's full set —
  VS is manuscript-level and has no page to apply cross-side logic to.
- **D-04:** **★both is keyed at the manuscript level** (locked Phase-106 `merge_candidates`:
  `vs_by_sid = {v.sys_id: v}`). A text hit on **any page** whose manuscript is also a VS look-alike
  becomes **★both** and sorts first — the intended strong join signal. No page-weighted refinement
  in v8 (deferred).

### VS volume / noise
- **D-05:** **Fetch + show all VS suggestions up to the service limit (200)**, ordered by
  `svm_score` desc, **paginated in the existing 20/page grid**. **No top-N cap** (matches the old
  dialog's full set).
- **D-06:** **No minimum-similarity (`svm_score`) floor** — ordering + pagination handle the
  low-similarity tail. (User knows the VS score distribution; a magic threshold wasn't warranted.)
- **D-07:** **Combined = a single merged list, paginated 20/page** — **no independent VS cap.**
  Both-first ordering surfaces ★both first, then ✎text-only, then ⊙VS-only by `vs_rank`.
- **D-08:** **When the anchor has no VS data (`has_suggestions()` is false — ~50% of manuscripts),
  GREY OUT / disable the Visual source option;** Combined falls back to **Text-only** on a no-VS
  anchor. No empty-state copy needed because the dead source is disabled rather than selectable.
- **D-09 (perf — realizes SC#3):** Because the merged set can reach **200**, per-candidate
  enrichment (browse text / measurement / thumbnail / snippet / cross-side membership) **MUST be
  PAGE-LAZY** — enrich only the visible 20-card page — **on top of** being batched (108 D-21).
  Page-lazy + batched is how SC#3's "~80-candidate VS load" budget holds when the set is 200.
  - **D-09 AMENDMENT (2026-06-07, post-codex-review concern #4 — user-confirmed):** The page-lazy
    rule is **scoped to the network/IIIF-bound work**: **thumbnails** (ThumbResolver — already
    page-lazy, ≤20/page) and any **per-candidate browse-text / network fetch** stay page-lazy.
    **Cheap local-only enrichment MAY run batched over the full ≤200 set:** measurements =
    a single batched SQL (`FjmsService.get_measurement_summaries_batch`), snippets = pure-Python
    over already-fetched text. These are O(1) DB round-trips / in-memory loops, not per-candidate
    network calls (RESEARCH A2/A3). **The hard rule that survives, and SC#3's actual gate:** NO
    per-candidate SERIAL network/IIIF fetch over the full set — anything network-bound is page-lazy.
    Plan 03's UAT perf check still observes the ~80-candidate load; an instrumentation assertion that
    the network/thumbnail path receives only the visible page is encouraged.

### Soft-retire depth (deferral #4)
- **D-10:** **Reroute BOTH normal-mode entry points to the Workbench (Visual):**
  - Browse "Visual similarity" — `genizah_app.py:4708 _browse_view_visual_similarity`
  - ResultDialog "Search visual similarity" — `desktop/result_dialog.py:758 _rd_search_visual_similarity`

  Each opens the Join Workbench with that fragment **pinned as anchor** and the **Visual source
  auto-loaded** (D-01). The Workbench becomes the single desktop home for browsing look-alikes.
- **D-11:** **The old standalone normal-mode dialog code (`_show_vs_dialog` normal path,
  `genizah_app.py:4788`) is MARKED REMOVABLE** (deprecation marker/comment) and **retained for one
  cycle** as a safety net; physical removal is a later cleanup. Not deleted in 109 — partly because
  pick-mode reuses the same method.
- **D-12:** **JoinsDialog pick-mode partner-picker kept AS-IS, untouched** —
  `_show_vs_dialog(..., on_pick=...)` (the `on_pick` branch at `genizah_app.py:5108`) remains the
  visual partner-picker for Add-as-Join. **SC#2 requires it keep working; this is why the dialog
  code survives** (it cannot be fully deleted in 109).
- **D-13:** **Desktop-only.** The web VS dialog (`web/components/visual_similarity_dialog.py`) is
  **untouched** — no web Join Workbench exists yet (deferred to a later phase).

### Parity & transition
- **D-14:** **Parity verification = BOTH** (a) an **automated invariant test** — the Workbench
  Visual source returns the **same `sys_id` set** as `get_vs_service().get_suggestions(anchor)` for
  sample anchors — AND (b) a **manual UAT sign-off** comparing a handful of anchors (old dialog vs
  Workbench Visual source).
- **D-15:** **Cutover = reroute immediately on ship**, retaining the old normal-mode code
  present-but-unreferenced (marked removable, D-11) as a one-cycle safety net. **No temporary
  fallback toggle** (no env var / hidden setting to reopen the old dialog).
- **D-16:** **Parity bar = (a) same look-alike suggestion set (`sys_id`s) reachable in the Workbench
  Visual source + (b) all four actions (Browse / Puzzle / Add-to-List / Add-as-Join) work on VS
  candidates.** Richer enrichment (material / dimensions / snippet) is a **bonus, not a gate**.
  Full per-item detail/expand layout parity is **NOT required**.

### Architecture / build constraints (carried forward — locked from 106/108)
- **D-17:** **i18n from line one** — every new string `tr()`-wrapped; the Visual/Combined sources +
  any reroute strings render fully under `lang=he`. The `tests/test_join_workbench_i18n.py` guard
  applies (add EN+HE keys to `genizah_translations.TRANSLATIONS`; see 108 RR-4).
- **D-18:** **No `_vs_*` private calls on the workbench path** — VS candidates' four actions go
  through the Phase-107 public methods (107 D-12 / 108 D-20). The reroute wiring uses the public
  open-workbench path, not `_vs_*`.
- **D-19:** **VS is reached via the shared service, NOT the `SearchExecutor` adapter** (Phase 106
  D-05): the desktop pane calls `get_vs_service().get_suggestions(...)` directly, normalizes the
  dicts to `Candidate` via the shared `normalize_candidate`, and feeds `merge_candidates`.

### Claude's Discretion
- Where the parity-pass record lives (lean: a parity scenario in `109-HUMAN-UAT.md` + the automated
  test); exact deprecation-marker style/comment.
- Whether ⊙VS cards display `vs_rank` / `svm_score` (a nicety — lean: show a compact rank/score).
- **RESOLVED (2026-06-07, post-codex-review concern #6 — user-confirmed): ✎text badge reading.**
  JWB-12 lists badges ★both / ⊙VS / ✎text, but **text-only candidates render UNBADGED** (Phase-108
  precedent). Only ★both and ⊙VS carry an explicit badge; an unbadged card == text-only by
  elimination (unambiguous, since the other two provenances ARE labeled). This is the intended
  reading of JWB-12's badge list — do NOT add a `tr("  ✎ text")` badge. ⚓self / ⇄other-side badges
  (108 plumbing) are unaffected.
- Grey-out vs hide for the disabled Visual option when the anchor has no VS data (D-08).
- Exact mechanism by which the rerouted entry points set source=Visual at/after open.
- How much of the old dialog's enrichment (`_enrich_vs_suggestions`) logic is reused vs replaced by
  the 108 batch enrichment path.

### Research flags (for gsd-phase-researcher / gsd-planner)
- **R-01 (VS → Candidate mapping):** `get_suggestions` returns
  `{'alma_id': str, 'svm_score': float, 'rank': int}`; `normalize_candidate`
  (`shared/joins_lab.py:248-276`) reads `res.get("vs_rank")` and `res.get("svm_score")` and builds
  `sys_id` from the result dict's id field. The VS→Candidate adapter MUST map `alma_id`→`sys_id`,
  `rank`→`vs_rank`, `svm_score`→`svm_score`, and set **`via_vs=True`, `page=None`**. Confirm the
  exact normalizer contract (it currently reads `svm_score`, so pass that key through verbatim) and
  whether a thin VS-dict shim is needed before `normalize_candidate`.
- **R-02 (None-page rows):** ⊙VS-only candidates have `page=None`. The 108 None-page guards
  (RR-12) must hold for `CandidateCard`, `_enqueue_image_for_pane`, and `CompareDialog` — a VS-only
  row renders a manuscript-level thumbnail; "open the matched page" is N/A for it.
- **R-03 (perf, SC#3):** Verify **page-lazy + batched** enrichment on an ~80+ (up to 200) merged
  set — reuse 108's batch paths (`FjmsService.get_measurement_summaries_batch`, thumbnail batch,
  `get_browse_page`). Do NOT enrich all 200 upfront.
- **R-04 (VS fetch path + caches):** Decide `get_vs_service(thread_safe=?)` for the Workbench and
  whether the `DesktopVSCache` + server-fallback chain the old dialog uses
  (`genizah_app.py:4736-4757`) is needed, or whether the local `visual_similarity.db` is assumed
  present (it is on most desktops; the service degrades to `[]` if absent). Respect the Phase-98 NLI
  circuit breaker on any VS thumbnail fetch (Phase-107 WR-02: `ThumbBatchWorker` currently bypasses
  it — don't reintroduce that gap for VS enrichment).
- **R-05 (reroute wiring):** Pin the anchor through the existing public open-workbench path
  (Phase 107 `open_joins_workbench` / find-joins) and trigger Visual auto-load; confirm both Browse
  and ResultDialog call sites pass the right `sys_id`/`shelfmark` and select source=Visual.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & roadmap
- `.planning/REQUIREMENTS.md` § "New requirements" **JWB-12** (unified candidate sources: VS source
  + combined view + soft-retire) and § "Deferrals / discuss-phase questions" **#4** (VS-dialog
  retirement timing — resolved here: reroute this phase, mark removable, delete later).
- `.planning/ROADMAP.md` § "Phase 109" — goal + the 3 success criteria (SC#1 VS+combined sources
  with badges/both-first; SC#2 reroute + deprecate-after-parity + pick-mode preserved; SC#3 batched
  enrichment on ~80-candidate load).

### Prior phase context (the seam this phase completes)
- `.planning/phases/108-…/108-CONTEXT.md` — **D-14** (the 108↔109 seam: source selector +
  provenance-badge + both-first merge plumbing built, VS stubbed), **D-21** (batched enrichment),
  **RR-2** (`Candidate` is the UI model; `merge_candidates` returns a plain LIST), **RR-12**
  (None-page guard), **D-20** (no `_vs_*`).
- `.planning/phases/106-…/106-CONTEXT.md` — **D-05** (VS via shared service, not the adapter),
  the `merge_candidates` provenance ordering + `normalize_candidate`/`Candidate` contract.

### Executable spec (Spike 002 — frozen at git tag `spike-002-joins-workbench`)
- `.planning/spikes/002-assisted-join-workbench/sketch/join_workbench.py.txt` — `_maybe_assemble`
  merge ordering (~L1149) + the source-selector/VS wiring behavior to extract (not copy the PyQt).
- `.planning/spikes/002-assisted-join-workbench/SPIKE-FINDINGS.md` — VS ~50% coverage; VS is
  supplementary, non-exhaustive signal.

### Code to extend / reuse (read before planning)
- `desktop/join_workbench.py` — `JoinCandidatePane` (source selector at ~`:1998`; `_maybe_assemble`
  merge stub at ~`:2399` currently `merge_candidates(self._text_cands or [], [])`), the grid/table +
  `_EnrichWorker` + None-page pump.
- `shared/visual_similarity_service.py` — `get_vs_service(thread_safe=)`, `get_suggestions(sys_id,
  limit=200)` → `{'alma_id','svm_score','rank'}`, `has_suggestions(sys_id)`, `get_suggestion_count`.
- `shared/joins_lab.py:248` `normalize_candidate`, `:511` `merge_candidates`, the `Candidate`
  dataclass (`vs_rank`/`vs_score`/`via_vs`/`page` fields, `:100-119`).
- `genizah_app.py:4708` `_browse_view_visual_similarity` (reroute target), `:4761`
  `_enrich_vs_suggestions`, `:4788` `_show_vs_dialog` (normal-mode = mark removable; **pick-mode
  `on_pick` branch at `:5108` = keep**), `:5254` `_vs_open_joins_with_partner` (pick-mode partner
  persist — leave intact).
- `desktop/result_dialog.py:758` `_rd_search_visual_similarity` (reroute target).
- `web/components/visual_similarity_dialog.py` — **untouched** (desktop-only soft-retire; listed so
  the planner knows NOT to change it).
- `genizah_translations.py` — add EN+HE keys for new strings (i18n guard, 108 RR-4).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **108's source selector + `merge_candidates` seam (D-14)** is the plug point — 109 fills the VS
  half of `_maybe_assemble` (replace the `[]` second arg with the normalized VS candidate list).
- **`shared/visual_similarity_service.py`** is mature and web-usable (Phase 106 D-05 designates it
  the VS data path, not the `SearchExecutor` adapter). `get_suggestions` is a single cheap SQL
  query against `visual_similarity.db`.
- **`merge_candidates` / `normalize_candidate` / `Candidate`** (Phase 106) already encode both-first
  ordering, ★both manuscript-level keying, and the `vs_rank`/`vs_score`/`via_vs` provenance fields —
  unit-tested.
- **108's batched + paginated grid/table + `_EnrichWorker`** is reused for the VS/combined surface;
  the only addition is page-lazy enrichment for the larger (≤200) set (D-09).
- **The old dialog's `_enrich_vs_suggestions` (`genizah_app.py:4761`)** is a reference for
  shelfmark/library/domain enrichment of raw VS rows — but prefer the 108 batch path.

### Established Patterns
- Provenance badges: ★ both / ✎ text / ⊙ VS#rank / ⚓ self (108 badge plumbing).
- `AlmaId == sys_id` (the long `99000…` ids); VS rows are manuscript-level (`page=None`).
- Candidate is the UI model; `merge_candidates` returns a plain list (RR-2); per-page enrichment
  keyed by `(sys_id, page)`, triage keyed by `sys_id` (108 R-05/RR-2).
- Workers guard QLabel/QWidget writes with `try/except RuntimeError` (deleted-widget safety).
- All candidate actions route through Phase-107 public methods (no `_vs_*`).

### Integration Points
- The Visual/Combined sources attach to the existing `JoinCandidatePane` source selector; the VS
  fetch happens in (or feeds) the same `_CrossSideWorker`/`_EnrichWorker`/`_maybe_assemble` flow.
- The reroute wires `genizah_app.py:4708` (Browse) + `desktop/result_dialog.py:758` (ResultDialog)
  to the public `open_joins_workbench`/find-joins path with source=Visual.
- Pick-mode (`_show_vs_dialog(on_pick=...)` ← JoinsDialog) stays on its existing path, untouched.

</code_context>

<specifics>
## Specific Ideas

- **VS = "show me this fragment's look-alikes" — instant.** Visual auto-loads on select; no query,
  no button. Mirrors how the old standalone dialog felt.
- **Show the full set (≤200), no floor, no cap** — the scholar (Hillel) prefers seeing everything
  paginated over an opinionated trim; ordering by similarity is enough.
- **Disable, don't apologize** — when a manuscript has no VS data (~half of them), grey out the
  Visual source rather than showing an empty "no results" surface.
- **Soft-retire, not hard-delete** — reroute the desktop normal-mode entry points now; leave the old
  code marked-removable for one cycle as a safety net; keep the JoinsDialog pick-mode picker working.
- **Parity = same look-alikes + working actions**, proven by an automated sys_id-set test AND a
  manual UAT sign-off, before the deprecation marker flips.

</specifics>

<deferred>
## Deferred Ideas

- **Page-weighted ★both** (refining the manuscript-level both signal by which page matched) — v8
  ships manuscript-level ★both (D-04).
- **Physical deletion of the old standalone normal-mode dialog code** — a later cleanup once the
  Workbench Visual source has proven itself (D-11 retains it one cycle).
- **Routing JoinsDialog pick-mode into the Workbench** — kept on the standalone path for v8 (D-12).
- **Web VS-dialog soft-retire / web Join Workbench** — later phase on the shared core (D-13).
- **Temporary fallback toggle** (env/hidden setting to reopen the old dialog) — rejected (D-15).
- **Minimum-similarity floor / top-N VS cap** — rejected for v8 (D-05/D-06); revisit only if the
  full 200-set proves noisy in practice.
- **Full per-item detail/expand layout parity** with the old dialog — not a deprecation gate (D-16).

### Reviewed Todos (not folded)
- `todo.match-phase 109` surfaced 6 keyword-coincidence hits (desktop corrections migration,
  server-side email search, unified metadata text search, FIST.db fill, Reading-Desk UX, one-click
  scholarly citations) — all matched on generic "search"/"shared"/"service"/"browse" areas; none
  touch the VS merge / soft-retire surface. **Not folded** (identical disposition to Phases 106/108).

</deferred>

<gap_closure_round>
## Gap-Closure Round (2026-06-07) — UAT REJECTED, 5 gaps (G-01..G-05)

Hillel's manual parity UAT (`109-HUMAN-UAT.md`) **rejected** sign-off. The reroute itself works
(Browse + ResultDialog open the Workbench with Visual), but the source UX must be **redesigned**
and **two locked decisions reversed**. `/gsd-plan-phase 109 --gaps` plans these. The
`_show_vs_dialog` deprecation marker stays "pending parity sign-off" (NOT live) until a clean re-UAT
after G-01..G-05 land.

**Authority note:** Where the gaps below conflict with the original D-01..D-19 decisions, **the gaps
win** (they are Hillel's post-UAT corrections). Specifically D-10's three-source *radio model* is
SUPERSEDED by the G-04 toggle, and D-12 ("keep pick-mode on the old dialog") is REVERSED by G-05.

### G-01 (low, quick) — HE label: חיצוני → חזותי
`genizah_translations.py` mistranslates "visual" as **חיצוני** ("external") in the Phase-109 VS keys.
Confirmed offenders: line 3832 `"Visual similarities": "דמיון חיצוני"`, 3833 `"Search + visual":
"חיפוש + חיצוני"`, 3835/3837 (108 stub strings), 4005 `"Visual look-alikes loaded": "דמיון חיצוני
נטען"`, 4006 `"No visual similarity data…": "…דמיון חיצוני…"`. Correct word = **חזותי**.
**SURGICAL ONLY** — many *legitimate* "external" uses of חיצוני exist (external services/website/
metadata, lines 43/107/112/315/1732/2867/3414…); do NOT blanket-replace. Fix only the VS keys, and
re-audit every Phase-109/108-VS key for the same slip.

### G-02 (medium) — VS candidate cards must show transcription text
VS cards currently show metadata/shelfmark only. They must ALSO render the candidate's transcription
text like text-source cards do. The VS adapter (`_normalize_vs_row`, Plan 01, `join_workbench.py:208`)
and `_load_visual_candidates` (Plan 02, `:2501`) must carry the candidate `full_text` through to the
`Candidate`, and `CandidateCard` (`:~1668`/`snip` at `:2000`) must display it for the via_vs path.
Source of VS card text: enrich via the existing batched browse-text path (page-lazy, D-09) — VS rows
are `page=None`, so text is manuscript/first-page level.

### G-03 (high, bug) — Combined "Search + visual" perpetually "loading", never renders
The combined assembly path hangs (never-completing fetch / missing finished-signal / assemble waiting
on a text search never triggered). The card-level `tr("loading…")` placeholder (`:1982`/`:2000`)
never resolves. **Disposition:** G-04 removes the Combined radio, so the fix lands *inside the new
toggle design*, not the old Combined branch — but the planner MUST identify the hang's root cause so
the new "toggle ON + search term" intersection path cannot reproduce it (esp. the empty-builder
degrade and the enrich-worker `enriched` signal completion).

### G-04 (high, REDESIGN — SUPERSEDES D-10 source model)
Replace the Text/Visual/Combined **radio group** (`_build_ui` `:2130-2151`, `_on_source_changed`
`:2470`, `apply_source` `:2570`, `set_source` `:4246`, source-aware `_maybe_assemble` `:2599`) with a
single **"Visual Similarity" toggle button placed next to "Find Candidates"**. Required behavior:
- **Toggle ON, search box empty** → show the anchor's VS candidates (pure visual; = old Visual source).
- **Toggle ON, with a search term** → show ONLY candidates that are BOTH VS look-alikes AND match the
  term (**intersection** `search ∩ VS`, i.e. ★both only — NOT the old both-first *union*).
- **Toggle ON after an existing search** → filter the existing results down to the VS∩term intersection.
- **Toggle OFF** → normal text results (no VS-only rows added), but text candidates that are also VS
  look-alikes STILL carry the VS/★both badge (informational regardless of toggle). ⇒ VS must be loaded
  for the anchor whenever available so the badge intersection can be computed even with the toggle OFF.
- **Same behavior in the side-by-side `CompareDialog`** (`:3435`) — it walks `wb.filtered`, so it
  inherits the toggle's filtering, but the planner must verify the toggle/badge state reaches it.
- No-VS anchor (D-08): the toggle is **disabled/greyed** (replaces the radio grey-out, Scenario 5).
- Keep the VS provenance badges (★both / ⊙VS). Text-only stays UNBADGED (CONTEXT ✎text RESOLVED).

This folds the Combined radio into "toggle ON + term". Re-plan the 109-02 source-selector internals
around a boolean toggle state (drop `_active_source` tri-state radios / `rb_text|rb_visual|rb_combined`
/ `_source_group`). `set_source('visual')` from the rerouted entry points (Plan 03) must keep working
against the toggle (map source='visual' → toggle ON).

### G-05 (medium, REVERSES D-12) — wire JoinsDialog pick-mode into the Workbench + tooltip
The JoinsDialog visual partner-picker (`corrections_ui.py::_show_vs_picker` `:4756` →
`parent_app._show_vs_dialog(..., on_pick=self._on_vs_pick)`; `_on_vs_pick` fills `frag_b_input`;
button tooltip at `:3445`) must NO LONGER open the old standalone orange dialog. Reroute it into the
**Workbench in a pick/partner capacity** and update the tooltip. This reverses D-12/SC#2.
**Recommended minimal pick surface (planner may refine, plan-checker validates):** give the Workbench
an optional `pick_callback` — when set, the Workbench is anchored on fragment A and candidate cards
expose a "Select as partner" affordance that invokes the callback with `(partner_sys_id,
partner_shelfmark)` and closes the window; the JoinsDialog wires `_on_vs_pick` as that callback. With
BOTH normal + pick paths rerouted, the planner should **re-evaluate** whether `_show_vs_dialog` can be
marked fully removable — but **retain the code one cycle** per D-11 (no physical deletion in 109).

### Re-verify after gap fixes (deferred UAT scenarios)
Scenarios 3 (four actions), 4 (reused-window re-anchor), 6 (perf ≥80 look-alikes) were NOT REACHED in
the UAT — they must be re-verified against the redesigned toggle UX once G-01..G-05 land. The new
`109-HUMAN-UAT.md` round must cover the toggle states (ON-empty / ON+term / OFF-badge), the Compare
dialog parity, G-05 pick-return, and the G-01 HE label.

### Unchanged decisions still in force
D-01 (VS auto-loads — now: toggle ON empty), D-04/D-05/D-06/D-07 (full set, no floor/cap, manuscript-
level ★both), D-08 (no-VS → now greyed *toggle*), D-09 + AMENDMENT (page-lazy network/thumbnail;
batched cheap local enrich), D-11 (retain old dialog one cycle), D-13 (desktop-only; web untouched),
D-14a (automated parity invariant stays green), D-17 (i18n from line one), D-18 (no `_vs_*` on
workbench path), D-19 (VS via shared service).

</gap_closure_round>

---

*Phase: 109-visual-similarity-merge-soft-retire*
*Context gathered: 2026-06-07*
*Gap-closure round appended: 2026-06-07 (G-01..G-05; D-10 source-model superseded, D-12 reversed)*
