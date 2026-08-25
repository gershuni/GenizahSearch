# Branch: `claude/multi-witness-passage-search`

**Multi-witness passage search** — searching one composition with several manuscript
copies of it at once, on `/parallels` and `POST /api/parallels`.

> Delete this file when the branch merges. It exists so an agent picking the branch up
> knows what was decided and, more importantly, **what was tried and rejected** — several
> of the obvious implementations here are measurably wrong, not merely different.

Base: `3e493cbf`, the head of `claude/letter-level-search-policy` -- this PR
is **stacked on that one** and its diff only makes sense against it.
16 commits, 7,277 insertions across 32 files.

Owning plan: `C:\Users\gersh\.claude\plans\let-s-plan-the-great-snoopy-shannon.md`.

---

## What it does

One work survives in many manuscripts. Previously the parallels search took **one** query
text. Now a user can paste (or promote from their own results) several *witnesses* of the
same work; each is searched **separately** and the result lists are merged by Reciprocal
Rank Fusion, k=60.

Measured on Birkat Hamazon through the shipped code (`max-40+short`, normal depth,
614 reachable manuscripts of a 673-entry census):

| approach | census found | % |
|---|---|---|
| 17 witnesses **concatenated** into one query | 296 | 48.2% |
| best **single** witness | 348 | 56.7% |
| 17 witnesses **fused** (page path) | 455 | 74.1% |
| 17 witnesses **fused** (API path) | 455 | 74.1% |

---

## The four load-bearing decisions

**1. Never concatenate witnesses on the passage engine.** The engine spends a per-query
*posting budget*; one long query starves. The 17 joined into a 33,180-character query
admitted 499,662 of 21,093,233 postings (2.4%) and hit the 3,000-candidate verify cap
against 27,106 candidates. Concatenation scores **below the best single witness**.

This is scoped to `method='passage'` **only**. Phase 0 of the plan measured the *chunk*
engine and found concatenation and union return the **identical** manuscript set (392
both ways, zero rows in either difference) — the chunk engine decomposes a query into
independent per-chunk lookups, so there is no shared budget to starve. Desktop's
`run_recursive_composition` concatenates and is **not** a defect for its own engine.

**2. Fuse by rank, not by score.** A passage `score` is matched *query* letters, so a
6,000-letter witness mechanically outscores a 1,200-letter one for reasons unrelated to
match quality. RRF ties sum-of-scores at similar witness lengths and beats it decisively
at mixed ones (18/26 vs 10/19 positives in the top 50/100 on the Antiochus deck).
Length normalisation was measured and is **dead** — worse than raw score at every cut-off
on both instruments.

**3. `score` always means matched letters — on every method, every response.** This one
was broken twice during the branch and both are worth knowing about:

* `601e640e` — threading the fusion key into the grouping made `aggregate_score` →
  `sort_score` → the item's public `score`, so a multi-witness API response returned
  `score ≈ 0.03` where a single-witness one returns ~200. Fixed by splitting one
  parameter into two: `order_key` decides group ORDER, `aggregate_score` stays letters.
* `bca4cc86` — the fused row reported `max()` across contributors while rendering the
  *rank winner's* label and highlighted span, so a row showed a 400-letter witness's
  evidence beside the number 900. Fixed: `score` comes from the row that supplies the
  evidence; the maximum moved to `witness_fusion.best_witness_score`.

**Consequence, documented rather than hidden:** a multi-witness response is ordered by
`witness_fusion.fusion_score`, so re-sorting it by `score` gives a **different** order
than the one returned. That was the deliberate trade against a `score` that means two
different things.

**4. One fusion module, two callers.** `shared/passage_fusion.py` is pure — no NiceGUI,
no engine, no I/O. The API fans out inside one request; the page searches per witness
across N calls and fuses page-side. They share only the pure module, because the page is a
*session* (adding a witness must search only that witness, or an R-round auto-expand is
quadratic) while the API is *stateless*.

---

## Where the code is

| Layer | Files |
|---|---|
| Fusion (pure) | `shared/passage_fusion.py` — `fuse`, `group_stats`, `tag_rows`, `split_pasted`, `split_by_length` |
| Engine | `shared/passage_parallels.py` — `witnesses=` kwarg, `_resolve_witnesses`, `NoWitnessesResolved` |
| Service | `shared/parallels_service.py`, `shared/search_serializer.py` — `order_key`, `witness_fusion` |
| API | `web/search_api.py` — `witnesses[]`, `sort`, `PASSAGE_MULTI_WITNESS_ENABLED` |
| Page | `web/pages/parallels.py` — the Witnesses panel (+1,500 lines) |
| Docs | `docs/SEARCH_API.md` (Multi-witness section), `docs/specs/passage-matching-algorithm.md` §10.2b, `web/pages/help.py` |

**Flags.** `PASSAGE_MULTI_WITNESS_ENABLED` (default OFF) ANDed with `passage_available()`
— flag alone is never sufficient. `SEARCH_API_PASSAGE_MAX_WITNESSES=25` (not 12: the
flagship case is a 17-witness set).

---

## Things that will bite you

* **Do not raise `SEARCH_API_PASSAGE_TIMEOUT` for multi-witness.** `run_through_passage_budget`
  cannot cancel an executor thread — the permit is held until the work actually finishes,
  so a longer ceiling means four slow requests occupy all four slots long after their
  clients got 504s. **Lower the witness cap instead.**
* **A value read after an `await` describes a configuration the search never used.** This
  page has a documented history of it; every input is captured at dispatch. `seed_digest`
  and `promoted_digest` are both dispatch-time captures for exactly this reason.
* **NiceGUI fires no event for a programmatic `.value` write.** Every handler in this page
  is called explicitly after its widget is set. Related: `run.io_bound` loses page context,
  so `ui.*` raises and `safe_user_*` degrades to `{}` silently.
* **`_persist_witness_state` is called at three mutation boundaries, deliberately not from
  `_refresh_witness_panel`.** The panel refresh runs once per witness during a search (it
  renders the progress line), so persisting there re-serialises the whole result set
  seventeen times in a 17-witness run.
* **Never nest the witness dispatch under a seed-result guard.** It shipped inside
  `if main_results or filtered_results:`, so a seed that matched nothing reported "No
  results" and left every witness unsearched — in a feature whose whole premise is that
  one witness finds what another misses (56.7% alone against 74.1% fused). An empty seed
  is a *searched witness with zero hits*, and it is stored as one: dropping it from
  `witness_rows` also drops it from `_searched_witness_count()`, which hides the fusion
  sort options and silently turns a two-witness fusion into a passthrough.
* **The witness depth cap is a DISPATCH cap, not an add-time one.** Enforced only while
  adding and promoting, it bounded the wrong quantity: `Find Parallels` resets every
  non-stale witness to `pending` and dispatches the batch, so 25 witnesses gathered at
  normal depth all re-run the moment the seed is re-run at `deepest` — ~8 minutes from one
  click, taking a slot of the shared budget 25 times. The rule now lives at module level
  (`witness_depth_cap`, `witnesses_over_dispatch_cap`) so a test can call it; inline in the
  closure, a `>` → `<` mutation left every available assertion green. Note which depth it
  reads: the LAST SEARCH's, because that is what `_run_one_witness_search` will use — the
  dropdown alone changes nothing.
* **`_PARALLELS_ROW_ALLOWLIST` is a whitelist, and it fails silently.** `best_witness_score`
  was missing from it, so every *downloaded* group reported `0.0` while the live rows on
  screen held the right number. Any new fused row field must be added there, and the test
  must assert its **value** — the existing export test set the field in its fixture and
  asserted only `witness_count`, which is exactly why this went unnoticed.
* **Routing a row is not the same as describing a record.** `filter_text` decides which
  BUCKET a witness's row lands in; a record's `fusion_score` / `witness_count` /
  `witness_ids` / `best_witness_score` describe the record. Fusing the two buckets separately
  conflated them, so a manuscript found by two witnesses — one of them on known source text —
  reported one, and ranked below records whose contributors happened to avoid the filter. Both
  the routing rule and the contributor arithmetic now live in
  `shared/passage_fusion.py::fuse_routed`, called by the page and the API: the rule had been
  written out twice, which is the drift that module exists to prevent. With no `filter_text`
  every filtered bucket is empty and the result is identical to a plain `fuse` — the common
  path is untouched.
* **Anything that changes the row set must also refresh the chrome around it.**
  `render_results` rewrites the results header from the rows it is handed; the summary line
  and the library-filter button are set once by the seed search and never again. After a
  witness run the summary described the seed's count beneath a list showing the fused one.
  TWO functions change the row set and both owe this: `_search_pending_witnesses` and
  `_remove_witness`. The second was missed on the first pass, and its unpaid debt was the
  worse one — the snapshot was persisted under the OLD witness set's fingerprint, so a reload
  matched it, judged the stored payload to be the same search, and restored the removed
  witness's contributions. The removal silently undid itself.
* **The page and the API return different manuscript SETS** (3,682 vs 3,850 on a
  17-witness run) while finding the same 455 census manuscripts. Cause: the
  duplicate-photography pass runs per-witness on the page and once post-fusion in the API.
  Accepted as inherent to the two shapes.

---

## Testing notes

Roughly 90 tests were added. Two things about them are worth carrying forward.

**Every gate here was proven able to fail.** Each fix ships with a mutation sweep that
reintroduces the defect and requires the suite to redden; the harness asserts its own edit
landed (a `.replace()` with LF literals against CRLF sources once reported five mutations
green while changing nothing). Roughly 70 mutations were run across the branch.

**Seven of them came back green, and only one was a plain coverage gap.** The rest were
tests that *could not fail* — asserting that a NAME appeared in the source, which every
mutation preserved while deleting the behaviour (`_long = []` still mentions
`MAX_WITNESS_CHARS` two lines up; `if False:` still contains the notify and its message
literal). One was worse: `test_best_rank_beats_higher_score_across_witnesses` asserted
`score == 900.0` directly below `witness_id == 'w1'` — it **pinned the defect**.

The fix each time was the same, and it is the pattern to reuse: move the decision to a
module-level pure function and test it by *calling* it. That is why `collect_witness_texts`,
`witnesses_needing_text`, `restore_witness_entries` and `split_by_length` live at module
level in a file whose page function is never imported.

**Render-smoke is not optional here.** `web/pages/parallels.py` is a 6,000-line closure;
headless tests cannot see a build-time `NameError`, and this page has taken a 500 from
exactly that twice. `tests/render_smoke/test_parallels_witness_render_smoke.py` and
`test_help_parallels_methods_render_smoke.py` drive the real routes in-process.

Known harness limit: NiceGUI's `User` does not deliver a Quasar option group's
`update:model-value`, so a simulated method switch changes nothing. Claims about both
branches of `on_passage_mode_change` are asserted at the AST level instead — see the
comment on `_method_branch_calls`.

**Full lane at the last run: 9,697 passed, 3 failed** — the same three that fail on the
pre-branch baseline (`test_cert01_grading_validator`,
`test_discovery_v4_extend_masks`, `test_phase_97_2_sqlite_vs_tantivy_consistency`), none
of which touches parallels, fusion or the API. Zero regressions.

---

## Open

* **End-to-end hand test on a live server** — the owner's, not yet done.
* Whether `witness_id` / `witness_label` belong in the **public** export JSON envelope is
  an owner decision the plan explicitly did not make.
* The **97-prefix sys_id divergence** — `shared/metadata_manager.py` (authoritative)
  accepts `97|99`, ~20 other sites accept `99` only. Latent: the live index holds 759,224
  records and none is 97-prefixed. Deliberately **not** fixed here (corpus-wide, not
  feature-specific); filed as its own task.
* The **`New!` markers** — one on the method radio label, one on the sidebar badge. The
  owner wants both removed eventually and chose to be reminded rather than set a date.
