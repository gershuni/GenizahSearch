# Discovery performance — situation assessment (2026-08-20)

> **Purpose.** A handoff for a session or agent picking this up cold. Five problems were
> found on 2026-08-19/20 around the discovery surfaces. Two were fixed and are recorded
> here only so nobody re-does them. Three are open, one of them a live user-facing outage.
>
> **Problem 1 is FIXED, DEPLOYED and CONFIRMED (2026-08-20, `4f6e31f4`)** — see the note under Problem 1;
> Problems 2-5 have NOT been implemented. No production change has been made since the V4.2
> artifact deploy of 2026-08-19.
>
> **Read this first, then `docs/specs/discovery-deploy.md`** if any option involving the
> artifact is on the table.

## Status at a glance

| # | Problem | Severity | State |
|---|---------|----------|-------|
| 1 | The citation-range filter on `/computed-identifications` always times out | **P1 — was live, user-facing** | **CLOSED 2026-08-20** — deployed (`4f6e31f4`), 10,478 ms → 97 ms on production, owner-confirmed in a browser. See the note under Problem 1. |
| 2 | `/computed-identifications` takes ~2 s; only ~0.9 s is accounted for | P2 | Open. Partly diagnosed; ~1.1–3.0 s unmeasured. |
| 3 | `bench_discovery.py` is not a smoke test, and it hung the V4.2 deploy | P2 | **CLOSED 2026-08-20** — all four parts written; see the note under Problem 3. |
| 4 | The checked-in V4.2 recipe cannot rebuild the artifact in production | P2 | Open. |
| 5 | A second index candidate on `discovery_identification` | P3 | Open, non-urgent. |
| — | perf-watch misattributed every slow request | (was P2) | **Fixed 2026-08-19**, not deployed. |
| — | `/admin` blocked the event loop on every page build | (was P2) | **Fixed 2026-08-19**, not deployed. |

**Evidence grades used below.** `[M]` measured in-session against the live artifact ·
`[C]` measured by the Codex audit · `[A]` measured/established by a subagent ·
`[S]` read from source · `[?]` asserted but unverified — re-check before relying on it.

---

## Problem 1 — the citation-range filter is non-functional (P1)

> **FIXED, DEPLOYED AND OWNER-CONFIRMED 2026-08-20 (`4f6e31f4`).** Measured on production through the
> real loader and service: the locus-filtered read answers in **97 ms** (`status=ok`, 1,231 of 2,433
> rows), a one-sided bound returns 366, and the journal is clean. Everything below is
> retained as the diagnosis, but two things changed. **(1) Option A was superseded.** The
> shipped fix keeps the predicate INSIDE the `WHERE` clause as an uncorrelated
> `di.identification_id IN (SELECT ... FROM locus_unit lu JOIN discovery_locus_piece p ...)`
> rather than prepending a `WITH ... AS MATERIALIZED` CTE. Both measured the same (61.3 ms
> vs 62.6 ms, against 10,478 ms correlated), but the WHERE-local form leaves
> `_build_findings_filter`'s `(where_sql, params)` contract and the parameter ORDER untouched,
> so no call site changes; the CTE is statement-level and needs its params re-spliced ahead of
> `_divergence_flag_sql`'s SELECT-list params. That splice is not a theoretical risk: run
> un-spliced against this artifact it returned WRONG identification sets on 6 of 15 probed
> (work x bound-shape) cases and SILENTLY CORRECT ones on the other 9, because those 9 were
> empty anyway. **(2) A second bug was found and fixed with it.**
> `web/pages/findings.py::_fetch_children` never read `locus_from`/`locus_to` back out of the
> child state, so a range-filtered parent expanded into UNFILTERED children -- reachable only
> once the parent query stopped timing out, i.e. this fix is what exposed it. Shipping the SQL
> alone would have converted a broken feature into a silently wrong one.
> **Option B and Problem 5 are now near-redundant** -- the rewrite removes most of the margin
> the index table below implies.


### What a reader experiences

Pick a work on `/computed-identifications`, then set a citation range. The request runs for
5 s, hits the findings timeout, and returns no results. Deterministic on heavy works, not
intermittent. **This is not slowness; the feature does not work.**

Exposure: 527 of 550 main-pool works are addressable by the range UI, and 29,368 of 30,528
main-pool identifications carry citation data `[C]`. The reader must deliberately choose a
work *and* a range, so the event rate is unknown — nobody has pulled analytics.

### The numbers

Probe: heaviest main-pool work carrying locus pieces (`w000112`, 16,043 identifications,
`citation_pos` 1..150, filtering 1..75). SQL obtained by **calling**
`shared/discovery_service.py::_build_findings_query`, never retyped.

| Query | Today | With index | With rewrite |
|---|---|---|---|
| locus-filtered count | 19,251 ms `[M]` | 114 ms `[M]` | 67 ms `[C]` |
| locus-filtered first 50 rows | 19,516 ms `[M]` | 116 ms `[M]` | 65 ms `[C]` |
| default page count (no filter) | 122 ms `[M]` | 91 ms `[M]` | unchanged |
| default page first 50 rows | 286 ms `[M]` | 308 ms `[M]` | unchanged |
| work filter, no range | 115 ms `[M]` | 110 ms `[M]` | unchanged |

Confirmed three independent ways: 19.2 s `[M]` warm, 38.4 s `[C]` fresh process, and 22.4 s
`[M]` against the *previous* artifact. **Pre-existing — the V4.2 deploy did not cause it.**

The rewrite and index figures come from different harnesses (Codex used an in-memory clone)
and are not directly comparable in absolute terms. Both are three orders of magnitude under
the 5 s budget, which is what decides the question.

### Mechanism

`shared/discovery_service.py:1315–1352` `[S]` builds a **correlated** `EXISTS` per findings row:

```sql
EXISTS (SELECT 1 FROM discovery_locus_piece p
        JOIN locus_unit lu ON lu.work_id = p.locus_work_id
             AND lu.unit_ord BETWEEN p.start_unit_ord AND p.end_unit_ord
        WHERE p.identification_id = di.identification_id
          AND p.locus_work_id = ? AND lu.citation_pos >= ? AND lu.citation_pos <= ?)
```

`discovery_locus_piece` (`scripts/build_discovery_sidecar.py:708–723` `[S]`) has
`PRIMARY KEY (identification_id, piece_ord)` and
`ix_discovery_locus_piece_range(locus_work_id, start_unit_ord, end_unit_ord, identification_id)`.

The PK autoindex *does* lead with `identification_id`, so the obvious question is why SQLite
ignores it. It doesn't prefer the range index for the identification lookup — it needs that
index to satisfy the `lu.unit_ord BETWEEN p.start_unit_ord AND p.end_unit_ord` join `[A]`.
So the inner loop costs **O(all pieces for that work)** per outer row, when an identification
carries on average **1.6 pieces** (96,059 pieces / 58,602 identifications) `[A]`. That ratio
is the whole 168×.

Plan before `[M]`:
```
SEARCH di USING INDEX ix_discovery_identification_order (main_pool=?)
CORRELATED SCALAR SUBQUERY 1
  SEARCH lu USING INDEX ix_locus_unit_part (work_id=?)
  SEARCH p USING COVERING INDEX ix_discovery_locus_piece_range (locus_work_id=? AND start_unit_ord<?)
```

### Amplification — why this is worse than one slow page

* `_DEFAULT_QUERY_TIMEOUT_FINDINGS = 5.0` (`shared/discovery_service.py:912`) `[S]`.
* Findings reads pass `heavy=True`; the heavy budget is `DISCOVERY_MAX_CONCURRENT_QUERIES=4`
  with a dedicated 4-worker executor (CLAUDE.md).
* `run_in_executor` threads **are not cancellable**. A read that times out at 5 s keeps its
  worker for the remaining ~14 s `[C]`. Four concurrent range readers exhaust the heavy pool
  and other heavy reads return `busy` for ~15 s.

**Do not "fix" this by raising the timeout** — that lengthens worker retention `[C]`.

One amplifier checked and ruled out: if `DISCOVERY_FINDINGS_COUNT_MAX` were enabled, the call
would run a second bounded count and hold a worker roughly twice as long `[C]`. Production
`.env` has 18 lines and `DISCOVERY_ENABLED` is the only discovery key set, so the cap is off `[M]`.

### Options

**Option A — rewrite the predicate (RECOMMENDED, code only).**
Materialize the matching ids once, driven from the citation units, instead of re-running the
interval search per row:

```sql
WITH matching_ids AS MATERIALIZED (
  SELECT DISTINCT p.identification_id
  FROM locus_unit AS lu
  CROSS JOIN discovery_locus_piece AS p
  WHERE lu.work_id = ? AND lu.citation_pos BETWEEN ? AND ?
    AND p.locus_work_id = lu.work_id
    AND p.start_unit_ord <= lu.unit_ord AND p.end_unit_ord >= lu.unit_ord
)
... AND di.identification_id IN (SELECT identification_id FROM matching_ids)
```

Semantically equivalent to the current `EXISTS`. 67 ms / 65 ms `[C]`.
**One-sided bounds must keep dynamic handling** — the current builder supports `locus_from`
alone, `locus_to` alone, or both, and the rewrite must preserve all three shapes.

*Why first:* ships as code. No rebuild, no manifest swap, no second deploy, rollback state
untouched. Given Problem 4, the artifact route is currently blocked anyway.

**Option B — add the index (artifact change).**
```sql
CREATE INDEX ix_locus_piece_by_identification
  ON discovery_locus_piece(identification_id, locus_work_id, start_unit_ord, end_unit_ord);
```
114 ms / 116 ms `[M]`; builds in 950 ms; grows the file 8,372,224 bytes (1.26%) `[M]`.
Faster than the rewrite, but requires a new artifact — see **Packaging** below.
Column order was checked: reversing the first two makes no material difference (both are
equality constraints), reversing start/end gains nothing, and replacing the PK is worse
(table rebuild, weakens the identity invariant) `[C]`.

**Options A and B are not exclusive.** A ships now; B rides the next canonical rebuild.

**Rejected — precomputing per-piece citation bounds.** Tested against the artifact: citation
positions are **not monotone globally, nor within every piece**, so per-piece min/max yields
false positives `[C]`. This is a correctness defect, not a slower optimisation. Do not revive
it without a richer derived relation and new correctness gates.

**Rejected — a covering index on `locus_unit(work_id, unit_ord, citation_pos)`.** Moved the
indexed count only 20.5 ms → 17.5 ms `[C]`. Not worth artifact growth.

### Packaging, if an artifact change is ever chosen

* `frame_content_hash` is **unaffected** by an index. `compute_frame_content_hash`
  (`scripts/build_discovery_sidecar.py:2873`) hashes only the ordered
  `discovery_claim` × `discovery_evidence` tuple set `[M, verified by reading]`. So the
  externally pinned `59021aba…` stays valid, and a rebuild can be *proved* to be the same
  corpus: same frame hash + same release-contract row counts + only the index differs.
* Nothing rejects a new index. `check_authorized_index_set`
  (`scripts/verify_discovery_sidecar.py:1857`) computes `_AUTHORIZED_INDEXES - present` —
  subset-only, no "unexpected extra" branch `[M]`. No file-size assertion, no schema
  fingerprint, and no test asserts an exhaustive index list `[A]`.
* **The index must go into the private builder's DDL, not the shipped file.**
  `scripts/project_discovery_public.py::_schema_ddl()` replays the *private* DB's
  `sqlite_master` DDL verbatim into the public artifact `[A]`. Patch only the deployed public
  `.db` and the next rebuild silently regenerates without the index.
* Convention: add the name to `_AUTHORIZED_INDEXES`
  (`scripts/verify_discovery_sidecar.py:1309`) and a dated Amendment in
  `docs/specs/discovery-sidecar-schema-v1.md`. Note `ix_discovery_locus_piece_range` is in
  neither, and `discovery_locus_piece` has **no entry at all** in the schema spec `[A]` —
  a pre-existing gap worth closing in the same edit.
* In-place mutation of the deployed artifact is contrary to the deploy spec, which calls the
  payload `IMMUTABLE — a new build is a new filename` `[A]`, and Codex independently advises
  against it `[C]`. Note however that `scripts/refresh_discovery_locus.py` already implements
  a *sanctioned* copy-and-mutate shape (copy → mutate with SQL → `integrity_check` +
  `foreign_key_check` → caller re-hashes and re-manifests) `[A]`. Treat that as an emergency
  procedure only, and never without a new hash-derived filename and the full staged
  verification + masking gate.

### How to verify a fix

1. Exact-result equivalence: for several `(work_id, from, to)` including one-sided bounds,
   the rewritten predicate must return the **identical identification id set** as the current
   one. Compare sets, not counts.
2. `EXPLAIN QUERY PLAN` over the statement produced by `_build_findings_query` — never a
   retyped copy. There is precedent for exactly this in `_build_manuscript_works_sql`'s docstring.
3. A bounded regression benchmark on the heavy work above, asserting well under 5 s.
4. Mutation-prove the new tests (project rule: a gate must be watched failing, and the
   mutation must change the artifact under test, not the test's own fixture).

---

## Problem 2 — the default findings page costs ~2 s and most of it is unexplained (P2)

Observed: 2.3–3.7 s locally, repeatedly; **1.99 s in production** `[M]`.

The event loop was **not** blocked during those requests — the loop-lag breach counter stayed
at 1 across seven consecutive slow requests `[M]`. The blocking work *is* correctly offloaded;
it simply sits serially on the request's own await chain.

Measured against the live artifact through the real service methods `[A]`:

| Component | Cost |
|---|---|
| rows + count | ~187 ms |
| **facet cascade** — domain 184 ms, author 80 ms, work 84 ms, **strictly sequential** | **~348 ms** |
| element construction, 50 rows + ~650 facet nodes | ~200–350 ms |
| **accounted for** | **~0.75–0.9 s** |

**~1.1–3.0 s remains unmeasured.** Two structural suspects, both verified in source, both
uncached round trips alone on the critical path:

* `_fetch_approved_review_map` (`web/pages/findings.py:1172`) — **no cache at all**, a live
  Supabase RPC on *every* render `[M]`. This is the identification-reviews beta, now taxing
  every findings page load.
* `suppressed_identification_ids()` (`web/pages/findings.py:1312`) — 30 s TTL, awaited
  **alone** before the body starts building `[A]`.

The facet cascade is a real, sequential `for level in ("domain","author","work")` with an
`await` inside; its only cache is a dict recreated per page load `[M]`.

### Trap — do not naively `asyncio.gather` the facets

The three levels are independent, so gathering looks free. It is not: findings reads take the
**heavy** budget of 4, so three concurrent facets plus the rows read consume the entire budget
for one visitor and the second visitor gets `busy`. This is the same failure CLAUDE.md records
for the browse connections panel, which is why that path was given its own 24-wide budget with
its own executor. Any parallelisation here needs the equivalent treatment.

### What would settle the missing seconds

Nobody has measured these; do this before optimising anything in this section:
1. `time.perf_counter()` around the two Supabase calls in a **live** process against the real
   project (reuse the `web/perf_watch.py` pattern).
2. A browser Network/Performance trace of one load, to separate server compute from websocket
   transfer and client-side mount. ~650 facet DOM nodes is not obviously cheap.

Optimising the ~0.9 s we can see while ignoring the ~2 s we cannot would be the wrong order.

---

## Problem 3 — `bench_discovery.py` is not a smoke test, and it hung the deploy (P2)

> **FIXED 2026-08-20.** All four parts:
>
> 1. **Off the deploy path.** `scripts/smoke_discovery_readiness.py` is new and is what
>    §2.6 and the deploy script now run: it answers "can the app load what it now serves,
>    and does that asset answer real reads" in about a second, through the real
>    `load_discovery_state()` and `web.discovery` paths. It deliberately includes a
>    citation-range read over the heaviest locus-bearing work, because a smoke that only
>    fetched the default page did not see Problem 1. §2.7's "or run `bench_discovery.py`
>    and read its added RSS" is gone too — the RSS now comes from `/proc/<pid>/status`.
>    Every surviving mention of the benchmark in the runbook is a warning, and a test
>    asserts no `python scripts/bench_discovery.py` command line remains in it.
> 2. **Every statement bounded.** A `BoundedConnection` wrapper installs a
>    `set_progress_handler` deadline, so all four connection sites are covered rather than
>    the three call sites the assessment named — wrapping the connection was chosen
>    precisely because a budget that must be remembered at eighteen `conn.execute` sites
>    is one that gets missed at the nineteenth. `--query-timeout-s` defaults to **30 s and
>    is ON**; an abort raises a named `QueryBudgetExceeded`, never confusable with a cap
>    breach (that means the statement finished, only too slowly). Proven firing at 20 ms
>    against a 0.02 s budget.
> 3. **ssh keepalive.** Both `Invoke-Box` helpers pass
>    `-o ServerAliveInterval=15 -o ServerAliveCountMax=8`.
> 4. **Divergence-correct picks.** `_coherent_bucket_pick` draws from
>    `_build_findings_filter`'s real predicate for the bucket, so the picks match the
>    population the timed query measures. Both parameter-order traps the assessment named
>    are handled per call site, and the builder's `"WHERE "` prefix is stripped once in
>    one helper rather than at six call sites. Measured on the V4.2 artifact: the
>    divergence default removes **12.6%** of main and **38.8%** of "more" —  the
>    assessment's 12.6%/38.7%, confirmed. `_state_skip`'s `keys` map gained `suppressed`
>    and `sys_id`.
>
> 7 tests; 9 mutations each watched failing. One of those mutations exposed a gap in the
> tests themselves: asserting `_bucket_predicate` in isolation left the consumer free to
> stop calling it, so a behavioural test now builds an asset whose raw-heaviest work is not
> its divergence-filtered heaviest and asserts the pick chooses the latter.


`_tmp/deploy_v42_discovery.ps1` step 8 called it as a "readiness smoke". It is not one.

`bench_findings_page()` is invoked **unconditionally** in `main()` with `repeats` defaulting
to 5; **neither `--sample` nor `--warm-passes` bounds it** `[M]`. Those flags only shrink the
service-level half. What actually runs `[A]`:

* 2 buckets × 2⁹ axis subsets = **1,024 filter states**
* × 3 units × 3 sorts, plus deep-page and visible-total specs = **15,363 combinations**
* × 5 repeats ≈ **76,815 timed SQL executions**

With Problem 1 unfixed, the expensive locus states cost ~940 ms each and full completion is
**one to two hours** `[A]`. A `--findings-only` run produced zero output in 9 minutes `[A]`.
Even *with* Problem 1 fixed this remains a ~25-minute step — it does not belong on a deploy path.

**How it actually failed:** `client_loop: send disconnect: Connection reset` →
`the readiness bench failed (ssh exit 255)` `[M]`. The ssh connection died under an idle
bench; the script correctly threw and exited 1. So the V4.2 deploy is recorded as **failed**
even though steps 0–7 succeeded and the swap is live and healthy (see Appendix).

### Four separate fixes

1. **Take the benchmark off the deploy path.** Step 8 needs a real smoke: a handful of queries
   proving the swapped-in asset loads and answers, plus the journal/RSS checks.
2. **Bound every statement.** Nothing in the file bounds any query — not `_time_sql` (:676),
   not `_state_skip`'s probe (:1057), not the `unit_rows` counts (:1111) `[A]`. Use
   `sqlite3.Connection.set_progress_handler` — same-thread, no signals, works on Windows —
   with a `--query-timeout-s` flag, raising a **named** failure distinct from a normal cap
   failure `[A]`. This is the guard that would have caught Problem 1 the day it landed.
3. **ssh keepalive.** The deploy's `Invoke-Box` needs `ServerAliveInterval`, or the long step
   must run detached with its output polled. A silent reset is indistinguishable from a real
   failure — which is exactly what happened.
4. **Divergence-correct picks** (below).

### The divergence gap in the picks — real defect, not the cause here

`_coherent_bucket_pick` (`scripts/bench_discovery.py:334`) builds
`bucket_sql = "di.main_pool = 1"` or `"= 0"` and **nothing else** `[S]`, while the timed
queries default to `divergence=DIVERGENCE_HIDDEN`. That default removes **12.6%** of the main
bucket and **38.7%** of "more" `[A]`. A picked value can therefore survive `main_pool` and be
emptied by the real query, tripping the F14 loud abort (:1334–1346) — and `_state_skip`'s
single-axis carve-out (:1045) explicitly skips verification for exactly those states.

**It did not fire on this artifact:** all 12 single-axis states were re-run through the real
builder with the default in force and every one is non-empty `[A]`. An earlier report that the
benchmark "aborted on a zero-row combination" is **not supported** on this artifact; the
observed stop is consistent with an external timeout.

**Fix:** replace the hand-written `bucket_sql` with the WHERE clause `_build_findings_filter`
returns for that bucket with no other axis set, so picks are drawn from the exact population
the timed query filters to. Two traps `[A]`:
* **Parameter order.** Several picking queries have a `?` in the `SELECT`/`CASE` that precedes
  the `WHERE`; the bound tuple must be `(prefer_novelty,) + tuple(where_params)`. Getting this
  backwards binds silently to the wrong placeholder.
* **String composition.** The builder returns a full `"WHERE …"` (or `""`), not a bare boolean.
  Call sites that AND an extra condition must append, not substitute.
* Residual: `_state_skip`'s `keys` map has no entries for `suppressed` or `sys_id`, so those
  fall through the same unverified carve-out. Close in the same edit.

---

## Problem 4 — the V4.2 recipe cannot rebuild what production serves (P2)

`_tmp/build_v42lit_sidecar.ps1` fails two of its own hash pins against current disk `[M]`:

| Input | Pinned | Actual |
|---|---|---|
| `discovery_builds/discovery_v4_2/build/work_domains_v42lit.json` | `4f90ffc7…` (literal, ps1) | `79c9ea13…` |
| `discovery_data/work_author_aliases-v42lit.json` | `b2bf3cff…` (`_tmp/v42lit_alias_hash.txt`) | `dc94b4b0…` |

The loaders fail closed, so running it verbatim **aborts before distillation** `[A]`.

Consequence: **rollback-by-rebuild does not exist right now.** Rollback by atomic manifest
repoint is unaffected and remains the primary path — `manifest.prev.json` and
`manifest.pre-v42-bbd81d70ae8dadaf.json` are both on the box `[M]`.

The recipe's own comment names the anti-pattern it fell into — for the novelty sha it says
*"the sha is written by the run, never hand-copied."* `--work-domains-content-hash` is a
hand-copied literal, and it is the one that went stale.

Note also that **byte-identity was never on offer**: `build_date = _now_iso()`
(`scripts/build_discovery_sidecar.py:5359`) is written into `meta` before the file hash is
computed (:9056) `[A]`. `frame_content_hash` exists to route around exactly that. Only the
`--golden`/`--smoke` fixture path uses a frozen timestamp.

Rebuild cost, from real timestamps and logs rather than estimates `[A]`:
* reusing cached intermediates (all present on disk): **~35–60 min**
* also regenerating the Track-1 matcher: **+99.9 min** (logged: `DONE v42lit/live: tier-A rows 496,890 (99.9 min)`)
* also refreshing the LLM novelty gate: tens of minutes, ~$9.43 at the last run

**Do this before any future rebuild**, and prefer deriving both hashes at run time from the
files rather than re-pinning literals.

---

## Problem 5 — a second index candidate (P3, non-urgent)

```sql
CREATE INDEX ... ON discovery_identification(
  display_work_id, main_pool, best_band_rank, max_coverage_ppm);
```

The work-filtered, no-range path currently enters through the main-pool ordering index and
filters the work afterward. On an in-memory clone this moved a representative work-filtered
count/page from 16.5/16.0 ms to 1.4/2.1 ms `[C]`; the corresponding real-file figure today is
115 ms `[M]`. Functional, just not optimal. Bundle into the next canonical rebuild alongside
Option B — not worth an artifact change of its own.

No other high-cost removable-index scan was found in the shipped service SQL `[C]`. Remaining
scans are full-population counts, window totals, or cached statistical reads, where an
ordinary index does not remove the required work.

---

## Fixed on 2026-08-19 — context only, do not redo

Both are committed to the working tree but **not deployed**.

**perf-watch misattribution** (`web/perf_watch.py`). The slow-request hint compared each
request against the **all-time maximum** lag, so one 4,094 ms stall at startup made every
later slow request print *"suspect the loop was blocked elsewhere"* — including the seven
`/computed-identifications` builds in Problem 2, during which the loop demonstrably was not
blocked. Replaced with a bounded ring of stall windows plus an in-flight branch for the stall
the monitor is itself stuck behind. Separately, `event loop BLOCKED for 3069031 ms` was a
suspended process; a tick minutes late having burned no CPU is now `monitor NOT SCHEDULED`
and kept out of `max_lag_ms` (`GENIZAH_NOT_SCHEDULED_MS`, default 60000). The same
`time.process_time()` measurement now names each real stall's kind — GIL-bound vs blocking I/O.
28 tests; 5 mutations each watched failing.

**`/admin` blocked the loop** (`web/pages/admin.py`). Six synchronous Supabase loader calls
(up to eight queries) on the event loop, two of them duplicates — Statistics re-fetched what
the Users and Pending tabs had already loaded. Four mutating handlers did the same. Now one
`_load_admin_data` payload through `run.io_bound`, shared across tabs (6 calls → 4), with
`get_user_client()` resolved on the loop and threaded in — building it in the worker would
read the auth session outside the NiceGUI context, get `{}`, and hand the moderation queue an
anonymous client. 34 tests; 6 mutations each watched failing.

---

## Recommended order

1. **Problem 1, Option A** — the rewrite. Fixes a live outage, ships as code.
2. **Problem 3** — bench off the deploy path + per-statement budget + ssh keepalive. Without
   this, the next deploy has no readiness verdict either.
3. **Problem 4** — refresh the two pins, before any rebuild is needed under pressure.
4. **Problem 2** — measure the two Supabase calls and the browser trace *first*, then decide.
5. **Problems 1B + 5** — indexes, bundled into the next canonical rebuild.

---

## Appendix — production state as of 2026-08-20

Verified read-only `[M]`:

```
asset      discovery-v1-e5bcd3473ae0ebe73c510fd80562055ae6298958938a6c56a50c760078bffd8b
frame      59021aba523b8e1b88578a7a9fb7316d051a512479a2ddaa2a98d8b6c1fc8db7
rollback   manifest.prev.json, manifest.pre-v42-bbd81d70ae8dadaf.json
journal    0 "sidecar not loaded" / "fail-closed" lines in 6 hours
RSS        1,619,328 KB
probe      GET /computed-identifications -> 200 in 1.99 s
```

Both gates passed on the box against the staged bytes before the swap
(`verify_discovery_sidecar`: all invariants pass; `check_atlas_masking`: no matches).
The deploy is healthy despite the script's exit 1 — see Problem 3.

## Appendix — reproducing the measurements

* Live artifact:
  `discovery_builds/discovery_v4_2/build/deploy/discovery-v1-e5bcd347….db` (664,911,872 bytes).
  Previous artifact, for comparison: `discovery_data/live/discovery-v1-528f6d36….db`.
* Open `mode=ro` via URI. To measure an index, `shutil.copy2` to a scratch path first — never
  mutate either file.
* **Always obtain SQL by calling `_build_findings_query` / `_build_findings_filter`.** An index
  assertion against a hand-retyped near-copy proves nothing.
* Take a median of several warm runs; single cold runs on a 665 MB file vary by 2× (19.2 s and
  11.0 s were both measured for the same probe).
* `MASKING_SCAN_PATTERNS_FILE=.masking_patterns` is required before committing tracked files;
  unset means the scan fails safe (RED), not green.
