# Pitfalls Research

**Domain:** Adding a corpus-scale same-work discovery module — network/atlas visualization, confidence-banded algorithmic identifications, and community judgment capture — to the existing GenizahSearch web app (NiceGUI + FastAPI + SQLite sidecars + Supabase; bilingual EN/HE RTL; prod = one 15.4 GB Linux box shared with the live app)
**Researched:** 2026-07-19
**Confidence:** HIGH (grounded in the SEED-029 probe artifacts — PROBE-RESULTS.md, METHOD.md, E1-ROUND2/3-RELEASE.md, rehearsal atlases — plus direct inspection of `supabase_setup.sql`, `shared/corrections_service.py`, and the CLS/NLI-breaker/`safe_storage`/deploy-ordering history in CLAUDE.md + MEMORY.md)

> This file is scoped to the mistakes specific to bolting THIS module onto THIS system. Generic "validate input / use HTTPS" advice is omitted. Every pitfall carries a concrete guard/test/design rule and the phase that should own it. The M-source provenance-masking pitfall (P1) is treated as a hard release blocker with a per-vector leak checklist.

---

## Critical Pitfalls

### P1: M-source provenance leaks through titles, URLs, API fields, exports, or SEO (HARD RELEASE BLOCKER)

**What goes wrong:**
The reference corpus that powers Track-1 identification derives partly from a licensed scholarly text site (M-source). The hard constraint is that this provenance must never be revealed: work titles must be neutral canonical names, no reference-edition text is ever displayed, and no naming convention (`M-source`, M-source sigla, DB-internal source tags, reference-edition locus formats) may leak through any surface. Because the module touches *every* output surface (browse panel, work pages, atlas labels, leads queue, community payloads, JSON API, XLSX/CSV exports, SEO/JSON-LD), a single un-audited surface breaks the constraint — and once a title string or a `source='m_source'` field ships in an export or a cached SEO snippet, it is effectively unrecallable.

**Why it happens:**
The research DB (`fullcorpus_v2.db`) was built by researchers for researchers — it carries raw provenance in table/column names, work-title strings, span-locus references, and reference-edition text columns because that was useful during the probe. Distilling it into a product sidecar without a *deny-by-default* projection lets research-internal fields ride along. Developers naturally add "just show the matched reference text so the scholar can compare" — which is exactly the forbidden display.

**How to avoid:**
- Treat masking as a **projection at the sidecar-build boundary, not a filter at render time.** The product sidecar must physically NOT CONTAIN reference-edition text, M-source sigla, or source-provenance columns. If the string isn't in the sidecar, it cannot leak downstream. Never ship `fullcorpus_v2.db`; ship a distilled sidecar whose schema was designed masked-first.
- Maintain a **canonical work-title allowlist**: every `work_id` maps to ONE neutral Hebrew canonical title + optional English, curated/reviewed, with a guard that rejects any title matching a M-source-convention denylist regex (`M-source`, known siglum patterns, reference-locus formats).
- Add a **leak-vector CI test** (see checklist below) that scans EVERY output surface for the denylist. This is a permanent guard, in the spirit of `tests/test_no_raw_storage_access.py` and `tests/test_web_library_options_no_local.py`.
- Only ever display **our** manuscript text (MiDRASH HTR / PGP / FGP transcriptions) — never the reference-edition text used to make the identification. The "compare against the reference" affordance is forbidden; the comparison is scholar-vs-our-manuscript-text only.

**Leak-vector checklist (must all pass before ship — release blocker):**
- [ ] **Work titles** — every displayed title is from the neutral allowlist; denylist regex finds zero hits across the whole `works` table.
- [ ] **Sidecar schema** — no column holds reference-edition text or a provenance/source tag; grep the built sidecar's schema + a full row-value scan for the denylist.
- [ ] **URLs / route params** — work-page slugs and query params use opaque `work_id`, never a title-derived or siglum-derived slug that encodes provenance.
- [ ] **JSON API responses** — `/api/*` discovery endpoints (if added) never emit a `source`, `ref_locus`, or `ref_text` field; response models are allowlist-only (Pydantic `extra='forbid'`, echo only whitelisted keys).
- [ ] **Exports** — XLSX/CSV/JSON dossiers of connections/witnesses carry only neutral titles + our-text; run the denylist scan over a generated export fixture.
- [ ] **SEO / JSON-LD** — homepage promo, work-page meta tags, and structured data emit neutral titles only (cached SEO snippets are the highest-regret vector — they are crawled and archived externally).
- [ ] **Atlas labels / tooltips** — node labels, edge tooltips, cluster names are neutral titles, not raw work strings.
- [ ] **Leads queue + community payloads** — the claim text a user confirms/rejects, and anything written to Supabase, carries no provenance.
- [ ] **Error messages / logs** — a stack trace or debug log surfacing a raw work string in a user-facing error is a leak.

**Warning signs:**
Any code path that reads a `source`/`ref_*` column; any title rendered directly from the research DB; a reviewer asking "should we show the reference text for comparison?"; a slug built from a title.

**Phase to address:** Data-layer / sidecar-distillation phase (design masked-first) + a dedicated **masking-hardening gate as a release blocker** (the leak-vector CI test spans every later phase's surfaces).

---

### P2: Screening leads get cited as facts (band labels lost in the gap between "identification" and "lead")

**What goes wrong:**
The module ships tiers of very different epistemic strength: tier-A (275,894 identifications, precision certificate pending), R-A "expert-reviewed" 0.889 (independent audit still pending), R-B screening 0.859, R-CANON screening 0.647, and an unmeasured algorithmic tail. A mixed scholar/layperson audience will screenshot, copy-paste, export, or cite a *screening lead* ("this fragment = work X") as if it were a certified fact. Roughly 1 in 3 R-CANON leads is wrong (0.647 precision); ~14% of R-B leads are wrong. If a scholar publishes "identified via GenizahSearch" on a 0.647-precision lead, that is a reputational hit to the platform and to the scholar.

**Why it happens:**
Band labels live in the UI chrome (a badge, a color, a toggle state) but the *claim text* ("Fragment T-S X = Mishnah Shabbat 3") is self-contained and travels without its chrome. Copy/paste, export rows, SEO snippets, and community-shared links strip the badge. Defaulting the high bands "on" and hiding leads behind a toggle helps display but does nothing for the detached claim.

**How to avoid:**
- **Bind the band label to the claim string itself, not to surrounding chrome.** Every exported/copied/API-serialized claim carries its band as an inseparable field (e.g. row includes a `confidence_band` column; copy-to-clipboard includes "(screening lead — not certified)"; JSON `band` is mandatory, never optional).
- **Default to high bands only**; put R-B/R-CANON behind an explicit "show uncertified leads" toggle (already the plan) AND label the leads queue itself "not certified — leads for review."
- Use **honest, self-explaining band names** in both languages — not opaque codes. "Expert-reviewed" vs "screening lead (unverified)" vs "algorithmic suggestion (unmeasured)". Never surface `R-A`/`R-B`/`R-CANON`/`tier-A` internal codes to users.
- **Recall-honesty label** ("no identification shown ≠ none exists") on every work/connections surface, per the milestone plan.
- Add a **precision number in the UI** for each certified band (the tier-A stratified certificate; R-A 0.889) so the strength is quantified, and explicitly mark uncertified tiers as *unmeasured/screening*.

**Warning signs:**
An export row or API object with a claim but no band field; a copy action that yields bare text; a UI where the band is a color only (colorblind + copy-unsafe); a user support question that quotes a lead as settled fact.

**Phase to address:** Band-schema phase (band travels with the claim in the data model) + every UI/export/API phase (connections panel, work pages, leads queue, exports) must re-verify the binding.

---

### P3: "Expert-verified" overclaims while the independent audit is still pending

**What goes wrong:**
The R-A band measured 0.889 precision but is explicitly **"confirmed Broad, single-expert, uncertified, valid-with-deviation"** in E1-ROUND2 — the ≥40-card independent-auditor gate has NOT been passed. If the UI says "expert-verified" (or worse "verified"/"certified") for R-A before that audit lands, the platform is making a stronger claim than the evidence supports, on its flagship feature. The milestone acknowledges this: labels say "expert-verified" until the audit passes — which means the exact wording and its conditional status is load-bearing and easy to get wrong.

**Why it happens:**
0.889 feels strong; the single-expert grading was done by the domain lead; the distinction between "single-expert confirmed" and "independently audited" is invisible to anyone not tracking the E1 registry. A copywriter will round "expert-reviewed, audit pending" up to "verified."

**How to avoid:**
- Pin the R-A label to the audit state with a **single source of truth** (a `certification_status` field / config flag) so flipping it post-audit is a one-line change, not a copy hunt across surfaces.
- Ship the honest wording ("expert-reviewed; independent audit pending") and treat the label string as an approval-gated artifact (per the "approve release texts before applying" project rule).
- Do NOT use the words "certified"/"verified" unqualified for R-A until the auditor gate passes. R-CANON and R-B are screening — never call them verified at all.
- Add a test that asserts no UI/API surface emits "certified"/"מאומת" for a band whose `certification_status != 'certified'`.

**Warning signs:**
A hardcoded "verified" string next to R-A; marketing/homepage copy that drops the "audit pending" qualifier; no single flag controlling the label.

**Phase to address:** Band-schema phase (certification_status field) + homepage-promotion/release phase (copy approval) + a label-consistency guard test.

---

### P4: Naive full-graph atlas rendering melts browsers AND is scholarly meaningless

**What goes wrong:**
The connection graph is ~52K MSS with a **giant liturgical component of 15,969 MSS (89% of nodes)** that survives flank-contrast — i.e. a naive force-directed render of the full graph is (a) a hairball that conveys zero information, and (b) tens of thousands of nodes/edges shipped to the browser, which hangs or crashes the client (especially mid-range devices) and blows the WebSocket/DOM budget. The probe already flagged this: "naive full-graph rendering is meaningless AND melts browsers; server-side subgraph bounding required."

**Why it happens:**
Graph libraries (vis-network, Cytoscape, sigma, d3-force) demo beautifully on 200 nodes. The corpus-scale reality only appears with real data. The 89% blob means "show me the connections" naively returns almost the whole corpus. Client-side filtering still requires shipping the full graph first.

**How to avoid:**
- **Server-side subgraph bounding is mandatory** — never send the full graph to the client. The decided model is a **per-manuscript ego-network** ("connections" panel = one fragment's bounded neighborhood, N hops, capped node/edge count) and **per-work witness subgraphs**, not a global canvas.
- For any global/atlas view, render **canon-masked, aggregated** structure (clusters/communities as super-nodes with counts), not raw nodes — the rehearsal showed the census only becomes legible AFTER Track-1 canon masking dissolves the liturgical blob.
- Enforce a **hard node/edge cap per response** (e.g. ≤300–500 nodes) with server-side truncation + an explicit "N more, refine to see" affordance; log truncations.
- Precompute community/cluster assignments and edge weights **in the sidecar at build time** — do not compute graph layout or connected-components on the request path.
- Choose a renderer that does WebGL/canvas for larger sets (sigma.js / Cytoscape canvas) rather than SVG-per-node, but the cap is the real fix, not the renderer.

**Warning signs:**
A query that returns >1K nodes; browser tab memory spiking on the atlas page; a "show full graph" button; graph layout computed in the request handler; the liturgical blob visible as an undifferentiated mass.

**Phase to address:** Atlas/graph phase (server-side bounding + caps) — and the connections-panel phase (ego-network model). Sidecar phase must precompute clusters/components.

---

### P5: Heavy graph/connection queries block the NiceGUI event loop and take down the shared box

**What goes wrong:**
A corpus-scale connection query (neighborhood expansion, work→witnesses fan-out, cluster lookup) run synchronously in a NiceGUI handler blocks the single asyncio event loop, freezing ALL users of the live app — the same failure class as the 2026-05-25 NLI-IIIF hang that saturated the Starlette threadpool and required SIGKILL. The discovery module runs on the SAME process and box as production search/browse.

**Why it happens:**
NiceGUI's `app` is a FastAPI instance on one event loop; SQLite calls and graph traversals are synchronous CPU/IO. Developers wire a query straight into a page handler. It's fine in dev with one user and a warm cache; it stalls under concurrent prod load.

**How to avoid:**
- Run all sidecar/graph queries **off the event loop** — `run.io_bound` / `asyncio.get_event_loop().run_in_executor`, mirroring the existing browse-enrichment executor pattern (`SEARCH_API_BROWSE_*` timeouts, bounded executor from SEED-016).
- Put **per-query timeouts + a concurrency cap** on heavy graph endpoints (mirror `SEARCH_API_HEAVY_CONCURRENCY` → 503 + Retry-After) so a burst of atlas requests can't exhaust workers.
- Keep the discovery sidecar queries **indexed and bounded** (the node/edge caps from P4 double as query bounds); never a full-table scan or an unbounded recursive CTE on the request path.
- Precompute expensive aggregates (component sizes, cluster membership, per-MS connection counts) at sidecar-build time.

**Warning signs:**
Synchronous `sqlite3`/service calls inside an `async` page function; no timeout on a graph query; p95 latency climbing on `/search` and `/browse` when the atlas gets traffic; recursive CTE with no depth bound.

**Phase to address:** Discovery-service phase (executor + timeout + concurrency wrapper) — establish this in the thin de-risk spine phase before any UI lands.

---

### P6: The distilled sidecar bloats the shared 15.4 GB box (and/or drifts from the research schema)

**What goes wrong:**
The research DB is 2.9 GB (`fullcorpus_v2.db`). If the product sidecar is distilled carelessly — keeping span-level rows, verifier scratch columns, full edge lists including the liturgical blob, or the reference text (which also breaks P1) — it can stay multi-GB. On a 15.4 GB no-swap box already running Tantivy indexes in RAM (5–6 GB baseline) plus the existing sidecars (fjms 1.5 GB, VS 1.3 GB, etc.), a large new memory-mapped sidecar pushes RSS toward the `MemoryHigh` cap, where the allocator-ratchet + SemrushBot crawl already drive OOM pressure (documented 2026-07-08).

**Why it happens:**
"Just ship the research DB" is the path of least resistance. Schema drift compounds it: the research pipeline evolved across MAPV2/E1/Q2 rounds, so column names and semantics differ from what a clean product schema needs, and an ad-hoc distillation copies the drift.

**How to avoid:**
- **Design a clean product schema** (works, identifications w/ band, edges w/ weight+band, precomputed clusters) and write a **documented, repeatable distillation script** research-DB → sidecar. Snapshot ship + documented rebuild recipe (refresh pipeline explicitly out of scope per the milestone).
- **Drop everything the UI doesn't read**: span offsets, verifier internals, reference text (P1), and the giant-liturgical-blob edges that the atlas will never render individually (store the aggregate cluster instead).
- Measure the sidecar's on-disk AND resident footprint; budget it against the box. Consider whether it needs to be memory-resident at all or can stay on-disk SQLite with indexes.
- Pin the distillation with a **schema/row-count assertion test** and a `PRAGMA integrity_check`, so schema drift from a re-run is caught.

**Warning signs:**
Sidecar > ~500 MB–1 GB without justification; RSS climbing after deploy; the distillation script is a one-off notebook, not committed + documented; column names in the sidecar that echo research-round jargon.

**Phase to address:** Data-layer / sidecar-distillation phase.

---

### P7: Deploy ships code before the sidecar (the 2026-05-11 incident class)

**What goes wrong:**
The new discovery pages/services expect the sidecar to exist. If a `deploy.sh` run pushes code before the (large) sidecar is scp'd + in place, the module 500s or serves empty/partial data — exactly the failure the 2026-05-11 incident codified into the project rule "scp DBs FIRST, then push code." A multi-hundred-MB sidecar upload also takes time and can fail mid-transfer, stranding the app on a half-present DB.

**Why it happens:**
Code deploys are fast and automated; DB uploads are manual and slow, so the tempting order is code-first. The dependency is invisible until prod.

**How to avoid:**
- Follow the codified order: **scp the sidecar FIRST, verify it in place (size + `integrity_check`), THEN deploy code.** Document this in the deploy recipe for this module.
- Make the code **fail-open / graceful when the sidecar is absent** (feature-flag the whole module, like `FGP_TRANSCRIPTIONS_ENABLED` / `WEB_FGP_ENABLED`), so a missing/half-uploaded sidecar degrades to "module unavailable" rather than 500s across the app.
- Add a startup check that logs sidecar presence + version and disables the module cleanly if missing.

**Warning signs:**
No feature flag on the module; code that assumes the sidecar exists at import; deploy runbook that doesn't mention the sidecar order.

**Phase to address:** Discovery-service phase (feature flag + fail-open) + release phase (deploy runbook).

---

### P8: Community judgment table missing GRANTs / RLS / audit trail (Supabase misconfiguration)

**What goes wrong:**
The community-judgment feature needs new Supabase table(s) (confirm/reject/annotate a work-witness claim). Three concrete failure modes: (1) **Missing `GRANT`s** — the existing `supabase_setup.sql` has RLS + policies but **zero `GRANT` statements**; per the project rule (2026-05-30 requirement) every new `public` table needs explicit `GRANT`s for the PostgREST roles in addition to RLS/policies, or `supabase-js` calls fail with confusing permission errors. (2) **RLS holes** — copying a policy wrong lets users edit others' judgments or read drafts. (3) **No audit trail** — judgments stored as mutable rows with no immutable history means a flipped/deleted vote leaves no trace, and future certification rounds can't reconstruct who-said-what-when.

**Why it happens:**
The existing tables predate the GRANT requirement so they're a misleading template (they work because they were provisioned before the requirement or via the dashboard). RLS is easy to get subtly wrong. Audit trails feel like over-engineering until you need to feed judgments into a certification round.

**How to avoid:**
- Every new table's migration includes **explicit `GRANT`** statements for the needed roles (per CLAUDE.md rule) AND `ENABLE ROW LEVEL SECURITY` AND per-operation policies — model on the `corrections`/`discoveries`/`*_votes` tables but ADD the grants the old ones lack.
- **Append-only judgment model**: store each judgment as an immutable row (user, claim_id, verdict, timestamp, optional note); "changing your vote" = a new row, not an UPDATE. This gives a free audit trail and lets certification rounds replay history. Mirror the `correction_votes`/`discovery_votes` pattern but design for immutability.
- Route ALL writes through the service layer + `safe_storage`/authenticated-client chokepoints (Phase 87 multitenant invariant); RLS `WITH CHECK (auth.uid() = author_id)` on insert; read policy scoped correctly for public-vs-own.
- Add the two-role GRANT + RLS-reachability to the phase's security gate (`/gsd-secure-phase`), and a smoke test that an anon and an authed client hit the expected allow/deny.

**Warning signs:**
A new-table migration with policies but no `GRANT`; `supabase-js` "permission denied for table" errors; judgments stored as UPDATE-in-place; no `created_at`/immutable history.

**Phase to address:** Community-judgment phase (schema + migration + RLS + GRANT + audit-trail design; security gate).

---

### P9: Community judgments pollute the certified bands / precision numbers

**What goes wrong:**
Unvetted community confirm/reject votes leak into the "certified" band computation or the displayed precision numbers, so a handful of enthusiastic (or malicious) users can inflate/deflate a band's apparent quality, or a screening lead gets visually "promoted" to look verified because it has community confirms. This corrupts the whole epistemic contract the module is built on (the E1 machinery is a *pre-registered, blind, expert* protocol — community votes are a different, weaker signal). The memory rule is explicit for the research side: "Catalogue = recall yardstick, NEVER acceptance evidence" — the same discipline must hold for community votes as product evidence.

**Why it happens:**
It's tempting to show "3 scholars confirmed this" as a trust signal and to let it bump a lead's ranking or band. But community votes are unweighted, self-selected, and gameable; conflating them with the certified precision undermines the certificate.

**How to avoid:**
- **Community judgments are a SEPARATE channel** feeding *future* certification rounds — they never mutate the shipped band, the certified precision number, or a lead's band label at runtime. Store them; display them as community signal clearly distinct from the algorithmic band; do not let them recompute certification.
- Never auto-promote a lead across bands on community votes. Promotion happens only through a new certified round.
- Show vote counts with honest framing ("community input — not part of the certified precision"); keep certified precision sourced from the frozen E1 measurement only.

**Warning signs:**
Band label / precision that changes with vote count; ranking that sorts by community confirms; a "verified by community" badge indistinguishable from "expert-reviewed."

**Phase to address:** Community-judgment phase (separation of channels) + band-schema phase (bands are immutable/frozen at ship).

---

### P10: Judgment spam / vandalism / contradictory votes with no moderation model

**What goes wrong:**
Logged-in users spam confirms/rejects, vandalize with junk annotations, or produce directly contradictory judgments on the same claim, and there's no moderation, rate-limit, or conflict-resolution model. The corpus has ~52K MSS × thousands of claims — a bored or motivated user can generate a lot of noise, and contradictory expert judgments (a genuine scholarly disagreement) need to be represented as *disagreement*, not silently last-write-wins.

**Why it happens:**
The MVP community feature copies the corrections pattern (which is low-volume, page-scoped) without accounting for the far larger claim surface and the mixed-expertise audience. Contradiction is treated as a bug rather than data.

**How to avoid:**
- **Model disagreement explicitly**: append-only judgments (P8) naturally represent "user A confirmed, user B rejected" as two rows; surface it as "2 confirm / 1 reject" rather than a single collapsed verdict.
- **Rate-limit + require auth** for writes (login-gated, like community joins/corrections); consider a lightweight per-user daily cap.
- Provide a **moderation/status field** (proposed/flagged/hidden) and an admin path to hide vandalism (mirror the `discoveries.status` / `corrections.status` enums that already exist).
- Annotations are free text → **XSS-sanitize on render** (NiceGUI + any HTML), and length-cap.

**Warning signs:**
No auth gate or rate limit on judgment writes; a single collapsed verdict per claim; free-text annotations rendered as HTML unescaped; no way to hide a bad row.

**Phase to address:** Community-judgment phase (auth gate, rate limit, moderation status, disagreement model, sanitization).

---

### P11: Homepage promotion regresses CLS / adds render-blocking work (the 2026-06-15 class)

**What goes wrong:**
The milestone promotes the Atlas + discovery suggestions "prominently on the homepage as the new flagship modules." Injecting a graph widget, dynamic suggestion cards, or a stats band into the homepage without reserving layout space regresses Cumulative Layout Shift — the exact problem fixed on 2026-06-15 (CLS p75 ~0.9 on `/search`, `/parallels`, `/` fixed by converting banners to `position:fixed` toasts and reserving thumbnail/container heights). A heavy graph on the landing page also adds render-blocking JS and first-load cost for every visitor.

**Why it happens:**
Homepage additions render asynchronously (suggestions fetched after load, graph canvas sized after data arrives) and push content down when they appear. Graph libraries are heavy JS bundles loaded eagerly.

**How to avoid:**
- **Reserve fixed height / min-height** for any async discovery widget on the homepage (the established fix); use skeletons, not layout-shifting late inserts.
- **Lazy-load the graph renderer** — the homepage shows a lightweight promo/teaser (static image or tiny bounded preview), and the full atlas JS loads only on the atlas route.
- Require a **live render-smoke test** for the homepage (per the "NiceGUI web UI needs render-smoke tests" rule) and check CLS on `/` after the change.
- Don't run discovery queries synchronously during homepage render (P5).

**Warning signs:**
CLS p75 climbing on `/`; a full graph library in the homepage bundle; suggestion cards popping in and shoving content; no reserved height on the promo module.

**Phase to address:** Homepage-promotion / release phase (CLS-safe layout + render-smoke test).

---

### P12: i18n/RTL breakage on graph labels, work titles, and band names

**What goes wrong:**
Graph node/edge labels, work titles (Hebrew), band names, and the leads-queue/connections UI break under Hebrew RTL: labels render LTR inside an SVG/canvas that doesn't honor `dir`, mixed Hebrew+Latin shelfmarks (e.g. `T-S 12.123` next to a Hebrew title) reorder wrongly, band names leak English under a Hebrew UI, or raw Hebrew literals bypass `tr()`. This is a recurring class in the codebase (SEED-014 a11y/RTL, the joins-lab i18n guard, "Hebrew RTL" convention).

**Why it happens:**
Graph libraries render text in `<canvas>`/`<svg>` where CSS `direction` doesn't apply, so bidi ordering must be handled manually. Band/label strings are added late and hardcoded. The web `tr()` reads the SAME `TRANSLATIONS` dict as desktop but renaming an EN key without a HE entry silently falls back to English.

**How to avoid:**
- Every user-facing string (band names, panel labels, atlas legend, leads-queue chrome) goes through `tr()` with BOTH EN + HE keys; add a **no-raw-Hebrew-outside-`tr()` guard test** for the new modules (mirror `tests/test_joins_lab_i18n.py`).
- For graph labels: apply explicit bidi handling (Unicode bidi marks / pre-ordering) for canvas/SVG text, and test with real Hebrew work titles + mixed shelfmarks.
- Manual `flex-row-reverse` for RTL layout of the connections panel / leads queue (NiceGUI/CSS RTL gotchas are documented); one-scroll-only, `flex:1 1 auto`/vh for heights.
- HE-mode RTL render-smoke test on each new surface.

**Warning signs:**
English band names under Hebrew UI; Hebrew titles rendering left-aligned/LTR in the graph; mixed shelfmark+title reordering; no HE key for a new EN string.

**Phase to address:** Every UI phase (connections panel, work pages, atlas, leads queue) + a shared i18n guard test.

---

### P13: The stratified precision certificate is skipped, rushed, or measured on the wrong estimand

**What goes wrong:**
The module's flagship promise is "the main band ships with a measured precision number" (one pre-registered ~200–250-card round). If this is skipped for schedule, measured post-hoc (peeking then choosing the favorable stratum), or measured on the wrong estimand, the entire epistemic value collapses — and the E1 history shows exactly how this goes wrong: R-CANON round 3 measured 0.647 and the gate correctly REFUSED certification; the Bible-only sub-stratum looked like 0.727 but "Bible-only was not the pre-registered estimand, so this certifies nothing." Certifying on a cherry-picked stratum is the specific trap.

**Why it happens:**
Pre-registration is annoying; a favorable-looking sub-slice is tempting; "we already know it's good" pressure. The blind/pre-registered discipline feels like bureaucracy until it's the only thing standing between you and an unsupported public claim.

**How to avoid:**
- Run the certificate as a **pre-registered, frozen-frame, blind** round using the existing E1/OC machinery and gold cards, on the ACTUAL shipped tier-A population — not a convenient sub-stratum. Freeze the estimand + gates BEFORE grading (the E1 protocol).
- If the gate fails, the band ships as **screening**, not certified (exactly what R-CANON did) — do not relabel to hit a date.
- Keep community/catalogue signals OUT of the acceptance evidence (memory rule: catalogue = recall yardstick, never acceptance evidence).

**Warning signs:**
Precision computed after seeing results; a sub-stratum chosen post-hoc; the estimand changing between freeze and report; schedule pressure to call a failing band "certified."

**Phase to address:** Precision-certificate phase (pre-registration + freeze, gated before the release phase).

---

### P14: Recall dishonesty — "no identification" read as "no such work exists"

**What goes wrong:**
Track-1 identification has real recall limits (physical joins share wording in only ~1% of groups; many same-work pairs share no passage BY CONSTRUCTION; short/noisy documentary hands under-recall). If the UI presents "no identification found" as authoritative, users conclude a fragment is unidentifiable or unique when the tool simply didn't catch it. For a scholarly audience this is a false-negative trust failure.

**Why it happens:**
Absence-of-evidence is silently rendered as evidence-of-absence; the empty state defaults to "nothing here."

**How to avoid:**
- Explicit **recall-honesty labeling** on every connections/work surface: "no identification shown ≠ none exists" (already in the milestone plan) — make it a required element of the empty state, not an afterthought.
- Frame the module as **discovery/leads**, not a complete index.

**Warning signs:**
An empty connections panel with no caveat; UI copy implying completeness; users reporting "the tool says my fragment is unique."

**Phase to address:** Connections-panel + work-pages phases (empty-state copy).

---

### P15: Browse-page integration adds latency/failure to the existing hot path

**What goes wrong:**
The "identified as ⟨work⟩ + related manuscripts" panel is added to browse pages — the existing hot path. If it fetches synchronously, adds an un-timed-out query, or fails hard, it degrades or breaks browse for everyone, and couples the flagship-but-new module to the most-trafficked existing page.

**Why it happens:**
The connections data is naturally shown inline on browse; the easy wiring is a direct call in the browse render path, inheriting none of the existing enrichment-timeout discipline.

**How to avoid:**
- Fetch connections **as bounded, timed-out enrichment off the event loop**, mirroring the existing PGP/FJMS/NLI browse-enrichment pattern (`SEARCH_API_BROWSE_TIMEOUT`, bounded executor) — a slow/absent sidecar must degrade the panel to empty, never stall or 500 the browse page.
- **Feature-flag** the panel (P7); fail-open.
- Cache per-MS connection lookups (bounded LRU) since browse revisits are common.

**Warning signs:**
Browse p95 rising after the panel lands; browse 500s traced to the discovery sidecar; no timeout on the connections fetch.

**Phase to address:** Connections-panel phase (enrichment-timeout + fail-open + cache).

---

### P16: Low-value / systematically-wrong canon leads surfaced with equal prominence

**What goes wrong:**
The R-CANON lane is Bible-heavy, and the domain lead's decision is that Bible identifications are "not discoveries" (low scholarly value). Worse, the Targum sub-lane measured 0.483 precision with a *systematic* confusion (Bible-page-claimed-as-Targum; verse-region overlap scores Targum claims high — reason code R3). If the leads queue surfaces R-CANON identifications with the same prominence as genuine discoveries, users wade through low-value Bible IDs and hit a known-wrong Targum class — eroding trust in the whole leads surface.

**Why it happens:**
The band's aggregate precision (0.647) hides the per-category split; a generic "screening leads" queue treats all R-CANON rows alike.

**How to avoid:**
- **Deprioritize or separately label** the canon lane; keep it screening-only (per the Hillel disposition), and do not promote Bible/Targum IDs as headline discoveries.
- If Targum IDs are shown at all, flag the known systematic confusion; the scholarly payoff is in rare-work quotation mining (the SZ playbook), not canon copies.

**Warning signs:**
Bible identifications topping the leads queue; Targum IDs shown with no caveat; a single undifferentiated "screening" bucket.

**Phase to address:** Leads-queue phase (per-category prioritization/labeling) + band-schema phase (carry claim-category, not just band).

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Ship `fullcorpus_v2.db` directly as the sidecar | No distillation work | Provenance leak (P1), size bloat (P6), schema drift; unrecallable once exported | **Never** — masked-first distillation is non-negotiable |
| Filter provenance at render time instead of at the sidecar boundary | Faster to prototype | One missed surface = leak; every new surface re-opens the risk | **Never** for masking — project rule is projection, not filter |
| Render the full graph client-side and filter in JS | Works in a 200-node demo | Melts browsers, blows WS/DOM budget at 52K/89%-blob (P4) | Only for a pre-bounded ≤300-node ego-network |
| Synchronous sidecar queries in page handlers | Simplest wiring | Blocks the shared event loop; prod-wide freeze (P5) | **Never** on the request path |
| Community votes bump band/ranking at runtime | Nice trust signal | Corrupts the certified precision contract (P9) | **Never** — votes feed future rounds only |
| Copy the existing corrections table sans GRANT | Familiar pattern | `supabase-js` permission failures; RLS holes (P8) | **Never** — new tables need explicit GRANTs post-2026-05-30 |
| Mutable (UPDATE-in-place) judgments | Simpler schema | No audit trail; can't feed certification; last-write-wins hides disagreement | Only a throwaway internal spike |
| Homepage graph loaded eagerly | One less route | CLS regression + first-load cost for all visitors (P11) | Only a static teaser image on `/` |
| Skip the render-smoke test | Faster CI | Headless pytest misses the async render path (documented) | **Never** for new NiceGUI surfaces |

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| NiceGUI event loop | Sync SQLite/graph query in an `async` handler | `run.io_bound` / executor + timeout + concurrency cap (mirror browse enrichment) |
| Browse page (hot path) | Direct connections fetch in render | Bounded, timed-out, fail-open enrichment worker; feature-flagged; LRU-cached |
| Supabase new tables | RLS + policies but no `GRANT` (old tables are a bad template) | Add explicit `GRANT`s for PostgREST roles + RLS + per-op policies (2026-05-30 rule) |
| Supabase writes | Raw `app.storage.user` / anon client | Route through `safe_storage` + authenticated `get_user_client()` (Phase 87 invariant) |
| Deploy | Push code before the large sidecar | scp sidecar FIRST → verify (`integrity_check`) → deploy code; feature-flag fail-open |
| SEO / JSON-LD | Emit raw work titles | Neutral allowlist titles only; denylist scan on meta/structured data (P1) |
| Graph renderer (canvas/SVG) | Rely on CSS `dir` for RTL labels | Manual bidi handling for Hebrew labels; RTL render-smoke test |
| Public API (if extended) | Optional/loose response fields | Allowlist-only Pydantic models (`extra='forbid'`); mandatory `band`; no `source`/`ref_*` |

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Full-graph render | Browser tab hangs/crashes; huge WS payload | Server-side ego-network bounding + ≤300–500 node cap; canon-masked aggregates | The 15,969-MS (89%) liturgical component on first real render |
| Unbounded neighborhood/CTE query | Rising p95 on `/browse` `/search` under atlas traffic | Precompute clusters/components in sidecar; index; depth/size bound; timeout | Any high-degree node in the giant component |
| Large memory-mapped sidecar | RSS toward `MemoryHigh`; OOM under crawl | Distill to a clean minimal schema; drop spans/ref-text/blob edges; measure resident footprint | Combined with Tantivy 5–6 GB + existing sidecars on the 15.4 GB no-swap box |
| Homepage graph eager-load | Slow first paint + CLS on `/` | Static teaser on `/`; lazy-load atlas JS on its route; reserve height | Every landing-page visit + crawler traffic |
| Per-request layout/component computation | CPU spikes on atlas requests | Precompute layout hints/weights/communities at build time | Concurrent atlas users |

## Security Mistakes

| Mistake | Risk | Prevention |
|---------|------|------------|
| Provenance leak via any surface (P1) | License/contract breach; unrecallable once exported/cached | Masked-first sidecar + leak-vector CI scan across titles/URLs/API/exports/SEO/logs (release blocker) |
| New Supabase table without GRANT/RLS holes (P8) | Data API failures; cross-user judgment tampering/read | Explicit GRANTs + RLS `WITH CHECK (auth.uid()=author_id)` + `/gsd-secure-phase` gate |
| Unsanitized community annotations | Stored XSS in NiceGUI render | Escape on render; length cap; free-text never rendered as HTML |
| No auth gate / rate limit on judgments | Spam/vandalism at corpus scale (P10) | Login-gated writes + per-user rate cap + moderation status/hide path |
| Community votes mutate certified bands (P9) | Gameable precision claims | Votes are a separate channel; bands frozen at ship |

## UX Pitfalls

| Pitfall | User Impact | Better Approach |
|---------|-------------|-----------------|
| Screening lead cited as fact (P2) | Scholar publishes a wrong ID; platform credibility hit | Band travels with the claim (copy/export/API); leads default-hidden + labeled uncertified |
| "Expert-verified" before independent audit (P3) | Overclaim on the flagship feature | Single-source `certification_status` flag; honest "audit pending" wording; no "certified" for R-A yet |
| Opaque band codes (R-A/R-B/R-CANON) shown to users | Confusion; no sense of strength | Self-explaining bilingual names + a precision number per certified band |
| Empty connections = "unique" (P14) | False-negative trust failure | Required recall-honesty caveat on every empty state |
| Full hairball atlas (P4) | Meaningless + browser crash | Bounded ego-networks + canon-masked cluster aggregates |
| Hebrew labels break in graph (P12) | Unreadable RTL text; wrong bidi order | `tr()` + manual bidi handling + RTL render-smoke |
| Low-value/wrong canon leads shown prominently (P16) | Wasted attention on Bible IDs; hits known-wrong Targum class | Deprioritize/separately label canon lane; keep screening-only |

## "Looks Done But Isn't" Checklist

- [ ] **Sidecar distillation:** Often missing the masked-first schema — verify a full row-value + schema scan finds zero denylist hits AND ref-text columns are absent, not just filtered.
- [ ] **Band labels:** Often missing on the detached claim — verify copy-to-clipboard, every export row, and every API object carry the band inseparably.
- [ ] **Atlas:** Often missing server-side bounding — verify no response exceeds the node/edge cap and no full-graph path exists.
- [ ] **Heavy queries:** Often missing the executor/timeout — verify no sync sidecar call sits in an `async` handler and every graph endpoint has a timeout + concurrency cap.
- [ ] **Supabase tables:** Often missing `GRANT`s — verify the migration has explicit grants for PostgREST roles (not just RLS/policies) and an anon/authed reachability smoke test.
- [ ] **Judgments:** Often missing the audit trail — verify judgments are append-only and disagreement is representable.
- [ ] **Homepage:** Often missing reserved layout height — verify CLS on `/` and a render-smoke test; graph JS is lazy-loaded.
- [ ] **Browse panel:** Often missing fail-open — verify browse still renders (empty panel) when the sidecar is absent/slow.
- [ ] **Precision certificate:** Often missing pre-registration — verify the estimand + gates were frozen before grading and match the shipped population.
- [ ] **Deploy:** Often missing the DB-first order + feature flag — verify the runbook scp's the sidecar first and the module fail-opens when it's absent.
- [ ] **i18n:** Often missing HE keys / bidi handling — verify the no-raw-Hebrew guard passes and Hebrew graph labels order correctly.

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Provenance leak shipped (P1) | HIGH | Pull the offending surface immediately; purge caches/SEO where possible (external archives may be unrecallable); rebuild sidecar masked; add the missed vector to the CI scan; post-mortem |
| Full graph shipped, browsers crash (P4) | MEDIUM | Hotfix a node/edge cap server-side; switch to ego-network responses; defer global atlas to a bounded/aggregated view |
| Event-loop block takes down prod (P5) | MEDIUM | Wrap the query in the executor + timeout (hotfix pattern exists from NLI-breaker/browse); add concurrency cap; feature-flag off if needed |
| Supabase GRANT missing (P8) | LOW | Run the GRANT migration; re-test `supabase-js` reachability |
| Community votes contaminated a band (P9) | MEDIUM | Recompute the band from the frozen E1 measurement only; sever the vote→band coupling; keep votes as a display-only separate channel |
| Screening lead cited as fact (P2) | HIGH (reputational) | Correct the record; strengthen inseparable band labeling; audit exports/API for un-banded claims |
| Sidecar bloat causes OOM (P6) | MEDIUM | Re-distill to minimal schema; move to on-disk (non-resident) SQLite; tune MALLOC/arena knobs already in prod |
| Precision certificate failed but shipped as certified (P13) | HIGH | Relabel to screening (the R-CANON precedent); re-run a pre-registered round on the correct estimand |

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| P1 Provenance leak | Data-layer (masked-first schema) + masking-hardening release gate | Leak-vector CI scan (titles/URLs/API/exports/SEO/logs) is green; sidecar has no ref-text/source columns |
| P2 Leads cited as facts | Band-schema + all UI/export/API phases | Every copied/exported/serialized claim includes its band; leads default-hidden + labeled |
| P3 Expert-verified overclaim | Band-schema + release (copy approval) | No surface emits "certified"/"מאומת" for a non-certified band; single `certification_status` flag |
| P4 Full-graph render | Atlas + connections phases; sidecar precompute | No response > node/edge cap; no full-graph path; canon-masked aggregates |
| P5 Event-loop block | Discovery-service (thin de-risk spine) | No sync sidecar call in async handler; every heavy endpoint has timeout + concurrency cap |
| P6 Sidecar bloat/drift | Data-layer / distillation | Sidecar size + resident footprint within budget; schema/row-count + integrity assertions |
| P7 Deploy order | Discovery-service (flag) + release (runbook) | Module fail-opens without sidecar; runbook scp's DB first |
| P8 Supabase GRANT/RLS/audit | Community-judgment | Migration has GRANTs; anon/authed reachability smoke; append-only history |
| P9 Votes pollute bands | Community-judgment + band-schema | Band/precision unchanged by votes; separate display channel |
| P10 Spam/vandalism/contradiction | Community-judgment | Auth gate + rate limit + moderation status; disagreement representable; annotations sanitized |
| P11 Homepage CLS | Homepage-promotion / release | CLS on `/` stable; graph JS lazy-loaded; reserved height; render-smoke |
| P12 i18n/RTL | Every UI phase + shared guard | No-raw-Hebrew guard green; Hebrew graph labels order correctly; HE render-smoke |
| P13 Precision certificate | Precision-certificate phase (pre-release) | Estimand + gates frozen pre-grading, matching shipped population |
| P14 Recall dishonesty | Connections + work-pages | Recall caveat present on every empty state |
| P15 Browse hot-path coupling | Connections-panel | Browse p95 stable; fail-open empty panel; timed-out enrichment; LRU cache |
| P16 Low-value canon leads | Leads-queue + band-schema | Canon lane deprioritized/labeled; claim-category carried alongside band |

## Sources

- `same_work_spike/probe/PROBE-RESULTS.md` — candidate recall 1.00; giant liturgical component; join≠parallel 1%; CER 16–20%; scale frontier (personal research artifact, HIGH)
- `same_work_spike/probe/METHOD.md` — Track-1 canon masking as the gate before the works census; M-source/Sefaria reference use (HIGH)
- `same_work_spike/probe/results/E1-ROUND2-RELEASE.md` — R-A 0.889 "single-expert, uncertified, valid-with-deviation, independent audit pending"; R-B 0.859 screening (HIGH)
- `same_work_spike/probe/results/E1-ROUND3-RELEASE.md` — R-CANON 0.647 → gate REFUSED → screening; Bible-only sub-stratum "certifies nothing"; Targum 0.483 systematic confusion (HIGH)
- `same_work_spike/probe/results/CODEX-BRIEF-atlas.md` — ego-network model decided; per-manuscript bounded neighborhood (HIGH)
- `supabase_setup.sql` — existing corrections/discoveries/*_votes tables have RLS + policies but ZERO GRANT statements; `status` enums exist (HIGH, direct inspection)
- `shared/corrections_service.py` — existing community-write service pattern (RLS + authenticated client) (HIGH)
- `CLAUDE.md` — 2026-05-30 GRANT requirement; scp-DB-first deploy rule; CLS 2026-06-15 fix; NLI circuit breaker / 2026-05-25 hang; `safe_storage` Phase 87 invariant; 15.4 GB box + MALLOC/allocator-ratchet notes; feature-flag pattern (HIGH)
- `MEMORY.md` — "Catalogue = recall yardstick, NEVER acceptance evidence"; NiceGUI render-smoke gap; deploy-DB-sync incident; web-memory reattribution; approve-release-texts rule (HIGH)
- `.planning/PROJECT.md` — v9.0.0 milestone scope, bands, "expert-verified until audit passes", recall-honesty labeling (HIGH)

---
*Pitfalls research for: v9.0.0 Discovery — same-work identification + connection atlas (web-only), added to the existing GenizahSearch platform*
*Researched: 2026-07-19*
