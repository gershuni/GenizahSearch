# SPEC — Rebuild `scripts/serve_v3_review.py` as a visual clone of `/computed-identifications`

**Target file:** `C:/Genizahsearch/scripts/serve_v3_review.py` (single file, stdlib only — `http.server` + `sqlite3`; a teammate needs the DB, this file, and Python)
**Data:** `C:/Genizahsearch/discovery_data/discovery-v3-REVIEW.db` (1.4 GB, `meta.audience=private`), grades in `discovery-v3-REVIEW.db.grades.db` (ATTACHed, currently 0 rows)
**Shape of the data, measured:** 254,612 rows = page-evidence grain; 110,110 identifications; 50,992 manuscripts; 1,269 works; 61 domains under 27 parents. 69,723 identifications are a single row.

---

## 0. THE ONE-LINE RULE

Copy the public page's **skeleton, tokens, class names and row anatomy**. Replace every **reader-facing claim sentence** with a grading instruction. Add **two axes the public DB does not carry** (relation, span rank) and **one pane** (source vs manuscript). Nothing that asserts a result to a reader survives the port.

---

## 1. LAYOUT

### 1.1 Stylesheet: copy, don't reinvent

Inline into the `PAGE` string, verbatim, in this order:

1. The **token blocks** from `web/static/common.css`: the light `:root` set (`--primary-600:#059669`, `--primary-700:#047857`, `--bg-secondary:#f8fafc`, `--bg-tertiary:#f1f5f9`, `--bg-active:#ecfdf5`, `--text-primary:#1e293b`, `--text-secondary:#475569`, `--border-light:#e2e8f0`, `--border-medium:#cbd5e1`, `--accent-gold:#d4a574`, `--accent-amber:#f59e0b`) and the `[data-theme="dark"]` override block. **Light is the default** — that is what makes it read as a clone. Ship a one-click dark toggle (the current tool is dark and reviewers sit in it for hours) writing `data-theme` on `<html>`; use the existing dark token values, invent none.
2. The **discovery CSS block**, `common.css` lines 1593–1957, copied as-is including its `.gs-discovery` scope. Keep the sketch class names exactly (`.row .chip .rel .nov .fg .fchip .dnote .needs .caveat .dnode .here`) so the two surfaces stay comparable and the reference docs stay accurate.
3. Private-only additions (§3, §4), each in its own commented block at the end.

**The root element MUST be `<div class="gs-discovery gs-findings …">`.** Every rule above is scoped under it; without the class nothing applies.

**Direction:** the document is `dir="ltr"` — this tool has an English-only UI. Direction is set **locally on data only**: work titles, catalogue titles and the two text panes. Do not mirror the layout. (The copied CSS uses logical properties throughout, so it is correct either way; the `[dir="rtl"]` mirror rules simply never fire.)

### 1.2 Regions — copy / drop / add

| Public region | Verdict | Notes |
|---|---|---|
| Page root: one centred column, `max-width ~80rem`, `p-4`, `gap-4` | **COPY** | `<div class="gs-discovery gs-findings">` |
| Header block (`.phead`), `gap-2` column | **COPY the slot, replace the copy** | see §1.3 |
| H1 title | **COPY, retitle** | `v3 review — computed identifications (private)` |
| Permanent caveat plate (`.caveat`, gold `border-inline-start`, `info` icon top-aligned) | **COPY the element, replace the text** | §1.3 |
| Beta line | **DROP** | marketing |
| Launch headline / stats band | **DROP entirely** | §5.1 |
| "How to read this page" collapsed `<details>` | **COPY** — this is the tool's most valuable public borrowing | §1.4 |
| Mode strip (`All findings` / `Screening leads` / `My saved` + "Coming soon" pills) | **DROP** | §5.4 |
| Two-column body: sidebar `flex:1 1 280px;min-width:240px` + results `flex:999 1 420px;min-width:0`, plain flex-wrap div, no breakpoint | **COPY exactly** | this is the whole layout; do not substitute a grid |
| Sidebar `.fg` cards (white card, `p-4 gap-2`, uppercase `.gs-findings-card-header`) | **COPY the card shape**, new control set | §4 |
| Domain facet tree (parent button + separate chevron, indented leaves, `max-height:340px;overflow-y:auto`) | **COPY exactly** | §4.5 |
| Result bar (`.rbar`, two wrapping rows) | **COPY**, private counts | §4.10 |
| Active-filter chip bar (pill + round ✕ + red "Clear All") | **COPY** | now *includes* a pool chip — §4.11 |
| Second-pool invitation strip | **DROP** | §5.5 |
| Rows container (`.rows`, `gap-2`, hairline `border-block-end:1px solid var(--border-light)` on every row identically) | **COPY** | §2 |
| Empty state (`search_off` icon, `No results found`) | **COPY the shape**, drop the pool invitation inside it | replace with "Loosen a filter — the counts beside each control show what is reachable." |
| Outage state | **REPLACE** with a SQLite-error state: the exception class, the query name, and a "Retry" button. Never an empty list. |
| Pager | **COPY**, plus a page-size select |
| Report `mailto:` link | **DROP** | §5.9 |
| Admin ✕ suppression | **DROP** | §5.10 |
| Bilingual EN/HE UI, `tr()`, RTL mirroring | **DROP** | §5.11 |

**ADD, private-only:**

- A slim **sticky top bar** (the current tool's `header`, kept): shelfmark/sys_id search, `Reset`, `Export grades`, dark toggle, and the live row count. Everything else moves into the sidebar. Sticky because grading is a scrolling job; the sidebar scrolls with the page (do not make it sticky — it is 9 cards tall).
- Per row: **two pane toggles**, **two panes**, **a grade bar** (§2, §3).
- A **session strip** under the header when any grade exists: `{n} graded this session · {m} graded in total · export`.

### 1.3 The caveat slot — same element, different job

Public copy addresses a reader ("read each row as a lead to check"). Replace with the grading instruction, in the same `.caveat` plate:

> **You are grading `divergence_correctness`, and only a person can.** When our identification and the catalogue disagree, which is right. The model scored 8/28 on this question — at or below chance for three options — while the owner scored 31/32, so it was removed from the model's job and the column is empty by design in every artifact. Your answers write to `<db>.grades.db` immediately and survive a rebuild of the review projection.

### 1.4 "What these columns mean" — the collapsed panel

Keep the public `<details>` treatment **and the CSS rule that bumps `.dnote` inside it from 11px to 14px/1.55** — this is prose a reader deliberately opened. Content: the existing help body of the current tool, sourced verbatim from the DB's own `meta` rows (`doc.router_verdict`, `doc.claim_type`, `doc.main_pool`, `doc.routing_status`, `doc.novelty_status`, `doc.divergence_correctness`, `doc.ms_match_vs_ref_match`, `doc.known_weakness`) — **read them from `meta` at request time, do not retype them into the HTML.** If the artifact's documentation changes, the tool must change with it.

Keep the two `.warn` / `.ok` inline-start rules already in the tool (gold / green), which are the same treatment as `.caveat`.

---

## 2. THE ROW

One `<div class="row gs-findings-row w-full gap-1 p-2" style="border-block-end:1px solid var(--border-light)">` per `review_row`. Public order preserved: **title → author → shelfmark line → meta line → affordances**.

### 2.1 Identity lines

| Element | Column | Rule |
|---|---|---|
| Title (`font-bold`) | `work_title` | Verbatim, `dir="auto"`, `unicode-bidi:isolate`. **Never machine-translated** — discovery work titles are Hebrew-only. Falls back to `work_id` when empty. |
| Work-id suffix | `work_id` | Small monospace `.chip` **always shown**. Private-only addition, and it is load-bearing: 43 titles are shared by more than one `work_id`, so a title alone does not name a work. |
| Author line | `work_author` | Rendered only when non-NULL (72.7% are NULL). **Never "Unknown author"** — that is a claim about the work. |
| Shelfmark line | `library_code` + `shelfmark` | `library_code` as `.chip`; `shelfmark` as a link to the **full** live browse URL (not `embed=1`) with `page` + `volume_ie`. Both are NULL on 14,349 rows (5.6%) → fall back to `sys_id` in a monospace chip; never blank. |
| Catalogued as | `catalogue_title` | Public treatment exactly: label `Catalogued as:` in the page's language, title **verbatim, one language, never translated**, `dir="auto"`, `unicode-bidi:isolate`, two separate elements never one concatenated string. Absent on 19,786 rows (7.8%) → render nothing at all, not an empty line. |

### 2.2 The meta line — every chip named

`<div class="gs-findings-row-meta side items-center gap-2 flex-wrap">`, in this order:

| # | Chip | Class | Source | Label | Tooltip |
|---|---|---|---|---|---|
| 1 | **Relation** | `.rel` (neutral, **no per-kind colour**) | `router_verdict` | `Witness` / `Quotation` / `Held back by the router` / `Shared text` | `meta['doc.router_verdict']` |
| 2 | **Span rank** | `.chip` | `claim_type` | `Largest span on page` / `Smaller span on page` | `meta['doc.claim_type']` |
| 3 | **Disagreement** | `.needs` (amber pill — the public page's own "not available yet" treatment, no new visual language) | derived | `span rank disagrees with the router` | `45,149 rows are in this state` |
| 4 | **Novelty** | `.nov`, or `.nov unknown` for `not_checked` | `novelty_status` | the raw shade name | `meta['doc.novelty_status']` |
| 5 | **Pool** | `.chip` | `main_pool` | `main pool` / `more matches` / `no identification record` | `meta['doc.main_pool']` |
| 6 | **Demotion reason** | `.dnote text-xs`, visible text (not a tooltip) | `main_pool_reason`, **only when `main_pool = 0`** | `missing_signal` / `low_coverage` / `shared_wording` / `overlapping_tie` / `insufficient_length` | — |
| 7 | **Routing** | `.needs` | `routing_status`, **only when ≠ `shipped`** | `review only — not shown on the site` | `meta['doc.routing_status']` |
| 8 | **Evidence** | `.dnote text-xs` | `matched_letters`, `n_spans`, `coverage_ppm` | `{n} letters` · `{n} spans` (only when >1) · `covers {p}% of this page's letters` | — |
| 9 | **Corpus** | `.chip` | `source_corpus` | **through the redaction map** — `general` / `Judeo-Arabic` / `<codename>` | — |
| 10 | **Band** | `.chip` | `confidence_band`, **only when ≠ `tier_a`** | the raw band name | — |
| 11 | **Prior review** | `.chip` | `adjudication_status`, **only when ≠ `unreviewed`** | `provisional` / `human confirmed (earlier pass)` | `The earlier adjudication pass. Grades entered here do not update it.` |
| 12 | **Short-match prompt** | `.dnote text-xs` | derived, `matched_letters < 150` | `short match — check whether this rests on shared scripture` | `meta['doc.known_weakness']` |
| 13 | **Graded** | `.nov` | grades sidecar | `graded: {value}` | — |

**Chips 3, 6, 7, 10, 11, 12, 13 are conditional.** Chips 1, 2, 4, 5, 8, 9 always render — they have no absent state on this grain.

**Chip 1 stays visually neutral** (public hard rule 2). It is a *kind*, not a quality, and the moment it is colour-coded the row acquires a confidence treatment the grader will read before reading the text. The only non-uniform mark on the row is chip 3, which flags a **data condition** between two columns, not a verdict.

Derivations, exactly:
- chip 3 fires iff `router_verdict='parallel' AND claim_type='direct_witness'`.
- chip 8's percentage is `coverage_ppm / 10000` to 1 dp. `coverage_status` is `measured` on all 254,612 rows, so no gating is needed; keep the qualifier `of this page's letters` attached — a bare percentage on a discovery surface is what the qualifier exists to prevent.

### 2.3 Affordances

At the end of the meta line, two buttons side by side, both `flat dense size=sm no-caps` with class `gs-findings-row-preview-toggle` (so the copied `@media (max-width:700px)` rule gives them `width:100%`):

1. `Hide the texts` ⇄ `Compare the texts`
2. `Preview the manuscript` ⇄ `Close` — public wording verbatim

Then, in DOM order: **text pane → folio pane → grade bar.** (Both panes before the grade bar: the grade is the conclusion, and it must sit under the evidence it concludes from.)

### 2.4 Grade bar

Unchanged from the current tool, restyled to `.fchip`: `Which is right?` + three buttons `catalogue correct` / `claim correct` / `unclear` + `clear`. Selected gets `.fchip.here` (public `aria-pressed=true` + `--bg-active` + `font-weight:700`), not the current blue.

**Add a free-text note field** beside them, debounced, POSTing to the same endpoint. The `note` column already exists in `human_grade` and is unused; a grader's reason is worth more than the three-way choice alone.

---

## 3. THE TWO PANES

**Two independent panes, two buttons, two regions. Both may be open at once. Neither closes the other.**

The reasoning is not symmetry: the text pane is the grading instrument (free — its content is already in the row payload) and the folio pane is the confirmation step (expensive — one live-site session per open). Sharing one region would hide the manuscript text at exactly the moment the reviewer is comparing the image against it.

### 3.1 Pane A — source vs manuscript text (**default OPEN**)

The owner asked for "an opened pane". Default `display:grid`; the toggle collapses it so a reviewer scanning a filtered list can compact rows.

**Keep from the current tool, unchanged:**

- `.cols { display:grid; grid-template-columns:1fr 1fr; gap:12px }` with `@media(max-width:900px){grid-template-columns:1fr}` — the same 900px breakpoint the public panel's `.dpanes` uses.
- Per pane: `<h4>` header, then `.txt` box — `direction:rtl; text-align:right; white-space:pre-wrap; font-size:15px; line-height:1.9; max-height:320px; overflow:auto`. Each pane scrolls inside itself; the page never scrolls horizontally.
- `before` in `.ctx` (dimmed) + `match` in `<mark>` + `after` in `.ctx`. Columns: `ms_before/ms_match/ms_after` and `ref_before/ref_match/ref_after`; `meta.context_chars = 320`.
- The `[unspaced letter stream]` marker when `ref_is_stream = 1` (188 rows).
- **The escaping function verbatim, including the quote escape.** Hebrew titles carry gershayim as a plain `"`, and these values are interpolated into HTML attributes; escaping only `&` and `<` silently truncated every such title. Keep the `?? ""` coercion too — `main_pool` arrives from SQLite as integer `1` and `0` is a real value.

**Change:**

- Headers become `Manuscript` (left) and `Reference edition — {source_corpus through the redaction map}` (right). Do not mirror the panes; the content is RTL, the layout is not.
- Restyle to the copied tokens: `.txt` background `var(--bg-tertiary)`, border `var(--border-light)`, `<mark>` background `var(--accent-gold)` with `color:#1a1a1a` (the token is theme-invariant, so a theme-following foreground would go white-on-gold in dark and fail AA).
- Add one `.dnote` line under the two panes, read from `meta['doc.ms_match_vs_ref_match']`: the two sides will **not** match closely — ~0.4 apart per character is what a witness looks like, not an error. This is the single most likely misreading and it belongs beside the thing being misread, not only in the help panel.
- Add a small copy-to-clipboard button per pane (a grader pastes into a catalogue search).

### 3.2 Pane B — the folio preview (**default CLOSED, lazy, capped at one**)

Keep the current tool's implementation and its three correct decisions:

- **Absolutize.** `browse_url()` in the app returns a *relative* `/browse?…` which on `127.0.0.1` resolves to a 404. The tool must build `{SITE}/browse?…`. Make `SITE` a **CLI flag** (`--site`, default `https://genizahsearch.com`) — see §6.1.
- **`page` and `volume_ie` travel together or not at all.** Gate on `page_num > 0 AND volume_ie` (both are non-NULL on all 254,612 rows here, but keep the gate). Folios are numbered per volume; a half address looks targeted and lands somewhere else.
- **`embed=1`.** The bare viewer skips snapshot persist *and* restore, so previewing here cannot overwrite wherever the reviewer left `/browse` in their own tab.
- **Build on click; destroy on close** (`box.innerHTML = ""`). The public page does *not* destroy — it latches `loaded` and leaves N live iframes and N websockets connected. Private must destroy. See §6.1.

**Add:**

- **At most one folio pane open across the whole page.** Opening row B closes row A. Every open mints a new server-side session on production (§6.1); uncapped, a day of grading ratchets prod memory and inodes.
- `title="Live manuscript viewer"` and `sandbox="allow-scripts allow-same-origin"` on the iframe. Document that this sandbox pair is close to no sandbox for a frame same-origin to itself — it is there to *deny* forms/popups/downloads, not as a security control.
- The existing bar above the frame: `live genizahsearch.com — folio {n}` + **`open in a tab ↗`**. Treat the tab link as the primary path and the frame as convenience.
- A **paint watchdog**: 8 s after open, if the reader has not interacted, show an inline `.needs` note — *the embedded viewer did not paint — open it in a tab*. The parent cannot read `contentDocument` and the `load` event fires on the spinner shell, so this timer is the only available signal.

---

## 4. THE FILTERS

Sidebar cards, top to bottom, in the public `.fg` card shape. **Card 1 carries the `order:-1` rule** (the public stylesheet pins `.fg.novgrp` first; rename the marker class to `.fg.relgrp` and repoint the rule) — on this tool the headline axis is the relation, not novelty.

**Counts:** every control shows counts, and **every facet is computed with its own axis excluded** — keep `_where(exclude=…)`. A facet computed with its own selection applied contains exactly one option, which forces the reader back to "all" before switching. This already works; do not lose it.

Counts are shown on **every** control, including pool and novelty where the public page deliberately shows none. That divergence is correct: on a public page a count beside a claim reads as a claim's size; on a grading tool it reads as workload.

| # | Card header | Control | Values (counts as measured) | Counts? |
|---|---|---|---|---|
| **1** | **Relation — is this page a copy of the work, or does it quote it?** | 4 checkboxes, multi-select, **all checked by default** | `same_work` → **Witness — this page is a copy of the work** (151,217) · `parallel` → **Quotation — this page quotes the work** (83,120) · `not_shipped` → **Held back by the router** (18,780) · `shared_text` → **Shared text** (1,495) | yes |
| | *Card prose* | | "The only witness-vs-quotation axis that was validated — ~1,400 blind cards plus 400 more, graded by hand. Decided by how much of the page the match covers." | |
| **2** | **Span rank — NOT a relation** | radio, 3 positions | **Any** (default) · **Largest span on page** (195,525) · **Smaller span on page** (59,087) | yes |
| | | **plus a one-click button:** `Only rows where span rank disagrees with the router` | 55,738 rows | yes |
| | *Card prose* | | "Says only which matched span is largest on this page. No minimum length, never reads the text, and a page with a single match gets 'largest' by default however short. Shown so you can see where it disagrees with the router." | |
| **3** | **Pool** | segmented, **3 positions + All**, multi-select | **main pool** (146,572) · **more matches** (43,634) · **no identification record** (64,406) | yes |
| | | nested select, appears **only** when `more matches` is the sole selection: **Why it was demoted** | `missing_signal` 16,654 · `low_coverage` 10,467 · `shared_wording` 8,691 · `overlapping_tie` 5,948 · `insufficient_length` 1,874 | yes |
| | *Card prose* | | "'More matches' means the evidence did not meet the rule — not that the identification is wrong. 'No identification record' is a third state: the rule was never evaluated. `shared_wording` and `insufficient_length` are the population the known-weakness note tells you to distrust." | |
| **4** | **What this adds to the catalogue** | chip list, multi-select | `not_checked` 65,628 · `confirms` 64,665 · `refines_granularity` 46,476 · `diverges_work` 39,866 · `fills_gap` 16,139 · `container_predicts` 12,391 · `aid_more_specific` 8,262 · `diverges_part` 1,165 · **other** (`alias_merge` 13 + `extends` 7 = 20, expandable) | yes |
| | *Card prose* | | "`not_checked` is an honest 'no answer', never a guess — and 64,406 of its 65,628 rows are the never-evaluated block, i.e. the rule never ran." | |
| **5** | **Domain of the identified work** | **two-level tree, public shape exactly** | 27 parents / 61 leaves, all of the form `Parent / Leaf`, + a `no domain recorded` node (737) | yes |
| **6** | **Author** | searchable single-select | 108 raw values → **normalised groups**; explicit `no author recorded` option (185,143 = 72.7%) | yes |
| **7** | **Work** | searchable single-select, **keyed on `work_id`**, displaying `work_title` | 1,269 | yes |
| **8** | **Reference corpus** | 3 checkboxes | general 197,763 · Judeo-Arabic 24,094 · `<codename>` 32,755 — **labels through the redaction map, never raw** | yes |
| **9** | **Escape hatches** (one card, three compact controls, all off by default) | checkbox · checkbox · radio | **Only non-tier-A evidence bands** (16,105) · **Only rows a person already looked at** (`provisional` + `human_confirmed` = 15,038) · **Grading: any / ungraded only / graded only** | yes |

### 4.1–4.9 notes an implementer must not skip

**Card 1 is the witness-vs-quotation axis, and it is the only one.** It reads `router_verdict`. Its labels say *witness* and *quotes* in full sentences precisely so it cannot be confused with card 2. Card 2's labels contain neither word, in either direction.

**`routing_status` gets NO control.** It is 100% redundant with `router_verdict` (`shipped ⟺ same_work`, four non-empty cells, zero exceptions). A separate control would let a reader build `shipped AND parallel` and see 0 rows with no explanation. It survives only as row chip 7 and one line in the help panel.

**`relation_kind` gets NO control.** Different vocabulary from `router_verdict`, superseded, and NULL on exactly the 64,406 never-evaluated rows — a control on it would silently drop a quarter of the DB. Surface it, if at all, as a line inside the row's help tooltip labelled *earlier relation verdict (superseded)*.

**`divergence_correctness` gets NO filter.** 100% NULL. It is *written*, not read.

**Card 5, parent selection is a strict superset:** `domain = ? OR domain LIKE ? || ' / %'`. All 61 non-null values carry ` / `, so the parent split is uniform. Leaf buttons show the tail name only with the full path on the tooltip; the branch starts collapsed unless the selection is the parent or one of its leaves; clicking a selected node deselects it; container is `max-height:340px;overflow-y:auto`. Chevron is a **separate round button** using a vertical glyph (`expand_more`/`expand_less`) so nothing flips for RTL.

**Card 6 must normalise before faceting.** There are 3 collision groups covering 30,501 rows where one person appears as 2–3 filter entries — one apostrophe glyph difference (U+05F4 vs ASCII `"`) and one spelling variant. The largest group splits one author across 574 / 3,486 / 12,674, so picking one entry hides 82% of that author's rows. Implementation: build a normalisation key (NFC → map U+05F4/U+2033/`''` to `"` → collapse whitespace), `GROUP BY` the key, display the most frequent surface form, and filter with `work_author IN (<every surface form in the group>)`.

**Card 7 must key on `work_id`, never `work_title`.** 1,269 work_ids map to 1,062 titles; 43 titles are shared. Keep the current tool's `label · <id>` disambiguation for duplicate labels, and keep its label→value map (a datalist's value is the label). Keep the **no silent cap** rule — an earlier version stopped at 400 of 1,269 works with nothing saying so.

**Card 9's grading radio is the reviewer's work queue**; its counts change as grading proceeds, so they must be recomputed per request, not cached with the rest.

### 4.10 Result bar

`<div class="rbar gs-findings-rbar w-full gap-2">`, two wrapping rows.

Row 1: `Showing {shown} of {total} rows` — the **real pre-LIMIT total, exact, never `len(items)` and never capped** (§5.12); `{n} identifications` beside it (`COUNT(DISTINCT sys_id||'|'||work_id)` under the same predicate — the grain the pool describes, and 254,612 rows collapse into 110,110); `{n} of these are graded`.

Row 2: **Sort by** and **Page size**.

**Sort by** — private columns, replacing the public sort vocabulary:
- `Work, then manuscript` (default — `ORDER BY work_id, sys_id`; keeps an identification's pages adjacent, which is how a grader reads them)
- `Most matched letters first`
- `Highest page coverage first`
- `Fewest matched pages first` (surfaces the 69,723 single-row identifications, the population the multi-folio signal cannot speak for)

**Page size:** 25 (default — the text pane is open by default) / 50 / 100.

**"Show as" (row unit) is DROPPED.** The private DB is one row per page-evidence with no grouped projection, and grading is per `evidence_id`. Do not build it.

### 4.11 Active-filter chip bar

Public shape exactly: pill with `border-radius:999px`, tiny round ✕ with `aria-label="Remove"`, then a red flat `Clear All`. One chip per active axis, in sidebar order.

**Unlike the public page, the pool DOES get a chip.** The public control has no neutral state, so a removable chip would promise one; the private control has three states plus All, so it does. Same for relation, span rank, corpus and the escape hatches. Clearing any axis resets to page 1.

---

## 5. WHAT MUST NOT BE COPIED

1. **The launch headline / stats band, in full** — `render_launch_headline`, `_render_launch_v1/v2/v3`, `_stat_card`, the shade cards, the scope line. Its centrepiece is a contribution **claim** — *"{n} identifications not found in the catalogues we checked"* — printed at `text-xl bold` on a page whose entire purpose is testing whether such claims hold. A tool that opens with the conclusion is a tool that has pre-registered its answer.
2. **The public caveat copy.** Keep the plate, replace the words (§1.3). The public text tells a reader how to read a result; a grader is not reading a result, they are producing one.
3. **The beta line** — *"This is a beta and it will grow"*. Product roadmap.
4. **The mode strip.** `Screening leads` and `My saved` are public promises about Phases 137–138, rendered visible-but-disabled with "Coming soon" pills. There is nothing to promise here.
5. **The second-pool invitation strip.** Persuasion copy, and its body asserts *"They are the same works and the same kinds of match"* — a claim about the second pool that this tool exists to test. The three-state pool control replaces it.
6. **The public relation vocabulary** — `Direct match` / `Partial match` / `Shared text`, i.e. `shared/discovery_display_strings.relation_chip`. The public page is *forbidden* from saying witness / copy of / quotes (a greppable honesty gate, and a negated use still violates it). This tool must say exactly those words, because that is the axis being graded. **Do not import the shared display-strings module, and do not let the private wording leak back into anything under `web/`.**
7. **The novelty badge gating.** Public renders a badge only for the candidacy shade and `not_checked`, and nothing at all for the other eight — because there is no ratified reader wording for them. Private renders all ten raw shade names: the grader is grading the gate, and hiding eight of its outputs makes it ungradeable.
8. **The "no bucket name on the row" ruling.** Public removed it because it was measurably *constant* across every row. Here it has three states, one of which (`no identification record`, 64,406 rows) is the never-evaluated block. It earns its place.
9. **`report_mailto`** and the `REPORT_ADDRESS` constant. The grade buttons are the channel; a mailto from an internal tool is a second, unreconcilable one.
10. **The admin ✕ / `on_suppress` hook.** No Supabase, no admin identity, and suppression from a grading tool would change what the artifact ships with no record of who or why.
11. **Bilingual EN/HE UI**, `tr()`, and document-level RTL mirroring. English UI, Hebrew data. `dir="auto"` on data only.
12. **`DISCOVERY_FINDINGS_COUNT_MAX` and the approximate-total escape.** A grading tool's totals must be exact; a capped total reported as exact is a correctness defect, not a tuning choice. `COUNT(*)` over the slim `facet_row` is fast enough.
13. **NiceGUI, Quasar, `safe_storage`, PostHog, analytics, `page_meta`, JSON-LD.** Stdlib only. The `q-expansion-item`, `q-select` and `q-card` markup must be reimplemented as plain `<details>`, `<select>`/`<input list>` and `<div class="fg">` carrying the same classes — the copied CSS keys on the class names, not on Quasar.
14. **Any raw render of `source_corpus`, or any real name for the masked corpus**, anywhere: chip, facet label, tooltip, pane header, export, or page `<title>`.
15. **Per-tier / per-confidence row styling** (public D-24). It still holds here, for a different reason: a grading tool that visually pre-judges rows biases the grader before they read the text. Uniform row treatment; the only non-uniform mark is the amber disagreement badge, which flags a two-column data condition, not a quality.

---

## 6. RISKS

### 6.1 The live-site iframe from localhost — measured, and it *works*, with five consequences

**Framing is not blocked.** Live headers on `/browse?...&embed=1` carry no `X-Frame-Options` and no `Content-Security-Policy`, and the repo has no XFO/CSP middleware anywhere. The obvious blocker is absent. What actually bites:

| # | Failure | Mitigation (all required) |
|---|---|---|
| a | **Relative URL.** `browse_url()` returns `/browse?…`, which on `127.0.0.1:8777` resolves to a local 404. | Absolutize against `SITE`. Already done — keep it, and expose it as `--site`. |
| b | **Third-party session cookie is dead.** `ui.run(storage_secret=…)` passes no `session_middleware_kwargs`, so Starlette's `same_site='lax'` applies; a Lax cookie is neither sent nor accepted cross-site (confirmed — two curls, two session ids). **Every open mints a new server-side session**: a new `.nicegui/storage-user-<uuid>.json` on the production box *and* a permanent entry in `core.app.storage._users`, cleared only at shutdown. `GENIZAH_STORAGE_RETENTION_DAYS=90` sweeps the files at startup but never the in-process dict. | **Cap at one open frame page-wide**, and **destroy on close**. Add `--no-preview` to disable the frame entirely (link-only) for long grading sessions. |
| c | **The frame renders in Hebrew/RTL and light theme** regardless of the reviewer's settings — `_resolve_ui_language()` reads empty storage and falls through to `'he'`. | Document it in the frame's own bar: *"the embedded viewer opens in Hebrew — it has no access to your site settings."* Do not try to force a language; there is no parameter for it. |
| d | **`sessionStorage` is unguarded.** `nicegui.js` does a bare top-level `sessionStorage.__nicegui_tab_id = …` with no try/catch. Chrome and Firefox partition third-party sessionStorage and it works; **Safari ITP — and any "block all cookies" setting — throws `SecurityError`, killing the client script at evaluation** and leaving the spinner forever with nothing surfaced to the parent. Highest-risk failure, and browser-dependent. | The 8 s paint watchdog (§3.2) and the always-present `open in a tab ↗` link. **Smoke-test in Safari, or in Chrome with third-party cookies blocked** — a Chrome-only test cannot see this. |
| e | **Telemetry pollution.** The embed keeps `ANALYTICS_SCRIPT` / `POSTHOG_SCRIPT` and a server-side `posthog_capture('browse_manuscript', …)` per load, and `referrerpolicy="no-referrer"` makes review traffic indistinguishable from real traffic in production PostHog. | The one-frame cap bounds it. Note it in the release notes for whoever reads the `browse_manuscript` numbers. |

**Also:** no same-origin JS in either direction. The parent cannot read `contentDocument`, cannot measure the browse page to size the frame, and cannot distinguish "loaded" from "spinner forever" (the `load` event fires on the 32 KB spinner shell). The browse page has no `postMessage` listener and no frame-busting JS. So the frame is locked to a fixed height (`62vh`, as today) and there is no workaround short of adding a postMessage channel to `browse.py` — **out of scope; do not add one for a private tool.**

**The best mitigation is `--site`.** A reviewer already running the web app locally (`python -m web.main`) can pass `--site http://127.0.0.1:8080`: same code path, first-party cookie, their own language and theme, and none of the prod-session, memory or telemetry cost. Print the active `SITE` at startup beside the DB path.

### 6.1a A folio pane that needs no session at all (measured addition)

Every failure in 6.1 comes from embedding a *page* that wants a session. The
folio IMAGE does not: `GET {SITE}/api/nli_image_by_sysid/{sys_id}?page={N}`
returns `200 image/jpeg` with no cookie, no session and no client script
(verified 2026-08-09 against the live site, from a cold client). A plain
`<img>` therefore has none of 6.1's five consequences -- no minted session, no
`storage-user` file, no Hebrew-only surprise, no Safari `SecurityError`, no
PostHog pollution -- and it works for a teammate who is not running the app.

It is strictly less capable: one image, no folio navigation, no transcription,
no metadata. So it is not a replacement for Pane B; it is the DEFAULT for it.

Recommended: three preview modes on one control, `--preview {image,frame,off}`,
default `image`.

  image  <img src="{SITE}/api/nli_image_by_sysid/{sys_id}?page={page_num}">
         plus the existing `open in a tab` link. No cap needed, no watchdog
         needed, no destroy-on-close needed -- an <img> holds no session.
         NOTE the endpoint is NLI-routed; Oxford manuscripts 404 on it
         (measured). Fall back to the frame, or to the tab link, on error --
         `<img onerror>` is a reliable signal, which is exactly what the
         iframe could not give us.
  frame  today's iframe, with every mitigation in 6.1 (cap at one, destroy on
         close, 8s watchdog). The right choice with `--site http://127.0.0.1:8080`.
  off    link only.

### 6.2 Masking

`source_corpus` labels and any work from the masked corpus are the one place a slip is possible. Three rules:

- Every corpus value goes through the redaction map — chip, facet label, tooltip, pane header, page `<title>`.
- **The export must be narrowed.** It currently emits `work_title` alongside the grade. The export is a file a teammate forwards. Restrict it to `evidence_id, sys_id, work_id, divergence_correctness, note, graded_at` — identifiers only, no titles, no authors, no catalogue titles, no corpus.
- No rendered HTML from this tool is ever committed. `scripts/check_atlas_masking.py --scan-repo` checks HEAD, not history; the tool's output belongs in `.gitignore` alongside the rest of `discovery_data/`.

### 6.3 Performance and the facet table

`review_row` carries ~6 KB of both-sides text per row (1.4 GB); `facet_row` is the slim projection (~40 MB). **Every filter, count and facet runs against `facet_row`; only the 25-row page body reads `review_row`.** This already holds — do not regress it. A facets response slow enough for the browser to cancel leaves every control empty with nothing saying why.

The new controls need columns `facet_row` does not carry: **`source_corpus`, `confidence_band`, `adjudication_status`, `main_pool_reason`, `matched_letters`, `coverage_ppm`, `n_spans`**. Extend `FACET_COLS` and add indexes for the four filterable ones. `ensure_facet_table` already drops and rebuilds when `set(FACET_COLS) <= cols` fails — **keep that check**; it is what auto-migrates a teammate's on-disk copy, and it is what previously caught a stale `facet_row` missing `router_verdict` that a row-count check had called current.

### 6.4 Multi-select and NULL — the failure that must be tested

Today every filter is equality. Multi-select on relation / novelty / pool / corpus needs `IN (…)`, and **a NULL member cannot ride in an `IN`**. Keep the `__null__` wire token and emit `(col IN (…) OR col IS NULL)` when it is selected. Getting this wrong silently drops the 64,406-row never-evaluated block — exactly the failure the three-state pool control exists to prevent. Write a test that asserts `pool = {main, more, none}` returns 254,612 and `pool = {none}` returns 64,406.

### 6.5 Grading integrity

- Grades stay in `<db>.grades.db`, ATTACHed, never in the review DB. Re-baking the projection must not destroy grading work. **Keep.**
- The POST validates the value against the closed vocabulary. **Add `evidence_id` existence validation** — an arbitrary id can currently be inserted, and an orphan grade is invisible until export.
- The `graded` filter forces a LEFT JOIN across 254,612 rows. Today the join is added only when that filter is active — **keep that**, and put it on `facet_row` for counts.
- The session strip (§1.2) is the safeguard against the worst outcome: an hour of grading that never reached disk. It is written per click, so the number must go up as the reviewer works, visibly.

### 6.6 Windows and ports

Keep `allow_reuse_address = False` and `_port_is_taken()`. Windows honours `SO_REUSEADDR` by letting a *second* process bind a port another already holds — the server starts, prints its URL, and quietly loses every request to whatever is already listening. This has happened; do not "clean it up".

---

## 7. IMPLEMENTATION ORDER

1. Extend `FACET_COLS` + indexes; verify the auto-drop/rebuild fires on an existing artifact.
2. Rewrite `_where()` for multi-value + NULL; write the §6.4 test.
3. Add the author-normalisation grouping to `/api/facets`.
4. Add `/api/facets` entries for `source_corpus`, `confidence_band`, `adjudication_status`, `main_pool_reason`, and the derived disagreement count.
5. Swap the stylesheet: tokens + the `common.css` 1593–1957 block + private additions; put `gs-discovery gs-findings` on the root.
6. Rebuild the shell: header block → caveat → help `<details>` → two-column body.
7. Rebuild the sidebar, cards 1→9 in order, `order:-1` on card 1.
8. Rebuild the row (§2), chip by chip, in the listed order.
9. Pane A: restyle the existing `.cols` grid in place; make it toggleable, default open.
10. Pane B: add the one-open cap, the watchdog, the sandbox/title attributes, `--site`, `--no-preview`.
11. Result bar, active-filter chips, sort, page size, pager.
12. Narrow the export; add the `note` field; add `evidence_id` validation.
13. Smoke: Chrome **and** Safari (or Chrome with third-party cookies blocked) for §6.1(d); one grade round-trip surviving a server restart; `pool={none}` returning exactly 64,406.