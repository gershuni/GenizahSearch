# Feature Ideas — Dicta Genizah Search Pro

> **The single hub for "what should we build next."** Created 2026-06-01. Covers the
> v8.0-headline discussion and the longer backlog. Sources: internal ideation, the Codex
> "get wild" pass (`_tmp/codex-v8-wild-ideas.md`), parked todos (`.planning/todos/`), seeds
> (`.planning/seeds/`), and the OPEN_ISSUES deferred list.
>
> Decision context: ship the rebrand + Phase-105 export + i18n pass as **v7.17.0**, or add one
> headline feature and call it **v8.0**? (Open decision in `.planning/STATE.md`.) "Major" here =
> a headline a **scholar** notices, not engineering size.

---

## ★ Leading v8.0 candidate — Assisted Join Workbench  *(spike done — FEASIBLE, ~1 phase / M)*

**Vision (Hillel, 2026-06-01):** an interactive **join-hunting workbench**, not a black-box
auto-finder. The scholar keeps **one anchor manuscript in view** (image + transcription) and,
while it stays on screen, drives the app's existing search tools to hunt for the fragments that
physically join it:

- **End-of-line / start-of-line search** — a join continues text across the tear, so you take
  the *end* of a line on the anchor and search for fragments whose *line starts* with the
  continuation (and vice versa). This is the manual analog of the report's "v7 two-hop."
- **Free-text + parallels search** — shared distinctive phrases across the corpus.
- **Visual similarity = supplementary signal ONLY.** ⚠️ VS covers only **part** of the Genizah
  and is **not exhaustive**, so it cannot be the backbone. The text/line tools (whole-corpus)
  drive the hunt; VS augments where it happens to have coverage.

Candidates surface ranked with evidence buckets ("candidate, not confirmed"); the scholar
inspects side-by-side and **opens a confirmed pair in the Fragment Puzzle**.

**Why this shape wins:** human-in-the-loop (trusts scholar judgment) → higher trust, lower
false-positive risk, and it *composes features that already exist* (line-start/end search,
parallels, Reading Desk anchor view, Visual Similarity, Fragment Puzzle, joins panel). The
algorithmic core is already researched: `docs/archive/JOIN_FINDER_REPORT.md` (8 approaches; v7
"two-hop via parallels" breakthrough; v8 + FIST visual; batch eval; tear-type detection).

**Most Genizah-native headline possible:** *"the app that helps you find new joins"* — and
uniquely ours (no competitor has text-parallels + visual-similarity + known-joins + a puzzle
canvas in one place).

**Spike verdict (2026-06-01 — `.planning/spikes/002-assisted-join-workbench/SPIKE-FINDINGS.md`): FEASIBLE, MVP ≈ M effort / ~1 phase.** The MVP is UI glue around primitives that already ship — every retrieval tool is in production and callable with arbitrary text: line-start/line-end search (`genizah_core.py:8564`; web `web/pages/search.py:646`; desktop `genizah_app.py:1655`), parallels over free text (`shared/parallels_service.py:151`, `POST /api/parallels`), and an open-in-Puzzle handoff already precedented by the Visual Similarity dialog (`web/components/visual_similarity_dialog.py:609` → `/puzzle?add=`).
- **The automated v7/v8 join finder is research-only — NO code exists** (`docs/plans/JOIN_FINDER_IMPLEMENTATION_PLAN.md` Status: Planning; never executed) and its stated numbers are weak as a backbone (v8 Recall@50 = 46.7%, ~90s/fragment, 40% of cases have no parallels at all). This *confirms* the human-in-the-loop framing: the scholar is the ranker; we don't need the slow auto-algorithm for the MVP.
- **VS coverage confirmed partial (queried):** `fist_data/visual_similarity.db` table `visual_suggestions` has **129,456 distinct manuscripts = 50.6% of the 255,723-record catalog (~59.7% of the transcribed corpus)** → VS must stay a supplementary signal, exactly as specified.
- **MVP shape:** two-pane page — pinned anchor (image + numbered transcription, reusing the Browse viewer) + a "Find continuations" action seeding a `line_start` search from the selected anchor line's END words (and the mirror) → bucketed candidate list (text-line / parallels / supplementary VS) → "Open pair in Puzzle." No new index, algorithm, or DB. Fuller workbench = L; the auto-finder = XL, out of scope.
- **⚠️ Build prerequisite:** needs the production Tantivy index carrying `line_starts`/`line_ends` (older indexes raise a rebuild error at `genizah_core.py:8583`); `Genizah_Index/` is absent on the current dev machine — verify first.

**Companion cheap win:** pair with one-click citations (below) → a "research workflow" v8.0.

---

## v8.0 shortlist — low-effort, high-value (internal ideation)

| # | Candidate | Effort | Headline | Why cheap (or not) |
|---|-----------|--------|----------|--------------------|
| 1 | **One-click scholarly citations** (BibTeX · RIS/Zotero · Chicago · plain) | **Low** | **High** | Fields already assembled for the xlsx Bibliography sheet (`shared/export_dossier.py`); pure string-templating, no data/auth/network. Captured todo: `.planning/todos/pending/2026-06-01-one-click-scholarly-citations.md`. |
| 2 | Unified bilingual metadata search | Med | Med-High | Wire PGP descriptions + libraries titles + FJMS translations into the filter (today only `catalog_fts`). Parked: todo `2026-03-09`. |
| 3 | Shareable collection links (public read-only list / Reading Desk URL) | Med | High | Reuses Supabase + the puzzle publish/HMAC pattern; needs a public route + RLS + render page. |
| 4 | Server-side search + email notification | Med-High | High | Real user ask; needs a background job queue + email + server-side result persistence. Parked: todo `2026-03-07`. |
| 5 | AI assistant (translate / explain a passage) | High | Highest | On-brand for Dicta; heavy (API-key handling in the desktop EXE, cost, eval). |

## Wild / ambitious (Codex pass — `_tmp/codex-v8-wild-ideas.md`)

| # | Idea | Headline | Effort |
|---|------|----------|--------|
| 1 | **Join Discovery Studio** | Find possible joins via combined text + visual + scholarly evidence | L/XL |
| 2 | **Ask the Genizah Corpus** | Ask research questions across 255K fragments, get cited evidence (RAG) | XL |
| 3 | **Knowledge Graph Atlas** | Persons · works · domains · joins · places · fragments as one living network | M/L |
| 4 | **Critical Edition / Synopsis Builder** | Turn parallel-search results into an aligned witness table | L |
| 5 | **Paleographic / Scribal-Hand Atlas** | Cluster fragments by hand, ruling, ink, layout → scribal families | XL |
| 6 | **Chrono-Geographic Atlas** | Map the Genizah by time, place, people, movement | M/L |
| 7 | **Semantic Concept Search** | Search by *meaning* (EN/HE query → Judeo-Arabic fragments) | L |
| 8 | **Work Reconstruction Map** | Reassemble all dispersed witnesses of a work; order; gaps | L |
| 9 | **Discovery Radar** | A daily queue of machine-suggested discoveries to Accept/Reject/Discuss | M |
| 10 | **Reproducible Research Notebook** | Reading Desk → full dossier (queries, fragments, notes, images, citations) | M |

- **Codex Top 3:** Join Discovery Studio · Knowledge Graph Atlas · Critical Edition Builder.
- **Secretly cheap (high wow-to-effort):** Knowledge Graph Atlas MVP (FJMS entities, no graph DB) ·
  Discovery Radar MVP (only "text + visual agree" candidates) · Synopsis Lite.
- **True moonshots:** full-corpus Join Discovery · Scribal-Hand Atlas · production Ask-the-Corpus.

> The Assisted Join Workbench (top) is the human-in-the-loop, de-risked scoping of Codex's #1
> (Join Discovery Studio): same Genizah-native headline, far lower risk because the scholar drives
> and confirms, and the algorithmic core is already prototyped.

## Parked backlog (promote via `/gsd-add-phase` or `/gsd-plant-seed`)

- **Todos** (`.planning/todos/pending/`): unified metadata search (`2026-03-09`), server-side
  search + email (`2026-03-07`), fill +38K manuscripts from FIST (`2026-03-18`), reading-desk UX,
  desktop corrections-fetch refactor, NLI MARC crawl.
- **Seeds** (`.planning/seeds/`): SEED-001 server IIIF image cache (reliability), SEED-003 opt-in
  OCR extension for image-only PDFs.
- **Deferred** (`docs/OPEN_ISSUES.md`): D-F2 (OCR), D-F12 (~8s search latency — quality angle),
  D-F17 (LOCAL export shape).

## Recommendation snapshot

Ship **v8.0 = Assisted Join Workbench (MVP) + one-click citations** — the definitive
Genizah-native headline plus the cheap publication-ready companion. Safer "still feels major"
fallback: **Knowledge Graph Atlas MVP + citations**.
