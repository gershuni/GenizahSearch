# Feature Research

**Domain:** Same-work / text-reuse discovery + witness-mapping + community-verification module for a scholarly manuscript research platform (Cairo Genizah; scholars + interested laypeople; bilingual EN/HE)
**Researched:** 2026-07-19
**Confidence:** MEDIUM (precedent systems are HIGH-confidence on *what they do*; the mapping to this specific mixed scholar/lay Genizah audience and the "right" UX is inferential — MEDIUM. The band schema + masking constraint are internal-brief facts — HIGH.)

## Scope note

This file covers ONLY the five NEW v9.0.0 surfaces:
1. Per-MS **connections panel** ("identified as ⟨work⟩" + related MSS, band-labeled) on browse pages
2. Per-work **witness-map page** (all carrier MSS of a work)
3. Corpus **connection atlas / graph explorer** (homepage-promoted flagship)
4. **Leads queue** (high-recall screening lane, labeled not-certified)
5. **Community judgment capture** (logged-in confirm / reject / annotate)

Everything is read from the Discovery sidecar (band schema: tier-A algorithmic; R-A "expert-verified" 0.889; R-B screening 0.859; R-CANON 0.647). The cross-cutting hard constraint — **reference-corpus provenance masking** (neutral canonical titles; never display reference-edition text; only our MS text) — is treated as a dependency of every display surface below.

---

## Feature Landscape

### Table Stakes (Users Expect These)

Missing these = the module feels broken or, worse, untrustworthy to a scholarly audience.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| **Connection panel on the manuscript reading page** ("identified as ⟨work⟩" + related MSS) | Sefaria's "Related Texts" resource panel is the archetype every user of Jewish digital texts already knows — click a text, a side panel shows what it connects to. A manuscript viewer that shows images/transcription but no connections reads as incomplete. | MEDIUM | Reuse the existing browse-page layout; new accordion/panel section keyed on `sys_id`. High bands render by default; a visible "show uncertified leads" toggle expands to screening bands. |
| **Explicit, glanceable confidence labels on every claim** | Scholarly databases communicate certainty on a gradient (high/medium/low); users of a research tool must know instantly whether a claim is *verified* or *a machine guess*. An unlabeled claim is either over-trusted or dismissed. | MEDIUM | Named bands with color + word (e.g., "Expert-verified" / "Screening lead"), NOT bare decimals like "0.889" for the lay half of the audience. Tooltip/legend explains each band + links to the precision certificate. Color must never be the *only* signal (accessibility + the "color = glanceable credibility" over-trust risk). |
| **Click-through navigation from a connection to its target** | A connection the user cannot open is dead weight. Sefaria, KITAB, impresso all make every listed relation a link to the related text/MS/passage. | LOW | Connection row → related MS browse page; work name → witness-map page. Standard routing. |
| **On-demand evidence: the shared passage, side-by-side** | Scholars will not accept "these are related — trust us." Every text-reuse viewer (Passim/KITAB DiffViewer, impresso Passages tab) lets the user *see* the aligned shared span. For a research platform this is table stakes, not a differentiator. | HIGH | **This is where masking bites.** Show OUR MS text (MiDRASH HTR / transcription) for the identified span, highlighted. NEVER render the reference-edition/M-source text on the other side. For MS↔MS pairs you can show both MSS' text; for MS↔work you show only our MS's span against a neutral work label. Design this asymmetry deliberately. |
| **Work → witness list page** (all carrier MSS of a work) | The "witness list" is the oldest primitive in textual scholarship (stemmatology): given a work, which manuscripts bear it. Clicking a work name and NOT getting its carriers would be surprising. | MEDIUM | List/table of carrier MSS with band, library, folio, thumbnail; links to each MS browse page. Reuse the existing library-filter component. |
| **Filter the witness list by band and by library** | The platform already trained users to filter by library (v8.3.0/v8.4.0 dual-mode filter). Band filtering is the new expectation given the multi-band design. | LOW | Reuse `library_filter` machinery; add a band facet. |
| **Recall-honesty disclaimer surfaced in-UI** | The corpus is HTR-noisy and coverage is partial. A scholar who sees an empty connections panel must not conclude "no such connection exists." The brief mandates "no identification shown ≠ none exists." | LOW | Persistent, quiet in-panel note + a help link. Ethical table stake for this specific corpus; cheap to build, expensive to omit (credibility damage). |
| **Bilingual EN/HE + RTL parity on all new surfaces** | Baseline for the whole platform; a Hebrew-first user base expects it from line one (Phase 121 lesson). | MEDIUM | New strings through `tr()`; RTL layout for panel, witness table, atlas labels. CI i18n guard already exists. |
| **Login-gated community actions following the existing corrections pattern** | Users already know the corrections/comments/lists flow (Supabase auth). Judgment capture must feel identical, not a new paradigm. | LOW–MEDIUM | Mirror `corrections_service` write path + RLS/GRANT conventions; anonymous users read, logged-in users act. |

### Differentiators (Competitive Advantage)

Where v9.0.0 competes. These align with the Core Value ("researchers can find what they need") and the SEED-029 research program.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| **Multi-band dual-lane design (precision by default + high-recall "leads" on demand)** | Almost every precedent (Passim, KITAB, FGP joins) ships ONE operating point. Exposing a precision-labeled default lane AND a high-recall screening lane — clearly separated — lets cautious scholars and aggressive hunters use the same data without either misleading the other. This is the module's signature. | MEDIUM | Default view = tier-A / R-A only. A single toggle reveals R-B / R-CANON as visibly-demoted "leads." Never blend the lanes silently. |
| **Corpus connection atlas / graph explorer** | Trismegistos proved network views over a text corpus generate genuinely new research questions (who/what connects across documents). A work-connection graph at Genizah scale (52K MSS, 4K works) is something no existing Genizah tool offers; strong homepage flagship. | HIGH | **Highest-risk feature — see anti-features on the hairball.** Must be work-centric ego-graphs / filtered / aggregated, never a raw force-directed dump of the 89%-liturgical giant component. Prefer work→work edges (shared carriers) as the primary graph, MS-level as drill-down. |
| **Indirect-witness / citation surfacing** | The SEED-029 flank-contrast "island" class identifies a MS that *quotes* a non-canonical work — an indirect textual witness, the highest-value scholarly class in the research program. Surfacing "this fragment preserves a citation of ⟨lost/rare work⟩" is a discovery no image-based join tool and no keyword search can produce. | MEDIUM–HIGH | Depends on the flank-contrast classifier's output being in the sidecar. Present as a distinct connection *type* ("cites / preserves a passage of"), not conflated with "is a copy of." |
| **Evidence viewer showing only our manuscript's scholarly text** | The masking constraint, inverted into a feature: instead of a canned printed edition, the user sees *this manuscript's own words* (MiDRASH HTR / human transcription) for the identified passage — exactly what a Genizah scholar wants and what the platform already renders well. | MEDIUM | Reuses the existing transcription/version-selector display. The "we show you the manuscript, not a book" framing is a positioning win over edition-centric tools. |
| **Measured-precision certificate surfaced in the UI** | Scholarly databases rarely publish a measured precision for their algorithmic claims. Linking each band label to a pre-registered, stratified precision number ("this band was measured at N% precision on an M-card audit") is a credibility differentiator and pre-empts "how do you know?" | LOW (UI) / (measurement is a separate work item) | A short methods/confidence page + per-band tooltip. Honesty about the R-A audit still being pending ("expert-verified" until the independent audit passes) is itself trust-building. |
| **Community judgment capture that feeds future certification** | FromThePage-style review + Zooniverse-style multi-judgment, but expert-grade: logged-in users confirm/reject/annotate a work-witness claim, and those judgments become a labeled pool for the *next* certification round. Turns the user base into a recall/precision flywheel without polluting the shipped bands. | MEDIUM–HIGH | The critical design is that judgments are an ADDITIVE annotation layer with full provenance (who/when/what), NOT edits to the band data. See anti-features. |
| **Homepage promotion of Atlas + discovery suggestions as flagship modules** | Repositions the platform from "search the Genizah" to "the Genizah's connections revealed" — a positioning differentiator for the v9.0.0 flagship release. | LOW (UI) | Product decision; cheap once the atlas + panel exist. |

### Anti-Features (Commonly Requested, Often Problematic)

| Anti-Feature | Why Requested | Why Problematic | Alternative |
|--------------|---------------|-----------------|-------------|
| **Unlabeled algorithmic claims presented as established facts** | "Just tell me what it is" reads cleaner than hedged labels. | The cardinal sin the brief calls out. An algorithmic identification shown as a bare fact will be cited as ground truth, then a wrong one damages the platform's scholarly credibility irreparably. | Every claim carries its band + confidence label inline; the label is not optional chrome. |
| **Crowd majority-vote auto-certification (Zooniverse consensus retirement)** | Zooniverse "retires" a subject once N volunteers agree; tempting to auto-promote community-confirmed claims into the certified band. | A lay majority can be confidently wrong on a scholarly identification; auto-promotion lets volume overwrite expertise and *pollutes the certified data*. Zooniverse's model works for "is there a galaxy here," not "is this Mishnah Shabbat 3." | Community judgments stay a **separate, visible layer**; they inform a *curated* re-certification round run by the research program, never auto-mutate a band. This is the direct answer to "how judgment capture avoids polluting data." |
| **Editable / overwritable identifications** | Wiki-style "just fix it" feels empowering. | Overwriting destroys provenance and the audit trail; one bad edit silently corrupts a claim used by others. | FromThePage's lesson: keep versions. Capture confirm/reject/annotate as append-only annotations with attribution + timestamp; the underlying claim is immutable in the shipped snapshot. |
| **Displaying the reference-edition / M-source text** | Showing both sides of a match is the obvious "complete" evidence view. | Hard constraint violation (provenance masking + licensing). Reveals M-source provenance and may expose non-redistributable reference text. | Show only OUR MS text against a neutral canonical work label; for MS↔MS pairs both MSS' own text is fine. |
| **Full critical apparatus / variant collation ("stemma")** | The "witness list" framing invites the expectation of a collated variorum with variant readings and a stemma tree. | We have "MS carries work" evidence, not aligned collated variants; a stemma view would misrepresent the data and is a massive scope explosion. | Ship the witness *list* (carriers + band + evidence span). Frame it explicitly as a witness census, not a critical edition. Defer collation indefinitely. |
| **Raw full-corpus force-directed graph as the primary atlas view** | "Show me everything connected" is the intuitive ask for a network. | The rehearsal found an 89%-liturgical giant component (a hairball); a raw whole-corpus graph is unreadable and slow. impresso's evaluators already flagged "a lot going on." | Work-centric ego-graphs, aggregated work→work edges, mandatory filtering, and Track-1 canon masking as the gate before any census graph. Progressive disclosure from a curated overview. |
| **Live / real-time recomputation of the map** | Users expect modern apps to update as data changes. | The refresh pipeline is explicitly out of scope; the sidecar ships as a dated snapshot. Promising freshness you can't deliver erodes trust. | Ship a clearly dated snapshot ("Discovery data as of ⟨date⟩") + a documented rebuild recipe. Set the expectation honestly. |
| **False-precision numeric scores in the lay-facing UI** | The raw confidence numbers (0.889, 0.647) exist and look authoritative. | Presenting three-decimal probabilities to interested laypeople implies a precision the data doesn't have and confuses more than it informs. | Map to named bands + plain-language tooltips; expose the exact number only in an expert/methods view or on hover. |
| **Gamification / leaderboards / notifications for community verification** | Zooniverse-style engagement mechanics drive volume. | Wrong incentive for an expert scholarly audience — rewards throughput over care and cheapens the tool's tone. | Quiet, credit-attributed contribution (mirroring corrections); recognition through attribution, not points. |
| **Auto-suggesting physical-fragment joins here** | The connection data looks join-like; users may expect "find the other half." | That's the Joins Lab's job (image + physical join); same-work text identification is a different claim (two MSS of the same *work* are usually NOT physical joins). Conflating them misleads. | Keep the vocabulary distinct ("same work / carries / cites" vs "physical join"); cross-link to Joins Lab where relevant, don't merge. |

---

## Feature Dependencies

```
[Discovery sidecar + band schema]  ← foundation; everything reads from it
    ├──requires──> [Reference-corpus masking logic]   (cross-cutting; gates ALL display)
    │
    ├──> [MS connections panel]
    │        └──requires──> [Evidence viewer (our-text-only)]
    │                            └──requires──> [existing transcription/version-selector display]
    │
    ├──> [Work → witness-map page]
    │        └──requires──> [existing library-filter component] (band facet added)
    │
    ├──> [Leads queue]  (reads R-B / R-CANON rows; = the panel's "show leads" lane as a dedicated page)
    │
    └──> [Connection atlas / graph explorer]
             ├──requires──> [Track-1 canon masking as the giant-component gate]
             └──enhances/enhanced-by──> [Work → witness-map page] (atlas node → witness page drill-down)

[Community judgment capture]
    ├──requires──> [existing Supabase auth + corrections write pattern]
    ├──requires──> [MS connections panel + witness page] (the surfaces judgments attach to)
    └──feeds (does NOT overwrite)──> [future certification round → new band snapshot]

[Homepage promotion] ──requires──> [Atlas] + [connections panel] exist first
```

### Dependency Notes

- **Sidecar + band schema is the hard prerequisite for all five surfaces.** No UI phase can start before the schema is stable. This is the natural Phase-1 spine.
- **Masking logic is cross-cutting, not a feature** — it must be a shared display helper enforced (and CI-guarded) on every surface that renders work identity or passage text. Treat it like the `safe_storage` chokepoint: one enforced path, tested.
- **Evidence viewer depends on the existing transcription display**, so it's cheap to reuse — but the masking asymmetry (show our MS, hide the reference) is new logic.
- **Community judgment capture depends on the panel + witness page existing** (there's nothing to judge otherwise) and on Supabase auth — so it lands AFTER the read surfaces, mirroring how corrections/comments layered onto browse.
- **Atlas depends on the giant-component / masking problem being solved first** — the rehearsal already proved a naive graph is a blob. It is the highest-complexity, highest-risk surface and should be the capstone, not the opener, even though it's the marketed flagship.
- **Leads queue and the panel's "show uncertified leads" toggle are the same data at two scales** — build the band-lane logic once; the queue is the corpus-wide view of what the panel shows per-MS.

---

## MVP Definition

Interpreting v1 / v1.x / v2 as **within-v9.0.0 phase ordering + explicitly-deferred**:

### Launch With (v1 — the de-risking spine + core value)

- [ ] **Discovery sidecar + band schema** — nothing works without it; also the object of the discuss-phase.
- [ ] **Masking display helper (enforced + CI-guarded)** — hard constraint; must exist before any surface renders.
- [ ] **MS connections panel** with high-bands-by-default + a visible "show uncertified leads" toggle — the single highest-value, most-expected surface; validates the whole concept on the page users already visit.
- [ ] **Evidence viewer (our-MS-text-only, masked)** — a scholarly audience will not trust bare claims; the panel is not credible without it.
- [ ] **Confidence-labeling system** (named bands + legend + recall-honesty disclaimer) — the ethical + credibility core.
- [ ] **Work → witness-map page** — the second-most-expected primitive; the click-through target for every work name in the panel.

### Add After Validation (v1.x — same milestone, later phases)

- [ ] **Community judgment capture** (confirm/reject/annotate, additive layer) — trigger: read surfaces are live and users are engaging; needs auth wiring + the "don't pollute the bands" guardrail designed carefully.
- [ ] **Leads queue as a dedicated page** — trigger: the panel's lead-lane logic is proven; promotes the high-recall lane to a browsable corpus-wide queue.
- [ ] **Indirect-witness / citation connection type** — trigger: flank-contrast classifier output is confirmed clean enough to surface as its own class.

### Future Consideration (v2+ / explicitly deferred)

- [ ] **Connection atlas / graph explorer** — the marketed flagship, but highest-complexity + highest-risk (hairball / giant-component problem). *Judgment call for the roadmap:* if v9.0.0 must ship the atlas as its flagship, treat it as the capstone phase AFTER panel + witness page de-risk the data, and scope it to work-centric ego-graphs, not a full-corpus dump. If schedule pressure appears, this is the first thing to cut to a fast-follow.
- [ ] **Precision-certificate auto-refresh / re-certification pipeline** — refresh pipeline is out of scope this cycle (snapshot ship).
- [ ] **Text-reuse engine as the `/parallels` (desktop: composition) backend** — explicitly deferred by the user (2026-07-19).
- [ ] **Desktop parity** — web-only this milestone.
- [ ] **Collation / variant apparatus / stemma** — deliberately never (anti-feature).

---

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| Discovery sidecar + band schema | HIGH (enabling) | MEDIUM | P1 |
| Masking display helper | HIGH (constraint) | MEDIUM | P1 |
| MS connections panel (bands + leads toggle) | HIGH | MEDIUM | P1 |
| Evidence viewer (our-text-only) | HIGH | HIGH | P1 |
| Confidence labels + recall-honesty disclaimer | HIGH | MEDIUM | P1 |
| Work → witness-map page | HIGH | MEDIUM | P1 |
| Community judgment capture | MEDIUM–HIGH | MEDIUM–HIGH | P2 |
| Leads queue (dedicated page) | MEDIUM | LOW–MEDIUM | P2 |
| Indirect-witness / citation type | HIGH (niche) | MEDIUM | P2 |
| Connection atlas / graph explorer | MEDIUM–HIGH | HIGH | P2/P3 (flagship-but-risky) |
| Homepage promotion | MEDIUM | LOW | P2 (rides on atlas + panel) |
| Precision-certificate page | MEDIUM | LOW (UI) | P2 |

**Priority key:** P1 = must have for launch · P2 = should have · P3 = defer.

---

## Competitor / Precedent Feature Analysis

| Feature area | Sefaria | KITAB / OpenITI (Passim-backed) | impresso Text Reuse at Scale | Zooniverse / FromThePage | Our v9.0.0 approach |
|---|---|---|---|---|---|
| **Connection surfacing** | "Related Texts" resource side-panel; 110K+ text-to-text links; one-click open (does well: familiar, instant). Weakness: links are curated/rule-based, no confidence gradient. | Book→corpus and book↔book reuse views. | Cluster-centric "Passages" tab. | n/a | Side panel on the MS page (Sefaria-familiar) BUT every connection carries a confidence band — the gradient Sefaria lacks. |
| **Evidence / alignment display** | Shows the connected text itself. | DiffViewer: side-by-side color-coded aligned passage; sub-word Arabic-aware (does well). Scroll view highlights shared spans + preserves order. | Passages tab: pick a start passage, cycle others, matching text highlighted in contrasting colors. | FromThePage: image ↔ transcription side-by-side. | Side-by-side highlighted span, BUT masked: our MS text only vs a neutral work label (no reference edition). |
| **Corpus overview / graph** | Seven thematic link visualizations. | Book-to-corpus reuse heatmap (which parts reused most); network of related books. | Overview + Statistics tabs (small-multiples, matrix of co-occurring clusters, time charts). Lesson: powerful but "a lot going on," learning curve, needs domain expertise. | n/a | Work-centric atlas; must avoid the impresso "too much at once" trap AND our own giant-component blob → aggregated, filtered, progressive. |
| **Confidence / uncertainty** | Minimal — links presented as facts. | Reuse *amount* encoded (heatmap intensity) but not a certainty band. | Lexical-overlap / cluster-size as sortable metrics (quantitative, not certainty labels). | Zooniverse: Bayesian consensus probability per subject; volunteer weighting; uncertainty estimates. | Named bands (algorithmic / expert-verified / screening) + measured precision certificate; numeric scores hidden behind bands for laypeople. |
| **Community verification** | Editorial + "Voices" sheets (not claim-verification). | n/a (algorithmic + scholar-curated corpus). | User collections, not verification. | Zooniverse: N-fold redundant classification → majority/Bayesian retirement (does well at scale; BAD model to copy for expert claims). FromThePage: "needs review" flag, version control, page comments, trusted-collaborator tiers (does well: provenance, no overwrite). | FromThePage's provenance/versioning model (append-only, attributed) — NOT Zooniverse auto-retirement. Judgments feed a curated re-certification; never auto-mutate a band. |
| **Witness list** | Manuscript images linked to Talmud/Mishnah/Tanakh via NLI. | Corpus is work-centric by design. | Newspaper "witnesses" per cluster. | n/a | Per-work carrier-MS list (stemmatology witness-census framing), band + library filterable — but explicitly NOT a collated critical apparatus. |

---

## Answers to the downstream consumer's specific questions

**What a witness-map page shows:** a work's neutral canonical title + a filterable (band, library) table/list of every carrier MS — thumbnail, shelfmark/folio, band label, and a link into each MS browse page and its evidence span. It is a *census* of carriers, not a variant collation. Optional summary counts (e.g., N carriers across M libraries, band breakdown). It must carry the recall-honesty note.

**How confidence is communicated:** named bands with color + plain-language word (not bare decimals), a legend, per-band tooltips, and a link to a measured-precision certificate page. Default view shows only high bands; a single explicit toggle reveals screening leads, visibly demoted. Color is never the sole signal. "Expert-verified" labeling is used honestly (R-A audit pending) and the recall disclaimer ("no identification shown ≠ none exists") is persistent.

**How judgment capture avoids polluting data:** community confirm/reject/annotate is an **append-only, attributed annotation layer** (FromThePage provenance model) stored in Supabase like corrections — it never edits the shipped band data and is never auto-promoted by majority vote (the explicit rejection of the Zooniverse consensus-retirement model). Judgments display as community signal alongside — not merged into — the algorithmic/expert bands, and feed a later *curated* certification round that produces the next snapshot.

---

## Sources

- Sefaria Help / Voices — Related Texts resource panel, 110K+ interconnections, manuscript links: https://help.sefaria.org/hc/en-us/articles/18613227644316-How-to-Find-Interconnected-Texts · https://www.sefaria.org/sheets/299491
- Passim (dasmiq) + Programming Historian lesson — n-gram filtering + local/global alignment, cluster/witness output, OCR-noise tolerance: https://github.com/dasmiq/passim · https://programminghistorian.org/en/lessons/detecting-text-reuse-with-passim
- KITAB text-reuse visualizations — pairwise scroll view, DiffViewer (color-coded aligned diff), reuse heatmap, book↔corpus explore: https://kitab-project.org/New-KITAB-visualizations/ · https://kitab-project.org/methods/text-reuse · https://github.com/kitab-project-org/explore
- impresso Text Reuse at Scale — Overview/Statistics/Passages tabs, cluster filtering, drill-down, "learning curve / a lot going on" evaluation finding: https://pmc.ncbi.nlm.nih.gov/articles/PMC10654985/
- Zooniverse — redundant classification, majority/Bayesian consensus (SWAP), volunteer weighting, retirement thresholds, uncertainty estimates: https://arxiv.org/pdf/1903.07776 · https://arxiv.org/pdf/2511.03016
- FromThePage — review workflows, "needs review" flag, version control / track changes, page comments, trusted-collaborator tiers: https://content.fromthepage.com/review-workflows-and-quality-control/ · https://github.com/benwbrum/fromthepage
- Trismegistos Networks — SNA over people/places across documents, graph metrics, research vs add-on network modes: https://wiki.digitalclassicist.org/Trismegistos · https://link.springer.com/chapter/10.1007/978-3-319-15168-7_38
- Scholarly certainty / provenance UI research — certainty gradient classification, provenance display, color-as-glanceable-credibility over-trust risk: https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7182025/ · https://arxiv.org/pdf/2303.12118
- Stemmatology / witness lists — witness = independent bearer, collation, variorum (framing + scope boundary): https://en.wikipedia.org/wiki/Textual_criticism
- FGP join suggestions (Genizah domain precedent) — ranked algorithmic candidates for expert inspection, human confirmation as gold standard: https://en.wikipedia.org/wiki/Friedberg_Geniza_Project
- Internal: `.planning/PROJECT.md` (v9.0.0 milestone), `.planning/seeds/SEED-029-*.md` (band schema, flank-contrast/indirect-witness class, masking rationale, rehearsal giant-component finding).

---
*Feature research for: same-work discovery + witness-mapping + community-verification module (Cairo Genizah)*
*Researched: 2026-07-19*
