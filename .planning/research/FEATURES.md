# Feature Landscape: External Data Integration for Scholarly Manuscript Platforms

**Domain:** Scholarly manuscript search platform with external data integration (PGP transcriptions, NLI joins)
**Researched:** February 5, 2026
**Confidence Level:** MEDIUM (based on web research of comparable platforms, verified against project-specific context)

---

## Executive Summary

GenizahSearch is adding two major data sources: 9,364 PGP transcriptions (human-curated, document-level) and ~424K NLI join relationships. This research identifies what features are expected (table stakes), what differentiates (competitive advantage), and what to avoid (anti-features) based on analysis of comparable platforms including:

- **Princeton Geniza Project (PGP)** - Primary Geniza scholarly interface
- **Friedberg Geniza Project (FGP)** - Comprehensive Geniza image/data platform
- **Fragmentarium** - Medieval manuscript fragment platform
- **e-codices** - Swiss virtual manuscript library
- **Transkribus** - HTR/transcription platform

**Key Finding:** The Cairo Genizah has several competing platforms. GenizahSearch's value proposition is **unified access across all holding libraries with scholarly enrichment**. Table stakes must include what PGP and FGP already offer. Differentiators should focus on what those platforms lack: unified cross-collection search with integrated transcriptions.

---

## Table Stakes

Features users expect. Missing = product feels incomplete or loses scholarly credibility.

### 1. Transcription Display

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Display transcription alongside image | PGP, FGP, Transkribus all do this | Medium | IIIF viewer + text panel |
| Source attribution | Scholars expect credit | Low | "Transcription by X" |
| Transcription type indicator | Users need to know what they're seeing | Low | "Digital Edition", "Edition", "Translation" |
| Link to original source | Academic integrity | Low | Link to PGP document page |
| RTL text display | Hebrew/Judeo-Arabic is RTL | Low | Already in GenizahSearch for titles |

**Rationale:** PGP already displays transcriptions with attribution. GenizahSearch must match this baseline to be taken seriously for transcription search.

### 2. Transcription Search

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Full-text search in transcriptions | Core value of adding transcriptions | Medium | Add to Tantivy index |
| Search toggle (titles vs transcriptions) | User control over search scope | Low | UI checkbox |
| Highlight matches in results | Standard search UX | Medium | Snippet with context |
| Hebrew/Arabic script search | PGP supports this | Medium | Already handling Hebrew |
| Transcription availability indicator | Users need to know what has transcriptions | Low | Badge/icon in results |

**Rationale:** The primary value of adding 9,364 transcriptions is searchability. PGP offers "keyword search in Judaeo-Arabic, Hebrew, Arabic and other languages." GenizahSearch must match this.

### 3. Fragment Relationship Display

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Show related fragments | FGP has this, PGP has "joins" | Medium | Display join group members |
| Relationship type indication | Different types matter (physical vs same scribe) | Low | Already in data model |
| Navigate to related fragments | One-click to see joined fragment | Low | Links to other records |
| Join source attribution | Scholars want to know who identified join | Low | "Identified by Scholar X" |
| Join count indicator | Users need to see relationships exist | Low | Badge showing N joins |

**Rationale:** FGP provides "computed joins-suggestions function" and scholarly joins. GenizahSearch already has a joins system; integrating FIST joins is expected.

### 4. Metadata Enrichment Display

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Document type display | PGP categorizes all documents | Low | "Letter", "Legal document", etc. |
| Subject/domain classification | FGP provides domains (Piyyut, Bible, etc.) | Low | From FIST domains |
| Date information | Standard manuscript metadata | Low | From PGP/FIST |
| Language indication | Critical for Genizah multilingual corpus | Low | Hebrew, Judeo-Arabic, Arabic |
| Description/summary | Two-thirds of PGP entries have descriptions | Low | Display when available |

**Rationale:** Both PGP and FGP provide rich metadata. GenizahSearch must display enriched metadata to be competitive.

---

## Differentiators

Features that set product apart. Not expected, but valued.

### 1. Unified Cross-Collection Search

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| **Search transcriptions across ALL libraries** | Neither PGP nor FGP covers all 217K | Already done | GenizahSearch's core advantage |
| Search titles + transcriptions + descriptions together | Unified discovery | Medium | Single query, multiple fields |
| Filter by holding institution + has transcription | Slice data across sources | Medium | Combine existing filters |

**Why Differentiating:** PGP covers ~28K documents primarily from Cambridge. FGP has images but limited transcription search. GenizahSearch can be the **only place to search transcriptions alongside 217K records from all libraries**.

### 2. Unified Document View for Multi-Fragment Joins

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| **Virtual document reconstruction** | See joined fragments as single document | High | Major feature |
| Combined transcription display | Full text across fragments | Medium | Concatenate transcriptions in order |
| IIIF manifest for joined fragments | Side-by-side viewing | High | Create composite IIIF manifest |
| Sequence reordering | Scholars disagree on fragment order | Medium | Allow manual reorder |

**Why Differentiating:** PGP links fragments but doesn't provide unified viewing. FGP suggests joins but doesn't reconstruct. Fragmentarium has "tool to link and assemble fragments" - GenizahSearch could match this for Genizah.

### 3. Heterogeneous Source Integration

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Show data provenance | "Transcription: PGP, Domain: FIST, Join: NLI" | Low | Track source per field |
| Confidence indicators | User knows which data is verified vs computed | Low | Visual indicators |
| Last updated dates | Currency of external data | Low | Track import dates |
| Link to external records | Users can verify in original source | Low | URLs to PGP, FGP |

**Why Differentiating:** No single platform shows integrated data from multiple authoritative sources with clear provenance.

### 4. Search Quality Differentiation

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Filter: "has human transcription" | Distinguish from auto-OCR | Low | Boolean field |
| Transcription completeness indicator | Full vs partial transcription | Low | Character count or % |
| Search weight preference | User chooses title vs transcription priority | Medium | Adjustable ranking |

**Why Differentiating:** As platforms add HTR/OCR alongside human transcriptions, quality differentiation becomes valuable.

### 5. Network Visualization (Future)

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Join network graph | Visual exploration of relationships | High | D3.js or similar |
| "Connected documents" discovery | Find related documents through joins | Medium | Graph traversal |
| Collection overlap visualization | See which collections share joins | Medium | Aggregation view |

**Why Differentiating:** Network analysis is common in DH but not applied to Genizah joins at scale.

---

## Anti-Features

Features to explicitly NOT build. Common mistakes in this domain.

### 1. Duplicate FGP/PGP Functionality

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Build our own transcription editor | FGP, PGP, Transkribus already exist | Link to external tools |
| Build our own join detection | FGP has AI join suggestions | Import FGP/NLI joins instead |
| Build crowdsourcing transcription | Scribes of Cairo Geniza exists on Zooniverse | Link to that project |

**Rationale:** "Tools and platforms specific to particular collections or discrete tasks have proliferated, but their content, functionality, and workflows are often siloed." The solution is integration, not reinvention.

### 2. Incomplete Metadata Display

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Show enriched data without source | Scholars need provenance | Always show "from PGP" / "from FIST" |
| Mix confidence levels invisibly | Misleads users | Visual confidence indicators |
| Display stale data without dates | Currency matters | Show "last updated" |

**Rationale:** "Metadata of digital objects often exhibits incomplete, inconsistent, and incorrect values." Don't compound the problem.

### 3. Overly Complex UI

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Expose all 259 FIST domains in UI | Information overload | Group into ~10 top-level categories |
| Show all join types equally | Confuses users | Prioritize physical joins |
| Multiple search modes without guidance | Users don't know which to use | Smart defaults with toggle |

**Rationale:** "Users do not feel they personally have the technical expertise" - keep UI simple.

### 4. Broken Cross-References

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Dead links to external sources | Frustrates users | Validate links, handle 404s gracefully |
| Inconsistent shelfmark formats | Matching breaks | Normalize all shelfmarks |
| Orphaned join members | Shows "joined to nothing" | Validate join integrity |

**Rationale:** From JOINS_TECHNICAL_SPEC.md - shelfmark format inconsistency is a known major issue.

### 5. Scope Creep

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Build HTR/OCR functionality | Complex ML infrastructure | Use Transkribus API if needed later |
| Add user-contributed transcriptions | Requires moderation, versioning | Link to existing crowdsource projects |
| Build bibliography management | Not core value | Link to Zotero or external sources |

**Rationale:** Focus on the integration value proposition, not competing with specialized tools.

---

## Feature Dependencies

```
[Foundation Layer]
    Tantivy index schema update (add transcription field)
            |
            v
[Transcription Layer]
    PGP data import --> Transcription search --> Transcription display
            |                    |
            v                    v
    Search toggle UI        Result snippets

[Joins Layer - Can proceed in parallel]
    FIST joins import --> Join display on browse page --> Navigate to joined
            |
            v
    Join count indicator in search results

[Enrichment Layer - Depends on Foundation]
    FIST domains import --> Domain display --> Domain filter in search
            |
            v
    PGP metadata (type, tags, dates)

[Advanced Layer - Future]
    Unified document view (depends on Joins + Transcriptions)
            |
            v
    Network visualization
```

---

## MVP Recommendation

For MVP, prioritize **table stakes** to establish credibility:

### Phase 1: Transcription Integration (Core Value)
1. Add transcription field to Tantivy index
2. Search toggle: "Search in transcriptions"
3. Display transcription on browse page with source attribution
4. Transcription availability indicator in search results

### Phase 2: Join Integration (Expected Feature)
1. Import FIST joins (35K records)
2. Display join groups on browse page
3. Navigate to joined fragments
4. Join count indicator

### Phase 3: Metadata Enrichment (Completeness)
1. Display document type from PGP
2. Display domains from FIST
3. Show date/language information

### Defer to Post-MVP

- Virtual document reconstruction (HIGH complexity)
- Network visualization (HIGH complexity)
- Search weight preferences (nice-to-have)
- User-contributed corrections (requires moderation system)

---

## Complexity Estimates

| Feature | Complexity | Rationale |
|---------|------------|-----------|
| Add transcription to Tantivy | Medium | Schema change + re-index |
| Search toggle UI | Low | UI only |
| Display transcription | Low | Already have viewer infrastructure |
| Import FIST joins | Medium | CSV parsing + validation |
| Display joins | Low | Already have joins display code |
| Domain filter | Medium | New filter + UI component |
| Virtual document view | High | New viewer paradigm, IIIF integration |
| Network visualization | High | New D3.js component |

---

## Sources

### Primary Platforms Analyzed

- [Princeton Geniza Project](https://geniza.princeton.edu/en/) - Document types, search features, transcription display
- [Friedberg Geniza Project](https://en.wikipedia.org/wiki/Friedberg_Geniza_Project) - Joins, images, computed suggestions
- [Fragmentarium](https://fragmentarium.ms/) - Fragment assembly tools, IIIF integration
- [e-codices](https://www.e-codices.unifr.ch/en) - Dispersed collection reunification
- [Scribes of the Cairo Geniza](https://judaicadh.github.io/cairogeniza/) - Citizen science transcription

### Technical Standards

- [IIIF Cookbook](https://iiif.io/api/cookbook/) - Multi-source manuscript viewing
- [IIIF Guide for Implementers](https://iiif.io/guides/guide_for_implementers/) - Best practices
- [Mirador Viewer](https://libraries.mit.edu/music/sequentiary/using-the-iiif-mirador-viewer/) - Side-by-side comparison

### Digital Humanities Context

- [DHQ: Manuscript Study in Digital Spaces](https://dhq.digitalhumanities.org/vol/12/2/000374/000374.html) - State of the field
- [Metadata Quality in Digital Libraries](https://jodi-ojs-tdl.tdl.org/jodi/article/view/jodi-171/68) - Data quality issues
- [Transkribus Integration](https://blog.transkribus.org/en/fromthepage-enhanced-transcription-platform-with-transkribus-api) - API integration patterns

### Project-Specific Documentation

- `TRANSCRIPTIONS_INTEGRATION_DESIGN.md` - PGP export details (96.5% match rate)
- `FIST_INTEGRATION_DESIGN.md` - NLI joins and domain data
- `JOINS_TECHNICAL_SPEC.md` - Existing joins system lessons learned

---

## Confidence Assessment

| Area | Level | Reason |
|------|-------|--------|
| Table Stakes | HIGH | Based on direct analysis of PGP and FGP feature sets |
| Differentiators | MEDIUM | Competitive positioning hypothesis, needs validation |
| Anti-Features | HIGH | Based on documented problems in field + project history |
| Complexity | MEDIUM | Based on existing codebase knowledge, estimates may vary |

---

*Generated: February 5, 2026*
*Research Mode: Ecosystem (Features dimension)*
