# Phase 18: Dead Code Removal - Context

**Gathered:** 2026-02-11
**Status:** Ready for planning

<domain>
## Phase Boundary

Remove all AI Search artifacts from both apps (desktop PyQt6 + web NiceGUI), help documentation, and genizah_core.py. Both apps must launch and function normally with no trace of AI features. This is pure removal — no new features, no repurposing of AI infrastructure.

</domain>

<decisions>
## Implementation Decisions

### Removal scope
- **Full sweep** — remove everything AI-related, not just the named classes in success criteria
- Includes: classes (AIManager, AIDialog, AIWorkerThread), helper functions, signal handlers, constants (AI_PROVIDER_ENDPOINTS), dead imports (google-genai), utility methods, and any code that only existed to serve AI features
- If a code path only existed for AI, it goes — no orphaned support code left behind

### File deletion
- Delete AI-only files entirely (e.g., any ai_manager.py, ai_config.py, or similar)
- No stubs, no empty files — clean removal

### Dependencies
- Remove AI-related pip packages from requirements.txt / pyproject.toml (e.g., google-generativeai)
- Verify packages aren't used elsewhere before removing

### Changelog
- Add a brief entry under v5.7.1 noting the AI code removal (e.g., "Removed deprecated AI Search feature code")

### Environment variables
- Leave .env templates and env var documentation alone — don't touch environment configuration

### Known state
- Desktop app (genizah_app.py) likely has more AI artifacts than the web app — audit desktop more thoroughly
- Web app may have lighter AI footprint

### Claude's Discretion
- **Comments:** Judge which AI-referencing comments are useful breadcrumbs vs. clutter. Remove obvious dead comments, keep any that explain why current code exists the way it does.
- **UI strings:** Delete pure AI UI elements. If any AI-related UI element could genuinely serve current features, Claude may repurpose rather than delete — but default to removal.

</decisions>

<specifics>
## Specific Ideas

No specific requirements — open to standard approaches. The success criteria from the roadmap are explicit and drive the work.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 18-dead-code-removal*
*Context gathered: 2026-02-11*
