---
phase: 81B
plan: 03
type: execute
wave: 2
depends_on: [81B-02]
files_modified:
  - skills/cairo-genizah-research/scripts/normalize_shelfmark.py
  - skills/cairo-genizah-research/scripts/format_output.py
  - skills/cairo-genizah-research/scripts/stage.py
autonomous: true
requirements: [SKILL-02, SKILL-04, SKILL-05]
tags: [skill, business-logic, ranking, honesty, wave-2]
must_haves:
  truths:
    - "All 15 RED tests in tests/test_skill_consumer.py are GREEN (honesty annotations, known-witness flag/exclude, shelfmark normalization, merge-by-uid + tier assignment)"
    - "honesty_annotation maps text_source='pgp_transcription' to NO annotation; any other value triggers '(full text unavailable; based on snippet of N chars)' (R2 mapping locked)"
    - "image-unavailable annotation '(no image available)' triggers when image.url is None AND image.sources == []"
    - "stage.merge_results dedupes by uid, aggregates _phrase_count, assigns tier A/B/C, sorts by phrase count desc then score desc"
    - "apply_known_witness_policy('flag') marks but keeps; ('exclude') drops; unknown policy raises ValueError"
    - "normalize_shelfmark is idempotent and collapses whitespace + strips MS prefix"
    - "Skill does NOT import from genizah_core or shared/* (SKILL-05 portability constraint)"
  artifacts:
    - path: "skills/cairo-genizah-research/scripts/normalize_shelfmark.py"
      provides: "Tier-1 lightweight shelfmark normalizer (SKILL-05)"
      exports: ["normalize"]
    - path: "skills/cairo-genizah-research/scripts/format_output.py"
      provides: "honesty_annotation + apply_known_witness_policy + render_markdown for ranked candidate output (SKILL-04, SKILL-05, ranking output schema)"
      exports: ["honesty_annotation", "apply_known_witness_policy", "render_markdown", "render_json"]
    - path: "skills/cairo-genizah-research/scripts/stage.py"
      provides: "Staged phrase discovery orchestrator: merge_results by uid + tier assignment + CLI for fan-out (SKILL-02)"
      exports: ["merge_results", "stage_search"]
  key_links:
    - from: "skills/cairo-genizah-research/scripts/stage.py"
      to: "skills/cairo-genizah-research/scripts/search.py"
      via: "import call_search; iterate phrases; merge by uid"
      pattern: "from .search import call_search|from scripts.search import call_search"
    - from: "skills/cairo-genizah-research/scripts/format_output.py"
      to: "browse response shape"
      via: "reads text_source, image.url, image.sources, text"
      pattern: "text_source"
    - from: "skills/cairo-genizah-research/scripts/format_output.py"
      to: "Phase 79 D-10 enum mapping"
      via: "treats 'pgp_transcription' as 'full' per R2"
      pattern: "pgp_transcription"
---

<objective>
Build the business-logic layer of the cairo-genizah-research skill: shelfmark normalization (SKILL-05 Tier 1), honesty annotations + known-witness policy (SKILL-04 + SKILL-05), and staged phrase discovery orchestrator (SKILL-02). Flips all 15 SKILL-02/04/05 RED tests from Plan 01 GREEN.

Purpose: Per CONTEXT D-05, the skill's value-add is Claude-authored justifications grounded in browse text. Per SKILL-04, those justifications must be honest about partial evidence (snippet vs full transcription, image availability). Per SKILL-02, the staged-discovery loop is the v7.10 acceptance harness — multiple `/api/search` calls merged by uid, top-N drilled via `/api/browse`. Per SKILL-05, shelfmark normalization is local (Tier 1) with optional API-resolution fallback (Tier 2 — invoked by the model from SKILL.md instructions, NOT this script).

Output: 3 files, ~250 lines of Python total. Pure stdlib + skills.cairo_genizah_research.scripts internal imports. No `genizah_core` / `shared/*` dependency (SKILL-05 portability lock).

R2 explicitly addressed: `honesty_annotation` treats `text_source == 'pgp_transcription'` as the "full" value per the locked Phase 79 D-10 enum, NOT the literal string `'full'` mentioned in REQUIREMENTS.md SKILL-04. This mapping is documented inline + tested by `test_honesty_annotation_maps_pgp_transcription_as_full_per_R2`.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@.planning/phases/81B-claude-skill-consumer/81B-CONTEXT.md
@.planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md
@.planning/phases/81B-claude-skill-consumer/81B-01-PLAN.md
@.planning/phases/81B-claude-skill-consumer/81B-02-PLAN.md
@.planning/phases/79-api-browse-drill-down/79-CONTEXT.md
@tests/test_skill_consumer.py
@skills/cairo-genizah-research/scripts/search.py
@skills/cairo-genizah-research/scripts/browse.py
@skills/cairo-genizah-research/scripts/fixtures/browse_pgp_full.json
@skills/cairo-genizah-research/scripts/fixtures/browse_snippet.json
@skills/cairo-genizah-research/scripts/fixtures/browse_no_image.json
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Shelfmark normalizer (Tier 1)</name>
  <files>skills/cairo-genizah-research/scripts/normalize_shelfmark.py</files>
  <read_first>
    - tests/test_skill_consumer.py (the 3 normalize_shelfmark tests)
    - .planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md (§5 normalization patterns; T-S, ENA-MS, MS Heb c examples)
    - .planning/phases/81B-claude-skill-consumer/81B-CONTEXT.md (SKILL-05 explicitly says "Skill does NOT depend on genizah_core")
  </read_first>
  <behavior>
    - Test: `normalize("T-S  12.123") == normalize("T-S 12.123")` (whitespace collapse).
    - Test: `normalize("MS T-S 12.123") == normalize("T-S 12.123")` (MS prefix strip).
    - Test: `normalize(normalize(s)) == normalize(s)` for all inputs (idempotent).
    - Edge: handles unicode (NFKC normalize), trims, uppercases for case-insensitive comparison.
  </behavior>
  <action>
    Implement Tier-1 lightweight normalizer per RESEARCH §5 with idempotency guarantee:

    ```python
    """Tier-1 lightweight shelfmark normalization for known-witness comparison.

    Best-effort, intentionally simple. Edge cases (paired-leaf bifolios, library
    code prefixes) drop to Tier 2 — `/api/search?search_mode=shelfmark` resolution
    invoked by the model from SKILL.md instructions, NOT this script.

    Per SKILL-05: this skill does NOT depend on genizah_core. All normalization
    logic is intentionally duplicated for portability to systems without the
    GenizahSearch repo.
    """
    from __future__ import annotations
    import re
    import unicodedata

    _MS_PREFIX_RE = re.compile(r"^(MS\.?\s+|MS_)", re.IGNORECASE)
    _MULTI_WS_RE = re.compile(r"\s+")
    _PUNCT_PAD_RE = re.compile(r"\s*([.\-])\s*")

    def normalize(s: str) -> str:
        """Return a normalized shelfmark string for case-insensitive comparison.

        Idempotent: normalize(normalize(s)) == normalize(s) for all str s.
        """
        if not s:
            return ""
        s = unicodedata.normalize("NFKC", s).strip()
        s = _MS_PREFIX_RE.sub("", s)
        s = _MULTI_WS_RE.sub(" ", s)
        s = _PUNCT_PAD_RE.sub(r"\1", s)
        return s.upper()
    ```

    Verify idempotency manually before running the test: `normalize("MS T-S  12.123")` → first pass strips `"MS "` → `"T-S  12.123"` → collapses whitespace → `"T-S 12.123"` → strips space-around-punct → `"T-S 12.123"` (already collapsed) → upper → `"T-S 12.123"`. Second pass: `"T-S 12.123"` → no MS prefix → no multi-ws → no padded punct → upper → `"T-S 12.123"`. Identical. ✓
  </action>
  <verify>
    <automated>pytest tests/test_skill_consumer.py -v -k "normalize_shelfmark"</automated>
  </verify>
  <acceptance_criteria>
    - `pytest tests/test_skill_consumer.py -k "normalize_shelfmark"` reports 3 passed.
    - `grep "^def normalize" skills/cairo-genizah-research/scripts/normalize_shelfmark.py` returns 1 line.
    - File contains `unicodedata.normalize("NFKC", ...)` call: `grep "NFKC" skills/cairo-genizah-research/scripts/normalize_shelfmark.py` returns ≥1.
    - File does NOT import from `genizah_core` or `shared`: `grep -E "from (genizah_core|shared)" skills/cairo-genizah-research/scripts/normalize_shelfmark.py` returns 0 lines.
    - Idempotency manually verified: `python -c "import sys; sys.path.insert(0, 'skills/cairo-genizah-research'); from scripts.normalize_shelfmark import normalize; assert normalize(normalize('MS T-S  12.123')) == normalize('MS T-S  12.123'); print('OK')"` prints `OK`.
  </acceptance_criteria>
  <done>3 normalize_shelfmark tests GREEN. SKILL-05 Tier 1 ready.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Honesty annotations + known-witness policy + output rendering</name>
  <files>skills/cairo-genizah-research/scripts/format_output.py</files>
  <read_first>
    - tests/test_skill_consumer.py (the 6 honesty_annotation tests + 3 known-witness policy tests)
    - skills/cairo-genizah-research/scripts/fixtures/browse_pgp_full.json
    - skills/cairo-genizah-research/scripts/fixtures/browse_snippet.json
    - skills/cairo-genizah-research/scripts/fixtures/browse_no_image.json
    - .planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md (§4 honesty annotation; R2 text_source mapping; Open Q8 ranking output schema)
    - .planning/phases/79-api-browse-drill-down/79-CONTEXT.md (D-10 text_source enum: pgp_transcription | snippet | none)
    - .planning/phases/81B-claude-skill-consumer/81B-CONTEXT.md (D-07 inline-note + continue; D-11 result-shape handling)
  </read_first>
  <behavior>
    - All 6 honesty_annotation tests GREEN (pgp_full → empty, snippet → text warning, char count match, no_image → image warning, text_source=none → text warning, R2 mapping locked).
    - All 3 known-witness policy tests GREEN (flag, exclude, unknown raises ValueError).
    - render_markdown produces output containing the SC-2 schema fields: shelfmark, library, catalog title, tier, known_witness flag, matching phrases, justification, browse URL, image URL or "(no image available)".
  </behavior>
  <action>
    ```python
    """Output formatting for the cairo-genizah-research skill.

    Three public functions:
    - honesty_annotation(browse_response): SKILL-04 — append honest disclaimers
      when full text or image is unavailable.
    - apply_known_witness_policy(candidates, known_uids, policy): SKILL-05 —
      'flag' marks; 'exclude' drops.
    - render_markdown(candidates, base_url): SKILL-02 — Markdown output with
      the SC-2 ranking schema (shelfmark, library, catalog title, tier,
      known-witness flag, matching phrases, justification, browse URL,
      image URL or '(no image available)').

    R2 mapping (locked): REQUIREMENTS.md SKILL-04 says `text_source != 'full'`
    triggers honesty annotation. The locked Phase 79 D-10 API enum is
    `pgp_transcription | snippet | none` — there is NO 'full' value. This module
    treats `text_source == 'pgp_transcription'` as the equivalent-of-full value;
    every other value triggers the annotation. See Plan 01's
    test_honesty_annotation_maps_pgp_transcription_as_full_per_R2.
    """
    from __future__ import annotations
    import json
    import sys
    from typing import Any, Iterable

    # Phase 79 D-10 enum value treated as "full text available".
    # REQUIREMENTS.md SKILL-04 prose says 'full'; this is the canonical mapping.
    _FULL_TEXT_SOURCE = "pgp_transcription"

    def honesty_annotation(browse_response: dict) -> str:
        """Return one or both honesty disclaimers, joined by space, or empty string.

        Triggers:
        - text_source != 'pgp_transcription' → '(full text unavailable; based on snippet of N chars)'
        - image.url is None AND image.sources is empty → '(no image available)'

        Both can fire on the same response.
        """
        parts: list[str] = []
        text_source = (browse_response or {}).get("text_source", "none")
        if text_source != _FULL_TEXT_SOURCE:
            text = (browse_response or {}).get("text", "") or ""
            n = len(text)
            parts.append(f"(full text unavailable; based on snippet of {n} chars)")
        image = (browse_response or {}).get("image") or {}
        url = image.get("url")
        sources = image.get("sources") or []
        if not url and not sources:
            parts.append("(no image available)")
        return " ".join(parts)

    def apply_known_witness_policy(
        candidates: list[dict],
        known_uids: set[str] | Iterable[str],
        policy: str = "flag",
    ) -> list[dict]:
        """Apply known-witness policy to a candidate list.

        - policy='flag': mark each candidate with `known_witness: bool`, keep all.
        - policy='exclude': drop candidates whose uid is in known_uids.
        Raises ValueError for unknown policy.
        """
        known = set(known_uids)
        if policy == "exclude":
            return [c for c in candidates if c.get("uid") not in known]
        if policy == "flag":
            out = []
            for c in candidates:
                marked = dict(c)
                marked["known_witness"] = c.get("uid") in known
                out.append(marked)
            return out
        raise ValueError(f"Unknown known_witness_policy: {policy!r}. Expected 'flag' or 'exclude'.")

    def _browse_url(base_url: str, candidate: dict) -> str:
        """Build a /browse URL from candidate locator."""
        loc = candidate.get("locator") or {}
        sys_id = loc.get("sys_id") or candidate.get("sys_id")
        if not sys_id:
            return ""
        return f"{base_url.rstrip('/')}/browse?sys_id={sys_id}"

    def render_markdown(candidates: list[dict], base_url: str = "https://genizahsearch.com") -> str:
        """Render ranked candidates as Markdown per SC-2 schema.

        Each candidate must have: uid, locator, score (or aggregate_score),
        shelfmark, title, metadata (library, library_name), _tier, _matched_phrases,
        _justification (added by the model from SKILL.md instructions),
        _browse_response (so we can compute honesty + image URL).
        """
        lines: list[str] = []
        lines.append(f"# Cairo Genizah candidates — {len(candidates)} result(s)\n")
        for i, c in enumerate(candidates, 1):
            shelfmark = c.get("shelfmark", "(unknown shelfmark)")
            md = c.get("metadata") or {}
            library = md.get("library_name") or md.get("library", "(unknown library)")
            title = c.get("title", "")
            tier = c.get("_tier", "?")
            known_flag = " 🔖 known witness" if c.get("known_witness") else ""
            phrases = c.get("_matched_phrases", []) or []
            justification = c.get("_justification", "(no justification produced)")
            browse_resp = c.get("_browse_response") or {}
            honesty = honesty_annotation(browse_resp)
            browse_url = _browse_url(base_url, c)
            image = (browse_resp.get("image") or {}) if browse_resp else {}
            img_url = image.get("url")
            image_line = img_url if img_url else "(no image available)"

            lines.append(f"## {i}. {shelfmark} — Tier {tier}{known_flag}")
            lines.append(f"- **Library:** {library}")
            if title:
                lines.append(f"- **Title:** {title}")
            if phrases:
                lines.append(f"- **Matching phrases:** {len(phrases)} ({', '.join(p[:40] + '…' if len(p) > 40 else p for p in phrases[:3])})")
            lines.append(f"- **Justification:** {justification} {honesty}".rstrip())
            lines.append(f"- **Browse:** {browse_url}")
            lines.append(f"- **Image:** {image_line}")
            lines.append("")
        return "\n".join(lines)

    def render_json(candidates: list[dict], base_url: str = "https://genizahsearch.com") -> str:
        """Render ranked candidates as JSON (one object per line, or wrapping array)."""
        out = []
        for c in candidates:
            browse_resp = c.get("_browse_response") or {}
            out.append({
                "uid": c.get("uid"),
                "shelfmark": c.get("shelfmark"),
                "library": (c.get("metadata") or {}).get("library_name"),
                "title": c.get("title"),
                "tier": c.get("_tier"),
                "known_witness": c.get("known_witness", False),
                "matched_phrases": c.get("_matched_phrases", []),
                "justification": c.get("_justification", ""),
                "honesty_annotation": honesty_annotation(browse_resp),
                "browse_url": _browse_url(base_url, c),
                "image_url": (browse_resp.get("image") or {}).get("url"),
                "locator": c.get("locator"),
            })
        return json.dumps(out, ensure_ascii=False, indent=2)
    ```
  </action>
  <verify>
    <automated>pytest tests/test_skill_consumer.py -v -k "honesty_annotation or known_witness_policy"</automated>
  </verify>
  <acceptance_criteria>
    - 9 tests GREEN: 6 honesty_annotation + 3 known-witness policy.
    - `grep "^def honesty_annotation" skills/cairo-genizah-research/scripts/format_output.py` returns 1 line.
    - `grep "^def apply_known_witness_policy" skills/cairo-genizah-research/scripts/format_output.py` returns 1 line.
    - `grep "^def render_markdown" skills/cairo-genizah-research/scripts/format_output.py` returns 1 line.
    - `grep "^def render_json" skills/cairo-genizah-research/scripts/format_output.py` returns 1 line.
    - `grep "_FULL_TEXT_SOURCE = \"pgp_transcription\"" skills/cairo-genizah-research/scripts/format_output.py` returns 1 line (R2 mapping constant).
    - `grep "R2" skills/cairo-genizah-research/scripts/format_output.py` returns ≥1 line (mapping documented).
    - `grep "(full text unavailable; based on snippet of" skills/cairo-genizah-research/scripts/format_output.py` returns 1 line — exact string from SKILL-04.
    - `grep "(no image available)" skills/cairo-genizah-research/scripts/format_output.py` returns ≥2 lines (annotation + render_markdown fallback).
    - `grep -E "from (genizah_core|shared)" skills/cairo-genizah-research/scripts/format_output.py` returns 0 lines (SKILL-05 portability).
  </acceptance_criteria>
  <done>9 honesty + policy tests GREEN. SKILL-04 + SKILL-05 (Tier 1) + ranking output schema all delivered.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Staged phrase discovery orchestrator</name>
  <files>skills/cairo-genizah-research/scripts/stage.py</files>
  <read_first>
    - tests/test_skill_consumer.py (the 3 merge_by_uid tests: aggregates phrase count, tier A/B/C, sort order)
    - skills/cairo-genizah-research/scripts/search.py (call_search signature)
    - .planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md (§3 staged phrase discovery + merge-by-uid pseudocode)
    - .planning/phases/77-serializer-json-export/77-CONTEXT.md (D-04 uid + locator both always populated)
  </read_first>
  <behavior>
    - Test: 3 phrase result lists with uid `U1` in 2, `U2` in 1 → merge returns 2 candidates, U1 has _phrase_count=2, U2 has _phrase_count=1.
    - Test: uid in 3+ phrase results → _tier='A'; in 2 → 'B'; in 1 → 'C'.
    - Test: result sorted by _phrase_count desc, then score desc.
    - CLI smoke: `python stage.py --phrase "ויאמר" --phrase "משה" --search-mode exact --limit 5` produces a JSON list to stdout containing merged candidates.
  </behavior>
  <action>
    ```python
    """Staged phrase discovery orchestrator (SKILL-02).

    The model in SKILL.md authors 2-4 distinctive phrases from the user's query,
    then invokes `python scripts/stage.py --phrase A --phrase B --phrase C ...`.
    This script:
      1. Calls /api/search once per phrase (sequentially, throttle-respecting).
      2. Merges results by uid; counts how many phrases each uid matched.
      3. Assigns tier A (≥3 phrases), B (2), C (1).
      4. Sorts: phrase count desc, then score desc.
      5. Emits merged candidate list as JSON to stdout.

    The model then picks top-N (default 10) and invokes browse.py per candidate.
    """
    from __future__ import annotations
    import argparse
    import json
    import sys
    from typing import Any

    try:
        from . import _config
        from .search import call_search
    except ImportError:
        import os
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")
        from scripts import _config  # type: ignore
        from scripts.search import call_search  # type: ignore


    def merge_results(per_phrase_results: list[list[dict]]) -> list[dict]:
        """Merge multiple /api/search result lists by uid.

        Each item must carry `uid` (Phase 77 D-13 guarantees populated). Returns
        merged candidates with `_phrase_count`, `_matched_phrases`, `_tier` set,
        sorted by phrase count desc then score desc.
        """
        by_uid: dict[str, dict] = {}
        for phrase_results in per_phrase_results:
            for item in phrase_results:
                uid = item.get("uid")
                if not uid:
                    continue
                if uid not in by_uid:
                    by_uid[uid] = {**item, "_matched_phrases": [], "_phrase_count": 0}
                by_uid[uid]["_matched_phrases"].append(item.get("snippet", "") or "")
                by_uid[uid]["_phrase_count"] += 1
                # Keep the highest score across phrase passes.
                existing = by_uid[uid].get("score") or 0.0
                new_score = item.get("score") or 0.0
                if new_score > existing:
                    by_uid[uid]["score"] = new_score

        candidates = list(by_uid.values())
        for c in candidates:
            n = c["_phrase_count"]
            if n >= 3:
                c["_tier"] = "A"
            elif n == 2:
                c["_tier"] = "B"
            else:
                c["_tier"] = "C"

        candidates.sort(key=lambda c: (-c["_phrase_count"], -(c.get("score") or 0.0)))
        return candidates


    def stage_search(
        phrases: list[str],
        *,
        search_mode: str = "exact",
        limit: int = 50,
        gap: int = 0,
        filters: dict | None = None,
        base_url: str | None = None,
    ) -> dict[str, Any]:
        """Run /api/search once per phrase; merge by uid.

        Returns a dict {candidates: [...], errors: [...], phrase_count: N}.
        Errors per-phrase are collected; merging proceeds with whatever succeeded
        (D-07 per-candidate inline note + continue spirit).
        """
        per_phrase: list[list[dict]] = []
        errors: list[dict] = []
        for phrase in phrases:
            response = call_search(
                query=phrase,
                search_mode=search_mode,
                limit=limit,
                gap=gap,
                filters=filters,
                base_url=base_url,
            )
            if "error" in response:
                errors.append({"phrase": phrase, "error": response["error"]})
                per_phrase.append([])
                continue
            per_phrase.append(response.get("results", []) or [])
        merged = merge_results(per_phrase)
        return {"candidates": merged, "errors": errors, "phrase_count": len(phrases)}


    def _main(argv: list[str] | None = None) -> int:
        p = argparse.ArgumentParser(description="Staged phrase discovery via /api/search fan-out + merge-by-uid")
        p.add_argument("--phrase", action="append", required=True,
                       help="Repeat for each distinctive phrase (2-4 typical).")
        p.add_argument("--search-mode", default="exact",
                       choices=["exact", "variants", "regex", "responsa", "title", "shelfmark"])
        p.add_argument("--limit", type=int, default=50)
        p.add_argument("--gap", type=int, default=0)
        p.add_argument("--filters-json", default=None)
        p.add_argument("--base-url", default=None)
        args = p.parse_args(argv)
        filters = json.loads(args.filters_json) if args.filters_json else None
        result = stage_search(
            phrases=args.phrase,
            search_mode=args.search_mode,
            limit=args.limit,
            gap=args.gap,
            filters=filters,
            base_url=args.base_url,
        )
        json.dump(result, sys.stdout, ensure_ascii=False)
        sys.stdout.write("\n")
        return 0


    if __name__ == "__main__":
        sys.exit(_main())
    ```
  </action>
  <verify>
    <automated>pytest tests/test_skill_consumer.py -v -k "merge_by_uid"</automated>
  </verify>
  <acceptance_criteria>
    - 3 merge_by_uid tests GREEN.
    - `grep "^def merge_results" skills/cairo-genizah-research/scripts/stage.py` returns 1 line.
    - `grep "^def stage_search" skills/cairo-genizah-research/scripts/stage.py` returns 1 line.
    - `grep "_phrase_count" skills/cairo-genizah-research/scripts/stage.py` returns ≥2 lines (set + read).
    - `grep "_tier" skills/cairo-genizah-research/scripts/stage.py` returns ≥3 lines (A, B, C branches).
    - `grep "from .search import call_search\|from scripts.search import call_search" skills/cairo-genizah-research/scripts/stage.py` returns ≥1 line.
    - `grep -E "from (genizah_core|shared)" skills/cairo-genizah-research/scripts/stage.py` returns 0 lines.
    - Full Plan 03 test pass: `pytest tests/test_skill_consumer.py -v` reports 15 passed.
    - Wider suite (including throttle): `pytest tests/test_skill_consumer.py tests/test_skill_throttle.py` reports 22 passed.
  </acceptance_criteria>
  <done>All 15 SKILL-02/04/05 RED tests GREEN. Stage script callable as CLI (the model invokes it from SKILL.md instructions).</done>
</task>

</tasks>

<threat_model>

## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| `_browse_response` field on candidate dict | Comes from `/api/browse`; trusted to match Phase 79 envelope shape; defensively guarded with `.get(...) or {}` patterns. |
| `known_witnesses[]` user input | Strings; pass through `normalize` before set membership check; no injection vector (not used in SQL or HTTP body). |
| Per-phrase `/api/search` errors | Collected into `errors[]`; merging proceeds with successful phrases; partial failure mode honored (D-07). |
| `_justification` field on candidate | Authored by the model in SKILL.md instructions, not by this script. R9 mitigation: SKILL.md explicitly instructs "if text_source != 'pgp_transcription', justification MUST be solely about the snippet's match." |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-81B-10 | Information Disclosure | `render_markdown` echoes browse text + metadata | accept | All data already public via the API; skill only renders what server returned. |
| T-81B-11 | Tampering | Malformed `metadata` / `image` dicts crash render | mitigate | All field accesses use `(d or {}).get(...)` defensive pattern; tests cover null subobjects. |
| T-81B-12 | Repudiation | Justification could hallucinate beyond browse text (R9) | mitigate | Honesty annotation is the safety net — when text_source != pgp_transcription, the disclaimer makes the partial-evidence basis explicit. SKILL.md instructions reinforce. Plan 04 owns the SKILL.md prose. |
| T-81B-13 | Denial of Service | Pathological phrase list (1000 entries) fans out 1000 search calls | mitigate | Throttle from Plan 02 caps at 24 rpm; SKILL.md instructions cap phrases at 4. CLI accepts arbitrary count but the model is instructed to limit. Server-side rate limit is the authoritative ceiling. |

</threat_model>

<verification>
- `pytest tests/test_skill_consumer.py -v` reports 15 passed.
- `pytest tests/test_skill_consumer.py tests/test_skill_throttle.py -v` reports 22 passed.
- Wider suite (excluding skill_smoke): `pytest -k "not skill_smoke"` reports 1465 (baseline) + 22 (skill) = 1487 passed / 15 skipped.
- `python -c "from skills.cairo_genizah_research.scripts.stage import merge_results, stage_search; from skills.cairo_genizah_research.scripts.format_output import honesty_annotation, apply_known_witness_policy, render_markdown, render_json; from skills.cairo_genizah_research.scripts.normalize_shelfmark import normalize; print('OK')"` prints `OK`.
- Smoke CLI: `python skills/cairo-genizah-research/scripts/stage.py --phrase "ויאמר" --search-mode exact --limit 2` returns valid JSON with `candidates`, `errors`, `phrase_count` keys.
</verification>

<success_criteria>
- All 15 RED tests in `tests/test_skill_consumer.py` GREEN.
- Combined with Plan 02: 22 total skill tests GREEN (15 consumer + 7 throttle).
- R2 mapping (text_source `pgp_transcription` → "full") locked in `_FULL_TEXT_SOURCE` constant + dedicated test.
- SKILL-04 (browse honesty), SKILL-05 (known-witness policy + Tier-1 normalize), SKILL-02 (merge-by-uid + tier assignment) requirements all delivered in code.
- Skill source tree contains zero imports from `genizah_core` or `shared/*` (SKILL-05 portability lock).
- SC-2 ranking output schema (shelfmark, library, catalog title, tier, known-witness flag, matching phrases, justification, browse URL, image URL or "(no image available)") fully surfaced by `render_markdown`.
</success_criteria>

<output>
After completion, create `.planning/phases/81B-claude-skill-consumer/81B-03-SUMMARY.md`:
- Files created, test results (15/15 GREEN)
- R2 mapping confirmation (constant name + test name)
- Any deviations (e.g., field renames in stage.py output)
- Confirmation that Plan 04's SKILL.md instructions can reference these script CLIs verbatim
</output>
