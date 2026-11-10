"""Staged phrase discovery orchestrator (SKILL-02).

The model in SKILL.md authors 2-4 distinctive phrases from the user's query,
then invokes `python scripts/stage.py --phrase A --phrase B --phrase C ...`.
This script:
  1. Calls /api/search once per phrase (sequentially, throttle-respecting).
  2. Merges results by uid; counts how many phrases each uid matched.
  3. Assigns tier A (>=3 phrases), B (2), C (1).
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
    from .search import call_search
except ImportError:
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")
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
    p = argparse.ArgumentParser(
        description="Staged phrase discovery via /api/search fan-out + merge-by-uid"
    )
    p.add_argument(
        "--phrase", action="append", required=True,
        help="Repeat for each distinctive phrase (2-4 typical).",
    )
    p.add_argument(
        "--search-mode", default="exact",
        choices=["exact", "variants", "responsa", "title", "shelfmark"],
    )
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
