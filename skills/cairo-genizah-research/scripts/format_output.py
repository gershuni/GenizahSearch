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
from typing import Iterable

# Phase 79 D-10 enum value treated as "full text available".
# REQUIREMENTS.md SKILL-04 prose says 'full'; this is the canonical R2 mapping.
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
    known_uids: "set[str] | Iterable[str]",
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
        known_flag = " known witness" if c.get("known_witness") else ""
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
            lines.append(
                f"- **Matching phrases:** {len(phrases)} "
                f"({', '.join(p[:40] + '...' if len(p) > 40 else p for p in phrases[:3])})"
            )
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
