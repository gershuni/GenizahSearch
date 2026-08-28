# -*- coding: utf-8 -*-
"""Resolving a WITNESS from the corpus -- pure, dependency-free.

A multi-witness search needs to turn three different things into searchable
text: a row on screen, a manuscript the reader promoted, and a witness list
coming back out of a saved session. Every function here was written at module
level inside `web/pages/parallels.py` specifically so the RULES could be
tested rather than their plumbing -- an AST assertion cannot tell
`for header in headers` from `for header in []`, and one of these rules was
proven vacuous exactly that way.

They live here now because the DESKTOP needs all five and must not import from
`web/`. The web page re-exports them, so its own callers and tests are
unchanged. Same rules, one definition, two surfaces -- the arrangement
`shared/passage_fusion.py` already uses for the arithmetic.

Nothing here does I/O of its own: the two fetchers are injected, so a caller
supplies Tantivy on the web, the desktop `SearchEngine` on the desktop, and a
dict in a test.
"""
from __future__ import annotations

from shared.sys_id_patterns import CORPUS_SYS_ID_RE

# The seed text is a witness like any other and is fused under this id.
WITNESS_SEED_ID = 'seed'

# Mirrors SEARCH_API_PASSAGE_MAX_WITNESSES. A ceiling on how many witnesses a
# surface may hold; how many it may SEARCH at once is per-surface policy (the
# web scales it by depth because it shares a timeout and four slots with every
# other visitor, the desktop does not).
WITNESS_CAP = 25

WITNESS_SYS_ID_RE = CORPUS_SYS_ID_RE


def witness_sys_id(row) -> str:
    r"""The sys_id a result row belongs to. THE one copy, for both surfaces.

    `shared.sys_id_patterns.CORPUS_SYS_ID_RE`, the single definition every
    corpus-facing site in the repo shares (PR #330). An earlier note here
    argued for a WIDER pattern, one that also admitted a 97 prefix. Both
    halves of that argument were wrong:

    * **97 is not a corpus prefix at all.** It is the LOCAL "My Library"
      namespace (Phase 95, `shared/local_sys_id.py`) -- a user's own files,
      generated on the DESKTOP, never a Genizah record. Measured on
      `libraries.csv`: 255,723 of 255,723 corpus records begin 99.
    * **Wide was not the safe direction.** `re.search` scans anywhere, so a
      corpus pattern run over a LOCAL header can match a 99 INSIDE the LOCAL
      id's own digits and return a truncated, WRONG sys_id (6.36% of LOCAL
      ids, measured). The shared constant is anchored on a digit boundary so
      a LOCAL header misses cleanly instead.

    That second point bites harder here than on the web, because this module
    is the DESKTOP's copy too and the desktop is where 97 ids actually exist.

    Do not re-widen and do not hand-roll a second pattern; both are enforced
    by tests/test_sys_id_patterns.py.
    """
    m = WITNESS_SYS_ID_RE.search((row or {}).get('raw_header') or '')
    return m.group(1) if m else None


def restore_witness_entries(raw, default_label: str, cap: int = None) -> list:
    """Normalise a tab snapshot's witness list back into witness dicts.

    Pure, module level and dependency-injected (`default_label` is the only
    thing it would otherwise need `tr()` for) so the RULES are testable.
    A mutation sweep proved they were not: reverting the drop rule below to
    the obvious `if not text.strip()` -- which deletes every restored
    manuscript witness -- left the entire page suite green.

    Three rules:

    * **A manuscript witness with a `sys_id` survives without text.** The
      snapshot drops its text deliberately (the corpus still has it) and
      `witnesses_needing_text` / the rehydrator put it back before dispatch.
      Dropping it here would quietly shrink a restored 17-witness search.
    * **Anything else with no text is dropped**, because nothing in the world
      can recover it and a witness that cannot be searched must not sit in
      the list pretending otherwise.
    * **Ids are renumbered `w1..wN` over the SURVIVORS.** Reusing the stored
      ids would leave gaps that `_witness_new_id` could then re-issue, and
      two witnesses sharing an id corrupt the per-witness row cache.

    Every witness comes back `pending`: the snapshot holds the FUSED rows, so
    per-witness ranks cannot be recovered, and a fusion rebuilt from partial
    inputs would be quietly wrong rather than visibly absent.

    Returns the list; the caller assigns it and resets the row caches.
    """
    if not isinstance(raw, list) or not raw:
        return []
    out = []
    for entry in raw[:(cap if cap is not None else WITNESS_CAP)]:
        if not isinstance(entry, dict):
            continue
        kind = 'manuscript' if entry.get('kind') == 'manuscript' else 'pasted'
        text = str(entry.get('text') or '')
        sys_id = entry.get('sys_id')
        if not text.strip() and not (kind == 'manuscript' and sys_id):
            continue
        out.append({
            'id': f'w{len(out) + 1}',
            'label': str(entry.get('label') or '') or (sys_id or default_label),
            'kind': kind,
            'sys_id': sys_id,
            'seed_digest': str(entry.get('seed_digest') or ''),
            'headers': [str(h) for h in (entry.get('headers') or []) if h],
            'text': text,
            'status': 'pending',
            'hits': 0,
            'error': '',
        })
    return out


def witnesses_needing_text(pending) -> list:
    """Which pending witnesses have no text to search and can get one back.

    `_persist_active_snapshot` stores a MANUSCRIPT witness without its text on
    purpose -- the corpus still has it, and copying up to 25 x 20,000 chars of
    corpus text into a tab snapshot buys nothing. Nothing re-fetched it on
    restore, so after a reload those witnesses searched the empty string and
    reported `searched, 0 matches`: a false negative indistinguishable from a
    real one. (Found by review, not by any test here.)

    Module level and pure so the RULE is tested rather than its plumbing. A
    pasted witness is never included -- its text existed nowhere but the
    snapshot, so there is nothing to re-fetch -- and neither is one with no
    `sys_id` to fetch by. Both are refused at dispatch instead.
    """
    return [w for w in (pending or [])
            if w.get('kind') == 'manuscript'
            and w.get('sys_id')
            and not (w.get('text') or '').strip()]


def witness_headers_for(sys_ids, rows) -> dict:
    """Which page headers make up each promoted manuscript's witness text.

    Extracted from `collect_witness_texts` so the promotion can RECORD its
    choice. A promoted witness is not a deterministic function of its
    `sys_id`: it is the concatenation of the pages that MATCHED, which is a
    property of the result set on screen at that moment. Re-deriving it later
    from a different result set yields a different witness under the same
    label.
    """
    wanted = set(sys_ids)
    headers_by_sid: dict = {}
    for row in rows or []:
        sid = witness_sys_id(row)
        if sid in wanted and row.get('raw_header'):
            headers_by_sid.setdefault(sid, []).append(row['raw_header'])
    return headers_by_sid


def collect_witness_texts(sys_ids, rows, fetch_header,
                          fetch_manuscript=None, headers_by_sid=None):
    """Gather the text to search a promoted manuscript WITH.

    Module level and dependency-injected so it can be tested without building
    a page: the AST tests that covered this logic in the closure were proven
    vacuous against the exact bug it was written to fix -- a source-text
    assertion cannot tell `for header in headers` from `for header in []`.

    Two rules, both learned the hard way:

    * **The matched pages' own `raw_header`s are the PRIMARY source.** The
      first version used `get_full_manuscript(sys_id)`, which resolves through
      `Config.BROWSE_MAP` -- an auxiliary pickle with no guarantee of holding
      an arbitrary manuscript. Owner-reported: every promotion failed, because
      that map held two entries. `fetch_header` is the same fetcher the engine
      just used to render those rows, so it cannot fail for a row on screen.
    * **Every matched page, not the best one.** A result GROUP spans several
      page-level hits; one page is usually a fraction of the witness.

    `fetch_manuscript` (optional) is the whole-manuscript fallback, tried only
    when no header resolves.

    Returns `(texts_by_sys_id, failed_sys_ids)` -- failures are RETURNED, not
    logged and dropped, so the caller can name them once instead of emitting
    one anonymous toast per manuscript.
    """
    # Caller-supplied headers win: on a REHYDRATE those are the headers the
    # promotion actually used, and re-deriving them from whatever rows are on
    # screen now would rebuild a different witness under the same label.
    if headers_by_sid is None:
        headers_by_sid = witness_headers_for(sys_ids, rows)

    out, failed = {}, []
    for sid in sys_ids:
        parts = []
        for header in (headers_by_sid.get(sid) or []):
            try:
                page = fetch_header(header)
            except Exception:
                page = None
            if page:
                parts.append(page)
        text = "\n".join(parts).strip()
        if not text and fetch_manuscript is not None:
            try:
                pages = fetch_manuscript(sid) or []
            except Exception:
                pages = []
            text = "\n".join(p.get('text') or '' for p in pages).strip()
        if text:
            out[sid] = text
        else:
            failed.append(sid)
    return out, failed
