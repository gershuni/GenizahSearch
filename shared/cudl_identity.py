# -*- coding: utf-8 -*-
"""Which Cambridge Digital Library (CUDL) item a catalogue record names.

A shelfmark and a CUDL label name the same item when they have the same letters
and numbers in the same order: separators, spacing, case and zero padding aside.
``T-S Ar.18(1).6`` and ``MS-TS-AR-00018-00001-00006`` do; ``T-S NS 318.41`` and
``MS-TS-NS-00318-00004-00001`` (T-S NS 318.4.1) do not, nor do ``T-S F 6.1`` and
``MS-TS-00006-F-00001`` (T-S 6F1). That comparison is the whole test of identity
here: a candidate whose structure differs from the name is never accepted, however
it was found.

Candidates come from the sidecar's ``cambridge_manifests`` table. Its
``normalized_shelfmark`` column is lossy (it drops every other dot of a numeric
run, and parentheses), so many labels share a key -- but two labels with the same
structure always share one, because the key is a function of the structure. So one
indexed query on the key computed from the NAME finds every label that can match,
and the structural comparison picks among them. No new column, no rebuild.

Record level (``resolve_record``): only CUL and Mosseri records are Cambridge's.
The record's main name decides; its other names decide only when the main name
names nothing. A name that matches several items decides nothing. A link from the
NLI catalogue (MARC 856) is used when the names name nothing, and to choose among
a name's candidates; it never overrides a name's unique match.

Every resolution carries ``RESOLVER_REVISION``. Callers that cache one (the NLI
cache, the image routes) treat an entry with another revision as not resolved, and the
browser URLs of Cambridge images carry it (``cv=``). Bump it when the rules change OR
when ``cambridge_manifests`` is refreshed: either can move a record to another item.
"""
from __future__ import annotations

import re
from typing import Callable, Iterable, List, Optional, Sequence, Tuple

#: Bump when the same data can resolve differently. Cached resolutions with another
#: revision are recomputed; image caches keyed on it are bypassed.
RESOLVER_REVISION = 1

#: The library codes whose records may link to CUDL.
CAMBRIDGE_LIBRARY_CODES = frozenset({'CUL', 'Mosseri'})

#: Mosseri series CUDL publishes (MS-MOSSERI-<series>-...).
_MOSSERI_SERIES = frozenset({'I', 'IA', 'II', 'III', 'IIIA', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X'})

_ALLOWED_CHARS = re.compile(r"[A-Za-z0-9 .,/()\-]+")
_NUMERIC_RANGE = re.compile(r"\d\s*-\s*\d")
_COLLECTION_START = re.compile(r"(?:T-S|Or\b|Add\b|Moss\b)", re.IGNORECASE)
_MOSSERI_NAME = re.compile(
    r"^(?:Mosseri\s*,?\s*(?:Jacques\s+)?)?(?:Ms\.?|Moss\.?)\s*([IVX]+A?)\s*[,\s]\s*(.+)$",
    re.IGNORECASE,
)
_CUDL_LABEL_IN_URL = re.compile(r"/(?:view|iiif)/(MS-[A-Z0-9-]+?)(?:/\d+)?/?$", re.IGNORECASE)

Signature = Tuple[object, ...]
Candidate = Tuple[str, str]  # (label, manifest_url)


def cudl_label_to_shelfmark(label: str) -> str:
    """Convert a CUDL manifest label to a human-readable shelfmark.

    This is the form ``scripts/import_nli_crossref.py`` normalizes into
    ``cambridge_manifests.normalized_shelfmark``, so it lives here, beside the
    lookup that has to reproduce that key.

    Steps:
    1. Strip 'MS-' prefix
    2. Split by '-'
    3. Strip leading zeros from numeric segments (e.g., '00006' -> '6')
    4. Rejoin: '.' between consecutive numeric parts, space otherwise

    Examples:
        MS-TS-00016-00114 -> 'TS 16.114'
        MS-ADD-00863-00002 -> 'ADD 863.2'
        MS-MOSSERI-II-00292-00002 -> 'MOSSERI II 292.2'
    """
    s = label
    if s.startswith("MS-"):
        s = s[3:]

    parts = s.split("-")
    cleaned = []
    for p in parts:
        if p.isdigit():
            cleaned.append(str(int(p)))  # strip leading zeros
        else:
            cleaned.append(p)

    if not cleaned:
        return label

    result = cleaned[0]
    for i in range(1, len(cleaned)):
        prev_numeric = cleaned[i - 1].isdigit()
        curr_numeric = cleaned[i].isdigit()
        if prev_numeric and curr_numeric:
            result += "." + cleaned[i]
        else:
            result += " " + cleaned[i]

    return result


def _signature(tokens: Iterable[str]) -> Optional[Signature]:
    """Letters merged between numbers (so ``Ja`` == ``J-A``), numbers as integers.

    A name without numbers ("T-S NS J. Minute Fragments") still has a structure: its
    letters, which must then match a label's letters exactly."""
    out: List[object] = []
    for tok in tokens:
        if tok.isdigit():
            out.append(int(tok))
        elif out and isinstance(out[-1], str):
            out[-1] += tok.upper()
        else:
            out.append(tok.upper())
    return tuple(out) or None


def label_signature(label: str) -> Optional[Signature]:
    """Structure of a CUDL label: ``MS-TS-00008-J-A-00002`` -> ('TS', 8, 'JA', 2)."""
    if not label or not label.upper().startswith('MS-'):
        return None
    return _signature(t for t in label[3:].split('-') if t)


def _generic_tokens(name: str) -> Optional[List[str]]:
    s = name.strip()
    if ':' in s:
        # A collection heading ("T-S Boxes: T-S K25.240", "Or. 1080–1081: Or.1080 127")
        # is dropped only when what follows is itself a shelfmark.
        tail = s.rsplit(':', 1)[1].strip()
        if not _COLLECTION_START.match(tail):
            return None
        s = tail
    s = re.sub(r"(?i)^cambridge university library\s+", "", s)
    s = re.sub(r"(?i)^ms\.?\s+", "", s)
    if not s or not _ALLOWED_CHARS.fullmatch(s) or _NUMERIC_RANGE.search(s):
        return None
    s = re.sub(r"(?i)\bT-S\b", "TS", s)
    return re.findall(r"[A-Za-z]+|\d+", s)


def _mosseri_tokens(name: str) -> Optional[List[str]]:
    m = _MOSSERI_NAME.match(name.strip())
    if not m:
        return None
    series, rest = m.group(1).upper(), m.group(2).strip()
    if series not in _MOSSERI_SERIES:
        return None
    if not _ALLOWED_CHARS.fullmatch(rest) or _NUMERIC_RANGE.search(rest):
        return None
    return ['MOSSERI', series] + re.findall(r"[A-Za-z]+|\d+", rest)


def _name_tokens(name: str, library_code: Optional[str]) -> Optional[List[str]]:
    if not name:
        return None
    if library_code == 'Mosseri':
        return _mosseri_tokens(name)
    if library_code == 'CUL':
        return _generic_tokens(name)
    # No library known (a bare shelfmark from a script or a search box).
    return _mosseri_tokens(name) or _generic_tokens(name)


def name_signature(name: str, library_code: Optional[str] = None) -> Optional[Signature]:
    """Structure of a catalogue name, or None when it is not a form we can read whole.

    Anything outside letters, digits and the separators ``space . , / ( ) -`` makes
    the name unreadable (``318.41β``, ``318.4+1``), as does a numeric range
    (``12.186-188``): a name we can only read in part names nothing.
    """
    tokens = _name_tokens(name, library_code)
    return _signature(tokens) if tokens else None


def _readable(sig: Signature) -> str:
    parts = [str(t) for t in sig]
    out = parts[0]
    for prev, cur, tok in zip(sig, sig[1:], parts[1:]):
        out += ('.' if isinstance(prev, int) and isinstance(cur, int) else ' ') + tok
    return out


def candidate_key(sig: Signature) -> str:
    """The ``normalized_shelfmark`` every label with this structure carries."""
    from shared.browse_map_utils import normalize_shelfmark
    return normalize_shelfmark(_readable(sig))


def guess_label(name: str, library_code: Optional[str] = None) -> Optional[str]:
    """The CUDL label spelled the way the name is written (``T-S Ar.18(1).6`` ->
    ``MS-TS-AR-00018-00001-00006``). Only a second way in to the table: the key
    query already finds it, unless the sidecar's keys were built differently."""
    tokens = _name_tokens(name, library_code)
    if not tokens:
        return None
    return 'MS-' + '-'.join(t.zfill(5) if t.isdigit() else t.upper() for t in tokens)


def label_from_url(url: str) -> Optional[str]:
    """``https://cudl.lib.cam.ac.uk/view/MS-TS-00001-00001/2`` -> ``MS-TS-00001-00001``."""
    if not url or 'cudl.lib.cam.ac.uk' not in url.lower():
        return None
    m = _CUDL_LABEL_IN_URL.search(url.strip())
    return m.group(1).upper() if m else None


def viewer_url(label: str) -> str:
    return f"https://cudl.lib.cam.ac.uk/view/{label}"


def manifest_url_for(label: str) -> str:
    return f"https://cudl.lib.cam.ac.uk/iiif/{label}"


def match_name(name: str, library_code: Optional[str],
               fetch: Callable[[str, Optional[str]], Sequence[Candidate]]) -> dict:
    """One name -> ``{'status': 'match'|'ambiguous'|'none', 'label', 'manifest_url', 'labels'}``.

    ``fetch(key, guess)`` returns the (label, manifest_url) rows whose key is ``key``
    or whose label is ``guess``.
    """
    sig = name_signature(name, library_code)
    if sig is None:
        return {'status': 'none', 'label': None, 'manifest_url': None, 'labels': []}
    matches = {}
    for label, url in fetch(candidate_key(sig), guess_label(name, library_code)):
        if label_signature(label) == sig:
            matches[label] = url
    if len(matches) == 1:
        label, url = next(iter(matches.items()))
        return {'status': 'match', 'label': label, 'manifest_url': url, 'labels': [label]}
    status = 'ambiguous' if matches else 'none'
    return {'status': status, 'label': None, 'manifest_url': None, 'labels': sorted(matches)}


def resolution_is_current(res) -> bool:
    """True when ``res`` is a final resolution made by this revision of the rules."""
    return bool(res) and res.get('revision') == RESOLVER_REVISION


def cached_decision_stale(meta_mgr, sys_id) -> bool:
    """True when ``meta_mgr``'s cached entry for ``sys_id`` has no current Cambridge
    decision -- made before these rules, or while the answer was unavailable. Every
    shortcut that serves cached external images (``images_ext``) without re-running
    enrich_metadata must treat such an entry as cold: it may hold another item's images.
    A metadata manager without the check (a test stub) is taken as current."""
    check = getattr(meta_mgr, 'cambridge_resolution_current', None)
    return callable(check) and not check(sys_id)


def unavailable() -> dict:
    """No answer yet (catalogue still loading, sidecar unreadable). Carries no
    revision, so nothing caches it as final."""
    res = _result('unavailable')
    res['revision'] = None
    res['marc_label'] = None
    return res


def _result(status, label=None, manifest_url=None, source=None, name=None, alternatives=()):
    return {
        'revision': RESOLVER_REVISION,
        'status': status,
        'label': label,
        'manifest_url': manifest_url,
        'viewer_url': viewer_url(label) if label else None,
        'source': source,
        'matched_name': name,
        'alternatives': list(alternatives),
        'several_items': False,
    }


def _resolve_record(names: Sequence[str], library_code: Optional[str],
                   fetch: Callable[[str, Optional[str]], Sequence[Candidate]],
                   marc_url: Optional[str] = None) -> dict:
    """The CUDL item a catalogue record links to.

    ``names``: the record's names, main name first. Statuses:
      ``match``         -- ``label`` is the item (``source`` 'name' or 'marc')
      ``none``          -- no name names an item, and no catalogue link
      ``ambiguous``     -- the deciding name matches several items
      ``not_cambridge`` -- not a CUL/Mosseri record
    When the main name names nothing and the other names name different items, the
    first of them in catalogue order is the ``match`` and ``several_items`` is True.
    ``alternatives`` lists items the record's other names name: kept, never shown in
    place of the selected one.
    """
    if library_code not in CAMBRIDGE_LIBRARY_CODES:
        return _result('not_cambridge')

    marc_label = label_from_url(marc_url) if marc_url else None
    seen = set()
    per_name = []
    for name in names:
        sig = name_signature(name, library_code)
        if sig is None or sig in seen:
            continue
        seen.add(sig)
        per_name.append((name, match_name(name, library_code, fetch)))

    def alternatives(selected):
        alts, done = [], {selected}
        for name, res in per_name:
            if res['status'] == 'match' and res['label'] not in done:
                done.add(res['label'])
                alts.append({'label': res['label'], 'viewer_url': viewer_url(res['label']), 'name': name})
        return alts

    # The main name decides when it names exactly one item -- or several, one of
    # which the NLI catalogue links.
    if per_name and per_name[0][0] == (names[0] if names else None):
        name, res = per_name[0]
        if res['status'] == 'match':
            return _result('match', res['label'], res['manifest_url'], 'name', name,
                           alternatives(res['label']))
        if res['status'] == 'ambiguous':
            if marc_label in res['labels']:
                return _result('match', marc_label, manifest_url_for(marc_label), 'marc', name,
                               alternatives(marc_label))
            return _result('ambiguous')

    found = {}
    for name, res in per_name:
        if res['status'] == 'match':
            found.setdefault(res['label'], (res['manifest_url'], name))
    if len(found) == 1:
        label, (url, name) = next(iter(found.items()))
        return _result('match', label, url, 'name', name)
    if len(found) > 1:
        if marc_label in found:
            url, name = found[marc_label]
            return _result('match', marc_label, url, 'marc', name, alternatives(marc_label))
        # Every one of them is a verified part of this record (a Mosseri second-series
        # record made of several first-series items: "Ms. P 59 II" = II.259.4-7). The
        # first in catalogue order is linked, as the apps did before this resolver --
        # Mosseri records have no NLI images to fall back on -- and flagged, with the
        # others as alternatives, so a chooser can offer them all.
        label, (url, name) = next(iter(found.items()))
        res = _result('match', label, url, 'name', name, alternatives(label))
        res['several_items'] = True
        return res

    if marc_url:
        # The names name nothing; the NLI catalogue's own link stands.
        if marc_label:
            return _result('match', marc_label, manifest_url_for(marc_label), 'marc')
        res = _result('match', None, None, 'marc')
        res['viewer_url'] = marc_url.replace('/iiif/', '/view/')
        return res
    if any(res['status'] == 'ambiguous' for _name, res in per_name):
        return _result('ambiguous')
    return _result('none')


def resolve_record(names: Sequence[str], library_code: Optional[str],
                   fetch: Callable[[str, Optional[str]], Sequence[Candidate]],
                   marc_url: Optional[str] = None) -> dict:
    """See ``_resolve_record``; the result also carries ``marc_label``, the item the
    NLI catalogue links (None without one), so a disagreement stays visible."""
    res = _resolve_record(names, library_code, fetch, marc_url=marc_url)
    res['marc_label'] = label_from_url(marc_url) if marc_url else None
    return res
