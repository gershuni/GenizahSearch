#!/usr/bin/env python
"""List what in a built shelf tree a reader would find incoherent (shelf tree picker plan §10.15).

Reads ``shelfmark_tree.db`` (read-only) and prints, per kind, how many places have it and the
first examples:

- other:        shelfmarks under "Other shelfmarks" (a library's) or a collection's own leaves
                beside its volumes: not in any volume;
- unspecified:  shelfmarks in a volume FGP calls "Unspecified volume";
- twins:        siblings whose names differ only in case or punctuation ("OR", "Or.");
- order:        volumes of one series (a name without its last number) out of their numbers'
                order, Roman numerals read as numbers ("Moss. IX" before "Moss. II"); the series
                themselves stand in FGP's order;
- same_label:   shelfmarks of one node that show the same label;
- mixed_form:   shelfmarks written in another form than most of their node's ("Ms. Evr. Antonin B"
                among "Yevr. III B").

Exit 0 always: an audit to read, not a gate. ``--limit`` sets the examples per kind.
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.build_shelfmark_tree import (  # noqa: E402
    _match_tokens,
    _series,
    form_of,
    name_matches,
    roman_natural_key,
    sibling_order_keys,
    split_form,
    twin_key,
)


def audit(db_path: str) -> dict:
    con = sqlite3.connect(f"file:{Path(db_path).resolve().as_posix()}?mode=ro", uri=True)
    try:
        nodes = {r[0]: r for r in con.execute(
            "SELECT id, parent_id, kind, name_en, sort_key, direct_leaves, child_nodes, manuscripts FROM node")}
        # a catalogue's list (Oxford by Neubauer-Cowley number) is a second way in, not the shelf
        nodes = {nid: r for nid, r in nodes.items() if not nid.startswith("cat:")}
        kids = defaultdict(list)
        for nid, parent, *_rest in nodes.values():
            kids[parent].append(nid)
        for k in kids:
            kids[k].sort(key=lambda n: nodes[n][4])
        leaves = defaultdict(list)
        for node_id, label in con.execute("SELECT node_id, label FROM leaf WHERE how <> 'catalogue' ORDER BY pos"):
            leaves[node_id].append(label)
    finally:
        con.close()

    def path(nid):
        out = []
        while nid:
            out.append(nodes[nid][3])
            nid = nodes[nid][1]
        return " › ".join(reversed(out))

    found = defaultdict(list)
    for nid, (_i, parent, kind, name, _s, direct, children, _m) in nodes.items():
        if kind == "other":
            under = sum(len(leaves[x]) for x in _below(nid, kids)) + len(leaves[nid])
            found["other"].append((under, path(nid)))
        elif kind == "collection" and direct and any(nodes[k][2] in ("volume", "group") for k in kids[nid]):
            found["other"].append((direct, path(nid) + " (beside its volumes)"))
        if kind == "volume" and name.strip().lower() == "unspecified volume":
            found["unspecified"].append((direct, path(nid)))
        if leaves[nid]:
            # one series written two ways is one form ("Fr. 27/26" in Lehmann's "Ms. FR")
            forms = Counter(_series(form_of(lb)) for lb in leaves[nid])
            main, n = forms.most_common(1)[0]
            odd = [lb for lb in leaves[nid] if _series(form_of(lb)) != main]
            if odd and n >= 3:
                usual = Counter(form_of(lb) for lb in leaves[nid] if _series(form_of(lb)) == main).most_common(1)[0][0]
                found["mixed_form"].append((len(odd), f"{path(nid)}: {odd[0]!r} among {usual!r}"))
    for nid, (_i, parent, kind, name, _s, direct, children, _m) in nodes.items():
        labels = leaves[nid]
        # a volume's shelfmarks that do not begin with its name ("Ms. Gaster 1785.1" in "Gaster heb. ms 1785")
        if kind == "volume" and labels and any(ch.isdigit() for ch in name):
            nt = _match_tokens(name)
            off = [lb for lb in labels if not name_matches(nt, _match_tokens(lb))]
            if off:
                found["not_under_name"].append((len(off), f"{path(nid)}: {off[0]!r}"))
        # shelfmarks out of natural order inside one shelf
        if len(labels) >= 3:
            keys = [roman_natural_key(lb) for lb in labels]
            if kind == "volume":
                # the builder's order: a volume's own shelfmarks first, the few under another name after
                own = [bool(name_matches(_match_tokens(name), _match_tokens(lb))) for lb in labels]
                if sum(own) * 2 >= len(own):
                    keys = [(not o, k) for o, k in zip(own, keys)]
            bad = [i for i in range(1, len(keys)) if keys[i] < keys[i - 1]]
            if bad and kind == "group":
                # a series is in the order of the numbers after it, however it is written
                keys = [(roman_natural_key(split_form(lb)[1]), roman_natural_key(lb)) for lb in labels]
                bad = [i for i in range(1, len(keys)) if keys[i] < keys[i - 1]]
            if bad:
                found["leaf_order"].append((len(bad), f"{path(nid)}: {labels[bad[0] - 1]!r} before {labels[bad[0]]!r}"))
        # names a reader would stumble on
        if kind in ("collection", "volume", "group") and (
                name.count("(") != name.count(")") or name.count("[") != name.count("]")
                or re.search(r"[/(\-,;:]$|^[\W_]", name) or re.search(r"unspecified|unclassified|unknown", name, re.I)):
            found["odd_name"].append((direct + 0, path(nid)))
        same = sum(n for n in Counter(labels).values() if n > 1)
        if same:
            found["same_label"].append((same, f"{path(nid)}: {Counter(labels).most_common(1)[0][0]!r}"))
        if kind == "collection":
            vols = [k for k in kids[nid] if nodes[k][2] == "volume"]
            single = [k for k in vols if len(leaves[k]) == 1 and not kids[k]]
            if len(single) >= 5:
                found["single_volumes"].append((len(single), f"{path(nid)}: {len(single)} of {len(vols)} volumes "
                                                f"hold one shelfmark, e.g. {nodes[single[0]][3]!r}"))
    for parent, ids in kids.items():
        names = [nodes[k][3] for k in ids if nodes[k][2] in ("collection", "volume", "group")]
        seen = defaultdict(list)
        for name in names:
            seen[twin_key(name)].append(name)
        for twins in (v for v in seen.values() if len(v) > 1):
            found["twins"].append((len(twins), f"{path(parent) if parent else '(top)'}: {twins}"))
        vols = [nodes[k][3] for k in ids if nodes[k][2] == "volume"]
        keys = sibling_order_keys(vols)
        series = defaultdict(list)                 # a name without its last number -> its volumes, as shown
        for v in vols:
            last = max((j for j, part in enumerate(keys[v]) if part[0] == 0), default=None)
            if last is not None:
                series[keys[v][:last] + keys[v][last + 1:]].append(v)
        for run in series.values():
            bad = [b for a, b in zip(run, run[1:]) if keys[b] < keys[a]]
            if bad:
                found["order"].append((len(run), f"{path(parent)}: {bad[0]!r} out of place"))
    return found


def _below(nid, kids):
    out = []
    for k in kids.get(nid, ()):
        out.append(k)
        out += _below(k, kids)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("db")
    ap.add_argument("--limit", type=int, default=12)
    args = ap.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8")
    found = audit(args.db)
    for kind in ("other", "unspecified", "twins", "order", "mixed_form", "not_under_name", "leaf_order",
                 "odd_name", "single_volumes", "same_label"):
        items = sorted(found.get(kind, []), key=lambda x: -x[0])
        print(f"== {kind}: {len(items)} places, {sum(n for n, _ in items):,} items")
        for n, text in items[:args.limit]:
            print(f"   {n:>6,}  {text}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
