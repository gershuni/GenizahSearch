#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Offline bake for the Phase 133 Visual Atlas Preview (ATLAS-01).

Reads the canon-masked research aggregation (table ``accepted_pairs_canonmask``
in a gitignored research DB) + ``libraries.csv`` (our own catalogue data) +
``fist_data/fjms_enrichment.db`` (FJMS domain classifications), aggregates
page-pairs into manuscript-pairs, clusters the same-work ("continuation")
graph with Louvain (seeded, deterministic), homes every remaining
island-only-connected manuscript in its own micro-cluster via the SAME
force-layout/dust-ring code path (closing the node-inclusion gap -- see
``docs/specs/atlas-asset-schema-v1.md`` and 133-CONTEXT.md D-09), and encodes
the result as a typed-array + string-heap binary payload per that frozen
schema, Brotli-compressed, with a content-hashed filename and a manifest.

This is a BAKE-TIME-ONLY tool (never imported by the running web app). Its
extra dependencies (networkx, python-louvain, Brotli) are pinned in
``requirements-atlas-bake.txt``, NOT ``requirements.txt``/``requirements-lock.txt``.

Emits NO discovery-overlay fields (no gold candidates, no discovery counts,
no identification claims) -- D-04. Never reads or mentions the restricted
reference corpus by its real name; this script's own data sources
(``libraries.csv``, ``fjms_enrichment.db``, the canon-masked pair table) are
all masking-safe by construction.

Usage:
    python scripts/build_atlas_asset.py <db_path> [--out-dir DIR] [--report]
    python scripts/build_atlas_asset.py --smoke 200 [--report] [--out-dir DIR]
    python scripts/build_atlas_asset.py --golden tests/fixtures/atlas/golden-v1.bin

``db_path`` is required UNLESS ``--smoke`` or ``--golden`` is given (both
build a small deterministic SYNTHETIC in-memory graph -- never derived from
real research data -- so the bake pipeline and its tests can run without the
~2.9 GB gitignored research DB).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
import struct
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np

# Bake-time-only deps -- see requirements-atlas-bake.txt. Never added to
# requirements.txt / requirements-lock.txt: this tooling never runs inside
# the web process.
import community as community_louvain  # python-louvain
import networkx as nx
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

try:
    import brotli
except ImportError:  # pragma: no cover - exercised only if the bake-time dep is absent
    brotli = None

REPO_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Deterministic bake parameters (recorded in the manifest -- see §8 of the
# frozen schema doc)
# ---------------------------------------------------------------------------
SEED = 42
ALGO_VERSION = "louvain-force-phyllotaxis-v1"
SPLIT_AT = 800          # Louvain-decompose components bigger than this
TOPK_LAYOUT = 1400      # communities force-laid-out; tail -> peripheral dust ring
LINK_CAP = 3000         # inter-cluster aggregate flows kept
LABEL_MIN_N = 25        # clusters below this size get no label (matches the layout's own legibility floor)
CONT_MIN_CLUSTER = 2    # continuation components are always >= 2 (an edge implies 2 endpoints) -- kept for parity/clarity
ISLAND_MIN_CLUSTER = 1  # RESOLVED decision: island-only components are NEVER dropped, incl. singletons
BYTE_BUDGET_CAP = 6_000_000  # D-10 / PERF-01 preview byte cap (Brotli-compressed)
REGRESSION_FLOOR = 62_414  # historical node-count floor for a REAL research-DB bake (D-09)

_SYS_ID_MAX = 2 ** 64
_PURE_DIGIT_RE = re.compile(r"^[0-9]+$")

# ---------------------------------------------------------------------------
# FJMS domain-group taxonomy (13 fixed groups). This is OUR OWN subject-matter
# classification (Bible/Piyyut/Halakha/...), not the restricted reference
# corpus -- masking-safe. Order is fixed; the index IS the NODE_DOMAIN /
# CLUSTER_LABEL_DGRP value (see schema §4).
# ---------------------------------------------------------------------------
DOMAIN_GROUPS = [
    ("Bible", "מקרא", "#4ea3ff"),
    ("Exegesis & Tafsir", "פרשנות ותפסיר", "#2ec8e6"),
    ("Piyyut", "פיוט", "#a06bff"),
    ("Liturgy", "תפילה וברכות", "#ffd35e"),
    ("Poetry", "שירה", "#ff5ed0"),
    ("Talmud & Midrash", "תלמוד ומדרש", "#c98a4b"),
    ("Halakha", "הלכה", "#8fe65e"),
    ("Documents & Letters", "תעודות ומכתבים", "#e8e8e8"),
    ("Thought & Kabbalah", "הגות וקבלה", "#ff7d5e"),
    ("Sciences & Medicine", "מדעים ורפואה", "#5effd0"),
    ("Philology", "בלשנות ומילונאות", "#9aa7ff"),
    ("Belles Lettres", "סיפורת", "#d4ff5e"),
    ("Other / Unidentified", "אחר", "#77808f"),
]
_OTHER_GROUP_IDX = len(DOMAIN_GROUPS) - 1
_KEYWORD_GROUPS = [
    ("tafsir", "Exegesis & Tafsir"), ("exegesis", "Exegesis & Tafsir"),
    ("piyyut", "Piyyut"), ("piyut", "Piyyut"), ("secular poetry", "Poetry"),
    ("liturgy", "Liturgy"), ("prayer", "Liturgy"),
    ("bible", "Bible"), ("masorah", "Bible"), ("massorah", "Bible"),
    ("halakh", "Halakha"), ("responsa", "Halakha"),
    ("talmud", "Talmud & Midrash"), ("mishnah", "Talmud & Midrash"),
    ("rabbinic", "Talmud & Midrash"), ("midrash", "Talmud & Midrash"),
    ("derashot", "Talmud & Midrash"),
    ("letters", "Documents & Letters"), ("document", "Documents & Letters"),
    ("lists", "Documents & Letters"),
    ("philosoph", "Thought & Kabbalah"), ("theolog", "Thought & Kabbalah"),
    ("kabbalah", "Thought & Kabbalah"), ("kalam", "Thought & Kabbalah"),
    ("ethical", "Thought & Kabbalah"), ("polemic", "Thought & Kabbalah"),
    ("science", "Sciences & Medicine"), ("medicine", "Sciences & Medicine"),
    ("astronomy", "Sciences & Medicine"), ("occult", "Sciences & Medicine"),
    ("predicting", "Sciences & Medicine"),
    ("philolog", "Philology"), ("glossar", "Philology"),
    ("stories", "Belles Lettres"), ("belles", "Belles Lettres"),
]
_GROUP_NAME_TO_IDX = {name: i for i, (name, _, _) in enumerate(DOMAIN_GROUPS)}


def _group_of(domain: str, parent: str) -> int:
    """Classify a raw FJMS (Domain, ParentDomain) pair into one of the 13
    fixed DOMAIN_GROUPS indices via keyword matching. Falls back to
    'Other / Unidentified'."""
    txt = f"{domain or ''} {parent or ''}".casefold()
    for kw, grp in _KEYWORD_GROUPS:
        if kw in txt:
            return _GROUP_NAME_TO_IDX[grp]
    return _OTHER_GROUP_IDX


# ---------------------------------------------------------------------------
# sys_id validation -- BigUint64-only, no fallback (Codex NEW LOW; schema §7)
# ---------------------------------------------------------------------------

def validate_sys_id(sys_id) -> int:
    """Validate the single bake-time sys_id invariant: pure-digit, < 2**64.
    Returns the value as a Python int. Raises ValueError on ANY violation --
    there is no fallback representation; an invalid sys_id FAILS the bake."""
    if isinstance(sys_id, bool):
        raise ValueError(f"invalid sys_id (bool, not an integer): {sys_id!r}")
    if isinstance(sys_id, int):
        value = sys_id
    elif isinstance(sys_id, str):
        if not _PURE_DIGIT_RE.match(sys_id):
            raise ValueError(f"invalid sys_id (not pure-digit): {sys_id!r}")
        value = int(sys_id)
    else:
        raise ValueError(f"invalid sys_id (unsupported type {type(sys_id).__name__}): {sys_id!r}")
    if value < 0 or value >= _SYS_ID_MAX:
        raise ValueError(f"invalid sys_id (out of BigUint64 range [0, 2**64)): {value!r}")
    return value


# ---------------------------------------------------------------------------
# Data loaders -- real research-DB / libraries.csv / fjms_enrichment.db path
# ---------------------------------------------------------------------------

def load_lib_meta(root: Path) -> dict:
    """libraries.csv -> {sys_id_str: (shelfmark, library_code, catalogue_title)}.
    Our own catalogue data (masking-safe)."""
    meta = {}
    csv_path = root / "libraries.csv"
    import csv as _csv
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        r = _csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 4 and row[0]:
                variants = [v.strip() for v in (row[2] or "").split("|") if v.strip()]
                title = row[7].strip() if len(row) >= 8 else ""
                meta[row[0]] = (
                    variants[0] if variants else row[0],
                    (row[3] or "").strip() or "?",
                    title,
                )
    return meta


def load_domains(fjms_db_path: Path) -> dict:
    """fjms_enrichment.db domains table -> {sys_id_str: (Counter[group_idx], Counter[label_text])}."""
    con = sqlite3.connect(f"file:{fjms_db_path.as_posix()}?mode=ro", uri=True)
    out: dict = {}
    try:
        for alma, dom, dom_he, par, par_he in con.execute(
            "SELECT AlmaId, Domain, DomainHeb, ParentDomain, ParentDomainHeb FROM domains"
        ):
            rec = out.get(alma)
            if rec is None:
                rec = out[alma] = (Counter(), Counter())
            rec[0][_group_of(dom, par)] += 1
            label = dom_he or dom
            if label and label not in ("לא מזוהה", "Unidentified"):
                rec[1][label] += 1
    finally:
        con.close()
    return out


def load_ms_pairs_from_db(db_path: str, table: str = "accepted_pairs_canonmask") -> dict:
    """Aggregate canon-masked page-pairs -> manuscript-pairs.
    Returns {(sys_a, sys_b) with sys_a < sys_b: [n, best_len, cont_count, isl_count]}."""
    con = sqlite3.connect("file:" + Path(db_path).as_posix() + "?mode=ro", uri=True)
    try:
        rows = con.execute(
            f"SELECT sys_a, sys_b, aligned_len, density, flank_class FROM {table} "
            "WHERE dup_shelf = 0 AND dup_lines < 0.6"
        ).fetchall()
    finally:
        con.close()
    ms_pairs: dict = defaultdict(lambda: [0, 0, 0, 0])
    for sa, sb, alen, _dens, fc in rows:
        key = (sa, sb) if sa < sb else (sb, sa)
        r = ms_pairs[key]
        r[0] += 1
        r[1] = max(r[1], alen or 0)
        if fc in ("continuation", "edge"):
            r[2] += 1
        elif fc == "island":
            r[3] += 1
    return dict(ms_pairs)


def _fjms_db_path(root: Path) -> Path:
    return root / "fist_data" / "fjms_enrichment.db"


def hash_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


# ---------------------------------------------------------------------------
# Synthetic dataset -- for --smoke / --golden. NEVER derived from real
# research data; purely fabricated sys_ids/shelfmarks/titles for pipeline
# validation (masking-safe by construction).
# ---------------------------------------------------------------------------

# > 2**53 (Number.MAX_SAFE_INTEGER) so the sys_id BigUint64 pathway is
# actually exercised by every synthetic bake; still comfortably < 2**64.
_SYNTHETIC_BASE_SYS_ID = 990_000_000_000_000_000
_SYNTHETIC_TITLES = [
    "Piyyut fragment for the Sabbath", "Bible commentary leaf", "Halakhic responsum",
    "Liturgical poem for festivals", "Talmudic gloss", "",
]
_SYNTHETIC_LIBS = ["CUL", "JTS", "RNL", "Oxford", "BL"]
# A deliberately fabricated, hostile-SHAPED (never real) catalogue string --
# gives the downstream 133-04 DOM-XSS decode test a synthetic fixture. Never
# derived from any real manuscript title.
_SYNTHETIC_MALICIOUS_TITLE = "<img src=x onerror=alert(1)></script>‮-fake-evil-title"


def synthetic_dataset(n: int, seed: int = SEED, malicious: bool = False, n_island: Optional[int] = None):
    """Build a small, deterministic, fabricated ms_pairs/meta/domains dataset.

    Guarantees:
      - at least one MULTI-node island-only connected component (a chain with
        zero continuation edges of its own) and one TRUE SINGLETON island-only
        component (a node whose only relation is an island-classified pair to
        a continuation-graph node) -- both are node-inclusion-gap edge cases.
      - at least one sys_id > 2**53 (all of them, in fact) but < 2**64.
      - if malicious=True, one node's title carries a fabricated XSS-shaped
        string (see _SYNTHETIC_MALICIOUS_TITLE) -- synthetic, never M-source.

    Returns (ms_pairs, sys_meta, domains, sys_ids).
    """
    if n_island is None:
        n_island = max(2, n // 10)
    n_island = max(2, n_island)
    n_cont = max(1, n - n_island)

    sys_ids = [_SYNTHETIC_BASE_SYS_ID + i for i in range(n_cont + n_island)]

    sys_meta = {}
    domains = {}
    for i, s in enumerate(sys_ids):
        shelfmark = f"SYN {i:04d}.{(i % 7) + 1}"
        title = _SYNTHETIC_TITLES[i % len(_SYNTHETIC_TITLES)]
        lib = _SYNTHETIC_LIBS[i % len(_SYNTHETIC_LIBS)]
        sys_meta[s] = (shelfmark, lib, title)
        grp = i % len(DOMAIN_GROUPS)
        group_counter = Counter({grp: 1})
        label_counter = Counter({DOMAIN_GROUPS[grp][1]: 1}) if title else Counter()
        domains[s] = (group_counter, label_counter)

    if malicious:
        evil_id = sys_ids[-1]
        shelfmark, lib, _old_title = sys_meta[evil_id]
        sys_meta[evil_id] = (shelfmark, lib, _SYNTHETIC_MALICIOUS_TITLE)

    ms_pairs: dict = {}

    def _add(a, b, n_, best_len, cont, isl):
        key = (a, b) if a < b else (b, a)
        ms_pairs[key] = [n_, best_len, cont, isl]

    cont_ids = sys_ids[:n_cont]
    for i in range(1, len(cont_ids)):
        _add(cont_ids[i - 1], cont_ids[i], 3, 40 + i, 3, 0)
    for i in range(0, max(0, len(cont_ids) - 4), 4):
        _add(cont_ids[i], cont_ids[i + 3], 2, 30, 2, 0)

    island_ids = sys_ids[n_cont:]
    chain_ids = island_ids[:-1]
    singleton_id = island_ids[-1]
    for i in range(1, len(chain_ids)):
        _add(chain_ids[i - 1], chain_ids[i], 2, 15, 0, 2)
    if cont_ids:
        # true singleton: its ONLY relation is an island-classified pair to a
        # continuation-graph node, so it has no edge to any other island-only
        # node and forms its own 1-node island component.
        _add(singleton_id, cont_ids[0], 1, 10, 0, 1)

    return ms_pairs, sys_meta, domains, sys_ids


# Canonical input for the COMMITTED golden fixture. Shared by the CLI
# --golden path and the encoder-output-lock test (MEDIUM-1) so the two can
# never drift. n=40 with n_island=6 yields a 34-node continuation component
# (>= LABEL_MIN_N=25) so the cluster-label sections are actually exercised
# (MEDIUM-2), plus the island chain + true-singleton edge cases and the
# fabricated XSS-shaped title (malicious=True).
_GOLDEN_N = 40
_GOLDEN_N_ISLAND = 6


def golden_dataset():
    """Build the exact synthetic dataset the committed golden fixture is baked
    from (deterministic, fabricated, masking-safe -- never M-source)."""
    return synthetic_dataset(_GOLDEN_N, seed=SEED, malicious=True, n_island=_GOLDEN_N_ISLAND)


# ---------------------------------------------------------------------------
# Recursive Louvain split (oversized components -> legible sub-regions, D-03)
# ---------------------------------------------------------------------------

def split_recursive(members: list, adj: dict, split_at: int = SPLIT_AT, depth: int = 0) -> list:
    if len(members) <= split_at or depth >= 3:
        return [members]
    g = nx.Graph()
    mset = set(members)
    g.add_nodes_from(members)
    for a in members:
        for b, w in adj.get(a, ()):
            if b in mset and a < b:
                g.add_edge(a, b, weight=w)
    part = community_louvain.best_partition(g, random_state=SEED)
    groups = defaultdict(list)
    for s, c in part.items():
        groups[c].append(s)
    if len(groups) <= 1:
        return [members]
    out = []
    for grp in groups.values():
        out.extend(split_recursive(grp, adj, split_at, depth + 1))
    return out


# ---------------------------------------------------------------------------
# Core bake result
# ---------------------------------------------------------------------------

@dataclass
class BakeResult:
    nodes: list                 # [x, y, cluster_idx, domain_idx, library_idx, prominence, sys_id(int), title, shelfmark]
    edges: list                 # [node_idx_a, node_idx_b, cls] sorted (source asc, target asc)
    clusters: list               # per-cluster dict: {n, dgrp, title, dom, members}
    cluster_labels: list         # filtered subset (n >= LABEL_MIN_N): {ci, x, y, r, n, dgrp, title, dom}
    flows: list                  # [source_ci, target_ci, weight]
    eligible_count: int
    placed_count: int
    missing: list                # sys_id ints present in eligible but not placed (must be empty)
    extra: list                  # sys_id ints present in placed but not eligible (must be empty)
    libraries: list               # bake-discovered library codes, index-matched to node[4]
    seed: int = SEED
    algo_version: str = ALGO_VERSION


def _ms_group(sys_id, domains: dict) -> int:
    rec = domains.get(sys_id)
    if not rec or not rec[0]:
        return _OTHER_GROUP_IDX
    top = rec[0].most_common(2)
    if top[0][0] == _OTHER_GROUP_IDX and len(top) > 1:
        return top[1][0]
    return top[0][0]


# ---------------------------------------------------------------------------
# sys_id canonicalization -- HIGH-2. Pair endpoints, graph node keys,
# libraries.csv metadata keys and FJMS domain keys can arrive as a MIX of
# `str` and `int` (sqlite TEXT columns vs INTEGER columns, libraries.csv is
# always text). Left unnormalized, `eligible` (canonicalized to int) never
# matches the raw graph/metadata keys, so lookups silently miss (losing
# title/domain/library) and set operations produce phantom duplicate/extra
# nodes. Everything is reduced to the SINGLE canonical int representation
# (validate_sys_id) BEFORE the bake so every lookup and set op uses one type.
# ---------------------------------------------------------------------------

def _canonicalize_ms_pairs(ms_pairs: dict) -> dict:
    """Reduce every pair endpoint to its canonical int sys_id. An invalid
    endpoint FAILS the bake (validate_sys_id raises) -- pair endpoints are
    eligible nodes. Re-orders each key by INT value (string order != int
    order) and merges any pairs that collapse onto the same canonical
    endpoints (n summed, best_len maxed, cont/island summed)."""
    out: dict = defaultdict(lambda: [0, 0, 0, 0])
    for (a, b), r in ms_pairs.items():
        ca, cb = validate_sys_id(a), validate_sys_id(b)
        key = (ca, cb) if ca <= cb else (cb, ca)
        acc = out[key]
        acc[0] += r[0]
        acc[1] = max(acc[1], r[1] or 0)
        acc[2] += r[2]
        acc[3] += r[3]
    return dict(out)


def _canonicalize_meta(meta: dict) -> dict:
    """Re-key metadata (libraries.csv titles / FJMS domains) by the canonical
    int sys_id so lookups match the canonicalized pair endpoints. Metadata for
    a non-conforming sys_id (one that could never be an eligible node) is
    dropped rather than failing the whole bake -- only pair endpoints are hard
    invariants."""
    out: dict = {}
    for k, v in meta.items():
        try:
            ck = validate_sys_id(k)
        except ValueError:
            continue
        out[ck] = v
    return out


def run_bake(ms_pairs: dict, sys_meta: dict, domains: dict, *, seed: int = SEED,
             split_at: int = SPLIT_AT, label_min_n: int = LABEL_MIN_N) -> BakeResult:
    """Pure function: clusters + lays out + validates the bake. Raises
    ValueError if any eligible sys_id fails the BigUint64 pure-digit-<2**64
    invariant (no fallback -- see validate_sys_id / schema §7)."""

    # ---- HIGH-2: canonicalize EVERY sys_id to its single int representation ----
    # (pair endpoints, graph node keys, and libraries.csv/domain metadata keys)
    # so lookups and set operations all use one type. Invalid pair endpoints
    # FAIL the bake; non-conforming metadata keys are dropped (they can never
    # match an eligible node).
    ms_pairs = _canonicalize_ms_pairs(ms_pairs)
    sys_meta = _canonicalize_meta(sys_meta)
    domains = _canonicalize_meta(domains)

    # ---- eligible set = every sys_id in ANY manuscript-pair relation ----
    # (keys are already canonical ints after _canonicalize_ms_pairs)
    eligible = {s for k in ms_pairs for s in k}

    # ---- continuation ("same-work") backbone ----
    cont_keys = [k for k, r in ms_pairs.items() if r[2] >= max(1, r[3])]
    cont_ids = sorted({s for k in cont_keys for s in k})
    cont_idx = {s: i for i, s in enumerate(cont_ids)}
    if cont_keys:
        ea = np.array([cont_idx[a] for a, b in cont_keys])
        eb = np.array([cont_idx[b] for a, b in cont_keys])
        m = coo_matrix((np.ones(len(ea)), (ea, eb)), shape=(len(cont_ids), len(cont_ids)))
        _, labels = connected_components(m, directed=False)
        comp_members = defaultdict(list)
        for s in cont_ids:
            comp_members[int(labels[cont_idx[s]])].append(s)
    else:
        comp_members = {}

    cont_adj = defaultdict(list)
    for (a, b) in cont_keys:
        w = ms_pairs[(a, b)][0]
        cont_adj[a].append((b, w))
        cont_adj[b].append((a, w))

    clusters: list = []  # list of member-lists
    for members in comp_members.values():
        if len(members) < CONT_MIN_CLUSTER:
            continue
        if len(members) > split_at:
            for grp in split_recursive(members, cont_adj, split_at):
                if len(grp) >= CONT_MIN_CLUSTER:
                    clusters.append(grp)
        else:
            clusters.append(members)

    placed_so_far = {s for c in clusters for s in c}

    # ---- island-only leftover: nodes with ZERO continuation-dominant pairs ----
    island_only_ids = eligible - placed_so_far
    island_adj = defaultdict(list)
    for (a, b), r in ms_pairs.items():
        if a in island_only_ids and b in island_only_ids:
            island_adj[a].append((b, r[0]))
            island_adj[b].append((a, r[0]))

    if island_only_ids:
        io_list = sorted(island_only_ids)
        io_idx = {s: i for i, s in enumerate(io_list)}
        rows_a, rows_b = [], []
        for a in island_only_ids:
            for b, _w in island_adj.get(a, ()):
                if a < b:
                    rows_a.append(io_idx[a])
                    rows_b.append(io_idx[b])
        if rows_a:
            m2 = coo_matrix((np.ones(len(rows_a)), (rows_a, rows_b)),
                            shape=(len(io_list), len(io_list)))
            _, io_labels = connected_components(m2, directed=False)
        else:
            io_labels = np.arange(len(io_list))  # every node its own singleton component
        io_comp_members = defaultdict(list)
        for s in io_list:
            io_comp_members[int(io_labels[io_idx[s]])].append(s)
        for members in io_comp_members.values():
            if len(members) < ISLAND_MIN_CLUSTER:
                continue  # unreachable at ISLAND_MIN_CLUSTER=1, kept for clarity/parity
            if len(members) > split_at:
                for grp in split_recursive(members, island_adj, split_at):
                    if grp:
                        clusters.append(grp)
            else:
                clusters.append(members)

    placed_so_far = {s for c in clusters for s in c}
    missing = sorted(eligible - placed_so_far)
    extra = sorted(placed_so_far - eligible)

    # Deterministic ordering: size descending, tie-broken by the smallest
    # member sys_id (stable regardless of dict/set iteration order or
    # PYTHONHASHSEED -- required for byte-identical determinism across runs).
    clusters.sort(key=lambda c: (-len(c), min(c)))

    # ---- degree (prominence signal) across ALL manuscript-pairs ----
    deg = Counter()
    for (a, b), r in ms_pairs.items():
        deg[a] += r[0]
        deg[b] += r[0]

    cluster_of = {}
    crecs = []
    for ci, members in enumerate(clusters):
        for s in members:
            cluster_of[s] = ci
        grp_cnt = Counter(_ms_group(s, domains) for s in members)
        titles = Counter(t for s in members for t in [sys_meta.get(s, ("", "?", ""))[2]] if t)
        dom_labels = Counter()
        for s in members:
            rec = domains.get(s)
            if rec:
                dom_labels.update(rec[1])
        dgrp = grp_cnt.most_common(1)[0][0]
        if dgrp == _OTHER_GROUP_IDX and len(grp_cnt) > 1:
            nd = [g for g, _ in grp_cnt.most_common(2) if g != _OTHER_GROUP_IDX]
            if nd and grp_cnt[nd[0]] >= max(2, 0.25 * len(members)):
                dgrp = nd[0]
        crecs.append({
            "n": len(members), "dgrp": dgrp, "members": members,
            "title": (titles.most_common(1)[0][0] if titles else ""),
            "dom": (dom_labels.most_common(1)[0][0] if dom_labels else ""),
        })

    # ---- inter-cluster aggregate flows ----
    links_agg = defaultdict(lambda: [0, 0])
    for (a, b), r in ms_pairs.items():
        ca, cb = cluster_of.get(a), cluster_of.get(b)
        if ca is None or cb is None or ca == cb:
            continue
        key = (ca, cb) if ca < cb else (cb, ca)
        links_agg[key][0] += r[2]
        links_agg[key][1] += r[3]
    links = sorted(
        ([ci, cj, w[0], w[1]] for (ci, cj), w in links_agg.items()),
        key=lambda x: (-(x[2] + x[3]), x[0], x[1]),
    )[:LINK_CAP]

    # ---- community layout: bounded force over TOP-K, tail as dust ring ----
    n = len(crecs)
    K = min(TOPK_LAYOUT, n)
    R = np.array([min(6 + 3.0 * math.sqrt(c["n"]), 240) for c in crecs]) if n else np.zeros(0)
    rng = np.random.default_rng(seed)
    P = np.zeros((n, 2))
    placed_xy = []
    for oi in range(K):
        r = R[oi]
        if not placed_xy:
            placed_xy.append((0.0, 0.0, r))
            P[oi] = (0, 0)
            continue
        ang, rad = float(rng.uniform(0, 6.28)), placed_xy[0][2] + r + 8
        for _ in range(4000):
            x, y = rad * math.cos(ang), rad * math.sin(ang)
            if all((x - px) ** 2 + (y - py) ** 2 >= (r + pr + 5) ** 2
                   for px, py, pr in placed_xy[-400:]):
                placed_xy.append((x, y, r))
                P[oi] = (x, y)
                break
            ang += 0.5
            rad += 1.4
        else:
            P[oi] = (rad * math.cos(ang), rad * math.sin(ang))

    la = np.array([link[0] for link in links if link[0] < K and link[1] < K], dtype=int)
    lb = np.array([link[1] for link in links if link[0] < K and link[1] < K], dtype=int)
    lw = np.log2(1 + np.array(
        [1 + (link[2] + link[3]) for link in links if link[0] < K and link[1] < K], float))
    if K:
        Pk = P[:K]
        Rk = R[:K]
        for _ in range(220):
            d = Pk[:, None, :] - Pk[None, :, :]
            dist = np.sqrt((d ** 2).sum(-1)) + 1e-6
            touch = Rk[:, None] + Rk[None, :] + 10
            f = np.where(dist < touch * 2.0, (touch * 2.0 - dist) * 0.05, 0.0)
            np.fill_diagonal(f, 0)
            F = (d / dist[..., None] * f[..., None]).sum(1)
            if len(la):
                dv = Pk[lb] - Pk[la]
                dd = np.sqrt((dv ** 2).sum(-1)) + 1e-6
                want = (Rk[la] + Rk[lb]) * 1.3 + 40
                pull = ((dd - want) * 0.004 * (0.5 + lw / 6))[:, None] * dv / dd[:, None]
                np.add.at(F, la, pull)
                np.add.at(F, lb, -pull)
            F -= Pk * 0.001
            Pk += np.clip(F, -18, 18)
        P[:K] = Pk

    if n > K:
        ext = float(np.max(np.abs(P[:K])) + 60) if K else 300.0
        for j in range(K, n):
            ang = (j - K) * 2.399963
            rad = ext * (1.15 + 0.9 * ((j - K) / max(1, n - K)))
            P[j] = (rad * math.cos(ang), rad * math.sin(ang))

    # ---- scatter members as stars (phyllotaxis within each cluster) ----
    nodes = []
    node_idx = {}
    lib_set = set()
    for ci, c in enumerate(crecs):
        cx, cy = P[ci]
        rad = R[ci]
        mem = sorted(c["members"], key=lambda s: (-deg[s], s))
        mcount = len(mem)
        for i, s in enumerate(mem):
            a = i * 2.399963
            rr = rad * 0.95 * math.sqrt((i + 0.5) / mcount)
            jx = float(rng.uniform(-1, 1)) * rad * 0.03
            jy = float(rng.uniform(-1, 1)) * rad * 0.03
            x = cx + rr * math.cos(a) + jx
            y = cy + rr * math.sin(a) + jy
            shelfmark, lib, title = sys_meta.get(s, (str(s), "?", ""))
            lib_set.add(lib)
            node_idx[s] = len(nodes)
            nodes.append([
                round(float(x), 1), round(float(y), 1), ci, _ms_group(s, domains),
                lib, int(min(deg[s], 65535)), int(s), title or "", shelfmark or str(s),
            ])

    libraries = sorted(lib_set)
    lib_pos = {lib: i for i, lib in enumerate(libraries)}
    for node in nodes:
        node[4] = lib_pos[node[4]]  # replace library code text with its index

    # ---- HIGH-3 defense-in-depth: the ENCODED node set must be duplicate-free
    # and exactly equal to the eligible connected-endpoint set. Correct
    # canonicalization (HIGH-2) + island-inclusion (ISLAND_MIN_CLUSTER=1)
    # guarantee this; a violation here is a bake bug, not a recoverable
    # condition, so it raises rather than silently emitting a wrong asset. ----
    encoded_ids = {nd[6] for nd in nodes}
    if len(encoded_ids) != len(nodes):
        raise ValueError(
            f"duplicate node sys_id in encoded output: {len(nodes)} nodes, "
            f"{len(encoded_ids)} unique"
        )
    if encoded_ids != eligible:
        raise ValueError(
            "encoded node-id set does not equal the eligible connected-endpoint "
            f"set (encoded={len(encoded_ids)}, eligible={len(eligible)})"
        )

    # ---- manuscript<->manuscript edges (node-index pairs; class byte) ----
    # EDGE_CLASS polarity is fixed by the FROZEN schema (§4 id 11): 0 =
    # continuation (same-work evidence), 1 = island (citation/quotation). A
    # continuation-DOMINANT pair (r[2] >= max(1, r[3])) is class 0; everything
    # else is class 1. (HIGH-1 — the encoder must match the authoritative
    # schema doc, not the reverse.)
    edges = []
    for (a, b), r in ms_pairs.items():
        ia, ib = node_idx.get(a), node_idx.get(b)
        if ia is None or ib is None:
            continue
        src, tgt = (ia, ib) if ia <= ib else (ib, ia)
        edges.append([src, tgt, 0 if r[2] >= max(1, r[3]) else 1])
    edges.sort(key=lambda e: (e[0], e[1]))

    cluster_labels = [
        {"ci": ci, "x": round(float(P[ci][0]), 1), "y": round(float(P[ci][1]), 1),
         "r": round(float(R[ci]), 1), "n": c["n"], "dgrp": c["dgrp"],
         "title": c["title"], "dom": c["dom"]}
        for ci, c in enumerate(crecs) if c["n"] >= label_min_n
    ]
    flows = [[a, b, float(cw + iw)] for a, b, cw, iw in links]

    return BakeResult(
        nodes=nodes, edges=edges, clusters=crecs, cluster_labels=cluster_labels,
        flows=flows, eligible_count=len(eligible), placed_count=len(placed_so_far),
        missing=missing, extra=extra, libraries=libraries, seed=seed,
    )


# ---------------------------------------------------------------------------
# Binary encoding -- implements docs/specs/atlas-asset-schema-v1.md field-for-
# field. See that document for the frozen contract this code MUST match.
# ---------------------------------------------------------------------------

MAGIC = b"ATLAS001"
SCHEMA_VERSION = 1

DTYPE_FLOAT32 = 1
DTYPE_UINT8 = 2
DTYPE_UINT16 = 3
DTYPE_UINT32 = 4
DTYPE_UINT64 = 5
_NP_DTYPE = {DTYPE_FLOAT32: "<f4", DTYPE_UINT8: "<u1", DTYPE_UINT16: "<u2",
             DTYPE_UINT32: "<u4", DTYPE_UINT64: "<u8"}
_ELEM_SIZE = {DTYPE_FLOAT32: 4, DTYPE_UINT8: 1, DTYPE_UINT16: 2, DTYPE_UINT32: 4, DTYPE_UINT64: 8}

# Section IDs -- schema §4. Order here matches file layout order (not load-bearing
# for decoding since the section table is self-describing, but kept consistent
# for readability).
SEC_NODE_POS = 1
SEC_NODE_CLUSTER = 2
SEC_NODE_DOMAIN = 3
SEC_NODE_LIBRARY = 4
SEC_NODE_PROMINENCE = 5
SEC_NODE_SYS_ID = 6
SEC_NODE_TITLE_REF = 7
SEC_NODE_SHELFMARK_REF = 8
SEC_EDGE_SOURCE_DELTA = 9
SEC_EDGE_TARGET_DELTA = 10
SEC_EDGE_CLASS = 11
SEC_FLOW_SOURCE_CLUSTER = 12
SEC_FLOW_TARGET_CLUSTER = 13
SEC_FLOW_WEIGHT = 14
SEC_CLUSTER_LABEL_CI = 15
SEC_CLUSTER_LABEL_X = 16
SEC_CLUSTER_LABEL_Y = 17
SEC_CLUSTER_LABEL_R = 18
SEC_CLUSTER_LABEL_N = 19
SEC_CLUSTER_LABEL_DGRP = 20
SEC_CLUSTER_LABEL_TITLE_REF = 21
SEC_CLUSTER_LABEL_DOM_REF = 22
SEC_STRING_HEAP = 23

_SECTION_NAMES = {
    SEC_NODE_POS: "NODE_POS", SEC_NODE_CLUSTER: "NODE_CLUSTER",
    SEC_NODE_DOMAIN: "NODE_DOMAIN", SEC_NODE_LIBRARY: "NODE_LIBRARY",
    SEC_NODE_PROMINENCE: "NODE_PROMINENCE", SEC_NODE_SYS_ID: "NODE_SYS_ID",
    SEC_NODE_TITLE_REF: "NODE_TITLE_REF", SEC_NODE_SHELFMARK_REF: "NODE_SHELFMARK_REF",
    SEC_EDGE_SOURCE_DELTA: "EDGE_SOURCE_DELTA", SEC_EDGE_TARGET_DELTA: "EDGE_TARGET_DELTA",
    SEC_EDGE_CLASS: "EDGE_CLASS", SEC_FLOW_SOURCE_CLUSTER: "FLOW_SOURCE_CLUSTER",
    SEC_FLOW_TARGET_CLUSTER: "FLOW_TARGET_CLUSTER", SEC_FLOW_WEIGHT: "FLOW_WEIGHT",
    SEC_CLUSTER_LABEL_CI: "CLUSTER_LABEL_CI", SEC_CLUSTER_LABEL_X: "CLUSTER_LABEL_X",
    SEC_CLUSTER_LABEL_Y: "CLUSTER_LABEL_Y", SEC_CLUSTER_LABEL_R: "CLUSTER_LABEL_R",
    SEC_CLUSTER_LABEL_N: "CLUSTER_LABEL_N", SEC_CLUSTER_LABEL_DGRP: "CLUSTER_LABEL_DGRP",
    SEC_CLUSTER_LABEL_TITLE_REF: "CLUSTER_LABEL_TITLE_REF",
    SEC_CLUSTER_LABEL_DOM_REF: "CLUSTER_LABEL_DOM_REF", SEC_STRING_HEAP: "STRING_HEAP",
}

# Section groups for the human-readable byte-breakdown report (Task 2 acceptance).
_BYTE_BREAKDOWN_GROUPS = {
    "nodes": {SEC_NODE_POS, SEC_NODE_CLUSTER, SEC_NODE_DOMAIN, SEC_NODE_LIBRARY,
              SEC_NODE_PROMINENCE, SEC_NODE_SYS_ID, SEC_NODE_TITLE_REF, SEC_NODE_SHELFMARK_REF},
    "edges": {SEC_EDGE_SOURCE_DELTA, SEC_EDGE_TARGET_DELTA, SEC_EDGE_CLASS},
    "flows_and_labels": {
        SEC_FLOW_SOURCE_CLUSTER, SEC_FLOW_TARGET_CLUSTER, SEC_FLOW_WEIGHT,
        SEC_CLUSTER_LABEL_CI, SEC_CLUSTER_LABEL_X, SEC_CLUSTER_LABEL_Y, SEC_CLUSTER_LABEL_R,
        SEC_CLUSTER_LABEL_N, SEC_CLUSTER_LABEL_DGRP, SEC_CLUSTER_LABEL_TITLE_REF,
        SEC_CLUSTER_LABEL_DOM_REF,
    },
    "string_heap": {SEC_STRING_HEAP},
}


def _pad8(n: int) -> int:
    return (n + 7) // 8 * 8


class _StringHeapBuilder:
    """Single shared UTF-8 string heap with content-dedup -- identical strings
    (e.g. a repeated catalogue title) share one heap slice (schema §4 id 23)."""

    def __init__(self):
        self._buf = bytearray()
        self._cache: dict = {}

    def add(self, s: Optional[str]):
        s = s or ""
        cached = self._cache.get(s)
        if cached is not None:
            return cached
        raw = s.encode("utf-8")
        ref = (len(self._buf), len(raw))
        self._buf.extend(raw)
        self._cache[s] = ref
        return ref

    @property
    def raw(self) -> bytes:
        return bytes(self._buf)


@dataclass
class EncodedAsset:
    plain_bytes: bytes
    sections: list  # [{id, name, dtype, elem_size, count, byte_offset, byte_length}]
    node_count: int
    edge_count: int
    cluster_count: int
    label_count: int
    flow_count: int


def encode_asset(result: BakeResult) -> EncodedAsset:
    """Encode a BakeResult per docs/specs/atlas-asset-schema-v1.md. Raises
    ValueError if any node's sys_id fails the BigUint64 invariant (defense in
    depth -- run_bake already validates the eligible set, but a caller could
    hand-construct a BakeResult directly, as the tests do)."""
    nodes = result.nodes
    edges = result.edges
    labels = result.cluster_labels
    flows = result.flows
    n_clusters = len(result.clusters)

    cluster_dtype = DTYPE_UINT16 if n_clusters <= 0xFFFF else DTYPE_UINT32
    cluster_np = _NP_DTYPE[cluster_dtype]

    heap = _StringHeapBuilder()

    node_count = len(nodes)
    node_pos = np.empty(node_count * 2, dtype="<f4")
    node_cluster = np.empty(node_count, dtype=cluster_np)
    node_domain = np.empty(node_count, dtype="<u2")
    node_library = np.empty(node_count, dtype="<u2")
    node_prominence = np.empty(node_count, dtype="<u2")
    node_sys_id = np.empty(node_count, dtype="<u8")
    node_title_ref = np.empty(node_count * 2, dtype="<u4")
    node_shelfmark_ref = np.empty(node_count * 2, dtype="<u4")

    for i, nd in enumerate(nodes):
        x, y, ci, dgrp, lib_idx, prom, sys_id, title, shelfmark = nd
        node_pos[2 * i] = x
        node_pos[2 * i + 1] = y
        node_cluster[i] = ci
        node_domain[i] = dgrp
        node_library[i] = lib_idx
        node_prominence[i] = prom
        node_sys_id[i] = validate_sys_id(sys_id)
        to, tl = heap.add(title)
        node_title_ref[2 * i], node_title_ref[2 * i + 1] = to, tl
        so, sl = heap.add(shelfmark)
        node_shelfmark_ref[2 * i], node_shelfmark_ref[2 * i + 1] = so, sl

    edge_count = len(edges)
    edge_source_delta = np.empty(edge_count, dtype="<u4")
    edge_target_delta = np.empty(edge_count, dtype="<u4")
    edge_class = np.empty(edge_count, dtype="<u1")
    running_source = 0
    running_target = 0
    for i, (src, tgt, cls) in enumerate(edges):
        sd = src - running_source
        running_source = src
        if sd > 0 or i == 0:
            td = tgt
        else:
            td = tgt - running_target
        running_target = tgt
        edge_source_delta[i] = sd
        edge_target_delta[i] = td
        edge_class[i] = cls

    flow_count = len(flows)
    flow_source = np.empty(flow_count, dtype=cluster_np)
    flow_target = np.empty(flow_count, dtype=cluster_np)
    flow_weight = np.empty(flow_count, dtype="<f4")
    for i, (a, b, w) in enumerate(flows):
        flow_source[i] = a
        flow_target[i] = b
        flow_weight[i] = w

    label_count = len(labels)
    cl_ci = np.empty(label_count, dtype=cluster_np)
    cl_x = np.empty(label_count, dtype="<f4")
    cl_y = np.empty(label_count, dtype="<f4")
    cl_r = np.empty(label_count, dtype="<f4")
    cl_n = np.empty(label_count, dtype="<u4")
    cl_dgrp = np.empty(label_count, dtype="<u2")
    cl_title_ref = np.empty(label_count * 2, dtype="<u4")
    cl_dom_ref = np.empty(label_count * 2, dtype="<u4")
    for i, lab in enumerate(labels):
        cl_ci[i] = lab["ci"]
        cl_x[i] = lab["x"]
        cl_y[i] = lab["y"]
        cl_r[i] = lab["r"]
        cl_n[i] = lab["n"]
        cl_dgrp[i] = lab["dgrp"]
        to, tl = heap.add(lab["title"])
        cl_title_ref[2 * i], cl_title_ref[2 * i + 1] = to, tl
        do, dl = heap.add(lab["dom"])
        cl_dom_ref[2 * i], cl_dom_ref[2 * i + 1] = do, dl

    heap_arr = np.frombuffer(heap.raw, dtype="<u1")

    section_defs = [
        (SEC_NODE_POS, DTYPE_FLOAT32, node_pos),
        (SEC_NODE_CLUSTER, cluster_dtype, node_cluster),
        (SEC_NODE_DOMAIN, DTYPE_UINT16, node_domain),
        (SEC_NODE_LIBRARY, DTYPE_UINT16, node_library),
        (SEC_NODE_PROMINENCE, DTYPE_UINT16, node_prominence),
        (SEC_NODE_SYS_ID, DTYPE_UINT64, node_sys_id),
        (SEC_NODE_TITLE_REF, DTYPE_UINT32, node_title_ref),
        (SEC_NODE_SHELFMARK_REF, DTYPE_UINT32, node_shelfmark_ref),
        (SEC_EDGE_SOURCE_DELTA, DTYPE_UINT32, edge_source_delta),
        (SEC_EDGE_TARGET_DELTA, DTYPE_UINT32, edge_target_delta),
        (SEC_EDGE_CLASS, DTYPE_UINT8, edge_class),
        (SEC_FLOW_SOURCE_CLUSTER, cluster_dtype, flow_source),
        (SEC_FLOW_TARGET_CLUSTER, cluster_dtype, flow_target),
        (SEC_FLOW_WEIGHT, DTYPE_FLOAT32, flow_weight),
        (SEC_CLUSTER_LABEL_CI, cluster_dtype, cl_ci),
        (SEC_CLUSTER_LABEL_X, DTYPE_FLOAT32, cl_x),
        (SEC_CLUSTER_LABEL_Y, DTYPE_FLOAT32, cl_y),
        (SEC_CLUSTER_LABEL_R, DTYPE_FLOAT32, cl_r),
        (SEC_CLUSTER_LABEL_N, DTYPE_UINT32, cl_n),
        (SEC_CLUSTER_LABEL_DGRP, DTYPE_UINT16, cl_dgrp),
        (SEC_CLUSTER_LABEL_TITLE_REF, DTYPE_UINT32, cl_title_ref),
        (SEC_CLUSTER_LABEL_DOM_REF, DTYPE_UINT32, cl_dom_ref),
        (SEC_STRING_HEAP, DTYPE_UINT8, heap_arr),
    ]

    header_size = 16 + 32 * len(section_defs)
    assert header_size % 8 == 0, "fixed header + section table must land on an 8-byte boundary"
    running_offset = header_size
    table_entries = []
    data_chunks = []
    for sec_id, dtype_code, arr in section_defs:
        arr = np.ascontiguousarray(arr)
        raw = arr.tobytes()
        byte_offset = running_offset
        byte_length = len(raw)
        table_entries.append({
            "id": sec_id, "name": _SECTION_NAMES[sec_id], "dtype": dtype_code,
            "elem_size": _ELEM_SIZE[dtype_code], "count": int(arr.size),
            "byte_offset": byte_offset, "byte_length": byte_length,
        })
        data_chunks.append(raw)
        running_offset += byte_length
        pad = _pad8(running_offset) - running_offset
        if pad:
            data_chunks.append(b"\x00" * pad)
            running_offset += pad

    header = struct.pack("<8sII", MAGIC, SCHEMA_VERSION, len(section_defs))
    table_bytes = b"".join(
        struct.pack("<IIIIQQ", e["id"], e["dtype"], e["elem_size"], e["count"],
                    e["byte_offset"], e["byte_length"])
        for e in table_entries
    )
    plain_bytes = header + table_bytes + b"".join(data_chunks)

    return EncodedAsset(
        plain_bytes=plain_bytes, sections=table_entries, node_count=node_count,
        edge_count=edge_count, cluster_count=n_clusters, label_count=label_count,
        flow_count=flow_count,
    )


def decode_asset(data: bytes) -> dict:
    """Pure-Python reference decoder implementing schema §10 step-by-step.
    Returns a friendly dict (not raw section byte arrays) -- sys_id is always
    a decimal string (schema §7), never a float/Number, to avoid precision
    loss above 2**53."""
    magic, schema_version, section_count = struct.unpack_from("<8sII", data, 0)
    if magic != MAGIC:
        raise ValueError(f"bad magic bytes: {magic!r}")
    if schema_version != SCHEMA_VERSION:
        raise ValueError(f"unsupported schema_version: {schema_version}")

    sections = {}
    off = 16
    for _ in range(section_count):
        sec_id, dtype_code, _elem_size, count, byte_offset, byte_length = \
            struct.unpack_from("<IIIIQQ", data, off)
        sections[sec_id] = (dtype_code, count, byte_offset, byte_length)
        off += 32

    def _arr(sec_id):
        dtype_code, count, byte_offset, _byte_length = sections[sec_id]
        return np.frombuffer(data, dtype=_NP_DTYPE[dtype_code], count=count, offset=byte_offset)

    heap = _arr(SEC_STRING_HEAP).tobytes()

    def _heap_str(ref_arr, i):
        offset, length = int(ref_arr[2 * i]), int(ref_arr[2 * i + 1])
        return heap[offset:offset + length].decode("utf-8")

    node_pos = _arr(SEC_NODE_POS)
    node_cluster = _arr(SEC_NODE_CLUSTER)
    node_domain = _arr(SEC_NODE_DOMAIN)
    node_library = _arr(SEC_NODE_LIBRARY)
    node_prominence = _arr(SEC_NODE_PROMINENCE)
    node_sys_id = _arr(SEC_NODE_SYS_ID)
    node_title_ref = _arr(SEC_NODE_TITLE_REF)
    node_shelfmark_ref = _arr(SEC_NODE_SHELFMARK_REF)
    node_count = sections[SEC_NODE_SYS_ID][1]

    nodes = [
        {
            "x": float(node_pos[2 * i]), "y": float(node_pos[2 * i + 1]),
            "cluster": int(node_cluster[i]), "domain": int(node_domain[i]),
            "library": int(node_library[i]), "prominence": int(node_prominence[i]),
            "sys_id": str(int(node_sys_id[i])),
            "title": _heap_str(node_title_ref, i),
            "shelfmark": _heap_str(node_shelfmark_ref, i),
        }
        for i in range(node_count)
    ]

    edge_source_delta = _arr(SEC_EDGE_SOURCE_DELTA)
    edge_target_delta = _arr(SEC_EDGE_TARGET_DELTA)
    edge_class = _arr(SEC_EDGE_CLASS)
    edges = []
    running_source = 0
    running_target = 0
    for i in range(len(edge_source_delta)):
        sd, td = int(edge_source_delta[i]), int(edge_target_delta[i])
        running_source += sd
        if sd > 0 or i == 0:
            running_target = td
        else:
            running_target += td
        edges.append({"source": running_source, "target": running_target, "cls": int(edge_class[i])})

    flow_source = _arr(SEC_FLOW_SOURCE_CLUSTER)
    flow_target = _arr(SEC_FLOW_TARGET_CLUSTER)
    flow_weight = _arr(SEC_FLOW_WEIGHT)
    flows = [
        {"source_cluster": int(flow_source[i]), "target_cluster": int(flow_target[i]),
         "weight": float(flow_weight[i])}
        for i in range(len(flow_source))
    ]

    cl_ci = _arr(SEC_CLUSTER_LABEL_CI)
    cl_x = _arr(SEC_CLUSTER_LABEL_X)
    cl_y = _arr(SEC_CLUSTER_LABEL_Y)
    cl_r = _arr(SEC_CLUSTER_LABEL_R)
    cl_n = _arr(SEC_CLUSTER_LABEL_N)
    cl_dgrp = _arr(SEC_CLUSTER_LABEL_DGRP)
    cl_title_ref = _arr(SEC_CLUSTER_LABEL_TITLE_REF)
    cl_dom_ref = _arr(SEC_CLUSTER_LABEL_DOM_REF)
    cluster_labels = [
        {
            "ci": int(cl_ci[i]), "x": float(cl_x[i]), "y": float(cl_y[i]),
            "r": float(cl_r[i]), "n": int(cl_n[i]), "dgrp": int(cl_dgrp[i]),
            "title": _heap_str(cl_title_ref, i), "dom": _heap_str(cl_dom_ref, i),
        }
        for i in range(len(cl_ci))
    ]

    return {
        "schema_version": int(schema_version),
        "nodes": nodes, "edges": edges, "flows": flows, "cluster_labels": cluster_labels,
    }


def assert_byte_budget(nbytes: int, cap: int = BYTE_BUDGET_CAP) -> None:
    """The D-10 / PERF-01 preview byte-budget gate. Raises ValueError (bake
    FAILS) if the Brotli-compressed payload exceeds the cap."""
    if nbytes > cap:
        raise ValueError(f"byte budget exceeded: {nbytes:,} bytes > cap {cap:,} bytes")


def assert_bake_complete(result: BakeResult) -> None:
    """HIGH-3: the bake REFUSES to emit an asset unless the placed node set
    exactly equals the eligible connected-endpoint set -- missing AND extra
    both empty (D-09 / HIGH-5; no ">=" fudge). Raises ValueError (bake FAILS)
    on any mismatch. Enforced before any bytes are written, and by main() for
    every mode (incl. --report), so a node-set mismatch can never be reported
    as a successful bake."""
    if result.missing or result.extra:
        raise ValueError(
            "node-set mismatch -- bake refuses to write: "
            f"{len(result.missing)} missing, {len(result.extra)} extra "
            f"(eligible={result.eligible_count}, placed={result.placed_count}); "
            "the bake requires EXACT set equality (missing==0 and extra==0)"
        )
    if result.placed_count != result.eligible_count:
        raise ValueError(
            f"placed_count ({result.placed_count}) != eligible_count "
            f"({result.eligible_count}) despite empty missing/extra"
        )


def build_manifest(result: BakeResult, encoded: EncodedAsset, content_hash: str,
                    asset_basename: str, source_db_hash: str) -> dict:
    """Schema §8 companion manifest. NO discovery-overlay fields (D-04)."""
    return {
        "schema_version": SCHEMA_VERSION,
        "content_hash": content_hash,
        "asset_basename": asset_basename,
        "algo_version": result.algo_version,
        "seed": result.seed,
        "eligible_count": result.eligible_count,
        "placed_count": result.placed_count,
        "missing": [str(s) for s in result.missing],
        "extra": [str(s) for s in result.extra],
        "node_count": encoded.node_count,
        "edge_count": encoded.edge_count,
        "cluster_count": encoded.cluster_count,
        "flow_count": encoded.flow_count,
        "label_count": encoded.label_count,
        "source_db_hash": source_db_hash,
        "domain_groups": [[en, he, color] for en, he, color in DOMAIN_GROUPS],
        "libraries": result.libraries,
        "sections": encoded.sections,
    }


def print_byte_breakdown(encoded: EncodedAsset, brotli_size: int) -> None:
    totals = {name: 0 for name in _BYTE_BREAKDOWN_GROUPS}
    for e in encoded.sections:
        for name, ids in _BYTE_BREAKDOWN_GROUPS.items():
            if e["id"] in ids:
                totals[name] += e["byte_length"]
    total_plain = len(encoded.plain_bytes)
    print("byte breakdown (raw, pre-Brotli):")
    for name, nbytes in totals.items():
        print(f"  {name}: {nbytes:,} bytes")
    print(f"  header+section-table: {total_plain - sum(totals.values()):,} bytes")
    print(f"  total plain: {total_plain:,} bytes")
    print(f"  total brotli: {brotli_size:,} bytes")


def print_stats(result: BakeResult) -> None:
    print(f"eligible_count={result.eligible_count}")
    print(f"placed_count={result.placed_count}")
    print(f"missing={len(result.missing)}")
    print(f"extra={len(result.extra)}")
    print(f"seed={result.seed}")
    print(f"algo_version={result.algo_version}")
    print(f"node_count={len(result.nodes)}")
    print(f"edge_count={len(result.edges)}")
    print(f"cluster_count={len(result.clusters)}")
    print(f"label_count={len(result.cluster_labels)}")
    print(f"flow_count={len(result.flows)}")
    print(f"library_count={len(result.libraries)}")
    print("discovery_overlay_fields=0")  # D-04: never present, printed for operator visibility


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("db_path", nargs="?", default=None,
                        help="Path to the research DB (required unless --smoke or --golden)")
    parser.add_argument("--table", default="accepted_pairs_canonmask",
                        help="Source table name (default: accepted_pairs_canonmask)")
    parser.add_argument("--out-dir", default=None,
                        help="Output directory for the production bake (default: <repo-root>/atlas_data)")
    parser.add_argument("--report", action="store_true",
                        help="Print counts/byte breakdown without writing any files")
    parser.add_argument("--smoke", type=int, default=None, metavar="N",
                        help="Run a synthetic in-memory bake with N continuation-ish nodes (no db_path needed)")
    parser.add_argument("--golden", default=None, metavar="PATH",
                        help="Write a small deterministic golden fixture (.bin + .bin.br + -expected.json) to PATH")
    return parser


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.golden is None and args.smoke is None and args.db_path is None:
        parser.error("db_path is required unless --smoke N or --golden PATH is given")

    t0 = time.time()
    if args.golden is not None:
        ms_pairs, sys_meta, domains, _ids = golden_dataset()
        source_db_hash = "golden-synthetic-v1"
    elif args.smoke is not None:
        ms_pairs, sys_meta, domains, _ids = synthetic_dataset(args.smoke, seed=SEED)
        source_db_hash = f"smoke-synthetic-{args.smoke}"
    else:
        sys_meta = load_lib_meta(REPO_ROOT)
        domains = load_domains(_fjms_db_path(REPO_ROOT))
        ms_pairs = load_ms_pairs_from_db(args.db_path, args.table)
        source_db_hash = hash_file(args.db_path)

    result = run_bake(ms_pairs, sys_meta, domains, seed=SEED)
    print_stats(result)
    print(f"(bake pipeline complete in {time.time() - t0:.1f}s)")

    # HIGH-3: FAIL every mode (incl. --report) unless eligible == placed exactly.
    assert_bake_complete(result)

    if args.golden is not None:
        return _write_golden(result, args.golden)
    if args.report:
        return 0
    return _write_production(result, args.out_dir, source_db_hash)


def _write_golden(result: BakeResult, golden_path: str) -> int:
    if brotli is None:
        print("ERROR: Brotli is required (pip install -r requirements-atlas-bake.txt)",
              file=sys.stderr)
        return 1
    assert_bake_complete(result)  # HIGH-3: refuse to write on any node-set mismatch
    encoded = encode_asset(result)
    bin_path = Path(golden_path)
    br_path = bin_path.with_name(bin_path.name + ".br")
    expected_path = bin_path.with_name(bin_path.stem + "-expected.json")
    bin_path.parent.mkdir(parents=True, exist_ok=True)
    bin_path.write_bytes(encoded.plain_bytes)
    br_bytes = brotli.compress(encoded.plain_bytes, quality=11)
    br_path.write_bytes(br_bytes)
    decoded = decode_asset(encoded.plain_bytes)
    expected_path.write_text(
        json.dumps(decoded, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(f"wrote golden fixture: {bin_path} ({len(encoded.plain_bytes)} bytes), "
          f"{br_path} ({len(br_bytes)} bytes), {expected_path}")
    return 0


def _write_production(result: BakeResult, out_dir: Optional[str], source_db_hash: str) -> int:
    if brotli is None:
        print("ERROR: Brotli is required (pip install -r requirements-atlas-bake.txt)",
              file=sys.stderr)
        return 1
    # HIGH-3: refuse to write on any node-set mismatch (missing/extra non-empty).
    assert_bake_complete(result)
    # HIGH-3 additional regression floor: a REAL research-DB bake must place at
    # least REGRESSION_FLOOR nodes (D-09 historical floor). Synthetic --smoke /
    # --golden bakes are far smaller by design, so the floor is scoped to real
    # bakes via the source-DB marker.
    if (not source_db_hash.startswith(("smoke-", "golden-"))
            and result.placed_count < REGRESSION_FLOOR):
        raise ValueError(
            f"regression floor: placed_count {result.placed_count:,} < "
            f"{REGRESSION_FLOOR:,} (a real research-DB bake is expected to place "
            "at least this many nodes)"
        )
    encoded = encode_asset(result)
    content_hash = hashlib.sha256(encoded.plain_bytes).hexdigest()[:12]
    basename = f"atlas-v1-{content_hash}"
    br_bytes = brotli.compress(encoded.plain_bytes, quality=11)
    try:
        assert_byte_budget(len(br_bytes))
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    out_path = Path(out_dir) if out_dir else (REPO_ROOT / "atlas_data")
    out_path.mkdir(parents=True, exist_ok=True)
    (out_path / f"{basename}.bin").write_bytes(encoded.plain_bytes)
    (out_path / f"{basename}.bin.br").write_bytes(br_bytes)
    manifest = build_manifest(result, encoded, content_hash, basename, source_db_hash)
    (out_path / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print_byte_breakdown(encoded, len(br_bytes))
    print(f"wrote {out_path / (basename + '.bin')} + .bin.br + manifest.json "
          f"(content_hash={content_hash})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
