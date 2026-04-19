#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnostic for bug 260419-nwv. Reproduces image-vs-text mis-alignment on
paired-leaf CUL shelfmarks (specifically T-S NS 158.112 / sys_id
990051537270205171). Not a test — a one-shot forensic tool.

What this script does
---------------------

Compares FOUR orderings for one sys_id to figure out which layer is
misaligned with the transcription text order:

  1. Transcriptions.txt          — text page order (p_num, fl_id)
  2. NLI IIIF manifest           — canvas order (FL IDs)
  3. nli_crossref.db nli_images  — stored image rows (ImageName, FGP)
  4. CUDL (Cambridge) manifest   — canvas labels if a Cambridge manifest
                                    is registered for this shelfmark.

It also prints parse_folio_label(ImageName) for each nli_images row, which
exposes the H2 paired-leaf regex bug (all rows currently return '').

Usage:
    python scripts/debug_ts_ns_158_112_image_alignment.py
    python scripts/debug_ts_ns_158_112_image_alignment.py --sys-id 990051537270205171

Safe to run in CI or on a dev box — makes at most two outbound HTTP requests
(NLI IIIF manifest + CUDL manifest). Timeouts are 10s each. If network is
unavailable, the script still completes and reports which sections were
unavailable; it does not raise.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
from pathlib import Path

# Make the repo root importable so we can reach shared.nli_crossref_service
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from shared.nli_crossref_service import parse_folio_label
except Exception as e:  # noqa: BLE001 — diagnostic tool: keep going
    print(f"WARN: could not import parse_folio_label: {e}")

    def parse_folio_label(name: str) -> str:  # fallback: inline copy
        _PAT = re.compile(r"L(\d+)F\d+B\d+S(\d+)")
        m = _PAT.search(name or "")
        if not m:
            return ""
        return f"{m.group(1)}{'r' if m.group(2) == '1' else 'v'}"


DEFAULT_SYS_ID = "990051537270205171"  # T-S NS 158.112
TRANSCRIPTIONS_PATH = REPO_ROOT / "Transcriptions.txt"
NLI_CROSSREF_DB = REPO_ROOT / "nli_data" / "nli_crossref.db"
IIIF_SAMPLES_DIR = REPO_ROOT / "_iiif_samples"

HEADERS = {
    "User-Agent": (
        "GenizahSearch-DebugScript/1.0 "
        "(bug 260419-nwv; hillel@dicta.org.il)"
    ),
    "Accept": "application/json",
}


# ──────────────────────────────────────────────────────────────────────
# Readers
# ──────────────────────────────────────────────────────────────────────


def read_transcription_entries(sys_id: str) -> list[tuple[int, str]]:
    """Return (p_num, fl_id) pairs in Transcriptions.txt order for this sys_id."""
    if not TRANSCRIPTIONS_PATH.exists():
        print(f"WARN: {TRANSCRIPTIONS_PATH} not found")
        return []
    prefix = f"==> {sys_id}_"
    pattern = re.compile(
        rf"==>\s+{re.escape(sys_id)}_IE\d+_P(\d+)_FL(\d+)\s+<=="
    )
    entries: list[tuple[int, str]] = []
    with TRANSCRIPTIONS_PATH.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if not line.startswith(prefix):
                continue
            m = pattern.match(line.strip())
            if m:
                entries.append((int(m.group(1)), m.group(2)))
    return entries


def read_nli_images(sys_id: str) -> tuple[list[dict], str | None]:
    """Return (image_rows, cambridge_manifest_url_or_None) from nli_crossref.db."""
    if not NLI_CROSSREF_DB.exists():
        print(f"WARN: {NLI_CROSSREF_DB} not found")
        return ([], None)
    try:
        conn = sqlite3.connect(str(NLI_CROSSREF_DB))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute(
            "SELECT ImageName, FGPImageNumberId, FGPNumber, Shelfmark, "
            "LibraryAbbrev "
            "FROM nli_images WHERE NLI_AlmaId = ? ORDER BY ImageName",
            (sys_id,),
        )
        rows = [dict(r) for r in cur.fetchall()]

        # Find cambridge manifest — need normalized shelfmark. We don't know
        # the exact normalization, so try a few forms derived from the
        # Shelfmark column.
        manifest_url: str | None = None
        shelfmark = rows[0]["Shelfmark"] if rows else ""
        candidates: list[str] = []
        if shelfmark:
            s = shelfmark.lower()
            # "T-S NS 158.112" → "tsns158.112" | "ts ns 158.112" | etc.
            candidates.append(
                re.sub(r"[\s\-]+", "", s)
            )  # tsns158.112
            candidates.append(s.replace("-", " ").replace("  ", " ").strip())
            candidates.append(s)
        # Also the specific known one for this bug
        candidates.append("tsns158.112")
        seen = set()
        uniq_candidates = [c for c in candidates if not (c in seen or seen.add(c))]

        for cand in uniq_candidates:
            cur.execute(
                "SELECT manifest_url FROM cambridge_manifests "
                "WHERE normalized_shelfmark = ?",
                (cand,),
            )
            got = cur.fetchone()
            if got:
                manifest_url = got[0]
                print(
                    f"INFO: Cambridge manifest lookup succeeded for "
                    f"normalized_shelfmark='{cand}'"
                )
                break
        if manifest_url is None:
            print(
                f"INFO: no Cambridge manifest found in nli_crossref.db for "
                f"any of {uniq_candidates!r}"
            )
        conn.close()
        return (rows, manifest_url)
    except Exception as e:  # noqa: BLE001
        print(f"WARN: nli_crossref.db read failed: {e}")
        return ([], None)


# ──────────────────────────────────────────────────────────────────────
# HTTP fetches (best-effort)
# ──────────────────────────────────────────────────────────────────────


def _http_get_json(url: str, timeout: float = 10.0) -> dict | None:
    try:
        import requests  # lazy import so missing requests doesn't break repo tooling
    except Exception as e:  # noqa: BLE001
        print(f"WARN: 'requests' not available: {e}")
        return None
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, verify=True)
        if resp.status_code != 200:
            print(f"WARN: {url} -> HTTP {resp.status_code}")
            return None
        return resp.json()
    except Exception as e:  # noqa: BLE001
        print(f"WARN: fetch of {url} failed: {e}")
        return None


def fetch_nli_manifest(sys_id: str, suffix: int = 1) -> tuple[list[str], list[str]]:
    """Return (fl_digit_strings_in_canvas_order, canvas_labels_in_same_order).

    Falls back to an on-disk cached sample under _iiif_samples/ if network
    fails, so the diagnostic is still useful offline.
    """
    url = (
        f"https://iiif.nli.org.il/IIIFv21/DOCID/"
        f"PNX_MANUSCRIPTS{sys_id}-{suffix}/manifest"
    )
    data = _http_get_json(url)
    if data is None:
        cached = IIIF_SAMPLES_DIR / f"nli_manifest_{sys_id}_{suffix}.json"
        if cached.exists():
            try:
                data = json.loads(cached.read_text(encoding="utf-8"))
                print(f"INFO: using cached NLI manifest at {cached}")
            except Exception as e:  # noqa: BLE001
                print(f"WARN: cached NLI manifest unreadable: {e}")
                return ([], [])
        else:
            return ([], [])

    fl_ids: list[str] = []
    labels: list[str] = []
    try:
        for canvas in (data.get("sequences") or [{}])[0].get("canvases", []) or []:
            label = canvas.get("label", "") or ""
            labels.append(str(label))
            images = canvas.get("images") or []
            fl = ""
            if images:
                service_id = (
                    (images[0].get("resource") or {}).get("service", {}).get("@id", "")
                )
                m = re.search(r"FL(\d+)", service_id)
                if m:
                    fl = m.group(1)
            fl_ids.append(fl)
    except Exception as e:  # noqa: BLE001
        print(f"WARN: parsing NLI manifest failed: {e}")
    return (fl_ids, labels)


def fetch_cudl_manifest(manifest_url: str | None, sys_id: str) -> list[str]:
    """Return canvas labels (in order) from a CUDL IIIF manifest."""
    if not manifest_url:
        return []
    data = _http_get_json(manifest_url)
    if data is None:
        cached = IIIF_SAMPLES_DIR / f"cudl_manifest_{sys_id}.json"
        if cached.exists():
            try:
                data = json.loads(cached.read_text(encoding="utf-8"))
                print(f"INFO: using cached CUDL manifest at {cached}")
            except Exception as e:  # noqa: BLE001
                print(f"WARN: cached CUDL manifest unreadable: {e}")
                return []
        else:
            return []
    labels: list[str] = []
    try:
        for canvas in (data.get("sequences") or [{}])[0].get("canvases", []) or []:
            label = canvas.get("label", "") or ""
            labels.append(str(label))
    except Exception as e:  # noqa: BLE001
        print(f"WARN: parsing CUDL manifest failed: {e}")
    return labels


# ──────────────────────────────────────────────────────────────────────
# Report
# ──────────────────────────────────────────────────────────────────────


def format_table(rows: list[list[str]], headers: list[str]) -> str:
    widths = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(str(cell)))
    fmt_line = lambda vals: "  ".join(  # noqa: E731
        str(v).ljust(widths[i]) for i, v in enumerate(vals)
    )
    sep = "  ".join("-" * w for w in widths)
    out = [fmt_line(headers), sep]
    out.extend(fmt_line(r) for r in rows)
    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sys-id", default=DEFAULT_SYS_ID)
    ap.add_argument(
        "--verify-resolver",
        action="store_true",
        help=(
            "After the existing alignment table, run "
            "resolve_cambridge_canvas_for_page for pages 0..N-1 and "
            "print a RESOLVER TABLE with one verdict line per page "
            "plus a single VERIFIED/BROKEN summary line (260419-cfx)."
        ),
    )
    args = ap.parse_args()
    sys_id = args.sys_id

    started = time.time()
    print(f"=== Bug 260419-nwv diagnostic for sys_id={sys_id} ===\n")

    # 1. Transcriptions.txt
    trans_entries = read_transcription_entries(sys_id)
    print(f"Transcriptions.txt: {len(trans_entries)} entries\n")
    if trans_entries:
        for p_num, fl in trans_entries[:5]:
            print(f"  P{p_num:04d} FL{fl}")
        if len(trans_entries) > 5:
            print(f"  ... (+{len(trans_entries) - 5} more)")
        print()

    # 2. nli_images + cambridge manifest URL
    nli_rows, cambridge_manifest_url = read_nli_images(sys_id)
    print(f"nli_crossref.db nli_images: {len(nli_rows)} rows")
    print(f"Cambridge manifest URL: {cambridge_manifest_url or '(none)'}")
    print()

    # 3. NLI IIIF manifest
    nli_fl_ids, nli_labels = fetch_nli_manifest(sys_id, suffix=1)
    print(f"NLI IIIF manifest canvas count: {len(nli_fl_ids)}")
    print()

    # 4. CUDL manifest
    cudl_labels = fetch_cudl_manifest(cambridge_manifest_url, sys_id)
    print(f"CUDL manifest canvas count: {len(cudl_labels)}")
    print()

    # ─── Alignment table ───
    max_n = max(
        len(trans_entries),
        len(nli_fl_ids),
        len(cudl_labels),
        len(nli_rows),
    )
    rows: list[list[str]] = []
    for i in range(max_n):
        p_num = trans_entries[i][0] if i < len(trans_entries) else ""
        trans_fl = trans_entries[i][1] if i < len(trans_entries) else ""
        nli_fl = nli_fl_ids[i] if i < len(nli_fl_ids) else ""
        cudl_label = cudl_labels[i] if i < len(cudl_labels) else ""

        # nli_images ordered by ImageName (alphabetical) — this is the
        # "get_images()" order before the folio-label sort is applied.
        nli_img = nli_rows[i] if i < len(nli_rows) else None
        if nli_img:
            img_name = nli_img.get("ImageName", "")
            parsed_label = parse_folio_label(img_name)
        else:
            img_name = ""
            parsed_label = ""
        rows.append(
            [
                str(p_num),
                trans_fl,
                nli_fl,
                cudl_label[:50] if cudl_label else "",
                img_name,
                f"'{parsed_label}'",
            ]
        )

    print("ALIGNMENT TABLE")
    print("-" * 40)
    print(
        format_table(
            rows,
            headers=[
                "p_num",
                "trans_FL",
                "nli_manifest_FL",
                "cudl_label",
                "nli_images.ImageName",
                "parse_folio_label()",
            ],
        )
    )
    print()

    # ─── Verdicts ───
    print("ALIGNMENT VERDICTS")
    print("-" * 40)

    # text <-> NLI
    text_nli_verdict: str
    if not trans_entries or not nli_fl_ids:
        text_nli_verdict = (
            "text<->NLI UNKNOWN - missing transcription entries or manifest"
        )
    else:
        n = min(len(trans_entries), len(nli_fl_ids))
        mismatches = []
        for i in range(n):
            t = trans_entries[i][1]
            m = nli_fl_ids[i]
            if t and m and t != m:
                mismatches.append((trans_entries[i][0], t, m))
        if not mismatches and len(trans_entries) == len(nli_fl_ids):
            text_nli_verdict = (
                "text<->NLI ALIGNED - Transcriptions.txt FL ids match "
                "NLI manifest canvas order 1:1"
            )
        elif not mismatches and len(trans_entries) != len(nli_fl_ids):
            text_nli_verdict = (
                f"text<->NLI PARTIAL - no ordering mismatches in the common "
                f"prefix, but counts differ (trans={len(trans_entries)}, "
                f"nli={len(nli_fl_ids)})"
            )
        else:
            text_nli_verdict = (
                f"text<->NLI MISALIGNED - {len(mismatches)} FL id mismatches "
                f"in position order (first: p_num={mismatches[0][0]} "
                f"trans=FL{mismatches[0][1]} vs nli=FL{mismatches[0][2]})"
            )
    print(text_nli_verdict)

    # text <-> CUDL
    text_cudl_verdict: str
    if not cudl_labels:
        text_cudl_verdict = (
            "text<->CUDL UNKNOWN - no CUDL manifest URL or fetch failed"
        )
    else:
        # Heuristic: are CUDL labels a sequence of "Nr", "Nv" or "f.Nr","f.Nv"
        # style (i.e. page order without binding/cover inserts)?
        def _is_folio_like(label: str) -> bool:
            s = label.strip().lower()
            return bool(
                re.fullmatch(r"(?:f\.?\s*)?\d+\s*[rv]?", s)
                or re.fullmatch(r"\d+[rv]", s)
            )

        n_folio_like = sum(1 for lbl in cudl_labels if _is_folio_like(lbl))
        non_folio = [lbl for lbl in cudl_labels if not _is_folio_like(lbl)]
        if (
            len(cudl_labels) == len(trans_entries)
            and n_folio_like == len(cudl_labels)
        ):
            text_cudl_verdict = (
                "text<->CUDL LIKELY ALIGNED - CUDL canvas count matches "
                "transcription count and all labels look folio-like "
                "(1r/1v/2r/...)"
            )
        elif len(cudl_labels) != len(trans_entries):
            text_cudl_verdict = (
                f"text<->CUDL COUNT MISMATCH - CUDL has {len(cudl_labels)} "
                f"canvases but transcription has {len(trans_entries)} pages. "
                f"This means positional /api/cambridge_image/{{sys_id}}?"
                f"page={{p-1}} WILL return the wrong image at least some of "
                f"the time. Non-folio-like CUDL labels: "
                f"{non_folio[:5]!r} (+{max(0, len(non_folio) - 5)} more)"
            )
        else:
            text_cudl_verdict = (
                f"text<->CUDL POSSIBLE MISMATCH - counts match but "
                f"{len(non_folio)} CUDL labels do not look folio-like "
                f"(e.g. {non_folio[:3]!r}); cannot confirm alignment by "
                f"labels alone"
            )
    print(text_cudl_verdict)

    # parse_folio_label verdict (H2)
    nli_image_names = [
        (r.get("ImageName") or "") for r in nli_rows if r.get("ImageName")
    ]
    if not nli_image_names:
        parse_verdict = (
            "parse_folio_label UNKNOWN — no nli_images rows for this sys_id"
        )
    else:
        parsed = [(n, parse_folio_label(n)) for n in nli_image_names]
        empty = [n for n, p in parsed if not p]
        if not empty:
            parse_verdict = (
                f"parse_folio_label OK - all {len(parsed)} ImageNames parsed "
                f"to non-empty folio labels"
            )
        else:
            parse_verdict = (
                f"parse_folio_label BROKEN (H2 CONFIRMED) - {len(empty)}/"
                f"{len(parsed)} ImageNames return empty string. Examples: "
                f"{empty[:3]!r}"
            )
    print(parse_verdict)

    # ─── Optional: post-fix resolver verification (260419-cfx) ────────
    if args.verify_resolver:
        print()
        print("RESOLVER TABLE (260419-cfx)")
        print("-" * 40)
        try:
            from shared.nli_crossref_service import (
                resolve_cambridge_canvas_for_page,
                get_nli_crossref_service,
            )
        except Exception as e:  # noqa: BLE001
            print(f"ERROR: could not import resolver: {e}")
            print("RESOLVER CUL-canvas-fix BROKEN — resolver import failed")
            elapsed = time.time() - started
            print(f"\n(diagnostic completed in {elapsed:.1f}s)")
            return 2

        # Build the images_ext CUDL canvas list from the fetched CUDL
        # labels (purely local; no extra network). Parse folio_num and
        # folio_side from each label so the resolver can match.
        def _parse_cudl_label_for_verify(lbl: str) -> tuple[int | None, str | None]:
            if not lbl:
                return (None, None)
            m = re.match(r'^\s*(?:f\.?\s*)?(\d+)\s*([rv])?\b', lbl.strip(), re.IGNORECASE)
            if not m:
                return (None, None)
            try:
                fn = int(m.group(1))
            except (TypeError, ValueError):
                return (None, None)
            side = m.group(2).lower() if m.group(2) else 'r'
            return (fn, side)

        images_ext_for_verify = []
        for lbl in cudl_labels:
            fn, fs = _parse_cudl_label_for_verify(lbl)
            images_ext_for_verify.append({
                'label': lbl,
                'url': f'https://synthetic.cudl/{lbl}',
                'folio_num': fn,
                'folio_side': fs,
            })

        svc = None
        try:
            svc = get_nli_crossref_service(thread_safe=False)
            if not svc.is_available():
                print("WARN: nli_crossref sidecar not available; resolver will be degraded")
        except Exception as e:  # noqa: BLE001
            print(f"WARN: could not construct NliCrossrefService: {e}")

        # Iterate pages. Primary range = nli_images row count (authoritative
        # transcription-page mapping after 260419-nwv sort fix).
        n_pages = len(nli_rows) if nli_rows else len(trans_entries)
        verdicts_per_page: list[tuple[int, str]] = []
        # Invariants for the summary:
        #  (a) every page in [0, min(len(cudl_canvases), len(nli_rows)))
        #      MUST resolve to a canvas_index (exact match)
        #  (b) every page outside that range (in [min, len(nli_rows)))
        #      MUST resolve to NLI_FALLBACK (None from resolver)
        in_range_expected_matches = min(len(cudl_labels), n_pages)
        for p in range(n_pages):
            # Folio label (for human-readable "folio=Xr|v" tag) from nli_rows.
            folio_tag = "?"
            if p < len(nli_rows):
                img_name = nli_rows[p].get("ImageName", "") or ""
                parsed = parse_folio_label(img_name)
                if parsed:
                    folio_tag = parsed

            out = resolve_cambridge_canvas_for_page(
                sys_id, p, images_ext_for_verify, svc=svc,
            )
            if out is None:
                verdicts_per_page.append(
                    (p, f"p={p} (folio={folio_tag}) → NLI_FALLBACK")
                )
            elif isinstance(out, dict) and out.get('degraded'):
                verdicts_per_page.append(
                    (p, f"p={p} (folio={folio_tag}) → DEGRADED (sidecar unavailable)")
                )
            else:
                ci = out.get('canvas_index')
                verdicts_per_page.append(
                    (p, f"p={p} (folio={folio_tag}) → canvas_index={ci}")
                )

        for _, line in verdicts_per_page:
            print(f"  {line}")

        # Summary verdict.
        broken_reason: str | None = None
        if not nli_rows:
            broken_reason = "no nli_images rows to verify against"
        elif not cudl_labels:
            broken_reason = "no CUDL canvases fetched (manifest unavailable?)"
        else:
            for p, _ in verdicts_per_page:
                out = resolve_cambridge_canvas_for_page(
                    sys_id, p, images_ext_for_verify, svc=svc,
                )
                if p < in_range_expected_matches:
                    if not (isinstance(out, dict) and out.get('canvas_index') is not None):
                        broken_reason = (
                            f"page {p} expected exact canvas match but got "
                            f"{out!r}"
                        )
                        break
                else:
                    if out is not None:
                        broken_reason = (
                            f"page {p} expected NLI_FALLBACK but got {out!r}"
                        )
                        break

        if broken_reason is None:
            print("RESOLVER CUL-canvas-fix VERIFIED")
        else:
            print(f"RESOLVER CUL-canvas-fix BROKEN — {broken_reason}")

    elapsed = time.time() - started
    print(f"\n(diagnostic completed in {elapsed:.1f}s)")

    # Non-zero exit if we could not read anything at all
    if not trans_entries and not nli_rows and not nli_fl_ids:
        print("FATAL: no data sources available; cannot diagnose")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
