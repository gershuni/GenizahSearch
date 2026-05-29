"""Spike 001 — corpus profiler. Sample one real PDF per folder and classify it,
so we can find the *sorts* of PDFs (and predict failure modes) across ~18K files
without reading them all. No OCR required.

Per sampled PDF, on a few sampled pages:
  - text_layer:   chars present? (else image-only -> OCR/D-F2 territory)
  - img_only:     pages dominated by images with ~no text
  - letterspace:  single-Hebrew-letter-token ratio (F-D fragmentation proxy)
  - nikud:        fraction of Hebrew codepoints that are nikud (vocalized text)
  - script:       heb vs latin token mix
  - columns:      rough 1 vs 2+ column guess from line x-center spread

Usage (from project root):
    python .planning/spikes/001-meiri-glyph-reorder-vs-current/profile_corpus.py
"""
from __future__ import annotations

import os
import sys

import fitz

ROOT = r"C:\Users\gersh\Dropbox\ספרים"


def _is_heb_letter(cp: int) -> bool:
    return 0x05D0 <= cp <= 0x05EA


def _is_nikud(cp: int) -> bool:
    return 0x05B0 <= cp <= 0x05C7


def first_real_pdf(folder: str) -> str | None:
    for dp, _, fs in os.walk(folder):
        for f in sorted(fs):
            if f.lower().endswith(".pdf") and not f.startswith("._"):
                p = os.path.join(dp, f)
                try:
                    if os.path.getsize(p) > 20000:  # skip placeholders/stubs
                        return p
                except OSError:
                    continue
    return None


def _sample_idxs(n: int, k: int = 4) -> list[int]:
    if n <= k:
        return list(range(n))
    return [int(n * frac) for frac in (0.2, 0.4, 0.6, 0.8)]


def profile(path: str) -> dict:
    doc = fitz.open(path)
    try:
        n = doc.page_count
        text_chars = 0
        img_pages = 0
        text_pages = 0
        heb_letters = nikud = 0
        heb_tok = lat_tok = single_heb_tok = 0
        left_lines = right_lines = 0
        for i in _sample_idxs(n):
            page = doc[i]
            txt = page.get_text("text")
            text_chars += len(txt.strip())
            imgs = page.get_images(full=True)
            if len(txt.strip()) < 20 and imgs:
                img_pages += 1
            if len(txt.strip()) >= 20:
                text_pages += 1
            for ch in txt:
                cp = ord(ch)
                if _is_heb_letter(cp):
                    heb_letters += 1
                elif _is_nikud(cp):
                    nikud += 1
            for t in txt.split():
                heb = [c for c in t if _is_heb_letter(ord(c))]
                lat = [c for c in t if c.isascii() and c.isalpha()]
                if heb:
                    heb_tok += 1
                    if len(heb) == 1:
                        single_heb_tok += 1
                elif lat:
                    lat_tok += 1
            # column guess: line center vs page center
            pw = page.rect.width
            d = page.get_text("dict")
            for blk in d.get("blocks", []):
                for ln in blk.get("lines", []):
                    bb = ln.get("bbox")
                    if not bb:
                        continue
                    cx = (bb[0] + bb[2]) / 2
                    if cx < pw * 0.45:
                        left_lines += 1
                    elif cx > pw * 0.55:
                        right_lines += 1
        ls = round(single_heb_tok / heb_tok, 2) if heb_tok else 0.0
        nk = round(nikud / heb_letters, 2) if heb_letters else 0.0
        total_lat = lat_tok + heb_tok
        scr = "heb" if heb_tok > 3 * max(lat_tok, 1) else ("lat" if lat_tok > 3 * max(heb_tok, 1) else "mix")
        cols = "2col?" if (left_lines > 8 and right_lines > 8) else "1col"
        kind = "IMAGE-ONLY" if (img_pages and text_pages == 0) else ("scan+text" if img_pages else "text")
        return {
            "pages": n, "kind": kind, "chars": text_chars,
            "letterspace": ls, "nikud": nk, "script": scr, "cols": cols,
        }
    finally:
        doc.close()


def main() -> int:
    cats = sorted(d for d in os.listdir(ROOT) if os.path.isdir(os.path.join(ROOT, d)))
    print(f"{'folder':<34}{'kind':<11}{'pages':>6}{'lspace':>7}{'nikud':>6}{'scr':>5}{'cols':>6}  file")
    print("-" * 120)
    for c in cats:
        base = os.path.join(ROOT, c)
        pdf = first_real_pdf(base)
        if not pdf:
            print(f"{c[:33]:<34}(no real pdf)")
            continue
        try:
            r = profile(pdf)
            print(f"{c[:33]:<34}{r['kind']:<11}{r['pages']:>6}{r['letterspace']:>7}{r['nikud']:>6}"
                  f"{r['script']:>5}{r['cols']:>6}  {os.path.basename(pdf)[:42]}")
        except Exception as e:
            print(f"{c[:33]:<34}ERROR {type(e).__name__}: {str(e)[:50]}  ({os.path.basename(pdf)[:40]})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
