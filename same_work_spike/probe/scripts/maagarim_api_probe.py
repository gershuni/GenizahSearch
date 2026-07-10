# -*- coding: utf-8 -*-
"""Maagarim (Academy of the Hebrew Language) API recipe — PROVEN facts.

Discovered 2026-07-10 (no auth; browser User-Agent required; Cloudflare
fronted). Base = https://maagarim.hebrew-academy.org.il/Pages/ws/Arachim.asmx
POST JSON, header 'Content-Type: application/json; charset=utf-8'.

The pipeline work id `Ytext<N>` (Maagarim filename stem) == the API `misyzira`
integer N. VERIFIED: misyzira=689002 -> title "סיפורים, איגרת מקהילת קיירואן
אל יהודי התפוצות" == the xlsx row for Ytext689002.

WORKING method GetYziraFull(misyzira) -> {yezira(html), authors, title,
  mesirot:[str], mador}. `mesirot` = the source manuscripts USED to prepare
  the edition (== the ##המסירה:## headers in the local AllTextsOnlyText files,
  but STRUCTURED). Example 689002 -> ['London, British Library, 1081',
  'New York, Jewish Theological Seminary (JTS), ENA, 1501, 1-7'].

TODO (this module's open puzzle) GetYzira(...) -> also returns `nosafot`
  (מסירות נוספות = additional witness manuscripts the Academy knows about but
  did NOT use for the edition — Hillel's requested 4th novelty channel) and
  `mahadurotnosafot`. Its request body `h` has these fields (from mainJs):
  misyzira, mm15, mm, tnua, page, tabNum, mismilim, missade, nPage, nMaxPage,
  size, sort, typeMmBm, takeMaxPage, maxWidthInX, strokeStyle.
  Type facts nailed so far: `mm15` is List<String[]> (send [] or [["..."]]);
  sending mm15 as "" -> "Cannot convert String to List<String[]>". One other
  String-typed field rejects arrays. The exact minimal working body is left
  for the harvest agent to finish (iterate the ~2 ambiguous fields).
  Fallback if GetYzira stays stubborn: the `nosafot` HTML is also rendered
  into the work page tab #liMsirotNosafot — scrape the rendered page.
"""
import json
import time
import urllib.request

BASE = "https://maagarim.hebrew-academy.org.il/Pages/ws/Arachim.asmx"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
THROTTLE_S = 1.0     # be polite — one request/second


def call(method, params):
    body = json.dumps(params).encode("utf-8")
    req = urllib.request.Request(
        f"{BASE}/{method}", data=body,
        headers={"Content-Type": "application/json; charset=utf-8",
                 "User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        d = json.loads(r.read().decode("utf-8"))
    v = d.get("d", d)
    return json.loads(v) if isinstance(v, str) else v


def get_yzira_full(misyzira):
    """PROVEN. Returns dict with mesirot (used source MSS), title, authors."""
    time.sleep(THROTTLE_S)
    return call("GetYziraFull", {"misyzira": int(misyzira)})


if __name__ == "__main__":
    import sys
    mid = int(sys.argv[1]) if len(sys.argv) > 1 else 689002
    r = get_yzira_full(mid)
    print("title:", r.get("title"))
    print("mesirot:", r.get("mesirot"))
