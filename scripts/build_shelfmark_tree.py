#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build ``shelfmark_tree_data/shelfmark_tree.db``: every manuscript on its library's shelf.

    python scripts/build_shelfmark_tree.py [--fist fist_data/FIST.db] [--libraries libraries.csv]
                                           [--out shelfmark_tree_data/shelfmark_tree.db] [--force]

Plan: docs/plans/SHELFMARK_TREE_PICKER_PLAN.md (private repository), §3.1 as amended by §10.
``scripts/verify_shelfmark_tree_sidecar.py`` gates the file; ``shared/shelfmark_tree_service.py`` reads it.

The tree is library > collection > volume > shelfmark, as the FGP "find a fragment" screen has it,
taken from the FGP dump (``FIST.db``, read-only). Only the manuscripts of ``libraries.csv`` are
leaves; a node with none is left out.

Placement (one leaf per placement; a manuscript may sit at two places):

* **FIST.** Every active inventory row linked to the manuscript through ``dbo_InventoryAlma``
  (``dbo_inventoryVsAlma`` is not used, §10.2) in an active collection of an active library; in its
  volume when the volume is active, else directly in the collection.
* **The rest** (no active link), by the owner's rules O1-O5 (§10.11), each inside the FIST
  libraries the record's ``library_code`` is tied to:

  0. a record whose call numbers name another institution in their long form (``<institution>,
     Ms. ...``; ``OWN_HEADS`` lists what counts as the code's own) goes to "Other shelfmarks";
  1. **exact**: a call number equal, token by token (numbers kept apart, so ``T-S AS 84.1`` is not
     ``T-S AS 8.41``), to an inventory shelfmark of those libraries. Matches in two collections are
     a collision ("Other"). A match is used only where, on the placed manuscripts, the exact step
     finds the manuscript's own collection at least ``PURITY`` of the time over ``MIN_CASES`` (the
     trust is per code and collection); then the leaf sits beside that row, or, when the matches
     are two rows of one collection, in the collection outside any volume;
  2. **collection head**: the call number up to its first digit names one collection: at least
     ``MIN_CASES`` placed manuscripts of the code carry that head and ``PURITY`` of their
     placements are in one collection of the code's libraries. Outside any volume (O4). Two
     collections: "Other";
  3. **single collection**: a code that names a holding institution (not ``COLLECTOR_CODES``, O5),
     tied to one library that has one active collection, with at least ``MIN_CASES`` placed
     manuscripts, takes its leftovers;
  4. otherwise "Other shelfmarks" under the code's library (its own fallback node when FIST has
     none).

Order: libraries by size (``TOP_LIBRARY_MIN`` manuscripts or more, largest first), then the rest
alphabetically (D9). An Oxford leaf carries its Neubauer Part (``libraries.csv`` column 1) as
information, not as a level (owner, 2026-11-19). Collections by ``DisplayOrder``; volumes in FGP's
own order (``OrderBy1Num``, ``OrderBy2Num``, ``OrderBy3Text``) with the numbers and Roman numerals
of one series in their own (``order_volumes``); shelfmarks by the ``OB*`` columns, then the
shelfmark's natural order. A node holding more than ``RANGE_MAX`` shelfmarks directly is divided
into ranges by round item numbers, as FGP's "Moss. IV,1 - 100" (``split_ranges``, D5). Every leaf
has a position in one shelf order across the whole tree, so previous / next cross ranges, volumes
and collections (§10.3), and every node owns the contiguous positions ``lo..hi``.

A catalogue FGP lists a collection by (Oxford's Neubauer-Cowley, ``dbo_InventoryCatalogEntry``) is a
group beside the collection's volumes, each entry holding the manuscripts FGP ties to it: a second
place for them (``how`` "catalogue", ``add_catalogues``), after their place on the shelf. Shelfmarks
that would show one label in a node show what their call number adds (``tell_apart``), and a
manuscript has one place per node and label. The records of ``LEFT_OUT`` have no place.

Read-only on its inputs; writes a temporary file and swaps it in. Refuses to write a file with
fewer manuscripts than the one it replaces unless ``--force``.
"""
from __future__ import annotations

import argparse
import csv
import functools
import hashlib
import os
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.browse_map_utils import LIBRARY_CODES, natural_sort_key  # noqa: E402
from shared.catalog_shelfmark import head_codes  # noqa: E402

#: The sidecar schema; shared/shelfmark_tree_service.py serves only this version.
SCHEMA_VERSION = "1.1.0"      # 1.1.0: leaf.aliases
DEFAULT_OUT = ROOT / "shelfmark_tree_data" / "shelfmark_tree.db"
DEFAULT_FIST = ROOT / "fist_data" / "FIST.db"
DEFAULT_LIBRARIES = ROOT / "libraries.csv"

MIN_CASES = 5
PURITY = 0.98
#: A code is also tied to a second FIST library holding this share (and MIN_CASES) of its placed
#: manuscripts.
INSTITUTION_SHARE = 0.02
#: Libraries with at least this many manuscripts come first, largest first (D9).
TOP_LIBRARY_MIN = 1000
#: A node holding more shelfmarks than this directly is divided into ranges, as FGP divides a large
#: volume ("Moss. II", 643 shelfmarks, is three ranges there; "Moss. III", 597, is whole).
RANGE_MAX = 600
#: No range holds more than this.
RANGE_SIZE = 500
#: A range is as wide, in round item numbers, as brings it nearest to this many shelfmarks: FGP's
#: "Moss. IV,1 - 100" (about 200 each) and "2nd Series: P 1 - 200".
RANGE_TARGET = 200
RANGE_WIDTHS = (10, 20, 50, 100, 200, 500, 1000, 2000, 5000)
#: FIST's "Undefined Shelfmarks" collection is not a place.
UNDEFINED_COLLECTION = 99

#: Codes that name a collector, a former owner or a catalogue, not the institution that holds the
#: manuscripts: step 3 never places by them (O5; owner, 2026-11-19).
COLLECTOR_CODES = frozenset({
    "Harkavy", "Mosseri", "Lutzki", "Sofer", "Sassoon", "Schoeyen", "Lehmann", "Bisno", "Solomon",
    "Combs", "Lehnardt", "Reinach", "Wallach", "Boesky", "Benayahu", "Salmon", "Adler", "Shapira",
    "Karp", "Weiss", "Nahum", "Mehlman", "Corwin", "Allony", "AllonyLoew", "AllonyKupf",
})

#: Long-form heads (``<head>, Ms. ...``) that count as the code's own institution besides its name
#: in ``LIBRARY_CODES`` and what ``shared.catalog_shelfmark.head_codes`` already gives it (owner,
#: 2026-11-19). Any other head, a former owner's name included, is another institution (O2).
OWN_HEADS = {
    # Halpern and Sassoon: former owners of manuscripts the BL holds as "Or." (owner, 2026-11-20)
    "BL": ("lon bl", "gaster, moses collection", "halpern, joseph", "sassoon, david solomon"),
    "Manchester": ("un of manchester library", "gaster, moses collection", "gaster heb."),
    "JTS": ("ny jts", "adler, elkan nathan"),
    "RNL": ("nl of russia", "constantin von tischendorf collection"),
    "Oxford": ("the bodleian lib un of ox",),
    "AIU": ("library of the alliance israélite universelle", "library of the alliance israélite un"),
    "Katz": ("library at the herbert d. katz center for advanced judaic studies, university of pennsylvania",
             "library at the herbert d. katz center for advanced judaic studies, university of pennsylvania cajs rar",
             "rar", "phil up"),
    "Leeds": ("roth, cecil",),
    "Geneva": ("library of geneva.",),
    "Sofer": ("sofer david london",),
    "Allony": ("the national library of israel",),
    "AllonyLoew": ("state library of berlin",),
    "AllonyKupf": ("the royal danish library",),
    "Senckenberg": ("university library johann christian senckenberg",),
    "Heidelberg": ("university library heidelberg",),
    "IOM": ("institute of oriental manuscripts, the russian academy of sciences",),
    "TCD": ("the library of trinity college dublin",),
    "Turin": ("oriental studies library of the department of humanities of the university of turin",),
    "UPenn": ("museum of archaeology and anthropology, university of pennsylvania",),
    "Chetham": ("chetham's library",),
}

OB_COLUMNS = ("OBCollectionCodeOrder", "OBBoxNumber", "OBBox", "OBVolume", "OBVolumeText", "OBFolio",
              "OBFolioText", "OBFolioMtf1", "OBFolioMtf1Text", "OBFolioMtf2", "OBFolioMtf2Text")
#: Libraries shown inside another (owner, 2026-11-19): FIST library id -> (the library it joins, its
#: collection's name there in English and Hebrew). The joined library also keeps a shortcut in the
#: list of libraries. The matching rules still run on FIST's own libraries.
MERGE_INTO = {
    177: (233, "Mosseri Collection", "אוסף מוצירי"),
    248: (233, "Lewis-Gibson (formerly Westminster College)", "לואיס-גיבסון (לשעבר ווסטמינסטר)"),
}
#: Library names as "City, Library" in both languages (owner, 2026-11-19): FIST library id ->
#: (English, Hebrew or None to keep FGP's). FGP's English names are inconsistent; its Hebrew ones
#: follow this form, and the ten it lacks (written "NULL") are supplied here. Fourteen more are
#: given here in one writing (owner, 2026-11-20): FGP writes the geresh with three different
#: characters ("מנצ'סטר"; here always U+0027, the one a keyboard types, so a filter finds it), Los
#: Angeles two ways, and six names without their city.
LIBRARY_NAMES = {
    13: ("Turin, National University Library", None),
    58: ("Vatican, Apostolic Library", None),
    79: ("New York, Jewish Theological Seminary (JTS)", None),
    81: ("New York, Columbia University", None),
    86: ("Cincinnati, Hebrew Union College", None),
    90: ("Philadelphia, Center for Advanced Judaic Studies", None),
    91: ("Philadelphia, University of Pennsylvania Museum", None),
    92: ("Cambridge (Mass.), Harvard University", None),
    103: ("Berlin, State Library", None),
    122: ("Munich, Bavarian State Library", "מינכן, ספריית המדינה הבווארית"),
    130: ("Frankfurt, University Library", "פרנקפורט, ספריית האוניברסיטה"),
    140: ("Copenhagen, Royal Library", None),
    147: ("Budapest, Library of the Hungarian Academy of Sciences", "בודפשט, ספריית האקדמיה ההונגרית למדעים"),
    168: ("Strasbourg, National and University Library", "שטרסבורג, הספרייה הלאומית והאוניברסיטאית"),
    169: ("Paris, Alliance Israélite Universelle", None),
    170: ("Paris, National Library of France", None),
    177: ("Cambridge, Mosseri Collection", None),
    183: ("Basel, University Library", None),
    195: ("Holon, Y. L. Nahum Collection", 'חולון, אוסף י"ל נחום'),
    196: ("Haifa, University of Haifa", "חיפה, אוניברסיטת חיפה"),
    205: ("Jerusalem, Y. Mehlman Collection", None),
    209: ("Jerusalem, Ben-Zvi Institute", None),
    210: ("Jerusalem, Schocken Institute", "ירושלים, מכון שוקן למחקר היהדות"),
    211: ("Jerusalem, National Library of Israel", None),
    224: ("Ramat Gan, Bar-Ilan University", "רמת גן, אוניברסיטת בר-אילן"),
    229: ("St. Petersburg, National Library of Russia", "סט פטרבורג, הספרייה הלאומית של רוסיה"),
    230: ("Kyiv, Vernadsky National Library", None),
    231: ("Moscow, Russian State Library", None),
    233: ("Cambridge, University Library", None),
    235: ("Oxford, Bodleian Library", "אוקספורד, הספרייה הבודליאנית"),
    236: ("Birmingham, Selly Oak College", "בירמינגהם, סלי אוק קולג'"),
    238: ("London, British Library", "לונדון, הספרייה הבריטית"),
    240: ("London, D. S. Sassoon Collection", None),
    242: ("Manchester, John Rylands Library", "מנצ'סטר, ספריית ג'ון ריילנדס"),
    248: ("Cambridge, Lewis-Gibson (formerly Westminster College)", "קיימברידג', לואיס-גיבסון (לשעבר ווסטמינסטר)"),
    255: ("Vienna, Austrian National Library", "וינה, הספרייה הלאומית של אוסטריה"),
    285: ("Toronto, Friedberg Collection", None),
    291: ("London, D. Sofer Collection", "לונדון, אוסף ד. סופר"),
    292: ("Leeds, Brotherton Library (Roth Collection)", None),
    293: ("Geneva, Library of Geneva", "ז'נבה, ספריית ז'נבה"),
    299: ("Oslo and London, Schøyen Collection", None),
    300: ("Los Angeles, S. Weiss Collection", "לוס אנג'לס, אוסף ס. וייס"),
    377: ("Boesky Collection", "אוסף בוסקי"),
    400: ("Tel Aviv, Tel Aviv University", "תל אביב, אוניברסיטת תל אביב"),
    401: ("Jerusalem, Central Archives for the History of the Jewish People", None),
    402: ("Montreal, McGill University Library", None),
    403: ("Dublin, Trinity College Library", "דבלין, טריניטי קולג'"),
    404: ("Ann Arbor, University of Michigan Library", None),
    405: ("Washington, Freer Gallery (Smithsonian)", None),
    406: ("Princeton, Theological Seminary", None),
    407: ("Heidelberg, Institute of Papyrology", None),
    408: ("St. Petersburg, Institute of Oriental Manuscripts", None),
    409: ("Mainz, Jewish Community", None),
    410: ("Jerusalem, H. Solomon Collection", None),
    411: ("Manchester, Chetham's Library", "מנצ'סטר, ספריית צ'תם"),
    412: ("Paris, Institut de France", None),
    413: ("Heidelberg, University Library", None),
    415: ("Private Collection 2 (anonymous)", None),
    500: ("Cairo, Egyptian Antiquities Authority", None),
    501: ("New York, M. Lehmann Collection", None),
    502: ("New York, Yeshiva University", None),
    503: ("Baltimore, Goldsmith Museum", None),
    504: ("Los Angeles, G. Combs Collection", "לוס אנג'לס, אוסף ג. קומבס"),
    505: ("London, Halpern Collection", None),
    507: ("Jerusalem, B. Chapira Collection", None),
    508: ("Mainz, A. Lehnardt Collection", None),
    510: ("Berlin, Jewish Community", None),
    511: ("Durham (N.C.), Duke University", None),
    513: ("Paris, T. Reinach Collection", None),
    514: ("Los Angeles, Temple Israel of Hollywood", "לוס אנג'לס, טמפל ישראל של הוליווד"),
    515: ("Warsaw, Jewish Community", None),
    516: ("Erfurt, Jewish Community", None),
    517: ("Los Angeles, J. Bisno Collection", "לוס אנג'לס, אוסף ג'י ביסנו"),
    518: ("Chicago, Oriental Institute Museum", "שיקגו, מוזיאון מכון המזרח, אוניברסיטת שיקגו"),
}
#: A collection with more volumes than this is divided into ranges by volume number, each holding
#: at most ``VOLUME_RANGE_SIZE`` volumes, on round bounds (1-1000, then 500, 200, 100 or 50 wide).
VOLUME_RANGE_MIN = 150      # T-S AS (225 volumes), T-S Bound (313), MS Lutzki (177) are divided too
VOLUME_RANGE_SIZE = 60
VOLUME_RANGE_WIDTHS = (1000, 500, 200, 100, 50, 20, 10)
#: FGP's own ranges of a collection's volumes (read from its "Select by collection" screen,
#: 2026-11-19 and 2026-11-20): FIST collection id -> (label, first, last) by volume number. Volumes
#: outside them fall to the prefix and number rules below.
FGP_RANGES = {
    401: (("ENA 1-1000", 1, 1000), ("ENA 1001-2000", 1001, 2000), ("ENA 2001-2500", 2001, 2500),
          ("ENA 2501-2800", 2501, 2800), ("ENA 2801-3000", 2801, 3000), ("ENA 3001-3200", 3001, 3200),
          ("ENA 3201-3400", 3201, 3400), ("ENA 3401-3600", 3401, 3600), ("ENA 3601-3800", 3601, 3800),
          ("ENA 3801-4000", 3801, 4000), ("ENA 4001-4200", 4001, 4200)),
    106: (("T-S NS 1-200", 1, 200), ("T-S NS 201-400", 201, 400)),
    102: (("T-S 6F - 8K", 6, 8), ("T-S 10C - 18K", 10, 18)),
}
#: FGP's own groups of a collection's volumes where they are not ranges of a number (the same
#: screen): FIST collection id -> (label, a pattern the volume's name matches; None: every other
#: [, the label in Hebrew]).
FGP_GROUPS = {
    750: (("MS heb.a - d", re.compile(r"^MS\s+heb\.\s*[a-d]\b", re.IGNORECASE)),
          ("MS heb.e - g", re.compile(r"^MS\s+heb\.\s*[e-g]\b", re.IGNORECASE)),
          ("Other", None, "אחר")),
}
#: A catalogue FGP lets a collection be browsed by, beside its shelfmarks (``CODE_Catalog`` id ->
#: the name of that list, English and Hebrew). Its entries are ``dbo_InventoryCatalogEntry``.
CATALOGUE_NAMES = {
    234: ("By Neubauer-Cowley catalogue", "לפי קטלוג נויבאואר-קאולי"),
}
#: Collections shown as FGP's groups of them alone, the groups standing in the collection's place in
#: its library (owner, 2026-11-20: instead of "T-S Bound", "T-S 6F - 8K" and "T-S 10C - 18K").
FGP_IN_PLACE = frozenset({102})
#: Collections that are only their library's name over a few of its shelfmarks (owner, 2026-11-20:
#: Lehmann's "Lehman" holds four, of its "Ms. FR"; Kyiv's "Kiev" and Ben-Zvi's "Ben Zvi" alike).
#: The records the library's "Other shelfmarks" would hold stand in it, a collector's code or not,
#: and it is divided by series as "Other shelfmarks" is. A library of this one collection then
#: shows the series themselves: a level with one child is passed through.
LIBRARY_SERIES = frozenset({175, 220, 800})
#: Collections divided by volume number, a group for each (owner, 2026-11-20: Toronto's
#: "Friedberg MSS 9-001", "9-002", "9-003", and "9-004" holding "9-004/1" .. "9-004/32"; Sofer's
#: "71/1" .. in "71" and "108 f. 16/1" in "108", Guenzburg's "Ms. 920/1" .. in "Ms. 920", Berlin
#: Community's "152.1" .. in "152"). Not HUC: our records there are one to a number ("1001",
#: "1002", ...), FGP's "1088.1" .. rows mostly linked to none.
BY_VOLUME_NUMBER = frozenset({820, 870, 922, 931})
#: A node that mixes several forms of shelfmark is divided by form first; a form of at least this
#: many shelfmarks has ranges of its own (Budapest: "1 - 463", the Roman numerals, "M 1 - 117").
FAMILY_MIN = 20
#: Empty records beside the record of the same shelfmark (owner, 2026-11-20, from RNL "Box D.37" and
#: "Box D.38"): no title, no page in the corpus (``Transcriptions.txt``), nothing but a call number,
#: where another record in the same place has pages or a title. Mostly records FGP made for its
#: rows (99005...). Listed once from the corpus: sys_id, shelfmark, the record kept.
LEFT_OUT_FILE = Path(__file__).with_name("shelfmark_tree_left_out.csv")


def _left_out() -> list:
    with open(LEFT_OUT_FILE, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


#: Records left out of the tree (sys_id -> why): the gate does not ask for a place for them.
LEFT_OUT: dict = {r["sys_id"]: f'empty beside {r["kept"]} ("{r["shelfmark"]}")' for r in _left_out()}
#: An empty record left out -> the record of its shelfmark kept in its place.
REPLACED_BY: dict = {r["sys_id"]: r["kept"] for r in _left_out()}
#: A record held back because its long form names another institution is shown under that
#: institution's library when the name is one of these (FIST library ids), in its "Other shelfmarks":
#: the 7 Vernadsky records coded CUL (open issue) are Kyiv's "Or." manuscripts, not CUL's Oriental.
INSTITUTION_LIBRARY = {
    "vernadsky national library of ukraine": 230,
    # a Harkavy record that is "The National Library of Israel Ms. Heb. 1151.3" (2026-11-20)
    "the national library of israel": 211,
}
#: A collection of more than this many volumes whose volumes are written with several prefixes (Oxford's
#: "MS heb. a." ... "MS heb. g.", BL's "OR" and "GASTER", "T-S A" ... "T-S K") is grouped by prefix.
PREFIX_GROUP_MIN = 100
#: Shelfmarks FGP keeps in a collection of its own that our records name by a prefix (owner,
#: 2026-11-19): library code -> ((prefix tokens, FIST collection name), ...).
PREFIX_COLLECTIONS = {
    "RNL": ((("box",), "RNL Boxes"),),
}
#: "Other shelfmarks" is divided by the shelfmarks' prefix (Box D., Ms., ...) when it holds at least
#: this many and more than one prefix.
GROUP_MIN = 10
OTHER_NAME_EN = "Other shelfmarks"
OTHER_NAME_HE = "מספרי מדף אחרים"
#: Records with no library code that no library is known for either.
NO_CODE_NAME = "Location unknown"
NO_CODE_NAME_HE = "מקום לא ידוע"
#: Records ``libraries.csv`` gives neither a call number nor a library code, by the library the
#: catalogue's own notes name (owner, 2026-11-20): sys_id -> library code. In a library with one
#: collection they stand in it, else under "Other shelfmarks"; they are shown by their system number.
RECORD_LIBRARY = {
    "990038631880205171": "Oxford",
    "990037567460205171": "RNL",
    "990037565670205171": "RNL",
    "990037566120205171": "RNL",
    "990037565150205171": "RNL",
    "990001839260205171": "Manchester",
    "990001832700205171": "Mosseri",
    "990001832720205171": "Mosseri",
    "990001827970205171": "Carpi",
    "990001024600205171": "HebrewAcademy",
    "990001383440205171": "Senckenberg",
}
#: Hebrew names of libraries known only by a library code (owner, 2026-11-20): code -> name.
CODE_NAMES_HE = {
    "Karp": "אוסף אברהם קארפ",
    "Benayahu": "אוסף בניהו",
    "Salmon": "אוסף חוה סלמון",
    "Wallach": "אוסף ולך",
    "Yale": "ניו הייבן, אוניברסיטת ייל",
}
#: Libraries of ``RECORD_LIBRARY`` that have no code elsewhere: code -> (English, Hebrew).
EXTRA_LIBRARIES = {
    "Carpi": ("Jerusalem, D. Carpi Collection", "ירושלים, אוסף ד. קרפי"),
    "HebrewAcademy": ("Jerusalem, Academy of the Hebrew Language", "ירושלים, האקדמיה ללשון העברית"),
}
#: "name": held back as "other", then placed by its name (``cohere`` step 4)
HOW = ("fist", "exact", "collection", "single", "name", "other")
#: A second place of a manuscript FGP has on its shelf: under its entry in a catalogue. Not one of
#: ``HOW``: every manuscript is counted once there, by its place on the shelf.
CATALOGUE = "catalogue"
_FILE_NAME = re.compile(r"\.(?:tiff?|jpe?g|png|pdf)$|_", re.IGNORECASE)

_TOKEN = re.compile(r"[^\W\d_]+|\d+")
_HEAD_RE = re.compile(r"^(?P<head>.+?)[,\s]+(?:Ms\.?|MS\.?)\s+\S")


def key(text: str) -> tuple:
    """Letters and numbers as separate tokens, numbers as ints: punctuation and case drop out,
    number boundaries stay."""
    return tuple(int(t) if t.isdigit() else t for t in _TOKEN.findall((text or "").lower()))


def head(text: str) -> tuple:
    """The tokens of a call number before its first digit."""
    m = re.match(r"\D*", text or "")
    return tuple(_TOKEN.findall(m.group(0).lower()))


def institution_head(variant: str) -> Optional[str]:
    """The institution head of a long form ``<head>, Ms. <rest>``, lowercased, or None."""
    m = _HEAD_RE.match(variant or "")
    return " ".join(m.group("head").lower().split()) if m else None


def display_shelfmark(variants: list[str]) -> str:
    """The shelfmark the apps show: the shortest call number, the first on a tie
    (``MetadataManager``'s CSV loader)."""
    shelf = variants[0] if variants else ""
    for v in variants:
        if v and len(v) < len(shelf):
            shelf = v
    return shelf


@functools.lru_cache(maxsize=1)
def _institution_names() -> tuple:
    names = {" ".join(str(n).lower().split()) for c, n in LIBRARY_CODES.items() if c != "LOCAL"}
    names |= {h for hs in OWN_HEADS.values() for h in hs}
    return tuple(sorted((n for n in names if len(n) > 6), key=len, reverse=True))


def clean_label(variants: list[str]) -> str:
    """A shelfmark as a reader looks for it: no image file names, no institution written out
    (``The National Library of Russia Box D.7`` -> ``Box D.7``). A plain call number is preferred,
    the shortest; a long form, cut after its institution, only when there is no plain one (a BL record
    is ``Or. 10587``, not the ``Ms. 289`` of its Gaster long form)."""
    names = _institution_names()
    plain, cut = [], []
    for v in variants:
        if not v or _FILE_NAME.search(v):
            continue
        m = _HEAD_RE.match(v)
        if m:
            at = v.lower().find("ms", len(m.group("head")))
            if at >= 0:
                cut.append(v[at:].strip(" ,"))
            continue
        low = v.lower()
        named = next((name for name in names if low.startswith(name)), None)
        if named:
            cut.append(v[len(named):].strip(" ,"))
        else:
            plain.append(v)
    out = [v for v in (plain or cut) if v]
    if not out:
        return display_shelfmark(variants)
    # A bare "Ms. 391" is a catalogue number (Gaster's); a call number with its own prefix ("Or.
    # 10112D") is what the shelf is written with.
    with_number = [v for v in out if re.search(r"\d", v)]       # "more" or "Gaster" alone is no shelfmark
    out = with_number or out
    named_prefix = [v for v in out if label_prefix(v).lower().rstrip(".") not in ("", "ms")]
    out = named_prefix or out
    return min(out, key=lambda x: (len(x), out.index(x)))


def institution_label(variants: list[str], named_head: str) -> str:
    """A record shown in the library its long form names, written as that library writes it: its
    call numbers without those another institution's long forms give (a Harkavy record that is
    "The National Library of Israel Ms. Heb. 1151.3=4" is "Ms. Heb. 1151.3=4", not "Ms. P 15")."""
    others = {clean_label([v]) for v in variants if institution_head(v) not in (None, named_head)}
    kept = [v for v in variants if institution_head(v) in (None, named_head) and v not in others]
    return clean_label(kept) if kept else clean_label(variants)


def label_prefix(label: str) -> str:
    """The part of a shelfmark before its first number (``Box D.7`` -> ``Box D.``)."""
    m = re.match(r"\D*", label or "")
    return m.group(0).strip(" ,") if m else ""


def volume_display_name(desc: str, shelves) -> str:
    """FGP names some volumes without the punctuation their shelfmarks have (AIU "IA" for "I.A.1",
    "I.A.2"): then the shelfmarks' shared prefix, as they write it ("I.A"). A name that differs
    otherwise (Lewis-Gibson "Glass" for "L-G Glass.2") is kept."""
    shelves = list(shelves)
    if not desc or not shelves:
        return desc
    prefix = os.path.commonprefix(shelves)
    prefix = re.sub(r"\d+$", "", prefix).rstrip(" .,")

    def bare(x):
        return re.sub(r"[\W_]", "", x.lower())

    if prefix and prefix != desc and bare(prefix) == bare(desc):
        return prefix
    return desc


def own_heads(code: str) -> frozenset:
    names = {" ".join(str(LIBRARY_CODES.get(code, "")).lower().split())} - {""}
    return frozenset(names | set(OWN_HEADS.get(code, ())))


def foreign_heads(code: str, variants: list[str]) -> list[str]:
    """Long-form heads of a record that are not its code's own institution (O2, O5)."""
    own = own_heads(code)
    out = []
    for v in variants:
        h = institution_head(v)
        if h is None or h in own or (code and code in head_codes(h)):
            continue
        out.append(h)
    return sorted(set(out))


def _text(value) -> str:
    """A FGP text value: the dump writes some missing names as the text "NULL" (11 Hebrew library
    names in the February 2026 dump)."""
    text = (value or "").strip() if isinstance(value, str) else ("" if value is None else str(value))
    return "" if text.upper() == "NULL" else text


def _ob_part(value):
    if value is None or value == "":
        return (1,)
    if isinstance(value, str):
        return (0, value.lower())
    return (0, float(value))


def ob_key(row: tuple, shelfmark: str) -> tuple:
    """Shelf order inside a node: the ``OB*`` columns (empty last), then the natural order."""
    return tuple(_ob_part(v) for v in row) + (tuple(natural_sort_key(shelfmark)),)


def _nat(text) -> tuple:
    return tuple((0, p) if isinstance(p, int) else (1, p) for p in natural_sort_key(text or ""))


# -- the coherence rules (owner's review of the picker, 2026-11-20; plan §10.15) -------------------
_ROMAN = re.compile(r"^(?=[IVX])(X{0,3})(IX|IV|V?I{0,3})([a-z]?)$")
_ROMAN_VALUE = {"I": 1, "V": 5, "X": 10}


def _roman(token: str):
    m = _ROMAN.match(token)
    if not m:
        return None
    digits = m.group(1) + m.group(2)
    total = 0
    for a, b in zip(digits, digits[1:] + " "):
        v = _ROMAN_VALUE[a]
        total += -v if b != " " and _ROMAN_VALUE[b] > v else v
    return total, m.group(3)


def roman_natural_key(text: str, letters_at: Optional[int] = None) -> tuple:
    """Natural order that reads Roman numerals as numbers: Moss. I, Ia, II, III, IIIa, IV ... IX,
    IXa (FGP's own order is by the letters). Only upper-case I, V, X count, with one small letter
    after ("IXa"); punctuation is not compared. ``letters_at``: the token there is a letter even
    when it looks like a numeral (Mosseri's series "V" among "A", "C", "Ch")."""
    out = []
    for i, m in enumerate(re.finditer(r"[A-Za-z]+|\d+", text or "")):
        t = m.group()
        if t.isdigit():
            out.append((0, int(t), ""))
            continue
        glued = m.start() > 0 and text[m.start() - 1].isdigit()     # "5557I": a letter after a number
        r = None if i == letters_at or glued else _roman(t)
        out.append((0, r[0], r[1]) if r else (1, t.lower(), ""))
    return tuple(out)


def sibling_order_keys(names: list) -> dict:
    """``roman_natural_key`` for siblings, reading a last word as a letter where most siblings of
    the same lead have a letter there that is no numeral (Mosseri "2nd Series: V" among "...: A")."""
    toks = {n: re.findall(r"[A-Za-z]+|\d+", n or "") for n in names}
    lead = defaultdict(list)
    for n, t in toks.items():
        if t:
            lead[tuple(x.lower() for x in t[:-1])].append(n)
    out = {}
    for _lead, group in lead.items():
        lasts = [toks[n][-1] for n in group if toks[n][-1].isalpha()]
        # letters where most of them are no numerals (Mosseri "Moss. Via" does not make "Moss. IX" a word)
        letters = sum(_roman(x) is None for x in lasts) > sum(_roman(x) is not None for x in lasts)
        for n in group:
            out[n] = roman_natural_key(n, letters_at=len(toks[n]) - 1 if letters else None)
    for n in names:
        out.setdefault(n, roman_natural_key(n))
    return out


def stem_of(label: str) -> str:
    """A shelfmark without its last part: "Box D.7" -> "Box D", "MS heb. c.9/1" -> "MS heb. c.9";
    "" when it has one part only."""
    # a library's size mark after the number is no part of it (NLI "Ms. Heb. 9463.1=4": quarto)
    m = re.match(r"^(.*\w)[./,](\w+)(?:=\d+)?$", label or "")
    return m.group(1) if m else ""


def bare_name(text: str) -> str:
    """A name without case or punctuation: "OR", "Or" and "Or." are one."""
    return re.sub(r"[\W_]", "", (text or "").lower())


def twin_key(name: str) -> str:
    """A volume's name for telling twins: case, spaces, dots, hyphens and underscores do not count
    ("OR", "Or.", "Com", "com."); a star or brackets do, so volumes FGP keeps apart stay apart
    ("MS heb. f.7*" is not "MS heb. f.7", "T-S F1(1)" is not "T-S F11")."""
    return re.sub(r"[\s.\-_]", "", (name or "").lower())


def split_form(label: str) -> tuple[str, str]:
    """A shelfmark's form (all before its first digit) and the rest: "Yevr. III B 1003" ->
    ("Yevr. III B ", "1003")."""
    m = re.search(r"\d", label or "")
    return ((label[:m.start()], label[m.start():]) if m else (label or "", ""))


def form_of(label: str) -> str:
    return split_form(label)[0].strip(" ,.;:")


def _match_tokens(text: str) -> list:
    """(token, abbreviated) for matching a shelfmark to a node's name; a leading "Ms." is not
    compared (AIU "Ms. I A 1" is in "I.A", Lewis-Gibson "Ms. Talm. II 15" in "Talmudica II"), and a
    number is its value ("Ms. heb. c. 07" is in "MS heb. c.7")."""
    toks = [(str(int(t)) if t.isdigit() else t.rstrip(".").lower(), t.endswith("."))
            for t in re.findall(r"[A-Za-z]+\.?|\d+", text or "")]
    if toks and toks[0][0] == "ms":
        toks = toks[1:]
    return toks


def name_matches(name_tokens: list, label_tokens: list) -> int:
    """How many tokens of a node's name lead the shelfmark (0: it does not). A shelfmark's
    abbreviation of at least three letters stands for the name's word ("Talm." for "Talmudica")."""
    if not name_tokens or len(name_tokens) > len(label_tokens):
        return 0
    for (n, _), (t, dot) in zip(name_tokens, label_tokens):
        if n != t and not (dot and len(t) >= 3 and n.startswith(t) and not t.isdigit()):
            return 0
    return len(name_tokens)


#: A shelfmark that begins with another library's siglum is that library's (FIST library ids):
#: one JTS record is "LON BL Or. 10578K f.10".
SIGLA = {("lon", "bl"): 238, ("cam", "cul"): 233, ("ny", "jts"): 79, ("ox", "bodl"): 235}
UNSPECIFIED_VOLUME = "unspecifiedvolume"


@dataclass
class Node:
    id: str
    kind: str                 # library | collection | volume | range | other
    parent: Optional[str]
    name_en: str
    name_he: str = ""
    short: str = ""
    private: int = 0
    order: tuple = ()
    children: list = field(default_factory=list)
    leaves: list = field(default_factory=list)   # [(sort key, Leaf)]
    aliases: list = field(default_factory=list)  # shelfmark heads a collection is also found by
    link: str = ""                                 # a shortcut: the node it opens
    fgp: tuple = ()                                # a volume's place in FGP's own order of its collection
    by_number: bool = False                        # a series: its shelfmarks by the numbers after it


@dataclass
class Leaf:
    id: str
    sys_id: str
    shelfmark: str
    shelf_label: str          # FIST's shelfmark for a FIST leaf, else ''
    inventory_id: Optional[int]
    how: str
    part: str = ""            # the Oxford (Neubauer) Part, ``libraries.csv`` column 1: information only
    label: str = ""           # what the picker shows: FGP's shelfmark, else ``clean_label``
    held: bool = False        # held back on purpose (its long form names another institution)
    aliases: str = ""         # the record's other call numbers (libraries.csv), " | ": found by a filter


class BuildError(RuntimeError):
    pass


def read_libraries(path) -> list[tuple[str, str, list[str], str, str]]:
    """(sys_id, library code, call numbers, Oxford Part, title) of every record."""
    rows = []
    with open(path, encoding="utf-8", newline="") as f:
        for row in csv.reader(f):
            if not row or not row[0].strip().isdigit():
                continue
            variants = [v.strip() for v in (row[2] if len(row) > 2 else "").split("|") if v.strip()]
            rows.append((row[0].strip(), (row[3].strip() if len(row) > 3 else ""), variants,
                         (row[1].strip() if len(row) > 1 else ""), (row[7].strip() if len(row) > 7 else "")))
    return rows


def build_tree(fist_path, libraries_path, log=print):
    """Read the inputs and return (nodes by id, library ids in order, stats, the FIST libraries
    each code is tied to). Raises BuildError."""
    con = sqlite3.connect(f"file:{Path(fist_path).resolve().as_posix()}?mode=ro", uri=True)
    q = lambda sql, *a: con.execute(sql, a).fetchall()  # noqa: E731
    try:
        moved = q("SELECT COUNT(*) FROM dbo_Inventory WHERE MoveToInventoryId IS NOT NULL "
                  "AND MoveToInventoryId <> 0 AND MoveToInventoryId <> ''")[0][0]
        if moved:
            raise BuildError(f"{moved} inventory rows have MoveToInventoryId; no rule is written for "
                             "moved rows yet (plan §10.2)")
        libraries = {lid: (_text(ac) or _text(eng) or str(lid), _text(eng) or _text(ac) or str(lid), _text(heb),
                           int(priv or 0))
                     for lid, ac, eng, heb, priv in
                     q("SELECT LibraryId, LibraryNameAc, LibraryNameEng, LibraryNameHeb, IsPrivate "
                       "FROM CODE_Library WHERE COALESCE(IsCanceledCode, 0) = 0")}
        collections = {cid: (lid, name or str(cid), order) for cid, lid, name, order in
                       q("SELECT CollectionId, LibraryId, CollectionName, DisplayOrder FROM CODE_Collection "
                         "WHERE COALESCE(IsCanceledCode, 0) = 0 AND CollectionId <> ?", UNDEFINED_COLLECTION)
                       if lid in libraries}
        volumes = {gid: (cid, desc or str(gid), (o1, o2, o3)) for gid, cid, desc, o1, o2, o3 in
                   q("SELECT InventoryGroupId, CollectionId, InventoryGroupDesc, OrderBy1Num, OrderBy2Num, "
                     "OrderBy3Text FROM dbo_InventoryGroup WHERE RecordStatus = 0")}
        inventory = {}
        for row in q(f"SELECT InventoryId, CollectionId, InventoryGroupId, Shelfmark, {', '.join(OB_COLUMNS)} "
                     "FROM dbo_Inventory WHERE RecordStatus = 0"):
            iid, cid, gid, shelf = row[:4]
            if cid not in collections:
                continue
            vol = gid if gid in volumes and volumes[gid][0] == cid else None
            inventory[iid] = (cid, vol, shelf or "", ob_key(row[4:], shelf or ""))
        links = defaultdict(list)
        for alma, iid in q("SELECT AlmaId, InventoryId FROM dbo_InventoryAlma"):
            if iid in inventory:
                links[str(alma)].append(iid)
        catalogues, catalogue_entries = {}, []
        if {"CODE_Catalog", "dbo_InventoryCatalogEntry"} <= {r[0] for r in q(
                "SELECT name FROM sqlite_master WHERE type = 'table'")}:
            catalogues = {cat: _text(acronym) or str(cat) for cat, acronym in
                          q("SELECT CatalogId, CatAcronym FROM CODE_Catalog WHERE COALESCE(IsCanceledCode, 0) = 0")}
            catalogue_entries = [r for r in q("SELECT CatalogId, InventoryId, CatalogEntry, OrderBy "
                                              "FROM dbo_InventoryCatalogEntry WHERE RecordStatus = 0")
                                 if r[0] in catalogues and r[1] in inventory and _text(r[2])]
    finally:
        con.close()

    records = read_libraries(libraries_path)
    by_sys = {}
    parts = {}
    shown_as = {}
    for sid, code, variants, part, _title in records:
        if sid in LEFT_OUT or sid in by_sys:
            continue
        shown_as[sid] = display_shelfmark(variants)
        by_sys[sid] = (code or RECORD_LIBRARY.get(sid, ""), [after_colon(v) for v in variants])
        parts[sid] = part
    lib_of_collection = {cid: v[0] for cid, v in collections.items()}
    collections_of_library = Counter(v[0] for v in collections.values())

    # -- where each code's placed manuscripts sit -----------------------------------------------
    placed = {sid: sorted(set(links[sid])) for sid in by_sys if links.get(sid)}
    # an empty record left out hands FGP's rows to the record of its shelfmark kept in its place,
    # where that record has none of its own (AIU "I.D.1": FGP linked its row to the empty record only)
    for sid, kept in REPLACED_BY.items():
        if kept in by_sys and kept not in placed and links.get(sid):
            placed[kept] = sorted(set(links[sid]))
    code_libs: dict[str, Counter] = defaultdict(Counter)
    for sid, iids in placed.items():
        for lid in {lib_of_collection[inventory[i][0]] for i in iids}:
            code_libs[by_sys[sid][0]][lid] += 1
    tied: dict[str, list[int]] = {}
    for code, counter in code_libs.items():
        if not code:
            continue                               # records without a code are no library's by that
        total = sum(counter.values())
        top = counter.most_common(1)[0][0]
        tied[code] = [lid for lid, n in counter.most_common()
                      if lid == top or (n / total >= INSTITUTION_SHARE and n >= MIN_CASES)]

    # A code FGP places nothing for, whose own name is an FGP library's ("Basel University Library":
    # FGP's "Basel, University Library", its one record there coded otherwise), is that library's.
    by_name_of_library = defaultdict(set)
    for lid, (_short, eng, _heb, _priv) in libraries.items():
        for name in {bare_name(eng), bare_name(LIBRARY_NAMES.get(lid, (eng, None))[0] or "")} - {""}:
            by_name_of_library[name].add(lid)
    named_ties = set()
    for code in sorted({cv[0] for cv in by_sys.values()} - set(tied) - {""}):
        hit = by_name_of_library.get(bare_name(str(LIBRARY_CODES.get(code, ""))), set())
        if len(hit) == 1:
            tied[code] = [next(iter(hit))]
            named_ties.add(code)
    if named_ties:
        log("codes tied to an FGP library by its name: " + ", ".join(sorted(named_ties)))

    fist_key = defaultdict(set)   # (library, key) -> {inventory id}
    for iid, (cid, _vol, shelf, _ob) in inventory.items():
        fist_key[(lib_of_collection[cid], key(shelf))].add(iid)

    def exact(code, variants):
        found = set()
        for v in variants:
            for lid in tied.get(code, ()):
                found |= fist_key.get((lid, key(v)), set())
        return found

    heads = defaultdict(Counter)   # (code, head) -> collections of every placement
    for sid, iids in placed.items():
        code, variants = by_sys[sid]
        cs = {inventory[i][0] for i in iids}
        for h in {head(v) for v in variants}:
            if h:
                for c in cs:
                    heads[(code, h)][c] += 1
    trust = defaultdict(Counter)   # (code, collection) -> right / wrong of the exact step
    for sid, iids in placed.items():
        code, variants = by_sys[sid]
        own = {inventory[i][0] for i in iids}
        for c in {inventory[i][0] for i in exact(code, variants)}:
            trust[(code, c)]["right" if c in own else "wrong"] += 1

    def trusted(code, cid):
        r = trust.get((code, cid))
        n = (r["right"] + r["wrong"]) if r else 0
        return n >= MIN_CASES and r["right"] / n >= PURITY

    def by_head(code, variants):
        allowed = set(tied.get(code, ()))
        out = set()
        for h in {head(v) for v in variants}:
            counter = heads.get((code, h))
            if not counter:
                continue
            n = sum(counter.values())
            top, k = counter.most_common(1)[0]
            if n >= MIN_CASES and k / n >= PURITY and lib_of_collection[top] in allowed:
                out.add(top)
        return out

    def single_collection(code):
        libs = tied.get(code, [])
        if code in COLLECTOR_CODES or len(libs) != 1 or (
                sum(code_libs[code].values()) < MIN_CASES and code not in named_ties):
            return None
        lid = libs[0]
        if collections_of_library[lid] != 1:
            return None
        return next(c for c, v in collections.items() if v[0] == lid)

    # the libraries shown by their series: library -> its collection of ``LIBRARY_SERIES``
    series_collection = {collections[c][0]: c for c in sorted(LIBRARY_SERIES) if c in collections}

    shelves_of_volume = defaultdict(list)
    for _iid, (_cid, vol, shelf, _ob) in inventory.items():
        if vol is not None and shelf:
            shelves_of_volume[vol].append(shelf)

    # -- nodes -------------------------------------------------------------------------------------
    nodes: dict[str, Node] = {}

    def library_node(lid):
        lid = MERGE_INTO.get(lid, (lid,))[0]
        nid = f"lib:{lid}"
        if nid not in nodes:
            short, eng, heb, priv = libraries[lid]
            en_name, he_name = LIBRARY_NAMES.get(lid, (eng, None))
            nodes[nid] = Node(nid, "library", None, en_name or eng, he_name or heb, short, priv)
        return nodes[nid]

    def code_node(code):
        nid = f"code:{code or '-'}"
        if nid not in nodes:
            if code in EXTRA_LIBRARIES:
                name, name_he = EXTRA_LIBRARIES[code]
            else:
                name, name_he = ((str(LIBRARY_CODES.get(code, code)), CODE_NAMES_HE.get(code, "")) if code
                                 else (NO_CODE_NAME, NO_CODE_NAME_HE))
            nodes[nid] = Node(nid, "library", None, name, name_he, code or "-")
        return nodes[nid]

    def collection_node(cid):
        nid = f"col:{cid}"
        if nid not in nodes:
            lid, name, order = collections[cid]
            parent = library_node(lid)
            name_he = ""
            if lid in MERGE_INTO:
                # a library shown inside another: after that library's own collections
                _target, merged_en, merged_he = MERGE_INTO[lid]
                if collections_of_library[lid] == 1:
                    name, name_he = merged_en, merged_he
                order = 1000 + (order or 0)
            nodes[nid] = Node(nid, "collection", parent.id, name, name_he,
                              order=((0, order) if order is not None else (1,), _nat(name)))
            parent.children.append(nid)
        return nodes[nid]

    def volume_node(gid):
        nid = f"vol:{gid}"
        if nid not in nodes:
            cid, desc, (o1, o2, o3) = volumes[gid]
            parent = collection_node(cid)
            desc = volume_display_name(desc, shelves_of_volume.get(gid, ()))
            # its place among its collection's volumes is settled by ``order_volumes``
            nodes[nid] = Node(nid, "volume", parent.id, desc, order=(roman_natural_key(desc),),
                              fgp=(_ob_part(o1), _ob_part(o2), _ob_part(o3)))
            parent.children.append(nid)
        return nodes[nid]

    def other_node(code, library=None):
        libs = [library] if library is not None else tied.get(code)
        parent = library_node(libs[0]) if libs else code_node(code)
        nid = f"other:{parent.id}"
        if nid not in nodes:
            nodes[nid] = Node(nid, "other", parent.id, OTHER_NAME_EN, OTHER_NAME_HE, order=((9,),))
            parent.children.append(nid)
        return nodes[nid]

    stats = Counter()
    foreign_seen: Counter = Counter()
    by_name = {(collections[c][0], collections[c][1].lower()): c for c in collections}

    # Each collection's own way of writing its shelfmarks: its name's head and the heads at least
    # MIN_CASES of its placed manuscripts carry with PURITY of the library's carrying them in it.
    head_count = defaultdict(Counter)          # library -> head -> placed manuscripts
    head_in = defaultdict(Counter)             # collection -> head -> placed manuscripts
    for sid, iids in placed.items():
        variants = by_sys[sid][1]
        for cid in {inventory[i][0] for i in iids}:
            heads_here = {head(inventory[i][2]) for i in iids if inventory[i][0] == cid}
            heads_here |= {head(clean_label(variants))}
            for h in heads_here - {()}:
                head_in[cid][h] += 1
                head_count[lib_of_collection[cid]][h] += 1
    collection_heads = defaultdict(set)        # library -> {(head, collection)}
    for cid, (lid, name, _order) in collections.items():
        own = head(name) or tuple(_TOKEN.findall(name.lower()))
        if own:
            collection_heads[lid].add((own, cid))
        for h, n in head_in[cid].items():
            if n >= MIN_CASES and n / head_count[lid][h] >= PURITY:
                collection_heads[lid].add((h, cid))

    def unreliable(code, cid):
        r = trust.get((code, cid))
        n = (r["right"] + r["wrong"]) if r else 0
        return n >= MIN_CASES and r["wrong"] / n > 1 - PURITY

    def named_collection(code, variants, label):
        """The one collection of the record's own library its shelfmark names: by the name before a
        colon ("T-S Ar.: T-S Ar.18(2).1141"), else by a collection's head ("Or. 85" -> Oriental)."""
        libs = tied.get(code, ())
        texts = [v for v in variants if v] + [label]
        for v in texts:
            if ":" in v:
                before = v.split(":", 1)[0].strip().lower()
                hit = {c for c, (lid, name, _o) in collections.items() if lid in libs and name.lower() == before}
                if len(hit) == 1:
                    return next(iter(hit))
        found = set()
        for v in texts:
            h = head(v.split(":", 1)[1] if ":" in v else v)
            for lid in libs:
                found |= {cid for key, cid in collection_heads[lid] if key and h[:len(key)] == key}
        if len(found) != 1:
            return None
        cid = next(iter(found))
        return None if unreliable(code, cid) else cid

    def prefix_collection(code, label):
        tokens = head(label)
        for prefix, name in PREFIX_COLLECTIONS.get(code, ()):
            if tokens[:len(prefix)] == prefix:
                for lid in tied.get(code, ()):
                    cid = by_name.get((lid, name.lower()))
                    if cid is not None:
                        return cid
        return None

    # the collections of each library that FGP places a manuscript of ours in
    held_collections = defaultdict(set)
    for iids in placed.values():
        for i in iids:
            held_collections[lib_of_collection[inventory[i][0]]].add(inventory[i][0])

    for sid, (code, variants) in by_sys.items():
        shown = shown_as[sid]
        label = clean_label(variants)
        if not variants:
            label = sid                  # no call number at all: by its system number (owner, 2026-11-20)
        nat = tuple(natural_sort_key(label))
        if sid in placed:
            for iid in placed[sid]:
                cid, vol, shelf, ob = inventory[iid]
                target = volume_node(vol) if vol is not None else collection_node(cid)
                target.leaves.append((ob + ((0,), nat), Leaf(f"inv:{iid}:{sid}", sid, shown, shelf, iid, "fist",
                                                                    parts[sid], shelf or label)))
            stats["fist"] += 1
            stats["fist_placements"] += len(placed[sid])
            continue
        leaf = Leaf(f"sys:{sid}", sid, shown, "", None, "other", parts[sid], label)
        foreign = foreign_heads(code, variants)
        target, sort = None, ((9,), nat)
        elsewhere = None
        if foreign:
            leaf.held = True
            stats["other_foreign"] += 1
            foreign_seen.update((code, h) for h in foreign)
            named_libs = {INSTITUTION_LIBRARY[h] for h in foreign if h in INSTITUTION_LIBRARY}
            if len(named_libs) == 1 and len(foreign) == 1 and next(iter(named_libs)) in libraries:
                elsewhere = next(iter(named_libs))
        else:
            found = exact(code, variants)
            cs = {inventory[i][0] for i in found}
            if len(cs) > 1:
                stats["other_collision"] += 1
            else:
                if cs and trusted(code, next(iter(cs))):
                    if len(found) == 1:
                        cid, vol, _shelf, ob = inventory[next(iter(found))]
                        target = volume_node(vol) if vol is not None else collection_node(cid)
                        sort = ob + ((1,), nat)
                    else:
                        target = collection_node(next(iter(cs)))
                    leaf.how = "exact"
                elif cs:
                    stats["exact_not_trusted"] += 1
                if target is None:
                    hits = by_head(code, variants)
                    if len(hits) == 1:
                        target, leaf.how = collection_node(next(iter(hits))), "collection"
                    elif len(hits) > 1:
                        stats["other_two_collections"] += 1
                    else:
                        named = prefix_collection(code, label)
                        if named is None:
                            named = named_collection(code, variants, label)
                            if named is not None:
                                stats["collection_by_name"] += 1
                        single = single_collection(code)
                        if named is not None:
                            target, leaf.how = collection_node(named), "collection"
                        elif single is not None:
                            target, leaf.how = collection_node(single), "single"
        if target is None and sid in RECORD_LIBRARY:
            # its library is named outright: the library's collection where it has one
            libs = tied.get(code, [])
            if len(libs) == 1 and len(held_collections[libs[0]]) == 1:
                target, leaf.how = collection_node(next(iter(held_collections[libs[0]]))), "single"
        if target is None and elsewhere is not None and len(held_collections[elsewhere]) == 1:
            # its long form names the library it is shown in (Kyiv's "Or." coded CUL; a Harkavy record
            # that is the NLI's "Ms. Heb. 1151.3=4"): that library's collection, written as it writes it
            target, leaf.how = collection_node(next(iter(held_collections[elsewhere]))), "single"
            leaf.label = institution_label(variants, foreign[0])
        if target is None and not leaf.held:
            # a library shown by its series: the records its "Other shelfmarks" would hold stand in
            # its collection, a collector's code or not
            libs = tied.get(code, [])
            series = series_collection.get(libs[0]) if libs else None
            if series is not None:
                target, leaf.how = collection_node(series), "single"
        if target is None:
            target = other_node(code, elsewhere)
        stats[leaf.how] += 1
        target.leaves.append((sort, leaf))

    for k, v in cohere(nodes, {sid: cv[1] for sid, cv in by_sys.items()}).items():
        stats[k] += v

    # -- groups: volumes of a large collection, the shelfmarks of "Other shelfmarks" ------------------
    order_volumes(nodes)
    stats["catalogue_placements"] = add_catalogues(nodes, catalogues, catalogue_entries, inventory, links,
                                                   by_sys, shown_as, parts)
    stats["told_apart"] = tell_apart(nodes, {sid: cv[1] for sid, cv in by_sys.items()})
    for node in nodes.values():
        # FGP's "alternate" and "duplicate" inventory rows repeat a shelfmark for the same record
        # (Budapest "10" three times): one place per node, manuscript and label
        seen, kept = set(), []
        for kl in sorted(node.leaves, key=lambda kl: (kl[0], kl[1].id)):
            mark = (kl[1].sys_id, kl[1].label)
            if mark in seen:
                stats["same_place_twice"] += 1
                if kl[1].how == CATALOGUE:
                    stats["catalogue_placements"] -= 1
                continue
            seen.add(mark)
            kept.append(kl)
        node.leaves = kept
    stats["marked_second"] = mark_same_labels(nodes)
    group_volumes(nodes, collections_by_node={f"col:{c}": c for c in collections})
    groups_in_place(nodes, collections_by_node={f"col:{c}": c for c in collections})
    for node in [n for n in nodes.values() if n.kind == "other"]:
        group_by_series(nodes, node)
    for nid in [f"col:{c}" for c in sorted(LIBRARY_SERIES)]:
        if nid in nodes and not nodes[nid].children:
            group_by_series(nodes, nodes[nid], minimum=0)
    for nid in [f"col:{c}" for c in sorted(BY_VOLUME_NUMBER)]:
        if nid in nodes:
            group_by_volume(nodes, nodes[nid])

    # -- ranges, order, positions ------------------------------------------------------------------
    for node in list(nodes.values()):
        node.leaves.sort(key=lambda kl: (kl[0], kl[1].id))
        # then in natural order, FGP's only between equals: "29.1" before "29.1b", "2" before "2bis";
        # a series by the numbers after it, as it may be written several ways ("Ms. FR 10", "Fr. 10/5")
        if node.by_number:
            node.leaves.sort(key=lambda kl: (roman_natural_key(split_form(kl[1].label)[1]),
                                             roman_natural_key(kl[1].label)))
        else:
            node.leaves.sort(key=lambda kl: roman_natural_key(kl[1].label))
        if node.kind == "volume":
            # the few shelfmarks a volume holds under another name come after its own ("Library Loan
            # Collection 1" in "T-S Loan", after "T-S Loan Collection 209")
            nt = _match_tokens(node.name_en)
            own = {id(kl): bool(name_matches(nt, _match_tokens(kl[1].label))) for kl in node.leaves}
            if sum(own.values()) * 2 >= len(own):
                node.leaves.sort(key=lambda kl: not own[id(kl)])
        if len(node.leaves) > RANGE_MAX:
            chunks = split_ranges(node)
            node.leaves = []
            for k, (name, chunk) in enumerate(chunks, 1):
                rid = f"rng:{node.id}:{k}"
                nodes[rid] = Node(rid, "range", node.id, name, order=((8,), k), leaves=chunk)
                node.children.append(rid)

    def manuscripts_of(nid):
        out = {leaf.sys_id for _k, leaf in nodes[nid].leaves}
        for child in nodes[nid].children:
            out |= manuscripts_of(child)
        return out

    sizes = {nid: len(manuscripts_of(nid)) for nid, n in nodes.items() if n.kind == "library"}
    for nid in [nid for nid in nodes if nid.startswith("lib:") and not nodes[nid].children and not nodes[nid].leaves]:
        del nodes[nid]   # a library merged into another before any of its own nodes was made
    top = sorted((n for n in nodes.values() if n.kind == "library" and sizes[n.id] >= TOP_LIBRARY_MIN),
                 key=lambda n: (-sizes[n.id], n.short.casefold()))
    rest = sorted((n for n in nodes.values() if n.kind == "library" and sizes[n.id] < TOP_LIBRARY_MIN),
                  key=lambda n: (n.name_en.casefold(), n.id))
    # The libraries shown inside another keep a shortcut in the list (owner, 2026-11-19).
    for lid, (target, _en, _he) in MERGE_INTO.items():
        cols = [f"col:{c}" for c, v in collections.items() if v[0] == lid and f"col:{c}" in nodes]
        if not cols or lid not in libraries:
            continue
        en_name, he_name = LIBRARY_NAMES.get(lid, (libraries[lid][1], None))
        link_to = cols[0] if len(cols) == 1 else f"lib:{target}"
        nodes[f"link:{lid}"] = Node(f"link:{lid}", "link", None, en_name, he_name or libraries[lid][2],
                                    libraries[lid][0], link=link_to)
    order = [n.id for n in top + rest]
    for node in nodes.values():
        node.children.sort(key=lambda c: (nodes[c].order, nodes[c].id))
    collection_aliases(nodes)
    stats["top_libraries"] = len(top)
    stats["manuscripts"] = len(by_sys)
    log(f"libraries first by size (>= {TOP_LIBRARY_MIN:,}): " +
        ", ".join(f"{n.short} {sizes[n.id]:,}" for n in top))
    if foreign_seen:
        log("records held back because a long form names another institution (code, head, records):")
        for (code, h), n in foreign_seen.most_common():
            log(f"  {code or '(none)'}\t{h}\t{n}")
    return nodes, order, stats, tied


def _volume_number(name: str, prefix: str):
    m = re.search(r"\d+", name[len(prefix):] if name.startswith(prefix) else name)
    return int(m.group(0)) if m else None


def _make_group(nodes, parent, gid, name, members, order, name_he=""):
    nodes[gid] = Node(gid, "group", parent.id, name, name_he, order=order)
    for m in members:
        nodes[m].parent = gid
    nodes[gid].children = list(members)
    return gid


def _number_groups(nodes, parent, members, prefix, base_id, fixed=()):
    """Divide volumes into ranges by number: FGP's own ranges where given, else round bounds."""
    numbered = [(n, _volume_number(nodes[n].name_en, prefix)) for n in members]
    out, left = [], []
    if fixed:
        buckets = {label: [] for label, _a, _b in fixed}
        for n, num in numbered:
            hit = next((label for label, a, b in fixed if num is not None and a <= num <= b), None)
            (buckets[hit] if hit else left).append(n)
        for k, (label, _a, _b) in enumerate(fixed):
            if buckets[label]:
                out.append((label, buckets[label], (k,)))
        return out, left

    def split(items, width_at):
        if len(items) <= VOLUME_RANGE_SIZE or width_at >= len(VOLUME_RANGE_WIDTHS):
            return [items]
        width = VOLUME_RANGE_WIDTHS[width_at]
        blocks: dict = {}
        for n, num in items:
            blocks.setdefault((num - 1) // width, []).append((n, num))
        if len(blocks) == 1:
            return split(items, width_at + 1)
        parts = []
        for key in sorted(blocks):
            parts += split(blocks[key], width_at + 1) if len(blocks[key]) > VOLUME_RANGE_SIZE else [blocks[key]]
        return parts

    with_num = sorted(((n, num) for n, num in numbered if num is not None), key=lambda x: x[1])
    left = [n for n, num in numbered if num is None]
    for k, part in enumerate(p for p in split(with_num, 0) if p):     # no numbered volumes: none
        lo, hi = part[0][1], part[-1][1]
        # written as its volumes are: "MS L33-489" over "MS L33" ..., "T-S AS 1-50" over "T-S AS 1" ...
        first = nodes[part[0][0]].name_en
        at = len(prefix) if first.startswith(prefix) else 0
        lead = first[:at + re.search(r"\d+", first[at:]).start()]
        out.append((f"{lead}{lo}-{hi}" if lo != hi else f"{lead}{lo}", [n for n, _ in part], (k,)))
    return out, left


def cohere(nodes: dict, variants_of: Optional[dict] = None) -> Counter:
    """Make the arrangement read as one system (plan §10.15), before the groups and ranges:

    1. a shelfmark without FGP's label is written the way FGP writes its collection, where the
       collection's FGP records show the rewrite ("Ms. Evr. Antonin B 1092" -> "Yevr. III B 1092",
       "Catalogue Lutzki L 45" -> "MS L45");
    2. a shelfmark that names another library by its siglum goes to that library ("LON BL Or.");
    3. volumes whose names differ only in case or punctuation are one ("OR", "Or"), and a volume
       named as its collection is the collection's own shelf;
    4. a shelfmark not in a volume (under "Other shelfmarks", beside a collection's volumes, in
       FGP's "Unspecified volume") goes to the one volume or collection of its library whose name
       leads it ("Moss. IV,366.2" -> Moss. IV, "Ms. Talm. II 15" -> Talmudica II);
    5. what is left of an "Unspecified volume" is divided by its form ("ENA L").
    Answers counts for the build's stats."""
    stats = Counter()

    def library_of(nid):
        while nodes[nid].parent:
            nid = nodes[nid].parent
        return nid

    def collection_of(nid):
        while nodes[nid].kind not in ("collection", "library"):
            nid = nodes[nid].parent
        return nid if nodes[nid].kind == "collection" else None

    def under(nid):
        out = [nid]
        for c in nodes[nid].children:
            out += under(c)
        return out

    def drop(nid):
        node = nodes.pop(nid)
        if node.parent and nid in nodes[node.parent].children:
            nodes[node.parent].children.remove(nid)

    received = set()
    moved = set()                                  # ids of entries placed by name, number or stem

    def move_all(src, dst):
        nodes[dst].leaves += nodes[src].leaves
        nodes[src].leaves = []
        received.add(dst)

    variants_of = variants_of or {}
    library_rules = defaultdict(set)               # (library, bare form) -> {(collection, its writing)}

    def respell(cid):
        """One spelling of each form in a collection, its usual one ("MS 1512", not "Ms. 1512" among
        them); both forms end as written ("KE." before "1"), one space where two met."""
        written = defaultdict(Counter)
        for nid in under(cid):
            for _k, leaf in nodes[nid].leaves:
                fa = split_form(leaf.label)[0]
                if fa.strip():
                    written[bare_name(fa)][fa] += 1
        spelling = {b: c.most_common(1)[0][0] for b, c in written.items() if len(c) > 1}
        for nid in under(cid):
            for _k, leaf in nodes[nid].leaves:
                fa, rest = split_form(leaf.label)
                want = spelling.get(bare_name(fa))
                if want is not None and fa != want and rest:
                    leaf.label = re.sub(r"  +", " ", want + rest)
                    stats["cohere_spelling"] += 1

    # 1. the collection's own way of writing a shelfmark: a form FGP's labels rewrite (learned from
    #    any of a record's call numbers), then one spelling of each form ("OR 10578R" / "Or. 10578R")
    for cid in [n for n, node in nodes.items() if node.kind == "collection"]:
        rules = defaultdict(Counter)
        raw_form = {}
        for nid in under(cid):
            for _k, leaf in nodes[nid].leaves:
                if leaf.how != "fist" or not leaf.shelf_label:
                    continue
                fb, rb = split_form(leaf.label)
                for v in {leaf.shelfmark, *variants_of.get(leaf.sys_id, ())}:
                    fa, ra = split_form(v)
                    if ra and bare_name(ra) == bare_name(rb) and bare_name(fa) != bare_name(fb):
                        rules[bare_name(fa)][bare_name(fb)] += 1
                        raw_form[bare_name(fb)] = fb
        chosen = {}
        for fa, targets in rules.items():
            fb, n = targets.most_common(1)[0]
            # never to nothing or to a bare "Ms." ("Ms. Guenzburg 920"), but for the collection that is
            # called so (JTS "MS": "The Jewish Theological Seminary of America Ms. 9160" is "MS 9160")
            if fb == "" or (fb == "ms" and bare_name(nodes[cid].name_en) != "ms"):
                continue
            if n >= MIN_CASES and n / sum(targets.values()) >= PURITY:
                chosen[fa] = raw_form[fb]
        for fa, fb in chosen.items():
            library_rules[(library_of(cid), fa)].add((cid, fb))
        for nid in under(cid):
            for _k, leaf in nodes[nid].leaves:
                fa, rest = split_form(leaf.label)
                if not leaf.shelf_label and rest and bare_name(fa) in chosen:
                    leaf.label = chosen[bare_name(fa)] + rest.lstrip()
                    stats["cohere_rewritten"] += 1
        respell(cid)

    # ... and in the library's "Other shelfmarks", where one collection's records show the rewrite
    # ("Catalogue Brumer Rab. 19" -> "MS R19", MS Rabbinica): written so, and that collection's
    rewritten_to = {}                              # id(entry) -> the collection whose rule wrote it
    for nid in [n for n, node in nodes.items() if node.kind == "other"]:
        for kl in nodes[nid].leaves:
            leaf = kl[1]
            fa, rest = split_form(leaf.label)
            if leaf.held or leaf.shelf_label or not rest:
                continue
            # The record's fullest call number of this number decides: a bare "Ms. 9160" is ENA's by
            # ENA's rule, but "The Jewish Theological Seminary of America Ms. 9160" is JTS MS 9160,
            # and "Adler, Elkan Nathan Ms. 88" is ENA 88. A form two collections rewrite settles nothing.
            forms = {bare_name(fa)} | {bare_name(f) for f, r in map(split_form, variants_of.get(leaf.sys_id, ()))
                                       if r and bare_name(r) == bare_name(rest)}
            rule = None
            for form in sorted(forms, key=lambda f: (-len(f), f)):
                rule = library_rules.get((library_of(nid), form))
                if not rule and library_of(nid).startswith("code:"):
                    # a library known only by its records' code ("Lutzki"): a rule of one collection
                    # anywhere (JTS MS Lutzki writes "Catalogue Lutzki L 588" as "MS L588")
                    rule = {r for (_lib, f), rs in library_rules.items() if f == form for r in rs}
                if rule:
                    break
            if not rule or len(rule) != 1:
                continue
            cid, fb = next(iter(rule))
            leaf.label = fb + rest.lstrip()
            rewritten_to[id(kl)] = cid
            stats["cohere_rewritten"] += 1

    # a volume is named as its shelfmarks are written ("OR 10578R" holding "Or. 10578R.1"): every volume
    for vid in [n for n, node in nodes.items() if node.kind == "volume"]:
        vol = nodes[vid]
        name = _usual_writing(vol)
        if name is not None and name != vol.name_en:
            vol.name_en = name
            stats["cohere_volume_renamed"] += 1

    # 2. a shelfmark that names another library
    pending = []                                   # (entry, from node, library node, tokens)
    for nid in [n for n, node in nodes.items() if node.kind == "other"]:
        for kl in list(nodes[nid].leaves):
            toks = _match_tokens(kl[1].label)
            lib = SIGLA.get(tuple(t for t, _ in toks[:2]))
            if lib is not None and not kl[1].held and f"lib:{lib}" in nodes and library_of(nid) != f"lib:{lib}":
                pending.append((kl, nid, f"lib:{lib}", toks[2:]))
                # the siglum said where; there the shelfmark is written without it ("ENA 2538")
                kl[1].label = re.sub(r"^\s*\S+\s+\S+\s+", "", kl[1].label, count=1) or kl[1].label

    # 3. twins and a volume named as its collection
    unspecified = {}                               # collection -> [entries out of "Unspecified volume"]
    for cid in [n for n, node in nodes.items() if node.kind == "collection"]:
        col = nodes[cid]
        twins = defaultdict(list)
        for v in [c for c in col.children if nodes[c].kind == "volume"]:
            twins[twin_key(nodes[v].name_en)].append(v)
        for vs in twins.values():
            keep = max(vs, key=lambda v: (len(nodes[v].leaves), v))
            name = bare_name(nodes[keep].name_en)
            for v in vs:
                if v != keep:
                    move_all(v, keep)
                    drop(v)
                    stats["cohere_twins"] += 1
            if name == bare_name(col.name_en):
                move_all(keep, cid)
                drop(keep)
                stats["cohere_volume_as_collection"] += 1
            elif name == UNSPECIFIED_VOLUME:
                unspecified[cid] = {id(kl) for kl in nodes[keep].leaves}
                move_all(keep, cid)
                drop(keep)

    # 4. into the volume or collection whose name leads the shelfmark
    targets = defaultdict(list)                    # library -> [(node, name tokens)]
    for nid, node in nodes.items():
        if node.kind in ("collection", "volume") and bare_name(node.name_en) != UNSPECIFIED_VOLUME:
            toks = _match_tokens(node.name_en)
            if toks:
                targets[library_of(nid)].append((nid, toks))

    def inside(nid, top):
        while nid is not None and nid != top:
            nid = nodes[nid].parent
        return nid == top

    def best(lib, toks, within=None, numbered=False):
        """The one node whose name leads the shelfmark. ``within``: a collection's own shelfmarks stay
        in it (a one-letter "L" of another collection is no match for "L-G Talm."); from "Other
        shelfmarks" a match needs a word or a number, not one letter ("T" for "T-S Boxes")."""
        scored = []
        for nid, nt in targets.get(lib, ()):
            if within is not None and not inside(nid, within):
                continue
            if numbered and not nt[0][0].isdigit():
                continue                           # only volumes named by a number
            s = name_matches(nt, toks)
            if s and within is None and not any(len(t) >= 2 or t.isdigit() for t, _ in nt):
                s = 0
            scored.append((s, nid))
        top = max((s for s, _ in scored), default=0)
        hits = [nid for s, nid in scored if s == top and s > 0]
        return hits[0] if len(hits) == 1 else None

    pending_ids = {id(p[0]) for p in pending}
    candidates = list(pending)
    for nid, node in list(nodes.items()):
        loose = node.kind == "other" or (
            node.kind == "collection" and any(nodes[c].kind == "volume" for c in node.children))
        if loose:
            candidates += [(kl, nid, library_of(rewritten_to.get(id(kl), nid)), _match_tokens(kl[1].label))
                           for kl in node.leaves if id(kl) not in pending_ids and not kl[1].held]
    gone = defaultdict(set)                        # node -> ids of entries moved out
    by_form = defaultdict(list)                    # library -> collections, by their bare name
    for nid, node in nodes.items():
        if node.kind == "collection":
            by_form[(library_of(nid), bare_name(node.name_en))].append(nid)
    for kl, src, lib, toks in candidates:
        within = src if nodes[src].kind == "collection" else None
        dst = best(lib, toks, within=within)
        if dst is None or dst == src:
            # the record's other call numbers (libraries.csv) may name its volume where its label does
            # not, or names only the collection it is in (FGP's "ENA 2b" is also "ENA 2557.1")
            by_alias = {}
            for v in variants_of.get(kl[1].sys_id, ()):
                hit = best(lib, _match_tokens(v), within=within)
                if hit is not None and hit != src:
                    by_alias.setdefault(hit, v)
            if len(by_alias) == 1:
                dst, alias = next(iter(by_alias.items()))
                if not kl[1].shelf_label:
                    kl[1].label = alias            # there it is called by that name ("Ms. 5440" in MS 5440)
                stats["cohere_by_alias"] += 1
        words = next((i for i, (t, _dot) in enumerate(toks) if t.isdigit()), 0)
        if (dst is None or dst == src) and within is not None and words:
            # a collection's volumes named by numbers alone: the one its number names, whatever word
            # stands before it (NLI "Ms. Heb. 577.2.26=4" is "577.2/26" of "577.2")
            hit = best(lib, toks[words:], within=within, numbered=True)
            if hit is not None and hit != src:
                dst = hit
                if not kl[1].shelf_label:
                    lead = words + name_matches(_match_tokens(nodes[hit].name_en), toks[words:])
                    written = _as_volume_writes(nodes[hit], kl[1].label, lead)
                    if not any("=" in leaf.label for _k, leaf in nodes[hit].leaves):
                        written = re.sub(r"=\d+$", "", written)       # the volume's shelfmarks carry no size mark
                    kl[1].label = written
                stats["cohere_by_number"] += 1
        if dst is None and nodes[src].kind == "other":
            # a bare "Ms. 88" in a library with a collection called "MS": that collection (owner, 2026-11-20)
            named = by_form.get((lib, bare_name(form_of(kl[1].label))), [])
            dst = named[0] if len(named) == 1 else rewritten_to.get(id(kl))
            if dst is None and not kl[1].shelf_label:
                # an alias in that form: JTS "Ms. II 573" is also "Ms. 10617", so JTS MS, as "MS 10617"
                by_name = {}
                for v in variants_of.get(kl[1].sys_id, ()):
                    hit = by_form.get((lib, bare_name(form_of(v))), [])
                    if len(hit) == 1 and split_form(v)[1]:
                        by_name.setdefault(hit[0], v)
                if len(by_name) == 1:
                    dst, kl[1].label = next(iter(by_name.items()))
                    stats["cohere_by_alias"] += 1
            if dst is not None and nodes[dst].kind == "collection":
                # placed by its form being the collection's name: written as the collection is ("MS 10617")
                fa, rest = split_form(kl[1].label)
                if rest and bare_name(fa) == bare_name(nodes[dst].name_en):
                    kl[1].label = f"{nodes[dst].name_en} {rest.lstrip()}"
        if dst is not None and id(kl) in rewritten_to and not inside(dst, rewritten_to[id(kl)]):
            dst = rewritten_to[id(kl)]             # its rule's collection, not a look-alike elsewhere
        if dst is None and lib != library_of(src):
            dst = _other_of(nodes, lib)            # another library's: its "Other shelfmarks"
        if dst is None or dst == src:
            continue
        a = src
        while a is not None and a != dst:
            a = nodes[a].parent
        if a == dst:                               # already in that collection
            continue
        gone[src].add(id(kl))
        if kl[1].how == "other" and nodes[dst].kind != "other":
            kl[1].how = "name"
            stats["other"] -= 1
            stats["name"] += 1
        nodes[dst].leaves.append(kl)
        moved.add(id(kl))
        received.add(dst)
        stats["cohere_placed"] += 1
    for src, ids in gone.items():
        nodes[src].leaves = [kl for kl in nodes[src].leaves if id(kl) not in ids]

    # 4b. a collection's shelfmarks beside its volumes: into the one volume of the same number
    #     ("Ms. Gaster 1785.1" -> "Gaster heb. ms 1785"), else together by their stem ("Box D.7",
    #     "Box D.8" -> "Box D"), each stem of two or more a volume of its own
    for cid in [n for n, node in nodes.items() if node.kind == "collection"]:
        col = nodes[cid]
        vols = [c for c in col.children if nodes[c].kind == "volume"]
        if not vols or not col.leaves:
            continue
        by_number = defaultdict(list)
        for v in vols:
            m = re.search(r"(\d+)\s?([A-Za-z]?)\W*$", nodes[v].name_en)
            if m:
                by_number[m.group(1) + m.group(2).lower()].append(v)

        def words(text):
            return {w.lower() for w in re.findall(r"[A-Za-z]{2,}", text)} - {"ms"}

        keep, stems = [], defaultdict(list)
        for kl in col.leaves:
            if kl[1].how == "fist" and kl[1].shelf_label and not stem_of(kl[1].label):
                keep.append(kl)
                continue
            first = _number_and_letter(kl[1].label)
            hit = (by_number.get(first[0] + first[1]) or by_number.get(first[0])) if first else None
            if (hit and len(hit) == 1 and kl[1].how != "fist"
                    and words(form_of(kl[1].label)) & words(nodes[hit[0]].name_en)):
                nodes[hit[0]].leaves.append(kl)
                moved.add(id(kl))
                received.add(hit[0])
                stats["cohere_by_number"] += 1
                continue
            stem = stem_of(kl[1].label)
            (stems[stem].append(kl) if stem else keep.append(kl))
        for stem, kls in stems.items():
            if len(kls) < 2:
                keep += kls
                continue
            vid = f"vol:{cid}:stem:{bare_name(stem)}"
            if vid not in nodes:
                nodes[vid] = Node(vid, "volume", cid, stem, order=(roman_natural_key(stem),))
                col.children.append(vid)
            nodes[vid].leaves += kls
            moved.update(id(kl) for kl in kls)
            received.add(vid)
            stats["cohere_by_stem"] += len(kls)
        col.leaves = keep

    # 5. what is left of an "Unspecified volume": by form
    for cid, ids in unspecified.items():
        left = [kl for kl in nodes[cid].leaves if id(kl) in ids]
        if not left:
            continue
        nodes[cid].leaves = [kl for kl in nodes[cid].leaves if id(kl) not in ids]
        by_form = defaultdict(list)
        for kl in left:
            by_form[form_of(kl[1].label)].append(kl)
        for form, kls in by_form.items():
            name = form if form and len(kls) >= 2 else "Unspecified volume"
            vid = f"vol:{cid}:form:{bare_name(name)}"
            if vid not in nodes:
                nodes[vid] = Node(vid, "volume", cid, name, order=(roman_natural_key(name),))
                nodes[cid].children.append(vid)
            nodes[vid].leaves += kls
            moved.update(id(kl) for kl in kls)
            received.add(vid)
            if name != "Unspecified volume":
                stats["cohere_unspecified_by_form"] += len(kls)

    # 4c. a volume named as another with the collection's own word before it is that volume ("Box F",
    #     made of our "Box F.13", and FGP's "F" in "RNL Boxes"); then a shelfmark in a volume that
    #     another volume of the collection names goes there ("D 37" in FGP's "F" -> "Box D", as
    #     "Box D.37"): not one its own volume's name leads, ends, or goes on from
    for cid in [n for n, node in nodes.items() if node.kind == "collection"]:
        own_words = _own_words(nodes[cid].name_en)
        by_bare = {twin_key(nodes[c].name_en): c for c in nodes[cid].children if nodes[c].kind == "volume"}
        for v in list(by_bare.values()):
            short = _without_own_word(nodes[v].name_en, own_words)
            twin = by_bare.get(twin_key(short)) if short else None
            if twin is not None and twin != v and twin in nodes:
                moved.update(id(kl) for kl in nodes[twin].leaves)
                move_all(twin, v)
                drop(twin)
                stats["cohere_twins"] += 1
        vols = [c for c in nodes[cid].children if nodes[c].kind == "volume"]
        if len(vols) < 2:
            continue
        named = {}                                 # volume -> the token lists its name is found by
        for v in vols:
            short = _without_own_word(nodes[v].name_en, own_words)
            named[v] = [t for t in (_match_tokens(nodes[v].name_en), _match_tokens(short or "")) if t]
        for v in vols:
            mine = _match_tokens(nodes[v].name_en)
            stay = []
            for kl in nodes[v].leaves:
                toks = _match_tokens(kl[1].label)
                if (not toks or not mine or mine[:len(toks)] == toks
                        or any(name_matches(mine[i:], toks) for i in range(len(mine)))):
                    stay.append(kl)
                    continue
                hits = [w for w in vols if w != v and any(name_matches(t, toks) for t in named[w])]
                if len(hits) != 1:
                    stay.append(kl)
                    continue
                lead = max(name_matches(t, toks) for t in named[hits[0]])
                kl[1].label = _as_volume_writes(nodes[hits[0]], kl[1].label, lead)
                nodes[hits[0]].leaves.append(kl)
                moved.add(id(kl))
                received.add(hits[0])
                stats["cohere_to_named_volume"] += 1
            nodes[v].leaves = stay
    # 5b. a collection whose volumes mostly hold one shelfmark shows those shelfmarks themselves
    #     (CUL Add.: 66 of 73; owner, 2026-11-20)
    for cid in [n for n, node in nodes.items() if node.kind == "collection"]:
        vols = [c for c in nodes[cid].children if nodes[c].kind == "volume" and not nodes[c].children]
        single = [v for v in vols if len(nodes[v].leaves) == 1]
        if len(vols) >= 5 and len(single) * 2 >= len(vols):
            for v in single:
                moved.add(id(nodes[v].leaves[0]))
                move_all(v, cid)
                drop(v)
            stats["cohere_flattened"] += len(single)

    for cid in {collection_of(nid) for nid in received if nid in nodes} - {None}:
        respell(cid)                               # what came in is written as its collection writes

    # 6. a volume's shelfmarks are written as the volume is ("Ms. Gaster 1785.1" in "Gaster heb. ms
    #    1785" -> "Gaster heb. ms 1785.1"), and a volume's name has no stray "/" at its end
    #    ("MS heb. e.39/" -> "MS heb. e.39")
    for vid in [n for n, node in nodes.items() if node.kind == "volume"]:
        vol = nodes[vid]
        nt = _match_tokens(vol.name_en)
        for _k, leaf in vol.leaves:
            if not name_matches(nt, _match_tokens(leaf.label)):
                cut = _bare_cut(leaf.label, bare_name(vol.name_en))
                if cut is None:
                    # the volume's number (and letter), written another way, with a word in common:
                    # "Ms. Gaster 1785.1", "Ms. Gaster 2110 A.1" in "Gaster heb. ms 2110a"; what follows
                    # is set off as the volume's own shelfmarks set it off ("/1")
                    num = re.search(r"(\d+)\s?([A-Za-z]?)\W*$", vol.name_en)
                    first = _number_and_letter(leaf.label)
                    shared = ({w.lower() for w in re.findall(r"[A-Za-z]{2,}", form_of(leaf.label))}
                              & {w.lower() for w in re.findall(r"[A-Za-z]{2,}", vol.name_en)}) - {"ms"}
                    if num and first and shared and first[0] == num.group(1) and (
                            first[1] == num.group(2).lower() or not num.group(2)):
                        rest = leaf.label[first[3] if num.group(2) else first[2]:]
                        sep = _volume_separator(vol)
                        if sep is not None and rest[:1] in tuple(" .,/"):
                            rest = sep + rest.lstrip(" .,/")
                        leaf.label = vol.name_en.rstrip("/") + rest
                        stats["cohere_written_as_volume"] += 1
                        continue
                if cut is None:
                    # the volume's name without its collection's word ("F 1" in "Box F" of "RNL Boxes")
                    short = _match_tokens(_without_own_word(vol.name_en, _own_words(
                        nodes[collection_of(vid)].name_en if collection_of(vid) else "")) or "")
                    lead = name_matches(short, _match_tokens(leaf.label)) if short else 0
                    if lead:
                        leaf.label = _as_volume_writes(vol, leaf.label, lead)
                        stats["cohere_written_as_volume"] += 1
                if cut is not None:
                    leaf.label = vol.name_en.rstrip("/") + leaf.label[cut:]
                    stats["cohere_written_as_volume"] += 1
        if vol.name_en.endswith("/") and len(vol.name_en) > 1:
            vol.name_en = vol.name_en.rstrip("/")
        # named again as most of its (now respelled) shelfmarks write it: "ADD.3124" holding "Add.3124.1"
        name = _usual_writing(vol)
        if name is not None and name.rstrip("/") != vol.name_en:
            vol.name_en = name.rstrip("/")
        # ... and each of them writes it so ("T-S 18k1.3" in "T-S 18K1" -> "T-S 18K1.3")
        core = vol.name_en.rstrip("()[]/.,;: ")
        for _k, leaf in vol.leaves:
            cut = _bare_cut(leaf.label, bare_name(vol.name_en))
            if cut is not None and core and leaf.label[:cut] != core and bare_name(leaf.label[:cut]) == bare_name(core):
                leaf.label = core + leaf.label[cut:]
                stats["cohere_written_as_volume"] += 1

    # 6b. FGP's slips in a volume's number
    for cid in [n for n, node in nodes.items() if node.kind == "collection"]:
        vols = [c for c in nodes[cid].children if nodes[c].kind == "volume"]
        names = {twin_key(nodes[v].name_en) for v in vols}
        for vid in vols:
            vol = nodes[vid]
            # "ENA II 177" holds "ENA II 117.1" ... "117.6": the number every one of its shelfmarks
            # carries and FGP's own order column gives, where no other volume has it
            m = re.search(r"(\d+)$", vol.name_en)
            if m and len(vol.leaves) >= 3:
                lead = vol.name_en[:m.start()]
                seen = set()
                for kl in vol.leaves:
                    lm = re.match(re.escape(lead) + r"(\d+)(?!\d)", kl[1].label)
                    seen.add((lm.group(1), kl[0][3] if len(kl[0]) > 3 else None)
                             if lm and kl[1].how == "fist" else None)
                number, column = next(iter(seen)) if len(seen) == 1 and None not in seen else (None, None)
                if (number is not None and number != m.group(1) and column == _ob_part(float(number))
                        and twin_key(lead + number) not in names):
                    names.discard(twin_key(vol.name_en))
                    vol.name_en = lead + number
                    names.add(twin_key(vol.name_en))
                    stats["cohere_volume_number"] += 1
            # "ENA NS I" among "ENA NS 2" ... "ENA NS 85": written "ENA NS 1", as most of its
            # shelfmarks' own call numbers write it ("Ms. ENA NS 1.5"), where no volume is called so
            last = list(re.finditer(r"[A-Za-z]+|\d+", vol.name_en))[-1:]
            numeral = _roman(last[0].group()) if last and vol.name_en.endswith(last[0].group()) else None
            if numeral is None or numeral[1] or not vol.leaves:
                continue
            lead = vol.name_en[:last[0].start()]
            arabic = lead + str(numeral[0])
            beside = [nodes[v].name_en[len(lead):] for v in vols if v != vid and nodes[v].name_en.startswith(lead)]
            if (len(beside) < MIN_CASES or not all(re.fullmatch(r"\d+[a-z]?", b) for b in beside)
                    or twin_key(arabic) in names):
                continue

            def as_arabic(label, old=vol.name_en, new=arabic):
                return new + label[len(old):] if label.startswith(old) else None

            def written(text):
                return [t for t, _dot in _match_tokens(text)]

            backed = sum(1 for _k, leaf in vol.leaves if as_arabic(leaf.label) is not None and any(
                written(v) == written(as_arabic(leaf.label)) for v in variants_of.get(leaf.sys_id, ())))
            if backed * 2 > len(vol.leaves):
                for _k, leaf in vol.leaves:
                    leaf.label = as_arabic(leaf.label) or leaf.label
                names.discard(twin_key(vol.name_en))
                vol.name_en = arabic
                names.add(twin_key(arabic))
                stats["cohere_roman_to_arabic"] += 1

    # 6c. a record's shelfmark that is an FGP shelfmark of its node written another way is written
    #     FGP's way ("X893.1 BC T 17" beside "X893.1 BC T17", "Ms. R II 5" beside "R II 5", "vol. 21"
    #     beside "v.21"): the two records of one shelfmark then stand together
    def one_shelfmark(a, b):
        if len(a) != len(b):
            return False
        for (x, x_dot), (y, y_dot) in zip(a, b):
            if x != y and (x.isdigit() or y.isdigit()
                           or not ((x_dot and y.startswith(x)) or (y_dot and x.startswith(y)))):
                return False
        return True

    def shape(toks):
        return tuple(t if t.isdigit() else t[:1] for t, _dot in toks)

    for node in nodes.values():
        ours = [leaf for _k, leaf in node.leaves if not leaf.shelf_label and not leaf.held]
        if not ours or len(ours) == len(node.leaves):
            continue
        fgp = defaultdict(list)
        for _k, leaf in node.leaves:
            if leaf.shelf_label:
                toks = _match_tokens(leaf.label)
                fgp[shape(toks)].append((toks, leaf.label))
        for leaf in ours:
            toks = _match_tokens(leaf.label)
            same = {label for ftoks, label in fgp.get(shape(toks), ()) if one_shelfmark(toks, ftoks)}
            if len(same) == 1 and leaf.label not in same:
                leaf.label = next(iter(same))
                stats["cohere_fgp_writing"] += 1

    # 7. each shelfmark keeps its record's other call numbers, and FGP's own writing where ours
    #    differs ("ENA NS I.5"), for a reader who knows one of them
    for node in nodes.values():
        for _k, leaf in node.leaves:
            known = dict.fromkeys([leaf.shelf_label, *variants_of.get(leaf.sys_id, ())])
            leaf.aliases = " | ".join(v for v in known if v and v != leaf.label)
    for nid in received:                           # FGP's order kept; what came in slotted by its name
        if nid in nodes:
            node = nodes[nid]
            out = sorted((kl for kl in node.leaves if id(kl) not in moved), key=lambda kl: (kl[0], kl[1].id))
            for kl in sorted((kl for kl in node.leaves if id(kl) in moved),
                             key=lambda kl: (roman_natural_key(kl[1].label), kl[1].id)):
                k = roman_natural_key(kl[1].label)
                i = len(out)
                while i > 0 and roman_natural_key(out[i - 1][1].label) > k:
                    i -= 1
                out.insert(i, kl)
            node.leaves = [(((0, i),), leaf) for i, (_k, leaf) in enumerate(out)]
    for kinds in (("other", "volume", "collection"), ("library",)):   # a library emptied by the moves too
        for nid in [n for n, node in nodes.items() if node.kind in kinds and not node.leaves and not node.children]:
            drop(nid)
    return stats


def _written_as(name: str, label: str) -> Optional[str]:
    """How a shelfmark writes the volume name it begins with: "Or. 10578R" for "OR 10578R" in
    "Or. 10578R.1"; None when it does not begin with it."""
    want = bare_name(name)
    if not want:
        return None
    got = ""
    for i, ch in enumerate(label):
        if ch.isalnum():
            got += ch.lower()
            if got == want:
                nxt = label[i + 1:i + 2]
                # the name ends here, not inside a longer number or word ("OR 1057" in "Or. 10578")
                if nxt and nxt.isalnum() and (nxt.isdigit() == ch.isdigit()):
                    return None
                # the name's own ending is kept: "T-S F2(1)" stays closed, "MS heb. e.39/" keeps its slash
                tail = name[len(name.rstrip("()[]/.,;: ")):].rstrip()
                return label[:i + 1] + tail
            if not want.startswith(got):
                return None
    return None


def _bare_cut(label: str, want: str) -> Optional[int]:
    """Where in ``label`` its letters and digits have spelled out ``want`` (a volume's bare name),
    when they do so whole: "T-S F11.1" spells "tsf11" up to ".1". None otherwise, or when the
    next character would go on with the same number or word ("T-S F110.1" is not "T-S F11")."""
    if not want:
        return None
    got = ""
    for i, ch in enumerate(label):
        if ch.isalnum():
            got += ch.lower()
            if got == want:
                nxt = label[i + 1:i + 2]
                if nxt and nxt.isalnum() and nxt.isdigit() == ch.isdigit():
                    return None
                return i + 1
            if not want.startswith(got):
                return None
    return None


def _usual_writing(vol) -> Optional[str]:
    """How more than half of a volume's shelfmarks that begin with its name write that name; None
    when no writing has a majority (FGP's own name stays: "T-S 18K1", two shelfmarks "18K1", two "18k1")."""
    seen = Counter(_written_as(vol.name_en, leaf.label) for _k, leaf in vol.leaves)
    seen.pop(None, None)
    if not seen:
        return None
    name, n = seen.most_common(1)[0]

    def numerals(text):
        return sum(_roman(t) is not None for t in re.findall(r"[A-Za-z]+", text))

    # never away from a Roman numeral: FGP's "Moss. VIa" holds "Moss. Via,1", and stays "VIa"
    return name if n * 2 > sum(seen.values()) and numerals(name) >= numerals(vol.name_en) else None


def _singular(word: str) -> str:
    if word.endswith("es") and len(word) > 3:
        return word[:-2]
    return word[:-1] if word.endswith("s") else word


def _own_words(collection_name: str) -> frozenset:
    """The words of a collection's name, singular: "RNL Boxes" -> rnl, box."""
    return frozenset(_singular(w) for w in re.findall(r"[a-z]{3,}", (collection_name or "").lower()))


def _without_own_word(name: str, own_words) -> Optional[str]:
    """A volume's name without a first word that is its collection's own ("Box F" -> "F" in "RNL
    Boxes"); None when it does not begin with one or is nothing more."""
    m = re.match(r"\s*([A-Za-z]{3,})\.?\s+(\S.*)$", name or "")
    if m and _singular(m.group(1).lower()) in own_words:
        return m.group(2)
    return None


def _number_and_letter(label: str):
    """(a shelfmark's first number, the one letter after it or "", where the number ends, where the
    letter ends): "Ms. Gaster 2110 A.1" -> ("2110", "a", ...). None without a number."""
    m = re.search(r"(\d+)(?:\s?([A-Za-z])(?![A-Za-z]))?", label or "")
    return (m.group(1), (m.group(2) or "").lower(), m.end(1), m.end()) if m else None


def _volume_separator(vol) -> Optional[str]:
    """What a volume's own shelfmarks put after its name ("/" in "Gaster heb. ms 2110a/1"); None
    when none of them begins with the name."""
    seps = Counter()
    for _k, leaf in vol.leaves:
        cut = _bare_cut(leaf.label, bare_name(vol.name_en))
        if cut is not None:
            m = re.match(r"[^A-Za-z0-9]*", leaf.label[cut:])
            seps[m.group()] += 1
    return seps.most_common(1)[0][0] if seps else None


def _as_volume_writes(vol, label: str, lead: int) -> str:
    """``label`` written as the volume's own shelfmarks are: the volume's name, what they put after
    it, then what follows the first ``lead`` words or numbers of ``label`` ("D 37" -> "Box D.37")."""
    sep = _volume_separator(vol)
    sep = " " if sep is None else sep
    found = list(re.finditer(r"[A-Za-z]+\.?|\d+", label))
    if found and re.match(r"ms\.?$", found[0].group(), re.IGNORECASE):
        found = found[1:]                          # a leading "Ms." is not compared (``_match_tokens``)
    if len(found) <= lead:
        return label
    return vol.name_en.rstrip("/") + sep + label[found[lead].start():]


def after_colon(text: str) -> str:
    """A call number written "<collection>: <shelfmark>" is the shelfmark: "T-S Boxes: T-S K25.240",
    "Oriental: Or.1080.46", where what follows the colon begins as what precedes it does, or
    shortens it. "2nd Series: A 12" is whole."""
    if ":" not in (text or ""):
        return text
    before, after = (part.strip() for part in text.split(":", 1))
    a, b = _TOKEN.findall(before.lower()), _TOKEN.findall(after.lower())
    if a and b and re.search(r"\d", after) and (a[0] == b[0] or (len(b[0]) >= 2 and a[0].startswith(b[0]))):
        return after
    return text


def order_volumes(nodes: dict) -> None:
    """A collection's volumes in FGP's own order (Mosseri's "Moss." volumes, then its letters, then
    its "2nd Series"; "T-S Loan" after "T-S Misc.36"), with the numbers and Roman numerals of one
    series in their own order where FGP reads them as letters ("Moss. IX" between "I" and "Ia"). A
    volume FGP does not have stands where its name puts it."""
    for cid in [n for n, node in nodes.items() if node.kind == "collection"]:
        vols = [c for c in nodes[cid].children if nodes[c].kind == "volume"]
        if not vols:
            continue
        keys = sibling_order_keys([nodes[v].name_en for v in vols])
        key = {v: keys[nodes[v].name_en] for v in vols}
        placed = sorted((v for v in vols if nodes[v].fgp), key=lambda v: (nodes[v].fgp, key[v], v))

        def series_of(v):                          # a name without its last number; None: it has none
            last = max((j for j, part in enumerate(key[v]) if part[0] == 0), default=None)
            return None if last is None else key[v][:last] + key[v][last + 1:]

        series = defaultdict(list)                 # a series -> where its volumes stand
        for i, v in enumerate(placed):
            if series_of(v) is not None:
                series[series_of(v)].append(i)
        for slots in series.values():
            for i, v in zip(slots, sorted((placed[i] for i in slots), key=lambda v: (key[v], v))):
                placed[i] = v
        for v in sorted((v for v in vols if not nodes[v].fgp), key=lambda v: (key[v], v)):
            mates = [i for i, other in enumerate(placed)
                     if series_of(v) is not None and series_of(other) == series_of(v)]
            if mates:                              # among its own series, by its number
                below = [i for i in mates if key[placed[i]] <= key[v]]
                at = below[-1] + 1 if below else mates[0]
            else:                                  # before the first volume its name comes before
                at = next((i for i, other in enumerate(placed) if key[other] > key[v]), len(placed))
            placed.insert(at, v)
        for rank, v in enumerate(placed):
            nodes[v].order = ((0, rank),)


def add_catalogues(nodes: dict, catalogues: dict, entries: list, inventory: dict, links: dict,
                   by_sys: dict, shown_as: dict, parts: dict) -> int:
    """FGP's second way into a collection: by the entries of a catalogue (Oxford "By Neubauer -
    Cowley catalog": 2603.1 ... 2918). A list beside the collection's volumes, each entry holding
    the manuscripts FGP ties to it: a second place for them, in the catalogue's order. Answers how
    many places were added."""
    of_inventory = defaultdict(set)
    for sid, iids in links.items():
        if sid in by_sys:
            for iid in iids:
                of_inventory[iid].add(sid)
    by_list = defaultdict(lambda: defaultdict(dict))       # (catalogue, collection) -> entry -> leaf id -> (order, leaf)
    first = {}                                             # (catalogue, collection, entry) -> FGP's order of it
    for cat, iid, entry, order in entries:
        cid, _vol, shelf, _ob = inventory[iid]
        if f"col:{cid}" not in nodes or not shelf:
            continue
        entry = _text(entry)
        for sid in sorted(of_inventory.get(iid, ())):
            # one place per entry, inventory row and manuscript (a row may stand under two entries)
            # the entry itself tells it: two written alike but for case or punctuation stay two
            mark = hashlib.blake2s(entry.encode("utf-8"), digest_size=4).hexdigest()
            lid = f"cat:{cat}:{bare_name(entry)}:{mark}:{iid}:{sid}"
            leaf = Leaf(lid, sid, shown_as[sid], shelf, iid, CATALOGUE, parts[sid], shelf)
            # found by the record's other call numbers here too, as its place on the shelf is
            leaf.aliases = " | ".join(v for v in dict.fromkeys(by_sys[sid][1]) if v and v != shelf)
            by_list[(cat, cid)][entry].setdefault(lid, ((_ob_part(order), tuple(natural_sort_key(shelf))), leaf))
            k = (cat, cid, entry)
            first[k] = min(first.get(k, _ob_part(order)), _ob_part(order))
    added = 0
    for (cat, cid), by_entry in sorted(by_list.items()):
        parent = nodes[f"col:{cid}"]
        en, he = CATALOGUE_NAMES.get(cat, (f'By "{catalogues[cat]}" catalogue', ""))
        gid = f"cat:{cat}:{cid}"
        nodes[gid] = Node(gid, "group", parent.id, en, he, order=((7,), cat))
        parent.children.append(gid)
        members = []
        # in the catalogue's own order (its OrderBy: Oxford "2723.A" ... "2723.I", where natural order
        # would read the "I" as a Roman one); entries FGP gives no order follow in natural order
        for k, entry in enumerate(sorted(by_entry, key=lambda e: (first[(cat, cid, e)], roman_natural_key(e))), 1):
            vid = f"{gid}:{k}"
            nodes[vid] = Node(vid, "volume", gid, entry, order=((0, k),), leaves=list(by_entry[entry].values()))
            members.append(vid)
            added += len(by_entry[entry])
        # ranges by number keep the catalogue's order where its numbers rise with it (Oxford's always
        # do; equal numbers keep their order) and an entry without a number comes only after them, as
        # the ranges put it; otherwise the list is divided as it stands ("1, Appendix A, 101")
        seq = [_volume_number(nodes[m].name_en, "") for m in members]
        nums = [x for x in seq if x is not None]
        first_bare = next((i for i, x in enumerate(seq) if x is None), len(seq))
        if all(a <= b for a, b in zip(nums, nums[1:])) and all(x is None for x in seq[first_bare:]):
            ranges, left = _number_groups(nodes, nodes[gid], members, "", gid)
        else:
            parts = [members[i:i + VOLUME_RANGE_SIZE] for i in range(0, len(members), VOLUME_RANGE_SIZE)]
            ranges = [(_span(nodes[p[0]].name_en, nodes[p[-1]].name_en), p, (k,)) for k, p in enumerate(parts)]
            left = []
        if len(ranges) > 1:
            for k, (label, part, order) in enumerate(ranges, 1):
                nodes[gid].children.append(_make_group(nodes, nodes[gid], f"{gid}:g{k}", label, part, ((5,), order)))
            for m in left:
                # an entry without a number, after the ranges as the catalogue has it ("Appendix A")
                nodes[m].order = ((6,),) + nodes[m].order
            nodes[gid].children += left
        else:
            nodes[gid].children = members
    return added


def tell_apart(nodes: dict, variants_of: dict) -> int:
    """Shelfmarks that would show one label in a node each show what their own call number adds to
    it: "OR 10578R" (107 of them) -> "OR 10578R f.1" ..., "T-S K7.15" -> "T-S K7.15n", "Yevr.-Arab.
    II 974" -> "Yevr.-Arab. II 974.1". The part is taken from a call number of the record that
    spells the label out, else from the one part all its call numbers of that number agree on. A
    label alone in its node is never touched. Answers how many were told apart."""
    count = 0
    for node in nodes.values():
        groups = defaultdict(list)
        for kl in node.leaves:
            groups[kl[1].label].append(kl)
        for label, kls in groups.items():
            if len(kls) < 2 or not label:
                continue
            want, rest = bare_name(label), bare_name(split_form(label)[1])
            for _k, leaf in kls:
                spelled, numbered = Counter(), Counter()
                for v in dict.fromkeys([leaf.shelfmark, *variants_of.get(leaf.sys_id, ())]):
                    if not v or _FILE_NAME.search(v):
                        continue
                    for m in re.finditer(r"[A-Za-z0-9]+", v):
                        cut = _bare_cut(v[m.start():], want)
                        if cut is not None:
                            spelled[_tail(v[m.start() + cut:])] += 1
                            break
                    v_rest = split_form(v)[1]
                    cut = _bare_cut(v_rest, rest) if rest else None
                    if cut is not None:
                        numbered[_tail(v_rest[cut:])] += 1
                spelled.pop("", None)
                numbered.pop("", None)             # a call number that is the label says nothing more
                tail = min(spelled, key=lambda t: (-spelled[t], len(t), t)) if spelled else ""
                if not tail and len(numbered) == 1:
                    tail = next(iter(numbered))
                if tail:
                    leaf.label = label + tail
                    count += 1
    return count


def mark_same_labels(nodes: dict) -> int:
    """Two records under one shelfmark in a node (the library catalogued it twice, or FGP has two
    rows for it) stay two rows, the second marked: "MS L45", "MS L45 [2]" (owner, 2026-11-20). The
    record FGP links comes first. Square brackets: round ones are part of shelfmarks ("T-S F1(2)").
    Answers how many were marked."""
    count = 0
    for node in nodes.values():
        groups = defaultdict(list)
        for kl in node.leaves:
            groups[kl[1].label].append(kl)
        for label, kls in groups.items():
            if len(kls) < 2 or not label:
                continue
            kls.sort(key=lambda kl: (kl[1].how not in ("fist", CATALOGUE), kl[1].id))
            for n, kl in enumerate(kls[1:], 2):
                kl[1].label = f"{label} [{n}]"
                count += 1
    return count


def _tail(text: str) -> str:
    """What a call number goes on with after a shelfmark, as a suffix to show (" f. 14", ".1", "n");
    "" when it is nothing or too long to be a part or a folio."""
    text = re.sub(r"\s+", " ", text or "").rstrip(" .,;:-_/")
    # a folio is written one way ("F.10", "ff 1-2", "FF. 35- 36" -> " f. 10", " f. 1-2", " f. 35-36")
    text = re.sub(r"^ ?f{1,2}\.? ?(?=\d)", " f. ", text, flags=re.IGNORECASE)
    text = re.sub(r"(?<=\d) ?- ?(?=\d)", "-", text)
    if not re.search(r"[A-Za-z0-9]", text) or len(text) > 16:
        return ""
    if text[0].isalnum() or text[0] in " .,/(_-":
        return text.replace("_", ".") if text[0] == "_" else text
    return ""


def _span(first: str, last: str) -> str:
    """A range's name by its first and last shelfmark, the last without what both begin with up to
    a separator: "ENA NS 77.489.1 – 807.1", never cut inside a number or a word."""
    if first == last:
        return first
    shared = os.path.commonprefix([first, last])
    at = max((shared.rfind(ch) for ch in " .,/"), default=-1) + 1
    return f"{first} – {last[at:]}" if at and last[at:] else f"{first} – {last}"


def item_number(label: str, holder: str):
    """(the number a shelfmark has in its node, where it begins): the first after the node's own
    name where the shelfmark spells that name ("T-S NS 329.17" in "T-S NS 329": 17), else its
    first number ("B 1922" in "B series"). None without one."""
    cut = _bare_cut(label, bare_name(holder)) if holder else None
    m = re.search(r"\d+", label[cut:] if cut is not None else label)
    return (int(m.group()), (cut or 0) + m.start()) if m else None


def split_ranges(node) -> list:
    """Divide a node's many shelfmarks into ranges, [(name, entries)]: by round item numbers, as
    FGP's "Moss. IV,1 - 100" ... "Moss. IV,301 - 400", the width that brings a range nearest to
    ``RANGE_TARGET`` shelfmarks; a range still above ``RANGE_SIZE`` is divided again, narrower.
    Where the shelfmarks' numbers do not rise with their order, the node mixes several forms (one
    of them of ``FAMILY_MIN`` shelfmarks or more): each form is ranged by itself (Budapest: "1 -
    200" ..., the Roman numerals, "M1 – M93", "H1 – H7"), the stray ones together at the end.
    Without forms to tell apart: by count, each range named by its first and last shelfmark."""
    return _ranges_of(node.leaves, node.name_en, families=True)


def _family(label: str) -> str:
    """The form a shelfmark is written in, to tell the families of a mixed node: what precedes its
    first number ("M" of "M 12"); a bare Roman numeral ("I", "XXXVII") is one family."""
    return "\x00" if _roman((label or "").strip()) is not None else form_of(label)


def _ranges_of(leaves: list, holder: str, families: bool) -> list:

    def by_count(entries):
        out = []
        for i in range(0, len(entries), RANGE_SIZE):
            chunk = entries[i:i + RANGE_SIZE]
            first, last = chunk[0][1], chunk[-1][1]
            out.append((_span(first.label or first.shelfmark, last.label or last.shelfmark), chunk))
        return out

    def by_family():
        of = defaultdict(list)
        for kl in leaves:
            of[_family(kl[1].label)].append(kl)
        if len(of) < 2 or not any(len(kls) >= FAMILY_MIN for kls in of.values()):
            return by_count(leaves)
        # every form of three shelfmarks or more is a range a reader can tell by its name ("H1 – H7");
        # the stray ones go together at the end
        own = [f for f, kls in of.items() if len(kls) >= 3]
        # a shelfmark written without its series' letter ("Yevr. III 1155" beside "Yevr. III A 1 - 13"
        # and "Yevr. III B 1 - 1154", owner 2026-11-20) stands in the series of its form its number
        # lies in or next to: the nearest, when nearer than that series is long; in its place by number
        moved = set()
        for f in [f for f in of if f not in own and f != "\x00"]:
            for kl in of[f]:
                n = item_number(kl[1].label, holder)
                if n is None:
                    continue
                gaps = {}
                for g in own:
                    nums = [m[0] for m in (item_number(x[1].label, holder) for x in of[g]) if m]
                    if g.startswith(f + " ") and nums:
                        gap = max(min(nums) - n[0], n[0] - max(nums), 0)
                        if gap <= len(nums):
                            gaps[g] = gap
                nearest = [g for g, gap in gaps.items() if gap == min(gaps.values())]
                if len(nearest) == 1:
                    series = of[nearest[0]]
                    at = next((i for i, x in enumerate(series)
                               if (item_number(x[1].label, holder) or (0,))[0] > n[0]), len(series))
                    series.insert(at, kl)
                    moved.add(id(kl))
        out = []
        for f in sorted(own, key=lambda f: (roman_natural_key(of[f][0][1].label), f)):
            if len(of[f]) > RANGE_SIZE:
                out += _ranges_of(of[f], holder, families=False)
            else:
                out.append((_span(of[f][0][1].label, of[f][-1][1].label), of[f]))
        rest = [kl for kl in leaves if _family(kl[1].label) not in own and id(kl) not in moved]
        return out + (by_count(rest) if rest else [])

    found = [item_number(leaf.label, holder) for _k, leaf in leaves]
    first = next((f for f in found if f is not None), None)
    if first is None:
        return by_count(leaves)
    numbers, last = [], first[0]
    for f in found:                                # a shelfmark without a number stays with the one before it
        last = f[0] if f is not None else last
        numbers.append(max(last, 1))
    if any(a > b for a, b in zip(numbers, numbers[1:])):
        return by_family() if families else by_count(leaves)
    at = found.index(first)
    prefix = leaves[at][1].label[:first[1]]

    def cut(lo, hi, width):
        blocks = defaultdict(list)
        for i in range(lo, hi):
            blocks[(numbers[i] - 1) // width].append(i)
        return [(k, v[0], v[-1] + 1) for k, v in sorted(blocks.items())]

    def divide(lo, hi, widths):
        width = min(widths, key=lambda w: (abs((hi - lo) / len(cut(lo, hi, w)) - RANGE_TARGET), w))
        out = []
        for k, a, b in cut(lo, hi, width):
            narrower = [w for w in widths if w < width]
            if b - a > RANGE_SIZE and narrower and len(cut(a, b, narrower[0])) > 1:
                out += divide(a, b, narrower)
            elif b - a > RANGE_SIZE:
                out += by_count(leaves[a:b])
            else:
                out.append((f"{prefix}{k * width + 1} - {(k + 1) * width}", leaves[a:b]))
        return out

    return divide(0, len(leaves), RANGE_WIDTHS)


def _other_of(nodes: dict, lib: str) -> str:
    nid = f"other:{lib}"
    if nid not in nodes:
        nodes[nid] = Node(nid, "other", lib, OTHER_NAME_EN, OTHER_NAME_HE, order=((9,),))
        nodes[lib].children.append(nid)
    return nid


def group_volumes(nodes: dict, collections_by_node: dict) -> None:
    """Ranges over a large collection's volumes, so a reader who knows the shelfmark finds its
    volume at once (owner, 2026-11-19): FGP's own ranges for ENA and T-S NS; by prefix where the
    volumes are written with several (Oxford "MS heb. a." ...); by number where one prefix has many."""
    for nid in [n for n, node in nodes.items() if node.kind == "collection"]:
        node = nodes[nid]
        vols = [c for c in node.children if nodes[c].kind == "volume"]
        cid = collections_by_node.get(nid)
        if len(vols) <= PREFIX_GROUP_MIN and cid not in FGP_GROUPS:
            continue
        rest = [c for c in node.children if nodes[c].kind != "volume"]
        # a prefix written two ways ("MS heb. c." / "MS Heb. C.") is one group, by its usual writing
        spelled = defaultdict(Counter)
        for v in vols:
            p = label_prefix(nodes[v].name_en)
            spelled[bare_name(p)][p] += 1
        by_prefix: dict = {}
        for v in vols:
            p = spelled[bare_name(label_prefix(nodes[v].name_en))].most_common(1)[0][0]
            by_prefix.setdefault(p, []).append(v)
        groups = []
        fixed = FGP_RANGES.get(cid, ())
        patterns = FGP_GROUPS.get(cid, ())
        big_prefixes = [p for p, vs in by_prefix.items() if len(vs) >= 5]
        hebrew = {p[0]: p[2] for p in patterns if len(p) > 2}
        if patterns:
            buckets: dict = {p[0]: [] for p in patterns}
            for v in vols:
                hit = next((p[0] for p in patterns if p[1] is None or p[1].search(nodes[v].name_en)), None)
                if hit is not None:
                    buckets[hit].append(v)
            if sum(map(len, buckets.values())) == len(vols):
                groups = [(p[0], buckets[p[0]], (0,)) for p in patterns if buckets[p[0]]]
        elif fixed:
            main = max(by_prefix, key=lambda p: len(by_prefix[p]))
            ranges, left = _number_groups(nodes, node, by_prefix.pop(main), main, nid, fixed)
            groups += [(label, members, (0,) + k) for label, members, k in ranges]
            if left:
                groups.append((f"{main} (other volumes)", left, (1,)))
            for p, vs in sorted(by_prefix.items(), key=lambda x: _nat(x[0])):
                groups.append((p or "(no prefix)", vs, (2, _nat(p))))
        elif len(big_prefixes) >= 2:
            for p, vs in sorted(by_prefix.items(), key=lambda x: _nat(x[0])):
                groups.append((p or "(no prefix)", vs, (0, _nat(p))))
        elif len(vols) > VOLUME_RANGE_MIN:
            main = max(by_prefix, key=lambda p: len(by_prefix[p]))
            ranges, left = _number_groups(nodes, node, by_prefix.pop(main), main, nid)
            groups += [(label, members, (0,) + k) for label, members, k in ranges]
            left += [v for vs in by_prefix.values() for v in vs]
            if left:
                groups.append((f"{main} (other volumes)", left, (1,)))
        if len(groups) < 2 and len(vols) > VOLUME_RANGE_MIN:
            # no prefixes or numbers to divide by: by the stem the volumes share ("T-S 6H1" ... -> "T-S 6H")
            stems: dict = {}
            for v in vols:
                stems.setdefault(re.sub(r"[\s.]*\d+\W*$", "", nodes[v].name_en) or nodes[v].name_en, []).append(v)
            if 3 <= len(stems) <= 80:
                groups = [(s, vs, (0, roman_natural_key(s))) for s, vs in stems.items()]
        if len(groups) < 2:
            continue
        made = []
        for k, (label, members, _order) in enumerate(groups):
            members.sort(key=lambda c: (nodes[c].order, c))
            gid = f"grp:{nid}:{k + 1}"
            # a group stands where its first volume stands in the collection's order
            made.append(_make_group(nodes, node, gid, label, members, ((5,), nodes[members[0]].order),
                                    name_he=hebrew.get(label, "")))
        node.children = made + rest


def groups_in_place(nodes: dict, collections_by_node: dict) -> None:
    """A collection of ``FGP_IN_PLACE`` is shown as FGP's groups of it alone: they stand in its
    place in the library, as collections ("T-S 6F - 8K" and "T-S 10C - 18K" for "T-S Bound"). Left
    as it is when it holds shelfmarks of its own or anything but groups."""
    for nid, cid in collections_by_node.items():
        col = nodes.get(nid)
        if (cid not in FGP_IN_PLACE or col is None or col.leaves or not col.children
                or any(nodes[c].kind != "group" for c in col.children)):
            continue
        library = nodes[col.parent]
        made = []
        for k, gid in enumerate(col.children, 1):
            group = nodes.pop(gid)
            group.id, group.kind, group.parent = f"{nid}:{k}", "collection", library.id
            group.order = (col.order[0], _nat(group.name_en))
            for child in group.children:
                nodes[child].parent = group.id
            nodes[group.id] = group
            made.append(group.id)
        at = library.children.index(nid)
        library.children[at:at + 1] = made
        del nodes[nid]


def _series(prefix: str) -> str:
    """A series as a reader names it, however it is written: by the word before the number. Case,
    punctuation and what stands before that word do not count ("Ms. FR", "MS FR" and "Fr." are
    one; Kyiv's "Harkavi R" is the library's "Ms. R")."""
    words = re.findall(r"[^\W\d_]+", (prefix or "").lower())
    return words[-1] if words else ""


def group_by_series(nodes: dict, node, minimum: Optional[int] = None) -> None:
    """Divide a node's shelfmarks by their series (Box D., Ms., ...): "Other shelfmarks" when it
    holds at least ``minimum`` (``GROUP_MIN``), a collection of ``LIBRARY_SERIES``. A series
    written several ways is one group, named as the library writes it: its records' call numbers
    before FGP's shelfmarks ("Ms. FR", not FGP's "Fr.")."""
    if len(node.leaves) < (GROUP_MIN if minimum is None else minimum):
        return
    by_series: dict = {}
    for kl in node.leaves:
        by_series.setdefault(_series(label_prefix(kl[1].label)), []).append(kl)
    if len(by_series) < 2:
        return
    named = []
    for kls in by_series.values():
        writings = (Counter(label_prefix(kl[1].label) for kl in kls if kl[1].how != "fist")
                    or Counter(label_prefix(kl[1].label) for kl in kls))
        named.append((writings.most_common(1)[0][0] or "(no prefix)", kls))
    node.leaves = []
    for k, (p, kls) in enumerate(sorted(named, key=lambda x: _nat(x[0]))):
        gid = f"grp:{node.id}:{k + 1}"
        nodes[gid] = Node(gid, "group", node.id, p, order=((5,), _nat(p)), leaves=kls, by_number=True)
        node.children.append(gid)


#: The item a shelfmark names after its volume's number: "/12", ".1", "/21bis", a folio ("f. 16/1"),
#: a Roman part ("A1 II").
_ITEM = re.compile(r"(?<=\d)(?:\s*f\.\s*\d+\w*(?:/\w+)?|[./]\d+[a-z]*|\s+[IVX]+)$")


def _volume_of(label: str) -> str:
    """The volume a shelfmark names before its item: "Friedberg MSS 9-004/12", "Friedberg MSS
    9-002.1", Sofer's "108 f. 16/1" and Berlin's "A1 II" are of "Friedberg MSS 9-004", "Friedberg
    MSS 9-002", "108" and "A1"; a shelfmark without an item is a volume of its own."""
    return _ITEM.sub("", label or "")


def _volume_key(volume: str, own_words) -> str:
    """A volume however it is written: "Ms." and the collection's own name do not count (Guenzburg's
    "Ms. Guenzburg 920" is its "Ms. 920")."""
    words = re.findall(r"[^\W\d_]+|\d+", (volume or "").lower())
    kept = [w for w in words if w != "ms" and _singular(w) not in own_words]
    return " ".join(kept or words)


def group_by_volume(nodes: dict, node) -> None:
    """Divide a collection of ``BY_VOLUME_NUMBER`` by volume, a group for each, in number order,
    named as most of its shelfmarks write it."""
    if node.children or not node.leaves:
        return
    own = _own_words(node.name_en)
    by_volume: dict = {}
    for kl in node.leaves:
        volume = _volume_of(kl[1].label)
        by_volume.setdefault(_volume_key(volume, own), []).append((volume, kl))
    if len(by_volume) < 2:
        return
    node.leaves = []
    for k, (key, items) in enumerate(sorted(by_volume.items(), key=lambda x: roman_natural_key(x[0])), 1):
        gid = f"grp:{node.id}:{k}"
        name = Counter(v for v, _kl in items).most_common(1)[0][0]
        nodes[gid] = Node(gid, "group", node.id, name, order=((5,), roman_natural_key(key)),
                          leaves=[kl for _v, kl in items])
        node.children.append(gid)


def collection_aliases(nodes: dict) -> None:
    """Give each collection the shelfmark heads its own leaves are written with, so a typed
    shelfmark finds a collection whose display name differs (CUL's "Oriental" holds ``Or. ...``).
    A head counts when at least ``MIN_CASES`` of a collection's leaves carry it (FGP's shelfmark or
    ours) and ``PURITY`` of the library's leaves with that head are in that collection. The
    collection's own name is not repeated."""
    def leaves_under(nid):
        out = [leaf for _k, leaf in nodes[nid].leaves]
        for child in nodes[nid].children:
            out += leaves_under(child)
        return out

    def library_of(nid):
        while nodes[nid].parent is not None:
            nid = nodes[nid].parent
        return nid

    per_library: dict[str, Counter] = defaultdict(Counter)   # library -> head -> leaves
    per_collection: dict[str, Counter] = {}
    for nid, node in nodes.items():
        if node.kind != "collection":
            continue
        counter = Counter()
        for leaf in leaves_under(nid):
            for h in {head(leaf.shelf_label), head(leaf.shelfmark)} - {()}:
                counter[h] += 1
        per_collection[nid] = counter
        per_library[library_of(nid)].update(counter)
    for nid, counter in per_collection.items():
        library = per_library[library_of(nid)]
        own = head(nodes[nid].name_en) or tuple(_TOKEN.findall(nodes[nid].name_en.lower()))
        nodes[nid].aliases = sorted(" ".join(map(str, h)) for h, n in counter.items()
                                    if n >= MIN_CASES and n / library[h] >= PURITY and h != own)


SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE node (
    id TEXT PRIMARY KEY, parent_id TEXT, kind TEXT NOT NULL, level INTEGER NOT NULL,
    name_en TEXT NOT NULL, name_he TEXT NOT NULL, short TEXT NOT NULL, private INTEGER NOT NULL,
    sort_key INTEGER NOT NULL, lo INTEGER NOT NULL, hi INTEGER NOT NULL,
    manuscripts INTEGER NOT NULL, direct_leaves INTEGER NOT NULL, child_nodes INTEGER NOT NULL);
CREATE TABLE leaf (
    id TEXT PRIMARY KEY, node_id TEXT NOT NULL, library_id TEXT NOT NULL, sys_id TEXT NOT NULL,
    shelfmark TEXT NOT NULL, shelf_label TEXT NOT NULL, label TEXT NOT NULL, part TEXT NOT NULL,
    inventory_id INTEGER,
    how TEXT NOT NULL,
    pos INTEGER NOT NULL UNIQUE,
    aliases TEXT NOT NULL);
CREATE TABLE library_code (code TEXT NOT NULL, node_id TEXT NOT NULL, PRIMARY KEY (code, node_id));
CREATE TABLE node_alias (node_id TEXT NOT NULL, alias TEXT NOT NULL, PRIMARY KEY (node_id, alias));
CREATE TABLE shortcut (id TEXT PRIMARY KEY, name_en TEXT NOT NULL, name_he TEXT NOT NULL, short TEXT NOT NULL,
    target_id TEXT NOT NULL);
CREATE INDEX idx_node_parent ON node(parent_id, sort_key);
CREATE INDEX idx_leaf_node ON leaf(node_id, pos);
CREATE INDEX idx_leaf_sys ON leaf(sys_id);
"""


def write(nodes, order, stats, out_path, inputs: dict, codes: dict):
    rows_node, rows_leaf = [], []
    pos = 0

    def walk(nid, level, sort_key, library_id):
        nonlocal pos
        node = nodes[nid]
        lo = pos
        ms = set()
        for i, child in enumerate(node.children):
            ms |= walk(child, level + 1, i, library_id)
        for _k, leaf in node.leaves:
            rows_leaf.append((leaf.id, nid, library_id, leaf.sys_id, leaf.shelfmark, leaf.shelf_label,
                              leaf.label or leaf.shelf_label or leaf.shelfmark, leaf.part, leaf.inventory_id,
                              leaf.how, pos, leaf.aliases))
            ms.add(leaf.sys_id)
            pos += 1
        rows_node.append((nid, node.parent, node.kind, level, node.name_en, node.name_he, node.short,
                          node.private, sort_key, lo, pos - 1, len(ms), len(node.leaves), len(node.children)))
        return ms

    for i, lib in enumerate(order):
        walk(lib, 0, i, lib)
    manuscripts = len({r[3] for r in rows_leaf})
    meta = {
        "version": SCHEMA_VERSION,
        "leaves_total": str(len(rows_leaf)),
        "nodes_total": str(len(rows_node)),
        "manuscripts_total": str(manuscripts),
        "top_library_min": str(TOP_LIBRARY_MIN),
        **{f"placed_{h}": str(stats.get(h, 0)) for h in HOW},
        "catalogue_placements": str(stats.get("catalogue_placements", 0)),
        **{k: str(v) for k, v in inputs.items()},
    }
    tmp = Path(str(out_path) + ".tmp")
    if tmp.exists():
        tmp.unlink()
    con = sqlite3.connect(str(tmp))
    try:
        con.executescript(SCHEMA)
        con.executemany("INSERT INTO meta VALUES (?, ?)", sorted(meta.items()))
        con.executemany("INSERT INTO node VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows_node)
        con.executemany("INSERT INTO leaf VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows_leaf)
        con.executemany("INSERT INTO shortcut VALUES (?, ?, ?, ?, ?)",
                        sorted((n.id, n.name_en, n.name_he, n.short, n.link) for n in nodes.values()
                               if n.kind == "link" and n.link in nodes))
        con.executemany("INSERT INTO library_code VALUES (?, ?)",
                        sorted((c, n) for c, ns in codes.items() for n in ns if n in nodes))
        con.executemany("INSERT INTO node_alias VALUES (?, ?)",
                        sorted((nid, a) for nid, n in nodes.items() for a in n.aliases))
        con.commit()
        con.execute("VACUUM")
    finally:
        con.close()
    return tmp, meta


def file_sha256(path) -> str:
    """The file's sha256: binds the tree to the ``libraries.csv`` it was built from (a change of a
    shelfmark or a code keeps the row count but must fail the gate until the tree is rebuilt)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _existing_manuscripts(path) -> Optional[int]:
    try:
        con = sqlite3.connect(f"file:{Path(path).resolve().as_posix()}?mode=ro", uri=True)
        try:
            row = con.execute("SELECT value FROM meta WHERE key = 'manuscripts_total'").fetchone()
        finally:
            con.close()
        return int(row[0]) if row else None
    except (sqlite3.Error, OSError, ValueError):
        return None


def build(fist, libraries, out, force=False, log=print) -> dict:
    # The inputs are fingerprinted before they are read and again before the file is swapped in:
    # an input replaced meanwhile would otherwise leave old placements under the new fingerprint.
    fist_sha, libraries_sha = file_sha256(fist), file_sha256(libraries)
    nodes, order, stats, tied = build_tree(fist, libraries, log=log)
    codes = defaultdict(set)
    for node in nodes.values():
        if node.kind == "library" and node.id.startswith("code:"):
            codes[node.short if node.short != "-" else ""].add(node.id)
    for code, lids in tied.items():
        # a library shown inside another answers for its codes there (Mosseri, Lewis-Gibson in CUL)
        codes[code] |= {f"lib:{MERGE_INTO.get(lid, (lid,))[0]}" for lid in lids}
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp, meta = write(nodes, order, stats, out, {
        "input_fist_bytes": os.path.getsize(fist),
        "input_fist_sha256": fist_sha,
        "input_libraries_rows": stats["manuscripts"],
        "input_libraries_sha256": libraries_sha,
    }, codes)
    changed = [name for name, path, sha in (("FIST.db", fist, fist_sha), ("libraries.csv", libraries, libraries_sha))
               if file_sha256(path) != sha]
    if changed:
        tmp.unlink()
        raise BuildError(f"{' and '.join(changed)} changed while the tree was built; build again")
    before = _existing_manuscripts(out) if out.exists() else None
    if before is not None and int(meta["manuscripts_total"]) < before and not force:
        tmp.unlink()
        raise BuildError(f"the new file holds {meta['manuscripts_total']} manuscripts, the old one {before}; "
                         "pass --force to replace it")
    os.replace(tmp, out)
    return meta


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--fist", default=str(DEFAULT_FIST))
    ap.add_argument("--libraries", default=str(DEFAULT_LIBRARIES))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--force", action="store_true", help="replace a file holding more manuscripts")
    args = ap.parse_args(argv)
    try:
        meta = build(args.fist, args.libraries, args.out, force=args.force)
    except BuildError as exc:
        print(f"FAIL: {exc}")
        return 1
    print("built", args.out)
    for k in sorted(meta):
        print(f"  {k} = {meta[k]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
