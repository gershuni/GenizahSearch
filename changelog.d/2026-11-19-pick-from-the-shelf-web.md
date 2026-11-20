## [Unreleased] -- Pick from the shelf, and previous / next on the shelf (web, branch `claude/shelfmark-picker-web`)

### Added
- **Pick from the shelf** on `/browse`: beside Go, a button opens the library > collection >
  volume > shelfmark tree as FGP's "find a fragment" screen has it, in columns on a wide screen
  and one level at a time on a phone, with counts, a filter on every column and the current
  manuscript marked. A click on a shelfmark opens it (`web/components/shelfmark_tree_picker.py`,
  `shared/shelfmark_tree_view.py`).
- **The place on the shelf** under the manuscript's title: its library, collection and volume,
  each opening the picker there, and its position ("7 of 279").

- **The shelf as a reader looks for it** (owner's review of the first version): libraries named
  "City, Library" in both languages and never cut; the Mosseri Collection and Lewis-Gibson shown inside
  Cambridge University Library, each with a shortcut in the list of libraries; a library's only
  collection is no extra step (Oxford); ENA and T-S NS in FGP's own ranges (ENA 1-1000, 1001-2000,
  2001-2500, ...), Oxford, BL Or. and T-S A - K by prefix, other large collections by number; RNL's
  "Box D./K./O." records in FGP's "RNL Boxes"; "Other shelfmarks" divided by prefix; shelfmarks shown
  without institution or image-file names. A column's filter also finds the volumes and shelfmarks
  inside its groups: typing "2556.4" in ENA finds ENA 2556.4.

- **FGP's own arrangement, and a tree audited library by library** (owner's notes and a read-only
  audit of the built tree, 2026-11-20). A collection's volumes stand in FGP's order, with the
  numbers and Roman numerals of one series in their own (Mosseri: "Moss. I ... Xa", then the letters,
  then the "2nd Series"; "T-S Loan" after "T-S Misc.36"; Lewis-Gibson as FGP lists it). In place of
  "T-S Bound" stand FGP's two groups of it, "T-S 6F - 8K" and "T-S 10C - 18K"; Oxford is "MS heb.a -
  d", "MS heb.e - g", "Other" and a list **by Neubauer-Cowley catalogue** (a second place for 10,381
  shelfmarks; previous / next stay inside the list they began in). A node of more than 600 shelfmarks is divided by round item
  numbers as FGP does ("Moss. IV,1 - 100", "2nd Series: P 1 - 200"). A shelfmark is placed by its
  record's other call numbers where its label names only its collection (FGP's "ENA 2b" is "ENA
  2557.1"), and goes to the volume its label names ("D 37" in FGP's "F" of RNL Boxes is "Box D.37").
  Volumes FGP keeps apart are no longer folded together ("T-S F11" and "T-S F1(1)", "MS heb. f.7" and
  "f.7*"). Shelfmarks that showed one label in a node show what their call number adds ("OR 10578R
  f. 37", "T-S K7.15n", "Yevr.-Arab. II 974.1": about 1,000), and a record FGP lists twice under one
  shelfmark is shown once (247). A bare JTS "Ms. 9160" is "MS 9160", not "ENA 9160". FGP's slips in
  a volume's number are read from its shelfmarks ("ENA NS I" among "ENA NS 2" ... is "ENA NS 1", the
  volume FGP calls "ENA II 177" is "ENA II 117"; FGP's own writing is still found by a filter), and
  a list that mixes several forms of shelfmark is divided by form (Budapest: "1 - 200" ..., the Roman
  numerals, "M1 – M93", "H1 – H6"). Two records of one shelfmark stay two rows, the second marked
  ("MS L45", "MS L45 [2]"), also where the library writes the shelfmark another way than FGP
  (Columbia "X893.1 BC T 17" is FGP's "X893.1 BC T17"). The British Library's Halpern and Sassoon
  manuscripts are in "Or."; a code that names an FGP library is that library's (Basel); the National
  Library of Israel's "Ms. Heb. 577.2.26=4" is "577.2/26" of its volume; in a volume, its own
  shelfmarks come before the few it holds under another name; Oxford's third group is "אחר" in
  Hebrew, and the Oxford Part in a shelfmark's tooltip is called a Part. Eleven records that
  `libraries.csv` gives neither a call number nor a library code stand in the library the
  catalogue's notes name, shown by their system number; the two no library is known for are under
  "Location unknown". The Hebrew library names are written one way: one geresh character
  (מנצ'סטר, לוס אנג'לס), the city before the six names that lacked it, and a Hebrew name for five
  libraries known only by a code. The Lehmann Collection, Kyiv and the Ben-Zvi Institute are shown
  by their own series, FGP's shelfmarks among the library's ("Fr. 27/26" in Lehmann's "Ms. FR",
  "Harkavi R 14" in Kyiv's "Ms. R"), in number order; Toronto, Sofer, Guenzburg and the Berlin
  Jewish Community by volume number ("Friedberg MSS 9-004" holds "9-004/1" ... "9-004/32", Sofer's
  "108" its folios). A record whose long form names the library it is shown in stands in that
  library's collection, written as there (a Harkavy record is the NLI's "Ms. Heb. 1151.3=4"). A
  shelfmark written without its series' letter stands in the series it continues (FGP's "Yevr. III
  1155" in "Yevr. III B 1001 - 1200"). Empty records are left out: 111 records with no title, no
  page in the corpus and nothing but a call number, each beside the record of its shelfmark that has
  them (RNL "Box D.37", JTS "MS L502", Columbia "General 163.1" ...;
  `scripts/shelfmark_tree_left_out.csv`); FGP's row passes to the record kept.
- **The filter boxes keep what is typed**: a filter draws only its own list again, so the box keeps
  the caret; and the shelfmarks stand in as many columns as show the longest one whole (three, else
  two, else one).

### Changed
- **Previous / next manuscript follow the shelf** wherever the manuscript has a place on it
  (T-S AS 39.7 -> 39.8, across volumes and collections, stopping at the library's ends); a
  manuscript with no place, or a server without the shelf tree file, keeps the transcription
  file's order. The tooltips name the manuscript they lead to.

### Data and deploy
- `SHELFMARK_TREE_ENABLED` (on unless set off) AND `shelfmark_tree_data/shelfmark_tree.db` opened
  and checked at startup (`web/shelfmark_tree_assets.py`). Deploy: CODE FIRST, then
  `scripts/deploy_shelfmark_tree_sidecar.ps1 -Leaves 266673` (the manuscripts' places on the
  shelf and in the catalogue list).
- The builder no longer takes FGP's text "NULL" for a Hebrew library name (10 libraries); the
  verifier, renamed `scripts/verify_shelfmark_tree_sidecar.py` like the other sidecars' gates,
  refuses a node named "NULL" or nothing.
