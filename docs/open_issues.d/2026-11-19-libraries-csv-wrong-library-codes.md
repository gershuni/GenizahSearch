## P3 data: Vernadsky and Budapest seminary records carry another library's code in libraries.csv
Where: `libraries.csv` column 3 (`library_code`)
Opened: 2026-11-19

Found while building the shelf tree (`scripts/build_shelfmark_tree.py`): 7 records whose catalogue
form names the Vernadsky National Library of Ukraine are coded `CUL`, and 2 records of the Budapest
rabbinical seminary ("The Jewish Theological Seminary - University of Jewish Studies", "BUD JTS")
are coded `JTS`. The library filter and the library name shown for them are therefore wrong. The
shelf tree is not misled (a record whose long form names another institution goes to "Other
shelfmarks"; the build prints them), but the codes should be corrected at the source, with a code
for each holding library.

## P3 data: 13 records in libraries.csv have no library code and no shelfmark
Where: `libraries.csv` columns 2-3
Opened: 2026-11-19

13 rows of `libraries.csv` have neither a call number nor a `library_code`. They cannot be found by
shelfmark or library. The shelf tree places 11 of them by the library the catalogue's own notes
name (`RECORD_LIBRARY` in `scripts/build_shelfmark_tree.py`, 2026-11-20: Oxford 1, the National
Library of Russia 4, Manchester 1, Mosseri 2, a private collection in Jerusalem 1, the Academy of
the Hebrew Language 1, Frankfurt 1) and shows them by their system number; the other 2 are under "Location
unknown". The codes and call numbers should still be filled at the source, which would make that
table unnecessary; one of the 13 is a typed list of Bodleian fragments, not a manuscript.
