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
shelfmark or library; the shelf tree lists them under "No library code" so they stay reachable.
Find their records in the NLI catalogue and fill both columns, or drop them if they are not
Genizah manuscripts.
