"""Small FJMS sidecar files for the filter-unavailable tests (#17).

A helper module, like tests/list_sync_scenarios.py: test files import it
after putting their own directory on sys.path.

``build_sidecar(path, with_line_height=..., with_measurements=...)`` writes a
real SQLite file with the tables get_filter_sys_ids reads for date and
measurement filters:

* with_line_height=False reproduces the v5.0.0 sidecar in use today (the
  shipped copy and production), whose manuscript_measurements table has no
  avg_line_height_mm column;
* with_measurements=False reproduces an older copy with no
  manuscript_measurements table at all.
"""
import sqlite3

ROWS = [
    # AlmaId, CopyDate, width, lines, line_height
    ("990001", "1050", 12.0, 20, 3.4),
    ("990002", "1150", 25.0, 30, 4.1),
    ("990003", "", 8.0, 12, 5.0),
]


def build_sidecar(path, *, with_line_height: bool = True,
                  with_measurements: bool = True) -> str:
    con = sqlite3.connect(str(path))
    con.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    con.execute("INSERT INTO meta VALUES ('version', '5.0.0')")
    con.execute("CREATE TABLE catalog (AlmaId TEXT PRIMARY KEY, CopyDate TEXT)")
    for alma, date, *_ in ROWS:
        con.execute("INSERT INTO catalog VALUES (?, ?)", (alma, date))
    if with_measurements:
        lh_col = ", avg_line_height_mm REAL" if with_line_height else ""
        con.execute(
            "CREATE TABLE manuscript_measurements ("
            " AlmaId TEXT PRIMARY KEY,"
            " catalog_width_cm REAL, max_computed_width_cm REAL,"
            " catalog_height_cm REAL, max_computed_height_cm REAL,"
            " avg_num_lines REAL, avg_text_density REAL, material TEXT"
            f"{lh_col})"
        )
        for alma, _date, width, lines, lh in ROWS:
            values = [alma, width, None, None, None, lines, None, "Paper"]
            if with_line_height:
                values.append(lh)
            ph = ",".join("?" * len(values))
            con.execute(f"INSERT INTO manuscript_measurements VALUES ({ph})", values)
    con.commit()
    con.close()
    return str(path)


SCRIPT_FIELDS = [
    # AlmaId, catalogue record, category, value, Hebrew
    ("990001", 1, "TypeOfScriptStyle", "Square", "מרובע"),
    ("990001", 1, "TypeOfScriptPlace", "Spanish", "ספרדי"),
    ("990002", 2, "TypeOfScriptStyle", "Semi-Cursive", "בינוני"),
    ("990002", 2, "TypeOfScriptPlace", "Oriental", "מזרחי"),
    # Two records of one manuscript that disagree: either value matches it.
    ("990003", 3, "TypeOfScriptStyle", "Square", "מרובע"),
    ("990003", 3, "TypeOfScriptPlace", "Oriental", "מזרחי"),
    ("990003", 4, "TypeOfScriptPlace", "Yemenite", "תימני"),
    # A third record repeating a value: the manuscript is counted once.
    ("990003", 5, "TypeOfScriptStyle", "Square", "מרובע"),
    # Recorded, but describes no script: never offered.
    ("990004", 6, "TypeOfScriptPlace", "Unknown", "לא ידוע"),
    ("990005", 7, "FragmentMaterial", "Paper", "נייר"),
]
# 990006 has no catalog_fields rows at all.
SCRIPT_DATES = {"990001": "1100", "990002": "1300", "990003": "1150",
                "990004": "1200", "990005": "1100", "990006": "1100"}


def build_script_sidecar(path) -> str:
    """A sidecar with the catalogue's script fields (the Script Style / Script
    Place filters): SCRIPT_FIELDS over the manuscripts of SCRIPT_DATES."""
    con = sqlite3.connect(str(path))
    con.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    con.execute("INSERT INTO meta VALUES ('version', '5.0.0')")
    con.execute("CREATE TABLE catalog (AlmaId TEXT PRIMARY KEY, CopyDate TEXT)")
    con.executemany("INSERT INTO catalog VALUES (?, ?)", SCRIPT_DATES.items())
    con.execute("CREATE TABLE catalog_fields (AlmaId TEXT NOT NULL, UnitCatalogRecId INTEGER NOT NULL,"
                " FieldCategory TEXT NOT NULL, FieldValue TEXT, FieldValueHeb TEXT)")
    con.executemany("INSERT INTO catalog_fields VALUES (?, ?, ?, ?, ?)", SCRIPT_FIELDS)
    con.commit()
    con.close()
    return str(path)


# Measurements of the SCRIPT_DATES manuscripts (990004 and 990006 have none).
# AlmaId: (catalog_width, computed_width, catalog_height, lines, material)
BROWSE_MEASUREMENTS = {
    "990001": (12.0, None, 20.0, 18, "Vellum"),
    "990002": (25.0, None, 30.0, 30, "Paper"),
    "990003": (None, 15.0, 22.0, 25, "Paper"),  # width only from the images
    "990005": (30.0, None, 40.0, 40, "Paper"),
}


def build_browse_sidecar(path) -> str:
    """build_script_sidecar plus what Catalog Browse reads: the catalog's
    display columns, domains, and manuscript_measurements."""
    con = sqlite3.connect(str(path))
    con.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    con.execute("INSERT INTO meta VALUES ('version', '5.0.0')")
    con.execute("CREATE TABLE catalog (AlmaId TEXT, Title TEXT, TitleHeb TEXT, AuthorText TEXT,"
                " CopyDate TEXT, TextualFrameHeb TEXT, TextualFrameEng TEXT)")
    con.executemany("INSERT INTO catalog VALUES (?, ?, '', '', ?, '', '')",
                    [(alma, f"Title {alma}", date) for alma, date in SCRIPT_DATES.items()])
    con.execute("CREATE TABLE domains (AlmaId TEXT, Domain TEXT, DomainHeb TEXT,"
                " ParentDomain TEXT, ParentDomainHeb TEXT)")
    con.execute("INSERT INTO domains VALUES ('990001', 'Liturgy', 'ליטורגיה', NULL, NULL)")
    con.execute("CREATE TABLE catalog_fields (AlmaId TEXT NOT NULL, UnitCatalogRecId INTEGER NOT NULL,"
                " FieldCategory TEXT NOT NULL, FieldValue TEXT, FieldValueHeb TEXT)")
    con.executemany("INSERT INTO catalog_fields VALUES (?, ?, ?, ?, ?)", SCRIPT_FIELDS)
    con.execute(
        "CREATE TABLE manuscript_measurements (AlmaId TEXT PRIMARY KEY,"
        " catalog_width_cm REAL, max_computed_width_cm REAL,"
        " catalog_height_cm REAL, max_computed_height_cm REAL,"
        " avg_num_lines REAL, avg_text_density REAL, material TEXT, avg_line_height_mm REAL)")
    con.executemany(
        "INSERT INTO manuscript_measurements VALUES (?, ?, ?, ?, NULL, ?, NULL, ?, NULL)",
        [(alma, *values) for alma, values in BROWSE_MEASUREMENTS.items()])
    con.commit()
    con.close()
    return str(path)


# A history summary long enough that truncation can cut between its two
# brackets: exclude mode puts the script value in a bracket of its own.
LONG_EXCLUDE = {'include_mode': False, 'domains': ['Bible', 'Liturgy', 'Halakha'],
                'date_from': 1000, 'date_to': 1300, 'script_places': ['Spanish']}
LONG_EXCLUDE_50 = '[exclude: Bible, Liturgy, Halakha, 1000-1300...]'


def brackets_balanced(s: str) -> bool:
    """Every '[' closed before the next opens, and nothing left open."""
    depth = 0
    for ch in s:
        depth += {'[': 1, ']': -1}.get(ch, 0)
        if depth not in (0, 1):
            return False
    return depth == 0


class FailingFinalExecute:
    """Wraps a real connection; the filter's final SELECT raises OperationalError.

    Every other statement (PRAGMA, vocabulary reads) goes to the real file, so
    the failure lands exactly where a disk or lock error would.
    """

    def __init__(self, real):
        self._real = real

    def execute(self, sql, params=()):
        if sql.lstrip().startswith("SELECT DISTINCT c.AlmaId FROM catalog c"):
            raise sqlite3.OperationalError("disk I/O error")
        return self._real.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._real, name)


class FailingSchemaRead:
    """Wraps a real connection; reading the measurement schema fails.

    Everything else goes to the real file. Models a sidecar whose schema
    could not be inspected (a locked or half-replaced file): the service must
    then NOT know whether a measurement filter is supported (K-16).
    """

    def __init__(self, real):
        self._real = real

    def execute(self, sql, params=()):
        if "table_info(manuscript_measurements)" in sql:
            raise sqlite3.OperationalError("database is locked")
        return self._real.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._real, name)


def unknown_schema_service(path):
    """A real FjmsService on ``path`` whose measurement schema is unknown."""
    from shared.fjms_service import FjmsService
    svc = FjmsService(db_path=str(path))
    svc._conn = FailingSchemaRead(svc._conn)
    svc._measurement_columns = None
    return svc


def call(fn, **kwargs):
    """Return ('returned', value) or ('raised', exc) so a test can say which."""
    try:
        return "returned", fn(**kwargs)
    except Exception as exc:  # noqa: BLE001 - the test inspects the type
        return "raised", exc


def assert_unavailable(outcome, value, reason):
    """The lookup must have RAISED FilterUnavailable with ``reason``."""
    assert outcome == "raised", (
        f"get_filter_sys_ids returned {value!r} for a lookup that could not run; "
        "callers read that as a real answer"
    )
    assert type(value).__name__ == "FilterUnavailable", repr(value)
    assert value.reason == reason, value.reason
