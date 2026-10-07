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
