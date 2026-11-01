"""Catalogue criteria: one definition of every "manuscript feature" a reader can
list or restrict by, for Catalog Browse and "Search only in...", on the web
and the desktop, and the API (docs/plans/CATALOGUE_CRITERIA_PLAN.md, settled
rev. 4; the rules below are its §6.1).

A criteria value is a dict ``{key: spec}``:

* list criteria — ``{"values": [...], "mode": "only"|"except",
  "source": "either"|"catalogue"|"measured"}``; the reserved value
  ``NOT_RECORDED`` ("_none") picks manuscripts for which nothing is recorded;
* range criteria — ``{"min": n, "max": n, "not_recorded": bool,
  "source": ...}``, either bound open.

``source`` exists only where the criterion has two sources (D6); its default
is "either". This module owns the shapes (``clean_criteria``), the address
form (``criteria_to_params`` / ``criteria_from_params``, ruling D8) and the
matching (``match_criteria``), which runs SQL on the FJMS sidecar but keeps
the set algebra in Python:

* any record (D4): a manuscript has a value when any of its records gives it;
* "all except" (D15) = the universe minus "only";
* the universe is the corpus, uncatalogued manuscripts included (D14, D16);
* "not recorded" follows the chosen source (D5);
* measured values are per measured page — one unflagged row of
  ``computed_measurements`` — and every measured range set holds on ONE page
  (R1, D21); catalogue width and height on one ``catalog_sizes`` record.

It must not import ``shared.fjms_service`` (which imports it): it raises
``CriteriaUnavailable``, which the service turns into ``FilterUnavailable``.
"""
from __future__ import annotations

import itertools
import json
import math
import re
from dataclasses import dataclass

NOT_RECORDED = "_none"
SOURCES = ("either", "catalogue", "measured")
_SOURCE_PREFIX = {"catalogue": "cat:", "measured": "meas:"}
_PREFIX_SOURCE = {v: k for k, v in _SOURCE_PREFIX.items()}
EXCEPT_MARK = "!"
RANGE_MARK = ".."

# An unflagged measured page (as manuscript_measurements' own aggregate).
PAGE_OK = ("Flag_DPI_High = 0 AND Flag_DPI_Low = 0 "
           "AND Flag_Negative_Margin = 0 AND Flag_BifolioLoc_Error = 0")
# An unflagged catalogue size record (idem).
SIZE_OK = "Flag_WH_Swap IS NULL AND Flag_Unit_Error IS NULL"


class CriteriaUnavailable(RuntimeError):
    """A criterion cannot be evaluated here. ``reason``: 'column_missing'
    (the sidecar lacks what it needs; ``filters`` names the criteria),
    'corpus_unavailable' (no corpus list for "not recorded" / "all except"),
    or 'query_failed'."""

    def __init__(self, reason="query_failed", message="", filters=()):
        self.reason = reason
        self.message = message
        self.filters = tuple(filters or ())
        super().__init__(reason, message, self.filters)

    def __str__(self):
        return self.message or self.reason


# ── Sections and criteria ────────────────────────────────────────────────

@dataclass(frozen=True)
class Section:
    key: str
    label_he: str
    label_en: str


# FJMS's Hebrew section names (D9); 16 and 17 ours until FJMS's are known.
SECTIONS = (
    Section("period", "תקופת העתקה", "Period of copy"),
    Section("language", "שפה ואות", "Language and script"),
    Section("script", "כתיבה", "Script"),
    Section("vocalization", "ניקוד", "Vocalization"),
    Section("columns", "מספר טורים", "No. of columns"),
    Section("lines", "טווח שורות", "Range of lines"),
    Section("status", "מצב פיזי", "Physical status"),
    Section("material", "חומר", "Material"),
    Section("attributes", "תכונות", "Attributes"),
    Section("measured", "נתונים ממוחשבים", "Computer generated data"),
)


@dataclass(frozen=True)
class Criterion:
    key: str                 # dict key and address parameter
    section: str
    kind: str                # 'list' or 'range'
    label_he: str
    label_en: str
    sources: tuple = ()      # ('catalogue', 'measured') where D6 applies
    category: str = ""       # catalog_fields.FieldCategory (list criteria)
    value_key: str = ""      # criteria_values.key (coded columns, 5.1.0)
    number_key: str = ""     # criteria_numbers.key (catalogue ranges, 5.1.0)
    code_table: int = 0      # code_values table of a coded column's labels
    page_column: str = ""    # computed_measurements column (measured ranges)
    integer: bool = False    # range bounds are whole numbers
    unit: str = ""
    not_recordable: bool = True


CRITERIA = (
    Criterion("period", "period", "range", "תקופת העתקה", "Period of copy",
              number_key="period", integer=True),
    Criterion("language", "language", "list", "שפה", "Language", category="GenizahLanguages"),
    Criterion("alphabet", "language", "list", "אות", "Type of script", category="TypeOfScript"),
    Criterion("style", "script", "list", "כתב", "Style", category="TypeOfScriptStyle"),
    Criterion("region", "script", "list", "אזור", "Region", category="TypeOfScriptPlace"),
    Criterion("vocal_extent", "vocalization", "list", "היקף הניקוד", "Vocalization extent",
              value_key="vocal_extent", code_table=36),
    Criterion("vocal_type", "vocalization", "list", "סוג הניקוד", "Vocalization type",
              category="TypeOfVocalization"),
    Criterion("columns", "columns", "range", "מספר טורים", "No. of columns",
              number_key="columns", integer=True),
    Criterion("lines", "lines", "range", "מספר שורות", "No. of lines",
              sources=("catalogue", "measured"), number_key="lines", page_column="Num_Lines",
              integer=True),
    Criterion("status", "status", "list", "מצב פיזי", "Physical status", category="FragmentStatus"),
    Criterion("material", "material", "list", "חומר", "Material",
              sources=("catalogue", "measured"), category="FragmentMaterial"),
    Criterion("attributes", "attributes", "list", "תכונות", "Attributes",
              value_key="attributes", not_recordable=False),
    Criterion("width", "measured", "range", "רוחב", "Width", sources=("catalogue", "measured"),
              page_column="Page_Width_cm", unit="cm"),
    Criterion("height", "measured", "range", "גובה", "Height", sources=("catalogue", "measured"),
              page_column="Page_Height_cm", unit="cm"),
    Criterion("written_width", "measured", "range", "רוחב הכתוב", "Written width",
              page_column="Written_Width_cm", unit="cm"),
    Criterion("written_height", "measured", "range", "גובה הכתוב", "Written height",
              page_column="Written_Height_cm", unit="cm"),
    Criterion("margin_right", "measured", "range", "שוליים ימניים", "Right margin",
              page_column="Right_Margin_cm", unit="cm"),
    Criterion("margin_left", "measured", "range", "שוליים שמאליים", "Left margin",
              page_column="Left_Margin_cm", unit="cm"),
    Criterion("margin_top", "measured", "range", "שוליים עליונים", "Top margin",
              page_column="Top_Margin_cm", unit="cm"),
    Criterion("margin_bottom", "measured", "range", "שוליים תחתונים", "Bottom margin",
              page_column="Bottom_Margin_cm", unit="cm"),
    Criterion("line_height", "measured", "range", "גובה שורה", "Average line height",
              page_column="Avg_Line_Height_Text_mm", unit="mm"),
    Criterion("text_density", "measured", "range", "צפיפות הכתב", "Average text density",
              page_column="Text_Density_per10cm", unit="per 10 cm"),
    Criterion("dpi", "measured", "range", "רזולוציה", "Resolution",
              page_column="DpiGrid", integer=True, unit="dpi"),
)
BY_KEY = {c.key: c for c in CRITERIA}
CRITERIA_KEYS = tuple(BY_KEY)

# Ranges that hold on one measured page together (R1, D21).
PAGE_KEYS = tuple(c.key for c in CRITERIA if c.page_column)
# Measured-only ranges: always on the page, whatever the other sources.
MEASURED_ONLY_KEYS = tuple(c.key for c in CRITERIA if c.page_column and not c.sources)
# Catalogue width / height columns of catalog_sizes.
_SIZE_COLUMNS = {"width": "SizeX_cm", "height": "SizeY_cm"}
# Measured material values (manuscript_measurements.material) shown under the
# catalogue names they share; 'Mix' and 'Wood' are values of their own.
MEASURED_MATERIALS = ("Paper", "Vellum", "Papyrus", "Mix", "Wood")

# Attributes (R3: any of; yes-only, no Unknown / Not recorded).
ATTRIBUTES = {
    "illustration": ("איור", "Illustration", "IllustrationCode = 1038"),
    "palimpsest": ("פלימפססט", "Palimpsest", "PalimpsestCode = 1038"),
    "colophon": ("קולופון", "Colophon", "Colophon IS NOT NULL AND TRIM(Colophon) <> ''"),
    "cantillation": ("טעמים", "Cantillation", "CantillationCode IN (1036, 3036, 4036)"),
    "massorah": ("מסורה", "Massorah", "Massorah > 0"),
    "autograph": ("אוטוגרף", "Autograph", "Autograph = 1"),
}


def stored_code(code: int, table: int) -> int:
    """How a coded catalog column stores ``code`` of ``code_values`` table
    ``table``: sign(code) * (|code| * 1000 + table) (5.0.0 sidecar)."""
    return (1 if code >= 0 else -1) * (abs(code) * 1000 + table)


# ── Shapes ───────────────────────────────────────────────────────────────

def _number(value, integer: bool):
    if isinstance(value, bool) or value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"not a number: {value!r}") from None
    if math.isnan(number) or math.isinf(number):
        raise ValueError(f"not a number: {value!r}")
    if integer:
        if number != int(number):
            raise ValueError(f"not a whole number: {value!r}")
        return int(number)
    return int(number) if number == int(number) else number


def _clean_one(criterion: Criterion, spec) -> dict | None:
    if not isinstance(spec, dict):
        raise ValueError("expected an object")
    unknown = set(spec) - ({"values", "mode", "source"} if criterion.kind == "list"
                           else {"min", "max", "not_recorded", "source"})
    if unknown:
        raise ValueError(f"unknown field(s) {sorted(unknown)}")
    out = {}
    source = spec.get("source") or "either"
    if source not in SOURCES:
        raise ValueError(f"source must be one of {SOURCES}")
    if source != "either" and not criterion.sources:
        raise ValueError("has only one source")
    if criterion.sources:
        out["source"] = source
    if criterion.kind == "list":
        raw = spec.get("values") or []
        if isinstance(raw, str) or not isinstance(raw, (list, tuple)):
            raise ValueError("values must be a list")
        values = []
        for value in raw:
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"bad value {value!r}")
            value = value.strip()
            if value == NOT_RECORDED and not criterion.not_recordable:
                raise ValueError("has no 'not recorded'")
            if criterion.key == "attributes" and value not in ATTRIBUTES:
                raise ValueError(f"unknown attribute {value!r}")
            if value not in values:
                values.append(value)
        if not values:
            return None
        mode = spec.get("mode") or "only"
        if mode not in ("only", "except"):
            raise ValueError("mode must be 'only' or 'except'")
        out.update(values=values, mode=mode)
        return out
    low = _number(spec.get("min"), criterion.integer)
    high = _number(spec.get("max"), criterion.integer)
    if low is not None and high is not None and low > high:
        low, high = high, low  # reversed bounds are swapped, as today (D-19)
    not_recorded = spec.get("not_recorded", False)
    if not isinstance(not_recorded, bool):
        raise ValueError("not_recorded must be true or false")
    if low is None and high is None and not not_recorded:
        return None
    if low is not None:
        out["min"] = low
    if high is not None:
        out["max"] = high
    if not_recorded:
        out["not_recorded"] = True
    return out


def clean_criteria(raw, *, strict: bool = True):
    """The canonical criteria of ``raw``: known keys only, empty ones dropped.

    ``strict`` (the API): a malformed criterion raises ``ValueError`` naming
    it. Not strict (addresses, restored sessions): it is dropped and
    reported. Returns ``(criteria, problems)``; ``problems`` lists
    ``(key, message)``."""
    if raw is None:
        return {}, []
    if not isinstance(raw, dict):
        if strict:
            raise ValueError("criteria must be an object")
        return {}, [("criteria", "not an object")]
    clean, problems = {}, []
    for key, spec in raw.items():
        criterion = BY_KEY.get(key)
        try:
            if criterion is None:
                raise ValueError("unknown criterion")
            cleaned = _clean_one(criterion, spec)
        except ValueError as e:
            if strict:
                raise ValueError(f"criteria.{key}: {e}") from None
            problems.append((key, str(e)))
            continue
        if cleaned is not None:
            clean[key] = cleaned
    return {k: clean[k] for k in CRITERIA_KEYS if k in clean}, problems


def criteria_token(criteria) -> str:
    """A stable text form, for caches keyed on a selection."""
    return json.dumps(criteria or {}, sort_keys=True, ensure_ascii=False)


# ── Address form (D8) ────────────────────────────────────────────────────

def _format_number(value) -> str:
    return str(int(value)) if float(value) == int(value) else repr(float(value))


def criteria_to_params(criteria) -> dict:
    """One address parameter per criterion, its whole state in the value:
    ``style=Square,Semi-Cursive``, ``region=!Oriental``,
    ``material=cat:Vellum,_none``, ``lines=meas:20..25``, ``lines=..25``,
    ``columns=2..2,_none``, ``columns=_none``."""
    params = {}
    for key, spec in (criteria or {}).items():
        criterion = BY_KEY.get(key)
        if criterion is None:
            continue
        prefix = _SOURCE_PREFIX.get(spec.get("source", "either"), "")
        if criterion.kind == "list":
            mark = EXCEPT_MARK if spec.get("mode") == "except" else ""
            params[key] = prefix + mark + ",".join(spec["values"])
            continue
        parts = []
        if "min" in spec or "max" in spec:
            low = _format_number(spec["min"]) if "min" in spec else ""
            high = _format_number(spec["max"]) if "max" in spec else ""
            parts.append(f"{low}{RANGE_MARK}{high}")
        if spec.get("not_recorded"):
            parts.append(NOT_RECORDED)
        params[key] = prefix + ",".join(parts)
    return params


_RANGE_TEXT = re.compile(r"^\s*(-?\d+(?:\.\d+)?)?\s*\.\.\s*(-?\d+(?:\.\d+)?)?\s*$")


def criteria_from_params(params):
    """The criteria an address carries; ``params`` maps names to strings.
    Names that are not criteria are ignored (they belong to the page). A
    malformed value is dropped and reported: returns ``(criteria,
    problems)``."""
    raw, problems = {}, []
    for key in CRITERIA_KEYS:
        text = (params or {}).get(key)
        if text is None or str(text).strip() == "":
            continue
        text = str(text).strip()
        criterion = BY_KEY[key]
        spec = {}
        for prefix, source in _PREFIX_SOURCE.items():
            if text.startswith(prefix):
                spec["source"] = source
                text = text[len(prefix):]
                break
        if criterion.kind == "list":
            if text.startswith(EXCEPT_MARK):
                spec["mode"] = "except"
                text = text[len(EXCEPT_MARK):]
            spec["values"] = [v for v in text.split(",") if v.strip()]
            raw[key] = spec
            continue
        if text.startswith(EXCEPT_MARK):
            problems.append((key, "a range has no 'all except'"))
            continue
        bad = False
        for part in (p for p in text.split(",") if p.strip()):
            if part.strip() == NOT_RECORDED:
                spec["not_recorded"] = True
                continue
            match = _RANGE_TEXT.match(part)
            if not match or not (match.group(1) or match.group(2)):
                problems.append((key, f"not a range: {part!r}"))
                bad = True
                break
            if match.group(1):
                spec["min"] = match.group(1)
            if match.group(2):
                spec["max"] = match.group(2)
        if not bad:
            raw[key] = spec
    criteria, more = clean_criteria(raw, strict=False)
    return criteria, problems + more


# ── Matching ─────────────────────────────────────────────────────────────

class _Schema:
    """What the open sidecar holds, read once per match."""

    def __init__(self, conn):
        self._conn = conn
        self._tables = None
        self._columns = {}

    def has_table(self, name: str) -> bool:
        if self._tables is None:
            self._tables = {row[0] for row in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'")}
        return name in self._tables

    def has_column(self, table: str, column: str) -> bool:
        if table not in self._columns:
            self._columns[table] = (
                {row[1] for row in self._conn.execute(f"PRAGMA table_info({table})")}
                if self.has_table(table) else set())
        return column in self._columns[table]


def _ids(conn, sql: str, params=()) -> set:
    return {row[0] for row in conn.execute(sql, list(params)) if row[0]}


def _bounds(column: str, spec: dict, params: list) -> list:
    conds = []
    if "min" in spec:
        conds.append(f"{column} >= ?")
        params.append(spec["min"])
    if "max" in spec:
        conds.append(f"{column} <= ?")
        params.append(spec["max"])
    return conds


def _source(spec) -> str:
    return spec.get("source", "either")


class _Matcher:
    def __init__(self, conn, universe):
        self.conn = conn
        self.universe = universe
        self.schema = _Schema(conn)

    # -- capabilities --------------------------------------------------
    def need_universe(self, key: str):
        if self.universe is None:
            raise CriteriaUnavailable(
                "corpus_unavailable",
                "the list of all manuscripts is not loaded yet", filters=(key,))
        return self.universe

    def require(self, key: str, table: str, column: str | None = None):
        if not self.schema.has_table(table) or (
                column and not self.schema.has_column(table, column)):
            raise CriteriaUnavailable(
                "column_missing",
                f"this catalogue file has no {table}{'.' + column if column else ''}",
                filters=(key,))

    def check(self, criteria):
        """Every criterion's data is in the file, or the whole search stops
        (rule 9): never evaluated by one half of its sources."""
        for key, spec in criteria.items():
            criterion = BY_KEY[key]
            source = _source(spec)
            if criterion.category and (key != "material" or source != "measured"):
                self.require(key, "catalog_fields")
            if criterion.value_key:
                self.require(key, "criteria_values")
            if criterion.number_key and (not criterion.sources or source != "measured"):
                self.require(key, "criteria_numbers")
            if key == "material" and source != "catalogue":
                self.require(key, "manuscript_measurements", "material")
            if key in _SIZE_COLUMNS and source != "measured":
                self.require(key, "catalog_sizes")
            if criterion.page_column and (not criterion.sources or source != "catalogue"):
                self.require(key, "computed_measurements", criterion.page_column)
            needs_universe = (spec.get("mode") == "except"
                              or NOT_RECORDED in spec.get("values", ())
                              or spec.get("not_recorded"))
            if needs_universe:
                self.need_universe(key)

    # -- list criteria ---------------------------------------------------
    def list_values(self, key: str, values, source: str) -> set:
        criterion = BY_KEY[key]
        found = set()
        if criterion.category and source != "measured":
            ph = ",".join("?" * len(values))
            found |= _ids(self.conn,
                          "SELECT DISTINCT AlmaId FROM catalog_fields "
                          f"WHERE FieldCategory = ? AND FieldValue IN ({ph})",
                          [criterion.category, *values])
        if key == "material" and source != "catalogue":
            measured = [v for v in values if v in MEASURED_MATERIALS]
            if measured:
                ph = ",".join("?" * len(measured))
                found |= _ids(self.conn, "SELECT AlmaId FROM manuscript_measurements "
                              f"WHERE material IN ({ph})", measured)
        if criterion.value_key:
            ph = ",".join("?" * len(values))
            found |= _ids(self.conn, "SELECT DISTINCT AlmaId FROM criteria_values "
                          f"WHERE key = ? AND value IN ({ph})", [criterion.value_key, *values])
        return found

    def recorded(self, key: str, source: str = "either") -> set:
        """Manuscripts for which ``key`` is recorded by ``source``."""
        criterion = BY_KEY[key]
        found = set()
        if criterion.number_key and (not criterion.sources or source != "measured"):
            found |= _ids(self.conn, "SELECT DISTINCT AlmaId FROM criteria_numbers "
                          "WHERE key = ?", [criterion.number_key])
        if criterion.category and source != "measured":
            found |= _ids(self.conn, "SELECT DISTINCT AlmaId FROM catalog_fields "
                          "WHERE FieldCategory = ?", [criterion.category])
        if criterion.value_key:
            found |= _ids(self.conn, "SELECT DISTINCT AlmaId FROM criteria_values "
                          "WHERE key = ?", [criterion.value_key])
        if key == "material" and source != "catalogue":
            found |= _ids(self.conn, "SELECT AlmaId FROM manuscript_measurements "
                          "WHERE material IS NOT NULL AND material <> ''")
        if key in _SIZE_COLUMNS and source != "measured":
            found |= _ids(self.conn, f"SELECT DISTINCT AlmaId FROM catalog_sizes "
                          f"WHERE {SIZE_OK} AND {_SIZE_COLUMNS[key]} IS NOT NULL")
        if criterion.page_column and (not criterion.sources or source != "catalogue"):
            found |= _ids(self.conn, f"SELECT DISTINCT AlmaId FROM computed_measurements "
                          f"WHERE {PAGE_OK} AND {criterion.page_column} IS NOT NULL")
        return found

    def not_recorded(self, key: str, source: str) -> set:
        return self.need_universe(key) - self.recorded(key, source)

    def list_criterion(self, key: str, spec: dict) -> set:
        source = _source(spec)
        values = [v for v in spec["values"] if v != NOT_RECORDED]
        found = self.list_values(key, values, source) if values else set()
        if NOT_RECORDED in spec["values"]:
            found |= self.not_recorded(key, source)
        if spec.get("mode") == "except":
            return self.need_universe(key) - found
        return found

    # -- range criteria outside the page group ----------------------------
    def number_overlap(self, number_key: str, spec: dict) -> set:
        """Manuscripts with a catalogue interval of ``number_key`` (lines,
        columns, the copy span of D18) overlapping the asked range."""
        params, conds = [number_key], ["key = ?"]
        if "max" in spec:
            conds.append("lo <= ?")
            params.append(spec["max"])
        if "min" in spec:
            conds.append("hi >= ?")
            params.append(spec["min"])
        return _ids(self.conn, "SELECT DISTINCT AlmaId FROM criteria_numbers WHERE "
                    + " AND ".join(conds), params)

    def simple_range(self, key: str, spec: dict) -> set:
        """Period and columns: catalogue only, outside the page group."""
        has_bounds = "min" in spec or "max" in spec
        found = self.number_overlap(BY_KEY[key].number_key, spec) if has_bounds else set()
        if spec.get("not_recorded"):
            found |= self.not_recorded(key, "catalogue")
        return found

    # -- the page group (lines, sizes, measured-only ranges) ---------------
    def pages(self, specs: dict) -> set:
        """Manuscripts with ONE unflagged measured page meeting every range."""
        params, conds = [], [PAGE_OK]
        for key, spec in specs.items():
            conds += _bounds(BY_KEY[key].page_column, spec, params)
        return _ids(self.conn, "SELECT DISTINCT AlmaId FROM computed_measurements "
                    "WHERE " + " AND ".join(conds), params)

    def size_records(self, specs: dict) -> set:
        """Manuscripts with ONE unflagged catalogue size record meeting them."""
        params, conds = [], [SIZE_OK]
        for key, spec in specs.items():
            conds += _bounds(_SIZE_COLUMNS[key], spec, params)
        return _ids(self.conn, "SELECT DISTINCT AlmaId FROM catalog_sizes "
                    "WHERE " + " AND ".join(conds), params)

    def page_group(self, specs: dict) -> set | None:
        """Rule 4. ``specs`` holds bounded ranges only. For each criterion in
        'either' mode both halves are tried; all measured choices plus the
        measured-only ranges hold on one page, the catalogue size choices on
        one size record."""
        if not specs:
            return None
        either = [k for k in specs if BY_KEY[k].sources and _source(specs[k]) == "either"]
        result = set()
        for choice in itertools.product(("catalogue", "measured"), repeat=len(either)):
            chosen = dict(zip(either, choice))

            def side(key):
                if not BY_KEY[key].sources:
                    return "measured"
                return chosen.get(key, _source(specs[key]))
            on_page = {k: s for k, s in specs.items() if side(k) == "measured"}
            sizes = {k: s for k, s in specs.items()
                     if k in _SIZE_COLUMNS and side(k) == "catalogue"}
            parts = []
            if on_page:
                parts.append(self.pages(on_page))
            if sizes:
                parts.append(self.size_records(sizes))
            if "lines" in specs and side("lines") == "catalogue":
                parts.append(self.number_overlap("lines", specs["lines"]))
            result |= set.intersection(*parts)
        return result

    def ranges_with_not_recorded(self, specs: dict) -> set | None:
        """The page group with 'not recorded': a range with no bounds asks
        for manuscripts with nothing recorded for it (a plain filter); a
        bounded range marked 'or not recorded' also lets those through,
        while every other range still applies to them."""
        only_missing = [k for k, s in specs.items() if "min" not in s and "max" not in s]
        bounded = {k: s for k, s in specs.items() if k not in only_missing}
        result = self._bounded(bounded, {}) if bounded else None
        for key in only_missing:
            missing = self.not_recorded(key, _source(specs[key]))
            result = missing if result is None else result & missing
        return result

    def _bounded(self, specs: dict, memo: dict) -> set:
        token = frozenset(specs)
        if token in memo:
            return memo[token]
        result = self.page_group(specs)
        for key, spec in specs.items():
            if not spec.get("not_recorded"):
                continue
            rest = {k: s for k, s in specs.items() if k != key}
            missing = self.not_recorded(key, _source(spec))
            result |= (missing & self._bounded(rest, memo)) if rest else missing
        memo[token] = result
        return result


def match_criteria(conn, criteria, universe=None):
    """The manuscripts the criteria select, as a set of sys_ids, or None when
    no criterion is active. ``universe`` is the corpus (a set of every
    sys_id); without it, "not recorded" and "all except" raise
    ``CriteriaUnavailable('corpus_unavailable')`` and the other criteria
    return catalogued (and measured) manuscripts only. Raises
    ``CriteriaUnavailable`` when the file cannot answer a criterion; any SQL
    error propagates (the caller reports it as a failed lookup)."""
    criteria, _ = clean_criteria(criteria)
    if not criteria:
        return None
    matcher = _Matcher(conn, universe)
    matcher.check(criteria)
    parts = []
    group = {}
    for key, spec in criteria.items():
        criterion = BY_KEY[key]
        if criterion.kind == "list":
            parts.append(matcher.list_criterion(key, spec))
        elif key in PAGE_KEYS:
            group[key] = spec
        else:
            parts.append(matcher.simple_range(key, spec))
    if group:
        grouped = matcher.ranges_with_not_recorded(group)
        if grouped is not None:
            parts.append(grouped)
    result = set.intersection(*parts) if parts else set()
    if universe is not None:
        result &= universe
    return result


# ── Value lists ──────────────────────────────────────────────────────────

_UNCERTAIN = ("Unknown", "Cannot be determined")


def criterion_values(conn, key: str, universe=None, source: str = "either") -> list:
    """The choices a list criterion offers: ``[{value, label_en, label_he,
    count}]`` by count, then the recorded Unknown / Cannot be determined,
    then Not recorded (with a universe; never for Attributes). Counts are
    distinct manuscripts, within the universe when given."""
    criterion = BY_KEY[key]
    if criterion.kind != "list":
        raise ValueError(f"{key} is not a list criterion")
    rows = {}  # value -> [label_he, ids]

    def add(value, label_he, alma):
        entry = rows.setdefault(value, [label_he or "", set()])
        if label_he and not entry[0]:
            entry[0] = label_he
        entry[1].add(alma)

    if criterion.category and source != "measured":
        for value, label_he, alma in conn.execute(
                "SELECT FieldValue, FieldValueHeb, AlmaId FROM catalog_fields "
                "WHERE FieldCategory = ? AND FieldValue IS NOT NULL", [criterion.category]):
            add(value, label_he, alma)
    if key == "material" and source != "catalogue":
        for value, alma in conn.execute(
                "SELECT material, AlmaId FROM manuscript_measurements "
                "WHERE material IS NOT NULL AND material <> ''"):
            add(value, "", alma)
    if criterion.value_key:
        hebrew = {}
        if key == "attributes":
            hebrew = {value: label_he for value, (label_he, _en, _sql) in ATTRIBUTES.items()}
        elif criterion.code_table:
            hebrew = dict(conn.execute("SELECT EngDesc, HebDesc FROM code_values "
                                       "WHERE FCDTableId = ?", [criterion.code_table]).fetchall())
        for value, alma in conn.execute("SELECT value, AlmaId FROM criteria_values "
                                        "WHERE key = ?", [criterion.value_key]):
            add(value, hebrew.get(value, ""), alma)
    out = []
    for value, (label_he, ids) in rows.items():
        if universe is not None:
            ids = ids & universe
        if not ids:
            continue
        label_en = ATTRIBUTES[value][1] if key == "attributes" else value
        out.append({"value": value, "label_en": label_en, "label_he": label_he,
                    "count": len(ids)})
    out.sort(key=lambda v: (v["value"] in _UNCERTAIN, _UNCERTAIN.index(v["value"])
                            if v["value"] in _UNCERTAIN else 0, -v["count"], v["value"]))
    if universe is not None and criterion.not_recordable:
        missing = _Matcher(conn, universe).not_recorded(key, source)
        out.append({"value": NOT_RECORDED, "label_en": "Not recorded",
                    "label_he": "לא נרשם", "count": len(missing)})
    return out
