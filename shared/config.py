"""Config class extracted from genizah_core.py (v8.3.0 decomposition).

Stdlib-only leaf module — no imports from genizah_core or any project module.
Both the web process and the desktop app import Config from here directly
(web and desktop via the genizah_core.py permanent facade, shared/ modules
directly from shared.config).
"""
import os
import sys


# ==============================================================================
#  CONFIG CLASS (EXE Compatible)
# ==============================================================================
class Config:
    """Static paths and limits used by the application and by bundled binaries."""

    def _pick_writable_dir(primary: str, fallback: str) -> str:
        """
        Prefer primary; if we cannot create/write there, use fallback.
        Returns a directory path that is guaranteed (best-effort) to exist and be writable.
        """
        # Try primary
        try:
            os.makedirs(primary, exist_ok=True)
            test_path = os.path.join(primary, ".__write_test__")
            with open(test_path, "w", encoding="utf-8") as f:
                f.write("ok")
            os.remove(test_path)
            return primary
        except Exception:
            pass  # Best-effort path resolution; falls through to next candidate

        # Fallback
        os.makedirs(fallback, exist_ok=True)
        return fallback

    def _get_documents_dir() -> str:
        """Best-effort Documents directory (Windows-aware), falling back to home."""
        documents_dir = None
        try:
            import ctypes.wintypes

            CSIDL_PERSONAL = 5  # My Documents
            buf = ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
            ctypes.windll.shell32.SHGetFolderPathW(None, CSIDL_PERSONAL, None, 0, buf)
            if buf.value:
                documents_dir = buf.value
        except Exception:
            pass  # Best-effort path resolution; falls through to next candidate

        if not documents_dir or not os.path.isdir(documents_dir):
            for folder_name in ["Documents", "My Documents"]:
                candidate = os.path.join(os.path.expanduser("~"), folder_name)
                if os.path.isdir(candidate):
                    documents_dir = candidate
                    break

        return documents_dir if documents_dir and os.path.isdir(documents_dir) else os.path.expanduser("~")

    # 1. Determine Base Paths
    if getattr(sys, "frozen", False):
        BASE_DIR = os.path.dirname(sys.executable)
        _cand = os.path.join(BASE_DIR, "_internal")
        INTERNAL_DIR = _cand if os.path.isdir(_cand) else getattr(sys, "_MEIPASS", BASE_DIR)
    else:
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        INTERNAL_DIR = BASE_DIR

    # 2. External Files (Must be placed NEXT to the EXE by the user)
    FILE_V8 = os.path.join(BASE_DIR, "Transcriptions.txt")
    FILE_V7 = os.path.join(BASE_DIR, "AllGenizah_OLD.txt")

    # 3. User Data Directory (Index, Caches) - Smart Logic
    _PORTABLE_INDEX_PATH = os.path.join(BASE_DIR, "Genizah_Index")
    _APPDATA_PATH = os.path.join(
        os.getenv("LOCALAPPDATA", os.path.expanduser("~")),
        "GenizahSearchPro",
        "Index",
    )
    _LEGACY_PATH = os.path.join(os.path.expanduser("~"), "Genizah_Tantivy_Index")

    if os.path.exists(_PORTABLE_INDEX_PATH):
        INDEX_DIR = _PORTABLE_INDEX_PATH
    elif os.path.exists(_LEGACY_PATH) and not os.path.exists(_APPDATA_PATH):
        INDEX_DIR = _LEGACY_PATH
    else:
        INDEX_DIR = _APPDATA_PATH

    # Ensure the directory is created
    try:
        os.makedirs(INDEX_DIR, exist_ok=True)
    except Exception:
        # Registry/env path failed; fall back to portable index location
        INDEX_DIR = _PORTABLE_INDEX_PATH
        os.makedirs(INDEX_DIR, exist_ok=True)

    # 4. Output folders: always use Documents\GenizahSearchPro\Reports
    REPORTS_DIR = _pick_writable_dir(
        os.path.join(_get_documents_dir(), "GenizahSearchPro", "Reports"),
        os.path.join(INDEX_DIR, "Reports"),
    )

    IMAGE_CACHE_DIR = os.path.join(INDEX_DIR, "images_cache")

    # 5. Generated Files (Logs, Configs, Caches - inside Index Dir)
    CACHE_META = os.path.join(INDEX_DIR, "metadata_cache.pkl")
    CACHE_NLI = os.path.join(INDEX_DIR, "nli_cache.pkl")
    CONFIG_FILE = os.path.join(INDEX_DIR, "config.pkl")
    SESSION_FILE = os.path.join(INDEX_DIR, "session.json")
    LANGUAGE_FILE = os.path.join(INDEX_DIR, "lang.pkl")
    BROWSE_MAP = os.path.join(INDEX_DIR, "browse_map.pkl")
    LOG_FILE = os.path.join(INDEX_DIR, "genizah.log")

    # Lab Mode Paths
    LAB_DIR = os.path.join(INDEX_DIR, "lab")
    LAB_INDEX_DIR = os.path.join(INDEX_DIR, "lab_index")
    PASSAGE_INDEX_DIR = os.path.join(INDEX_DIR, "passage_index")
    LAB_CONFIG_FILE = os.path.join(LAB_DIR, "lab_config.json")
    LAB_WEIGHTS_FILE = os.path.join(LAB_DIR, "lab_weights.json")
    LAB_LOG_FILE = os.path.join(LAB_DIR, "lab_genizah.log")

    # Phase 95 D-14 — My Library side-indexes (co-located with INDEX_DIR for
    # portable-mode inheritance).
    LOCAL_INDEX_DIR = os.path.join(INDEX_DIR, "LocalIndex")
    LOCAL_LAB_INDEX_DIR = os.path.join(INDEX_DIR, "LocalLabIndex")
    # The My Library letter-level index. Its own directory, never inside
    # LocalIndex: it is built FROM the LOCAL sidecar, read-only, and the two
    # are replaced on independent schedules.
    LOCAL_PASSAGE_INDEX_DIR = os.path.join(INDEX_DIR, "passage_index_local")

    # 6. Bundled Internal Resources (Packaged inside the EXE/_internal)
    LIBRARIES_CSV = os.path.join(INTERNAL_DIR, "libraries.csv")
    OXFORD_DB = os.path.join(INTERNAL_DIR, "oxford_full_db.json")
    HELP_FILE = os.path.join(INTERNAL_DIR, "Help.html")

    # Settings
    SEARCH_LIMIT = 50000
    # Responsa reads up to this many page candidates (owner, decision 3 of
    # docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md, 2026-10-21): a true * scores every
    # match alike, so a cut at 50,000 kept an arbitrary part (*נים *לים has about
    # 100,000 candidate pages). The whole-manuscript documents and the seams each
    # read up to SEARCH_LIMIT of their own. Beyond it the search says "N+".
    RESPONSA_PAGE_LIMIT = 150_000
    # A Responsa query with a star reads the whole-manuscript documents for its matches
    # across pages only when at most this many hold its words; more, and those matches
    # are not searched and the list says "N+" (plan §7, Phase 3 point 7). Under a broad
    # star they would be billions of characters (§3.5), past the website's time limit.
    RESPONSA_STAR_AGGREGATE_LIMIT = 2_000
    # A website search (and an API background job) stops after this many seconds
    # and returns what it had checked, marked "N+" (owner ruling 2026-09-28): the
    # website has one search worker, and a stuck search would hold every other
    # visitor's search behind it. The desktop has no limit.
    WEB_SEARCH_TIME_LIMIT = 180
    VARIANT_GEN_LIMIT = 8000
    REGEX_VARIANTS_LIMIT = 8000
    WORD_TOKEN_PATTERN = r"[\w֐-׿\']+"
    # Responsa: the most terms one query's lookup may hold, counted as they are made
    # (shared/responsa_match.py::expand_under_guard). Measured 2026-10-21 on the local
    # index: 1,200 scored terms 0.6-0.8 s, 4,800 2.9-3.7 s, 9,600 5.5-6.0 s. It was
    # 500 estimated forms, which cut #X#'s 600 (24 prefixes x 25 suffixes).
    MAX_EXPANDED_TERMS = 5000
    # Responsa: the largest check or highlight pattern a query may make. Each bounded
    # word carries its edge classes (about 9 KB; three times inside AND), so a query of
    # many short words made megabytes of pattern within the 1,000-character cap (500
    # words: 12 MB, over 300 MB and 40 s to compile, which nothing interrupts). Measured
    # 2026-10-22: 0.5 MB compiles in about 1.6 s with re and 2.3 s with regex (both run);
    # the largest real queries make under 0.15 MB (#X# AND #Y#, 1,200 forms). The cap
    # allows about 55 words in a row, 27 bidirectional, 18 under AND.
    MAX_RESPONSA_PATTERN_CHARS = 600_000
    # Responsa: the longest form (a word with its prefixes and suffixes) a query may
    # hold. Its bracket-tolerant lookup is one regex_query, and tantivy-fst refuses
    # one longer than 332 characters on content_search, 141 on content (measured
    # 2026-10-22, the same for Hebrew, Latin and digits): past that the lookup lost
    # the form's bracketed spellings, so the query is refused instead.
    MAX_RESPONSA_FORM_CHARS = 120
    NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    HTTP_HEADERS = {"User-Agent": USER_AGENT}

    @staticmethod
    def resource_path(relative_path: str) -> str:
        """Return absolute path to bundled resources."""
        return os.path.join(Config.INTERNAL_DIR, relative_path)

    @staticmethod
    def relocate_index_dir(new_dir: str) -> str:
        """Point INDEX_DIR and every path under it (browse map, caches, side indexes) at
        *new_dir* (created), so a build or a measurement writes nothing into the live
        index folder (build_index.py --index-dir). Returns the absolute *new_dir*."""
        old = Config.INDEX_DIR
        new_dir = os.path.abspath(new_dir)
        os.makedirs(new_dir, exist_ok=True)
        for key, value in list(vars(Config).items()):
            if key.startswith('_') or not isinstance(value, str):
                continue
            if value == old or value.startswith(old + os.sep):
                setattr(Config, key, new_dir + value[len(old):])
        return new_dir

    @staticmethod
    def copy_shelfmark_caches(from_dir: str) -> list:
        """Copy metadata_cache.pkl and nli_cache.pkl from *from_dir* to CACHE_META and
        CACHE_NLI (after relocate_index_dir): the index's shelfmark field is read from
        them, so a build in a new folder matches one in the live folder. Returns the
        files copied; a missing one is skipped."""
        import shutil
        copied = []
        for target in (Config.CACHE_META, Config.CACHE_NLI):
            source = os.path.join(from_dir, os.path.basename(target))
            if os.path.isfile(source) and os.path.abspath(source) != os.path.abspath(target):
                shutil.copyfile(source, target)
                copied.append(source)
        return copied
