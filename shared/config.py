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

    # 6. Bundled Internal Resources (Packaged inside the EXE/_internal)
    LIBRARIES_CSV = os.path.join(INTERNAL_DIR, "libraries.csv")
    OXFORD_DB = os.path.join(INTERNAL_DIR, "oxford_full_db.json")
    HELP_FILE = os.path.join(INTERNAL_DIR, "Help.html")

    # Settings
    SEARCH_LIMIT = 50000
    VARIANT_GEN_LIMIT = 8000
    REGEX_VARIANTS_LIMIT = 8000
    WORD_TOKEN_PATTERN = r"[\w֐-׿\']+"
    MAX_EXPANDED_TERMS = 500
    NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    HTTP_HEADERS = {"User-Agent": USER_AGENT}

    @staticmethod
    def resource_path(relative_path: str) -> str:
        """Return absolute path to bundled resources."""
        return os.path.join(Config.INTERNAL_DIR, relative_path)
