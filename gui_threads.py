"""Worker threads used by the PyQt GUI for long-running operations."""

# gui_threads.py
from PyQt6.QtCore import QThread, pyqtSignal
from genizah_core import SearchEngine, Indexer, MetadataManager, VariantManager, AIManager, check_external_services

class ConnectivityThread(QThread):
    """Check connectivity in a separate thread and emit signal with result."""
    finished_signal = pyqtSignal(dict)

    def __init__(self, ai_mgr):
        super().__init__()
        self.ai_mgr = ai_mgr

    def run(self):
        try:
            extra = {}
            if self.ai_mgr:
                ai_endpoint = self.ai_mgr.get_healthcheck_endpoint()
                if ai_endpoint:
                    extra['ai_provider'] = ai_endpoint

            statuses = check_external_services(extra_endpoints=extra, timeout=3)
            self.finished_signal.emit(statuses)
        except Exception as e:
            # Emit a minimal "failure" status so the UI can update
            self.finished_signal.emit({"error": str(e)})

class IndexerThread(QThread):
    """Build or refresh the index without blocking the UI."""

    progress_signal = pyqtSignal(int, int)
    finished_signal = pyqtSignal(int)
    error_signal = pyqtSignal(str)
    def __init__(self, meta_mgr):
        super().__init__()
        self.indexer = Indexer(meta_mgr)

    def run(self):
        try:
            def callback(curr, total): self.progress_signal.emit(curr, total)
            total_docs = self.indexer.create_index(progress_callback=callback)
            self.finished_signal.emit(total_docs)
        except Exception as e: self.error_signal.emit(str(e))

class SearchThread(QThread):
    """Execute a search query asynchronously."""

    results_signal = pyqtSignal(list)
    progress_signal = pyqtSignal(int, int)
    error_signal = pyqtSignal(str)
    def __init__(self, searcher, query, mode, gap):
        super().__init__()
        self.searcher = searcher; self.query = query; self.mode = mode; self.gap = gap

    def run(self):
        try:
            def cb(curr, total): self.progress_signal.emit(curr, total)
            results = self.searcher.execute_search(self.query, self.mode, self.gap, progress_callback=cb)
            self.results_signal.emit(results)
        except Exception as e: self.error_signal.emit(str(e))

class LabSearchThread(QThread):
    """Execute a Lab Mode search query."""

    results_signal = pyqtSignal(list)
    progress_signal = pyqtSignal(int, int) # Not fully utilized yet but good for future
    status_signal = pyqtSignal(str)
    error_signal = pyqtSignal(str)

    def __init__(self, lab_engine, query, mode, gap=0, deep_scan=False, scan_limit=50000):
        super().__init__()
        self.lab_engine = lab_engine
        self.query = query
        self.gap = gap
        self.mode = mode
        self.deep_scan = deep_scan
        self.scan_limit = scan_limit

    def run(self):
        try:
            # Helper to handle different callback signatures
            def cb(arg1, arg2=None):
                if isinstance(arg1, str):
                    self.status_signal.emit(arg1)
                elif isinstance(arg1, int) and arg2 is not None:
                    self.progress_signal.emit(arg1, arg2)

            results = self.lab_engine.lab_search(
                self.query,
                mode=self.mode,
                progress_callback=cb,
                gap=self.gap,
                deep_scan=self.deep_scan,
                scan_limit=self.scan_limit
            )
            self.results_signal.emit(results)
        except Exception as e: self.error_signal.emit(str(e))

class CompositionThread(QThread):
    """Scan compositions in background to keep UI responsive."""

    progress_signal = pyqtSignal(int, int)
    status_signal = pyqtSignal(str)
    scan_finished_signal = pyqtSignal(object) # Changed from list to object to support dict return
    error_signal = pyqtSignal(str)

    def __init__(self, searcher, text, chunk, freq, mode, filter_text=None, threshold=5):
        super().__init__()
        self.searcher = searcher
        self.text = text
        self.chunk = chunk
        self.freq = freq
        self.mode = mode
        self.filter_text = filter_text
        self.threshold = threshold

    def run(self):
        try:
            self.status_signal.emit("Scanning chunks...")
            def cb(curr, total): self.progress_signal.emit(curr, total)

            # Returns dict {'main': [], 'filtered': []} or list [] (legacy safety)
            result = self.searcher.search_composition_logic(
                self.text, self.chunk, self.freq, self.mode,
                filter_text=self.filter_text, progress_callback=cb
            )
            self.scan_finished_signal.emit(result)
        except Exception as e: self.error_signal.emit(str(e))

class LabCompositionThread(QThread):
    """Execute Lab Composition Search (Broad-to-Narrow)."""

    progress_signal = pyqtSignal(int, int)
    status_signal = pyqtSignal(str)
    scan_finished_signal = pyqtSignal(object) 
    error_signal = pyqtSignal(str)

    # --- הוספנו כאן את excluded_ids ואת filter_text ---
    def __init__(self, lab_engine, text, mode, chunk_size=None, excluded_ids=None, filter_text=None, deep_scan=False, scan_limit=50000):
        super().__init__()
        self.lab_engine = lab_engine
        self.text = text
        self.chunk_size = chunk_size
        self.mode = mode
        self.excluded_ids = excluded_ids # שמירה
        self.filter_text = filter_text
        self.deep_scan = deep_scan
        self.scan_limit = scan_limit

    def run(self):
        try:
            self.status_signal.emit("Lab Mode: Broad-to-Narrow Scan...")

            # Callback handler that supports both (int, int) and (str)
            def cb(arg1, arg2=None):
                if isinstance(arg1, str):
                    self.status_signal.emit(arg1)
                elif isinstance(arg1, int) and arg2 is not None:
                    self.progress_signal.emit(arg1, arg2)
            
            # --- העברה למנוע ---
            result = self.lab_engine.lab_composition_search(
                self.text,
                mode=self.mode,
                progress_callback=cb,
                chunk_size=self.chunk_size,
                excluded_ids=self.excluded_ids,
                filter_text=self.filter_text,
                deep_scan=self.deep_scan,
                scan_limit=self.scan_limit
            )
            self.scan_finished_signal.emit(result)
        except Exception as e: self.error_signal.emit(str(e))

class GroupingThread(QThread):
    """Group composition results while reporting progress to the UI."""

    progress_signal = pyqtSignal(int, int)
    status_signal = pyqtSignal(str)
    # Emit 6 args: main_res, main_appx, main_summ, filt_res, filt_appx, filt_summ
    finished_signal = pyqtSignal(list, dict, dict, list, dict, dict)
    error_signal = pyqtSignal(str)

    def __init__(self, searcher, items, threshold=5, filtered_items=None):
        super().__init__()
        self.searcher = searcher
        self.items = items
        self.threshold = threshold
        self.filtered_items = filtered_items or []

    def run(self):
        try:
            def check(): return self.isInterruptionRequested()

            # 1. Group Main Items
            # תיקון: הוספת *args כדי להתעלם מהפרמטר השלישי (sid) אם נשלח
            def cb1(curr, total, *args): self.progress_signal.emit(curr, total)
            self.status_signal.emit("Grouping main results...")

            result_main = self.searcher.group_composition_results(
                self.items, self.threshold, progress_callback=cb1, check_cancel=check, status_callback=self.status_signal.emit
            )
            if not result_main or result_main[0] is None:
                return # Cancelled

            main_res, main_appx, main_summ = result_main

            # 2. Group Filtered Items
            filt_res, filt_appx, filt_summ = [], {}, {}
            if self.filtered_items:
                self.status_signal.emit("Grouping filtered results...")
                # תיקון: הוספת *args גם כאן
                def cb2(curr, total, *args): self.progress_signal.emit(curr, total)

                result_filt = self.searcher.group_composition_results(
                    self.filtered_items, self.threshold, progress_callback=cb2, check_cancel=check, status_callback=self.status_signal.emit
                )
                if not result_filt or result_filt[0] is None:
                    return # Cancelled

                filt_res, filt_appx, filt_summ = result_filt

            self.finished_signal.emit(main_res, main_appx, main_summ, filt_res, filt_appx, filt_summ)
        except Exception as e: self.error_signal.emit(str(e))

class ShelfmarkLoaderThread(QThread):
    """
    Background thread to load metadata.
    OPTIMIZED: Delegates work to the efficient batch_fetch_shelfmarks manager method.
    """
    # Signal: current_count, total_count, current_sid
    progress_signal = pyqtSignal(int, int, str)
    finished_signal = pyqtSignal(bool)
    error_signal = pyqtSignal(str)

    def __init__(self, meta_mgr, sids):
        super().__init__()
        self.meta_mgr = meta_mgr
        self.sids = sids

    def run(self):
        try:
            total = len(self.sids)
            if total == 0:
                self.finished_signal.emit(True)
                return

            # הגדרת Callback שמקשר בין המנהל (Core) לבין ה-GUI (Signals)
            def update_gui(curr, tot, sid):
                self.progress_signal.emit(curr, tot, sid)

            # שימוש בפונקציה היעילה החדשה שכתבנו ב-MetadataManager
            # היא תטפל לבד בבדיקת CSV ובשימוש ב-20 ה-Threads לרשת
            self.meta_mgr.batch_fetch_shelfmarks(self.sids, progress_callback=update_gui)
            
            self.finished_signal.emit(True)
        except Exception as e:
            # במקרה של שגיאה קריטית, נסיים בכל זאת כדי לא לתקוע את הממשק
            print(f"Error in background loader: {e}")
            self.finished_signal.emit(False)

class AIWorkerThread(QThread):
    """Send a prompt to the AI manager in the background."""

    finished_signal = pyqtSignal(dict, str)
    def __init__(self, ai_mgr, prompt):
        super().__init__()
        self.ai_mgr = ai_mgr; self.prompt = prompt
    def run(self):
        data, err = self.ai_mgr.send_prompt(self.prompt)
        self.finished_signal.emit(data if data else {}, err if err else "")

class StartupThread(QThread):
    """Initialize heavy components in the background."""
    finished_signal = pyqtSignal(object, object, object, object, object)
    error_signal = pyqtSignal(str)

    def run(self):
        try:
            meta_mgr = MetadataManager()
            var_mgr = VariantManager()
            searcher = SearchEngine(meta_mgr, var_mgr)
            indexer = Indexer(meta_mgr)
            ai_mgr = AIManager()

            # Start loading heavy resources in background
            meta_mgr.start_background_loading()

            self.finished_signal.emit(meta_mgr, var_mgr, searcher, indexer, ai_mgr)
        except Exception as e:
            self.error_signal.emit(str(e))
