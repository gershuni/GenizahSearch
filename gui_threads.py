# gui_threads.py
from PyQt6.QtCore import QThread, pyqtSignal
from genizah_core import SearchEngine, Indexer, MetadataManager, VariantManager

class IndexerThread(QThread):
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

class CompositionThread(QThread):
    progress_signal = pyqtSignal(int, int)
    status_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(list, dict, dict)
    error_signal = pyqtSignal(str)

    def __init__(self, searcher, text, chunk, freq, mode, threshold=5):
        super().__init__()
        self.searcher = searcher; self.text = text; self.chunk = chunk
        self.freq = freq; self.mode = mode; self.threshold = threshold

    def run(self):
        try:
            self.status_signal.emit("Scanning chunks...")
            def cb(curr, total): self.progress_signal.emit(curr, total)
            items = self.searcher.search_composition_logic(
                self.text, self.chunk, self.freq, self.mode, progress_callback=cb
            )
            if not items:
                self.finished_signal.emit([], {}, {})
                return
            self.status_signal.emit(f"Found {len(items)} matches. Grouping...")
            main, appendix, summary = self.searcher.group_composition_results(items, self.threshold)
            self.finished_signal.emit(main, appendix, summary)
        except Exception as e: self.error_signal.emit(str(e))

class ShelfmarkLoaderThread(QThread):
    progress_signal = pyqtSignal(int, int, str)
    finished_signal = pyqtSignal()

    def __init__(self, meta_mgr, id_list):
        super().__init__()
        self.meta_mgr = meta_mgr; self.id_list = id_list

    def run(self):
        def cb(curr, total, sid): self.progress_signal.emit(curr, total, sid)
        self.meta_mgr.batch_fetch_shelfmarks(self.id_list, progress_callback=cb)
        self.finished_signal.emit()

class AIWorkerThread(QThread):
    finished_signal = pyqtSignal(dict, str)
    def __init__(self, ai_mgr, prompt):
        super().__init__()
        self.ai_mgr = ai_mgr; self.prompt = prompt
    def run(self):
        data, err = self.ai_mgr.send_prompt(self.prompt)
        self.finished_signal.emit(data if data else {}, err if err else "")