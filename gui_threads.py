"""Worker threads used by the PyQt GUI for long-running operations."""

# gui_threads.py
import requests
from PyQt6.QtCore import QThread, pyqtSignal
from genizah_core import SearchEngine, Indexer, MetadataManager, VariantManager

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
    def __init__(self, searcher, query, mode, gap, exclude_words=None, responsa_options=None):
        super().__init__()
        self.searcher = searcher; self.query = query; self.mode = mode; self.gap = gap
        self.exclude_words = exclude_words
        self.responsa_options = responsa_options

    def run(self):
        try:
            def cb(curr, total): self.progress_signal.emit(curr, total)
            results = self.searcher.execute_search(
                self.query,
                self.mode,
                self.gap,
                progress_callback=cb,
                exclude_words=self.exclude_words,
                responsa_options=self.responsa_options
            )
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

    def __init__(self, searcher, text, chunk, freq, mode, filter_text=None, threshold=5,
                 boundary_mode='full', boundary_delimiter='\n', boundary_boost=1.5,
                 min_boundary_matches=0, min_delimiter_distance=3):
        super().__init__()
        self.searcher = searcher
        self.text = text
        self.chunk = chunk
        self.freq = freq
        self.mode = mode
        self.filter_text = filter_text
        self.threshold = threshold
        # Boundary search parameters
        self.boundary_mode = boundary_mode
        self.boundary_delimiter = boundary_delimiter
        self.boundary_boost = boundary_boost
        self.min_boundary_matches = min_boundary_matches
        self.min_delimiter_distance = min_delimiter_distance

    def run(self):
        try:
            self.status_signal.emit("Scanning chunks...")
            def cb(curr, total): self.progress_signal.emit(curr, total)

            # Returns dict {'main': [], 'filtered': []} or list [] (legacy safety)
            result = self.searcher.search_composition_logic(
                self.text, self.chunk, self.freq, self.mode,
                filter_text=self.filter_text, progress_callback=cb,
                boundary_mode=self.boundary_mode,
                boundary_delimiter=self.boundary_delimiter,
                boundary_boost=self.boundary_boost,
                min_boundary_matches=self.min_boundary_matches,
                min_delimiter_distance=self.min_delimiter_distance
            )
            self.scan_finished_signal.emit(result)
        except Exception as e: self.error_signal.emit(str(e))

class LabCompositionThread(QThread):
    """Execute Lab Composition Search (Broad-to-Narrow)."""

    progress_signal = pyqtSignal(int, int)
    status_signal = pyqtSignal(str)
    scan_finished_signal = pyqtSignal(object)
    error_signal = pyqtSignal(str)

    def __init__(self, lab_engine, text, mode, chunk_size=None, excluded_ids=None, filter_text=None,
                 deep_scan=False, scan_limit=50000, boundary_mode='full', boundary_delimiter='\n',
                 boundary_boost=1.5, min_boundary_matches=0, min_delimiter_distance=3):
        super().__init__()
        self.lab_engine = lab_engine
        self.text = text
        self.chunk_size = chunk_size
        self.mode = mode
        self.excluded_ids = excluded_ids
        self.filter_text = filter_text
        self.deep_scan = deep_scan
        self.scan_limit = scan_limit
        # Boundary search parameters
        self.boundary_mode = boundary_mode
        self.boundary_delimiter = boundary_delimiter
        self.boundary_boost = boundary_boost
        self.min_boundary_matches = min_boundary_matches
        self.min_delimiter_distance = min_delimiter_distance

    def run(self):
        try:
            self.status_signal.emit("Lab Mode: Broad-to-Narrow Scan...")

            # Callback handler that supports both (int, int) and (str)
            def cb(arg1, arg2=None):
                if isinstance(arg1, str):
                    self.status_signal.emit(arg1)
                elif isinstance(arg1, int) and arg2 is not None:
                    self.progress_signal.emit(arg1, arg2)

            result = self.lab_engine.lab_composition_search(
                self.text,
                mode=self.mode,
                progress_callback=cb,
                chunk_size=self.chunk_size,
                excluded_ids=self.excluded_ids,
                filter_text=self.filter_text,
                deep_scan=self.deep_scan,
                scan_limit=self.scan_limit,
                boundary_mode=self.boundary_mode,
                boundary_delimiter=self.boundary_delimiter,
                boundary_boost=self.boundary_boost,
                min_boundary_matches=self.min_boundary_matches,
                min_delimiter_distance=self.min_delimiter_distance
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

    def request_cancel(self):
        self.requestInterruption()

    def run(self):
        try:
            total = len(self.sids)
            if total == 0:
                self.finished_signal.emit(False) # Not cancelled (Success)
                return

            def update_gui(curr, tot, sid):
                self.progress_signal.emit(curr, tot, sid)

            def check_cancel():
                return self.isInterruptionRequested()

            self.meta_mgr.batch_fetch_shelfmarks(self.sids, progress_callback=update_gui, check_cancel=check_cancel)
            
            # If interrupted, emit True (Cancelled), else False (Success)
            if self.isInterruptionRequested():
                self.finished_signal.emit(True)
            else:
                self.finished_signal.emit(False)
        except Exception as e:
            print(f"Error in background loader: {e}")
            self.finished_signal.emit(False)

class StartupThread(QThread):
    """Initialize heavy components in the background."""
    finished_signal = pyqtSignal(object, object, object, object)
    error_signal = pyqtSignal(str)

    def run(self):
        try:
            meta_mgr = MetadataManager()
            var_mgr = VariantManager()
            searcher = SearchEngine(meta_mgr, var_mgr)
            indexer = Indexer(meta_mgr)

            # Start loading heavy resources in background
            meta_mgr.start_background_loading()

            self.finished_signal.emit(meta_mgr, var_mgr, searcher, indexer)
        except Exception as e:
            self.error_signal.emit(str(e))


class EnrichMetadataThread(QThread):
    """Fetch extended metadata (IIIF/MARC) in the background."""
    finished_signal = pyqtSignal(str, dict)

    def __init__(self, meta_mgr, system_id):
        super().__init__()
        self.meta_mgr = meta_mgr
        self.system_id = system_id

    def run(self):
        try:
            # This method (in genizah_core.py) handles network errors gracefully
            data = self.meta_mgr.enrich_metadata(self.system_id)
            self.finished_signal.emit(self.system_id, data)
        except Exception:
            # If something unexpected happens, just emit empty to avoid hanging
            self.finished_signal.emit(self.system_id, {})


class ExternalResourceThread(QThread):
    """Fetch external IIIF resources (e.g. Cambridge) in background."""
    finished_signal = pyqtSignal(dict)

    def __init__(self, meta_mgr, url):
        super().__init__()
        self.meta_mgr = meta_mgr
        self.url = url

    def run(self):
        try:
            data = self.meta_mgr.fetch_external_iiif_data(self.url)
            self.finished_signal.emit(data)
        except Exception:
            self.finished_signal.emit({})

class UpdateCheckerThread(QThread):
    """Check for updates on GitHub."""

    # found, version, html_url, installer_url, is_manual_check
    finished_signal = pyqtSignal(bool, str, str, str, bool)
    error_signal = pyqtSignal(str, bool)

    def __init__(self, current_version, is_manual=False):
        super().__init__()
        self.current_version = current_version
        self.is_manual = is_manual

    def run(self):
        try:
            url = "https://api.github.com/repos/gershuni/GenizahSearch/releases/latest"
            resp = requests.get(url, timeout=5)

            if resp.status_code == 200:
                data = resp.json()
                tag = data.get('tag_name', '').strip()
                html_url = data.get('html_url', '')

                # Get installer (.exe) asset URL for direct download
                assets = data.get('assets', [])
                installer_url = ''
                for asset in assets:
                    asset_name = asset.get('name', '').lower()
                    # Look for setup/installer exe (not the main app exe)
                    if asset_name.endswith('.exe') and ('setup' in asset_name or 'install' in asset_name):
                        installer_url = asset.get('browser_download_url', '')
                        break

                # Simple SemVer comparison (stripping 'v' prefix)
                curr_v = [int(x) for x in self.current_version.replace('v','').split('.') if x.isdigit()]
                remote_v = [int(x) for x in tag.replace('v','').split('.') if x.isdigit()]

                # Normalize version lists to same length (pad with zeros)
                max_len = max(len(curr_v), len(remote_v))
                curr_v.extend([0] * (max_len - len(curr_v)))
                remote_v.extend([0] * (max_len - len(remote_v)))

                if remote_v > curr_v:
                    self.finished_signal.emit(True, tag, html_url, installer_url, self.is_manual)
                else:
                    self.finished_signal.emit(False, tag, html_url, installer_url, self.is_manual)
            else:
                self.error_signal.emit(f"GitHub API Error: {resp.status_code}", self.is_manual)

        except Exception as e:
            self.error_signal.emit(str(e), self.is_manual)


class UpdateDownloaderThread(QThread):
    """Download update installer from GitHub Releases with progress reporting."""

    progress_signal = pyqtSignal(int, int)  # downloaded_bytes, total_bytes
    finished_signal = pyqtSignal(bool, str)  # success, file_path_or_error

    def __init__(self, download_url: str, target_path: str):
        super().__init__()
        self.download_url = download_url
        self.target_path = target_path
        self._cancelled = False

    def cancel(self):
        """Request cancellation of the download."""
        self._cancelled = True

    def run(self):
        try:
            # Validate URL is from GitHub
            if not self.download_url.startswith('https://github.com/gershuni/GenizahSearch/'):
                self.finished_signal.emit(False, "Invalid download URL: not from official repository")
                return

            # Stream download with progress
            response = requests.get(
                self.download_url,
                stream=True,
                timeout=600,  # 10 minute timeout for large files
                allow_redirects=True
            )
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            chunk_size = 1024 * 1024  # 1MB chunks

            with open(self.target_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if self._cancelled:
                        f.close()
                        # Clean up partial file
                        try:
                            import os
                            os.remove(self.target_path)
                        except:
                            pass
                        self.finished_signal.emit(False, "Download cancelled")
                        return

                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        self.progress_signal.emit(downloaded, total_size)

            # Verify the downloaded file exists and has content
            import os
            if not os.path.exists(self.target_path) or os.path.getsize(self.target_path) == 0:
                self.finished_signal.emit(False, "Downloaded file is empty or missing")
                return

            self.finished_signal.emit(True, self.target_path)

        except requests.exceptions.Timeout:
            self.finished_signal.emit(False, "Download timed out. Please try again.")
        except requests.exceptions.ConnectionError:
            self.finished_signal.emit(False, "Network connection error. Check your internet connection.")
        except requests.exceptions.HTTPError as e:
            self.finished_signal.emit(False, f"Download failed: HTTP {e.response.status_code}")
        except IOError as e:
            self.finished_signal.emit(False, f"Disk error: {str(e)}")
        except Exception as e:
            self.finished_signal.emit(False, f"Download error: {str(e)}")


class PGPSourceWorker(QThread):
    """Fetch PGP edition/translation sources for a fragment in the background.

    Calls shared/document_service functions (lazy-imported) to get all sources
    and document metadata for a given sys_id and page number.
    """
    finished_signal = pyqtSignal(str, list, dict)  # sys_id, page_sources, pgp_doc_dict
    error_signal = pyqtSignal(str, str)  # sys_id, error_message

    def __init__(self, sys_id: str, page_num: int = 1):
        super().__init__()
        self.sys_id = sys_id
        self.page_num = page_num

    def run(self):
        try:
            # Lazy import to avoid issues if Supabase is not configured
            from shared.document_service import (
                get_all_sources_for_fragment,
                get_document_for_fragment,
                get_section_for_page
            )

            # Get all sources (editions + translations) for this fragment
            all_sources = get_all_sources_for_fragment(self.sys_id)

            # Filter sources by page (recto/verso)
            current_page_info = 'recto' if self.page_num == 1 else 'verso'
            page_sources = []
            for source in all_sources:
                source_page = source.get('page_info')
                is_translation = 'Translation' in (source.get('doc_relation') or '')

                # Include translations regardless of page_info,
                # but extract correct section if content has recto/verso markers
                if is_translation:
                    if not source_page:
                        content = source.get('content')
                        if content:
                            source['content'] = get_section_for_page(content, self.page_num, source.get('sections'))
                    page_sources.append(source)
                    continue

                # For editions: include if page matches or page_info is not set
                if source_page == current_page_info or not source_page:
                    # If no page_info, extract the correct recto/verso section
                    if not source_page:
                        content = source.get('content')
                        if content:
                            source['content'] = get_section_for_page(content, self.page_num, source.get('sections'))
                    page_sources.append(source)

            # Get document metadata
            pgp_doc = get_document_for_fragment(self.sys_id, self.page_num)
            pgp_doc_dict = pgp_doc if pgp_doc else {}

            self.finished_signal.emit(self.sys_id, page_sources, pgp_doc_dict)
        except Exception as e:
            self.error_signal.emit(self.sys_id, str(e))


class PGPBadgeWorker(QThread):
    """Batch check which sys_ids have PGP transcriptions for badge display."""
    finished = pyqtSignal(set)

    def __init__(self, sys_ids: list, parent=None):
        super().__init__(parent)
        self.sys_ids = sys_ids

    def run(self):
        try:
            from shared.document_service import get_sys_ids_with_transcriptions
            result = get_sys_ids_with_transcriptions(self.sys_ids)
            self.finished.emit(result)
        except Exception as e:
            print(f"PGPBadgeWorker error: {e}")
            self.finished.emit(set())


class PGPTagsWorker(QThread):
    """Fetch all distinct PGP tags for dropdown population."""
    finished = pyqtSignal(list)

    def __init__(self, parent=None):
        super().__init__(parent)

    def run(self):
        try:
            from shared.document_service import get_all_distinct_tags
            result = get_all_distinct_tags()
            self.finished.emit(result)
        except Exception as e:
            print(f"PGPTagsWorker error: {e}")
            self.finished.emit([])


class PGPTagSearchWorker(QThread):
    """Search for fragments by PGP tag."""
    finished = pyqtSignal(str, list)

    def __init__(self, tag: str, parent=None):
        super().__init__(parent)
        self.tag = tag

    def run(self):
        try:
            from shared.document_service import get_fragments_by_tag
            result = get_fragments_by_tag(self.tag)
            self.finished.emit(self.tag, result)
        except Exception as e:
            print(f"PGPTagSearchWorker error: {e}")
            self.finished.emit(self.tag, [])


class ReadingDeskWorker(QThread):
    """Batch load PGP sources for multiple fragments for the reading desk.

    Fetches all PGP sources and document metadata for a list of sys_ids
    in a background thread, preventing UI freeze when entering reading desk mode.
    """
    finished = pyqtSignal(list)  # list of (sys_id, sources, pgp_doc)
    error = pyqtSignal(str)

    def __init__(self, sys_ids: list, parent=None):
        super().__init__(parent)
        self.sys_ids = sys_ids

    def run(self):
        try:
            from shared.document_service import (
                get_all_sources_for_fragment,
                get_document_for_fragment,
            )
            results = []
            for sys_id in self.sys_ids:
                try:
                    sources = get_all_sources_for_fragment(sys_id) or []
                    pgp_doc = get_document_for_fragment(sys_id)
                    results.append((sys_id, sources, pgp_doc or {}))
                except Exception as e:
                    print(f"ReadingDeskWorker: error loading {sys_id}: {e}")
                    results.append((sys_id, [], {}))
            self.finished.emit(results)
        except Exception as e:
            self.error.emit(str(e))
