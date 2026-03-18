"""Worker threads used by the PyQt GUI for long-running operations."""

# gui_threads.py
import ctypes
import platform
import requests
from PyQt6.QtCore import QThread, pyqtSignal
from genizah_core import SearchEngine, Indexer, MetadataManager, VariantManager, get_logger

logger = get_logger(__name__)


def _prevent_sleep():
    """Prevent OS sleep while search is running (Windows only)."""
    if platform.system() == 'Windows':
        try:
            ES_CONTINUOUS = 0x80000000
            ES_SYSTEM_REQUIRED = 0x00000001
            ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
        except Exception:
            pass


def _allow_sleep():
    """Re-allow OS sleep after search completes (Windows only)."""
    if platform.system() == 'Windows':
        try:
            ES_CONTINUOUS = 0x80000000
            ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)
        except Exception:
            pass

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
    def __init__(self, searcher, query, mode, gap, exclude_words=None, responsa_options=None, restrict_sys_ids=None, text_position=None):
        super().__init__()
        self.searcher = searcher; self.query = query; self.mode = mode; self.gap = gap
        self.exclude_words = exclude_words
        self.responsa_options = responsa_options
        self.restrict_sys_ids = restrict_sys_ids
        self.text_position = text_position
        self.cancel_flag = False

    def run(self):
        _prevent_sleep()
        try:
            def cb(curr, total):
                if self.cancel_flag:
                    raise InterruptedError("Search cancelled by user")
                self.progress_signal.emit(curr, total)
            results = self.searcher.execute_search(
                self.query,
                self.mode,
                self.gap,
                progress_callback=cb,
                exclude_words=self.exclude_words,
                responsa_options=self.responsa_options,
                restrict_sys_ids=self.restrict_sys_ids,
                text_position=self.text_position,
            )

            self.results_signal.emit(results)
        except InterruptedError:
            # Emit empty list -- partial results not available from execute_search
            self.results_signal.emit([])
        except Exception as e: self.error_signal.emit(str(e))
        finally:
            _allow_sleep()

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
        _prevent_sleep()
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
        finally:
            _allow_sleep()

class CompositionThread(QThread):
    """Scan compositions in background to keep UI responsive."""

    progress_signal = pyqtSignal(int, int)
    status_signal = pyqtSignal(str)
    scan_finished_signal = pyqtSignal(object) # Changed from list to object to support dict return
    error_signal = pyqtSignal(str)

    def __init__(self, searcher, text, chunk, freq, mode, filter_text=None, threshold=5,
                 boundary_mode='full', boundary_delimiter='\n', boundary_boost=1.5,
                 min_boundary_matches=0, min_delimiter_distance=3, restrict_sys_ids=None):
        super().__init__()
        self.searcher = searcher
        self.text = text
        self.chunk = chunk
        self.freq = freq
        self.mode = mode
        self.filter_text = filter_text
        self.threshold = threshold
        self.cancel_flag = False
        self.restrict_sys_ids = restrict_sys_ids
        # Boundary search parameters
        self.boundary_mode = boundary_mode
        self.boundary_delimiter = boundary_delimiter
        self.boundary_boost = boundary_boost
        self.min_boundary_matches = min_boundary_matches
        self.min_delimiter_distance = min_delimiter_distance

    def run(self):
        _prevent_sleep()
        try:
            self.status_signal.emit("Scanning chunks...")
            def cb(curr, total):
                if self.cancel_flag:
                    raise InterruptedError("Search cancelled by user")
                self.progress_signal.emit(curr, total)

            # Returns dict {'main': [], 'filtered': []} or list [] (legacy safety)
            result = self.searcher.search_composition_logic(
                self.text, self.chunk, self.freq, self.mode,
                filter_text=self.filter_text, progress_callback=cb,
                boundary_mode=self.boundary_mode,
                boundary_delimiter=self.boundary_delimiter,
                boundary_boost=self.boundary_boost,
                min_boundary_matches=self.min_boundary_matches,
                min_delimiter_distance=self.min_delimiter_distance,
                restrict_sys_ids=self.restrict_sys_ids
            )
            self.scan_finished_signal.emit(result)
        except Exception as e: self.error_signal.emit(str(e))
        finally:
            _allow_sleep()

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
        self.cancel_flag = False
        # Boundary search parameters
        self.boundary_mode = boundary_mode
        self.boundary_delimiter = boundary_delimiter
        self.boundary_boost = boundary_boost
        self.min_boundary_matches = min_boundary_matches
        self.min_delimiter_distance = min_delimiter_distance

    def run(self):
        _prevent_sleep()
        try:
            self.status_signal.emit("Lab Mode: Broad-to-Narrow Scan...")

            # Callback handler that supports both (int, int) and (str)
            def cb(arg1, arg2=None):
                if self.cancel_flag:
                    raise InterruptedError("Search cancelled by user")
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
        finally:
            _allow_sleep()

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
            logger.error("Error in background loader: %s", e)
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

_field_translation_cache: dict = {}  # Module-level cache: {field_key: translated_text}


class TranslateTextThread(QThread):
    """Translate a single text field via Dicta API in the background."""

    finished_signal = pyqtSignal(str, str, str)  # field_key, original_text, translated_text

    def __init__(self, field_key: str, text: str, direction: str = 'en2he'):
        super().__init__()
        self.field_key = field_key
        self.text = text
        self.direction = direction

    def run(self):
        try:
            import os
            from shared.dicta_client import translate_text, load_few_shot_template, build_few_shot_prompt
            template_name = 'few_shot_en2he_scholarly.json' if self.direction == 'en2he' else 'few_shot_he2en_scholarly.json'
            data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
            template = load_few_shot_template(os.path.join(data_dir, template_name))
            prompt = build_few_shot_prompt(template, self.direction)
            result = translate_text(self.text, prompt, self.direction)
            if result:
                _field_translation_cache[self.field_key] = result
                self.finished_signal.emit(self.field_key, self.text, result)
            else:
                self.finished_signal.emit(self.field_key, self.text, '')
        except Exception as e:
            logger.warning("TranslateTextThread error for %s: %s", self.field_key, e)
            self.finished_signal.emit(self.field_key, self.text, '')


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
                        except OSError:
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


class DomainEnrichmentWorker(QThread):
    """Batch-fetch FJMS domain classifications for search results in background.

    Runs get_domains_for_sys_ids off the main thread so search results can
    display immediately while domain badges fill in asynchronously.
    """
    finished = pyqtSignal(dict)  # raw_domains: sys_id -> list of domain dicts

    def __init__(self, results: list, parent=None):
        super().__init__(parent)
        self.results = results

    def run(self):
        try:
            # Create a thread-local FjmsService connection -- the main-thread
            # singleton uses check_same_thread=True so cannot be reused here.
            from shared.fjms_service import FjmsService
            fjms = FjmsService(thread_safe=True)
            if not fjms.is_available():
                self.finished.emit({})
                return
            all_sys_ids = [
                r.get('display', {}).get('id')
                for r in self.results
                if r.get('display', {}).get('id')
            ]
            if not all_sys_ids:
                self.finished.emit({})
                return
            raw_domains = fjms.get_domains_for_sys_ids(all_sys_ids)
            self.finished.emit(raw_domains)
        except Exception:
            self.finished.emit({})


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
            logger.error("PGPBadgeWorker error: %s", e)
            self.finished.emit(set())


class PrintedBadgeWorker(QThread):
    """Batch check which sys_ids have FragmentMaterial=Printed for badge display."""
    finished = pyqtSignal(set)

    def __init__(self, sys_ids: list, parent=None):
        super().__init__(parent)
        self.sys_ids = sys_ids

    def run(self):
        try:
            from shared.fjms_service import FjmsService
            fjms = FjmsService(thread_safe=True)
            if not fjms.is_available():
                self.finished.emit(set())
                return
            result = fjms.get_printed_sys_ids(self.sys_ids)
            self.finished.emit(result)
        except Exception as e:
            logger.error("PrintedBadgeWorker error: %s", e)
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
            logger.error("PGPTagsWorker error: %s", e)
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
            logger.error("PGPTagSearchWorker error: %s", e)
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
                    logger.error("ReadingDeskWorker: error loading %s: %s", sys_id, e)
                    results.append((sys_id, [], {}))
            self.finished.emit(results)
        except Exception as e:
            self.error.emit(str(e))


class SidecarUpdateThread(QThread):
    """Check for sidecar data updates via GitHub release manifest.

    Reads a sidecar-versions.json asset from a dedicated GitHub release tag.
    Compares remote versions against locally installed sidecar versions.
    Emits update_available with list of available updates.
    """

    update_available = pyqtSignal(list)  # list of dicts: {name, current, available, size_mb, url}

    RELEASE_URL = "https://api.github.com/repos/gershuni/GenizahSearch/releases/tags/data-latest"

    def run(self):
        try:
            resp = requests.get(self.RELEASE_URL, timeout=5)
            if resp.status_code != 200:
                return  # Silent failure for auto-check

            data = resp.json()
            # Find sidecar-versions.json asset
            manifest_url = None
            for asset in data.get('assets', []):
                if asset['name'] == 'sidecar-versions.json':
                    manifest_url = asset['browser_download_url']
                    break
            if not manifest_url:
                return

            manifest_resp = requests.get(manifest_url, timeout=5)
            if manifest_resp.status_code != 200:
                return
            manifest = manifest_resp.json()

            # Compare local versions
            updates = []
            service_map = {
                'pgp.db': ('pgp_data', 'shared.document_service', 'get_pgp_service'),
                'fjms_enrichment.db': ('fist_data', 'shared.fjms_service', 'get_fjms_service'),
                'nli_crossref.db': ('nli_data', 'shared.nli_crossref_service', 'get_nli_crossref_service'),
            }

            for sidecar_name, remote_info in manifest.items():
                if sidecar_name not in service_map:
                    continue
                local_version = self._get_local_version(sidecar_name, service_map[sidecar_name])
                remote_version = remote_info.get('version', '')
                if self._is_newer(remote_version, local_version):
                    updates.append({
                        'name': sidecar_name,
                        'current': local_version or 'not installed',
                        'available': remote_version,
                        'size_mb': remote_info.get('size_mb', 0),
                        'url': remote_info.get('url', ''),
                        'subdir': service_map[sidecar_name][0],
                    })

            if updates:
                self.update_available.emit(updates)

        except Exception:
            pass  # Silent failure -- this is a background convenience check

    def _get_local_version(self, sidecar_name, service_info):
        """Get local sidecar version from the service singleton."""
        try:
            module_name, factory_name = service_info[1], service_info[2]
            import importlib
            mod = importlib.import_module(module_name)
            svc = getattr(mod, factory_name)()
            if svc.is_available():
                return svc.get_version()
        except Exception:
            pass
        return None

    @staticmethod
    def _is_newer(remote: str, local: str) -> bool:
        """Compare SemVer strings. Returns True if remote > local."""
        if not remote or not local:
            return bool(remote)  # If no local version, any remote is newer
        try:
            r = [int(x) for x in remote.split('.') if x.isdigit()]
            l = [int(x) for x in local.split('.') if x.isdigit()]
            max_len = max(len(r), len(l))
            r.extend([0] * (max_len - len(r)))
            l.extend([0] * (max_len - len(l)))
            return r > l
        except (ValueError, AttributeError):
            return False


class SidecarDownloadThread(QThread):
    """Download a sidecar database update from GitHub Releases."""

    progress_signal = pyqtSignal(int, int)  # downloaded_bytes, total_bytes
    finished_signal = pyqtSignal(bool, str, str)  # success, file_path_or_error, sidecar_name

    def __init__(self, url: str, target_path: str, sidecar_name: str):
        super().__init__()
        self.url = url
        self.target_path = target_path
        self.sidecar_name = sidecar_name
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    def run(self):
        try:
            # Validate URL is from GitHub
            if 'github.com/gershuni/GenizahSearch/' not in self.url:
                self.finished_signal.emit(False, "Invalid download URL", self.sidecar_name)
                return

            response = requests.get(self.url, stream=True, timeout=600, allow_redirects=True)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0

            import os
            import shutil
            # Download to temp file first, then move (atomic replacement)
            tmp_path = self.target_path + '.tmp'
            os.makedirs(os.path.dirname(self.target_path), exist_ok=True)

            with open(tmp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if self._cancelled:
                        f.close()
                        try:
                            os.remove(tmp_path)
                        except OSError:
                            pass
                        self.finished_signal.emit(False, "Download cancelled", self.sidecar_name)
                        return
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        self.progress_signal.emit(downloaded, total_size)

            # Move temp to final location
            shutil.move(tmp_path, self.target_path)
            self.finished_signal.emit(True, self.target_path, self.sidecar_name)

        except Exception as e:
            self.finished_signal.emit(False, str(e), self.sidecar_name)


class PuzzleImageLoaderThread(QThread):
    """Load and process a fragment image in the background via PuzzleImageService."""
    image_ready = pyqtSignal(str, bytes)   # (fl_id, rgba_png_bytes or jpeg_bytes)
    load_failed = pyqtSignal(str, str)     # (fl_id, error_message)

    def __init__(self, fl_id: str, threshold: float = 30.0, size: int = 800,
                 processed: bool = True, is_cul: bool = False, image_url: str = ''):
        super().__init__()
        self.fl_id = fl_id
        self.threshold = threshold
        self.size = size
        self.processed = processed
        self.is_cul = is_cul
        self.image_url = image_url

    def run(self):
        try:
            from shared.puzzle_image_service import resolve_fragment_image
            result = resolve_fragment_image(
                self.fl_id, size=self.size,
                threshold=self.threshold, processed=self.processed,
                is_cul=self.is_cul, image_url=self.image_url
            )
            emit_id = self.fl_id or self.image_url
            if result:
                self.image_ready.emit(emit_id, result)
            else:
                self.load_failed.emit(emit_id, "Image not available")
        except Exception as e:
            self.load_failed.emit(self.fl_id or self.image_url, str(e))


class PuzzleMetaLoaderThread(QThread):
    """Resolve images_nli (with fl_ids) for a sys_id in the background.

    FL IDs are NOT available from CSV cache -- they require a network fetch
    of the NLI MARC record and/or IIIF manifest. This thread wraps that
    async resolution so PuzzleCanvasWindow._on_add_shelfmark doesn't block.
    """
    meta_ready = pyqtSignal(str, str, list)   # (sys_id, shelfmark, images_nli_list)
    meta_failed = pyqtSignal(str, str)        # (sys_id, error_message)

    def __init__(self, meta_mgr, sys_id: str, shelfmark: str = ''):
        super().__init__()
        self.meta_mgr = meta_mgr
        self.sys_id = sys_id
        self.shelfmark = shelfmark

    def run(self):
        try:
            # enrich_metadata fetches NLI MARC + IIIF manifest, populating images_nli with fl_ids
            data = self.meta_mgr.enrich_metadata(self.sys_id)
            images_nli = data.get('images_nli', []) if data else []
            images_ext = data.get('images_ext', []) if data else []
            external_provider = (data or {}).get('external_provider', '')

            # Manchester external_provider: NLI FL IDs are catalog stubs (503) — use images_ext.
            # Cambridge external_provider: NLI FL IDs are real CUL images — use NLI.
            # Oxford: enrich_metadata populates images_ext but may leave external_provider empty.
            lib_code = self.meta_mgr.get_library_for_id(self.sys_id) or ''
            use_ext = (images_ext and external_provider
                       and external_provider != 'cambridge')
            # Oxford: use images_ext even without external_provider set
            if not use_ext and images_ext and lib_code == 'Oxford':
                external_provider = 'oxford'
                use_ext = True
            # Oxford fallback: if no images_ext, try shelfmark-based part lookup
            if lib_code == 'Oxford' and not images_ext:
                oxford_images = self._resolve_oxford_images()
                if oxford_images:
                    images_ext = oxford_images
                    external_provider = 'oxford'
                    use_ext = True

            if images_nli and not use_ext:
                self.meta_ready.emit(self.sys_id, self.shelfmark, images_nli)
                return
            # External library images (Manchester, Oxford, JTS, Cambridge)
            if images_ext:
                folio_list = []
                for i, img in enumerate(images_ext):
                    label = img.get('label', '') or str(i + 1)
                    folio_list.append({
                        'fl_id': '',
                        'label': label,
                        'image_url': img.get('url', ''),
                        'page_index': i,
                        'external_provider': external_provider,
                    })
                self.meta_ready.emit(self.sys_id, self.shelfmark, folio_list)
                return
            # Last resort: NLI fetch_nli_data directly
            nli_data = self.meta_mgr.fetch_nli_data(self.sys_id)
            if nli_data and nli_data.get('fl_ids'):
                images_nli = [{'fl_id': fid, 'label': f'FL{fid}', 'url': ''} for fid in nli_data['fl_ids']]
                self.meta_ready.emit(self.sys_id, self.shelfmark, images_nli)
            else:
                self.meta_failed.emit(self.sys_id, "No images found for this manuscript")
        except Exception as e:
            self.meta_failed.emit(self.sys_id, str(e))

    def _resolve_oxford_images(self):
        """Try to resolve Oxford part images via shelfmark-based lookup.

        Replicates the fallback logic from /api/oxford_image: when
        get_part_for_folio fails (no sys_id mapping), try parsing
        the shelfmark to find the Oxford part and its images.
        """
        try:
            codico = getattr(self.meta_mgr, 'codico_mgr', None)
            if not codico or not getattr(codico, '_loaded', False):
                return []

            # Try sys_id first
            part_id = codico.get_part_for_folio(self.sys_id)

            # Fallback: parse shelfmark
            if not part_id and self.shelfmark:
                part_id, is_part = codico.parse_part_identifier(self.shelfmark)
                if not is_part:
                    part_id = None

            if not part_id:
                return []

            images = codico.get_part_images(part_id)
            if not images:
                return []

            # Convert to images_ext format
            return [{
                'label': img.get('label', ''),
                'url': img.get('full_url', ''),
                'folio_num': img.get('folio_num')
            } for img in images]
        except Exception:
            return []
