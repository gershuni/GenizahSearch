"""Image loader thread for desktop manuscript viewers."""

import os
import re

import requests
from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtGui import QImage

from genizah_core import Config, MetadataManager, get_logger

logger = get_logger(__name__)


class ImageLoaderThread(QThread):
    """
    Smart Image Loader:
    1. Checks Local Disk Cache first.
    2. If missing, Downloads from IIIF (with Rosetta fallback).
    3. Saves successful downloads to Disk Cache.
    """

    image_loaded = pyqtSignal(QImage)
    load_failed = pyqtSignal()

    def __init__(self, url):
        super().__init__()
        self.url = url
        self._cancelled = False

        # Ensure cache directory exists
        if not os.path.exists(Config.IMAGE_CACHE_DIR):
            try:
                os.makedirs(Config.IMAGE_CACHE_DIR)
            except Exception as e:
                logger.warning(
                    "Could not create image cache directory at %s: %s; image caching disabled for this session.",
                    Config.IMAGE_CACHE_DIR,
                    e,
                )

    def cancel(self):
        self._cancelled = True

    def run(self):
        if not self.url:
            self.load_failed.emit()
            return

        # 1. Determine cache filename: FL ID for NLI, URL hash for external
        fl_match = re.search(r'FL(\d+)', self.url)
        local_path = None

        if fl_match:
            fl_id = fl_match.group(1)
            # v2 cache: high resolution (2000px). Old v1 cache was 600px.
            local_path = os.path.join(Config.IMAGE_CACHE_DIR, f"FL{fl_id}_v2.jpg")
        else:
            # Cache external images (Cambridge, Manchester, Oxford, JTS) by URL hash
            import hashlib
            url_hash = hashlib.md5(self.url.encode('utf-8')).hexdigest()[:16]
            local_path = os.path.join(Config.IMAGE_CACHE_DIR, f"ext_{url_hash}.jpg")

        # --- CHECK LOCAL CACHE ---
        if local_path and os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            img = QImage(local_path)
            if not img.isNull():
                self.image_loaded.emit(img)
                return
            else:
                # Corrupt file? Delete it so we re-download
                try:
                    os.remove(local_path)
                except Exception as e:
                    logger.warning("Failed to remove corrupt cache file %s: %s", local_path, e)

        # 2. Download from Network (if not in cache)
        headers = dict(Config.HTTP_HEADERS)
        headers["Referer"] = "https://www.nli.org.il/"

        data = None

        # Attempt A: Original URL
        data = self._download_bytes(self.url, headers)

        # Attempt B: Fallback to Rosetta stream if IIIF failed (full-res TIFF)
        if data is None and fl_match and not self._cancelled:
            fl_digits = fl_match.group(1)
            logger.info("IIIF failed for FL%s. Trying Rosetta stream fallback...", fl_digits)
            fallback_url = MetadataManager.get_rosetta_fallback_url(fl_digits)
            if fallback_url:
                data = self._download_bytes(fallback_url, headers)

        # Attempt C: Rosetta thumbnail if stream also failed (e.g. 401 for some libraries)
        if data is None and fl_match and not self._cancelled:
            fl_digits = fl_match.group(1)
            logger.info("Rosetta stream failed for FL%s. Trying thumbnail fallback...", fl_digits)
            thumb_url = f"https://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_func=thumbnail&dps_pid=FL{fl_digits}"
            data = self._download_bytes(thumb_url, headers)

        # 3. Process Result
        if data:
            img = QImage.fromData(data)
            if not img.isNull():
                self.image_loaded.emit(img)

                # --- SAVE TO LOCAL CACHE (always as JPEG for compact storage) ---
                if local_path and not self._cancelled:
                    try:
                        img.save(local_path, "JPEG", 85)
                        logger.debug("Saved image cache to %s", local_path)
                    except Exception as e:
                        logger.warning(
                            "Failed to write image cache for %s: %s; future loads will re-download.",
                            local_path,
                            e,
                        )
            else:
                self.load_failed.emit()
        else:
            self.load_failed.emit()

    def _download_bytes(self, target_url, headers):
        """Helper to download bytes safely."""
        try:
            # Rosetta stream returns large TIFF files (7-15MB) — allow longer timeout
            timeout = 30 if 'rosetta.nli.org.il' in target_url else 10
            resp = requests.get(target_url, headers=headers, timeout=timeout, stream=True, verify=False)
            if self._cancelled: return None
            if resp.status_code == 200:
                return resp.content
            return None
        except Exception as e:
            logger.warning("Image download failed for %s: %s", target_url, e)
            return None
