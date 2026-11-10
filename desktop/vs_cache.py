"""Visual similarity desktop cache and fetch threads extracted from genizah_app.py (v7.9 decomposition)."""

import os
import json

from PyQt6.QtCore import QThread, pyqtSignal


# -- Visual Similarity Desktop Cache + Fetch Thread -------------------------

class DesktopVSCache:
    """Local SQLite cache for visual similarity suggestions fetched from server.
    Tracks server version for staleness detection."""

    def __init__(self):
        import sqlite3 as _sqlite3
        cache_dir = os.path.join(
            os.environ.get('LOCALAPPDATA', os.path.expanduser('~')),
            'GenizahSearchPro', 'data'
        )
        os.makedirs(cache_dir, exist_ok=True)
        self._db_path = os.path.join(cache_dir, 'vs_cache.db')
        self._conn = _sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.execute('''CREATE TABLE IF NOT EXISTS cached_suggestions (
            sys_id TEXT NOT NULL, partner_sys_id TEXT NOT NULL, svm_score REAL NOT NULL,
            rank INTEGER NOT NULL, shelfmark TEXT DEFAULT '', library_code TEXT DEFAULT '',
            domain TEXT DEFAULT '', fetched_at TEXT NOT NULL,
            PRIMARY KEY (sys_id, partner_sys_id)
        )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS cache_manifest (
            sys_id TEXT PRIMARY KEY, fetched_at TEXT NOT NULL, partner_count INTEGER NOT NULL
        )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS cache_metadata (
            key TEXT PRIMARY KEY, value TEXT
        )''')
        self._conn.commit()

    def get_server_version(self) -> str:
        row = self._conn.execute("SELECT value FROM cache_metadata WHERE key = 'server_version'").fetchone()
        return row[0] if row else ''

    def set_server_version(self, version: str):
        current = self.get_server_version()
        if current and current != version:
            self._conn.execute('DELETE FROM cached_suggestions')
            self._conn.execute('DELETE FROM cache_manifest')
        self._conn.execute("INSERT OR REPLACE INTO cache_metadata VALUES ('server_version', ?)", (version,))
        self._conn.commit()

    def check_and_update_version(self, server_url: str):
        """Check server version and invalidate cache if stale. Called on app startup."""
        try:
            import urllib.request
            url = f'{server_url}/api/visual_suggestions/version'
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                version = data.get('version', '')
                if version:
                    self.set_server_version(version)
        except Exception:
            pass  # Offline -- keep existing cache

    def get_cached(self, sys_id: str):
        row = self._conn.execute('SELECT 1 FROM cache_manifest WHERE sys_id = ?', (sys_id,)).fetchone()
        if not row:
            return None
        rows = self._conn.execute(
            'SELECT partner_sys_id, svm_score, rank, shelfmark, library_code, domain '
            'FROM cached_suggestions WHERE sys_id = ? ORDER BY rank', (sys_id,)
        ).fetchall()
        return [{'alma_id': r[0], 'svm_score': r[1], 'rank': r[2],
                 'shelfmark': r[3], 'library_code': r[4], 'domain': r[5]} for r in rows]

    def store(self, sys_id: str, suggestions: list):
        from datetime import datetime
        now = datetime.utcnow().isoformat()
        self._conn.execute('DELETE FROM cached_suggestions WHERE sys_id = ?', (sys_id,))
        for s in suggestions:
            self._conn.execute(
                'INSERT OR REPLACE INTO cached_suggestions VALUES (?,?,?,?,?,?,?,?)',
                (sys_id, s.get('alma_id', ''), s.get('svm_score', 0), s.get('rank', 0),
                 s.get('shelfmark', ''), s.get('library_code', ''), s.get('domain', ''), now)
            )
        self._conn.execute(
            'INSERT OR REPLACE INTO cache_manifest VALUES (?,?,?)',
            (sys_id, now, len(suggestions))
        )
        self._conn.commit()

    def has_cached(self, sys_id: str) -> bool:
        return self._conn.execute('SELECT 1 FROM cache_manifest WHERE sys_id = ?', (sys_id,)).fetchone() is not None

    def get_cached_partners(self, sys_ids: list, mode: str = 'union') -> set:
        partner_sets = []
        for sid in sys_ids:
            rows = self._conn.execute(
                'SELECT partner_sys_id FROM cached_suggestions WHERE sys_id = ?', (sid,)
            ).fetchall()
            partner_sets.append({r[0] for r in rows})
        if not partner_sets:
            return set()
        if mode == 'intersection':
            return set.intersection(*partner_sets) if partner_sets else set()
        return set.union(*partner_sets)


class VSFetchThread(QThread):
    """Fetch visual similarity suggestions from server for a single manuscript."""
    finished = pyqtSignal(list)
    error = pyqtSignal(str)

    def __init__(self, sys_id: str, server_url: str, parent=None):
        super().__init__(parent)
        self.sys_id = sys_id
        self.server_url = server_url

    def run(self):
        try:
            import urllib.request
            url = f'{self.server_url}/api/visual_suggestions/{self.sys_id}?limit=200'
            with urllib.request.urlopen(url, timeout=15) as resp:
                data = json.loads(resp.read().decode())
            self.finished.emit(data)
        except Exception as e:
            self.error.emit(str(e))


class VSDownloadThread(QThread):
    """Download full visual_similarity.db with checksum, disk-space, and corruption checks."""
    progress = pyqtSignal(int, int)  # bytes_downloaded, total_bytes
    finished = pyqtSignal(str)  # local file path
    error = pyqtSignal(str)

    def __init__(self, server_url: str, dest_dir: str, parent=None):
        super().__init__(parent)
        self.server_url = server_url
        self.dest_dir = dest_dir

    def run(self):
        import urllib.request, hashlib, shutil  # noqa: E401
        import sqlite3 as _sqlite3
        url = f'{self.server_url}/api/visual_similarity_db'
        try:
            # Step 1: HEAD request to get size and checksum
            req = urllib.request.Request(url, method='HEAD')
            with urllib.request.urlopen(req, timeout=10) as resp:
                total_size = int(resp.headers.get('Content-Length', 0))
                expected_checksum = resp.headers.get('X-Checksum-SHA256', '')

            # Step 2: Disk-space pre-check
            dest_path = os.path.join(self.dest_dir, 'fist_data', 'visual_similarity.db')
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            free_space = shutil.disk_usage(self.dest_dir).free
            required = total_size * 2
            if free_space < required:
                self.error.emit(
                    f'Insufficient disk space. Need {required // (1024*1024)} MB, '
                    f'have {free_space // (1024*1024)} MB free.'
                )
                return

            # Step 3: Download to temp file with progress
            tmp_path = dest_path + '.downloading'
            sha256 = hashlib.sha256()
            downloaded = 0
            with urllib.request.urlopen(url, timeout=300) as resp:
                with open(tmp_path, 'wb') as f:
                    while True:
                        chunk = resp.read(65536)
                        if not chunk:
                            break
                        f.write(chunk)
                        sha256.update(chunk)
                        downloaded += len(chunk)
                        self.progress.emit(downloaded, total_size)

            # Step 4: Checksum verification
            actual_checksum = sha256.hexdigest()
            if expected_checksum and actual_checksum != expected_checksum:
                os.remove(tmp_path)
                self.error.emit(
                    f'Checksum mismatch. Expected {expected_checksum[:12]}..., '
                    f'got {actual_checksum[:12]}... Download may be corrupted.'
                )
                return

            # Step 5: SQLite integrity check (corruption detection)
            try:
                test_conn = _sqlite3.connect(tmp_path)
                result = test_conn.execute('PRAGMA integrity_check').fetchone()
                test_conn.close()
                if result[0] != 'ok':
                    os.remove(tmp_path)
                    self.error.emit(f'Downloaded file failed integrity check: {result[0]}')
                    return
            except Exception as e:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                self.error.emit(f'Downloaded file is not a valid SQLite database: {e}')
                return

            # Step 6: Atomic rename
            if os.path.exists(dest_path):
                os.remove(dest_path)
            os.rename(tmp_path, dest_path)

            self.finished.emit(dest_path)
        except Exception as e:
            tmp_path = os.path.join(self.dest_dir, 'fist_data', 'visual_similarity.db.downloading')
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass  # Download failed; emit None so caller knows
            self.error.emit(str(e))
