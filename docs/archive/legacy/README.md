# Archived legacy launchers (moved 2026-09-18)

> **Superseded.** Kept for history, not for use.

- `start_servers.sh` launched `python -m backend.main` on port 8000 next to the web app. The
  standalone backend process was removed in early 2026 (decision 0002); the web app runs as the
  `genizah-web` systemd unit in production and as `python -m web.main` locally. The companion
  `START_SERVERS_README.md` in the parent directory describes the old two-process setup.
- `web_pilot.py` was an early NiceGUI search-and-viewer spike with no importer, invoker or test;
  the real web app is `web/main.py`.

Where things live today: [docs/architecture/OVERVIEW.md](../../architecture/OVERVIEW.md).
