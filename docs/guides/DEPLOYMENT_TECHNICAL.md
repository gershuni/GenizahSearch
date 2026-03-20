# GenizahSearch Technical Deployment Guide

> Last updated: 2026-03-13
> For: Developers, System Administrators, AI Assistants

---

## Architecture Overview (February 2026)

GenizahSearch uses a simplified architecture with Supabase as the backend and SQLite sidecars for reference data:

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLOUDFLARE                               │
│                   (DNS, SSL, DDoS Protection)                    │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                           NGINX                                  │
│                      (Reverse Proxy)                             │
│                   Port 80/443 → Port 8081                        │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      WEB APPLICATION (NiceGUI on port 8081)             │
│                                                                         │
│  ┌──────────────┐  ┌───────────────────┐  ┌─────────────────────────┐  │
│  │ Search/Browse │  │ SQLite Sidecars   │  │    User Data            │  │
│  │              │  │ (read-only)       │  │                         │  │
│  │ Tantivy Index│  │                   │  │  Supabase (Cloud)       │  │
│  │  (Local)     │  │ fjms_enrichment.db│  │  - Authentication       │  │
│  │              │  │  Domains, Joins,  │  │  - Lists & Items        │  │
│  │ - tantivy_db │  │  Catalog, Biblio  │  │  - Corrections          │  │
│  │ - lab_index  │  │                   │  │  - Comments             │  │
│  │              │  │ nli_crossref.db   │  │  - Discoveries          │  │
│  │              │  │  Images, Metadata,│  │  - Joins                │  │
│  │              │  │  LUNA, DPUL IDs   │  │                         │  │
│  │              │  │                   │  │                         │  │
│  │              │  │ pgp.db            │  │                         │  │
│  │              │  │  Documents, Sources│  │                         │  │
│  │              │  │  Footnotes, Frags │  │                         │  │
│  └──────────────┘  └───────────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

### Key Changes from Previous Architecture

| Component | Before (Pre-Jan 2026) | Now |
|-----------|----------------------|-----|
| Backend API | FastAPI on port 8000 | **Removed** - Supabase replaces it |
| Database | PostgreSQL (self-hosted) | **Supabase** (cloud) |
| Authentication | Custom JWT | **Supabase Auth** |
| Services | 2 systemd services | **1 service** (genizah-web only) |

---

## Server Details

| Component | Value |
|-----------|-------|
| Provider | AWS EC2 |
| Instance | Ubuntu 24.04.3 LTS |
| IP | 44.247.206.248 |
| SSH | `ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com` |
| Region | us-west-2 |

### Domain & DNS

| Component | Value |
|-----------|-------|
| Domain | genizahsearch.com |
| Registrar | Cloudflare |
| DNS | Cloudflare (Proxied) |
| SSL | Let's Encrypt + Cloudflare |

### Application Stack

| Component | Technology | Port |
|-----------|------------|------|
| Web Application | NiceGUI | 8081 |
| Web Server | Nginx | 80, 443 |
| Search Engine | Tantivy | - (embedded) |
| Reference Data | SQLite sidecars (FJMS + NLI + PGP) | - (embedded, read-only) |
| User Database | Supabase | Cloud |

---

## Directory Structure

```
/home/ubuntu/GenizahSearch/
├── web/                    # NiceGUI web application
│   ├── main.py            # Entry point
│   ├── pages/             # Page components
│   ├── components/        # UI components
│   └── supabase_client.py # Supabase integration
├── shared/                # Shared service layer (both apps)
│   ├── document_service.py    # PGP data access
│   ├── corrections_service.py # Corrections data access
│   ├── fjms_service.py        # FJMS domain/join/catalog queries
│   ├── nli_crossref_service.py # NLI crossref/image/metadata queries
│   ├── translation_service.py # Dicta translation lookups (libraries, PGP, FJMS)
│   ├── translation_qc.py     # Translation quality checks
│   ├── dicta_client.py        # Dicta Translation API client
│   ├── session_persistence.py # Session state save/restore
│   ├── supabase_provider.py   # Supabase client factory
│   └── reading_desk_model.py  # Virtual Reading Desk data model
├── Genizah_Index/         # Search indexes
│   ├── tantivy_db/        # Main search index (3.3GB)
│   ├── lab_index/         # Parallels index (3.0GB)
│   ├── browse_map.pkl     # Browse navigation data
│   ├── metadata_cache.pkl # Metadata cache
│   └── lab/               # Lab configuration
├── fist_data/             # FJMS sidecar (NOT in git)
│   └── fjms_enrichment.db # SQLite sidecar v5.0.0 (~941MB)
├── nli_data/              # NLI crossref sidecar (NOT in git)
│   └── nli_crossref.db   # SQLite sidecar v1.2.0 (248MB)
├── pgp_data/              # PGP data + sidecar (NOT in git)
│   ├── pgp.db             # SQLite sidecar (~165MB, includes translations)
│   ├── documents.csv      # 35K PGP document records (export source)
│   ├── fragments.csv      # 36K fragment links (export source)
│   ├── footnotes.csv      # 23K footnotes (export source)
│   └── transcriptions_linked.csv # Linked transcription sources
├── libraries.csv          # Master manuscript metadata (~217K records)
├── libraries_translations.db # Dicta translations sidecar (76MB, NOT in git)
├── genizah_core.py        # Core search logic
├── venv/                  # Python virtual environment
├── Transcriptions.txt     # Source transcription data (1.4GB)
├── .env                   # Environment variables
├── deploy.sh              # Deployment script
└── build_index.py         # Index building script
```

---

## Configuration

### Environment Variables (`.env`)

```bash
# Supabase Configuration (REQUIRED)
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

# Application Settings
GENIZAH_PORT=8081
NICEGUI_RELOAD=false
NICEGUI_SHOW=false
ENVIRONMENT=production
WEB_PUZZLE_ENABLED=false

# Optional - PostHog analytics
POSTHOG_API_KEY=phc_xxxxx
```

`WEB_PUZZLE_ENABLED` is an emergency kill switch for the web puzzle UI and route. Leave it set to `false` until the puzzle image pipeline is considered production-ready again.

### Systemd Service (`/etc/systemd/system/genizah-web.service`)

```ini
[Unit]
Description=Genizah Web Interface
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/GenizahSearch
EnvironmentFile=/home/ubuntu/GenizahSearch/.env
Environment=GENIZAH_PORT=8081
Environment=NICEGUI_RELOAD=false
Environment=NICEGUI_SHOW=false
ExecStart=/home/ubuntu/GenizahSearch/venv/bin/python -m web.main
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Nginx Configuration (`/etc/nginx/sites-available/genizah`)

```nginx
server {
    listen 80;
    server_name genizahsearch.com www.genizahsearch.com;
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl;
    server_name genizahsearch.com www.genizahsearch.com;

    ssl_certificate /etc/letsencrypt/live/genizahsearch.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/genizahsearch.com/privkey.pem;

    # All traffic goes to NiceGUI (no separate /api route needed)
    location / {
        proxy_pass http://127.0.0.1:8081;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket support (required for NiceGUI)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
    }
}
```

---

## Browser Extension (GenizahSearch Image Helper)

The web puzzle requires a browser extension to fetch manuscript images from NLI, because NLI blocks requests from datacenter IPs (including AWS and Cloudflare). The extension fetches images through the user's own browser (residential/institutional IP), then sends them to the server for background removal and caching.

### Architecture

```
User's Browser (extension) ──fetch──► iiif.nli.org.il
         │                                (user's IP, accepted by NLI)
         │ raw image bytes
         ▼
Server /api/puzzle_process ──► background removal ──► disk cache
         │                                              │
         └─── processed PNG ◄──────────────────────────┘
                                  (future loads served from cache)
```

### Extension Files

| File | Purpose |
|------|---------|
| `extension/manifest.json` | Chrome MV3 manifest, NLI host permissions |
| `extension/manifest.firefox.json` | Firefox MV3 manifest (gecko settings, `background.scripts`) |
| `extension/background.js` | Service worker, fetches NLI images as binary |
| `extension/content_script.js` | Page↔background bridge, extension detection |
| `extension/icons/` | Store icons (16/48/128px) |
| `extension/store/` | Chrome Web Store listing assets |
| `extension/build.py` | Builds Chrome and Firefox ZIP packages into `extension/dist/` |

### Store Listings

| Store | Status | URL |
|-------|--------|-----|
| Chrome Web Store | Live | https://chromewebstore.google.com/detail/ngohnlbbdifmccjdnjhcpmilpdpjmkmc |
| Firefox AMO | Submitted 2026-03-18, pending review | (pending approval) |

- **Privacy policy**: `https://genizahsearch.com/privacy-extension`
- **Update process**: Bump version in both manifests, run `python extension/build.py`, upload ZIPs to respective developer dashboards

### Building the Extension ZIPs

```bash
python extension/build.py
# Outputs:
#   extension/dist/genizah-extension-chrome-v{version}.zip
#   extension/dist/genizah-extension-firefox-v{version}.zip
```

### Development Testing

**Chrome:**
1. Chrome → `chrome://extensions` → Developer mode → Load unpacked
2. Select the `extension/` directory
3. Set `WEB_PUZZLE_ENABLED=true` in `.env`
4. Start the web app: `python -m web.main`
5. Visit `localhost:8081/puzzle` — green "Extension active" indicator should appear

**Firefox:**
1. Firefox → `about:debugging#/runtime/this-firefox` → Load Temporary Add-on
2. Select `extension/manifest.firefox.json`
3. Same steps 3-5 as Chrome above

### Security

- **HMAC upload tokens**: Server issues signed tokens on cache miss (`X-Puzzle-Upload-Token` header). Uploads to `/api/puzzle_process` and `/api/puzzle_upload_derivative` require valid tokens (5-min expiry, fl_id-bound).
- **URL validation**: Extension only fetches from `iiif.nli.org.il`
- **Origin validation**: Content script only responds to messages from `genizahsearch.com` and `localhost`
- **Rate limiting**: 60 requests/min/IP on upload endpoints
- **`PUZZLE_UPLOAD_SECRET`**: Set in `.env` for stable tokens across restarts. Auto-generated if unset (tokens invalidated on restart).

### Server Cache

Processed puzzle images are cached on the server disk at:
```
/home/ubuntu/.local/share/GenizahSearchPro/cache/puzzle/
```

Cache key format: `{fl_id}_{size}_{threshold}[_cul]_{PROCESSING_VERSION}.png`

Once cached, images serve all users instantly without the extension. The cache grows from:
- Extension users (via `/api/puzzle_process`)
- Desktop users (future: via `/api/puzzle_upload_derivative`)

---

## Service Management

### Systemd Commands

```bash
# Check status
sudo systemctl status genizah-web

# Start/Stop/Restart
sudo systemctl start genizah-web
sudo systemctl stop genizah-web
sudo systemctl restart genizah-web

# View logs
sudo journalctl -u genizah-web -f          # Real-time
sudo journalctl -u genizah-web -n 100      # Last 100 lines
sudo journalctl -u genizah-web --since today

# Nginx logs
sudo tail -f /var/log/nginx/access.log
sudo tail -f /var/log/nginx/error.log
```

---

## Deployment Procedures

### Standard Code Update

```bash
cd /home/ubuntu/GenizahSearch
./deploy.sh
```

The `deploy.sh` script:
1. Pulls latest code from `master-main` branch
2. Installs any new dependencies
3. Restarts the web service

Or manually:
```bash
cd /home/ubuntu/GenizahSearch
git fetch origin
git reset --hard origin/master-main
source venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart genizah-web
```

### Toggle the Puzzle Feature

To keep the puzzle hidden in production:

```bash
sudo systemctl edit genizah-web
```

Add or update:

```ini
[Service]
Environment=WEB_PUZZLE_ENABLED=false
```

Then reload and restart:

```bash
sudo systemctl daemon-reload
sudo systemctl restart genizah-web
```

### Rebuild Search Indexes

```bash
cd /home/ubuntu/GenizahSearch
source venv/bin/activate

# Build both indexes (~1 hour)
python build_index.py

# Build specific index
python build_index.py main    # Main search index only
python build_index.py lab     # Lab/parallels index only

# Restart service after rebuild
sudo systemctl restart genizah-web
```

---

## Supabase Management

### Dashboard Access

- URL: https://supabase.com/dashboard
- Project: GenizahSearch

### Database Tables

| Table | Description |
|-------|-------------|
| `profiles` | User profiles (extends auth.users) |
| `user_lists` | Personal manuscript lists |
| `list_items` | Items in each list |
| `corrections` | Transcription corrections |
| `comments` | User comments on manuscripts |
| `discoveries` | Community discoveries/findings |
| `joins` | Fragment join relationships |

### Common Operations

```sql
-- View all users
SELECT id, email, full_name, role FROM profiles;

-- View user's lists
SELECT * FROM user_lists WHERE user_id = 'uuid-here';

-- View pending corrections
SELECT * FROM corrections WHERE status = 'pending';
```

### Backups

Supabase handles automatic daily backups. To restore:
1. Dashboard → Settings → Database → Backups
2. Select backup point → Restore

---

## Monitoring & Health Checks

### Quick Health Check

```bash
# Check service
curl -s http://localhost:8081 -o /dev/null -w '%{http_code}\n'  # Should be 200

# Check external access
curl -s https://genizahsearch.com -o /dev/null -w '%{http_code}\n'  # Should be 200
```

### Server Resources

```bash
# Disk usage
df -h /
du -sh /home/ubuntu/GenizahSearch/Genizah_Index/*

# Memory
free -h

# Processes
ps aux | grep python
```

---

## Troubleshooting

### Service Won't Start

1. Check logs: `sudo journalctl -u genizah-web -n 50`
2. Verify environment: `cat /home/ubuntu/GenizahSearch/.env`
3. Test manually:
   ```bash
   cd /home/ubuntu/GenizahSearch
   source venv/bin/activate
   python -m web.main
   ```

### 502 Bad Gateway

1. Check service: `sudo systemctl status genizah-web`
2. Check nginx: `sudo nginx -t`
3. Restart: `sudo systemctl restart genizah-web && sudo systemctl reload nginx`

### User Data Not Syncing

1. Check Supabase status: https://status.supabase.com
2. Verify `.env` has correct `SUPABASE_URL` and `SUPABASE_ANON_KEY`
3. Check browser console for errors
4. Test Supabase connection:
   ```python
   from web.supabase_client import get_client
   client = get_client()
   print(client.table('profiles').select('*').limit(1).execute())
   ```

### Search Not Working

1. Check index exists: `ls -la /home/ubuntu/GenizahSearch/Genizah_Index/tantivy_db/`
2. Check permissions: `ls -la /home/ubuntu/GenizahSearch/Genizah_Index/`
3. Restart service: `sudo systemctl restart genizah-web`
4. If still broken, rebuild index (see above)

### WebSocket Connection Issues (Connection Lost)

If users report frequent "Connection Lost" or yellow/red status indicators:

**1. Check server resources:**
```bash
# Check memory
free -h

# Check CPU
top -bn1 | head -20

# Check open connections
ss -s
netstat -an | grep :8081 | wc -l
```

**2. Increase Nginx connection limits** (in `/etc/nginx/nginx.conf`):
```nginx
events {
    worker_connections 4096;  # Increase from default 768
}

http {
    # Add keepalive for upstream
    upstream nicegui {
        server 127.0.0.1:8081;
        keepalive 64;
    }
}
```

**3. Optimize Nginx proxy settings** (in `/etc/nginx/sites-available/genizah`):
```nginx
location / {
    proxy_pass http://127.0.0.1:8081;
    # ... existing headers ...

    # WebSocket stability improvements
    proxy_read_timeout 86400;      # 24 hours (keep long connections alive)
    proxy_send_timeout 86400;
    proxy_connect_timeout 60;
    proxy_buffering off;           # Disable buffering for WebSocket

    # Connection reuse
    proxy_http_version 1.1;
    proxy_set_header Connection "";  # Allow connection reuse
}
```

**4. Application-level settings** (in `.env`):
```bash
# Increase reconnect timeout for clients (seconds)
NICEGUI_RECONNECT_TIMEOUT=30
```

**5. Monitor WebSocket connections:**
```bash
# Count active WebSocket connections
sudo ss -tnp | grep ':8081' | wc -l

# Watch connection count in real-time
watch -n 1 "sudo ss -tnp | grep ':8081' | wc -l"
```

**6. If under heavy load, consider:**
- Enabling Cloudflare's "Under Attack" mode temporarily
- Implementing rate limiting in Cloudflare WAF
- Scaling up the EC2 instance

---

## SSL Certificate

- Provider: Let's Encrypt
- Auto-renewal: Enabled via certbot
- Location: `/etc/letsencrypt/live/genizahsearch.com/`

```bash
# Manual renewal
sudo certbot renew
sudo systemctl reload nginx

# Check expiry
sudo certbot certificates
```

---

## Security Notes

- SSH: Key-based authentication only
- Supabase: Row Level Security (RLS) enabled
- External traffic: All through Cloudflare proxy
- SSL/TLS: Encryption enabled
- Database: No direct access (Supabase handles it)

### Cloudflare Configuration

GenizahSearch uses Cloudflare for DNS, SSL termination, and DDoS protection.

**Dashboard:** https://dash.cloudflare.com

#### Proxy Settings
- Proxy status: **Proxied** (orange cloud) for genizahsearch.com
- SSL/TLS mode: **Full (strict)**
- Always Use HTTPS: **Enabled**
- Minimum TLS Version: **TLS 1.2**

#### Rate Limiting (Optional)

Rate limiting can be configured in Cloudflare Dashboard → Security → WAF → Rate limiting rules.

**Recommended settings for API protection:**

| Rule | Path | Rate | Action |
|------|------|------|--------|
| Auth endpoints | `/auth/*` | 10 req/min | Challenge |
| API calls | `/api/*` | 100 req/min | Challenge |
| General | `*` | 1000 req/min | Block |

**To create a rate limiting rule:**
1. Go to Security → WAF → Rate limiting rules
2. Click "Create rule"
3. Set matching criteria (URI path, HTTP method)
4. Set rate threshold (requests per period)
5. Choose action (Block, Challenge, Log)

**Note:** Basic DDoS protection is automatic with Cloudflare proxy enabled.
No explicit rate limiting rules are currently configured - Cloudflare's
default DDoS protection handles most abuse cases.

#### Caching

Cloudflare caching is configured to:
- Cache static assets (CSS, JS, images)
- Bypass cache for dynamic content
- Respect `Cache-Control` headers from origin

**Page Rules (if needed):**
- `*genizahsearch.com/static/*` → Cache Level: Standard
- `*genizahsearch.com/api/*` → Cache Level: Bypass

---

## Cockpit Server Management

Web-based UI for server management:

- URL: https://admin.genizahsearch.com
- User: `ubuntu`
- Auth: Set password via `sudo passwd ubuntu`

---

## Data Sources & Sidecar Databases

### Static Data Files

| File | Source | Size | In Git? |
|------|--------|------|---------|
| Transcriptions.txt (V0.8) | [Zenodo](https://zenodo.org/records/17734473) | 1.4 GB | No |
| Genizah_OLD.txt (V0.7) | Optional, historical | ~1 GB | No |
| libraries.csv | Master manuscript metadata | ~15 MB | Yes |

### Search Index Sizes

| Index | Size | Purpose |
|-------|------|---------|
| tantivy_db | 3.3 GB | Main manuscript search |
| lab_index | 3.0 GB | Parallels/lab features |

### SQLite Sidecar Databases (v5.8.0+)

All sidecar databases are **NOT in git** (listed in `.gitignore`). They must be uploaded manually to the server and regenerated when source data changes.

| Database | Directory | Size | Version | Contents |
|----------|-----------|------|---------|----------|
| `fjms_enrichment.db` | `fist_data/` | ~941 MB | v5.0.0 | FJMS domains (390K), joins (48K), catalog (685K, 37 cols), bibliography (542K), catalog_refs (64K), genizah_persons (2,286), genizah_titles (775), code_values (3,440), translations |
| `nli_crossref.db` | `nli_data/` | 248 MB | v1.2.0 | NLI crossref images (815K), Cambridge manifests (141K), Manchester LUNA (28K), JTS DPUL (453) |
| `pgp.db` | `pgp_data/` | ~165 MB | - | PGP documents (35K), sources (9K), footnotes (23K), fragments (36K), pgp_translations (35K) |
| `libraries_translations.db` | project root | 76 MB | - | Dicta translations for library titles (~185K records, Hebrew↔English) |

#### Initial Upload to Server

```bash
# From local machine:
scp fist_data/fjms_enrichment.db ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com:/home/ubuntu/GenizahSearch/fist_data/
scp nli_data/nli_crossref.db ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com:/home/ubuntu/GenizahSearch/nli_data/
scp pgp_data/pgp.db ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com:/home/ubuntu/GenizahSearch/pgp_data/
scp libraries_translations.db ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com:/home/ubuntu/GenizahSearch/

# On server, create directories if needed:
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com
mkdir -p /home/ubuntu/GenizahSearch/fist_data /home/ubuntu/GenizahSearch/nli_data /home/ubuntu/GenizahSearch/pgp_data
```

#### Regenerating Sidecar Databases

Only needed when source data is updated (new FIST.db version, new NLI crossref CSV).

```bash
cd /home/ubuntu/GenizahSearch
source venv/bin/activate

# FJMS sidecar (requires FIST_DB_BACKUP/FIST.db):
python scripts/export_fist_enrichment.py

# NLI crossref sidecar (requires nli_crossreference.csv + cambridge_genizah.json):
python scripts/import_nli_crossref.py

# Manchester LUNA IDs (fetches from API, ~30 min):
python scripts/import_manchester_luna.py

# JTS/Princeton DPUL (fetches from API, uses checkpoints):
python scripts/import_jts_dpul.py

# PGP sidecar (requires pgp_data/*.csv source files):
python scripts/export_pgp_sidecar.py

# Restart service after regeneration:
sudo systemctl restart genizah-web
```

#### When to Update

| Database | Update Trigger | Frequency |
|----------|---------------|-----------|
| `fjms_enrichment.db` | New FIST.db version from FJMS project | Rare (quarterly?) |
| `nli_crossref.db` | New NLI crossreference CSV or Cambridge JSON | Rare (when NLI provides) |
| Manchester/JTS tables | New manuscripts added to LUNA or DPUL | Rare (can re-run import scripts) |
| `pgp.db` | New PGP data exported from Princeton Geniza Project | Rare (when PGP releases new data) |
| `libraries_translations.db` | New Dicta translations batch or corrections | Rare (after translation runs) |

**Note:** All sidecar databases are read-only at runtime. The web app opens them in `?mode=ro` URI mode. No write operations occur during normal operation.

**FJMS Performance Indexes (v6.5.4):** The export script creates 6 performance indexes at build time for domain and catalog queries used during search enrichment. These indexes must be present in the sidecar — they cannot be created at runtime (read-only connection). If you rebuild `fjms_enrichment.db`, verify indexes exist: `idx_domains_parent`, `idx_domains_group`, `idx_domains_domain_alma`, `idx_domains_parent_alma`, `idx_catalog_author`, `idx_catalog_title`.

### PGP Data Maintenance

PGP data is updated regularly on GitHub at [princeton-geniza-project](https://github.com/Princeton-CDH/geniza). When updated:

```bash
cd /home/ubuntu/GenizahSearch
source venv/bin/activate

# 1. Download latest PGP data exports to pgp_data/
#    (documents.csv, fragments.csv, footnotes.csv, transcriptions from pgp-text repo)

# 2. Import documents, fragments, footnotes into Supabase:
python scripts/import_pgp_documents.py
python scripts/import_pgp_full.py

# 3. Import transcription sources:
python scripts/import_document_sources.py

# 4. Import sections (parses pgp-text HTML):
python scripts/import_pgp_sections.py

# 5. Restart service:
sudo systemctl restart genizah-web

# 6. Regenerate pgp.db sidecar (for desktop and web offline access):
python scripts/export_pgp_sidecar.py

# 7. Upload pgp.db to server:
scp pgp_data/pgp.db ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com:/home/ubuntu/GenizahSearch/pgp_data/
```

**PGP data files** (in `pgp_data/`): Not in git. Must be downloaded from PGP GitHub and placed on server before running import scripts.

---

## Server Maintenance

### Session Cleanup (Critical!)

NiceGUI stores session data in `.nicegui/` directory. Sessions include cached images users view, so they can grow large (10-20MB each). Without cleanup, this directory can consume 10GB+ and cause memory issues.

**Automated cleanup (via cron):**
```bash
# View current cron jobs
crontab -l

# Should show:
# 0 3 * * * find /home/ubuntu/GenizahSearch/.nicegui/ -type f -mtime +7 -delete
```

**Manual cleanup:**
```bash
# Check current size
du -sh /home/ubuntu/GenizahSearch/.nicegui/
ls -la /home/ubuntu/GenizahSearch/.nicegui/ | wc -l

# Delete sessions older than 7 days
find /home/ubuntu/GenizahSearch/.nicegui/ -type f -mtime +7 -delete

# Or delete sessions older than 1 day (more aggressive)
find /home/ubuntu/GenizahSearch/.nicegui/ -type f -mtime +1 -delete

# Nuclear option - delete all (users will need to reconnect)
rm -rf /home/ubuntu/GenizahSearch/.nicegui/*
```

### Memory Monitoring

The web application typically uses 2-4GB of RAM. If it exceeds 8GB, session cleanup is needed.

```bash
# Quick memory check
free -h

# Check web.main memory usage specifically
ps aux | grep web.main

# Detailed view with htop
htop
```

**Warning signs:**
- Memory > 8GB → Clean sessions
- Memory > 12GB → Clean sessions + restart service

### Service Management

```bash
# Status
sudo systemctl status genizah-web

# Restart (clears memory)
sudo systemctl restart genizah-web

# Stop/Start
sudo systemctl stop genizah-web
sudo systemctl start genizah-web

# View logs (recent)
sudo journalctl -u genizah-web -n 100

# View logs (follow live)
sudo journalctl -u genizah-web -f
```

### Log Monitoring

**Common log messages to ignore:**
- `wp-admin/setup-config.php not found` - WordPress scanner bots
- `/.env not found` - Security scanner bots
- `RuntimeError: The parent slot...` - User disconnected (handled gracefully)

**Log messages requiring attention:**
- `MemoryError` - Clean sessions, restart service
- `Connection refused` to Supabase - Check Supabase status
- Repeated `502 Bad Gateway` in nginx - Service crashed, restart needed
- `502` only on `/api/` routes - Check nginx has NO separate `location /api/` block (was removed March 2026; the old block proxied to port 8000 which no longer exists)

### Backup

Daily backup runs at 3 AM via cron:
```bash
# Check backup status
cat /home/ubuntu/backups/backup.log

# Manual backup
/home/ubuntu/GenizahSearch/backup.sh
```

### Health Check Commands

Run these periodically or when issues are reported:

```bash
# 1. Memory status
free -h

# 2. Disk usage
df -h

# 3. Service status
sudo systemctl status genizah-web

# 4. Connection count
netstat -an | grep :8081 | wc -l

# 5. Session storage size
du -sh /home/ubuntu/GenizahSearch/.nicegui/

# 6. Recent errors
sudo journalctl -u genizah-web -p err -n 20
```

### Environment Variables

Key environment variables in `/home/ubuntu/GenizahSearch/.env`:

| Variable | Description | Default |
|----------|-------------|---------|
| `SUPABASE_URL` | Supabase project URL | Required |
| `SUPABASE_ANON_KEY` | Supabase anonymous key | Required |
| `POSTHOG_API_KEY` | PostHog analytics key | Optional |
| `NICEGUI_RECONNECT_TIMEOUT` | WebSocket reconnect timeout (seconds) | 30 |
| `NICEGUI_RELOAD` | Hot reload (dev only) | false |
| `NICEGUI_SHOW` | Open browser on start | false |

---

## Resources

- GitHub: https://github.com/gershuni/GenizahSearch
- Website: https://genizahsearch.com
- Server Management: https://admin.genizahsearch.com
- Supabase Dashboard: https://supabase.com/dashboard
- Cloudflare Dashboard: https://dash.cloudflare.com
