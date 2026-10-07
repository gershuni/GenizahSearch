# GenizahSearch Technical Deployment Guide

> Last updated: 2026-10-07
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
│   ├── pgp.db             # SQLite sidecar (~156MB; pgp_translations is WITHHELD, see below)
│   ├── documents.csv      # 35K PGP document records (export source)
│   ├── fragments.csv      # 36K fragment links (export source)
│   ├── footnotes.csv      # 23K footnotes (export source)
│   └── transcriptions_linked.csv # Linked transcription sources
├── libraries.csv          # Master manuscript metadata (~217K records)
├── libraries_translations.db # Dicta translations sidecar (76MB, NOT in git)
├── atlas_data/            # Connections Atlas beta asset (NOT in git; OUTSIDE web/static/)
│   ├── manifest.json      # Mutable pointer -> the content-hashed asset (no-cache + ETag)
│   ├── atlas-v1-<hash>.bin    # Offline-baked, content-hashed binary payload
│   └── atlas-v1-<hash>.bin.br # Brotli-precompressed representation (optional)
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

# Web session storage secret (REQUIRED; the app refuses to start without it)
GENIZAH_STORAGE_SECRET=<random, 32+ characters>

# Application Settings
GENIZAH_PORT=8081
NICEGUI_RELOAD=false
NICEGUI_SHOW=false
ENVIRONMENT=production
WEB_PUZZLE_ENABLED=false

# Optional - PostHog analytics
POSTHOG_API_KEY=phc_xxxxx
```

`GENIZAH_STORAGE_SECRET` must be in this file **before** deploying code that reads it (from
2026-09-25): without it `python -m web.main` exits at startup and `Restart=always` restarts it
every 5 s. Generate it on the server with
`python3 -c "import secrets; print(secrets.token_urlsafe(32))"`. Setting or changing it signs
every web user out once; never commit it or paste it into a log.

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

As on the server (copied read-only 2026-10-07), plus the Cloudflare realip `include` described
below. The SEO-bot snippet and its `map` (`conf.d/seo_bot_block_map.conf`) exist only on the
server; the realip snippet is generated from this repo.

```nginx
server {
    # Cloudflare edge -> real visitor in $remote_addr (see "Real visitor addresses behind Cloudflare").
    include /etc/nginx/snippets/genizah_cloudflare_realip.conf;

    server_name genizahsearch.com www.genizahsearch.com;
    # Block aggressive crawlers that don't execute JS
    if ($http_user_agent ~* "meta-externalagent") {
        return 403;
    }

    # robots.txt must stay reachable by ALL crawlers (served by the app, web/api.py::robots_txt).
    location = /robots.txt {
        proxy_pass http://127.0.0.1:8081;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Web frontend (NiceGUI) - requires WebSocket support
    location / {
        # SEO-tool crawler 403 (2026-07-08); UA map in conf.d/seo_bot_block_map.conf.
        include /etc/nginx/snippets/genizah_seo_bot_block.conf;

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

    listen 443 ssl; # managed by Certbot
    ssl_certificate /etc/letsencrypt/live/genizahsearch.com/fullchain.pem; # managed by Certbot
    ssl_certificate_key /etc/letsencrypt/live/genizahsearch.com/privkey.pem; # managed by Certbot
    include /etc/letsencrypt/options-ssl-nginx.conf; # managed by Certbot
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem; # managed by Certbot
}
server {
    include /etc/nginx/snippets/genizah_cloudflare_realip.conf;

    if ($host = www.genizahsearch.com) {
        return 301 https://$host$request_uri;
    } # managed by Certbot
    if ($host = genizahsearch.com) {
        return 301 https://$host$request_uri;
    } # managed by Certbot

    listen 80;
    server_name genizahsearch.com www.genizahsearch.com;
    return 404; # managed by Certbot
}
```

Despite its comment, `/robots.txt` is not reachable by every crawler: the server-level
`meta-externalagent` `if` runs before any `location` is chosen, so that crawler gets 403 there too.

### Real visitor addresses behind Cloudflare

**Why.** DNS is proxied through Cloudflare, so every request reaches nginx from a Cloudflare
edge. Without the realip module `$remote_addr` is that edge: the access log, `X-Real-IP` and the
address nginx appends to `X-Forwarded-For` all name Cloudflare. On 2026-10-07 the site was
overloaded after a restart, and the access log showed only `172.70.153.x`: the source had to be
found in the Cloudflare dashboard. The app's per-IP limiters and the
PostHog IP hash keyed on the edge too (sweep item L8), so visitors sharing an edge shared a bucket.

**The change.** `scripts/genizah_cloudflare_realip.conf` lists every Cloudflare range as
`set_real_ip_from` (fetched 2026-10-07 from `https://www.cloudflare.com/ips-v4` and `/ips-v6`)
and sets `real_ip_header CF-Connecting-IP`. It is included at the top of both `server` blocks.
For a connection whose peer is in those ranges, nginx replaces `$remote_addr` with the
`CF-Connecting-IP` value Cloudflare set; from any other peer the header is ignored.

**What the app then receives** (no app change was needed):

| Request | `X-Forwarded-For` reaching the app | Address the app keys on |
|---|---|---|
| Visitor V via Cloudflare, before | `V, <edge>` | `<edge>` |
| Visitor V via Cloudflare, after | `V, V` | `V` |
| V sends its own `X-Forwarded-For: F` and `CF-Connecting-IP: F` | `F, V, V` | `V` |
| Client A connects to the origin directly, forging `CF-Connecting-IP: V` | `<A's own XFF>, A` | `A` |

uvicorn's `ProxyHeadersMiddleware` (on by default under `ui.run`, trusting only `127.0.0.1`,
i.e. nginx) sets `request.client` to the right-most `X-Forwarded-For` entry -- the one nginx
appended -- and `web/api_hardening._resolve_rate_limit_key` and the puzzle limiter in
`web/api.py` read `request.client.host`. Two things must stay true. `FORWARDED_ALLOW_IPS` stays
unset -- not empty, and never `*` (with `*` uvicorn takes the LEFT-most entry, which the visitor
writes; empty trusts no host, so every request keeps nginx's `127.0.0.1`). And no module in
`web/` reads `CF-Connecting-IP` or `X-Real-IP` itself: nginx passes a client-sent
`CF-Connecting-IP` through untouched from any peer, and although it always overwrites
`X-Real-IP`, a client that reached the app on port 8081 directly could send either.
`tests/test_api_hardening_behind_cloudflare.py` runs the real uvicorn middleware on header values
written out by hand from nginx's documented behaviour (it runs neither nginx nor the puzzle
limiter itself), shows what `*`, an empty value or an extra `API_TRUSTED_PROXIES` entry would
break, and fails if `web/` reads those headers or sets `FORWARDED_ALLOW_IPS`. It cannot see the
server's environment: step 0 below checks that.

**Limits.** The origin listens on `0.0.0.0:443` and `:80` with no Cloudflare-only restriction in
the nginx config, and its address is in this public repo (Server Details above); whether the AWS
security group admits only Cloudflare is not visible from the config. The snippet does not stop a
direct request -- it only guarantees that such a request is logged and limited under its true
address. The app itself listens on `0.0.0.0:8081` (NiceGUI's default when `ui.run` gets no
`host`), which would skip nginx and Cloudflare altogether; checked 2026-10-07, port 8081 is not
reachable from outside (the security group drops it), and binding the app to `127.0.0.1` would
make that hold whatever the security group says. Restricting 443/80 to Cloudflare's ranges
(security group) or enabling Cloudflare
Authenticated Origin Pulls is the separate step that closes the origin. A range Cloudflare adds
before the next refresh shows up as an edge address again (degraded, never spoofable). Per-IP
limits now apply per visitor: one heavy visitor gets the whole per-IP allowance to itself, and
other visitors behind the same edge are no longer throttled with it.

**Refreshing the ranges.** `scripts/refresh_cloudflare_realip.py` (stdlib only, run as root)
fetches both lists from `https://www.cloudflare.com/` only (a redirect elsewhere is refused
before it is followed) and refuses a body shorter than its `Content-Length`; an empty, malformed,
non-public or over-broad (`< /8` IPv4, `< /16` IPv6) list; and, unless `--allow-large-change`, a
list that keeps fewer than half of the installed networks of either family (`--dry-run` applies
this too). A cut-off list it can still miss: a body with no length to check that keeps enough
networks -- at least half of each family against an installed snippet, or any count when nothing
is installed yet (hence the diff in step 2). A new write marks the reload pending before the
snippet changes, so a run killed before its reload finishes is finished by the next. It keeps the
previous snippet as `<dest>.prev`, runs `nginx -t` before reloading and puts the previous snippet
back if the test fails. A failed reload leaves `<dest>.reload-pending`, and the next run retries
`nginx -t` and the reload even if nothing changed. While one run holds `<dest>.lock`, a second is
refused. Unchanged ranges with no reload pending mean no write and no reload. `--dry-run` prints
and writes nothing; `--no-reload` writes and tests only (reloading is then yours). Exit codes: 0
ok/unchanged, 1 refused or locked (nothing written), 2 `nginx -t` failed (nothing reloaded; a new
snippet was put back), 3 reload failed (the next run retries it).

**Install (owner, once).** Code first (`./deploy.sh master-main`), then on the server. First the
checks, which change nothing nginx reads:

```bash
# 0. uvicorn must keep its default (expect NO output from both):
grep -n FORWARDED_ALLOW_IPS /home/ubuntu/GenizahSearch/.env
systemctl show genizah-web -p Environment | grep -o 'FORWARDED_ALLOW_IPS=[^ ]*'
ls -l /etc/nginx/sites-enabled/          # confirm the live site file is .../sites-available/genizah

# 1. A root-owned copy of the refresh script (cron must not run a file the deploy user can rewrite):
sudo install -o root -g root -m 0755 /home/ubuntu/GenizahSearch/scripts/refresh_cloudflare_realip.py \
    /usr/local/sbin/genizah-refresh-cloudflare-realip

# 2. Fetch + validate, write nothing; compare with the reviewed copy (expect only the
#    Fetched / Last-Modified comment lines to differ, unless Cloudflare changed a range).
#    A REFUSED message means: stop here.
sudo /usr/local/sbin/genizah-refresh-cloudflare-realip --dry-run \
    | diff - /home/ubuntu/GenizahSearch/scripts/genizah_cloudflare_realip.conf
```

Then, only after reading that diff, paste this block whole. It stops at the first failure, puts
the site file back if step 5 or `nginx -t` fails, and is safe to paste again after fixing the
cause: the first backup is never overwritten and the `include` is never added twice. (If only the
final reload fails, the tested file stays and nginx keeps its running config; rerun the reload.)

```bash
(
set -eu
SITE=/etc/nginx/sites-available/genizah
BAK=/root/genizah.nginx.before-realip
INC='include /etc/nginx/snippets/genizah_cloudflare_realip.conf;'
undo() { sudo cp -a "$BAK" "$SITE"; echo "STOPPED at $1: $SITE restored from $BAK; nginx NOT reloaded" >&2; exit 1; }

# 3. Back up the site file OUTSIDE sites-enabled/ (nginx loads every file there) -- once.
sudo test -e "$BAK" || sudo cp -a "$SITE" "$BAK"

# 4. Write the snippet (not yet included anywhere). A refusal stops the block here.
sudo /usr/local/sbin/genizah-refresh-cloudflare-realip --no-reload

# 5. Include it as the first line of BOTH server blocks (unless an earlier paste already did).
sudo grep -qF "$INC" "$SITE" || sudo sed -i "s|^server {\$|server {\n    $INC|" "$SITE" || undo 'step 5'
[ "$(sudo grep -cF "$INC" "$SITE")" = 2 ] || undo 'step 5: the include is not there exactly twice'

# 6. Test, then reload (a reload keeps serving; nothing restarts).
sudo nginx -t || undo 'step 6: nginx -t'
sudo systemctl reload nginx
echo 'realip snippet installed and nginx reloaded'
)
```

`&&` and `||` are fine here because this is bash on the server (the PowerShell warning in this
repo is about the Windows side).

**Verify.** From your own computer (not the server):

```bash
curl -s -o /dev/null -w '%{http_code}\n' -A realip-check https://genizahsearch.com/robots.txt
curl -s -o /dev/null -w '%{http_code}\n' -A realip-spoof -H 'CF-Connecting-IP: 203.0.113.9' \
     -H 'X-Forwarded-For: 203.0.113.9' https://genizahsearch.com/robots.txt
```

Both should print `200`. Another code for the spoof request does not by itself say who answered
it: if its line is missing from the access log below, Cloudflare did; if its line is there, it
reached nginx, and the rule below holds for it too.

then on the server:

```bash
sudo grep -E 'realip-(check|spoof)' /var/log/nginx/access.log | tail -n 2
sudo tail -n 2000 /var/log/nginx/access.log | awk '{print $1}' | sort | uniq -c | sort -rn | head
```

Both lines must start with your own public address (the one Cloudflare's dashboard shows for
you), never `203.0.113.9` and never a Cloudflare range (`172.64-71.x`, `162.158-159.x`,
`104.16-27.x`, ...). The marked lines are the decisive check: the second command counts the last
2,000 requests, some from before the reload, so edges leave its top entries only as those age out.

**Keep it current.** Weekly, Monday 04:17 server time:

```bash
printf '%s\n' 'PATH=/usr/sbin:/usr/bin:/sbin:/bin' \
    '17 4 * * 1 root /usr/local/sbin/genizah-refresh-cloudflare-realip >> /var/log/genizah-cloudflare-realip.log 2>&1 || logger -t genizah-realip "refresh failed (exit $?); see /var/log/genizah-cloudflare-realip.log"' \
    | sudo tee /etc/cron.d/genizah-cloudflare-realip
```

Nothing alerts you by itself. A failed run (any non-zero exit) also leaves a `genizah-realip` line
in the system journal; when you look at the server, check both of these:

```bash
journalctl -t genizah-realip -n 20        # expect "-- No entries --"
ls /etc/nginx/snippets/genizah_cloudflare_realip.conf.reload-pending   # expect "No such file"
```

An occasional exit 1 (a fetch that failed or was refused) is simply retried by the next week's
run; repeated failures, or a pending reload, need a look at the log. Re-run step 1 whenever
`scripts/refresh_cloudflare_realip.py` changes in the repo.

**Roll back.**

```bash
sudo cp -a /root/genizah.nginx.before-realip /etc/nginx/sites-available/genizah
sudo nginx -t && sudo systemctl reload nginx
sudo rm /etc/cron.d/genizah-cloudflare-realip      # if the cron entry was added
```

The snippet file can stay; nothing reads it once the `include` lines are gone.

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

- **HMAC upload tokens**: Server issues signed tokens on cache miss (`X-Puzzle-Upload-Token` header). Uploads to `/api/puzzle_process` require a valid token bound to the exact cache entry (fl_id, size, threshold, processed, CUL flag), usable once, 5-min expiry.
- **Who fills the shared cache**: an upload from a signed-in visitor goes into the shared cache and is recorded (user id, UTC time) in `_uploads.jsonl` beside it; an upload from a signed-out browser is kept under `_browser/<hash>/` and served only to that browser (`Cache-Control: private`). No upload replaces an existing file. Server-side image fetches are limited to the known IIIF hosts (`DIRECT_IMAGE_HOST_SUFFIXES`).
- **Saved joins (drafts)** are kept per visitor: per account when signed in, per browser when signed out (`owner_key` column in `joins.db`, schema v3, added automatically on first open). Published joins are unchanged (Supabase `published_joins`, public, edited only by their author). Rows saved before v3 have no owner and are not shown on the web; `scripts/assign_puzzle_draft_owners.py` (owner-run, on a copy, dry run by default) assigns owners from a mapping.
- **URL validation**: Extension only fetches from `iiif.nli.org.il`
- **Origin validation**: Content script only responds to messages from `genizahsearch.com` and `localhost`
- **Rate limiting**: 60 requests/min/IP on upload endpoints
- **`PUZZLE_UPLOAD_SECRET`**: Set in `.env` for stable tokens across restarts. Auto-generated if unset (tokens invalidated on restart).

### Server Cache

Processed puzzle images are cached on the server disk at (production, checked 2026-09-29):
```
/home/ubuntu/GenizahSearch/cache/puzzle/
```

Cache key format: `{fl_id}_{size}_{threshold}[_cul]_{PROCESSING_VERSION}.png`; images fetched from a direct
IIIF URL are filed under a name derived from that URL, never under a fragment id.

Once in the shared cache, images serve all users without the extension. The shared cache grows from
signed-in extension users (via `/api/puzzle_process`) and from the server's own IIIF fetches; signed-out
uploads stay under `_browser/<hash>/` for that browser. Before a deploy that touches the puzzle, save a file
list (`find cache/puzzle -type f -printf '%P %s %T@\n' | sort`) so that any change can be checked afterwards.

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

# Nginx logs (first field = the real visitor once the Cloudflare realip snippet is in;
# before it, a Cloudflare edge address)
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

### Deploy the Connections Atlas Beta (asset-first)

The Connections Atlas is a claim-free preview page (`/atlas`), gated by the
`ATLAS_PREVIEW_ENABLED` flag (default **OFF**). Its data is a single, offline-baked,
content-hashed binary asset that lives in a **new sidecar-style directory** `atlas_data/`
at the repo root — **OUTSIDE** `web/static/` so it can never be served through the public
static mount and thereby bypass the flag. It is served ONLY through the flag- and
readiness-gated `/atlas-data/*` routes, and it is **gitignored** (like the SQLite sidecars),
so `deploy.sh`'s `git reset --hard` never carries it.

Because the asset is gitignored, it must be uploaded **asset-first** — exactly the same
scp-first posture as the SQLite reference sidecars (upload the data, THEN push code). This
ordering guarantees there is never a flag-ON / asset-missing window (a broken beta):

```bash
# 1. Bake the asset locally (offline; bake-time deps are in requirements-atlas-bake.txt,
#    NOT requirements.txt — this tooling never runs inside the web process):
pip install -r requirements-atlas-bake.txt
python scripts/build_atlas_asset.py <research-db>        # writes atlas_data/

# 2. Upload atlas_data/ to the server FIRST (outside the static root), like the sidecars:
scp -r atlas_data/ ubuntu@<server>:/home/ubuntu/GenizahSearch/atlas_data/

# 3. THEN deploy code:
ssh ubuntu@<server> 'cd /home/ubuntu/GenizahSearch && ./deploy.sh master-main'

# 4. THEN set the flag and restart so web/atlas_assets.load_atlas_state() re-runs and loads
#    the asset (ready=True) BEFORE the flag is observed live:
sudo systemctl edit genizah-web     # add: Environment=ATLAS_PREVIEW_ENABLED=1
sudo systemctl daemon-reload && sudo systemctl restart genizah-web
```

The asset load is authoritative at startup (there is deliberately no per-request
`os.path.exists`). Confirm the manifest route serves the expected content-hashed asset:
`GET /atlas-data/manifest.json` should return `200` with `Cache-Control: no-cache` + an
`ETag`. **Rollback** is the flag: setting `ATLAS_PREVIEW_ENABLED` OFF (and restarting)
clean-hides `/atlas`, drops the nav link + homepage teaser, and 404s the `/atlas-data/*`
routes — with the rest of the app untouched.

### Deploy Letter-Level Parallels Search (code-first, index built on the box)

Letter-level search (`/parallels`, and `method='passage'` on `POST /api/parallels`) is gated by
`PASSAGE_PARALLELS_ENABLED`, and its Witnesses panel by `PASSAGE_MULTI_WITNESS_ENABLED` — both
default **OFF**. As with the Atlas, each flag is necessary but not sufficient:
`web/passage_assets.py::passage_available()` also requires the index to have opened cleanly at
startup, and `shared/passage_index.py::open_index` is itself the fail-closed validator (manifest,
layout and normalizer versions, bit budgets, byte order, CSR sanity, declared-vs-actual file
sizes).

**This deploy is the INVERSE of the Atlas one, and the ordering matters for the opposite reason.**
The Atlas asset is baked offline and uploaded, so it goes asset-first to avoid a flag-ON /
asset-missing window. The passage index is ~3.5 GB and is built **from the corpus the server
already serves**, so the code must arrive first and the index is made in place. Building it
elsewhere and uploading would also mean shipping an index of a *different* corpus than the one the
site fetches text from.

Deploying code without the index is safe and is the normal intermediate state: the loader is
fail-closed, so the whole surface hides rather than half-working. **One visible consequence** —
the What's New toast builds its list from `passage_available()`, so between the code deploy and
the flag flip the toast shows nothing at all.

```bash
# 1. Code first. The flags are still off, so nothing new is user-visible yet.
ssh ubuntu@<server> 'cd /home/ubuntu/GenizahSearch && ./deploy.sh master-main'

# 2. Preflight. Reports the paths it will use and the free space it needs, and
#    builds nothing. Run this before committing the box to a long job.
ssh ubuntu@<server> 'cd /home/ubuntu/GenizahSearch && source venv/bin/activate && \
    python scripts/build_passage_index.py --check'

# 3. Build, detached and niced. ~10 GB free is needed while it runs; the finished index is
#    ~3.5 GB. Peak RSS is ~1.75 GB at the default 16 partitions (it is 3.5 GB at 8 — the
#    partition count is halved precisely because this box is also serving the site). The only
#    dependency is numpy, already in requirements.txt.
ssh ubuntu@<server> 'cd /home/ubuntu/GenizahSearch && source venv/bin/activate && \
    setsid nice -n 10 nohup python scripts/build_passage_index.py \
    </dev/null >/home/ubuntu/passage_build.log 2>&1 & echo $! > /home/ubuntu/passage_build.pid'

# 4. Watch it. A non-zero exit is a real failure: the script reports a build that RETURNED
#    an error status as a failure, not as a completed build.
ssh ubuntu@<server> 'tail -f /home/ubuntu/passage_build.log'

# 5. Only once the log says "installed", set the flags and restart, so
#    load_passage_state() re-runs and the index is ready BEFORE the flags are observed
#    live. Flags live in `.env` on this box — NOT in a systemd override, which is the
#    Atlas section's mechanism and is not what this service reads:
cp .env .env.bak-before-passage-golive
printf 'PASSAGE_PARALLELS_ENABLED=1\nPASSAGE_MULTI_WITNESS_ENABLED=1\n' >> .env

#    Set them BEFORE the code deploy and let its restart pick up both, so the go-live is
#    ONE restart rather than two. Nothing changes in the window between — the flags are
#    read once at startup.
./deploy.sh master-main
```

**Running the build against a live site is safe.** On Linux a rename over an open mmap is legal:
the server keeps serving its current index from the now-unlinked inode and notices nothing, and the
new one is picked up on the next restart. (This is also why the desktop needs an elaborate
handle-release protocol and the server does not — Windows refuses that rename, Linux does not
care.) Nothing about the swap is visible to a reader until step 5.

**Do NOT use `scripts/bench_passage_build.py` here.** It is a measurement harness that builds
repeatedly to compare constructions, and its own docstring says "Dev-box / owner-machine only.
Never run it on the web server." `scripts/build_passage_index.py` is the production entry point.

**Rollback** is the flags: setting both OFF and restarting clean-hides the method selector, rejects
`method='passage'` at the API, empties the What's New toast, and leaves the rest of the app
untouched. The index on disk can stay; it costs nothing while the flag is off, because the flag is
checked *before* the directory is opened (an earlier incident: unconditionally mmapping a large
index while its flag was OFF evicted 1.4 GB of page cache on a 15.8 GB host).

**Rebuilding later.** The index is a snapshot of the corpus at build time. After any corpus
refresh, re-run step 3 — it stages, validates and swaps atomically, and on any failure it restores
the previous index rather than leaving a broken one. `passage_index/` is gitignored, so
`deploy.sh`'s `git reset --hard` never touches it.

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
| `pgp.db` | `pgp_data/` | ~156 MB | 1.1.0 | PGP documents (36.6K), sources (10.4K), footnotes (23.8K), fragments (37.4K), meta. **No `pgp_translations`** -- withheld by owner decision (`docs/plans/PGP_TRANSLATION_QUALITY.md`); `scripts/check_shipping_sidecar.py` refuses a copy that carries it |
| `libraries_translations.db` | project root | 76 MB | - | Dicta translations for library titles (~185K records, Hebrew↔English) |

#### Initial Upload to Server

```bash
# From local machine:
scp fist_data/fjms_enrichment.db ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com:/home/ubuntu/GenizahSearch/fist_data/
scp nli_data/nli_crossref.db ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com:/home/ubuntu/GenizahSearch/nli_data/

# pgp.db goes through the guarded deploy script, never a bare scp: the web reads
# pgp_translations through TranslationService, so an unconditional upload can publish a
# withheld corpus. (Deploys run from PowerShell 5.1 with native OpenSSH, where a bash
# `&&` chain is a parse error -- so the guard and the upload live in one script.)
#   powershell -File scripts/deploy_pgp_sidecar.ps1

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

Princeton publishes the PGP metadata as CSV exports in
**[princetongenizalab/pgp-metadata](https://github.com/princetongenizalab/pgp-metadata)**
(`data/documents.csv`, `fragments.csv`, `footnotes.csv`), auto-committed several times a
day. Per-canvas transcription HTML comes from a second repo,
[princetongenizalab/pgp-text](https://github.com/princetongenizalab/pgp-text), cloned into
`pgp_data/pgp-text/`.

The chain is: **upstream CSVs -> Supabase -> `pgp_data/pgp.db` -> both apps.** Supabase is
the staging area, not the thing the apps read; at runtime PGP is served entirely from the
sidecar (see [decision 0002](../decisions/0002-sidecars-instead-of-a-backend-process.md)).
That is why a refresh is not finished until the sidecar is rebuilt and deployed.

**Steps 0-8 run on your workstation** -- they need `libraries.csv`, download ~65 MB of
CSVs and build a 156 MB sidecar. Only the deploy touches the server.

**Run them as ONE sequence, not as pasted lines.** Every step exits non-zero when it must
not be followed (a checksum mismatch, a classification that was not applied, an export
that came back short) -- but a pasted block does not consume exit codes: line 5 runs after
line 4 failed. That was demonstrated end to end on this very procedure: `update_doc_relation.py`
exited 1, the export ran anyway, and the shipping guard said "fit to ship". So the sequence
is a script, and each step is followed by an exit-code check:

```powershell
# Steps 0-3 (FIST supplement, pinned fetch, derive transcriptions_linked.csv, import DRY RUN).
# Stops at the first failure. Then read pgp_data/full_import_dry_run_report.txt.
powershell -File scripts/refresh_pgp_data.ps1

# Steps 4-8: import --execute, classify doc_relation (verified by read-back), sections,
# export the sidecar, run the shipping guard on it.
powershell -File scripts/refresh_pgp_data.ps1 -Execute -StartAt 4

# CODE FIRST, then this sidecar. The usual "DBs before code" rule assumes the schema is
# unchanged; a refresh that ADDS a column inverts it. On 2026-09-22 the refreshed pgp.db
# went up ahead of its code and added `documents.doc_relation`, NULL for 29,226 of 36,642
# rows; the deployed browse code read it with `.get(k, '')`, a default that fires only on a
# MISSING key, so browse enrichment raised TypeError -- images, folios and pagination gone
# on ~80% of PGP pages -- until the sidecar was rolled back.
#   ssh ubuntu@<server> 'cd /home/ubuntu/GenizahSearch && ./deploy.sh master-main'
#
# Then: guard, code check, scp, restart -- each gated on the previous exit code. Step 2 reads
# meta.source_revision, which export_pgp_sidecar.py stamps into the DATABASE at build time,
# and refuses unless the RUNNING genizah-web already has that commit (deploy.sh records it in
# .deployed_revision after a successful restart; a checkout alone proves nothing about the
# process in memory). It also refuses a sidecar built from a dirty tree, since that code is in
# no commit at all. So the order above is enforced, not merely documented.
powershell -File scripts/deploy_pgp_sidecar.ps1
```

What each step is, for when one fails and you need to re-run it by hand (the runner prints
the exact command it ran, and `-StartAt N` resumes there):

| Step | Command | Why it exists / what stops it |
|---|---|---|
| 0 | `python scripts/fist_shelfmarks_export.py` | ~35,600 shelfmarks `libraries.csv` lacks. Without it the fragment match rate falls 94.5% -> 87.5% (~2,900 fragments lose their IIIF images). Steps 2 and 4 refuse to run without it. |
| 1 | `python scripts/fetch_pgp_metadata.py` | The three upstream CSVs, all at ONE commit, with a per-file SHA-256 manifest (`upstream_provenance.json`). `--dry-run` prints row deltas against the current `pgp.db`. |
| 2 | `python scripts/pgp_transcriptions_export.py` | Derives `transcriptions_linked.csv` (NOT an upstream file) from documents + footnotes + `libraries.csv` + the supplement. Refuses CSVs that fail the manifest; refuses to overwrite with an empty result; stamps `derived_provenance.json` with the commit, the output's hash, and the hashes of `libraries.csv` and the supplement. |
| 3 | `python scripts/import_pgp_full.py` | Dry run: validates and writes `full_import_dry_run_report.txt`. Read THAT file -- `full_import_report.txt` is the previous `--execute`'s report. |
| 4 | `python scripts/import_pgp_full.py --execute` | Refuses inputs that fail their checksums (`--no-provenance-check` imports anyway but records no commit). Removes the previous `import_provenance.json` before the first push and writes a new one only on completion -- so an interrupted import cannot be stamped. |
| 5 | `python scripts/update_doc_relation.py --execute` | Same provenance check as step 4 on the same derived file. Fatal if any row is unusable, if nothing was classified, if any pgpid matched no row, if any update raised -- and every classification is READ BACK and compared before it reports success. |
| 6 | `python scripts/import_pgp_sections.py --execute` | Per-canvas sections from the `pgp-text` repo (clones/pulls it first). |
| 7 | `python scripts/export_pgp_sidecar.py` | Builds `pgp.db.new` beside the live file and swaps it in only after validation. Fatal on an EMPTY core table and on a core table that came back SMALLER than the live sidecar's (upserts never delete; a shrink is a restricted client returning part of the corpus). Stamps the upstream commit only when `import_provenance.json` says its inputs were verified, names this Supabase project, and its counts match. |
| 8 | `python scripts/check_shipping_sidecar.py --sidecar pgp_data/pgp.db` | The same guard `build_app.bat` and the installer run. |

Sanity-check what you are about to ship (read-only):

```bash
python -c "import sqlite3; c=sqlite3.connect('file:pgp_data/pgp.db?mode=ro',uri=True); \
print(dict(c.execute('SELECT key,value FROM meta')))"
```

**What the desktop needs.** `GenizahSearchPro.spec` bundles `pgp_data/pgp.db` into the
installer, so desktop users get a refresh only in the next build. There is no working
auto-update path for it: `SidecarUpdateThread` fetches a GitHub release tagged
`data-latest`, which does not exist. A single install can be updated by dropping the file
at `%LOCALAPPDATA%\GenizahSearchPro\data\pgp_data\pgp.db`, which
[shared/document_service.py](../../shared/document_service.py) prefers over the bundled copy.

**Recompute the homepage stats.** `web/stats_service.py::CORPUS_STATS` is hardcoded, and
one of its five numbers (`scholarly_transcriptions`) is derived from
`document_sources.doc_relation LIKE '%Edition%'`. It only changes on a refresh + redeploy.

#### Traps this procedure exists to avoid

| Trap | What happens | Status |
|---|---|---|
| `import_pgp_documents.py` / `import_document_sources.py` | superseded by `import_pgp_full.py`; running them first means it overwrites their partial work | they now refuse to run without `--run-superseded` |
| Missing `--execute` | both importers default to `--dry-run`, so the whole procedure silently writes nothing | spelled out above |
| `transcriptions_linked.csv` | generated locally, not downloaded; step 3 aborts without it | step 2 |
| `documents.doc_relation` | dropped by the sidecar exporter before v1.1.0, so 891 translation-flagged documents rendered as "PGP Transcription" | carried now, and a fail-closed check refuses to build a sidecar missing any column Supabase returns (it cannot see columns on a table that comes back empty, and says so) |
| `pgp_translations` | destroyed by every rebuild (the 2026-04-22 refresh took 34,954 rows with it, unnoticed for five months) | carried forward across rebuilds |
| A failed export | deleted `pgp.db` before building, so a failure left no sidecar at all | builds beside the live file, swaps after validation |
| Stale `pgp-text` checkout | a failed `git pull` warned and imported the old checkout as if fresh | now aborts; `--skip-clone` is the deliberate opt-out |
| Missing FIST supplement | fragment match rate drops 94.5% -> 87.5% with no warning, so ~2,900 fragments never link and their IIIF images never appear | step 0; both consumers now refuse to run without it |
| `fist_shelfmarks_export.py` | pointed at `FIST_DB_BACKUP/FIST.db`, a directory that no longer exists, so the supplement could not be regenerated at all | now finds `fist_data/FIST.db` |

**Upserts never delete.** `import_pgp_full.py` upserts on natural keys, so documents
Princeton has *withdrawn* stay in Supabase and in the sidecar (344 such pgpids as of
2026-09-22). Removing them is a separate, deliberate decision -- not something a refresh
should do silently.

**PGP data files** (in `pgp_data/`): not in git. `fetch_pgp_metadata.py` downloads them and
writes `pgp_data/upstream_provenance.json`, which the exporter folds into `pgp.db`'s `meta`
table -- so a built sidecar records *which upstream commit its data came from*, not just
when it was built.

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
| `GENIZAH_STORAGE_SECRET` | Signs the web session cookie; 32+ characters; the app refuses to start without it | Required |
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
