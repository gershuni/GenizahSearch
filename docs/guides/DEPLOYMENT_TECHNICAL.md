# GenizahSearch Technical Deployment Guide

> Last updated: 2026-02-01
> For: Developers, System Administrators, AI Assistants

---

## Architecture Overview (January 2026)

GenizahSearch uses a simplified architecture with Supabase as the backend:

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
┌─────────────────────────────────────────────────────────────────┐
│                      WEB APPLICATION                             │
│                    (NiceGUI on port 8081)                        │
│                                                                  │
│   ┌─────────────────┐              ┌─────────────────────────┐  │
│   │  Search/Browse  │              │    User Data            │  │
│   │                 │              │                         │  │
│   │  Tantivy Index  │              │  Supabase (Cloud)       │  │
│   │    (Local)      │              │  - Authentication       │  │
│   │                 │              │  - Lists & Items        │  │
│   │  - tantivy_db   │              │  - Corrections          │  │
│   │  - lab_index    │              │  - Comments             │  │
│   │                 │              │  - Discoveries          │  │
│   └─────────────────┘              │  - Joins                │  │
│                                    └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
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
├── Genizah_Index/         # Search indexes
│   ├── tantivy_db/        # Main search index (3.3GB)
│   ├── lab_index/         # Parallels index (3.0GB)
│   ├── browse_map.pkl     # Browse navigation data
│   ├── metadata_cache.pkl # Metadata cache
│   └── lab/               # Lab configuration
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
```

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

## Data Sources

| File | Source | Size |
|------|--------|------|
| Transcriptions.txt (V0.8) | [Zenodo](https://zenodo.org/records/17734473) | 1.4 GB |
| Genizah_OLD.txt (V0.7) | Optional, historical | ~1 GB |

### Index Sizes

| Index | Size | Purpose |
|-------|------|---------|
| tantivy_db | 3.3 GB | Main manuscript search |
| lab_index | 3.0 GB | Parallels/lab features |

---

## Resources

- GitHub: https://github.com/gershuni/GenizahSearch
- Website: https://genizahsearch.com
- Server Management: https://admin.genizahsearch.com
- Supabase Dashboard: https://supabase.com/dashboard
- Cloudflare Dashboard: https://dash.cloudflare.com
