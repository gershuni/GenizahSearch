# GenizahSearch Technical Deployment Guide

> Last updated: 2026-01-23
> For: Developers, System Administrators, AI Assistants

---

## Deployment Summary

This production deployment was set up in January 2026. Key decisions:

- **Domain**: Registered via Cloudflare (includes DNS, SSL, DDoS protection)
- **Transcriptions data**: Downloaded from Zenodo (https://zenodo.org/records/17734473)
- **Search indexes**: Built on-server using `build_index.py` (~1 hour build time)
- **Database**: PostgreSQL for user data; Tantivy for manuscript search
- **Management**: Cockpit web UI at admin.genizahsearch.com

### Data Sources
| File | Source | Size |
|------|--------|------|
| Transcriptions.txt (V0.8) | Zenodo | 1.4 GB |
| Genizah_OLD.txt (V0.7) | Optional, local upload | ~1 GB |

### Index Sizes (built from Transcriptions.txt)
| Index | Size | Purpose |
|-------|------|---------|
| tantivy_db | 3.3 GB | Main manuscript search |
| lab_index | 3.0 GB | Parallels/lab features |

---

## Infrastructure Overview

### Server Details
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

### Cloudflare DNS Records
| Type | Name | Target | Proxy |
|------|------|--------|-------|
| A | genizahsearch.com | 44.247.206.248 | Proxied |
| CNAME | www | genizahsearch.com | Proxied |
| CNAME | admin | genizahsearch.com | Proxied |

### Application Stack
| Component | Technology | Port |
|-----------|------------|------|
| Backend API | FastAPI + Uvicorn | 8000 |
| Web Frontend | NiceGUI | 8081 |
| Database | PostgreSQL 16 | 5432 |
| Web Server | Nginx 1.24 | 80, 443 |
| Search Engine | Tantivy | - |

---

## Directory Structure

```
/home/ubuntu/GenizahSearch/
├── backend/                 # FastAPI backend
│   ├── api/routes/         # API endpoints
│   ├── models/             # SQLAlchemy models
│   ├── schemas/            # Pydantic schemas
│   └── services/           # Business logic
├── web/                    # NiceGUI frontend
│   ├── pages/              # Application pages
│   └── components/         # UI components
├── Genizah_Index/          # Search indexes
│   ├── tantivy_db/         # Main search index (3.3GB)
│   ├── lab_index/          # Parallels index (3.0GB)
│   ├── browse_map.pkl      # Browse navigation data
│   ├── metadata_cache.pkl  # Metadata cache
│   └── lab/                # Lab configuration
├── data/                   # Application data
│   └── genizah_users.db    # SQLite (dev) - not used in prod
├── venv/                   # Python virtual environment
├── Transcriptions.txt      # Source transcription data (1.4GB)
├── .env                    # Environment variables
├── deploy.sh               # Deployment script
├── build_index.py          # Index building script
└── create_admin.py         # Admin user creation script
```

---

## Configuration Files

### Environment Variables (`/home/ubuntu/GenizahSearch/.env`)
```bash
DATABASE_URL=postgresql://genizah:genizah_secure_pwd_2024@localhost:5432/genizah_db
SECRET_KEY=cC7D6nzn_wJn091OuY_14V_9-IJK0SDjNUy7BAep078
ENVIRONMENT=production
DEBUG=false
```

### Systemd Services

#### Backend Service (`/etc/systemd/system/genizah-backend.service`)
```ini
[Unit]
Description=Genizah Backend API
After=network.target postgresql.service

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/GenizahSearch
EnvironmentFile=/home/ubuntu/GenizahSearch/.env
Environment="CORS_ORIGINS=[\"https://genizahsearch.com\",\"https://www.genizahsearch.com\",\"http://localhost:8081\"]"
ExecStart=/home/ubuntu/GenizahSearch/venv/bin/python -m uvicorn backend.main:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

#### Web Service (`/etc/systemd/system/genizah-web.service`)
```ini
[Unit]
Description=Genizah Web Interface
After=network.target genizah-backend.service

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/GenizahSearch
EnvironmentFile=/home/ubuntu/GenizahSearch/.env
Environment=CORRECTIONS_API_URL=https://genizahsearch.com/api/v1
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
    # Redirects to HTTPS (managed by certbot)
}

server {
    listen 443 ssl;
    server_name genizahsearch.com www.genizahsearch.com;

    ssl_certificate /etc/letsencrypt/live/genizahsearch.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/genizahsearch.com/privkey.pem;

    # Backend API
    location /api/ {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Web frontend (NiceGUI with WebSocket)
    location / {
        proxy_pass http://127.0.0.1:8081;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
    }
}
```

---

## Database

### PostgreSQL Connection
```
Host: localhost
Port: 5432
Database: genizah_db
User: genizah
Password: genizah_secure_pwd_2024
```

### Useful Commands
```bash
# Connect to database
sudo -u postgres psql genizah_db

# List tables
\dt

# View users
SELECT id, email, username, role, is_active FROM users;

# Backup database
pg_dump -U genizah genizah_db > backup_$(date +%Y%m%d).sql

# Restore database
psql -U genizah genizah_db < backup.sql
```

---

## Service Management

### Systemd Commands
```bash
# Check status
sudo systemctl status genizah-backend genizah-web

# Start/Stop/Restart
sudo systemctl start genizah-backend genizah-web
sudo systemctl stop genizah-backend genizah-web
sudo systemctl restart genizah-backend genizah-web

# Enable/Disable on boot
sudo systemctl enable genizah-backend genizah-web
sudo systemctl disable genizah-backend genizah-web

# Reload systemd after config changes
sudo systemctl daemon-reload
```

### View Logs
```bash
# Real-time backend logs
sudo journalctl -u genizah-backend -f

# Real-time web logs
sudo journalctl -u genizah-web -f

# Last 100 lines
sudo journalctl -u genizah-backend -n 100

# Logs since today
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

Or manually:
```bash
cd /home/ubuntu/GenizahSearch
git fetch origin
git reset --hard origin/web-parallels-snippet-fix-7917465670133512409
source venv/bin/activate
pip install -r requirements.txt
pip install -r backend/requirements.txt
sudo systemctl restart genizah-backend genizah-web
```

### Rebuild Indexes
```bash
cd /home/ubuntu/GenizahSearch
source venv/bin/activate

# Build both indexes
python build_index.py

# Build main index only
python build_index.py main

# Build lab index only
python build_index.py lab

# Restart services after rebuild
sudo systemctl restart genizah-backend genizah-web
```

### Create Admin User
```bash
cd /home/ubuntu/GenizahSearch
source venv/bin/activate
python create_admin.py email@example.com password "Full Name"
```

---

## SSL Certificate

### Certificate Details
- Provider: Let's Encrypt
- Auto-renewal: Enabled via certbot timer
- Location: `/etc/letsencrypt/live/genizahsearch.com/`

### Manual Renewal
```bash
sudo certbot renew
sudo systemctl reload nginx
```

### Check Certificate Expiry
```bash
sudo certbot certificates
```

---

## Monitoring & Health Checks

### Quick Health Check
```bash
# Check services
curl -s http://localhost:8000/api/docs -o /dev/null -w '%{http_code}\n'  # Should be 200
curl -s http://localhost:8081 -o /dev/null -w '%{http_code}\n'           # Should be 200

# Check external access
curl -s https://genizahsearch.com -o /dev/null -w '%{http_code}\n'       # Should be 200
```

### Disk Usage
```bash
df -h /
du -sh /home/ubuntu/GenizahSearch/Genizah_Index/*
```

### Memory Usage
```bash
free -h
```

### Process Status
```bash
ps aux | grep -E '(uvicorn|nicegui|python)'
```

---

## Troubleshooting

### Service Won't Start
1. Check logs: `sudo journalctl -u genizah-backend -n 50`
2. Verify environment file: `cat /home/ubuntu/GenizahSearch/.env`
3. Test manually: `cd /home/ubuntu/GenizahSearch && source venv/bin/activate && python -m uvicorn backend.main:app`

### 502 Bad Gateway
1. Check if services are running: `sudo systemctl status genizah-backend genizah-web`
2. Check nginx config: `sudo nginx -t`
3. Reload nginx: `sudo systemctl reload nginx`

### Database Connection Error
1. Check PostgreSQL: `sudo systemctl status postgresql`
2. Test connection: `psql -U genizah -h localhost genizah_db`
3. Check DATABASE_URL in `.env`

### Index Not Loading
1. Verify index exists: `ls -la /home/ubuntu/GenizahSearch/Genizah_Index/tantivy_db/`
2. Check permissions: `ls -la /home/ubuntu/GenizahSearch/Genizah_Index/`
3. Restart services: `sudo systemctl restart genizah-backend genizah-web`

---

## Backup Procedures

### Database Backup
```bash
# Create backup
pg_dump -U genizah genizah_db > /home/ubuntu/backups/db_$(date +%Y%m%d_%H%M%S).sql

# Automated daily backup (add to crontab)
0 2 * * * pg_dump -U genizah genizah_db > /home/ubuntu/backups/db_$(date +\%Y\%m\%d).sql
```

### Index Backup
```bash
# Compress and backup indexes
tar -czvf /home/ubuntu/backups/indexes_$(date +%Y%m%d).tar.gz /home/ubuntu/GenizahSearch/Genizah_Index/
```

---

## Security Notes

- SSH access is key-based only
- PostgreSQL only accepts local connections
- All external traffic goes through Cloudflare proxy
- SSL/TLS encryption enabled
- CORS restricted to genizahsearch.com domains

---

## Cockpit Server Management

Cockpit provides a web-based UI for server management.

### Access
- URL: https://admin.genizahsearch.com
- User: `ubuntu`
- Auth: Password (set via `sudo passwd ubuntu`)

### Nginx Configuration (`/etc/nginx/sites-available/cockpit`)
```nginx
server {
    listen 443 ssl;
    server_name admin.genizahsearch.com;

    ssl_certificate /etc/letsencrypt/live/genizahsearch.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/genizahsearch.com/privkey.pem;

    location / {
        proxy_pass https://127.0.0.1:9090;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_ssl_verify off;
    }
}
```

### Service Management
```bash
# Cockpit is socket-activated
sudo systemctl status cockpit.socket
sudo systemctl restart cockpit.socket
```

---

## Pending / Optional Tasks

- [ ] Upload V0.7 transcriptions (Genizah_OLD.txt) for historical comparison
- [ ] Set Cloudflare SSL mode to "Full (strict)" for additional security
- [ ] Configure automated database backups (cron job)
- [ ] Set up monitoring alerts (optional: UptimeRobot, Healthchecks.io)

---

## Contact & Resources

- GitHub Repository: https://github.com/gershuni/GenizahSearch
- API Documentation: https://genizahsearch.com/api/docs
- Server Management: https://admin.genizahsearch.com
- Cloudflare Dashboard: https://dash.cloudflare.com
