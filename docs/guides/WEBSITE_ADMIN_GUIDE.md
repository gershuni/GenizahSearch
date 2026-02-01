# GenizahSearch Website Admin Guide

> Last updated: 2026-01-31
> For: Site administrators and non-technical users

---

## Quick Reference Card

| What | Where/How |
|------|-----------|
| **Website URL** | https://genizahsearch.com |
| **Supabase Dashboard** | https://supabase.com/dashboard |
| **Server Management** | https://admin.genizahsearch.com (Cockpit) |
| **Server Access** | `ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com` |
| **Cloudflare Dashboard** | https://dash.cloudflare.com |

---

## Architecture Overview (Updated 2026-01-31)

```
User's Browser
     │
     ▼
Cloudflare (DNS + Security + SSL)
     │
     ▼
Nginx (Web Server on port 80/443)
     │
     └─── /* ───────► Frontend (NiceGUI on port 8081)
                          │
                          ├── Search/Browse ──► Tantivy Index (local)
                          │
                          └── User Data ──────► Supabase (cloud)
                                                    │
                                                    ├── PostgreSQL Database
                                                    ├── Authentication
                                                    └── Row Level Security
```

**Important Change:** The FastAPI backend has been removed. All user data (lists, corrections, comments, discoveries) is now stored directly in **Supabase** cloud database.

### Key Components

| Component | What it does |
|-----------|--------------|
| **Cloudflare** | Protects the site, provides SSL, caches content |
| **Nginx** | Routes traffic to the web app |
| **Frontend (NiceGUI)** | The visual interface users see |
| **Supabase** | Cloud database for user data, auth, lists |
| **Tantivy Index** | Powers the manuscript search (local) |

---

## Common Tasks

### 1. Check if the Website is Working

**From your browser:**
- Visit https://genizahsearch.com
- The page should load within a few seconds

**From command line:**
```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "sudo systemctl status genizah-web"
```
Look for "Active: active (running)" in green.

---

### 2. Restart the Website (if something is wrong)

If the website is slow or not responding:

```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "sudo systemctl restart genizah-web"
```

Wait 10 seconds, then check if it's working.

---

### 3. Update the Website with New Code

When you've made changes to the code and pushed to GitHub:

```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "cd /home/ubuntu/GenizahSearch && ./deploy.sh"
```

This will:
1. Download the latest code
2. Install any new dependencies
3. Restart the website

---

### 4. Manage Users (via Supabase)

User management is now done through **Supabase Dashboard**:

1. Go to https://supabase.com/dashboard
2. Select the GenizahSearch project
3. Go to **Authentication** → **Users**
4. From here you can:
   - View all users
   - Delete users
   - Reset passwords

To change a user's role (admin, editor, etc.):
1. Go to **Table Editor** → **profiles**
2. Find the user by email
3. Edit their `role` field

---

### 5. View Recent Activity/Logs

To see what's happening on the website:

```bash
# See the last 50 log entries
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "sudo journalctl -u genizah-web -n 50"
```

To watch logs in real-time (press Ctrl+C to stop):
```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "sudo journalctl -u genizah-web -f"
```

---

### 6. Check Server Health

```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "df -h / && free -h"
```

This shows:
- **Disk space**: Should not be above 80% full
- **Memory**: Should have some free memory available

---

## Troubleshooting

### Website shows "502 Bad Gateway"

**What it means:** The web server can't connect to the application.

**How to fix:**
1. Restart the service:
   ```bash
   ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "sudo systemctl restart genizah-web"
   ```
2. Wait 30 seconds and try again.

---

### Website is very slow

**Possible causes:**
- High traffic
- Server running out of memory
- Index files corrupted

**How to fix:**
1. Check server resources:
   ```bash
   ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "free -h && df -h /"
   ```
2. Restart service:
   ```bash
   ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "sudo systemctl restart genizah-web"
   ```

---

### Can't connect via SSH

**What it means:** The server might be down or there's a network issue.

**What to do:**
1. Check if the website loads in browser (https://genizahsearch.com)
2. If website works but SSH doesn't, contact the AWS account owner
3. If nothing works, the server may need to be restarted from AWS console

---

### User data not syncing

User data (lists, corrections) is stored in Supabase cloud. If data isn't syncing:

1. Check Supabase status: https://status.supabase.com
2. Verify the `.env` file has correct Supabase credentials
3. Check browser console for API errors

---

### Search not returning results

**What it means:** The search index might be corrupted or not loaded.

**How to fix:**
1. Restart service first:
   ```bash
   ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "sudo systemctl restart genizah-web"
   ```
2. If still not working, rebuild the index:
   ```bash
   ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "cd /home/ubuntu/GenizahSearch && source venv/bin/activate && nohup python build_index.py > index_rebuild.log 2>&1 &"
   ```

---

## Important Files & Locations

| File/Folder | Purpose |
|-------------|---------|
| `/home/ubuntu/GenizahSearch/` | Main application folder |
| `/home/ubuntu/GenizahSearch/.env` | Configuration (Supabase URL, keys) |
| `/home/ubuntu/GenizahSearch/Genizah_Index/` | Search indexes |
| `/home/ubuntu/GenizahSearch/Transcriptions.txt` | Source manuscript data |
| `/home/ubuntu/GenizahSearch/deploy.sh` | Update script |

---

## Environment Variables

The `.env` file should contain:

```
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_ANON_KEY=eyJ...
```

**Note:** Never share these keys publicly.

---

## Regular Maintenance

### Weekly
- [ ] Check the website loads correctly
- [ ] Review any error reports from users

### Monthly
- [ ] Check disk space usage
- [ ] Review Supabase usage (dashboard → Project Settings → Usage)
- [ ] Update code if there are pending improvements

### When Needed
- [ ] Rebuild indexes after data updates
- [ ] SSL certificate renewal (automatic via Cloudflare)

---

## Getting Help

### For Technical Issues
1. Check the logs (see "View Recent Activity/Logs" above)
2. Try restarting the service
3. Consult the Technical Guide (`DEPLOYMENT_TECHNICAL.md`)
4. Ask an AI assistant (Claude, ChatGPT) with the error message

### For AWS Issues
Contact the AWS account owner - you don't have direct AWS console access.

### For Code Issues
Check GitHub: https://github.com/gershuni/GenizahSearch

---

## Quick Commands Cheat Sheet

```bash
# Connect to server
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com

# Once connected, you can run:

# Check status
sudo systemctl status genizah-web

# Restart website
sudo systemctl restart genizah-web

# View logs
sudo journalctl -u genizah-web -n 50

# Deploy updates
cd /home/ubuntu/GenizahSearch && ./deploy.sh
```

---

## Server Management with Cockpit (Easy Mode)

Cockpit is a web-based tool that lets you manage the server through your browser - no command line needed!

### How to Access
1. Go to: **https://admin.genizahsearch.com**
2. Login with username: `ubuntu`
3. Use the password you set (see below if you haven't set one)

### First Time Setup
Set a password for Cockpit login:
```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "sudo passwd ubuntu"
```
Enter a password when prompted (you'll type it twice).

### What Each Section Does

| Section | What It's For | When to Use |
|---------|---------------|-------------|
| **Overview** | See CPU, memory, disk at a glance | Check if server is healthy |
| **Services** | Start/stop/restart programs | Restart website if broken |
| **Logs** | See what's happening | Troubleshoot problems |
| **Storage** | Check disk space | Make sure disk isn't full |
| **Terminal** | Run commands in browser | When you can't use SSH |

### Most Common Task: Restart the Website

1. Go to **Services**
2. Search for `genizah`
3. Click on `genizah-web`
4. Click the **Restart** button (circular arrow)

### What to Ignore
You can safely ignore these sections - they're for advanced users:
- Networking
- Accounts
- Software Updates (ask before updating)
- podman/Containers

---

## Supabase Dashboard Guide

### Viewing User Data

1. Go to https://supabase.com/dashboard
2. Select GenizahSearch project
3. Click **Table Editor**

**Tables:**
| Table | Contains |
|-------|----------|
| `profiles` | User profiles and roles |
| `user_lists` | Personal manuscript lists |
| `list_items` | Items in each list |
| `corrections` | Transcription corrections |
| `comments` | User comments |
| `discoveries` | Community discoveries |

### Database Backups

Supabase automatically backs up the database daily. To restore:
1. Go to **Settings** → **Database**
2. Scroll to **Backups**
3. Click **Restore** on the desired backup

---

## Emergency Contacts

| Issue Type | Who to Contact |
|------------|----------------|
| Website code bugs | GitHub Issues |
| Server/AWS issues | AWS account owner |
| Domain/DNS issues | Cloudflare dashboard |
| Database issues | Supabase dashboard/support |

---

## What Changed (January 2026)

The architecture was simplified by migrating to Supabase:

**Removed:**
- FastAPI backend server (port 8000)
- Local PostgreSQL/SQLite database
- Complex backend services

**Added:**
- Direct Supabase connection from web app
- Cloud-hosted database with automatic backups
- Built-in authentication and rate limiting

**Benefits:**
- Simpler deployment (one service instead of two)
- Better data safety (cloud backups)
- Less maintenance required

---

*For technical details, see `DEPLOYMENT_TECHNICAL.md`*
