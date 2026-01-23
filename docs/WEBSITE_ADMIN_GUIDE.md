# GenizahSearch Website Admin Guide

> Last updated: 2026-01-23
> For: Site administrators and non-technical users

---

## Quick Reference Card

| What | Where/How |
|------|-----------|
| **Website URL** | https://genizahsearch.com |
| **API Documentation** | https://genizahsearch.com/api/docs |
| **Server Management** | https://admin.genizahsearch.com (Cockpit) |
| **Server Access** | `ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com` |
| **Cloudflare Dashboard** | https://dash.cloudflare.com |

---

## Common Tasks

### 1. Check if the Website is Working

**From your browser:**
- Visit https://genizahsearch.com
- The page should load within a few seconds

**From command line:**
```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "sudo systemctl status genizah-backend genizah-web"
```
Look for "Active: active (running)" in green.

---

### 2. Restart the Website (if something is wrong)

If the website is slow or not responding:

```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "sudo systemctl restart genizah-backend genizah-web"
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

### 4. Create an Admin User

To create a new admin account:

```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "cd /home/ubuntu/GenizahSearch && source venv/bin/activate && python create_admin.py EMAIL PASSWORD 'FULL NAME'"
```

Replace:
- `EMAIL` with the user's email
- `PASSWORD` with a secure password
- `FULL NAME` with their name

**Example:**
```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "cd /home/ubuntu/GenizahSearch && source venv/bin/activate && python create_admin.py john@university.edu MySecurePass123 'John Smith'"
```

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
1. Restart the services:
   ```bash
   ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "sudo systemctl restart genizah-backend genizah-web"
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
2. Restart services:
   ```bash
   ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "sudo systemctl restart genizah-backend genizah-web"
   ```

---

### Can't connect via SSH

**What it means:** The server might be down or there's a network issue.

**What to do:**
1. Check if the website loads in browser (https://genizahsearch.com)
2. If website works but SSH doesn't, contact the AWS account owner
3. If nothing works, the server may need to be restarted from AWS console

---

### Search not returning results

**What it means:** The search index might be corrupted or not loaded.

**How to fix:**
1. Restart services first:
   ```bash
   ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "sudo systemctl restart genizah-backend genizah-web"
   ```
2. If still not working, rebuild the index (this takes ~1 hour):
   ```bash
   ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com "cd /home/ubuntu/GenizahSearch && source venv/bin/activate && nohup python build_index.py > index_rebuild.log 2>&1 &"
   ```

---

## Understanding the System

### How the Website Works

```
User's Browser
     │
     ▼
Cloudflare (DNS + Security + SSL)
     │
     ▼
Nginx (Web Server on port 80/443)
     │
     ├─── /api/* ───► Backend (FastAPI on port 8000)
     │                    │
     │                    ▼
     │               PostgreSQL Database
     │
     └─── /* ───────► Frontend (NiceGUI on port 8081)
                          │
                          ▼
                     Tantivy Search Index
```

### Key Components

| Component | What it does |
|-----------|--------------|
| **Cloudflare** | Protects the site, provides SSL, caches content |
| **Nginx** | Routes traffic to the right service |
| **Backend (FastAPI)** | Handles user accounts, corrections, comments |
| **Frontend (NiceGUI)** | The visual interface users see |
| **PostgreSQL** | Stores user data, corrections, comments |
| **Tantivy Index** | Powers the manuscript search |

---

## Important Files & Locations

| File/Folder | Purpose |
|-------------|---------|
| `/home/ubuntu/GenizahSearch/` | Main application folder |
| `/home/ubuntu/GenizahSearch/.env` | Configuration (passwords, etc.) |
| `/home/ubuntu/GenizahSearch/Genizah_Index/` | Search indexes |
| `/home/ubuntu/GenizahSearch/Transcriptions.txt` | Source manuscript data |
| `/home/ubuntu/GenizahSearch/deploy.sh` | Update script |

---

## Regular Maintenance

### Weekly
- [ ] Check the website loads correctly
- [ ] Review any error reports from users

### Monthly
- [ ] Check disk space usage
- [ ] Review logs for any recurring errors
- [ ] Update code if there are pending improvements

### When Needed
- [ ] Create new admin users
- [ ] Rebuild indexes after data updates
- [ ] SSL certificate renewal (automatic, but verify)

---

## Getting Help

### For Technical Issues
1. Check the logs (see "View Recent Activity/Logs" above)
2. Try restarting the services
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
sudo systemctl status genizah-backend genizah-web

# Restart everything
sudo systemctl restart genizah-backend genizah-web

# View logs
sudo journalctl -u genizah-web -n 50

# Deploy updates
cd /home/ubuntu/GenizahSearch && ./deploy.sh

# Create admin user
cd /home/ubuntu/GenizahSearch && source venv/bin/activate && python create_admin.py email password "Name"
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
3. Click on `genizah-backend`
4. Click the **Restart** button (circular arrow)
5. Repeat for `genizah-web`

### What to Ignore
You can safely ignore these sections - they're for advanced users:
- Networking
- Accounts
- Software Updates (ask before updating)
- podman/Containers

---

## Emergency Contacts

| Issue Type | Who to Contact |
|------------|----------------|
| Website code bugs | GitHub Issues |
| Server/AWS issues | AWS account owner |
| Domain/DNS issues | Cloudflare dashboard |
