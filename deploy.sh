#!/bin/bash
# Genizah Search Deployment Script
# Usage: ./deploy.sh [branch]

set -e

BRANCH=${1:-master-main}
cd /home/ubuntu/GenizahSearch

echo "=== Genizah Search Deployment ==="
echo "Branch: $BRANCH"
echo

echo "[1/3] Pulling latest code..."
git fetch origin
git reset --hard origin/$BRANCH

echo "[2/3] Activating virtual environment..."
source venv/bin/activate

echo "[3/3] Installing any new dependencies..."
pip install -q -r requirements.txt

echo "[4/4] Restarting services..."
sudo systemctl restart genizah-web

echo
echo "=== Deployment complete ==="
sudo systemctl status genizah-web --no-pager | grep -E '(Active|●)'
