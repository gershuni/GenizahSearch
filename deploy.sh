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

# Record what is actually RUNNING, and only after the restart succeeded. A checkout is not a
# deployment: git reset --hard can land new code while the old process keeps serving, and
# scripts/deploy_pgp_sidecar.ps1 would then see a server whose HEAD contains the sidecar's
# revision while the reader in memory is older. This file is what that check reads, so it is
# written last and only on success. Deliberately not in git (see .gitignore).
if systemctl is-active --quiet genizah-web; then
    git rev-parse HEAD > .deployed_revision
    echo "Running revision recorded: $(cat .deployed_revision)"
else
    rm -f .deployed_revision
    echo "WARNING: genizah-web is not active after the restart; .deployed_revision cleared" >&2
    exit 1
fi

echo
echo "=== Deployment complete ==="
sudo systemctl status genizah-web --no-pager | grep -E '(Active|●)'
