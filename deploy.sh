#!/bin/bash
# Genizah Search Deployment Script
# Usage: ./deploy.sh master-main <full commit id>
#
# Deploys EXACTLY the given commit, which must already be on origin/<branch>. Pass the id the
# publishing step reported, after the full CI passed on it: the server then runs the commit that was
# tested, even if origin/<branch> has moved on since. Without a valid id nothing is changed.
# One deploy at a time: a second one started while the first runs is refused (exit 3).

set -e

BRANCH=${1:-master-main}
EXPECT=${2:-}
cd "$(dirname "$(readlink -f "$0")")"

if ! printf '%s' "$BRANCH" | grep -Eq '^[A-Za-z0-9][A-Za-z0-9._/-]*$' ||
   ! printf '%s' "$EXPECT" | grep -Eq '^[0-9a-f]{40}$'; then
    echo "Usage: ./deploy.sh master-main <full 40-character commit id>" >&2
    echo "Deploys exactly that commit; it must already be on origin/master-main. Nothing was changed." >&2
    exit 2
fi
REMOTE_REF="refs/remotes/origin/$BRANCH"

# Held until this script exits, so fetch, checkout, install, restart and the record below are one step.
if ! command -v flock >/dev/null 2>&1; then
    echo "flock (util-linux) is needed to deploy. Nothing was changed." >&2
    exit 3
fi
exec 9>>"$HOME/.genizah-deploy.lock"
if ! flock -n 9; then
    echo "Another deploy is running. Nothing was changed." >&2
    exit 3
fi

echo "=== Genizah Search Deployment ==="
echo "Branch: $BRANCH"
echo "Commit: $EXPECT"
echo

echo "[1/4] Fetching and checking out the commit..."
git fetch origin "+refs/heads/$BRANCH:$REMOTE_REF"
if ! git merge-base --is-ancestor "$EXPECT^{commit}" "$REMOTE_REF" 2>/dev/null; then
    echo "$EXPECT is not on origin/$BRANCH. Nothing was changed." >&2
    exit 1
fi
git reset --hard "$EXPECT^{commit}"
if [ "$(git rev-parse HEAD)" != "$EXPECT" ]; then
    echo "The checkout is not $EXPECT after the reset; not restarting." >&2
    exit 1
fi

echo "[2/4] Activating virtual environment..."
source venv/bin/activate

echo "[3/4] Installing any new dependencies..."
# -c: every package, transitive ones included, at the version CI tested (requirements-lock.txt).
pip install -q -r requirements.txt -c requirements-lock.txt

echo "[4/4] Restarting services..."
# Record what is actually RUNNING, and only after the restart succeeded. A checkout is not a
# deployment: git reset --hard can land new code while the old process keeps serving, and
# scripts/deploy_pgp_sidecar.ps1 would then see a server whose HEAD contains the sidecar's
# revision while the reader in memory is older. This file is what that check reads, so it is
# cleared before the restart and written last, only on success. Deliberately not in git (see .gitignore).
rm -f .deployed_revision
sudo systemctl restart genizah-web

if systemctl is-active --quiet genizah-web; then
    git rev-parse HEAD > .deployed_revision
    echo "Running revision recorded: $(cat .deployed_revision)"
else
    echo "WARNING: genizah-web is not active after the restart; .deployed_revision cleared" >&2
    exit 1
fi

echo
echo "=== Deployment complete ==="
sudo systemctl status genizah-web --no-pager | grep -E '(Active|●)'
