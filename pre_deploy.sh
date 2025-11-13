#!/bin/bash
# Pre-deployment script: Backup database before deploying
# Usage: ./pre_deploy.sh [optional: flyctl deploy command]

set -e

echo "=========================================="
echo "Pre-Deployment Database Backup"
echo "=========================================="
echo ""

# Run backup
./backup_db.sh

echo ""
echo "Backup completed. Proceeding with deployment..."
echo ""

# If deploy command provided, run it
if [ "$1" = "deploy" ]; then
    echo "Running deployment..."
    flyctl deploy --app fly-cornhole
else
    echo "Backup complete. Run 'flyctl deploy --app fly-cornhole' when ready."
fi



