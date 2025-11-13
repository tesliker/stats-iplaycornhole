#!/bin/bash
# Database backup script for Fly.io PostgreSQL
# Run this before deploying to ensure data is backed up

set -e

echo "=========================================="
echo "Database Backup Script"
echo "=========================================="
echo ""

# Get the app name from fly.toml or use default
APP_NAME=${FLY_APP:-fly-cornhole}

echo "App: $APP_NAME"
echo "Timestamp: $(date -u +%Y%m%d_%H%M%S)"
echo ""

# Check if flyctl is available
if ! command -v flyctl &> /dev/null; then
    echo "ERROR: flyctl not found. Install it from https://fly.io/docs/getting-started/installing-flyctl/"
    exit 1
fi

# Check if authenticated
if ! flyctl auth whoami &> /dev/null; then
    echo "ERROR: Not authenticated. Run: flyctl auth login"
    exit 1
fi

# List available databases
echo "Checking for PostgreSQL databases..."
DB_LIST=$(flyctl mpg list --app "$APP_NAME" 2>&1 || echo "")

if echo "$DB_LIST" | grep -q "no Postgres apps found"; then
    echo "WARNING: No PostgreSQL databases found for app $APP_NAME"
    echo "If you're using SQLite, backups should be done via volume snapshots"
    echo "For PostgreSQL, create one first: flyctl mpg create --name cornhole-db --app $APP_NAME"
    exit 1
fi

# Create backup using Fly.io CLI
echo ""
echo "Creating backup via flyctl..."
if flyctl mpg backup --app "$APP_NAME"; then
    echo ""
    echo "✓ Backup completed successfully!"
    echo ""
    echo "To list backups: flyctl mpg backups --app $APP_NAME"
    echo "To restore: flyctl mpg restore <backup-id> --app $APP_NAME"
else
    echo ""
    echo "ERROR: Backup failed!"
    echo "Troubleshooting:"
    echo "  1. Check database exists: flyctl mpg list --app $APP_NAME"
    echo "  2. Verify app name is correct"
    echo "  3. Check Fly.io status: flyctl status --app $APP_NAME"
    exit 1
fi

