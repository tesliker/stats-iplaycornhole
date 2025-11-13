#!/bin/bash
# Push database to Fly.io instance
# Automatically detects SQLite or PostgreSQL and handles accordingly

set -e

echo "=========================================="
echo "Push Database to Fly.io"
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

# Check if app is running
echo "Checking app status..."
APP_STATUS=$(flyctl status --app "$APP_NAME" 2>/dev/null | grep -c "started" || echo "0")

if [ "$APP_STATUS" -eq 0 ]; then
    echo "App is not running. Starting it..."
    flyctl apps start "$APP_NAME" > /dev/null 2>&1
    sleep 5
    echo "✓ App started"
    echo ""
fi

# Detect database type
echo "Detecting database type..."
echo ""

# Check for DATABASE_URL (PostgreSQL)
DB_URL=$(flyctl ssh console --app "$APP_NAME" -C "echo \$DATABASE_URL" 2>/dev/null | grep -E "^postgres" | head -1 || echo "")

if [ -n "$DB_URL" ]; then
    echo "✓ Detected PostgreSQL database"
    DB_TYPE="postgresql"
    
    # Check if psql is available
    if ! command -v psql &> /dev/null; then
        echo ""
        echo "ERROR: psql not found locally."
        echo "Install PostgreSQL client tools:"
        echo "  macOS: brew install postgresql"
        echo "  Linux: apt-get install postgresql-client"
        exit 1
    fi
    
    # Find dump file
    DUMP_FILE=""
    if [ -n "$1" ]; then
        DUMP_FILE="$1"
    else
        # Find most recent dump file
        LATEST_DUMP=$(ls -t cornhole_dump_*.sql 2>/dev/null | head -1)
        if [ -n "$LATEST_DUMP" ]; then
            DUMP_FILE="$LATEST_DUMP"
            echo "Found dump file: $DUMP_FILE"
        fi
    fi
    
    if [ -z "$DUMP_FILE" ] || [ ! -f "$DUMP_FILE" ]; then
        echo ""
        echo "No PostgreSQL dump file found."
        echo ""
        echo "If you're using DATABASE_URL to connect directly to production,"
        echo "your changes are already in the database. No push needed."
        echo ""
        echo "To push a dump file, provide it as an argument:"
        echo "  ./push_db.sh cornhole_dump_YYYYMMDD_HHMMSS.sql"
        exit 0
    fi
    
    FILE_SIZE=$(du -h "$DUMP_FILE" | cut -f1)
    echo ""
    echo "WARNING: This will replace the database on Fly.io!"
    echo "Dump file: $DUMP_FILE ($FILE_SIZE)"
    echo ""
    read -p "Are you sure you want to push this database to Fly.io? (yes/no): " CONFIRM
    
    if [ "$CONFIRM" != "yes" ]; then
        echo "Aborted."
        exit 0
    fi
    
    # Create backup first
    echo ""
    echo "Creating backup on Fly.io..."
    # Try to create backup via flyctl or manual dump
    BACKUP_FILE="/tmp/cornhole_backup_$(date +%Y%m%d_%H%M%S).sql"
    if flyctl ssh console --app "$APP_NAME" -C "pg_dump \$DATABASE_URL" > "$BACKUP_FILE" 2>/dev/null; then
        echo "✓ Backup created locally: $BACKUP_FILE"
    else
        echo "⚠ Could not create automatic backup"
        echo "  Create manual backup: flyctl ssh console --app $APP_NAME -C 'pg_dump \$DATABASE_URL' > backup.sql"
    fi
    
    # Import the dump
    echo ""
    echo "Importing database to Fly.io..."
    echo "This may take a few minutes depending on database size..."
    echo ""
    
    # Parse connection string
    DB_HOST=$(echo "$DB_URL" | sed -n 's/.*@\([^:]*\):.*/\1/p')
    DB_PORT=$(echo "$DB_URL" | sed -n 's/.*:\([0-9]*\)\/.*/\1/p')
    
    # Try proxy method first
    if [ -n "$DB_PORT" ]; then
        echo "Using proxy method..."
        flyctl proxy "$DB_PORT" -a "$APP_NAME" > /tmp/fly_proxy.log 2>&1 &
        PROXY_PID=$!
        sleep 3
        
        if kill -0 $PROXY_PID 2>/dev/null; then
            LOCAL_DB_URL=$(echo "$DB_URL" | sed "s/@${DB_HOST}:${DB_PORT}/@localhost:${DB_PORT}/")
            if psql "$LOCAL_DB_URL" < "$DUMP_FILE" 2>&1 | tee /tmp/push_db_output.log; then
                kill $PROXY_PID 2>/dev/null || true
                if grep -qi "error" /tmp/push_db_output.log; then
                    echo "⚠ Some errors occurred during import. Check the output above."
                else
                    SUCCESS=true
                fi
            else
                kill $PROXY_PID 2>/dev/null || true
                SUCCESS=false
            fi
        else
            SUCCESS=false
        fi
    else
        SUCCESS=false
    fi
    
    # Fallback to SSH method
    if [ "$SUCCESS" != true ]; then
        echo "Trying SSH method..."
        if flyctl ssh console --app "$APP_NAME" -C "psql \$DATABASE_URL" < "$DUMP_FILE" 2>&1 | tee /tmp/push_db_output.log; then
            if grep -qi "error" /tmp/push_db_output.log; then
                echo "⚠ Some errors occurred during import. Check the output above."
            else
                SUCCESS=true
            fi
        else
            SUCCESS=false
        fi
    fi
    
    if [ "$SUCCESS" = true ]; then
        echo ""
        echo "✓ Database imported successfully!"
    else
        echo ""
        echo "ERROR: Failed to import database"
        echo "Check the output above for errors."
        exit 1
    fi
    
else
    echo "✓ Detected SQLite database (no DATABASE_URL found)"
    DB_TYPE="sqlite"
    
    # Check for local SQLite database
    LOCAL_DB_PATH="./cornhole.db"
    REMOTE_DB_PATH="/data/cornhole.db"
    REMOTE_BACKUP_PATH="/data/cornhole.db.backup.$(date +%Y%m%d_%H%M%S)"
    
    if [ ! -f "$LOCAL_DB_PATH" ]; then
        echo "ERROR: Local database not found at $LOCAL_DB_PATH"
        echo "Make sure you've run indexing locally first."
        exit 1
    fi
    
    FILE_SIZE=$(du -h "$LOCAL_DB_PATH" | cut -f1)
    echo ""
    echo "WARNING: This will replace the database on Fly.io!"
    echo "Local database: $LOCAL_DB_PATH ($FILE_SIZE)"
    echo "Remote path: $REMOTE_DB_PATH"
    echo ""
    read -p "Are you sure you want to push the database to Fly.io? (yes/no): " CONFIRM
    
    if [ "$CONFIRM" != "yes" ]; then
        echo "Aborted."
        exit 0
    fi
    
    # Create backup on remote
    echo ""
    echo "Creating backup on remote server..."
    if flyctl ssh console --app "$APP_NAME" -C "cp $REMOTE_DB_PATH $REMOTE_BACKUP_PATH 2>/dev/null || echo 'No existing database to backup'" > /dev/null 2>&1; then
        echo "✓ Backup created (or no existing database)"
    else
        echo "⚠ Could not create backup (this is OK if database doesn't exist yet)"
    fi
    
    # Push the database
    echo ""
    echo "Pushing database to Fly.io..."
    
    SUCCESS=false
    
    # Method 1: Try SFTP
    echo "Trying SFTP method..."
    MACHINE_ID=$(flyctl machine list --app "$APP_NAME" 2>/dev/null | awk 'NR>2 && /app/ {print $1; exit}' || echo "")
    if [ -n "$MACHINE_ID" ]; then
        flyctl machine start "$MACHINE_ID" --app "$APP_NAME" > /dev/null 2>&1
        sleep 15
    else
        flyctl apps start "$APP_NAME" > /dev/null 2>&1
        sleep 15
    fi
    
    if flyctl sftp put "$LOCAL_DB_PATH" "$REMOTE_DB_PATH" --app "$APP_NAME" 2>&1; then
        echo ""
        echo "✓ Database pushed successfully via SFTP!"
        SUCCESS=true
    fi
    
    # Method 2: Try SSH with base64 if SFTP failed
    if [ "$SUCCESS" != true ]; then
        echo "SFTP failed, trying SSH with base64 encoding..."
        MACHINE_ID=$(flyctl machine list --app "$APP_NAME" 2>/dev/null | awk 'NR>2 && /app/ {print $1; exit}' || echo "")
        if [ -n "$MACHINE_ID" ]; then
            flyctl machine start "$MACHINE_ID" --app "$APP_NAME" > /dev/null 2>&1
            sleep 15
        else
            flyctl apps start "$APP_NAME" > /dev/null 2>&1
            sleep 15
        fi
        
        echo "Encoding database file..."
        if base64 "$LOCAL_DB_PATH" > /tmp/db_base64.txt 2>/dev/null; then
            if flyctl ssh console --app "$APP_NAME" -C "base64 -d > $REMOTE_DB_PATH" < /tmp/db_base64.txt 2>&1; then
                echo "✓ Database pushed successfully via base64!"
                SUCCESS=true
            fi
            rm -f /tmp/db_base64.txt
        fi
    fi
    
    # Method 3: Try direct SSH cat if base64 failed
    if [ "$SUCCESS" != true ]; then
        echo "Base64 method failed, trying direct SSH method..."
        MACHINE_ID=$(flyctl machine list --app "$APP_NAME" 2>/dev/null | awk 'NR>2 && /app/ {print $1; exit}' || echo "")
        if [ -n "$MACHINE_ID" ]; then
            flyctl machine start "$MACHINE_ID" --app "$APP_NAME" > /dev/null 2>&1
            sleep 15
        else
            flyctl apps start "$APP_NAME" > /dev/null 2>&1
            sleep 15
        fi
        
        if flyctl ssh console --app "$APP_NAME" -C "cat > $REMOTE_DB_PATH" < "$LOCAL_DB_PATH" 2>&1; then
            echo "✓ Database pushed successfully via SSH!"
            SUCCESS=true
        fi
    fi
    
    if [ "$SUCCESS" != true ]; then
        echo ""
        echo "ERROR: All methods failed to push database"
        echo ""
        echo "Troubleshooting:"
        echo "  1. Check volume exists: flyctl volumes list --app $APP_NAME"
        echo "  2. Check write permissions: flyctl ssh console --app $APP_NAME -C 'ls -ld /data/'"
        echo "  3. Check app status: flyctl status --app $APP_NAME"
    fi
fi

if [ "$SUCCESS" = true ]; then
    echo ""
    echo "✓ Database push completed!"
    echo ""
    echo "You may want to restart the app: flyctl apps restart $APP_NAME"
else
    exit 1
fi
