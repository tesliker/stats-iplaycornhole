#!/bin/bash
# Pull database from Fly.io instance
# Automatically detects SQLite or PostgreSQL and handles accordingly

set -e

echo "=========================================="
echo "Pull Database from Fly.io"
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
APP_STATUS=$(echo "$APP_STATUS" | tr -d '\n' | head -c 1)

if [ "$APP_STATUS" = "0" ] || [ -z "$APP_STATUS" ]; then
    echo "App is not running. Starting it..."
    flyctl apps start "$APP_NAME" > /dev/null 2>&1
    echo "Waiting for app to be ready..."
    sleep 10
    # Check again
    APP_STATUS=$(flyctl status --app "$APP_NAME" 2>/dev/null | grep -c "started" || echo "0")
    APP_STATUS=$(echo "$APP_STATUS" | tr -d '\n' | head -c 1)
    if [ "$APP_STATUS" != "0" ] && [ -n "$APP_STATUS" ]; then
        echo "✓ App started"
    else
        echo "⚠ App may still be starting, will try anyway..."
    fi
    echo ""
fi

# Detect database type
echo "Detecting database type..."
echo ""

# Check for DATABASE_URL (PostgreSQL)
# Try multiple methods to get DATABASE_URL
DB_URL=$(flyctl ssh console --app "$APP_NAME" -C 'printenv DATABASE_URL' 2>/dev/null | grep -E "^postgres" | head -1 || echo "")
if [ -z "$DB_URL" ]; then
    # Try via secrets
    DB_URL=$(flyctl secrets list --app "$APP_NAME" 2>/dev/null | grep DATABASE_URL | awk '{print $1}' | xargs -I {} flyctl ssh console --app "$APP_NAME" -C 'printenv {}' 2>/dev/null | grep -E "^postgres" | head -1 || echo "")
fi
if [ -z "$DB_URL" ]; then
    # Try Python method
    DB_URL=$(flyctl ssh console --app "$APP_NAME" -C 'python3 -c "import os; print(os.getenv(\"DATABASE_URL\", \"\"))"' 2>/dev/null | grep -E "^postgres" | head -1 || echo "")
fi

if [ -n "$DB_URL" ]; then
    echo "✓ Detected PostgreSQL database"
    DB_TYPE="postgresql"
    
    # Check if pg_dump is available
    if ! command -v pg_dump &> /dev/null; then
        echo ""
        echo "ERROR: pg_dump not found locally."
        echo "Install PostgreSQL client tools:"
        echo "  macOS: brew install postgresql"
        echo "  Linux: apt-get install postgresql-client"
        exit 1
    fi
    
    # Parse connection string
    # Format: postgresql://user:pass@host:port/dbname or postgresql://user:pass@host/dbname
    DB_NAME=$(echo "$DB_URL" | sed -n 's|.*/\([^?]*\).*|\1|p')
    # Extract host - could be @host:port or @host
    if echo "$DB_URL" | grep -q '@[^:]*:[0-9]'; then
        DB_HOST=$(echo "$DB_URL" | sed -n 's|.*@\([^:]*\):.*|\1|p')
        DB_PORT=$(echo "$DB_URL" | sed -n 's|.*@[^:]*:\([0-9]*\)/.*|\1|p')
    else
        DB_HOST=$(echo "$DB_URL" | sed -n 's|.*@\([^/]*\)/.*|\1|p')
        DB_PORT="5432"  # Default PostgreSQL port
    fi
    
    echo "Database: $DB_NAME"
    echo "Host: $DB_HOST"
    echo "Port: $DB_PORT"
    echo ""
    
    # Create dump file
    DUMP_FILE="./cornhole_dump_$(date +%Y%m%d_%H%M%S).sql"
    echo "Exporting PostgreSQL database to: $DUMP_FILE"
    echo "This may take a few minutes depending on database size..."
    echo ""
    
    # Try proxy method first
    if [ -n "$DB_PORT" ]; then
        echo "Using proxy method (local pg_dump via tunnel)..."
        # Use a different local port to avoid conflicts
        LOCAL_PROXY_PORT="6543"
        # Kill any existing proxy on this port
        pkill -f "flyctl proxy.*$LOCAL_PROXY_PORT" 2>/dev/null || true
        sleep 1
        
        flyctl proxy "$LOCAL_PROXY_PORT:$DB_HOST:$DB_PORT" -a "$APP_NAME" > /tmp/fly_proxy_${LOCAL_PROXY_PORT}.log 2>&1 &
        PROXY_PID=$!
        sleep 5  # Give proxy more time to establish
        
        if kill -0 $PROXY_PID 2>/dev/null; then
            # Replace host:port with localhost:proxy_port
            LOCAL_DB_URL=$(echo "$DB_URL" | sed "s|@${DB_HOST}:${DB_PORT}|@localhost:${LOCAL_PROXY_PORT}|" | sed "s|@${DB_HOST}/|@localhost:${LOCAL_PROXY_PORT}/|")
            echo "Attempting dump via tunnel to localhost:${LOCAL_PROXY_PORT}..."
            echo "Connection string: ${LOCAL_DB_URL%%@*}@localhost:${LOCAL_PROXY_PORT}/${DB_NAME}"
            if /opt/homebrew/opt/postgresql@16/bin/pg_dump --no-owner --no-acl "$LOCAL_DB_URL" > "$DUMP_FILE" 2>&1; then
                kill $PROXY_PID 2>/dev/null || true
                if head -1 "$DUMP_FILE" | grep -q "PostgreSQL database dump"; then
                    SUCCESS=true
                else
                    echo "Dump file doesn't look valid:"
                    head -5 "$DUMP_FILE"
                    SUCCESS=false
                fi
            else
                echo "pg_dump failed. Error:"
                tail -10 "$DUMP_FILE" 2>&1 || head -10 "$DUMP_FILE"
                kill $PROXY_PID 2>/dev/null || true
                SUCCESS=false
            fi
        else
            echo "Proxy failed to start. Log:"
            cat /tmp/fly_proxy_${LOCAL_PROXY_PORT}.log 2>&1 | tail -10
            SUCCESS=false
        fi
    else
        SUCCESS=false
    fi
    
    # Fallback to direct connection
    if [ "$SUCCESS" != true ]; then
        echo "Trying direct connection..."
        if pg_dump "$DB_URL" > "$DUMP_FILE" 2>&1; then
            if head -1 "$DUMP_FILE" | grep -q "PostgreSQL database dump"; then
                SUCCESS=true
            else
                SUCCESS=false
            fi
        else
            SUCCESS=false
        fi
    fi
    
    if [ "$SUCCESS" = true ]; then
        FILE_SIZE=$(du -h "$DUMP_FILE" | cut -f1)
        echo "✓ PostgreSQL database exported successfully!"
        echo "  File: $DUMP_FILE"
        echo "  Size: $FILE_SIZE"
    else
        echo "ERROR: Failed to export PostgreSQL database"
        echo "Check the dump file for errors:"
        head -30 "$DUMP_FILE"
        exit 1
    fi
    
else
    echo "✓ Detected SQLite database (no DATABASE_URL found)"
    DB_TYPE="sqlite"
    
    # Check for SQLite database file
    DB_PATH="/data/cornhole.db"
    LOCAL_DB_PATH="./cornhole.db"
    BACKUP_DB_PATH="./cornhole.db.backup.$(date +%Y%m%d_%H%M%S)"
    
    echo "Database path: $DB_PATH"
    echo ""
    
    # Backup existing local database if it exists
    if [ -f "$LOCAL_DB_PATH" ]; then
        echo "Backing up existing local database to $BACKUP_DB_PATH..."
        cp "$LOCAL_DB_PATH" "$BACKUP_DB_PATH"
        echo "✓ Local database backed up"
        echo ""
        # Remove the existing file so sftp can overwrite it
        rm -f "$LOCAL_DB_PATH"
    fi
    
    # Pull the database file
    echo "Pulling SQLite database from Fly.io..."
    echo "Remote: $DB_PATH"
    echo "Local: $LOCAL_DB_PATH"
    echo ""
    
    SUCCESS=false
    
    # Method 1: Try SFTP
    echo "Trying SFTP method..."
    flyctl apps start "$APP_NAME" > /dev/null 2>&1
    sleep 8
    
    if flyctl sftp get "$DB_PATH" "$LOCAL_DB_PATH" --app "$APP_NAME" 2>&1; then
        if [ -f "$LOCAL_DB_PATH" ] && [ -s "$LOCAL_DB_PATH" ]; then
            FILE_SIZE=$(du -h "$LOCAL_DB_PATH" | cut -f1)
            echo "✓ SQLite database pulled successfully via SFTP!"
            echo "  File: $LOCAL_DB_PATH"
            echo "  Size: $FILE_SIZE"
            SUCCESS=true
        fi
    fi
    
    # Method 2: Try SSH with base64 (more reliable for binary files)
    if [ "$SUCCESS" != true ]; then
        echo "SFTP failed, trying SSH with base64 encoding..."
        echo "Starting app machine..."
        # Get machine ID and start it directly (skip header, get first machine)
        # Machine list format: ID is first column, skip header lines
        MACHINE_ID=$(flyctl machine list --app "$APP_NAME" 2>/dev/null | awk 'NR>2 && /app/ {print $1; exit}' || echo "")
        if [ -n "$MACHINE_ID" ]; then
            echo "Starting machine $MACHINE_ID..."
            flyctl machine start "$MACHINE_ID" --app "$APP_NAME" > /dev/null 2>&1
            sleep 15
        else
            flyctl apps start "$APP_NAME" > /dev/null 2>&1
            sleep 15
        fi
        
        echo "Encoding database file (this may take a moment for large files)..."
        # Use a single SSH command that starts the machine if needed
        if flyctl ssh console --app "$APP_NAME" -C "base64 < $DB_PATH" > /tmp/db_base64.txt 2>&1; then
            if [ -f /tmp/db_base64.txt ] && [ -s /tmp/db_base64.txt ]; then
                # Check if it's actually base64 or an error message
                if head -1 /tmp/db_base64.txt | grep -qi "error\|command not found"; then
                    echo "Base64 command failed, trying direct cat..."
                    rm -f /tmp/db_base64.txt
                else
                    echo "Decoding base64 data..."
                    if base64 -d /tmp/db_base64.txt > "$LOCAL_DB_PATH" 2>&1; then
                        if [ -f "$LOCAL_DB_PATH" ] && [ -s "$LOCAL_DB_PATH" ]; then
                            # Verify it's a valid SQLite file
                            if head -c 16 "$LOCAL_DB_PATH" | grep -q "SQLite format" || [ $(stat -f%z "$LOCAL_DB_PATH" 2>/dev/null || stat -c%s "$LOCAL_DB_PATH" 2>/dev/null || echo 0) -gt 1000 ]; then
                                FILE_SIZE=$(du -h "$LOCAL_DB_PATH" | cut -f1)
                                echo "✓ SQLite database pulled successfully via base64!"
                                echo "  File: $LOCAL_DB_PATH"
                                echo "  Size: $FILE_SIZE"
                                SUCCESS=true
                            else
                                echo "WARNING: File may not be a valid SQLite database"
                                rm -f "$LOCAL_DB_PATH"
                            fi
                        fi
                    fi
                    rm -f /tmp/db_base64.txt
                fi
            fi
        fi
    fi
    
    # Method 3: Try direct SSH cat (may have issues with binary but worth trying)
    if [ "$SUCCESS" != true ]; then
        echo "Base64 method failed, trying direct SSH cat..."
        echo "Starting app machine..."
        # Get machine ID and start it directly (skip header, get first machine)
        # Machine list format: ID is first column, skip header lines
        MACHINE_ID=$(flyctl machine list --app "$APP_NAME" 2>/dev/null | awk 'NR>2 && /app/ {print $1; exit}' || echo "")
        if [ -n "$MACHINE_ID" ]; then
            echo "Starting machine $MACHINE_ID..."
            flyctl machine start "$MACHINE_ID" --app "$APP_NAME" > /dev/null 2>&1
            sleep 15
        else
            flyctl apps start "$APP_NAME" > /dev/null 2>&1
            sleep 15
        fi
        
        if flyctl ssh console --app "$APP_NAME" -C "cat $DB_PATH" > "$LOCAL_DB_PATH" 2>&1; then
            if [ -f "$LOCAL_DB_PATH" ] && [ -s "$LOCAL_DB_PATH" ]; then
                # Check file size - SQLite databases are usually > 1KB
                FILE_SIZE_BYTES=$(stat -f%z "$LOCAL_DB_PATH" 2>/dev/null || stat -c%s "$LOCAL_DB_PATH" 2>/dev/null || echo 0)
                if [ "$FILE_SIZE_BYTES" -gt 1000 ]; then
                    # Remove SSH connection messages from the beginning if present
                    echo "Cleaning SSH connection messages from file..."
                    python3 -c "
import sys
with open('$LOCAL_DB_PATH', 'rb') as f:
    data = f.read()
    start = data.find(b'SQLite format')
    if start > 0:
        with open('$LOCAL_DB_PATH', 'wb') as out:
            out.write(data[start:])
        print(f'Removed {start} bytes from start')
    elif start == 0:
        print('File is already clean')
    else:
        print('Warning: SQLite format header not found', file=sys.stderr)
" 2>&1
                    
                    FILE_SIZE=$(du -h "$LOCAL_DB_PATH" | cut -f1)
                    echo "✓ SQLite database pulled successfully via SSH cat!"
                    echo "  File: $LOCAL_DB_PATH"
                    echo "  Size: $FILE_SIZE"
                    SUCCESS=true
                else
                    # Check if it's an error message
                    if head -1 "$LOCAL_DB_PATH" | grep -qi "error"; then
                        echo "Error from server:"
                        head -5 "$LOCAL_DB_PATH"
                        rm -f "$LOCAL_DB_PATH"
                    fi
                fi
            fi
        fi
    fi
    
    if [ "$SUCCESS" != true ]; then
        echo ""
        echo "ERROR: All methods failed to pull SQLite database"
        echo ""
        echo "Troubleshooting:"
        echo "  1. Check volume exists: flyctl volumes list --app $APP_NAME"
        echo "  2. Check file exists: flyctl ssh console --app $APP_NAME -C 'ls -lh /data/'"
        echo "  3. Check app status: flyctl status --app $APP_NAME"
        echo "  4. Try manually: flyctl ssh console --app $APP_NAME"
        SUCCESS=false
    fi
fi

if [ "$SUCCESS" = true ]; then
    echo ""
    echo "✓ Database pull completed!"
    echo ""
    if [ "$DB_TYPE" = "postgresql" ]; then
        # Check if local PostgreSQL is configured
        LOCAL_DB_URL=$(grep -E "^DATABASE_URL=" .env 2>/dev/null | cut -d'=' -f2- | tr -d '"' || echo "")
        
        if [ -n "$LOCAL_DB_URL" ] && echo "$LOCAL_DB_URL" | grep -q "postgres.*localhost.*cornhole_local"; then
            echo "Detected local PostgreSQL configuration in .env"
            echo "Importing dump into local database..."
            echo ""
            
            # Find psql command (try postgresql@16 first, then system)
            PSQL_CMD=""
            if [ -f "/opt/homebrew/opt/postgresql@16/bin/psql" ]; then
                PSQL_CMD="/opt/homebrew/opt/postgresql@16/bin/psql"
            elif command -v psql &> /dev/null; then
                PSQL_CMD="psql"
            else
                echo "ERROR: psql not found. Cannot import database."
                echo "Install PostgreSQL: brew install postgresql@16"
                exit 1
            fi
            
            # Extract database name from LOCAL_DB_URL
            LOCAL_DB_NAME=$(echo "$LOCAL_DB_URL" | sed -n 's/.*\/\([^?]*\).*/\1/p')
            if [ -z "$LOCAL_DB_NAME" ]; then
                LOCAL_DB_NAME="cornhole_local"
            fi
            
            # Check if database exists, create if not
            if ! $PSQL_CMD -lqt | cut -d \| -f 1 | grep -qw "$LOCAL_DB_NAME"; then
                echo "Creating local database: $LOCAL_DB_NAME"
                if [ -f "/opt/homebrew/opt/postgresql@16/bin/createdb" ]; then
                    /opt/homebrew/opt/postgresql@16/bin/createdb "$LOCAL_DB_NAME"
                elif command -v createdb &> /dev/null; then
                    createdb "$LOCAL_DB_NAME"
                else
                    echo "ERROR: createdb not found"
                    exit 1
                fi
            fi
            
            # Drop existing tables/schema and import fresh
            echo "Dropping existing schema (if any)..."
            $PSQL_CMD -d "$LOCAL_DB_NAME" -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;" 2>&1 | grep -v "does not exist" || true
            
            echo "Importing database dump..."
            echo "This may take a few minutes..."
            if $PSQL_CMD -d "$LOCAL_DB_NAME" < "$DUMP_FILE" 2>&1; then
                echo ""
                echo "✓ Database imported successfully into local PostgreSQL!"
                echo "  Database: $LOCAL_DB_NAME"
                echo "  Dump file: $DUMP_FILE"
                echo ""
                echo "Next steps:"
                echo "  1. Restart your app: python -m uvicorn main:app --reload"
                echo "  2. Visit http://localhost:8000/admin"
                echo "  3. After indexing, use ./push_db.sh to push back"
            else
                echo ""
                echo "ERROR: Failed to import database dump"
                echo "Dump file saved at: $DUMP_FILE"
                echo "You can manually import it with:"
                echo "  psql $LOCAL_DB_NAME < $DUMP_FILE"
                exit 1
            fi
        else
            echo "Next steps:"
            echo "  1. To use locally with PostgreSQL:"
            echo "     createdb cornhole_local"
            echo "     psql cornhole_local < $DUMP_FILE"
            echo "     echo 'DATABASE_URL=postgresql://localhost:5432/cornhole_local' > .env"
            echo ""
            echo "  2. Or use production database directly:"
            echo "     export DATABASE_URL='$DB_URL'"
            echo "     python -m uvicorn main:app --reload"
            echo ""
            echo "  3. After indexing, use ./push_db.sh to push back"
        fi
    else
        echo "Next steps:"
        echo "  1. Run indexing locally (database is at $LOCAL_DB_PATH):"
        echo "     python -m uvicorn main:app --reload"
        echo "     Visit http://localhost:8000/admin"
        echo ""
        echo "  2. After indexing, use ./push_db.sh to push back"
    fi
else
    exit 1
fi
