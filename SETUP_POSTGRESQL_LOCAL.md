# Setting Up PostgreSQL Locally

## Why Use PostgreSQL Locally?

- **Better concurrency**: Run multiple indexing operations simultaneously (2-3 at once)
- **Production parity**: Same database as production, fewer surprises
- **Better performance**: Faster for large datasets and complex queries
- **No write locks**: SQLite has database-level write locks, PostgreSQL doesn't

## Installation (macOS)

### 1. Install PostgreSQL

```bash
# Using Homebrew (recommended)
brew install postgresql@16

# Start PostgreSQL service
brew services start postgresql@16
```

### 2. Create Database

```bash
# Create a database for the cornhole app
createdb cornhole_local

# Verify it was created
psql -l | grep cornhole_local
```

### 3. Set Environment Variable

You have a few options:

#### Option A: Export in Terminal (Temporary)
```bash
export DATABASE_URL='postgresql://localhost:5432/cornhole_local'
# Or with username:
export DATABASE_URL='postgresql://your_username@localhost:5432/cornhole_local'
```

#### Option B: Use .env File (Recommended)
Create a `.env` file in the project root:
```bash
echo 'DATABASE_URL=postgresql://localhost:5432/cornhole_local' > .env
```

Then load it when running:
```bash
# Install python-dotenv if not already installed
pip install python-dotenv

# The app should auto-load .env (check if main.py loads it)
```

#### Option C: Add to Shell Profile (Permanent)
```bash
# Add to ~/.zshrc or ~/.bash_profile
echo 'export DATABASE_URL="postgresql://localhost:5432/cornhole_local"' >> ~/.zshrc
source ~/.zshrc
```

### 4. Initialize Database Schema

```bash
# Start the app - it will auto-create tables via init_db()
python -m uvicorn main:app --reload
```

The app will automatically:
- Detect PostgreSQL from `DATABASE_URL`
- Create all tables via `init_db()`
- Add missing columns (like `region`)

### 5. Import Existing Data (Optional)

If you want to import your SQLite data:

```bash
# Export from SQLite
sqlite3 cornhole.db .dump > cornhole_dump.sql

# Convert SQLite dump to PostgreSQL format (manual cleanup may be needed)
# Or use a tool like pgloader:
brew install pgloader
pgloader sqlite://cornhole.db postgresql://localhost:5432/cornhole_local
```

## Switching Between SQLite and PostgreSQL

### Use SQLite (Default)
```bash
unset DATABASE_URL
# or
export DATABASE_URL=""
```

### Use PostgreSQL
```bash
export DATABASE_URL='postgresql://localhost:5432/cornhole_local'
```

## Verify It's Working

```bash
# Check which database is being used
python3 -c "
import os
db_url = os.getenv('DATABASE_URL', 'Not set')
if 'postgres' in db_url.lower():
    print('✓ Using PostgreSQL:', db_url)
else:
    print('✓ Using SQLite (default)')
"

# Start app and check logs
python -m uvicorn main:app --reload
# Look for: "Adding region column to players table (PostgreSQL)..." or "(SQLite)..."
```

## Benefits for Your Workflow

With PostgreSQL locally, you can:
- ✅ Run 2-3 indexing operations simultaneously
- ✅ Test production-like behavior
- ✅ Faster queries on large datasets
- ✅ Better for concurrent API requests while indexing

## Troubleshooting

### Connection Refused
```bash
# Check if PostgreSQL is running
brew services list | grep postgresql

# Start if not running
brew services start postgresql@16
```

### Permission Denied
```bash
# Check your PostgreSQL username
whoami

# Use that in DATABASE_URL:
export DATABASE_URL='postgresql://your_username@localhost:5432/cornhole_local'
```

### Database Doesn't Exist
```bash
# Create it
createdb cornhole_local

# Or connect and create manually
psql postgres
CREATE DATABASE cornhole_local;
\q
```


