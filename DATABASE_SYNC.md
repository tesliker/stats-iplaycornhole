# Database Sync with Fly.io (PostgreSQL)

This guide explains how to work with the PostgreSQL database on Fly.io for local indexing.

## Quick Start (Recommended)

**The easiest approach is to connect directly to the production database:**

```bash
# Get the database connection string
flyctl secrets list --app fly-cornhole | grep DATABASE_URL

# Set it locally
export DATABASE_URL='postgres://user:pass@host:port/dbname'

# Run your local app - it will use the production database
python -m uvicorn main:app --reload
```

With this approach:
- ✅ No push/pull needed - changes are immediate
- ✅ No local PostgreSQL setup required
- ✅ Simplest workflow
- ⚠️ **Be careful** - you're modifying production data directly

## Alternative: Pull → Index → Push

If you want to work with a local copy:

### 1. Pull Database

```bash
./pull_db.sh
```

This exports the PostgreSQL database to a SQL dump file (e.g., `cornhole_dump_20250101_120000.sql`).

### 2. Set Up Local PostgreSQL (Optional)

If you want a true local copy:

```bash
# Install PostgreSQL (if not already installed)
# macOS: brew install postgresql
# Linux: apt-get install postgresql

# Create local database
createdb cornhole_local

# Import the dump
psql cornhole_local < cornhole_dump_YYYYMMDD_HHMMSS.sql

# Set DATABASE_URL to local
export DATABASE_URL='postgres://localhost:5432/cornhole_local'
```

### 3. Run Indexing

```bash
python -m uvicorn main:app --reload
# Visit http://localhost:8000/admin
```

### 4. Push Database Back

```bash
./push_db.sh [dump_file.sql]
```

This will:
- Create a backup on Fly.io
- Import your local database
- Replace the database on the remote server

## Manual Commands

### Get Database Connection String

```bash
# From secrets
flyctl secrets list --app fly-cornhole | grep DATABASE_URL

# From Postgres app
flyctl postgres connect -a <db-name>
```

### Export Database

```bash
# Via SSH (what pull_db.sh does)
flyctl ssh console --app fly-cornhole -C "pg_dump \$DATABASE_URL" > dump.sql

# Via flyctl proxy
flyctl proxy 5432 -a fly-cornhole &
pg_dump 'postgres://localhost:5432/dbname' > dump.sql
```

### Import Database

```bash
# Via SSH (what push_db.sh does)
flyctl ssh console --app fly-cornhole -C "psql \$DATABASE_URL" < dump.sql

# Via flyctl proxy
flyctl proxy 5432 -a fly-cornhole &
psql 'postgres://localhost:5432/dbname' < dump.sql
```

### Create Backup

```bash
# Via flyctl (recommended)
flyctl postgres backups create -a <db-name>

# Manual dump
flyctl ssh console --app fly-cornhole -C "pg_dump \$DATABASE_URL" > backup.sql
```

## Troubleshooting

### Cannot find DATABASE_URL

```bash
# Check secrets
flyctl secrets list --app fly-cornhole

# Check attached databases
flyctl postgres list --app fly-cornhole

# Set if needed
flyctl secrets set DATABASE_URL='postgres://...' --app fly-cornhole
```

### Connection issues

- Check app is running: `flyctl status --app fly-cornhole`
- Test connection: `flyctl postgres connect -a <db-name>`
- Check network/firewall settings

### Large database operations

- Use compression: `pg_dump | gzip > dump.sql.gz`
- Be patient - large operations can take time
- Consider using `flyctl postgres backups` for automated backups

## Best Practices

1. **Use DATABASE_URL directly** - Simplest and most reliable
2. **Always backup before pushing** - Scripts do this automatically
3. **Test on small datasets first** - If possible
4. **Restart app after changes** - `flyctl apps restart fly-cornhole`
5. **Monitor database size** - Large databases take longer to sync

## Notes

- PostgreSQL dumps are not compatible with SQLite
- Using `DATABASE_URL` to connect directly is the recommended approach
- Scripts handle backups automatically, but manual backups are recommended
- Database operations can take time for large datasets
