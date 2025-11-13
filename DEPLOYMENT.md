# Deployment Guide

## Pre-Deployment Checklist

1. **Backup Database** (Always do this first!)
   ```bash
   ./backup_db.sh
   ```
   Or manually:
   ```bash
   flyctl mpg backup --app fly-cornhole
   ```

2. **Verify Changes**
   - Review code changes
   - Test locally if possible
   - Check for syntax errors

3. **Deploy**
   ```bash
   flyctl deploy --app fly-cornhole
   ```

## Quick Deploy with Backup

Use the pre-deploy script:
```bash
./pre_deploy.sh deploy
```

This will:
1. Backup the database automatically
2. Deploy the application

## Manual Backup and Deploy

```bash
# Step 1: Backup
./backup_db.sh

# Step 2: Deploy
flyctl deploy --app fly-cornhole
```

## Verifying Deployment

After deployment, check:
1. **App Status**: `flyctl status --app fly-cornhole`
2. **Logs**: `flyctl logs --app fly-cornhole --limit 50`
3. **Website**: Visit https://stats.iplaycornhole.me
4. **Health Check**: `curl https://stats.iplaycornhole.me/api/db-health` (requires auth)

## Database Backups

### List Backups
```bash
flyctl mpg backups --app fly-cornhole
```

### Restore from Backup
```bash
flyctl mpg restore <backup-id> --app fly-cornhole
```

### Automatic Backups
Fly.io Managed Postgres automatically creates backups, but you can also:
- Run `./backup_db.sh` before each deployment
- Set up a scheduled backup (via cron or external service)

## Weekly Schedule Setup

See [CRON_SETUP.md](./CRON_SETUP.md) for details on setting up the weekly Monday fetch.

## Emergency Rollback

If a deployment goes wrong:

1. **Stop the app** (if needed):
   ```bash
   flyctl apps suspend fly-cornhole
   ```

2. **Restore database** (if data was corrupted):
   ```bash
   flyctl mpg backups --app fly-cornhole
   flyctl mpg restore <backup-id> --app fly-cornhole
   ```

3. **Redeploy previous version**:
   ```bash
   flyctl releases --app fly-cornhole  # List releases
   flyctl releases deploy <release-id> --app fly-cornhole
   ```



