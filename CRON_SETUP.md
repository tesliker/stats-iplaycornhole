# Weekly Cron Setup for Season 11

## Overview
The application fetches Season 11 data weekly on Monday mornings to create weekly snapshots for comparison. We use two methods for reliability:

1. **APScheduler** (in-process): Runs when the machine is running
2. **External Cron Service** (backup): Calls the HTTP endpoint even if machine was stopped

## Current Schedule
- **Time**: Every Monday at 3:00 AM UTC
- **Equivalent**: 
  - Sunday 11:00 PM EST
  - Sunday 8:00 PM PST

## Setup External Cron (Recommended for Production)

Since Fly.io machines can auto-stop, we recommend using an external cron service as a backup.

### Option 1: cron-job.org (Free)

1. Go to https://cron-job.org (free account)
2. Create a new cron job:
   - **URL**: `https://stats.iplaycornhole.me/api/cron/weekly-fetch?token=YOUR_SECRET`
   - **Schedule**: Weekly, Monday, 03:00 UTC
   - **Method**: GET
3. Set `CRON_SECRET` environment variable in Fly.io:
   ```bash
   flyctl secrets set CRON_SECRET=your-random-secret-here --app fly-cornhole
   ```
4. Use the same secret in the cron job URL

### Option 2: EasyCron (Free tier available)

Similar setup to cron-job.org.

### Option 3: Manual Testing

You can manually trigger the weekly fetch:
```bash
curl "https://stats.iplaycornhole.me/api/cron/weekly-fetch?token=YOUR_SECRET"
```

## How Weekly Snapshots Work

Each Monday, a new snapshot is created using the week start date (Monday 00:00 UTC) as the `snapshot_date`. This means:

- **Week 1 (Jan 6-12)**: All snapshots have `snapshot_date = 2025-01-06 00:00:00`
- **Week 2 (Jan 13-19)**: All snapshots have `snapshot_date = 2025-01-13 00:00:00`
- And so on...

This allows you to:
- Compare player stats across different weeks
- See how rankings change week-to-week
- Track player progress over the season

## Verifying the Schedule

Check logs after a scheduled run:
```bash
flyctl logs --app fly-cornhole | grep -i "weekly\|scheduled\|cron"
```

## Troubleshooting

### Scheduler didn't run
1. Check if machine was running: `flyctl status --app fly-cornhole`
2. Check logs for errors: `flyctl logs --app fly-cornhole --limit 100`
3. Manually trigger via cron endpoint
4. Check APScheduler logs: Look for "Scheduler started" message

### Snapshots not being created separately
- Verify `snapshot_date` is using week start (Monday 00:00 UTC)
- Check database for multiple snapshots: Query by `bucket_id=11` and group by `snapshot_date`



