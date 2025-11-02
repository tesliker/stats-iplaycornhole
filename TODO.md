# TODO - Implementation Checklist

## Phase 1: Immediate (After Current Data Fetch Completes)

### 1. Dynamic Standings URL
- [ ] Create bucket_id to year range mapping function
- [ ] Update `fetch_standings()` in `fetcher.py` to use dynamic URL
- [ ] Test with multiple bucket_ids

### 2. Automated Data Fetching (Weekly Snapshots)
- [ ] **Database Schema Changes**:
  - [ ] Add `snapshot_date` field to Player model (DateTime, indexed)
  - [ ] Remove unique constraint on (player_id, bucket_id)
  - [ ] Add composite unique constraint on (player_id, bucket_id, snapshot_date)
  - [ ] Create database migration or update init_db()
- [ ] Update `update_player_data()` to always INSERT (not upsert) with snapshot_date
- [ ] Install/configure scheduler library (APScheduler)
- [ ] Create scheduler service module
- [ ] Implement: Season 11 weekly, others fetch once
- [ ] Add database table/field to track last fetch time per bucket_id
- [ ] Update queries to default to latest snapshot (ORDER BY snapshot_date DESC)
- [ ] For historical seasons (9, 10), fetch once (no weekly snapshots needed)
- [ ] Create endpoint to manually trigger fetch (admin only, optional)
- [ ] Remove/hide "Fetch Latest Data" button from public UI

### 3. Clickable Column Header Sorting
- [ ] Update table headers in `index.html` to be clickable
- [ ] Add visual indicators (↑↓ arrows) for sort direction
- [ ] Update `app.js` to handle header clicks
- [ ] Toggle between asc/desc on click
- [ ] Apply to columns: Rank, PPR, DPR, CPI, Win %, Overall Total, Total Games, Rounds Total
- [ ] Persist sort state when changing filters

### 4. Domain Configuration
- [ ] Configure Fly.io custom domain: stats.iplaycornhole.me
- [ ] Update DNS records (user will need to do this)
- [ ] Test domain routing

## Phase 2: Enhancements

### Database Indexes (if performance issues)
- [ ] Add index on (bucket_id, rank)
- [ ] Add index on (bucket_id, state)
- [ ] Add index on (bucket_id, skill_level)
- [ ] Add index on (bucket_id, pts_per_rnd)
- [ ] Add index on (player_id, bucket_id) - composite for lookups

### Error Handling
- [ ] Add retry logic with exponential backoff
- [ ] Store failed player IDs for retry
- [ ] Better error logging
- [ ] Alert on scheduler failures

### Progress Tracking (Admin)
- [ ] Add endpoint for fetch status
- [ ] Store progress in memory/database
- [ ] Admin dashboard or endpoint to view status

## Files to Modify

### fetcher.py
- Add bucket_id to year mapping
- Update `fetch_standings()` URL generation

### main.py
- Update `update_player_data()` to always INSERT with snapshot_date (no upsert for season 11)
- For season 11: Set snapshot_date = current date when fetching
- For historical seasons: Fetch once, snapshot_date optional or use season end date
- Modify queries to get latest snapshot by default (subquery or DISTINCT ON equivalent)
- Add scheduler initialization
- Remove public fetch endpoint or add auth
- Add admin fetch status endpoint

### database.py
- Add `snapshot_date` field to Player model
- Remove unique constraint on (player_id, bucket_id)
- Add composite unique constraint on (player_id, bucket_id, snapshot_date)
- Add index on (player_id, bucket_id, snapshot_date) for fast queries
- Add table/model to track fetch history (bucket_id, last_fetch_time, status)

### templates/index.html
- Make table headers clickable
- Add sort indicators
- Remove/hide fetch button

### static/app.js
- Add header click handlers
- Toggle sort logic
- Update sort UI state

### fly.toml
- Configure custom domain (if needed)

## Dependencies to Add
- [ ] APScheduler (for scheduled tasks)

