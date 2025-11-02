# Database Schema Changes for Weekly Snapshots

## Overview
To support trending data over time, we need to store weekly snapshots of player data instead of replacing it.

## Current Schema Issue
- Unique constraint on `(player_id, bucket_id)` prevents storing multiple snapshots
- `update_player_data()` uses upsert logic (updates existing records)

## Required Changes

### 1. Player Model Changes (`database.py`)

**Add:**
```python
snapshot_date = Column(DateTime, index=True, nullable=False)
```

**Modify:**
- Remove `unique=True` from `player_id` column
- Remove unique constraint on `(player_id, bucket_id)`
- Add composite unique constraint: `UniqueConstraint('player_id', 'bucket_id', 'snapshot_date')`

### 2. Query Strategy

**Default queries (latest snapshot):**
```python
# Get latest snapshot per player
subquery = select(
    Player.player_id,
    Player.bucket_id,
    func.max(Player.snapshot_date).label('latest_date')
).group_by(Player.player_id, Player.bucket_id).subquery()

query = select(Player).join(
    subquery,
    and_(
        Player.player_id == subquery.c.player_id,
        Player.bucket_id == subquery.c.bucket_id,
        Player.snapshot_date == subquery.c.latest_date
    )
)
```

**Or simpler approach for SQLite:**
```python
# Use window function or subquery to get latest per player
# SQLite 3.25+ supports window functions
query = select(Player).where(
    Player.snapshot_date == select(func.max(Player.snapshot_date))
    .where(
        and_(
            Player.player_id == player_id,
            Player.bucket_id == bucket_id
        )
    )
)
```

### 3. Fetch Logic Changes (`main.py`)

**For Season 11 (weekly snapshots):**
- Always INSERT new records (never update)
- Set `snapshot_date = datetime.utcnow()` or fetch date
- Each week creates ~10k new records

**For Historical Seasons (9, 10):**
- Fetch once only
- Set `snapshot_date = season_end_date` or null
- Can still use upsert since data never changes

### 4. Migration Strategy

**Option A: Fresh Start (Recommended for now)**
- Drop existing table
- Recreate with new schema
- Re-fetch all data (historical seasons once, season 11 weekly going forward)

**Option B: Migration Script**
- Add `snapshot_date` column
- Set existing records to a default date (e.g., today)
- Remove old unique constraint
- Add new composite constraint

### 5. Index Strategy

**Recommended indexes:**
```python
Index('idx_player_bucket_date', Player.player_id, Player.bucket_id, Player.snapshot_date)
Index('idx_bucket_date', Player.bucket_id, Player.snapshot_date)
Index('idx_snapshot_date', Player.snapshot_date)
```

### 6. Data Growth Estimate

**Season 11 (weekly for ~10 months = ~40 weeks):**
- ~10,000 players × 40 weeks = ~400,000 records per season

**Storage:**
- ~1KB per player record
- ~400MB for season 11 snapshots
- SQLite handles this fine (<10 visitors/day)

### 7. Future Query Examples

**Get player's stats over time:**
```python
query = select(Player).where(
    and_(
        Player.player_id == player_id,
        Player.bucket_id == 11
    )
).order_by(Player.snapshot_date.asc())
```

**Get trends (compare latest vs previous):**
```python
# Latest snapshot
latest = get_latest_snapshot(player_id, bucket_id)

# Previous snapshot (7 days ago)
previous = get_snapshot_by_date(player_id, bucket_id, date_7_days_ago)

# Calculate trend
trend = latest.pts_per_rnd - previous.pts_per_rnd
```

### 8. API Changes

**Default behavior (no change):**
- `/api/players` returns latest snapshot per player
- Queries use latest snapshot_date

**Future endpoints:**
- `/api/players/{id}/history` - Get all snapshots for a player
- `/api/players/{id}/trends` - Calculate trends over time
- `/api/players?snapshot_date=2025-01-15` - Get snapshot from specific date

