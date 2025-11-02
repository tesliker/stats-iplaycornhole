# Cornhole Stats App - Planning & Roadmap

## Current Status ✅
- Basic app structure complete
- Data fetching in progress
- Database schema defined
- Basic UI with filtering/sorting
- Season comparison charts

## Immediate Requirements (Post Data Fetch)

### 1. Automated Data Fetching System ⭐ HIGH PRIORITY
**Requirements**:
- Season 11 (current): Auto-fetch **weekly** (store historical snapshots)
- Historical seasons (9, 10, etc.): Fetch once only (data never changes)
- **NEW APPROACH**: Store each week's data as a separate snapshot (don't replace/upsert)
- This allows trending/charting over time (to be implemented later)
- Remove or hide "Fetch Latest Data" button from public UI
- Background scheduler (cron-like or periodic task)

**Implementation**:
- **Database Schema Change**: Add `snapshot_date` field to Player model
- Remove unique constraint on (player_id, bucket_id)
- Add composite unique constraint on (player_id, bucket_id, snapshot_date)
- Default queries get latest snapshot (ORDER BY snapshot_date DESC LIMIT 1 per player)
- Create scheduler service (APScheduler) - weekly for bucket_id 11
- Store last fetch timestamp per bucket_id
- For historical seasons, check if data exists, if not fetch once (no snapshot date needed)

### 2. Dynamic Standings URL ⭐ HIGH PRIORITY
**Issue**: Currently hardcoded to 2025-2026
- Map: bucket_id 11 → 2025-2026, bucket_id 10 → 2024-2025, etc.
- **Location**: `fetcher.py` - `fetch_standings()` function
- Pattern: `https://mysqlvm.blob.core.windows.net/acl-standings/{year_range}/acl-overall-standings.json`

### 3. Clickable Column Header Sorting ⭐ HIGH PRIORITY
**Feature**: Click column headers to sort
- Click once: Sort ascending
- Click again: Toggle to descending
- Apply to stat columns: Rank, PPR, DPR, CPI, Win %, Overall Total, Total Games, etc.
- Visual indicator (arrow up/down) showing current sort
- **Location**: Frontend `app.js` and table headers in `index.html`

### 4. Domain Setup
**Requirement**: stats.iplaycornhole.me
- Configure Fly.io custom domain
- Update CORS if needed
- SSL certificate (auto-handled by Fly.io)

## Priority 1: Critical Fixes & Improvements

### ~~1. Dynamic Standings URL Based on Season~~ → MOVED TO IMMEDIATE REQUIREMENTS

### 2. Data Fetch Progress Tracking (Optional - Admin Only)
**Enhancement**: Users can't see progress of the data fetch
- Add API endpoint to check fetch status (`GET /api/fetch-status/{bucket_id}`)
- Store progress in memory or database (current player being processed, total count, percentage)
- Update frontend to poll and display progress bar
- Show estimated time remaining

### 3. Error Handling & Retry Logic
**Enhancement**: Better handling of API failures
- Add retry logic with exponential backoff for failed player stats requests
- Store failed player IDs for later retry
- Log errors more comprehensively
- Add endpoint to retry failed fetches

### 4. Rate Limiting Protection
**Issue**: May hit API rate limits with 10k+ requests
- Increase delay between requests (currently 0.1s)
- Add configurable batch size and delays
- Implement exponential backoff on 429 errors
- Consider batching or queuing system

## Priority 2: Feature Enhancements

### 5. Enhanced Player Detail Page
**Feature**: Dedicated page for individual player
- Route: `/players/{player_id}?bucket_id={bucket_id}`
- Show comprehensive stats (all performance metrics, win/loss breakdown)
- Display chart of stats over time (if multiple seasons available)
- Show event history or notable achievements

### 6. Advanced Filtering
**Enhancement**: More filter options
- Filter by conference name (not just ID)
- Filter by membership type/level
- Range filters (e.g., PPR between X and Y, CPI above threshold)
- Multiple skill level selection
- Filter by win percentage ranges

### 7. Enhanced Charts & Visualizations
**Enhancement**: More chart options
- Player vs Player comparison (select multiple players)
- Top N players chart (e.g., top 10 by PPR)
- Distribution charts (e.g., PPR distribution, state distribution)
- Heatmap by state/conference
- Trend analysis (how stats change over seasons)

### 8. Export Functionality
**Feature**: Allow data export
- Export filtered results to CSV/JSON
- Export player comparison data
- Bulk export options

### 9. Search Improvements
**Enhancement**: Better search
- Fuzzy search (typo tolerance)
- Search by player ID
- Autocomplete/suggestions
- Search history

### 10. Season Management
**Enhancement**: Better season handling
- Auto-detect available seasons from API
- Show which seasons have data loaded
- Bulk fetch for multiple seasons
- Season comparison at aggregate level

## Priority 3: Performance & UX

### 11. Pagination Improvements
**Enhancement**: Better pagination
- Show more page size options (25, 50, 100, 200, 500)
- Jump to page number
- Remember user's preferred page size
- Virtual scrolling for large datasets

### 12. Caching Strategy
**Enhancement**: Reduce API calls
- Cache standings data (refresh every X hours)
- Cache player stats with TTL
- Browser-side caching for filter options
- Redis for production (if moving to Fly.io Postgres)

### 13. Loading States
**Enhancement**: Better UX during operations
- Skeleton loaders for table
- Loading spinners for individual actions
- Optimistic UI updates
- Disable actions during fetch

### 14. Data Refresh/Update
**Feature**: Incremental updates
- Track last update time per player
- Incremental refresh (only update changed players)
- Background refresh scheduler
- Manual refresh button

## Priority 4: Production Readiness

### 15. Database Optimization
**Enhancement**: Performance improvements
- Add indexes on frequently queried fields (rank, pts_per_rnd, dpr, player_cpi, state, skill_level)
- Database connection pooling
- Query optimization
- Consider PostgreSQL migration path

### 16. Monitoring & Logging
**Feature**: Observability
- Structured logging
- Error tracking (Sentry integration)
- Performance metrics
- Data fetch monitoring

### 17. Authentication (Optional)
**Feature**: If needed for production
- Basic auth or API keys for admin endpoints
- Public read, protected write

### 18. Fly.io Specific
**Enhancement**: Production deployment
- Persistent volume for SQLite (or PostgreSQL)
- Health check endpoint
- Graceful shutdown handling
- Resource limits configuration
- Background worker for data fetching (separate from web)

### 19. Documentation
**Enhancement**: Better docs
- API documentation (OpenAPI/Swagger - FastAPI auto-generates this)
- User guide
- Development setup guide
- Deployment guide

## Priority 5: Nice-to-Have Features

### 20. Player Alerts/Notifications
**Feature**: Track specific players
- Watchlist functionality
- Alert when player stats change significantly
- Weekly digest emails (future)

### 21. Leaderboards
**Feature**: Pre-defined views
- Top 10 by various metrics
- State rankings
- Conference rankings
- Skill level rankings

### 22. Data Validation
**Enhancement**: Ensure data quality
- Validate API responses
- Flag suspicious data (outliers)
- Data integrity checks

### 23. Mobile App / PWA
**Feature**: Mobile experience
- Progressive Web App support
- Mobile-optimized UI
- Offline capability

## Technical Debt

### Code Organization
- Split `main.py` into separate route files (players, stats, admin)
- Create service layer for business logic
- Better separation of concerns

### Testing
- Unit tests for fetcher
- Integration tests for API endpoints
- Frontend tests

### Configuration
- Environment-based config (dev/staging/prod)
- Config file for API URLs, delays, batch sizes

## Implementation Order Recommendation

**Phase 1 (Immediate - Post Current Fetch)**:
1. ✅ Dynamic standings URL mapping (bucket_id → year range)
2. ✅ Automated scheduler (season 11 every 2 days, others once)
3. ✅ Clickable column header sorting (toggle asc/desc)
4. ✅ Remove/hide manual fetch button from public UI
5. Domain configuration (stats.iplaycornhole.me)

**Phase 2 (Short-term)**:
6. Progress tracking (admin endpoint only)
7. Enhanced error handling (#3)
8. Database indexes (#15)

**Phase 2 (Short-term)**:
5. Player detail page (#5)
6. Advanced filtering (#6)
7. Enhanced charts (#7)
8. Better pagination (#11)

**Phase 3 (Medium-term)**:
9. Export functionality (#8)
10. Caching strategy (#12)
11. Data refresh (#14)
12. Production optimizations (#15-18)

**Phase 4 (Long-term)**:
13. Leaderboards (#21)
14. Mobile/PWA (#23)
15. Testing suite

## Answers & Decisions ✅

1. **Standings URL Pattern**: ✅ Confirmed - bucket_id maps to season (11 = 2025-2026, 10 = 2024-2025, etc.)
2. **API Rate Limits**: Unknown, but we'll be conservative with delays (0.1s+ between requests)
3. **Data Update Frequency**: Season 11 updates regularly, historical seasons static → Fetch season 11 every 2 days, others once
4. **Storage**: ✅ SQLite is fine - low traffic (<10 visitors/day), no concurrent writes during fetch
5. **Deployment**: Single machine is fine - scheduled tasks can run on web server

## Technical Notes

### Scheduler Options
- **Option A**: APScheduler (in-process, simple, works for single machine)
- **Option B**: Fly.io cron jobs (separate process, more reliable for production)
- **Recommendation**: Start with APScheduler, can migrate to Fly cron later

### Bucket ID to Year Mapping
```python
BUCKET_YEAR_MAP = {
    11: "2025-2026",
    10: "2024-2025",
    9: "2023-2024",
    # Add more as needed
}
```

### Database Considerations
- SQLite fine for this use case (read-heavy, low concurrency)
- **Schema Change Required**: Add `snapshot_date` to track weekly snapshots
- Remove unique constraint on (player_id, bucket_id)
- Add composite unique (player_id, bucket_id, snapshot_date) to prevent duplicates
- Default queries: Get latest snapshot (ORDER BY snapshot_date DESC)
- Historical queries: Filter by snapshot_date range for trending
- Can add indexes later for performance (player_id, bucket_id, snapshot_date)

### Weekly Snapshot Strategy
- Each week, create new records with `snapshot_date = fetch_date`
- For season 11: Store all weekly snapshots for trending analysis
- For historical seasons (9, 10): Single snapshot (no snapshot_date or use season end date)
- Default view: Show latest snapshot only
- Future: Add "View Trends" feature to show progression over time

