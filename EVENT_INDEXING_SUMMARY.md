# Event Data Indexing - Implementation Summary

## What I've Built

### 1. Database Schema ✅
- **Event** table - Stores event info (name, type, date, location)
- **PlayerEventStats** table - Player performance per event
- **EventMatchup** table - Match results (who played who, scores)
- **EventStanding** table - Final event rankings

All tables include proper indexes for efficient querying.

### 2. Fetcher Functions ✅
Added to `fetcher.py`:
- `fetch_player_events_list()` - Get all events a player played
- `fetch_event_info()` - Get event metadata
- `fetch_event_player_stats()` - Get all player stats for an event
- `fetch_event_standings()` - Get final standings
- `fetch_bracket_data()` - Get match/bracket data
- `detect_event_type()` - Auto-detect event type (open/regional/signature)
- `extract_event_number()` - Extract "Open #2" number

### 3. Indexing Logic ✅
Created `event_indexer.py` with:
- `index_event()` - Index a single event (with deduplication)
- `index_player_events()` - Index all events for a player
- `index_season_events()` - Index all events for a season (main function)

**Strategy**: Goes through each player → gets their events → indexes each event once

### 4. MCP Tools ✅
Added 6 new tools to `mcp_routes.py`:

1. **`get_event_stats`** - Get top performers for a specific event
   - "Who were the most impressive from Open #1?"

2. **`get_player_event_history`** - Player's event history
   - "Show me Tommy Sliker's event history"

3. **`get_notable_wins`** - Find wins against high CPI opponents
   - "Who are Tommy Sliker's most notable wins?"

4. **`get_recent_event_performers`** - Recent standouts by event type
   - "Who has been doing well in regionals lately?"

5. **`search_events`** - Search events by name/type

6. Plus existing tools updated to use "2025-2026 Season" format

### 5. API Endpoint ✅
Added `/api/index-events/{bucket_id}` endpoint (admin only) to trigger indexing

## How to Use

### Step 1: Index Events

**Option A: Via API (Admin)**
```bash
curl -X POST https://fly-cornhole.fly.dev/api/index-events/11 \
  -u tesliker:outkast
```

**Option B: Direct Python**
```python
from event_indexer import index_season_events
await index_season_events(bucket_id=11)
```

### Step 2: Query via GPT/MCP

Once indexed, you can ask:

- **"Who were the most impressive statistically from Open #2?"**
  - Uses: `get_event_stats` with event_name="Open #2"

- **"Who are some of Tommy Sliker's most notable wins this season?"**
  - Uses: `get_notable_wins` with player_name="Tommy Sliker"

- **"Who has been doing really well in regionals lately?"**
  - Uses: `get_recent_event_performers` with event_type="regional"

## Indexing Efficiency

The system is designed to be efficient:

1. **Deduplication**: Checks if event already indexed before fetching
2. **Resume Capability**: Can stop and resume - skips already indexed events
3. **Batch Processing**: Processes players in batches
4. **Rate Limiting**: Built-in delays to respect API limits

## Data Flow

```
Player → fetch_player_events_list() → List of Event IDs
  ↓
For each Event ID:
  ↓
  Check: Already indexed? → Skip
  ↓
  No → Fetch:
    - Event info
    - Player stats (all players)
    - Bracket/matchups
    - Standings
  ↓
  Store in database
```

## Example Queries After Indexing

### "Who were the top 5 performers in Open #2?"
ChatGPT will use `get_event_stats` with:
```json
{
  "event_name": "Open #2",
  "limit": 5
}
```

### "Show me Tommy Sliker's notable wins"
ChatGPT will use `get_notable_wins` with:
```json
{
  "player_name": "Tommy Sliker",
  "min_opponent_cpi": 100,
  "season": 11
}
```

### "Who's been hot in regionals the last 30 days?"
ChatGPT will use `get_recent_event_performers` with:
```json
{
  "event_type": "regional",
  "days_back": 30,
  "season": 11
}
```

## Next Steps

1. **Test the indexing**:
   ```bash
   # Start your server
   uvicorn main:app --reload
   
   # In another terminal, trigger indexing (you'll need to be logged in)
   # Or use Python directly
   ```

2. **Index a small subset first**:
   - Modify `index_season_events()` to accept `limit_players` parameter
   - Test with 10-20 players first

3. **Once working, index full season**:
   - Run for all players in season 11
   - Monitor progress

4. **Test GPT queries**:
   - Once events are indexed, test the new tools in ChatGPT

## Notes

- Event indexing will take time (similar to player data fetching)
- Each event requires multiple API calls (info, stats, brackets, standings)
- The system automatically deduplicates, so you can run it multiple times safely
- Consider running indexing during off-peak hours

## Future Enhancements

- Add event indexing to weekly scheduler
- Add event search/filtering to web UI
- Add event performance charts
- Add "head-to-head" matchup queries
- Add event comparison tools



