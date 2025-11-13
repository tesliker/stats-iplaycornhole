# Event Data Indexing Plan

## Goals
- Index per-event data for regionals, opens, and nationals (signatures)
- Enable GPT queries like:
  - "Who were the most impressive statistically from Open #1?"
  - "Who are some of Tommy Sliker's most notable wins this season?"
  - "Who has been doing really well in regionals lately?"

## API Endpoints Available

1. **Player Events List**: `https://api.iplayacl.com/api/v1/player-events-list/playerID/{playerID}/bucketID/{bucketID}`
   - Returns all events a player participated in for a season

2. **Event Info**: `https://api.iplayacl.com/api/v1/events/{eventID}`
   - Basic event metadata (name, date, type, location)

3. **Event Player Stats**: `https://api.iplayacl.com/api/v1/event-player-stats/{eventID}`
   - Individual player stats for that event

4. **Event Standings**: `https://api.iplayacl.com/api/v1/event-standings/{eventID}`
   - Final standings/rankings for the event

5. **Bracket Data**: `https://api.iplayacl.com/api/v1/bracket-data/{eventID}`
   - Match results, who played who, wins/losses

## Database Schema Design

### Events Table
```sql
CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    event_id INTEGER UNIQUE NOT NULL,
    event_name TEXT,
    event_type TEXT,  -- 'regional', 'open', 'national', 'signature'
    event_date DATE,
    location TEXT,
    bucket_id INTEGER,
    region TEXT,  -- 'us', 'canada'
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

### Player Event Stats Table
```sql
CREATE TABLE player_event_stats (
    id INTEGER PRIMARY KEY,
    event_id INTEGER,
    player_id INTEGER,
    rank INTEGER,
    pts_per_rnd FLOAT,
    dpr FLOAT,
    total_games INTEGER,
    wins INTEGER,
    losses INTEGER,
    win_pct FLOAT,
    rounds_played INTEGER,
    total_pts INTEGER,
    opponent_pts_per_rnd FLOAT,
    four_bagger_pct FLOAT,
    bags_in_pct FLOAT,
    bags_on_pct FLOAT,
    bags_off_pct FLOAT,
    created_at TIMESTAMP,
    UNIQUE(event_id, player_id)
);
```

### Event Matchups Table (from bracket data)
```sql
CREATE TABLE event_matchups (
    id INTEGER PRIMARY KEY,
    event_id INTEGER,
    round_number INTEGER,
    player1_id INTEGER,
    player2_id INTEGER,
    winner_id INTEGER,
    score TEXT,
    created_at TIMESTAMP
);
```

### Event Standings Table
```sql
CREATE TABLE event_standings (
    id INTEGER PRIMARY KEY,
    event_id INTEGER,
    player_id INTEGER,
    final_rank INTEGER,
    points FLOAT,
    created_at TIMESTAMP,
    UNIQUE(event_id, player_id)
);
```

## Indexing Strategy

### Approach 1: Player-Based Discovery (Recommended)
**Pros**: Efficient, leverages existing player data
**Cons**: Might miss events if player not in our DB

1. For each player in season 11:
   - Fetch their events list: `/player-events-list/playerID/{id}/bucketID/11`
   - For each event in the list:
     - Check if event already indexed
     - If not, fetch and store:
       - Event info
       - All player stats for event (not just this player)
       - Bracket data
       - Event standings

2. **Deduplication**: Check `events.event_id` before fetching

### Approach 2: Event Season List
- Use `/event-seasons-list` to get all events for a season
- Process each event

### Approach 3: Hybrid
- Start with player-based discovery
- Periodically check for new events via event-seasons-list
- Fill in any gaps

## Implementation Plan

### Phase 1: Database Schema
- [ ] Create Events table
- [ ] Create PlayerEventStats table
- [ ] Create EventMatchups table
- [ ] Create EventStandings table
- [ ] Add indexes for common queries

### Phase 2: Fetcher Functions
- [ ] `fetch_player_events_list(player_id, bucket_id)`
- [ ] `fetch_event_info(event_id)`
- [ ] `fetch_event_player_stats(event_id)`
- [ ] `fetch_event_standings(event_id)`
- [ ] `fetch_bracket_data(event_id)`
- [ ] `parse_event_data()` helper functions

### Phase 3: Indexing Logic
- [ ] `index_player_events(player_id, bucket_id)` - Index all events for a player
- [ ] `index_event(event_id)` - Index a single event (with deduplication)
- [ ] Background task to index all season 11 events
- [ ] Resume capability (skip already indexed events)

### Phase 4: MCP Tools
- [ ] `get_event_stats` - Get stats for a specific event
- [ ] `get_top_event_performers` - Top performers in an event
- [ ] `get_player_event_history` - Player's event history
- [ ] `get_notable_wins` - Find notable wins (high CPI opponents)
- [ ] `get_recent_regional_performers` - Recent regional standouts
- [ ] `search_events` - Search events by name, type, date

### Phase 5: Query Examples
- "Who were the most impressive from Open #1?"
- "Who are Tommy Sliker's most notable wins?"
- "Who's been doing well in regionals lately?"

## Event Type Detection

From event info, determine type:
- Event name patterns: "Open #", "Regional", "National", "Signature"
- Event type field (if available in API)
- Event size/number of players

## Performance Considerations

1. **Batch Processing**: Process events in batches
2. **Deduplication**: Check before fetching to avoid duplicate work
3. **Incremental Updates**: Only fetch new events
4. **Rate Limiting**: Add delays between API calls
5. **Resume Capability**: Track which events are indexed

## Data Relationships

```
Player (existing)
  └─> PlayerEventStats (player performance per event)
  └─> EventMatchups (who they played)

Event
  └─> PlayerEventStats (all players in event)
  └─> EventStandings (final rankings)
  └─> EventMatchups (all matches)
```

## Query Patterns

1. **Event-focused**: "Top performers in Open #1"
   - Query: Event → PlayerEventStats → Top by stat

2. **Player-focused**: "Tommy Sliker's notable wins"
   - Query: Player → EventMatchups → Filter wins → Check opponent CPI

3. **Type-focused**: "Recent regional performers"
   - Query: Events (type=regional) → Recent dates → Top PlayerEventStats

4. **Comparison**: "Who improved most between events?"
   - Query: Player → Multiple PlayerEventStats → Compare stats

## Next Steps

1. Create database schema
2. Build fetcher functions
3. Implement indexing logic
4. Add MCP tools
5. Test with GPT queries



