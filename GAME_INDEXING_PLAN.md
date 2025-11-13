# Game/Match Data Indexing Plan

## Goal
Index individual game/match data for each event to enable queries like:
- "Who has thrown the most rounds in a match?"
- "What were the best games of Open #2?"
- "Show me the highest scoring games this season"

## API Structure

**Endpoint**: `https://api.iplayacl.com/api/v1/match-stats/eventid/{event_id}/matchid/{match_id}/gameid/{game_id}`

**Response includes**:
- Match info (matchID, gameID, status, scores)
- `event_match_details` array with per-player stats:
  - Player ID, name
  - Rounds played
  - Bags in/on/off
  - Total points, PPR
  - Four baggers
  - Opponent stats
- Round-by-round data (if available)

## Database Schema

### Match Table
```sql
CREATE TABLE event_matches (
    id INTEGER PRIMARY KEY,
    event_id INTEGER,
    match_id INTEGER,  -- API match ID
    round_number INTEGER,
    player1_id INTEGER,
    player2_id INTEGER,
    winner_id INTEGER,
    match_status INTEGER,  -- 5 = completed
    home_score INTEGER,
    away_score INTEGER,
    court_id INTEGER,
    created_at TIMESTAMP,
    UNIQUE(event_id, match_id)
);
```

### Game Table
```sql
CREATE TABLE event_games (
    id INTEGER PRIMARY KEY,
    event_id INTEGER,
    match_id INTEGER,
    game_id INTEGER,  -- Usually 1, but can have multiple games per match
    player1_id INTEGER,
    player2_id INTEGER,
    player1_points INTEGER,
    player2_points INTEGER,
    player1_rounds INTEGER,
    player2_rounds INTEGER,
    player1_bags_in INTEGER,
    player1_bags_on INTEGER,
    player1_bags_off INTEGER,
    player1_four_baggers INTEGER,
    player1_ppr FLOAT,
    player2_bags_in INTEGER,
    player2_bags_on INTEGER,
    player2_bags_off INTEGER,
    player2_four_baggers INTEGER,
    player2_ppr FLOAT,
    created_at TIMESTAMP,
    UNIQUE(event_id, match_id, game_id)
);
```

## Indexing Strategy

1. **Discover matches from bracket data**
   - Bracket data should have match IDs
   - If not, we may need to iterate through possible match IDs

2. **For each match, discover games**
   - Try game_id = 1, 2, 3... until we get a 404
   - Or check if match stats API tells us how many games

3. **Index each game**
   - Fetch match stats
   - Parse player stats
   - Store in database

## Implementation Steps

1. Add database tables
2. Add fetcher functions
3. Create game indexing logic
4. Add admin button to index games
5. Add MCP tools for querying games



