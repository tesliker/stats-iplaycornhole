"""
Game/Match Data Indexing Module

Indexes individual game/match data for events by:
1. Discovering match IDs (try iterating through match IDs)
2. Fetching match stats for each match
3. Storing game data in database
"""
import asyncio
from typing import Dict, List, Optional, Set, Callable
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func
from database import async_session_maker, Event, EventMatch, EventGame
from fetcher import fetch_match_stats

async def is_match_indexed(db: AsyncSession, event_id: int, match_id: int) -> bool:
    """Check if a match is already indexed."""
    from sqlalchemy import exists
    from sqlalchemy.sql import select
    
    query = select(
        exists(select(EventMatch.id).where(
            and_(EventMatch.event_id == event_id, EventMatch.match_id == match_id)
        ))
    )
    result = await db.execute(query)
    return result.scalar()


async def is_game_indexed(db: AsyncSession, event_id: int, match_id: int, game_id: int) -> bool:
    """Check if a game is already indexed."""
    from sqlalchemy import exists
    from sqlalchemy.sql import select
    
    query = select(
        exists(select(EventGame.id).where(
            and_(
                EventGame.event_id == event_id,
                EventGame.match_id == match_id,
                EventGame.game_id == game_id
            )
        ))
    )
    result = await db.execute(query)
    return result.scalar()


async def parse_match_data(match_data: Dict, event_id: int) -> Dict:
    """Parse match stats data into database record format.
    
    Captures ALL available match-level fields to ensure complete data capture.
    """
    match_id = int(match_data.get("matchID") or match_data.get("match_id") or match_data.get("matchId") or 0)
    game_id = int(match_data.get("gameID") or match_data.get("game_id") or match_data.get("gameId") or 1)
    
    # Extract player IDs from event_match_details
    event_match_details = match_data.get("event_match_details") or match_data.get("eventMatchDetails") or []
    player1_id = None
    player2_id = None
    
    if len(event_match_details) >= 1:
        player1_id = (event_match_details[0].get("playerid") or 
                     event_match_details[0].get("player_id") or 
                     event_match_details[0].get("playerId") or
                     event_match_details[0].get("playerID"))
    if len(event_match_details) >= 2:
        player2_id = (event_match_details[1].get("playerid") or 
                     event_match_details[1].get("player_id") or 
                     event_match_details[1].get("playerId") or
                     event_match_details[1].get("playerID"))
    
    # Determine winner (highest score)
    home_score = (match_data.get("homeScore") or match_data.get("home_score") or 
                 match_data.get("homeScore") or 0)
    away_score = (match_data.get("awayScore") or match_data.get("away_score") or 
                 match_data.get("awayScore") or 0)
    winner_id = player1_id if home_score > away_score else (player2_id if away_score > home_score else None)
    
    # Capture all match-level fields with multiple fallback variations
    return {
        "event_id": event_id,
        "match_id": match_id,
        "round_number": (match_data.get("currentRound") or match_data.get("round_number") or 
                        match_data.get("current_round") or match_data.get("round") or 
                        match_data.get("roundNumber") or 0),
        "player1_id": player1_id,
        "player2_id": player2_id,
        "winner_id": winner_id,
        "match_status": (match_data.get("matchStatus") or match_data.get("match_status") or 
                        match_data.get("matchStatus") or match_data.get("status")),
        "match_status_desc": (match_data.get("matchStatusDesc") or match_data.get("match_status_desc") or 
                             match_data.get("matchStatusDesc") or match_data.get("statusDesc") or 
                             match_data.get("status_desc") or match_data.get("statusDescription")),
        "home_score": home_score,
        "away_score": away_score,
        "court_id": (match_data.get("courtid") or match_data.get("court_id") or 
                    match_data.get("courtId") or match_data.get("courtID") or 
                    match_data.get("court")),
        "match_type": (match_data.get("matchType") or match_data.get("match_type") or 
                      match_data.get("matchType") or match_data.get("type")),
    }


async def parse_game_data(match_data: Dict, event_id: int) -> Optional[Dict]:
    """Parse game stats from match data.
    
    Captures ALL available game-level stats from event_match_details to ensure complete data capture.
    """
    event_match_details = (match_data.get("event_match_details") or 
                          match_data.get("eventMatchDetails") or [])
    
    if len(event_match_details) < 2:
        return None
    
    player1_data = event_match_details[0]
    player2_data = event_match_details[1]
    
    match_id = int(match_data.get("matchID") or match_data.get("match_id") or match_data.get("matchId") or 0)
    game_id = int(match_data.get("gameID") or match_data.get("game_id") or match_data.get("gameId") or 1)
    
    player1_id = (player1_data.get("playerid") or player1_data.get("player_id") or 
                 player1_data.get("playerId") or player1_data.get("playerID"))
    player2_id = (player2_data.get("playerid") or player2_data.get("player_id") or 
                 player2_data.get("playerId") or player2_data.get("playerID"))
    
    if not player1_id or not player2_id:
        return None
    
    # Helper function to safely extract numeric values with multiple fallbacks
    def get_int(data, *keys, default=0):
        for key in keys:
            val = data.get(key)
            if val is not None:
                try:
                    return int(val)
                except (ValueError, TypeError):
                    continue
        return default
    
    def get_float(data, *keys, default=0.0):
        for key in keys:
            val = data.get(key)
            if val is not None:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    continue
        return default
    
    return {
        "event_id": event_id,
        "match_id": match_id,
        "game_id": game_id,
        "player1_id": player1_id,
        "player2_id": player2_id,
        
        # Player 1 stats - capture all variations of field names
        "player1_points": get_int(player1_data, "totalpts", "total_pts", "totalPts", "totalPoints", "points"),
        "player1_rounds": get_int(player1_data, "rounds", "rounds_played", "roundsPlayed", "roundsTotal", "rounds_total"),
        "player1_bags_in": get_int(player1_data, "bagsin", "bags_in", "bagsIn", "bagsInTotal"),
        "player1_bags_on": get_int(player1_data, "bagson", "bags_on", "bagsOn", "bagsOnTotal"),
        "player1_bags_off": get_int(player1_data, "bagsoff", "bags_off", "bagsOff", "bagsOffTotal"),
        "player1_total_bags_thrown": get_int(player1_data, "totalbagsthrown", "total_bags_thrown", "totalBagsThrown", "totalBags", "bags_thrown"),
        "player1_four_baggers": get_int(player1_data, "totalfourbaggers", "total_four_baggers", "totalFourBaggers", "fourBaggers", "four_baggers"),
        "player1_ppr": get_float(player1_data, "ptsperrnd", "pts_per_rnd", "ptsPerRnd", "pointsPerRound", "ppr"),
        "player1_bags_in_pct": get_float(player1_data, "bagsinpct", "bags_in_pct", "bagsInPct", "bagsInPercentage"),
        "player1_bags_on_pct": get_float(player1_data, "bagsonpct", "bags_on_pct", "bagsOnPct", "bagsOnPercentage"),
        "player1_bags_off_pct": get_float(player1_data, "bagsoffpct", "bags_off_pct", "bagsOffPct", "bagsOffPercentage"),
        "player1_four_bagger_pct": get_float(player1_data, "fourbaggerpct", "four_bagger_pct", "fourBaggerPct", "fourBaggerPercentage"),
        "player1_opponent_points": get_int(player2_data, "totalpts", "total_pts", "totalPts", "totalPoints", "points"),
        "player1_opponent_ppr": get_float(player2_data, "ptsperrnd", "pts_per_rnd", "ptsPerRnd", "pointsPerRound", "ppr"),
        
        # Player 2 stats - capture all variations of field names
        "player2_points": get_int(player2_data, "totalpts", "total_pts", "totalPts", "totalPoints", "points"),
        "player2_rounds": get_int(player2_data, "rounds", "rounds_played", "roundsPlayed", "roundsTotal", "rounds_total"),
        "player2_bags_in": get_int(player2_data, "bagsin", "bags_in", "bagsIn", "bagsInTotal"),
        "player2_bags_on": get_int(player2_data, "bagson", "bags_on", "bagsOn", "bagsOnTotal"),
        "player2_bags_off": get_int(player2_data, "bagsoff", "bags_off", "bagsOff", "bagsOffTotal"),
        "player2_total_bags_thrown": get_int(player2_data, "totalbagsthrown", "total_bags_thrown", "totalBagsThrown", "totalBags", "bags_thrown"),
        "player2_four_baggers": get_int(player2_data, "totalfourbaggers", "total_four_baggers", "totalFourBaggers", "fourBaggers", "four_baggers"),
        "player2_ppr": get_float(player2_data, "ptsperrnd", "pts_per_rnd", "ptsPerRnd", "pointsPerRound", "ppr"),
        "player2_bags_in_pct": get_float(player2_data, "bagsinpct", "bags_in_pct", "bagsInPct", "bagsInPercentage"),
        "player2_bags_on_pct": get_float(player2_data, "bagsonpct", "bags_on_pct", "bagsOnPct", "bagsOnPercentage"),
        "player2_bags_off_pct": get_float(player2_data, "bagsoffpct", "bags_off_pct", "bagsOffPct", "bagsOffPercentage"),
        "player2_four_bagger_pct": get_float(player2_data, "fourbaggerpct", "four_bagger_pct", "fourBaggerPct", "fourBaggerPercentage"),
        "player2_opponent_points": get_int(player1_data, "totalpts", "total_pts", "totalPts", "totalPoints", "points"),
        "player2_opponent_ppr": get_float(player1_data, "ptsperrnd", "pts_per_rnd", "ptsPerRnd", "pointsPerRound", "ppr"),
    }


async def index_match_game(event_id: int, match_id: int, game_id: int, db: AsyncSession) -> bool:
    """Index a single match/game. Returns True if successful or already indexed.
    
    Also checks for additional games (game_id 2, 3, etc.) if game_id 1 exists.
    """
    try:
        # Check if already indexed
        if await is_game_indexed(db, event_id, match_id, game_id):
            return True  # Already indexed
        
        # Fetch match stats
        match_data = await fetch_match_stats(event_id, match_id, game_id)
        if not match_data:
            return False  # Match/game doesn't exist
        
        # Parse and store match (only if not already indexed)
        if not await is_match_indexed(db, event_id, match_id):
            match_record = await parse_match_data(match_data, event_id)
            if match_record.get("match_id"):
                # Store raw API response to preserve all data
                match_record["raw_data"] = match_data
                match = EventMatch(**match_record)
                db.add(match)
                await db.flush()
        
        # Parse and store game
        game_record = await parse_game_data(match_data, event_id)
        if game_record:
            # Store raw API response to preserve all data
            game_record["raw_data"] = match_data
            game = EventGame(**game_record)
            db.add(game)
        else:
            print(f"Warning: Could not parse game data for event {event_id}, match {match_id}, game {game_id}")
            # Still commit the match if we have it
            if await is_match_indexed(db, event_id, match_id):
                await db.commit()
                return True
            return False
        
        await db.commit()
        return True
        
    except Exception as e:
        print(f"Error indexing match {match_id}, game {game_id} for event {event_id}: {e}")
        import traceback
        traceback.print_exc()
        await db.rollback()
        return False


async def index_match_with_all_games(event_id: int, match_id: int, db: AsyncSession, check_additional_games: bool = False) -> int:
    """Index a match and its first game (game_id 1).
    
    For speed, by default only indexes game_id 1. Set check_additional_games=True
    to check for game_id 2, 3, etc. (slower but more comprehensive).
    
    Returns the number of games indexed (0 if already indexed or doesn't exist).
    """
    # Check if already indexed first (skip if already done)
    if await is_game_indexed(db, event_id, match_id, 1):
        return 0  # Already indexed, no new games
    
    games_indexed = 0
    
    # Start with game_id 1
    game1_result = await index_match_game(event_id, match_id, 1, db)
    if game1_result:
        games_indexed += 1
        
        # Only check for additional games if explicitly requested (slower)
        if check_additional_games:
            game_id = 2
            consecutive_failures = 0
            while consecutive_failures < 3:
                try:
                    match_data = await fetch_match_stats(event_id, match_id, game_id)
                    if not match_data:
                        consecutive_failures += 1
                        game_id += 1
                        continue
                    
                    consecutive_failures = 0
                    if await index_match_game(event_id, match_id, game_id, db):
                        games_indexed += 1
                    game_id += 1
                    await asyncio.sleep(0.05)
                except Exception as e:
                    consecutive_failures += 1
                    game_id += 1
                    await asyncio.sleep(0.05)
    
    return games_indexed


async def get_bracket_data_for_event(event_id: int, db: AsyncSession) -> Optional[Dict]:
    """Get bracket data for an event, either from database or by fetching from API.
    
    Stores the bracket data in the event record if fetched from API.
    """
    from database import Event
    from fetcher import fetch_bracket_data
    
    # First check if we have it stored in the event record
    event_query = select(Event).where(Event.event_id == event_id)
    event_result = await db.execute(event_query)
    event = event_result.scalar_one_or_none()
    
    if event and event.game_data:
        # Use stored bracket data
        return event.game_data
    
    # Fetch from API and store it
    bracket_data = await fetch_bracket_data(event_id)
    if bracket_data and event:
        event.game_data = bracket_data
        await db.commit()
        print(f"Stored bracket data for event {event_id}")
    
    return bracket_data


async def discover_all_games_from_bracket(event_id: int, db: AsyncSession, log_callback: Optional[Callable] = None) -> List[Dict]:
    """Discover all match/game combinations from bracket data.
    
    Uses bracketDetails to find all bracketmatchid values, then checks for games.
    Returns a list of dicts with 'match_id' and 'game_id' keys.
    """
    def log(msg: str):
        if log_callback:
            log_callback(msg)
        else:
            print(msg)
    
    bracket_data = await get_bracket_data_for_event(event_id, db)
    if not bracket_data:
        log(f"✗ No bracket data found for event {event_id}")
        return []
    games = []
    seen_matches = set()  # Track match_ids we've already processed
    
    # Check bracketDetails (list of bracket positions)
    bracket_details = bracket_data.get("bracketDetails") or []
    if isinstance(bracket_details, list):
        for detail in bracket_details:
            # Get bracketmatchid - this is the match ID we need to fetch
            bracket_match_id = detail.get("bracketmatchid") or detail.get("bracketMatchID")
            if bracket_match_id:
                try:
                    # Handle both string and int match IDs
                    match_id = int(bracket_match_id) if bracket_match_id else None
                    if not match_id:
                        continue
                    
                    # Skip if we've already processed this match
                    if match_id in seen_matches:
                        continue
                    seen_matches.add(match_id)
                    
                    # Only include matches that have gameResults (non-empty)
                    # This filters out forfeited/unplayed matches
                    game_results = detail.get("gameResults") or detail.get("gameresults") or []
                    if not game_results or len(game_results) == 0:
                        # Skip matches without game results - they don't have data yet
                        continue
                    
                    # Start with game_id 1 (most common)
                    games.append({"match_id": match_id, "game_id": 1})
                    
                    # Note: We'll check for additional games (game_id 2, 3, etc.) 
                    # when we actually fetch the match stats, since we don't know 
                    # from bracket data if there are multiple games
                except (ValueError, TypeError) as e:
                    pass
    
    return games


async def is_event_games_fully_indexed(db: AsyncSession, event_id: int) -> bool:
    """Check if all games for an event have been indexed."""
    event_query = select(Event).where(Event.event_id == event_id)
    event_result = await db.execute(event_query)
    event = event_result.scalar_one_or_none()
    
    if event and event.games_fully_indexed:
        return True
    
    return False


async def discover_and_index_event_games_with_status(
    event_id: int, 
    db: AsyncSession, 
    status_callback: Optional[Callable] = None,
    log_callback: Optional[Callable] = None,
    max_match_id: int = 200, 
    skip_if_complete: bool = True
) -> int:
    """Discover and index all games for an event with status callbacks.
    
    Wrapper around discover_and_index_event_games that provides status updates.
    """
    def log(msg: str):
        if log_callback:
            log_callback(msg)
        else:
            print(msg)
    
    # Try to discover all games from bracket data first to get total count
    games_from_bracket = await discover_all_games_from_bracket(event_id, db, log_callback=log)
    total_games = len(games_from_bracket) if games_from_bracket else None
    
    if status_callback:
        status_callback(
            total_games=total_games,
            processed_games=0,
            current_match=None
        )
    
    # Check if already fully indexed
    if skip_if_complete and await is_event_games_fully_indexed(db, event_id):
        log(f"Event {event_id} games already fully indexed, skipping...")
        if status_callback:
            status_callback(processed_games=total_games or 0)
        return 0
    
    if games_from_bracket:
        log(f"Found {total_games} unique matches in bracket data for event {event_id}")
    
    new_games = 0
    processed = 0
    
    if games_from_bracket:
        # Filter out already-indexed games before processing
        log(f"Checking database for already-indexed games...")
        already_indexed_query = select(EventGame.match_id, EventGame.game_id).where(
            EventGame.event_id == event_id
        )
        indexed_result = await db.execute(already_indexed_query)
        indexed_games = set()
        for row in indexed_result:
            indexed_games.add((int(row.match_id), int(row.game_id)))
        
        # Filter out matches that are already indexed
        games_to_index = []
        for game_info in games_from_bracket:
            match_id = game_info["match_id"]
            game_id = game_info["game_id"]
            if (match_id, game_id) not in indexed_games:
                games_to_index.append(game_info)
        
        skipped_count = len(games_from_bracket) - len(games_to_index)
        if skipped_count > 0:
            log(f"Skipping {skipped_count} already-indexed games, processing {len(games_to_index)} new games")
        elif len(games_to_index) == 0:
            log(f"All {len(games_from_bracket)} games already indexed, nothing to process")
        
        # Process only games that haven't been indexed yet
        for idx, game_info in enumerate(games_to_index):
            match_id = game_info["match_id"]
            game_id = game_info["game_id"]
            
            if status_callback:
                status_callback(
                    current_match=match_id,
                    processed_games=idx,
                    new_games_indexed=new_games
                )
            
            # Index this match (game_id 1 only for speed)
            if idx % 20 == 0 or idx <= 3:
                log(f"Processing match {idx}/{len(games_to_index)}: match_id={match_id}")
            games_for_match = await index_match_with_all_games(event_id, match_id, db, check_additional_games=False)
            
            if games_for_match > 0:
                new_games += games_for_match
                processed += games_for_match
                # Only log every 20 matches to reduce verbosity
                if idx % 20 == 0:
                    log(f"Match {match_id}: Indexed {games_for_match} games (progress: {idx}/{len(games_to_index)})")
            else:
                processed += 1  # Count processed even if no games found
                # Don't log "no game data" - these are expected for future/unplayed matches
                # Only log summary at the end
    else:
        # Fallback: iterative approach
        new_games = await discover_and_index_event_games(
            event_id, db, max_match_id=max_match_id, skip_if_complete=False
        )
        # For iterative approach, we can't easily track progress
        if status_callback:
            status_callback(
                processed_games=new_games,
                new_games_indexed=new_games
            )
    
    # Update event record
    from database import Event
    from sqlalchemy import func
    from datetime import datetime
    
    games_count_query = select(func.count()).select_from(EventGame).where(EventGame.event_id == event_id)
    games_count_result = await db.execute(games_count_query)
    indexed_count = games_count_result.scalar() or 0
    
    event_query = select(Event).where(Event.event_id == event_id)
    event_result = await db.execute(event_query)
    event = event_result.scalar_one_or_none()
    
    if event:
        event.games_total_count = total_games or indexed_count
        event.games_indexed_count = indexed_count
        event.games_fully_indexed = (total_games is not None and indexed_count >= total_games)
        if event.games_fully_indexed:
            event.games_indexed_at = datetime.utcnow()
        await db.commit()
    
    if status_callback:
        status_callback(
            processed_games=processed,
            new_games_indexed=new_games,
            total_games=indexed_count
        )
    
    # Calculate how many matches had no game data
    matches_without_data = processed - new_games
    if matches_without_data > 0:
        log(f"Event {event_id}: {new_games} new games indexed, {matches_without_data} matches had no game data available (may not exist yet), {indexed_count} total games in database")
    else:
        log(f"Event {event_id}: {new_games} new games indexed, {processed} matches processed, {indexed_count} total games in database")
    return new_games


async def discover_and_index_event_games(event_id: int, db: AsyncSession, max_match_id: int = 200, skip_if_complete: bool = True) -> int:
    """Discover and index all games for an event.
    
    First tries to get all games from bracket data, then falls back to iterating if needed.
    
    Args:
        event_id: Event ID to index games for
        db: Database session
        max_match_id: Maximum match ID to try if bracket data not available (default 200)
        skip_if_complete: If True, skip events that are already fully indexed (default True)
    
    Returns:
        Number of new games indexed (0 if skipped)
    """
    # Check if already fully indexed
    if skip_if_complete and await is_event_games_fully_indexed(db, event_id):
        print(f"Event {event_id} games already fully indexed, skipping...")
        return 0
    
    # Try to discover all games from bracket data
    def log(msg: str):
        if log_callback:
            log_callback(msg)
        else:
            print(msg)
    
    log(f"Fetching bracket data for event {event_id}...")
    games_from_bracket = await discover_all_games_from_bracket(event_id, db, log_callback=log)
    
    if games_from_bracket:
        # Use games from bracket data - this is the complete list
        total_games = len(games_from_bracket)
        log(f"Found {total_games} unique matches in bracket data for event {event_id}")
        new_games = 0
        already_indexed = 0
        
        for idx, game_info in enumerate(games_from_bracket, 1):
            match_id = game_info["match_id"]
            
            if idx % 10 == 0 or idx <= 5:
                log(f"Processing match {idx}/{total_games}: match_id={match_id}")
            
            # Index this match and all its games (game_id 1, 2, 3, etc.)
            log(f"Indexing match {match_id} (game_id 1, then checking for more)...")
            games_for_match = await index_match_with_all_games(event_id, match_id, db)
            
            if games_for_match > 0:
                new_games += games_for_match
                log(f"Match {match_id}: Indexed {games_for_match} games")
            else:
                # Check if game_id 1 was already indexed
                if await is_game_indexed(db, event_id, match_id, 1):
                    already_indexed += 1
                    log(f"Match {match_id}: Already indexed (skipped)")
                else:
                    log(f"Match {match_id}: No games found (may not exist yet)")
            
            # Small delay to avoid rate limiting
            await asyncio.sleep(0.05)
        
        # Update event indexing status
        from database import Event
        from sqlalchemy import func
        from datetime import datetime
        
        # Count total indexed games for this event
        games_count_query = select(func.count()).select_from(EventGame).where(EventGame.event_id == event_id)
        games_count_result = await db.execute(games_count_query)
        indexed_count = games_count_result.scalar() or 0
        
        # Update event record
        event_query = select(Event).where(Event.event_id == event_id)
        event_result = await db.execute(event_query)
        event = event_result.scalar_one_or_none()
        
        if event:
            event.games_total_count = total_games
            event.games_indexed_count = indexed_count
            event.games_fully_indexed = (indexed_count >= total_games)
            if event.games_fully_indexed:
                event.games_indexed_at = datetime.utcnow()
            await db.commit()
        
        print(f"Event {event_id}: {new_games} new games indexed, {already_indexed} already indexed, {indexed_count}/{total_games} total")
        
        return new_games
    else:
        # Fallback: try iterating through match IDs if bracket data doesn't have game info
        print(f"No games found in bracket data for event {event_id}, trying iterative approach...")
        new_games = 0
        consecutive_404s = 0
        max_consecutive_404s = 10  # Stop after 10 consecutive 404s
        
        # Get all already-indexed games for this event to filter out
        already_indexed_query = select(EventGame.match_id, EventGame.game_id).where(
            EventGame.event_id == event_id
        )
        indexed_result = await db.execute(already_indexed_query)
        indexed_games = set()
        for row in indexed_result:
            indexed_games.add((row.match_id, row.game_id))
        
        for match_id in range(1, max_match_id + 1):
            # Skip if already indexed
            if (match_id, 1) in indexed_games:
                continue
            
            # Try game_id = 1 first (most matches have only one game)
            match_data = await fetch_match_stats(event_id, match_id, game_id=1)
            
            if not match_data:
                consecutive_404s += 1
                if consecutive_404s >= max_consecutive_404s:
                    # No more matches found
                    break
                continue
            
            # Reset consecutive 404 counter
            consecutive_404s = 0
            
            # Index game 1
            if await index_match_game(event_id, match_id, 1, db):
                new_games += 1
            
            # Skip checking for additional games (game_id 2, 3, etc.) for speed
            # Only index game_id 1
            
            # Small delay to avoid rate limiting
            await asyncio.sleep(0.05)
        
        # Update event indexing status (for fallback case)
        games_count_query = select(func.count()).select_from(EventGame).where(EventGame.event_id == event_id)
        games_count_result = await db.execute(games_count_query)
        indexed_count = games_count_result.scalar() or 0
        
        event_query = select(Event).where(Event.event_id == event_id)
        event_result = await db.execute(event_query)
        event = event_result.scalar_one_or_none()
        
        if event:
            event.games_indexed_count = indexed_count
            # Can't mark fully indexed without knowing total, so leave games_fully_indexed as False
            await db.commit()
        
        return new_games


async def index_season_games_with_status(bucket_id: int = 11, limit_events: Optional[int] = None):
    """Index all games for all events in a season, with status updates."""
    from datetime import datetime
    import sys
    main_module = sys.modules.get('__main__')
    
    async with async_session_maker() as db:
        # Get all indexed events for this season (skip events that are already fully indexed)
        # Only index: regional, open, signature, national, and local finals
        events_query = select(Event.event_id).where(
            Event.bucket_id == bucket_id,
            Event.games_fully_indexed == False,  # Only index events that aren't fully indexed
            or_(
                # Allow regional, open, signature, national
                Event.event_type.in_(["regional", "open", "signature", "national", "r", "o", "s", "n"]),
                # Allow local events that are finals
                and_(
                    Event.event_type.in_(["local", "l"]),
                    Event.event_name.ilike("%Final%")
                )
            )
        )
        if limit_events:
            events_query = events_query.limit(limit_events)
        
        result = await db.execute(events_query)
        event_ids = [row[0] for row in result.all()]
        
        print(f"Found {len(event_ids)} events to index games for")
        
        # Get initial game count (for tracking new games)
        from sqlalchemy import func
        initial_games_result = await db.execute(
            select(func.count()).select_from(EventGame)
            .join(Event, EventGame.event_id == Event.event_id)
            .where(Event.bucket_id == bucket_id)
        )
        initial_game_count = initial_games_result.scalar() or 0
        
        # Initialize status
        if main_module and hasattr(main_module, 'game_indexing_status'):
            main_module.game_indexing_status[bucket_id] = {
                "status": "running",
                "bucket_id": bucket_id,
                "started_at": datetime.utcnow().isoformat(),
                "total_events": len(event_ids),
                "processed_events": 0,
                "current_event": None,
                "total_games": initial_game_count,
                "new_games_indexed": 0,
                "initial_game_count": initial_game_count,
                "error": None,
            }
        
        total_new_games = 0
        for idx, event_id in enumerate(event_ids, 1):
            try:
                # Update status
                if main_module and hasattr(main_module, 'game_indexing_status'):
                    if bucket_id in main_module.game_indexing_status:
                        main_module.game_indexing_status[bucket_id].update({
                            "processed_events": idx,
                            "current_event": event_id,
                        })
                
                # Skip if already fully indexed (double-check)
                if await is_event_games_fully_indexed(db, event_id):
                    print(f"Skipping event {event_id} - already fully indexed")
                    continue
                
                new_games = await discover_and_index_event_games(event_id, db, skip_if_complete=True)
                total_new_games += new_games
                
                # Update status
                if main_module and hasattr(main_module, 'game_indexing_status'):
                    if bucket_id in main_module.game_indexing_status:
                        # Count total games
                        from sqlalchemy import func
                        total_games_result = await db.execute(
                            select(func.count()).select_from(EventGame).where(
                                EventGame.event_id == event_id
                            )
                        )
                        total_games = total_games_result.scalar() or 0
                        
                        # Get total for all events
                        all_games_result = await db.execute(
                            select(func.count()).select_from(EventGame)
                            .join(Event, EventGame.event_id == Event.event_id)
                            .where(Event.bucket_id == bucket_id)
                        )
                        all_total_games = all_games_result.scalar() or 0
                        
                        main_module.game_indexing_status[bucket_id].update({
                            "new_games_indexed": total_new_games,
                            "total_games": all_total_games,
                        })
                
                print(f"Event {event_id}: {new_games} new games indexed (total so far: {total_new_games})")
                
                if idx % 10 == 0:
                    print(f"Processed {idx}/{len(event_ids)} events, {total_new_games} new games indexed")
                
                # Delay between events
                await asyncio.sleep(0.1)
                
            except Exception as e:
                print(f"Error indexing games for event {event_id}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # Mark as completed
        if main_module and hasattr(main_module, 'game_indexing_status'):
            if bucket_id in main_module.game_indexing_status:
                main_module.game_indexing_status[bucket_id]["status"] = "completed"
        
        print(f"Completed indexing games for season {bucket_id}. {total_new_games} new games indexed.")

