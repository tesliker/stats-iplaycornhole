"""
Event Data Indexing Module

Indexes event data (regionals, opens, nationals) by:
1. Discovering events from player event lists
2. Fetching and storing event data
3. Deduplicating to avoid re-indexing
"""

import asyncio
from typing import Dict, List, Optional, Set
from datetime import datetime, date
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from database import (
    async_session_maker, Event, PlayerEventStats, EventMatchup, EventStanding, Player,
    EventMatch, EventGame
)
from fetcher import (
    fetch_player_events_list,
    fetch_event_info,
    fetch_event_player_stats,
    fetch_event_standings,
    fetch_bracket_data,
    fetch_match_stats,
    detect_event_type,
    extract_event_number,
    extract_base_event_name,
    extract_bracket_name
)
import os


async def get_event_indexing_status(bucket_id: int) -> Dict:
    """Get status of event indexing for a season."""
    import sys
    import importlib
    
    # Import main to access event_indexing_status
    if 'main' in sys.modules:
        main_module = sys.modules['main']
    else:
        try:
            main_module = importlib.import_module('main')
        except:
            main_module = None
    
    if main_module and hasattr(main_module, 'event_indexing_status'):
        if bucket_id in main_module.event_indexing_status:
            return main_module.event_indexing_status[bucket_id]
    
    return {
        "status": "not_running",
        "bucket_id": bucket_id,
        "message": "No active indexing for this season"
    }

async def is_event_indexed(db: AsyncSession, event_id: int) -> bool:
    """Check if an event is already indexed. Fast check using exists()."""
    from sqlalchemy import exists
    result = await db.execute(
        select(exists().where(Event.event_id == event_id))
    )
    return result.scalar()


async def get_indexed_event_ids(db: AsyncSession, bucket_id: Optional[int] = None) -> Set[int]:
    """Get set of already indexed event IDs."""
    query = select(Event.event_id)
    if bucket_id:
        query = query.where(Event.bucket_id == bucket_id)
    result = await db.execute(query)
    return set(result.scalars().all())


async def parse_event_info(event_data: Dict, bucket_id: int) -> Dict:
    """Parse event info data into database record format."""
    event_id = event_data.get("leagueID") or event_data.get("eventID") or event_data.get("event_id") or event_data.get("id")
    event_name = event_data.get("leagueName") or event_data.get("eventName") or event_data.get("event_name") or event_data.get("name", "")
    
    # Parse date
    event_date = None
    date_str = event_data.get("leagueStartDate") or event_data.get("leaguestartdate") or event_data.get("eventDate") or event_data.get("event_date") or event_data.get("date")
    if date_str:
        try:
            if isinstance(date_str, str):
                # Try various date formats
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"]:
                    try:
                        event_date = datetime.strptime(date_str.split("T")[0], fmt).date()
                        break
                    except:
                        continue
            elif isinstance(date_str, (datetime, date)):
                if isinstance(date_str, datetime):
                    event_date = date_str.date()
                else:
                    event_date = date_str
        except:
            pass
    
    location = event_data.get("leagueLocationName") or event_data.get("location") or event_data.get("eventLocation") or ""
    city = event_data.get("city") or event_data.get("eventCity") or ""
    state = event_data.get("locationState") or event_data.get("state") or event_data.get("eventState") or ""
    region = event_data.get("region") or event_data.get("_region") or "us"
    
    event_type = detect_event_type(event_name, event_data)
    event_number = extract_event_number(event_name)
    is_signature = 1 if event_type == "signature" else 0
    
    # Extract grouping information
    event_group_id = event_data.get("eventGroupID") or event_data.get("event_group_id")
    bracket_name = extract_bracket_name(event_name)
    base_event_name = extract_base_event_name(event_name)
    
    return {
        "event_id": event_id,
        "event_name": event_name,
        "event_type": event_type,
        "event_date": event_date,
        "location": location,
        "city": city,
        "state": state,
        "bucket_id": bucket_id,
        "region": region,
        "event_number": event_number,
        "is_signature": is_signature,
        "event_group_id": event_group_id,
        "bracket_name": bracket_name,
        "base_event_name": base_event_name,
    }


async def parse_player_event_stats(stats_data: Dict, event_id: int) -> Dict:
    """Parse player event stats into database record format.
    
    Note: Do NOT use "ranking" from stats_data - it's PPR-based.
    Use fldEventRank from standings instead (merged in index_event).
    """
    player_id = stats_data.get("playerID") or stats_data.get("player_id")
    
    # Don't set rank here - it will be set from standings data
    # Don't set wins/losses here - they'll come from standings
    
    return {
        "event_id": event_id,
        "player_id": player_id,
        "rank": None,  # Will be set from standings (fldEventRank)
        "pts_per_rnd": stats_data.get("ptsPerRnd") or stats_data.get("pts_per_rnd"),
        "dpr": stats_data.get("diffPerRnd") or stats_data.get("dpr"),  # API uses "diffPerRnd" for DPR
        "total_games": stats_data.get("totalGames") or stats_data.get("total_games"),
        "wins": None,  # Will be set from standings
        "losses": None,  # Will be set from standings
        "win_pct": None,  # Will be calculated from wins/losses
        "rounds_played": stats_data.get("rounds") or stats_data.get("roundsPlayed") or stats_data.get("rounds_played") or stats_data.get("roundsTotal"),  # API uses "rounds"
        "total_pts": stats_data.get("totalPts") or stats_data.get("total_pts"),
        "opponent_pts_per_rnd": stats_data.get("opponentPtsPerRnd") or stats_data.get("opponent_pts_per_rnd"),
        "opponent_pts_total": stats_data.get("opponentPts") or stats_data.get("opponentPtsTotal") or stats_data.get("opponent_pts_total"),  # API uses "opponentPts"
        "four_bagger_pct": stats_data.get("fourBaggerPct") or stats_data.get("four_bagger_pct"),
        "bags_in_pct": stats_data.get("bagsInPct") or stats_data.get("bags_in_pct"),
        "bags_on_pct": stats_data.get("bagsOnPct") or stats_data.get("bags_on_pct"),
        "bags_off_pct": stats_data.get("bagsOffPct") or stats_data.get("bags_off_pct"),
    }


async def parse_bracket_matchups(bracket_data: Dict, event_id: int) -> List[Dict]:
    """Parse bracket data into matchup records."""
    matchups = []
    
    # Bracket data structure varies, so we need to handle different formats
    rounds = bracket_data.get("rounds") or bracket_data.get("data") or []
    
    if not isinstance(rounds, list):
        return matchups
    
    for round_idx, round_data in enumerate(rounds):
        round_number = round_data.get("roundNumber") or round_data.get("round") or (round_idx + 1)
        matches = round_data.get("matches") or round_data.get("data") or []
        
        if not isinstance(matches, list):
            continue
        
        for match in matches:
            player1_id = match.get("player1ID") or match.get("player1_id") or match.get("player1")
            player2_id = match.get("player2ID") or match.get("player2_id") or match.get("player2")
            winner_id = match.get("winnerID") or match.get("winner_id") or match.get("winner")
            
            if not player1_id or not player2_id:
                continue
            
            score = match.get("score") or ""
            player1_score = match.get("player1Score") or match.get("player1_score")
            player2_score = match.get("player2Score") or match.get("player2_score")
            
            loser_id = player2_id if winner_id == player1_id else (player1_id if winner_id == player2_id else None)
            
            matchups.append({
                "event_id": event_id,
                "round_number": round_number,
                "player1_id": player1_id,
                "player2_id": player2_id,
                "winner_id": winner_id,
                "loser_id": loser_id,
                "score": score,
                "player1_score": player1_score,
                "player2_score": player2_score,
            })
    
    return matchups


async def parse_event_standings(standings_data: List[Dict], event_id: int) -> List[Dict]:
    """Parse event standings into database record format.
    
    Captures all available fields from standings API to ensure complete data capture.
    """
    standings = []
    
    for standing in standings_data:
        player_id = standing.get("playerID") or standing.get("fldPlayerID") or standing.get("player_id")
        final_rank = standing.get("fldEventRank") or standing.get("fldEventPos") or standing.get("rank") or standing.get("finalRank") or standing.get("final_rank")
        points = standing.get("fldEventTotalPoints") or standing.get("points") or standing.get("totalPoints") or standing.get("total_points") or standing.get("fldEventPoints")
        
        if player_id:
            standings.append({
                "event_id": event_id,
                "player_id": player_id,
                "final_rank": final_rank,
                "points": points,
                # Note: wins/losses are stored in PlayerEventStats, not here
                # This ensures we have complete data without duplication
            })
    
    return standings


async def index_event(event_id: int, bucket_id: int, db: AsyncSession, force_reindex: bool = False) -> bool:
    """Index a single event with all its data.
    
    Args:
        event_id: The event ID to index
        bucket_id: The season/bucket ID
        db: Database session
        force_reindex: If True, re-index even if already indexed (updates existing data)
    """
    try:
        # Fast check if already indexed (using exists() which is optimized)
        if await is_event_indexed(db, event_id) and not force_reindex:
            return True  # Already indexed, skip silently
        
        # If force_reindex, delete existing player stats for this event first
        if force_reindex:
            from database import PlayerEventStats
            result = await db.execute(select(PlayerEventStats).where(PlayerEventStats.event_id == event_id))
            existing_stats = result.scalars().all()
            for stat in existing_stats:
                await db.delete(stat)
            await db.flush()
        
        print(f"Indexing event {event_id}...")
        
        # Fetch event info
        event_info_data = await fetch_event_info(event_id)
        if not event_info_data:
            print(f"Could not fetch event info for {event_id}")
            return False
        
        # Parse and store event
        event_record = await parse_event_info(event_info_data, bucket_id)
        event_record["event_id"] = event_id  # Ensure event_id is set
        event = Event(**event_record)
        db.add(event)
        await db.flush()  # Get the event ID
        
        # Fetch and store player stats
        player_stats_data = await fetch_event_player_stats(event_id)
        # Fetch standings to get wins/losses and rank (fldEventRank is the actual event rank)
        standings_data = await fetch_event_standings(event_id)
        standings_dict = {}
        if standings_data:
            # standings_data might be a list or a dict with a "data" key
            if isinstance(standings_data, dict) and "data" in standings_data:
                standings_list = standings_data["data"]
            elif isinstance(standings_data, list):
                standings_list = standings_data
            else:
                standings_list = []
            
            for standing in standings_list:
                player_id = standing.get("playerID") or standing.get("fldPlayerID")
                if player_id:
                    # Use fldEventRank as the actual event rank (not ranking from stats which is PPR-based)
                    event_rank = standing.get("fldEventRank") or standing.get("fldEventPos") or standing.get("rank")
                    standings_dict[player_id] = {
                        "rank": event_rank,
                        "wins": standing.get("wins"),
                        "losses": standing.get("losses"),
                    }
        
        if player_stats_data:
            # player_stats_data might be a list or a dict with a "data" key
            if isinstance(player_stats_data, dict) and "data" in player_stats_data:
                stats_list = player_stats_data["data"]
            elif isinstance(player_stats_data, list):
                stats_list = player_stats_data
            else:
                stats_list = []
            
            for stats in stats_list:
                try:
                    stats_record = await parse_player_event_stats(stats, event_id)
                    player_id = stats_record.get("player_id")
                    if player_id:
                        # Always prioritize standings data for rank/wins/losses if available
                        if player_id in standings_dict:
                            standing_info = standings_dict[player_id]
                            # Use fldEventRank from standings (not ranking from stats)
                            if standing_info.get("rank") is not None:
                                stats_record["rank"] = standing_info["rank"]
                            # Use wins/losses from standings
                            if standing_info.get("wins") is not None:
                                stats_record["wins"] = standing_info["wins"]
                            if standing_info.get("losses") is not None:
                                stats_record["losses"] = standing_info["losses"]
                                # Recalculate win_pct if we have wins/losses
                                if stats_record.get("wins") is not None and stats_record.get("losses") is not None:
                                    total = stats_record["wins"] + stats_record["losses"]
                                    if total > 0:
                                        stats_record["win_pct"] = (stats_record["wins"] / total) * 100
                        
                        player_stats = PlayerEventStats(**stats_record)
                        db.add(player_stats)
                except Exception as e:
                    print(f"Error parsing player stats for event {event_id}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
        
        # Fetch and store bracket data
        bracket_data = await fetch_bracket_data(event_id)
        if bracket_data:
            # Store complete bracket data in event record for game indexing
            event.game_data = bracket_data
            
            # Also parse and store matchups
            matchups = await parse_bracket_matchups(bracket_data, event_id)
            for matchup in matchups:
                try:
                    matchup_record = EventMatchup(**matchup)
                    db.add(matchup_record)
                except Exception as e:
                    print(f"Error parsing matchup for event {event_id}: {e}")
                    continue
        
        # Fetch and store standings
        standings_data = await fetch_event_standings(event_id)
        if standings_data:
            standings = await parse_event_standings(standings_data, event_id)
            for standing in standings:
                try:
                    standing_record = EventStanding(**standing)
                    db.add(standing_record)
                except Exception as e:
                    print(f"Error parsing standing for event {event_id}: {e}")
                    continue
        
        await db.commit()
        print(f"Successfully indexed event {event_id}")
        return True
        
    except Exception as e:
        print(f"Error indexing event {event_id}: {e}")
        import traceback
        traceback.print_exc()
        await db.rollback()
        return False


async def index_player_events(player_id: int, bucket_id: int, db: AsyncSession, indexed_event_ids: Set[int]) -> int:
    """Index all events for a player. Returns number of new events indexed."""
    events_list = await fetch_player_events_list(player_id, bucket_id)
    if not events_list:
        return 0
    
    new_events = 0
    for event_data in events_list:
        # The API returns leagueID, not eventID
        event_id = event_data.get("leagueID") or event_data.get("eventID") or event_data.get("event_id") or event_data.get("id")
        if not event_id:
            continue
        
        # Skip if already indexed (fast in-memory check)
        if event_id in indexed_event_ids:
            continue
        
        # Skip local events EXCEPT if they're finals
        # Finals are often marked as "L" but should still be indexed
        api_event_type = event_data.get("eventType") or event_data.get("event_type")
        event_name = event_data.get("leagueName", "") or event_data.get("leagueName", "") or ""
        
        # Allow events that are finals (contain "Final" in name)
        is_final = "Final" in event_name or "final" in event_name
        
        # Skip local events UNLESS they're finals
        if api_event_type == "L" and not is_final:
            continue
        
        # Also check detected type (in case API type is missing)
        detected_type = detect_event_type(event_name, event_data)
        if detected_type == "local" and not is_final:
            continue
        
        # Index the event
        success = await index_event(event_id, bucket_id, db)
        if success:
            indexed_event_ids.add(event_id)
            new_events += 1
        
        # Small delay to avoid rate limiting
        await asyncio.sleep(0.05)  # Reduced from 0.1 to 0.05 for faster processing
    
    return new_events


async def index_season_events(bucket_id: int = 11, limit_players: Optional[int] = None):
    """Index all events for a season by going through players."""
    async with async_session_maker() as db:
        # Get all players for the season (latest snapshot)
        latest_dates = select(
            Player.player_id,
            func.max(Player.snapshot_date).label('max_date')
        ).where(
            Player.bucket_id == bucket_id
        ).group_by(Player.player_id).subquery()
        
        players_query = select(Player.player_id).join(
            latest_dates,
            and_(
                Player.player_id == latest_dates.c.player_id,
                Player.bucket_id == bucket_id,
                Player.snapshot_date == latest_dates.c.max_date
            )
        ).distinct()
        
        if limit_players:
            players_query = players_query.limit(limit_players)
        
        result = await db.execute(players_query)
        player_ids = list(result.scalars().all())
        
        print(f"Found {len(player_ids)} players for season {bucket_id}")
        
        # Get already indexed events
        indexed_event_ids = await get_indexed_event_ids(db, bucket_id)
        print(f"Already have {len(indexed_event_ids)} events indexed")
        
        # Process players
        total_new_events = 0
        for idx, player_id in enumerate(player_ids, 1):
            try:
                new_events = await index_player_events(player_id, bucket_id, db, indexed_event_ids)
                total_new_events += new_events
                
                if idx % 10 == 0:
                    print(f"Processed {idx}/{len(player_ids)} players, {total_new_events} new events indexed so far")
                
                # Delay between players
                await asyncio.sleep(0.2)
            except Exception as e:
                print(f"Error processing player {player_id}: {e}")
                continue
        
        print(f"Indexing complete: {total_new_events} new events indexed")
        return total_new_events

async def index_season_events_with_status(bucket_id: int = 11, limit_players: Optional[int] = None, skip_processed: bool = True):
    """Index all events for a season with status tracking.
    
    Args:
        bucket_id: Season bucket ID
        limit_players: Optional limit on number of players to process
        skip_processed: If True, skip players who already have events indexed
    """
    import sys
    import importlib
    
    # Import main to access event_indexing_status
    if 'main' in sys.modules:
        main_module = sys.modules['main']
    else:
        # Try to import it
        try:
            main_module = importlib.import_module('main')
        except:
            main_module = None
    
    try:
        async with async_session_maker() as db:
            # Get all players for the season (latest snapshot)
            latest_dates = select(
                Player.player_id,
                func.max(Player.snapshot_date).label('max_date')
            ).where(
                Player.bucket_id == bucket_id
            ).group_by(Player.player_id).subquery()
            
            players_query = select(Player.player_id).join(
                latest_dates,
                and_(
                    Player.player_id == latest_dates.c.player_id,
                    Player.bucket_id == bucket_id,
                    Player.snapshot_date == latest_dates.c.max_date
                )
            ).distinct()
            
            if limit_players:
                players_query = players_query.limit(limit_players)
            
            result = await db.execute(players_query)
            player_ids = list(result.scalars().all())
            
            # Update status
            if main_module and hasattr(main_module, 'event_indexing_status'):
                if bucket_id in main_module.event_indexing_status:
                    main_module.event_indexing_status[bucket_id].update({
                        "total_players": len(player_ids),
                        "processed_players": 0,
                        "total_events": len(await get_indexed_event_ids(db, bucket_id))
                    })
            
            print(f"Found {len(player_ids)} players for season {bucket_id}")
            
            # Get already indexed events
            indexed_event_ids = await get_indexed_event_ids(db, bucket_id)
            initial_event_count = len(indexed_event_ids)
            print(f"Already have {initial_event_count} events indexed")
            
            # Get players who already have events indexed (if skip_processed is True)
            processed_player_ids = set()
            if skip_processed:
                # Find players who have at least one event indexed
                processed_query = select(PlayerEventStats.player_id.distinct()).join(
                    Event, PlayerEventStats.event_id == Event.event_id
                ).where(Event.bucket_id == bucket_id)
                processed_result = await db.execute(processed_query)
                processed_player_ids = set(processed_result.scalars().all())
                print(f"Found {len(processed_player_ids)} players with events already indexed (will skip)")
            
            # Update status with initial count
            if main_module and hasattr(main_module, 'event_indexing_status'):
                if bucket_id in main_module.event_indexing_status:
                    main_module.event_indexing_status[bucket_id]["initial_event_count"] = initial_event_count
                    main_module.event_indexing_status[bucket_id]["total_events"] = initial_event_count
                    main_module.event_indexing_status[bucket_id]["skipped_players"] = 0
            
            # Process players
            total_new_events = 0
            skipped_count = 0
            for idx, player_id in enumerate(player_ids, 1):
                try:
                    # Skip if already processed
                    if skip_processed and player_id in processed_player_ids:
                        skipped_count += 1
                        if main_module and hasattr(main_module, 'event_indexing_status'):
                            if bucket_id in main_module.event_indexing_status:
                                main_module.event_indexing_status[bucket_id]["skipped_players"] = skipped_count
                        continue
                    
                    new_events = await index_player_events(player_id, bucket_id, db, indexed_event_ids)
                    total_new_events += new_events
                    
                    # Update status AFTER indexing (so counts are accurate)
                    if main_module and hasattr(main_module, 'event_indexing_status'):
                        if bucket_id in main_module.event_indexing_status:
                            # Get current total events count from DB
                            current_total = len(await get_indexed_event_ids(db, bucket_id))
                            main_module.event_indexing_status[bucket_id].update({
                                "processed_players": idx,
                                "current_player": player_id,
                                "new_events_indexed": total_new_events,
                                "total_events": current_total
                            })
                    
                    if idx % 10 == 0:
                        print(f"Processed {idx}/{len(player_ids)} players, {total_new_events} new events indexed so far")
                    
                    # Delay between players (reduced for faster processing)
                    await asyncio.sleep(0.1)
                except Exception as e:
                    print(f"Error processing player {player_id}: {e}")
                    if main_module and hasattr(main_module, 'event_indexing_status'):
                        if bucket_id in main_module.event_indexing_status:
                            main_module.event_indexing_status[bucket_id]["error"] = str(e)
                    continue
            
            # Update final status
            if main_module and hasattr(main_module, 'event_indexing_status'):
                if bucket_id in main_module.event_indexing_status:
                    final_event_count = len(await get_indexed_event_ids(db, bucket_id))
                    main_module.event_indexing_status[bucket_id].update({
                        "status": "completed",
                        "processed_players": len(player_ids),
                        "new_events_indexed": total_new_events,
                        "total_events": final_event_count,
                        "completed_at": datetime.utcnow().isoformat()
                    })
            
            print(f"Indexing complete: {total_new_events} new events indexed")
            return total_new_events
    except Exception as e:
        print(f"Error in event indexing: {e}")
        import traceback
        traceback.print_exc()
        if main_module and hasattr(main_module, 'event_indexing_status'):
            if bucket_id in main_module.event_indexing_status:
                main_module.event_indexing_status[bucket_id].update({
                    "status": "error",
                    "error": str(e)
                })
        raise

# Open #2 Winter Haven event IDs (all brackets)
OPEN_2_WINTER_HAVEN_EVENT_IDS = [
    # Friday Events
    220556,  # Open Women's Singles
    220557,  # Open Junior Singles
    220558,  # Open Senior Singles
    220559,  # College Singles
    220584,  # High School Doubles
    220561,  # Upper Crew Cup
    220562,  # Lower Crew Cup
    220563,  # USA Forces Doubles Qualifier
    220564,  # Open Women's Doubles
    220566,  # Open Junior Doubles
    220565,  # Open Senior Doubles
    220567,  # Tier 1 Blind Draw
    220568,  # Tier 3 Blind Draw
    220569,  # Tier 4 Blind Draw
    # Saturday Events
    220572,  # Open Doubles Rounders
    221756,  # Tier 1 Doubles Bracket A
    221757,  # Tier 1 Doubles Bracket B
    221762,  # Tier 2 Doubles Bracket
    221763,  # Tier 3 Doubles Bracket
    221764,  # Tier 4 Doubles Bracket
    # Sunday Events
    220573,  # Tier 1 Singles Bracket A
    220574,  # Tier 1 Singles Bracket B
    220575,  # Tier 1 Singles Bracket C
    220576,  # Tier 1 Singles Bracket D
    220582,  # Tier 4 Singles Bracket
    220583,  # Tier 5 Singles Bracket
    220578,  # Tier 2 Singles Bracket
    220581,  # Tier 3 Singles Bracket A
    221437,  # Tier 3 Singles Bracket B
    # Finals
    220577,  # Tier 1 Singles Final
    221803,  # Tier 1 Doubles Final
]

async def index_player_events_local(player_id: int, bucket_id: int, db: AsyncSession, indexed_event_ids: Set[int]) -> int:
    """Index events for a player (LOCAL: Only Open #2 Winter Haven events). Returns number of new events indexed."""
    events_list = await fetch_player_events_list(player_id, bucket_id)
    if not events_list:
        return 0
    
    new_events = 0
    # LOCAL MODE: Only index Open #2 Winter Haven events
    target_event_ids = set(OPEN_2_WINTER_HAVEN_EVENT_IDS)
    
    for event_data in events_list:
        # The API returns leagueID, not eventID
        event_id = event_data.get("leagueID") or event_data.get("eventID") or event_data.get("event_id") or event_data.get("id")
        if not event_id:
            continue
        
        # LOCAL MODE: Only index Open #2 Winter Haven events
        # Check this FIRST so we can allow finals even if they're marked as local
        if event_id not in target_event_ids:
            continue
        
        # Skip local events EXCEPT if they're finals (which are in our target list)
        # Finals are marked as "L" but should still be indexed
        api_event_type = event_data.get("eventType") or event_data.get("event_type")
        event_name = event_data.get("leagueName", "") or event_data.get("leagueName", "") or ""
        
        # Allow events that:
        # 1. Are in our target list (already checked above), OR
        # 2. Are finals (contain "Final" in name) and are part of Open #2 Winter Haven
        is_final = "Final" in event_name or "final" in event_name
        is_open_2_winter_haven = "Open #2 Winter Haven" in event_name or "Open # 2 Winter Haven" in event_name
        
        # Skip local events UNLESS they're finals for Open #2 Winter Haven
        if api_event_type == "L" and not (is_final and is_open_2_winter_haven):
            continue
        
        detected_type = detect_event_type(event_name, event_data)
        if detected_type == "local" and not (is_final and is_open_2_winter_haven):
            continue
        
        # Skip if already indexed
        if event_id in indexed_event_ids:
            continue
        
        # Index the event
        success = await index_event(event_id, bucket_id, db)
        if success:
            indexed_event_ids.add(event_id)
            new_events += 1
        
        await asyncio.sleep(0.05)
    
    return new_events

async def index_season_events_with_status_local(bucket_id: int = 11, skip_processed: bool = True):
    """Index events for a season (LOCAL: 100 players max, Open #2 Winter Haven events only).
    
    Args:
        bucket_id: Season bucket ID
        skip_processed: If True, skip players who already have events indexed
    """
    import sys
    import importlib
    
    # Import main to access event_indexing_status
    if 'main' in sys.modules:
        main_module = sys.modules['main']
    else:
        try:
            main_module = importlib.import_module('main')
        except:
            main_module = None
    
    try:
        async with async_session_maker() as db:
            # Get all players for the season (latest snapshot) - LIMITED TO 100
            latest_dates = select(
                Player.player_id,
                func.max(Player.snapshot_date).label('max_date')
            ).where(
                Player.bucket_id == bucket_id
            ).group_by(Player.player_id).subquery()
            
            players_query = select(Player.player_id).join(
                latest_dates,
                and_(
                    Player.player_id == latest_dates.c.player_id,
                    Player.bucket_id == bucket_id,
                    Player.snapshot_date == latest_dates.c.max_date
                )
            ).distinct().limit(100)  # LOCAL MODE: Limit to 100 players
            
            result = await db.execute(players_query)
            player_ids = list(result.scalars().all())
            
            # Initialize status
            if main_module and hasattr(main_module, 'event_indexing_status'):
                if bucket_id not in main_module.event_indexing_status:
                    from datetime import datetime
                    main_module.event_indexing_status[bucket_id] = {
                        "status": "running",
                        "bucket_id": bucket_id,
                        "started_at": datetime.utcnow().isoformat(),
                        "total_players": len(player_ids),
                        "processed_players": 0,
                        "current_player": None,
                        "new_events_indexed": 0,
                        "total_events": 0,
                        "skipped_players": 0,
                        "error": None,
                        "local_mode": True
                    }
                else:
                    main_module.event_indexing_status[bucket_id].update({
                        "total_players": len(player_ids),
                        "processed_players": 0,
                        "status": "running",
                        "local_mode": True
                    })
            
            print(f"LOCAL MODE: Found {len(player_ids)} players for season {bucket_id} (limited to 100)")
            
            # Get already indexed events
            indexed_event_ids = await get_indexed_event_ids(db, bucket_id)
            initial_event_count = len(indexed_event_ids)
            print(f"Already have {initial_event_count} events indexed")
            
            # Get players who already have events indexed (if skip_processed is True)
            processed_player_ids = set()
            if skip_processed:
                processed_query = select(PlayerEventStats.player_id.distinct()).join(
                    Event, PlayerEventStats.event_id == Event.event_id
                ).where(Event.bucket_id == bucket_id)
                processed_result = await db.execute(processed_query)
                processed_player_ids = set(processed_result.scalars().all())
                print(f"Found {len(processed_player_ids)} players with events already indexed (will skip)")
            
            # Update status with initial count
            if main_module and hasattr(main_module, 'event_indexing_status'):
                if bucket_id in main_module.event_indexing_status:
                    main_module.event_indexing_status[bucket_id]["initial_event_count"] = initial_event_count
                    main_module.event_indexing_status[bucket_id]["total_events"] = initial_event_count
                    main_module.event_indexing_status[bucket_id]["skipped_players"] = 0
            
            # Process players
            total_new_events = 0
            skipped_count = 0
            for idx, player_id in enumerate(player_ids, 1):
                try:
                    # Skip if already processed
                    if skip_processed and player_id in processed_player_ids:
                        skipped_count += 1
                        if main_module and hasattr(main_module, 'event_indexing_status'):
                            if bucket_id in main_module.event_indexing_status:
                                main_module.event_indexing_status[bucket_id]["skipped_players"] = skipped_count
                        continue
                    
                    # Use local version that filters events
                    new_events = await index_player_events_local(player_id, bucket_id, db, indexed_event_ids)
                    total_new_events += new_events
                    
                    # Update status
                    if main_module and hasattr(main_module, 'event_indexing_status'):
                        if bucket_id in main_module.event_indexing_status:
                            current_total = len(await get_indexed_event_ids(db, bucket_id))
                            main_module.event_indexing_status[bucket_id].update({
                                "processed_players": idx,
                                "current_player": player_id,
                                "new_events_indexed": total_new_events,
                                "total_events": current_total
                            })
                    
                    if idx % 10 == 0:
                        print(f"Processed {idx}/{len(player_ids)} players, {total_new_events} new events indexed so far (LOCAL: Open #2 Winter Haven events only)")
                    
                    await asyncio.sleep(0.1)
                except Exception as e:
                    print(f"Error processing player {player_id}: {e}")
                    if main_module and hasattr(main_module, 'event_indexing_status'):
                        if bucket_id in main_module.event_indexing_status:
                            main_module.event_indexing_status[bucket_id]["error"] = str(e)
                    continue
            
            # Update final status
            if main_module and hasattr(main_module, 'event_indexing_status'):
                if bucket_id in main_module.event_indexing_status:
                    from datetime import datetime
                    final_event_count = len(await get_indexed_event_ids(db, bucket_id))
                    main_module.event_indexing_status[bucket_id].update({
                        "status": "completed",
                        "processed_players": len(player_ids),
                        "new_events_indexed": total_new_events,
                        "total_events": final_event_count,
                        "completed_at": datetime.utcnow().isoformat()
                    })
            
            print(f"LOCAL MODE indexing complete: {total_new_events} new events indexed (Open #2 Winter Haven events: {len(OPEN_2_WINTER_HAVEN_EVENT_IDS)} events)")
            return total_new_events
    except Exception as e:
        print(f"Error in local event indexing: {e}")
        import traceback
        traceback.print_exc()
        if main_module and hasattr(main_module, 'event_indexing_status'):
            if bucket_id in main_module.event_indexing_status:
                main_module.event_indexing_status[bucket_id].update({
                    "status": "error",
                    "error": str(e)
                })
        raise

