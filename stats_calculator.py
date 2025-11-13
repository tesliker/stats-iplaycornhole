"""Functions for calculating and storing aggregated event statistics."""

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from database import EventGame, EventStanding, Event, EventAggregatedStats, Player, EventMatch
from typing import Dict, List, Optional, Set, Tuple
import hashlib
import json
from datetime import datetime


def is_doubles_event(event: Event) -> bool:
    """Check if an event is doubles based on bracket_name."""
    if not event.bracket_name:
        return False
    return "doubles" in event.bracket_name.lower()


async def extract_doubles_partners(
    event_id: int,
    games: List[EventGame],
    db: AsyncSession
) -> Dict[int, int]:
    """Extract partner relationships for doubles events.
    
    Primary method: Use EventStanding - players with same final_rank are partners.
    Fallback: Extract from game raw_data if standings don't have consistent pairs.
    
    Returns a dict mapping player_id -> partner_id.
    """
    partners = {}
    
    # PRIMARY METHOD: Use EventStanding - in doubles, partners have the same final_rank
    standings_query = select(EventStanding).where(EventStanding.event_id == event_id).order_by(EventStanding.final_rank)
    standings_result = await db.execute(standings_query)
    standings = standings_result.scalars().all()
    
    # Group by rank
    rank_groups = {}
    for standing in standings:
        rank = standing.final_rank
        if rank not in rank_groups:
            rank_groups[rank] = []
        rank_groups[rank].append(standing.player_id)
    
    # For doubles, each rank should have exactly 2 players (partners)
    # But also handle cases where there might be more (e.g., ties)
    for rank, player_ids in rank_groups.items():
        if len(player_ids) == 2:
            # Perfect - exactly 2 players at this rank, they are partners
            partners[player_ids[0]] = player_ids[1]
            partners[player_ids[1]] = player_ids[0]
        elif len(player_ids) > 2:
            # More than 2 players at same rank - this shouldn't happen in doubles
            # But if it does, we can't reliably pair them from standings alone
            # Will fall back to raw_data extraction
            pass
    
    # FALLBACK/SUPPLEMENT: Always try raw_data to fill in missing partners
    # Collect all partner pairs from games
    all_partner_pairs = []
    
    for game in games:
        if not game.raw_data:
            continue
        
        event_match_details = game.raw_data.get("event_match_details") or game.raw_data.get("eventMatchDetails") or []
        
        # For doubles, event_match_details should have 4 players
        # Players 0-1 are team 1, players 2-3 are team 2
        if len(event_match_details) >= 4:
            # Extract player IDs
            def get_player_id(player_data):
                val = (player_data.get("playerid") or player_data.get("player_id") or 
                       player_data.get("playerId") or player_data.get("playerID"))
                try:
                    return int(val) if val else None
                except (ValueError, TypeError):
                    return None
            
            team1_player1 = get_player_id(event_match_details[0])
            team1_player2 = get_player_id(event_match_details[1])
            team2_player1 = get_player_id(event_match_details[2])
            team2_player2 = get_player_id(event_match_details[3])
            
            # Store partner pairs (use sorted tuple for consistency)
            if team1_player1 and team1_player2:
                pair = tuple(sorted([team1_player1, team1_player2]))
                all_partner_pairs.append(pair)
            if team2_player1 and team2_player2:
                pair = tuple(sorted([team2_player1, team2_player2]))
                all_partner_pairs.append(pair)
    
    # Build partner dict from consistent pairs
    # Find pairs that appear in multiple games (most consistent)
    from collections import Counter
    pair_counts = Counter(all_partner_pairs)
    
    # Add pairs from raw_data (only if not already in partners, or to supplement)
    for pair, count in pair_counts.most_common():
        if count >= 1:  # At least one game
            # Only add if both players aren't already in partners, or if one is missing
            p1, p2 = pair
            if p1 not in partners or p2 not in partners:
                # Add if not conflicting
                if p1 not in partners or partners[p1] == p2:
                    if p2 not in partners or partners[p2] == p1:
                        partners[p1] = p2
                        partners[p2] = p1
    
    return partners


async def calculate_bracket_stats(
    event_id: int,
    db: AsyncSession
) -> Dict:
    """Calculate stats for a single bracket/event (bracket-specific rankings).
    
    Returns a dict with player_stats (list) and metadata.
    Uses EventStanding.final_rank for bracket-specific rankings.
    For doubles, groups partners together and displays both names.
    """
    # Get event to check if doubles
    event_query = select(Event).where(Event.event_id == event_id)
    event_result = await db.execute(event_query)
    event = event_result.scalar_one_or_none()
    
    is_doubles = False
    if event:
        is_doubles = is_doubles_event(event)
    
    # Get standings first to ensure we have all players
    standings_query = select(EventStanding).where(EventStanding.event_id == event_id)
    standings_result = await db.execute(standings_query)
    standings = standings_result.scalars().all()
    
    # Get all games from this event
    games_query = select(EventGame).where(EventGame.event_id == event_id)
    games_result = await db.execute(games_query)
    games = games_result.scalars().all()
    
    # Extract partner relationships for doubles (use standings + games)
    partners = {}
    if is_doubles:
        partners = await extract_doubles_partners(event_id, games, db)
    
    # Aggregate stats by player
    player_stats = {}
    
    def get_player_id(player_data):
        """Extract player ID from player data dict."""
        val = (player_data.get("playerid") or player_data.get("player_id") or 
               player_data.get("playerId") or player_data.get("playerID"))
        try:
            return int(val) if val else None
        except (ValueError, TypeError):
            return None
    
    def get_int(data, *keys, default=0):
        """Extract integer value from data dict."""
        for key in keys:
            val = data.get(key)
            if val is not None:
                try:
                    return int(val)
                except (ValueError, TypeError):
                    continue
        return default
    
    def get_float(data, *keys, default=0.0):
        """Extract float value from data dict."""
        for key in keys:
            val = data.get(key)
            if val is not None:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    continue
        return default
    
    for game in games:
        if is_doubles and game.raw_data:
            # For doubles, extract all 4 players from raw_data
            event_match_details = game.raw_data.get("event_match_details") or game.raw_data.get("eventMatchDetails") or []
            
            if len(event_match_details) >= 4:
                # Doubles: 4 players (2 per team)
                # Players 0-1 are team 1, players 2-3 are team 2
                team1_players = [event_match_details[0], event_match_details[1]]
                team2_players = [event_match_details[2], event_match_details[3]]
                
                # Calculate team scores (sum of both players' points)
                team1_points = sum(get_int(p, "totalpts", "total_pts", "totalPts", "totalPoints", "points") for p in team1_players)
                team2_points = sum(get_int(p, "totalpts", "total_pts", "totalPts", "totalPoints", "points") for p in team2_players)
                
                # Process each individual player
                for player_data in team1_players + team2_players:
                    player_id = get_player_id(player_data)
                    if not player_id:
                        continue
                    
                    if player_id not in player_stats:
                        player_stats[player_id] = {
                            "player_id": player_id,
                            "games_played": 0,
                            "total_points": 0,
                            "total_rounds": 0,
                            "total_bags_in": 0,
                            "total_bags_on": 0,
                            "total_bags_off": 0,
                            "total_bags_thrown": 0,
                            "total_four_baggers": 0,
                            "total_opponent_points": 0,
                            "total_opponent_ppr": 0.0,
                            "wins": 0,
                            "losses": 0,
                        }
                    
                    p = player_stats[player_id]
                    p["games_played"] += 1
                    p["total_points"] += get_int(player_data, "totalpts", "total_pts", "totalPts", "totalPoints", "points")
                    p["total_rounds"] += get_int(player_data, "rounds", "rounds_played", "roundsPlayed", "roundsTotal", "rounds_total")
                    p["total_bags_in"] += get_int(player_data, "bagsin", "bags_in", "bagsIn", "bagsInTotal")
                    p["total_bags_on"] += get_int(player_data, "bagson", "bags_on", "bagsOn", "bagsOnTotal")
                    p["total_bags_off"] += get_int(player_data, "bagsoff", "bags_off", "bagsOff", "bagsOffTotal")
                    p["total_bags_thrown"] += get_int(player_data, "totalbagsthrown", "total_bags_thrown", "totalBagsThrown", "totalBags", "bags_thrown")
                    p["total_four_baggers"] += get_int(player_data, "totalfourbaggers", "total_four_baggers", "totalFourBaggers", "fourBaggers", "four_baggers")
                    
                    # Opponent stats: get from the other team
                    if player_data in team1_players:
                        # Opponent is team 2
                        opp_points = team2_points
                        opp_ppr = sum(get_float(p2, "ptsperrnd", "pts_per_rnd", "ptsPerRnd", "pointsPerRound", "ppr") for p2 in team2_players) / len(team2_players) if team2_players else 0.0
                    else:
                        # Opponent is team 1
                        opp_points = team1_points
                        opp_ppr = sum(get_float(p1, "ptsperrnd", "pts_per_rnd", "ptsPerRnd", "pointsPerRound", "ppr") for p1 in team1_players) / len(team1_players) if team1_players else 0.0
                    
                    p["total_opponent_points"] += opp_points
                    p["total_opponent_ppr"] += opp_ppr
                    
                    # Win/loss: team wins if team score is higher
                    if player_data in team1_players:
                        if team1_points > team2_points:
                            p["wins"] += 1
                        elif team2_points > team1_points:
                            p["losses"] += 1
                    else:
                        if team2_points > team1_points:
                            p["wins"] += 1
                        elif team1_points > team2_points:
                            p["losses"] += 1
            else:
                # Fallback: use player1_id and player2_id if raw_data doesn't have 4 players
                if game.player1_id:
                    if game.player1_id not in player_stats:
                        player_stats[game.player1_id] = {
                            "player_id": game.player1_id,
                            "games_played": 0,
                            "total_points": 0,
                            "total_rounds": 0,
                            "total_bags_in": 0,
                            "total_bags_on": 0,
                            "total_bags_off": 0,
                            "total_bags_thrown": 0,
                            "total_four_baggers": 0,
                            "total_opponent_points": 0,
                            "total_opponent_ppr": 0.0,
                            "wins": 0,
                            "losses": 0,
                        }
                    
                    p = player_stats[game.player1_id]
                    p["games_played"] += 1
                    p["total_points"] += game.player1_points or 0
                    p["total_rounds"] += game.player1_rounds or 0
                    p["total_bags_in"] += game.player1_bags_in or 0
                    p["total_bags_on"] += game.player1_bags_on or 0
                    p["total_bags_off"] += game.player1_bags_off or 0
                    p["total_bags_thrown"] += game.player1_total_bags_thrown or 0
                    p["total_four_baggers"] += game.player1_four_baggers or 0
                    p["total_opponent_points"] += game.player1_opponent_points or 0
                    p["total_opponent_ppr"] += game.player1_opponent_ppr or 0.0
                    
                    if game.player1_points and game.player2_points:
                        if game.player1_points > game.player2_points:
                            p["wins"] += 1
                        else:
                            p["losses"] += 1
                
                if game.player2_id:
                    if game.player2_id not in player_stats:
                        player_stats[game.player2_id] = {
                            "player_id": game.player2_id,
                            "games_played": 0,
                            "total_points": 0,
                            "total_rounds": 0,
                            "total_bags_in": 0,
                            "total_bags_on": 0,
                            "total_bags_off": 0,
                            "total_bags_thrown": 0,
                            "total_four_baggers": 0,
                            "total_opponent_points": 0,
                            "total_opponent_ppr": 0.0,
                            "wins": 0,
                            "losses": 0,
                        }
                    
                    p = player_stats[game.player2_id]
                    p["games_played"] += 1
                    p["total_points"] += game.player2_points or 0
                    p["total_rounds"] += game.player2_rounds or 0
                    p["total_bags_in"] += game.player2_bags_in or 0
                    p["total_bags_on"] += game.player2_bags_on or 0
                    p["total_bags_off"] += game.player2_bags_off or 0
                    p["total_bags_thrown"] += game.player2_total_bags_thrown or 0
                    p["total_four_baggers"] += game.player2_four_baggers or 0
                    p["total_opponent_points"] += game.player2_opponent_points or 0
                    p["total_opponent_ppr"] += game.player2_opponent_ppr or 0.0
                    
                    if game.player1_points and game.player2_points:
                        if game.player2_points > game.player1_points:
                            p["wins"] += 1
                        else:
                            p["losses"] += 1
        else:
            # Singles: use player1_id and player2_id directly
            if game.player1_id:
                if game.player1_id not in player_stats:
                    player_stats[game.player1_id] = {
                        "player_id": game.player1_id,
                        "games_played": 0,
                        "total_points": 0,
                        "total_rounds": 0,
                        "total_bags_in": 0,
                        "total_bags_on": 0,
                        "total_bags_off": 0,
                        "total_bags_thrown": 0,
                        "total_four_baggers": 0,
                        "total_opponent_points": 0,
                        "total_opponent_ppr": 0.0,
                        "wins": 0,
                        "losses": 0,
                    }
                
                p = player_stats[game.player1_id]
                p["games_played"] += 1
                p["total_points"] += game.player1_points or 0
                p["total_rounds"] += game.player1_rounds or 0
                p["total_bags_in"] += game.player1_bags_in or 0
                p["total_bags_on"] += game.player1_bags_on or 0
                p["total_bags_off"] += game.player1_bags_off or 0
                p["total_bags_thrown"] += game.player1_total_bags_thrown or 0
                p["total_four_baggers"] += game.player1_four_baggers or 0
                p["total_opponent_points"] += game.player1_opponent_points or 0
                p["total_opponent_ppr"] += game.player1_opponent_ppr or 0.0
                
                if game.player1_points and game.player2_points:
                    if game.player1_points > game.player2_points:
                        p["wins"] += 1
                    else:
                        p["losses"] += 1
            
            if game.player2_id:
                if game.player2_id not in player_stats:
                    player_stats[game.player2_id] = {
                        "player_id": game.player2_id,
                        "games_played": 0,
                        "total_points": 0,
                        "total_rounds": 0,
                        "total_bags_in": 0,
                        "total_bags_on": 0,
                        "total_bags_off": 0,
                        "total_bags_thrown": 0,
                        "total_four_baggers": 0,
                        "total_opponent_points": 0,
                        "total_opponent_ppr": 0.0,
                        "wins": 0,
                        "losses": 0,
                    }
                
                p = player_stats[game.player2_id]
                p["games_played"] += 1
                p["total_points"] += game.player2_points or 0
                p["total_rounds"] += game.player2_rounds or 0
                p["total_bags_in"] += game.player2_bags_in or 0
                p["total_bags_on"] += game.player2_bags_on or 0
                p["total_bags_off"] += game.player2_bags_off or 0
                p["total_bags_thrown"] += game.player2_total_bags_thrown or 0
                p["total_four_baggers"] += game.player2_four_baggers or 0
                p["total_opponent_points"] += game.player2_opponent_points or 0
                p["total_opponent_ppr"] += game.player2_opponent_ppr or 0.0
                
                if game.player1_points and game.player2_points:
                    if game.player2_points > game.player1_points:
                        p["wins"] += 1
                    else:
                        p["losses"] += 1
    
    # Use standings we already fetched earlier
    standings_dict = {standing.player_id: standing.final_rank for standing in standings}
    
    # Also group by rank for doubles partner lookup
    rank_groups = {}
    for standing in standings:
        rank = standing.final_rank
        if rank not in rank_groups:
            rank_groups[rank] = []
        rank_groups[rank].append(standing.player_id)
    
    # For doubles, ensure all players from standings are included in player_stats
    # (even if they don't have game stats yet)
    if is_doubles:
        for standing in standings:
            player_id = standing.player_id
            if player_id not in player_stats:
                # Add placeholder stats for players who appear in standings but not in games
                player_stats[player_id] = {
                    "player_id": player_id,
                    "games_played": 0,
                    "total_points": 0,
                    "total_rounds": 0,
                    "total_bags_in": 0,
                    "total_bags_on": 0,
                    "total_bags_off": 0,
                    "total_bags_thrown": 0,
                    "total_four_baggers": 0,
                    "total_opponent_points": 0,
                    "total_opponent_ppr": 0.0,
                    "wins": 0,
                    "losses": 0,
                }
    
    # Get player names and CPI - include all players from standings and stats
    all_player_ids = set(player_stats.keys())
    all_player_ids.update(standings_dict.keys())  # Include all players from standings
    player_ids_list = list(all_player_ids)
    player_names_dict = {}
    player_cpi_dict = {}
    
    if player_ids_list:
        latest_player_dates = select(
            Player.player_id,
            func.max(Player.snapshot_date).label('max_date')
        ).group_by(Player.player_id).subquery()
        
        latest_players_query = select(
            Player.player_id,
            Player.first_name,
            Player.last_name,
            Player.player_cpi
        ).join(
            latest_player_dates,
            and_(
                Player.player_id == latest_player_dates.c.player_id,
                Player.snapshot_date == latest_player_dates.c.max_date
            )
        ).where(Player.player_id.in_(player_ids_list))
        
        players_result = await db.execute(latest_players_query)
        for row in players_result.all():
            player_id = row[0]
            player_names_dict[player_id] = (row[1] or "", row[2] or "")
            if row[3] is not None:
                player_cpi_dict[player_id] = row[3]
    
    # For doubles, add partner_id to each player's stats (but keep separate rows)
    if is_doubles and partners:
        # Add partner_id to each player's stats, but don't combine them
        for player_id in player_stats:
            partner_id = partners.get(player_id)
            if partner_id:
                player_stats[player_id]["partner_id"] = partner_id
            else:
                player_stats[player_id]["partner_id"] = None
    else:
        # Singles - keep individual stats, add partner_id=None for consistency
        for player_id in player_stats:
            if "partner_id" not in player_stats[player_id]:
                player_stats[player_id]["partner_id"] = None
    
    # Calculate derived stats and format for storage
    stats_list = []
    
    for player_id, stats in player_stats.items():
        
        games_played = stats["games_played"]
        # Include players even if they have 0 games (they might be in standings)
        if games_played == 0 and not is_doubles:
            continue
        
        if stats["total_rounds"] > 0:
            ppr = stats["total_points"] / stats["total_rounds"]
            opp_ppr = stats["total_opponent_ppr"] / games_played if games_played > 0 else 0.0
            dpr = ppr - opp_ppr
        else:
            ppr = 0.0
            opp_ppr = 0.0
            dpr = 0.0
        
        bags_in_pct = stats["total_bags_in"] / stats["total_bags_thrown"] if stats["total_bags_thrown"] > 0 else 0.0
        bags_on_pct = stats["total_bags_on"] / stats["total_bags_thrown"] if stats["total_bags_thrown"] > 0 else 0.0
        bags_off_pct = stats["total_bags_off"] / stats["total_bags_thrown"] if stats["total_bags_thrown"] > 0 else 0.0
        four_bagger_pct = stats["total_four_baggers"] / stats["total_rounds"] if stats["total_rounds"] > 0 else 0.0
        win_pct = stats["wins"] / games_played if games_played > 0 else 0.0
        
        # Get player name(s)
        player_id = stats["player_id"]
        first_name, last_name = player_names_dict.get(player_id, ("", ""))
        player_name = f"{first_name} {last_name}".strip() or f"Player {player_id}"
        
        # For doubles, add partner name in parentheses
        partner_id = stats.get("partner_id")
        if is_doubles and partner_id:
            partner_first, partner_last = player_names_dict.get(partner_id, ("", ""))
            partner_name = f"{partner_first} {partner_last}".strip() or f"Player {partner_id}"
            if partner_name != f"Player {partner_id}":  # Only add if we have a real name
                player_name = f"{player_name} ({partner_name})"
        
        # Get bracket-specific rank from standings
        # For doubles, both partners have the same rank in standings (that's how we identify them)
        bracket_rank = standings_dict.get(player_id)
        if is_doubles and stats.get("partner_id"):
            # Partners should have the same rank in standings - verify and use it
            partner_rank = standings_dict.get(stats["partner_id"])
            if partner_rank is not None:
                # Use partner's rank (should be same as player's, but partner might be more reliable)
                bracket_rank = partner_rank
            elif bracket_rank is None:
                # Try to find rank by looking at which rank has both players
                for rank, player_ids in rank_groups.items():
                    if player_id in player_ids and stats.get("partner_id") in player_ids:
                        bracket_rank = rank
                        break
        
        # Get CPI (individual, not averaged for doubles)
        cpi = player_cpi_dict.get(player_id)
        
        stats_list.append({
            "player_id": player_id,
            "partner_id": stats.get("partner_id"),
            "player_name": player_name,
            "overall_rank": bracket_rank,  # Use bracket-specific rank from standings
            "games_played": games_played,
            "wins": stats["wins"],
            "losses": stats["losses"],
            "win_pct": win_pct,
            "rounds_total": stats["total_rounds"],
            "rounds_avg": stats["total_rounds"] / games_played if games_played > 0 else 0.0,
            "ppr": round(ppr, 3),
            "dpr": round(dpr, 3),
            "player_cpi": round(cpi, 2) if cpi is not None else None,
            "bags_in_pct": round(bags_in_pct, 4),
            "bags_on_pct": round(bags_on_pct, 4),
            "bags_off_pct": round(bags_off_pct, 4),
            "four_bagger_pct": round(four_bagger_pct, 4),
            "opponent_ppr": round(opp_ppr, 3),
            "event_ids": [event_id],  # Single event for bracket view
        })
    
    # Format ties (multiple players with same rank)
    # For doubles, partners already share rank, so don't add "T-" prefix
    rank_groups = {}
    for player in stats_list:
        rank = player["overall_rank"]
        if rank is not None:
            if rank not in rank_groups:
                rank_groups[rank] = []
            rank_groups[rank].append(player)
    
    # Format ties - but skip for doubles (partners already share rank)
    if not is_doubles:
        for rank, players in rank_groups.items():
            if len(players) > 1:
                # Multiple players with same rank - format as tie
                for player in players:
                    player["overall_rank"] = f"T-{rank}"
    
    # Sort by rank
    def sort_key(x):
        rank = x["overall_rank"]
        if isinstance(rank, int):
            return (0, rank)
        elif isinstance(rank, str):
            if rank.startswith("T-"):
                try:
                    return (1, int(rank.split("-")[1]))
                except:
                    return (2, 999)
        return (2, 999)
    
    stats_list.sort(key=sort_key)
    
    return {
        "player_stats": stats_list,
        "total_players": len(stats_list),
        "total_games": len(games),
    }


async def calculate_event_aggregated_stats(
    event_ids: List[int],
    base_event_name: str,
    bracket_type: str,
    db: AsyncSession,
    group_type: str = "grouped"
) -> Dict:
    """Calculate aggregated stats for a group of events.
    
    Returns a dict with player_stats (list) and metadata.
    """
    # Check if any of these events are doubles
    events_query = select(Event).where(Event.event_id.in_(event_ids))
    events_result = await db.execute(events_query)
    events = events_result.scalars().all()
    
    is_doubles = False
    for event in events:
        if is_doubles_event(event):
            is_doubles = True
            break
    
    # Get all games from these events
    games_query = select(EventGame).where(EventGame.event_id.in_(event_ids))
    games_result = await db.execute(games_query)
    games = games_result.scalars().all()
    
    # Aggregate stats by player
    player_stats = {}
    
    def get_player_id(player_data):
        """Extract player ID from player data dict."""
        val = (player_data.get("playerid") or player_data.get("player_id") or 
               player_data.get("playerId") or player_data.get("playerID"))
        try:
            return int(val) if val else None
        except (ValueError, TypeError):
            return None
    
    def get_int(data, *keys, default=0):
        """Extract integer value from data dict."""
        for key in keys:
            val = data.get(key)
            if val is not None:
                try:
                    return int(val)
                except (ValueError, TypeError):
                    continue
        return default
    
    def get_float(data, *keys, default=0.0):
        """Extract float value from data dict."""
        for key in keys:
            val = data.get(key)
            if val is not None:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    continue
        return default
    
    for game in games:
        if is_doubles and game.raw_data:
            # For doubles, extract all 4 players from raw_data
            event_match_details = game.raw_data.get("event_match_details") or game.raw_data.get("eventMatchDetails") or []
            
            if len(event_match_details) >= 4:
                # Doubles: 4 players (2 per team)
                team1_players = [event_match_details[0], event_match_details[1]]
                team2_players = [event_match_details[2], event_match_details[3]]
                
                # Calculate team scores
                team1_points = sum(get_int(p, "totalpts", "total_pts", "totalPts", "totalPoints", "points") for p in team1_players)
                team2_points = sum(get_int(p, "totalpts", "total_pts", "totalPts", "totalPoints", "points") for p in team2_players)
                
                # Process each individual player
                for player_data in team1_players + team2_players:
                    player_id = get_player_id(player_data)
                    if not player_id:
                        continue
                    
                    if player_id not in player_stats:
                        player_stats[player_id] = {
                            "player_id": player_id,
                            "games_played": 0,
                            "total_points": 0,
                            "total_rounds": 0,
                            "total_bags_in": 0,
                            "total_bags_on": 0,
                            "total_bags_off": 0,
                            "total_bags_thrown": 0,
                            "total_four_baggers": 0,
                            "total_opponent_points": 0,
                            "total_opponent_ppr": 0.0,
                            "wins": 0,
                            "losses": 0,
                            "bracket_ranks": {}
                        }
                    
                    p = player_stats[player_id]
                    p["games_played"] += 1
                    p["total_points"] += get_int(player_data, "totalpts", "total_pts", "totalPts", "totalPoints", "points")
                    p["total_rounds"] += get_int(player_data, "rounds", "rounds_played", "roundsPlayed", "roundsTotal", "rounds_total")
                    p["total_bags_in"] += get_int(player_data, "bagsin", "bags_in", "bagsIn", "bagsInTotal")
                    p["total_bags_on"] += get_int(player_data, "bagson", "bags_on", "bagsOn", "bagsOnTotal")
                    p["total_bags_off"] += get_int(player_data, "bagsoff", "bags_off", "bagsOff", "bagsOffTotal")
                    p["total_bags_thrown"] += get_int(player_data, "totalbagsthrown", "total_bags_thrown", "totalBagsThrown", "totalBags", "bags_thrown")
                    p["total_four_baggers"] += get_int(player_data, "totalfourbaggers", "total_four_baggers", "totalFourBaggers", "fourBaggers", "four_baggers")
                    
                    # Opponent stats
                    if player_data in team1_players:
                        opp_points = team2_points
                        opp_ppr = sum(get_float(p2, "ptsperrnd", "pts_per_rnd", "ptsPerRnd", "pointsPerRound", "ppr") for p2 in team2_players) / len(team2_players) if team2_players else 0.0
                    else:
                        opp_points = team1_points
                        opp_ppr = sum(get_float(p1, "ptsperrnd", "pts_per_rnd", "ptsPerRnd", "pointsPerRound", "ppr") for p1 in team1_players) / len(team1_players) if team1_players else 0.0
                    
                    p["total_opponent_points"] += opp_points
                    p["total_opponent_ppr"] += opp_ppr
                    
                    # Win/loss
                    if player_data in team1_players:
                        if team1_points > team2_points:
                            p["wins"] += 1
                        elif team2_points > team1_points:
                            p["losses"] += 1
                    else:
                        if team2_points > team1_points:
                            p["wins"] += 1
                        elif team1_points > team2_points:
                            p["losses"] += 1
            else:
                # Fallback: use player1_id and player2_id
                if game.player1_id:
                    if game.player1_id not in player_stats:
                        player_stats[game.player1_id] = {
                            "player_id": game.player1_id,
                            "games_played": 0,
                            "total_points": 0,
                            "total_rounds": 0,
                            "total_bags_in": 0,
                            "total_bags_on": 0,
                            "total_bags_off": 0,
                            "total_bags_thrown": 0,
                            "total_four_baggers": 0,
                            "total_opponent_points": 0,
                            "total_opponent_ppr": 0.0,
                            "wins": 0,
                            "losses": 0,
                            "bracket_ranks": {}
                        }
                    
                    p = player_stats[game.player1_id]
                    p["games_played"] += 1
                    p["total_points"] += game.player1_points or 0
                    p["total_rounds"] += game.player1_rounds or 0
                    p["total_bags_in"] += game.player1_bags_in or 0
                    p["total_bags_on"] += game.player1_bags_on or 0
                    p["total_bags_off"] += game.player1_bags_off or 0
                    p["total_bags_thrown"] += game.player1_total_bags_thrown or 0
                    p["total_four_baggers"] += game.player1_four_baggers or 0
                    p["total_opponent_points"] += game.player1_opponent_points or 0
                    p["total_opponent_ppr"] += game.player1_opponent_ppr or 0.0
                    
                    if game.player1_points and game.player2_points:
                        if game.player1_points > game.player2_points:
                            p["wins"] += 1
                        else:
                            p["losses"] += 1
                
                if game.player2_id:
                    if game.player2_id not in player_stats:
                        player_stats[game.player2_id] = {
                            "player_id": game.player2_id,
                            "games_played": 0,
                            "total_points": 0,
                            "total_rounds": 0,
                            "total_bags_in": 0,
                            "total_bags_on": 0,
                            "total_bags_off": 0,
                            "total_bags_thrown": 0,
                            "total_four_baggers": 0,
                            "total_opponent_points": 0,
                            "total_opponent_ppr": 0.0,
                            "wins": 0,
                            "losses": 0,
                            "bracket_ranks": {}
                        }
                    
                    p = player_stats[game.player2_id]
                    p["games_played"] += 1
                    p["total_points"] += game.player2_points or 0
                    p["total_rounds"] += game.player2_rounds or 0
                    p["total_bags_in"] += game.player2_bags_in or 0
                    p["total_bags_on"] += game.player2_bags_on or 0
                    p["total_bags_off"] += game.player2_bags_off or 0
                    p["total_bags_thrown"] += game.player2_total_bags_thrown or 0
                    p["total_four_baggers"] += game.player2_four_baggers or 0
                    p["total_opponent_points"] += game.player2_opponent_points or 0
                    p["total_opponent_ppr"] += game.player2_opponent_ppr or 0.0
                    
                    if game.player1_points and game.player2_points:
                        if game.player2_points > game.player1_points:
                            p["wins"] += 1
                        else:
                            p["losses"] += 1
        else:
            # Singles: use player1_id and player2_id directly
            if game.player1_id:
                if game.player1_id not in player_stats:
                    player_stats[game.player1_id] = {
                        "player_id": game.player1_id,
                        "games_played": 0,
                        "total_points": 0,
                        "total_rounds": 0,
                        "total_bags_in": 0,
                        "total_bags_on": 0,
                        "total_bags_off": 0,
                        "total_bags_thrown": 0,
                        "total_four_baggers": 0,
                        "total_opponent_points": 0,
                        "total_opponent_ppr": 0.0,
                        "wins": 0,
                        "losses": 0,
                        "bracket_ranks": {}
                    }
                
                p = player_stats[game.player1_id]
                p["games_played"] += 1
                p["total_points"] += game.player1_points or 0
                p["total_rounds"] += game.player1_rounds or 0
                p["total_bags_in"] += game.player1_bags_in or 0
                p["total_bags_on"] += game.player1_bags_on or 0
                p["total_bags_off"] += game.player1_bags_off or 0
                p["total_bags_thrown"] += game.player1_total_bags_thrown or 0
                p["total_four_baggers"] += game.player1_four_baggers or 0
                p["total_opponent_points"] += game.player1_opponent_points or 0
                p["total_opponent_ppr"] += game.player1_opponent_ppr or 0.0
                
                if game.player1_points and game.player2_points:
                    if game.player1_points > game.player2_points:
                        p["wins"] += 1
                    else:
                        p["losses"] += 1
            
            if game.player2_id:
                if game.player2_id not in player_stats:
                    player_stats[game.player2_id] = {
                        "player_id": game.player2_id,
                        "games_played": 0,
                        "total_points": 0,
                        "total_rounds": 0,
                        "total_bags_in": 0,
                        "total_bags_on": 0,
                        "total_bags_off": 0,
                        "total_bags_thrown": 0,
                        "total_four_baggers": 0,
                        "total_opponent_points": 0,
                        "total_opponent_ppr": 0.0,
                        "wins": 0,
                        "losses": 0,
                        "bracket_ranks": {}
                    }
                
                p = player_stats[game.player2_id]
                p["games_played"] += 1
                p["total_points"] += game.player2_points or 0
                p["total_rounds"] += game.player2_rounds or 0
                p["total_bags_in"] += game.player2_bags_in or 0
                p["total_bags_on"] += game.player2_bags_on or 0
                p["total_bags_off"] += game.player2_bags_off or 0
                p["total_bags_thrown"] += game.player2_total_bags_thrown or 0
                p["total_four_baggers"] += game.player2_four_baggers or 0
                p["total_opponent_points"] += game.player2_opponent_points or 0
                p["total_opponent_ppr"] += game.player2_opponent_ppr or 0.0
                
                if game.player1_points and game.player2_points:
                    if game.player2_points > game.player1_points:
                        p["wins"] += 1
                    else:
                        p["losses"] += 1
    
    # Get standings to track bracket ranks
    standings_query = select(EventStanding).where(EventStanding.event_id.in_(event_ids))
    standings_result = await db.execute(standings_query)
    all_standings = standings_result.scalars().all()
    
    for standing in all_standings:
        if standing.player_id in player_stats:
            player_stats[standing.player_id]["bracket_ranks"][standing.event_id] = standing.final_rank
    
    # Get player names and CPI
    player_ids_list = list(player_stats.keys())
    player_names_dict = {}
    player_cpi_dict = {}
    
    if player_ids_list:
        latest_player_dates = select(
            Player.player_id,
            func.max(Player.snapshot_date).label('max_date')
        ).group_by(Player.player_id).subquery()
        
        latest_players_query = select(
            Player.player_id,
            Player.first_name,
            Player.last_name,
            Player.player_cpi
        ).join(
            latest_player_dates,
            and_(
                Player.player_id == latest_player_dates.c.player_id,
                Player.snapshot_date == latest_player_dates.c.max_date
            )
        ).where(Player.player_id.in_(player_ids_list))
        
        players_result = await db.execute(latest_players_query)
        for row in players_result.all():
            player_id = row[0]
            player_names_dict[player_id] = (row[1] or "", row[2] or "")
            if row[3] is not None:
                player_cpi_dict[player_id] = row[3]
    
    # Calculate derived stats and format for storage
    stats_list = []
    for player_id, stats in player_stats.items():
        games_played = stats["games_played"]
        if games_played == 0:
            continue
        
        if stats["total_rounds"] > 0:
            ppr = stats["total_points"] / stats["total_rounds"]
            opp_ppr = stats["total_opponent_ppr"] / games_played if games_played > 0 else 0.0
            dpr = ppr - opp_ppr
        else:
            ppr = 0.0
            opp_ppr = 0.0
            dpr = 0.0
        
        bags_in_pct = stats["total_bags_in"] / stats["total_bags_thrown"] if stats["total_bags_thrown"] > 0 else 0.0
        bags_on_pct = stats["total_bags_on"] / stats["total_bags_thrown"] if stats["total_bags_thrown"] > 0 else 0.0
        bags_off_pct = stats["total_bags_off"] / stats["total_bags_thrown"] if stats["total_bags_thrown"] > 0 else 0.0
        four_bagger_pct = stats["total_four_baggers"] / stats["total_rounds"] if stats["total_rounds"] > 0 else 0.0
        win_pct = stats["wins"] / games_played if games_played > 0 else 0.0
        
        first_name, last_name = player_names_dict.get(player_id, ("", ""))
        player_name = f"{first_name} {last_name}".strip() or f"Player {player_id}"
        
        stats_list.append({
            "player_id": player_id,
            "player_name": player_name,
            "best_rank": min(stats["bracket_ranks"].values()) if stats["bracket_ranks"] else None,
            "worst_rank": max(stats["bracket_ranks"].values()) if stats["bracket_ranks"] else None,
            "bracket_ranks": stats["bracket_ranks"],
            "games_played": games_played,
            "wins": stats["wins"],
            "losses": stats["losses"],
            "win_pct": win_pct,
            "rounds_total": stats["total_rounds"],
            "rounds_avg": stats["total_rounds"] / games_played if games_played > 0 else 0.0,
            "ppr": round(ppr, 3),
            "dpr": round(dpr, 3),
            "player_cpi": round(player_cpi_dict.get(player_id), 2) if player_cpi_dict.get(player_id) is not None else None,
            "bags_in_pct": round(bags_in_pct, 4),
            "bags_on_pct": round(bags_on_pct, 4),
            "bags_off_pct": round(bags_off_pct, 4),
            "four_bagger_pct": round(four_bagger_pct, 4),
            "opponent_ppr": round(opp_ppr, 3),
            "event_ids": list(stats["bracket_ranks"].keys()),
        })
    
    # Calculate overall rankings using the same algorithm as main.py
    # Strategy: 1st, 2nd, T-3rd for bracket winners, then T-2nd in bracket, etc.
    
    # Store PPR and win_pct in stats for tie-breaking
    for player_id, stats in player_stats.items():
        if stats["total_rounds"] > 0:
            stats["ppr"] = stats["total_points"] / stats["total_rounds"]
        else:
            stats["ppr"] = 0.0
        
        games_played = stats.get("games_played", 0)
        if games_played > 0:
            stats["win_pct"] = stats["wins"] / games_played
        else:
            stats["win_pct"] = 0.0
    
    # Group players by bracket ranks
    bracket_winners = []
    players_by_rank = {}
    
    for player_id, stats in player_stats.items():
        bracket_ranks = stats.get("bracket_ranks", {})
        for event_id, rank in bracket_ranks.items():
            if rank == 1:
                bracket_winners.append(player_id)
            if rank not in players_by_rank:
                players_by_rank[rank] = []
            if player_id not in players_by_rank[rank]:
                players_by_rank[rank].append(player_id)
    
    # Remove duplicates
    bracket_winners = list(set(bracket_winners))
    
    # Sort bracket winners by PPR
    bracket_winners.sort(key=lambda pid: player_stats[pid].get("ppr", 0.0), reverse=True)
    
    # Count brackets
    num_brackets = len(event_ids)
    
    overall_rank = 1
    overall_ranks = {}
    assigned_players = set()
    
    # 1st overall
    if bracket_winners:
        first_place = bracket_winners[0]
        overall_ranks[first_place] = 1
        assigned_players.add(first_place)
        overall_rank = 2
    
    # 2nd overall
    if len(bracket_winners) > 1:
        second_place = bracket_winners[1]
        overall_ranks[second_place] = 2
        assigned_players.add(second_place)
        overall_rank = 3
    
    # T-3rd: remaining bracket winners
    remaining_winners = [pid for pid in bracket_winners if pid not in assigned_players]
    if remaining_winners:
        for pid in remaining_winners:
            overall_ranks[pid] = f"T-{overall_rank}"
        overall_rank += len(remaining_winners)
        assigned_players.update(remaining_winners)
    
    # Rank 2 finishers
    rank_2_players = [pid for pid in players_by_rank.get(2, []) if pid not in assigned_players]
    if rank_2_players:
        if num_brackets == 2:
            for pid in rank_2_players:
                overall_ranks[pid] = "T-2nd in bracket"
        else:
            for pid in rank_2_players:
                overall_ranks[pid] = f"T-{overall_rank}"
        overall_rank += len(rank_2_players)
        assigned_players.update(rank_2_players)
    
    # Continue with rank 3, 4, etc.
    for rank in sorted(players_by_rank.keys()):
        if rank <= 2:
            continue
        rank_players = [pid for pid in players_by_rank[rank] if pid not in assigned_players]
        if rank_players:
            rank_players.sort(key=lambda pid: (
                -player_stats[pid].get("ppr", 0.0),
                -player_stats[pid].get("win_pct", 0.0)
            ))
            
            if len(rank_players) > 1:
                for pid in rank_players:
                    overall_ranks[pid] = f"T-{overall_rank}"
                overall_rank += len(rank_players)
            else:
                overall_ranks[rank_players[0]] = overall_rank
                overall_rank += 1
            assigned_players.update(rank_players)
    
    # Remaining players
    remaining_players = [pid for pid in player_stats.keys() if pid not in assigned_players]
    if remaining_players:
        remaining_players.sort(key=lambda pid: player_stats[pid].get("ppr", 0.0), reverse=True)
        for pid in remaining_players:
            overall_ranks[pid] = f"T-{overall_rank}"
        overall_rank += len(remaining_players)
    
    # Add overall_rank to each player's stats
    for player_stat in stats_list:
        player_id = player_stat["player_id"]
        player_stat["overall_rank"] = overall_ranks.get(player_id, "N/A")
    
    # Sort by overall rank
    def sort_key(x):
        rank = x["overall_rank"]
        if isinstance(rank, int):
            return (0, rank)
        elif isinstance(rank, str):
            if rank.startswith("T-"):
                try:
                    return (1, int(rank.split("-")[1]))
                except:
                    if "2nd in bracket" in rank:
                        return (1, 4)
                    return (2, 999)
        return (2, 999)
    
    stats_list.sort(key=sort_key)
    
    return {
        "player_stats": stats_list,
        "total_players": len(stats_list),
        "total_games": len(games),
    }


async def store_aggregated_stats(
    group_key: str,
    group_type: str,
    event_ids: List[int],
    base_event_name: str,
    bracket_type: str,
    stats_data: Dict,
    db: AsyncSession
) -> None:
    """Store pre-computed aggregated stats in the database."""
    # Calculate games hash for change detection
    games_query = select(EventGame.id).where(EventGame.event_id.in_(event_ids)).order_by(EventGame.id)
    games_result = await db.execute(games_query)
    game_ids = [str(row[0]) for row in games_result.all()]
    games_hash = hashlib.md5(json.dumps(game_ids, sort_keys=True).encode()).hexdigest()
    
    # Check if stats already exist
    existing_query = select(EventAggregatedStats).where(EventAggregatedStats.group_key == group_key)
    existing_result = await db.execute(existing_query)
    existing = existing_result.scalar_one_or_none()
    
    if existing:
        # Update existing record
        existing.player_stats = stats_data["player_stats"]
        existing.total_players = stats_data["total_players"]
        existing.total_games = stats_data["total_games"]
        existing.calculated_at = datetime.utcnow()
        existing.games_hash = games_hash
    else:
        # Create new record
        aggregated_stats = EventAggregatedStats(
            group_key=group_key,
            group_type=group_type,
            event_ids=event_ids,
            base_event_name=base_event_name,
            bracket_type=bracket_type,
            player_stats=stats_data["player_stats"],
            total_players=stats_data["total_players"],
            total_games=stats_data["total_games"],
            calculated_at=datetime.utcnow(),
            games_hash=games_hash
        )
        db.add(aggregated_stats)
    
    await db.commit()


async def get_aggregated_stats(
    group_key: str,
    db: AsyncSession
) -> Optional[Dict]:
    """Retrieve pre-computed aggregated stats from the database."""
    query = select(EventAggregatedStats).where(EventAggregatedStats.group_key == group_key)
    result = await db.execute(query)
    stats = result.scalar_one_or_none()
    
    if not stats:
        return None
    
    # Verify games hash to ensure stats are still valid
    games_query = select(EventGame.id).where(EventGame.event_id.in_(stats.event_ids)).order_by(EventGame.id)
    games_result = await db.execute(games_query)
    game_ids = [str(row[0]) for row in games_result.all()]
    current_hash = hashlib.md5(json.dumps(game_ids, sort_keys=True).encode()).hexdigest()
    
    if current_hash != stats.games_hash:
        # Stats are stale, return None to trigger recalculation
        return None
    
    return {
        "player_stats": stats.player_stats,
        "total_players": stats.total_players,
        "total_games": stats.total_games,
        "base_event_name": stats.base_event_name,
        "bracket_type": stats.bracket_type,
        "event_ids": stats.event_ids,
    }
