"""
Calculate event player statistics from indexed game/match data.
"""
from typing import Dict, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_
from database import EventGame, EventMatch, EventStanding


async def calculate_player_stats_from_games(
    event_id: int, 
    db: AsyncSession
) -> Dict[int, Dict]:
    """Calculate player statistics from indexed games for an event.
    
    Returns a dictionary mapping player_id to stats dict.
    """
    # Get all games for this event
    games_query = select(EventGame).where(EventGame.event_id == event_id)
    games_result = await db.execute(games_query)
    games = games_result.scalars().all()
    
    # Get standings for rank
    standings_query = select(EventStanding).where(EventStanding.event_id == event_id)
    standings_result = await db.execute(standings_query)
    standings = standings_result.scalars().all()
    standings_dict = {s.player_id: s for s in standings}
    
    # Aggregate stats by player
    player_stats = {}
    
    for game in games:
        # Player 1 stats
        if game.player1_id:
            if game.player1_id not in player_stats:
                player_stats[game.player1_id] = {
                    "player_id": game.player1_id,
                    "total_rounds": 0,
                    "total_pts": 0,
                    "opponent_pts": 0,
                    "four_baggers": 0,
                    "bags_in": 0,
                    "bags_on": 0,
                    "bags_off": 0,
                    "total_bags_thrown": 0,
                    "games_played": 0,
                }
            
            stats = player_stats[game.player1_id]
            stats["total_rounds"] += (game.player1_rounds or 0)
            stats["total_pts"] += (game.player1_points or 0)
            stats["opponent_pts"] += (game.player2_points or 0)
            stats["four_baggers"] += (game.player1_four_baggers or 0)
            stats["bags_in"] += (game.player1_bags_in or 0)
            stats["bags_on"] += (game.player1_bags_on or 0)
            stats["bags_off"] += (game.player1_bags_off or 0)
            stats["total_bags_thrown"] += (game.player1_total_bags_thrown or 0)
            stats["games_played"] += 1
        
        # Player 2 stats
        if game.player2_id:
            if game.player2_id not in player_stats:
                player_stats[game.player2_id] = {
                    "player_id": game.player2_id,
                    "total_rounds": 0,
                    "total_pts": 0,
                    "opponent_pts": 0,
                    "four_baggers": 0,
                    "bags_in": 0,
                    "bags_on": 0,
                    "bags_off": 0,
                    "total_bags_thrown": 0,
                    "games_played": 0,
                }
            
            stats = player_stats[game.player2_id]
            stats["total_rounds"] += (game.player2_rounds or 0)
            stats["total_pts"] += (game.player2_points or 0)
            stats["opponent_pts"] += (game.player1_points or 0)
            stats["four_baggers"] += (game.player2_four_baggers or 0)
            stats["bags_in"] += (game.player2_bags_in or 0)
            stats["bags_on"] += (game.player2_bags_on or 0)
            stats["bags_off"] += (game.player2_bags_off or 0)
            stats["total_bags_thrown"] += (game.player2_total_bags_thrown or 0)
            stats["games_played"] += 1
    
    # Calculate wins/losses from matches
    matches_query = select(EventMatch).where(EventMatch.event_id == event_id)
    matches_result = await db.execute(matches_query)
    matches = matches_result.scalars().all()
    
    for match in matches:
        if match.winner_id:
            if match.winner_id not in player_stats:
                player_stats[match.winner_id] = {
                    "player_id": match.winner_id,
                    "total_rounds": 0,
                    "total_pts": 0,
                    "opponent_pts": 0,
                    "four_baggers": 0,
                    "bags_in": 0,
                    "bags_on": 0,
                    "bags_off": 0,
                    "total_bags_thrown": 0,
                    "games_played": 0,
                    "wins": 0,
                    "losses": 0,
                }
            if "wins" not in player_stats[match.winner_id]:
                player_stats[match.winner_id]["wins"] = 0
                player_stats[match.winner_id]["losses"] = 0
            player_stats[match.winner_id]["wins"] = player_stats[match.winner_id].get("wins", 0) + 1
        
        # Determine loser
        if match.player1_id and match.player2_id:
            loser_id = match.player2_id if match.winner_id == match.player1_id else match.player1_id
            if loser_id:
                if loser_id not in player_stats:
                    player_stats[loser_id] = {
                        "player_id": loser_id,
                        "total_rounds": 0,
                        "total_pts": 0,
                        "opponent_pts": 0,
                        "four_baggers": 0,
                        "bags_in": 0,
                        "bags_on": 0,
                        "bags_off": 0,
                        "total_bags_thrown": 0,
                        "games_played": 0,
                        "wins": 0,
                        "losses": 0,
                    }
                if "losses" not in player_stats[loser_id]:
                    player_stats[loser_id]["wins"] = 0
                    player_stats[loser_id]["losses"] = 0
                player_stats[loser_id]["losses"] = player_stats[loser_id].get("losses", 0) + 1
    
    # Calculate derived stats and add rank from standings
    for player_id, stats in player_stats.items():
        # Calculate PPR
        if stats["total_rounds"] > 0:
            stats["pts_per_rnd"] = stats["total_pts"] / stats["total_rounds"]
        else:
            stats["pts_per_rnd"] = None
        
        # Calculate DPR (difference per round)
        if stats["total_rounds"] > 0:
            stats["dpr"] = (stats["total_pts"] - stats["opponent_pts"]) / stats["total_rounds"]
        else:
            stats["dpr"] = None
        
        # Calculate opponent PPR
        if stats["total_rounds"] > 0:
            stats["opponent_pts_per_rnd"] = stats["opponent_pts"] / stats["total_rounds"]
        else:
            stats["opponent_pts_per_rnd"] = None
        
        # Calculate win percentage
        wins = stats.get("wins", 0)
        losses = stats.get("losses", 0)
        total_matches = wins + losses
        if total_matches > 0:
            stats["win_pct"] = (wins / total_matches) * 100
        else:
            stats["win_pct"] = None
        
        # Calculate bag percentages
        if stats["total_bags_thrown"] > 0:
            stats["bags_in_pct"] = (stats["bags_in"] / stats["total_bags_thrown"]) * 100
            stats["bags_on_pct"] = (stats["bags_on"] / stats["total_bags_thrown"]) * 100
            stats["bags_off_pct"] = (stats["bags_off"] / stats["total_bags_thrown"]) * 100
        else:
            stats["bags_in_pct"] = None
            stats["bags_on_pct"] = None
            stats["bags_off_pct"] = None
        
        # Calculate four bagger percentage
        if stats["total_rounds"] > 0:
            stats["four_bagger_pct"] = (stats["four_baggers"] / stats["total_rounds"]) * 100
        else:
            stats["four_bagger_pct"] = None
        
        # Add rank from standings
        if player_id in standings_dict:
            stats["rank"] = standings_dict[player_id].final_rank
        else:
            stats["rank"] = None
        
        # Map field names to match PlayerEventStats format
        stats["rounds_played"] = stats["total_rounds"]
        stats["opponent_pts_total"] = stats["opponent_pts"]
        stats["total_games"] = stats["games_played"]
    
    return player_stats



