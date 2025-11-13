"""
MCP HTTP Routes for Remote Access

This module adds MCP (Model Context Protocol) HTTP endpoints to the FastAPI app,
allowing remote clients to query the cornhole database.
"""

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Any, Dict, List, Optional
import json
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func, desc, asc
from database import async_session_maker, Player, Event, PlayerEventStats, EventMatchup, EventStanding
from models import PlayerResponse

router = APIRouter(prefix="/mcp", tags=["MCP"])

# Season mapping: bucket_id -> season name
SEASON_MAP = {
    11: "2025-2026 Season",
    10: "2024-2025 Season",
    9: "2023-2024 Season",
    8: "2022-2023 Season",
    7: "2021-2022 Season",
    6: "2020-2021 Season",
    5: "2019-2020 Season",
    4: "2018-2019 Season",
    3: "2017-2018 Season",
    2: "2016-2017 Season",
    1: "2015-2016 Season",
    0: "2014-2015 Season",
}

def get_season_name(bucket_id: int) -> str:
    """Convert bucket_id to human-readable season name"""
    return SEASON_MAP.get(bucket_id, f"Season {bucket_id}")

def _get_latest_snapshot_query(bucket_id: int):
    """Create a subquery to get the latest snapshot for each player in a bucket."""
    latest_dates = select(
        Player.player_id,
        func.max(Player.snapshot_date).label('max_date')
    ).where(
        Player.bucket_id == bucket_id
    ).group_by(Player.player_id).subquery()
    
    return select(Player).join(
        latest_dates,
        and_(
            Player.player_id == latest_dates.c.player_id,
            Player.bucket_id == bucket_id,
            Player.snapshot_date == latest_dates.c.max_date
        )
    )


async def _find_player_by_name(db: AsyncSession, name: str, bucket_id: int) -> Optional[Player]:
    """Find a player by name (full or partial)"""
    query = _get_latest_snapshot_query(bucket_id)
    query = query.where(
        or_(
            Player.first_name.ilike(f"%{name}%"),
            Player.last_name.ilike(f"%{name}%"),
            func.concat(Player.first_name, ' ', Player.last_name).ilike(f"%{name}%")
        )
    ).limit(1)
    
    result = await db.execute(query)
    return result.scalar_one_or_none()


@router.get("/tools")
async def list_tools():
    """List all available MCP tools"""
    return {
        "tools": [
            {
                "name": "get_player_stats",
                "description": "Get statistics for a specific player by name or player ID. Returns current season stats including rank, PPR, DPR, CPI, win percentage, games played, and more.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "player_name": {
                            "type": "string",
                            "description": "Player's full name (first and last) or partial name to search for"
                        },
                        "player_id": {
                            "type": "integer",
                            "description": "Numeric player ID (alternative to player_name)"
                        },
                        "season": {
                            "type": "integer",
                            "description": "Season ID - 11 for 2025-2026 Season (current), 10 for 2024-2025, etc. (default: 11)",
                            "default": 11
                        }
                    }
                }
            },
            {
                "name": "search_players",
                "description": "Search for players by name, state, skill level, or other criteria. Returns a list of matching players with their key statistics.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "search": {
                            "type": "string",
                            "description": "Search term to match against player names"
                        },
                        "state": {
                            "type": "string",
                            "description": "Filter by US state (e.g., 'CA', 'TX', 'FL')"
                        },
                        "skill_level": {
                            "type": "string",
                            "description": "Filter by skill level (P, A, B, C, S, T)"
                        },
                        "season": {
                            "type": "integer",
                            "description": "Season ID - 11 for 2025-2026 Season (current), 10 for 2024-2025, etc. (default: 11)",
                            "default": 11
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of results (default: 20)",
                            "default": 20
                        },
                        "sort_by": {
                            "type": "string",
                            "description": "Field to sort by: rank, pts_per_rnd, dpr, player_cpi, win_pct, total_games",
                            "enum": ["rank", "pts_per_rnd", "dpr", "player_cpi", "win_pct", "total_games", "rounds_total"],
                            "default": "rank"
                        },
                        "sort_order": {
                            "type": "string",
                            "description": "Sort order: asc or desc",
                            "enum": ["asc", "desc"],
                            "default": "asc"
                        }
                    }
                }
            },
            {
                "name": "get_top_players",
                "description": "Get top players by various statistics like PPR, DPR, CPI, rank, games played, etc.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "stat": {
                            "type": "string",
                            "description": "Statistic to rank by",
                            "enum": ["pts_per_rnd", "dpr", "player_cpi", "win_pct", "total_games", "rounds_total", "rank"],
                            "default": "pts_per_rnd"
                        },
                        "season": {
                            "type": "integer",
                            "description": "Season ID - 11 for 2025-2026 Season (current), 10 for 2024-2025, etc. (default: 11)",
                            "default": 11
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of top players to return (default: 10)",
                            "default": 10
                        },
                        "state": {
                            "type": "string",
                            "description": "Optional: Filter by state"
                        },
                        "skill_level": {
                            "type": "string",
                            "description": "Optional: Filter by skill level"
                        }
                    }
                }
            },
            {
                "name": "compare_player_seasons",
                "description": "Compare a player's statistics across multiple seasons to see how they've improved or changed over time.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "player_name": {
                            "type": "string",
                            "description": "Player's full name"
                        },
                        "player_id": {
                            "type": "integer",
                            "description": "Numeric player ID (alternative to player_name)"
                        },
                        "seasons": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": "List of season IDs to compare - 11 is 2025-2026 Season, 10 is 2024-2025, etc. (e.g., [11, 10, 9])",
                            "default": [11, 10, 9]
                        }
                    }
                }
            },
            {
                "name": "get_player_rankings",
                "description": "Get player rankings and leaderboards. Returns players ranked by the specified statistic.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "stat": {
                            "type": "string",
                            "description": "Statistic to rank by",
                            "enum": ["pts_per_rnd", "dpr", "player_cpi", "win_pct", "total_games", "rounds_total", "rank"],
                            "default": "rank"
                        },
                        "season": {
                            "type": "integer",
                            "description": "Season ID - 11 for 2025-2026 Season (current), 10 for 2024-2025, etc. (default: 11)",
                            "default": 11
                        },
                        "min_games": {
                            "type": "integer",
                            "description": "Minimum number of games played to be included (default: 0)",
                            "default": 0
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of players to return (default: 50)",
                            "default": 50
                        }
                    }
                }
            },
            {
                "name": "get_filter_options",
                "description": "Get available filter options like states, skill levels, and seasons available in the database.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "season": {
                            "type": "integer",
                            "description": "Season ID - 11 for 2025-2026 Season (current), 10 for 2024-2025, etc. (default: 11)",
                            "default": 11
                        }
                    }
                }
            },
            {
                "name": "get_event_stats",
                "description": "Get statistics for a specific event. Returns top performers and player stats for that event.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "event_id": {
                            "type": "integer",
                            "description": "Event ID (from event info)"
                        },
                        "event_name": {
                            "type": "string",
                            "description": "Event name to search for (e.g., 'Open #2')"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of top performers to return (default: 10)",
                            "default": 10
                        }
                    }
                }
            },
            {
                "name": "get_player_event_history",
                "description": "Get a player's event history with their performance in each event.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "player_name": {
                            "type": "string",
                            "description": "Player's name"
                        },
                        "player_id": {
                            "type": "integer",
                            "description": "Player ID (alternative to name)"
                        },
                        "event_type": {
                            "type": "string",
                            "enum": ["open", "regional", "signature", "all"],
                            "description": "Filter by event type (default: all)",
                            "default": "all"
                        },
                        "season": {
                            "type": "integer",
                            "description": "Season ID (default: 11 for 2025-2026 Season)",
                            "default": 11
                        }
                    }
                }
            },
            {
                "name": "get_notable_wins",
                "description": "Find notable wins for a player - wins against opponents with high CPI or high win percentage.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "player_name": {
                            "type": "string",
                            "description": "Player's name"
                        },
                        "player_id": {
                            "type": "integer",
                            "description": "Player ID (alternative to name)"
                        },
                        "min_opponent_cpi": {
                            "type": "number",
                            "description": "Minimum opponent CPI to consider notable (default: 100)",
                            "default": 100
                        },
                        "season": {
                            "type": "integer",
                            "description": "Season ID (default: 11 for 2025-2026 Season)",
                            "default": 11
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of notable wins to return (default: 10)",
                            "default": 10
                        }
                    }
                }
            },
            {
                "name": "get_recent_event_performers",
                "description": "Get players who have been performing well recently in a specific event type (opens, regionals, etc.).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "event_type": {
                            "type": "string",
                            "enum": ["open", "regional", "signature"],
                            "description": "Type of event to analyze"
                        },
                        "days_back": {
                            "type": "integer",
                            "description": "How many days back to look (default: 30)",
                            "default": 30
                        },
                        "min_events": {
                            "type": "integer",
                            "description": "Minimum number of events to be included (default: 1)",
                            "default": 1
                        },
                        "season": {
                            "type": "integer",
                            "description": "Season ID (default: 11 for 2025-2026 Season)",
                            "default": 11
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of players to return (default: 10)",
                            "default": 10
                        }
                    }
                }
            },
            {
                "name": "search_events",
                "description": "Search for events by name, type, date, or location.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "search": {
                            "type": "string",
                            "description": "Search term for event name"
                        },
                        "event_type": {
                            "type": "string",
                            "enum": ["open", "regional", "signature", "all"],
                            "description": "Filter by event type (default: all)",
                            "default": "all"
                        },
                        "season": {
                            "type": "integer",
                            "description": "Season ID (default: 11 for 2025-2026 Season)",
                            "default": 11
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of events to return (default: 20)",
                            "default": 20
                        }
                    }
                }
            }
        ]
    }


@router.post("/call")
async def call_tool(request: Request):
    """Call an MCP tool"""
    try:
        body = await request.json()
        tool_name = body.get("name")
        arguments = body.get("arguments", {})
        
        if not tool_name:
            raise HTTPException(status_code=400, detail="Tool name is required")
        
        # Route to appropriate tool handler
        if tool_name == "get_player_stats":
            return await _handle_get_player_stats(arguments)
        elif tool_name == "search_players":
            return await _handle_search_players(arguments)
        elif tool_name == "get_top_players":
            return await _handle_get_top_players(arguments)
        elif tool_name == "compare_player_seasons":
            return await _handle_compare_player_seasons(arguments)
        elif tool_name == "get_player_rankings":
            return await _handle_get_player_rankings(arguments)
        elif tool_name == "get_filter_options":
            return await _handle_get_filter_options(arguments)
        elif tool_name == "get_event_stats":
            return await _handle_get_event_stats(arguments)
        elif tool_name == "get_player_event_history":
            return await _handle_get_player_event_history(arguments)
        elif tool_name == "get_notable_wins":
            return await _handle_get_notable_wins(arguments)
        elif tool_name == "get_recent_event_performers":
            return await _handle_get_recent_event_performers(arguments)
        elif tool_name == "search_events":
            return await _handle_search_events(arguments)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown tool: {tool_name}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calling tool: {str(e)}")


async def _handle_get_player_stats(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Handle get_player_stats tool call"""
    async with async_session_maker() as db:
        player_id = arguments.get("player_id")
        player_name = arguments.get("player_name")
        season = arguments.get("season", 11)
        
        if not player_id and not player_name:
            return {
                "content": [{
                    "type": "text",
                    "text": "Error: Either player_id or player_name must be provided"
                }],
                "isError": True
            }
        
        # Find player
        if player_id:
            latest_dates = select(
                func.max(Player.snapshot_date).label('max_date')
            ).where(
                and_(Player.player_id == player_id, Player.bucket_id == season)
            ).subquery()
            
            result = await db.execute(
                select(Player).join(
                    latest_dates,
                    and_(
                        Player.player_id == player_id,
                        Player.bucket_id == season,
                        Player.snapshot_date == latest_dates.c.max_date
                    )
                )
            )
            player = result.scalar_one_or_none()
        else:
            player = await _find_player_by_name(db, player_name, season)
        
        if not player:
            season_name = get_season_name(season)
            return {
                "content": [{
                    "type": "text",
                    "text": f"Player not found in {season_name}"
                }]
            }
        
        # Convert to response format
        player_data = PlayerResponse.model_validate(player)
        season_name = get_season_name(season)
        
        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "player": {
                        "id": player_data.player_id,
                        "name": f"{player_data.first_name} {player_data.last_name}",
                        "state": player_data.state,
                        "skill_level": player_data.skill_level,
                        "season": season_name,
                        "season_id": season,
                        "rank": player_data.rank,
                        "stats": {
                            "points_per_round": player_data.pts_per_rnd,
                            "defense_per_round": player_data.dpr,
                            "cpi": player_data.player_cpi,
                            "win_percentage": player_data.win_pct,
                            "total_games": player_data.total_games,
                            "wins": player_data.total_wins,
                            "losses": player_data.total_losses,
                            "rounds_played": player_data.rounds_total,
                            "overall_total": player_data.overall_total,
                            "four_bagger_percentage": player_data.four_bagger_pct,
                            "bags_in_percentage": player_data.bags_in_pct,
                            "bags_on_percentage": player_data.bags_on_pct,
                            "bags_off_percentage": player_data.bags_off_pct
                        }
                    }
                }, indent=2)
            }]
        }


async def _handle_search_players(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Handle search_players tool call"""
    async with async_session_maker() as db:
        season = arguments.get("season", 11)
        search = arguments.get("search")
        state = arguments.get("state")
        skill_level = arguments.get("skill_level")
        limit = arguments.get("limit", 20)
        sort_by = arguments.get("sort_by", "rank")
        sort_order = arguments.get("sort_order", "asc")
        
        query = _get_latest_snapshot_query(season)
        
        # Apply filters
        if search:
            query = query.where(
                or_(
                    Player.first_name.ilike(f"%{search}%"),
                    Player.last_name.ilike(f"%{search}%")
                )
            )
        if state:
            query = query.where(Player.state == state)
        if skill_level:
            query = query.where(Player.skill_level == skill_level)
        
        # Apply sorting
        sort_column = getattr(Player, sort_by, Player.rank)
        if sort_by in ['pts_per_rnd', 'dpr', 'player_cpi', 'win_pct', 'total_games', 'rounds_total', 'overall_total']:
            query = query.where(sort_column.isnot(None))
        
        if sort_order.lower() == "desc":
            query = query.order_by(desc(sort_column))
        else:
            query = query.order_by(asc(sort_column))
        
        query = query.limit(limit)
        
        result = await db.execute(query)
        players = result.scalars().all()
        
        players_list = []
        for player in players:
            player_data = PlayerResponse.model_validate(player)
            players_list.append({
                "id": player_data.player_id,
                "name": f"{player_data.first_name} {player_data.last_name}",
                "state": player_data.state,
                "rank": player_data.rank,
                "pts_per_round": player_data.pts_per_rnd,
                "dpr": player_data.dpr,
                "cpi": player_data.player_cpi,
                "win_percentage": player_data.win_pct,
                "total_games": player_data.total_games
            })
        
        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "count": len(players_list),
                    "players": players_list
                }, indent=2)
            }]
        }


async def _handle_get_top_players(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Handle get_top_players tool call"""
    async with async_session_maker() as db:
        stat = arguments.get("stat", "pts_per_rnd")
        season = arguments.get("season", 11)
        limit = arguments.get("limit", 10)
        state = arguments.get("state")
        skill_level = arguments.get("skill_level")
        
        query = _get_latest_snapshot_query(season)
        
        # Apply filters
        if state:
            query = query.where(Player.state == state)
        if skill_level:
            query = query.where(Player.skill_level == skill_level)
        
        # Filter out NULL values for the stat
        sort_column = getattr(Player, stat, Player.pts_per_rnd)
        query = query.where(sort_column.isnot(None))
        
        # Sort descending to get top players
        query = query.order_by(desc(sort_column)).limit(limit)
        
        result = await db.execute(query)
        players = result.scalars().all()
        
        players_list = []
        for player in players:
            player_data = PlayerResponse.model_validate(player)
            stat_value = getattr(player_data, stat, None)
            players_list.append({
                "rank": player_data.rank,
                "name": f"{player_data.first_name} {player_data.last_name}",
                "state": player_data.state,
                stat: stat_value,
                "pts_per_round": player_data.pts_per_rnd,
                "dpr": player_data.dpr,
                "cpi": player_data.player_cpi,
                "win_percentage": player_data.win_pct,
                "total_games": player_data.total_games
            })
        
        season_name = get_season_name(season)
        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "stat": stat,
                    "season": season_name,
                    "season_id": season,
                    "count": len(players_list),
                    "top_players": players_list
                }, indent=2)
            }]
        }


async def _handle_compare_player_seasons(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Handle compare_player_seasons tool call"""
    async with async_session_maker() as db:
        player_id = arguments.get("player_id")
        player_name = arguments.get("player_name")
        seasons = arguments.get("seasons", [11, 10, 9])
        
        if not player_id and not player_name:
            return {
                "content": [{
                    "type": "text",
                    "text": "Error: Either player_id or player_name must be provided"
                }],
                "isError": True
            }
        
        # Find player ID if name provided
        if player_name and not player_id:
            player = await _find_player_by_name(db, player_name, seasons[0] if seasons else 11)
            if not player:
                return {
                    "content": [{
                        "type": "text",
                        "text": f"Player '{player_name}' not found"
                    }]
                }
            player_id = player.player_id
        
        # Get stats for each season
        season_stats = []
        for season in seasons:
            latest_dates = select(
                func.max(Player.snapshot_date).label('max_date')
            ).where(
                and_(Player.player_id == player_id, Player.bucket_id == season)
            ).subquery()
            
            result = await db.execute(
                select(Player).join(
                    latest_dates,
                    and_(
                        Player.player_id == player_id,
                        Player.bucket_id == season,
                        Player.snapshot_date == latest_dates.c.max_date
                    )
                )
            )
            player = result.scalar_one_or_none()
            
            if player:
                player_data = PlayerResponse.model_validate(player)
                season_name = get_season_name(season)
                season_stats.append({
                    "season": season_name,
                    "season_id": season,
                    "rank": player_data.rank,
                    "pts_per_round": player_data.pts_per_rnd,
                    "dpr": player_data.dpr,
                    "cpi": player_data.player_cpi,
                    "win_percentage": player_data.win_pct,
                    "total_games": player_data.total_games,
                    "rounds_played": player_data.rounds_total
                })
            else:
                season_name = get_season_name(season)
                season_stats.append({
                    "season": season_name,
                    "season_id": season,
                    "status": "not_found"
                })
        
        if not season_stats:
            return {
                "content": [{
                    "type": "text",
                    "text": f"Player {player_id} not found in any of the specified seasons"
                }]
            }
        
        # Get player name from first found season
        player_name_display = "Unknown"
        if season_stats and season_stats[0].get("status") != "not_found":
            latest_dates = select(
                func.max(Player.snapshot_date).label('max_date')
            ).where(
                and_(Player.player_id == player_id, Player.bucket_id == seasons[0])
            ).subquery()
            result = await db.execute(
                select(Player).join(
                    latest_dates,
                    and_(
                        Player.player_id == player_id,
                        Player.bucket_id == seasons[0],
                        Player.snapshot_date == latest_dates.c.max_date
                    )
                )
            )
            player = result.scalar_one_or_none()
            if player:
                player_name_display = f"{player.first_name} {player.last_name}"
        
        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "player_id": player_id,
                    "player_name": player_name_display,
                    "seasons": season_stats
                }, indent=2)
            }]
        }


async def _handle_get_player_rankings(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Handle get_player_rankings tool call"""
    async with async_session_maker() as db:
        stat = arguments.get("stat", "rank")
        season = arguments.get("season", 11)
        min_games = arguments.get("min_games", 0)
        limit = arguments.get("limit", 50)
        
        query = _get_latest_snapshot_query(season)
        
        # Filter by minimum games
        if min_games > 0:
            query = query.where(Player.total_games >= min_games)
        
        # Filter out NULL values for the stat
        sort_column = getattr(Player, stat, Player.rank)
        query = query.where(sort_column.isnot(None))
        
        # Sort
        if stat == "rank":
            query = query.order_by(asc(sort_column))
        else:
            query = query.order_by(desc(sort_column))
        
        query = query.limit(limit)
        
        result = await db.execute(query)
        players = result.scalars().all()
        
        rankings = []
        for idx, player in enumerate(players, 1):
            player_data = PlayerResponse.model_validate(player)
            stat_value = getattr(player_data, stat, None)
            rankings.append({
                "position": idx,
                "rank": player_data.rank,
                "name": f"{player_data.first_name} {player_data.last_name}",
                "state": player_data.state,
                stat: stat_value,
                "total_games": player_data.total_games
            })
        
        season_name = get_season_name(season)
        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "stat": stat,
                    "season": season_name,
                    "season_id": season,
                    "min_games": min_games,
                    "rankings": rankings
                }, indent=2)
            }]
        }


async def _handle_get_filter_options(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Handle get_filter_options tool call"""
    async with async_session_maker() as db:
        season = arguments.get("season", 11)
        
        # Get latest snapshot date per player
        latest_dates = select(
            Player.player_id,
            func.max(Player.snapshot_date).label('max_date')
        ).where(Player.bucket_id == season).group_by(Player.player_id).subquery()
        
        # Get distinct states
        states_query = select(Player.state).join(
            latest_dates,
            and_(
                Player.player_id == latest_dates.c.player_id,
                Player.bucket_id == season,
                Player.snapshot_date == latest_dates.c.max_date,
                Player.state.isnot(None)
            )
        ).distinct()
        states_result = await db.execute(states_query)
        states = sorted([s for s in states_result.scalars().all() if s])
        
        # Get distinct skill levels
        skills_query = select(Player.skill_level).join(
            latest_dates,
            and_(
                Player.player_id == latest_dates.c.player_id,
                Player.bucket_id == season,
                Player.snapshot_date == latest_dates.c.max_date,
                Player.skill_level.isnot(None)
            )
        ).distinct()
        skills_result = await db.execute(skills_query)
        skill_levels = sorted([s for s in skills_result.scalars().all() if s])
        
        # Get available seasons
        buckets_query = select(Player.bucket_id).distinct()
        buckets_result = await db.execute(buckets_query)
        available_bucket_ids = sorted([b for b in buckets_result.scalars().all()], reverse=True)
        
        # Convert to season names with IDs
        available_seasons = [
            {
                "season_id": bucket_id,
                "season_name": get_season_name(bucket_id)
            }
            for bucket_id in available_bucket_ids
        ]
        
        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "states": states,
                    "skill_levels": skill_levels,
                    "available_seasons": available_seasons
                }, indent=2)
            }]
        }


async def _handle_get_event_stats(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Handle get_event_stats tool call"""
    async with async_session_maker() as db:
        try:
            event_id = arguments.get("event_id")
            event_name = arguments.get("event_name")
            limit = arguments.get("limit", 10)
            
            if not event_id and not event_name:
                return {
                    "content": [{
                        "type": "text",
                        "text": "Error: Either event_id or event_name must be provided"
                    }],
                    "isError": True
                }
            
            # Find event
            if event_id:
                event_query = select(Event).where(Event.event_id == event_id)
            else:
                event_query = select(Event).where(Event.event_name.ilike(f"%{event_name}%")).limit(1)
            
            result = await db.execute(event_query)
            event = result.scalar_one_or_none()
            
            if not event:
                return {
                    "content": [{
                        "type": "text",
                        "text": f"Event not found"
                    }]
                }
            
            # Get top performers
            stats_query = select(PlayerEventStats).where(
                PlayerEventStats.event_id == event.event_id
            ).order_by(PlayerEventStats.rank.asc()).limit(limit)
            
            stats_result = await db.execute(stats_query)
            player_stats = stats_result.scalars().all()
            
            # Get player names
            performers = []
            for stat in player_stats:
                player_query = select(Player).where(Player.player_id == stat.player_id).limit(1)
                player_result = await db.execute(player_query)
                player = player_result.scalar_one_or_none()
                
                player_name = "Unknown"
                if player:
                    player_name = f"{player.first_name} {player.last_name}"
                
                performers.append({
                    "rank": stat.rank,
                    "player_id": stat.player_id,
                    "player_name": player_name,
                    "pts_per_round": stat.pts_per_rnd,
                    "dpr": stat.dpr,
                    "wins": stat.wins,
                    "losses": stat.losses,
                    "win_percentage": stat.win_pct,
                    "total_games": stat.total_games
                })
            
            season_name = get_season_name(event.bucket_id) if event.bucket_id else "Unknown"
            
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "event": {
                            "event_id": event.event_id,
                            "event_name": event.event_name,
                            "base_event_name": event.base_event_name,
                            "bracket_name": event.bracket_name,
                            "event_group_id": event.event_group_id,
                            "event_type": event.event_type,
                            "event_date": event.event_date.isoformat() if event.event_date else None,
                            "location": event.location,
                            "season": season_name
                        },
                        "top_performers": performers
                    }, indent=2)
                }]
            }
        except Exception as e:
            return {
                "content": [{
                    "type": "text",
                    "text": f"Error: {str(e)}"
                }],
                "isError": True
            }


async def _handle_get_player_event_history(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Handle get_player_event_history tool call"""
    async with async_session_maker() as db:
        try:
            player_id = arguments.get("player_id")
            player_name = arguments.get("player_name")
            event_type = arguments.get("event_type", "all")
            season = arguments.get("season", 11)
            
            if not player_id and not player_name:
                return {
                    "content": [{
                        "type": "text",
                        "text": "Error: Either player_id or player_name must be provided"
                    }],
                    "isError": True
                }
            
            # Find player
            if player_name and not player_id:
                player = await _find_player_by_name(db, player_name, season)
                if not player:
                    return {
                        "content": [{
                            "type": "text",
                            "text": f"Player '{player_name}' not found"
                        }]
                    }
                player_id = player.player_id
            
            # Get player's event stats
            query = select(PlayerEventStats, Event).join(
                Event, PlayerEventStats.event_id == Event.event_id
            ).where(
                PlayerEventStats.player_id == player_id
            )
            
            if event_type != "all":
                query = query.where(Event.event_type == event_type)
            
            if season:
                query = query.where(Event.bucket_id == season)
            
            query = query.order_by(Event.event_date.desc() if Event.event_date else Event.id.desc())
            
            result = await db.execute(query)
            rows = result.all()
            
            events = []
            for stat, event in rows:
                events.append({
                    "event_id": event.event_id,
                    "event_name": event.event_name,
                    "base_event_name": event.base_event_name,
                    "bracket_name": event.bracket_name,
                    "event_group_id": event.event_group_id,
                    "event_type": event.event_type,
                    "event_date": event.event_date.isoformat() if event.event_date else None,
                    "rank": stat.rank,
                    "pts_per_round": stat.pts_per_rnd,
                    "dpr": stat.dpr,
                    "wins": stat.wins,
                    "losses": stat.losses,
                    "win_percentage": stat.win_pct,
                    "total_games": stat.total_games
                })
            
            # Get player name
            player_query = select(Player).where(Player.player_id == player_id).limit(1)
            player_result = await db.execute(player_query)
            player = player_result.scalar_one_or_none()
            player_name_display = f"{player.first_name} {player.last_name}" if player else "Unknown"
            
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "player_id": player_id,
                        "player_name": player_name_display,
                        "event_count": len(events),
                        "events": events
                    }, indent=2)
                }]
            }
        except Exception as e:
            return {
                "content": [{
                    "type": "text",
                    "text": f"Error: {str(e)}"
                }],
                "isError": True
            }


async def _handle_get_notable_wins(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Handle get_notable_wins tool call"""
    async with async_session_maker() as db:
        try:
            player_id = arguments.get("player_id")
            player_name = arguments.get("player_name")
            min_opponent_cpi = arguments.get("min_opponent_cpi", 100)
            season = arguments.get("season", 11)
            limit = arguments.get("limit", 10)
            
            if not player_id and not player_name:
                return {
                    "content": [{
                        "type": "text",
                        "text": "Error: Either player_id or player_name must be provided"
                    }],
                    "isError": True
                }
            
            # Find player
            if player_name and not player_id:
                player = await _find_player_by_name(db, player_name, season)
                if not player:
                    return {
                        "content": [{
                            "type": "text",
                            "text": f"Player '{player_name}' not found"
                        }]
                    }
                player_id = player.player_id
            
            # Get player's wins from matchups
            wins_query = select(EventMatchup, Event).join(
                Event, EventMatchup.event_id == Event.event_id
            ).where(
                and_(
                    EventMatchup.winner_id == player_id,
                    Event.bucket_id == season
                )
            )
            
            wins_result = await db.execute(wins_query)
            wins = wins_result.all()
            
            # Get current season player stats for CPI lookup
            latest_dates = select(
                Player.player_id,
                func.max(Player.snapshot_date).label('max_date')
            ).where(Player.bucket_id == season).group_by(Player.player_id).subquery()
            
            player_cpi_query = select(Player.player_id, Player.player_cpi).join(
                latest_dates,
                and_(
                    Player.player_id == latest_dates.c.player_id,
                    Player.bucket_id == season,
                    Player.snapshot_date == latest_dates.c.max_date
                )
            ).where(Player.player_cpi >= min_opponent_cpi)
            
            cpi_result = await db.execute(player_cpi_query)
            high_cpi_players = {row.player_id: row.player_cpi for row in cpi_result.all()}
            
            # Filter wins against high CPI opponents
            notable_wins = []
            for matchup, event in wins:
                opponent_id = matchup.player1_id if matchup.player2_id == player_id else matchup.player2_id
                opponent_cpi = high_cpi_players.get(opponent_id)
                
                if opponent_cpi:
                    # Get opponent name
                    opponent_query = select(Player).where(Player.player_id == opponent_id).limit(1)
                    opponent_result = await db.execute(opponent_query)
                    opponent = opponent_result.scalar_one_or_none()
                    opponent_name = f"{opponent.first_name} {opponent.last_name}" if opponent else "Unknown"
                    
                    notable_wins.append({
                        "event_id": event.event_id,
                        "event_name": event.event_name,
                        "base_event_name": event.base_event_name,
                        "bracket_name": event.bracket_name,
                        "event_group_id": event.event_group_id,
                        "event_type": event.event_type,
                        "event_date": event.event_date.isoformat() if event.event_date else None,
                        "opponent_id": opponent_id,
                        "opponent_name": opponent_name,
                        "opponent_cpi": opponent_cpi,
                        "score": matchup.score
                    })
            
            # Sort by opponent CPI (highest first) and limit
            notable_wins.sort(key=lambda x: x.get("opponent_cpi", 0), reverse=True)
            notable_wins = notable_wins[:limit]
            
            # Get player name
            player_query = select(Player).where(Player.player_id == player_id).limit(1)
            player_result = await db.execute(player_query)
            player = player_result.scalar_one_or_none()
            player_name_display = f"{player.first_name} {player.last_name}" if player else "Unknown"
            
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "player_id": player_id,
                        "player_name": player_name_display,
                        "min_opponent_cpi": min_opponent_cpi,
                        "notable_wins_count": len(notable_wins),
                        "notable_wins": notable_wins
                    }, indent=2)
                }]
            }
        except Exception as e:
            return {
                "content": [{
                    "type": "text",
                    "text": f"Error: {str(e)}"
                }],
                "isError": True
            }


async def _handle_get_recent_event_performers(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Handle get_recent_event_performers tool call"""
    async with async_session_maker() as db:
        try:
            event_type = arguments.get("event_type")
            days_back = arguments.get("days_back", 30)
            min_events = arguments.get("min_events", 1)
            season = arguments.get("season", 11)
            limit = arguments.get("limit", 10)
            
            if not event_type:
                return {
                    "content": [{
                        "type": "text",
                        "text": "Error: event_type is required"
                    }],
                    "isError": True
                }
            
            # Calculate cutoff date
            from datetime import timedelta
            cutoff_date = datetime.now().date() - timedelta(days=days_back)
            
            # Get recent events of this type
            events_query = select(Event.event_id).where(
                and_(
                    Event.event_type == event_type,
                    Event.bucket_id == season,
                    Event.event_date >= cutoff_date
                )
            )
            events_result = await db.execute(events_query)
            event_ids = [row[0] for row in events_result.all()]
            
            if not event_ids:
                return {
                    "content": [{
                        "type": "text",
                        "text": json.dumps({
                            "message": f"No {event_type} events found in the last {days_back} days",
                            "performers": []
                        }, indent=2)
                    }]
                }
            
            # Aggregate player stats across these events
            stats_query = select(
                PlayerEventStats.player_id,
                func.count(PlayerEventStats.event_id).label('event_count'),
                func.avg(PlayerEventStats.pts_per_rnd).label('avg_ppr'),
                func.avg(PlayerEventStats.win_pct).label('avg_win_pct'),
                func.sum(PlayerEventStats.wins).label('total_wins'),
                func.sum(PlayerEventStats.total_games).label('total_games')
            ).where(
                PlayerEventStats.event_id.in_(event_ids)
            ).group_by(PlayerEventStats.player_id).having(
                func.count(PlayerEventStats.event_id) >= min_events
            ).order_by(func.avg(PlayerEventStats.pts_per_rnd).desc()).limit(limit)
            
            stats_result = await db.execute(stats_query)
            performers_data = stats_result.all()
            
            # Get player names
            performers = []
            for row in performers_data:
                player_query = select(Player).where(Player.player_id == row.player_id).limit(1)
                player_result = await db.execute(player_query)
                player = player_result.scalar_one_or_none()
                
                player_name = "Unknown"
                if player:
                    player_name = f"{player.first_name} {player.last_name}"
                
                performers.append({
                    "player_id": row.player_id,
                    "player_name": player_name,
                    "events_played": row.event_count,
                    "avg_pts_per_round": round(row.avg_ppr, 2) if row.avg_ppr else None,
                    "avg_win_percentage": round(row.avg_win_pct, 2) if row.avg_win_pct else None,
                    "total_wins": row.total_wins,
                    "total_games": row.total_games
                })
            
            season_name = get_season_name(season)
            
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "event_type": event_type,
                        "season": season_name,
                        "days_back": days_back,
                        "events_analyzed": len(event_ids),
                        "performers": performers
                    }, indent=2)
                }]
            }
        except Exception as e:
            return {
                "content": [{
                    "type": "text",
                    "text": f"Error: {str(e)}"
                }],
                "isError": True
            }


async def _handle_search_events(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Handle search_events tool call"""
    async with async_session_maker() as db:
        try:
            search = arguments.get("search")
            event_type = arguments.get("event_type", "all")
            season = arguments.get("season", 11)
            limit = arguments.get("limit", 20)
            
            query = select(Event).where(Event.bucket_id == season)
            
            if search:
                query = query.where(Event.event_name.ilike(f"%{search}%"))
            
            if event_type != "all":
                query = query.where(Event.event_type == event_type)
            
            query = query.order_by(Event.event_date.desc() if Event.event_date else Event.id.desc()).limit(limit)
            
            result = await db.execute(query)
            events = result.scalars().all()
            
            events_list = []
            for event in events:
                season_name = get_season_name(event.bucket_id) if event.bucket_id else "Unknown"
                events_list.append({
                    "event_id": event.event_id,
                    "event_name": event.event_name,
                    "base_event_name": event.base_event_name,
                    "bracket_name": event.bracket_name,
                    "event_group_id": event.event_group_id,
                    "event_type": event.event_type,
                    "event_date": event.event_date.isoformat() if event.event_date else None,
                    "location": event.location,
                    "season": season_name
                })
            
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "count": len(events_list),
                        "events": events_list
                    }, indent=2)
                }]
            }
        except Exception as e:
            return {
                "content": [{
                    "type": "text",
                    "text": f"Error: {str(e)}"
                }],
                "isError": True
            }

