#!/usr/bin/env python3
"""
MCP Server for Cornhole Player Statistics Database

This server exposes the cornhole database to AI assistants like Claude/ChatGPT
via the Model Context Protocol (MCP).

Usage:
    python mcp_server.py

Or configure in Claude Desktop:
    Add to ~/Library/Application Support/Claude/claude_desktop_config.json:
    {
      "mcpServers": {
        "cornhole-stats": {
          "command": "python",
          "args": ["/absolute/path/to/fly-cornhole/mcp_server.py"]
        }
      }
    }
"""

import asyncio
import json
from typing import Any, Dict, List, Optional
from datetime import datetime

try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import Tool, TextContent
    MCP_AVAILABLE = True
except ImportError:
    # Fallback implementation if mcp package not available
    MCP_AVAILABLE = False
    import sys

# Import database models and functions
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func, desc, asc
from database import async_session_maker, Player, init_db
from models import PlayerResponse


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


if MCP_AVAILABLE:
    # Use official MCP SDK
    server = Server("cornhole-stats")
    
    @server.list_tools()
    async def list_tools() -> List[Tool]:
        """Return list of available MCP tools"""
        return [
            Tool(
                name="get_player_stats",
                description="Get statistics for a specific player by name or player ID. Returns current season stats including rank, PPR, DPR, CPI, win percentage, games played, and more.",
                inputSchema={
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
                            "description": "Season/bucket ID (default: 11 for current season)",
                            "default": 11
                        }
                    }
                }
            ),
            Tool(
                name="search_players",
                description="Search for players by name, state, skill level, or other criteria. Returns a list of matching players with their key statistics.",
                inputSchema={
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
                            "description": "Season/bucket ID (default: 11)",
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
            ),
            Tool(
                name="get_top_players",
                description="Get top players by various statistics like PPR, DPR, CPI, rank, games played, etc.",
                inputSchema={
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
                            "description": "Season/bucket ID (default: 11)",
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
            ),
            Tool(
                name="compare_player_seasons",
                description="Compare a player's statistics across multiple seasons to see how they've improved or changed over time.",
                inputSchema={
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
                            "description": "List of season/bucket IDs to compare (e.g., [11, 10, 9])",
                            "default": [11, 10, 9]
                        }
                    }
                }
            ),
            Tool(
                name="get_player_rankings",
                description="Get player rankings and leaderboards. Returns players ranked by the specified statistic.",
                inputSchema={
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
                            "description": "Season/bucket ID (default: 11)",
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
            ),
            Tool(
                name="get_filter_options",
                description="Get available filter options like states, skill levels, and seasons available in the database.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "season": {
                            "type": "integer",
                            "description": "Season/bucket ID to get filters for (default: 11)",
                            "default": 11
                        }
                    }
                }
            )
        ]
    
    @server.call_tool()
    async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
        """Handle tool calls"""
        await init_db()  # Ensure DB is initialized
        
        if name == "get_player_stats":
            async with async_session_maker() as db:
                try:
                    player_id = arguments.get("player_id")
                    player_name = arguments.get("player_name")
                    season = arguments.get("season", 11)
                    
                    if not player_id and not player_name:
                        return [TextContent(
                            type="text",
                            text="Error: Either player_id or player_name must be provided"
                        )]
                    
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
                        return [TextContent(
                            type="text",
                            text=f"Player not found in season {season}"
                        )]
                    
                    # Convert to response format
                    player_data = PlayerResponse.model_validate(player)
                    
                    result_text = json.dumps({
                        "player": {
                            "id": player_data.player_id,
                            "name": f"{player_data.first_name} {player_data.last_name}",
                            "state": player_data.state,
                            "skill_level": player_data.skill_level,
                            "season": season,
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
                    
                    return [TextContent(type="text", text=result_text)]
                except Exception as e:
                    return [TextContent(type="text", text=f"Error: {str(e)}")]
        
        elif name == "search_players":
            async with async_session_maker() as db:
                try:
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
                    
                    return [TextContent(
                        type="text",
                        text=json.dumps({
                            "count": len(players_list),
                            "players": players_list
                        }, indent=2)
                    )]
                except Exception as e:
                    return [TextContent(type="text", text=f"Error: {str(e)}")]
        
        elif name == "get_top_players":
            async with async_session_maker() as db:
                try:
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
                    
                    return [TextContent(
                        type="text",
                        text=json.dumps({
                            "stat": stat,
                            "season": season,
                            "count": len(players_list),
                            "top_players": players_list
                        }, indent=2)
                    )]
                except Exception as e:
                    return [TextContent(type="text", text=f"Error: {str(e)}")]
        
        elif name == "compare_player_seasons":
            async with async_session_maker() as db:
                try:
                    player_id = arguments.get("player_id")
                    player_name = arguments.get("player_name")
                    seasons = arguments.get("seasons", [11, 10, 9])
                    
                    if not player_id and not player_name:
                        return [TextContent(
                            type="text",
                            text="Error: Either player_id or player_name must be provided"
                        )]
                    
                    # Find player ID if name provided
                    if player_name and not player_id:
                        player = await _find_player_by_name(db, player_name, seasons[0] if seasons else 11)
                        if not player:
                            return [TextContent(
                                type="text",
                                text=f"Player '{player_name}' not found"
                            )]
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
                            season_stats.append({
                                "season": season,
                                "rank": player_data.rank,
                                "pts_per_round": player_data.pts_per_rnd,
                                "dpr": player_data.dpr,
                                "cpi": player_data.player_cpi,
                                "win_percentage": player_data.win_pct,
                                "total_games": player_data.total_games,
                                "rounds_played": player_data.rounds_total
                            })
                        else:
                            season_stats.append({
                                "season": season,
                                "status": "not_found"
                            })
                    
                    if not season_stats:
                        return [TextContent(
                            type="text",
                            text=f"Player {player_id} not found in any of the specified seasons"
                        )]
                    
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
                    
                    return [TextContent(
                        type="text",
                        text=json.dumps({
                            "player_id": player_id,
                            "player_name": player_name_display,
                            "seasons": season_stats
                        }, indent=2)
                    )]
                except Exception as e:
                    return [TextContent(type="text", text=f"Error: {str(e)}")]
        
        elif name == "get_player_rankings":
            async with async_session_maker() as db:
                try:
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
                    
                    return [TextContent(
                        type="text",
                        text=json.dumps({
                            "stat": stat,
                            "season": season,
                            "min_games": min_games,
                            "rankings": rankings
                        }, indent=2)
                    )]
                except Exception as e:
                    return [TextContent(type="text", text=f"Error: {str(e)}")]
        
        elif name == "get_filter_options":
            async with async_session_maker() as db:
                try:
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
                    available_seasons = sorted([b for b in buckets_result.scalars().all()], reverse=True)
                    
                    return [TextContent(
                        type="text",
                        text=json.dumps({
                            "states": states,
                            "skill_levels": skill_levels,
                            "available_seasons": available_seasons
                        }, indent=2)
                    )]
                except Exception as e:
                    return [TextContent(type="text", text=f"Error: {str(e)}")]
        
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    
    async def main():
        """Main entry point for MCP server"""
        # Initialize database on startup
        await init_db()
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options()
            )
    
    if __name__ == "__main__":
        asyncio.run(main())

else:
    # Fallback implementation without MCP SDK
    async def main():
        """Fallback main entry point"""
        print("MCP SDK not available. Please install: pip install mcp", file=sys.stderr)
        print("See: https://github.com/modelcontextprotocol/python-sdk", file=sys.stderr)
        sys.exit(1)
    
    if __name__ == "__main__":
        asyncio.run(main())
