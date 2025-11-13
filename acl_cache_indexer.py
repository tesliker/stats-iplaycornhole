"""
ACL API Cache Indexer - Fetches and stores raw JSON responses from ACL API endpoints.
This allows us to never hit ACL servers again and process data at our own pace.
"""
import httpx
import asyncio
import json
import hashlib
from typing import List, Dict, Optional
from datetime import datetime
from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from database import ACLAPICache, async_session_maker
from fetcher import (
    get_standings_url, BUCKET_YEAR_MAP,
    PLAYER_STATS_URL, PLAYER_EVENTS_LIST_URL,
    EVENT_INFO_URL, EVENT_PLAYER_STATS_URL,
    EVENT_STANDINGS_URL, EVENT_BRACKET_URL, EVENT_MATCH_STATS_URL
)

# In-memory status tracking
cache_indexing_status = {}
cache_indexing_logs = {}


def get_url_hash(url: str) -> str:
    """Generate a hash for a URL to use as a unique identifier."""
    return hashlib.sha256(url.encode()).hexdigest()


async def get_cached_response(url: str, db: AsyncSession) -> Optional[Dict]:
    """Check if we have a cached response for this URL."""
    url_hash = get_url_hash(url)
    result = await db.execute(
        select(ACLAPICache).where(ACLAPICache.url_hash == url_hash)
    )
    cached = result.scalar_one_or_none()
    if cached:
        return cached.response_json
    return None


async def cache_response(
    endpoint_type: str,
    url: str,
    response_json: Dict,
    db: AsyncSession,
    bucket_id: Optional[int] = None,
    player_id: Optional[int] = None,
    event_id: Optional[int] = None,
    match_id: Optional[int] = None,
    game_id: Optional[int] = None,
    region: Optional[str] = None,
    http_status: Optional[int] = None
) -> ACLAPICache:
    """Store a raw JSON response in the cache."""
    url_hash = get_url_hash(url)
    
    # Check if already cached
    existing = await db.execute(
        select(ACLAPICache).where(ACLAPICache.url_hash == url_hash)
    )
    cached = existing.scalar_one_or_none()
    
    if cached:
        # Update existing cache entry
        cached.response_json = response_json
        cached.http_status = http_status
        cached.fetched_at = datetime.utcnow()
        return cached
    else:
        # Create new cache entry
        cache_entry = ACLAPICache(
            endpoint_type=endpoint_type,
            url=url,
            url_hash=url_hash,
            bucket_id=bucket_id,
            player_id=player_id,
            event_id=event_id,
            match_id=match_id,
            game_id=game_id,
            region=region,
            response_json=response_json,
            http_status=http_status,
            fetched_at=datetime.utcnow()
        )
        db.add(cache_entry)
        return cache_entry


async def index_standings(bucket_id: int, region: str = "us", use_cache: bool = True, db: AsyncSession = None) -> Dict:
    """Index standings JSON for a season and region."""
    if db is None:
        async with async_session_maker() as session:
            return await index_standings(bucket_id, region, use_cache, session)
    
    url = get_standings_url(bucket_id, region)
    status_key = f"standings_{bucket_id}_{region}"
    
    # Check cache first
    if use_cache:
        cached = await get_cached_response(url, db)
        if cached:
            cache_indexing_status[status_key] = {
                "status": "completed",
                "bucket_id": bucket_id,
                "region": region,
                "cached": True,
                "message": "Already cached"
            }
            return cached
    
    # Fetch from API
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            
            # Cache it
            await cache_response(
                endpoint_type="standings",
                url=url,
                response_json=data,
                db=db,
                bucket_id=bucket_id,
                region=region,
                http_status=response.status_code
            )
            await db.commit()
            
            cache_indexing_status[status_key] = {
                "status": "completed",
                "bucket_id": bucket_id,
                "region": region,
                "cached": False,
                "message": "Fetched and cached"
            }
            return data
        except Exception as e:
            cache_indexing_status[status_key] = {
                "status": "error",
                "bucket_id": bucket_id,
                "region": region,
                "error": str(e)
            }
            raise


async def index_player_stats(player_id: int, bucket_id: int, use_cache: bool = True, db: AsyncSession = None) -> Optional[Dict]:
    """Index player stats JSON for a player and season."""
    if db is None:
        async with async_session_maker() as session:
            return await index_player_stats(player_id, bucket_id, use_cache, session)
    
    url = PLAYER_STATS_URL.format(player_id=player_id, bucket_id=bucket_id)
    
    # Check cache first
    if use_cache:
        cached = await get_cached_response(url, db)
        if cached:
            return cached
    
    # Fetch from API
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            if response.status_code == 404:
                # Player doesn't exist or no data
                return None
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == "OK":
                stats_data = data.get("data")
                if stats_data:
                    # Cache the full response
                    await cache_response(
                        endpoint_type="player_stats",
                        url=url,
                        response_json=data,
                        db=db,
                        bucket_id=bucket_id,
                        player_id=player_id,
                        http_status=response.status_code
                    )
                    await db.commit()
                return stats_data
            return None
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception as e:
            print(f"Error fetching player stats for {player_id}: {e}")
            return None


async def index_player_events_list(player_id: int, bucket_id: int, use_cache: bool = True, db: AsyncSession = None) -> Optional[List[Dict]]:
    """Index player events list JSON for a player and season."""
    if db is None:
        async with async_session_maker() as session:
            return await index_player_events_list(player_id, bucket_id, use_cache, session)
    
    url = PLAYER_EVENTS_LIST_URL.format(player_id=player_id, bucket_id=bucket_id)
    
    # Check cache first
    if use_cache:
        cached = await get_cached_response(url, db)
        if cached:
            if cached.get("status") == "OK":
                return cached.get("data", [])
            return None
    
    # Fetch from API
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            
            # Cache the full response
            await cache_response(
                endpoint_type="player_events",
                url=url,
                response_json=data,
                db=db,
                bucket_id=bucket_id,
                player_id=player_id,
                http_status=response.status_code
            )
            await db.commit()
            
            if data.get("status") == "OK":
                return data.get("data", [])
            return None
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception as e:
            print(f"Error fetching player events for {player_id}: {e}")
            return None


async def index_event_info(event_id: int, use_cache: bool = True, db: AsyncSession = None) -> Optional[Dict]:
    """Index event info JSON for an event."""
    if db is None:
        async with async_session_maker() as session:
            return await index_event_info(event_id, use_cache, session)
    
    url = EVENT_INFO_URL.format(event_id=event_id)
    
    # Check cache first
    if use_cache:
        cached = await get_cached_response(url, db)
        if cached:
            if cached.get("status") == "OK":
                return cached.get("data")
            return None
    
    # Fetch from API
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            
            # Cache the full response
            await cache_response(
                endpoint_type="event_info",
                url=url,
                response_json=data,
                db=db,
                event_id=event_id,
                http_status=response.status_code
            )
            await db.commit()
            
            if data.get("status") == "OK":
                return data.get("data")
            return None
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception as e:
            print(f"Error fetching event info for {event_id}: {e}")
            return None


async def index_event_player_stats(event_id: int, use_cache: bool = True, db: AsyncSession = None) -> Optional[List[Dict]]:
    """Index event player stats JSON for an event."""
    if db is None:
        async with async_session_maker() as session:
            return await index_event_player_stats(event_id, use_cache, session)
    
    url = EVENT_PLAYER_STATS_URL.format(event_id=event_id)
    
    # Check cache first
    if use_cache:
        cached = await get_cached_response(url, db)
        if cached:
            if cached.get("status") == "OK":
                return cached.get("data", [])
            return None
    
    # Fetch from API
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            
            # Cache the full response
            await cache_response(
                endpoint_type="event_player_stats",
                url=url,
                response_json=data,
                db=db,
                event_id=event_id,
                http_status=response.status_code
            )
            await db.commit()
            
            if data.get("status") == "OK":
                return data.get("data", [])
            return None
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception as e:
            print(f"Error fetching event player stats for {event_id}: {e}")
            return None


async def index_event_standings(event_id: int, use_cache: bool = True, db: AsyncSession = None) -> Optional[List[Dict]]:
    """Index event standings JSON for an event."""
    if db is None:
        async with async_session_maker() as session:
            return await index_event_standings(event_id, use_cache, session)
    
    url = EVENT_STANDINGS_URL.format(event_id=event_id)
    
    # Check cache first
    if use_cache:
        cached = await get_cached_response(url, db)
        if cached:
            if cached.get("status") == "OK":
                return cached.get("data", [])
            return None
    
    # Fetch from API
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            
            # Cache the full response
            await cache_response(
                endpoint_type="event_standings",
                url=url,
                response_json=data,
                db=db,
                event_id=event_id,
                http_status=response.status_code
            )
            await db.commit()
            
            if data.get("status") == "OK":
                return data.get("data", [])
            return None
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception as e:
            print(f"Error fetching event standings for {event_id}: {e}")
            return None


async def index_bracket_data(event_id: int, use_cache: bool = True, db: AsyncSession = None) -> Optional[Dict]:
    """Index bracket data JSON for an event."""
    if db is None:
        async with async_session_maker() as session:
            return await index_bracket_data(event_id, use_cache, session)
    
    url = EVENT_BRACKET_URL.format(event_id=event_id)
    
    # Check cache first
    if use_cache:
        cached = await get_cached_response(url, db)
        if cached:
            return cached
    
    # Fetch from API
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            
            # Cache the full response
            await cache_response(
                endpoint_type="bracket_data",
                url=url,
                response_json=data,
                db=db,
                event_id=event_id,
                http_status=response.status_code
            )
            await db.commit()
            
            if data.get("status") == "OK":
                return data
            return None
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception as e:
            print(f"Error fetching bracket data for {event_id}: {e}")
            return None


async def index_match_stats(event_id: int, match_id: int, game_id: int = 1, use_cache: bool = True, db: AsyncSession = None) -> Optional[Dict]:
    """Index match stats JSON for a match and game."""
    if db is None:
        async with async_session_maker() as session:
            return await index_match_stats(event_id, match_id, game_id, use_cache, session)
    
    url = EVENT_MATCH_STATS_URL.format(event_id=event_id, match_id=match_id, game_id=game_id)
    
    # Check cache first
    if use_cache:
        cached = await get_cached_response(url, db)
        if cached:
            return cached
    
    # Fetch from API
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            # Handle 4xx errors as "game doesn't exist"
            if 400 <= response.status_code < 500:
                return None
            response.raise_for_status()
            data = response.json()
            
            # Check for error status
            if data.get("status") == "ERROR" or data.get("status") == "error":
                return None
            
            # Cache the full response
            await cache_response(
                endpoint_type="match_stats",
                url=url,
                response_json=data,
                db=db,
                event_id=event_id,
                match_id=match_id,
                game_id=game_id,
                http_status=response.status_code
            )
            await db.commit()
            
            return data
        except httpx.HTTPStatusError as e:
            if 400 <= e.response.status_code < 500:
                return None
            raise
        except Exception as e:
            print(f"Error fetching match stats for event {event_id}, match {match_id}, game {game_id}: {e}")
            return None


# Bulk indexing functions for full season indexing

async def index_all_standings_for_season(bucket_id: int, db: AsyncSession = None) -> Dict:
    """Index standings for US only for a season."""
    if db is None:
        async with async_session_maker() as session:
            return await index_all_standings_for_season(bucket_id, session)
    
    status_key = f"standings_{bucket_id}"
    cache_indexing_status[status_key] = {
        "status": "running",
        "bucket_id": bucket_id,
        "started_at": datetime.utcnow().isoformat(),
        "us_status": "pending"
    }
    
    try:
        # Index US standings only
        cache_indexing_status[status_key]["us_status"] = "running"
        await index_standings(bucket_id, "us", use_cache=True, db=db)
        cache_indexing_status[status_key]["us_status"] = "completed"
        
        cache_indexing_status[status_key]["status"] = "completed"
        cache_indexing_status[status_key]["completed_at"] = datetime.utcnow().isoformat()
        return cache_indexing_status[status_key]
    except Exception as e:
        cache_indexing_status[status_key]["status"] = "error"
        cache_indexing_status[status_key]["error"] = str(e)
        raise


async def index_all_player_data_for_season(bucket_id: int, db: AsyncSession = None, max_players: Optional[int] = None) -> Dict:
    """Index all player stats and events lists for a season.
    
    First fetches standings to get player list, then indexes stats and events for each player.
    """
    if db is None:
        async with async_session_maker() as session:
            return await index_all_player_data_for_season(bucket_id, session, max_players)
    
    status_key = f"players_{bucket_id}"
    cache_indexing_status[status_key] = {
        "status": "running",
        "bucket_id": bucket_id,
        "started_at": datetime.utcnow().isoformat(),
        "total_players": 0,
        "processed_players": 0,
        "stats_indexed": 0,
        "events_indexed": 0,
        "errors": 0
    }
    
    try:
        # Get US standings to find all players
        standings_us = await index_standings(bucket_id, "us", use_cache=True, db=db)
        
        # Collect all player IDs from US standings only
        player_ids = set()
        if standings_us and "playerACLStandingsList" in standings_us:
            for player in standings_us["playerACLStandingsList"]:
                player_id = player.get("playerID")
                if player_id:
                    player_ids.add(player_id)
        
        player_list = list(player_ids)
        if max_players:
            player_list = player_list[:max_players]
        
        total = len(player_list)
        cache_indexing_status[status_key]["total_players"] = total
        
        # Index player stats and events
        for i, player_id in enumerate(player_list):
            try:
                # Index player stats
                stats = await index_player_stats(player_id, bucket_id, use_cache=True, db=db)
                if stats:
                    cache_indexing_status[status_key]["stats_indexed"] += 1
                
                # Index player events list
                events = await index_player_events_list(player_id, bucket_id, use_cache=True, db=db)
                if events:
                    cache_indexing_status[status_key]["events_indexed"] += 1
                
                cache_indexing_status[status_key]["processed_players"] = i + 1
                
                # Commit every 10 players
                if (i + 1) % 10 == 0:
                    await db.commit()
                    await asyncio.sleep(0.1)  # Small delay to avoid rate limiting
            except Exception as e:
                cache_indexing_status[status_key]["errors"] += 1
                print(f"Error indexing player {player_id}: {e}")
                continue
        
        await db.commit()
        cache_indexing_status[status_key]["status"] = "completed"
        cache_indexing_status[status_key]["completed_at"] = datetime.utcnow().isoformat()
        return cache_indexing_status[status_key]
    except Exception as e:
        cache_indexing_status[status_key]["status"] = "error"
        cache_indexing_status[status_key]["error"] = str(e)
        raise


async def index_all_events_for_season(bucket_id: int, db: AsyncSession = None) -> Dict:
    """Index all events for a season by discovering them from player events lists.
    
    Can work from cached player events lists OR by fetching player events lists
    directly from standings (if standings are cached but player events aren't).
    """
    if db is None:
        async with async_session_maker() as session:
            return await index_all_events_for_season(bucket_id, session)
    
    status_key = f"events_{bucket_id}"
    cache_indexing_status[status_key] = {
        "status": "running",
        "bucket_id": bucket_id,
        "started_at": datetime.utcnow().isoformat(),
        "total_events": 0,
        "processed_events": 0,
        "event_info_indexed": 0,
        "event_player_stats_indexed": 0,
        "event_standings_indexed": 0,
        "bracket_data_indexed": 0,
        "players_processed": 0
    }
    
    try:
        # First, try to get cached player events lists
        result = await db.execute(
            select(ACLAPICache).where(
                and_(
                    ACLAPICache.endpoint_type == "player_events",
                    ACLAPICache.bucket_id == bucket_id
                )
            )
        )
        cached_events_lists = result.scalars().all()
        
        event_ids = set()
        
        # If we have cached player events lists, use those
        if cached_events_lists:
            for cache_entry in cached_events_lists:
                events_data = cache_entry.response_json.get("data", [])
                for event in events_data:
                    event_id = event.get("eventID") or event.get("event_id") or event.get("eventId")
                    if event_id:
                        event_ids.add(int(event_id))
        else:
            # No cached player events lists - get player IDs from standings and fetch their events
            standings_result = await db.execute(
                select(ACLAPICache).where(
                    and_(
                        ACLAPICache.endpoint_type == "standings",
                        ACLAPICache.bucket_id == bucket_id,
                        ACLAPICache.region == "us"
                    )
                )
            )
            standings_entry = standings_result.scalar_one_or_none()
            
            if standings_entry:
                standings_data = standings_entry.response_json
                player_list = standings_data.get("playerACLStandingsList", [])
                
                # Fetch player events lists for each player (and cache them)
                for i, player in enumerate(player_list):
                    player_id = player.get("playerID")
                    if not player_id:
                        continue
                    
                    try:
                        # Fetch and cache player events list
                        events_list = await index_player_events_list(player_id, bucket_id, use_cache=True, db=db)
                        if events_list:
                            for event in events_list:
                                event_id = event.get("eventID") or event.get("event_id") or event.get("eventId")
                                if event_id:
                                    event_ids.add(int(event_id))
                        
                        cache_indexing_status[status_key]["players_processed"] = i + 1
                        
                        # Commit every 50 players
                        if (i + 1) % 50 == 0:
                            await db.commit()
                            await asyncio.sleep(0.1)
                    except Exception as e:
                        print(f"Error fetching events for player {player_id}: {e}")
                        continue
                
                await db.commit()
            else:
                # No standings cached either - can't discover events
                cache_indexing_status[status_key]["status"] = "error"
                cache_indexing_status[status_key]["error"] = "No cached standings or player events lists found. Index standings first."
                return cache_indexing_status[status_key]
        
        event_list = list(event_ids)
        total = len(event_list)
        cache_indexing_status[status_key]["total_events"] = total
        
        # Index each event
        for i, event_id in enumerate(event_list):
            try:
                # Index event info
                event_info = await index_event_info(event_id, use_cache=True, db=db)
                if event_info:
                    cache_indexing_status[status_key]["event_info_indexed"] += 1
                
                # Index event player stats
                player_stats = await index_event_player_stats(event_id, use_cache=True, db=db)
                if player_stats:
                    cache_indexing_status[status_key]["event_player_stats_indexed"] += 1
                
                # Index event standings
                standings = await index_event_standings(event_id, use_cache=True, db=db)
                if standings:
                    cache_indexing_status[status_key]["event_standings_indexed"] += 1
                
                # Index bracket data
                bracket = await index_bracket_data(event_id, use_cache=True, db=db)
                if bracket:
                    cache_indexing_status[status_key]["bracket_data_indexed"] += 1
                
                cache_indexing_status[status_key]["processed_events"] = i + 1
                
                # Commit every 5 events
                if (i + 1) % 5 == 0:
                    await db.commit()
                    await asyncio.sleep(0.2)  # Small delay
            except Exception as e:
                print(f"Error indexing event {event_id}: {e}")
                continue
        
        await db.commit()
        cache_indexing_status[status_key]["status"] = "completed"
        cache_indexing_status[status_key]["completed_at"] = datetime.utcnow().isoformat()
        return cache_indexing_status[status_key]
    except Exception as e:
        cache_indexing_status[status_key]["status"] = "error"
        cache_indexing_status[status_key]["error"] = str(e)
        raise


async def index_all_games_for_season(bucket_id: int, db: AsyncSession = None) -> Dict:
    """Index all match/game stats for all events in a season."""
    if db is None:
        async with async_session_maker() as session:
            return await index_all_games_for_season(bucket_id, session)
    
    status_key = f"games_{bucket_id}"
    cache_indexing_status[status_key] = {
        "status": "running",
        "bucket_id": bucket_id,
        "started_at": datetime.utcnow().isoformat(),
        "total_events": 0,
        "processed_events": 0,
        "total_matches": 0,
        "processed_matches": 0,
        "total_games": 0,
        "processed_games": 0
    }
    
    try:
        # Get all cached bracket data for this season's events
        # First, get all events for this season
        events_result = await db.execute(
            select(ACLAPICache).where(
                and_(
                    ACLAPICache.endpoint_type == "event_info",
                    ACLAPICache.bucket_id == bucket_id
                )
            )
        )
        # Actually, we need to get events from bracket_data or event_info
        # Let's get bracket data which has match info
        bracket_result = await db.execute(
            select(ACLAPICache).where(ACLAPICache.endpoint_type == "bracket_data")
        )
        bracket_entries = bracket_result.scalars().all()
        
        # Process each bracket to find matches
        for bracket_entry in bracket_entries:
            event_id = bracket_entry.event_id
            bracket_data = bracket_entry.response_json
            
            # Extract matches from bracketDetails
            bracket_details = bracket_data.get("bracketDetails", [])
            if not bracket_details:
                continue
            
            matches_found = 0
            games_found = 0
            
            for bracket_match in bracket_details:
                match_id = bracket_match.get("bracketmatchid")
                if not match_id:
                    continue
                
                matches_found += 1
                cache_indexing_status[status_key]["total_matches"] += 1
                
                # Try to index games for this match (game_id 1, 2, 3, ...)
                for game_id in range(1, 10):  # Try up to 9 games per match
                    try:
                        game_data = await index_match_stats(
                            event_id, match_id, game_id, use_cache=True, db=db
                        )
                        if game_data:
                            games_found += 1
                            cache_indexing_status[status_key]["total_games"] += 1
                            cache_indexing_status[status_key]["processed_games"] += 1
                        else:
                            # Game doesn't exist, stop trying
                            break
                    except Exception as e:
                        # Error, stop trying more games for this match
                        break
                
                cache_indexing_status[status_key]["processed_matches"] += 1
                
                # Commit every 10 matches
                if matches_found % 10 == 0:
                    await db.commit()
                    await asyncio.sleep(0.1)
            
            cache_indexing_status[status_key]["processed_events"] += 1
        
        await db.commit()
        cache_indexing_status[status_key]["status"] = "completed"
        cache_indexing_status[status_key]["completed_at"] = datetime.utcnow().isoformat()
        return cache_indexing_status[status_key]
    except Exception as e:
        cache_indexing_status[status_key]["status"] = "error"
        cache_indexing_status[status_key]["error"] = str(e)
        raise

