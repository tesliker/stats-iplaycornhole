import httpx
import asyncio
from typing import List, Dict, Optional
from datetime import datetime

# Mapping bucket_id to year range for standings URL
BUCKET_YEAR_MAP = {
    11: "2025-2026",
    10: "2024-2025",
    9: "2023-2024",
    8: "2022-2023",
    7: "2021-2022",
    6: "2020-2021",
    5: "2019-2020",
    4: "2018-2019",
    3: "2017-2018",
    2: "2016-2017",
    1: "2015-2016",
    0: "2014-2015",
}

PLAYER_STATS_URL = "https://api.iplayacl.com/api/v1/yearly-player-stats/{player_id}?bucketID={bucket_id}"

def get_standings_url(bucket_id: int, region: str = "us") -> str:
    """Generate standings URL based on bucket_id and region.
    
    Args:
        bucket_id: Season bucket ID
        region: "us" or "canada"
    """
    year_range = BUCKET_YEAR_MAP.get(bucket_id)
    if not year_range:
        # Fallback: try to derive from bucket_id (bucket 11 = 2025-2026, etc.)
        # This assumes bucket_id 11 is 2025-2026, bucket_id 10 is 2024-2025
        start_year = 2025 - (11 - bucket_id)
        year_range = f"{start_year}-{start_year + 1}"
    
    if region.lower() == "canada":
        return f"https://mysqlvm.blob.core.windows.net/acl-standings/{year_range}/acl-overall-canada-standings.json"
    else:
        return f"https://mysqlvm.blob.core.windows.net/acl-standings/{year_range}/acl-overall-standings.json"

async def fetch_standings(bucket_id: int = 11, region: str = "us") -> Dict:
    """Fetch overall standings for a given bucket/season and region.
    
    Args:
        bucket_id: Season bucket ID
        region: "us" or "canada" (default: "us")
    """
    url = get_standings_url(bucket_id, region)
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()

async def fetch_standings_both(bucket_id: int = 11) -> Dict:
    """Fetch both US and Canada standings for a given bucket/season.
    Returns combined data with region information.
    
    Args:
        bucket_id: Season bucket ID
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Fetch both in parallel
        us_url = get_standings_url(bucket_id, "us")
        canada_url = get_standings_url(bucket_id, "canada")
        
        try:
            us_response, canada_response = await asyncio.gather(
                client.get(us_url),
                client.get(canada_url),
                return_exceptions=True
            )
            
            # Process US standings
            us_data = {}
            if isinstance(us_response, httpx.Response):
                us_response.raise_for_status()
                us_data = us_response.json()
                # Add region marker to each player
                if "playerACLStandingsList" in us_data:
                    for player in us_data["playerACLStandingsList"]:
                        player["_region"] = "us"
            
            # Process Canada standings
            canada_data = {}
            if isinstance(canada_response, httpx.Response):
                canada_response.raise_for_status()
                canada_data = canada_response.json()
                # Add region marker to each player
                if "playerACLStandingsList" in canada_data:
                    for player in canada_data["playerACLStandingsList"]:
                        player["_region"] = "canada"
            
            # Combine the data
            combined_players = []
            if "playerACLStandingsList" in us_data:
                combined_players.extend(us_data["playerACLStandingsList"])
            if "playerACLStandingsList" in canada_data:
                combined_players.extend(canada_data["playerACLStandingsList"])
            
            return {
                "status": "OK",
                "playerACLStandingsList": combined_players,
                "us_count": len(us_data.get("playerACLStandingsList", [])),
                "canada_count": len(canada_data.get("playerACLStandingsList", []))
            }
            
        except Exception as e:
            print(f"Error fetching combined standings: {e}")
            # Fallback to US only if Canada fails
            print("Falling back to US-only standings...")
            try:
                us_response = await client.get(us_url)
                us_response.raise_for_status()
                us_data = us_response.json()
                if "playerACLStandingsList" in us_data:
                    for player in us_data["playerACLStandingsList"]:
                        player["_region"] = "us"
                print(f"Successfully fetched US standings: {len(us_data.get('playerACLStandingsList', []))} players")
                return us_data
            except Exception as fallback_error:
                print(f"Error fetching US standings as fallback: {fallback_error}")
                raise

async def fetch_player_stats(player_id: int, bucket_id: int = 11) -> Optional[Dict]:
    """Fetch detailed stats for a specific player."""
    url = PLAYER_STATS_URL.format(player_id=player_id, bucket_id=bucket_id)
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "OK":
                return data.get("data")
            return None
        except Exception as e:
            print(f"Error fetching stats for player {player_id}: {e}")
            return None

# Event-related API endpoints
PLAYER_EVENTS_LIST_URL = "https://api.iplayacl.com/api/v1/player-events-list/playerID/{player_id}/bucketID/{bucket_id}"
EVENT_INFO_URL = "https://api.iplayacl.com/api/v1/events/{event_id}"
EVENT_PLAYER_STATS_URL = "https://api.iplayacl.com/api/v1/event-player-stats/{event_id}"
EVENT_STANDINGS_URL = "https://api.iplayacl.com/api/v1/event-standings/{event_id}"
EVENT_BRACKET_URL = "https://api.iplayacl.com/api/v1/bracket-data/{event_id}"
EVENT_MATCH_STATS_URL = "https://api.iplayacl.com/api/v1/match-stats/eventid/{event_id}/matchid/{match_id}/gameid/{game_id}"

async def fetch_player_events_list(player_id: int, bucket_id: int = 11) -> Optional[List[Dict]]:
    """Fetch list of events a player participated in for a season."""
    url = PLAYER_EVENTS_LIST_URL.format(player_id=player_id, bucket_id=bucket_id)
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "OK":
                return data.get("data", [])
            return None
        except Exception as e:
            print(f"Error fetching events list for player {player_id}: {e}")
            return None

async def fetch_event_info(event_id: int) -> Optional[Dict]:
    """Fetch event information."""
    url = EVENT_INFO_URL.format(event_id=event_id)
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "OK":
                return data.get("data")
            return None
        except Exception as e:
            print(f"Error fetching event info for {event_id}: {e}")
            return None

async def fetch_event_player_stats(event_id: int) -> Optional[List[Dict]]:
    """Fetch player statistics for an event."""
    url = EVENT_PLAYER_STATS_URL.format(event_id=event_id)
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "OK":
                return data.get("data", [])
            return None
        except Exception as e:
            print(f"Error fetching event player stats for {event_id}: {e}")
            return None

async def fetch_event_standings(event_id: int) -> Optional[List[Dict]]:
    """Fetch event standings."""
    url = EVENT_STANDINGS_URL.format(event_id=event_id)
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "OK":
                return data.get("data", [])
            return None
        except Exception as e:
            print(f"Error fetching event standings for {event_id}: {e}")
            return None

async def fetch_bracket_data(event_id: int) -> Optional[Dict]:
    """Fetch bracket/match data for an event.
    
    Returns the complete response data, including bracketDetails at the top level.
    """
    url = EVENT_BRACKET_URL.format(event_id=event_id)
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "OK":
                # bracketDetails is at the top level of the response, not in data
                # Return the full response so we have access to bracketDetails
                return data
            return None
        except Exception as e:
            print(f"Error fetching bracket data for {event_id}: {e}")
            return None

async def fetch_match_stats(event_id: int, match_id: int, game_id: int = 1) -> Optional[Dict]:
    """Fetch match stats for a specific match and game.
    
    Returns None if the match/game doesn't exist (404, 409, or other 4xx errors).
    """
    url = EVENT_MATCH_STATS_URL.format(event_id=event_id, match_id=match_id, game_id=game_id)
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            # Handle 4xx errors as "game doesn't exist" (404, 409, etc.)
            if 400 <= response.status_code < 500:
                # Match/game doesn't exist or is in conflict state
                return None
            response.raise_for_status()
            data = response.json()
            # Check for error status in the response
            if data.get("status") == "ERROR" or data.get("status") == "error":
                return None
            # Return the data (should have event_match_details or match data)
            return data
        except httpx.HTTPStatusError as e:
            # Handle 4xx errors (including 409 Conflict) as "game doesn't exist"
            if 400 <= e.response.status_code < 500:
                return None
            raise
        except Exception as e:
            print(f"Error fetching match stats for event {event_id}, match {match_id}, game {game_id}: {e}")
            return None

def detect_event_type(event_name: str, event_data: Optional[Dict] = None) -> str:
    """Detect event type from name or data. Prioritizes API eventType over name matching."""
    # First, check API eventType if available (most reliable)
    if event_data:
        api_event_type = event_data.get("eventType") or event_data.get("type")
        if api_event_type:
            api_type_lower = api_event_type.lower()
            # Map API types to our types
            if api_event_type == "O":
                return "open"
            elif api_event_type == "R":
                return "regional"
            elif api_event_type == "N":
                return "national"
            elif api_event_type == "S":
                return "signature"
            elif api_event_type == "L":
                return "local"  # Keep as "local" so we can filter it out
    
    # Fallback to name-based detection if no API type
    if not event_name:
        return "unknown"
    
    name_lower = event_name.lower()
    
    # Check for signature/national
    if "signature" in name_lower or "national" in name_lower:
        return "signature"
    
    # Check for open (only if it has "#" to avoid false positives)
    if "open" in name_lower and "#" in event_name:
        return "open"
    
    # Check for regional
    if "regional" in name_lower:
        return "regional"
    
    return "unknown"

def extract_event_number(event_name: str) -> Optional[int]:
    """Extract event number from name like 'Open #2'."""
    import re
    match = re.search(r'#(\d+)', event_name)
    if match:
        return int(match.group(1))
    return None

def extract_base_event_name(event_name: str) -> str:
    """Extract base event name from full bracket name.
    
    Examples:
    - "2025/26 ACL Open #2 Winter Haven Tier 1 Singles Bracket C" -> "Open #2 Winter Haven"
    - "2025/26 ACL Open #2 Winter Haven Tier 1 - Doubles Bracket B" -> "Open #2 Winter Haven"
    - "Winter Haven Open SitnGo #2" -> "Open SitnGo #2 Winter Haven"
    """
    import re
    if not event_name:
        return ""
    
    # Remove year prefix like "2025/26 ACL"
    name = re.sub(r'^\d{4}/\d{2}\s+ACL\s+', '', event_name)
    
    # Try to extract "Open #X Location" pattern
    open_match = re.search(r'(Open\s+#?\d+[^T]*?)(?:\s+Tier|\s+Bracket|\s+-|\s+Doubles|\s+Singles|\s+Blind|\s+SitnGo|$)', name, re.IGNORECASE)
    if open_match:
        base = open_match.group(1).strip()
        # Try to extract location (usually after Open #X)
        location_match = re.search(r'Open\s+#?\d+\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)', name)
        if location_match:
            location = location_match.group(1)
            return f"{base} {location}"
        return base
    
    # Try regional pattern
    regional_match = re.search(r'(Regional[^T]*?)(?:\s+Tier|\s+Bracket|\s+-|$)', name, re.IGNORECASE)
    if regional_match:
        return regional_match.group(1).strip()
    
    # Fallback: return first part before "Tier" or "Bracket"
    fallback = re.split(r'\s+(?:Tier|Bracket|Doubles|Singles|Blind|SitnGo)', name, flags=re.IGNORECASE)[0]
    return fallback.strip()

def extract_bracket_name(event_name: str) -> str:
    """Extract bracket/tier name from full event name.
    
    Examples:
    - "2025/26 ACL Open #2 Winter Haven Tier 1 Singles Bracket C" -> "Tier 1 Singles Bracket C"
    - "2025/26 ACL Open #2 Winter Haven Tier 1 - Doubles Bracket B" -> "Tier 1 Doubles Bracket B"
    - "2025/26 ACL Open #2 Winter Haven Tier 1 Singles Final 4" -> "Tier 1 Singles Final 4"
    """
    import re
    if not event_name:
        return ""
    
    # Try to find bracket info after base event name
    # Include "Final" to capture finals like "Tier 1 Singles Final 4"
    bracket_match = re.search(r'(?:Tier\s+\d+|Bracket\s+[A-Z]|Doubles|Singles|Blind\s+Draw|SitnGo|Final).*$', event_name, re.IGNORECASE)
    if bracket_match:
        return bracket_match.group(0).strip()
    
    return ""

def parse_player_data(standings_data: Dict, stats_data: Optional[Dict], bucket_id: int, snapshot_date: Optional[datetime] = None, region: Optional[str] = None) -> Dict:
    """Parse and combine standings and stats data into a player record.
    
    Args:
        standings_data: Player data from standings JSON
        stats_data: Detailed stats from player stats API
        bucket_id: Season bucket ID
        snapshot_date: Date for this snapshot
        region: "us" or "canada" (from _region field in standings data)
    """
    if snapshot_date is None:
        snapshot_date = datetime.utcnow()
    
    # Determine region from _region field or country_code
    if region is None:
        region = standings_data.get("_region")
        if not region:
            # Fallback: infer from country_code
            country_code = standings_data.get("playerCountryCode", "").upper()
            if country_code == "CA" or country_code == "CAN":
                region = "canada"
            else:
                region = "us"
    
    player = {
        "player_id": standings_data.get("playerID"),
        "bucket_id": bucket_id,
        "snapshot_date": snapshot_date,
        "first_name": standings_data.get("playerFirstName"),
        "last_name": standings_data.get("playerLastName"),
        "country_code": standings_data.get("playerCountryCode"),
        "country_name": standings_data.get("playerCountryName"),
        "state": standings_data.get("playerState"),
        "region": region,  # Add region field to distinguish US vs Canada
        "conference_id": standings_data.get("conferenceID"),
        "skill_level": standings_data.get("playerSkillLevel"),
        "rank": standings_data.get("rank"),
        "overall_total": standings_data.get("playerOverAllTotal", 0),
        "conference_bonus_points": standings_data.get("conferenceBonusPoints", 0),
        "conference_events_counter": standings_data.get("conferenceEventsCounter", 0),
        "national_bonus_points": standings_data.get("nationalBonusPoints", 0),
        "national_events_counter": standings_data.get("nationalEventsCounter", 0),
        "monthly_bonus": standings_data.get("playerMonthlyBonus", 0),
        "membership_bonus": standings_data.get("playerMembershipBonus", 0),
        "player_50_event_bonus": standings_data.get("player50EventBonus", 0),
        "monthly_event_counts": standings_data.get("monthlyEventCounts", {}),
        "pts_per_rnd": None,
        "rounds_total": None,
        "total_pts": None,
        "opponent_pts_per_rnd": None,
        "opponent_pts_total": None,
        "dpr": None,
        "four_bagger_pct": None,
        "bags_in_pct": None,
        "bags_on_pct": None,
        "bags_off_pct": None,
        "local_wins": 0,
        "local_losses": 0,
        "regional_wins": 0,
        "regional_losses": 0,
        "state_wins": 0,
        "state_losses": 0,
        "conference_wins": 0,
        "conference_losses": 0,
        "open_wins": 0,
        "open_losses": 0,
        "national_wins": 0,
        "national_losses": 0,
        "total_games": 0,
        "total_wins": 0,
        "total_losses": 0,
        "win_pct": 0,
        "player_cpi": None,
        "cpi_qualified": 0,
        "membership_id": None,
        "membership_expiry_date": None,
        "membership_status": None,
        "membership_type": None,
        "membership_name": None,
    }
    
    if stats_data:
        # Performance stats
        perf = stats_data.get("playerPerformanceStats", {})
        if perf:
            player["pts_per_rnd"] = perf.get("ptsPerRnd")
            player["rounds_total"] = perf.get("rdsTotal") or perf.get("rounds")
            player["total_pts"] = perf.get("totPtsTotal") or perf.get("totalPts")
            player["opponent_pts_per_rnd"] = perf.get("opponentPtsPerRnd") or perf.get("OppPtsPerRnd")
            player["opponent_pts_total"] = perf.get("oppPtsTotal") or perf.get("opponentPts")
            player["dpr"] = perf.get("DPR") or perf.get("diffPerRnd")
            player["four_bagger_pct"] = float(perf.get("fourBagPct") or perf.get("fourBaggerPct") or 0)
            player["bags_in_pct"] = float(perf.get("bagsInPct") or perf.get("BagsInPct") or 0)
            player["bags_on_pct"] = float(perf.get("bagsOnPct") or perf.get("BagsOnPct") or 0)
            player["bags_off_pct"] = float(perf.get("bagsOffPct") or perf.get("BagsOffPct") or 0)
        
        # Win/Loss stats
        wl = stats_data.get("playerWinLossStats", {})
        if wl:
            player["local_wins"] = wl.get("localWins", 0)
            player["local_losses"] = wl.get("localLosses", 0)
            player["regional_wins"] = wl.get("regionalWins", 0)
            player["regional_losses"] = wl.get("regionalLosses", 0)
            player["state_wins"] = wl.get("stateWins", 0)
            player["state_losses"] = wl.get("stateLosses", 0)
            player["conference_wins"] = wl.get("conferenceWins", 0)
            player["conference_losses"] = wl.get("conferenceLosses", 0)
            player["open_wins"] = wl.get("openWins", 0)
            player["open_losses"] = wl.get("openLosses", 0)
            player["national_wins"] = wl.get("nationalWins", 0)
            player["national_losses"] = wl.get("nationalLosses", 0)
            player["total_games"] = wl.get("totalGames", 0)
            player["total_wins"] = wl.get("totalWins", 0)
            player["total_losses"] = wl.get("totalLosses", 0)
            player["win_pct"] = wl.get("winPct", 0)
        
        # CPI stats
        cpi = stats_data.get("playerCPIStats", {})
        if cpi:
            player["player_cpi"] = cpi.get("playerCPI")
            player["cpi_qualified"] = cpi.get("CPIQualified", 0)
            player["membership_id"] = cpi.get("playerMembershipID")
            player["membership_expiry_date"] = cpi.get("playerMembershipExpiryDate")
            player["membership_status"] = cpi.get("playerMembershipStatus")
            player["membership_type"] = cpi.get("playerMembershipType")
            player["membership_name"] = cpi.get("playerMembershipName")
    
    return player

