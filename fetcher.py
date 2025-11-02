import httpx
from typing import List, Dict, Optional
from datetime import datetime

# Mapping bucket_id to year range for standings URL
BUCKET_YEAR_MAP = {
    11: "2025-2026",
    10: "2024-2025",
    9: "2023-2024",
    8: "2022-2023",
    7: "2021-2022",
    # Add more as needed
}

PLAYER_STATS_URL = "https://api.iplayacl.com/api/v1/yearly-player-stats/{player_id}?bucketID={bucket_id}"

def get_standings_url(bucket_id: int) -> str:
    """Generate standings URL based on bucket_id."""
    year_range = BUCKET_YEAR_MAP.get(bucket_id)
    if not year_range:
        # Fallback: try to derive from bucket_id (bucket 11 = 2025-2026, etc.)
        # This assumes bucket_id 11 is 2025-2026, bucket_id 10 is 2024-2025
        start_year = 2025 - (11 - bucket_id)
        year_range = f"{start_year}-{start_year + 1}"
    return f"https://mysqlvm.blob.core.windows.net/acl-standings/{year_range}/acl-overall-standings.json"

async def fetch_standings(bucket_id: int = 11) -> Dict:
    """Fetch overall standings for a given bucket/season."""
    url = get_standings_url(bucket_id)
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()

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

def parse_player_data(standings_data: Dict, stats_data: Optional[Dict], bucket_id: int, snapshot_date: Optional[datetime] = None) -> Dict:
    """Parse and combine standings and stats data into a player record."""
    if snapshot_date is None:
        snapshot_date = datetime.utcnow()
    
    player = {
        "player_id": standings_data.get("playerID"),
        "bucket_id": bucket_id,
        "snapshot_date": snapshot_date,
        "first_name": standings_data.get("playerFirstName"),
        "last_name": standings_data.get("playerLastName"),
        "country_code": standings_data.get("playerCountryCode"),
        "country_name": standings_data.get("playerCountryName"),
        "state": standings_data.get("playerState"),
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

