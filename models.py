from pydantic import BaseModel
from typing import Optional, List, Dict
from datetime import datetime

class PlayerResponse(BaseModel):
    id: int
    player_id: int
    bucket_id: int
    first_name: str
    last_name: str
    country_code: Optional[str]
    country_name: Optional[str]
    state: Optional[str]
    conference_id: Optional[int]
    skill_level: Optional[str]
    rank: Optional[int]
    overall_total: Optional[float]
    pts_per_rnd: Optional[float]
    rounds_total: Optional[int]
    total_pts: Optional[int]
    opponent_pts_per_rnd: Optional[float]
    opponent_pts_total: Optional[int]
    dpr: Optional[float]
    four_bagger_pct: Optional[float]
    bags_in_pct: Optional[float]
    bags_on_pct: Optional[float]
    bags_off_pct: Optional[float]
    total_games: Optional[int]
    total_wins: Optional[int]
    total_losses: Optional[int]
    win_pct: Optional[float]
    player_cpi: Optional[float]
    membership_name: Optional[str]
    last_updated: Optional[datetime]
    
    model_config = {"from_attributes": True}

class PlayerListResponse(BaseModel):
    players: List[PlayerResponse]
    total: int
    page: int
    page_size: int
    bucket_id: int

class StatsComparisonResponse(BaseModel):
    player_id: int
    first_name: str
    last_name: str
    seasons: List[Dict]
    
    model_config = {"from_attributes": True}

