from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Query, Request, Form, status as http_status, Path
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func, desc, asc
from sqlalchemy.orm import aliased
from typing import Optional, List
import asyncio
import os
from datetime import datetime, timedelta
import secrets
from functools import wraps

from database import get_db, init_db, Player, Event, PlayerEventStats, EventStanding, EventGame, EventMatch
from fetcher import fetch_standings, fetch_player_stats, parse_player_data
from models import PlayerResponse, PlayerListResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pydantic import BaseModel

app = FastAPI(title="Cornhole Player Stats")
scheduler = AsyncIOScheduler()

# Status tracking for ongoing fetches (in-memory, lightweight)
fetch_status = {}
fetch_control = {}  # Control flags: 'pause', 'stop', 'resume'
game_indexing_status = {}  # Status tracking for game indexing
event_indexing_status = {}  # Status tracking for event indexing
game_indexing_logs = {}  # Log buffer for game indexing operations
cache_indexing_status = {}  # Status tracking for cache indexing
cache_indexing_logs = {}  # Log buffer for cache indexing operations

# Admin credentials
ADMIN_USERNAME = "tesliker"
ADMIN_PASSWORD = "outkast"

security = HTTPBasic()

def verify_admin(credentials: HTTPBasicCredentials = Depends(security)):
    """Verify admin credentials."""
    is_correct_username = secrets.compare_digest(credentials.username, ADMIN_USERNAME)
    is_correct_password = secrets.compare_digest(credentials.password, ADMIN_PASSWORD)
    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=http_status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

class FetchPlayersRequest(BaseModel):
    player_ids: List[int]

# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    await init_db()
    
    # Setup weekly scheduler for season 11 (current season)
    # Run every Monday at 3 AM UTC
    scheduler.add_job(
        schedule_weekly_fetch,
        trigger=CronTrigger(day_of_week='mon', hour=3, minute=0),
        id='weekly_fetch_season_11',
        replace_existing=True
    )
    scheduler.start()
    print("Scheduler started: Weekly fetch for season 11 scheduled for Mondays at 3 AM UTC")

@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()

async def schedule_weekly_fetch():
    """Scheduled task to fetch season 11 data weekly."""
    from database import async_session_maker
    async with async_session_maker() as db:
        print(f"Weekly scheduled fetch started for bucket_id 11 at {datetime.utcnow()}")
        await update_player_data(11)
        print(f"Weekly scheduled fetch completed for bucket_id 11")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def read_root():
    with open("templates/index.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(username: str = Depends(verify_admin)):
    """Admin page for controlling data fetches."""
    with open("templates/admin.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/admin-local", response_class=HTMLResponse)
async def admin_local_page():  # Temporarily removed auth for debugging
    """Local admin page for testing with limited data."""
    with open("templates/admin-local.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/admin-acl", response_class=HTMLResponse)
async def admin_acl_page(username: str = Depends(verify_admin)):
    """ACL API cache indexer admin interface."""
    with open("templates/admin-acl.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/games", response_class=HTMLResponse)
async def games_page():
    """Games listing page."""
    with open("templates/games.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/head-to-head", response_class=HTMLResponse)
async def head_to_head_page():
    """Head-to-head matchup lookup page."""
    with open("templates/head_to_head.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/games/{game_id}", response_class=HTMLResponse)
async def game_detail_page(game_id: int):
    """Individual game detail page."""
    with open("templates/game_detail.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.post("/api/fetch-data/{bucket_id}")
async def fetch_data(bucket_id: int, background_tasks: BackgroundTasks):
    """Trigger data fetch for a specific bucket/season."""
    # Initialize status tracking
    fetch_status[bucket_id] = {
        "status": "running",
        "bucket_id": bucket_id,
        "started_at": datetime.utcnow().isoformat(),
        "total_players": None,
        "processed_players": 0,
        "current_player": None,
        "error": None
    }
    background_tasks.add_task(update_player_data, bucket_id)
    return {"status": "started", "bucket_id": bucket_id, "message": "Data fetch started in background"}

@app.get("/api/fetch-status")
async def get_fetch_status():
    """Get status of all ongoing fetches and all monitored buckets."""
    # Also check database for additional info
    from database import async_session_maker
    async with async_session_maker() as db:
        # Always check these bucket IDs, even if not in fetch_status
        monitored_buckets = [11, 10, 9, 8, 7]
        status_with_db = {}
        
        for bucket_id in monitored_buckets:
            # Start with in-memory status if it exists, otherwise create default
            if bucket_id in fetch_status:
                status_copy = fetch_status[bucket_id].copy()
            else:
                status_copy = {
                    "bucket_id": bucket_id,
                    "status": "not_running",
                    "total_players": 0,
                    "processed_players": 0,
                    "current_player": None,
                    "started_at": None,
                    "completed_at": None,
                    "error": None
                }
            
            # Always get current record count from database for this bucket
            try:
                # For season 11, get count of latest snapshot
                if bucket_id == 11:
                    # Get the most recent snapshot date
                    latest_snapshot = await db.execute(
                        select(func.max(Player.snapshot_date)).where(Player.bucket_id == bucket_id)
                    )
                    latest_date = latest_snapshot.scalar()
                    if latest_date:
                        count_query = await db.execute(
                            select(func.count()).select_from(Player).where(
                                and_(Player.bucket_id == bucket_id, Player.snapshot_date == latest_date)
                            )
                        )
                        status_copy["records_in_db"] = count_query.scalar()
                        status_copy["latest_snapshot_date"] = latest_date.isoformat()
                    else:
                        status_copy["records_in_db"] = 0
                        status_copy["latest_snapshot_date"] = None
                else:
                    # For historical seasons, just count all records
                    count_query = await db.execute(
                        select(func.count()).select_from(Player).where(Player.bucket_id == bucket_id)
                    )
                    status_copy["records_in_db"] = count_query.scalar()
                    status_copy["latest_snapshot_date"] = None
            except Exception as e:
                status_copy["records_in_db"] = None
                status_copy["db_check_error"] = str(e)
                print(f"Error checking DB for bucket {bucket_id}: {e}")
            
            status_with_db[bucket_id] = status_copy
        
        return {
            "active_fetches": len([s for s in fetch_status.values() if s.get("status") == "running"]),
            "fetch_status": status_with_db
        }

@app.post("/api/fetch-control/{bucket_id}/pause")
async def pause_fetch(bucket_id: int, username: str = Depends(verify_admin)):
    """Pause a running fetch."""
    if bucket_id in fetch_status:
        fetch_control[bucket_id] = 'pause'
        fetch_status[bucket_id]["status"] = "paused"
        return {"status": "paused", "bucket_id": bucket_id, "message": "Fetch paused"}
    return {"status": "error", "message": "No active fetch found"}

@app.post("/api/fetch-control/{bucket_id}/resume")
async def resume_fetch(bucket_id: int, username: str = Depends(verify_admin)):
    """Resume a paused fetch."""
    if bucket_id in fetch_status:
        fetch_control[bucket_id] = 'resume'
        fetch_status[bucket_id]["status"] = "running"
        return {"status": "running", "bucket_id": bucket_id, "message": "Fetch resumed"}
    return {"status": "error", "message": "No fetch found to resume"}

@app.post("/api/fetch-control/{bucket_id}/stop")
async def stop_fetch(bucket_id: int, username: str = Depends(verify_admin)):
    """Stop a running fetch."""
    if bucket_id in fetch_status:
        fetch_control[bucket_id] = 'stop'
        fetch_status[bucket_id]["status"] = "stopped"
        return {"status": "stopped", "bucket_id": bucket_id, "message": "Fetch stopped"}
    return {"status": "error", "message": "No active fetch found"}

@app.post("/api/backup-database")
async def backup_database(username: str = Depends(verify_admin)):
    """Create a backup of the database. Admin only."""
    import subprocess
    import tempfile
    
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL not configured")
    
    try:
        # Parse connection string to extract components
        # Format: postgresql://user:pass@host:port/dbname
        import urllib.parse
        parsed = urllib.parse.urlparse(DATABASE_URL)
        
        # Create backup filename with timestamp
        backup_filename = f"cornhole_backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.sql"
        
        # Use pg_dump via psql (since we're in Python, we'll use subprocess)
        # Note: This requires pg_dump to be available in the container
        # For now, return instructions or use a simpler method
        return {
            "status": "backup_requested",
            "message": "Database backup initiated. In production, use flyctl mpg backup or pg_dump directly.",
            "instructions": "To backup: flyctl mpg backup --app fly-cornhole",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Backup failed: {str(e)}")

@app.get("/api/db-health")
async def db_health_check(username: str = Depends(verify_admin)):
    """Check database health and integrity."""
    from database import async_session_maker
    async with async_session_maker() as db:
        try:
            # Check integrity
            integrity_result = await db.execute(
                select(func.count()).select_from(Player)
            )
            total_records = integrity_result.scalar()
            
            # Get counts by bucket
            bucket_counts_query = await db.execute(
                select(Player.bucket_id, func.count()).group_by(Player.bucket_id)
            )
            bucket_counts = {str(bucket_id): count for bucket_id, count in bucket_counts_query.all()}
            
            # Get latest snapshot dates
            latest_snapshots = {}
            for bucket_id in [11, 10, 9, 8, 7]:
                if bucket_id == 11:
                    latest = await db.execute(
                        select(func.max(Player.snapshot_date)).where(Player.bucket_id == bucket_id)
                    )
                else:
                    latest = await db.execute(
                        select(func.max(Player.snapshot_date)).where(Player.bucket_id == bucket_id)
                    )
                date = latest.scalar()
                latest_snapshots[bucket_id] = date.isoformat() if date else None
            
            return {
                "status": "healthy",
                "total_records": total_records,
                "bucket_counts": bucket_counts,
                "latest_snapshots": latest_snapshots,
                "database_path": os.getenv("DATABASE_PATH", "/data/cornhole.db"),
                "volume_mounted": os.path.exists("/data"),
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }

@app.get("/api/fetch-status/{bucket_id}")
async def get_fetch_status_bucket(bucket_id: int):
    """Get status of a specific bucket fetch."""
    from database import async_session_maker
    async with async_session_maker() as db:
        # Get DB record count
        try:
            if bucket_id == 11:
                # Get the most recent snapshot date
                latest_snapshot = await db.execute(
                    select(func.max(Player.snapshot_date)).where(Player.bucket_id == bucket_id)
                )
                latest_date = latest_snapshot.scalar()
                if latest_date:
                    count_query = await db.execute(
                        select(func.count()).select_from(Player).where(
                            and_(Player.bucket_id == bucket_id, Player.snapshot_date == latest_date)
                        )
                    )
                    records_in_db = count_query.scalar()
                    # Get snapshot date info
                    snapshot_info = await db.execute(
                        select(Player.snapshot_date).where(
                            and_(Player.bucket_id == bucket_id, Player.snapshot_date == latest_date)
                        ).limit(1)
                    )
                    snapshot_date = snapshot_info.scalar()
                else:
                    records_in_db = 0
                    snapshot_date = None
            else:
                count_query = await db.execute(
                    select(func.count()).select_from(Player).where(Player.bucket_id == bucket_id)
                )
                records_in_db = count_query.scalar()
                snapshot_date = None
        except Exception as e:
            records_in_db = None
            snapshot_date = None
    
    # Check in-memory status first (for active fetches)
    if bucket_id in fetch_status:
        status = fetch_status[bucket_id].copy()
        status["records_in_db"] = records_in_db
        if snapshot_date:
            status["latest_snapshot_date"] = snapshot_date.isoformat()
        return status
    
    # No active fetch, return DB status
    response = {
        "status": "not_running",
        "bucket_id": bucket_id,
        "records_in_db": records_in_db,
        "message": "No active fetch for this bucket"
    }
    if snapshot_date:
        response["latest_snapshot_date"] = snapshot_date.isoformat()
    
    return response

@app.post("/api/fetch-specific-players/{bucket_id}")
async def fetch_specific_players(bucket_id: int, request: FetchPlayersRequest, background_tasks: BackgroundTasks):
    """Fetch data for specific players only (for testing/comparison)."""
    background_tasks.add_task(update_specific_players_data, bucket_id, request.player_ids)
    return {"status": "started", "bucket_id": bucket_id, "player_count": len(request.player_ids), "message": f"Fetching {len(request.player_ids)} specific players in background"}

async def update_specific_players_data(bucket_id: int, player_ids: List[int]):
    """Fetch data for specific players only."""
    from database import async_session_maker
    async with async_session_maker() as db:
        try:
            import os
            snapshot_date = datetime.utcnow()
            delay = float(os.getenv("FETCH_DELAY", "0.05"))
            
            print(f"Fetching data for {len(player_ids)} specific players for bucket {bucket_id}...")
            
            # Check if data already exists for historical seasons
            if bucket_id != 11:
                existing_check = await db.execute(
                    select(func.count()).select_from(Player).where(
                        and_(Player.bucket_id == bucket_id, Player.player_id.in_(player_ids))
                    )
                )
                if existing_check.scalar() == len(player_ids):
                    print(f"All {len(player_ids)} players already exist for bucket {bucket_id}. Skipping.")
                    return
            
            for idx, player_id in enumerate(player_ids):
                try:
                    # Fetch player stats
                    stats_data = await fetch_player_stats(player_id, bucket_id)
                    
                    # We need standings data too - get from standings API
                    standings_data = await fetch_standings(bucket_id)
                    player_standings = None
                    if standings_data.get("status") == "OK":
                        players_list = standings_data.get("playerACLStandingsList", [])
                        player_standings = next((p for p in players_list if p.get("playerID") == player_id), None)
                    
                    if not player_standings:
                        print(f"Player {player_id} not found in standings for bucket {bucket_id}")
                        continue
                    
                    # Parse combined data
                    player_record = parse_player_data(player_standings, stats_data, bucket_id, snapshot_date)
                    
                    # Check if this player already exists for this bucket
                    existing = await db.execute(
                        select(Player).where(
                            and_(
                                Player.player_id == player_id,
                                Player.bucket_id == bucket_id,
                                Player.snapshot_date == snapshot_date
                            )
                        )
                    )
                    if existing.scalar_one_or_none():
                        print(f"Player {player_id} already exists for bucket {bucket_id} with this snapshot_date, skipping")
                        continue
                    
                    new_player = Player(**player_record)
                    db.add(new_player)
                    
                    # Commit after each player for SQLite to avoid locking issues
                    try:
                        await db.commit()
                    except Exception as commit_error:
                        await db.rollback()
                        print(f"Commit error for player {player_id}, retrying...")
                        await asyncio.sleep(0.2)
                        try:
                            db.add(new_player)
                            await db.commit()
                        except Exception as retry_error:
                            print(f"Retry failed for player {player_id}: {retry_error}")
                            continue
                    
                    if (idx + 1) % 10 == 0:
                        print(f"Processed {idx + 1}/{len(player_ids)} players...")
                    
                    await asyncio.sleep(delay)
                    
                except Exception as e:
                    print(f"Error processing player {player_id}: {e}")
                    await db.rollback()
                    continue
            
            print(f"Finished updating {len(player_ids)} players for bucket {bucket_id} (snapshot_date: {snapshot_date})")
            
        except Exception as e:
            print(f"Error in update_specific_players_data: {e}")
            import traceback
            traceback.print_exc()

async def update_player_data_local(bucket_id: int):
    """Background task to fetch and update player data (LOCAL: Limited to 100 players)."""
    from database import async_session_maker
    async with async_session_maker() as db:
        try:
            # For Season 11, try to fetch both US and Canada. For historical seasons, fetch US only.
            if bucket_id == 11:
                try:
                    standings_data = await fetch_standings_both(bucket_id)
                    print(f"Fetched combined standings: {standings_data.get('us_count', 0)} US, {standings_data.get('canada_count', 0)} Canada players")
                except Exception as e:
                    print(f"Error fetching combined standings for Season 11: {e}")
                    print("Falling back to US-only for Season 11...")
                    standings_data = await fetch_standings(bucket_id, region="us")
                    # Add region marker
                    if "playerACLStandingsList" in standings_data:
                        for player in standings_data["playerACLStandingsList"]:
                            player["_region"] = "us"
                    standings_data["status"] = "OK"  # Ensure status is OK
            else:
                standings_data = await fetch_standings(bucket_id, region="us")
                # Add region marker for historical data
                if "playerACLStandingsList" in standings_data:
                    for player in standings_data["playerACLStandingsList"]:
                        player["_region"] = "us"
                standings_data["status"] = "OK"  # Ensure status is OK
            
            if standings_data.get("status") != "OK":
                print(f"Error fetching standings: {standings_data.get('message')}")
                return
            
            players = standings_data.get("playerACLStandingsList", [])
            # LOCAL MODE: Limit to 100 players
            max_players = 100
            if len(players) > max_players:
                print(f"LOCAL MODE: Limiting to {max_players} players (total available: {len(players)})")
                players = players[:max_players]
            
            print(f"Fetching data for {len(players)} players for bucket {bucket_id} (LOCAL MODE: max {max_players})...")
            
            # Update status tracking
            if bucket_id in fetch_status:
                fetch_status[bucket_id]["total_players"] = len(players)
                fetch_status[bucket_id]["status"] = "running"
            
            # Use same logic as regular update_player_data but with limited players
            # (Copy the rest of the logic from update_player_data)
            is_current_season = bucket_id == 11
            
            if is_current_season:
                now = datetime.utcnow()
                days_since_monday = now.weekday()
                week_start = now - timedelta(days=days_since_monday)
                week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
                snapshot_date = week_start
            else:
                snapshot_date = datetime.utcnow()
            
            # Skip existing check for local mode - just process
            delay = float(os.getenv("FETCH_DELAY", "0.05"))
            batch_size = int(os.getenv("BATCH_SIZE", "500"))
            
            existing_players_query = await db.execute(
                select(Player.player_id).where(
                    and_(
                        Player.bucket_id == bucket_id,
                        Player.snapshot_date == snapshot_date if is_current_season else True
                    )
                ).distinct()
            )
            existing_player_ids = set(existing_players_query.scalars().all())
            
            players_to_process = [p for p in players if p.get("playerID") not in existing_player_ids]
            
            if len(players_to_process) == 0:
                print(f"All players already processed for bucket {bucket_id}. Skipping.")
                if bucket_id in fetch_status:
                    fetch_status[bucket_id]["status"] = "completed"
                return
            
            print(f"Resuming: {len(existing_player_ids)} already processed, {len(players_to_process)} remaining to fetch...")
            
            # Process players
            for idx, player_data in enumerate(players_to_process):
                player_id = player_data.get("playerID")
                if not player_id or player_id in existing_player_ids:
                    continue
                
                try:
                    stats_data = await fetch_player_stats(player_id, bucket_id)
                    region = player_data.get("_region", "us")
                    player_record = parse_player_data(player_data, stats_data, bucket_id, snapshot_date, region=region)
                    
                    new_player = Player(**player_record)
                    db.add(new_player)
                    existing_player_ids.add(player_id)
                    
                    if (idx + 1) % 10 == 0:
                        await db.commit()
                        if bucket_id in fetch_status:
                            fetch_status[bucket_id]["processed_players"] = len(existing_player_ids)
                    
                    await asyncio.sleep(delay)
                except Exception as e:
                    print(f"Error processing player {player_id}: {e}")
                    await db.rollback()
                    continue
            
            await db.commit()
            print(f"Finished updating data for bucket {bucket_id} (LOCAL MODE: {len(players)} players)")
            
            if bucket_id in fetch_status:
                fetch_status[bucket_id]["status"] = "completed"
                fetch_status[bucket_id]["processed_players"] = len(players)
                fetch_status[bucket_id]["completed_at"] = datetime.utcnow().isoformat()
                fetch_status[bucket_id]["snapshot_date"] = snapshot_date.isoformat()
        except Exception as e:
            print(f"Error in update_player_data_local: {e}")
            import traceback
            traceback.print_exc()
            if bucket_id in fetch_status:
                fetch_status[bucket_id]["status"] = "error"
                fetch_status[bucket_id]["error"] = str(e)

async def update_player_data(bucket_id: int):
    """Background task to fetch and update player data."""
    from database import async_session_maker
    async with async_session_maker() as db:
        try:
            standings_data = await fetch_standings(bucket_id)
            
            if standings_data.get("status") != "OK":
                print(f"Error fetching standings: {standings_data.get('message')}")
                return
            
            players = standings_data.get("playerACLStandingsList", [])
            # Limit players for development/testing (only if DEV_PLAYER_LIMIT env var is explicitly set)
            import os
            dev_limit_str = os.getenv("DEV_PLAYER_LIMIT")
            if dev_limit_str:
                try:
                    dev_limit = int(dev_limit_str)
                    if dev_limit > 0 and len(players) > dev_limit:
                        print(f"Development mode: Limiting to {dev_limit} players (total available: {len(players)})")
                        players = players[:dev_limit]
                except ValueError:
                    pass  # Ignore invalid values, fetch all players
            print(f"Fetching data for {len(players)} players for bucket {bucket_id}...")
            
            # Update status tracking
            if bucket_id in fetch_status:
                fetch_status[bucket_id]["total_players"] = len(players)
                fetch_status[bucket_id]["status"] = "running"
            
            # For season 11, create weekly snapshots. For historical seasons, check if exists first
            is_current_season = bucket_id == 11
            
            if is_current_season:
                # Season 11: Use start of current week as snapshot date (Monday 00:00 UTC)
                # This way each week gets a fresh snapshot, but same week resumes from same snapshot
                now = datetime.utcnow()
                days_since_monday = now.weekday()  # 0 = Monday, 6 = Sunday
                week_start = now - timedelta(days=days_since_monday)
                week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
                snapshot_date = week_start
                print(f"Season 11: Using week snapshot date: {snapshot_date} (current week started {days_since_monday} days ago)")
            else:
                # Historical seasons: Use current date (but will only fetch once)
                snapshot_date = datetime.utcnow()
            
            # For historical seasons, check if data already exists
            # Only skip if we have a reasonable amount of data (at least 80% of expected)
            if not is_current_season:
                existing_check = await db.execute(
                    select(func.count()).select_from(Player).where(Player.bucket_id == bucket_id)
                )
                existing_count = existing_check.scalar()
                # Only skip if we have substantial data (at least 80% of total players)
                if existing_count > 0:
                    expected_minimum = int(len(players) * 0.8)  # At least 80% complete
                    if existing_count >= expected_minimum:
                        print(f"Data already exists for bucket {bucket_id} ({existing_count} records, expected ~{len(players)}). Skipping (historical season).")
                        return
                    else:
                        print(f"Partial data exists for bucket {bucket_id} ({existing_count}/{len(players)}). Continuing fetch to complete...")
            
            # Batch processing with resume capability
            delay = float(os.getenv("FETCH_DELAY", "0.05"))
            batch_size = int(os.getenv("BATCH_SIZE", "500"))  # Process 500 players per batch
            
            # Check which players we already have (for resume)
            # For season 11: check this week's snapshot. For historical: check any snapshot
            if is_current_season:
                # Season 11: Resume from this week's snapshot
                existing_players_query = await db.execute(
                    select(Player.player_id).where(
                        and_(
                            Player.bucket_id == bucket_id,
                            Player.snapshot_date == snapshot_date
                        )
                    ).distinct()
                )
            else:
                # Historical: Resume from any snapshot
                existing_players_query = await db.execute(
                    select(Player.player_id).where(Player.bucket_id == bucket_id).distinct()
                )
            existing_player_ids = set(existing_players_query.scalars().all())
            
            # Filter out already processed players
            players_to_process = [p for p in players if p.get("playerID") not in existing_player_ids]
            
            if len(players_to_process) == 0:
                print(f"All players already processed for bucket {bucket_id}. Skipping.")
                if bucket_id in fetch_status:
                    fetch_status[bucket_id]["status"] = "completed"
                return
            
            print(f"Resuming: {len(existing_player_ids)} already processed, {len(players_to_process)} remaining to fetch...")
            
            # Process in batches
            total_batches = (len(players_to_process) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                batch_start = batch_num * batch_size
                batch_end = min(batch_start + batch_size, len(players_to_process))
                batch = players_to_process[batch_start:batch_end]
                
                print(f"Processing batch {batch_num + 1}/{total_batches} (players {batch_start + 1}-{batch_end} of {len(players_to_process)})...")
                
                batch_processed = 0
                for player_data in batch:
                    # Check for control signals
                    if bucket_id in fetch_control:
                        control = fetch_control.get(bucket_id)
                        if control == 'stop':
                            print(f"Fetch stopped by user for bucket {bucket_id}")
                            if bucket_id in fetch_status:
                                fetch_status[bucket_id]["status"] = "stopped"
                            fetch_control.pop(bucket_id, None)
                            await db.commit()
                            return
                        elif control == 'pause':
                            print(f"Fetch paused for bucket {bucket_id}. Waiting for resume...")
                            while fetch_control.get(bucket_id) == 'pause':
                                await asyncio.sleep(1)
                            if fetch_control.get(bucket_id) == 'stop':
                                print(f"Fetch stopped while paused for bucket {bucket_id}")
                                if bucket_id in fetch_status:
                                    fetch_status[bucket_id]["status"] = "stopped"
                                fetch_control.pop(bucket_id, None)
                                await db.commit()
                                return
                            print(f"Fetch resumed for bucket {bucket_id}")
                    
                    player_id = player_data.get("playerID")
                    if not player_id or player_id in existing_player_ids:
                        continue
                    
                    try:
                        # Fetch detailed stats
                        stats_data = await fetch_player_stats(player_id, bucket_id)
                        
                        # Parse combined data with snapshot_date
                        player_record = parse_player_data(player_data, stats_data, bucket_id, snapshot_date)
                        
                        # Insert player record
                        new_player = Player(**player_record)
                        db.add(new_player)
                        existing_player_ids.add(player_id)  # Mark as processed
                        batch_processed += 1
                        
                        # Commit every 10 players within batch (with retry for SQLite locks)
                        if batch_processed % 10 == 0:
                            max_retries = 3
                            for retry in range(max_retries):
                                try:
                                    await db.commit()
                                    break
                                except Exception as commit_error:
                                    if "locked" in str(commit_error).lower() and retry < max_retries - 1:
                                        await db.rollback()
                                        await asyncio.sleep(0.1 * (retry + 1))  # Exponential backoff
                                        continue
                                    else:
                                        raise
                        
                        # Update status tracking
                        if bucket_id in fetch_status:
                            fetch_status[bucket_id]["processed_players"] = len(existing_player_ids)
                            fetch_status[bucket_id]["current_player"] = player_id
                        
                        await asyncio.sleep(delay)
                        
                    except Exception as e:
                        print(f"Error processing player {player_id}: {e}")
                        # Don't lose data - ensure we commit what we have before continuing
                        try:
                            await db.commit()
                        except:
                            await db.rollback()
                        continue
                
                # Commit batch (with retry for SQLite locks)
                max_retries = 5
                for retry in range(max_retries):
                    try:
                        await db.commit()
                        break
                    except Exception as commit_error:
                        error_msg = str(commit_error).lower()
                        if ("locked" in error_msg or "database is locked" in error_msg) and retry < max_retries - 1:
                            await db.rollback()
                            wait_time = 0.2 * (retry + 1)  # Exponential backoff: 0.2s, 0.4s, 0.6s, 0.8s, 1.0s
                            print(f"Database locked, retrying in {wait_time}s... (attempt {retry + 1}/{max_retries})")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            print(f"Commit failed after {max_retries} retries: {commit_error}")
                            raise
                
                total_processed = len(existing_player_ids)
                print(f"Batch {batch_num + 1}/{total_batches} complete. Total processed: {total_processed}/{len(players)}")
                
                # Log checkpoint
                if bucket_id in fetch_status:
                    fetch_status[bucket_id]["last_checkpoint"] = {
                        "batch": batch_num + 1,
                        "total_batches": total_batches,
                        "players_processed": total_processed,
                        "snapshot_date": snapshot_date.isoformat() if is_current_season else None,
                        "timestamp": datetime.utcnow().isoformat()
                    }
            
            # Final commit with retry
            max_retries = 5
            for retry in range(max_retries):
                try:
                    await db.commit()
                    break
                except Exception as commit_error:
                    error_msg = str(commit_error).lower()
                    if ("locked" in error_msg or "database is locked" in error_msg) and retry < max_retries - 1:
                        await db.rollback()
                        wait_time = 0.2 * (retry + 1)
                        print(f"Final commit locked, retrying in {wait_time}s... (attempt {retry + 1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        print(f"Final commit failed after {max_retries} retries: {commit_error}")
                        raise
            
            print(f"Finished updating data for bucket {bucket_id} (snapshot_date: {snapshot_date})")
            
            # Update status tracking - completed successfully
            if bucket_id in fetch_status:
                fetch_status[bucket_id]["status"] = "completed"
                fetch_status[bucket_id]["processed_players"] = len(players)
                fetch_status[bucket_id]["completed_at"] = datetime.utcnow().isoformat()
                fetch_status[bucket_id]["snapshot_date"] = snapshot_date.isoformat()
            
        except Exception as e:
            print(f"Error in update_player_data: {e}")
            import traceback
            traceback.print_exc()
            
            # CRITICAL: Try to save any data that was added before the error
            try:
                await db.commit()
                print("Successfully committed partial data before error")
            except Exception as commit_error:
                try:
                    await db.rollback()
                    print("Rolled back due to commit error")
                except:
                    pass
                print(f"Failed to commit partial data: {commit_error}")
            
            # Update status tracking - error
            if bucket_id in fetch_status:
                fetch_status[bucket_id]["status"] = "error"
                fetch_status[bucket_id]["error"] = str(e)
                fetch_status[bucket_id]["error_at"] = datetime.utcnow().isoformat()

def get_latest_snapshot_query(bucket_id: int):
    """Create a subquery to get the latest snapshot for each player in a bucket."""
    # Subquery: Get the latest snapshot_date for each player_id in this bucket
    latest_dates = select(
        Player.player_id,
        func.max(Player.snapshot_date).label('max_date')
    ).where(
        Player.bucket_id == bucket_id
    ).group_by(Player.player_id).subquery()
    
    # Main query: Join with Player to get full records matching latest dates
    return select(Player).join(
        latest_dates,
        and_(
            Player.player_id == latest_dates.c.player_id,
            Player.bucket_id == bucket_id,
            Player.snapshot_date == latest_dates.c.max_date
        )
    )

@app.get("/api/players", response_model=PlayerListResponse)
async def get_players(
    bucket_id: int = Query(11, description="Season bucket ID"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    search: Optional[str] = Query(None, description="Search by name"),
    state: Optional[str] = Query(None, description="Filter by state"),
    skill_level: Optional[str] = Query(None, description="Filter by skill level"),
    conference_id: Optional[int] = Query(None, description="Filter by conference ID"),
    sort_by: str = Query("rank", description="Sort field"),
    sort_order: str = Query("asc", description="Sort order (asc/desc)"),
    db: AsyncSession = Depends(get_db)
):
    """Get players with filtering and sorting. Returns latest snapshot per player."""
    query = get_latest_snapshot_query(bucket_id)
    
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
    if conference_id:
        query = query.where(Player.conference_id == conference_id)
    
    # Apply sorting - filter out NULL values for numeric columns when sorting
    # Numeric columns that should exclude NULL when sorting
    numeric_columns = ['pts_per_rnd', 'dpr', 'player_cpi', 'win_pct', 'total_games', 'rounds_total', 'overall_total']
    
    if sort_by in numeric_columns:
        # Filter out NULL values for the column being sorted
        sort_column = getattr(Player, sort_by, Player.rank)
        query = query.where(sort_column.isnot(None))
    
    # Get total count (after NULL filtering)
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar()
    
    sort_column = getattr(Player, sort_by, Player.rank)
    if sort_order.lower() == "desc":
        query = query.order_by(desc(sort_column))
    else:
        query = query.order_by(asc(sort_column))
    
    # Apply pagination
    offset = (page - 1) * page_size
    query = query.offset(offset).limit(page_size)
    
    result = await db.execute(query)
    players = result.scalars().all()
    
    return PlayerListResponse(
        players=[PlayerResponse.model_validate(p) for p in players],
        total=total,
        page=page,
        page_size=page_size,
        bucket_id=bucket_id
    )

@app.get("/api/players/multi-season", response_model=PlayerListResponse)
async def get_players_multi_season(
    bucket_ids: List[int] = Query(..., description="List of bucket IDs (seasons) to filter"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    search: Optional[str] = Query(None, description="Search by name"),
    state: Optional[str] = Query(None, description="Filter by state"),
    skill_level: Optional[str] = Query(None, description="Filter by skill level"),
    sort_by: str = Query("rounds_total", description="Sort field"),
    sort_order: str = Query("desc", description="Sort order (asc/desc)"),
    db: AsyncSession = Depends(get_db)
):
    """Get players who played in ALL specified seasons. Aggregates stats across seasons."""
    try:
        if not bucket_ids or len(bucket_ids) == 0:
            raise HTTPException(status_code=400, detail="At least one bucket_id must be provided")
        
        # For each bucket_id, get the set of player_ids from latest snapshots
        player_sets = []
        for bucket_id in bucket_ids:
            latest_dates = select(
                Player.player_id,
                func.max(Player.snapshot_date).label('max_date')
            ).where(Player.bucket_id == bucket_id).group_by(Player.player_id).subquery()
            
            players_in_bucket = select(Player.player_id).join(
                latest_dates,
                and_(
                    Player.player_id == latest_dates.c.player_id,
                    Player.bucket_id == bucket_id,
                    Player.snapshot_date == latest_dates.c.max_date
                )
            ).distinct()
            result = await db.execute(players_in_bucket)
            player_sets.append(set(result.scalars().all()))
        
        # Intersection: players who exist in ALL buckets
        if not player_sets:
            common_player_ids = set()
        else:
            common_player_ids = player_sets[0]
            for player_set in player_sets[1:]:
                common_player_ids &= player_set
        
        if not common_player_ids:
            return PlayerListResponse(
                players=[],
                total=0,
                page=page,
                page_size=page_size,
                bucket_id=bucket_ids[0] if bucket_ids else None
            )
        
        # Step 2: Bulk fetch latest snapshots for all players across all seasons
        # This is much more efficient than querying each player individually
        aggregated_players = []
        
        # Get all latest snapshot dates per player per bucket in one query
        latest_dates_subq = select(
            Player.player_id,
            Player.bucket_id,
            func.max(Player.snapshot_date).label('max_date')
        ).where(
            and_(
                Player.player_id.in_(common_player_ids),
                Player.bucket_id.in_(bucket_ids)
            )
        ).group_by(Player.player_id, Player.bucket_id).subquery()
        
        # Get all player records for latest snapshots in bulk
        players_query = select(Player).join(
            latest_dates_subq,
            and_(
                Player.player_id == latest_dates_subq.c.player_id,
                Player.bucket_id == latest_dates_subq.c.bucket_id,
                Player.snapshot_date == latest_dates_subq.c.max_date
            )
        )
        players_result = await db.execute(players_query)
        all_players = players_result.scalars().all()
        
        # Group players by player_id
        players_by_id = {}
        for player in all_players:
            if player.player_id not in players_by_id:
                players_by_id[player.player_id] = {}
            players_by_id[player.player_id][player.bucket_id] = player
        
        # Only process players who have data in ALL seasons
        for player_id in common_player_ids:
            if player_id not in players_by_id:
                continue
            
            player_seasons = players_by_id[player_id]
            if len(player_seasons) == len(bucket_ids):  # Player has data in ALL seasons
                # Aggregate stats
                first_player = list(player_seasons.values())[0]
                
                # Sum stats that should be aggregated (rounds, games, etc.)
                total_rounds = sum(p.rounds_total or 0 for p in player_seasons.values())
                total_games = sum(p.total_games or 0 for p in player_seasons.values())
                total_wins = sum(p.total_wins or 0 for p in player_seasons.values())
                total_losses = sum(p.total_losses or 0 for p in player_seasons.values())
                
                # Average stats (PPR, DPR, CPI) - weighted average would be better but simple avg for now
                avg_ppr = sum(p.pts_per_rnd or 0 for p in player_seasons.values()) / len(player_seasons) if player_seasons else None
                avg_dpr = sum(p.dpr or 0 for p in player_seasons.values()) / len(player_seasons) if player_seasons else None
                avg_cpi = sum(p.player_cpi or 0 for p in player_seasons.values()) / len(player_seasons) if player_seasons else None
                
                # Calculate win percentage from totals
                win_pct = (total_wins / (total_wins + total_losses) * 100) if (total_wins + total_losses) > 0 else None
                
                # Create aggregated player dict
                aggregated_player = {
                    "id": first_player.id,
                    "player_id": first_player.player_id,
                    "bucket_id": bucket_ids[0],  # Use first bucket for display
                    "first_name": first_player.first_name,
                    "last_name": first_player.last_name,
                    "state": first_player.state,
                    "skill_level": first_player.skill_level,
                    "rank": first_player.rank,  # Use most recent rank
                    "pts_per_rnd": avg_ppr,
                    "dpr": avg_dpr,
                    "player_cpi": avg_cpi,
                    "win_pct": win_pct,
                    "total_games": total_games,
                    "rounds_total": total_rounds,
                    "overall_total": sum(p.overall_total or 0 for p in player_seasons.values()),
                    "snapshot_date": datetime.utcnow(),
                }
                aggregated_players.append(aggregated_player)
        
        # Step 3: Apply filters
        filtered_players = aggregated_players
        if search:
            search_lower = search.lower()
            filtered_players = [p for p in filtered_players if 
                               search_lower in (p.get("first_name", "") or "").lower() or
                               search_lower in (p.get("last_name", "") or "").lower()]
        if state:
            filtered_players = [p for p in filtered_players if p.get("state") == state]
        if skill_level:
            filtered_players = [p for p in filtered_players if p.get("skill_level") == skill_level]
        
        # Step 4: Sort
        numeric_columns = ['pts_per_rnd', 'dpr', 'player_cpi', 'win_pct', 'total_games', 'rounds_total', 'overall_total']
        if sort_by in numeric_columns:
            # Filter out NULL values
            filtered_players = [p for p in filtered_players if p.get(sort_by) is not None]
        
        if sort_by == "rounds_total":
            filtered_players.sort(key=lambda p: p.get("rounds_total", 0), reverse=(sort_order.lower() == "desc"))
        elif sort_by == "total_games":
            filtered_players.sort(key=lambda p: p.get("total_games", 0), reverse=(sort_order.lower() == "desc"))
        elif sort_by == "pts_per_rnd":
            filtered_players.sort(key=lambda p: p.get("pts_per_rnd", 0), reverse=(sort_order.lower() == "desc"))
        elif sort_by == "dpr":
            filtered_players.sort(key=lambda p: p.get("dpr", 0), reverse=(sort_order.lower() == "desc"))
        elif sort_by == "player_cpi":
            filtered_players.sort(key=lambda p: p.get("player_cpi", 0), reverse=(sort_order.lower() == "desc"))
        elif sort_by == "win_pct":
            filtered_players.sort(key=lambda p: p.get("win_pct", 0), reverse=(sort_order.lower() == "desc"))
        elif sort_by == "overall_total":
            filtered_players.sort(key=lambda p: p.get("overall_total", 0), reverse=(sort_order.lower() == "desc"))
        elif sort_by == "rank":
            filtered_players.sort(key=lambda p: p.get("rank", 999999), reverse=(sort_order.lower() == "desc"))
        
        # Step 5: Paginate
        total = len(filtered_players)
        offset = (page - 1) * page_size
        paginated_players = filtered_players[offset:offset + page_size]
        
        # Convert to PlayerResponse format
        from models import PlayerResponse
        player_responses = []
        for p_dict in paginated_players:
            player_response = PlayerResponse(
                id=p_dict.get("id", 0),
                player_id=p_dict.get("player_id", 0),
                bucket_id=p_dict.get("bucket_id", bucket_ids[0]),
                first_name=p_dict.get("first_name", ""),
                last_name=p_dict.get("last_name", ""),
                state=p_dict.get("state"),
                skill_level=p_dict.get("skill_level"),
                rank=p_dict.get("rank"),
                pts_per_rnd=p_dict.get("pts_per_rnd"),
                dpr=p_dict.get("dpr"),
                player_cpi=p_dict.get("player_cpi"),
                win_pct=p_dict.get("win_pct"),
                total_games=p_dict.get("total_games", 0),
                rounds_total=p_dict.get("rounds_total", 0),
                overall_total=p_dict.get("overall_total"),
                country_code=None,
                country_name=None,
                conference_id=None,
                total_pts=None,
                opponent_pts_per_rnd=None,
                opponent_pts_total=None,
                four_bagger_pct=None,
                bags_in_pct=None,
                bags_on_pct=None,
                bags_off_pct=None,
                total_wins=None,
                total_losses=None,
                membership_name=None,
                last_updated=datetime.utcnow()
            )
            player_responses.append(player_response)
    
        return PlayerListResponse(
            players=player_responses,
            total=total,
            page=page,
            page_size=page_size,
            bucket_id=bucket_ids[0] if bucket_ids else None
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in get_players_multi_season: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error processing multi-season request: {str(e)}")

@app.get("/api/players/{player_id}", response_model=PlayerResponse)
async def get_player(
    player_id: int,
    bucket_id: int = Query(11, description="Season bucket ID"),
    db: AsyncSession = Depends(get_db)
):
    """Get a specific player by ID. Returns latest snapshot."""
    # Get latest snapshot for this player
    latest_dates = select(
        func.max(Player.snapshot_date).label('max_date')
    ).where(
        and_(Player.player_id == player_id, Player.bucket_id == bucket_id)
    ).subquery()
    
    result = await db.execute(
        select(Player).join(
            latest_dates,
            and_(
                Player.player_id == player_id,
                Player.bucket_id == bucket_id,
                Player.snapshot_date == latest_dates.c.max_date
            )
        )
    )
    player = result.scalar_one_or_none()
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    return PlayerResponse.model_validate(player)

@app.get("/api/players/{player_id}/comparison")
async def get_player_comparison(
    player_id: int,
    bucket_ids: List[int] = Query(..., description="List of bucket IDs to compare"),
    db: AsyncSession = Depends(get_db)
):
    """Get a player's stats across multiple seasons. Returns latest snapshot per season."""
    players = []
    for bucket_id in bucket_ids:
        # Get latest snapshot for this player in this bucket
        latest_dates = select(
            func.max(Player.snapshot_date).label('max_date')
        ).where(
            and_(Player.player_id == player_id, Player.bucket_id == bucket_id)
        ).subquery()
        
        result = await db.execute(
            select(Player).join(
                latest_dates,
                and_(
                    Player.player_id == player_id,
                    Player.bucket_id == bucket_id,
                    Player.snapshot_date == latest_dates.c.max_date
                )
            )
        )
        player = result.scalar_one_or_none()
        if player:
            players.append(PlayerResponse.model_validate(player))
    
    if not players:
        raise HTTPException(status_code=404, detail="Player not found in any of the specified seasons")
    
    return {
        "player_id": player_id,
        "first_name": players[0].first_name,
        "last_name": players[0].last_name,
        "seasons": [
            {
                "bucket_id": p.bucket_id,
                "rank": p.rank,
                "pts_per_rnd": p.pts_per_rnd,
                "dpr": p.dpr,
                "player_cpi": p.player_cpi,
                "win_pct": p.win_pct,
                "total_games": p.total_games,
            }
            for p in players
        ]
    }

@app.get("/api/stats/filters")
async def get_filter_options(
    bucket_id: int = Query(11, description="Season bucket ID"),
    db: AsyncSession = Depends(get_db)
):
    """Get available filter options. Uses latest snapshot per player."""
    # Get latest snapshot date per player
    latest_dates = select(
        Player.player_id,
        func.max(Player.snapshot_date).label('max_date')
    ).where(Player.bucket_id == bucket_id).group_by(Player.player_id).subquery()
    
    # Get distinct states from latest snapshots
    states_query = select(Player.state).join(
        latest_dates,
        and_(
            Player.player_id == latest_dates.c.player_id,
            Player.bucket_id == bucket_id,
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
            Player.bucket_id == bucket_id,
            Player.snapshot_date == latest_dates.c.max_date,
            Player.skill_level.isnot(None)
        )
    ).distinct()
    skills_result = await db.execute(skills_query)
    skill_levels = sorted([s for s in skills_result.scalars().all() if s])
    
    # Get distinct conference IDs
    conf_query = select(Player.conference_id).join(
        latest_dates,
        and_(
            Player.player_id == latest_dates.c.player_id,
            Player.bucket_id == bucket_id,
            Player.snapshot_date == latest_dates.c.max_date,
            Player.conference_id.isnot(None)
        )
    ).distinct()
    conf_result = await db.execute(conf_query)
    conference_ids = sorted([c for c in conf_result.scalars().all() if c])
    
    # Get available bucket IDs (seasons) from database
    buckets_query = select(Player.bucket_id).distinct()
    buckets_result = await db.execute(buckets_query)
    available_buckets = sorted([b for b in buckets_result.scalars().all()], reverse=True)
    
    return {
        "states": states,
        "skill_levels": skill_levels,
        "conference_ids": conference_ids,
        "available_seasons": available_buckets
    }

@app.post("/api/index-events/{bucket_id}")
async def index_events_for_season(
    bucket_id: int,
    skip_processed: bool = Query(True, description="Skip players who already have events indexed"),
    background_tasks: BackgroundTasks = None,
    db: AsyncSession = Depends(get_db)
):
    """Index all events for a season by going through players."""
    from event_indexer import index_season_events_with_status
    import asyncio
    
    # Initialize status tracking
    if bucket_id not in event_indexing_status:
        event_indexing_status[bucket_id] = {
            "status": "running",
            "bucket_id": bucket_id,
            "started_at": datetime.utcnow().isoformat(),
            "total_players": None,
            "processed_players": 0,
            "current_player": None,
            "new_events_indexed": 0,
            "total_events": 0,
            "skipped_players": 0,
            "error": None
        }
    
    async def index_task():
        try:
            await index_season_events_with_status(bucket_id=bucket_id, skip_processed=skip_processed)
            print(f"Completed indexing events for season {bucket_id}")
        except Exception as e:
            print(f"Error indexing events for season {bucket_id}: {e}")
            import traceback
            traceback.print_exc()
    
    if background_tasks:
        background_tasks.add_task(index_task)
    else:
        # Run as background task if no background_tasks available
        asyncio.create_task(index_task())
    
    return {"message": f"Started indexing events for season {bucket_id}", "bucket_id": bucket_id, "skip_processed": skip_processed}

@app.get("/api/event-indexing-status/{bucket_id}")
async def get_event_indexing_status(bucket_id: int):
    """Get status of event indexing for a season."""
    from event_indexer import get_event_indexing_status as get_status
    return await get_status(bucket_id)

@app.post("/api/reindex-event/{event_id}")
async def reindex_event(
    event_id: int,
    bucket_id: int = Query(11, description="Season bucket ID"),
    background_tasks: BackgroundTasks = None,
    db: AsyncSession = Depends(get_db)
):
    """Re-index a specific event (forces re-indexing even if already indexed)."""
    from event_indexer import index_event
    from database import async_session_maker
    
    async def reindex_task():
        async with async_session_maker() as db_task:
            try:
                success = await index_event(event_id, bucket_id, db_task, force_reindex=True)
                await db_task.commit()
                if success:
                    print(f"Successfully re-indexed event {event_id}")
                else:
                    print(f"Failed to re-index event {event_id}")
            except Exception as e:
                print(f"Error re-indexing event {event_id}: {e}")
                import traceback
                traceback.print_exc()
                await db_task.rollback()
    
    if background_tasks:
        background_tasks.add_task(reindex_task)
        return {"message": f"Started re-indexing event {event_id}", "event_id": event_id}
    else:
        # Run synchronously if no background tasks
        import asyncio
        asyncio.create_task(reindex_task())
        return {"message": f"Started re-indexing event {event_id}", "event_id": event_id}

@app.post("/api/index-games/event/{event_id}")
async def index_games_for_event(
    event_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Index games for a specific event with status tracking (regional, open, signature, national, and local finals only)."""
    from game_indexer import discover_and_index_event_games
    from database import async_session_maker, Event
    
    # Get event info to check bucket_id and event type
    event_query = select(Event).where(Event.event_id == event_id)
    event_result = await db.execute(event_query)
    event = event_result.scalar_one_or_none()
    
    if not event:
        raise HTTPException(status_code=404, detail=f"Event {event_id} not found")
    
    # Verify event type is allowed (regional, open, signature, national, or local final)
    event_type = (event.event_type or "").lower()
    is_allowed_type = event_type in ["regional", "open", "signature", "national", "r", "o", "s", "n"]
    is_local_final = event_type in ["local", "l"] and event.event_name and "Final" in event.event_name
    
    if not (is_allowed_type or is_local_final):
        raise HTTPException(
            status_code=400, 
            detail=f"Event {event_id} is type '{event.event_type}' and is not a final. Only regional, open, signature, national events, or local finals can be indexed."
        )
    
    bucket_id = event.bucket_id or 11
    
    # Initialize status tracking
    status_key = f"event_{event_id}"
    game_indexing_status[status_key] = {
        "status": "running",
        "event_id": event_id,
        "bucket_id": bucket_id,
        "started_at": datetime.utcnow().isoformat(),
        "total_games": None,
        "processed_games": 0,
        "new_games_indexed": 0,
        "current_match": None,
        "error": None
    }
    
    async def index_task():
        async with async_session_maker() as db_task:
            try:
                # Update status: starting
                if status_key in game_indexing_status:
                    game_indexing_status[status_key]["status"] = "running"
                
                # Create a status update callback
                def update_status(**kwargs):
                    if status_key in game_indexing_status:
                        game_indexing_status[status_key].update(kwargs)
                
                # Call with status callback
                from game_indexer import discover_and_index_event_games_with_status
                new_games = await discover_and_index_event_games_with_status(
                    event_id, db_task, status_callback=update_status, skip_if_complete=False
                )
                await db_task.commit()
                
                # Update status: completed
                if status_key in game_indexing_status:
                    # Get final counts
                    from sqlalchemy import func
                    from database import EventGame
                    games_count_query = select(func.count()).select_from(EventGame).where(EventGame.event_id == event_id)
                    games_count_result = await db_task.execute(games_count_query)
                    total_games = games_count_result.scalar() or 0
                    
                    game_indexing_status[status_key].update({
                        "status": "completed",
                        "new_games_indexed": new_games,
                        "total_games": total_games,
                        "processed_games": total_games,
                        "completed_at": datetime.utcnow().isoformat()
                    })
                
                print(f"Indexed {new_games} new games for event {event_id}")
            except Exception as e:
                print(f"Error indexing games for event {event_id}: {e}")
                import traceback
                traceback.print_exc()
                await db_task.rollback()
                
                # Update status: error
                if status_key in game_indexing_status:
                    game_indexing_status[status_key].update({
                        "status": "error",
                        "error": str(e),
                        "error_at": datetime.utcnow().isoformat()
                    })
    
    background_tasks.add_task(index_task)
    return {
        "status": "started",
        "event_id": event_id,
        "message": f"Started indexing games for event {event_id}",
        "status_key": status_key
    }

@app.get("/api/game-indexing-status/event/{event_id}")
async def get_game_indexing_status(event_id: int):
    """Get status of game indexing for a specific event."""
    status_key = f"event_{event_id}"
    if status_key in game_indexing_status:
        return game_indexing_status[status_key]
    return {
        "status": "not_running",
        "event_id": event_id,
        "message": "No active indexing for this event"
    }

@app.get("/api/game-indexing-status")
async def get_all_game_indexing_status():
    """Get status of all game indexing operations."""
    return {
        "active_indexing": len([s for s in game_indexing_status.values() if s.get("status") == "running"]),
        "statuses": game_indexing_status
    }

# ============================================================================
# LOCAL TESTING ENDPOINTS - Limited data for testing
# ============================================================================

@app.post("/api/local/fetch-data/{bucket_id}")
async def fetch_data_local(bucket_id: int, background_tasks: BackgroundTasks):
    """Trigger data fetch for a specific bucket/season (LOCAL: Limited to 100 players)."""
    # Initialize status tracking
    fetch_status[bucket_id] = {
        "status": "running",
        "bucket_id": bucket_id,
        "started_at": datetime.utcnow().isoformat(),
        "total_players": None,
        "processed_players": 0,
        "current_player": None,
        "error": None,
        "local_mode": True,
        "max_players": 100
    }
    background_tasks.add_task(update_player_data_local, bucket_id)
    return {"status": "started", "bucket_id": bucket_id, "message": "Data fetch started in background (LOCAL: Max 100 players)"}

@app.get("/api/local/fetch-status")
async def get_fetch_status_local():
    """Get status of all ongoing fetches (LOCAL mode)."""
    return await get_fetch_status()

@app.get("/api/local/fetch-status/{bucket_id}")
async def get_fetch_status_bucket_local(bucket_id: int):
    """Get status of a specific bucket fetch (LOCAL mode)."""
    return await get_fetch_status_bucket(bucket_id)

@app.post("/api/local/fetch-control/{bucket_id}/pause")
async def pause_fetch_local(bucket_id: int, username: str = Depends(verify_admin)):
    """Pause a running fetch (LOCAL mode)."""
    return await pause_fetch(bucket_id, username)

@app.post("/api/local/fetch-control/{bucket_id}/resume")
async def resume_fetch_local(bucket_id: int, username: str = Depends(verify_admin)):
    """Resume a paused fetch (LOCAL mode)."""
    return await resume_fetch(bucket_id, username)

@app.post("/api/local/fetch-control/{bucket_id}/stop")
async def stop_fetch_local(bucket_id: int, username: str = Depends(verify_admin)):
    """Stop a running fetch (LOCAL mode)."""
    return await stop_fetch(bucket_id, username)

@app.get("/api/local/db-health")
async def db_health_check_local(username: str = Depends(verify_admin)):
    """Check database health and integrity (LOCAL mode)."""
    return await db_health_check(username)

@app.post("/api/local/index-event/{event_id}")
async def index_single_event_local(
    event_id: int,
    bucket_id: int = Query(11, description="Season/bucket ID"),
    background_tasks: BackgroundTasks = None,
    db: AsyncSession = Depends(get_db)
):
    """Index a single event directly (for testing)."""
    from event_indexer import index_event
    from database import async_session_maker
    
    async def index_task():
        try:
            async with async_session_maker() as db_task:
                success = await index_event(event_id, bucket_id, db_task, force_reindex=False)
                await db_task.commit()
                if success:
                    print(f"Successfully indexed event {event_id}")
                else:
                    print(f"Failed to index event {event_id}")
        except Exception as e:
            print(f"Error indexing event {event_id}: {e}")
            import traceback
            traceback.print_exc()
    
    if background_tasks:
        background_tasks.add_task(index_task)
    else:
        import asyncio
        asyncio.create_task(index_task())
    
    return {
        "message": f"Started indexing event {event_id}",
        "event_id": event_id,
        "bucket_id": bucket_id
    }

@app.post("/api/local/index-events/{bucket_id}")
async def index_events_for_season_local(
    bucket_id: int,
    skip_processed: bool = Query(True, description="Skip players who already have events indexed"),
    background_tasks: BackgroundTasks = None,
    db: AsyncSession = Depends(get_db)
):
    """Index events for a season (LOCAL: 100 players max, Open #2 Winter Haven events only)."""
    from event_indexer import index_season_events_with_status_local
    import asyncio
    
    async def index_task():
        try:
            await index_season_events_with_status_local(bucket_id=bucket_id, skip_processed=skip_processed)
            print(f"Completed indexing events for season {bucket_id} (LOCAL mode)")
        except Exception as e:
            print(f"Error indexing events for season {bucket_id}: {e}")
            import traceback
            traceback.print_exc()
    
    if background_tasks:
        background_tasks.add_task(index_task)
    else:
        asyncio.create_task(index_task())
    
    return {
        "message": f"Started indexing events for season {bucket_id} (LOCAL: 100 players max, Open #2 Winter Haven events only)",
        "bucket_id": bucket_id,
        "skip_processed": skip_processed,
        "local_mode": True
    }

@app.post("/api/cache-index-stats")
async def cache_index_stats(
    bucket_id: Optional[int] = Query(None, description="Optional: Filter by season/bucket ID"),
    background_tasks: BackgroundTasks = None,
    db: AsyncSession = Depends(get_db)
):
    """Index/cache aggregated stats for all grouped events.
    
    This pre-computes and stores aggregated stats to dramatically improve page load times.
    Run this after indexing games to build the cache. Can be run independently without re-indexing games.
    """
    from database import Event, async_session_maker
    from stats_calculator import calculate_event_aggregated_stats, store_aggregated_stats
    from sqlalchemy import func, and_, or_
    import re
    import asyncio
    
    # Get all events (optionally filtered by bucket_id)
    events_query = select(
        Event.event_id,
        Event.event_name,
        Event.base_event_name,
        Event.bracket_name,
        Event.bucket_id
    ).where(Event.base_event_name.isnot(None))
    
    if bucket_id:
        events_query = events_query.where(Event.bucket_id == bucket_id)
    
    result = await db.execute(events_query)
    all_events = result.all()
    
    if not all_events:
        return {
            "status": "error",
            "message": "No events found",
            "bucket_id": bucket_id
        }
    
    # Group events by base_event_name and bracket_type
    grouped_events = {}
    for event in all_events:
        base_name = event[2] or ""
        bracket_name = event[3] or ""
        
        if not base_name:
            continue
        
        # Extract bracket type
        bracket_type = bracket_name
        if bracket_name:
            bracket_type = re.sub(r'\s*-\s*', ' ', bracket_name)
            bracket_type = re.sub(r'\s*Final\s+\d+\s*$', '', bracket_type, flags=re.IGNORECASE)
            bracket_type = re.sub(r'\s*Final\s*$', '', bracket_type, flags=re.IGNORECASE)
            bracket_type = re.sub(r'\s*Bracket\s+[A-Z]\s*$', '', bracket_type, flags=re.IGNORECASE)
            bracket_type = re.sub(r'\s*Bracket\s*$', '', bracket_type, flags=re.IGNORECASE)
            bracket_type = re.sub(r'\s+', ' ', bracket_type).strip()
        
        group_key = f"{base_name}|||{bracket_type}" if bracket_type else base_name
        
        if group_key not in grouped_events:
            grouped_events[group_key] = {
                "base_event_name": base_name,
                "bracket_type": bracket_type,
                "event_ids": []
            }
        
        grouped_events[group_key]["event_ids"].append(event[0])
    
    total_groups = len([g for g in grouped_events.values() if len(g["event_ids"]) > 1])
    total_brackets = len(all_events)
    
    # Initialize status tracking and logs
    status_key = f"cache_{bucket_id}" if bucket_id else "cache_all"
    cache_indexing_status[status_key] = {
        "status": "running",
        "bucket_id": bucket_id,
        "started_at": datetime.utcnow().isoformat(),
        "total_brackets": total_brackets,
        "total_groups": total_groups,
        "processed_brackets": 0,
        "processed_groups": 0,
        "current_item": None,
        "error": None
    }
    
    cache_indexing_logs[status_key] = []
    
    def add_log(message: str):
        timestamp = datetime.utcnow().isoformat()
        log_entry = f"[{timestamp}] {message}"
        cache_indexing_logs[status_key].append(log_entry)
        print(log_entry)  # Also print to console
        # Keep only last 1000 log entries
        if len(cache_indexing_logs[status_key]) > 1000:
            cache_indexing_logs[status_key] = cache_indexing_logs[status_key][-1000:]
    
    add_log("Cache indexing started")
    add_log(f"Found {total_brackets} brackets and {total_groups} groups to process")
    
    async def cache_task():
        async with async_session_maker() as db_task:
            try:
                from stats_calculator import calculate_bracket_stats
                processed_groups = 0
                processed_brackets = 0
                
                if status_key in cache_indexing_status:
                    cache_indexing_status[status_key]["status"] = "running"
                
                # First, cache individual bracket stats
                for idx, event in enumerate(all_events, 1):
                    event_id = event[0]
                    event_name = event[1]
                    
                    if status_key in cache_indexing_status:
                        cache_indexing_status[status_key]["current_item"] = f"Bracket {idx}/{total_brackets}: {event_name}"
                    
                    add_log(f"Calculating bracket stats for event {event_id} ({event_name})...")
                    
                    # Calculate bracket-specific stats
                    bracket_stats = await calculate_bracket_stats(event_id, db_task)
                    
                    # Store bracket stats
                    await store_aggregated_stats(
                        group_key=f"event_{event_id}",
                        group_type="event",
                        event_ids=[event_id],
                        base_event_name=event[2] or "",
                        bracket_type=event[3] or "",
                        stats_data=bracket_stats,
                        db=db_task
                    )
                    
                    processed_brackets += 1
                    if status_key in cache_indexing_status:
                        cache_indexing_status[status_key]["processed_brackets"] = processed_brackets
                    
                    add_log(f"✓ Cached bracket stats for event {event_id} ({bracket_stats['total_players']} players, {bracket_stats['total_games']} games)")
                
                # Then, cache grouped event stats
                group_idx = 0
                for group_key, group_data in grouped_events.items():
                    event_ids = group_data["event_ids"]
                    if len(event_ids) <= 1:
                        continue  # Skip single events (only cache grouped events)
                    
                    group_idx += 1
                    if status_key in cache_indexing_status:
                        cache_indexing_status[status_key]["current_item"] = f"Group {group_idx}/{total_groups}: {group_data['base_event_name']} {group_data['bracket_type']}"
                    
                    add_log(f"Calculating grouped stats for {group_data['base_event_name']} {group_data['bracket_type']} ({len(event_ids)} events)...")
                    
                    # Calculate stats
                    stats_data = await calculate_event_aggregated_stats(
                        event_ids=event_ids,
                        base_event_name=group_data["base_event_name"],
                        bracket_type=group_data["bracket_type"],
                        db=db_task,
                        group_type="grouped"
                    )
                    
                    # Store stats
                    await store_aggregated_stats(
                        group_key=f"grouped_{group_data['base_event_name']}_{group_data['bracket_type']}",
                        group_type="grouped",
                        event_ids=event_ids,
                        base_event_name=group_data["base_event_name"],
                        bracket_type=group_data["bracket_type"],
                        stats_data=stats_data,
                        db=db_task
                    )
                    
                    processed_groups += 1
                    if status_key in cache_indexing_status:
                        cache_indexing_status[status_key]["processed_groups"] = processed_groups
                    
                    add_log(f"✓ Cached grouped stats for {group_data['base_event_name']} {group_data['bracket_type']} ({len(event_ids)} events, {stats_data['total_players']} players, {stats_data['total_games']} games)")
                
                if status_key in cache_indexing_status:
                    cache_indexing_status[status_key]["status"] = "completed"
                    cache_indexing_status[status_key]["current_item"] = None
                
                add_log(f"✓ Completed caching: {processed_brackets} brackets, {processed_groups}/{total_groups} groups")
                cache_indexing_logs[status_key].append("=== CACHE INDEXING COMPLETE ===")
            except Exception as e:
                error_msg = f"✗ Error caching stats: {str(e)}"
                add_log(error_msg)
                if status_key in cache_indexing_status:
                    cache_indexing_status[status_key]["status"] = "error"
                    cache_indexing_status[status_key]["error"] = str(e)
                import traceback
                traceback.print_exc()
                cache_indexing_logs[status_key].append(f"=== ERROR: {error_msg} ===")
    
    if background_tasks:
        background_tasks.add_task(cache_task)
    else:
        asyncio.create_task(cache_task())
    
    return {
        "status": "started",
        "message": f"Started caching stats for {total_brackets} brackets and {total_groups} groups",
        "brackets_count": total_brackets,
        "groups_count": total_groups,
        "bucket_id": bucket_id,
        "status_key": status_key
    }

@app.get("/api/cache-indexing-status")
async def get_cache_indexing_status(bucket_id: Optional[int] = Query(None)):
    """Get status of cache indexing operations."""
    status_key = f"cache_{bucket_id}" if bucket_id else "cache_all"
    
    if status_key in cache_indexing_status:
        return cache_indexing_status[status_key]
    
    # Also check for any active cache indexing
    active_statuses = {k: v for k, v in cache_indexing_status.items() if v.get("status") == "running"}
    if active_statuses:
        # Return the most recent one
        latest = max(active_statuses.items(), key=lambda x: x[1].get("started_at", ""))
        return latest[1]
    
    return {
        "status": "not_running",
        "message": "No active cache indexing"
    }

@app.get("/api/cache-indexing-logs/{status_key}")
async def get_cache_indexing_logs(status_key: str):
    """Get logs for cache indexing operation."""
    logs = cache_indexing_logs.get(status_key, [])
    return {
        "status_key": status_key,
        "logs": logs,
        "count": len(logs)
    }

@app.post("/api/index-games-for-all-events")
async def index_games_for_all_events(
    bucket_id: Optional[int] = Query(None, description="Optional: Filter by season/bucket ID"),
    background_tasks: BackgroundTasks = None,
    db: AsyncSession = Depends(get_db)
):
    """Index games for all existing events (regional, open, signature, national, and local finals only)."""
    import asyncio
    from database import Event
    from game_indexer import discover_and_index_event_games_with_status
    from database import async_session_maker
    
    # Get all events, filtering by event type
    # Only index: regional, open, signature, national
    # Also allow local events if they're finals (contain "Final" in name)
    events_query = select(Event.event_id, Event.event_name).where(
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
    if bucket_id is not None:
        events_query = events_query.where(Event.bucket_id == bucket_id)
    
    result = await db.execute(events_query)
    events = result.all()
    
    if not events:
        return {
            "status": "error",
            "message": f"No events found" + (f" for season {bucket_id}" if bucket_id else ""),
            "events_count": 0
        }
    
    # Initialize log buffer
    log_key = "all_events"
    game_indexing_logs[log_key] = []
    
    def add_log(message: str):
        timestamp = datetime.utcnow().isoformat()
        log_entry = f"[{timestamp}] {message}"
        if log_key not in game_indexing_logs:
            game_indexing_logs[log_key] = []
        game_indexing_logs[log_key].append(log_entry)
        print(log_entry)  # Also print to console
        # Keep only last 1000 log entries
        if len(game_indexing_logs[log_key]) > 1000:
            game_indexing_logs[log_key] = game_indexing_logs[log_key][-1000:]
    
    # Add initial log entry
    add_log("Logging system initialized")
    
    async def index_task():
        async with async_session_maker() as db_task:
            try:
                add_log(f"Starting game indexing for {len(events)} events")
                total_new_games = 0
                for idx, (event_id, event_name) in enumerate(events, 1):
                    add_log(f"Processing event {idx}/{len(events)}: {event_id} ({event_name})")
                    
                    new_games = await discover_and_index_event_games_with_status(
                        event_id, db_task, status_callback=None, skip_if_complete=False
                    )
                    total_new_games += new_games
                    
                    add_log(f"Event {event_id}: {new_games} new games indexed (total so far: {total_new_games})")
                    
                    # Small delay between events
                    await asyncio.sleep(0.1)
                
                add_log(f"✓ Completed indexing games for {len(events)} events: {total_new_games} total new games")
                game_indexing_logs[log_key].append("=== INDEXING COMPLETE ===")
            except Exception as e:
                error_msg = f"✗ Error indexing games for all events: {str(e)}"
                add_log(error_msg)
                import traceback
                traceback.print_exc()
                game_indexing_logs[log_key].append(f"=== ERROR: {error_msg} ===")
    
    if background_tasks:
        background_tasks.add_task(index_task)
    else:
        asyncio.create_task(index_task())
    
    return {
        "status": "started",
        "message": f"Started indexing games for {len(events)} events",
        "events_count": len(events),
        "bucket_id": bucket_id,
        "log_key": log_key
    }

@app.post("/api/local/index-games/{bucket_id}")
async def index_games_for_season_local(
    bucket_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Index games for all events in a season (LOCAL: Open #2 Winter Haven events only)."""
    from database import Event, async_session_maker
    from game_indexer import discover_and_index_event_games_with_status
    from event_indexer import OPEN_2_WINTER_HAVEN_EVENT_IDS
    
    # Get only Open #2 Winter Haven events
    events_query = select(Event.event_id, Event.event_name).where(
        Event.bucket_id == bucket_id,
        Event.event_id.in_(OPEN_2_WINTER_HAVEN_EVENT_IDS)
    )
    result = await db.execute(events_query)
    events = result.all()
    
    if not events:
        return {
            "status": "error",
            "message": f"No Open #2 Tier 1 Singles events found for season {bucket_id}",
            "bucket_id": bucket_id
        }
    
    # Initialize status tracking
    status_key = f"season_{bucket_id}_local"
    log_key = f"local_{bucket_id}"
    game_indexing_status[status_key] = {
        "status": "running",
        "bucket_id": bucket_id,
        "started_at": datetime.utcnow().isoformat(),
        "total_events": len(events),
        "processed_events": 0,
        "new_games_indexed": 0,
        "total_games": 0,
        "current_event": None,
        "error": None,
        "local_mode": True
    }
    game_indexing_logs[log_key] = []
    
    def add_log(message: str):
        timestamp = datetime.utcnow().isoformat()
        log_entry = f"[{timestamp}] {message}"
        if log_key not in game_indexing_logs:
            game_indexing_logs[log_key] = []
        game_indexing_logs[log_key].append(log_entry)
        print(log_entry)  # Also print to console
        # Keep only last 1000 log entries
        if len(game_indexing_logs[log_key]) > 1000:
            game_indexing_logs[log_key] = game_indexing_logs[log_key][-1000:]
    
    # Add initial log entry immediately (before background task starts)
    add_log("Logging system initialized (LOCAL mode)")
    add_log(f"Found {len(events)} events to process")
    add_log("Background task starting...")
    
    async def index_task():
        try:
            add_log("Background task started - entering database session...")
            async with async_session_maker() as db_task:
                add_log(f"Database session opened, starting game indexing for {len(events)} events (LOCAL mode)")
                total_new_games = 0
                
                def update_status(**kwargs):
                    if status_key in game_indexing_status:
                        game_indexing_status[status_key].update(kwargs)
                
                for idx, (event_id, event_name) in enumerate(events, 1):
                    add_log(f"Processing event {idx}/{len(events)}: {event_id} ({event_name})")
                    update_status(
                        current_event=event_id,
                        processed_events=idx - 1
                    )
                    
                    new_games = await discover_and_index_event_games_with_status(
                        event_id, db_task, status_callback=update_status, log_callback=add_log, skip_if_complete=False
                    )
                    total_new_games += new_games
                    
                    add_log(f"Event {event_id}: {new_games} new games indexed (total so far: {total_new_games})")
                    update_status(
                        processed_events=idx,
                        new_games_indexed=total_new_games
                    )
                
                # Get final total games count
                from sqlalchemy import func
                from database import EventGame
                total_games_query = select(func.count()).select_from(EventGame).join(
                    Event, EventGame.event_id == Event.event_id
                ).where(
                    Event.bucket_id == bucket_id,
                    Event.event_id.in_(OPEN_2_WINTER_HAVEN_EVENT_IDS)
                )
                total_games_result = await db_task.execute(total_games_query)
                total_games = total_games_result.scalar() or 0
                
                add_log(f"✓ Completed indexing games for season {bucket_id} (LOCAL mode): {total_new_games} new games, {total_games} total")
                update_status(
                    status="completed",
                    total_games=total_games,
                    completed_at=datetime.utcnow().isoformat()
                )
                
                print(f"Completed indexing games for season {bucket_id} (LOCAL mode): {total_new_games} new games, {total_games} total")
                game_indexing_logs[log_key].append("=== INDEXING COMPLETE ===")
        except Exception as e:
            error_msg = f"✗ Error indexing games for season {bucket_id}: {str(e)}"
            add_log(error_msg)
            print(f"Error indexing games for season {bucket_id}: {e}")
            import traceback
            traceback.print_exc()
            if status_key in game_indexing_status:
                game_indexing_status[status_key].update({
                    "status": "error",
                    "error": str(e),
                    "error_at": datetime.utcnow().isoformat()
                })
            game_indexing_logs[log_key].append(f"=== ERROR: {error_msg} ===")
    
    # Add log entry before starting background task
    add_log("Background task queued, waiting to start...")
    
    background_tasks.add_task(index_task)
    
    # Add log entry after queuing
    add_log("Background task queued successfully")
    
    return {
        "status": "started",
        "bucket_id": bucket_id,
        "message": f"Started indexing games for {len(events)} events (LOCAL: Open #2 Winter Haven events only)",
        "events_count": len(events),
        "status_key": status_key,
        "log_key": log_key
    }

@app.get("/api/game-indexing-logs/{log_key}")
async def get_game_indexing_logs(log_key: str):
    """Get logs for a game indexing operation."""
    logs = game_indexing_logs.get(log_key, [])
    print(f"GET /api/game-indexing-logs/{log_key}: Returning {len(logs)} logs")
    return {
        "log_key": log_key,
        "logs": logs,
        "count": len(logs)
    }

@app.get("/api/game-indexing-status/{bucket_id}")
async def get_game_indexing_status_by_season(bucket_id: int):
    """Get status of game indexing for a season."""
    # Check for season-level status key (for batch indexing)
    status_key = f"season_{bucket_id}"
    if status_key in game_indexing_status:
        return game_indexing_status[status_key]
    
    # Also check for individual event status keys that might be running for this season
    event_statuses = {k: v for k, v in game_indexing_status.items() if k.startswith("event_") and v.get("bucket_id") == bucket_id}
    if event_statuses:
        # Return the most recent one
        latest = max(event_statuses.items(), key=lambda x: x[1].get("started_at", ""))
        return latest[1]
    
    return {
        "status": "not_running",
        "bucket_id": bucket_id,
        "message": "No active indexing for this season"
    }

@app.post("/api/index-games/{bucket_id}")
async def index_games_for_season(
    bucket_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Index games for all events in a season (regional, open, signature, national, and local finals only)."""
    from database import Event, EventGame, async_session_maker
    from game_indexer import discover_and_index_event_games_with_status
    import asyncio
    
    # Get all events for this season, filtering by event type
    # Only index: regional, open, signature, national
    # Also allow local events if they're finals (contain "Final" in name)
    events_query = select(Event.event_id, Event.event_name).where(
        Event.bucket_id == bucket_id
    ).where(
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
    result = await db.execute(events_query)
    events = result.all()
    
    if not events:
        return {
            "status": "error",
            "message": f"No events found for season {bucket_id}",
            "bucket_id": bucket_id
        }
    
    # Initialize status tracking
    status_key = f"season_{bucket_id}"
    log_key = f"season_{bucket_id}"
    game_indexing_status[status_key] = {
        "status": "running",
        "bucket_id": bucket_id,
        "started_at": datetime.utcnow().isoformat(),
        "total_events": len(events),
        "processed_events": 0,
        "new_games_indexed": 0,
        "total_games": 0,
        "current_event": None,
        "error": None
    }
    
    # Initialize log buffer
    game_indexing_logs[log_key] = []
    
    def add_log(message: str):
        timestamp = datetime.utcnow().isoformat()
        log_entry = f"[{timestamp}] {message}"
        if log_key not in game_indexing_logs:
            game_indexing_logs[log_key] = []
        game_indexing_logs[log_key].append(log_entry)
        print(log_entry)
        if len(game_indexing_logs[log_key]) > 1000:
            game_indexing_logs[log_key] = game_indexing_logs[log_key][-1000:]
    
    async def index_task():
        async with async_session_maker() as db_task:
            try:
                add_log(f"Starting game indexing for season {bucket_id}: {len(events)} events")
                total_new_games = 0
                total_games = 0
                
                for idx, (event_id, event_name) in enumerate(events, 1):
                    if status_key in game_indexing_status:
                        game_indexing_status[status_key]["current_event"] = f"{event_id}: {event_name}"
                        game_indexing_status[status_key]["processed_events"] = idx - 1
                    
                    add_log(f"Processing event {idx}/{len(events)}: {event_id} ({event_name})")
                    
                    new_games = await discover_and_index_event_games_with_status(
                        event_id, db_task, status_callback=None, skip_if_complete=False
                    )
                    total_new_games += new_games
                    
                    # Get total games for this event
                    games_query = select(func.count()).select_from(EventGame).where(EventGame.event_id == event_id)
                    games_result = await db_task.execute(games_query)
                    event_games_count = games_result.scalar() or 0
                    total_games += event_games_count
                    
                    add_log(f"Event {event_id}: {new_games} new games indexed (total: {event_games_count})")
                    
                    if status_key in game_indexing_status:
                        game_indexing_status[status_key]["new_games_indexed"] = total_new_games
                        game_indexing_status[status_key]["total_games"] = total_games
                        game_indexing_status[status_key]["processed_events"] = idx
                    
                    await asyncio.sleep(0.1)
                
                add_log(f"✓ Completed indexing games for season {bucket_id}: {total_new_games} new games, {total_games} total")
                
                if status_key in game_indexing_status:
                    game_indexing_status[status_key]["status"] = "completed"
                    game_indexing_status[status_key]["processed_events"] = len(events)
                    game_indexing_status[status_key]["current_event"] = None
                
                print(f"Completed indexing games for season {bucket_id}: {total_new_games} new games, {total_games} total")
            except Exception as e:
                error_msg = f"✗ Error indexing games for season {bucket_id}: {str(e)}"
                add_log(error_msg)
                import traceback
                traceback.print_exc()
                if status_key in game_indexing_status:
                    game_indexing_status[status_key]["status"] = "error"
                    game_indexing_status[status_key]["error"] = error_msg
                print(f"Error indexing games for season {bucket_id}: {e}")
    
    if background_tasks:
        background_tasks.add_task(index_task)
    else:
        asyncio.create_task(index_task())
    
    return {
        "status": "started",
        "message": f"Started indexing games for season {bucket_id} ({len(events)} events)",
        "bucket_id": bucket_id,
        "events_count": len(events),
        "status_key": status_key,
        "log_key": log_key
    }

@app.get("/api/local/game-indexing-status/{bucket_id}")
async def get_game_indexing_status_local(bucket_id: int):
    """Get status of game indexing for a season (LOCAL mode)."""
    # Check for season-level status key (for batch indexing)
    status_key = f"season_{bucket_id}_local"
    if status_key in game_indexing_status:
        return game_indexing_status[status_key]
    
    # Also check for individual event status keys that might be running
    # Return the most recent one if multiple exist
    event_statuses = {k: v for k, v in game_indexing_status.items() if k.startswith("event_") and v.get("bucket_id") == bucket_id and v.get("local_mode")}
    if event_statuses:
        # Return the most recent one
        latest = max(event_statuses.items(), key=lambda x: x[1].get("started_at", ""))
        return latest[1]
    
    return {
        "status": "not_running",
        "bucket_id": bucket_id,
        "message": "No active indexing for this season"
    }

@app.get("/api/local/event-indexing-status/{bucket_id}")
async def get_event_indexing_status_local(bucket_id: int):
    """Get status of event indexing for a season (LOCAL mode)."""
    # Use the same status tracking as regular event indexing
    from event_indexer import get_event_indexing_status
    return await get_event_indexing_status(bucket_id)

@app.get("/api/events/{event_id}/games-count")
async def get_event_games_count(event_id: int, db: AsyncSession = Depends(get_db)):
    """Get count of indexed games and matches for an event."""
    from sqlalchemy import func
    from database import EventGame, EventMatch
    
    games_count_query = select(func.count()).select_from(EventGame).where(EventGame.event_id == event_id)
    games_result = await db.execute(games_count_query)
    games_count = games_result.scalar() or 0
    
    matches_count_query = select(func.count()).select_from(EventMatch).where(EventMatch.event_id == event_id)
    matches_result = await db.execute(matches_count_query)
    matches_count = matches_result.scalar() or 0
    
    # Get unique player count
    player1_ids_query = select(EventGame.player1_id).where(EventGame.event_id == event_id).distinct()
    player2_ids_query = select(EventGame.player2_id).where(EventGame.event_id == event_id).distinct()
    player1_result = await db.execute(player1_ids_query)
    player2_result = await db.execute(player2_ids_query)
    player_ids = set()
    for row in player1_result.all():
        if row[0]:
            player_ids.add(row[0])
    for row in player2_result.all():
        if row[0]:
            player_ids.add(row[0])
    unique_players = len(player_ids)
    
    return {
        "event_id": event_id,
        "games_count": games_count,
        "matches_count": matches_count,
        "unique_players": unique_players,
        "has_games": games_count > 0
    }

@app.get("/api/events/stats")
async def get_event_stats(db: AsyncSession = Depends(get_db)):
    """Get statistics about indexed events."""
    from sqlalchemy import func
    from database import Event, PlayerEventStats, EventMatchup, EventStanding
    
    # Total events (excluding local)
    total_events_result = await db.execute(
        select(func.count()).select_from(Event).where(
            (Event.event_type != "local") & (Event.event_type != "l")
        )
    )
    total_events = total_events_result.scalar() or 0
    
    # Total player event stats
    total_stats_result = await db.execute(select(func.count()).select_from(PlayerEventStats))
    total_player_event_stats = total_stats_result.scalar() or 0
    
    # Total matchups
    total_matchups_result = await db.execute(select(func.count()).select_from(EventMatchup))
    total_matchups = total_matchups_result.scalar() or 0
    
    # Total standings
    total_standings_result = await db.execute(select(func.count()).select_from(EventStanding))
    total_standings = total_standings_result.scalar() or 0
    
    # Events by type (excluding local) - normalize types
    # Use CASE to normalize "r" -> "regional", "n" -> "national", "s" -> "signature"
    from sqlalchemy import case
    events_by_type_result = await db.execute(
        select(
            case(
                (Event.event_type.in_(["r", "regional"]), "regional"),
                (Event.event_type.in_(["n", "national"]), "national"),
                (Event.event_type.in_(["s", "signature"]), "signature"),
                else_=Event.event_type
            ).label("normalized_type"),
            func.count(Event.id).label('count')
        )
        .where((Event.event_type != "local") & (Event.event_type != "l"))
        .group_by("normalized_type")
    )
    events_by_type = {}
    for row in events_by_type_result.all():
        normalized_type = row[0] or "unknown"
        count = row[1]
        # Merge counts if type already exists (shouldn't happen with normalization, but just in case)
        if normalized_type in events_by_type:
            events_by_type[normalized_type] += count
        else:
            events_by_type[normalized_type] = count
    
    # Count local events separately
    local_count_result = await db.execute(
        select(func.count()).select_from(Event).where(
            (Event.event_type == "local") | (Event.event_type == "l")
        )
    )
    local_events_count = local_count_result.scalar() or 0
    
    return {
        "total_events": total_events,
        "total_player_event_stats": total_player_event_stats,
        "total_matchups": total_matchups,
        "total_standings": total_standings,
        "events_by_type": events_by_type,
        "local_events_count": local_events_count,
        "note": "Local events are excluded from totals. Use /api/delete-local-events to remove them."
    }

@app.get("/api/events")
async def get_events(
    event_type: Optional[str] = Query(None, description="Filter by event type (open, regional, national, signature)"),
    bucket_id: Optional[int] = Query(None, description="Filter by season/bucket ID"),
    player_id: Optional[int] = Query(None, description="Filter by player ID"),
    player_name: Optional[str] = Query(None, description="Search by player name"),
    limit: int = Query(100, description="Maximum number of events to return"),
    offset: int = Query(0, description="Number of events to skip"),
    db: AsyncSession = Depends(get_db)
):
    """Get list of events with optional filters."""
    # Select only columns that definitely exist (avoid missing column errors)
    query = select(
        Event.event_id,
        Event.event_name,
        Event.event_type,
        Event.event_date,
        Event.location,
        Event.city,
        Event.state,
        Event.bucket_id,
        Event.event_number,
        Event.is_signature
    )
    player_event_ids_filter = None
    
    try:
        
        # Filter out "payout" events
        query = query.where(~Event.event_name.ilike("%payout%"))
        
        # Apply filters - normalize event types
        if event_type:
            # Handle both "r" and "regional", "n" and "national", etc.
            if event_type == "regional":
                query = query.where(or_(Event.event_type == "regional", Event.event_type == "r"))
            elif event_type == "open":
                query = query.where(Event.event_type == "open")
            elif event_type == "national":
                query = query.where(or_(Event.event_type == "national", Event.event_type == "n"))
            elif event_type == "signature":
                query = query.where(or_(Event.event_type == "signature", Event.event_type == "s"))
            else:
                query = query.where(Event.event_type == event_type)
        
        if bucket_id:
            query = query.where(Event.bucket_id == bucket_id)
        
        # Filter by player if provided
        if player_id:
            # Get events where this player participated
            player_events_query = select(PlayerEventStats.event_id).where(
                PlayerEventStats.player_id == player_id
            ).distinct()
            player_events_result = await db.execute(player_events_query)
            player_event_ids_filter = [row[0] for row in player_events_result.all()]
            if not player_event_ids_filter:
                # Player has no events, return empty
                return {
                    "events": [],
                    "total": 0,
                    "limit": limit,
                    "offset": offset
                }
        
        # Search by player name
        if player_name:
            # Find player IDs matching name
            player_search_query = select(Player.player_id).where(
                or_(
                    Player.first_name.ilike(f"%{player_name}%"),
                    Player.last_name.ilike(f"%{player_name}%")
                )
            ).distinct()
            player_search_result = await db.execute(player_search_query)
            player_ids = [row[0] for row in player_search_result.all()]
            
            if player_ids:
                # Get events for these players
                player_events_query = select(PlayerEventStats.event_id).where(
                    PlayerEventStats.player_id.in_(player_ids)
                ).distinct()
                player_events_result = await db.execute(player_events_query)
                found_event_ids = [row[0] for row in player_events_result.all()]
                if found_event_ids:
                    player_event_ids_filter = found_event_ids
                else:
                    return {
                        "events": [],
                        "total": 0,
                        "limit": limit,
                        "offset": offset
                    }
            else:
                return {
                    "events": [],
                    "total": 0,
                    "limit": limit,
                    "offset": offset
                }
        
        # Apply player event filter if we have one
        if player_event_ids_filter:
            query = query.where(Event.event_id.in_(player_event_ids_filter))
        
        # Get total count (before pagination) - use same filters as main query
        count_query = select(func.count()).select_from(Event)
        # Filter out "payout" events
        count_query = count_query.where(~Event.event_name.ilike("%payout%"))
        if event_type:
            # Use same normalization as main query
            if event_type == "regional":
                count_query = count_query.where(or_(Event.event_type == "regional", Event.event_type == "r"))
            elif event_type == "open":
                count_query = count_query.where(Event.event_type == "open")
            elif event_type == "national":
                count_query = count_query.where(or_(Event.event_type == "national", Event.event_type == "n"))
            elif event_type == "signature":
                count_query = count_query.where(or_(Event.event_type == "signature", Event.event_type == "s"))
            else:
                count_query = count_query.where(Event.event_type == event_type)
        if bucket_id:
            count_query = count_query.where(Event.bucket_id == bucket_id)
        if player_event_ids_filter:
            count_query = count_query.where(Event.event_id.in_(player_event_ids_filter))
        
        total_result = await db.execute(count_query)
        total = total_result.scalar() or 0
        
        # Order by importance: nationals > opens > regionals, and within opens: Tier 1 > Tier 2 > Tier 3
        # Use CASE to assign importance scores
        from sqlalchemy import case
        importance_expr = case(
            (Event.event_type.in_(["national", "n"]), 1),  # Nationals first
            (Event.event_type.in_(["signature", "s"]), 1),  # Signatures also first
            (Event.event_type == "open", 2),  # Opens second
            (Event.event_type.in_(["regional", "r"]), 3),  # Regionals last
            else_=4
        )
        
        # For opens, extract tier number for sorting (Tier 1 > Tier 2 > Tier 3)
        # We'll use a subquery to extract tier number, but for simplicity, use name-based sorting
        # Tier 1 events should come before Tier 2, etc.
        query = query.order_by(
            importance_expr,  # First by event type importance
            case(
                (Event.event_name.ilike("%tier 1%"), 1),
                (Event.event_name.ilike("%tier 2%"), 2),
                (Event.event_name.ilike("%tier 3%"), 3),
                (Event.event_name.ilike("%tier 4%"), 4),
                else_=5
            ),  # Then by tier within type
            desc(Event.event_date),  # Then by date (most recent first)
            desc(Event.event_id)
        )
        
        # Apply pagination
        query = query.limit(limit).offset(offset)
        
        result = await db.execute(query)
        events = result.all()
        
        # Get base_event_name and bracket_name if they exist
        try:
            query_with_names = select(
                Event.event_id,
                Event.event_name,
                Event.base_event_name,
                Event.bracket_name,
                Event.event_type,
                Event.event_date,
                Event.location,
                Event.city,
                Event.state,
                Event.bucket_id,
                Event.event_number,
                Event.is_signature
            )
            
            # Apply same filters
            if event_type:
                if event_type == "regional":
                    query_with_names = query_with_names.where(or_(Event.event_type == "regional", Event.event_type == "r"))
                elif event_type == "open":
                    query_with_names = query_with_names.where(Event.event_type == "open")
                elif event_type == "national":
                    query_with_names = query_with_names.where(or_(Event.event_type == "national", Event.event_type == "n"))
                elif event_type == "signature":
                    query_with_names = query_with_names.where(or_(Event.event_type == "signature", Event.event_type == "s"))
                else:
                    query_with_names = query_with_names.where(Event.event_type == event_type)
            
            if bucket_id:
                query_with_names = query_with_names.where(Event.bucket_id == bucket_id)
            
            if player_event_ids_filter:
                query_with_names = query_with_names.where(Event.event_id.in_(player_event_ids_filter))
            
            query_with_names = query_with_names.where(~Event.event_name.ilike("%payout%"))
            
            # Apply same ordering
            query_with_names = query_with_names.order_by(
                importance_expr,
                case(
                    (Event.event_name.ilike("%tier 1%"), 1),
                    (Event.event_name.ilike("%tier 2%"), 2),
                    (Event.event_name.ilike("%tier 3%"), 3),
                    (Event.event_name.ilike("%tier 4%"), 4),
                    else_=5
                ),
                desc(Event.event_date),
                desc(Event.event_id)
            ).limit(limit).offset(offset)
            
            result_with_names = await db.execute(query_with_names)
            events_with_names = result_with_names.all()
            
            # Format events with names
            events_list = []
            for event in events_with_names:
                event_dict = {
                    "event_id": event[0],
                    "event_name": event[1],
                    "base_event_name": event[2],
                    "bracket_name": event[3],
                    "event_type": event[4],
                    "event_date": event[5].isoformat() if event[5] else None,
                    "location": event[6],
                    "city": event[7],
                    "state": event[8],
                    "bucket_id": event[9],
                    "event_number": event[10],
                    "is_signature": event[11],
                }
                events_list.append(event_dict)
        except Exception as e:
            # Fallback if columns don't exist
            print(f"Error getting base_event_name/bracket_name: {e}")
            events_list = []
            for event in events:
                event_dict = {
                    "event_id": event[0],
                    "event_name": event[1],
                    "base_event_name": None,
                    "bracket_name": None,
                    "event_type": event[2],
                    "event_date": event[3].isoformat() if event[3] else None,
                    "location": event[4],
                    "city": event[5],
                    "state": event[6],
                    "bucket_id": event[7],
                    "event_number": event[8],
                    "is_signature": event[9],
                }
                events_list.append(event_dict)
        except Exception as e:
            # Fallback if columns don't exist - use basic event data
            print(f"Error getting base_event_name/bracket_name, using basic columns: {e}")
            events_list = []
            for event in events:
                event_dict = {
                    "event_id": event[0],
                    "event_name": event[1],
                    "base_event_name": None,
                    "bracket_name": None,
                    "event_type": event[2],
                    "event_date": event[3].isoformat() if event[3] else None,
                    "location": event[4],
                    "city": event[5],
                    "state": event[6],
                    "bucket_id": event[7],
                    "event_number": event[8],
                    "is_signature": event[9],
                }
                events_list.append(event_dict)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching events: {str(e)}")
    
    # Group events by base_event_name and bracket type
    import re
    grouped_events = {}
    for event in events_list:
        base_name = event.get("base_event_name") or event.get("event_name", "")
        bracket_name = event.get("bracket_name") or ""
        
        # Extract bracket type (e.g., "Tier 1 Singles" from "Tier 1 Singles Bracket C" or "Tier 1 Singles Final 4")
        # Handle formats like:
        # - "Tier 1 Singles Bracket C" -> "Tier 1 Singles"
        # - "Tier 1 - Doubles Bracket B" -> "Tier 1 Doubles"
        # - "Tier 1 Doubles - Bracket A" -> "Tier 1 Doubles"
        # - "Tier 1 Singles Final 4" -> "Tier 1 Singles"
        # - "Tier 1 Doubles - Final 2" -> "Tier 1 Doubles"
        bracket_type = bracket_name
        if bracket_name:
            # Normalize dashes - remove standalone dashes and extra spaces
            bracket_type = re.sub(r'\s*-\s*', ' ', bracket_name)
            # Remove "Final X" or "Final" suffix (for finals to group with brackets)
            bracket_type = re.sub(r'\s*Final\s+\d+\s*$', '', bracket_type, flags=re.IGNORECASE)
            bracket_type = re.sub(r'\s*Final\s*$', '', bracket_type, flags=re.IGNORECASE)
            # Remove "Bracket X" or "Bracket" suffix using regex
            bracket_type = re.sub(r'\s*Bracket\s+[A-Z]\s*$', '', bracket_type, flags=re.IGNORECASE)
            bracket_type = re.sub(r'\s*Bracket\s*$', '', bracket_type, flags=re.IGNORECASE)
            # Clean up multiple spaces
            bracket_type = re.sub(r'\s+', ' ', bracket_type).strip()
        
        # Create group key: base_event_name + bracket_type
        if base_name and bracket_type:
            group_key = f"{base_name}|||{bracket_type}"
        elif base_name:
            group_key = base_name
        else:
            group_key = event.get("event_name", "")
        
        if group_key not in grouped_events:
            grouped_events[group_key] = {
                "base_event_name": base_name,
                "bracket_type": bracket_type,
                "events": [],
                "event_type": event.get("event_type"),
                "event_date": event.get("event_date"),
                "location": event.get("location"),
                "city": event.get("city"),
                "state": event.get("state"),
                "bucket_id": event.get("bucket_id"),
                "event_number": event.get("event_number"),
                "is_signature": event.get("is_signature"),
            }
        
        grouped_events[group_key]["events"].append(event)
    
    # Convert to list format
    grouped_list = []
    for group_key, group_data in grouped_events.items():
        if len(group_data["events"]) > 1:
            # Multiple brackets - create grouped entry
            grouped_list.append({
                "type": "grouped",
                "base_event_name": group_data["base_event_name"],
                "bracket_type": group_data["bracket_type"],
                "display_name": group_data["base_event_name"] if not group_data["bracket_type"] else f"{group_data['base_event_name']} {group_data['bracket_type']}",
                "event_type": group_data["event_type"],
                "event_date": group_data["event_date"],
                "location": group_data["location"],
                "city": group_data["city"],
                "state": group_data["state"],
                "bucket_id": group_data["bucket_id"],
                "event_number": group_data["event_number"],
                "is_signature": group_data["is_signature"],
                "event_count": len(group_data["events"]),
                "events": group_data["events"],
            })
        else:
            # Single event - add as-is
            grouped_list.append({
                "type": "single",
                **group_data["events"][0]
            })
    
    return {
        "events": grouped_list,
        "total": total,
        "limit": limit,
        "offset": offset,
        "grouped": True
    }

@app.get("/api/events/grouped")
async def get_grouped_events_stats(
    base: str = Query(..., description="Base event name (e.g., 'Open #2 Winter Haven')"),
    bracket: str = Query(..., description="Bracket type (e.g., 'Tier 1 Singles')"),
    bracket_id: Optional[int] = Query(None, description="Optional: Filter to specific bracket event_id for bracket-specific rankings"),
    db: AsyncSession = Depends(get_db)
):
    """Get combined stats for a group of events (e.g., all Tier 1 Singles brackets).
    
    Now uses pre-computed cached stats if available for fast response.
    """
    try:
        from database import Event, EventGame, EventStanding, Player
        from sqlalchemy import func, and_, or_
        from stats_calculator import get_aggregated_stats, calculate_event_aggregated_stats, store_aggregated_stats
        import re
        
        # Normalize bracket type to match cache indexing logic
        # This must match exactly how bracket_type is normalized during cache indexing
        bracket_type_pattern = bracket
        if bracket:
            # Normalize dashes - remove standalone dashes and extra spaces
            bracket_type_pattern = re.sub(r'\s*-\s*', ' ', bracket)
            # Remove "Final X" or "Final" suffix (for finals to group with brackets)
            bracket_type_pattern = re.sub(r'\s*Final\s+\d+\s*$', '', bracket_type_pattern, flags=re.IGNORECASE)
            bracket_type_pattern = re.sub(r'\s*Final\s*$', '', bracket_type_pattern, flags=re.IGNORECASE)
            # Remove "Bracket X" or "Bracket" suffix using regex
            bracket_type_pattern = re.sub(r'\s*Bracket\s+[A-Z]\s*$', '', bracket_type_pattern, flags=re.IGNORECASE)
            bracket_type_pattern = re.sub(r'\s*Bracket\s*$', '', bracket_type_pattern, flags=re.IGNORECASE)
            # Clean up multiple spaces
            bracket_type_pattern = re.sub(r'\s+', ' ', bracket_type_pattern).strip()
        
        # Query events matching base_name and bracket_type
        # We need to normalize bracket_name in the query to match the pattern
        # Since we can't easily do regex in SQL, we'll use a broader ILIKE pattern
        # and then filter in Python
        events_query = select(
            Event.event_id,
            Event.event_name,
            Event.base_event_name,
            Event.bracket_name
        ).where(
            Event.base_event_name == base
        )
        
        events_result = await db.execute(events_query)
        all_events = events_result.all()
        
        # Filter events by normalized bracket_type pattern
        events = []
        for event in all_events:
            bracket_name = event[3] or ""
            event_name = event[1] or ""
            
            # Normalize the bracket_name the same way we normalize the pattern
            normalized_bracket_name = bracket_name
            if bracket_name:
                normalized_bracket_name = re.sub(r'\s*-\s*', ' ', bracket_name)
                normalized_bracket_name = re.sub(r'\s*Final\s+\d+\s*$', '', normalized_bracket_name, flags=re.IGNORECASE)
                normalized_bracket_name = re.sub(r'\s*Final\s*$', '', normalized_bracket_name, flags=re.IGNORECASE)
                normalized_bracket_name = re.sub(r'\s*Bracket\s+[A-Z]\s*$', '', normalized_bracket_name, flags=re.IGNORECASE)
                normalized_bracket_name = re.sub(r'\s*Bracket\s*$', '', normalized_bracket_name, flags=re.IGNORECASE)
                normalized_bracket_name = re.sub(r'\s+', ' ', normalized_bracket_name).strip()
            
            # Check if normalized bracket_name matches the pattern, or if event_name contains it
            if bracket_type_pattern.lower() in normalized_bracket_name.lower() or bracket_type_pattern.lower() in event_name.lower():
                events.append(event)
        
        if not events:
            raise HTTPException(status_code=404, detail=f"No events found matching base='{base}' and bracket='{bracket}'")
        
        event_ids = [event[0] for event in events]
        
        # If filtering by specific bracket, try to get bracket-specific cached stats
        if bracket_id:
            bracket_key = f"event_{bracket_id}"
            bracket_cached_stats = await get_aggregated_stats(bracket_key, db)
            
            if bracket_cached_stats and bracket_id in bracket_cached_stats.get("event_ids", []):
                # Use bracket-specific cached stats
                stats_list = bracket_cached_stats["player_stats"]
                total_games = bracket_cached_stats.get("total_games", 0)
                
                # Build bracket list
                bracket_list = []
                for e in events:
                    bracket_name = e[3] or e[1]
                    bracket_list.append({
                        "event_id": e[0],
                        "event_name": e[1],
                        "bracket_name": bracket_name,
                        "display_name": bracket_name.replace(f"{base} ", "").strip() if bracket_name else e[1]
                    })
                
                return {
                    "base_event_name": base,
                    "bracket_type": bracket_type_pattern,
                    "events": bracket_list,
                    "brackets": bracket_list,
                    "player_stats": stats_list,
                    "total_players": len(stats_list),
                    "total_games": total_games,
                }
        
        # Try to get pre-computed overall stats (for all brackets combined)
        # Cache key must match exactly how it's generated during cache indexing
        # Normalize base name to match what's stored (trim whitespace)
        base_normalized = base.strip()
        group_key = f"grouped_{base_normalized}_{bracket_type_pattern}"
        print(f"Looking for cached stats with key: '{group_key}' for event_ids: {event_ids}")
        cached_stats = await get_aggregated_stats(group_key, db)
        
        if cached_stats:
            cached_event_ids = cached_stats.get("event_ids", [])
            print(f"Found cached stats, cached event_ids: {cached_event_ids}")
            # Check if event_ids match (using set comparison for order independence)
            if set(cached_event_ids) == set(event_ids):
                # Use cached overall stats - much faster!
                print(f"✓ Using cached stats for {base_normalized} {bracket_type_pattern}")
                stats_list = cached_stats["player_stats"]
                total_games = cached_stats.get("total_games", 0)
            else:
                print(f"⚠ Cached stats event_ids mismatch: cached={cached_event_ids}, requested={event_ids}")
                cached_stats = None  # Force recalculation
        else:
            print(f"⚠ No cached stats found for key: '{group_key}'")
            # Try to find any cached stats for this bracket type to see what keys exist
            from database import EventAggregatedStats
            all_stats_query = select(EventAggregatedStats).where(
                EventAggregatedStats.group_type == "grouped",
                EventAggregatedStats.base_event_name == base_normalized,
                EventAggregatedStats.bracket_type == bracket_type_pattern
            )
            all_stats_result = await db.execute(all_stats_query)
            all_stats = all_stats_result.scalars().all()
            if all_stats:
                print(f"Found {len(all_stats)} cached stats with matching base/bracket but different keys:")
                for stat in all_stats:
                    print(f"  - Key: '{stat.group_key}', event_ids: {stat.event_ids}")
        
        if cached_stats:
            # Use cached overall stats - much faster!
            stats_list = cached_stats["player_stats"]
            total_games = cached_stats.get("total_games", 0)
            
            # Format ties for bracket view
            if bracket_id:
                rank_groups = {}
                for player in stats_list:
                    rank = player["overall_rank"]
                    if isinstance(rank, int):
                        if rank not in rank_groups:
                            rank_groups[rank] = []
                        rank_groups[rank].append(player)
                
                for rank, players in rank_groups.items():
                    if len(players) > 1:
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
                            if "2nd in bracket" in rank:
                                return (1, 4)
                            return (2, 999)
                return (2, 999)
            
            stats_list.sort(key=sort_key)
            
            # Build bracket list
            bracket_list = []
            for e in events:
                bracket_name = e[3] or e[1]
                bracket_list.append({
                    "event_id": e[0],
                    "event_name": e[1],
                    "bracket_name": bracket_name,
                    "display_name": bracket_name.replace(f"{base} ", "").strip() if bracket_name else e[1]
                })
            
            return {
                "base_event_name": base,
                "bracket_type": bracket_type_pattern,
                "events": bracket_list,
                "brackets": bracket_list,
                "player_stats": stats_list,
                "total_players": len(stats_list),
                "total_games": total_games,
            }
        
        # Cache miss - calculate on the fly (fallback, slower)
        # Get all games from all these events
        games_query = select(EventGame).where(EventGame.event_id.in_(event_ids))
        games_result = await db.execute(games_query)
        games = games_result.scalars().all()
        
        # Aggregate stats by player across all events
        player_stats = {}
        
        for game in games:
            # Process player 1 stats
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
                        "bracket_ranks": {}  # Track rank per bracket
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
            
            # Process player 2 stats
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
        
        # Get standings from all brackets to calculate combined ranking
        standings_query = select(EventStanding).where(EventStanding.event_id.in_(event_ids))
        standings_result = await db.execute(standings_query)
        all_standings = standings_result.scalars().all()
        
        # Group standings by event_id
        standings_by_event = {}
        for standing in all_standings:
            if standing.event_id not in standings_by_event:
                standings_by_event[standing.event_id] = []
            standings_by_event[standing.event_id].append(standing)
            # Track rank per bracket for each player
            if standing.player_id in player_stats:
                player_stats[standing.player_id]["bracket_ranks"][standing.event_id] = standing.final_rank
        
        # Calculate combined ranking:
        # 1st overall = best rank across all brackets
        # 2nd overall = second best rank
        # T-3rd = all bracket winners (rank 1 in any bracket)
        # T-2nd = all 2nd place finishers (rank 2 in any bracket)
        # etc.
        for player_id, stats in player_stats.items():
            bracket_ranks = list(stats["bracket_ranks"].values())
            if bracket_ranks:
                stats["best_rank"] = min(bracket_ranks)
                stats["worst_rank"] = max(bracket_ranks)
            else:
                stats["best_rank"] = None
                stats["worst_rank"] = None
        
        # Get player names and CPI
        player_ids_list = list(player_stats.keys())
        player_names_dict = {}
        player_cpi_dict = {}
        
        if player_ids_list:
            try:
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
            except Exception as e:
                print(f"Error getting player data: {e}")
        
        # Calculate combined ranking algorithm
        # Strategy:
        # 1. 1st overall = single best player (best rank, tie-break by PPR)
        # 2. 2nd overall = second best player
        # 3. T-3rd = all remaining bracket winners (rank 1 in any bracket)
        # 4. T-4th (or "T-2nd in bracket") = all players with rank 2 in any bracket
        # 5. Continue with rank 3, 4, etc.
        
        # First, calculate PPR and win_pct for tie-breaking
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
        
        # Group players by their bracket ranks
        bracket_winners = []  # Players with rank 1 in any bracket
        players_by_rank = {}  # rank -> list of player_ids
        
        for player_id, stats in player_stats.items():
            bracket_ranks = stats.get("bracket_ranks", {})
            for event_id, rank in bracket_ranks.items():
                if rank == 1:
                    bracket_winners.append(player_id)
                if rank not in players_by_rank:
                    players_by_rank[rank] = []
                if player_id not in players_by_rank[rank]:
                    players_by_rank[rank].append(player_id)
        
        # Remove duplicates from bracket_winners
        bracket_winners = list(set(bracket_winners))
        
        # Sort bracket winners by PPR (descending) for tie-breaking
        bracket_winners.sort(key=lambda pid: player_stats[pid].get("ppr", 0.0), reverse=True)
        
        # Count number of brackets (for doubles logic)
        num_brackets = len(event_ids)
        
        overall_rank = 1
        overall_ranks = {}
        assigned_players = set()
        
        # 1st overall: Best bracket winner
        if bracket_winners:
            first_place = bracket_winners[0]
            overall_ranks[first_place] = 1
            assigned_players.add(first_place)
            overall_rank = 2
        
        # 2nd overall: Second best bracket winner (if exists)
        if len(bracket_winners) > 1:
            second_place = bracket_winners[1]
            overall_ranks[second_place] = 2
            assigned_players.add(second_place)
            overall_rank = 3
        
        # T-3rd: All remaining bracket winners
        remaining_winners = [pid for pid in bracket_winners if pid not in assigned_players]
        if remaining_winners:
            for pid in remaining_winners:
                overall_ranks[pid] = f"T-{overall_rank}"
            overall_rank += len(remaining_winners)
            assigned_players.update(remaining_winners)
        
        # For rank 2 finishers (T-2nd in bracket or T-4th)
        rank_2_players = [pid for pid in players_by_rank.get(2, []) if pid not in assigned_players]
        if rank_2_players:
            # For doubles (2 brackets), call it "T-2nd in bracket"
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
            if rank <= 2:  # Already handled
                continue
            rank_players = [pid for pid in players_by_rank[rank] if pid not in assigned_players]
            if rank_players:
                # Sort by best stats for tie-breaking
                rank_players.sort(key=lambda pid: (
                    -player_stats[pid].get("ppr", 0.0),  # Higher PPR first
                    -player_stats[pid].get("win_pct", 0.0)  # Then win percentage
                ))
                
                if len(rank_players) > 1:
                    # Multiple players at this bracket rank - tie
                    for pid in rank_players:
                        overall_ranks[pid] = f"T-{overall_rank}"
                    overall_rank += len(rank_players)
                else:
                    # Single player
                    overall_ranks[rank_players[0]] = overall_rank
                    overall_rank += 1
                assigned_players.update(rank_players)
        
        # Any remaining players without bracket ranks get assigned based on stats
        remaining_players = [pid for pid in player_stats.keys() if pid not in assigned_players]
        if remaining_players:
            # Sort by PPR
            remaining_players.sort(key=lambda pid: player_stats[pid].get("ppr", 0.0), reverse=True)
            for pid in remaining_players:
                overall_ranks[pid] = f"T-{overall_rank}"
            overall_rank += len(remaining_players)
        
        # Format stats list
        stats_list = []
        for player_id, stats in player_stats.items():
            games_played = stats["games_played"]
            if games_played > 0:
                ppr = stats["total_points"] / stats["total_rounds"] if stats["total_rounds"] > 0 else 0.0
                opp_ppr = stats["total_opponent_ppr"] / games_played if games_played > 0 else 0.0
                dpr = ppr - opp_ppr
                
                bags_in_pct = stats["total_bags_in"] / stats["total_bags_thrown"] if stats["total_bags_thrown"] > 0 else 0.0
                bags_on_pct = stats["total_bags_on"] / stats["total_bags_thrown"] if stats["total_bags_thrown"] > 0 else 0.0
                bags_off_pct = stats["total_bags_off"] / stats["total_bags_thrown"] if stats["total_bags_thrown"] > 0 else 0.0
                four_bagger_pct = stats["total_four_baggers"] / stats["total_rounds"] if stats["total_rounds"] > 0 else 0.0
                win_pct = stats["wins"] / games_played if games_played > 0 else 0.0
                
                first_name, last_name = player_names_dict.get(player_id, ("", ""))
                player_name = f"{first_name} {last_name}".strip() or f"Player {player_id}"
                
                # Determine rank to display
                display_rank = overall_ranks.get(player_id, "N/A")
                
                # If filtering by specific bracket, use bracket-specific rank
                if bracket_id:
                    bracket_rank = stats.get("bracket_ranks", {}).get(bracket_id)
                    if bracket_rank:
                        # Use bracket-specific rank (will format ties later)
                        display_rank = bracket_rank
                    else:
                        # Player not in this bracket, skip them
                        continue
                
                stats_list.append({
                    "player_id": player_id,
                    "player_name": player_name,
                    "overall_rank": display_rank,
                    "best_rank": stats.get("best_rank"),
                    "worst_rank": stats.get("worst_rank"),
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
                    "event_ids": list(stats.get("bracket_ranks", {}).keys()),  # List of event_ids this player participated in
                })
        
        # Sort by overall rank (numeric first, then ties)
        def sort_key(x):
            rank = x["overall_rank"]
            if isinstance(rank, int):
                return (0, rank)
            elif isinstance(rank, str):
                if rank.startswith("T-"):
                    # Handle "T-3" format
                    try:
                        return (1, int(rank.split("-")[1]))
                    except:
                        # Handle "T-2nd in bracket" format
                        if "2nd in bracket" in rank:
                            return (1, 4)  # Treat as T-4th
                        return (2, 999)
            return (2, 999)
        
        stats_list.sort(key=sort_key)
        
        # If filtering by bracket, format ties properly (T-X for players with same rank)
        if bracket_id:
            # Group by rank and format ties
            rank_groups = {}
            for player in stats_list:
                rank = player["overall_rank"]
                if isinstance(rank, int):
                    if rank not in rank_groups:
                        rank_groups[rank] = []
                    rank_groups[rank].append(player)
            
            # Format ties
            for rank, players in rank_groups.items():
                if len(players) > 1:
                    # Multiple players with same rank - format as tie
                    for player in players:
                        player["overall_rank"] = f"T-{rank}"
        
        # Build bracket list for filter buttons
        bracket_list = []
        for e in events:
            bracket_name = e[3] or e[1]  # Use bracket_name if available, fallback to event_name
            bracket_list.append({
                "event_id": e[0],
                "event_name": e[1],
                "bracket_name": bracket_name,
                "display_name": bracket_name.replace(f"{base} ", "").strip() if bracket_name else e[1]  # Remove base name for cleaner display
            })
        
        return {
            "base_event_name": base,
            "bracket_type": bracket_type_pattern,
            "events": bracket_list,
            "brackets": bracket_list,  # Also include as "brackets" for clarity
            "player_stats": stats_list,
            "total_players": len(stats_list),
            "total_games": len(games),
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error calculating grouped stats: {str(e)}")

@app.get("/api/events/{event_id}/info")
async def get_event_info(event_id: int, db: AsyncSession = Depends(get_db)):
    """Get basic event information only (fast, for initial page load)."""
    try:
        from mcp_routes import get_season_name
    except ImportError:
        def get_season_name(bucket_id: int) -> str:
            season_map = {11: "2025-2026 Season", 10: "2024-2025 Season", 9: "2023-2024 Season", 8: "2022-2023 Season"}
            return season_map.get(bucket_id, f"Season {bucket_id}")
    
    try:
        await db.rollback()
        
        # Get event info - select only columns that exist
        event_query = select(
            Event.event_id,
            Event.event_name,
            Event.base_event_name,
            Event.bracket_name,
            Event.event_type,
            Event.event_date,
            Event.location,
            Event.city,
            Event.state,
            Event.bucket_id,
            Event.event_number,
            Event.is_signature
        ).where(Event.event_id == event_id)
        event_result = await db.execute(event_query)
        event_row = event_result.first()
        
        if not event_row:
            raise HTTPException(status_code=404, detail="Event not found")
        
        # Convert row to dict-like object
        class EventObj:
            def __init__(self, row):
                self.event_id = row[0]
                self.event_name = row[1]
                self.base_event_name = row[2]
                self.bracket_name = row[3]
                self.event_type = row[4]
                self.event_date = row[5]
                self.location = row[6]
                self.city = row[7]
                self.state = row[8]
                self.bucket_id = row[9]
                self.event_number = row[10]
                self.is_signature = row[11]
        
        event = EventObj(event_row)
        season_name = get_season_name(event.bucket_id)
        
        return {
            "event": {
                "event_id": event.event_id,
                "event_name": event.event_name,
                "base_event_name": event.base_event_name,
                "bracket_name": event.bracket_name,
                "event_type": event.event_type,
                "event_date": event.event_date.isoformat() if event.event_date else None,
                "location": event.location,
                "city": event.city,
                "state": event.state,
                "bucket_id": event.bucket_id,
                "season": season_name,
                "event_number": event.event_number,
                "is_signature": event.is_signature,
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching event info: {str(e)}")

@app.get("/api/events/{event_id}")
async def get_event_details(event_id: int, db: AsyncSession = Depends(get_db)):
    """Get detailed information about a specific event, including standings and player stats.
    
    Now uses pre-computed cached stats if available for fast response.
    """
    try:
        from mcp_routes import get_season_name
        from stats_calculator import get_aggregated_stats, calculate_bracket_stats
    except ImportError:
        # Fallback if mcp_routes not available
        def get_season_name(bucket_id: int) -> str:
            season_map = {11: "2025-2026 Season", 10: "2024-2025 Season", 9: "2023-2024 Season", 8: "2022-2023 Season"}
            return season_map.get(bucket_id, f"Season {bucket_id}")
    
    try:
        # Use a fresh transaction - rollback any previous errors
        await db.rollback()
        
        # Try to get cached bracket stats first
        bracket_key = f"event_{event_id}"
        cached_stats = await get_aggregated_stats(bracket_key, db)
        
        if cached_stats:
            # Verify event_id matches (should be in event_ids list)
            cached_event_ids = cached_stats.get("event_ids", [])
            if event_id in cached_event_ids:
                # Use cached stats - much faster!
                stats_list = cached_stats["player_stats"]
                print(f"✓ Using cached stats for event {event_id} (key: {bracket_key})")
                
                # Get event info for return
                event_query = select(
                    Event.event_id,
                    Event.event_name,
                    Event.base_event_name,
                    Event.bracket_name,
                    Event.event_type,
                    Event.event_date,
                    Event.location,
                    Event.city,
                    Event.state,
                    Event.bucket_id,
                    Event.event_number,
                    Event.is_signature
                ).where(Event.event_id == event_id)
                event_result = await db.execute(event_query)
                event_row = event_result.first()
                
                if not event_row:
                    raise HTTPException(status_code=404, detail="Event not found")
                
                class EventObj:
                    def __init__(self, row):
                        self.event_id = row[0]
                        self.event_name = row[1]
                        self.base_event_name = row[2]
                        self.bracket_name = row[3]
                        self.event_type = row[4]
                        self.event_date = row[5]
                        self.location = row[6]
                        self.city = row[7]
                        self.state = row[8]
                        self.bucket_id = row[9]
                        self.event_number = row[10]
                        self.is_signature = row[11]
                
                event = EventObj(event_row)
                season_name = get_season_name(event.bucket_id)
                
                # Convert stats_list to standings format for compatibility
                standings_list = []
                for player_stat in stats_list:
                    standings_list.append({
                        "player_id": player_stat["player_id"],
                        "final_rank": player_stat["overall_rank"],
                        "player_name": player_stat["player_name"],
                    })
                
                return {
                    "event": {
                        "event_id": event.event_id,
                        "event_name": event.event_name,
                        "base_event_name": event.base_event_name,
                        "bracket_name": event.bracket_name,
                        "event_type": event.event_type,
                        "event_date": event.event_date.isoformat() if event.event_date else None,
                        "location": event.location,
                        "city": event.city,
                        "state": event.state,
                        "bucket_id": event.bucket_id,
                        "season": season_name,
                        "event_number": event.event_number,
                        "is_signature": event.is_signature,
                    },
                    "player_stats": stats_list,
                    "standings": standings_list,
                    "total_players": len(stats_list),
                }
            else:
                print(f"⚠ Cached stats found but event_id mismatch: expected {event_id}, got {cached_event_ids}")
                cached_stats = None  # Force recalculation
        else:
            print(f"⚠ No cached stats found for event {event_id} (key: {bracket_key})")
        
        # Cache miss - calculate on the fly (fallback, slower)
        # Get event info - select only columns that exist
        event_query = select(
            Event.event_id,
            Event.event_name,
            Event.event_type,
            Event.event_date,
            Event.location,
            Event.city,
            Event.state,
            Event.bucket_id,
            Event.event_number,
            Event.is_signature
        ).where(Event.event_id == event_id)
        event_result = await db.execute(event_query)
        event_row = event_result.first()
        
        if not event_row:
            raise HTTPException(status_code=404, detail="Event not found")
        
        # Convert row to dict-like object
        class EventObj:
            def __init__(self, row):
                self.event_id = row[0]
                self.event_name = row[1]
                self.event_type = row[2]
                self.event_date = row[3]
                self.location = row[4]
                self.city = row[5]
                self.state = row[6]
                self.bucket_id = row[7]
                self.event_number = row[8]
                self.is_signature = row[9]
                self.base_event_name = None  # Column doesn't exist yet
                self.bracket_name = None     # Column doesn't exist yet
        
        event = EventObj(event_row)
    
        # Get standings
        standings_query = select(EventStanding).where(EventStanding.event_id == event_id).order_by(EventStanding.final_rank)
        standings_result = await db.execute(standings_query)
        standings = standings_result.scalars().all()
        
        # Check if we have game data for this event - if so, calculate from games
        from sqlalchemy import func
        from database import EventGame
        games_count_query = select(func.count()).select_from(EventGame).where(EventGame.event_id == event_id)
        games_count_result = await db.execute(games_count_query)
        games_count = games_count_result.scalar() or 0
        
        if games_count > 0:
            # Calculate stats from games
            from event_stats_calculator import calculate_player_stats_from_games
            calculated_stats = await calculate_player_stats_from_games(event_id, db)
            
            # Get player names
            from sqlalchemy import func
            latest_player_dates = select(
                Player.player_id,
                func.max(Player.snapshot_date).label('max_date')
            ).group_by(Player.player_id).subquery()
            
            # Select only columns that exist to avoid schema issues
            latest_players = select(
                Player.player_id,
                Player.first_name,
                Player.last_name,
                Player.snapshot_date
            ).join(
                latest_player_dates,
                and_(
                    Player.player_id == latest_player_dates.c.player_id,
                    Player.snapshot_date == latest_player_dates.c.max_date
                )
            ).subquery()
            
            # Get player names for all calculated stats
            player_ids = list(calculated_stats.keys())
            if player_ids:
                # Select only columns that exist in the database
                try:
                    players_query = select(
                        latest_players.c.player_id,
                        latest_players.c.first_name,
                        latest_players.c.last_name
                    ).where(latest_players.c.player_id.in_(player_ids))
                    players_result = await db.execute(players_query)
                    player_names = {row[0]: (row[1] or "", row[2] or "") for row in players_result.all()}
                except Exception as e:
                    # If query fails (e.g., missing columns), try simpler approach
                    print(f"Error querying players with latest snapshot: {e}")
                    # Fallback: just get any player record
                    simple_players_query = select(
                        Player.player_id,
                        Player.first_name,
                        Player.last_name
                    ).where(Player.player_id.in_(player_ids)).distinct(Player.player_id)
                    players_result = await db.execute(simple_players_query)
                    player_names = {}
                    for row in players_result.all():
                        if row[0] not in player_names:
                            player_names[row[0]] = (row[1] or "", row[2] or "")
            else:
                player_names = {}
            
            # Format calculated stats
            stats_list = []
            for player_id, stats in calculated_stats.items():
                first_name, last_name = player_names.get(player_id, ("", ""))
                stats_list.append({
                    "player_id": player_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "rank": stats.get("rank"),
                    "pts_per_rnd": stats.get("pts_per_rnd"),
                    "dpr": stats.get("dpr"),
                    "player_cpi": None,  # Not available from games
                    "win_pct": stats.get("win_pct"),
                    "total_games": stats.get("total_games", 0),
                    "rounds_total": stats.get("rounds_played", 0),
                    "total_pts": stats.get("total_pts", 0),
                    "opponent_pts_per_rnd": stats.get("opponent_pts_per_rnd"),
                    "opponent_pts_total": stats.get("opponent_pts_total", 0),
                    "four_bagger_pct": stats.get("four_bagger_pct"),
                    "bags_in_pct": stats.get("bags_in_pct"),
                    "bags_on_pct": stats.get("bags_on_pct"),
                    "bags_off_pct": stats.get("bags_off_pct"),
                    "local_wins": None,
                    "local_losses": None,
                    "regional_wins": None,
                    "regional_losses": None,
                    "open_wins": stats.get("wins", 0),
                    "open_losses": stats.get("losses", 0),
                    "national_wins": None,
                    "national_losses": None,
                })
        else:
            # Fall back to PlayerEventStats if no games indexed
            # Get stats first, then get player names separately to avoid schema issues
            stats_query = select(PlayerEventStats).where(PlayerEventStats.event_id == event_id)
            stats_result = await db.execute(stats_query)
            stats = stats_result.scalars().all()
            
            # Get unique player IDs
            player_ids = list(set([stat.player_id for stat in stats]))
            
            # Get player names - use simple query without complex joins
            player_names = {}
            if player_ids:
                try:
                    # Try to get latest snapshot per player
                    from sqlalchemy import func
                    latest_player_dates = select(
                        Player.player_id,
                        func.max(Player.snapshot_date).label('max_date')
                    ).group_by(Player.player_id).subquery()
                    
                    # Select only columns that exist
                    latest_players_query = select(
                        Player.player_id,
                        Player.first_name,
                        Player.last_name,
                        Player.snapshot_date
                    ).join(
                        latest_player_dates,
                        and_(
                            Player.player_id == latest_player_dates.c.player_id,
                            Player.snapshot_date == latest_player_dates.c.max_date
                        )
                    ).where(Player.player_id.in_(player_ids))
                    
                    players_result = await db.execute(latest_players_query)
                    for row in players_result.all():
                        player_names[row[0]] = (row[1] or "", row[2] or "")
                except Exception as e:
                    # Fallback: just get any player record
                    print(f"Error getting latest player snapshots: {e}")
                    simple_players_query = select(
                        Player.player_id,
                        Player.first_name,
                        Player.last_name
                    ).where(Player.player_id.in_(player_ids)).distinct(Player.player_id)
                    players_result = await db.execute(simple_players_query)
                    for row in players_result.all():
                        if row[0] not in player_names:
                            player_names[row[0]] = (row[1] or "", row[2] or "")
            
            # Format stats rows
            stats_rows = [(stat, player_names.get(stat.player_id, ("", ""))[0], player_names.get(stat.player_id, ("", ""))[1]) for stat in stats]
            
            # Format player stats
            stats_list = []
            for row in stats_rows:
                stat = row[0]  # PlayerEventStats object
                first_name = row[1] or ""
                last_name = row[2] or ""
                
                stats_list.append({
                    "player_id": stat.player_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "rank": stat.rank if stat.rank else None,
                    "pts_per_rnd": stat.pts_per_rnd if stat.pts_per_rnd else None,
                    "dpr": stat.dpr if stat.dpr else None,
                    "player_cpi": None,  # Not in PlayerEventStats
                    "win_pct": stat.win_pct,
                    "total_games": stat.total_games,
                    "rounds_total": stat.rounds_played,  # Map rounds_played to rounds_total
                    "total_pts": stat.total_pts,
                    "opponent_pts_per_rnd": stat.opponent_pts_per_rnd,
                    "opponent_pts_total": stat.opponent_pts_total,
                    "four_bagger_pct": stat.four_bagger_pct,
                    "bags_in_pct": stat.bags_in_pct,
                    "bags_on_pct": stat.bags_on_pct,
                    "bags_off_pct": stat.bags_off_pct,
                    "local_wins": None,  # Not in PlayerEventStats
                    "local_losses": None,
                    "regional_wins": None,
                    "regional_losses": None,
                    "open_wins": stat.wins if stat.wins else 0,  # Use wins/losses from event
                    "open_losses": stat.losses if stat.losses else 0,
                    "national_wins": None,
                    "national_losses": None,
                })
        
        # Format standings
        standings_list = []
        for standing in standings:
            standings_list.append({
                "player_id": standing.player_id,
                "final_rank": standing.final_rank,
                "points": standing.points,
            })
        
        season_name = get_season_name(event.bucket_id)
        
        return {
            "event": {
                "event_id": event.event_id,
                "event_name": event.event_name,
                "base_event_name": event.base_event_name,
                "bracket_name": event.bracket_name,
                "event_type": event.event_type,
                "event_date": event.event_date.isoformat() if event.event_date else None,
                "location": event.location,
                "city": event.city,
                "state": event.state,
                "bucket_id": event.bucket_id,
                "season": season_name,
                "event_number": event.event_number,
                "is_signature": event.is_signature,
            },
            "player_stats": stats_list,
            "standings": standings_list,
            "total_players": len(stats_list),
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching event details: {str(e)}")

@app.get("/api/events/{event_id}/game-stats")
async def get_event_game_stats(event_id: int, db: AsyncSession = Depends(get_db)):
    """Calculate tournament-long average stats per player from EventGame records."""
    try:
        from database import EventGame, Player
        from sqlalchemy import func, case, or_
        
        # Get all games for this event
        games_query = select(EventGame).where(EventGame.event_id == event_id)
        games_result = await db.execute(games_query)
        games = games_result.scalars().all()
        
        if not games:
            return {
                "event_id": event_id,
                "player_stats": [],
                "total_games": 0
            }
        
        # Aggregate stats by player
        # We need to handle both player1 and player2 positions
        player_stats = {}
        
        for game in games:
            # Process player 1 stats
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
                        "points": 0,
                        "rounds": 0,
                        "bags_in": 0,
                        "bags_on": 0,
                        "bags_off": 0,
                        "bags_thrown": 0,
                        "four_baggers": 0,
                        "ppr": 0.0,
                        "bags_in_pct": 0.0,
                        "bags_on_pct": 0.0,
                        "bags_off_pct": 0.0,
                        "four_bagger_pct": 0.0,
                        "opponent_points": 0,
                        "opponent_ppr": 0.0,
                        "wins": 0,
                        "losses": 0
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
                
                # Track wins/losses (player1 wins if player1_points > player2_points)
                if game.player1_points and game.player2_points:
                    if game.player1_points > game.player2_points:
                        p["wins"] += 1
                    else:
                        p["losses"] += 1
            
            # Process player 2 stats
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
                        "points": 0,
                        "rounds": 0,
                        "bags_in": 0,
                        "bags_on": 0,
                        "bags_off": 0,
                        "bags_thrown": 0,
                        "four_baggers": 0,
                        "ppr": 0.0,
                        "bags_in_pct": 0.0,
                        "bags_on_pct": 0.0,
                        "bags_off_pct": 0.0,
                        "four_bagger_pct": 0.0,
                        "opponent_points": 0,
                        "opponent_ppr": 0.0,
                        "wins": 0,
                        "losses": 0
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
                
                # Track wins/losses (player2 wins if player2_points > player1_points)
                if game.player1_points and game.player2_points:
                    if game.player2_points > game.player1_points:
                        p["wins"] += 1
                    else:
                        p["losses"] += 1
        
        # Get event standings for rank and player data in parallel
        from database import EventStanding
        from sqlalchemy import func, and_
        
        player_ids_list = list(player_stats.keys())
        
        # Batch queries: get standings, player names, and CPI in parallel
        standings_query = select(EventStanding).where(EventStanding.event_id == event_id).order_by(EventStanding.final_rank)
        standings_task = db.execute(standings_query)
        
        # Get all player data at once (names and CPI)
        player_data_dict = {}
        if player_ids_list:
            try:
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
                
                players_task = db.execute(latest_players_query)
            except Exception as e:
                print(f"Error setting up player query: {e}")
                players_task = None
        else:
            players_task = None
        
        # Wait for both queries
        standings_result = await standings_task
        standings = standings_result.scalars().all()
        standings_dict = {standing.player_id: standing for standing in standings}
        
        player_cpi_dict = {}
        player_names_dict = {}
        if players_task:
            try:
                players_result = await players_task
                for row in players_result.all():
                    player_id = row[0]
                    player_names_dict[player_id] = (row[1] or "", row[2] or "")
                    if row[3] is not None:
                        player_cpi_dict[player_id] = row[3]
            except Exception as e:
                print(f"Error getting player data: {e}")
        
        # Calculate averages and get player names
        stats_list = []
        for player_id, stats in player_stats.items():
            games_played = stats["games_played"]
            if games_played > 0:
                stats["points"] = stats["total_points"] / games_played
                stats["rounds_avg"] = stats["total_rounds"] / games_played  # Average rounds per game
                stats["rounds_total"] = stats["total_rounds"]  # Total rounds (not average)
                stats["bags_in"] = stats["total_bags_in"] / games_played
                stats["bags_on"] = stats["total_bags_on"] / games_played
                stats["bags_off"] = stats["total_bags_off"] / games_played
                stats["bags_thrown"] = stats["total_bags_thrown"] / games_played
                stats["four_baggers"] = stats["total_four_baggers"] / games_played
                stats["ppr"] = stats["total_points"] / stats["total_rounds"] if stats["total_rounds"] > 0 else 0.0
                stats["opponent_points"] = stats["total_opponent_points"] / games_played
                stats["opponent_ppr"] = stats["total_opponent_ppr"] / games_played if games_played > 0 else 0.0
                
                # Calculate DPR (difference between PPR and Opp PPR)
                stats["dpr"] = stats["ppr"] - stats["opponent_ppr"] if stats["ppr"] and stats["opponent_ppr"] else 0.0
                
                # Calculate percentages (as decimals 0.0-1.0, frontend will multiply by 100)
                if stats["total_bags_thrown"] > 0:
                    stats["bags_in_pct"] = stats["total_bags_in"] / stats["total_bags_thrown"]  # Decimal 0-1
                    stats["bags_on_pct"] = stats["total_bags_on"] / stats["total_bags_thrown"]  # Decimal 0-1
                    stats["bags_off_pct"] = stats["total_bags_off"] / stats["total_bags_thrown"]  # Decimal 0-1
                else:
                    stats["bags_in_pct"] = 0.0
                    stats["bags_on_pct"] = 0.0
                    stats["bags_off_pct"] = 0.0
                
                if stats["total_rounds"] > 0:
                    stats["four_bagger_pct"] = stats["total_four_baggers"] / stats["total_rounds"]  # Decimal 0-1
                else:
                    stats["four_bagger_pct"] = 0.0
                
                # Get rank from standings
                rank = None
                if player_id in standings_dict:
                    rank = standings_dict[player_id].final_rank
                
                # Get CPI from player data
                cpi = player_cpi_dict.get(player_id)
                
                # Get player name from pre-fetched data
                first_name, last_name = player_names_dict.get(player_id, ("", ""))
                if first_name and last_name:
                    player_name = f"{first_name} {last_name}"
                else:
                    player_name = f"Player {player_id}"
                
                # Calculate win percentage (as decimal 0.0-1.0, frontend will multiply by 100)
                win_pct = (stats["wins"] / games_played) if games_played > 0 else 0.0
                
                stats_list.append({
                    "player_id": player_id,
                    "player_name": player_name,
                    "rank": rank,
                    "games_played": games_played,
                    "wins": stats["wins"],
                    "losses": stats["losses"],
                    "win_pct": win_pct,  # Decimal 0-1 (frontend will format as percentage)
                    "points": round(stats["points"], 2),
                    "rounds_total": stats["rounds_total"],  # Total rounds
                    "rounds_avg": round(stats["rounds_avg"], 2),  # Average rounds per game
                    "ppr": round(stats["ppr"], 3),
                    "dpr": round(stats["dpr"], 3),  # PPR - Opp PPR
                    "player_cpi": round(cpi, 2) if cpi else None,
                    "bags_in": round(stats["bags_in"], 2),
                    "bags_on": round(stats["bags_on"], 2),
                    "bags_off": round(stats["bags_off"], 2),
                    "bags_thrown": round(stats["bags_thrown"], 2),
                    "four_baggers": round(stats["four_baggers"], 2),
                    "bags_in_pct": round(stats["bags_in_pct"], 4),  # Decimal 0-1 (frontend will format as percentage)
                    "bags_on_pct": round(stats["bags_on_pct"], 4),  # Decimal 0-1 (frontend will format as percentage)
                    "bags_off_pct": round(stats["bags_off_pct"], 4),  # Decimal 0-1 (frontend will format as percentage)
                    "four_bagger_pct": round(stats["four_bagger_pct"], 4),  # Decimal 0-1 (frontend will format as percentage)
                    "opponent_points": round(stats["opponent_points"], 2),
                    "opponent_ppr": round(stats["opponent_ppr"], 3),
                })
        
        # Sort by PPR descending
        stats_list.sort(key=lambda x: x["ppr"], reverse=True)
        
        return {
            "event_id": event_id,
            "player_stats": stats_list,
            "total_games": len(games),
            "total_players": len(stats_list)
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error calculating game stats: {str(e)}")

@app.get("/api/players/search")
async def search_players(
    q: str = Query(..., description="Search query"),
    limit: int = Query(20, description="Maximum number of results"),
    db: AsyncSession = Depends(get_db)
):
    """Search for players by name (for autocomplete)."""
    # Get latest snapshot per player for better results
    from sqlalchemy import func
    latest_dates = select(
        Player.player_id,
        func.max(Player.snapshot_date).label('max_date')
    ).group_by(Player.player_id).subquery()
    
    # Search by first name, last name, or full name
    query = select(Player.player_id, Player.first_name, Player.last_name, Player.state).join(
        latest_dates,
        and_(
            Player.player_id == latest_dates.c.player_id,
            Player.snapshot_date == latest_dates.c.max_date
        )
    ).where(
        or_(
            Player.first_name.ilike(f"%{q}%"),
            Player.last_name.ilike(f"%{q}%"),
            func.concat(Player.first_name, ' ', Player.last_name).ilike(f"%{q}%")
        )
    ).distinct().limit(limit)
    
    result = await db.execute(query)
    players = []
    for row in result.all():
        players.append({
            "player_id": row[0],
            "name": f"{row[1]} {row[2]}",
            "state": row[3] or ""
        })
    
    return {"players": players}

@app.get("/events", response_class=HTMLResponse)
async def events_page():
    """Events listing page."""
    with open("templates/events.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/events/grouped", response_class=HTMLResponse)
async def grouped_event_page():
    """Grouped event detail page (combined stats across multiple brackets)."""
    with open("templates/event_detail.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/events/{event_id}", response_class=HTMLResponse)
async def event_detail_page(event_id: int):
    """Event detail page showing player stats and standings."""
    with open("templates/event_detail.html", "r") as f:
        return HTMLResponse(content=f.read())

# NOTE: This route must come BEFORE /events/{event_id} to match URLs with .md extension
# Using a string path parameter and converting to int manually to avoid FastAPI parsing issues with .md
@app.get("/events/{event_id_str}.md", response_class=HTMLResponse)
async def event_schema_page(
    event_id_str: str = Path(..., description="Event ID as string"),
    db: AsyncSession = Depends(get_db)
):
    """Event schema/documentation page showing all available data fields and example data."""
    from fastapi.responses import Response
    import json
    
    # Convert string to int
    try:
        event_id = int(event_id_str)
    except ValueError:
        return Response(
            content=f"# Invalid Event ID\n\nEvent ID must be a number, got: {event_id_str}",
            media_type="text/markdown"
        )
    
    try:
        # Get full event data
        event_query = select(Event).where(Event.event_id == event_id)
        event_result = await db.execute(event_query)
        event = event_result.scalar_one_or_none()
        
        if not event:
            return Response(
                content=f"# Event {event_id} Not Found\n\nEvent not found in database.",
                media_type="text/markdown"
            )
        
        # Get related data counts
        from sqlalchemy import func
        from database import PlayerEventStats, EventStanding, EventMatch, EventGame, EventMatchup
        
        stats_count = await db.execute(select(func.count()).select_from(PlayerEventStats).where(PlayerEventStats.event_id == event_id))
        stats_count = stats_count.scalar() or 0
        
        standings_count = await db.execute(select(func.count()).select_from(EventStanding).where(EventStanding.event_id == event_id))
        standings_count = standings_count.scalar() or 0
        
        matches_count = await db.execute(select(func.count()).select_from(EventMatch).where(EventMatch.event_id == event_id))
        matches_count = matches_count.scalar() or 0
        
        games_count = await db.execute(select(func.count()).select_from(EventGame).where(EventGame.event_id == event_id))
        games_count = games_count.scalar() or 0
        
        matchups_count = await db.execute(select(func.count()).select_from(EventMatchup).where(EventMatchup.event_id == event_id))
        matchups_count = matchups_count.scalar() or 0
        
        # Get sample data
        sample_stats = await db.execute(
            select(PlayerEventStats).where(PlayerEventStats.event_id == event_id).limit(3)
        )
        sample_stats = sample_stats.scalars().all()
        
        sample_standings = await db.execute(
            select(EventStanding).where(EventStanding.event_id == event_id).order_by(EventStanding.final_rank).limit(5)
        )
        sample_standings = sample_standings.scalars().all()
        
        # Select matches - use try/except to handle missing raw_data column gracefully
        try:
            sample_matches = await db.execute(
                select(EventMatch).where(EventMatch.event_id == event_id).limit(3)
            )
            sample_matches = sample_matches.scalars().all()
        except Exception as e:
            # If raw_data column doesn't exist, select without it
            sample_matches_query = select(
                EventMatch.id,
                EventMatch.event_id,
                EventMatch.match_id,
                EventMatch.round_number,
                EventMatch.player1_id,
                EventMatch.player2_id,
                EventMatch.winner_id,
                EventMatch.match_status,
                EventMatch.match_status_desc,
                EventMatch.home_score,
                EventMatch.away_score,
                EventMatch.court_id,
                EventMatch.match_type,
                EventMatch.created_at
            ).where(EventMatch.event_id == event_id).limit(3)
            sample_matches_result = await db.execute(sample_matches_query)
            sample_matches_rows = sample_matches_result.all()
            
            # Convert to objects for easier access
            class MatchObj:
                def __init__(self, row):
                    self.id = row[0]
                    self.event_id = row[1]
                    self.match_id = row[2]
                    self.round_number = row[3]
                    self.player1_id = row[4]
                    self.player2_id = row[5]
                    self.winner_id = row[6]
                    self.match_status = row[7]
                    self.match_status_desc = row[8]
                    self.home_score = row[9]
                    self.away_score = row[10]
                    self.court_id = row[11]
                    self.match_type = row[12]
                    self.created_at = row[13]
                    self.raw_data = None
            
            sample_matches = [MatchObj(row) for row in sample_matches_rows]
        
        # Select games - use try/except to handle missing raw_data column gracefully
        try:
            sample_games = await db.execute(
                select(EventGame).where(EventGame.event_id == event_id).limit(2)
            )
            sample_games = sample_games.scalars().all()
        except Exception as e:
            # If raw_data column doesn't exist, select without it
            sample_games_query = select(
                EventGame.id,
                EventGame.event_id,
                EventGame.match_id,
                EventGame.game_id,
                EventGame.player1_id,
                EventGame.player2_id,
                EventGame.player1_points,
                EventGame.player1_rounds,
                EventGame.player1_bags_in,
                EventGame.player1_bags_on,
                EventGame.player1_bags_off,
                EventGame.player1_total_bags_thrown,
                EventGame.player1_four_baggers,
                EventGame.player1_ppr,
                EventGame.player1_bags_in_pct,
                EventGame.player1_bags_on_pct,
                EventGame.player1_bags_off_pct,
                EventGame.player1_four_bagger_pct,
                EventGame.player1_opponent_points,
                EventGame.player1_opponent_ppr,
                EventGame.player2_points,
                EventGame.player2_rounds,
                EventGame.player2_bags_in,
                EventGame.player2_bags_on,
                EventGame.player2_bags_off,
                EventGame.player2_total_bags_thrown,
                EventGame.player2_four_baggers,
                EventGame.player2_ppr,
                EventGame.player2_bags_in_pct,
                EventGame.player2_bags_on_pct,
                EventGame.player2_bags_off_pct,
                EventGame.player2_four_bagger_pct,
                EventGame.player2_opponent_points,
                EventGame.player2_opponent_ppr,
                EventGame.created_at
            ).where(EventGame.event_id == event_id).limit(2)
            sample_games_result = await db.execute(sample_games_query)
            sample_games_rows = sample_games_result.all()
            
            # Convert to objects for easier access
            class GameObj:
                def __init__(self, row):
                    self.id = row[0]
                    self.event_id = row[1]
                    self.match_id = row[2]
                    self.game_id = row[3]
                    self.player1_id = row[4]
                    self.player2_id = row[5]
                    self.player1_points = row[6]
                    self.player1_rounds = row[7]
                    self.player1_bags_in = row[8]
                    self.player1_bags_on = row[9]
                    self.player1_bags_off = row[10]
                    self.player1_total_bags_thrown = row[11]
                    self.player1_four_baggers = row[12]
                    self.player1_ppr = row[13]
                    self.player1_bags_in_pct = row[14]
                    self.player1_bags_on_pct = row[15]
                    self.player1_bags_off_pct = row[16]
                    self.player1_four_bagger_pct = row[17]
                    self.player1_opponent_points = row[18]
                    self.player1_opponent_ppr = row[19]
                    self.player2_points = row[20]
                    self.player2_rounds = row[21]
                    self.player2_bags_in = row[22]
                    self.player2_bags_on = row[23]
                    self.player2_bags_off = row[24]
                    self.player2_total_bags_thrown = row[25]
                    self.player2_four_baggers = row[26]
                    self.player2_ppr = row[27]
                    self.player2_bags_in_pct = row[28]
                    self.player2_bags_on_pct = row[29]
                    self.player2_bags_off_pct = row[30]
                    self.player2_four_bagger_pct = row[31]
                    self.player2_opponent_points = row[32]
                    self.player2_opponent_ppr = row[33]
                    self.created_at = row[34]
                    self.raw_data = None
            
            sample_games = [GameObj(row) for row in sample_games_rows]
        
        # Build markdown content
        md_content = f"""# Event Data Schema & Documentation

## Event: {event.event_name or f'Event {event_id}'}

**Event ID:** `{event_id}`  
**View Event:** [http://localhost:8000/events/{event_id}](http://localhost:8000/events/{event_id})

---

## Event Table Schema

The `events` table contains the following fields:

| Field | Type | Description | Example Value |
|-------|------|-------------|---------------|
| `id` | Integer | Primary key (internal) | `{event.id}` |
| `event_id` | Integer | Unique API event ID | `{event.event_id}` |
| `event_name` | String | Full event name | `{event.event_name or 'N/A'}` |
| `base_event_name` | String | Normalized base name | `{event.base_event_name or 'N/A'}` |
| `bracket_name` | String | Specific bracket name | `{event.bracket_name or 'N/A'}` |
| `event_type` | String | Event type (open/regional/national/signature) | `{event.event_type or 'N/A'}` |
| `event_date` | Date | Event date | `{event.event_date.isoformat() if event.event_date else 'N/A'}` |
| `location` | String | Venue name | `{event.location or 'N/A'}` |
| `city` | String | City | `{event.city or 'N/A'}` |
| `state` | String | State/Province | `{event.state or 'N/A'}` |
| `bucket_id` | Integer | Season/bucket ID | `{event.bucket_id or 'N/A'}` |
| `region` | String | Region (us/canada) | `{event.region or 'N/A'}` |
| `event_number` | Integer | Event number (e.g., Open #2) | `{event.event_number or 'N/A'}` |
| `is_signature` | Integer | 1 if signature event | `{event.is_signature or 0}` |
| `event_group_id` | Integer | Groups related brackets | `{event.event_group_id or 'N/A'}` |
| `games_fully_indexed` | Boolean | All games indexed? | `{event.games_fully_indexed or False}` |
| `games_indexed_count` | Integer | Number of games indexed | `{event.games_indexed_count or 0}` |
| `games_total_count` | Integer | Total expected games | `{event.games_total_count or 0}` |
| `games_indexed_at` | DateTime | When games were indexed | `{event.games_indexed_at.isoformat() if event.games_indexed_at else 'N/A'}` |
| `created_at` | DateTime | Record creation time | `{event.created_at.isoformat() if event.created_at else 'N/A'}` |
| `updated_at` | DateTime | Last update time | `{event.updated_at.isoformat() if event.updated_at else 'N/A'}` |

### Current Event Data

```json
{json.dumps({
    "event_id": event.event_id,
    "event_name": event.event_name,
    "base_event_name": event.base_event_name,
    "bracket_name": event.bracket_name,
    "event_type": event.event_type,
    "event_date": event.event_date.isoformat() if event.event_date else None,
    "location": event.location,
    "city": event.city,
    "state": event.state,
    "bucket_id": event.bucket_id,
    "region": event.region,
    "event_number": event.event_number,
    "is_signature": event.is_signature,
    "event_group_id": event.event_group_id,
    "games_fully_indexed": event.games_fully_indexed,
    "games_indexed_count": event.games_indexed_count,
    "games_total_count": event.games_total_count,
    "games_indexed_at": event.games_indexed_at.isoformat() if event.games_indexed_at else None,
}, indent=2)}
```

---

## Related Tables

### Player Event Stats (`player_event_stats`)

**Count for this event:** {stats_count}

| Field | Type | Description |
|-------|------|-------------|
| `id` | Integer | Primary key |
| `event_id` | Integer | Event ID (FK) |
| `player_id` | Integer | Player ID (FK) |
| `rank` | Integer | Final rank in event |
| `pts_per_rnd` | Float | Points per round (PPR) |
| `dpr` | Float | Defensive points per round |
| `total_games` | Integer | Total games played |
| `wins` | Integer | Number of wins |
| `losses` | Integer | Number of losses |
| `win_pct` | Float | Win percentage |
| `rounds_played` | Integer | Total rounds played |
| `total_pts` | Integer | Total points scored |
| `opponent_pts_per_rnd` | Float | Opponent PPR average |
| `opponent_pts_total` | Integer | Total opponent points |
| `four_bagger_pct` | Float | Four bagger percentage |
| `bags_in_pct` | Float | Bags in percentage |
| `bags_on_pct` | Float | Bags on percentage |
| `bags_off_pct` | Float | Bags off percentage |

**Sample Records:**
"""
        
        if sample_stats:
            for stat in sample_stats:
                md_content += f"""
```json
{json.dumps({
    "player_id": stat.player_id,
    "rank": stat.rank,
    "pts_per_rnd": stat.pts_per_rnd,
    "dpr": stat.dpr,
    "wins": stat.wins,
    "losses": stat.losses,
    "win_pct": stat.win_pct,
    "total_games": stat.total_games,
    "rounds_played": stat.rounds_played,
    "total_pts": stat.total_pts,
    "four_bagger_pct": stat.four_bagger_pct,
    "bags_in_pct": stat.bags_in_pct,
    "bags_on_pct": stat.bags_on_pct,
    "bags_off_pct": stat.bags_off_pct,
}, indent=2)}
```
"""
        else:
            md_content += "\n*No player stats indexed yet.*\n"
        
        md_content += f"""
---

### Event Standings (`event_standings`)

**Count for this event:** {standings_count}

| Field | Type | Description |
|-------|------|-------------|
| `id` | Integer | Primary key |
| `event_id` | Integer | Event ID (FK) |
| `player_id` | Integer | Player ID (FK) |
| `final_rank` | Integer | Final ranking position |
| `points` | Float | Total points earned |

**Sample Records:**
"""
        
        if sample_standings:
            for standing in sample_standings:
                md_content += f"""
```json
{json.dumps({
    "player_id": standing.player_id,
    "final_rank": standing.final_rank,
    "points": standing.points,
}, indent=2)}
```
"""
        else:
            md_content += "\n*No standings indexed yet.*\n"
        
        md_content += f"""
---

### Event Matchups (`event_matchups`)

**Count for this event:** {matchups_count}

| Field | Type | Description |
|-------|------|-------------|
| `id` | Integer | Primary key |
| `event_id` | Integer | Event ID (FK) |
| `round_number` | Integer | Round number |
| `player1_id` | Integer | Player 1 ID |
| `player2_id` | Integer | Player 2 ID |
| `winner_id` | Integer | Winner player ID |
| `loser_id` | Integer | Loser player ID |
| `score` | String | Match score (e.g., "21-15") |
| `player1_score` | Integer | Player 1 score |
| `player2_score` | Integer | Player 2 score |

---

### Event Matches (`event_matches`)

**Count for this event:** {matches_count}

| Field | Type | Description |
|-------|------|-------------|
| `id` | Integer | Primary key |
| `event_id` | Integer | Event ID (FK) |
| `match_id` | Integer | API match ID |
| `round_number` | Integer | Round number |
| `player1_id` | Integer | Player 1 ID |
| `player2_id` | Integer | Player 2 ID |
| `winner_id` | Integer | Winner player ID |
| `match_status` | Integer | Match status (5 = completed) |
| `match_status_desc` | String | Status description |
| `home_score` | Integer | Home score |
| `away_score` | Integer | Away score |
| `court_id` | Integer | Court ID |
| `match_type` | String | Match type ("S" = singles, "D" = doubles) |
| `raw_data` | JSON | Complete raw API response |

**Sample Records:**
"""
        
        if sample_matches:
            for match in sample_matches:
                match_dict = {
                    "match_id": match.match_id,
                    "round_number": match.round_number,
                    "player1_id": match.player1_id,
                    "player2_id": match.player2_id,
                    "winner_id": match.winner_id,
                    "match_status": match.match_status,
                    "match_status_desc": match.match_status_desc,
                    "home_score": match.home_score,
                    "away_score": match.away_score,
                    "court_id": match.court_id,
                    "match_type": match.match_type,
                    "raw_data": "Not available" if not match.raw_data else "Present (JSON object)",
                }
                md_content += f"""
```json
{json.dumps(match_dict, indent=2, default=str)}
```
"""
        else:
            md_content += "\n*No matches indexed yet.*\n"
        
        md_content += f"""
---

### Event Games (`event_games`)

**Count for this event:** {games_count}

| Field | Type | Description |
|-------|------|-------------|
| `id` | Integer | Primary key |
| `event_id` | Integer | Event ID (FK) |
| `match_id` | Integer | Match ID (FK) |
| `game_id` | Integer | Game ID within match |
| `player1_id` | Integer | Player 1 ID |
| `player2_id` | Integer | Player 2 ID |
| `player1_points` | Integer | Player 1 points |
| `player1_rounds` | Integer | Player 1 rounds |
| `player1_bags_in` | Integer | Player 1 bags in |
| `player1_bags_on` | Integer | Player 1 bags on |
| `player1_bags_off` | Integer | Player 1 bags off |
| `player1_total_bags_thrown` | Integer | Player 1 total bags |
| `player1_four_baggers` | Integer | Player 1 four baggers |
| `player1_ppr` | Float | Player 1 points per round |
| `player1_bags_in_pct` | Float | Player 1 bags in % |
| `player1_bags_on_pct` | Float | Player 1 bags on % |
| `player1_bags_off_pct` | Float | Player 1 bags off % |
| `player1_four_bagger_pct` | Float | Player 1 four bagger % |
| `player1_opponent_points` | Integer | Player 1 opponent points |
| `player1_opponent_ppr` | Float | Player 1 opponent PPR |
| `player2_points` | Integer | Player 2 points |
| `player2_rounds` | Integer | Player 2 rounds |
| `player2_bags_in` | Integer | Player 2 bags in |
| `player2_bags_on` | Integer | Player 2 bags on |
| `player2_bags_off` | Integer | Player 2 bags off |
| `player2_total_bags_thrown` | Integer | Player 2 total bags |
| `player2_four_baggers` | Integer | Player 2 four baggers |
| `player2_ppr` | Float | Player 2 points per round |
| `player2_bags_in_pct` | Float | Player 2 bags in % |
| `player2_bags_on_pct` | Float | Player 2 bags on % |
| `player2_bags_off_pct` | Float | Player 2 bags off % |
| `player2_four_bagger_pct` | Float | Player 2 four bagger % |
| `player2_opponent_points` | Integer | Player 2 opponent points |
| `player2_opponent_ppr` | Float | Player 2 opponent PPR |
| `raw_data` | JSON | Complete raw API response |

**Sample Records:**
"""
        
        if sample_games:
            for game in sample_games:
                game_dict = {
                    "match_id": game.match_id,
                    "game_id": game.game_id,
                    "player1_id": game.player1_id,
                    "player2_id": game.player2_id,
                    "player1_points": game.player1_points,
                    "player1_rounds": game.player1_rounds,
                    "player1_ppr": game.player1_ppr,
                    "player1_bags_in_pct": game.player1_bags_in_pct,
                    "player1_four_bagger_pct": game.player1_four_bagger_pct,
                    "player2_points": game.player2_points,
                    "player2_rounds": game.player2_rounds,
                    "player2_ppr": game.player2_ppr,
                    "player2_bags_in_pct": game.player2_bags_in_pct,
                    "player2_four_bagger_pct": game.player2_four_bagger_pct,
                }
                if game.raw_data:
                    game_dict["raw_data"] = "Present (JSON object)"
                md_content += f"""
```json
{json.dumps(game_dict, indent=2)}
```
"""
        else:
            md_content += "\n*No games indexed yet.*\n"
        
        md_content += f"""
---

## API Endpoints

- **Event Details:** `GET /api/events/{{event_id}}`
- **Event Games Count:** `GET /api/events/{{event_id}}/games-count`
- **Event Stats:** `GET /api/events/stats`

## Notes

- All tables include `raw_data` JSON columns that store complete API responses to preserve all available data
- The `events` table tracks indexing status via `games_fully_indexed`, `games_indexed_count`, and `games_total_count`
- Related data is linked via `event_id` foreign keys
- Player data is linked via `player_id` foreign keys

---

*Generated for event {event_id}*
"""
        
        return Response(content=md_content, media_type="text/markdown")
        
    except Exception as e:
        import traceback
        error_msg = f"# Error\n\nError generating schema: {str(e)}\n\n```\n{traceback.format_exc()}\n```"
        return Response(content=error_msg, media_type="text/markdown")

@app.get("/events/{event_id}/games/{match_id}/{game_id}.md", response_class=HTMLResponse)
async def game_schema_page(
    event_id: int,
    match_id: int,
    game_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Game schema/documentation page showing all available data fields and example data."""
    from fastapi.responses import Response
    import json
    
    try:
        # Get game data
        from database import EventGame, EventMatch, Event, Player
        from sqlalchemy import func
        
        game_query = select(EventGame).where(
            EventGame.event_id == event_id,
            EventGame.match_id == match_id,
            EventGame.game_id == game_id
        )
        game_result = await db.execute(game_query)
        game = game_result.scalar_one_or_none()
        
        if not game:
            return Response(
                content=f"# Game Not Found\n\nGame not found: Event {event_id}, Match {match_id}, Game {game_id}",
                media_type="text/markdown"
            )
        
        # Get related match data
        match_query = select(EventMatch).where(
            EventMatch.event_id == event_id,
            EventMatch.match_id == match_id
        )
        match_result = await db.execute(match_query)
        match = match_result.scalar_one_or_none()
        
        # Get event data
        event_query = select(Event).where(Event.event_id == event_id)
        event_result = await db.execute(event_query)
        event = event_result.scalar_one_or_none()
        
        # Get player names if available
        player1_name = None
        player2_name = None
        if game.player1_id:
            player1_query = select(Player.first_name, Player.last_name).where(
                Player.player_id == game.player1_id
            ).order_by(Player.snapshot_date.desc()).limit(1)
            player1_result = await db.execute(player1_query)
            player1_row = player1_result.first()
            if player1_row:
                player1_name = f"{player1_row[0] or ''} {player1_row[1] or ''}".strip() or f"Player {game.player1_id}"
        
        if game.player2_id:
            player2_query = select(Player.first_name, Player.last_name).where(
                Player.player_id == game.player2_id
            ).order_by(Player.snapshot_date.desc()).limit(1)
            player2_result = await db.execute(player2_query)
            player2_row = player2_result.first()
            if player2_row:
                player2_name = f"{player2_row[0] or ''} {player2_row[1] or ''}".strip() or f"Player {game.player2_id}"
        
        # Build markdown content
        md_content = f"""# Game Data Schema & Documentation

## Game: Event {event_id}, Match {match_id}, Game {game_id}

**Event:** {event.event_name if event else f'Event {event_id}'}  
**View Event:** [http://localhost:8000/events/{event_id}](http://localhost:8000/events/{event_id})  
**View Event Schema:** [http://localhost:8000/events/{event_id}.md](http://localhost:8000/events/{event_id}.md)

**Players:**
- **Player 1:** {player1_name or f'Player {game.player1_id}'} (ID: `{game.player1_id}`)
- **Player 2:** {player2_name or f'Player {game.player2_id}'} (ID: `{game.player2_id}`)

---

## Game Table Schema (`event_games`)

The `event_games` table contains detailed game-level statistics for each game within a match.

| Field | Type | Description | Current Value |
|-------|------|-------------|---------------|
| `id` | Integer | Primary key (internal) | `{game.id}` |
| `event_id` | Integer | Event ID (FK) | `{game.event_id}` |
| `match_id` | Integer | Match ID (FK) | `{game.match_id}` |
| `game_id` | Integer | Game ID within match | `{game.game_id}` |
| `player1_id` | Integer | Player 1 ID (FK) | `{game.player1_id or 'N/A'}` |
| `player2_id` | Integer | Player 2 ID (FK) | `{game.player2_id or 'N/A'}` |

### Player 1 Statistics

| Field | Type | Description | Current Value |
|-------|------|-------------|---------------|
| `player1_points` | Integer | Total points scored | `{game.player1_points or 'N/A'}` |
| `player1_rounds` | Integer | Rounds played | `{game.player1_rounds or 'N/A'}` |
| `player1_bags_in` | Integer | Bags that went in | `{game.player1_bags_in or 'N/A'}` |
| `player1_bags_on` | Integer | Bags that landed on board | `{game.player1_bags_on or 'N/A'}` |
| `player1_bags_off` | Integer | Bags that missed | `{game.player1_bags_off or 'N/A'}` |
| `player1_total_bags_thrown` | Integer | Total bags thrown | `{game.player1_total_bags_thrown or 'N/A'}` |
| `player1_four_baggers` | Integer | Number of four baggers | `{game.player1_four_baggers or 'N/A'}` |
| `player1_ppr` | Float | Points per round | `{game.player1_ppr or 'N/A'}` |
| `player1_bags_in_pct` | Float | Bags in percentage | `{game.player1_bags_in_pct or 'N/A'}` |
| `player1_bags_on_pct` | Float | Bags on percentage | `{game.player1_bags_on_pct or 'N/A'}` |
| `player1_bags_off_pct` | Float | Bags off percentage | `{game.player1_bags_off_pct or 'N/A'}` |
| `player1_four_bagger_pct` | Float | Four bagger percentage | `{game.player1_four_bagger_pct or 'N/A'}` |
| `player1_opponent_points` | Integer | Opponent's points | `{game.player1_opponent_points or 'N/A'}` |
| `player1_opponent_ppr` | Float | Opponent's PPR | `{game.player1_opponent_ppr or 'N/A'}` |

### Player 2 Statistics

| Field | Type | Description | Current Value |
|-------|------|-------------|---------------|
| `player2_points` | Integer | Total points scored | `{game.player2_points or 'N/A'}` |
| `player2_rounds` | Integer | Rounds played | `{game.player2_rounds or 'N/A'}` |
| `player2_bags_in` | Integer | Bags that went in | `{game.player2_bags_in or 'N/A'}` |
| `player2_bags_on` | Integer | Bags that landed on board | `{game.player2_bags_on or 'N/A'}` |
| `player2_bags_off` | Integer | Bags that missed | `{game.player2_bags_off or 'N/A'}` |
| `player2_total_bags_thrown` | Integer | Total bags thrown | `{game.player2_total_bags_thrown or 'N/A'}` |
| `player2_four_baggers` | Integer | Number of four baggers | `{game.player2_four_baggers or 'N/A'}` |
| `player2_ppr` | Float | Points per round | `{game.player2_ppr or 'N/A'}` |
| `player2_bags_in_pct` | Float | Bags in percentage | `{game.player2_bags_in_pct or 'N/A'}` |
| `player2_bags_on_pct` | Float | Bags on percentage | `{game.player2_bags_on_pct or 'N/A'}` |
| `player2_bags_off_pct` | Float | Bags off percentage | `{game.player2_bags_off_pct or 'N/A'}` |
| `player2_four_bagger_pct` | Float | Four bagger percentage | `{game.player2_four_bagger_pct or 'N/A'}` |
| `player2_opponent_points` | Integer | Opponent's points | `{game.player2_opponent_points or 'N/A'}` |
| `player2_opponent_ppr` | Float | Opponent's PPR | `{game.player2_opponent_ppr or 'N/A'}` |

### Metadata

| Field | Type | Description | Current Value |
|-------|------|-------------|---------------|
| `raw_data` | JSON | Complete raw API response | {'Present' if game.raw_data else 'Not available'} |
| `created_at` | DateTime | Record creation time | `{game.created_at.isoformat() if game.created_at else 'N/A'}` |

### Current Game Data

```json
{json.dumps({
    "id": game.id,
    "event_id": game.event_id,
    "match_id": game.match_id,
    "game_id": game.game_id,
    "player1_id": game.player1_id,
    "player2_id": game.player2_id,
    "player1_points": game.player1_points,
    "player1_rounds": game.player1_rounds,
    "player1_bags_in": game.player1_bags_in,
    "player1_bags_on": game.player1_bags_on,
    "player1_bags_off": game.player1_bags_off,
    "player1_total_bags_thrown": game.player1_total_bags_thrown,
    "player1_four_baggers": game.player1_four_baggers,
    "player1_ppr": game.player1_ppr,
    "player1_bags_in_pct": game.player1_bags_in_pct,
    "player1_bags_on_pct": game.player1_bags_on_pct,
    "player1_bags_off_pct": game.player1_bags_off_pct,
    "player1_four_bagger_pct": game.player1_four_bagger_pct,
    "player1_opponent_points": game.player1_opponent_points,
    "player1_opponent_ppr": game.player1_opponent_ppr,
    "player2_points": game.player2_points,
    "player2_rounds": game.player2_rounds,
    "player2_bags_in": game.player2_bags_in,
    "player2_bags_on": game.player2_bags_on,
    "player2_bags_off": game.player2_bags_off,
    "player2_total_bags_thrown": game.player2_total_bags_thrown,
    "player2_four_baggers": game.player2_four_baggers,
    "player2_ppr": game.player2_ppr,
    "player2_bags_in_pct": game.player2_bags_in_pct,
    "player2_bags_on_pct": game.player2_bags_on_pct,
    "player2_bags_off_pct": game.player2_bags_off_pct,
    "player2_four_bagger_pct": game.player2_four_bagger_pct,
    "player2_opponent_points": game.player2_opponent_points,
    "player2_opponent_ppr": game.player2_opponent_ppr,
    "raw_data": "Present" if game.raw_data else None,
    "created_at": game.created_at.isoformat() if game.created_at else None,
}, indent=2, default=str)}
```

---

## Related Match Data (`event_matches`)

"""
        
        if match:
            md_content += f"""**Match Information:**

| Field | Value |
|-------|-------|
| Match ID | `{match.match_id}` |
| Round Number | `{match.round_number or 'N/A'}` |
| Player 1 ID | `{match.player1_id or 'N/A'}` |
| Player 2 ID | `{match.player2_id or 'N/A'}` |
| Winner ID | `{match.winner_id or 'N/A'}` |
| Match Status | `{match.match_status or 'N/A'}` ({match.match_status_desc or 'N/A'}) |
| Home Score | `{match.home_score or 'N/A'}` |
| Away Score | `{match.away_score or 'N/A'}` |
| Court ID | `{match.court_id or 'N/A'}` |
| Match Type | `{match.match_type or 'N/A'}` (S=Singles, D=Doubles) |

**Match Data:**
```json
{json.dumps({
    "match_id": match.match_id,
    "round_number": match.round_number,
    "player1_id": match.player1_id,
    "player2_id": match.player2_id,
    "winner_id": match.winner_id,
    "match_status": match.match_status,
    "match_status_desc": match.match_status_desc,
    "home_score": match.home_score,
    "away_score": match.away_score,
    "court_id": match.court_id,
    "match_type": match.match_type,
    "raw_data": "Present" if match.raw_data else None,
}, indent=2, default=str)}
```

"""
        else:
            md_content += "*No match data found for this game.*\n\n"
        
        md_content += f"""
---

## Related Event Data

"""
        
        if event:
            md_content += f"""**Event:** {event.event_name}  
**Type:** {event.event_type}  
**Date:** {event.event_date.isoformat() if event.event_date else 'N/A'}  
**Location:** {event.location or 'N/A'} ({event.city or ''}, {event.state or ''})

"""
        else:
            md_content += "*Event data not available.*\n\n"
        
        md_content += f"""
---

## Raw Data

"""
        
        if game.raw_data:
            md_content += f"""The `raw_data` field contains the complete raw API response from the match stats endpoint:

```json
{json.dumps(game.raw_data, indent=2, default=str)}
```

**Note:** This preserves all original data from the API, even if not all fields are stored in structured columns.

"""
        else:
            md_content += "*No raw data available for this game.*\n\n"
        
        md_content += f"""
---

## Data Relationships

- **Event → Matches → Games**: Each event can have multiple matches, each match can have multiple games
- **Game ID**: Usually 1, but can be higher for multi-game matches
- **Match ID**: Unique within an event, links games to their match
- **Player IDs**: Link to the `players` table for player information

## Notes

- All game statistics are calculated per-game
- `raw_data` JSON column stores the complete API response to preserve all available data
- Opponent statistics (opponent_points, opponent_ppr) represent the other player's performance
- Percentages are calculated from total bags thrown
- PPR (Points Per Round) is a key performance metric

---

*Generated for Event {event_id}, Match {match_id}, Game {game_id}*
"""
        
        return Response(content=md_content, media_type="text/markdown")
        
    except Exception as e:
        import traceback
        error_msg = f"# Error\n\nError generating game schema: {str(e)}\n\n```\n{traceback.format_exc()}\n```"
        return Response(content=error_msg, media_type="text/markdown")

# ============================================================================
# Games Listing and Head-to-Head Matchup Endpoints
# ============================================================================

@app.get("/api/games")
async def get_games(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    player1_id: Optional[int] = Query(None),
    player2_id: Optional[int] = Query(None),
    event_id: Optional[int] = Query(None),
    sort_by: Optional[str] = Query("date", description="Sort by: 'date', 'rounds', 'cpi'"),
    db: AsyncSession = Depends(get_db)
):
    """Get paginated list of games with optional filters."""
    try:
        # Build base query
        query = select(EventGame)
        
        # Apply filters
        conditions = []
        if player1_id:
            # Filter for games where player1_id appears as either player1 or player2
            # For doubles, we'll also need to check raw_data, but start with the basic filter
            conditions.append(
                or_(
                    EventGame.player1_id == player1_id,
                    EventGame.player2_id == player1_id
                )
            )
        if player2_id:
            # Filter for games where player2_id appears as either player1 or player2
            conditions.append(
                or_(
                    EventGame.player1_id == player2_id,
                    EventGame.player2_id == player2_id
                )
            )
        if event_id:
            conditions.append(EventGame.event_id == event_id)
        
        # Apply all conditions with AND logic
        if conditions:
            query = query.where(and_(*conditions))
        
        # Get total count (before filtering by raw_data for doubles)
        # For player filters, we'll update this after filtering by raw_data
        count_query = select(func.count()).select_from(EventGame)
        if conditions:
            count_query = count_query.where(and_(*conditions))
        count_result = await db.execute(count_query)
        total_count = count_result.scalar() or 0
        
        # For player filters, we need to check raw_data for doubles matches
        # So we fetch all matching games first, then filter, then paginate
        if player1_id or player2_id:
            # Fetch all games matching the SQL filter (before doubles check)
            result = await db.execute(query)
            all_games = result.scalars().all()
            
            # Filter by checking raw_data for doubles matches
            filtered_games = []
            search_player_ids = []
            if player1_id:
                search_player_ids.append(player1_id)
            if player2_id:
                search_player_ids.append(player2_id)
            
            for game in all_games:
                player_found = False
                
                # First check if player is in player1_id or player2_id (works for singles and some doubles)
                if player1_id and (game.player1_id == player1_id or game.player2_id == player1_id):
                    player_found = True
                if player2_id and (game.player1_id == player2_id or game.player2_id == player2_id):
                    player_found = True
                
                # For doubles, also check raw_data for all 4 players
                if not player_found and game.raw_data:
                    event_match_details = game.raw_data.get("event_match_details") or game.raw_data.get("eventMatchDetails") or []
                    if len(event_match_details) >= 4:
                        # Doubles: check all 4 players
                        for player_data in event_match_details:
                            player_id = (player_data.get("playerid") or player_data.get("player_id") or 
                                        player_data.get("playerId") or player_data.get("playerID"))
                            try:
                                pid = int(player_id) if player_id else None
                                if pid and pid in search_player_ids:
                                    player_found = True
                                    break
                            except (ValueError, TypeError):
                                pass
                
                if player_found:
                    filtered_games.append(game)
            
            # Update total count after filtering
            total_count = len(filtered_games)
            
            # Apply sorting to filtered games (CPI sorting happens later after we get player CPI data)
            if sort_by == "rounds":
                filtered_games.sort(key=lambda g: (g.player1_rounds or 0) + (g.player2_rounds or 0), reverse=True)
            else:
                # Sort by date (default) - CPI sorting will happen after we fetch CPI data
                from datetime import datetime as dt
                filtered_games.sort(key=lambda g: g.created_at if g.created_at else dt.min, reverse=True)
            
            # Paginate the filtered and sorted list
            games = filtered_games[(page - 1) * page_size:page * page_size]
        else:
            # No player filters - can apply ordering and pagination at SQL level
            if sort_by == "rounds":
                query = query.order_by(
                    desc(EventGame.player1_rounds + EventGame.player2_rounds),
                    EventGame.created_at.desc()
                )
            elif sort_by == "cpi":
                query = query.order_by(EventGame.created_at.desc())
            else:
                query = query.order_by(EventGame.created_at.desc())
            
            query = query.offset((page - 1) * page_size).limit(page_size)
            result = await db.execute(query)
            games = result.scalars().all()
        
        # Get unique player IDs and event IDs
        player_ids = set()
        event_ids = set()
        for game in games:
            if game.player1_id:
                player_ids.add(game.player1_id)
            if game.player2_id:
                player_ids.add(game.player2_id)
            event_ids.add(game.event_id)
        
        # Get player names and CPI
        player_names = {}
        player_cpis = {}
        if player_ids:
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
            ).where(Player.player_id.in_(player_ids))
            
            players_result = await db.execute(latest_players_query)
            for row in players_result.all():
                player_names[row[0]] = f"{row[1] or ''} {row[2] or ''}".strip() or f"Player {row[0]}"
                if row[3] is not None:
                    player_cpis[row[0]] = row[3]
        
        # Get event names
        event_names = {}
        if event_ids:
            events_query = select(Event.event_id, Event.event_name, Event.base_event_name, Event.bracket_name)
            events_query = events_query.where(Event.event_id.in_(event_ids))
            events_result = await db.execute(events_query)
            for row in events_result.all():
                event_id_val, event_name_val, base_event_name_val, bracket_name_val = row
                event_names[event_id_val] = {
                    "event_name": event_name_val or f"Event {event_id_val}",
                    "base_event_name": base_event_name_val,
                    "bracket_name": bracket_name_val
                }
        
        # Get match scores for games
        match_ids = set([game.match_id for game in games])
        match_scores = {}
        if match_ids:
            matches_query = select(EventMatch.match_id, EventMatch.home_score, EventMatch.away_score, EventMatch.player1_id, EventMatch.player2_id)
            matches_query = matches_query.where(
                and_(
                    EventMatch.event_id.in_(event_ids),
                    EventMatch.match_id.in_(match_ids)
                )
            )
            matches_result = await db.execute(matches_query)
            for row in matches_result.all():
                match_id_val, home_score, away_score, match_p1_id, match_p2_id = row
                match_scores[match_id_val] = {
                    "home_score": home_score,
                    "away_score": away_score,
                    "player1_id": match_p1_id,
                    "player2_id": match_p2_id
                }
        
        # Format games with player names and event info
        games_list = []
        for game in games:
            # Calculate total rounds for sorting
            total_rounds = (game.player1_rounds or 0) + (game.player2_rounds or 0)
            
            # Calculate combined/average CPI for sorting
            cpi1 = player_cpis.get(game.player1_id, 0) if game.player1_id else 0
            cpi2 = player_cpis.get(game.player2_id, 0) if game.player2_id else 0
            combined_cpi = (cpi1 + cpi2) / 2 if (cpi1 > 0 and cpi2 > 0) else max(cpi1, cpi2)
            player1_name = player_names.get(game.player1_id, f"Player {game.player1_id}") if game.player1_id else None
            player2_name = player_names.get(game.player2_id, f"Player {game.player2_id}") if game.player2_id else None
            event_info = event_names.get(game.event_id, {"event_name": f"Event {game.event_id}"})
            
            # Get game score from match data or calculate from raw_data
            game_score_p1 = None
            game_score_p2 = None
            match_info = match_scores.get(game.match_id)
            
            if match_info:
                # Match has home_score and away_score
                # Need to determine which player is home/away
                if match_info["player1_id"] == game.player1_id:
                    game_score_p1 = match_info["home_score"]
                    game_score_p2 = match_info["away_score"]
                elif match_info["player1_id"] == game.player2_id:
                    game_score_p1 = match_info["away_score"]
                    game_score_p2 = match_info["home_score"]
                else:
                    # Fallback: use home/away as-is if player IDs don't match
                    game_score_p1 = match_info["home_score"]
                    game_score_p2 = match_info["away_score"]
            
            # If no match score, try to get from raw_data or calculate
            if game_score_p1 is None and game.raw_data:
                # Try to extract score from raw_data
                match_data = game.raw_data.get("matchData") or game.raw_data.get("match_data") or game.raw_data
                if match_data:
                    game_score_p1 = match_data.get("homeScore") or match_data.get("home_score")
                    game_score_p2 = match_data.get("awayScore") or match_data.get("away_score")
            
            # If still no score, calculate from differential scoring
            # In cornhole, score is differential - only the difference counts
            # Game ends at 21, so we need to calculate the running score
            if game_score_p1 is None:
                # Use the match's home_score/away_score if available, otherwise calculate
                # For now, use player1_points and player2_points as fallback but cap at reasonable values
                # This is a temporary solution - ideally we'd calculate from round-by-round data
                game_score_p1 = min(game.player1_points or 0, 50)  # Cap at reasonable value
                game_score_p2 = min(game.player2_points or 0, 50)
            
            # Determine winner based on game score (first to 21+ wins)
            winner_id = None
            if game_score_p1 is not None and game_score_p2 is not None:
                if game_score_p1 >= 21 and game_score_p1 > game_score_p2:
                    winner_id = game.player1_id
                elif game_score_p2 >= 21 and game_score_p2 > game_score_p1:
                    winner_id = game.player2_id
                elif game_score_p1 > game_score_p2:
                    winner_id = game.player1_id
                elif game_score_p2 > game_score_p1:
                    winner_id = game.player2_id
            
            games_list.append({
                "id": game.id,
                "event_id": game.event_id,
                "match_id": game.match_id,
                "game_id": game.game_id,
                "player1_id": game.player1_id,
                "player1_name": player1_name,
                "player2_id": game.player2_id,
                "player2_name": player2_name,
                "player1_score": game_score_p1,  # Game score (final score to 21)
                "player2_score": game_score_p2,  # Game score (final score to 21)
                "player1_points": game.player1_points,  # Total points scored (for stats)
                "player2_points": game.player2_points,  # Total points scored (for stats)
                "player1_rounds": game.player1_rounds,
                "player2_rounds": game.player2_rounds,
                "total_rounds": total_rounds,  # For sorting
                "player1_ppr": game.player1_ppr,
                "player2_ppr": game.player2_ppr,
                "player1_four_baggers": game.player1_four_baggers,
                "player2_four_baggers": game.player2_four_baggers,
                "combined_cpi": combined_cpi,  # For sorting
                "player1_cpi": cpi1,
                "player2_cpi": cpi2,
                "winner_id": winner_id,
                "event_name": event_info.get("event_name"),
                "base_event_name": event_info.get("base_event_name"),
                "bracket_name": event_info.get("bracket_name"),
                "created_at": game.created_at.isoformat() if game.created_at else None
            })
        
        # If sorting by CPI, sort the list after fetching (since we need CPI from Player table)
        if sort_by == "cpi":
            games_list.sort(key=lambda x: x.get("combined_cpi", 0), reverse=True)
        
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    
        return {
            "games": games_list,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_count": total_count,
                "total_pages": total_pages
            }
        }
    except Exception as e:
        import traceback
        print(f"Error in get_games: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error loading games: {str(e)}")

@app.get("/api/games/{game_id}")
async def get_game_details(game_id: int, db: AsyncSession = Depends(get_db)):
    """Get detailed information about a specific game."""
    # Get game
    game_query = select(EventGame).where(EventGame.id == game_id)
    game_result = await db.execute(game_query)
    game = game_result.scalar_one_or_none()
    
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    
    # Get match info
    match_query = select(EventMatch).where(
        EventMatch.event_id == game.event_id,
        EventMatch.match_id == game.match_id
    )
    match_result = await db.execute(match_query)
    match = match_result.scalar_one_or_none()
    
    # Get event info
    event_query = select(Event).where(Event.event_id == game.event_id)
    event_result = await db.execute(event_query)
    event = event_result.scalar_one_or_none()
    
    # Get player names
    player_names = {}
    player_ids = []
    if game.player1_id:
        player_ids.append(game.player1_id)
    if game.player2_id:
        player_ids.append(game.player2_id)
    
    if player_ids:
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
        ).where(Player.player_id.in_(player_ids))
        
        players_result = await db.execute(latest_players_query)
        for row in players_result.all():
            player_names[row[0]] = {
                "name": f"{row[1] or ''} {row[2] or ''}".strip() or f"Player {row[0]}",
                "cpi": row[3]
            }
    
    # Get game score from match
    game_score_p1 = None
    game_score_p2 = None
    if match:
        # Determine which player is home/away
        if match.player1_id == game.player1_id:
            game_score_p1 = match.home_score
            game_score_p2 = match.away_score
        elif match.player1_id == game.player2_id:
            game_score_p1 = match.away_score
            game_score_p2 = match.home_score
        else:
            game_score_p1 = match.home_score
            game_score_p2 = match.away_score
    
    # If no match score, try raw_data
    if game_score_p1 is None and game.raw_data:
        match_data = game.raw_data.get("matchData") or game.raw_data.get("match_data") or game.raw_data
        if match_data:
            game_score_p1 = match_data.get("homeScore") or match_data.get("home_score")
            game_score_p2 = match_data.get("awayScore") or match_data.get("away_score")
    
    # Determine winner based on game score (first to 21+ wins)
    winner_id = None
    if game_score_p1 is not None and game_score_p2 is not None:
        if game_score_p1 >= 21 and game_score_p1 > game_score_p2:
            winner_id = game.player1_id
        elif game_score_p2 >= 21 and game_score_p2 > game_score_p1:
            winner_id = game.player2_id
        elif game_score_p1 > game_score_p2:
            winner_id = game.player1_id
        elif game_score_p2 > game_score_p1:
            winner_id = game.player2_id
    
    return {
        "id": game.id,
        "event_id": game.event_id,
        "match_id": game.match_id,
        "game_id": game.game_id,
        "player1": {
            "id": game.player1_id,
            "name": player_names.get(game.player1_id, {}).get("name", f"Player {game.player1_id}") if game.player1_id else None,
            "cpi": player_names.get(game.player1_id, {}).get("cpi") if game.player1_id else None,
            "score": game_score_p1,  # Game score (to 21)
            "points": game.player1_points,  # Total points scored
            "rounds": game.player1_rounds,
            "ppr": game.player1_ppr,
            "bags_in": game.player1_bags_in,
            "bags_on": game.player1_bags_on,
            "bags_off": game.player1_bags_off,
            "total_bags_thrown": game.player1_total_bags_thrown,
            "four_baggers": game.player1_four_baggers,
            "four_bagger_pct": game.player1_four_bagger_pct,
            "bags_in_pct": game.player1_bags_in_pct,
            "bags_on_pct": game.player1_bags_on_pct,
            "bags_off_pct": game.player1_bags_off_pct,
            "opponent_points": game.player1_opponent_points,
            "opponent_ppr": game.player1_opponent_ppr
        },
        "player2": {
            "id": game.player2_id,
            "name": player_names.get(game.player2_id, {}).get("name", f"Player {game.player2_id}") if game.player2_id else None,
            "cpi": player_names.get(game.player2_id, {}).get("cpi") if game.player2_id else None,
            "score": game_score_p2,  # Game score (to 21)
            "points": game.player2_points,  # Total points scored
            "rounds": game.player2_rounds,
            "ppr": game.player2_ppr,
            "bags_in": game.player2_bags_in,
            "bags_on": game.player2_bags_on,
            "bags_off": game.player2_bags_off,
            "total_bags_thrown": game.player2_total_bags_thrown,
            "four_baggers": game.player2_four_baggers,
            "four_bagger_pct": game.player2_four_bagger_pct,
            "bags_in_pct": game.player2_bags_in_pct,
            "bags_on_pct": game.player2_bags_on_pct,
            "bags_off_pct": game.player2_bags_off_pct,
            "opponent_points": game.player2_opponent_points,
            "opponent_ppr": game.player2_opponent_ppr
        },
        "winner_id": winner_id,
        "match": {
            "match_id": match.match_id if match else None,
            "round_number": match.round_number if match else None,
            "match_type": match.match_type if match else None,
            "match_status": match.match_status if match else None,
            "home_score": match.home_score if match else None,
            "away_score": match.away_score if match else None
        } if match else None,
        "event": {
            "event_id": event.event_id if event else None,
            "event_name": event.event_name if event else None,
            "base_event_name": event.base_event_name if event else None,
            "bracket_name": event.bracket_name if event else None,
            "event_date": event.event_date.isoformat() if event and event.event_date else None,
            "location": event.location if event else None
        } if event else None,
        "created_at": game.created_at.isoformat() if game.created_at else None
    }

@app.get("/api/head-to-head/{player1_id}/{player2_id}")
async def get_head_to_head(
    player1_id: int,
    player2_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get head-to-head matchup statistics between two players."""
    # Get all games where these two players faced each other
    games_query = select(EventGame).where(
        or_(
            and_(EventGame.player1_id == player1_id, EventGame.player2_id == player2_id),
            and_(EventGame.player1_id == player2_id, EventGame.player2_id == player1_id)
        )
    ).order_by(EventGame.created_at.desc())
    
    games_result = await db.execute(games_query)
    games = games_result.scalars().all()
    
    if not games:
        # Still return player names even if no games found
        player_ids = [player1_id, player2_id]
        player_names = {}
        if player_ids:
            latest_player_dates = select(
                Player.player_id,
                func.max(Player.snapshot_date).label('max_date')
            ).group_by(Player.player_id).subquery()
            
            latest_players_query = select(
                Player.player_id,
                Player.first_name,
                Player.last_name
            ).join(
                latest_player_dates,
                and_(
                    Player.player_id == latest_player_dates.c.player_id,
                    Player.snapshot_date == latest_player_dates.c.max_date
                )
            ).where(Player.player_id.in_(player_ids))
            
            players_result = await db.execute(latest_players_query)
            for row in players_result.all():
                player_names[row[0]] = f"{row[1] or ''} {row[2] or ''}".strip() or f"Player {row[0]}"
        
        return {
            "player1_id": player1_id,
            "player1_name": player_names.get(player1_id, f"Player {player1_id}"),
            "player2_id": player2_id,
            "player2_name": player_names.get(player2_id, f"Player {player2_id}"),
            "total_games": 0,
            "player1_wins": 0,
            "player2_wins": 0,
            "player1_stats": {},
            "player2_stats": {},
            "games": []
        }
    
    # Get player names
    player_ids = [player1_id, player2_id]
    player_names = {}
    if player_ids:
        latest_player_dates = select(
            Player.player_id,
            func.max(Player.snapshot_date).label('max_date')
        ).group_by(Player.player_id).subquery()
        
        latest_players_query = select(
            Player.player_id,
            Player.first_name,
            Player.last_name
        ).join(
            latest_player_dates,
            and_(
                Player.player_id == latest_player_dates.c.player_id,
                Player.snapshot_date == latest_player_dates.c.max_date
            )
        ).where(Player.player_id.in_(player_ids))
        
        players_result = await db.execute(latest_players_query)
        for row in players_result.all():
            player_names[row[0]] = f"{row[1] or ''} {row[2] or ''}".strip() or f"Player {row[0]}"
    
    # Calculate stats
    player1_wins = 0
    player2_wins = 0
    player1_total_points = 0
    player2_total_points = 0
    player1_total_rounds = 0
    player2_total_rounds = 0
    player1_total_bags_in = 0
    player2_total_bags_in = 0
    player1_total_four_baggers = 0
    player2_total_four_baggers = 0
    
    games_list = []
    event_ids = set()
    
    for game in games:
        event_ids.add(game.event_id)
        
        # Determine which player is which (could be swapped)
        is_player1_first = (game.player1_id == player1_id and game.player2_id == player2_id)
        
        if is_player1_first:
            p1_points = game.player1_points or 0
            p2_points = game.player2_points or 0
            p1_rounds = game.player1_rounds or 0
            p2_rounds = game.player2_rounds or 0
            p1_bags_in = game.player1_bags_in or 0
            p2_bags_in = game.player2_bags_in or 0
            p1_four_baggers = game.player1_four_baggers or 0
            p2_four_baggers = game.player2_four_baggers or 0
        else:
            p1_points = game.player2_points or 0
            p2_points = game.player1_points or 0
            p1_rounds = game.player2_rounds or 0
            p2_rounds = game.player1_rounds or 0
            p1_bags_in = game.player2_bags_in or 0
            p2_bags_in = game.player1_bags_in or 0
            p1_four_baggers = game.player2_four_baggers or 0
            p2_four_baggers = game.player1_four_baggers or 0
        
        # Count wins
        if p1_points > p2_points:
            player1_wins += 1
        elif p2_points > p1_points:
            player2_wins += 1
        
        # Accumulate stats
        player1_total_points += p1_points
        player2_total_points += p2_points
        player1_total_rounds += p1_rounds
        player2_total_rounds += p2_rounds
        player1_total_bags_in += p1_bags_in
        player2_total_bags_in += p2_bags_in
        player1_total_four_baggers += p1_four_baggers
        player2_total_four_baggers += p2_four_baggers
        
        # Get event info for this game
        event_query = select(Event.event_name, Event.base_event_name, Event.bracket_name)
        event_query = event_query.where(Event.event_id == game.event_id)
        event_result = await db.execute(event_query)
        event_row = event_result.first()
        event_name = event_row[0] if event_row and event_row[0] else f"Event {game.event_id}"
        
        # Get game score from match
        game_score_p1 = None
        game_score_p2 = None
        match_query = select(EventMatch).where(
            EventMatch.event_id == game.event_id,
            EventMatch.match_id == game.match_id
        )
        match_result = await db.execute(match_query)
        match = match_result.scalar_one_or_none()
        
        if match:
            if match.player1_id == player1_id:
                game_score_p1 = match.home_score
                game_score_p2 = match.away_score
            elif match.player1_id == player2_id:
                game_score_p1 = match.away_score
                game_score_p2 = match.home_score
            else:
                game_score_p1 = match.home_score
                game_score_p2 = match.away_score
        
        # Determine winner based on game score
        winner_id = None
        if game_score_p1 is not None and game_score_p2 is not None:
            if game_score_p1 >= 21 and game_score_p1 > game_score_p2:
                winner_id = player1_id
            elif game_score_p2 >= 21 and game_score_p2 > game_score_p1:
                winner_id = player2_id
            elif game_score_p1 > game_score_p2:
                winner_id = player1_id
            elif game_score_p2 > game_score_p1:
                winner_id = player2_id
        
        games_list.append({
            "game_id": game.id,
            "event_id": game.event_id,
            "match_id": game.match_id,
            "event_name": event_name,
            "player1_score": game_score_p1,
            "player2_score": game_score_p2,
            "player1_points": p1_points,  # Total points for reference
            "player2_points": p2_points,  # Total points for reference
            "player1_rounds": p1_rounds,
            "player2_rounds": p2_rounds,
            "player1_ppr": game.player1_ppr if is_player1_first else game.player2_ppr,
            "player2_ppr": game.player2_ppr if is_player1_first else game.player1_ppr,
            "winner_id": winner_id,
            "created_at": game.created_at.isoformat() if game.created_at else None
        })
    
    total_games = len(games)
    player1_ppr = player1_total_points / player1_total_rounds if player1_total_rounds > 0 else 0
    player2_ppr = player2_total_points / player2_total_rounds if player2_total_rounds > 0 else 0
    
    return {
        "player1_id": player1_id,
        "player1_name": player_names.get(player1_id, f"Player {player1_id}"),
        "player2_id": player2_id,
        "player2_name": player_names.get(player2_id, f"Player {player2_id}"),
        "total_games": total_games,
        "player1_wins": player1_wins,
        "player2_wins": player2_wins,
        "player1_win_pct": (player1_wins / total_games * 100) if total_games > 0 else 0,
        "player2_win_pct": (player2_wins / total_games * 100) if total_games > 0 else 0,
        "player1_stats": {
            "total_points": player1_total_points,
            "total_rounds": player1_total_rounds,
            "ppr": round(player1_ppr, 2),
            "total_bags_in": player1_total_bags_in,
            "total_four_baggers": player1_total_four_baggers
        },
        "player2_stats": {
            "total_points": player2_total_points,
            "total_rounds": player2_total_rounds,
            "ppr": round(player2_ppr, 2),
            "total_bags_in": player2_total_bags_in,
            "total_four_baggers": player2_total_four_baggers
        },
        "games": games_list
    }


# ACL API Cache Indexing Endpoints

from acl_cache_indexer import (
    index_all_standings_for_season,
    index_all_player_data_for_season,
    index_all_events_for_season,
    index_all_games_for_season,
    cache_indexing_status as acl_cache_status
)

@app.post("/api/acl-cache/index-standings/{bucket_id}")
async def index_acl_standings(
    bucket_id: int,
    background_tasks: BackgroundTasks,
    username: str = Depends(verify_admin),
    db: AsyncSession = Depends(get_db)
):
    """Index standings JSON for a season."""
    background_tasks.add_task(index_all_standings_for_season, bucket_id)
    return {"message": f"Standings indexing started for season {bucket_id}"}

@app.get("/api/acl-cache/status-standings/{bucket_id}")
async def get_acl_standings_status(bucket_id: int):
    """Get status of standings indexing for a season."""
    status_key = f"standings_{bucket_id}"
    return acl_cache_status.get(status_key, {
        "status": "not_running",
        "bucket_id": bucket_id
    })

@app.post("/api/acl-cache/index-players/{bucket_id}")
async def index_acl_players(
    bucket_id: int,
    background_tasks: BackgroundTasks,
    username: str = Depends(verify_admin),
    db: AsyncSession = Depends(get_db)
):
    """Index all player stats and events lists for a season."""
    background_tasks.add_task(index_all_player_data_for_season, bucket_id)
    return {"message": f"Player data indexing started for season {bucket_id}"}

@app.get("/api/acl-cache/status-players/{bucket_id}")
async def get_acl_players_status(bucket_id: int):
    """Get status of player data indexing for a season."""
    status_key = f"players_{bucket_id}"
    return acl_cache_status.get(status_key, {
        "status": "not_running",
        "bucket_id": bucket_id
    })

@app.post("/api/acl-cache/index-events/{bucket_id}")
async def index_acl_events(
    bucket_id: int,
    background_tasks: BackgroundTasks,
    username: str = Depends(verify_admin),
    db: AsyncSession = Depends(get_db)
):
    """Index all events for a season."""
    background_tasks.add_task(index_all_events_for_season, bucket_id)
    return {"message": f"Events indexing started for season {bucket_id}"}

@app.get("/api/acl-cache/status-events/{bucket_id}")
async def get_acl_events_status(bucket_id: int):
    """Get status of events indexing for a season."""
    status_key = f"events_{bucket_id}"
    return acl_cache_status.get(status_key, {
        "status": "not_running",
        "bucket_id": bucket_id
    })

@app.post("/api/acl-cache/index-games/{bucket_id}")
async def index_acl_games(
    bucket_id: int,
    background_tasks: BackgroundTasks,
    username: str = Depends(verify_admin),
    db: AsyncSession = Depends(get_db)
):
    """Index all games/matches for a season."""
    background_tasks.add_task(index_all_games_for_season, bucket_id)
    return {"message": f"Games indexing started for season {bucket_id}"}

@app.get("/api/acl-cache/status-games/{bucket_id}")
async def get_acl_games_status(bucket_id: int):
    """Get status of games indexing for a season."""
    status_key = f"games_{bucket_id}"
    return acl_cache_status.get(status_key, {
        "status": "not_running",
        "bucket_id": bucket_id
    })


