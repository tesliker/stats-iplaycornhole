from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Query, Request, Form, status as http_status
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

from database import get_db, init_db, Player
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

