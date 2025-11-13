from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, JSON, UniqueConstraint, Index, Date, Boolean
from datetime import datetime
import hashlib

Base = declarative_base()

class Player(Base):
    __tablename__ = "players"
    
    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, index=True)  # Removed unique=True to allow multiple snapshots
    bucket_id = Column(Integer, index=True)
    snapshot_date = Column(DateTime, index=True, nullable=False, default=datetime.utcnow)
    first_name = Column(String)
    last_name = Column(String)
    country_code = Column(String)
    country_name = Column(String)
    state = Column(String)
    region = Column(String, index=True, nullable=True)  # "us" or "canada" to distinguish US vs Canada players. NULL = legacy US data
    conference_id = Column(Integer)
    skill_level = Column(String)
    
    # Overall standings data
    rank = Column(Integer)
    overall_total = Column(Float)
    conference_bonus_points = Column(Float, default=0)
    conference_events_counter = Column(Integer, default=0)
    national_bonus_points = Column(Float, default=0)
    national_events_counter = Column(Integer, default=0)
    monthly_bonus = Column(Float, default=0)
    membership_bonus = Column(Float, default=0)
    player_50_event_bonus = Column(Float, default=0)
    monthly_event_counts = Column(JSON)
    
    # Performance stats
    pts_per_rnd = Column(Float)
    rounds_total = Column(Integer)
    total_pts = Column(Integer)
    opponent_pts_per_rnd = Column(Float)
    opponent_pts_total = Column(Integer)
    dpr = Column(Float)
    four_bagger_pct = Column(Float)
    bags_in_pct = Column(Float)
    bags_on_pct = Column(Float)
    bags_off_pct = Column(Float)
    
    # Win/Loss stats
    local_wins = Column(Integer, default=0)
    local_losses = Column(Integer, default=0)
    regional_wins = Column(Integer, default=0)
    regional_losses = Column(Integer, default=0)
    state_wins = Column(Integer, default=0)
    state_losses = Column(Integer, default=0)
    conference_wins = Column(Integer, default=0)
    conference_losses = Column(Integer, default=0)
    open_wins = Column(Integer, default=0)
    open_losses = Column(Integer, default=0)
    national_wins = Column(Integer, default=0)
    national_losses = Column(Integer, default=0)
    total_games = Column(Integer, default=0)
    total_wins = Column(Integer, default=0)
    total_losses = Column(Integer, default=0)
    win_pct = Column(Float, default=0)
    
    # CPI stats
    player_cpi = Column(Float)
    cpi_qualified = Column(Integer, default=0)
    membership_id = Column(Integer)
    membership_expiry_date = Column(String)
    membership_status = Column(String)
    membership_type = Column(String)
    membership_name = Column(String)
    
    last_updated = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Composite unique constraint: prevent duplicate snapshots for same player/bucket/date
    __table_args__ = (
        UniqueConstraint('player_id', 'bucket_id', 'snapshot_date', name='uq_player_bucket_snapshot'),
        Index('idx_player_bucket_date', 'player_id', 'bucket_id', 'snapshot_date'),
    )


class Event(Base):
    """Event information (regionals, opens, nationals)"""
    __tablename__ = "events"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, unique=True, index=True, nullable=False)  # API event ID
    event_name = Column(String)
    event_type = Column(String, index=True)  # 'regional', 'open', 'national', 'signature'
    event_date = Column(Date, index=True)
    location = Column(String)
    city = Column(String)
    state = Column(String)
    bucket_id = Column(Integer, index=True)  # Season
    region = Column(String, index=True)  # 'us', 'canada'
    event_number = Column(Integer)  # For "Open #2" type events
    is_signature = Column(Integer, default=0)  # 1 if signature event
    event_group_id = Column(Integer, index=True)  # Groups related brackets (A, B, C tiers, etc.)
    bracket_name = Column(String)  # Specific bracket name (e.g., "Tier 1 Singles Bracket C")
    base_event_name = Column(String, index=True)  # Normalized event name (e.g., "Open #2 Winter Haven")
    games_fully_indexed = Column(Boolean, default=False, index=True)  # True when all games for this event are indexed
    games_indexed_count = Column(Integer, default=0)  # Number of games indexed for this event
    games_total_count = Column(Integer, default=0)  # Total number of games expected (from bracket data)
    games_indexed_at = Column(DateTime)  # Timestamp when games were fully indexed
    game_data = Column(JSON, nullable=True)  # Complete bracket data from API (bracketDetails, etc.)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_event_date_type', 'event_date', 'event_type'),
        Index('idx_event_bucket', 'bucket_id', 'event_date'),
    )


class PlayerEventStats(Base):
    """Player statistics for a specific event"""
    __tablename__ = "player_event_stats"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, index=True, nullable=False)
    player_id = Column(Integer, index=True, nullable=False)
    rank = Column(Integer)
    pts_per_rnd = Column(Float)
    dpr = Column(Float)
    total_games = Column(Integer)
    wins = Column(Integer)
    losses = Column(Integer)
    win_pct = Column(Float)
    rounds_played = Column(Integer)
    total_pts = Column(Integer)
    opponent_pts_per_rnd = Column(Float)
    opponent_pts_total = Column(Integer)
    four_bagger_pct = Column(Float)
    bags_in_pct = Column(Float)
    bags_on_pct = Column(Float)
    bags_off_pct = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('event_id', 'player_id', name='uq_event_player'),
        Index('idx_player_event', 'player_id', 'event_id'),
        Index('idx_event_rank', 'event_id', 'rank'),
    )


class EventMatchup(Base):
    """Match results from bracket data"""
    __tablename__ = "event_matchups"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, index=True, nullable=False)
    round_number = Column(Integer, index=True)
    player1_id = Column(Integer, index=True)
    player2_id = Column(Integer, index=True)
    winner_id = Column(Integer, index=True)
    loser_id = Column(Integer, index=True)
    score = Column(String)  # e.g., "21-15"
    player1_score = Column(Integer)
    player2_score = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_event_round', 'event_id', 'round_number'),
        Index('idx_player1_matches', 'player1_id', 'event_id'),
        Index('idx_player2_matches', 'player2_id', 'event_id'),
        Index('idx_winner', 'winner_id', 'event_id'),
    )


class EventStanding(Base):
    """Final standings/rankings for an event"""
    __tablename__ = "event_standings"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, index=True, nullable=False)
    player_id = Column(Integer, index=True, nullable=False)
    final_rank = Column(Integer, index=True)
    points = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('event_id', 'player_id', name='uq_event_standing'),
        Index('idx_event_rank_final', 'event_id', 'final_rank'),
    )


class EventMatch(Base):
    """Match information for an event"""
    __tablename__ = "event_matches"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, index=True, nullable=False)
    match_id = Column(Integer, nullable=False)  # API match ID
    round_number = Column(Integer, index=True)
    player1_id = Column(Integer, index=True)
    player2_id = Column(Integer, index=True)
    winner_id = Column(Integer, index=True)
    match_status = Column(Integer)  # 5 = completed
    match_status_desc = Column(String)
    home_score = Column(Integer)
    away_score = Column(Integer)
    court_id = Column(Integer)
    match_type = Column(String)  # "S" = singles, "D" = doubles
    raw_data = Column(JSON, nullable=True)  # Store complete raw API response to preserve all data
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('event_id', 'match_id', name='uq_event_match'),
        Index('idx_event_match', 'event_id', 'match_id'),
        Index('idx_match_players', 'player1_id', 'player2_id'),
    )


class EventGame(Base):
    """Individual game data within a match"""
    __tablename__ = "event_games"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, index=True, nullable=False)
    match_id = Column(Integer, nullable=False)
    game_id = Column(Integer, nullable=False)  # Usually 1, but can have multiple games per match
    player1_id = Column(Integer, index=True)
    player2_id = Column(Integer, index=True)
    
    # Player 1 stats
    player1_points = Column(Integer)
    player1_rounds = Column(Integer)
    player1_bags_in = Column(Integer)
    player1_bags_on = Column(Integer)
    player1_bags_off = Column(Integer)
    player1_total_bags_thrown = Column(Integer)
    player1_four_baggers = Column(Integer)
    player1_ppr = Column(Float)
    player1_bags_in_pct = Column(Float)
    player1_bags_on_pct = Column(Float)
    player1_bags_off_pct = Column(Float)
    player1_four_bagger_pct = Column(Float)
    player1_opponent_points = Column(Integer)
    player1_opponent_ppr = Column(Float)
    
    # Player 2 stats
    player2_points = Column(Integer)
    player2_rounds = Column(Integer)
    player2_bags_in = Column(Integer)
    player2_bags_on = Column(Integer)
    player2_bags_off = Column(Integer)
    player2_total_bags_thrown = Column(Integer)
    player2_four_baggers = Column(Integer)
    player2_ppr = Column(Float)
    player2_bags_in_pct = Column(Float)
    player2_bags_on_pct = Column(Float)
    player2_bags_off_pct = Column(Float)
    player2_four_bagger_pct = Column(Float)
    player2_opponent_points = Column(Integer)
    player2_opponent_ppr = Column(Float)
    
    raw_data = Column(JSON, nullable=True)  # Store complete raw API response to preserve all data
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('event_id', 'match_id', 'game_id', name='uq_event_game'),
        Index('idx_event_game', 'event_id', 'match_id', 'game_id'),
        Index('idx_game_players', 'player1_id', 'player2_id'),
        Index('idx_game_rounds', 'player1_rounds', 'player2_rounds'),
    )


class EventAggregatedStats(Base):
    """Pre-computed aggregated player statistics per event/bracket group.
    
    This table stores pre-calculated stats to avoid expensive aggregation
    on every page load. Stats are recalculated when games are indexed.
    """
    __tablename__ = "event_aggregated_stats"
    
    id = Column(Integer, primary_key=True, index=True)
    # Group identifier: either event_id (single bracket) or group_key (combined brackets)
    group_key = Column(String, index=True, nullable=False)  # e.g., "event_220575" or "grouped_Open #2 Winter Haven_Tier 1 Singles"
    group_type = Column(String, nullable=False)  # "event" or "grouped"
    event_ids = Column(JSON, nullable=False)  # List of event_ids in this group
    base_event_name = Column(String, index=True)
    bracket_type = Column(String)
    
    # Player stats (stored as JSON for flexibility)
    player_stats = Column(JSON, nullable=False)  # List of player stat objects
    
    # Metadata
    total_players = Column(Integer)
    total_games = Column(Integer)
    calculated_at = Column(DateTime, default=datetime.utcnow, index=True)
    games_hash = Column(String)  # Hash of game IDs to detect when recalculation is needed
    
    __table_args__ = (
        UniqueConstraint('group_key', name='uq_event_aggregated_stats'),
        Index('idx_group_type', 'group_type', 'calculated_at'),
    )

# Database setup
# Prefer PostgreSQL (production), fallback to SQLite (local dev)
import os

# Load .env file if it exists (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv not installed, that's okay
    pass

# Check for PostgreSQL connection string from Fly.io or environment
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    # PostgreSQL from Fly.io (format: postgres://user:pass@host:port/dbname)
    # SQLAlchemy async needs postgresql+asyncpg://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
    elif DATABASE_URL.startswith("postgresql://"):
        DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
    
    # SQLAlchemy pool settings for PostgreSQL concurrent writes
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_size=10,  # Allow 10 concurrent connections
        max_overflow=20,  # Allow up to 20 additional connections
        pool_pre_ping=True,  # Verify connections before using
        pool_recycle=300,  # Recycle connections after 5 minutes
    )
else:
    # Fallback to SQLite for local development
    db_path = os.getenv("DATABASE_PATH", "./cornhole.db")
    if os.path.exists("/data") and not os.getenv("DATABASE_PATH"):
        db_path = "/data/cornhole.db"
    DATABASE_URL = f"sqlite+aiosqlite:///{db_path}"
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        # SQLite connection pool settings
        pool_pre_ping=True,
    )

async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    """Initialize database - creates tables and adds missing columns."""
    async with engine.begin() as conn:
        # Create all tables (this will create new tables but won't alter existing ones)
        await conn.run_sync(Base.metadata.create_all)
        
        # Manually add the region column if it doesn't exist
        # SQLAlchemy's create_all doesn't alter existing tables
        from sqlalchemy import text
        try:
            # Check if region column exists
            if DATABASE_URL and "sqlite" in DATABASE_URL.lower():
                # SQLite: Check using PRAGMA
                result = await conn.execute(
                    text("PRAGMA table_info(players)")
                )
                columns = [row[1] for row in result.fetchall()]
                if "region" not in columns:
                    print("Adding region column to players table (SQLite)...")
                    await conn.execute(text("ALTER TABLE players ADD COLUMN region VARCHAR"))
                    await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_players_region ON players(region)"))
                    print("Region column added successfully")
            else:
                # PostgreSQL: Check using information_schema
                result = await conn.execute(
                    text("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name='players' AND column_name='region'
                    """)
                )
                if not result.fetchone():
                    print("Adding region column to players table (PostgreSQL)...")
                    await conn.execute(text("ALTER TABLE players ADD COLUMN region VARCHAR"))
                    await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_players_region ON players(region)"))
                    print("Region column added successfully")
        except Exception as e:
            # If column already exists, that's fine
            error_str = str(e).lower()
            if "already exists" not in error_str and "duplicate" not in error_str and "no such column" not in error_str:
                print(f"Note: Could not check/add region column: {e}")

class ACLAPICache(Base):
    """Raw JSON cache for ACL API responses. Never hit ACL servers again!"""
    __tablename__ = "acl_api_cache"
    
    id = Column(Integer, primary_key=True, index=True)
    endpoint_type = Column(String, index=True, nullable=False)  # 'standings', 'player_stats', 'player_events', 'event_info', 'event_player_stats', 'event_standings', 'bracket_data', 'match_stats'
    url = Column(Text, nullable=False)  # Full URL that was called
    url_hash = Column(String, index=True, nullable=False)  # Hash of URL for quick lookup
    bucket_id = Column(Integer, index=True, nullable=True)  # Season bucket ID (for standings, player stats, player events)
    player_id = Column(Integer, index=True, nullable=True)  # Player ID (for player stats, player events)
    event_id = Column(Integer, index=True, nullable=True)  # Event ID (for event-related endpoints)
    match_id = Column(Integer, nullable=True)  # Match ID (for match stats)
    game_id = Column(Integer, nullable=True)  # Game ID (for match stats)
    region = Column(String, nullable=True)  # 'us' or 'canada' (for standings)
    response_json = Column(JSON, nullable=False)  # The raw JSON response
    http_status = Column(Integer, nullable=True)  # HTTP status code
    fetched_at = Column(DateTime, default=datetime.utcnow, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Unique constraint: same URL should only be cached once
    __table_args__ = (
        UniqueConstraint('url_hash', name='uq_acl_cache_url_hash'),
        Index('idx_acl_cache_endpoint_bucket', 'endpoint_type', 'bucket_id'),
        Index('idx_acl_cache_player_bucket', 'player_id', 'bucket_id'),
        Index('idx_acl_cache_event', 'event_id'),
    )

async def get_db():
    async with async_session_maker() as session:
        yield session

