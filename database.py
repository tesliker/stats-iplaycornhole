from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, JSON, UniqueConstraint, Index
from datetime import datetime

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

# Database setup
# Prefer PostgreSQL (production), fallback to SQLite (local dev)
import os

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
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def get_db():
    async with async_session_maker() as session:
        yield session

