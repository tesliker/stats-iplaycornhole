"""
Quick script to check event indexing status in the database
"""
import asyncio
from database import async_session_maker, Event, PlayerEventStats
from sqlalchemy import select, func

async def check_status():
    async with async_session_maker() as db:
        # Count events
        events_result = await db.execute(select(func.count()).select_from(Event).where(Event.bucket_id == 11))
        event_count = events_result.scalar() or 0
        
        # Count player event stats
        stats_result = await db.execute(select(func.count()).select_from(PlayerEventStats))
        stats_count = stats_result.scalar() or 0
        
        # Get event IDs
        event_ids_result = await db.execute(select(Event.event_id).where(Event.bucket_id == 11).limit(10))
        event_ids = [row[0] for row in event_ids_result.all()]
        
        print(f"Total Events in DB (bucket 11): {event_count}")
        print(f"Total Player Event Stats: {stats_count}")
        print(f"Sample Event IDs: {event_ids}")
        
        # Check events by type
        type_result = await db.execute(
            select(Event.event_type, func.count(Event.id).label('count'))
            .where(Event.bucket_id == 11)
            .group_by(Event.event_type)
        )
        events_by_type = {row[0]: row[1] for row in type_result.all() if row[0]}
        print(f"Events by Type: {events_by_type}")

if __name__ == "__main__":
    asyncio.run(check_status())



