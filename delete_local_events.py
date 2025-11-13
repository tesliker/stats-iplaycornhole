"""
Script to delete local events (type "L" or "local") from the database.
Run this to clean up the 891 local events that were indexed before we added the skip logic.
"""
import asyncio
from database import async_session_maker, Event, PlayerEventStats, EventMatchup, EventStanding
from sqlalchemy import select, delete

async def delete_local_events():
    """Delete all local events and their related data."""
    async with async_session_maker() as db:
        try:
            # Find all local events
            # Check for event_type = "local" or "l", or eventType "L" in the name
            local_events_query = select(Event.event_id).where(
                (Event.event_type == "local") | (Event.event_type == "l")
            )
            result = await db.execute(local_events_query)
            local_event_ids = [row[0] for row in result.all()]
            
            print(f"Found {len(local_event_ids)} local events to delete")
            
            if not local_event_ids:
                print("No local events found to delete")
                return
            
            # Delete related data first (foreign key constraints)
            print("Deleting player event stats...")
            await db.execute(
                delete(PlayerEventStats).where(
                    PlayerEventStats.event_id.in_(local_event_ids)
                )
            )
            
            print("Deleting event matchups...")
            await db.execute(
                delete(EventMatchup).where(
                    EventMatchup.event_id.in_(local_event_ids)
                )
            )
            
            print("Deleting event standings...")
            await db.execute(
                delete(EventStanding).where(
                    EventStanding.event_id.in_(local_event_ids)
                )
            )
            
            print("Deleting events...")
            await db.execute(
                delete(Event).where(
                    Event.event_id.in_(local_event_ids)
                )
            )
            
            await db.commit()
            print(f"Successfully deleted {len(local_event_ids)} local events and all related data")
            
        except Exception as e:
            print(f"Error deleting local events: {e}")
            import traceback
            traceback.print_exc()
            await db.rollback()
            raise

if __name__ == "__main__":
    asyncio.run(delete_local_events())



