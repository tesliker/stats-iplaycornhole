"""
Test game indexing for a specific event (Open #2 Tier 1 Singles Bracket D - event 220576)
"""
import asyncio
from game_indexer import discover_and_index_event_games
from database import async_session_maker

async def test_index_event_games(event_id: int = 220576):
    """Index games for a specific event for testing."""
    async with async_session_maker() as db:
        print(f"Indexing games for event {event_id}...")
        new_games = await discover_and_index_event_games(event_id, db)
        await db.commit()
        print(f"Indexed {new_games} new games for event {event_id}")
        return new_games

if __name__ == "__main__":
    asyncio.run(test_index_event_games(220576))



