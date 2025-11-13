#!/usr/bin/env python3
"""Verify events and games indexed for season 11"""
import asyncio
from database import async_session_maker, Event, EventGame, EventMatch
from sqlalchemy import select, func, distinct
from datetime import datetime

async def verify_season11():
    """Verify events and games indexed for season 11"""
    async with async_session_maker() as db:
        print("=" * 60)
        print("Season 11 (Bucket ID 11) Verification Report")
        print("=" * 60)
        print()
        
        # 1. Count total events for season 11
        events_query = await db.execute(
            select(func.count()).select_from(Event).where(Event.bucket_id == 11)
        )
        total_events = events_query.scalar()
        print(f"📅 Total Events Indexed: {total_events}")
        
        if total_events > 0:
            # Breakdown by event type
            type_query = await db.execute(
                select(Event.event_type, func.count()).where(Event.bucket_id == 11).group_by(Event.event_type)
            )
            print("\n   Event Type Breakdown:")
            for event_type, count in type_query.all():
                print(f"   - {event_type or 'Unknown'}: {count}")
            
            # Events with games fully indexed
            fully_indexed_query = await db.execute(
                select(func.count()).select_from(Event).where(
                    Event.bucket_id == 11,
                    Event.games_fully_indexed == True
                )
            )
            fully_indexed = fully_indexed_query.scalar()
            print(f"\n   ✅ Events with games fully indexed: {fully_indexed}")
            
            # Events partially indexed
            partial_query = await db.execute(
                select(func.count()).select_from(Event).where(
                    Event.bucket_id == 11,
                    Event.games_indexed_count > 0,
                    Event.games_fully_indexed == False
                )
            )
            partial = partial_query.scalar()
            print(f"   ⏳ Events partially indexed: {partial}")
            
            # Events not yet indexed
            not_indexed = total_events - fully_indexed - partial
            print(f"   ⏸️  Events not yet indexed: {not_indexed}")
            
            # Show sample events
            sample_query = await db.execute(
                select(Event).where(Event.bucket_id == 11).order_by(Event.event_date.desc()).limit(5)
            )
            sample_events = sample_query.scalars().all()
            print("\n   Sample Events (most recent):")
            for event in sample_events:
                indexed_status = "✅" if event.games_fully_indexed else f"⏳ {event.games_indexed_count}/{event.games_total_count}" if event.games_indexed_count > 0 else "⏸️"
                print(f"   - {event.event_name} ({event.event_type}) - {event.event_date} {indexed_status}")
        
        print()
        
        # 2. Count total games for season 11 events
        games_query = await db.execute(
            select(func.count()).select_from(EventGame).join(
                Event, EventGame.event_id == Event.event_id
            ).where(Event.bucket_id == 11)
        )
        total_games = games_query.scalar()
        print(f"🎮 Total Games Indexed: {total_games}")
        
        if total_games > 0:
            # Count unique events that have games
            unique_events_query = await db.execute(
                select(func.count(distinct(EventGame.event_id))).select_from(EventGame).join(
                    Event, EventGame.event_id == Event.event_id
                ).where(Event.bucket_id == 11)
            )
            events_with_games = unique_events_query.scalar()
            print(f"   Events with games indexed: {events_with_games}")
            
            # Average games per event
            if events_with_games > 0:
                avg_games = total_games / events_with_games
                print(f"   Average games per event: {avg_games:.1f}")
            
            # Get games breakdown by event
            games_by_event_query = await db.execute(
                select(
                    Event.event_id,
                    Event.event_name,
                    Event.event_type,
                    func.count(EventGame.id).label('game_count')
                ).select_from(EventGame).join(
                    Event, EventGame.event_id == Event.event_id
                ).where(Event.bucket_id == 11).group_by(
                    Event.event_id, Event.event_name, Event.event_type
                ).order_by(func.count(EventGame.id).desc()).limit(10)
            )
            
            print("\n   Top Events by Game Count:")
            for event_id, event_name, event_type, game_count in games_by_event_query.all():
                print(f"   - {event_name} ({event_type}): {game_count} games")
        
        print()
        
        # 3. Count total matches for season 11 events
        matches_query = await db.execute(
            select(func.count()).select_from(EventMatch).join(
                Event, EventMatch.event_id == Event.event_id
            ).where(Event.bucket_id == 11)
        )
        total_matches = matches_query.scalar()
        print(f"🏆 Total Matches Indexed: {total_matches}")
        
        if total_matches > 0:
            # Count unique events that have matches
            unique_events_matches_query = await db.execute(
                select(func.count(distinct(EventMatch.event_id))).select_from(EventMatch).join(
                    Event, EventMatch.event_id == Event.event_id
                ).where(Event.bucket_id == 11)
            )
            events_with_matches = unique_events_matches_query.scalar()
            print(f"   Events with matches indexed: {events_with_matches}")
        
        print()
        print("=" * 60)
        print("Summary:")
        print(f"  Events: {total_events}")
        print(f"  Games: {total_games}")
        print(f"  Matches: {total_matches}")
        print("=" * 60)

if __name__ == "__main__":
    asyncio.run(verify_season11())



