#!/usr/bin/env python3
import requests
import json

# Get events for season 11
events_url = "https://stats.iplaycornhole.me/api/events?bucket_id=11&limit=100"
response = requests.get(events_url)
events_data = response.json()
events = events_data.get('events', [])

print(f"Checking {len(events)} events for season 11...")
print()

total_games = 0
total_matches = 0
events_with_games = 0
events_with_matches = 0

for i, event in enumerate(events[:20]):  # Check first 20 events
    event_id = event['event_id']
    event_name = event['event_name']
    
    games_url = f"https://stats.iplaycornhole.me/api/events/{event_id}/games-count"
    games_response = requests.get(games_url)
    games_data = games_response.json()
    
    games_count = games_data.get('games_count', 0)
    matches_count = games_data.get('matches_count', 0)
    
    if games_count > 0:
        events_with_games += 1
        total_games += games_count
        print(f"✓ Event {event_id}: {games_count} games, {matches_count} matches")
        print(f"  {event_name[:60]}...")
    
    if matches_count > 0:
        events_with_matches += 1
        total_matches += matches_count

print()
print("=" * 60)
print(f"Summary (first 20 events checked):")
print(f"  Events with games: {events_with_games}/{len(events[:20])}")
print(f"  Total games found: {total_games}")
print(f"  Total matches found: {total_matches}")
print("=" * 60)



