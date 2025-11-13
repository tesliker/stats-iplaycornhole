#!/bin/bash
# Quick script to check games indexing status for an event

EVENT_ID=${1:-220576}

echo "Checking games status for event $EVENT_ID..."
curl -s "https://fly-cornhole.fly.dev/api/events/$EVENT_ID/games-count" | python3 -m json.tool



