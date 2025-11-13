# MCP Server Setup for Cornhole Statistics

This guide explains how to set up the MCP (Model Context Protocol) server to connect your cornhole database to AI assistants like Claude or ChatGPT.

## What is MCP?

MCP (Model Context Protocol) is a protocol developed by Anthropic that allows AI assistants to connect to external data sources and tools. This enables AI assistants to answer questions about your cornhole statistics database.

## Installation

1. **Install the MCP Python SDK** (if not already installed):
```bash
pip install mcp
```

2. **Verify the server works**:
```bash
python mcp_server.py
```

If it starts without errors, the server is ready.

## Configuring Claude Desktop

1. **Find your Claude Desktop config file**:
   - **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

2. **Edit the config file** (create it if it doesn't exist):
```json
{
  "mcpServers": {
    "cornhole-stats": {
      "command": "python",
      "args": ["/absolute/path/to/fly-cornhole/mcp_server.py"]
    }
  }
}
```

**Important**: Replace `/absolute/path/to/fly-cornhole/mcp_server.py` with the actual absolute path to your `mcp_server.py` file.

3. **Restart Claude Desktop** to load the new configuration.

## Available Tools

Once connected, the AI assistant will have access to these tools:

### 1. `get_player_stats`
Get statistics for a specific player by name or ID.

**Example queries:**
- "What are the stats for John Smith?"
- "Get player stats for player ID 12345"
- "Show me stats for Mike Johnson in season 10"

### 2. `search_players`
Search for players by name, state, skill level, or other criteria.

**Example queries:**
- "Find all players from California"
- "Search for players with skill level P"
- "Show me players named 'Smith'"

### 3. `get_top_players`
Get top players by various statistics.

**Example queries:**
- "Who are the top 10 players by PPR?"
- "Show me the top 5 players by CPI in season 11"
- "Who has the highest win percentage?"

### 4. `compare_player_seasons`
Compare a player's statistics across multiple seasons.

**Example queries:**
- "Compare John Smith's stats across seasons 11, 10, and 9"
- "How has player 12345 improved over the last 3 seasons?"

### 5. `get_player_rankings`
Get player rankings and leaderboards.

**Example queries:**
- "Show me the top 50 ranked players"
- "Who are the top 20 players by total games played?"
- "Rank players by DPR with at least 10 games"

### 6. `get_filter_options`
Get available filter options (states, skill levels, seasons).

**Example queries:**
- "What states are available in the database?"
- "What skill levels can I filter by?"
- "What seasons are available?"

## Example Conversations

Once set up, you can ask questions like:

- "Who is the top ranked player in season 11?"
- "What are the stats for players from Texas?"
- "Compare Mike Johnson's performance across the last 3 seasons"
- "Who has the highest points per round this season?"
- "Find all players with skill level P and show their stats"
- "What's the average win percentage for players from California?"

## Troubleshooting

### Server won't start
- Check that all dependencies are installed: `pip install -r requirements.txt`
- Verify the database file exists and is accessible
- Check that the Python path in the config is correct (use absolute path)

### Claude can't find the server
- Ensure the config file path is correct
- Restart Claude Desktop after making config changes
- Check that the `args` path is absolute (not relative)
- Verify Python is in your PATH

### Database errors
- Make sure the database file exists at the expected location
- Check that the database has been initialized (run your FastAPI app once)
- Verify database permissions

## Testing the Server

You can test the server directly using Python:

```python
import asyncio
from mcp_server import server, stdio_server

async def test():
    # Test that tools are registered
    tools = await server.list_tools()
    print(f"Found {len(tools)} tools")
    
    # Test a tool call
    result = await server.call_tool(
        "get_top_players",
        {"stat": "pts_per_rnd", "limit": 5, "season": 11}
    )
    print(result)

asyncio.run(test())
```

## Notes

- The server uses the same database as your FastAPI application
- All queries use the latest snapshot per player for each season
- The server is read-only (no write operations)
- Performance depends on your database size and location

