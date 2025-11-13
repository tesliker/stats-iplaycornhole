# MCP Server for Cornhole Statistics

This repository provides an MCP (Model Context Protocol) server that exposes cornhole player statistics to AI assistants like Claude.

## Quick Start for Users

### Install & Configure (2 steps)

**1. Install the proxy:**
```bash
pip install git+https://github.com/yourusername/fly-cornhole.git
```

**2. Add to Claude Desktop config:**

Location: `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows)

```json
{
  "mcpServers": {
    "cornhole-stats": {
      "command": "cornhole-mcp",
      "env": {
        "MCP_SERVER_URL": "https://fly-cornhole.fly.dev/mcp"
      }
    }
  }
}
```

**3. Restart Claude Desktop**

Done! Now you can ask Claude questions about cornhole statistics.

## Example Questions

Once connected, try asking:
- "Who has the highest points per round this season?"
- "Show me statistics for John Smith"
- "Compare player 12345 across the last 3 seasons"
- "Find all players from California with skill level P"
- "Who are the top 10 players by CPI?"

## Available Tools

The server provides these MCP tools:

1. **get_player_stats** - Get stats for a specific player
2. **search_players** - Search players by name, state, skill level
3. **get_top_players** - Get top players by any statistic
4. **compare_player_seasons** - Compare player across multiple seasons
5. **get_player_rankings** - Get rankings and leaderboards
6. **get_filter_options** - Get available filters (states, skill levels, seasons)

## Server Deployment

The MCP server is deployed as part of the FastAPI application on Fly.io. The endpoints are:
- `GET /mcp/tools` - List available tools
- `POST /mcp/call` - Call a tool

## Development

To run the server locally:
```bash
uvicorn main:app --reload
```

The MCP endpoints will be available at `http://localhost:8000/mcp`

