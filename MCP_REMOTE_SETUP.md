# Remote MCP Server Setup

This guide explains how to connect to the remote MCP server running on your Fly.io deployment.

## Overview

The MCP server is now accessible via HTTP endpoints on your deployed FastAPI application. This allows anyone with the URL to connect their Claude Desktop (or other MCP clients) to query your cornhole statistics database.

## Server URL

Your MCP server is available at:
```
https://fly-cornhole.fly.dev/mcp
```

(Replace `fly-cornhole` with your actual Fly.io app name if different)

## Available Endpoints

### 1. List Tools
```
GET https://fly-cornhole.fly.dev/mcp/tools
```

Returns all available MCP tools with their schemas.

### 2. Call Tool
```
POST https://fly-cornhole.fly.dev/mcp/call
Content-Type: application/json

{
  "name": "tool_name",
  "arguments": {
    "arg1": "value1",
    "arg2": "value2"
  }
}
```

Calls a specific tool with the provided arguments.

## Configuring Claude Desktop for Remote MCP

Since Claude Desktop's built-in MCP client uses stdio (local processes), you'll need to use an HTTP proxy or MCP client that supports HTTP transport.

### Option 1: Use MCP HTTP Client (Recommended)

You can use a custom MCP HTTP client. Here's a Python script that acts as a bridge:

```python
# mcp_http_client.py
import httpx
import json
import sys
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def main():
    server_url = "https://fly-cornhole.fly.dev/mcp"
    
    # For HTTP transport, you'd need a custom implementation
    # For now, use stdio with a local proxy script
    pass

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

### Option 2: Use MCP HTTP Transport (If Available)

Some MCP clients support HTTP transport directly. Check if your MCP client supports:

```json
{
  "mcpServers": {
    "cornhole-stats-remote": {
      "transport": "http",
      "url": "https://fly-cornhole.fly.dev/mcp"
    }
  }
}
```

### Option 3: Use the Provided Proxy Script (Recommended)

A proxy script is included in this repository: `mcp_proxy.py`

1. **Make it executable:**
```bash
chmod +x mcp_proxy.py
```

2. **Configure Claude Desktop:**
```json
{
  "mcpServers": {
    "cornhole-stats": {
      "command": "python",
      "args": ["/absolute/path/to/fly-cornhole/mcp_proxy.py"],
      "env": {
        "MCP_SERVER_URL": "https://fly-cornhole.fly.dev/mcp"
      }
    }
  }
}
```

3. **Optional: Set API key (if you add authentication):**
```json
{
  "mcpServers": {
    "cornhole-stats": {
      "command": "python",
      "args": ["/absolute/path/to/fly-cornhole/mcp_proxy.py"],
      "env": {
        "MCP_SERVER_URL": "https://fly-cornhole.fly.dev/mcp",
        "MCP_API_KEY": "your-secret-key-here"
      }
    }
  }
}
```

## Testing the Remote Server

### Test with curl

1. **List available tools:**
```bash
curl https://fly-cornhole.fly.dev/mcp/tools
```

2. **Call a tool:**
```bash
curl -X POST https://fly-cornhole.fly.dev/mcp/call \
  -H "Content-Type: application/json" \
  -d '{
    "name": "get_top_players",
    "arguments": {
      "stat": "pts_per_rnd",
      "limit": 5,
      "season": 11
    }
  }'
```

3. **Search for players:**
```bash
curl -X POST https://fly-cornhole.fly.dev/mcp/call \
  -H "Content-Type: application/json" \
  -d '{
    "name": "search_players",
    "arguments": {
      "search": "Smith",
      "limit": 10
    }
  }'
```

## Example Tool Calls

### Get Player Stats
```json
{
  "name": "get_player_stats",
  "arguments": {
    "player_name": "John Smith",
    "season": 11
  }
}
```

### Get Top Players
```json
{
  "name": "get_top_players",
  "arguments": {
    "stat": "pts_per_rnd",
    "season": 11,
    "limit": 10
  }
}
```

### Compare Player Across Seasons
```json
{
  "name": "compare_player_seasons",
  "arguments": {
    "player_name": "John Smith",
    "seasons": [11, 10, 9]
  }
}
```

### Search Players
```json
{
  "name": "search_players",
  "arguments": {
    "search": "Smith",
    "state": "CA",
    "limit": 20
  }
}
```

## Security Considerations

Currently, the MCP endpoints are publicly accessible. Consider adding:

1. **API Key Authentication:**
   - Add an API key requirement
   - Store keys in environment variables
   - Validate keys in the MCP routes

2. **Rate Limiting:**
   - Add rate limiting to prevent abuse
   - Use FastAPI's rate limiting middleware

3. **CORS Configuration:**
   - Configure CORS if needed for web clients
   - Restrict origins if applicable

## Adding Authentication (Optional)

To add API key authentication, modify `mcp_routes.py`:

```python
from fastapi import Header
import os

API_KEY = os.getenv("MCP_API_KEY", "your-secret-key")

async def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return x_api_key

@router.post("/call")
async def call_tool(request: Request, api_key: str = Depends(verify_api_key)):
    # ... existing code
```

Then clients would need to include the header:
```
X-API-Key: your-secret-key
```

## Deployment

The MCP routes are automatically included when you deploy your FastAPI app:

```bash
fly deploy
```

After deployment, the MCP endpoints will be available at:
```
https://your-app-name.fly.dev/mcp
```

## Troubleshooting

### Endpoints not accessible
- Check that the app is deployed: `fly status`
- Verify the routes are included: Check `main.py` for `app.include_router(mcp_router)`
- Test locally: `curl http://localhost:8000/mcp/tools`

### CORS errors
- Add CORS middleware to `main.py`:
```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### Database connection issues
- Ensure DATABASE_URL is set in Fly.io: `fly secrets set DATABASE_URL=...`
- Check database is accessible from the app

