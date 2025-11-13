# Adding Cornhole Stats to ChatGPT

ChatGPT web doesn't support MCP directly, but there are several ways to give ChatGPT access to your cornhole statistics:

## Option 1: Create a Custom GPT with Actions (Recommended)

This is the best way for ChatGPT Plus users to access your data.

### Step 1: Create a Custom GPT

1. Go to [ChatGPT](https://chat.openai.com)
2. Click your profile → **"Create a GPT"** or go to **"Explore GPTs"**
3. Click **"Create"** or **"+"** to create a new GPT

### Step 2: Configure the GPT

1. **Name**: "Cornhole Stats Assistant"
2. **Description**: "Ask questions about cornhole player statistics, rankings, and comparisons across seasons"

### Step 3: Add Actions (API Integration)

1. In the GPT builder, go to **"Actions"** tab
2. Click **"Create new action"**
3. You'll need to create an OpenAPI schema. Use this configuration:

```json
{
  "openapi": "3.1.0",
  "info": {
    "title": "Cornhole Statistics API",
    "description": "API for querying cornhole player statistics",
    "version": "1.0.0"
  },
  "servers": [
    {
      "url": "https://fly-cornhole.fly.dev",
      "description": "Production server"
    }
  ],
  "paths": {
    "/mcp/call": {
      "post": {
        "summary": "Call an MCP tool",
        "description": "Execute a tool to query cornhole statistics",
        "operationId": "callTool",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "name": {
                    "type": "string",
                    "enum": ["get_player_stats", "search_players", "get_top_players", "compare_player_seasons", "get_player_rankings", "get_filter_options"],
                    "description": "The tool to call"
                  },
                  "arguments": {
                    "type": "object",
                    "description": "Arguments for the tool"
                  }
                },
                "required": ["name"]
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Tool response",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object"
                }
              }
            }
          }
        }
      }
    }
  }
}
```

4. **Authentication**: If your server requires auth, add it here
5. **Save** the action

### Step 4: Configure Instructions

Add these instructions to help ChatGPT use the tools:

```
You are a cornhole statistics assistant. Use the available tools to answer questions about:
- Player statistics (PPR, DPR, CPI, win percentage, etc.)
- Player rankings and leaderboards
- Player comparisons across seasons
- Searching for players by name, state, or skill level

Available tools:
- get_player_stats: Get stats for a specific player by name or ID
- search_players: Search for players by various criteria
- get_top_players: Get top players by any statistic
- compare_player_seasons: Compare a player across multiple seasons
- get_player_rankings: Get rankings and leaderboards
- get_filter_options: Get available filters

When a user asks about stats, use the appropriate tool to get the data, then provide a clear, formatted response.
```

### Step 5: Test Your GPT

1. Save and test the GPT
2. Try asking: "Who has the highest PPR this season?"

---

## Option 2: Use ChatGPT's Web Browsing (Simpler, but less direct)

If you don't have ChatGPT Plus or want a simpler setup:

1. **Enable Web Browsing** in ChatGPT (if available)
2. **Share a public URL** that ChatGPT can access
3. **Create a simple web page** that displays stats

Or just tell ChatGPT the API endpoint exists and provide examples:

```
"You can access cornhole statistics at https://fly-cornhole.fly.dev/mcp/call
Use POST requests with JSON like:
{
  "name": "get_top_players",
  "arguments": {"stat": "pts_per_rnd", "limit": 10, "season": 11}
}"
```

---

## Option 3: Create a Simple Web Interface

Create a simple HTML page that ChatGPT can browse to query stats.

See `chatgpt_web_interface.html` for a simple interface.

---

## Option 4: Use ChatGPT API with Function Calling

For developers, you can use ChatGPT's API with function calling:

```python
import openai

functions = [
    {
        "name": "get_player_stats",
        "description": "Get statistics for a specific player",
        "parameters": {
            "type": "object",
            "properties": {
                "player_name": {"type": "string"},
                "season": {"type": "integer", "default": 11}
            }
        }
    }
    # ... more functions
]

# Then call the API with function calling enabled
```

---

## Quick Setup for Custom GPT (Recommended)

The easiest way is Option 1 - create a Custom GPT with Actions. Here's a simplified version:

1. **Create Custom GPT** at chat.openai.com
2. **Add Action** with this URL: `https://fly-cornhole.fly.dev/mcp/call`
3. **Add this schema** (simplified):

```yaml
openapi: 3.1.0
info:
  title: Cornhole Stats
  version: 1.0.0
servers:
  - url: https://fly-cornhole.fly.dev
paths:
  /mcp/call:
    post:
      operationId: callTool
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                name:
                  type: string
                  enum: [get_player_stats, search_players, get_top_players, compare_player_seasons, get_player_rankings, get_filter_options]
                arguments:
                  type: object
      responses:
        '200':
          description: Success
```

4. **Test it!**

---

## Testing

Once set up, try these prompts:
- "Who has the highest points per round this season?"
- "Show me the top 10 players by CPI"
- "Search for players named Smith"
- "Compare player stats across seasons 11, 10, and 9"

---

## Note for Sharing

To let others use your Custom GPT:
1. Make it **Public** in GPT settings
2. Share the GPT link
3. Or publish it in the GPT Store (if you have access)

