# Quick Start: Add Cornhole Stats to ChatGPT

## Method 1: Custom GPT with Actions (Best for ChatGPT Plus)

### Step 1: Create Custom GPT
1. Go to [chat.openai.com](https://chat.openai.com)
2. Click your profile → **"Create a GPT"** or go to **"Explore GPTs"**
3. Click **"Create"**

### Step 2: Configure Basic Info
- **Name**: `Cornhole Stats`
- **Description**: `Ask questions about cornhole player statistics, rankings, and comparisons`

### Step 3: Add Action (API)
1. Click **"Actions"** tab
2. Click **"Create new action"**
3. Choose **"Import from URL"** or **"Manual"**

**If Manual, paste this OpenAPI schema:**

```yaml
openapi: 3.1.0
info:
  title: Cornhole Statistics API
  version: 1.0.0
servers:
  - url: https://fly-cornhole.fly.dev
paths:
  /mcp/call:
    post:
      summary: Call an MCP tool
      operationId: callTool
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              properties:
                name:
                  type: string
                  enum:
                    - get_player_stats
                    - search_players
                    - get_top_players
                    - compare_player_seasons
                    - get_player_rankings
                    - get_filter_options
                arguments:
                  type: object
              required:
                - name
      responses:
        '200':
          description: Tool response
          content:
            application/json:
              schema:
                type: object
```

4. **Base URL**: `https://fly-cornhole.fly.dev`
5. **Save**

### Step 4: Add Instructions
In the **"Instructions"** field, add:

```
You are a cornhole statistics assistant. Use the available API tools to answer questions about player stats.

Available tools:
- get_player_stats: Get stats for a player by name or ID
- search_players: Search players by name, state, skill level
- get_top_players: Get top players by any statistic (PPR, DPR, CPI, etc.)
- compare_player_seasons: Compare a player across multiple seasons
- get_player_rankings: Get rankings and leaderboards
- get_filter_options: Get available filters

When users ask about stats, use the appropriate tool and format the results clearly.
```

### Step 5: Test
1. Click **"Save"**
2. Try asking: **"Who has the highest PPR this season?"**

---

## Method 2: Share Web Interface

If you host the `chatgpt_web_interface.html` file, users can:
1. Access it via URL
2. Tell ChatGPT to browse to that URL
3. ChatGPT can read and interact with it

**To host it:**
1. Add it to your FastAPI app's static files
2. Or host it on GitHub Pages
3. Share the URL

---

## Method 3: Direct API Access (For Technical Users)

Share this with users who want direct API access:

```
You can query cornhole statistics via API:

Endpoint: https://fly-cornhole.fly.dev/mcp/call
Method: POST
Body: {
  "name": "get_top_players",
  "arguments": {
    "stat": "pts_per_rnd",
    "limit": 10,
    "season": 11
  }
}
```

Then ChatGPT can use this information when prompted.

---

## Testing Your Custom GPT

Once set up, try these prompts:
- ✅ "Who has the highest points per round this season?"
- ✅ "Show me the top 10 players by CPI"
- ✅ "Search for players from California"
- ✅ "Compare player stats across seasons 11, 10, and 9"
- ✅ "What are the available filter options?"

---

## Sharing Your GPT

To let others use it:
1. In GPT settings, make it **"Public"**
2. Share the GPT link
3. Or publish to GPT Store (if available)

---

## Troubleshooting

**Action not working?**
- Check that your server is running: `https://fly-cornhole.fly.dev/mcp/tools`
- Verify the OpenAPI schema is correct
- Check ChatGPT's error messages

**Can't create Custom GPT?**
- Make sure you have ChatGPT Plus subscription
- Try refreshing the page

**API errors?**
- Test the endpoint directly: `curl -X POST https://fly-cornhole.fly.dev/mcp/call -H "Content-Type: application/json" -d '{"name":"get_filter_options","arguments":{}}'`

