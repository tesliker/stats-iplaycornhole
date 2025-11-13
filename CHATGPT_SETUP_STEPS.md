# Step-by-Step: Add Cornhole Stats to ChatGPT

## Prerequisites
- ChatGPT Plus subscription (required for Custom GPTs)
- Your server deployed at `https://fly-cornhole.fly.dev` (or running locally)

---

## Step 1: Create a Custom GPT

1. Go to [chat.openai.com](https://chat.openai.com)
2. In the left sidebar, click **"Explore GPTs"** (or your profile → "Create a GPT")
3. Click the **"+"** button or **"Create"** button to create a new GPT

---

## Step 2: Configure Basic Settings

1. In the **"Create"** tab:
   - **Name**: Enter `Cornhole Stats Assistant`
   - **Description**: Enter `Helps you query cornhole player statistics, rankings, and comparisons`

2. Click **"Next"** or go to the **"Actions"** tab

---

## Step 3: Add API Action

1. Click the **"Actions"** tab at the top
2. Click **"Create new action"**
3. You'll see a form with fields for the API schema

### Option A: Import from URL (Easiest)

If you host an OpenAPI schema file, you can import it directly.

### Option B: Manual Entry (Recommended)

1. In the **"Schema"** section, paste this complete OpenAPI schema:

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
        "summary": "Call an MCP tool to query cornhole statistics",
        "description": "Execute a tool to get player stats, rankings, or comparisons. Available tools: get_player_stats, search_players, get_top_players, compare_player_seasons, get_player_rankings, get_filter_options",
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
                    "enum": [
                      "get_player_stats",
                      "search_players",
                      "get_top_players",
                      "compare_player_seasons",
                      "get_player_rankings",
                      "get_filter_options"
                    ],
                    "description": "The tool to call"
                  },
                  "arguments": {
                    "type": "object",
                    "description": "Arguments for the tool (varies by tool)",
                    "properties": {},
                    "additionalProperties": true
                  }
                },
                "required": ["name"]
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Tool response with statistics data",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "content": {
                      "type": "array",
                      "items": {
                        "type": "object",
                        "properties": {
                          "type": {
                            "type": "string",
                            "description": "Content type"
                          },
                          "text": {
                            "type": "string",
                            "description": "Response text content"
                          }
                        },
                        "required": ["type", "text"]
                      },
                      "description": "Array of content items"
                    },
                    "isError": {
                      "type": "boolean",
                      "description": "Whether the response is an error"
                    },
                    "error": {
                      "type": "object",
                      "description": "Error information if isError is true",
                      "properties": {
                        "code": {
                          "type": "integer"
                        },
                        "message": {
                          "type": "string"
                        }
                      }
                    }
                  },
                  "required": ["content"]
                }
              }
            }
          },
          "400": {
            "description": "Bad request",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "detail": {
                      "type": "string"
                    }
                  }
                }
              }
            }
          },
          "500": {
            "description": "Server error",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "detail": {
                      "type": "string"
                    }
                  }
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

**Note**: The key fix is that all object schemas now have complete `properties` definitions and `required` fields specified.

2. **Authentication**: Leave as "None" (or add if you implement auth later)
3. Click **"Save"**

---

## Step 4: Add Instructions

1. Go back to the **"Create"** tab
2. In the **"Instructions"** field, add:

```
You are a cornhole statistics assistant. Use the available API tools to answer questions about player statistics.

Available tools:
- get_player_stats: Get detailed stats for a specific player by name or ID
- search_players: Search for players by name, state, skill level, or other criteria
- get_top_players: Get top players ranked by any statistic (PPR, DPR, CPI, win percentage, etc.)
- compare_player_seasons: Compare a player's performance across multiple seasons
- get_player_rankings: Get rankings and leaderboards
- get_filter_options: Get available filter options (states, skill levels, seasons)

When users ask about stats:
1. Use the appropriate tool to get the data
2. Format the results clearly with player names, stats, and rankings
3. For comparisons, show how stats changed over time
4. Be helpful and explain what the statistics mean

Always use the tools to get real data - don't make up statistics.
```

3. Click **"Save"** in the top right

---

## Step 5: Test Your GPT

1. Click **"Save"** to finish creating the GPT
2. You'll be taken to a chat with your new GPT
3. Try asking:
   - **"Who has the highest points per round this season?"**
   - **"Show me the top 10 players by CPI"**
   - **"Search for players from California"**

If it works, you should see the GPT using the API to fetch real data!

---

## Step 6: Make It Available to Others

1. Click the **"..."** menu next to your GPT name
2. Select **"Edit GPT"**
3. In settings, change visibility to **"Public"** or **"Anyone with a link"**
4. Share the GPT link with others

---

## Troubleshooting

### "Action failed" or API errors

1. **Test your server directly:**
   ```bash
   curl https://fly-cornhole.fly.dev/mcp/tools
   ```
   Should return a list of tools.

2. **Check the server URL** in the schema matches your deployment

3. **Test the API endpoint:**
   ```bash
   curl -X POST https://fly-cornhole.fly.dev/mcp/call \
     -H "Content-Type: application/json" \
     -d '{"name":"get_filter_options","arguments":{}}'
   ```

### GPT not calling the API

- Check that the schema is valid JSON
- Make sure the operationId is set
- Verify the server URL is correct
- Try re-saving the GPT

### Can't create Custom GPT

- Make sure you have ChatGPT Plus
- Try refreshing the page
- Check if Custom GPTs are available in your region

---

## Alternative: Using Web Interface

If Custom GPTs aren't available, you can:

1. Host the web interface at `https://fly-cornhole.fly.dev/chatgpt`
2. Tell ChatGPT to browse to that URL
3. ChatGPT can read and interact with the interface

---

## Quick Reference

**Server URL**: `https://fly-cornhole.fly.dev/mcp/call`

**Available Tools**:
- `get_player_stats` - Get player by name/ID
- `search_players` - Search players
- `get_top_players` - Top players by stat
- `compare_player_seasons` - Compare across seasons
- `get_player_rankings` - Rankings/leaderboards
- `get_filter_options` - Available filters

**Example Request**:
```json
{
  "name": "get_top_players",
  "arguments": {
    "stat": "pts_per_rnd",
    "limit": 10,
    "season": 11
  }
}
```

---

That's it! Once set up, you and others can ask ChatGPT questions about cornhole statistics and it will use your database to answer.

