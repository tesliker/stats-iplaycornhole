# Fixed OpenAPI Schema for ChatGPT

The error "object schema missing properties" occurs because ChatGPT's validator requires complete schema definitions. Here's the corrected schema:

## Copy This Schema (Fixed Version)

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
        "description": "Execute a tool to get player stats, rankings, or comparisons",
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
                            "type": "string"
                          },
                          "text": {
                            "type": "string"
                          }
                        },
                        "required": ["type", "text"]
                      }
                    },
                    "isError": {
                      "type": "boolean"
                    },
                    "error": {
                      "type": "object",
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
          }
        }
      }
    }
  }
}
```

## Key Fixes

1. **Response schema** now has `"required": ["content"]` instead of empty array
2. **All object schemas** have complete `properties` definitions
3. **Nested objects** have their `required` fields properly specified

## How to Use

1. Copy the entire JSON above
2. In ChatGPT Custom GPT builder → Actions → Create new action
3. Paste into the Schema field
4. Save

This should now validate without the "missing properties" error!



