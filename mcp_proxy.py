#!/usr/bin/env python3
"""
MCP Proxy Script - Connects local MCP clients to remote HTTP MCP server

This script acts as a bridge between MCP clients (like Claude Desktop) that use
stdio transport and the remote HTTP MCP server.

After installation, configure in Claude Desktop:
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
"""

import sys
import json
import httpx
import asyncio
import os

# Get server URL from environment or use default
SERVER_URL = os.getenv("MCP_SERVER_URL", "https://fly-cornhole.fly.dev/mcp")
API_KEY = os.getenv("MCP_API_KEY")  # Optional API key


async def handle_request(request: dict) -> dict:
    """Proxy stdio MCP request to HTTP server"""
    method = request.get("method")
    params = request.get("params", {})
    
    headers = {}
    if API_KEY:
        headers["X-API-Key"] = API_KEY
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            if method == "initialize":
                # Return initialization response
                return {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "tools": {}
                    },
                    "serverInfo": {
                        "name": "cornhole-stats-remote",
                        "version": "1.0.0"
                    }
                }
            
            elif method == "tools/list":
                response = await client.get(f"{SERVER_URL}/tools", headers=headers)
                response.raise_for_status()
                tools_data = response.json()
                return {
                    "tools": tools_data.get("tools", [])
                }
            
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                
                response = await client.post(
                    f"{SERVER_URL}/call",
                    json={
                        "name": tool_name,
                        "arguments": arguments
                    },
                    headers=headers
                )
                response.raise_for_status()
                return response.json()
            
            else:
                return {
                    "error": {
                        "code": -32601,
                        "message": f"Method not found: {method}"
                    }
                }
    
    except httpx.HTTPError as e:
        return {
            "error": {
                "code": -32603,
                "message": f"HTTP error: {str(e)}"
            }
        }
    except Exception as e:
        return {
            "error": {
                "code": -32603,
                "message": f"Error: {str(e)}"
            }
        }


def main():
    """Main entry point for console script"""
    asyncio.run(main_async())

async def main_async():
    """Read from stdin, proxy to HTTP, write to stdout"""
    for line in sys.stdin:
        if not line.strip():
            continue
        
        try:
            request = json.loads(line.strip())
            response = await handle_request(request)
            
            # Include request ID if present
            if "id" in request:
                response["id"] = request["id"]
            
            print(json.dumps(response))
            sys.stdout.flush()
        
        except json.JSONDecodeError:
            # Skip invalid JSON
            continue
        except Exception as e:
            error_response = {
                "error": {
                    "code": -32603,
                    "message": str(e)
                }
            }
            try:
                if "id" in request:
                    error_response["id"] = request.get("id")
            except:
                pass  # request might not be defined if error occurred before parsing
            print(json.dumps(error_response))
            sys.stdout.flush()


# No database initialization needed for proxy


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

