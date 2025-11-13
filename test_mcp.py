#!/usr/bin/env python3
"""
Quick test script for MCP proxy
"""

import asyncio
import json
import os
import sys

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

from mcp_proxy import handle_request

async def test():
    """Test the MCP proxy with a simple request"""
    print("Testing MCP proxy...")
    print("=" * 50)
    
    # Test initialize
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {}
    }
    print("\n1. Testing initialize:")
    response = await handle_request(request)
    print(json.dumps(response, indent=2))
    
    # Test tools/list
    request = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    }
    print("\n2. Testing tools/list:")
    response = await handle_request(request)
    print(json.dumps(response, indent=2))
    
    # Test a tool call
    request = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "get_top_players",
            "arguments": {
                "stat": "pts_per_rnd",
                "limit": 3,
                "season": 11
            }
        }
    }
    print("\n3. Testing get_top_players tool:")
    response = await handle_request(request)
    print(json.dumps(response, indent=2))
    
    print("\n" + "=" * 50)
    print("Test complete!")

if __name__ == "__main__":
    asyncio.run(test())

