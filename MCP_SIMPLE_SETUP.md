# Simple MCP Setup - Copy & Paste Ready! 🎯

## Option 1: One-Command Install (Easiest!)

**Step 1:** Install the proxy (one time):
```bash
pip install git+https://github.com/yourusername/fly-cornhole.git#subdirectory=.
```

**Step 2:** Copy this into Claude Desktop config:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`  
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

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

**That's it!** Just restart Claude Desktop.

---

## Option 2: Using npx (No Installation!)

If you have Node.js installed, use this instead:

```json
{
  "mcpServers": {
    "cornhole-stats": {
      "command": "npx",
      "args": ["-y", "https://github.com/yourusername/fly-cornhole.git#mcp-proxy-node", "https://fly-cornhole.fly.dev/mcp"]
    }
  }
}
```

---

## Option 3: Download & Run (No Installation!)

1. Download `mcp_proxy.py` from this repo
2. Put it anywhere (like `~/bin/` or `C:\bin\`)
3. Use this config:

```json
{
  "mcpServers": {
    "cornhole-stats": {
      "command": "python",
      "args": ["mcp_proxy.py"],
      "env": {
        "MCP_SERVER_URL": "https://fly-cornhole.fly.dev/mcp",
        "PATH": "/usr/local/bin:/usr/bin:/bin:~/bin"
      }
    }
  }
}
```

---

## What It Does

This connects Claude Desktop to your remote cornhole statistics server, allowing you to ask questions like:

- "Who has the highest PPR this season?"
- "Show me stats for John Smith"
- "Compare player 12345 across seasons 11, 10, and 9"
- "Find all players from California with skill level P"
- "Who are the top 10 players by CPI?"

---

## Troubleshooting

**Command not found?**
- Make sure Python is installed: `python --version`
- Make sure the package is installed: `pip list | grep cornhole`

**Connection errors?**
- Check that the server URL is correct: `https://fly-cornhole.fly.dev/mcp`
- Test the server: Open `https://fly-cornhole.fly.dev/mcp/tools` in your browser

**Still having issues?**
- Restart Claude Desktop after making config changes
- Check Claude Desktop logs for error messages

---

## Server URL

If the server is hosted at a different URL, just replace `https://fly-cornhole.fly.dev/mcp` in the JSON above with your actual server URL.
