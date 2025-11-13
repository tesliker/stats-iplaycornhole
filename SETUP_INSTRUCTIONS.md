# Setup Instructions for Users

## Quick Setup (Copy & Paste)

### Step 1: Install the proxy

Run this command once:
```bash
pip install git+https://github.com/YOUR_USERNAME/fly-cornhole.git
```

*(Replace YOUR_USERNAME with the actual GitHub username)*

### Step 2: Add to Claude Desktop

Open your Claude Desktop config file:
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

Add this JSON (merge with existing `mcpServers` if you have other servers):

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

### Step 3: Restart Claude Desktop

That's it! Now you can ask Claude questions about cornhole statistics.

---

## Example Questions

Once connected, try:
- "Who has the highest PPR this season?"
- "Show me stats for John Smith"
- "Compare player 12345 across seasons 11, 10, and 9"
- "Find all players from California"
- "Who are the top 10 players by CPI?"

---

## Troubleshooting

**"cornhole-mcp: command not found"**
- Make sure pip install completed successfully
- Check that Python scripts are in your PATH
- Try: `python -m pip install git+https://github.com/YOUR_USERNAME/fly-cornhole.git`

**Connection errors**
- Verify the server URL: `https://fly-cornhole.fly.dev/mcp/tools`
- Check that the server is running

**Still not working?**
- Restart Claude Desktop completely
- Check Claude Desktop logs for errors
- Make sure Python 3.8+ is installed

