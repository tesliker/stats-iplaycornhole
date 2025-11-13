# Setting Up MCP in Cursor IDE

## Quick Setup

### Step 1: Install the Proxy (One Time)

First, install the MCP proxy package. From your project directory:

```bash
cd /Users/tesliker/work/fly-cornhole
pip install -e .
```

This will install the `cornhole-mcp` command globally.

**Verify it works:**
```bash
which cornhole-mcp
# Should show a path like /usr/local/bin/cornhole-mcp
```

### Step 2: Configure Cursor

**Option A: Using Cursor Settings UI**

1. Open Cursor Settings:
   - **macOS**: `Cmd + ,` (or Cursor → Settings)
   - **Windows/Linux**: `Ctrl + ,`

2. Search for "MCP" or navigate to **Features** → **Model Context Protocol**

3. Click **"Add New MCP Server"** or **"Edit MCP Settings"**

4. Add:
   - **Name**: `cornhole-stats`
   - **Command**: `cornhole-mcp`
   - **Environment Variables**: Add `MCP_SERVER_URL` = `http://localhost:8000/mcp` (for local testing)

**Option B: Edit Config File Directly**

1. Open the MCP config file:

   **macOS**: 
   ```bash
   open ~/Library/Application\ Support/Cursor/User/globalStorage/mcp.json
   ```
   
   **Windows**: 
   ```
   %APPDATA%\Cursor\User\globalStorage\mcp.json
   ```
   
   **Linux**: 
   ```bash
   ~/.config/Cursor/User/globalStorage/mcp.json
   ```

2. Add this configuration (create the file if it doesn't exist):

```json
{
  "mcpServers": {
    "cornhole-stats": {
      "command": "cornhole-mcp",
      "env": {
        "MCP_SERVER_URL": "http://localhost:8000/mcp"
      }
    }
  }
}
```

**Note**: For local testing, use `http://localhost:8000/mcp`. For production, use `https://fly-cornhole.fly.dev/mcp`.

### Step 3: Start Your Local Server (For Testing)

Before testing in Cursor, make sure your FastAPI server is running:

```bash
cd /Users/tesliker/work/fly-cornhole
uvicorn main:app --reload
```

The server should start on `http://localhost:8000`

### Step 4: Test the Proxy First

Before adding to Cursor, test the proxy works:

```bash
# Quick test
python test_mcp.py

# Or test manually
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | cornhole-mcp
```

You should see a list of tools returned.

### Step 5: Restart Cursor

Close and reopen Cursor to load the MCP server.

### Step 6: Test in Cursor

1. Open a chat in Cursor (Cmd+L or Ctrl+L)
2. Try asking:
   - "Who has the highest PPR this season?"
   - "Show me stats for a player"
   - "Get the top 5 players by CPI"
   - "Search for players from California"

If MCP is working, Cursor should use the tools to query your database.

---

## Alternative: Test Locally First

If you want to test against your local server before deploying:

1. Start your local server:
   ```bash
   uvicorn main:app --reload
   ```

2. Update the config to use localhost:
   ```json
   {
     "mcpServers": {
       "cornhole-stats": {
         "command": "cornhole-mcp",
         "env": {
           "MCP_SERVER_URL": "http://localhost:8000/mcp"
         }
       }
     }
   }
   ```

3. Restart Cursor

---

## Troubleshooting

### Command Not Found

If `cornhole-mcp` command isn't found:

1. Check installation:
   ```bash
   which cornhole-mcp
   # or
   python -m mcp_proxy
   ```

2. If it's not found, install it:
   ```bash
   cd /Users/tesliker/work/fly-cornhole
   pip install -e .
   ```

3. If still not working, use the full Python path:
   ```json
   {
     "mcpServers": {
       "cornhole-stats": {
         "command": "python",
         "args": ["-m", "mcp_proxy"],
         "env": {
           "MCP_SERVER_URL": "http://localhost:8000/mcp"
         }
       }
     }
   }
   ```

### Connection Errors

1. **Test the server directly:**
   ```bash
   curl http://localhost:8000/mcp/tools
   ```

2. **Check Cursor logs:**
   - Open Cursor → Help → Toggle Developer Tools
   - Check the Console for MCP errors

3. **Verify the proxy works:**
   ```bash
   echo '{"method":"tools/list"}' | cornhole-mcp
   ```

### MCP Settings Not Found

If you can't find MCP settings in Cursor:
- Make sure you're using a recent version of Cursor
- MCP support might be in beta - check Cursor's release notes
- Try accessing the config file directly at the path above

---

## Quick Test Script

Create a test file to verify everything works:

```bash
# Test the proxy directly
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | python mcp_proxy.py

# Should return a list of tools
```

---

## Next Steps

Once it's working in Cursor, you can:
1. Deploy to Fly.io
2. Update the URL to the production server
3. Share the setup instructions with others

