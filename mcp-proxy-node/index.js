#!/usr/bin/env node
/**
 * MCP Proxy - Connects local MCP clients to remote HTTP MCP server
 * 
 * Usage: npx cornhole-mcp-proxy@latest <server-url>
 * Or: cornhole-mcp-proxy <server-url>
 */

const axios = require('axios');
const readline = require('readline');

const SERVER_URL = process.argv[2] || process.env.MCP_SERVER_URL || 'https://fly-cornhole.fly.dev/mcp';
const API_KEY = process.env.MCP_API_KEY;

async function handleRequest(request) {
  const method = request.method;
  const params = request.params || {};
  
  const headers = {};
  if (API_KEY) {
    headers['X-API-Key'] = API_KEY;
  }
  
  try {
    if (method === 'initialize') {
      return {
        protocolVersion: '2024-11-05',
        capabilities: {
          tools: {}
        },
        serverInfo: {
          name: 'cornhole-stats-remote',
          version: '1.0.0'
        }
      };
    } else if (method === 'tools/list') {
      const response = await axios.get(`${SERVER_URL}/tools`, { headers, timeout: 30000 });
      return {
        tools: response.data.tools || []
      };
    } else if (method === 'tools/call') {
      const toolName = params.name;
      const arguments_ = params.arguments || {};
      
      const response = await axios.post(
        `${SERVER_URL}/call`,
        {
          name: toolName,
          arguments: arguments_
        },
        { headers, timeout: 30000 }
      );
      
      return response.data;
    } else {
      return {
        error: {
          code: -32601,
          message: `Method not found: ${method}`
        }
      };
    }
  } catch (error) {
    if (error.response) {
      return {
        error: {
          code: -32603,
          message: `HTTP error: ${error.response.status} ${error.response.statusText}`
        }
      };
    } else if (error.request) {
      return {
        error: {
          code: -32603,
          message: `Network error: ${error.message}`
        }
      };
    } else {
      return {
        error: {
          code: -32603,
          message: `Error: ${error.message}`
        }
      };
    }
  }
}

async function main() {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false
  });
  
  rl.on('line', async (line) => {
    if (!line.trim()) return;
    
    try {
      const request = JSON.parse(line.trim());
      const response = await handleRequest(request);
      
      // Include request ID if present
      if (request.id !== undefined) {
        response.id = request.id;
      }
      
      console.log(JSON.stringify(response));
    } catch (error) {
      if (error instanceof SyntaxError) {
        // Skip invalid JSON
        return;
      }
      
      const errorResponse = {
        error: {
          code: -32603,
          message: error.message
        }
      };
      
      try {
        const request = JSON.parse(line.trim());
        if (request.id !== undefined) {
          errorResponse.id = request.id;
        }
      } catch {
        // Ignore
      }
      
      console.log(JSON.stringify(errorResponse));
    }
  });
}

// Handle errors
process.on('SIGINT', () => {
  process.exit(0);
});

main().catch((error) => {
  console.error(JSON.stringify({
    error: {
      code: -32603,
      message: error.message
    }
  }));
  process.exit(1);
});

