#!/usr/bin/env python3
"""
Setup script to install MCP proxy as a command-line tool
"""

import setuptools

setuptools.setup(
    name="cornhole-mcp-proxy",
    version="1.0.0",
    py_modules=["mcp_proxy"],
    entry_points={
        "console_scripts": [
            "cornhole-mcp=mcp_proxy:main",
        ],
    },
    install_requires=[
        "httpx>=0.27.0",
    ],
)

