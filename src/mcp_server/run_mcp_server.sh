#!/bin/bash

# PyToolkit MCP Server Runner
# This script activates the virtual environment and runs the MCP server

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Check if venv exists
if [ ! -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    echo "Virtual environment not found at $PROJECT_ROOT/.venv/bin/activate" >&2
    exit 1
fi

# Change to project root directory
cd "$PROJECT_ROOT"

# Activate virtual environment
source "$PROJECT_ROOT/.venv/bin/activate"

# Set PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT/src"

# MCP Server runs in STDIO mode only (for local usage)
# Don't output any messages as they interfere with JSON-RPC protocol
exec python "$PROJECT_ROOT/src/mcp_server/management_mcp_server.py" "$@"
