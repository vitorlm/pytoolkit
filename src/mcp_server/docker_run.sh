#!/bin/bash

# Docker Helper Script for PyToolkit MCP Server
# This script provides easy commands to run the MCP server with Docker

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

show_help() {
    echo -e "${BLUE}PyToolkit MCP Server - Docker Helper${NC}"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo -e "  ${GREEN}stdio${NC}     Start MCP server in stdio mode (for local Claude Desktop)"
    echo -e "  ${GREEN}http${NC}      Start MCP server in HTTP mode (for remote connections)"
    echo -e "  ${GREEN}both${NC}      Start both servers simultaneously"
    echo -e "  ${GREEN}stop${NC}      Stop all running MCP servers"
    echo -e "  ${GREEN}logs${NC}      Show logs from running containers"
    echo -e "  ${GREEN}build${NC}     Build/rebuild the Docker images"
    echo -e "  ${GREEN}status${NC}    Show status of MCP containers"
    echo ""
    echo "Examples:"
    echo "  $0 stdio    # Start stdio server"
    echo "  $0 http     # Start HTTP server on port 8000"
    echo "  $0 both     # Start both servers"
}

case "$1" in
    "stdio")
        echo -e "${GREEN}Starting MCP Server in stdio mode...${NC}"
        docker compose up mcp-server-stdio -d
        ;;
    "http")
        echo -e "${GREEN}Starting MCP Server in HTTP mode...${NC}"
        docker compose up mcp-server-http -d
        echo -e "${YELLOW}HTTP server will be available at: http://localhost:8000${NC}"
        ;;
    "both")
        echo -e "${GREEN}Starting both MCP servers...${NC}"
        docker compose up -d
        echo -e "${YELLOW}HTTP server will be available at: http://localhost:8000${NC}"
        ;;
    "stop")
        echo -e "${YELLOW}Stopping all MCP servers...${NC}"
        docker compose down
        ;;
    "logs")
        echo -e "${BLUE}Showing logs (press Ctrl+C to exit)...${NC}"
        docker compose logs -f
        ;;
    "build")
        echo -e "${BLUE}Building Docker images...${NC}"
        docker compose build
        ;;
    "status")
        echo -e "${BLUE}MCP Server Status:${NC}"
        docker compose ps
        ;;
    "help"|"-h"|"--help"|"")
        show_help
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac
