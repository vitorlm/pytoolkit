#!/bin/bash

# Enhanced Weekly Metrics Report Script with Custom Date Support
# Usage: ./run_reports_custom_date.sh [YYYY-MM-DD] [--config team|tribe]

# Handle help option
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    echo "🚀 Weekly Metrics Report Script (Custom Date)"
    echo ""
    echo "DESCRIPTION:"
    echo "  Generate reports for a specific date/week instead of current week."
    echo ""
    echo "USAGE:"
    echo "  ./run_reports_custom_date.sh                           # Use current date"
    echo "  ./run_reports_custom_date.sh 2025-08-26               # Use specific date"
    echo "  ./run_reports_custom_date.sh 2025-08-26 --config team # Use specific date + config"
    echo "  ./run_reports_custom_date.sh --help                   # Show this help"
    echo ""
    echo "EXAMPLES:"
    echo "  ./run_reports_custom_date.sh 2025-08-26               # Reports for week of Aug 26"
    echo "  ./run_reports_custom_date.sh 2025-09-02 --config tribe # Tribe reports for week of Sep 2"
    echo ""
    exit 0
fi

# Parse custom date parameter
CUSTOM_DATE=""
CONFIG_PARAM=""

for arg in "$@"; do
    if [[ "$arg" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        CUSTOM_DATE="$arg"
    elif [[ "$arg" == "--config" ]]; then
        CONFIG_PARAM="$arg"
    elif [[ "$arg" == "team" || "$arg" == "tribe" ]]; then
        CONFIG_TYPE="$arg"
    fi
done

# Set TODAY to custom date or current date
if [[ -n "$CUSTOM_DATE" ]]; then
    echo "📅 Using custom date: $CUSTOM_DATE"
    export CUSTOM_TODAY="$CUSTOM_DATE"
else
    echo "📅 Using current date: $(date +%Y-%m-%d)"
    export CUSTOM_TODAY=$(date +%Y-%m-%d)
fi

# Call original script with modified environment
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Temporarily modify the original script to use custom date
sed "s/TODAY=\$(date +%Y-%m-%d)/TODAY=\"$CUSTOM_TODAY\"/" "$SCRIPT_DIR/run_reports.sh" > "/tmp/run_reports_temp.sh"
chmod +x "/tmp/run_reports_temp.sh"

# Run the modified script
if [[ -n "$CONFIG_PARAM" && -n "$CONFIG_TYPE" ]]; then
    "/tmp/run_reports_temp.sh" "$CONFIG_PARAM" "$CONFIG_TYPE"
else
    "/tmp/run_reports_temp.sh"
fi

# Cleanup
rm -f "/tmp/run_reports_temp.sh"
