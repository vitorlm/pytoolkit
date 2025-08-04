#!/bin/bash

# Enhanced Weekly Team Metrics Report Script
# Purpose: Generate comprehensive team performance reports combining JIRA, SonarQube, and LinearB data
# Usage: Run this script weekly to get consistent team metrics for analysis

set -e  # Exit on any error

# Load configuration from config file if it exists
if [ -f "config_reports.env" ]; then
    echo "🔧 Loading configuration from config_reports.env..."
    source config_reports.env
else
    echo "⚠️  Warning: config_reports.env not found."
    exit 1
fi

# --- Ensure Python venv is active ---
if [ -z "$VIRTUAL_ENV" ]; then
    if [ -f ".venv/bin/activate" ]; then
        echo "🐍 Activating Python virtual environment (.venv)..."
        source .venv/bin/activate
    else
        echo "❌ Python virtual environment not found. Run ./setup.sh to create it."
        exit 1
    fi
else
    echo "🐍 Python virtual environment already active."
fi

# Configuration (with defaults if not loaded from config)
PROJECT_KEY="${PROJECT_KEY:-CWS}"
TEAM="${TEAM:-Catalog}"
LINEARB_TEAM_IDS="${LINEARB_TEAM_IDS:-41576}"  # Farm Operations Team
SONARQUBE_ORGANIZATION="${SONARQUBE_ORGANIZATION:-syngenta-digital}"
INCLUDE_OPTIONAL_REPORTS="${INCLUDE_OPTIONAL_REPORTS:-true}"
CLEAR_CACHE="${CLEAR_CACHE:-true}"
USE_DATE_SUFFIX="${USE_DATE_SUFFIX:-true}"
OUTPUT_BASE_DIR="${OUTPUT_BASE_DIR:-output}"

# Set up output directory
if [ "$USE_DATE_SUFFIX" = "true" ]; then
    DATE_SUFFIX=$(date +"%Y%m%d")
    OUTPUT_DIR="${OUTPUT_BASE_DIR}/weekly_reports_${DATE_SUFFIX}"
else
    OUTPUT_DIR="${OUTPUT_BASE_DIR}/weekly_reports"
fi

# Clean and create output directory
if [ -d "$OUTPUT_DIR" ]; then
    echo "🧹 Cleaning existing output directory: $OUTPUT_DIR"
    rm -rf "$OUTPUT_DIR"
fi
mkdir -p "$OUTPUT_DIR"

# Header
echo ""
echo "============================================================"
echo "           📊 WEEKLY TEAM METRICS REPORT TOOL"
echo "============================================================"
echo "📅 Run date:        $(date)"
echo "🏷️  Project:         $PROJECT_KEY"
echo "👥 Team:            $TEAM"
echo "📁 Output folder:   $OUTPUT_DIR"
echo ""



# --- Dynamic date range calculation ---
TODAY=$(date +%Y-%m-%d)
WEEKDAY_NUM=$(date -jf "%Y-%m-%d" "$TODAY" +%u)
THIS_MONDAY=$(date -jf "%Y-%m-%d" -v-"$((WEEKDAY_NUM - 1))"d "$TODAY" +%Y-%m-%d)
LAST_MONDAY=$(date -jf "%Y-%m-%d" -v-1w "$THIS_MONDAY" +%Y-%m-%d)
LAST_SUNDAY=$(date -jf "%Y-%m-%d" -v+6d "$LAST_MONDAY" +%Y-%m-%d)
WEEK_BEFORE_MONDAY=$(date -jf "%Y-%m-%d" -v-2w "$THIS_MONDAY" +%Y-%m-%d)
WEEK_BEFORE_SUNDAY=$(date -jf "%Y-%m-%d" -v+6d "$WEEK_BEFORE_MONDAY" +%Y-%m-%d)

JIRA_LAST_WEEK="$LAST_MONDAY to $LAST_SUNDAY"
JIRA_WEEK_BEFORE="$WEEK_BEFORE_MONDAY to $WEEK_BEFORE_SUNDAY"
JIRA_TWO_WEEKS="$WEEK_BEFORE_MONDAY to $LAST_SUNDAY"

# Print ranges
echo "📆 Reporting periods:"
printf "   • %-18s %s\n" "Last week:" "$JIRA_LAST_WEEK"
printf "   • %-18s %s\n" "Week before:" "$JIRA_WEEK_BEFORE"
printf "   • %-18s %s\n" "Combined 2 weeks:" "$JIRA_TWO_WEEKS"
echo ""

# JIRA Reports
STEP=1
echo "📊 [$STEP/8] JIRA – Bug & Support (Combined 2 Weeks)"
echo "   ⏳ Period: $JIRA_TWO_WEEKS"
((STEP++))
python src/main.py syngenta jira issue-adherence \
  --project-key "$PROJECT_KEY" \
  --time-period "$JIRA_TWO_WEEKS" \
  --issue-types 'Bug,Support' \
  --status-categories 'Done' \
  --include-no-due-date \
  --output-file "$OUTPUT_DIR/jira-bugs-support-2-weeks.json" \
  --team "$TEAM"

echo "📊 [$STEP/8] JIRA – Bug & Support (Last Week)"
echo "   ⏳ Period: $JIRA_LAST_WEEK"
((STEP++))
python src/main.py syngenta jira issue-adherence \
  --project-key "$PROJECT_KEY" \
  --time-period "$JIRA_LAST_WEEK" \
  --issue-types 'Bug,Support' \
  --status-categories 'Done' \
  --include-no-due-date \
  --output-file "$OUTPUT_DIR/jira-bugs-support-lastweek.json" \
  --team "$TEAM"

echo "📊 [$STEP/8] JIRA – Bug & Support (Week Before)"
echo "   ⏳ Period: $JIRA_WEEK_BEFORE"
((STEP++))
python src/main.py syngenta jira issue-adherence \
  --project-key "$PROJECT_KEY" \
  --time-period "$JIRA_WEEK_BEFORE" \
  --issue-types 'Bug,Support' \
  --status-categories 'Done' \
  --include-no-due-date \
  --output-file "$OUTPUT_DIR/jira-bugs-support-weekbefore.json" \
  --team "$TEAM"

echo "📊 [$STEP/8] JIRA – Task Completion (2 Weeks)"
echo "   ⏳ Period: $JIRA_TWO_WEEKS"
((STEP++))
python src/main.py syngenta jira issue-adherence \
  --project-key "$PROJECT_KEY" \
  --time-period "$JIRA_TWO_WEEKS" \
  --issue-types 'Story,Task,Epic,Technical Debt,Improvement' \
  --status-categories 'Done' \
  --include-no-due-date \
  --output-file "$OUTPUT_DIR/jira-tasks-2weeks.json" \
  --team "$TEAM"

echo "🐛 [$STEP/8] JIRA – Open Issues (Bugs & Support)"
echo "   📋 Current open issues for team: $TEAM"
((STEP++))
python src/main.py syngenta jira open-issues \
  --project-key "$PROJECT_KEY" \
  --issue-types 'Bug,Support' \
  --team "$TEAM" \
  --output-file "$OUTPUT_DIR/jira-open-bugs-support.json"

# JIRA Cycle Time Report
echo "⏱️  [$STEP/8] JIRA – Cycle Time Analysis (Last Week)"
echo "   ⏳ Period: $JIRA_LAST_WEEK"
((STEP++))
python src/main.py syngenta jira cycle-time \
  --project-key "$PROJECT_KEY" \
  --time-period "$JIRA_LAST_WEEK" \
  --output-file "$OUTPUT_DIR/jira-cycle-time-lastweek.json"

# SonarQube Report
echo "🔍 [$STEP/8] SonarQube – Code Quality Metrics"
((STEP++))
CLEAR_CACHE_FLAG=""
[ "$CLEAR_CACHE" = "true" ] && CLEAR_CACHE_FLAG="--clear-cache"

python src/main.py syngenta sonarqube sonarqube \
  --operation list-projects \
  --organization "$SONARQUBE_ORGANIZATION" \
  --include-measures \
  --output-file "$OUTPUT_DIR/sonarqube-quality-metrics.json" \
  $CLEAR_CACHE_FLAG \
  --project-keys "$SONARQUBE_PROJECT_KEYS"

# LinearB Report
echo "🚀 [$STEP/8] LinearB – Engineering Metrics"
LINEARB_TIME_RANGE="$WEEK_BEFORE_MONDAY,$LAST_SUNDAY"
echo "   ⏳ Period: $LINEARB_TIME_RANGE"
python src/main.py linearb export-report \
  --team-ids "$LINEARB_TEAM_IDS" \
  --time-range "$LINEARB_TIME_RANGE" \
  --format csv \
  --filter-type team \
  --granularity 1w \
  --beautified \
  --return-no-data \
  --aggregation avg \
  --output-folder "$OUTPUT_DIR" \
  || echo "⚠️  Warning: LinearB export report failed - check configuration"

# Summary
echo ""
echo "✅ WEEKLY REPORT GENERATION COMPLETED!"
echo "════════════════════════════════════════════════════════════"
echo "📁 Output directory: $OUTPUT_DIR"
echo ""
echo "📊 Reports generated:"
printf "   • %-45s %s\n" "Bug & Support (2 weeks):"       "jira-bugs-support-2-weeks.json"
printf "   • %-45s %s\n" "Bug & Support (last week):"     "jira-bugs-support-lastweek.json"
printf "   • %-45s %s\n" "Bug & Support (week before):"   "jira-bugs-support-weekbefore.json"
printf "   • %-45s %s\n" "Tasks (2 weeks):"              "jira-tasks-2weeks.json"
printf "   • %-45s %s\n" "Open Bugs & Support:"          "jira-open-bugs-support.json"
printf "   • %-45s %s\n" "Cycle Time (last week):"       "jira-cycle-time-lastweek.json"
printf "   • %-45s %s\n" "SonarQube (13 projects):"      "sonarqube-quality-metrics.json"
printf "   • %-45s %s\n" "LinearB (CSV format):"         "linearb_export*.csv"
echo ""
echo "📈 Week-over-week comparison:"
echo "   → Last week:    $OUTPUT_DIR/jira-bugs-support-lastweek.json"
echo "   ← Week before:  $OUTPUT_DIR/jira-bugs-support-weekbefore.json"
echo ""
echo "📋 Generated files:"
ls -1 "$OUTPUT_DIR" | sed 's/^/   • /'
