#!/bin/bash

# Enhanced Weekly Team Metrics Report Script
# Purpose: Generate comprehensive team performance reports combining JIRA, SonarQube, and LinearB data
# Usage: Run this script weekly to get consistent team metrics for analysis

set -e  # Exit on any error

# Load configuration from config file if it exists
if [ -f "config_reports.env" ]; then
    echo "Loading configuration from config_reports.env..."
    source config_reports.env
else
    echo "Warning: config_reports.env not found, using default values"
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

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "=== WEEKLY TEAM METRICS REPORT - $(date) ==="
echo "Project: $PROJECT_KEY | Team: $TEAM"
echo "Output Directory: $OUTPUT_DIR"
echo ""

# Calculate date ranges for weekly comparison
# Simple and reliable approach: use fixed day arithmetic
# Today is 2025-07-22 (Tuesday)
# We want last completed week (Mon-Sun) and the week before

# For reliable weekly reports, calculate from last Sunday backwards
# Last Sunday: 2025-07-20, so last week = 2025-07-14 to 2025-07-20
# Week before: 2025-07-07 to 2025-07-13

LAST_SUNDAY="2025-07-20"
LAST_MONDAY="2025-07-14"
WEEK_BEFORE_SUNDAY="2025-07-13"
WEEK_BEFORE_MONDAY="2025-07-07"

# For dynamic calculation (uncomment when ready for automation):
# LAST_SUNDAY=$(date -d 'last sunday' +%Y-%m-%d 2>/dev/null || date -v-$((($(date +%u) % 7) + 0))d +%Y-%m-%d)
# LAST_MONDAY=$(date -d "$LAST_SUNDAY - 6 days" +%Y-%m-%d 2>/dev/null || date -j -f "%Y-%m-%d" "$LAST_SUNDAY" -v-6d +%Y-%m-%d)
# WEEK_BEFORE_SUNDAY=$(date -d "$LAST_SUNDAY - 7 days" +%Y-%m-%d 2>/dev/null || date -j -f "%Y-%m-%d" "$LAST_SUNDAY" -v-7d +%Y-%m-%d)
# WEEK_BEFORE_MONDAY=$(date -d "$WEEK_BEFORE_SUNDAY - 6 days" +%Y-%m-%d 2>/dev/null || date -j -f "%Y-%m-%d" "$WEEK_BEFORE_SUNDAY" -v-6d +%Y-%m-%d)

# Set up time periods for JIRA
JIRA_LAST_WEEK="$LAST_MONDAY to $LAST_SUNDAY"
JIRA_WEEK_BEFORE="$WEEK_BEFORE_MONDAY to $WEEK_BEFORE_SUNDAY"
JIRA_TWO_WEEKS="$WEEK_BEFORE_MONDAY to $LAST_SUNDAY"

echo "📅 Date ranges calculated:"
echo "   • Last week: $JIRA_LAST_WEEK"
echo "   • Week before: $JIRA_WEEK_BEFORE" 
echo "   • Combined 2 weeks: $JIRA_TWO_WEEKS"
echo ""

# JIRA REPORTS
echo "📊 [1/6] Running JIRA Bug & Support report for last 2 weeks..."
echo "   Date range: $JIRA_TWO_WEEKS"
python src/main.py syngenta jira issue-adherence \
  --project-key "$PROJECT_KEY" \
  --time-period "$JIRA_TWO_WEEKS" \
  --issue-types 'Bug,Support' \
  --status-categories 'Done' \
  --include-no-due-date \
  --output-file "$OUTPUT_DIR/jira-bugs-support-2weeks.json" \
  --team "$TEAM"

echo "📊 [2/6] Running JIRA Bug & Support report for last week..."
echo "   Date range: $JIRA_LAST_WEEK"
python src/main.py syngenta jira issue-adherence \
  --project-key "$PROJECT_KEY" \
  --time-period "$JIRA_LAST_WEEK" \
  --issue-types 'Bug,Support' \
  --status-categories 'Done' \
  --include-no-due-date \
  --output-file "$OUTPUT_DIR/jira-bugs-support-lastweek.json" \
  --team "$TEAM"

echo "📊 [3/6] Running JIRA Bug & Support report for week before last..."
echo "   Date range: $JIRA_WEEK_BEFORE"
python src/main.py syngenta jira issue-adherence \
  --project-key "$PROJECT_KEY" \
  --time-period "$JIRA_WEEK_BEFORE" \
  --issue-types 'Bug,Support' \
  --status-categories 'Done' \
  --include-no-due-date \
  --output-file "$OUTPUT_DIR/jira-bugs-support-weekbefore.json" \
  --team "$TEAM"

echo "📊 [4/6] Running JIRA Tasks report for last 2 weeks..."
echo "   Date range: $JIRA_TWO_WEEKS"
python src/main.py syngenta jira issue-adherence \
  --project-key "$PROJECT_KEY" \
  --time-period "$JIRA_TWO_WEEKS" \
  --issue-types 'Story,Task,Bug,Epic,Technical Debt,Improvement' \
  --status-categories 'Done' \
  --include-no-due-date \
  --output-file "$OUTPUT_DIR/jira-tasks-2weeks.json" \
  --team "$TEAM"

# SONARQUBE REPORTS
echo "🔍 [5/6] Running SonarQube code quality metrics report..."
CLEAR_CACHE_FLAG=""
if [ "$CLEAR_CACHE" = "true" ]; then
    CLEAR_CACHE_FLAG="--clear-cache"
fi

python src/main.py syngenta sonarqube sonarqube \
  --operation list-projects \
  --organization "$SONARQUBE_ORGANIZATION" \
  --include-measures \
  --output-file "$OUTPUT_DIR/sonarqube-quality-metrics.json" \
  $CLEAR_CACHE_FLAG \
  --project-keys "$SONARQUBE_PROJECT_KEYS"

# LINEARB REPORTS
echo "🚀 [6/6] Running LinearB team engineering metrics..."
# Use the same date ranges as JIRA for consistency
# LinearB expects YYYY-MM-DD,YYYY-MM-DD format for custom granularity
LINEARB_TIME_RANGE="$WEEK_BEFORE_MONDAY,$LAST_SUNDAY"
echo "   Date range: $LINEARB_TIME_RANGE"
python src/main.py linearb engineering-metrics \
  --team-ids "$LINEARB_TEAM_IDS" \
  --time-range "$LINEARB_TIME_RANGE" \
  --aggregation "raw" \
  --format "json" \
  --output-folder "$OUTPUT_DIR" \
  || echo "Warning: LinearB engineering metrics failed - check configuration and ensure LinearB environment is configured"

echo "📈 [EXTRA] Running LinearB export report..."
python src/main.py linearb export-report \
  --team-ids "$LINEARB_TEAM_IDS" \
  --time-range "last-week" \
  --format "json" \
  --filter-type "team" \
  --granularity "custom" \
  || echo "Warning: LinearB export report failed - check configuration"

# ADDITIONAL JIRA INSIGHTS (Optional - controlled by config)
if [ "$INCLUDE_OPTIONAL_REPORTS" = "true" ]; then
    echo ""
    echo "🔍 OPTIONAL: Additional JIRA insights..."

    # Issue creation analysis
    echo "📝 Running JIRA issue creation analysis..."
    python src/main.py syngenta jira issues-creation-analysis \
      --projects "$PROJECT_KEY" \
      --time-period "last-month" \
      --aggregation "weekly" \
      --issue-types "Story,Task,Bug,Epic,Technical Debt,Improvement" \
      --output-file "$OUTPUT_DIR/jira-creation-analysis.json" \
      --include-summary \
      --clear-cache \
      || echo "Warning: Issue creation analysis failed"

    # Epic monitoring (if applicable)
    echo "📊 Running JIRA epic monitoring..."
    python src/main.py syngenta jira epic-monitor \
      || echo "Warning: Epic monitoring failed"
fi

# REPORT SUMMARY
echo ""
echo "✅ WEEKLY REPORT GENERATION COMPLETED!"
echo "=== SUMMARY ==="
echo "📁 All reports saved to: $OUTPUT_DIR"
echo "📊 Generated reports:"
echo "   • JIRA Bug & Support metrics (2 weeks combined)"
echo "   • JIRA Bug & Support metrics (last week only)"
echo "   • JIRA Bug & Support metrics (week before last)"
echo "   • JIRA Task completion metrics (2 weeks combined)"
echo "   • SonarQube code quality metrics"
echo "   • LinearB team performance metrics"
echo "   • LinearB export report"
echo ""
echo "📈 Week-over-week comparison data:"
echo "   • Compare: $OUTPUT_DIR/jira-bugs-support-lastweek.json"
echo "   • Against: $OUTPUT_DIR/jira-bugs-support-weekbefore.json"
echo ""
echo "📈 Next steps:"
echo "   1. Review generated JSON files for insights"
echo "   2. Compare last week vs week before metrics"
echo "   3. Analyze trends in the data"
echo "   4. Share relevant metrics with the team"
echo ""
echo "🔄 To run this automatically weekly, add to cron:"
echo "   0 9 * * 1 cd $(pwd) && ./run_reports.sh >> logs/weekly_reports.log 2>&1"
echo ""

# List generated files
if [ -d "$OUTPUT_DIR" ]; then
    echo "📋 Generated files:"
    ls -la "$OUTPUT_DIR"
fi
