#!/bin/bash

# Enhanced Weekly Metrics Report Script
# Purpose: Generate comprehensive performance reports combining JIRA, SonarQube, and LinearB data
# Usage: Run this script weekly to get consistent metrics for analysis
# Supports both team-specific and tribe-wide reporting based on configuration

# Handle help option and config selection
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    echo "🚀 Weekly Metrics Report Script"
    echo ""
    echo "DESCRIPTION:"
    echo "  Generates comprehensive weekly reports combining JIRA, SonarQube, and LinearB metrics."
    echo "  Automatically detects configuration type and adapts behavior accordingly."
    echo ""
    echo "USAGE:"
    echo "  ./run_reports.sh                      # Interactive mode (if multiple configs)"
    echo "  ./run_reports.sh --config team        # Use team-specific config"
    echo "  ./run_reports.sh --config tribe       # Use tribe-wide config"
    echo "  ./run_reports.sh --help               # Show this help"
    echo ""
    echo "CONFIGURATION:"
    echo "  Place .env files in the scripts/ directory:"
    echo "  • config_reports.env          - Team-specific reporting"
    echo "  • config_reports_tribe.env    - Tribe-wide reporting"
    echo ""
    echo "OUTPUT:"
    echo "  Reports are generated in ../output/ with timestamps:"
    echo "  • team_weekly_reports_YYYYMMDD/    (team mode)"
    echo "  • tribe_weekly_reports_YYYYMMDD/   (tribe mode)"
    echo ""
    echo "FEATURES:"
    echo "  ✓ Intelligent configuration detection"
    echo "  ✓ Interactive selection for multiple configs"
    echo "  ✓ Non-interactive mode with --config parameter"
    echo "  ✓ Adaptive JIRA filtering and SonarQube project selection"
    echo "  ✓ Consolidated reporting with analysis recommendations"
    echo ""
    exit 0
fi

set -e  # Exit on any error

# Function to display available configurations
show_config_menu() {
    echo ""
    echo "🔧 Multiple configuration files found!"
    echo "Please select which configuration to use:"
    echo ""
    
    local configs=("$@")
    for i in "${!configs[@]}"; do
        local config="${configs[$i]}"
        local mode_type=""
        local description=""
        
        if [[ "$config" == *"tribe"* ]]; then
            mode_type="TRIBE-WIDE"
            description="Complete tribe reporting (no team filtering)"
        else
            mode_type="TEAM-SPECIFIC"
            description="Individual team reporting with filters"
        fi
        
        printf "   %d) %-25s [%s] - %s\n" $((i+1)) "$config" "$mode_type" "$description"
    done
    
    echo ""
    echo "   0) Exit"
    echo ""
}

# Function to get user selection
get_user_selection() {
    local configs=("$@")
    local max_options=${#configs[@]}
    
    echo -n "👉 Select option (0-$max_options): "
    read -r selection
    
    if [[ "$selection" == "0" ]]; then
        echo "❌ Exiting script."
        exit 0
    elif [[ "$selection" == "1" ]] && [ "$max_options" -ge 1 ]; then
        return 0
    elif [[ "$selection" == "2" ]] && [ "$max_options" -ge 2 ]; then
        return 1
    else
        echo "⚠️  Invalid selection. Please enter a number between 0 and $max_options."
        exit 1
    fi
}

# Auto-detect configuration files and handle selection
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Find all config files
CONFIG_FILES=($(ls config_reports*.env 2>/dev/null || true))

if [ ${#CONFIG_FILES[@]} -eq 0 ]; then
    echo "❌ No configuration files found in $SCRIPT_DIR"
    echo ""
    echo "Please create one of the following:"
    echo "   • config_reports_tribe.env   - For tribe-wide reports"
    echo "   • config_reports.env         - For team-specific reports"
    echo ""
    exit 1
elif [ ${#CONFIG_FILES[@]} -eq 1 ]; then
    # Single config file found - use it directly
    SELECTED_CONFIG="${CONFIG_FILES[0]}"
    echo "🔧 Found configuration: $SELECTED_CONFIG"
else
    # Multiple config files found - check for --config parameter
    if [[ "$1" == "--config" && "$2" == "team" ]]; then
        SELECTED_CONFIG="config_reports.env"
        if [[ ! -f "$SELECTED_CONFIG" ]]; then
            echo "❌ Team configuration file 'config_reports.env' not found"
            exit 1
        fi
        echo "🔧 Using team configuration: $SELECTED_CONFIG"
    elif [[ "$1" == "--config" && "$2" == "tribe" ]]; then
        SELECTED_CONFIG="config_reports_tribe.env"
        if [[ ! -f "$SELECTED_CONFIG" ]]; then
            echo "❌ Tribe configuration file 'config_reports_tribe.env' not found"
            exit 1
        fi
        echo "🔧 Using tribe configuration: $SELECTED_CONFIG"
    else
        # Interactive mode
        show_config_menu "${CONFIG_FILES[@]}"
        get_user_selection "${CONFIG_FILES[@]}"
        SELECTION_INDEX=$?
        SELECTED_CONFIG="${CONFIG_FILES[$SELECTION_INDEX]}"
        echo ""
        echo "✅ Selected configuration: $SELECTED_CONFIG"
    fi
fi

# Load the selected configuration
echo "🔧 Loading configuration from $SELECTED_CONFIG..."
source "$SELECTED_CONFIG"

# Determine mode based on config file name
TRIBE_MODE=false
if [[ "$SELECTED_CONFIG" == *"tribe"* ]]; then
    TRIBE_MODE=true
fi

# --- Ensure Python venv is active ---
# Navigate back to project root for Python operations
cd ..

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
SONARQUBE_ORGANIZATION="${SONARQUBE_ORGANIZATION:-syngenta-digital}"
SONARQUBE_PROJECT_KEYS="${SONARQUBE_PROJECT_KEYS:-}"
INCLUDE_OPTIONAL_REPORTS="${INCLUDE_OPTIONAL_REPORTS:-true}"
CLEAR_CACHE="${CLEAR_CACHE:-true}"
USE_DATE_SUFFIX="${USE_DATE_SUFFIX:-true}"
OUTPUT_BASE_DIR="${OUTPUT_BASE_DIR:-output}"

# Mode-specific configuration
if [ "$TRIBE_MODE" = true ]; then
    # Tribe-wide configuration
    LINEARB_TEAM_ID="${TRIBE_LINEARB_TEAM_ID:-19767}"
    REPORT_SCOPE="tribe"
    echo "📊 Mode: TRIBE-WIDE reporting"
    echo "   👥 LinearB Team: $LINEARB_TEAM_ID (tribe parent team)"
    echo "   📋 JIRA: Complete CWS project (no team filter)"
else
    # Team-specific configuration
    TEAM="${TEAM:-Catalog}"
    LINEARB_TEAM_ID="${LINEARB_TEAM_IDS:-41576}"
    REPORT_SCOPE="team"
    echo "📊 Mode: TEAM-SPECIFIC reporting"
    echo "   👥 Team: $TEAM"
    echo "   📋 LinearB Team: $LINEARB_TEAM_ID"
fi

# Validate team-specific configuration
if [ "$TRIBE_MODE" = false ] && [ -z "$SONARQUBE_PROJECT_KEYS" ]; then
    echo "❌ Error: SONARQUBE_PROJECT_KEYS is required for team-specific reporting"
    echo ""
    echo "Please add SONARQUBE_PROJECT_KEYS to your configuration file:"
    echo "   SONARQUBE_PROJECT_KEYS=\"project1,project2,project3\""
    echo ""
    echo "You can find your project keys by running:"
    echo "   python src/main.py syngenta sonarqube sonarqube --operation list-projects"
    echo ""
    exit 1
fi

# Set up output directory
if [ "$USE_DATE_SUFFIX" = "true" ]; then
    DATE_SUFFIX=$(date +"%Y%m%d")
    OUTPUT_DIR="${OUTPUT_BASE_DIR}/${REPORT_SCOPE}_weekly_reports_${DATE_SUFFIX}"
else
    OUTPUT_DIR="${OUTPUT_BASE_DIR}/${REPORT_SCOPE}_weekly_reports"
fi

# Clean and create output directory
if [ -d "$OUTPUT_DIR" ]; then
    echo "🧹 Cleaning existing output directory: $OUTPUT_DIR"
    rm -rf "$OUTPUT_DIR"
fi
mkdir -p "$OUTPUT_DIR"

# Create subdirectories for better organization
mkdir -p "$OUTPUT_DIR/jira"
mkdir -p "$OUTPUT_DIR/sonarqube"
mkdir -p "$OUTPUT_DIR/linearb"
mkdir -p "$OUTPUT_DIR/consolidated"

# Header
echo ""
echo "============================================================"
if [ "$TRIBE_MODE" = true ]; then
    echo "           📊 TRIBE-WIDE WEEKLY METRICS REPORT TOOL"
else
    echo "           📊 TEAM WEEKLY METRICS REPORT TOOL"
fi
echo "============================================================"
echo "📅 Run date:        $(date)"
echo "🏷️  Project:         $PROJECT_KEY"
echo "⚙️  Config file:     scripts/$SELECTED_CONFIG"
if [ "$TRIBE_MODE" = true ]; then
    echo "👥 Scope:           Entire tribe (no team filtering)"
else
    echo "👥 Team:            $TEAM"
fi
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

# Step counter for progress tracking
if [ "$TRIBE_MODE" = true ]; then
    TOTAL_STEPS=8  # Tribe mode: 5 JIRA reports + 3 cycle time reports = 8 total
else
    TOTAL_STEPS=10  # Team mode: 5 JIRA reports + 3 cycle time reports + 2 others = 10 total
fi
STEP=1

# JIRA Reports
if [ "$TRIBE_MODE" = true ]; then
    # Tribe-wide reports (no team filter)
    echo "🌟 [$STEP/$TOTAL_STEPS] TRIBE – Bug & Support (Combined 2 Weeks)"
    echo "   ⏳ Period: $JIRA_TWO_WEEKS"
    echo "   👥 Scope: Complete tribe (CWS project)"
    ((STEP++))
    python src/main.py syngenta jira issue-adherence \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_TWO_WEEKS" \
      --issue-types 'Bug,Support' \
      --status-categories 'Done' \
      --include-no-due-date \
      --output-file "$OUTPUT_DIR/jira/tribe-bugs-support-2weeks.json"

    echo "🌟 [$STEP/$TOTAL_STEPS] TRIBE – Bug & Support (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Scope: Complete tribe (CWS project)"
    ((STEP++))
    python src/main.py syngenta jira issue-adherence \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_LAST_WEEK" \
      --issue-types 'Bug,Support' \
      --status-categories 'Done' \
      --include-no-due-date \
      --output-file "$OUTPUT_DIR/jira/tribe-bugs-support-lastweek.json"

    echo "🌟 [$STEP/$TOTAL_STEPS] TRIBE – Bug & Support (Week Before)"
    echo "   ⏳ Period: $JIRA_WEEK_BEFORE"
    echo "   👥 Scope: Complete tribe (CWS project)"
    ((STEP++))
    python src/main.py syngenta jira issue-adherence \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_WEEK_BEFORE" \
      --issue-types 'Bug,Support' \
      --status-categories 'Done' \
      --include-no-due-date \
      --output-file "$OUTPUT_DIR/jira/tribe-bugs-support-weekbefore.json"

    echo "🌟 [$STEP/$TOTAL_STEPS] TRIBE – Task Completion (Combined 2 Weeks)"
    echo "   ⏳ Period: $JIRA_TWO_WEEKS"
    echo "   👥 Scope: Complete tribe (CWS project)"
    ((STEP++))
    python src/main.py syngenta jira issue-adherence \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_TWO_WEEKS" \
      --issue-types 'Story,Task,Technical Debt,Improvement,Defect' \
      --status-categories 'Done' \
      --include-no-due-date \
      --output-file "$OUTPUT_DIR/jira/tribe-tasks-2weeks.json"

    echo "🐛 [$STEP/$TOTAL_STEPS] TRIBE – Open Issues (All Types)"
    echo "   � Current open issues for entire tribe"
    ((STEP++))
    python src/main.py syngenta jira open-issues \
      --project-key "$PROJECT_KEY" \
      --issue-types 'Bug,Support,Story,Task,Technical Debt,Improvement,Defect' \
      --output-file "$OUTPUT_DIR/jira/tribe-open-issues.json"

    echo "⏱️  [$STEP/$TOTAL_STEPS] TRIBE – Cycle Time Analysis - Bugs (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Scope: Complete tribe (CWS project)"
    echo "   🐛 Issue Types: Bug"
    ((STEP++))
    python src/main.py syngenta jira cycle-time \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_LAST_WEEK" \
      --issue-types "Bug" \
      --output-file "$OUTPUT_DIR/jira/tribe-cycle-time-bugs-lastweek.json"

    echo "⏱️  [$STEP/$TOTAL_STEPS] TRIBE – Cycle Time Analysis - Support (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Scope: Complete tribe (CWS project)"
    echo "   🎧 Issue Types: Support"
    ((STEP++))
    python src/main.py syngenta jira cycle-time \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_LAST_WEEK" \
      --issue-types "Support" \
      --output-file "$OUTPUT_DIR/jira/tribe-cycle-time-support-lastweek.json"

    echo "⏱️  [$STEP/$TOTAL_STEPS] TRIBE – Cycle Time Analysis - Development (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Scope: Complete tribe (CWS project)"
    echo "   🚀 Issue Types: Story,Task,Technical Debt,Improvement,Defect"
    ((STEP++))
    python src/main.py syngenta jira cycle-time \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_LAST_WEEK" \
      --issue-types "Story,Task,Technical Debt,Improvement,Defect" \
      --output-file "$OUTPUT_DIR/jira/tribe-cycle-time-development-lastweek.json"
else
    # Team-specific reports (with team filter)
    echo "📊 [$STEP/$TOTAL_STEPS] TEAM – Bug & Support (Combined 2 Weeks)"
    echo "   ⏳ Period: $JIRA_TWO_WEEKS"
    echo "   👥 Team: $TEAM"
    ((STEP++))
    python src/main.py syngenta jira issue-adherence \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_TWO_WEEKS" \
      --issue-types 'Bug,Support' \
      --status-categories 'Done' \
      --include-no-due-date \
      --output-file "$OUTPUT_DIR/jira/team-bugs-support-2weeks.json" \
      --team "$TEAM"

    echo "📊 [$STEP/$TOTAL_STEPS] TEAM – Bug & Support (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Team: $TEAM"
    ((STEP++))
    python src/main.py syngenta jira issue-adherence \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_LAST_WEEK" \
      --issue-types 'Bug,Support' \
      --status-categories 'Done' \
      --include-no-due-date \
      --output-file "$OUTPUT_DIR/jira/team-bugs-support-lastweek.json" \
      --team "$TEAM"

    echo "📊 [$STEP/$TOTAL_STEPS] TEAM – Bug & Support (Week Before)"
    echo "   ⏳ Period: $JIRA_WEEK_BEFORE"
    echo "   👥 Team: $TEAM"
    ((STEP++))
    python src/main.py syngenta jira issue-adherence \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_WEEK_BEFORE" \
      --issue-types 'Bug,Support' \
      --status-categories 'Done' \
      --include-no-due-date \
      --output-file "$OUTPUT_DIR/jira/team-bugs-support-weekbefore.json" \
      --team "$TEAM"

    echo "📊 [$STEP/$TOTAL_STEPS] TEAM – Task Completion (2 Weeks)"
    echo "   ⏳ Period: $JIRA_TWO_WEEKS"
    echo "   👥 Team: $TEAM"
    ((STEP++))
    python src/main.py syngenta jira issue-adherence \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_TWO_WEEKS" \
      --issue-types 'Story,Task,Technical Debt,Improvement,Defect' \
      --status-categories 'Done' \
      --include-no-due-date \
      --output-file "$OUTPUT_DIR/jira/team-tasks-2weeks.json" \
      --team "$TEAM"

    echo "🐛 [$STEP/$TOTAL_STEPS] TEAM – Open Issues (Bugs & Support)"
    echo "   📋 Current open issues for team: $TEAM"
    ((STEP++))
    python src/main.py syngenta jira open-issues \
      --project-key "$PROJECT_KEY" \
      --issue-types 'Bug,Support' \
      --team "$TEAM" \
      --output-file "$OUTPUT_DIR/jira/team-open-bugs-support.json"

    # JIRA Cycle Time Reports
    echo "⏱️  [$STEP/$TOTAL_STEPS] TEAM – Cycle Time Analysis - Bugs (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Team: $TEAM"
    echo "   🐛 Issue Types: Bug"
    ((STEP++))
    python src/main.py syngenta jira cycle-time \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_LAST_WEEK" \
      --team "$TEAM" \
      --issue-types "Bug" \
      --output-file "$OUTPUT_DIR/jira/team-cycle-time-bugs-lastweek.json"

    echo "⏱️  [$STEP/$TOTAL_STEPS] TEAM – Cycle Time Analysis - Support (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Team: $TEAM"
    echo "   🎧 Issue Types: Support"
    ((STEP++))
    python src/main.py syngenta jira cycle-time \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_LAST_WEEK" \
      --team "$TEAM" \
      --issue-types "Support" \
      --output-file "$OUTPUT_DIR/jira/team-cycle-time-support-lastweek.json"

    echo "⏱️  [$STEP/$TOTAL_STEPS] TEAM – Cycle Time Analysis - Development (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Team: $TEAM"
    echo "   🚀 Issue Types: Story,Task,Technical Debt,Improvement,Defect"
    ((STEP++))
    python src/main.py syngenta jira cycle-time \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_LAST_WEEK" \
      --team "$TEAM" \
      --issue-types "Story,Task,Technical Debt,Improvement,Defect" \
      --output-file "$OUTPUT_DIR/jira/team-cycle-time-development-lastweek.json"
fi

# SonarQube Report
echo ""
if [ "$TRIBE_MODE" = true ]; then
    echo "🔍 [$STEP/$TOTAL_STEPS] SonarQube – Code Quality Metrics (ALL PROJECTS)"
    echo "   📊 Including all 27 Syngenta Digital projects"
else
    echo "� [$STEP/$TOTAL_STEPS] SonarQube – Code Quality Metrics"
    echo "   � Team-specific projects"
fi
((STEP++))

CLEAR_CACHE_FLAG=""
[ "$CLEAR_CACHE" = "true" ] && CLEAR_CACHE_FLAG="--clear-cache"

if [ "$TRIBE_MODE" = true ]; then
    # Use all projects from the projects_list.json file (tribe mode)
    python src/main.py syngenta sonarqube sonarqube \
      --operation list-projects \
      --organization "$SONARQUBE_ORGANIZATION" \
      --include-measures \
      --output-file "$OUTPUT_DIR/sonarqube/tribe-quality-metrics.json" \
      $CLEAR_CACHE_FLAG
else
    # Use specific project keys (team mode)
    python src/main.py syngenta sonarqube sonarqube \
      --operation list-projects \
      --organization "$SONARQUBE_ORGANIZATION" \
      --include-measures \
      --output-file "$OUTPUT_DIR/sonarqube/team-quality-metrics.json" \
      $CLEAR_CACHE_FLAG \
      --project-keys "$SONARQUBE_PROJECT_KEYS"
fi

# LinearB Report
if [ "$TRIBE_MODE" = true ]; then
    echo "🚀 [$STEP/$TOTAL_STEPS] LinearB – Engineering Metrics (TRIBE PARENT TEAM)"
    LINEARB_TIME_RANGE="$WEEK_BEFORE_MONDAY,$LAST_SUNDAY"
    echo "   ⏳ Period: $LINEARB_TIME_RANGE"
    echo "   👥 Tribe Team ID: $LINEARB_TEAM_ID (parent team)"
else
    echo "🚀 [$STEP/$TOTAL_STEPS] LinearB – Engineering Metrics"
    LINEARB_TIME_RANGE="$WEEK_BEFORE_MONDAY,$LAST_SUNDAY"
    echo "   ⏳ Period: $LINEARB_TIME_RANGE"
    echo "   👥 Team: $TEAM (ID: $LINEARB_TEAM_ID)"
fi
((STEP++))

python src/main.py linearb export-report \
  --team-ids "$LINEARB_TEAM_ID" \
  --time-range "$LINEARB_TIME_RANGE" \
  --format csv \
  --filter-type team \
  --granularity 1w \
  --beautified \
  --return-no-data \
  --aggregation avg \
  --output-folder "$OUTPUT_DIR/linearb" \
  || echo "⚠️  Warning: LinearB export report failed - check configuration"

# Generate Consolidated Summary Report
echo "📋 [$STEP/$TOTAL_STEPS] Generating Consolidated Summary Report"
((STEP++))

SUMMARY_FILE="$OUTPUT_DIR/consolidated/weekly-summary.md"

cat > "$SUMMARY_FILE" << EOF
# Weekly Metrics Summary

**Generated on:** $(date)  
**Period:** $JIRA_TWO_WEEKS  
**Project:** $PROJECT_KEY  
**Mode:** $(if [ "$TRIBE_MODE" = true ]; then echo "Tribe-wide"; else echo "Team-specific ($TEAM)"; fi)
**Config:** scripts/$SELECTED_CONFIG

## 📊 Reports Generated

### JIRA Analysis
EOF

if [ "$TRIBE_MODE" = true ]; then
    cat >> "$SUMMARY_FILE" << EOF
- Bug & Support Issues (2 weeks): \`jira/tribe-bugs-support-2weeks.json\`
- Bug & Support Issues (last week): \`jira/tribe-bugs-support-lastweek.json\`
- Bug & Support Issues (week before): \`jira/tribe-bugs-support-weekbefore.json\`
- Task Completion (2 weeks): \`jira/tribe-tasks-2weeks.json\`  
- Open Issues (all types): \`jira/tribe-open-issues.json\`
- Cycle Time Analysis - Bugs: \`jira/tribe-cycle-time-bugs-lastweek.json\`
- Cycle Time Analysis - Support: \`jira/tribe-cycle-time-support-lastweek.json\`
- Cycle Time Analysis - Development: \`jira/tribe-cycle-time-development-lastweek.json\`

### Code Quality Analysis
- SonarQube Metrics (27 projects): \`sonarqube/tribe-quality-metrics.json\`

### Engineering Metrics
- LinearB Metrics (tribe parent team): \`linearb/linearb_export*.csv\`
EOF
else
    cat >> "$SUMMARY_FILE" << EOF
- Bug & Support Issues (2 weeks): \`jira/team-bugs-support-2weeks.json\`
- Bug & Support Issues (last week): \`jira/team-bugs-support-lastweek.json\`
- Bug & Support Issues (week before): \`jira/team-bugs-support-weekbefore.json\`
- Task Completion (2 weeks): \`jira/team-tasks-2weeks.json\`
- Open Issues: \`jira/team-open-bugs-support.json\`
- Cycle Time Analysis - Bugs: \`jira/team-cycle-time-bugs-lastweek.json\`
- Cycle Time Analysis - Support: \`jira/team-cycle-time-support-lastweek.json\`
- Cycle Time Analysis - Development: \`jira/team-cycle-time-development-lastweek.json\`

### Code Quality Analysis
- SonarQube Metrics (team projects): \`sonarqube/team-quality-metrics.json\`

### Engineering Metrics
- LinearB Metrics ($TEAM team): \`linearb/linearb_export*.csv\`
EOF
fi

cat >> "$SUMMARY_FILE" << EOF

## 📈 Week-over-week Comparison Available
Compare performance between:
EOF

if [ "$TRIBE_MODE" = true ]; then
    cat >> "$SUMMARY_FILE" << EOF
- **Last week:** tribe-bugs-support-lastweek.json
- **Week before:** tribe-bugs-support-weekbefore.json
EOF
else
    cat >> "$SUMMARY_FILE" << EOF
- **Last week:** team-bugs-support-lastweek.json
- **Week before:** team-bugs-support-weekbefore.json
EOF
fi

cat >> "$SUMMARY_FILE" << EOF

## 🔍 Analysis Recommendations

1. **Performance Trends:** Monitor cycle times and adherence rates
2. **Quality Trends:** Track SonarQube metrics for quality gates and technical debt
3. **Bug Resolution:** Analyze bug resolution efficiency
4. **Delivery Patterns:** Identify consistent delivery patterns and potential blockers
5. **Capacity Planning:** Use historical data for future sprint planning

---
*This report provides comprehensive metrics for informed decision-making and continuous improvement.*
EOF

echo "   ✅ Summary written to: $SUMMARY_FILE"

# Final Summary
echo ""
if [ "$TRIBE_MODE" = true ]; then
    echo "✅ TRIBE WEEKLY REPORT GENERATION COMPLETED!"
    echo "════════════════════════════════════════════════════════════"
    echo "📁 Output directory: $OUTPUT_DIR"
    echo ""
    echo "📊 Generated reports:"
    printf "   • %-30s %s\n" "JIRA tribe reports:" "8 files (5 adherence + 3 cycle time)"
    printf "   • %-30s %s\n" "SonarQube analysis:" "1 file (27 projects)"
    printf "   • %-30s %s\n" "LinearB metrics:" "CSV files (tribe parent team)"
    printf "   • %-30s %s\n" "Consolidated summary:" "1 markdown file"
else
    echo "✅ TEAM WEEKLY REPORT GENERATION COMPLETED!"
    echo "════════════════════════════════════════════════════════════"
    echo "📁 Output directory: $OUTPUT_DIR"
    echo ""
    echo "� Generated reports:"
    printf "   • %-30s %s\n" "JIRA team reports:" "8 files (5 adherence + 3 cycle time)"
    printf "   • %-30s %s\n" "SonarQube analysis:" "1 file (team projects)"
    printf "   • %-30s %s\n" "LinearB metrics:" "CSV files ($TEAM team)"
    printf "   • %-30s %s\n" "Consolidated summary:" "1 markdown file"
fi
echo ""
echo "📂 Directory structure:"
echo "   $OUTPUT_DIR/"
echo "   ├── jira/                   (JIRA reports)"
echo "   ├── sonarqube/              (Code quality metrics)"
echo "   ├── linearb/                (Engineering metrics)"
echo "   └── consolidated/           (Summary and analysis)"
echo ""
echo "📋 Total files generated:"
TOTAL_FILES=$(find "$OUTPUT_DIR" -type f | wc -l | tr -d ' ')
echo "   $TOTAL_FILES files across all categories"
echo ""
if [ "$TRIBE_MODE" = true ]; then
    echo "🎯 Key insights available:"
    echo "   → Complete tribe performance metrics"
    echo "   → Week-over-week trend analysis"
    echo "   → Quality metrics across all projects"
    echo "   → Engineering velocity patterns"
    echo "   → Comprehensive tribe health assessment"
else
    echo "🎯 Key insights available:"
    echo "   → Team performance metrics"
    echo "   → Week-over-week trend analysis"
    echo "   → Quality metrics for team projects"
    echo "   → Engineering velocity patterns"
    echo "   → Team health assessment"
fi
echo ""
echo "📖 Start analysis with: $OUTPUT_DIR/consolidated/weekly-summary.md"
