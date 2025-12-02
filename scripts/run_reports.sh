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

# Find all config files and organize them in a specific order
TEMP_FILES=($(ls config_reports*.env 2>/dev/null || true))
CONFIG_FILES=()

# First add team config (config_reports.env) if it exists
for file in "${TEMP_FILES[@]}"; do
    if [[ "$file" == "config_reports.env" ]]; then
        CONFIG_FILES+=("$file")
        break
    fi
done

# Then add tribe config (config_reports_tribe.env) if it exists
for file in "${TEMP_FILES[@]}"; do
    if [[ "$file" == "config_reports_tribe.env" ]]; then
        CONFIG_FILES+=("$file")
        break
    fi
done

# Add any other config files that might exist
for file in "${TEMP_FILES[@]}"; do
    if [[ "$file" != "config_reports.env" && "$file" != "config_reports_tribe.env" ]]; then
        CONFIG_FILES+=("$file")
    fi
done

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
        get_user_selection "${CONFIG_FILES[@]}" || true  # Prevent set -e from exiting
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

# Validate team-specific configuration removed - SonarQube temporarily disabled

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
echo ""



# --- Dynamic date range calculation ---
# Nova lógica: semanas segunda-feira → domingo
# Regras:
# 1. Se TODAY for domingo: usar a semana que está terminando (incluindo o próprio domingo)
# 2. Se TODAY for segunda-sábado: usar a semana anterior completa
# 3. Período customizado: usar start/end se informados
# 4. Fuso horário: America/Sao_Paulo

# Função para validar data no formato YYYY-MM-DD
validate_date() {
    local date_str="$1"
    if [[ ! "$date_str" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        return 1
    fi
    # Testar se a data é válida usando date
    date -jf "%Y-%m-%d" "$date_str" >/dev/null 2>&1
    return $?
}

# Função para calcular o dia da semana (1=segunda, 7=domingo)
get_weekday() {
    local date_str="$1"
    date -jf "%Y-%m-%d" "$date_str" +%u
}

# Verificar se período customizado foi especificado
if [[ -n "$CUSTOM_START_DATE" && -n "$CUSTOM_END_DATE" ]]; then
    # Validar datas customizadas
    if ! validate_date "$CUSTOM_START_DATE"; then
        echo "❌ Error: Invalid CUSTOM_START_DATE format: $CUSTOM_START_DATE (expected: YYYY-MM-DD)"
        exit 1
    fi
    if ! validate_date "$CUSTOM_END_DATE"; then
        echo "❌ Error: Invalid CUSTOM_END_DATE format: $CUSTOM_END_DATE (expected: YYYY-MM-DD)"
        exit 1
    fi
    
    # Validar que start <= end
    start_timestamp=$(date -jf "%Y-%m-%d" "$CUSTOM_START_DATE" +%s)
    end_timestamp=$(date -jf "%Y-%m-%d" "$CUSTOM_END_DATE" +%s)
    if [ "$start_timestamp" -gt "$end_timestamp" ]; then
        echo "❌ Error: CUSTOM_START_DATE ($CUSTOM_START_DATE) must be <= CUSTOM_END_DATE ($CUSTOM_END_DATE)"
        exit 1
    fi
    
    CUSTOM_PERIOD_START="$CUSTOM_START_DATE"
    CUSTOM_PERIOD_END="$CUSTOM_END_DATE"
    
    echo "📅 Using custom period: $CUSTOM_PERIOD_START to $CUSTOM_PERIOD_END"
    
    # Para compatibilidade com o resto do script, definir também as variáveis de semana
    LAST_WEEK_START="$CUSTOM_PERIOD_START"
    LAST_WEEK_END="$CUSTOM_PERIOD_END"
    JIRA_LAST_WEEK="$LAST_WEEK_START to $LAST_WEEK_END"
    
    # Calcular semana anterior para análises comparativas
    custom_duration_days=$(( (end_timestamp - start_timestamp) / 86400 + 1 ))
    WEEK_BEFORE_START=$(date -jf "%Y-%m-%d" -v-${custom_duration_days}d "$CUSTOM_PERIOD_START" +%Y-%m-%d)
    WEEK_BEFORE_END=$(date -jf "%Y-%m-%d" -v-1d "$CUSTOM_PERIOD_START" +%Y-%m-%d)
    JIRA_WEEK_BEFORE="$WEEK_BEFORE_START to $WEEK_BEFORE_END"
    
    # Período combinado
    TWO_WEEKS_START="$WEEK_BEFORE_START"
    TWO_WEEKS_END="$CUSTOM_PERIOD_END"
    JIRA_TWO_WEEKS="$TWO_WEEKS_START to $TWO_WEEKS_END"
    
elif [[ -n "$CUSTOM_START_DATE" || -n "$CUSTOM_END_DATE" ]]; then
    echo "❌ Error: When using custom period, both CUSTOM_START_DATE and CUSTOM_END_DATE must be specified"
    exit 1
else
    # Lógica de semana padrão
    
    # Determinar TODAY (com fuso America/Sao_Paulo se não especificado)
    if [[ -n "$CUSTOM_REPORT_DATE" ]]; then
        echo "📅 Using custom date from config: $CUSTOM_REPORT_DATE"
        TODAY="$CUSTOM_REPORT_DATE"
        if ! validate_date "$TODAY"; then
            echo "❌ Error: Invalid CUSTOM_REPORT_DATE format: $TODAY (expected: YYYY-MM-DD)"
            exit 1
        fi
    else
        # Usar data atual no fuso America/Sao_Paulo
        TODAY=$(TZ="America/Sao_Paulo" date +%Y-%m-%d)
    fi
    
    # Calcular dia da semana (1=segunda, 7=domingo)
    weekday=$(get_weekday "$TODAY")
    
    if [ "$weekday" -eq 7 ]; then
        # TODAY é domingo: usar a semana que está terminando (incluindo hoje)
        # Encontrar a segunda-feira desta semana
        LAST_WEEK_START=$(date -jf "%Y-%m-%d" -v-6d "$TODAY" +%Y-%m-%d)
        LAST_WEEK_END="$TODAY"
        echo "📅 TODAY is Sunday ($TODAY) - using current week ending today"
    else
        # TODAY é segunda-sábado: usar a semana anterior completa
        # Encontrar o domingo da semana anterior
        days_since_monday=$((weekday - 1))
        last_sunday=$(date -jf "%Y-%m-%d" -v-${days_since_monday}d -v-1d "$TODAY" +%Y-%m-%d)
        LAST_WEEK_END="$last_sunday"
        LAST_WEEK_START=$(date -jf "%Y-%m-%d" -v-6d "$last_sunday" +%Y-%m-%d)
        echo "📅 TODAY is $(date -jf "%Y-%m-%d" "$TODAY" +%A) ($TODAY) - using previous complete week"
    fi
    
    # Calcular semana anterior à semana selecionada
    WEEK_BEFORE_END=$(date -jf "%Y-%m-%d" -v-1d "$LAST_WEEK_START" +%Y-%m-%d)
    WEEK_BEFORE_START=$(date -jf "%Y-%m-%d" -v-6d "$WEEK_BEFORE_END" +%Y-%m-%d)
    
    # Calcular período combinado de 2 semanas
    TWO_WEEKS_START="$WEEK_BEFORE_START"
    TWO_WEEKS_END="$LAST_WEEK_END"
    
    JIRA_LAST_WEEK="$LAST_WEEK_START to $LAST_WEEK_END"
    JIRA_WEEK_BEFORE="$WEEK_BEFORE_START to $WEEK_BEFORE_END"
    JIRA_TWO_WEEKS="$TWO_WEEKS_START to $TWO_WEEKS_END"
fi

# Print ranges
if [[ -n "$CUSTOM_PERIOD_START" && -n "$CUSTOM_PERIOD_END" ]]; then
    echo "📆 Using custom reporting period:"
    printf "   • %-18s %s\n" "Custom period:" "$JIRA_LAST_WEEK"
    printf "   • %-18s %s\n" "Previous period:" "$JIRA_WEEK_BEFORE"
    printf "   • %-18s %s\n" "Combined periods:" "$JIRA_TWO_WEEKS"
else
    echo "📆 Weekly reporting periods (Monday→Sunday):"
    printf "   • %-18s %s\n" "Selected week:" "$JIRA_LAST_WEEK"
    printf "   • %-18s %s\n" "Previous week:" "$JIRA_WEEK_BEFORE"
    printf "   • %-18s %s\n" "Combined 2 weeks:" "$JIRA_TWO_WEEKS"
fi
echo ""

# Set up output directory using the reference date (not current date)
if [ "$USE_DATE_SUFFIX" = "true" ]; then
    # Use the reference date (configured or current) instead of current date
    if [[ -n "$CUSTOM_PERIOD_END" ]]; then
        # Use custom period end date for suffix
        DATE_SUFFIX=$(date -jf "%Y-%m-%d" "$CUSTOM_PERIOD_END" +"%Y%m%d")
    elif [[ -n "$TODAY" ]]; then
        DATE_SUFFIX=$(date -jf "%Y-%m-%d" "$TODAY" +"%Y%m%d")
    else
        # Fallback to current date
        DATE_SUFFIX=$(TZ="America/Sao_Paulo" date +"%Y%m%d")
    fi
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
# mkdir -p "$OUTPUT_DIR/sonarqube"  # Temporarily disabled
mkdir -p "$OUTPUT_DIR/linearb"
mkdir -p "$OUTPUT_DIR/consolidated"

echo "📁 Output folder: $OUTPUT_DIR"
echo ""

# Step counter for progress tracking
if [ "$TRIBE_MODE" = true ]; then
    TOTAL_STEPS=9  # Tribe mode: 5 JIRA reports + 3 cycle time + 1 net flow + 1 adherence = 10 total (SonarQube removed)
else
    TOTAL_STEPS=13  # Team mode: 5 JIRA reports + 3 cycle time + 2 open issues + 1 wip age + 1 net flow + 1 adherence + 1 historical open = 14 total (SonarQube removed)
fi
STEP=1

# JIRA Reports
if [ "$TRIBE_MODE" = true ]; then
    # Tribe-wide reports (no team filter)
    echo "🌟 [$STEP/$TOTAL_STEPS] TRIBE – Bug & Support (Combined 2 Weeks)"
    echo "   ⏳ Period: $JIRA_TWO_WEEKS"
    echo "   👥 Scope: Complete tribe (CWS project)"
    ((STEP++))
    python3 src/main.py syngenta jira issue-adherence \
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
    python3 src/main.py syngenta jira issue-adherence \
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
    python3 src/main.py syngenta jira issue-adherence \
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
    python3 src/main.py syngenta jira issue-adherence \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_TWO_WEEKS" \
      --issue-types '    cd scripts && ./run_reports.sh --config team' \
      --status-categories 'Done' \
      --include-no-due-date \
      --output-file "$OUTPUT_DIR/jira/tribe-tasks-2weeks.json"

    echo "🐛 [$STEP/$TOTAL_STEPS] TRIBE – Open Issues (All Types)"
    echo "   � Current open issues for entire tribe"
    ((STEP++))
    python3 src/main.py syngenta jira open-issues \
      --project-key "$PROJECT_KEY" \
      --issue-types 'Bug,Support,Story,Task,Technical Debt,Improvement,Defect' \
      --output-file "$OUTPUT_DIR/jira/tribe-open-issues.json"

    echo "⏱️  [$STEP/$TOTAL_STEPS] TRIBE – Cycle Time Analysis - Bugs (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Scope: Complete tribe (CWS project)"
    echo "   🐛 Issue Types: Bug"
    ((STEP++))
    python3 src/main.py syngenta jira cycle-time \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_LAST_WEEK" \
      --issue-types "Bug" \
      --output-file "$OUTPUT_DIR/jira/tribe-cycle-time-bugs-lastweek.json"

    echo "⏱️  [$STEP/$TOTAL_STEPS] TRIBE – Cycle Time Analysis - Support (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Scope: Complete tribe (CWS project)"
    echo "   🎧 Issue Types: Support"
    ((STEP++))
    python3 src/main.py syngenta jira cycle-time \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_LAST_WEEK" \
      --issue-types "Support" \
      --output-file "$OUTPUT_DIR/jira/tribe-cycle-time-support-lastweek.json"

    echo "⏱️  [$STEP/$TOTAL_STEPS] TRIBE – Cycle Time Analysis - Development (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Scope: Complete tribe (CWS project)"
    echo "   🚀 Issue Types: Story,Task,Technical Debt,Improvement,Defect"
    ((STEP++))
    python3 src/main.py syngenta jira cycle-time \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_LAST_WEEK" \
      --issue-types "Story,Task,Technical Debt,Improvement,Defect" \
      --output-file "$OUTPUT_DIR/jira/tribe-cycle-time-development-lastweek.json"

    # Net Flow Analysis (Tribe)
    echo "🌊 [$STEP/$TOTAL_STEPS] TRIBE – Net Flow Health Scorecard (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Scope: Complete tribe (CWS project)"
    echo "   📊 Analysis: Arrival vs Throughput with 4-week trend"
    ((STEP++))
    python3 src/main.py syngenta jira net-flow-calculation \
      --project-key "$PROJECT_KEY" \
      --end-date "$LAST_WEEK_END" \
      --output-format md \
      --extended \
      --verbose

    # Issue Adherence Analysis (Tribe)
    echo "📅 [$STEP/$TOTAL_STEPS] TRIBE – Issue Adherence Analysis (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Scope: Complete tribe (CWS project)"
    echo "   📊 Analysis: Due date compliance with weighted metrics"
    ((STEP++))
    python3 src/main.py syngenta jira issue-adherence \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_LAST_WEEK" \
      --issue-types "Story,Task,Bug,Technical Debt,Improvement,Defect" \
      --output-format md \
      --extended \
      --weighted-adherence \
      --verbose
else
    # Team-specific reports (with team filter)
    echo "📊 [$STEP/$TOTAL_STEPS] TEAM – Bug & Support (Combined 2 Weeks)"
    echo "   ⏳ Period: $JIRA_TWO_WEEKS"
    echo "   👥 Team: $TEAM"
    ((STEP++))
    python3 src/main.py syngenta jira issue-adherence \
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
    python3 src/main.py syngenta jira issue-adherence \
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
    python3 src/main.py syngenta jira issue-adherence \
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
    python3 src/main.py syngenta jira issue-adherence \
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
    python3 src/main.py syngenta jira open-issues \
      --project-key "$PROJECT_KEY" \
      --issue-types 'Bug,Support' \
      --team "$TEAM" \
      --output-file "$OUTPUT_DIR/jira/team-open-bugs-support.json" \
      --verbose

    echo "📊 [$STEP/$TOTAL_STEPS] TEAM – Open Issues Week Before (Bugs & Support)"
    echo "   📋 Issues that were open at end of: $WEEK_BEFORE_END"
    echo "   📝 Logic: Created before $WEEK_BEFORE_END AND (still open OR resolved after $WEEK_BEFORE_END)"
    ((STEP++))
    python3 src/main.py syngenta jira issue-adherence \
      --project-key "$PROJECT_KEY" \
      --time-period "2020-01-01 to $WEEK_BEFORE_END" \
      --issue-types 'Bug,Support' \
      --status-categories 'To Do,In Progress,Done' \
      --include-no-due-date \
      --output-file "$OUTPUT_DIR/jira/team-open-bugs-support-weekbefore.json" \
      --team "$TEAM"

    echo "⏰ [$STEP/$TOTAL_STEPS] TEAM – WIP Age Tracking (Bugs & Support)"
    echo "   📋 Oldest issues in progress for team: $TEAM"
    ((STEP++))
    python3 src/main.py syngenta jira wip-age-tracking \
      --project-key "$PROJECT_KEY" \
      --team "$TEAM" \
      --issue-types 'Bug,Support' \
      --alert-threshold 5 \
      --output-format json \
      --output-file "$OUTPUT_DIR/jira/team-wip-age-bugs-support.json" \
      --verbose

    # JIRA Cycle Time Reports
    echo "⏱️  [$STEP/$TOTAL_STEPS] TEAM – Cycle Time Analysis - Bugs (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Team: $TEAM"
    echo "   🐛 Issue Types: Bug"
    ((STEP++))
    python3 src/main.py syngenta jira cycle-time \
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
    python3 src/main.py syngenta jira cycle-time \
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
    python3 src/main.py syngenta jira cycle-time \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_LAST_WEEK" \
      --team "$TEAM" \
      --issue-types "Story,Task,Technical Debt,Improvement,Defect" \
      --output-file "$OUTPUT_DIR/jira/team-cycle-time-development-lastweek.json"

    # Net Flow Analysis
    echo "🌊 [$STEP/$TOTAL_STEPS] TEAM – Net Flow Health Scorecard (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Team: $TEAM"
    echo "   📊 Analysis: Arrival vs Throughput with 4-week trend"
    ((STEP++))
    python3 src/main.py syngenta jira net-flow-calculation \
      --project-key "$PROJECT_KEY" \
      --end-date "$LAST_WEEK_END" \
      --team "$TEAM" \
      --output-format md \
      --extended \
      --verbose

    # Issue Adherence Analysis
    echo "📅 [$STEP/$TOTAL_STEPS] TEAM – Issue Adherence Analysis (Last Week)"
    echo "   ⏳ Period: $JIRA_LAST_WEEK"
    echo "   👥 Team: $TEAM"
    echo "   📊 Analysis: Due date compliance with weighted metrics"
    ((STEP++))
    python3 src/main.py syngenta jira issue-adherence \
      --project-key "$PROJECT_KEY" \
      --time-period "$JIRA_LAST_WEEK" \
      --team "$TEAM" \
      --issue-types "Story,Task,Bug,Technical Debt,Improvement,Defect" \
      --output-format md \
      --extended \
      --weighted-adherence \
      --verbose
fi

# SonarQube Report - TEMPORARILY DISABLED
# echo ""
# if [ "$TRIBE_MODE" = true ]; then
#     echo "🔍 [$STEP/$TOTAL_STEPS] SonarQube – Code Quality Metrics (ALL PROJECTS)"
#     echo "   📊 Including all 27 Syngenta Digital projects"
# else
#     echo "🔍 [$STEP/$TOTAL_STEPS] SonarQube – Code Quality Metrics"
#     echo "   📊 Team-specific projects"
# fi
# ((STEP++))
# 
# CLEAR_CACHE_FLAG=""
# [ "$CLEAR_CACHE" = "true" ] && CLEAR_CACHE_FLAG="--clear-cache"
# 
# if [ "$TRIBE_MODE" = true ]; then
#     # Use all projects from the projects_list.json file (tribe mode)
#     python3 src/main.py syngenta sonarqube sonarqube \
#       --operation list-projects \
#       --organization "$SONARQUBE_ORGANIZATION" \
#       --include-measures \
#       --output-file "$OUTPUT_DIR/sonarqube/tribe-quality-metrics.json" \
#       $CLEAR_CACHE_FLAG
# else
#     # Use specific project keys (team mode)
#     python3 src/main.py syngenta sonarqube sonarqube \
#       --operation list-projects \
#       --organization "$SONARQUBE_ORGANIZATION" \
#       --include-measures \
#       --output-file "$OUTPUT_DIR/sonarqube/team-quality-metrics.json" \
#       $CLEAR_CACHE_FLAG \
#       --project-keys "$SONARQUBE_PROJECT_KEYS"
# fi

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

python3 src/main.py linearb export-report \
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
- Net Flow Health Scorecard: \`jira/net-flow-*.md\`
- Issue Adherence Analysis: \`jira/issue-adherence-*.md\`

### Engineering Metrics
- LinearB Metrics (tribe parent team): \`linearb/linearb_export*.csv\`

<!-- SonarQube temporarily disabled
### Code Quality Analysis
- SonarQube Metrics (27 projects): \`sonarqube/tribe-quality-metrics.json\`
-->
EOF
else
    cat >> "$SUMMARY_FILE" << EOF
- Bug & Support Issues (2 weeks): \`jira/team-bugs-support-2weeks.json\`
- Bug & Support Issues (last week): \`jira/team-bugs-support-lastweek.json\`
- Bug & Support Issues (week before): \`jira/team-bugs-support-weekbefore.json\`
- Task Completion (2 weeks): \`jira/team-tasks-2weeks.json\`
- Open Issues: \`jira/team-open-bugs-support.json\`
- Open Issues (week before snapshot): \`jira/team-open-bugs-support-weekbefore.json\`
- WIP Age Tracking: \`jira/team-wip-age-bugs-support.json\`
- Cycle Time Analysis - Bugs: \`jira/team-cycle-time-bugs-lastweek.json\`
- Cycle Time Analysis - Support: \`jira/team-cycle-time-support-lastweek.json\`
- Cycle Time Analysis - Development: \`jira/team-cycle-time-development-lastweek.json\`
- Net Flow Health Scorecard: \`jira/net-flow-*.md\`
- Issue Adherence Analysis: \`jira/issue-adherence-*.md\`

### Engineering Metrics
- LinearB Metrics ($TEAM team): \`linearb/linearb_export*.csv\`

<!-- SonarQube temporarily disabled
### Code Quality Analysis
- SonarQube Metrics (team projects): \`sonarqube/team-quality-metrics.json\`
-->
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
    printf "   • %-30s %s\n" "JIRA tribe reports:" "10 files (5 adherence + 3 cycle time + 2 analytics)"
    printf "   • %-30s %s\n" "LinearB metrics:" "CSV files (tribe parent team)"
    printf "   • %-30s %s\n" "Consolidated summary:" "1 markdown file"
    # printf "   • %-30s %s\n" "SonarQube analysis:" "DISABLED (temporarily)"
else
    echo "✅ TEAM WEEKLY REPORT GENERATION COMPLETED!"
    echo "════════════════════════════════════════════════════════════"
    echo "📁 Output directory: $OUTPUT_DIR"
    echo ""
    echo "📊 Generated reports:"
    printf "   • %-30s %s\n" "JIRA team reports:" "12 files (5 adherence + 3 cycle time + 2 open issues + 1 wip age + 2 analytics)"
    printf "   • %-30s %s\n" "LinearB metrics:" "CSV files ($TEAM team)"
    printf "   • %-30s %s\n" "Consolidated summary:" "1 markdown file"
    # printf "   • %-30s %s\n" "SonarQube analysis:" "DISABLED (temporarily)"
fi
echo ""
echo "📂 Directory structure:"
echo "   $OUTPUT_DIR/"
echo "   ├── jira/                   (JIRA reports)"
echo "   ├── linearb/                (Engineering metrics)"
echo "   └── consolidated/           (Summary and analysis)"
# echo "   ├── sonarqube/              (Code quality metrics - DISABLED)"
echo ""
echo "📋 Total files generated:"
TOTAL_FILES=$(find "$OUTPUT_DIR" -type f | wc -l | tr -d ' ')
echo "   $TOTAL_FILES files across all categories"
echo ""
if [ "$TRIBE_MODE" = true ]; then
    echo "🎯 Key insights available:"
    echo "   → Complete tribe performance metrics"
    echo "   → Week-over-week trend analysis"
    # echo "   → Quality metrics across all projects (SonarQube disabled)"
    echo "   → Engineering velocity patterns"
    echo "   → Comprehensive tribe health assessment"
else
    echo "🎯 Key insights available:"
    echo "   → Team performance metrics"
    echo "   → Week-over-week trend analysis"
    # echo "   → Quality metrics for team projects (SonarQube disabled)"
    echo "   → Engineering velocity patterns"
    echo "   → Team health assessment"
fi
echo ""
echo "📖 Start analysis with: $OUTPUT_DIR/consolidated/weekly-summary.md"
