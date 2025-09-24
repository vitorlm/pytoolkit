# JIRA Domain - Command Reference & Features

A comprehensive suite of JIRA integration commands for issue management, analysis, and reporting within the Syngenta Digital ecosystem.

## Available Commands

All commands are accessed via: `python src/main.py syngenta jira [command] [options]`

### 📊 Analysis & Reporting Commands

#### 1. **epic-monitor** - Epic Monitoring & Notifications
- **Purpose**: Monitor JIRA epics for problems and send Slack notifications
- **Usage**: `python src/main.py syngenta jira epic-monitor [--slack-webhook URL]`
- **Features**:
  - Automated epic monitoring with cycle-based filtering
  - Detects 6 types of epic problems (missing dates, overdue, assignments)
  - Slack notifications via Block Kit
  - GitHub Actions integration (Mon/Wed/Fri at 9:00 AM BRT)

#### 2. **cycle-time** - Cycle Time Analysis
- **Purpose**: Analyze cycle time from Started to Done status
- **Usage**: `python src/main.py syngenta jira cycle-time --project-key CWS --end-date 2025-09-21 --window-days 7`
- **Features**:
  - Calculate time from Started (07) to Done (10) status
  - Filter by issue types, team, priority
  - Statistical metrics (median, P95, average)
  - Time distribution analysis
  - Priority breakdown

#### 3. **issue-adherence** - Due Date Adherence Analysis
- **Purpose**: Analyze issue completion against due dates
- **Usage**: `python src/main.py syngenta jira issue-adherence --project-key CWS --end-date 2025-09-21 --window-days 7`
- **Features**:
  - On-time vs late completion tracking
  - Adherence rate calculations
  - Team-based filtering
  - Export to JSON format

#### 4. **calculate-resolution-time** - SLA & Resolution Time Analysis
- **Purpose**: Comprehensive resolution time analysis with SLA recommendations
- **Usage**: `python src/main.py syngenta jira calculate-resolution-time --project-key CWS --time-period last-month`
- **Features**:
  - Bug/Support: Creation to resolution time
  - Other types: Started to resolution time
  - P95, median, standard deviation metrics
  - SLA recommendations with risk levels
  - Outlier detection and exclusion
  - Chart generation and visualization
  - Priority-based breakdown

#### 5. **issues-creation-analysis** - Issue Creation Patterns
- **Purpose**: Analyze JIRA issue creation patterns over time
- **Usage**: `python src/main.py syngenta jira issues-creation-analysis --aggregation weekly`
- **Features**:
  - Daily, weekly, monthly aggregation
  - Issue type filtering
  - Project and label filtering
  - Export to CSV/JSON
  - Summary statistics

#### 6. **open-issues** - Current Open Issues Snapshot
- **Purpose**: Fetch all currently open issues without date filtering
- **Usage**: `python src/main.py syngenta jira open-issues --project-key CWS --issue-types Bug`
- **Features**:
  - Real-time snapshot of active work
  - Status category filtering
  - Team/squad filtering
  - Breakdown by status, type, priority

#### 7. **issue-velocity** - Monthly Issue Creation vs Resolution Analysis
- **Purpose**: Analyze issue velocity through creation vs resolution tracking
- **Usage**: `python src/main.py syngenta jira issue-velocity --project-key CWS --time-period last-6-months`
- **Features**:
  - Combined created + resolved monthly analysis
  - Net velocity and efficiency calculations
  - Team/squad filtering via Squad[Dropdown]
  - Issue type filtering (default: Bug)
  - Label filtering support
  - Monthly/quarterly aggregation
  - Backlog impact tracking
  - Velocity trend analysis
  - CSV/JSON export options

### ⚙️ Management & Utility Commands

#### 8. **components** - Component Management
- **Purpose**: Comprehensive JIRA component management
- **Usage**: `python src/main.py syngenta jira components --operation list --project-key CWS`
- **Operations**:
  - `list`: List all project components
  - `create`: Create single component
  - `delete`: Delete component
  - `create-batch`: Bulk create from JSON
  - `delete-batch`: Bulk delete by IDs
  - `update-issue`: Update issue components
  - `update-issues-batch`: Bulk update from JSON

#### 9. **fill-missing-dates** - Date Management
- **Purpose**: Fill missing dates in JIRA issues
- **Usage**: `python src/main.py syngenta jira fill-missing-dates --project CWS --team_name Catalog`
- **Features**:
  - Automated date filling for completed epics
  - Date range filtering
  - Team-specific processing

#### 10. **list-custom-fields** - Field Discovery
- **Purpose**: List available custom fields for debugging
- **Usage**: `python src/main.py syngenta jira list-custom-fields --output-file fields.json`
- **Features**:
  - Complete custom field enumeration
  - JSON export for reference
  - Useful for troubleshooting commands

#### 11. **cycle-info** - Cycle Information Utility
- **Purpose**: Display and test cycle calculations
- **Usage**: `python src/main.py syngenta jira cycle-info --show-all`
- **Features**:
  - Current cycle detection
  - Cycle date calculations
  - Fix version pattern testing
  - Year configuration display

## Key Features

- **Comprehensive Analytics**: 11 specialized commands covering all aspects of JIRA workflow analysis
- **Time-based Filtering**: Flexible date ranges (last-week, date ranges, specific periods)
- **Export Capabilities**: JSON, CSV export with detailed metrics
- **Team/Squad Filtering**: Filter by Squad[Dropdown] field across commands
- **Caching System**: 1-hour cache expiration for improved performance
- **Statistical Analysis**: P95, median, standard deviation, outlier detection
- **Visualization**: Chart generation for resolution time analysis
- **Batch Operations**: Bulk component management and issue updates
- **Error Handling**: Comprehensive error messages with troubleshooting guidance

## Common Usage Patterns

### Issue Analysis Workflows

```bash
# 1. Get overview of current open work
python src/main.py syngenta jira open-issues --project-key CWS --issue-types "Bug,Story,Task"

# 2. Analyze team performance over time (last 30 days)
python src/main.py syngenta jira cycle-time --project-key CWS --end-date 2025-09-21 --window-days 30 --team Catalog

# 3. Check SLA compliance and set recommendations  
python src/main.py syngenta jira calculate-resolution-time --project-key CWS --time-period last-month --generate-charts

# 4. Monitor due date adherence (last 2 weeks)
python src/main.py syngenta jira issue-adherence --project-key CWS --end-date 2025-09-21 --window-days 14 --team Catalog

# 5. Analyze issue creation trends
python src/main.py syngenta jira issues-creation-analysis --time-period last-2-months --aggregation weekly

# 6. Track monthly issue velocity (created vs resolved)
python src/main.py syngenta jira issue-velocity --project-key CWS --time-period last-6-months --team Catalog
```

### Monthly Reporting Workflow

```bash
# Generate comprehensive monthly report
python src/main.py syngenta jira calculate-resolution-time --project-key CWS --time-period last-month --output-file monthly_sla.csv
python src/main.py syngenta jira issue-adherence --project-key CWS --time-period last-month --output-file monthly_adherence.json
python src/main.py syngenta jira issue-velocity --project-key CWS --time-period last-month --export csv --include-summary --output-file monthly_velocity.csv
python src/main.py syngenta jira issues-creation-analysis --time-period last-month --export csv --output-file monthly_creation.csv
```

### Time Period Options (Available in Most Commands)

- `last-week`: Last 7 days
- `last-2-weeks`: Last 14 days  
- `last-month`: Last 30 days
- `N-days`: Specific number of days (e.g., `15-days`)
- `YYYY-MM-DD to YYYY-MM-DD`: Date range (e.g., `2025-06-09 to 2025-06-22`)
- `YYYY-MM-DD`: Single date (e.g., `2025-06-15`)

## Setup & Configuration

### Environment Variables

Required environment variables (configure in `.env` files):

```env
# JIRA Configuration (Main .env or src/domains/syngenta/jira/.env)
JIRA_URL=https://your-jira-instance.atlassian.net
JIRA_USER_EMAIL=your-email@company.com  
JIRA_API_TOKEN=your-jira-api-token

# Slack Configuration (for epic-monitor)
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
SLACK_BOT_TOKEN=xoxb-your-bot-token-here
SLACK_CHANNEL_ID=YOUR_CHANNEL_ID

# Cycle Configuration
YEAR_START_DATE=2025-01-06
EPIC_MONITOR_BUSINESS_DAYS_THRESHOLD=3
EPIC_MONITOR_DUE_DATE_WARNING_DAYS=3
```

### JIRA API Setup

1. Generate JIRA API token from your Atlassian account
2. Ensure your account has appropriate project permissions
3. Test connection: `python src/main.py syngenta jira list-custom-fields`

### GitHub Actions Integration

The `epic-monitor` command supports automated execution:
- **Schedule**: Monday, Wednesday, Friday at 9:00 AM BRT
- **Configuration**: GitHub Secrets for JIRA and Slack credentials
- **Monitoring**: Automatic failure notifications and log uploads

## Monthly Issue Filtering Analysis

### Current Commands with Monthly Capabilities

**✅ EXISTING: Issues Creation Analysis**
- Command: `issues-creation-analysis`
- **Monthly filtering**: ✅ Built-in via `--time-period last-month` or date ranges
- **Issue type filtering**: ✅ Via `--issue-types` parameter
- **Monthly aggregation**: ✅ Via `--aggregation monthly`
- **Created vs Resolved**: ❌ Only tracks **created** issues

**✅ EXISTING: Resolution Time Analysis**
- Command: `calculate-resolution-time`
- **Monthly filtering**: ✅ Built-in via `--time-period last-month`  
- **Issue type filtering**: ✅ Via `--issue-types` parameter
- **Created vs Resolved**: ❌ Only tracks **resolved** issues

**✅ NEW: Combined Created + Resolved Monthly Analysis**
- Command: `issue-velocity`
- **Monthly filtering**: ✅ Built-in via `--time-period last-6-months`, date ranges
- **Issue type filtering**: ✅ Via `--issue-types` parameter (default: Bug)
- **Team filtering**: ✅ Via `--team` parameter (Squad[Dropdown] field)
- **Label filtering**: ✅ Via `--labels` parameter
- **Created vs Resolved**: ✅ Tracks **both** created and resolved monthly
- **Velocity metrics**: ✅ Net velocity, efficiency, backlog impact, trends
- **Aggregation**: ✅ Monthly and quarterly options
- **Export**: ✅ JSON and CSV formats

## Architecture & Integration

### File Structure
```
src/domains/syngenta/jira/
├── *_command.py              # CLI command implementations
├── *_service.py              # Business logic services  
├── jira_processor.py         # Core JIRA API wrapper
├── jira_to_slack_user_mapping.json # User mapping for notifications
└── README.md                 # This documentation
```

### Core Dependencies
- **JIRA API Client**: Custom `JiraApiClient` with authentication
- **Caching System**: `CacheManager` with 1-hour expiration
- **Logging**: Centralized `LogManager` with color-coded output
- **Data Export**: JSON/CSV export via `JSONManager` and `OutputManager`

### Cycle System
- **Quarters**: Q1-Q4 with 2 cycles each (Q1C1, Q1C2, etc.)
- **Duration**: C1 = 6 weeks, C2 = 7 weeks (13 weeks/quarter)
- **Configuration**: `YEAR_START_DATE` environment variable
- **Pattern Matching**: Flexible fix version detection

## Data Flow & Caching

1. **Command Layer**: Argument parsing and validation
2. **Service Layer**: Business logic and data processing  
3. **JIRA API**: REST calls with authentication
4. **Caching**: File-based cache with TTL (1 hour default)
5. **Export**: JSON/CSV output with structured data
6. **Logging**: Multi-level logging with rotation

## Troubleshooting

### Common Issues

1. **Invalid Issue Type Error**
   ```bash
   # Diagnose: List available issue types for project
   python src/main.py syngenta jira list-custom-fields --project-key CWS
   
   # Fix: Use correct issue types in commands
   --issue-types "Bug,Story,Task,Epic"
   ```

2. **JIRA Authentication Failed**
   ```bash
   # Check environment variables are loaded
   python src/main.py syngenta jira list-custom-fields
   
   # Verify credentials in .env file
   # Ensure JIRA_API_TOKEN has project permissions
   ```

3. **No Data Found**
   ```bash
   # Check date ranges and filters
   python src/main.py syngenta jira open-issues --project-key CWS --issue-types Bug
   
   # Verify project key exists and is accessible
   # Adjust time periods for historical data
   ```

4. **Cache Issues**
   ```bash
   # Clear cache for fresh data
   python src/main.py syngenta jira issues-creation-analysis --clear-cache
   
   # Cache files stored in cache/ directory (1-hour TTL)
   ```

### Debug Commands

```bash
# Test JIRA connectivity
python src/main.py syngenta jira list-custom-fields

# Test cycle calculations  
python src/main.py syngenta jira cycle-info --show-all

# Get current open issues snapshot
python src/main.py syngenta jira open-issues --project-key CWS --issue-types Bug --verbose
```

## Development & Extension

### Adding New Commands

1. **Create Command Class**: Inherit from `BaseCommand`
2. **Create Service Class**: Implement business logic
3. **Follow Patterns**: Use existing commands as templates
4. **Add Documentation**: Update this README

### Key Integration Points

- **JiraProcessor**: Core JIRA API wrapper (`jira_processor.py`)
- **CacheManager**: File-based caching with TTL
- **LogManager**: Centralized logging with colors
- **OutputManager**: Structured file exports

### Service Layer Patterns

```python
# Standard service initialization
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager

ensure_env_loaded()
logger = LogManager.get_instance().get_logger("ServiceName")
```

## Security & Best Practices

- **Environment Variables**: Never commit credentials
- **API Token Rotation**: Regular security maintenance
- **Least Privilege**: Minimal JIRA project permissions
- **Cache Management**: Sensitive data in temporary files
- **Error Handling**: No credential exposure in logs
