# PyToolkit MCP Management Server

A sophisticated Model Context Protocol (MCP) server that provides **46 capabilities** (16 Tools + 17 Resources + 13 Prompts) to expose PyToolkit's functionality to Large Language Models like Claude, ChatGPT, and Copilot. This server achieves **100% reuse** of existing PyToolkit infrastructure without code modification.

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- PyToolkit environment configured
- Node.js (for client applications)
- API tokens for integrated services (JIRA, SonarQube, etc.)

### Installation

1. **Clone and Setup PyToolkit** (if not already done):
```bash
git clone [repository]
cd PyToolkit
./setup.sh
source .venv/bin/activate
```

2. **Configure Environment Variables**:
```bash
# JIRA Configuration (src/domains/syngenta/jira/.env)
JIRA_BASE_URL=https://your-instance.atlassian.net
JIRA_EMAIL=your-email@company.com
JIRA_API_TOKEN=your-jira-token

# SonarQube Configuration (src/domains/syngenta/sonarqube/.env)
SONARQUBE_TOKEN=your-sonarqube-token
SONARCLOUD_TOKEN=your-sonarcloud-token

# CircleCI Configuration
CIRCLECI_TOKEN=your-circleci-token
CIRCLECI_PROJECT_SLUG=gh/your-org/your-repo  # Optional: specific project

# LinearB Configuration 
LINEARB_API_TOKEN=your-linearb-token
```

3. **Test the Server**:
```bash
cd src/mcp
python management_mcp_server.py
```

## 🔧 Platform Configuration

### Claude Desktop Setup

#### 1. Install Claude Desktop
Download from [Claude's official website](https://claude.ai/download) for macOS or Windows.

#### 2. Configure MCP Server

**macOS Configuration:**
```bash
# Open Claude Desktop configuration
open "~/Library/Application Support/Claude/claude_desktop_config.json"
```

**Windows Configuration:**
```bash
# Open Claude Desktop configuration
notepad "%APPDATA%\Claude\claude_desktop_config.json"
```

**Configuration Content:**
```json
{
  "mcpServers": {
    "pytoolkit-management": {
      "command": "python",
      "args": [
        "/path/to/PyToolkit/src/mcp/management_mcp_server.py"
      ],
      "env": {
        "PYTHONPATH": "/path/to/PyToolkit/src"
      }
    }
  }
}
```

#### 3. Verify Connection
1. Restart Claude Desktop
2. Look for the 🔌 connector icon in the chat interface
3. Test with: "What tools are available in the MCP server?"

### VS Code with GitHub Copilot

#### 1. Prerequisites
- VS Code 1.99+
- GitHub Copilot extension
- MCP support enabled in Copilot settings

#### 2. Workspace Configuration
Create `.vscode/mcp.json` in your project root:

```json
{
  "servers": {
    "PyToolkit-Management": {
      "type": "stdio",
      "command": "python",
      "args": [
        "/path/to/PyToolkit/src/mcp/management_mcp_server.py"
      ],
      "env": {
        "PYTHONPATH": "/path/to/PyToolkit/src"
      }
    }
  }
}
```

#### 3. Global Configuration
Use **MCP: Open User Configuration** command in VS Code Command Palette to add server to global `mcp.json`.

## 📊 Container Deployment

### Docker Setup

#### 1. Build Container
```bash
cd src/mcp_server
docker build -t pytoolkit-mcp .
```

#### 2. Run with Docker Compose

**Option A: Run stdio server (for local Claude Desktop connections)**
```bash
docker compose up mcp-server-stdio -d
```

**Option B: Run HTTP server (for remote connections)**  
```bash
docker compose up mcp-server-http -d
```

**Option C: Run both servers simultaneously**
```bash
docker compose up -d
```

#### Option D: Use the Docker Helper Script (Recommended)

```bash
# Make script executable (first time only)
chmod +x docker_run.sh

# Start stdio server
./docker_run.sh stdio

# Start HTTP server  
./docker_run.sh http

# Start both servers
./docker_run.sh both

# Other helpful commands
./docker_run.sh stop    # Stop all servers
./docker_run.sh logs    # Show logs
./docker_run.sh status  # Check status
./docker_run.sh build   # Rebuild images
```

#### 3. HTTP Interface (when using HTTP mode)
Access via HTTP at `http://localhost:8000`:
- Health check: `GET /health`
- List tools: `GET /tools`
- List resources: `GET /resources`  
- List prompts: `GET /prompts`
- Execute tool: `POST /tools/execute`
- Get resource: `POST /resources/get`
- Get prompt: `POST /prompts/get`

#### 4. Container Configuration

**Environment Variables:**
- `MCP_SERVER_MODE`: `stdio` or `http` (default: stdio)
- `MCP_SERVER_HOST`: Host binding for HTTP mode (default: 0.0.0.0)
- `MCP_SERVER_PORT`: Port for HTTP mode (default: 8000)
- `PYTHONPATH`: Python path (default: /app)

**Volumes Mounted:**
- Source code: `../../src:/app/src` (for development)
- Data: `../../data:/app/data`
- Cache: `../../cache:/app/cache`
- Logs: `../../logs:/app/logs`  
- Output: `../../output:/app/output`
- Environment files: `../../.env` and domain-specific `.env` files

## 🏗️ Architecture Overview

The MCP server is built on four core component types that work together to provide comprehensive functionality:

### 🔌 Adapters (4 Components)
**Purpose**: Direct integration with external services
**Location**: `src/mcp/adapters/`

#### JiraAdapter
- **Integration**: JIRA Cloud/Server APIs
- **Features**: Complete integration with PyToolkit JIRA services, 8+ command capabilities
- **Methods**: 
  - `get_epic_monitoring_data()` - Epic progress with problem detection
  - `get_cycle_time_analysis()` - Started to Done metrics
  - `get_velocity_analysis()` - Creation vs resolution trends
  - `get_adherence_analysis()` - Due date compliance
  - `get_comprehensive_dashboard()` - Integrated JIRA view

#### SonarQubeAdapter  
- **Integration**: SonarQube/SonarCloud APIs
- **Features**: 27 predefined Syngenta Digital projects, 16 quality metrics
- **Methods**:
  - `get_project_quality_metrics()` - Comprehensive quality data
  - `get_all_projects_metrics()` - Batch project analysis
  - `get_security_analysis()` - Security-focused metrics

#### CircleCIAdapter
- **Integration**: CircleCI Cloud/Server APIs integration
- **Features**: Complete integration with `src/domains/circleci/` infrastructure
- **Available Methods**: 
  - `get_pipeline_status()` - Real pipeline status and metrics
  - `get_build_metrics()` - Actual build performance data
  - `get_deployment_frequency()` - Deployment analytics
  - `get_project_list()` - Available projects discovery

#### LinearBAdapter
- **Integration**: LinearB APIs integration
- **Features**: Complete integration with `src/domains/linearb/` infrastructure
- **Available Methods**:
  - `get_engineering_metrics()` - Real engineering productivity data
  - `get_team_performance()` - Actual team performance analytics
  - `get_pr_metrics()` - Pull request metrics from LinearB API
  - `get_deployment_metrics()` - Real deployment performance data

### 🛠️ Tools (17 Components)
**Purpose**: Direct action execution for LLMs with **project and team agnostic** capabilities
**Location**: `src/mcp/tools/`

All tools are designed to support both **team-specific** and **tribe-wide** operations, matching the functionality of `run_reports.sh`.

#### JIRA Tools (5 tools) ✅ **Enhanced with Team Agnostic Parameters**
- `jira_get_epic_monitoring` - Monitor epic progress 
  - **Parameters**: `project_key` (required), `team` (optional - if not provided, returns all teams)
- `jira_get_cycle_time_metrics` - Team performance analysis
  - **Parameters**: `project_key`, `team`, `time_period`, `issue_types`
- `jira_get_team_velocity` - Velocity based on sprint history
  - **Parameters**: `project_key`, `team`, `time_period`, `issue_types`
- `jira_get_issue_adherence` - Due date adherence analysis
  - **Parameters**: `project_key`, `team`, `time_period`, `issue_types`, `status_categories`, `priorities`
- `jira_get_open_issues` - **NEW**: Open issues analysis (matches run_reports.sh)
  - **Parameters**: `project_key`, `team`, `issue_types`, `status_categories`, `priorities`

#### SonarQube Tools (4 tools) ✅ **Enhanced with Organization Filtering**
- `sonar_get_project_metrics` - Quality metrics for specific project
  - **Parameters**: `project_key` (required), `organization` (optional)
- `sonar_get_project_issues` - Issues by type and severity
  - **Parameters**: `project_key`, `issue_type`, `organization`
- `sonar_get_quality_overview` - Overview across all or filtered projects
  - **Parameters**: `organization` (optional), `project_keys` (optional - for team-specific filtering)
- `sonar_compare_projects_quality` - Compare multiple projects
  - **Parameters**: `project_keys` (array), `organization`

#### LinearB Tools (5 tools) ✅ **Enhanced with Comprehensive Parameters**
- `linearb_get_engineering_metrics` - Engineering productivity data
  - **Parameters**: `time_range`, `team_ids` (optional), `filter_type`, `granularity`, `aggregation`
- `linearb_get_team_performance` - Team performance analytics
  - **Parameters**: `team_ids` (optional), `time_range`, `filter_type`
- `linearb_get_pr_metrics` - Pull request metrics
  - **Parameters**: `time_range`, `team_ids` (optional), `filter_type`
- `linearb_get_deployment_metrics` - Deployment performance data
  - **Parameters**: `time_range`, `team_ids` (optional), `filter_type`, `granularity`, `aggregation`
- `linearb_export_report` - **NEW**: Comprehensive report export (matches run_reports.sh)
  - **Parameters**: `team_ids`, `time_range`, `filter_type`, `granularity`, `aggregation`, `format`, `beautified`, `return_no_data`

#### CircleCI Tools (3 tools) ✅ **Project-Centric Design**
- `circleci_get_pipeline_status` - Pipeline status and metrics
  - **Parameters**: `project_slug` (required), `limit`
- `circleci_get_build_metrics` - Build performance data  
  - **Parameters**: `project_slug` (required), `days`
- `circleci_analyze_deployment_frequency` - Deployment analytics
  - **Parameters**: `project_slug` (required), `days`

#### System Tools (1 tool)
- `health_check` - Server health verification

### 🎯 **Project/Team Agnostic Design**

All tools now support the dual-mode operation pattern from `run_reports.sh`:

**Team-Specific Mode** (Catalog team example):
```json
{
  "project_key": "CWS",
  "team": "Catalog",
  "time_period": "last-week"
}
```

**Tribe-Wide Mode** (all teams in project):
```json
{
  "project_key": "CWS",
  "time_period": "last-week"
}
```

This allows LLMs to generate reports for individual teams or entire tribes with the same tools.

### 📚 Resources (17 Components)
**Purpose**: Structured data aggregation with intelligent caching
**Location**: `src/mcp/resources/`

Resources provide rich, contextual data by combining multiple sources:

#### Team Metrics Resources (4 resources)
- `team://performance_dashboard` - Consolidated performance view (2h cache)
- `team://quarterly_summary` - Q1-C1, Q1-C2 structure (3h cache)
- `team://health_indicators` - AI-powered health analysis (1.5h cache)
- `team://weekly_metrics` - Weekly metrics compatible with run_reports.sh (30min cache)

#### Quality Metrics Resources (4 resources)
- `quality://code_quality_overview` - Consolidated quality across projects (1.5h cache)
- `quality://technical_debt_analysis` - Prioritized technical debt (4h cache)
- `quality://security_vulnerabilities_summary` - Security analysis (1h cache)
- `quality://weekly_quality_health` - Weekly quality snapshot (1h cache)

#### Pipeline Resources (4 resources)
- `pipeline://deployment_pipeline_status` - Real-time pipeline status (15min cache)
- `pipeline://build_success_rates` - Build success analysis (1h cache)
- `pipeline://deployment_frequency_trends` - Long-term deployment performance (3h cache)
- `pipeline://ci_cd_health_dashboard` - Comprehensive CI/CD health (45min cache)

#### Weekly Report Resources (5 resources)
- `weekly://complete_engineering_report` - Complete run_reports.sh equivalent (30min cache)
- `weekly://jira_metrics_summary` - JIRA data formatted for templates (45min cache)
- `weekly://sonarqube_quality_snapshot` - SonarQube data for reports (1h cache)
- `weekly://linearb_engineering_summary` - LinearB weekly metrics (1.5h cache)
- `weekly://template_ready_data` - Pre-formatted template data (30min cache)

### 💬 Prompts (13 Components)
**Purpose**: Specialized AI prompts for complex analysis with **project/team agnostic** support
**Location**: `src/mcp/prompts/`

Prompts provide sophisticated AI-driven analysis and reporting with full support for both team-specific and tribe-wide operations:

#### Weekly Report Prompts (5 prompts) ✅ **Team Agnostic Enhanced**

- `generate_weekly_engineering_report` - Complete weekly report generation
  - **Parameters**: `project_key` (default: "CWS"), `team_name` (optional - defaults to tribe-wide), `include_comparison`, `output_format`
  - **Team-Specific**: `{"project_key": "CWS", "team_name": "Catalog"}`
  - **Tribe-Wide**: `{"project_key": "CWS"}` (no team_name = all teams)
- `analyze_weekly_data_collection` - Data-driven insights from collections
  - **Parameters**: `focus_areas`, `include_recommendations`
- `format_template_sections` - Precise template formatting  
  - **Parameters**: `sections`, `week_range`
- `generate_next_actions` - Data-driven action items
- `compare_weekly_metrics` - Trend analysis for improvements

#### Quarterly Review Prompts (3 prompts)

- `quarterly_cycle_analysis` - Complete Q1-C1, Q1-C2 analysis
- `quarterly_retrospective_data` - Retrospective data preparation  
- `cycle_planning_insights` - Strategic cycle planning

#### Quality Report Prompts (3 prompts) ✅ **Project Filtering Enhanced**

- `code_quality_report` - Comprehensive quality assessment
  - **Parameters**: `project_key` (optional), `include_trends`
- `technical_debt_prioritization` - Prioritized technical debt analysis
  - **Parameters**: `project_key` (required)
- `security_assessment` - Security analysis and recommendations
  - **Parameters**: `include_all_projects` (for organization-wide analysis)

#### Team Performance Prompts (2 prompts) ✅ **Project/Team Agnostic**

- `team_health_assessment` - Comprehensive team evaluation
  - **Parameters**: `project_key` (required), `time_period`
- `productivity_improvement_plan` - Data-driven improvement recommendations
  - **Parameters**: `project_key` (required), `focus_areas`

## 🎯 Usage Examples

### Basic Tool Usage

**Get Epic Monitoring:**
```
Use the jira_get_epic_monitoring tool with project_key "CWS" to check current epic status.
```

**CircleCI Pipeline Status:**
```
Use circleci_get_pipeline_status to get real-time pipeline status and build metrics.
```

**LinearB Engineering Metrics:**
```
Use linearb_get_engineering_metrics with time_range "last-week" to get team productivity data.
```

**Quality Overview:**
```
Run sonar_get_quality_overview to see the health of all our projects.
```

### Resource Access

**Team Dashboard:**
```
Show me the team performance dashboard resource to get our consolidated metrics.
```

**Weekly Report Data:**
```
Access the weekly complete engineering report resource for this week's data.
```

### Prompt-Driven Analysis

**Weekly Report Generation:**
```
Use the generate_weekly_engineering_report prompt to create this week's report for the CWS project and Catalog team.
```

**Quarterly Analysis:**
```
Generate a quarterly cycle analysis for Q1-C2 with recommendations for next cycle.
```

### Advanced Workflows

**Complete Weekly Report:**
```
1. Use the weekly://template_ready_data resource to get pre-formatted data
2. Apply the generate_weekly_engineering_report prompt with template formatting
3. Use the generate_next_actions prompt to create actionable items
4. Format everything using the format_template_sections prompt
```

**Quality Assessment:**
```
1. Get quality overview with sonar_get_quality_overview tool
2. Access quality://technical_debt_analysis resource for priorities
3. Use technical_debt_prioritization prompt for recommendations
4. Apply security_assessment prompt for security review
```

## 🔒 Security Best Practices

### Authentication Security
- **Token Isolation**: Each adapter uses separate environment files
- **Resource Indicators**: Implement RFC 8707 for token scoping
- **Zero Trust**: Each component validates access independently

### Configuration Security
```bash
# Use restrictive file permissions
chmod 600 src/domains/syngenta/jira/.env
chmod 600 src/domains/syngenta/sonarqube/.env

# Rotate tokens regularly
# Monitor access logs
tail -f logs/mcp_server.log | grep "Authentication"
```

### Network Security
- **Local Deployment**: Run MCP server locally when possible
- **Container Isolation**: Use Docker for remote deployments
- **TLS Encryption**: Always use HTTPS for remote connections

### Data Protection
- **Cache Security**: Sensitive data cached with encryption
- **Log Sanitization**: API tokens never logged
- **Access Auditing**: All resource access logged

## 🚀 Advanced Configuration

### Performance Optimization

#### Cache Configuration
```python
# Adjust cache expiration based on data sensitivity
CACHE_EXPIRATION = {
    "pipeline_status": 15,      # Fast-changing
    "quality_metrics": 60,      # Moderate
    "technical_debt": 240       # Slow-changing
}
```

#### Resource Tuning
```python
# Parallel resource loading
CONCURRENT_ADAPTERS = 4
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
```

### Custom Integration

#### Adding New Adapters
```python
# src/mcp/adapters/custom_adapter.py
from .base_adapter import BaseAdapter

class CustomAdapter(BaseAdapter):
    def __init__(self):
        super().__init__("Custom")
        
    def get_custom_data(self):
        # Your integration logic
        pass
```

#### Creating Custom Resources
```python
# src/mcp/resources/custom_resources.py
from .base_resource import BaseResourceHandler

class CustomResourceHandler(BaseResourceHandler):
    def get_resource_definitions(self):
        return [
            Resource(
                uri=AnyUrl("custom://data"),
                name="Custom Data",
                description="Your custom data source",
                mimeType="text/markdown"
            )
        ]
```

## 📊 Monitoring and Maintenance

### Health Monitoring
```bash
# Server health check
curl http://localhost:8000/health

# Check adapter connectivity
python -c "
from src.mcp.management_mcp_server import ManagementMCPServer
server = ManagementMCPServer()
print(server._health_check())
"
```

### Log Analysis
```bash
# Monitor MCP operations
tail -f logs/mcp_server.log | grep -E "(Tool|Resource|Prompt)"

# Check adapter performance
grep "Adapter" logs/mcp_server.log | tail -20
```

### Cache Management
```bash
# Clear cache for fresh data
python -c "
from utils.cache_manager.cache_manager import CacheManager
CacheManager.get_instance().clear_all()
"

# Monitor cache efficiency
grep "cache" logs/mcp_server.log | grep -E "(hit|miss)"
```

## 🔄 Integration with Existing Workflows

### run_reports.sh Replacement
The MCP server completely replaces manual `run_reports.sh` execution:

**Before:**
```bash
python src/main.py syngenta jira issue-adherence --project-key CWS
python src/main.py syngenta jira cycle-time --project-key CWS 
python src/main.py syngenta sonarqube sonarqube --operation list-projects
```

**After:**
```
Access the weekly://complete_engineering_report resource for automated data collection.
```

### report_template.md Integration
Resources provide data in exact template format:
- **Structured Tables**: Pre-formatted markdown tables
- **Trend Indicators**: Week-over-week comparisons
- **Action Items**: Data-driven recommendations

### Quarterly/Cycle Workflow
Replaces sprint-based planning:
- **Q1-C1, Q1-C2**: Structured periods (~45 days each)
- **Integrated Planning**: Cross-functional data in single view
- **Historical Analysis**: Performance tracking across cycles

## 🆘 Troubleshooting

### Common Issues

#### Connection Problems
```bash
# Check server status
python src/mcp/management_mcp_server.py

# Verify environment
python -c "from utils.env_loader import ensure_env_loaded; ensure_env_loaded()"
```

#### Authentication Errors  
```bash
# Test JIRA connection
python -c "
from src.mcp.adapters.jira_adapter import JiraAdapter
adapter = JiraAdapter()
print(adapter.initialize_service())
"

# Test SonarQube connection
python -c "
from src.mcp.adapters.sonarqube_adapter import SonarQubeAdapter  
adapter = SonarQubeAdapter()
print(adapter.initialize_service())
"
```

#### Performance Issues
```bash
# Check cache status
grep "cache" logs/mcp_server.log | tail -10

# Monitor resource timing
grep "Resource.*completed" logs/mcp_server.log
```

### Debug Mode
Enable detailed logging:
```bash
export PYTOOLKIT_LOG_LEVEL=DEBUG
python src/mcp/management_mcp_server.py
```

## 📈 Maximizing MCP Benefits

### Best Practices for AI Interactions

#### 1. **Contextual Queries**
```
Instead of: "Show me JIRA data"
Use: "Access the team performance dashboard resource to analyze our current cycle metrics and identify bottlenecks"
```

#### 2. **Workflow Automation**
```
"Generate a complete weekly engineering report using:
1. The weekly template-ready data resource  
2. The generate weekly engineering report prompt
3. Include comparison analysis and next actions"
```

#### 3. **Cross-Platform Analysis**
```
"Compare our code quality trends from SonarQube with our deployment frequency from CircleCI to identify correlation patterns"
```

### Advanced AI Workflows

#### Quarterly Planning Session
```
1. Use quarterly_cycle_analysis prompt for Q1-C2 retrospective
2. Access team://quarterly_summary resource for historical data
3. Apply cycle_planning_insights prompt for Q2-C1 planning
4. Generate productivity_improvement_plan based on findings
```

#### Quality-Driven Development
```
1. Get technical debt analysis from quality://technical_debt_analysis
2. Use technical_debt_prioritization prompt for sprint planning
3. Track progress with weekly quality health resource
4. Adjust priorities based on security assessment prompt
```

#### Performance Optimization Cycle
```
1. Monitor team health with team://health_indicators resource
2. Identify bottlenecks using team_health_assessment prompt
3. Track improvements with weekly metrics resource
4. Validate changes with quarterly analysis
```

## 🤝 Contributing

### Adding New Functionality

1. **Create Adapter**: For new service integrations
2. **Add Tools**: For direct LLM actions
3. **Build Resources**: For complex data aggregation
4. **Design Prompts**: For sophisticated AI analysis

### Testing Your Changes
```bash
# Run health check
python src/mcp/management_mcp_server.py

# Test specific components
python -c "from src.mcp.adapters.your_adapter import YourAdapter; YourAdapter().initialize_service()"
```

## 📚 Additional Resources

- **MCP Specification**: https://modelcontextprotocol.io/specification
- **Claude Desktop Documentation**: https://docs.anthropic.com/claude/docs
- **PyToolkit Documentation**: See main project README
- **Security Guidelines**: https://modelcontextprotocol.io/security

## 📝 License

This project inherits the license from the main PyToolkit project.

---