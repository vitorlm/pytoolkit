# PyToolkit - Agents Guide

This file provides comprehensive guidance for AI coding agents working with the PyToolkit domain-driven CLI framework. PyToolkit is a production-grade enterprise system for data processing, analytics, and automation across multiple business domains.

## Project Overview

PyToolKit is a domain-driven CLI framework that dynamically discovers and loads commands from domain-specific modules. It's designed for enterprise data processing, analytics, and automation across multiple business domains including personal finance, Syngenta operations, JIRA management, SonarQube analysis, and more.

### Core Innovation: Auto-Discovery System

The framework's "magic" is **zero-registration command discovery**:

- **CommandManager** scans `src/domains/` recursively using Python's introspection
- Dynamically imports modules and inspects classes inheriting from `BaseCommand`
- Builds hierarchical CLI structure automatically
- **Key Principle**: Create any class inheriting from `BaseCommand` in `src/domains/` and it becomes available in CLI automatically

### Architecture Principles

- **Domain-Driven Design**: Commands organized by business domains
- **Command-Service Separation**: Thin CLI wrappers delegate to service classes
- **Singleton Infrastructure**: LogManager, CacheManager, Configuration management
- **Enterprise-Grade**: Comprehensive logging, caching, error handling

## Development Environment Setup

### Initial Setup

```bash
# Create virtual environment and install dependencies
./setup.sh

# Activate virtual environment (ALWAYS run before development)
source .venv/bin/activate

# Install dependencies manually if needed
pip install -r requirements.txt
```

### Environment Configuration

PyToolkit uses multi-environment support:

- Main `.env` file in project root
- Domain-specific `.env` files:
  - `src/domains/syngenta/jira/.env` - JIRA authentication and endpoints
  - `src/domains/syngenta/sonarqube/.env` - SonarQube/SonarCloud configuration
  - `src/domains/syngenta/datadog/.env` - Datadog API configuration

Environment loader automatically discovers and loads all relevant `.env` files.

## Code Style Guidelines

### Mandatory Code Quality Pipeline

Run these commands before every commit:

```bash
# Code formatting (MANDATORY)
ruff format src/

# Linting (MUST pass)
ruff check src/
```

### Command Structure Pattern (STRICT)

Every command MUST follow this exact pattern:

```python
from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager

class YourCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "command-name"  # kebab-case, becomes CLI subcommand

    @staticmethod
    def get_description() -> str:
        return "Brief description of command functionality"

    @staticmethod
    def get_help() -> str:
        return "Detailed help text with usage examples"

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument("--required-arg", required=True, help="Required argument")
        parser.add_argument("--optional-arg", required=False, help="Optional argument")

    @staticmethod
    def main(args: Namespace):
        # ALWAYS start with these two lines
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("YourCommand")

        try:
            # Delegate ALL business logic to service
            service = YourService()
            result = service.execute(args)
            logger.info("Command completed successfully")
        except Exception as e:
            logger.error(f"Command failed: {e}")
            exit(1)  # CLI commands MUST exit with error codes
```

### Command-Service Separation (MANDATORY)

- **Commands**: Thin CLI wrappers, argument parsing only
- **Services**: ALL business logic, data processing, API calls
- **Never**: Put complex logic directly in command files

### Domain Structure

```bash
src/domains/
  └── your_domain/
      ├── __init__.py
      ├── your_command.py          # Command implementation (thin layer)
      ├── your_service.py          # Business logic (MANDATORY)
      ├── .env                     # Domain-specific config (optional)
      └── data/                    # Domain-specific data (optional)
```

## Build and Test Commands

### Running the CLI

```bash
# Show available domains and commands
python src/main.py --help

# Execute specific command
python src/main.py <domain> <command> [args]

# Examples from existing commands
python src/main.py syngenta jira epic-monitoring --project-key "CWS"
python src/main.py syngenta sonarqube sonarqube --operation list-projects
python src/main.py personal_finance nfce process --input-folder "data/"
```

### Health Checks

```bash
# Test JIRA connectivity
python src/main.py syngenta jira list-custom-fields

# Test SonarQube connectivity
python src/main.py syngenta sonarqube sonarqube --operation list-projects

# Check environment loading
python -c "from utils.env_loader import ensure_env_loaded; ensure_env_loaded()"
```

## Essential Utilities and Patterns

### Environment Loading (MANDATORY)

```python
from utils.env_loader import ensure_env_loaded

# ALWAYS call this first in main() method
ensure_env_loaded()  # General environment loading

# Domain-specific loading available
from utils.env_loader import ensure_jira_env_loaded
ensure_jira_env_loaded()   # For JIRA commands
```

### Logging System (MANDATORY)

```python
from utils.logging.logging_manager import LogManager

# Use singleton pattern - get logger with component name
logger = LogManager.get_instance().get_logger("ComponentName")

# For submodules, use hierarchical naming
logger = LogManager.get_instance().get_logger("MainComponent", "SubModule")

# Standard usage
logger.debug("Debug information")
logger.info("General information")
logger.warning("Warning message")
logger.error("Error occurred", exc_info=True)  # Include stack trace
```

**Features**: Automatic file rotation (hourly), color-coded console output, module-specific color coding, automatic log file management in `logs/` directory.

### Caching System (MANDATORY for API operations)

```python
from utils.cache_manager.cache_manager import CacheManager

# Get singleton instance
cache = CacheManager.get_instance()

# Standard caching pattern
cache_key = f"operation_{param1}_{param2}"
cached_data = cache.load(cache_key, expiration_minutes=60)

if cached_data is None:
    # Cache miss - fetch fresh data
    fresh_data = expensive_operation()
    cache.save(cache_key, fresh_data)
    data = fresh_data
else:
    logger.info("Using cached data")
    data = cached_data

# Clear cache when needed
cache.clear_all()  # or cache.invalidate(cache_key)
```

### Data Management Utilities

#### JSON Manager

```python
from utils.data.json_manager import JSONManager

# Read with default fallback
data = JSONManager.read_json("file.json", default={})

# Write with automatic backup
JSONManager.write_json(data, "file.json")

# Update existing JSON files
JSONManager.append_or_update_json("file.json", {"new_key": "value"})
```

#### DuckDB Manager

```python
from utils.data.duckdb_manager import DuckDBManager

# Initialize and configure
db_manager = DuckDBManager()
db_manager.add_connection_config({
    "name": "main_db",
    "path": "data/database.duckdb",
    "read_only": False
})

# Get connection and execute queries
conn = db_manager.get_connection("main_db")
results = conn.execute("SELECT * FROM table").fetchall()

# Create tables with automatic schema inference
schema = db_manager.create_table("main_db", "table_name", sample_data=sample_records)

# Bulk insert with validation and batching
db_manager.insert_records("main_db", "table_name", schema, records, batch_size=2500)
```

#### Excel Manager

```python
from utils.data.excel_manager import ExcelManager

# Read Excel files
excel_file = ExcelManager.read_excel("file.xlsx")

# Write DataFrames to Excel
ExcelManager.write_excel(dataframe, "output.xlsx", sheet_name="Data")

# List available sheets
sheets = ExcelManager.list_excel_sheets("file.xlsx")
```

### File Management

```python
from utils.file_manager import FileManager

# File operations with validation
files = FileManager.list_files("directory", extension=".json")
content = FileManager.read_file("file.txt")
FileManager.delete_file("file.txt")

# Directory operations
FileManager.create_folder("new_folder")
FileManager.validate_folder("existing_folder")

# File validation with allowed extensions
FileManager.validate_file("file.pdf", allowed_extensions=[".pdf", ".doc"])

# Generate standardized file names
filename = FileManager.generate_file_name(
    module="processor",
    suffix="results",
    extension=".json"
)
```

### Error Handling Pattern

```python
from utils.error.error_manager import handle_generic_exception

try:
    result = risky_operation()
except Exception as e:
    logger.error(f"Operation failed: {e}")
    handle_generic_exception(e, "Context about what failed", {"key": "metadata"})
    exit(1)  # Always exit with error code for CLI commands
```

### JIRA Integration

```python
from utils.jira.jira_api_client import JiraApiClient

# Initialize client (usually in service layer)
client = JiraApiClient(base_url, email, api_token)

# Standard REST operations
response = client.get("issue/PROJECT-123")
created = client.post("issue", json=issue_data)
updated = client.put("issue/PROJECT-123", json=update_data)
client.delete("issue/PROJECT-123")
```

### Base Processor Pattern

```python
from utils.base_processor import BaseProcessor

class MyProcessor(BaseProcessor):
    def __init__(self):
        super().__init__(allowed_extensions=[".xlsx", ".csv"])

    def process_file(self, file_path, **kwargs):
        # Process single file
        return {"data": "processed"}

    def process_sheet(self, sheet_data, **kwargs):
        # Process sheet data
        return processed_data

# Usage in commands
processor = MyProcessor()
results = processor.process_folder("input_folder")
```

## Domain-Specific Knowledge

### Current Domain Structure

```bash
src/domains/
├── personal_finance/           # Financial data processing (credit cards, payroll)
├── syngenta/                  # Syngenta enterprise operations
│   ├── ag_operations/         # Agricultural operations data management
│   ├── jira/                  # JIRA integration and management (11 commands)
│   ├── sonarqube/             # SonarQube code quality analysis
│   ├── team_assessment/       # Team performance analysis
│   └── datadog/               # Datadog Teams & Services audit
```

### JIRA Domain (11 Commands)

Comprehensive JIRA integration with enterprise features:

**Analysis & Reporting Commands:**

- `epic-monitoring` - Monitor epic progress with Slack notifications
- `cycle-time` - Started to Done analysis with statistical metrics
- `issue-adherence` - Due date compliance analysis
- `calculate-resolution-time` - SLA analysis with P95 metrics
- `issues-creation-analysis` - Issue creation patterns over time
- `open-issues` - Current open issues snapshot
- `issue-velocity` - Monthly creation vs resolution analysis

**Management Commands:**

- `components` - Comprehensive component management (CRUD operations)
- `fill-missing-dates` - Automated date filling for epics
- `list-custom-fields` - Field discovery for debugging
- `cycle-info` - Cycle calculations and testing

**Key Features:**

- Time-based filtering: `last-week`, `last-month`, date ranges
- Team/squad filtering via Squad[Dropdown] field
- Caching system (1-hour expiration)
- Statistical analysis: P95, median, standard deviation
- Export capabilities: JSON, CSV
- GitHub Actions integration

### SonarQube Domain

Code quality analysis for Syngenta Digital projects:

**Key Features:**

- 27 predefined Syngenta Digital projects
- 16 quality metrics (security, reliability, maintainability)
- 1-hour cache expiration for performance
- Batch operations for multiple projects
- Quality Gate status, security ratings, coverage metrics

**Available Commands:**

- `sonarqube` - Main command with operations: `list-projects`, `measures`, `issues`

### Personal Finance Domain

Financial data processing:

- Credit card statement processing
- Payroll data analysis
- NFCE (Brazilian tax receipt) processing

### Data Storage Architecture

**Standard Directories:**

- `data/` - DuckDB databases and analytical data
- `cache/` - JSON cache files with expiration
- `logs/` - Rotated log files (hourly rotation)
- `output/` - Generated reports and visualizations

**Database Integration:**

- **Primary**: DuckDB for analytical operations (`data/ag_operations.duckdb`)
- **Caching**: File-based JSON caching with expiration
- **External**: JIRA, SonarQube, Datadog API integrations

## Development Best Practices

### Command Development Checklist

1. ✅ Inherit from `BaseCommand`
2. ✅ Call `ensure_env_loaded()` first in main()
3. ✅ Use `LogManager.get_instance().get_logger()`
4. ✅ **Keep command files thin** - delegate logic to service classes
5. ✅ Create separate service file for business logic
6. ✅ Implement proper error handling with exit codes
7. ✅ Add caching for expensive operations
8. ✅ Validate inputs and file paths
9. ✅ Follow naming conventions (kebab-case for command names)
10. ✅ Add comprehensive help text with examples

### DO's and DON'Ts

#### ✅ DO

- Always use singleton managers (LogManager, CacheManager)
- **Keep command files thin** - delegate business logic to service classes
- Create separate service files for all business logic
- Implement proper error handling with logging and exit codes
- Cache expensive operations (API calls, file processing)
- Use type hints and validate inputs
- Follow the BaseCommand pattern exactly
- Use domain-specific environment files

#### ❌ DON'T

- Create commands without inheriting from BaseCommand
- **Put business logic directly in command files** - use service classes instead
- Skip environment loading in main() methods
- Ignore error handling or logging
- Hardcode configuration values
- Skip input validation
- Use print() instead of logging
- Create commands without proper argument parsing

### Time Range Patterns

Standardized across JIRA and other time-based commands:

- `last-week`: Last 7 days
- `last-2-weeks`: Last 14 days
- `last-month`: Last 30 days
- `N-days`: Specific number of days (e.g., `30-days`)
- `YYYY-MM-DD,YYYY-MM-DD`: Date ranges
- `YYYY-MM-DD`: Single date

### Performance Optimization

#### Caching Strategy

- **API Responses**: Always cache expensive API calls (JIRA, SonarQube, DataDog, etc.)
- **File Operations**: Use JSONManager for consistent file handling
- **Database**: Use DuckDB for analytical queries with proper indexing
- **Expiration**: Set appropriate cache expiration (1 hour for most operations)

#### Memory Management

- **Batch Processing**: Use appropriate batch sizes (2500 records for DuckDB)
- **File Streaming**: Use streaming for large file operations
- **Connection Pooling**: Reuse database connections where possible

## Security Best Practices

### Environment Security

```bash
# Use restrictive file permissions for environment files
chmod 600 src/domains/syngenta/jira/.env
chmod 600 src/domains/syngenta/sonarqube/.env
```

### Configuration Security

- **Token Isolation**: Each domain uses separate environment files
- **Never Commit Credentials**: Use .env files and .gitignore
- **Regular Token Rotation**: Implement security maintenance schedule
- **Least Privilege**: Minimal API permissions for each service
- **No Credential Exposure**: Never log sensitive information

### Data Protection

- **Cache Security**: Sensitive data cached with appropriate access controls
- **Log Sanitization**: API tokens never appear in logs
- **Access Auditing**: All API access properly logged

## CI/CD and Automation

### GitHub Actions Integration

- **Epic Monitoring**: Automated workflow runs Mon/Wed/Fri at 9:00 AM BRT
- **Location**: `.github/workflows/epic-monitoring.yml`
- **Features**: Comprehensive secret management, failure log uploads
- **Required Secrets**: JIRA authentication, Slack notification endpoints

### Docker Support

- Team assessment services: `src/domains/syngenta/team_assessment/docker-compose.yml`
- Ag operations services: `src/domains/syngenta/ag_operations/docker-compose.yml`
- Custom Dockerfiles available for containerized development

## Key Dependencies

### Core Dependencies

- **Data Processing**: pandas, duckdb, pyarrow
- **Visualization**: matplotlib, altair
- **CLI Framework**: argparse (built-in)
- **Logging**: Custom LogManager with color-coded output
- **File Processing**: openpyxl, pdfplumber for document handling

### External Integrations

- **JIRA**: Custom API client (`utils.jira.jira_api_client`)
- **SonarQube**: Project metrics and code quality analysis
- **Datadog**: Teams and services audit capabilities
- **LLM**: ollama for AI-powered analysis
- **Web Automation**: selenium for web scraping

## Advanced Features

### MCP (Model Context Protocol) Server

PyToolkit includes a sophisticated MCP server providing **46 capabilities** (16 Tools + 17 Resources + 13 Prompts):

**Key Features:**

- **100% reuse** of existing PyToolkit infrastructure
- Integration with Claude Desktop, VS Code Copilot, ChatGPT
- Project/team agnostic operations
- Comprehensive reporting automation

**Usage:**

```bash
# Start MCP server
cd src/mcp_server
python management_mcp_server.py

# Docker deployment available
docker-compose up -d
```

### Cycle System Integration

- **Quarters**: Q1-Q4 with 2 cycles each (Q1C1, Q1C2, etc.)
- **Duration**: C1 = 6 weeks, C2 = 7 weeks (13 weeks/quarter)
- **Configuration**: `YEAR_START_DATE` environment variable
- **Pattern Matching**: Flexible fix version detection

## Troubleshooting

### Common Issues

#### Environment Issues

```bash
# Check environment variables are loaded
python -c "from utils.env_loader import ensure_env_loaded; ensure_env_loaded()"

# Test specific domain environment
python src/main.py syngenta jira list-custom-fields
```

#### Authentication Problems

```bash
# Test JIRA connectivity
python src/main.py syngenta jira list-custom-fields

# Test SonarQube connectivity
python src/main.py syngenta sonarqube sonarqube --operation list-projects

# Verify credentials in appropriate .env files
```

#### Cache Issues

```bash
# Clear cache for fresh data
python -c "from utils.cache_manager.cache_manager import CacheManager; CacheManager.get_instance().clear_all()"

# Check cache directory
ls -la cache/
```

#### No Data Found

```bash
# Check date ranges and filters
python src/main.py syngenta jira open-issues --project-key CWS --issue-types Bug

# Verify project access and permissions
# Adjust time periods for historical data
```

### Debug Commands

```bash
# Test CLI discovery
python src/main.py --help

# Test specific domain
python src/main.py syngenta --help

# Enable debug logging
export PYTOOLKIT_LOG_LEVEL=DEBUG
python src/main.py <command>
```

## Creating New Commands

### 4-Step Process

```bash
# 1. Create domain directory with __init__.py
mkdir -p src/domains/new_domain
touch src/domains/new_domain/__init__.py

# 2. Create command file inheriting from BaseCommand
# File: src/domains/new_domain/command_name.py

# 3. Create service file for business logic
# File: src/domains/new_domain/service_name.py

# 4. Optional: Add domain-specific environment
# File: src/domains/new_domain/.env
```

### Template Files

#### Command Template

```python
# src/domains/your_domain/your_command.py
from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from .your_service import YourService

class YourCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "your-command"

    @staticmethod
    def get_description() -> str:
        return "Brief description of functionality"

    @staticmethod
    def get_help() -> str:
        return """
        Detailed help text with examples:

        Examples:
            python src/main.py your_domain your-command --arg value
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument("--arg", required=True, help="Required argument")
        parser.add_argument("--optional", required=False, help="Optional argument")

    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("YourCommand")

        try:
            service = YourService()
            result = service.execute(args)
            logger.info("Command completed successfully")
        except Exception as e:
            logger.error(f"Command failed: {e}")
            exit(1)
```

#### Service Template

```python
# src/domains/your_domain/your_service.py
from argparse import Namespace
from utils.logging.logging_manager import LogManager
from utils.cache_manager.cache_manager import CacheManager

class YourService:
    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("YourService")
        self.cache = CacheManager.get_instance()

    def execute(self, args: Namespace) -> any:
        """
        Main business logic implementation
        """
        self.logger.info("Starting service execution")

        # Implement your business logic here
        result = self._process_business_logic(args)

        self.logger.info("Service execution completed")
        return result

    def _process_business_logic(self, args: Namespace) -> any:
        """
        Complex business logic, API calls, data processing
        """
        # Your implementation here
        pass
```

## Additional Resources

### Project Structure Reference

```bash
PyToolkit/
├── src/
│   ├── main.py                    # CLI entry point
│   ├── config.py                  # Configuration management
│   ├── domains/                   # Domain commands (auto-discovered)
│   │   ├── personal_finance/      # Personal finance domain
│   │   └── syngenta/              # Syngenta enterprise domain
│   │       ├── jira/              # JIRA integration (11 commands)
│   │       ├── sonarqube/         # Code quality analysis
│   │       ├── team_assessment/   # Team performance
│   │       ├── ag_operations/     # Agricultural operations
│   │       └── datadog/           # Datadog integration
│   ├── utils/                     # Utility infrastructure
│   │   ├── command/               # Command framework
│   │   ├── logging/               # Logging system
│   │   ├── cache_manager/         # Caching system
│   │   ├── data/                  # Data management utilities
│   │   ├── jira/                  # JIRA API client
│   │   └── error/                 # Error handling
│   └── mcp_server/                # MCP server for AI integration
├── data/                          # DuckDB databases
├── cache/                         # JSON cache files
├── logs/                          # Rotated log files
├── output/                        # Generated reports
├── .env                          # Main environment file
├── requirements.txt              # Python dependencies
├── setup.sh                     # Environment setup script
└── agents.md                    # This file
```

### External Documentation

- **JIRA API**: <https://developer.atlassian.com/cloud/jira/platform/>
- **SonarQube API**: <https://docs.sonarqube.org/latest/extend/web-api/>
- **MCP Specification**: <https://modelcontextprotocol.io/specification>
- **Claude Desktop**: <https://docs.anthropic.com/claude/docs>
- **Python Type Hints**: <https://docs.python.org/3/library/typing.html>

---

This agents.md file serves as the comprehensive guide for AI coding agents working with PyToolkit. It provides structured, actionable instructions while maintaining the flexibility to adapt to various development scenarios within the domain-driven architecture.
