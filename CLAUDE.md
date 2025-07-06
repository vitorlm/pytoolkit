# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Environment Setup
- `./setup.sh` - Creates virtual environment and installs dependencies
- `source .venv/bin/activate` - Activates the virtual environment
- `pip install -r requirements.txt` - Installs Python dependencies

### Running the CLI
- `python src/main.py --help` - Shows available domains and commands
- `python src/main.py <domain> <command> [args]` - Executes specific commands

### Code Quality
- `flake8 src/` - Lints Python code
- `black src/` - Formats Python code
- `isort src/` - Sorts imports

### Alternative Package Management
- `poetry install` - Install dependencies using Poetry (alternative to pip)
- `poetry shell` - Activate virtual environment with Poetry

## Architecture Overview

PyToolkit is a domain-driven CLI framework that dynamically loads commands from modules. The architecture follows these key principles:

### Command System
- **BaseCommand**: Abstract base class that all commands must inherit from (`src/utils/command/base_command.py`)
- **CommandManager**: Dynamically discovers and loads commands from the domains directory (`src/utils/command/command_manager.py`)
- Commands are auto-discovered by scanning `src/domains/` for classes inheriting from `BaseCommand`

### Domain Structure
Commands are organized by business domains under `src/domains/`:
- `personal_finance/` - Financial data processing (credit cards, payroll)
- `syngenta/` - Syngenta-specific operations:
  - `ag_operations/` - Agricultural operations data management
  - `jira/` - JIRA integration and management
  - `sonarqube/` - SonarQube code quality analysis
  - `team_assessment/` - Team performance analysis

### Utilities Infrastructure
- **Logging**: Centralized logging via `LogManager` with configurable levels and file rotation
- **Caching**: File-based caching system with expiration support
- **Data Management**: Specialized managers for DuckDB, DynamoDB, Excel, and JSON
- **Error Handling**: Structured error management with custom exception types

### Configuration and Environment
- Environment variables loaded via `src/utils/env_loader.py` with smart fallback
- Configuration centralized in `src/config.py`
- **Multi-environment support**: Main `.env` file plus domain-specific `.env` files in:
  - `src/domains/syngenta/jira/.env` - JIRA authentication and endpoints
  - `src/domains/syngenta/sonarqube/.env` - SonarQube/SonarCloud configuration
- Environment loader automatically discovers and loads all relevant `.env` files

## Creating New Commands

### Command Structure Pattern

All commands must inherit from `BaseCommand` (`src/utils/command/base_command.py`) and follow this standard pattern:

```python
from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager

class YourCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "command-name"
    
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
        # ALWAYS start with environment loading
        ensure_env_loaded()
        
        # Get logger with component name
        logger = LogManager.get_instance().get_logger("YourCommand")
        
        try:
            # Command logic here
            logger.info("Command completed successfully")
        except Exception as e:
            logger.error(f"Command failed: {e}")
            exit(1)
```

### Directory Structure
```
src/domains/
  └── your_domain/
      ├── __init__.py
      ├── your_command.py          # Command implementation
      ├── your_service.py          # Business logic
      └── .env                     # Domain-specific environment (optional)
```

### Auto-Discovery
Commands are automatically discovered by the `CommandManager` - no manual registration required. Just create the command class inheriting from `BaseCommand`.

## Data Storage

- **DuckDB**: Main analytical database stored in `data/ag_operations.duckdb`
- **Cache**: JSON cache files stored in `cache/` directory
- **Logs**: Rotated log files in `logs/` directory
- **Output**: Generated reports and visualizations in `output/` directory

## Key Dependencies

- **Data Processing**: pandas, duckdb, pyarrow
- **Visualization**: matplotlib, altair
- **JIRA Integration**: Custom JIRA API client
- **LLM Integration**: ollama for AI-powered analysis
- **Web Automation**: selenium for web scraping
- **File Processing**: openpyxl, pdfplumber for document handling

## SonarQube Integration

The SonarQube domain provides comprehensive code quality analysis for Syngenta Digital projects:

### Key Commands
- `python src/main.py syngenta sonarqube sonarqube --operation list-projects --include-measures` - Get quality metrics for all predefined projects
- `python src/main.py syngenta sonarqube sonarqube --operation measures --project-key <project>` - Get metrics for specific project
- `python src/main.py syngenta sonarqube sonarqube --operation issues --project-key <project>` - Get issues for specific project

### Features
- **Predefined Project List**: 27 Syngenta Digital projects in `src/domains/syngenta/sonarqube/projects_list.json`
- **Comprehensive Metrics**: 16 quality metrics including security, reliability, maintainability
- **File Caching**: 1-hour cache expiration for improved performance
- **Batch Operations**: Efficiently fetch metrics for multiple projects
- **Clear Cache Option**: Use `--clear-cache` to force fresh data

### Default Quality Metrics
- Quality Gate status, bugs, vulnerabilities, code smells
- Security rating, security hotspots reviewed, security review rating
- Coverage, duplicated lines, maintainability rating
- Lines of code, language distribution, issue counts by type

## JIRA Integration

Extended JIRA functionality beyond basic issue management:

### Available Commands
- `epic-monitoring` - Monitor epic progress and generate reports
- `issue-adherence` - Analyze issue completion against due dates
- `components` - Manage JIRA project components with batch operations
- `cycle-info` - Display current cycle information
- `fill-missing-dates` - Fill missing dates for JIRA issues
- `list-custom-fields` - List available custom fields

### Features
- **File Caching**: Intelligent caching system for API responses
- **Batch Operations**: Efficient bulk processing of JIRA data
- **CI/CD Integration**: Automated epic monitoring via GitHub Actions

## CI/CD and Automation

### GitHub Actions
- **Epic Monitoring**: Automated workflow runs Monday/Wednesday/Friday at 9:00 AM BRT
- **Location**: `.github/workflows/epic-monitoring.yml`
- **Features**: Comprehensive secret management, failure log uploads
- **Required Secrets**: JIRA authentication, notification endpoints

## Development Environment

### Docker Support
- Team assessment services: `src/domains/syngenta/team_assessment/docker-compose.yml`
- Ag operations services: `src/domains/syngenta/ag_operations/docker-compose.yml`
- Custom Dockerfiles available for containerized development

## Essential Utilities and Patterns

### Logging System (LogManager)

**Singleton Pattern** - Use consistently across all components:

```python
from utils.logging.logging_manager import LogManager

# Standard usage in commands and services
logger = LogManager.get_instance().get_logger("ComponentName")

# With module-specific naming for detailed tracking
logger = LogManager.get_instance().get_logger("MainComponent", "SubModule")

# Log levels with color-coded console output
logger.debug("Debug information")
logger.info("General information") 
logger.warning("Warning message")
logger.error("Error occurred", exc_info=True)  # Include stack trace
```

**Features**: Automatic file rotation (hourly), color-coded console output, module-specific color coding, automatic log file management in `logs/` directory.

### Caching System (CacheManager)

**File-based caching with expiration** - Essential for API operations:

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

### Environment Loading

**ALWAYS call first in command main() methods**:

```python
from utils.env_loader import ensure_env_loaded, ensure_jira_env_loaded

# In command main() method - call before any other operations
ensure_env_loaded()  # General environment loading

# Domain-specific loading when needed
ensure_jira_env_loaded()   # For JIRA commands  
```

**Multi-environment support**: Automatically discovers and loads `.env` files from project root and domain directories.

### Data Management Utilities

#### JSON Manager
**Consistent JSON operations with backup and validation**:

```python
from utils.data.json_manager import JSONManager

# Read with default fallback
data = JSONManager.read_json("file.json", default={})

# Write with automatic backup
JSONManager.write_json(data, "file.json")

# Update existing JSON files
JSONManager.append_or_update_json("file.json", {"new_key": "value"})

# Create formatted JSON strings
json_string = JSONManager.create_json(data, indent=2)
```

#### DuckDB Manager  
**Analytical database operations with schema management**:

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
**Excel file operations with pandas integration**:

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

**Standardized file operations with validation**:

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

**Consistent error handling across commands**:

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

**Standardized JIRA API client**:

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

**For data processing commands that handle multiple files**:

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

## Development Best Practices

1. **Always inherit from BaseCommand** for CLI commands
2. **Call ensure_env_loaded() first** in main() methods  
3. **Use LogManager singleton** for consistent logging across components
4. **Implement caching** for expensive operations (API calls, data processing)
5. **Follow error handling patterns** with proper logging and exit codes
6. **Use data managers** for consistent file operations and validation
7. **Validate inputs** using FileManager and JSONManager utilities  
8. **Create service layers** to separate business logic from CLI parsing
9. **Use singleton patterns** for managers (LogManager, CacheManager)
10. **Structure domains** with command/service separation for maintainability