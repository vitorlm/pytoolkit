# GitHub Copilot Instructions for PyToolkit

## Project Overview

PyToolkit is a domain-driven CLI framework built in Python that dynamically discovers and loads commands from domain-specific modules. It's designed for enterprise-level data processing, analytics, and automation across multiple business domains (personal finance, Syngenta operations, JIRA management, SonarQube analysis, etc.).

## Architecture & Design Patterns

### Core Architecture
- **Domain-Driven Design**: Commands organized by business domains under `src/domains/`
- **Command Pattern**: All commands inherit from `BaseCommand` (`src/utils/command/base_command.py`)
- **Auto-Discovery**: Commands are automatically discovered via `CommandManager` - no manual registration
- **Singleton Pattern**: Used for managers (LogManager, CacheManager) for centralized resource management

### Command System Structure
```python
# Standard command pattern - ALL commands must follow this structure
class YourCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "command-name"
    
    @staticmethod
    def get_description() -> str:
        return "Brief description"
    
    @staticmethod
    def get_help() -> str:
        return "Detailed help with examples"
    
    @staticmethod
    def get_arguments(parser: ArgumentParser):
        # Add command-specific arguments
        parser.add_argument("--arg", required=True, help="Description")
    
    @staticmethod
    def main(args: Namespace):
        # ALWAYS start with this
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("YourCommand")
        
        try:
            # Command logic here
            logger.info("Command completed successfully")
        except Exception as e:
            logger.error(f"Command failed: {e}")
            exit(1)
```

## Domain Structure

### Existing Domains
- **personal_finance/**: Financial data processing (credit cards, payroll)
- **syngenta/**: Syngenta-specific operations with subdomains:
  - **ag_operations/**: Agricultural operations data management
  - **jira/**: JIRA integration and issue management
  - **sonarqube/**: SonarQube/SonarCloud code quality analysis
  - **team_assessment/**: Team performance and competency analysis

### Adding New Domains
1. Create directory: `src/domains/new_domain/`
2. Add `__init__.py` file
3. Create command files inheriting from `BaseCommand`
4. Optional: Add domain-specific `.env` file for configuration

## Essential Utilities & Required Usage Patterns

### Environment Loading (MANDATORY)
```python
from utils.env_loader import ensure_env_loaded

# ALWAYS call this first in main() method
ensure_env_loaded()  # General environment loading
```

### Logging System (MANDATORY)
```python
from utils.logging.logging_manager import LogManager

# Use singleton pattern - get logger with component name
logger = LogManager.get_instance().get_logger("ComponentName")

# For submodules, use hierarchical naming
logger = LogManager.get_instance().get_logger("MainComponent", "SubModule")

# Standard usage
logger.info("General information")
logger.error("Error occurred", exc_info=True)  # Include stack trace for errors
```

### Caching System (RECOMMENDED for API operations)
```python
from utils.cache_manager.cache_manager import CacheManager

cache = CacheManager.get_instance()
cache_key = f"operation_{param1}_{param2}"
cached_data = cache.load(cache_key, expiration_minutes=60)

if cached_data is None:
    fresh_data = expensive_operation()
    cache.save(cache_key, fresh_data)
    data = fresh_data
else:
    logger.info("Using cached data")
    data = cached_data
```

### Data Management
```python
# JSON operations with automatic backup
from utils.data.json_manager import JSONManager
data = JSONManager.read_json("file.json", default={})
JSONManager.write_json(data, "file.json")

# DuckDB for analytical operations
from utils.data.duckdb_manager import DuckDBManager
db_manager = DuckDBManager()
db_manager.add_connection_config({"name": "main_db", "path": "data/database.duckdb"})
conn = db_manager.get_connection("main_db")

# Excel operations
from utils.data.excel_manager import ExcelManager
excel_file = ExcelManager.read_excel("file.xlsx")
ExcelManager.write_excel(dataframe, "output.xlsx")
```

### File Management
```python
from utils.file_manager import FileManager

# Always validate files and directories
files = FileManager.list_files("directory", extension=".json")
FileManager.validate_file("file.pdf", allowed_extensions=[".pdf", ".doc"])
FileManager.create_folder("new_folder")

# Generate standardized filenames
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
    handle_generic_exception(e, "Context description", {"key": "metadata"})
    exit(1)  # Always exit with error code for CLI commands
```

## Environment Configuration

### Multi-Environment Support
- Main `.env` file in project root
- Domain-specific `.env` files in domain directories:
  - `src/domains/syngenta/jira/.env` - JIRA configuration
  - `src/domains/syngenta/sonarqube/.env` - SonarQube configuration
- Environment loader automatically discovers and loads all relevant files

### Configuration Access
```python
import os
from utils.env_loader import ensure_env_loaded

ensure_env_loaded()
api_token = os.getenv("JIRA_API_TOKEN")
base_url = os.getenv("JIRA_BASE_URL")
```

## Data Storage Patterns

### Standard Directories
- **data/**: DuckDB databases and analytical data
- **cache/**: JSON cache files with expiration
- **logs/**: Rotated log files (hourly rotation)
- **output/**: Generated reports and visualizations

### Database Integration
- **Primary**: DuckDB for analytical operations (`data/ag_operations.duckdb`)
- **Caching**: File-based JSON caching with expiration
- **External**: JIRA, SonarQube API integrations

## Key Dependencies & Integrations

### Core Dependencies
- **Data Processing**: pandas, duckdb, pyarrow
- **Visualization**: matplotlib, altair
- **CLI Framework**: argparse (built-in)
- **Logging**: Custom LogManager with color-coded output

### External Integrations
- **JIRA**: Custom API client (`utils.jira.jira_api_client`)
- **SonarQube**: Project metrics and code quality analysis
- **LLM**: ollama for AI-powered analysis
- **Web Automation**: selenium for web scraping

## Development Standards

### Code Quality Requirements
- **Linting**: `flake8 src/` - MUST pass before commits
- **Formatting**: `black src/` - MUST be applied
- **Import Sorting**: `isort src/` - MUST be organized
- **Type Hints**: Use Python 3.12+ typing throughout

### Testing & Validation
- Check for existing test frameworks in the codebase
- Always validate inputs using FileManager utilities
- Use proper error handling patterns with logging

### Command Development Checklist
1. ✅ Inherit from `BaseCommand`
2. ✅ Call `ensure_env_loaded()` first in main()
3. ✅ Use `LogManager.get_instance().get_logger()`
4. ✅ Implement proper error handling with exit codes
5. ✅ Add caching for expensive operations
6. ✅ Validate inputs and file paths
7. ✅ Follow naming conventions (kebab-case for command names)
8. ✅ Add comprehensive help text with examples

## Specialized Domain Knowledge

### JIRA Integration
- **Available Commands**: epic-monitoring, issue-adherence, components, cycle-info, fill-missing-dates, list-custom-fields, issue-resolution-time, issues-creation-analysis
- **Features**: File caching, batch operations, GitHub Actions integration
- **Authentication**: Uses JIRA API tokens stored in environment

### SonarQube Integration
- **Key Features**: 27 predefined Syngenta Digital projects, 16 quality metrics
- **Caching**: 1-hour expiration for performance optimization
- **Metrics**: Quality gates, security ratings, coverage, maintainability
- **Projects List**: `src/domains/syngenta/sonarqube/projects_list.json`

### CI/CD Integration
- **GitHub Actions**: Automated epic monitoring (Mon/Wed/Fri at 9:00 AM BRT)
- **Secrets Management**: Comprehensive secret handling for JIRA/SonarQube
- **Failure Handling**: Automated log uploads on failures

## Performance & Optimization

### Caching Strategy
- **API Responses**: Always cache expensive API calls (JIRA, SonarQube)
- **File Operations**: Use JSONManager for consistent file handling
- **Database**: Use DuckDB for analytical queries with proper indexing
- **Expiration**: Set appropriate cache expiration (1 hour for most operations)

### Memory Management
- **Batch Processing**: Use appropriate batch sizes (2500 records for DuckDB)
- **File Streaming**: Use streaming for large file operations
- **Connection Pooling**: Reuse database connections where possible

## CLI Usage Examples

### Basic Command Structure
```bash
# Show available domains and commands
python src/main.py --help

# Execute specific command
python src/main.py <domain> <command> [args]

# Examples
python src/main.py syngenta jira epic-monitoring --project-key "PROJECT"
python src/main.py syngenta sonarqube sonarqube --operation list-projects
```

### Environment Setup
```bash
# Setup virtual environment
./setup.sh

# Activate environment
source .venv/bin/activate

# Alternative with Poetry
poetry install
poetry shell
```

## Common Patterns & Anti-Patterns

### ✅ DO
- Always use singleton managers (LogManager, CacheManager)
- Implement proper error handling with logging and exit codes
- Cache expensive operations (API calls, file processing)
- Use type hints and validate inputs
- Follow the BaseCommand pattern exactly
- Use domain-specific environment files

### ❌ DON'T
- Create commands without inheriting from BaseCommand
- Skip environment loading in main() methods
- Ignore error handling or logging
- Hardcode configuration values
- Skip input validation
- Use print() instead of logging
- Create commands without proper argument parsing

## File Structure Template

```
src/domains/your_domain/
├── __init__.py
├── your_command.py          # Command implementation
├── your_service.py          # Business logic (optional)
├── .env                     # Domain-specific config (optional)
└── data/                    # Domain-specific data (optional)
```

## Additional Context

This is a production-grade enterprise CLI framework with sophisticated logging, caching, and error handling. When creating new commands or domains, prioritize reliability, performance, and maintainability. Always consider the multi-user, multi-environment nature of the codebase and implement appropriate abstractions for different business domains.

The framework is designed to handle complex data processing workflows across different business contexts while maintaining consistency in command structure, error handling, and logging patterns.