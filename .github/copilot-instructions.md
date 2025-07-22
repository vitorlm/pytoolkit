# GitHub Copilot Instructions for PyToolkit

## Project Overview

PyToolkit is a domain-driven CLI framework that dynamically discovers and loads commands from domain-specific modules. It's designed for enterprise data processing, analytics, and automation across multiple business domains (personal finance, Syngenta operations, JIRA management, SonarQube analysis, etc.).

## Critical Architecture Understanding

### Auto-Discovery System (The "Magic" Behind Command Loading)
The framework's core innovation is its **zero-registration command discovery**:

1. **CommandManager** (`src/utils/command/command_manager.py`) scans `src/domains/` recursively
2. Uses Python's `pkgutil.iter_modules()` and `importlib` to dynamically import modules
3. Inspects each module with `inspect.getmembers()` to find `BaseCommand` subclasses
4. Builds a hierarchical command structure that maps to CLI argument parsing
5. **Entry Point**: `src/main.py` instantiates CommandManager → loads commands → builds parser → executes

This means: **Just create a class inheriting from BaseCommand in any subdirectory of `src/domains/` and it becomes available in the CLI automatically.**

### Singleton Infrastructure Pattern
Three critical singletons manage the entire application lifecycle:
- **LogManager**: Centralized logging with color-coded output, file rotation, hierarchical loggers
- **CacheManager**: File-based caching with expiration (crucial for API operations)
- **Configuration**: Environment loading through `src/config.py` and `src/log_config.py`

These are initialized once at startup and accessed throughout the application via `.get_instance()`.

### Domain-Service-Command Separation (MANDATORY PATTERN)
```
Command (CLI interface) → Service (business logic) → Data Layer (managers/APIs)
```
**Commands** MUST be thin wrappers. **Services** contain all business logic. This enables testing, reusability, and clean separation of concerns.

## Essential Command Structure (STRICT PATTERN)

Every command MUST follow this exact pattern. The CommandManager validates these abstract methods:

```python
from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager

class YourCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "command-name"  # kebab-case, will be CLI subcommand
    
    @staticmethod
    def get_description() -> str:
        return "Brief description"
    
    @staticmethod
    def get_help() -> str:
        return "Detailed help with examples"
    
    @staticmethod
    def get_arguments(parser: ArgumentParser):
        # CLI argument definitions
        parser.add_argument("--arg", required=True, help="Description")
    
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

## Critical Developer Workflows

### Environment Setup & Activation
```bash
# Initial setup (creates .venv and installs dependencies)
./setup.sh

# Daily activation (always run before development)
source .venv/bin/activate

# Code quality pipeline (run before every commit)
black src/        # Code formatting (MANDATORY)
isort src/        # Import sorting (MANDATORY) 
flake8 src/       # Linting (MUST pass)
```

### CLI Usage Patterns
```bash
# Discovery - see all available domains and commands
python src/main.py --help

# Execution pattern
python src/main.py <domain> <command> [args]

# Real examples from the codebase
python src/main.py syngenta jira epic-monitoring --project-key "PROJ"
python src/main.py syngenta sonarqube sonarqube --operation list-projects
python src/main.py syngenta jira issue-adherence --time-period "last-week"
```

### Time Range Patterns (Used across JIRA commands)
The framework has standardized time period handling:
- `last-week`, `last-2-weeks`, `last-month`
- `N-days` format (e.g., `30-days`)
- Date ranges: `YYYY-MM-DD,YYYY-MM-DD`

This pattern should be reused for any new time-based commands.

## Domain Organization & New Domain Creation

### Current Domain Structure
```
src/domains/
├── personal_finance/           # Financial data processing
├── syngenta/                  # Syngenta enterprise operations
│   ├── ag_operations/         # Agricultural data management
│   ├── jira/                  # JIRA integration (8+ commands)
│   ├── sonarqube/             # Code quality analysis  
│   └── team_assessment/       # Performance analysis
```

### Adding New Domains (4-Step Process)
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

**Zero registration required** - CommandManager auto-discovers new commands.

### Command-Service Separation (MANDATORY)
```python
# your_command.py - Thin CLI wrapper
class YourCommand(BaseCommand):
    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("YourCommand")
        
        try:
            service = YourService()
            service.execute(args)
        except Exception as e:
            logger.error(f"Command failed: {e}")
            exit(1)

# your_service.py - ALL business logic here
class YourService:
    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("YourService")
        self.cache = CacheManager.get_instance()
    
    def execute(self, args: Namespace) -> Any:
        # Complex business logic, API calls, data processing
        return self._process_business_logic(args)
```

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
4. ✅ **Keep command files thin** - delegate logic to service classes
5. ✅ Create separate service file for business logic
6. ✅ Implement proper error handling with exit codes
7. ✅ Add caching for expensive operations
8. ✅ Validate inputs and file paths
9. ✅ Follow naming conventions (kebab-case for command names)
10. ✅ Add comprehensive help text with examples

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
- **Keep command files thin** - delegate business logic to service classes
- Create separate service files for all business logic
- Implement proper error handling with logging and exit codes
- Cache expensive operations (API calls, file processing)
- Use type hints and validate inputs
- Follow the BaseCommand pattern exactly
- Use domain-specific environment files

### ❌ DON'T
- Create commands without inheriting from BaseCommand
- **Put business logic directly in command files** - use service classes instead
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
├── your_command.py          # Command implementation (thin layer)
├── your_service.py          # Business logic (MANDATORY for complex logic)
├── .env                     # Domain-specific config (optional)
└── data/                    # Domain-specific data (optional)
```

### Command-Service Pattern Example
```python
# your_command.py - Keep this file minimal
class YourCommand(BaseCommand):
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

# your_service.py - All business logic goes here
class YourService:
    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("YourService")
        self.cache = CacheManager.get_instance()
    
    def execute(self, args: Namespace) -> Any:
        # Complex business logic, API calls, data processing
        return self._process_business_logic(args)
```

## Additional Context

This is a production-grade enterprise CLI framework with sophisticated logging, caching, and error handling. When creating new commands or domains, prioritize reliability, performance, and maintainability. Always consider the multi-user, multi-environment nature of the codebase and implement appropriate abstractions for different business domains.

The framework is designed to handle complex data processing workflows across different business contexts while maintaining consistency in command structure, error handling, and logging patterns.