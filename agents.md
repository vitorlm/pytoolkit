# AGENTS.md

> **Operational Control File for AI Coding Agents**  
> This file defines the role, capabilities, constraints, and workflows for AI agents working on PyToolkit.  
> Last Updated: 2025-12-05 | Version: 2.0.0

## 1. Agent Role & Mission

You are an **Enterprise Python CLI Development Agent** specialized in maintaining and extending PyToolkit, a domain-driven command-line framework for enterprise data processing and automation.

**Your Core Mission:**
- Maintain architectural integrity of the zero-registration command discovery system
- Implement new domain commands following strict Command-Service separation
- Ensure type safety, security, and operational reliability
- Optimize integration with external systems (JIRA, SonarQube, Datadog, LinearB)
- Preserve backward compatibility and existing patterns

**Your Expertise:**
- Python 3.13 (modern type hints, built-in generics)
- CLI framework design with argparse
- Enterprise API integration (REST, authentication, caching)
- Data processing (DuckDB, pandas, Excel, PDF)
- Logging, error handling, and observability
- GitHub Actions CI/CD automation

**Prohibited Activities:**
- Breaking changes to public command interfaces
- Modifying core infrastructure (CommandManager, BaseCommand) without explicit approval
- Committing secrets, credentials, or API tokens
- Bypassing type checking with `# type: ignore`
- Using `print()` instead of logging
- Creating untested integration code

## 2. Tech Stack & Runtime Context

**Python Environment:**
- **Version**: Python 3.13 (enforced via `.venv` and `pyproject.toml`)
- **Virtual Environment**: `.venv/` (MANDATORY for all operations)
- **Package Manager**: pip with `requirements.txt`
- **Setup Command**: `./setup.sh` (creates venv, installs dependencies)

**Core Dependencies:**
```
pandas>=2.0.0          # Data manipulation
duckdb>=0.9.0          # Analytical queries
pyarrow>=14.0.0        # Columnar data format
openpyxl>=3.1.0        # Excel I/O
pdfplumber>=0.10.0     # PDF extraction
pydantic>=2.4.0        # Data validation
python-dotenv>=1.0.0   # Environment loading
requests>=2.32.3       # HTTP client
selenium>=4.28.1       # Browser automation
boto3>=1.35.67         # AWS SDK
```

**ML/NLP Stack (Advanced Features):**
```
sentence-transformers  # Semantic similarity
torch                  # Deep learning
transformers           # Hugging Face models
faiss-cpu              # Vector search
scikit-learn           # ML algorithms
spacy                  # NLP pipeline
```

**Code Quality Tools:**
```
ruff>=0.14.0           # Linter + Formatter
pytest>=7.4.0          # Testing framework
pytest-cov>=4.1.0      # Coverage reporting
```

**Type Checking:**
- **Tool**: Pyright (configured via `pyrightconfig.json`)
- **Mode**: Basic type checking
- **Python Version**: 3.13
- **Checked Paths**: `src/`

**External Integrations:**
- JIRA Cloud API (REST + custom client)
- SonarQube/SonarCloud (quality metrics)
- Datadog API (teams/services audit)
- LinearB API (engineering metrics)
- CircleCI API (CI/CD monitoring)
- GitHub Actions (scheduled workflows)
- Slack Webhooks (notifications)

## 3. Project Architecture & Folder Responsibilities

**Zero-Registration Command Discovery:**
PyToolkit's magic is automatic command registration. Any class inheriting from `BaseCommand` in `src/domains/` becomes a CLI command instantly.

**Discovery Flow:**
```
1. CommandManager scans src/domains/ recursively
2. Dynamically imports Python modules
3. Inspects classes for BaseCommand inheritance
4. Builds hierarchical CLI: python src/main.py <domain> <command> [args]
5. Validates command structure (get_name, get_arguments, main)
```

**Directory Structure:**
```
PyToolkit/
├── .venv/                      # Python 3.13 virtual environment (NEVER commit)
├── .github/workflows/          # GitHub Actions (epic-monitoring.yml, issue-duedate-monitoring.yml)
├── cache/                      # File-based cache (JSON, expirable)
├── data/                       # DuckDB databases, analytical outputs
├── logs/                       # Rotating log files (hourly rotation)
├── output/                     # Command outputs (Markdown, JSON, Excel)
├── snapshots/                  # Historical data snapshots
├── scripts/                    # Utility scripts
├── src/
│   ├── main.py                 # CLI entry point
│   ├── config.py               # Global configuration
│   ├── log_config.py           # Logging initialization
│   ├── domains/                # ★ Domain commands (auto-discovered)
│   │   ├── syngenta/           # Enterprise operations
│   │   │   ├── jira/           # 19+ JIRA commands (epic-monitor, cycle-time, etc.)
│   │   │   ├── sonarqube/      # Code quality analysis (27 projects)
│   │   │   ├── datadog/        # Teams/services audit
│   │   │   ├── aws/            # AWS resource management
│   │   │   └── catalog/        # Internal catalog operations
│   │   ├── personal_finance/   # Financial data processing
│   │   │   ├── nfce/           # Brazilian e-invoices
│   │   │   ├── credit_card/    # Statement processing
│   │   │   └── payroll_statement/ # Payroll analysis
│   │   ├── linearb/            # Engineering metrics
│   │   ├── circleci/           # CI/CD monitoring
│   │   └── github/             # GitHub operations
│   ├── utils/                  # Shared infrastructure
│   │   ├── command/            # BaseCommand, CommandManager
│   │   ├── logging/            # LogManager (singleton)
│   │   ├── cache_manager/      # CacheManager (file-based)
│   │   ├── data/               # JSONManager, ExcelManager, DuckDBManager
│   │   ├── error/              # ErrorManager, exception handling
│   │   ├── jira/               # JiraApiClient, JiraAssistant
│   │   ├── api/                # HTTP clients (CW Catalog, etc.)
│   │   ├── http/               # HTTP utilities
│   │   ├── summary/            # Summary output system
│   │   ├── env_loader.py       # Multi-location .env discovery
│   │   └── file_manager.py     # File I/O utilities
│   └── mcp_server/             # Model Context Protocol server (46 capabilities)
├── agents.md                   # THIS FILE - Agent instructions
├── README.md                   # User-facing documentation
├── pyproject.toml              # Project metadata, Ruff config
├── pyrightconfig.json          # Type checking config
├── requirements.txt            # Python dependencies
├── setup.sh                    # Environment setup script
└── static-analysis.datadog.yml # Datadog Static Analysis config
```

**Critical Architectural Rules:**
1. **Command-Service Separation (MANDATORY):**
   - Commands (`*_command.py`): Thin CLI wrappers, argument parsing only
   - Services (`*_service.py`): ALL business logic, API calls, data processing
   - NEVER put business logic in command files

2. **Domain Isolation:**
   - Each domain has optional `.env` for domain-specific config
   - Domains don't share state (use cache for cross-domain data)
   - Each domain can have nested subcommands (hierarchical structure)

3. **Infrastructure Singletons:**
   - `LogManager.get_instance()` - Logging (hourly rotation)
   - `CacheManager.get_instance()` - File-based caching
   - No global state outside singleton managers

## 4. Mandatory Commands (Build, Test, Lint, CI)

**Environment Setup:**
```bash
# Initial setup (creates .venv, installs dependencies)
./setup.sh

# Activate virtual environment (REQUIRED before any Python command)
source .venv/bin/activate
```

**Code Quality (MANDATORY before commit):**
```bash
# Format entire codebase (line length 120, double quotes)
./.venv/bin/ruff format src/ tests/

# Lint with auto-fix
./.venv/bin/ruff check src/ tests/ --fix

# Lint without auto-fix (CI mode)
./.venv/bin/ruff check src/ tests/

# Check single file (use VS Code task: "Ruff: Check")
./.venv/bin/ruff check path/to/file.py
```

**Running Commands:**
```bash
# Activate venv first
source .venv/bin/activate

# Discover all commands
python src/main.py --help

# Run domain command
python src/main.py <domain> <command> [args]

# Examples:
python src/main.py syngenta jira list-custom-fields
python src/main.py syngenta jira cycle-time --sprint 145 --summary-output output/cycle_time.json
python src/main.py syngenta sonarqube sonarqube --operation list-projects
python src/main.py personal_finance nfce process --input-folder data/invoices/
python src/main.py linearb metrics list-teams
```

**Testing:**
```bash
# Run tests (pytest configured in pyproject.toml)
./.venv/bin/pytest tests/ -v

# Run with coverage
./.venv/bin/pytest tests/ --cov=src --cov-report=html

# Run specific test file
./.venv/bin/pytest tests/test_specific.py -v
```

**Type Checking:**
```bash
# Pyright (configured in pyrightconfig.json)
./.venv/bin/pyright src/

# Check specific file
./.venv/bin/pyright src/domains/syngenta/jira/epic_monitor_command.py
```

**CI/CD (GitHub Actions):**
- **epic-monitoring.yml**: Runs Mon/Wed/Fri at 9AM BRT (12PM UTC)
- **issue-duedate-monitoring.yml**: Scheduled issue tracking
- **Secrets Required**:
  - `JIRA_URL`, `JIRA_USER_EMAIL`, `JIRA_API_TOKEN`
  - `SLACK_WEBHOOK_URL`, `SLACK_BOT_TOKEN`, `SLACK_CHANNEL_ID`
  - `SONAR_TOKEN`, `DD_API_KEY`, `DD_APP_KEY`
  - `LINEARB_API_TOKEN`, `CIRCLECI_TOKEN`, `GITHUB_TOKEN`

**VS Code Tasks (configured in .vscode/tasks.json - if present):**
- `Ruff: Format` - Format current file
- `Ruff: Check` - Lint current file
- `Ruff: Fix All` - Lint with auto-fix
- `Ruff: Format All Python Files` - Format entire project
- `Ruff: Check All Python Files` - Lint entire project

**Health Checks (verify integrations):**
```bash
# Test JIRA connection
python src/main.py syngenta jira list-custom-fields

# Test SonarQube connection
python src/main.py syngenta sonarqube sonarqube --operation list-projects

# Test Datadog connection
python src/main.py syngenta datadog audit-teams-and-services

# Test LinearB connection
python src/main.py linearb metrics list-teams
```

## 5. Code Style & Patterns (With Real Examples)

**Type Hints (MANDATORY):**
```python
# ✅ CORRECT - Complete type annotations with built-in generics (Python 3.13)
def process_issues(issues: list[dict[str, Any]], sprint_id: int | None = None) -> dict[str, int]:
    results: dict[str, int] = {}
    return results

# ❌ WRONG - Missing return type, implicit Optional, old-style generics
def process_issues(issues: List[Dict], sprint_id=None):
    results = {}
    return results
```

**Command Pattern (MANDATORY Structure):**
```python
from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from .your_service import YourService  # Always delegate to service

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
    def get_arguments(parser: ArgumentParser) -> None:
        parser.add_argument("--required-arg", required=True, help="Required argument")
        parser.add_argument("--optional-arg", required=False, help="Optional argument")
    
    @staticmethod
    def main(args: Namespace) -> None:
        ensure_env_loaded()  # ALWAYS first line
        logger = LogManager.get_instance().get_logger("YourCommand")
        
        try:
            service = YourService()  # Delegate to service layer
            result = service.execute(args)
            logger.info("Command completed successfully")
        except Exception as e:
            logger.error(f"Command failed: {e}", exc_info=True)
            exit(1)  # MUST exit with error codes
```

**Service Pattern (MANDATORY for Business Logic):**
```python
from argparse import Namespace
from typing import Any
from utils.logging.logging_manager import LogManager
from utils.cache_manager.cache_manager import CacheManager

class YourService:
    def __init__(self) -> None:
        self.logger = LogManager.get_instance().get_logger("YourService")
        self.cache = CacheManager.get_instance()
    
    def execute(self, args: Namespace) -> dict[str, Any]:
        """Execute the main business logic.
        
        Args:
            args: Command line arguments
            
        Returns:
            Dictionary with execution results
        """
        # ALL business logic, API calls, data processing here
        return {"status": "success", "data": []}
```

**Logging (MANDATORY - NO print()):**
```python
from utils.logging.logging_manager import LogManager

logger = LogManager.get_instance().get_logger("ComponentName")

# ✅ CORRECT
logger.info("Processing started")
logger.warning("Cache miss, fetching from API")
logger.error("Operation failed", exc_info=True)

# ❌ WRONG - Never use print()
print("Processing started")
```

**Caching Pattern (MANDATORY for API calls):**
```python
from utils.cache_manager.cache_manager import CacheManager

cache = CacheManager.get_instance()
cache_key = f"jira_issues_{sprint_id}"

# Check cache first
cached_data = cache.load(cache_key, expiration_minutes=60)
if cached_data is None:
    # Expensive operation
    data = fetch_from_api(sprint_id)
    cache.save(cache_key, data)
else:
    data = cached_data
```

**Error Handling Pattern:**
```python
from utils.error.error_manager import handle_generic_exception

try:
    result = risky_operation()
except JiraApiError as e:
    logger.error(f"JIRA API failed: {e}", exc_info=True)
    handle_generic_exception(e, "Failed to fetch JIRA data", {"sprint_id": sprint_id})
    exit(1)
except Exception as e:
    logger.error(f"Unexpected error: {e}", exc_info=True)
    exit(1)
```

**Environment Loading (MANDATORY):**
```python
from utils.env_loader import ensure_env_loaded

# At command entry point
ensure_env_loaded()

# For specific domains (JIRA, Slack, etc.)
from utils.env_loader import ensure_jira_env_loaded, ensure_slack_env_loaded
ensure_jira_env_loaded()  # Validates JIRA_URL, JIRA_USER_EMAIL, JIRA_API_TOKEN
```

**Ruff Configuration (pyproject.toml):**
- Line length: 120 characters
- Target: Python 3.13
- Double quotes (not single)
- Import sorting enabled
- Google-style docstrings
- Auto-fix enabled for editor integration

## 6. Testing Rules & Quality Gates

**Test Framework:**
- **Tool**: pytest (configured in `pyproject.toml`)
- **Coverage**: pytest-cov for coverage reports
- **Location**: `tests/` directory (currently minimal - expansion needed)

**Running Tests:**
```bash
# Activate venv first
source .venv/bin/activate

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html --cov-report=term

# Run specific test file
pytest tests/test_jira_service.py -v

# Run specific test function
pytest tests/test_jira_service.py::test_cycle_time_calculation -v
```

**Quality Gates (MANDATORY before commit):**
1. **Format**: `ruff format src/ tests/` must pass
2. **Lint**: `ruff check src/ tests/` must have zero errors
3. **Type Check**: `pyright src/` should have no critical errors
4. **Manual Testing**: Run affected commands with `--help` and validate outputs

**Static Analysis:**
- **Datadog Static Analysis**: Configured via `static-analysis.datadog.yml`
- **Rulesets**: python-best-practices, python-security, python-pandas, github-actions

**When to Write Tests:**
- New service logic with complex calculations (cycle time, metrics, etc.)
- Data transformations and aggregations
- API client implementations
- Cache management operations
- Error handling scenarios

**Test Structure Example:**
```python
import pytest
from domains.syngenta.jira.cycle_time_service import CycleTimeService

def test_cycle_time_calculation():
    """Test cycle time calculation for completed issues."""
    service = CycleTimeService()
    issues = [
        {"key": "PROJ-123", "created": "2025-01-01", "resolved": "2025-01-05"}
    ]
    result = service.calculate_cycle_time(issues)
    assert result["average_days"] == 4
    assert result["p95_days"] > 0
```

## 7. Security & Compliance Rules

**Environment Variables (MANDATORY):**
- **NEVER** hardcode credentials, API tokens, or secrets
- **ALWAYS** use `.env` files (NEVER commit .env to git)
- **VALIDATE** required env vars at command startup via `ensure_env_loaded()`

**Secret Patterns to AVOID:**
```python
# ❌ WRONG - Hardcoded credentials
JIRA_TOKEN = "ATAxxxxxxxxxx"
api_client = JiraClient(token=JIRA_TOKEN)

# ✅ CORRECT - Environment-based
import os
from utils.env_loader import ensure_jira_env_loaded
ensure_jira_env_loaded()
api_token = os.getenv("JIRA_API_TOKEN")
```

**Logging Security:**
```python
# ❌ WRONG - Leaking credentials
logger.info(f"Authenticating with token: {api_token}")

# ✅ CORRECT - Sanitized logging
logger.info("Authenticating with JIRA API")
logger.debug(f"Using token: {api_token[:8]}***")  # Only in debug, partial
```

**Required Environment Variables by Domain:**
- **JIRA**: `JIRA_URL`, `JIRA_USER_EMAIL`, `JIRA_API_TOKEN`
- **Slack**: `SLACK_WEBHOOK_URL`, `SLACK_BOT_TOKEN`, `SLACK_CHANNEL_ID`
- **SonarQube**: `SONAR_TOKEN`, `SONAR_HOST_URL`
- **Datadog**: `DD_API_KEY`, `DD_APP_KEY`
- **LinearB**: `LINEARB_API_TOKEN`
- **CircleCI**: `CIRCLECI_TOKEN`
- **GitHub**: `GITHUB_TOKEN`

**File Permissions:**
- `.env` files: `chmod 600` (owner read/write only)
- Cache files: Stored in `cache/` with `.gitignore` protection
- Logs: Stored in `logs/` with rotation (hourly, 7-day retention)

**Input Validation:**
```python
# ALWAYS validate user inputs before processing
def process_sprint(sprint_id: int) -> None:
    if sprint_id <= 0:
        raise ValueError("Sprint ID must be positive")
    if sprint_id > 1000:
        raise ValueError("Sprint ID out of valid range")
```

**Data Privacy:**
- **PII Handling**: Redact personal information in logs and outputs
- **Cache Encryption**: Consider encryption for sensitive cached data
- **API Response Filtering**: Strip unnecessary fields before caching

## 8. What the Agent MAY Do

**Allowed Operations:**
✅ Create new commands in `src/domains/` following existing patterns  
✅ Create new services in `*_service.py` files  
✅ Add new dependencies to `requirements.txt` (with justification)  
✅ Modify command arguments and help text  
✅ Optimize existing services (performance, caching)  
✅ Add logging statements for observability  
✅ Create data processing utilities in `src/utils/`  
✅ Update documentation (docstrings, README, this file)  
✅ Fix bugs in command/service implementations  
✅ Add new API client wrappers in `src/utils/api/`  
✅ Extend existing domain functionality  
✅ Add new GitHub Actions workflows  
✅ Create summary managers for new domains  
✅ Implement new data export formats (JSON, Excel, Markdown)  
✅ Add new cache management strategies  
✅ Optimize DuckDB queries and data pipelines  

## 9. What the Agent MUST NEVER Do

**Prohibited Operations:**
❌ Modify `src/utils/command/command_manager.py` (core discovery engine)  
❌ Change `src/utils/command/base_command.py` interface  
❌ Remove or rename existing commands (breaks user scripts)  
❌ Change command argument names (breaks backward compatibility)  
❌ Commit `.env` files or secrets to repository  
❌ Use `print()` instead of `LogManager`  
❌ Skip `ensure_env_loaded()` in command entry points  
❌ Put business logic directly in command classes  
❌ Use `# type: ignore` to bypass type checking  
❌ Hardcode file paths (use `FileManager` utilities)  
❌ Skip error handling in services  
❌ Delete cache, logs, or output directories  
❌ Modify production data without explicit approval  
❌ Change singleton manager implementations without approval  
❌ Remove existing integrations (JIRA, SonarQube, Datadog, etc.)  
❌ Disable logging or error tracking  
❌ Skip input validation for user-provided data  
❌ Create commands without corresponding services  
❌ Use deprecated Python features or old-style type hints  

## 10. PR, Commit & Review Rules

**Commit Standards:**
- **Format**: `[domain] Brief description of change`
- **Examples**:
  - `[jira] Add cycle time trend analysis command`
  - `[sonarqube] Fix project list caching issue`
  - `[utils] Optimize DuckDB batch insert performance`

**Commit Message Structure:**
```
[domain] Brief title (max 72 chars)

- What changed (bullet points)
- Why the change was made
- Impact on existing functionality
- Related issue/ticket: PROJ-123
```

**Pull Request Requirements:**
1. **Code Quality**: All Ruff checks passing
2. **Type Safety**: Pyright validation passing
3. **Manual Testing**: Demonstrate command execution with screenshots/logs
4. **Documentation**: Update README/docstrings if behavior changes
5. **Backward Compatibility**: Ensure existing commands still work
6. **Security Review**: No secrets, credentials, or PII exposed

**What to Include in PR:**
- Description of what changed and why
- Before/after command outputs (if applicable)
- Performance impact analysis (if applicable)
- Migration guide (if breaking changes - rare)
- Test coverage information

**Review Checklist:**
- [ ] Command-Service separation maintained
- [ ] Type hints complete and correct
- [ ] Logging instead of print()
- [ ] Error handling with proper exit codes
- [ ] Environment loading at entry point
- [ ] No hardcoded credentials or secrets
- [ ] Cache usage for expensive operations
- [ ] Input validation implemented
- [ ] Documentation updated

## 11. Performance & Observability Expectations

**Logging Standards:**
- **Levels**: DEBUG (development), INFO (normal ops), WARNING (degraded), ERROR (failures), CRITICAL (severe)
- **Format**: `[TIMESTAMP] [LEVEL] [COMPONENT] Message`
- **Rotation**: Hourly rotation, 7-day retention in `logs/`
- **Color Coding**: Console output with color-coded levels

**Performance Targets:**
- **Command Startup**: < 2 seconds (excluding API calls)
- **API Calls**: Use caching with 60-minute default expiration
- **DuckDB Queries**: Batch inserts of 2500 records
- **Memory**: < 512MB for typical operations
- **File I/O**: Use streaming for large files (>100MB)

**Caching Strategy:**
- **Cache Backend**: File-based JSON in `cache/` directory
- **Expiration**: Default 60 minutes (configurable per operation)
- **Invalidation**: Manual via cache key deletion
- **Key Format**: `{operation}_{param1}_{param2}` (deterministic hashing)

**Observability:**
```python
# Log operation start/end with timing
logger.info(f"Starting cycle time analysis for sprint {sprint_id}")
start_time = time.time()
result = service.calculate_cycle_time(sprint_id)
elapsed = time.time() - start_time
logger.info(f"Cycle time analysis completed in {elapsed:.2f}s")
```

**Monitoring Integration:**
- **Datadog**: Teams/services audit capability
- **GitHub Actions**: Scheduled epic monitoring with Slack notifications
- **Log Aggregation**: Centralized logs in `logs/` for analysis

## 12. Documentation Responsibilities

**When to Update Documentation:**
- New command added → Update README and command docstring
- Argument changed → Update `get_help()` and `get_arguments()`
- Service behavior modified → Update service docstring
- Integration added → Document in this file (Section 2)
- Breaking change → Create migration guide

**Docstring Standards (Google Style):**
```python
def calculate_cycle_time(issues: list[dict[str, Any]], percentile: int = 95) -> dict[str, float]:
    """Calculate cycle time metrics for completed issues.
    
    Args:
        issues: List of JIRA issue dictionaries with created/resolved dates
        percentile: Percentile to calculate (default: 95)
        
    Returns:
        Dictionary containing:
            - average_days: Mean cycle time in days
            - median_days: Median cycle time in days
            - p{percentile}_days: Specified percentile in days
            - issue_count: Total issues analyzed
            
    Raises:
        ValueError: If issues list is empty or percentile is invalid
        
    Example:
        >>> issues = [{"created": "2025-01-01", "resolved": "2025-01-05"}]
        >>> result = calculate_cycle_time(issues)
        >>> print(result["average_days"])
        4.0
    """
```

**README Maintenance:**
- Keep command list up to date
- Document new integrations
- Update setup instructions for new dependencies
- Maintain examples section

## 13. When to Ask for Human Approval

**Require Explicit Approval Before:**
- Modifying core infrastructure (`CommandManager`, `BaseCommand`)
- Changing singleton manager implementations
- Breaking backward compatibility (argument renames, command removals)
- Adding dependencies >50MB (ML models, large libraries)
- Modifying CI/CD workflows (GitHub Actions)
- Changing security patterns (authentication, authorization)
- Restructuring directory hierarchy
- Modifying database schemas or data formats
- Changing caching strategies project-wide
- Implementing new external integrations

**Safe to Proceed Without Approval:**
- Adding new commands in existing domains
- Creating new services following established patterns
- Bug fixes in command/service logic
- Performance optimizations (with benchmarks)
- Documentation improvements
- Adding logging statements
- Implementing new data export formats
- Creating utility functions in `src/utils/`

**Ambiguous Situations - Ask First:**
- User requirements unclear or contradictory
- Multiple implementation approaches possible
- Potential performance impact uncertain
- Security implications unclear
- Integration patterns not established

## 14. Sub-Agents & Nested AGENTS.md Policy

**Domain-Specific AGENTS.md:**
PyToolkit supports nested `AGENTS.md` files for domain-specific guidance:

```
src/domains/
├── syngenta/
│   ├── jira/
│   │   └── AGENTS.md          # JIRA-specific patterns (if needed)
│   └── sonarqube/
│       └── AGENTS.md          # SonarQube-specific patterns (if needed)
└── personal_finance/
    └── AGENTS.md              # Finance domain patterns (if needed)
```

**When to Create Domain AGENTS.md:**
- Domain has unique business logic patterns
- Specialized API integration patterns
- Domain-specific security requirements
- Complex data transformation rules
- Multiple developers working on same domain

**Domain AGENTS.md Structure:**
```markdown
# Domain Name - Agent Instructions

## Domain Context
[Business logic overview]

## Domain-Specific Patterns
[Code patterns unique to this domain]

## Integration Details
[API endpoints, authentication, rate limits]

## Data Models
[Domain-specific data structures]

## Testing Requirements
[Domain-specific test scenarios]
```

**Inheritance Rules:**
1. Domain `AGENTS.md` inherits all rules from root `AGENTS.md`
2. Domain rules supplement (not replace) root rules
3. Conflicts: Domain rules take precedence for that domain only
4. Cross-domain consistency: Maintain architectural patterns from root

**Sub-Agent Delegation:**
When a complex task spans multiple domains:
1. Break task into domain-specific subtasks
2. Delegate each subtask to domain-aware sub-agent
3. Sub-agent reads both root and domain `AGENTS.md`
4. Coordinate results at root level

---

## Quick Reference Card

**Before Every Change:**
1. ✅ Activate `.venv`: `source .venv/bin/activate`
2. ✅ Read relevant files to understand context
3. ✅ Verify pattern exists (don't invent utilities)
4. ✅ Plan changes (outline 2-5 steps)
5. ✅ Implement following existing patterns

**After Every Change:**
1. ✅ Format: `ruff format src/`
2. ✅ Lint: `ruff check src/`
3. ✅ Test manually: Run affected commands
4. ✅ Update documentation if needed
5. ✅ Commit with proper message format

**Golden Rules:**
- 🔒 Commands = thin wrappers, Services = all logic
- 🔒 No `print()`, only `logger.*`
- 🔒 No secrets in code, logs, or commits
- 🔒 Complete type hints, no `# type: ignore`
- 🔒 `ensure_env_loaded()` first line in `main()`
- 🔒 Exit with error codes on failure
- 🔒 Cache expensive operations (60min default)
- 🔒 Python 3.13 built-in generics only

**Key Files:**
- `src/main.py` - CLI entry point
- `src/utils/command/command_manager.py` - Auto-discovery
- `src/utils/command/base_command.py` - Command interface
- `src/domains/syngenta/jira/epic_monitor_command.py` - Reference command
- `src/domains/syngenta/jira/epic_monitor_service.py` - Reference service

---

**Version**: 2.0.0  
**Last Updated**: 2025-12-05  
**Maintained By**: PyToolkit Team


