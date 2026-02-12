# PyToolkit Installation Guide

This guide covers installation options and dependency management for PyToolkit.

## Quick Start

```bash
# Clone the repository
git clone <repository-url>
cd pytoolkit

# Install base dependencies
pip install -e .

# Verify installation
python src/main.py --help
```

## Installation Options

PyToolkit supports modular dependency installation using Python's standard `extras` mechanism.

### Base Installation (Core)

Install only the core dependencies required for basic CLI functionality:

```bash
pip install -e .
```

**Includes:** pandas, duckdb, pyarrow, openpyxl, pdfplumber, pydantic, requests, beautifulsoup4, boto3, datadog-api-client, slack-sdk, matplotlib, altair, selenium

**Use case:** Production CI/CD, basic domain commands

### ML/NLP Features

Install machine learning and NLP dependencies for advanced similarity and classification features:

```bash
pip install -e ".[ml]"
```

**Includes:** scikit-learn, scipy, sentence-transformers, torch, transformers

**Features enabled:**
- Supervised similarity training
- Embedding-based similarity
- Advanced ML classifiers

### LLM Providers

Install dependencies for various LLM provider integrations:

```bash
pip install -e ".[llm]"
```

**Includes:** portkey-ai, openai, google-genai, zai-sdk

**Features enabled:**
- LLM-based issue classification
- Component classifier
- Text generation features

### Development Setup

Install all dependencies including development tools:

```bash
pip install -e ".[ml,llm,dev]"
```

**Includes:** All extras plus pytest, pytest-asyncio, pytest-cov, pytest-mock, ruff

## Optional Dependencies Explained

Some PyToolkit features depend on packages that are not required for core functionality. This keeps the base installation lightweight and faster to install.

### How It Works

When you try to use a feature that requires optional dependencies, you'll get a helpful error message:

```
OptionalDependencyError: Optional dependency 'sklearn' is not installed.

Install with: pip install -e '.[ml]'

Documentation: https://github.com/user/PyToolkit#optional-dependencies
```

This is better than a cryptic `ModuleNotFoundError` because:
1. It tells you exactly what's missing
2. It shows the installation command
3. It links to documentation

### Available Extras

| Extra | Description | Size Impact |
|-------|-------------|-------------|
| (none) | Core dependencies only | ~30 packages |
| `ml` | ML/NLP stack | +~40 packages |
| `llm` | LLM provider integrations | +~4 packages |
| `dev` | Development tools | +~5 packages |
| `all` | Everything | +~49 packages |

## Troubleshooting

### Import Errors

If you see `ModuleNotFoundError`, it usually means you're missing an optional dependency:

```python
ModuleNotFoundError: No module named 'sklearn'
```

**Solution:** Install the required extra:
```bash
pip install -e ".[ml]"  # For sklearn
pip install -e ".[llm]"  # For LLM providers
```

### Command Not Found

If a command doesn't appear in `--help` output, it may require optional dependencies:

```bash
python src/main.py --help | grep my-ml-command
# No output? Command requires ML dependencies.
```

**Solution:** Install the required extra and check the warning log.

### CI/CD Issues

If CI fails with import errors, verify:
1. CI installs base dependencies only: `pip install -e .`
2. Import safety verification step passes
3. Workflow doesn't use `requirements.txt`

## Dependency Management (For Developers)

### Adding New Optional Dependencies

1. Add to `pyproject.toml` under `[project.optional-dependencies]`:

```toml
[project.optional-dependencies]
ml = [
    "scikit-learn>=1.3.0",
    # Add your new ML dependency here
]
```

2. Use the helper in code:

```python
from utils.dependencies import require_optional, is_available

# Lazy import with helpful error
sklearn = require_optional("sklearn", "ml")

# Or check without importing
if is_available("sklearn"):
    from sklearn.metrics import accuracy_score
```

3. Update documentation to explain the new dependency.

### Creating Optional Commands

For commands that require optional dependencies, inherit from `OptionalCommand`:

```python
from utils.command.optional_command import OptionalCommand

class MyMLCommand(OptionalCommand):
    REQUIRED_GROUP = "ml"
    REQUIRED_MODULES = ["sklearn", "scipy"]

    @staticmethod
    def get_name() -> str:
        return "my-ml-command"

    # ... implement other methods
```

The command will only register if dependencies are available, with a helpful warning log if not.

## Migration from requirements.txt

The `requirements.txt` file is deprecated. Use `pyproject.toml` extras instead:

| Old (Deprecated) | New (Recommended) |
|------------------|-------------------|
| `pip install -r requirements.txt` | `pip install -e ".[all]"` |
| `pip install -r requirements-dev.txt` | `pip install -e ".[ml,llm,dev]"` |

The `requirements.txt` file will be removed in a future release.

## Version Compatibility

- Python: 3.13 (<3.14)
- pandas: >=2.0.0
- scikit-learn: >=1.3.0 (optional)
- torch: >=2.0.0 (optional)

For detailed version information, see `pyproject.toml`.

## Getting Help

If you encounter installation issues:
1. Check this guide's troubleshooting section
2. Verify Python version: `python --version`
3. Try with a fresh virtual environment
4. Check GitHub Issues for known problems
