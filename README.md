# Project README

## Overview

This project provides a command-line interface (CLI) for managing various domain-specific operations. The core functionalities are dynamically loaded from modules in the `src/domains` directory. The CLI allows users to run different commands based on the modules available.

The project also includes tools to facilitate debugging using VS Code, making it easy to develop, debug, and extend the CLI functionalities.

## Summary System

PyToolkit includes a centralized summary system that emits standardized JSON summaries for command outputs. Domain-specific managers encapsulate summary generation to avoid duplication and ensure consistency, while preserving backward compatibility of metric formats and output paths.

- JIRA: `domains.syngenta.jira.summary.jira_summary_manager.JiraSummaryManager`
- Datadog: `domains.syngenta.datadog.summary.datadog_summary_manager.DatadogSummaryManager`

Commands delegate summary emission to these managers (e.g., via `emit_summary_compatible`), keeping Markdown rendering and printing logic within the command.

## Project Structure

Here is a brief description of the main components in this project:

```
project-root/
├── README.md                # Project documentation
├── src/
│   ├── main.py              # Main entry point for the CLI
│   ├── domains/             # Directory for different domain modules
│   │   └── ag_operation/    # Domain example containing commands related to Agro Operations
│   │       ├── copy_data.py # Contains main functions to manage agro operation data
│   │       └── config_copy_data.py # Configuration file for data copying
│   ├── utils/               # Utilities for various support tasks
│       ├── dynamodb_manager.py # Manages DynamoDB-related operations
│       └── logging_config.py  # Handles logging configuration
└── .vscode/
    └── launch.json         # VS Code launch configurations for debugging
```

## Setup and Installation

To set up the project, follow these steps:

1. **Clone the Repository**:

   ```sh
   git clone <repository-url>
   cd project-root
   ```

2. **Install Dependencies**:
   Make sure to have Python 3.12 installed. Install required Python packages with:

   ```sh
   pip install -r requirements.txt
   ```

3. **Environment Configuration**:
   Create an `.env` file in the `src/domains/ag_operation` folder with the necessary environment variables for AWS credentials, DynamoDB table names, etc.

   Example:

   ```
   SOURCE_AWS_ACCESS_KEY_ID=your_source_key
   SOURCE_AWS_SECRET_ACCESS_KEY=your_source_secret
   TARGET_AWS_ACCESS_KEY_ID=your_target_key
   TARGET_AWS_SECRET_ACCESS_KEY=your_target_secret
   AGRO_OPERATIONS_TABLE=your_table_name
   REVERSED_KEYS_TABLE=your_reversed_keys_table
   PRODUCT_TYPE_TABLE=your_product_table
   SUMMARIZED_AGRO_OP_TABLE=your_summarized_table
   ```

## Running the CLI

The main entry point for the CLI is `src/main.py`. You can explore the available domains and commands using the help option:

```sh
python src/main.py --help
```

To run a specific command, such as copying agro operation data, use:

```sh
python src/main.py ag_operation copy_data
```

## JIRA Commands

### Component Classifier

Classify JIRA issues into components using LLM analysis.

**Usage**:
```bash
python src/main.py syngenta jira classify-components \
  --project-key CATALOG \
  --source-component "Legacy System" \
  --output output/classification.json \
  --dry-run
```

**Configuration**:
- Edit `src/domains/syngenta/jira/component_classifier_config.json` to define target components
- Set `PORTKEY_API_KEY` and `PORTKEY_PROVIDER_SLUG` in `.env`
- Configure `LLM_MODEL` (default: gpt-4o-mini)

**Output**: JSON file with classification results and summary statistics

## Debugging with VS Code

This project is set up to support debugging in Visual Studio Code. The `launch.json` file in the `.vscode` directory includes configurations for running the CLI with debugging capabilities.

### Debugging Setup

1. **Open Project in VS Code**: Make sure you have the project open in VS Code.

2. **Launch Configuration**:
   - Open the **Run and Debug** pane (`Ctrl+Shift+D`).
   - Select `"Debug ag_operation copy_data"` from the dropdown.
   - Hit the **Start Debugging** button or press `F5`.

### VS Code Debug Configuration (`launch.json`)

Here is a brief description of the debug configuration:

- **`program`**: Specifies the Python script to be executed (`src/main.py`).
- **`args`**: Arguments passed to the script, e.g., `ag_operation copy_data`.
- **`env`**: Environment variables such as `PYTHONPATH` to help locate the modules correctly.
- **`cwd`**: Sets the current working directory to the root of the project to ensure correct path resolution.

Example of running the command:

```sh
python3.12 src/main.py ag_operation copy_data
```

## Adding New Domain Commands

To add new commands to the CLI:

1. **Create a New Folder** in `src/domains/` for the new domain.
2. **Add Python Modules** inside the domain folder, each with a `main()` function that serves as the command's entry point.
3. **Use `args` Parameter**: If the command requires additional arguments, add a `get_arguments()` function in the module to specify them.

For example, to create a new domain called `weather`, add `src/domains/weather` and create a file `get_weather.py` with a `main()` function that will be dynamically loaded by `src/main.py`.

## Troubleshooting

- **Import Errors**: If you encounter `No module named 'src'`, ensure `PYTHONPATH` includes the root directory (`src`). The `launch.json` configuration and environment variable help to mitigate these issues.
- **Module Ignored**: If you see the message like `Ignoring module ... as it does not have a 'main' function`, ensure that the module defines a `main()` function as an entry point.

## Contact and Support

For issues, please reach out to the project maintainer or raise an issue in the GitHub repository. Contributions are welcome!

## License

This project is licensed under the MIT License. See `LICENSE` for more information.
