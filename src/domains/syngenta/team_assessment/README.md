# Team Assessment Module

## Overview

The **Team Assessment Module** is a comprehensive system for generating, analyzing, and managing team and individual member assessments. It aggregates data from multiple sources including competency matrices, feedback evaluations, planning documents, and JIRA to produce detailed performance reports with historical tracking and trend analysis.

## Key Features

- **Multi-source Data Integration**: Combines competency matrices, feedback surveys, planning data, and JIRA metrics
- **Historical Tracking**: Automatically discovers and compares assessment data across multiple periods
- **Comprehensive Metrics**: Calculates productivity, efficiency, resource utilization, and adherence metrics
- **Individual & Team Analysis**: Generates detailed reports at both member and team levels
- **AI-Powered Insights**: Uses LLM (Ollama) for intelligent analysis and recommendations
- **Visualization**: Generates charts and visual representations of performance trends
- **Epic Enrichment**: Integrates JIRA issue data for enhanced context

## Architecture

```
team_assessment/
├── core/                          # Domain models and core logic
│   ├── member.py                  # Member data model
│   ├── team.py                    # Team data model
│   ├── task.py                    # Task data model
│   ├── issue.py                   # JIRA issue model
│   ├── cycle.py                   # Cycle/sprint model
│   ├── indicators.py              # Performance indicators
│   ├── statistics.py              # Statistical calculations
│   ├── health_check.py            # Health check metrics
│   ├── config.py                  # Configuration management
│   └── validations.py             # Data validation
├── services/                      # Business logic services
│   ├── assessment_merge_service.py      # Multi-period assessment merging
│   ├── assessment_display_service.py    # Assessment visualization
│   ├── member_analyzer.py               # Member performance analysis
│   ├── team_analyzer.py                 # Team performance analysis
│   ├── feedback_analyzer.py             # Feedback processing
│   ├── epic_enrichment_service.py       # JIRA epic enrichment
│   ├── historical_period_discovery.py   # Historical data discovery
│   ├── member_productivity_service.py   # Individual productivity metrics
│   ├── team_productivity_service.py     # Team productivity metrics
│   ├── visualization_service.py         # Chart generation
│   └── jira_issue_fetcher.py            # JIRA API integration
├── processors/                    # Data processors
│   ├── criteria_processor.py      # Competency criteria processing
│   ├── feedback_processor.py      # Feedback data processing
│   ├── members_task_processor.py  # Member task allocation processing
│   ├── team_task_processor.py     # Team task processing
│   └── health_check_processor.py  # Health check processing
├── assessment_generator.py        # Main assessment generation orchestrator
├── generate_assessment_command.py # CLI command for assessment generation
├── merge_assessment_command.py    # CLI command for merging assessments
├── show_assessment_command.py     # CLI command for displaying assessments
├── epics_adherence_planning_command.py # CLI command for epic planning
└── .env                          # Configuration and environment variables
```

## Available Commands

### 1. Generate Assessment

Generates comprehensive assessment reports by processing competency matrix, feedback, and planning data.

**Command:**
```bash
python src/main.py syngenta team_assessment generate_assessment \
  --competencyMatrixFile <path_to_competency_matrix.xlsx> \
  --feedbackFolder <path_to_feedback_folder> \
  --planningFile <path_to_planning.xlsx> \
  --outputFolder <path_to_output_folder> \
  [--ignoredMembers <path_to_ignored_members.json>] \
  [--disableHistorical]
```

**Required Inputs:**
- `--competencyMatrixFile`: Excel file containing the competency matrix structure with team member names and evaluation criteria
- `--feedbackFolder`: Directory containing feedback Excel files from evaluators
- `--outputFolder`: Directory where assessment reports will be saved (JSON and charts)

**Optional Inputs:**
- `--planningFile`: Excel file (.xlsx or .xlsm) containing task allocations and planning data
- `--ignoredMembers`: JSON file with list of member names to exclude from assessment
- `--disableHistorical`: Flag to disable historical period discovery and comparison (enabled by default)

**Outputs:**
- JSON assessment reports for each team member
- Team-level assessment summary
- Visualization charts (performance trends, category breakdowns)
- Productivity metrics and statistics

**Example:**
```bash
python src/main.py syngenta team_assessment generate_assessment \
  --competencyMatrixFile ./data/competency_matrix_2024.xlsx \
  --feedbackFolder ./data/feedback/Q4_2024 \
  --planningFile ./data/planning_Q4_2024.xlsx \
  --outputFolder ./output/assessment_Q4_2024
```

---

### 2. Merge Assessment

Merges assessment data for a team member across multiple evaluation periods, calculating trends and evolution.

**Command:**
```bash
python src/main.py syngenta team_assessment merge-assessment \
  --member-name <"Member Full Name"> \
  [--output-dir <path_to_output_dir>] \
  [--base-path <path_to_base_path>]
```

**Required Inputs:**
- `--member-name`: Full name of the team member (e.g., "Italo Ortega")

**Optional Inputs:**
- `--output-dir`: Directory to save merged assessment output (defaults to `./output/merged_assessments`)
- `--base-path`: Base path for assessment period folders (defaults to `./output`)

**Outputs:**
- Merged JSON file with multi-period assessment data
- Overall averages per period
- Category-level averages per period
- Evolution trends across periods
- Summary statistics

**Example:**
```bash
python src/main.py syngenta team_assessment merge-assessment \
  --member-name "Fernando Couto" \
  --output-dir ./output/merged_assessments
```

---

### 3. Show Assessment

Displays merged assessment data for a team member in a formatted, human-readable way.

**Command:**
```bash
python src/main.py syngenta team_assessment show-assessment \
  --member-name <"Member Full Name"> \
  [--merged-dir <path_to_merged_dir>]
```

**Required Inputs:**
- `--member-name`: Full name of the team member (e.g., "Italo Ortega")

**Optional Inputs:**
- `--merged-dir`: Directory containing merged assessment files (defaults to `./output/merged_assessments`)

**Outputs:**
- Formatted console output showing:
  - Assessment details for each period
  - Overall and category averages
  - Self-evaluation status
  - Individual evaluator scores
  - Evolution trends between periods
  - Categories that improved, declined, or remained stable

**Example:**
```bash
python src/main.py syngenta team_assessment show-assessment \
  --member-name "Josiel Nascimento"
```

---

### 4. Epics Adherence Planning

Analyzes epic adherence and planning data, optionally enriched with JIRA project information.

**Command:**
```bash
python src/main.py syngenta team_assessment epics-adherence-planning \
  --planningFolder <path_to_planning_folder> \
  [--jira_project <JIRA_PROJECT_KEY>] \
  [--team_name <Team Name>]
```

**Required Inputs:**
- `--planningFolder`: Directory containing planning Excel files

**Optional Inputs:**
- `--jira_project`: JIRA project key to load (e.g., "PROJ")
- `--team_name`: Name of the team (required if `--jira_project` is provided)

**Note:** Both `--jira_project` and `--team_name` must be provided together.

**Outputs:**
- Epic adherence analysis
- Planning metrics and alignment

**Example:**
```bash
python src/main.py syngenta team_assessment epics-adherence-planning \
  --planningFolder ./data/planning \
  --jira_project "MYPROJECT" \
  --team_name "Engineering Team Alpha"
```

---

## Input File Formats

### 1. Competency Matrix File (.xlsx)

Excel file containing:
- Team member names (Column C, rows 4-14 by default)
- Evaluation criteria organized by categories:
  - Technical Skills
  - Delivery Skills
  - Soft Skills
  - Values and Behaviors
- Expected structure with headers and criteria definitions

### 2. Feedback Files (.xlsx)

Excel files in the feedback folder containing:
- Evaluator information
- Evaluatee (team member being evaluated)
- Scores for each competency criterion
- Comments and qualitative feedback
- Self-evaluation indicators

Multiple feedback files from different evaluators are supported in the same folder.

### 3. Planning File (.xlsx or .xlsm)

Excel file containing:
- Epic/task definitions (rows 6-33 by default)
  - Priority (Column H)
  - Dev Type (Column I)
  - Code (Column J)
  - JIRA Key (Column K)
  - Subject (Column L)
  - Type (Column S)
- Team member allocation matrix (rows 39-48)
  - Member names (Column J)
  - Task assignments (columns K onwards)
- Calendar dates (row 38, starting at column K)
- Planned epic assignments (rows 53-61)

### 4. Ignored Members File (.json)

JSON array containing names of members to exclude:
```json
[
  "John Doe",
  "Jane Smith"
]
```

---

## Configuration

The module is configured via the `.env` file in the `team_assessment` directory.

### Ollama LLM Settings

```env
OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=TalentForgeAI
OLLAMA_NUM_CTX=16384
OLLAMA_TEMPERATURE=0.5
OLLAMA_NUM_THREAD=8
```

### Activity Categories

Activities are classified into categories for analysis:
- **ABSENCE**: Out, OutPlanned, OutUnplanned, Kick-Off
- **DEVELOPMENT**: Epics and roadmap features (identified dynamically)
- **SUPPORT**: Bug, Support
- **IMPROVEMENT**: Tech Debt
- **OVERDUE**: Spillover
- **SECONDED**: Seconded (loaned to another team)
- **LEARNING**: Onboarding, AWS Study
- **PLANNING**: Refinement
- **UNPLANNED**: NotPlanned

### Excel Data Extraction Settings

```env
# Team members
COL_MEMBERS="C"
ROW_MEMBERS_START=4
ROW_MEMBERS_END=14

# Calendar dates
ROW_DAYS=38
COL_DAYS_START="K"

# Epic definitions
ROW_EPICS_START=6
ROW_EPICS_END=33

# Task allocation
ROW_EPICS_ASSIGNMENT_START=39
ROW_EPICS_ASSIGNMENT_END=48
```

### Evaluation Settings

```env
CRITERIA_WEIGHTS={
  "Technical Skills": 0.4,
  "Delivery Skills": 0.25,
  "Soft Skills": 0.25,
  "Values and Behaviors": 0.1
}
OUTLIER_THRESHOLD=1.5
PERCENTILE_Q1=25
PERCENTILE_Q3=75
```

---

## Output Format

### Member Assessment JSON

```json
{
  "name": "John",
  "last_name": "Doe",
  "feedback": {
    "Technical Skills": {
      "Criterion 1": [
        {
          "evaluator": "Manager Name",
          "score": 4.5,
          "comment": "Excellent performance"
        }
      ]
    }
  },
  "feedback_stats": {
    "overall_average": 4.2,
    "category_averages": {
      "Technical Skills": 4.5,
      "Delivery Skills": 4.0
    }
  },
  "productivity_metrics": {
    "total_tasks": 15,
    "completed_tasks": 12,
    "completion_rate": 0.8,
    "efficiency": 0.85
  },
  "health_check": {
    "status": "healthy",
    "indicators": []
  }
}
```

### Merged Assessment JSON

```json
{
  "member_name": "John Doe",
  "periods": [
    {
      "period": "2024_Q3",
      "overall_average": 4.2,
      "self_evaluation": true,
      "evaluators": ["Manager", "Peer 1", "Peer 2"],
      "category_averages": {
        "Technical Skills": 4.5
      }
    }
  ],
  "summary": {
    "total_assessments": 4,
    "periods_covered": ["2024_Q1", "2024_Q2", "2024_Q3", "2024_Q4"],
    "overall_trend": {
      "direction": "improving",
      "first": 3.8,
      "last": 4.2,
      "change": 0.4,
      "change_percentage": 10.5
    }
  }
}
```

---

## Workflow Examples

### Complete Assessment Workflow

1. **Generate Initial Assessment**
```bash
python src/main.py syngenta team_assessment generate_assessment \
  --competencyMatrixFile ./data/competency_matrix.xlsx \
  --feedbackFolder ./data/feedback/2024_Q4 \
  --planningFile ./data/planning_Q4.xlsx \
  --outputFolder ./output/2024_Q4
```

2. **Merge Historical Data**
```bash
python src/main.py syngenta team_assessment merge-assessment \
  --member-name "Fernando Couto"
```

3. **Display Results**
```bash
python src/main.py syngenta team_assessment show-assessment \
  --member-name "Fernando Couto"
```

### Planning Analysis Workflow

```bash
python src/main.py syngenta team_assessment epics-adherence-planning \
  --planningFolder ./data/planning/2024 \
  --jira_project "MYPROJ" \
  --team_name "Backend Team"
```

---

## Dependencies

### Core Dependencies
- `openpyxl`: Excel file processing
- `pydantic`: Data validation and modeling
- `pandas`: Data manipulation and analysis
- Python 3.10+

### Optional Dependencies
- Ollama server for AI-powered insights
- JIRA API access for epic enrichment

---

## Docker Support

The module includes Docker configuration for containerized execution:

**Files:**
- `Dockerfile`: Container image definition
- `docker-compose.yml`: Service orchestration
- `entrypoint.sh`: Container startup script
- `Modelfile`: Ollama model configuration

**Usage:**
```bash
docker-compose up
```

---

## Troubleshooting

### Common Issues

1. **File Not Found Errors**
   - Verify file paths are absolute or relative to the working directory
   - Check file permissions

2. **Missing Historical Data**
   - Ensure output folders follow the expected naming pattern
   - Use `--disableHistorical` flag if historical data is not needed

3. **Ollama Connection Issues**
   - Verify Ollama server is running at `OLLAMA_HOST`
   - Check model availability: `ollama list`

4. **Invalid Excel Structure**
   - Ensure Excel files match the expected structure defined in `.env`
   - Check row/column configurations

---

## Best Practices

1. **Consistent File Structure**: Maintain consistent Excel file structures across periods for historical tracking
2. **Naming Conventions**: Use clear period identifiers in output folder names (e.g., `2024_Q4`, `2024_Q3`)
3. **Regular Backups**: Keep backup copies of assessment data
4. **Incremental Processing**: Run assessments regularly (quarterly) for better trend analysis
5. **Validation**: Review generated reports for data quality before sharing with stakeholders

---

## Future Enhancements

- Web-based dashboard for interactive visualization
- Real-time JIRA synchronization
- Multi-team comparison reports
- Automated report distribution
- Custom evaluation criteria templates

---

## Support

For issues or questions:
1. Check the logs in the `output` directory
2. Review the `.env` configuration
3. Ensure all required input files are properly formatted
4. Verify Ollama service is running (if using AI features)

---

## License

Internal use only - Syngenta proprietary module
