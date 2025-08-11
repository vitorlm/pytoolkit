# Scripts Directory

This directory contains the weekly metrics reporting system for the PyToolkit project.

## 📁 Contents

### Scripts

- **`run_reports.sh`** - Main reporting script with intelligent configuration detection

### Configuration Files

- **`config_reports_tribe.env`** - Configuration for tribe-wide reporting
- **`config_reports.env`** - Configuration for team-specific reporting

## 🚀 Usage

### Simple Usage (Single Configuration)

If you have only one configuration file, just run:

```bash
cd scripts
./run_reports.sh
```

### Interactive Selection (Multiple Configurations)

If you have multiple configuration files, the script will show an interactive menu:

```bash
cd scripts
./run_reports.sh

🔧 Multiple configuration files found!
Please select which configuration to use:

   1) config_reports.env         [TEAM-SPECIFIC] - Individual team reporting with filters
   2) config_reports_tribe.env   [TRIBE-WIDE]    - Complete tribe reporting (no team filtering)

   0) Exit

👉 Select option (0-2): 
```

## ⚙️ Configuration

### Tribe-Wide Reports (`config_reports_tribe.env`)

```bash
PROJECT_KEY="CWS"
TRIBE_LINEARB_TEAM_ID="19767"
SONARQUBE_PROJECT_KEYS="[all 27 projects]"
SONARQUBE_ORGANIZATION="syngenta-digital"
```

### Team-Specific Reports (`config_reports.env`)

```bash
PROJECT_KEY="CWS"
TEAM="Catalog"
LINEARB_TEAM_IDS="41576"
SONARQUBE_PROJECT_KEYS="[team-specific projects]"
SONARQUBE_ORGANIZATION="syngenta-digital"
```

## 📊 Output Structure

### Tribe Mode

```text
../output/tribe_weekly_reports_YYYYMMDD/
├── jira/
│   ├── tribe-bugs-support-2weeks.json
│   ├── tribe-bugs-support-lastweek.json
│   ├── tribe-bugs-support-weekbefore.json
│   ├── tribe-tasks-2weeks.json
│   ├── tribe-open-issues.json
│   └── tribe-cycle-time-lastweek.json
├── sonarqube/
│   └── tribe-quality-metrics.json (27 projects)
├── linearb/
│   └── linearb_export*.csv
└── consolidated/
    └── weekly-summary.md
```

### Team Mode

```text
../output/team_weekly_reports_YYYYMMDD/
├── jira/
│   ├── team-bugs-support-2weeks.json
│   ├── team-bugs-support-lastweek.json
│   ├── team-bugs-support-weekbefore.json
│   ├── team-tasks-2weeks.json
│   ├── team-open-bugs-support.json
│   └── team-cycle-time-lastweek.json
├── sonarqube/
│   └── team-quality-metrics.json (team projects)
├── linearb/
│   └── linearb_export*.csv
└── consolidated/
    └── weekly-summary.md
```

## 🔧 Features

### Intelligent Configuration Detection

- **Single Config:** Automatically uses the available configuration
- **Multiple Configs:** Interactive menu for selection
- **No Config:** Clear error message with instructions

### Adaptive Behavior

- **JIRA Filtering:** Automatic team filtering based on mode
- **SonarQube Projects:** All projects (tribe) vs specific projects (team)
- **LinearB Teams:** Parent team (tribe) vs specific team
- **Output Naming:** Appropriate prefixes (tribe- vs team-)

### Error Handling

- **Missing .env:** Clear instructions for creation
- **Failed Commands:** Graceful degradation with warnings
- **Path Issues:** Automatic navigation between directories

## 📈 Analysis Features

### Week-over-Week Comparison

Compare performance between consecutive weeks using the generated JSON files.

### Consolidated Reporting

Each run generates a markdown summary in `consolidated/weekly-summary.md` with:

- Report inventory
- Analysis recommendations  
- Key metrics focus areas
- Performance trends guidance

## 🛠️ Development

### Adding New Configurations

1. Create a new `config_reports_*.env` file
2. The script will automatically detect it
3. Interactive menu will include the new option

### Script Modifications

The script automatically:

- Detects its own location (`scripts/` directory)
- Navigates to project root for Python operations
- Returns to scripts directory for config file access
- Handles relative paths correctly

## 🚨 Prerequisites

1. **Python Virtual Environment:** Run `../setup.sh` from project root
2. **Environment Variables:** Configured in .env files (JIRA, LinearB, SonarQube)
3. **PyToolkit Commands:** Ensure JIRA and LinearB commands are working

## 📞 Support

For issues or questions:

1. Check that virtual environment is activated
2. Verify environment variables in .env files
3. Test individual PyToolkit commands
4. Review output logs for specific error messages
