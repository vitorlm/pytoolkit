# Epic Monitor Service

A comprehensive JIRA epic monitoring service that automatically tracks Catalog squad epics and sends Slack notifications when problems are detected.

## Features

- **Automated Epic Monitoring**: Fetches epics from JIRA based on configurable queries
- **Cycle-based Filtering**: Filters epics by current cycle (Q1C1, Q1C2, Q2C1, Q2C2, etc.)
- **Problem Detection**: Identifies 6 types of epic problems:
  1. Status is "7 PI Started" but Start Date is missing
  2. Status is "7 PI Started" but Due Date is missing
  3. Status is "7 PI Started" with Start Date but Due Date missing for X business days
  4. Epic is overdue (Due Date in the past)
  5. Epic is approaching due date (3 or fewer business days remaining)
  6. Status is "7 PI Started" but no person is assigned to this epic
- **Slack Notifications**: Sends formatted notifications using Slack Block Kit
- **GitHub Actions Integration**: Runs as automated workflow (Mon, Wed, Fri at 9:00 AM BRT)
- **Configurable**: All settings controlled via environment variables

## Setup

### 1. Environment Configuration

Copy the example environment file and configure your settings:

```bash
cp .env.example .env
```

Edit `.env` with your actual values:

```env
# JIRA Configuration
JIRA_URL=https://your-jira-instance.atlassian.net
JIRA_USER_EMAIL=your-email@company.com
JIRA_API_TOKEN=your-jira-api-token

# Slack Configuration
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR_WORKSPACE/YOUR_CHANNEL/YOUR_SECRET_KEY
SLACK_BOT_TOKEN=xoxb-your-bot-token-here
SLACK_CHANNEL_ID=YOUR_CHANNEL_OR_USER_ID

# Epic Monitor Settings
EPIC_MONITOR_BUSINESS_DAYS_THRESHOLD=3
EPIC_MONITOR_DUE_DATE_WARNING_DAYS=3
EPIC_MONITOR_CHECK_INTERVAL_MINUTES=60

# Cycle Configuration
YEAR_START_DATE=2025-01-06
```

### 2. JIRA Configuration

Ensure your JIRA connection is properly configured. The service uses the existing `JiraAssistant` utility.

### 3. Slack Configuration

#### Option A: Using Slack Bot Token (Recommended)
1. Create a Slack app in your workspace
2. Add the `chat:write` scope to your bot token
3. Install the app to your workspace
4. Set `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` in your `.env` file

#### Option B: Using Webhook URL
1. Create an incoming webhook in your Slack workspace
2. Set `SLACK_WEBHOOK_URL` in your `.env` file

## Deployment

### GitHub Actions (Recommended)

The service is designed to run as a GitHub Actions workflow for reliable, serverless execution.

**Schedule**: Runs Monday, Wednesday, Friday at 9:00 AM BRT (12:00 PM UTC)

#### Setup Steps:

1. **Configure GitHub Secrets** (see [GitHub Actions Setup Guide](../../../.github/GITHUB_ACTIONS_SETUP.md)):
   - `JIRA_SERVER_URL`, `JIRA_USERNAME`, `JIRA_API_TOKEN`
   - `SLACK_WEBHOOK_URL`
   - `EPIC_MONITOR_YEAR_START_DATE`, `EPIC_MONITOR_BUSINESS_DAYS_THRESHOLD`, `EPIC_MONITOR_DUE_DATE_WARNING_DAYS`

2. **Ensure Mapping File**: Make sure `jira_to_slack_user_mapping.json` exists in this directory

3. **Manual Trigger**: You can manually run the workflow from GitHub Actions tab

4. **Monitor**: Check the Actions tab for execution logs and results

### Local Development

For testing and development, you can run the service locally:

### Local Command Line Interface

All commands are available through the main CLI:

```bash
# Run epic monitoring once (for testing)
python src/main.py syngenta jira epic-monitor

# Run epic monitoring once with custom webhook
python src/main.py syngenta jira epic-monitor --slack-webhook "https://hooks.slack.com/services/..."

# Get current cycle information
python src/main.py syngenta jira cycle-info

# Get cycle information with detailed pattern tests
python src/main.py syngenta jira cycle-info --show-all
```

## Configuration Reference

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `JIRA_URL` | JIRA instance URL | Required |
| `JIRA_USER_EMAIL` | JIRA user email | Required |
| `JIRA_API_TOKEN` | JIRA API token | Required |
| `SLACK_WEBHOOK_URL` | Slack webhook URL | Required |
| `SLACK_BOT_TOKEN` | Slack bot token | Optional |
| `SLACK_CHANNEL_ID` | Slack channel/user ID | Optional |
| `EPIC_MONITOR_BUSINESS_DAYS_THRESHOLD` | Days threshold for missing due date | 3 |
| `EPIC_MONITOR_DUE_DATE_WARNING_DAYS` | Days warning before due date | 3 |
| `EPIC_MONITOR_CHECK_INTERVAL_MINUTES` | Check interval in minutes | 60 |
| `YEAR_START_DATE` | First day of the year (YYYY-MM-DD) | 2025-01-06 |

### Cycle Configuration

The service uses a configurable cycle system:
- Each quarter has 2 cycles: C1 (6 weeks) and C2 (7 weeks)
- Total of 13 weeks per quarter
- Cycles: Q1C1, Q1C2, Q2C1, Q2C2, Q3C1, Q3C2, Q4C1, Q4C2
- Year start date is configurable via `YEAR_START_DATE`

### JIRA Query

The service uses the following JQL query to fetch epics:

```jql
type = Epic AND statusCategory != Done AND "Squad[Dropdown]" = "Catalog" ORDER BY priority
```

Epics are then filtered by fix version to match the current cycle.

## Logging

The service uses the centralized logging system. Logs are written to:
- Console output
- Log files in the `logs/` directory
- Specific logger: `EpicMonitorService`, `SlackNotificationService`, `EpicCronService`

## Troubleshooting

### Common Issues

1. **JIRA Connection Failed**
   - Verify `JIRA_URL`, `JIRA_USER_EMAIL`, and `JIRA_API_TOKEN` are correct
   - Check JIRA API token permissions

2. **Slack Notifications Not Sent**
   - Verify `SLACK_WEBHOOK_URL` or `SLACK_BOT_TOKEN` is correct
   - Check Slack app permissions
   - Verify channel ID format

3. **No Epics Found**
   - Check if epics exist with the configured JQL query
   - Verify fix version patterns match current cycle
   - Use `cycle-info` command to verify current cycle

4. **Service Not Running**
   - Check environment variables are loaded
   - Verify business hours configuration
   - Check service logs for errors

### Debug Commands

```bash
# Test cycle detection
python src/main.py syngenta jira cycle-info --show-all

# Run once with verbose logging
python src/main.py syngenta jira epic-monitor
```

## Development

### File Structure

```
src/domains/syngenta/jira/
├── epic_monitor_service.py     # Core service classes
├── epic_monitor_command.py     # One-time monitoring command
├── cycle_info_command.py       # Cycle information utility
├── .env.example               # Environment configuration template
├── jira_to_slack_user_mapping.json # User mapping file
└── README.md                  # This file
```

### Key Classes

- `EpicIssue`: Represents a JIRA epic with problems
- `CycleDetector`: Handles cycle detection and validation
- `EpicMonitorService`: Core monitoring logic
- `SlackNotificationService`: Slack integration
- `EpicCronService`: Main orchestration service (used by both one-time and scheduled execution)

### Adding New Problem Rules

To add new problem detection rules:

1. Edit `EpicMonitorService.analyze_epic_problems()`
2. Add the new rule logic
3. Update documentation and tests
4. Consider adding configuration options

### Testing

```bash
# Test individual components
python -c "from domains.syngenta.jira.epic_monitor_service import CycleDetector; print(CycleDetector.get_current_cycle())"

# Test full flow
python src/main.py syngenta jira epic-monitor
```

## Security

- Never commit `.env` files to version control
- Use environment variables for all sensitive data
- Rotate API tokens and webhook URLs regularly
- Use least-privilege access for JIRA and Slack integrations

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review logs for error messages
3. Verify environment configuration
4. Test individual components
