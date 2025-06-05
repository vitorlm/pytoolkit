# GitHub Actions Setup for Epic Monitoring

This document describes how to configure GitHub Actions secrets for the Epic Monitoring service.

## Required Secrets

Navigate to your repository → Settings → Secrets and variables → Actions → Repository secrets

Add the following secrets:

### JIRA Configuration
- **JIRA_URL**: Your JIRA server URL (e.g., `https://yourcompany.atlassian.net`)
- **JIRA_USER_EMAIL**: Your JIRA username/email
- **JIRA_API_TOKEN**: Your JIRA API token

### Slack Configuration
- **SLACK_WEBHOOK_URL**: Your Slack webhook URL for notifications
- **SLACK_BOT_TOKEN**: Your Slack bot token (optional, for advanced features)
- **SLACK_CHANNEL_ID**: Your Slack channel ID for notifications

## How to Generate JIRA API Token

1. Go to [Atlassian Account Settings](https://id.atlassian.com/manage-profile/security/api-tokens)
2. Click "Create API token"
3. Give it a label (e.g., "Epic Monitoring GitHub Actions")
4. Copy the generated token
5. Add it as `JIRA_API_TOKEN` secret in GitHub

## How to Create Slack Webhook

1. Go to your Slack workspace
2. Navigate to Apps → Incoming Webhooks
3. Create a new webhook for your desired channel
4. Copy the webhook URL
5. Add it as `SLACK_WEBHOOK_URL` secret in GitHub

## Testing the Workflow

You can manually trigger the workflow by:
1. Going to Actions tab in your repository
2. Selecting "Epic Monitoring" workflow
3. Clicking "Run workflow" button
4. Optionally enabling "Force run regardless of business hours"

## Schedule

The workflow runs automatically:
- **Monday, Wednesday, Friday at 9:00 AM BRT (12:00 PM UTC)**
- The schedule accounts for Brazil Time (UTC-3)

## Troubleshooting

- Check the Actions tab for workflow execution logs
- Failed runs will upload logs as artifacts for debugging
- Ensure all required secrets are properly configured
- Verify the mapping file `jira_to_slack_user_mapping.json` exists in the repository
