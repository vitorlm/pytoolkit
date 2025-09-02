#!/bin/bash

# Script para testar o monitoramento de issues localmente
# Usage: ./test_issue_monitor.sh [SQUAD] [ISSUE_TYPES] [SLACK_CHANNEL_ID]

# Configurações padrão
DEFAULT_SQUAD="Catalog"
DEFAULT_ISSUE_TYPES="Bug,Support,Story,Task,Technical Debt,Improvement,Defect"
DEFAULT_SLACK_CHANNEL="U040XE9K0NA"  # Seu DM

# Usar argumentos ou valores padrão
SQUAD=${1:-$DEFAULT_SQUAD}
ISSUE_TYPES=${2:-$DEFAULT_ISSUE_TYPES}
SLACK_CHANNEL=${3:-$DEFAULT_SLACK_CHANNEL}

echo "🧪 Testando monitoramento de issues..."
echo "📊 Squad: $SQUAD"
echo "🏷️  Issue Types: $ISSUE_TYPES"
echo "💬 Slack Channel: $SLACK_CHANNEL"
echo "🔍 Modo: DRY RUN (sem notificações)"
echo ""

# Ativar ambiente virtual se não estiver ativo
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "Ativando ambiente virtual..."
    source .venv/bin/activate
fi

# Teste em modo dry-run primeiro
echo "🚀 Executando teste em modo DRY-RUN..."
SLACK_CHANNEL_ID="$SLACK_CHANNEL" python src/main.py syngenta jira issue-duedate-monitor \
  --squad "$SQUAD" \
  --project-key "CWS" \
  --issue-types "$ISSUE_TYPES" \
  --dry-run

echo ""
echo "✅ Teste dry-run concluído!"
echo ""
echo "📝 Para enviar notificação real, execute:"
echo "SLACK_CHANNEL_ID=\"$SLACK_CHANNEL\" python src/main.py syngenta jira issue-duedate-monitor --squad \"$SQUAD\" --project-key \"CWS\" --issue-types \"$ISSUE_TYPES\""
