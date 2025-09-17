# JIRA Workflow Configuration System

Este sistema permite configurar workflows de diferentes projetos JIRA de forma flexível e reutilizável.

## Estrutura de Arquivos

```
workflow_configs/
├── README.md                    # Este arquivo
├── project_mappings.json        # Mapeamento projeto -> configuração
├── cropwise_workflow.json       # Configuração específica do CWS
├── default_workflow.json        # Configuração padrão
└── [project]_workflow.json      # Outras configurações de projeto
```

## Como Usar

### 1. Comandos Básicos

```bash
# Listar configurações disponíveis
python src/main.py syngenta jira workflow-config --operation list

# Validar configuração do projeto CWS
python src/main.py syngenta jira workflow-config --project-key CWS --operation validate

# Testar configuração do projeto CWS
python src/main.py syngenta jira workflow-config --project-key CWS --operation test

# Mostrar configuração detalhada
python src/main.py syngenta jira workflow-config --project-key CWS --operation show --verbose

# Limpar cache
python src/main.py syngenta jira workflow-config --operation clear-cache
```

### 2. Usando no Código

```python
from domains.syngenta.jira.workflow_config_service import WorkflowConfigService

# Inicializar service
workflow_service = WorkflowConfigService()

# Verificar se status é WIP
is_wip = workflow_service.is_wip_status("CWS", "07 Started")  # True

# Obter status semântico
dev_start = workflow_service.get_semantic_status("CWS", "development_start")  # "07 Started"

# Obter configuração de cycle time
start, end = workflow_service.get_cycle_time_statuses("CWS")  # ("07 Started", "10 Done")

# Obter custom field
squad_field = workflow_service.get_custom_field("CWS", "squad_field")  # "customfield_10265"
```

## Criando Nova Configuração de Projeto

### 1. Criar arquivo de configuração

Crie `novo_projeto_workflow.json`:

```json
{
  "project_key": "NOVO",
  "project_name": "Novo Projeto",
  "workflow_name": "Workflow do Novo Projeto",
  "version": "1.0",
  "status_mapping": {
    "backlog": ["To Do", "Backlog"],
    "wip": ["In Progress", "Review"],
    "done": ["Done"],
    "archived": ["Archived"]
  },
  "semantic_statuses": {
    "development_start": "In Progress",
    "completed": "Done"
  },
  "flow_metrics": {
    "cycle_time": {
      "start": "In Progress",
      "end": "Done"
    }
  },
  "custom_fields": {
    "squad_field": "customfield_10001"
  }
}
```

### 2. Atualizar mapeamento

Edite `project_mappings.json`:

```json
{
  "project_mappings": {
    "CWS": "cropwise_workflow.json",
    "NOVO": "novo_projeto_workflow.json"
  }
}
```

### 3. Validar configuração

```bash
python src/main.py syngenta jira workflow-config --project-key NOVO --operation validate
```

## Exemplo de Status Mapping

Para o projeto CWS, os status são mapeados semanticamente:

```
Backlog Statuses:
- "01 New"
- "02 Review"
- "06 Dev Backlog"
- "06A Test Script"

WIP Statuses:
- "07 Started"        → development_start
- "07a Code Review"   → code_review
- "07b To Test"       → test_preparation
- "08 Testing"        → testing
- "09B Ready to Deploy" → deployment_ready

Done Statuses:
- "10 Done"           → completed

Archived Statuses:
- "11 Archived"       → archived
```

## Benefícios

1. **Flexibilidade**: Cada projeto pode ter seu próprio workflow
2. **Semântica**: Use nomes descritivos ao invés de IDs hardcoded
3. **Reutilização**: Configurações podem ser compartilhadas entre projetos
4. **Validação**: Sistema de validação integrado
5. **Cache**: Performance otimizada com cache automático
6. **Extensibilidade**: Fácil adicionar novos projetos e métricas

## Troubleshooting

### Cache Issues
```bash
# Limpar cache se houver problemas
python src/main.py syngenta jira workflow-config --operation clear-cache
```

### Validation Errors
```bash
# Validar configuração para identificar problemas
python src/main.py syngenta jira workflow-config --project-key CWS --operation validate --verbose
```

### Status Not Found
- Verifique se o nome do status no JIRA corresponde exatamente ao configurado
- Use o comando `test` para verificar mapeamentos de status
- Lembre-se: nomes devem ser exatos, incluindo espaços e casing