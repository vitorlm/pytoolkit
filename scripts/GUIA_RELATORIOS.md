# 📊 Guia de Relatórios Semanais - PyToolkit

Guia prático para gerar relatórios semanais de performance dos times usando o PyToolkit.

## 🚀 Como Executar

### 1. Preparação Inicial (apenas na primeira vez)

```bash
# Ativar o ambiente virtual Python
cd /Users/vitormendonca/git-pessoal/python/PyToolkit
source .venv/bin/activate
```

### 2. Gerar Relatórios da Semana Atual

**Relatórios por Time (exemplo: Catalog):**
```bash
cd scripts
./run_reports.sh --config team
```

**Relatórios da Tribo Completa:**
```bash
cd scripts
./run_reports.sh --config tribe
```

### 3. Gerar Relatórios de Data Específica

Se você precisa gerar relatórios de uma semana passada:

```bash
cd scripts
./run_reports_custom_date.sh 2025-11-25 --config team
```

## 📁 Onde Encontrar os Relatórios

Os relatórios são salvos em:
```
output/
├── team_weekly_reports_YYYYMMDD/    # Relatórios por time
│   ├── jira/                        # Métricas de JIRA
│   ├── linearb/                     # Métricas de engenharia
│   └── consolidated/                # Resumo consolidado
│
└── tribe_weekly_reports_YYYYMMDD/   # Relatórios da tribo
    ├── jira/
    ├── linearb/
    └── consolidated/
```

**Exemplo:**
```
output/team_weekly_reports_20251202/consolidated/weekly-summary.md
```

## 📊 O Que Cada Relatório Contém

### 🎯 Modo Time (Team)

Relatórios gerados para um time específico (ex: Catalog):

**JIRA (12 arquivos):**
- `team-bugs-support-2weeks.json` - Bugs e Suportes das últimas 2 semanas
- `team-bugs-support-lastweek.json` - Bugs e Suportes da semana passada
- `team-bugs-support-weekbefore.json` - Bugs e Suportes da semana anterior
- `team-tasks-2weeks.json` - Tasks concluídas (2 semanas)
- `team-open-bugs-support.json` - Issues abertas atualmente
- `team-wip-age-bugs-support.json` - Idade dos trabalhos em progresso
- `team-cycle-time-bugs-lastweek.json` - Tempo de ciclo de Bugs
- `team-cycle-time-support-lastweek.json` - Tempo de ciclo de Suportes
- `team-cycle-time-development-lastweek.json` - Tempo de ciclo de Desenvolvimento
- `net-flow-*.md` - Análise de fluxo (entrada vs saída)
- `issue-adherence-*.md` - Aderência a prazos

**LinearB:**
- Métricas de engenharia do time (PR, commits, deploy frequency, etc.)

**Resumo:**
- `weekly-summary.md` - Resumo executivo com todos os insights

### 🌟 Modo Tribo (Tribe)

Relatórios consolidados de toda a tribo Core Services:

**JIRA (10 arquivos):**
- Similar ao modo time, mas com dados de toda a tribo
- Prefixo `tribe-` nos arquivos

**LinearB:**
- Métricas consolidadas do time pai (ID: 19767)

## ⚙️ Configuração

### Arquivo: `config_reports.env` (Time)

```bash
PROJECT_KEY="CWS"
TEAM="Catalog"                    # Nome do seu time
LINEARB_TEAM_IDS="41576"          # ID do time no LinearB
SONARQUBE_PROJECT_KEYS=""         # Projetos do time no SonarQube
```

### Arquivo: `config_reports_tribe.env` (Tribo)

```bash
PROJECT_KEY="CWS"
TRIBE_LINEARB_TEAM_ID="19767"     # ID do time pai (tribo completa)
SONARQUBE_PROJECT_KEYS="..."      # Todos os 27 projetos da tribo
```

## 📆 Períodos de Análise

Os relatórios usam semanas de **segunda-feira a domingo**:

- **Se hoje é domingo:** Usa a semana atual (incluindo hoje)
- **Se hoje é segunda-sábado:** Usa a semana anterior completa

**Exemplo (hoje é terça, 02/12/2025):**
- **Semana selecionada:** 25/11 (seg) até 01/12 (dom)
- **Semana anterior:** 18/11 (seg) até 24/11 (dom)
- **Período combinado:** 18/11 até 01/12 (2 semanas)

## 💡 Dicas de Uso

### Análise Semanal Padrão

1. **Toda segunda-feira de manhã:**
   ```bash
   cd scripts && ./run_reports.sh --config team
   ```

2. **Abrir o resumo:**
   ```bash
   # O script mostra o caminho no final da execução
   open output/team_weekly_reports_YYYYMMDD/consolidated/weekly-summary.md
   ```

### Comparar Semanas

```bash
# Os arquivos *-lastweek.json e *-weekbefore.json permitem comparação
# Use qualquer ferramenta de diff ou análise de JSON
```

### Relatório de Data Específica

**Opção 1: Usando o script custom_date**
```bash
cd scripts
./run_reports_custom_date.sh 2025-11-18 --config team
```

**Opção 2: Configurando diretamente no .env**

Você pode definir datas customizadas no arquivo `config_reports.env`:

#### Para uma data/semana específica:
```bash
# Descomente e defina a data de referência
CUSTOM_REPORT_DATE="2025-11-25"  # O script calcula a semana que contém esta data
```

#### Para um período totalmente customizado:
```bash
# Descomente e defina o período exato
CUSTOM_START_DATE="2025-11-18"   # Data inicial do período
CUSTOM_END_DATE="2025-11-24"     # Data final do período
```

**Como funciona:**

1. **CUSTOM_REPORT_DATE** - Define uma data de referência:
   - Se for domingo: usa a semana que termina nesse dia
   - Se for segunda-sábado: usa a semana anterior completa
   - Exemplo: `CUSTOM_REPORT_DATE="2025-11-25"` (terça) → semana de 18/11 a 24/11

2. **CUSTOM_START_DATE + CUSTOM_END_DATE** - Período totalmente customizado:
   - Você define início e fim exatos
   - Útil para análises trimestrais, mensais, ou períodos especiais
   - **Ambos devem ser informados juntos**
   - Exemplo: `CUSTOM_START_DATE="2025-11-01"` + `CUSTOM_END_DATE="2025-11-30"` → mês completo

3. **Deixar tudo comentado** - Comportamento padrão:
   - Usa a data atual do sistema
   - Calcula automaticamente as semanas (segunda-domingo)

**⚠️ Importante:**
- Formato de data: `YYYY-MM-DD` (ex: 2025-11-25)
- Se usar `CUSTOM_START_DATE`, **deve** informar também `CUSTOM_END_DATE`
- `CUSTOM_START_DATE` deve ser ≤ `CUSTOM_END_DATE`

**Exemplos práticos:**

```bash
# Relatório da semana que contém 25 de novembro
CUSTOM_REPORT_DATE="2025-11-25"

# Relatório do mês de novembro completo
CUSTOM_START_DATE="2025-11-01"
CUSTOM_END_DATE="2025-11-30"

# Relatório de um período de 3 semanas
CUSTOM_START_DATE="2025-11-04"
CUSTOM_END_DATE="2025-11-24"

# Relatório trimestral (Q4)
CUSTOM_START_DATE="2025-10-01"
CUSTOM_END_DATE="2025-12-31"
```

Após configurar, execute normalmente:
```bash
cd scripts
./run_reports.sh --config team
```

## 🛠️ Resolução de Problemas

### Ambiente virtual não ativo

```bash
cd /Users/vitormendonca/git-pessoal/python/PyToolkit
source .venv/bin/activate
```

### Script não executável

```bash
cd scripts
chmod +x run_reports.sh run_reports_custom_date.sh
```

### Erro de configuração

Verifique se os arquivos `.env` existem:
```bash
ls -la scripts/config_reports*.env
```

### Testar comando individual

```bash
python src/main.py syngenta jira open-issues --project-key CWS --help
```

## 📞 Comandos Úteis

```bash
# Ver estrutura dos relatórios gerados
tree output/team_weekly_reports_YYYYMMDD/

# Contar arquivos gerados
find output/team_weekly_reports_YYYYMMDD/ -type f | wc -l

# Ver logs de execução
tail -f logs/pytoolkit_mcp.log.*

# Limpar cache (forçar dados frescos)
rm -rf cache/*.json
```

## 📈 Métricas Principais

Os relatórios fornecem insights sobre:

- ✅ **Bugs & Support:** Quantos foram resolvidos, tempo de resolução
- ⏱️ **Cycle Time:** Quanto tempo leva do início ao fim (por tipo de issue)
- 📊 **Net Flow:** Balanceamento entre issues entrando vs saindo
- 📅 **Adherence:** Aderência a prazos e due dates
- 🔄 **WIP Age:** Quanto tempo as issues ficam "em progresso"
- 🚀 **Engineering Metrics:** PRs, commits, deploy frequency (LinearB)

---

**Última atualização:** Dezembro 2025  
**Projeto:** PyToolkit - Syngenta Digital Core Services Tribe
