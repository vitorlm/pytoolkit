# Datadog Observability Analysis Suite

Comprehensive CLI tools for Datadog monitoring analysis, featuring:

## 🔍 Events Analysis (Enhanced)
- **Advanced Alert Classification**: Distinguishes flapping, benign transients, and actionable alerts
- **Temporal Trend Analysis**: Week-over-week monitoring health trends
- **Auto-Healing Insights**: Data-driven recommendations for threshold tuning
- **Business Impact Analysis**: Brazilian timezone and business hours correlation

## 🏗️ Teams & Services Audit
- Verifies team handles exist (Teams v2 API)
- Lists services (Service Definition v2) and flags missing team linkage
- Outputs JSON or Markdown reports

## Usage

### 1) Environment Setup

Required environment variables:
- `DD_API_KEY` - Datadog API key
- `DD_APP_KEY` - Datadog application key
- `DD_SITE` - Datadog site (optional, defaults to datadoghq.eu)

Configure via domain-specific `.env` file:
```bash
cp src/domains/syngenta/datadog/.env.example src/domains/syngenta/datadog/.env
# Edit with your keys
```

Or export in shell:
```bash
export DD_SITE=datadoghq.eu
export DD_API_KEY=your_api_key
export DD_APP_KEY=your_app_key
```

### 2) Enhanced Events Analysis

**Basic Analysis:**
```bash
python src/main.py syngenta datadog events \
  --teams "team1,team2" \
  --days 7 \
  --output-format md
```

**Advanced Analysis with Classification & Trends:**
```bash
python src/main.py syngenta datadog events \
  --teams "team1,team2" \
  --advanced-analysis \
  --detailed-stats \
  --analysis-period 30 \
  --min-confidence 0.8 \
  --output-format md
```

**Key Flags:**
- `--advanced-analysis` - Enable auto-healing classification and trend analysis
- `--detailed-stats` - Include comprehensive per-monitor health scores
- `--analysis-period N` - Analysis window in days (default: 30)
- `--min-confidence X` - Confidence threshold for removal recommendations
- `--use-cache` - Enable 30-minute API response caching

### 3) Teams & Services Audit

**JSON Output:**
```bash
python src/main.py syngenta datadog teams-services \
  --teams "team1,team2"
```

**Markdown Report:**
```bash
python src/main.py syngenta datadog teams-services \
  --teams "team1,team2" \
  --out md
```

## Output Files

### Events Analysis Reports
- **Markdown**: `output/datadog-events_YYYYMMDD/datadog_events_YYYYMMDD_HHMMSS.md`
- **JSON**: `output/datadog-events_YYYYMMDD/datadog_events_YYYYMMDD_HHMMSS.json`

### Teams & Services Audit
- **JSON**: `output/datadog_teams_services_<timestamp>.json`
- **Markdown**: `output/datadog_teams_services_<timestamp>.md`

### Weekly Trend Snapshots
- **Monitor Snapshots**: `snapshots/monitors_YYYY-WXX.json`
- **Summary Snapshots**: `snapshots/summary_YYYY-WXX.json`

## Enhanced Features

### 🔬 Auto-Healing Classification

**Alert Cycle Types:**
- 🔄 **Flapping**: Rapid state oscillations (threshold/system issues)
- ⚡ **Benign Transient**: Short self-resolving issues (<5min, no action needed)
- 🎯 **Actionable**: Legitimate alerts requiring human intervention

**Classification Algorithms:**
- Duration thresholds and transition pattern analysis
- Business hours correlation (Brazilian timezone)
- Evidence of human intervention detection
- Statistical confidence scoring (0.0-1.0)

### 📈 Temporal Trend Analysis

**Weekly Metrics:**
- Monitor health scores and noise levels
- Self-healing rates and actionability
- MTTR/MTBF and cycle frequency
- Business hours impact analysis

**Trend Detection:**
- Linear regression with significance testing
- Week-over-week delta calculations
- 4-week moving averages
- Small-sample safeguards (≥3 weeks required)

### 🎯 Actionable Recommendations

**Flapping Mitigation:**
- Debounce window suggestions (60-300s)
- Hysteresis threshold recommendations
- Composite monitor strategies

**Benign Transient Policies:**
- Dashboard-only routing
- Increased duration thresholds
- Informational-only classification

**Threshold Optimization:**
- Data-driven parameter tuning
- Environment-specific adjustments
- Confidence-based prioritization

## Configuration

### Configuration File (Optional)

Create `config/observability.yml` for advanced settings:

```yaml
# Advanced analysis settings
flapping:
  flap_window_minutes: 60          # Time window for cycle detection
  flap_min_cycles: 3               # Minimum cycles for flapping

transient:
  transient_max_duration_seconds: 300.0  # 5 minute threshold
  require_simple_transition: true        # Only alert→recovery

business_hours:
  start_hour: 9                    # Brazilian business hours
  end_hour: 17
  timezone_offset_hours: -3        # UTC-3 (BRT/BRST)

trend_analysis:
  min_weeks_for_trends: 3          # Minimum data for trends
  default_lookback_weeks: 8        # Analysis window
```

### Environment Variables

Override config via environment:
```bash
export OBSERVABILITY_FLAP_WINDOW_MINUTES=90
export OBSERVABILITY_TRANSIENT_MAX_DURATION=240.0
export OBSERVABILITY_ENABLE_HYSTERESIS=true
```

## Installation

Required dependencies:

```bash
pip install datadog-api-client>=2.26.0 pyyaml
```

Or add to `requirements.txt`:
```
datadog-api-client>=2.26.0
pyyaml>=6.0
```

## Example Report Sections

### Enhanced Analysis Output

```markdown
## 🔬 Enhanced Auto-Healing Analysis

### Alert Cycle Classification
- Total Cycles Analyzed: 157
- 🔄 Flapping Cycles: 23 (14.6%)
- ⚡ Benign Transient Cycles: 89 (56.7%)
- 🎯 Actionable Cycles: 45 (28.7%)
- Average Classification Confidence: 82.4%

### 🎯 Actionable Recommendations

#### 🔄 Flapping Mitigation
- **[API Health Check](https://app.datadoghq.eu/monitors/123456)**
  - Issue: 8/12 cycles are flapping
  - Recommended Action: Increase debounce window or add hysteresis
  - Suggested Debounce: 120 seconds

#### ⚡ Benign Transient Policy Changes
- **[Ingress 502/504 errors](https://app.datadoghq.eu/monitors/789012)**
  - Pattern: 18/20 cycles are benign transients
  - Recommended Action: Route to dashboard instead of paging
```

### Trend Analysis Output

```markdown
## 📈 Temporal Trend Analysis

### Trend Summary
- Analysis Period: Week 2025-W42
- Monitors Analyzed: 43
- Historical Data: 8 weeks available

**Monitor Trend Distribution:**
- 🟢 Improving: 12 monitors (27.9%)
- 🔴 Degrading: 8 monitors (18.6%)
- ⚪ Stable: 23 monitors (53.5%)

### 📢 Significant Changes This Week
- API Gateway Monitor: noise_score decreased by 34.2% week-over-week
- Database Connection Monitor: health_score showing strong improvement trend
```

## Best Practices

### Operational Guidelines

**Safety First:**
- Review high-confidence recommendations (>0.85) with domain experts
- Test threshold changes in staging environments
- Monitor impact of changes for ≥1 week
- Keep backup configurations

**Gradual Implementation:**
- Start with `--advanced-analysis` on 2-3 teams
- Build 4+ weeks of historical data before major changes
- Focus on flapping monitors first (immediate noise reduction)
- Address benign transients through policy changes

**Monitoring Health:**
- Run weekly analysis to track improvements
- Set up alerts for trend degradations
- Review removal candidates quarterly
- Validate classification accuracy with on-call feedback

## Troubleshooting

**Common Issues:**

1. **Insufficient Trend Data**: Need ≥3 weeks for reliable trends
2. **Low Classification Confidence**: Increase analysis period to 45+ days
3. **Missing Snapshots**: Check disk space and permissions in snapshots/
4. **API Rate Limits**: Use `--use-cache` and stagger team analysis

For detailed documentation, see [docs/observability-analysis.md](../../../docs/observability-analysis.md)
