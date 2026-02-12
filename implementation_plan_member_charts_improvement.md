# Implementation Plan: Improve Member Assessment Charts for Better Feedback

> **Goal**: Restructure the per-member chart suite in the `team_assessment` domain to produce actionable, best-practice feedback visualizations using data already available in `stats.json`.  
> **Created**: 2026-02-09 | **Status**: Not Started  
> **Estimated Tasks**: 10 | **Files Modified**: ~4 | **Files Created**: 0

---

## Context & Problem Statement

The current `generate_assessment` command produces 6 files per member under `output/<run>/members/<Name>/`. Three of them have structural issues that limit their feedback value:

| Chart | Problem |
|---|---|
| `member_team_comparison_radar_chart.png` | Only 4 axes (criteria-level averages). Too coarse to reveal individual strengths/weaknesses. A score of 3.22 vs 3.44 is not actionable. |
| `member_indicator_strengths_weaknesses_radar_chart.png` | Shows all 12 indicators but **only the individual** — no team benchmark, so the reader cannot tell if a score is good or bad relative to the team. |
| `member_team_comparison_bar_chart.png` | Same 4 criteria; readable but still too coarse to surface indicator-level differences. |

Meanwhile, `stats.json` contains rich unused data:
- **Per-evaluator raw scores** (feedback dict) → can compute evaluator agreement/consistency
- **Productivity metrics** (distribution, key_metrics, epic_adherence) → completely unvisualized
- **Historical evaluations** with per-indicator granularity → only used at criteria level for evolution charts
- **Insights** (strengths/opportunities with member_level vs team_level) → computed but not visualized

### Research-Backed Design Decisions

Based on data visualization and feedback best practices:

1. **Diverging bar chart > radar chart** for strengths/weaknesses identification (12+ axes with gap analysis). Radar charts suffer from area distortion, category-order dependency, and cross-axis comparison difficulty. Diverging bars provide precise, immediately readable gap information.
2. **Keep 12-axis radar as secondary view** with team benchmark added — defensible at 12 axes / same 1-5 scale / 2-3 series. Serves as "shape fingerprint" for pattern recognition.
3. **Dumbbell (dot) plot** for before/after comparison — the gold standard for showing change between two time points per category.
4. **Strengths-first ordering** — HBR's "Feedback Fallacy" research shows people learn best when you highlight what already works. Growth delta charts should lead with improvements.
5. **Evaluator consistency** — high standard deviation across evaluators signals blind spots or inconsistent behavior, which is the most actionable multi-source feedback insight.
6. **Donut chart + key metrics** for productivity profile — compact, scannable visualization of work type allocation.

---

## Architecture Notes for Agent

### Key Files

| File | Role | Lines |
|---|---|---|
| `src/domains/syngenta/team_assessment/services/member_analyzer.py` | Per-member chart orchestration. All new chart methods go here. | ~514 |
| `src/domains/syngenta/team_assessment/services/chart_mixin.py` | Generic chart primitives (radar, bar, boxplot). New generic chart types go here. | ~282 |
| `src/domains/syngenta/team_assessment/core/statistics.py` | `IndividualStatistics`, `TeamStatistics`, `BaseStatistics` models. | ~324 |
| `src/domains/syngenta/team_assessment/README.md` | Domain documentation. Update chart list. | ~515 |

### Data Flow

```
IndividualStatistics (member_data) ──┐
                                     ├──► MemberAnalyzer ──► .png charts + stats.json
TeamStatistics (team_data) ──────────┘
```

### Data Available in `IndividualStatistics`

```python
# criteria_stats structure (for each of 4 criteria):
criteria_stats["Technical Skills"] = {
    "average": 4.11,
    "highest": 5, "lowest": 4,
    "q1": 4.0, "q3": 4.0,
    "levels": [5, 4, 4, 4, 4, 4, 4, 4, 4],
    "indicator_stats": {
        "Code Principles and Practices": {"average": 4.33, "highest": 5, "lowest": 4, "q1": 4.0, "q3": 4.5},
        "System Quality and Operations": {"average": 4.0, ...},
        "Solution Design, Documentation, and Implementation": {"average": 4.0, ...}
    }
}

# Same structure exists in TeamStatistics.criteria_stats

# historical_evaluations: list of {period: {period_name, year, timestamp}, data: {evaluator: {criterion: [indicators]}}}
# insights: {strengths: [{indicator, member_level, team_level, reason}], opportunities: [...]}
# productivity_metrics: {distribution: {...}, key_metrics: {...}, summary: {...}, epic_adherence_summary: {...}}
```

### Constraints

- Follow Command-Service separation (all logic in services, never in commands)
- Use `LogManager.get_instance().get_logger()` — never `print()`
- Type hints mandatory (Python 3.13 built-in generics: `list[str]`, `dict[str, Any]`)
- Line length: 120 chars (Ruff)
- Double quotes
- After all changes: `ruff format src/` and `ruff check src/` must pass with zero errors

---

## Tasks

### Task 1: Add `plot_diverging_bar_chart()` to ChartMixin

**File**: `src/domains/syngenta/team_assessment/services/chart_mixin.py`

**What**: Add a new generic method for horizontal diverging bar charts (bars extending left/right from a zero baseline).

**Signature**:
```python
def plot_diverging_bar_chart(
    self,
    labels: list[str],
    values: list[float],
    title: str | None = None,
    filename: str = "diverging_bar_chart.png",
    positive_color: str = "#2ecc71",
    negative_color: str = "#e74c3c",
    neutral_color: str = "#95a5a6",
    xlabel: str = "Gap from Team Average",
    group_labels: list[str] | None = None,
    annotations: list[str] | None = None,
    threshold: float = 0.1,
) -> None:
```

**Behavior**:
- Horizontal bars diverging from x=0
- Values > `threshold` colored `positive_color`, < `-threshold` colored `negative_color`, else `neutral_color`
- If `group_labels` provided, draw horizontal separator lines and group header labels between groups (for criterion grouping)
- If `annotations` provided, place text at end of each bar (e.g., "member: 4.33 | team: 3.0")
- Y-axis shows `labels`, X-axis shows gap values
- Grid on x-axis only, dashed
- Save via `self._save_plot()`

**Acceptance**: Method exists, is callable with test data, produces a valid PNG.

---

### Task 2: Add `plot_dumbbell_chart()` to ChartMixin

**File**: `src/domains/syngenta/team_assessment/services/chart_mixin.py`

**What**: Add a generic dumbbell/dot plot for before/after comparison.

**Signature**:
```python
def plot_dumbbell_chart(
    self,
    labels: list[str],
    before_values: list[float],
    after_values: list[float],
    before_label: str = "Previous",
    after_label: str = "Current",
    title: str | None = None,
    filename: str = "dumbbell_chart.png",
    improve_color: str = "#2ecc71",
    decline_color: str = "#e74c3c",
    stable_color: str = "#95a5a6",
    threshold: float = 0.1,
    show_delta: bool = True,
) -> None:
```

**Behavior**:
- Horizontal layout: each row is a label (criterion/indicator)
- Two dots connected by a line per row: `before_values[i]` and `after_values[i]`
- Line/dot color based on direction: green if improved, red if declined, gray if stable (within `threshold`)
- If `show_delta`, annotate each row with delta text ("+0.5", "-0.3", "=")
- X-axis: score scale (0-5), Y-axis: labels
- Sort rows by delta descending (biggest improvements first — strengths-first approach)
- Save via `self._save_plot()`

**Acceptance**: Method exists, produces valid PNG with connected dots.

---

### Task 3: Add `plot_donut_chart()` to ChartMixin

**File**: `src/domains/syngenta/team_assessment/services/chart_mixin.py`

**What**: Add a generic donut/ring chart with center text.

**Signature**:
```python
def plot_donut_chart(
    self,
    labels: list[str],
    sizes: list[float],
    title: str | None = None,
    filename: str = "donut_chart.png",
    colors: list[str] | None = None,
    center_text: str | None = None,
    annotations: list[str] | None = None,
) -> None:
```

**Behavior**:
- Standard matplotlib donut (pie with `wedgeprops=dict(width=0.4)`)
- Labels with percentage values
- If `center_text` provided, render in center of donut (e.g., "70.1% Value Focus")
- If `annotations` provided, render as text block below chart (key metrics summary)
- Filter out labels with `sizes[i] == 0` to avoid clutter
- Save via `self._save_plot()`

**Acceptance**: Method exists, produces valid donut PNG.

---

### Task 4: Add `plot_indicator_gap_chart()` to MemberAnalyzer

**File**: `src/domains/syngenta/team_assessment/services/member_analyzer.py`

**What**: New method that generates a diverging horizontal bar chart showing the gap between member and team average for each of the 12 indicators, grouped by criterion.

**Data Preparation** — new private method `_get_indicator_gap_data()`:
```python
def _get_indicator_gap_data(self) -> tuple[list[str], list[float], list[str], list[str]]:
    """Returns (labels, gaps, group_labels, annotations) for indicator gap chart."""
```

**Logic**:
1. Iterate `self.individual_data.criteria_stats` (skip `_period_metadata`)
2. For each criterion, iterate `indicator_stats`
3. For each indicator: `gap = member_indicator_avg - team_indicator_avg`
4. Build: `labels` (indicator names), `gaps` (float values), `group_labels` (criterion name per indicator), `annotations` (f"member: {m:.2f} | team: {t:.2f}")
5. Team indicator data from `self.team_data.criteria_stats[criterion]["indicator_stats"][indicator]["average"]`

**Chart Method**:
```python
def plot_indicator_gap_chart(self, title: str = "Indicator Gap: Individual vs Team") -> None:
```
- Call `_get_indicator_gap_data()` to prepare data
- Call `self.plot_diverging_bar_chart(...)` from ChartMixin
- Filename: `member_indicator_gap_chart.png`

**Acceptance**: Chart shows 12 horizontal bars grouped by criterion, diverging from zero. Andrea's chart should show positive bars for Decision Making (+1.33), Code Principles (+0.33), and negative bars for Leadership (-0.17) approximately.

---

### Task 5: Fix `member_indicator_strengths_weaknesses_radar_chart` to Include Team Benchmark

**File**: `src/domains/syngenta/team_assessment/services/member_analyzer.py`

**What**: Modify `_get_indicators_radar_data()` to return 2-3 series (Individual + Team Average + optional Historical Avg) instead of just Individual.

**Current code** (lines ~161-173):
```python
def _get_indicators_radar_data(self) -> tuple[list[str], dict[str, list[float]]]:
    labels = []
    individual_values = []
    for _, criterion_stats in self.individual_data.criteria_stats.items():
        for indicator, indicator_stats in criterion_stats["indicator_stats"].items():
            labels.append(indicator)
            individual_values.append(indicator_stats.get("average", 0))
    data = {"Individual": individual_values}
    return labels, data
```

**New code**:
1. Also extract team indicator averages from `self.team_data.criteria_stats[criterion]["indicator_stats"][indicator]["average"]`
2. Optionally compute historical indicator averages by extending `_get_historical_criteria_averages()` to work at indicator level (new method `_get_historical_indicator_averages()`)
3. Return `data` dict with "Individual" (or `self.current_period_label`), "Team Average", and optional "Historical Avg"
4. Skip `_period_metadata` entries in iteration

**Also update `plot_member_strengths_weaknesses_radar_chart()`**:
- Change filename to `member_indicator_comparison_radar_chart.png`
- Update title to `"Individual vs Team: Indicator Comparison"`

**Acceptance**: Radar chart now shows 2-3 overlaid series on 12 axes. Andrea's radar clearly shows her Technical Skills indicators higher than team and Values and Behaviors indicators lower.

---

### Task 6: Add `plot_evaluator_consistency_chart()` to MemberAnalyzer

**File**: `src/domains/syngenta/team_assessment/services/member_analyzer.py`

**What**: New method that shows per-indicator standard deviation across evaluators, highlighting where evaluator agreement is high vs. low.

**Data Preparation** — new private method `_get_evaluator_consistency_data()`:
```python
def _get_evaluator_consistency_data(self) -> tuple[list[str], list[float], list[float], list[float]]:
    """Returns (indicator_labels, std_devs, member_avgs, score_ranges) for consistency chart."""
```

**Logic**:
1. Access raw feedback from `self.individual_data.feedback` (the raw evaluator-level dict at root of stats.json)
   - **Note**: `IndividualStatistics` does NOT currently store the raw `feedback` dict — only `criteria_stats` and `historical_evaluations`. The raw feedback is stored separately.
   - **Solution**: Add a `feedback` parameter to `MemberAnalyzer.__init__()` accepting the raw feedback dict (already passed elsewhere in the pipeline). Alternatively, extract per-indicator scores from `criteria_stats[criterion]["indicator_stats"][indicator]["levels"]` if that field exists, or from the `criteria_stats[criterion]["levels"]` list. Check which is available.
   - **Simplest approach**: Use the `levels` list inside each `indicator_stats[indicator]` — but this doesn't exist currently. Use instead the `criteria_stats[criterion]["levels"]` which is a flat list. However, per-indicator levels aren't stored separately.
   - **Best approach**: Extract from the raw feedback data. The `MemberAnalyzer` is constructed in `assessment_generator.py`. Check where it's called and pass the feedback dict. Look at `assessment_generator.py` to find the construction site.
2. For each evaluator → for each criterion → for each indicator → collect the `level` value
3. Group by indicator name, compute `statistics.stdev()` across evaluator scores
4. Compute the range (max - min) per indicator as secondary info
5. Return sorted by std_dev descending (most inconsistent first)

**Chart Method**:
```python
def plot_evaluator_consistency_chart(self, title: str = "Evaluator Agreement by Indicator") -> None:
```
- Horizontal bar chart (can use `plot_horizontal_bar_chart` from ChartMixin)
- X-axis: standard deviation (lower = more agreement)
- Color bars: green (std < 0.5), yellow (0.5-1.0), red (> 1.0)
- Annotate each bar with evaluator count and score range
- Filename: `member_evaluator_consistency.png`

**Acceptance**: Chart shows 12 indicator bars with agreement levels. For Andrea, "Solution Design" (all evaluators give 4) should show very low std dev (green), while "Prioritization" (scores: 2, 3, 4) should show higher std dev.

---

### Task 7: Add `plot_growth_delta_chart()` to MemberAnalyzer

**File**: `src/domains/syngenta/team_assessment/services/member_analyzer.py`

**What**: Dumbbell chart showing score changes from last historical period to current period for each criterion (and optionally each indicator).

**Data Preparation** — new private method `_get_growth_delta_data()`:
```python
def _get_growth_delta_data(self) -> tuple[list[str], list[float], list[float]] | None:
    """Returns (labels, previous_values, current_values) or None if no history."""
```

**Logic**:
1. If `historical_evaluations` is empty, return `None`
2. Get the most recent historical period (sort by timestamp, take last)
3. Call `_calculate_period_statistics()` on that period's data to get criteria averages
4. Get current criteria averages from `criteria_stats`
5. Build parallel lists: labels, previous_values, current_values

**Chart Method**:
```python
def plot_growth_delta_chart(self, title: str = "Growth: Previous Period vs Current") -> None:
```
- Call `_get_growth_delta_data()`; skip if returns None
- Call `self.plot_dumbbell_chart(...)` from ChartMixin
- Filename: `member_growth_delta.png`
- Set `before_label` to the period name (e.g., "Jun/2025"), `after_label` to current period label

**Acceptance**: Chart shows 4 rows (one per criterion) with connected dots. For Andrea, should show the delta between Jun/2025 and Nov/2025 scores.

---

### Task 8: Add `plot_productivity_profile()` to MemberAnalyzer

**File**: `src/domains/syngenta/team_assessment/services/member_analyzer.py`

**What**: Donut chart showing work type distribution plus key metrics annotations.

**Data Preparation** — new private method `_get_productivity_data()`:
```python
def _get_productivity_data(self) -> tuple[list[str], list[float], str, list[str]] | None:
    """Returns (labels, sizes, center_text, metric_annotations) or None if no data."""
```

**Logic**:
1. Access productivity metrics. Check how `MemberAnalyzer` receives this data.
   - **Note**: `IndividualStatistics` does not contain `productivity_metrics` directly. It's in the full `stats.json` at root level alongside `feedback_stats`. Check how the data flows.
   - **Solution**: The `MemberAnalyzer` receives `member_data: IndividualStatistics`. The productivity metrics are likely stored in a separate object. Check `assessment_generator.py` for how `MemberAnalyzer` is constructed and whether it has access to productivity data.
   - If not directly available, add an optional `productivity_metrics: dict | None` parameter to `MemberAnalyzer.__init__()`.
2. From `distribution`, extract labels (work types) and sizes (percentage), filtering out zero-percentage entries
3. Build center_text from `key_metrics.value_focus.value` (e.g., "70.1% Value Focus")
4. Build metric_annotations list from `key_metrics` entries with their benchmark ratings

**Chart Method**:
```python
def plot_productivity_profile(self, title: str = "Work Distribution Profile") -> None:
```
- Call `_get_productivity_data()`; skip if returns None
- Call `self.plot_donut_chart(...)` from ChartMixin
- Filename: `member_productivity_profile.png`

**Acceptance**: Donut chart shows EPIC (57%), ABSENCE (19%), SUPPORT (15%), etc. Center text shows value focus percentage. Metrics annotated below.

---

### Task 9: Update `plot_all_charts()` Orchestration and Remove Old Radar

**File**: `src/domains/syngenta/team_assessment/services/member_analyzer.py`

**What**: Update the `plot_all_charts()` method to call new methods and remove the old coarse criteria-level radar.

**Current code** (lines ~498-514):
```python
def plot_all_charts(self) -> None:
    """Generates all comparison charts (bar and radar) for the individual vs team analysis."""
    # Current period charts
    self.plot_comparison_bar_chart()
    self.plot_criterion_comparison_radar_chart()
    self.plot_member_strengths_weaknesses_radar_chart()

    # Temporal evolution charts (if historical data available)
    if self.individual_data.historical_evaluations:
        self._logger.info(f"Generating temporal evolution charts for {self.name}")
        self.plot_criteria_evolution()
        self.plot_overall_evolution()
    else:
        self._logger.info(f"No historical data available for {self.name} - skipping temporal charts")
```

**New code**:
```python
def plot_all_charts(self) -> None:
    """Generates all assessment charts for the individual vs team analysis."""
    # Core comparison charts
    self.plot_comparison_bar_chart()                       # criteria-level bar (existing)
    self.plot_indicator_gap_chart()                        # NEW: indicator-level diverging bars
    self.plot_member_strengths_weaknesses_radar_chart()    # FIXED: now with team benchmark
    self.plot_evaluator_consistency_chart()                # NEW: evaluator agreement

    # Temporal evolution charts (if historical data available)
    if self.individual_data.historical_evaluations:
        self._logger.info(f"Generating temporal evolution charts for {self.name}")
        self.plot_criteria_evolution()                     # existing
        self.plot_overall_evolution()                      # existing
        self.plot_growth_delta_chart()                     # NEW: before/after dumbbell
    else:
        self._logger.info(f"No historical data available for {self.name} - skipping temporal charts")

    # Productivity charts (if data available)
    self.plot_productivity_profile()                       # NEW: donut + metrics
```

**Also**: Do **NOT** delete `plot_criterion_comparison_radar_chart()` method itself (keep for backward compatibility if called directly), but remove it from `plot_all_charts()`.

**Acceptance**: Running `generate_assessment` produces the new set of charts per member. No old `member_team_comparison_radar_chart.png` is generated.

---

### Task 10: Wire Data Dependencies and Update Constructor

**File**: `src/domains/syngenta/team_assessment/services/member_analyzer.py` and `src/domains/syngenta/team_assessment/assessment_generator.py`

**What**: Ensure MemberAnalyzer has access to raw feedback data and productivity metrics needed by Tasks 6 and 8.

**Steps**:
1. Read `assessment_generator.py` to find where `MemberAnalyzer` is constructed
2. Check what data is available at that point (raw feedback dict, productivity metrics)
3. Add optional parameters to `MemberAnalyzer.__init__()`:
   ```python
   def __init__(
       self,
       member_name: str,
       member_data: IndividualStatistics,
       team_data: TeamStatistics,
       output_path: str | None = None,
       current_period_label: str | None = None,
       raw_feedback: dict[str, Any] | None = None,          # NEW
       productivity_metrics: dict[str, Any] | None = None,   # NEW
   ):
   ```
4. Store as `self.raw_feedback` and `self.productivity_metrics`
5. Update the construction site in `assessment_generator.py` to pass these values
6. Update `plot_evaluator_consistency_chart()` to use `self.raw_feedback`
7. Update `plot_productivity_profile()` to use `self.productivity_metrics`

**Acceptance**: Both new charts render correctly with real data. No attribute errors at runtime.

---

## Post-Implementation Checklist

After all 10 tasks are complete:

1. **Format**: Run `ruff format src/domains/syngenta/team_assessment/`
2. **Lint**: Run `ruff check src/domains/syngenta/team_assessment/` — must be zero errors
3. **Type check**: Run `pyright src/domains/syngenta/team_assessment/` — no new errors
4. **Integration test**: Run the full command:
   ```bash
   python src/main.py syngenta team_assessment generate_assessment \
     --competencyMatrixFile "<path_to_matrix>" \
     --feedbackFolder "<path_to_feedback>" \
     --planningFile "<path_to_planning>" \
     --outputFolder ./output/test_charts \
     --ignoredMembers ./ignored_members.json
   ```
5. **Visual inspection**: Open `output/test_charts/members/Andrea/` and verify:
   - `member_indicator_gap_chart.png` — 12 bars, diverging from zero, grouped by criterion
   - `member_indicator_comparison_radar_chart.png` — 12 axes, 2-3 series overlaid
   - `member_evaluator_consistency.png` — 12 bars with color-coded agreement
   - `member_growth_delta.png` — 4 rows with connected dots (only if historical data exists)
   - `member_productivity_profile.png` — donut with metrics (only if productivity data exists)
   - `member_team_comparison_bar_chart.png` — unchanged, still 4 criteria
   - `member_criteria_evolution.png` — unchanged
   - `member_overall_evolution.png` — unchanged
   - NO `member_team_comparison_radar_chart.png` (old coarse radar removed from pipeline)
6. **Update README**: Update `src/domains/syngenta/team_assessment/README.md` chart documentation section

---

## Execution Order & Dependencies

```
Task 1 (diverging bar in ChartMixin) ─────► Task 4 (indicator gap chart)
Task 2 (dumbbell in ChartMixin) ──────────► Task 7 (growth delta chart)
Task 3 (donut in ChartMixin) ─────────────► Task 8 (productivity profile)
Task 5 (fix indicator radar) ─────────────► Task 9 (update plot_all_charts)
Task 10 (wire data) ─────────────────────► Task 6 (evaluator consistency)
                                           Task 8 (productivity profile)
Task 4, 5, 6, 7, 8 ──────────────────────► Task 9 (orchestration)
```

**Parallel execution batches**:
- **Batch 1**: Tasks 1, 2, 3 (independent ChartMixin additions)
- **Batch 2**: Tasks 4, 5, 10 (depend on Batch 1 + require reading assessment_generator.py)
- **Batch 3**: Tasks 6, 7, 8 (depend on Batch 2)
- **Batch 4**: Task 9 (depends on all above)
- **Batch 5**: Post-implementation checklist (format, lint, test, README)
