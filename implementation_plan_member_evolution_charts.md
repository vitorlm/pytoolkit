# Implementation Plan: Member Historical Evolution Charts

## Context
The `MemberAnalyzer` class currently generates 3 charts per member showing only current period data:
1. Comparison bar chart (member vs team)
2. Criterion radar chart
3. Strengths/weaknesses radar

The `IndividualStatistics` class already collects historical evaluation data in the `historical_evaluations` field, but this data is **not visualized** in member-level charts. Only the `TeamAnalyzer` class has temporal charts.

## Objective
Add temporal evolution charts to `MemberAnalyzer` to visualize individual member progression across multiple evaluation periods (2024/Nov → 2025/Jun → 2025/Nov).

## Requirements

### Input Data Structure
The `IndividualStatistics` object contains:
```python
{
  "historical_evaluations": [
    {
      "period": {"year": 2024, "period_name": "Nov", "timestamp": "2024-11-01..."},
      "data": {
        "evaluator_name": {
          "criterion_name": [Indicator objects with scores]
        }
      }
    },
    {
      "period": {"year": 2025, "period_name": "Jun", ...},
      "data": {...}
    }
  ],
  "criteria_stats": {
    "Technical Skills": {
      "average": 4.2,
      "q1": 3.8,
      "q3": 4.5,
      ...
    },
    ...
  }
}
```

### Charts to Implement

#### Chart 1: Criteria Evolution Line Chart
**File:** `member_criteria_evolution.png`
**Type:** Multi-line chart
**Description:** Shows evolution of each criterion's average score over time

**Data Structure:**
- X-axis: Period labels (e.g., "Nov/2024", "Jun/2025", "Nov/2025")
- Y-axis: Average score (0-5)
- Lines: One line per criterion (Technical Skills, Delivery Skills, etc.)
- Colors: Different color per criterion
- Markers: Points at each period

**Example visualization:**
```
5.0 ┤
4.5 ┤     ●━━━●━━━●  Technical Skills
4.0 ┤   ●━━━━●━━━●    Delivery Skills
3.5 ┤ ●━━━━━●━━━●     Soft Skills
3.0 ┤
    └─────┬─────┬─────┬
       Nov/24 Jun/25 Nov/25
```

#### Chart 2: Overall Average Evolution
**File:** `member_overall_evolution.png`
**Type:** Single line chart with trend annotation
**Description:** Shows member's overall average progression

**Features:**
- Single bold line showing overall average
- Start and end point annotations with values
- Trend arrow/label indicating direction (↑ Improving, → Stable, ↓ Declining)
- Percentage change annotation

#### Chart 3: Criteria Category Breakdown (Stacked Area - Optional)
**File:** `member_category_distribution.png`
**Type:** Stacked area chart
**Description:** Shows contribution of each category to overall score over time

**Note:** This is optional - implement only if time permits

## Implementation Steps

### Step 1: Add Helper Method to Extract Historical Criteria Averages
**Location:** `src/domains/syngenta/team_assessment/services/member_analyzer.py`

**Method signature:**
```python
def _get_historical_criteria_data(self) -> tuple[list[str], dict[str, list[float]]]:
    """
    Extracts historical criteria averages from historical_evaluations.

    Returns:
        Tuple of (period_labels, criteria_data)
        - period_labels: ["Nov/2024", "Jun/2025", "Nov/2025"]
        - criteria_data: {
            "Technical Skills": [3.5, 3.8, 4.2],
            "Delivery Skills": [4.0, 4.1, 4.3],
            ...
          }
    """
```

**Implementation details:**
1. Check if `self.individual_data.historical_evaluations` exists and is not empty
2. Sort historical periods by timestamp
3. For each period:
   - Calculate average per criterion from raw evaluation data
   - Format period label as "{period_name}/{year}"
4. Add current period data from `self.individual_data.criteria_stats`
5. Return period labels and criteria averages dictionary

**Edge cases to handle:**
- No historical data: return empty lists
- Missing criteria in some periods: fill with None or skip
- Different evaluators per period: aggregate properly

### Step 2: Implement Criteria Evolution Line Chart
**Location:** Same file, new method

**Method signature:**
```python
def plot_criteria_evolution(self, title: str = "Criteria Evolution Over Time") -> None:
```

**Implementation:**
1. Call `_get_historical_criteria_data()` to get data
2. If no historical data, log warning and return early
3. Create figure with appropriate size (e.g., 12x6)
4. For each criterion:
   - Plot line with markers
   - Use distinct color from palette
   - Add legend entry
5. Configure:
   - X-axis: period labels, rotated if needed
   - Y-axis: 0-5 range with 0.5 increments
   - Grid: light horizontal lines
   - Legend: outside plot area or best location
6. Save to `self.output_path/member_criteria_evolution.png`

**Dependencies:**
- matplotlib.pyplot
- Use ChartMixin utilities if applicable

### Step 3: Implement Overall Average Evolution Chart
**Method signature:**
```python
def plot_overall_evolution(self, title: str = "Overall Performance Evolution") -> None:
```

**Implementation:**
1. Extract overall averages from historical periods + current
2. Calculate:
   - Overall trend direction (improving/stable/declining)
   - Percentage change from first to last period
3. Create figure
4. Plot single line with:
   - Thicker line (linewidth=3)
   - Markers at each point
   - Annotation at start point: "Start: X.XX"
   - Annotation at end point: "Current: X.XX (+Y%)"
5. Add trend indicator:
   - Arrow or text showing direction
   - Color: green (up), gray (stable), red (down)
6. Save to `self.output_path/member_overall_evolution.png`

### Step 4: Update `plot_all_charts()` Method
**Location:** Same file

**Current code:**
```python
def plot_all_charts(self) -> None:
    """Generates all comparison charts (bar and radar) for the individual vs team analysis."""
    self.plot_comparison_bar_chart()
    self.plot_criterion_comparison_radar_chart()
    self.plot_member_strengths_weaknesses_radar_chart()
```

**Updated code:**
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

### Step 5: Add Calculation Helper for Historical Statistics
**Method signature:**
```python
def _calculate_period_statistics(self, period_data: dict) -> dict[str, float]:
    """
    Calculates criteria averages from raw period evaluation data.

    Args:
        period_data: Raw evaluation data for a period
            {
              "evaluator_name": {
                "criterion": [Indicator objects]
              }
            }

    Returns:
        Dictionary mapping criterion names to average scores
        {"Technical Skills": 4.2, "Delivery Skills": 4.0, ...}
    """
```

**Implementation:**
1. Aggregate all indicators across all evaluators for each criterion
2. Calculate mean score per criterion
3. Return dictionary

**Note:** This logic may already exist in `FeedbackAnalyzer` - check if we can reuse

### Step 6: Testing and Validation

**Test scenarios:**
1. **Member with full history** (e.g., Fernando Couto: 2024/Nov, 2025/Jun, 2025/Nov)
   - Verify 3 periods are plotted
   - Check line continuity
   - Validate trend calculation

2. **Member with partial history** (e.g., Andrea Zambrana: only 2024/Nov, 2025/Nov)
   - Should plot available periods
   - No errors on missing data

3. **New member with no history** (e.g., Arthur Melo: only 2025/Nov)
   - Should skip temporal charts gracefully
   - Log appropriate message

4. **Edge case: All criteria missing in one period**
   - Handle gracefully (skip or show gap)

**Validation checklist:**
- [ ] Charts are generated in correct output folder
- [ ] PNG files are created with correct names
- [ ] No crashes when historical data is missing
- [ ] Period labels are properly formatted
- [ ] Trends are calculated correctly
- [ ] Colors are distinct and readable
- [ ] Legends are clear
- [ ] Axes are properly labeled
- [ ] Grid enhances readability

## File Modifications Required

### Primary File
`src/domains/syngenta/team_assessment/services/member_analyzer.py`

**Changes:**
1. Add `_get_historical_criteria_data()` method
2. Add `_calculate_period_statistics()` method
3. Add `plot_criteria_evolution()` method
4. Add `plot_overall_evolution()` method
5. Update `plot_all_charts()` method

**Estimated lines of code:** ~150-200 LOC

### Dependencies
- No new imports needed (matplotlib already used via ChartMixin)
- Leverage existing `ChartMixin` utilities where applicable
- Use existing logging infrastructure

## Expected Output

After implementation, each member with historical data will have **5 charts** instead of 3:

**Existing (3):**
1. `member_team_comparison_bar_chart.png`
2. `member_criterion_radar_chart.png`
3. `member_indicator_strengths_weaknesses_radar_chart.png`

**New (2):**
4. `member_criteria_evolution.png` - Multi-line evolution of all criteria
5. `member_overall_evolution.png` - Single line showing overall performance

## Success Criteria

✅ Member charts folder contains temporal evolution charts when historical data exists
✅ Charts clearly show progression across periods
✅ No errors when historical data is absent
✅ Consistent visual style with existing charts
✅ Period labels are clear and sortable
✅ Trend indicators provide at-a-glance understanding
✅ Code is maintainable and well-documented
✅ Logging provides clear information about chart generation

## References

**Similar implementation in codebase:**
- `src/domains/syngenta/team_assessment/services/team_analyzer.py`
  - Methods: `plot_temporal_evolution()`, `plot_criteria_comparison_over_time()`
  - Lines: 174-297
  - Use as reference for data structure and plotting approach

**Data models:**
- `src/domains/syngenta/team_assessment/core/statistics.py`
  - `IndividualStatistics` class (line 80)
  - `historical_evaluations` field (line 91)

## Notes for Claude Agent

- **Reuse existing patterns:** Follow the structure of `TeamAnalyzer.plot_temporal_evolution()`
- **Error handling:** Fail gracefully when data is missing
- **Logging:** Add INFO logs at start and completion of each chart generation
- **Code style:** Match existing code style (docstrings, type hints, formatting)
- **Testing:** After implementation, suggest running the assessment command to verify charts are generated
