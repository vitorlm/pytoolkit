# Implementation Plan: Add Historical Series to Member Radar Chart

## Context

The member radar chart (`member_team_comparison_radar_chart.png`) currently displays two series:
1. **Individual** - Current period performance (e.g., Nov/2025)
2. **Team Average** - Current period team benchmark

**Enhancement Goal:** Add a third series showing the member's historical average across all previous periods, providing temporal context for current performance.

## Objective

Add a "Historical Average" series to the criteria radar chart that displays the mean of all historical periods (e.g., average of Nov/2024 and Jun/2025), allowing comparison between:
- Current performance
- Team benchmark
- Personal historical baseline

## Data Availability

✅ **Data is available** in `IndividualStatistics.historical_evaluations`:

```python
historical_evaluations = [
    {
        "period": {
            "period_name": "Nov",
            "year": 2024,
            "timestamp": "2024-11-01T00:00:00"
        },
        "data": {
            "Evaluator Name": {
                "Delivery Skills": [
                    {"level": 4, "name": "Planning Accuracy", "evidence": "..."},
                    {"level": 4, "name": "Prioritization and Allocation", "evidence": "..."},
                    ...
                ],
                "Soft Skills": [...],
                "Technical Skills": [...],
                "Values and Behaviors": [...]
            }
        }
    },
    # ... more historical periods
]
```

## Implementation Steps

### Step 1: Create Helper Method for Historical Averages

**Location:** `src/domains/syngenta/team_assessment/services/member_analyzer.py`
**Position:** After `_calculate_period_statistics()` method (~line 234)

**Method Signature:**
```python
def _get_historical_criteria_averages(self) -> dict[str, float]:
    """Calculates average of each criterion across all historical periods.

    Returns:
        Dictionary with criterion averages from historical periods only.
        Example: {
            "Delivery Skills": 3.11,  # average of 3.33 (Nov/24) + 2.89 (Jun/25)
            "Soft Skills": 3.47,       # average of 3.50 (Nov/24) + 3.44 (Jun/25)
            "Technical Skills": 3.78,
            "Values and Behaviors": 2.92
        }
        Returns empty dict if no historical data exists.
    """
```

**Implementation Logic:**
1. Check if `self.individual_data.historical_evaluations` exists
2. If empty, return `{}`
3. For each historical period:
   - Extract period data
   - Call `self._calculate_period_statistics(period_data)` to get criteria averages
   - Accumulate scores per criterion
4. Calculate mean for each criterion across all periods
5. Return dictionary of historical averages

**Key Points:**
- Reuse existing `_calculate_period_statistics()` method to ensure consistency
- Filter out `_period_metadata` (handled by `_calculate_period_statistics`)
- Handle missing criteria gracefully

**Example Code:**
```python
def _get_historical_criteria_averages(self) -> dict[str, float]:
    """Calculates average of each criterion across all historical periods."""
    if not self.individual_data.historical_evaluations:
        return {}

    # Accumulate scores per criterion across all historical periods
    criteria_all_scores = {}

    for hist_entry in self.individual_data.historical_evaluations:
        period_data = hist_entry.get("data", {})
        period_averages = self._calculate_period_statistics(period_data)

        for criterion, avg in period_averages.items():
            if criterion not in criteria_all_scores:
                criteria_all_scores[criterion] = []
            criteria_all_scores[criterion].append(avg)

    # Calculate mean for each criterion
    historical_averages = {}
    for criterion, scores in criteria_all_scores.items():
        if scores:
            historical_averages[criterion] = sum(scores) / len(scores)

    return historical_averages
```

### Step 2: Update `_get_comparison_radar_data()` Method

**Location:** `src/domains/syngenta/team_assessment/services/member_analyzer.py`
**Position:** Replace existing method (~lines 114-133)

**Method Signature:**
```python
def _get_comparison_radar_data(self) -> tuple[list[str], dict[str, list[float]]]:
    """Prepares the data for the comparison radar chart.

    Returns:
        Tuple of (labels, data) where:
        - labels: List of criterion names
        - data: Dictionary with 2-3 series:
            * Current period label (e.g., "Nov/2025"): Individual current values
            * "Team Average": Team current values
            * "Historical Avg": Historical averages (only if historical data exists)
    """
```

**Implementation Changes:**

**Before:**
```python
data = {
    "Individual": individual_values,
    "Team Average": team_values,
}
```

**After:**
```python
# Get historical averages
historical_averages = self._get_historical_criteria_averages()

# Build data dictionary
data = {
    self.current_period_label: individual_values,  # e.g., "Nov/2025"
    "Team Average": team_values,
}

# Add historical series only if historical data exists
if historical_averages:
    historical_values = []
    for criterion in labels:
        hist_value = historical_averages.get(criterion, 0)
        historical_values.append(hist_value)
    data["Historical Avg"] = historical_values
```

**Key Points:**
- Use `self.current_period_label` instead of "Individual" for clarity
- Only add "Historical Avg" series if data exists
- Maintain same criterion order as labels
- Add filter for `_period_metadata` in the loop

**Complete Implementation:**
```python
def _get_comparison_radar_data(self) -> tuple[list[str], dict[str, list[float]]]:
    """Prepares the data for the comparison radar chart."""
    labels = []
    individual_values = []
    team_values = []

    # Get historical averages
    historical_averages = self._get_historical_criteria_averages()

    # Process current period criteria
    for criterion, criterion_stats in self.individual_data.criteria_stats.items():
        # Skip metadata entries
        if criterion == "_period_metadata":
            continue

        labels.append(criterion)
        individual_values.append(criterion_stats.get("average", 0))
        team_values.append(self.team_data.criteria_stats.get(criterion, {}).get("average", 0))

    # Build data dictionary with current values
    data = {
        self.current_period_label: individual_values,
        "Team Average": team_values,
    }

    # Add historical series only if historical data exists
    if historical_averages:
        historical_values = []
        for criterion in labels:
            hist_value = historical_averages.get(criterion, 0)
            historical_values.append(hist_value)
        data["Historical Avg"] = historical_values

    return labels, data
```

### Step 3: Test and Validate

**Test Scenarios:**

1. **Member with historical data** (e.g., Andrea):
   - ✅ Radar shows 3 series
   - ✅ Historical Avg values are correct
   - ✅ Legend displays all 3 series
   - ✅ Colors are distinct

2. **Member without historical data** (e.g., new member):
   - ✅ Radar shows 2 series (current behavior maintained)
   - ✅ No errors or warnings
   - ✅ Chart renders correctly

3. **Edge cases:**
   - ✅ _period_metadata is filtered out
   - ✅ Missing criteria handled gracefully
   - ✅ Empty historical data handled correctly

**Validation Command:**
```bash
python src/main.py syngenta team_assessment generate_assessment \
  --competencyMatrixFile "path/to/matrix.xlsx" \
  --feedbackFolder "path/to/feedback" \
  --planningFile "path/to/planning.xlsm" \
  --outputFolder ./output/test \
  --ignoredMembers ./ignored_members.json
```

**Expected Andrea's Radar Values:**
```
Criterion           Current  Historical  Team
-------------------------------------------------
Delivery Skills     3.22     3.11        3.15
Soft Skills         3.67     3.47        3.50
Technical Skills    4.11     3.78        3.85
Values/Behaviors    2.78     2.92        2.80
```

## Visual Design Specifications

### Chart Configuration
- **Chart Type:** Polar/Radar chart
- **Figure Size:** Current size maintained (defined in parent class)
- **Series Count:** 2-3 depending on data availability

### Series Styling (Suggested)
1. **Current Period** (Blue)
   - Label: Dynamic (e.g., "Nov/2025")
   - Line: Solid, Width: 2
   - Marker: Circle

2. **Team Average** (Orange)
   - Label: "Team Average"
   - Line: Solid, Width: 2
   - Marker: Circle

3. **Historical Avg** (Green) ⭐ NEW
   - Label: "Historical Avg"
   - Line: Dashed, Width: 2
   - Marker: Triangle
   - Alpha: 0.7 (slightly transparent)

### Legend
- Position: Best fit (automatic)
- Font size: 10pt
- Show all series

## Example Output

**Andrea's Radar Chart:**

```
          Delivery Skills (3.22 / 3.11 / 3.15)
                    /\
                   /  \
                  /    \
Technical (4.11) /      \ Soft (3.67)
                /        \
               /          \
              /            \
             /______________\
          Values/Behaviors (2.78)

Legend:
● Nov/2025 (Blue solid)
● Team Average (Orange solid)
▲ Historical Avg (Green dashed)
```

**Interpretation:**
- Andrea's Technical Skills (4.11) are **above** her historical average (3.78) → Improvement
- Values/Behaviors (2.78) are **below** historical average (2.92) → Area of focus

## Benefits

1. **Temporal Context** - See if current performance is consistent with past
2. **Trend Identification** - Quickly spot improvements or regressions
3. **Personalized Baseline** - Compare against own history, not just team
4. **Actionable Insights** - Identify criteria needing attention

## Code Changes Summary

### Files Modified
- `src/domains/syngenta/team_assessment/services/member_analyzer.py`

### Methods Added (1)
- `_get_historical_criteria_averages()` - ~25 lines

### Methods Modified (1)
- `_get_comparison_radar_data()` - ~40 lines (from 15)

### Total Lines Added
- Approximately 50-60 lines

## Rollback Plan

If issues arise:
1. Remove "Historical Avg" series from `data` dictionary
2. Remove call to `_get_historical_criteria_averages()`
3. Keep helper method for potential future use

## Success Criteria

✅ Historical series appears on radar when historical data exists
✅ Historical series is omitted when no historical data exists
✅ Values are mathematically correct (mean of all historical periods)
✅ No errors for members without historical data
✅ _period_metadata is properly filtered
✅ Chart legend displays all series clearly
✅ Visual styling is consistent and professional

## Estimated Effort

- **Implementation:** 30-45 minutes
- **Testing:** 15-20 minutes
- **Total:** ~1 hour

## Risk Assessment

- **Risk Level:** LOW
- **Impact:** Cosmetic enhancement (no data model changes)
- **Reversibility:** HIGH (easy to rollback)

## Notes for Claude Agent

- Reuse existing `_calculate_period_statistics()` method for consistency
- Ensure `_period_metadata` filtering is applied
- Test with Andrea's data (has 2 historical periods)
- Maintain backward compatibility (2 series when no history)
- Follow existing code style (docstrings, type hints)
- Add INFO-level logging if helpful for debugging

## References

### Similar Implementation
- `src/domains/syngenta/team_assessment/services/member_analyzer.py`
  - Method: `_get_historical_criteria_data()` (lines 235-285)
  - Shows pattern for processing historical data

### Data Model
- `src/domains/syngenta/team_assessment/core/statistics.py`
  - Class: `IndividualStatistics`
  - Field: `historical_evaluations` (line 91)

### Example Data Location
- `/Users/vitormendonca/git-pessoal/python/PyToolkit/output/2025_assessment/members/Andrea/stats.json`
  - Contains sample historical data for validation
