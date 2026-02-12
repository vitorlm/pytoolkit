# Execution Prompt for Member Evolution Charts Implementation

Please read and execute the implementation plan located at:
`/Users/vitormendonca/git-pessoal/python/PyToolkit/implementation_plan_member_evolution_charts.md`

## Your Task

Implement temporal evolution charts for individual team members in the Assessment Generator system by following the detailed plan in the file above.

## Instructions

1. **Read the plan thoroughly** - The markdown file contains:
   - Context and objectives
   - Data structure specifications
   - Step-by-step implementation guide with method signatures
   - Testing scenarios and success criteria

2. **Follow the implementation steps in order** (Steps 1-6):
   - Step 1: Add helper method to extract historical data
   - Step 2: Implement criteria evolution line chart
   - Step 3: Implement overall evolution chart
   - Step 4: Update plot_all_charts() method
   - Step 5: Add calculation helper
   - Step 6: Test and validate

3. **Key requirements**:
   - All changes go in: `src/domains/syngenta/team_assessment/services/member_analyzer.py`
   - Follow existing code style and patterns
   - Use the `TeamAnalyzer.plot_temporal_evolution()` method as reference (lines 174-297)
   - Handle edge cases gracefully (no historical data, missing periods, etc.)
   - Add proper logging at INFO level
   - Fail gracefully when data is missing

4. **Expected outcome**:
   - 2 new chart methods added to MemberAnalyzer class
   - Charts generated only when historical data exists
   - No errors when historical data is absent
   - Total of 5 charts per member (3 existing + 2 new)

5. **Testing**:
   - After implementation, suggest running the assessment command with historical data to verify charts are generated
   - Check that both new PNG files appear in member output folders

## Context

The current `MemberAnalyzer` generates 3 charts showing only current period data. The `IndividualStatistics` object already contains historical evaluation data in the `historical_evaluations` field, but this data is not visualized. Your task is to add temporal evolution charts similar to what `TeamAnalyzer` already does, but at the individual member level.

## Notes

- The implementation should take approximately 2-3 hours
- Estimated code addition: 150-200 lines
- Reference file for similar patterns: `src/domains/syngenta/team_assessment/services/team_analyzer.py`
- Data model reference: `src/domains/syngenta/team_assessment/core/statistics.py` (IndividualStatistics class, line 80)

## Important

Read the entire plan before starting implementation. The plan contains detailed specifications, method signatures, data structures, and edge cases to consider. Following the plan step-by-step will ensure a clean, maintainable implementation.

---

Begin by reading the plan file, then proceed with the implementation. Use the TodoWrite tool to track your progress through the 6 steps.
