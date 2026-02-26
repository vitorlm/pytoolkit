# Team Assessment Method (BMAD Migration Scaffold)

This repository root is prepared as a standalone workspace for the Team Assessment Method migration to BMAD format.

## Purpose

- Host BMAD-compatible assets under `_bmad/` and `core/`
- Keep runtime scripts and tests for assessment execution
- Preserve PyToolkit source artifacts while migration is executed by phases

## Project Layout (Phase 0)

```text
_bmad/
  agents/
  workflows/
  modules/
core/
  agents/
  workflows/
  schemas/
  policies/
  rubrics/
  templates/
  checklists/
_config/
scripts/
runtime/scripts/
tests/
runs/
examples/
providers/
_team_assessment_output/
```

## Tooling

- Python: `3.13`
- Node.js + npm: required for BMAD CLI
- BMAD install command: `npx bmad-method install`

## Notes

- Migration policy is COPY/CREATE only; avoid deleting unrelated files.
- Do not modify source data under `_team_assessment_method`.
- Runtime output is written to `_team_assessment_output/` and ignored by git.
