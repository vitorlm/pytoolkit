# Datadog Teams → Services Audit (MVP)

Small CLI to validate Datadog Teams and enumerate their Service Catalog services.

- Verifies each provided team handle exists (Teams v2)
- Lists services (Service Definition v2) and flags "missing team linkage"
- Outputs JSON (default) or Markdown into `output/`

## Usage

1) Env vars (required)

- `DD_API_KEY`
- `DD_APP_KEY`
- `DD_SITE` (optional; can also pass `--site`)

You can configure env vars via a domain-specific `.env` file. The CLI auto-loads `src/domains/syngenta/datadog/.env`.

- Start from the example and fill your keys:

```
cp src/domains/syngenta/datadog/.env.example src/domains/syngenta/datadog/.env
```

- Or export them in your shell environment:

```
export DD_SITE=us3.datadoghq.com
export DD_API_KEY=xxxx
export DD_APP_KEY=yyyy
```

2) Run

- JSON (default):

```
python src/main.py syngenta datadog teams-services \
  --teams "cropwise-core-services-catalog,cropwise-core-services-identity"
```

- Markdown:

```
python src/main.py syngenta datadog teams-services \
  --teams "cropwise-core-services-da-backbone,cropwise-unified-platform" \
  --out md
```

- Optional flags:
  - `--site us3.datadoghq.com` (overrides `DD_SITE`)
  - `--use-cache` enables 30-minute caching via CacheManager
  - `--out json|md` selects output format

## Output

- JSON: `output/datadog_teams_services_<timestamp>.json`
- Markdown: `output/datadog_teams_services_<timestamp>.md`

Example JSON structure:

```
{
  "site": "us3.datadoghq.com",
  "queried_teams": ["teamA", "teamB"],
  "teams": [
    {
      "team": "teamA",
      "services": [
        {
          "name": "agops-api",
          "id": "...",
          "team_link_ok": true,
          "contacts": [],
          "links": {"docs": "https://...", "repo": "https://..."}
        }
      ],
      "notes": ["0 services missing team linkage"]
    }
  ],
  "generated_at": "2025-01-01T00:00:00+00:00"
}
```

## Installation

This command uses the official Datadog Python client:

```
pip install datadog-api-client
```

If you manage dependencies via `requirements.txt`, add:

```
datadog-api-client>=2.26.0
```

## Notes

- Pagination is handled for Teams and Service Definitions (page size 100).
- Team linkage is determined by `service.team == <handle>` or fallback tag `team:<handle>`.
- Invalid team handles are logged and skipped; the command exits non-zero on fatal errors (auth, network).
- The CLI automatically loads `src/domains/syngenta/datadog/.env` (if present) and validates `DD_API_KEY`/`DD_APP_KEY`.
