from __future__ import annotations

import os
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, TypedDict, cast

import requests
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_datadog_env_loaded
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager

from .datadog_software_service import DatadogSoftwareService


class TeamStats(TypedDict):
    count: int


class ServiceEntry(TypedDict, total=False):
    id: Optional[str]
    name: Optional[str]
    owner: str
    links: Dict[str, str]
    env_facets: List[str]
    notes: List[str]


class TeamReport(TypedDict):
    team: str
    services: List[ServiceEntry]
    stats: TeamStats


class ReportPayload(TypedDict):
    site: str
    queried_teams: List[str]
    teams: List[TeamReport]
    generated_at: str


class TeamSummary(TypedDict):
    team: str
    count: int


class TeamsSoftwareCommand(BaseCommand):
    """
    Datadog Software Catalog — List kind=service entities owned by teams.

    Input: --teams "teamA,teamB" and optional --site, --use-cache, --out (json|md)
    Behavior:
      1) Validate team handles via Teams API v2 when possible
      2) For each team, list Software Catalog entities with filter[kind]=service
         - Prefer server-side filter[owner]=<team>
         - Fallback to client-side filtering if owner filter is unavailable/incomplete
      3) Paginate until all entities are retrieved
      4) Produce mapping: team -> [services] with id, name, owner, links, env_facets
    """

    @staticmethod
    def get_name() -> str:
        return "teams-software"

    @staticmethod
    def get_description() -> str:
        return "List Software Catalog services owned by the specified Datadog teams."

    @staticmethod
    def get_help() -> str:
        return (
            "Given one or more Datadog team handles, fetches kind=service entities from the "
            "Software Catalog (v2), using server-side owner filtering when available and "
            "falling back to client-side filtering if needed. Outputs JSON or Markdown."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--teams",
            type=str,
            required=True,
            help=(
                "Comma-separated Datadog team handles, e.g. "
                '"cropwise-core-services-catalog,cropwise-core-services-identity"'
            ),
        )
        parser.add_argument(
            "--site",
            type=str,
            required=False,
            default=None,
            help="Datadog site (e.g., datadoghq.com, us3.datadoghq.com, eu.datadoghq.eu).",
        )
        parser.add_argument(
            "--use-cache",
            action="store_true",
            help="Enable 30-minute caching via CacheManager.",
        )
        parser.add_argument(
            "--out",
            type=str,
            required=False,
            choices=["json", "md"],
            default="json",
            help="Output format: json (default) or md.",
        )
        parser.add_argument(
            "--output-file",
            type=str,
            required=False,
            help="Optional explicit output file path (overrides default).",
        )
        parser.add_argument(
            "--no-validate-teams",
            action="store_true",
            help="Skip validating team handles via Teams API (restricted tenants).",
        )

    @staticmethod
    def main(args: Namespace):
        # Ensure env and required keys are available
        ensure_datadog_env_loaded()
        logger = LogManager.get_instance().get_logger("TeamsSoftwareCommand")

        try:
            teams = TeamsSoftwareCommand._parse_teams_arg(args.teams)
            if not teams:
                logger.error("No valid team handles provided via --teams")
                exit(1)

            site = args.site or os.getenv("DD_SITE") or "datadoghq.com"
            service = DatadogSoftwareService(
                site=site,
                api_key=os.getenv("DD_API_KEY"),
                app_key=os.getenv("DD_APP_KEY"),
                use_cache=bool(args.use_cache),
                cache_ttl_minutes=30,
            )

            teams_payload: List[TeamReport] = []
            payload: ReportPayload = {
                "site": site,
                "queried_teams": list(teams),
                "teams": teams_payload,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

            total_services = 0
            teams_found = 0
            fallbacks_used: List[str] = []
            per_team_stats: List[TeamSummary] = []

            for handle in teams:
                team_info = None if args.no_validate_teams else service.get_team(handle)
                if team_info is None and not args.no_validate_teams:
                    logger.warning(f"Team not found or inaccessible: {handle}. Continuing with entity discovery.")
                else:
                    teams_found += 1

                services_raw, meta_raw = service.list_services_for_team(handle)
                services = cast(List[Mapping[str, Any]], services_raw)
                meta = cast(Dict[str, Any], meta_raw)
                if meta.get("fallback") is not None:
                    fallbacks_used.append(str(meta["fallback"]))

                normalized: List[ServiceEntry] = []
                for service_record in services:
                    identifier = service_record.get("id")
                    service_id = str(identifier) if identifier is not None else None

                    name_value = service_record.get("name")
                    service_name = str(name_value) if isinstance(name_value, str) and name_value else None

                    owner_value = service_record.get("owner")
                    owner = str(owner_value) if isinstance(owner_value, str) and owner_value else handle

                    raw_links = service_record.get("links")
                    links: Dict[str, str] = {}
                    if isinstance(raw_links, Mapping):
                        links = {
                            str(key): str(value)
                            for key, value in raw_links.items()
                            if isinstance(key, str) and isinstance(value, str)
                        }

                    raw_env_facets = service_record.get("env_facets")
                    env_facets: List[str] = []
                    if isinstance(raw_env_facets, list):
                        env_facets = [str(item) for item in raw_env_facets if isinstance(item, str)]

                    raw_notes = service_record.get("notes")
                    notes: List[str] = []
                    if isinstance(raw_notes, list):
                        notes = [str(note) for note in raw_notes if isinstance(note, str)]

                    normalized.append(
                        {
                            "id": service_id,
                            "name": service_name or service_id,
                            "owner": owner,
                            "links": links,
                            "env_facets": env_facets,
                            "notes": notes,
                        }
                    )

                total_services += len(normalized)
                team_entry: TeamReport = {
                    "team": handle,
                    "services": normalized,
                    "stats": {"count": len(normalized)},
                }
                payload["teams"].append(team_entry)
                per_team_stats.append({"team": handle, "count": len(normalized)})

            # Summary logs
            TeamsSoftwareCommand._print_executive_summary(
                site=site,
                queried=len(teams),
                valid=teams_found,
                total_services=total_services,
                fallbacks=list(sorted(set(fallbacks_used))) if fallbacks_used else [],
                per_team=per_team_stats,
            )

            # Persist output (JSON or Markdown)
            sub_dir = f"datadog-teams-software_{datetime.now().strftime('%Y%m%d')}"
            base = "datadog_teams_services"
            if args.out == "json":
                out_path = args.output_file or OutputManager.get_output_path(sub_dir, base, "json")
                print(f"\nOutput file:\n- {out_path}")
                OutputManager.save_json_report(payload, sub_dir, base, output_path=out_path)
                print("✅ Report saved in JSON format")
            else:
                md = TeamsSoftwareCommand._to_markdown(payload)
                out_path = args.output_file or OutputManager.get_output_path(sub_dir, base, "md")
                print(f"\nOutput file:\n- {out_path}")
                OutputManager.save_markdown_report(md, sub_dir, base, output_path=out_path)
                print("📄 Report saved in MD format")

        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            logger.error(f"HTTP error from Datadog API: {status} {e}")
            print("\n====================================================")
            print("❌ FAILED: Datadog API HTTP Error")
            print("====================================================")
            print(f"Status: {status}")
            if status == 401:
                print("Action: Check DD_API_KEY/DD_APP_KEY")
            elif status in (403,):
                print("Action: Ensure app key has permissions for Teams/Software Catalog APIs")
            elif status in (404,):
                print("Action: Verify endpoint/site and team handles")
            else:
                print("Action: Re-run with correct site or try later")
            exit(1)
        except Exception as e:
            logger.error(f"Failed to fetch teams/software entities: {e}")
            print("\n====================================================")
            print("❌ FAILED TO FETCH DATADOG SOFTWARE ENTITIES")
            print("====================================================")
            print(f"Error: {e}")
            print("Hints:")
            print("- Verify network access to api.<site> (e.g., api.datadoghq.com)")
            print("- Check DD_SITE, DD_API_KEY, DD_APP_KEY values")
            print("- If Teams read is restricted, try --no-validate-teams")
            exit(1)

    # ---- helpers ----
    @staticmethod
    def _parse_teams_arg(raw: str) -> List[str]:
        parts = [t.strip() for t in (raw or "").split(",") if t and t.strip()]
        # Deduplicate preserving order
        seen = set()
        out: List[str] = []
        for p in parts:
            if p not in seen:
                seen.add(p)
                out.append(p)
        return out

    @staticmethod
    def _to_markdown(payload: ReportPayload) -> str:
        lines: List[str] = []
        site = payload["site"]
        teams = payload["teams"]

        total_services = sum(team["stats"]["count"] for team in teams)
        lines.append("## Datadog Teams → Software Services")
        lines.append("")
        lines.append(f"- Site: `{site}`")
        lines.append(f"- Teams processed: {len(teams)}")
        lines.append(f"- Services discovered: {total_services}")
        lines.append("")

        for team_report in teams:
            team_handle = team_report["team"]
            lines.append(f"### Team: {team_handle}")
            lines.append("")
            lines.append("| Service | Owner | Links | Notes |")
            lines.append("|---|---|---|---|")
            for service_entry in team_report["services"]:
                resolved_name = service_entry.get("name") or service_entry.get("id") or "-"
                owner = service_entry.get("owner") or team_handle
                links = service_entry.get("links", {})
                link_md_parts: List[str] = []
                for key in ("docs", "repo", "runbook"):
                    url = links.get(key)
                    if isinstance(url, str) and url:
                        link_md_parts.append(f"[{key}]({url})")
                links_md = ", ".join(link_md_parts) if link_md_parts else "-"
                notes_list = service_entry.get("notes", [])
                notes_text = ", ".join(notes_list) if notes_list else ""
                lines.append(f"| {resolved_name} | {owner} | {links_md} | {notes_text} |")
            lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _print_executive_summary(
        *,
        site: str,
        queried: int,
        valid: int,
        total_services: int,
        fallbacks: List[str],
        per_team: List[TeamSummary],
    ):
        header = f"📡 DATADOG TEAMS → SOFTWARE (site: {site})"
        print("\n" + "=" * len(header))
        print(header)
        print("=" * len(header))
        print("\nEXECUTIVE SUMMARY:")
        print(f"- Teams (queried/validated): {queried}/{valid}")
        print(f"- Services discovered: {total_services}")
        if fallbacks:
            print(f"- Fallbacks used: {', '.join(fallbacks)}")
        # Per-team one-liners
        for row in per_team:
            team = row["team"]
            count = row["count"]
            print(f"  • {team}: {count} services")
        print("=" * len(header))
