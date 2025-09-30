from __future__ import annotations

import os
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from typing import Dict, List

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_datadog_env_loaded
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager

from domains.syngenta.datadog.datadog_service import DatadogService


class TeamsServicesCommand(BaseCommand):
    """
    Datadog Audit MVP — List teams and their services from Service Catalog (v2).

    Input: --teams "teamA,teamB" and optional --site, --use-cache, --out (json|md)
    Output: JSON (default) or Markdown tables, saved under output/ with timestamp.
    """

    @staticmethod
    def get_name() -> str:
        return "teams-services"

    @staticmethod
    def get_description() -> str:
        return "List Datadog services for given team handles (Service Catalog)."

    @staticmethod
    def get_help() -> str:
        return (
            "Given one or more Datadog team handles, validates teams and enumerates services "
            "from the Service Catalog, flagging missing team linkage when applicable."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--teams",
            type=str,
            required=True,
            help=(
                'Comma-separated list of Datadog team handles, e.g. "cropwise-core-services-catalog,'
                'cropwise-core-services-identity"'
            ),
        )
        parser.add_argument(
            "--site",
            type=str,
            required=False,
            default=None,
            help="Datadog site (e.g., datadoghq.eu, us3.datadoghq.eu). Defaults to $DD_SITE or datadoghq.eu.",
        )
        parser.add_argument(
            "--use-cache",
            action="store_true",
            help="Enable caching for 30 minutes to reduce API calls.",
        )
        parser.add_argument(
            "--out",
            type=str,
            required=False,
            choices=["console", "json", "md"],
            default="console",
            help="Output format: console (default), json or md.",
        )
        parser.add_argument(
            "--output-file",
            type=str,
            required=False,
            help="Optional custom output file path (overrides default location).",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Print extra debugging about team resolution and service discovery.",
        )
        parser.add_argument(
            "--no-validate-teams",
            action="store_true",
            help="Do not validate team handles via Teams API; proceed to service discovery.",
        )

    @staticmethod
    def main(args: Namespace):
        # Ensure environment and required secrets are present
        ensure_datadog_env_loaded()  # load .env in this domain and check secrets

        logger = LogManager.get_instance().get_logger("TeamsServicesCommand")

        try:
            teams = TeamsServicesCommand._parse_teams_arg(args.teams)
            if not teams:
                logger.error("No valid team handles provided via --teams")
                exit(1)

            site = args.site or os.getenv("DD_SITE") or "datadoghq.eu"
            service = DatadogService(
                site=site,
                api_key=os.getenv("DD_API_KEY"),
                app_key=os.getenv("DD_APP_KEY"),
                use_cache=bool(args.use_cache),
                cache_ttl_minutes=30,
            )

            if args.verbose:
                print("[debug] Listing available team handles (first 50)...")
                try:
                    sample = service.list_team_handles(limit=50)
                    for t in sample:
                        print(f"   - {t.get('handle')}  [{t.get('name', '')}]")
                except Exception as _e:
                    print(f"[debug] Unable to list team handles: {_e}")

            result = {
                "site": site,
                "queried_teams": teams,
                "teams": [],
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

            valid_teams = 0
            total_services = 0
            total_missing = 0

            for handle in teams:
                info = None if args.no_validate_teams else service.get_team(handle)
                if not info and not args.no_validate_teams:
                    logger.warning(
                        f"Team handle not found or inaccessible: {handle}. Skipping."
                    )
                    if args.verbose:
                        print(f"[debug] Team not resolved: {handle}")
                    continue
                if info:
                    valid_teams += 1

                services = service.list_services_for_team(handle)
                # Filter services actually belonging to the team and mark missing linkage
                normalized, missing_count = (
                    TeamsServicesCommand._normalize_services_for_team(handle, services)
                )
                total_services += len(normalized)
                total_missing += missing_count
                if args.verbose:
                    ok_count = sum(1 for s in normalized if s.get("team_link_ok"))
                    miss_count = sum(1 for s in normalized if not s.get("team_link_ok"))
                    print(
                        f"[debug] {handle}: services={len(normalized)}, ok={ok_count}, missing_linkage={miss_count}"
                    )

                team_entry: Dict[str, object] = {
                    "team": handle,
                    "services": normalized,
                    "notes": [f"{missing_count} services missing team linkage"]
                    if missing_count
                    else ["0 services missing team linkage"],
                }
                result["teams"].append(team_entry)

            # Always print a concise Executive Summary to console
            TeamsServicesCommand._print_executive_summary(
                site=site,
                queried=len(teams),
                valid=valid_teams,
                total_services=total_services,
                total_missing=total_missing,
                per_team=result["teams"],
            )

            # Persist output only if requested (json or md)
            if args.out in ("json", "md"):
                from datetime import datetime as _dt

                sub_dir = f"datadog-teams-services_{_dt.now().strftime('%Y%m%d')}"
                output_basename = "datadog_teams_services"
                if args.out == "json":
                    out_path = (
                        args.output_file
                        if args.output_file
                        else OutputManager.get_output_path(
                            sub_dir, output_basename, "json"
                        )
                    )
                    print(f"\nOutput file:\n- {out_path}")
                    OutputManager.save_json_report(
                        result, sub_dir, output_basename, output_path=out_path
                    )
                    result["output_file"] = out_path
                    print("✅ Detailed report saved in JSON format")
                else:
                    md_content = TeamsServicesCommand._to_markdown(result)
                    out_path = (
                        args.output_file
                        if args.output_file
                        else OutputManager.get_output_path(
                            sub_dir, output_basename, "md"
                        )
                    )
                    print(f"\nOutput file:\n- {out_path}")
                    OutputManager.save_markdown_report(
                        md_content, sub_dir, output_basename, output_path=out_path
                    )
                    result["output_file"] = out_path
                    print("📄 Detailed report saved in MD format")

        except RuntimeError as e:
            logger.error(str(e))
            print("\n====================================================")
            print("❌ FAILED TO FETCH DATADOG TEAMS/SERVICES")
            print("====================================================")
            print(f"Error: {e}")
            print("Hints:")
            print("- Verify network access to api.<site> (e.g., api.datadoghq.eu)")
            print("- Check DD_SITE, DD_API_KEY, DD_APP_KEY values")
            print("- Try --verbose or --no-validate-teams for diagnostics")
            exit(1)
        except Exception as e:
            logger.error(f"Failed to fetch teams/services: {e}")
            print("\n====================================================")
            print("❌ FAILED TO FETCH DATADOG TEAMS/SERVICES")
            print("====================================================")
            print(f"Error: {e}")
            print("Hints:")
            print("- Verify network access to api.<site> (e.g., api.datadoghq.eu)")
            print("- Check DD_SITE, DD_API_KEY, DD_APP_KEY values")
            print("- Try --verbose or --no-validate-teams for diagnostics")
            exit(1)

    # ---- helpers (also used by smoke tests) ----
    @staticmethod
    def _parse_teams_arg(raw: str) -> List[str]:
        return [t.strip() for t in (raw or "").split(",") if t and t.strip()]

    @staticmethod
    def _normalize_services_for_team(
        team_handle: str, services: List[Dict]
    ) -> tuple[List[Dict], int]:
        normalized: List[Dict] = []
        missing = 0
        for svc in services or []:
            name = svc.get("name")
            sid = svc.get("id")
            links = svc.get("links") or {}
            contacts = svc.get("contacts") or []
            ok = bool(svc.get("team_link_ok")) and (
                str(svc.get("team") or "").lower() == team_handle.lower()
                or svc.get("team_link_ok") is True
            )
            if not ok:
                missing += 1
            normalized.append(
                {
                    "name": name,
                    "id": sid,
                    "team_link_ok": bool(ok),
                    "contacts": contacts,
                    "links": links,
                }
            )
        return normalized, missing

    @staticmethod
    def _to_markdown(payload: Dict) -> str:
        lines: List[str] = []
        site = payload.get("site", "-")
        generated_at = payload.get("generated_at", "")
        teams = payload.get("teams", [])

        # Executive Summary
        total_services = sum(len(t.get("services", [])) for t in teams)
        total_missing = 0
        for t in teams:
            for s in t.get("services", []):
                if not s.get("team_link_ok"):
                    total_missing += 1

        lines.append("## Executive Summary")
        lines.append("")
        lines.append(f"- Site: `{site}`")
        lines.append(f"- Teams processed: {len(teams)}")
        lines.append(f"- Services discovered: {total_services}")
        lines.append(f"- Services missing team linkage: {total_missing}")
        if generated_at:
            lines.append(f"- Generated at: {generated_at}")
        lines.append("")

        # Detailed per-team sections
        lines.append(f"## Teams → Services (site: {site})\n")
        for team in payload.get("teams", []):
            team_handle = team.get("team")
            notes = team.get("notes", [])
            lines.append(f"### Team: {team_handle}")
            if notes:
                for n in notes:
                    lines.append(f"- Note: {n}")
            lines.append("")
            lines.append("| Service Name | Service ID | Links | Notes |")
            lines.append("|---|---|---|---|")
            for svc in team.get("services", []):
                name = svc.get("name", "-")
                sid = svc.get("id", "-")
                team_ok = svc.get("team_link_ok", False)
                links = svc.get("links") or {}
                link_parts = []
                for lk, url in (links or {}).items():
                    if isinstance(url, str) and url:
                        link_parts.append(f"[{lk}]({url})")
                links_md = ", ".join(link_parts) if link_parts else "-"
                note = "" if team_ok else "missing team linkage"
                lines.append(f"| {name} | {sid} | {links_md} | {note} |")
            lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _print_executive_summary(
        *,
        site: str,
        queried: int,
        valid: int,
        total_services: int,
        total_missing: int,
        per_team: List[Dict],
    ) -> None:
        header = f"📡 DATADOG TEAMS → SERVICES (site: {site})"
        print("\n" + "=" * len(header))
        print(header)
        print("=" * len(header))
        print("\nEXECUTIVE SUMMARY:")
        print(f"- Teams (queried/valid): {queried}/{valid}")
        print(f"- Services discovered: {total_services}")
        print(f"- Missing team linkage: {total_missing}")
        # Top-level per-team one-liners
        for t in per_team:
            name = t.get("team")
            svc_count = len(t.get("services", []))
            notes = ", ".join(t.get("notes", []) or [])
            print(f"  • {name}: {svc_count} services ({notes})")
        print("=" * len(header))
