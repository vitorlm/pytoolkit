from __future__ import annotations

import os
from argparse import ArgumentParser, Namespace
from typing import Dict, List

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_datadog_env_loaded
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager

from domains.syngenta.datadog.datadog_service import DatadogService


class ListTeamsCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "list-teams"

    @staticmethod
    def get_description() -> str:
        return "List Datadog teams (handle, name, id) with optional search and export."

    @staticmethod
    def get_help() -> str:
        return (
            "Lists teams from Datadog Teams API v2. Supports optional search filtering, "
            "console output, and JSON/Markdown export."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--search",
            type=str,
            required=False,
            help="Optional text to filter teams (by handle/name).",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=500,
            help="Maximum number of teams to list (default: 500).",
        )
        parser.add_argument(
            "--site",
            type=str,
            required=False,
            help="Datadog site (e.g., datadoghq.eu). Defaults to $DD_SITE or datadoghq.eu.",
        )
        parser.add_argument(
            "--out",
            type=str,
            choices=["console", "json", "md"],
            default="console",
            help="Output format: console (default), json, or md.",
        )
        parser.add_argument(
            "--output-file",
            type=str,
            required=False,
            help="Optional custom output file path (overrides default location).",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_datadog_env_loaded()
        logger = LogManager.get_instance().get_logger("ListTeamsCommand")

        site = args.site or os.getenv("DD_SITE") or "datadoghq.eu"
        try:
            service = DatadogService(site=site)
            teams = service.list_teams(query=args.search, limit=args.limit)

            payload: Dict[str, object] = {
                "site": site,
                "search": args.search or "",
                "count": len(teams),
                "teams": teams,
            }

            ListTeamsCommand._print_summary(payload)

            if args.out == "console":
                ListTeamsCommand._print_table(teams)
                return

            # Save to file
            from datetime import datetime as _dt

            sub_dir = f"datadog-teams_{_dt.now().strftime('%Y%m%d')}"
            base = "datadog_teams"
            if args.out == "json":
                out_path = (
                    args.output_file
                    if args.output_file
                    else OutputManager.get_output_path(sub_dir, base, "json")
                )
                print(f"\nOutput file:\n- {out_path}")
                OutputManager.save_json_report(
                    payload, sub_dir, base, output_path=out_path
                )
                print("✅ Teams list saved in JSON format")
            else:
                md = ListTeamsCommand._to_markdown(payload)
                out_path = (
                    args.output_file
                    if args.output_file
                    else OutputManager.get_output_path(sub_dir, base, "md")
                )
                print(f"\nOutput file:\n- {out_path}")
                OutputManager.save_markdown_report(
                    md, sub_dir, base, output_path=out_path
                )
                print("📄 Teams list saved in MD format")

        except Exception as e:
            logger.error(f"Failed to list teams: {e}")
            print("\n====================================================")
            print("❌ FAILED TO LIST DATADOG TEAMS")
            print("====================================================")
            print(f"Error: {e}")
            print("Hints:")
            print("- Verify network access to api.datadoghq.eu (or site configured)")
            print("- Check DD_SITE, DD_API_KEY, DD_APP_KEY values")
            print("- Try --site datadoghq.eu or export DD_SITE=datadoghq.eu")
            exit(1)

    @staticmethod
    def _print_summary(payload: Dict[str, object]) -> None:
        site = payload.get("site", "-")
        count = payload.get("count", 0)
        search = payload.get("search") or ""
        header = f"📡 DATADOG TEAMS (site: {site})"
        print("\n" + "=" * len(header))
        print(header)
        print("=" * len(header))
        print("\nEXECUTIVE SUMMARY:")
        if search:
            print(f"- Filter: '{search}'")
        print(f"- Teams found: {count}")
        print("=" * len(header))

    @staticmethod
    def _print_table(teams: List[Dict[str, str]]) -> None:
        if not teams:
            print("No teams found.")
            return
        print("\nHandle                          | Name")
        print(
            "--------------------------------|----------------------------------------------"
        )
        for t in teams[:100]:
            handle = (t.get("handle") or "")[:30]
            name = (t.get("name") or "")[:46]
            print(f"{handle:<30} | {name}")

    @staticmethod
    def _to_markdown(payload: Dict[str, object]) -> str:
        site = payload.get("site", "-")
        count = payload.get("count", 0)
        search = payload.get("search") or ""
        teams: List[Dict[str, str]] = payload.get("teams", [])  # type: ignore[assignment]
        lines: List[str] = []
        lines.append("## Datadog Teams")
        lines.append("")
        lines.append(f"- Site: `{site}`")
        if search:
            lines.append(f"- Filter: `{search}`")
        lines.append(f"- Teams found: {count}")
        lines.append("")
        lines.append("| Handle | Name | ID |")
        lines.append("|---|---|---|")
        for t in teams:
            lines.append(
                f"| {t.get('handle', '')} | {t.get('name', '')} | {t.get('id', '')} |"
            )
        return "\n".join(lines)
