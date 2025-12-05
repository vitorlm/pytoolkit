from __future__ import annotations

import os
from argparse import ArgumentParser, Namespace
from datetime import datetime, timedelta
from typing import Any

from domains.syngenta.datadog.hits_by_endpoint_service import HitsByEndpointService
from domains.syngenta.datadog.summary.datadog_summary_manager import (
    DatadogSummaryManager,
)
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_datadog_env_loaded, ensure_env_loaded
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager


class HitsByEndpointCommand(BaseCommand):
    """Query Datadog for trace.express.request.hits grouped by endpoint."""

    @staticmethod
    def get_name() -> str:
        return "hits-by-endpoint"

    @staticmethod
    def get_description() -> str:
        return "Retrieve hits per Express.js endpoint for a given service and environment"

    @staticmethod
    def get_help() -> str:
        return """
        Queries Datadog for trace.express.request.hits metrics grouped by resource_name (endpoint).
        Produces console output, Markdown report, JSON export, and standardized Summary JSON.

        Examples:
            python src/main.py syngenta datadog hits-by-endpoint \\
                --service cropwise-catalog-products-api \\
                --env prod \\
                --last-n-days 60 \\
                --granularity 12h \\
                --top 25

            python src/main.py syngenta datadog hits-by-endpoint \\
                --service my-api \\
                --env staging \\
                --from 2025-01-01 \\
                --to 2025-01-31 \\
                --granularity 24h
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--service",
            type=str,
            required=True,
            help="Datadog service tag value (e.g., 'cropwise-catalog-products-api')",
        )
        parser.add_argument(
            "--env",
            type=str,
            required=True,
            help="Datadog env tag value (e.g., 'prod')",
        )
        parser.add_argument(
            "--last-n-days",
            type=int,
            help="Lookback window in days (e.g., 60)",
        )
        parser.add_argument(
            "--from",
            type=str,
            dest="from_date",
            help="Start date (YYYY-MM-DD)",
        )
        parser.add_argument(
            "--to",
            type=str,
            dest="to_date",
            help="End date (YYYY-MM-DD)",
        )
        parser.add_argument(
            "--range",
            type=str,
            help="Date range as 'YYYY-MM-DD,YYYY-MM-DD'",
        )
        parser.add_argument(
            "--granularity",
            type=str,
            default="12h",
            choices=["1h", "6h", "12h", "24h"],
            help="Time granularity for metric aggregation (default: 12h)",
        )
        parser.add_argument(
            "--tag",
            type=str,
            action="append",
            dest="tags",
            help="Additional tag filter (repeatable, e.g., --tag team:core)",
        )
        parser.add_argument(
            "--top",
            type=int,
            default=20,
            help="Number of top endpoints to show (default: 20)",
        )
        parser.add_argument(
            "--output-json",
            type=str,
            help="Path to save JSON output (default: auto in output/)",
        )
        parser.add_argument(
            "--output-md",
            type=str,
            help="Path to save Markdown report (default: auto in output/)",
        )
        parser.add_argument(
            "--summary-output",
            type=str,
            choices=["auto", "json", "none"],
            default="auto",
            help="Control summary persistence (default: auto)",
        )
        parser.add_argument(
            "--use-cache",
            action="store_true",
            help="Enable caching (60-minute TTL)",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        ensure_datadog_env_loaded()
        logger = LogManager.get_instance().get_logger("HitsByEndpointCommand")

        try:
            # Parse time window
            from_ts, to_ts = HitsByEndpointCommand._parse_time_window(args)

            service = HitsByEndpointCommand._validate_service(args.service)
            env = HitsByEndpointCommand._validate_env(args.env)
            granularity = args.granularity or "12h"
            tags = args.tags or []
            top = args.top or 20

            service_instance = HitsByEndpointService(
                site=os.getenv("DD_SITE") or "datadoghq.eu",
                api_key=os.getenv("DD_API_KEY"),
                app_key=os.getenv("DD_APP_KEY"),
                use_cache=bool(args.use_cache),
                cache_ttl_minutes=60,
            )

            result = service_instance.execute(
                service=service,
                env=env,
                from_ts=from_ts,
                to_ts=to_ts,
                granularity=granularity,
                tags=tags,
            )

            # Print console summary
            HitsByEndpointCommand._print_console_summary(result, top)

            # Handle output
            output_file = HitsByEndpointCommand._handle_output(result, args.output_json, args.output_md, top)
            if output_file:
                result["output_file"] = output_file

            # Emit summary JSON
            summary_mode = getattr(args, "summary_output", "auto")
            manager = DatadogSummaryManager()
            summary_path = manager.emit_summary_compatible(result, summary_mode, result.get("output_file"), [service])
            if summary_path:
                print(f"[summary] wrote: {summary_path}")

        except ValueError as e:
            logger.error(f"Invalid arguments: {e}")
            print(f"❌ Invalid input: {e}")
            exit(2)
        except Exception as e:
            logger.error(f"Command failed: {e}", exc_info=True)
            print(f"❌ Unexpected error: {e}")
            exit(1)

    @staticmethod
    def _parse_time_window(args: Namespace) -> tuple[int, int]:
        """Parse time window from args and return (from_ts, to_ts) in epoch seconds."""
        now = datetime.utcnow()

        # Priority: --last-n-days, --range, --from/--to
        if args.last_n_days:
            if args.last_n_days <= 0:
                raise ValueError("--last-n-days must be positive")
            to_ts = int(now.timestamp())
            from_dt = now - timedelta(days=args.last_n_days)
            from_ts = int(from_dt.timestamp())
            return from_ts, to_ts

        if args.range:
            parts = args.range.split(",")
            if len(parts) != 2:
                raise ValueError("--range must be 'YYYY-MM-DD,YYYY-MM-DD'")
            from_str, to_str = parts[0].strip(), parts[1].strip()
            from_dt = datetime.strptime(from_str, "%Y-%m-%d")
            to_dt = datetime.strptime(to_str, "%Y-%m-%d")
            return int(from_dt.timestamp()), int(to_dt.timestamp())

        if args.from_date and args.to_date:
            from_dt = datetime.strptime(args.from_date, "%Y-%m-%d")
            to_dt = datetime.strptime(args.to_date, "%Y-%m-%d")
            return int(from_dt.timestamp()), int(to_dt.timestamp())

        raise ValueError("Must specify time window: --last-n-days, --range, or --from/--to")

    @staticmethod
    def _validate_service(service: str) -> str:
        if not service or not service.strip():
            raise ValueError("--service cannot be empty")
        return service.strip()

    @staticmethod
    def _validate_env(env: str) -> str:
        if not env or not env.strip():
            raise ValueError("--env cannot be empty")
        return env.strip()

    @staticmethod
    def _print_console_summary(result: dict[str, Any], top: int):
        """Print console table with top N endpoints by total hits."""
        metadata = result.get("metadata", {})
        aggregations = result.get("aggregations", {})

        print("\n🚀 DATADOG HITS & LATENCY BY ENDPOINT")
        print("=" * 105)
        print(f"Service: {metadata.get('service')}")
        print(f"Environment: {metadata.get('env')}")
        print(f"Period: {metadata.get('from_ts')} - {metadata.get('to_ts')}")
        print(f"Granularity: {metadata.get('granularity')}")
        print("=" * 105)

        if not aggregations:
            print("\n⚠️  No data found for the specified parameters.")
            return

        # Sort by total hits descending
        sorted_endpoints = sorted(aggregations.items(), key=lambda x: x[1]["total_hits"], reverse=True)[:top]

        print(f"\nTop {len(sorted_endpoints)} Endpoints by Total Hits:\n")
        print(
            f"{'Endpoint':<50} {'Total Hits':>12} {'Avg/Month':>12} {'p50 (ms)':>10} {'p95 (ms)':>10} {'p99 (ms)':>10}"
        )
        print("-" * 105)

        for endpoint, agg in sorted_endpoints:
            total = agg["total_hits"]
            monthly_avg = agg["monthly_avg"]
            latency = agg.get("latency", {})
            p50_ms = latency.get("p50", 0) * 1000
            p95_ms = latency.get("p95", 0) * 1000
            p99_ms = latency.get("p99", 0) * 1000
            endpoint_display = endpoint if len(endpoint) <= 50 else endpoint[:47] + "..."
            # Format numbers in Portuguese (no thousands separator, comma for decimals)
            total_str = f"{int(total)}"
            monthly_str = f"{monthly_avg:.1f}".replace(".", ",")
            p50_str = f"{p50_ms:.1f}".replace(".", ",")
            p95_str = f"{p95_ms:.1f}".replace(".", ",")
            p99_str = f"{p99_ms:.1f}".replace(".", ",")
            print(f"{endpoint_display:<50} {total_str:>12} {monthly_str:>12} {p50_str:>10} {p95_str:>10} {p99_str:>10}")

        print("\n✅ Console summary complete.\n")

    @staticmethod
    def _handle_output(
        result: dict[str, Any],
        output_json: str | None,
        output_md: str | None,
        top: int,
    ) -> str | None:
        """Save JSON and/or Markdown output."""
        sub_dir = f"datadog-hits_{datetime.now().strftime('%Y%m%d')}"
        base_name = "hits_by_endpoint"

        # Save JSON
        if output_json or output_json is None:
            json_path = output_json or OutputManager.save_json_report(result, sub_dir, base_name)
            if not output_json:
                json_path = OutputManager.save_json_report(result, sub_dir, base_name)
            print(f"JSON saved: {json_path}")

        # Save Markdown
        if output_md or output_md is None:
            markdown = HitsByEndpointCommand._to_markdown(result, top)
            md_path = output_md or OutputManager.save_markdown_report(markdown, sub_dir, base_name)
            if not output_md:
                md_path = OutputManager.save_markdown_report(markdown, sub_dir, base_name)
            print(f"Markdown saved: {md_path}")
            return md_path

        return None

    @staticmethod
    def _to_markdown(result: dict[str, Any], top: int) -> str:
        """Generate Markdown report."""
        metadata = result.get("metadata", {})
        aggregations = result.get("aggregations", {})

        lines: list[str] = []
        lines.append("## Datadog Hits by Endpoint Report")
        lines.append("")
        lines.append(f"- **Service**: `{metadata.get('service')}`")
        lines.append(f"- **Environment**: `{metadata.get('env')}`")

        # Convert epoch timestamps to human-readable dates
        from_ts = metadata.get("from_ts")
        to_ts = metadata.get("to_ts")
        if from_ts and to_ts:
            from_date = datetime.utcfromtimestamp(from_ts).strftime("%Y-%m-%d %H:%M:%S UTC")
            to_date = datetime.utcfromtimestamp(to_ts).strftime("%Y-%m-%d %H:%M:%S UTC")
            lines.append(f"- **Period**: {from_date} to {to_date}")
        else:
            lines.append(f"- **Period**: {from_ts} - {to_ts}")

        lines.append(f"- **Granularity**: {metadata.get('granularity')}")
        lines.append(f"- **Query**: `{metadata.get('query')}`")
        lines.append(f"- **Generated**: {metadata.get('generated_at')}")
        lines.append("")

        if not aggregations:
            lines.append("⚠️ No data available for the specified parameters.")
            return "\n".join(lines)

        # Show ALL endpoints in Markdown, sorted by total hits
        sorted_endpoints = sorted(aggregations.items(), key=lambda x: x[1]["total_hits"], reverse=True)

        lines.append(f"### All Endpoints by Total Hits ({len(sorted_endpoints)} total)")
        lines.append("")
        lines.append(
            "| # | Endpoint | Total Hits | Monthly Avg | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) | StdDev (ms) | CV |"
        )
        lines.append(
            "|---|----------|------------|-------------|----------|----------|----------|----------|-------------|-----|"
        )

        for idx, (endpoint, agg) in enumerate(sorted_endpoints, start=1):
            total = agg["total_hits"]
            monthly_avg = agg["monthly_avg"]
            latency = agg.get("latency", {})
            p50_ms = latency.get("p50", 0) * 1000
            p95_ms = latency.get("p95", 0) * 1000
            p99_ms = latency.get("p99", 0) * 1000
            max_ms = latency.get("max", 0) * 1000
            stddev_ms = latency.get("stddev", 0) * 1000
            cv = latency.get("cv", 0)
            endpoint_clean = endpoint.replace("|", "\\|")
            # Format numbers in Portuguese
            lines.append(
                f"| {idx} | {endpoint_clean} | {int(total)} | {monthly_avg:.1f}".replace(".", ",")
                + f" | {p50_ms:.1f}".replace(".", ",")
                + f" | {p95_ms:.1f}".replace(".", ",")
                + f" | {p99_ms:.1f}".replace(".", ",")
                + f" | {max_ms:.1f}".replace(".", ",")
                + f" | {stddev_ms:.1f}".replace(".", ",")
                + f" | {cv:.2f}".replace(".", ",")
                + " |"
            )

        # Add detailed latency statistics section for top endpoints
        lines.append("")
        lines.append("### Latency Benchmark Statistics (Top 10 Endpoints)")
        lines.append("")
        lines.append("| Endpoint | p50 | p90 | p95 | p99 | p99.9 | Mean | Mean (Trimmed) | StdDev | CV | Max |")
        lines.append("|----------|-----|-----|-----|-----|-------|------|----------------|--------|----|----|")

        for endpoint, agg in sorted_endpoints[:10]:
            latency = agg.get("latency", {})
            endpoint_clean = endpoint.replace("|", "\\|")
            # Format all numbers in Portuguese
            p50 = f"{latency.get('p50', 0) * 1000:.1f}".replace(".", ",")
            p90 = f"{latency.get('p90', 0) * 1000:.1f}".replace(".", ",")
            p95 = f"{latency.get('p95', 0) * 1000:.1f}".replace(".", ",")
            p99 = f"{latency.get('p99', 0) * 1000:.1f}".replace(".", ",")
            p99_9 = f"{latency.get('p99_9', 0) * 1000:.1f}".replace(".", ",")
            mean = f"{latency.get('mean', 0) * 1000:.1f}".replace(".", ",")
            mean_trim = f"{latency.get('mean_trimmed', 0) * 1000:.1f}".replace(".", ",")
            stddev = f"{latency.get('stddev', 0) * 1000:.1f}".replace(".", ",")
            cv = f"{latency.get('cv', 0):.2f}".replace(".", ",")
            max_lat = f"{latency.get('max', 0) * 1000:.1f}".replace(".", ",")

            lines.append(
                f"| {endpoint_clean} | "
                f"{p50}ms | {p90}ms | {p95}ms | {p99}ms | {p99_9}ms | "
                f"{mean}ms | {mean_trim}ms | {stddev}ms | {cv} | {max_lat}ms |"
            )

        lines.append("")
        lines.append("### Findings")
        lines.append("")
        lines.append(f"- Total endpoints analyzed: {len(aggregations)}")
        lines.append(f"- Top endpoint: {sorted_endpoints[0][0]} with {sorted_endpoints[0][1]['total_hits']:,.0f} hits")
        lines.append("")

        return "\n".join(lines)
