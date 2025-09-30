from argparse import ArgumentParser, Namespace
from datetime import datetime
from utils.command.base_command import BaseCommand
from domains.syngenta.jira.epic_monitor_service import CycleDetector
from utils.env_loader import ensure_env_loaded


class CycleInfoCommand(BaseCommand):
    """Command to display cycle information and test cycle calculations."""

    @staticmethod
    def get_name() -> str:
        return "cycle-info"

    @staticmethod
    def get_description() -> str:
        return "Display cycle information and test cycle calculations."

    @staticmethod
    def get_help() -> str:
        return (
            "This command shows current cycle information and allows "
            "testing cycle calculations."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--year-start",
            type=str,
            help=(
                "Override year start date (YYYY-MM-DD format). "
                "Uses YEAR_START_DATE env var if not provided."
            ),
        )
        parser.add_argument(
            "--test-date",
            type=str,
            help="Test what cycle a specific date falls into (YYYY-MM-DD format)",
        )
        parser.add_argument(
            "--show-all", action="store_true", help="Show all cycles for the year"
        )

    @staticmethod
    def main(args: Namespace):
        # Ensure environment variables are loaded
        ensure_env_loaded()

        print("🗓️ Cycle Information")
        print("=" * 50)

        # Show year start date
        year_start = CycleDetector._get_year_start_date()
        print(f"📅 Year Start Date: {year_start}")
        print()

        # Show current cycle
        current_cycle = CycleDetector.get_current_cycle()
        current_info = CycleDetector.get_cycle_info(current_cycle)
        print(f"🎯 Current Cycle: {current_cycle}")
        print(f"   Start: {current_info['start']}")
        print(f"   End: {current_info['end']}")
        print()

        # Test specific date if provided
        if args.test_date:
            try:
                test_date = datetime.strptime(args.test_date, "%Y-%m-%d").date()

                # Calculate what cycle the test date would be in
                year_start = CycleDetector._get_year_start_date()
                cycle_dates = CycleDetector._calculate_cycle_dates(year_start)

                test_cycle = None
                for cycle_name, dates in cycle_dates.items():
                    if dates["start"] <= test_date <= dates["end"]:
                        test_cycle = cycle_name
                        break

                if not test_cycle:
                    if test_date < year_start:
                        test_cycle = "Q1C1"  # Before year start
                    else:
                        test_cycle = "Q4C2"  # After year end

                test_info = CycleDetector.get_cycle_info(test_cycle)

                print(f"🧪 Test Date: {test_date}")
                print(f"   Would be in cycle: {test_cycle}")
                print(f"   Cycle Start: {test_info['start']}")
                print(f"   Cycle End: {test_info['end']}")
                print()
            except ValueError:
                print(f"❌ Invalid test date format: {args.test_date}")
                print("   Use YYYY-MM-DD format")
                return

        # Show all cycles if requested
        if args.show_all:
            print("📋 All Cycles for the Year:")
            print("-" * 30)

            cycle_dates = CycleDetector._calculate_cycle_dates(year_start)

            for quarter in range(1, 5):
                print(f"\n📊 Quarter {quarter}:")
                for cycle in ["C1", "C2"]:
                    cycle_name = f"Q{quarter}{cycle}"
                    if cycle_name in cycle_dates:
                        dates = cycle_dates[cycle_name]
                        duration = (dates["end"] - dates["start"]).days + 1
                        weeks = duration / 7
                        print(
                            f"   {cycle_name}: {dates['start']} to {dates['end']} "
                            f"({duration} days, {weeks:.1f} weeks)"
                        )

            # Show some example fix version tests
            print("\n🔍 Fix Version Pattern Tests:")
            print("-" * 30)

            test_versions = [
                "Q2 C2 2025",
                "QI 2025/Q2 C2",
                "Q2C2",
                "2025 Q2 C2",
                "Q3C1 2025",
                "invalid version",
            ]

            for version in test_versions:
                is_current = CycleDetector.is_fix_version_current_cycle(version)
                status = "✅ MATCH" if is_current else "❌ NO MATCH"
                print(f"   '{version}' -> {status}")
