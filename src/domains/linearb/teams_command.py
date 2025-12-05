"""LinearB Teams Command."""

from argparse import ArgumentParser, Namespace

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_linearb_env_loaded
from utils.logging.logging_manager import LogManager

from .linearb_service import LinearBService


class TeamsCommand(BaseCommand):
    """Command to list and search teams in LinearB."""

    @staticmethod
    def get_name() -> str:
        return "teams"

    @staticmethod
    def get_description() -> str:
        return "List and search teams in LinearB"

    @staticmethod
    def get_help() -> str:
        return """
List and search teams in LinearB.

This command allows you to view all teams or search for specific teams
in your LinearB organization.

Examples:
  # List all teams
  python src/main.py linearb teams

  # Search for teams containing "operations"
  python src/main.py linearb teams --search "operations"

  # List teams with pagination
  python src/main.py linearb teams --page-size 10 --offset 0
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument("--search", type=str, help="Search term to filter teams by name")

        parser.add_argument(
            "--page-size",
            type=int,
            default=50,
            help="Number of teams per page (max 50, default: 50)",
        )

        parser.add_argument("--offset", type=int, default=0, help="Offset for pagination (default: 0)")

        parser.add_argument(
            "--show-details",
            action="store_true",
            help="Show detailed team information including members",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_linearb_env_loaded()
        logger = LogManager.get_instance().get_logger("TeamsCommand")

        try:
            service = LinearBService()

            search_term = getattr(args, "search", None)
            if search_term:
                logger.info(f"Searching teams with term: '{search_term}'")
            else:
                logger.info("Fetching all teams from LinearB...")

            # Get teams information
            teams_data = service.get_teams_info(search_term=search_term)

            total_teams = teams_data.get("total", 0)
            teams_list = teams_data.get("items", [])

            logger.info(f"Found {total_teams} total teams")
            logger.info(f"Showing {len(teams_list)} teams in current page")

            # Display teams information
            if teams_list:
                logger.info("\nTeams List:")
                logger.info("-" * 80)

                for i, team in enumerate(teams_list, 1):
                    team_id = team.get("id", "N/A")
                    team_name = team.get("name", "Unknown")
                    created_at = team.get("created_at", "N/A")

                    logger.info(f"{i:2d}. ID: {team_id:<8} Name: {team_name}")

                    if args.show_details:
                        logger.info(f"     Created: {created_at}")

                        # Show parent team if available
                        parent_id = team.get("parent_id")
                        if parent_id:
                            logger.info(f"     Parent Team ID: {parent_id}")

                        # Show organization ID
                        org_id = team.get("organization_id", "N/A")
                        logger.info(f"     Organization ID: {org_id}")

                        # Show contributors count if available
                        contributors = team.get("contributors", [])
                        if contributors:
                            logger.info(f"     Contributors: {len(contributors)}")

                        logger.info("")
            else:
                logger.info("No teams found matching the criteria")

            # Show pagination info
            if args.offset > 0 or len(teams_list) == args.page_size:
                current_page = (args.offset // args.page_size) + 1
                logger.info(f"\nPage {current_page} (offset: {args.offset}, page size: {args.page_size})")

                if len(teams_list) == args.page_size:
                    logger.info("There may be more teams available. Use --offset to see more.")

            logger.info("Teams listing completed successfully")

        except Exception as e:
            logger.error(f"Teams command failed: {e}")
            exit(1)
