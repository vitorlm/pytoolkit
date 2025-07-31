"""
CircleCI Domain Command - Main entry point for CircleCI commands
"""

from argparse import ArgumentParser
from utils.command.base_command import BaseCommand


class CircleCICommand(BaseCommand):
    """Main CircleCI domain command that provides subcommands"""
    
    @staticmethod
    def get_name() -> str:
        return "circleci"
    
    @staticmethod
    def get_description() -> str:
        return "CircleCI pipeline analysis and optimization tools"
    
    @staticmethod
    def get_help() -> str:
        return """
CircleCI pipeline analysis and optimization tools.

This domain provides commands for analyzing CircleCI pipeline performance,
identifying bottlenecks, and generating optimization recommendations.

Available commands:
  circleci-analyze  - Analyze pipeline performance and generate recommendations

Setup:
  1. Add your CircleCI token to .env file:
     CIRCLECI_TOKEN=your_token_here
  
  2. Get your token from: https://app.circleci.com/settings/user/tokens

Examples:
  # Analyze pipeline performance  
  python src/main.py circleci circleci-analyze --project-slug gh/org/repo

  # Quick summary analysis
  python src/main.py circleci circleci-analyze --project-slug gh/org/repo --summary-only
        """
    
    @staticmethod
    def get_arguments(parser: ArgumentParser):
        # No domain-level arguments
        pass
    
    @staticmethod
    def add_subcommands(parser: ArgumentParser):
        from .circleci_analysis_command import CircleCIAnalysisCommand
        
        subparsers = parser.add_subparsers(
            dest="circleci_command",
            help="CircleCI commands"
        )
        
        # Register analysis command
        CircleCIAnalysisCommand.register_command(subparsers)
    
    @staticmethod
    def main(args):
        # This should not be called directly since this is a parent command
        print("CircleCI domain - use the circleci-analyze subcommand")
        print("Run 'python src/main.py circleci --help' for available commands")
