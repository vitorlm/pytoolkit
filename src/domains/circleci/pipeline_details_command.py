"""
CircleCI Pipeline Details Command
Command interface for extracting detailed information from a specific CircleCI pipeline
"""

import os
from argparse import ArgumentParser, Namespace

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded, load_domain_env
from utils.logging.logging_manager import LogManager

from .pipeline_details_service import PipelineDetailsService


class PipelineDetailsCommand(BaseCommand):
    """Command for extracting detailed information from a specific CircleCI pipeline"""
    
    @staticmethod
    def get_name() -> str:
        return "pipeline-details"
    
    @staticmethod
    def get_description() -> str:
        return "Extract detailed information from a specific CircleCI pipeline for analysis"
    
    @staticmethod
    def get_help() -> str:
        return """Extract detailed information from a specific CircleCI pipeline for analysis.

This command extracts comprehensive metadata from a CircleCI pipeline including:
- Pipeline trigger information
- All workflows and their statuses
- All jobs and their details
- Timing information
- Failure reasons and error logs
- Git commit information
- Configuration analysis

Examples:
  # Analyze pipeline by ID (from URL like /pipelines/github/org/repo/9832)
  python src/main.py circleci pipeline-details --pipeline-id 9832 --project-slug gh/organization/repository

  # Analyze pipeline by number with verbose output
  python src/main.py circleci pipeline-details --pipeline-number 9832 --project-slug gh/organization/repository --verbose

  # Extract and save to JSON file
  python src/main.py circleci pipeline-details --pipeline-id 9832 --project-slug gh/organization/repository --output-file ./pipeline-analysis.json

Setup:
  1. Add your CircleCI token to .env file:
     CIRCLECI_TOKEN=your_token_here
  
  2. Get your token from: https://app.circleci.com/settings/user/tokens
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        """Configure command line arguments"""
        parser.add_argument(
            '--token',
            type=str,
            help='CircleCI API token (or set CIRCLECI_TOKEN env var)'
        )
        parser.add_argument(
            '--project-slug',
            type=str,
            required=True,
            help='Project slug (e.g., gh/syngenta-digital/package-react-cropwise-elements)'
        )
        parser.add_argument(
            '--pipeline-id',
            type=str,
            help='Pipeline ID (UUID format)'
        )
        parser.add_argument(
            '--pipeline-number',
            type=int,
            help='Pipeline number (from URL or pipeline list)'
        )
        parser.add_argument(
            '--output-file',
            type=str,
            help='Output file path for JSON results (default: console output)'
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed output including job logs and error details'
        )
        parser.add_argument(
            '--analyze-config',
            action='store_true',
            help='Analyze CircleCI config.yml for workflow triggers and conditions'
        )
    
    @staticmethod
    def main(args: Namespace):
        """Main execution method"""
        ensure_env_loaded([])  # Don't require any specific env vars for CircleCI
        load_domain_env("domains/circleci")
        logger = LogManager.get_instance().get_logger("PipelineDetailsCommand")
        
        try:
            # Get CircleCI token
            token = args.token or os.getenv("CIRCLECI_TOKEN")
            if not token:
                logger.error("❌ CircleCI token is required. Set CIRCLECI_TOKEN env var or use --token")
                exit(1)
            
            # Validate pipeline identifier
            if not args.pipeline_id and not args.pipeline_number:
                logger.error("❌ Either --pipeline-id or --pipeline-number is required")
                exit(1)
            
            # Initialize service
            service = PipelineDetailsService(token, args.project_slug)
            
            # Extract pipeline details
            logger.info(f"🔍 Extracting details for pipeline in project: {args.project_slug}")
            
            pipeline_data = service.get_pipeline_details(
                pipeline_id=args.pipeline_id,
                pipeline_number=args.pipeline_number,
                verbose=args.verbose
            )
            
            if not pipeline_data:
                logger.error("❌ Failed to extract pipeline data")
                exit(1)
            
            # Analyze configuration if requested
            if args.analyze_config:
                logger.info("🔧 Analyzing CircleCI configuration...")
                config_analysis = service.analyze_pipeline_config(pipeline_data)
                pipeline_data['config_analysis'] = config_analysis
            
            # Output results
            if args.output_file:
                service.save_to_file(pipeline_data, args.output_file)
                logger.info(f"✅ Results saved to: {args.output_file}")
            else:
                service.print_pipeline_summary(pipeline_data, verbose=args.verbose)
            
            logger.info("✅ Pipeline analysis completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Command failed: {e}", exc_info=True)
            exit(1)
