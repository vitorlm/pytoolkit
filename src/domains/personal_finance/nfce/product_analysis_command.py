#!/usr/bin/env python3
"""Product Analysis Command - Analyze existing products for similarity patterns"""

from argparse import ArgumentParser, Namespace

from domains.personal_finance.nfce.product_analysis_service import (
    ProductAnalysisService,
)
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class ProductAnalysisCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "product-analysis"

    @staticmethod
    def get_description() -> str:
        return "Analyze existing products for similarity patterns and potential duplicates"

    @staticmethod
    def get_help() -> str:
        return """
Analyze existing products in the NFCe database to identify patterns, potential duplicates,
and preparation for product deduplication.

This command performs comprehensive analysis of product names, codes, and patterns across
different establishments to understand the current data landscape and identify opportunities
for product consolidation.

Examples:
  # Basic analysis of all products
  python src/main.py personal_finance nfce product-analysis

  # Analysis with detailed output
  python src/main.py personal_finance nfce product-analysis --detailed

  # Export analysis to specific file
  python src/main.py personal_finance nfce product-analysis --output analysis_results.json

  # Analyze only specific establishment
  python src/main.py personal_finance nfce product-analysis --cnpj "12345678000100"

  # Focus on specific product categories
  python src/main.py personal_finance nfce product-analysis --category-filter "beverages,fruits"

Output includes:
  - Total unique products per establishment
  - Most common product names and variations
  - Potential duplicate groups
  - Product naming patterns
  - Establishment-specific coding patterns
  - Recommendations for similarity thresholds
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        # Output options
        parser.add_argument(
            "--output",
            help="Output file path for analysis results (JSON format). If not specified, saves to output/ folder with timestamp",
        )

        # Analysis options
        parser.add_argument(
            "--detailed",
            action="store_true",
            help="Generate detailed analysis with extended statistics and examples",
        )

        parser.add_argument("--cnpj", help="Analyze products from specific establishment (CNPJ)")

        parser.add_argument(
            "--category-filter",
            help="Comma-separated list of categories to focus on (e.g., 'beverages,fruits,dairy')",
        )

        parser.add_argument(
            "--similarity-threshold",
            type=float,
            default=0.8,
            help="Similarity threshold for potential duplicate detection (default: 0.8)",
        )

        parser.add_argument(
            "--min-frequency",
            type=int,
            default=2,
            help="Minimum frequency for product to be considered in analysis (default: 2)",
        )

        # Sample options
        parser.add_argument(
            "--sample-size",
            type=int,
            help="Limit analysis to random sample of products (useful for large datasets)",
        )

        parser.add_argument(
            "--show-examples",
            action="store_true",
            help="Include concrete examples in analysis output",
        )

        # Phase 3: Similarity Engine options
        parser.add_argument(
            "--use-similarity-engine",
            action="store_true",
            help="Use the Phase 2 Similarity Engine for advanced duplicate detection",
        )

        parser.add_argument(
            "--similarity-only",
            action="store_true",
            help="Run only similarity analysis (requires --use-similarity-engine)",
        )

    @staticmethod
    def main(args: Namespace):
        # Always start with environment loading
        ensure_env_loaded()

        # Get logger with component name
        logger = LogManager.get_instance().get_logger("ProductAnalysisCommand")

        try:
            logger.info("Starting product analysis")

            # Initialize service
            service = ProductAnalysisService()

            # Validate arguments
            ProductAnalysisCommand._validate_arguments(args, logger)

            # Choose analysis type based on arguments
            if args.use_similarity_engine:
                logger.info("Using Phase 2 Similarity Engine for analysis")

                if args.similarity_only:
                    # Run only similarity analysis
                    analysis_results = service.analyze_similarity_only(
                        cnpj_filter=args.cnpj,
                        similarity_threshold=args.similarity_threshold,
                        sample_size=args.sample_size,
                    )
                else:
                    # Run enhanced analysis with similarity engine
                    analysis_results = service.analyze_products_with_similarity(
                        cnpj_filter=args.cnpj,
                        category_filter=args.category_filter.split(",") if args.category_filter else None,
                        similarity_threshold=args.similarity_threshold,
                        min_frequency=args.min_frequency,
                        sample_size=args.sample_size,
                        detailed=args.detailed,
                        include_examples=args.show_examples,
                    )
            else:
                # Run traditional analysis
                logger.info("Using traditional analysis method")
                analysis_results = service.analyze_products(
                    cnpj_filter=args.cnpj,
                    category_filter=args.category_filter.split(",") if args.category_filter else None,
                    similarity_threshold=args.similarity_threshold,
                    min_frequency=args.min_frequency,
                    sample_size=args.sample_size,
                    detailed=args.detailed,
                    include_examples=args.show_examples,
                )

            # Generate output path
            if args.output:
                output_path = args.output
            else:
                from datetime import datetime

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"product_analysis_{timestamp}.json"
                output_path = f"output/{output_filename}"

            # Save results
            logger.info(f"Saving analysis results to: {output_path}")
            service.save_analysis_results(analysis_results, output_path)

            # Print summary to console
            service.print_analysis_summary(analysis_results)

            logger.info("Product analysis completed successfully")
            print(f"Detailed results saved to: {output_path}")

        except FileNotFoundError as e:
            logger.error(f"Database not found: {e}")
            print(f"Error: Database file not found - {e}")
            print("Please run the NFCe processor first to create the database.")
            exit(1)
        except ValueError as e:
            logger.error(f"Invalid input: {e}")
            print(f"Error: Invalid input - {e}")
            exit(1)
        except Exception as e:
            logger.error(f"Unexpected error during product analysis: {e}", exc_info=True)
            print(f"Error: An unexpected error occurred - {e}")
            print("Please check the logs for more details.")
            exit(1)

    @staticmethod
    def _validate_arguments(args: Namespace, logger) -> None:
        """Validate command arguments"""
        # Validate similarity threshold
        if not 0.0 <= args.similarity_threshold <= 1.0:
            raise ValueError("Similarity threshold must be between 0.0 and 1.0")

        # Validate min frequency
        if args.min_frequency < 1:
            raise ValueError("Minimum frequency must be at least 1")

        # Validate sample size if provided
        if args.sample_size and args.sample_size < 1:
            raise ValueError("Sample size must be at least 1")

        # Validate CNPJ format if provided
        if args.cnpj:
            # Basic CNPJ format validation
            clean_cnpj = "".join(filter(str.isdigit, args.cnpj))
            if len(clean_cnpj) != 14:
                raise ValueError("CNPJ must have exactly 14 digits")

        logger.info(f"Arguments validated: threshold={args.similarity_threshold}, min_freq={args.min_frequency}")
