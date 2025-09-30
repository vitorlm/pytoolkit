#!/usr/bin/env python3
"""
NFCe Command - Process Brazilian electronic invoices (NFCe) from Portal SPED URLs
"""

from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from domains.personal_finance.nfce.nfce_processor_service import NFCeService


class NFCeCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "processor"

    @staticmethod
    def get_description() -> str:
        return "Process Brazilian electronic invoices (NFCe) from Portal SPED URLs"

    @staticmethod
    def get_help() -> str:
        return """
Process Brazilian electronic invoices (NFCe) from Portal SPED URLs with comprehensive data analysis.

This command processes NFCe URLs and extracts structured data including:
- Invoice basic information (number, series, date)
- Establishment data (business name, CNPJ, address)
- Consumer information (when available)
- Product items with quantities and prices
- Financial totals and tax information

Examples:
  # Process URLs from JSON file (saves to output/ automatically)
  python src/main.py personal_finance nfce processor --input urls.json
  
  # Process URLs with similarity detection (hybrid system)
  python src/main.py personal_finance nfce processor --input urls.json --detect-similar
  
  # Process URLs with SBERT embeddings for enhanced similarity
  python src/main.py personal_finance nfce processor --input urls.json --detect-similar --use-sbert
  
  # Process with custom similarity threshold
  python src/main.py personal_finance nfce processor --input urls.json --detect-similar --similarity-threshold 0.70
  
  # Process single URL with similarity detection
  python src/main.py personal_finance nfce processor --url "https://portalsped.fazenda.mg.gov.br/..." --detect-similar
  
  # Import existing data with similarity analysis
  python src/main.py personal_finance nfce processor --import-data results.json --detect-similar --save-db
  
  # Process URLs and save to database with analysis
  python src/main.py personal_finance nfce processor --input urls.json --save-db --analysis --detect-similar

Input format (JSON file):
  {
    "urls": [
      "https://portalsped.fazenda.mg.gov.br/portalnfce/sistema/qrcode.xhtml?p=...",
      "https://portalsped.fazenda.mg.gov.br/portalnfce/sistema/qrcode.xhtml?p=..."
    ]
  }

Output format:
  {
    "total_processed": 2,
    "successful": 2,
    "failed": 0,
    "invoices": [
      {
        "access_key": "31240565124307001626651520000234411152653586",
        "invoice_number": "23441",
        "series": "152",
        "issue_date": "2024-05-05T18:52:10",
        "establishment": { "business_name": "...", "cnpj": "..." },
        "items": [...],
        "total_amount": 190.77,
        "scraping_success": true
      }
    ]
  }
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        # Input options (mutually exclusive)
        input_group = parser.add_mutually_exclusive_group(required=True)
        input_group.add_argument(
            "--input", help="JSON file containing list of NFCe URLs to process"
        )
        input_group.add_argument("--url", help="Single NFCe URL to process")
        input_group.add_argument(
            "--import-data",
            help="Import existing processed NFCe data (JSON file with 'invoices' array) directly to database",
        )

        # Output options
        parser.add_argument(
            "--output",
            help="Output file path for results (JSON format). If not specified, saves to output/ folder with timestamp",
        )
        parser.add_argument(
            "--save-db", action="store_true", help="Save results to local database"
        )

        # Processing options
        parser.add_argument(
            "--analysis",
            action="store_true",
            help="Generate analysis report with statistics",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=10,
            help="Number of URLs to process concurrently (default: 10)",
        )
        parser.add_argument(
            "--timeout",
            type=int,
            default=30,
            help="Request timeout in seconds (default: 30)",
        )

        # Database options
        parser.add_argument(
            "--clear-cache",
            action="store_true",
            help="Clear cached data before processing",
        )
        parser.add_argument(
            "--force-refresh",
            action="store_true",
            help="Force refresh of all data, ignoring cache",
        )

        # Similarity detection options
        parser.add_argument(
            "--detect-similar",
            action="store_true",
            help="Detect similar products across different establishments using hybrid similarity engine",
        )
        parser.add_argument(
            "--similarity-threshold",
            type=float,
            default=0.60,
            help="Similarity threshold for product matching (default: 0.60)",
        )
        parser.add_argument(
            "--use-sbert",
            action="store_true",
            help="Use SBERT Portuguese embeddings for enhanced similarity detection",
        )
        parser.add_argument(
            "--sbert-model",
            type=str,
            default="paraphrase-multilingual-MiniLM-L12-v2",
            help="SBERT model for Portuguese embeddings (default: multilingual MiniLM)",
        )

    @staticmethod
    def main(args: Namespace):
        # Always start with environment loading
        ensure_env_loaded()

        # Get logger with component name
        logger = LogManager.get_instance().get_logger("NFCeCommand")

        try:
            logger.info("Starting NFCe processing command")

            # Validate input arguments
            NFCeCommand._validate_arguments(args, logger)

            # Initialize standard NFCe service
            logger.info("Initializing NFCe service")
            service = NFCeService()

            # Warn about similarity features being disabled
            if args.detect_similar:
                logger.warning(
                    "Similarity detection not available in basic processor. Use 'product-analysis' command for similarity analysis."
                )

            # Clear cache if requested
            if args.clear_cache:
                logger.info("Clearing cache")
                service.clear_cache()

            # Process based on input type
            if args.import_data:
                logger.info(f"Importing existing data from file: {args.import_data}")
                result = service.import_existing_data(args.import_data)
            elif args.input:
                logger.info(f"Processing URLs from file: {args.input}")
                result = service.process_urls_from_file(
                    input_file=args.input,
                    batch_size=args.batch_size,
                    timeout=args.timeout,
                    force_refresh=args.force_refresh,
                )
            else:  # args.url
                logger.info(f"Processing single URL: {args.url}")
                result = service.process_single_url(
                    url=args.url, timeout=args.timeout, force_refresh=args.force_refresh
                )

            # Validate processing results
            if not result or not isinstance(result, dict):
                raise ValueError("Invalid processing result: empty or invalid format")

            # Generate analysis if requested
            if args.analysis:
                logger.info("Generating analysis report")
                try:
                    analysis = service.generate_analysis_report(result)
                    result["analysis"] = analysis
                except Exception as e:
                    logger.warning(f"Failed to generate analysis report: {e}")
                    result["analysis"] = {
                        "error": f"Analysis generation failed: {str(e)}"
                    }

            # Save to database if requested
            if args.save_db:
                logger.info("Saving results to database")
                try:
                    service.save_to_database(result)
                except Exception as e:
                    logger.error(f"Failed to save to database: {e}")
                    print(f"Warning: Database save failed - {e}")

            # Save to output file (always save to output folder)
            if args.output:
                # Use user-specified path
                output_path = args.output
            else:
                # Generate default output path in output folder
                from datetime import datetime

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"nfce_results_{timestamp}.json"
                output_path = f"output/{output_filename}"

            logger.info(f"Saving results to: {output_path}")
            try:
                service.save_results(result, output_path)
                print(f"Results saved to: {output_path}")
            except Exception as e:
                logger.error(f"Failed to save results to file: {e}")
                raise ValueError(f"Failed to save results to {output_path}: {e}")

            # Always print summary to console as well
            service.print_summary(result)

            # Log completion status
            total_processed = result.get("total_processed", 0)
            successful = result.get("successful", 0)
            failed = result.get("failed", 0)

            if total_processed == 0:
                logger.warning("No URLs were processed")
                print("Warning: No URLs were processed. Check your input file or URL.")
            elif failed > 0:
                logger.warning(
                    f"Processing completed with errors: {successful}/{total_processed} successful"
                )
                print(
                    f"Warning: {failed} out of {total_processed} URLs failed to process."
                )
            else:
                logger.info(
                    f"NFCe processing completed successfully: {successful}/{total_processed} processed"
                )

        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")
            print(f"Error: File not found - {e}")
            print(
                "Please check that the input file path is correct and the file exists."
            )
            exit(1)
        except ValueError as e:
            logger.error(f"Invalid input: {e}")
            print(f"Error: Invalid input - {e}")
            exit(1)
        except PermissionError as e:
            logger.error(f"Permission denied: {e}")
            print(f"Error: Permission denied - {e}")
            print("Please check file permissions for input/output files.")
            exit(1)
        except KeyboardInterrupt:
            logger.info("Processing interrupted by user")
            print("\nProcessing interrupted by user.")
            exit(130)  # Standard exit code for SIGINT
        except Exception as e:
            logger.error(f"Unexpected error during NFCe processing: {e}", exc_info=True)
            print(f"Error: An unexpected error occurred - {e}")
            print("Please check the logs for more details.")
            exit(1)

    @staticmethod
    def _validate_arguments(args: Namespace, logger) -> None:
        """Validate command line arguments"""
        # Validate batch size
        if args.batch_size <= 0 or args.batch_size > 50:
            raise ValueError("Batch size must be between 1 and 50")

        # Validate timeout
        if args.timeout <= 0 or args.timeout > 300:
            raise ValueError("Timeout must be between 1 and 300 seconds")

        # Validate input file exists if provided
        if args.input:
            import os

            if not os.path.exists(args.input):
                raise FileNotFoundError(f"Input file not found: {args.input}")
            if not os.path.isfile(args.input):
                raise ValueError(f"Input path is not a file: {args.input}")
            if not args.input.lower().endswith(".json"):
                logger.warning(
                    f"Input file does not have .json extension: {args.input}"
                )

        # Validate import data file exists if provided
        if args.import_data:
            import os

            if not os.path.exists(args.import_data):
                raise FileNotFoundError(
                    f"Import data file not found: {args.import_data}"
                )
            if not os.path.isfile(args.import_data):
                raise ValueError(f"Import data path is not a file: {args.import_data}")
            if not args.import_data.lower().endswith(".json"):
                raise ValueError(
                    f"Import data file must be a JSON file: {args.import_data}"
                )

        # Validate URL format if provided
        if args.url:
            if not args.url.startswith("http"):
                raise ValueError("URL must start with http:// or https://")
            if "portalsped.fazenda.mg.gov.br" not in args.url:
                logger.warning("URL does not appear to be from Portal SPED MG")

        # Validate output path if provided
        if args.output:
            import os

            output_dir = os.path.dirname(args.output)
            # Only check if output_dir exists if it's not empty and not the default output folder
            if output_dir and output_dir != "output" and not os.path.exists(output_dir):
                logger.warning(
                    f"Output directory does not exist and will be created: {output_dir}"
                )
            if not args.output.lower().endswith(".json"):
                logger.warning(
                    f"Output file does not have .json extension: {args.output}"
                )

        logger.debug("All input arguments validated successfully")
