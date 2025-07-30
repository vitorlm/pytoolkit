"""
CW Catalog API Comparison Command

Command to compare CSV data against CW Catalog API products by downloading products 
by country and performing detailed matching analysis.
"""

from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from .config import CatalogConfig


class CWCatalogComparisonCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "cw-catalog-comparison"

    @staticmethod
    def get_description() -> str:
        return "Compare CSV data against CW Catalog API products by country"

    @staticmethod
    def get_help() -> str:
        return """
        Compare CSV data against CW Catalog API products by downloading products 
        by country and performing detailed matching analysis.
        
        This command addresses the data reconciliation requirement by:
        1. Analyzing CSV data and grouping by country
        2. Downloading all products from CW Catalog API for each country found in CSV
        3. Performing detailed matching (by ID and by name+country)
        4. Generating comprehensive comparison reports
        
        Features:
        - Country-by-country product downloading and caching
        - Multiple matching strategies (exact ID, name+country)
        - Detailed statistics and match rate analysis
        - Comprehensive JSON reports and summary CSV output
        - API response caching to avoid duplicate requests
        
        Examples:
        # Basic comparison (canonical products only)
        python src/main.py syngenta catalog cw-catalog-comparison \\
            --csv-path data/tube-deleted-products.csv \\
            --api-base-url https://api.cropwise.com \\
            --api-key YOUR_API_KEY
        
        # With organization-specific products
        python src/main.py syngenta catalog cw-catalog-comparison \\
            --csv-path data/tube-deleted-products.csv \\
            --api-base-url https://api.cropwise.com \\
            --api-key YOUR_API_KEY \\
            --org-id YOUR_ORG_UUID
        
        # Full comparison with output and deleted products
        python src/main.py syngenta catalog cw-catalog-comparison \\
            --csv-path data/tube-deleted-products.csv \\
            --api-base-url https://api.cropwise.com \\
            --api-key YOUR_API_KEY \\
            --output-path output/cw_catalog_comparison.json \\
            --include-deleted \\
            --source TUBE \\
            --batch-size 500
        
        # With custom cache duration
        python src/main.py syngenta catalog cw-catalog-comparison \\
            --csv-path data/tube-deleted-products.csv \\
            --api-base-url https://api.cropwise.com \\
            --api-key YOUR_API_KEY \\
            --cache-duration 120
        
        # Simplified output showing only deletion summary
        python src/main.py syngenta catalog cw-catalog-comparison \\
            --csv-path data/tube-deleted-products.csv \\
            --api-base-url https://api.cropwise.com \\
            --api-key YOUR_API_KEY \\
            --simplified-output
        
        # Simplified output with CSV export
        python src/main.py syngenta catalog cw-catalog-comparison \\
            --csv-path data/tube-deleted-products.csv \\
            --api-base-url https://api.cropwise.com \\
            --api-key YOUR_API_KEY \\
            --simplified-output \\
            --simplified-csv output/deletion_summary.csv
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument("--csv-path", required=True, help="Path to CSV file with products to compare")
        parser.add_argument(
            "--api-base-url",
            default=CatalogConfig.CROPWISE_API_BASE_URL,
            help="Base URL for CW Catalog API (default: from CROPWISE_API_BASE_URL env var)",
        )
        parser.add_argument(
            "--api-key",
            default=CatalogConfig.CROPWISE_API_KEY,
            help="API key for CW Catalog authentication (default: from CROPWISE_API_KEY env var)",
        )
        parser.add_argument(
            "--org-id", help="Organization UUID for API requests (optional - omit for canonical products only)"
        )
        parser.add_argument(
            "--source",
            default=CatalogConfig.DEFAULT_SOURCE,
            help="Product source to filter in API (default: from DEFAULT_SOURCE env var or TUBE)",
        )
        parser.add_argument("--output-path", help="Path to save detailed comparison report (JSON format)")
        parser.add_argument("--include-deleted", action="store_true", help="Include deleted products from API")
        parser.add_argument(
            "--batch-size",
            type=int,
            default=CatalogConfig.DEFAULT_BATCH_SIZE,
            help=f"Number of products per API call (default: {CatalogConfig.DEFAULT_BATCH_SIZE})",
        )
        parser.add_argument(
            "--cache-duration",
            type=int,
            default=CatalogConfig.DEFAULT_CACHE_DURATION,
            help=f"Cache duration for API responses in minutes (default: {CatalogConfig.DEFAULT_CACHE_DURATION})",
        )
        parser.add_argument(
            "--country",
            required=False,
            help="Country filter (ISO2 code) to process specific country (e.g., 'BR', 'AR', 'US'). If not provided, processes all countries from CSV.",
        )
        parser.add_argument(
            "--simplified-output",
            action="store_true",
            help="Show simplified output with only country, already deleted, and need to delete columns",
        )
        parser.add_argument(
            "--simplified-csv",
            help="Path to save simplified CSV with only country, already deleted, and need to delete columns",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("CWCatalogComparisonCommand")

        try:
            CatalogConfig.validate_required_config()
            logger.info("Configuration validation passed")
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            print(f"Configuration Error: {e}")
            print("Please check your .env file in src/domains/syngenta/catalog/.env")
            exit(1)

        # Validate API parameters
        if not args.api_base_url or not args.api_key:
            missing = []
            if not args.api_base_url:
                missing.append("API base URL")
            if not args.api_key:
                missing.append("API key")

            error_msg = f"Error: {' and '.join(missing)} required"
            logger.error(error_msg)
            print(error_msg)
            print("Use command line arguments or set environment variables")
            exit(1)

        try:
            logger.info("Starting CW Catalog API comparison")
            logger.info(f"CSV file: {args.csv_path}")
            logger.info(f"API base URL: {args.api_base_url}")
            logger.info(f"Organization ID: {args.org_id or 'None (canonical products only)'}")
            logger.info(f"Source: {args.source}")
            logger.info(f"Include deleted: {args.include_deleted}")
            logger.info(f"Batch size: {args.batch_size}")
            logger.info(f"Cache duration: {args.cache_duration} minutes")
            country_msg = args.country if args.country else "All countries from CSV"
            logger.info(f"Country filter: {country_msg}")

            # Import here to avoid circular import issues
            from .cw_catalog_comparison_service import CWCatalogComparisonService

            service = CWCatalogComparisonService()

            # Use the comparison method (optimized by default for deletion check)
            result = service.compare_csv_against_api_by_ids(
                csv_path=args.csv_path,
                country_filter=args.country,
                api_base_url=args.api_base_url,
                api_key=args.api_key,
                output_path=args.output_path,
                include_deleted=args.include_deleted,
                batch_size=args.batch_size,
                cache_duration_minutes=args.cache_duration,
            )

            logger.info("CW Catalog API comparison completed successfully")

            # Check if simplified output is requested
            if args.simplified_output:
                service.print_simplified_deletion_summary(result)
                
                # Save simplified CSV if path provided
                if args.simplified_csv:
                    service.save_simplified_deletion_csv(result, args.simplified_csv)
                    print(f"\nSimplified CSV saved to: {args.simplified_csv}")
                
                return  # Exit early, skip detailed output

            # Print comprehensive summary to console
            print("\n" + "=" * 80)
            print("CW CATALOG DELETION CHECK RESULTS")
            print("=" * 80)

            # Check if this is deletion analysis format or comparison format
            if "summary" in result:
                # Deletion analysis format
                summary = result["summary"]
                csv_stats = result["csv_stats"]

                print("Data Sources:")
                print(f"  CSV Products: {summary['total_csv_products']:,}")
                print(f"  API Products Found: {summary['total_api_products']:,}")
                print(f"  Countries in CSV: {len(csv_stats['countries'])}")

                print("\nDeletion Analysis:")
                print(f"  Products Found in API: {summary['products_found_in_api']:,}")
                print(f"  Products NOT Found in API: {summary['products_not_found_in_api']:,}")
                print(f"  Products Ready for Deletion: {summary['products_ready_for_deletion']:,}")
                print(f"  Products Already Deleted: {summary['products_already_deleted']:,}")

                ready_pct = (summary['products_ready_for_deletion'] / summary['total_csv_products'] * 100) if summary['total_csv_products'] > 0 else 0
                deleted_pct = (summary['products_already_deleted'] / summary['total_csv_products'] * 100) if summary['total_csv_products'] > 0 else 0
                not_found_pct = (summary['products_not_found_in_api'] / summary['total_csv_products'] * 100) if summary['total_csv_products'] > 0 else 0

                print("\nPercentages:")
                print(f"  Ready for Deletion: {ready_pct:.1f}%")
                print(f"  Already Deleted: {deleted_pct:.1f}%")
                print(f"  Not Found in API: {not_found_pct:.1f}%")
            else:
                # Original comparison format
                overall = result["overall_comparison"]
                csv_stats = result["csv_stats"]

                print("Data Sources:")
                print(f"  CSV Products: {overall['csv_total']:,}")
                print(f"  API Products: {overall['api_total']:,}")
                print(f"  Countries Processed: {overall['countries_processed']}")
                print(f"  Countries in CSV: {csv_stats['countries_count']}")

                print("\nMatching Results:")
                print(f"  Exact ID Matches: {overall['matched_by_id']:,}")
                print(f"  Name+Country Matches: {overall['matched_by_name_country']:,}")
                print(f"  Total Matches: {overall['matched_by_id'] + overall['matched_by_name_country']:,}")
                print(f"  CSV-only Products: {overall['csv_only']:,}")
                print(f"  API-only Products: {overall['api_only']:,}")

                print("\nMatch Rates:")
                print(f"  ID Match Rate: {overall['id_match_rate']:.1f}%")
                print(f"  Total Match Rate: {overall['total_match_rate']:.1f}%")

            # Per-country breakdown for deletion analysis
            if "summary" in result and "country_comparisons" in result:
                print("\n" + "-" * 60)
                print("PRODUCTS TO DELETE BY COUNTRY")
                print("-" * 60)
                
                # Sort countries by products ready for deletion (descending)
                country_data = []
                for country, comparison in result["country_comparisons"].items():
                    if "summary" in comparison:
                        country_data.append({
                            "country": country,
                            "ready_for_deletion": comparison["summary"]["products_ready_for_deletion"],
                            "already_deleted": comparison["summary"]["products_already_deleted"],
                            "not_found": comparison["summary"]["products_not_found_in_api"],
                            "total_csv": comparison["summary"]["total_csv_products"]
                        })
                
                country_data.sort(key=lambda x: x["ready_for_deletion"], reverse=True)
                
                if country_data:
                    print(f"{'Country':<8} {'Ready':<8} {'Deleted':<8} {'NotFound':<10} {'Total':<8} {'%Ready':<8}")
                    print("-" * 60)
                    
                    for data in country_data:
                        ready_pct = (data["ready_for_deletion"] / data["total_csv"] * 100) if data["total_csv"] > 0 else 0
                        print(f"{data['country']:<8} {data['ready_for_deletion']:<8} {data['already_deleted']:<8} {data['not_found']:<10} {data['total_csv']:<8} {ready_pct:<7.1f}%")
                    
                    # Show countries with products ready for deletion
                    countries_with_deletions = [data for data in country_data if data["ready_for_deletion"] > 0]
                    if countries_with_deletions:
                        total_to_delete = sum(data["ready_for_deletion"] for data in countries_with_deletions)
                        print(f"\n📊 DELETION SUMMARY:")
                        print(f"   Total countries with products to delete: {len(countries_with_deletions)}")
                        print(f"   Total products ready for deletion: {total_to_delete:,}")
                        print(f"   Countries: {', '.join([f'{d['country']} ({d['ready_for_deletion']})' for d in countries_with_deletions[:10]])}")
                        if len(countries_with_deletions) > 10:
                            print(f"   ... and {len(countries_with_deletions) - 10} more countries")

            # Analysis recommendations
            if "summary" in result:
                # Deletion analysis recommendations
                summary = result["summary"]
                if summary['products_ready_for_deletion'] > 0:
                    print(f"\n🔄 ACTION REQUIRED: {summary['products_ready_for_deletion']:,} products ready for deletion")
                if summary['products_already_deleted'] > 0:
                    print(f"\n✅ INFO: {summary['products_already_deleted']:,} products already deleted")
                if summary['products_not_found_in_api'] > 0:
                    print(f"\n❓ INFO: {summary['products_not_found_in_api']:,} products not found in API")
            else:
                # Original comparison analysis
                if overall["id_match_rate"] > 80:
                    print("\n✅ EXCELLENT: High ID match rate indicates good data consistency")
                elif overall["id_match_rate"] > 50:
                    print("\n⚠️  MODERATE: Decent ID match rate, but room for improvement")
                else:
                    print("\n🔍 INVESTIGATION NEEDED: Low ID match rate suggests data inconsistencies")

                # Top countries by volume
                if "country_summary" in result and result["country_summary"]:
                    top_countries = result["country_summary"][:5]
                    print("\nTop Countries by CSV Volume:")
                    for i, country_info in enumerate(top_countries, 1):
                        print(
                            f"  {i}. {country_info['country']}: {country_info['csv_count']:,} products "
                            f"({country_info['match_rate']:.1f}% match rate)"
                        )

                # Error handling summary
                error_countries = [country for country, data in result["country_comparisons"].items() if "error" in data]
                if error_countries:
                    print(f"\n❌ Countries with Errors: {', '.join(error_countries)}")

            if args.output_path:
                print("\nDetailed reports saved:")
                base_name = args.output_path.rsplit(".", 1)[0]
                print(f"  Main report: {args.output_path}")
                print(f"  Summary CSV: {base_name}_summary.csv")

            # Next steps recommendation
            print("\n" + "=" * 80)
            print("NEXT STEPS RECOMMENDATION:")
            print("=" * 80)

            if "summary" in result:
                # Deletion-specific recommendations
                summary = result["summary"]
                ready_count = summary['products_ready_for_deletion']
                deleted_count = summary['products_already_deleted']
                
                if ready_count > 0:
                    print(f"🔄 DELETION READY: {ready_count:,} products can be deleted from the API")
                    print("   Review the detailed report before proceeding with deletion.")
                if deleted_count > 0:
                    print(f"✅ ALREADY PROCESSED: {deleted_count:,} products already deleted")
                if summary['products_not_found_in_api'] > 0:
                    print(f"❓ INVESTIGATION: {summary['products_not_found_in_api']:,} products not found in API")
                    print("   These may have been deleted previously or never uploaded.")
            else:
                # Original comparison recommendations
                if overall["total_match_rate"] > 90:
                    print("✅ EXCELLENT DATA CONSISTENCY:")
                    print("   The API and CSV data are highly aligned.")
                    print("   Focus on the small percentage of mismatches for final cleanup.")
                elif overall["total_match_rate"] > 70:
                    print("⚠️  GOOD ALIGNMENT WITH ROOM FOR IMPROVEMENT:")
                    print("   Most products are matched between API and CSV.")
                    print("   Investigate countries with low match rates for data quality issues.")
                else:
                    print("🔍 SIGNIFICANT DATA INCONSISTENCIES DETECTED:")
                    print("   Large gaps between API and CSV data require investigation.")
                    print("   Consider data source validation and synchronization improvements.")
                    print("   Review API filtering and CSV data generation processes.")

        except Exception as e:
            logger.error(f"CW Catalog comparison failed: {e}", exc_info=True)
            print(f"Error: {e}")
            exit(1)
