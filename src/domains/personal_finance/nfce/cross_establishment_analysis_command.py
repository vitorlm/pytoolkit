#!/usr/bin/env python3
"""
Cross-Establishment Product Analysis Command - Analyze products that appear across multiple establishments
"""

from argparse import ArgumentParser, Namespace
from typing import Dict, Any
from datetime import datetime

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from utils.data.json_manager import JSONManager
from domains.personal_finance.nfce.cross_establishment_analysis_service import CrossEstablishmentAnalysisService


class CrossEstablishmentAnalysisCommand(BaseCommand):
    
    @staticmethod
    def get_name() -> str:
        return "cross-establishment-analysis"
    
    @staticmethod
    def get_description() -> str:
        return "Analyze products that appear across multiple establishments"
    
    @staticmethod
    def get_help() -> str:
        return """
Analyze products that appear across multiple establishments to identify:
1. Products with high cross-establishment frequency
2. Potential branded vs generic products
3. Regional vs national product distribution
4. Price variation patterns across establishments
5. Unit standardization opportunities

Examples:
  # Basic cross-establishment analysis
  python src/main.py personal_finance nfce cross-establishment-analysis

  # Focus on products in 3+ establishments
  python src/main.py personal_finance nfce cross-establishment-analysis --min-establishments 3

  # Include price analysis
  python src/main.py personal_finance nfce cross-establishment-analysis --include-prices

  # Export detailed results
  python src/main.py personal_finance nfce cross-establishment-analysis --export-excel --detailed
        """
    
    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--min-establishments",
            type=int,
            default=2,
            help="Minimum number of establishments a product must appear in (default: 2)"
        )
        
        parser.add_argument(
            "--include-prices",
            action="store_true",
            help="Include price analysis across establishments"
        )
        
        parser.add_argument(
            "--detailed",
            action="store_true",
            help="Generate detailed analysis with establishment breakdowns"
        )
        
        parser.add_argument(
            "--export-csv",
            action="store_true",
            help="Export results to CSV format"
        )
        
        parser.add_argument(
            "--export-excel",
            action="store_true",
            help="Export results to Excel format with multiple sheets"
        )
        
        parser.add_argument(
            "--output",
            help="Output file for results (default: cross_establishment_analysis_{timestamp}.json)"
        )
        
        parser.add_argument(
            "--category-focus",
            choices=["beverages", "fruits", "dairy", "meat", "bread", "all"],
            default="all",
            help="Focus analysis on specific product category"
        )

    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("CrossEstablishmentAnalysisCommand")
        
        try:
            logger.info("Starting cross-establishment product analysis")
            
            # Generate output filename
            if args.output:
                output_path = args.output
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = f"output/cross_establishment_analysis_{timestamp}.json"
            
            # Initialize service
            service = CrossEstablishmentAnalysisService()
            
            # Perform analysis
            analysis_results = service.analyze_cross_establishment_products(
                min_establishments=args.min_establishments,
                include_prices=args.include_prices,
                detailed=args.detailed,
                category_focus=args.category_focus
            )
            
            # Export to additional formats if requested
            if args.export_csv:
                service.export_to_csv(analysis_results, output_path.replace('.json', '.csv'))
            
            if args.export_excel:
                service.export_to_excel(analysis_results, output_path.replace('.json', '.xlsx'))
            
            # Print summary
            CrossEstablishmentAnalysisCommand._print_analysis_summary(analysis_results)
            
            # Save results
            logger.info(f"Saving analysis results to: {output_path}")
            JSONManager.write_json(analysis_results, output_path)
            
            logger.info("Cross-establishment analysis completed successfully")
            print(f"Results saved to: {output_path}")
            
        except Exception as e:
            logger.error(f"Cross-establishment analysis failed: {e}", exc_info=True)
            print(f"Error: {e}")
            exit(1)
    
    @staticmethod
    def _print_analysis_summary(results: Dict[str, Any]):
        """Print analysis summary to console"""
        
        metadata = results["metadata"]
        stats = results["statistics"]
        top_products = results.get("top_cross_establishment_products", [])
        
        print("\n" + "="*80)
        print("🏪 CROSS-ESTABLISHMENT PRODUCT ANALYSIS SUMMARY")
        print("="*80)
        print(f"Total Products Analyzed: {metadata['total_products']}")
        print(f"Cross-Establishment Products: {stats['cross_establishment_products']}")
        print(f"Cross-Establishment Rate: {stats['cross_establishment_rate']:.1f}%")
        print(f"Average Establishments per Cross Product: {stats['avg_establishments_per_cross_product']:.1f}")
        
        print("\n🏆 TOP CROSS-ESTABLISHMENT PRODUCTS:")
        for i, product in enumerate(top_products[:10], 1):
            est_count = product['establishment_count']
            desc = product['master_description'][:50]
            print(f"  {i:2d}. {desc:<50} ({est_count} establishments)")
        
        if "establishment_analysis" in results:
            est_analysis = results["establishment_analysis"]
            print("\n🏢 ESTABLISHMENT ANALYSIS:")
            print(f"Total Establishments: {est_analysis['total_establishments']}")
            print(f"Avg Cross Products per Establishment: {est_analysis['avg_cross_products_per_establishment']:.1f}")
            
            print("\n🏪 TOP ESTABLISHMENTS BY CROSS PRODUCTS:")
            top_establishments = est_analysis.get("top_establishments_by_cross_products", [])
            for i, est in enumerate(top_establishments[:5], 1):
                name = est['business_name'][:40]
                count = est['cross_product_count']
                print(f"  {i}. {name:<40} ({count} cross products)")
        
        if "category_analysis" in results:
            cat_analysis = results["category_analysis"]
            print("\n📦 CATEGORY DISTRIBUTION (Cross-Establishment):")
            categories = cat_analysis.get("cross_establishment_by_category", {})
            sorted_categories = sorted(categories.items(), key=lambda x: x[1], reverse=True)
            for category, count in sorted_categories[:8]:
                print(f"  {category.title()}: {count}")
        
        if "price_analysis" in results and results["price_analysis"]["available"]:
            price_analysis = results["price_analysis"]
            print("\n💰 PRICE ANALYSIS:")
            print(f"Products with Price Data: {price_analysis['products_with_prices']}")
            print(f"Avg Price Variation: {price_analysis['avg_price_variation']:.1f}%")
            
            high_variation = price_analysis.get("high_price_variation_products", [])
            if high_variation:
                print("\n💸 HIGH PRICE VARIATION PRODUCTS:")
                for product in high_variation[:5]:
                    desc = product['description'][:40]
                    variation = product['price_variation_percent']
                    print(f"  {desc:<40} ({variation:.1f}% variation)")
        
        print("\n💡 KEY INSIGHTS:")
        insights = results.get("insights", [])
        for insight in insights:
            print(f"  • {insight}")
        
        print("="*80)
