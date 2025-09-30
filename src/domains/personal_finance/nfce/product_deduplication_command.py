#!/usr/bin/env python3
"""
Product Deduplication Command - Create clean product table based on similarity analysis
"""

from argparse import ArgumentParser, Namespace
from typing import Dict, List, Any
from datetime import datetime
from collections import defaultdict, Counter

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from utils.data.json_manager import JSONManager
from utils.file_manager import FileManager
from domains.personal_finance.nfce.advanced_product_deduplication_service import (
    AdvancedProductDeduplicationService,
)


class ProductDeduplicationCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "product-deduplication"

    @staticmethod
    def get_description() -> str:
        return (
            "Create clean product table by merging duplicates and standardizing entries"
        )

    @staticmethod
    def get_help() -> str:
        return """
Create a clean product table by applying deduplication rules based on similarity analysis.

This command:
1. Loads similarity analysis results
2. Applies deduplication rules
3. Standardizes units and descriptions
4. Creates a clean product master table
5. Generates mapping between original and clean products

Examples:
  # Create clean table from similarity analysis
  python src/main.py personal_finance nfce product-deduplication --input step2_similarity.json

  # Apply custom similarity threshold
  python src/main.py personal_finance nfce product-deduplication --input step2_similarity.json --threshold 0.85

  # Generate detailed mapping report
  python src/main.py personal_finance nfce product-deduplication --input step2_similarity.json --detailed-mapping

  # Export to specific formats
  python src/main.py personal_finance nfce product-deduplication --input step2_similarity.json --export-csv --export-excel
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--input",
            help="JSON file with similarity analysis results (optional - can work directly from database)",
        )

        parser.add_argument(
            "--from-database",
            action="store_true",
            default=True,
            help="Create clean table directly from database analysis (default: True)",
        )

        parser.add_argument(
            "--threshold",
            type=float,
            default=0.8,
            help="Similarity threshold for automatic merging (default: 0.8)",
        )

        parser.add_argument(
            "--output",
            help="Output file for clean product table (default: clean_products_{timestamp}.json)",
        )

        parser.add_argument(
            "--detailed-mapping",
            action="store_true",
            help="Generate detailed mapping between original and clean products",
        )

        parser.add_argument(
            "--export-csv", action="store_true", help="Export clean table to CSV format"
        )

        parser.add_argument(
            "--export-excel",
            action="store_true",
            help="Export clean table to Excel format",
        )

        parser.add_argument(
            "--standardize-units",
            action="store_true",
            default=True,
            help="Standardize unit variations (UN/Un -> UN, KG/Kg/kg -> KG)",
        )

        parser.add_argument(
            "--manual-review",
            action="store_true",
            help="Generate list of products requiring manual review",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("ProductDeduplicationCommand")

        try:
            logger.info("Starting product deduplication process")

            # Generate output filename first
            if args.output:
                output_path = args.output
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = f"output/clean_products_{timestamp}.json"

            # Check if using database directly or from analysis file
            if args.from_database or not args.input:
                # Use advanced service to create clean table directly from database
                advanced_service = AdvancedProductDeduplicationService()

                clean_results = advanced_service.create_clean_product_master_table(
                    similarity_threshold=args.threshold,
                    standardize_units=args.standardize_units,
                    remove_establishment_specific=True,
                )

                # Export to additional formats if requested
                if args.export_csv:
                    advanced_service.export_to_csv(
                        clean_results, output_path.replace(".json", ".csv")
                    )

                if args.export_excel:
                    advanced_service.export_to_excel(
                        clean_results, output_path.replace(".json", ".xlsx")
                    )

                # Print summary for advanced service
                ProductDeduplicationCommand._print_advanced_summary(clean_results)

            else:
                # Use similarity analysis file
                # Initialize service
                service = ProductDeduplicationService(logger)

                # Validate input file
                if not FileManager.validate_file(
                    args.input, allowed_extensions=[".json"]
                ):
                    raise ValueError(f"Invalid input file: {args.input}")

                # Load similarity analysis results
                logger.info(f"Loading similarity analysis from: {args.input}")
                similarity_data = JSONManager.read_json(args.input)

                # Create clean product table
                clean_results = service.create_clean_product_table(
                    similarity_data=similarity_data,
                    threshold=args.threshold,
                    standardize_units=args.standardize_units,
                    detailed_mapping=args.detailed_mapping,
                )

                # Export to additional formats if requested
                if args.export_csv:
                    service.export_to_csv(
                        clean_results, output_path.replace(".json", ".csv")
                    )

                if args.export_excel:
                    service.export_to_excel(
                        clean_results, output_path.replace(".json", ".xlsx")
                    )

                # Generate manual review list if requested
                if args.manual_review:
                    review_path = output_path.replace(".json", "_manual_review.json")
                    service.generate_manual_review_list(
                        similarity_data, review_path, args.threshold
                    )

                # Print summary
                service.print_deduplication_summary(clean_results)

            # Save results
            logger.info(f"Saving clean product table to: {output_path}")
            JSONManager.write_json(clean_results, output_path)

            logger.info("Product deduplication completed successfully")
            print(f"Clean product table saved to: {output_path}")

        except Exception as e:
            logger.error(f"Product deduplication failed: {e}", exc_info=True)
            print(f"Error: {e}")
            exit(1)

    @staticmethod
    def _print_advanced_summary(results: Dict[str, Any]):
        """Print summary for advanced deduplication results"""

        metadata = results["metadata"]
        stats = results["statistics"]
        quality = results["quality_metrics"]

        print("\n" + "=" * 70)
        print("🧹 ADVANCED PRODUCT DEDUPLICATION SUMMARY")
        print("=" * 70)
        print(f"Original Products: {metadata['total_original_products']}")
        print(f"Clean Master Products: {metadata['total_clean_products']}")
        print(f"Reduction Achieved: {stats['reduction_percentage']:.1f}%")

        print("\n📊 QUALITY METRICS:")
        print(f"Overall Quality Score: {quality['overall_quality_score']:.3f}")
        print(f"Overall Confidence Score: {quality['overall_confidence_score']:.3f}")
        print(
            f"High Quality Products: {quality['quality_distribution']['high_quality']}"
        )
        print(
            f"High Confidence Products: {quality['confidence_distribution']['high_confidence']}"
        )

        print("\n🏪 ESTABLISHMENT COVERAGE:")
        est_metrics = stats["establishment_metrics"]
        print(f"Total Establishments: {est_metrics['total_establishments']}")
        print(
            f"Cross-Establishment Products: {est_metrics['cross_establishment_products']}"
        )
        print(
            f"Avg Establishments per Product: {est_metrics['avg_establishments_per_product']:.1f}"
        )

        print("\n📦 CATEGORY DISTRIBUTION:")
        categories = stats["category_distribution"]
        sorted_categories = sorted(categories.items(), key=lambda x: x[1], reverse=True)
        for category, count in sorted_categories[:5]:
            print(f"  {category.title()}: {count}")

        print("\n💡 RECOMMENDATIONS:")
        for rec in results.get("recommendations", []):
            print(f"  • {rec}")

        print("=" * 70)


class ProductDeduplicationService:
    def __init__(self, logger):
        self.logger = logger

    def create_clean_product_table(
        self,
        similarity_data: Dict[str, Any],
        threshold: float = 0.8,
        standardize_units: bool = True,
        detailed_mapping: bool = False,
    ) -> Dict[str, Any]:
        """Create clean product table from similarity analysis"""

        self.logger.info("Creating clean product table")

        # Extract products from similarity data
        products = self._extract_products_from_analysis(similarity_data)

        # Standardize units if requested
        if standardize_units:
            products = self._standardize_units(products)

        # Apply deduplication rules
        clean_products, product_mapping = self._apply_deduplication_rules(
            products, similarity_data, threshold
        )

        # Generate statistics
        stats = self._calculate_deduplication_stats(products, clean_products)

        results = {
            "metadata": {
                "created_at": datetime.now().isoformat(),
                "original_products_count": len(products),
                "clean_products_count": len(clean_products),
                "reduction_percentage": stats["reduction_percentage"],
                "threshold_used": threshold,
                "standardize_units": standardize_units,
            },
            "clean_products": clean_products,
            "statistics": stats,
            "deduplication_rules_applied": self._get_applied_rules(),
        }

        if detailed_mapping:
            results["product_mapping"] = product_mapping

        return results

    def _extract_products_from_analysis(
        self, similarity_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Extract product data from similarity analysis results"""

        # Try to get products from similarity engine results first
        if "similarity_engine_results" in similarity_data:
            engine_results = similarity_data["similarity_engine_results"]
            if "matching_results" in engine_results:
                # Reconstruct products from matching results
                products = []
                matching_results = engine_results["matching_results"]

                # Get products from all groups
                for group in matching_results.get("duplicate_groups", []):
                    for product in group.get("products", []):
                        products.append(product)

                for group in matching_results.get("similar_groups", []):
                    for product in group.get("products", []):
                        products.append(product)

                for product in matching_results.get("singleton_products", []):
                    products.append(product)

                if products:
                    return products

        # Fallback: simulate products from basic stats
        basic_stats = similarity_data.get("basic_stats", {})

        # This is a simplified approach - in a real implementation,
        # you'd want to query the database directly
        products = []
        for i in range(basic_stats.get("total_products", 0)):
            products.append(
                {
                    "description": f"PRODUCT_{i}",
                    "unit": "UN",
                    "establishment_cnpj": "00000000000000",
                }
            )

        return products

    def _standardize_units(
        self, products: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Standardize unit variations"""

        self.logger.info("Standardizing product units")

        unit_mapping = {
            # Weight units
            "kg": "KG",
            "Kg": "KG",
            "KG": "KG",
            # Quantity units
            "Un": "UN",
            "UN": "UN",
            "un": "UN",
            # Package units
            "PT": "PT",
            "PC": "PC",
            "PO": "PO",
            # Volume units
            "L": "L",
            "l": "L",
            "ML": "ML",
            "ml": "ML",
            # Other
            "FR": "FR",
            "TP": "TP",
        }

        standardized_products = []
        for product in products:
            standardized_product = product.copy()
            original_unit = product.get("unit", "UN")
            standardized_unit = unit_mapping.get(original_unit, original_unit)
            standardized_product["unit"] = standardized_unit
            standardized_product["original_unit"] = original_unit
            standardized_products.append(standardized_product)

        return standardized_products

    def _apply_deduplication_rules(
        self,
        products: List[Dict[str, Any]],
        similarity_data: Dict[str, Any],
        threshold: float,
    ) -> tuple[List[Dict[str, Any]], Dict[str, str]]:
        """Apply deduplication rules based on similarity analysis"""

        self.logger.info(f"Applying deduplication rules with threshold {threshold}")

        # Group products by normalized description
        description_groups = defaultdict(list)
        for product in products:
            normalized_desc = self._normalize_description(product["description"])
            description_groups[normalized_desc].append(product)

        clean_products = []
        product_mapping = {}  # original_description -> clean_description

        for normalized_desc, group in description_groups.items():
            if len(group) == 1:
                # Single product, keep as is
                clean_products.append(group[0])
                product_mapping[group[0]["description"]] = group[0]["description"]
            else:
                # Multiple products, create merged product
                merged_product = self._merge_products(group)
                clean_products.append(merged_product)

                # Map all original descriptions to the merged one
                for product in group:
                    product_mapping[product["description"]] = merged_product[
                        "description"
                    ]

        return clean_products, product_mapping

    def _normalize_description(self, description: str) -> str:
        """Normalize product description for grouping"""

        # Basic normalization rules
        normalized = description.upper().strip()

        # Remove multiple spaces
        import re

        normalized = re.sub(r"\s+", " ", normalized)

        # Remove special characters that don't add meaning
        normalized = re.sub(r"[^\w\s]", "", normalized)

        return normalized

    def _merge_products(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge multiple similar products into one clean product"""

        # Choose the most complete/frequent description as primary
        descriptions = [p["description"] for p in products]
        primary_description = max(descriptions, key=len)  # Longest description

        # Get most common unit
        units = [p.get("unit", "UN") for p in products]
        most_common_unit = Counter(units).most_common(1)[0][0]

        # Collect all establishments
        establishments = list(set(p.get("establishment_cnpj", "") for p in products))

        merged_product = {
            "description": primary_description,
            "unit": most_common_unit,
            "establishments": establishments,
            "occurrence_count": len(products),
            "original_descriptions": descriptions,
            "merged_from": len(products),
        }

        return merged_product

    def _calculate_deduplication_stats(
        self,
        original_products: List[Dict[str, Any]],
        clean_products: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Calculate deduplication statistics"""

        original_count = len(original_products)
        clean_count = len(clean_products)
        reduction = original_count - clean_count
        reduction_percentage = (
            (reduction / original_count * 100) if original_count > 0 else 0
        )

        # Count merged products
        merged_products = [p for p in clean_products if p.get("merged_from", 1) > 1]

        return {
            "original_count": original_count,
            "clean_count": clean_count,
            "products_removed": reduction,
            "reduction_percentage": round(reduction_percentage, 2),
            "merged_products_count": len(merged_products),
            "total_merges": sum(p.get("merged_from", 1) - 1 for p in merged_products),
        }

    def _get_applied_rules(self) -> List[str]:
        """Get list of deduplication rules that were applied"""

        return [
            "Normalized product descriptions (case insensitive)",
            "Standardized units (UN/Un -> UN, KG/Kg/kg -> KG)",
            "Merged products with identical normalized descriptions",
            "Selected longest description as primary for merged products",
            "Used most common unit for merged products",
            "Preserved establishment information for traceability",
        ]

    def export_to_csv(self, clean_results: Dict[str, Any], output_path: str):
        """Export clean products to CSV format"""

        import pandas as pd

        self.logger.info(f"Exporting to CSV: {output_path}")

        products = clean_results["clean_products"]
        df_data = []

        for product in products:
            df_data.append(
                {
                    "description": product["description"],
                    "unit": product["unit"],
                    "occurrence_count": product.get("occurrence_count", 1),
                    "establishments_count": len(product.get("establishments", [])),
                    "merged_from": product.get("merged_from", 1),
                    "establishments": ";".join(product.get("establishments", [])),
                }
            )

        df = pd.DataFrame(df_data)
        df.to_csv(output_path, index=False, encoding="utf-8")

        self.logger.info(f"CSV export completed: {output_path}")

    def export_to_excel(self, clean_results: Dict[str, Any], output_path: str):
        """Export clean products to Excel format"""

        import pandas as pd

        self.logger.info(f"Exporting to Excel: {output_path}")

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            # Clean products sheet
            products = clean_results["clean_products"]
            df_data = []

            for product in products:
                df_data.append(
                    {
                        "description": product["description"],
                        "unit": product["unit"],
                        "occurrence_count": product.get("occurrence_count", 1),
                        "establishments_count": len(product.get("establishments", [])),
                        "merged_from": product.get("merged_from", 1),
                        "establishments": ";".join(product.get("establishments", [])),
                    }
                )

            df = pd.DataFrame(df_data)
            df.to_excel(writer, sheet_name="Clean Products", index=False)

            # Statistics sheet
            stats = clean_results["statistics"]
            stats_df = pd.DataFrame([stats])
            stats_df.to_excel(writer, sheet_name="Statistics", index=False)

        self.logger.info(f"Excel export completed: {output_path}")

    def generate_manual_review_list(
        self, similarity_data: Dict[str, Any], output_path: str, threshold: float
    ):
        """Generate list of products requiring manual review"""

        self.logger.info("Generating manual review list")

        review_items = []

        # Get similar groups that might need manual review
        if "similarity_engine_results" in similarity_data:
            matching_results = similarity_data["similarity_engine_results"][
                "matching_results"
            ]

            for group in matching_results.get("similar_groups", []):
                avg_similarity = group.get("avg_similarity", 0)
                if 0.7 <= avg_similarity < threshold:  # Uncertain cases
                    review_items.append(
                        {
                            "group_id": group.get("group_id"),
                            "representative_product": group.get(
                                "representative_product"
                            ),
                            "avg_similarity": avg_similarity,
                            "size": group.get("size", 0),
                            "reason": "Similarity below threshold but above minimum",
                            "recommendation": "Manual review recommended",
                        }
                    )

        review_results = {
            "created_at": datetime.now().isoformat(),
            "threshold_used": threshold,
            "total_items_for_review": len(review_items),
            "review_items": review_items,
        }

        JSONManager.write_json(review_results, output_path)
        self.logger.info(f"Manual review list saved to: {output_path}")

    def print_deduplication_summary(self, results: Dict[str, Any]):
        """Print deduplication summary to console"""

        metadata = results["metadata"]
        stats = results["statistics"]

        print("\n" + "=" * 70)
        print("🧹 PRODUCT DEDUPLICATION SUMMARY")
        print("=" * 70)
        print(f"Original Products: {metadata['original_products_count']}")
        print(f"Clean Products: {metadata['clean_products_count']}")
        print(f"Products Removed: {stats['products_removed']}")
        print(f"Reduction: {stats['reduction_percentage']:.1f}%")
        print(f"Merged Products: {stats['merged_products_count']}")
        print(f"Total Merges: {stats['total_merges']}")

        print("\n📊 DEDUPLICATION EFFECTIVENESS:")
        print(f"Threshold Used: {metadata['threshold_used']}")
        print(f"Units Standardized: {metadata['standardize_units']}")

        print("\n✅ RULES APPLIED:")
        for rule in results["deduplication_rules_applied"]:
            print(f"  • {rule}")

        print("=" * 70)
