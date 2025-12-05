#!/usr/bin/env python3
"""Cross-Establishment Analysis Service - Analyze products across multiple establishments"""

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

import pandas as pd

from domains.personal_finance.nfce.database.nfce_database_manager import (
    NFCeDatabaseManager,
)
from utils.logging.logging_manager import LogManager


class CrossEstablishmentAnalysisService:
    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("CrossEstablishmentAnalysisService")
        self.db_manager = NFCeDatabaseManager()

    def analyze_cross_establishment_products(
        self,
        min_establishments: int = 2,
        include_prices: bool = False,
        detailed: bool = False,
        category_focus: str = "all",
    ) -> dict[str, Any]:
        """Analyze products that appear across multiple establishments"""
        self.logger.info("Starting cross-establishment product analysis")

        # Load products with establishment info
        products_data = self._load_products_with_establishments()

        # Group products by normalized description
        product_groups = self._group_products_by_description(products_data)

        # Filter cross-establishment products
        cross_establishment_products = self._filter_cross_establishment_products(product_groups, min_establishments)

        # Apply category filter if specified
        if category_focus != "all":
            cross_establishment_products = self._filter_by_category(cross_establishment_products, category_focus)

        # Calculate statistics
        statistics = self._calculate_statistics(products_data, cross_establishment_products, min_establishments)

        # Analyze establishments
        establishment_analysis = self._analyze_establishments(cross_establishment_products)

        # Category analysis
        category_analysis = self._analyze_categories(cross_establishment_products)

        # Price analysis if requested
        price_analysis = {}
        if include_prices:
            price_analysis = self._analyze_prices(cross_establishment_products)

        # Generate insights
        insights = self._generate_insights(cross_establishment_products, statistics, establishment_analysis)

        # Prepare results
        results = {
            "metadata": {
                "created_at": datetime.now().isoformat(),
                "min_establishments": min_establishments,
                "category_focus": category_focus,
                "include_prices": include_prices,
                "detailed": detailed,
                "total_products": len(products_data),
            },
            "statistics": statistics,
            "top_cross_establishment_products": self._get_top_cross_establishment_products(
                cross_establishment_products, limit=20
            ),
            "establishment_analysis": establishment_analysis,
            "category_analysis": category_analysis,
            "insights": insights,
        }

        if include_prices:
            results["price_analysis"] = price_analysis

        if detailed:
            results["detailed_products"] = cross_establishment_products

        return results

    def _load_products_with_establishments(self) -> list[dict[str, Any]]:
        """Load all products with establishment information"""
        self.logger.info("Loading products with establishment information")

        query = """
        SELECT 
            p.id,
            p.description,
            p.unit,
            p.product_code,
            p.occurrence_count,
            p.created_at,
            e.cnpj as establishment_cnpj,
            e.business_name as establishment_name,
            e.city as establishment_city,
            e.state as establishment_state
        FROM products p
        LEFT JOIN establishments e ON p.establishment_cnpj = e.cnpj
        ORDER BY p.description, e.business_name
        """

        conn = self.db_manager.db_manager.get_connection("nfce_db")
        result = conn.execute(query).fetchall()

        products = []
        for row in result:
            products.append(
                {
                    "id": row[0],
                    "description": row[1],
                    "unit": row[2],
                    "product_code": row[3],
                    "occurrence_count": row[4],
                    "created_at": row[5],
                    "establishment_cnpj": row[6],
                    "establishment_name": row[7],
                    "establishment_city": row[8],
                    "establishment_state": row[9],
                }
            )

        self.logger.info(f"Loaded {len(products)} products with establishment info")
        return products

    def _group_products_by_description(self, products: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        """Group products by normalized description"""
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for product in products:
            normalized_desc = self._normalize_description(product["description"])
            groups[normalized_desc].append(product)

        return dict(groups)

    def _normalize_description(self, description: str) -> str:
        """Normalize product description for grouping"""
        import re

        # Convert to uppercase and strip
        normalized = description.upper().strip()

        # Remove multiple spaces
        normalized = re.sub(r"\s+", " ", normalized)

        # Standardize common abbreviations
        abbreviations = {
            "KG": "KG",
            "ML": "ML",
            "GR": "G",
            "GRAMA": "G",
            "GRAMAS": "G",
            "LITRO": "L",
            "LITROS": "L",
        }

        for abbrev, standard in abbreviations.items():
            normalized = re.sub(r"\b" + abbrev + r"\b", standard, normalized)

        return normalized

    def _filter_cross_establishment_products(
        self, product_groups: dict[str, list[dict[str, Any]]], min_establishments: int
    ) -> list[dict[str, Any]]:
        """Filter products that appear in multiple establishments"""
        cross_establishment_products = []

        for normalized_desc, products in product_groups.items():
            # Get unique establishments for this product group
            establishments = set(p["establishment_cnpj"] for p in products)

            if len(establishments) >= min_establishments:
                # Create consolidated product entry
                cross_product = self._consolidate_product_group(products, normalized_desc)
                cross_establishment_products.append(cross_product)

        # Sort by establishment count (descending)
        cross_establishment_products.sort(key=lambda x: x["establishment_count"], reverse=True)

        return cross_establishment_products

    def _consolidate_product_group(self, products: list[dict[str, Any]], normalized_desc: str) -> dict[str, Any]:
        """Consolidate a group of similar products into one entry"""
        # Get unique establishments
        establishments = {}
        for product in products:
            cnpj = product["establishment_cnpj"]
            if cnpj not in establishments:
                establishments[cnpj] = {
                    "cnpj": cnpj,
                    "business_name": product["establishment_name"],
                    "city": product["establishment_city"],
                    "state": product["establishment_state"],
                    "product_count": 0,
                    "products": [],
                }
            establishments[cnpj]["product_count"] += 1
            establishments[cnpj]["products"].append(product)

        # Get all unique descriptions
        original_descriptions = list(set(p["description"] for p in products))

        # Choose primary description (longest)
        primary_description = max(original_descriptions, key=len)

        # Get most common unit
        units = [p["unit"] for p in products]
        most_common_unit = Counter(units).most_common(1)[0][0]

        # Determine category
        category = self._determine_category(primary_description)

        return {
            "normalized_description": normalized_desc,
            "master_description": primary_description,
            "original_descriptions": original_descriptions,
            "unit": most_common_unit,
            "category": category,
            "establishment_count": len(establishments),
            "total_products": len(products),
            "total_occurrences": sum(p["occurrence_count"] for p in products),
            "establishments": list(establishments.values()),
            "quality_score": self._calculate_quality_score(products, establishments),
        }

    def _determine_category(self, description: str) -> str:
        """Determine product category based on description"""
        desc_upper = description.upper()

        # Category keywords
        categories = {
            "fruits": [
                "BANANA",
                "MAÇA",
                "LARANJA",
                "LIMAO",
                "UVA",
                "MEXERICA",
                "ABACAXI",
            ],
            "vegetables": [
                "TOMATE",
                "CEBOLA",
                "BATATA",
                "CENOURA",
                "ALFACE",
                "REPOLHO",
            ],
            "beverages": ["CAFE", "CHA", "SUCO", "REFRIGERANTE", "AGUA", "CERVEJA"],
            "dairy": ["LEITE", "QUEIJO", "IOGURTE", "MANTEIGA", "REQUEIJAO"],
            "meat": ["CARNE", "FRANGO", "PEIXE", "LINGUICA", "SALSICHA"],
            "bread": ["PAO", "BOLO", "BISCOITO", "TORRADA"],
            "grains": ["ARROZ", "FEIJAO", "MACARRAO", "FARINHA"],
            "condiments": ["SAL", "AÇUCAR", "OLEO", "VINAGRE", "TEMPERO"],
        }

        for category, keywords in categories.items():
            for keyword in keywords:
                if keyword in desc_upper:
                    return category

        return "uncategorized"

    def _calculate_quality_score(self, products: list[dict[str, Any]], establishments: dict[str, Any]) -> float:
        """Calculate quality score for cross-establishment product"""
        # Base score
        score = 0.5

        # Bonus for more establishments
        establishment_bonus = min(len(establishments) * 0.1, 0.3)
        score += establishment_bonus

        # Bonus for consistent descriptions
        unique_descriptions = len(set(p["description"] for p in products))
        if unique_descriptions == 1:
            score += 0.2  # Perfect consistency
        elif unique_descriptions <= 3:
            score += 0.1  # Good consistency

        # Bonus for higher occurrence count
        total_occurrences = sum(p["occurrence_count"] for p in products)
        if total_occurrences >= 10:
            score += 0.1
        elif total_occurrences >= 5:
            score += 0.05

        return min(score, 1.0)

    def _filter_by_category(self, cross_products: list[dict[str, Any]], category: str) -> list[dict[str, Any]]:
        """Filter products by category"""
        return [p for p in cross_products if p["category"] == category]

    def _calculate_statistics(
        self,
        all_products: list[dict[str, Any]],
        cross_products: list[dict[str, Any]],
        min_establishments: int,
    ) -> dict[str, Any]:
        """Calculate cross-establishment statistics"""
        total_products = len(all_products)
        cross_count = len(cross_products)
        cross_rate = (cross_count / total_products * 100) if total_products > 0 else 0

        if cross_products:
            avg_establishments = sum(p["establishment_count"] for p in cross_products) / len(cross_products)
            max_establishments = max(p["establishment_count"] for p in cross_products)
            total_cross_occurrences = sum(p["total_occurrences"] for p in cross_products)
        else:
            avg_establishments = 0
            max_establishments = 0
            total_cross_occurrences = 0

        return {
            "total_products_analyzed": total_products,
            "cross_establishment_products": cross_count,
            "cross_establishment_rate": round(cross_rate, 2),
            "min_establishments_required": min_establishments,
            "avg_establishments_per_cross_product": round(avg_establishments, 2),
            "max_establishments_for_product": max_establishments,
            "total_cross_establishment_occurrences": total_cross_occurrences,
        }

    def _analyze_establishments(self, cross_products: list[dict[str, Any]]) -> dict[str, Any]:
        """Analyze establishment patterns in cross products"""
        establishment_stats: dict[str, int] = defaultdict(int)
        establishment_info = {}

        for product in cross_products:
            for est in product["establishments"]:
                cnpj = est["cnpj"]
                establishment_stats[cnpj] += 1
                if cnpj not in establishment_info:
                    establishment_info[cnpj] = {
                        "cnpj": cnpj,
                        "business_name": est["business_name"],
                        "city": est["city"],
                        "state": est["state"],
                    }

        # Get top establishments by cross-product count
        top_establishments = []
        establishment_counter = Counter(establishment_stats)
        for cnpj, count in establishment_counter.most_common(10):
            est_info = establishment_info[cnpj].copy()
            est_info["cross_product_count"] = count
            top_establishments.append(est_info)

        return {
            "total_establishments": len(establishment_stats),
            "avg_cross_products_per_establishment": round(
                sum(establishment_stats.values()) / len(establishment_stats), 2
            )
            if establishment_stats
            else 0,
            "top_establishments_by_cross_products": top_establishments,
            "establishment_distribution": dict(Counter(establishment_stats.values())),
        }

    def _analyze_categories(self, cross_products: list[dict[str, Any]]) -> dict[str, Any]:
        """Analyze category distribution in cross products"""
        category_counts = Counter(p["category"] for p in cross_products)

        category_establishment_avg = {}
        for category in category_counts:
            category_products = [p for p in cross_products if p["category"] == category]
            avg_establishments = sum(p["establishment_count"] for p in category_products) / len(category_products)
            category_establishment_avg[category] = round(avg_establishments, 2)

        return {
            "cross_establishment_by_category": dict(category_counts),
            "avg_establishments_per_category": category_establishment_avg,
            "most_distributed_category": category_counts.most_common(1)[0][0] if category_counts else None,
        }

    def _analyze_prices(self, cross_products: list[dict[str, Any]]) -> dict[str, Any]:
        """Analyze price variations across establishments"""
        # Note: This is a placeholder for price analysis
        # In a real implementation, you would query invoice_items for price data

        return {
            "available": False,
            "note": "Price analysis requires integration with invoice_items table",
            "recommendation": "Implement price tracking to enable cross-establishment price comparison",
        }

    def _get_top_cross_establishment_products(
        self, cross_products: list[dict[str, Any]], limit: int = 20
    ) -> list[dict[str, Any]]:
        """Get top cross-establishment products"""
        # Already sorted by establishment count
        top_products = []
        for product in cross_products[:limit]:
            top_products.append(
                {
                    "master_description": product["master_description"],
                    "establishment_count": product["establishment_count"],
                    "total_occurrences": product["total_occurrences"],
                    "category": product["category"],
                    "quality_score": product["quality_score"],
                    "unit": product["unit"],
                }
            )

        return top_products

    def _generate_insights(
        self,
        cross_products: list[dict[str, Any]],
        statistics: dict[str, Any],
        establishment_analysis: dict[str, Any],
    ) -> list[str]:
        """Generate analytical insights"""
        insights = []

        cross_rate = statistics["cross_establishment_rate"]
        if cross_rate < 10:
            insights.append(
                f"Low cross-establishment rate ({cross_rate:.1f}%) indicates high product specialization by establishment"
            )
        elif cross_rate > 30:
            insights.append(f"High cross-establishment rate ({cross_rate:.1f}%) suggests many common/standard products")

        if cross_products:
            top_product = cross_products[0]
            insights.append(
                f"Most distributed product: '{top_product['master_description']}' appears in {top_product['establishment_count']} establishments"
            )

        avg_establishments = statistics["avg_establishments_per_cross_product"]
        if avg_establishments > 5:
            insights.append("High cross-establishment products suggest strong brand presence or commodity items")

        total_establishments = establishment_analysis["total_establishments"]
        if total_establishments > 15:
            insights.append(f"Large establishment network ({total_establishments}) provides good geographic coverage")

        return insights

    def export_to_csv(self, results: dict[str, Any], output_path: str):
        """Export cross-establishment analysis to CSV"""
        self.logger.info(f"Exporting cross-establishment analysis to CSV: {output_path}")

        # Prepare data for CSV
        top_products = results["top_cross_establishment_products"]

        df_data = []
        for product in top_products:
            df_data.append(
                {
                    "description": product["master_description"],
                    "establishment_count": product["establishment_count"],
                    "total_occurrences": product["total_occurrences"],
                    "category": product["category"],
                    "quality_score": product["quality_score"],
                    "unit": product["unit"],
                }
            )

        df = pd.DataFrame(df_data)
        df.to_csv(output_path, index=False, encoding="utf-8")

        self.logger.info(f"CSV export completed: {output_path}")

    def export_to_excel(self, results: dict[str, Any], output_path: str):
        """Export cross-establishment analysis to Excel with multiple sheets"""
        self.logger.info(f"Exporting cross-establishment analysis to Excel: {output_path}")

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            # Cross-establishment products sheet
            top_products = results["top_cross_establishment_products"]
            df_products = pd.DataFrame(top_products)
            df_products.to_excel(writer, sheet_name="Cross Products", index=False)

            # Establishment analysis sheet
            est_analysis = results["establishment_analysis"]
            df_establishments = pd.DataFrame(est_analysis["top_establishments_by_cross_products"])
            df_establishments.to_excel(writer, sheet_name="Establishments", index=False)

            # Category analysis sheet
            cat_analysis = results["category_analysis"]
            cat_data = []
            for category, count in cat_analysis["cross_establishment_by_category"].items():
                cat_data.append(
                    {
                        "category": category,
                        "cross_products_count": count,
                        "avg_establishments": cat_analysis["avg_establishments_per_category"].get(category, 0),
                    }
                )
            df_categories = pd.DataFrame(cat_data)
            df_categories.to_excel(writer, sheet_name="Categories", index=False)

            # Statistics sheet
            stats_data = [results["statistics"]]
            df_stats = pd.DataFrame(stats_data)
            df_stats.to_excel(writer, sheet_name="Statistics", index=False)

        self.logger.info(f"Excel export completed: {output_path}")
