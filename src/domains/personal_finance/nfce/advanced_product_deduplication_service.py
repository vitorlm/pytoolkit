#!/usr/bin/env python3
"""
Advanced Product Deduplication Service - Creates clean product table based on database analysis
"""

import re
from typing import Dict, List, Any
from collections import defaultdict, Counter
from datetime import datetime

from utils.logging.logging_manager import LogManager
from domains.personal_finance.nfce.database.nfce_database_manager import (
    NFCeDatabaseManager,
)


class AdvancedProductDeduplicationService:
    def __init__(self):
        self.logger = LogManager.get_instance().get_logger(
            "AdvancedProductDeduplicationService"
        )
        self._db_manager = None

    @property
    def db_manager(self):
        """Lazy-loaded database manager"""
        if self._db_manager is None:
            self._db_manager = NFCeDatabaseManager()
        return self._db_manager

    def create_clean_product_master_table(
        self,
        similarity_threshold: float = 0.85,
        standardize_units: bool = True,
        remove_establishment_specific: bool = True,
    ) -> Dict[str, Any]:
        """Create a comprehensive clean product master table"""

        self.logger.info("Starting advanced product deduplication process")

        # 1. Load all products from database
        all_products = self._load_all_products_from_database()
        self.logger.info(f"Loaded {len(all_products)} products from database")

        # 2. Standardize units
        if standardize_units:
            all_products = self._standardize_units(all_products)
            self.logger.info("Units standardized")

        # 3. Normalize and clean descriptions
        all_products = self._normalize_descriptions(all_products)
        self.logger.info("Descriptions normalized")

        # 4. Group products by similarity
        product_groups = self._group_products_by_similarity(
            all_products, similarity_threshold
        )
        self.logger.info(f"Created {len(product_groups)} product groups")

        # 5. Create clean master products
        clean_master_products = self._create_master_products(product_groups)
        self.logger.info(f"Created {len(clean_master_products)} clean master products")

        # 6. Generate mappings and statistics
        mapping_table = self._generate_mapping_table(
            all_products, clean_master_products
        )
        statistics = self._calculate_comprehensive_stats(
            all_products, clean_master_products
        )

        # 7. Create final results
        results = {
            "metadata": {
                "created_at": datetime.now().isoformat(),
                "similarity_threshold": similarity_threshold,
                "standardize_units": standardize_units,
                "remove_establishment_specific": remove_establishment_specific,
                "total_original_products": len(all_products),
                "total_clean_products": len(clean_master_products),
                "reduction_achieved": statistics["reduction_percentage"],
            },
            "clean_master_products": clean_master_products,
            "product_mapping": mapping_table,
            "statistics": statistics,
            "quality_metrics": self._calculate_quality_metrics(clean_master_products),
            "recommendations": self._generate_cleanup_recommendations(statistics),
        }

        return results

    def _load_all_products_from_database(self) -> List[Dict[str, Any]]:
        """Load all products from the database with establishment context"""

        try:
            conn = self.db_manager.db_manager.get_connection("nfce_db")

            query = """
            SELECT DISTINCT
                p.id,
                p.description,
                p.unit,
                p.product_code,
                e.cnpj as establishment_cnpj,
                e.business_name,
                e.city,
                e.state,
                COUNT(*) OVER (PARTITION BY p.description) as occurrence_count,
                COUNT(*) OVER (PARTITION BY p.description, e.cnpj) as establishment_count
            FROM products p
            JOIN establishments e ON p.establishment_id = e.id
            WHERE p.description IS NOT NULL 
            AND TRIM(p.description) != ''
            ORDER BY p.description, e.cnpj
            """

            results = conn.execute(query).fetchall()

            products = []
            for row in results:
                products.append(
                    {
                        "id": row[0],
                        "description": row[1],
                        "unit": row[2] or "UN",
                        "product_code": row[3],
                        "establishment_cnpj": row[4],
                        "business_name": row[5],
                        "city": row[6],
                        "state": row[7],
                        "occurrence_count": row[8],
                        "establishment_count": row[9],
                        "original_description": row[1],  # Keep original for mapping
                    }
                )

            return products

        except Exception as e:
            self.logger.error(f"Error loading products from database: {e}")
            raise

    def _standardize_units(
        self, products: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Apply comprehensive unit standardization"""

        # Enhanced unit mapping based on your data analysis
        unit_mapping = {
            # Weight units - standardize to KG
            "kg": "KG",
            "Kg": "KG",
            "KG": "KG",
            # Quantity units - standardize to UN
            "Un": "UN",
            "UN": "UN",
            "un": "UN",
            # Package/piece units
            "PT": "PT",  # Pacote
            "PC": "PC",  # Peça
            "PO": "PC",  # Pote -> Peça
            # Volume units
            "L": "L",
            "l": "L",
            "ML": "ML",
            "ml": "ML",
            # Special units
            "FR": "FR",  # Frasco
            "TP": "TP",  # Tipo
        }

        for product in products:
            original_unit = product.get("unit", "UN")
            standardized_unit = unit_mapping.get(original_unit, original_unit)
            product["unit"] = standardized_unit
            product["original_unit"] = original_unit

            # Flag if unit was changed
            product["unit_standardized"] = original_unit != standardized_unit

        return products

    def _normalize_descriptions(
        self, products: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Apply comprehensive description normalization"""

        for product in products:
            original_desc = product["description"]
            normalized = self._apply_normalization_rules(original_desc)

            product["normalized_description"] = normalized
            product["description_changed"] = original_desc != normalized

        return products

    def _apply_normalization_rules(self, description: str) -> str:
        """Apply advanced normalization rules for Brazilian product descriptions"""

        # Step 1: Basic cleanup
        normalized = description.upper().strip()

        # Step 2: Remove multiple spaces
        normalized = re.sub(r"\s+", " ", normalized)

        # Step 3: Standardize common abbreviations found in your data
        abbreviation_mapping = {
            "VM": "VILA MADALENA",  # Common brand abbreviation
            "VC": "VILA CAMPO",
            "INT": "INTEGRAL",
            "TRAD": "TRADICIONAL",
            "DEF": "DEFUMADO",
            "NAC": "NACIONAL",
            "AGRANEL": "A GRANEL",
            "C\\": "COM",
            "C/": "COM",
            "S/": "SEM",
            "P/": "PARA",
        }

        for abbrev, full_form in abbreviation_mapping.items():
            normalized = re.sub(f"\\b{re.escape(abbrev)}\\b", full_form, normalized)

        # Step 4: Standardize weights and measures
        # Convert weight variations: 1KG, 1kg, 1 KG -> 1KG
        normalized = re.sub(r"(\d+)\s*(KG|kg|Kg)", r"\1KG", normalized)
        normalized = re.sub(r"(\d+)\s*(G|g)", r"\1G", normalized)
        normalized = re.sub(r"(\d+)\s*(L|l)", r"\1L", normalized)
        normalized = re.sub(r"(\d+)\s*(ML|ml)", r"\1ML", normalized)

        # Step 5: Remove common noise words that don't add meaning
        noise_patterns = [
            r"\*+",  # Remove asterisks
            r"^\s*-\s*",  # Remove leading dashes
            r"\s*-\s*$",  # Remove trailing dashes
        ]

        for pattern in noise_patterns:
            normalized = re.sub(pattern, "", normalized)

        # Step 6: Final cleanup
        normalized = re.sub(r"\s+", " ", normalized).strip()

        return normalized

    def _group_products_by_similarity(
        self, products: List[Dict[str, Any]], threshold: float
    ) -> List[List[Dict[str, Any]]]:
        """Group products by normalized description similarity"""

        # Group by exact normalized description first
        exact_groups = defaultdict(list)

        for product in products:
            normalized_key = product["normalized_description"]
            exact_groups[normalized_key].append(product)

        # Convert to list of groups
        product_groups = list(exact_groups.values())

        # TODO: In future versions, add fuzzy matching for similar but not identical descriptions
        # For now, we use exact matching after normalization

        return product_groups

    def _create_master_products(
        self, product_groups: List[List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """Create master product records from grouped products"""

        master_products = []

        for group_idx, group in enumerate(product_groups):
            if not group:
                continue

            master_product = self._create_single_master_product(group, group_idx)
            master_products.append(master_product)

        return master_products

    def _create_single_master_product(
        self, group: List[Dict[str, Any]], group_id: int
    ) -> Dict[str, Any]:
        """Create a single master product from a group of similar products"""

        # Choose the best representative description (longest, most complete)
        descriptions = [p["description"] for p in group]
        master_description = max(descriptions, key=len)

        # Get normalized description (should be same for all in group)
        normalized_description = group[0]["normalized_description"]

        # Get most common unit
        units = [p["unit"] for p in group]
        master_unit = Counter(units).most_common(1)[0][0]

        # Aggregate establishment information
        establishments = {}
        for product in group:
            cnpj = product["establishment_cnpj"]
            if cnpj not in establishments:
                establishments[cnpj] = {
                    "cnpj": cnpj,
                    "business_name": product["business_name"],
                    "city": product["city"],
                    "state": product["state"],
                    "product_count": 0,
                }
            establishments[cnpj]["product_count"] += 1

        # Calculate statistics
        total_occurrences = sum(p["occurrence_count"] for p in group)
        unique_establishments = len(establishments)

        # Determine product category (simplified)
        category = self._determine_category(master_description)

        # Generate unique master ID
        master_id = f"MASTER_{group_id:06d}"

        master_product = {
            "master_id": master_id,
            "master_description": master_description,
            "normalized_description": normalized_description,
            "category": category,
            "unit": master_unit,
            "total_occurrences": total_occurrences,
            "unique_establishments": unique_establishments,
            "establishments": list(establishments.values()),
            "original_products_count": len(group),
            "original_descriptions": list(set(descriptions)),
            "quality_score": self._calculate_product_quality_score(group),
            "consolidation_confidence": self._calculate_consolidation_confidence(group),
        }

        return master_product

    def _determine_category(self, description: str) -> str:
        """Determine product category based on description keywords"""

        # Enhanced category mapping based on your data analysis
        category_keywords = {
            "fruits": [
                "ABACATE",
                "ABACAXI",
                "BANANA",
                "LARANJA",
                "LIMAO",
                "MEXERICA",
                "UVA",
                "MACA",
            ],
            "vegetables": ["ALHO", "CHUCHU", "PIMENTAO", "BATATA"],
            "beverages": ["COCA", "SUCO", "CAFE", "CHA", "CERVEJA", "AGUA"],
            "dairy": ["LEITE", "IOGURTE", "IOG", "QUEIJO", "QJO", "MANTEIGA", "MANT"],
            "meat": ["BACON", "FRANGO", "CARNE", "LINGUICA", "LING"],
            "grains": ["ARROZ", "ARR", "FEIJAO", "FEIJ", "AVEIA"],
            "bread": ["PAO", "BISCOITO", "BISC", "BOLO", "BRIOCHE"],
            "condiments": ["MOLHO", "AZEITE", "OLEO", "VINAGRE", "VINAG", "SAL"],
            "snacks": ["BATATA", "BAT", "LAYS", "CHOCOLATE"],
            "personal_care": ["SABONETE", "SHAMPOO", "PASTA", "ESCOVA"],
            "cleaning": ["DETERGENTE", "DET", "SABAO", "AMACIANTE"],
        }

        description_upper = description.upper()

        for category, keywords in category_keywords.items():
            for keyword in keywords:
                if keyword in description_upper:
                    return category

        return "uncategorized"

    def _calculate_product_quality_score(self, group: List[Dict[str, Any]]) -> float:
        """Calculate quality score for a product group (0-1)"""

        # Factors for quality score:
        # 1. Consistency across establishments (higher = better)
        # 2. Description completeness (longer descriptions usually better)
        # 3. Unit consistency

        descriptions = [p["description"] for p in group]
        units = [p["unit"] for p in group]

        # Description consistency (how similar are the descriptions)
        avg_desc_length = sum(len(d) for d in descriptions) / len(descriptions)
        desc_score = min(avg_desc_length / 25.0, 1.0)  # Normalize to 0-1

        # Unit consistency
        unit_consistency = len(set(units)) == 1
        unit_score = 1.0 if unit_consistency else 0.7

        # Establishment diversity (products found in multiple places are usually more standard)
        establishment_count = len(set(p["establishment_cnpj"] for p in group))
        diversity_score = min(establishment_count / 5.0, 1.0)  # Normalize to 0-1

        # Combined score
        quality_score = desc_score * 0.4 + unit_score * 0.3 + diversity_score * 0.3

        return round(quality_score, 3)

    def _calculate_consolidation_confidence(self, group: List[Dict[str, Any]]) -> float:
        """Calculate confidence in consolidation decision (0-1)"""

        # High confidence if:
        # - Descriptions are very similar after normalization
        # - Same units
        # - Found in multiple establishments

        if len(group) == 1:
            return 1.0  # Single product, no consolidation needed

        # Check unit consistency
        units = set(p["unit"] for p in group)
        unit_consistency = len(units) == 1

        # Check description similarity (after normalization, should be identical)
        normalized_descs = set(p["normalized_description"] for p in group)
        desc_consistency = len(normalized_descs) == 1

        # Calculate confidence
        confidence = 0.5  # Base confidence

        if desc_consistency:
            confidence += 0.3

        if unit_consistency:
            confidence += 0.2

        return round(min(confidence, 1.0), 3)

    def _generate_mapping_table(
        self,
        original_products: List[Dict[str, Any]],
        master_products: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Generate mapping table from original products to master products"""

        mapping_table = []

        # Create lookup for master products by normalized description
        master_lookup = {}
        for master in master_products:
            master_lookup[master["normalized_description"]] = master["master_id"]

        for original in original_products:
            normalized_desc = original["normalized_description"]
            master_id = master_lookup.get(normalized_desc, "UNMAPPED")

            mapping_entry = {
                "original_product_id": original["id"],
                "original_description": original["original_description"],
                "normalized_description": normalized_desc,
                "master_id": master_id,
                "establishment_cnpj": original["establishment_cnpj"],
                "business_name": original["business_name"],
                "unit_standardized": original.get("unit_standardized", False),
                "description_changed": original.get("description_changed", False),
            }

            mapping_table.append(mapping_entry)

        return mapping_table

    def _calculate_comprehensive_stats(
        self,
        original_products: List[Dict[str, Any]],
        master_products: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Calculate comprehensive statistics about the deduplication process"""

        original_count = len(original_products)
        master_count = len(master_products)
        reduction = original_count - master_count
        reduction_percentage = (
            (reduction / original_count * 100) if original_count > 0 else 0
        )

        # Category analysis
        category_distribution = Counter(p["category"] for p in master_products)

        # Unit analysis
        original_units = Counter(
            p.get("original_unit", p["unit"]) for p in original_products
        )
        standardized_units = Counter(p["unit"] for p in master_products)

        # Quality metrics
        avg_quality_score = (
            sum(p["quality_score"] for p in master_products) / len(master_products)
            if master_products
            else 0
        )
        avg_confidence = (
            sum(p["consolidation_confidence"] for p in master_products)
            / len(master_products)
            if master_products
            else 0
        )

        # Establishment coverage
        establishments_covered = len(
            set(p["establishment_cnpj"] for p in original_products)
        )
        avg_establishments_per_product = (
            sum(p["unique_establishments"] for p in master_products)
            / len(master_products)
            if master_products
            else 0
        )

        return {
            "original_count": original_count,
            "master_count": master_count,
            "reduction_count": reduction,
            "reduction_percentage": round(reduction_percentage, 2),
            "category_distribution": dict(category_distribution),
            "original_unit_distribution": dict(original_units),
            "standardized_unit_distribution": dict(standardized_units),
            "quality_metrics": {
                "average_quality_score": round(avg_quality_score, 3),
                "average_consolidation_confidence": round(avg_confidence, 3),
                "high_quality_products": len(
                    [p for p in master_products if p["quality_score"] > 0.8]
                ),
                "low_confidence_consolidations": len(
                    [p for p in master_products if p["consolidation_confidence"] < 0.7]
                ),
            },
            "establishment_metrics": {
                "total_establishments": establishments_covered,
                "avg_establishments_per_product": round(
                    avg_establishments_per_product, 2
                ),
                "cross_establishment_products": len(
                    [p for p in master_products if p["unique_establishments"] > 1]
                ),
            },
        }

    def _calculate_quality_metrics(
        self, master_products: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Calculate quality metrics for the clean product table"""

        total_products = len(master_products)

        if total_products == 0:
            return {"error": "No products to analyze"}

        # Quality score distribution
        quality_scores = [p["quality_score"] for p in master_products]
        high_quality = len([s for s in quality_scores if s > 0.8])
        medium_quality = len([s for s in quality_scores if 0.6 <= s <= 0.8])
        low_quality = len([s for s in quality_scores if s < 0.6])

        # Confidence distribution
        confidence_scores = [p["consolidation_confidence"] for p in master_products]
        high_confidence = len([s for s in confidence_scores if s > 0.8])
        medium_confidence = len([s for s in confidence_scores if 0.6 <= s <= 0.8])
        low_confidence = len([s for s in confidence_scores if s < 0.6])

        return {
            "quality_distribution": {
                "high_quality": high_quality,
                "medium_quality": medium_quality,
                "low_quality": low_quality,
                "high_quality_percentage": round(
                    high_quality / total_products * 100, 2
                ),
            },
            "confidence_distribution": {
                "high_confidence": high_confidence,
                "medium_confidence": medium_confidence,
                "low_confidence": low_confidence,
                "high_confidence_percentage": round(
                    high_confidence / total_products * 100, 2
                ),
            },
            "overall_quality_score": round(
                sum(quality_scores) / len(quality_scores), 3
            ),
            "overall_confidence_score": round(
                sum(confidence_scores) / len(confidence_scores), 3
            ),
        }

    def _generate_cleanup_recommendations(
        self, statistics: Dict[str, Any]
    ) -> List[str]:
        """Generate recommendations based on analysis results"""

        recommendations = []

        reduction_pct = statistics["reduction_percentage"]
        quality_metrics = statistics["quality_metrics"]

        # Reduction recommendations
        if reduction_pct > 50:
            recommendations.append(
                f"Excellent deduplication achieved: {reduction_pct:.1f}% reduction in product count"
            )
        elif reduction_pct > 20:
            recommendations.append(
                f"Good deduplication achieved: {reduction_pct:.1f}% reduction in product count"
            )
        else:
            recommendations.append(
                f"Limited deduplication: {reduction_pct:.1f}% reduction. Consider reviewing similarity thresholds"
            )

        # Quality recommendations
        if quality_metrics["average_quality_score"] < 0.7:
            recommendations.append(
                "Consider manual review of low-quality product groups"
            )

        if quality_metrics["low_confidence_consolidations"] > 0:
            recommendations.append(
                f"Review {quality_metrics['low_confidence_consolidations']} low-confidence consolidations manually"
            )

        # Category recommendations
        uncategorized_count = statistics["category_distribution"].get(
            "uncategorized", 0
        )
        total_count = statistics["master_count"]
        uncategorized_pct = (
            (uncategorized_count / total_count * 100) if total_count > 0 else 0
        )

        if uncategorized_pct > 50:
            recommendations.append(
                f"Consider expanding category classification - {uncategorized_pct:.1f}% products uncategorized"
            )

        # Unit standardization recommendations
        unit_diversity = len(statistics["standardized_unit_distribution"])
        if unit_diversity > 10:
            recommendations.append(
                "Consider further unit standardization - high unit diversity detected"
            )

        return recommendations

    def export_to_csv(self, results: Dict[str, Any], output_path: str):
        """Export clean master products to CSV"""

        import pandas as pd

        self.logger.info(f"Exporting clean products to CSV: {output_path}")

        # Prepare data for CSV export
        csv_data = []
        for product in results["clean_master_products"]:
            csv_data.append(
                {
                    "master_id": product["master_id"],
                    "master_description": product["master_description"],
                    "category": product["category"],
                    "unit": product["unit"],
                    "total_occurrences": product["total_occurrences"],
                    "unique_establishments": product["unique_establishments"],
                    "original_products_count": product["original_products_count"],
                    "quality_score": product["quality_score"],
                    "consolidation_confidence": product["consolidation_confidence"],
                    "original_descriptions": "; ".join(
                        product["original_descriptions"]
                    ),
                }
            )

        df = pd.DataFrame(csv_data)
        df.to_csv(output_path, index=False, encoding="utf-8")

        self.logger.info(f"CSV export completed: {output_path}")

    def export_to_excel(self, results: Dict[str, Any], output_path: str):
        """Export comprehensive results to Excel with multiple sheets"""

        import pandas as pd

        self.logger.info(f"Exporting to Excel: {output_path}")

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            # Master Products sheet
            master_data = []
            for product in results["clean_master_products"]:
                master_data.append(
                    {
                        "master_id": product["master_id"],
                        "master_description": product["master_description"],
                        "category": product["category"],
                        "unit": product["unit"],
                        "total_occurrences": product["total_occurrences"],
                        "unique_establishments": product["unique_establishments"],
                        "quality_score": product["quality_score"],
                        "consolidation_confidence": product["consolidation_confidence"],
                    }
                )

            df_master = pd.DataFrame(master_data)
            df_master.to_excel(writer, sheet_name="Master Products", index=False)

            # Product Mapping sheet
            df_mapping = pd.DataFrame(results["product_mapping"])
            df_mapping.to_excel(writer, sheet_name="Product Mapping", index=False)

            # Statistics sheet
            stats_data = [results["statistics"]]
            df_stats = pd.DataFrame(stats_data)
            df_stats.to_excel(writer, sheet_name="Statistics", index=False)

            # Quality Metrics sheet
            quality_data = [results["quality_metrics"]]
            df_quality = pd.DataFrame(quality_data)
            df_quality.to_excel(writer, sheet_name="Quality Metrics", index=False)

        self.logger.info(f"Excel export completed: {output_path}")
