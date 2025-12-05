#!/usr/bin/env python3
"""Product Analysis Service - Analyze existing products for similarity patterns and duplicates"""

import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from domains.personal_finance.nfce.database.nfce_database_manager import (
    NFCeDatabaseManager,
)
from domains.personal_finance.nfce.similarity.product_matcher import ProductMatcher
from utils.data.json_manager import JSONManager
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager


class ProductAnalysisService:
    """Service for analyzing product data and identifying similarity patterns"""

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("ProductAnalysisService")
        self._db_manager = None
        self.product_matcher = ProductMatcher(cache_enabled=True)

    @property
    def db_manager(self):
        """Lazy-loaded database manager"""
        if self._db_manager is None:
            self._db_manager = NFCeDatabaseManager()
        return self._db_manager

    def analyze_products(
        self,
        cnpj_filter: str | None = None,
        category_filter: list[str] | None = None,
        similarity_threshold: float = 0.8,
        min_frequency: int = 2,
        sample_size: int | None = None,
        detailed: bool = False,
        include_examples: bool = False,
    ) -> dict[str, Any]:
        """Perform comprehensive analysis of products in the database

        Args:
            cnpj_filter: Filter by specific establishment CNPJ
            category_filter: Filter by product categories
            similarity_threshold: Threshold for similarity detection
            min_frequency: Minimum frequency for inclusion in analysis
            sample_size: Limit to random sample size
            detailed: Include detailed statistics
            include_examples: Include concrete examples

        Returns:
            Dictionary with analysis results
        """
        self.logger.info("Starting product analysis")

        # Load products from database
        products = self._load_products_from_database(cnpj_filter, sample_size)
        self.logger.info(f"Loaded {len(products)} products for analysis")

        if not products:
            return {"error": "No products found in database"}

        # Apply category filter if specified
        if category_filter:
            products = self._filter_by_category(products, category_filter)
            self.logger.info(f"After category filtering: {len(products)} products")

        # Perform analysis
        analysis_results = {
            "metadata": {
                "analysis_date": datetime.now().isoformat(),
                "total_products_analyzed": len(products),
                "parameters": {
                    "cnpj_filter": cnpj_filter,
                    "category_filter": category_filter,
                    "similarity_threshold": similarity_threshold,
                    "min_frequency": min_frequency,
                    "sample_size": sample_size,
                    "detailed": detailed,
                    "include_examples": include_examples,
                },
            }
        }

        # Basic statistics
        analysis_results["basic_stats"] = self._analyze_basic_statistics(products)

        # Establishment analysis
        analysis_results["establishment_analysis"] = self._analyze_by_establishment(products)

        # Product name patterns
        analysis_results["naming_patterns"] = self._analyze_naming_patterns(products, min_frequency)

        # Potential duplicates
        analysis_results["potential_duplicates"] = self._find_potential_duplicates(
            products, similarity_threshold, include_examples
        )

        # Product categories (auto-detected)
        analysis_results["category_analysis"] = self._analyze_categories(products)

        # Price analysis
        analysis_results["price_analysis"] = self._analyze_prices(products)

        if detailed:
            # Detailed statistics
            analysis_results["detailed_stats"] = self._analyze_detailed_statistics(products)

            # Similarity distribution
            analysis_results["similarity_distribution"] = self._analyze_similarity_distribution(
                products, similarity_threshold
            )

        self.logger.info("Product analysis completed")
        return analysis_results

    def analyze_products_with_similarity_engine(
        self,
        cnpj_filter: str | None = None,
        similarity_threshold: float = 0.8,
        sample_size: int | None = None,
        detailed: bool = False,
    ) -> dict[str, Any]:
        """Perform advanced product analysis using the Phase 2 Similarity Engine

        Args:
            cnpj_filter: Filter by specific establishment CNPJ
            similarity_threshold: Threshold for similarity detection (default: 0.8)
            sample_size: Limit to random sample size
            detailed: Include detailed similarity analysis

        Returns:
            Dictionary with advanced similarity analysis results
        """
        self.logger.info("Starting advanced product analysis with Similarity Engine")

        # Load products from database
        products = self._load_products_from_database(cnpj_filter, sample_size)
        self.logger.info(f"Loaded {len(products)} products for similarity analysis")

        if not products:
            return {"error": "No products found in database"}

        # Convert to format expected by ProductMatcher
        product_descriptions = [{"description": p["description"]} for p in products]

        # Use ProductMatcher for advanced analysis
        self.logger.info("Running similarity engine analysis...")
        matching_results = self.product_matcher.analyze_products(product_descriptions)

        # Get deduplication recommendations
        recommendations = self.product_matcher.get_deduplication_recommendations(product_descriptions)

        # Build comprehensive results
        analysis_results = {
            "metadata": {
                "analysis_date": datetime.now().isoformat(),
                "engine_version": "Phase 2 - Motor de Similaridade",
                "total_products_analyzed": len(products),
                "parameters": {
                    "cnpj_filter": cnpj_filter,
                    "similarity_threshold": similarity_threshold,
                    "sample_size": sample_size,
                    "detailed": detailed,
                },
            },
            "similarity_analysis": {
                "total_groups": matching_results.total_groups,
                "duplicate_groups": len(matching_results.duplicate_groups),
                "similar_groups": len(matching_results.similar_groups),
                "singleton_products": len(matching_results.singleton_products),
                "deduplication_ratio": matching_results.deduplication_ratio,
                "avg_group_size": matching_results.avg_group_size,
                "largest_group_size": matching_results.largest_group_size,
            },
            "deduplication_recommendations": recommendations,
            "duplicate_groups_details": [group.to_dict() for group in matching_results.duplicate_groups],
            "similar_groups_details": [group.to_dict() for group in matching_results.similar_groups],
        }

        # Add detailed analysis if requested
        if detailed:
            analysis_results["detailed_analysis"] = self._add_detailed_similarity_analysis(products, matching_results)

        # Add establishment context
        analysis_results["establishment_context"] = self._add_establishment_context_to_duplicates(
            products, matching_results
        )

        self.logger.info(
            f"Similarity analysis completed. Found {len(matching_results.duplicate_groups)} duplicate groups"
        )
        return analysis_results

    def _load_products_from_database(
        self, cnpj_filter: str | None = None, sample_size: int | None = None
    ) -> list[dict[str, Any]]:
        """Load products from database with optional filtering"""
        try:
            conn = self.db_manager.db_manager.get_connection("nfce_db")

            # Build query
            query = """
                SELECT 
                    p.id,
                    p.product_code,
                    p.description,
                    p.unit,
                    p.occurrence_count,
                    p.created_at,
                    e.cnpj as establishment_cnpj,
                    e.business_name as establishment_name,
                    e.city as establishment_city,
                    e.state as establishment_state
                FROM products p
                JOIN establishments e ON p.establishment_id = e.id
                WHERE p.description IS NOT NULL AND p.description != ''
            """

            params = []
            if cnpj_filter:
                query += " AND e.cnpj = ?"
                params.append(cnpj_filter)

            if sample_size:
                query += " ORDER BY RANDOM() LIMIT ?"
                params.append(sample_size)
            else:
                query += " ORDER BY p.description, e.cnpj"

            results = conn.execute(query, params).fetchall()

            # Convert to list of dictionaries
            products = []
            for row in results:
                products.append(
                    {
                        "id": row[0],
                        "product_code": row[1],
                        "description": row[2],
                        "unit": row[3],
                        "occurrence_count": row[4],
                        "created_at": row[5].isoformat() if row[5] else None,
                        "establishment_cnpj": row[6],
                        "establishment_name": row[7],
                        "establishment_city": row[8],
                        "establishment_state": row[9],
                    }
                )

            return products

        except Exception as e:
            self.logger.error(f"Error loading products from database: {e}")
            raise

    def _filter_by_category(self, products: list[dict], categories: list[str]) -> list[dict]:
        """Filter products by detected categories"""
        category_keywords = {
            "beverages": [
                "coca",
                "refri",
                "suco",
                "agua",
                "cerveja",
                "vinho",
                "cafe",
                "cha",
                "guarana",
                "sprite",
                "fanta",
                "pepsi",
                "energetico",
                "isotonic",
                "achocolatado",
                "leite",
                "vitamina",
            ],
            "fruits": [
                "banana",
                "maca",
                "laranja",
                "uva",
                "abacaxi",
                "manga",
                "fruta",
                "limao",
                "melancia",
                "melao",
                "mamao",
                "goiaba",
                "pera",
                "pessego",
                "morango",
                "kiwi",
                "mexerica",
                "tangerina",
                "abacate",
                "caju",
                "acerola",
            ],
            "dairy": [
                "leite",
                "queijo",
                "iogurte",
                "manteiga",
                "nata",
                "creme",
                "requeijao",
                "ricota",
                "mussarela",
                "parmesao",
                "gouda",
                "coalho",
            ],
            "meat": [
                "carne",
                "frango",
                "peixe",
                "porco",
                "boi",
                "linguica",
                "salsicha",
                "hamburguer",
                "bacon",
                "presunto",
                "mortadela",
                "salame",
                "peito",
                "coxa",
                "asa",
                "file",
                "costela",
                "alcatra",
                "picanha",
            ],
            "bread": [
                "pao",
                "biscoito",
                "bolo",
                "torrada",
                "padaria",
                "croissant",
                "brioche",
                "rosca",
                "sonho",
                "bolacha",
                "wafer",
                "cracker",
            ],
            "cleaning": [
                "detergente",
                "sabao",
                "amaciante",
                "desinfetante",
                "limpa",
                "alvejante",
                "multiuso",
                "vidro",
                "chao",
                "vaso",
            ],
            "snacks": [
                "salgadinho",
                "chips",
                "pipoca",
                "amendoim",
                "castanha",
                "chocolate",
                "bala",
                "chiclete",
                "pirulito",
                "doce",
                "guloseima",
            ],
            "grains": [
                "arroz",
                "feijao",
                "macarrao",
                "massa",
                "farinha",
                "aveia",
                "quinoa",
                "granola",
                "cereal",
                "milho",
                "trigo",
            ],
            "condiments": [
                "sal",
                "acucar",
                "oleo",
                "vinagre",
                "tempero",
                "molho",
                "ketchup",
                "mostarda",
                "maionese",
                "pimenta",
                "oregano",
                "cominho",
            ],
            "frozen": [
                "congelado",
                "sorvete",
                "picole",
                "lasanha",
                "nuggets",
                "hamburguer",
                "pizza",
                "gelado",
            ],
            "personal_care": [
                "shampoo",
                "condicionador",
                "sabonete",
                "pasta",
                "dente",
                "desodorante",
                "perfume",
                "creme",
                "hidratante",
            ],
            "vegetables": [
                "tomate",
                "cebola",
                "alho",
                "batata",
                "cenoura",
                "abobrinha",
                "pimentao",
                "alface",
                "couve",
                "brocolis",
                "pepino",
                "verdura",
                "legume",
            ],
        }

        filtered_products = []
        for product in products:
            description = product["description"].lower()
            for category in categories:
                if category.lower() in category_keywords:
                    keywords = category_keywords[category.lower()]
                    if any(keyword in description for keyword in keywords):
                        filtered_products.append(product)
                        break

        return filtered_products

    def _analyze_basic_statistics(self, products: list[dict]) -> dict[str, Any]:
        """Analyze basic product statistics"""
        total_products = len(products)
        unique_descriptions = len(set(p["description"] for p in products))
        unique_establishments = len(set(p["establishment_cnpj"] for p in products))

        # Unit analysis
        units = [p["unit"] for p in products if p["unit"]]
        unit_counts = Counter(units)

        # Occurrence analysis
        occurrences = [p["occurrence_count"] or 1 for p in products]
        avg_occurrence = sum(occurrences) / len(occurrences) if occurrences else 0

        return {
            "total_products": total_products,
            "unique_descriptions": unique_descriptions,
            "unique_establishments": unique_establishments,
            "duplication_ratio": round((total_products - unique_descriptions) / total_products * 100, 2)
            if total_products > 0
            else 0,
            "average_occurrence_per_product": round(avg_occurrence, 2),
            "most_common_units": dict(unit_counts.most_common(10)),
            "establishments_per_product": round(total_products / unique_descriptions, 2)
            if unique_descriptions > 0
            else 0,
        }

    def _analyze_by_establishment(self, products: list[dict]) -> dict[str, Any]:
        """Analyze products by establishment"""
        establishment_stats = {}

        for product in products:
            cnpj = product["establishment_cnpj"]
            if cnpj not in establishment_stats:
                establishment_stats[cnpj] = {
                    "total_products": 0,
                    "unique_descriptions": set(),
                    "business_name": product["establishment_name"],
                    "city": product["establishment_city"],
                    "state": product["establishment_state"],
                }

            stats = establishment_stats[cnpj]
            stats["total_products"] += 1
            stats["unique_descriptions"].add(product["description"])

        # Convert to final format
        result = {}
        for cnpj, stats in establishment_stats.items():
            unique_count = len(stats["unique_descriptions"])
            total_count = stats["total_products"]

            result[cnpj] = {
                "business_name": stats["business_name"],
                "city": stats["city"],
                "state": stats["state"],
                "total_products": total_count,
                "unique_products": unique_count,
                "duplication_ratio": round((total_count - unique_count) / total_count * 100, 2)
                if total_count > 0
                else 0,
            }

        # Summary statistics
        establishment_counts = [stats["total_products"] for stats in result.values()]
        unique_counts = [stats["unique_products"] for stats in result.values()]

        summary = {
            "total_establishments": len(result),
            "avg_products_per_establishment": round(sum(establishment_counts) / len(establishment_counts), 2)
            if establishment_counts
            else 0,
            "avg_unique_products_per_establishment": round(sum(unique_counts) / len(unique_counts), 2)
            if unique_counts
            else 0,
            "establishments": result,
        }

        return summary

    def _analyze_naming_patterns(self, products: list[dict], min_frequency: int) -> dict[str, Any]:
        """Analyze product naming patterns"""
        descriptions = [p["description"] for p in products]

        # Word frequency analysis
        all_words = []
        for desc in descriptions:
            # Split and clean words
            words = re.findall(r"\b\w+\b", desc.upper())
            all_words.extend(words)

        word_freq = Counter(all_words)
        common_words = {word: count for word, count in word_freq.items() if count >= min_frequency}

        # Common prefixes/suffixes
        prefixes = defaultdict(int)
        suffixes = defaultdict(int)

        for desc in descriptions:
            words = desc.split()
            if words:
                if len(words) > 1:
                    prefixes[words[0]] += 1
                    suffixes[words[-1]] += 1

        # Brand detection (heuristic)
        potential_brands = []
        for word, count in word_freq.most_common(50):
            if len(word) > 2 and count >= min_frequency * 2:
                # Check if word appears as first word frequently
                first_word_count = sum(1 for desc in descriptions if desc.upper().startswith(word))
                if first_word_count >= min_frequency:
                    potential_brands.append(
                        {
                            "brand": word,
                            "frequency": count,
                            "as_first_word": first_word_count,
                        }
                    )

        # Length analysis
        lengths = [len(desc) for desc in descriptions]

        return {
            "most_common_words": dict(word_freq.most_common(20)),
            "common_prefixes": dict(Counter(prefixes).most_common(10)),
            "common_suffixes": dict(Counter(suffixes).most_common(10)),
            "potential_brands": potential_brands[:15],
            "description_length_stats": {
                "min": min(lengths) if lengths else 0,
                "max": max(lengths) if lengths else 0,
                "avg": round(sum(lengths) / len(lengths), 2) if lengths else 0,
            },
            "total_unique_words": len(word_freq),
            "words_above_threshold": len(common_words),
        }

    def _find_potential_duplicates(
        self, products: list[dict], threshold: float, include_examples: bool
    ) -> dict[str, Any]:
        """Find potential duplicate products using simple similarity"""
        # Group by normalized description for exact matches
        normalized_groups = defaultdict(list)

        for product in products:
            normalized = self._normalize_description(product["description"])
            normalized_groups[normalized].append(product)

        # Find groups with multiple products (potential duplicates)
        duplicate_groups = []
        exact_matches = 0

        for normalized_desc, group in normalized_groups.items():
            if len(group) > 1:
                exact_matches += len(group)

                # Get establishments involved
                establishments = list(set(p["establishment_cnpj"] for p in group))

                group_info = {
                    "normalized_description": normalized_desc,
                    "original_descriptions": list(set(p["description"] for p in group)),
                    "product_count": len(group),
                    "establishment_count": len(establishments),
                    "establishments": establishments[:5],  # Limit for readability
                }

                if include_examples:
                    group_info["examples"] = group[:3]  # First 3 examples

                duplicate_groups.append(group_info)

        # Sort by product count (most duplicated first)
        duplicate_groups.sort(key=lambda x: x["product_count"], reverse=True)

        # Fuzzy matching analysis (simplified for Phase 1)
        fuzzy_analysis = self._analyze_fuzzy_similarities(products, threshold)

        return {
            "exact_matches": {
                "total_products_in_groups": exact_matches,
                "total_groups": len(duplicate_groups),
                "groups": duplicate_groups[:20],  # Top 20 groups
            },
            "fuzzy_analysis": fuzzy_analysis,
            "summary": {
                "potential_exact_duplicates": exact_matches,
                "largest_duplicate_group": max([g["product_count"] for g in duplicate_groups])
                if duplicate_groups
                else 0,
                "avg_establishments_per_group": round(
                    sum(g["establishment_count"] for g in duplicate_groups) / len(duplicate_groups),
                    2,
                )
                if duplicate_groups
                else 0,
            },
        }

    def _normalize_description(self, description: str) -> str:
        """Normalize product description for comparison"""
        if not description:
            return ""

        # Convert to uppercase
        normalized = description.upper()

        # Remove common variations
        normalized = re.sub(r"\b(UN|UF|KG|ML|LT|L|G|MG)\b", "", normalized)
        normalized = re.sub(r"\d+\s*(ML|L|KG|G|MG)\b", "", normalized)
        normalized = re.sub(r"\s+", " ", normalized)

        return normalized.strip()

    def _analyze_fuzzy_similarities(self, products: list[dict], threshold: float) -> dict[str, Any]:
        """Basic fuzzy similarity analysis using string comparison"""
        # For Phase 1, use simple similarity measures
        similar_pairs = []
        descriptions = list(set(p["description"] for p in products))

        # Sample analysis to avoid performance issues
        sample_size = min(100, len(descriptions))
        if len(descriptions) > sample_size:
            import random

            descriptions = random.sample(descriptions, sample_size)

        for i, desc1 in enumerate(descriptions):
            for j, desc2 in enumerate(descriptions[i + 1 :], i + 1):
                similarity = self._simple_similarity(desc1, desc2)
                if similarity >= threshold:
                    similar_pairs.append(
                        {
                            "description1": desc1,
                            "description2": desc2,
                            "similarity": round(similarity, 3),
                        }
                    )

        return {
            "analyzed_pairs": len(descriptions) * (len(descriptions) - 1) // 2,
            "similar_pairs_found": len(similar_pairs),
            "threshold_used": threshold,
            "top_similar_pairs": sorted(similar_pairs, key=lambda x: x["similarity"], reverse=True)[:10],
        }

    def _simple_similarity(self, str1: str, str2: str) -> float:
        """Simple similarity calculation based on common words"""
        if not str1 or not str2:
            return 0.0

        words1 = set(str1.upper().split())
        words2 = set(str2.upper().split())

        if not words1 or not words2:
            return 0.0

        intersection = words1.intersection(words2)
        union = words1.union(words2)

        return len(intersection) / len(union) if union else 0.0

    def _analyze_categories(self, products: list[dict]) -> dict[str, Any]:
        """Auto-detect product categories based on keywords"""
        categories = {
            "beverages": [
                "coca",
                "refri",
                "suco",
                "agua",
                "cerveja",
                "cafe",
                "cha",
                "guarana",
                "sprite",
                "fanta",
                "pepsi",
                "energetico",
                "isotonic",
                "achocolatado",
                "leite",
                "vitamina",
            ],
            "fruits": [
                "banana",
                "maca",
                "laranja",
                "uva",
                "fruta",
                "abacaxi",
                "limao",
                "melancia",
                "melao",
                "mamao",
                "goiaba",
                "pera",
                "pessego",
                "morango",
                "kiwi",
                "mexerica",
                "tangerina",
                "abacate",
                "caju",
                "acerola",
            ],
            "dairy": [
                "leite",
                "queijo",
                "iogurte",
                "manteiga",
                "nata",
                "requeijao",
                "ricota",
                "mussarela",
                "parmesao",
                "gouda",
                "coalho",
            ],
            "meat": [
                "carne",
                "frango",
                "peixe",
                "porco",
                "boi",
                "linguica",
                "salsicha",
                "hamburguer",
                "bacon",
                "presunto",
                "mortadela",
                "salame",
                "peito",
                "coxa",
                "asa",
                "file",
                "costela",
                "alcatra",
                "picanha",
            ],
            "bread": [
                "pao",
                "biscoito",
                "bolo",
                "torrada",
                "padaria",
                "croissant",
                "brioche",
                "rosca",
                "sonho",
                "bolacha",
                "wafer",
                "cracker",
            ],
            "cleaning": [
                "detergente",
                "sabao",
                "amaciante",
                "limpa",
                "alvejante",
                "multiuso",
                "vidro",
                "chao",
                "vaso",
            ],
            "snacks": [
                "salgadinho",
                "chips",
                "pipoca",
                "amendoim",
                "castanha",
                "chocolate",
                "bala",
                "chiclete",
                "pirulito",
                "doce",
                "guloseima",
            ],
            "grains": [
                "arroz",
                "feijao",
                "macarrao",
                "massa",
                "farinha",
                "aveia",
                "quinoa",
                "granola",
                "cereal",
                "milho",
                "trigo",
            ],
            "condiments": [
                "sal",
                "acucar",
                "oleo",
                "vinagre",
                "tempero",
                "molho",
                "ketchup",
                "mostarda",
                "maionese",
                "pimenta",
                "oregano",
                "cominho",
            ],
            "frozen": [
                "congelado",
                "sorvete",
                "picole",
                "lasanha",
                "nuggets",
                "hamburguer",
                "pizza",
                "gelado",
            ],
            "personal_care": [
                "shampoo",
                "condicionador",
                "sabonete",
                "pasta",
                "dente",
                "desodorante",
                "perfume",
                "creme",
                "hidratante",
            ],
            "vegetables": [
                "tomate",
                "cebola",
                "alho",
                "batata",
                "cenoura",
                "abobrinha",
                "pimentao",
                "alface",
                "couve",
                "brocolis",
                "pepino",
                "verdura",
                "legume",
            ],
        }

        category_counts = defaultdict(int)
        categorized_products = defaultdict(list)

        for product in products:
            description = product["description"].lower()
            product_categorized = False

            for category, keywords in categories.items():
                if any(keyword in description for keyword in keywords):
                    category_counts[category] += 1
                    categorized_products[category].append(product["description"])
                    product_categorized = True
                    break

            if not product_categorized:
                category_counts["uncategorized"] += 1

        return {
            "category_distribution": dict(category_counts),
            "categorization_rate": round(
                (sum(category_counts.values()) - category_counts.get("uncategorized", 0))
                / sum(category_counts.values())
                * 100,
                2,
            )
            if category_counts
            else 0,
            "sample_products_by_category": {cat: products[:5] for cat, products in categorized_products.items()},
        }

    def _analyze_prices(self, products: list[dict]) -> dict[str, Any]:
        """Analyze price patterns (placeholder for future price data)"""
        # Note: Current schema doesn't store price in products table
        # This is a placeholder for when price tracking is implemented

        return {
            "note": "Price analysis will be available when price history is implemented",
            "current_status": "Prices are stored in invoice_items table, not products table",
            "recommendation": "Consider adding price tracking to products for better analysis",
        }

    def _analyze_detailed_statistics(self, products: list[dict]) -> dict[str, Any]:
        """Generate detailed statistics for in-depth analysis"""
        # Description length distribution
        lengths = [len(p["description"]) for p in products]
        length_distribution = Counter(lengths)

        # Product code patterns
        codes = [p["product_code"] for p in products if p["product_code"]]
        code_lengths = [len(str(code)) for code in codes]

        # Temporal analysis
        creation_dates = [p["created_at"] for p in products if p["created_at"]]

        return {
            "description_length_distribution": dict(length_distribution),
            "product_code_statistics": {
                "total_with_codes": len(codes),
                "total_without_codes": len(products) - len(codes),
                "avg_code_length": round(sum(code_lengths) / len(code_lengths), 2) if code_lengths else 0,
                "unique_codes": len(set(codes)),
            },
            "temporal_distribution": {
                "products_with_dates": len(creation_dates),
                "date_range": {
                    "earliest": min(creation_dates) if creation_dates else None,
                    "latest": max(creation_dates) if creation_dates else None,
                },
            },
        }

    def _analyze_similarity_distribution(self, products: list[dict], threshold: float) -> dict[str, Any]:
        """Analyze distribution of similarity scores"""
        # This is a simplified version for Phase 1
        return {
            "note": "Detailed similarity distribution analysis will be implemented in Phase 2",
            "threshold_used": threshold,
            "planned_features": [
                "Similarity score histograms",
                "Optimal threshold recommendations",
                "Cross-establishment similarity patterns",
            ],
        }

    def save_analysis_results(self, results: dict[str, Any], output_path: str) -> None:
        """Save analysis results to JSON file"""
        try:
            # Ensure output directory exists
            FileManager.create_folder("output")

            # Convert any datetime objects to strings for JSON serialization
            serializable_results = self._make_json_serializable(results)

            JSONManager.write_json(serializable_results, output_path)
            self.logger.info(f"Analysis results saved to {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving analysis results: {e}")
            raise

    def _make_json_serializable(self, obj: Any) -> Any:
        """Convert objects to JSON serializable format"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {key: self._make_json_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_serializable(item) for item in obj]
        elif hasattr(obj, "__dict__"):
            return self._make_json_serializable(obj.__dict__)
        else:
            return obj

    def print_analysis_summary(self, results: dict[str, Any]) -> None:
        """Print analysis summary to console"""
        print("\n" + "=" * 70)
        print("NFCe Product Analysis Summary")
        print("=" * 70)

        # Basic stats
        basic = results.get("basic_stats", {})
        print(f"Total Products Analyzed: {basic.get('total_products', 0):,}")
        print(f"Unique Descriptions: {basic.get('unique_descriptions', 0):,}")
        print(f"Unique Establishments: {basic.get('unique_establishments', 0):,}")
        print(f"Duplication Ratio: {basic.get('duplication_ratio', 0)}%")

        # Establishment stats
        establishment = results.get("establishment_analysis", {})
        print(f"\nEstablishments: {establishment.get('total_establishments', 0)}")
        print(f"Avg Products per Establishment: {establishment.get('avg_products_per_establishment', 0)}")

        # Potential duplicates
        duplicates = results.get("potential_duplicates", {})
        exact = duplicates.get("exact_matches", {})
        print(f"\nPotential Exact Duplicates: {exact.get('total_products_in_groups', 0)}")
        print(f"Duplicate Groups Found: {exact.get('total_groups', 0)}")

        # Top duplicate groups
        groups = exact.get("groups", [])
        if groups:
            print("\nTop Duplicate Groups:")
            for i, group in enumerate(groups[:5], 1):
                print(
                    f"  {i}. '{group['normalized_description']}' - {group['product_count']} products across {group['establishment_count']} establishments"
                )

        # Categories
        categories = results.get("category_analysis", {})
        cat_dist = categories.get("category_distribution", {})
        if cat_dist:
            print("\nProduct Categories:")
            for category, count in sorted(cat_dist.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"  {category.title()}: {count}")

        print("=" * 70)
        print(f"Analysis completed at: {results.get('metadata', {}).get('analysis_date', 'Unknown')}")
        print("=" * 70)

    def _add_detailed_similarity_analysis(self, products: list[dict], matching_results) -> dict[str, Any]:
        """Add detailed similarity analysis to results"""
        # Calculate similarity statistics
        all_similarities = []
        for group in matching_results.duplicate_groups + matching_results.similar_groups:
            all_similarities.extend(group.similarity_scores)

        if not all_similarities:
            return {"message": "No similarity scores available"}

        # Product establishment distribution in duplicate groups
        establishment_distribution = defaultdict(int)
        cross_establishment_duplicates = 0

        for group in matching_results.duplicate_groups:
            establishments = set()
            for product_desc in [p["description"] for p in group.products]:
                # Find establishment for this product
                for product in products:
                    if product["description"] == product_desc:
                        establishments.add(product.get("establishment_cnpj", "unknown"))
                        establishment_distribution[product.get("establishment_cnpj", "unknown")] += 1
                        break

            if len(establishments) > 1:
                cross_establishment_duplicates += 1

        return {
            "similarity_statistics": {
                "total_similarities_calculated": len(all_similarities),
                "average_similarity": sum(all_similarities) / len(all_similarities),
                "min_similarity": min(all_similarities),
                "max_similarity": max(all_similarities),
                "high_confidence_matches": sum(1 for s in all_similarities if s > 0.9),
            },
            "cross_establishment_analysis": {
                "cross_establishment_duplicates": cross_establishment_duplicates,
                "establishment_distribution": dict(establishment_distribution),
                "total_establishments_with_duplicates": len(establishment_distribution),
            },
        }

    def _add_establishment_context_to_duplicates(self, products: list[dict], matching_results) -> dict[str, Any]:
        """Add establishment context to duplicate analysis"""
        # Use regular dict with type casting
        establishment_summary = {}

        # Count products per establishment
        for product in products:
            cnpj = product.get("establishment_cnpj", "unknown")
            if cnpj not in establishment_summary:
                establishment_summary[cnpj] = {
                    "total_products": 0,
                    "duplicate_products": 0,
                    "business_name": "",
                    "city": "",
                    "state": "",
                    "duplicate_ratio": 0.0,
                }

            establishment_summary[cnpj]["total_products"] += 1
            establishment_summary[cnpj]["business_name"] = product.get("establishment_name", "")
            establishment_summary[cnpj]["city"] = product.get("establishment_city", "")
            establishment_summary[cnpj]["state"] = product.get("establishment_state", "")

        # Count duplicates per establishment
        for group in matching_results.duplicate_groups:
            for product_desc in [p["description"] for p in group.products]:
                for product in products:
                    if product["description"] == product_desc:
                        cnpj = product.get("establishment_cnpj", "unknown")
                        if cnpj in establishment_summary:
                            establishment_summary[cnpj]["duplicate_products"] += 1
                        break

        # Calculate duplicate ratios
        for cnpj, data in establishment_summary.items():
            total = data["total_products"]
            duplicates = data["duplicate_products"]
            if total > 0:
                data["duplicate_ratio"] = duplicates / total
            else:
                data["duplicate_ratio"] = 0.0

        # Sort by duplicate ratio
        sorted_establishments = sorted(
            establishment_summary.items(),
            key=lambda x: x[1]["duplicate_ratio"],
            reverse=True,
        )

        # Count establishments with duplicates
        establishments_with_duplicates = sum(
            1 for _, data in establishment_summary.items() if data["duplicate_products"] > 0
        )

        return {
            "total_establishments": len(establishment_summary),
            "establishments_with_duplicates": establishments_with_duplicates,
            "establishment_ranking": [
                {
                    "cnpj": cnpj,
                    "business_name": data["business_name"],
                    "location": f"{data['city']}, {data['state']}",
                    "total_products": data["total_products"],
                    "duplicate_products": data["duplicate_products"],
                    "duplicate_ratio": data["duplicate_ratio"],
                }
                for cnpj, data in sorted_establishments[:10]  # Top 10
            ],
        }

    def print_similarity_analysis_summary(self, results: dict[str, Any]) -> None:
        """Print a formatted summary of similarity analysis results"""
        print("\n" + "=" * 70)
        print("🔍 SIMILARITY ENGINE ANALYSIS SUMMARY")
        print("=" * 70)

        metadata = results.get("metadata", {})
        print(f"Analysis Date: {metadata.get('analysis_date', 'Unknown')}")
        print(f"Engine: {metadata.get('engine_version', 'Unknown')}")
        print(f"Products Analyzed: {metadata.get('total_products_analyzed', 0)}")

        # Similarity analysis results
        similarity = results.get("similarity_analysis", {})
        print("\n📊 SIMILARITY RESULTS:")
        print(f"Total Groups: {similarity.get('total_groups', 0)}")
        print(f"Duplicate Groups: {similarity.get('duplicate_groups', 0)}")
        print(f"Similar Groups: {similarity.get('similar_groups', 0)}")
        print(f"Singleton Products: {similarity.get('singleton_products', 0)}")
        print(f"Deduplication Potential: {similarity.get('deduplication_ratio', 0):.1%}")
        print(f"Average Group Size: {similarity.get('avg_group_size', 0):.1f}")
        print(f"Largest Group: {similarity.get('largest_group_size', 0)} products")

        # Recommendations
        recommendations = results.get("deduplication_recommendations", {})
        summary = recommendations.get("summary", {})
        print("\n💡 DEDUPLICATION RECOMMENDATIONS:")
        print(f"Potential Duplicates: {summary.get('potential_duplicates', 0)}")
        print(f"Estimated Reduction: {summary.get('estimated_reduction', 0):.1%}")
        print(f"Products to Review: {summary.get('products_to_review', 0)}")

        # Top duplicate groups
        duplicate_groups = results.get("duplicate_groups_details", [])
        if duplicate_groups:
            print("\n🔄 TOP DUPLICATE GROUPS:")
            for i, group in enumerate(duplicate_groups[:5], 1):
                print(
                    f"  {i}. '{group['representative_product']}' - {group['size']} products (similarity: {group['avg_similarity']:.3f})"
                )

        # Establishment context
        establishment = results.get("establishment_context", {})
        ranking = establishment.get("establishment_ranking", [])
        if ranking:
            print("\n🏪 ESTABLISHMENTS WITH MOST DUPLICATES:")
            for i, est in enumerate(ranking[:3], 1):
                print(
                    f"  {i}. {est['business_name']} - {est['duplicate_products']}/{est['total_products']} products ({est['duplicate_ratio']:.1%})"
                )

        # Actions
        actions = recommendations.get("actions", [])
        if actions:
            print("\n📋 RECOMMENDED ACTIONS:")
            for action in actions:
                print(f"  • {action}")

        print("=" * 70)

    def analyze_products_with_similarity(
        self,
        cnpj_filter: str | None = None,
        category_filter: list[str] | None = None,
        similarity_threshold: float = 0.8,
        min_frequency: int = 2,
        sample_size: int | None = None,
        detailed: bool = False,
        include_examples: bool = False,
    ) -> dict[str, Any]:
        """Enhanced product analysis using the Phase 2 Similarity Engine

        This method combines traditional analysis with advanced similarity detection
        to provide comprehensive insights into product duplicates and patterns.
        """
        self.logger.info("Starting enhanced product analysis with Similarity Engine")

        # First run traditional analysis for baseline
        traditional_results = self.analyze_products(
            cnpj_filter=cnpj_filter,
            category_filter=category_filter,
            similarity_threshold=similarity_threshold,
            min_frequency=min_frequency,
            sample_size=sample_size,
            detailed=detailed,
            include_examples=include_examples,
        )

        # Get products from database for similarity analysis
        products_data = self._get_products_for_similarity_analysis(cnpj_filter=cnpj_filter, sample_size=sample_size)

        if not products_data:
            self.logger.warning("No products found for similarity analysis")
            return traditional_results

        self.logger.info(f"Running Similarity Engine on {len(products_data)} products")

        # Run similarity analysis
        similarity_results = self.product_matcher.analyze_products(products_data)

        # Generate deduplication recommendations
        dedup_recommendations = self.product_matcher.get_deduplication_recommendations(products_data)

        # Combine results
        enhanced_results = {
            **traditional_results,
            "similarity_engine_results": {
                "analysis_timestamp": datetime.now().isoformat(),
                "products_analyzed": len(products_data),
                "similarity_threshold": similarity_threshold,
                "matching_results": similarity_results.to_dict(),
                "deduplication_recommendations": dedup_recommendations,
                "summary": {
                    "duplicate_groups_found": len(similarity_results.duplicate_groups),
                    "similar_groups_found": len(similarity_results.similar_groups),
                    "singleton_products": len(similarity_results.singleton_products),
                    "potential_reduction": similarity_results.deduplication_ratio,
                    "largest_group_size": similarity_results.largest_group_size,
                },
            },
        }

        self.logger.info(f"Similarity Engine found {len(similarity_results.duplicate_groups)} duplicate groups")

        return enhanced_results

    def analyze_similarity_only(
        self,
        cnpj_filter: str | None = None,
        similarity_threshold: float = 0.8,
        sample_size: int | None = None,
    ) -> dict[str, Any]:
        """Run only similarity analysis using the Phase 2 Similarity Engine

        This method focuses exclusively on duplicate detection using advanced
        similarity algorithms without traditional pattern analysis.
        """
        self.logger.info("Starting similarity-only analysis")

        # Get products from database
        products_data = self._get_products_for_similarity_analysis(cnpj_filter=cnpj_filter, sample_size=sample_size)

        if not products_data:
            raise ValueError("No products found in database for analysis")

        self.logger.info(f"Analyzing {len(products_data)} products with Similarity Engine")

        # Run similarity analysis
        similarity_results = self.product_matcher.analyze_products(products_data)

        # Generate recommendations
        recommendations = self.product_matcher.get_deduplication_recommendations(products_data)

        # Build focused results
        results = {
            "analysis_type": "similarity_only",
            "analysis_timestamp": datetime.now().isoformat(),
            "products_analyzed": len(products_data),
            "similarity_threshold": similarity_threshold,
            "results": similarity_results.to_dict(),
            "recommendations": recommendations,
            "summary": {
                "total_products": len(products_data),
                "duplicate_groups": len(similarity_results.duplicate_groups),
                "similar_groups": len(similarity_results.similar_groups),
                "singleton_products": len(similarity_results.singleton_products),
                "deduplication_ratio": similarity_results.deduplication_ratio,
                "largest_duplicate_group": similarity_results.largest_group_size,
            },
        }

        # Add establishment context if CNPJ filter was used
        if cnpj_filter:
            establishment_info = self._get_establishment_info(cnpj_filter)
            if establishment_info:
                results["establishment_info"] = establishment_info

        self.logger.info(
            f"Similarity analysis complete: {len(similarity_results.duplicate_groups)} duplicate groups found"
        )

        return results

    def _get_products_for_similarity_analysis(
        self, cnpj_filter: str | None = None, sample_size: int | None = None
    ) -> list[dict[str, Any]]:
        """Get products from database formatted for similarity analysis"""
        try:
            conn = self.db_manager.db_manager.get_connection("nfce_db")

            # Base query
            query = """
            SELECT DISTINCT
                p.description,
                p.product_code,
                e.cnpj,
                e.business_name,
                COUNT(*) as frequency
            FROM products p
            JOIN establishments e ON p.establishment_id = e.id
            WHERE p.description IS NOT NULL 
            AND TRIM(p.description) != ''
            """

            params = []

            # Add CNPJ filter if specified
            if cnpj_filter:
                query += " AND e.cnpj = ?"
                params.append(cnpj_filter)

            # Group and order
            query += """
            GROUP BY p.description, p.product_code, e.cnpj, e.business_name
            HAVING COUNT(*) >= 1
            ORDER BY frequency DESC
            """

            # Add limit if sample size specified
            if sample_size:
                query += f" LIMIT {sample_size}"

            results = conn.execute(query, params).fetchall()

            # Convert to format expected by similarity engine
            products_data = []
            for row in results:
                products_data.append(
                    {
                        "description": row[0],
                        "code": row[1] if row[1] else "",
                        "cnpj": row[2],
                        "business_name": row[3],
                        "frequency": row[4],
                    }
                )

            self.logger.info(f"Retrieved {len(products_data)} products for similarity analysis")
            return products_data

        except Exception as e:
            self.logger.error(f"Error retrieving products for similarity analysis: {e}")
            raise

    def _get_establishment_info(self, cnpj: str) -> dict[str, Any] | None:
        """Get establishment information for context"""
        try:
            conn = self.db_manager.db_manager.get_connection("nfce_db")

            query = """
            SELECT 
                business_name,
                fantasy_name,
                cnpj,
                COUNT(DISTINCT p.id) as total_products
            FROM establishments e
            LEFT JOIN products p ON e.id = p.establishment_id
            WHERE e.cnpj = ?
            GROUP BY e.id, e.business_name, e.fantasy_name, e.cnpj
            """

            result = conn.execute(query, [cnpj]).fetchone()

            if result:
                return {
                    "business_name": result[0],
                    "fantasy_name": result[1],
                    "cnpj": result[2],
                    "total_products": result[3],
                }

            return None

        except Exception as e:
            self.logger.error(f"Error getting establishment info: {e}")
            return None
