#!/usr/bin/env python3
"""
Product Matcher - Main component that orchestrates product similarity detection and matching
"""

from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from utils.logging.logging_manager import LogManager
from utils.cache_manager.cache_manager import CacheManager
from .product_normalizer import ProductNormalizer
from .feature_extractor import FeatureExtractor, ProductFeatures
from .similarity_calculator import SimilarityCalculator, SimilarityResult


@dataclass
class MatchGroup:
    """Data class representing a group of similar products"""

    group_id: str
    representative_product: str  # Most representative product description
    products: List[Dict]  # List of products in this group
    similarity_scores: List[float]  # Similarity scores within group
    avg_similarity: float
    size: int

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)


@dataclass
class MatchingResults:
    """Data class containing complete matching results"""

    total_products: int
    total_groups: int
    duplicate_groups: List[MatchGroup]
    similar_groups: List[MatchGroup]
    singleton_products: List[Dict]  # Products with no matches

    # Statistics
    deduplication_ratio: float  # Reduction in unique products
    avg_group_size: float
    largest_group_size: int

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "total_products": self.total_products,
            "total_groups": self.total_groups,
            "duplicate_groups": [group.to_dict() for group in self.duplicate_groups],
            "similar_groups": [group.to_dict() for group in self.similar_groups],
            "singleton_products": self.singleton_products,
            "deduplication_ratio": self.deduplication_ratio,
            "avg_group_size": self.avg_group_size,
            "largest_group_size": self.largest_group_size,
        }


class ProductMatcher:
    """
    Main product matching engine that coordinates all similarity components.

    This class provides the high-level interface for:
    - Finding duplicate products
    - Grouping similar products
    - Analyzing product relationships
    - Generating deduplication recommendations
    """

    def __init__(self, cache_enabled: bool = True):
        self.logger = LogManager.get_instance().get_logger("ProductMatcher")
        self.cache_enabled = cache_enabled

        if cache_enabled:
            self.cache = CacheManager.get_instance()

        # Initialize components
        self.normalizer = ProductNormalizer()
        self.feature_extractor = FeatureExtractor()
        self.similarity_calculator = SimilarityCalculator()

        # Configurable thresholds
        self.thresholds = {
            "duplicate": 0.85,  # Very high similarity = duplicate
            "similar": 0.65,  # Medium similarity = related products
            "minimum": 0.3,  # Minimum threshold for any matching
        }

    def analyze_products(self, products: List[Dict]) -> MatchingResults:
        """
        Analyze a list of products to find duplicates and similar items.

        Args:
            products: List of product dictionaries with 'description' field

        Returns:
            MatchingResults with complete analysis
        """

        self.logger.info(f"Starting product analysis for {len(products)} products")

        # Validate input
        if not products:
            raise ValueError("Product list cannot be empty")

        # Check cache first
        cache_key = f"product_analysis_{hash(str(sorted([p.get('description', '') for p in products])))}"

        if self.cache_enabled:
            cached_result = self.cache.load(cache_key, expiration_minutes=60)
            if cached_result:
                self.logger.info("Using cached analysis results")
                return self._dict_to_matching_results(cached_result)

        # Step 1: Extract features for all products
        self.logger.info("Extracting features from products...")
        features_list = self._extract_features_from_products(products)

        # Step 2: Calculate similarity between all products
        self.logger.info("Calculating similarity scores...")
        similarity_results = self.similarity_calculator.calculate_batch_similarity(
            features_list, threshold=self.thresholds["minimum"]
        )

        # Step 3: Group products by similarity
        self.logger.info("Grouping similar products...")
        matching_results = self._group_products_by_similarity(
            products, similarity_results
        )

        # Step 4: Cache results
        if self.cache_enabled:
            self.cache.save(cache_key, matching_results.to_dict())

        self.logger.info(
            f"Analysis complete. Found {len(matching_results.duplicate_groups)} duplicate groups"
        )

        return matching_results

    def find_duplicates_only(self, products: List[Dict]) -> List[MatchGroup]:
        """
        Find only duplicate products (very high similarity).

        Args:
            products: List of product dictionaries

        Returns:
            List of duplicate groups
        """

        self.logger.info(f"Finding duplicates in {len(products)} products")

        features_list = self._extract_features_from_products(products)

        duplicate_results = self.similarity_calculator.find_duplicates(
            features_list, duplicate_threshold=self.thresholds["duplicate"]
        )

        # Group duplicates
        duplicate_groups = self._create_duplicate_groups(products, duplicate_results)

        self.logger.info(f"Found {len(duplicate_groups)} duplicate groups")

        return duplicate_groups

    def find_similar_to_product(
        self, target_product: Dict, product_list: List[Dict], limit: int = 10
    ) -> List[Dict]:
        """
        Find products similar to a specific target product.

        Args:
            target_product: Product to find matches for
            product_list: List of products to search in
            limit: Maximum number of similar products to return

        Returns:
            List of similar products with similarity scores
        """

        target_description = target_product.get("description", "")
        if not target_description:
            raise ValueError("Target product must have a description")

        self.logger.info(f"Finding products similar to: '{target_description}'")

        # Extract features for target product
        target_features = self.feature_extractor.extract(target_description)

        # Extract features for all products in list
        all_features = self._extract_features_from_products(product_list)

        # Calculate similarity with target
        similarities = []
        for i, features in enumerate(all_features):
            result = self.similarity_calculator.calculate_similarity(
                target_features, features
            )

            if result.final_score > self.thresholds["minimum"]:
                product_with_score = product_list[i].copy()
                product_with_score["similarity_score"] = result.final_score
                product_with_score["matching_details"] = result.to_dict()
                similarities.append(product_with_score)

        # Sort by similarity and limit results
        similarities.sort(key=lambda x: x["similarity_score"], reverse=True)

        results = similarities[:limit]
        self.logger.info(f"Found {len(results)} similar products")

        return results

    def get_deduplication_recommendations(self, products: List[Dict]) -> Dict:
        """
        Generate recommendations for product deduplication.

        Args:
            products: List of products to analyze

        Returns:
            Dictionary with deduplication recommendations
        """

        self.logger.info("Generating deduplication recommendations")

        # Analyze products
        results = self.analyze_products(products)

        recommendations = {
            "summary": {
                "total_products": results.total_products,
                "potential_duplicates": len(results.duplicate_groups),
                "estimated_reduction": results.deduplication_ratio,
                "products_to_review": sum(
                    group.size for group in results.duplicate_groups
                ),
            },
            "high_priority_groups": [],
            "medium_priority_groups": [],
            "actions": [],
        }

        # Categorize groups by priority
        for group in results.duplicate_groups:
            if group.avg_similarity > 0.9 and group.size > 2:
                recommendations["high_priority_groups"].append(group.to_dict())  # type: ignore
            else:
                recommendations["medium_priority_groups"].append(group.to_dict())  # type: ignore

        # Generate action recommendations
        if recommendations["high_priority_groups"]:
            recommendations["actions"].append(  # type: ignore
                "Review high-priority groups first - these are likely true duplicates"
            )

        if recommendations["medium_priority_groups"]:
            recommendations["actions"].append(  # type: ignore
                "Medium-priority groups may be variants of the same product"
            )

        if results.deduplication_ratio > 0.1:
            recommendations["actions"].append(  # type: ignore
                f"Significant deduplication opportunity: {results.deduplication_ratio:.1%} reduction possible"
            )

        return recommendations

    def _extract_features_from_products(
        self, products: List[Dict]
    ) -> List[ProductFeatures]:
        """Extract features from all products in the list"""

        features_list = []
        errors = 0

        for product in products:
            description = product.get("description", "")
            if not description:
                self.logger.warning(f"Product missing description: {product}")
                errors += 1
                continue

            try:
                features = self.feature_extractor.extract(description)
                features_list.append(features)
            except Exception as e:
                self.logger.error(
                    f"Error extracting features from '{description}': {e}"
                )
                errors += 1

        if errors > 0:
            self.logger.warning(f"Failed to extract features from {errors} products")

        return features_list

    def _group_products_by_similarity(
        self, products: List[Dict], similarity_results: List[SimilarityResult]
    ) -> MatchingResults:
        """Group products into similarity groups"""

        # Create adjacency graph of similar products
        product_graph: Dict[int, set] = {}
        for i in range(len(products)):
            product_graph[i] = set()

        # Add edges for similar products
        for result in similarity_results:
            # Find indices of products in similarity result
            desc1 = result.product1.original_description
            desc2 = result.product2.original_description

            idx1 = self._find_product_index(products, desc1)
            idx2 = self._find_product_index(products, desc2)

            if idx1 is not None and idx2 is not None:
                product_graph[idx1].add(idx2)
                product_graph[idx2].add(idx1)

        # Find connected components (groups)
        visited: set = set()
        groups = []

        for i in range(len(products)):
            if i not in visited:
                group = self._dfs_group(product_graph, i, visited)
                groups.append(group)

        # Create MatchGroup objects
        duplicate_groups: List[MatchGroup] = []
        similar_groups: List[MatchGroup] = []
        singleton_products = []

        for group_indices in groups:
            if len(group_indices) == 1:
                # Singleton product
                idx = list(group_indices)[0]
                singleton_products.append(products[idx])
            else:
                # Multi-product group
                group_products = [products[i] for i in group_indices]

                # Calculate average similarity within group
                group_similarities = self._calculate_group_similarities(
                    group_indices, similarity_results, products
                )

                avg_similarity = (
                    sum(group_similarities) / len(group_similarities)
                    if group_similarities
                    else 0
                )

                # Choose representative product (most central/common)
                representative = self._choose_representative_product(
                    group_products, group_similarities
                )

                match_group = MatchGroup(
                    group_id=f"group_{len(duplicate_groups + similar_groups)}",
                    representative_product=representative,
                    products=group_products,
                    similarity_scores=group_similarities,
                    avg_similarity=avg_similarity,
                    size=len(group_products),
                )

                # Classify as duplicate or similar
                if avg_similarity >= self.thresholds["duplicate"]:
                    duplicate_groups.append(match_group)
                else:
                    similar_groups.append(match_group)

        # Calculate statistics
        total_groups = (
            len(duplicate_groups) + len(similar_groups) + len(singleton_products)
        )
        deduplication_ratio = (
            1 - (total_groups / len(products)) if len(products) > 0 else 0
        )

        all_groups = duplicate_groups + similar_groups
        avg_group_size = (
            sum(g.size for g in all_groups) / len(all_groups) if all_groups else 1
        )
        largest_group_size = max((g.size for g in all_groups), default=1)

        return MatchingResults(
            total_products=len(products),
            total_groups=total_groups,
            duplicate_groups=duplicate_groups,
            similar_groups=similar_groups,
            singleton_products=singleton_products,
            deduplication_ratio=deduplication_ratio,
            avg_group_size=avg_group_size,
            largest_group_size=largest_group_size,
        )

    def _find_product_index(
        self, products: List[Dict], description: str
    ) -> Optional[int]:
        """Find the index of a product by its description"""

        for i, product in enumerate(products):
            if product.get("description", "") == description:
                return i
        return None

    def _dfs_group(self, graph: Dict[int, set], start: int, visited: set) -> set:
        """Depth-first search to find connected components"""

        group = set()
        stack = [start]

        while stack:
            node = stack.pop()
            if node not in visited:
                visited.add(node)
                group.add(node)
                stack.extend(graph[node] - visited)

        return group

    def _calculate_group_similarities(
        self,
        group_indices: set,
        similarity_results: List[SimilarityResult],
        products: List[Dict],
    ) -> List[float]:
        """Calculate similarities within a group"""

        similarities = []

        for result in similarity_results:
            desc1 = result.product1.original_description
            desc2 = result.product2.original_description

            idx1 = self._find_product_index(products, desc1)
            idx2 = self._find_product_index(products, desc2)

            if idx1 in group_indices and idx2 in group_indices:
                similarities.append(result.final_score)

        return similarities

    def _choose_representative_product(
        self, group_products: List[Dict], similarities: List[float]
    ) -> str:
        """Choose the most representative product from a group"""

        if not group_products:
            return ""

        # For now, choose the first product
        # TODO: Implement more sophisticated selection based on:
        # - Most common words
        # - Shortest description
        # - Highest frequency in dataset

        return group_products[0].get("description", "")

    def _create_duplicate_groups(
        self, products: List[Dict], duplicate_results: List[SimilarityResult]
    ) -> List[MatchGroup]:
        """Create MatchGroup objects from duplicate results"""

        # Implementation similar to _group_products_by_similarity but simpler
        # since we only care about duplicates

        groups = []
        processed_descriptions = set()

        for i, result in enumerate(duplicate_results):
            desc1 = result.product1.original_description
            desc2 = result.product2.original_description

            if desc1 in processed_descriptions or desc2 in processed_descriptions:
                continue

            # Find all products in this group
            group_products = []
            for product in products:
                if product.get("description") in [desc1, desc2]:
                    group_products.append(product)

            if len(group_products) >= 2:
                match_group = MatchGroup(
                    group_id=f"duplicate_group_{i}",
                    representative_product=group_products[0].get("description", ""),
                    products=group_products,
                    similarity_scores=[result.final_score],
                    avg_similarity=result.final_score,
                    size=len(group_products),
                )

                groups.append(match_group)

                # Mark as processed
                for product in group_products:
                    processed_descriptions.add(product.get("description", ""))

        return groups

    def _dict_to_matching_results(self, data: Dict) -> MatchingResults:
        """Convert dictionary back to MatchingResults object"""

        # Convert group dictionaries back to MatchGroup objects
        duplicate_groups = [
            MatchGroup(**group) for group in data.get("duplicate_groups", [])
        ]
        similar_groups = [
            MatchGroup(**group) for group in data.get("similar_groups", [])
        ]

        return MatchingResults(
            total_products=data.get("total_products", 0),
            total_groups=data.get("total_groups", 0),
            duplicate_groups=duplicate_groups,
            similar_groups=similar_groups,
            singleton_products=data.get("singleton_products", []),
            deduplication_ratio=data.get("deduplication_ratio", 0.0),
            avg_group_size=data.get("avg_group_size", 0.0),
            largest_group_size=data.get("largest_group_size", 0),
        )
