#!/usr/bin/env python3
"""
Similarity Calculator - Calculate similarity scores between products using various algorithms
"""

import math
from typing import Dict, List
from dataclasses import dataclass
from utils.logging.logging_manager import LogManager
from .feature_extractor import ProductFeatures


@dataclass
class SimilarityResult:
    """Data class to hold similarity calculation results"""

    product1: ProductFeatures
    product2: ProductFeatures

    # Individual similarity scores
    jaccard_score: float
    cosine_score: float
    levenshtein_score: float
    token_overlap_score: float

    # Weighted final score
    final_score: float

    # Matching details
    matching_tokens: List[str]
    matching_bigrams: List[str]
    brand_match: bool
    category_match: bool

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "product1_description": self.product1.original_description,
            "product2_description": self.product2.original_description,
            "jaccard_score": self.jaccard_score,
            "cosine_score": self.cosine_score,
            "levenshtein_score": self.levenshtein_score,
            "token_overlap_score": self.token_overlap_score,
            "final_score": self.final_score,
            "matching_tokens": self.matching_tokens,
            "matching_bigrams": self.matching_bigrams,
            "brand_match": self.brand_match,
            "category_match": self.category_match,
        }


class SimilarityCalculator:
    """
    Calculate similarity scores between products using multiple algorithms.

    This class implements various similarity metrics and combines them
    to produce a robust similarity score for product matching.
    """

    def __init__(self, similarity_threshold: float = 0.60):
        self.logger = LogManager.get_instance().get_logger("SimilarityCalculator")

        # Optimal similarity threshold (determined through training)
        self.similarity_threshold = similarity_threshold

        # Algorithm weights for final score calculation
        self.weights = {
            "jaccard": 0.3,
            "cosine": 0.25,
            "levenshtein": 0.2,
            "token_overlap": 0.25,
        }

        # Bonuses for specific matches
        self.bonuses = {"same_brand": 0.1, "same_category": 0.15, "core_key_match": 0.2}

        # Penalties for mismatches
        self.penalties = {"different_category": -0.3, "no_token_overlap": -0.2}

    def calculate_similarity(
        self, features1: ProductFeatures, features2: ProductFeatures
    ) -> SimilarityResult:
        """
        Calculate comprehensive similarity between two product features.

        Args:
            features1: First product features
            features2: Second product features

        Returns:
            SimilarityResult with detailed scores
        """

        self.logger.debug(
            f"Calculating similarity between '{features1.original_description}' and '{features2.original_description}'"
        )

        # Calculate individual similarity scores
        jaccard_score = self._jaccard_similarity(features1, features2)
        cosine_score = self._cosine_similarity(features1, features2)
        levenshtein_score = self._levenshtein_similarity(features1, features2)
        token_overlap_score = self._token_overlap_similarity(features1, features2)

        # Calculate matching details
        matching_tokens = self._get_matching_tokens(features1, features2)
        matching_bigrams = self._get_matching_bigrams(features1, features2)
        brand_match = self._is_brand_match(features1, features2)
        category_match = self._is_category_match(features1, features2)

        # Calculate weighted final score
        final_score = self._calculate_final_score(
            jaccard_score,
            cosine_score,
            levenshtein_score,
            token_overlap_score,
            features1,
            features2,
            matching_tokens,
        )

        result = SimilarityResult(
            product1=features1,
            product2=features2,
            jaccard_score=jaccard_score,
            cosine_score=cosine_score,
            levenshtein_score=levenshtein_score,
            token_overlap_score=token_overlap_score,
            final_score=final_score,
            matching_tokens=matching_tokens,
            matching_bigrams=matching_bigrams,
            brand_match=brand_match,
            category_match=category_match,
        )

        self.logger.debug(f"Similarity result: {final_score:.3f}")
        return result

    def _jaccard_similarity(
        self, features1: ProductFeatures, features2: ProductFeatures
    ) -> float:
        """
        Calculate Jaccard similarity based on token sets.

        Jaccard = |A ∩ B| / |A ∪ B|
        """

        set1 = set(features1.tokens)
        set2 = set(features2.tokens)

        if not set1 and not set2:
            return 1.0

        if not set1 or not set2:
            return 0.0

        intersection = set1.intersection(set2)
        union = set1.union(set2)

        return len(intersection) / len(union)

    def _cosine_similarity(
        self, features1: ProductFeatures, features2: ProductFeatures
    ) -> float:
        """
        Calculate cosine similarity based on token frequency vectors.

        Cosine = (A · B) / (||A|| × ||B||)
        """

        # Create frequency vectors for all unique tokens
        all_tokens = set(features1.tokens + features2.tokens)

        if not all_tokens:
            return 1.0

        # Build frequency vectors
        vector1 = [features1.tokens.count(token) for token in all_tokens]
        vector2 = [features2.tokens.count(token) for token in all_tokens]

        # Calculate dot product
        dot_product = sum(v1 * v2 for v1, v2 in zip(vector1, vector2))

        # Calculate magnitudes
        magnitude1 = math.sqrt(sum(v * v for v in vector1))
        magnitude2 = math.sqrt(sum(v * v for v in vector2))

        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0

        return dot_product / (magnitude1 * magnitude2)

    def _levenshtein_similarity(
        self, features1: ProductFeatures, features2: ProductFeatures
    ) -> float:
        """
        Calculate normalized Levenshtein similarity based on normalized descriptions.

        Similarity = 1 - (distance / max_length)
        """

        text1 = features1.normalized_description
        text2 = features2.normalized_description

        if text1 == text2:
            return 1.0

        if not text1 or not text2:
            return 0.0

        distance = self._levenshtein_distance(text1, text2)
        max_length = max(len(text1), len(text2))

        return 1.0 - (distance / max_length)

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings"""

        if len(s1) < len(s2):
            s1, s2 = s2, s1

        if len(s2) == 0:
            return len(s1)

        previous_row = list(range(len(s2) + 1))

        for i, c1 in enumerate(s1):
            current_row = [i + 1]

            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))

            previous_row = current_row

        return previous_row[-1]

    def _token_overlap_similarity(
        self, features1: ProductFeatures, features2: ProductFeatures
    ) -> float:
        """
        Calculate token overlap similarity with position weighting.

        Gives higher weight to matching tokens in similar positions.
        """

        tokens1 = features1.tokens
        tokens2 = features2.tokens

        if not tokens1 and not tokens2:
            return 1.0

        if not tokens1 or not tokens2:
            return 0.0

        # Simple overlap calculation
        overlap_count = 0
        total_tokens = len(set(tokens1 + tokens2))

        for token in set(tokens1):
            if token in tokens2:
                overlap_count += 1

        return overlap_count / total_tokens if total_tokens > 0 else 0.0

    def _get_matching_tokens(
        self, features1: ProductFeatures, features2: ProductFeatures
    ) -> List[str]:
        """Get list of matching tokens between two products"""

        set1 = set(features1.tokens)
        set2 = set(features2.tokens)

        return list(set1.intersection(set2))

    def _get_matching_bigrams(
        self, features1: ProductFeatures, features2: ProductFeatures
    ) -> List[str]:
        """Get list of matching bigrams between two products"""

        set1 = set(features1.bigrams)
        set2 = set(features2.bigrams)

        return list(set1.intersection(set2))

    def _is_brand_match(
        self, features1: ProductFeatures, features2: ProductFeatures
    ) -> bool:
        """Check if brands match"""

        if features1.brand is None or features2.brand is None:
            return False

        return features1.brand == features2.brand

    def _is_category_match(
        self, features1: ProductFeatures, features2: ProductFeatures
    ) -> bool:
        """Check if categories match"""

        return features1.category == features2.category

    def _calculate_final_score(
        self,
        jaccard: float,
        cosine: float,
        levenshtein: float,
        token_overlap: float,
        features1: ProductFeatures,
        features2: ProductFeatures,
        matching_tokens: List[str],
    ) -> float:
        """
        Calculate final weighted similarity score with bonuses and penalties.
        """

        # Base weighted score
        base_score = (
            jaccard * self.weights["jaccard"]
            + cosine * self.weights["cosine"]
            + levenshtein * self.weights["levenshtein"]
            + token_overlap * self.weights["token_overlap"]
        )

        # Apply bonuses
        if self._is_brand_match(features1, features2):
            base_score += self.bonuses["same_brand"]

        if self._is_category_match(features1, features2):
            base_score += self.bonuses["same_category"]

        # Core key matching bonus
        if features1.core_key and features2.core_key:
            core_similarity = self._jaccard_similarity_text(
                features1.core_key, features2.core_key
            )
            if core_similarity > 0.7:
                base_score += self.bonuses["core_key_match"]

        # Apply penalties
        if not self._is_category_match(features1, features2):
            base_score += self.penalties["different_category"]

        if not matching_tokens:
            base_score += self.penalties["no_token_overlap"]

        # Ensure score is between 0 and 1
        return max(0.0, min(1.0, base_score))

    def _jaccard_similarity_text(self, text1: str, text2: str) -> float:
        """Calculate Jaccard similarity between two text strings"""

        if not text1 and not text2:
            return 1.0

        if not text1 or not text2:
            return 0.0

        set1 = set(text1.split())
        set2 = set(text2.split())

        intersection = set1.intersection(set2)
        union = set1.union(set2)

        return len(intersection) / len(union) if union else 0.0

    def calculate_batch_similarity(
        self, features_list: List[ProductFeatures], threshold: float = None
    ) -> List[SimilarityResult]:
        """
        Calculate similarity for all pairs in a batch of features.

        Args:
            features_list: List of ProductFeatures to compare
            threshold: Minimum similarity threshold to include in results

        Returns:
            List of SimilarityResult objects above threshold
        """

        if threshold is None:
            threshold = self.similarity_threshold

        self.logger.info(
            f"Calculating batch similarity for {len(features_list)} products (threshold: {threshold})"
        )

        results = []
        total_comparisons = len(features_list) * (len(features_list) - 1) // 2
        processed = 0

        for i in range(len(features_list)):
            for j in range(i + 1, len(features_list)):
                result = self.calculate_similarity(features_list[i], features_list[j])

                if result.final_score >= threshold:
                    results.append(result)

                processed += 1

                if processed % 1000 == 0:
                    self.logger.debug(
                        f"Processed {processed}/{total_comparisons} comparisons"
                    )

        # Sort by similarity score (descending)
        results.sort(key=lambda x: x.final_score, reverse=True)

        self.logger.info(
            f"Found {len(results)} similar pairs above threshold {threshold}"
        )

        return results

    def find_duplicates(
        self, features_list: List[ProductFeatures], duplicate_threshold: float = None
    ) -> List[SimilarityResult]:
        """
        Find likely duplicate products based on high similarity scores.

        Args:
            features_list: List of ProductFeatures to analyze
            duplicate_threshold: Minimum similarity to consider as duplicate (uses self.similarity_threshold if None)

        Returns:
            List of high-confidence duplicate pairs
        """

        if duplicate_threshold is None:
            duplicate_threshold = self.similarity_threshold

        self.logger.info(f"Finding duplicates with threshold {duplicate_threshold}")

        duplicates = self.calculate_batch_similarity(features_list, duplicate_threshold)

        # Additional filtering for duplicates
        high_confidence_duplicates = []

        for result in duplicates:
            # High confidence criteria
            if (
                result.final_score >= duplicate_threshold
                and result.category_match
                and len(result.matching_tokens) >= 2
            ):
                high_confidence_duplicates.append(result)

        self.logger.info(
            f"Found {len(high_confidence_duplicates)} high-confidence duplicates"
        )

        return high_confidence_duplicates

    def get_similarity_stats(self, results: List[SimilarityResult]) -> Dict:
        """
        Get statistics about similarity results.

        Args:
            results: List of SimilarityResult objects

        Returns:
            Statistics dictionary
        """

        if not results:
            return {}

        scores = [r.final_score for r in results]
        jaccard_scores = [r.jaccard_score for r in results]
        cosine_scores = [r.cosine_score for r in results]

        stats = {
            "total_pairs": len(results),
            "score_stats": {
                "min": min(scores),
                "max": max(scores),
                "avg": sum(scores) / len(scores),
                "median": sorted(scores)[len(scores) // 2],
            },
            "algorithm_averages": {
                "jaccard": sum(jaccard_scores) / len(jaccard_scores),
                "cosine": sum(cosine_scores) / len(cosine_scores),
            },
            "brand_matches": sum(1 for r in results if r.brand_match),
            "category_matches": sum(1 for r in results if r.category_match),
            "high_similarity_count": sum(1 for r in results if r.final_score > 0.8),
        }

        return stats
