#!/usr/bin/env python3
"""Enhanced Similarity Calculator - Integrates SBERT embeddings with Brazilian token rules"""

import math
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass

from utils.logging.logging_manager import LogManager

from .feature_extractor import ProductFeatures
from .hybrid_similarity_engine import HybridSimilarityEngine


@dataclass
class EnhancedSimilarityResult:
    """Enhanced similarity result combining traditional and hybrid approaches"""

    product1: ProductFeatures
    product2: ProductFeatures

    # Traditional similarity scores
    jaccard_score: float
    cosine_score: float
    levenshtein_score: float
    token_overlap_score: float

    # Hybrid similarity scores
    embedding_similarity: float
    token_rule_similarity: float
    quantity_similarity: float
    brand_similarity: float

    # Final weighted score
    final_score: float
    confidence_score: float

    # Matching details
    matching_tokens: list[str]
    matching_bigrams: list[str]
    brazilian_tokens: list[str]
    quantity_matches: list[str]
    brand_match: bool
    category_match: bool

    # Explanation
    explanation: str

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "product1_description": self.product1.original_description,
            "product2_description": self.product2.original_description,
            # Traditional scores
            "jaccard_score": self.jaccard_score,
            "cosine_score": self.cosine_score,
            "levenshtein_score": self.levenshtein_score,
            "token_overlap_score": self.token_overlap_score,
            # Hybrid scores
            "embedding_similarity": self.embedding_similarity,
            "token_rule_similarity": self.token_rule_similarity,
            "quantity_similarity": self.quantity_similarity,
            "brand_similarity": self.brand_similarity,
            # Final results
            "final_score": self.final_score,
            "confidence_score": self.confidence_score,
            # Details
            "matching_tokens": self.matching_tokens,
            "matching_bigrams": self.matching_bigrams,
            "brazilian_tokens": self.brazilian_tokens,
            "quantity_matches": self.quantity_matches,
            "brand_match": self.brand_match,
            "category_match": self.category_match,
            "explanation": self.explanation,
        }


class EnhancedSimilarityCalculator:
    """Enhanced similarity calculator that combines:
    - Traditional algorithms (Jaccard, Cosine, Levenshtein)
    - SBERT embeddings for semantic similarity
    - Brazilian product token rules
    - Quantity and brand matching
    """

    def __init__(
        self,
        similarity_threshold: float = 0.60,
        use_hybrid: bool = True,
        sbert_model: str = "rufimelo/Legal-BERTimbau-large",
    ):
        """Initialize enhanced similarity calculator

        Args:
            similarity_threshold: Optimal threshold for similarity matching
            use_hybrid: Whether to use hybrid SBERT + rules approach
            sbert_model: SBERT model name for Portuguese embeddings
        """
        self.logger = LogManager.get_instance().get_logger("EnhancedSimilarityCalculator")
        self.similarity_threshold = similarity_threshold
        self.use_hybrid = use_hybrid

        # Initialize hybrid engine
        if use_hybrid:
            try:
                self.hybrid_engine = HybridSimilarityEngine(model_name=sbert_model)
                self.logger.info("Hybrid similarity engine initialized successfully")
            except Exception as e:
                self.logger.warning(f"Failed to initialize hybrid engine: {e}")
                self.hybrid_engine = None
                self.use_hybrid = False
        else:
            self.hybrid_engine = None

        # Traditional algorithm weights (adjusted for hybrid mode)
        if use_hybrid and self.hybrid_engine:
            # Lower traditional weights when hybrid is available
            self.weights = {
                "traditional": 0.3,  # Combined traditional algorithms
                "hybrid": 0.7,  # Hybrid SBERT + rules
            }
            self.traditional_weights = {
                "jaccard": 0.4,
                "cosine": 0.3,
                "levenshtein": 0.15,
                "token_overlap": 0.15,
            }
        else:
            # Original weights when hybrid not available
            self.weights = {"traditional": 1.0, "hybrid": 0.0}
            self.traditional_weights = {
                "jaccard": 0.3,
                "cosine": 0.25,
                "levenshtein": 0.2,
                "token_overlap": 0.25,
            }

        # Bonuses and penalties
        self.bonuses = {
            "same_brand": 0.05,
            "same_category": 0.1,
            "core_key_match": 0.15,
            "high_confidence": 0.05,
        }

        self.penalties = {
            "different_category": -0.2,
            "no_token_overlap": -0.1,
            "low_confidence": -0.05,
        }

        self.logger.info(
            f"Enhanced similarity calculator initialized (hybrid: {use_hybrid}, threshold: {similarity_threshold})"
        )

    def calculate_similarity(self, features1: ProductFeatures, features2: ProductFeatures) -> EnhancedSimilarityResult:
        """Calculate enhanced similarity between two product features

        Args:
            features1: First product features
            features2: Second product features

        Returns:
            EnhancedSimilarityResult with detailed analysis
        """
        self.logger.debug(
            f"Calculating enhanced similarity: '{features1.original_description}' vs '{features2.original_description}'"
        )

        # Calculate traditional similarity scores
        jaccard_score = self._jaccard_similarity(features1, features2)
        cosine_score = self._cosine_similarity(features1, features2)
        levenshtein_score = self._levenshtein_similarity(features1, features2)
        token_overlap_score = self._token_overlap_similarity(features1, features2)

        # Calculate traditional matching details
        matching_tokens = self._get_matching_tokens(features1, features2)
        matching_bigrams = self._get_matching_bigrams(features1, features2)
        brand_match = self._is_brand_match(features1, features2)
        category_match = self._is_category_match(features1, features2)

        # Calculate traditional combined score
        traditional_score = (
            jaccard_score * self.traditional_weights["jaccard"]
            + cosine_score * self.traditional_weights["cosine"]
            + levenshtein_score * self.traditional_weights["levenshtein"]
            + token_overlap_score * self.traditional_weights["token_overlap"]
        )

        # Initialize hybrid scores
        hybrid_result = None
        embedding_similarity = 0.0
        token_rule_similarity = 0.0
        quantity_similarity = 0.0
        brand_similarity = 0.0
        brazilian_tokens = []
        quantity_matches = []
        confidence_score = 0.5
        explanation = "Traditional algorithms only"

        # Calculate hybrid similarity if available
        if self.use_hybrid and self.hybrid_engine:
            try:
                hybrid_result = self.hybrid_engine.calculate_similarity(
                    features1.original_description, features2.original_description
                )

                embedding_similarity = hybrid_result.embedding_similarity
                token_rule_similarity = hybrid_result.token_rule_similarity
                quantity_similarity = (hybrid_result.quantity_matches and 1.0) or 0.0
                brand_similarity = hybrid_result.brand_similarity
                brazilian_tokens = hybrid_result.brazilian_tokens
                quantity_matches = hybrid_result.quantity_matches
                confidence_score = hybrid_result.confidence_score
                explanation = hybrid_result.explanation

            except Exception as e:
                self.logger.error(f"Error in hybrid similarity calculation: {e}")
                # Fall back to traditional only
                hybrid_result = None

        # Calculate final combined score
        if hybrid_result:
            # Combine traditional and hybrid scores
            final_score = (
                traditional_score * self.weights["traditional"]
                + hybrid_result.final_similarity * self.weights["hybrid"]
            )
        else:
            # Use only traditional score
            final_score = traditional_score

        # Apply bonuses and penalties
        final_score = self._apply_bonuses_penalties(
            final_score,
            brand_match,
            category_match,
            matching_tokens,
            confidence_score,
            features1,
            features2,
        )

        # Ensure score is between 0 and 1
        final_score = max(0.0, min(1.0, final_score))

        # Create enhanced result
        result = EnhancedSimilarityResult(
            product1=features1,
            product2=features2,
            # Traditional scores
            jaccard_score=jaccard_score,
            cosine_score=cosine_score,
            levenshtein_score=levenshtein_score,
            token_overlap_score=token_overlap_score,
            # Hybrid scores
            embedding_similarity=embedding_similarity,
            token_rule_similarity=token_rule_similarity,
            quantity_similarity=quantity_similarity,
            brand_similarity=brand_similarity,
            # Final results
            final_score=final_score,
            confidence_score=confidence_score,
            # Details
            matching_tokens=matching_tokens,
            matching_bigrams=matching_bigrams,
            brazilian_tokens=brazilian_tokens,
            quantity_matches=quantity_matches,
            brand_match=brand_match,
            category_match=category_match,
            explanation=explanation,
        )

        self.logger.debug(f"Enhanced similarity result: {final_score:.3f} (confidence: {confidence_score:.3f})")

        return result

    def calculate_batch_similarity(
        self,
        features_list: list[ProductFeatures],
        threshold: float = None,
        use_parallel: bool = True,
        max_workers: int = None,
    ) -> list[EnhancedSimilarityResult]:
        """Calculate enhanced similarity for all pairs in a batch with optimizations

        Args:
            features_list: List of ProductFeatures to compare
            threshold: Minimum similarity threshold to include in results
            use_parallel: Whether to use parallel processing
            max_workers: Number of parallel workers (default: CPU count)

        Returns:
            List of EnhancedSimilarityResult objects above threshold
        """
        if threshold is None:
            threshold = self.similarity_threshold

        n_products = len(features_list)
        self.logger.info(f"Calculating enhanced batch similarity for {n_products} products (threshold: {threshold})")

        # Quick exit for small datasets
        if n_products < 2:
            return []

        # Apply fast filters first to reduce comparison space
        filtered_features = self._apply_fast_filters(features_list, threshold)
        self.logger.info(f"After fast filtering: {len(filtered_features)} products remain")

        if len(filtered_features) < 2:
            return []

        total_comparisons = len(filtered_features) * (len(filtered_features) - 1) // 2
        self.logger.info(f"Total comparisons: {total_comparisons:,}")

        # Decide processing strategy based on size and parallel flag
        if use_parallel and total_comparisons > 10000 and len(filtered_features) > 100:
            return self._calculate_parallel(filtered_features, threshold, max_workers)
        else:
            return self._calculate_sequential(filtered_features, threshold)

    def _apply_fast_filters(self, features_list: list[ProductFeatures], threshold: float) -> list[ProductFeatures]:
        """Apply fast pre-filters to reduce comparison space"""
        # Filter 1: Remove products with very short descriptions (likely noise)
        filtered = [f for f in features_list if len(f.normalized_description) >= 3]

        # Filter 2: Group by core characteristics to avoid obviously different products
        category_groups = {}
        for feat in filtered:
            # Group by category or first significant token
            key = feat.category or (feat.tokens[0] if feat.tokens else "unknown")
            if key not in category_groups:
                category_groups[key] = []
            category_groups[key].append(feat)

        # Only keep products in groups with more than 1 item (potential for matches)
        result = []
        for group in category_groups.values():
            if len(group) > 1:
                result.extend(group)

        return result

    def _calculate_parallel(
        self,
        features_list: list[ProductFeatures],
        threshold: float,
        max_workers: int = None,
    ) -> list[EnhancedSimilarityResult]:
        """Calculate similarity using parallel processing"""
        if max_workers is None:
            max_workers = min(mp.cpu_count(), 8)  # Limit to 8 to avoid overwhelming

        self.logger.info(f"Using parallel processing with {max_workers} workers")

        # Split features into chunks for parallel processing
        chunk_size = max(10, len(features_list) // (max_workers * 4))
        chunks = [features_list[i : i + chunk_size] for i in range(0, len(features_list), chunk_size)]

        self.logger.info(f"Split into {len(chunks)} chunks of ~{chunk_size} products each")

        # Prepare arguments for parallel processing
        args_list = []
        for i, chunk in enumerate(chunks):
            # Each chunk compares against all features that come after it
            remaining_features = features_list[i * chunk_size :]
            if remaining_features:
                args_list.append(
                    (
                        chunk,
                        remaining_features,
                        threshold,
                        self.use_hybrid,
                        "paraphrase-multilingual-MiniLM-L12-v2",
                        self.similarity_threshold,
                    )
                )

        results = []
        processed_chunks = 0

        # Process chunks in parallel
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_chunk = {
                executor.submit(_calculate_similarity_chunk, args): i for i, args in enumerate(args_list)
            }

            for future in as_completed(future_to_chunk):
                try:
                    chunk_results = future.result()
                    results.extend(chunk_results)
                    processed_chunks += 1

                    percentage = (processed_chunks / len(args_list)) * 100
                    self.logger.info(
                        f"Completed chunk {processed_chunks}/{len(args_list)} ({percentage:.1f}%) - "
                        f"Found {len(chunk_results)} matches, Total: {len(results)}"
                    )

                except Exception as e:
                    self.logger.error(f"Error processing chunk: {e}")

        # Sort and deduplicate results
        results = self._deduplicate_results(results)
        results.sort(key=lambda x: (x.final_score, x.confidence_score), reverse=True)

        self.logger.info(f"Parallel processing complete: {len(results)} unique matches found")
        return results

    def _calculate_sequential(
        self, features_list: list[ProductFeatures], threshold: float
    ) -> list[EnhancedSimilarityResult]:
        """Calculate similarity sequentially with progress reporting"""
        results = []
        total_comparisons = len(features_list) * (len(features_list) - 1) // 2
        processed = 0
        matches_found = 0

        # Calculate progress reporting interval
        progress_interval = max(1000, total_comparisons // 50)
        self.logger.info(f"Sequential processing - reporting every {progress_interval:,} comparisons")

        for i in range(len(features_list)):
            for j in range(i + 1, len(features_list)):
                # Quick pre-check to avoid expensive calculation
                if self._quick_dissimilarity_check(features_list[i], features_list[j]):
                    processed += 1
                    continue

                result = self.calculate_similarity(features_list[i], features_list[j])

                if result.final_score >= threshold:
                    results.append(result)
                    matches_found += 1

                processed += 1

                if processed % progress_interval == 0:
                    percentage = (processed / total_comparisons) * 100
                    self.logger.info(
                        f"Progress: {processed:,}/{total_comparisons:,} ({percentage:.1f}%) - Matches: {matches_found}"
                    )

        results.sort(key=lambda x: (x.final_score, x.confidence_score), reverse=True)
        self.logger.info(f"Sequential processing complete: {len(results)} matches found")

        return results

    def _quick_dissimilarity_check(self, feat1: ProductFeatures, feat2: ProductFeatures) -> bool:
        """Quick check to identify obviously dissimilar products"""
        # Check 1: Very different description lengths
        len1, len2 = (
            len(feat1.normalized_description),
            len(feat2.normalized_description),
        )
        if max(len1, len2) / max(min(len1, len2), 1) > 3:
            return True

        # Check 2: No common tokens at all
        tokens1, tokens2 = set(feat1.tokens), set(feat2.tokens)
        if not tokens1.intersection(tokens2):
            return True

        # Check 3: Very different categories
        if feat1.category and feat2.category and feat1.category != feat2.category:
            # Allow some flexibility for related categories
            related_categories = {
                "bebida": ["refrigerante", "agua", "suco"],
                "alimento": ["doce", "salgado", "carne", "fruta"],
                "higiene": ["limpeza", "perfumaria"],
            }

            cat1, cat2 = feat1.category.lower(), feat2.category.lower()
            is_related = False

            for main_cat, sub_cats in related_categories.items():
                if (cat1 == main_cat and cat2 in sub_cats) or (cat2 == main_cat and cat1 in sub_cats):
                    is_related = True
                    break

            if not is_related:
                return True

        return False

    def _deduplicate_results(self, results: list[EnhancedSimilarityResult]) -> list[EnhancedSimilarityResult]:
        """Remove duplicate pairs from results"""
        seen = set()
        unique_results = []

        for result in results:
            # Create a normalized pair key
            desc1 = result.product1.normalized_description
            desc2 = result.product2.normalized_description
            pair_key = tuple(sorted([desc1, desc2]))

            if pair_key not in seen:
                seen.add(pair_key)
                unique_results.append(result)

        return unique_results

    def find_duplicates(
        self, features_list: list[ProductFeatures], duplicate_threshold: float = None
    ) -> list[EnhancedSimilarityResult]:
        """Find likely duplicate products using enhanced similarity

        Args:
            features_list: List of ProductFeatures to analyze
            duplicate_threshold: Minimum similarity to consider as duplicate

        Returns:
            List of high-confidence duplicate pairs
        """
        if duplicate_threshold is None:
            duplicate_threshold = self.similarity_threshold

        self.logger.info(f"Finding enhanced duplicates with threshold {duplicate_threshold}")

        duplicates = self.calculate_batch_similarity(features_list, duplicate_threshold)

        # Additional filtering for high-confidence duplicates
        high_confidence_duplicates = []

        for result in duplicates:
            # Enhanced confidence criteria
            if (
                result.final_score >= duplicate_threshold
                and result.confidence_score >= 0.6
                and (result.category_match or len(result.matching_tokens) >= 2 or result.embedding_similarity >= 0.7)
            ):
                high_confidence_duplicates.append(result)

        self.logger.info(f"Found {len(high_confidence_duplicates)} high-confidence enhanced duplicates")

        return high_confidence_duplicates

    # Traditional similarity methods (inherited from original SimilarityCalculator)
    def _jaccard_similarity(self, features1: ProductFeatures, features2: ProductFeatures) -> float:
        """Calculate Jaccard similarity based on token sets"""
        set1 = set(features1.tokens)
        set2 = set(features2.tokens)

        if not set1 and not set2:
            return 1.0
        if not set1 or not set2:
            return 0.0

        intersection = set1.intersection(set2)
        union = set1.union(set2)

        return len(intersection) / len(union)

    def _cosine_similarity(self, features1: ProductFeatures, features2: ProductFeatures) -> float:
        """Calculate cosine similarity based on token frequency vectors"""
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

    def _levenshtein_similarity(self, features1: ProductFeatures, features2: ProductFeatures) -> float:
        """Calculate normalized Levenshtein similarity"""
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

    def _token_overlap_similarity(self, features1: ProductFeatures, features2: ProductFeatures) -> float:
        """Calculate token overlap similarity with position weighting"""
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

    def _get_matching_tokens(self, features1: ProductFeatures, features2: ProductFeatures) -> list[str]:
        """Get list of matching tokens between two products"""
        set1 = set(features1.tokens)
        set2 = set(features2.tokens)
        return list(set1.intersection(set2))

    def _get_matching_bigrams(self, features1: ProductFeatures, features2: ProductFeatures) -> list[str]:
        """Get list of matching bigrams between two products"""
        set1 = set(features1.bigrams)
        set2 = set(features2.bigrams)
        return list(set1.intersection(set2))

    def _is_brand_match(self, features1: ProductFeatures, features2: ProductFeatures) -> bool:
        """Check if brands match"""
        if features1.brand is None or features2.brand is None:
            return False
        return features1.brand == features2.brand

    def _is_category_match(self, features1: ProductFeatures, features2: ProductFeatures) -> bool:
        """Check if categories match"""
        return features1.category == features2.category

    def _apply_bonuses_penalties(
        self,
        base_score: float,
        brand_match: bool,
        category_match: bool,
        matching_tokens: list[str],
        confidence_score: float,
        features1: ProductFeatures,
        features2: ProductFeatures,
    ) -> float:
        """Apply bonuses and penalties to the base score"""
        score = base_score

        # Apply bonuses
        if brand_match:
            score += self.bonuses["same_brand"]

        if category_match:
            score += self.bonuses["same_category"]

        # Core key matching bonus
        if features1.core_key and features2.core_key:
            core_similarity = self._jaccard_similarity_text(features1.core_key, features2.core_key)
            if core_similarity > 0.7:
                score += self.bonuses["core_key_match"]

        # High confidence bonus
        if confidence_score > 0.8:
            score += self.bonuses["high_confidence"]

        # Apply penalties
        if not category_match:
            score += self.penalties["different_category"]

        if not matching_tokens:
            score += self.penalties["no_token_overlap"]

        if confidence_score < 0.4:
            score += self.penalties["low_confidence"]

        return score

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


def _calculate_similarity_chunk(args) -> list[EnhancedSimilarityResult]:
    """Helper function for parallel similarity calculation
    This needs to be a top-level function for multiprocessing to work
    """
    (
        features_chunk,
        other_features,
        threshold,
        use_hybrid,
        sbert_model,
        similarity_threshold,
    ) = args

    # Initialize calculator for this process
    calc = EnhancedSimilarityCalculator(
        similarity_threshold=similarity_threshold,
        use_hybrid=use_hybrid,
        sbert_model=sbert_model,
    )

    results = []
    for feat1 in features_chunk:
        for feat2 in other_features:
            if feat1 != feat2:  # Avoid self-comparison
                result = calc.calculate_similarity(feat1, feat2)
                if result.final_score >= threshold:
                    results.append(result)

    return results
