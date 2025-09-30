#!/usr/bin/env python3
"""
Embedding Similarity - Calculate similarity using semantic embeddings
"""

import numpy as np
from typing import List, Dict, Tuple
from dataclasses import dataclass
from utils.logging.logging_manager import LogManager
from utils.cache_manager.cache_manager import CacheManager
from .feature_extractor import ProductFeatures


@dataclass
class EmbeddingResult:
    """Data class to hold embedding similarity results"""

    product1_description: str
    product2_description: str

    # Embedding similarity scores
    cosine_similarity: float
    euclidean_distance: float
    manhattan_distance: float

    # Normalized scores (0-1 range)
    normalized_cosine: float
    normalized_euclidean: float
    normalized_manhattan: float

    # Combined score
    final_score: float

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "product1_description": self.product1_description,
            "product2_description": self.product2_description,
            "cosine_similarity": self.cosine_similarity,
            "euclidean_distance": self.euclidean_distance,
            "manhattan_distance": self.manhattan_distance,
            "normalized_cosine": self.normalized_cosine,
            "normalized_euclidean": self.normalized_euclidean,
            "normalized_manhattan": self.normalized_manhattan,
            "final_score": self.final_score,
        }


class EmbeddingSimilarity:
    """
    Calculate similarity using semantic embeddings from sentence transformers.

    This class provides semantic similarity calculation using pre-trained
    multilingual models that can capture deep semantic relationships
    between product descriptions.
    """

    def __init__(
        self,
        model_name: str = "distiluse-base-multilingual-cased-v2",
        cache_enabled: bool = True,
    ):
        self.logger = LogManager.get_instance().get_logger("EmbeddingSimilarity")
        self.cache_enabled = cache_enabled
        self.model_name = model_name

        if cache_enabled:
            self.cache = CacheManager.get_instance()

        # Initialize model lazily
        self._model = None
        self._tokenizer = None

        # Weights for combining different similarity metrics
        self.weights = {"cosine": 0.6, "euclidean": 0.2, "manhattan": 0.2}

        # Cache keys
        self.embedding_cache_key = f"embeddings_{model_name.replace('-', '_')}"

    @property
    def model(self):
        """Lazy load the sentence transformer model"""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer

                self.logger.info(
                    f"Loading sentence transformer model: {self.model_name}"
                )
                self._model = SentenceTransformer(self.model_name)
                self.logger.info("Model loaded successfully")
            except ImportError:
                self.logger.error(
                    "sentence-transformers not installed. "
                    "Please install with: pip install sentence-transformers"
                )
                raise ImportError(
                    "sentence-transformers is required for embedding similarity"
                )
        return self._model

    def get_embedding(self, text: str) -> np.ndarray:
        """
        Get embedding for a text string.

        Args:
            text: Input text to embed

        Returns:
            numpy array with embedding vector
        """
        if not text or not text.strip():
            # Return zero vector for empty text
            return np.zeros(512)  # Default embedding size

        # Check cache first
        if self.cache_enabled:
            cache_key = f"{self.embedding_cache_key}:{hash(text)}"
            cached_embedding = self.cache.load(
                cache_key, expiration_minutes=1440
            )  # 24 hours
            if cached_embedding is not None:
                return np.array(cached_embedding)

        try:
            # Get embedding from model
            embedding = self.model.encode(text, convert_to_numpy=True)

            # Cache the result
            if self.cache_enabled:
                self.cache.save(cache_key, embedding.tolist())

            return embedding

        except Exception as e:
            self.logger.error(f"Error getting embedding for text '{text}': {e}")
            # Return zero vector on error
            return np.zeros(512)

    def get_embeddings_batch(self, texts: List[str]) -> List[np.ndarray]:
        """
        Get embeddings for a batch of texts (more efficient).

        Args:
            texts: List of texts to embed

        Returns:
            List of numpy arrays with embeddings
        """
        if not texts:
            return []

        # Filter out empty texts
        valid_texts = [text.strip() for text in texts if text and text.strip()]
        if not valid_texts:
            return [np.zeros(512)] * len(texts)

        # Check cache for all texts
        embeddings = []
        texts_to_embed = []
        indices_to_embed = []

        for i, text in enumerate(valid_texts):
            if self.cache_enabled:
                cache_key = f"{self.embedding_cache_key}:{hash(text)}"
                cached_embedding = self.cache.load(
                    cache_key, expiration_minutes=1440
                )  # 24 hours
                if cached_embedding is not None:
                    embeddings.append(np.array(cached_embedding))
                else:
                    texts_to_embed.append(text)
                    indices_to_embed.append(i)
            else:
                texts_to_embed.append(text)
                indices_to_embed.append(i)

        # Get embeddings for uncached texts
        if texts_to_embed:
            try:
                batch_embeddings = self.model.encode(
                    texts_to_embed, convert_to_numpy=True
                )

                # Cache new embeddings
                if self.cache_enabled:
                    for text, embedding in zip(texts_to_embed, batch_embeddings):
                        cache_key = f"{self.embedding_cache_key}:{hash(text)}"
                        self.cache.save(cache_key, embedding.tolist())

                # Insert embeddings at correct positions
                for i, embedding in zip(indices_to_embed, batch_embeddings):
                    embeddings.insert(i, embedding)

            except Exception as e:
                self.logger.error(f"Error getting batch embeddings: {e}")
                # Fill with zero vectors on error
                for i in indices_to_embed:
                    embeddings.insert(i, np.zeros(512))

        # Handle original empty texts
        final_embeddings = []
        valid_idx = 0
        for text in texts:
            if text and text.strip():
                final_embeddings.append(embeddings[valid_idx])
                valid_idx += 1
            else:
                final_embeddings.append(np.zeros(512))

        return final_embeddings

    def calculate_similarity(
        self, features1: ProductFeatures, features2: ProductFeatures
    ) -> EmbeddingResult:
        """
        Calculate semantic similarity between two product features.

        Args:
            features1: First product features
            features2: Second product features

        Returns:
            EmbeddingResult with similarity scores
        """
        self.logger.debug(
            f"Calculating embedding similarity between '{features1.original_description}' and '{features2.original_description}'"
        )

        # Get embeddings
        embedding1 = self.get_embedding(features1.original_description)
        embedding2 = self.get_embedding(features2.original_description)

        # Calculate different similarity metrics
        cosine_sim = self._cosine_similarity(embedding1, embedding2)
        euclidean_dist = self._euclidean_distance(embedding1, embedding2)
        manhattan_dist = self._manhattan_distance(embedding1, embedding2)

        # Normalize distances to similarity scores (0-1)
        normalized_euclidean = self._normalize_distance(
            euclidean_dist, max_distance=2.0
        )
        normalized_manhattan = self._normalize_distance(
            manhattan_dist, max_distance=4.0
        )

        # Calculate weighted final score
        final_score = (
            cosine_sim * self.weights["cosine"]
            + normalized_euclidean * self.weights["euclidean"]
            + normalized_manhattan * self.weights["manhattan"]
        )

        result = EmbeddingResult(
            product1_description=features1.original_description,
            product2_description=features2.original_description,
            cosine_similarity=cosine_sim,
            euclidean_distance=euclidean_dist,
            manhattan_distance=manhattan_dist,
            normalized_cosine=cosine_sim,
            normalized_euclidean=normalized_euclidean,
            normalized_manhattan=normalized_manhattan,
            final_score=final_score,
        )

        self.logger.debug(f"Embedding similarity result: {final_score:.3f}")
        return result

    def calculate_batch_similarity(
        self, features_list: List[ProductFeatures], threshold: float = 0.5
    ) -> List[EmbeddingResult]:
        """
        Calculate similarity for all pairs in a batch.

        Args:
            features_list: List of product features
            threshold: Minimum similarity threshold

        Returns:
            List of EmbeddingResult for pairs above threshold
        """
        if len(features_list) < 2:
            return []

        # Get all embeddings at once
        descriptions = [features.original_description for features in features_list]
        embeddings = self.get_embeddings_batch(descriptions)

        results = []

        # Calculate similarity for all pairs
        for i in range(len(features_list)):
            for j in range(i + 1, len(features_list)):
                embedding1 = embeddings[i]
                embedding2 = embeddings[j]

                # Calculate similarity
                cosine_sim = self._cosine_similarity(embedding1, embedding2)
                euclidean_dist = self._euclidean_distance(embedding1, embedding2)
                manhattan_dist = self._manhattan_distance(embedding1, embedding2)

                # Normalize distances
                normalized_euclidean = self._normalize_distance(
                    euclidean_dist, max_distance=2.0
                )
                normalized_manhattan = self._normalize_distance(
                    manhattan_dist, max_distance=4.0
                )

                # Calculate final score
                final_score = (
                    cosine_sim * self.weights["cosine"]
                    + normalized_euclidean * self.weights["euclidean"]
                    + normalized_manhattan * self.weights["manhattan"]
                )

                # Only include results above threshold
                if final_score >= threshold:
                    result = EmbeddingResult(
                        product1_description=features_list[i].original_description,
                        product2_description=features_list[j].original_description,
                        cosine_similarity=cosine_sim,
                        euclidean_distance=euclidean_dist,
                        manhattan_distance=manhattan_dist,
                        normalized_cosine=cosine_sim,
                        normalized_euclidean=normalized_euclidean,
                        normalized_manhattan=normalized_manhattan,
                        final_score=final_score,
                    )
                    results.append(result)

        return results

    def find_similar_products(
        self,
        target_features: ProductFeatures,
        candidate_features: List[ProductFeatures],
        top_k: int = 10,
        threshold: float = 0.5,
    ) -> List[Tuple[ProductFeatures, float]]:
        """
        Find most similar products to a target product.

        Args:
            target_features: Target product features
            candidate_features: List of candidate products
            top_k: Number of top results to return
            threshold: Minimum similarity threshold

        Returns:
            List of (ProductFeatures, similarity_score) tuples
        """
        if not candidate_features:
            return []

        # Get embeddings
        target_embedding = self.get_embedding(target_features.original_description)
        candidate_descriptions = [f.original_description for f in candidate_features]
        candidate_embeddings = self.get_embeddings_batch(candidate_descriptions)

        similarities = []

        # Calculate similarity with all candidates
        for i, candidate_embedding in enumerate(candidate_embeddings):
            cosine_sim = self._cosine_similarity(target_embedding, candidate_embedding)
            euclidean_dist = self._euclidean_distance(
                target_embedding, candidate_embedding
            )
            manhattan_dist = self._manhattan_distance(
                target_embedding, candidate_embedding
            )

            # Normalize distances
            normalized_euclidean = self._normalize_distance(
                euclidean_dist, max_distance=2.0
            )
            normalized_manhattan = self._normalize_distance(
                manhattan_dist, max_distance=4.0
            )

            # Calculate final score
            final_score = (
                cosine_sim * self.weights["cosine"]
                + normalized_euclidean * self.weights["euclidean"]
                + normalized_manhattan * self.weights["manhattan"]
            )

            if final_score >= threshold:
                similarities.append((candidate_features[i], final_score))

        # Sort by similarity score (descending) and return top_k
        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_k]

    def _cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """Calculate cosine similarity between two vectors"""
        if np.all(vec1 == 0) and np.all(vec2 == 0):
            return 1.0

        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return dot_product / (norm1 * norm2)

    def _euclidean_distance(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """Calculate Euclidean distance between two vectors"""
        return np.linalg.norm(vec1 - vec2)

    def _manhattan_distance(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """Calculate Manhattan distance between two vectors"""
        return np.sum(np.abs(vec1 - vec2))

    def _normalize_distance(self, distance: float, max_distance: float) -> float:
        """Normalize distance to similarity score (0-1)"""
        if distance >= max_distance:
            return 0.0
        return 1.0 - (distance / max_distance)

    def get_model_info(self) -> Dict:
        """Get information about the current model"""
        return {
            "model_name": self.model_name,
            "embedding_dimension": 512,  # Default for distiluse-base-multilingual
            "weights": self.weights,
            "cache_enabled": self.cache_enabled,
        }
