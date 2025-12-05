#!/usr/bin/env python3
"""Advanced Embedding Engine - Multi-model embedding system optimized for Brazilian Portuguese products"""

from dataclasses import dataclass

import numpy as np

from utils.cache_manager.cache_manager import CacheManager
from utils.logging.logging_manager import LogManager


@dataclass
class EmbeddingConfig:
    """Configuration for embedding models"""

    primary_model: str = "neuralmind/bert-base-portuguese-cased"  # BERTimbau for Portuguese
    secondary_model: str = "intfloat/multilingual-e5-large"  # E5 for multilingual support
    fallback_model: str = "distiluse-base-multilingual-cased-v2"  # Original fallback
    cache_ttl: int = 86400  # 24 hours
    ensemble_weights: dict[str, float] = None

    def __post_init__(self):
        if self.ensemble_weights is None:
            self.ensemble_weights = {
                "portuguese": 0.6,  # Primary Portuguese model weight
                "multilingual": 0.3,  # Secondary multilingual model weight
                "fallback": 0.1,  # Fallback model weight
            }


@dataclass
class EmbeddingResult:
    """Enhanced embedding result with multiple representations"""

    product_description: str
    primary_embedding: np.ndarray
    secondary_embedding: np.ndarray
    ensemble_embedding: np.ndarray
    confidence_score: float
    model_used: str
    processing_time: float


class AdvancedEmbeddingEngine:
    """Advanced embedding engine using multiple Portuguese-optimized models

    Features:
    - Portuguese-specific BERTimbau model as primary
    - Multilingual E5 model as secondary
    - Ensemble embedding combination
    - Intelligent fallback strategy
    - Performance monitoring and caching
    """

    def __init__(self, config: EmbeddingConfig | None = None):
        self.logger = LogManager.get_instance().get_logger("AdvancedEmbeddingEngine")
        self.config = config or EmbeddingConfig()
        self.cache = CacheManager.get_instance()

        # Model instances (lazy loaded)
        self._primary_model = None
        self._secondary_model = None
        self._fallback_model = None

        # Performance tracking
        self.performance_stats = {
            "total_embeddings": 0,
            "cache_hits": 0,
            "model_usage": {"primary": 0, "secondary": 0, "fallback": 0},
            "avg_processing_time": 0.0,
        }

        self.logger.info("Advanced Embedding Engine initialized")

    @property
    def primary_model(self):
        """Lazy load Portuguese-specific model"""
        if self._primary_model is None:
            try:
                from sentence_transformers import SentenceTransformer

                self.logger.info(f"Loading primary Portuguese model: {self.config.primary_model}")
                self._primary_model = SentenceTransformer(self.config.primary_model)
                self.logger.info("Primary Portuguese model loaded successfully")
            except Exception as e:
                self.logger.warning(f"Failed to load primary model: {e}")
                self._primary_model = False  # Mark as failed
        return self._primary_model if self._primary_model is not False else None

    @property
    def secondary_model(self):
        """Lazy load multilingual E5 model"""
        if self._secondary_model is None:
            try:
                from sentence_transformers import SentenceTransformer

                self.logger.info(f"Loading secondary multilingual model: {self.config.secondary_model}")
                self._secondary_model = SentenceTransformer(self.config.secondary_model)
                self.logger.info("Secondary multilingual model loaded successfully")
            except Exception as e:
                self.logger.warning(f"Failed to load secondary model: {e}")
                self._secondary_model = False
        return self._secondary_model if self._secondary_model is not False else None

    @property
    def fallback_model(self):
        """Lazy load fallback model"""
        if self._fallback_model is None:
            try:
                from sentence_transformers import SentenceTransformer

                self.logger.info(f"Loading fallback model: {self.config.fallback_model}")
                self._fallback_model = SentenceTransformer(self.config.fallback_model)
                self.logger.info("Fallback model loaded successfully")
            except Exception as e:
                self.logger.error(f"Failed to load fallback model: {e}")
                self._fallback_model = False
        return self._fallback_model if self._fallback_model is not False else None

    def get_embedding(self, text: str, use_ensemble: bool = True) -> EmbeddingResult:
        """Get advanced embedding for text using ensemble approach

        Args:
            text: Input text to embed
            use_ensemble: Whether to use ensemble of multiple models

        Returns:
            EmbeddingResult with multiple embedding representations
        """
        import time

        start_time = time.time()

        if not text or not text.strip():
            return self._create_zero_embedding_result(text, start_time)

        # Check cache first
        cache_key = f"advanced_embedding:{hash(text)}:{use_ensemble}"
        cached_result = self.cache.load(cache_key, expiration_minutes=self.config.cache_ttl // 60)

        if cached_result is not None:
            self.performance_stats["cache_hits"] += 1
            return self._dict_to_embedding_result(cached_result)

        # Get embeddings from available models
        primary_emb = self._get_model_embedding(text, "primary")
        secondary_emb = self._get_model_embedding(text, "secondary")

        # Determine best available embeddings
        if primary_emb is not None and secondary_emb is not None:
            # Use ensemble if both models available
            ensemble_emb = self._create_ensemble_embedding(primary_emb, secondary_emb)
            confidence = 0.95
            model_used = "ensemble"
        elif primary_emb is not None:
            # Use primary only
            ensemble_emb = primary_emb
            secondary_emb = primary_emb.copy()  # Duplicate for consistency
            confidence = 0.85
            model_used = "primary"
        elif secondary_emb is not None:
            # Use secondary only
            ensemble_emb = secondary_emb
            primary_emb = secondary_emb.copy()
            confidence = 0.75
            model_used = "secondary"
        else:
            # Fallback to basic model
            fallback_emb = self._get_model_embedding(text, "fallback")
            if fallback_emb is not None:
                primary_emb = secondary_emb = ensemble_emb = fallback_emb
                confidence = 0.60
                model_used = "fallback"
            else:
                return self._create_zero_embedding_result(text, start_time)

        processing_time = time.time() - start_time

        result = EmbeddingResult(
            product_description=text,
            primary_embedding=primary_emb,
            secondary_embedding=secondary_emb,
            ensemble_embedding=ensemble_emb,
            confidence_score=confidence,
            model_used=model_used,
            processing_time=processing_time,
        )

        # Cache result
        self.cache.save(cache_key, self._embedding_result_to_dict(result))

        # Update performance stats
        self._update_performance_stats(model_used, processing_time)

        return result

    def get_embeddings_batch(self, texts: list[str], use_ensemble: bool = True) -> list[EmbeddingResult]:
        """Get embeddings for batch of texts (more efficient)

        Args:
            texts: List of texts to embed
            use_ensemble: Whether to use ensemble approach

        Returns:
            List of EmbeddingResult objects
        """
        if not texts:
            return []

        self.logger.info(f"Processing batch of {len(texts)} texts")

        results = []
        uncached_texts = []
        uncached_indices = []

        # Check cache for all texts
        for i, text in enumerate(texts):
            cache_key = f"advanced_embedding:{hash(text)}:{use_ensemble}"
            cached_result = self.cache.load(cache_key, expiration_minutes=self.config.cache_ttl // 60)

            if cached_result is not None:
                results.append(self._dict_to_embedding_result(cached_result))
                self.performance_stats["cache_hits"] += 1
            else:
                results.append(None)  # Placeholder
                uncached_texts.append(text)
                uncached_indices.append(i)

        # Process uncached texts
        if uncached_texts:
            self.logger.info(f"Processing {len(uncached_texts)} uncached texts")

            # Get batch embeddings from available models
            primary_batch = self._get_batch_embeddings(uncached_texts, "primary")
            secondary_batch = self._get_batch_embeddings(uncached_texts, "secondary")

            # Process each uncached text
            for j, (text, orig_idx) in enumerate(zip(uncached_texts, uncached_indices)):
                primary_emb = primary_batch[j] if primary_batch else None
                secondary_emb = secondary_batch[j] if secondary_batch else None

                # Create ensemble or fallback
                if primary_emb is not None and secondary_emb is not None:
                    ensemble_emb = self._create_ensemble_embedding(primary_emb, secondary_emb)
                    confidence = 0.95
                    model_used = "ensemble"
                elif primary_emb is not None:
                    ensemble_emb = primary_emb
                    secondary_emb = primary_emb.copy()
                    confidence = 0.85
                    model_used = "primary"
                elif secondary_emb is not None:
                    ensemble_emb = secondary_emb
                    primary_emb = secondary_emb.copy()
                    confidence = 0.75
                    model_used = "secondary"
                else:
                    # Individual fallback
                    fallback_emb = self._get_model_embedding(text, "fallback")
                    if fallback_emb is not None:
                        primary_emb = secondary_emb = ensemble_emb = fallback_emb
                        confidence = 0.60
                        model_used = "fallback"
                    else:
                        primary_emb = secondary_emb = ensemble_emb = np.zeros(768)
                        confidence = 0.0
                        model_used = "zero"

                result = EmbeddingResult(
                    product_description=text,
                    primary_embedding=primary_emb,
                    secondary_embedding=secondary_emb,
                    ensemble_embedding=ensemble_emb,
                    confidence_score=confidence,
                    model_used=model_used,
                    processing_time=0.0,  # Batch processing time not individual
                )

                # Cache and store result
                cache_key = f"advanced_embedding:{hash(text)}:{use_ensemble}"
                self.cache.save(cache_key, self._embedding_result_to_dict(result))
                results[orig_idx] = result

                # Update stats
                self._update_performance_stats(model_used, 0.0)

        self.logger.info(f"Batch processing completed: {len(results)} embeddings")
        return results

    def _get_model_embedding(self, text: str, model_type: str) -> np.ndarray | None:
        """Get embedding from specific model type"""
        try:
            if model_type == "primary" and self.primary_model:
                # For E5 models, add query prefix
                if "e5" in self.config.primary_model.lower():
                    text = f"query: {text}"
                return self.primary_model.encode(text, convert_to_numpy=True)
            elif model_type == "secondary" and self.secondary_model:
                if "e5" in self.config.secondary_model.lower():
                    text = f"query: {text}"
                return self.secondary_model.encode(text, convert_to_numpy=True)
            elif model_type == "fallback" and self.fallback_model:
                return self.fallback_model.encode(text, convert_to_numpy=True)
        except Exception as e:
            self.logger.error(f"Error getting {model_type} embedding: {e}")

        return None

    def _get_batch_embeddings(self, texts: list[str], model_type: str) -> list[np.ndarray] | None:
        """Get batch embeddings from specific model"""
        try:
            processed_texts = texts.copy()

            if model_type == "primary" and self.primary_model:
                if "e5" in self.config.primary_model.lower():
                    processed_texts = [f"query: {text}" for text in texts]
                embeddings = self.primary_model.encode(processed_texts, convert_to_numpy=True)
                return [emb for emb in embeddings]
            elif model_type == "secondary" and self.secondary_model:
                if "e5" in self.config.secondary_model.lower():
                    processed_texts = [f"query: {text}" for text in texts]
                embeddings = self.secondary_model.encode(processed_texts, convert_to_numpy=True)
                return [emb for emb in embeddings]
            elif model_type == "fallback" and self.fallback_model:
                embeddings = self.fallback_model.encode(processed_texts, convert_to_numpy=True)
                return [emb for emb in embeddings]
        except Exception as e:
            self.logger.error(f"Error getting batch {model_type} embeddings: {e}")

        return None

    def _create_ensemble_embedding(self, emb1: np.ndarray, emb2: np.ndarray) -> np.ndarray:
        """Create ensemble embedding from multiple embeddings"""
        # Ensure embeddings have same dimension (pad/truncate if needed)
        if emb1.shape != emb2.shape:
            min_dim = min(emb1.shape[0], emb2.shape[0])
            emb1 = emb1[:min_dim]
            emb2 = emb2[:min_dim]

        # Weighted average
        ensemble = (
            emb1 * self.config.ensemble_weights["portuguese"] + emb2 * self.config.ensemble_weights["multilingual"]
        )

        # Normalize
        norm = np.linalg.norm(ensemble)
        if norm > 0:
            ensemble = ensemble / norm

        return ensemble

    def _create_zero_embedding_result(self, text: str, start_time: float) -> EmbeddingResult:
        """Create zero embedding result for empty/invalid text"""
        processing_time = time.time() - start_time
        zero_embedding = np.zeros(768)  # Standard embedding size

        return EmbeddingResult(
            product_description=text,
            primary_embedding=zero_embedding,
            secondary_embedding=zero_embedding,
            ensemble_embedding=zero_embedding,
            confidence_score=0.0,
            model_used="zero",
            processing_time=processing_time,
        )

    def _update_performance_stats(self, model_used: str, processing_time: float):
        """Update performance statistics"""
        self.performance_stats["total_embeddings"] += 1

        if model_used in self.performance_stats["model_usage"]:
            self.performance_stats["model_usage"][model_used] += 1

        # Update average processing time
        total = self.performance_stats["total_embeddings"]
        current_avg = self.performance_stats["avg_processing_time"]
        self.performance_stats["avg_processing_time"] = (current_avg * (total - 1) + processing_time) / total

    def _embedding_result_to_dict(self, result: EmbeddingResult) -> dict:
        """Convert EmbeddingResult to dictionary for caching"""
        return {
            "product_description": result.product_description,
            "primary_embedding": result.primary_embedding.tolist(),
            "secondary_embedding": result.secondary_embedding.tolist(),
            "ensemble_embedding": result.ensemble_embedding.tolist(),
            "confidence_score": result.confidence_score,
            "model_used": result.model_used,
            "processing_time": result.processing_time,
        }

    def _dict_to_embedding_result(self, data: dict) -> EmbeddingResult:
        """Convert dictionary back to EmbeddingResult"""
        return EmbeddingResult(
            product_description=data["product_description"],
            primary_embedding=np.array(data["primary_embedding"]),
            secondary_embedding=np.array(data["secondary_embedding"]),
            ensemble_embedding=np.array(data["ensemble_embedding"]),
            confidence_score=data["confidence_score"],
            model_used=data["model_used"],
            processing_time=data["processing_time"],
        )

    def get_performance_stats(self) -> dict:
        """Get performance statistics"""
        stats = self.performance_stats.copy()

        # Calculate additional metrics
        total = stats["total_embeddings"]
        if total > 0:
            stats["cache_hit_rate"] = stats["cache_hits"] / total
            stats["model_distribution"] = {model: count / total for model, count in stats["model_usage"].items()}

        return stats

    def clear_cache(self):
        """Clear embedding cache"""
        # Clear all embedding-related cache entries
        # Note: This is a simplified version - real implementation would need
        # more sophisticated cache key pattern matching
        self.cache.clear_all()
        self.logger.info("Embedding cache cleared")

    def warmup_models(self, sample_texts: list[str] | None = None):
        """Warm up models with sample texts for better initial performance"""
        if sample_texts is None:
            sample_texts = [
                "COCA COLA LATA 350ML",
                "AÇÚCAR CRISTAL UNIÃO 1KG",
                "DETERGENTE YPÊ CLEAR 500ML",
            ]

        self.logger.info("Warming up embedding models...")

        for text in sample_texts:
            self.get_embedding(text)

        self.logger.info("Model warmup completed")
