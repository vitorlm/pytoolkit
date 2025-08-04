#!/usr/bin/env python3
"""
Similarity Module - Product similarity detection and matching
"""

from .product_normalizer import ProductNormalizer
from .feature_extractor import FeatureExtractor, ProductFeatures
from .similarity_calculator import SimilarityCalculator, SimilarityResult
from .product_matcher import ProductMatcher, MatchGroup, MatchingResults
from .embedding_similarity import EmbeddingSimilarity, EmbeddingResult

__all__ = [
    "ProductNormalizer",
    "FeatureExtractor",
    "ProductFeatures",
    "SimilarityCalculator",
    "SimilarityResult",
    "ProductMatcher",
    "MatchGroup",
    "MatchingResults",
    "EmbeddingSimilarity",
    "EmbeddingResult",
]

# Version info
__version__ = "1.0.0"
__author__ = "PyToolkit"
__description__ = "Advanced product similarity detection and matching engine"
