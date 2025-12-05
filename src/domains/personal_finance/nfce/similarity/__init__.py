#!/usr/bin/env python3
"""Similarity Module - Product similarity detection and matching"""

from .embedding_similarity import EmbeddingResult, EmbeddingSimilarity
from .feature_extractor import FeatureExtractor, ProductFeatures
from .product_matcher import MatchGroup, MatchingResults, ProductMatcher
from .product_normalizer import ProductNormalizer
from .similarity_calculator import SimilarityCalculator, SimilarityResult

__all__ = [
    "EmbeddingResult",
    "EmbeddingSimilarity",
    "FeatureExtractor",
    "MatchGroup",
    "MatchingResults",
    "ProductFeatures",
    "ProductMatcher",
    "ProductNormalizer",
    "SimilarityCalculator",
    "SimilarityResult",
]

# Version info
__version__ = "1.0.0"
__author__ = "PyToolkit"
__description__ = "Advanced product similarity detection and matching engine"
