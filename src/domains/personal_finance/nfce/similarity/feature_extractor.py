#!/usr/bin/env python3
"""Feature Extractor - Extract meaningful features from normalized product descriptions"""

import re
from dataclasses import dataclass

from utils.logging.logging_manager import LogManager

from .product_normalizer import ProductNormalizer


@dataclass
class ProductFeatures:
    """Data class to hold extracted product features"""

    # Core identifiers
    normalized_description: str
    original_description: str

    # Text features
    tokens: list[str]
    bigrams: list[str]
    trigrams: list[str]

    # Semantic features
    brand: str | None
    product_type: str | None
    category: str
    variant: str | None

    # Structural features
    word_count: int
    char_count: int
    first_word: str | None
    last_word: str | None

    # Advanced features
    numeric_tokens: list[str]
    alphabetic_tokens: list[str]
    mixed_tokens: list[str]

    # Similarity keys for comparison
    core_key: str  # Most important words for matching
    variant_key: str  # Secondary words for variant detection

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "normalized_description": self.normalized_description,
            "original_description": self.original_description,
            "tokens": self.tokens,
            "bigrams": self.bigrams,
            "trigrams": self.trigrams,
            "brand": self.brand,
            "product_type": self.product_type,
            "category": self.category,
            "variant": self.variant,
            "word_count": self.word_count,
            "char_count": self.char_count,
            "first_word": self.first_word,
            "last_word": self.last_word,
            "numeric_tokens": self.numeric_tokens,
            "alphabetic_tokens": self.alphabetic_tokens,
            "mixed_tokens": self.mixed_tokens,
            "core_key": self.core_key,
            "variant_key": self.variant_key,
        }


class FeatureExtractor:
    """Extract comprehensive features from normalized product descriptions.

    This class works with ProductNormalizer to create rich feature sets
    that enable sophisticated product matching and similarity detection.
    """

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("FeatureExtractor")
        self.normalizer = ProductNormalizer()

        # Stop words that don't contribute to similarity
        self.stop_words = {
            "COM",
            "SEM",
            "E",
            "OU",
            "DO",
            "DA",
            "DE",
            "NO",
            "NA",
            "EM",
            "PARA",
            "POR",
            "ATE",
            "ATÉ",
            "TIPO",
            "MARCA",
            "TAMANHO",
        }

        # Words that indicate product variants/attributes
        self.variant_indicators = {
            # Flavors
            "SABOR",
            "SABORES",
            "CHOCOLATE",
            "BAUNILHA",
            "MORANGO",
            "UVA",
            "LARANJA",
            "LIMAO",
            "LIMÃO",
            "MENTA",
            "NATURAL",
            # Types/Styles
            "LIGHT",
            "DIET",
            "ZERO",
            "INTEGRAL",
            "NORMAL",
            "TRADICIONAL",
            "ESPECIAL",
            "PREMIUM",
            "CLASSICO",
            "CLÁSSICO",
            # Colors/Visual
            "BRANCO",
            "PRETO",
            "AZUL",
            "VERDE",
            "VERMELHO",
            "AMARELO",
            "DOURADO",
            "CLARO",
            "ESCURO",
            # Textures/Consistency
            "CREMOSO",
            "LIQUIDO",
            "LÍQUIDO",
            "SOLIDO",
            "SÓLIDO",
            "POWDER",
            "CRISTAL",
            "GRANULADO",
            "FINO",
            "GROSSO",
        }

        # Core product indicators (brand-independent)
        self.core_indicators = {
            # Main products
            "ARROZ",
            "FEIJAO",
            "FEIJÃO",
            "AÇUCAR",
            "ACUCAR",
            "SAL",
            "CAFE",
            "CAFÉ",
            "LEITE",
            "OVO",
            "OVOS",
            "FARINHA",
            "MACARRAO",
            "MACARRÃO",
            "MASSA",
            "REFRIGERANTE",
            "SUCO",
            "AGUA",
            "ÁGUA",
            "CERVEJA",
            "BANANA",
            "MACA",
            "MAÇÃ",
            "LARANJA",
            "UVA",
            "LIMAO",
            "LIMÃO",
            "CARNE",
            "FRANGO",
            "PEIXE",
            "PORCO",
            "LINGUIÇA",
            "LINGUICA",
            "PAO",
            "PÃO",
            "BISCOITO",
            "BOLO",
            "TORRADA",
            "QUEIJO",
            "IOGURTE",
            "MANTEIGA",
            "MARGARINA",
            "OLEO",
            "ÓLEO",
            "VINAGRE",
            "MOLHO",
            "TEMPERO",
            "CONDIMENTO",
            "SABAO",
            "SABÃO",
            "DETERGENTE",
            "AMACIANTE",
            "DESINFETANTE",
        }

    def extract(self, description: str) -> ProductFeatures:
        """Extract comprehensive features from a product description.

        Args:
            description: Raw product description

        Returns:
            ProductFeatures object with all extracted features
        """
        if not description:
            raise ValueError("Description cannot be empty")

        self.logger.debug(f"Extracting features from: '{description}'")

        # Step 1: Normalize the description
        normalized = self.normalizer.normalize(description)

        # Step 2: Extract basic text features
        tokens = self._extract_tokens(normalized)
        bigrams = self._extract_bigrams(tokens)
        trigrams = self._extract_trigrams(tokens)

        # Step 3: Extract semantic features using normalizer
        basic_features = self.normalizer.extract_features(description)

        # Safe type conversion for features
        brand = basic_features.get("brand")
        brand = str(brand) if brand is not None and brand != 0 else None

        product_type = basic_features.get("product_type")
        product_type = str(product_type) if product_type is not None and product_type != 0 else None

        category = basic_features.get("category", "other")
        category = str(category) if category is not None and category != 0 else "other"

        variant = basic_features.get("variant")
        variant = str(variant) if variant is not None and variant != 0 else None

        # Step 4: Extract advanced structural features
        numeric_tokens = self._extract_numeric_tokens(tokens)
        alphabetic_tokens = self._extract_alphabetic_tokens(tokens)
        mixed_tokens = self._extract_mixed_tokens(tokens)

        # Step 5: Generate similarity keys
        core_key = self._generate_core_key(tokens)
        variant_key = self._generate_variant_key(tokens)

        # Step 6: Create features object
        features = ProductFeatures(
            normalized_description=normalized,
            original_description=description,
            tokens=tokens,
            bigrams=bigrams,
            trigrams=trigrams,
            brand=brand,
            product_type=product_type,
            category=category,
            variant=variant,
            word_count=len(tokens),
            char_count=len(normalized),
            first_word=tokens[0] if tokens else None,
            last_word=tokens[-1] if tokens else None,
            numeric_tokens=numeric_tokens,
            alphabetic_tokens=alphabetic_tokens,
            mixed_tokens=mixed_tokens,
            core_key=core_key,
            variant_key=variant_key,
        )

        self.logger.debug(f"Extracted features: {features.core_key}")
        return features

    def _extract_tokens(self, normalized_text: str) -> list[str]:
        """Extract individual tokens from normalized text"""
        if not normalized_text:
            return []

        # Split by whitespace and filter empty strings
        tokens = [token.strip() for token in normalized_text.split() if token.strip()]

        # Remove stop words
        tokens = [token for token in tokens if token not in self.stop_words]

        return tokens

    def _extract_bigrams(self, tokens: list[str]) -> list[str]:
        """Extract bigrams (2-word combinations) from tokens"""
        if len(tokens) < 2:
            return []

        bigrams = []
        for i in range(len(tokens) - 1):
            bigram = f"{tokens[i]} {tokens[i + 1]}"
            bigrams.append(bigram)

        return bigrams

    def _extract_trigrams(self, tokens: list[str]) -> list[str]:
        """Extract trigrams (3-word combinations) from tokens"""
        if len(tokens) < 3:
            return []

        trigrams = []
        for i in range(len(tokens) - 2):
            trigram = f"{tokens[i]} {tokens[i + 1]} {tokens[i + 2]}"
            trigrams.append(trigram)

        return trigrams

    def _extract_numeric_tokens(self, tokens: list[str]) -> list[str]:
        """Extract tokens that contain only numbers"""
        numeric_pattern = r"^\d+$"
        return [token for token in tokens if re.match(numeric_pattern, token)]

    def _extract_alphabetic_tokens(self, tokens: list[str]) -> list[str]:
        """Extract tokens that contain only letters"""
        alphabetic_pattern = r"^[A-Za-z]+$"
        return [token for token in tokens if re.match(alphabetic_pattern, token)]

    def _extract_mixed_tokens(self, tokens: list[str]) -> list[str]:
        """Extract tokens that contain both letters and numbers"""
        mixed_pattern = r"^.*[A-Za-z].*\d.*$|^.*\d.*[A-Za-z].*$"
        return [token for token in tokens if re.match(mixed_pattern, token)]

    def _generate_core_key(self, tokens: list[str]) -> str:
        """Generate core similarity key using most important words.

        This key focuses on the essential product identification,
        ignoring variants and minor details.
        """
        if not tokens:
            return ""

        # Identify core product words
        core_words = []

        # Add tokens that match core indicators
        for token in tokens:
            if token in self.core_indicators:
                core_words.append(token)

        # If no core indicators found, use first 2-3 tokens
        if not core_words:
            core_words = tokens[: min(3, len(tokens))]

        # Sort for consistent ordering
        core_words.sort()

        return " ".join(core_words)

    def _generate_variant_key(self, tokens: list[str]) -> str:
        """Generate variant key using words that indicate product variants.

        This key captures flavor, style, color, and other attributes
        that differentiate product variants.
        """
        if not tokens:
            return ""

        # Identify variant words
        variant_words = []

        for token in tokens:
            if token in self.variant_indicators:
                variant_words.append(token)

        # Sort for consistent ordering
        variant_words.sort()

        return " ".join(variant_words)

    def extract_batch(self, descriptions: list[str]) -> list[ProductFeatures]:
        """Extract features from a batch of descriptions.

        Args:
            descriptions: List of product descriptions

        Returns:
            List of ProductFeatures objects
        """
        self.logger.info(f"Extracting features from {len(descriptions)} descriptions")

        features_list = []
        errors = 0

        for i, description in enumerate(descriptions):
            try:
                features = self.extract(description)
                features_list.append(features)

                if (i + 1) % 100 == 0:
                    self.logger.debug(f"Processed {i + 1}/{len(descriptions)} descriptions")

            except Exception as e:
                self.logger.error(f"Error processing description '{description}': {e}")
                errors += 1

        self.logger.info(f"Feature extraction completed. Success: {len(features_list)}, Errors: {errors}")

        return features_list

    def analyze_feature_distribution(self, features_list: list[ProductFeatures]) -> dict:
        """Analyze the distribution of extracted features.

        Args:
            features_list: List of ProductFeatures objects

        Returns:
            Analysis statistics
        """
        if not features_list:
            return {}

        # Collect statistics
        categories = [f.category for f in features_list]
        brands = [f.brand for f in features_list if f.brand]
        word_counts = [f.word_count for f in features_list]
        core_keys = [f.core_key for f in features_list if f.core_key]

        # Calculate distributions
        category_dist = self._calculate_distribution(categories)
        brand_dist = self._calculate_distribution(brands)
        core_key_dist = self._calculate_distribution(core_keys)

        analysis = {
            "total_products": len(features_list),
            "category_distribution": category_dist,
            "brand_distribution": brand_dist,
            "core_key_distribution": core_key_dist,
            "word_count_stats": {
                "min": min(word_counts) if word_counts else 0,
                "max": max(word_counts) if word_counts else 0,
                "avg": sum(word_counts) / len(word_counts) if word_counts else 0,
            },
            "unique_categories": len(set(categories)),
            "unique_brands": len(set(brands)),
            "unique_core_keys": len(set(core_keys)),
        }

        return analysis

    def _calculate_distribution(self, items: list[str]) -> dict[str, int]:
        """Calculate frequency distribution of items"""
        distribution: dict[str, int] = {}
        for item in items:
            distribution[item] = distribution.get(item, 0) + 1

        # Sort by frequency (descending)
        sorted_items = sorted(distribution.items(), key=lambda x: x[1], reverse=True)

        return dict(sorted_items)

    def find_potential_duplicates(
        self, features_list: list[ProductFeatures]
    ) -> list[tuple[ProductFeatures, ProductFeatures, float]]:
        """Find potential duplicate products based on feature similarity.

        Args:
            features_list: List of ProductFeatures objects

        Returns:
            List of tuples (features1, features2, similarity_score)
        """
        self.logger.info(f"Searching for duplicates among {len(features_list)} products")

        duplicates = []
        processed = 0

        for i in range(len(features_list)):
            for j in range(i + 1, len(features_list)):
                features1 = features_list[i]
                features2 = features_list[j]

                # Calculate similarity based on core keys
                similarity = self._calculate_core_similarity(features1, features2)

                # Consider as potential duplicate if similarity > threshold
                if similarity > 0.7:  # Adjustable threshold
                    duplicates.append((features1, features2, similarity))

                processed += 1

                if processed % 10000 == 0:
                    self.logger.debug(f"Processed {processed} comparisons")

        # Sort by similarity score (descending)
        duplicates.sort(key=lambda x: x[2], reverse=True)

        self.logger.info(f"Found {len(duplicates)} potential duplicate pairs")

        return duplicates

    def _calculate_core_similarity(self, features1: ProductFeatures, features2: ProductFeatures) -> float:
        """Calculate similarity between two ProductFeatures based on core keys.

        This is a sophisticated similarity calculation that considers:
        - Core key overlap
        - Category matching
        - Brand matching (optional)
        """
        # If different categories, very low similarity
        if features1.category != features2.category:
            return 0.1

        # Core key similarity (most important)
        core_similarity = self._jaccard_similarity(set(features1.core_key.split()), set(features2.core_key.split()))

        # Brand matching bonus
        brand_bonus = 0.1 if features1.brand == features2.brand and features1.brand is not None else 0

        # Token overlap similarity
        token_similarity = self._jaccard_similarity(set(features1.tokens), set(features2.tokens))

        # Weighted combination
        final_similarity = (
            core_similarity * 0.6  # Core key is most important
            + token_similarity * 0.3  # Token overlap secondary
            + brand_bonus  # Brand matching bonus
        )

        return min(final_similarity, 1.0)  # Cap at 1.0

    def _jaccard_similarity(self, set1: set[str], set2: set[str]) -> float:
        """Calculate Jaccard similarity between two sets"""
        if not set1 and not set2:
            return 1.0

        if not set1 or not set2:
            return 0.0

        intersection = set1.intersection(set2)
        union = set1.union(set2)

        return len(intersection) / len(union)
