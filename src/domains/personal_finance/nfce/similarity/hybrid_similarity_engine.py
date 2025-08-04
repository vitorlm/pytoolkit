#!/usr/bin/env python3
"""
Hybrid Similarity Engine - Combines SBERT embeddings with Brazilian token rules
"""

import math
import re
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import numpy as np
from utils.logging.logging_manager import LogManager
from utils.cache_manager.cache_manager import CacheManager

try:
    from sentence_transformers import SentenceTransformer
    SBERT_AVAILABLE = True
except ImportError:
    SBERT_AVAILABLE = False


@dataclass
class HybridSimilarityResult:
    """Results from hybrid similarity calculation"""
    
    # Core similarity scores
    embedding_similarity: float
    token_rule_similarity: float
    final_similarity: float
    
    # Detailed analysis
    matching_tokens: List[str]
    brazilian_tokens: List[str]
    quantity_matches: List[str]
    brand_similarity: float
    
    # Confidence metrics
    confidence_score: float
    explanation: str


class HybridSimilarityEngine:
    """
    Advanced similarity engine combining:
    - SBERT Portuguese embeddings (semantic similarity)
    - Brazilian product token rules (domain-specific patterns)
    - Quantity and unit matching
    - Brand recognition
    """
    
    def __init__(self, model_name: str = "paraphrase-multilingual-MiniLM-L12-v2", cache_enabled: bool = True):
        """
        Initialize hybrid similarity engine
        
        Args:
            model_name: SBERT model name (default: Legal-BERTimbau for Portuguese)
            cache_enabled: Whether to cache embeddings
        """
        
        self.logger = LogManager.get_instance().get_logger("HybridSimilarityEngine")
        self.cache_enabled = cache_enabled
        
        if cache_enabled:
            self.cache = CacheManager.get_instance()
        
        # Initialize SBERT model
        self._model = None
        self.model_name = model_name
        
        # Expanded Brazilian product token patterns
        self.brazilian_tokens = {
            # Queijo/Cheese patterns (expanded)
            'qjo': ['queijo', 'cheese'],
            'mus': ['mussarela', 'muçarela', 'mozzarella'],
            'fat': ['fatiado', 'fatias', 'sliced'],
            'presid': ['presidente', 'president'],
            'sandw': ['sandwich', 'sanduiche'],
            'ched': ['cheddar'],
            'pol': ['poli', 'polenghi'],
            'gruyere': ['gruyere', 'gruyère'],
            'form': ['formato', 'forma', 'forma'],
            'd': ['de', 'do', 'da'],
            
            # Medication patterns (expanded)
            'cpr': ['comprimido', 'comprimidos', 'tablets', 'comp'],
            'mg': ['miligramas', 'milligrams', 'mgr'],
            'mkg': ['microgramas', 'micrograms', 'mcg'],
            'ml': ['mililitros', 'milliliters'],
            'xpe': ['xarope', 'syrup'],
            'vurtuoso': ['vurtuoso', 'virtuoso'],
            'levotirox': ['levotiroxina', 'levothyroxine'],
            'leucogen': ['leucogen', 'leukogen'],
            's': ['comprimidos', 'tablets', 'units'],
            'c1': ['caixa', 'box', 'pack'],
            
            # Food/Beverage patterns (expanded)
            'refr': ['refrigerante', 'refri', 'soda', 'soft drink'],
            'coca': ['coca-cola', 'coke', 'cocacola'],
            'cola': ['cola'],
            'trad': ['tradicional', 'traditional', 'classico'],
            'bomb': ['bombom', 'chocolate', 'candy'],
            'cerv': ['cerveja', 'beer'],
            'choc': ['chocolate', 'choco'],
            'nest': ['nestle', 'nestlé'],
            'prestigio': ['prestigio', 'prestige'],
            'sonho': ['sonho', 'dream'],
            'valsa': ['valsa', 'waltz'],
            'garoto': ['garoto', 'boy'],
            'heineken': ['heineken'],
            'krug': ['krug'],
            
            # Fruits patterns (expanded)
            'kg': ['quilograma', 'kilogram', 'kilo'],
            'g': ['gramas', 'grams'],
            'pocam': ['pokan', 'ponkan', 'mexerica'],
            'ponkan': ['pocam', 'pokan', 'mexerica'],
            'formosa': ['formoso', 'papaya', 'mamao'],
            'mamao': ['mamão', 'papaya', 'formosa'],
            'banana': ['banana'],
            'prata': ['prata', 'silver'],
            'melancia': ['melancia', 'watermelon'],
            'pin': ['pintado', 'pintada'],
            'do': ['de', 'da', 'do'],
            'mexerica': ['mexerica', 'tangerina', 'pocam', 'ponkan'],
            
            # Eggs and dairy (expanded)
            'ovos': ['ovos', 'ovo', 'eggs'],
            'bcos': ['brancos', 'branco', 'white'],
            'mant': ['mantidos', 'kept', 'mantido'],
            'branco': ['brancos', 'bcos', 'white'],
            'pente': ['pente', 'tray', 'bandeja'],
            
            # Pizza patterns (expanded)
            'pizza': ['pizza'],
            'esp': ['especial', 'special', 'especial'],
            'vm': ['vila madalena', 'gourmet'],
            'gorgonz': ['gorgonzola'],
            'mel': ['mel', 'honey'],
            'gra': ['grande', 'large', 'big'],
            'carne': ['carne', 'meat'],
            'frango': ['frango', 'chicken'],
            'requeijao': ['requeijão', 'cream cheese', 'requeijao'],
            'cremoso': ['cremoso', 'creamy'],
            'sol': ['sol', 'sun'],
            
            # Beverages (expanded)
            'dgusto': ['dgusto', 'degusta', 'taste'],
            'intss': ['intenso', 'intense'],
            'caser': ['caseiro', 'homemade'],
            'doub': ['double', 'duplo'],
            
            # Units and measures (expanded)
            'un': ['unidade', 'unit', 'unid'],
            'unid': ['unidade', 'unit', 'un'],
            'cp': ['comprimidos', 'pills', 'cpr'],
            'l': ['litros', 'liters', 'liter'],
            'ml': ['ml', 'mililitros'],
            '160': ['160g', '160gr', '160 gramas'],
            '250g': ['250gr', '250 gramas'],
            '30': ['30 unidades', '30un', '30 comp'],
            '473ml': ['473 ml', '500ml'],
            '500ml': ['500 ml', '473ml'],
            '120ml': ['120 ml'],
            '200mg': ['200 mg'],
            '88': ['88mg', '88 mg'],
            '100': ['100mg', '100 mg'],
            '10mg': ['10 mg'],
            '20mg': ['20 mg'],
            '60cp': ['60 comprimidos'],
            '1.5l': ['1,5l', '1.5 litros'],
            '600': ['600ml', '600 ml'],
            '3': ['3 litros', '3l'],
            
            # Brand abbreviations (expanded)
            'la': ['lata', 'can'],
            'pin': ['pintado', 'spotted'],
            'doub': ['double', 'duplo', 'dobro']
        }
        
        # Quantity and unit patterns
        self.quantity_patterns = [
            r'\d+\s*mg',
            r'\d+\s*mkg', 
            r'\d+\s*ml',
            r'\d+\s*l',
            r'\d+\s*kg',
            r'\d+\s*g',
            r'\d+\s*cp',
            r'\d+\s*cpr',
            r'\d+\s*un',
            r'\d+\s*unid',
            r'\d+\'\s*s',
            r'\d+\s*c\d+',
            r'\d+\s*/\s*\d+'
        ]
        
        # Weight configuration
        self.weights = {
            'embedding': 0.4,      # Semantic similarity weight
            'token_rules': 0.35,   # Brazilian token rules weight
            'quantity_match': 0.15,  # Quantity matching weight
            'brand_match': 0.1     # Brand matching weight
        }
        
        self.logger.info(f"Initialized HybridSimilarityEngine with model: {model_name}")
    
    @property
    def model(self):
        """Lazy loading of SBERT model"""
        if self._model is None:
            if not SBERT_AVAILABLE:
                self.logger.warning("sentence-transformers not available, using fallback")
                return None
                
            try:
                self.logger.info(f"Loading SBERT model: {self.model_name}")
                self._model = SentenceTransformer(self.model_name)
                self.logger.info("SBERT model loaded successfully")
            except Exception as e:
                self.logger.error(f"Failed to load SBERT model: {e}")
                # Fallback to a smaller model
                try:
                    self.logger.info("Trying fallback model: paraphrase-multilingual-MiniLM-L12-v2")
                    self._model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
                except Exception as e2:
                    self.logger.error(f"Failed to load fallback model: {e2}")
                    return None
        
        return self._model
    
    def calculate_similarity(self, product1: str, product2: str) -> HybridSimilarityResult:
        """
        Calculate hybrid similarity between two products
        
        Args:
            product1: First product description
            product2: Second product description
            
        Returns:
            HybridSimilarityResult with detailed analysis
        """
        
        self.logger.debug(f"Calculating hybrid similarity: '{product1}' vs '{product2}'")
        
        # Normalize products
        norm1 = self._normalize_product(product1)
        norm2 = self._normalize_product(product2)
        
        # Calculate embedding similarity
        embedding_sim = self._calculate_embedding_similarity(norm1, norm2)
        
        # Calculate token rule similarity
        token_sim, matching_tokens, brazilian_tokens = self._calculate_token_similarity(norm1, norm2)
        
        # Calculate quantity matching
        quantity_sim, quantity_matches = self._calculate_quantity_similarity(product1, product2)
        
        # Calculate brand similarity
        brand_sim = self._calculate_brand_similarity(norm1, norm2)
        
        # Calculate final weighted similarity
        final_sim = (
            embedding_sim * self.weights['embedding'] +
            token_sim * self.weights['token_rules'] +
            quantity_sim * self.weights['quantity_match'] +
            brand_sim * self.weights['brand_match']
        )
        
        # Calculate confidence score
        confidence = self._calculate_confidence(embedding_sim, token_sim, quantity_sim, brand_sim)
        
        # Generate explanation
        explanation = self._generate_explanation(
            embedding_sim, token_sim, quantity_sim, brand_sim, 
            matching_tokens, brazilian_tokens, quantity_matches
        )
        
        result = HybridSimilarityResult(
            embedding_similarity=embedding_sim,
            token_rule_similarity=token_sim,
            final_similarity=final_sim,
            matching_tokens=matching_tokens,
            brazilian_tokens=brazilian_tokens,
            quantity_matches=quantity_matches,
            brand_similarity=brand_sim,
            confidence_score=confidence,
            explanation=explanation
        )
        
        self.logger.debug(f"Hybrid similarity result: {final_sim:.3f} (confidence: {confidence:.3f})")
        
        return result
    
    def _normalize_product(self, product: str) -> str:
        """Normalize product description for processing"""
        if not product:
            return ""
        
        # Convert to lowercase
        normalized = product.lower().strip()
        
        # Remove extra spaces
        normalized = re.sub(r'\s+', ' ', normalized)
        
        # Remove special characters but keep important ones
        normalized = re.sub(r'[^\w\s\d\'\"/\\.-]', ' ', normalized)
        
        return normalized
    
    def _calculate_embedding_similarity(self, product1: str, product2: str) -> float:
        """Calculate semantic similarity using SBERT embeddings"""
        
        if not self.model or not product1 or not product2:
            return 0.0
        
        try:
            # Check cache first
            cache_key = f"embedding_{hash(product1)}_{hash(product2)}"
            if self.cache_enabled:
                cached_result = self.cache.load(cache_key, expiration_minutes=60)
                if cached_result is not None:
                    return cached_result
            
            # Calculate embeddings
            embeddings = self.model.encode([product1, product2])
            
            # Calculate cosine similarity
            embedding1, embedding2 = embeddings[0], embeddings[1]
            
            # Normalize vectors
            norm1 = np.linalg.norm(embedding1)
            norm2 = np.linalg.norm(embedding2)
            
            if norm1 == 0 or norm2 == 0:
                similarity = 0.0
            else:
                similarity = np.dot(embedding1, embedding2) / (norm1 * norm2)
                # Ensure similarity is between 0 and 1
                similarity = max(0.0, min(1.0, float(similarity)))
            
            # Cache result
            if self.cache_enabled:
                self.cache.save(cache_key, similarity)
            
            return similarity
            
        except Exception as e:
            self.logger.error(f"Error calculating embedding similarity: {e}")
            return 0.0
    
    def _calculate_token_similarity(self, product1: str, product2: str) -> Tuple[float, List[str], List[str]]:
        """Calculate similarity based on Brazilian token rules"""
        
        if not product1 or not product2:
            return 0.0, [], []
        
        tokens1 = set(product1.split())
        tokens2 = set(product2.split())
        
        matching_tokens = []
        brazilian_tokens = []
        
        # Direct token matching
        direct_matches = tokens1.intersection(tokens2)
        matching_tokens.extend(list(direct_matches))
        
        # Brazilian token pattern matching
        rule_matches = 0
        total_rules = 0
        
        for token1 in tokens1:
            for token2 in tokens2:
                # Check if tokens match Brazilian patterns
                match_found = False
                
                for abbrev, full_forms in self.brazilian_tokens.items():
                    # Check if one token is abbreviation and other is full form
                    if ((token1 == abbrev and any(form in token2 for form in full_forms)) or
                        (token2 == abbrev and any(form in token1 for form in full_forms)) or
                        any(form in token1 for form in full_forms) and any(form in token2 for form in full_forms)):
                        
                        rule_matches += 1
                        brazilian_tokens.append(f"{token1}↔{token2}")
                        match_found = True
                        break
                
                total_rules += 1
                if match_found:
                    break
        
        # Calculate token similarity score
        if len(tokens1.union(tokens2)) == 0:
            token_similarity = 1.0
        else:
            jaccard_sim = len(direct_matches) / len(tokens1.union(tokens2))
            rule_sim = rule_matches / max(total_rules, 1) if total_rules > 0 else 0
            token_similarity = (jaccard_sim * 0.6) + (rule_sim * 0.4)
        
        return token_similarity, matching_tokens, brazilian_tokens
    
    def _calculate_quantity_similarity(self, product1: str, product2: str) -> Tuple[float, List[str]]:
        """Calculate similarity based on quantities and units"""
        
        quantities1 = self._extract_quantities(product1)
        quantities2 = self._extract_quantities(product2)
        
        if not quantities1 and not quantities2:
            return 1.0, []  # Both have no quantities, consider similar
        
        if not quantities1 or not quantities2:
            return 0.0, []  # One has quantities, other doesn't
        
        # Find matching quantities
        matches = []
        for q1 in quantities1:
            for q2 in quantities2:
                if self._quantities_match(q1, q2):
                    matches.append(f"{q1}≈{q2}")
        
        # Calculate similarity based on matching ratio
        total_quantities = len(set(quantities1 + quantities2))
        similarity = len(matches) / total_quantities if total_quantities > 0 else 0.0
        
        return similarity, matches
    
    def _extract_quantities(self, product: str) -> List[str]:
        """Extract quantities and units from product description"""
        quantities = []
        
        for pattern in self.quantity_patterns:
            matches = re.findall(pattern, product.lower())
            quantities.extend(matches)
        
        return quantities
    
    def _quantities_match(self, q1: str, q2: str) -> bool:
        """Check if two quantities are equivalent"""
        # Normalize quantities for comparison
        q1_norm = re.sub(r'\s+', '', q1.lower())
        q2_norm = re.sub(r'\s+', '', q2.lower())
        
        # Direct match
        if q1_norm == q2_norm:
            return True
        
        # Extract numbers and units
        q1_match = re.match(r'(\d+)\s*([a-z\']+)', q1_norm)
        q2_match = re.match(r'(\d+)\s*([a-z\']+)', q2_norm)
        
        if q1_match and q2_match:
            num1, unit1 = q1_match.groups()
            num2, unit2 = q2_match.groups()
            
            # Same unit, check if numbers are close
            if unit1 == unit2:
                return abs(int(num1) - int(num2)) <= 2  # Allow small differences
        
        return False
    
    def _calculate_brand_similarity(self, product1: str, product2: str) -> float:
        """Calculate brand similarity"""
        
        # Extract potential brand names (capitalized words, common brands)
        brands1 = self._extract_brands(product1)
        brands2 = self._extract_brands(product2)
        
        if not brands1 and not brands2:
            return 1.0  # No brands in either
        
        if not brands1 or not brands2:
            return 0.0  # One has brand, other doesn't
        
        # Calculate brand matching
        matches = len(set(brands1).intersection(set(brands2)))
        total = len(set(brands1 + brands2))
        
        return matches / total if total > 0 else 0.0
    
    def _extract_brands(self, product: str) -> List[str]:
        """Extract brand names from product description"""
        
        common_brands = [
            'coca', 'cola', 'nestle', 'nest', 'garoto', 'heineken', 
            'presidente', 'polenghi', 'vila', 'madalena', 'prestigio',
            'sonho', 'valsa', 'krug', 'dgusto'
        ]
        
        brands = []
        product_lower = product.lower()
        
        for brand in common_brands:
            if brand in product_lower:
                brands.append(brand)
        
        return brands
    
    def _calculate_confidence(self, embedding_sim: float, token_sim: float, 
                            quantity_sim: float, brand_sim: float) -> float:
        """Calculate confidence score for the similarity result"""
        
        # Higher confidence when multiple methods agree
        similarities = [embedding_sim, token_sim, quantity_sim, brand_sim]
        
        # Calculate standard deviation (lower = more agreement)
        mean_sim = sum(similarities) / len(similarities)
        variance = sum((s - mean_sim) ** 2 for s in similarities) / len(similarities)
        std_dev = math.sqrt(variance)
        
        # Confidence is higher when std deviation is lower and mean is higher
        confidence = mean_sim * (1 - std_dev)
        
        return max(0.0, min(1.0, confidence))
    
    def _generate_explanation(self, embedding_sim: float, token_sim: float, 
                            quantity_sim: float, brand_sim: float,
                            matching_tokens: List[str], brazilian_tokens: List[str],
                            quantity_matches: List[str]) -> str:
        """Generate human-readable explanation of similarity"""
        
        explanations = []
        
        if embedding_sim > 0.7:
            explanations.append(f"Alta similaridade semântica ({embedding_sim:.2f})")
        elif embedding_sim > 0.4:
            explanations.append(f"Similaridade semântica moderada ({embedding_sim:.2f})")
        
        if token_sim > 0.5:
            explanations.append(f"Tokens similares: {', '.join(matching_tokens[:3])}")
        
        if brazilian_tokens:
            explanations.append(f"Padrões brasileiros: {', '.join(brazilian_tokens[:2])}")
        
        if quantity_matches:
            explanations.append(f"Quantidades similares: {', '.join(quantity_matches)}")
        
        if brand_sim > 0.5:
            explanations.append(f"Marcas similares ({brand_sim:.2f})")
        
        return "; ".join(explanations) if explanations else "Baixa similaridade geral"