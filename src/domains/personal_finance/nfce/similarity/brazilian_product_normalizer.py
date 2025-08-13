#!/usr/bin/env python3
"""
Brazilian Product Normalizer - Advanced normalization for Brazilian product names
"""

import re
import unicodedata
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from utils.logging.logging_manager import LogManager
from utils.cache_manager.cache_manager import CacheManager


@dataclass
class NormalizationResult:
    """Result of product normalization"""
    original: str
    normalized: str
    extracted_brand: Optional[str]
    extracted_size: Optional[str]
    extracted_unit: Optional[str]
    category_hints: List[str]
    confidence_score: float
    normalization_steps: List[str]


class BrazilianProductNormalizer:
    """
    Advanced product normalizer specifically designed for Brazilian product names
    
    Features:
    - Brazilian brand recognition
    - Size and unit extraction
    - Category detection
    - Abbreviation expansion
    - Regional variations handling
    - Confidence scoring
    """
    
    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("BrazilianProductNormalizer")
        self.cache = CacheManager.get_instance()
        
        # Brazilian-specific abbreviations and expansions
        self.abbreviations = {
            # Business terms
            'LTDA': 'LIMITADA',
            'CIA': 'COMPANHIA',
            'IND': 'INDUSTRIA',
            'INDL': 'INDUSTRIAL',
            'COM': 'COMERCIAL',
            'DIST': 'DISTRIBUIDORA',
            'PROD': 'PRODUTOS',
            'ALIM': 'ALIMENTOS',
            'BEBID': 'BEBIDAS',
            
            # Product terms
            'UND': 'UNIDADE',
            'UN': 'UNIDADE',
            'PCT': 'PACOTE',
            'PC': 'PACOTE',
            'CX': 'CAIXA',
            'EMB': 'EMBALAGEM',
            'FARD': 'FARDO',
            'DISP': 'DISPLAY',
            'BDJ': 'BANDEJA',
            'SCH': 'SACHÊ',
            'RESH': 'FRESH', 
            'TRAD': 'TRADICIONAL',
            'ORIG': 'ORIGINAL',
            'NATUR': 'NATURAL',
            'INTEGR': 'INTEGRAL',
            'DESC': 'DESCREMADO',
            'SEMIDESN': 'SEMIDESNATADO',
            'ZERO': 'ZERO AÇÚCAR',
            'DIET': 'DIETÉTICO',
            'LIGHT': 'LIGHT',
            
            # Units
            'ML': 'MILILITROS',
            'LT': 'LITROS',
            'L': 'LITROS', 
            'KG': 'QUILOS',
            'G': 'GRAMAS',
            'MG': 'MILIGRAMAS',
            'CM': 'CENTIMETROS',
            'MM': 'MILIMETROS',
            'M': 'METROS',
            
            # Common abbreviations
            'REF': 'REFRIGERANTE',
            'REFRIG': 'REFRIGERANTE',
            'ACHOC': 'ACHOCOLATADO',
            'BISCT': 'BISCOITO',
            'CHOC': 'CHOCOLATE',
            'MARG': 'MARGARINA',
            'MANT': 'MANTEIGA',
            'IOGU': 'IOGURTE',
            'HAMB': 'HAMBÚRGUER',
            'SALC': 'SALSICHA',
            'LING': 'LINGUIÇA',
            'MORT': 'MORTADELA',
            'PRES': 'PRESUNTO',
            'QUEIJ': 'QUEIJO',
            'DETER': 'DETERGENTE',
            'AMAC': 'AMACIANTE',
            'DESINF': 'DESINFETANTE'
        }
        
        # Brazilian brands (common ones)
        self.brazilian_brands = {
            # Foods
            'NESTLE', 'NESTLÉ', 'UNILEVER', 'SADIA', 'PERDIGAO', 'PERDIGÃO', 'SEARA',
            'FRIMESA', 'AURORA', 'MARFRIG', 'MINERVA', 'QUALY', 'DORIANA', 'LIZA',
            'UNIÃO', 'UNIAO', 'CRISTAL', 'REFINADO', 'AÇÚCAR', 'ACUCAR',
            
            # Beverages
            'COCA', 'COLA', 'PEPSI', 'GUARANÁ', 'GUARANA', 'ANTARCTICA', 'BRAHMA',
            'SKOL', 'HEINEKEN', 'AMBEV', 'KUAT', 'SPRITE', 'FANTA', 'SCHWEPPES',
            'H2OH', 'AQUARIUS', 'POWERADE', 'TANG', 'FRESH', 'MAGUARY', 'MAIS',
            'CAMP', 'ÁGUA', 'AGUA', 'CRYSTAL', 'BONAFONT', 'PETRA', 'LINDOYA',
            'PETRÓPOLIS', 'PETROPOLIS',
            
            # Personal care
            'DOVE', 'LUX', 'REXONA', 'AXE', 'CLEAR', 'SEDA', 'PANTENE', 'HEAD',
            'SHOULDERS', 'HERBAL', 'ESSENCES', 'JOHNSON', 'NIVEA', 'OBOTICÁRIO', 'OBOTICARIO',
            
            # Cleaning
            'OMO', 'SURF', 'ARIEL', 'ACE', 'VANISH', 'COMFORT', 'FOFO', 'YPÊ', 'YPE',
            'MINUANO', 'THUS', 'VEJA', 'CIF', 'AJAX', 'BOMBRIL', 'ASSOLAN',
            
            # Dairy
            'NESTLÉ', 'NESTLE', 'DANONE', 'VIGOR', 'ITAMBÉ', 'ITAMBE', 'PARMALAT',
            'PIRACANJUBA', 'BETÂNIA', 'BETANIA', 'FRIMESA', 'ELEGÊ', 'ELEGE',
            'TIROLEZ', 'POLENGHI', 'CATUPIRY'
        }
        
        # Size patterns (Brazilian format)
        self.size_patterns = [
            # Liquid measures
            r'(\d+(?:,\d+)?)\s*(?:ML|MILILITROS|L|LITROS?|LT)\b',
            # Weight measures  
            r'(\d+(?:,\d+)?)\s*(?:G|GRAMAS?|KG|QUILOS?|MG|MILIGRAMAS?)\b',
            # Count measures
            r'(\d+)\s*(?:UN|UND|UNIDADES?|PCT|PACOTES?|CX|CAIXAS?)\b',
            # Combined measures
            r'(\d+)\s*X\s*(\d+(?:,\d+)?)\s*(?:ML|G|L|KG)\b'
        ]
        
        # Category keywords (Brazilian products)
        self.category_keywords = {
            'bebidas': [
                'refrigerante', 'refri', 'suco', 'agua', 'água', 'cerveja', 'vinho',
                'cafe', 'café', 'cha', 'chá', 'guarana', 'guaraná', 'sprite', 'fanta',
                'pepsi', 'coca', 'cola', 'energetico', 'energético', 'isotonic',
                'achocolatado', 'leite', 'vitamina', 'fresh', 'crystal', 'bonafont'
            ],
            'alimentos': [
                'arroz', 'feijao', 'feijão', 'macarrao', 'macarrão', 'massa', 'farinha',
                'acucar', 'açúcar', 'sal', 'oleo', 'óleo', 'vinagre', 'molho', 'tempero',
                'biscoito', 'bolacha', 'pao', 'pão', 'bolo', 'doce'
            ],
            'carnes': [
                'carne', 'frango', 'peixe', 'porco', 'boi', 'linguica', 'linguiça',
                'salsicha', 'hamburguer', 'hambúrguer', 'bacon', 'presunto', 'mortadela',
                'salame', 'peito', 'coxa', 'asa', 'file', 'filé', 'costela'
            ],
            'laticinios': [
                'leite', 'queijo', 'iogurte', 'manteiga', 'nata', 'creme', 'requeijao',
                'requeijão', 'ricota', 'mussarela', 'parmesao', 'parmesão', 'catupiry'
            ],
            'limpeza': [
                'detergente', 'sabao', 'sabão', 'amaciante', 'desinfetante', 'limpa',
                'alvejante', 'multiuso', 'vidro', 'chao', 'chão', 'vaso', 'omo', 'ariel'
            ],
            'higiene': [
                'shampoo', 'condicionador', 'sabonete', 'pasta', 'dente', 'desodorante',
                'perfume', 'creme', 'hidratante', 'dove', 'rexona', 'nivea'
            ]
        }
        
        # Noise words to remove
        self.noise_words = {
            'COM', 'DE', 'DA', 'DO', 'DOS', 'DAS', 'PARA', 'POR', 'EM', 'NA', 'NO',
            'E', 'OU', 'SEU', 'SUA', 'SEUS', 'SUAS', 'ESSE', 'ESSA', 'ESSES', 'ESSAS',
            'ESTE', 'ESTA', 'ESTES', 'ESTAS', 'AQUELE', 'AQUELA', 'AQUELES', 'AQUELAS',
            'MEU', 'MINHA', 'MEUS', 'MINHAS', 'TEU', 'TUA', 'TEUS', 'TUAS'
        }
    
    def normalize(self, product_name: str) -> NormalizationResult:
        """
        Perform comprehensive normalization of Brazilian product name
        
        Args:
            product_name: Original product name
            
        Returns:
            NormalizationResult with all extracted information
        """
        if not product_name or not product_name.strip():
            return self._create_empty_result(product_name)
        
        # Check cache
        cache_key = f"normalize:{hash(product_name)}"
        cached_result = self.cache.load(cache_key, expiration_minutes=60)
        if cached_result:
            return self._dict_to_result(cached_result)
        
        original = product_name.strip()
        normalized = original
        steps = []
        
        # Step 1: Basic cleaning
        normalized = self._basic_cleaning(normalized)
        steps.append("basic_cleaning")
        
        # Step 2: Remove accents and normalize unicode
        normalized = self._remove_accents(normalized)
        steps.append("remove_accents")
        
        # Step 3: Extract and normalize sizes
        extracted_size, extracted_unit, size_normalized = self._extract_size_info(normalized)
        normalized = size_normalized
        steps.append("extract_size")
        
        # Step 4: Extract brand information
        extracted_brand, brand_normalized = self._extract_brand(normalized)
        normalized = brand_normalized
        steps.append("extract_brand")
        
        # Step 5: Expand abbreviations
        normalized = self._expand_abbreviations(normalized)
        steps.append("expand_abbreviations")
        
        # Step 6: Remove noise words
        normalized = self._remove_noise_words(normalized)
        steps.append("remove_noise")
        
        # Step 7: Final normalization
        normalized = self._final_normalization(normalized)
        steps.append("final_normalization")
        
        # Step 8: Extract category hints
        category_hints = self._extract_category_hints(original)
        steps.append("extract_categories")
        
        # Step 9: Calculate confidence score
        confidence_score = self._calculate_confidence_score(
            original, normalized, extracted_brand, extracted_size, category_hints
        )
        
        result = NormalizationResult(
            original=original,
            normalized=normalized,
            extracted_brand=extracted_brand,
            extracted_size=extracted_size,
            extracted_unit=extracted_unit,
            category_hints=category_hints,
            confidence_score=confidence_score,
            normalization_steps=steps
        )
        
        # Cache result
        self.cache.save(cache_key, self._result_to_dict(result))
        
        return result
    
    def normalize_batch(self, product_names: List[str]) -> List[NormalizationResult]:
        """Normalize a batch of product names efficiently"""
        results = []
        
        for name in product_names:
            result = self.normalize(name)
            results.append(result)
        
        return results
    
    def _basic_cleaning(self, text: str) -> str:
        """Basic text cleaning"""
        # Convert to uppercase
        text = text.upper()
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        # Remove special characters but keep essential ones
        text = re.sub(r'[^\w\s\-\.\,\%\+\&\(\)\/\*]', ' ', text)
        
        # Clean up multiple spaces
        text = ' '.join(text.split())
        
        return text
    
    def _remove_accents(self, text: str) -> str:
        """Remove accents from text"""
        # Normalize unicode and remove combining characters
        text = unicodedata.normalize('NFD', text)
        text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
        
        return text
    
    def _extract_size_info(self, text: str) -> Tuple[Optional[str], Optional[str], str]:
        """Extract size and unit information"""
        extracted_size = None
        extracted_unit = None
        
        for pattern in self.size_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                if isinstance(matches[0], tuple):
                    # Handle complex patterns like "6 X 350ML"
                    if len(matches[0]) >= 2:
                        extracted_size = f"{matches[0][0]}x{matches[0][1]}"
                        # Extract unit from the original match
                        unit_match = re.search(r'(ML|L|G|KG|UN|PCT|CX)', text)
                        if unit_match:
                            extracted_unit = unit_match.group(1)
                else:
                    extracted_size = matches[0]
                    # Extract unit
                    unit_match = re.search(pattern, text, re.IGNORECASE)
                    if unit_match:
                        full_match = unit_match.group(0)
                        unit_search = re.search(r'(ML|MILILITROS|L|LITROS?|G|GRAMAS?|KG|QUILOS?|UN|UND|UNIDADES?|PCT|PACOTES?|CX|CAIXAS?)', full_match)
                        if unit_search:
                            extracted_unit = unit_search.group(1)
                
                # Remove size information from text for further processing
                text = re.sub(pattern, ' ', text, flags=re.IGNORECASE)
                break
        
        # Clean up text after size removal
        text = ' '.join(text.split())
        
        return extracted_size, extracted_unit, text
    
    def _extract_brand(self, text: str) -> Tuple[Optional[str], str]:
        """Extract brand information from text"""
        extracted_brand = None
        
        # Check for known Brazilian brands
        words = text.split()
        for brand in self.brazilian_brands:
            if brand in text:
                extracted_brand = brand
                # Remove brand from text (keep first occurrence)
                text = text.replace(brand, ' ', 1)
                break
        
        # If no known brand found, check for potential brand patterns
        if not extracted_brand:
            # Look for capitalized words at the beginning (likely brands)
            for word in words[:3]:  # Check first 3 words
                if len(word) >= 3 and word.isupper() and word not in self.noise_words:
                    # Additional heuristics to identify brands
                    if (
                        not word.isdigit() and
                        word not in ['COM', 'DE', 'DA', 'DO', 'PARA'] and
                        not re.match(r'^\d+$', word)
                    ):
                        extracted_brand = word
                        text = text.replace(word, ' ', 1)
                        break
        
        # Clean up text
        text = ' '.join(text.split())
        
        return extracted_brand, text
    
    def _expand_abbreviations(self, text: str) -> str:
        """Expand common Brazilian abbreviations"""
        for abbr, expansion in self.abbreviations.items():
            # Use word boundaries to avoid partial matches
            pattern = r'\b' + re.escape(abbr) + r'\b'
            text = re.sub(pattern, expansion, text)
        
        return text
    
    def _remove_noise_words(self, text: str) -> str:
        """Remove noise words"""
        words = text.split()
        filtered_words = []
        
        for word in words:
            if word not in self.noise_words and len(word) > 1:
                filtered_words.append(word)
        
        return ' '.join(filtered_words)
    
    def _final_normalization(self, text: str) -> str:
        """Final text normalization"""
        # Remove extra punctuation
        text = re.sub(r'[^\w\s]', ' ', text)
        
        # Remove numbers that are likely codes (keep size-related numbers)
        # This is conservative - only remove obvious product codes
        words = text.split()
        filtered_words = []
        
        for word in words:
            # Keep word if it's not a pure number or if it might be relevant
            if not word.isdigit() or len(word) <= 4:  # Keep short numbers
                filtered_words.append(word)
        
        text = ' '.join(filtered_words)
        
        # Final cleanup
        text = ' '.join(text.split())
        
        return text.strip()
    
    def _extract_category_hints(self, original_text: str) -> List[str]:
        """Extract category hints from original text"""
        hints = []
        text_lower = original_text.lower()
        
        for category, keywords in self.category_keywords.items():
            for keyword in keywords:
                if keyword in text_lower:
                    hints.append(category)
                    break  # One hint per category
        
        return list(set(hints))  # Remove duplicates
    
    def _calculate_confidence_score(self, original: str, normalized: str, 
                                   brand: Optional[str], size: Optional[str],
                                   categories: List[str]) -> float:
        """Calculate confidence score for normalization"""
        score = 0.0
        
        # Base score for successful normalization
        if normalized and len(normalized) > 0:
            score += 0.3
        
        # Bonus for brand extraction
        if brand:
            score += 0.2
            # Extra bonus for known Brazilian brands
            if brand in self.brazilian_brands:
                score += 0.1
        
        # Bonus for size extraction
        if size:
            score += 0.15
        
        # Bonus for category detection
        if categories:
            score += 0.1 * min(len(categories), 2)  # Cap at 2 categories
        
        # Penalty for too much reduction
        reduction_ratio = len(normalized) / len(original) if original else 0
        if reduction_ratio < 0.3:  # Too much reduction
            score -= 0.2
        elif reduction_ratio > 0.8:  # Too little reduction
            score -= 0.1
        
        # Normalize score to 0-1 range
        return max(0.0, min(1.0, score))
    
    def _create_empty_result(self, original: str) -> NormalizationResult:
        """Create empty result for invalid input"""
        return NormalizationResult(
            original=original or "",
            normalized="",
            extracted_brand=None,
            extracted_size=None,
            extracted_unit=None,
            category_hints=[],
            confidence_score=0.0,
            normalization_steps=[]
        )
    
    def _result_to_dict(self, result: NormalizationResult) -> Dict:
        """Convert result to dictionary for caching"""
        return {
            'original': result.original,
            'normalized': result.normalized,
            'extracted_brand': result.extracted_brand,
            'extracted_size': result.extracted_size,
            'extracted_unit': result.extracted_unit,
            'category_hints': result.category_hints,
            'confidence_score': result.confidence_score,
            'normalization_steps': result.normalization_steps
        }
    
    def _dict_to_result(self, data: Dict) -> NormalizationResult:
        """Convert dictionary back to result"""
        return NormalizationResult(
            original=data['original'],
            normalized=data['normalized'],
            extracted_brand=data['extracted_brand'],
            extracted_size=data['extracted_size'],
            extracted_unit=data['extracted_unit'],
            category_hints=data['category_hints'],
            confidence_score=data['confidence_score'],
            normalization_steps=data['normalization_steps']
        )
    
    def get_brand_suggestions(self, text: str, top_k: int = 5) -> List[Tuple[str, float]]:
        """Get brand suggestions for ambiguous cases"""
        text_upper = text.upper()
        suggestions = []
        
        for brand in self.brazilian_brands:
            # Calculate simple similarity score
            if brand in text_upper:
                similarity = 1.0
            else:
                # Use simple character overlap
                common_chars = set(brand) & set(text_upper)
                similarity = len(common_chars) / len(set(brand)) if brand else 0
            
            if similarity > 0.3:  # Minimum threshold
                suggestions.append((brand, similarity))
        
        # Sort by similarity and return top_k
        suggestions.sort(key=lambda x: x[1], reverse=True)
        return suggestions[:top_k]
    
    def add_custom_brand(self, brand: str):
        """Add custom brand to the recognition list"""
        self.brazilian_brands.add(brand.upper())
        self.logger.info(f"Added custom brand: {brand}")
    
    def add_custom_abbreviation(self, abbr: str, expansion: str):
        """Add custom abbreviation expansion"""
        self.abbreviations[abbr.upper()] = expansion.upper()
        self.logger.info(f"Added custom abbreviation: {abbr} -> {expansion}")
    
    def get_normalization_statistics(self) -> Dict:
        """Get statistics about normalization performance"""
        # This would require tracking statistics over time
        # For now, return basic info about the normalizer
        return {
            'total_brands': len(self.brazilian_brands),
            'total_abbreviations': len(self.abbreviations),
            'total_categories': len(self.category_keywords),
            'supported_units': ['ML', 'L', 'G', 'KG', 'UN', 'PCT', 'CX'],
            'noise_words_count': len(self.noise_words)
        }