#!/usr/bin/env python3
"""
Product Normalizer - Normalize product descriptions for comparison
"""

import re
import unicodedata
from typing import Dict, List, Optional, Union
from utils.logging.logging_manager import LogManager


class ProductNormalizer:
    """
    Normalize product descriptions for better matching and comparison.
    
    This class handles various normalization tasks:
    - Text cleaning and standardization
    - Unit removal and standardization
    - Abbreviation expansion
    - Brand name standardization
    """
    
    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("ProductNormalizer")
        
        # Common units to remove/standardize
        self.units_to_remove = {
            # Weight units
            r'\b\d+\s*(KG|Kg|kg|KILOS?|G|g|GRAMAS?)\b',
            r'\b\d+,?\d*\s*(KG|Kg|kg|KILOS?|G|g|GRAMAS?)\b',
            
            # Volume units  
            r'\b\d+\s*(ML|ml|L|l|LITROS?)\b',
            r'\b\d+,?\d*\s*(ML|ml|L|l|LITROS?)\b',
            
            # Package units
            r'\b\d+\s*(UN|UNID|UNIDADES?|PC|PECAS?|CX|CAIXAS?)\b',
            
            # Standalone units at end
            r'\s+(KG|Kg|kg|UN|UF|ML|L|G|UNID|PC|CX)$',
            r'\s+(KILOS?|GRAMAS?|LITROS?|UNIDADES?|PECAS?|CAIXAS?)$'
        }
        
        # Common abbreviations to expand (expandido)
        self.abbreviations = {
            # Beverages
            'REFRI': 'REFRIGERANTE',
            'REFRIGER': 'REFRIGERANTE', 
            'REF': 'REFRIGERANTE',
            'COCA': 'COCA-COLA',
            'PEPSI': 'PEPSI-COLA',
            'GUARANA': 'GUARANÁ',
            'CAF': 'CAFÉ',
            'CAFE': 'CAFÉ',
            'AGUA': 'ÁGUA',
            'H2O': 'ÁGUA',
            
            # Food items
            'BISCT': 'BISCOITO',
            'BISC': 'BISCOITO',
            'CHOC': 'CHOCOLATE',
            'CHOCOL': 'CHOCOLATE',
            'FRANG': 'FRANGO',
            'FRG': 'FRANGO',
            'QUEIJ': 'QUEIJO',
            'QJ': 'QUEIJO',
            'PAO': 'PÃO',
            'PAOZ': 'PÃO',
            'ACUC': 'AÇÚCAR',
            'ACUCAR': 'AÇÚCAR',
            'OLEO': 'ÓLEO',
            'MARG': 'MARGARINA',
            'MANTE': 'MANTEIGA',
            'MANT': 'MANTEIGA',
            'MACARR': 'MACARRÃO',
            'MAC': 'MACARRÃO',
            'MACARRO': 'MACARRÃO',
            
            # Brands & Variations from our data
            'NEST': 'NESTLÉ',
            'NESTLE': 'NESTLÉ',
            'GAROTO': 'GAROTO',
            'PRESTÍGIO': 'PRESTÍGIO',
            'PRESTIGIO': 'PRESTÍGIO',
            'LACTA': 'LACTA',
            'POCAM': 'POCA',  # Size variation we saw
            'POCA': 'POUCA',
            'GDE': 'GRANDE',
            'PEQ': 'PEQUENO',
            'PQN': 'PEQUENO',
            'MED': 'MÉDIO',
            'MEDIO': 'MÉDIO',
            'MINI': 'PEQUENO',
            
            # Fruits (from our data)
            'MEXER': 'MEXERICA',
            'MEXERIC': 'MEXERICA',
            'BANAN': 'BANANA',
            'MELAN': 'MELANCIA',
            'MELANCIA': 'MELANCIA',
            'MELO': 'MELÃO',
            'MELAO': 'MELÃO',
            'CATUR': 'CATURRA',  # Banana variety
            'PRAT': 'PRATA',     # Banana variety
            
            # Vegetables (from our data)
            'CEBOL': 'CEBOLA',
            'MORAN': 'MORANGA',
            'MORANG': 'MORANGA',
            'JAPON': 'JAPONESA',
            'JAPONESA': 'JAPONESA',
            'MIUD': 'MIÚDA',
            'MIUDA': 'MIÚDA',
            
            # Units and packages
            'KG': 'QUILOGRAMA',
            'KILO': 'QUILOGRAMA',
            'KILOS': 'QUILOGRAMA', 
            'G': 'GRAMA',
            'GR': 'GRAMA',
            'L': 'LITRO',
            'LIT': 'LITRO',
            'ML': 'MILILITRO',
            'UN': 'UNIDADE',
            'UND': 'UNIDADE',
            'UNID': 'UNIDADE',
            'PC': 'PEÇA',
            'PCT': 'PACOTE',
            'PACOT': 'PACOTE',
            'CX': 'CAIXA',
            'LAT': 'LATA',
            'GAR': 'GARRAFA',
            'GARRAF': 'GARRAFA',
            'PT': 'PACOTE',
            'EMB': 'EMBALAGEM',
            
            # Colors and descriptions  
            'VM': 'VERMELHO',
            'VD': 'VERDE',
            'AM': 'AMARELO',
            'AZ': 'AZUL',
            'BR': 'BRANCO',
            'PTO': 'PRETO',
            'RS': 'ROSA',
            'RX': 'ROXO',
            
            # Cleaning products
            'DETERG': 'DETERGENTE',
            'DET': 'DETERGENTE',
            'SABAO': 'SABÃO',
            'SB': 'SABÃO',
            'SHAMP': 'SHAMPOO',
            'SH': 'SHAMPOO',
            'COND': 'CONDICIONADOR',
            'DESOD': 'DESODORANTE',
            'PERFUM': 'PERFUME',
            
            # Common patterns
            'LEITE': 'LEITE',
            'LT': 'LEITE',
            'CERT': 'CERVEJA',
            'CERV': 'CERVEJA',
            'ARROZ': 'ARROZ',
            'FEIJ': 'FEIJÃO',
            'FEIJAO': 'FEIJÃO'
        }
        
        # Brand standardization
        self.brand_patterns = {
            'coca': ['COCA', 'COCACOLA', 'COCA-COLA'],
            'pepsi': ['PEPSI', 'PEPSI-COLA'],
            'nestle': ['NESTLE', 'NESTLÉ'],
            'unilever': ['UNILEVER', 'UNI'],
            'garoto': ['GAROTO', 'GAR'],
            'sadia': ['SADIA', 'SAD'],
            'perdigao': ['PERDIGÃO', 'PERDIGAO', 'PERD']
        }
        
        # Common product categories for context (expandido)
        self.category_keywords = {
            'beverages': [
                'REFRIGERANTE', 'SUCO', 'ÁGUA', 'ÁGUA', 'CERVEJA', 'VINHO', 'CAFÉ', 'CHÁ', 'CHA',
                'ENERGÉTICO', 'ENERGETICO', 'ISOTÔNICO', 'ISOTONICO', 'GUARANÁ', 'GUARANA',
                'COCA', 'PEPSI', 'SPRITE', 'FANTA', 'DOLLY', 'SUKITA', 'REFRI', 'REFRIGER',
                'BEBIDA', 'DRINK', 'SODA', 'H2O', 'ÁGUA DE COCO', 'AGUA DE COCO'
            ],
            'fruits': [
                'BANANA', 'MAÇÃ', 'MACA', 'LARANJA', 'UVA', 'MANGA', 'ABACAXI', 'MORANGO',
                'MEXERICA', 'TANGERINA', 'LIMÃO', 'LIMAO', 'KIWI', 'PÊRA', 'PERA', 'CAQUI',
                'MELANCIA', 'MELÃO', 'MELAO', 'MAMÃO', 'MAMAO', 'GOIABA', 'MARACUJÁ', 'MARACUJA',
                'PÊSSEGO', 'PESSEGO', 'AMEIXA', 'COCO', 'ABACATE', 'CARAMBOLA', 'PITANGA',
                'JABUTICABA', 'AÇAÍ', 'ACAI', 'CUPUAÇU', 'CUPUACU', 'PITAYA', 'ROMÃ', 'ROMA',
                'FRUTA', 'FRUTAS'
            ],
            'vegetables': [
                'ALFACE', 'TOMATE', 'CEBOLA', 'ALHO', 'CENOURA', 'BATATA', 'ABOBRINHA',
                'BRÓCOLIS', 'BROCOLIS', 'COUVE', 'ESPINAFRE', 'RÚCULA', 'RUCULA', 'AGRIÃO', 'AGRIAO',
                'PEPINO', 'PIMENTÃO', 'PIMENTAO', 'BERINJELA', 'ABOBRINHA', 'CHUCHU',
                'MANDIOCA', 'INHAME', 'BETERRABA', 'RABANETE', 'NABO', 'ACELGA',
                'VERDURA', 'VERDURAS', 'LEGUME', 'LEGUMES', 'HORTIFRUTI', 'HORTALIÇA', 'HORTALICA'
            ],
            'dairy': [
                'LEITE', 'QUEIJO', 'IOGURTE', 'MANTEIGA', 'NATA', 'CREME', 'REQUEIJÃO', 'REQUEIJAO',
                'MUSSARELA', 'MOZZARELLA', 'PRATO', 'CHEDDAR', 'COALHO', 'RICOTA',
                'CREAM CHEESE', 'COTTAGE', 'GORGONZOLA', 'PARMESÃO', 'PARMESAO',
                'LÁCTEO', 'LACTEO', 'LATICÍNIO', 'LATICINIO'
            ],
            'meat': [
                'CARNE', 'FRANGO', 'PEIXE', 'PORCO', 'BOI', 'LINGUIÇA', 'LINGUICA', 'SALSICHA',
                'PRESUNTO', 'MORTADELA', 'SALAME', 'BACON', 'PEITO', 'COXA', 'SOBRECOXA',
                'FILÉ', 'FILE', 'PICANHA', 'ALCATRA', 'MAMINHA', 'COSTELA', 'ACÉM', 'ACEM',
                'PATINHO', 'COXÃO', 'COXAO', 'TILÁPIA', 'TILAPIA', 'SALMÃO', 'SALMAO',
                'SARDINHA', 'ATUM', 'BACALHAU', 'CAMARÃO', 'CAMARAO', 'LAGOSTA'
            ],
            'bread': [
                'PÃO', 'PAO', 'BISCOITO', 'BOLO', 'TORRADA', 'PÃOZINHO', 'PAOZINHO',
                'FRANCÊS', 'FRANCES', 'FORMA', 'INTEGRAL', 'DOCE', 'SALGADO',
                'CROISSANT', 'BRIOCHE', 'CIABATTA', 'BAGUETE', 'PANETTONE',
                'PADARIA', 'PANIFICAÇÃO', 'PANIFICACAO'
            ],
            'snacks': [
                'CHOCOLATE', 'BALA', 'CHICLETE', 'PIRULITO', 'DOCE', 'BOMBOM', 'TRUFA',
                'SALGADINHO', 'CHIPS', 'PIPOCA', 'AMENDOIM', 'CASTANHA', 'NOZ',
                'BISCOITO', 'WAFER', 'COOKIE', 'CRACKER', 'ROSQUINHA',
                'PRESTÍGIO', 'PRESTIGIO', 'NEST', 'GAROTO', 'LACTA', 'HERSHEY'
            ],
            'grains': [
                'ARROZ', 'FEIJÃO', 'FEIJAO', 'MACARRÃO', 'MACARRAO', 'ESPAGUETE',
                'FARINHA', 'FUBÁ', 'FUBA', 'AVEIA', 'QUINOA', 'LENTILHA',
                'GRÃO', 'GRAO', 'CEREAL', 'GRANOLA', 'MUESLI'
            ],
            'cleaning': [
                'DETERGENTE', 'SABÃO', 'SABAO', 'AMACIANTE', 'DESINFETANTE', 'ÁLCOOL', 'ALCOOL',
                'ÁGUA SANITÁRIA', 'AGUA SANITARIA', 'CLORO', 'LIMPEZA', 'LIMPA',
                'BACTERICIDA', 'ANTISÉTICO', 'ANTISSETICO', 'HIGIENE'
            ],
            'condiments': [
                'SAL', 'AÇÚCAR', 'ACUCAR', 'ÓLEO', 'OLEO', 'VINAGRE', 'AZEITE',
                'TEMPERO', 'CONDIMENTO', 'MOLHO', 'KETCHUP', 'MOSTARDA',
                'MAIONESE', 'BARBECUE', 'PIMENTA', 'ORÉGANO', 'OREGANO'
            ],
            'hygiene': [
                'SHAMPOO', 'CONDICIONADOR', 'SABONETE', 'PASTA', 'DENTE', 'DENTAL',
                'ESCOVA', 'DESODORANTE', 'PERFUME', 'CREME', 'LOÇÃO', 'LOCAO',
                'PAPEL HIGIÊNICO', 'PAPEL HIGIENICO', 'ABSORVENTE', 'FRALDA'
            ]
        }
    
    def normalize(self, description: str, preserve_brand: bool = True) -> str:
        """
        Main normalization method that applies all transformations.
        
        Args:
            description: Raw product description
            preserve_brand: Whether to preserve brand information
            
        Returns:
            Normalized description
        """
        if not description:
            return ""
        
        self.logger.debug(f"Normalizing: '{description}'")
        
        # Step 1: Basic cleaning
        normalized = self._clean_text(description)
        
        # Step 2: Remove units and quantities  
        normalized = self._remove_units_and_quantities(normalized)
        
        # Step 3: Expand abbreviations
        normalized = self._expand_abbreviations(normalized)
        
        # Step 4: Standardize brands (if preserving)
        if preserve_brand:
            normalized = self._standardize_brands(normalized)
        
        # Step 5: Final cleanup
        normalized = self._final_cleanup(normalized)
        
        self.logger.debug(f"Normalized result: '{normalized}'")
        return normalized
    
    def _clean_text(self, text: str) -> str:
        """Basic text cleaning and standardization"""
        
        # Convert to uppercase
        text = text.upper()
        
        # Remove accents and special characters
        text = unicodedata.normalize('NFD', text)
        text = ''.join(char for char in text if unicodedata.category(char) != 'Mn')
        
        # Remove special characters but keep spaces and hyphens
        text = re.sub(r'[^\w\s\-]', ' ', text)
        
        # Normalize multiple spaces
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def _remove_units_and_quantities(self, text: str) -> str:
        """Remove weight, volume, and quantity specifications"""
        
        # Apply all unit removal patterns
        for pattern in self.units_to_remove:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        # Remove isolated numbers that might be quantities/sizes
        text = re.sub(r'\b\d+[.,]?\d*\b', '', text)
        
        # Clean up extra spaces
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def _expand_abbreviations(self, text: str) -> str:
        """Expand common abbreviations to full words"""
        
        words = text.split()
        expanded_words = []
        
        for word in words:
            # Check if word is an abbreviation
            expanded = self.abbreviations.get(word, word)
            expanded_words.append(expanded)
        
        return ' '.join(expanded_words)
    
    def _standardize_brands(self, text: str) -> str:
        """Standardize brand names for better matching"""
        
        for standard_brand, variations in self.brand_patterns.items():
            for variation in variations:
                # Replace brand variations with standard form
                pattern = r'\b' + re.escape(variation) + r'\b'
                text = re.sub(pattern, variations[0], text, flags=re.IGNORECASE)
        
        return text
    
    def _final_cleanup(self, text: str) -> str:
        """Final cleanup and standardization"""
        
        # Remove extra spaces
        text = re.sub(r'\s+', ' ', text)
        
        # Remove leading/trailing spaces
        text = text.strip()
        
        # Remove empty words
        words = [word for word in text.split() if word.strip()]
        
        return ' '.join(words)
    
    def extract_features(self, description: str) -> Dict[str, Union[str, int, None]]:
        """
        Extract structured features from product description.
        
        Args:
            description: Product description
            
        Returns:
            Dictionary with extracted features (brand, product, category, etc.)
        """
        normalized = self.normalize(description)
        words = normalized.split()
        
        features = {
            'brand': self._extract_brand(normalized),
            'product_type': self._extract_product_type(normalized),
            'category': self._extract_category(normalized),
            'variant': self._extract_variant(normalized),
            'normalized_text': normalized,
            'word_count': len(words),
            'first_word': words[0] if words else None,
            'last_word': words[-1] if words else None
        }
        
        return features
    
    def _extract_brand(self, text: str) -> Optional[str]:
        """Extract brand name from normalized text"""
        
        words = text.split()
        if not words:
            return None
        
        # Check for known brand patterns
        for standard_brand, variations in self.brand_patterns.items():
            for variation in variations:
                if variation in text:
                    return variations[0]  # Return standard form
        
        # Heuristic: first word might be brand if it appears frequently
        # This would require frequency analysis from the database
        return words[0] if len(words) > 1 else None
    
    def _extract_product_type(self, text: str) -> Optional[str]:
        """Extract main product type"""
        
        # Look for product keywords after potential brand
        words = text.split()
        if len(words) < 2:
            return words[0] if words else None
        
        # Skip first word (likely brand) and find product type
        for word in words[1:]:
            # Check against category keywords
            for category, keywords in self.category_keywords.items():
                if word in keywords:
                    return word
        
        # Fallback to second word
        return words[1] if len(words) > 1 else None
    
    def _extract_category(self, text: str) -> Optional[str]:
        """Extract product category"""
        
        for category, keywords in self.category_keywords.items():
            for keyword in keywords:
                if keyword in text:
                    return category
        
        return 'other'
    
    def _extract_variant(self, text: str) -> Optional[str]:
        """Extract product variant (flavor, type, etc.)"""
        
        words = text.split()
        if len(words) <= 2:
            return None
        
        # Variant is typically the last part of the description
        # after brand and product type
        variant_words = words[2:]  # Skip brand and product type
        
        return ' '.join(variant_words) if variant_words else None
    
    def similarity_score(self, text1: str, text2: str) -> float:
        """
        Calculate basic similarity score between two normalized texts.
        
        This is a simple implementation for Phase 2.
        More sophisticated algorithms will be added later.
        """
        
        if not text1 or not text2:
            return 0.0
        
        norm1 = self.normalize(text1)
        norm2 = self.normalize(text2)
        
        if norm1 == norm2:
            return 1.0
        
        # Simple word-based similarity
        words1 = set(norm1.split())
        words2 = set(norm2.split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        return len(intersection) / len(union) if union else 0.0
    
    def get_normalization_stats(self, descriptions: List[str]) -> Dict[str, Union[int, float, str]]:
        """
        Get statistics about normalization effectiveness.
        
        Args:
            descriptions: List of product descriptions
            
        Returns:
            Statistics dictionary
        """
        
        if not descriptions:
            return {}
        
        original_unique = len(set(descriptions))
        normalized_unique = len(set(self.normalize(desc) for desc in descriptions))
        
        # Feature extraction stats
        features_list = [self.extract_features(desc) for desc in descriptions]
        brands = [f['brand'] for f in features_list if f['brand']]
        categories = [f['category'] for f in features_list if f['category']]
        
        # Calculate average word count safely
        word_counts = [f['word_count'] for f in features_list if isinstance(f['word_count'], int)]
        avg_word_count = sum(word_counts) / len(word_counts) if word_counts else 0
        
        stats = {
            'total_descriptions': len(descriptions),
            'original_unique': original_unique,
            'normalized_unique': normalized_unique,
            'reduction_ratio': (original_unique - normalized_unique) / original_unique if original_unique > 0 else 0,
            'unique_brands': len(set(brands)),
            'unique_categories': len(set(categories)),
            'avg_word_count': avg_word_count
        }
        
        return stats
