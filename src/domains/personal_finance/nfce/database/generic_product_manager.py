#!/usr/bin/env python3
"""
Generic Product Manager - Manages product deduplication and similarity matching
"""

import json
import uuid
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from utils.logging.logging_manager import LogManager
from utils.data.duckdb_manager import DuckDBManager
from domains.personal_finance.nfce.similarity.enhanced_similarity_calculator import EnhancedSimilarityCalculator
from domains.personal_finance.nfce.similarity.feature_extractor import FeatureExtractor


@dataclass
class GenericProduct:
    """Represents a generic product across establishments"""
    
    id: str
    normalized_name: str
    canonical_description: str
    alternative_descriptions: List[str]
    category: Optional[str]
    brand: Optional[str]
    unit: Optional[str]
    similarity_features: str  # JSON string
    confidence_score: float
    total_occurrences: int
    establishments_count: int
    avg_price: Optional[Decimal]
    min_price: Optional[Decimal]
    max_price: Optional[Decimal]
    price_variance: Optional[Decimal]
    first_seen: datetime
    last_seen: datetime


@dataclass
class ProductMatchResult:
    """Result of product matching operation"""
    
    generic_product_id: str
    similarity_score: float
    confidence_score: float
    match_method: str  # 'exact', 'similarity', 'manual'
    matching_tokens: List[str]
    brazilian_patterns: List[str]
    quantity_matches: List[str]
    is_new_product: bool = False


class GenericProductManager:
    """Manages generic products and automatic deduplication using similarity detection"""
    
    def __init__(self, 
                 db_manager: DuckDBManager,
                 similarity_threshold: float = 0.60,
                 use_sbert: bool = False,
                 sbert_model: str = "paraphrase-multilingual-MiniLM-L12-v2"):
        """
        Initialize generic product manager
        
        Args:
            db_manager: Database manager instance
            similarity_threshold: Threshold for product similarity matching
            use_sbert: Whether to use SBERT embeddings
            sbert_model: SBERT model name
        """
        
        self.logger = LogManager.get_instance().get_logger("GenericProductManager")
        self.db_manager = db_manager
        
        # Initialize similarity detection
        try:
            self.similarity_calculator = EnhancedSimilarityCalculator(
                similarity_threshold=similarity_threshold,
                use_hybrid=use_sbert,
                sbert_model=sbert_model
            )
            self.feature_extractor = FeatureExtractor()
            self.similarity_enabled = True
            self.logger.info(f"Similarity detection initialized (SBERT: {use_sbert}, threshold: {similarity_threshold})")
        except Exception as e:
            self.logger.warning(f"Failed to initialize similarity detection: {e}")
            self.similarity_enabled = False
            self.similarity_calculator = None
            self.feature_extractor = None
    
    def find_or_create_generic_product(self, 
                                     description: str,
                                     establishment_id: str,
                                     unit_price: Optional[Decimal] = None,
                                     unit: Optional[str] = None,
                                     product_code: Optional[str] = None) -> ProductMatchResult:
        """
        Find existing generic product or create new one if no similar product exists
        
        Args:
            description: Product description from invoice
            establishment_id: ID of the establishment
            unit_price: Price of the product
            unit: Unit of measurement
            product_code: Local product code
            
        Returns:
            ProductMatchResult with match information
        """
        
        self.logger.debug(f"Finding or creating generic product for: '{description}'")
        
        try:
            # Step 1: Try exact match first (fastest)
            exact_match = self._find_exact_match(description)
            if exact_match:
                self.logger.debug(f"Found exact match: {exact_match['id']}")
                return self._create_match_result(
                    exact_match, 1.0, 1.0, 'exact', [], [], []
                )
            
            # Step 2: Try similarity matching if enabled
            if self.similarity_enabled:
                similarity_match = self._find_similarity_match(description, establishment_id)
                if similarity_match:
                    self.logger.debug(f"Found similarity match: {similarity_match['result']['generic_product_id']}")
                    return similarity_match['result']
            
            # Step 3: No match found, create new generic product
            self.logger.debug("No match found, creating new generic product")
            new_product = self._create_new_generic_product(
                description, establishment_id, unit_price, unit, product_code
            )
            
            return ProductMatchResult(
                generic_product_id=new_product['id'],
                similarity_score=1.0,
                confidence_score=1.0,
                match_method='new',
                matching_tokens=[],
                brazilian_patterns=[],
                quantity_matches=[],
                is_new_product=True
            )
            
        except Exception as e:
            self.logger.error(f"Error finding/creating generic product: {e}")
            raise
    
    def _find_exact_match(self, description: str) -> Optional[Dict[str, Any]]:
        """Find exact match by canonical description or alternative descriptions"""
        
        try:
            conn = self.db_manager.get_connection("main_db")
            
            # Try canonical description first
            query = """
            SELECT id, canonical_description, alternative_descriptions, category, brand, unit,
                   confidence_score, total_occurrences, establishments_count
            FROM generic_products 
            WHERE canonical_description = ? OR ? = ANY(alternative_descriptions)
            LIMIT 1
            """
            
            result = conn.execute(query, [description, description]).fetchone()
            
            if result:
                return {
                    'id': result[0],
                    'canonical_description': result[1],
                    'alternative_descriptions': json.loads(result[2]) if result[2] else [],
                    'category': result[3],
                    'brand': result[4],
                    'unit': result[5],
                    'confidence_score': float(result[6]),
                    'total_occurrences': result[7],
                    'establishments_count': result[8]
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error in exact match search: {e}")
            return None
    
    def _find_similarity_match(self, description: str, establishment_id: str) -> Optional[Dict[str, Any]]:
        """Find similar product using similarity calculator"""
        
        if not self.similarity_enabled:
            return None
        
        try:
            # Extract features for the input description
            input_features = self.feature_extractor.extract(description)
            
            # Get all existing generic products for comparison
            existing_products = self._get_all_generic_products_for_similarity()
            
            if not existing_products:
                return None
            
            # Extract features for all existing products
            existing_features_list = []
            for product in existing_products:
                try:
                    features = self.feature_extractor.extract(product['canonical_description'])
                    existing_features_list.append((product, features))
                except Exception as e:
                    self.logger.warning(f"Failed to extract features for '{product['canonical_description']}': {e}")
                    continue
            
            if not existing_features_list:
                return None
            
            # Find best match
            best_match = None
            best_score = 0
            
            for product_data, features in existing_features_list:
                try:
                    result = self.similarity_calculator.calculate_similarity(input_features, features)
                    
                    if result.final_score > best_score:
                        best_score = result.final_score
                        best_match = {
                            'product': product_data,
                            'similarity_result': result
                        }
                
                except Exception as e:
                    self.logger.warning(f"Error calculating similarity: {e}")
                    continue
            
            # Check if best match meets threshold
            if best_match and best_score >= self.similarity_calculator.similarity_threshold:
                
                # Log similarity match for auditing
                self._log_similarity_match(
                    description, 
                    best_match['product']['id'],
                    best_match['similarity_result'],
                    establishment_id
                )
                
                return {
                    'result': self._create_match_result_from_similarity(
                        best_match['product'],
                        best_match['similarity_result']
                    )
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error in similarity matching: {e}")
            return None
    
    def _get_all_generic_products_for_similarity(self) -> List[Dict[str, Any]]:
        """Get all generic products for similarity comparison"""
        
        try:
            conn = self.db_manager.get_connection("main_db")
            
            query = """
            SELECT id, canonical_description, category, brand, unit, confidence_score,
                   total_occurrences, establishments_count
            FROM generic_products
            ORDER BY total_occurrences DESC
            LIMIT 1000  -- Limit for performance
            """
            
            results = conn.execute(query).fetchall()
            
            products = []
            for row in results:
                products.append({
                    'id': row[0],
                    'canonical_description': row[1],
                    'category': row[2],
                    'brand': row[3],
                    'unit': row[4],
                    'confidence_score': float(row[5]),
                    'total_occurrences': row[6],
                    'establishments_count': row[7]
                })
            
            return products
            
        except Exception as e:
            self.logger.error(f"Error getting generic products for similarity: {e}")
            return []
    
    def _create_new_generic_product(self, 
                                  description: str,
                                  establishment_id: str,
                                  unit_price: Optional[Decimal],
                                  unit: Optional[str],
                                  product_code: Optional[str]) -> Dict[str, Any]:
        """Create new generic product"""
        
        try:
            # Generate ID
            product_id = str(uuid.uuid4())
            
            # Extract features and metadata
            if self.feature_extractor:
                features = self.feature_extractor.extract(description)
                normalized_name = features.normalized_description
                category = features.category
                brand = features.brand
                similarity_features = json.dumps({
                    'tokens': features.tokens,
                    'bigrams': features.bigrams,
                    'category': features.category,
                    'brand': features.brand,
                    'core_key': features.core_key
                })
            else:
                normalized_name = description.lower().strip()
                category = None
                brand = None
                similarity_features = json.dumps({'tokens': normalized_name.split()})
            
            # Insert into database
            conn = self.db_manager.get_connection("main_db")
            
            insert_query = """
            INSERT INTO generic_products (
                id, normalized_name, canonical_description, alternative_descriptions,
                category, brand, unit, similarity_features, confidence_score,
                total_occurrences, establishments_count, avg_price, min_price, max_price,
                first_seen, last_seen, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            now = datetime.now()
            
            conn.execute(insert_query, [
                product_id,
                normalized_name,
                description,  # Use original as canonical for new products
                json.dumps([description]),  # Start with original description
                category,
                brand,
                unit,
                similarity_features,
                1.0,  # New product has max confidence
                1,    # First occurrence
                1,    # First establishment
                float(unit_price) if unit_price else None,
                float(unit_price) if unit_price else None,
                float(unit_price) if unit_price else None,
                now,
                now,
                now,
                now
            ])
            
            self.logger.info(f"Created new generic product: {product_id} - '{description}'")
            
            return {
                'id': product_id,
                'canonical_description': description,
                'normalized_name': normalized_name,
                'category': category,
                'brand': brand
            }
            
        except Exception as e:
            self.logger.error(f"Error creating new generic product: {e}")
            raise
    
    def _create_match_result(self, product: Dict[str, Any], similarity_score: float, 
                           confidence_score: float, match_method: str,
                           matching_tokens: List[str], brazilian_patterns: List[str],
                           quantity_matches: List[str]) -> ProductMatchResult:
        """Create ProductMatchResult from product data"""
        
        return ProductMatchResult(
            generic_product_id=product['id'],
            similarity_score=similarity_score,
            confidence_score=confidence_score,
            match_method=match_method,
            matching_tokens=matching_tokens,
            brazilian_patterns=brazilian_patterns,
            quantity_matches=quantity_matches
        )
    
    def _create_match_result_from_similarity(self, product: Dict[str, Any], 
                                           similarity_result) -> ProductMatchResult:
        """Create ProductMatchResult from similarity calculation result"""
        
        return ProductMatchResult(
            generic_product_id=product['id'],
            similarity_score=similarity_result.final_score,
            confidence_score=getattr(similarity_result, 'confidence_score', 0.8),
            match_method='similarity',
            matching_tokens=getattr(similarity_result, 'matching_tokens', []),
            brazilian_patterns=getattr(similarity_result, 'brazilian_tokens', []),
            quantity_matches=getattr(similarity_result, 'quantity_matches', [])
        )
    
    def _log_similarity_match(self, source_description: str, matched_product_id: str,
                            similarity_result, establishment_id: str):
        """Log similarity match for auditing and improvement"""
        
        try:
            conn = self.db_manager.get_connection("main_db")
            
            insert_query = """
            INSERT INTO product_similarity_matches (
                id, source_description, matched_generic_product_id, similarity_score,
                confidence_score, match_method, matching_tokens, brazilian_patterns,
                quantity_matches, establishment_id, processed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            conn.execute(insert_query, [
                str(uuid.uuid4()),
                source_description,
                matched_product_id,
                float(similarity_result.final_score),
                float(getattr(similarity_result, 'confidence_score', 0.8)),
                'similarity',
                json.dumps(getattr(similarity_result, 'matching_tokens', [])),
                json.dumps(getattr(similarity_result, 'brazilian_tokens', [])),
                json.dumps(getattr(similarity_result, 'quantity_matches', [])),
                establishment_id,
                datetime.now()
            ])
            
        except Exception as e:
            self.logger.warning(f"Failed to log similarity match: {e}")
    
    def update_generic_product_statistics(self, generic_product_id: str, 
                                        establishment_id: str, unit_price: Optional[Decimal]):
        """Update statistics for generic product after new occurrence"""
        
        try:
            conn = self.db_manager.get_connection("main_db")
            
            # Get current statistics
            query = """
            SELECT total_occurrences, establishments_count, avg_price, min_price, max_price
            FROM generic_products 
            WHERE id = ?
            """
            
            current = conn.execute(query, [generic_product_id]).fetchone()
            if not current:
                return
            
            total_occ, est_count, avg_price, min_price, max_price = current
            
            # Check if this is a new establishment
            est_query = """
            SELECT COUNT(*) FROM establishment_products 
            WHERE generic_product_id = ? AND establishment_id = ?
            """
            
            is_new_establishment = conn.execute(est_query, [generic_product_id, establishment_id]).fetchone()[0] == 0
            
            # Update counters
            new_total_occ = total_occ + 1
            new_est_count = est_count + 1 if is_new_establishment else est_count
            
            # Update pricing if price provided
            if unit_price:
                price_float = float(unit_price)
                
                if avg_price is None:
                    new_avg_price = price_float
                    new_min_price = price_float
                    new_max_price = price_float
                else:
                    # Calculate new average
                    new_avg_price = ((avg_price * (total_occ - 1)) + price_float) / total_occ
                    new_min_price = min(min_price or price_float, price_float)
                    new_max_price = max(max_price or price_float, price_float)
                
                # Calculate price variance
                price_variance = ((new_max_price - new_min_price) / new_min_price * 100) if new_min_price > 0 else 0
            else:
                new_avg_price = avg_price
                new_min_price = min_price
                new_max_price = max_price
                price_variance = 0
            
            # Update generic product
            update_query = """
            UPDATE generic_products SET
                total_occurrences = ?,
                establishments_count = ?,
                avg_price = ?,
                min_price = ?,
                max_price = ?,
                price_variance = ?,
                last_seen = ?,
                updated_at = ?
            WHERE id = ?
            """
            
            now = datetime.now()
            conn.execute(update_query, [
                new_total_occ,
                new_est_count,
                new_avg_price,
                new_min_price,
                new_max_price,
                price_variance,
                now,
                now,
                generic_product_id
            ])
            
            self.logger.debug(f"Updated statistics for generic product {generic_product_id}")
            
        except Exception as e:
            self.logger.error(f"Error updating generic product statistics: {e}")
    
    def create_or_update_establishment_product(self, generic_product_id: str,
                                             establishment_id: str, local_description: str,
                                             unit_price: Optional[Decimal], 
                                             local_product_code: Optional[str] = None,
                                             local_unit: Optional[str] = None) -> str:
        """Create or update establishment-specific product data"""
        
        try:
            conn = self.db_manager.get_connection("main_db")
            
            # Check if establishment product already exists
            check_query = """
            SELECT id, occurrence_count, avg_price, min_price, max_price
            FROM establishment_products
            WHERE generic_product_id = ? AND establishment_id = ?
            """
            
            existing = conn.execute(check_query, [generic_product_id, establishment_id]).fetchone()
            
            if existing:
                # Update existing establishment product
                ep_id, occ_count, avg_price, min_price, max_price = existing
                
                new_occ_count = occ_count + 1
                
                if unit_price:
                    price_float = float(unit_price)
                    new_avg_price = ((avg_price * occ_count) + price_float) / new_occ_count if avg_price else price_float
                    new_min_price = min(min_price or price_float, price_float)
                    new_max_price = max(max_price or price_float, price_float)
                    new_current_price = price_float
                else:
                    new_avg_price = avg_price
                    new_min_price = min_price
                    new_max_price = max_price
                    new_current_price = None
                
                update_query = """
                UPDATE establishment_products SET
                    occurrence_count = ?,
                    current_price = ?,
                    avg_price = ?,
                    min_price = ?,
                    max_price = ?,
                    last_seen = ?,
                    updated_at = ?
                WHERE id = ?
                """
                
                now = datetime.now()
                conn.execute(update_query, [
                    new_occ_count,
                    new_current_price,
                    new_avg_price,
                    new_min_price,
                    new_max_price,
                    now,
                    now,
                    ep_id
                ])
                
                return ep_id
            
            else:
                # Create new establishment product
                ep_id = str(uuid.uuid4())
                
                insert_query = """
                INSERT INTO establishment_products (
                    id, generic_product_id, establishment_id, local_product_code,
                    local_description, local_unit, current_price, avg_price,
                    min_price, max_price, occurrence_count, first_seen, last_seen,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                now = datetime.now()
                price_float = float(unit_price) if unit_price else None
                
                conn.execute(insert_query, [
                    ep_id,
                    generic_product_id,
                    establishment_id,
                    local_product_code,
                    local_description,
                    local_unit,
                    price_float,
                    price_float,
                    price_float,
                    price_float,
                    1,
                    now,
                    now,
                    now,
                    now
                ])
                
                return ep_id
            
        except Exception as e:
            self.logger.error(f"Error creating/updating establishment product: {e}")
            raise
    
    def get_generic_product_analytics(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get analytics for generic products"""
        
        try:
            conn = self.db_manager.get_connection("main_db")
            
            query = """
            SELECT * FROM v_generic_products_analytics
            ORDER BY total_occurrences DESC
            LIMIT ?
            """
            
            results = conn.execute(query, [limit]).fetchall()
            
            # Convert to dictionaries
            analytics = []
            columns = [desc[0] for desc in conn.description]
            
            for row in results:
                analytics.append(dict(zip(columns, row)))
            
            return analytics
            
        except Exception as e:
            self.logger.error(f"Error getting generic product analytics: {e}")
            return []