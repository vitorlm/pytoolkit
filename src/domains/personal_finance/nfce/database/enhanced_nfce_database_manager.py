#!/usr/bin/env python3
"""
Enhanced NFCe Database Manager - Integrates generic product management with similarity detection
"""

import os
import uuid
from typing import List, Dict, Optional, Any, Tuple
from decimal import Decimal
from datetime import datetime
from pathlib import Path

from utils.logging.logging_manager import LogManager
from utils.data.duckdb_manager import DuckDBManager
from domains.personal_finance.nfce.utils.cnae_classifier import CNAEClassifier
from domains.personal_finance.nfce.utils.cnpj_relationship_detector import CNPJRelationshipDetector
from domains.personal_finance.nfce.database.generic_product_manager import GenericProductManager
from domains.personal_finance.nfce.models.invoice_data import (
    InvoiceData, EstablishmentData, ProductData, ConsumerData, TaxData
)


class EnhancedNFCeDatabaseManager:
    """
    Enhanced NFCe database manager with generic product management and automatic deduplication
    
    Key changes from original:
    - Products are now generic across establishments
    - Automatic similarity-based product deduplication
    - Relationship tracking between invoices and generic products
    - Price comparison and analytics across establishments
    """
    
    def __init__(self, 
                 database_path: str = "data/nfce_processor.duckdb",
                 similarity_threshold: float = 0.60,
                 use_sbert: bool = False,
                 sbert_model: str = "paraphrase-multilingual-MiniLM-L12-v2"):
        """
        Initialize enhanced NFCe database manager
        
        Args:
            database_path: Path to DuckDB database file
            similarity_threshold: Threshold for product similarity matching
            use_sbert: Whether to use SBERT embeddings for similarity
            sbert_model: SBERT model name
        """
        
        self.logger = LogManager.get_instance().get_logger("EnhancedNFCeDatabaseManager")
        self.database_path = database_path
        self.cnae_classifier = CNAEClassifier()
        
        # Cache em memória para evitar consultas CNAE duplicadas na mesma execução
        self._cnae_session_cache = {}
        
        # Ensure database directory exists
        os.makedirs(os.path.dirname(database_path), exist_ok=True)
        
        # Initialize DuckDB manager
        self.db_manager = DuckDBManager()
        self.db_manager.add_connection_config({
            "name": "main_db",
            "path": database_path,
            "read_only": False
        })
        
        # Initialize schema with new structure
        self._initialize_enhanced_schema()
        
        # Initialize generic product manager
        self.generic_product_manager = GenericProductManager(
            db_manager=self.db_manager,
            similarity_threshold=similarity_threshold,
            use_sbert=use_sbert,
            sbert_model=sbert_model
        )
        
        # Statistics
        self.stats = {
            'invoices_inserted': 0,
            'invoices_updated': 0,
            'invoices_skipped': 0,
            'establishments_inserted': 0,
            'establishments_updated': 0,
            'generic_products_created': 0,
            'generic_products_matched': 0,
            'establishment_products_created': 0,
            'establishment_products_updated': 0,
            'items_inserted': 0,
            'similarity_matches': 0,
            'exact_matches': 0
        }
    
    def _initialize_enhanced_schema(self) -> None:
        """Initialize enhanced database schema with generic products"""
        
        try:
            self.logger.info("Initializing enhanced NFCe database schema")
            
            # Read new schema file
            schema_path = Path(__file__).parent / "new_schema.sql"
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            # Execute schema creation
            conn = self.db_manager.get_connection("main_db")
            
            # Execute schema statements
            try:
                # Split into individual statements and execute
                statements = [stmt.strip() for stmt in schema_sql.split(';') 
                            if stmt.strip() and not stmt.strip().startswith('--')]
                
                for statement in statements:
                    if statement:
                        conn.execute(statement)
                
                conn.commit()
                self.logger.info("Enhanced database schema initialized successfully")
                
            except Exception as e:
                self.logger.error(f"Enhanced schema execution failed: {e}")
                raise
                
        except Exception as e:
            self.logger.error(f"Error initializing enhanced database schema: {e}")
            raise
    
    def store_invoice_data(self, invoice_data: InvoiceData) -> bool:
        """
        Store complete invoice data with generic product management
        
        Args:
            invoice_data: Complete invoice data object
            
        Returns:
            True if stored successfully, False otherwise
        """
        
        try:
            self.logger.debug(f"Storing invoice data: {invoice_data.access_key}")
            
            # Start transaction
            conn = self.db_manager.get_connection("main_db")
            conn.execute("BEGIN TRANSACTION")
            
            try:
                # 1. Store/update establishment
                establishment_id = self._store_establishment(invoice_data.establishment)
                
                # 2. Store/update invoice
                invoice_stored = self._store_invoice(invoice_data, establishment_id)
                
                if not invoice_stored:
                    self.logger.debug(f"Invoice {invoice_data.access_key} already exists, skipping")
                    conn.execute("ROLLBACK")
                    self.stats['invoices_skipped'] += 1
                    return True
                
                # 3. Store items with generic product management
                items_stored = self._store_items_with_generic_products(
                    invoice_data.access_key, 
                    invoice_data.items, 
                    establishment_id
                )
                
                if items_stored:
                    conn.execute("COMMIT")
                    self.stats['invoices_inserted'] += 1
                    self.logger.debug(f"Successfully stored invoice: {invoice_data.access_key}")
                    return True
                else:
                    conn.execute("ROLLBACK")
                    self.logger.error(f"Failed to store items for invoice: {invoice_data.access_key}")
                    return False
                    
            except Exception as e:
                conn.execute("ROLLBACK")
                self.logger.error(f"Error in transaction for invoice {invoice_data.access_key}: {e}")
                raise
                
        except Exception as e:
            self.logger.error(f"Error storing invoice data: {e}")
            return False
    
    def _store_establishment(self, establishment: EstablishmentData) -> str:
        """Store or update establishment data (unchanged logic)"""
        
        try:
            conn = self.db_manager.get_connection("main_db")
            
            # Check if establishment already exists
            check_query = "SELECT id FROM establishments WHERE cnpj = ?"
            existing = conn.execute(check_query, [establishment.cnpj]).fetchone()
            
            if existing:
                establishment_id = existing[0]
                self.logger.debug(f"Establishment {establishment.cnpj} already exists with ID: {establishment_id}")
                return establishment_id
            
            # Create new establishment
            establishment_id = str(uuid.uuid4())
            
            # Extract CNPJ components for relationship tracking
            cnpj_root = establishment.cnpj[:8] if establishment.cnpj else ""
            branch_number = establishment.cnpj[8:12] if len(establishment.cnpj) >= 12 else "0001"
            is_main_office = branch_number == "0001"
            
            # Classify establishment type using CNAE
            establishment_type = self._get_establishment_type(establishment.cnae_code)
            
            insert_query = """
            INSERT INTO establishments (
                id, cnpj, business_name, establishment_type, address, city, state, 
                cnae_code, cnpj_root, branch_number, is_main_office, company_group_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            conn.execute(insert_query, [
                establishment_id,
                establishment.cnpj,
                establishment.business_name,
                establishment_type,
                establishment.address,
                establishment.city,
                establishment.state,
                establishment.cnae_code,
                cnpj_root,
                branch_number,
                is_main_office,
                cnpj_root,  # company_group_id same as cnpj_root
                datetime.now()
            ])
            
            self.stats['establishments_inserted'] += 1
            self.logger.debug(f"Created new establishment: {establishment_id} - {establishment.business_name}")
            
            return establishment_id
            
        except Exception as e:
            self.logger.error(f"Error storing establishment: {e}")
            raise
    
    def _store_invoice(self, invoice_data: InvoiceData, establishment_id: str) -> bool:
        """Store invoice data (unchanged logic)"""
        
        try:
            conn = self.db_manager.get_connection("main_db")
            
            # Check if invoice already exists
            check_query = "SELECT id FROM invoices WHERE access_key = ?"
            existing = conn.execute(check_query, [invoice_data.access_key]).fetchone()
            
            if existing:
                return False  # Invoice already exists
            
            # Insert new invoice
            invoice_id = str(uuid.uuid4())
            
            insert_query = """
            INSERT INTO invoices (
                id, access_key, invoice_number, series, issuer_cnpj, 
                issue_date, total_amount, items_count, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            conn.execute(insert_query, [
                invoice_id,
                invoice_data.access_key,
                invoice_data.invoice_number,
                invoice_data.series,
                invoice_data.establishment.cnpj,
                invoice_data.issue_date,
                float(invoice_data.total_amount) if invoice_data.total_amount else 0,
                len(invoice_data.items),
                datetime.now()
            ])
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error storing invoice: {e}")
            raise
    
    def _store_items_with_generic_products(self, access_key: str, items: List[ProductData], 
                                         establishment_id: str) -> bool:
        """Store invoice items using generic product management"""
        
        try:
            conn = self.db_manager.get_connection("main_db")
            
            for item in items:
                # Find or create generic product
                match_result = self.generic_product_manager.find_or_create_generic_product(
                    description=item.description,
                    establishment_id=establishment_id,
                    unit_price=item.unit_price,
                    unit=item.unit,
                    product_code=getattr(item, 'product_code', None)
                )
                
                # Update statistics
                if match_result.is_new_product:
                    self.stats['generic_products_created'] += 1
                elif match_result.match_method == 'exact':
                    self.stats['exact_matches'] += 1
                elif match_result.match_method == 'similarity':
                    self.stats['similarity_matches'] += 1
                    self.stats['generic_products_matched'] += 1
                
                # Update generic product statistics
                self.generic_product_manager.update_generic_product_statistics(
                    match_result.generic_product_id,
                    establishment_id,
                    item.unit_price
                )
                
                # Create or update establishment product
                establishment_product_id = self.generic_product_manager.create_or_update_establishment_product(
                    generic_product_id=match_result.generic_product_id,
                    establishment_id=establishment_id,
                    local_description=item.description,
                    unit_price=item.unit_price,
                    local_product_code=getattr(item, 'product_code', None),
                    local_unit=item.unit
                )
                
                # Insert invoice item with reference to generic product
                item_id = str(uuid.uuid4())
                
                insert_query = """
                INSERT INTO invoice_items (
                    id, access_key, generic_product_id, establishment_product_id,
                    original_description, quantity, unit_price, total_amount,
                    similarity_match_score, match_confidence, is_manual_match, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                conn.execute(insert_query, [
                    item_id,
                    access_key,
                    match_result.generic_product_id,
                    establishment_product_id,
                    item.description,
                    float(item.quantity) if item.quantity else 0,
                    float(item.unit_price) if item.unit_price else 0,
                    float(item.total_price) if item.total_price else 0,
                    match_result.similarity_score,
                    match_result.confidence_score,
                    False,  # Not manual match
                    datetime.now()
                ])
                
                self.stats['items_inserted'] += 1
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error storing items with generic products: {e}")
            raise
    
    def _get_establishment_type(self, cnae_code: Optional[str]) -> str:
        """Get establishment type from CNAE code with caching"""
        
        if not cnae_code:
            return "Não classificado"
        
        # Check session cache first
        if cnae_code in self._cnae_session_cache:
            return self._cnae_session_cache[cnae_code]
        
        try:
            classification = self.cnae_classifier.classify_cnae(cnae_code)
            establishment_type = classification.get('main_activity', 'Não classificado')
            
            # Cache the result
            self._cnae_session_cache[cnae_code] = establishment_type
            
            return establishment_type
            
        except Exception as e:
            self.logger.warning(f"Error classifying CNAE {cnae_code}: {e}")
            self._cnae_session_cache[cnae_code] = "Não classificado"
            return "Não classificado"
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics"""
        return self.stats.copy()
    
    def get_generic_product_analytics(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get analytics for generic products"""
        return self.generic_product_manager.get_generic_product_analytics(limit)
    
    def get_price_comparison_report(self, product_limit: int = 50) -> List[Dict[str, Any]]:
        """Get price comparison report across establishments"""
        
        try:
            conn = self.db_manager.get_connection("main_db")
            
            query = """
            SELECT * FROM v_price_comparison
            ORDER BY price_diff_percent DESC
            LIMIT ?
            """
            
            results = conn.execute(query, [product_limit]).fetchall()
            
            # Convert to dictionaries
            report = []
            columns = [desc[0] for desc in conn.description]
            
            for row in results:
                report.append(dict(zip(columns, row)))
            
            return report
            
        except Exception as e:
            self.logger.error(f"Error generating price comparison report: {e}")
            return []
    
    def get_similarity_quality_report(self) -> List[Dict[str, Any]]:
        """Get similarity matching quality report"""
        
        try:
            conn = self.db_manager.get_connection("main_db")
            
            query = "SELECT * FROM v_similarity_quality ORDER BY total_matches DESC"
            
            results = conn.execute(query).fetchall()
            
            # Convert to dictionaries
            report = []
            columns = [desc[0] for desc in conn.description]
            
            for row in results:
                report.append(dict(zip(columns, row)))
            
            return report
            
        except Exception as e:
            self.logger.error(f"Error generating similarity quality report: {e}")
            return []
    
    def get_spending_summary_with_generic_products(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get spending summary using generic products view"""
        
        try:
            conn = self.db_manager.get_connection("main_db")
            
            query = """
            SELECT * FROM v_spending_summary
            ORDER BY total_spent DESC
            LIMIT ?
            """
            
            results = conn.execute(query, [limit]).fetchall()
            
            # Convert to dictionaries
            summary = []
            columns = [desc[0] for desc in conn.description]
            
            for row in results:
                summary.append(dict(zip(columns, row)))
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting spending summary: {e}")
            return []
    
    def close(self):
        """Close database connections"""
        try:
            if hasattr(self.db_manager, 'close'):
                self.db_manager.close()
            self.logger.info("Database connections closed")
        except Exception as e:
            self.logger.warning(f"Error closing database connections: {e}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()