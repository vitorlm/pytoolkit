import os
import hashlib
import uuid
from typing import List, Dict, Optional, Any, Tuple
from decimal import Decimal
from datetime import datetime
from pathlib import Path

from utils.logging.logging_manager import LogManager
from utils.data.duckdb_manager import DuckDBManager
from domains.personal_finance.nfce.models.invoice_data import (
    InvoiceData, EstablishmentData, ProductData, ConsumerData, TaxData
)


class NFCeDatabaseManager:
    """
    Database manager for NFCe data storage and retrieval
    Handles all database operations with duplicate prevention and data integrity
    """
    
    def __init__(self, database_path: str = "data/nfce_processor.duckdb"):
        """
        Initialize NFCe database manager
        
        Args:
            database_path: Path to DuckDB database file
        """
        self.logger = LogManager.get_instance().get_logger("NFCeDatabaseManager")
        self.database_path = database_path
        
        # Ensure database directory exists
        os.makedirs(os.path.dirname(database_path), exist_ok=True)
        
        # Initialize DuckDB manager
        self.db_manager = DuckDBManager()
        self.db_manager.add_connection_config({
            "name": "nfce_db",
            "path": database_path,
            "read_only": False
        })
        
        # Initialize schema
        self._initialize_schema()
        
        # Statistics
        self.stats = {
            'invoices_inserted': 0,
            'invoices_updated': 0,
            'invoices_skipped': 0,
            'establishments_inserted': 0,
            'establishments_updated': 0,
            'products_inserted': 0,
            'products_updated': 0,
            'items_inserted': 0
        }
    
    def _initialize_schema(self) -> None:
        """Initialize database schema if not exists"""
        try:
            self.logger.info("Initializing NFCe database schema")
            
            # Read schema file
            schema_path = Path(__file__).parent / "schema.sql"
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            # Execute schema creation
            conn = self.db_manager.get_connection("nfce_db")
            
            # Execute the entire schema as one batch
            try:
                # Remove comments and empty lines for cleaner execution
                clean_sql = '\n'.join([line for line in schema_sql.split('\n') 
                                     if line.strip() and not line.strip().startswith('--')])
                conn.execute(clean_sql)
                self.logger.info("Schema executed successfully as batch")
            except Exception as e:
                self.logger.error(f"Batch schema execution failed: {e}")
                raise
            
            conn.commit()
            self.logger.info("Database schema initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing database schema: {e}")
            raise
    
    def store_invoice_data(self, invoice_data: InvoiceData) -> bool:
        """
        Store complete invoice data with all related entities
        
        Args:
            invoice_data: Complete invoice data object
            
        Returns:
            True if stored successfully, False if skipped (duplicate)
        """
        try:
            conn = self.db_manager.get_connection("nfce_db")
            
            # Start transaction
            conn.begin()
            
            try:
                # Check if invoice already exists
                if self._invoice_exists(conn, invoice_data.access_key):
                    self.logger.debug(f"Invoice {invoice_data.access_key} already exists, skipping")
                    self.stats['invoices_skipped'] += 1
                    conn.rollback()
                    return False
                
                # Store establishment first (required for foreign key)
                establishment_id = None
                if invoice_data.establishment:
                    establishment_id = self._store_establishment(conn, invoice_data.establishment)
                
                # If no establishment or establishment couldn't be stored (empty CNPJ), create minimal one
                if establishment_id is None:
                    # Create minimal establishment from access key to satisfy foreign key constraint
                    cnpj_from_key = invoice_data.access_key[6:20] if invoice_data.access_key and len(invoice_data.access_key) >= 20 else "00000000000000"
                    
                    # Create minimal EstablishmentData object
                    from domains.personal_finance.nfce.models.invoice_data import EstablishmentData
                    minimal_establishment = EstablishmentData(
                        cnpj=cnpj_from_key,
                        business_name="[Nome não informado]",
                        trade_name=None,
                        address=None,
                        city=None,
                        state=None,
                        zip_code=None,
                        state_registration=None,
                        phone=None,
                        email=None
                    )
                    establishment_id = self._store_establishment(conn, minimal_establishment)
                
                # Store/update products and get product IDs
                product_mappings = {}
                if invoice_data.items:
                    for item in invoice_data.items:
                        product_id = self._store_product(conn, item, establishment_id)
                        if product_id:
                            # Map item to product ID for later use
                            key = (item.barcode or "", item.product_code or "", item.description)
                            product_mappings[key] = product_id
                
                # Store main invoice record
                invoice_id = self._store_invoice(conn, invoice_data, establishment_id)
                
                # Store invoice items
                if invoice_data.items:
                    self._store_invoice_items(conn, invoice_data, product_mappings)
                
                # Store tax information
                if invoice_data.taxes:
                    self._store_tax_information(conn, invoice_data)
                
                # Log successful processing
                self._log_processing_result(conn, invoice_data, "success", None, 0)
                
                # Commit transaction
                conn.commit()
                
                self.stats['invoices_inserted'] += 1
                self.logger.info(f"Successfully stored invoice {invoice_data.access_key}")
                return True
                
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Error storing invoice {invoice_data.access_key}: {e}", exc_info=True)
                
                # Log failed processing
                try:
                    self._log_processing_result(conn, invoice_data, "error", str(e), 0)
                    conn.commit()
                except Exception as log_error:
                    self.logger.error(f"Error logging processing result: {log_error}")
                
                # Don't re-raise, just return False
                return False
                
        except Exception as e:
            self.logger.error(f"Database error storing invoice: {e}", exc_info=True)
            return False
    
    def _invoice_exists(self, conn, access_key: str) -> bool:
        """Check if invoice already exists in database"""
        result = conn.execute(
            "SELECT COUNT(*) FROM invoices WHERE access_key = ?",
            [access_key]
        ).fetchone()
        return result[0] > 0
    
    def _store_establishment(self, conn, establishment: EstablishmentData) -> Optional[str]:
        """Store or update establishment data"""
        if not establishment.cnpj or not establishment.cnpj.strip():
            self.logger.warning(f"Establishment has empty CNPJ, cannot store: {establishment.business_name}")
            return None
        
        try:
            # Check if establishment exists
            existing = conn.execute(
                "SELECT id FROM establishments WHERE cnpj = ?",
                [establishment.cnpj]
            ).fetchone()
            
            if existing:
                # Update existing establishment
                establishment_id = existing[0]
                conn.execute("""
                    UPDATE establishments SET
                        cnpj_formatted = COALESCE(?, cnpj_formatted),
                        business_name = COALESCE(?, business_name),
                        trade_name = COALESCE(?, trade_name),
                        address = COALESCE(?, address),
                        city = COALESCE(?, city),
                        state = COALESCE(?, state),
                        zip_code = COALESCE(?, zip_code),
                        state_registration = COALESCE(?, state_registration),
                        phone = COALESCE(?, phone),
                        email = COALESCE(?, email),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, [
                    getattr(establishment, 'cnpj_formatted', None),
                    establishment.business_name,
                    establishment.trade_name,
                    establishment.address,
                    establishment.city,
                    establishment.state,
                    establishment.zip_code,
                    establishment.state_registration,
                    establishment.phone,
                    establishment.email,
                    establishment_id
                ])
                self.stats['establishments_updated'] += 1
                
            else:
                # Insert new establishment
                establishment_id = self._generate_id()
                conn.execute("""
                    INSERT INTO establishments (
                        id, cnpj, cnpj_formatted, business_name, trade_name,
                        address, city, state, zip_code, state_registration,
                        phone, email
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    establishment_id,
                    establishment.cnpj,
                    getattr(establishment, 'cnpj_formatted', None),
                    establishment.business_name,
                    establishment.trade_name,
                    establishment.address,
                    establishment.city,
                    establishment.state,
                    establishment.zip_code,
                    establishment.state_registration,
                    establishment.phone,
                    establishment.email
                ])
                self.stats['establishments_inserted'] += 1
            
            return establishment_id
            
        except Exception as e:
            self.logger.error(f"Error storing establishment {establishment.cnpj}: {e}")
            raise
    
    def _store_product(self, conn, item: ProductData, establishment_id: str) -> Optional[str]:
        """Store or update product data with deduplication within the same establishment"""
        if not item.description:
            return None
        
        try:
            # Create unique identifier for product within establishment
            barcode = item.barcode or ""
            product_code = item.product_code or ""
            description = item.description
            
            # Check if product exists within the same establishment (based on product_code primarily)
            # Products with the same code within the same establishment are considered the same product
            existing = conn.execute("""
                SELECT id FROM products 
                WHERE establishment_id = ? AND product_code = ?
            """, [establishment_id, product_code]).fetchone()
            
            if existing:
                # Update existing product (increment occurrence count, update last seen)
                product_id = existing[0]
                conn.execute("""
                    UPDATE products SET
                        ncm_code = COALESCE(?, ncm_code),
                        cest_code = COALESCE(?, cest_code),
                        cfop_code = COALESCE(?, cfop_code),
                        unit = COALESCE(?, unit),
                        last_seen_date = CURRENT_TIMESTAMP,
                        occurrence_count = occurrence_count + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, [
                    item.ncm_code,
                    item.cest_code,
                    item.cfop_code,
                    item.unit,
                    product_id
                ])
                self.stats['products_updated'] += 1
                
            else:
                # Insert new product
                product_id = self._generate_id()
                conn.execute("""
                    INSERT INTO products (
                        id, establishment_id, product_code, barcode, description, ncm_code,
                        cest_code, cfop_code, unit
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    product_id,
                    establishment_id,
                    product_code or None,
                    barcode or None,
                    description,
                    item.ncm_code,
                    item.cest_code,
                    item.cfop_code,
                    item.unit
                ])
                self.stats['products_inserted'] += 1
            
            return product_id
            
        except Exception as e:
            self.logger.error(f"Error storing product {item.description}: {e}", exc_info=True)
            raise
    
    def _store_invoice(self, conn, invoice_data: InvoiceData, establishment_id: str) -> str:
        """Store main invoice record"""
        try:
            invoice_id = self._generate_id()
            
            # Get CNPJ from establishment using establishment_id
            establishment_cnpj = conn.execute(
                "SELECT cnpj FROM establishments WHERE id = ?",
                [establishment_id]
            ).fetchone()[0]
            
            # Calculate full invoice number
            full_invoice_number = None
            if invoice_data.series and invoice_data.invoice_number:
                full_invoice_number = f"{invoice_data.series}-{invoice_data.invoice_number}"
            
            # Convert scraping errors to JSON array string
            scraping_errors = None
            if invoice_data.scraping_errors:
                scraping_errors = str(invoice_data.scraping_errors)
            
            conn.execute("""
                INSERT INTO invoices (
                    id, access_key, invoice_number, series, full_invoice_number,
                    model, issuer_cnpj, state_code, year_month, emission_form,
                    numeric_code, check_digit, issue_date, authorization_date,
                    environment, total_amount, discount_amount, products_amount,
                    tax_amount, consumer_cpf, consumer_name, consumer_email,
                    original_url, validation_hash, processing_status,
                    has_consumer_info, items_count, scraped_at, scraping_success,
                    scraping_errors
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                invoice_id,
                invoice_data.access_key,
                invoice_data.invoice_number,
                invoice_data.series,
                full_invoice_number,
                "65",  # NFCe model
                establishment_cnpj,
                invoice_data.access_key[:2] if invoice_data.access_key else None,  # State code from access key
                invoice_data.access_key[2:6] if invoice_data.access_key else None,  # YYMM from access key
                "1",  # Normal emission
                invoice_data.access_key[35:43] if invoice_data.access_key else None,  # Numeric code
                invoice_data.access_key[43:44] if invoice_data.access_key else None,  # Check digit
                invoice_data.issue_date,
                invoice_data.authorization_date,
                invoice_data.environment,
                float(invoice_data.total_amount) if invoice_data.total_amount else None,
                float(invoice_data.discount_amount) if invoice_data.discount_amount else None,
                float(invoice_data.products_amount) if invoice_data.products_amount else None,
                None,  # Tax amount - calculated later
                invoice_data.consumer.cpf if invoice_data.consumer else None,
                invoice_data.consumer.name if invoice_data.consumer else None,
                invoice_data.consumer.email if invoice_data.consumer else None,
                invoice_data.source_url,
                None,  # Validation hash - would need to be extracted from QR
                "completed" if invoice_data.scraping_success else "failed",
                invoice_data.has_consumer_info,
                invoice_data.items_count,
                invoice_data.scraped_at,
                invoice_data.scraping_success,
                scraping_errors
            ])
            
            return invoice_id
            
        except Exception as e:
            self.logger.error(f"Error storing invoice {invoice_data.access_key}: {e}", exc_info=True)
            raise
    
    def _store_invoice_items(self, conn, invoice_data: InvoiceData, product_mappings: Dict[Tuple, int]) -> None:
        """Store invoice line items"""
        if not invoice_data.items:
            return
        
        try:
            for item in invoice_data.items:
                item_id = self._generate_id()
                
                # Find product ID from mappings
                key = (item.barcode or "", item.product_code or "", item.description)
                product_id = product_mappings.get(key)
                
                conn.execute("""
                    INSERT INTO invoice_items (
                        id, access_key, item_number, product_id, product_code,
                        barcode, description, ncm_code, cest_code, cfop_code,
                        unit, quantity, unit_price, total_amount, discount_amount,
                        icms_rate, icms_amount, pis_rate, pis_amount,
                        cofins_rate, cofins_amount
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    item_id,
                    invoice_data.access_key,
                    item.item_number,
                    product_id,
                    item.product_code,
                    item.barcode,
                    item.description,
                    item.ncm_code,
                    item.cest_code,
                    item.cfop_code,
                    item.unit,
                    float(item.quantity) if item.quantity else None,
                    float(item.unit_price) if item.unit_price else None,
                    float(item.total_amount) if item.total_amount else None,
                    float(item.discount_amount) if item.discount_amount else None,
                    None,  # ICMS rate - would need extraction
                    None,  # ICMS amount
                    None,  # PIS rate
                    None,  # PIS amount  
                    None,  # COFINS rate
                    None   # COFINS amount
                ])
                
                self.stats['items_inserted'] += 1
                
        except Exception as e:
            self.logger.error(f"Error storing invoice items: {e}")
            raise
    
    def _store_tax_information(self, conn, invoice_data: InvoiceData) -> None:
        """Store tax information if available"""
        if not invoice_data.taxes:
            return
        
        try:
            tax_id = self._generate_id()
            
            conn.execute("""
                INSERT INTO tax_information (
                    id, access_key, total_taxes, icms_total, pis_total, cofins_total
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, [
                tax_id,
                invoice_data.access_key,
                float(invoice_data.taxes.total_taxes) if invoice_data.taxes.total_taxes else None,
                float(invoice_data.taxes.icms_total) if invoice_data.taxes.icms_total else None,
                float(invoice_data.taxes.pis_total) if invoice_data.taxes.pis_total else None,
                float(invoice_data.taxes.cofins_total) if invoice_data.taxes.cofins_total else None
            ])
            
        except Exception as e:
            self.logger.error(f"Error storing tax information: {e}")
            raise
    
    def _log_processing_result(self, conn, invoice_data: InvoiceData, status: str, 
                              error_message: Optional[str], processing_time_ms: int) -> None:
        """Log processing result for audit trail"""
        try:
            log_id = self._generate_id()
            
            conn.execute("""
                INSERT INTO processing_log (
                    id, access_key, url, status, error_message, processing_time_ms
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, [
                log_id,
                invoice_data.access_key,
                invoice_data.source_url,
                status,
                error_message,
                processing_time_ms
            ])
            
        except Exception as e:
            self.logger.error(f"Error logging processing result: {e}")
            # Don't raise - this is just for audit
    
    def _generate_id(self) -> str:
        """Generate unique ID using UUID4"""
        return str(uuid.uuid4())
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            conn = self.db_manager.get_connection("nfce_db")
            
            # Get table counts
            tables_stats = {}
            tables = ['establishments', 'invoices', 'products', 'invoice_items', 'processing_log']
            
            for table in tables:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                tables_stats[f"{table}_count"] = result[0]
            
            # Get processing statistics
            processing_stats = conn.execute("""
                SELECT 
                    status,
                    COUNT(*) as count
                FROM processing_log 
                GROUP BY status
            """).fetchall()
            
            processing_counts = {row[0]: row[1] for row in processing_stats}
            
            # Get date range
            date_range = conn.execute("""
                SELECT 
                    MIN(issue_date) as earliest_date,
                    MAX(issue_date) as latest_date
                FROM invoices 
                WHERE issue_date IS NOT NULL
            """).fetchone()
            
            return {
                **tables_stats,
                **processing_counts,
                'earliest_date': date_range[0] if date_range[0] else None,
                'latest_date': date_range[1] if date_range[1] else None,
                **self.stats
            }
            
        except Exception as e:
            self.logger.error(f"Error getting statistics: {e}")
            return self.stats
    
    def get_duplicate_invoices(self) -> List[str]:
        """Get list of duplicate invoice access keys"""
        try:
            conn = self.db_manager.get_connection("nfce_db")
            
            duplicates = conn.execute("""
                SELECT access_key 
                FROM invoices 
                GROUP BY access_key 
                HAVING COUNT(*) > 1
            """).fetchall()
            
            return [row[0] for row in duplicates]
            
        except Exception as e:
            self.logger.error(f"Error finding duplicates: {e}")
            return []
    
    def cleanup_duplicates(self) -> int:
        """Remove duplicate invoices, keeping the most recent"""
        try:
            conn = self.db_manager.get_connection("nfce_db")
            
            result = conn.execute("""
                DELETE FROM invoices 
                WHERE id NOT IN (
                    SELECT MIN(id) 
                    FROM invoices 
                    GROUP BY access_key
                )
            """)
            
            removed_count = result.rowcount if hasattr(result, 'rowcount') else 0
            conn.commit()
            
            self.logger.info(f"Removed {removed_count} duplicate invoices")
            return removed_count
            
        except Exception as e:
            self.logger.error(f"Error cleaning up duplicates: {e}")
            return 0
    
    def close(self) -> None:
        """Close database connections"""
        try:
            self.db_manager.close_all_connections()
            self.logger.info("Database connections closed")
        except Exception as e:
            self.logger.error(f"Error closing database: {e}")