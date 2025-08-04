import os
import hashlib
import uuid
from typing import List, Dict, Optional, Any, Tuple
from decimal import Decimal
from datetime import datetime
from pathlib import Path

from utils.logging.logging_manager import LogManager
from utils.data.duckdb_manager import DuckDBManager
from domains.personal_finance.nfce.utils.cnae_classifier import CNAEClassifier
from domains.personal_finance.nfce.utils.cnpj_relationship_detector import CNPJRelationshipDetector
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
        self.cnae_classifier = CNAEClassifier()
        
        # Cache em memória para evitar consultas CNAE duplicadas na mesma execução
        self._cnae_session_cache = {}
        
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
                
                # Tax information removed in simplified schema
                # if invoice_data.taxes:
                #     pass  # Removed in simplified schema
                
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
        
        # Normalizar CNPJ para evitar duplicações
        normalized_cnpj = self._normalize_cnpj(establishment.cnpj)
        if not normalized_cnpj:
            self.logger.warning(f"Invalid CNPJ format: {establishment.cnpj}")
            return None
        
        establishment.cnpj = normalized_cnpj
        
        try:
            # Check if establishment exists
            existing = conn.execute(
                "SELECT id, establishment_type, cnae_code FROM establishments WHERE cnpj = ?",
                [establishment.cnpj]
            ).fetchone()
            
            # Get CNAE classification for new establishments or if missing
            establishment_type = None
            cnae_code = None
            
            if not existing or not existing[1]:  # New establishment or missing type
                # Verificar cache de sessão primeiro
                if establishment.cnpj in self._cnae_session_cache:
                    self.logger.info(f"Using session cache for CNPJ: {establishment.cnpj}")
                    cnae_data = self._cnae_session_cache[establishment.cnpj]
                else:
                    # Verificar se existe cache para a matriz da empresa (mesma raiz CNPJ)
                    cnpj_root = establishment.cnpj[:8]  # Primeiros 8 dígitos (raiz da empresa)
                    cached_root_data = None
                    
                    # Primeiro verificar session cache
                    for cached_cnpj, cached_data in self._cnae_session_cache.items():
                        if cached_cnpj.startswith(cnpj_root) and cached_data is not None:
                            self.logger.info(f"Using related company session cache for CNPJ: {establishment.cnpj} (from {cached_cnpj})")
                            cached_root_data = cached_data
                            break
                    
                    # Se não encontrou no session cache, verificar no banco de dados
                    if not cached_root_data:
                        db_related = conn.execute(
                            "SELECT cnae_code, establishment_type FROM establishments WHERE cnpj LIKE ? AND cnae_code IS NOT NULL AND establishment_type != 'Outros' LIMIT 1",
                            [f"{cnpj_root}%"]
                        ).fetchone()
                        
                        if db_related:
                            self.logger.info(f"Using related company database data for CNPJ: {establishment.cnpj} (CNAE: {db_related[0]})")
                            # Criar objeto similar ao retornado pela API
                            cached_root_data = {
                                'cnae_principal': db_related[0],
                                'establishment_type': db_related[1]
                            }
                    
                    if cached_root_data:
                        cnae_data = cached_root_data
                        # Salvar no cache de sessão para este CNPJ específico
                        self._cnae_session_cache[establishment.cnpj] = cnae_data
                    else:
                        self.logger.info(f"Getting CNAE classification for CNPJ: {establishment.cnpj}")
                        cnae_data = self.cnae_classifier.get_establishment_info(establishment.cnpj)
                        
                        # Salvar no cache de sessão
                        self._cnae_session_cache[establishment.cnpj] = cnae_data
                
                if cnae_data:
                    establishment_type = cnae_data.get('establishment_type', 'Outros')
                    cnae_code = cnae_data.get('cnae_principal')
                    
                    # Update establishment data with API data if more complete
                    if cnae_data.get('business_name') and not establishment.business_name:
                        establishment.business_name = cnae_data['business_name']
                    if cnae_data.get('address') and not establishment.address:
                        establishment.address = cnae_data['address']
                    if cnae_data.get('city') and not establishment.city:
                        establishment.city = cnae_data['city']
                    if cnae_data.get('state') and not establishment.state:
                        establishment.state = cnae_data['state']
                else:
                    establishment_type = 'Outros'
                    self.logger.warning(f"Could not classify establishment type for CNPJ: {establishment.cnpj} - API returned None")
            
            if existing:
                # Update existing establishment
                establishment_id = existing[0]
                current_type = existing[1] or establishment_type
                current_cnae = existing[2] or cnae_code
                
                conn.execute("""
                    UPDATE establishments SET
                        business_name = COALESCE(?, business_name),
                        establishment_type = COALESCE(?, establishment_type),
                        address = COALESCE(?, address),
                        city = COALESCE(?, city),
                        state = COALESCE(?, state),
                        cnae_code = COALESCE(?, cnae_code)
                    WHERE id = ?
                """, [
                    establishment.business_name,
                    current_type,
                    establishment.address,
                    establishment.city,
                    establishment.state,
                    current_cnae,
                    establishment_id
                ])
                self.stats['establishments_updated'] += 1
                
            else:
                # Insert new establishment
                establishment_id = self._generate_id()
                
                # Calculate CNPJ components for relationship tracking
                cnpj_root = establishment.cnpj[:8]  # First 8 digits identify the company
                branch_number = establishment.cnpj[8:12]  # Branch number (0001 = main office)
                is_main_office = branch_number == '0001'
                company_group_id = cnpj_root  # Use cnpj_root as group identifier
                
                conn.execute("""
                    INSERT INTO establishments (
                        id, cnpj, business_name, establishment_type, address, city, state, cnae_code,
                        cnpj_root, branch_number, is_main_office, company_group_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    establishment_id,
                    establishment.cnpj,
                    establishment.business_name,
                    establishment_type,
                    establishment.address,
                    establishment.city,
                    establishment.state,
                    cnae_code,
                    cnpj_root,
                    branch_number,
                    is_main_office,
                    company_group_id
                ])
                self.stats['establishments_inserted'] += 1
                
                # Create or update company group
                business_name_str = establishment.business_name or establishment.cnpj  # Fallback to CNPJ if no name
                self._manage_company_group(conn, cnpj_root, business_name_str, establishment.cnpj, is_main_office)
                
                self.logger.info(f"Stored new establishment: {establishment.business_name} ({establishment_type}) - {'MATRIZ' if is_main_office else 'FILIAL'}")
            
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
                # Update existing product (increment occurrence count only)
                product_id = existing[0]
                conn.execute("""
                    UPDATE products SET
                        unit = COALESCE(?, unit),
                        occurrence_count = occurrence_count + 1
                    WHERE id = ?
                """, [
                    item.unit,
                    product_id
                ])
                self.stats['products_updated'] += 1
                
            else:
                # Insert new product
                product_id = self._generate_id()
                conn.execute("""
                    INSERT INTO products (
                        id, establishment_id, product_code, description, unit
                    ) VALUES (?, ?, ?, ?, ?)
                """, [
                    product_id,
                    establishment_id,
                    product_code or None,
                    description,
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
                    id, access_key, invoice_number, series, issuer_cnpj, 
                    issue_date, total_amount, items_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                invoice_id,
                invoice_data.access_key,
                invoice_data.invoice_number,
                invoice_data.series,
                establishment_cnpj,
                invoice_data.issue_date,
                float(invoice_data.total_amount) if invoice_data.total_amount else None,
                invoice_data.items_count
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
                        id, access_key, product_id, quantity, unit_price, total_amount
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, [
                    item_id,
                    invoice_data.access_key,
                    product_id,
                    float(item.quantity) if item.quantity else None,
                    float(item.unit_price) if item.unit_price else None,
                    float(item.total_amount) if item.total_amount else None
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
    
    def _normalize_cnpj(self, cnpj: str) -> Optional[str]:
        """Normalize CNPJ to prevent duplicates"""
        if not cnpj:
            return None
        
        # Remove tudo que não for dígito
        clean = ''.join(filter(str.isdigit, cnpj))
        
        # Normalizar para 14 dígitos
        if len(clean) == 14:
            return clean
        elif len(clean) < 14:
            # Preencher com zeros à esquerda
            normalized = clean.zfill(14)
            self.logger.info(f"CNPJ padded: {cnpj} -> {normalized}")
            return normalized
        elif len(clean) > 14:
            # Se tem mais de 14 dígitos, pegar apenas os primeiros 14
            normalized = clean[:14]
            self.logger.warning(f"CNPJ truncated: {cnpj} -> {normalized}")
            return normalized
        else:
            return None
    
    def _manage_company_group(self, conn, cnpj_root: str, business_name: str, full_cnpj: str, is_main_office: bool):
        """Manage company group entries in the database"""
        try:
            # Check if company group already exists  
            result = conn.execute("""
                SELECT id FROM company_groups WHERE id = ?
            """, [cnpj_root]).fetchone()
            
            if not result:
                # Create new company group
                company_name = business_name.split(' - ')[0]  # Take main business name before any "-"
                
                conn.execute("""
                    INSERT INTO company_groups (
                        id, company_name, main_office_cnpj, total_establishments
                    ) VALUES (?, ?, ?, ?)
                """, [
                    cnpj_root,  # Use cnpj_root as the id
                    company_name,
                    full_cnpj if is_main_office else None,
                    1
                ])
                
                self.logger.info(f"Created new company group: {company_name} (CNPJ root: {cnpj_root})")
            else:
                # Update existing company group
                update_sql = "UPDATE company_groups SET updated_at = CURRENT_TIMESTAMP"
                update_values = []
                
                # Update main establishment if this is a main office
                if is_main_office:
                    update_sql += ", main_office_cnpj = ?"
                    update_values.append(full_cnpj)
                
                # Update establishment count
                update_sql += """, total_establishments = (
                    SELECT COUNT(DISTINCT cnpj) FROM establishments WHERE cnpj_root = ?
                ) WHERE id = ?"""
                update_values.extend([cnpj_root, cnpj_root])
                
                conn.execute(update_sql, update_values)
                
        except Exception as e:
            self.logger.error(f"Error managing company group for {cnpj_root}: {e}")
    
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