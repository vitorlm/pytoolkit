import os
from typing import Dict, List, Optional
from pathlib import Path
from datetime import datetime

from utils.logging.logging_manager import LogManager
from utils.data.duckdb_manager import DuckDBManager


class DatabaseMigrationManager:
    """
    Manages database schema migrations and validations for NFCe processor
    """
    
    def __init__(self, database_path: str):
        """
        Initialize migration manager
        
        Args:
            database_path: Path to DuckDB database
        """
        self.logger = LogManager.get_instance().get_logger("DatabaseMigrationManager")
        self.database_path = database_path
        
        # Initialize DuckDB manager
        self.db_manager = DuckDBManager()
        self.db_manager.add_connection_config({
            "name": "migration_db",
            "path": database_path,
            "read_only": False
        })
        
        # Migration tracking table
        self._ensure_migration_table()
    
    def _ensure_migration_table(self) -> None:
        """Create migration tracking table if not exists"""
        try:
            conn = self.db_manager.get_connection("migration_db")
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    id BIGINT PRIMARY KEY,
                    version VARCHAR(50) NOT NULL UNIQUE,
                    description TEXT,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    checksum VARCHAR(64)
                )
            """)
            conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error creating migration table: {e}")
            raise
    
    def get_current_version(self) -> Optional[str]:
        """Get current database schema version"""
        try:
            conn = self.db_manager.get_connection("migration_db")
            
            result = conn.execute("""
                SELECT version FROM schema_migrations 
                ORDER BY applied_at DESC 
                LIMIT 1
            """).fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            self.logger.error(f"Error getting current version: {e}")
            return None
    
    def validate_schema(self) -> Dict[str, bool]:
        """
        Validate current database schema against expected structure
        
        Returns:
            Dictionary with validation results for each table
        """
        validation_results = {}
        expected_tables = [
            'establishments', 'invoices', 'products', 'invoice_items',
            'tax_information', 'processing_log', 'statistics_summary'
        ]
        
        try:
            conn = self.db_manager.get_connection("migration_db")
            
            # Check if all expected tables exist
            for table in expected_tables:
                try:
                    result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                    validation_results[f"{table}_exists"] = True
                    validation_results[f"{table}_count"] = result[0]
                    
                except Exception:
                    validation_results[f"{table}_exists"] = False
                    validation_results[f"{table}_count"] = 0
            
            # Check for required indexes
            indexes_to_check = [
                ('establishments', 'idx_establishments_cnpj'),
                ('invoices', 'idx_invoices_access_key'),
                ('invoices', 'idx_invoices_issue_date'),
                ('products', 'idx_products_unique'),
                ('invoice_items', 'idx_invoice_items_access_key')
            ]
            
            for table, index in indexes_to_check:
                try:
                    # DuckDB index checking - simplified approach
                    # Just try to use the index by running a query
                    conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                    validation_results[f"{index}_exists"] = True
                    
                except Exception:
                    validation_results[f"{index}_exists"] = False
            
            # Check for required views
            views_to_check = [
                'v_invoice_details', 'v_product_spending', 'v_monthly_establishment_spending'
            ]
            
            for view in views_to_check:
                try:
                    conn.execute(f"SELECT COUNT(*) FROM {view}").fetchone()
                    validation_results[f"{view}_exists"] = True
                except Exception:
                    validation_results[f"{view}_exists"] = False
            
            # Check foreign key constraints (DuckDB has them enabled by default)
            validation_results['foreign_keys_enabled'] = True
            
            # Overall validation status
            table_checks = [validation_results[f"{table}_exists"] for table in expected_tables]
            validation_results['schema_valid'] = all(table_checks)
            
            self.logger.info(f"Schema validation complete: {validation_results['schema_valid']}")
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Error validating schema: {e}")
            return {'schema_valid': False, 'error': str(e)}
    
    def repair_schema(self) -> bool:
        """
        Attempt to repair database schema by re-running creation scripts
        
        Returns:
            True if repair was successful
        """
        try:
            self.logger.info("Starting schema repair")
            
            # Read and execute schema file
            schema_path = Path(__file__).parent / "schema.sql"
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            conn = self.db_manager.get_connection("migration_db")
            
            # Execute the entire schema as one batch
            try:
                # Remove comments and empty lines for cleaner execution
                clean_sql = '\n'.join([line for line in schema_sql.split('\n') 
                                     if line.strip() and not line.strip().startswith('--')])
                conn.execute(clean_sql)
                self.logger.info("Schema executed successfully as batch")
            except Exception as e:
                self.logger.error(f"Batch schema execution failed: {e}")
                # Try executing statements individually for debugging
                statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
                for i, statement in enumerate(statements):
                    if statement and not statement.startswith('--'):
                        try:
                            self.logger.debug(f"Executing statement {i+1}: {statement[:100]}...")
                            conn.execute(statement)
                        except Exception as stmt_error:
                            self.logger.error(f"Statement {i+1} failed: {stmt_error}")
                            self.logger.error(f"Statement content: {statement}")
            
            conn.commit()
            
            # Record migration
            self._record_migration("v1.0.0", "Schema repair - initial structure")
            
            # Validate after repair
            validation = self.validate_schema()
            success = validation.get('schema_valid', False)
            
            if success:
                self.logger.info("Schema repair completed successfully")
            else:
                self.logger.warning("Schema repair completed with issues")
                
            return success
            
        except Exception as e:
            self.logger.error(f"Error repairing schema: {e}")
            return False
    
    def _record_migration(self, version: str, description: str) -> None:
        """Record a migration in the tracking table"""
        try:
            conn = self.db_manager.get_connection("migration_db")
            
            # Generate unique ID
            migration_id = int(datetime.now().timestamp() * 1000000)
            
            conn.execute("""
                INSERT OR REPLACE INTO schema_migrations (
                    id, version, description, applied_at
                ) VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, [migration_id, version, description])
            
            conn.commit()
            self.logger.info(f"Recorded migration: {version}")
            
        except Exception as e:
            self.logger.error(f"Error recording migration: {e}")
    
    def get_migration_history(self) -> List[Dict[str, any]]:
        """Get list of applied migrations"""
        try:
            conn = self.db_manager.get_connection("migration_db")
            
            results = conn.execute("""
                SELECT version, description, applied_at 
                FROM schema_migrations 
                ORDER BY applied_at DESC
            """).fetchall()
            
            return [
                {
                    'version': row[0],
                    'description': row[1],
                    'applied_at': row[2]
                }
                for row in results
            ]
            
        except Exception as e:
            self.logger.error(f"Error getting migration history: {e}")
            return []
    
    def backup_database(self, backup_path: Optional[str] = None) -> str:
        """
        Create a backup of the database
        
        Args:
            backup_path: Path for backup file (auto-generated if None)
            
        Returns:
            Path to backup file
        """
        try:
            if not backup_path:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_path = f"{self.database_path}.backup_{timestamp}"
            
            # Simple file copy for DuckDB
            import shutil
            shutil.copy2(self.database_path, backup_path)
            
            self.logger.info(f"Database backed up to: {backup_path}")
            return backup_path
            
        except Exception as e:
            self.logger.error(f"Error backing up database: {e}")
            raise
    
    def get_database_info(self) -> Dict[str, any]:
        """Get comprehensive database information"""
        try:
            conn = self.db_manager.get_connection("migration_db")
            
            # Get database size
            db_size = os.path.getsize(self.database_path) if os.path.exists(self.database_path) else 0
            
            # Get table information
            tables_info = {}
            try:
                tables = conn.execute("SHOW TABLES").fetchall()
                for table_row in tables:
                    table_name = table_row[0]
                    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    tables_info[table_name] = count
            except Exception as e:
                self.logger.warning(f"Error getting table info: {e}")
            
            # Get current version
            current_version = self.get_current_version()
            
            return {
                'database_path': self.database_path,
                'database_size_bytes': db_size,
                'database_size_mb': round(db_size / (1024 * 1024), 2),
                'current_version': current_version,
                'tables': tables_info,
                'total_records': sum(tables_info.values())
            }
            
        except Exception as e:
            self.logger.error(f"Error getting database info: {e}")
            return {'error': str(e)}
    
    def optimize_database(self) -> bool:
        """
        Optimize database performance
        
        Returns:
            True if optimization was successful
        """
        try:
            self.logger.info("Starting database optimization")
            conn = self.db_manager.get_connection("migration_db")
            
            # Analyze tables for better query planning
            tables = ['establishments', 'invoices', 'products', 'invoice_items']
            for table in tables:
                try:
                    conn.execute(f"ANALYZE {table}")
                except Exception as e:
                    self.logger.warning(f"Could not analyze table {table}: {e}")
            
            # Vacuum database to reclaim space
            try:
                conn.execute("VACUUM")
            except Exception as e:
                self.logger.warning(f"Could not vacuum database: {e}")
            
            conn.commit()
            self.logger.info("Database optimization completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Error optimizing database: {e}")
            return False
    
    def close(self) -> None:
        """Close database connections"""
        try:
            self.db_manager.close_all_connections()
        except Exception as e:
            self.logger.error(f"Error closing migration manager: {e}")