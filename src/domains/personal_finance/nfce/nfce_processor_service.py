#!/usr/bin/env python3
"""
NFCe Service - Business logic for processing Brazilian electronic invoices (NFCe)
"""

import json
import asyncio
import requests
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from decimal import Decimal

from utils.logging.logging_manager import LogManager
from utils.cache_manager.cache_manager import CacheManager
from utils.data.json_manager import JSONManager
from utils.file_manager import FileManager
from domains.personal_finance.nfce.utils.html_parser import NFCeDataExtractor
from domains.personal_finance.nfce.http_client import NFCeHttpClient
from domains.personal_finance.nfce.database.nfce_database_manager import (
    NFCeDatabaseManager
)
from domains.personal_finance.nfce.models.invoice_data import (
    InvoiceData, EstablishmentData, ConsumerData, ProductData, TaxData
)


class NFCeService:
    """Service for processing NFCe URLs and extracting invoice data"""
    
    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("NFCeService")
        self.cache = CacheManager.get_instance()
        self.extractor = NFCeDataExtractor()
        self.http_client = NFCeHttpClient()
        self._db_manager = None  # Lazy-loaded when needed
    
    @property
    def db_manager(self):
        """Lazy-loaded database manager"""
        if self._db_manager is None:
            self._db_manager = NFCeDatabaseManager()
        return self._db_manager
        
    def clear_cache(self) -> None:
        """Clear all cached data"""
        self.cache.clear_all()
        self.logger.info("Cache cleared")
    
    def process_single_url(
        self, 
        url: str, 
        timeout: int = 30, 
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Process a single NFCe URL and extract invoice data
        
        Args:
            url: NFCe URL to process
            timeout: Request timeout in seconds
            force_refresh: Force refresh ignoring cache
            
        Returns:
            Dictionary with processing results
        """
        try:
            # Validate input parameters
            if not url or not isinstance(url, str):
                raise ValueError("URL must be a non-empty string")
            
            url = url.strip()
            if not url.startswith(('http://', 'https://')):
                raise ValueError(f"Invalid URL format: {url}")
            
            if timeout <= 0:
                raise ValueError(f"Timeout must be positive: {timeout}")
            
            self.logger.info(f"Processing single URL: {url[:100]}...")
            
            # Validate Portal SPED URL
            if 'portalsped.fazenda.mg.gov.br' not in url:
                self.logger.warning("URL is not from Portal SPED MG - results may be unreliable")
            
            # Check cache first (unless force refresh)
            cache_key = f"nfce_url_{hash(url)}"
            if not force_refresh:
                try:
                    cached_result = self.cache.load(cache_key, expiration_minutes=60)
                    if cached_result:
                        self.logger.info("Using cached result")
                        return {
                            "total_processed": 1,
                            "successful": 1,
                            "failed": 0,
                            "invoices": [cached_result],
                            "cached": True
                        }
                except Exception as cache_error:
                    self.logger.warning(f"Cache lookup failed: {cache_error}")
            
            # Extract access key from URL
            try:
                access_key = self._extract_access_key_from_url(url)
                if not access_key:
                    self.logger.warning("Could not extract access key from URL")
            except Exception as e:
                self.logger.warning(f"Access key extraction failed: {e}")
                access_key = ""
            
            # Fetch HTML content with error handling
            try:
                html_content = self.http_client.fetch_nfce_page(url, timeout=timeout)
                if not html_content or len(html_content.strip()) < 100:
                    raise ValueError("Received empty or very short HTML content")
            except requests.exceptions.Timeout:
                raise ValueError(f"Request timeout after {timeout} seconds")
            except requests.exceptions.ConnectionError:
                raise ValueError("Connection error - check internet connection and URL")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    raise ValueError("NFCe page not found (404) - URL may be invalid or expired")
                elif e.response.status_code == 403:
                    raise ValueError("Access forbidden (403) - URL may be blocked or invalid")
                else:
                    raise ValueError(f"HTTP error {e.response.status_code}: {e}")
            except Exception as e:
                raise ValueError(f"Failed to fetch NFCe page: {str(e)}")
            
            # Extract invoice data with error handling
            try:
                invoice_data = self.extractor.extract_invoice_data(html_content, url)
                if not invoice_data:
                    raise ValueError("Extractor returned no data")
                
                # Set access key if extracted
                if access_key:
                    invoice_data.access_key = access_key
                
                # Validate critical fields
                if not invoice_data.invoice_number and not invoice_data.access_key:
                    self.logger.warning("No invoice number or access key found - data may be incomplete")
                
            except Exception as e:
                self.logger.error(f"Data extraction failed: {e}")
                # Create minimal invoice data for failed extraction
                from domains.personal_finance.nfce.models.invoice_data import InvoiceData
                invoice_data = InvoiceData(
                    access_key=access_key or "",
                    invoice_number="",
                    series="",
                    source_url=url,
                    scraping_success=False,
                    scraping_errors=[f"Extraction failed: {str(e)}"]
                )
            
            # Convert to dictionary with error handling
            try:
                invoice_dict = invoice_data.to_dict()
            except Exception as e:
                self.logger.error(f"Failed to convert invoice data to dict: {e}")
                invoice_dict = {
                    "access_key": getattr(invoice_data, 'access_key', ''),
                    "source_url": url,
                    "scraping_success": False,
                    "errors": [f"Data conversion failed: {str(e)}"]
                }
            
            # Cache the result (even if failed)
            try:
                self.cache.save(cache_key, invoice_dict)
            except Exception as cache_error:
                self.logger.warning(f"Failed to cache result: {cache_error}")
            
            # Build result
            is_successful = invoice_data.scraping_success if hasattr(invoice_data, 'scraping_success') else False
            result = {
                "total_processed": 1,
                "successful": 1 if is_successful else 0,
                "failed": 0 if is_successful else 1,
                "invoices": [invoice_dict] if is_successful else [],
                "cached": False
            }
            
            if not is_successful:
                result["errors"] = [{"url": url, "error": "Data extraction failed"}]
            
            status = "successfully" if is_successful else "with errors"
            self.logger.info(f"Processed URL {status}: {url[:50]}...")
            return result
            
        except ValueError as e:
            # Known validation/processing errors
            self.logger.error(f"Validation error for URL {url[:50]}...: {e}")
            return {
                "total_processed": 1,
                "successful": 0,
                "failed": 1,
                "invoices": [],
                "errors": [{"url": url, "error": str(e), "type": "validation_error"}]
            }
        except Exception as e:
            # Unexpected errors
            self.logger.error(f"Unexpected error processing URL {url[:50]}...: {e}", exc_info=True)
            return {
                "total_processed": 1,
                "successful": 0,
                "failed": 1,
                "invoices": [],
                "errors": [{"url": url, "error": f"Unexpected error: {str(e)}", "type": "unexpected_error"}]
            }
    
    def process_urls_from_file(
        self, 
        input_file: str, 
        batch_size: int = 10, 
        timeout: int = 30,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Process multiple NFCe URLs from a JSON file
        
        Args:
            input_file: Path to JSON file containing URLs
            batch_size: Number of concurrent requests
            timeout: Request timeout in seconds
            force_refresh: Force refresh ignoring cache
            
        Returns:
            Dictionary with processing results
        """
        try:
            self.logger.info(f"Processing URLs from file: {input_file}")
            
            # Validate input file
            FileManager.validate_file(input_file, allowed_extensions=[".json"])
            
            # Load URLs from file
            urls = self._load_urls_from_file(input_file)
            self.logger.info(f"Loaded {len(urls)} URLs to process")
            
            # Process URLs in batches
            results = self._process_urls_batch(
                urls, 
                batch_size=batch_size, 
                timeout=timeout,
                force_refresh=force_refresh
            )
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error processing URLs from file {input_file}: {e}")
            return {
                "total_processed": 0,
                "successful": 0,
                "failed": 0,
                "invoices": [],
                "errors": [{"file": input_file, "error": str(e)}]
            }
    
    def _load_urls_from_file(self, input_file: str) -> List[str]:
        """Load URLs from JSON file with robust error handling"""
        try:
            # Validate file exists and is readable
            if not input_file or not isinstance(input_file, str):
                raise ValueError("Input file path must be a non-empty string")
            
            # Read JSON data
            data = JSONManager.read_json(input_file)
            if data is None:
                raise ValueError(f"File {input_file} is empty or contains null data")
            
            urls = []
            
            # Support different JSON formats
            if isinstance(data, dict) and "urls" in data:
                # Format: {"urls": ["url1", "url2", ...]}
                if not isinstance(data["urls"], list):
                    raise ValueError("'urls' field must be an array")
                urls = data["urls"]
                
            elif isinstance(data, list):
                if len(data) == 0:
                    raise ValueError("Input file contains empty array")
                
                # Check if it's WhatsApp format with objects containing URLs
                if isinstance(data[0], dict):
                    # WhatsApp format: [{"content": "url", "urls": ["url"], ...}, ...]
                    for i, item in enumerate(data):
                        if not isinstance(item, dict):
                            self.logger.warning(f"Skipping invalid item at index {i}: not an object")
                            continue
                            
                        if "urls" in item and isinstance(item["urls"], list):
                            # Extract from urls array
                            valid_urls = [url for url in item["urls"] if isinstance(url, str) and url.strip()]
                            urls.extend(valid_urls)
                        elif "content" in item and isinstance(item["content"], str):
                            # Fallback: use content field if it's a URL
                            content = item["content"].strip()
                            if content.startswith("http"):
                                urls.append(content)
                            else:
                                self.logger.warning(f"Skipping invalid content at index {i}: not a URL")
                        else:
                            self.logger.warning(f"Skipping item at index {i}: no valid URL found")
                else:
                    # Direct array of strings: ["url1", "url2", ...]
                    for i, item in enumerate(data):
                        if isinstance(item, str) and item.strip():
                            urls.append(item.strip())
                        else:
                            self.logger.warning(f"Skipping invalid URL at index {i}: {item}")
            else:
                raise ValueError(
                    "Invalid JSON format. Expected one of:\n"
                    "1. Object with 'urls' array: {\"urls\": [\"url1\", \"url2\"]}\n"
                    "2. Direct array of URLs: [\"url1\", \"url2\"]\n"
                    "3. WhatsApp format: [{\"content\": \"url\", \"urls\": [\"url\"]}]"
                )
            
            # Validate extracted URLs
            if not urls:
                raise ValueError("No valid URLs found in input file")
            
            # Filter and validate URLs
            valid_urls = []
            for i, url in enumerate(urls):
                if not isinstance(url, str):
                    self.logger.warning(f"Skipping non-string URL at position {i}: {type(url)}")
                    continue
                
                url = url.strip()
                if not url:
                    self.logger.warning(f"Skipping empty URL at position {i}")
                    continue
                
                if not url.startswith(('http://', 'https://')):
                    self.logger.warning(f"Skipping invalid URL at position {i}: {url[:50]}...")
                    continue
                
                # Warn about non-Portal SPED URLs but still include them
                if 'portalsped.fazenda.mg.gov.br' not in url:
                    self.logger.warning(f"URL at position {i} is not from Portal SPED MG: {url[:50]}...")
                
                valid_urls.append(url)
            
            if not valid_urls:
                raise ValueError("No valid HTTP/HTTPS URLs found in input file")
            
            # Remove duplicates while preserving order
            seen = set()
            unique_urls = []
            for url in valid_urls:
                if url not in seen:
                    seen.add(url)
                    unique_urls.append(url)
                else:
                    self.logger.info(f"Removing duplicate URL: {url[:50]}...")
            
            self.logger.info(f"Loaded {len(unique_urls)} unique valid URLs from {input_file}")
            if len(unique_urls) != len(urls):
                self.logger.warning(f"Filtered out {len(urls) - len(unique_urls)} invalid/duplicate URLs")
            
            return unique_urls
            
        except (FileNotFoundError, PermissionError) as e:
            self.logger.error(f"File access error: {e}")
            raise
        except ValueError as e:
            self.logger.error(f"Invalid file format: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error loading URLs from file: {e}")
            raise ValueError(f"Failed to load URLs from {input_file}: {str(e)}")
    
    def _process_urls_batch(
        self, 
        urls: List[str], 
        batch_size: int = 10, 
        timeout: int = 30,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """Process URLs in batches using concurrent execution"""
        total_processed = 0
        successful = 0
        failed = 0
        all_invoices = []
        all_errors = []
        
        # Process URLs in batches
        for i in range(0, len(urls), batch_size):
            batch_urls = urls[i:i + batch_size]
            self.logger.info(f"Processing batch {i//batch_size + 1} ({len(batch_urls)} URLs)")
            
            # Use ThreadPoolExecutor for concurrent processing
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                # Submit all URLs in current batch
                future_to_url = {
                    executor.submit(
                        self._process_single_url_internal, 
                        url, 
                        timeout,
                        force_refresh
                    ): url 
                    for url in batch_urls
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    total_processed += 1
                    
                    try:
                        result = future.result()
                        if result.get("success", False):
                            successful += 1
                            all_invoices.append(result["invoice"])
                        else:
                            failed += 1
                            all_errors.append({
                                "url": url,
                                "error": result.get("error", "Unknown error")
                            })
                            
                    except Exception as e:
                        failed += 1
                        all_errors.append({"url": url, "error": str(e)})
                        self.logger.error(f"Exception processing {url}: {e}")
        
        result = {
            "total_processed": total_processed,
            "successful": successful,
            "failed": failed,
            "invoices": all_invoices,
            "processing_date": datetime.now().isoformat()
        }
        
        if all_errors:
            result["errors"] = all_errors
        
        self.logger.info(f"Batch processing completed: {successful}/{total_processed} successful")
        return result
    
    def _process_single_url_internal(
        self, 
        url: str, 
        timeout: int,
        force_refresh: bool
    ) -> Dict[str, Any]:
        """Internal method for processing a single URL (used in concurrent execution)"""
        try:
            # Check cache first
            cache_key = f"nfce_url_{hash(url)}"
            if not force_refresh:
                cached_result = self.cache.load(cache_key, expiration_minutes=60)
                if cached_result:
                    return {"success": True, "invoice": cached_result, "cached": True}
            
            # Extract access key
            access_key = self._extract_access_key_from_url(url)
            
            # Fetch and process
            html_content = self.http_client.fetch_nfce_page(url, timeout=timeout)
            invoice_data = self.extractor.extract_invoice_data(html_content, url)
            invoice_data.access_key = access_key
            
            # Convert to dict and cache
            invoice_dict = invoice_data.to_dict()
            self.cache.save(cache_key, invoice_dict)
            
            return {
                "success": invoice_data.scraping_success, 
                "invoice": invoice_dict,
                "cached": False
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _extract_access_key_from_url(self, url: str) -> str:
        """Extract access key from NFCe URL"""
        try:
            if "?p=" in url:
                param = url.split("?p=")[1]
                # URL decode if needed
                from urllib.parse import unquote
                param = unquote(param)
                # Split by | and get first part (access key)
                return param.split("|")[0] if "|" in param else param
            return ""
        except Exception as e:
            self.logger.warning(f"Could not extract access key from URL: {e}")
            return ""
    
    def _invoice_to_dict(self, invoice_data) -> Dict[str, Any]:
        """Convert InvoiceData object to dictionary"""
        try:
            invoice_dict = {
                "access_key": invoice_data.access_key,
                "invoice_number": invoice_data.invoice_number,
                "series": invoice_data.series,
                "issue_date": invoice_data.issue_date.isoformat() if invoice_data.issue_date else None,
                "total_amount": float(invoice_data.total_amount) if invoice_data.total_amount else None,
                "products_amount": float(invoice_data.products_amount) if invoice_data.products_amount else None,
                "discount_amount": float(invoice_data.discount_amount) if invoice_data.discount_amount else None,
                "source_url": invoice_data.source_url,
                "scraped_at": invoice_data.scraped_at.isoformat(),
                "scraping_success": invoice_data.scraping_success,
                "errors": invoice_data.errors
            }
            
            # Add establishment data
            if invoice_data.establishment:
                invoice_dict["establishment"] = {
                    "business_name": invoice_data.establishment.business_name,
                    "trade_name": invoice_data.establishment.trade_name,
                    "cnpj": invoice_data.establishment.cnpj,
                    "state_registration": invoice_data.establishment.state_registration,
                    "state": invoice_data.establishment.state,
                    "city": invoice_data.establishment.city,
                    "address": invoice_data.establishment.address
                }
            
            # Add consumer data
            if invoice_data.consumer:
                invoice_dict["consumer"] = {
                    "name": invoice_data.consumer.name,
                    "cpf": invoice_data.consumer.cpf,
                    "final_consumer": invoice_data.consumer.final_consumer
                }
            
            # Add items
            if invoice_data.items:
                invoice_dict["items"] = []
                for item in invoice_data.items:
                    item_dict = {
                        "item_number": item.item_number,
                        "product_code": item.product_code,
                        "description": item.description,
                        "quantity": float(item.quantity) if item.quantity else None,
                        "unit": item.unit,
                        "unit_price": float(item.unit_price) if item.unit_price else None,
                        "total_amount": float(item.total_amount) if item.total_amount else None,
                        "barcode": item.barcode
                    }
                    invoice_dict["items"].append(item_dict)
            
            # Add tax data
            if invoice_data.taxes:
                invoice_dict["taxes"] = {
                    "icms_total": float(invoice_data.taxes.icms_total) if invoice_data.taxes.icms_total else None,
                    "pis_total": float(invoice_data.taxes.pis_total) if invoice_data.taxes.pis_total else None,
                    "cofins_total": float(invoice_data.taxes.cofins_total) if invoice_data.taxes.cofins_total else None
                }
            
            return invoice_dict
            
        except Exception as e:
            self.logger.error(f"Error converting invoice to dict: {e}")
            return {
                "access_key": getattr(invoice_data, 'access_key', ''),
                "scraping_success": False,
                "errors": [str(e)]
            }
    
    def generate_analysis_report(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate analysis report from processing results"""
        invoices = results.get("invoices", [])
        
        if not invoices:
            return {"message": "No invoices to analyze"}
        
        # Basic statistics
        total_amount = sum(
            inv.get("total_amount", 0) for inv in invoices 
            if inv.get("total_amount") is not None
        )
        
        # Group by establishment
        establishments = {}
        for invoice in invoices:
            est = invoice.get("establishment", {})
            cnpj = est.get("cnpj", "Unknown")
            if cnpj not in establishments:
                establishments[cnpj] = {
                    "business_name": est.get("business_name", "Unknown"),
                    "count": 0,
                    "total_amount": 0
                }
            establishments[cnpj]["count"] += 1
            establishments[cnpj]["total_amount"] += invoice.get("total_amount", 0)
        
        # Items analysis
        total_items = sum(
            len(inv.get("items", [])) for inv in invoices
        )
        
        analysis = {
            "summary": {
                "total_invoices": len(invoices),
                "total_amount": round(total_amount, 2),
                "average_amount": round(total_amount / len(invoices), 2) if invoices else 0,
                "total_items": total_items,
                "average_items_per_invoice": round(total_items / len(invoices), 2) if invoices else 0
            },
            "establishments": establishments,
            "processing_stats": {
                "total_processed": results.get("total_processed", 0),
                "successful": results.get("successful", 0),
                "failed": results.get("failed", 0),
                "success_rate": round(
                    (results.get("successful", 0) / results.get("total_processed", 1)) * 100, 2
                )
            }
        }
        
        return analysis
    
    def _dict_to_invoice_data(self, invoice_dict: Dict[str, Any]) -> InvoiceData:
        """Convert dictionary back to InvoiceData object"""
        try:
            # Parse dates
            issue_date = None
            if invoice_dict.get("issue_date"):
                issue_date = datetime.fromisoformat(invoice_dict["issue_date"].replace('Z', '+00:00'))
            
            scraped_at = None
            if invoice_dict.get("scraped_at"):
                scraped_at = datetime.fromisoformat(invoice_dict["scraped_at"].replace('Z', '+00:00'))
            
            # Create establishment object
            establishment = None
            if invoice_dict.get("establishment"):
                est_data = invoice_dict["establishment"]
                establishment = EstablishmentData(
                    cnpj=est_data.get("cnpj", ""),
                    business_name=est_data.get("business_name"),
                    trade_name=est_data.get("trade_name"),
                    address=est_data.get("address"),
                    city=est_data.get("city"),
                    state=est_data.get("state"),
                    zip_code=est_data.get("zip_code"),
                    state_registration=est_data.get("state_registration"),
                    phone=est_data.get("phone"),
                    email=est_data.get("email")
                )
            
            # Create consumer object
            consumer = None
            if invoice_dict.get("consumer"):
                cons_data = invoice_dict["consumer"]
                consumer = ConsumerData(
                    cpf=cons_data.get("cpf"),
                    name=cons_data.get("name"),
                    email=cons_data.get("email"),
                    final_consumer=cons_data.get("final_consumer")
                )
            
            # Create items
            items = []
            if invoice_dict.get("items"):
                for i, item_data in enumerate(invoice_dict["items"]):
                    item = ProductData(
                        item_number=item_data.get("item_number", i + 1),
                        product_code=item_data.get("product_code"),
                        barcode=item_data.get("barcode"),
                        description=item_data.get("description", ""),
                        ncm_code=item_data.get("ncm_code"),
                        cest_code=item_data.get("cest_code"),
                        cfop_code=item_data.get("cfop_code"),
                        unit=item_data.get("unit"),
                        quantity=Decimal(str(item_data["quantity"])) if item_data.get("quantity") else None,
                        unit_price=Decimal(str(item_data["unit_price"])) if item_data.get("unit_price") else None,
                        total_amount=Decimal(str(item_data["total_amount"])) if item_data.get("total_amount") else None,
                        discount_amount=Decimal(str(item_data["discount_amount"])) if item_data.get("discount_amount") else None
                    )
                    items.append(item)
            
            # Create taxes object
            taxes = None
            if invoice_dict.get("taxes"):
                tax_data = invoice_dict["taxes"]
                taxes = TaxData(
                    icms_total=Decimal(str(tax_data["icms_total"])) if tax_data.get("icms_total") else None,
                    pis_total=Decimal(str(tax_data["pis_total"])) if tax_data.get("pis_total") else None,
                    cofins_total=Decimal(str(tax_data["cofins_total"])) if tax_data.get("cofins_total") else None
                )
            
            # Create main invoice object
            invoice_data = InvoiceData(
                access_key=invoice_dict.get("access_key", ""),
                invoice_number=invoice_dict.get("invoice_number", ""),
                series=invoice_dict.get("series", ""),
                issue_date=issue_date,
                total_amount=Decimal(str(invoice_dict["total_amount"])) if invoice_dict.get("total_amount") else None,
                discount_amount=Decimal(str(invoice_dict["discount_amount"])) if invoice_dict.get("discount_amount") else None,
                products_amount=Decimal(str(invoice_dict["products_amount"])) if invoice_dict.get("products_amount") else None,
                establishment=establishment,
                consumer=consumer,
                items=items,
                taxes=taxes,
                source_url=invoice_dict.get("source_url", ""),
                scraped_at=scraped_at,
                scraping_success=invoice_dict.get("scraping_success", False),
                scraping_errors=invoice_dict.get("errors", [])
            )
            
            return invoice_data
            
        except Exception as e:
            self.logger.error(f"Error converting dict to InvoiceData: {e}")
            # Return minimal valid object
            return InvoiceData(
                access_key=invoice_dict.get("access_key", ""),
                invoice_number=invoice_dict.get("invoice_number", ""),
                series=invoice_dict.get("series", ""),
                scraping_success=False,
                scraping_errors=[f"Conversion error: {str(e)}"]
            )
    
    def save_to_database(self, results: Dict[str, Any]) -> None:
        """Save processing results to database"""
        try:
            invoices = results.get("invoices", [])
            self.logger.info(f"Attempting to save {len(invoices)} invoices to database")
            
            if not invoices:
                self.logger.warning("No invoices to save to database")
                return
            
            saved_count = 0
            for i, invoice_dict in enumerate(invoices):
                try:
                    # Convert dict back to InvoiceData object
                    invoice_data = self._dict_to_invoice_data(invoice_dict)
                    
                    if not invoice_data.access_key:
                        self.logger.warning(f"Invoice {i+1} has no access_key, skipping database save")
                        continue
                    
                    result = self.db_manager.store_invoice_data(invoice_data)
                    
                    if result:
                        saved_count += 1
                    else:
                        self.logger.warning(f"Invoice {i+1} with access_key {invoice_data.access_key[-10:]}... was skipped (likely duplicate)")
                        
                except Exception as e:
                    self.logger.error(f"Error saving invoice {i+1} to database: {e}", exc_info=True)
            
            self.logger.info(f"Saved {saved_count}/{len(invoices)} invoices to database")
            
        except Exception as e:
            self.logger.error(f"Error saving to database: {e}", exc_info=True)
    
    def import_existing_data(self, data_file: str) -> Dict[str, Any]:
        """Import existing processed NFCe data directly to database"""
        try:
            self.logger.info(f"Importing existing data from file: {data_file}")
            
            # Validate file exists
            FileManager.validate_file(data_file, allowed_extensions=[".json"])
            
            # Load the data
            with open(data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract invoices array
            invoices = data.get('invoices', [])
            if not invoices:
                self.logger.warning("No invoices found in data file")
                return {
                    "total_processed": 0,
                    "successful": 0,
                    "failed": 0,
                    "invoices": [],
                    "errors": ["No invoices found in data file"]
                }
            
            self.logger.info(f"Found {len(invoices)} invoices to import")
            
            # Save to database (always save when importing)
            self.logger.info("Saving imported data to database")
            saved_count = 0
            errors = []
            
            for i, invoice_dict in enumerate(invoices):
                try:
                    # Convert dict to InvoiceData object
                    invoice_data = self._dict_to_invoice_data(invoice_dict)
                    
                    if not invoice_data.access_key:
                        self.logger.warning(f"Invoice {i+1} has no access_key, skipping")
                        errors.append(f"Invoice {i+1}: Missing access_key")
                        continue
                    
                    # Store in database
                    result = self.db_manager.store_invoice_data(invoice_data)
                    
                    if result:
                        saved_count += 1
                        self.logger.debug(f"Imported invoice {i+1}: {invoice_data.access_key[-10:]}...")
                    else:
                        self.logger.info(f"Invoice {i+1} with access_key {invoice_data.access_key[-10:]}... already exists, skipped")
                        
                except Exception as e:
                    error_msg = f"Invoice {i+1}: {str(e)}"
                    errors.append(error_msg)
                    self.logger.error(f"Error importing invoice {i+1}: {e}")
            
            result = {
                "total_processed": len(invoices),
                "successful": saved_count,
                "failed": len(invoices) - saved_count,
                "invoices": invoices,
                "errors": errors,
                "import_mode": True
            }
            
            self.logger.info(f"Import completed: {saved_count}/{len(invoices)} invoices imported to database")
            return result
            
        except Exception as e:
            self.logger.error(f"Error importing existing data: {e}", exc_info=True)
            raise
    
    def save_results(self, results: Dict[str, Any], output_file: str) -> None:
        """Save processing results to JSON file"""
        try:
            # Ensure output directory exists
            import os
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                self.logger.info(f"Created output directory: {output_dir}")
            
            JSONManager.write_json(results, output_file)
            self.logger.info(f"Results saved to {output_file}")
        except Exception as e:
            self.logger.error(f"Error saving results to file: {e}")
            raise
    
    def print_summary(self, results: Dict[str, Any]) -> None:
        """Print processing summary to console"""
        print("\n" + "="*60)
        print("NFCe Processing Summary")
        print("="*60)
        
        print(f"Total Processed: {results.get('total_processed', 0)}")
        print(f"Successful: {results.get('successful', 0)}")
        print(f"Failed: {results.get('failed', 0)}")
        
        if results.get('total_processed', 0) > 0:
            success_rate = (results.get('successful', 0) / results.get('total_processed', 1)) * 100
            print(f"Success Rate: {success_rate:.1f}%")
        
        invoices = results.get('invoices', [])
        if invoices:
            total_amount = sum(
                inv.get('total_amount', 0) for inv in invoices 
                if inv.get('total_amount') is not None
            )
            print(f"Total Amount: R$ {total_amount:.2f}")
            print(f"Average Amount: R$ {total_amount/len(invoices):.2f}")
        
        if results.get('errors'):
            print(f"\nErrors: {len(results['errors'])}")
            for error in results['errors'][:3]:  # Show first 3 errors
                print(f"  - {error.get('url', 'Unknown')}: {error.get('error', 'Unknown error')}")
            if len(results['errors']) > 3:
                print(f"  ... and {len(results['errors']) - 3} more errors")
        
        print("="*60)