from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime
from decimal import Decimal


@dataclass
class EstablishmentData:
    """Data class for establishment/business information"""
    cnpj: str
    business_name: Optional[str] = None
    trade_name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    state_registration: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None


@dataclass
class ConsumerData:
    """Data class for consumer information"""
    cpf: Optional[str] = None
    name: Optional[str] = None
    email: Optional[str] = None
    final_consumer: Optional[str] = None  # "1 - Sim" or similar from NFCe


@dataclass
class ProductData:
    """Data class for individual product/item information"""
    item_number: int
    product_code: Optional[str] = None
    barcode: Optional[str] = None
    description: str = ""
    ncm_code: Optional[str] = None
    cest_code: Optional[str] = None
    cfop_code: Optional[str] = None
    unit: Optional[str] = None
    quantity: Optional[Decimal] = None
    unit_price: Optional[Decimal] = None
    total_amount: Optional[Decimal] = None
    discount_amount: Optional[Decimal] = None
    
    # Tax information
    icms_rate: Optional[Decimal] = None
    icms_amount: Optional[Decimal] = None
    pis_rate: Optional[Decimal] = None
    pis_amount: Optional[Decimal] = None
    cofins_rate: Optional[Decimal] = None
    cofins_amount: Optional[Decimal] = None


@dataclass
class TaxData:
    """Data class for tax information"""
    total_taxes: Optional[Decimal] = None
    icms_total: Optional[Decimal] = None
    pis_total: Optional[Decimal] = None
    cofins_total: Optional[Decimal] = None
    other_taxes: Optional[Decimal] = None


@dataclass
class InvoiceData:
    """Complete invoice data from web scraping"""
    # Basic identification (from QR code)
    access_key: str
    invoice_number: str
    series: str
    issue_date: Optional[datetime] = None
    authorization_date: Optional[datetime] = None
    
    # Financial information
    total_amount: Optional[Decimal] = None
    discount_amount: Optional[Decimal] = None
    products_amount: Optional[Decimal] = None
    
    # Status and environment
    status: Optional[str] = None
    environment: str = "2"  # Default to homologation
    
    # Related data
    establishment: Optional[EstablishmentData] = None
    consumer: Optional[ConsumerData] = None
    items: List[ProductData] = None
    taxes: Optional[TaxData] = None
    
    # Metadata
    source_url: str = ""
    scraped_at: Optional[datetime] = None
    scraping_success: bool = False
    scraping_errors: Optional[List[str]] = None
    
    def __post_init__(self):
        if self.items is None:
            self.items = []
        if self.scraping_errors is None:
            self.scraping_errors = []
    
    @property
    def full_invoice_number(self) -> str:
        """Get formatted full invoice number with series"""
        return f"{self.series}-{self.invoice_number}"
    
    @property
    def establishment_cnpj_formatted(self) -> str:
        """Get formatted CNPJ from establishment"""
        if self.establishment and self.establishment.cnpj:
            cnpj = self.establishment.cnpj
            if len(cnpj) == 14:
                return f"{cnpj[0:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
        return ""
    
    @property
    def items_count(self) -> int:
        """Get total number of items"""
        return len(self.items) if self.items else 0
    
    @property
    def has_consumer_info(self) -> bool:
        """Check if consumer information is available"""
        return (self.consumer is not None and 
                (self.consumer.cpf is not None or self.consumer.name is not None))
    
    def add_item(self, item: ProductData) -> None:
        """Add a product item to the invoice"""
        if self.items is None:
            self.items = []
        self.items.append(item)
    
    def add_error(self, error: str) -> None:
        """Add a scraping error"""
        if self.scraping_errors is None:
            self.scraping_errors = []
        self.scraping_errors.append(error)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "access_key": self.access_key,
            "invoice_number": self.invoice_number,
            "series": self.series,
            "full_invoice_number": self.full_invoice_number,
            "issue_date": self.issue_date.isoformat() if self.issue_date else None,
            "authorization_date": self.authorization_date.isoformat() if self.authorization_date else None,
            "total_amount": float(self.total_amount) if self.total_amount else None,
            "discount_amount": float(self.discount_amount) if self.discount_amount else None,
            "products_amount": float(self.products_amount) if self.products_amount else None,
            "status": self.status,
            "environment": self.environment,
            "establishment": self._establishment_to_dict(),
            "consumer": self._consumer_to_dict(),
            "items": [self._item_to_dict(item) for item in (self.items or [])],
            "taxes": self._taxes_to_dict(),
            "items_count": self.items_count,
            "has_consumer_info": self.has_consumer_info,
            "source_url": self.source_url,
            "scraped_at": self.scraped_at.isoformat() if self.scraped_at else None,
            "scraping_success": self.scraping_success,
            "scraping_errors": self.scraping_errors or []
        }
    
    def _establishment_to_dict(self) -> Optional[dict]:
        """Convert establishment to dict"""
        if not self.establishment:
            return None
        return {
            "cnpj": self.establishment.cnpj,
            "cnpj_formatted": self.establishment_cnpj_formatted,
            "business_name": self.establishment.business_name,
            "trade_name": self.establishment.trade_name,
            "address": self.establishment.address,
            "city": self.establishment.city,
            "state": self.establishment.state,
            "zip_code": self.establishment.zip_code,
            "state_registration": self.establishment.state_registration,
            "phone": self.establishment.phone,
            "email": self.establishment.email
        }
    
    def _consumer_to_dict(self) -> Optional[dict]:
        """Convert consumer to dict"""
        if not self.consumer:
            return None
        return {
            "cpf": self.consumer.cpf,
            "name": self.consumer.name,
            "email": self.consumer.email,
            "final_consumer": self.consumer.final_consumer
        }
    
    def _item_to_dict(self, item: ProductData) -> dict:
        """Convert product item to dict"""
        return {
            "item_number": item.item_number,
            "product_code": item.product_code,
            "barcode": item.barcode,
            "description": item.description,
            "ncm_code": item.ncm_code,
            "cest_code": item.cest_code,
            "cfop_code": item.cfop_code,
            "unit": item.unit,
            "quantity": float(item.quantity) if item.quantity else None,
            "unit_price": float(item.unit_price) if item.unit_price else None,
            "total_amount": float(item.total_amount) if item.total_amount else None,
            "discount_amount": float(item.discount_amount) if item.discount_amount else None,
            "icms_rate": float(item.icms_rate) if item.icms_rate else None,
            "icms_amount": float(item.icms_amount) if item.icms_amount else None,
            "pis_rate": float(item.pis_rate) if item.pis_rate else None,
            "pis_amount": float(item.pis_amount) if item.pis_amount else None,
            "cofins_rate": float(item.cofins_rate) if item.cofins_rate else None,
            "cofins_amount": float(item.cofins_amount) if item.cofins_amount else None
        }
    
    def _taxes_to_dict(self) -> Optional[dict]:
        """Convert taxes to dict"""
        if not self.taxes:
            return None
        return {
            "total_taxes": float(self.taxes.total_taxes) if self.taxes.total_taxes else None,
            "icms_total": float(self.taxes.icms_total) if self.taxes.icms_total else None,
            "pis_total": float(self.taxes.pis_total) if self.taxes.pis_total else None,
            "cofins_total": float(self.taxes.cofins_total) if self.taxes.cofins_total else None,
            "other_taxes": float(self.taxes.other_taxes) if self.taxes.other_taxes else None
        }


@dataclass
class ScrapingResult:
    """Result of scraping operation"""
    url: str
    success: bool
    invoice_data: Optional[InvoiceData] = None
    error_message: Optional[str] = None
    processing_time_ms: Optional[int] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            "url": self.url,
            "success": self.success,
            "invoice_data": self.invoice_data.to_dict() if self.invoice_data else None,
            "error_message": self.error_message,
            "processing_time_ms": self.processing_time_ms
        }