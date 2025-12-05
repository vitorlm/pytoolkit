from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class EstablishmentData:
    """Data class for establishment/business information"""

    cnpj: str
    business_name: str | None = None
    trade_name: str | None = None
    address: str | None = None
    city: str | None = None
    state: str | None = None
    zip_code: str | None = None
    state_registration: str | None = None
    phone: str | None = None
    email: str | None = None


@dataclass
class ConsumerData:
    """Data class for consumer information"""

    cpf: str | None = None
    name: str | None = None
    email: str | None = None
    final_consumer: str | None = None  # "1 - Sim" or similar from NFCe


@dataclass
class ProductData:
    """Data class for individual product/item information"""

    item_number: int
    product_code: str | None = None
    barcode: str | None = None
    description: str = ""
    ncm_code: str | None = None
    cest_code: str | None = None
    cfop_code: str | None = None
    unit: str | None = None
    quantity: Decimal | None = None
    unit_price: Decimal | None = None
    total_amount: Decimal | None = None
    discount_amount: Decimal | None = None

    # Tax information
    icms_rate: Decimal | None = None
    icms_amount: Decimal | None = None
    pis_rate: Decimal | None = None
    pis_amount: Decimal | None = None
    cofins_rate: Decimal | None = None
    cofins_amount: Decimal | None = None


@dataclass
class TaxData:
    """Data class for tax information"""

    total_taxes: Decimal | None = None
    icms_total: Decimal | None = None
    pis_total: Decimal | None = None
    cofins_total: Decimal | None = None
    other_taxes: Decimal | None = None


@dataclass
class InvoiceData:
    """Complete invoice data from web scraping"""

    # Basic identification (from QR code)
    access_key: str
    invoice_number: str
    series: str
    issue_date: datetime | None = None
    authorization_date: datetime | None = None

    # Financial information
    total_amount: Decimal | None = None
    discount_amount: Decimal | None = None
    products_amount: Decimal | None = None

    # Status and environment
    status: str | None = None
    environment: str = "2"  # Default to homologation

    # Related data
    establishment: EstablishmentData | None = None
    consumer: ConsumerData | None = None
    items: list[ProductData] = None
    taxes: TaxData | None = None

    # Metadata
    source_url: str = ""
    scraped_at: datetime | None = None
    scraping_success: bool = False
    scraping_errors: list[str] | None = None

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
        return self.consumer is not None and (self.consumer.cpf is not None or self.consumer.name is not None)

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
            "scraping_errors": self.scraping_errors or [],
        }

    def _establishment_to_dict(self) -> dict | None:
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
            "email": self.establishment.email,
        }

    def _consumer_to_dict(self) -> dict | None:
        """Convert consumer to dict"""
        if not self.consumer:
            return None
        return {
            "cpf": self.consumer.cpf,
            "name": self.consumer.name,
            "email": self.consumer.email,
            "final_consumer": self.consumer.final_consumer,
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
            "cofins_amount": float(item.cofins_amount) if item.cofins_amount else None,
        }

    def _taxes_to_dict(self) -> dict | None:
        """Convert taxes to dict"""
        if not self.taxes:
            return None
        return {
            "total_taxes": float(self.taxes.total_taxes) if self.taxes.total_taxes else None,
            "icms_total": float(self.taxes.icms_total) if self.taxes.icms_total else None,
            "pis_total": float(self.taxes.pis_total) if self.taxes.pis_total else None,
            "cofins_total": float(self.taxes.cofins_total) if self.taxes.cofins_total else None,
            "other_taxes": float(self.taxes.other_taxes) if self.taxes.other_taxes else None,
        }


@dataclass
class ScrapingResult:
    """Result of scraping operation"""

    url: str
    success: bool
    invoice_data: InvoiceData | None = None
    error_message: str | None = None
    processing_time_ms: int | None = None

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            "url": self.url,
            "success": self.success,
            "invoice_data": self.invoice_data.to_dict() if self.invoice_data else None,
            "error_message": self.error_message,
            "processing_time_ms": self.processing_time_ms,
        }
