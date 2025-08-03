from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class QRCodeData:
    """
    Data class representing parsed NFCe QR code information
    
    Contains both raw parameters from the QR code URL and decoded
    access key components following NFCe specification.
    """
    
    # Raw QR code parameters (pipe-separated)
    access_key: str
    environment: str
    consumer_id: str
    version: str
    validation_hash: str
    
    # Parsed access key components (44 characters total)
    state_code: str                    # Positions 0-1: UF code (31 = Minas Gerais)
    year_month: str                    # Positions 2-5: YYMM format
    issuer_cnpj: str                   # Positions 6-19: 14-digit CNPJ
    model: str                         # Positions 20-21: 65 = NFCe
    series: str                        # Positions 22-24: 3-digit series
    invoice_number: str                # Positions 25-33: 9-digit invoice number
    emission_form: str                 # Position 34: Emission form (1 = Normal)
    numeric_code: str                  # Positions 35-42: 8-digit numeric code
    check_digit: str                   # Position 43: Check digit
    
    # Formatted and derived data
    cnpj_formatted: str                # CNPJ with standard formatting (XX.XXX.XXX/XXXX-XX)
    issue_date: Optional[datetime]     # Parsed date from year_month
    is_production: bool                # True if environment = "1"
    is_valid: bool                     # True if all validations pass
    
    # Validation details
    validation_errors: Optional[list] = None
    
    @property
    def state_name(self) -> str:
        """Get state name from state code"""
        state_codes = {
            "11": "Rondônia", "12": "Acre", "13": "Amazonas", "14": "Roraima",
            "15": "Pará", "16": "Amapá", "17": "Tocantins", "21": "Maranhão",
            "22": "Piauí", "23": "Ceará", "24": "Rio Grande do Norte",
            "25": "Paraíba", "26": "Pernambuco", "27": "Alagoas", "28": "Sergipe",
            "29": "Bahia", "31": "Minas Gerais", "32": "Espírito Santo",
            "33": "Rio de Janeiro", "35": "São Paulo", "41": "Paraná",
            "42": "Santa Catarina", "43": "Rio Grande do Sul", "50": "Mato Grosso do Sul",
            "51": "Mato Grosso", "52": "Goiás", "53": "Distrito Federal"
        }
        return state_codes.get(self.state_code, f"Unknown ({self.state_code})")
    
    @property
    def model_description(self) -> str:
        """Get model description from model code"""
        model_codes = {
            "55": "NF-e (Nota Fiscal Eletrônica)",
            "65": "NFC-e (Nota Fiscal de Consumidor Eletrônica)"
        }
        return model_codes.get(self.model, f"Unknown ({self.model})")
    
    @property
    def environment_description(self) -> str:
        """Get environment description"""
        return "Production" if self.is_production else "Homologation"
    
    @property
    def full_invoice_number(self) -> str:
        """Get formatted full invoice number with series"""
        return f"{self.series}-{self.invoice_number}"
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "access_key": self.access_key,
            "environment": self.environment,
            "consumer_id": self.consumer_id,
            "version": self.version,
            "validation_hash": self.validation_hash,
            "state_code": self.state_code,
            "state_name": self.state_name,
            "year_month": self.year_month,
            "issuer_cnpj": self.issuer_cnpj,
            "cnpj_formatted": self.cnpj_formatted,
            "model": self.model,
            "model_description": self.model_description,
            "series": self.series,
            "invoice_number": self.invoice_number,
            "full_invoice_number": self.full_invoice_number,
            "emission_form": self.emission_form,
            "numeric_code": self.numeric_code,
            "check_digit": self.check_digit,
            "issue_date": self.issue_date.isoformat() if self.issue_date else None,
            "is_production": self.is_production,
            "environment_description": self.environment_description,
            "is_valid": self.is_valid,
            "validation_errors": self.validation_errors or []
        }
    
    def __str__(self) -> str:
        """String representation for logging and display"""
        return (f"NFCe {self.full_invoice_number} - {self.cnpj_formatted} - "
                f"{self.state_name} - {self.environment_description}")