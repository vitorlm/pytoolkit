from utils.error.base_custom_error import BaseCustomError


class PdfError(BaseCustomError):
    """Base exception for all PDF-related errors."""

    pass


class PdfFileNotFoundError(PdfError):
    """Raised when the PDF file does not exist or is not a PDF."""

    pass


class PdfExtractionError(PdfError):
    """Raised when extraction of text or tables from a PDF fails."""

    pass


class PdfParsingError(PdfError):
    """Raised when extracted text does not match expected patterns."""

    pass
