import re
from pathlib import Path

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

from utils.logging.logging_manager import LogManager

from .error import PdfExtractionError, PdfFileNotFoundError


class PdfExtractor:
    """Shared PDF text and table extraction utility wrapping pdfplumber."""

    def __init__(self) -> None:
        self.logger = LogManager.get_instance().get_logger("PdfExtractor")
        if pdfplumber is None:
            self.logger.error("pdfplumber is not installed. PDF extraction will fail.")

    def _ensure_pdfplumber(self) -> None:
        """Internal check to ensure pdfplumber is available."""
        if pdfplumber is None:
            raise PdfExtractionError("pdfplumber is required for PDF operations but is not installed.")

    def validate_pdf(self, file_path: str) -> None:
        """Validates file exists, has .pdf extension, and is readable.

        Args:
            file_path: Absolute path to the PDF file.

        Raises:
            PdfFileNotFoundError: If file is missing or not a .pdf.
            PdfExtractionError: If file is not a valid PDF.
        """
        path = Path(file_path)
        if not path.exists() or path.suffix.lower() != ".pdf":
            raise PdfFileNotFoundError(f"File not found or not a PDF: {file_path}")

        try:
            self._ensure_pdfplumber()
            with pdfplumber.open(file_path) as pdf:
                if len(pdf.pages) == 0:
                    raise PdfExtractionError(f"PDF contains no pages: {file_path}")
        except Exception as e:
            if isinstance(e, (PdfFileNotFoundError, PdfExtractionError)):
                raise
            raise PdfExtractionError(f"Failed to open/validate PDF {file_path}: {e}") from e

    def extract_full_text(self, file_path: str) -> str:
        """Opens PDF and concatenates text from all pages.

        Args:
            file_path: Path to the PDF file.

        Returns:
            Extracted text from all pages.

        Raises:
            PdfFileNotFoundError: If file missing.
            PdfExtractionError: If extraction fails or result is empty.
        """
        self.validate_pdf(file_path)
        self.logger.info(f"Extracting full text from PDF: {file_path}")

        try:
            full_text = []
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        full_text.append(text)

            result = "\n".join(full_text)
            if not result.strip():
                raise PdfExtractionError(f"PDF contains no extractable text: {file_path}")

            self.logger.info(f"Extracted {len(full_text)} pages, {len(result)} characters from {file_path}")
            self.logger.debug(f"Text preview: {result[:200]}...")
            return result
        except Exception as e:
            if isinstance(e, (PdfFileNotFoundError, PdfExtractionError)):
                raise
            raise PdfExtractionError(f"Failed to extract text from PDF {file_path}: {e}") from e

    def extract_text_per_page(self, file_path: str) -> list[str]:
        """Returns list of per-page text strings.

        Args:
            file_path: Path to the PDF file.

        Returns:
            List of strings, one per page.
        """
        self.validate_pdf(file_path)
        self.logger.info(f"Extracting per-page text from PDF: {file_path}")

        try:
            pages_text = []
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    pages_text.append(text)

            return pages_text
        except Exception as e:
            if isinstance(e, (PdfFileNotFoundError, PdfExtractionError)):
                raise
            raise PdfExtractionError(f"Failed to extract per-page text from PDF {file_path}: {e}") from e

    def extract_columns_text(self, file_path: str, columns: int = 2) -> list[list[str]]:
        """Extracts text from a multi-column PDF layout by splitting pages vertically.

        Args:
            file_path: Path to the PDF file.
            columns: Number of vertical columns to split into. Defaults to 2.

        Returns:
            List of lists where each inner list contains text for each column on that page.
        """
        self.validate_pdf(file_path)
        self.logger.info(f"Extracting {columns}-column text from PDF: {file_path}")

        try:
            results: list[list[str]] = []
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    width = page.width
                    col_width = width / columns
                    page_cols: list[str] = []

                    for i in range(columns):
                        bbox = (i * col_width, 0, (i + 1) * col_width, page.height)
                        col_text = page.within_bbox(bbox).extract_text() or ""
                        page_cols.append(col_text)

                    results.append(page_cols)

            return results
        except Exception as e:
            if isinstance(e, (PdfFileNotFoundError, PdfExtractionError)):
                raise
            raise PdfExtractionError(f"Failed to extract column text from PDF {file_path}: {e}") from e

    def extract_tables(self, file_path: str) -> list[list[list[str | None]]]:
        """Returns all tables from all pages.

        Args:
            file_path: Path to the PDF file.

        Returns:
            List of pages -> list of tables -> rows -> cells.
        """
        self.validate_pdf(file_path)
        self.logger.info(f"Extracting tables from PDF: {file_path}")

        try:
            all_tables = []
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    all_tables.append(tables)
            return all_tables
        except Exception as e:
            raise PdfExtractionError(f"Failed to extract tables from PDF {file_path}: {e}") from e

    def extract_text_with_filter(self, file_path: str, exclude_patterns: list[str]) -> str:
        """Extracts full text and removes lines matching exclude patterns.

        Args:
            file_path: Path to the PDF file.
            exclude_patterns: List of regex or substring patterns to exclude.

        Returns:
            Cleaned text.
        """
        text = self.extract_full_text(file_path)
        lines = text.splitlines()
        filtered_lines = []

        for line in lines:
            line_stripped = line.strip()
            if not line_stripped:
                filtered_lines.append("")
                continue

            exclude = False
            for pattern in exclude_patterns:
                if re.search(pattern, line_stripped, re.IGNORECASE):
                    exclude = True
                    break

            if not exclude:
                filtered_lines.append(line_stripped)

        # Collapse blank lines
        result_text = "\n".join(filtered_lines)
        result_text = re.sub(r"\n\s*\n+", "\n\n", result_text)
        return result_text.strip()
