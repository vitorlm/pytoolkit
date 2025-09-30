from typing import List, Dict
from utils.file_manager import FileManager
import re
import pdfplumber

from utils.logging.logging_manager import LogManager


class PayrollStatementProcessor:
    """
    Processor for extracting relevant data from payroll statements for IRPF calculations.
    """

    _logger = LogManager.get_instance().get_logger("PayrollStatementProcessor")
    LogManager.add_custom_handler(
        logger_name="PyPDF2", replace_existing=True, handler_id="PyPDF2"
    )

    def __init__(self):
        self.data = []

    def process_pdf(self, file_path: str) -> List[Dict[str, str]]:
        """
        Processes a single PDF file to extract payroll statement data.

        Args:
            file_path (str): The path to the PDF file.

        Returns:
            List[Dict[str, str]]: A list of dictionaries containing extracted data
                                  for each payroll statement.
        """
        self._logger.info(f"Processing PDF file: {file_path}")
        FileManager.validate_file(file_path, allowed_extensions=[".pdf"])

        output = self._extract_from_pdf(file_path)

        # payroll_statements = []
        # reader = PdfReader(file_path)

        # current_statement = {}
        # for page_num, page in enumerate(reader.pages, start=1):
        #     text = page.extract_text()

        #     # Check for new payroll statement header
        #     match = re.search(r"MÊS/ANO\s+.*?(\d{2}\s*/\s*\d{4})", text, re.DOTALL)
        #     if match:
        #         # Save previous statement if complete
        #         if current_statement:
        #             payroll_statements.append(current_statement)
        #             current_statement = {}

        #         current_statement["month_year"] = match.group(1).strip()

        #     # Extract details like salary, INSS, IRRF, FGTS, and base values
        #     current_statement.update(self._extract_statement_details(text))

        # # Save last payroll statement
        # if current_statement:
        #     payroll_statements.append(current_statement)

        # self._logger.info(f"Extracted {len(payroll_statements)} payroll statements from the PDF.")
        # return payroll_statements

    def process_folder(self, folder_path: str) -> List[Dict[str, str]]:
        """
        Processes all PDF files in a folder to extract payroll statement data.

        Args:
            folder_path (str): The path to the folder containing PDF files.

        Returns:
            List[Dict[str, str]]: A list of dictionaries containing extracted data
                                  for all payroll statements.
        """
        self._logger.info(f"Processing all PDF files in folder: {folder_path}")
        FileManager.validate_folder(folder_path)

        all_statements = []
        pdf_files = FileManager.list_files(folder_path, extension=".pdf")
        for pdf_file in pdf_files:
            try:
                statements = self.process_pdf(pdf_file)
                all_statements.extend(statements)
            except Exception as e:
                self._logger.error(
                    f"Error processing file {pdf_file}: {e}", exc_info=True
                )

        self._logger.info(
            f"Processed {len(pdf_files)} files with {len(all_statements)} "
            "payroll statements extracted."
        )
        return all_statements

    def _extract_statement_details(self, text: str) -> Dict[str, str]:
        """
        Extracts specific details from a payroll statement text.

        Args:
            text (str): The text content of the payroll statement.

        Returns:
            Dict[str, str]: Extracted data as key-value pairs.
        """
        details = {}

        patterns = {
            "gross_salary": r"TOTAL DE VENCIMENTOS\s+([\d,.]+)",
            "net_salary": r"VALOR LÍQUIDO\s+([\d,.]+)",
            "inss": r"INSS.*?([\d,.]+)",
            "irrf": r"IRRF.*?([\d,.]+)",
            "fgts": r"FGTS.*?([\d,.]+)",
            "irrf_base": r"BASE CÁLCULO IRRF\s+([\d,.]+)",
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, text)
            if match:
                details[key] = match.group(1).replace(".", "").replace(",", ".")

        return details

    def _extract_from_pdf(self, file_path: str) -> list:
        """
        Extracts payroll statement data from a PDF file.

        Args:
            file_path (str): Path to the PDF file.

        Returns:
            list: List of extracted data for each statement.
        """
        extracted_data = []

        with pdfplumber.open(file_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                page_data = {}

                # Extract and process tables
                tables = page.extract_tables()
                if tables and self._is_valid_table(tables):
                    print(f"Page {page_num}: Processing tables.")
                    table_data = self._process_table(tables)
                    page_data.update(table_data)

                # Extract and process text
                text = page.extract_text()
                if text:
                    print(f"Page {page_num}: Processing text.")
                    text_data = self._process_text(text)
                    page_data.update(text_data)

                # Append combined data for the page
                if page_data:
                    extracted_data.append(page_data)

        return extracted_data

    @staticmethod
    def _parse_value(value: str) -> float:
        """
        Parses a string value into a float.

        Args:
            value (str): Value as a string.

        Returns:
            float: Parsed value as a float.
        """
        try:
            return float(value.replace(".", "").replace(",", "."))
        except ValueError:
            return 0.0

    def _is_valid_table(self, tables: list) -> bool:
        """
        Validates if the detected tables are meaningful.

        Args:
            tables (list): List of extracted tables.

        Returns:
            bool: True if the tables are valid, False otherwise.
        """
        # Simple validation: check if tables contain at least one meaningful row
        for table in tables:
            if len(table) > 1 and any(
                row for row in table if any(cell.strip() for cell in row)
            ):
                return True
        return False

    def _process_table(self, tables: list) -> dict:
        """
        Processes table data and extracts key information.

        Args:
            tables (list): List of tables extracted from a page.

        Returns:
            dict: Extracted data from the tables.
        """
        parsed_data = {}
        for table in tables:
            for row in table:
                # Example row mapping logic
                if "13o Salário Integral" in row:
                    parsed_data["13o_salario_integral"] = self._parse_value(row[3])
                if "Desc.13o Salário Adto" in row:
                    parsed_data["desconto_13o"] = self._parse_value(row[4])
        return parsed_data

    def _process_text(self, text: str) -> dict:
        """
        Processes text data and extracts key information.

        Args:
            text (str): Text content of a page.

        Returns:
            dict: Extracted data from the text.
        """
        parsed_data = {}
        patterns = {
            "salario_base": r"SALÁRIO BASE\s+([\d.,]+)",
            "salario_contribuicao_inss": r"SALÁRIO CONTR\. INSS\s+([\d.,]+)",
            "faixa_irrf": r"FAIXA IRRF\s+([\d.,]+)",
            "total_vencimentos": r"TOTAL DE VENCIMENTOS\s+([\d.,]+)",
            "total_descontos": r"TOTAL DE DESCONTOS\s+([\d.,]+)",
            "valor_liquido": r"VALOR LÍQUIDO\s+([\d.,]+)",
            "base_fgts": r"BASE CÁLC\. FGTS\s+([\d.,]+)",
            "fgts_mes": r"FGTS DO MÊS\s+([\d.,]+)",
            "base_calculo_irrf": r"BASE CÁLCULO IRRF\s+([\d.,]+)",
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, text)
            if match:
                parsed_data[key] = self._parse_value(match.group(1))
        return parsed_data
