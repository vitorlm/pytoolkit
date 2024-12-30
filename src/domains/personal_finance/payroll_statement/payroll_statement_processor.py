from typing import List, Dict
from utils.file_manager import FileManager
from log_config import log_manager
from PyPDF2 import PdfReader
import re


class PayrollStatementProcessor:
    """
    Processor for extracting relevant data from payroll statements for IRPF calculations.
    """

    log_manager.add_custom_handler(logger_name="PyPDF2", replace_existing=True, handler_id="PyPDF2")

    def __init__(self):
        self.logger = log_manager.get_logger(module_name=FileManager.get_module_name(__file__))

    def process_pdf(self, file_path: str) -> List[Dict[str, str]]:
        """
        Processes a single PDF file to extract payroll statement data.

        Args:
            file_path (str): The path to the PDF file.

        Returns:
            List[Dict[str, str]]: A list of dictionaries containing extracted data
                                  for each payroll statement.
        """
        self.logger.info(f"Processing PDF file: {file_path}")
        FileManager.validate_file(file_path, allowed_extensions=[".pdf"])

        payroll_statements = []
        reader = PdfReader(file_path)

        current_statement = {}
        for page_num, page in enumerate(reader.pages, start=1):
            text = page.extract_text()

            # Check for new payroll statement header
            match = re.search(r"MÊS/ANO\s+.*?(\d{2}\s*/\s*\d{4})", text, re.DOTALL)
            if match:
                # Save previous statement if complete
                if current_statement:
                    payroll_statements.append(current_statement)
                    current_statement = {}

                current_statement["month_year"] = match.group(1).strip()

            # Extract details like salary, INSS, IRRF, FGTS, and base values
            current_statement.update(self._extract_statement_details(text))

        # Save last payroll statement
        if current_statement:
            payroll_statements.append(current_statement)

        self.logger.info(f"Extracted {len(payroll_statements)} payroll statements from the PDF.")
        return payroll_statements

    def process_folder(self, folder_path: str) -> List[Dict[str, str]]:
        """
        Processes all PDF files in a folder to extract payroll statement data.

        Args:
            folder_path (str): The path to the folder containing PDF files.

        Returns:
            List[Dict[str, str]]: A list of dictionaries containing extracted data
                                  for all payroll statements.
        """
        self.logger.info(f"Processing all PDF files in folder: {folder_path}")
        FileManager.validate_folder(folder_path)

        all_statements = []
        pdf_files = FileManager.list_files(folder_path, extension=".pdf")
        for pdf_file in pdf_files:
            try:
                statements = self.process_pdf(pdf_file)
                all_statements.extend(statements)
            except Exception as e:
                self.logger.error(f"Error processing file {pdf_file}: {e}", exc_info=True)

        self.logger.info(
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
