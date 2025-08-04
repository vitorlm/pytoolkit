import re
from typing import Optional, List, Dict, Any
from decimal import Decimal, InvalidOperation
from datetime import datetime
from bs4 import BeautifulSoup, Tag

from utils.logging.logging_manager import LogManager
from domains.personal_finance.nfce.models.invoice_data import (
    InvoiceData,
    EstablishmentData,
    ConsumerData,
    ProductData,
    TaxData,
)


class NFCeDataExtractor:
    """
    Extracts structured data from Portal SPED NFCe HTML pages
    """

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("NFCeDataExtractor")

        # Portuguese-based CSS selectors for NFCe pages
        # These use actual Portuguese terms found on Portal SPED pages
        self.portuguese_selectors = {
            # Invoice identification (Portuguese keys)
            "Número": [
                'span:contains("Número")',
                'td:contains("Número")',
                'th:contains("Número")',
                'label:contains("Número")',
                '[title*="Número"]',
            ],
            "Série": [
                'span:contains("Série")',
                'td:contains("Série")',
                'th:contains("Série")',
                'label:contains("Série")',
                '[title*="Série"]',
            ],
            "Data Emissão": [
                'span:contains("Data Emissão")',
                'td:contains("Data Emissão")',
                'span:contains("Emissão")',
                'td:contains("Emissão")',
                'th:contains("Data Emissão")',
                'label:contains("Data")',
            ],
            # Financial values (Portuguese keys)
            "Valor total do serviço": [
                'span:contains("Valor total do serviço")',
                'td:contains("Valor total do serviço")',
                'span:contains("Total")',
                'td:contains("Total")',
                'th:contains("Total")',
            ],
            "Desconto": [
                'span:contains("Desconto")',
                'td:contains("Desconto")',
                'th:contains("Desconto")',
            ],
            # Establishment data (Portuguese keys)
            "Nome / Razão Social": [
                'span:contains("Nome / Razão Social")',
                'td:contains("Nome / Razão Social")',
                'th:contains("Nome / Razão Social")',
                'span:contains("Razão Social")',
                'td:contains("Razão Social")',
                'span:contains("Nome")',
                'td:contains("Nome")',
            ],
            "CNPJ": [
                'span:contains("CNPJ")',
                'td:contains("CNPJ")',
                'th:contains("CNPJ")',
                'label:contains("CNPJ")',
                '[title*="CNPJ"]',
            ],
            "Inscrição Estadual": [
                'span:contains("Inscrição Estadual")',
                'td:contains("Inscrição Estadual")',
                'th:contains("Inscrição Estadual")',
                'span:contains("Inscrição")',
                'td:contains("Inscrição")',
            ],
            "UF": ['span:contains("UF")', 'td:contains("UF")', 'th:contains("UF")'],
            # Consumer data (Portuguese keys)
            "CPF": [
                'span:contains("CPF")',
                'td:contains("CPF")',
                'th:contains("CPF")',
                'label:contains("CPF")',
                '[title*="CPF"]',
            ],
            "Consumidor final": [
                'span:contains("Consumidor final")',
                'td:contains("Consumidor final")',
                'span:contains("Consumidor")',
                'td:contains("Consumidor")',
            ],
            # Product table (Portuguese keys)
            "Descrição": [
                'table:contains("Descrição")',
                'table:contains("Produto")',
                'table:contains("Item")',
                'table:contains("Código")',
                'th:contains("Descrição")',
                'td:contains("Descrição")',
            ],
        }

    def extract_invoice_data(self, html_content: str, url: str) -> InvoiceData:
        """
        Extract complete invoice data from HTML content

        Args:
            html_content: HTML content from Portal SPED page
            url: Source URL

        Returns:
            InvoiceData object with extracted information
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")

            # Create invoice data object
            invoice_data = InvoiceData(
                access_key="",  # Will be set by caller
                invoice_number="",
                series="",
                source_url=url,
                scraped_at=datetime.now(),
            )

            # Extract basic invoice information
            self._extract_basic_info(soup, invoice_data)

            # Extract establishment data
            invoice_data.establishment = self._extract_establishment_data(soup)

            # Extract consumer data
            invoice_data.consumer = self._extract_consumer_data(soup)

            # Extract product items
            invoice_data.items = self._extract_product_items(soup)

            # Extract tax information
            invoice_data.taxes = self._extract_tax_information(soup)

            # Extract financial totals
            self._extract_financial_data(soup, invoice_data)

            # Check if this is an empty/expired NFCe page
            if self._is_empty_nfce_page(soup, invoice_data):
                invoice_data.scraping_success = False
                invoice_data.add_error("NFCe appears to be expired or invalid - all data fields are empty")
                self.logger.warning(f"NFCe page appears to be expired or invalid: {url}")
                return invoice_data

            # Mark as successful if we got basic data
            invoice_data.scraping_success = bool(
                invoice_data.establishment
                or invoice_data.items
                or invoice_data.total_amount
            )

            if invoice_data.scraping_success:
                self.logger.info(f"Successfully extracted data from {url}")
            else:
                self.logger.warning(f"No meaningful data extracted from {url}")
                invoice_data.add_error("No meaningful data found in NFCe page")
            
            return invoice_data

        except Exception as e:
            self.logger.error(f"Error extracting data from {url}: {e}")

            # Return basic invoice data with error
            invoice_data = InvoiceData(
                access_key="",
                invoice_number="",
                series="",
                source_url=url,
                scraped_at=datetime.now(),
                scraping_success=False,
            )
            invoice_data.add_error(str(e))
            return invoice_data

    def _extract_from_invoice_table(
        self, soup: BeautifulSoup
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Extract invoice data from the specific table structure in "Informações gerais da Nota" section
        """
        try:
            # First, try to find the specific section "Informações gerais da Nota"
            info_section = None

            # Look for the panel with "Informações gerais da Nota"
            panels = soup.find_all("div", class_="panel panel-default")
            for panel in panels:
                panel_title = panel.find("h4", class_="panel-title")
                if (
                    panel_title
                    and "Informações gerais da Nota" in panel_title.get_text()
                ):
                    info_section = panel
                    break

            # If we found the specific section, search only within it
            search_area = info_section if info_section else soup

            # Find table containing the headers "Modelo", "Série", "Número", "Data Emissão"
            tables = search_area.find_all("table", class_="table table-hover")

            for table in tables:
                # Check if this table has the expected headers
                headers = table.find_all("th")
                if len(headers) >= 4:
                    header_texts = [th.get_text().strip() for th in headers]

                    # Check if we have the expected headers
                    if (
                        "Modelo" in header_texts
                        and "Série" in header_texts
                        and "Número" in header_texts
                        and "Data Emissão" in header_texts
                    ):

                        # Find the indices of our target columns
                        modelo_idx = (
                            header_texts.index("Modelo")
                            if "Modelo" in header_texts
                            else None
                        )
                        serie_idx = (
                            header_texts.index("Série")
                            if "Série" in header_texts
                            else None
                        )
                        numero_idx = (
                            header_texts.index("Número")
                            if "Número" in header_texts
                            else None
                        )
                        data_idx = (
                            header_texts.index("Data Emissão")
                            if "Data Emissão" in header_texts
                            else None
                        )

                        # Find the data row (first tbody tr)
                        tbody = table.find("tbody")
                        if tbody:
                            data_row = tbody.find("tr")
                        else:
                            # Fallback: find first tr that's not the header
                            rows = table.find_all("tr")
                            data_row = rows[1] if len(rows) > 1 else None

                        if data_row:
                            cells = data_row.find_all(["td", "th"])

                            # Extract the values based on column indices
                            invoice_number = None
                            series = None
                            issue_date = None

                            if numero_idx is not None and numero_idx < len(cells):
                                invoice_number = cells[numero_idx].get_text().strip()

                            if serie_idx is not None and serie_idx < len(cells):
                                series = cells[serie_idx].get_text().strip()

                            if data_idx is not None and data_idx < len(cells):
                                issue_date = cells[data_idx].get_text().strip()

                            self.logger.debug(
                                f"Extracted from table - Número: {invoice_number}, Série: {series}, Data: {issue_date}"
                            )
                            return invoice_number, series, issue_date

            # If no matching table found, return None values
            return None, None, None

        except Exception as e:
            self.logger.warning(f"Error extracting from invoice table: {e}")
            return None, None, None

    def _extract_from_page_header(self, soup: BeautifulSoup) -> Optional[dict]:
        """
        Extract establishment data from the header table at the top of the page
        Structure:
        <th>Nota Fiscal de Consumidor Eletrônica (NFC-e)</th>
        <th><b>ORGANIZACAO VERDEMAR LTDA</b></th>
        <td>CNPJ: 65.124.307/0016-26, Inscrição Estadual: 062705396.16-12</td>
        <td>Rua do Ouro, 195, Serra, 3106200 - Belo Horizonte, MG</td>
        """
        try:
            # Look for the main table at the top with the NFCe header
            main_table = soup.find("table", class_="table text-center")
            if not main_table:
                return None
            
            # Find the business name (in <b> tag within <th>)
            business_name_th = main_table.find("th", class_="text-center text-uppercase")
            business_name = None
            if business_name_th:
                business_name_b = business_name_th.find("b")
                if business_name_b:
                    business_name = business_name_b.get_text().strip()
            
            # Find CNPJ and address in tbody td elements
            tbody = main_table.find("tbody")
            if not tbody:
                return None
            
            tds = tbody.find_all("td")
            cnpj = None
            address = None
            city = None
            state = None
            
            for td in tds:
                text = td.get_text().strip()
                
                # Extract CNPJ from text like "CNPJ: 65.124.307/0016-26, Inscrição Estadual: 062705396.16-12"
                if "CNPJ:" in text:
                    cnpj_match = re.search(r'CNPJ:\s*([0-9]{2}\.?[0-9]{3}\.?[0-9]{3}/?[0-9]{4}-?[0-9]{2})', text)
                    if cnpj_match:
                        cnpj = self._clean_cnpj(cnpj_match.group(1))
                
                # Extract address from text like "Rua do Ouro, 195, Serra, 3106200 - Belo Horizonte, MG"
                if any(keyword in text.lower() for keyword in ['rua', 'av', 'avenida', 'estrada', 'rodovia']) and ',' in text:
                    address = text.strip()
                    # Try to extract city and state from the end of address
                    # Pattern: "... - CIDADE, UF" or "... CIDADE, UF"
                    city_state_match = re.search(r'[-,]\s*([^,]+),\s*([A-Z]{2})\s*$', text)
                    if city_state_match:
                        city = city_state_match.group(1).strip()
                        state = city_state_match.group(2).strip()
            
            # Return extracted data if we found at least business name and CNPJ
            if business_name and cnpj:
                return {
                    "business_name": business_name,
                    "cnpj": cnpj,
                    "address": address,
                    "city": city,
                    "state": state
                }
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error extracting from page header: {e}")
            return None

    def _extract_from_establishment_table(self, soup: BeautifulSoup) -> Optional[dict]:
        """
        Extract establishment data from the specific "Emitente" table in "Informações gerais da Nota" section
        """
        try:
            # First, try to find the specific section "Informações gerais da Nota"
            info_section = None

            # Look for the panel with "Informações gerais da Nota"
            panels = soup.find_all("div", class_="panel panel-default")
            for panel in panels:
                panel_title = panel.find("h4", class_="panel-title")
                if (
                    panel_title
                    and "Informações gerais da Nota" in panel_title.get_text()
                ):
                    info_section = panel
                    break

            if not info_section:
                return None

            # Look for the "Emitente" table
            emitente_heading = info_section.find(
                "h5", text=lambda t: t and "Emitente" in t
            )
            if not emitente_heading:
                return None

            # Find the table after the "Emitente" heading
            table = emitente_heading.find_next("table", class_="table table-hover")
            if not table:
                return None

            # Check if this table has the expected headers for establishment data
            headers = table.find_all("th")
            header_texts = [th.get_text().strip() for th in headers]

            if (
                "Nome / Razão Social" in header_texts
                and "CNPJ" in header_texts
                and "Inscrição Estadual" in header_texts
                and "UF" in header_texts
            ):

                # Find the indices of our target columns
                nome_idx = header_texts.index("Nome / Razão Social")
                cnpj_idx = header_texts.index("CNPJ")
                ie_idx = header_texts.index("Inscrição Estadual")
                uf_idx = header_texts.index("UF")

                # Find the data row
                tbody = table.find("tbody")
                data_row = tbody.find("tr") if tbody else None

                if not data_row:
                    rows = table.find_all("tr")
                    data_row = rows[1] if len(rows) > 1 else None

                if data_row:
                    cells = data_row.find_all(["td", "th"])

                    establishment_data = {}

                    if nome_idx < len(cells):
                        establishment_data["business_name"] = self._clean_text(
                            cells[nome_idx].get_text()
                        )

                    if cnpj_idx < len(cells):
                        establishment_data["cnpj"] = self._clean_cnpj(
                            cells[cnpj_idx].get_text()
                        )

                    if ie_idx < len(cells):
                        establishment_data["state_registration"] = self._clean_text(
                            cells[ie_idx].get_text()
                        )

                    if uf_idx < len(cells):
                        establishment_data["state"] = self._clean_text(
                            cells[uf_idx].get_text()
                        )

                    self.logger.debug(
                        f"Extracted establishment from table: {establishment_data}"
                    )
                    return establishment_data

            return None

        except Exception as e:
            self.logger.warning(f"Error extracting from establishment table: {e}")
            return None

    def _extract_from_financial_table(self, soup: BeautifulSoup) -> Optional[dict]:
        """
        Extract financial data from the specific "Valor total do serviço" table in "Informações gerais da Nota" section
        """
        try:
            # First, try to find the specific section "Informações gerais da Nota"
            info_section = None

            # Look for the panel with "Informações gerais da Nota"
            panels = soup.find_all("div", class_="panel panel-default")
            for panel in panels:
                panel_title = panel.find("h4", class_="panel-title")
                if (
                    panel_title
                    and "Informações gerais da Nota" in panel_title.get_text()
                ):
                    info_section = panel
                    break

            if not info_section:
                return None

            # Find table with financial data headers
            tables = info_section.find_all("table", class_="table table-hover")

            for table in tables:
                headers = table.find_all("th")
                header_texts = [th.get_text().strip() for th in headers]

                # Check if this is the financial table
                if (
                    "Valor total do serviço" in header_texts
                    and "Base de Cálculo ICMS" in header_texts
                    and "Valor ICMS" in header_texts
                ):

                    # Find the indices of our target columns
                    total_idx = header_texts.index("Valor total do serviço")
                    base_icms_idx = header_texts.index("Base de Cálculo ICMS")
                    valor_icms_idx = header_texts.index("Valor ICMS")

                    # Find the data row
                    tbody = table.find("tbody")
                    data_row = tbody.find("tr") if tbody else None

                    if not data_row:
                        rows = table.find_all("tr")
                        data_row = rows[1] if len(rows) > 1 else None

                    if data_row:
                        cells = data_row.find_all(["td", "th"])

                        financial_data = {}

                        if total_idx < len(cells):
                            total_text = cells[total_idx].get_text().strip()
                            financial_data["total_amount"] = self._parse_currency(
                                total_text
                            )

                        if base_icms_idx < len(cells):
                            base_text = cells[base_icms_idx].get_text().strip()
                            financial_data["icms_base"] = self._parse_currency(
                                base_text
                            )

                        if valor_icms_idx < len(cells):
                            icms_text = cells[valor_icms_idx].get_text().strip()
                            financial_data["icms_amount"] = self._parse_currency(
                                icms_text
                            )

                        self.logger.debug(
                            f"Extracted financial from table: {financial_data}"
                        )
                        return financial_data

            return None

        except Exception as e:
            self.logger.warning(f"Error extracting from financial table: {e}")
            return None

    def _extract_from_consumer_table(self, soup: BeautifulSoup) -> Optional[dict]:
        """
        Extract consumer data from the specific "Consumidor" table
        """
        try:
            # Look for the panel with "Consumidor"
            panels = soup.find_all("div", class_="panel panel-default")
            for panel in panels:
                panel_title = panel.find("h4", class_="panel-title")
                if panel_title and "Consumidor" in panel_title.get_text():

                    # Find the table in this panel
                    table = panel.find("table", class_="table table-hover")
                    if not table:
                        continue

                    # Check if this table has the expected headers for consumer data
                    headers = table.find_all("th")
                    header_texts = [th.get_text().strip() for th in headers]

                    if "Nome / Razão Social" in header_texts and "UF" in header_texts:

                        # Find the indices of our target columns
                        nome_idx = header_texts.index("Nome / Razão Social")
                        uf_idx = header_texts.index("UF")

                        # Find the data row
                        tbody = table.find("tbody")
                        data_row = tbody.find("tr") if tbody else None

                        if not data_row:
                            rows = table.find_all("tr")
                            data_row = rows[1] if len(rows) > 1 else None

                        if data_row:
                            cells = data_row.find_all(["td", "th"])

                            consumer_data = {}

                            if nome_idx < len(cells):
                                name_text = self._clean_text(cells[nome_idx].get_text())
                                if name_text:  # Only add if not empty
                                    consumer_data["name"] = name_text

                            if uf_idx < len(cells):
                                uf_text = self._clean_text(cells[uf_idx].get_text())
                                if uf_text:  # Only add if not empty
                                    consumer_data["state"] = uf_text

                            # Look for CPF in other tables in the same section
                            # (Sometimes CPF might be in a different table)
                            consumer_data.update(
                                self._find_cpf_in_consumer_section(panel)
                            )

                            if consumer_data:  # Only return if we found some data
                                self.logger.debug(
                                    f"Extracted consumer from table: {consumer_data}"
                                )
                                return consumer_data

            return None

        except Exception as e:
            self.logger.warning(f"Error extracting from consumer table: {e}")
            return None

    def _find_cpf_in_consumer_section(self, consumer_panel) -> dict:
        """
        Look for CPF information in the consumer panel section
        """
        try:
            cpf_data = {}

            # Look for any text containing CPF patterns in the consumer section
            panel_text = consumer_panel.get_text()

            # Look for CPF pattern (XXX.XXX.XXX-XX or XXXXXXXXXXX)
            cpf_patterns = [
                r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b",  # Formatted CPF
                r"\b\d{11}\b",  # Raw CPF numbers
            ]

            for pattern in cpf_patterns:
                import re

                matches = re.findall(pattern, panel_text)
                for match in matches:
                    # Validate CPF length
                    digits_only = re.sub(r"\D", "", match)
                    if len(digits_only) == 11:
                        cpf_data["cpf"] = self._clean_cpf(match)
                        break

                if "cpf" in cpf_data:
                    break

            return cpf_data

        except Exception as e:
            self.logger.debug(f"Error finding CPF in consumer section: {e}")
            return {}

    def _extract_basic_info(
        self, soup: BeautifulSoup, invoice_data: InvoiceData
    ) -> None:
        """Extract basic invoice information using Portuguese field names"""
        try:
            # First try to extract from the specific table structure
            invoice_number, series, issue_date_text = self._extract_from_invoice_table(
                soup
            )

            # If table extraction failed, fall back to the general method
            if not invoice_number:
                invoice_number = self._find_text_by_portuguese_key(soup, "Número")
            if invoice_number:
                invoice_data.invoice_number = self._clean_text(invoice_number)

            if not series:
                series = self._find_text_by_portuguese_key(soup, "Série")
            if series:
                invoice_data.series = self._clean_text(series)

            if not issue_date_text:
                issue_date_text = self._find_text_by_portuguese_key(
                    soup, "Data Emissão"
                )
            if issue_date_text:
                invoice_data.issue_date = self._parse_date(issue_date_text)

            # Log what we found with Portuguese keys
            self.logger.debug(
                f"Found invoice data: Número={invoice_number}, Série={series}, Data={issue_date_text}"
            )

        except Exception as e:
            self.logger.warning(f"Error extracting basic info: {e}")
            invoice_data.add_error(f"Failed to extract basic info: {e}")

    def _extract_establishment_data(
        self, soup: BeautifulSoup
    ) -> Optional[EstablishmentData]:
        """Extract establishment/business data from the top of the page"""
        try:
            establishment = EstablishmentData(cnpj="")

            # First try to extract from the header table at the top of the page
            header_establishment = self._extract_from_page_header(soup)
            if header_establishment:
                establishment.business_name = header_establishment.get("business_name")
                establishment.cnpj = header_establishment.get("cnpj")
                establishment.address = header_establishment.get("address")
                establishment.city = header_establishment.get("city")
                establishment.state = header_establishment.get("state")
                self.logger.info(f"Extracted establishment from header: {establishment.business_name}, CNPJ: {establishment.cnpj}")
            
            # Fallback: try to extract from the specific "Emitente" table in "Informações gerais da Nota"
            if not establishment.cnpj:
                establishment_data = self._extract_from_establishment_table(soup)
                if establishment_data:
                    establishment.business_name = establishment_data.get("business_name")
                    establishment.cnpj = establishment_data.get("cnpj")
                    establishment.state_registration = establishment_data.get(
                        "state_registration"
                    )
                    establishment.state = establishment_data.get("state")

            # Fallback to the general method if specific table extraction failed
            if not establishment.cnpj:
                cnpj_text = self._find_text_by_portuguese_key(soup, "CNPJ")
                if cnpj_text:
                    establishment.cnpj = self._clean_cnpj(cnpj_text)

            if not establishment.business_name:
                name_text = self._find_text_by_portuguese_key(
                    soup, "Nome / Razão Social"
                )
                if name_text:
                    establishment.business_name = self._clean_text(name_text)
                    establishment.trade_name = establishment.business_name

            if not establishment.state_registration:
                state_reg_text = self._find_text_by_portuguese_key(
                    soup, "Inscrição Estadual"
                )
                if state_reg_text:
                    establishment.state_registration = self._clean_text(state_reg_text)

            if not establishment.state:
                state_text = self._find_text_by_portuguese_key(soup, "UF")
                if state_text:
                    establishment.state = self._clean_text(state_text)
                else:
                    establishment.state = "MG"  # Default for Portal SPED MG

            # Try to find address from any text near CNPJ/company name
            address_text = self._find_address_near_establishment(
                soup, establishment.business_name
            )
            if address_text:
                establishment.address = self._clean_text(address_text)
                establishment.city = self._extract_city_from_address(address_text)

            # Log what we found
            self.logger.debug(
                f"Found establishment: CNPJ={establishment.cnpj}, Nome={establishment.business_name}, UF={establishment.state}"
            )

            # Return establishment if we have at least CNPJ or name
            if establishment.cnpj or establishment.business_name:
                return establishment

            return None

        except Exception as e:
            self.logger.warning(f"Error extracting establishment data: {e}")
            return None

    def _extract_consumer_data(self, soup: BeautifulSoup) -> Optional[ConsumerData]:
        """Extract consumer data if available using Portuguese field names"""
        try:
            consumer = ConsumerData()

            # First try to extract from the specific "Consumidor" table
            consumer_data = self._extract_from_consumer_table(soup)
            if consumer_data:
                if consumer_data.get("name"):
                    consumer.name = consumer_data["name"]
                if consumer_data.get("cpf"):
                    consumer.cpf = consumer_data["cpf"]
                if consumer_data.get("state"):
                    # Store state info if needed (may add to ConsumerData model later)
                    pass

            # Fallback to the general method if specific table extraction failed
            if not consumer.cpf:
                cpf_text = self._find_text_by_portuguese_key(soup, "CPF")
                if cpf_text:
                    consumer.cpf = self._clean_cpf(cpf_text)

            # Extract consumer final flag (using Portuguese key)
            consumer_final_text = self._find_text_by_portuguese_key(
                soup, "Consumidor final"
            )
            if consumer_final_text:
                consumer.final_consumer = self._clean_text(consumer_final_text)

            # Try to find consumer name if not found in table
            if not consumer.name:
                name_text = self._find_consumer_name(soup)
                if name_text:
                    consumer.name = self._clean_text(name_text)

            # Log what we found
            self.logger.debug(
                f"Found consumer: CPF={consumer.cpf}, Final={consumer.final_consumer}, Nome={consumer.name}"
            )

            # Return consumer if we have data
            if consumer.cpf or consumer.name or consumer.final_consumer:
                return consumer

            return None

        except Exception as e:
            self.logger.warning(f"Error extracting consumer data: {e}")
            return None

    def _extract_product_items(self, soup: BeautifulSoup) -> List[ProductData]:
        """Extract product/item data from tables using Portuguese field names"""
        items = []

        try:
            # First try to find the specific product table structure
            specific_items = self._extract_from_specific_product_table(soup)
            if specific_items:
                items.extend(specific_items)
                self.logger.info(
                    f"Extracted {len(specific_items)} items from specific table structure"
                )

            # If no items found, fallback to the general method
            if not items:
                table = self._find_product_table_by_portuguese_keys(soup)
                if not table:
                    self.logger.warning("No product table found with Portuguese keys")
                    return items

                # Extract table rows (skip header)
                rows = table.find_all("tr")[1:] if table.find_all("tr") else []

                for i, row in enumerate(rows, 1):
                    try:
                        item = self._extract_item_from_row_portuguese(row, i)
                        if item:
                            items.append(item)
                            self.logger.debug(
                                f"Extracted item {i}: {item.description[:50]}..."
                            )
                    except Exception as e:
                        self.logger.warning(f"Error extracting item {i}: {e}")
                        continue

            self.logger.info(f"Extracted {len(items)} product items total")

        except Exception as e:
            self.logger.warning(f"Error extracting product items: {e}")

        return items

    def _extract_from_specific_product_table(
        self, soup: BeautifulSoup
    ) -> List[ProductData]:
        """
        Extract product items from the specific NFCe table structure:
        <table class="table table-striped">
            <tbody id="myTable">
                <tr>
                    <td><h7>PRODUCT NAME</h7>(Código: XXXX)</td>
                    <td>Qtde total de ítens: X.XXXX</td>
                    <td>UN: XX</td>
                    <td>Valor total R$: R$ XX,XX</td>
                </tr>
            </tbody>
        </table>
        """
        items = []

        try:
            # Find the specific product table
            product_table = soup.find("table", class_="table table-striped")
            if not product_table:
                return items

            # Find the tbody with id="myTable" or just tbody
            tbody = product_table.find("tbody", id="myTable")
            if not tbody:
                tbody = product_table.find("tbody")

            if not tbody:
                return items

            # Extract all rows
            rows = tbody.find_all("tr")

            for i, row in enumerate(rows, 1):
                try:
                    item = self._extract_item_from_specific_row(row, i)
                    if item:
                        items.append(item)
                        self.logger.debug(
                            f"Extracted specific item {i}: {item.description[:30]}..."
                        )

                except Exception as e:
                    self.logger.warning(f"Error extracting specific item {i}: {e}")
                    continue

            return items

        except Exception as e:
            self.logger.warning(f"Error extracting from specific product table: {e}")
            return items

    def _extract_item_from_specific_row(
        self, row: Tag, item_number: int
    ) -> Optional[ProductData]:
        """
        Extract product data from the specific NFCe row structure:
        <tr>
            <td><h7>PRODUCT NAME</h7>(Código: XXXX)</td>
            <td>Qtde total de ítens: X.XXXX</td>
            <td>UN: XX</td>
            <td>Valor total R$: R$ XX,XX</td>
        </tr>
        """
        try:
            cells = row.find_all("td")
            if len(cells) != 4:
                return None

            item = ProductData(item_number=item_number)

            # Cell 1: Description and Code
            # Format: <h7>PRODUCT NAME</h7>(Código: XXXX)
            desc_cell = cells[0]
            desc_text = desc_cell.get_text()

            # Extract description (text before "(Código:")
            if "(Código:" in desc_text:
                parts = desc_text.split("(Código:", 1)
                item.description = parts[0].strip()

                # Extract product code (text between "Código:" and ")")
                if len(parts) > 1 and ")" in parts[1]:
                    code_part = parts[1].split(")", 1)[0].strip()
                    item.product_code = code_part
            else:
                item.description = desc_text.strip()

            # Cell 2: Quantity
            # Format: "Qtde total de ítens: X.XXXX"
            qty_cell = cells[1]
            qty_text = qty_cell.get_text()
            if "Qtde total de ítens:" in qty_text:
                qty_value = qty_text.replace("Qtde total de ítens:", "").strip()
                item.quantity = self._parse_decimal(qty_value)

            # Cell 3: Unit
            # Format: "UN: XX"
            unit_cell = cells[2]
            unit_text = unit_cell.get_text()
            if "UN:" in unit_text:
                unit_value = unit_text.replace("UN:", "").strip()
                item.unit = unit_value

            # Cell 4: Total Amount
            # Format: "Valor total R$: R$ XX,XX"
            value_cell = cells[3]
            value_text = value_cell.get_text()
            if "Valor total R$:" in value_text:
                # Extract the currency value after "R$:"
                value_part = value_text.replace("Valor total R$:", "").strip()
                item.total_amount = self._parse_currency(value_part)

            # Calculate unit price if we have quantity and total
            if item.quantity and item.total_amount and item.quantity > 0:
                item.unit_price = item.total_amount / item.quantity

            # Return item only if we have meaningful data
            return item if (item.description and item.description.strip()) else None

        except Exception as e:
            self.logger.warning(f"Error extracting item from specific row: {e}")
            return None

    def _extract_item_from_row(
        self, row: Tag, item_number: int
    ) -> Optional[ProductData]:
        """Extract product data from table row"""
        try:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                return None

            item = ProductData(item_number=item_number)

            # Try to extract common fields from cells
            # This is a generic approach - specific sites may need customization
            for i, cell in enumerate(cells):
                text = self._clean_text(cell.get_text())

                if i == 0:  # Usually item/product code
                    item.product_code = text
                elif i == 1:  # Usually description
                    item.description = text
                elif i == 2:  # Usually quantity
                    item.quantity = self._parse_decimal(text)
                elif i == 3:  # Usually unit price
                    item.unit_price = self._parse_decimal(text)
                elif i == 4:  # Usually total amount
                    item.total_amount = self._parse_decimal(text)

            # Look for barcode in the row
            barcode_text = self._find_text_in_element(row, ["codigo", "barcode", "ean"])
            if barcode_text:
                item.barcode = self._clean_text(barcode_text)

            return item if item.description else None

        except Exception as e:
            self.logger.warning(f"Error extracting item from row: {e}")
            return None

    def _extract_tax_information(self, soup: BeautifulSoup) -> Optional[TaxData]:
        """Extract tax information"""
        try:
            taxes = TaxData()

            # Look for tax-related text
            tax_selectors = [
                'span:contains("ICMS")',
                'td:contains("ICMS")',
                'span:contains("PIS")',
                'td:contains("PIS")',
                'span:contains("COFINS")',
                'td:contains("COFINS")',
                'span:contains("Tributo")',
                'td:contains("Tributo")',
            ]

            for selector in tax_selectors:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text()

                    # Extract tax values
                    if "ICMS" in text.upper():
                        value = self._extract_currency_from_text(text)
                        if value:
                            taxes.icms_total = value
                    elif "PIS" in text.upper():
                        value = self._extract_currency_from_text(text)
                        if value:
                            taxes.pis_total = value
                    elif "COFINS" in text.upper():
                        value = self._extract_currency_from_text(text)
                        if value:
                            taxes.cofins_total = value

            return (
                taxes
                if any([taxes.icms_total, taxes.pis_total, taxes.cofins_total])
                else None
            )

        except Exception as e:
            self.logger.warning(f"Error extracting tax information: {e}")
            return None

    def _extract_financial_data(
        self, soup: BeautifulSoup, invoice_data: InvoiceData
    ) -> None:
        """Extract financial totals using Portuguese field names"""
        try:
            # First try to extract from the specific financial table
            financial_data = self._extract_from_financial_table(soup)
            if financial_data:
                if financial_data.get("total_amount"):
                    invoice_data.total_amount = financial_data["total_amount"]
                if financial_data.get("icms_amount"):
                    if not invoice_data.taxes:
                        invoice_data.taxes = TaxData()
                    invoice_data.taxes.icms_total = financial_data["icms_amount"]

            # Fallback to the general method if specific table extraction failed
            if not invoice_data.total_amount:
                total_text = self._find_text_by_portuguese_key(
                    soup, "Valor total do serviço"
                )
                if total_text:
                    invoice_data.total_amount = self._parse_currency(total_text)

            # Extract discount amount (using Portuguese key)
            discount_text = self._find_text_by_portuguese_key(soup, "Desconto")
            if discount_text:
                invoice_data.discount_amount = self._parse_currency(discount_text)

            # Try to find any "Total" or "Valor" references for fallback
            if not invoice_data.total_amount:
                total_fallback = self._find_any_total_value(soup)
                if total_fallback:
                    invoice_data.total_amount = self._parse_currency(total_fallback)

            # Calculate products amount if not found
            if not invoice_data.products_amount and invoice_data.total_amount:
                discount = invoice_data.discount_amount or Decimal("0")
                invoice_data.products_amount = invoice_data.total_amount + discount

            # Log what we found
            self.logger.debug(
                f"Found financial: Total={invoice_data.total_amount}, Desconto={invoice_data.discount_amount}"
            )

        except Exception as e:
            self.logger.warning(f"Error extracting financial data: {e}")
            invoice_data.add_error(f"Failed to extract financial data: {e}")

    # Helper methods
    def _find_text_by_selectors(
        self, soup: BeautifulSoup, selectors: List[str]
    ) -> Optional[str]:
        """Find text using multiple CSS selectors"""
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text()
        return None

    def _find_text_by_portuguese_key(
        self, soup: BeautifulSoup, portuguese_key: str
    ) -> Optional[str]:
        """Find text using Portuguese field name with multiple selector patterns"""
        if portuguese_key not in self.portuguese_selectors:
            self.logger.debug(
                f"No selectors defined for Portuguese key: {portuguese_key}"
            )
            return None

        selectors = self.portuguese_selectors[portuguese_key]

        for selector in selectors:
            try:
                # Handle :contains() selectors specially
                if ":contains(" in selector:
                    elements = soup.find_all(
                        text=lambda text: text and portuguese_key in text
                    )
                    for element in elements:
                        if element.parent:
                            # Get next sibling text or parent text
                            next_text = self._get_associated_value(
                                element.parent, portuguese_key
                            )
                            if next_text:
                                return next_text
                else:
                    element = soup.select_one(selector)
                    if element:
                        return element.get_text()
            except Exception as e:
                self.logger.debug(f"Error with selector {selector}: {e}")
                continue

        return None

    def _get_associated_value(self, element: Tag, field_name: str) -> Optional[str]:
        """Get the value associated with a field name in various HTML patterns"""
        try:
            # Pattern 1: Next sibling
            if element.next_sibling:
                text = (
                    element.next_sibling.get_text()
                    if hasattr(element.next_sibling, "get_text")
                    else str(element.next_sibling)
                )
                if text and text.strip() and field_name not in text:
                    return text.strip()

            # Pattern 2: Parent's next sibling
            if element.parent and element.parent.next_sibling:
                sibling = element.parent.next_sibling
                if hasattr(sibling, "get_text"):
                    text = sibling.get_text().strip()
                    if text and field_name not in text:
                        return text

            # Pattern 3: Same row, next cell (for table data)
            if element.parent and element.parent.name in ["td", "th"]:
                row = element.parent.parent
                if row:
                    cells = row.find_all(["td", "th"])
                    for i, cell in enumerate(cells):
                        if field_name in cell.get_text():
                            # Return next cell's text
                            if i + 1 < len(cells):
                                next_cell_text = cells[i + 1].get_text().strip()
                                if next_cell_text:
                                    return next_cell_text

            # Pattern 4: Colon-separated value on same line
            element_text = element.get_text()
            if ":" in element_text:
                parts = element_text.split(":", 1)
                if len(parts) == 2 and field_name in parts[0]:
                    return parts[1].strip()

            return None

        except Exception as e:
            self.logger.debug(f"Error getting associated value for {field_name}: {e}")
            return None

    def _find_element_by_selectors(
        self, soup: BeautifulSoup, selectors: List[str]
    ) -> Optional[Tag]:
        """Find element using multiple CSS selectors"""
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element
        return None

    def _find_text_in_element(self, element: Tag, keywords: List[str]) -> Optional[str]:
        """Find text containing keywords in element"""
        text = element.get_text().lower()
        for keyword in keywords:
            if keyword.lower() in text:
                return element.get_text()
        return None

    def _find_product_table_by_portuguese_keys(
        self, soup: BeautifulSoup
    ) -> Optional[Tag]:
        """Find product table using Portuguese keys and patterns"""
        # Look for tables containing product-related Portuguese terms
        product_terms = ["Descrição", "Código", "Quantidade", "Valor", "Total", "UN"]

        tables = soup.find_all("table")
        for table in tables:
            table_text = table.get_text()
            # Count how many product terms appear in this table
            term_count = sum(1 for term in product_terms if term in table_text)

            # If table contains multiple product terms, it's likely the product table
            if term_count >= 2:
                return table

        # Fallback: look for the largest table (often contains products)
        if tables:
            return max(tables, key=lambda t: len(t.find_all("tr")))

        return None

    def _extract_item_from_row_portuguese(
        self, row: Tag, item_number: int
    ) -> Optional[ProductData]:
        """Extract product data from table row using Portuguese patterns"""
        try:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                return None

            item = ProductData(item_number=item_number)

            # Extract data from each cell, looking for Portuguese patterns
            row_text = row.get_text()

            # Pattern 1: Look for "Código:" pattern in description
            codigo_match = re.search(r"\(Código[:\s]*([^)]+)\)", row_text)
            if codigo_match:
                item.product_code = codigo_match.group(1).strip()

            # Pattern 2: Extract description (usually the longest text before codigo)
            if codigo_match:
                desc_text = row_text[: codigo_match.start()].strip()
                if desc_text:
                    item.description = desc_text
            else:
                # Fallback: use first cell as description
                item.description = self._clean_text(cells[0].get_text())

            # Pattern 3: Look for "Qtde total de ítens:" pattern
            qtde_match = re.search(r"Qtde total de ítens[:\s]+(\d+[.,]?\d*)", row_text)
            if qtde_match:
                qtde_text = qtde_match.group(1).replace(",", ".")
                item.quantity = self._parse_decimal(qtde_text)

            # Pattern 4: Look for "UN:" pattern
            un_match = re.search(r"UN[:\s]+([A-Z]{2,3})", row_text)
            if un_match:
                item.unit = un_match.group(1)

            # Pattern 5: Look for "Valor total R$:" pattern
            valor_match = re.search(r"Valor total R\$[:\s]*R\$\s*([\d,\.]+)", row_text)
            if valor_match:
                valor_text = valor_match.group(1).replace(",", ".")
                item.total_amount = self._parse_currency(f"R$ {valor_text}")

            # Return item only if we have meaningful data
            return item if (item.description or item.product_code) else None

        except Exception as e:
            self.logger.warning(f"Error extracting item from row: {e}")
            return None

    def _find_address_near_establishment(
        self, soup: BeautifulSoup, company_name: str
    ) -> Optional[str]:
        """Find address text near company name"""
        if not company_name:
            return None

        try:
            # Look for text elements containing the company name
            company_elements = soup.find_all(
                text=lambda text: text and company_name in text
            )

            for element in company_elements:
                if element.parent:
                    # Look in siblings for address-like patterns
                    parent = element.parent
                    siblings = parent.find_next_siblings() if parent else []

                    for sibling in siblings[:3]:  # Check next 3 siblings
                        sibling_text = (
                            sibling.get_text()
                            if hasattr(sibling, "get_text")
                            else str(sibling)
                        )

                        # Check if this looks like an address (contains street indicators)
                        if any(
                            word in sibling_text.upper()
                            for word in ["RUA", "AV", "AVENIDA", "PRAC", "EST", "ROD"]
                        ):
                            return sibling_text.strip()

            return None

        except Exception as e:
            self.logger.debug(f"Error finding address near establishment: {e}")
            return None

    def _find_consumer_name(self, soup: BeautifulSoup) -> Optional[str]:
        """Find consumer name in various contexts"""
        try:
            # Look for "Nome" in consumer/CPF context
            cpf_elements = soup.find_all(text=lambda text: text and "CPF" in text)

            for element in cpf_elements:
                if element.parent:
                    # Look for "Nome" near CPF
                    nearby_text = element.parent.get_text()
                    if "Nome" in nearby_text:
                        # Extract name after "Nome"
                        name_match = re.search(r"Nome[:\s]+([^\n\r]+)", nearby_text)
                        if name_match:
                            return name_match.group(1).strip()

            return None

        except Exception as e:
            self.logger.debug(f"Error finding consumer name: {e}")
            return None

    def _find_any_total_value(self, soup: BeautifulSoup) -> Optional[str]:
        """Find any total value as fallback"""
        try:
            # Look for "R$" patterns that might be totals
            total_patterns = [
                r"Total[^\d]*R\$\s*([\d,.]+)",
                r"Valor[^\d]*R\$\s*([\d,.]+)",
                r"R\$\s*([\d,.]+)",
            ]

            page_text = soup.get_text()

            for pattern in total_patterns:
                matches = re.findall(pattern, page_text)
                if matches:
                    # Return the largest value found (likely the total)
                    values = []
                    for match in matches:
                        try:
                            value = float(match.replace(",", "."))
                            values.append((value, f"R$ {match}"))
                        except ValueError:
                            continue

                    if values:
                        # Return the largest value
                        largest = max(values, key=lambda x: x[0])
                        return largest[1]

            return None

        except Exception as e:
            self.logger.debug(f"Error finding any total value: {e}")
            return None

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        return " ".join(text.strip().split())

    def _clean_cnpj(self, text: str) -> str:
        """Extract and clean CNPJ"""
        if not text:
            return ""
        # Remove all non-digits
        digits = re.sub(r"\D", "", text)
        # Return only if it looks like a CNPJ (14 digits)
        return digits if len(digits) == 14 else ""

    def _clean_cpf(self, text: str) -> str:
        """Extract and clean CPF"""
        if not text:
            return ""
        # Remove all non-digits
        digits = re.sub(r"\D", "", text)
        # Return only if it looks like a CPF (11 digits)
        return digits if len(digits) == 11 else ""

    def _parse_currency(self, text: str) -> Optional[Decimal]:
        """Parse currency value from text"""
        if not text:
            return None

        try:
            # Remove currency symbols and normalize
            cleaned = re.sub(r"[R$\s]", "", text)
            cleaned = cleaned.replace(",", ".")

            # Extract decimal number
            match = re.search(r"\d+\.?\d*", cleaned)
            if match:
                return Decimal(match.group())

        except (InvalidOperation, ValueError):
            pass

        return None

    def _parse_decimal(self, text: str) -> Optional[Decimal]:
        """Parse decimal value from text"""
        if not text:
            return None

        try:
            # Remove non-numeric characters except decimal point
            cleaned = re.sub(r"[^\d,.]", "", text)
            cleaned = cleaned.replace(",", ".")

            if cleaned:
                return Decimal(cleaned)

        except (InvalidOperation, ValueError):
            pass

        return None

    def _parse_date(self, text: str) -> Optional[datetime]:
        """Parse date from text"""
        if not text:
            return None

        try:
            # Try different date formats
            date_patterns = [
                r"(\d{2})/(\d{2})/(\d{4})",  # DD/MM/YYYY
                r"(\d{2})/(\d{2})/(\d{2})",  # DD/MM/YY
                r"(\d{4})-(\d{2})-(\d{2})",  # YYYY-MM-DD
            ]

            for pattern in date_patterns:
                match = re.search(pattern, text)
                if match:
                    groups = match.groups()
                    if len(groups) == 3:
                        if len(groups[2]) == 2:  # YY format
                            day, month, year = groups
                            year = int(year)
                            year = 2000 + year if year <= 30 else 1900 + year
                        elif pattern.startswith(r"(\d{4})"):  # YYYY-MM-DD
                            year, month, day = groups
                        else:  # DD/MM/YYYY
                            day, month, year = groups

                        return datetime(int(year), int(month), int(day))

        except (ValueError, TypeError):
            pass

        return None

    def _extract_currency_from_text(self, text: str) -> Optional[Decimal]:
        """Extract currency value from any text"""
        if not text:
            return None

        # Look for currency patterns
        currency_patterns = [
            r"R\$\s*(\d+[,.]?\d*)",
            r"(\d+[,.]?\d*)\s*R\$",
            r"(\d+[,.]?\d*)",
        ]

        for pattern in currency_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                try:
                    cleaned = match.replace(",", ".")
                    value = Decimal(cleaned)
                    if value > 0:
                        return value
                except (InvalidOperation, ValueError):
                    continue

        return None

    def _extract_city_from_address(self, address: str) -> Optional[str]:
        """Try to extract city from address text"""
        if not address:
            return None

        # Simple approach - look for common patterns
        # This would need refinement for production use
        parts = address.split(",")
        if len(parts) >= 2:
            # Usually city is in the last or second-to-last part
            potential_city = parts[-2].strip() if len(parts) > 2 else parts[-1].strip()
            return potential_city if len(potential_city) > 2 else None

        return None

    def _is_empty_nfce_page(self, soup: BeautifulSoup, invoice_data: InvoiceData) -> bool:
        """
        Check if the NFCe page has valid structure but empty data (expired/invalid NFCe)
        
        Args:
            soup: BeautifulSoup object of the page
            invoice_data: Partially extracted invoice data
            
        Returns:
            True if page appears to be empty/expired NFCe
        """
        try:
            # Check if we have the NFCe page structure but empty data
            has_nfce_structure = (
                soup.find(string=lambda text: text and "Nota Fiscal de Consumidor Eletrônica" in text) is not None
                or soup.find("title", string=lambda text: text and "SEF" in text) is not None
                or soup.find("form", id="formPrincipal") is not None
            )
            
            if not has_nfce_structure:
                return False
            
            # Check if critical data fields are all empty
            critical_fields_empty = (
                not invoice_data.invoice_number or invoice_data.invoice_number.strip() == ""
            ) and (
                not invoice_data.series or invoice_data.series.strip() == ""
            ) and (
                invoice_data.total_amount is None or invoice_data.total_amount == 0
            ) and (
                not invoice_data.establishment or not invoice_data.establishment.business_name
            ) and (
                not invoice_data.items or len(invoice_data.items) == 0
            )
            
            # Additional check: look for empty table cells in the main data table
            main_data_tables = soup.find_all("table", class_="table table-hover")
            has_empty_data_tables = False
            
            for table in main_data_tables:
                tbody = table.find("tbody")
                if tbody:
                    rows = tbody.find_all("tr")
                    for row in rows:
                        cells = row.find_all(["td", "th"])
                        # Check if all cells in the row are empty or contain only whitespace
                        if all(not cell.get_text().strip() for cell in cells):
                            has_empty_data_tables = True
                            break
                    if has_empty_data_tables:
                        break
            
            return critical_fields_empty and has_empty_data_tables
            
        except Exception as e:
            self.logger.warning(f"Error checking for empty NFCe page: {e}")
            return False
