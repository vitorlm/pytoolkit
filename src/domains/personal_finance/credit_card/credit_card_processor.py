import datetime
import os
import csv
from typing import Dict, List
from utils.data.json_manager import JSONManager
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager


class CreditCardProcessor:
    """
    Handles the processing of credit card statement CSV files and expense classification.
    """

    _logger = LogManager.get_instance().get_logger("CreditCardProcessor")

    def __init__(self, input_file: str, output_path: str):
        self.input_file = input_file
        self.output_path = output_path
        self.categories = {
            "Supermercado": ["SUPERMERCADOS", "SACOLAO", "PADARIA", "CONFEIT"],
            "Restaurante": ["RESTAURANT", "LANCHES", "PIZZARIA", "CHURRASQUINHO", "COFFEE"],
            "Farmácia": ["DROGARIA", "FARMACIA"],
            "Transporte": ["CONECTCAR", "ESTAPAR", "CONTAPARK", "METRO"],
            "Entretenimento": ["CINEMARK", "EVENTIM", "CLUBE ATLETIC"],
            "Compras Online": ["AMAZON", "MERCADOLIVRE", "PAYPAL", "PADDLE"],
            "Combustível": ["SHELLBOX"],
            "Saúde": ["GYMPASS", "RDSAUDE"],
            "Educação": ["UDEMY"],
            "Seguro": ["ALLIANZ"],
            "Outros": [],  # Default category
        }

    def run(self):
        """Executes the processing of the credit card statement."""
        self._logger.info("Starting credit card processing.")
        self._validate_input_file()
        transactions = self._read_csv()
        categorized_expenses = self._classify_expenses(transactions)
        self._generate_output(categorized_expenses)

    def _validate_input_file(self):
        """Validates the existence of the input file."""
        self._logger.debug("Validating input file.")
        FileManager.validate_file(self.input_file)

    def _read_csv(self) -> List[Dict]:
        """Reads the CSV file and returns a list of transactions."""
        self._logger.info(f"Reading CSV file from: {self.input_file}")
        transactions = []

        with open(self.input_file, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter=";")
            for row in reader:
                try:
                    # Convert date to standard format
                    date = datetime.strptime(row["Data"], "%d/%m/%Y").isoformat()
                    # Convert value to float (removing R$ and converting comma to dot)
                    value = float(row["Valor"].replace("R$", "").replace(",", ".").strip())

                    transactions.append(
                        {
                            "date": date,
                            "establishment": row["Estabelecimento"],
                            "cardholder": row["Portador"],
                            "value": value,
                            "installment": row["Parcela"],
                        }
                    )
                except (ValueError, KeyError) as e:
                    self._logger.warning(f"Skipping malformed row: {row}. Error: {e}")
                    continue

        return transactions

    def _classify_expenses(self, transactions: List[Dict]) -> Dict:
        """Classifies transactions into expense categories."""
        self._logger.info("Classifying expenses.")

        categorized = {category: [] for category in self.categories.keys()}
        total_by_category = {category: 0.0 for category in self.categories.keys()}

        for transaction in transactions:
            category = "Outros"  # Default category
            description = transaction["establishment"].upper()

            # Find matching category
            for cat, keywords in self.categories.items():
                if cat == "Outros":
                    continue
                if any(keyword in description for keyword in keywords):
                    category = cat
                    break

            # Add transaction to category
            categorized[category].append(transaction)
            total_by_category[category] += transaction["value"]

        return {
            "transactions": categorized,
            "totals": total_by_category,
            "transaction_count": len(transactions),
            "total_spent": sum(total_by_category.values()),
        }

    def _generate_output(self, categorized_expenses: Dict):
        """Generates the final output report."""
        self._logger.info(f"Generating output report at {self.output_path}")

        # Ensure output directory exists
        output_dir = os.path.dirname(self.output_path)
        if output_dir:
            FileManager.create_folder(output_dir)

        # Write JSON output
        JSONManager.write_json(categorized_expenses, self.output_path)
        self._logger.info("Credit card processing completed successfully.")
