import re
from pathlib import Path

from domains.syngenta.team_assessment.core.workday import WorkdayRatingPair, WorkdayRatings
from utils.logging.logging_manager import LogManager
from utils.pdf.pdf_extractor import PdfExtractor


class WorkdayProcessor:
    """Processes Workday evaluation and feedback PDFs."""

    def __init__(self) -> None:
        self.logger = LogManager.get_instance().get_logger("WorkdayProcessor")
        self.pdf_extractor = PdfExtractor()

    def parse_evaluation_pdf(self, file_path: str) -> WorkdayRatings | None:
        """Parses avaliacao-what-how.pdf to extract WHAT/HOW ratings and comments.

        Args:
            file_path: Path to the evaluation PDF.

        Returns:
            WorkdayRatings object or None if extraction fails.
        """
        try:
            # 1. Extract ratings using full text (most reliable for simple tokens)
            full_text = self.pdf_extractor.extract_full_text(file_path)
            ratings_matches = re.findall(r"Evaluation:\s*(PA|PE|EX)", full_text, re.IGNORECASE)

            ratings = WorkdayRatings()
            if len(ratings_matches) >= 1:
                ratings.manager.what = ratings_matches[0].upper()
            if len(ratings_matches) >= 2:
                ratings.manager.how = ratings_matches[1].upper()

            # Handle self-eval if there are 4 ratings
            if len(ratings_matches) >= 4:
                ratings.self_eval = WorkdayRatingPair(what=ratings_matches[2].upper(), how=ratings_matches[3].upper())

            # 2. Extract comments using two-column layout
            columns = self.pdf_extractor.extract_columns_text(file_path, columns=2)
            manager_text = "\n".join([page[0] for page in columns])
            employee_text = "\n".join([page[1] for page in columns])

            def extract_comments(text: str, pair: WorkdayRatingPair):
                # find all segments starting with "Resposta:"
                # Split the text by "Resposta:"
                segments = re.split(r"Resposta:", text, flags=re.IGNORECASE)
                # First segment is header, skip it
                if len(segments) > 1:
                    # Segment 1 is WHAT comment
                    # Clean up: remove "Evaluation: ..." and metadata headers
                    comment_1 = segments[1]
                    comment_1 = re.sub(r"Evaluation:\s*[A-Z]+", "", comment_1, flags=re.IGNORECASE)
                    comment_1 = re.sub(r"Forneça uma avaliação para.*", "", comment_1, flags=re.IGNORECASE | re.DOTALL)
                    comment_1 = re.sub(r"Avaliação do (gestor|colaborador)", "", comment_1, flags=re.IGNORECASE)
                    pair.what_comment = comment_1.strip()

                if len(segments) > 2:
                    # Segment 2 is HOW comment
                    comment_2 = segments[2]
                    comment_2 = re.sub(r"Evaluation:\s*[A-Z]+", "", comment_2, flags=re.IGNORECASE)
                    comment_2 = re.sub(r"Forneça uma avaliação para.*", "", comment_2, flags=re.IGNORECASE | re.DOTALL)
                    comment_2 = re.sub(r"Avaliação do (gestor|colaborador)", "", comment_2, flags=re.IGNORECASE)
                    pair.how_comment = comment_2.strip()

            # Extract manager comments from left column
            extract_comments(manager_text, ratings.manager)

            # Extract employee comments from right column
            if not ratings.self_eval:
                ratings.self_eval = WorkdayRatingPair()
            extract_comments(employee_text, ratings.self_eval)

            # Cleanup self_eval if empty
            if not ratings.self_eval.what and not ratings.self_eval.what_comment:
                ratings.self_eval = None

            self.logger.info(f"Successfully parsed detailed ratings/comments from {file_path}")
            return ratings

        except Exception as e:
            self.logger.warning(f"Failed to parse detailed evaluation PDF {file_path}: {e}")
            return None

    def parse_feedback_pdf(self, file_path: str) -> str:
        """Parses feedback PDF and returns cleaned text.

        Args:
            file_path: Path to the feedback PDF.

        Returns:
            Cleaned feedback text.
        """
        exclude_patterns = [
            r"Proprietário e Confidencial",
            r"Visualizar feedback",
            r"Pergunta",
            r"\d{2}/\d{2}/\d{4}",
            r"Página \d+ de \d+",
            r"\d{2}:\d{2}\s",  # Timestamp check
            r"Foto\s",  # "Foto" label usually precedes author name in PDF
        ]

        try:
            cleaned_text = self.pdf_extractor.extract_text_with_filter(file_path, exclude_patterns)

            # Additional pass to strip inline boilerplate that might be on the same line
            # 1. Strip timestamps (HH:MM)
            cleaned_text = re.sub(r"\b\d{2}:\d{2}\b", "", cleaned_text)

            # 2. Strip "Foto [Name]" (Workday profile indicator)
            cleaned_text = re.sub(r"Foto\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*", "", cleaned_text)

            # 3. Strip "Fornecido em ..." (Date provided)
            # Handle variations in dates and "Fornecido" presence
            cleaned_text = re.sub(
                r"(Fornecido\s+)?em\s+\d{1,2}\s+de\s+\w+\.?\s+de\s+\d{4}", "", cleaned_text, flags=re.IGNORECASE
            )

            # 4. Strip common Workday redundant phrases
            redundant_phrases = [
                r"I’d love to ask for your feedback as we wrap up the year\.",
                r"As we close out the year, I’d love to get your feedback",
                r"As we wrap up the year, I wanted to kindly ask for your feedback",
                r"We interacted a few times this year",
                r"I’d love to get your feedback on how our collaboration worked",
                r"on how we worked together in 2025",
            ]
            for phrase in redundant_phrases:
                cleaned_text = re.sub(phrase, "", cleaned_text, flags=re.IGNORECASE)

            # 5. Strip the "Feedback" label if it's hanging at the start of a paragraph
            cleaned_text = re.sub(r"^\s*Feedback\s+", "", cleaned_text, flags=re.IGNORECASE | re.MULTILINE)

            # Final cleanup of whitespace and leading/trailing punctuation
            cleaned_text = re.sub(r"\n\s*\n+", "\n\n", cleaned_text)
            cleaned_text = cleaned_text.strip()
            # Remove leading dots or commas often left from phrase removal
            cleaned_text = re.sub(r"^[.,\s;:-]+", "", cleaned_text)

            return cleaned_text.strip()
        except Exception as e:
            self.logger.warning(f"Failed to parse feedback PDF {file_path}: {e}")
            return ""

    def parse_authors_from_filename(self, filename: str) -> list[str]:
        """Extracts author names from filename.

        Example:
            "barbara-aguiar--marcela-almeida.pdf" -> ["Barbara Aguiar", "Marcela Almeida"]
            "mariana-ribeiro.pdf" -> ["Mariana Ribeiro"]
        """
        name_without_ext = Path(filename).stem

        if "--" in name_without_ext:
            author_segments = name_without_ext.split("--")
        else:
            # If no -- separator, treat the whole filename as one author
            author_segments = [name_without_ext]

        authors = []
        for segment in author_segments:
            if not segment or segment.lower() == "feedback":
                continue
            # "john-doe" -> "John Doe"
            author_name = " ".join(part.capitalize() for part in segment.split("-"))
            authors.append(author_name)

        return authors
