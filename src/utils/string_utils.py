import unicodedata


class StringUtils:
    @staticmethod
    def remove_accents(text: str) -> str:
        """
        Removes accents from the given text.

        Args:
            text (str): The input text with accents.

        Returns:
            str: The text without accents.
        """
        # Normalize text to decompose accented characters
        normalized_text = unicodedata.normalize("NFD", text)
        # Remove combining characters (accents)
        text_without_accents = "".join(
            char for char in normalized_text if not unicodedata.combining(char)
        )
        return text_without_accents

    @staticmethod
    def compare_words(word1: str, word2: str) -> bool:
        """
        Compares two words, ignoring accents and case sensitivity.

        Args:
            word1 (str): The first word to compare.
            word2 (str): The second word to compare.

        Returns:
            bool: True if the words are equivalent without accents, False otherwise.
        """
        return (
            StringUtils.remove_accents(word1).lower() == StringUtils.remove_accents(word2).lower()
        )
