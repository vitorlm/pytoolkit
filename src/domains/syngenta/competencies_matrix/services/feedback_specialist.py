from typing import Dict, List, Optional, Union

from langdetect import detect_langs

from utils.logging_manager import LogManager
from utils.ollama_assistant import OllamaAssistant

# Configure logger
logger = LogManager.get_instance().get_logger("FeedbackSpecialist")


class FeedbackSpecialist(OllamaAssistant):
    """
    A specialized assistant for generating professional feedback using OllamaAssistant.
    Focuses on competency matrices and provides structured feedback with actionable insights.
    """

    def __init__(self, host: Optional[str] = None, model: Optional[str] = None, **kwargs):
        """
        Initialize the FeedbackSpecialist with customizable OllamaAssistant configurations.

        Args:
            host: The host for the Ollama server (default is None).
            model: The model to use for inference (default is None).
            **kwargs: Additional configuration parameters for OllamaAssistant.

        Raises:
            ValueError: If invalid types or values are provided for parameters.
        """
        if host is not None and not isinstance(host, str):
            raise ValueError("'host' must be a string or None.")
        if model is not None and not isinstance(model, str):
            raise ValueError("'model' must be a string or None.")

        super().__init__(
            host=host or "http://localhost:11434",
            model=model or "llama3.2",
            **kwargs,
        )

        self.logger = logger

    def _validate_competency_data(self, competency_data: Dict):
        """
        Validates the competency data structure.

        Args:
            competency_data: Dictionary containing competency evaluations.

        Raises:
            ValueError: If competency_data is not a dictionary or has invalid contents.
        """
        if not isinstance(competency_data, dict):
            raise ValueError("competency_data must be a dictionary.")
        for key, value in competency_data.items():
            if not isinstance(key, str):
                raise ValueError(f"Key '{key}' in competency_data must be a string.")
            if not isinstance(value, list):
                raise ValueError(f"Value for key '{key}' in competency_data must be a list.")

    def _create_feedback_prompt(
        self, member_name: str, competency_data: Dict
    ) -> List[Dict[str, str]]:
        """
        Creates a feedback prompt for OllamaAssistant based on competency data.

        Args:
            member_name: Name of the team member.
            competency_data: Dictionary containing competency evaluations.

        Returns:
            List of message dictionaries formatted for the OllamaAssistant.

        Raises:
            ValueError: If member_name is not a string or competency_data is invalid.
        """
        if not isinstance(member_name, str):
            raise ValueError("member_name must be a string.")

        self._validate_competency_data(competency_data)
        self.logger.debug(
            f"Creating feedback prompt for {member_name} with data: {competency_data}"
        )
        return [
            {
                "role": "system",
                "content": (
                    "You are an HR feedback specialist focusing on software engineering "
                    "competencies. "
                    "Your task is to analyze the provided information and generate a "
                    "comprehensive evaluation of the individual's performance, highlighting "
                    "their overall strengths, areas for growth, and potential for development. "
                    "The feedback should be written in a cohesive and explanatory manner, "
                    "avoiding direct references to specific criteria or indicators. "
                    "Ensure the response is professional, detailed, and captures the nuances "
                    "of their performance. "
                    "Provide all feedback in English, regardless of the input language. "
                    "The response should not include bullet "
                    "points, lists, or segmented sections, and instead flow naturally "
                    "as a narrative."
                ),
            },
            {
                "role": "user",
                "content": f"Generate feedback based on this data:\n{competency_data}",
            },
        ]

    def generate_feedback(
        self,
        member_name: str,
        competency_data: Dict,
        include_recommendations: bool = True,
    ) -> Dict[str, str]:
        """
        Generates detailed feedback for a team member.

        Args:
            member_name: Name of the team member.
            competency_data: Dictionary containing competency evaluations.
            include_recommendations: Whether to include growth recommendations.

        Returns:
            Dictionary containing feedback and optional recommendations.

        Raises:
            ValueError: If member_name is not a string or competency_data is invalid.
        """
        if not isinstance(member_name, str):
            raise ValueError("member_name must be a string.")

        self.logger.info(f"Generating feedback for {member_name}.")

        feedback_messages = self._create_feedback_prompt(member_name, competency_data)

        try:
            feedback = self.generate_text(feedback_messages)
            self.logger.debug(f"Feedback generated: {feedback}")
            result = {"feedback": feedback}

            if include_recommendations:
                self.logger.info(f"Generating recommendations for {member_name}.")
                recommendation_messages = [
                    {
                        "role": "system",
                        "content": (
                            "Based on the feedback, provide actionable growth recommendations for "
                            "the individual."
                            " Include:\n"
                            "- Short-term actions (1-3 months)\n"
                            "- Medium-term goals (3-6 months)\n"
                            "- Long-term development objectives (6-12 months)."
                        ),
                    },
                    {"role": "assistant", "content": feedback},
                    {
                        "role": "user",
                        "content": "What specific growth recommendations "
                        f"would you suggest for {member_name}?",
                    },
                ]

                recommendations = self.generate_text(recommendation_messages)
                self.logger.debug(f"Recommendations generated: {recommendations}")
                result["recommendations"] = recommendations

            return result

        except Exception as e:
            self.logger.error(
                f"Error generating feedback for {member_name}: {e}",
                exc_info=True,
            )
            return {"error": str(e)}

    def summarize_evidence(
        self,
        evaluatee_name: str,
        criteria: str,
        indicator: str,
        data: Dict[
            str,
            Union[List[Dict[str, Union[int, str]]], float, Dict[str, float]],
        ],
    ) -> str:
        """
        Summarizes evidence for a specific indicator within a criterion.

        Args:
            evaluatee_name (str): The name of the individual being evaluated.
            criteria (str): The name of the competency criterion being evaluated.
            indicator (str): The specific indicator for the criterion.
            data (Dict[str, Union[List[Dict[str, Union[int, str]]], float, Dict[str, float]]]):
                A dictionary containing:
                    - "evidence_list" (List[Dict[str, Union[int, str]]]):
                        List of dictionaries with "level" and "text".
                    - "average" (float): The average level for the indicator.
                    - "highest" (float): The highest level for the indicator.
                    - "lowest" (float): The lowest level for the indicator.
                    - "team_comparison" (Dict[str, float]):
                        Comparison values for "average", "highest", and "lowest".

        Returns:
            str: Summarized evidence string.

        Raises:
            ValueError: If input types are not as expected.
        """
        # Input validation
        if not isinstance(evaluatee_name, str):
            raise ValueError("evaluatee_name must be a string.")
        if not isinstance(criteria, str):
            raise ValueError("criteria must be a string.")
        if not isinstance(indicator, str):
            raise ValueError("indicator must be a string.")
        if not isinstance(data, dict):
            raise ValueError("data must be a dictionary.")

        evidence_list = data.get("evidence_list")
        average = data.get("average")
        highest = data.get("highest")
        lowest = data.get("lowest")
        team_comparison = data.get("team_comparison")

        if not isinstance(evidence_list, list) or not all(
            isinstance(item, dict) and "level" in item and "text" in item for item in evidence_list
        ):
            raise ValueError(
                "evidence_list must be a list of dictionaries containing 'level' and 'text'."
            )

        if (
            not isinstance(average, (float, int))
            or not isinstance(highest, (float, int))
            or not isinstance(lowest, (float, int))
        ):
            raise ValueError("average, highest, and lowest must be numeric values.")

        if not isinstance(team_comparison, dict) or not all(
            key in team_comparison for key in ["average", "highest", "lowest"]
        ):
            raise ValueError(
                "team_comparison must be a dictionary containing 'average', 'highest', "
                "and 'lowest'."
            )

        if not evidence_list:
            self.logger.warning(f"No evidence provided for indicator: {indicator}")
            return f"No evidence provided for {evaluatee_name}."

        # Preparing evidence text for summarization
        evidence_text = "\n".join(
            f"Level {item['level']}: {item['text']} (justifying the assigned level)"
            for item in evidence_list
        )

        self.logger.info(
            f"Summarizing evidence for {evaluatee_name} on indicator: {indicator} "
            f"in criteria: {criteria}."
        )

        # Constructing prompt for summarization
        summary_messages = [
            {
                "role": "system",
                "content": (
                    "You are an expert in evaluating and summarizing performance evidence. "
                    "Your task is to analyze the evidence provided for the indicator "
                    f"'{indicator}' within the criterion '{criteria}' for the individual "
                    f"'{evaluatee_name}'. Use the following data for context:\n"
                    f"- Average Level: {average}\n"
                    f"- Highest Level: {highest}\n"
                    f"- Lowest Level: {lowest}\n"
                    f"- Team Comparison (Average): {team_comparison['average']}\n"
                    f"- Team Comparison (Highest): {team_comparison['highest']}\n"
                    f"- Team Comparison (Lowest): {team_comparison['lowest']}\n"
                    "Your summary should highlight how each piece of evidence justifies "
                    "the assigned level, reflect the individual's performance relative to "
                    "the team's metrics, and identify any notable patterns or trends. "
                    "Ensure the summary is detailed, capturing the nuances of the evidence, "
                    f"and provides a clear and comprehensive evaluation of {evaluatee_name}'s "
                    "performance. Avoid introductions or additional context, "
                    "and focus solely on summarizing the evidence effectively."
                ),
            },
            {
                "role": "user",
                "content": f"Summarize this evidence:\n{evidence_text}",
            },
        ]

        try:
            # Generating summary using the assistant
            summary = self.generate_text(summary_messages)
            self.logger.debug(f"Evidence summary for {evaluatee_name} on {indicator}: {summary}")
            return summary
        except Exception as e:
            self.logger.error(
                f"Error summarizing evidence for {evaluatee_name} on indicator '{indicator}' "
                f"in criteria '{criteria}': {e}",
                exc_info=True,
            )
            return f"Error summarizing evidence for {evaluatee_name}: {e}"

    def analyze_matrix(self, competency_matrix: Dict[str, Dict]) -> Dict[str, str]:
        """
        Analyzes an entire competency matrix and generates a summary for each team member.

        Args:
            competency_matrix: A dictionary mapping team members to their competency data.

        Returns:
            A dictionary of summaries and recommendations for all team members.

        Raises:
            ValueError: If competency_matrix is not a dictionary or its contents are invalid.
        """
        if not isinstance(competency_matrix, dict):
            raise ValueError("competency_matrix must be a dictionary.")

        self.logger.info("Analyzing competency matrix for all team members.")
        results = {}
        for member_name, data in competency_matrix.items():
            if not isinstance(member_name, str):
                raise ValueError("Each member_name in competency_matrix must be a string.")

            self.logger.info(f"Processing feedback for {member_name}.")
            results[member_name] = self.generate_feedback(member_name, data)
        self.logger.info("Analysis of competency matrix completed.")
        return results

    def translate_evidence(self, evidence: str, expected_language_code: str = "en") -> str:
        """
        Translates the given evidence to the expected language if necessary.

        This method detects the language of the provided evidence and translates it to the
        expected language if the detected language is different. It supports translation to
        English, Spanish, and Portuguese.

        Args:
            evidence (str): The evidence text to be translated.
            expected_language_code (str): The expected language for the translation.
                                          Must be one of 'en', 'es', or 'pt'.
                                          Default is 'en' (English).

        Returns:
            str: The translated evidence text if translation was necessary,
                 otherwise the original evidence text.

        Raises:
            ValueError: If the evidence is not a string or if the expected_language
                        is not one of 'en', 'es', or 'pt'.
        """
        if not isinstance(evidence, str):
            raise ValueError("evidence must be a string.")

        if not isinstance(expected_language_code, str) and expected_language_code not in (
            "en",
            "es",
            "pt",
        ):
            raise ValueError("expected_language_code must be 'en', 'es', or 'pt'.")

        languages = {"en": "English", "es": "Spanish", "pt": "Portuguese"}

        self.logger.debug("Detecting language of evidence:")
        detected_languages = detect_langs(evidence)
        lang_scores = {lang.lang: lang.prob for lang in detected_languages}

        expected_language = languages.get(expected_language_code.lower())

        if lang_scores.get(expected_language_code, 0) > 0.9:
            self.logger.debug(f"Evidence is already in {expected_language}.")
            return evidence
        else:
            self.logger.info(f"Translating evidence to {expected_language}.")

            try:
                translation = self.translate_text(evidence, expected_language)
                self.logger.info("Translation successful.")
                return translation
            except Exception as e:
                self.logger.error(f"Error translating evidence: {e}", exc_info=True)
                return f"Error translating evidence: {e}"
