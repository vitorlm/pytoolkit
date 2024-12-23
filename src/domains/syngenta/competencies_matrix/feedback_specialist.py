import os
from typing import Dict, List, Optional

from log_config import log_manager
from utils.ollama_assistant import OllamaAssistant

logger = log_manager.get_logger(os.path.splitext(os.path.basename(__file__))[0])


class FeedbackSpecialist(OllamaAssistant):
    """
    A specialized assistant for generating professional feedback using OllamaAssistant.
    Focuses on competency matrices and provides structured feedback with actionable insights.
    """

    def __init__(
        self, host: Optional[str] = None, model: Optional[str] = None, **kwargs
    ):
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
                raise ValueError(
                    f"Value for key '{key}' in competency_data must be a list."
                )

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
                    "You are an HR feedback specialist focusing on software engineering competencies. "
                    "Your task is to analyze the provided information and generate a comprehensive evaluation of the individual's"
                    "performance, highlighting their overall strengths, areas for growth, and potential for development. "
                    "The feedback should be written in a cohesive and explanatory manner, avoiding direct references to specific "
                    "criteria or indicators. "
                    "Ensure the response is professional, detailed, and captures the nuances of their performance. "
                    "Provide all feedback in English, regardless of the input language. The response should not include bullet "
                    "points, lists, or segmented sections, and instead flow naturally as a narrative."
                ),
            },
            {
                "role": "user",
                "content": f"Generate feedback for {member_name} based on this data:\n{competency_data}",
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

        self._validate_competency_data(competency_data)
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
                            "Based on the feedback, provide actionable growth recommendations for the individual."
                            " Include:\n"
                            "- Short-term actions (1-3 months)\n"
                            "- Medium-term goals (3-6 months)\n"
                            "- Long-term development objectives (6-12 months)."
                        ),
                    },
                    {"role": "assistant", "content": feedback},
                    {
                        "role": "user",
                        "content": f"What specific growth recommendations would you suggest for {member_name}?",
                    },
                ]

                recommendations = self.generate_text(recommendation_messages)
                self.logger.debug(f"Recommendations generated: {recommendations}")
                result["recommendations"] = recommendations

            return result

        except Exception as e:
            self.logger.error(
                f"Error generating feedback for {member_name}: {e}", exc_info=True
            )
            return {"error": str(e)}

    def summarize_evidence(
        self, evidence_list: List[str], criteria: str, current_level: int
    ) -> str:
        """
        Summarizes evidence for a specific competency.

        Args:
            evidence_list: List of evidence strings.
            criteria: The competency criteria being evaluated.
            current_level: Current competency level (1-5).

        Returns:
            Summarized evidence string.

        Raises:
            ValueError: If evidence_list is not a list or criteria is not a string.
        """
        if not isinstance(evidence_list, list):
            raise ValueError("evidence_list must be a list.")
        if not isinstance(criteria, str):
            raise ValueError("criteria must be a string.")

        if not evidence_list:
            self.logger.warning(f"No evidence provided for criteria: {criteria}")
            return "No evidence provided."

        evidence_text = "\n".join(evidence_list)
        self.logger.info(
            f"Summarizing evidence for criteria: {criteria} at level {current_level}."
        )
        summary_messages = [
            {
                "role": "system",
                "content": (
                    f"Summarize the evidence for the {criteria} competency at Level {current_level}."
                    " Highlight achievements, areas for improvement, and growth opportunities."
                ),
            },
            {"role": "user", "content": f"Summarize this evidence:\n{evidence_text}"},
        ]

        try:
            summary = self.generate_text(summary_messages)
            self.logger.debug(f"Evidence summary: {summary}")
            return summary
        except Exception as e:
            self.logger.error(
                f"Error summarizing evidence for criteria '{criteria}': {e}",
                exc_info=True,
            )
            return f"Error summarizing evidence: {e}"

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
                raise ValueError(
                    "Each member_name in competency_matrix must be a string."
                )

            self.logger.info(f"Processing feedback for {member_name}.")
            results[member_name] = self.generate_feedback(member_name, data)
        self.logger.info("Analysis of competency matrix completed.")
        return results
