from typing import Any, Dict, List, Union, Optional
from utils.logging_manager import LogManager
from utils.error_manager import handle_generic_exception
from utils.cache_manager import CacheManager
from .indicators import Indicator

# Initialize logger
logger = LogManager.get_instance().get_logger("ValidationHelper")


class ValidationError(Exception):
    """
    Custom exception class for validation errors.
    """

    def __init__(self, message: str, context: Optional[str] = None):
        super().__init__(message)
        logger.error(f"Validation error: {message}")
        self.context = context


class ValidationHelper:
    """
    Helper class for validating data structures and inputs.
    """

    @staticmethod
    def validate_competency_matrix(competency_matrix: Dict[str, Dict]) -> None:
        """
        Validates the structure and contents of the competency matrix.
        Logs each step and error encountered.

        Args:
            competency_matrix (Dict[str, Dict]): The competency matrix to validate.

        Raises:
            ValidationError: If the competency matrix does not meet the expected structure.
        """
        logger.info("Starting validation for competency matrix.")

        try:
            if not isinstance(competency_matrix, dict):
                raise ValidationError("Competency matrix must be a dictionary.")

            for evaluatee, evaluator_data in competency_matrix.items():
                if not isinstance(evaluatee, str):
                    raise ValidationError(f"Evaluatee name '{evaluatee}' is not a string.")
                if not isinstance(evaluator_data, dict):
                    raise ValidationError(f"Evaluator data for {evaluatee} must be a dictionary.")

                for evaluator, criteria_data in evaluator_data.items():
                    if not isinstance(evaluator, str):
                        raise ValidationError(f"Evaluator name must be a string for {evaluatee}.")
                    if not isinstance(criteria_data, dict):
                        raise ValidationError(
                            f"Criteria data for {evaluator} must be a dictionary."
                        )

                    for criterion, indicators in criteria_data.items():
                        if not isinstance(criterion, str):
                            raise ValidationError(
                                f"Criterion name must be a string for {evaluator}."
                            )
                        if not isinstance(indicators, list):
                            raise ValidationError(f"Indicators for {criterion} must be a list.")

                        for indicator in indicators:
                            if not isinstance(indicator, Indicator):
                                raise ValidationError(
                                    "Each indicator must be an instance of "
                                    f"Indicator in {criterion}."
                                )
                            if not hasattr(indicator, "level") or not isinstance(
                                indicator.level, int
                            ):
                                raise ValidationError(
                                    f"Each indicator in {criterion} must have an integer 'level'."
                                )

            logger.info("Competency matrix validation completed successfully.")
        except Exception as e:
            handle_generic_exception(e, "Error validating competency matrix")

    @staticmethod
    def validate_task_data(task_data: List[Dict[str, Union[str, int]]]) -> None:
        """
        Validates the structure of task data and handles exceptions using ErrorManager.

        Args:
            task_data (List[Dict[str, Union[str, int]]]): Task data to validate.

        Raises:
            ValidationError: If task data does not meet the expected structure.
        """
        logger.info("Starting validation for task data.")

        try:
            if not isinstance(task_data, list):
                raise ValidationError("Task data must be a list of dictionaries.")

            for task in task_data:
                if not isinstance(task, dict):
                    raise ValidationError("Each task must be a dictionary.")
                if "code" not in task or not isinstance(task["code"], str):
                    raise ValidationError("Each task must have a string 'code'.")
                if "type" not in task or not isinstance(task["type"], str):
                    raise ValidationError("Each task must have a string 'type'.")

            logger.info("Task data validation completed successfully.")
        except Exception as e:
            handle_generic_exception(e, "Error validating task data")

    @staticmethod
    def validate_feedback_data(feedback_data: Dict[str, List[Dict[str, Any]]]) -> None:
        """
        Validates the structure of feedback data and logs each step.

        Args:
            feedback_data (Dict[str, List[Dict[str, Any]]]): Feedback data to validate.

        Raises:
            ValidationError: If feedback data does not meet the expected structure.
        """
        logger.info("Starting validation for feedback data.")

        try:
            if not isinstance(feedback_data, dict):
                raise ValidationError("Feedback data must be a dictionary.")

            for evaluator, feedback_list in feedback_data.items():
                if not isinstance(evaluator, str):
                    raise ValidationError("Each evaluator name must be a string.")
                if not isinstance(feedback_list, list):
                    raise ValidationError(f"Feedback for evaluator {evaluator} must be a list.")

                for feedback in feedback_list:
                    if not isinstance(feedback, dict):
                        raise ValidationError("Each feedback entry must be a dictionary.")
                    if "comment" not in feedback or not isinstance(feedback["comment"], str):
                        raise ValidationError("Each feedback entry must have a string 'comment'.")

            logger.info("Feedback data validation completed successfully.")
        except Exception as e:
            handle_generic_exception(e, "Error validating feedback data")

    @staticmethod
    def validate_health_check_data(health_check_data: List[Dict[str, Any]]) -> None:
        """
        Validates health check data and caches results to optimize repeated validations.

        Args:
            health_check_data (List[Dict[str, Any]]): Health check data to validate.

        Raises:
            ValidationError: If health check data does not meet the expected structure.
        """
        cache_key = "health_check_validation"
        cache_manager = CacheManager.get_instance()

        logger.info("Starting validation for health check data.")

        # Try to load from cache
        cached_data = cache_manager.load(cache_key)
        if cached_data:
            logger.info("Using cached validation result for health check data.")
            return

        try:
            if not isinstance(health_check_data, list):
                raise ValidationError("Health check data must be a list of dictionaries.")

            for entry in health_check_data:
                if not isinstance(entry, dict):
                    raise ValidationError("Each health check entry must be a dictionary.")
                if "date" not in entry or not isinstance(entry["date"], str):
                    raise ValidationError("Each health check entry must have a string 'date'.")
                if "effort" in entry and not isinstance(entry["effort"], int):
                    raise ValidationError("'effort' must be an integer if present.")
                if "morale" in entry and not isinstance(entry["morale"], int):
                    raise ValidationError("'morale' must be an integer if present.")

            logger.info("Health check data validation completed successfully.")

            # Save validation in cache
            cache_manager.save(cache_key, {"status": "validated"})
        except Exception as e:
            handle_generic_exception(e, "Error validating health check data")
