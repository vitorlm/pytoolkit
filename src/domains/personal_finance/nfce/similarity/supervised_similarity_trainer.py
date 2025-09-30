#!/usr/bin/env python3
"""
Supervised Similarity Trainer - Learn from user feedback to improve similarity detection
"""

import numpy as np
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
import pickle
from pathlib import Path

from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix, roc_auc_score
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler

from utils.logging.logging_manager import LogManager
from utils.data.json_manager import JSONManager
from .feature_extractor import ProductFeatures
from .similarity_calculator import SimilarityCalculator


@dataclass
class TrainingExample:
    """Training example with features and user feedback"""

    product1_description: str
    product2_description: str
    product1_features: Dict
    product2_features: Dict
    similarity_scores: Dict  # Individual algorithm scores
    user_label: bool  # True if user says they're similar, False otherwise
    confidence: float  # User confidence (0-1)
    timestamp: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    context: Optional[Dict] = None


@dataclass
class ModelPerformance:
    """Model performance metrics"""

    accuracy: float
    precision: float
    recall: float
    f1_score: float
    auc_score: float
    confusion_matrix: List[List[int]]
    training_examples_count: int
    model_version: str
    timestamp: str


class SupervisedSimilarityTrainer:
    """
    Learn optimal similarity thresholds and weights from user feedback

    Features:
    - Collect user feedback on product pairs
    - Train ML models to optimize similarity detection
    - Active learning for intelligent sample selection
    - Model performance tracking and validation
    - Automatic threshold optimization
    """

    def __init__(self, data_dir: str = "data/similarity_training"):
        self.logger = LogManager.get_instance().get_logger(
            "SupervisedSimilarityTrainer"
        )
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Training data storage
        self.training_examples_file = self.data_dir / "training_examples.json"
        self.model_file = self.data_dir / "similarity_model.pkl"
        self.performance_history_file = self.data_dir / "performance_history.json"

        # Components
        self.similarity_calculator = SimilarityCalculator()

        # ML Models
        self.models = {
            "random_forest": RandomForestClassifier(n_estimators=100, random_state=42),
            "gradient_boosting": GradientBoostingClassifier(random_state=42),
            "logistic_regression": LogisticRegression(random_state=42),
        }
        self.current_model = None
        self.scaler = StandardScaler()

        # Training data
        self.training_examples = []
        self.load_training_data()

        # Performance tracking
        self.performance_history = []
        self.load_performance_history()

        self.logger.info("Supervised Similarity Trainer initialized")

    def add_training_example(
        self,
        product1: str,
        product2: str,
        product1_features: ProductFeatures,
        product2_features: ProductFeatures,
        user_says_similar: bool,
        confidence: float = 1.0,
        user_id: Optional[str] = None,
        context: Optional[Dict] = None,
    ) -> TrainingExample:
        """
        Add a training example from user feedback

        Args:
            product1: First product description
            product2: Second product description
            product1_features: Features for first product
            product2_features: Features for second product
            user_says_similar: Whether user thinks they're similar
            confidence: User confidence level (0-1)
            user_id: Optional user identifier
            context: Optional context information

        Returns:
            Created TrainingExample
        """
        # Calculate similarity scores
        similarity_result = self.similarity_calculator.calculate_similarity(
            product1_features, product2_features
        )

        # Create training example
        example = TrainingExample(
            product1_description=product1,
            product2_description=product2,
            product1_features=self._features_to_dict(product1_features),
            product2_features=self._features_to_dict(product2_features),
            similarity_scores={
                "jaccard": similarity_result.jaccard_score,
                "cosine": similarity_result.cosine_score,
                "levenshtein": similarity_result.levenshtein_score,
                "token_overlap": similarity_result.token_overlap_score,
                "final_score": similarity_result.final_score,
            },
            user_label=user_says_similar,
            confidence=confidence,
            timestamp=datetime.now().isoformat(),
            user_id=user_id,
            context=context,
        )

        # Add to training data
        self.training_examples.append(example)

        # Save immediately
        self.save_training_data()

        self.logger.info(
            f"Added training example: {product1[:30]}... vs {product2[:30]}... -> {user_says_similar}"
        )

        return example

    def train_model(
        self,
        model_type: str = "random_forest",
        test_size: float = 0.2,
        validate_model: bool = True,
    ) -> ModelPerformance:
        """
        Train similarity detection model on collected examples

        Args:
            model_type: Type of model to train
            test_size: Proportion of data for testing
            validate_model: Whether to perform validation

        Returns:
            ModelPerformance with training results
        """
        if len(self.training_examples) < 10:
            raise ValueError(
                f"Need at least 10 training examples, have {len(self.training_examples)}"
            )

        self.logger.info(
            f"Training {model_type} model with {len(self.training_examples)} examples"
        )

        # Prepare features and labels
        X, y = self._prepare_training_data()

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42, stratify=y
        )

        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        # Train model
        model = self.models[model_type]
        model.fit(X_train_scaled, y_train)

        # Make predictions
        y_pred = model.predict(X_test_scaled)
        y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]

        # Calculate performance metrics
        from sklearn.metrics import (
            accuracy_score,
            precision_score,
            recall_score,
            f1_score,
        )

        performance = ModelPerformance(
            accuracy=float(accuracy_score(y_test, y_pred)),
            precision=float(precision_score(y_test, y_pred)),
            recall=float(recall_score(y_test, y_pred)),
            f1_score=float(f1_score(y_test, y_pred)),
            auc_score=float(roc_auc_score(y_test, y_pred_proba)),
            confusion_matrix=confusion_matrix(y_test, y_pred).tolist(),
            training_examples_count=len(self.training_examples),
            model_version=f"{model_type}_v{len(self.performance_history) + 1}",
            timestamp=datetime.now().isoformat(),
        )

        # Store model and performance
        self.current_model = model
        self.performance_history.append(performance)

        # Save model and performance
        self.save_model()
        self.save_performance_history()

        self.logger.info(
            f"Model training completed. Accuracy: {performance.accuracy:.3f}, F1: {performance.f1_score:.3f}"
        )

        if validate_model:
            self._validate_model(X_train_scaled, y_train)

        return performance

    def predict_similarity(
        self, product1_features: ProductFeatures, product2_features: ProductFeatures
    ) -> Tuple[bool, float]:
        """
        Predict if two products are similar using trained model

        Args:
            product1_features: Features for first product
            product2_features: Features for second product

        Returns:
            Tuple of (is_similar, confidence_score)
        """
        if self.current_model is None:
            # Fallback to original similarity calculator
            result = self.similarity_calculator.calculate_similarity(
                product1_features, product2_features
            )
            return result.final_score > 0.7, result.final_score

        # Extract features for ML model
        features = self._extract_ml_features(product1_features, product2_features)
        features_scaled = self.scaler.transform([features])

        # Predict
        is_similar = bool(self.current_model.predict(features_scaled)[0])
        confidence = float(self.current_model.predict_proba(features_scaled)[0].max())

        return is_similar, confidence

    def get_optimal_threshold(
        self, target_precision: float = 0.8, target_recall: float = 0.8
    ) -> float:
        """
        Find optimal similarity threshold based on training data

        Args:
            target_precision: Target precision level
            target_recall: Target recall level

        Returns:
            Optimal threshold value
        """
        if not self.training_examples:
            return 0.7  # Default threshold

        # Extract similarity scores and labels
        scores = []
        labels = []

        for example in self.training_examples:
            scores.append(example.similarity_scores["final_score"])
            labels.append(example.user_label)

        scores = np.array(scores)
        labels = np.array(labels)

        # Try different thresholds
        thresholds = np.arange(0.1, 1.0, 0.05)
        best_threshold = 0.7
        best_f1 = 0.0

        for threshold in thresholds:
            predictions = scores >= threshold

            if len(np.unique(predictions)) < 2:
                continue

            from sklearn.metrics import precision_score, recall_score, f1_score

            try:
                precision = precision_score(labels, predictions)
                recall = recall_score(labels, predictions)
                f1 = f1_score(labels, predictions)

                # Check if meets minimum requirements and has better F1
                if (
                    precision >= target_precision
                    and recall >= target_recall
                    and f1 > best_f1
                ):
                    best_threshold = threshold
                    best_f1 = f1
            except:
                continue

        self.logger.info(
            f"Optimal threshold found: {best_threshold:.3f} (F1: {best_f1:.3f})"
        )
        return best_threshold

    def suggest_training_examples(
        self,
        candidate_pairs: List[Tuple[ProductFeatures, ProductFeatures]],
        n_suggestions: int = 10,
    ) -> List[Tuple[ProductFeatures, ProductFeatures, float]]:
        """
        Suggest product pairs for manual labeling using uncertainty sampling

        Args:
            candidate_pairs: List of product feature pairs
            n_suggestions: Number of suggestions to return

        Returns:
            List of (features1, features2, uncertainty_score) sorted by uncertainty
        """
        suggestions = []

        for features1, features2 in candidate_pairs:
            # Calculate similarity with current method
            similarity_result = self.similarity_calculator.calculate_similarity(
                features1, features2
            )

            # Calculate uncertainty score
            # Uncertainty is highest when similarity is near decision boundary
            decision_boundary = 0.7  # Default threshold
            uncertainty = 1.0 - abs(similarity_result.final_score - decision_boundary)

            # Additional uncertainty factors
            if self.current_model is not None:
                # Use ML model uncertainty if available
                features = self._extract_ml_features(features1, features2)
                features_scaled = self.scaler.transform([features])
                proba = self.current_model.predict_proba(features_scaled)[0]
                ml_uncertainty = 1.0 - abs(
                    proba[1] - proba[0]
                )  # Closer to 0.5 = more uncertain
                uncertainty = (uncertainty + ml_uncertainty) / 2

            suggestions.append((features1, features2, uncertainty))

        # Sort by uncertainty (highest first) and return top N
        suggestions.sort(key=lambda x: x[2], reverse=True)
        return suggestions[:n_suggestions]

    def _prepare_training_data(self) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare training data for ML model"""
        X = []
        y = []

        for example in self.training_examples:
            # Extract features
            features = [
                example.similarity_scores["jaccard"],
                example.similarity_scores["cosine"],
                example.similarity_scores["levenshtein"],
                example.similarity_scores["token_overlap"],
                example.similarity_scores["final_score"],
                # Additional features from product properties
                len(example.product1_description),
                len(example.product2_description),
                abs(
                    len(example.product1_description)
                    - len(example.product2_description)
                ),
                # Brand and category matches (if available)
                1.0
                if example.product1_features.get("brand")
                == example.product2_features.get("brand")
                else 0.0,
                1.0
                if example.product1_features.get("category")
                == example.product2_features.get("category")
                else 0.0,
                # User confidence
                example.confidence,
            ]

            X.append(features)
            y.append(example.user_label)

        return np.array(X), np.array(y)

    def _extract_ml_features(
        self, features1: ProductFeatures, features2: ProductFeatures
    ) -> List[float]:
        """Extract features for ML model from ProductFeatures"""
        # Calculate similarity scores
        similarity_result = self.similarity_calculator.calculate_similarity(
            features1, features2
        )

        return [
            similarity_result.jaccard_score,
            similarity_result.cosine_score,
            similarity_result.levenshtein_score,
            similarity_result.token_overlap_score,
            similarity_result.final_score,
            len(features1.original_description),
            len(features2.original_description),
            abs(
                len(features1.original_description)
                - len(features2.original_description)
            ),
            1.0 if features1.brand == features2.brand else 0.0,
            1.0 if features1.category == features2.category else 0.0,
            1.0,  # Default confidence
        ]

    def _validate_model(self, X_train: np.ndarray, y_train: np.ndarray):
        """Perform cross-validation on the model"""
        if self.current_model is None:
            return

        cv_scores = cross_val_score(
            self.current_model, X_train, y_train, cv=5, scoring="f1"
        )
        self.logger.info(f"Cross-validation F1 scores: {cv_scores}")
        self.logger.info(
            f"Mean CV F1 score: {cv_scores.mean():.3f} (+/- {cv_scores.std() * 2:.3f})"
        )

    def _features_to_dict(self, features: ProductFeatures) -> Dict:
        """Convert ProductFeatures to dictionary"""
        return {
            "original_description": features.original_description,
            "normalized_description": features.normalized_description,
            "tokens": features.tokens,
            "bigrams": features.bigrams,
            "core_key": features.core_key,
            "brand": features.brand,
            "category": features.category,
        }

    def save_training_data(self):
        """Save training examples to file"""
        try:
            data = [asdict(example) for example in self.training_examples]
            JSONManager.write_json(data, str(self.training_examples_file))
            self.logger.debug(f"Saved {len(self.training_examples)} training examples")
        except Exception as e:
            self.logger.error(f"Error saving training data: {e}")

    def load_training_data(self):
        """Load training examples from file"""
        try:
            if self.training_examples_file.exists():
                data = JSONManager.read_json(str(self.training_examples_file))
                self.training_examples = [
                    TrainingExample(**example) for example in data
                ]
                self.logger.info(
                    f"Loaded {len(self.training_examples)} training examples"
                )
        except Exception as e:
            self.logger.error(f"Error loading training data: {e}")
            self.training_examples = []

    def save_model(self):
        """Save trained model to file"""
        try:
            if self.current_model is not None:
                model_data = {
                    "model": self.current_model,
                    "scaler": self.scaler,
                    "timestamp": datetime.now().isoformat(),
                }
                with open(self.model_file, "wb") as f:
                    pickle.dump(model_data, f)
                self.logger.info("Model saved successfully")
        except Exception as e:
            self.logger.error(f"Error saving model: {e}")

    def load_model(self):
        """Load trained model from file"""
        try:
            if self.model_file.exists():
                with open(self.model_file, "rb") as f:
                    model_data = pickle.load(f)
                self.current_model = model_data["model"]
                self.scaler = model_data["scaler"]
                self.logger.info("Model loaded successfully")
        except Exception as e:
            self.logger.error(f"Error loading model: {e}")

    def save_performance_history(self):
        """Save performance history to file"""
        try:
            data = [asdict(perf) for perf in self.performance_history]
            JSONManager.write_json(data, str(self.performance_history_file))
            self.logger.debug(
                f"Saved performance history: {len(self.performance_history)} entries"
            )
        except Exception as e:
            self.logger.error(f"Error saving performance history: {e}")

    def load_performance_history(self):
        """Load performance history from file"""
        try:
            if self.performance_history_file.exists():
                data = JSONManager.read_json(str(self.performance_history_file))
                self.performance_history = [ModelPerformance(**perf) for perf in data]
                self.logger.info(
                    f"Loaded performance history: {len(self.performance_history)} entries"
                )
        except Exception as e:
            self.logger.error(f"Error loading performance history: {e}")
            self.performance_history = []

    def get_training_statistics(self) -> Dict:
        """Get statistics about training data"""
        if not self.training_examples:
            return {"message": "No training data available"}

        total_examples = len(self.training_examples)
        positive_examples = sum(1 for ex in self.training_examples if ex.user_label)
        negative_examples = total_examples - positive_examples

        # Calculate average confidence by label
        positive_confidence = np.mean(
            [ex.confidence for ex in self.training_examples if ex.user_label]
        )
        negative_confidence = np.mean(
            [ex.confidence for ex in self.training_examples if not ex.user_label]
        )

        return {
            "total_examples": total_examples,
            "positive_examples": positive_examples,
            "negative_examples": negative_examples,
            "positive_ratio": positive_examples / total_examples,
            "average_positive_confidence": float(positive_confidence)
            if positive_examples > 0
            else 0.0,
            "average_negative_confidence": float(negative_confidence)
            if negative_examples > 0
            else 0.0,
            "models_trained": len(self.performance_history),
            "current_model_available": self.current_model is not None,
        }

    def export_training_data(self, output_file: str, format: str = "json"):
        """Export training data for external analysis"""
        try:
            if format == "json":
                data = [asdict(example) for example in self.training_examples]
                JSONManager.write_json(data, output_file)
            elif format == "csv":
                import pandas as pd

                # Flatten data for CSV
                rows = []
                for example in self.training_examples:
                    row = {
                        "product1": example.product1_description,
                        "product2": example.product2_description,
                        "user_label": example.user_label,
                        "confidence": example.confidence,
                        "timestamp": example.timestamp,
                        **example.similarity_scores,
                    }
                    rows.append(row)

                df = pd.DataFrame(rows)
                df.to_csv(output_file, index=False)

            self.logger.info(f"Training data exported to {output_file}")
        except Exception as e:
            self.logger.error(f"Error exporting training data: {e}")
            raise
