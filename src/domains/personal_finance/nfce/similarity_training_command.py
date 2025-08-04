#!/usr/bin/env python3
"""
Similarity Training Command - Interactive training data collection for similarity algorithms
"""

import os
import sys
from argparse import ArgumentParser, Namespace
from typing import List, Dict, Any, Optional
import json
from datetime import datetime

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from utils.data.json_manager import JSONManager
from utils.file_manager import FileManager

from domains.personal_finance.nfce.database.nfce_database_manager import NFCeDatabaseManager
from domains.personal_finance.nfce.similarity.feature_extractor import FeatureExtractor

# Try to import advanced components
try:
    from domains.personal_finance.nfce.similarity.supervised_similarity_trainer import SupervisedSimilarityTrainer
    from domains.personal_finance.nfce.similarity.brazilian_product_normalizer import BrazilianProductNormalizer
    from domains.personal_finance.nfce.similarity.advanced_embedding_engine import AdvancedEmbeddingEngine
    ADVANCED_AVAILABLE = True
except ImportError as e:
    print(f"Advanced similarity components not available: {e}")
    ADVANCED_AVAILABLE = False
    SupervisedSimilarityTrainer = None
    BrazilianProductNormalizer = None
    AdvancedEmbeddingEngine = None


class SimilarityTrainingCommand(BaseCommand):
    """
    Interactive command for collecting training data for similarity algorithms
    
    This command provides different modes:
    1. Manual labeling of product pairs
    2. Uncertainty-based active learning
    3. Model training and evaluation
    4. Performance analysis and reporting
    """
    
    @staticmethod
    def get_name() -> str:
        return "advanced-similarity-training"
    
    @staticmethod
    def get_description() -> str:
        return "Interactive training data collection and model training for product similarity"
    
    @staticmethod
    def get_help() -> str:
        return """
Similarity Training Command

This command helps improve the product similarity detection through interactive training:

MODES:
  collect     - Collect training data by manually labeling product pairs
  active      - Use active learning to suggest best examples to label
  train       - Train machine learning models on collected data
  evaluate    - Evaluate model performance and analyze results
  export      - Export training data for external analysis

EXAMPLES:
  # Start interactive training data collection
  python src/main.py personal_finance nfce similarity-training --mode collect --samples 50

  # Use active learning to find uncertain examples
  python src/main.py personal_finance nfce similarity-training --mode active --suggestions 20

  # Train models on collected data
  python src/main.py personal_finance nfce similarity-training --mode train --model-type random_forest

  # Evaluate model performance
  python src/main.py personal_finance nfce similarity-training --mode evaluate --detailed

  # Export training data for analysis
  python src/main.py personal_finance nfce similarity-training --mode export --format csv --output training_data.csv

OPTIONS:
  --mode              Training mode (collect, active, train, evaluate, export)
  --samples           Number of samples to process (default: 20)
  --suggestions       Number of active learning suggestions (default: 10)
  --model-type        ML model type (random_forest, gradient_boosting, logistic_regression)
  --cnpj              Filter by establishment CNPJ
  --threshold         Similarity threshold for filtering (default: 0.4-0.9)
  --output            Output file path for exports
  --format            Export format (json, csv)
  --detailed          Show detailed analysis
  --auto-accept       Auto-accept obvious matches (for faster training)
        """
    
    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--mode",
            required=True,
            choices=["collect", "active", "train", "evaluate", "export", "demo"],
            help="Training mode to run"
        )
        
        parser.add_argument(
            "--samples",
            type=int,
            default=20,
            help="Number of samples to process"
        )
        
        parser.add_argument(
            "--suggestions",
            type=int,
            default=10,
            help="Number of active learning suggestions"
        )
        
        parser.add_argument(
            "--model-type",
            choices=["random_forest", "gradient_boosting", "logistic_regression"],
            default="random_forest",
            help="ML model type for training"
        )
        
        parser.add_argument(
            "--cnpj",
            help="Filter by establishment CNPJ"
        )
        
        parser.add_argument(
            "--threshold",
            type=float,
            default=0.6,
            help="Similarity threshold for filtering candidates"
        )
        
        parser.add_argument(
            "--output",
            help="Output file path for exports"
        )
        
        parser.add_argument(
            "--format",
            choices=["json", "csv"],
            default="json",
            help="Export format"
        )
        
        parser.add_argument(
            "--detailed",
            action="store_true",
            help="Show detailed analysis"
        )
        
        parser.add_argument(
            "--auto-accept",
            action="store_true",
            help="Auto-accept obvious matches (similarity > 0.9)"
        )
        
        parser.add_argument(
            "--skip-existing",
            action="store_true",
            help="Skip pairs that have already been labeled"
        )
    
    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        
        logger = LogManager.get_instance().get_logger("SimilarityTrainingCommand")
        
        try:
            # Check if advanced components are available
            if not ADVANCED_AVAILABLE:
                print("❌ Advanced similarity components are not available.")
                print("   Please install required dependencies:")
                print("   pip install sentence-transformers torch transformers scikit-learn")
                sys.exit(1)
            
            # Initialize components
            trainer = SupervisedSimilarityTrainer()
            db_manager = NFCeDatabaseManager()
            feature_extractor = FeatureExtractor()
            normalizer = BrazilianProductNormalizer()
            
            # Initialize advanced components if available
            try:
                embedding_engine = AdvancedEmbeddingEngine()
                logger.info("Advanced embedding engine initialized")
            except Exception as e:
                logger.warning(f"Advanced embedding engine not available: {e}")
                embedding_engine = None
            
            # Create command handler
            handler = SimilarityTrainingHandler(
                trainer=trainer,
                db_manager=db_manager,
                feature_extractor=feature_extractor,
                normalizer=normalizer,
                embedding_engine=embedding_engine,
                logger=logger
            )
            
            # Execute based on mode
            if args.mode == "collect":
                handler.collect_training_data(
                    samples=args.samples,
                    cnpj_filter=args.cnpj,
                    threshold=args.threshold,
                    auto_accept=args.auto_accept,
                    skip_existing=args.skip_existing
                )
            
            elif args.mode == "active":
                handler.active_learning_suggestions(
                    suggestions=args.suggestions,
                    cnpj_filter=args.cnpj,
                    threshold=args.threshold
                )
            
            elif args.mode == "train":
                handler.train_model(
                    model_type=args.model_type,
                    detailed=args.detailed
                )
            
            elif args.mode == "evaluate":
                handler.evaluate_model(
                    detailed=args.detailed
                )
            
            elif args.mode == "export":
                handler.export_training_data(
                    output_file=args.output,
                    format=args.format
                )
            
            elif args.mode == "demo":
                handler.demo_similarity_improvements()
            
            logger.info("Similarity training command completed successfully")
            
        except Exception as e:
            logger.error(f"Similarity training command failed: {e}")
            sys.exit(1)


class SimilarityTrainingHandler:
    """Handler for similarity training operations"""
    
    def __init__(self, trainer, db_manager, feature_extractor, normalizer, embedding_engine, logger):
        self.trainer = trainer
        self.db_manager = db_manager
        self.feature_extractor = feature_extractor
        self.normalizer = normalizer
        self.embedding_engine = embedding_engine
        self.logger = logger
        
        # Track labeled pairs to avoid duplicates
        self.labeled_pairs = set()
        self._load_labeled_pairs()
    
    def collect_training_data(self, 
                            samples: int = 20,
                            cnpj_filter: Optional[str] = None,
                            threshold: float = 0.6,
                            auto_accept: bool = False,
                            skip_existing: bool = True):
        """Interactive training data collection"""
        
        print("\n" + "="*70)
        print("🎯 SIMILARITY TRAINING DATA COLLECTION")
        print("="*70)
        print("This will help improve the product similarity detection algorithm.")
        print("You'll be shown product pairs and asked if they represent the same product.")
        print("="*70)
        
        # Get candidate product pairs
        candidates = self._get_candidate_pairs(
            max_pairs=samples * 3,  # Get more candidates for filtering
            cnpj_filter=cnpj_filter,
            threshold=threshold
        )
        
        if not candidates:
            print("❌ No candidate product pairs found.")
            return
        
        print(f"Found {len(candidates)} candidate pairs for labeling")
        
        labeled_count = 0
        skipped_count = 0
        
        for i, (product1_desc, product2_desc, features1, features2, similarity_score) in enumerate(candidates):
            if labeled_count >= samples:
                break
            
            # Skip if already labeled
            pair_key = self._get_pair_key(product1_desc, product2_desc)
            if skip_existing and pair_key in self.labeled_pairs:
                skipped_count += 1
                continue
            
            # Auto-accept obvious matches
            if auto_accept and similarity_score > 0.9:
                self.trainer.add_training_example(
                    product1=product1_desc,
                    product2=product2_desc,
                    product1_features=features1,
                    product2_features=features2,
                    user_says_similar=True,
                    confidence=0.9
                )
                self.labeled_pairs.add(pair_key)
                labeled_count += 1
                print(f"✅ Auto-accepted high similarity pair ({similarity_score:.3f})")
                continue
            
            # Interactive labeling
            print(f"\n📋 Pair {labeled_count + 1}/{samples} (Similarity: {similarity_score:.3f})")
            print("-" * 50)
            
            # Show normalized versions if available
            norm1 = self.normalizer.normalize(product1_desc)
            norm2 = self.normalizer.normalize(product2_desc)
            
            print(f"Product 1: {product1_desc}")
            if norm1.normalized != product1_desc:
                print(f"Normalized: {norm1.normalized}")
            if norm1.extracted_brand:
                print(f"Brand: {norm1.extracted_brand}")
            
            print(f"\nProduct 2: {product2_desc}")
            if norm2.normalized != product2_desc:
                print(f"Normalized: {norm2.normalized}")
            if norm2.extracted_brand:
                print(f"Brand: {norm2.extracted_brand}")
            
            print(f"\nSimilarity Details:")
            print(f"  Algorithm Score: {similarity_score:.3f}")
            print(f"  Brand Match: {norm1.extracted_brand == norm2.extracted_brand if norm1.extracted_brand and norm2.extracted_brand else 'N/A'}")
            print(f"  Categories: {', '.join(norm1.category_hints)} | {', '.join(norm2.category_hints)}")
            
            # Get user input
            while True:
                response = input("\nAre these the SAME product? (y/n/s/q): ").lower().strip()
                
                if response in ['y', 'yes']:
                    confidence = self._get_confidence_level()
                    self.trainer.add_training_example(
                        product1=product1_desc,
                        product2=product2_desc,
                        product1_features=features1,
                        product2_features=features2,
                        user_says_similar=True,
                        confidence=confidence
                    )
                    self.labeled_pairs.add(pair_key)
                    labeled_count += 1
                    print("✅ Marked as SIMILAR")
                    break
                
                elif response in ['n', 'no']:
                    confidence = self._get_confidence_level()
                    self.trainer.add_training_example(
                        product1=product1_desc,
                        product2=product2_desc,
                        product1_features=features1,
                        product2_features=features2,
                        user_says_similar=False,
                        confidence=confidence
                    )
                    self.labeled_pairs.add(pair_key)
                    labeled_count += 1
                    print("❌ Marked as DIFFERENT")
                    break
                
                elif response in ['s', 'skip']:
                    print("⏭️  Skipped")
                    skipped_count += 1
                    break
                
                elif response in ['q', 'quit']:
                    print("🛑 Stopping training data collection")
                    self._save_labeled_pairs()
                    return
                
                else:
                    print("Please enter 'y' (yes), 'n' (no), 's' (skip), or 'q' (quit)")
        
        self._save_labeled_pairs()
        
        print(f"\n✅ Training data collection completed!")
        print(f"   Labeled: {labeled_count}")
        print(f"   Skipped: {skipped_count}")
        print(f"   Total training examples: {len(self.trainer.training_examples)}")
        
        # Show training statistics
        stats = self.trainer.get_training_statistics()
        print(f"   Positive examples: {stats['positive_examples']}")
        print(f"   Negative examples: {stats['negative_examples']}")
    
    def active_learning_suggestions(self,
                                  suggestions: int = 10,
                                  cnpj_filter: Optional[str] = None,
                                  threshold: float = 0.6):
        """Use active learning to suggest best examples to label"""
        
        print("\n" + "="*70)
        print("🧠 ACTIVE LEARNING SUGGESTIONS")
        print("="*70)
        
        # Get candidate pairs
        candidates = self._get_candidate_pairs(
            max_pairs=suggestions * 10,  # Get more for uncertainty sampling
            cnpj_filter=cnpj_filter,
            threshold=threshold
        )
        
        if not candidates:
            print("❌ No candidate pairs found for active learning")
            return
        
        # Convert to format expected by trainer
        candidate_pairs = [(features1, features2) for _, _, features1, features2, _ in candidates]
        
        # Get uncertainty-based suggestions
        uncertain_pairs = self.trainer.suggest_training_examples(
            candidate_pairs=candidate_pairs,
            n_suggestions=suggestions
        )
        
        print(f"Found {len(uncertain_pairs)} uncertain examples to label:")
        print("\nMost uncertain product pairs (highest learning value):")
        print("-" * 60)
        
        for i, (features1, features2, uncertainty) in enumerate(uncertain_pairs, 1):
            print(f"{i:2d}. Uncertainty: {uncertainty:.3f}")
            print(f"    Product 1: {features1.original_description}")
            print(f"    Product 2: {features2.original_description}")
            print()
        
        # Ask if user wants to label these
        response = input("Label these uncertain examples now? (y/n): ").lower().strip()
        
        if response in ['y', 'yes']:
            # Convert back to full format for labeling
            selected_candidates = []
            for features1, features2, uncertainty in uncertain_pairs:
                # Find original candidate
                for candidate in candidates:
                    if (candidate[2].original_description == features1.original_description and
                        candidate[3].original_description == features2.original_description):
                        selected_candidates.append(candidate)
                        break
            
            # Run interactive labeling on selected candidates
            self._label_candidates_interactively(selected_candidates)
    
    def train_model(self, model_type: str = "random_forest", detailed: bool = False):
        """Train ML model on collected data"""
        
        print("\n" + "="*70)
        print("🤖 MODEL TRAINING")
        print("="*70)
        
        # Get training statistics
        stats = self.trainer.get_training_statistics()
        
        if stats.get('total_examples', 0) < 10:
            print("❌ Not enough training examples. Need at least 10 examples.")
            print(f"   Current examples: {stats.get('total_examples', 0)}")
            print("   Use 'collect' mode to gather more training data.")
            return
        
        print(f"Training {model_type} model...")
        print(f"  Training examples: {stats['total_examples']}")
        print(f"  Positive examples: {stats['positive_examples']}")
        print(f"  Negative examples: {stats['negative_examples']}")
        print(f"  Balance ratio: {stats['positive_ratio']:.2f}")
        
        try:
            # Train model
            performance = self.trainer.train_model(
                model_type=model_type,
                validate_model=detailed
            )
            
            # Display results
            print(f"\n✅ Model training completed!")
            print(f"   Accuracy:  {performance.accuracy:.3f}")
            print(f"   Precision: {performance.precision:.3f}")
            print(f"   Recall:    {performance.recall:.3f}")
            print(f"   F1 Score:  {performance.f1_score:.3f}")
            print(f"   AUC Score: {performance.auc_score:.3f}")
            
            if detailed:
                print(f"\n📊 Confusion Matrix:")
                cm = performance.confusion_matrix
                print(f"                 Predicted")
                print(f"              Not Same  Same")
                print(f"Actual Not Same  {cm[0][0]:4d}   {cm[0][1]:4d}")
                print(f"       Same      {cm[1][0]:4d}   {cm[1][1]:4d}")
            
            # Get optimal threshold
            optimal_threshold = self.trainer.get_optimal_threshold()
            print(f"\n🎯 Optimal Similarity Threshold: {optimal_threshold:.3f}")
            
        except Exception as e:
            print(f"❌ Model training failed: {e}")
            self.logger.error(f"Model training error: {e}")
    
    def evaluate_model(self, detailed: bool = False):
        """Evaluate model performance"""
        
        print("\n" + "="*70)
        print("📈 MODEL EVALUATION")
        print("="*70)
        
        # Get training statistics
        stats = self.trainer.get_training_statistics()
        
        print(f"Training Data Summary:")
        print(f"  Total examples: {stats['total_examples']}")
        print(f"  Positive examples: {stats['positive_examples']}")
        print(f"  Negative examples: {stats['negative_examples']}")
        print(f"  Positive ratio: {stats['positive_ratio']:.2%}")
        print(f"  Avg positive confidence: {stats['average_positive_confidence']:.3f}")
        print(f"  Avg negative confidence: {stats['average_negative_confidence']:.3f}")
        print(f"  Models trained: {stats['models_trained']}")
        print(f"  Current model available: {stats['current_model_available']}")
        
        if stats['models_trained'] > 0:
            print(f"\n📊 Performance History:")
            for i, perf in enumerate(self.trainer.performance_history[-3:], 1):  # Last 3 models
                print(f"  Model {i}: F1={perf.f1_score:.3f}, Acc={perf.accuracy:.3f}, AUC={perf.auc_score:.3f}")
        
        if detailed and self.trainer.current_model is not None:
            print(f"\n🔍 Detailed Analysis:")
            
            # Test on some examples
            test_pairs = self._get_candidate_pairs(max_pairs=10)
            if test_pairs:
                print(f"Sample predictions on new data:")
                for i, (prod1, prod2, feat1, feat2, orig_score) in enumerate(test_pairs[:5], 1):
                    is_similar, confidence = self.trainer.predict_similarity(feat1, feat2)
                    print(f"  {i}. {prod1[:40]}...")
                    print(f"     {prod2[:40]}...")
                    print(f"     Original: {orig_score:.3f} | ML: {confidence:.3f} ({'Similar' if is_similar else 'Different'})")
                    print()
    
    def export_training_data(self, output_file: Optional[str] = None, format: str = "json"):
        """Export training data"""
        
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"output/similarity_training_data_{timestamp}.{format}"
        
        # Ensure output directory exists
        FileManager.create_folder("output")
        
        print(f"\n📤 Exporting training data to {output_file}")
        
        try:
            self.trainer.export_training_data(output_file, format)
            print(f"✅ Training data exported successfully")
            
            # Show export summary
            stats = self.trainer.get_training_statistics()
            print(f"   Exported {stats['total_examples']} training examples")
            print(f"   Format: {format.upper()}")
            
        except Exception as e:
            print(f"❌ Export failed: {e}")
            self.logger.error(f"Export error: {e}")
    
    def demo_similarity_improvements(self):
        """Demo the improvements in similarity detection"""
        
        print("\n" + "="*70)
        print("🚀 SIMILARITY IMPROVEMENTS DEMO")
        print("="*70)
        
        # Sample product pairs for demonstration
        demo_pairs = [
            ("COCA COLA LATA 350ML", "COCA-COLA REFRIGERANTE LATA 350ML"),
            ("ACUCAR CRISTAL UNIAO 1KG", "AÇÚCAR UNIÃO CRISTAL 1000G"),
            ("DETERGENTE YPE CLEAR 500ML", "DETERGENTE YPÊ NEUTRO 500ML"),
            ("LEITE NESTLE MOLICO DESNATADO", "LEITE MOLICO NESTLÉ INTEGRAL"),
            ("ARROZ CAMIL TIPO 1 5KG", "ARROZ CAMIL AGULHINHA TIPO1 5000G")
        ]
        
        print("Comparing different similarity approaches:\n")
        
        for i, (prod1, prod2) in enumerate(demo_pairs, 1):
            print(f"{i}. Product Pair:")
            print(f"   A: {prod1}")
            print(f"   B: {prod2}")
            
            # Extract features
            features1 = self.feature_extractor.extract_features(prod1)
            features2 = self.feature_extractor.extract_features(prod2)
            
            # Original similarity
            similarity_result = self.trainer.similarity_calculator.calculate_similarity(features1, features2)
            orig_score = similarity_result.final_score
            
            # ML-based similarity (if model available)
            if self.trainer.current_model is not None:
                ml_similar, ml_confidence = self.trainer.predict_similarity(features1, features2)
            else:
                ml_similar, ml_confidence = None, None
            
            # Advanced normalization
            norm1 = self.normalizer.normalize(prod1)
            norm2 = self.normalizer.normalize(prod2)
            
            # Embedding similarity (if available)
            if self.embedding_engine:
                emb_result1 = self.embedding_engine.get_embedding(prod1)
                emb_result2 = self.embedding_engine.get_embedding(prod2)
                emb_similarity = float(np.dot(emb_result1.ensemble_embedding, emb_result2.ensemble_embedding))
            else:
                emb_similarity = None
            
            print(f"   Results:")
            print(f"     Original Algorithm: {orig_score:.3f}")
            if ml_confidence is not None:
                print(f"     ML Model: {ml_confidence:.3f} ({'Similar' if ml_similar else 'Different'})")
            if emb_similarity is not None:
                print(f"     Advanced Embeddings: {emb_similarity:.3f}")
            
            print(f"   Normalized:")
            print(f"     A: {norm1.normalized} (Brand: {norm1.extracted_brand or 'N/A'})")
            print(f"     B: {norm2.normalized} (Brand: {norm2.extracted_brand or 'N/A'})")
            print()
        
        print("💡 Key Improvements:")
        print("   • Advanced Brazilian product normalization")
        print("   • Brand and category extraction")
        print("   • Machine learning from user feedback")
        print("   • Multiple embedding models")
        print("   • Uncertainty-based active learning")
    
    def _get_candidate_pairs(self, 
                           max_pairs: int = 100,
                           cnpj_filter: Optional[str] = None,
                           threshold: float = 0.6) -> List:
        """Get candidate product pairs for labeling"""
        
        try:
            conn = self.db_manager.db_manager.get_connection("nfce_db")
            
            # Query to get product pairs with some similarity
            query = """
            SELECT DISTINCT
                p1.description as desc1,
                p2.description as desc2,
                e1.cnpj as cnpj1,
                e2.cnpj as cnpj2
            FROM products p1
            JOIN establishments e1 ON p1.establishment_id = e1.id
            JOIN products p2 ON p2.id > p1.id
            JOIN establishments e2 ON p2.establishment_id = e2.id
            WHERE p1.description IS NOT NULL 
            AND p2.description IS NOT NULL
            AND p1.description != p2.description
            AND LENGTH(p1.description) > 10
            AND LENGTH(p2.description) > 10
            """
            
            params = []
            if cnpj_filter:
                query += " AND (e1.cnpj = ? OR e2.cnpj = ?)"
                params.extend([cnpj_filter, cnpj_filter])
            
            query += " ORDER BY RANDOM() LIMIT ?"
            params.append(max_pairs * 2)  # Get more for filtering
            
            results = conn.execute(query, params).fetchall()
            
            candidates = []
            
            for row in results:
                desc1, desc2, cnpj1, cnpj2 = row
                
                # Extract features
                features1 = self.feature_extractor.extract_features(desc1)
                features2 = self.feature_extractor.extract_features(desc2)
                
                # Calculate similarity
                similarity_result = self.trainer.similarity_calculator.calculate_similarity(
                    features1, features2
                )
                
                # Filter by threshold
                if threshold <= similarity_result.final_score <= 0.95:  # Avoid obvious matches
                    candidates.append((
                        desc1, desc2, features1, features2, similarity_result.final_score
                    ))
                
                if len(candidates) >= max_pairs:
                    break
            
            # Sort by similarity (middle scores first - most informative)
            candidates.sort(key=lambda x: abs(x[4] - 0.7))
            
            return candidates
            
        except Exception as e:
            self.logger.error(f"Error getting candidate pairs: {e}")
            return []
    
    def _get_confidence_level(self) -> float:
        """Get user confidence level"""
        while True:
            try:
                confidence_str = input("Confidence level (1-5, or just press Enter for 5): ").strip()
                if not confidence_str:
                    return 1.0  # Default high confidence
                
                confidence_int = int(confidence_str)
                if 1 <= confidence_int <= 5:
                    return confidence_int / 5.0  # Convert to 0-1 scale
                else:
                    print("Please enter a number between 1 and 5")
            except ValueError:
                print("Please enter a valid number")
    
    def _get_pair_key(self, desc1: str, desc2: str) -> str:
        """Generate consistent key for product pair"""
        # Sort to ensure consistent key regardless of order
        sorted_descs = sorted([desc1, desc2])
        return f"{hash(sorted_descs[0])}:{hash(sorted_descs[1])}"
    
    def _load_labeled_pairs(self):
        """Load previously labeled pairs to avoid duplicates"""
        try:
            labeled_file = "data/similarity_training/labeled_pairs.json"
            if os.path.exists(labeled_file):
                data = JSONManager.read_json(labeled_file)
                self.labeled_pairs = set(data.get('pairs', []))
                self.logger.info(f"Loaded {len(self.labeled_pairs)} previously labeled pairs")
        except Exception as e:
            self.logger.error(f"Error loading labeled pairs: {e}")
            self.labeled_pairs = set()
    
    def _save_labeled_pairs(self):
        """Save labeled pairs to avoid duplicates"""
        try:
            FileManager.create_folder("data/similarity_training")
            labeled_file = "data/similarity_training/labeled_pairs.json"
            data = {'pairs': list(self.labeled_pairs)}
            JSONManager.write_json(data, labeled_file)
            self.logger.info(f"Saved {len(self.labeled_pairs)} labeled pairs")
        except Exception as e:
            self.logger.error(f"Error saving labeled pairs: {e}")
    
    def _label_candidates_interactively(self, candidates: List):
        """Label candidates interactively"""
        print(f"\n🏷️  Labeling {len(candidates)} selected candidates...")
        
        for i, (prod1, prod2, feat1, feat2, sim_score) in enumerate(candidates, 1):
            print(f"\n📋 Candidate {i}/{len(candidates)} (Similarity: {sim_score:.3f})")
            print(f"Product 1: {prod1}")
            print(f"Product 2: {prod2}")
            
            while True:
                response = input("Same product? (y/n/s/q): ").lower().strip()
                
                if response in ['y', 'yes']:
                    confidence = self._get_confidence_level()
                    self.trainer.add_training_example(
                        product1=prod1, product2=prod2,
                        product1_features=feat1, product2_features=feat2,
                        user_says_similar=True, confidence=confidence
                    )
                    print("✅ Marked as SIMILAR")
                    break
                elif response in ['n', 'no']:
                    confidence = self._get_confidence_level()
                    self.trainer.add_training_example(
                        product1=prod1, product2=prod2,
                        product1_features=feat1, product2_features=feat2,
                        user_says_similar=False, confidence=confidence
                    )
                    print("❌ Marked as DIFFERENT")
                    break
                elif response in ['s', 'skip']:
                    print("⏭️  Skipped")
                    break
                elif response in ['q', 'quit']:
                    print("🛑 Stopping labeling")
                    return
                else:
                    print("Please enter 'y' (yes), 'n' (no), 's' (skip), or 'q' (quit)")