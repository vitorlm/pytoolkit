#!/usr/bin/env python3
"""
Similarity Comparison Command - Compare different similarity algorithms and configurations
"""

import sys
from argparse import ArgumentParser, Namespace
from typing import List, Dict, Any, Optional, Tuple
import time
import json
from datetime import datetime
import numpy as np

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from utils.data.json_manager import JSONManager
from utils.file_manager import FileManager

from domains.personal_finance.nfce.database.nfce_database_manager import NFCeDatabaseManager
from domains.personal_finance.nfce.similarity.feature_extractor import FeatureExtractor
from domains.personal_finance.nfce.similarity.similarity_calculator import SimilarityCalculator

# Try to import advanced components
try:
    from domains.personal_finance.nfce.similarity.brazilian_product_normalizer import BrazilianProductNormalizer
    from domains.personal_finance.nfce.similarity.supervised_similarity_trainer import SupervisedSimilarityTrainer
    from domains.personal_finance.nfce.similarity.advanced_embedding_engine import AdvancedEmbeddingEngine, EmbeddingConfig
    ADVANCED_AVAILABLE = True
except ImportError:
    ADVANCED_AVAILABLE = False
    BrazilianProductNormalizer = None
    SupervisedSimilarityTrainer = None
    AdvancedEmbeddingEngine = None
    EmbeddingConfig = None


class SimilarityComparisonCommand(BaseCommand):
    """
    Compare different similarity algorithms and configurations
    
    This command allows you to:
    1. Compare traditional vs advanced similarity algorithms
    2. Benchmark performance across different model configurations
    3. Analyze accuracy improvements with different approaches
    4. Generate detailed comparison reports
    """
    
    @staticmethod
    def get_name() -> str:
        return "advanced-similarity-comparison"
    
    @staticmethod
    def get_description() -> str:
        return "Compare different similarity algorithms and analyze performance improvements"
    
    @staticmethod
    def get_help() -> str:
        return """
Similarity Comparison Command

Compare different similarity detection approaches and analyze performance:

MODES:
  algorithms  - Compare different similarity algorithms
  models      - Compare different embedding models
  thresholds  - Find optimal similarity thresholds
  benchmark   - Benchmark performance and speed
  analysis    - Detailed analysis of algorithm behavior

EXAMPLES:
  # Compare basic vs advanced algorithms
  python src/main.py personal_finance nfce similarity-comparison --mode algorithms --samples 100

  # Compare different embedding models
  python src/main.py personal_finance nfce similarity-comparison --mode models --detailed

  # Find optimal thresholds
  python src/main.py personal_finance nfce similarity-comparison --mode thresholds --range 0.5-0.9

  # Performance benchmark
  python src/main.py personal_finance nfce similarity-comparison --mode benchmark --samples 500

  # Detailed analysis with reports
  python src/main.py personal_finance nfce similarity-comparison --mode analysis --output comparison_report.json

OPTIONS:
  --mode          Comparison mode (algorithms, models, thresholds, benchmark, analysis)
  --samples       Number of product pairs to test (default: 100)
  --cnpj          Filter by establishment CNPJ
  --range         Threshold range for testing (e.g., "0.5-0.9")
  --output        Output file for detailed reports
  --detailed      Show detailed analysis
  --export-pairs  Export test pairs for manual validation
        """
    
    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--mode",
            required=True,
            choices=["algorithms", "models", "thresholds", "benchmark", "analysis"],
            help="Comparison mode"
        )
        
        parser.add_argument(
            "--samples",
            type=int,
            default=100,
            help="Number of product pairs to test"
        )
        
        parser.add_argument(
            "--cnpj",
            help="Filter by establishment CNPJ"
        )
        
        parser.add_argument(
            "--range",
            default="0.5-0.9",
            help="Threshold range for testing (format: min-max)"
        )
        
        parser.add_argument(
            "--output",
            help="Output file for detailed reports"
        )
        
        parser.add_argument(
            "--detailed",
            action="store_true",
            help="Show detailed analysis"
        )
        
        parser.add_argument(
            "--export-pairs",
            action="store_true",
            help="Export test pairs for manual validation"
        )
    
    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        
        logger = LogManager.get_instance().get_logger("SimilarityComparisonCommand")
        
        try:
            # Initialize components
            handler = SimilarityComparisonHandler(logger)
            
            # Execute based on mode
            if args.mode == "algorithms":
                handler.compare_algorithms(
                    samples=args.samples,
                    cnpj_filter=args.cnpj,
                    detailed=args.detailed,
                    export_pairs=args.export_pairs
                )
            
            elif args.mode == "models":
                handler.compare_embedding_models(
                    samples=args.samples,
                    cnpj_filter=args.cnpj,
                    detailed=args.detailed
                )
            
            elif args.mode == "thresholds":
                threshold_range = [float(x) for x in args.range.split('-')]
                handler.optimize_thresholds(
                    samples=args.samples,
                    cnpj_filter=args.cnpj,
                    threshold_range=threshold_range
                )
            
            elif args.mode == "benchmark":
                handler.performance_benchmark(
                    samples=args.samples,
                    cnpj_filter=args.cnpj
                )
            
            elif args.mode == "analysis":
                handler.detailed_analysis(
                    samples=args.samples,
                    cnpj_filter=args.cnpj,
                    output_file=args.output
                )
            
            logger.info("Similarity comparison completed successfully")
            
        except Exception as e:
            logger.error(f"Similarity comparison failed: {e}")
            sys.exit(1)


class SimilarityComparisonHandler:
    """Handler for similarity comparison operations"""
    
    def __init__(self, logger):
        self.logger = logger
        
        # Initialize basic components
        self.db_manager = NFCeDatabaseManager()
        self.feature_extractor = FeatureExtractor()
        self.similarity_calculator = SimilarityCalculator()
        
        # Initialize advanced components if available
        self.normalizer = None
        self.advanced_embedding = None
        self.trainer = None
        
        if ADVANCED_AVAILABLE:
            try:
                self.normalizer = BrazilianProductNormalizer()
                self.advanced_embedding = AdvancedEmbeddingEngine()
                self.trainer = SupervisedSimilarityTrainer()
                self.logger.info("Advanced similarity components available")
            except Exception as e:
                self.logger.warning(f"Some advanced components not available: {e}")
        else:
            self.logger.warning("Advanced similarity components not available - install dependencies for full functionality")
    
    def compare_algorithms(self, 
                          samples: int = 100,
                          cnpj_filter: Optional[str] = None,
                          detailed: bool = False,
                          export_pairs: bool = False):
        """Compare different similarity algorithms"""
        
        print("\n" + "="*70)
        print("🔬 SIMILARITY ALGORITHMS COMPARISON")
        print("="*70)
        
        # Get test pairs
        test_pairs = self._get_test_pairs(samples, cnpj_filter)
        if not test_pairs:
            print("❌ No test pairs found")
            return
        
        print(f"Testing {len(test_pairs)} product pairs...")
        
        # Define algorithms to compare
        algorithms = {
            'Basic Combined': self._basic_similarity,
            'Advanced Normalization': self._normalized_similarity,
            'With Embeddings': self._embedding_similarity,
            'ML Enhanced': self._ml_similarity
        }
        
        results = {}
        
        for algo_name, algo_func in algorithms.items():
            print(f"\n📊 Testing {algo_name}...")
            
            start_time = time.time()
            algo_results = []
            
            for product1, product2 in test_pairs:
                try:
                    score = algo_func(product1, product2)
                    algo_results.append(score)
                except Exception as e:
                    self.logger.warning(f"Error in {algo_name} for pair: {e}")
                    algo_results.append(0.0)
            
            processing_time = time.time() - start_time
            
            results[algo_name] = {
                'scores': algo_results,
                'avg_score': np.mean(algo_results),
                'std_score': np.std(algo_results),
                'processing_time': processing_time,
                'pairs_per_second': len(test_pairs) / processing_time if processing_time > 0 else 0
            }
            
            print(f"   Average Score: {results[algo_name]['avg_score']:.3f}")
            print(f"   Std Deviation: {results[algo_name]['std_score']:.3f}")
            print(f"   Processing Time: {processing_time:.2f}s")
            print(f"   Speed: {results[algo_name]['pairs_per_second']:.1f} pairs/sec")
        
        # Show comparison summary
        print(f"\n📈 COMPARISON SUMMARY")
        print("-" * 50)
        
        for algo_name, result in results.items():
            print(f"{algo_name:20s}: Avg={result['avg_score']:.3f}, Speed={result['pairs_per_second']:6.1f} p/s")
        
        if detailed:
            self._show_detailed_algorithm_analysis(results, test_pairs)
        
        if export_pairs:
            self._export_test_pairs(test_pairs, results)
    
    def compare_embedding_models(self,
                               samples: int = 100,
                               cnpj_filter: Optional[str] = None,
                               detailed: bool = False):
        """Compare different embedding models"""
        
        print("\n" + "="*70)
        print("🧠 EMBEDDING MODELS COMPARISON")
        print("="*70)
        
        if not ADVANCED_AVAILABLE:
            print("❌ Advanced embedding engine not available")
            return
        
        # Get test pairs
        test_pairs = self._get_test_pairs(samples, cnpj_filter)
        if not test_pairs:
            print("❌ No test pairs found")
            return
        
        print(f"Testing {len(test_pairs)} product pairs with different embedding models...")
        
        # Define embedding configurations to test
        model_configs = {
            'Portuguese BERTimbau': EmbeddingConfig(
                primary_model="neuralmind/bert-base-portuguese-cased",
                secondary_model=None,
                ensemble_weights={'portuguese': 1.0, 'multilingual': 0.0, 'fallback': 0.0}
            ),
            'Multilingual E5': EmbeddingConfig(
                primary_model="intfloat/multilingual-e5-large",
                secondary_model=None,
                ensemble_weights={'portuguese': 1.0, 'multilingual': 0.0, 'fallback': 0.0}
            ),
            'Ensemble (PT + E5)': EmbeddingConfig(
                primary_model="neuralmind/bert-base-portuguese-cased",
                secondary_model="intfloat/multilingual-e5-large",
                ensemble_weights={'portuguese': 0.6, 'multilingual': 0.4, 'fallback': 0.0}
            ),
            'Original SentenceT': EmbeddingConfig(
                primary_model="distiluse-base-multilingual-cased-v2",
                secondary_model=None,
                ensemble_weights={'portuguese': 1.0, 'multilingual': 0.0, 'fallback': 0.0}
            )
        }
        
        results = {}
        
        for model_name, config in model_configs.items():
            print(f"\n🤖 Testing {model_name}...")
            
            try:
                # Initialize embedding engine with specific config
                embedding_engine = AdvancedEmbeddingEngine(config)
                
                start_time = time.time()
                scores = []
                
                for product1, product2 in test_pairs:
                    try:
                        emb_result1 = embedding_engine.get_embedding(product1)
                        emb_result2 = embedding_engine.get_embedding(product2)
                        
                        # Calculate cosine similarity
                        similarity = float(np.dot(
                            emb_result1.ensemble_embedding,
                            emb_result2.ensemble_embedding
                        ))
                        scores.append(similarity)
                        
                    except Exception as e:
                        self.logger.warning(f"Error processing pair with {model_name}: {e}")
                        scores.append(0.0)
                
                processing_time = time.time() - start_time
                
                results[model_name] = {
                    'scores': scores,
                    'avg_score': np.mean(scores),
                    'std_score': np.std(scores),
                    'processing_time': processing_time,
                    'pairs_per_second': len(test_pairs) / processing_time if processing_time > 0 else 0,
                    'config': config
                }
                
                print(f"   Average Similarity: {results[model_name]['avg_score']:.3f}")
                print(f"   Std Deviation: {results[model_name]['std_score']:.3f}")
                print(f"   Processing Time: {processing_time:.2f}s")
                print(f"   Speed: {results[model_name]['pairs_per_second']:.1f} pairs/sec")
                
            except Exception as e:
                print(f"   ❌ Failed to test {model_name}: {e}")
                self.logger.error(f"Error testing {model_name}: {e}")
        
        # Show comparison summary
        print(f"\n📈 EMBEDDING MODELS COMPARISON")
        print("-" * 60)
        
        for model_name, result in results.items():
            print(f"{model_name:20s}: Avg={result['avg_score']:.3f}, Speed={result['pairs_per_second']:6.1f} p/s")
        
        if detailed:
            self._show_detailed_embedding_analysis(results, test_pairs)
    
    def optimize_thresholds(self,
                          samples: int = 100,
                          cnpj_filter: Optional[str] = None,
                          threshold_range: List[float] = [0.5, 0.9]):
        """Find optimal similarity thresholds"""
        
        print("\n" + "="*70)
        print("🎯 THRESHOLD OPTIMIZATION")
        print("="*70)
        
        # Get test pairs with known labels (if available)
        if self.trainer and len(self.trainer.training_examples) > 10:
            print("Using training data for threshold optimization...")
            self._optimize_with_training_data(threshold_range)
        else:
            print("No training data available. Using heuristic optimization...")
            self._optimize_with_heuristics(samples, cnpj_filter, threshold_range)
    
    def performance_benchmark(self,
                            samples: int = 500,
                            cnpj_filter: Optional[str] = None):
        """Benchmark performance of different approaches"""
        
        print("\n" + "="*70)
        print("⚡ PERFORMANCE BENCHMARK")
        print("="*70)
        
        # Get test pairs
        test_pairs = self._get_test_pairs(samples, cnpj_filter)
        if not test_pairs:
            print("❌ No test pairs found")
            return
        
        print(f"Benchmarking with {len(test_pairs)} product pairs...")
        
        # Define benchmarks
        benchmarks = {
            'String Similarity Only': self._string_similarity_only,
            'Traditional Approach': self._basic_similarity,
            'With Normalization': self._normalized_similarity,
            'With Embeddings': self._embedding_similarity,
            'Full Pipeline': self._full_pipeline_similarity
        }
        
        results = {}
        
        for bench_name, bench_func in benchmarks.items():
            print(f"\n⏱️  Benchmarking {bench_name}...")
            
            # Warm up
            for i in range(min(5, len(test_pairs))):
                bench_func(test_pairs[i][0], test_pairs[i][1])
            
            # Actual benchmark
            start_time = time.time()
            
            for product1, product2 in test_pairs:
                try:
                    bench_func(product1, product2)
                except Exception as e:
                    self.logger.warning(f"Error in {bench_name}: {e}")
            
            total_time = time.time() - start_time
            
            results[bench_name] = {
                'total_time': total_time,
                'pairs_per_second': len(test_pairs) / total_time if total_time > 0 else 0,
                'ms_per_pair': (total_time * 1000) / len(test_pairs) if len(test_pairs) > 0 else 0
            }
            
            print(f"   Total Time: {total_time:.2f}s")
            print(f"   Speed: {results[bench_name]['pairs_per_second']:.1f} pairs/sec")
            print(f"   Time per pair: {results[bench_name]['ms_per_pair']:.2f}ms")
        
        # Show benchmark summary
        print(f"\n🏁 BENCHMARK SUMMARY")
        print("-" * 60)
        print(f"{'Method':<25} {'Speed (p/s)':<12} {'Time/pair (ms)':<15}")
        print("-" * 60)
        
        for bench_name, result in results.items():
            print(f"{bench_name:<25} {result['pairs_per_second']:>8.1f}    {result['ms_per_pair']:>10.2f}")
    
    def detailed_analysis(self,
                        samples: int = 100,
                        cnpj_filter: Optional[str] = None,
                        output_file: Optional[str] = None):
        """Perform detailed analysis of similarity behavior"""
        
        print("\n" + "="*70)
        print("🔍 DETAILED SIMILARITY ANALYSIS")
        print("="*70)
        
        # Get test pairs
        test_pairs = self._get_test_pairs(samples, cnpj_filter)
        if not test_pairs:
            print("❌ No test pairs found")
            return
        
        print(f"Performing detailed analysis on {len(test_pairs)} product pairs...")
        
        analysis_results = {
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'samples': len(test_pairs),
                'cnpj_filter': cnpj_filter
            },
            'similarity_distribution': {},
            'algorithm_comparison': {},
            'normalization_impact': {},
            'embedding_analysis': {},
            'performance_metrics': {}
        }
        
        # Analyze similarity score distribution
        print("\n📊 Analyzing similarity score distribution...")
        analysis_results['similarity_distribution'] = self._analyze_similarity_distribution(test_pairs)
        
        # Compare algorithm performance
        print("🔬 Comparing algorithm performance...")
        analysis_results['algorithm_comparison'] = self._compare_algorithms_detailed(test_pairs)
        
        # Analyze normalization impact
        print("✨ Analyzing normalization impact...")
        analysis_results['normalization_impact'] = self._analyze_normalization_impact(test_pairs)
        
        # Embedding analysis (if available)
        if ADVANCED_AVAILABLE:
            print("🧠 Analyzing embedding performance...")
            analysis_results['embedding_analysis'] = self._analyze_embedding_performance(test_pairs)
        
        # Performance metrics
        print("⚡ Collecting performance metrics...")
        analysis_results['performance_metrics'] = self._collect_performance_metrics(test_pairs)
        
        # Show summary
        self._show_analysis_summary(analysis_results)
        
        # Save detailed results
        if output_file:
            FileManager.create_folder("output")
            JSONManager.write_json(analysis_results, output_file)
            print(f"\n💾 Detailed analysis saved to {output_file}")
    
    def _get_test_pairs(self, 
                       max_pairs: int,
                       cnpj_filter: Optional[str] = None) -> List[Tuple[str, str]]:
        """Get test product pairs from database"""
        
        try:
            conn = self.db_manager.db_manager.get_connection("nfce_db")
            
            query = """
            SELECT DISTINCT
                p1.description,
                p2.description
            FROM products p1
            JOIN establishments e1 ON p1.establishment_id = e1.id
            JOIN products p2 ON p2.id > p1.id
            JOIN establishments e2 ON p2.establishment_id = e2.id
            WHERE p1.description IS NOT NULL 
            AND p2.description IS NOT NULL
            AND p1.description != p2.description
            AND LENGTH(p1.description) > 5
            AND LENGTH(p2.description) > 5
            """
            
            params = []
            if cnpj_filter:
                query += " AND (e1.cnpj = ? OR e2.cnpj = ?)"
                params.extend([cnpj_filter, cnpj_filter])
            
            query += " ORDER BY RANDOM() LIMIT ?"
            params.append(max_pairs)
            
            results = conn.execute(query, params).fetchall()
            return [(row[0], row[1]) for row in results]
            
        except Exception as e:
            self.logger.error(f"Error getting test pairs: {e}")
            return []
    
    def _basic_similarity(self, product1: str, product2: str) -> float:
        """Basic similarity calculation"""
        features1 = self.feature_extractor.extract_features(product1)
        features2 = self.feature_extractor.extract_features(product2)
        result = self.similarity_calculator.calculate_similarity(features1, features2)
        return result.final_score
    
    def _normalized_similarity(self, product1: str, product2: str) -> float:
        """Similarity with advanced normalization"""
        norm1 = self.normalizer.normalize(product1)
        norm2 = self.normalizer.normalize(product2)
        
        features1 = self.feature_extractor.extract_features(norm1.normalized)
        features2 = self.feature_extractor.extract_features(norm2.normalized)
        
        result = self.similarity_calculator.calculate_similarity(features1, features2)
        
        # Bonus for brand match
        if norm1.extracted_brand and norm2.extracted_brand:
            if norm1.extracted_brand == norm2.extracted_brand:
                result.final_score = min(1.0, result.final_score + 0.1)
        
        return result.final_score
    
    def _embedding_similarity(self, product1: str, product2: str) -> float:
        """Similarity using embeddings"""
        if not self.advanced_embedding:
            return self._basic_similarity(product1, product2)
        
        try:
            emb1 = self.advanced_embedding.get_embedding(product1)
            emb2 = self.advanced_embedding.get_embedding(product2)
            
            similarity = float(np.dot(emb1.ensemble_embedding, emb2.ensemble_embedding))
            return max(0.0, min(1.0, similarity))
        except Exception:
            return self._basic_similarity(product1, product2)
    
    def _ml_similarity(self, product1: str, product2: str) -> float:
        """Similarity using ML model"""
        if not self.trainer or not self.trainer.current_model:
            return self._basic_similarity(product1, product2)
        
        try:
            features1 = self.feature_extractor.extract_features(product1)
            features2 = self.feature_extractor.extract_features(product2)
            
            is_similar, confidence = self.trainer.predict_similarity(features1, features2)
            return confidence
        except Exception:
            return self._basic_similarity(product1, product2)
    
    def _string_similarity_only(self, product1: str, product2: str) -> float:
        """Simple string similarity"""
        from fuzzywuzzy import fuzz
        return fuzz.token_sort_ratio(product1, product2) / 100.0
    
    def _full_pipeline_similarity(self, product1: str, product2: str) -> float:
        """Full pipeline with all improvements"""
        # Combine normalized, embedding, and ML approaches
        norm_score = self._normalized_similarity(product1, product2)
        emb_score = self._embedding_similarity(product1, product2)
        ml_score = self._ml_similarity(product1, product2)
        
        # Weighted combination
        return (norm_score * 0.3 + emb_score * 0.4 + ml_score * 0.3)
    
    # Additional helper methods for detailed analysis would go here...
    # (Implementation continues with analysis methods)
    
    def _optimize_with_training_data(self, threshold_range: List[float]):
        """Optimize thresholds using training data"""
        optimal_threshold = self.trainer.get_optimal_threshold()
        print(f"✅ Optimal threshold from training data: {optimal_threshold:.3f}")
        
        # Test different thresholds
        thresholds = np.arange(threshold_range[0], threshold_range[1], 0.05)
        
        print(f"\n📊 Testing thresholds from {threshold_range[0]} to {threshold_range[1]}:")
        print("Threshold | Precision | Recall | F1-Score")
        print("-" * 40)
        
        for threshold in thresholds:
            # Calculate metrics with this threshold
            precision, recall, f1 = self._calculate_threshold_metrics(threshold)
            print(f"{threshold:8.2f} | {precision:8.3f} | {recall:6.3f} | {f1:8.3f}")
    
    def _optimize_with_heuristics(self, samples: int, cnpj_filter: Optional[str], threshold_range: List[float]):
        """Optimize thresholds using heuristic analysis"""
        test_pairs = self._get_test_pairs(samples, cnpj_filter)
        
        # Calculate similarities for all pairs
        similarities = []
        for product1, product2 in test_pairs:
            score = self._basic_similarity(product1, product2)
            similarities.append(score)
        
        similarities = np.array(similarities)
        
        print(f"Similarity Score Distribution:")
        print(f"  Mean: {np.mean(similarities):.3f}")
        print(f"  Std:  {np.std(similarities):.3f}")
        print(f"  Min:  {np.min(similarities):.3f}")
        print(f"  Max:  {np.max(similarities):.3f}")
        
        # Suggest thresholds based on distribution
        percentiles = [25, 50, 75, 90, 95]
        print(f"\nSuggested thresholds based on percentiles:")
        for p in percentiles:
            threshold = np.percentile(similarities, p)
            print(f"  {p:2d}th percentile: {threshold:.3f}")
    
    def _calculate_threshold_metrics(self, threshold: float) -> Tuple[float, float, float]:
        """Calculate precision, recall, F1 for a given threshold"""
        if not self.trainer.training_examples:
            return 0.0, 0.0, 0.0
        
        true_positives = 0
        false_positives = 0
        true_negatives = 0
        false_negatives = 0
        
        for example in self.trainer.training_examples:
            predicted_similar = example.similarity_scores['final_score'] >= threshold
            actual_similar = example.user_label
            
            if predicted_similar and actual_similar:
                true_positives += 1
            elif predicted_similar and not actual_similar:
                false_positives += 1
            elif not predicted_similar and not actual_similar:
                true_negatives += 1
            else:
                false_negatives += 1
        
        # Calculate metrics
        precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
        recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
        
        return precision, recall, f1
    
    def _show_detailed_algorithm_analysis(self, results: Dict, test_pairs: List):
        """Show detailed analysis of algorithm results"""
        print(f"\n🔍 DETAILED ALGORITHM ANALYSIS")
        print("-" * 50)
        
        # Find pairs with biggest score differences
        algorithms = list(results.keys())
        if len(algorithms) >= 2:
            algo1, algo2 = algorithms[0], algorithms[1]
            scores1 = np.array(results[algo1]['scores'])
            scores2 = np.array(results[algo2]['scores'])
            
            differences = np.abs(scores1 - scores2)
            biggest_diff_indices = np.argsort(differences)[-5:]  # Top 5 differences
            
            print(f"Pairs with biggest differences between {algo1} and {algo2}:")
            for i, idx in enumerate(biggest_diff_indices):
                product1, product2 = test_pairs[idx]
                print(f"  {i+1}. Difference: {differences[idx]:.3f}")
                print(f"     {product1[:50]}...")
                print(f"     {product2[:50]}...")
                print(f"     {algo1}: {scores1[idx]:.3f} | {algo2}: {scores2[idx]:.3f}")
                print()
    
    def _export_test_pairs(self, test_pairs: List, results: Dict):
        """Export test pairs and results for manual validation"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"output/similarity_test_pairs_{timestamp}.json"
        
        FileManager.create_folder("output")
        
        export_data = {
            'metadata': {
                'timestamp': timestamp,
                'total_pairs': len(test_pairs),
                'algorithms_tested': list(results.keys())
            },
            'test_pairs': []
        }
        
        for i, (product1, product2) in enumerate(test_pairs):
            pair_data = {
                'id': i,
                'product1': product1,
                'product2': product2,
                'algorithm_scores': {
                    algo_name: result['scores'][i] 
                    for algo_name, result in results.items()
                }
            }
            export_data['test_pairs'].append(pair_data)
        
        JSONManager.write_json(export_data, output_file)
        print(f"\n💾 Test pairs exported to {output_file}")
    
    def _analyze_similarity_distribution(self, test_pairs: List) -> Dict:
        """Analyze distribution of similarity scores"""
        similarities = []
        for product1, product2 in test_pairs:
            score = self._basic_similarity(product1, product2)
            similarities.append(score)
        
        similarities = np.array(similarities)
        
        return {
            'total_pairs': len(similarities),
            'mean': float(np.mean(similarities)),
            'std': float(np.std(similarities)),
            'min': float(np.min(similarities)),
            'max': float(np.max(similarities)),
            'percentiles': {
                '25th': float(np.percentile(similarities, 25)),
                '50th': float(np.percentile(similarities, 50)),
                '75th': float(np.percentile(similarities, 75)),
                '90th': float(np.percentile(similarities, 90)),
                '95th': float(np.percentile(similarities, 95))
            }
        }
    
    def _compare_algorithms_detailed(self, test_pairs: List) -> Dict:
        """Detailed comparison of algorithms"""
        algorithms = {
            'basic': self._basic_similarity,
            'normalized': self._normalized_similarity,
            'embedding': self._embedding_similarity,
            'ml': self._ml_similarity
        }
        
        comparison = {}
        
        for algo_name, algo_func in algorithms.items():
            scores = []
            processing_times = []
            
            for product1, product2 in test_pairs:
                start_time = time.time()
                try:
                    score = algo_func(product1, product2)
                    scores.append(score)
                except Exception:
                    scores.append(0.0)
                processing_times.append(time.time() - start_time)
            
            comparison[algo_name] = {
                'avg_score': float(np.mean(scores)),
                'std_score': float(np.std(scores)),
                'avg_processing_time': float(np.mean(processing_times)),
                'total_processing_time': float(np.sum(processing_times))
            }
        
        return comparison
    
    def _analyze_normalization_impact(self, test_pairs: List) -> Dict:
        """Analyze impact of normalization"""
        improvements = []
        
        for product1, product2 in test_pairs:
            basic_score = self._basic_similarity(product1, product2)
            normalized_score = self._normalized_similarity(product1, product2)
            improvement = normalized_score - basic_score
            improvements.append(improvement)
        
        improvements = np.array(improvements)
        
        return {
            'avg_improvement': float(np.mean(improvements)),
            'std_improvement': float(np.std(improvements)),
            'positive_improvements': int(np.sum(improvements > 0)),
            'negative_improvements': int(np.sum(improvements < 0)),
            'neutral_improvements': int(np.sum(improvements == 0)),
            'max_improvement': float(np.max(improvements)),
            'min_improvement': float(np.min(improvements))
        }
    
    def _analyze_embedding_performance(self, test_pairs: List) -> Dict:
        """Analyze embedding performance"""
        if not ADVANCED_AVAILABLE or not self.advanced_embedding:
            return {'error': 'Advanced embedding not available'}
        
        embedding_scores = []
        basic_scores = []
        
        for product1, product2 in test_pairs:
            emb_score = self._embedding_similarity(product1, product2)
            basic_score = self._basic_similarity(product1, product2)
            
            embedding_scores.append(emb_score)
            basic_scores.append(basic_score)
        
        embedding_scores = np.array(embedding_scores)
        basic_scores = np.array(basic_scores)
        
        correlation = np.corrcoef(embedding_scores, basic_scores)[0, 1]
        
        return {
            'avg_embedding_score': float(np.mean(embedding_scores)),
            'avg_basic_score': float(np.mean(basic_scores)),
            'correlation_with_basic': float(correlation),
            'embedding_higher_count': int(np.sum(embedding_scores > basic_scores)),
            'basic_higher_count': int(np.sum(basic_scores > embedding_scores))
        }
    
    def _collect_performance_metrics(self, test_pairs: List) -> Dict:
        """Collect comprehensive performance metrics"""
        return {
            'test_pairs_count': len(test_pairs),
            'advanced_components_available': {
                'embedding_engine': ADVANCED_AVAILABLE and self.advanced_embedding is not None,
                'supervised_trainer': self.trainer is not None,
                'trained_model': self.trainer and self.trainer.current_model is not None
            },
            'processing_capabilities': {
                'basic_similarity': True,
                'advanced_normalization': True,
                'embedding_similarity': ADVANCED_AVAILABLE,
                'ml_similarity': self.trainer and self.trainer.current_model is not None
            }
        }
    
    def _show_analysis_summary(self, analysis_results: Dict):
        """Show summary of detailed analysis"""
        print(f"\n📋 ANALYSIS SUMMARY")
        print("=" * 50)
        
        # Similarity distribution
        dist = analysis_results['similarity_distribution']
        print(f"Similarity Distribution:")
        print(f"  Mean: {dist['mean']:.3f} ± {dist['std']:.3f}")
        print(f"  Range: {dist['min']:.3f} - {dist['max']:.3f}")
        print(f"  Median: {dist['percentiles']['50th']:.3f}")
        
        # Algorithm comparison
        comp = analysis_results['algorithm_comparison']
        print(f"\nBest Performing Algorithm:")
        best_algo = max(comp.keys(), key=lambda x: comp[x]['avg_score'])
        print(f"  {best_algo}: {comp[best_algo]['avg_score']:.3f} avg score")
        
        # Normalization impact
        norm = analysis_results['normalization_impact']
        if norm['avg_improvement'] > 0:
            print(f"\nNormalization Impact: +{norm['avg_improvement']:.3f} avg improvement")
        else:
            print(f"\nNormalization Impact: {norm['avg_improvement']:.3f} avg change")
        
        print("=" * 50)


# Add the TodoWrite completion
<function_calls>
<invoke name="TodoWrite">
<parameter name="todos">[{"id": "1", "content": "Read and analyze the improvement plan document (PLANO_MELHORIAS_SIMILARIDADE.md)", "status": "completed", "priority": "high"}, {"id": "2", "content": "Examine existing NFCe domain implementation in src/domains/personal_finance/nfce/", "status": "completed", "priority": "high"}, {"id": "3", "content": "Analyze current similarity detection algorithms and data structures", "status": "completed", "priority": "high"}, {"id": "4", "content": "Research advanced similarity detection techniques", "status": "completed", "priority": "medium"}, {"id": "5", "content": "Propose architectural improvements for similarity detection", "status": "completed", "priority": "medium"}, {"id": "6", "content": "Design training/data collection script for manual intervention", "status": "completed", "priority": "low"}]