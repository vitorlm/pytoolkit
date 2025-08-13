#!/usr/bin/env python3
"""
Hybrid Similarity Test Command - Test the new SBERT + Brazilian rules system
"""

import json
from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from .similarity.enhanced_similarity_calculator import EnhancedSimilarityCalculator
from .similarity.feature_extractor import FeatureExtractor


class HybridSimilarityTestCommand(BaseCommand):
    
    @staticmethod
    def get_name() -> str:
        return "hybrid-similarity-test"
    
    @staticmethod
    def get_description() -> str:
        return "Test hybrid SBERT + Brazilian rules similarity system"
    
    @staticmethod
    def get_help() -> str:
        return """
        Test the new hybrid similarity system that combines:
        - SBERT Portuguese embeddings for semantic similarity
        - Brazilian product token rules and patterns
        - Quantity and brand matching
        
        Examples:
        python src/main.py personal_finance nfce hybrid-similarity-test --compare-systems
        python src/main.py personal_finance nfce hybrid-similarity-test --test-samples
        python src/main.py personal_finance nfce hybrid-similarity-test --benchmark
        """
    
    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument("--compare-systems", action="store_true", 
                          help="Compare traditional vs hybrid systems")
        parser.add_argument("--test-samples", action="store_true",
                          help="Test with sample product pairs")
        parser.add_argument("--benchmark", action="store_true",
                          help="Benchmark against clean training data")
        parser.add_argument("--threshold", type=float, default=0.80,
                          help="Similarity threshold for testing")
        parser.add_argument("--sbert-model", type=str, 
                          default="rufimelo/Legal-BERTimbau-large",
                          help="SBERT model to use for Portuguese embeddings")
        parser.add_argument("--disable-hybrid", action="store_true",
                          help="Test with traditional algorithms only")
    
    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("HybridSimilarityTestCommand")
        
        try:
            logger.info("🧪 Starting hybrid similarity system test")
            
            # Initialize systems
            use_hybrid = not args.disable_hybrid
            enhanced_calc = EnhancedSimilarityCalculator(
                similarity_threshold=args.threshold,
                use_hybrid=use_hybrid,
                sbert_model=args.sbert_model
            )
            
            feature_extractor = FeatureExtractor()
            
            if args.test_samples:
                HybridSimilarityTestCommand._test_sample_pairs(enhanced_calc, feature_extractor, logger)
            
            if args.benchmark:
                HybridSimilarityTestCommand._benchmark_with_training_data(enhanced_calc, feature_extractor, logger)
            
            if args.compare_systems:
                HybridSimilarityTestCommand._compare_systems(enhanced_calc, feature_extractor, logger, args.threshold)
            
            # Default action if no specific test requested
            if not any([args.test_samples, args.benchmark, args.compare_systems]):
                logger.info("No specific test requested. Running sample test...")
                HybridSimilarityTestCommand._test_sample_pairs(enhanced_calc, feature_extractor, logger)
            
            logger.info("✅ Hybrid similarity test completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Hybrid similarity test failed: {e}")
            exit(1)
    
    @staticmethod
    def _test_sample_pairs(enhanced_calc: EnhancedSimilarityCalculator, 
                          feature_extractor: FeatureExtractor, logger):
        """Test with sample product pairs"""
        
        logger.info("🔬 Testing with sample product pairs")
        
        # Test pairs with expected results
        test_pairs = [
            # High similarity pairs
            ("BANANA PRATA", "BANANA PRATA KG", True),
            ("COCA COLA 600", "COCA COLA TRAD", True),
            ("QJO MUS FAT", "MUSS D FORM FAT", True),
            ("MEXERICA POCAM", "MEXERICA PONKAN", True),
            ("VURTUOSO 10MG 30'S C1", "VURTUOSO 10MG CPR 30", True),
            
            # Low similarity pairs  
            ("PIZZA ESP VM CARNE D", "GRA CARNE DE SOL", False),
            ("GRUYERE PRESID 160", "QJO MUS FAT", False),
            ("BOMB SONHO VALSA UN", "BOMBOM GAROTO 250G", False),
            ("CERV HEINEKEN 473ML", "CERV KRUG 500ML DOUB", False),
            ("LEVOTIROX 100 MKG 30'S", "LEVOTIROX 88 MKG 30'S", False),
        ]
        
        print("\n" + "="*80)
        print("🧪 TESTE COM PARES DE EXEMPLO")
        print("="*80)
        
        correct_predictions = 0
        total_predictions = 0
        
        for product1, product2, expected_similar in test_pairs:
            
            # Extract features
            features1 = feature_extractor.extract_features(product1)
            features2 = feature_extractor.extract_features(product2)
            
            # Calculate similarity
            result = enhanced_calc.calculate_similarity(features1, features2)
            
            # Make prediction
            predicted_similar = result.final_score >= enhanced_calc.similarity_threshold
            is_correct = predicted_similar == expected_similar
            
            if is_correct:
                correct_predictions += 1
            total_predictions += 1
            
            # Display result
            status = "✅" if is_correct else "❌"
            expected_text = "SIM" if expected_similar else "NÃO"
            predicted_text = "SIM" if predicted_similar else "NÃO"
            
            print(f"\n{status} Produto 1: {product1}")
            print(f"   Produto 2: {product2}")
            print(f"   Esperado: {expected_text} | Predito: {predicted_text} | Score: {result.final_score:.3f}")
            print(f"   Confiança: {result.confidence_score:.3f}")
            
            # Show detailed scores
            if hasattr(result, 'embedding_similarity'):
                print(f"   📊 Embedding: {result.embedding_similarity:.3f} | "
                      f"Tokens BR: {result.token_rule_similarity:.3f} | "
                      f"Tradicional: {(result.jaccard_score + result.cosine_score) / 2:.3f}")
            
            if result.brazilian_tokens:
                print(f"   🇧🇷 Tokens BR: {', '.join(result.brazilian_tokens[:3])}")
            
            if result.quantity_matches:
                print(f"   📏 Quantidades: {', '.join(result.quantity_matches)}")
            
            print(f"   💬 {result.explanation}")
        
        accuracy = correct_predictions / total_predictions
        print("\n📊 RESULTADO FINAL:")
        print(f"Acurácia: {accuracy:.1%} ({correct_predictions}/{total_predictions})")
        
        if accuracy >= 0.9:
            print("🎉 EXCELENTE! Sistema híbrido funcionando muito bem!")
        elif accuracy >= 0.8:
            print("✅ BOM! Sistema híbrido com boa performance!")
        else:
            print("⚠️  Precisa de ajustes no sistema híbrido")
    
    @staticmethod
    def _benchmark_with_training_data(enhanced_calc: EnhancedSimilarityCalculator,
                                    feature_extractor: FeatureExtractor, logger):
        """Benchmark against clean training data"""
        
        logger.info("📊 Benchmarking against clean training data")
        
        try:
            # Load clean training data
            with open('similarity_training_data.json', 'r', encoding='utf-8') as f:
                training_data = json.load(f)
            
            pairs = training_data['pairs']
            logger.info(f"Loaded {len(pairs)} training pairs")
            
            print("\n" + "="*80)
            print("📊 BENCHMARK COM DADOS LIMPOS DE TREINAMENTO")
            print("="*80)
            
            # Test each pair
            predictions = []
            actuals = []
            scores = []
            confidences = []
            
            for pair in pairs:
                product1 = pair['product1']
                product2 = pair['product2']
                actual_similar = pair['is_same']
                
                # Extract features
                features1 = feature_extractor.extract_features(product1)
                features2 = feature_extractor.extract_features(product2)
                
                # Calculate similarity
                result = enhanced_calc.calculate_similarity(features1, features2)
                
                # Make prediction
                predicted_similar = result.final_score >= enhanced_calc.similarity_threshold
                
                predictions.append(predicted_similar)
                actuals.append(actual_similar)
                scores.append(result.final_score)
                confidences.append(result.confidence_score)
            
            # Calculate metrics
            tp = sum(1 for i in range(len(actuals)) if actuals[i] and predictions[i])
            fp = sum(1 for i in range(len(actuals)) if not actuals[i] and predictions[i])
            tn = sum(1 for i in range(len(actuals)) if not actuals[i] and not predictions[i])
            fn = sum(1 for i in range(len(actuals)) if actuals[i] and not predictions[i])
            
            accuracy = (tp + tn) / len(actuals)
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
            
            avg_confidence = sum(confidences) / len(confidences)
            avg_score = sum(scores) / len(scores)
            
            print("\n🎯 MÉTRICAS DO SISTEMA HÍBRIDO:")
            print(f"Accuracy:    {accuracy:.1%} ({tp + tn}/{len(actuals)})")
            print(f"Precision:   {precision:.1%}")
            print(f"Recall:      {recall:.1%}")
            print(f"F1-Score:    {f1:.1%}")
            print(f"Confiança Média: {avg_confidence:.3f}")
            print(f"Score Médio:     {avg_score:.3f}")
            
            print("\n📈 Confusion Matrix:")
            print(f"True Positives:  {tp}")
            print(f"False Positives: {fp}")  
            print(f"True Negatives:  {tn}")
            print(f"False Negatives: {fn}")
            
            # Compare with baseline
            baseline_f1 = 0.848  # Previous best result
            improvement = (f1 - baseline_f1) * 100
            
            print("\n📊 COMPARAÇÃO COM BASELINE:")
            print(f"Baseline F1-Score: {baseline_f1:.1%}")
            print(f"Híbrido F1-Score:  {f1:.1%}")
            if improvement > 0:
                print(f"🎉 MELHORIA: +{improvement:.1f} pontos percentuais!")
            else:
                print(f"📉 Diferença: {improvement:.1f} pontos percentuais")
            
            if f1 >= 0.90:
                print("\n🏆 META ALCANÇADA! F1-Score ≥ 90%")
            elif f1 >= baseline_f1:
                print("\n✅ SISTEMA MELHORADO! Superou baseline anterior")
            else:
                print("\n⚠️  Sistema precisa de ajustes para superar baseline")
                
        except FileNotFoundError:
            logger.error("Training data file not found: similarity_training_data.json")
        except Exception as e:
            logger.error(f"Error benchmarking: {e}")
    
    @staticmethod
    def _compare_systems(enhanced_calc: EnhancedSimilarityCalculator,
                        feature_extractor: FeatureExtractor, logger, threshold: float):
        """Compare traditional vs hybrid systems"""
        
        logger.info("⚖️  Comparing traditional vs hybrid systems")
        
        # Test with both systems
        test_pairs = [
            ("QJO MUS FAT", "MUSS D FORM FAT"),
            ("BANANA PRATA", "BANANA PRATA KG"),
            ("COCA COLA 600", "COCA COLA TRAD"),
            ("PIZZA ESP VM CARNE D", "GRA CARNE DE SOL"),
            ("VURTUOSO 10MG 30'S C1", "VURTUOSO 10MG CPR 30")
        ]
        
        print("\n" + "="*80)
        print("⚖️  COMPARAÇÃO: TRADICIONAL vs HÍBRIDO")
        print("="*80)
        
        for product1, product2 in test_pairs:
            features1 = feature_extractor.extract_features(product1)
            features2 = feature_extractor.extract_features(product2)
            
            # Test with hybrid system
            result_hybrid = enhanced_calc.calculate_similarity(features1, features2)
            
            # Test with traditional only (create new calculator)
            traditional_calc = EnhancedSimilarityCalculator(
                similarity_threshold=threshold,
                use_hybrid=False
            )
            result_traditional = traditional_calc.calculate_similarity(features1, features2)
            
            print(f"\n📦 {product1}")
            print(f"📦 {product2}")
            print(f"   🔧 Tradicional: {result_traditional.final_score:.3f}")
            print(f"   🚀 Híbrido:     {result_hybrid.final_score:.3f}")
            
            if hasattr(result_hybrid, 'embedding_similarity'):
                print(f"   📊 Embedding:   {result_hybrid.embedding_similarity:.3f}")
                print(f"   🇧🇷 Tokens BR:   {result_hybrid.token_rule_similarity:.3f}")
            
            diff = result_hybrid.final_score - result_traditional.final_score
            if abs(diff) > 0.1:
                symbol = "📈" if diff > 0 else "📉"
                print(f"   {symbol} Diferença: {diff:+.3f}")
        
        print(f"\n💡 Threshold usado: {threshold}")
        print("🔧 Tradicional: Jaccard + Cosine + Levenshtein + Token Overlap")
        print("🚀 Híbrido: Tradicional + SBERT + Regras Brasileiras + Quantidades")