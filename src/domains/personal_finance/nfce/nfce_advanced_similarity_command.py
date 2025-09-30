#!/usr/bin/env python3
"""
Advanced NFCe Similarity Command - Uses larger Portuguese models for enhanced similarity detection
"""

from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from domains.personal_finance.nfce.enhanced_nfce_service import EnhancedNFCeService


class AdvancedNFCeSimilarityCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "advanced-similarity"

    @staticmethod
    def get_description() -> str:
        return (
            "Advanced NFCe similarity detection using larger Portuguese language models"
        )

    @staticmethod
    def get_help() -> str:
        return """
        Advanced NFCe similarity detection using larger Portuguese language models for enhanced accuracy.
        
        This command uses the most advanced models available for Brazilian Portuguese:
        - BERTimbau Large/Base for semantic understanding
        - Extensive Brazilian product pattern recognition
        - Advanced similarity metrics with confidence scoring
        
        ⚠️  WARNING: Larger models require significant computational resources:
        - Minimum 8GB RAM recommended
        - GPU acceleration recommended for large datasets
        - Processing time will be significantly longer
        
        Examples:
          # Use BERTimbau Large (best accuracy, requires more resources)
          python src/main.py personal_finance nfce advanced-similarity --import-data results.json --model bertimbau-large
          
          # Use BERTimbau Base (good balance of accuracy and speed)
          python src/main.py personal_finance nfce advanced-similarity --import-data results.json --model bertimbau-base
          
          # Use Legal BERTimbau (specialized for legal/formal texts)
          python src/main.py personal_finance nfce advanced-similarity --import-data results.json --model legal-bertimbau
          
          # Custom similarity threshold for high precision
          python src/main.py personal_finance nfce advanced-similarity --import-data results.json --model bertimbau-large --threshold 0.85
          
          # Process with detailed similarity report
          python src/main.py personal_finance nfce advanced-similarity --import-data results.json --model bertimbau-base --detailed-report
        
        Available Models:
          - bertimbau-large: neuralmind/bert-large-portuguese-cased (334M parameters)
          - bertimbau-base: neuralmind/bert-base-portuguese-cased (110M parameters) 
          - legal-bertimbau: rufimelo/Legal-BERTimbau-large (legal domain specialized)
          - multilingual: paraphrase-multilingual-MiniLM-L12-v2 (lighter, multilingual)
        
        Performance Expectations:
          - bertimbau-large: Highest accuracy (90%+ F1-score target), slowest
          - bertimbau-base: Good accuracy (85%+ F1-score), moderate speed
          - legal-bertimbau: Best for formal product descriptions
          - multilingual: Fastest, good baseline (80%+ F1-score)
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        # Input options
        input_group = parser.add_mutually_exclusive_group(required=True)
        input_group.add_argument(
            "--import-data",
            help="Import existing processed NFCe data (JSON file) for similarity analysis",
        )
        input_group.add_argument(
            "--input",
            help="JSON file containing list of NFCe URLs to process with advanced similarity",
        )

        # Model selection
        parser.add_argument(
            "--model",
            type=str,
            choices=[
                "bertimbau-large",
                "bertimbau-base",
                "legal-bertimbau",
                "multilingual",
            ],
            default="bertimbau-base",
            help="Portuguese language model to use (default: bertimbau-base)",
        )

        # Similarity options
        parser.add_argument(
            "--threshold",
            type=float,
            default=0.70,
            help="Similarity threshold for advanced matching (default: 0.70)",
        )
        parser.add_argument(
            "--detailed-report",
            action="store_true",
            help="Generate detailed similarity analysis report with confidence scores",
        )
        parser.add_argument(
            "--min-confidence",
            type=float,
            default=0.6,
            help="Minimum confidence score for similarity matches (default: 0.6)",
        )

        # Processing options
        parser.add_argument(
            "--save-db", action="store_true", help="Save results to local database"
        )
        parser.add_argument(
            "--output",
            help="Output file path for results (default: auto-generated in output/)",
        )
        parser.add_argument(
            "--force-refresh",
            action="store_true",
            help="Force refresh of all data, ignoring cache",
        )
        parser.add_argument(
            "--clear-cache",
            action="store_true",
            help="Clear cached embeddings before processing",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("AdvancedNFCeSimilarityCommand")

        try:
            logger.info("🚀 Starting advanced NFCe similarity analysis")

            # Map model names to actual model identifiers
            model_mapping = {
                "bertimbau-large": "neuralmind/bert-large-portuguese-cased",
                "bertimbau-base": "neuralmind/bert-base-portuguese-cased",
                "legal-bertimbau": "rufimelo/Legal-BERTimbau-large",
                "multilingual": "paraphrase-multilingual-MiniLM-L12-v2",
            }

            selected_model = model_mapping[args.model]
            logger.info(f"Using model: {args.model} ({selected_model})")

            # Check system resources for large models
            if args.model in ["bertimbau-large", "legal-bertimbau"]:
                logger.warning(
                    "Using large model - ensure sufficient RAM (8GB+) and consider GPU acceleration"
                )
                print(
                    "⚠️  Large model selected - this may take significant time and resources"
                )

            # Initialize enhanced service with advanced model
            service = EnhancedNFCeService(
                similarity_threshold=args.threshold,
                use_sbert=True,
                sbert_model=selected_model,
            )

            # Clear cache if requested
            if args.clear_cache:
                logger.info("Clearing cache")
                service.clear_cache()

            # Process based on input type
            if args.import_data:
                logger.info(f"Importing and analyzing data from: {args.import_data}")
                result = service.process_import_data_with_similarity(
                    import_file=args.import_data,
                    save_to_db=args.save_db,
                    detect_similar=True,
                )
            else:  # args.input
                logger.info(
                    f"Processing URLs with advanced similarity from: {args.input}"
                )
                from utils.data.json_manager import JSONManager

                url_data = JSONManager.read_json(args.input)
                urls = url_data.get("urls", [])

                result = service.process_urls_with_similarity(
                    urls=urls,
                    batch_size=5,  # Smaller batch for large models
                    timeout=60,  # Longer timeout for processing
                    force_refresh=args.force_refresh,
                    detect_similar=True,
                )

            # Validate results
            if not result or not isinstance(result, dict):
                raise ValueError("Invalid processing result")

            # Filter results by minimum confidence if specified
            if args.min_confidence > 0 and "similarity_analysis" in result:
                AdvancedNFCeSimilarityCommand._filter_by_confidence(
                    result, args.min_confidence, logger
                )

            # Generate detailed report
            if "similarity_analysis" in result:
                logger.info("Generating advanced similarity report")
                try:
                    from datetime import datetime

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                    if args.detailed_report:
                        report_path = (
                            f"output/advanced_similarity_report_{timestamp}.md"
                        )
                    else:
                        report_path = f"output/similarity_summary_{timestamp}.md"

                    report_content = service.generate_similarity_report(
                        result, report_path
                    )
                    print(f"Advanced similarity report saved to: {report_path}")

                    # Print detailed summary to console
                    AdvancedNFCeSimilarityCommand._print_advanced_summary(
                        result, args.model
                    )

                except Exception as e:
                    logger.warning(
                        f"Failed to generate advanced similarity report: {e}"
                    )

            # Save results
            if args.output:
                output_path = args.output
            else:
                from datetime import datetime

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = f"output/advanced_nfce_results_{timestamp}.json"

            logger.info(f"Saving results to: {output_path}")
            service.save_results(result, output_path)
            print(f"Results saved to: {output_path}")

            # Print completion status
            total_processed = result.get("total_processed", 0)
            successful = result.get("successful", 0)

            if successful > 0:
                logger.info(
                    f"✅ Advanced similarity analysis completed: {successful}/{total_processed} processed"
                )
                print("✅ Analysis completed successfully!")
            else:
                logger.warning("No data was processed successfully")
                print("⚠️  No data was processed - check input file and logs")

        except Exception as e:
            logger.error(f"❌ Advanced similarity analysis failed: {e}")
            print(f"❌ Error: {e}")
            exit(1)

    @staticmethod
    def _filter_by_confidence(result: dict, min_confidence: float, logger):
        """Filter similarity results by minimum confidence score"""

        similarity_analysis = result["similarity_analysis"]
        original_groups = similarity_analysis.get("similar_groups", [])

        filtered_groups = [
            group
            for group in original_groups
            if group.get("confidence_score", 0) >= min_confidence
        ]

        filtered_count = len(original_groups) - len(filtered_groups)
        if filtered_count > 0:
            logger.info(
                f"Filtered {filtered_count} low-confidence groups (min confidence: {min_confidence})"
            )
            similarity_analysis["similar_groups"] = filtered_groups

            # Recalculate statistics
            total_similar_products = sum(
                len(group["products"]) for group in filtered_groups
            )
            similarity_analysis["statistics"]["total_similar_groups"] = len(
                filtered_groups
            )
            similarity_analysis["statistics"]["total_similar_products"] = (
                total_similar_products
            )
            similarity_analysis["statistics"]["high_confidence_groups"] = len(
                [g for g in filtered_groups if g.get("confidence_score", 0) > 0.8]
            )

    @staticmethod
    def _print_advanced_summary(result: dict, model_name: str):
        """Print detailed summary of advanced similarity analysis"""

        if "similarity_analysis" not in result:
            return

        analysis = result["similarity_analysis"]
        stats = analysis.get("statistics", {})

        print(f"\n🎯 ANÁLISE AVANÇADA DE SIMILARIDADE ({model_name.upper()})")
        print("=" * 60)
        print(f"📊 Total de produtos: {analysis.get('total_products', 0)}")
        print(f"📋 Total de notas fiscais: {analysis.get('total_invoices', 0)}")
        print(f"🎯 Threshold: {analysis.get('similarity_threshold', 0):.2f}")
        print()
        print("🔍 RESULTADOS:")
        print(f"   Grupos similares: {stats.get('total_similar_groups', 0)}")
        print(f"   Produtos similares: {stats.get('total_similar_products', 0)}")
        print(f"   Taxa de similaridade: {stats.get('similarity_rate', 0):.1f}%")
        print(f"   Alta confiança: {stats.get('high_confidence_groups', 0)} grupos")
        print()
        print("💰 ANÁLISE DE PREÇOS:")
        print(f"   Variação média: {stats.get('average_price_variation', 0):.1f}%")
        print(f"   Variação máxima: {stats.get('max_price_variation', 0):.1f}%")
        print(f"   Estabelecimentos únicos: {stats.get('unique_establishments', 0)}")

        # Show top similarity groups
        groups = analysis.get("similar_groups", [])[:5]  # Top 5
        if groups:
            print(f"\n🏆 TOP {len(groups)} GRUPOS MAIS SIMILARES:")
            for i, group in enumerate(groups, 1):
                products = group.get("products", [])
                if len(products) >= 2:
                    prod1 = (
                        products[0]["description"][:40] + "..."
                        if len(products[0]["description"]) > 40
                        else products[0]["description"]
                    )
                    prod2 = (
                        products[1]["description"][:40] + "..."
                        if len(products[1]["description"]) > 40
                        else products[1]["description"]
                    )

                    print(
                        f"{i}. Score: {group['similarity_score']:.3f} | Confiança: {group.get('confidence_score', 0):.3f}"
                    )
                    print(f"   {prod1}")
                    print(f"   {prod2}")

        print("\n" + "=" * 60)

        # Performance recommendations
        if model_name == "bertimbau-large":
            print("🏆 Usando modelo mais avançado - máxima precisão alcançada!")
        elif model_name == "bertimbau-base":
            print("✅ Boa performance com modelo balanceado!")
            print("💡 Para máxima precisão, tente: --model bertimbau-large")
        else:
            print(
                "⚡ Modelo rápido usado - para melhor precisão, tente modelos BERTimbau!"
            )
