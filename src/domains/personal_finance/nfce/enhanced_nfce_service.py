#!/usr/bin/env python3
"""
Enhanced NFCe Service - Integrates hybrid similarity detection with NFCe processing
"""

import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from utils.logging.logging_manager import LogManager
from utils.data.json_manager import JSONManager
from domains.personal_finance.nfce.nfce_processor_service import NFCeService
from domains.personal_finance.nfce.similarity.enhanced_similarity_calculator import EnhancedSimilarityCalculator
from domains.personal_finance.nfce.similarity.feature_extractor import FeatureExtractor
from domains.personal_finance.nfce.database.enhanced_nfce_database_manager import EnhancedNFCeDatabaseManager


class EnhancedNFCeService(NFCeService):
    """Enhanced NFCe service with hybrid similarity detection capabilities"""
    
    def __init__(self, 
                 similarity_threshold: float = 0.60,
                 use_sbert: bool = False,
                 sbert_model: str = "paraphrase-multilingual-MiniLM-L12-v2"):
        """
        Initialize enhanced NFCe service
        
        Args:
            similarity_threshold: Threshold for product similarity matching
            use_sbert: Whether to use SBERT embeddings for enhanced similarity
            sbert_model: SBERT model name for Portuguese embeddings
        """
        
        super().__init__()
        self.logger = LogManager.get_instance().get_logger("EnhancedNFCeService")
        
        # Initialize similarity components
        self.similarity_enabled = True
        self.similarity_threshold = similarity_threshold
        self.use_sbert = use_sbert
        self.sbert_model = sbert_model
        
        try:
            self.similarity_calculator = EnhancedSimilarityCalculator(
                similarity_threshold=similarity_threshold,
                use_hybrid=use_sbert,
                sbert_model=sbert_model
            )
            self.feature_extractor = FeatureExtractor()
            self.logger.info(f"Similarity detection initialized (SBERT: {use_sbert}, threshold: {similarity_threshold})")
        except Exception as e:
            self.logger.warning(f"Failed to initialize similarity detection: {e}")
            self.similarity_enabled = False
            self.similarity_calculator = None
            self.feature_extractor = None
        
        # Override database manager with enhanced version
        self._enhanced_db_manager = None
    
    @property
    def enhanced_db_manager(self):
        """Lazy-loaded enhanced database manager"""
        if self._enhanced_db_manager is None:
            self._enhanced_db_manager = EnhancedNFCeDatabaseManager(
                similarity_threshold=self.similarity_threshold,
                use_sbert=self.use_sbert,
                sbert_model=self.sbert_model
            )
        return self._enhanced_db_manager
    
    def process_urls_with_similarity(self,
                                   urls: List[str],
                                   batch_size: int = 10,
                                   timeout: int = 30,
                                   force_refresh: bool = False,
                                   detect_similar: bool = True) -> Dict[str, Any]:
        """
        Process URLs and detect similar products across establishments
        
        Args:
            urls: List of NFCe URLs to process
            batch_size: Number of URLs to process concurrently
            timeout: Request timeout in seconds
            force_refresh: Force refresh ignoring cache
            detect_similar: Whether to perform similarity detection
            
        Returns:
            Enhanced results with similarity analysis
        """
        
        self.logger.info(f"Processing {len(urls)} URLs with enhanced similarity detection")
        
        # Process URLs directly using the batch processing logic
        results = self._process_urls_batch(
            urls=urls,
            batch_size=batch_size,
            timeout=timeout,
            force_refresh=force_refresh
        )
        
        # Add similarity analysis if enabled
        if detect_similar and self.similarity_enabled and results.get('successful', 0) > 0:
            self.logger.info("Starting similarity analysis...")
            similarity_results = self._analyze_product_similarity(results['invoices'])
            results['similarity_analysis'] = similarity_results
        
        return results
    
    def process_import_data_with_similarity(self,
                                          import_file: str,
                                          save_to_db: bool = False,
                                          detect_similar: bool = True) -> Dict[str, Any]:
        """
        Import existing NFCe data and perform similarity analysis
        
        Args:
            import_file: JSON file with processed NFCe data
            save_to_db: Whether to save to database
            detect_similar: Whether to perform similarity detection
            
        Returns:
            Results with similarity analysis
        """
        
        self.logger.info(f"Importing NFCe data with similarity analysis from: {import_file}")
        
        # Import data using parent method
        results = self.import_existing_data(import_file)
        
        # Debug: check results type
        self.logger.info(f"Import results type: {type(results)}")
        if not isinstance(results, dict):
            self.logger.error(f"Expected dict but got {type(results)}: {results}")
            return {"error": f"Invalid results type: {type(results)}"}
        
        # Add similarity analysis if enabled
        if detect_similar and self.similarity_enabled and results.get('successful', 0) > 0:
            self.logger.info("Starting similarity analysis on imported data...")
            similarity_results = self._analyze_product_similarity(results['invoices'])
            results['similarity_analysis'] = similarity_results
        
        return results
    
    def _analyze_product_similarity(self, invoices: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze product similarity across all invoices
        
        Args:
            invoices: List of processed invoice data
            
        Returns:
            Similarity analysis results
        """
        
        if not self.similarity_enabled:
            return {"error": "Similarity detection not available"}
        
        try:
            # Extract all products from all invoices
            all_products = []
            product_sources = []  # Track which invoice/establishment each product came from
            
            for invoice in invoices:
                establishment_name = invoice.get('establishment', {}).get('business_name', 'Unknown')
                cnpj = invoice.get('establishment', {}).get('cnpj', 'Unknown')
                invoice_number = invoice.get('invoice_number', 'Unknown')
                
                for item in invoice.get('items', []):
                    product_description = item.get('description', '')
                    if product_description.strip():
                        all_products.append(product_description)
                        product_sources.append({
                            'establishment_name': establishment_name,
                            'cnpj': cnpj,
                            'invoice_number': invoice_number,
                            'description': product_description,
                            'unit_price': item.get('unit_price', 0),
                            'quantity': item.get('quantity', 0),
                            'total_price': item.get('total_price', 0)
                        })
            
            self.logger.info(f"Analyzing similarity for {len(all_products)} products from {len(invoices)} invoices")
            
            if len(all_products) < 2:
                return {
                    "total_products": len(all_products),
                    "similar_groups": [],
                    "message": "Need at least 2 products for similarity analysis"
                }
            
            # Extract features for all products
            features_list = []
            for product in all_products:
                try:
                    features = self.feature_extractor.extract(product)
                    features_list.append(features)
                except Exception as e:
                    self.logger.warning(f"Failed to extract features for '{product}': {e}")
                    features_list.append(None)
            
            # Find similar products
            similar_pairs = self.similarity_calculator.calculate_batch_similarity(
                [f for f in features_list if f is not None],
                threshold=self.similarity_threshold
            )
            
            # Group similar products
            similar_groups = self._group_similar_products(similar_pairs, all_products, product_sources)
            
            # Generate statistics
            stats = self._generate_similarity_stats(similar_groups, all_products, invoices)
            
            return {
                "total_products": len(all_products),
                "total_invoices": len(invoices),
                "similar_groups": similar_groups,
                "statistics": stats,
                "similarity_threshold": self.similarity_threshold,
                "analysis_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error in similarity analysis: {e}")
            return {"error": f"Similarity analysis failed: {str(e)}"}
    
    def _group_similar_products(self, 
                               similar_pairs: List[Any], 
                               all_products: List[str],
                               product_sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Group similar products into clusters"""
        
        groups = []
        processed_indices = set()
        
        for pair in similar_pairs:
            # Get product indices (assuming features maintain order)
            prod1_desc = pair.product1.original_description
            prod2_desc = pair.product2.original_description
            
            try:
                idx1 = all_products.index(prod1_desc)
                idx2 = all_products.index(prod2_desc)
            except ValueError:
                continue
            
            if idx1 in processed_indices and idx2 in processed_indices:
                continue
            
            # Create new group or add to existing
            group = {
                "group_id": len(groups) + 1,
                "similarity_score": pair.final_score,
                "confidence_score": getattr(pair, 'confidence_score', 0.0),
                "products": [
                    {
                        **product_sources[idx1],
                        "product_index": idx1
                    },
                    {
                        **product_sources[idx2], 
                        "product_index": idx2
                    }
                ],
                "analysis": {
                    "matching_tokens": getattr(pair, 'matching_tokens', []),
                    "brazilian_tokens": getattr(pair, 'brazilian_tokens', []),
                    "quantity_matches": getattr(pair, 'quantity_matches', []),
                    "explanation": getattr(pair, 'explanation', ''),
                    "embedding_similarity": getattr(pair, 'embedding_similarity', 0.0),
                    "token_rule_similarity": getattr(pair, 'token_rule_similarity', 0.0)
                }
            }
            
            groups.append(group)
            processed_indices.add(idx1)
            processed_indices.add(idx2)
        
        # Sort groups by similarity score
        groups.sort(key=lambda x: x['similarity_score'], reverse=True)
        
        return groups
    
    def _generate_similarity_stats(self, 
                                  similar_groups: List[Dict[str, Any]],
                                  all_products: List[str],
                                  invoices: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate similarity analysis statistics"""
        
        total_similar_products = sum(len(group['products']) for group in similar_groups)
        unique_establishments = set()
        
        for invoice in invoices:
            cnpj = invoice.get('establishment', {}).get('cnpj', '')
            if cnpj:
                unique_establishments.add(cnpj)
        
        # Calculate price variation for similar products
        price_variations = []
        for group in similar_groups:
            prices = [p['unit_price'] for p in group['products'] if p['unit_price'] > 0]
            if len(prices) > 1:
                min_price = min(prices)
                max_price = max(prices)
                variation = ((max_price - min_price) / min_price) * 100 if min_price > 0 else 0
                price_variations.append(variation)
        
        avg_price_variation = sum(price_variations) / len(price_variations) if price_variations else 0
        
        return {
            "total_similar_groups": len(similar_groups),
            "total_similar_products": total_similar_products,
            "similarity_rate": (total_similar_products / len(all_products)) * 100 if all_products else 0,
            "unique_establishments": len(unique_establishments),
            "average_price_variation": round(avg_price_variation, 2),
            "max_price_variation": max(price_variations) if price_variations else 0,
            "groups_with_price_variation": len(price_variations),
            "high_confidence_groups": len([g for g in similar_groups if g.get('confidence_score', 0) > 0.8])
        }
    
    def generate_similarity_report(self, results: Dict[str, Any], output_file: Optional[str] = None) -> str:
        """Generate detailed similarity analysis report"""
        
        if 'similarity_analysis' not in results:
            return "No similarity analysis data available"
        
        analysis = results['similarity_analysis']
        
        report_lines = [
            "# RELATÓRIO DE ANÁLISE DE SIMILARIDADE DE PRODUTOS NFCe",
            "=" * 60,
            "",
            f"📊 **ESTATÍSTICAS GERAIS**",
            f"Total de produtos analisados: {analysis.get('total_products', 0)}",
            f"Total de notas fiscais: {analysis.get('total_invoices', 0)}",
            f"Threshold de similaridade: {analysis.get('similarity_threshold', 0):.2f}",
            f"Data da análise: {analysis.get('analysis_timestamp', 'N/A')}",
            "",
            f"🎯 **RESULTADOS DA SIMILARIDADE**",
            f"Grupos similares encontrados: {analysis['statistics']['total_similar_groups']}",
            f"Produtos com similaridade: {analysis['statistics']['total_similar_products']}",
            f"Taxa de similaridade: {analysis['statistics']['similarity_rate']:.1f}%",
            f"Estabelecimentos únicos: {analysis['statistics']['unique_establishments']}",
            "",
            f"💰 **ANÁLISE DE PREÇOS**",
            f"Variação média de preços: {analysis['statistics']['average_price_variation']:.1f}%",
            f"Variação máxima de preços: {analysis['statistics']['max_price_variation']:.1f}%",
            f"Grupos com variação de preço: {analysis['statistics']['groups_with_price_variation']}",
            f"Grupos alta confiança: {analysis['statistics']['high_confidence_groups']}",
            "",
            "🔍 **GRUPOS DE PRODUTOS SIMILARES**",
            "-" * 40
        ]
        
        # Add detailed group information
        for i, group in enumerate(analysis.get('similar_groups', [])[:10], 1):  # Show top 10
            report_lines.extend([
                "",
                f"**Grupo {i} (Score: {group['similarity_score']:.3f})**",
                f"Confiança: {group.get('confidence_score', 0):.3f}",
                ""
            ])
            
            for j, product in enumerate(group['products'], 1):
                establishment = product['establishment_name'][:30] + "..." if len(product['establishment_name']) > 30 else product['establishment_name']
                report_lines.append(
                    f"{j}. {product['description']} | "
                    f"R$ {product['unit_price']:.2f} | "
                    f"{establishment}"
                )
            
            analysis_info = group.get('analysis', {})
            if analysis_info.get('brazilian_tokens'):
                report_lines.append(f"   🇧🇷 Padrões BR: {', '.join(analysis_info['brazilian_tokens'][:3])}")
            if analysis_info.get('quantity_matches'):
                report_lines.append(f"   📏 Quantidades: {', '.join(analysis_info['quantity_matches'])}")
            if analysis_info.get('explanation'):
                report_lines.append(f"   💬 {analysis_info['explanation']}")
        
        report_content = "\n".join(report_lines)
        
        # Save to file if specified
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(report_content)
                self.logger.info(f"Similarity report saved to: {output_file}")
            except Exception as e:
                self.logger.error(f"Failed to save report: {e}")
        
        return report_content
    
    def save_to_database_with_generic_products(self, results: Dict[str, Any]) -> bool:
        """Save results to database using enhanced database manager with generic products"""
        
        try:
            self.logger.info("Saving results to database with generic product management")
            
            invoices = results.get('invoices', [])
            self.logger.info(f"Found {len(invoices)} invoices to save")
            if not invoices:
                self.logger.warning("No invoices to save")
                return True
            
            # Convert JSON data back to InvoiceData objects
            from domains.personal_finance.nfce.models.invoice_data import InvoiceData
            from decimal import Decimal
            
            saved_count = 0
            failed_count = 0
            
            for i, invoice_dict in enumerate(invoices):
                try:
                    self.logger.debug(f"Processing invoice {i+1}/{len(invoices)}: type={type(invoice_dict)}")
                    
                    if not isinstance(invoice_dict, dict):
                        self.logger.error(f"Invoice {i+1} is not a dict: {type(invoice_dict)}")
                        failed_count += 1
                        continue
                    
                    # Convert dict to InvoiceData object
                    try:
                        invoice_data = self._dict_to_invoice_data(invoice_dict)
                    except Exception as convert_error:
                        self.logger.error(f"Error converting invoice {i+1} to InvoiceData: {convert_error}")
                        failed_count += 1
                        continue
                    
                    # Store using enhanced database manager
                    success = self.enhanced_db_manager.store_invoice_data(invoice_data)
                    
                    if success:
                        saved_count += 1
                    else:
                        failed_count += 1
                        
                except Exception as e:
                    self.logger.error(f"Error saving invoice {invoice_dict.get('access_key', 'unknown')}: {e}")
                    failed_count += 1
            
            # Get processing statistics
            stats = self.enhanced_db_manager.get_statistics()
            
            self.logger.info(f"Database save completed: {saved_count} saved, {failed_count} failed")
            self.logger.info(f"Generic products created: {stats['generic_products_created']}")
            self.logger.info(f"Similarity matches: {stats['similarity_matches']}")
            self.logger.info(f"Exact matches: {stats['exact_matches']}")
            
            return failed_count == 0
            
        except Exception as e:
            self.logger.error(f"Error saving to database with generic products: {e}")
            return False
    
    def _dict_to_invoice_data(self, invoice_dict: Dict[str, Any]):
        """Convert dictionary back to InvoiceData object"""
        
        from domains.personal_finance.nfce.models.invoice_data import (
            InvoiceData, EstablishmentData, ProductData, ConsumerData, TaxData
        )
        from decimal import Decimal
        
        # Convert establishment
        est_dict = invoice_dict.get('establishment', {})
        establishment = EstablishmentData(
            cnpj=est_dict.get('cnpj', ''),
            business_name=est_dict.get('business_name', ''),
            trade_name=est_dict.get('trade_name', ''),
            address=est_dict.get('address', ''),
            city=est_dict.get('city', ''),
            state=est_dict.get('state', ''),
            zip_code=est_dict.get('zip_code', ''),
            cnae_code=est_dict.get('cnae_code', '')
        )
        
        # Convert items
        items = []
        for item_dict in invoice_dict.get('items', []):
            item = ProductData(
                description=item_dict.get('description', ''),
                quantity=Decimal(str(item_dict.get('quantity', 0))),
                unit=item_dict.get('unit', ''),
                unit_price=Decimal(str(item_dict.get('unit_price', 0))),
                total_price=Decimal(str(item_dict.get('total_price', 0)))
            )
            # Add product_code if available
            if 'product_code' in item_dict:
                item.product_code = item_dict['product_code']
            items.append(item)
        
        # Convert consumer (optional)
        consumer = None
        if 'consumer' in invoice_dict and invoice_dict['consumer']:
            cons_dict = invoice_dict['consumer']
            consumer = ConsumerData(
                name=cons_dict.get('name', ''),
                document=cons_dict.get('document', ''),
                address=cons_dict.get('address', '')
            )
        
        # Convert tax data (optional)  
        tax_data = None
        if 'tax_data' in invoice_dict and invoice_dict['tax_data']:
            tax_dict = invoice_dict['tax_data']
            tax_data = TaxData(
                icms_total=Decimal(str(tax_dict.get('icms_total', 0))),
                pis_total=Decimal(str(tax_dict.get('pis_total', 0))),
                cofins_total=Decimal(str(tax_dict.get('cofins_total', 0)))
            )
        
        # Create InvoiceData
        invoice_data = InvoiceData(
            access_key=invoice_dict.get('access_key', ''),
            invoice_number=invoice_dict.get('invoice_number', ''),
            series=invoice_dict.get('series', ''),
            issue_date=datetime.fromisoformat(invoice_dict['issue_date'].replace('Z', '+00:00')) if invoice_dict.get('issue_date') else None,
            total_amount=Decimal(str(invoice_dict.get('total_amount', 0))),
            establishment=establishment,
            items=items,
            consumer=consumer,
            tax_data=tax_data,
            scraping_success=invoice_dict.get('scraping_success', True)
        )
        
        return invoice_data