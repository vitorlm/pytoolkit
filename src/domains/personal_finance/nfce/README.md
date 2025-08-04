# NFCe Domain - Brazilian Electronic Invoice Processing

This domain provides comprehensive processing, analysis, and management of Brazilian electronic invoices (NFCe - Nota Fiscal do Consumidor Eletrônica) from Portal SPED MG. The system offers data extraction, product similarity detection, deduplication, and cross-establishment analysis capabilities.

## Overview

The NFCe domain is designed to:
- Extract structured data from Brazilian electronic invoices via web scraping
- Store invoice and product data in a DuckDB database with proper relationships
- Perform advanced product similarity detection and deduplication
- Analyze purchasing patterns across multiple establishments
- Provide comprehensive reporting and data export capabilities

## Architecture

### Core Components

```
src/domains/personal_finance/nfce/
├── commands/           # CLI command implementations
├── services/           # Business logic layer
├── database/           # Data storage and management
├── models/             # Data models and structures
├── similarity/         # Advanced similarity detection engine
└── utils/              # Utility functions and helpers
```

## Commands

### 1. NFCe Processor (`processor`)
**Primary command for processing NFCe URLs and extracting invoice data.**

**Service:** `NFCeService`  
**Dependencies:** `NFCeDataExtractor`, `NFCeHttpClient`, `NFCeDatabaseManager`

```bash
# Process URLs from JSON file
python src/main.py personal_finance nfce processor --input urls.json

# Process single URL
python src/main.py personal_finance nfce processor --url "https://portalsped.fazenda.mg.gov.br/..."

# Import existing data to database
python src/main.py personal_finance nfce processor --import-data results.json --save-db
```

**Features:**
- Concurrent URL processing with configurable batch sizes
- Automatic caching with expiration
- Database storage with relationship management
- Analysis report generation
- Support for force refresh and timeout configuration

### 2. Product Analysis (`product-analysis`)
**Analyze existing products for similarity patterns and potential duplicates.**

**Service:** `ProductAnalysisService`  
**Dependencies:** `NFCeDatabaseManager`, `ProductMatcher`

```bash
# Basic analysis of all products
python src/main.py personal_finance nfce product-analysis

# Analysis with similarity engine
python src/main.py personal_finance nfce product-analysis --use-similarity-engine

# Filter by establishment
python src/main.py personal_finance nfce product-analysis --cnpj "12345678000100"
```

**Features:**
- Basic product statistics and patterns
- Establishment-specific analysis
- Category-based filtering
- Similarity threshold optimization
- Sample-based analysis for large datasets

### 3. Product Deduplication (`product-deduplication`)
**Create clean product master table by merging duplicates and standardizing entries.**

**Service:** `AdvancedProductDeduplicationService`  
**Dependencies:** `NFCeDatabaseManager`

```bash
# Create clean table from database analysis
python src/main.py personal_finance nfce product-deduplication --from-database

# Apply custom similarity threshold
python src/main.py personal_finance nfce product-deduplication --threshold 0.85

# Export to multiple formats
python src/main.py personal_finance nfce product-deduplication --export-csv --export-excel
```

**Features:**
- Advanced product grouping by similarity
- Unit standardization (UN/Un → UN, KG/Kg/kg → KG)
- Quality metrics and confidence scoring
- Comprehensive mapping between original and clean products
- Multiple export formats (JSON, CSV, Excel)

### 4. Cross-Establishment Analysis (`cross-establishment-analysis`)
**Analyze products that appear across multiple establishments.**

**Service:** `CrossEstablishmentAnalysisService`  
**Dependencies:** `NFCeDatabaseManager`

```bash
# Basic cross-establishment analysis
python src/main.py personal_finance nfce cross-establishment-analysis

# Focus on products in 3+ establishments
python src/main.py personal_finance nfce cross-establishment-analysis --min-establishments 3

# Include price analysis
python src/main.py personal_finance nfce cross-establishment-analysis --include-prices
```

**Features:**
- Cross-establishment product frequency analysis
- Price variation detection across establishments
- Category distribution analysis
- Regional vs national product identification
- Establishment relationship detection

### 5. Advanced Similarity (`advanced-similarity`)
**Advanced similarity detection using larger Portuguese language models.**

**Service:** `EnhancedNFCeService`  
**Dependencies:** `AdvancedEmbeddingEngine` (optional), Portuguese BERT models

```bash
# Use BERTimbau Large for highest accuracy
python src/main.py personal_finance nfce advanced-similarity --import-data results.json --model bertimbau-large

# Use Legal BERTimbau for formal product descriptions
python src/main.py personal_finance nfce advanced-similarity --import-data results.json --model legal-bertimbau
```

**Available Models:**
- `bertimbau-large`: neuralmind/bert-large-portuguese-cased (334M parameters)
- `bertimbau-base`: neuralmind/bert-base-portuguese-cased (110M parameters)
- `legal-bertimbau`: rufimelo/Legal-BERTimbau-large (legal domain specialized)
- `multilingual`: paraphrase-multilingual-MiniLM-L12-v2 (lighter, multilingual)

### 6. Similarity Training (`advanced-similarity-training`)
**Interactive training data collection and model training for product similarity.**

**Service:** `SimilarityTrainingHandler`  
**Dependencies:** `SupervisedSimilarityTrainer`, `BrazilianProductNormalizer`, `AdvancedEmbeddingEngine`

```bash
# Collect training data interactively
python src/main.py personal_finance nfce advanced-similarity-training --mode collect --samples 50

# Use active learning for uncertain examples
python src/main.py personal_finance nfce advanced-similarity-training --mode active --suggestions 20

# Train ML models
python src/main.py personal_finance nfce advanced-similarity-training --mode train --model-type random_forest
```

**Training Modes:**
- `collect`: Manual labeling of product pairs
- `active`: Active learning suggestions
- `train`: ML model training
- `evaluate`: Model performance evaluation
- `export`: Training data export

### 7. Similarity Comparison (`advanced-similarity-comparison`)
**Compare different similarity algorithms and analyze performance improvements.**

**Service:** `SimilarityComparisonHandler`  
**Dependencies:** Multiple similarity engines and calculators

```bash
# Compare algorithms
python src/main.py personal_finance nfce advanced-similarity-comparison --mode algorithms --samples 100

# Compare embedding models
python src/main.py personal_finance nfce advanced-similarity-comparison --mode models --detailed

# Find optimal thresholds
python src/main.py personal_finance nfce advanced-similarity-comparison --mode thresholds --range 0.5-0.9
```

### 8. Hybrid Similarity Test (`hybrid-similarity-test`)
**Test the hybrid SBERT + Brazilian rules similarity system.**

**Dependencies:** `EnhancedSimilarityCalculator`, `FeatureExtractor`

```bash
# Test with sample product pairs
python src/main.py personal_finance nfce hybrid-similarity-test --test-samples

# Compare traditional vs hybrid systems
python src/main.py personal_finance nfce hybrid-similarity-test --compare-systems
```

## Core Services

### NFCeService
Primary service for NFCe URL processing and data extraction.

**Key Methods:**
- `process_single_url()`: Process individual NFCe URL
- `process_urls_from_file()`: Batch process URLs from JSON file
- `import_existing_data()`: Import pre-processed invoice data
- `save_to_database()`: Store results in DuckDB database
- `generate_analysis_report()`: Create comprehensive analysis reports

### ProductAnalysisService
Analyzes product data for patterns and similarity detection.

**Key Methods:**
- `analyze_products()`: Comprehensive product analysis
- `analyze_similarity_only()`: Similarity-focused analysis
- `save_analysis_results()`: Export analysis to various formats

### AdvancedProductDeduplicationService
Creates clean product master tables through advanced deduplication.

**Key Methods:**  
- `create_clean_product_master_table()`: Main deduplication process
- `export_to_csv()` / `export_to_excel()`: Multi-format export
- `_calculate_quality_metrics()`: Quality assessment

### CrossEstablishmentAnalysisService
Analyzes products across multiple establishments.

**Key Methods:**
- `analyze_cross_establishment_products()`: Main analysis method
- `export_to_csv()` / `export_to_excel()`: Export capabilities

## Database Schema

The system uses DuckDB with the following core tables:

### establishments
Stores business/establishment information:
- `id` (Primary Key)
- `cnpj` (Brazilian business registration)
- `business_name`
- `city`, `state`
- `cnae_code` (Economic activity classification)
- Company relationship fields (`cnpj_root`, `branch_number`, `company_group_id`)

### invoices
Electronic invoice registry:
- `id` (Primary Key)
- `access_key` (NFCe unique identifier)
- `invoice_number`, `series`
- `issuer_cnpj` (Foreign Key → establishments.cnpj)
- `issue_date`, `total_amount`

### products
Product catalog with establishment context:
- `id` (Primary Key)
- `establishment_id` (Foreign Key → establishments.id)
- `product_code`, `description`, `unit`
- `occurrence_count`

### invoice_items
Purchase line items:
- `id` (Primary Key)
- `access_key` (Foreign Key → invoices.access_key)
- `product_id` (Foreign Key → products.id)
- `quantity`, `unit_price`, `total_amount`

## Similarity Detection Engine

### Traditional Approach
- **Feature Extraction**: Text normalization, tokenization, quantity extraction
- **Similarity Metrics**: Jaccard, Cosine, Levenshtein distance, Token overlap
- **Threshold-based Classification**: Configurable similarity thresholds

### Advanced Approach
- **Portuguese Language Models**: BERTimbau, Legal BERTimbau, E5 Multilingual
- **Brazilian Product Normalization**: Brand extraction, category classification, unit standardization
- **Hybrid Scoring**: Traditional metrics + semantic embeddings + Brazilian-specific rules
- **Machine Learning**: Supervised training with uncertainty-based active learning

### Similarity Components

#### Core Files:
- `feature_extractor.py`: Text feature extraction and normalization
- `similarity_calculator.py`: Traditional similarity algorithms
- `enhanced_similarity_calculator.py`: Hybrid approach with ML integration
- `product_matcher.py`: High-level matching orchestration

#### Advanced Components:
- `advanced_embedding_engine.py`: Multi-model embedding generation
- `brazilian_product_normalizer.py`: BR-specific product normalization
- `supervised_similarity_trainer.py`: ML model training and evaluation
- `hybrid_similarity_engine.py`: Complete hybrid similarity pipeline

## Models and Data Structures

### InvoiceData
Complete invoice representation with establishment, consumer, items, and tax information.

### ProductData
Individual product/item with pricing, quantities, tax details, and classification codes.

### EstablishmentData
Business information including CNPJ, addresses, and contact details.

## Utilities

### HTML Parser (`html_parser.py`)
Extracts structured data from Portal SPED MG HTML pages.

### HTTP Client (`http_client.py`)
Rate-limited HTTP client for ethical web scraping.

### CNAE Classifier (`cnae_classifier.py`)
Classifies establishments by economic activity.

### CNPJ Relationship Detector (`cnpj_relationship_detector.py`)
Identifies corporate relationships between establishments.

## Configuration Files

### nfce_urls.json
Sample NFCe URLs for testing and development (88 real Portal SPED MG URLs).

### Database Schemas
- `schema.sql`: Main database schema with relationships and indexes
- `new_schema.sql`: Enhanced schema with additional features

## Dependencies

### Core Dependencies
- `requests`: HTTP requests
- `duckdb`: Database engine
- `pandas`: Data manipulation
- `beautifulsoup4`: HTML parsing
- `fuzzywuzzy`: String similarity

### Advanced Dependencies (Optional)
- `sentence-transformers`: SBERT embeddings
- `transformers`: HuggingFace models
- `torch`: PyTorch backend
- `scikit-learn`: ML algorithms
- `numpy`: Numerical computations

## Complete Workflow Guide

This section provides a step-by-step guide to create an optimal NFCe database with clean product data for price tracking across establishments.

### Step-by-Step Process

#### Phase 1: Initial Data Collection ⭐ **REQUIRED**

**Goal:** Extract and store raw invoice data from NFCe URLs

**Input:** 
- JSON file with NFCe URLs (format: `{"urls": ["url1", "url2", ...]}`)
- OR single URL
- OR existing processed data (JSON with 'invoices' array)

**Output:**
- JSON file: `output/nfce_results_YYYYMMDD_HHMMSS.json` (always generated)
- Database tables: `establishments`, `invoices`, `products`, `invoice_items` (if `--save-db`)

```bash
# REQUIRED: Process NFCe URLs and create initial database
python src/main.py personal_finance nfce processor --input nfce_urls.json --save-db --analysis

# Alternative: Process single URL
python src/main.py personal_finance nfce processor --url "https://portalsped.fazenda.mg.gov.br/..." --save-db

# Alternative: Import existing data
python src/main.py personal_finance nfce processor --import-data results.json --save-db
```

**What happens:**
- Scrapes invoice data from Portal SPED MG (rate-limited, 30s timeout)
- Creates database schema with indexes and relationships
- Stores raw product descriptions without deduplication
- Generates processing statistics and analysis report
- Uses 60-minute cache for performance

**Database Changes:** Creates and populates all core tables

#### Phase 2: Product Analysis and Pattern Detection ⭐ **REQUIRED**

**Goal:** Understand data quality and identify similarity patterns

**Input:** 
- Database tables: `products` (joined with `establishments`)
- No files required - works directly from database

**Output:**
- JSON file: `output/product_analysis_YYYYMMDD_HHMMSS.json` (always generated)
- Console summary with key statistics

```bash
# REQUIRED: Analyze product patterns and similarity
python src/main.py personal_finance nfce product-analysis --detailed --show-examples --use-similarity-engine

# Optional: Focus on specific establishment
python src/main.py personal_finance nfce product-analysis --cnpj "12345678000100" --detailed

# Optional: Filter by categories
python src/main.py personal_finance nfce product-analysis --category-filter "beverages,fruits" --similarity-threshold 0.75
```

**What happens:**
- Queries all products from database with establishment context
- Auto-detects 12 product categories using keyword matching
- Calculates exact and fuzzy duplicate rates using similarity engines
- Provides establishment-specific analysis and naming patterns
- Recommends optimal similarity thresholds for deduplication

**Database Changes:** Read-only (no modifications)

#### Phase 3: Similarity Model Training 🔧 **OPTIONAL** (Recommended for large datasets)

**Goal:** Improve similarity detection through machine learning

**Input:** 
- Database tables: `products` + `establishments` (for candidate pairs)
- User interaction (for labeling)
- Optional: Existing training data in `data/similarity_training/`

**Output:**
- Training data: `data/similarity_training/similarity_training_data.json`
- Trained models: Saved in memory for current session
- Export files: `output/similarity_training_data_YYYYMMDD.json/csv` (if exported)

**Dependencies:** Requires ML packages: `sentence-transformers`, `torch`, `transformers`, `scikit-learn`

```bash
# 3a. Collect training data interactively (USER INPUT REQUIRED)
python src/main.py personal_finance nfce advanced-similarity-training --mode collect --samples 50 --auto-accept

# 3b. Use active learning for uncertain cases (USER INPUT REQUIRED)
python src/main.py personal_finance nfce advanced-similarity-training --mode active --suggestions 20

# 3c. Train ML model (requires ≥10 labeled examples)
python src/main.py personal_finance nfce advanced-similarity-training --mode train --model-type random_forest

# 3d. Evaluate model performance
python src/main.py personal_finance nfce advanced-similarity-training --mode evaluate --detailed
```

**What happens:**
- Queries database for candidate product pairs with similarity scores 0.4-0.9
- Interactive terminal interface for user to label pairs as same/different
- Active learning suggests most uncertain examples for maximum learning value
- Trains supervised ML models (Random Forest, Gradient Boosting, Logistic Regression)
- Provides performance metrics: accuracy, precision, recall, F1 score, AUC

**Database Changes:** Read-only (stores training data in files)

#### Phase 4: Product Deduplication and Master Table Creation ⭐ **REQUIRED**

**Goal:** Create clean, unified product catalog

**Input:** 
- Database tables: `products` + `establishments` (primary source)
- Optional: Similarity analysis JSON file from Phase 2
- Optional: Trained ML models from Phase 3 (if available)

**Output:**
- JSON file: `output/clean_products_YYYYMMDD_HHMMSS.json` (master catalog + mapping)
- CSV file: `output/clean_products_YYYYMMDD_HHMMSS.csv` (if `--export-csv`)
- Excel file: `output/clean_products_YYYYMMDD_HHMMSS.xlsx` (if `--export-excel`)

```bash
# REQUIRED: Create clean product master table from database
python src/main.py personal_finance nfce product-deduplication --from-database --threshold 0.85 --export-csv --export-excel

# Alternative: Use specific similarity analysis file
python src/main.py personal_finance nfce product-deduplication --input step2_similarity.json --threshold 0.8

# Optional: Generate manual review list for uncertain cases
python src/main.py personal_finance nfce product-deduplication --from-database --manual-review --detailed-mapping
```

**What happens:**
- Groups similar products using similarity algorithms (Phase 2) + ML models (Phase 3 if available)
- Applies unit standardization (UN/Un/un → UN, KG/Kg/kg → KG, etc.)
- Creates master product entries with unique IDs (`MASTER_xxxxxx`)
- Generates complete mapping: original product ID → master product ID
- Calculates quality metrics: confidence scores, reduction percentages
- Identifies products requiring manual review

**Database Changes:** Read-only (creates export files, no database modifications)

#### Phase 5: Cross-Establishment Analysis 🔧 **OPTIONAL** (Recommended for price tracking)

**Goal:** Analyze price variations and purchasing patterns

**Input:** 
- Database tables: `products` joined with `establishments`
- Optional: Clean product master table from Phase 4

**Output:**
- JSON file: `output/cross_establishment_analysis_YYYYMMDD.json`
- CSV file: `output/cross_establishment_analysis_YYYYMMDD.csv` (if `--export-csv`)
- Excel file: Multi-sheet workbook (if `--export-excel`)

```bash
# RECOMMENDED: Analyze products across establishments
python src/main.py personal_finance nfce cross-establishment-analysis --min-establishments 2 --detailed --export-csv

# Optional: Focus on specific categories
python src/main.py personal_finance nfce cross-establishment-analysis --category-focus beverages --min-establishments 3

# Future: Price analysis (placeholder - not yet implemented)
# python src/main.py personal_finance nfce cross-establishment-analysis --include-prices
```

**What happens:**
- Queries products appearing in ≥ min_establishments (default: 2)
- Groups products by normalized descriptions across establishments
- Identifies top cross-establishment products and most diversified establishments
- Analyzes category distribution patterns (beverages, dairy, meat, etc.)
- Generates insights for purchasing optimization
- **Note:** Price analysis is planned but not yet implemented

**Database Changes:** Read-only (no modifications)

#### Phase 6: Advanced Similarity Analysis 🔧 **OPTIONAL** (Research/Enhancement)

**Goal:** Use state-of-the-art Portuguese language models

**Input:** 
- Existing processed NFCe data (JSON file from Phase 1)
- Portuguese BERT models (BERTimbau, Legal-BERTimbau)

**Output:**
- Enhanced similarity analysis with semantic embeddings
- Comparison reports between different similarity approaches

**Dependencies:** Requires `sentence-transformers`, `torch`, `transformers`

```bash
# OPTIONAL: Apply advanced Portuguese language models
python src/main.py personal_finance nfce advanced-similarity --import-data results.json --model bertimbau-large

# OPTIONAL: Compare similarity approaches
python src/main.py personal_finance nfce similarity-comparison --mode algorithms --samples 100 --detailed
```

**What happens:**
- Uses BERTimbau (334M parameters) or Legal-BERTimbau for semantic understanding
- Generates high-dimensional embeddings for product descriptions
- Provides highest accuracy similarity detection using transformer models
- Compares traditional vs advanced similarity approaches

**Database Changes:** Read-only (no modifications)

### Recommended Workflow Order

#### **Minimum Required (Basic Clean Catalog):**
1. ⭐ **Phase 1:** `processor` (with `--save-db`)
2. ⭐ **Phase 2:** `product-analysis` 
3. ⭐ **Phase 4:** `product-deduplication`

#### **Recommended (Enhanced Quality):**
1. ⭐ **Phase 1:** `processor` (with `--save-db`)
2. ⭐ **Phase 2:** `product-analysis`
3. 🔧 **Phase 3:** `advanced-similarity-training` (if dataset > 100 products)
4. ⭐ **Phase 4:** `product-deduplication`
5. 🔧 **Phase 5:** `cross-establishment-analysis`

#### **Research/Advanced (Maximum Accuracy):**
- Add **Phase 6** for comparative analysis and Portuguese BERT models
- Use **Phase 3** extensively for model training and optimization

### Expected Outputs

After completing the workflow, you will have:

#### **Minimum Setup (Phases 1, 2, 4):**
- **Database:** 4 core tables with ~50-90% deduplication rate
- **Clean Catalog:** Master product list with standardized units
- **Export Files:** CSV/Excel ready for price tracking
- **Processing Time:** ~10-30 minutes for 100 invoices

#### **Complete Setup (All Phases):**
- **Optimized Database:** ML-enhanced similarity with ~85-95% accuracy
- **Price Tracking:** Cross-establishment analysis and insights
- **Quality Metrics:** Confidence scores and detailed analytics
- **Training Data:** Reusable ML models for future processing
- **Processing Time:** ~1-3 hours for 100 invoices (including training)

#### **Key Benefits:**
- 🎯 **Price Tracking:** Track same products across different establishments
- 📊 **Data Quality:** 70-90% reduction in duplicate product entries
- 🤖 **ML Enhancement:** Improved similarity detection through training
- 📈 **Insights:** Purchasing patterns and optimization opportunities
- 🔄 **Reusability:** Trained models work on new invoice data

### File Outputs by Phase

```text
output/
├── nfce_results_YYYYMMDD_HHMMSS.json         # Phase 1: Raw processing results
├── product_analysis_YYYYMMDD_HHMMSS.json     # Phase 2: Analysis insights
├── similarity_training_data_YYYYMMDD.json    # Phase 3: Training data
├── clean_products_YYYYMMDD_HHMMSS.csv        # Phase 4: Clean product catalog
├── clean_products_YYYYMMDD_HHMMSS.xlsx       # Phase 4: Excel format
├── cross_establishment_YYYYMMDD.csv          # Phase 5: Multi-establishment analysis
└── advanced_similarity_YYYYMMDD.json         # Phase 6: Enhanced results
```

## Usage Examples

### Quick Start (Minimal Setup - 3 Commands)

```bash
# ⭐ REQUIRED: Phase 1 - Process NFCe URLs and create database
python src/main.py personal_finance nfce processor --input nfce_urls.json --save-db --analysis

# ⭐ REQUIRED: Phase 2 - Analyze patterns and identify duplicates  
python src/main.py personal_finance nfce product-analysis --detailed --use-similarity-engine

# ⭐ REQUIRED: Phase 4 - Create clean product catalog
python src/main.py personal_finance nfce product-deduplication --from-database --export-csv --export-excel
```

**Result:** Clean product catalog ready for price tracking across establishments

### Complete Analysis Pipeline (5-6 Commands)

```bash
# ⭐ Phase 1: Process data and build database
python src/main.py personal_finance nfce processor --input nfce_urls.json --save-db --analysis

# ⭐ Phase 2: Comprehensive product analysis
python src/main.py personal_finance nfce product-analysis --detailed --show-examples --use-similarity-engine

# 🔧 Phase 3a: Interactive similarity training (USER INPUT REQUIRED)
python src/main.py personal_finance nfce advanced-similarity-training --mode collect --samples 30 --auto-accept

# 🔧 Phase 3b: Train ML model
python src/main.py personal_finance nfce advanced-similarity-training --mode train --model-type random_forest

# ⭐ Phase 4: Create master catalog with trained models
python src/main.py personal_finance nfce product-deduplication --from-database --export-excel --detailed-mapping

# 🔧 Phase 5: Cross-establishment analysis for price tracking
python src/main.py personal_finance nfce cross-establishment-analysis --min-establishments 2 --detailed --export-csv
```

**Result:** Optimized database with ML-enhanced similarity detection and cross-establishment insights

### Advanced Similarity Research

```bash
# Compare different similarity approaches
python src/main.py personal_finance nfce similarity-comparison --mode algorithms --samples 100 --detailed
python src/main.py personal_finance nfce similarity-comparison --mode models --compare-all
python src/main.py personal_finance nfce hybrid-similarity-test --compare-systems
```

## Requirements and Troubleshooting

### **System Requirements**

#### **Core Dependencies (Always Required):**
```bash
pip install requests duckdb pandas beautifulsoup4 fuzzywuzzy
```

#### **Advanced ML Dependencies (For Phases 3 & 6):**
```bash
pip install sentence-transformers torch transformers scikit-learn numpy
```

### **Common Issues and Solutions**

#### **Phase 1 Issues:**
- **"No URLs found"**: Check JSON format: `{"urls": ["url1", "url2"]}`
- **Timeout errors**: Increase `--timeout 60` for slow connections
- **Database permission errors**: Ensure write access to `data/` folder

#### **Phase 2 Issues:**
- **"No products in database"**: Run Phase 1 with `--save-db` first
- **Empty analysis**: Check if database has product descriptions
- **Memory issues**: Use `--sample-size 1000` for large datasets

#### **Phase 3 Issues:**
- **"Advanced components not available"**: Install ML dependencies
- **"Need at least 10 examples"**: Label more product pairs in collect mode
- **Training fails**: Ensure balanced positive/negative examples

#### **Phase 4 Issues:**
- **"No similarity analysis found"**: Run Phase 2 first or use `--from-database`
- **Low deduplication rate**: Adjust `--threshold` (try 0.7-0.9 range)
- **Export fails**: Check write permissions in `output/` folder

### **Performance Tips**

- **Small datasets (<50 invoices):** Skip Phase 3, use Phases 1, 2, 4 only
- **Large datasets (>500 products):** Use `--sample-size` in Phase 2 and Phase 3
- **Slow processing:** Reduce `--batch-size` in Phase 1, use `--clear-cache` if needed
- **Memory issues:** Process in smaller batches, avoid keeping all data in memory

## Output and Reports

### Generated Files

- **JSON Results**: Structured data with metadata and statistics
- **CSV Exports**: Tabular data for spreadsheet analysis
- **Excel Reports**: Multi-sheet workbooks with detailed breakdowns
- **Analysis Reports**: Comprehensive insights and recommendations

### Typical Output Structure

```json
{
  "metadata": {
    "created_at": "2024-01-01T12:00:00",
    "total_processed": 100,
    "parameters": {...}
  },
  "results": [...],
  "statistics": {...},
  "quality_metrics": {...},
  "recommendations": [...]
}
```

## Performance Considerations

### Database Optimization

- Indexes on CNPJ, access keys, and description fields
- Partitioning by establishment for large datasets
- Query optimization for cross-establishment analysis

### Similarity Processing

- Caching of computed embeddings and features
- Batch processing for large product sets
- Configurable similarity thresholds for precision/recall balance

### Memory Management

- Lazy loading of database connections
- Streaming processing for large files
- Configurable batch sizes for concurrent operations

## Development and Extension

### Adding New Commands

1. Create command class inheriting from `BaseCommand`
2. Implement required methods: `get_name()`, `get_description()`, `get_help()`, `get_arguments()`, `main()`
3. Create corresponding service class with business logic
4. Add any new database tables or indexes as needed

### Extending Similarity Detection

1. Implement new similarity calculators in `similarity/` directory
2. Add new embedding models to `AdvancedEmbeddingEngine`
3. Extend feature extraction with domain-specific rules
4. Update training pipeline with new features

### Integration Points

- **Logging**: All components use `LogManager` for consistent logging
- **Caching**: `CacheManager` for performance optimization
- **Database**: `NFCeDatabaseManager` for data persistence
- **Configuration**: Environment variables and command-line arguments

This NFCe domain provides a comprehensive solution for Brazilian electronic invoice processing with advanced similarity detection, making it suitable for personal finance analysis, procurement optimization, and market research applications.

## Archived Components

The following components have been archived in `/archive/nfce_unused/` as they are not currently integrated into the command system but may be useful for future development:

- **`qr_code_data.py`**: Complete QR code parsing model with NFCe specification compliance
- **`migration_manager.py`**: Database migration management functionality  
- **`consolidate_duplicates.py`**: CNPJ consolidation utility script

These components can be restored and integrated if needed in future iterations.