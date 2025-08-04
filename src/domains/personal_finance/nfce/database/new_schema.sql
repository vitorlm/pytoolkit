-- NFCe Processor Database Schema - REDESIGNED WITH GENERIC PRODUCTS
-- DuckDB schema for storing Brazilian electronic invoice data with product deduplication

-- Establishments table - Business registry (unchanged)
CREATE TABLE IF NOT EXISTS establishments (
    id VARCHAR(36) PRIMARY KEY,
    cnpj VARCHAR(14) NOT NULL UNIQUE,
    business_name VARCHAR(255) NOT NULL,
    establishment_type VARCHAR(50),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(2),
    cnae_code VARCHAR(10),
    cnpj_root VARCHAR(8), -- Primeiros 8 dígitos do CNPJ (identifica a empresa)
    branch_number VARCHAR(4), -- Número da filial (0001 = matriz)
    is_main_office BOOLEAN DEFAULT FALSE, -- TRUE se for matriz (branch_number = 0001)
    company_group_id VARCHAR(8), -- ID do grupo empresarial (mesmo que cnpj_root)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on CNPJ for faster lookups
CREATE INDEX IF NOT EXISTS idx_establishments_cnpj ON establishments (cnpj);
CREATE INDEX IF NOT EXISTS idx_establishments_cnpj_root ON establishments (cnpj_root);
CREATE INDEX IF NOT EXISTS idx_establishments_company_group ON establishments (company_group_id);
CREATE INDEX IF NOT EXISTS idx_establishments_branch_number ON establishments (branch_number);

-- Invoices table - Electronic invoice registry (unchanged)
CREATE TABLE IF NOT EXISTS invoices (
    id VARCHAR(36) PRIMARY KEY,
    access_key VARCHAR(44) NOT NULL UNIQUE,
    invoice_number VARCHAR(9) NOT NULL,
    series VARCHAR(3) NOT NULL,
    issuer_cnpj VARCHAR(14) NOT NULL,
    issue_date TIMESTAMP,
    total_amount DECIMAL(10, 2),
    items_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (issuer_cnpj) REFERENCES establishments (cnpj)
);

CREATE INDEX IF NOT EXISTS idx_invoices_access_key ON invoices (access_key);
CREATE INDEX IF NOT EXISTS idx_invoices_issuer_cnpj ON invoices (issuer_cnpj);
CREATE INDEX IF NOT EXISTS idx_invoices_issue_date ON invoices (issue_date);

-- ==========================================
-- NEW: GENERIC PRODUCTS TABLE
-- ==========================================
-- Generic products - Products that exist across establishments
CREATE TABLE IF NOT EXISTS generic_products (
    id VARCHAR(36) PRIMARY KEY,
    normalized_name VARCHAR(500) NOT NULL, -- Nome normalizado para deduplicação
    canonical_description TEXT NOT NULL, -- Descrição canônica principal
    alternative_descriptions TEXT[], -- Descrições alternativas encontradas
    category VARCHAR(100), -- Categoria inferida (alimento, medicamento, etc.)
    brand VARCHAR(100), -- Marca inferida
    unit VARCHAR(10), -- Unidade padrão (KG, UN, ML, etc.)
    
    -- Similarity metadata
    similarity_features TEXT, -- Features extraídas para comparação (JSON)
    confidence_score DECIMAL(3, 2) DEFAULT 1.0, -- Confiança na deduplicação (0.0-1.0)
    
    -- Aggregated statistics
    total_occurrences INTEGER DEFAULT 1, -- Quantas vezes apareceu em notas
    establishments_count INTEGER DEFAULT 1, -- Em quantos estabelecimentos diferentes
    avg_price DECIMAL(10, 4), -- Preço médio
    min_price DECIMAL(10, 4), -- Menor preço encontrado
    max_price DECIMAL(10, 4), -- Maior preço encontrado
    price_variance DECIMAL(5, 2), -- Variância de preços (%)
    
    -- Timestamps
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for generic products
CREATE UNIQUE INDEX IF NOT EXISTS idx_generic_products_normalized ON generic_products (normalized_name);
CREATE INDEX IF NOT EXISTS idx_generic_products_category ON generic_products (category);
CREATE INDEX IF NOT EXISTS idx_generic_products_brand ON generic_products (brand);
CREATE INDEX IF NOT EXISTS idx_generic_products_occurrences ON generic_products (total_occurrences);
CREATE INDEX IF NOT EXISTS idx_generic_products_establishments ON generic_products (establishments_count);

-- ==========================================
-- UPDATED: ESTABLISHMENT PRODUCTS TABLE
-- ==========================================
-- Establishment-specific product information (pricing, codes, etc.)
CREATE TABLE IF NOT EXISTS establishment_products (
    id VARCHAR(36) PRIMARY KEY,
    generic_product_id VARCHAR(36) NOT NULL, -- Referência ao produto genérico
    establishment_id VARCHAR(36) NOT NULL, -- Estabelecimento específico
    
    -- Establishment-specific data
    local_product_code VARCHAR(50), -- Código do produto no estabelecimento
    local_description TEXT NOT NULL, -- Descrição original no estabelecimento
    local_unit VARCHAR(10), -- Unidade usada pelo estabelecimento
    
    -- Pricing statistics for this establishment
    current_price DECIMAL(10, 4), -- Último preço conhecido
    avg_price DECIMAL(10, 4), -- Preço médio neste estabelecimento
    min_price DECIMAL(10, 4), -- Menor preço neste estabelecimento
    max_price DECIMAL(10, 4), -- Maior preço neste estabelecimento
    
    -- Occurrence tracking
    occurrence_count INTEGER DEFAULT 1, -- Quantas vezes apareceu neste estabelecimento
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (generic_product_id) REFERENCES generic_products (id),
    FOREIGN KEY (establishment_id) REFERENCES establishments (id)
);

-- Unique constraint: one entry per generic product per establishment
CREATE UNIQUE INDEX IF NOT EXISTS idx_establishment_products_unique ON establishment_products (
    generic_product_id, 
    establishment_id
);

CREATE INDEX IF NOT EXISTS idx_establishment_products_generic ON establishment_products (generic_product_id);
CREATE INDEX IF NOT EXISTS idx_establishment_products_establishment ON establishment_products (establishment_id);
CREATE INDEX IF NOT EXISTS idx_establishment_products_code ON establishment_products (local_product_code);

-- ==========================================
-- UPDATED: INVOICE ITEMS TABLE
-- ==========================================
-- Invoice items - Now points to generic products
CREATE TABLE IF NOT EXISTS invoice_items (
    id VARCHAR(36) PRIMARY KEY,
    access_key VARCHAR(44) NOT NULL,
    generic_product_id VARCHAR(36) NOT NULL, -- Aponta para produto genérico
    establishment_product_id VARCHAR(36), -- Referência opcional aos dados específicos do estabelecimento
    
    -- Item-specific data from the invoice
    original_description TEXT NOT NULL, -- Descrição original na nota fiscal
    quantity DECIMAL(10, 4),
    unit_price DECIMAL(10, 4),
    total_amount DECIMAL(10, 2),
    
    -- Similarity tracking
    similarity_match_score DECIMAL(3, 2), -- Score de similaridade usado para match
    match_confidence DECIMAL(3, 2), -- Confiança no match automático
    is_manual_match BOOLEAN DEFAULT FALSE, -- Se foi validado manualmente
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (access_key) REFERENCES invoices (access_key),
    FOREIGN KEY (generic_product_id) REFERENCES generic_products (id),
    FOREIGN KEY (establishment_product_id) REFERENCES establishment_products (id)
);

CREATE INDEX IF NOT EXISTS idx_invoice_items_access_key ON invoice_items (access_key);
CREATE INDEX IF NOT EXISTS idx_invoice_items_generic_product ON invoice_items (generic_product_id);
CREATE INDEX IF NOT EXISTS idx_invoice_items_establishment_product ON invoice_items (establishment_product_id);
CREATE INDEX IF NOT EXISTS idx_invoice_items_similarity_score ON invoice_items (similarity_match_score);

-- ==========================================
-- PRODUCT SIMILARITY MATCHES TABLE
-- ==========================================
-- Track similarity matches for auditing and improvement
CREATE TABLE IF NOT EXISTS product_similarity_matches (
    id VARCHAR(36) PRIMARY KEY,
    source_description TEXT NOT NULL, -- Descrição original sendo processada
    matched_generic_product_id VARCHAR(36) NOT NULL, -- Produto genérico encontrado
    similarity_score DECIMAL(3, 2) NOT NULL, -- Score de similaridade
    confidence_score DECIMAL(3, 2), -- Score de confiança
    match_method VARCHAR(50), -- Método usado (exact, similarity, manual)
    
    -- Detailed match information
    matching_tokens TEXT[], -- Tokens que fizeram match
    brazilian_patterns TEXT[], -- Padrões brasileiros identificados
    quantity_matches TEXT[], -- Quantidades que fizeram match
    
    -- Processing metadata
    establishment_id VARCHAR(36), -- Estabelecimento onde foi processado
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (matched_generic_product_id) REFERENCES generic_products (id),
    FOREIGN KEY (establishment_id) REFERENCES establishments (id)
);

CREATE INDEX IF NOT EXISTS idx_similarity_matches_generic_product ON product_similarity_matches (matched_generic_product_id);
CREATE INDEX IF NOT EXISTS idx_similarity_matches_score ON product_similarity_matches (similarity_score);
CREATE INDEX IF NOT EXISTS idx_similarity_matches_method ON product_similarity_matches (match_method);

-- ==========================================
-- Company Groups table (unchanged)
-- ==========================================
CREATE TABLE IF NOT EXISTS company_groups (
    id VARCHAR(8) PRIMARY KEY,
    company_name VARCHAR(255),
    total_establishments INTEGER DEFAULT 0,
    main_office_cnpj VARCHAR(14),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_company_groups_main_office ON company_groups (main_office_cnpj);

-- ==========================================
-- UPDATED VIEWS WITH GENERIC PRODUCTS
-- ==========================================

-- View for spending analysis with generic products
CREATE VIEW IF NOT EXISTS v_spending_summary AS
SELECT
    e.business_name as establishment_name,
    e.establishment_type,
    e.cnpj,
    e.cnae_code,
    gp.canonical_description as product_description,
    gp.category as product_category,
    gp.brand as product_brand,
    COUNT(ii.id) as purchase_count,
    SUM(ii.quantity) as total_quantity,
    SUM(ii.total_amount) as total_spent,
    AVG(ii.unit_price) as avg_unit_price,
    MIN(ii.unit_price) as min_unit_price,
    MAX(ii.unit_price) as max_unit_price,
    (MAX(ii.unit_price) - MIN(ii.unit_price)) / MIN(ii.unit_price) * 100 as price_variation_percent
FROM
    establishments e
    JOIN invoices i ON e.cnpj = i.issuer_cnpj
    JOIN invoice_items ii ON i.access_key = ii.access_key
    JOIN generic_products gp ON ii.generic_product_id = gp.id
GROUP BY
    e.business_name,
    e.establishment_type,
    e.cnpj,
    e.cnae_code,
    gp.canonical_description,
    gp.category,
    gp.brand;

-- View for generic product analytics
CREATE VIEW IF NOT EXISTS v_generic_products_analytics AS
SELECT
    gp.id,
    gp.canonical_description,
    gp.category,
    gp.brand,
    gp.total_occurrences,
    gp.establishments_count,
    gp.avg_price,
    gp.min_price,
    gp.max_price,
    gp.price_variance,
    gp.confidence_score,
    COUNT(DISTINCT ii.access_key) as unique_invoices,
    SUM(ii.total_amount) as total_revenue,
    AVG(ii.unit_price) as current_avg_price,
    STDDEV(ii.unit_price) as price_stddev,
    DATE_DIFF('day', gp.first_seen, gp.last_seen) as days_in_market
FROM
    generic_products gp
    LEFT JOIN invoice_items ii ON gp.id = ii.generic_product_id
GROUP BY
    gp.id,
    gp.canonical_description,
    gp.category,
    gp.brand,
    gp.total_occurrences,
    gp.establishments_count,
    gp.avg_price,
    gp.min_price,
    gp.max_price,
    gp.price_variance,
    gp.confidence_score,
    gp.first_seen,
    gp.last_seen
ORDER BY gp.total_occurrences DESC;

-- View for price comparison across establishments
CREATE VIEW IF NOT EXISTS v_price_comparison AS
SELECT
    gp.canonical_description as product,
    gp.category,
    gp.brand,
    e.business_name as establishment,
    e.cnpj,
    e.city,
    ep.current_price,
    ep.avg_price as establishment_avg_price,
    gp.avg_price as global_avg_price,
    (ep.avg_price - gp.avg_price) / gp.avg_price * 100 as price_diff_percent,
    ep.occurrence_count as times_bought,
    RANK() OVER (PARTITION BY gp.id ORDER BY ep.avg_price) as price_rank
FROM
    generic_products gp
    JOIN establishment_products ep ON gp.id = ep.generic_product_id
    JOIN establishments e ON ep.establishment_id = e.id
WHERE 
    gp.establishments_count > 1 -- Only products available in multiple establishments
ORDER BY 
    gp.total_occurrences DESC,
    price_rank;

-- View for similarity match quality
CREATE VIEW IF NOT EXISTS v_similarity_quality AS
SELECT
    psm.match_method,
    COUNT(*) as total_matches,
    AVG(psm.similarity_score) as avg_similarity_score,
    AVG(psm.confidence_score) as avg_confidence_score,
    COUNT(CASE WHEN psm.similarity_score >= 0.8 THEN 1 END) as high_similarity_matches,
    COUNT(CASE WHEN psm.confidence_score >= 0.8 THEN 1 END) as high_confidence_matches,
    COUNT(DISTINCT psm.matched_generic_product_id) as unique_products_matched,
    COUNT(DISTINCT psm.establishment_id) as unique_establishments
FROM
    product_similarity_matches psm
GROUP BY
    psm.match_method
ORDER BY
    total_matches DESC;