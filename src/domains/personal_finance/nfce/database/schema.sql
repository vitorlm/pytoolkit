-- NFCe Processor Database Schema
-- DuckDB schema for storing Brazilian electronic invoice data

-- Establishments table - Business registry
CREATE TABLE IF NOT EXISTS establishments (
    id VARCHAR(36) PRIMARY KEY,
    cnpj VARCHAR(14) NOT NULL UNIQUE,
    cnpj_formatted VARCHAR(18),
    business_name VARCHAR(255),
    trade_name VARCHAR(255),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(8),
    state_registration VARCHAR(20),
    phone VARCHAR(20),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on CNPJ for faster lookups
CREATE INDEX IF NOT EXISTS idx_establishments_cnpj ON establishments(cnpj);

-- Invoices table - Electronic invoice registry
CREATE TABLE IF NOT EXISTS invoices (
    id VARCHAR(36) PRIMARY KEY,
    access_key VARCHAR(44) NOT NULL UNIQUE,
    invoice_number VARCHAR(9) NOT NULL,
    series VARCHAR(3) NOT NULL,
    full_invoice_number VARCHAR(15), -- Series + Invoice Number formatted
    model VARCHAR(2) DEFAULT '65',
    issuer_cnpj VARCHAR(14) NOT NULL,
    state_code VARCHAR(2),
    year_month VARCHAR(4),
    emission_form VARCHAR(1),
    numeric_code VARCHAR(8),
    check_digit VARCHAR(1),
    issue_date TIMESTAMP,
    authorization_date TIMESTAMP,
    environment VARCHAR(1), -- 1=Production, 2=Homologation
    total_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    products_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    consumer_cpf VARCHAR(11),
    consumer_name VARCHAR(255),
    consumer_email VARCHAR(100),
    original_url TEXT,
    validation_hash VARCHAR(40),
    processing_status VARCHAR(20) DEFAULT 'pending',
    has_consumer_info BOOLEAN DEFAULT FALSE,
    items_count INTEGER DEFAULT 0,
    scraped_at TIMESTAMP,
    scraping_success BOOLEAN DEFAULT FALSE,
    scraping_errors TEXT, -- JSON array of error messages
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (issuer_cnpj) REFERENCES establishments(cnpj)
);

-- Create indexes for faster querying
CREATE INDEX IF NOT EXISTS idx_invoices_access_key ON invoices(access_key);
CREATE INDEX IF NOT EXISTS idx_invoices_issuer_cnpj ON invoices(issuer_cnpj);
CREATE INDEX IF NOT EXISTS idx_invoices_issue_date ON invoices(issue_date);
CREATE INDEX IF NOT EXISTS idx_invoices_status ON invoices(processing_status);

-- Products table - Product catalog with deduplication within establishments
CREATE TABLE IF NOT EXISTS products (
    id VARCHAR(36) PRIMARY KEY,
    establishment_id VARCHAR(36) NOT NULL,
    product_code VARCHAR(50),
    barcode VARCHAR(20),
    description TEXT NOT NULL,
    ncm_code VARCHAR(8),
    cest_code VARCHAR(10),
    cfop_code VARCHAR(4),
    unit VARCHAR(10),
    category VARCHAR(100),
    first_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    occurrence_count INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (establishment_id) REFERENCES establishments(id)
);

-- Unique constraint for product identification within each establishment (based on product_code)
CREATE UNIQUE INDEX IF NOT EXISTS idx_products_unique ON products(establishment_id, product_code);
CREATE INDEX IF NOT EXISTS idx_products_establishment_id ON products(establishment_id);
CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode);
CREATE INDEX IF NOT EXISTS idx_products_description ON products(description);

-- Invoice items table - Purchase line items
CREATE TABLE IF NOT EXISTS invoice_items (
    id VARCHAR(36) PRIMARY KEY,
    access_key VARCHAR(44) NOT NULL,
    item_number INTEGER NOT NULL,
    product_id VARCHAR(36),
    product_code VARCHAR(50),
    barcode VARCHAR(20),
    description TEXT NOT NULL,
    ncm_code VARCHAR(8),
    cest_code VARCHAR(10),
    cfop_code VARCHAR(4),
    unit VARCHAR(10),
    quantity DECIMAL(10,4),
    unit_price DECIMAL(10,4),
    total_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    icms_rate DECIMAL(5,2),
    icms_amount DECIMAL(10,2),
    pis_rate DECIMAL(5,2),
    pis_amount DECIMAL(10,2),
    cofins_rate DECIMAL(5,2),
    cofins_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (access_key) REFERENCES invoices(access_key),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Create indexes for invoice items
CREATE INDEX IF NOT EXISTS idx_invoice_items_access_key ON invoice_items(access_key);
CREATE INDEX IF NOT EXISTS idx_invoice_items_product_id ON invoice_items(product_id);
CREATE INDEX IF NOT EXISTS idx_invoice_items_barcode ON invoice_items(barcode);

-- Tax information table - Detailed tax breakdown
CREATE TABLE IF NOT EXISTS tax_information (
    id VARCHAR(36) PRIMARY KEY,
    access_key VARCHAR(44) NOT NULL,
    total_taxes DECIMAL(10,2),
    icms_total DECIMAL(10,2),
    pis_total DECIMAL(10,2),
    cofins_total DECIMAL(10,2),
    ipi_total DECIMAL(10,2),
    iss_total DECIMAL(10,2),
    tax_regime VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (access_key) REFERENCES invoices(access_key)
);

-- Processing log table - Audit trail
CREATE TABLE IF NOT EXISTS processing_log (
    id VARCHAR(36) PRIMARY KEY,
    access_key VARCHAR(44),
    url TEXT,
    status VARCHAR(20), -- 'success', 'error', 'skipped', 'duplicate'
    error_message TEXT,
    processing_time_ms INTEGER,
    scraped_data_quality INTEGER, -- 0-100 score
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for processing log queries
CREATE INDEX IF NOT EXISTS idx_processing_log_status ON processing_log(status);
CREATE INDEX IF NOT EXISTS idx_processing_log_created_at ON processing_log(created_at);

-- Statistics summary table - Pre-computed aggregations
CREATE TABLE IF NOT EXISTS statistics_summary (
    id VARCHAR(36) PRIMARY KEY,
    period_type VARCHAR(20), -- 'daily', 'monthly', 'yearly'
    period_value VARCHAR(20), -- '2024-01', '2024-01-15', '2024'
    total_invoices INTEGER,
    total_amount DECIMAL(12,2),
    total_products INTEGER,
    unique_establishments INTEGER,
    average_invoice_value DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create unique index for statistics periods
CREATE UNIQUE INDEX IF NOT EXISTS idx_statistics_period ON statistics_summary(period_type, period_value);

-- Views for common queries

-- Detailed invoice view with establishment info
CREATE VIEW IF NOT EXISTS v_invoice_details AS
SELECT 
    i.access_key,
    i.full_invoice_number,
    i.issue_date,
    i.total_amount,
    i.items_count,
    e.trade_name as establishment_name,
    e.cnpj_formatted as establishment_cnpj,
    e.city as establishment_city,
    i.consumer_name,
    i.has_consumer_info,
    i.processing_status
FROM invoices i
LEFT JOIN establishments e ON i.issuer_cnpj = e.cnpj
WHERE i.scraping_success = true;

-- Product spending analysis view
CREATE VIEW IF NOT EXISTS v_product_spending AS
SELECT 
    p.description as product_description,
    p.barcode,
    p.category,
    COUNT(ii.id) as purchase_count,
    SUM(ii.quantity) as total_quantity,
    SUM(ii.total_amount) as total_spent,
    AVG(ii.unit_price) as avg_unit_price,
    MIN(ii.unit_price) as min_price,
    MAX(ii.unit_price) as max_price,
    COUNT(DISTINCT ii.access_key) as unique_invoices
FROM products p
JOIN invoice_items ii ON p.id = ii.product_id
GROUP BY p.id, p.description, p.barcode, p.category;

-- Monthly spending by establishment view
CREATE VIEW IF NOT EXISTS v_monthly_establishment_spending AS
SELECT 
    e.trade_name as establishment_name,
    e.cnpj_formatted as establishment_cnpj,
    DATE_TRUNC('month', i.issue_date) as month,
    COUNT(i.id) as invoice_count,
    SUM(i.total_amount) as monthly_total,
    AVG(i.total_amount) as avg_invoice_value,
    SUM(i.items_count) as total_items
FROM invoices i
JOIN establishments e ON i.issuer_cnpj = e.cnpj
WHERE i.scraping_success = true
GROUP BY e.trade_name, e.cnpj_formatted, DATE_TRUNC('month', i.issue_date)
ORDER BY month DESC, monthly_total DESC;