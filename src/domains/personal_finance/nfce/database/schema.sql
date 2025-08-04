-- NFCe Processor Database Schema
-- DuckDB schema for storing Brazilian electronic invoice data

-- Establishments table - Business registry (simplified)
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
-- Create indexes for company relationships
CREATE INDEX IF NOT EXISTS idx_establishments_cnpj_root ON establishments (cnpj_root);

CREATE INDEX IF NOT EXISTS idx_establishments_company_group ON establishments (company_group_id);

CREATE INDEX IF NOT EXISTS idx_establishments_branch_number ON establishments (branch_number);

-- Invoices table - Electronic invoice registry (simplified)
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

-- Create indexes for faster querying
CREATE INDEX IF NOT EXISTS idx_invoices_access_key ON invoices (access_key);

CREATE INDEX IF NOT EXISTS idx_invoices_issuer_cnpj ON invoices (issuer_cnpj);

CREATE INDEX IF NOT EXISTS idx_invoices_issue_date ON invoices (issue_date);

-- Products table - Product catalog with deduplication within establishments (simplified)
CREATE TABLE IF NOT EXISTS products (
    id VARCHAR(36) PRIMARY KEY,
    establishment_id VARCHAR(36) NOT NULL,
    product_code VARCHAR(50),
    description TEXT NOT NULL,
    unit VARCHAR(10),
    occurrence_count INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (establishment_id) REFERENCES establishments (id)
);

-- Unique constraint for product identification within each establishment (based on product_code)
CREATE UNIQUE INDEX IF NOT EXISTS idx_products_unique ON products (
    establishment_id,
    product_code
);

CREATE INDEX IF NOT EXISTS idx_products_establishment_id ON products (establishment_id);

CREATE INDEX IF NOT EXISTS idx_products_description ON products (description);

-- Invoice items table - Purchase line items (simplified)
CREATE TABLE IF NOT EXISTS invoice_items (
    id VARCHAR(36) PRIMARY KEY,
    access_key VARCHAR(44) NOT NULL,
    product_id VARCHAR(36),
    quantity DECIMAL(10, 4),
    unit_price DECIMAL(10, 4),
    total_amount DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (access_key) REFERENCES invoices (access_key),
    FOREIGN KEY (product_id) REFERENCES products (id)
);

-- Create indexes for invoice items
CREATE INDEX IF NOT EXISTS idx_invoice_items_access_key ON invoice_items (access_key);

CREATE INDEX IF NOT EXISTS idx_invoice_items_product_id ON invoice_items (product_id);

-- Company Groups table - Track relationships between establishments
CREATE TABLE IF NOT EXISTS company_groups (
    id VARCHAR(8) PRIMARY KEY, -- Same as cnpj_root
    company_name VARCHAR(255), -- Main company name
    total_establishments INTEGER DEFAULT 0,
    main_office_cnpj VARCHAR(14), -- CNPJ of the main office (ends with 0001)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for company groups
CREATE INDEX IF NOT EXISTS idx_company_groups_main_office ON company_groups (main_office_cnpj);

-- Simple view for spending analysis with establishment types
CREATE VIEW IF NOT EXISTS v_spending_summary AS
SELECT
    e.business_name as establishment_name,
    e.establishment_type,
    e.cnpj,
    e.cnae_code,
    p.description as product_description,
    COUNT(ii.id) as purchase_count,
    SUM(ii.quantity) as total_quantity,
    SUM(ii.total_amount) as total_spent,
    AVG(ii.unit_price) as avg_unit_price
FROM
    establishments e
    JOIN invoices i ON e.cnpj = i.issuer_cnpj
    JOIN invoice_items ii ON i.access_key = ii.access_key
    JOIN products p ON ii.product_id = p.id
GROUP BY
    e.business_name,
    e.establishment_type,
    e.cnpj,
    e.cnae_code,
    p.description;

-- View for spending by establishment type
CREATE VIEW IF NOT EXISTS v_spending_by_type AS
SELECT
    e.establishment_type,
    COUNT(DISTINCT e.id) as establishment_count,
    COUNT(i.id) as invoice_count,
    SUM(i.total_amount) as total_spent,
    AVG(i.total_amount) as avg_invoice_value,
    SUM(i.items_count) as total_items
FROM establishments e
    JOIN invoices i ON e.cnpj = i.issuer_cnpj
GROUP BY
    e.establishment_type
ORDER BY total_spent DESC;

-- View for establishment relationships - Shows which establishments belong to the same company
CREATE VIEW IF NOT EXISTS v_establishment_relationships AS
SELECT
    e.cnpj,
    e.business_name,
    e.cnpj_root,
    e.branch_number,
    e.is_main_office,
    e.company_group_id,
    cg.company_name as group_company_name,
    cg.total_establishments as related_establishments_count,
    cg.main_office_cnpj as group_main_office_cnpj,
    CASE
        WHEN e.is_main_office THEN 'MATRIZ'
        ELSE 'FILIAL'
    END as establishment_role
FROM
    establishments e
    LEFT JOIN company_groups cg ON e.company_group_id = cg.id
ORDER BY e.cnpj_root, e.branch_number;

-- View for company groups summary
CREATE VIEW IF NOT EXISTS v_company_groups_summary AS
SELECT
    cg.id as company_group_id,
    cg.company_name,
    cg.total_establishments,
    cg.main_office_cnpj,
    COUNT(e.id) as actual_establishments_count,
    STRING_AGG (e.cnpj, ', ') as all_cnpjs,
    STRING_AGG (e.business_name, ' | ') as all_business_names,
    SUM(
        CASE
            WHEN e.is_main_office THEN 1
            ELSE 0
        END
    ) as main_offices_count,
    SUM(
        CASE
            WHEN NOT e.is_main_office THEN 1
            ELSE 0
        END
    ) as branches_count
FROM
    company_groups cg
    LEFT JOIN establishments e ON cg.id = e.company_group_id
GROUP BY
    cg.id,
    cg.company_name,
    cg.total_establishments,
    cg.main_office_cnpj
ORDER BY cg.total_establishments DESC;