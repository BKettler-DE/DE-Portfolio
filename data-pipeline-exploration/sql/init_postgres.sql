-- This runs automatically when PostgreSQL container starts for the first time

-- Create tables for batch pipeline
CREATE TABLE raw_products (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100),
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data JSONB,
    processed BOOLEAN DEFAULT FALSE
);

CREATE TABLE clean_products (
    product_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) CHECK (price > 0),
    stock INTEGER CHECK (stock >= 0),
    source VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE quarantine_products (
    id SERIAL PRIMARY KEY,
    raw_data JSONB,
    issues TEXT,
    quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert some sample data
INSERT INTO raw_products (source, raw_data) VALUES
('source_a', '{"product_id": "P001", "name": "Widget", "price": "$19.99", "stock": "10"}'),
('source_a', '{"product_id": "P002", "name": "Gadget", "price": "25.00", "stock": "5"}'),
('source_b', '{"product_id": "P003", "name": null, "price": "CALL", "stock": "-1"}'),
('source_a', '{"product_id": "P001", "name": "Widget", "price": "$19.99", "stock": "10"}');

-- Create an index for faster queries
CREATE INDEX idx_raw_products_processed ON raw_products(processed);

-- Some pre-populated clean data for testing
INSERT INTO clean_products (product_id, name, price, stock, source) VALUES
('P100', 'Premium Widget', 99.99, 15, 'source_a'),
('P101', 'Standard Gadget', 49.99, 30, 'source_a'),
('P102', 'Budget Tool', 9.99, 100, 'source_b');

COMMENT ON TABLE raw_products IS 'Landing zone for unprocessed data';
COMMENT ON TABLE clean_products IS 'Validated, cleaned data ready for analytics';
COMMENT ON TABLE quarantine_products IS 'Invalid records for manual review';