-- PostgreSQL initialization script
-- Creates tables for batch pipeline

CREATE TABLE raw_products (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100),
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data JSONB,
    batch_id VARCHAR(200),
    processed BOOLEAN DEFAULT FALSE
);

CREATE TABLE clean_products (
    product_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) CHECK (price > 0),
    stock INTEGER CHECK (stock >= 0),
    source VARCHAR(100),
    category VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE quarantine_products (
    id SERIAL PRIMARY KEY,
    raw_data JSONB,
    issues TEXT,
    quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data for exploration
INSERT INTO raw_products (source, raw_data) VALUES
('source_a', '{"product_id": "P001", "name": "Widget", "price": "$19.99", "stock": "10"}'),
('source_a', '{"product_id": "P002", "name": "Gadget", "price": "25.00", "stock": "5"}'),
('source_b', '{"product_id": "P003", "name": null, "price": "CALL", "stock": "-1"}'),
('source_a', '{"product_id": "P001", "name": "Widget", "price": "$19.99", "stock": "10"}');

INSERT INTO clean_products (product_id, name, price, stock, source, category) VALUES
('P100', 'Premium Widget', 99.99, 15, 'source_a', 'Electronics'),
('P101', 'Standard Gadget', 49.99, 30, 'source_a', 'Electronics'),
('P102', 'Budget Tool', 9.99, 100, 'source_b', 'Home');

CREATE INDEX idx_raw_products_processed ON raw_products(processed);
CREATE INDEX idx_raw_products_batch_id ON raw_products(batch_id);

COMMENT ON TABLE raw_products IS 'Landing zone for unprocessed batch data';
COMMENT ON TABLE clean_products IS 'Validated, cleaned products ready for analytics';
COMMENT ON TABLE quarantine_products IS 'Invalid records for manual review';