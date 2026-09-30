CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    country VARCHAR(2) NOT NULL,
    timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_files (
    filename VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    rows_processed INTEGER NOT NULL
);

ALTER TABLE transactions
ADD COLUMN IF NOT EXISTS source_file VARCHAR(255);
