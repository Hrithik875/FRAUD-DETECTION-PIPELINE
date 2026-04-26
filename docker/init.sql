-- Create fraud transactions table
CREATE TABLE IF NOT EXISTS fraud_transactions (
    id              SERIAL PRIMARY KEY,
    transaction_id  VARCHAR(50) UNIQUE NOT NULL,
    amount          DECIMAL(10, 2),
    merchant        VARCHAR(100),
    merchant_category VARCHAR(50),
    card_number     VARCHAR(20),
    country         VARCHAR(50),
    fraud_score     DECIMAL(4, 3),
    fraud_label     VARCHAR(20),
    is_fraud        BOOLEAN,
    timestamp       TIMESTAMP,
    processed_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create aggregated stats table
CREATE TABLE IF NOT EXISTS merchant_stats (
    id                  SERIAL PRIMARY KEY,
    merchant            VARCHAR(100),
    total_transactions  INTEGER,
    fraud_count         INTEGER,
    fraud_rate          DECIMAL(5, 4),
    avg_amount          DECIMAL(10, 2),
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast dashboard queries
CREATE INDEX IF NOT EXISTS idx_fraud_timestamp 
    ON fraud_transactions(timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_fraud_label 
    ON fraud_transactions(fraud_label);

CREATE INDEX IF NOT EXISTS idx_merchant 
    ON fraud_transactions(merchant);