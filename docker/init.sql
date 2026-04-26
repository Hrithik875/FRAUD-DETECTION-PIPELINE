-- Drop existing tables to recreate with correct schema
DROP TABLE IF EXISTS fraud_transactions;
DROP TABLE IF EXISTS merchant_stats;

-- Fraud transactions table (complete schema)
CREATE TABLE fraud_transactions (
    id                SERIAL PRIMARY KEY,
    transaction_id    VARCHAR(50) UNIQUE NOT NULL,
    amount            DECIMAL(10, 2),
    merchant          VARCHAR(100),
    merchant_category VARCHAR(50),
    card_number       VARCHAR(20),
    country           VARCHAR(50),
    fraud_score       DECIMAL(5, 3),
    fraud_label       VARCHAR(20),
    is_fraud          BOOLEAN,
    anomaly_score     DECIMAL(5, 3),
    producer_label    VARCHAR(20),
    actual_class      INTEGER,
    timestamp         TIMESTAMP,
    processed_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Merchant aggregation table (complete schema)
CREATE TABLE merchant_stats (
    id                  SERIAL PRIMARY KEY,
    merchant            VARCHAR(100),
    merchant_category   VARCHAR(50),
    total_transactions  INTEGER,
    fraud_count         INTEGER,
    suspicious_count    INTEGER,
    fraud_rate          DECIMAL(6, 4),
    avg_amount          DECIMAL(10, 2),
    avg_fraud_score     DECIMAL(5, 3),
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for fast dashboard queries
CREATE INDEX idx_fraud_timestamp
    ON fraud_transactions(timestamp DESC);
CREATE INDEX idx_fraud_label
    ON fraud_transactions(fraud_label);
CREATE INDEX idx_merchant
    ON fraud_transactions(merchant);
CREATE INDEX idx_merchant_stats
    ON merchant_stats(merchant);