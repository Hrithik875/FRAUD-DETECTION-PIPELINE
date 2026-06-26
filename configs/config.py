# ============================================
# Central Configuration File
# fraud-detection-pipeline/configs/config.py
# ============================================

# Kafka Settings
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "transactions"
KAFKA_PARTITIONS = 3
KAFKA_REPLICATION = 1
KAFKA_GROUP_ID = "fraud-consumer-group"

# PostgreSQL Settings
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "frauddb"
POSTGRES_USER = "frauduser"
POSTGRES_PASSWORD = "fraudpass"
POSTGRES_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Fraud Scoring Thresholds
HIGH_AMOUNT_THRESHOLD = 5000.0      # Transactions above $5000
VELOCITY_WINDOW_MINUTES = 10        # Time window for velocity check
VELOCITY_MAX_TRANSACTIONS = 5       # Max txns per card in window
HIGH_RISK_CATEGORIES = [
    "Electronics",
    "Jewelry",
    "Crypto",
    "Wire Transfer",
    "Gift Cards"
]

# Fraud Score Weights
WEIGHT_HIGH_AMOUNT = 0.4
WEIGHT_VELOCITY = 0.4
WEIGHT_HIGH_RISK_MERCHANT = 0.2

# Fraud Classification Thresholds
FRAUD_THRESHOLD = 0.7               # score >= 0.7 → FRAUD
SUSPICIOUS_THRESHOLD = 0.4          # score >= 0.4 → SUSPICIOUS

# Spark Settings
SPARK_APP_NAME = "FraudDetectionPipeline"
SPARK_BATCH_INTERVAL = "5 seconds"
CHECKPOINT_DIR = "../data_output/checkpoints"

# Flask Settings
FLASK_HOST = "0.0.0.0"
FLASK_PORT = 5000
FLASK_DEBUG = True
DASHBOARD_REFRESH_SECONDS = 3

# Producer Settings
PRODUCER_DELAY_SECONDS = 1          # 1 transaction per second
DATASET_PATH = "../data/creditcard.csv"

# BENCHMARK & PERFORMANCE CONFIG
PRODUCER_RATE_PER_SEC = 1           # default: 1 msg/sec (existing behavior)
BENCHMARK_MODE = False              # when True: removes sleep, sends at max speed
BENCHMARK_MESSAGE_COUNT = 100_000   # number of messages to send in benchmark mode
METRICS_FLUSH_INTERVAL_SEC = 5      # how often to write throughput metrics to DB