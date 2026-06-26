# Fraud Detection Pipeline

An end-to-end real-time fraud detection pipeline built with Apache Kafka, Spark Structured Streaming, PostgreSQL, and Flask — benchmarked at 3,200+ msg/sec with an offline ML evaluation achieving 83.5% precision / 82.7% recall (Random Forest, AUC 0.964) on the ULB Credit Card Fraud dataset.

## Architecture

  creditcard.csv
       │
       ▼
  [Producer] ──── 3,200+ msg/sec ────► [Apache Kafka]
                                              │
                                              ▼
                                   [Spark Structured Streaming]
                                   • 5-layer fraud scoring
                                   • Latency instrumentation
                                              │
                              ┌───────────────┤
                              ▼               ▼
                        [PostgreSQL]    [Pipeline Metrics]
                              │
                              ▼
                        [Flask REST API]
                              │
                              ▼
                    [Chart.js Dashboard]
                    • Live fraud feed
                    • Pipeline health metrics
                    • Latency sparkline

## Tech Stack

Streaming:    Apache Kafka, Spark Structured Streaming (PySpark)
Storage:      PostgreSQL
Backend:      Python, Flask, psycopg2
Frontend:     HTML, JavaScript, Chart.js
DevOps:       Docker, Docker Compose
ML Eval:      scikit-learn, imbalanced-learn (SMOTE)
Dataset:      ULB Credit Card Fraud (Kaggle, 284,807 transactions)

## Benchmark Results

| Model               | Precision | Recall | F1     | AUC-ROC |
|---------------------|-----------|--------|--------|---------|
| Random Forest       | 0.8351    | 0.8265 | 0.8308 | 0.9644  |
| Logistic Regression | 0.1350    | 0.8980 | 0.2347 | 0.9772  |
| Rule-Based Pipeline | 0.0293    | 0.5408 | 0.0556 | N/A     |

> Evaluated on 20% held-out test set (56,962 transactions) with SMOTE applied to training set only. Random Forest with class_weight='balanced' achieves best overall F1. Rule-based scorer reflects the heuristic scoring logic in the live Spark pipeline.

## Pipeline Performance

| Metric              | Value                    |
|---------------------|--------------------------|
| Producer Throughput | 3,200+ msg/sec           |
| Avg Pipeline Latency| ~5s (local, single node) |
| Microbatch Interval | 2 seconds                |
| Containerized       | Yes (Docker Compose)     |

> Latency measured end-to-end from Kafka produce to PostgreSQL write on a single-machine deployment (Kafka + Spark + PostgreSQL + Flask running concurrently). Production deployment with dedicated nodes would reduce this to sub-second.

## Project Structure

  FRAUD-DETECTION-PIPELINE/
  ├── api/                  # Flask REST API
  ├── benchmark/            # Offline ML evaluation scripts
  │   ├── evaluate.py       # Benchmark runner (LR, RF, Rule-based)
  │   └── results.json      # Latest benchmark output
  ├── configs/              # Central configuration
  ├── dashboard/            # Chart.js frontend
  ├── docker/               # Docker Compose + PostgreSQL init
  ├── producer/             # Kafka transaction producer
  ├── streaming/            # Spark Structured Streaming engine
  ├── requirements.txt
  └── start_pipeline.bat    # One-command pipeline startup (Windows)

## Quick Start

  # 1. Clone and install dependencies
  pip install -r requirements.txt

  # 2. Start the pipeline (Kafka, PostgreSQL, Spark, Producer, Flask)
  make up

  # 3. Open dashboard
  http://localhost:5000

  # 4. Run ML benchmark (optional, standalone)
  make benchmark

  # 5. Stop the pipeline
  make down

## Dataset

Uses the ULB Credit Card Fraud Detection dataset (Kaggle). Download creditcard.csv and place it at data/creditcard.csv before running.
Link: https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud
