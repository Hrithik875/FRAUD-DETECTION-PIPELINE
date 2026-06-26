# ============================================================
# benchmark/evaluate.py
# Offline ML Benchmark — Fraud Detection Pipeline
#
# Compares:
#   1. Logistic Regression (sklearn, SMOTE-balanced)
#   2. Random Forest       (sklearn, SMOTE-balanced)
#   3. Rule-Based Baseline (replicates pipeline heuristic scoring)
#
# Usage: python benchmark/evaluate.py   (from project root)
#
# No database, Kafka, or Docker required.
# Fully deterministic via random_state=42 throughout.
# ============================================================

import os
import json
import numpy as np
import pandas as pd
from collections import Counter

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
)
from imblearn.over_sampling import SMOTE


# ── Path resolution ─────────────────────────────────────────
# Works whether invoked from project root or from benchmark/
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
DATASET_PATH = os.path.join(_ROOT, "data", "creditcard.csv")
RESULTS_PATH = os.path.join(_HERE, "results.json")


# ── Step 7 helper — rule-based scorer ───────────────────────

def rule_based_predict(X):
    """
    Simplified replication of pipeline fraud scoring using
    available creditcard.csv features.
    Returns binary predictions (0 or 1).
    """
    scores = np.zeros(len(X))

    # Rule 1: High transaction amount (proxy for amount anomaly)
    amount = X["Amount"].values
    scores += (amount > np.percentile(amount, 95)).astype(float) * 0.3

    # Rule 2: Unusual V1 (strongest PCA component for fraud in this dataset)
    v1 = X["V1"].values
    scores += (v1 < np.percentile(v1, 5)).astype(float) * 0.3

    # Rule 3: Unusual V3
    v3 = X["V3"].values
    scores += (v3 < np.percentile(v3, 5)).astype(float) * 0.2

    # Rule 4: Unusual V14 (known high-signal feature for this dataset)
    v14 = X["V14"].values
    scores += (v14 < np.percentile(v14, 5)).astype(float) * 0.2

    return (scores >= 0.5).astype(int)


# ── Main ─────────────────────────────────────────────────────

def main():

    # ── STEP 1: Load and validate dataset ───────────────────
    df = pd.read_csv(DATASET_PATH)
    print(f"[DATA] Loaded {len(df):,} transactions")
    print(f"[DATA] Fraud rate: {df['Class'].mean() * 100:.3f}%")

    X = df.drop(columns=["Class"])
    y = df["Class"]

    # ── STEP 2: Train / test split ───────────────────────────
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    print(f"[SPLIT] Train: {len(X_train):,} | Test: {len(X_test):,}")

    # ── STEP 3: SMOTE on training set only ───────────────────
    smote = SMOTE(random_state=42)
    X_train_resampled, y_resampled = smote.fit_resample(X_train, y_train)
    print(f"[SMOTE] Resampled training set: {Counter(y_resampled)}")

    # ── STEP 4: Feature scaling ──────────────────────────────
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train_resampled)
    X_test_scaled  = scaler.transform(X_test)

    # ── STEP 5: Logistic Regression ──────────────────────────
    lr_model = LogisticRegression(
        max_iter=1000, random_state=42, class_weight="balanced"
    )
    lr_model.fit(X_train_scaled, y_resampled)
    print("[LR] Training complete")

    lr_pred  = lr_model.predict(X_test_scaled)
    lr_proba = lr_model.predict_proba(X_test_scaled)[:, 1]

    lr_precision = precision_score(y_test, lr_pred,  average="binary")
    lr_recall    = recall_score(   y_test, lr_pred,  average="binary")
    lr_f1        = f1_score(       y_test, lr_pred,  average="binary")
    lr_auc       = roc_auc_score(  y_test, lr_proba)

    # ── STEP 6: Random Forest ────────────────────────────────
    rf_model = RandomForestClassifier(
        n_estimators=100, random_state=42,
        class_weight="balanced", n_jobs=-1
    )
    rf_model.fit(X_train_scaled, y_resampled)
    print("[RF] Training complete")

    rf_pred  = rf_model.predict(X_test_scaled)
    rf_proba = rf_model.predict_proba(X_test_scaled)[:, 1]

    rf_precision = precision_score(y_test, rf_pred,  average="binary")
    rf_recall    = recall_score(   y_test, rf_pred,  average="binary")
    rf_f1        = f1_score(       y_test, rf_pred,  average="binary")
    rf_auc       = roc_auc_score(  y_test, rf_proba)

    # ── STEP 7: Rule-based baseline ──────────────────────────
    # Use original unscaled X_test (as a DataFrame) for interpretability
    X_test_df = pd.DataFrame(X_test, columns=X.columns)
    rb_pred   = rule_based_predict(X_test_df)

    rb_precision = precision_score(y_test, rb_pred, average="binary")
    rb_recall    = recall_score(   y_test, rb_pred, average="binary")
    rb_f1        = f1_score(       y_test, rb_pred, average="binary")
    # roc_auc requires probabilities — rule-based gives only binary outputs
    rule_based_auc = "N/A"
    print("[RULE] Scoring complete")

    # ── STEP 8: Print results table ──────────────────────────
    print("\n" + "=" * 65)
    print("FRAUD DETECTION BENCHMARK RESULTS")
    print("Dataset: ULB Credit Card Fraud (284,807 transactions, SMOTE resampling)")
    print("=" * 65)
    print(f"{'Model':<25} {'Precision':>10} {'Recall':>8} {'F1':>8} {'AUC-ROC':>10}")
    print("-" * 65)
    print(
        f"{'Logistic Regression':<25} "
        f"{lr_precision:>10.4f} {lr_recall:>8.4f} "
        f"{lr_f1:>8.4f} {lr_auc:>10.4f}"
    )
    print(
        f"{'Random Forest':<25} "
        f"{rf_precision:>10.4f} {rf_recall:>8.4f} "
        f"{rf_f1:>8.4f} {rf_auc:>10.4f}"
    )
    print(
        f"{'Rule-Based (Pipeline)':<25} "
        f"{rb_precision:>10.4f} {rb_recall:>8.4f} "
        f"{rb_f1:>8.4f} {'N/A':>10}"
    )
    print("=" * 65)
    print(
        f"\n[SUMMARY] Best Precision: "
        f"{'RF' if rf_precision > lr_precision else 'LR'}"
    )
    print(
        f"[SUMMARY] Best Recall:    "
        f"{'RF' if rf_recall > lr_recall else 'LR'}"
    )
    print(
        f"[SUMMARY] Best F1:        "
        f"{'RF' if rf_f1 > lr_f1 else 'LR'}"
    )
    print(
        f"[SUMMARY] Rule-based F1 vs RF: "
        f"{((rb_f1 - rf_f1) / rf_f1 * 100):+.1f}%"
    )

    # ── STEP 9: Save results.json ────────────────────────────
    results = {
        "dataset": "ULB Credit Card Fraud",
        "total_transactions": int(len(df)),
        "fraud_rate_pct": round(float(df["Class"].mean() * 100), 3),
        "test_set_size": int(len(X_test)),
        "smote_applied": True,
        "models": {
            "logistic_regression": {
                "precision": round(float(lr_precision), 6),
                "recall":    round(float(lr_recall),    6),
                "f1":        round(float(lr_f1),        6),
                "roc_auc":   round(float(lr_auc),       6),
            },
            "random_forest": {
                "precision": round(float(rf_precision), 6),
                "recall":    round(float(rf_recall),    6),
                "f1":        round(float(rf_f1),        6),
                "roc_auc":   round(float(rf_auc),       6),
            },
            "rule_based_pipeline": {
                "precision": round(float(rb_precision), 6),
                "recall":    round(float(rb_recall),    6),
                "f1":        round(float(rb_f1),        6),
                "roc_auc":   None,
            },
        },
    }

    os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n[SAVED] Results written to benchmark/results.json")


if __name__ == "__main__":
    main()
