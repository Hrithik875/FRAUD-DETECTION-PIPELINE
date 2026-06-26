# ============================================
# Flask API Server
# fraud-detection-pipeline/api/app.py
# ============================================

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.stdout.reconfigure(encoding='utf-8')

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import psycopg2
import psycopg2.extras
from datetime import datetime
import configs.config as cfg

app = Flask(__name__)
CORS(app)

# ── DB Connection ─────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(
        host=cfg.POSTGRES_HOST,
        port=cfg.POSTGRES_PORT,
        dbname=cfg.POSTGRES_DB,
        user=cfg.POSTGRES_USER,
        password=cfg.POSTGRES_PASSWORD
    )


def query(sql, params=None):
    """Execute a query and return all rows as dicts"""
    conn = get_db()
    try:
        with conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def query_one(sql, params=None):
    """Execute a query and return one row as dict"""
    rows = query(sql, params)
    return rows[0] if rows else {}


# ── Helper ────────────────────────────────────────────────────────────

def serialize(rows):
    """Convert Decimal/datetime to JSON-serializable types"""
    result = []
    for row in rows:
        clean = {}
        for k, v in row.items():
            if hasattr(v, '__float__'):
                clean[k] = float(v)
            elif isinstance(v, datetime):
                clean[k] = v.isoformat()
            else:
                clean[k] = v
        result.append(clean)
    return result


# ── API Routes ────────────────────────────────────────────────────────

@app.route('/api/stats')
def stats():
    """
    Overall pipeline statistics.
    Used by: summary cards at top of dashboard.
    """
    row = query_one("""
        SELECT
            COUNT(*)                                    AS total,
            COUNT(*) FILTER (WHERE fraud_label='FRAUD')
                                                        AS fraud,
            COUNT(*) FILTER (WHERE fraud_label='SUSPICIOUS')
                                                        AS suspicious,
            COUNT(*) FILTER (WHERE fraud_label='LEGIT')
                                                        AS legit,
            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE fraud_label='FRAUD'
                ) / NULLIF(COUNT(*), 0), 2
            )                                           AS fraud_pct,
            ROUND(AVG(fraud_score)::numeric, 3)         AS avg_score,
            ROUND(AVG(amount)::numeric, 2)              AS avg_amount,
            MAX(processed_at)                           AS last_updated
        FROM fraud_transactions
    """)
    return jsonify(serialize([row])[0] if row else {})


@app.route('/api/transactions')
def transactions():
    """
    Latest 20 transactions for live feed.
    """
    rows = query("""
        SELECT
            transaction_id,
            amount,
            merchant,
            merchant_category,
            country,
            fraud_score,
            fraud_label,
            producer_label,
            anomaly_score,
            timestamp,
            processed_at
        FROM fraud_transactions
        ORDER BY processed_at DESC
        LIMIT 20
    """)
    return jsonify(serialize(rows))


@app.route('/api/fraud-rate')
def fraud_rate():
    """
    Fraud rate over time (last 50 batches grouped by minute).
    Used by: line chart.
    """
    rows = query("""
        SELECT
            DATE_TRUNC('minute', processed_at)  AS minute,
            COUNT(*)                             AS total,
            COUNT(*) FILTER (
                WHERE fraud_label = 'FRAUD'
            )                                    AS fraud_count,
            COUNT(*) FILTER (
                WHERE fraud_label = 'SUSPICIOUS'
            )                                    AS suspicious_count,
            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE fraud_label = 'FRAUD'
                ) / NULLIF(COUNT(*), 0), 2
            )                                    AS fraud_pct
        FROM fraud_transactions
        GROUP BY DATE_TRUNC('minute', processed_at)
        ORDER BY minute DESC
        LIMIT 20
    """)
    return jsonify(serialize(rows[::-1]))   # chronological order


@app.route('/api/merchants')
def merchants():
    """
    Top 10 merchants by fraud count.
    Used by: bar chart.
    """
    rows = query("""
        SELECT
            merchant,
            merchant_category,
            COUNT(*)                                AS total,
            COUNT(*) FILTER (
                WHERE fraud_label = 'FRAUD'
            )                                       AS fraud_count,
            COUNT(*) FILTER (
                WHERE fraud_label = 'SUSPICIOUS'
            )                                       AS suspicious_count,
            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE fraud_label = 'FRAUD'
                ) / NULLIF(COUNT(*), 0), 2
            )                                       AS fraud_pct,
            ROUND(AVG(amount)::numeric, 2)          AS avg_amount,
            ROUND(AVG(fraud_score)::numeric, 3)     AS avg_score
        FROM fraud_transactions
        GROUP BY merchant, merchant_category
        ORDER BY fraud_count DESC, fraud_pct DESC
        LIMIT 10
    """)
    return jsonify(serialize(rows))


@app.route('/api/labels')
def labels():
    """
    Label distribution for pie chart.
    """
    rows = query("""
        SELECT
            fraud_label,
            COUNT(*) AS count
        FROM fraud_transactions
        GROUP BY fraud_label
        ORDER BY count DESC
    """)
    return jsonify(serialize(rows))


@app.route('/api/countries')
def countries():
    """
    Fraud count by country.
    Used by: geographic breakdown table.
    """
    rows = query("""
        SELECT
            country,
            COUNT(*)                            AS total,
            COUNT(*) FILTER (
                WHERE fraud_label = 'FRAUD'
            )                                   AS fraud_count,
            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE fraud_label = 'FRAUD'
                ) / NULLIF(COUNT(*), 0), 2
            )                                   AS fraud_pct
        FROM fraud_transactions
        GROUP BY country
        ORDER BY fraud_count DESC
        LIMIT 10
    """)
    return jsonify(serialize(rows))


@app.route('/api/alerts')
def alerts():
    """
    Latest 10 FRAUD transactions for alert panel.
    """
    rows = query("""
        SELECT
            transaction_id,
            amount,
            merchant,
            merchant_category,
            country,
            fraud_score,
            producer_label,
            anomaly_score,
            timestamp
        FROM fraud_transactions
        WHERE fraud_label = 'FRAUD'
        ORDER BY processed_at DESC
        LIMIT 10
    """)
    return jsonify(serialize(rows))


# ── Serve Dashboard ───────────────────────────────────────────────────

@app.route('/')
def dashboard():
    dashboard_path = os.path.join(
        os.path.dirname(__file__), '..', 'dashboard'
    )
    return send_from_directory(dashboard_path, 'index.html')


# ── Health Check ──────────────────────────────────────────────────────

@app.route('/health')
def health():
    try:
        conn = get_db()
        conn.close()
        return jsonify({
            "status": "ok",
            "db": "connected",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "db": str(e)
        }), 500



# ── Pipeline Metrics ──────────────────────────────────────────────────

@app.route('/api/metrics')
def get_pipeline_metrics():
    """Returns last 60 seconds of pipeline throughput and latency metrics."""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                recorded_at,
                batch_size,
                avg_latency_ms
            FROM pipeline_metrics
            WHERE recorded_at >= NOW() - INTERVAL '60 seconds'
            ORDER BY recorded_at DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        total_records = sum(r[1] for r in rows)
        avg_latency = (
            sum(r[2] for r in rows if r[2] is not None) /
            max(len([r for r in rows if r[2] is not None]), 1)
        )

        return jsonify({
            "recent_records_60s": total_records,
            "avg_latency_ms": round(avg_latency, 2),
            "data_points": [
                {
                    "recorded_at": r[0].isoformat(),
                    "batch_size": r[1],
                    "avg_latency_ms": r[2]
                } for r in rows
            ]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Run ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print("=" * 50)
    print("  FRAUD DETECTION - FLASK API")
    print("=" * 50)
    print(f"  Dashboard : http://localhost:{cfg.FLASK_PORT}")
    print(f"  Health    : http://localhost:{cfg.FLASK_PORT}/health")
    print(f"  Stats     : http://localhost:{cfg.FLASK_PORT}/api/stats")
    print("=" * 50)
    app.run(
        host=cfg.FLASK_HOST,
        port=cfg.FLASK_PORT,
        debug=cfg.FLASK_DEBUG
    )