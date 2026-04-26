# ============================================
# Data Producer — v3 (Production-Grade)
# fraud-detection-pipeline/producer/producer.py
#
# Strategy:
#   1. Stratified sampling (FRAUD_RATE = 5%)
#   2. Contextual behavioral anomalies
#   3. User profiles (home country, avg spend)
#   4. Probabilistic fraud injection
#   5. Burst attack simulation
# ============================================

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import json
import time
import uuid
import random
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
from configs.config import (
    KAFKA_BROKER,
    KAFKA_TOPIC,
    PRODUCER_DELAY_SECONDS
)

# ── Configuration ─────────────────────────────────────────────────────

FRAUD_RATE           = 0.03    # base 5% fraud rate for demo
SUSPICIOUS_RATE      = 0.08    # base 12% suspicious rate
BURST_TRIGGER_PROB   = 0.015    # 2% chance of entering burst mode
BURST_SIZE_RANGE     = (3, 8)  # burst = 3 to 8 consecutive frauds

# ── Data Pools ────────────────────────────────────────────────────────

MERCHANTS = {
    "Electronics":   ["BestBuy", "Apple Store", "Samsung Shop",
                      "Croma", "Reliance Digital"],
    "Grocery":       ["Walmart", "BigBasket", "DMart",
                      "Whole Foods", "Reliance Fresh"],
    "Travel":        ["MakeMyTrip", "Expedia", "AirIndia",
                      "IndiGo Airlines", "Booking.com"],
    "Jewelry":       ["Tanishq", "Tiffany & Co",
                      "Malabar Gold", "CaratLane"],
    "Crypto":        ["Binance", "CoinDCX", "WazirX", "Coinbase"],
    "Wire Transfer": ["Western Union", "MoneyGram",
                      "PayPal Transfer", "SWIFT Transfer"],
    "Gift Cards":    ["Amazon Gift Card", "Google Play",
                      "iTunes Store", "Steam Wallet"],
    "Food & Dining": ["Zomato", "Swiggy", "McDonald's",
                      "Domino's", "Starbucks"],
    "Healthcare":    ["Apollo Pharmacy", "MedPlus", "Practo", "1mg"],
    "Clothing":      ["Myntra", "H&M", "Zara",
                      "Flipkart Fashion", "Ajio"]
}

# Country risk tiers
SAFE_COUNTRIES      = ["India", "USA", "UK", "Canada", "Australia"]
MEDIUM_RISK         = ["China", "Brazil", "Mexico", "Turkey"]
HIGH_RISK_COUNTRIES = ["Nigeria", "Russia", "Unknown", "Belarus"]

ALL_COUNTRIES       = SAFE_COUNTRIES + MEDIUM_RISK + HIGH_RISK_COUNTRIES

# Category risk tiers
SAFE_CATEGORIES     = ["Grocery", "Food & Dining",
                        "Healthcare", "Clothing"]
MEDIUM_CATEGORIES   = ["Travel", "Electronics"]
HIGH_RISK_CATS      = ["Crypto", "Wire Transfer",
                        "Gift Cards", "Jewelry"]

ALL_CATEGORIES      = SAFE_CATEGORIES + MEDIUM_CATEGORIES + HIGH_RISK_CATS

CARD_PREFIXES       = ["4111", "5500", "3714", "6011", "3528"]

# ── User Profile System ───────────────────────────────────────────────

class UserProfileStore:
    """
    Maintains behavioral profiles for each card.
    Simulates a real fraud detection system's
    user behavior baseline.

    Profile tracks:
      - home_country     : where this card normally transacts
      - avg_amount       : typical spend amount
      - usual_categories : categories this card normally uses
      - last_txn_time    : last transaction timestamp
      - txn_count        : total transactions seen
      - recent_amounts   : last 5 amounts (for spike detection)
    """

    def __init__(self):
        self.profiles = {}

    def get_or_create(self, card_number):
        if card_number not in self.profiles:
            home = random.choice(SAFE_COUNTRIES)
            avg  = round(random.uniform(200, 2000), 2)
            cats = random.sample(
                SAFE_CATEGORIES + MEDIUM_CATEGORIES, k=3
            )
            self.profiles[card_number] = {
                "home_country":      home,
                "avg_amount":        avg,
                "usual_categories":  cats,
                "last_txn_time":     datetime.now(),
                "txn_count":         0,
                "recent_amounts":    []
            }
        return self.profiles[card_number]

    def update(self, card_number, amount, country, category):
        profile = self.get_or_create(card_number)
        profile["last_txn_time"] = datetime.now()
        profile["txn_count"]    += 1

        # Rolling average of last 5 amounts
        profile["recent_amounts"].append(amount)
        if len(profile["recent_amounts"]) > 5:
            profile["recent_amounts"].pop(0)

        # Slowly update avg_amount (exponential moving average)
        profile["avg_amount"] = round(
            0.8 * profile["avg_amount"] + 0.2 * amount, 2
        )

    def get_anomaly_score(self, card_number, amount,
                          country, category):
        """
        Returns anomaly score 0.0 → 1.0 based on
        deviation from user's normal behavior.

        High score = this transaction is unusual for this card.
        """
        profile = self.get_or_create(card_number)
        score   = 0.0

        # 1. Amount anomaly
        avg = profile["avg_amount"]
        if avg > 0:
            ratio = amount / avg
            if ratio > 10:
                score += 0.45    # 10x normal spend → very suspicious
            elif ratio > 5:
                score += 0.30    # 5x normal spend → suspicious
            elif ratio > 3:
                score += 0.15    # 3x normal spend → slightly off

        # 2. Geographic anomaly
        if country != profile["home_country"]:
            if country in HIGH_RISK_COUNTRIES:
                score += 0.35    # home=India, txn=Nigeria → very risky
            elif country in MEDIUM_RISK:
                score += 0.15    # home=India, txn=Brazil → moderate
            else:
                score += 0.05    # home=India, txn=USA → minor

        # 3. Velocity anomaly (time since last transaction)
        time_since = (
            datetime.now() - profile["last_txn_time"]
        ).total_seconds()

        if time_since < 30 and profile["txn_count"] > 0:
            score += 0.20        # < 30 sec since last txn → velocity

        # 4. Category anomaly
        if category not in profile["usual_categories"]:
            if category in HIGH_RISK_CATS:
                score += 0.25    # card never used crypto before
            elif category in MEDIUM_CATEGORIES:
                score += 0.05

        return min(round(score, 3), 1.0)    # cap at 1.0


# ── Probabilistic Fraud Probability ──────────────────────────────────

def compute_fraud_probability(amount, country, category,
                               anomaly_score, in_burst):
    """
    Dynamically computes fraud probability based on
    transaction context.

    Base rate + contextual boosters = final probability.
    This is how real fraud scoring engines work.
    """
    prob = FRAUD_RATE    # start at base 5%

    # Amount booster
    if amount > 8000:
        prob += 0.15
    elif amount > 5000:
        prob += 0.08
    elif amount > 2000:
        prob += 0.03

    # Country booster
    if country in HIGH_RISK_COUNTRIES:
        prob += 0.12
    elif country in MEDIUM_RISK:
        prob += 0.04

    # Category booster
    if category in HIGH_RISK_CATS:
        prob += 0.10
    elif category in MEDIUM_CATEGORIES:
        prob += 0.02

    # Anomaly score booster (behavioral deviation)
    prob += anomaly_score * 0.20

    # Burst mode booster
    if in_burst:
        prob += 0.35

    return min(prob, 0.95)    # cap at 95%


def compute_suspicious_probability(amount, country,
                                    category, anomaly_score):
    """
    Computes suspicious probability for non-fraud transactions.
    """
    prob = SUSPICIOUS_RATE

    if amount > 3000:
        prob += 0.10
    elif amount > 1500:
        prob += 0.05

    if country in MEDIUM_RISK:
        prob += 0.08
    elif country in HIGH_RISK_COUNTRIES:
        prob += 0.05    # lower here — already likely fraud

    if category in ["Jewelry", "Gift Cards"]:
        prob += 0.06

    prob += anomaly_score * 0.15

    return min(prob, 0.70)


# ── Transaction Builders ──────────────────────────────────────────────

def generate_card_number():
    prefix = random.choice(CARD_PREFIXES)
    last4  = str(random.randint(1000, 9999))
    return f"****-****-{prefix}-{last4}"


def build_transaction(row, index, card_number,
                       profile_store, label_override=None):
    """
    Core transaction builder.
    Uses user profile to generate contextually
    realistic transactions.

    label_override:
      None       → compute probabilistically
      "FRAUD"    → force fraud (burst mode / dataset fraud row)
      "LEGIT"    → force legit
    """

    profile      = profile_store.get_or_create(card_number)
    home_country = profile["home_country"]
    avg_amount   = profile["avg_amount"]

    if label_override == "FRAUD":
        # ── Fraud transaction ──────────────────────────────
        # Strongly deviates from profile

        category = random.choice(HIGH_RISK_CATS)
        merchant = random.choice(MERCHANTS[category])

        # Amount = large spike over user's normal spend
        amount = round(
            random.uniform(
                max(avg_amount * 4, 3000),
                max(avg_amount * 12, 12000)
            ), 2
        )

        # Country = far from home (geographic anomaly)
        foreign_pool = [
            c for c in HIGH_RISK_COUNTRIES + MEDIUM_RISK
            if c != home_country
        ]
        country = random.choice(foreign_pool)

        producer_label = "FRAUD"

    elif label_override == "LEGIT":
        # ── Legit transaction ──────────────────────────────
        # Stays close to profile

        category = random.choice(
            profile["usual_categories"] + SAFE_CATEGORIES
        )
        merchant = random.choice(MERCHANTS[category])

        # Amount = near user's average (with small variance)
        amount = round(
            abs(random.gauss(avg_amount, avg_amount * 0.3)), 2
        )
        amount = max(10.0, min(amount, avg_amount * 2))

        # Country = mostly home country
        country = (
            home_country if random.random() < 0.85
            else random.choice(SAFE_COUNTRIES)
        )

        producer_label = "LEGIT"

    else:
        # ── Probabilistic path ─────────────────────────────
        # Pick random context first,
        # then decide label based on probabilities

        category = random.choice(ALL_CATEGORIES)
        merchant = random.choice(MERCHANTS[category])
        country  = random.choice(ALL_COUNTRIES)
        amount   = round(
            abs(random.gauss(avg_amount, avg_amount * 0.5)), 2
        )
        amount   = max(5.0, amount)

        # Compute anomaly score for this card
        anomaly = profile_store.get_anomaly_score(
            card_number, amount, country, category
        )

        # Compute fraud probability
        fraud_prob = compute_fraud_probability(
            amount, country, category, anomaly,
            in_burst=False
        )
        suspicious_prob = compute_suspicious_probability(
            amount, country, category, anomaly
        )

        r = random.random()
        if r < fraud_prob:
            producer_label = "FRAUD"
            # Adjust to look more fraudulent
            if category not in HIGH_RISK_CATS:
                category = random.choice(HIGH_RISK_CATS)
                merchant = random.choice(MERCHANTS[category])
            if country not in HIGH_RISK_COUNTRIES + MEDIUM_RISK:
                country = random.choice(
                    HIGH_RISK_COUNTRIES + MEDIUM_RISK
                )
        elif r < fraud_prob + suspicious_prob:
            producer_label = "SUSPICIOUS"
        else:
            producer_label = "LEGIT"

    # Compute final anomaly score for the transaction
    anomaly_score = profile_store.get_anomaly_score(
        card_number, amount, country, category
    )

    # Update profile after building transaction
    profile_store.update(card_number, amount, country, category)

    return {
        "transaction_id":    str(uuid.uuid4()),
        "amount":            round(amount, 2),
        "merchant":          merchant,
        "merchant_category": category,
        "card_number":       card_number,
        "country":           country,
        "home_country":      home_country,
        "timestamp":         datetime.now().isoformat(),
        "v1":                round(float(row['V1']), 6),
        "v2":                round(float(row['V2']), 6),
        "v3":                round(float(row['V3']), 6),
        "actual_class":      int(row['Class']),
        "anomaly_score":     anomaly_score,
        "producer_label":    producer_label,
        "sequence_index":    index
    }


# ── Kafka Producer ────────────────────────────────────────────────────

def create_kafka_producer():
    retries = 5
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: (
                    json.dumps(v).encode('utf-8')
                ),
                key_serializer=lambda k: k.encode('utf-8'),
                acks='all',
                retries=3,
                max_block_ms=10000
            )
            print(f"✅ Connected to Kafka @ {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"⚠️  Attempt {attempt+1}/{retries}: {e}")
            time.sleep(3)
    raise Exception("❌ Kafka connection failed")


def load_dataset():
    path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'creditcard.csv'
    )
    print(f"📂 Loading: {path}")
    df = pd.read_csv(path)
    print(f"✅ {len(df):,} transactions loaded")

    fraud_df = df[df['Class'] == 1].reset_index(drop=True)
    legit_df = df[df['Class'] == 0].reset_index(drop=True)

    print(f"   🔴 Fraud rows : {len(fraud_df):,}")
    print(f"   🟢 Legit rows : {len(legit_df):,}")
    return fraud_df, legit_df


# ── Burst Mode Manager ────────────────────────────────────────────────

class BurstManager:
    """
    Simulates real-world fraud attack bursts.
    When burst_mode is active, the next N transactions
    are all forced FRAUD — simulating a stolen card
    being used rapidly before the victim notices.
    """
    def __init__(self):
        self.active          = False
        self.remaining       = 0
        self.burst_card      = None
        self.total_bursts    = 0

    def check_and_trigger(self, card_number):
        """Maybe start a new burst"""
        if not self.active:
            if random.random() < BURST_TRIGGER_PROB:
                self.active       = True
                self.remaining    = random.randint(*BURST_SIZE_RANGE)
                self.burst_card   = card_number
                self.total_bursts += 1
                print(
                    f"\n  ⚡ BURST ATTACK STARTED on "
                    f"{card_number} "
                    f"({self.remaining} transactions)\n"
                )

    def consume(self):
        """Use one burst transaction. Returns True if in burst."""
        if self.active and self.remaining > 0:
            self.remaining -= 1
            if self.remaining == 0:
                self.active = False
                print(f"\n  ✅ Burst attack ended\n")
            return True
        return False

    @property
    def card(self):
        return self.burst_card


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  🚨 REAL-TIME FRAUD DETECTION — PRODUCER v3")
    print("  Strategy: Stratified + Profiling + Probabilistic + Burst")
    print("=" * 62)

    # Load dataset — stratified pools
    fraud_df, legit_df = load_dataset()

    # Create components
    producer      = create_kafka_producer()
    profile_store = UserProfileStore()
    burst_mgr     = BurstManager()

    # Pre-generate a pool of card numbers
    # (realistic: limited set of users, not every row = new card)
    CARD_POOL = [generate_card_number() for _ in range(50)]

    print(f"\n📤 Topic    : {KAFKA_TOPIC}")
    print(f"⏱️  Rate     : 1 txn / {PRODUCER_DELAY_SECONDS}s")
    print(f"🎲 Fraud    : ~{FRAUD_RATE*100:.0f}% base "
          f"(probabilistic, context-boosted)")
    print(f"⚡ Bursts   : ~{BURST_TRIGGER_PROB*100:.0f}% chance per txn")
    print(f"👤 Cards    : {len(CARD_POOL)} unique card profiles")
    print("\nPress Ctrl+C to stop.\n")
    print("-" * 62)
    print(f"{'#':>6}  {'LABEL':<12} {'AMOUNT':>10}  "
          f"{'MERCHANT':<20} {'COUNTRY':<10}  "
          f"{'ANOM':>5}  TIME")
    print("-" * 62)

    counts  = {"FRAUD": 0, "SUSPICIOUS": 0, "LEGIT": 0, "TOTAL": 0}
    index   = 0

    try:
        while True:    # infinite stream (re-samples dataset)

            # Pick a card from the pool
            card_number = random.choice(CARD_POOL)

            # Check if burst mode should trigger
            burst_mgr.check_and_trigger(card_number)

            # ── Decide sampling strategy ──────────────────────
            if burst_mgr.consume():
                # BURST MODE: force fraud on burst card
                row            = fraud_df.sample(1).iloc[0]
                label_override = "FRAUD"
                card_number    = burst_mgr.card

            elif random.random() < FRAUD_RATE:
                # STRATIFIED: sample from actual fraud rows
                row            = fraud_df.sample(1).iloc[0]
                label_override = "FRAUD"

            else:
                # NORMAL: sample from legit rows
                # let probabilistic logic decide final label
                row            = legit_df.sample(1).iloc[0]
                label_override = None    # probabilistic path

            # Build transaction
            txn = build_transaction(
                row, index, card_number,
                profile_store, label_override
            )

            # Send to Kafka
            producer.send(
                KAFKA_TOPIC,
                key=txn['card_number'],
                value=txn
            )

            # Update counters
            label = txn['producer_label']
            counts[label]   += 1
            counts["TOTAL"] += 1
            index           += 1

            # Console display
            icon = {"FRAUD": "🔴", "SUSPICIOUS": "🟡",
                    "LEGIT": "🟢"}[label]

            print(
                f"[{counts['TOTAL']:>6}]  "
                f"{icon} {label:<12} "
                f"${txn['amount']:>9.2f}  "
                f"{txn['merchant']:<20} "
                f"{txn['country']:<10}  "
                f"{txn['anomaly_score']:>5.3f}  "
                f"{txn['timestamp'][11:19]}"
            )

            # Stats every 25 messages
            if counts["TOTAL"] % 25 == 0:
                producer.flush()
                total = counts["TOTAL"]
                print(f"\n  {'─'*55}")
                print(f"  📊 LIVE STATS @ {total} transactions")
                print(f"  {'─'*55}")
                print(f"  🔴 FRAUD      : "
                      f"{counts['FRAUD']:>4} "
                      f"({counts['FRAUD']/total*100:5.1f}%)")
                print(f"  🟡 SUSPICIOUS : "
                      f"{counts['SUSPICIOUS']:>4} "
                      f"({counts['SUSPICIOUS']/total*100:5.1f}%)")
                print(f"  🟢 LEGIT      : "
                      f"{counts['LEGIT']:>4} "
                      f"({counts['LEGIT']/total*100:5.1f}%)")
                print(f"  ⚡ Bursts     : "
                      f"{burst_mgr.total_bursts}")
                print(f"  {'─'*55}\n")

            time.sleep(PRODUCER_DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\n\n⏹️  Producer stopped.")

    finally:
        producer.flush()
        producer.close()
        total = counts["TOTAL"]
        if total > 0:
            print(f"\n{'='*62}")
            print(f"  ✅ FINAL SUMMARY — {total} transactions")
            print(f"{'='*62}")
            print(f"  🔴 FRAUD      : "
                  f"{counts['FRAUD']:>5} "
                  f"({counts['FRAUD']/total*100:.1f}%)")
            print(f"  🟡 SUSPICIOUS : "
                  f"{counts['SUSPICIOUS']:>5} "
                  f"({counts['SUSPICIOUS']/total*100:.1f}%)")
            print(f"  🟢 LEGIT      : "
                  f"{counts['LEGIT']:>5} "
                  f"({counts['LEGIT']/total*100:.1f}%)")
            print(f"  ⚡ Bursts     : {burst_mgr.total_bursts}")


if __name__ == "__main__":
    main()