"""
Configuration centralisée du projet Détection de Fraude.
Tous les seuils, chemins et paramètres sont définis ici.
"""
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ──────────────────────────── Chemins fichiers ────────────────────────────
RAW_DATA_PATH = os.path.join(BASE_DIR, "Bank_Transactions.csv")
CLEANED_DATA_PATH = os.path.join(BASE_DIR, "data", "cleaned_transactions.csv")
ENRICHED_DATA_PATH = os.path.join(BASE_DIR, "data", "enriched_transactions.csv")
FRAUD_RESULTS_PATH = os.path.join(BASE_DIR, "data", "fraud_results.csv")
SPARK_OUTPUT_PATH = os.path.join(BASE_DIR, "data", "spark_output")

# ──────────────────────────── Seuils de détection ─────────────────────────
FRAUD_RULES = {
    "high_amount_threshold": 8000,
    "very_high_amount_threshold": 9500,
    "night_start_hour": 0,
    "night_end_hour": 5,
    "high_frequency_window_days": 1,
    "high_frequency_count": 5,
    "zscore_threshold": 1.5,
    "online_high_amount_threshold": 7000,
    "rapid_succession_minutes": 10,
    "rapid_succession_count": 3,
}

RISK_WEIGHTS = {
    "high_amount": 25,
    "very_high_amount": 15,
    "night_transaction": 15,
    "online_high_amount": 20,
    "statistical_anomaly": 15,
    "high_frequency": 10,
}

RISK_LEVELS = {
    "critical": 60,
    "high": 45,
    "medium": 25,
    "low": 0,
}

# ──────────────────────────── BigQuery ────────────────────────────────────
BIGQUERY_PROJECT = os.getenv("GCP_PROJECT_ID", "fraud-detection-project")
BIGQUERY_DATASET = "fraud_detection"
BIGQUERY_TABLE_RAW = "raw_transactions"
BIGQUERY_TABLE_ENRICHED = "enriched_transactions"
BIGQUERY_TABLE_FRAUD = "fraud_alerts"

# ──────────────────────────── Spark ───────────────────────────────────────
SPARK_APP_NAME = "FraudDetectionPipeline"
SPARK_MASTER = "local[*]"
