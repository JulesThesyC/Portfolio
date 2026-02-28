"""
Module d'intégration BigQuery.
Gestion du schéma, chargement des données et requêtes analytiques
pour le stockage et l'analyse des transactions bancaires.
"""
import os
import sys
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    BIGQUERY_PROJECT, BIGQUERY_DATASET,
    BIGQUERY_TABLE_RAW, BIGQUERY_TABLE_ENRICHED, BIGQUERY_TABLE_FRAUD,
    FRAUD_RESULTS_PATH, ENRICHED_DATA_PATH,
)

try:
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    BQ_AVAILABLE = True
except ImportError:
    BQ_AVAILABLE = False
    print("[BQ] google-cloud-bigquery non installe - mode simulation active")


# ═══════════════════════════════════════════════════════════════════════════
#  SCHÉMAS BIGQUERY
# ═══════════════════════════════════════════════════════════════════════════

RAW_SCHEMA = [
    {"name": "User_ID", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Transaction_Time", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "Amount", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "Transaction_Type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Location", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Status", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Is_Fraud", "type": "STRING", "mode": "REQUIRED"},
]

FRAUD_RESULTS_SCHEMA = [
    {"name": "User_ID", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Transaction_Time", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "Amount", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "Transaction_Type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Location", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Status", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Is_Fraud", "type": "STRING", "mode": "REQUIRED"},
    {"name": "risk_score", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "risk_level", "type": "STRING", "mode": "NULLABLE"},
    {"name": "is_suspected_fraud", "type": "INT64", "mode": "NULLABLE"},
    {"name": "flag_high_amount", "type": "INT64", "mode": "NULLABLE"},
    {"name": "flag_very_high_amount", "type": "INT64", "mode": "NULLABLE"},
    {"name": "flag_night", "type": "INT64", "mode": "NULLABLE"},
    {"name": "flag_online_high", "type": "INT64", "mode": "NULLABLE"},
    {"name": "flag_anomaly", "type": "INT64", "mode": "NULLABLE"},
    {"name": "flag_high_freq", "type": "INT64", "mode": "NULLABLE"},
]


def _schema_to_bq(schema_list):
    """Convertit la liste de dicts en objets SchemaField BigQuery."""
    if not BQ_AVAILABLE:
        return schema_list
    return [
        bigquery.SchemaField(f["name"], f["type"], mode=f.get("mode", "NULLABLE"))
        for f in schema_list
    ]


# ═══════════════════════════════════════════════════════════════════════════
#  CLIENT & DATASET
# ═══════════════════════════════════════════════════════════════════════════

def get_client():
    if not BQ_AVAILABLE:
        raise RuntimeError("google-cloud-bigquery non disponible")
    return bigquery.Client(project=BIGQUERY_PROJECT)


def ensure_dataset(client):
    dataset_ref = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}"
    try:
        client.get_dataset(dataset_ref)
        print(f"[BQ] Dataset {dataset_ref} existe deja")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        client.create_dataset(dataset, exists_ok=True)
        print(f"[BQ] Dataset {dataset_ref} cree")


# ═══════════════════════════════════════════════════════════════════════════
#  CHARGEMENT DES DONNÉES
# ═══════════════════════════════════════════════════════════════════════════

def load_to_bigquery(
    df: pd.DataFrame,
    table_name: str,
    schema_list: list,
    write_disposition: str = "WRITE_TRUNCATE",
) -> None:
    """Charge un DataFrame pandas dans une table BigQuery."""
    if not BQ_AVAILABLE:
        print(f"[BQ-SIM] Simulation : {len(df)} lignes -> {table_name}")
        print(f"[BQ-SIM] Colonnes : {list(df.columns[:8])}...")
        return

    client = get_client()
    ensure_dataset(client)
    table_ref = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=_schema_to_bq(schema_list),
        write_disposition=write_disposition,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"[BQ] {job.output_rows} lignes chargees -> {table_ref}")


def load_raw_transactions(csv_path: str = None) -> None:
    """Charge les transactions brutes dans BigQuery."""
    from config.settings import RAW_DATA_PATH
    path = csv_path or RAW_DATA_PATH
    df = pd.read_csv(path, parse_dates=["Transaction_Time"])
    load_to_bigquery(df, BIGQUERY_TABLE_RAW, RAW_SCHEMA)


def load_fraud_results(csv_path: str = FRAUD_RESULTS_PATH) -> None:
    """Charge les résultats de détection dans BigQuery."""
    df = pd.read_csv(csv_path, parse_dates=["Transaction_Time"])
    cols = [f["name"] for f in FRAUD_RESULTS_SCHEMA]
    available = [c for c in cols if c in df.columns]
    load_to_bigquery(df[available], BIGQUERY_TABLE_FRAUD, FRAUD_RESULTS_SCHEMA)


# ═══════════════════════════════════════════════════════════════════════════
#  REQUÊTES ANALYTIQUES
# ═══════════════════════════════════════════════════════════════════════════

ANALYTICAL_QUERIES = {
    "fraud_summary": f"""
        SELECT
            risk_level,
            COUNT(*) AS total_transactions,
            ROUND(AVG(Amount), 2) AS avg_amount,
            ROUND(SUM(Amount), 2) AS total_amount,
            SUM(CASE WHEN Is_Fraud = 'YES' THEN 1 ELSE 0 END) AS actual_frauds
        FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_FRAUD}`
        GROUP BY risk_level
        ORDER BY total_transactions DESC
    """,

    "top_suspicious_users": f"""
        SELECT
            User_ID,
            COUNT(*) AS suspicious_count,
            ROUND(AVG(risk_score), 1) AS avg_risk_score,
            ROUND(SUM(Amount), 2) AS total_suspicious_amount,
            ARRAY_AGG(DISTINCT Location) AS locations
        FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_FRAUD}`
        WHERE is_suspected_fraud = 1
        GROUP BY User_ID
        ORDER BY avg_risk_score DESC
        LIMIT 20
    """,

    "hourly_fraud_pattern": f"""
        SELECT
            EXTRACT(HOUR FROM Transaction_Time) AS hour,
            COUNT(*) AS total_tx,
            SUM(is_suspected_fraud) AS suspicious_tx,
            ROUND(SUM(is_suspected_fraud) / COUNT(*) * 100, 2) AS fraud_pct
        FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_FRAUD}`
        GROUP BY hour
        ORDER BY hour
    """,

    "monthly_trend": f"""
        SELECT
            FORMAT_TIMESTAMP('%Y-%m', Transaction_Time) AS month,
            COUNT(*) AS total_tx,
            SUM(is_suspected_fraud) AS suspicious_tx,
            ROUND(AVG(risk_score), 2) AS avg_risk_score
        FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_FRAUD}`
        GROUP BY month
        ORDER BY month
    """,

    "fraud_by_type_location": f"""
        SELECT
            Transaction_Type,
            Location,
            COUNT(*) AS total,
            SUM(is_suspected_fraud) AS suspicious,
            ROUND(AVG(Amount), 2) AS avg_amount
        FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_FRAUD}`
        GROUP BY Transaction_Type, Location
        ORDER BY suspicious DESC
    """,
}


def run_query(query_name: str) -> pd.DataFrame:
    """Exécute une requête analytique prédéfinie."""
    if query_name not in ANALYTICAL_QUERIES:
        raise ValueError(f"Requête inconnue: {query_name}. Disponibles: {list(ANALYTICAL_QUERIES.keys())}")

    if not BQ_AVAILABLE:
        print(f"[BQ-SIM] Requete '{query_name}' (mode simulation - utiliser les CSV locaux)")
        return pd.DataFrame()

    client = get_client()
    query = ANALYTICAL_QUERIES[query_name]
    print(f"[BQ] Execution requete : {query_name}")
    return client.query(query).to_dataframe()


def run_custom_query(sql: str) -> pd.DataFrame:
    """Exécute une requête SQL personnalisée."""
    if not BQ_AVAILABLE:
        print("[BQ-SIM] Requete custom (mode simulation)")
        return pd.DataFrame()

    client = get_client()
    return client.query(sql).to_dataframe()


# ═══════════════════════════════════════════════════════════════════════════
#  PIPELINE BIGQUERY
# ═══════════════════════════════════════════════════════════════════════════

def run_bigquery_pipeline() -> None:
    """Exécute le pipeline complet BigQuery."""
    print("=" * 60)
    print("  PIPELINE BIGQUERY")
    print("=" * 60)

    load_raw_transactions()
    load_fraud_results()

    if BQ_AVAILABLE:
        for name in ANALYTICAL_QUERIES:
            result = run_query(name)
            print(f"\n[BQ] {name} - {len(result)} lignes")
            if not result.empty:
                print(result.head(10).to_string(index=False))

    print("=" * 60)


if __name__ == "__main__":
    run_bigquery_pipeline()
