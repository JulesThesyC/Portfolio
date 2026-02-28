"""
DAG Apache Airflow — Pipeline de Détection de Fraude Bancaire.

Orchestre les 5 étapes du pipeline :
  1. Prétraitement des données (nettoyage + feature engineering)
  2. Détection de fraude par heuristiques
  3. Traitement PySpark (pipeline distribué)
  4. Chargement dans BigQuery
  5. Contrôle qualité et notification

Planification : toutes les heures (adaptable à du quasi-temps réel).
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


# ─────────────────────────── Configuration DAG ────────────────────────────

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email": ["alerts@fraud-detection.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

dag = DAG(
    dag_id="fraud_detection_pipeline",
    default_args=default_args,
    description="Pipeline de détection de fraude dans les transactions bancaires",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["fraud", "banking", "data-engineering"],
    max_active_runs=1,
)


# ─────────────────────────── Callables ────────────────────────────────────

def _preprocess(**kwargs):
    """Étape 1 : Nettoyage et feature engineering."""
    import sys, os
    sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", ""), "project"))
    from src.data_preprocessing import run_preprocessing_pipeline

    df = run_preprocessing_pipeline()
    kwargs["ti"].xcom_push(key="row_count", value=len(df))
    kwargs["ti"].xcom_push(key="fraud_count", value=int((df["Is_Fraud"] == "YES").sum()))
    return f"Preprocessing terminé : {len(df)} lignes"


def _detect_fraud(**kwargs):
    """Étape 2 : Détection de fraude par heuristiques."""
    import sys, os
    sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", ""), "project"))
    from src.fraud_detection import run_fraud_detection

    df = run_fraud_detection()
    suspected = int((df["is_suspected_fraud"] == 1).sum())
    kwargs["ti"].xcom_push(key="suspected_count", value=suspected)
    return f"Détection terminée : {suspected} transactions suspectes"


def _spark_processing(**kwargs):
    """Étape 3 : Traitement PySpark distribué."""
    import sys, os
    sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", ""), "project"))
    from src.spark_processing import run_spark_pipeline

    df = run_spark_pipeline()
    return "Pipeline Spark terminé"


def _load_bigquery(**kwargs):
    """Étape 4 : Chargement dans BigQuery."""
    import sys, os
    sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", ""), "project"))
    from src.bigquery_utils import run_bigquery_pipeline

    run_bigquery_pipeline()
    return "Chargement BigQuery terminé"


def _quality_check(**kwargs):
    """Étape 5 : Contrôle qualité des résultats."""
    ti = kwargs["ti"]
    row_count = ti.xcom_pull(task_ids="preprocess_data", key="row_count")
    suspected = ti.xcom_pull(task_ids="detect_fraud", key="suspected_count")

    checks = {
        "data_not_empty": row_count is not None and row_count > 0,
        "detection_ran": suspected is not None,
        "anomaly_rate_reasonable": suspected is not None and (suspected / max(row_count, 1)) < 0.5,
    }

    passed = all(checks.values())
    for name, result in checks.items():
        status = "PASS" if result else "FAIL"
        print(f"  [{status}] {name}")

    if not passed:
        raise ValueError("Contrôle qualité échoué — voir les détails ci-dessus")

    return "Tous les contrôles qualité passés"


def _choose_notification(**kwargs):
    """Branche : alerte critique si taux de suspicion élevé."""
    ti = kwargs["ti"]
    suspected = ti.xcom_pull(task_ids="detect_fraud", key="suspected_count") or 0
    row_count = ti.xcom_pull(task_ids="preprocess_data", key="row_count") or 1

    rate = suspected / max(row_count, 1)
    if rate > 0.1:
        return "send_critical_alert"
    return "send_normal_report"


def _send_critical_alert(**kwargs):
    """Notification : alerte critique."""
    ti = kwargs["ti"]
    suspected = ti.xcom_pull(task_ids="detect_fraud", key="suspected_count")
    print(f"[ALERTE CRITIQUE] {suspected} transactions suspectes détectées !")
    print("→ Notification envoyée aux équipes sécurité et conformité")


def _send_normal_report(**kwargs):
    """Notification : rapport standard."""
    ti = kwargs["ti"]
    suspected = ti.xcom_pull(task_ids="detect_fraud", key="suspected_count")
    print(f"[RAPPORT] Pipeline terminé — {suspected} transactions suspectes")


# ─────────────────────────── Tâches ───────────────────────────────────────

start = EmptyOperator(task_id="start", dag=dag)

preprocess = PythonOperator(
    task_id="preprocess_data",
    python_callable=_preprocess,
    dag=dag,
)

detect = PythonOperator(
    task_id="detect_fraud",
    python_callable=_detect_fraud,
    dag=dag,
)

spark = PythonOperator(
    task_id="spark_processing",
    python_callable=_spark_processing,
    dag=dag,
)

load_bq = PythonOperator(
    task_id="load_bigquery",
    python_callable=_load_bigquery,
    dag=dag,
)

quality = PythonOperator(
    task_id="quality_check",
    python_callable=_quality_check,
    dag=dag,
)

branch = BranchPythonOperator(
    task_id="choose_notification",
    python_callable=_choose_notification,
    dag=dag,
)

critical_alert = PythonOperator(
    task_id="send_critical_alert",
    python_callable=_send_critical_alert,
    dag=dag,
)

normal_report = PythonOperator(
    task_id="send_normal_report",
    python_callable=_send_normal_report,
    dag=dag,
)

end = EmptyOperator(
    task_id="end",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# ─────────────────────────── Dépendances ──────────────────────────────────
#
#  start → preprocess → detect ──→ spark ──→ load_bq → quality → branch
#                                                                  ├→ critical_alert → end
#                                                                  └→ normal_report  → end

start >> preprocess >> detect >> spark >> load_bq >> quality >> branch
branch >> [critical_alert, normal_report] >> end
