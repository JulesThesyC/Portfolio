"""
Apache Airflow DAG - Pipeline IoT Environnemental
-------------------------------------------------
Orchestre l'ETL quotidien des capteurs environnementaux.
"""

from datetime import datetime, timedelta
from pathlib import Path

# À placer dans AIRFLOW_HOME/dags/ (ex: ~/airflow/dags/)
# ou copier ce fichier dans le répertoire DAGs d'Airflow

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Ajuster le chemin selon votre installation
PROJECT_ROOT = Path(__file__).parent.parent


def run_etl():
    """Exécute le pipeline ETL."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from etl.pipeline import run_pipeline
    result = run_pipeline()
    print(result)
    return result


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="iot_environmental_etl",
    default_args=default_args,
    description="ETL quotidien des capteurs environnementaux IoT",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["iot", "environmental", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")
    extract_transform_load = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_etl,
    )
    end = EmptyOperator(task_id="end")

    start >> extract_transform_load >> end
