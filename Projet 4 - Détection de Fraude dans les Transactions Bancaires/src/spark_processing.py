"""
Pipeline PySpark pour le traitement distribué des transactions bancaires.
Reproduit la logique de preprocessing et de détection de fraude
dans un environnement Spark, adapté au traitement à grande échelle.
"""
import os
import sys

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    RAW_DATA_PATH, SPARK_OUTPUT_PATH,
    SPARK_APP_NAME, SPARK_MASTER, FRAUD_RULES, RISK_WEIGHTS, RISK_LEVELS,
)


# ═══════════════════════════════════════════════════════════════════════════
#  SESSION SPARK
# ═══════════════════════════════════════════════════════════════════════════

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


SCHEMA = StructType([
    StructField("User_ID", StringType(), True),
    StructField("Transaction_Time", StringType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("Transaction_Type", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Is_Fraud", StringType(), True),
])


# ═══════════════════════════════════════════════════════════════════════════
#  CHARGEMENT & NETTOYAGE
# ═══════════════════════════════════════════════════════════════════════════

def load_data(spark: SparkSession, path: str = RAW_DATA_PATH) -> DataFrame:
    df = spark.read.csv(path, header=True, schema=SCHEMA)
    print(f"[SPARK-LOAD] {df.count()} lignes chargees")
    return df


def clean_data(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates()
    df = df.withColumn("Transaction_Time", F.to_timestamp("Transaction_Time"))
    df = df.filter(F.col("Transaction_Time").isNotNull())
    df = df.filter(F.col("Amount") > 0)

    for col_name in ["Transaction_Type", "Location", "Status", "Is_Fraud"]:
        df = df.withColumn(col_name, F.upper(F.trim(F.col(col_name))))

    df = df.filter(F.col("Transaction_Type").isin("DEPOSIT", "WITHDRAWAL", "TRANSFER"))
    df = df.filter(F.col("Location").isin("ONLINE", "ATM", "BRANCH"))
    df = df.filter(F.col("Status").isin("COMPLETED", "PENDING", "FAILED"))

    print(f"[SPARK-CLEAN] {df.count()} lignes apres nettoyage")
    return df


# ═══════════════════════════════════════════════════════════════════════════
#  FEATURE ENGINEERING
# ═══════════════════════════════════════════════════════════════════════════

def engineer_features(df: DataFrame) -> DataFrame:
    # Features temporelles
    df = (
        df
        .withColumn("Hour", F.hour("Transaction_Time"))
        .withColumn("DayOfWeek", F.dayofweek("Transaction_Time"))
        .withColumn("Month", F.month("Transaction_Time"))
        .withColumn("Date", F.to_date("Transaction_Time"))
        .withColumn("Is_Weekend", F.when(F.dayofweek("Transaction_Time").isin(1, 7), 1).otherwise(0))
        .withColumn("Is_Night", F.when(F.hour("Transaction_Time").between(0, 5), 1).otherwise(0))
    )

    # Catégorisation du montant
    df = df.withColumn(
        "Amount_Bin",
        F.when(F.col("Amount") < 100, "Micro")
        .when(F.col("Amount") < 500, "Petit")
        .when(F.col("Amount") < 2000, "Moyen")
        .when(F.col("Amount") < 5000, "Élevé")
        .when(F.col("Amount") < 8000, "Très_Élevé")
        .otherwise("Extrême"),
    )

    # Statistiques par utilisateur
    user_window = Window.partitionBy("User_ID")
    df = (
        df
        .withColumn("user_tx_count", F.count("*").over(user_window))
        .withColumn("user_mean_amount", F.mean("Amount").over(user_window))
        .withColumn("user_std_amount", F.stddev("Amount").over(user_window))
        .withColumn("user_max_amount", F.max("Amount").over(user_window))
    )
    df = df.withColumn(
        "user_std_amount",
        F.coalesce(F.col("user_std_amount"), F.lit(0.0)),
    )

    # Z-scores
    df = df.withColumn(
        "amount_zscore_user",
        F.when(
            F.col("user_std_amount") > 0,
            (F.col("Amount") - F.col("user_mean_amount")) / F.col("user_std_amount"),
        ).otherwise(0.0),
    )

    global_mean = df.select(F.mean("Amount")).first()[0]
    global_std = df.select(F.stddev("Amount")).first()[0]
    df = df.withColumn(
        "amount_zscore_global",
        (F.col("Amount") - F.lit(global_mean)) / F.lit(global_std),
    )

    # Comptage journalier par utilisateur
    daily_window = Window.partitionBy("User_ID", "Date")
    df = df.withColumn("daily_tx_count", F.count("*").over(daily_window))

    print(f"[SPARK-FEATURES] {len(df.columns)} colonnes apres feature engineering")
    return df


# ═══════════════════════════════════════════════════════════════════════════
#  DÉTECTION DE FRAUDE
# ═══════════════════════════════════════════════════════════════════════════

def detect_fraud(df: DataFrame) -> DataFrame:
    rules = FRAUD_RULES
    weights = RISK_WEIGHTS

    df = (
        df
        .withColumn(
            "flag_high_amount",
            F.when(F.col("Amount") >= rules["high_amount_threshold"], 1).otherwise(0),
        )
        .withColumn(
            "flag_very_high_amount",
            F.when(F.col("Amount") >= rules["very_high_amount_threshold"], 1).otherwise(0),
        )
        .withColumn("flag_night", F.col("Is_Night"))
        .withColumn(
            "flag_online_high",
            F.when(
                (F.col("Location") == "ONLINE")
                & (F.col("Amount") >= rules["online_high_amount_threshold"]),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "flag_anomaly",
            F.when(F.abs(F.col("amount_zscore_global")) >= rules["zscore_threshold"], 1).otherwise(0),
        )
        .withColumn(
            "flag_high_freq",
            F.when(F.col("daily_tx_count") >= rules["high_frequency_count"], 1).otherwise(0),
        )
    )

    # Score de risque pondéré
    df = df.withColumn(
        "risk_score",
        (
            F.col("flag_high_amount") * weights["high_amount"]
            + F.col("flag_very_high_amount") * weights["very_high_amount"]
            + F.col("flag_night") * weights["night_transaction"]
            + F.col("flag_online_high") * weights["online_high_amount"]
            + F.col("flag_anomaly") * weights["statistical_anomaly"]
            + F.col("flag_high_freq") * weights["high_frequency"]
        ),
    )
    df = df.withColumn("risk_score", F.least(F.col("risk_score"), F.lit(100)))

    # Niveau de risque
    df = df.withColumn(
        "risk_level",
        F.when(F.col("risk_score") >= RISK_LEVELS["critical"], "CRITICAL")
        .when(F.col("risk_score") >= RISK_LEVELS["high"], "HIGH")
        .when(F.col("risk_score") >= RISK_LEVELS["medium"], "MEDIUM")
        .otherwise("LOW"),
    )

    df = df.withColumn(
        "is_suspected_fraud",
        F.when(F.col("risk_score") >= RISK_LEVELS["high"], 1).otherwise(0),
    )

    return df


def print_spark_report(df: DataFrame) -> None:
    total = df.count()
    suspected = df.filter(F.col("is_suspected_fraud") == 1).count()
    actual_fraud = df.filter(F.col("Is_Fraud") == "YES").count()

    print("\n" + "=" * 60)
    print("  RAPPORT SPARK - DETECTION DE FRAUDE")
    print("=" * 60)
    print(f"  Total transactions     : {total:,}")
    print(f"  Fraudes etiquetees     : {actual_fraud:,}")
    print(f"  Transactions suspectes : {suspected:,}")
    print("-" * 60)
    print("  REPARTITION PAR NIVEAU DE RISQUE")
    df.groupBy("risk_level").count().orderBy("count", ascending=False).show()
    print("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════
#  PIPELINE PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════

def run_spark_pipeline(input_path: str = RAW_DATA_PATH) -> DataFrame:
    """Exécute le pipeline Spark complet : chargement → nettoyage → features → détection."""
    print("=" * 60)
    print("  PIPELINE SPARK - TRAITEMENT DISTRIBUE")
    print("=" * 60)

    spark = create_spark_session()

    try:
        df = load_data(spark, input_path)
        df = clean_data(df)
        df = engineer_features(df)
        df = detect_fraud(df)

        print_spark_report(df)

        # Sauvegarde en format Parquet (optimisé pour BigQuery)
        os.makedirs(SPARK_OUTPUT_PATH, exist_ok=True)
        (
            df.coalesce(1)
            .write.mode("overwrite")
            .parquet(SPARK_OUTPUT_PATH)
        )
        print(f"[SPARK-SAVE] Resultats Parquet -> {SPARK_OUTPUT_PATH}")

        # Export CSV pour compatibilité
        csv_path = os.path.join(SPARK_OUTPUT_PATH, "fraud_results_spark.csv")
        df.toPandas().to_csv(csv_path, index=False)
        print(f"[SPARK-SAVE] Resultats CSV -> {csv_path}")

        return df

    finally:
        spark.stop()
        print("[SPARK] Session fermee")


if __name__ == "__main__":
    run_spark_pipeline()
