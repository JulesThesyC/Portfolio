"""
Moteur de détection de fraude basé sur des heuristiques.
Implémente 6 règles métier et un système de scoring pondéré
pour qualifier chaque transaction avec un niveau de risque.
"""
import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    FRAUD_RULES, RISK_WEIGHTS, RISK_LEVELS,
    ENRICHED_DATA_PATH, FRAUD_RESULTS_PATH,
)


# ═══════════════════════════════════════════════════════════════════════════
#  RÈGLES DE DÉTECTION
# ═══════════════════════════════════════════════════════════════════════════

def rule_high_amount(df: pd.DataFrame) -> pd.Series:
    """R1 — Montant anormalement élevé (> seuil configuré)."""
    return (df["Amount"] >= FRAUD_RULES["high_amount_threshold"]).astype(int)


def rule_very_high_amount(df: pd.DataFrame) -> pd.Series:
    """R2 — Montant extrêmement élevé (> seuil critique)."""
    return (df["Amount"] >= FRAUD_RULES["very_high_amount_threshold"]).astype(int)


def rule_night_transaction(df: pd.DataFrame) -> pd.Series:
    """R3 — Transaction effectuée la nuit (00h-05h), période à risque."""
    return df["Is_Night"].astype(int)


def rule_online_high_amount(df: pd.DataFrame) -> pd.Series:
    """R4 — Transaction en ligne de montant élevé (combinaison suspecte)."""
    return (
        (df["Location"] == "ONLINE")
        & (df["Amount"] >= FRAUD_RULES["online_high_amount_threshold"])
    ).astype(int)


def rule_statistical_anomaly(df: pd.DataFrame) -> pd.Series:
    """R5 — Z-score global dépasse le seuil → anomalie statistique."""
    return (
        df["amount_zscore_global"].abs() >= FRAUD_RULES["zscore_threshold"]
    ).astype(int)


def rule_high_frequency(df: pd.DataFrame) -> pd.Series:
    """R6 — Nombre de transactions journalières par utilisateur anormalement élevé."""
    return (
        df["daily_tx_count"] >= FRAUD_RULES["high_frequency_count"]
    ).astype(int)


# ═══════════════════════════════════════════════════════════════════════════
#  SCORING & CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════

def compute_risk_score(df: pd.DataFrame) -> pd.DataFrame:
    """Applique toutes les règles et calcule un score de risque pondéré (0-100)."""
    df = df.copy()

    df["flag_high_amount"] = rule_high_amount(df)
    df["flag_very_high_amount"] = rule_very_high_amount(df)
    df["flag_night"] = rule_night_transaction(df)
    df["flag_online_high"] = rule_online_high_amount(df)
    df["flag_anomaly"] = rule_statistical_anomaly(df)
    df["flag_high_freq"] = rule_high_frequency(df)

    df["risk_score"] = (
        df["flag_high_amount"] * RISK_WEIGHTS["high_amount"]
        + df["flag_very_high_amount"] * RISK_WEIGHTS["very_high_amount"]
        + df["flag_night"] * RISK_WEIGHTS["night_transaction"]
        + df["flag_online_high"] * RISK_WEIGHTS["online_high_amount"]
        + df["flag_anomaly"] * RISK_WEIGHTS["statistical_anomaly"]
        + df["flag_high_freq"] * RISK_WEIGHTS["high_frequency"]
    )

    df["risk_score"] = df["risk_score"].clip(0, 100)

    df["risk_level"] = pd.cut(
        df["risk_score"],
        bins=[-1, RISK_LEVELS["medium"], RISK_LEVELS["high"],
              RISK_LEVELS["critical"], 101],
        labels=["LOW", "MEDIUM", "HIGH", "CRITICAL"],
        right=False,
    )

    df["is_suspected_fraud"] = (df["risk_score"] >= RISK_LEVELS["high"]).astype(int)

    return df


def generate_alerts(df: pd.DataFrame) -> pd.DataFrame:
    """Génère un DataFrame d'alertes pour les transactions suspectes."""
    alerts = df[df["is_suspected_fraud"] == 1].copy()
    alerts = alerts.sort_values("risk_score", ascending=False)

    alert_cols = [
        "User_ID", "Transaction_Time", "Amount", "Transaction_Type",
        "Location", "Status", "risk_score", "risk_level",
        "flag_high_amount", "flag_very_high_amount", "flag_night",
        "flag_online_high", "flag_anomaly", "flag_high_freq",
        "Is_Fraud",
    ]
    return alerts[alert_cols].reset_index(drop=True)


def print_detection_report(df: pd.DataFrame) -> None:
    """Affiche un rapport synthétique de la détection."""
    total = len(df)
    suspected = (df["is_suspected_fraud"] == 1).sum()
    actual = (df["Is_Fraud"] == "YES").sum()

    # Matrice de confusion simplifiée
    true_pos = ((df["is_suspected_fraud"] == 1) & (df["Is_Fraud"] == "YES")).sum()
    false_pos = ((df["is_suspected_fraud"] == 1) & (df["Is_Fraud"] == "NO")).sum()
    false_neg = ((df["is_suspected_fraud"] == 0) & (df["Is_Fraud"] == "YES")).sum()
    true_neg = ((df["is_suspected_fraud"] == 0) & (df["Is_Fraud"] == "NO")).sum()

    precision = true_pos / (true_pos + false_pos) if (true_pos + false_pos) > 0 else 0
    recall = true_pos / (true_pos + false_neg) if (true_pos + false_neg) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    print("\n" + "=" * 60)
    print("  RAPPORT DE DETECTION DE FRAUDE")
    print("=" * 60)
    print(f"  Total transactions     : {total:,}")
    print(f"  Fraudes etiquetees     : {actual:,}")
    print(f"  Transactions suspectes : {suspected:,}")
    print(f"  Taux de suspicion      : {suspected / total * 100:.1f}%")
    print("-" * 60)
    print("  MATRICE DE CONFUSION")
    print(f"    True Positives  : {true_pos:,}")
    print(f"    False Positives : {false_pos:,}")
    print(f"    False Negatives : {false_neg:,}")
    print(f"    True Negatives  : {true_neg:,}")
    print("-" * 60)
    print(f"  Precision : {precision:.4f}")
    print(f"  Rappel    : {recall:.4f}")
    print(f"  F1-Score  : {f1:.4f}")
    print("-" * 60)
    print("  REPARTITION PAR NIVEAU DE RISQUE")
    for level in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        count = (df["risk_level"] == level).sum()
        print(f"    {level:10s} : {count:,} ({count / total * 100:.1f}%)")
    print("=" * 60)


def run_fraud_detection(input_path: str = ENRICHED_DATA_PATH) -> pd.DataFrame:
    """Exécute le pipeline complet de détection."""
    print("=" * 60)
    print("  PIPELINE DE DETECTION DE FRAUDE")
    print("=" * 60)

    df = pd.read_csv(input_path, parse_dates=["Transaction_Time"])
    print(f"[LOAD] {len(df)} transactions enrichies chargees")

    df = compute_risk_score(df)
    print_detection_report(df)

    # Sauvegarde complète
    os.makedirs(os.path.dirname(FRAUD_RESULTS_PATH), exist_ok=True)
    df.to_csv(FRAUD_RESULTS_PATH, index=False)
    print(f"\n[SAVE] Resultats complets -> {FRAUD_RESULTS_PATH}")

    # Sauvegarde des alertes
    alerts = generate_alerts(df)
    alerts_path = FRAUD_RESULTS_PATH.replace(".csv", "_alerts.csv")
    alerts.to_csv(alerts_path, index=False)
    print(f"[SAVE] Alertes ({len(alerts)}) -> {alerts_path}")

    return df


if __name__ == "__main__":
    df = run_fraud_detection()
