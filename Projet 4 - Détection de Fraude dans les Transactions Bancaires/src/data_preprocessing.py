"""
Module de prétraitement des données bancaires.
Nettoyage, validation des types, traitement des valeurs manquantes
et feature engineering pour la détection de fraude.
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    RAW_DATA_PATH, CLEANED_DATA_PATH, ENRICHED_DATA_PATH
)


def load_raw_data(path: str = RAW_DATA_PATH) -> pd.DataFrame:
    """Charge le fichier CSV brut et effectue un premier diagnostic."""
    df = pd.read_csv(path)
    print(f"[LOAD] {len(df)} transactions chargees - {df.shape[1]} colonnes")
    print(f"[LOAD] Colonnes : {list(df.columns)}")
    print(f"[LOAD] Valeurs manquantes :\n{df.isnull().sum()}")
    print(f"[LOAD] Doublons : {df.duplicated().sum()}")
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoyage : types, valeurs manquantes, doublons, cohérence."""
    df = df.copy()

    # Suppression des doublons exacts
    before = len(df)
    df.drop_duplicates(inplace=True)
    print(f"[CLEAN] {before - len(df)} doublons supprimes")

    # Conversion du timestamp
    df["Transaction_Time"] = pd.to_datetime(df["Transaction_Time"], errors="coerce")
    invalid_dates = df["Transaction_Time"].isnull().sum()
    if invalid_dates > 0:
        print(f"[CLEAN] {invalid_dates} dates invalides detectees - supprimees")
        df.dropna(subset=["Transaction_Time"], inplace=True)

    # Validation du montant
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    df = df[df["Amount"] > 0].copy()

    # Normalisation des colonnes catégorielles
    for col in ["Transaction_Type", "Location", "Status", "Is_Fraud"]:
        df[col] = df[col].astype(str).str.strip().str.upper()

    # Validation des valeurs autorisées
    valid_types = {"DEPOSIT", "WITHDRAWAL", "TRANSFER"}
    valid_locations = {"ONLINE", "ATM", "BRANCH"}
    valid_status = {"COMPLETED", "PENDING", "FAILED"}
    valid_fraud = {"YES", "NO"}

    df = df[df["Transaction_Type"].isin(valid_types)]
    df = df[df["Location"].isin(valid_locations)]
    df = df[df["Status"].isin(valid_status)]
    df = df[df["Is_Fraud"].isin(valid_fraud)]

    df.reset_index(drop=True, inplace=True)
    print(f"[CLEAN] {len(df)} transactions valides apres nettoyage")
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Feature engineering temporel et comportemental."""
    df = df.copy()

    # ── Features temporelles ──
    df["Hour"] = df["Transaction_Time"].dt.hour
    df["DayOfWeek"] = df["Transaction_Time"].dt.dayofweek  # 0=Lundi
    df["DayOfWeek_Name"] = df["Transaction_Time"].dt.day_name()
    df["Month"] = df["Transaction_Time"].dt.month
    df["Date"] = df["Transaction_Time"].dt.date
    df["Is_Weekend"] = df["DayOfWeek"].isin([5, 6]).astype(int)
    df["Is_Night"] = df["Hour"].between(0, 5).astype(int)

    # ── Catégorisation du montant ──
    df["Amount_Bin"] = pd.cut(
        df["Amount"],
        bins=[0, 100, 500, 2000, 5000, 8000, 10000],
        labels=["Micro", "Petit", "Moyen", "Élevé", "Très_Élevé", "Extrême"],
    )

    # ── Statistiques par utilisateur ──
    user_stats = df.groupby("User_ID").agg(
        user_tx_count=("Amount", "count"),
        user_mean_amount=("Amount", "mean"),
        user_std_amount=("Amount", "std"),
        user_max_amount=("Amount", "max"),
        user_total_amount=("Amount", "sum"),
    ).reset_index()
    user_stats["user_std_amount"] = user_stats["user_std_amount"].fillna(0)

    df = df.merge(user_stats, on="User_ID", how="left")

    # ── Z-score du montant par rapport à l'utilisateur ──
    df["amount_zscore_user"] = np.where(
        df["user_std_amount"] > 0,
        (df["Amount"] - df["user_mean_amount"]) / df["user_std_amount"],
        0,
    )

    # ── Z-score global ──
    global_mean = df["Amount"].mean()
    global_std = df["Amount"].std()
    df["amount_zscore_global"] = (df["Amount"] - global_mean) / global_std

    # ── Comptage de transactions par jour par utilisateur ──
    daily_counts = (
        df.groupby(["User_ID", "Date"])
        .size()
        .reset_index(name="daily_tx_count")
    )
    df = df.merge(daily_counts, on=["User_ID", "Date"], how="left")

    print(f"[FEATURES] {df.shape[1]} colonnes apres feature engineering")
    return df


def save_data(df: pd.DataFrame, path: str) -> None:
    """Sauvegarde le DataFrame en CSV."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"[SAVE] Donnees sauvegardees -> {path} ({len(df)} lignes)")


def run_preprocessing_pipeline() -> pd.DataFrame:
    """Exécute le pipeline complet de prétraitement."""
    print("=" * 60)
    print("  PIPELINE DE PRETRAITEMENT DES DONNEES")
    print("=" * 60)

    df = load_raw_data()
    df = clean_data(df)
    save_data(df, CLEANED_DATA_PATH)

    df = engineer_features(df)
    save_data(df, ENRICHED_DATA_PATH)

    print("\n[STATS] Resume du dataset enrichi :")
    print(f"  - Transactions : {len(df)}")
    print(f"  - Utilisateurs uniques : {df['User_ID'].nunique()}")
    print(f"  - Periode : {df['Transaction_Time'].min()} -> {df['Transaction_Time'].max()}")
    print(f"  - Montant moyen : {df['Amount'].mean():.2f} EUR")
    print(f"  - Fraudes existantes : {(df['Is_Fraud'] == 'YES').sum()}")
    print("=" * 60)
    return df


if __name__ == "__main__":
    df = run_preprocessing_pipeline()
