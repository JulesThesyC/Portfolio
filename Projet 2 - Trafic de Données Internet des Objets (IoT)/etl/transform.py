"""
ETL - Transformation
--------------------
Nettoyage, validation et enrichissement des données.
"""

import pandas as pd
from config import THRESHOLDS


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme les données des capteurs :
    - Validation des types
    - Détection des outliers
    - Enrichissement (seuils, alertes)
    """
    df = df.copy()

    # Validation et conversion des types
    df["Temperature"] = pd.to_numeric(df["Temperature"], errors="coerce")
    df["Humidity"] = pd.to_numeric(df["Humidity"], errors="coerce")
    df["Pollution_Level"] = pd.to_numeric(df["Pollution_Level"], errors="coerce")

    # Suppression des lignes avec valeurs manquantes
    df = df.dropna(subset=["Temperature", "Humidity", "Pollution_Level"])

    # Extraction date/heure pour l'analyse
    df["date"] = df["Timestamp"].dt.date
    df["hour"] = df["Timestamp"].dt.hour

    # Catégorisation des alertes
    df["temp_alert"] = (
        (df["Temperature"] < THRESHOLDS["temperature"]["min"])
        | (df["Temperature"] > THRESHOLDS["temperature"]["max"])
    )
    df["humidity_alert"] = (
        (df["Humidity"] < THRESHOLDS["humidity"]["min"])
        | (df["Humidity"] > THRESHOLDS["humidity"]["max"])
    )
    df["pollution_critical"] = df["Pollution_Level"] >= THRESHOLDS["pollution"]["critical"]
    df["pollution_warning"] = (
        (df["Pollution_Level"] >= THRESHOLDS["pollution"]["warning"])
        & (df["Pollution_Level"] < THRESHOLDS["pollution"]["critical"])
    )
    df["has_alert"] = (
        df["temp_alert"] | df["humidity_alert"] | df["pollution_critical"]
    )

    return df
