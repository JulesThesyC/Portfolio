"""
ETL - Extraction
----------------
Charge les données brutes depuis le CSV ou Kafka.
"""

import pandas as pd
from pathlib import Path
from config import DATASET_CSV, DATA_RAW


def extract_from_csv(file_path: Path = None) -> pd.DataFrame:
    """Extrait les données du fichier CSV des capteurs IoT."""
    path = file_path or DATASET_CSV
    df = pd.read_csv(path)
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    return df


def extract_from_raw(data_raw_dir: Path = None) -> pd.DataFrame:
    """Extrait et concatène tous les fichiers CSV du dossier raw."""
    raw_dir = data_raw_dir or DATA_RAW
    if not raw_dir.exists():
        return pd.DataFrame()

    files = list(raw_dir.glob("*.csv"))
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        df = pd.read_csv(f)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
