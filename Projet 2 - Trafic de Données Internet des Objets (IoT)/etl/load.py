"""
ETL - Chargement
----------------
Charge les données transformées vers le Data Lake (S3 ou local).
"""

import json
from pathlib import Path
from datetime import datetime
import pandas as pd

from config import DATA_LAKE, USE_LOCAL_LAKE, DATA_PROCESSED


def _ensure_dirs(base: Path):
    """Crée les répertoires nécessaires."""
    base.mkdir(parents=True, exist_ok=True)


def load_to_data_lake(df: pd.DataFrame, prefix: str = "processed") -> str:
    """
    Charge les données dans le Data Lake.
    Structure : data_lake/YYYY/MM/DD/iot_sensors_<timestamp>.parquet
    """
    _ensure_dirs(DATA_LAKE)
    now = datetime.utcnow()
    path = DATA_LAKE / str(now.year) / f"{now.month:02d}" / f"{now.day:02d}"
    path.mkdir(parents=True, exist_ok=True)
    filename = f"iot_sensors_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
    filepath = path / filename
    df.to_parquet(filepath, index=False)
    return str(filepath)


def load_to_processed(df: pd.DataFrame, filename: str = None) -> str:
    """Charge les données transformées en format Parquet et JSON (métadonnées)."""
    _ensure_dirs(DATA_PROCESSED)
    now = datetime.utcnow()
    base = filename or f"sensors_processed_{now.strftime('%Y%m%d_%H%M%S')}"
    parquet_path = DATA_PROCESSED / f"{base}.parquet"
    meta_path = DATA_PROCESSED / f"{base}_meta.json"

    df.to_parquet(parquet_path, index=False)
    meta = {
        "rows": len(df),
        "processed_at": now.isoformat(),
        "columns": list(df.columns),
        "date_range": {
            "min": str(df["Timestamp"].min()),
            "max": str(df["Timestamp"].max()),
        },
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    return str(parquet_path)
