"""
Pipeline ETL complet
--------------------
Orchestre Extraction, Transformation et Chargement.
"""

import pandas as pd
from pathlib import Path

from etl.extract import extract_from_csv
from etl.transform import transform
from etl.load import load_to_data_lake, load_to_processed


def run_pipeline(csv_path: Path = None) -> dict:
    """
    Exécute le pipeline ETL complet.
    Retourne un résumé de l'exécution.
    """
    # Extract
    df = extract_from_csv(csv_path)

    # Transform
    df_transformed = transform(df)

    # Load
    lake_path = load_to_data_lake(df_transformed)
    processed_path = load_to_processed(df_transformed)

    return {
        "rows_extracted": len(df),
        "rows_loaded": len(df_transformed),
        "data_lake_path": lake_path,
        "processed_path": processed_path,
        "alerts_count": int(df_transformed["has_alert"].sum()),
    }


if __name__ == "__main__":
    result = run_pipeline()
    print("Pipeline ETL terminé avec succès.")
    for k, v in result.items():
        print(f"  {k}: {v}")
