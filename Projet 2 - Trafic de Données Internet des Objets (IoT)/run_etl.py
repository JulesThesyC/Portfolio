"""
Point d'entrée - Exécution du pipeline ETL
------------------------------------------
Usage: python run_etl.py
"""

from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent))

from etl.pipeline import run_pipeline

if __name__ == "__main__":
    result = run_pipeline()
    print("\n--- Résumé du pipeline ETL ---")
    for k, v in result.items():
        print(f"  {k}: {v}")
    print("\nPipeline terminé avec succès.")
