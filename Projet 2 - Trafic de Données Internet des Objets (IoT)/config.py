"""
Configuration centrale - Surveillance Environnementale IoT
---------------------------------------------------------
Définit les seuils d'alerte, chemins et paramètres du projet.
"""

import os
from pathlib import Path

# Chemins du projet
PROJECT_ROOT = Path(__file__).parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_LAKE = PROJECT_ROOT / "data" / "data_lake"
DATASET_CSV = PROJECT_ROOT / "IoT_Environmental_Sensors.csv"

# Seuils critiques pour les alertes
THRESHOLDS = {
    "temperature": {"min": -5, "max": 35, "unit": "°C"},
    "humidity": {"min": 20, "max": 90, "unit": "%"},
    "pollution": {"critical": 8, "warning": 6, "unit": "indice"},
}

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_SENSORS = "iot-environmental-sensors"

# S3 / Data Lake
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "iot-environmental-data")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")
USE_LOCAL_LAKE = os.getenv("USE_LOCAL_LAKE", "true").lower() == "true"
