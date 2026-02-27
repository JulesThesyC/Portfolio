"""
Kafka Producer - Ingestion temps réel
-------------------------------------
Envoie les données des capteurs vers Kafka (simulation).
"""

import json
import time
from pathlib import Path
import pandas as pd

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_SENSORS, DATASET_CSV


def produce_from_csv(csv_path: Path = None, delay: float = 0.1, limit: int = None):
    """
    Lit le CSV et envoie chaque ligne vers Kafka.
    Simule un flux temps réel en replayant les données.
    """
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("kafka-python requis : pip install kafka-python")
        return

    df = pd.read_csv(csv_path or DATASET_CSV)
    if limit:
        df = df.head(limit)

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    for _, row in df.iterrows():
        msg = {
            "timestamp": str(row["Timestamp"]),
            "location": row["Location"],
            "temperature": float(row["Temperature"]),
            "humidity": float(row["Humidity"]),
            "pollution_level": int(row["Pollution_Level"]),
        }
        producer.send(KAFKA_TOPIC_SENSORS, value=msg)
        time.sleep(delay)

    producer.flush()
    producer.close()
    print(f"Envoyé {len(df)} messages vers {KAFKA_TOPIC_SENSORS}")


if __name__ == "__main__":
    produce_from_csv(limit=50, delay=0.05)
