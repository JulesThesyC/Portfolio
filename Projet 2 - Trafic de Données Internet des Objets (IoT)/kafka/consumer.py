"""
Kafka Consumer - Traitement temps réel
--------------------------------------
Consomme les messages Kafka et les persiste / traite.
"""

import json
from pathlib import Path
from datetime import datetime

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_SENSORS, DATA_RAW, THRESHOLDS


def consume_to_csv(output_dir: Path = None, max_messages: int = None):
    """
    Consomme les messages Kafka et les écrit dans un fichier CSV.
    Détecte les seuils critiques en temps réel.
    """
    try:
        from kafka import KafkaConsumer
    except ImportError:
        print("kafka-python requis : pip install kafka-python")
        return

    output_dir = output_dir or DATA_RAW
    output_dir.mkdir(parents=True, exist_ok=True)

    consumer = KafkaConsumer(
        KAFKA_TOPIC_SENSORS,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    rows = []
    count = 0
    for msg in consumer:
        data = msg.value
        # Vérification des seuils
        alert = (
            data["temperature"] < THRESHOLDS["temperature"]["min"]
            or data["temperature"] > THRESHOLDS["temperature"]["max"]
            or data["humidity"] < THRESHOLDS["humidity"]["min"]
            or data["humidity"] > THRESHOLDS["humidity"]["max"]
            or data["pollution_level"] >= THRESHOLDS["pollution"]["critical"]
        )
        data["alert"] = alert
        rows.append(data)
        count += 1
        if max_messages and count >= max_messages:
            break

    consumer.close()

    if rows:
        import pandas as pd
        df = pd.DataFrame(rows)
        df = df.rename(columns={"pollution_level": "Pollution_Level"})
        filepath = output_dir / f"kafka_ingest_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filepath, index=False)
        print(f"Enregistré {len(df)} messages dans {filepath}")
