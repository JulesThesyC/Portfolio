# Trafic de Données Internet des Objets (IoT)

**Projet Data Engineering** : 🌍 Surveillance de l'environnement en temps réel  

---

## A. Description

Système de collecte et d'analyse de données provenant de capteurs environnementaux (température, humidité, pollution) dans une ville.

---

## B. Objectifs

- ✅ **Pipeline ETL** : intégration des données des capteurs
- ✅ **Data Lake** : stockage structuré (S3 ou local)
- ✅ **Dashboard interactif** : visualisation des tendances et alertes

---

## C. Technologies

| Composant   | Technologie              |
|------------|---------------------------|
| Streaming  | Apache Kafka              |
| Stockage   | AWS S3 / Data Lake local  |
| Orchestration | Apache Airflow        |
| Visualisation | Streamlit + Plotly   |

---

## D. Structure du projet

```
.
├── config.py              # Configuration et seuils
├── run_etl.py             # Lancer le pipeline ETL
├── run_dashboard.py       # Lancer le dashboard
├── IoT_Environmental_Sensors.csv
├── etl/
│   ├── extract.py         # Extraction CSV/Kafka
│   ├── transform.py       # Transformation et alertes
│   ├── load.py            # Chargement Data Lake
│   └── pipeline.py        # Orchestration ETL
├── data_lake/
│   └── s3_uploader.py     # Upload S3 (optionnel)
├── kafka/
│   ├── producer.py        # Ingestion temps réel
│   └── consumer.py        # Traitement temps réel
├── dags/
│   └── iot_etl_dag.py     # DAG Airflow
└── dashboard/
    └── app.py             # Dashboard Streamlit
```

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Utilisation

### 1. Pipeline ETL

```bash
python run_etl.py
```

Crée les dossiers `data/processed` et `data/data_lake` avec les fichiers Parquet.

### 2. Dashboard

```bash
streamlit run dashboard/app.py
```

Ouvre le navigateur sur `http://localhost:8501`.

### 3. Kafka (optionnel)

Prérequis : Kafka en cours d'exécution.

```bash
# Producer (envoi des données)
python kafka/producer.py

# Consumer (réception et écriture CSV)
python kafka/consumer.py
```

### 4. Airflow

Copier `dags/iot_etl_dag.py` dans `AIRFLOW_HOME/dags/`.

---

## Seuils d'alerte

| Métrique   | Seuil min | Seuil max | Critique     |
|-----------|-----------|-----------|--------------|
| Température | -5°C    | 35°C      | Hors plage   |
| Humidité  | 20%       | 90%       | Hors plage   |
| Pollution | -         | -         | ≥ 8 (indice) |

---

## Données

- **Source** : `IoT_Environmental_Sensors.csv`
- **Zones** : 10 zones urbaines
- **Période** : 7 jours, mesures toutes les 10 minutes
- **Colonnes** : Timestamp, Location, Temperature, Humidity, Pollution_Level

---

## Contact

Souhaitez-vous discuter de ce projet ou des architectures Data Engineering ?  
👉 N'hésitez pas à me contacter pour échanger sur les cas d'usage et les évolutions possibles.
