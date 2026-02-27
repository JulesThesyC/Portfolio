# Rapport de Projet — Surveillance Environnementale IoT

**Auteur** : Data Manager / Data Engineer  
**Date** : Février 2025  
**Thématique** : Surveillance de l'environnement en temps réel

---

## 1. Contexte et objectifs

### Description
Système de collecte et d'analyse de données provenant de capteurs environnementaux (température, humidité, pollution) dans une ville.

### Objectifs atteints
1. **Pipeline ETL** pour intégrer les données des capteurs
2. **Stockage Data Lake** (local et S3 prêt)
3. **Dashboard interactif** avec tendances et alertes

---

## 2. Architecture

```
[Capteurs IoT] → [Kafka] → [ETL Pipeline] → [Data Lake / S3]
                                    ↓
                            [Dashboard Streamlit]
                                    ↑
                            [Apache Airflow - DAG]
```

---

## 3. Technologies utilisées

| Composant | Technologie | Rôle |
|-----------|-------------|------|
| Streaming | Apache Kafka | Ingestion temps réel |
| Stockage | AWS S3 / Data Lake local | Persistance données |
| Orchestration | Apache Airflow | Planification ETL quotidien |
| Visualisation | Streamlit + Plotly | Dashboard interactif |

---

## 4. Pipeline ETL

### Extract
- Lecture du CSV des capteurs ou des fichiers bruts
- Conversion des timestamps

### Transform
- Validation des types (température, humidité, pollution)
- Enrichissement : date, heure, flags d’alerte
- Détection des seuils critiques

### Load
- Écriture en Parquet dans le Data Lake
- Structure : `data_lake/YYYY/MM/DD/iot_sensors_*.parquet`

---

## 5. Seuils d’alerte

| Métrique | Min | Max | Alerte pollution (critique) |
|----------|-----|-----|-----------------------------|
| Température | -5°C | 35°C | - |
| Humidité | 20% | 90% | - |
| Pollution | - | - | ≥ 8 |

---

## 6. Visualisations et résultats

### Dashboard
- **KPIs** : nombre d’enregistrements, moyennes, alertes
- **Graphiques** :
  - Évolution température / humidité / pollution par zone
  - Barres de pollution moyenne par zone
  - Radar de comparaison par zone
  - Heatmap température (heure × zone)
- **Alertes** : tableau des événements hors seuils

### Exemple de tendances observées
- Variations de température selon les zones et l’heure
- Humidité parfois hors plage recommandée
- Pics de pollution (niveau ≥ 8) identifiés par zone

---

## 7. Livrables

- Code source : pipeline ETL, Kafka, Airflow, dashboard
- `requirements.txt` pour les dépendances
- README avec instructions d’installation et d’exécution
- Rapport projet (ce document)

---

## 8. Call to Action

Ce projet illustre une architecture Data Engineering pour l’IoT (ETL, streaming, Data Lake, orchestration, visualisation).

**Souhaitez-vous en discuter ?**  
👉 Je suis disponible pour présenter l’architecture, les choix techniques et les pistes d’évolution (temps réel, ML, alerting avancé).
