# Détection de Fraude dans les Transactions Bancaires

Pipeline de data engineering complet pour l'identification en temps réel des transactions bancaires suspectes, combinant prétraitement Python, traitement distribué PySpark, stockage BigQuery, orchestration Airflow et visualisation Streamlit.

---

## Architecture du Projet

```
Projet 4 - Détection de Fraude/
│
├── config/
│   ├── __init__.py
│   └── settings.py              # Configuration centralisée (seuils, chemins, BigQuery)
│
├── src/
│   ├── __init__.py
│   ├── data_preprocessing.py    # Nettoyage + feature engineering (Python/Pandas)
│   ├── fraud_detection.py       # Moteur de détection (6 règles + scoring pondéré)
│   ├── spark_processing.py      # Pipeline distribué PySpark
│   └── bigquery_utils.py        # Intégration BigQuery (schéma, chargement, requêtes)
│
├── dags/
│   └── fraud_detection_dag.py   # DAG Airflow (orchestration complète)
│
├── dashboard/
│   └── app.py                   # Tableau de bord interactif Streamlit
│
├── data/                        # Données générées par le pipeline (gitignored)
│   ├── cleaned_transactions.csv
│   ├── enriched_transactions.csv
│   ├── fraud_results.csv
│   ├── fraud_results_alerts.csv
│   └── spark_output/
│
├── Bank_Transactions.csv        # Dataset source (25 000 transactions)
├── Data.py                      # Script de génération du dataset
├── requirements.txt             # Dépendances Python
└── README.md
```

---

## Dataset

| Colonne            | Type      | Description                                       |
|--------------------|-----------|---------------------------------------------------|
| `User_ID`          | String    | Identifiant utilisateur (User_1 à User_5000)     |
| `Transaction_Time` | Timestamp | Date et heure de la transaction                   |
| `Amount`           | Float     | Montant de la transaction (0.01 - 10 000 €)      |
| `Transaction_Type` | String    | DEPOSIT, WITHDRAWAL, TRANSFER                     |
| `Location`         | String    | ONLINE, ATM, BRANCH                               |
| `Status`           | String    | COMPLETED, PENDING, FAILED                         |
| `Is_Fraud`         | String    | YES / NO (label de référence)                     |

- **25 000 transactions** générées de manière réaliste
- **5 000 utilisateurs** uniques
- Période couverte : ~1 an

---

## Pipeline de Détection

### Étape 1 — Prétraitement (`data_preprocessing.py`)

- Suppression des doublons
- Validation et conversion des types (timestamp, montant)
- Normalisation des colonnes catégorielles
- **Feature engineering** :
  - Variables temporelles : heure, jour de semaine, mois, week-end, nuit
  - Catégorisation du montant (Micro → Extrême)
  - Statistiques par utilisateur (moyenne, écart-type, max, total)
  - Z-scores (par utilisateur et global)
  - Comptage de transactions journalières par utilisateur

### Étape 2 — Détection de Fraude (`fraud_detection.py`)

Six règles heuristiques avec un système de scoring pondéré (0-100) :

| Règle | Description | Poids |
|-------|------------|-------|
| R1 — Montant Élevé | Montant ≥ 8 000 € | 25 |
| R2 — Montant Extrême | Montant ≥ 9 500 € | 15 |
| R3 — Transaction Nocturne | Heure entre 00h et 05h | 15 |
| R4 — En Ligne + Élevé | Online + montant ≥ 7 000 € | 20 |
| R5 — Anomalie Statistique | Z-score global ≥ 2.5 | 15 |
| R6 — Fréquence Élevée | ≥ 5 transactions/jour/utilisateur | 10 |

**Niveaux de risque** :
- **CRITICAL** (≥ 70) : Alerte immédiate
- **HIGH** (≥ 50) : Investigation requise
- **MEDIUM** (≥ 30) : Surveillance renforcée
- **LOW** (< 30) : Normal

Inclut une **matrice de confusion** et les métriques Précision / Rappel / F1-Score.

### Étape 3 — Traitement PySpark (`spark_processing.py`)

Reproduit l'intégralité du pipeline en environnement distribué :
- Chargement avec schéma typé
- Nettoyage et validation
- Feature engineering via Window Functions
- Détection de fraude avec les mêmes règles
- Export en **Parquet** (optimisé pour BigQuery) et CSV

### Étape 4 — BigQuery (`bigquery_utils.py`)

- Schémas typés pour 3 tables : `raw_transactions`, `enriched_transactions`, `fraud_alerts`
- Chargement automatisé depuis Pandas
- **5 requêtes analytiques prédéfinies** :
  - Résumé des fraudes par niveau de risque
  - Top 20 utilisateurs suspects
  - Pattern horaire des fraudes
  - Tendance mensuelle
  - Analyse croisée type × localisation
- Mode simulation si BigQuery n'est pas configuré

### Étape 5 — Orchestration Airflow (`fraud_detection_dag.py`)

```
start → preprocess → detect → spark → load_bq → quality_check → branch
                                                                   ├→ critical_alert → end
                                                                   └→ normal_report  → end
```

- Exécution planifiée : `@hourly`
- Communication inter-tâches via **XCom**
- **Contrôle qualité** automatique (données non vides, taux d'anomalie raisonnable)
- **Branchement conditionnel** : alerte critique si taux de suspicion > 10%
- Retry (2 tentatives), timeout 30min, alertes email en cas d'échec

---

## Tableau de Bord Streamlit

Dashboard interactif avec 5 onglets :

1. **Vue d'ensemble** : KPIs, répartition par risque, type, localisation, scatter plot
2. **Alertes** : Tableau des transactions CRITICAL/HIGH avec export CSV
3. **Tendances** : Analyse par heure, jour de semaine, mois avec taux de fraude
4. **Analyse Utilisateurs** : Top 20 suspects, recherche individuelle, profil complet
5. **Détail Règles** : Performance de chaque règle, corrélation entre règles

Filtres interactifs : niveau de risque, type, localisation, montant, période.

---

## Installation & Exécution

### 1. Installer les dépendances

```bash
pip install -r requirements.txt
```

### 2. Exécuter le pipeline complet (Python)

```bash
# Étape 1 : Prétraitement
python src/data_preprocessing.py

# Étape 2 : Détection de fraude
python src/fraud_detection.py

# Étape 3 : Pipeline Spark (nécessite Java + PySpark)
python src/spark_processing.py

# Étape 4 : BigQuery (nécessite config GCP)
python src/bigquery_utils.py
```

### 3. Lancer le tableau de bord

```bash
streamlit run dashboard/app.py
```

### 4. Configuration Airflow (optionnel)

```bash
# Initialiser Airflow
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create --username admin --password admin --role Admin --firstname Admin --lastname User --email admin@example.com

# Copier le DAG
cp dags/fraud_detection_dag.py $AIRFLOW_HOME/dags/

# Lancer Airflow
airflow webserver -p 8080 &
airflow scheduler &
```

### 5. Configuration BigQuery (optionnel)

```bash
# Définir le projet GCP
export GCP_PROJECT_ID="votre-projet-gcp"

# Authentification
gcloud auth application-default login
```

---

## Technologies

| Technologie | Usage |
|-------------|-------|
| **Python / Pandas** | Prétraitement, feature engineering, détection heuristique |
| **PySpark** | Traitement distribué à grande échelle |
| **BigQuery** | Stockage cloud, requêtes analytiques SQL |
| **Apache Airflow** | Orchestration et automatisation du pipeline |
| **Streamlit** | Tableau de bord interactif temps réel |
| **Plotly** | Visualisations interactives |
| **Scikit-learn** | Métriques de performance |

---

## Résultats Clés

- **6 règles heuristiques** combinées en un score de risque pondéré
- **4 niveaux de risque** (LOW → CRITICAL) pour prioriser les investigations
- **Matrice de confusion** avec métriques Précision / Rappel / F1
- **Pipeline entièrement automatisé** via Airflow avec contrôle qualité intégré
- **Dashboard temps réel** avec filtres dynamiques et export des alertes


## Contact

Intéressé par mon profil ? N'hésitez pas à me contacter pour discuter de ces projets en détail.

- **Auteur** — Jules COLONAS
- **Profil LinkedIn** — **[Jules COLONAS](https://www.linkedin.com/in/julescolonas)**