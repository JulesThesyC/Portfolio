# Portfolio Data Engineer — Jules COLONAS

> **[Voir la vitrine en ligne](https://julesthesyc.github.io/Portfolio/)**

Portfolio regroupant 4 projets Data Engineering / Data Science, conçu pour mettre en avant mes compétences techniques concrètes auprès de recruteurs.

---

## Projets présentés

### 1. Analyse des Données de Construction
**Thématique** : Optimisation des coûts et de la durée des chantiers

Analyse de 1 000 projets historiques (2018–2023) pour modéliser et prédire les coûts de chantier. Un modèle Random Forest (R² = 0.999) identifie **77,4 M€ d'économies potentielles**.

| Indicateur | Valeur |
|---|---|
| Projets analysés | 1 000 |
| R² Random Forest | 0.999 |
| Économies identifiées | 77,4 M€ |

**Technologies** : Python, pandas, scikit-learn, SQL, Power BI, Matplotlib, Seaborn

🔗 **[Dépôt GitHub](https://github.com/JulesThesyC/Analyse-Donnees-Construction)** | 📄 [Rapport du projet](Projet%201%20-%20Analyse%20des%20Donn%C3%A9es%20de%20Construction/RAPPORT_ANALYSE_CONSTRUCTION.html)

---

### 2. Surveillance Environnementale IoT
**Thématique** : Surveillance de l'environnement en temps réel

Système end-to-end de collecte et d'analyse de données de capteurs (température, humidité, pollution) avec pipeline ETL, Data Lake Parquet et dashboard interactif.

```
[Capteurs IoT] → [Kafka] → [ETL Pipeline] → [Data Lake / S3]
                                  ↓
                          [Dashboard Streamlit]
                                  ↑
                          [Apache Airflow - DAG]
```

**Technologies** : Apache Kafka, AWS S3, Apache Airflow, Streamlit, Plotly, Python

🔗 **[Dépôt GitHub](https://github.com/JulesThesyC/Surveillance-IoT)** | 📄 [Rapport du projet](Projet%202%20-%20Trafic%20de%20Donn%C3%A9es%20Internet%20des%20Objets%20(IoT)/docs/PROJET_REPORT.html)

---

### 3. Système de Recommandation Streaming
**Thématique** : Amélioration de l'engagement des utilisateurs

Système de recommandation collaboratif (User-Based Collaborative Filtering) pour un service de streaming vidéo, avec API REST Flask pour servir les résultats.

| Indicateur | Valeur |
|---|---|
| Utilisateurs | 436 |
| Films | 198 |
| Taux d'engagement | 87 % |
| Couverture catalogue | 99 % |

**Technologies** : Python, pandas, scikit-learn, SQL, Flask, SQLite

🔗 **[Dépôt GitHub](https://github.com/JulesThesyC/Recommandation-Streaming)** | 📄 [Rapport du projet](Projet%203%20-%20Syst%C3%A8me%20de%20Recommandation%20pour%20un%20Service%20de%20Streaming/rapport.html)

---

### 4. Détection de Fraude Bancaire
**Thématique** : Identification de transactions suspectes en temps réel

Pipeline end-to-end de détection de fraude combinant 6 règles heuristiques avec un système de scoring pondéré (0-100) sur 25 000 transactions de 5 000 utilisateurs. Dashboard Streamlit 5 onglets avec alertes temps réel.

| Indicateur | Valeur |
|---|---|
| Transactions | 25 000 |
| Utilisateurs | 5 000 |
| Règles de détection | 6 |
| Niveaux de risque | 4 (LOW → CRITICAL) |

```
start → preprocess → detect → spark → load_bq → quality_check → branch
                                                                   ├→ critical_alert → end
                                                                   └→ normal_report  → end
```

**Technologies** : Python, PySpark, BigQuery, Apache Airflow, Streamlit, Plotly, scikit-learn

🔗 **[Dépôt GitHub](https://github.com/JulesThesyC/Detection-Fraude-Bancaire)** | 📄 [Rapport du projet](Projet%204%20-%20D%C3%A9tection%20de%20Fraude%20dans%20les%20Transactions%20Bancaires/RAPPORT_DETECTION_FRAUDE.html)

---

## Stack technique globale

| Domaine | Technologies |
|---|---|
| Langages & Analyse | Python, SQL, pandas, NumPy |
| Machine Learning | scikit-learn, Random Forest, Collaborative Filtering, Scoring heuristique |
| Data Engineering | Apache Kafka, Apache Airflow, PySpark, AWS S3, ETL, Parquet |
| Cloud & Stockage | BigQuery, AWS S3, SQLite, Data Lake |
| Visualisation & BI | Power BI, Streamlit, Plotly, Matplotlib, Seaborn |
| API & Déploiement | Flask, REST API, Streamlit |

## Structure du dépôt

```
Portfolio/
├── index.html                          ← Vitrine web (page d'accueil)
├── README.md
│
├── Projet 1 - Analyse des Données de Construction/
│   ├── construction_analysis.py        ← Pipeline Python
│   ├── RAPPORT_ANALYSE_CONSTRUCTION.html
│   ├── outputs/                        ← Graphiques et métriques
│   ├── sql/                            ← Requêtes SQL
│   └── power_bi/                       ← Instructions Power BI
│
├── Projet 2 - Trafic de Données IoT/
│   ├── etl/                            ← Pipeline ETL (Extract, Transform, Load)
│   ├── kafka/                          ← Producer & Consumer Kafka
│   ├── dags/                           ← DAG Apache Airflow
│   ├── dashboard/                      ← Dashboard Streamlit
│   ├── docs/PROJET_REPORT.html
│   └── data/                           ← Data Lake local (Parquet)
│
├── Projet 3 - Recommandation Streaming/
│   ├── app.py                          ← API Flask
│   ├── recommender.py                  ← Modèle collaboratif
│   ├── analysis.py                     ← Analyse exploratoire
│   ├── evaluation.py                   ← Métriques du modèle
│   ├── rapport.html
│   └── static/visualizations/          ← Graphiques de performance
│
└── Projet 4 - Détection de Fraude Bancaire/
    ├── src/                            ← Prétraitement, détection, Spark, BigQuery
    ├── dags/                           ← DAG Airflow (orchestration complète)
    ├── dashboard/                      ← Dashboard Streamlit 5 onglets
    ├── config/                         ← Configuration centralisée (seuils, chemins)
    ├── RAPPORT_DETECTION_FRAUDE.html
    └── data/                           ← Résultats et alertes
```

## Déploiement

La vitrine est hébergée via **GitHub Pages** et accessible à :

**https://julesthesyc.github.io/Portfolio/**

Pour visualiser en local, ouvrir `index.html` dans un navigateur.

---

## Contact

Intéressé par mon profil ? N'hésitez pas à me contacter pour discuter de ces projets en détail.

- **Auteur** — Jules COLONAS
- **LinkedIn** — **[Jules COLONAS](https://www.linkedin.com/in/julescolonas)**
- **GitHub** — **[JulesThesyC](https://github.com/JulesThesyC)**
