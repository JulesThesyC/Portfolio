# Système de Recommandation pour un Service de Streaming

**Projet Data Engineering** — Amélioration de l'engagement des utilisateurs via le filtrage collaboratif.

---

## Objectif

Développer un système de recommandation personnalisé basé sur l'historique de visionnage de 500 utilisateurs sur 200 films, en utilisant un algorithme de filtrage collaboratif (User-Based Collaborative Filtering).

## Structure du Projet

```
├── Streaming_Usage.csv      # Dataset (1000 évaluations)
├── Data.py                  # Script de génération du dataset
├── requirements.txt         # Dépendances Python
├── database.py              # Base SQLite normalisée + requêtes SQL
├── analysis.py              # Analyse exploratoire + visualisations
├── recommender.py           # Moteur de recommandation collaboratif
├── evaluation.py            # Évaluation du modèle (RMSE, Precision@K...)
├── app.py                   # API Flask + interface web
├── generate_report.py       # Génération du rapport HTML complet
├── rapport.html             # Rapport final (auto-généré)
├── templates/
│   └── index.html           # Interface web de l'API
└── static/
    └── visualizations/      # Graphiques générés
```

## Installation

```bash
pip install -r requirements.txt
```

## Utilisation

### 1. Générer le rapport complet (recommandé)

```bash
python generate_report.py
```

Exécute l'ensemble du pipeline (DB → Analyse → Modèle → Évaluation) et génère `rapport.html`.

### 2. Lancer l'API Flask

```bash
python app.py
```

Accéder à l'interface : [http://localhost:5000](http://localhost:5000)

### 3. Exécuter les modules individuellement

```bash
python database.py       # Créer la base SQLite
python analysis.py       # Générer les visualisations
python recommender.py    # Tester le moteur de recommandation
python evaluation.py     # Évaluer les performances
```

## API Endpoints

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| `GET` | `/api/recommendations/<user_id>?n=10` | Recommandations personnalisées |
| `GET` | `/api/similar-users/<user_id>?n=5` | Utilisateurs similaires |
| `GET` | `/api/user/<user_id>` | Profil et historique |
| `GET` | `/api/top-movies?n=20` | Films les mieux notés |
| `GET` | `/api/genres` | Statistiques par genre |
| `GET` | `/api/predict?user_id=42&movie=Movie 10` | Prédiction de note |
| `GET` | `/api/model-info` | Informations du modèle |
| `GET` | `/api/stats` | Statistiques globales |

## Technologies

- **Python 3** — Langage principal
- **pandas / NumPy** — Manipulation de données
- **scikit-learn** — Similarité cosinus, validation croisée
- **SQLite** — Base de données relationnelle
- **Flask** — API REST
- **Matplotlib / Seaborn** — Visualisations

## Métriques d'Évaluation

- **RMSE** — Root Mean Squared Error (validation croisée 5-fold)
- **MAE** — Mean Absolute Error
- **Precision@K / Recall@K** — Pertinence des recommandations
- **Couverture** — Proportion du catalogue recommandé
- **Taux d'engagement** — Prédiction de l'engagement utilisateur

## Contact
- **Auteur** — Jules COLONAS
- **Mail** — jucolonas@gmail.com
- **Portable** — 07 55 15 67 39