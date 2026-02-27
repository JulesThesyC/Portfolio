"""
Génère le rapport HTML professionnel avec toutes les visualisations et métriques.
Ce script orchestre l'ensemble du pipeline : DB → Analyse → Modèle → Évaluation → Rapport.
"""

import os
import base64
from datetime import datetime

from database import init_database, DB_PATH
from analysis import run_full_analysis
from evaluation import run_full_evaluation
from recommender import build_recommender

REPORT_PATH = os.path.join(os.path.dirname(__file__), "rapport.html")
VIZ_DIR = os.path.join(os.path.dirname(__file__), "static", "visualizations")


def img_to_base64(filename):
    path = os.path.join(VIZ_DIR, filename)
    if not os.path.exists(path):
        return ""
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def generate_report():
    print("=" * 60)
    print("  GÉNÉRATION DU RAPPORT COMPLET")
    print("=" * 60)

    # Pipeline complet
    if not os.path.exists(DB_PATH):
        print("\n> Initialisation de la base de donnees...")
        init_database()

    print("\n> Analyse exploratoire...")
    stats = run_full_analysis()

    print("\n> Construction du modele...")
    rec = build_recommender()
    model_info = rec.get_model_info()

    print("\n> Evaluation du modele...")
    eval_results = run_full_evaluation()

    # Exemples de recommandations
    sample_user = rec.user_ids[0]
    sample_recs = rec.recommend(sample_user, n=5)
    sample_similar = rec.get_similar_users(sample_user, n=5)

    # Images en base64
    images = {
        "rating_dist": img_to_base64("rating_distribution.png"),
        "genre": img_to_base64("genre_analysis.png"),
        "temporal": img_to_base64("temporal_trends.png"),
        "user_activity": img_to_base64("user_activity.png"),
        "top_movies": img_to_base64("top_movies.png"),
        "heatmap": img_to_base64("heatmap_genre_rating.png"),
        "engagement": img_to_base64("engagement_metrics.png"),
        "performance": img_to_base64("model_performance.png"),
    }

    rmse = eval_results["rmse_mae"]
    pk = eval_results["precision_recall"]

    reco_rows = ""
    for _, r in sample_recs.iterrows():
        stars = "★" * round(r["predicted_rating"]) + "☆" * (5 - round(r["predicted_rating"]))
        reco_rows += f"""
        <tr>
            <td>{r['movie_name']}</td>
            <td><span class="stars">{stars}</span> {r['predicted_rating']}</td>
            <td>
                <div class="conf-bar"><div class="conf-fill" style="width:{r['confidence']*100}%"></div></div>
                {r['confidence']*100:.0f}%
            </td>
        </tr>"""

    similar_rows = ""
    for _, s in sample_similar.iterrows():
        similar_rows += f"<tr><td>#{int(s['user_id'])}</td><td>{s['similarity']:.4f}</td></tr>"

    pk5 = pk.get(5, {"precision": 0, "recall": 0})
    pk10 = pk.get(10, {"precision": 0, "recall": 0})
    pk15 = pk.get(15, {"precision": 0, "recall": 0})

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Rapport — Système de Recommandation Streaming</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        :root {{
            --red: #E50914;
            --dark: #1a1a2e;
            --bg: #f8f9fa;
            --card: #ffffff;
            --text: #2d3436;
            --muted: #636e72;
            --border: #e0e0e0;
            --green: #00b894;
            --orange: #fdcb6e;
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Inter', sans-serif; background: var(--bg); color: var(--text); line-height: 1.7; }}

        .cover {{
            background: linear-gradient(135deg, var(--dark), #16213e, #0f3460);
            color: white;
            padding: 4rem 2rem;
            text-align: center;
        }}
        .cover h1 {{ font-size: 2.8rem; font-weight: 800; margin-bottom: 0.5rem; }}
        .cover h1 span {{ color: var(--red); }}
        .cover .subtitle {{ font-size: 1.2rem; font-weight: 300; opacity: 0.9; margin-bottom: 1rem; }}
        .cover .meta {{ font-size: 0.9rem; opacity: 0.7; }}

        .container {{ max-width: 1000px; margin: 0 auto; padding: 2rem; }}

        .section {{
            background: var(--card);
            border-radius: 16px;
            padding: 2rem;
            margin-bottom: 2rem;
            box-shadow: 0 2px 12px rgba(0,0,0,0.06);
            border: 1px solid var(--border);
        }}

        .section h2 {{
            font-size: 1.5rem;
            font-weight: 700;
            color: var(--dark);
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 3px solid var(--red);
            display: inline-block;
        }}

        .section h3 {{ font-size: 1.1rem; font-weight: 600; margin: 1.2rem 0 0.5rem; color: var(--dark); }}

        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 1rem;
            margin: 1.5rem 0;
        }}

        .kpi {{
            background: linear-gradient(135deg, var(--dark), #16213e);
            color: white;
            padding: 1.2rem;
            border-radius: 12px;
            text-align: center;
        }}
        .kpi .value {{ font-size: 1.8rem; font-weight: 800; color: var(--red); }}
        .kpi .label {{ font-size: 0.8rem; opacity: 0.8; margin-top: 0.3rem; }}

        .viz {{ text-align: center; margin: 1.5rem 0; }}
        .viz img {{ max-width: 100%; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.1); }}
        .viz .caption {{ font-size: 0.85rem; color: var(--muted); margin-top: 0.5rem; font-style: italic; }}

        table {{ width: 100%; border-collapse: collapse; margin: 1rem 0; }}
        th, td {{ padding: 0.6rem 1rem; text-align: left; border-bottom: 1px solid var(--border); }}
        th {{ font-size: 0.8rem; text-transform: uppercase; color: var(--muted); font-weight: 600; }}
        tr:hover {{ background: #f0f0f0; }}

        .stars {{ color: #e17055; letter-spacing: 1px; }}
        .conf-bar {{ display: inline-block; width: 80px; height: 6px; background: #eee; border-radius: 3px; vertical-align: middle; margin-right: 6px; }}
        .conf-fill {{ height: 100%; background: var(--green); border-radius: 3px; }}

        .metric-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin: 1rem 0;
        }}

        .metric-card {{
            border: 2px solid var(--border);
            border-radius: 12px;
            padding: 1rem;
            text-align: center;
        }}
        .metric-card .metric-value {{ font-size: 1.6rem; font-weight: 700; color: var(--red); }}
        .metric-card .metric-label {{ font-size: 0.85rem; color: var(--muted); }}

        .highlight {{ background: #fff3e0; border-left: 4px solid var(--orange); padding: 1rem; border-radius: 0 8px 8px 0; margin: 1rem 0; }}

        .code-block {{
            background: var(--dark);
            color: #dfe6e9;
            padding: 1rem;
            border-radius: 8px;
            font-family: 'Courier New', monospace;
            font-size: 0.85rem;
            overflow-x: auto;
            margin: 0.5rem 0;
        }}

        .cta {{
            background: linear-gradient(135deg, var(--red), #b71c1c);
            color: white;
            padding: 2.5rem;
            border-radius: 16px;
            text-align: center;
        }}
        .cta h2 {{ color: white; border-bottom: none; font-size: 1.6rem; }}
        .cta p {{ opacity: 0.95; margin-top: 0.5rem; font-size: 1.05rem; }}

        ul {{ padding-left: 1.5rem; margin: 0.5rem 0; }}
        li {{ margin-bottom: 0.3rem; }}

        @media print {{
            .section {{ break-inside: avoid; }}
            body {{ font-size: 11pt; }}
        }}
    </style>
</head>
<body>

<!-- COUVERTURE -->
<div class="cover">
    <h1>Système de <span>Recommandation</span><br>pour un Service de Streaming</h1>
    <div class="subtitle">Amélioration de l'engagement des utilisateurs par filtrage collaboratif</div>
    <div class="meta">
        Projet Data Engineering | Python, pandas, scikit-learn, SQL, Flask<br>
        Généré le {datetime.now().strftime("%d/%m/%Y à %H:%M")}
    </div>
</div>

<div class="container">

<!-- 1. RÉSUMÉ EXÉCUTIF -->
<div class="section">
    <h2>1. Résumé Exécutif</h2>
    <p>Ce projet implémente un <strong>système de recommandation collaboratif</strong> (User-Based Collaborative Filtering) pour un service de streaming vidéo. En analysant les comportements de <strong>{stats['unique_users']} utilisateurs</strong> sur <strong>{stats['unique_movies']} films</strong> répartis en <strong>{stats['unique_genres']} genres</strong>, le système identifie les similarités entre utilisateurs pour générer des recommandations personnalisées.</p>

    <div class="kpi-grid">
        <div class="kpi"><div class="value">{stats['total_ratings']}</div><div class="label">Évaluations</div></div>
        <div class="kpi"><div class="value">{stats['unique_users']}</div><div class="label">Utilisateurs</div></div>
        <div class="kpi"><div class="value">{stats['unique_movies']}</div><div class="label">Films</div></div>
        <div class="kpi"><div class="value">{stats['avg_rating']}</div><div class="label">Note Moyenne</div></div>
        <div class="kpi"><div class="value">{rmse['rmse_mean']}</div><div class="label">RMSE</div></div>
        <div class="kpi"><div class="value">{eval_results['coverage']:.0%}</div><div class="label">Couverture</div></div>
    </div>

    <div class="highlight">
        <strong>Points clés :</strong>
        <ul>
            <li>Pipeline complet : ingestion CSV → base SQL → modèle ML → API REST</li>
            <li>RMSE de <strong>{rmse['rmse_mean']}</strong> (±{rmse['rmse_std']}) via validation croisée 5-fold</li>
            <li>Couverture catalogue de <strong>{eval_results['coverage']:.0%}</strong></li>
            <li>Taux d'engagement simulé de <strong>{eval_results['engagement_rate']:.0%}</strong></li>
        </ul>
    </div>
</div>

<!-- 2. ARCHITECTURE TECHNIQUE -->
<div class="section">
    <h2>2. Architecture Technique</h2>
    <h3>Stack Technologique</h3>
    <ul>
        <li><strong>Python 3</strong> — Langage principal</li>
        <li><strong>pandas / NumPy</strong> — Manipulation et analyse de données</li>
        <li><strong>scikit-learn</strong> — Similarité cosinus, validation croisée, métriques</li>
        <li><strong>SQLite / SQL</strong> — Base de données relationnelle normalisée</li>
        <li><strong>Flask</strong> — API REST et interface web</li>
        <li><strong>Matplotlib / Seaborn</strong> — Visualisations</li>
    </ul>

    <h3>Schéma de la Base de Données</h3>
    <div class="code-block">
-- Tables normalisées (3NF)<br>
genres (genre_id PK, genre_name)<br>
users (user_id PK)<br>
movies (movie_id PK, movie_name)<br>
ratings (rating_id PK, user_id FK, movie_id FK, genre_id FK, rating, watch_date)
    </div>

    <h3>Pipeline de Données</h3>
    <div class="code-block">
CSV (Streaming_Usage.csv)<br>
&nbsp;&nbsp;→ Nettoyage &amp; Validation (pandas)<br>
&nbsp;&nbsp;→ Base SQLite normalisée (database.py)<br>
&nbsp;&nbsp;→ Matrice User-Item (SQL pivot)<br>
&nbsp;&nbsp;→ Similarité Cosinus (scikit-learn)<br>
&nbsp;&nbsp;→ Recommandations (recommender.py)<br>
&nbsp;&nbsp;→ API REST (Flask app.py)
    </div>
</div>

<!-- 3. ANALYSE EXPLORATOIRE -->
<div class="section">
    <h2>3. Analyse Exploratoire des Données</h2>

    <h3>3.1 Vue d'ensemble du dataset</h3>
    <table>
        <tr><th>Métrique</th><th>Valeur</th></tr>
        <tr><td>Total évaluations</td><td>{stats['total_ratings']}</td></tr>
        <tr><td>Utilisateurs uniques</td><td>{stats['unique_users']}</td></tr>
        <tr><td>Films uniques</td><td>{stats['unique_movies']}</td></tr>
        <tr><td>Genres</td><td>{stats['unique_genres']}</td></tr>
        <tr><td>Note moyenne</td><td>{stats['avg_rating']}</td></tr>
        <tr><td>Note médiane</td><td>{stats['median_rating']}</td></tr>
        <tr><td>Films par utilisateur (moy.)</td><td>{stats['ratings_per_user']}</td></tr>
        <tr><td>Période</td><td>{stats['date_range']}</td></tr>
        <tr><td>Genre le plus populaire</td><td>{stats['most_popular_genre']}</td></tr>
        <tr><td>Genre le mieux noté</td><td>{stats['highest_rated_genre']}</td></tr>
    </table>

    <h3>3.2 Distribution des Notes</h3>
    <div class="viz">
        <img src="data:image/png;base64,{images['rating_dist']}" alt="Distribution des notes">
        <div class="caption">Figure 1 — Les notes sont réparties de manière relativement uniforme (données générées aléatoirement).</div>
    </div>

    <h3>3.3 Analyse des Genres</h3>
    <div class="viz">
        <img src="data:image/png;base64,{images['genre']}" alt="Analyse des genres">
        <div class="caption">Figure 2 — Répartition des visionnages et note moyenne par genre.</div>
    </div>

    <h3>3.4 Tendances Temporelles</h3>
    <div class="viz">
        <img src="data:image/png;base64,{images['temporal']}" alt="Tendances temporelles">
        <div class="caption">Figure 3 — Évolution mensuelle des visionnages et tendances par genre.</div>
    </div>

    <h3>3.5 Activité des Utilisateurs</h3>
    <div class="viz">
        <img src="data:image/png;base64,{images['user_activity']}" alt="Activité utilisateurs">
        <div class="caption">Figure 4 — Distribution de l'activité et des notes moyennes par utilisateur.</div>
    </div>

    <h3>3.6 Films les Mieux Notés</h3>
    <div class="viz">
        <img src="data:image/png;base64,{images['top_movies']}" alt="Top films">
        <div class="caption">Figure 5 — Top 15 des films avec le meilleur score moyen (minimum 3 votes).</div>
    </div>

    <h3>3.7 Heatmap Genres / Notes</h3>
    <div class="viz">
        <img src="data:image/png;base64,{images['heatmap']}" alt="Heatmap">
        <div class="caption">Figure 6 — Distribution croisée des notes par genre.</div>
    </div>
</div>

<!-- 4. MODÈLE DE RECOMMANDATION -->
<div class="section">
    <h2>4. Modèle de Recommandation</h2>

    <h3>4.1 Approche : Filtrage Collaboratif User-Based</h3>
    <p>L'algorithme repose sur l'hypothèse que des utilisateurs ayant des goûts similaires dans le passé continueront à partager des préférences similaires.</p>
    <ul>
        <li><strong>Étape 1 :</strong> Construction de la matrice User-Item (utilisateurs × films)</li>
        <li><strong>Étape 2 :</strong> Calcul de la similarité cosinus entre chaque paire d'utilisateurs</li>
        <li><strong>Étape 3 :</strong> Pour un utilisateur cible, identification des K voisins les plus similaires</li>
        <li><strong>Étape 4 :</strong> Prédiction des notes pour les films non vus, pondérées par la similarité</li>
        <li><strong>Étape 5 :</strong> Classement et filtrage des meilleures recommandations</li>
    </ul>

    <div class="code-block">
Formule de prédiction :<br><br>
pred(u, i) = Σ(sim(u, v) × rating(v, i)) / Σ|sim(u, v)|<br><br>
où v ∈ voisins ayant noté le film i
    </div>

    <h3>4.2 Caractéristiques du Modèle</h3>
    <table>
        <tr><th>Paramètre</th><th>Valeur</th></tr>
        <tr><td>Taille de la matrice</td><td>{model_info['matrix_shape'][0]} × {model_info['matrix_shape'][1]}</td></tr>
        <tr><td>Sparsité</td><td>{model_info['sparsity']}</td></tr>
        <tr><td>Similarité moyenne</td><td>{model_info['avg_similarity']}</td></tr>
        <tr><td>Métrique de similarité</td><td>Cosinus</td></tr>
        <tr><td>Seuil min. de similarité</td><td>0.1</td></tr>
        <tr><td>Gestion cold-start</td><td>Recommandations par popularité</td></tr>
    </table>

    <h3>4.3 Exemple de Recommandations</h3>
    <p>Recommandations pour l'utilisateur <strong>#{sample_user}</strong> :</p>
    <table>
        <thead><tr><th>Film</th><th>Note Prédite</th><th>Confiance</th></tr></thead>
        <tbody>{reco_rows}</tbody>
    </table>

    <h3>4.4 Utilisateurs Similaires</h3>
    <p>Top 5 des utilisateurs les plus similaires à <strong>#{sample_user}</strong> :</p>
    <table>
        <thead><tr><th>Utilisateur</th><th>Similarité (cosinus)</th></tr></thead>
        <tbody>{similar_rows}</tbody>
    </table>
</div>

<!-- 5. ÉVALUATION -->
<div class="section">
    <h2>5. Évaluation du Modèle</h2>

    <h3>5.1 Métriques d'Erreur</h3>
    <div class="metric-grid">
        <div class="metric-card">
            <div class="metric-value">{rmse['rmse_mean']}</div>
            <div class="metric-label">RMSE (±{rmse['rmse_std']})</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">{rmse['mae_mean']}</div>
            <div class="metric-label">MAE (±{rmse['mae_std']})</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">{eval_results['coverage']:.0%}</div>
            <div class="metric-label">Couverture Catalogue</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">{eval_results['engagement_rate']:.0%}</div>
            <div class="metric-label">Taux d'Engagement</div>
        </div>
    </div>

    <h3>5.2 Precision@K et Recall@K</h3>
    <table>
        <thead><tr><th>K</th><th>Precision@K</th><th>Recall@K</th></tr></thead>
        <tbody>
            <tr><td>5</td><td>{pk5['precision']:.4f}</td><td>{pk5['recall']:.4f}</td></tr>
            <tr><td>10</td><td>{pk10['precision']:.4f}</td><td>{pk10['recall']:.4f}</td></tr>
            <tr><td>15</td><td>{pk15['precision']:.4f}</td><td>{pk15['recall']:.4f}</td></tr>
        </tbody>
    </table>

    <h3>5.3 Visualisation des Performances</h3>
    <div class="viz">
        <img src="data:image/png;base64,{images['performance']}" alt="Performance du modèle">
        <div class="caption">Figure 7 — Tableau de bord des performances : erreur, precision/recall, couverture et amélioration.</div>
    </div>

    <h3>5.4 Métriques d'Engagement</h3>
    <div class="viz">
        <img src="data:image/png;base64,{images['engagement']}" alt="Engagement">
        <div class="caption">Figure 8 — Segmentation des utilisateurs et taux de satisfaction par genre.</div>
    </div>

    <div class="highlight">
        <strong>Interprétation :</strong>
        <ul>
            <li>Le RMSE de {rmse['rmse_mean']} indique une erreur moyenne de ~{rmse['rmse_mean']} étoiles sur les prédictions</li>
            <li>La couverture de {eval_results['coverage']:.0%} montre que le modèle peut recommander une large portion du catalogue</li>
            <li>Le taux d'engagement de {eval_results['engagement_rate']:.0%} suggère que les recommandations sont perçues positivement</li>
        </ul>
    </div>
</div>

<!-- 6. API REST -->
<div class="section">
    <h2>6. API Flask — Documentation</h2>
    <p>L'API expose le système de recommandation via des endpoints REST, permettant l'intégration dans n'importe quelle application frontend.</p>

    <table>
        <thead><tr><th>Méthode</th><th>Endpoint</th><th>Description</th></tr></thead>
        <tbody>
            <tr><td><code>GET</code></td><td>/api/recommendations/&lt;user_id&gt;</td><td>Recommandations personnalisées</td></tr>
            <tr><td><code>GET</code></td><td>/api/similar-users/&lt;user_id&gt;</td><td>Utilisateurs similaires</td></tr>
            <tr><td><code>GET</code></td><td>/api/user/&lt;user_id&gt;</td><td>Profil et historique</td></tr>
            <tr><td><code>GET</code></td><td>/api/top-movies</td><td>Films les mieux notés</td></tr>
            <tr><td><code>GET</code></td><td>/api/genres</td><td>Statistiques par genre</td></tr>
            <tr><td><code>GET</code></td><td>/api/predict?user_id=X&amp;movie=Y</td><td>Prédiction de note</td></tr>
            <tr><td><code>GET</code></td><td>/api/model-info</td><td>Infos du modèle</td></tr>
            <tr><td><code>GET</code></td><td>/api/stats</td><td>Statistiques globales</td></tr>
        </tbody>
    </table>

    <h3>Exemple d'appel</h3>
    <div class="code-block">
GET /api/recommendations/42?n=5<br><br>
Response:<br>
{{"user_id": 42, "count": 5, "recommendations": [<br>
&nbsp;&nbsp;{{"movie_name": "Movie 87", "predicted_rating": 4.21, "confidence": 0.8}},<br>
&nbsp;&nbsp;{{"movie_name": "Movie 15", "predicted_rating": 3.95, "confidence": 0.6}},<br>
&nbsp;&nbsp;...<br>
]}}
    </div>

    <h3>Démarrage</h3>
    <div class="code-block">
pip install -r requirements.txt<br>
python app.py<br>
# → http://localhost:5000
    </div>
</div>

<!-- 7. CONCLUSIONS & AMÉLIORATIONS -->
<div class="section">
    <h2>7. Conclusions et Axes d'Amélioration</h2>

    <h3>Résultats Obtenus</h3>
    <ul>
        <li>Pipeline de données complet : CSV → SQL → Modèle → API</li>
        <li>Système de recommandation fonctionnel avec filtrage collaboratif</li>
        <li>API REST prête pour l'intégration</li>
        <li>Évaluation rigoureuse avec validation croisée et métriques standard</li>
        <li>Interface web interactive pour la démonstration</li>
    </ul>

    <h3>Axes d'Amélioration</h3>
    <ul>
        <li><strong>Hybridation :</strong> Combiner filtrage collaboratif et content-based (NLP sur descriptions de films)</li>
        <li><strong>Matrix Factorization :</strong> Implémenter SVD/NMF pour mieux gérer la sparsité</li>
        <li><strong>Deep Learning :</strong> Modèles neuronaux (autoencoders, NCF) pour capturer des patterns non-linéaires</li>
        <li><strong>Données réelles :</strong> Intégrer des données de navigation (temps de visionnage, clics, scrolls)</li>
        <li><strong>A/B Testing :</strong> Framework de test pour mesurer l'impact en production</li>
        <li><strong>Scalabilité :</strong> Migration vers PostgreSQL + cache Redis pour la production</li>
    </ul>
</div>

<!-- CALL TO ACTION -->
<div class="section cta">
    <h2>Discutons de ce Projet</h2>
    <p>Ce système de recommandation démontre ma capacité à construire des pipelines de données complets,<br>
    de l'ingestion à la mise en production via une API REST.</p>
    <p style="margin-top: 1rem; font-weight: 600;">Je serais ravi d'échanger sur les choix techniques, les résultats obtenus,<br>
    et comment cette approche peut s'adapter à votre contexte métier.</p>
    <p style="margin-top: 1.5rem; opacity: 0.8; font-size: 0.9rem;">
        Technologies : Python | pandas | scikit-learn | SQL | Flask | Git
    </p>
</div>

</div>

</body>
</html>"""

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n[OK] Rapport genere : {REPORT_PATH}")
    return REPORT_PATH


if __name__ == "__main__":
    generate_report()
