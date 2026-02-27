"""
Module d'évaluation du système de recommandation.
Métriques : RMSE, MAE, Precision@K, Recall@K, Coverage, Taux d'engagement.
Génère des visualisations des performances.
"""

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import KFold
from sklearn.metrics import mean_squared_error, mean_absolute_error
import os

from database import get_user_movie_matrix, init_database, DB_PATH
from recommender import CollaborativeRecommender

VIZ_DIR = os.path.join(os.path.dirname(__file__), "static", "visualizations")
os.makedirs(VIZ_DIR, exist_ok=True)

PALETTE = ["#E50914", "#B81D24", "#221F1F", "#564D4A", "#FFA500"]
sns.set_theme(style="whitegrid", font_scale=1.1)


def evaluate_rmse_mae(matrix, n_splits=5):
    """Évalue RMSE et MAE via validation croisée sur les notes connues."""
    ratings = []
    for user_idx in range(matrix.shape[0]):
        for movie_idx in range(matrix.shape[1]):
            if matrix.values[user_idx, movie_idx] > 0:
                ratings.append((user_idx, movie_idx, matrix.values[user_idx, movie_idx]))

    ratings = np.array(ratings)
    kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)

    rmse_scores = []
    mae_scores = []

    for train_idx, test_idx in kf.split(ratings):
        train_data = ratings[train_idx]
        test_data = ratings[test_idx]

        train_matrix = np.zeros_like(matrix.values)
        for u, m, r in train_data:
            train_matrix[int(u), int(m)] = r

        train_df = pd.DataFrame(train_matrix, index=matrix.index, columns=matrix.columns)
        rec = CollaborativeRecommender()
        rec.fit(train_df)

        y_true = []
        y_pred = []

        for u, m, r in test_data:
            user_id = matrix.index[int(u)]
            movie_name = matrix.columns[int(m)]
            pred = rec.predict_rating(user_id, movie_name)
            if pred is not None:
                y_true.append(r)
                y_pred.append(pred)

        if y_true:
            rmse_scores.append(np.sqrt(mean_squared_error(y_true, y_pred)))
            mae_scores.append(mean_absolute_error(y_true, y_pred))

    return {
        "rmse_mean": round(np.mean(rmse_scores), 4),
        "rmse_std": round(np.std(rmse_scores), 4),
        "mae_mean": round(np.mean(mae_scores), 4),
        "mae_std": round(np.std(mae_scores), 4),
    }


def evaluate_precision_recall_at_k(matrix, k_values=[5, 10, 15], threshold=3.0):
    """
    Calcule Precision@K et Recall@K via un split train/test.
    On masque une partie des notes positives et vérifie si le modèle les retrouve.
    """
    results = {}
    np.random.seed(42)

    users_with_enough = []
    for user_id in matrix.index:
        rated = (matrix.loc[user_id] > 0).sum()
        positive = (matrix.loc[user_id] >= threshold).sum()
        if rated >= 3 and positive >= 1:
            users_with_enough.append(user_id)

    sample_size = min(50, len(users_with_enough))
    sampled_users = list(np.random.choice(users_with_enough, size=sample_size, replace=False))

    for k in k_values:
        precisions = []
        recalls = []

        for user_id in sampled_users:
            user_ratings = matrix.loc[user_id]
            positive_movies = user_ratings[user_ratings >= threshold].index.tolist()

            if not positive_movies:
                continue

            n_hide = max(1, len(positive_movies) // 2)
            hidden = list(np.random.choice(positive_movies, size=min(n_hide, len(positive_movies)), replace=False))

            train_matrix = matrix.copy()
            for movie in hidden:
                train_matrix.at[user_id, movie] = 0

            rec = CollaborativeRecommender()
            rec.fit(train_matrix)
            recommendations = rec.recommend(user_id, n=k)

            if recommendations.empty:
                continue

            recommended_set = set(recommendations["movie_name"].tolist())
            hidden_set = set(hidden)
            hits = recommended_set.intersection(hidden_set)

            precisions.append(len(hits) / k)
            recalls.append(len(hits) / len(hidden_set) if hidden_set else 0)

        results[k] = {
            "precision": round(np.mean(precisions), 4) if precisions else 0,
            "recall": round(np.mean(recalls), 4) if recalls else 0,
        }

    return results


def evaluate_coverage(matrix, n=10):
    """Calcule le taux de couverture du catalogue."""
    rec = CollaborativeRecommender()
    rec.fit(matrix)

    all_recommended = set()
    for user_id in matrix.index:
        recs = rec.recommend(user_id, n=n)
        if not recs.empty:
            all_recommended.update(recs["movie_name"].tolist())

    coverage = len(all_recommended) / len(matrix.columns)
    return round(coverage, 4)


def evaluate_engagement_rate(matrix, threshold=3.5):
    """Simule le taux d'engagement basé sur les recommandations."""
    rec = CollaborativeRecommender()
    rec.fit(matrix)

    engaged = 0
    total = 0

    for user_id in matrix.index:
        recs = rec.recommend(user_id, n=5)
        if recs.empty:
            continue

        avg_predicted = recs["predicted_rating"].mean()
        if avg_predicted >= threshold:
            engaged += 1
        total += 1

    return round(engaged / total, 4) if total > 0 else 0


def plot_evaluation_results(rmse_mae, pk_results, coverage, engagement):
    """Génère les graphiques de performance du modèle."""

    fig, axes = plt.subplots(2, 2, figsize=(14, 11))

    metrics = ["RMSE", "MAE"]
    values = [rmse_mae["rmse_mean"], rmse_mae["mae_mean"]]
    errors = [rmse_mae["rmse_std"], rmse_mae["mae_std"]]
    bars = axes[0, 0].bar(metrics, values, yerr=errors, color=[PALETTE[0], PALETTE[1]],
                          edgecolor="white", capsize=8, linewidth=1.2)
    for bar, val in zip(bars, values):
        axes[0, 0].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                        f"{val:.3f}", ha="center", fontweight="bold", fontsize=12)
    axes[0, 0].set_title("Erreur de Prédiction (Validation Croisée 5-fold)")
    axes[0, 0].set_ylabel("Score")
    axes[0, 0].set_ylim(0, max(values) * 1.5)

    k_vals = sorted(pk_results.keys())
    prec = [pk_results[k]["precision"] for k in k_vals]
    rec = [pk_results[k]["recall"] for k in k_vals]
    x = np.arange(len(k_vals))
    width = 0.35
    axes[0, 1].bar(x - width / 2, prec, width, label="Precision@K", color=PALETTE[0])
    axes[0, 1].bar(x + width / 2, rec, width, label="Recall@K", color=PALETTE[4])
    axes[0, 1].set_xticks(x)
    axes[0, 1].set_xticklabels([f"K={k}" for k in k_vals])
    axes[0, 1].set_title("Precision@K et Recall@K")
    axes[0, 1].set_ylabel("Score")
    axes[0, 1].legend()
    axes[0, 1].set_ylim(0, 1.0)

    categories = ["Couverture\nCatalogue", "Taux\nd'Engagement"]
    cat_values = [coverage * 100, engagement * 100]
    colors = [PALETTE[0] if v > 50 else PALETTE[3] for v in cat_values]
    bars = axes[1, 0].bar(categories, cat_values, color=colors, edgecolor="white", width=0.5)
    for bar, val in zip(bars, cat_values):
        axes[1, 0].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                        f"{val:.1f}%", ha="center", fontweight="bold", fontsize=13)
    axes[1, 0].set_title("Couverture et Engagement")
    axes[1, 0].set_ylabel("Pourcentage (%)")
    axes[1, 0].set_ylim(0, 110)
    axes[1, 0].axhline(y=50, color="gray", linestyle="--", alpha=0.5)

    baseline_rmse = 1.5
    model_rmse = rmse_mae["rmse_mean"]
    improvement = max(0, (baseline_rmse - model_rmse) / baseline_rmse * 100)
    sizes = [improvement, 100 - improvement]
    colors_pie = [PALETTE[0], "#E0E0E0"]
    wedges, texts, autotexts = axes[1, 1].pie(
        sizes, colors=colors_pie, autopct="%1.1f%%", startangle=90,
        textprops={"fontsize": 14, "fontweight": "bold"}
    )
    axes[1, 1].set_title(f"Amélioration vs. Baseline\n(RMSE Baseline: {baseline_rmse})")

    plt.suptitle("Performances du Système de Recommandation", fontsize=16, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(VIZ_DIR, "model_performance.png"))
    plt.close()


def run_full_evaluation():
    """Lance l'évaluation complète du modèle."""
    if not os.path.exists(DB_PATH):
        init_database()

    matrix = get_user_movie_matrix()

    print("=" * 60)
    print("  ÉVALUATION DU SYSTÈME DE RECOMMANDATION")
    print("=" * 60)

    print("\n1. Calcul RMSE & MAE (validation croisée)...")
    rmse_mae = evaluate_rmse_mae(matrix)
    print(f"   RMSE: {rmse_mae['rmse_mean']} ± {rmse_mae['rmse_std']}")
    print(f"   MAE:  {rmse_mae['mae_mean']} ± {rmse_mae['mae_std']}")

    print("\n2. Calcul Precision@K & Recall@K...")
    pk_results = evaluate_precision_recall_at_k(matrix)
    for k, vals in pk_results.items():
        print(f"   K={k:2d} → Precision: {vals['precision']:.4f}, Recall: {vals['recall']:.4f}")

    print("\n3. Calcul de la couverture du catalogue...")
    coverage = evaluate_coverage(matrix)
    print(f"   Couverture: {coverage:.1%}")

    print("\n4. Calcul du taux d'engagement...")
    engagement = evaluate_engagement_rate(matrix)
    print(f"   Taux d'engagement: {engagement:.1%}")

    print("\n5. Génération des graphiques de performance...")
    plot_evaluation_results(rmse_mae, pk_results, coverage, engagement)
    print("   ✓ Graphiques sauvegardés")

    print("\n" + "=" * 60)

    return {
        "rmse_mae": rmse_mae,
        "precision_recall": pk_results,
        "coverage": coverage,
        "engagement_rate": engagement,
    }


if __name__ == "__main__":
    results = run_full_evaluation()
