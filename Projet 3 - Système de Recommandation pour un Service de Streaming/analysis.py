"""
Analyse exploratoire des données (EDA) et génération des visualisations.
Produit des graphiques sauvegardés dans static/visualizations/.
"""

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import os
from database import init_database, query_sql, get_genre_stats, get_top_movies, DB_PATH

VIZ_DIR = os.path.join(os.path.dirname(__file__), "static", "visualizations")
os.makedirs(VIZ_DIR, exist_ok=True)

PALETTE = ["#E50914", "#B81D24", "#221F1F", "#F5F5F1", "#564D4A", "#831010", "#FFA500"]
sns.set_theme(style="whitegrid", palette=PALETTE, font_scale=1.1)
plt.rcParams.update({
    "figure.figsize": (10, 6),
    "axes.titlesize": 14,
    "axes.labelsize": 12,
    "figure.facecolor": "white",
    "savefig.dpi": 150,
    "savefig.bbox": "tight"
})


def load_data():
    csv_path = os.path.join(os.path.dirname(__file__), "Streaming_Usage.csv")
    df = pd.read_csv(csv_path, parse_dates=["Watch_Date"])
    df["Year"] = df["Watch_Date"].dt.year
    df["Month"] = df["Watch_Date"].dt.month
    df["YearMonth"] = df["Watch_Date"].dt.to_period("M")
    return df


def plot_rating_distribution(df):
    fig, ax = plt.subplots()
    counts = df["Rating"].value_counts().sort_index()
    bars = ax.bar(counts.index, counts.values, color=PALETTE[:5], edgecolor="white", linewidth=1.2)
    for bar, val in zip(bars, counts.values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5,
                str(val), ha="center", va="bottom", fontweight="bold", fontsize=11)
    ax.set_xlabel("Note")
    ax.set_ylabel("Nombre d'évaluations")
    ax.set_title("Distribution des Notes")
    ax.set_xticks([1, 2, 3, 4, 5])
    plt.savefig(os.path.join(VIZ_DIR, "rating_distribution.png"))
    plt.close()


def plot_genre_popularity(df):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    genre_counts = df["Genre"].value_counts()
    axes[0].pie(genre_counts.values, labels=genre_counts.index, autopct="%1.1f%%",
                colors=PALETTE, startangle=140, textprops={"fontsize": 10})
    axes[0].set_title("Répartition des Visionnages par Genre")

    genre_ratings = df.groupby("Genre")["Rating"].mean().sort_values(ascending=True)
    bars = axes[1].barh(genre_ratings.index, genre_ratings.values, color=PALETTE[:len(genre_ratings)])
    for bar, val in zip(bars, genre_ratings.values):
        axes[1].text(bar.get_width() + 0.02, bar.get_y() + bar.get_height() / 2,
                     f"{val:.2f}", va="center", fontweight="bold")
    axes[1].set_xlabel("Note Moyenne")
    axes[1].set_title("Note Moyenne par Genre")
    axes[1].set_xlim(0, 5.5)

    plt.tight_layout()
    plt.savefig(os.path.join(VIZ_DIR, "genre_analysis.png"))
    plt.close()


def plot_temporal_trends(df):
    fig, axes = plt.subplots(2, 1, figsize=(12, 10))

    monthly = df.groupby("YearMonth").size()
    monthly.index = monthly.index.astype(str)
    axes[0].plot(monthly.index, monthly.values, marker="o", color=PALETTE[0], linewidth=2)
    axes[0].fill_between(range(len(monthly)), monthly.values, alpha=0.15, color=PALETTE[0])
    axes[0].set_xticks(range(0, len(monthly), 3))
    axes[0].set_xticklabels(monthly.index[::3], rotation=45, ha="right")
    axes[0].set_ylabel("Nombre de Visionnages")
    axes[0].set_title("Évolution Mensuelle des Visionnages")

    genre_monthly = df.groupby(["YearMonth", "Genre"]).size().unstack(fill_value=0)
    genre_monthly.index = genre_monthly.index.astype(str)
    genre_monthly.plot(kind="area", stacked=True, ax=axes[1], alpha=0.7, color=PALETTE)
    axes[1].set_xticks(range(0, len(genre_monthly), 3))
    axes[1].set_xticklabels(genre_monthly.index[::3], rotation=45, ha="right")
    axes[1].set_ylabel("Nombre de Visionnages")
    axes[1].set_title("Tendance des Genres dans le Temps")
    axes[1].legend(loc="upper left", fontsize=9)

    plt.tight_layout()
    plt.savefig(os.path.join(VIZ_DIR, "temporal_trends.png"))
    plt.close()


def plot_user_activity(df):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    user_counts = df.groupby("User_ID").size()
    axes[0].hist(user_counts, bins=range(1, user_counts.max() + 2), color=PALETTE[0],
                 edgecolor="white", alpha=0.85)
    axes[0].axvline(user_counts.mean(), color=PALETTE[5], linestyle="--", linewidth=2,
                    label=f"Moyenne: {user_counts.mean():.1f}")
    axes[0].set_xlabel("Nombre de Films Vus")
    axes[0].set_ylabel("Nombre d'Utilisateurs")
    axes[0].set_title("Distribution de l'Activité des Utilisateurs")
    axes[0].legend()

    user_avg_rating = df.groupby("User_ID")["Rating"].mean()
    axes[1].hist(user_avg_rating, bins=20, color=PALETTE[1], edgecolor="white", alpha=0.85)
    axes[1].axvline(user_avg_rating.mean(), color=PALETTE[5], linestyle="--", linewidth=2,
                    label=f"Moyenne: {user_avg_rating.mean():.2f}")
    axes[1].set_xlabel("Note Moyenne Donnée")
    axes[1].set_ylabel("Nombre d'Utilisateurs")
    axes[1].set_title("Distribution des Notes Moyennes par Utilisateur")
    axes[1].legend()

    plt.tight_layout()
    plt.savefig(os.path.join(VIZ_DIR, "user_activity.png"))
    plt.close()


def plot_top_movies():
    top = get_top_movies(15)
    fig, ax = plt.subplots(figsize=(10, 7))
    colors = [PALETTE[0] if r >= 4 else PALETTE[2] for r in top["avg_rating"]]
    bars = ax.barh(top["movie_name"], top["avg_rating"], color=colors, edgecolor="white")
    for bar, n in zip(bars, top["num_ratings"]):
        ax.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height() / 2,
                f"({n} votes)", va="center", fontsize=9, color="#666")
    ax.set_xlabel("Note Moyenne")
    ax.set_title("Top 15 Films les Mieux Notés (min. 3 votes)")
    ax.set_xlim(0, 5.5)
    ax.invert_yaxis()
    plt.savefig(os.path.join(VIZ_DIR, "top_movies.png"))
    plt.close()


def plot_heatmap_genre_rating(df):
    pivot = df.pivot_table(index="Genre", columns="Rating", values="User_ID", aggfunc="count", fill_value=0)
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.heatmap(pivot, annot=True, fmt="d", cmap="Reds", ax=ax, linewidths=0.5)
    ax.set_title("Heatmap : Distribution des Notes par Genre")
    ax.set_xlabel("Note")
    ax.set_ylabel("Genre")
    plt.savefig(os.path.join(VIZ_DIR, "heatmap_genre_rating.png"))
    plt.close()


def plot_engagement_metrics(df):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    user_counts = df.groupby("User_ID").size()
    engaged = (user_counts >= 3).sum()
    casual = (user_counts < 3).sum()
    axes[0].pie([engaged, casual], labels=["Engagés (≥3 films)", "Occasionnels (<3 films)"],
                autopct="%1.1f%%", colors=[PALETTE[0], PALETTE[4]], startangle=90,
                textprops={"fontsize": 12, "fontweight": "bold"})
    axes[0].set_title("Segmentation des Utilisateurs par Engagement")

    high_rated = df[df["Rating"] >= 4].groupby("Genre").size()
    total = df.groupby("Genre").size()
    satisfaction = (high_rated / total * 100).sort_values(ascending=True)
    bars = axes[1].barh(satisfaction.index, satisfaction.values, color=PALETTE[0])
    for bar, val in zip(bars, satisfaction.values):
        axes[1].text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                     f"{val:.1f}%", va="center", fontweight="bold", fontsize=10)
    axes[1].set_xlabel("% de Notes ≥ 4")
    axes[1].set_title("Taux de Satisfaction par Genre")
    axes[1].set_xlim(0, 100)

    plt.tight_layout()
    plt.savefig(os.path.join(VIZ_DIR, "engagement_metrics.png"))
    plt.close()


def generate_summary_stats(df):
    """Retourne un dictionnaire de statistiques clés."""
    return {
        "total_ratings": len(df),
        "unique_users": df["User_ID"].nunique(),
        "unique_movies": df["Movie_Name"].nunique(),
        "unique_genres": df["Genre"].nunique(),
        "avg_rating": round(df["Rating"].mean(), 2),
        "median_rating": df["Rating"].median(),
        "ratings_per_user": round(len(df) / df["User_ID"].nunique(), 2),
        "date_range": f"{df['Watch_Date'].min().date()} → {df['Watch_Date'].max().date()}",
        "most_popular_genre": df["Genre"].value_counts().index[0],
        "highest_rated_genre": df.groupby("Genre")["Rating"].mean().idxmax(),
    }


def run_full_analysis():
    """Lance l'analyse complète et génère toutes les visualisations."""
    if not os.path.exists(DB_PATH):
        init_database()

    df = load_data()
    stats = generate_summary_stats(df)

    print("=" * 60)
    print("  ANALYSE EXPLORATOIRE DES DONNÉES - STREAMING")
    print("=" * 60)
    for k, v in stats.items():
        print(f"  {k:25s}: {v}")
    print("=" * 60)

    print("\nGénération des visualisations...")
    plot_rating_distribution(df)
    print("  ✓ Distribution des notes")
    plot_genre_popularity(df)
    print("  ✓ Analyse des genres")
    plot_temporal_trends(df)
    print("  ✓ Tendances temporelles")
    plot_user_activity(df)
    print("  ✓ Activité des utilisateurs")
    plot_top_movies()
    print("  ✓ Top films")
    plot_heatmap_genre_rating(df)
    print("  ✓ Heatmap genres/notes")
    plot_engagement_metrics(df)
    print("  ✓ Métriques d'engagement")

    print(f"\nVisualisations sauvegardées dans : {VIZ_DIR}")
    return stats


if __name__ == "__main__":
    run_full_analysis()
