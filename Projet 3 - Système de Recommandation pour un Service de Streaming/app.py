"""
API Flask pour le système de recommandation de streaming.
Endpoints REST + interface web interactive.
"""

from flask import Flask, jsonify, request, render_template
import os
from database import (
    init_database, get_user_preferences, get_top_movies,
    get_genre_stats, query_sql, DB_PATH
)
from recommender import CollaborativeRecommender
from analysis import run_full_analysis
from evaluation import run_full_evaluation

app = Flask(__name__)

recommender = None


def get_recommender():
    global recommender
    if recommender is None:
        if not os.path.exists(DB_PATH):
            init_database()
        recommender = CollaborativeRecommender()
        recommender.fit()
    return recommender


# ─── Pages Web ────────────────────────────────────────────────

@app.route("/")
def index():
    rec = get_recommender()
    info = rec.get_model_info()
    genre_stats = get_genre_stats().to_dict("records")
    top_movies = get_top_movies(10).to_dict("records")
    return render_template("index.html", info=info, genre_stats=genre_stats, top_movies=top_movies)


# ─── API REST ─────────────────────────────────────────────────

@app.route("/api/recommendations/<int:user_id>")
def api_recommendations(user_id):
    """Retourne les recommandations pour un utilisateur."""
    n = request.args.get("n", 10, type=int)
    rec = get_recommender()
    recommendations = rec.recommend(user_id, n=n)
    return jsonify({
        "user_id": user_id,
        "count": len(recommendations),
        "recommendations": recommendations.to_dict("records")
    })


@app.route("/api/similar-users/<int:user_id>")
def api_similar_users(user_id):
    """Retourne les utilisateurs similaires."""
    n = request.args.get("n", 5, type=int)
    rec = get_recommender()
    similar = rec.get_similar_users(user_id, n=n)
    return jsonify({
        "user_id": user_id,
        "similar_users": similar.to_dict("records")
    })


@app.route("/api/user/<int:user_id>")
def api_user_profile(user_id):
    """Retourne le profil et l'historique d'un utilisateur."""
    prefs = get_user_preferences(user_id)
    if prefs.empty:
        return jsonify({"error": "Utilisateur non trouvé"}), 404

    genre_dist = prefs.groupby("genre_name")["rating"].agg(["count", "mean"]).reset_index()
    genre_dist.columns = ["genre", "count", "avg_rating"]

    return jsonify({
        "user_id": user_id,
        "total_movies_watched": len(prefs),
        "avg_rating": round(prefs["rating"].mean(), 2),
        "favorite_genre": prefs.groupby("genre_name")["rating"].mean().idxmax(),
        "genre_distribution": genre_dist.to_dict("records"),
        "watch_history": prefs.to_dict("records")
    })


@app.route("/api/top-movies")
def api_top_movies():
    """Retourne les films les mieux notés."""
    n = request.args.get("n", 20, type=int)
    top = get_top_movies(n)
    return jsonify({"top_movies": top.to_dict("records")})


@app.route("/api/genres")
def api_genres():
    """Retourne les statistiques par genre."""
    stats = get_genre_stats()
    return jsonify({"genres": stats.to_dict("records")})


@app.route("/api/model-info")
def api_model_info():
    """Retourne les informations sur le modèle."""
    rec = get_recommender()
    info = rec.get_model_info()
    info["matrix_shape"] = list(info["matrix_shape"])
    return jsonify(info)


@app.route("/api/predict")
def api_predict():
    """Prédit la note d'un utilisateur pour un film."""
    user_id = request.args.get("user_id", type=int)
    movie_name = request.args.get("movie", type=str)

    if not user_id or not movie_name:
        return jsonify({"error": "Paramètres user_id et movie requis"}), 400

    rec = get_recommender()
    prediction = rec.predict_rating(user_id, movie_name)

    return jsonify({
        "user_id": user_id,
        "movie": movie_name,
        "predicted_rating": prediction
    })


@app.route("/api/stats")
def api_stats():
    """Statistiques globales de la plateforme."""
    stats = query_sql("""
        SELECT
            COUNT(*) as total_ratings,
            COUNT(DISTINCT user_id) as total_users,
            COUNT(DISTINCT movie_id) as total_movies,
            ROUND(AVG(rating), 2) as avg_rating,
            MIN(watch_date) as first_date,
            MAX(watch_date) as last_date
        FROM ratings
    """)
    return jsonify(stats.to_dict("records")[0])


# ─── Initialisation ──────────────────────────────────────────

@app.route("/api/init", methods=["POST"])
def api_init():
    """Réinitialise la base de données et le modèle."""
    global recommender
    init_database()
    recommender = None
    get_recommender()
    return jsonify({"status": "ok", "message": "Système réinitialisé"})


if __name__ == "__main__":
    if not os.path.exists(DB_PATH):
        init_database()

    viz_dir = os.path.join(os.path.dirname(__file__), "static", "visualizations")
    if not os.listdir(viz_dir):
        print("Génération des analyses et visualisations...")
        run_full_analysis()

    print("Démarrage de l'API Flask...")
    app.run(debug=True, host="0.0.0.0", port=5000)
