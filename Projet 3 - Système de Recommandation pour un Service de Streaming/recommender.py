"""
Moteur de recommandation collaboratif basé sur la similarité cosinus entre utilisateurs.
Utilise scikit-learn pour le calcul de similarité et la validation croisée.
"""

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import train_test_split
from database import get_user_movie_matrix, get_user_preferences, init_database, DB_PATH
import os


class CollaborativeRecommender:
    """Système de recommandation collaboratif User-Based."""

    def __init__(self):
        self.user_movie_matrix = None
        self.similarity_matrix = None
        self.user_ids = None
        self.movie_names = None
        self._is_fitted = False

    def fit(self, user_movie_matrix=None):
        """Entraîne le modèle sur la matrice utilisateur-film."""
        if user_movie_matrix is None:
            if not os.path.exists(DB_PATH):
                init_database()
            self.user_movie_matrix = get_user_movie_matrix()
        else:
            self.user_movie_matrix = user_movie_matrix

        self.user_ids = self.user_movie_matrix.index.tolist()
        self.movie_names = self.user_movie_matrix.columns.tolist()

        matrix_values = self.user_movie_matrix.values
        self.similarity_matrix = cosine_similarity(matrix_values)

        self._is_fitted = True
        return self

    def get_similar_users(self, user_id, n=10):
        """Retourne les n utilisateurs les plus similaires."""
        if not self._is_fitted:
            raise ValueError("Le modèle n'est pas encore entraîné. Appelez fit() d'abord.")

        if user_id not in self.user_ids:
            return pd.DataFrame(columns=["user_id", "similarity"])

        user_idx = self.user_ids.index(user_id)
        similarities = self.similarity_matrix[user_idx]

        similar_indices = np.argsort(similarities)[::-1][1:n + 1]

        return pd.DataFrame({
            "user_id": [self.user_ids[i] for i in similar_indices],
            "similarity": [similarities[i] for i in similar_indices]
        })

    def recommend(self, user_id, n=10, min_similarity=0.1):
        """
        Génère des recommandations pour un utilisateur.
        
        Algorithme :
        1. Trouver les utilisateurs similaires (cosine similarity)
        2. Identifier les films non vus par l'utilisateur cible
        3. Pondérer les notes des voisins par leur similarité
        4. Retourner les films avec le score pondéré le plus élevé
        """
        if not self._is_fitted:
            raise ValueError("Le modèle n'est pas encore entraîné. Appelez fit() d'abord.")

        if user_id not in self.user_ids:
            return self._cold_start_recommendations(n)

        user_idx = self.user_ids.index(user_id)
        user_ratings = self.user_movie_matrix.loc[user_id]

        unseen_movies = user_ratings[user_ratings == 0].index.tolist()
        if not unseen_movies:
            return pd.DataFrame(columns=["movie_name", "predicted_rating", "confidence"])

        similarities = self.similarity_matrix[user_idx]
        neighbor_mask = similarities > min_similarity
        neighbor_mask[user_idx] = False

        if not neighbor_mask.any():
            return self._cold_start_recommendations(n)

        neighbor_sims = similarities[neighbor_mask]
        neighbor_ratings = self.user_movie_matrix.values[neighbor_mask]

        predictions = []
        for movie in unseen_movies:
            movie_idx = self.movie_names.index(movie)
            movie_ratings = neighbor_ratings[:, movie_idx]

            rated_mask = movie_ratings > 0
            if not rated_mask.any():
                continue

            relevant_sims = neighbor_sims[rated_mask]
            relevant_ratings = movie_ratings[rated_mask]

            sim_sum = np.sum(np.abs(relevant_sims))
            if sim_sum == 0:
                continue

            predicted_rating = np.sum(relevant_sims * relevant_ratings) / sim_sum
            confidence = min(len(relevant_ratings) / 5, 1.0)

            predictions.append({
                "movie_name": movie,
                "predicted_rating": round(predicted_rating, 2),
                "confidence": round(confidence, 2)
            })

        result = pd.DataFrame(predictions)
        if result.empty:
            return self._cold_start_recommendations(n)

        result = result.sort_values(
            by=["predicted_rating", "confidence"],
            ascending=[False, False]
        ).head(n).reset_index(drop=True)

        return result

    def _cold_start_recommendations(self, n=10):
        """Recommandations par défaut basées sur la popularité globale."""
        mean_ratings = self.user_movie_matrix.replace(0, np.nan).mean()
        count_ratings = (self.user_movie_matrix > 0).sum()

        popularity = pd.DataFrame({
            "movie_name": self.movie_names,
            "predicted_rating": mean_ratings.values,
            "confidence": (count_ratings / count_ratings.max()).values
        }).dropna()

        return popularity.sort_values(
            by=["predicted_rating", "confidence"],
            ascending=[False, False]
        ).head(n).reset_index(drop=True)

    def predict_rating(self, user_id, movie_name):
        """Prédit la note qu'un utilisateur donnerait à un film spécifique."""
        if not self._is_fitted:
            raise ValueError("Le modèle n'est pas encore entraîné.")

        if user_id not in self.user_ids or movie_name not in self.movie_names:
            return None

        user_idx = self.user_ids.index(user_id)
        movie_idx = self.movie_names.index(movie_name)

        similarities = self.similarity_matrix[user_idx]
        all_ratings = self.user_movie_matrix.values[:, movie_idx]

        rated_mask = all_ratings > 0
        rated_mask[user_idx] = False

        if not rated_mask.any():
            return None

        relevant_sims = similarities[rated_mask]
        relevant_ratings = all_ratings[rated_mask]

        sim_sum = np.sum(np.abs(relevant_sims))
        if sim_sum == 0:
            return None

        return round(np.sum(relevant_sims * relevant_ratings) / sim_sum, 2)

    def get_model_info(self):
        """Retourne les informations sur le modèle."""
        if not self._is_fitted:
            return {"status": "Non entraîné"}

        matrix = self.user_movie_matrix.values
        sparsity = 1 - (np.count_nonzero(matrix) / matrix.size)

        return {
            "status": "Entraîné",
            "num_users": len(self.user_ids),
            "num_movies": len(self.movie_names),
            "matrix_shape": matrix.shape,
            "sparsity": f"{sparsity:.1%}",
            "avg_similarity": f"{self.similarity_matrix[np.triu_indices_from(self.similarity_matrix, k=1)].mean():.4f}",
            "num_ratings": int(np.count_nonzero(matrix)),
        }


def build_recommender():
    """Construit et retourne un recommender entraîné."""
    recommender = CollaborativeRecommender()
    recommender.fit()
    return recommender


if __name__ == "__main__":
    rec = build_recommender()
    info = rec.get_model_info()

    print("=" * 50)
    print("  MODÈLE DE RECOMMANDATION")
    print("=" * 50)
    for k, v in info.items():
        print(f"  {k:20s}: {v}")

    test_user = rec.user_ids[0]
    print(f"\nRecommandations pour l'utilisateur {test_user} :")
    print(rec.recommend(test_user, n=10).to_string(index=False))

    print(f"\nUtilisateurs similaires à {test_user} :")
    print(rec.get_similar_users(test_user, n=5).to_string(index=False))
