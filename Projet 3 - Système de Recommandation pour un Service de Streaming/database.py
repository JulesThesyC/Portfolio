"""
Module de gestion de la base de données SQLite.
Charge le CSV, crée les tables normalisées et expose des fonctions de requête SQL.
"""

import sqlite3
import pandas as pd
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "streaming.db")
CSV_PATH = os.path.join(os.path.dirname(__file__), "Streaming_Usage.csv")


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_database():
    """Crée la base SQLite à partir du CSV avec un schéma normalisé."""
    df = pd.read_csv(CSV_PATH, parse_dates=["Watch_Date"])

    conn = get_connection()
    cursor = conn.cursor()

    cursor.executescript("""
        DROP TABLE IF EXISTS ratings;
        DROP TABLE IF EXISTS movies;
        DROP TABLE IF EXISTS users;
        DROP TABLE IF EXISTS genres;

        CREATE TABLE genres (
            genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
            genre_name TEXT UNIQUE NOT NULL
        );

        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY
        );

        CREATE TABLE movies (
            movie_id INTEGER PRIMARY KEY AUTOINCREMENT,
            movie_name TEXT UNIQUE NOT NULL
        );

        CREATE TABLE ratings (
            rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            movie_id INTEGER NOT NULL,
            genre_id INTEGER NOT NULL,
            rating INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
            watch_date DATE NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
            FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
        );

        CREATE INDEX idx_ratings_user ON ratings(user_id);
        CREATE INDEX idx_ratings_movie ON ratings(movie_id);
    """)

    genres = df["Genre"].unique()
    for g in genres:
        cursor.execute("INSERT INTO genres (genre_name) VALUES (?)", (g,))

    users = df["User_ID"].unique()
    for u in users:
        cursor.execute("INSERT INTO users (user_id) VALUES (?)", (int(u),))

    movies = df["Movie_Name"].unique()
    for m in movies:
        cursor.execute("INSERT INTO movies (movie_name) VALUES (?)", (m,))

    genre_map = dict(cursor.execute("SELECT genre_name, genre_id FROM genres").fetchall())
    movie_map = dict(cursor.execute("SELECT movie_name, movie_id FROM movies").fetchall())

    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO ratings (user_id, movie_id, genre_id, rating, watch_date) VALUES (?, ?, ?, ?, ?)",
            (int(row["User_ID"]), movie_map[row["Movie_Name"]],
             genre_map[row["Genre"]], int(row["Rating"]), str(row["Watch_Date"].date()))
        )

    conn.commit()
    conn.close()
    print(f"Base de données créée : {DB_PATH}")
    print(f"  - {len(users)} utilisateurs")
    print(f"  - {len(movies)} films")
    print(f"  - {len(genres)} genres")
    print(f"  - {len(df)} évaluations")


def query_sql(sql, params=None):
    """Exécute une requête SQL et retourne un DataFrame."""
    conn = get_connection()
    result = pd.read_sql_query(sql, conn, params=params or [])
    conn.close()
    return result


def get_user_movie_matrix():
    """Retourne la matrice utilisateur-film (pivot) depuis SQL."""
    sql = """
        SELECT r.user_id, m.movie_name, r.rating
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
    """
    df = query_sql(sql)
    return df.pivot_table(index="user_id", columns="movie_name", values="rating").fillna(0)


def get_user_preferences(user_id):
    """Retourne les films vus et les préférences de genre d'un utilisateur."""
    sql = """
        SELECT m.movie_name, g.genre_name, r.rating, r.watch_date
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        JOIN genres g ON r.genre_id = g.genre_id
        WHERE r.user_id = ?
        ORDER BY r.rating DESC
    """
    return query_sql(sql, [user_id])


def get_top_movies(n=20):
    """Retourne les films les mieux notés (moyenne pondérée par nombre de vues)."""
    sql = """
        SELECT m.movie_name, g.genre_name,
               ROUND(AVG(r.rating), 2) as avg_rating,
               COUNT(*) as num_ratings
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        JOIN genres g ON r.genre_id = g.genre_id
        GROUP BY m.movie_name
        HAVING num_ratings >= 3
        ORDER BY avg_rating DESC, num_ratings DESC
        LIMIT ?
    """
    return query_sql(sql, [n])


def get_genre_stats():
    """Statistiques par genre."""
    sql = """
        SELECT g.genre_name,
               COUNT(*) as total_views,
               ROUND(AVG(r.rating), 2) as avg_rating,
               COUNT(DISTINCT r.user_id) as unique_users
        FROM ratings r
        JOIN genres g ON r.genre_id = g.genre_id
        GROUP BY g.genre_name
        ORDER BY total_views DESC
    """
    return query_sql(sql)


if __name__ == "__main__":
    init_database()
