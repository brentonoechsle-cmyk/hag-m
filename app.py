from flask import Flask, render_template, request
import pandas as pd
import requests, json, re, time
from pathlib import Path
from urllib.parse import quote_plus

app = Flask(__name__)


@app.after_request
def add_header(response):
    if request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=31536000"
    return response

# --- Settings ---
TMDB_API_KEY = "96872e54c86124eeb781f961d12e1aaf"
INPUT_CSV = Path("movies_min85.csv")
CACHE_PATH = Path("tmdb_cache.json")

# --- Cache helpers ---
def load_cache():
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cache(cache: dict):
    CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")

cache = load_cache()

# --- TMDb helpers ---
def tmdb_search(title, year=None):
    """Search TMDb by title and optional year to get a movie ID."""
    params = {"api_key": TMDB_API_KEY, "query": title}
    if year:
        params["year"] = int(year)
    url = "https://api.themoviedb.org/3/search/movie"
    r = requests.get(url, params=params, timeout=10)
    data = r.json()
    if data.get("results"):
        return data["results"][0]["id"]
    return None

def tmdb_details(movie_id):
    """Fetch movie details (runtime, genres, rating, poster, plot)."""
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {"api_key": TMDB_API_KEY, "append_to_response": "release_dates"}
    r = requests.get(url, params=params, timeout=10)
    return r.json()

def query_tmdb(title, year=None):
    """Full TMDb enrichment with caching."""
    ykey = "" if year is None or pd.isna(year) else str(int(year))
    key = f"{title}|||{ykey}"
    if key in cache:
        return cache[key]

    try:
        movie_id = tmdb_search(title, year)
        if not movie_id:
            data = {"Response": "False", "Error": "Not found"}
        else:
            details = tmdb_details(movie_id)
            # Extract fields
            runtime = details.get("runtime", None)
            genres = ", ".join([g["name"] for g in details.get("genres", [])]) if details.get("genres") else ""
            # Certification: look for US certification in release_dates
            rating = ""
            releases = details.get("release_dates", {}).get("results", [])
            for entry in releases:
                if entry.get("iso_3166_1") == "US":
                    for rel in entry.get("release_dates", []):
                        cert = rel.get("certification", "")
                        if cert:
                            rating = cert
                            break
            poster = f"https://image.tmdb.org/t/p/w500{details['poster_path']}" if details.get("poster_path") else None
            plot = details.get("overview", "")

            data = {
                "Response": "True",
                "RuntimeMin": runtime,
                "Genre": genres,
                "Rated": rating,
                "Poster": poster,
                "Plot": plot
            }
    except Exception as e:
        data = {"Response": "False", "Error": str(e)}

    cache[key] = data
    save_cache(cache)
    time.sleep(0.2)  # Be polite to TMDb API
    return data

# --- Data loading ---
def load_movies():
    if not INPUT_CSV.exists():
        return pd.DataFrame(), 0

    df = pd.read_csv(INPUT_CSV, on_bad_lines="skip", low_memory=False)
    total_raw = len(df)

    # Normalize year
    year_col = "release_year" if "release_year" in df.columns else "release_date" if "release_date" in df.columns else None
    if year_col:
        df["release_year"] = pd.to_numeric(df[year_col].astype(str).str.extract(r"(\d{4})")[0], errors="coerce")
    else:
        df["release_year"] = pd.NA

    ratings, genres, runtimes, posters, plots = [], [], [], [], []
    for i, row in df.iterrows():
        title = row["title"]
        year = row.get("release_year", None)
        tmdb = query_tmdb(title, year)

        ratings.append(tmdb.get("Rated", ""))
        genres.append(tmdb.get("Genre", ""))
        runtimes.append(tmdb.get("RuntimeMin", None))
        posters.append(tmdb.get("Poster", None))
        plots.append(tmdb.get("Plot", ""))

        # Progress every 25 movies
        if (i + 1) % 25 == 0 or (i + 1) == total_raw:
            print(f"Enriched {i+1}/{total_raw} movies...")

    df["Rating"] = ratings
    df["Genre"] = genres
    df["RuntimeMin"] = runtimes
    df["Poster"] = posters
    df["Plot"] = plots

    return df, total_raw

# --- Flask routes ---
@app.route("/", methods=["GET", "POST"])
def index():
    df, total_raw = load_movies()

    # Build dropdowns
    all_genres = ["Any"] + sorted({g.strip() for row in df["Genre"].fillna("") for g in str(row).split(",") if g.strip()})
    all_rated = ["Any"] + sorted({r for r in df["Rating"].fillna("") if r and r != "N/A"})

    # Current selections
    selected_rated = request.form.get("rated", "Any")
    selected_genre = request.form.get("genre", "Any")
    selected_runtime = request.form.get("runtime", "Any")

    # Apply filters
    filtered = df.copy()
    if selected_rated != "Any":
        filtered = filtered[
            filtered["Rating"].fillna("").str.strip().str.lower() == selected_rated.lower()
        ]
    if selected_genre != "Any":
        filtered = filtered[
            filtered["Genre"].fillna("").str.lower().str.contains(selected_genre.lower())
        ]
    if selected_runtime != "Any":
        rt = pd.to_numeric(filtered["RuntimeMin"], errors="coerce")
        if selected_runtime == "<90":
            filtered = filtered[rt.fillna(9999) < 90]
        elif selected_runtime == "90-120":
            filtered = filtered[(rt.fillna(-1) >= 90) & (rt <= 120)]
        elif selected_runtime == "120-150":
            filtered = filtered[(rt.fillna(-1) >= 120) & (rt <= 150)]
        elif selected_runtime == "150+":
            filtered = filtered[rt.fillna(-1) >= 150]

    count_after_ui = len(filtered)

    # Only pick random if >=1 match
    movie = None
    if request.method == "POST" and "random" in request.form and count_after_ui > 0:
        row = filtered.sample(1).iloc[0]
        movie = row.to_dict()

    return render_template(
        "index.html",
        all_genres=all_genres,
        all_rated=all_rated,
        selected_rated=selected_rated,
        selected_genre=selected_genre,
        selected_runtime=selected_runtime,
        total_raw=total_raw,
        count=count_after_ui,
        movie=movie
    )

if __name__ == "__main__":
    app.run(debug=True)
