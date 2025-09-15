from flask import Flask, render_template, request
import pandas as pd
import requests, json, re, time
from pathlib import Path
from urllib.parse import quote_plus

app = Flask(__name__)

API_KEY = "bebf6ae6"  # Replace with your OMDb API key
INPUT_CSV = Path("movies_min85.csv")
CACHE_PATH = Path("omdb_cache.json")

# ---------------- Cache helpers ----------------
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

# ---------------- OMDb lookup ----------------
def query_omdb(title: str, year=None):
    """Query OMDb API for a movie by title/year. Returns dict with fields including Rated, Genre, Runtime."""
    ykey = "" if year is None or pd.isna(year) else str(int(year))
    key = f"{title}|||{ykey}"
    if key in cache:
        return cache[key]

    params = {"t": title, "apikey": API_KEY}
    if ykey:
        params["y"] = ykey

    url = "http://www.omdbapi.com/?" + "&".join(f"{k}={quote_plus(str(v))}" for k, v in params.items())
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
    except Exception as e:
        data = {"Response": "False", "Error": str(e)}

    cache[key] = data
    save_cache(cache)
    # Respect free OMDb API limits
    time.sleep(0.2)
    return data

# ---------------- Runtime parsing ----------------
def parse_runtime_to_minutes(runtime_str: str) -> int | None:
    """Parse a runtime string like '123 min' or '2h 5m' into minutes."""
    if not runtime_str or runtime_str == "N/A":
        return None
    runtime_str = runtime_str.lower()

    # h/m pattern
    match = re.match(r"(?:(\d+)\s*h)?\s*(?:(\d+)\s*m)?", runtime_str)
    if match:
        h = int(match.group(1) or 0)
        m = int(match.group(2) or 0)
        total = h * 60 + m
        if total > 0:
            return total

    # plain digits fallback
    digits = re.findall(r"\d+", runtime_str)
    if digits:
        return int(digits[0])
    return None

# ---------------- Data loading ----------------
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

    # Enrich with OMDb (Rating, Genre, Runtime) + show progress
    ratings, genres, runtimes = [], [], []
    for i, row in df.iterrows():
        title = row["title"]
        year = row.get("release_year", None)

        omdb = query_omdb(title, year)

        ratings.append(omdb.get("Rated", ""))
        genres.append(omdb.get("Genre", ""))
        runtimes.append(parse_runtime_to_minutes(omdb.get("Runtime", "")))

        # Progress every 25 movies
        if (i + 1) % 25 == 0 or (i + 1) == total_raw:
            print(f"Enriched {i+1}/{total_raw} movies...")

    df["Rating"] = ratings
    df["Genre"] = genres
    df["RuntimeMin"] = runtimes

    return df, total_raw

# ---------------- Flask routes ----------------
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
        omdb = query_omdb(movie.get("title", ""), movie.get("release_year"))
        if omdb.get("Response") == "True":
            movie["Plot"] = omdb.get("Plot", "")
            poster = omdb.get("Poster", "")
            movie["Poster"] = poster if poster and poster != "N/A" else None

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
