from flask import Flask, render_template, request
import pandas as pd
import requests, json, time
from pathlib import Path
import re

app = Flask(__name__)

# ----- Static files cache (prevents header flicker) -----
@app.after_request
def add_header(response):
    if request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=31536000"
    return response

# ----- Settings -----
TMDB_API_KEY = "96872e54c86124eeb781f961d12e1aaf"
INPUT_CSV = Path("rotten_tomatoes_min85.csv")   # <-- your 1600-movie CSV
CACHE_PATH = Path("tmdb_cache.json")            # poster/plot cache

# ----- Simple JSON cache helpers -----
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

# ----- TMDb helpers (for poster + plot only) -----
def tmdb_search(title, year=None):
    params = {"api_key": TMDB_API_KEY, "query": title}
    if pd.notna(year):
        try:
            params["year"] = int(year)
        except Exception:
            pass
    r = requests.get("https://api.themoviedb.org/3/search/movie", params=params, timeout=10)
    data = r.json()
    if data.get("results"):
        return data["results"][0]["id"]
    return None

def tmdb_details(movie_id):
    params = {"api_key": TMDB_API_KEY}
    r = requests.get(f"https://api.themoviedb.org/3/movie/{movie_id}", params=params, timeout=10)
    return r.json()

def query_tmdb_minimal(title, year=None):
    """
    Get poster + plot from TMDb with caching.
    Keyed by 'title|||year' (year optional).
    """
    # Normalize year into a safe string key
    if year is None or pd.isna(year):
        ykey = ""
    else:
        try:
            # handle cases where year is float, string, or int
            ykey = str(int(float(year)))
        except Exception:
            ykey = ""

    key = f"{title}|||{ykey}"

    # Use cache if available
    if key in cache:
        return cache[key]

    data = {"Poster": None, "Plot": ""}

    try:
        movie_id = tmdb_search(title, ykey if ykey else None)
        if movie_id:
            details = tmdb_details(movie_id)
            poster = (
                f"https://image.tmdb.org/t/p/w500{details['poster_path']}"
                if details.get("poster_path")
                else None
            )
            plot = details.get("overview", "") or ""
            data = {"Poster": poster, "Plot": plot}
    except Exception as e:
        data["_error"] = str(e)

    cache[key] = data
    save_cache(cache)
    time.sleep(0.18)  # throttle requests
    return data


# ----- CSV loading & normalization -----
def parse_runtime_to_minutes(runtime_val):
    """
    Convert runtime field to minutes (int) if possible.
    Accepts formats like '123', '123 min', '1h 45m', '1h45m', etc.
    """
    if pd.isna(runtime_val):
        return pd.NA

    s = str(runtime_val).strip().lower()

    # 1) Plain minutes like '123' or '123 min'
    m = re.search(r"(\d+)\s*min", s)
    if m:
        return int(m.group(1))
    if s.isdigit():
        return int(s)

    # 2) '1h 45m' or '1h45m' or '2h'
    h = re.search(r"(\d+)\s*h", s)
    mins = re.search(r"(\d+)\s*m", s)
    if h and mins:
        return int(h.group(1)) * 60 + int(mins.group(1))
    if h and not mins:
        return int(h.group(1)) * 60

    # fallback: try to pull any number
    m2 = re.search(r"(\d+)", s)
    if m2:
        return int(m2.group(1))

    return pd.NA

def extract_year(date_val):
    """
    Extract a 4-digit year from a date string like '1995-07-14' or '1995'.
    """
    if pd.isna(date_val):
        return pd.NA
    m = re.search(r"(\d{4})", str(date_val))
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return pd.NA
    return pd.NA

# Keep a module-level dataframe so we don’t reload every request
_MOVIES_DF = None
_TOTAL_RAW = 0

def load_movies():
    global _MOVIES_DF, _TOTAL_RAW
    if _MOVIES_DF is not None:
        return _MOVIES_DF, _TOTAL_RAW

    if not INPUT_CSV.exists():
        _MOVIES_DF, _TOTAL_RAW = pd.DataFrame(), 0
        return _MOVIES_DF, _TOTAL_RAW

    df = pd.read_csv(INPUT_CSV, on_bad_lines="skip", low_memory=False)

    # Expected columns from your list:
    #   movie_title, content_rating, genres, directors, original_release_date, runtime,
    #   tomatometer_rating, audience_rating
    # Normalize columns into a common schema the app uses
    df["Title"] = df["movie_title"].astype(str).str.strip()
    df["ContentRating"] = df.get("content_rating", "").astype(str).str.strip()
    df["Genres"] = df.get("genres", "").astype(str)
    df["Directors"] = df.get("directors", "").astype(str)
    df["RuntimeMin"] = df.get("runtime", pd.NA).apply(parse_runtime_to_minutes)
    df["Year"] = df.get("original_release_date", pd.NA).apply(extract_year)
    df["CriticScore"] = pd.to_numeric(df.get("tomatometer_rating", pd.NA), errors="coerce")
    df["AudienceScore"] = pd.to_numeric(df.get("audience_rating", pd.NA), errors="coerce")

    # Keep a slim working set
    cols = [
        "Title", "Year", "ContentRating", "Genres", "Directors",
        "RuntimeMin", "CriticScore", "AudienceScore"
    ]
    df = df[cols].copy()

    _MOVIES_DF = df
    _TOTAL_RAW = len(df)
    return _MOVIES_DF, _TOTAL_RAW

# ----- Routes -----
@app.route("/", methods=["GET", "POST"])
def index():
    df, total_raw = load_movies()

    # Build dropdowns from CSV values (not TMDb)
    all_rated = ["Any"] + sorted({r.strip() for r in df["ContentRating"].fillna("") if r.strip()})
    all_genres = ["Any"] + sorted({
        g.strip()
        for row in df["Genres"].fillna("")
        for g in str(row).split(",")
        if g.strip()
    })

    # Read current selections; default to "Any"
    selected_rated = request.form.get("rated", "Any")
    selected_genre = request.form.get("genre", "Any")
    selected_runtime = request.form.get("runtime", "Any")  # "<90", "90-120", "120-150", "150+", "Any"

    # Apply filters
    filtered = df.copy()

    if selected_rated != "Any":
        filtered = filtered[
            filtered["ContentRating"].fillna("").str.lower() == selected_rated.lower()
        ]

    if selected_genre != "Any":
        # case-insensitive containment in the comma-separated genres string
        filtered = filtered[
            filtered["Genres"].fillna("").str.contains(fr"\b{re.escape(selected_genre)}\b", case=False, regex=True)
        ]

    if selected_runtime != "Any":
        rt = pd.to_numeric(filtered["RuntimeMin"], errors="coerce")
        if selected_runtime == "<90":
            filtered = filtered[rt.fillna(99999) < 90]
        elif selected_runtime == "90-120":
            filtered = filtered[(rt.fillna(-1) >= 90) & (rt <= 120)]
        elif selected_runtime == "120-150":
            filtered = filtered[(rt.fillna(-1) >= 120) & (rt <= 150)]
        elif selected_runtime == "150+":
            filtered = filtered[rt.fillna(-1) >= 150]

    count_after_ui = len(filtered)

    # Only pick random when button pressed AND we have matches
    movie = None
    if request.method == "POST" and request.form.get("random") == "1" and count_after_ui > 0:
        row = filtered.sample(1).iloc[0]
        # Minimal TMDb enrichment (poster + plot)
        enrich = query_tmdb_minimal(row["Title"], row["Year"])
        movie = {
            "Title": row["Title"],
            "Year": int(row["Year"]) if pd.notna(row["Year"]) else "",
            "ContentRating": row["ContentRating"],
            "Genres": row["Genres"],
            "Directors": row["Directors"],
            "RuntimeMin": int(row["RuntimeMin"]) if pd.notna(row["RuntimeMin"]) else None,
            "CriticScore": float(row["CriticScore"]) if pd.notna(row["CriticScore"]) else None,
            "AudienceScore": float(row["AudienceScore"]) if pd.notna(row["AudienceScore"]) else None,
            "Poster": enrich.get("Poster"),
            "Plot": enrich.get("Plot", "")
        }

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
