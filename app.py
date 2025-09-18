from flask import Flask, render_template, request, jsonify, render_template_string, url_for
import pandas as pd
import requests, json, time, re
from pathlib import Path

app = Flask(__name__)

# ----- Static cache headers -----
@app.after_request
def add_header(response):
    if request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=31536000"
    return response

# ----- Settings -----
TMDB_API_KEY = "96872e54c86124eeb781f961d12e1aaf"
OMDB_API_KEY = "bebf6ae6"   # replace with your OMDb key
INPUT_CSV = Path("rotten_tomatoes_min85.csv")
CACHE_PATH = Path("movie_cache.json")

# ----- JSON cache helpers -----
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

# ----- Poster/plot helpers -----
def get_poster_and_plot(title, year=None):
    """Try TMDb → OMDb → placeholder fallback, with caching."""
    ykey = ""
    if year is not None and pd.notna(year):
        try:
            ykey = str(int(float(year)))
        except Exception:
            pass

    key = f"{title}|||{ykey}"
    if key in cache:
        return cache[key]

    data = {"Poster": None, "Plot": ""}

    # --- 1. TMDb ---
    try:
        params = {"api_key": TMDB_API_KEY, "query": title}
        if ykey:
            params["year"] = int(ykey)
        r = requests.get("https://api.themoviedb.org/3/search/movie", params=params, timeout=10)
        res = r.json()
        if res.get("results"):
            movie = res["results"][0]
            if movie.get("poster_path"):
                data["Poster"] = f"https://image.tmdb.org/t/p/w500{movie['poster_path']}"
            # details for plot
            mid = movie["id"]
            det = requests.get(
                f"https://api.themoviedb.org/3/movie/{mid}",
                params={"api_key": TMDB_API_KEY}, timeout=10
            ).json()
            data["Plot"] = det.get("overview") or ""
    except Exception as e:
        data["_tmdb_error"] = str(e)

    # --- 2. OMDb fallback ---
    if not data["Poster"] or data["Poster"] in ["N/A", ""]:
        try:
            omdb_url = f"http://www.omdbapi.com/?apikey={OMDB_API_KEY}&t={title}"
            if ykey:
                omdb_url += f"&y={ykey}"
            r = requests.get(omdb_url, timeout=10).json()
            if r.get("Poster") and r["Poster"] != "N/A":
                data["Poster"] = r["Poster"]
            if not data["Plot"] and r.get("Plot") and r["Plot"] != "N/A":
                data["Plot"] = r["Plot"]
        except Exception as e:
            data["_omdb_error"] = str(e)

 

    cache[key] = data
    save_cache(cache)
    time.sleep(0.18)  # throttle
    return data

# ----- CSV normalization -----
def parse_runtime_to_minutes(runtime_val):
    if pd.isna(runtime_val):
        return pd.NA
    s = str(runtime_val).strip().lower()
    m = re.search(r"(\d+)\s*min", s)
    if m:
        return int(m.group(1))
    if s.isdigit():
        return int(s)
    h = re.search(r"(\d+)\s*h", s)
    mins = re.search(r"(\d+)\s*m", s)
    if h and mins:
        return int(h.group(1)) * 60 + int(mins.group(1))
    if h and not mins:
        return int(h.group(1)) * 60
    m2 = re.search(r"(\d+)", s)
    if m2:
        return int(m2.group(1))
    return pd.NA

def extract_year(date_val):
    if pd.isna(date_val):
        return pd.NA
    m = re.search(r"(\d{4})", str(date_val))
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return pd.NA
    return pd.NA

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
    df["Title"] = df["movie_title"].astype(str).str.strip()
    df["ContentRating"] = df.get("content_rating", "").astype(str).str.strip()
    df["Genres"] = df.get("genres", "").astype(str)
    df["Directors"] = df.get("directors", "").astype(str)
    df["RuntimeMin"] = df.get("runtime", pd.NA).apply(parse_runtime_to_minutes)
    df["Year"] = df.get("original_release_date", pd.NA).apply(extract_year)
    df["CriticScore"] = pd.to_numeric(df.get("tomatometer_rating", pd.NA), errors="coerce")
    df["AudienceScore"] = pd.to_numeric(df.get("audience_rating", pd.NA), errors="coerce")

    cols = [
        "Title", "Year", "ContentRating", "Genres", "Directors",
        "RuntimeMin", "CriticScore", "AudienceScore"
    ]
    df = df[cols].copy()

    _MOVIES_DF = df
    _TOTAL_RAW = len(df)
    return _MOVIES_DF, _TOTAL_RAW

# ----- Main route -----
@app.route("/", methods=["GET", "POST"])
def index():
    df, total_raw = load_movies()

    all_rated = ["Any"] + sorted({r.strip() for r in df["ContentRating"].fillna("") if r.strip()})
    all_genres = ["Any"] + sorted({
        g.strip()
        for row in df["Genres"].fillna("")
        for g in str(row).split(",")
        if g.strip()
    })

    selected_rated = request.form.get("rated", "Any")
    selected_genre = request.form.get("genre", "Any")
    selected_runtime = request.form.get("runtime", "Any")

    filtered = df.copy()
    if selected_rated != "Any":
        filtered = filtered[filtered["ContentRating"].fillna("").str.lower() == selected_rated.lower()]
    if selected_genre != "Any":
        filtered = filtered[filtered["Genres"].fillna("").str.contains(fr"\b{re.escape(selected_genre)}\b", case=False, regex=True)]
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

    movie = None
    if request.method == "POST" and request.form.get("random") == "1" and count_after_ui > 0:
        row = filtered.sample(1).iloc[0]
        enrich = get_poster_and_plot(row["Title"], row["Year"])
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

# ----- AJAX route -----
@app.route("/random_movie", methods=["POST"])
def random_movie():
    df, total_raw = load_movies()

    selected_rated = request.form.get("rated", "Any")
    selected_genre = request.form.get("genre", "Any")
    selected_runtime = request.form.get("runtime", "Any")

    filtered = df.copy()
    if selected_rated != "Any":
        filtered = filtered[filtered["ContentRating"].fillna("").str.lower() == selected_rated.lower()]
    if selected_genre != "Any":
        filtered = filtered[filtered["Genres"].fillna("").str.contains(fr"\b{re.escape(selected_genre)}\b", case=False, regex=True)]
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
    if count_after_ui == 0:
        return jsonify({"html": "<p>No movies match your filters.</p>", "count": 0, "total": total_raw})

    row = filtered.sample(1).iloc[0]
    enrich = get_poster_and_plot(row["Title"], row["Year"])
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

    html = render_template_string(
        """{% include '_movie_card.html' %}""",
        movie=movie
    )
    return jsonify({"html": html, "count": count_after_ui, "total": total_raw})

if __name__ == "__main__":
    app.run(debug=True)
